//! Duplicate detection over accepted records.

use std::collections::{HashMap, HashSet, VecDeque};

use rusqlite::{Connection, OptionalExtension, params};
use tempfile::NamedTempFile;

use serde_json::Value;

use crate::{
    Error, Result,
    spec::{DedupConfig, DedupStorage, Normalize, Tokenization},
};

/// Conservative accounting units include keys, signatures and hash-table overhead.
/// This bounds index growth, not allocator metadata or transient record buffers.
pub(crate) struct DedupState {
    index: DedupIndex,
    used: usize,
    budget: usize,
    disk: Option<DiskIndex>,
}
impl DedupState {
    #[cfg(test)]
    pub(crate) fn new(config: &DedupConfig) -> Self {
        Self::with_budget(config, 64 * 1024 * 1024)
    }
    #[cfg(test)]
    pub(crate) fn with_budget(config: &DedupConfig, budget: usize) -> Self {
        Self::with_storage(config, budget, DedupStorage::Memory).expect("memory index")
    }
    pub(crate) fn with_storage(
        config: &DedupConfig,
        budget: usize,
        storage: DedupStorage,
    ) -> Result<Self> {
        Ok(Self {
            index: DedupIndex::new(config),
            used: 0,
            budget,
            disk: match storage {
                DedupStorage::Memory => None,
                DedupStorage::Disk => Some(DiskIndex::new()?),
            },
        })
    }
    fn charge(&self, key: &PreparedKey) -> usize {
        match key {
            PreparedKey::Exact(_) => 128,
            PreparedKey::MinHash(signature) => {
                let DedupIndex::MinHash(index) = &self.index else {
                    unreachable!()
                };
                128 + signature.len() * 8 + index.bands * 128
            }
        }
    }
    pub(crate) fn check(&self, record: &Value) -> Result<DedupAdmission> {
        if let Some(disk) = &self.disk {
            let key = self.index.prepare(record)?;
            return if disk.contains(&key, &self.index)? {
                Ok(DedupAdmission::Duplicate)
            } else {
                Ok(DedupAdmission::Unique(key))
            };
        }
        let admission = self.index.check(record)?;
        if let DedupAdmission::Unique(key) = &admission
            && self.charge(key) > self.budget.saturating_sub(self.used)
        {
            return Err(Error::RunFailed("dedup memory accounting budget exceeded; increase dedup_max_bytes or set dedup_storage to disk".into()));
        }
        Ok(admission)
    }
    pub(crate) fn register(&mut self, key: PreparedKey) -> Result<()> {
        if let Some(disk) = &mut self.disk {
            disk.register(&key, &self.index)?;
        } else {
            self.used += self.charge(&key);
            self.index.register(key);
        }
        Ok(())
    }
    pub(crate) fn restore(&mut self, record: &Value) -> Result<()> {
        if let DedupAdmission::Unique(key) = self.check(record)? {
            self.register(key)?;
        }
        Ok(())
    }
}

/// This is a disposable cache, not a source of recovery truth. Keep SQLite's
/// page cache at 2 MiB and use file-backed temporary storage. Each candidate is
/// read separately, so popular LSH buckets cannot materialize an unbounded list.
struct DiskIndex {
    connection: Connection,
    // Fields drop in declaration order: close the connection before unlinking.
    _file: NamedTempFile,
}
impl DiskIndex {
    fn new() -> Result<Self> {
        let file = NamedTempFile::new()
            .map_err(|error| Error::RunFailed(format!("create disk dedup index: {error}")))?;
        let connection = Connection::open(file.path()).map_err(disk_error)?;
        connection
            .execute_batch(
                "PRAGMA cache_size = -2048;
             PRAGMA temp_store = FILE;
             PRAGMA mmap_size = 0;
             PRAGMA journal_mode = MEMORY;
             PRAGMA synchronous = OFF;
             CREATE TABLE exact_keys (key BLOB PRIMARY KEY) WITHOUT ROWID;
             CREATE TABLE signatures (id INTEGER PRIMARY KEY, signature BLOB NOT NULL);
             CREATE TABLE buckets (
                 band INTEGER NOT NULL,
                 key BLOB NOT NULL,
                 signature_id INTEGER NOT NULL,
                 PRIMARY KEY (band, key, signature_id)
             ) WITHOUT ROWID;",
            )
            .map_err(disk_error)?;
        Ok(Self {
            connection,
            _file: file,
        })
    }

    fn contains(&self, key: &PreparedKey, index: &DedupIndex) -> Result<bool> {
        match (key, index) {
            (PreparedKey::Exact(key), DedupIndex::Exact { .. }) => {
                let mut statement = self
                    .connection
                    .prepare_cached("SELECT 1 FROM exact_keys WHERE key = ?1")
                    .map_err(disk_error)?;
                Ok(statement
                    .query_row([key.as_slice()], |_| Ok(()))
                    .optional()
                    .map_err(disk_error)?
                    .is_some())
            }
            (PreparedKey::MinHash(signature), DedupIndex::MinHash(index)) => {
                let mut statement = self
                    .connection
                    .prepare_cached(
                        "SELECT signatures.signature FROM buckets
                     JOIN signatures ON signatures.id = buckets.signature_id
                     WHERE buckets.band = ?1 AND buckets.key = ?2",
                    )
                    .map_err(disk_error)?;
                for band in 0..index.bands {
                    let key = band_hash(&signature[band * index.rows..(band + 1) * index.rows])
                        .to_be_bytes();
                    let mut candidates = statement
                        .query(params![band as i64, key.as_slice()])
                        .map_err(disk_error)?;
                    while let Some(candidate) = candidates.next().map_err(disk_error)? {
                        let bytes = candidate
                            .get_ref(0)
                            .map_err(disk_error)?
                            .as_blob()
                            .map_err(|error| {
                                Error::RunFailed(format!("invalid disk dedup signature: {error}"))
                            })?;
                        if bytes.len() != signature.len() * 8 {
                            return Err(Error::RunFailed(
                                "invalid disk dedup signature length".into(),
                            ));
                        }
                        let equal = signature
                            .iter()
                            .zip(bytes.chunks_exact(8))
                            .filter(|(value, bytes)| value.to_be_bytes().as_slice() == *bytes)
                            .count();
                        if equal as f64 / signature.len() as f64 >= index.threshold {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            _ => unreachable!("prepared key matches its dedup configuration"),
        }
    }

    fn register(&mut self, key: &PreparedKey, index: &DedupIndex) -> Result<()> {
        match (key, index) {
            (PreparedKey::Exact(key), DedupIndex::Exact { .. }) => {
                self.connection
                    .prepare_cached("INSERT INTO exact_keys (key) VALUES (?1)")
                    .map_err(disk_error)?
                    .execute([key.as_slice()])
                    .map_err(disk_error)?;
            }
            (PreparedKey::MinHash(signature), DedupIndex::MinHash(index)) => {
                let bytes: Vec<u8> = signature
                    .iter()
                    .flat_map(|value| value.to_be_bytes())
                    .collect();
                let transaction = self.connection.transaction().map_err(disk_error)?;
                transaction
                    .execute("INSERT INTO signatures (signature) VALUES (?1)", [bytes])
                    .map_err(disk_error)?;
                let id = transaction.last_insert_rowid();
                {
                    let mut statement = transaction
                        .prepare_cached(
                            "INSERT INTO buckets (band, key, signature_id) VALUES (?1, ?2, ?3)",
                        )
                        .map_err(disk_error)?;
                    for band in 0..index.bands {
                        let key = band_hash(&signature[band * index.rows..(band + 1) * index.rows])
                            .to_be_bytes();
                        statement
                            .execute(params![band as i64, key.as_slice(), id])
                            .map_err(disk_error)?;
                    }
                }
                transaction.commit().map_err(disk_error)?;
            }
            _ => unreachable!("prepared key matches its dedup configuration"),
        }
        Ok(())
    }
}

fn disk_error(error: rusqlite::Error) -> Error {
    Error::RunFailed(format!("disk dedup index: {error}"))
}

enum DedupIndex {
    Exact {
        seen: HashSet<[u8; 32]>,
        fields: Vec<String>,
        normalize: Normalize,
    },
    MinHash(MinHashIndex),
}

impl DedupIndex {
    pub(crate) fn new(config: &DedupConfig) -> Self {
        match config {
            DedupConfig::Exact { fields, normalize } => Self::Exact {
                seen: HashSet::new(),
                fields: fields.clone().unwrap_or_default(),
                normalize: *normalize,
            },
            DedupConfig::MinHash {
                field,
                num_perm,
                bands,
                threshold,
                shingle_words,
                tokenization,
                shingle_chars,
            } => Self::MinHash(MinHashIndex::new(
                field.clone(),
                *num_perm as usize,
                *bands as usize,
                *threshold,
                *shingle_words,
                *tokenization,
                *shingle_chars,
            )),
        }
    }

    fn prepare(&self, record: &Value) -> Result<PreparedKey> {
        match self {
            Self::Exact {
                fields, normalize, ..
            } => Ok(PreparedKey::Exact(exact_key(record, fields, *normalize)?)),
            Self::MinHash(index) => Ok(PreparedKey::MinHash(
                index.signature(text_field(record, &index.field)?),
            )),
        }
    }

    /// Decide whether a record duplicates an earlier accepted one without
    /// touching the state; the caller registers the prepared key only after
    /// the sink accepted the record.
    pub(crate) fn check(&self, record: &Value) -> Result<DedupAdmission> {
        match self {
            Self::Exact {
                seen,
                fields,
                normalize,
            } => {
                let key = exact_key(record, fields, *normalize)?;
                if seen.contains(&key) {
                    Ok(DedupAdmission::Duplicate)
                } else {
                    Ok(DedupAdmission::Unique(PreparedKey::Exact(key)))
                }
            }
            Self::MinHash(index) => index.check(record),
        }
    }

    /// Commit a unique admission into the state after sink acceptance.
    pub(crate) fn register(&mut self, prepared: PreparedKey) {
        match (self, prepared) {
            (Self::Exact { seen, .. }, PreparedKey::Exact(key)) => {
                seen.insert(key);
            }
            (Self::MinHash(index), PreparedKey::MinHash(signature)) => {
                index.register(signature);
            }
            _ => {}
        }
    }
}

pub(crate) enum DedupAdmission {
    Duplicate,
    Unique(PreparedKey),
}

pub(crate) enum PreparedKey {
    Exact([u8; 32]),
    MinHash(Vec<u64>),
}

/// Banded MinHash/LSH index over one text field. Signatures estimate Jaccard
/// similarity; bands keep candidate lookup sublinear.
pub(crate) struct MinHashIndex {
    field: String,
    num_perm: usize,
    rows: usize,
    bands: usize,
    threshold: f64,
    shingle_words: usize,
    tokenization: Tokenization,
    shingle_chars: usize,
    signatures: Vec<Vec<u64>>,
    buckets: HashMap<(usize, u64), Vec<u32>>,
}

impl MinHashIndex {
    fn new(
        field: String,
        num_perm: usize,
        bands: usize,
        threshold: f64,
        shingle_words: usize,
        tokenization: Tokenization,
        shingle_chars: usize,
    ) -> Self {
        Self {
            field,
            num_perm,
            rows: num_perm / bands,
            bands,
            threshold,
            shingle_words,
            tokenization,
            shingle_chars,
            signatures: Vec::new(),
            buckets: HashMap::new(),
        }
    }

    fn check(&self, record: &Value) -> Result<DedupAdmission> {
        let text = text_field(record, &self.field)?;
        let signature = self.signature(text);
        for band in 0..self.bands {
            let key = (
                band,
                band_hash(&signature[band * self.rows..(band + 1) * self.rows]),
            );
            if let Some(candidates) = self.buckets.get(&key)
                && candidates.iter().any(|&candidate| {
                    similarity(&signature, &self.signatures[candidate as usize]) >= self.threshold
                })
            {
                return Ok(DedupAdmission::Duplicate);
            }
        }
        Ok(DedupAdmission::Unique(PreparedKey::MinHash(signature)))
    }

    fn register(&mut self, signature: Vec<u64>) {
        let index = self.signatures.len() as u32;
        for band in 0..self.bands {
            let key = (
                band,
                band_hash(&signature[band * self.rows..(band + 1) * self.rows]),
            );
            self.buckets.entry(key).or_default().push(index);
        }
        self.signatures.push(signature);
    }

    fn signature(&self, text: &str) -> Vec<u64> {
        let mut signature = vec![u64::MAX; self.num_perm];
        let size = match self.tokenization {
            Tokenization::Words => self.shingle_words,
            Tokenization::Characters => self.shingle_chars,
        };
        for_each_shingle(text, size, self.tokenization, |shingle| {
            let base = u64::from_be_bytes(
                blake3::hash(shingle.as_bytes()).as_bytes()[..8]
                    .try_into()
                    .expect("eight bytes"),
            );
            for (permutation, slot) in signature.iter_mut().enumerate() {
                let hashed = permutation_hash(permutation as u64, base);
                if hashed < *slot {
                    *slot = hashed;
                }
            }
        });
        signature
    }
}

fn text_field<'a>(record: &'a Value, field: &str) -> Result<&'a str> {
    let value = extract(record, field).ok_or_else(|| Error::DedupField {
        field: field.to_owned(),
    })?;
    value.as_str().ok_or_else(|| Error::DedupField {
        field: format!("{field} is not a string"),
    })
}

/// Emit one shingle at a time. The window is bounded by the configured size;
/// character mode never materializes a character or shingle list for the text.
/// Short (including empty) inputs emit one shingle for deterministic dedup.
fn for_each_shingle(text: &str, n: usize, mode: Tokenization, mut emit: impl FnMut(&str)) {
    match mode {
        Tokenization::Words => {
            let mut window = VecDeque::with_capacity(n);
            let mut emitted = false;
            for word in text.split_whitespace().map(str::to_lowercase) {
                if window.len() == n {
                    window.pop_front();
                }
                window.push_back(word);
                if window.len() == n {
                    emit(
                        &window
                            .iter()
                            .map(String::as_str)
                            .collect::<Vec<_>>()
                            .join("\u{1f}"),
                    );
                    emitted = true;
                }
            }
            if !emitted {
                emit(
                    &window
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join("\u{1f}"),
                );
            }
        }
        Tokenization::Characters => {
            let mut window = VecDeque::with_capacity(n);
            let mut emitted = false;
            for character in text
                .chars()
                .filter(|c| !c.is_whitespace())
                .flat_map(char::to_lowercase)
            {
                if window.len() == n {
                    window.pop_front();
                }
                window.push_back(character);
                if window.len() == n {
                    emit(&window.iter().collect::<String>());
                    emitted = true;
                }
            }
            if !emitted {
                emit(&window.iter().collect::<String>());
            }
        }
    }
}

#[cfg(test)]
fn shingles(text: &str, n: usize) -> Vec<String> {
    let mut result = Vec::new();
    for_each_shingle(text, n, Tokenization::Words, |shingle| {
        result.push(shingle.to_owned())
    });
    result
}

/// Pairwise-independent permutation of the 64-bit hash space.
fn permutation_hash(seed: u64, value: u64) -> u64 {
    let mut z = value ^ seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn band_hash(rows: &[u64]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    for value in rows {
        hasher.update(&value.to_be_bytes());
    }
    u64::from_be_bytes(
        hasher.finalize().as_bytes()[..8]
            .try_into()
            .expect("eight bytes"),
    )
}

/// Fraction of equal signature components estimates Jaccard similarity.
fn similarity(a: &[u64], b: &[u64]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let equal = a.iter().zip(b).filter(|(x, y)| x == y).count();
    equal as f64 / a.len() as f64
}

fn extract<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = record;
    for segment in path.split('.') {
        current = match current {
            Value::Object(map) => map.get(segment)?,
            Value::Array(items) => items.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// An empty field list keys on the whole generated object.
fn exact_key(record: &Value, fields: &[String], normalize: Normalize) -> Result<[u8; 32]> {
    let mut parts = Vec::new();
    if fields.is_empty() {
        parts.push(
            record
                .get("generated")
                .ok_or_else(|| Error::DedupField {
                    field: "generated".into(),
                })?
                .clone(),
        );
    } else {
        for field in fields {
            parts.push(
                extract(record, field)
                    .ok_or_else(|| Error::DedupField {
                        field: field.clone(),
                    })?
                    .clone(),
            );
        }
    }
    if matches!(normalize, Normalize::Lowercase) {
        for part in &mut parts {
            lowercase(part);
        }
    }
    let canonical = serde_json::to_string(&parts)
        .map_err(|_| Error::Configuration("dedup key serialization failed".into()))?;
    Ok(blake3::hash(canonical.as_bytes()).into())
}

fn lowercase(value: &mut Value) {
    match value {
        Value::String(text) => *text = text.to_lowercase(),
        Value::Array(items) => items.iter_mut().for_each(lowercase),
        // Object keys identify distinct fields and must never be normalized:
        // lowercasing them can overwrite values when both "A" and "a" exist.
        Value::Object(map) => map.values_mut().for_each(lowercase),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(answer: &str) -> Value {
        json!({"topic": "t", "generated": {"answer": answer}})
    }

    #[test]
    fn exact_keys_cover_field_paths_and_normalization() {
        let config = DedupConfig::Exact {
            fields: Some(vec!["generated.answer".into()]),
            normalize: Normalize::Lowercase,
        };
        let mut state = DedupState::new(&config);
        assert!(
            state
                .check(&record("Rust Ownership"))
                .expect("first")
                .is_unique()
        );
        let key = state
            .check(&record("rust ownership"))
            .expect("second")
            .unique_key();
        state.register(key).unwrap();
        assert!(
            state
                .check(&record("different"))
                .expect("third")
                .is_unique()
        );
    }

    #[test]
    fn lowercase_preserves_distinct_object_keys_recursively() {
        for storage in [DedupStorage::Memory, DedupStorage::Disk] {
            let config = DedupConfig::Exact {
                fields: None,
                normalize: Normalize::Lowercase,
            };
            let mut state = DedupState::with_storage(&config, 8192, storage).unwrap();
            let original = json!({"generated": {"A": "ONE", "a": [ {"B": "TWO", "b": "THREE"} ]}});
            state.restore(&original).unwrap();
            assert!(
                state
                    .check(&json!({"generated": {"a": [{"b": "three"}]}}))
                    .unwrap()
                    .is_unique()
            );
            assert!(
                state
                    .check(&json!({"generated": {"A": "one", "a": [{"B": "two", "b": "three"}]}}))
                    .unwrap()
                    .is_duplicate()
            );
            assert_eq!(
                original["generated"]["A"], "ONE",
                "the source record is not mutated"
            );
            let collision = json!({"generated": {"A": 1, "a": 2}});
            state.restore(&collision).unwrap();
            assert!(
                state
                    .check(&json!({"generated": {"a": 2}}))
                    .unwrap()
                    .is_unique()
            );
        }
    }

    #[test]
    fn disk_index_exceeds_memory_budget_and_is_deleted_after_use() {
        for config in [
            DedupConfig::Exact {
                fields: None,
                normalize: Normalize::None,
            },
            DedupConfig::MinHash {
                field: "generated.answer".into(),
                num_perm: 128,
                bands: 16,
                threshold: 1.0,
                shingle_words: 3,
                tokenization: Tokenization::Words,
                shingle_chars: 3,
            },
        ] {
            let mut state = DedupState::with_storage(&config, 128, DedupStorage::Disk).unwrap();
            let path = state.disk.as_ref().unwrap()._file.path().to_owned();
            for index in 0..256 {
                state.restore(&record(&format!("entry{index}"))).unwrap();
            }
            assert!(state.check(&record("entry128")).unwrap().is_duplicate());
            assert!(state.check(&record("unseen")).unwrap().is_unique());
            assert_eq!(state.used, 0);
            match &state.index {
                DedupIndex::Exact { seen, .. } => assert!(seen.is_empty()),
                DedupIndex::MinHash(index) => {
                    assert!(index.signatures.is_empty());
                    assert!(index.buckets.is_empty());
                }
            }
            assert!(path.exists());
            drop(state);
            assert!(!path.exists());
        }
    }

    #[test]
    fn disk_index_errors_are_propagated() {
        let config = DedupConfig::Exact {
            fields: None,
            normalize: Normalize::None,
        };
        let mut state = DedupState::with_storage(&config, 128, DedupStorage::Disk).unwrap();
        let key = state.check(&record("a")).unwrap().unique_key();
        state
            .disk
            .as_ref()
            .unwrap()
            .connection
            .execute("DROP TABLE exact_keys", [])
            .unwrap();
        assert!(matches!(state.register(key), Err(Error::RunFailed(_))));
        assert!(matches!(
            state.check(&record("a")),
            Err(Error::RunFailed(_))
        ));
    }

    #[test]
    fn character_shingles_detect_near_duplicate_chinese_in_both_backends() {
        let config = DedupConfig::MinHash {
            field: "generated.answer".into(),
            num_perm: 128,
            bands: 16,
            threshold: 0.6,
            shingle_words: 3,
            tokenization: Tokenization::Characters,
            shingle_chars: 3,
        };
        let original = "所有权规则帮助程序在编译阶段发现内存安全问题并且避免数据竞争，学习这些规则需要结合实际的代码示例认真练习。";
        let variant = "所有权规则帮助程序在编译阶段发现内存安全问题并且避免数据竞争，学习这些规则需要结合实际的代码示例反复练习。";
        for storage in [DedupStorage::Memory, DedupStorage::Disk] {
            let mut state = DedupState::with_storage(&config, 8192, storage).unwrap();
            state.restore(&record(original)).unwrap();
            assert!(state.check(&record(variant)).unwrap().is_duplicate());
            assert!(
                state
                    .check(&record(
                        "今天北京下雨，明天我们去上海旅游，参观博物馆并且品尝当地美食。"
                    ))
                    .unwrap()
                    .is_unique()
            );
            let mut restored = DedupState::with_storage(&config, 8192, storage).unwrap();
            restored.restore(&record(original)).unwrap();
            assert!(restored.check(&record(variant)).unwrap().is_duplicate());
        }
        let mut word_state = minhash("generated.answer", 0.6);
        word_state.restore(&record(original)).unwrap();
        assert!(word_state.check(&record(variant)).unwrap().is_unique());
    }

    #[test]
    fn character_shingles_use_bounded_unicode_windows() {
        let mut actual = Vec::new();
        for_each_shingle(" 你 好🌎ＡB ", 3, Tokenization::Characters, |shingle| {
            actual.push(shingle.to_owned())
        });
        assert_eq!(actual, ["你好🌎", "好🌎ａ", "🌎ａb"]);
        let mut emitted = 0;
        for_each_shingle(
            &"汉".repeat(100_000),
            3,
            Tokenization::Characters,
            |shingle| {
                assert_eq!(shingle.chars().count(), 3);
                emitted += 1;
            },
        );
        assert_eq!(emitted, 99_998);
        let mut short = Vec::new();
        for text in ["", " ", "中", "中文"] {
            for_each_shingle(text, 3, Tokenization::Characters, |shingle| {
                short.push(shingle.to_owned())
            });
        }
        assert_eq!(short, ["", "", "中", "中文"]);
    }

    #[test]
    fn missing_fields_surface_dedup_diagnostics() {
        let config = DedupConfig::Exact {
            fields: Some(vec!["generated.missing".into()]),
            normalize: Normalize::None,
        };
        let state = DedupState::new(&config);
        assert!(matches!(
            state.check(&record("a")),
            Err(Error::DedupField { .. })
        ));
    }

    #[test]
    fn default_keys_cover_the_whole_generated_object() {
        let config = DedupConfig::Exact {
            fields: None,
            normalize: Normalize::None,
        };
        let mut state = DedupState::new(&config);
        let key = state.check(&record("a")).expect("first").unique_key();
        state.register(key).unwrap();
        assert!(state.check(&record("a")).expect("second").is_duplicate());
        // The input topic differs but the generated object is the dedup key.
        let other_topic = json!({"topic": "other", "generated": {"answer": "a"}});
        assert!(state.check(&other_topic).expect("third").is_duplicate());
    }

    impl DedupAdmission {
        fn is_duplicate(&self) -> bool {
            matches!(self, DedupAdmission::Duplicate)
        }
        fn is_unique(&self) -> bool {
            !self.is_duplicate()
        }
        fn unique_key(self) -> PreparedKey {
            match self {
                DedupAdmission::Unique(prepared) => prepared,
                DedupAdmission::Duplicate => panic!("expected a unique admission"),
            }
        }
    }

    fn minhash(field: &str, threshold: f64) -> DedupState {
        DedupState::new(&DedupConfig::MinHash {
            field: field.into(),
            num_perm: 128,
            bands: 16,
            threshold,
            shingle_words: 3,
            tokenization: Tokenization::Words,
            shingle_chars: 3,
        })
    }

    #[test]
    fn minhash_normalizes_case_and_whitespace_at_every_length() {
        for (a, b) in [
            (" RUST ", "rust"),
            ("Rust   Types", "rust types"),
            ("Rust\tTypes\nAnd Ownership", "rust types and ownership"),
            ("", "   "),
        ] {
            assert_eq!(shingles(a, 3), shingles(b, 3));
            let mut state = minhash("generated.answer", 0.8);
            let key = state.check(&record(a)).unwrap().unique_key();
            state.register(key).unwrap();
            assert!(state.check(&record(b)).unwrap().is_duplicate());
        }
    }

    #[test]
    fn budget_limits_new_entries_but_still_allows_duplicates_and_restore() {
        let config = DedupConfig::Exact {
            fields: None,
            normalize: Normalize::None,
        };
        let mut state = DedupState::with_budget(&config, 128);
        state.restore(&record("a")).unwrap();
        assert!(state.check(&record("a")).unwrap().is_duplicate());
        assert!(state.restore(&record("b")).is_err());
        assert_eq!(state.used, 128);
        let config = DedupConfig::MinHash {
            field: "generated.answer".into(),
            num_perm: 128,
            bands: 16,
            threshold: 0.8,
            shingle_words: 3,
            tokenization: Tokenization::Words,
            shingle_chars: 3,
        };
        assert!(
            DedupState::with_budget(&config, 128)
                .check(&record("a"))
                .is_err()
        );
    }

    #[test]
    fn identical_texts_are_minhash_duplicates() {
        let text = "the quick brown fox jumps over the lazy dog again and again";
        let mut state = minhash("generated.answer", 0.8);
        let prepared = state.check(&record(text)).expect("first").unique_key();
        state.register(prepared).unwrap();
        assert!(state.check(&record(text)).expect("second").is_duplicate());
    }

    #[test]
    fn disjoint_texts_are_not_minhash_duplicates() {
        let mut state = minhash("generated.answer", 0.8);
        for text in [
            "alpha beta gamma delta epsilon zeta eta theta iota kappa",
            "one two three four five six seven eight nine ten eleven",
        ] {
            let prepared = state.check(&record(text)).expect("unique").unique_key();
            state.register(prepared).unwrap();
        }
        assert!(
            !state
                .check(&record(
                    "a completely different sentence about rust modules"
                ))
                .expect("third")
                .is_duplicate()
        );
    }

    #[test]
    fn near_identical_texts_collide_at_a_high_threshold() {
        let mut state = minhash("generated.answer", 0.6);
        let base = "rust ownership moves values between owners with move semantics always";
        let variant = "rust ownership moves values between owners with move semantics usually";
        let prepared = state.check(&record(base)).expect("first").unique_key();
        state.register(prepared).unwrap();
        assert!(
            state
                .check(&record(variant))
                .expect("near duplicate")
                .is_duplicate()
        );
    }

    #[test]
    fn signature_similarity_tracks_word_overlap() {
        let state = minhash("generated.answer", 0.8);
        let a = "a b c d e f g h i j k l m n o p";
        let b = "a b c d e f g h i j k l m n o q";
        let c = "a b c d x y z w v u t s r q p o";
        let signature = |text| {
            if let DedupIndex::MinHash(index) = &state.index {
                index.signature(text)
            } else {
                unreachable!("minhash state")
            }
        };
        let close = similarity(&signature(a), &signature(b));
        let far = similarity(&signature(a), &signature(c));
        assert!(close > far, "close {close} far {far}");
        assert!(close > 0.7 && close < 1.0, "close {close}");
        assert!(far < 0.4, "far {far}");
    }

    #[test]
    fn non_string_fields_surface_dedup_diagnostics() {
        let state = minhash("generated.count", 0.8);
        let numeric = json!({"topic": "t", "generated": {"count": 3}});
        assert!(matches!(
            state.check(&numeric),
            Err(Error::DedupField { .. })
        ));
    }
}
