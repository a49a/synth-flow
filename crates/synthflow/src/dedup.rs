//! Duplicate detection over accepted records.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::{
    Error, Result,
    spec::{DedupConfig, Normalize},
};

pub(crate) enum DedupState {
    Exact {
        seen: HashSet<[u8; 32]>,
        fields: Vec<String>,
        normalize: Normalize,
    },
    MinHash(MinHashIndex),
}

impl DedupState {
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
            } => Self::MinHash(MinHashIndex::new(
                field.clone(),
                *num_perm as usize,
                *bands as usize,
                *threshold,
                *shingle_words,
            )),
        }
    }

    /// Register a record; `Ok(true)` means it duplicates an earlier one.
    pub(crate) fn insert(&mut self, record: &Value) -> Result<bool> {
        match self {
            Self::Exact {
                seen,
                fields,
                normalize,
            } => {
                let key = exact_key(record, fields, *normalize)?;
                Ok(!seen.insert(key))
            }
            Self::MinHash(index) => index.insert(record),
        }
    }

    /// Rebuild state from previously accepted records during resume.
    #[allow(dead_code)] // consumed by the resume implementation
    pub(crate) fn restore(&mut self, record: &Value) -> Result<()> {
        self.insert(record).map(|_| ())
    }
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
    ) -> Self {
        Self {
            field,
            num_perm,
            rows: num_perm / bands,
            bands,
            threshold,
            shingle_words,
            signatures: Vec::new(),
            buckets: HashMap::new(),
        }
    }

    fn insert(&mut self, record: &Value) -> Result<bool> {
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
                return Ok(true);
            }
        }
        let index = self.signatures.len() as u32;
        for band in 0..self.bands {
            let key = (
                band,
                band_hash(&signature[band * self.rows..(band + 1) * self.rows]),
            );
            self.buckets.entry(key).or_default().push(index);
        }
        self.signatures.push(signature);
        Ok(false)
    }

    fn signature(&self, text: &str) -> Vec<u64> {
        let mut signature = vec![u64::MAX; self.num_perm];
        for shingle in shingles(text, self.shingle_words) {
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
        }
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

/// Word n-grams joined with a separator; short texts fall back to a single
/// shingle so empty or one-word values still dedup deterministically.
fn shingles(text: &str, n: usize) -> Vec<String> {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < n {
        return vec![text.trim().to_lowercase()];
    }
    words
        .windows(n)
        .map(|window| window.join("\u{1f}"))
        .collect()
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
        Value::Object(map) => {
            let lowered: Vec<(String, Value)> = map
                .iter_mut()
                .map(|(key, value)| {
                    lowercase(value);
                    (key.to_lowercase(), value.take())
                })
                .collect();
            *map = lowered.into_iter().collect();
        }
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
        assert!(!state.insert(&record("Rust Ownership")).expect("first"));
        assert!(state.insert(&record("rust ownership")).expect("second"));
        assert!(!state.insert(&record("different")).expect("third"));
    }

    #[test]
    fn missing_fields_surface_dedup_diagnostics() {
        let config = DedupConfig::Exact {
            fields: Some(vec!["generated.missing".into()]),
            normalize: Normalize::None,
        };
        let mut state = DedupState::new(&config);
        assert!(matches!(
            state.insert(&record("a")),
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
        assert!(!state.insert(&record("a")).expect("first"));
        assert!(state.insert(&record("a")).expect("second"));
        // The input topic differs but the generated object is the dedup key.
        let other_topic = json!({"topic": "other", "generated": {"answer": "a"}});
        assert!(state.insert(&other_topic).expect("third"));
    }

    fn minhash(field: &str, threshold: f64) -> DedupState {
        DedupState::new(&DedupConfig::MinHash {
            field: field.into(),
            num_perm: 128,
            bands: 16,
            threshold,
            shingle_words: 3,
        })
    }

    #[test]
    fn identical_texts_are_minhash_duplicates() {
        let text = "the quick brown fox jumps over the lazy dog again and again";
        let mut state = minhash("generated.answer", 0.8);
        assert!(!state.insert(&record(text)).expect("first"));
        assert!(state.insert(&record(text)).expect("second"));
    }

    #[test]
    fn disjoint_texts_are_not_minhash_duplicates() {
        let mut state = minhash("generated.answer", 0.8);
        assert!(
            !state
                .insert(&record(
                    "alpha beta gamma delta epsilon zeta eta theta iota kappa"
                ))
                .expect("first")
        );
        assert!(
            !state
                .insert(&record(
                    "one two three four five six seven eight nine ten eleven"
                ))
                .expect("second")
        );
    }

    #[test]
    fn near_identical_texts_collide_at_a_high_threshold() {
        let mut state = minhash("generated.answer", 0.6);
        let base = "rust ownership moves values between owners with move semantics always";
        let variant = "rust ownership moves values between owners with move semantics usually";
        assert!(!state.insert(&record(base)).expect("first"));
        assert!(state.insert(&record(variant)).expect("near duplicate"));
    }

    #[test]
    fn signature_similarity_tracks_word_overlap() {
        let state = minhash("generated.answer", 0.8);
        let a = "a b c d e f g h i j k l m n o p";
        let b = "a b c d e f g h i j k l m n o q";
        let c = "a b c d x y z w v u t s r q p o";
        let signature = |text| {
            if let DedupState::MinHash(index) = &state {
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
        let mut state = minhash("generated.count", 0.8);
        let numeric = json!({"topic": "t", "generated": {"count": 3}});
        assert!(matches!(
            state.insert(&numeric),
            Err(Error::DedupField { .. })
        ));
    }
}
