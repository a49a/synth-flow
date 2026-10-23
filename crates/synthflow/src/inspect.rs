//! Basic dataset statistics for published outputs.

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{self, BufRead, Read, Write},
    path::{Path, PathBuf},
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use serde_json::{Number, Value};
use std::cmp::Ordering;

use crate::{Error, Result};

/// Distinct counting stops growing past this many values per column.
const DISTINCT_CAP: usize = 1_000_000;

/// Retained statistics use conservative accounting, not a process RSS guarantee.
#[derive(Debug, Clone, Copy)]
pub struct InspectLimits {
    pub max_row_bytes: usize,
    pub statistics_bytes: usize,
    pub max_columns: usize,
}
impl Default for InspectLimits {
    fn default() -> Self {
        Self {
            max_row_bytes: 32 * 1024 * 1024,
            statistics_bytes: 64 * 1024 * 1024,
            max_columns: 4096,
        }
    }
}
const DISTINCT_ENTRY_BYTES: usize = 128;

#[derive(Debug, Clone, Serialize)]
pub struct ColumnStats {
    pub name: String,
    /// JSON type of non-null values, or "mixed" when they disagree.
    pub data_type: String,
    pub count: u64,
    pub null_count: u64,
    /// Numeric summary over the numeric values only; non-numeric values in a
    /// mixed column are excluded, never counted as zero. `None` when the
    /// column contains no numbers.
    pub min: Option<Number>,
    pub max: Option<Number>,
    pub mean: Option<f64>,
    pub distinct: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatasetSummary {
    pub path: PathBuf,
    pub rows: u64,
    pub bytes: u64,
    pub columns: Vec<ColumnStats>,
}

pub fn summarize(path: impl AsRef<Path>) -> Result<DatasetSummary> {
    let path = path.as_ref();
    match path.extension().and_then(|e| e.to_str()) {
        Some("parquet") => summarize_parquet(path),
        Some("jsonl") | Some("ndjson") => summarize_jsonl(path),
        _ => Err(Error::Configuration(
            "inspect supports .jsonl and .parquet files".into(),
        )),
    }
}

pub fn summarize_jsonl(path: &Path) -> Result<DatasetSummary> {
    summarize_jsonl_with_limits(path, InspectLimits::default())
}

pub fn summarize_jsonl_with_limits(path: &Path, limits: InspectLimits) -> Result<DatasetSummary> {
    let file = fs::File::open(path).map_err(|e| Error::io(path, e))?;
    let bytes = file.metadata().map_err(|e| Error::io(path, e))?.len();
    let mut accumulator = Accumulator::new(limits);
    let mut reader = io::BufReader::new(file);
    let mut line = Vec::new();
    loop {
        line.clear();
        let n = reader
            .by_ref()
            .take(limits.max_row_bytes.saturating_add(1) as u64)
            .read_until(b'\n', &mut line)
            .map_err(|e| Error::io(path, e))?;
        if n == 0 {
            break;
        }
        if n > limits.max_row_bytes {
            return Err(Error::Configuration(
                "inspect row exceeds byte limit".into(),
            ));
        }
        let value: Value = serde_json::from_slice(&line)
            .map_err(|_| Error::Configuration(format!("invalid JSON row in {}", path.display())))?;
        accumulator.record(&value)?;
    }
    Ok(accumulator.finish(path, bytes))
}

pub fn summarize_parquet(path: &Path) -> Result<DatasetSummary> {
    let file = fs::File::open(path).map_err(|e| Error::io(path, e))?;
    let bytes = file.metadata().map_err(|e| Error::io(path, e))?.len();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Configuration(format!("cannot open parquet: {e}")))?
        .with_batch_size(1)
        .build()
        .map_err(|e| Error::Configuration(format!("cannot read parquet: {e}")))?;
    let mut accumulator = Accumulator::new(InspectLimits::default());
    for batch in reader {
        let batch = batch.map_err(|e| Error::Configuration(format!("parquet read failed: {e}")))?;
        let mut json = CappedBuffer {
            data: Vec::new(),
            limit: InspectLimits::default().max_row_bytes,
        };
        let mut writer = arrow_json::writer::WriterBuilder::new()
            .build::<_, arrow_json::writer::LineDelimited>(&mut json);
        writer
            .write(&batch)
            .map_err(|e| Error::Configuration(format!("parquet decode failed: {e}")))?;
        drop(writer);
        for line in String::from_utf8_lossy(&json.data).lines() {
            let value: Value = serde_json::from_str(line)
                .map_err(|_| Error::Configuration("parquet row decode failed".into()))?;
            accumulator.record(&value)?;
        }
    }
    Ok(accumulator.finish(path, bytes))
}

struct CappedBuffer {
    data: Vec<u8>,
    limit: usize,
}
impl Write for CappedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.data.len()) {
            return Err(io::Error::other("inspect row exceeds byte limit"));
        }
        self.data.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Accumulator {
    remaining: usize,
    max_columns: usize,
    rows: u64,
    columns: BTreeMap<String, ColumnAccumulator>,
}

impl Accumulator {
    fn new(limits: InspectLimits) -> Self {
        Self {
            rows: 0,
            columns: BTreeMap::new(),
            remaining: limits.statistics_bytes,
            max_columns: limits.max_columns,
        }
    }
    fn record(&mut self, value: &Value) -> Result<()> {
        self.rows += 1;
        if let Some(map) = value.as_object() {
            for (name, field) in map {
                if !self.columns.contains_key(name) {
                    let charge = name.len().saturating_add(512);
                    if self.columns.len() >= self.max_columns || charge > self.remaining {
                        return Err(Error::Configuration(
                            "inspect column statistics budget exceeded".into(),
                        ));
                    }
                    self.remaining -= charge;
                    self.columns
                        .insert(name.clone(), ColumnAccumulator::default());
                }
                self.columns
                    .get_mut(name)
                    .unwrap()
                    .observe(field, &mut self.remaining);
            }
        }
        Ok(())
    }

    fn finish(self, path: &Path, bytes: u64) -> DatasetSummary {
        DatasetSummary {
            path: path.to_owned(),
            rows: self.rows,
            bytes,
            columns: self
                .columns
                .into_iter()
                .map(|(name, column)| ColumnStats {
                    name,
                    data_type: column.data_type(),
                    count: column.count,
                    // Absent keys are nulls, not silently skipped rows.
                    null_count: self.rows.saturating_sub(column.count),
                    min: column.min,
                    max: column.max,
                    mean: (column.numeric_count > 0).then_some(column.mean),
                    distinct: (!column.distinct_capped).then_some(column.distinct.len() as u64),
                })
                .collect(),
        }
    }
}

#[derive(Default)]
struct ColumnAccumulator {
    types: HashSet<&'static str>,
    count: u64,
    numeric_count: u64,
    min: Option<Number>,
    max: Option<Number>,
    mean: f64,
    distinct: HashSet<[u8; 32]>,
    distinct_capped: bool,
}

impl ColumnAccumulator {
    fn observe(&mut self, value: &Value, remaining: &mut usize) {
        if value.is_null() {
            return;
        }
        self.count += 1;
        self.types.insert(type_name(value));
        if let Value::Number(number) = value {
            self.numeric_count += 1;
            if self
                .min
                .as_ref()
                .is_none_or(|min| compare_numbers(number, min).is_lt())
            {
                self.min = Some(number.clone());
            }
            if self
                .max
                .as_ref()
                .is_none_or(|max| compare_numbers(number, max).is_gt())
            {
                self.max = Some(number.clone());
            }
            let value = number.as_f64().unwrap_or(0.0);
            let n = self.numeric_count as f64;
            // Same-sign differences cannot overflow. Opposite signs use
            // weighted terms to avoid overflowing value - mean.
            self.mean = if value.is_sign_negative() == self.mean.is_sign_negative() {
                self.mean + (value - self.mean) / n
            } else {
                self.mean * ((n - 1.0) / n) + value / n
            };
        }
        if !self.distinct_capped {
            // Stream canonical JSON into a digest without retaining long values.
            let mut hasher = blake3::Hasher::new();
            if serde_json::to_writer(&mut hasher, value).is_ok() {
                let key = *hasher.finalize().as_bytes();
                if !self.distinct.contains(&key) {
                    if self.distinct.len() >= DISTINCT_CAP || *remaining < DISTINCT_ENTRY_BYTES {
                        self.distinct_capped = true;
                        *remaining += self.distinct.len() * DISTINCT_ENTRY_BYTES;
                        self.distinct = HashSet::new();
                    } else {
                        self.distinct.insert(key);
                        *remaining -= DISTINCT_ENTRY_BYTES;
                    }
                }
            }
        }
    }

    fn data_type(&self) -> String {
        match self.types.len() {
            0 => "empty".into(),
            1 => self.types.iter().next().copied().unwrap_or("empty").into(),
            _ => "mixed".into(),
        }
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::Null => "null",
    }
}

// Compare integer extrema without rounding them through f64, including mixed
// integer/float columns at the 2^53 and u64::MAX boundaries.
fn compare_numbers(a: &Number, b: &Number) -> Ordering {
    fn integer(value: &Number) -> Option<i128> {
        value
            .as_i64()
            .map(i128::from)
            .or_else(|| value.as_u64().map(i128::from))
    }
    fn integer_float(integer: i128, float: f64) -> Ordering {
        if float < i64::MIN as f64 {
            return Ordering::Greater;
        }
        if float >= u64::MAX as f64 {
            return Ordering::Less;
        }
        match integer.cmp(&(float as i128)) {
            Ordering::Equal => 0.0_f64
                .partial_cmp(&float.fract())
                .unwrap_or(Ordering::Equal),
            other => other,
        }
    }
    match (integer(a), integer(b)) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(a), None) => integer_float(a, b.as_f64().unwrap_or(0.0)),
        (None, Some(b)) => integer_float(b, a.as_f64().unwrap_or(0.0)).reverse(),
        (None, None) => a
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&b.as_f64().unwrap_or(0.0))
            .unwrap_or(Ordering::Equal),
    }
}
