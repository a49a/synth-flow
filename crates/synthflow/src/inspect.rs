//! Basic dataset statistics for published outputs.

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{self, BufRead},
    path::{Path, PathBuf},
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use serde_json::Value;

use crate::{Error, Result};

/// Distinct counting stops growing past this many values per column.
const DISTINCT_CAP: usize = 1_000_000;

#[derive(Debug, Clone, Serialize)]
pub struct ColumnStats {
    pub name: String,
    /// JSON type of non-null values, or "mixed" when they disagree.
    pub data_type: String,
    pub count: u64,
    pub null_count: u64,
    pub min: Option<f64>,
    pub max: Option<f64>,
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
    let file = fs::File::open(path).map_err(|e| Error::io(path, e))?;
    let bytes = file.metadata().map_err(|e| Error::io(path, e))?.len();
    let mut accumulator = Accumulator::default();
    for line in io::BufReader::new(file).lines() {
        let line = line.map_err(|e| Error::io(path, e))?;
        let value: Value = serde_json::from_str(&line)
            .map_err(|_| Error::Configuration(format!("invalid JSON row in {}", path.display())))?;
        accumulator.record(&value);
    }
    Ok(accumulator.finish(path, bytes))
}

pub fn summarize_parquet(path: &Path) -> Result<DatasetSummary> {
    let file = fs::File::open(path).map_err(|e| Error::io(path, e))?;
    let bytes = file.metadata().map_err(|e| Error::io(path, e))?.len();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Configuration(format!("cannot open parquet: {e}")))?
        .build()
        .map_err(|e| Error::Configuration(format!("cannot read parquet: {e}")))?;
    let mut accumulator = Accumulator::default();
    for batch in reader {
        let batch = batch.map_err(|e| Error::Configuration(format!("parquet read failed: {e}")))?;
        let mut json = Vec::new();
        let mut writer = arrow_json::writer::WriterBuilder::new()
            .build::<_, arrow_json::writer::LineDelimited>(&mut json);
        writer
            .write(&batch)
            .map_err(|e| Error::Configuration(format!("parquet decode failed: {e}")))?;
        drop(writer);
        for line in String::from_utf8_lossy(&json).lines() {
            let value: Value = serde_json::from_str(line)
                .map_err(|_| Error::Configuration("parquet row decode failed".into()))?;
            accumulator.record(&value);
        }
    }
    Ok(accumulator.finish(path, bytes))
}

#[derive(Default)]
struct Accumulator {
    rows: u64,
    columns: BTreeMap<String, ColumnAccumulator>,
}

impl Accumulator {
    fn record(&mut self, value: &Value) {
        self.rows += 1;
        if let Some(map) = value.as_object() {
            for (name, field) in map {
                self.columns.entry(name.clone()).or_default().observe(field);
            }
        }
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
                    mean: (column.min.is_some()).then(|| column.sum / column.count as f64),
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
    min: Option<f64>,
    max: Option<f64>,
    sum: f64,
    distinct: HashSet<String>,
    distinct_capped: bool,
}

impl ColumnAccumulator {
    fn observe(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        self.count += 1;
        self.types.insert(type_name(value));
        if let Some(number) = value.as_f64() {
            self.min = Some(self.min.map_or(number, |min| min.min(number)));
            self.max = Some(self.max.map_or(number, |max| max.max(number)));
            self.sum += number;
        }
        if !self.distinct_capped {
            if self.distinct.len() >= DISTINCT_CAP {
                self.distinct_capped = true;
                self.distinct.clear();
            } else if let Ok(canonical) = serde_json::to_string(value) {
                self.distinct.insert(canonical);
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
