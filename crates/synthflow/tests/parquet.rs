use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};
use std::fs;
use synthflow::{Pipeline, RunStatus, run_async};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn fixture(dir: &TempDir, records: Value, batch_size: usize) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "parquet"},
        "source": {"type": "inline", "records": records},
        "providers": {"generator": {"type": "mock", "response": RESPONSE}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}, "extra": {"type": "string"}}}
        },
        "output": {"format": "parquet", "path": dir.path().join("result.parquet"), "batch_size": batch_size}
    }))
    .expect("test spec");
    spec.output.path = dir.path().join("result.parquet");
    spec
}

const RESPONSE: &str = r##"{"answer": {{ record.topic | tojson }}{% if record.extra is defined %}, "extra": {{ record.extra | tojson }}{% endif %}}"##;

fn read_rows(path: &std::path::Path) -> Vec<Value> {
    let file = fs::File::open(path).expect("parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader")
        .build()
        .expect("batches");
    let mut json = Vec::new();
    {
        let mut writer = arrow_json::writer::WriterBuilder::new()
            .build::<_, arrow_json::writer::LineDelimited>(&mut json);
        for batch in reader {
            writer.write(&batch.expect("batch")).expect("json");
        }
    }
    String::from_utf8(json)
        .expect("utf8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("row"))
        .collect()
}

#[tokio::test]
async fn parquet_run_publishes_a_readable_file() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([{"topic": "ownership"}, {"topic": "borrowing"}, {"topic": "traits"}]),
        2,
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 3);
    assert_eq!(report.status, RunStatus::Completed);
    let rows = read_rows(&spec.output.path);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["generated"]["answer"], "ownership");
    assert_eq!(rows[0]["_meta"]["source_position"], 1);
    assert!(rows.iter().all(|row| row["_meta"]["run_id"].is_string()));
    assert!(
        !spec
            .output
            .path
            .with_file_name("result.parquet.partial")
            .exists()
    );
}

#[tokio::test]
async fn later_records_with_new_fields_are_rejected_not_fatal() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "ownership"},
            {"topic": "borrowing", "extra": "late field"},
            {"topic": "traits"}
        ]),
        1024,
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 2);
    assert_eq!(report.statistics.rejected_records_total, 1);
    let dead = fs::read_to_string(
        spec.output
            .path
            .with_file_name("result.parquet.rejected.jsonl"),
    )
    .expect("dead letter");
    let line: Value = serde_json::from_str(dead.lines().next().expect("line")).expect("json");
    assert_eq!(line["diagnostic"]["category"], "sink_row");
}

#[tokio::test]
async fn empty_parquet_run_publishes_a_valid_empty_file() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, json!([]), 16);
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert!(read_rows(&spec.output.path).is_empty());
}
