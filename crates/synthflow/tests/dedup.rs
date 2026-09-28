use serde_json::{Value, json};
use std::fs;
use synthflow::{Error, Pipeline, run_async};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

// {"answer": <record.answer as JSON>} — three closing braces: expression
// close, then the literal JSON object close.
const RESPONSE: &str = r##"{"answer": {{ record.answer | tojson }}}"##;

fn fixture(dir: &TempDir, records: Value, dedup: Value) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "dedup"},
        "source": {"type": "inline", "records": records},
        "providers": {"generator": {"type": "mock", "response": RESPONSE}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        },
        "dedup": dedup,
        "output": {"format": "jsonl", "path": "result.jsonl"}
    }))
    .expect("test spec");
    spec.output.path = dir.path().join("result.jsonl");
    spec
}

fn dead_letter(spec: &Pipeline) -> Vec<Value> {
    fs::read_to_string(
        spec.output
            .path
            .with_file_name("result.jsonl.rejected.jsonl"),
    )
    .expect("dead letter")
    .lines()
    .map(|line| serde_json::from_str(line).expect("JSON row"))
    .collect()
}

#[tokio::test]
async fn exact_duplicates_are_rejected_in_commit_order() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "ownership", "answer": "same answer"},
            {"topic": "borrowing", "answer": "same answer"},
            {"topic": "traits", "answer": "different answer"}
        ]),
        json!({"method": "exact"}),
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 2);
    assert_eq!(report.statistics.duplicate_records_total, 1);
    assert_eq!(report.statistics.rejected_records_total, 1);
    let rejected = &dead_letter(&spec)[0];
    assert_eq!(rejected["diagnostic"]["category"], "duplicate");
    assert_eq!(rejected["source_position"], 2);
}

#[tokio::test]
async fn field_extraction_and_normalization_control_the_key() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "Ownership", "answer": "one"},
            {"topic": "ownership", "answer": "totally different text"}
        ]),
        json!({"method": "exact", "fields": ["topic"], "normalize": "lowercase"}),
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert_eq!(report.statistics.accepted_records_total, 1);
    assert_eq!(report.statistics.duplicate_records_total, 1);
}

#[tokio::test]
async fn missing_configured_field_rejects_with_a_dedup_diagnostic() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([{"topic": "ownership", "answer": "one"}]),
        json!({"method": "exact", "fields": ["generated.absent"]}),
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(!report.succeeded());
    let rejected = &dead_letter(&spec)[0];
    assert_eq!(rejected["diagnostic"]["category"], "dedup_field");
    assert_eq!(
        report.errors.first().map(|e| e.category.as_str()),
        Some("failure_policy")
    );
}

#[test]
fn dedup_field_paths_are_validated() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([]),
        json!({"method": "exact", "fields": ["a..b", ""]}),
    );
    let error = spec.validate().expect_err("bad paths");
    assert!(
        matches!(error, Error::Configuration(ref message) if message.contains("dedup.fields")),
        "{error:?}"
    );
    let unknown: Result<Pipeline, _> = serde_json::from_value(json!({
        "version": 1, "dataset": {"name": "dedup"},
        "source": {"type": "inline", "records": []},
        "providers": {"generator": {"type": "mock", "response": RESPONSE}},
        "generate": {"provider": "generator", "prompt": "p", "output_schema": {"type": "object"}},
        "dedup": {"method": "unknown"},
        "output": {"format": "jsonl", "path": dir.path().join("result.jsonl")}
    }));
    assert!(unknown.is_err());
}
