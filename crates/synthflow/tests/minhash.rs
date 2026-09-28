use serde_json::{Value, json};
use std::fs;
use synthflow::{Pipeline, run_async};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const RESPONSE: &str = r##"{"answer": {{ record.answer | tojson }}}"##;

fn fixture(dir: &TempDir, records: Value, threshold: f64) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "minhash"},
        "source": {"type": "inline", "records": records},
        "providers": {"generator": {"type": "mock", "response": RESPONSE}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        },
        "dedup": {"method": "minhash", "field": "generated.answer", "threshold": threshold},
        "output": {"format": "jsonl", "path": "result.jsonl"}
    }))
    .expect("test spec");
    spec.output.path = dir.path().join("result.jsonl");
    spec
}

#[tokio::test]
async fn near_duplicate_answers_are_deduplicated_end_to_end() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "ownership", "answer": "rust ownership moves values between owners with move semantics always"},
            {"topic": "borrowing", "answer": "rust ownership moves values between owners with move semantics usually"},
            {"topic": "traits", "answer": "traits define shared behavior that many different types can implement"}
        ]),
        0.6,
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 2);
    assert_eq!(report.statistics.duplicate_records_total, 1);
    let output = fs::read_to_string(&spec.output.path).expect("output");
    let topics: Vec<String> = output
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("row"))
        .map(|row| row["topic"].as_str().expect("topic").to_owned())
        .collect();
    assert_eq!(topics, ["ownership", "traits"]);
    let dead = fs::read_to_string(
        spec.output
            .path
            .with_file_name("result.jsonl.rejected.jsonl"),
    )
    .expect("dead letter");
    let rejected: Value = serde_json::from_str(dead.lines().next().expect("line")).expect("json");
    assert_eq!(rejected["diagnostic"]["category"], "duplicate");
    assert_eq!(rejected["source_position"], 2);
}

#[test]
fn minhash_parameters_are_validated() {
    let dir = TempDir::new().expect("tempdir");
    for bad in [
        json!({"method": "minhash", "field": "generated.answer", "num_perm": 100, "bands": 16}),
        json!({"method": "minhash", "field": "generated.answer", "bands": 0}),
        json!({"method": "minhash", "field": "generated.answer", "threshold": 1.5}),
        json!({"method": "minhash", "field": "", "threshold": 0.8}),
        json!({"method": "minhash", "field": "generated.answer", "shingle_words": 0}),
    ] {
        let mut spec: Pipeline = serde_json::from_value(json!({
            "version": 1, "dataset": {"name": "minhash"},
            "source": {"type": "inline", "records": []},
            "providers": {"generator": {"type": "mock", "response": RESPONSE}},
            "generate": {"provider": "generator", "prompt": "p", "output_schema": {"type": "object"}},
            "dedup": bad,
            "output": {"format": "jsonl", "path": dir.path().join("result.jsonl")}
        }))
        .expect("spec");
        spec.output.path = dir.path().join("result.jsonl");
        assert!(spec.validate().is_err(), "expected rejection");
    }
}
