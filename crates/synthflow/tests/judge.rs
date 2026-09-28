use serde_json::{Value, json};
use std::fs;
use synthflow::{Error, Pipeline, run_async};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn fixture(dir: &TempDir, judge: Value) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "judge"},
        "source": {"type": "inline", "records": [
            {"topic": "ownership", "quality": 0.9},
            {"topic": "borrowing", "quality": 0.2}
        ]},
        "providers": {
            "generator": {"type": "mock", "response": "{\"answer\": {{ record.topic | tojson }}, \"quality\": {{ record.quality }}}"},
            "reviewer": {"type": "mock", "response": "{\"score\": {{ generated.quality }}}"}
        },
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}, "quality": {"type": "number"}}}
        },
        "judge": judge,
        "output": {"format": "jsonl", "path": "result.jsonl"}
    }))
    .expect("test spec");
    spec.output.path = dir.path().join("result.jsonl");
    spec
}

fn rows(spec: &Pipeline) -> Vec<Value> {
    fs::read_to_string(&spec.output.path)
        .expect("output")
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSON row"))
        .collect()
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
async fn judge_scores_generated_records_and_appends_evidence() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!({
            "provider": "reviewer", "prompt": "Rate {{ generated.answer }}", "min_score": 0.5
        }),
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.judge_requests_total, 2);
    assert_eq!(report.statistics.judge_rejected_total, 1);
    assert_eq!(report.statistics.accepted_records_total, 1);
    let row = &rows(&spec)[0];
    assert_eq!(row["judge"]["provider"], "reviewer");
    assert_eq!(row["judge"]["score"], 0.9);
    assert_eq!(row["_meta"]["judge_score"], 0.9);
    assert_eq!(row["_meta"]["judge_model"], "synthflow-mock-v1");
    let rejected = &dead_letter(&spec)[0];
    assert_eq!(rejected["diagnostic"]["category"], "judge_score");
    assert!(
        rejected["diagnostic"]["message"]
            .as_str()
            .expect("message")
            .contains("0.2")
    );
}

#[tokio::test]
async fn judge_output_that_cannot_be_scored_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(
        &dir,
        json!({
            "provider": "reviewer", "prompt": "Rate {{ generated.answer }}", "min_score": 0.5
        }),
    );
    spec.providers.insert(
        "reviewer".into(),
        serde_json::from_value(json!({
            "type": "mock", "response": "{\"verdict\": \"fine\"}"
        }))
        .expect("provider"),
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(!report.succeeded());
    assert_eq!(report.statistics.judge_failed_total, 2);
    let rejected = &dead_letter(&spec)[0];
    assert_eq!(rejected["diagnostic"]["category"], "judge_output");
}

#[tokio::test]
async fn custom_score_fields_are_honoured() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(
        &dir,
        json!({
            "provider": "reviewer", "prompt": "Rate", "score_field": "rating", "min_score": 0.5
        }),
    );
    spec.providers.insert(
        "reviewer".into(),
        serde_json::from_value(json!({
            "type": "mock", "response": "{\"rating\": {{ generated.quality }}}"
        }))
        .expect("provider"),
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert_eq!(report.statistics.accepted_records_total, 1);
    assert_eq!(rows(&spec)[0]["judge"]["score"], 0.9);
}

#[test]
fn judge_configuration_is_validated() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!({"provider": "missing", "prompt": "p", "min_score": 0.5}),
    );
    let error = spec.validate().expect_err("unknown provider");
    assert!(matches!(error, Error::Configuration(_)));
    let mut spec = fixture(
        &dir,
        json!({"provider": "reviewer", "prompt": "p", "min_score": 0.5}),
    );
    spec.judge.as_mut().expect("judge").min_score = f64::NAN;
    assert!(spec.validate().is_err());
    let spec = fixture(
        &dir,
        json!({"provider": "reviewer", "prompt": "{% if", "min_score": 0.5}),
    );
    assert!(spec.validate().is_err());
}
