use serde_json::json;
use synthflow::{Pipeline, inspect::summarize, run_async};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn column<'a>(
    summary: &'a synthflow::inspect::DatasetSummary,
    name: &str,
) -> &'a synthflow::inspect::ColumnStats {
    summary
        .columns
        .iter()
        .find(|column| column.name == name)
        .unwrap_or_else(|| panic!("column {name}"))
}

#[tokio::test]
async fn jsonl_summaries_cover_types_nulls_and_numeric_ranges() {
    let dir = TempDir::new().expect("tempdir");
    let data = dir.path().join("data.jsonl");
    let rows = [
        json!({"topic": "ownership", "weight": 0.9, "tags": ["a"]}),
        json!({"topic": "borrowing", "weight": 0.2}),
        json!({"topic": "ownership", "weight": 0.5, "tags": ["b"]}),
    ];
    let body: String = rows.iter().map(|row| format!("{row}\n")).collect();
    std::fs::write(&data, body).expect("write");
    let summary = summarize(&data).expect("summary");
    assert_eq!(summary.rows, 3);
    assert_eq!(summary.bytes, std::fs::metadata(&data).expect("meta").len());
    let topic = column(&summary, "topic");
    assert_eq!(topic.data_type, "string");
    assert_eq!(topic.count, 3);
    assert_eq!(topic.null_count, 0);
    assert_eq!(topic.distinct, Some(2));
    let weight = column(&summary, "weight");
    assert_eq!(weight.data_type, "number");
    assert_eq!(weight.min, Some(0.2));
    assert_eq!(weight.max, Some(0.9));
    assert!((weight.mean.expect("mean") - (0.9 + 0.2 + 0.5) / 3.0).abs() < 1e-9);
    let tags = column(&summary, "tags");
    assert_eq!(tags.data_type, "array");
    assert_eq!(tags.null_count, 1);
    assert!(tags.min.is_none());
}

#[tokio::test]
async fn parquet_outputs_can_be_inspected() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1, "dataset": {"name": "inspect"},
        "source": {"type": "inline", "records": [
            {"topic": "a", "quality": 0.9},
            {"topic": "b", "quality": 0.4}
        ]},
        "providers": {"generator": {"type": "mock", "response": "{\"answer\": {{ record.quality | tojson }}}"}},
        "generate": {
            "provider": "generator", "prompt": "p",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "number"}}}
        },
        "output": {"format": "parquet", "path": dir.path().join("out.parquet")}
    }))
    .expect("spec");
    spec.output.path = dir.path().join("out.parquet");
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded());
    let summary = summarize(&spec.output.path).expect("summary");
    assert_eq!(summary.rows, 2);
    let answer = column(&summary, "generated");
    assert_eq!(answer.data_type, "object");
    let meta = column(&summary, "_meta");
    assert_eq!(meta.count, 2);
}

#[test]
fn mixed_types_and_unsupported_extensions_are_reported() {
    let dir = TempDir::new().expect("tempdir");
    let data = dir.path().join("mixed.jsonl");
    std::fs::write(&data, "{\"a\": 1}\n{\"a\": \"one\"}\n").expect("write");
    let summary = summarize(&data).expect("summary");
    assert_eq!(column(&summary, "a").data_type, "mixed");

    let other = dir.path().join("data.csv");
    assert!(summarize(&other).is_err());
    let missing = dir.path().join("missing.jsonl");
    assert!(summarize(&missing).is_err());
}
