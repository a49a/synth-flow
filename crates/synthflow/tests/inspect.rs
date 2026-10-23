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
    assert_eq!(weight.min, Some(serde_json::Number::from_f64(0.2).unwrap()));
    assert_eq!(weight.max, Some(serde_json::Number::from_f64(0.9).unwrap()));
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
    assert_eq!(column(&summary, "a").mean, Some(1.0));
    assert_eq!(column(&summary, "a").min, Some(1.into()));
    assert_eq!(column(&summary, "a").max, Some(1.into()));

    let nulls = dir.path().join("nulls.jsonl");
    std::fs::write(&nulls, "{\"a\":null,\"b\":\"text\"}\n{}\n").expect("write");
    let summary = summarize(&nulls).expect("summary");
    assert_eq!(column(&summary, "a").mean, None);
    assert_eq!(column(&summary, "a").null_count, 2);
    assert_eq!(column(&summary, "b").mean, None);
    assert_eq!(column(&summary, "b").null_count, 1);

    let other = dir.path().join("data.csv");
    assert!(summarize(&other).is_err());
    let missing = dir.path().join("missing.jsonl");
    assert!(summarize(&missing).is_err());
}

#[test]
fn inspect_bounds_rows_columns_and_retained_distinct_values() {
    use synthflow::inspect::{InspectLimits, summarize_jsonl_with_limits};
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data.jsonl");
    let limits = InspectLimits {
        max_row_bytes: 1024,
        statistics_bytes: 800,
        max_columns: 1,
    };
    std::fs::write(&path, "{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n").unwrap();
    let summary = summarize_jsonl_with_limits(&path, limits).unwrap();
    assert_eq!(column(&summary, "x").distinct, None);
    assert_eq!(column(&summary, "x").mean, Some(2.0));
    std::fs::write(&path, "{\"x\":1,\"y\":2}\n").unwrap();
    assert!(
        summarize_jsonl_with_limits(&path, limits)
            .unwrap_err()
            .to_string()
            .contains("budget")
    );
    std::fs::write(&path, format!("{{\"x\":\"{}\"}}\n", "a".repeat(1024))).unwrap();
    assert!(
        summarize_jsonl_with_limits(&path, limits)
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );
}

#[test]
fn numeric_extrema_preserve_integers_and_finite_means_do_not_overflow() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("numeric.jsonl");
    std::fs::write(&data, "{\"x\":1e308}\n{\"x\":1e308}\n").unwrap();
    let summary = summarize(&data).unwrap();
    assert_eq!(column(&summary, "x").mean, Some(1e308));
    std::fs::write(
        &data,
        "{\"x\":9007199254740992.0}\n{\"x\":9007199254740993}\n{\"x\":18446744073709551615}\n",
    )
    .unwrap();
    let summary = summarize(&data).unwrap();
    assert_eq!(
        column(&summary, "x").max.as_ref().unwrap().as_u64(),
        Some(u64::MAX)
    );
    assert_eq!(
        column(&summary, "x").min.as_ref().unwrap().as_f64(),
        Some(9007199254740992.0)
    );
    let json = serde_json::to_value(&summary).unwrap();
    assert_eq!(json["columns"][0]["max"], serde_json::json!(u64::MAX));
    std::fs::write(&data, "{\"x\":-1e308}\n{\"x\":1e308}\n").unwrap();
    assert_eq!(column(&summarize(&data).unwrap(), "x").mean, Some(0.0));
}
