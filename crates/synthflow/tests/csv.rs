use serde_json::{Value, json};
use std::fs;
use synthflow::{Pipeline, run};
use tempfile::TempDir;

fn fixture(dir: &TempDir, csv_path: std::path::PathBuf) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "csv"},
        "source": {"type": "csv", "path": csv_path},
        "providers": {"generator": {"type": "mock", "response": "{\"answer\": {{ record.topic | tojson }}}"}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        },
        "output": {"format": "jsonl", "path": dir.path().join("result.jsonl")}
    }))
    .expect("test spec");
    spec.source = synthflow::spec::SourceConfig::Csv { path: csv_path };
    spec
}

fn rows(spec: &Pipeline) -> Vec<Value> {
    fs::read_to_string(&spec.output.path)
        .expect("output")
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSON row"))
        .collect()
}

#[test]
fn csv_source_handles_quotes_and_crlf_and_keeps_strings() {
    let dir = TempDir::new().expect("tempdir");
    let csv_path = dir.path().join("topics.csv");
    fs::write(
        &csv_path,
        "topic,summary,weight\r\nownership,\"each value has one, exactly one, owner\",3\r\nborrowing,\"references \"\"borrow\"\" access\",2\r\n",
    )
    .expect("csv");
    let spec = fixture(&dir, csv_path);
    let stats = run(&spec).expect("run");
    assert_eq!(stats.accepted_records_total, 2);
    let rows = rows(&spec);
    assert_eq!(rows[0]["topic"], "ownership");
    assert_eq!(rows[0]["summary"], "each value has one, exactly one, owner");
    assert_eq!(rows[1]["summary"], "references \"borrow\" access");
    // CSV values stay strings; the weight column is untouched input data.
    assert_eq!(rows[0]["weight"], json!("3"));
    assert_eq!(rows[0]["_meta"]["source_position"], 1);
    assert_eq!(rows[1]["_meta"]["source_position"], 2);
}

#[test]
fn header_only_csv_is_an_empty_successful_run() {
    let dir = TempDir::new().expect("tempdir");
    let csv_path = dir.path().join("topics.csv");
    fs::write(&csv_path, "topic,summary\n").expect("csv");
    let spec = fixture(&dir, csv_path);
    let stats = run(&spec).expect("run");
    assert_eq!(stats.source_records_total, 0);
    assert_eq!(stats.accepted_records_total, 0);
}

#[test]
fn ragged_rows_fail_the_source() {
    let dir = TempDir::new().expect("tempdir");
    let csv_path = dir.path().join("topics.csv");
    fs::write(&csv_path, "topic,summary\nownership\n").expect("csv");
    let spec = fixture(&dir, csv_path);
    let error = run(&spec).expect_err("ragged");
    assert!(error.to_string().contains("CSV parse failed"));
}

#[test]
fn missing_csv_file_fails_validation() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, dir.path().join("absent.csv"));
    assert!(spec.validate().is_err());
}
