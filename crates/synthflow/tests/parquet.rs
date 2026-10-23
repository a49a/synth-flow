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

fn explicit_schema() -> Value {
    json!({
        "type": "object",
        "required": ["topic", "generated"],
        "properties": {
            "topic": {"type": "string"},
            "count": {"type": ["integer", "null"]},
            "tags": {"type": "array", "items": {"type": ["string", "null"]}},
            "details": {
                "type": ["object", "null"],
                "properties": {"label": {"type": "string"}},
                "required": ["label"]
            },
            "generated": {
                "type": "object",
                "required": ["answer"],
                "properties": {"answer": {"type": "string"}, "extra": {"type": "string"}}
            }
        }
    })
}

#[tokio::test]
async fn explicit_schema_handles_initial_null_empty_arrays_and_optional_nested_fields() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(
        &dir,
        json!([
            {"topic": "first", "count": null, "tags": [], "details": null},
            {"topic": "second", "count": 7, "tags": ["rust", null], "details": {"label": "later"}},
            {"topic": "third"}
        ]),
        2,
    );
    spec.output.schema = Some(explicit_schema());
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 3);
    assert_eq!(report.statistics.rejected_records_total, 0);
    let rows = read_rows(&spec.output.path);
    assert!(rows[0]["count"].is_null());
    assert_eq!(rows[0]["tags"], json!([]));
    assert_eq!(rows[1]["count"], 7);
    assert_eq!(rows[1]["tags"], json!(["rust", null]));
    assert_eq!(rows[1]["details"]["label"], "later");
    assert!(rows[2]["generated"]["extra"].is_null());
    assert_eq!(rows[2]["_meta"]["source_position"], 3);
}

#[tokio::test]
async fn explicit_schema_rejects_missing_required_and_invalid_optional_values_per_row() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(
        &dir,
        json!([
            {"topic": "valid"},
            {"topic": "null optional array", "tags": null},
            {"topic": "wrong item", "tags": [42]},
            {"topic": "missing nested required", "details": {}},
            {"topic": "new nested field", "details": {"label": "x", "extra": true}},
            {"topic": "lossy integer", "count": 1.5},
            {"topic": "integer overflow", "count": 18446744073709551615u64},
            {"topic": "last valid", "count": 3}
        ]),
        2,
    );
    spec.output.schema = Some(explicit_schema());
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 2);
    assert_eq!(report.statistics.rejected_records_total, 6);
    assert_eq!(read_rows(&spec.output.path).len(), 2);
}

#[tokio::test]
async fn inferred_integer_schema_stays_fixed_and_preserves_large_values() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "large", "n": 9007199254740993i64},
            {"topic": "fraction", "n": 1.5},
            {"topic": "last", "n": 2}
        ]),
        2,
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 2);
    assert_eq!(report.statistics.rejected_records_total, 1);
    let rows = read_rows(&spec.output.path);
    assert_eq!(rows[0]["n"].as_i64(), Some(9007199254740993));
    assert_eq!(rows[1]["n"], 2);
}

#[tokio::test]
async fn float_schema_accepts_exact_integer_promotions_and_rejects_rounding() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "float", "n": 1.5},
            {"topic": "inexact", "n": 9007199254740993i64},
            {"topic": "exact", "n": 9007199254740992i64}
        ]),
        2,
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 2);
    assert_eq!(report.statistics.rejected_records_total, 1);
    assert_eq!(read_rows(&spec.output.path)[1]["n"], 9007199254740992f64);
}

#[tokio::test]
async fn explicit_unsigned_integer_retains_full_u64_range() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, json!([{"topic": "id", "id": u64::MAX}]), 2);
    let mut schema = explicit_schema();
    schema["properties"]["id"] = json!({"type": "integer", "format": "uint64"});
    spec.output.schema = Some(schema);
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(
        read_rows(&spec.output.path)[0]["id"].as_u64(),
        Some(u64::MAX)
    );
}

#[tokio::test]
async fn empty_explicit_run_preserves_the_declared_parquet_schema() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, json!([]), 2);
    spec.output.schema = Some(explicit_schema());
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&spec.output.path).expect("file"))
            .expect("reader");
    let schema = builder.schema();
    assert!(schema.field_with_name("generated").is_ok());
    assert!(schema.field_with_name("_meta").is_ok());
    assert_eq!(
        schema.field_with_name("count").expect("count").data_type(),
        &arrow::datatypes::DataType::Int64
    );
}

#[test]
fn output_schema_rejects_unsupported_or_ambiguous_contracts() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, json!([]), 2);
    for schema in [
        json!({"type": "object", "properties": {"x": {"type": ["integer", "string"]}}}),
        json!({"type": "object", "properties": {"x": {"type": "array"}}}),
        json!({"type": "object", "properties": {"x": {"type": "string", "minLength": 1}}}),
        json!({"type": "object", "properties": {"_meta": {"type": "string"}}}),
        json!({"type": "object", "properties": {"x": {"type": "string"}}, "required": ["missing"]}),
        json!({"type": "object", "properties": {"x": {"type": "string"}}, "additionalProperties": true}),
    ] {
        spec.output.schema = Some(schema);
        assert!(spec.validate().is_err());
    }
    spec.output.schema = Some(explicit_schema());
    spec.output.format = synthflow::spec::OutputFormat::Jsonl;
    assert!(spec.validate().is_err());
}

struct StopAfterOne(std::sync::atomic::AtomicUsize);

#[async_trait::async_trait]
impl synthflow::provider::LlmProvider for StopAfterOne {
    async fn generate(
        &self,
        _request: synthflow::provider::GenerateRequest<'_>,
        cancellation: &CancellationToken,
    ) -> synthflow::Result<synthflow::provider::GenerateResponse> {
        if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
            cancellation.cancel();
            return Err(synthflow::Error::Cancelled);
        }
        Ok(synthflow::provider::GenerateResponse {
            text: json!({"answer": "first"}).to_string(),
            model: "test".into(),
            attempts: 1,
            prompt_tokens: None,
            completion_tokens: None,
        })
    }

    fn concurrency(&self) -> usize {
        1
    }
}

#[tokio::test]
async fn resume_retains_explicit_types_after_an_initial_null_and_empty_array() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(
        &dir,
        json!([
            {"topic": "first", "count": null, "tags": []},
            {"topic": "second", "count": 9, "tags": ["typed"]}
        ]),
        2,
    );
    spec.output.schema = Some(explicit_schema());
    let interrupted = synthflow::run_with_provider(
        &spec,
        std::sync::Arc::new(StopAfterOne(std::sync::atomic::AtomicUsize::new(0))),
        CancellationToken::new(),
    )
    .await
    .expect("interrupted");
    assert_eq!(interrupted.status, RunStatus::Cancelled);
    assert_eq!(interrupted.sink_state.committed_accepted_records, 1);
    let resumed = synthflow::resume_async(&spec, CancellationToken::new())
        .await
        .expect("resume");
    assert!(resumed.succeeded(), "{:?}", resumed.errors);
    let rows = read_rows(&spec.output.path);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["count"], 9);
    assert_eq!(rows[1]["tags"], json!(["typed"]));
}

#[tokio::test]
async fn rejected_first_row_does_not_fix_the_inferred_schema() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "lossy", "values": [9007199254740993i64, 1.5]},
            {"topic": "valid", "values": [1, 2], "later": "kept"}
        ]),
        2,
    );
    let report = run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 1);
    assert_eq!(report.statistics.rejected_records_total, 1);
    let rows = read_rows(&spec.output.path);
    assert_eq!(rows[0]["values"], json!([1, 2]));
    assert_eq!(rows[0]["later"], "kept");
}
