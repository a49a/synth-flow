use serde_json::{Value, json};
use std::fs;
use synthflow::{
    Error, Pipeline,
    record::record_id,
    run,
    spec::{ProviderConfig, SourceConfig},
    template,
};
use tempfile::TempDir;

fn fixture(dir: &TempDir, records: Value, response: &str) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "test"},
        "source": {"type": "inline", "records": records},
        "providers": {"generator": {"type": "mock", "response": response}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        },
        "output": {"format": "jsonl", "path": "result.jsonl"}
    })).expect("test spec");
    spec.output.path = dir.path().join("result.jsonl");
    spec
}

fn output(spec: &Pipeline) -> Vec<Value> {
    fs::read_to_string(&spec.output.path)
        .expect("output")
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSON row"))
        .collect()
}

#[test]
fn jsonl_end_to_end_is_deterministic_and_preserves_lineage() {
    let dir = TempDir::new().expect("tempdir");
    let input = dir.path().join("input.jsonl");
    fs::write(
        &input,
        "{\"topic\":\"ownership\"}\r\n{\"topic\":\"中文 \\\"quotes\\\"\"}",
    )
    .expect("input");
    let mut spec = fixture(&dir, json!([]), r#"{"answer": {{ prompt | tojson }}}"#);
    spec.source = SourceConfig::Jsonl { path: input };
    let stats = run(&spec).expect("run");
    assert_eq!(stats.source_records_total, 2);
    assert_eq!(stats.accepted_records_total, 2);
    assert_eq!(stats.rejected_records_total, 0);
    let rows = output(&spec);
    assert_eq!(rows[0]["generated"]["answer"], "Explain ownership");
    assert_eq!(rows[1]["generated"]["answer"], "Explain 中文 \"quotes\"");
    assert_eq!(rows[0]["_meta"]["source_position"], 1);
    assert_eq!(rows[0]["_meta"]["generator_model"], "synthflow-mock-v1");
    let original = fs::read(&spec.output.path).expect("read");
    assert!(
        run(&spec).is_err(),
        "existing output must not be overwritten"
    );
    assert_eq!(fs::read(&spec.output.path).expect("read"), original);
    for path in [
        &spec.output.path,
        &synthflow::spec::suffix(&spec.output.path, ".manifest.json"),
        &spec.dead_letter_path().expect("dead path"),
    ] {
        fs::remove_file(path).expect("remove generated fixture");
    }
    run(&spec).expect("repeat run");
    let mut repeated = output(&spec);
    let mut original_rows = rows;
    for row in original_rows.iter_mut().chain(repeated.iter_mut()) {
        row["_meta"].as_object_mut().expect("meta").remove("run_id");
    }
    assert_eq!(repeated, original_rows);
}

#[test]
fn failures_are_counted_and_valid_records_continue() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "ok", "response": "{\"answer\":\"yes\"}"},
            {"topic": "bad_json", "response": "not JSON"},
            {"topic": "bad_schema", "response": "{\"answer\":42}"},
            {"response": "{\"answer\":\"missing topic\"}"},
            {"topic": "array", "response": "[]"},
            {"topic": "ok_again", "response": "{\"answer\":\"fine\"}"}
        ]),
        "{{ record.response }}",
    );
    let stats = run(&spec).expect("run");
    assert_eq!(stats.source_records_total, 6);
    assert_eq!(stats.generation_requests_total, 5);
    assert_eq!(stats.generation_success_total, 5);
    assert_eq!(stats.template_failed_total, 1);
    assert_eq!(stats.structured_output_failed_total, 2);
    assert_eq!(stats.validation_failed_total, 1);
    assert_eq!(stats.rejected_records_total, 4);
    assert_eq!(stats.accepted_records_total, 2);
    assert_eq!(output(&spec).len(), 2);
}

#[test]
fn invalid_mock_template_context_is_a_provider_failure() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, json!([{"topic": "rust"}]), "{{ record.missing }}");
    assert!(run(&spec).is_err());
    let report: synthflow::RunReport = serde_json::from_slice(
        &fs::read(synthflow::spec::suffix(&spec.output.path, ".manifest.json")).expect("manifest"),
    )
    .expect("report");
    assert_eq!(report.statistics.generation_failed_total, 1);
    assert_eq!(report.statistics.rejected_records_total, 1);
}

#[test]
fn malformed_source_is_fatal_and_flushes_accepted_prefix() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("input.jsonl");
    fs::write(&path, "{\"topic\":\"rust\"}\ninvalid\n").expect("input");
    let mut spec = fixture(&dir, json!([]), r#"{"answer":"ok"}"#);
    spec.source = SourceConfig::Jsonl { path };
    assert!(matches!(run(&spec), Err(Error::RunFailed(_))));
    assert!(!spec.output.path.exists());
    assert_eq!(
        fs::read_to_string(synthflow::spec::suffix(&spec.output.path, ".partial"))
            .expect("partial")
            .lines()
            .count(),
        1
    );
}

#[test]
fn empty_source_writes_empty_output() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, json!([]), "{}");
    let stats = run(&spec).expect("run");
    assert_eq!(stats.source_records_total, 0);
    assert_eq!(fs::metadata(&spec.output.path).expect("output").len(), 0);
}

#[test]
fn schema_supports_nested_contracts() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, json!([]), "{}");
    spec.generate.output_schema = json!({
        "type":"object", "required":["items", "flag", "nothing"],
        "properties": {
            "items": {"type":"array", "items":{"type":"integer"}},
            "flag": {"type":"boolean"}, "nothing":{"type":"null"},
            "difficulty":{"enum":["easy", "hard"]}, "score":{"type":"number"}
        }
    });
    let schema = spec.compile_schema().expect("schema");
    assert!(schema.is_valid(
        &json!({"items":[1,2], "flag":true, "nothing":null, "difficulty":"hard", "score":0.9})
    ));
    assert!(!schema.is_valid(&json!({"items":[1.5], "flag":true, "nothing":null})));
    assert!(!schema.is_valid(&json!({"items":[], "flag":true})));
    assert!(
        !schema.is_valid(&json!({"items":[], "flag":true, "nothing":null, "difficulty":"unknown"}))
    );
}

#[test]
fn validation_rejects_bad_configuration_before_creating_output() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, json!([]), "{}");
    spec.version = 2;
    assert!(run(&spec).is_err());
    spec.version = 1;
    spec.generate.provider = "missing".into();
    assert!(spec.validate().is_err());
    spec.generate.provider = "generator".into();
    spec.generate.prompt = "{{ unclosed".into();
    assert!(spec.validate().is_err());
    spec.generate.prompt = "ok".into();
    spec.generate.output_schema = json!({"type":"imaginary"});
    assert!(spec.validate().is_err());
    spec.generate.output_schema = json!({"$ref":"https://example.invalid/schema"});
    assert!(spec.validate().is_err());
    assert!(!spec.output.path.exists());
}

#[test]
fn input_objects_and_reserved_fields_are_enforced() {
    let dir = TempDir::new().expect("tempdir");
    for value in [
        json!(12),
        json!([]),
        json!({"generated": {}}),
        json!({"_meta": {}}),
        json!({"judge": {}}),
    ] {
        let spec = fixture(&dir, json!([value]), "{}");
        assert!(spec.validate().is_err());
    }
}

#[test]
fn network_configuration_and_missing_credentials_are_validated() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, json!([]), "{}");
    spec.providers.insert(
        "generator".into(),
        ProviderConfig::OpenaiCompatible {
            base_url: "http://localhost:8000/v1".into(),
            model: "local".into(),
            api_key_env: Some("SYNTHFLOW_NONEXISTENT_TEST_KEY_671".into()),
            concurrency: 1,
            timeout_ms: 100,
            retry: Default::default(),
        },
    );
    spec.validate().expect("valid network config");
    assert!(matches!(run(&spec), Err(Error::Configuration(_))));
    assert!(!spec.output.path.exists());
    if let Some(ProviderConfig::OpenaiCompatible { concurrency, .. }) =
        spec.providers.get_mut("generator")
    {
        *concurrency = 0;
    }
    assert!(spec.validate().is_err());
}

#[test]
fn stable_identity_ignores_key_order_but_includes_position_and_values() {
    let a: Value = serde_json::from_str(r#"{"b":{"y":2,"x":1},"a":0}"#).expect("json");
    let b: Value = serde_json::from_str(r#"{"a":0,"b":{"x":1,"y":2}}"#).expect("json");
    let a = a.as_object().expect("object");
    let b = b.as_object().expect("object");
    assert_eq!(record_id(a, 1), record_id(b, 1));
    assert_ne!(record_id(a, 1), record_id(a, 2));
    assert_ne!(
        record_id(a, 1),
        record_id(json!({"a":1}).as_object().expect("object"), 1)
    );
}

#[test]
fn paths_are_relative_to_yaml_and_hashes_are_stable() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, json!([]), "{}");
    let mut value = serde_json::to_value(spec).expect("serialize");
    value["output"]["path"] = json!("nested/result.jsonl");
    let path = dir.path().join("pipeline.yaml");
    // JSON is a valid subset of YAML.
    fs::write(&path, value.to_string()).expect("write");
    let loaded = Pipeline::load(&path).expect("load");
    assert_eq!(
        loaded.output.path,
        fs::canonicalize(dir.path())
            .expect("canonical tempdir")
            .join("nested/result.jsonl")
    );
    assert_eq!(
        loaded.hash().expect("hash"),
        Pipeline::load(&path).expect("load").hash().expect("hash")
    );
    assert!(loaded.plan().starts_with("InlineSource"));
    let previous = loaded.hash().expect("hash");
    value["generate"]["prompt"] = json!("different");
    fs::write(&path, value.to_string()).expect("write");
    assert_ne!(
        previous,
        Pipeline::load(&path).expect("load").hash().expect("hash")
    );
    value["checkpoint"] = json!({"every_records": 10});
    fs::write(&path, value.to_string()).expect("write");
    assert!(
        Pipeline::load(&path).is_err(),
        "unsupported features must not be silently ignored"
    );
}

#[test]
fn templates_are_strict_and_do_not_expose_environment() {
    let mut env = template::environment();
    env.add_template("test", "{{ env.HOME }}")
        .expect("template");
    assert!(template::render(&env, "test", &json!({})).is_err());
    assert!(template::validate("{% if %}", "test").is_err());
}

#[test]
fn template_execution_and_output_are_bounded() {
    let mut env = template::environment();
    env.add_template(
        "loop",
        "{% for i in range(10000) %}{% for j in range(10000) %}x{% endfor %}{% endfor %}",
    )
    .expect("template");
    assert!(template::render(&env, "loop", &json!({})).is_err());
    env.add_template("large", "{{ value }}{{ value }}")
        .expect("template");
    let context = json!({"value": "x".repeat(synthflow::spec::MAX_RECORD_BYTES / 2 + 1)});
    assert!(template::render(&env, "large", &context).is_err());
}

#[test]
fn oversized_source_lines_are_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("input.jsonl");
    fs::write(&path, "x".repeat(synthflow::spec::MAX_RECORD_BYTES + 1)).expect("input");
    let config = SourceConfig::Jsonl { path };
    let mut source = synthflow::source::Source::open(&config).expect("open");
    assert!(matches!(
        source.next(),
        Some(Err(Error::Source { position: 1, .. }))
    ));
}
