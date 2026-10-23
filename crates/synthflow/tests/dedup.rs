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

#[tokio::test]
async fn dedup_budget_failure_preserves_checkpoint_without_rejecting_unique_data() {
    let dir = TempDir::new().unwrap();
    let mut spec = fixture(
        &dir,
        json!([
            {"topic":"a","answer":"first"}, {"topic":"b","answer":"second"}
        ]),
        json!({"method":"exact"}),
    );
    spec.dedup_max_bytes = 128;
    let report = run_async(&spec, CancellationToken::new()).await.unwrap();
    assert_eq!(report.status, synthflow::RunStatus::Failed);
    assert!(report.errors[0].message.contains("budget exceeded"));
    assert_eq!(report.sink_state.committed_source_position, 1);
    assert_eq!(report.sink_state.committed_accepted_records, 1);
    assert_eq!(report.statistics.rejected_records_total, 0);
    assert!(report.partial_path.exists());
    assert!(!report.output_path.exists());
}

#[tokio::test]
async fn disk_backend_accepts_more_than_the_memory_budget() {
    let dir = TempDir::new().unwrap();
    let mut records: Vec<Value> = (0..64)
        .map(|index| json!({"topic": "t", "answer": format!("answer-{index}")}))
        .collect();
    records.push(records[0].clone());
    let mut spec = fixture(&dir, json!(records), json!({"method": "exact"}));
    spec.dedup_storage = synthflow::spec::DedupStorage::Disk;
    spec.dedup_max_bytes = 128;
    let report = run_async(&spec, CancellationToken::new()).await.unwrap();
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 64);
    assert_eq!(report.statistics.duplicate_records_total, 1);
}

#[tokio::test]
async fn lowercase_object_fields_do_not_drop_colliding_keys() {
    let dir = TempDir::new().unwrap();
    let spec = fixture(
        &dir,
        json!([
            {"topic": "t", "answer": "a", "metadata": {"A": 1, "a": 2}},
            {"topic": "t", "answer": "b", "metadata": {"a": 2}}
        ]),
        json!({"method": "exact", "fields": ["metadata"], "normalize": "lowercase"}),
    );
    let report = run_async(&spec, CancellationToken::new()).await.unwrap();
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.accepted_records_total, 2);
    assert_eq!(report.statistics.duplicate_records_total, 0);
}

struct PauseThird(synthflow::provider::MockProvider);

#[async_trait::async_trait]
impl synthflow::provider::LlmProvider for PauseThird {
    async fn generate(
        &self,
        request: synthflow::provider::GenerateRequest<'_>,
        cancellation: &CancellationToken,
    ) -> synthflow::Result<synthflow::provider::GenerateResponse> {
        if request.record["topic"] == "pause" {
            cancellation.cancelled().await;
            return Err(Error::Cancelled);
        }
        self.0.generate(request, cancellation).await
    }
}

#[tokio::test]
async fn disk_indexes_rebuild_committed_records_before_resuming() {
    let original = "所有权规则帮助程序在编译阶段发现内存安全问题并且避免数据竞争，学习这些规则需要结合实际的代码示例认真练习。";
    let near_duplicate = "所有权规则帮助程序在编译阶段发现内存安全问题并且避免数据竞争，学习这些规则需要结合实际的代码示例反复练习。";
    for (config, duplicate) in [
        (json!({"method": "exact"}), original),
        (
            json!({"method": "minhash", "field": "generated.answer", "threshold": 0.6,
                "tokenization": "characters", "shingle_chars": 3}),
            near_duplicate,
        ),
    ] {
        let dir = TempDir::new().unwrap();
        let mut spec = fixture(
            &dir,
            json!([
                {"topic": "first", "answer": original},
                {"topic": "second", "answer": "独一无二的第二条数据"},
                {"topic": "pause", "answer": duplicate},
                {"topic": "last", "answer": "完成恢复后写入最后一条新数据"}
            ]),
            config,
        );
        spec.dedup_storage = synthflow::spec::DedupStorage::Disk;
        spec.dedup_max_bytes = 128;
        let token = CancellationToken::new();
        let watcher_token = token.clone();
        let manifest = synthflow::spec::suffix(&spec.output.path, ".manifest.json");
        let watcher = async move {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let report = fs::read(&manifest).ok().and_then(|bytes| {
                        serde_json::from_slice::<synthflow::RunReport>(&bytes).ok()
                    });
                    if report.is_some_and(|report| report.sink_state.committed_source_position == 2)
                    {
                        watcher_token.cancel();
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("first two records committed");
        };
        let provider = std::sync::Arc::new(PauseThird(
            synthflow::provider::MockProvider::new(RESPONSE, 1).unwrap(),
        ));
        let (interrupted, ()) = tokio::join!(
            synthflow::run_with_provider(&spec, provider, token),
            watcher
        );
        let interrupted = interrupted.unwrap();
        assert_eq!(interrupted.status, synthflow::RunStatus::Cancelled);
        assert_eq!(interrupted.sink_state.committed_accepted_records, 2);
        let resumed = synthflow::resume_async(&spec, CancellationToken::new())
            .await
            .unwrap();
        assert!(resumed.succeeded(), "{:?}", resumed.errors);
        assert_eq!(resumed.statistics.duplicate_records_total, 1);
        assert_eq!(
            fs::read_to_string(&spec.output.path)
                .unwrap()
                .lines()
                .count(),
            3
        );
        assert_eq!(dead_letter(&spec)[0]["source_position"], 3);
    }
}
