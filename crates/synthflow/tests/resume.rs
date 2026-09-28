use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use synthflow::{
    Error, Pipeline, Result, RunReport, RunStatus,
    provider::{GenerateRequest, GenerateResponse, LlmProvider},
    resume_with_provider, run_with_provider,
    spec::suffix,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const RESPONSE: &str = r##"{"answer": {{ record.topic | tojson }}}"##;

/// Fails all calls after `allow` successful ones; used to interrupt a run
/// mid-dataset while keeping the first records deterministic.
struct HaltingProvider {
    allow: usize,
    calls: AtomicUsize,
    /// Set when the first failing call starts.
    halted: Arc<Mutex<bool>>,
}
#[async_trait]
impl LlmProvider for HaltingProvider {
    async fn generate(
        &self,
        _request: GenerateRequest<'_>,
        cancellation: &CancellationToken,
    ) -> Result<GenerateResponse> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call < self.allow {
            return Ok(GenerateResponse {
                text: json!({"answer": format!("record-{}", call + 1)}).to_string(),
                model: "halting".into(),
                attempts: 1,
                prompt_tokens: None,
                completion_tokens: None,
            });
        }
        *self.halted.lock().expect("halted") = true;
        cancellation.cancelled().await;
        Err(Error::Cancelled)
    }
    fn concurrency(&self) -> usize {
        1
    }
}

struct Scripted {
    responses: Mutex<Vec<String>>,
}
#[async_trait]
impl LlmProvider for Scripted {
    async fn generate(
        &self,
        _request: GenerateRequest<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<GenerateResponse> {
        Ok(GenerateResponse {
            text: self
                .responses
                .lock()
                .expect("script")
                .pop()
                .expect("scripted response"),
            model: "scripted".into(),
            attempts: 1,
            prompt_tokens: None,
            completion_tokens: None,
        })
    }
    fn concurrency(&self) -> usize {
        1
    }
}
impl Scripted {
    fn from_answers(answers: &[&str]) -> Self {
        let mut responses: Vec<String> = answers
            .iter()
            .map(|answer| json!({"answer": answer}).to_string())
            .collect();
        responses.reverse(); // pop() serves in order
        Self {
            responses: Mutex::new(responses),
        }
    }
}

fn fixture(dir: &TempDir, records: Value) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "resume"},
        "source": {"type": "inline", "records": records},
        "providers": {"generator": {"type": "mock", "response": RESPONSE}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        },
        "output": {"format": "jsonl", "path": dir.path().join("result.jsonl")}
    }))
    .expect("test spec");
    spec.output.path = dir.path().join("result.jsonl");
    spec
}

fn read_manifest(spec: &Pipeline) -> RunReport {
    let path = suffix(&spec.output.path, ".manifest.json");
    serde_json::from_slice(&fs::read(&path).expect("manifest")).expect("report")
}

fn rows(spec: &Pipeline) -> Vec<Value> {
    fs::read_to_string(&spec.output.path)
        .expect("output")
        .lines()
        .map(|line| serde_json::from_str(line).expect("row"))
        .collect()
}

#[tokio::test]
async fn interrupted_run_resumes_without_duplicating_output() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([
            {"topic": "a"}, {"topic": "b"}, {"topic": "c"}, {"topic": "d"}, {"topic": "e"}
        ]),
    );
    // First run accepts records 1-2, then blocks on record 3 until cancelled.
    let halted = Arc::new(Mutex::new(false));
    let halting = Arc::new(HaltingProvider {
        allow: 2,
        calls: AtomicUsize::new(0),
        halted: halted.clone(),
    });
    let token = CancellationToken::new();
    let cancel = token.clone();
    let watcher = tokio::spawn(async move {
        while !*halted.lock().expect("halted") {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        cancel.cancel();
    });
    let interrupted = run_with_provider(&spec, halting, token.clone())
        .await
        .expect("interrupted run");
    watcher.abort();
    token.cancel();
    assert_eq!(interrupted.status, RunStatus::Cancelled);
    assert_eq!(interrupted.sink_state.committed_accepted_records, 2);
    assert_eq!(interrupted.sink_state.committed_source_position, 2);
    let prior = read_manifest(&spec);
    let prior_ids: Vec<String> = fs::read_to_string(suffix(&spec.output.path, ".partial"))
        .expect("partial")
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).expect("row")["_meta"]["record_id"]
                .as_str()
                .expect("id")
                .to_owned()
        })
        .collect();

    // Resume with a healthy provider; only positions 3-5 call it.
    let resumed = resume_with_provider(
        &spec,
        Arc::new(Scripted::from_answers(&["c", "d", "e"])),
        CancellationToken::new(),
    )
    .await
    .expect("resume");
    assert!(resumed.succeeded(), "{:?}", resumed.errors);
    assert_eq!(resumed.statistics.accepted_records_total, 3);
    let all = rows(&spec);
    assert_eq!(all.len(), 5, "no duplicated rows");
    let positions: Vec<u64> = all
        .iter()
        .map(|row| row["_meta"]["source_position"].as_u64().expect("position"))
        .collect();
    assert_eq!(positions, [1, 2, 3, 4, 5]);
    let ids: Vec<&str> = all
        .iter()
        .map(|row| row["_meta"]["record_id"].as_str().expect("id"))
        .collect();
    assert_eq!(
        &ids[..2],
        &prior_ids[..2],
        "committed prefix is preserved verbatim"
    );
    assert_eq!(
        resumed.resumed_from.as_ref().expect("provenance").run_id,
        prior.run_id
    );
    assert_eq!(
        resumed
            .resumed_from
            .as_ref()
            .expect("provenance")
            .accepted_records,
        2
    );
    assert_eq!(read_manifest(&spec).status, RunStatus::Completed);
    assert!(!suffix(&spec.output.path, ".partial").exists());
}

#[tokio::test]
async fn resume_rebuilds_dedup_state_from_the_committed_prefix() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(
        &dir,
        json!([
            {"topic": "a"}, {"topic": "duplicate"}, {"topic": "duplicate"}
        ]),
    );
    spec.dedup = Some(
        serde_json::from_value(json!({
            "method": "exact", "fields": ["topic"]
        }))
        .expect("dedup"),
    );
    let halted = Arc::new(Mutex::new(false));
    let halting = Arc::new(HaltingProvider {
        allow: 2,
        calls: AtomicUsize::new(0),
        halted: halted.clone(),
    });
    let token = CancellationToken::new();
    let cancel = token.clone();
    let watcher = tokio::spawn(async move {
        while !*halted.lock().expect("halted") {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        cancel.cancel();
    });
    let interrupted = run_with_provider(&spec, halting, token).await.expect("run");
    watcher.abort();
    assert_eq!(interrupted.status, RunStatus::Cancelled);
    assert_eq!(interrupted.sink_state.committed_accepted_records, 2);
    // Position 3 repeats topic "duplicate" from the committed prefix.
    let resumed = resume_with_provider(
        &spec,
        Arc::new(Scripted::from_answers(&["anything"])),
        CancellationToken::new(),
    )
    .await
    .expect("resume");
    assert!(resumed.succeeded(), "{:?}", resumed.errors);
    assert_eq!(resumed.statistics.duplicate_records_total, 1);
    assert_eq!(rows(&spec).len(), 2);
}

#[tokio::test]
async fn resume_refuses_changed_pipelines_and_missing_states() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, json!([{"topic": "a"}]));
    // No manifest yet.
    let error = resume_with_provider(
        &spec,
        Arc::new(Scripted::from_answers(&[])),
        CancellationToken::new(),
    )
    .await
    .expect_err("no manifest");
    assert!(matches!(error, Error::Configuration(_)));

    // Completed runs cannot be resumed.
    let done = run_with_provider(
        &spec,
        Arc::new(Scripted::from_answers(&["a"])),
        CancellationToken::new(),
    )
    .await
    .expect("run");
    assert!(done.succeeded());
    let error = resume_with_provider(
        &spec,
        Arc::new(Scripted::from_answers(&[])),
        CancellationToken::new(),
    )
    .await
    .expect_err("completed");
    assert!(error.to_string().contains("completed"));

    // A changed configuration hash is rejected.
    let mut spec2 = fixture(&dir, json!([{"topic": "a"}, {"topic": "b"}]));
    spec2.output.path = dir.path().join("other.jsonl");
    let halted = Arc::new(Mutex::new(false));
    let halting = Arc::new(HaltingProvider {
        allow: 1,
        calls: AtomicUsize::new(0),
        halted,
    });
    let token = CancellationToken::new();
    let cancel = token.clone();
    let watcher = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
    });
    let interrupted = run_with_provider(&spec2, halting, token)
        .await
        .expect("run");
    watcher.abort();
    if interrupted.status == RunStatus::Cancelled {
        spec2.dataset.name = "changed".into();
        let error = resume_with_provider(
            &spec2,
            Arc::new(Scripted::from_answers(&[])),
            CancellationToken::new(),
        )
        .await
        .expect_err("changed pipeline");
        assert!(error.to_string().contains("pipeline changed"));
    }
}
