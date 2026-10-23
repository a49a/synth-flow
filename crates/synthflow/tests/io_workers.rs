use async_trait::async_trait;
use serde_json::json;
use std::{fs, sync::Arc, time::Duration};
use synthflow::{
    Error, Pipeline, Result, RunStatus,
    provider::{GenerateRequest, GenerateResponse, LlmProvider, MockProvider},
    resume_async, run_async, run_with_provider,
    spec::DedupStorage,
};
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const RESPONSE: &str = r##"{"answer": {{ record.answer | tojson }}}"##;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

fn fixture(dir: &TempDir, storage: DedupStorage) -> Pipeline {
    let source = dir.path().join("source.jsonl");
    fs::write(
        &source,
        [
            json!({"topic": "first", "answer": "same"}),
            json!({"topic": "pause", "answer": "same"}),
            json!({"topic": "last", "answer": "different"}),
        ]
        .iter()
        .map(|record| format!("{record}\n"))
        .collect::<String>(),
    )
    .unwrap();
    let mut pipeline: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "io-workers"},
        "source": {"type": "jsonl", "path": source},
        "providers": {"generator": {"type": "mock", "response": RESPONSE}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        },
        "dedup": {"method": "exact"},
        "output": {"format": "jsonl", "path": dir.path().join("result.jsonl")}
    }))
    .unwrap();
    pipeline.dedup_storage = storage;
    if matches!(storage, DedupStorage::Disk) {
        // A one-entry memory budget must not limit disk storage, including
        // the index reconstructed from the committed prefix during resume.
        pipeline.dedup_max_bytes = 128;
    }
    pipeline
}

#[test]
fn source_and_sink_workers_run_with_one_shared_blocking_thread() {
    runtime().block_on(async {
        for storage in [DedupStorage::Memory, DedupStorage::Disk] {
            let dir = TempDir::new().unwrap();
            let pipeline = fixture(&dir, storage);
            let report = tokio::time::timeout(
                Duration::from_secs(10),
                run_async(&pipeline, CancellationToken::new()),
            )
            .await
            .expect("pipeline must not exhaust the shared blocking pool")
            .unwrap();
            assert!(report.succeeded(), "{:?}", report.errors);
            assert_eq!(report.statistics.accepted_records_total, 2);
            assert_eq!(report.statistics.duplicate_records_total, 1);
        }
    });
}

struct PauseSecond {
    mock: MockProvider,
    reached: Arc<Notify>,
}

#[async_trait]
impl LlmProvider for PauseSecond {
    async fn generate(
        &self,
        request: GenerateRequest<'_>,
        cancellation: &CancellationToken,
    ) -> Result<GenerateResponse> {
        if request.record["topic"] == "pause" {
            // Concurrency is one: this call starts after the first record's
            // sink write and checkpoint have completed.
            self.reached.notify_one();
            cancellation.cancelled().await;
            return Err(Error::Cancelled);
        }
        self.mock.generate(request, cancellation).await
    }
}

#[test]
fn cancellation_and_resume_work_with_one_shared_blocking_thread() {
    runtime().block_on(async {
        for storage in [DedupStorage::Memory, DedupStorage::Disk] {
            let dir = TempDir::new().unwrap();
            let pipeline = fixture(&dir, storage);
            let reached = Arc::new(Notify::new());
            let provider = Arc::new(PauseSecond {
                mock: MockProvider::new(RESPONSE, 1).unwrap(),
                reached: reached.clone(),
            });
            let token = CancellationToken::new();
            let cancel = token.clone();
            let (interrupted, ()) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(run_with_provider(&pipeline, provider, token), async {
                    reached.notified().await;
                    cancel.cancel();
                })
            })
            .await
            .expect("cancellation must not exhaust the shared blocking pool");
            let interrupted = interrupted.unwrap();
            assert_eq!(interrupted.status, RunStatus::Cancelled);
            assert_eq!(interrupted.sink_state.committed_source_position, 1);
            assert_eq!(interrupted.sink_state.committed_accepted_records, 1);
            assert!(!pipeline.output.path.exists());

            let resumed = tokio::time::timeout(
                Duration::from_secs(10),
                resume_async(&pipeline, CancellationToken::new()),
            )
            .await
            .expect("resume must not exhaust the shared blocking pool")
            .unwrap();
            assert!(resumed.succeeded(), "{:?}", resumed.errors);
            assert_eq!(resumed.statistics.duplicate_records_total, 1);
            assert_eq!(resumed.sink_state.committed_source_position, 3);
            assert_eq!(resumed.sink_state.committed_accepted_records, 2);
            assert_eq!(
                fs::read_to_string(&pipeline.output.path)
                    .unwrap()
                    .lines()
                    .count(),
                2
            );
        }
    });
}
