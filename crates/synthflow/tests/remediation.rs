//! Regression tests for the second review round: concurrent resume locking,
//! checkpoint integrity on parquet publish failure, schema stability on
//! resume, dedup commit ordering, and failure-policy enforcement on resume.

use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

/// Serves scripted response texts in order. The `gate` call parks until a
/// sentinel path appears (or the run is cancelled), letting tests inject
/// external state at an exact record boundary.
struct ScriptedProvider {
    responses: Vec<String>,
    gate_call: Option<usize>,
    gate_path: PathBuf,
    reached: AtomicBool,
    calls: AtomicUsize,
}
#[async_trait]
impl LlmProvider for ScriptedProvider {
    async fn generate(
        &self,
        _request: GenerateRequest<'_>,
        cancellation: &CancellationToken,
    ) -> Result<GenerateResponse> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let text = self.responses.get(call).cloned().unwrap_or_default();
        if self.gate_call == Some(call) {
            self.reached.store(true, Ordering::SeqCst);
            tokio::select! {
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                _ = async {
                    while !self.gate_path.exists() {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                } => {}
            }
        }
        Ok(GenerateResponse {
            text,
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

impl ScriptedProvider {
    fn answers(answers: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            responses: answers.iter().map(|a| (*a).to_owned()).collect(),
            gate_call: None,
            gate_path: PathBuf::new(),
            reached: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        })
    }
    fn gated(answers: &[&str], gate_call: usize, gate_path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            responses: answers.iter().map(|a| (*a).to_owned()).collect(),
            gate_call: Some(gate_call),
            gate_path,
            reached: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        })
    }
    async fn wait_until_reached(&self) {
        while !self.reached.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}

fn fixture(dir: &TempDir, records: Value, format: &str) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "remediation"},
        "source": {"type": "inline", "records": records},
        "providers": {"generator": {"type": "mock", "response": "{\"answer\": {{ record.topic | tojson }}}}"}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}, "extra": {"type": "string"}}}
        },
        "output": {"format": format, "path": dir.path().join(format!("result.{format}"))}
    }))
    .expect("test spec");
    spec.output.path = dir.path().join(format!("result.{format}"));
    spec
}

fn rows_jsonl(path: &std::path::Path) -> Vec<Value> {
    fs::read_to_string(path)
        .expect("partial")
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("row"))
        .collect()
}

fn read_manifest(spec: &Pipeline) -> RunReport {
    let path = suffix(&spec.output.path, ".manifest.json");
    serde_json::from_slice(&fs::read(&path).expect("manifest")).expect("report")
}

fn parquet_rows(path: &std::path::Path) -> usize {
    let file = fs::File::open(path).expect("parquet");
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader")
        .build()
        .expect("batches");
    reader.map(|batch| batch.expect("batch").num_rows()).sum()
}

#[test]
fn source_and_dead_letter_cannot_use_internal_artifact_paths() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, json!([]), "parquet");
    for suffix_name in [".lock", ".publishing"] {
        let path = suffix(&spec.output.path, suffix_name);
        fs::write(&path, b"{}\n").expect("source fixture");
        spec.source = synthflow::spec::SourceConfig::Jsonl { path: path.clone() };
        assert!(matches!(spec.validate(), Err(Error::Configuration(_))));
        spec.source = synthflow::spec::SourceConfig::Inline { records: vec![] };
        spec.errors.dead_letter = Some(path.clone());
        assert!(matches!(spec.validate(), Err(Error::Configuration(_))));
        spec.errors.dead_letter = None;
    }
}

#[tokio::test]
async fn preexisting_parquet_conversion_file_is_preserved() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, json!([{"topic": "a"}]), "parquet");
    let staged = suffix(&spec.output.path, ".publishing");
    fs::write(&staged, b"owned by another process").expect("stage");
    let result = run_with_provider(
        &spec,
        ScriptedProvider::answers(&[r#"{"answer":"a"}"#]),
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(result, Err(Error::Configuration(_))));
    assert_eq!(
        fs::read(staged).expect("stage remains"),
        b"owned by another process"
    );
}

/// Interrupt a run after `committed` records are durable: the next provider
/// call parks, the test cancels, and the parked call unwinds.
async fn interrupt_after(spec: &Pipeline, committed: usize, answers: &[&str]) {
    let sentinel = spec.output.path.with_extension("gate");
    let provider = ScriptedProvider::gated(answers, committed, sentinel);
    let token = CancellationToken::new();
    let run = run_with_provider(spec, provider.clone(), token.clone());
    tokio::pin!(run);
    let report = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::select! {
            report = &mut run => report,
            _ = provider.wait_until_reached() => {
                token.cancel();
                run.await
            }
        }
    })
    .await
    .expect("bounded test");
    provider.reached.store(true, Ordering::SeqCst);
    let report = report.expect("interrupted run returns a report");
    assert_eq!(report.status, RunStatus::Cancelled, "{:?}", report.errors);
    assert_eq!(
        report.sink_state.committed_accepted_records as usize, committed,
        "{:?}",
        report.sink_state
    );
}

// ---------------------------------------------------------------- issue 1

#[tokio::test]
async fn concurrent_resumes_allow_exactly_one_writer() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([{"topic": "a"}, {"topic": "b"}, {"topic": "c"}, {"topic": "d"}, {"topic": "e"}]),
        "jsonl",
    );
    interrupt_after(
        &spec,
        2,
        &[
            r#"{"answer":"a"}"#,
            r#"{"answer":"b"}"#,
            r#"{"answer":"c"}"#,
            r#"{"answer":"d"}"#,
            r#"{"answer":"e"}"#,
        ],
    )
    .await;

    // Two resumes race on the same checkpoint; the run lock must refuse the
    // loser before it reads or truncates anything.
    let (first, second) = tokio::join!(
        resume_with_provider(
            &spec,
            ScriptedProvider::answers(&[
                r#"{"answer":"c"}"#,
                r#"{"answer":"d"}"#,
                r#"{"answer":"e"}"#
            ]),
            CancellationToken::new()
        ),
        resume_with_provider(
            &spec,
            ScriptedProvider::answers(&[
                r#"{"answer":"c"}"#,
                r#"{"answer":"d"}"#,
                r#"{"answer":"e"}"#
            ]),
            CancellationToken::new()
        ),
    );
    let succeeded = [&first, &second]
        .iter()
        .filter(|result| matches!(result, Ok(report) if report.succeeded()))
        .count();
    let refused = [&first, &second]
        .iter()
        .filter(|result| matches!(result, Err(Error::Configuration(message)) if message.contains("run lock")))
        .count();
    assert_eq!(
        succeeded, 1,
        "exactly one resume may complete: {first:?} {second:?}"
    );
    assert_eq!(
        refused, 1,
        "the loser must be refused by the lock: {first:?} {second:?}"
    );

    let rows = rows_jsonl(&spec.output.path);
    let positions: Vec<u64> = rows
        .iter()
        .map(|row| row["_meta"]["source_position"].as_u64().expect("position"))
        .collect();
    assert_eq!(positions, [1, 2, 3, 4, 5], "no duplicated output");
    let manifest = read_manifest(&spec);
    assert_eq!(manifest.status, RunStatus::Completed);
    assert_eq!(
        manifest.sink_state.committed_accepted_records,
        rows.len() as u64,
        "manifest agrees with the published data"
    );
}

// ---------------------------------------------------------------- issue 2

#[tokio::test]
async fn parquet_publish_failure_keeps_the_jsonl_checkpoint_resumable() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([{"topic": "a"}, {"topic": "b"}, {"topic": "c"}]),
        "parquet",
    );
    // The last provider call parks until the sentinel appears, so the test
    // can create a conflicting final output path before publication runs.
    let sentinel = spec.output.path.with_extension("gate");
    let provider = ScriptedProvider::gated(
        &[
            r#"{"answer":"a"}"#,
            r#"{"answer":"b"}"#,
            r#"{"answer":"c"}"#,
        ],
        2,
        sentinel.clone(),
    );
    let run = run_with_provider(&spec, provider.clone(), CancellationToken::new());
    tokio::pin!(run);
    let failed = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::select! {
            report = &mut run => report,
            _ = provider.wait_until_reached() => {
                fs::write(&spec.output.path, b"external conflict").expect("create conflict");
                // Release the parked provider call; publication runs and fails.
                fs::write(&sentinel, b"").expect("release gate");
                run.await
            }
        }
    })
    .await
    .expect("bounded test")
    .expect("report");
    assert_eq!(failed.status, RunStatus::Failed, "{:?}", failed.errors);
    assert!(
        failed.errors.iter().any(|e| e.category == "io"),
        "{:?}",
        failed.errors
    );

    // The checkpoint must still be JSONL with the committed prefix intact.
    let partial = suffix(&spec.output.path, ".partial");
    let bytes = fs::read(&partial).expect("partial");
    assert_eq!(
        bytes.first(),
        Some(&b'{'),
        "partial stays JSONL, not parquet"
    );
    assert_eq!(
        rows_jsonl(&partial).len(),
        3,
        "prefix survives the failed publish"
    );
    assert!(
        !suffix(&spec.output.path, ".publishing").exists(),
        "staged file cleaned up"
    );
    assert_eq!(read_manifest(&spec).status, RunStatus::Failed);

    // Remove the external conflict and resume: no data lost or duplicated.
    fs::remove_file(&spec.output.path).expect("remove conflict");
    let resumed = resume_with_provider(
        &spec,
        ScriptedProvider::answers(&[]),
        CancellationToken::new(),
    )
    .await
    .expect("resume");
    assert!(resumed.succeeded(), "{:?}", resumed.errors);
    assert_eq!(
        resumed.statistics.accepted_records_total, 0,
        "nothing left to process"
    );
    assert_eq!(
        parquet_rows(&spec.output.path),
        3,
        "each record exactly once"
    );
}

// ---------------------------------------------------------------- issue 3

#[tokio::test]
async fn parquet_resume_keeps_the_original_schema_constraints() {
    let records = json!([{"topic": "first"}, {"topic": "second"}]);
    let answers = [r#"{"answer":"a"}"#, r#"{"answer":"b","extra":"new"}"#];

    // Continuous run: the second record violates the schema inferred from the
    // first record and must be rejected.
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, records.clone(), "parquet");
    let continuous = run_with_provider(
        &spec,
        ScriptedProvider::answers(&answers),
        CancellationToken::new(),
    )
    .await
    .expect("continuous run");
    assert!(continuous.succeeded(), "{:?}", continuous.errors);
    assert_eq!(continuous.statistics.accepted_records_total, 1);
    assert_eq!(continuous.statistics.rejected_records_total, 1);
    let dead = fs::read_to_string(suffix(&spec.output.path, ".rejected.jsonl")).expect("dead");
    assert!(dead.contains("sink_row"), "{dead}");
    assert_eq!(parquet_rows(&spec.output.path), 1);

    // Interrupt after the first record, then resume: the schema constraint
    // must survive, so acceptance decisions stay identical.
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, records, "parquet");
    interrupt_after(&spec, 1, &answers).await;
    let resumed = resume_with_provider(
        &spec,
        ScriptedProvider::answers(&[answers[1]]),
        CancellationToken::new(),
    )
    .await
    .expect("resume");
    assert!(resumed.succeeded(), "{:?}", resumed.errors);
    assert_eq!(resumed.statistics.accepted_records_total, 0);
    assert_eq!(resumed.statistics.rejected_records_total, 1);
    let dead = fs::read_to_string(suffix(&spec.output.path, ".rejected.jsonl")).expect("dead");
    assert!(dead.contains("sink_row"), "{dead}");
    assert_eq!(
        parquet_rows(&spec.output.path),
        1,
        "same logical output as continuous"
    );
}

// ---------------------------------------------------------------- issue 4

#[tokio::test]
async fn sink_rejected_records_do_not_pollute_dedup_state() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(
        &dir,
        json!([{"topic": "one"}, {"topic": "two"}, {"topic": "three"}]),
        "parquet",
    );
    spec.dedup = Some(
        serde_json::from_value(json!({"method": "exact", "fields": ["generated.key"]}))
            .expect("dedup"),
    );
    let provider = ScriptedProvider::answers(&[
        r#"{"key":"A","answer":"ok"}"#,
        r#"{"key":"B","answer":"ok","extra":"late"}"#,
        r#"{"key":"B","answer":"ok"}"#,
    ]);
    let report = run_with_provider(&spec, provider, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    // B-with-extra is rejected by the parquet schema; that must not register
    // B's dedup key, so the clean B afterwards is accepted.
    assert_eq!(
        report.statistics.accepted_records_total, 2,
        "{:?}",
        report.statistics
    );
    assert_eq!(report.statistics.rejected_records_total, 1);
    assert_eq!(report.statistics.duplicate_records_total, 0);
    let dead = fs::read_to_string(suffix(&spec.output.path, ".rejected.jsonl")).expect("dead");
    assert!(dead.contains("sink_row"), "{dead}");
    assert_eq!(parquet_rows(&spec.output.path), 2);
}

// ---------------------------------------------------------------- issue 5

async fn violating_resume_is_refused(strict: bool, max_failed_records: Option<u64>) {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, json!([{"topic": "good"}, {"topic": "bad"}]), "jsonl");
    spec.errors.strict = strict;
    spec.errors.max_failed_records = max_failed_records;
    // First run: record 1 accepted, record 2 produces invalid JSON and is
    // rejected, so the run fails with the violation already committed.
    let first = run_with_provider(
        &spec,
        ScriptedProvider::answers(&[r#"{"answer":"ok"}"#, "not json"]),
        CancellationToken::new(),
    )
    .await
    .expect("first run");
    assert_eq!(first.status, RunStatus::Failed, "{:?}", first.errors);
    assert_eq!(first.sink_state.committed_accepted_records, 1);
    assert_eq!(first.sink_state.committed_rejected_records, 1);

    // Resume with the same configuration must not turn it into a success:
    // either refused before a run is established, or a failed report.
    match resume_with_provider(
        &spec,
        ScriptedProvider::answers(&[]),
        CancellationToken::new(),
    )
    .await
    {
        Ok(report) => {
            assert_eq!(report.status, RunStatus::Failed, "{:?}", report.errors);
            assert!(
                report.errors.iter().any(|e| e.category == "failure_policy"),
                "{:?}",
                report.errors
            );
        }
        Err(Error::FailurePolicy(_)) => {} // refused at the checkpoint gate
        Err(other) => panic!("unexpected resume error: {other:?}"),
    }
    assert!(
        !spec.output.path.exists(),
        "violating dataset stays unpublished"
    );
}

#[tokio::test]
async fn resume_cannot_bypass_strict_mode() {
    violating_resume_is_refused(true, None).await;
}

#[tokio::test]
async fn resume_cannot_bypass_max_failed_records() {
    violating_resume_is_refused(false, Some(0)).await;
}

#[tokio::test]
async fn publishing_snapshot_finishes_only_the_recorded_output() {
    for format in ["jsonl", "parquet"] {
        let dir = TempDir::new().unwrap();
        let spec = fixture(&dir, json!([{"topic":"a"}]), format);
        let mut report = run_with_provider(
            &spec,
            ScriptedProvider::answers(&[r#"{"answer":"a"}"#]),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(report.succeeded());
        let bytes = fs::read(&spec.output.path).unwrap();
        report.status = RunStatus::Publishing;
        report.finished_at_unix_ms = None;
        fs::write(&report.manifest_path, serde_json::to_vec(&report).unwrap()).unwrap();
        let provider = ScriptedProvider::answers(&[]);
        let resumed = resume_with_provider(&spec, provider.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert!(resumed.succeeded());
        assert_eq!(resumed.run_id, report.run_id);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(fs::read(&spec.output.path).unwrap(), bytes);

        // The same publication intent must never claim unrelated bytes.
        fs::write(&report.manifest_path, serde_json::to_vec(&report).unwrap()).unwrap();
        fs::write(&spec.output.path, b"conflicting output").unwrap();
        let result = resume_with_provider(&spec, provider, CancellationToken::new()).await;
        assert!(matches!(result, Err(Error::Configuration(_))));
        assert_eq!(fs::read(&spec.output.path).unwrap(), b"conflicting output");
        assert_eq!(read_manifest(&spec).status, RunStatus::Publishing);
    }
}

#[tokio::test]
async fn running_and_unlinked_publishing_snapshots_resume_the_prefix() {
    use std::io::Write;
    for status in [RunStatus::Running, RunStatus::Publishing] {
        let dir = TempDir::new().unwrap();
        let spec = fixture(&dir, json!([{"topic":"a"},{"topic":"b"}]), "parquet");
        interrupt_after(&spec, 1, &[r#"{"answer":"a"}"#, r#"{"answer":"b"}"#]).await;
        let mut prior = read_manifest(&spec);
        prior.status = status;
        fs::write(&prior.manifest_path, serde_json::to_vec(&prior).unwrap()).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&prior.partial_path)
            .unwrap()
            .write_all(b"uncommitted tail")
            .unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&prior.dead_letter_path)
            .unwrap()
            .write_all(b"uncommitted tail")
            .unwrap();
        // Legacy conversion leftovers must not block or be overwritten by resume.
        let abandoned = suffix(&spec.output.path, ".publishing");
        fs::write(&abandoned, b"partial conversion").unwrap();
        let report = resume_with_provider(
            &spec,
            ScriptedProvider::answers(&[r#"{"answer":"b"}"#]),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(report.succeeded(), "{:?}", report.errors);
        assert_eq!(parquet_rows(&spec.output.path), 2);
        assert_eq!(fs::read(abandoned).unwrap(), b"partial conversion");
    }
}

#[tokio::test]
async fn jsonl_and_csv_resume_use_committed_source_offsets() {
    for format in ["jsonl", "csv"] {
        let dir = TempDir::new().unwrap();
        let input = dir.path().join(format!("input.{format}"));
        let body = if format == "csv" {
            "topic\r\n\"a\nmultiline\"\r\nb\r\nc"
        } else {
            "{\"topic\":\"a\"}\n{\"topic\":\"b\"}\n{\"topic\":\"c\"}"
        };
        fs::write(&input, body).unwrap();
        let mut spec = fixture(&dir, json!([]), "jsonl");
        spec.source = if format == "csv" {
            synthflow::spec::SourceConfig::Csv { path: input }
        } else {
            synthflow::spec::SourceConfig::Jsonl { path: input }
        };
        interrupt_after(&spec, 1, &[r#"{"answer":"a"}"#, r#"{"answer":"b"}"#]).await;
        let before = read_manifest(&spec);
        assert!(before.sink_state.source_byte_offset.unwrap() > 0);
        let report = resume_with_provider(
            &spec,
            ScriptedProvider::answers(&[r#"{"answer":"b"}"#, r#"{"answer":"c"}"#]),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(report.succeeded(), "{:?}", report.errors);
        assert_eq!(report.statistics.source_records_total, 2);
        assert_eq!(
            report.sink_state.source_byte_offset,
            Some(body.len() as u64)
        );
        let rows = rows_jsonl(&spec.output.path);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1]["topic"], "b");
        assert_eq!(rows[2]["_meta"]["source_position"], 3);
    }
}

#[tokio::test]
async fn cancelled_rejection_ratio_can_improve_after_resume() {
    let dir = TempDir::new().unwrap();
    let mut spec = fixture(&dir, json!([{"topic":"a"},{"topic":"b"}]), "jsonl");
    spec.errors.max_failed_ratio = Some(0.5);
    let provider = ScriptedProvider::gated(
        &["invalid", r#"{"answer":"b"}"#],
        1,
        dir.path().join("gate"),
    );
    let token = CancellationToken::new();
    let run = run_with_provider(&spec, provider.clone(), token.clone());
    tokio::pin!(run);
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            report = &mut run => report,
            _ = provider.wait_until_reached() => { token.cancel(); run.await }
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.status, RunStatus::Cancelled);
    assert_eq!(first.sink_state.committed_rejected_records, 1);
    let report = resume_with_provider(
        &spec,
        ScriptedProvider::answers(&[r#"{"answer":"b"}"#]),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.sink_state.committed_accepted_records, 1);
}

#[tokio::test]
async fn legacy_checkpoint_without_file_offset_remains_resumable() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.jsonl");
    fs::write(&path, "{\"topic\":\"a\"}\n{\"topic\":\"b\"}\n").unwrap();
    let mut spec = fixture(&dir, json!([]), "jsonl");
    spec.source = synthflow::spec::SourceConfig::Jsonl { path };
    interrupt_after(&spec, 1, &[r#"{"answer":"a"}"#, r#"{"answer":"b"}"#]).await;
    let mut prior = read_manifest(&spec);
    prior.sink_state.source_byte_offset = None;
    fs::write(&prior.manifest_path, serde_json::to_vec(&prior).unwrap()).unwrap();
    let report = resume_with_provider(
        &spec,
        ScriptedProvider::answers(&[r#"{"answer":"b"}"#]),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.succeeded());
    assert_eq!(rows_jsonl(&spec.output.path).len(), 2);
    assert_eq!(report.statistics.source_records_total, 1);
}

#[tokio::test]
async fn judge_reuses_the_same_configured_provider_instance() {
    let dir = TempDir::new().unwrap();
    let mut spec = fixture(&dir, json!([{"topic":"a"}]), "jsonl");
    spec.judge = Some(
        serde_json::from_value(json!({"provider":"generator","prompt":"score","min_score":0.5}))
            .unwrap(),
    );
    let provider = ScriptedProvider::answers(&[r#"{"answer":"a"}"#, r#"{"score":1}"#]);
    let report = run_with_provider(&spec, provider.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(report.statistics.latency_calls_total, 2);
}
