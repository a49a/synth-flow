use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    fs,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use synthflow::{
    Pipeline, RunReport, RunStatus,
    provider::{GenerateRequest, GenerateResponse, LlmProvider},
    run_async, run_with_provider,
    spec::{SourceConfig, suffix},
};
use tokio_util::sync::CancellationToken;

fn pipeline(dir: &Path, records: Value) -> Pipeline {
    let mut p: Pipeline = serde_json::from_value(json!({
        "version":1,"dataset":{"name":"reliability"},"source":{"type":"inline","records":records},
        "providers":{"generator":{"type":"mock","response":"{{ record.response }}"}},
        "generate":{"provider":"generator","prompt":"Explain {{ record.topic }}", "output_schema":{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}},
        "output":{"format":"jsonl","path":dir.join("output.jsonl")}
    })).expect("pipeline");
    p.errors.max_failed_records = None;
    p
}
fn good_records(n: usize) -> Value {
    Value::Array(
        (0..n)
            .map(|i| json!({"topic":i,"response":"{\"answer\":\"ok\"}"}))
            .collect(),
    )
}
fn read_report(path: &Path) -> RunReport {
    serde_json::from_slice(&fs::read(path).expect("manifest")).expect("report")
}

#[tokio::test]
async fn failed_runs_preserve_durable_prefix_and_diagnostics() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut p = pipeline(dir.path(), good_records(0));
    let input = dir.path().join("input.jsonl");
    fs::write(
        &input,
        "{\"topic\":1,\"response\":\"{\\\"answer\\\":\\\"ok\\\"}\"}\nBAD\n",
    )
    .expect("input");
    p.source = SourceConfig::Jsonl { path: input };
    let report = run_async(&p, CancellationToken::new())
        .await
        .expect("report");
    assert_eq!(report.status, RunStatus::Failed);
    assert!(!p.output.path.exists());
    assert_eq!(report.statistics.accepted_records_total, 1);
    assert_eq!(report.sink_state.committed_source_position, 1);
    assert_eq!(
        fs::metadata(&report.partial_path).expect("partial").len(),
        report.sink_state.accepted_bytes
    );
    assert_eq!(read_report(&report.manifest_path).status, RunStatus::Failed);
    assert_eq!(report.errors[0].category, "source");
}

#[tokio::test]
async fn schema_diagnostics_and_failure_policies() {
    for (strict, limit, ratio, expected_processed) in [
        (true, None, None, 2),
        (false, Some(0), None, 2),
        (false, None, Some(0.2), 3),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut records = good_records(3);
        records[1]["response"] = json!("{\"answer\":42,\"private\":\"SECRET_RESPONSE\"}");
        let mut p = pipeline(dir.path(), records);
        p.errors.strict = strict;
        p.errors.max_failed_records = limit;
        p.errors.max_failed_ratio = ratio;
        let report = run_async(&p, CancellationToken::new()).await.expect("run");
        assert_eq!(report.status, RunStatus::Failed);
        assert_eq!(
            report.statistics.processed_records_total,
            expected_processed
        );
        assert!(!report.output_path.exists());
        let dead = fs::read_to_string(&report.dead_letter_path).expect("dead letter");
        assert!(!dead.contains("SECRET_RESPONSE"));
        let diagnostic: Value = serde_json::from_str(dead.trim()).expect("entry");
        assert_eq!(diagnostic["diagnostic"]["instance_path"], "/answer");
        assert_eq!(
            diagnostic["diagnostic"]["schema_path"],
            "/properties/answer/type"
        );
        assert!(diagnostic["record_id"].as_str().is_some());
    }
}

#[tokio::test]
async fn all_rejected_fails_but_empty_and_allowed_partial_success_publish() {
    for (records, expected) in [
        (json!([{"topic":1,"response":"bad"}]), RunStatus::Failed),
        (good_records(0), RunStatus::Completed),
        (
            json!([{"topic":1,"response":"bad"},{"topic":2,"response":"{\"answer\":\"ok\"}"}]),
            RunStatus::Completed,
        ),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let p = pipeline(dir.path(), records);
        let report = run_async(&p, CancellationToken::new()).await.expect("run");
        assert_eq!(report.status, expected);
        assert_eq!(report.output_path.exists(), report.succeeded());
        assert_eq!(report.partial_path.exists(), !report.succeeded());
    }
}

#[test]
fn schema_reference_detection_respects_schema_positions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut p = pipeline(dir.path(), good_records(0));
    for schema in [
        json!({"type":"object","properties":{"$ref":{"type":"string"}}}),
        json!({"enum":[{"$ref":"ordinary data"}]}),
        json!({"default":{"$ref":"data"}}),
    ] {
        p.generate.output_schema = schema;
        p.compile_schema().expect("literal $ref is not a reference");
    }
    for schema in [
        json!({"properties":{"x":{"$ref":"file:///secret"}}}),
        json!({"allOf":[{"$ref":"https://invalid"}]}),
        json!({"items":{"$dynamicRef":"#x"}}),
    ] {
        p.generate.output_schema = schema;
        assert!(p.compile_schema().is_err());
    }
}

#[test]
fn config_diagnostics_locate_fields_without_echoing_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = pipeline(dir.path(), good_records(0));
    let mut value = serde_json::to_value(p).expect("json");
    value["errors"]["max_failed_records"] = json!("SECRET_BAD_VALUE");
    let path = dir.path().join("p.yaml");
    fs::write(&path, value.to_string()).expect("write");
    let error = Pipeline::load(path).expect_err("invalid").to_string();
    assert!(error.contains("errors.max_failed_records"), "{error}");
    assert!(!error.contains("SECRET_BAD_VALUE"));
}

#[test]
fn equivalent_paths_have_equal_hashes_and_source_changes_have_new_fingerprints() {
    let dir = tempfile::tempdir_in(std::env::current_dir().expect("cwd")).expect("tempdir");
    let p = pipeline(dir.path(), good_records(0));
    let path = dir.path().join("p.yaml");
    fs::write(&path, serde_json::to_vec(&p).expect("json")).expect("write");
    let absolute = Pipeline::load(&path).expect("load");
    let relative = Pipeline::load(
        path.strip_prefix(std::env::current_dir().expect("cwd"))
            .expect("relative"),
    )
    .expect("load");
    assert_eq!(
        absolute.hash().expect("hash"),
        relative.hash().expect("hash")
    );
    let input = dir.path().join("input.jsonl");
    fs::write(&input, "first").expect("write");
    let source = SourceConfig::Jsonl {
        path: input.clone(),
    };
    let old = synthflow::run::fingerprint(&source, &CancellationToken::new()).expect("hash");
    fs::write(&input, "second").expect("write");
    assert_ne!(
        old,
        synthflow::run::fingerprint(&source, &CancellationToken::new()).expect("hash")
    );
}

struct MutateSource {
    path: std::path::PathBuf,
}
#[async_trait]
impl LlmProvider for MutateSource {
    async fn generate(
        &self,
        _: GenerateRequest<'_>,
        _: &CancellationToken,
    ) -> synthflow::Result<GenerateResponse> {
        fs::write(&self.path, "{\"topic\":2}\n").expect("mutate fixture");
        Ok(GenerateResponse {
            text: "{\"answer\":\"ok\"}".into(),
            model: "custom".into(),
            attempts: 1,
        })
    }
}
#[tokio::test]
async fn changing_source_prevents_publication() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut p = pipeline(dir.path(), good_records(0));
    let path = dir.path().join("input.jsonl");
    fs::write(&path, "{\"topic\":1}\n").expect("write");
    p.source = SourceConfig::Jsonl { path: path.clone() };
    let report = run_with_provider(
        &p,
        Arc::new(MutateSource { path }),
        CancellationToken::new(),
    )
    .await
    .expect("run");
    assert_eq!(report.status, RunStatus::Failed);
    assert!(report.errors[0].message.contains("source changed"));
    assert!(!report.output_path.exists());
}

struct Reply {
    status: u16,
    body: Value,
    delay: u64,
    retry_after: Option<&'static str>,
}
fn ok_reply(delay: u64) -> Reply {
    Reply {
        status: 200,
        body: json!({"choices":[{"message":{"content":"{\"answer\":\"ok\"}"}}], "model":"local-test", "usage":{"prompt_tokens":7,"completion_tokens":3}}),
        delay,
        retry_after: None,
    }
}
#[derive(Default)]
struct HttpState {
    replies: Mutex<VecDeque<Reply>>,
    calls: Mutex<Vec<(HeaderMap, Value)>>,
    active: AtomicUsize,
    peak: AtomicUsize,
}
async fn handler(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    state.calls.lock().expect("calls").push((headers, body));
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    state.peak.fetch_max(active, Ordering::SeqCst);
    let reply = state
        .replies
        .lock()
        .expect("replies")
        .pop_front()
        .unwrap_or_else(|| ok_reply(0));
    tokio::time::sleep(Duration::from_millis(reply.delay)).await;
    state.active.fetch_sub(1, Ordering::SeqCst);
    let mut response = (
        StatusCode::from_u16(reply.status).expect("status"),
        Json(reply.body),
    )
        .into_response();
    if let Some(value) = reply.retry_after {
        response
            .headers_mut()
            .insert("retry-after", value.parse().expect("header"));
    }
    response
}
struct Server {
    url: String,
    state: Arc<HttpState>,
    handle: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
async fn server(replies: Vec<Reply>) -> Server {
    let state = Arc::new(HttpState {
        replies: Mutex::new(replies.into()),
        ..Default::default()
    });
    let app = Router::new()
        .route("/v1/chat/completions", post(handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let url = format!("http://{}/v1", listener.local_addr().expect("address"));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server");
    });
    Server { url, state, handle }
}
fn http_pipeline(
    dir: &Path,
    server: &Server,
    count: usize,
    timeout: u64,
    concurrency: usize,
) -> Pipeline {
    let mut p = pipeline(dir, good_records(count));
    p.providers.insert("generator".into(), serde_json::from_value(json!({"type":"openai_compatible","base_url":server.url,"model":"test","concurrency":concurrency,"timeout_ms":timeout,"retry":{"max_attempts":3,"initial_delay_ms":1,"max_delay_ms":5}})).expect("provider"));
    p
}

#[tokio::test]
async fn http_retries_transient_errors_and_tracks_usage() {
    let server = server(vec![
        Reply {
            status: 429,
            body: json!({"secret":"not logged"}),
            delay: 0,
            retry_after: Some("0"),
        },
        Reply {
            status: 503,
            ..ok_reply(0)
        },
        ok_reply(0),
    ])
    .await;
    let dir = tempfile::tempdir().expect("tempdir");
    let p = http_pipeline(dir.path(), &server, 1, 1000, 1);
    let report = run_async(&p, CancellationToken::new()).await.expect("run");
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.provider_usage.requests, 3);
    assert_eq!(report.statistics.provider_usage.retries, 2);
    assert_eq!(report.statistics.provider_usage.prompt_tokens, 7);
    let row: Value = serde_json::from_str(
        fs::read_to_string(&report.output_path)
            .expect("file")
            .trim(),
    )
    .expect("row");
    assert_eq!(row["_meta"]["attempt"], 3);
    let calls = server.state.calls.lock().expect("calls");
    assert_eq!(calls[0].1["messages"][0]["role"], "user");
    assert_eq!(calls[0].1["response_format"]["type"], "json_object");
}

#[tokio::test]
async fn permanent_http_errors_are_not_retried() {
    for status in [400, 401, 403] {
        let server = server(vec![Reply {
            status,
            body: json!({"error":"PRIVATE_RESPONSE"}),
            delay: 0,
            retry_after: None,
        }])
        .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let p = http_pipeline(dir.path(), &server, 1, 1000, 1);
        let report = run_async(&p, CancellationToken::new()).await.expect("run");
        assert_eq!(report.status, RunStatus::Failed);
        assert_eq!(server.state.calls.lock().expect("calls").len(), 1);
        let dead = fs::read_to_string(&report.dead_letter_path).expect("dead");
        assert!(dead.contains("provider_http"));
        assert!(!dead.contains("PRIVATE_RESPONSE"));
    }
}

#[tokio::test]
async fn timeouts_exhaust_bounded_retries() {
    let server = server((0..3).map(|_| ok_reply(200)).collect()).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let p = http_pipeline(dir.path(), &server, 1, 20, 1);
    let report = run_async(&p, CancellationToken::new()).await.expect("run");
    assert_eq!(report.status, RunStatus::Failed);
    assert_eq!(report.statistics.provider_usage.requests, 3);
    let dead: Value = serde_json::from_str(
        fs::read_to_string(&report.dead_letter_path)
            .expect("dead")
            .trim(),
    )
    .expect("entry");
    assert_eq!(dead["diagnostic"]["category"], "provider_timeout");
    assert_eq!(dead["diagnostic"]["attempts"], 3);
}

#[tokio::test]
async fn concurrent_calls_are_bounded_and_output_is_in_source_order() {
    let server = server(
        (0..8)
            .map(|i| ok_reply(if i == 0 { 80 } else { 10 }))
            .collect(),
    )
    .await;
    let dir = tempfile::tempdir().expect("tempdir");
    let p = http_pipeline(dir.path(), &server, 8, 2000, 3);
    let report = run_async(&p, CancellationToken::new()).await.expect("run");
    assert!(report.succeeded());
    assert_eq!(server.state.peak.load(Ordering::SeqCst), 3);
    let rows = fs::read_to_string(&report.output_path).expect("output");
    let positions: Vec<u64> = rows
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).expect("row")["_meta"]["source_position"]
                .as_u64()
                .expect("position")
        })
        .collect();
    assert_eq!(positions, (1..=8).collect::<Vec<_>>());
    assert_eq!(report.sink_state.committed_source_position, 8);
    assert_eq!(report.sink_state.accepted_bytes, rows.len() as u64);
}

#[tokio::test]
async fn cancellation_stops_admission_and_preserves_report() {
    let server = server(vec![ok_reply(500), ok_reply(500)]).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let p = http_pipeline(dir.path(), &server, 100, 2000, 2);
    let cancellation = CancellationToken::new();
    let signal = cancellation.clone();
    let state = server.state.clone();
    let cancel = tokio::spawn(async move {
        while state.calls.lock().expect("calls").len() < 2 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        signal.cancel();
    });
    let report = tokio::time::timeout(Duration::from_secs(2), run_async(&p, cancellation))
        .await
        .expect("bounded cancellation")
        .expect("report");
    cancel.await.expect("task");
    assert_eq!(report.status, RunStatus::Cancelled);
    assert_eq!(report.statistics.source_records_total, 2);
    assert_eq!(report.sink_state.committed_source_position, 0);
    assert!(!report.output_path.exists());
    assert_eq!(
        read_report(&report.manifest_path).status,
        RunStatus::Cancelled
    );
}

#[tokio::test]
async fn existing_artifacts_are_never_overwritten() {
    for ending in ["", ".partial", ".manifest.json", ".rejected.jsonl"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let p = pipeline(dir.path(), good_records(1));
        let path = suffix(&p.output.path, ending);
        fs::write(&path, "existing").expect("fixture");
        assert!(run_async(&p, CancellationToken::new()).await.is_err());
        assert_eq!(fs::read_to_string(&path).expect("unchanged"), "existing");
    }
}

struct InterfereWithOutput {
    output: std::path::PathBuf,
    break_manifest: bool,
}
#[async_trait]
impl LlmProvider for InterfereWithOutput {
    async fn generate(
        &self,
        _: GenerateRequest<'_>,
        _: &CancellationToken,
    ) -> synthflow::Result<GenerateResponse> {
        if self.break_manifest {
            let manifest = suffix(&self.output, ".manifest.json");
            fs::remove_file(&manifest).expect("remove fixture manifest");
            fs::create_dir(&manifest).expect("replace fixture manifest with directory");
        } else {
            fs::write(&self.output, "concurrent writer").expect("race fixture");
        }
        Ok(GenerateResponse {
            text: "{\"answer\":\"ok\"}".into(),
            model: "test".into(),
            attempts: 1,
        })
    }
}

#[tokio::test]
async fn publish_race_never_clobbers_another_writers_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = pipeline(dir.path(), good_records(1));
    let report = run_with_provider(
        &p,
        Arc::new(InterfereWithOutput {
            output: p.output.path.clone(),
            break_manifest: false,
        }),
        CancellationToken::new(),
    )
    .await
    .expect("report");
    assert_eq!(report.status, RunStatus::Failed);
    assert_eq!(
        fs::read_to_string(&p.output.path).expect("output"),
        "concurrent writer"
    );
    assert!(report.partial_path.exists());
}

#[tokio::test]
async fn manifest_write_failure_preserves_data_and_returns_failure_report() {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = pipeline(dir.path(), good_records(1));
    let report = run_with_provider(
        &p,
        Arc::new(InterfereWithOutput {
            output: p.output.path.clone(),
            break_manifest: true,
        }),
        CancellationToken::new(),
    )
    .await
    .expect("report");
    assert_eq!(report.status, RunStatus::Failed);
    assert!(!report.output_path.exists());
    assert!(!report.errors.is_empty());
    // Never truncate below a possibly renamed manifest's durable byte position.
    assert_eq!(
        fs::metadata(&report.partial_path).expect("partial").len(),
        report.sink_state.accepted_bytes
    );
    assert_eq!(report.statistics.processed_records_total, 1);
}

#[tokio::test]
async fn invalid_http_envelopes_and_oversized_bodies_are_not_retried() {
    for body in [
        json!({"choices":[]}),
        json!({"content":"x".repeat(synthflow::spec::MAX_RECORD_BYTES)}),
    ] {
        let server = server(vec![Reply {
            body,
            ..ok_reply(0)
        }])
        .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let p = http_pipeline(dir.path(), &server, 1, 2000, 1);
        let report = run_async(&p, CancellationToken::new())
            .await
            .expect("report");
        assert_eq!(report.status, RunStatus::Failed);
        assert_eq!(report.statistics.provider_usage.requests, 1);
        assert_eq!(server.state.calls.lock().expect("calls").len(), 1);
    }
}

#[tokio::test]
async fn invalid_generated_json_keeps_actual_attempt_count_in_dead_letter() {
    let server = server(vec![
        Reply {
            status: 503,
            ..ok_reply(0)
        },
        Reply {
            body: json!({"choices":[{"message":{"content":"invalid JSON"}}]}),
            ..ok_reply(0)
        },
    ])
    .await;
    let dir = tempfile::tempdir().expect("tempdir");
    let p = http_pipeline(dir.path(), &server, 1, 2000, 1);
    let report = run_async(&p, CancellationToken::new())
        .await
        .expect("report");
    assert_eq!(report.statistics.structured_output_failed_total, 1);
    let dead: Value = serde_json::from_str(
        fs::read_to_string(report.dead_letter_path)
            .expect("dead")
            .trim(),
    )
    .expect("entry");
    assert_eq!(dead["diagnostic"]["attempts"], 2);
    assert_eq!(server.state.calls.lock().expect("calls").len(), 2);
}

#[tokio::test]
async fn provider_semaphore_limits_callers_outside_the_engine() {
    let server = server((0..6).map(|_| ok_reply(25)).collect()).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let p = http_pipeline(dir.path(), &server, 0, 2000, 2);
    let provider =
        synthflow::provider::create_provider(&p.providers["generator"]).expect("provider");
    let data = json!({});
    let cancellation = CancellationToken::new();
    let futures = (0..6).map(|_| {
        provider.generate(
            GenerateRequest {
                prompt: "test",
                record: &data,
            },
            &cancellation,
        )
    });
    let results = futures_util::future::join_all(futures).await;
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(server.state.peak.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn provider_backoff_is_cancellable() {
    let server = server(vec![Reply {
        status: 429,
        retry_after: Some("60"),
        ..ok_reply(0)
    }])
    .await;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut p = http_pipeline(dir.path(), &server, 0, 2000, 1);
    if let synthflow::spec::ProviderConfig::OpenaiCompatible { retry, .. } =
        p.providers.get_mut("generator").expect("config")
    {
        retry.max_delay_ms = 60_000;
    }
    let provider =
        synthflow::provider::create_provider(&p.providers["generator"]).expect("provider");
    let token = CancellationToken::new();
    let cancelled = token.clone();
    let state = server.state.clone();
    let task = tokio::spawn(async move {
        while state.calls.lock().expect("calls").is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancelled.cancel();
    });
    let data = json!({});
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        provider.generate(
            GenerateRequest {
                prompt: "test",
                record: &data,
            },
            &token,
        ),
    )
    .await
    .expect("cancel bounded");
    assert!(matches!(result, Err(synthflow::Error::Cancelled)));
    task.await.expect("cancel task");
    assert_eq!(server.state.calls.lock().expect("calls").len(), 1);
}
