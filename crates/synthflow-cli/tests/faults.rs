use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
use serde_json::{Value, json};
use std::{
    fs,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct StateData {
    calls: AtomicUsize,
    block_after_first: bool,
}
async fn handler(
    State(state): State<Arc<StateData>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    assert_eq!(
        headers.get("authorization").expect("auth"),
        "Bearer fixture-secret-key"
    );
    assert_eq!(body["model"], "test-model");
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    if state.block_after_first && call > 0 {
        std::future::pending::<()>().await;
    }
    Json(json!({"choices":[{"message":{"content":"{\"answer\":\"ok\"}"}}]}))
}
async fn start(
    dir: &std::path::Path,
    block: bool,
) -> (
    std::process::Child,
    Arc<StateData>,
    tokio::task::JoinHandle<()>,
) {
    let state = Arc::new(StateData {
        calls: AtomicUsize::new(0),
        block_after_first: block,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    let app = Router::new()
        .route("/v1/chat/completions", post(handler))
        .with_state(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server");
    });
    let pipeline = json!({
        "version":1,"dataset":{"name":"cli"},"source":{"type":"inline","records":[{"topic":"one"},{"topic":"two"}]},
        "providers":{"generator":{"type":"openai_compatible","base_url":format!("http://{address}/v1"),"model":"test-model","api_key_env":"SYNTHFLOW_TEST_KEY","concurrency":1}},
        "generate":{"provider":"generator","prompt":"{{ record.topic }}","output_schema":{"type":"object","required":["answer"]}},
        "output":{"format":"jsonl","path":"result.jsonl"}
    });
    let config = dir.join("pipeline.yaml");
    fs::write(&config, pipeline.to_string()).expect("write");
    let child = Command::new(env!("CARGO_BIN_EXE_synthflow"))
        .arg("run")
        .arg(config)
        .env("SYNTHFLOW_TEST_KEY", "fixture-secret-key")
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    (child, state, server)
}
async fn wait(mut child: std::process::Child) -> std::process::Output {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait().expect("poll").is_some() {
            return child.wait_with_output().expect("output");
        }
        if tokio::time::Instant::now() > deadline {
            child.kill().expect("kill stuck fixture");
            panic!("CLI did not terminate");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn wait_for_second_request(child: &mut std::process::Child, state: &StateData) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while state.calls.load(Ordering::SeqCst) < 2 {
        if tokio::time::Instant::now() > deadline {
            child.kill().expect("kill fixture");
            panic!("requests did not start");
        }
        assert!(
            child.try_wait().expect("poll").is_none(),
            "CLI exited early"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_authentication_uses_env_and_keeps_secrets_out_of_artifacts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, state, server) = start(dir.path(), false).await;
    let result = wait(child).await;
    server.abort();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(state.calls.load(Ordering::SeqCst), 2);
    assert!(!String::from_utf8_lossy(&result.stdout).contains("fixture-secret-key"));
    assert!(!String::from_utf8_lossy(&result.stderr).contains("fixture-secret-key"));
    for name in [
        "result.jsonl",
        "result.jsonl.manifest.json",
        "result.jsonl.rejected.jsonl",
    ] {
        assert!(
            !fs::read_to_string(dir.path().join(name))
                .expect("artifact")
                .contains("fixture-secret-key")
        );
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigint_flushes_and_reports_cancelled_with_exit_130() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, state, server) = start(dir.path(), true).await;
    wait_for_second_request(&mut child, &state).await;
    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .expect("signal")
            .success()
    );
    let output = wait(child).await;
    server.abort();
    assert_eq!(output.status.code(), Some(130));
    let report: Value = serde_json::from_slice(&output.stdout).expect("report");
    assert_eq!(report["status"], "cancelled");
    assert_eq!(report["sink_state"]["committed_source_position"], 1);
    assert!(!dir.path().join("result.jsonl").exists());
    let manifest: Value = serde_json::from_slice(
        &fs::read(dir.path().join("result.jsonl.manifest.json")).expect("manifest"),
    )
    .expect("json");
    assert_eq!(manifest["status"], "cancelled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_termination_leaves_only_partial_and_last_durable_position() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, state, server) = start(dir.path(), true).await;
    wait_for_second_request(&mut child, &state).await;
    child.kill().expect("terminate process");
    let output = wait(child).await;
    server.abort();
    assert!(!output.status.success());
    assert!(!dir.path().join("result.jsonl").exists());
    let manifest: Value = serde_json::from_slice(
        &fs::read(dir.path().join("result.jsonl.manifest.json")).expect("manifest"),
    )
    .expect("json");
    assert_eq!(manifest["status"], "running"); // Last durable observation, not a live-process assertion.
    assert_eq!(manifest["sink_state"]["committed_source_position"], 1);
    let partial = fs::read_to_string(dir.path().join("result.jsonl.partial")).expect("partial");
    assert_eq!(partial.lines().count(), 1);
    assert_eq!(manifest["sink_state"]["accepted_bytes"], partial.len());
}

#[test]
fn strict_cli_failure_still_prints_statistics_and_preflight_is_json() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = dir.path().join("p.yaml");
    fs::write(&config, json!({
        "version":1,"dataset":{"name":"strict"},"source":{"type":"inline","records":[{"topic":1},{"topic":2}]},
        "providers":{"mock":{"type":"mock","response":"bad"}},
        "generate":{"provider":"mock","prompt":"{{ topic }}","output_schema":{"type":"object"}},
        "output":{"format":"jsonl","path":"result.jsonl"}
    }).to_string()).expect("write");
    let result = Command::new(env!("CARGO_BIN_EXE_synthflow"))
        .arg("run")
        .arg("--strict")
        .arg(&config)
        .output()
        .expect("CLI");
    assert!(!result.status.success());
    let report: Value = serde_json::from_slice(&result.stdout).expect("report");
    assert_eq!(report["status"], "failed");
    assert_eq!(report["rejected_records_total"], 1);
    let result = Command::new(env!("CARGO_BIN_EXE_synthflow"))
        .arg("run")
        .arg(dir.path().join("missing.yaml"))
        .output()
        .expect("CLI");
    assert!(!result.status.success());
    let report: Value = serde_json::from_slice(&result.stdout).expect("preflight");
    assert_eq!(report["preflight"], true);
}
