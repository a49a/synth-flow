use async_trait::async_trait;
use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    fs,
    sync::{Arc, Mutex},
};
use synthflow::{
    Pipeline, Result,
    provider::{GenerateRequest, GenerateResponse, LlmProvider},
    run_with_provider,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

struct ScriptedProvider {
    responses: Mutex<VecDeque<&'static str>>,
}
#[async_trait]
impl LlmProvider for ScriptedProvider {
    async fn generate(
        &self,
        _request: GenerateRequest<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<GenerateResponse> {
        let text = self
            .responses
            .lock()
            .expect("script")
            .pop_front()
            .expect("script exhausted")
            .to_owned();
        Ok(GenerateResponse {
            text,
            model: "scripted".into(),
            attempts: 1,
            prompt_tokens: None,
            completion_tokens: None,
        })
    }
    fn concurrency(&self) -> usize {
        4
    }
}

fn fixture(dir: &TempDir, regenerate: u32) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "regeneration"},
        "source": {"type": "inline", "records": [{"topic": "ownership"}]},
        "providers": {"generator": {"type": "mock", "response": "unused"}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}},
            "regenerate_on_invalid": regenerate
        },
        "output": {"format": "jsonl", "path": "result.jsonl"}
    }))
    .expect("test spec");
    spec.output.path = dir.path().join("result.jsonl");
    spec
}

fn rows(spec: &Pipeline) -> Vec<Value> {
    fs::read_to_string(&spec.output.path)
        .expect("output")
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSON row"))
        .collect()
}

#[tokio::test]
async fn invalid_then_valid_output_is_repaired_and_accepted() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, 2);
    let provider = Arc::new(ScriptedProvider {
        responses: Mutex::new(VecDeque::from([
            "not json at all",
            r#"{"answer": 42}"#,
            r#"{"answer": "ok"}"#,
        ])),
    });
    let report = run_with_provider(&spec, provider, CancellationToken::new())
        .await
        .expect("run");
    assert!(report.succeeded());
    assert_eq!(report.statistics.accepted_records_total, 1);
    assert_eq!(report.statistics.regeneration_attempts_total, 2);
    assert_eq!(report.statistics.rejected_records_total, 0);
    let row = &rows(&spec)[0];
    assert_eq!(row["generated"]["answer"], "ok");
    assert_eq!(row["_meta"]["attempt"], 3);
}

#[tokio::test]
async fn without_regeneration_invalid_output_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, 0);
    let provider = Arc::new(ScriptedProvider {
        responses: Mutex::new(VecDeque::from(["not json"])),
    });
    let report = run_with_provider(&spec, provider, CancellationToken::new())
        .await
        .expect("run");
    assert!(
        !report.succeeded(),
        "all records rejected must fail the run"
    );
    assert_eq!(report.statistics.rejected_records_total, 1);
    let dead = fs::read_to_string(
        spec.output
            .path
            .with_file_name("result.jsonl.rejected.jsonl"),
    )
    .expect("dead letter");
    let line: Value = serde_json::from_str(dead.lines().next().expect("line")).expect("json");
    assert_eq!(line["diagnostic"]["category"], "structured_output");
}

#[tokio::test]
async fn exhausted_regenerations_report_the_final_schema_error() {
    let dir = TempDir::new().expect("tempdir");
    let spec = fixture(&dir, 1);
    let provider = Arc::new(ScriptedProvider {
        responses: Mutex::new(VecDeque::from([
            r#"{"wrong": true}"#,
            r#"{"still_wrong": true}"#,
        ])),
    });
    let report = run_with_provider(&spec, provider, CancellationToken::new())
        .await
        .expect("run");
    assert!(!report.succeeded());
    assert_eq!(report.statistics.regeneration_attempts_total, 1);
    assert_eq!(report.statistics.validation_failed_total, 1);
    let errors = &report.errors;
    assert!(
        errors
            .iter()
            .any(|e| e.category == "failure_policy" || e.category == "validation")
    );
}

#[derive(Default)]
struct Capture {
    bodies: Mutex<Vec<String>>,
}
async fn capture_handler(State(state): State<Arc<Capture>>, body: String) -> Json<Value> {
    let mut bodies = state.bodies.lock().expect("bodies");
    let replies = [
        r#"{"choices":[{"message":{"content":"{\"wrong\":1}"}}],"model":"repair-model"}"#,
        r#"{"choices":[{"message":{"content":"{\"answer\":\"fixed\"}"}}],"model":"repair-model"}"#,
    ];
    let reply = replies[bodies.len().min(1)].to_owned();
    bodies.push(body);
    Json(serde_json::from_str::<Value>(&reply).expect("static reply"))
}

#[tokio::test]
async fn http_provider_receives_repair_feedback_in_the_user_message() {
    let dir = TempDir::new().expect("tempdir");
    let mut spec = fixture(&dir, 1);
    let state = Arc::new(Capture::default());
    let app = Router::new()
        .route("/v1/chat/completions", post(capture_handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server");
    });
    spec.providers.insert(
        "generator".into(),
        serde_json::from_value(json!({
            "type": "openai_compatible",
            "base_url": format!("http://127.0.0.1:{port}/v1"),
            "model": "repair-model",
            "retry": {"max_attempts": 1, "initial_delay_ms": 1, "max_delay_ms": 1}
        }))
        .expect("provider"),
    );
    let report = synthflow::run_async(&spec, CancellationToken::new())
        .await
        .expect("run");
    server.abort();
    assert!(report.succeeded(), "{:?}", report.errors);
    let bodies = state.bodies.lock().expect("bodies");
    assert_eq!(bodies.len(), 2);
    let second: Value = serde_json::from_str(&bodies[1]).expect("body");
    let content = second["messages"][0]["content"].as_str().expect("content");
    assert!(content.contains("Explain ownership"));
    assert!(content.contains("rejected"));
    assert!(content.contains("schema validation failed"));
}
