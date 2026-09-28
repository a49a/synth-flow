use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::sync::{Mutex, atomic::AtomicUsize};
use synthflow::{
    Error, Pipeline,
    provider::{GenerateRequest, create_provider},
};
use tokio_util::sync::CancellationToken;

fn pipeline(rate_limit: Value) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "ratelimit"},
        "source": {"type": "inline", "records": []},
        "providers": {"generator": {
            "type": "openai_compatible",
            "base_url": "http://127.0.0.1:9/v1",
            "model": "limited",
            "rate_limit": rate_limit
        }},
        "generate": {
            "provider": "generator", "prompt": "p",
            "output_schema": {"type": "object"}
        },
        "output": {"format": "jsonl", "path": "unused.jsonl"}
    }))
    .expect("test spec");
    spec.output.path = std::env::temp_dir().join("synthflow-ratelimit-unused.jsonl");
    spec
}

#[test]
fn rate_limit_configuration_is_validated() {
    assert!(
        pipeline(json!({"requests_per_minute": 60}))
            .validate()
            .is_ok()
    );
    assert!(
        pipeline(json!({"tokens_per_minute": 1000}))
            .validate()
            .is_ok()
    );
    for bad in [
        json!({}),
        json!({"requests_per_minute": 0}),
        json!({"tokens_per_minute": 0, "requests_per_minute": 5}),
    ] {
        let error = pipeline(bad).validate().expect_err("invalid rate limit");
        assert!(
            matches!(error, Error::Configuration(ref message) if message.contains("rate_limit")),
            "{error:?}"
        );
    }
}

#[tokio::test]
async fn generous_limits_do_not_disturb_normal_requests() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            Json(json!({
                "choices": [{"message": {"content": "{\"answer\":\"ok\"}"}}],
                "model": "limited",
                "usage": {"prompt_tokens": 10, "completion_tokens": 5}
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server");
    });
    let mut spec = pipeline(json!({"requests_per_minute": 100000, "tokens_per_minute": 1000000}));
    if let Some(config) = spec.providers.get_mut("generator") {
        let url = format!("http://127.0.0.1:{port}/v1");
        if let synthflow::spec::ProviderConfig::OpenaiCompatible { base_url, .. } = config {
            *base_url = url;
        }
    }
    let provider = create_provider(&spec.providers["generator"]).expect("provider");
    let record = json!({});
    let response = provider
        .generate(
            GenerateRequest {
                prompt: "test",
                record: &record,
                feedback: None,
                generated: None,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("generate");
    server.abort();
    assert_eq!(response.text, r#"{"answer":"ok"}"#);
    assert_eq!(response.prompt_tokens, Some(10));
    assert_eq!(response.completion_tokens, Some(5));
    assert_eq!(provider.statistics().requests, 1);
}

#[derive(Default)]
struct ReplyServer {
    statuses: Mutex<Vec<u16>>,
    requests: AtomicUsize,
}
async fn reply_server(statuses: Vec<u16>) -> (axum::Router, std::sync::Arc<ReplyServer>) {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let state = std::sync::Arc::new(ReplyServer {
        statuses: Mutex::new(statuses),
        requests: AtomicUsize::new(0),
    });
    let handler_state = state.clone();
    let router = axum::Router::new().route(
        "/v1/chat/completions",
        axum::routing::post(move || {
            let state = handler_state.clone();
            async move {
                let n = state
                    .requests
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let status = state
                    .statuses
                    .lock()
                    .expect("statuses")
                    .get(n)
                    .copied()
                    .unwrap_or(200);
                if status == 200 {
                    axum::Json(json!({
                        "choices": [{"message": {"content": "{\"answer\":\"ok\"}"}}],
                        "model": "limited"
                    }))
                    .into_response()
                } else {
                    (StatusCode::from_u16(status).expect("status"), "busy").into_response()
                }
            }
        }),
    );
    (router, state)
}

#[tokio::test]
async fn every_retry_passes_the_request_rate_limit() {
    use std::sync::atomic::Ordering;
    use synthflow::provider::create_provider;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (router, server) = reply_server(vec![503, 503, 200]).await;
    tokio::spawn(async move { axum::serve(listener, router).await.expect("server") });

    let mut spec = pipeline(json!({"requests_per_minute": 2}));
    spec.providers.insert(
        "generator".into(),
        serde_json::from_value(json!({
            "type": "openai_compatible",
            "base_url": format!("http://127.0.0.1:{port}/v1"),
            "model": "limited",
            "rate_limit": {"requests_per_minute": 2},
            "retry": {"max_attempts": 3, "initial_delay_ms": 1, "max_delay_ms": 1}
        }))
        .expect("provider"),
    );
    let provider = create_provider(&spec.providers["generator"]).expect("provider");
    let token = CancellationToken::new();
    let cancelled = token.clone();
    let watcher_state = server.clone();
    tokio::spawn(async move {
        while watcher_state.requests.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancelled.cancel();
    });
    let record = json!({});
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        provider.generate(
            GenerateRequest {
                prompt: "test",
                record: &record,
                feedback: None,
                generated: None,
            },
            &token,
        ),
    )
    .await
    .expect("bounded");
    assert!(matches!(response, Err(Error::Cancelled)));
    assert_eq!(
        server.requests.load(Ordering::SeqCst),
        2,
        "the third attempt must wait for a new rate-limit window"
    );
}

#[tokio::test]
async fn rate_limit_waits_are_cancellable() {
    use std::sync::atomic::Ordering;
    use synthflow::provider::create_provider;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (router, server) = reply_server(vec![503, 503, 503]).await;
    tokio::spawn(async move { axum::serve(listener, router).await.expect("server") });

    let mut spec = pipeline(json!({"requests_per_minute": 1})); // next slot in 60s
    spec.providers.insert(
        "generator".into(),
        serde_json::from_value(json!({
            "type": "openai_compatible",
            "base_url": format!("http://127.0.0.1:{port}/v1"),
            "model": "limited",
            "rate_limit": {"requests_per_minute": 1},
            "retry": {"max_attempts": 2, "initial_delay_ms": 1, "max_delay_ms": 1}
        }))
        .expect("provider"),
    );
    let provider = create_provider(&spec.providers["generator"]).expect("provider");
    let token = CancellationToken::new();
    let cancelled = token.clone();
    let watcher_state = server.clone();
    tokio::spawn(async move {
        loop {
            if watcher_state.requests.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        cancelled.cancel();
    });
    let record = json!({});
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        provider.generate(
            GenerateRequest {
                prompt: "test",
                record: &record,
                feedback: None,
                generated: None,
            },
            &token,
        ),
    )
    .await
    .expect("cancel must interrupt the rate wait");
    assert!(matches!(result, Err(synthflow::Error::Cancelled)));
    assert_eq!(server.requests.load(Ordering::SeqCst), 1);
}
