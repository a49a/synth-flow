use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
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
