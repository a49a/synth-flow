use axum::{Json, Router, http::StatusCode, routing::post};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use synthflow::{
    Error, Pipeline,
    error::ProviderErrorKind,
    provider::{GenerateRequest, create_provider},
    spec::ProviderConfig,
};
use tokio_util::sync::CancellationToken;

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(responses: Vec<(StatusCode, Value)>) -> Server {
    let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let router = Router::new().route(
        "/v1/chat/completions",
        post(move |Json(body): Json<Value>| {
            captured.lock().unwrap().push(body);
            let response = responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request");
            async move { (response.0, Json(response.1)) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Server {
        url,
        requests,
        task,
    }
}
fn config(url: &str, options: Value) -> ProviderConfig {
    let mut value = json!({"type":"openai_compatible", "base_url":url, "model":"fixture"});
    value
        .as_object_mut()
        .unwrap()
        .extend(options.as_object().unwrap().clone());
    serde_json::from_value(value).unwrap()
}
fn reply(content: Value) -> Value {
    json!({"choices":[{"message":{"content":content},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":7,"completion_tokens":3}})
}
fn request<'a>(record: &'a Value, feedback: Option<&'a str>) -> GenerateRequest<'a> {
    GenerateRequest {
        prompt: "p",
        record,
        feedback,
        generated: None,
    }
}

#[tokio::test]
async fn request_parameters_and_all_output_modes_reach_the_server() {
    let server = server(vec![(StatusCode::OK, reply(json!("{}"))); 4]).await;
    let record = json!({});
    let cancel = CancellationToken::new();
    let schema = json!({"type":"json_schema","json_schema":{"name":"answer","strict":true,
        "schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}}});
    for format in [
        json!({"type":"json_object"}),
        json!({"type":"text"}),
        schema.clone(),
    ] {
        let provider = create_provider(&config(&server.url, json!({"max_completion_tokens":321,
            "estimated_completion_tokens":9,"temperature":0.5,"top_p":0.9,"seed":42,"response_format":format}))).unwrap();
        provider
            .generate(request(&record, Some("missing answer")), &cancel)
            .await
            .unwrap();
    }
    create_provider(&config(&server.url, json!({})))
        .unwrap()
        .generate(request(&record, None), &cancel)
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    for body in &requests[..3] {
        assert_eq!(body["max_completion_tokens"], 321);
        assert_eq!(body["temperature"], 0.5);
        assert_eq!(body["top_p"], 0.9);
        assert_eq!(body["seed"], 42);
        assert!(
            body["messages"][0]["content"]
                .as_str()
                .unwrap()
                .contains("missing answer")
        );
        assert!(body.get("estimated_completion_tokens").is_none());
    }
    assert_eq!(requests[0]["response_format"]["type"], "json_object");
    assert_eq!(requests[1]["response_format"]["type"], "text");
    assert_eq!(requests[2]["response_format"], schema);
    assert_eq!(
        requests[3]["response_format"],
        json!({"type":"json_object"})
    );
    for field in ["max_completion_tokens", "temperature", "top_p", "seed"] {
        assert!(requests[3].get(field).is_none());
    }
}

#[tokio::test]
async fn rejected_responses_keep_billable_usage_and_safe_error_categories() {
    let mut refusal = reply(Value::Null);
    refusal["choices"][0]["message"]["refusal"] = json!("private refusal body");
    let mut truncated = reply(json!("{}"));
    truncated["choices"][0]["finish_reason"] = json!("length");
    let mut filtered = reply(Value::Null);
    filtered["choices"][0]["finish_reason"] = json!("content_filter");
    let server = server(
        vec![reply(Value::Null), refusal, truncated, filtered]
            .into_iter()
            .map(|body| (StatusCode::OK, body))
            .collect(),
    )
    .await;
    let provider = create_provider(&config(&server.url, json!({}))).unwrap();
    let record = json!({});
    for (i, expected) in [
        ProviderErrorKind::InvalidResponse,
        ProviderErrorKind::Refusal,
        ProviderErrorKind::Truncated,
        ProviderErrorKind::Refusal,
    ]
    .into_iter()
    .enumerate()
    {
        let error = provider
            .generate(request(&record, None), &CancellationToken::new())
            .await
            .err()
            .unwrap();
        assert!(matches!(error, Error::Provider {ref kind, attempts:1, ..} if *kind == expected));
        assert!(!error.to_string().contains("private refusal"));
        let stats = provider.statistics();
        assert_eq!(stats.usage_reports, i as u64 + 1);
        assert_eq!(stats.prompt_tokens, 7 * (i as u64 + 1));
        assert_eq!(stats.completion_tokens, 3 * (i as u64 + 1));
    }
    assert_eq!(provider.statistics().requests, 4);
    assert_eq!(provider.statistics().retries, 0);
}

#[tokio::test]
async fn retry_usage_is_counted_once_per_physical_response() {
    let server = server(vec![
        (StatusCode::SERVICE_UNAVAILABLE, reply(Value::Null)),
        (StatusCode::OK, reply(json!("{}"))),
    ])
    .await;
    let provider = create_provider(&config(
        &server.url,
        json!({"retry":{
        "max_attempts":2,"initial_delay_ms":1,"max_delay_ms":1}}),
    ))
    .unwrap();
    let record = json!({});
    let result = provider
        .generate(request(&record, None), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.attempts, 2);
    assert_eq!(result.prompt_tokens, Some(7));
    let stats = provider.statistics();
    assert_eq!(
        (stats.requests, stats.retries, stats.usage_reports),
        (2, 1, 2)
    );
    assert_eq!((stats.prompt_tokens, stats.completion_tokens), (14, 6));
}

#[tokio::test]
async fn run_cost_includes_responses_without_usable_content() {
    let mut truncated = reply(json!("{}"));
    truncated["choices"][0]["finish_reason"] = json!("length");
    let server = server(vec![
        (StatusCode::OK, reply(Value::Null)),
        (StatusCode::OK, truncated),
    ])
    .await;
    let dir = tempfile::tempdir().unwrap();
    let pipeline: Pipeline = serde_json::from_value(json!({
        "version":1,"dataset":{"name":"failed-cost"},"source":{"type":"inline","records":[{},{}]},
        "providers":{"generator":config(&server.url, json!({}))},
        "generate":{"provider":"generator","prompt":"p","output_schema":{"type":"object"}},
        "output":{"format":"jsonl","path":dir.path().join("output.jsonl")},
        "pricing":{"input_usd_per_mtok":10,"output_usd_per_mtok":20}
    }))
    .unwrap();
    let report = synthflow::run_async(&pipeline, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.statistics.generation_failed_total, 2);
    assert_eq!(report.statistics.provider_usage.usage_reports, 2);
    assert_eq!(report.statistics.provider_usage.prompt_tokens, 14);
    assert_eq!(report.statistics.prompt_tokens_total, 0);
    assert!(report.statistics.returned_tokens_by_provider.is_empty());
    assert!((report.statistics.estimated_cost_usd.unwrap() - 0.00026).abs() < 1e-12);
}

#[tokio::test]
async fn invalid_content_still_corrects_its_token_reservation() {
    let server = server(vec![(StatusCode::OK, reply(Value::Null)); 2]).await;
    let provider = create_provider(&config(
        &server.url,
        json!({"max_completion_tokens":16,
        "rate_limit":{"tokens_per_minute":160}}),
    ))
    .unwrap();
    let record = json!({});
    for _ in 0..2 {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            provider.generate(request(&record, None), &CancellationToken::new()),
        )
        .await
        .expect("usage must release unused token reservation");
        assert!(matches!(
            result,
            Err(Error::Provider {
                kind: ProviderErrorKind::InvalidResponse,
                ..
            })
        ));
    }
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn output_budget_and_feedback_are_admitted_before_sending() {
    let record = json!({});
    for (options, feedback) in [
        (
            json!({"max_completion_tokens":1000,"rate_limit":{"tokens_per_minute":1000}}),
            None,
        ),
        (
            json!({"max_completion_tokens":1,"rate_limit":{"tokens_per_minute":160}}),
            Some("请完整输出包含所有必需字段的 JSON 对象"),
        ),
        (
            json!({"estimated_completion_tokens":1000,"rate_limit":{"tokens_per_minute":1000}}),
            None,
        ),
        (json!({"rate_limit":{"tokens_per_minute":1000}}), None),
        (
            json!({"max_completion_tokens":1,"rate_limit":{"tokens_per_minute":300},
                "response_format":{"type":"json_schema","json_schema":{"name":"answer", "schema":{
                    "type":"object","description":"x".repeat(400)}}}}),
            None,
        ),
    ] {
        let provider = create_provider(&config("http://127.0.0.1:9/v1", options)).unwrap();
        let result = provider
            .generate(request(&record, feedback), &CancellationToken::new())
            .await;
        assert!(matches!(result, Err(Error::Configuration(_))));
        assert_eq!(provider.statistics().requests, 0);
    }
}

#[test]
fn sampling_and_response_schema_configuration_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    let mut pipeline: Pipeline = serde_json::from_value(json!({
        "version":1,"dataset":{"name":"validation"},"source":{"type":"inline","records":[]},
        "providers":{"generator":config("http://127.0.0.1:9/v1", json!({}))},
        "generate":{"provider":"generator","prompt":"p","output_schema":{"type":"object"}},
        "output":{"format":"jsonl","path":dir.path().join("output.jsonl")}
    }))
    .unwrap();
    for options in [
        json!({"max_completion_tokens":0}),
        json!({"estimated_completion_tokens":0}),
        json!({"temperature":-0.1}),
        json!({"temperature":2.1}),
        json!({"top_p":-0.1}),
        json!({"top_p":1.1}),
        json!({"response_format":{"type":"json_schema","json_schema":{"name":"bad name","schema":{}}}}),
        json!({"response_format":{"type":"json_schema","json_schema":{"name":"good","schema":{"type":"nonexistent"}}}}),
    ] {
        pipeline
            .providers
            .insert("generator".into(), config("http://127.0.0.1:9/v1", options));
        assert!(pipeline.validate().is_err());
    }
    pipeline.providers.insert("generator".into(), config("http://127.0.0.1:9/v1", json!({
        "max_completion_tokens":1,"temperature":0,"top_p":1,"seed":-1,
        "response_format":{"type":"json_schema","json_schema":{"name":"valid_schema-1","schema":{"type":"object"}}}
    })));
    pipeline.validate().unwrap();
}
