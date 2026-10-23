use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use synthflow::{
    Pipeline,
    provider::{GenerateRequest, GenerateResponse, LlmProvider},
    run_with_provider,
};
use tokio_util::sync::CancellationToken;

/// Sleeps per call so latency percentiles are observable, and reports usage.
struct SlowProvider {
    delay_ms: u64,
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
}
#[async_trait]
impl LlmProvider for SlowProvider {
    async fn generate(
        &self,
        request: GenerateRequest<'_>,
        _cancellation: &CancellationToken,
    ) -> synthflow::Result<GenerateResponse> {
        tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
        if request
            .record
            .get("fail")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(synthflow::Error::Provider {
                kind: synthflow::error::ProviderErrorKind::Timeout,
                status: None,
                attempts: 1,
            });
        }
        Ok(GenerateResponse {
            text: r#"{"answer": "ok"}"#.into(),
            model: "slow".into(),
            attempts: 1,
            prompt_tokens: self.prompt_tokens,
            completion_tokens: self.completion_tokens,
        })
    }
    fn concurrency(&self) -> usize {
        1
    }
}

fn fixture(dir: &tempfile::TempDir, records: Value, pricing: Option<Value>) -> Pipeline {
    let mut spec: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "metrics"},
        "source": {"type": "inline", "records": records},
        "providers": {"generator": {"type": "mock", "response": "{\"answer\": \"ok\"}"}},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        },
        "output": {"format": "jsonl", "path": dir.path().join("result.jsonl")},
        "pricing": pricing
    }))
    .expect("test spec");
    spec.output.path = dir.path().join("result.jsonl");
    spec
}

#[tokio::test]
async fn latency_percentiles_reflect_provider_calls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([{"topic": "a"}, {"topic": "b"}, {"topic": "c"}]),
        None,
    );
    let report = run_with_provider(
        &spec,
        Arc::new(SlowProvider {
            delay_ms: 20,
            prompt_tokens: None,
            completion_tokens: None,
        }),
        CancellationToken::new(),
    )
    .await
    .expect("run");
    assert!(report.succeeded());
    let latency = &report.statistics.latency;
    assert!(latency.mean_ms >= 15.0, "mean {latency:?}");
    assert!(latency.p50_ms >= 15.0, "p50 {latency:?}");
    assert!(latency.p99_ms >= latency.p50_ms);
    assert!(latency.p95_ms >= latency.p50_ms);
    assert!(report.statistics.estimated_cost_usd.is_none());
}

#[tokio::test]
async fn pricing_estimates_cost_from_reported_usage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([{"topic": "a"}, {"topic": "b"}]),
        Some(json!({"input_usd_per_mtok": 10.0, "output_usd_per_mtok": 20.0})),
    );
    let report = run_with_provider(
        &spec,
        Arc::new(SlowProvider {
            delay_ms: 0,
            prompt_tokens: Some(1000),
            completion_tokens: Some(500),
        }),
        CancellationToken::new(),
    )
    .await
    .expect("run");
    assert!(report.succeeded());
    let cost = report.statistics.estimated_cost_usd.expect("cost");
    // 2 * (1000 * 10 + 500 * 20) / 1e6 = 0.04
    assert!((cost - 0.04).abs() < 1e-9, "cost {cost}");
}

#[test]
fn pricing_must_be_finite_and_non_negative() {
    let dir = tempfile::tempdir().expect("tempdir");
    let spec = fixture(
        &dir,
        json!([]),
        Some(json!({"input_usd_per_mtok": -1.0, "output_usd_per_mtok": 2.0})),
    );
    assert!(spec.validate().is_err());
    let spec = fixture(
        &dir,
        json!([]),
        Some(json!({"input_usd_per_mtok": 0.0, "output_usd_per_mtok": 2.0})),
    );
    assert!(spec.validate().is_ok());
}

#[tokio::test]
async fn latency_counts_failed_calls_and_judge_calls() {
    let dir = tempfile::tempdir().unwrap();
    let mut spec = fixture(&dir, json!([{"topic":"a","fail":true},{"topic":"b"}]), None);
    spec.providers.insert(
        "judge".into(),
        serde_json::from_value(json!({"type":"mock","response":"{\"score\":1}"})).unwrap(),
    );
    spec.judge = Some(
        serde_json::from_value(json!({"provider":"judge","prompt":"score","min_score":0.5}))
            .unwrap(),
    );
    let report = run_with_provider(
        &spec,
        Arc::new(SlowProvider {
            delay_ms: 20,
            prompt_tokens: None,
            completion_tokens: None,
        }),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.statistics.generation_failed_total, 1);
    assert_eq!(report.statistics.judge_requests_total, 1);
    assert_eq!(report.statistics.latency_calls_total, 3);
    assert!(report.statistics.latency.mean_ms >= 10.0);
}

#[tokio::test]
async fn separate_http_providers_report_usage_and_different_prices() {
    use axum::{Json, Router, routing::post};
    let app = Router::new()
        .route(
            "/generator/chat/completions",
            post(|| async {
                Json(
                    json!({"choices":[{"message":{"content":"{\"answer\":\"ok\",\"score\":1}"}}],
                "usage":{"prompt_tokens":1000,"completion_tokens":500}}),
                )
            }),
        )
        .route(
            "/judge/chat/completions",
            post(|| async {
                Json(json!({"choices":[{"message":{"content":"{\"score\":1}"}}],
                "usage":{"prompt_tokens":2000,"completion_tokens":100}}))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let mut spec = fixture(&dir, json!([{"topic":"a"}]), None);
    for name in ["generator", "judge"] {
        spec.providers.insert(
            name.into(),
            serde_json::from_value(json!({
                "type":"openai_compatible", "base_url":format!("http://{addr}/{name}"), "model":name
            }))
            .unwrap(),
        );
    }
    spec.judge = Some(
        serde_json::from_value(json!({"provider":"judge","prompt":"score","min_score":0.5}))
            .unwrap(),
    );
    spec.provider_pricing = serde_json::from_value(json!({
        "generator":{"input_usd_per_mtok":10,"output_usd_per_mtok":20},
        "judge":{"input_usd_per_mtok":1,"output_usd_per_mtok":2}
    }))
    .unwrap();
    let report = synthflow::run_async(&spec, CancellationToken::new())
        .await
        .unwrap();
    assert!(report.succeeded(), "{:?}", report.errors);
    let stats = report.statistics;
    assert_eq!(stats.provider_usage.requests, 2);
    assert_eq!(stats.provider_usage.prompt_tokens, 3000);
    assert_eq!(stats.provider_usage_by_name["judge"].requests, 1);
    assert_eq!(
        stats.returned_tokens_by_provider["judge"].completion_tokens,
        100
    );
    assert!((stats.estimated_cost_by_provider["generator"] - 0.02).abs() < 1e-9);
    assert!((stats.estimated_cost_usd.unwrap() - 0.0222).abs() < 1e-9);
    spec.output.path = dir.path().join("shared.jsonl");
    spec.judge.as_mut().unwrap().provider = "generator".into();
    let shared = synthflow::run_async(&spec, CancellationToken::new())
        .await
        .unwrap();
    server.abort();
    assert!(shared.succeeded(), "{:?}", shared.errors);
    assert_eq!(shared.statistics.provider_usage_by_name.len(), 1);
    assert_eq!(shared.statistics.provider_usage.requests, 2);
    assert_eq!(shared.statistics.provider_usage.prompt_tokens, 2000);
    assert!((shared.statistics.estimated_cost_usd.unwrap() - 0.04).abs() < 1e-9);
}

#[tokio::test]
async fn missing_provider_price_does_not_report_a_partial_total() {
    let dir = tempfile::tempdir().unwrap();
    let mut spec = fixture(&dir, json!([{"topic":"a"}]), None);
    spec.providers.insert(
        "judge".into(),
        serde_json::from_value(json!({"type":"mock","response":"{\"score\":1}"})).unwrap(),
    );
    spec.judge = Some(
        serde_json::from_value(json!({"provider":"judge","prompt":"score","min_score":0.5}))
            .unwrap(),
    );
    spec.provider_pricing.insert(
        "generator".into(),
        serde_json::from_value(json!({"input_usd_per_mtok":1,"output_usd_per_mtok":1})).unwrap(),
    );
    let report = synthflow::run_async(&spec, CancellationToken::new())
        .await
        .unwrap();
    assert!(report.succeeded());
    assert!(report.statistics.estimated_cost_usd.is_none());
    assert_eq!(report.statistics.estimated_cost_by_provider.len(), 1);
}

#[test]
fn new_resource_and_price_configuration_is_validated() {
    let dir = tempfile::tempdir().unwrap();
    let mut spec = fixture(&dir, json!([]), None);
    let serialized = serde_json::to_value(&spec).unwrap();
    assert!(serialized.get("provider_pricing").is_none());
    assert!(serialized.get("dedup_max_bytes").is_none());
    assert!(serialized["output"].get("commit_every").is_none());
    for interval in [0, 65537] {
        spec.output.commit_every = interval;
        assert!(spec.validate().is_err());
    }
    spec.output.commit_every = 1;
    spec.dedup_max_bytes = 0;
    assert!(spec.validate().is_err());
    spec.dedup_max_bytes = 128;
    spec.provider_pricing.insert(
        "unknown".into(),
        serde_json::from_value(json!({"input_usd_per_mtok":1,"output_usd_per_mtok":1})).unwrap(),
    );
    assert!(spec.validate().is_err());
    let price = spec.provider_pricing.remove("unknown").unwrap();
    spec.provider_pricing.insert("generator".into(), price);
    assert!(spec.validate().is_ok());
    spec.provider_pricing
        .get_mut("generator")
        .unwrap()
        .input_usd_per_mtok = f64::INFINITY;
    assert!(spec.validate().is_err());
}
