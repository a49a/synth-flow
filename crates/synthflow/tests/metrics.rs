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
