use crate::{
    Error, Result,
    error::ProviderErrorKind,
    ratelimit::{SlidingWindow, estimate_tokens},
    spec::{MAX_RECORD_BYTES, ProviderConfig, RateLimitConfig, RetryPolicy},
    template,
};
use async_trait::async_trait;
use minijinja::Environment;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

pub struct GenerateRequest<'a> {
    pub prompt: &'a str,
    pub record: &'a Value,
    /// Structured-output repair feedback for regeneration attempts.
    pub feedback: Option<&'a str>,
}
pub struct GenerateResponse {
    pub text: String,
    pub model: String,
    pub attempts: u32,
    /// Reported token usage when the provider returns it.
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderStatistics {
    pub requests: u64,
    pub retries: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub latency_ms: u64,
}
#[derive(Default)]
struct Metrics {
    requests: AtomicU64,
    retries: AtomicU64,
    prompt: AtomicU64,
    completion: AtomicU64,
    latency: AtomicU64,
}
impl Metrics {
    fn snapshot(&self) -> ProviderStatistics {
        ProviderStatistics {
            requests: self.requests.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            prompt_tokens: self.prompt.load(Ordering::Relaxed),
            completion_tokens: self.completion.load(Ordering::Relaxed),
            latency_ms: self.latency.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn generate(
        &self,
        request: GenerateRequest<'_>,
        cancellation: &CancellationToken,
    ) -> Result<GenerateResponse>;
    fn concurrency(&self) -> usize {
        1
    }
    fn statistics(&self) -> ProviderStatistics {
        ProviderStatistics::default()
    }
}

pub fn create_provider(config: &ProviderConfig) -> Result<Arc<dyn LlmProvider>> {
    match config {
        ProviderConfig::Mock {
            response,
            concurrency,
        } => Ok(Arc::new(MockProvider::new(response, *concurrency)?)),
        ProviderConfig::OpenaiCompatible { .. } => Ok(Arc::new(OpenAiProvider::new(config)?)),
    }
}

pub struct MockProvider {
    env: Environment<'static>,
    concurrency: usize,
    metrics: Metrics,
}
impl MockProvider {
    pub fn new(response: &str, concurrency: usize) -> Result<Self> {
        let mut env = template::environment();
        env.add_template_owned("mock".to_owned(), response.to_owned())
            .map_err(|_| Error::Configuration("invalid mock response template".into()))?;
        Ok(Self {
            env,
            concurrency,
            metrics: Metrics::default(),
        })
    }
}
#[async_trait]
impl LlmProvider for MockProvider {
    async fn generate(
        &self,
        request: GenerateRequest<'_>,
        cancellation: &CancellationToken,
    ) -> Result<GenerateResponse> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let context = json!({"record": request.record, "prompt": request.prompt, "prompt_hash": blake3::hash(request.prompt.as_bytes()).to_hex().to_string(), "feedback": request.feedback});
        Ok(GenerateResponse {
            text: template::render(&self.env, "mock", &context)?,
            model: "synthflow-mock-v1".into(),
            attempts: 1,
            prompt_tokens: None,
            completion_tokens: None,
        })
    }
    fn concurrency(&self) -> usize {
        self.concurrency
    }
    fn statistics(&self) -> ProviderStatistics {
        self.metrics.snapshot()
    }
}

pub struct OpenAiProvider {
    client: reqwest::Client,
    endpoint: String,
    model: String,
    key: Option<String>,
    permits: Semaphore,
    concurrency: usize,
    timeout: Duration,
    retry: RetryPolicy,
    rpm: Option<SlidingWindow>,
    tpm: Option<SlidingWindow>,
    metrics: Metrics,
}
impl OpenAiProvider {
    pub fn new(config: &ProviderConfig) -> Result<Self> {
        let ProviderConfig::OpenaiCompatible {
            base_url,
            model,
            api_key_env,
            concurrency,
            timeout_ms,
            retry,
            rate_limit,
        } = config
        else {
            return Err(Error::Configuration(
                "expected openai_compatible provider".into(),
            ));
        };
        let key = api_key_env
            .as_ref()
            .map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .ok_or_else(|| {
                        Error::Configuration(format!(
                            "required API key environment variable {name} is missing or empty"
                        ))
                    })
            })
            .transpose()?;
        if let Some(key) = &key {
            reqwest::header::HeaderValue::from_str(&format!("Bearer {key}")).map_err(|_| {
                Error::Configuration("API key is not a valid HTTP header value".into())
            })?;
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Configuration("cannot initialize HTTP client".into()))?;
        let rate_limit = rate_limit
            .as_ref()
            .unwrap_or(&RateLimitConfig {
                requests_per_minute: None,
                tokens_per_minute: None,
            })
            .clone();
        let RateLimitConfig {
            requests_per_minute,
            tokens_per_minute,
        } = rate_limit;
        Ok(Self {
            client,
            endpoint: format!("{}/chat/completions", base_url.trim_end_matches('/')),
            model: model.clone(),
            key,
            permits: Semaphore::new(*concurrency),
            concurrency: *concurrency,
            timeout: Duration::from_millis(*timeout_ms),
            retry: retry.clone(),
            rpm: requests_per_minute.map(|v| SlidingWindow::new(v as u64)),
            tpm: tokens_per_minute.map(|v| SlidingWindow::new(v as u64)),
            metrics: Metrics::default(),
        })
    }

    async fn attempt(
        &self,
        request: &GenerateRequest<'_>,
        attempt: u32,
    ) -> std::result::Result<GenerateResponse, (Error, Option<Duration>)> {
        let failure = |kind, status| Error::Provider {
            kind,
            status,
            attempts: attempt,
        };
        // Repair feedback rides in the user message; the base prompt hash stays stable.
        let content = match request.feedback {
            Some(feedback) => format!(
                "{}\n\nYour previous reply was rejected: {feedback}\nReply again with a corrected JSON object only.",
                request.prompt
            ),
            None => request.prompt.to_owned(),
        };
        let mut builder = self.client.post(&self.endpoint).json(&json!({
            "model": self.model, "messages": [{"role":"user", "content": content}],
            "response_format": {"type":"json_object"}
        }));
        if let Some(key) = &self.key {
            builder = builder.bearer_auth(key);
        }
        let mut response = builder.send().await.map_err(|e| {
            (
                failure(
                    if e.is_timeout() {
                        ProviderErrorKind::Timeout
                    } else {
                        ProviderErrorKind::Network
                    },
                    None,
                ),
                None,
            )
        })?;
        if !response.status().is_success() {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| {
                    v.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
                        httpdate::parse_http_date(v)
                            .ok()
                            .and_then(|date| date.duration_since(SystemTime::now()).ok())
                    })
                });
            return Err((
                failure(ProviderErrorKind::Http, Some(response.status().as_u16())),
                retry_after,
            ));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| (failure(ProviderErrorKind::Network, None), None))?
        {
            if chunk.len() > MAX_RECORD_BYTES.saturating_sub(body.len()) {
                return Err((failure(ProviderErrorKind::ResponseTooLarge, None), None));
            }
            body.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&body)
            .map_err(|_| (failure(ProviderErrorKind::InvalidResponse, None), None))?;
        let text = value
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .ok_or_else(|| (failure(ProviderErrorKind::InvalidResponse, None), None))?;
        let prompt_tokens = value
            .pointer("/usage/prompt_tokens")
            .and_then(Value::as_u64);
        let completion_tokens = value
            .pointer("/usage/completion_tokens")
            .and_then(Value::as_u64);
        self.metrics
            .prompt
            .fetch_add(prompt_tokens.unwrap_or(0), Ordering::Relaxed);
        self.metrics
            .completion
            .fetch_add(completion_tokens.unwrap_or(0), Ordering::Relaxed);
        Ok(GenerateResponse {
            text: text.to_owned(),
            model: value
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or(&self.model)
                .to_owned(),
            attempts: attempt,
            prompt_tokens,
            completion_tokens,
        })
    }
}

pub fn retryable(error: &Error) -> bool {
    matches!(
        error,
        Error::Provider {
            kind: ProviderErrorKind::Timeout | ProviderErrorKind::Network,
            ..
        } | Error::Provider {
            kind: ProviderErrorKind::Http,
            status: Some(429 | 502 | 503 | 504),
            ..
        }
    )
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn generate(
        &self,
        request: GenerateRequest<'_>,
        cancellation: &CancellationToken,
    ) -> Result<GenerateResponse> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Error::Cancelled),
            result = async {
                // Rate limits reserve before the semaphore and backoff; a
                // cancelled or failed call only wastes its own reservation.
                let token_estimate = estimate_tokens(request.prompt);
                let admission = [
                    self.rpm.as_ref().map(|limiter| limiter.reserve(1)),
                    self.tpm.as_ref().map(|limiter| limiter.reserve(token_estimate)),
                ]
                .into_iter()
                .flatten()
                .max();
                if let Some(until) = admission {
                    let now = tokio::time::Instant::now();
                    if until > now {
                        tracing::debug!(
                            wait_ms = (until - now).as_millis() as u64,
                            event = "rate_limit_wait"
                        );
                        tokio::time::sleep_until(until).await;
                    }
                }
                for attempt in 1..=self.retry.max_attempts {
                    let permit = self.permits.acquire().await.map_err(|_| Error::Cancelled)?;
                    self.metrics.requests.fetch_add(1, Ordering::Relaxed);
                    if attempt > 1 { self.metrics.retries.fetch_add(1, Ordering::Relaxed); }
                    let started = Instant::now();
                    let result = tokio::time::timeout(self.timeout, self.attempt(&request, attempt)).await.unwrap_or(Err((Error::Provider { kind: ProviderErrorKind::Timeout, status: None, attempts: attempt }, None)));
                    self.metrics.latency.fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                    drop(permit); // Never hold provider capacity during backoff.
                    match result {
                        Ok(response) => {
                            // Replace the token estimate with reported usage.
                            if let Some(tpm) = &self.tpm
                                && let (Some(prompt), Some(completion)) =
                                    (response.prompt_tokens, response.completion_tokens)
                            {
                                tpm.adjust((prompt + completion) as i64 - token_estimate);
                            }
                            return Ok(response);
                        }
                        Err((error, retry_after)) => {
                            if !retryable(&error) || attempt == self.retry.max_attempts { return Err(error); }
                            let cap = self.retry.initial_delay_ms.saturating_mul(2u64.saturating_pow(attempt - 1)).min(self.retry.max_delay_ms);
                            let jitter = Duration::from_millis(rand::random_range((cap / 2).max(1)..=cap.max(1)));
                            let delay = retry_after.unwrap_or(jitter).min(Duration::from_millis(self.retry.max_delay_ms));
                            tracing::warn!(attempt, delay_ms = delay.as_millis(), event = "provider_request_retry");
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
                Err(Error::Configuration("retry policy must have at least one attempt".into()))
            } => result,
        }
    }
    fn concurrency(&self) -> usize {
        self.concurrency
    }
    fn statistics(&self) -> ProviderStatistics {
        self.metrics.snapshot()
    }
}
