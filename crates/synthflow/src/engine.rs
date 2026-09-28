use crate::{
    Error, Pipeline, Result,
    dedup::{DedupAdmission, DedupState},
    provider::{GenerateRequest, LlmProvider, ProviderStatistics, create_provider},
    record::{RecordMeta, record_id},
    run::{
        Artifacts, RunLock, RunReport, RunStatus, WriteResult, fingerprint, now_ms, save_report,
    },
    source::Source,
    template,
};
use futures_util::{StreamExt, stream::FuturesOrdered};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunStatistics {
    pub source_records_total: u64,
    pub source_failed_total: u64,
    pub processed_records_total: u64,
    pub generation_requests_total: u64,
    pub generation_success_total: u64,
    pub generation_failed_total: u64,
    #[serde(default)]
    pub regeneration_attempts_total: u64,
    #[serde(default)]
    pub prompt_tokens_total: u64,
    #[serde(default)]
    pub completion_tokens_total: u64,
    #[serde(default)]
    pub judge_requests_total: u64,
    #[serde(default)]
    pub judge_rejected_total: u64,
    #[serde(default)]
    pub judge_failed_total: u64,
    #[serde(default)]
    pub duplicate_records_total: u64,
    pub template_failed_total: u64,
    pub structured_output_failed_total: u64,
    pub validation_failed_total: u64,
    pub accepted_records_total: u64,
    pub rejected_records_total: u64,
    pub elapsed_seconds: f64,
    pub records_per_second: f64,
    #[serde(default)]
    pub latency: LatencySummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_cost_usd: Option<f64>,
    pub provider_usage: ProviderStatistics,
}

/// Generation wall-clock percentiles in milliseconds over accepted and
/// rejected calls alike; zeros when nothing reached a provider.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct LatencySummary {
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}
impl RunStatistics {
    fn merge(&mut self, other: &Self) {
        self.generation_requests_total += other.generation_requests_total;
        self.generation_success_total += other.generation_success_total;
        self.generation_failed_total += other.generation_failed_total;
        self.regeneration_attempts_total += other.regeneration_attempts_total;
        self.prompt_tokens_total += other.prompt_tokens_total;
        self.completion_tokens_total += other.completion_tokens_total;
        self.judge_requests_total += other.judge_requests_total;
        self.judge_rejected_total += other.judge_rejected_total;
        self.judge_failed_total += other.judge_failed_total;
        self.duplicate_records_total += other.duplicate_records_total;
        self.template_failed_total += other.template_failed_total;
        self.structured_output_failed_total += other.structured_output_failed_total;
        self.validation_failed_total += other.validation_failed_total;
    }
}

/// Compatibility entry point for synchronous callers. Async callers should use run_async.
pub fn run(pipeline: &Pipeline) -> Result<RunStatistics> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::Configuration(
            "use run_async inside a Tokio runtime".into(),
        ));
    }
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Configuration(format!("cannot create runtime: {e}")))?;
    let report = runtime.block_on(run_async(pipeline, CancellationToken::new()))?;
    if report.succeeded() {
        Ok(report.statistics)
    } else {
        Err(Error::RunFailed(
            report
                .errors
                .first()
                .map(|e| e.message.clone())
                .unwrap_or_else(|| "run did not complete".into()),
        ))
    }
}

pub async fn run_async(pipeline: &Pipeline, cancellation: CancellationToken) -> Result<RunReport> {
    pipeline.validate()?;
    let config = pipeline
        .providers
        .get(&pipeline.generate.provider)
        .ok_or_else(|| Error::Configuration("unknown provider".into()))?;
    run_with_provider(pipeline, create_provider(config)?, cancellation).await
}

/// Inject a transport-independent provider for tests or custom integrations.
/// Post-start errors return a report (including failed/cancelled state); preflight
/// failures return Err before a run can be established.
pub async fn run_with_provider(
    pipeline: &Pipeline,
    provider: Arc<dyn LlmProvider>,
    cancellation: CancellationToken,
) -> Result<RunReport> {
    pipeline.validate()?;
    let lock = RunLock::acquire(&crate::spec::normalize_path(&pipeline.output.path)?)?;
    execute(pipeline, provider, cancellation, None, lock).await
}

/// Continue the failed or cancelled run recorded in the output manifest.
/// Rejects changed pipelines or sources, truncates to the committed prefix,
/// rebuilds dedup state, and skips already committed source positions.
pub async fn resume_async(
    pipeline: &Pipeline,
    cancellation: CancellationToken,
) -> Result<RunReport> {
    pipeline.validate()?;
    let config = pipeline
        .providers
        .get(&pipeline.generate.provider)
        .ok_or_else(|| Error::Configuration("unknown provider".into()))?;
    resume_with_provider(pipeline, create_provider(config)?, cancellation).await
}

pub async fn resume_with_provider(
    pipeline: &Pipeline,
    provider: Arc<dyn LlmProvider>,
    cancellation: CancellationToken,
) -> Result<RunReport> {
    pipeline.validate()?;
    if !(1..=1024).contains(&provider.concurrency()) {
        return Err(Error::Configuration(
            "provider concurrency must be in 1..=1024".into(),
        ));
    }
    // The lock is taken before any checkpoint read so a loser re-reads the
    // winner's terminal state instead of truncating shared artifacts.
    let lock = RunLock::acquire(&crate::spec::normalize_path(&pipeline.output.path)?)?;
    let prior = prior_manifest(pipeline)?;
    // A prefix that already violates the active policies cannot be resumed
    // into a published dataset; fail before touching any artifact.
    if let Some(violation) = policies_violated(
        pipeline,
        prior.sink_state.committed_accepted_records,
        prior.sink_state.committed_rejected_records,
    ) {
        return Err(Error::FailurePolicy(format!(
            "{violation}; refusing to resume"
        )));
    }
    let source_hash = source_fingerprint(pipeline, &cancellation).await?;
    if source_hash != prior.source_fingerprint {
        return Err(Error::Configuration(
            "source changed since the recorded run; refusing to resume".into(),
        ));
    }
    let mut dedup = pipeline.dedup.as_ref().map(DedupState::new);
    if let Some(state) = &mut dedup
        && let Err(error) = committed_rows(&prior)
            .and_then(|rows| rows.iter().try_for_each(|row| state.restore(row)))
    {
        return Err(Error::Configuration(format!(
            "cannot rebuild dedup state from the committed prefix: {error}"
        )));
    }
    let context = ResumeContext {
        skip_through: prior.sink_state.committed_source_position,
        dedup,
        prior,
    };
    execute(pipeline, provider, cancellation, Some(context), lock).await
}

struct ResumeContext {
    skip_through: u64,
    dedup: Option<DedupState>,
    prior: crate::run::RunReport,
}

fn prior_manifest(pipeline: &Pipeline) -> Result<crate::run::RunReport> {
    let manifest = crate::spec::suffix(
        &crate::spec::normalize_path(&pipeline.output.path)?,
        ".manifest.json",
    );
    let bytes = std::fs::read(&manifest).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Error::Configuration(format!("no run manifest to resume: {}", manifest.display()))
        } else {
            Error::io(&manifest, e)
        }
    })?;
    let prior: crate::run::RunReport = serde_json::from_slice(&bytes)
        .map_err(|_| Error::Configuration("recorded manifest is not valid JSON".into()))?;
    if prior.pipeline_hash != pipeline.hash()? {
        return Err(Error::Configuration(
            "pipeline changed since the recorded run; use a new output path".into(),
        ));
    }
    match prior.status {
        crate::run::RunStatus::Failed | crate::run::RunStatus::Cancelled => Ok(prior),
        crate::run::RunStatus::Completed => Err(Error::Configuration(
            "the recorded run completed; nothing to resume".into(),
        )),
        crate::run::RunStatus::Running | crate::run::RunStatus::Publishing => {
            Err(Error::Configuration(
                "the recorded run has no terminal state; it may still be running".into(),
            ))
        }
    }
}

/// Evaluate every failure policy over dataset-wide counts. Resume runs
/// inherit the committed prefix, so violations that already exist must not
/// become publishable just because no new record was processed.
fn policies_violated(pipeline: &Pipeline, accepted: u64, rejected: u64) -> Option<&'static str> {
    if pipeline.errors.strict && rejected > 0 {
        return Some("strict mode rejects any failed record");
    }
    if pipeline
        .errors
        .max_failed_records
        .is_some_and(|max| rejected > max)
    {
        return Some("max_failed_records exceeded");
    }
    if accepted + rejected > 0 {
        let ratio = rejected as f64 / (accepted + rejected) as f64;
        if accepted == 0 {
            return Some("all records were rejected");
        }
        if pipeline
            .errors
            .max_failed_ratio
            .is_some_and(|limit| ratio > limit)
        {
            return Some("max_failed_ratio exceeded");
        }
    }
    None
}

/// The accepted rows of the committed prefix, used to rebuild dedup state.
fn committed_rows(prior: &crate::run::RunReport) -> Result<Vec<Value>> {
    use std::io::{BufRead, Read};
    let file =
        std::fs::File::open(&prior.partial_path).map_err(|e| Error::io(&prior.partial_path, e))?;
    let mut rows = Vec::new();
    for line in std::io::BufReader::new(file)
        .take(prior.sink_state.accepted_bytes)
        .lines()
    {
        let line = line.map_err(|e| Error::io(&prior.partial_path, e))?;
        rows.push(serde_json::from_str(&line).map_err(|_| {
            Error::Configuration("committed prefix contains an invalid row".into())
        })?);
    }
    if rows.len() as u64 != prior.sink_state.committed_accepted_records {
        return Err(Error::Configuration(
            "committed prefix does not match the recorded row count".into(),
        ));
    }
    Ok(rows)
}

async fn execute(
    pipeline: &Pipeline,
    provider: Arc<dyn LlmProvider>,
    cancellation: CancellationToken,
    resume: Option<ResumeContext>,
    // Held until this function returns: run/resume, publication, and every
    // manifest write happen under the same exclusive namespace lock.
    _run_lock: RunLock,
) -> Result<RunReport> {
    pipeline.validate()?;
    if !(1..=1024).contains(&provider.concurrency()) {
        return Err(Error::Configuration(
            "provider concurrency must be in 1..=1024".into(),
        ));
    }
    let schema = pipeline.compile_schema()?;
    let mut env = template::environment();
    env.add_template("generate", &pipeline.generate.prompt)
        .map_err(|_| Error::Configuration("invalid generation template".into()))?;
    let judge = build_judge(pipeline)?;
    let source_hash = source_fingerprint(pipeline, &cancellation).await?;
    let mut source = Source::open(&pipeline.source)?;
    let (mut artifacts, mut report, skip_through, mut dedup) = match resume {
        None => {
            let (artifacts, report) = Artifacts::create(pipeline, source_hash)?;
            (
                artifacts,
                report,
                0,
                pipeline.dedup.as_ref().map(DedupState::new),
            )
        }
        Some(context) => {
            let ResumeContext {
                skip_through,
                dedup,
                prior,
            } = context;
            let (artifacts, report) = Artifacts::resume(pipeline, source_hash, &prior)?;
            tracing::info!(
                run_id = report.run_id,
                resumed_from = prior.run_id,
                skip_through,
                event = "run_resumed"
            );
            (artifacts, report, skip_through, dedup)
        }
    };
    let started = Instant::now();
    tracing::info!(run_id = report.run_id, event = "run_started");
    let tools = StageTools {
        env: &env,
        provider: provider.as_ref(),
        schema: &schema,
        regenerate_on_invalid: pipeline.generate.regenerate_on_invalid,
        judge: judge.as_ref(),
    };
    let mut pending = FuturesOrdered::new();
    let mut source_done = false;
    let mut source_error = None;
    let mut latencies: Vec<f64> = Vec::new();
    let mut progress_at = started;
    let mut progress_count = 0u64;
    let result = async {
        loop {
            if cancellation.is_cancelled() { return Err(Error::Cancelled); }
            while !source_done && pending.len() < provider.concurrency() {
                match source.next() {
                    Some(Ok((position, data))) => {
                        report.statistics.source_records_total += 1;
                        if position <= skip_through {
                            continue; // already durable from the resumed run
                        }
                        pending.push_back(process(position, data, &tools, &cancellation));
                    }
                    Some(Err(error)) => {
                        report.statistics.source_records_total += 1;
                        report.statistics.source_failed_total += 1;
                        source_error = Some(error);
                        source_done = true;
                    }
                    None => source_done = true,
                }
            }
            let outcome = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                item = pending.next() => item,
            };
            let Some(outcome) = outcome else { break; };
            if matches!(outcome.result, Err(Error::Cancelled)) { return Err(Error::Cancelled); }
            report.statistics.merge(&outcome.statistics);
            report.statistics.processed_records_total += 1;
            if outcome.latency_ms > 0.0 {
                latencies.push(outcome.latency_ms);
            }
            let mut input = outcome.data.as_object().cloned().ok_or_else(|| Error::Source { position: outcome.position, message: "expected object".into() })?;
            let id = record_id(&input, outcome.position);
            let rejected = match outcome.result {
                Ok(generated) => {
                    if let Some(judge) = &generated.judge {
                        input.insert("judge".into(), json!({"provider": judge.provider, "model": judge.model, "score": judge.score}));
                    }
                    input.insert("generated".into(), generated.value);
                    // Dedup keys cover input plus generated; metadata is
                    // per-run. Admission registers only after the sink
                    // accepts the record, so sink-rejected rows never
                    // pollute the state; missing key fields reject the
                    // record itself instead of failing the run.
                    let admission = match &dedup {
                        None => DedupOutcome::Proceed(None),
                        Some(state) => match state.check(&Value::Object(input.clone())) {
                            Ok(DedupAdmission::Duplicate) => {
                                DedupOutcome::Reject(Error::Duplicate)
                            }
                            Ok(DedupAdmission::Unique(key)) => DedupOutcome::Proceed(Some(key)),
                            Err(error @ Error::DedupField { .. }) => DedupOutcome::Reject(error),
                            Err(error) => return Err(error),
                        },
                    };
                    match admission {
                        DedupOutcome::Reject(error) => {
                            if matches!(error, Error::Duplicate) {
                                report.statistics.duplicate_records_total += 1;
                            }
                            write_rejection(&mut artifacts, &mut report, &id, outcome.position, &error, outcome.attempts)?;
                            true
                        }
                        DedupOutcome::Proceed(key) => {
                            let meta = RecordMeta {
                                record_id: id.clone(), run_id: report.run_id.clone(), source_position: outcome.position,
                                pipeline_hash: report.pipeline_hash.clone(), pipeline_version: pipeline.version,
                                source_fingerprint: report.source_fingerprint.clone(),
                                stage: "accepted", attempt: generated.attempts, generator_provider: pipeline.generate.provider.clone(), generator_model: generated.model,
                                prompt_hash: generated.prompt_hash, template_engine: "minijinja/2",
                                judge_provider: generated.judge.as_ref().map(|j| j.provider.clone()),
                                judge_model: generated.judge.as_ref().map(|j| j.model.clone()),
                                judge_score: generated.judge.as_ref().map(|j| j.score),
                            };
                            input.insert("_meta".into(), serde_json::to_value(meta).map_err(|_| Error::Sink("metadata serialization failed".into()))?);
                            match artifacts.write_accepted(outcome.position, &Value::Object(input))? {
                                WriteResult::Written => {
                                    if let (Some(state), Some(key)) = (&mut dedup, key) {
                                        state.register(key);
                                    }
                                    report.statistics.accepted_records_total += 1;
                                    false
                                }
                                WriteResult::Rejected(error) => {
                                    write_rejection(&mut artifacts, &mut report, &id, outcome.position, &error, outcome.attempts)?;
                                    true
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    write_rejection(&mut artifacts, &mut report, &id, outcome.position, &error, outcome.attempts)?;
                    true
                }
            };
            refresh(&mut report, started, provider.as_ref(), pipeline, &latencies);
            artifacts.commit(&mut report, outcome.position)?;
            if report.statistics.processed_records_total - progress_count >= 500
                || progress_at.elapsed() >= std::time::Duration::from_secs(2)
            {
                tracing::info!(
                    processed = report.statistics.processed_records_total,
                    accepted = report.statistics.accepted_records_total,
                    rejected = report.statistics.rejected_records_total,
                    rate = report.statistics.records_per_second,
                    event = "progress"
                );
                progress_count = report.statistics.processed_records_total;
                progress_at = Instant::now();
            }
            if rejected && pipeline.errors.strict { return Err(Error::FailurePolicy("strict mode rejects any failed record".into())); }
            if pipeline.errors.max_failed_records.is_some_and(|max| report.base_rejected_records + report.statistics.rejected_records_total > max) {
                return Err(Error::FailurePolicy("max_failed_records exceeded".into()));
            }
            // Ready mock responses and synchronous file operations must still let
            // the signal handler run on a single-thread Tokio runtime.
            tokio::task::yield_now().await;
        }
        if let Some(error) = source_error.take() { return Err(error); }
        // Re-validate every policy over dataset-wide counts before publishing;
        // a resume with no new input must not publish an already-violating run.
        if let Some(violation) = policies_violated(
            pipeline,
            report.base_accepted_records + report.statistics.accepted_records_total,
            report.base_rejected_records + report.statistics.rejected_records_total,
        ) {
            return Err(Error::FailurePolicy(violation.into()));
        }
        if source_fingerprint(pipeline, &cancellation).await? != report.source_fingerprint {
            return Err(Error::Source { position: report.sink_state.committed_source_position, message: "source changed during execution; output was not published".into() });
        }
        if cancellation.is_cancelled() { return Err(Error::Cancelled); }
        Ok(())
    }.await;
    drop(pending); // Cancels in-flight HTTP work without detached tasks.
    refresh(
        &mut report,
        started,
        provider.as_ref(),
        pipeline,
        &latencies,
    );
    report.finished_at_unix_ms = Some(now_ms());
    let result = result.and_then(|()| artifacts.publish(&mut report));
    if let Err(error) = result {
        report.status = if matches!(error, Error::Cancelled) {
            RunStatus::Cancelled
        } else {
            RunStatus::Failed
        };
        report.errors.push(error.diagnostic());
        if let Err(error) = artifacts.rollback_uncommitted(&report) {
            report.errors.push(error.diagnostic());
        }
        if let Err(error) = save_report(&report) {
            report.errors.push(error.diagnostic());
        }
        tracing::error!(run_id = report.run_id, status = ?report.status, event = "run_failed");
    } else {
        tracing::info!(
            run_id = report.run_id,
            accepted = report.statistics.accepted_records_total,
            event = "run_completed"
        );
    }
    Ok(report)
}

fn write_rejection(
    artifacts: &mut Artifacts,
    report: &mut RunReport,
    id: &str,
    position: u64,
    error: &Error,
    attempts: u32,
) -> Result<()> {
    let mut diagnostic = error.diagnostic();
    diagnostic.attempts = diagnostic.attempts.max(attempts);
    artifacts.write_rejected(&json!({
        "run_id": report.run_id,
        "record_id": id,
        "source_position": position,
        "diagnostic": diagnostic,
    }))?;
    report.statistics.rejected_records_total += 1;
    tracing::warn!(
        position,
        category = diagnostic.category,
        event = "record_rejected"
    );
    Ok(())
}

fn refresh(
    report: &mut RunReport,
    started: Instant,
    provider: &dyn LlmProvider,
    pipeline: &Pipeline,
    latencies: &[f64],
) {
    report.statistics.elapsed_seconds = started.elapsed().as_secs_f64();
    report.statistics.records_per_second = report.statistics.processed_records_total as f64
        / report.statistics.elapsed_seconds.max(f64::EPSILON);
    report.statistics.provider_usage = provider.statistics();
    report.statistics.latency = summarize(latencies);
    if let Some(pricing) = &pipeline.pricing {
        report.statistics.estimated_cost_usd = Some(
            (report.statistics.prompt_tokens_total as f64 * pricing.input_usd_per_mtok
                + report.statistics.completion_tokens_total as f64 * pricing.output_usd_per_mtok)
                / 1_000_000.0,
        );
    }
}

/// Nearest-rank percentiles over the recorded generation latencies.
fn summarize(latencies: &[f64]) -> LatencySummary {
    if latencies.is_empty() {
        return LatencySummary::default();
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("finite latencies"));
    let pick = |fraction: f64| sorted[((sorted.len() - 1) as f64 * fraction).round() as usize];
    LatencySummary {
        mean_ms: sorted.iter().sum::<f64>() / sorted.len() as f64,
        p50_ms: pick(0.50),
        p95_ms: pick(0.95),
        p99_ms: pick(0.99),
    }
}

async fn source_fingerprint(
    pipeline: &Pipeline,
    cancellation: &CancellationToken,
) -> Result<String> {
    let source = pipeline.source.clone();
    let cancellation = cancellation.clone();
    tokio::task::spawn_blocking(move || fingerprint(&source, &cancellation))
        .await
        .map_err(|_| Error::RunFailed("source fingerprint worker failed".into()))?
}

/// Dedup decision for one record: reject it, or proceed carrying the key to
/// register once the sink accepts.
enum DedupOutcome {
    Reject(Error),
    Proceed(Option<crate::dedup::PreparedKey>),
}

struct Generated {
    value: Value,
    prompt_hash: String,
    model: String,
    attempts: u32,
    judge: Option<Judged>,
}
struct Judged {
    provider: String,
    model: String,
    score: f64,
}
struct JudgeTools {
    env: minijinja::Environment<'static>,
    provider: Arc<dyn LlmProvider>,
    provider_name: String,
    score_field: String,
    min_score: f64,
}

fn build_judge(pipeline: &Pipeline) -> Result<Option<JudgeTools>> {
    let Some(config) = &pipeline.judge else {
        return Ok(None);
    };
    let provider = create_provider(pipeline.providers.get(&config.provider).ok_or_else(|| {
        Error::Configuration("judge.provider references an unknown provider".into())
    })?)?;
    if !(1..=1024).contains(&provider.concurrency()) {
        return Err(Error::Configuration(
            "judge provider concurrency must be in 1..=1024".into(),
        ));
    }
    let mut env = template::environment();
    env.add_template_owned("judge".to_owned(), config.prompt.clone())
        .map_err(|_| Error::Configuration("invalid judge template".into()))?;
    Ok(Some(JudgeTools {
        env,
        provider,
        provider_name: config.provider.clone(),
        score_field: config.score_field.clone(),
        min_score: config.min_score,
    }))
}

struct Outcome {
    position: u64,
    data: Value,
    result: Result<Generated>,
    statistics: RunStatistics,
    attempts: u32,
    latency_ms: f64,
}
struct StageTools<'a> {
    env: &'a minijinja::Environment<'a>,
    provider: &'a dyn LlmProvider,
    schema: &'a jsonschema::Validator,
    regenerate_on_invalid: u32,
    judge: Option<&'a JudgeTools>,
}
async fn process(
    position: u64,
    data: Value,
    tools: &StageTools<'_>,
    cancellation: &CancellationToken,
) -> Outcome {
    let mut statistics = RunStatistics::default();
    let mut attempts = 0;
    let mut latency_ms = 0.0f64;
    let result = async {
        let mut context = data.as_object().cloned().unwrap_or_default();
        context.insert("record".into(), data.clone());
        let prompt =
            template::render(tools.env, "generate", &json!(context)).inspect_err(|_| {
                statistics.template_failed_total += 1;
            })?;
        let mut feedback: Option<String> = None;
        let mut regenerations = 0;
        loop {
            statistics.generation_requests_total += 1;
            let call = Instant::now();
            let response = tools
                .provider
                .generate(
                    GenerateRequest {
                        prompt: &prompt,
                        record: &data,
                        feedback: feedback.as_deref(),
                        generated: None,
                    },
                    cancellation,
                )
                .await
                .inspect_err(|_| {
                    statistics.generation_failed_total += 1;
                })?;
            latency_ms += call.elapsed().as_secs_f64() * 1000.0;
            statistics.prompt_tokens_total += response.prompt_tokens.unwrap_or(0);
            statistics.completion_tokens_total += response.completion_tokens.unwrap_or(0);
            statistics.generation_success_total += 1;
            attempts += response.attempts.max(1);
            let structured = serde_json::from_str::<Value>(&response.text)
                .map_err(|_| Error::StructuredOutput)
                .and_then(|generated| {
                    if generated.is_object() {
                        Ok(generated)
                    } else {
                        Err(Error::StructuredOutput)
                    }
                })
                .and_then(|generated| match tools.schema.validate(&generated) {
                    Ok(()) => Ok(generated),
                    Err(error) => Err(Error::Validation {
                        instance_path: error.instance_path.to_string(),
                        schema_path: error.schema_path.to_string(),
                    }),
                });
            match structured {
                Ok(generated) => {
                    let judged = judge_record(
                        &generated,
                        &prompt,
                        &data,
                        tools.judge,
                        &mut statistics,
                        cancellation,
                    )
                    .await?;
                    return Ok(Generated {
                        value: generated,
                        prompt_hash: blake3::hash(prompt.as_bytes()).to_hex().to_string(),
                        model: response.model,
                        attempts,
                        judge: judged,
                    });
                }
                Err(error) if regenerations < tools.regenerate_on_invalid => {
                    regenerations += 1;
                    statistics.regeneration_attempts_total += 1;
                    feedback = Some(repair_feedback(&error));
                }
                Err(error) => {
                    if matches!(error, Error::StructuredOutput) {
                        statistics.structured_output_failed_total += 1;
                    } else {
                        statistics.validation_failed_total += 1;
                    }
                    return Err(error);
                }
            }
        }
    }
    .await;
    Outcome {
        position,
        data,
        result,
        statistics,
        attempts,
        latency_ms,
    }
}

/// Diagnostics sent back to the provider describe rule paths, never raw output.
fn repair_feedback(error: &Error) -> String {
    match error {
        Error::StructuredOutput => "the reply was not a valid JSON object".to_owned(),
        Error::Validation {
            instance_path,
            schema_path,
        } => {
            format!("schema validation failed at {instance_path} (rule {schema_path})")
        }
        other => other.to_string(),
    }
}

async fn judge_record(
    generated: &Value,
    prompt: &str,
    data: &Value,
    judge: Option<&JudgeTools>,
    statistics: &mut RunStatistics,
    cancellation: &CancellationToken,
) -> Result<Option<Judged>> {
    let Some(tools) = judge else {
        return Ok(None);
    };
    statistics.judge_requests_total += 1;
    let context = json!({"record": data, "generated": generated, "prompt": prompt});
    let prompt = template::render(&tools.env, "judge", &context)?;
    let response = tools
        .provider
        .generate(
            GenerateRequest {
                prompt: &prompt,
                record: data,
                feedback: None,
                generated: Some(generated),
            },
            cancellation,
        )
        .await
        .inspect_err(|_| {
            statistics.judge_failed_total += 1;
        })?;
    statistics.prompt_tokens_total += response.prompt_tokens.unwrap_or(0);
    statistics.completion_tokens_total += response.completion_tokens.unwrap_or(0);
    let verdict = serde_json::from_str::<Value>(&response.text)
        .map_err(|_| {
            statistics.judge_failed_total += 1;
            Error::JudgeOutput {
                message: "judge reply was not valid JSON".into(),
            }
        })?
        .get(&tools.score_field)
        .cloned()
        .ok_or_else(|| {
            statistics.judge_failed_total += 1;
            Error::JudgeOutput {
                message: format!("judge reply is missing the field {}", tools.score_field),
            }
        })?;
    let score = verdict.as_f64().ok_or_else(|| {
        statistics.judge_failed_total += 1;
        Error::JudgeOutput {
            message: format!("judge field {} is not numeric", tools.score_field),
        }
    })?;
    if score < tools.min_score {
        statistics.judge_rejected_total += 1;
        return Err(Error::JudgeScore {
            score,
            min_score: tools.min_score,
        });
    }
    Ok(Some(Judged {
        provider: tools.provider_name.clone(),
        model: response.model,
        score,
    }))
}
