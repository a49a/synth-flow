use crate::{
    Error, Pipeline, Result,
    provider::{GenerateRequest, LlmProvider, ProviderStatistics, create_provider},
    record::{RecordMeta, record_id},
    run::{Artifacts, RunReport, RunStatus, fingerprint, now_ms, save_report},
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
    pub template_failed_total: u64,
    pub structured_output_failed_total: u64,
    pub validation_failed_total: u64,
    pub accepted_records_total: u64,
    pub rejected_records_total: u64,
    pub elapsed_seconds: f64,
    pub records_per_second: f64,
    pub provider_usage: ProviderStatistics,
}
impl RunStatistics {
    fn merge(&mut self, other: &Self) {
        self.generation_requests_total += other.generation_requests_total;
        self.generation_success_total += other.generation_success_total;
        self.generation_failed_total += other.generation_failed_total;
        self.regeneration_attempts_total += other.regeneration_attempts_total;
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
    if !(1..=1024).contains(&provider.concurrency()) {
        return Err(Error::Configuration(
            "provider concurrency must be in 1..=1024".into(),
        ));
    }
    let schema = pipeline.compile_schema()?;
    let mut env = template::environment();
    env.add_template("generate", &pipeline.generate.prompt)
        .map_err(|_| Error::Configuration("invalid generation template".into()))?;
    let source_hash = source_fingerprint(pipeline, &cancellation).await?;
    let mut source = Source::open(&pipeline.source)?;
    let (mut artifacts, mut report) = Artifacts::create(pipeline, source_hash)?;
    let started = Instant::now();
    tracing::info!(run_id = report.run_id, event = "run_started");
    let mut pending = FuturesOrdered::new();
    let mut source_done = false;
    let mut source_error = None;
    let result = async {
        loop {
            if cancellation.is_cancelled() { return Err(Error::Cancelled); }
            while !source_done && pending.len() < provider.concurrency() {
                match source.next() {
                    Some(Ok((position, data))) => {
                        report.statistics.source_records_total += 1;
                        pending.push_back(process(
                            position,
                            data,
                            &env,
                            provider.as_ref(),
                            &schema,
                            pipeline.generate.regenerate_on_invalid,
                            &cancellation,
                        ));
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
            let mut input = outcome.data.as_object().cloned().ok_or_else(|| Error::Source { position: outcome.position, message: "expected object".into() })?;
            let id = record_id(&input, outcome.position);
            let rejected = match outcome.result {
                Ok(generated) => {
                    let meta = RecordMeta {
                        record_id: id, run_id: report.run_id.clone(), source_position: outcome.position,
                        pipeline_hash: report.pipeline_hash.clone(), pipeline_version: pipeline.version,
                        source_fingerprint: report.source_fingerprint.clone(),
                        stage: "accepted", attempt: generated.attempts, generator_provider: pipeline.generate.provider.clone(), generator_model: generated.model,
                        prompt_hash: generated.prompt_hash, template_engine: "minijinja/2",
                    };
                    input.insert("generated".into(), generated.value);
                    input.insert("_meta".into(), serde_json::to_value(meta).map_err(|_| Error::Sink("metadata serialization failed".into()))?);
                    artifacts.write_accepted(&Value::Object(input))?;
                    report.statistics.accepted_records_total += 1;
                    false
                }
                Err(error) => {
                    let mut diagnostic = error.diagnostic();
                    diagnostic.attempts = diagnostic.attempts.max(outcome.attempts);
                    artifacts.write_rejected(&json!({"run_id":report.run_id, "record_id":id, "source_position":outcome.position, "diagnostic":diagnostic}))?;
                    report.statistics.rejected_records_total += 1;
                    tracing::warn!(position = outcome.position, category = error.diagnostic().category, event = "record_rejected");
                    true
                }
            };
            refresh(&mut report, started, provider.as_ref());
            artifacts.commit(&mut report, outcome.position)?;
            if rejected && pipeline.errors.strict { return Err(Error::FailurePolicy("strict mode rejects any failed record".into())); }
            if pipeline.errors.max_failed_records.is_some_and(|max| report.statistics.rejected_records_total > max) {
                return Err(Error::FailurePolicy("max_failed_records exceeded".into()));
            }
            // Ready mock responses and synchronous file operations must still let
            // the signal handler run on a single-thread Tokio runtime.
            tokio::task::yield_now().await;
        }
        if let Some(error) = source_error.take() { return Err(error); }
        if report.statistics.processed_records_total > 0 {
            let ratio = report.statistics.rejected_records_total as f64 / report.statistics.processed_records_total as f64;
            if ratio == 1.0 { return Err(Error::FailurePolicy("all records were rejected".into())); }
            if pipeline.errors.max_failed_ratio.is_some_and(|limit| ratio > limit) { return Err(Error::FailurePolicy("max_failed_ratio exceeded".into())); }
        }
        if source_fingerprint(pipeline, &cancellation).await? != report.source_fingerprint {
            return Err(Error::Source { position: report.sink_state.committed_source_position, message: "source changed during execution; output was not published".into() });
        }
        if cancellation.is_cancelled() { return Err(Error::Cancelled); }
        Ok(())
    }.await;
    drop(pending); // Cancels in-flight HTTP work without detached tasks.
    refresh(&mut report, started, provider.as_ref());
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

fn refresh(report: &mut RunReport, started: Instant, provider: &dyn LlmProvider) {
    report.statistics.elapsed_seconds = started.elapsed().as_secs_f64();
    report.statistics.records_per_second = report.statistics.processed_records_total as f64
        / report.statistics.elapsed_seconds.max(f64::EPSILON);
    report.statistics.provider_usage = provider.statistics();
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

struct Generated {
    value: Value,
    prompt_hash: String,
    model: String,
    attempts: u32,
}
struct Outcome {
    position: u64,
    data: Value,
    result: Result<Generated>,
    statistics: RunStatistics,
    attempts: u32,
}
async fn process(
    position: u64,
    data: Value,
    env: &minijinja::Environment<'_>,
    provider: &dyn LlmProvider,
    schema: &jsonschema::Validator,
    regenerate_on_invalid: u32,
    cancellation: &CancellationToken,
) -> Outcome {
    let mut statistics = RunStatistics::default();
    let mut attempts = 0;
    let result = async {
        let mut context = data.as_object().cloned().unwrap_or_default();
        context.insert("record".into(), data.clone());
        let prompt = template::render(env, "generate", &json!(context)).inspect_err(|_| {
            statistics.template_failed_total += 1;
        })?;
        let mut feedback: Option<String> = None;
        let mut regenerations = 0;
        loop {
            statistics.generation_requests_total += 1;
            let response = provider
                .generate(
                    GenerateRequest {
                        prompt: &prompt,
                        record: &data,
                        feedback: feedback.as_deref(),
                    },
                    cancellation,
                )
                .await
                .inspect_err(|_| {
                    statistics.generation_failed_total += 1;
                })?;
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
                .and_then(|generated| match schema.validate(&generated) {
                    Ok(()) => Ok(generated),
                    Err(error) => Err(Error::Validation {
                        instance_path: error.instance_path.to_string(),
                        schema_path: error.schema_path.to_string(),
                    }),
                });
            match structured {
                Ok(generated) => {
                    return Ok(Generated {
                        value: generated,
                        prompt_hash: blake3::hash(prompt.as_bytes()).to_hex().to_string(),
                        model: response.model,
                        attempts,
                    });
                }
                Err(error) if regenerations < regenerate_on_invalid => {
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
