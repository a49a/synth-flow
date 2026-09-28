use clap::{Parser, Subcommand};
use std::{path::PathBuf, process::ExitCode};
use synthflow::Pipeline;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    name = "synthflow",
    version,
    about = "Local-first synthetic data pipelines"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Validate YAML, paths, templates, provider references, and JSON Schema.
    Validate { pipeline: PathBuf },
    /// Print the logical execution plan without calling a provider.
    Plan { pipeline: PathBuf },
    /// Run a pipeline; publish output only on success. Ctrl+C preserves partial progress.
    Run {
        pipeline: PathBuf,
        #[arg(long)]
        strict: bool,
    },
    /// Recover an interrupted run from its checkpoint or verified publication intent.
    Resume {
        pipeline: PathBuf,
        #[arg(long)]
        strict: bool,
    },
    /// Print basic statistics for a published .jsonl or .parquet dataset.
    Inspect {
        dataset: PathBuf,
        /// Emit the full summary as JSON.
        #[arg(long)]
        json: bool,
    },
}
#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
    match execute(Cli::parse()).await {
        Ok(code) => code,
        Err(error) => {
            // Even preflight failures are machine-readable on stdout.
            println!(
                "{}",
                serde_json::json!({"status":"failed", "preflight":true, "errors":[error.diagnostic()]})
            );
            eprintln!("Error: {error}");
            if matches!(error, synthflow::Error::Cancelled) {
                ExitCode::from(130)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}
async fn execute(cli: Cli) -> synthflow::Result<ExitCode> {
    if let Command::Inspect { dataset, json } = &cli.command {
        let summary = synthflow::inspect::summarize(dataset)?;
        if *json {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .map_err(|_| synthflow::Error::Sink("summary serialization failed".into()))?
            );
        } else {
            println!(
                "{}: {} rows, {} bytes, {} columns",
                summary.path.display(),
                summary.rows,
                summary.bytes,
                summary.columns.len()
            );
            println!(
                "{:<24} {:<8} {:>10} {:>8}  numeric",
                "column", "type", "non-null", "null"
            );
            for column in &summary.columns {
                let numeric = match (column.min, column.max, column.mean) {
                    (Some(min), Some(max), Some(mean)) => {
                        format!("min={min} max={max} mean={mean:.3}")
                    }
                    _ => "-".to_owned(),
                };
                println!(
                    "{:<24} {:<8} {:>10} {:>8}  {}",
                    column.name, column.data_type, column.count, column.null_count, numeric
                );
            }
        }
        return Ok(ExitCode::SUCCESS);
    }
    let path = match &cli.command {
        Command::Validate { pipeline }
        | Command::Plan { pipeline }
        | Command::Run { pipeline, .. }
        | Command::Resume { pipeline, .. } => pipeline,
        Command::Inspect { .. } => unreachable!("handled above"),
    };
    let mut pipeline = Pipeline::load(path)?;
    match cli.command {
        Command::Validate { .. } => println!(
            "Valid pipeline: {} (version {})",
            pipeline.dataset.name, pipeline.version
        ),
        Command::Plan { .. } => println!("{}", pipeline.plan()),
        Command::Run { strict, .. } | Command::Resume { strict, .. } => {
            pipeline.errors.strict |= strict;
            let cancellation = CancellationToken::new();
            let signal_token = cancellation.clone();
            let signals = tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    signal_token.cancel();
                    if tokio::signal::ctrl_c().await.is_ok() {
                        std::process::exit(130);
                    }
                }
            });
            let result = match cli.command {
                Command::Resume { .. } => synthflow::resume_async(&pipeline, cancellation).await,
                _ => synthflow::run_async(&pipeline, cancellation).await,
            };
            signals.abort();
            let report = result?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .map_err(|_| synthflow::Error::Sink("report serialization failed".into()))?
            );
            return Ok(if report.succeeded() {
                ExitCode::SUCCESS
            } else if report.status == synthflow::RunStatus::Cancelled {
                ExitCode::from(130)
            } else {
                ExitCode::FAILURE
            });
        }
        Command::Inspect { .. } => unreachable!("handled above"),
    }
    Ok(ExitCode::SUCCESS)
}
