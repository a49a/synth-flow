use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    Http,
    Timeout,
    Network,
    InvalidResponse,
    ResponseTooLarge,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("source error at logical record {position}: {message}")]
    Source { position: u64, message: String },
    #[error("template error in {stage}: {message}")]
    Template { stage: String, message: String },
    #[error("provider {kind:?} error (HTTP {status:?}, attempts {attempts})")]
    Provider {
        kind: ProviderErrorKind,
        status: Option<u16>,
        attempts: u32,
    },
    #[error("structured output error: expected a JSON object")]
    StructuredOutput,
    #[error("schema validation failed at {instance_path} (rule {schema_path})")]
    Validation {
        instance_path: String,
        schema_path: String,
    },
    #[error("judge output error: {message}")]
    JudgeOutput { message: String },
    #[error("judge score {score} is below the minimum {min_score}")]
    JudgeScore { score: f64, min_score: f64 },
    #[error("duplicate of an earlier accepted record")]
    Duplicate,
    #[error("dedup field {field} is missing from the record")]
    DedupField { field: String },
    #[error("sink error: {0}")]
    Sink(String),
    #[error("run cancelled")]
    Cancelled,
    #[error("failure policy exceeded: {0}")]
    FailurePolicy(String),
    #[error("run failed: {0}")]
    RunFailed(String),
}

/// Safe diagnostics never include prompts, response bodies, credentials, or input values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub category: String,
    pub stage: String,
    pub message: String,
    pub attempts: u32,
    pub instance_path: Option<String>,
    pub schema_path: Option<String>,
}

impl Error {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
    pub fn diagnostic(&self) -> Diagnostic {
        let (category, stage) = match self {
            Self::Configuration(_) => ("configuration", "configuration"),
            Self::Io { .. } => ("io", "io"),
            Self::Source { .. } => ("source", "source"),
            Self::Template { stage, .. } => ("template", stage.as_str()),
            Self::Provider { kind, .. } => (
                match kind {
                    ProviderErrorKind::Timeout => "provider_timeout",
                    ProviderErrorKind::Network => "provider_network",
                    ProviderErrorKind::Http => "provider_http",
                    ProviderErrorKind::InvalidResponse => "provider_response",
                    ProviderErrorKind::ResponseTooLarge => "provider_response_size",
                },
                "generate",
            ),
            Self::StructuredOutput => ("structured_output", "parse"),
            Self::Validation { .. } => ("validation", "validate"),
            Self::JudgeOutput { .. } => ("judge_output", "judge"),
            Self::JudgeScore { .. } => ("judge_score", "judge"),
            Self::Duplicate => ("duplicate", "dedup"),
            Self::DedupField { .. } => ("dedup_field", "dedup"),
            Self::Sink(_) => ("sink", "sink"),
            Self::Cancelled => ("cancelled", "run"),
            Self::FailurePolicy(_) => ("failure_policy", "run"),
            Self::RunFailed(_) => ("run", "run"),
        };
        let (instance_path, schema_path) = if let Self::Validation {
            instance_path,
            schema_path,
        } = self
        {
            (Some(instance_path.clone()), Some(schema_path.clone()))
        } else {
            (None, None)
        };
        Diagnostic {
            category: category.into(),
            stage: stage.into(),
            message: self.to_string(),
            attempts: if let Self::Provider { attempts, .. } = self {
                *attempts
            } else {
                1
            },
            instance_path,
            schema_path,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
