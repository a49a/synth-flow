use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result, template};

pub const MAX_SPEC_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    pub version: u32,
    pub dataset: Dataset,
    pub source: SourceConfig,
    pub providers: BTreeMap<String, ProviderConfig>,
    pub generate: Generation,
    #[serde(default)]
    pub judge: Option<JudgeConfig>,
    #[serde(default)]
    pub dedup: Option<DedupConfig>,
    pub output: Output,
    #[serde(default)]
    pub errors: ErrorPolicy,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceConfig {
    Inline { records: Vec<Value> },
    Jsonl { path: PathBuf },
    Csv { path: PathBuf },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderConfig {
    Mock {
        response: String,
        #[serde(default = "default_concurrency")]
        concurrency: usize,
    },
    OpenaiCompatible {
        base_url: String,
        model: String,
        api_key_env: Option<String>,
        #[serde(default = "default_concurrency")]
        concurrency: usize,
        #[serde(default = "default_timeout")]
        timeout_ms: u64,
        #[serde(default)]
        retry: RetryPolicy,
        #[serde(default)]
        rate_limit: Option<RateLimitConfig>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    pub requests_per_minute: Option<u32>,
    pub tokens_per_minute: Option<u32>,
}

fn default_timeout() -> u64 {
    60_000
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay_ms: 500,
            max_delay_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ErrorPolicy {
    pub dead_letter: Option<PathBuf>,
    pub max_failed_records: Option<u64>,
    pub max_failed_ratio: Option<f64>,
    pub strict: bool,
}

fn default_concurrency() -> usize {
    1
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Generation {
    pub provider: String,
    pub prompt: String,
    pub output_schema: Value,
    #[serde(default)]
    pub regenerate_on_invalid: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JudgeConfig {
    pub provider: String,
    pub prompt: String,
    #[serde(default = "default_score_field")]
    pub score_field: String,
    pub min_score: f64,
}

fn default_score_field() -> String {
    "score".into()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum DedupConfig {
    Exact {
        /// Dotted paths into input plus generated; defaults to the whole
        /// generated object.
        fields: Option<Vec<String>>,
        #[serde(default)]
        normalize: Normalize,
    },
    #[serde(rename = "minhash")]
    MinHash {
        /// Dotted path to the text field compared for near duplicates.
        field: String,
        #[serde(default = "default_num_perm")]
        num_perm: u16,
        #[serde(default = "default_bands")]
        bands: u16,
        #[serde(default = "default_threshold")]
        threshold: f64,
        #[serde(default = "default_shingle_words")]
        shingle_words: usize,
    },
}

fn default_num_perm() -> u16 {
    128
}
fn default_bands() -> u16 {
    16
}
fn default_threshold() -> f64 {
    0.8
}
fn default_shingle_words() -> usize {
    3
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Normalize {
    #[default]
    None,
    Lowercase,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub format: OutputFormat,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    Jsonl,
}

impl Pipeline {
    /// Resolve relative paths against the pipeline file, not the caller's cwd.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut text = String::new();
        File::open(path)
            .map_err(|e| Error::io(path, e))?
            .take(MAX_SPEC_BYTES + 1)
            .read_to_string(&mut text)
            .map_err(|e| Error::io(path, e))?;
        if text.len() as u64 > MAX_SPEC_BYTES {
            return Err(Error::Configuration("pipeline exceeds 8 MiB limit".into()));
        }
        let mut spec: Self = serde_path_to_error::deserialize(serde_yaml::Deserializer::from_str(
            &text,
        ))
        .map_err(|e| {
            let raw = e.inner().to_string();
            let location = e
                .inner()
                .location()
                .map(|l| format!(" (line {}, column {})", l.line(), l.column()))
                .unwrap_or_default();
            // Preserve the expected contract and field path, never the invalid value.
            let detail = if let Some((_, expected)) = raw.split_once("expected ") {
                format!("expected {expected}")
            } else if raw.contains("missing field") {
                raw.split(" at line")
                    .next()
                    .unwrap_or("missing field")
                    .to_owned()
            } else {
                "invalid YAML or unsupported field".into()
            };
            Error::Configuration(format!("{}: {detail}{location}", e.path()))
        })?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        if let SourceConfig::Jsonl { path } | SourceConfig::Csv { path } = &mut spec.source {
            *path = normalize_path(&base.join(&*path))?;
        }
        spec.output.path = normalize_path(&base.join(&spec.output.path))?;
        if let Some(path) = &mut spec.errors.dead_letter {
            *path = normalize_path(&base.join(&*path))?;
        }
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<()> {
        let invalid = |message: &str| Error::Configuration(message.into());
        if self.version != 1 {
            return Err(invalid("only pipeline version 1 is supported"));
        }
        if self.dataset.name.trim().is_empty() {
            return Err(invalid("dataset.name cannot be empty"));
        }
        if !self.providers.contains_key(&self.generate.provider) {
            return Err(invalid("generate.provider references an unknown provider"));
        }
        if self.generate.prompt.trim().is_empty() {
            return Err(invalid("generate.prompt cannot be empty"));
        }
        if self.generate.regenerate_on_invalid > 3 {
            return Err(invalid("generate.regenerate_on_invalid must be in 0..=3"));
        }
        let dotted = |field: &str| {
            field.trim().is_empty() || field.split('.').any(|segment| segment.trim().is_empty())
        };
        match &self.dedup {
            Some(DedupConfig::Exact { fields, .. }) => {
                if fields
                    .as_ref()
                    .is_some_and(|fields| fields.iter().any(|field| dotted(field)))
                {
                    return Err(invalid(
                        "dedup.fields entries must be non-empty dotted paths",
                    ));
                }
            }
            Some(DedupConfig::MinHash {
                field,
                num_perm,
                bands,
                threshold,
                shingle_words,
            }) => {
                if dotted(field) {
                    return Err(invalid("dedup.field must be a non-empty dotted path"));
                }
                if *num_perm < 8
                    || *num_perm > 1024
                    || *bands == 0
                    || *bands > *num_perm
                    || *num_perm % *bands != 0
                {
                    return Err(invalid(
                        "dedup num_perm (8..=1024) must be divisible by bands (1..=num_perm)",
                    ));
                }
                if !threshold.is_finite() || !(0.0..=1.0).contains(threshold) {
                    return Err(invalid("dedup.threshold must be in 0..=1"));
                }
                if *shingle_words == 0 || *shingle_words > 10 {
                    return Err(invalid("dedup.shingle_words must be in 1..=10"));
                }
            }
            None => {}
        }
        if let Some(judge) = &self.judge {
            if !self.providers.contains_key(&judge.provider) {
                return Err(invalid("judge.provider references an unknown provider"));
            }
            if judge.prompt.trim().is_empty() {
                return Err(invalid("judge.prompt cannot be empty"));
            }
            template::validate(&judge.prompt, "judge")?;
            if !judge.min_score.is_finite() || judge.score_field.trim().is_empty() {
                return Err(invalid(
                    "judge requires a finite min_score and a non-empty score_field",
                ));
            }
        }
        template::validate(&self.generate.prompt, "generation")?;
        for provider in self.providers.values() {
            match provider {
                ProviderConfig::Mock {
                    response,
                    concurrency,
                } => {
                    template::validate(response, "mock response")?;
                    if !(1..=1024).contains(concurrency) {
                        return Err(invalid("provider concurrency must be in 1..=1024"));
                    }
                }
                ProviderConfig::OpenaiCompatible {
                    base_url,
                    model,
                    api_key_env,
                    concurrency,
                    timeout_ms,
                    retry,
                    rate_limit,
                } => {
                    if *timeout_ms == 0
                        || *timeout_ms > 3_600_000
                        || retry.max_attempts == 0
                        || retry.max_attempts > 100
                        || retry.initial_delay_ms == 0
                        || retry.max_delay_ms < retry.initial_delay_ms
                        || retry.max_delay_ms > 3_600_000
                    {
                        return Err(invalid("invalid provider timeout_ms or retry policy"));
                    }
                    if let Some(limit) = rate_limit
                        && ([limit.requests_per_minute, limit.tokens_per_minute]
                            .into_iter()
                            .flatten()
                            .any(|v| v == 0 || v > 100_000_000)
                            || (limit.requests_per_minute.is_none()
                                && limit.tokens_per_minute.is_none()))
                    {
                        return Err(invalid(
                            "rate_limit needs a positive requests_per_minute or tokens_per_minute",
                        ));
                    }
                    let url = url::Url::parse(base_url)
                        .map_err(|_| invalid("provider base_url must be a valid URL"))?;
                    if !matches!(url.scheme(), "http" | "https")
                        || url.host_str().is_none()
                        || !url.username().is_empty()
                        || url.password().is_some()
                        || url.query().is_some()
                        || url.fragment().is_some()
                    {
                        return Err(invalid(
                            "provider base_url must be an HTTP(S) URL without credentials",
                        ));
                    }
                    if model.trim().is_empty() || *concurrency == 0 || *concurrency > 1024 {
                        return Err(invalid(
                            "provider requires a model and concurrency in 1..=1024",
                        ));
                    }
                    if api_key_env.as_ref().is_some_and(|s| {
                        s.is_empty()
                            || s.starts_with(|c: char| c.is_ascii_digit())
                            || !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    }) {
                        return Err(invalid("api_key_env must name an environment variable"));
                    }
                }
            }
        }
        match &self.source {
            SourceConfig::Inline { records } => {
                for (i, record) in records.iter().enumerate() {
                    validate_input(record, i as u64 + 1)?;
                }
            }
            SourceConfig::Jsonl { path } => {
                if !path.is_file() {
                    return Err(invalid("JSONL source path must refer to an existing file"));
                }
            }
            SourceConfig::Csv { path } => {
                if !path.is_file() {
                    return Err(invalid("CSV source path must refer to an existing file"));
                }
            }
        }
        if self.output.path.file_name().is_none() || self.output.path.is_dir() {
            return Err(invalid("output.path must name a file"));
        }
        let mut ancestor = self.output.path.parent();
        while let Some(path) = ancestor {
            if path.exists() {
                if !path.is_dir() {
                    return Err(invalid("output parent must be a directory"));
                }
                break;
            }
            ancestor = path.parent();
        }
        if self
            .errors
            .max_failed_ratio
            .is_some_and(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
        {
            return Err(invalid("errors.max_failed_ratio must be in 0..=1"));
        }
        let out = normalize_path(&self.output.path)?;
        let dead = self.dead_letter_path()?;
        let artifacts = [
            out.clone(),
            suffix(&out, ".partial"),
            suffix(&out, ".manifest.json"),
            dead.clone(),
        ];
        for (i, path) in artifacts.iter().enumerate() {
            if artifacts[..i].contains(path) {
                return Err(invalid(
                    "output, partial, manifest and dead-letter paths must be distinct",
                ));
            }
            if let SourceConfig::Jsonl { path: source } | SourceConfig::Csv { path: source } =
                &self.source
                && *path == normalize_path(source)?
            {
                return Err(invalid("run artifacts must not overwrite the source"));
            }
        }
        self.compile_schema()?;
        Ok(())
    }

    pub fn compile_schema(&self) -> Result<jsonschema::Validator> {
        // Traverse schema positions only. A property or an enum value named
        // `$ref` is data, not a schema reference.
        fn has_reference(value: &Value) -> bool {
            let Some(m) = value.as_object() else {
                return false;
            };
            if ["$ref", "$dynamicRef", "$recursiveRef"]
                .iter()
                .any(|k| m.contains_key(*k))
            {
                return true;
            }
            for key in [
                "properties",
                "patternProperties",
                "$defs",
                "definitions",
                "dependentSchemas",
                "dependencies",
            ] {
                if m.get(key)
                    .and_then(Value::as_object)
                    .is_some_and(|map| map.values().any(has_reference))
                {
                    return true;
                }
            }
            for key in [
                "additionalProperties",
                "additionalItems",
                "unevaluatedProperties",
                "unevaluatedItems",
                "contains",
                "propertyNames",
                "not",
                "if",
                "then",
                "else",
                "contentSchema",
                "items",
            ] {
                if let Some(v) = m.get(key)
                    && (has_reference(v)
                        || v.as_array().is_some_and(|a| a.iter().any(has_reference)))
                {
                    return true;
                }
            }
            ["allOf", "anyOf", "oneOf", "prefixItems"].iter().any(|k| {
                m.get(*k)
                    .and_then(Value::as_array)
                    .is_some_and(|a| a.iter().any(has_reference))
            })
        }
        if has_reference(&self.generate.output_schema) {
            return Err(Error::Configuration(
                "schema references are not supported; inline the schema".into(),
            ));
        }
        jsonschema::validator_for(&self.generate.output_schema).map_err(|error| {
            Error::Configuration(format!(
                "invalid generate.output_schema at {} (rule {})",
                error.instance_path, error.schema_path
            ))
        })
    }

    pub fn dead_letter_path(&self) -> Result<PathBuf> {
        normalize_path(
            &self
                .errors
                .dead_letter
                .clone()
                .unwrap_or_else(|| suffix(&self.output.path, ".rejected.jsonl")),
        )
    }

    pub fn hash(&self) -> Result<String> {
        let mut canonical = self.clone();
        canonical.output.path = normalize_path(&self.output.path)?;
        canonical.errors.dead_letter = Some(self.dead_letter_path()?);
        if let SourceConfig::Jsonl { path } | SourceConfig::Csv { path } = &mut canonical.source {
            *path = normalize_path(path)?;
        }
        let value = serde_json::to_value(canonical)
            .map_err(|_| Error::Configuration("pipeline cannot be serialized".into()))?;
        Ok(blake3::hash(value.to_string().as_bytes())
            .to_hex()
            .to_string())
    }

    pub fn plan(&self) -> String {
        let source = match self.source {
            SourceConfig::Inline { .. } => "InlineSource",
            SourceConfig::Jsonl { .. } => "JsonlSource",
            SourceConfig::Csv { .. } => "CsvSource",
        };
        let regenerate = if self.generate.regenerate_on_invalid > 0 {
            format!(
                "\n  ↓\nRegenerate(x{})",
                self.generate.regenerate_on_invalid
            )
        } else {
            String::new()
        };
        let judge = if self.judge.is_some() {
            "\n  ↓\nJudge"
        } else {
            ""
        };
        let dedup = match &self.dedup {
            Some(DedupConfig::Exact { .. }) => "\n  ↓\nDedupExact",
            Some(DedupConfig::MinHash { .. }) => "\n  ↓\nDedupMinHash",
            None => "",
        };
        format!(
            "{source}\n  ↓\nPromptRender\n  ↓\nGenerate({})\n  ↓\nJsonParse\n  ↓\nSchemaValidate{regenerate}{judge}{dedup}\n  ↓\nJsonlSink",
            self.generate.provider
        )
    }
}

pub fn validate_input(value: &Value, position: u64) -> Result<()> {
    let map = value.as_object().ok_or_else(|| Error::Source {
        position,
        message: "each input record must be a JSON object".into(),
    })?;
    if ["generated", "judge", "_meta"]
        .iter()
        .any(|key| map.contains_key(*key))
    {
        return Err(Error::Source {
            position,
            message: "input contains a reserved field: generated, judge, or _meta".into(),
        });
    }
    Ok(())
}

/// Canonicalize existing components and resolve missing output paths without creating them.
pub fn normalize_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|e| Error::io(path, e))?
            .join(path)
    };
    let mut result = PathBuf::new();
    for part in absolute.components() {
        match part {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                result.pop();
            }
            other => {
                result.push(other.as_os_str());
                if result.exists() {
                    result = std::fs::canonicalize(&result).map_err(|e| Error::io(&result, e))?;
                }
            }
        }
    }
    Ok(result)
}

pub fn suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}
