use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Debug, Serialize)]
pub struct RecordMeta {
    pub record_id: String,
    pub run_id: String,
    pub source_fingerprint: String,
    pub source_position: u64,
    pub pipeline_hash: String,
    pub pipeline_version: u32,
    pub stage: &'static str,
    pub attempt: u32,
    pub generator_provider: String,
    pub generator_model: String,
    pub prompt_hash: String,
    pub template_engine: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub judge_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub judge_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub judge_score: Option<f64>,
}

/// serde_json's default object map is key-sorted, including nested objects.
pub fn record_id(data: &Map<String, Value>, position: u64) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&position.to_le_bytes());
    hasher.update(Value::Object(data.clone()).to_string().as_bytes());
    hasher.finalize().to_hex().to_string()
}
