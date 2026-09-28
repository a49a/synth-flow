//! Duplicate detection over accepted records.

use std::collections::HashSet;

use serde_json::Value;

use crate::{
    Error, Result,
    spec::{DedupConfig, Normalize},
};

pub(crate) enum DedupState {
    Exact {
        seen: HashSet<[u8; 32]>,
        fields: Vec<String>,
        normalize: Normalize,
    },
}

impl DedupState {
    pub(crate) fn new(config: &DedupConfig) -> Self {
        match config {
            DedupConfig::Exact { fields, normalize } => Self::Exact {
                seen: HashSet::new(),
                fields: fields.clone().unwrap_or_default(),
                normalize: *normalize,
            },
        }
    }

    /// Register a record; `Ok(true)` means it duplicates an earlier one.
    pub(crate) fn insert(&mut self, record: &Value) -> Result<bool> {
        match self {
            Self::Exact {
                seen,
                fields,
                normalize,
            } => {
                let key = exact_key(record, fields, *normalize)?;
                Ok(!seen.insert(key))
            }
        }
    }

    /// Rebuild state from previously accepted records during resume.
    #[allow(dead_code)] // consumed by the resume implementation
    pub(crate) fn restore(&mut self, record: &Value) -> Result<()> {
        self.insert(record).map(|_| ())
    }
}

fn extract<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = record;
    for segment in path.split('.') {
        current = match current {
            Value::Object(map) => map.get(segment)?,
            Value::Array(items) => items.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// An empty field list keys on the whole generated object.
fn exact_key(record: &Value, fields: &[String], normalize: Normalize) -> Result<[u8; 32]> {
    let mut parts = Vec::new();
    if fields.is_empty() {
        parts.push(
            record
                .get("generated")
                .ok_or_else(|| Error::DedupField {
                    field: "generated".into(),
                })?
                .clone(),
        );
    } else {
        for field in fields {
            parts.push(
                extract(record, field)
                    .ok_or_else(|| Error::DedupField {
                        field: field.clone(),
                    })?
                    .clone(),
            );
        }
    }
    if matches!(normalize, Normalize::Lowercase) {
        for part in &mut parts {
            lowercase(part);
        }
    }
    let canonical = serde_json::to_string(&parts)
        .map_err(|_| Error::Configuration("dedup key serialization failed".into()))?;
    Ok(blake3::hash(canonical.as_bytes()).into())
}

fn lowercase(value: &mut Value) {
    match value {
        Value::String(text) => *text = text.to_lowercase(),
        Value::Array(items) => items.iter_mut().for_each(lowercase),
        Value::Object(map) => {
            let lowered: Vec<(String, Value)> = map
                .iter_mut()
                .map(|(key, value)| {
                    lowercase(value);
                    (key.to_lowercase(), value.take())
                })
                .collect();
            *map = lowered.into_iter().collect();
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(answer: &str) -> Value {
        json!({"topic": "t", "generated": {"answer": answer}})
    }

    #[test]
    fn exact_keys_cover_field_paths_and_normalization() {
        let config = DedupConfig::Exact {
            fields: Some(vec!["generated.answer".into()]),
            normalize: Normalize::Lowercase,
        };
        let mut state = DedupState::new(&config);
        assert!(!state.insert(&record("Rust Ownership")).expect("first"));
        assert!(state.insert(&record("rust ownership")).expect("second"));
        assert!(!state.insert(&record("different")).expect("third"));
    }

    #[test]
    fn missing_fields_surface_dedup_diagnostics() {
        let config = DedupConfig::Exact {
            fields: Some(vec!["generated.missing".into()]),
            normalize: Normalize::None,
        };
        let mut state = DedupState::new(&config);
        assert!(matches!(
            state.insert(&record("a")),
            Err(Error::DedupField { .. })
        ));
    }

    #[test]
    fn default_keys_cover_the_whole_generated_object() {
        let config = DedupConfig::Exact {
            fields: None,
            normalize: Normalize::None,
        };
        let mut state = DedupState::new(&config);
        assert!(!state.insert(&record("a")).expect("first"));
        assert!(state.insert(&record("a")).expect("second"));
        // The input topic differs but the generated object is the dedup key.
        let other_topic = json!({"topic": "other", "generated": {"answer": "a"}});
        assert!(state.insert(&other_topic).expect("third"));
    }
}
