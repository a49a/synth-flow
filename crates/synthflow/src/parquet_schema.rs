//! Fixed Parquet schemas and lossless JSON-to-Arrow type checks.
use crate::{Error, Result};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Clone)]
pub(crate) struct OutputSchema {
    root: Node,
    pub(crate) arrow: SchemaRef,
}

#[derive(Clone)]
struct Node {
    nullable: bool,
    kind: Kind,
}

#[derive(Clone)]
enum Kind {
    Null,
    Boolean,
    String,
    Integer,
    Unsigned,
    Number,
    Array(Box<Node>),
    Object(Vec<(String, Node, bool)>),
}

impl OutputSchema {
    pub(crate) fn compile(value: &Value) -> Result<Self> {
        let root = Node::compile(value, "output.schema")?;
        let Kind::Object(fields) = &root.kind else {
            return Err(invalid("output.schema", "root must have type object"));
        };
        if root.nullable {
            return Err(invalid("output.schema", "root cannot be nullable"));
        }
        if fields.iter().any(|(name, _, _)| name == "_meta") {
            return Err(invalid("output.schema", "_meta is managed automatically"));
        }
        let mut arrow_fields: Vec<Field> = fields
            .iter()
            .map(|(name, node, required)| node.field(name, *required))
            .collect();
        arrow_fields.push(metadata_field());
        Ok(Self {
            root,
            arrow: Arc::new(Schema::new(arrow_fields)),
        })
    }

    pub(crate) fn accepts(&self, value: &Value) -> bool {
        self.root.accepts(value, true)
    }
}

fn invalid(path: &str, message: &str) -> Error {
    Error::Configuration(format!("{path}: {message}"))
}

impl Node {
    fn compile(value: &Value, path: &str) -> Result<Self> {
        let map = value
            .as_object()
            .ok_or_else(|| invalid(path, "schema must be an object"))?;
        for key in map.keys() {
            if !matches!(
                key.as_str(),
                "type"
                    | "properties"
                    | "required"
                    | "items"
                    | "additionalProperties"
                    | "format"
                    | "description"
                    | "title"
            ) {
                return Err(invalid(path, &format!("unsupported schema keyword {key}")));
            }
        }
        let (ty, nullable) = match map.get("type") {
            Some(Value::String(ty)) => (ty.as_str(), ty == "null"),
            Some(Value::Array(types)) if types.len() == 2 => {
                let has_null = types.iter().any(|ty| ty.as_str() == Some("null"));
                let other = types
                    .iter()
                    .filter_map(Value::as_str)
                    .find(|ty| *ty != "null");
                if !has_null || other.is_none() {
                    return Err(invalid(path, "type union must contain one type and null"));
                }
                (other.expect("checked"), true)
            }
            _ => {
                return Err(invalid(
                    path,
                    "type is required; only nullable unions are supported",
                ));
            }
        };
        for (keyword, allowed) in [
            ("properties", ty == "object"),
            ("required", ty == "object"),
            ("additionalProperties", ty == "object"),
            ("items", ty == "array"),
            ("format", ty == "integer"),
        ] {
            if map.contains_key(keyword) && !allowed {
                return Err(invalid(path, &format!("{keyword} is not valid for {ty}")));
            }
        }
        let kind = match ty {
            "null" => Kind::Null,
            "boolean" => Kind::Boolean,
            "string" => Kind::String,
            "integer" => match map.get("format").and_then(Value::as_str) {
                None if !map.contains_key("format") => Kind::Integer,
                Some("int64") => Kind::Integer,
                Some("uint64") => Kind::Unsigned,
                _ => return Err(invalid(path, "integer format must be int64 or uint64")),
            },
            "number" => Kind::Number,
            "array" => Kind::Array(Box::new(Self::compile(
                map.get("items")
                    .ok_or_else(|| invalid(path, "array items are required"))?,
                &format!("{path}.items"),
            )?)),
            "object" => {
                if map
                    .get("additionalProperties")
                    .is_some_and(|value| value != &Value::Bool(false))
                {
                    return Err(invalid(
                        path,
                        "additionalProperties must be false when provided",
                    ));
                }
                let properties = map
                    .get("properties")
                    .and_then(Value::as_object)
                    .ok_or_else(|| invalid(path, "object properties are required"))?;
                // Parquet cannot encode a struct with no leaf columns.
                if properties.is_empty() {
                    return Err(invalid(path, "object properties cannot be empty"));
                }
                let mut required = BTreeSet::new();
                if let Some(required_value) = map.get("required") {
                    for name in required_value
                        .as_array()
                        .ok_or_else(|| invalid(path, "required must be an array"))?
                    {
                        let name = name
                            .as_str()
                            .ok_or_else(|| invalid(path, "required entries must be strings"))?;
                        if !properties.contains_key(name) || !required.insert(name) {
                            return Err(invalid(
                                path,
                                "required entries must name distinct declared properties",
                            ));
                        }
                    }
                }
                Kind::Object(
                    properties
                        .iter()
                        .map(|(name, value)| {
                            Ok((
                                name.clone(),
                                Self::compile(value, &format!("{path}.properties.{name}"))?,
                                required.contains(name.as_str()),
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?,
                )
            }
            _ => return Err(invalid(path, "unsupported type")),
        };
        Ok(Self { nullable, kind })
    }

    fn field(&self, name: &str, required: bool) -> Field {
        Field::new(name, self.data_type(), self.nullable || !required)
    }

    fn data_type(&self) -> DataType {
        match &self.kind {
            Kind::Null => DataType::Null,
            Kind::Boolean => DataType::Boolean,
            Kind::String => DataType::Utf8,
            Kind::Integer => DataType::Int64,
            Kind::Unsigned => DataType::UInt64,
            Kind::Number => DataType::Float64,
            Kind::Array(node) => DataType::List(Arc::new(node.field("item", true))),
            Kind::Object(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|(name, node, required)| node.field(name, *required))
                    .collect(),
            ),
        }
    }

    fn accepts(&self, value: &Value, root: bool) -> bool {
        if value.is_null() {
            return self.nullable;
        }
        match (&self.kind, value) {
            (Kind::Boolean, Value::Bool(_)) | (Kind::String, Value::String(_)) => true,
            (Kind::Integer, Value::Number(number)) => number.as_i64().is_some(),
            (Kind::Unsigned, Value::Number(number)) => number.as_u64().is_some(),
            (Kind::Number, Value::Number(_)) => exact_float(value),
            (Kind::Array(node), Value::Array(items)) => {
                items.iter().all(|item| node.accepts(item, false))
            }
            (Kind::Object(fields), Value::Object(map)) => {
                fields
                    .iter()
                    .all(|(name, node, required)| match map.get(name) {
                        Some(value) => node.accepts(value, false),
                        None => !required,
                    })
                    && map.keys().all(|name| {
                        (root && name == "_meta")
                            || fields.iter().any(|(field, _, _)| field == name)
                    })
            }
            _ => false,
        }
    }
}

fn metadata_field() -> Field {
    let mut fields = vec![];
    for name in [
        "record_id",
        "run_id",
        "source_fingerprint",
        "pipeline_hash",
        "stage",
        "generator_provider",
        "generator_model",
        "prompt_hash",
        "template_engine",
    ] {
        fields.push(Field::new(name, DataType::Utf8, false));
    }
    for name in ["source_position", "pipeline_version", "attempt"] {
        fields.push(Field::new(name, DataType::UInt64, false));
    }
    for name in ["judge_provider", "judge_model"] {
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new("judge_score", DataType::Float64, true));
    Field::new("_meta", DataType::Struct(fields.into()), false)
}

/// Arrow's JSON decoder accepts numeric casts that can round or truncate. Check
/// scalar types ourselves before committing a row that would lose information.
pub(crate) fn fits_fields(fields: &Fields, value: &Value) -> bool {
    let Some(map) = value.as_object() else {
        return false;
    };
    map.iter().all(|(name, value)| {
        fields
            .find(name)
            .is_some_and(|(_, field)| fits_type(field.data_type(), value))
    }) && fields.iter().all(|field| {
        field.is_nullable() || map.get(field.name()).is_some_and(|value| !value.is_null())
    })
}

fn fits_type(ty: &DataType, value: &Value) -> bool {
    if value.is_null() {
        return true; // Field/nullability and the explicit contract handle nulls.
    }
    match ty {
        DataType::Null => false,
        DataType::Boolean => value.is_boolean(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => value.is_string(),
        DataType::Int64 => value.as_i64().is_some(),
        DataType::UInt64 => value.as_u64().is_some(),
        DataType::Float64 => value.is_number() && exact_float(value),
        DataType::Struct(fields) => fits_fields(fields, value),
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            value.as_array().is_some_and(|items| {
                items.iter().all(|value| {
                    (child.is_nullable() || !value.is_null()) && fits_type(child.data_type(), value)
                })
            })
        }
        _ => false,
    }
}

fn exact_float(value: &Value) -> bool {
    if let Some(number) = value.as_i64() {
        return (number as f64) as i128 == i128::from(number);
    }
    if let Some(number) = value.as_u64() {
        return (number as f64) as i128 == i128::from(number);
    }
    value.as_f64().is_some_and(f64::is_finite)
}
