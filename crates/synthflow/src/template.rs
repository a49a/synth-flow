use minijinja::{Environment, UndefinedBehavior};
use serde_json::Value;
use std::io::{self, Write};

use crate::{Error, Result};

pub fn environment() -> Environment<'static> {
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Strict);
    env.set_fuel(Some(100_000));
    env
}

pub fn validate(text: &str, stage: &str) -> Result<()> {
    environment()
        .template_from_str(text)
        .map_err(|e| Error::Configuration(format!("invalid {stage} template: {}", e.kind())))?;
    Ok(())
}

pub fn render(env: &Environment<'_>, name: &str, context: &Value) -> Result<String> {
    let mut output = LimitedOutput(Vec::new());
    env.get_template(name)
        .and_then(|t| t.render_captured_to(context, &mut output).map(|_| ()))
        .map_err(|e| Error::Template {
            stage: name.into(),
            message: e.kind().to_string(),
        })?;
    String::from_utf8(output.0).map_err(|_| Error::Template {
        stage: name.into(),
        message: "invalid UTF-8 output".into(),
    })
}

struct LimitedOutput(Vec<u8>);

impl Write for LimitedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > crate::spec::MAX_RECORD_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("template output exceeds 8 MiB limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
