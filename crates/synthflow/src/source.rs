use crate::{
    Error, Result,
    spec::{MAX_RECORD_BYTES, SourceConfig, validate_input},
};
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::PathBuf,
};

pub struct Source<'a> {
    input: Input<'a>,
    position: u64,
    finished: bool,
}

enum Input<'a> {
    Inline(std::slice::Iter<'a, Value>),
    Jsonl {
        reader: BufReader<File>,
        path: PathBuf,
    },
}

impl<'a> Source<'a> {
    pub fn open(config: &'a SourceConfig) -> Result<Self> {
        let input = match config {
            SourceConfig::Inline { records } => Input::Inline(records.iter()),
            SourceConfig::Jsonl { path } => Input::Jsonl {
                reader: BufReader::new(File::open(path).map_err(|e| Error::io(path, e))?),
                path: path.clone(),
            },
        };
        Ok(Self {
            input,
            position: 0,
            finished: false,
        })
    }
}

impl Iterator for Source<'_> {
    type Item = Result<(u64, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let result = match &mut self.input {
            Input::Inline(iter) => iter.next().cloned().map(Ok),
            Input::Jsonl { reader, path } => {
                let mut line = Vec::new();
                match reader
                    .take(MAX_RECORD_BYTES as u64 + 1)
                    .read_until(b'\n', &mut line)
                {
                    Ok(0) => None,
                    Ok(size) if size > MAX_RECORD_BYTES => Some(Err(Error::Source {
                        position: self.position + 1,
                        message: "JSONL line exceeds 8 MiB limit".into(),
                    })),
                    Ok(_) => Some(serde_json::from_slice(&line).map_err(|_| Error::Source {
                        position: self.position + 1,
                        message: "invalid JSON (blank lines are not allowed)".into(),
                    })),
                    Err(e) => Some(Err(Error::io(path.clone(), e))),
                }
            }
        };
        let item = result.map(|result| {
            self.position += 1;
            let value = result?;
            validate_input(&value, self.position)?;
            Ok((self.position, value))
        });
        self.finished = item.as_ref().is_none_or(|item| item.is_err());
        item
    }
}
