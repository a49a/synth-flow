use crate::{
    Error, Result,
    spec::{MAX_RECORD_BYTES, SourceConfig, validate_input},
};
use serde_json::{Map, Value};
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
    Csv {
        reader: csv::Reader<File>,
        headers: csv::StringRecord,
        record: csv::StringRecord,
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
            SourceConfig::Csv { path } => {
                let mut reader = csv::ReaderBuilder::new()
                    .flexible(false)
                    .from_path(path)
                    .map_err(|e| Error::Source {
                        position: 0,
                        message: format!("cannot open CSV source: {e}"),
                    })?;
                let headers = reader
                    .headers()
                    .map_err(|e| Error::Source {
                        position: 0,
                        message: format!("cannot read CSV header: {e}"),
                    })?
                    .clone();
                Input::Csv {
                    reader,
                    headers,
                    record: csv::StringRecord::new(),
                }
            }
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
            Input::Csv {
                reader,
                headers,
                record,
            } => {
                let position = self.position + 1;
                match reader.read_record(record) {
                    Ok(false) => None,
                    Ok(true) => {
                        let bytes =
                            headers.iter().map(str::len).sum::<usize>() + record.as_slice().len();
                        if bytes > MAX_RECORD_BYTES {
                            Some(Err(Error::Source {
                                position,
                                message: "CSV row exceeds 8 MiB limit".into(),
                            }))
                        } else if record.len() != headers.len() {
                            Some(Err(Error::Source {
                                position,
                                message: "CSV row does not match the header width".into(),
                            }))
                        } else {
                            // Every value arrives as a string; duplicate
                            // headers keep the last occurrence.
                            let mut map = Map::new();
                            for (header, value) in headers.iter().zip(record.iter()) {
                                map.insert(header.to_owned(), Value::String(value.to_owned()));
                            }
                            Some(Ok(Value::Object(map)))
                        }
                    }
                    Err(e) => Some(Err(Error::Source {
                        position,
                        message: format!("CSV parse failed: {e}"),
                    })),
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
