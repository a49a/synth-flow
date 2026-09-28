use crate::{
    Error, Result,
    bounded_csv::BoundedCsv,
    spec::{MAX_RECORD_BYTES, SourceConfig, validate_input},
};
use serde_json::{Map, Value};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
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
        reader: Box<BoundedCsv<BufReader<File>>>,
        headers: Vec<String>,
    },
}

impl<'a> Source<'a> {
    pub(crate) fn byte_offset(&mut self) -> Result<Option<u64>> {
        match &mut self.input {
            Input::Inline(_) => Ok(None),
            Input::Jsonl { reader, path } => reader
                .stream_position()
                .map(Some)
                .map_err(|e| Error::io(path.clone(), e)),
            Input::Csv { reader, .. } => Ok(Some(reader.offset())),
        }
    }
    pub(crate) fn resume_from(&mut self, position: u64, offset: Option<u64>) -> Result<()> {
        match &mut self.input {
            Input::Inline(iter) => {
                if position > iter.len() as u64 {
                    return Err(Error::Configuration(
                        "committed source position exceeds input length".into(),
                    ));
                }
                if position > 0 {
                    iter.nth(position as usize - 1);
                }
            }
            Input::Jsonl { reader, path } => {
                let Some(offset) = offset else {
                    return Ok(());
                };
                if offset
                    > reader
                        .get_ref()
                        .metadata()
                        .map_err(|e| Error::io(path.clone(), e))?
                        .len()
                {
                    return Err(Error::Configuration(
                        "source offset exceeds file length".into(),
                    ));
                }
                reader
                    .seek(SeekFrom::Start(offset))
                    .map_err(|e| Error::io(path.clone(), e))?;
            }
            Input::Csv { reader, .. } => {
                let Some(offset) = offset else {
                    return Ok(());
                };
                reader
                    .seek_to(offset)
                    .map_err(|e| Error::Configuration(format!("invalid CSV source offset: {e}")))?;
            }
        }
        self.position = position;
        Ok(())
    }
    pub fn open(config: &'a SourceConfig) -> Result<Self> {
        let input = match config {
            SourceConfig::Inline { records } => Input::Inline(records.iter()),
            SourceConfig::Jsonl { path } => Input::Jsonl {
                reader: BufReader::new(File::open(path).map_err(|e| Error::io(path, e))?),
                path: path.clone(),
            },
            SourceConfig::Csv { path } => {
                let mut reader = BoundedCsv::new(BufReader::new(
                    File::open(path).map_err(|e| Error::io(path, e))?,
                ));
                let headers = reader.row(0)?.unwrap_or_default();
                let mut unique = std::collections::HashSet::new();
                for header in &headers {
                    if header.trim().is_empty() || !unique.insert(header) {
                        return Err(Error::Source {
                            position: 0,
                            message: "CSV headers must be non-empty and unique".into(),
                        });
                    }
                }
                Input::Csv {
                    reader: Box::new(reader),
                    headers,
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
            Input::Csv { reader, headers } => {
                let position = self.position + 1;
                match reader.row(position) {
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                    Ok(Some(record)) if record.len() != headers.len() => Some(Err(Error::Source {
                        position,
                        message: "CSV parse failed: row does not match the header width".into(),
                    })),
                    Ok(Some(record)) => {
                        let map: Map<String, Value> = headers
                            .iter()
                            .cloned()
                            .zip(record.into_iter().map(Value::String))
                            .collect();
                        Some(Ok(Value::Object(map)))
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn csv_resume_preserves_bom_inside_a_data_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.csv");
        std::fs::write(&path, "topic\r\na\r\n\u{feff}b\r\nc").unwrap();
        let config = SourceConfig::Csv { path };
        let mut original = Source::open(&config).unwrap();
        original.next().unwrap().unwrap();
        let offset = original.byte_offset().unwrap();
        let expected = original.next().unwrap().unwrap();
        let mut resumed = Source::open(&config).unwrap();
        resumed.resume_from(1, offset).unwrap();
        assert_eq!(resumed.next().unwrap().unwrap(), expected);
        assert_eq!(expected.1["topic"], "\u{feff}b");
    }
}
