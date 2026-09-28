//! CSV parsing with caller-owned, bounded buffers (including headers).
use crate::{Error, Result, spec::MAX_RECORD_BYTES};
use csv_core::ReadRecordResult;
use std::io::BufRead;

const MAX_FIELDS: usize = 65_536;
pub(crate) struct BoundedCsv<R> {
    input: R,
    offset: u64,
    parser: csv_core::Reader,
    data: Vec<u8>,
    ends: Vec<usize>,
}
impl<R: BufRead> BoundedCsv<R> {
    pub(crate) fn new(input: R) -> Self {
        Self {
            input,
            offset: 0,
            parser: csv_core::Reader::new(),
            data: vec![0; 8192],
            ends: vec![0; 128],
        }
    }
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    pub(crate) fn row(&mut self, position: u64) -> Result<Option<Vec<String>>> {
        let error = |message: &str| Error::Source {
            position,
            message: message.into(),
        };
        let (mut raw, mut written, mut fields) = (0, 0, 0);
        loop {
            let input = self
                .input
                .fill_buf()
                .map_err(|_| error("CSV read failed"))?;
            let input = &input[..input.len().min(MAX_RECORD_BYTES + 1 - raw)];
            let (result, consumed, output, count) =
                self.parser
                    .read_record(input, &mut self.data[written..], &mut self.ends[fields..]);
            self.input.consume(consumed);
            self.offset += consumed as u64;
            raw += consumed;
            written += output;
            fields += count;
            if raw > MAX_RECORD_BYTES || written > MAX_RECORD_BYTES {
                return Err(error("CSV row exceeds 8 MiB limit"));
            }
            if fields > MAX_FIELDS {
                return Err(error("CSV row exceeds 65536 fields"));
            }
            match result {
                ReadRecordResult::End => return Ok(None),
                ReadRecordResult::Record => {
                    let mut start = 0;
                    let mut row = Vec::with_capacity(fields);
                    for &end in &self.ends[..fields] {
                        let value = std::str::from_utf8(&self.data[start..end])
                            .map_err(|_| error("CSV parse failed: invalid UTF-8"))?;
                        row.push(value.to_owned());
                        start = end;
                    }
                    return Ok(Some(row));
                }
                ReadRecordResult::OutputFull => {
                    if self.data.len() == MAX_RECORD_BYTES + 1 {
                        return Err(error("CSV row exceeds 8 MiB limit"));
                    }
                    self.data
                        .resize((self.data.len() * 2).min(MAX_RECORD_BYTES + 1), 0);
                }
                ReadRecordResult::OutputEndsFull => {
                    if self.ends.len() == MAX_FIELDS + 1 {
                        return Err(error("CSV row exceeds 65536 fields"));
                    }
                    self.ends
                        .resize((self.ends.len() * 2).min(MAX_FIELDS + 1), 0);
                }
                ReadRecordResult::InputEmpty => {}
            }
        }
    }
}

impl<R: BufRead + std::io::Seek> BoundedCsv<R> {
    pub(crate) fn seek_to(&mut self, offset: u64) -> std::io::Result<()> {
        let end = self.input.seek(std::io::SeekFrom::End(0))?;
        if offset > end {
            return Err(std::io::Error::other("source offset exceeds file length"));
        }
        self.input.seek(std::io::SeekFrom::Start(offset))?;
        self.parser = csv_core::Reader::new();
        // A resumed data row may legitimately begin with a BOM character;
        // BOM stripping belongs only to the beginning of the source file.
        self.parser.read_record(b"\n", &mut [0; 1], &mut [0; 1]);
        self.offset = offset;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Read};
    struct Infinite {
        byte: u8,
        consumed: usize,
    }
    impl Read for Infinite {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            out.fill(self.byte);
            self.consumed += out.len();
            Ok(out.len())
        }
    }
    impl BufRead for Infinite {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Ok(if self.byte == b'a' {
                &[b'a'; 8192]
            } else {
                &[b','; 8192]
            })
        }
        fn consume(&mut self, amount: usize) {
            self.consumed += amount;
        }
    }
    #[test]
    fn oversized_input_stops_reading_at_the_budget() {
        let mut csv = BoundedCsv::new(Infinite {
            byte: b'a',
            consumed: 0,
        });
        assert!(csv.row(0).is_err());
        assert!(csv.input.consumed <= MAX_RECORD_BYTES + 1);
        assert!(csv.data.len() <= MAX_RECORD_BYTES + 1);
    }
    #[test]
    fn too_many_empty_fields_are_bounded() {
        let mut csv = BoundedCsv::new(Infinite {
            byte: b',',
            consumed: 0,
        });
        assert!(csv.row(0).is_err());
        assert!(csv.input.consumed <= MAX_FIELDS + 2);
        assert!(csv.ends.len() <= MAX_FIELDS + 1);
    }
}
