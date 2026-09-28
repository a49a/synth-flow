use crate::{
    Error, Pipeline, Result,
    engine::RunStatistics,
    error::Diagnostic,
    spec::{OutputFormat, SourceConfig, normalize_path, suffix},
};
use arrow::datatypes::SchemaRef;
use fs2::FileExt;
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Publishing,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SinkState {
    pub committed_source_position: u64,
    pub committed_accepted_records: u64,
    pub committed_rejected_records: u64,
    pub accepted_bytes: u64,
    pub rejected_bytes: u64,
}

/// Provenance of a resumed run: which interrupted run produced the
/// committed prefix this run continues.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumedFrom {
    pub run_id: String,
    pub accepted_records: u64,
    pub rejected_records: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub run_id: String,
    pub status: RunStatus,
    pub pipeline_version: u32,
    pub pipeline_hash: String,
    pub source_fingerprint: String,
    pub started_at_unix_ms: u128,
    pub finished_at_unix_ms: Option<u128>,
    pub output_path: PathBuf,
    pub partial_path: PathBuf,
    pub dead_letter_path: PathBuf,
    pub manifest_path: PathBuf,
    pub provider: String,
    pub model: String,
    pub sink_state: SinkState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_from: Option<ResumedFrom>,
    // Counts carried over from the run being resumed; fresh runs keep zero.
    #[serde(skip)]
    pub base_accepted_records: u64,
    #[serde(skip)]
    pub base_rejected_records: u64,
    #[serde(flatten)]
    pub statistics: RunStatistics,
    pub errors: Vec<Diagnostic>,
}
impl RunReport {
    pub fn succeeded(&self) -> bool {
        self.status == RunStatus::Completed
    }
}
pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Exclusive, advisory, cross-process lock over one output namespace. A run
/// or resume holds it from the first checkpoint read until the final manifest
/// write; concurrent attempts fail fast instead of racing on shared files.
pub(crate) struct RunLock {
    file: File,
}

impl RunLock {
    pub(crate) fn acquire(output: &Path) -> Result<Self> {
        let lock_path = suffix(output, ".lock");
        if let Some(parent) = lock_path.parent() {
            create_directory_durable(parent)?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| Error::io(&lock_path, e))?;
        file.try_lock_exclusive().map_err(|_| {
            Error::Configuration(format!(
                "another process holds the run lock: {}",
                lock_path.display()
            ))
        })?;
        Ok(Self { file })
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        // Release explicitly; the file itself may outlive the run harmlessly.
        let _ = self.file.unlock();
    }
}

pub fn fingerprint(source: &SourceConfig, cancellation: &CancellationToken) -> Result<String> {
    let mut hash = blake3::Hasher::new();
    match source {
        SourceConfig::Inline { records } => {
            hash.update(
                serde_json::to_string(records)
                    .map_err(|_| Error::Configuration("invalid inline source".into()))?
                    .as_bytes(),
            );
        }
        SourceConfig::Jsonl { path } | SourceConfig::Csv { path } => {
            let mut file = File::open(path).map_err(|e| Error::io(path, e))?;
            let mut buffer = [0u8; 65536];
            loop {
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                let count = file.read(&mut buffer).map_err(|e| Error::io(path, e))?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
            }
        }
    }
    Ok(hash.finalize().to_hex().to_string())
}

/// A record-level sink decision; parquet rows that cannot be represented in
/// the inferred schema are rejected instead of failing the whole run.
pub(crate) enum WriteResult {
    Written,
    Rejected(Error),
}

/// Parquet output always commits a JSONL partial (readable prefix, identical
/// resume semantics) and converts it to parquet at publish time. This checker
/// decodes every row against the schema inferred from the first record so
/// incompatible rows are rejected at their own position.
pub(crate) struct ParquetChecker {
    schema: Option<SchemaRef>,
}

impl ParquetChecker {
    fn check(&mut self, value: &Value) -> Result<WriteResult> {
        let schema = match &self.schema {
            Some(schema) => schema.clone(),
            None => {
                let schema =
                    arrow_json::reader::infer_json_schema_from_iterator(std::iter::once(Ok(value)))
                        .map_err(|e| {
                            Error::Sink(format!("parquet schema inference failed: {e}"))
                        })?;
                let schema = SchemaRef::new(schema);
                self.schema = Some(schema.clone());
                schema
            }
        };
        if !covers_fields(schema.fields(), value) {
            return Ok(WriteResult::Rejected(Error::SinkRow(
                "record contains a field outside the parquet schema".into(),
            )));
        }
        let line = json_line(value)?;
        let mut reader = arrow_json::ReaderBuilder::new(schema)
            .with_batch_size(1)
            .build(line.as_bytes())
            .map_err(|e| {
                Error::SinkRow(format!("record does not match the parquet schema: {e}"))
            })?;
        match reader.next() {
            Some(Ok(_)) => Ok(WriteResult::Written),
            Some(Err(_)) => Ok(WriteResult::Rejected(Error::SinkRow(
                "record does not match the parquet schema".into(),
            ))),
            None => Ok(WriteResult::Rejected(Error::SinkRow(
                "record could not be decoded".into(),
            ))),
        }
    }
}

/// arrow-json silently drops fields that are absent from the schema, so the
/// checker rejects such rows explicitly instead of losing data.
fn covers_fields(fields: &arrow::datatypes::Fields, value: &Value) -> bool {
    let Some(map) = value.as_object() else {
        return true;
    };
    map.iter().all(|(key, field_value)| match fields.find(key) {
        None => false,
        Some((_, field)) => value_fits(field.data_type(), field_value),
    })
}

fn value_fits(data_type: &arrow::datatypes::DataType, value: &Value) -> bool {
    use arrow::datatypes::DataType;
    match (data_type, value) {
        (DataType::Struct(child), _) => covers_fields(child, value),
        (
            DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _),
            Value::Array(items),
        ) => items.iter().all(|item| value_fits(child.data_type(), item)),
        // Scalars are left to arrow-json's decoder, which rejects type clashes.
        _ => true,
    }
}

fn json_line(value: &Value) -> Result<String> {
    let mut line = serde_json::to_string(value)
        .map_err(|_| Error::Sink("JSON serialization failed".into()))?;
    line.push('\n');
    Ok(line)
}

pub(crate) struct Artifacts {
    accepted: File,
    rejected: File,
    parquet: Option<ParquetChecker>,
    format: OutputFormat,
    batch_size: usize,
}

impl Artifacts {
    pub fn create(pipeline: &Pipeline, fingerprint: String) -> Result<(Self, RunReport)> {
        let output = normalize_path(&pipeline.output.path)?;
        let partial = suffix(&output, ".partial");
        let manifest = suffix(&output, ".manifest.json");
        let dead = pipeline.dead_letter_path()?;
        let publishing = suffix(&output, ".publishing");
        for path in [&output, &partial, &manifest, &dead, &publishing] {
            if path.try_exists().map_err(|e| Error::io(path, e))? {
                return Err(Error::Configuration(format!(
                    "run artifact already exists: {}",
                    path.display()
                )));
            }
            if let Some(parent) = path.parent() {
                create_directory_durable(parent)?;
            }
        }
        // Reserve this output namespace. No existing artifacts are ever removed.
        let reservation = create_new(&manifest)?;
        let accepted = match create_new(&partial) {
            Ok(f) => f,
            Err(e) => {
                let _ = fs::remove_file(&manifest);
                return Err(e);
            }
        };
        let rejected = match create_new(&dead) {
            Ok(f) => f,
            Err(e) => {
                let _ = fs::remove_file(&partial);
                let _ = fs::remove_file(&manifest);
                return Err(e);
            }
        };
        drop(reservation);
        let model = match pipeline.providers.get(&pipeline.generate.provider) {
            Some(crate::spec::ProviderConfig::OpenaiCompatible { model, .. }) => model.clone(),
            _ => "synthflow-mock-v1".into(),
        };
        let report = RunReport {
            run_id: uuid::Uuid::new_v4().to_string(),
            status: RunStatus::Running,
            pipeline_version: pipeline.version,
            pipeline_hash: pipeline.hash()?,
            source_fingerprint: fingerprint,
            started_at_unix_ms: now_ms(),
            finished_at_unix_ms: None,
            output_path: output,
            partial_path: partial,
            dead_letter_path: dead,
            manifest_path: manifest,
            provider: pipeline.generate.provider.clone(),
            model,
            sink_state: SinkState::default(),
            resumed_from: None,
            base_accepted_records: 0,
            base_rejected_records: 0,
            statistics: RunStatistics::default(),
            errors: Vec::new(),
        };
        let this = Self {
            accepted,
            rejected,
            parquet: matches!(pipeline.output.format, OutputFormat::Parquet)
                .then_some(ParquetChecker { schema: None }),
            format: pipeline.output.format.clone(),
            batch_size: pipeline.output.batch_size,
        };
        this.rejected
            .sync_all()
            .map_err(|e| Error::io(&report.dead_letter_path, e))?;
        sync_parent(&report.partial_path)?;
        sync_parent(&report.dead_letter_path)?;
        save_report(&report)?;
        Ok((this, report))
    }

    /// Continue a failed or cancelled run: truncate both files to the
    /// committed prefix, reopen them for appends, and start a fresh report
    /// that carries the prior committed state and provenance.
    pub fn resume(
        pipeline: &Pipeline,
        fingerprint: String,
        prior: &RunReport,
    ) -> Result<(Self, RunReport)> {
        let output = normalize_path(&pipeline.output.path)?;
        let partial = suffix(&output, ".partial");
        let manifest = suffix(&output, ".manifest.json");
        let dead = pipeline.dead_letter_path()?;
        if prior.partial_path != partial
            || prior.manifest_path != manifest
            || prior.dead_letter_path != dead
        {
            return Err(Error::Configuration(
                "artifact paths changed since the recorded run".into(),
            ));
        }
        if output.try_exists().map_err(|e| Error::io(&output, e))? {
            return Err(Error::Configuration(
                "final output already exists; nothing to resume".into(),
            ));
        }
        let accepted = open_append_truncated(&partial, prior.sink_state.accepted_bytes)?;
        let rejected = open_append_truncated(&dead, prior.sink_state.rejected_bytes)?;
        let parquet = matches!(pipeline.output.format, OutputFormat::Parquet)
            .then(|| committed_parquet_schema(&partial, prior))
            .transpose()?
            .map(|schema| ParquetChecker { schema });
        let model = match pipeline.providers.get(&pipeline.generate.provider) {
            Some(crate::spec::ProviderConfig::OpenaiCompatible { model, .. }) => model.clone(),
            _ => "synthflow-mock-v1".into(),
        };
        let report = RunReport {
            run_id: uuid::Uuid::new_v4().to_string(),
            status: RunStatus::Running,
            pipeline_version: pipeline.version,
            pipeline_hash: pipeline.hash()?,
            source_fingerprint: fingerprint,
            started_at_unix_ms: now_ms(),
            finished_at_unix_ms: None,
            output_path: output,
            partial_path: partial,
            dead_letter_path: dead,
            manifest_path: manifest,
            provider: pipeline.generate.provider.clone(),
            model,
            sink_state: prior.sink_state.clone(),
            resumed_from: Some(ResumedFrom {
                run_id: prior.run_id.clone(),
                accepted_records: prior.sink_state.committed_accepted_records,
                rejected_records: prior.sink_state.committed_rejected_records,
            }),
            base_accepted_records: prior.sink_state.committed_accepted_records,
            base_rejected_records: prior.sink_state.committed_rejected_records,
            statistics: RunStatistics::default(),
            errors: Vec::new(),
        };
        let this = Self {
            accepted,
            rejected,
            parquet,
            format: pipeline.output.format.clone(),
            batch_size: pipeline.output.batch_size,
        };
        save_report(&report)?;
        Ok((this, report))
    }

    pub fn write_accepted(&mut self, _position: u64, value: &Value) -> Result<WriteResult> {
        if let Some(checker) = &mut self.parquet
            && let WriteResult::Rejected(error) = checker.check(value)?
        {
            return Ok(WriteResult::Rejected(error));
        }
        write_jsonl(&mut self.accepted, value)?;
        Ok(WriteResult::Written)
    }
    pub fn write_rejected(&mut self, value: &Value) -> Result<()> {
        write_jsonl(&mut self.rejected, value)
    }

    pub fn commit(&mut self, report: &mut RunReport, position: u64) -> Result<()> {
        self.rejected
            .sync_all()
            .map_err(|e| Error::io(&report.dead_letter_path, e))?;
        let rejected_bytes = self
            .rejected
            .metadata()
            .map_err(|e| Error::io(&report.dead_letter_path, e))?
            .len();
        self.accepted
            .sync_all()
            .map_err(|e| Error::io(&report.partial_path, e))?;
        let next = SinkState {
            committed_source_position: position,
            committed_accepted_records: report.base_accepted_records
                + report.statistics.accepted_records_total,
            committed_rejected_records: report.base_rejected_records
                + report.statistics.rejected_records_total,
            accepted_bytes: self
                .accepted
                .metadata()
                .map_err(|e| Error::io(&report.partial_path, e))?
                .len(),
            rejected_bytes,
        };
        // Both data files are durable. If manifest rename succeeds but directory
        // sync fails, it may already reference `next`: never truncate below it.
        // An older manifest still describes a valid prefix of these files.
        report.sink_state = next;
        save_report(report)
    }

    pub fn rollback_uncommitted(&mut self, report: &RunReport) -> Result<()> {
        self.accepted
            .set_len(report.sink_state.accepted_bytes)
            .and_then(|_| self.accepted.sync_all())
            .map_err(|e| Error::io(&report.partial_path, e))?;
        self.rejected
            .set_len(report.sink_state.rejected_bytes)
            .and_then(|_| self.rejected.sync_all())
            .map_err(|e| Error::io(&report.dead_letter_path, e))?;
        Ok(())
    }

    pub fn publish(&mut self, report: &mut RunReport) -> Result<()> {
        // Parquet publishes the staged conversion; JSONL publishes the
        // partial itself. Either way the link source is fully synced first.
        let link_source = match self.format {
            OutputFormat::Jsonl => report.partial_path.clone(),
            OutputFormat::Parquet => write_parquet_output(report, self.batch_size)?,
        };
        report.status = RunStatus::Publishing;
        if let Err(error) = save_report(report) {
            if link_source != report.partial_path {
                let _ = fs::remove_file(&link_source);
            }
            return Err(error);
        }
        // Same-directory hard link publishes a fully synced file atomically and
        // fails if another process created the final output. rename would clobber.
        if let Err(e) = fs::hard_link(&link_source, &report.output_path) {
            // The JSONL checkpoint survives every publish failure; only the
            // disposable staged conversion is cleaned up here.
            if link_source != report.partial_path {
                let _ = fs::remove_file(&link_source);
            }
            return Err(Error::io(&report.output_path, e));
        }
        let finalize = (|| {
            sync_parent(&report.output_path)?;
            report.status = RunStatus::Completed;
            save_report(report)
        })();
        if let Err(error) = finalize {
            // This link was created by us above; never remove a pre-existing output.
            fs::remove_file(&report.output_path).map_err(|e| Error::io(&report.output_path, e))?;
            sync_parent(&report.output_path)?;
            if link_source != report.partial_path {
                let _ = fs::remove_file(&link_source);
            }
            return Err(error);
        }
        // Only after the completed state is durable do intermediates expire:
        // the staged conversion and, for parquet runs, the JSONL checkpoint.
        if let Err(e) = (|| -> std::io::Result<()> {
            if link_source != report.partial_path {
                fs::remove_file(&link_source)?;
            }
            fs::remove_file(&report.partial_path)?;
            File::open(report.output_path.parent().unwrap_or(Path::new(".")))?.sync_all()
        })() {
            tracing::warn!(error = %e, event = "partial_cleanup_failed");
        }
        Ok(())
    }
}

/// Convert the durable JSONL partial into the final parquet file. The
/// temporary conversion lives next to the output so publication is still an
/// atomic same-directory hard link. The JSONL partial is the resume
/// checkpoint and is never modified: a later publish failure must still be
/// able to truncate and parse it as JSONL.
fn write_parquet_output(report: &RunReport, batch_size: usize) -> Result<PathBuf> {
    let staged = suffix(&report.output_path, ".publishing");
    // An existing staged path can belong to another writer or a failed
    // process. Never delete it implicitly.
    let file = create_new(&staged)?;
    let result = (|| -> Result<()> {
        let schema = match report.sink_state.committed_accepted_records {
            0 => SchemaRef::new(arrow::datatypes::Schema::empty()),
            _ => {
                let partial = File::open(&report.partial_path)
                    .map_err(|e| Error::io(&report.partial_path, e))?;
                let lines = std::io::BufReader::new(partial).lines().map(|line| {
                    let line =
                        line.map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e)))?;
                    serde_json::from_str::<Value>(&line).map_err(|e| {
                        arrow_schema::ArrowError::ExternalError(Box::new(std::io::Error::other(
                            format!("partial row is not valid JSON: {e}"),
                        )))
                    })
                });
                let schema = arrow_json::reader::infer_json_schema_from_iterator(lines)
                    .map_err(|e| Error::Sink(format!("parquet schema inference failed: {e}")))?;
                SchemaRef::new(schema)
            }
        };
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(batch_size.max(1)))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
            .map_err(|e| Error::Sink(format!("parquet writer init failed: {e}")))?;
        let partial =
            File::open(&report.partial_path).map_err(|e| Error::io(&report.partial_path, e))?;
        let mut reader = arrow_json::ReaderBuilder::new(schema)
            .with_batch_size(batch_size.max(1))
            .build(std::io::BufReader::new(partial))
            .map_err(|e| Error::Sink(format!("parquet conversion failed: {e}")))?;
        while let Some(batch) = reader
            .next()
            .transpose()
            .map_err(|e| Error::Sink(format!("parquet conversion failed: {e}")))?
        {
            writer
                .write(&batch)
                .map_err(|e| Error::Sink(format!("parquet write failed: {e}")))?;
        }
        writer
            .close()
            .map_err(|e| Error::Sink(format!("parquet close failed: {e}")))?;
        File::open(&staged)
            .and_then(|file| file.sync_all())
            .map_err(|e| Error::io(&staged, e))?;
        sync_parent(&staged)?;
        Ok(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&staged);
        return Err(error);
    }
    Ok(staged)
}

fn create_directory_durable(path: &Path) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        create_directory_durable(parent)?;
    }
    match fs::create_dir(path) {
        Ok(()) => sync_parent(path),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        Err(error) => Err(Error::io(path, error)),
    }
}

/// Rebuild the parquet schema a continuous run would have inferred: take the
/// first row of the committed prefix. All committed rows were checked against
/// that schema, so inference over them yields the same fields and types.
fn committed_parquet_schema(partial: &Path, prior: &RunReport) -> Result<Option<SchemaRef>> {
    use std::io::{BufRead, Read};
    if prior.sink_state.committed_accepted_records == 0 {
        // Nothing committed: a fresh inference on the first new record matches
        // what a continuous run would have done.
        return Ok(None);
    }
    let file = File::open(partial).map_err(|e| Error::io(partial, e))?;
    let mut line = String::new();
    std::io::BufReader::new(file)
        .take(prior.sink_state.accepted_bytes)
        .read_line(&mut line)
        .map_err(|e| Error::io(partial, e))?;
    let value: Value = serde_json::from_str(line.trim_end())
        .map_err(|_| Error::Configuration("committed prefix contains an invalid row".into()))?;
    let schema =
        arrow_json::reader::infer_json_schema_from_iterator(std::iter::once(Ok(&value)))
            .map_err(|e| Error::Configuration(format!("cannot rebuild parquet schema: {e}")))?;
    Ok(Some(SchemaRef::new(schema)))
}

fn open_append_truncated(path: &Path, committed_bytes: u64) -> Result<File> {
    let file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|e| Error::io(path, e))?;
    let actual = file.metadata().map_err(|e| Error::io(path, e))?.len();
    if actual < committed_bytes {
        return Err(Error::Configuration(format!(
            "committed prefix is shorter than the manifest records: {}",
            path.display()
        )));
    }
    file.set_len(committed_bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| Error::io(path, e))?;
    Ok(file)
}

fn create_new(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| Error::io(path, e))
}

pub(crate) fn write_jsonl(writer: &mut impl Write, value: &Value) -> Result<()> {
    let mut row =
        serde_json::to_vec(value).map_err(|_| Error::Sink("JSON serialization failed".into()))?;
    row.push(b'\n');
    writer
        .write_all(&row)
        .map_err(|e| Error::Sink(format!("JSONL write failed: {e}")))
}

pub(crate) fn save_report(report: &RunReport) -> Result<()> {
    let path = &report.manifest_path;
    let parent = path.parent().unwrap_or(Path::new("."));
    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|e| Error::io(parent, e))?;
    serde_json::to_writer_pretty(&mut temp, report)
        .map_err(|_| Error::Sink("manifest serialization/write failed".into()))?;
    temp.as_file().sync_all().map_err(|e| Error::io(path, e))?;
    temp.persist(path).map_err(|e| Error::io(path, e.error))?;
    sync_parent(path)
}
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| Error::io(parent, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    struct BrokenWriter;
    impl Write for BrokenWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk full"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn sink_write_failures_are_fatal() {
        assert!(matches!(
            write_jsonl(&mut BrokenWriter, &serde_json::json!({"a":1})),
            Err(Error::Sink(_))
        ));
    }
}
