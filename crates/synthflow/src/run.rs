use crate::{
    Error, Pipeline, Result,
    engine::RunStatistics,
    error::Diagnostic,
    parquet_schema::{OutputSchema, fits_fields},
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
    #[serde(default)]
    pub source_byte_offset: Option<u64>,
    pub committed_accepted_records: u64,
    pub committed_rejected_records: u64,
    pub accepted_bytes: u64,
    pub rejected_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected_digest: Option<String>,
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
    /// Hash of the fully synced file recorded before linking the final path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_digest: Option<String>,
    /// A complete manifest exists, but creation of the empty data files may
    /// have been interrupted. Only this state permits recreating missing files.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub initializing: bool,
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
/// uses the explicit schema or infers it from the first accepted record.
/// Incompatible rows are rejected at their own position.
pub(crate) struct ParquetChecker {
    schema: Option<SchemaRef>,
    contract: Option<OutputSchema>,
}

impl ParquetChecker {
    fn check(&mut self, value: &Value) -> Result<WriteResult> {
        if self
            .contract
            .as_ref()
            .is_some_and(|contract| !contract.accepts(value))
        {
            return Ok(WriteResult::Rejected(Error::SinkRow(
                "record does not match output.schema".into(),
            )));
        }
        let schema = match &self.schema {
            Some(schema) => schema.clone(),
            None => {
                let schema =
                    arrow_json::reader::infer_json_schema_from_iterator(std::iter::once(Ok(value)))
                        .map_err(|_| Error::Sink("parquet schema inference failed".into()))?;
                SchemaRef::new(schema)
            }
        };
        if !fits_fields(schema.fields(), value) {
            return Ok(WriteResult::Rejected(Error::SinkRow(
                "record fields or values cannot be represented losslessly in the parquet schema"
                    .into(),
            )));
        }
        let line = json_line(value)?;
        let mut reader = arrow_json::ReaderBuilder::new(schema.clone())
            .with_batch_size(1)
            .build(line.as_bytes())
            .map_err(|_| Error::SinkRow("record does not match the parquet schema".into()))?;
        match reader.next() {
            Some(Ok(_)) => {
                self.schema = Some(schema);
                Ok(WriteResult::Written)
            }
            Some(Err(_)) => Ok(WriteResult::Rejected(Error::SinkRow(
                "record does not match the parquet schema".into(),
            ))),
            None => Ok(WriteResult::Rejected(Error::SinkRow(
                "record could not be decoded".into(),
            ))),
        }
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
    accepted_hash: blake3::Hasher,
    rejected_hash: blake3::Hasher,
    committed_accepted_hash: blake3::Hasher,
    committed_rejected_hash: blake3::Hasher,
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
        let model = match pipeline.providers.get(&pipeline.generate.provider) {
            Some(crate::spec::ProviderConfig::OpenaiCompatible { model, .. }) => model.clone(),
            _ => "synthflow-mock-v1".into(),
        };
        let mut report = RunReport {
            run_id: uuid::Uuid::new_v4().to_string(),
            status: RunStatus::Running,
            pipeline_version: pipeline.version,
            pipeline_hash: pipeline.hash()?,
            source_fingerprint: fingerprint,
            publication_digest: None,
            initializing: true,
            started_at_unix_ms: now_ms(),
            finished_at_unix_ms: None,
            output_path: output,
            partial_path: partial,
            dead_letter_path: dead,
            manifest_path: manifest,
            provider: pipeline.generate.provider.clone(),
            model,
            sink_state: SinkState {
                accepted_digest: Some(blake3::hash(&[]).to_hex().to_string()),
                rejected_digest: Some(blake3::hash(&[]).to_hex().to_string()),
                ..SinkState::default()
            },
            resumed_from: None,
            base_accepted_records: 0,
            base_rejected_records: 0,
            statistics: RunStatistics::default(),
            errors: Vec::new(),
        };
        // Publish a fully written reservation atomically. A crash leaves either
        // no manifest, or a resumable zero-progress initialization intent.
        let contract = pipeline
            .output
            .schema
            .as_ref()
            .map(OutputSchema::compile)
            .transpose()?;
        save_new_report(&report)?;
        let accepted = create_new(&report.partial_path)?;
        let rejected = create_new(&report.dead_letter_path)?;
        let this = Self {
            accepted,
            rejected,
            parquet: matches!(pipeline.output.format, OutputFormat::Parquet).then(|| {
                ParquetChecker {
                    schema: contract.as_ref().map(|schema| schema.arrow.clone()),
                    contract,
                }
            }),
            format: pipeline.output.format.clone(),
            batch_size: pipeline.output.batch_size,
            accepted_hash: blake3::Hasher::new(),
            rejected_hash: blake3::Hasher::new(),
            committed_accepted_hash: blake3::Hasher::new(),
            committed_rejected_hash: blake3::Hasher::new(),
        };
        this.accepted
            .sync_all()
            .map_err(|e| Error::io(&report.partial_path, e))?;
        this.rejected
            .sync_all()
            .map_err(|e| Error::io(&report.dead_letter_path, e))?;
        sync_parent(&report.partial_path)?;
        sync_parent(&report.dead_letter_path)?;
        report.initializing = false;
        save_report(&report)?;
        Ok((this, report))
    }

    /// Continue an interrupted run: truncate both files to the
    /// committed prefix, reopen them for appends, and start a fresh report
    /// that carries the prior committed state and provenance.
    pub fn resume(
        pipeline: &Pipeline,
        fingerprint: String,
        prior: &RunReport,
        cancellation: &CancellationToken,
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
        let contract = pipeline
            .output
            .schema
            .as_ref()
            .map(OutputSchema::compile)
            .transpose()?;
        let parquet = if matches!(pipeline.output.format, OutputFormat::Parquet) {
            let schema = match &contract {
                Some(contract) => Some(contract.arrow.clone()),
                None => committed_parquet_schema(&partial, prior)?,
            };
            Some(ParquetChecker { schema, contract })
        } else {
            None
        };
        // Recheck every committed Parquet row against the recovered schema.
        // Older versions permitted lossy casts; valid bytes and a matching hash
        // alone do not establish that those rows meet the current contract.
        // Check both files before any truncate, creation, or manifest update.
        let (accepted_hash, rejected_hash) =
            verified_prefix_hashes(prior, cancellation, parquet.as_ref())?;
        if prior.initializing {
            for path in [&partial, &dead] {
                if !path.try_exists().map_err(|e| Error::io(path, e))? {
                    create_new(path)?
                        .sync_all()
                        .map_err(|e| Error::io(path, e))?;
                    sync_parent(path)?;
                }
            }
        }
        let accepted = open_append_truncated(&partial, prior.sink_state.accepted_bytes)?;
        let rejected = open_append_truncated(&dead, prior.sink_state.rejected_bytes)?;
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
            publication_digest: None,
            initializing: false,
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
            committed_accepted_hash: accepted_hash.clone(),
            committed_rejected_hash: rejected_hash.clone(),
            accepted_hash,
            rejected_hash,
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
        write_jsonl(&mut self.accepted, value, &mut self.accepted_hash)?;
        Ok(WriteResult::Written)
    }
    pub fn write_rejected(&mut self, value: &Value) -> Result<()> {
        write_jsonl(&mut self.rejected, value, &mut self.rejected_hash)
    }

    pub fn commit(
        &mut self,
        report: &mut RunReport,
        position: u64,
        source_byte_offset: Option<u64>,
    ) -> Result<()> {
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
            source_byte_offset,
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
            accepted_digest: Some(self.accepted_hash.finalize().to_hex().to_string()),
            rejected_digest: Some(self.rejected_hash.finalize().to_hex().to_string()),
        };
        // Both data files are durable. If manifest rename succeeds but directory
        // sync fails, it may already reference `next`: never truncate below it.
        // An older manifest still describes a valid prefix of these files.
        report.sink_state = next;
        self.committed_accepted_hash = self.accepted_hash.clone();
        self.committed_rejected_hash = self.rejected_hash.clone();
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
        self.accepted_hash = self.committed_accepted_hash.clone();
        self.rejected_hash = self.committed_rejected_hash.clone();
        Ok(())
    }

    pub fn publish(
        &mut self,
        report: &mut RunReport,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let publication_started = std::time::Instant::now();
        // Parquet publishes the staged conversion; JSONL publishes the
        // partial itself. Either way the link source is fully synced first.
        let staged = match self.format {
            OutputFormat::Jsonl => None,
            OutputFormat::Parquet => Some(write_parquet_output(
                report,
                self.batch_size,
                self.parquet
                    .as_ref()
                    .and_then(|checker| checker.schema.clone()),
                cancellation,
            )?),
        };
        let link_source = staged
            .as_ref()
            .map_or_else(|| report.partial_path.clone(), |path| path.to_path_buf());
        report.publication_digest = Some(fingerprint(
            &SourceConfig::Jsonl {
                path: link_source.clone(),
            },
            cancellation,
        )?);
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
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
            report.finished_at_unix_ms = Some(now_ms());
            report.statistics.elapsed_seconds += publication_started.elapsed().as_secs_f64();
            report.statistics.records_per_second = report.statistics.processed_records_total as f64
                / report.statistics.elapsed_seconds.max(f64::EPSILON);
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
fn write_parquet_output(
    report: &RunReport,
    batch_size: usize,
    schema: Option<SchemaRef>,
    cancellation: &CancellationToken,
) -> Result<tempfile::TempPath> {
    // A unique conversion file makes an abandoned conversion harmless on
    // restart. RAII removes it on every handled failure.
    let temp = tempfile::Builder::new()
        .prefix(".synthflow-publish-")
        .tempfile_in(report.output_path.parent().unwrap_or(Path::new(".")))
        .map_err(|e| Error::io(&report.output_path, e))?;
    let (file, staged) = temp.into_parts();
    let result = (|| -> Result<()> {
        // Use the same schema that validated accepted rows. Re-inferring over
        // the entire prefix can silently promote integers to lossy floats.
        let schema = schema.unwrap_or_else(|| SchemaRef::new(arrow::datatypes::Schema::empty()));
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(batch_size.max(1)))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
            .map_err(|_| Error::Sink("parquet writer init failed".into()))?;
        let partial =
            File::open(&report.partial_path).map_err(|e| Error::io(&report.partial_path, e))?;
        let mut reader = arrow_json::ReaderBuilder::new(schema)
            .with_batch_size(batch_size.max(1))
            .build(std::io::BufReader::new(partial))
            .map_err(|_| Error::Sink("parquet conversion failed".into()))?;
        while let Some(batch) = reader
            .next()
            .transpose()
            .map_err(|_| Error::Sink("parquet conversion failed".into()))?
        {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            writer
                .write(&batch)
                .map_err(|_| Error::Sink("parquet write failed".into()))?;
        }
        writer
            .close()
            .map_err(|_| Error::Sink("parquet close failed".into()))?;
        File::open(&staged)
            .and_then(|file| file.sync_all())
            .map_err(|e| Error::io(&staged, e))?;
        sync_parent(&staged)?;
        Ok(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&staged);
        return Err(if cancellation.is_cancelled() {
            Error::Cancelled
        } else {
            error
        });
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
    let schema = arrow_json::reader::infer_json_schema_from_iterator(std::iter::once(Ok(&value)))
        .map_err(|_| Error::Configuration("cannot rebuild parquet schema".into()))?;
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
        .append(true)
        .create_new(true)
        .open(path)
        .map_err(|e| Error::io(path, e))
}

/// Verify every durable prefix before replaying dedup state or modifying any
/// artifact. Legacy manifests without digests retain structural validation;
/// their next checkpoint records digests as well.
pub(crate) fn verify_committed_prefixes(
    prior: &RunReport,
    cancellation: &CancellationToken,
) -> Result<()> {
    verified_prefix_hashes(prior, cancellation, None).map(|_| ())
}

fn verified_prefix_hashes(
    prior: &RunReport,
    cancellation: &CancellationToken,
    parquet: Option<&ParquetChecker>,
) -> Result<(blake3::Hasher, blake3::Hasher)> {
    let state = &prior.sink_state;
    if prior.initializing {
        let empty = blake3::hash(&[]).to_hex().to_string();
        if state.committed_source_position != 0
            || state.source_byte_offset.is_some()
            || state.committed_accepted_records != 0
            || state.committed_rejected_records != 0
            || state.accepted_bytes != 0
            || state.rejected_bytes != 0
            || state.accepted_digest.as_deref() != Some(&empty)
            || state.rejected_digest.as_deref() != Some(&empty)
        {
            return Err(Error::Configuration(
                "invalid initialization manifest".into(),
            ));
        }
    }
    let accepted = verify_prefix(
        &prior.partial_path,
        state.accepted_bytes,
        state.committed_accepted_records,
        state.accepted_digest.as_deref(),
        prior.initializing,
        cancellation,
        parquet,
    )?;
    let rejected = verify_prefix(
        &prior.dead_letter_path,
        state.rejected_bytes,
        state.committed_rejected_records,
        state.rejected_digest.as_deref(),
        prior.initializing,
        cancellation,
        None,
    )?;
    Ok((accepted, rejected))
}

fn verify_prefix(
    path: &Path,
    bytes: u64,
    rows: u64,
    expected_digest: Option<&str>,
    initializing: bool,
    cancellation: &CancellationToken,
    parquet: Option<&ParquetChecker>,
) -> Result<blake3::Hasher> {
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if initializing && error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(blake3::Hasher::new());
        }
        Err(error) => return Err(Error::io(path, error)),
    };
    let actual = file.metadata().map_err(|e| Error::io(path, e))?.len();
    if actual < bytes || (initializing && actual != 0) {
        return Err(Error::Configuration(format!(
            "committed prefix length does not match the manifest: {}",
            path.display()
        )));
    }
    let mut reader = std::io::BufReader::new(file.take(bytes));
    let mut hash = blake3::Hasher::new();
    let mut count = 0;
    let mut line = Vec::new();
    loop {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        line.clear();
        if reader
            .read_until(b'\n', &mut line)
            .map_err(|e| Error::io(path, e))?
            == 0
        {
            break;
        }
        let row = serde_json::from_slice::<Value>(&line);
        if line.last() != Some(&b'\n') || row.is_err() {
            return Err(Error::Configuration(format!(
                "committed prefix contains an invalid row or boundary: {}",
                path.display()
            )));
        }
        let row = row.expect("validated JSON");
        if parquet.is_some_and(|checker| {
            !checker
                .schema
                .as_ref()
                .is_some_and(|schema| fits_fields(schema.fields(), &row))
                || checker
                    .contract
                    .as_ref()
                    .is_some_and(|contract| !contract.accepts(&row))
        }) {
            return Err(Error::Configuration(
                "committed parquet prefix is incompatible with its fixed schema; use a new output path".into(),
            ));
        }
        hash.update(&line);
        count += 1;
    }
    if count != rows {
        return Err(Error::Configuration(format!(
            "committed prefix does not match the recorded row count: {}",
            path.display()
        )));
    }
    if expected_digest.is_some_and(|expected| hash.finalize().to_hex().as_str() != expected) {
        return Err(Error::Configuration(format!(
            "committed prefix digest mismatch: {}",
            path.display()
        )));
    }
    Ok(hash)
}

fn write_jsonl(writer: &mut impl Write, value: &Value, hash: &mut blake3::Hasher) -> Result<()> {
    let mut row =
        serde_json::to_vec(value).map_err(|_| Error::Sink("JSON serialization failed".into()))?;
    row.push(b'\n');
    writer
        .write_all(&row)
        .map_err(|e| Error::Sink(format!("JSONL write failed: {e}")))?;
    hash.update(&row);
    Ok(())
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

fn save_new_report(report: &RunReport) -> Result<()> {
    let path = &report.manifest_path;
    let parent = path.parent().unwrap_or(Path::new("."));
    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|e| Error::io(parent, e))?;
    serde_json::to_writer_pretty(&mut temp, report)
        .map_err(|_| Error::Sink("manifest serialization/write failed".into()))?;
    temp.as_file().sync_all().map_err(|e| Error::io(path, e))?;
    temp.persist_noclobber(path)
        .map_err(|e| Error::io(path, e.error))?;
    sync_parent(path)
}
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| Error::io(parent, e))
}

/// Called only with the namespace lock held and source/config identity checked.
/// Complete a link made before a crash only if it matches the durable publish
/// intent. Unknown or conflicting final files are never removed or overwritten.
pub(crate) fn finish_interrupted_publication(
    report: &mut RunReport,
    cancellation: &CancellationToken,
) -> Result<bool> {
    if !report
        .output_path
        .try_exists()
        .map_err(|e| Error::io(&report.output_path, e))?
    {
        return Ok(false);
    }
    if report.status != RunStatus::Publishing {
        return Err(Error::Configuration(
            "final output exists without a recoverable publication intent".into(),
        ));
    }
    let expected = report.publication_digest.as_ref().ok_or_else(|| {
        Error::Configuration(
            "legacy publishing manifest lacks an output digest; inspect artifacts manually".into(),
        )
    })?;
    let actual = fingerprint(
        &SourceConfig::Jsonl {
            path: report.output_path.clone(),
        },
        cancellation,
    )?;
    if actual != *expected {
        return Err(Error::Configuration(
            "final output does not match the recorded publication digest".into(),
        ));
    }
    // The published file replaces the accepted partial as recovery evidence,
    // but the dead-letter file remains part of the completed dataset. Verify
    // its committed prefix before recording success, even when cleanup has
    // already removed the accepted partial.
    verify_prefix(
        &report.dead_letter_path,
        report.sink_state.rejected_bytes,
        report.sink_state.committed_rejected_records,
        report.sink_state.rejected_digest.as_deref(),
        false,
        cancellation,
        None,
    )?;
    File::open(&report.output_path)
        .and_then(|file| file.sync_all())
        .map_err(|e| Error::io(&report.output_path, e))?;
    sync_parent(&report.output_path)?;
    report.status = RunStatus::Completed;
    report.finished_at_unix_ms = Some(now_ms());
    save_report(report)?;
    if report.partial_path.exists()
        && let Err(error) = fs::remove_file(&report.partial_path).and_then(|_| {
            File::open(report.partial_path.parent().unwrap_or(Path::new(".")))?.sync_all()
        })
    {
        tracing::warn!(%error, event = "partial_cleanup_failed");
    }
    Ok(true)
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
            write_jsonl(
                &mut BrokenWriter,
                &serde_json::json!({"a":1}),
                &mut blake3::Hasher::new()
            ),
            Err(Error::Sink(_))
        ));
    }

    fn fixture(dir: &tempfile::TempDir) -> Pipeline {
        serde_json::from_value(serde_json::json!({
            "version": 1,
            "dataset": {"name": "integrity"},
            "source": {"type": "inline", "records": []},
            "providers": {"mock": {"type": "mock", "response": "{}"}},
            "generate": {"provider": "mock", "prompt": "Generate", "output_schema": {"type": "object"}},
            "output": {"format": "jsonl", "path": dir.path().join("result.jsonl")}
        })).expect("spec")
    }

    #[test]
    fn both_digests_are_verified_before_any_tail_is_truncated() {
        for corrupt_accepted in [true, false] {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let spec = fixture(&dir);
            let (mut artifacts, mut report) =
                Artifacts::create(&spec, "source".into()).expect("create");
            assert!(matches!(
                artifacts
                    .write_accepted(1, &serde_json::json!({"n": 1}))
                    .expect("write"),
                WriteResult::Written
            ));
            artifacts
                .write_rejected(&serde_json::json!({"n": 1}))
                .expect("reject");
            report.statistics.accepted_records_total = 1;
            report.statistics.rejected_records_total = 1;
            artifacts.commit(&mut report, 2, None).expect("commit");
            drop(artifacts);
            let corrupted = if corrupt_accepted {
                &report.partial_path
            } else {
                &report.dead_letter_path
            };
            let mut content = fs::read(corrupted).expect("read");
            let digit = content
                .iter()
                .position(|byte| *byte == b'1')
                .expect("digit");
            content[digit] = b'2'; // Same length and valid JSON; only the digest catches this.
            fs::write(corrupted, content).expect("corrupt");
            for path in [&report.partial_path, &report.dead_letter_path] {
                OpenOptions::new()
                    .append(true)
                    .open(path)
                    .expect("open")
                    .write_all(b"uncommitted tail")
                    .expect("tail");
            }
            let accepted = fs::read(&report.partial_path).expect("accepted");
            let rejected = fs::read(&report.dead_letter_path).expect("rejected");
            let error =
                match Artifacts::resume(&spec, "source".into(), &report, &CancellationToken::new())
                {
                    Ok(_) => panic!("corruption must be refused"),
                    Err(error) => error,
                };
            assert!(error.to_string().contains("digest mismatch"), "{error}");
            assert_eq!(fs::read(&report.partial_path).expect("accepted"), accepted);
            assert_eq!(
                fs::read(&report.dead_letter_path).expect("rejected"),
                rejected
            );
        }
    }

    #[test]
    fn incremental_digests_survive_rollback_and_legacy_resume() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let spec = fixture(&dir);
        let (mut artifacts, mut report) =
            Artifacts::create(&spec, "source".into()).expect("create");
        artifacts
            .write_accepted(1, &serde_json::json!({"n": 1}))
            .expect("write");
        report.statistics.accepted_records_total = 1;
        artifacts.commit(&mut report, 1, None).expect("commit");
        artifacts
            .write_accepted(2, &serde_json::json!({"n": 99}))
            .expect("tail");
        artifacts
            .write_rejected(&serde_json::json!({"n": 99}))
            .expect("tail");
        artifacts.rollback_uncommitted(&report).expect("rollback");
        artifacts
            .write_accepted(2, &serde_json::json!({"n": 2}))
            .expect("write");
        report.statistics.accepted_records_total = 2;
        artifacts.commit(&mut report, 2, None).expect("commit");
        assert_eq!(
            report.sink_state.accepted_digest,
            Some(
                blake3::hash(&fs::read(&report.partial_path).expect("read"))
                    .to_hex()
                    .to_string()
            )
        );
        assert_eq!(
            report.sink_state.rejected_digest,
            Some(blake3::hash(&[]).to_hex().to_string())
        );
        drop(artifacts);
        report.sink_state.accepted_digest = None;
        report.sink_state.rejected_digest = None;
        let (mut artifacts, mut resumed) =
            Artifacts::resume(&spec, "source".into(), &report, &CancellationToken::new())
                .expect("legacy resume");
        artifacts
            .write_accepted(3, &serde_json::json!({"n": 3}))
            .expect("write");
        resumed.statistics.accepted_records_total = 1;
        artifacts.commit(&mut resumed, 3, None).expect("commit");
        verify_committed_prefixes(&resumed, &CancellationToken::new()).expect("integrity");
        assert_eq!(resumed.sink_state.committed_accepted_records, 3);
        assert_eq!(
            resumed.sink_state.accepted_digest,
            Some(
                blake3::hash(&fs::read(&resumed.partial_path).expect("read"))
                    .to_hex()
                    .to_string()
            )
        );
    }

    #[test]
    fn initialization_recovers_missing_empty_files_but_preserves_unknown_content() {
        for corrupt_existing in [true, false] {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let spec = fixture(&dir);
            let (artifacts, mut report) =
                Artifacts::create(&spec, "source".into()).expect("create");
            drop(artifacts);
            report.initializing = true;
            save_report(&report).expect("initializing intent");
            fs::remove_file(&report.partial_path).expect("remove");
            fs::remove_file(&report.dead_letter_path).expect("remove");
            if corrupt_existing {
                fs::write(&report.dead_letter_path, b"unknown data").expect("unknown");
                assert!(
                    Artifacts::resume(&spec, "source".into(), &report, &CancellationToken::new())
                        .is_err()
                );
                assert!(
                    !report.partial_path.exists(),
                    "validate both before creating either"
                );
                assert_eq!(
                    fs::read(&report.dead_letter_path).expect("read"),
                    b"unknown data"
                );
            } else {
                verify_committed_prefixes(&report, &CancellationToken::new())
                    .expect("initializing prefix");
                let (_, resumed) =
                    Artifacts::resume(&spec, "source".into(), &report, &CancellationToken::new())
                        .expect("resume");
                assert!(!resumed.initializing);
                assert_eq!(
                    fs::metadata(&resumed.partial_path).expect("partial").len(),
                    0
                );
                assert_eq!(
                    fs::metadata(&resumed.dead_letter_path)
                        .expect("rejected")
                        .len(),
                    0
                );
            }
        }
    }

    #[test]
    fn missing_files_without_initialization_intent_are_never_recreated() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let spec = fixture(&dir);
        let (artifacts, report) = Artifacts::create(&spec, "source".into()).expect("create");
        drop(artifacts);
        fs::remove_file(&report.partial_path).expect("remove");
        assert!(
            Artifacts::resume(&spec, "source".into(), &report, &CancellationToken::new()).is_err()
        );
        assert!(!report.partial_path.exists());
    }

    #[test]
    fn legacy_parquet_lossy_prefixes_are_refused_before_any_artifact_changes() {
        // A valid digest proves byte integrity, not compatibility with the
        // stricter type checks introduced after these rows were committed.
        for with_digests in [false, true] {
            for values in [
                [serde_json::json!(1), serde_json::json!(1.5)],
                [
                    serde_json::json!(1.5),
                    serde_json::json!(9007199254740993i64),
                ],
            ] {
                let dir = tempfile::TempDir::new().expect("tempdir");
                let mut spec = fixture(&dir);
                spec.output.format = OutputFormat::Parquet;
                let (artifacts, mut report) =
                    Artifacts::create(&spec, "source".into()).expect("create");
                drop(artifacts);
                let prefix: String = values
                    .into_iter()
                    .map(|n| json_line(&serde_json::json!({"n": n})).expect("line"))
                    .collect();
                report.sink_state.committed_source_position = 2;
                report.sink_state.committed_accepted_records = 2;
                report.sink_state.accepted_bytes = prefix.len() as u64;
                report.sink_state.accepted_digest =
                    with_digests.then(|| blake3::hash(prefix.as_bytes()).to_hex().to_string());
                report.sink_state.rejected_digest =
                    with_digests.then(|| blake3::hash(&[]).to_hex().to_string());
                fs::write(
                    &report.partial_path,
                    format!("{prefix}uncommitted accepted tail"),
                )
                .expect("accepted");
                fs::write(&report.dead_letter_path, b"uncommitted rejected tail")
                    .expect("rejected");
                save_report(&report).expect("legacy manifest");
                let before = [
                    &report.partial_path,
                    &report.dead_letter_path,
                    &report.manifest_path,
                ]
                .map(|path| fs::read(path).expect("snapshot"));
                // Deserialize the stored manifest so the no-digest case also
                // covers absent legacy fields rather than only in-memory None.
                let prior = serde_json::from_slice(&before[2]).expect("manifest");
                let error = match Artifacts::resume(
                    &spec,
                    "source".into(),
                    &prior,
                    &CancellationToken::new(),
                ) {
                    Ok(_) => panic!("lossy committed prefix must be refused"),
                    Err(error) => error,
                };
                assert!(
                    error
                        .to_string()
                        .contains("committed parquet prefix is incompatible"),
                    "{error}"
                );
                for (index, path) in [
                    &report.partial_path,
                    &report.dead_letter_path,
                    &report.manifest_path,
                ]
                .iter()
                .enumerate()
                {
                    assert_eq!(fs::read(path).expect("unchanged artifact"), before[index]);
                }
                assert!(!report.output_path.exists());
            }
        }
    }
}
