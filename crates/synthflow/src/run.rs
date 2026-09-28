use crate::{
    Error, Pipeline, Result,
    engine::RunStatistics,
    error::Diagnostic,
    spec::{SourceConfig, normalize_path, suffix},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
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

pub(crate) struct Artifacts {
    accepted: File,
    rejected: File,
}
impl Artifacts {
    pub fn create(pipeline: &Pipeline, fingerprint: String) -> Result<(Self, RunReport)> {
        let output = normalize_path(&pipeline.output.path)?;
        let partial = suffix(&output, ".partial");
        let manifest = suffix(&output, ".manifest.json");
        let dead = pipeline.dead_letter_path()?;
        for path in [&output, &partial, &manifest, &dead] {
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
            statistics: RunStatistics::default(),
            errors: Vec::new(),
        };
        let this = Self { accepted, rejected };
        this.accepted
            .sync_all()
            .map_err(|e| Error::io(&report.partial_path, e))?;
        this.rejected
            .sync_all()
            .map_err(|e| Error::io(&report.dead_letter_path, e))?;
        sync_parent(&report.partial_path)?;
        sync_parent(&report.dead_letter_path)?;
        save_report(&report)?;
        Ok((this, report))
    }
    pub fn write_accepted(&mut self, value: &Value) -> Result<()> {
        write_jsonl(&mut self.accepted, value)
    }
    pub fn write_rejected(&mut self, value: &Value) -> Result<()> {
        write_jsonl(&mut self.rejected, value)
    }

    pub fn commit(&mut self, report: &mut RunReport, position: u64) -> Result<()> {
        self.accepted
            .sync_all()
            .map_err(|e| Error::io(&report.partial_path, e))?;
        self.rejected
            .sync_all()
            .map_err(|e| Error::io(&report.dead_letter_path, e))?;
        let next = SinkState {
            committed_source_position: position,
            committed_accepted_records: report.statistics.accepted_records_total,
            committed_rejected_records: report.statistics.rejected_records_total,
            accepted_bytes: self
                .accepted
                .metadata()
                .map_err(|e| Error::io(&report.partial_path, e))?
                .len(),
            rejected_bytes: self
                .rejected
                .metadata()
                .map_err(|e| Error::io(&report.dead_letter_path, e))?
                .len(),
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

    pub fn publish(&self, report: &mut RunReport) -> Result<()> {
        report.status = RunStatus::Publishing;
        save_report(report)?;
        // Same-directory hard link publishes a fully synced file atomically and
        // fails if another process created the final output. rename would clobber.
        fs::hard_link(&report.partial_path, &report.output_path)
            .map_err(|e| Error::io(&report.output_path, e))?;
        let finalize = (|| {
            sync_parent(&report.output_path)?;
            report.status = RunStatus::Completed;
            save_report(report)
        })();
        if let Err(error) = finalize {
            // This link was created by us above; never remove a pre-existing output.
            fs::remove_file(&report.output_path).map_err(|e| Error::io(&report.output_path, e))?;
            sync_parent(&report.output_path)?;
            return Err(error);
        }
        // A leftover partial link is harmless if cleanup fails after publication.
        if let Err(e) = fs::remove_file(&report.partial_path).and_then(|_| {
            File::open(report.output_path.parent().unwrap_or(Path::new(".")))?.sync_all()
        }) {
            tracing::warn!(error = %e, event = "partial_cleanup_failed");
        }
        Ok(())
    }
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
