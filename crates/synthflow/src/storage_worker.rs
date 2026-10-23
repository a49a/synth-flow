use crate::{
    Error, Result,
    blocking::BlockingWorker,
    dedup::{DedupAdmission, DedupState},
    run::{Artifacts, RunLock, RunReport, WriteResult, save_report},
};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

struct State {
    artifacts: Artifacts,
    dedup: Option<DedupState>,
    _lock: RunLock,
}

/// Serializes dedup admission and sink writes through one bounded mailbox.
/// The worker owns the output lock until every queued operation has exited.
pub(crate) struct StorageWorker(BlockingWorker<State>);

impl StorageWorker {
    pub(crate) fn new(
        artifacts: Artifacts,
        dedup: Option<DedupState>,
        lock: RunLock,
    ) -> Result<Self> {
        Ok(Self(BlockingWorker::new(State {
            artifacts,
            dedup,
            _lock: lock,
        })?))
    }

    pub(crate) async fn accept(&self, mut input: Value, meta: Value) -> Result<WriteResult> {
        self.0
            .call(move |state| {
                let key = match &state.dedup {
                    None => None,
                    Some(dedup) => match dedup.check(&input) {
                        Ok(DedupAdmission::Duplicate) => {
                            return Ok(WriteResult::Rejected(Error::Duplicate));
                        }
                        Ok(DedupAdmission::Unique(key)) => Some(key),
                        Err(error @ Error::DedupField { .. }) => {
                            return Ok(WriteResult::Rejected(error));
                        }
                        Err(error) => return Err(error),
                    },
                };
                input
                    .as_object_mut()
                    .ok_or_else(|| Error::Sink("expected object".into()))?
                    .insert("_meta".into(), meta);
                let result = state.artifacts.write_accepted(0, &input)?;
                if matches!(result, WriteResult::Written)
                    && let (Some(dedup), Some(key)) = (&mut state.dedup, key)
                {
                    dedup.register(key)?;
                }
                Ok(result)
            })
            .await
    }

    pub(crate) async fn write_rejected(&self, row: Value) -> Result<()> {
        self.0
            .call(move |state| state.artifacts.write_rejected(&row))
            .await
    }

    async fn update(
        &self,
        report: &mut RunReport,
        operation: impl FnOnce(&mut Artifacts, &mut RunReport) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        let mut next = report.clone();
        let (next, result) = self
            .0
            .call(move |state| {
                let result = operation(&mut state.artifacts, &mut next);
                Ok((next, result))
            })
            .await?;
        // A failed manifest sync may already have committed the newer prefix.
        *report = next;
        result
    }

    pub(crate) async fn commit(
        &self,
        report: &mut RunReport,
        position: u64,
        offset: Option<u64>,
    ) -> Result<()> {
        self.update(report, move |artifacts, report| {
            artifacts.commit(report, position, offset)
        })
        .await
    }

    pub(crate) async fn publish(
        &self,
        report: &mut RunReport,
        token: CancellationToken,
    ) -> Result<()> {
        self.update(report, move |artifacts, report| {
            artifacts.publish(report, &token)
        })
        .await
    }

    pub(crate) async fn rollback(&self, report: &RunReport) -> Result<()> {
        let report = report.clone();
        self.0
            .call(move |state| state.artifacts.rollback_uncommitted(&report))
            .await
    }

    pub(crate) async fn save(&self, report: &RunReport) -> Result<()> {
        let report = report.clone();
        self.0.call(move |_| save_report(&report)).await
    }

    pub(crate) async fn close(self) -> Result<()> {
        self.0.close().await
    }
}
