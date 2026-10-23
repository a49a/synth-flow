use crate::run::RunReport;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct Snapshot {
    run_id: String,
    processed: u64,
    accepted: u64,
    rejected: u64,
    committed: u64,
    in_flight: usize,
    phase: &'static str,
}
impl Snapshot {
    fn emit(&self, started: Instant) {
        tracing::info!(
            run_id = self.run_id,
            processed = self.processed,
            accepted = self.accepted,
            rejected = self.rejected,
            committed = self.committed,
            in_flight = self.in_flight,
            phase = self.phase,
            rate = self.processed as f64 / started.elapsed().as_secs_f64().max(f64::EPSILON),
            event = "progress"
        );
    }
    fn new(report: &RunReport, in_flight: usize, phase: &'static str) -> Self {
        Self {
            run_id: report.run_id.clone(),
            processed: report.statistics.processed_records_total,
            accepted: report.statistics.accepted_records_total,
            rejected: report.statistics.rejected_records_total,
            committed: report.sink_state.committed_source_position,
            in_flight,
            phase,
        }
    }
}

pub(crate) struct ProgressReporter {
    sender: tokio::sync::watch::Sender<Snapshot>,
    task: tokio::task::JoinHandle<()>,
    started: Instant,
    last_emitted: u64,
}
impl ProgressReporter {
    pub(crate) fn new(report: &RunReport, started: Instant) -> Self {
        let (sender, receiver) = tokio::sync::watch::channel(Snapshot::new(report, 0, "starting"));
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(2),
                Duration::from_secs(2),
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                receiver.borrow().emit(started);
            }
        });
        Self {
            sender,
            task,
            started,
            last_emitted: 0,
        }
    }
    pub(crate) fn update(&mut self, report: &RunReport, in_flight: usize, phase: &'static str) {
        let snapshot = Snapshot::new(report, in_flight, phase);
        if snapshot.processed.saturating_sub(self.last_emitted) >= 500 {
            snapshot.emit(self.started);
            self.last_emitted = snapshot.processed;
        }
        self.sender.send_replace(snapshot);
    }
}
impl Drop for ProgressReporter {
    fn drop(&mut self) {
        self.task.abort();
    }
}
