//! Local, streaming synthetic-data pipelines. Reliable streaming execution with mock and HTTP providers.
mod blocking;
mod bounded_csv;
pub mod dedup;
pub mod engine;
pub mod error;
pub mod inspect;
mod parquet_schema;
mod progress;
pub mod provider;
mod ratelimit;
pub mod record;
pub mod run;
pub mod source;
pub mod spec;
mod storage_worker;
pub mod template;

pub use engine::{
    RunStatistics, resume_async, resume_with_provider, run, run_async, run_with_provider,
};
pub use error::{Error, Result};
pub use run::{ResumedFrom, RunReport, RunStatus};
pub use spec::Pipeline;
