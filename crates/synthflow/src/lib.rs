//! Local, streaming synthetic-data pipelines. Reliable streaming execution with mock and HTTP providers.
pub mod dedup;
pub mod engine;
pub mod error;
pub mod inspect;
pub mod provider;
mod ratelimit;
pub mod record;
pub mod run;
pub mod source;
pub mod spec;
pub mod template;

pub use engine::{
    RunStatistics, resume_async, resume_with_provider, run, run_async, run_with_provider,
};
pub use error::{Error, Result};
pub use run::{ResumedFrom, RunReport, RunStatus};
pub use spec::Pipeline;
