//! Local, streaming synthetic-data pipelines. Reliable streaming execution with mock and HTTP providers.
pub mod dedup;
pub mod engine;
pub mod error;
pub mod provider;
mod ratelimit;
pub mod record;
pub mod run;
pub mod source;
pub mod spec;
pub mod template;

pub use engine::{RunStatistics, run, run_async, run_with_provider};
pub use error::{Error, Result};
pub use run::{RunReport, RunStatus};
pub use spec::Pipeline;
