# SynthFlow library

A local-first Rust engine for synthetic data pipelines: streaming JSONL/CSV,
OpenAI-compatible HTTP and offline mock providers, schema validation, judging,
exact/MinHash deduplication, JSONL/Parquet output, and checkpoint recovery.

Async callers use `run_async(&pipeline, cancellation)` and
`resume_async(&pipeline, cancellation)`. Check `report.succeeded()` even when
the call returns `Ok(report)`: failed and cancelled runs retain their report.

See the [project documentation](https://github.com/a49a/synth-flow) for the
pipeline YAML reference, examples, and reliability guarantees.

Requires Rust 1.93 or newer. Supported local filesystems: Linux and macOS.
License: MIT.
