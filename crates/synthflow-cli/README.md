# SynthFlow CLI

Run local synthetic data pipelines with resumable checkpoints.

From the source checkout:

```sh
cargo install --path crates/synthflow-cli --locked
synthflow validate examples/simple_generation.yaml
synthflow run examples/simple_generation.yaml
synthflow inspect examples/output/rust_instruction.jsonl
```

Use `synthflow resume pipeline.yaml` after an interrupted run. Run/resume emit
JSON reports on stdout and progress logs on stderr. SIGINT and SIGTERM request
graceful cancellation; a second termination signal forces exit.

The [project documentation](https://github.com/a49a/synth-flow) includes
configuration examples, teaching labs, and the supported filesystem contract.
Requires Rust 1.93 or newer. License: MIT.
