use std::{fs, process::Command};

#[test]
fn shipped_demo_produces_three_accepted_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/simple_generation.yaml");
    let mut spec = synthflow::Pipeline::load(example).expect("load shipped example");
    spec.output.path = dir.path().join("result.jsonl");
    let config = dir.path().join("pipeline.yaml");
    fs::write(&config, serde_json::to_vec(&spec).expect("serialize")).expect("write");
    let result = Command::new(env!("CARGO_BIN_EXE_synthflow"))
        .arg("run")
        .arg(config)
        .output()
        .expect("run");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stats: serde_json::Value = serde_json::from_slice(&result.stdout).expect("stats");
    assert_eq!(stats["accepted_records_total"], 3);
    assert_eq!(stats["rejected_records_total"], 0);
    assert_eq!(
        fs::read_to_string(spec.output.path)
            .expect("output")
            .lines()
            .count(),
        3
    );
}

#[test]
fn cli_help_validate_plan_run_and_failure_exit_codes() {
    let binary = env!("CARGO_BIN_EXE_synthflow");
    assert!(
        Command::new(binary)
            .arg("--help")
            .output()
            .expect("help")
            .status
            .success()
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = dir.path().join("pipeline.yaml");
    fs::write(
        &pipeline,
        r#"
version: 1
dataset: {name: cli_test}
source:
  type: inline
  records: [{topic: Rust}]
providers:
  mock:
    type: mock
    response: '{"answer": {{ prompt | tojson }}}'
generate:
  provider: mock
  prompt: 'Explain {{ topic }}'
  output_schema:
    type: object
    required: [answer]
    properties: {answer: {type: string}}
output: {format: jsonl, path: output/result.jsonl}
"#,
    )
    .expect("write");
    for command in ["validate", "plan", "run"] {
        let result = Command::new(binary)
            .arg(command)
            .arg(&pipeline)
            .output()
            .expect("command");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let stdout = String::from_utf8_lossy(&result.stdout);
        match command {
            "validate" => assert!(stdout.contains("Valid pipeline")),
            "plan" => assert!(stdout.contains("SchemaValidate")),
            _ => assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&result.stdout).expect("stats")["accepted_records_total"],
                1
            ),
        }
    }
    assert!(dir.path().join("output/result.jsonl").is_file());
    assert!(
        !Command::new(binary)
            .arg("run")
            .arg(&pipeline)
            .output()
            .expect("rerun")
            .status
            .success()
    );
    assert!(
        !Command::new(binary)
            .arg("validate")
            .arg(dir.path().join("missing.yaml"))
            .output()
            .expect("missing")
            .status
            .success()
    );
}
