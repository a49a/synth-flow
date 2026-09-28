use serde_json::Value;

#[test]
fn cli_inspect_prints_table_and_json() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data.jsonl");
    std::fs::write(&data, "{\"topic\": \"a\"}\n{\"topic\": \"b\"}\n").expect("write");
    let binary = env!("CARGO_BIN_EXE_synthflow");
    let table = std::process::Command::new(binary)
        .arg("inspect")
        .arg(&data)
        .output()
        .expect("inspect");
    assert!(table.status.success());
    let text = String::from_utf8(table.stdout).expect("utf8");
    assert!(text.contains("2 rows"), "{text}");
    assert!(text.contains("topic"), "{text}");

    let json = std::process::Command::new(binary)
        .arg("inspect")
        .arg(&data)
        .arg("--json")
        .output()
        .expect("inspect json");
    assert!(json.status.success());
    let parsed: Value = serde_json::from_slice(&json.stdout).expect("json");
    assert_eq!(parsed["rows"], 2);
    assert_eq!(parsed["columns"][0]["name"], "topic");
}
