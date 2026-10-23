use serde_json::json;
use std::fs;
use synthflow::{Pipeline, RunReport, RunStatus, resume_async, run_async};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn fixture(dir: &TempDir, format: &str) -> Pipeline {
    serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "finalization"},
        "source": {"type": "inline", "records": [{"topic": "accepted"}, {"topic": "rejected"}]},
        "providers": {"generator": {
            "type": "mock",
            "response": "{% if record.topic == 'accepted' %}{\"answer\":\"valid\"}{% else %}invalid-json{% endif %}"
        }},
        "generate": {
            "provider": "generator", "prompt": "Explain {{ record.topic }}",
            "output_schema": {"type": "object", "required": ["answer"], "properties": {"answer": {"type": "string"}}}
        },
        "output": {"format": format, "path": dir.path().join(format!("result.{format}"))}
    }))
    .unwrap()
}

async fn published_intent(pipeline: &Pipeline) -> RunReport {
    let mut report = run_async(pipeline, CancellationToken::new()).await.unwrap();
    assert!(report.succeeded(), "{:?}", report.errors);
    assert_eq!(report.sink_state.committed_accepted_records, 1);
    assert_eq!(report.sink_state.committed_rejected_records, 1);
    assert!(!report.partial_path.exists());
    // Recreate the recoverable publication phase with the fully linked final
    // file. The accepted partial is deliberately absent, as it may already
    // have been cleaned up; the final digest is sufficient to verify it.
    report.status = RunStatus::Publishing;
    fs::write(&report.manifest_path, serde_json::to_vec(&report).unwrap()).unwrap();
    report
}

#[tokio::test]
async fn publication_recovery_requires_an_intact_dead_letter_prefix() {
    for format in ["jsonl", "parquet"] {
        for remove in [false, true] {
            let dir = TempDir::new().unwrap();
            let pipeline = fixture(&dir, format);
            let report = published_intent(&pipeline).await;
            let final_before = fs::read(&report.output_path).unwrap();
            let manifest_before = fs::read(&report.manifest_path).unwrap();
            if remove {
                fs::remove_file(&report.dead_letter_path).unwrap();
            } else {
                let original = fs::read_to_string(&report.dead_letter_path).unwrap();
                let changed = original.replace("\"source_position\":2", "\"source_position\":3");
                assert_ne!(changed, original);
                assert_eq!(changed.len(), original.len());
                fs::write(&report.dead_letter_path, changed).unwrap();
            }
            let rejected_before = fs::read(&report.dead_letter_path).ok();
            let error = resume_async(&pipeline, CancellationToken::new())
                .await
                .expect_err("corrupt or missing rejected data must prevent completion");
            if !remove {
                assert!(error.to_string().contains("digest mismatch"), "{error}");
            }
            assert_eq!(fs::read(&report.output_path).unwrap(), final_before);
            assert_eq!(fs::read(&report.manifest_path).unwrap(), manifest_before);
            assert_eq!(fs::read(&report.dead_letter_path).ok(), rejected_before);
            assert!(!report.partial_path.exists());
        }
    }
}

#[tokio::test]
async fn intact_publication_recovers_without_the_accepted_partial() {
    for format in ["jsonl", "parquet"] {
        let dir = TempDir::new().unwrap();
        let pipeline = fixture(&dir, format);
        let report = published_intent(&pipeline).await;
        let final_before = fs::read(&report.output_path).unwrap();
        let rejected_before = fs::read(&report.dead_letter_path).unwrap();
        let resumed = resume_async(&pipeline, CancellationToken::new())
            .await
            .unwrap();
        assert!(resumed.succeeded(), "{:?}", resumed.errors);
        assert_eq!(fs::read(&report.output_path).unwrap(), final_before);
        assert_eq!(fs::read(&report.dead_letter_path).unwrap(), rejected_before);
        assert!(!report.partial_path.exists());
        let persisted: RunReport =
            serde_json::from_slice(&fs::read(&report.manifest_path).unwrap()).unwrap();
        assert_eq!(persisted.status, RunStatus::Completed);
    }
}

#[tokio::test]
async fn parquet_inference_errors_do_not_expose_model_response_values() {
    let dir = TempDir::new().unwrap();
    let response = json!({"answer": [1, {"hidden": "SECRET_MODEL_RESPONSE"}]}).to_string();
    let pipeline: Pipeline = serde_json::from_value(json!({
        "version": 1,
        "dataset": {"name": "safe-parquet-diagnostics"},
        "source": {"type": "inline", "records": [{"topic": "public"}]},
        "providers": {"generator": {"type": "mock", "response": response}},
        "generate": {"provider": "generator", "prompt": "Generate", "output_schema": {"type": "object"}},
        "output": {"format": "parquet", "path": dir.path().join("result.parquet")}
    })).unwrap();
    let report = run_async(&pipeline, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.status, RunStatus::Failed);
    assert_eq!(report.errors[0].category, "sink");
    assert_eq!(
        report.errors[0].message,
        "sink error: parquet schema inference failed"
    );
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains("SECRET_MODEL_RESPONSE")
    );
    assert!(
        !fs::read_to_string(&report.manifest_path)
            .unwrap()
            .contains("SECRET_MODEL_RESPONSE")
    );
    assert!(
        !fs::read_to_string(&report.dead_letter_path)
            .unwrap()
            .contains("SECRET_MODEL_RESPONSE")
    );
}
