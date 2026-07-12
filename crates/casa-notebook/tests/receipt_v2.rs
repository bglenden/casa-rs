// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, path::PathBuf};

use casa_notebook::{
    ExecutionInput, ExecutionReceipt, ExecutionStatus, NotebookStore, PythonEnvironmentIdentity,
    PythonExecutionAuthority, PythonExecutionInput, ReceiptFinalization, RecordingRequest,
    RunSafetyRecord, StoreError, Timestamp,
};

fn environment() -> PythonEnvironmentIdentity {
    PythonEnvironmentIdentity::new(
        "project-python-3.13",
        PathBuf::from(".casa-rs/python/bin/python3"),
        "cpython",
        "3.13.5",
        Some("0.24.1".into()),
        BTreeMap::from([
            ("casa-rs".into(), "0.24.1".into()),
            ("numpy".into(), "2.3.1".into()),
        ]),
    )
}

fn request(input: PythonExecutionInput) -> RecordingRequest {
    RecordingRequest {
        initiating_surface: "macos_gui".into(),
        operation_id: "python.execute".into(),
        notebook_id: None,
        cell_id: None,
        task_intent: None,
        execution_input: Some(ExecutionInput::Python(input)),
        provider_contract_version: 1,
        resolved_parameters: BTreeMap::new(),
        run_safety: RunSafetyRecord {
            classification: "writes_notebook_artifacts".into(),
            affected_paths: vec![PathBuf::from("notebooks/assets/spectrum.png")],
        },
        approvals: Vec::new(),
    }
}

fn finalization() -> ReceiptFinalization {
    ReceiptFinalization {
        status: ExecutionStatus::Succeeded,
        finished_at: Timestamp::now(),
        affected_paths: Vec::new(),
        products: Vec::new(),
        artifacts: Vec::new(),
        diagnostics: Vec::new(),
        stdout: b"42\n".to_vec(),
        stderr: Vec::new(),
        casa_log: None,
    }
}

#[test]
fn python_receipt_v2_preserves_exact_source_authority_inputs_and_environment() {
    let root = tempfile::tempdir().expect("temporary project");
    let store = NotebookStore::open(root.path().canonicalize().expect("canonical project"))
        .expect("open notebook store");
    let source = "from casars import image\n\nprint(image.shape)\n";
    let input = PythonExecutionInput::new(
        source,
        PythonExecutionAuthority::User,
        vec![PathBuf::from("data/twhya.image")],
        environment(),
    );
    let expected = input.clone();

    let handle = store
        .begin_attempt(request(input))
        .expect("begin Python attempt");
    let receipt = store
        .finalize_attempt(&handle, finalization())
        .expect("finalize Python attempt");

    assert_eq!(receipt.schema_version, 2);
    assert_eq!(
        receipt.execution_input,
        Some(ExecutionInput::Python(expected.clone()))
    );
    assert!(expected.has_valid_source_hash());
    assert!(expected.environment.has_valid_fingerprint());

    let reopened = store
        .receipts_for_notebook(handle.notebook_id)
        .expect("reopen receipts");
    assert_eq!(reopened.len(), 1);
    assert_eq!(reopened[0].execution_input, receipt.execution_input);
}

#[test]
fn tampered_python_source_or_environment_is_rejected_before_recording() {
    let root = tempfile::tempdir().expect("temporary project");
    let store = NotebookStore::open(root.path().canonicalize().expect("canonical project"))
        .expect("open notebook store");
    let mut source_tampered = PythonExecutionInput::new(
        "print(1)\n",
        PythonExecutionAuthority::AiWorker,
        Vec::new(),
        environment(),
    );
    source_tampered.source.push_str("print(2)\n");
    assert!(matches!(
        store.begin_attempt(request(source_tampered)),
        Err(StoreError::InvalidExecutionInput { .. })
    ));

    let mut environment_tampered = PythonExecutionInput::new(
        "print(1)\n",
        PythonExecutionAuthority::AiWorker,
        Vec::new(),
        environment(),
    );
    environment_tampered
        .environment
        .packages
        .insert("numpy".into(), "0.0.0".into());
    assert!(matches!(
        store.begin_attempt(request(environment_tampered)),
        Err(StoreError::InvalidExecutionInput { .. })
    ));
}

#[test]
fn schema_v1_receipts_remain_readable_without_execution_input() {
    let receipt: ExecutionReceipt = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "run_id": "019f69dc-35f8-7000-8000-000000000001",
        "revision": 1,
        "notebook_id": "019f69dc-35f8-7000-8000-000000000002",
        "cell_id": "019f69dc-35f8-7000-8000-000000000003",
        "initiating_surface": "gui",
        "operation_id": "imager",
        "started_at": 1,
        "finished_at": 2,
        "status": "succeeded",
        "sparse_intent": null,
        "resolved_parameters": {},
        "provider_contract_version": 1,
        "run_safety": {"classification": "read_only", "affected_paths": []},
        "approvals": [],
        "affected_paths": [],
        "products": [],
        "artifacts": [],
        "logs": {},
        "diagnostics": [],
        "replay_claim": "historical resolved values"
    }))
    .expect("deserialize a schema-v1 receipt");

    assert_eq!(receipt.schema_version, 1);
    assert_eq!(receipt.execution_input, None);
}
