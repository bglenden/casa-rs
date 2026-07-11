// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, fs, path::PathBuf};

use casa_notebook::{
    ArtifactReference, CellId, ConflictResolution, ExecutionStatus, ExportMode, NotebookDocument,
    NotebookId, NotebookStore, ReceiptFinalization, RecordingPolicy, RecordingRequest,
    RunSafetyRecord, SaveResult, TaskCellIntent, Timestamp,
};
use tempfile::TempDir;

fn project() -> (TempDir, NotebookStore) {
    let root = tempfile::tempdir().expect("temporary project");
    let store = NotebookStore::open(root.path().canonicalize().expect("canonical project"))
        .expect("open notebook store");
    (root, store)
}

fn intent() -> TaskCellIntent {
    TaskCellIntent {
        format: 1,
        surface: "imager".into(),
        kind: "task".into(),
        contract: 3,
        parameters: BTreeMap::from([
            ("vis".into(), toml::Value::String("data/twhya.ms".into())),
            ("niter".into(), toml::Value::Integer(1000)),
        ]),
    }
}

fn request(notebook_id: Option<NotebookId>, cell_id: Option<CellId>) -> RecordingRequest {
    RecordingRequest {
        initiating_surface: "gui".into(),
        operation_id: "imager".into(),
        notebook_id,
        cell_id,
        task_intent: Some(intent()),
        provider_contract_version: 3,
        resolved_parameters: BTreeMap::from([
            ("vis".into(), serde_json::json!("data/twhya.ms")),
            ("niter".into(), serde_json::json!(1000)),
        ]),
        run_safety: RunSafetyRecord {
            classification: "writes_products".into(),
            affected_paths: vec![PathBuf::from("products/twhya")],
        },
        approvals: Vec::new(),
    }
}

fn terminal(status: ExecutionStatus) -> ReceiptFinalization {
    ReceiptFinalization {
        status,
        finished_at: Timestamp::now(),
        affected_paths: vec![PathBuf::from("products/twhya.image")],
        products: vec![ArtifactReference {
            role: "image".into(),
            path: PathBuf::from("products/twhya.image"),
            media_type: None,
        }],
        artifacts: Vec::new(),
        diagnostics: Vec::new(),
        stdout: b"structured output".to_vec(),
        stderr: Vec::new(),
        casa_log: Some(PathBuf::from("casa.log")),
    }
}

#[test]
fn markdown_parser_preserves_unknown_content_and_future_cells_byte_for_byte() {
    let notebook_id = NotebookId::new();
    let future_cell = CellId::new();
    let source = format!(
        "<!-- casa-rs-notebook:v1 id={notebook_id} -->\n\n# Heading\n\n<div data-user=\"yes\">raw</div>\n\n<!-- future comment -->\n\n<!-- casa-rs-cell:v1 id={future_cell} kind=future-science -->\n```mystery\n{{value = 7}}\n```\n<!-- /casa-rs-cell -->\n\nTrailing prose.\n"
    );
    let parsed = NotebookDocument::parse(source.clone()).expect("parse notebook");
    assert_eq!(parsed.source(), source);
    assert_eq!(parsed.cells().len(), 1);
    assert_eq!(parsed.cells()[0].kind.as_str(), "future-science");
}

#[test]
fn default_and_named_notebooks_save_reopen_and_reconcile_external_edits() {
    let (root, store) = project();
    assert!(store.list_notebooks().expect("empty list").is_empty());

    let named = store
        .create_named("Analysis.md", "TW Hya analysis")
        .expect("create named notebook");
    let proposed = format!("{}\nUser note.\n", named.document.source());
    let saved = match store
        .save_notebook(&named, &proposed, ConflictResolution::Reject)
        .expect("save named notebook")
    {
        SaveResult::Saved(saved) => saved,
        other => panic!("unexpected save result: {other:?}"),
    };
    assert_eq!(
        store
            .open_notebook("Analysis.md")
            .expect("reopen named")
            .document
            .source(),
        proposed
    );

    let path = root.path().join("notebooks/Analysis.md");
    fs::write(
        &path,
        format!("{}\n<!-- third-party -->\n", saved.document.source()),
    )
    .expect("external edit");
    let local = format!("{}\nLocal dirty note.\n", saved.document.source());
    let conflict = store
        .save_notebook(&saved, &local, ConflictResolution::Reject)
        .expect("detect conflict");
    let SaveResult::Conflict(conflict) = conflict else {
        panic!("external edit should conflict")
    };
    assert!(conflict.external.document.source().contains("third-party"));
    assert!(conflict.proposed_source.contains("Local dirty note"));
    let reloaded = store
        .save_notebook(&saved, &local, ConflictResolution::ReloadExternal)
        .expect("reload external");
    assert!(matches!(reloaded, SaveResult::Reloaded(_)));

    let handle = store
        .begin_attempt(request(None, None))
        .expect("record to lazy default");
    assert!(root.path().join("notebooks/default.md").is_file());
    assert_eq!(handle.revision, 1);
    store
        .finalize_attempt(&handle, terminal(ExecutionStatus::Succeeded))
        .expect("finalize lazy-default recording");
}

#[test]
fn concurrent_runs_reserve_distinct_revisions_and_finalize_immutably() {
    let (_root, store) = project();
    let notebook = store
        .create_named("Analysis.md", "Analysis")
        .expect("create notebook");
    let first = store
        .begin_attempt(request(Some(notebook.entry.id), None))
        .expect("first attempt");
    let second = store
        .begin_attempt(request(Some(notebook.entry.id), Some(first.cell_id)))
        .expect("concurrent second attempt");
    assert_ne!(first.run_id, second.run_id);
    assert_eq!((first.revision, second.revision), (1, 2));

    let cancelled = store
        .finalize_attempt(&second, terminal(ExecutionStatus::Cancelled))
        .expect("cancel second");
    let succeeded = store
        .finalize_attempt(&first, terminal(ExecutionStatus::Succeeded))
        .expect("finish first");
    assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
    assert_eq!(succeeded.status, ExecutionStatus::Succeeded);
    assert!(
        store
            .finalize_attempt(&first, terminal(ExecutionStatus::Failed))
            .expect_err("receipt cannot be overwritten")
            .to_string()
            .contains("already finalized")
    );

    let receipts = store
        .receipts_for_notebook(notebook.entry.id)
        .expect("load receipts");
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].revision, 1);
    assert_eq!(receipts[1].revision, 2);
}

#[test]
fn live_attempts_are_not_recovered_and_interrupted_receipts_report_replay_drift() {
    let (_root, store) = project();
    let handle = store
        .begin_attempt(request(None, None))
        .expect("begin attempt");
    let recovered = store.recover_interrupted().expect("recover attempt");
    assert!(
        recovered.is_empty(),
        "a live attempt lease must not be recovered"
    );
    let interrupted = store
        .finalize_attempt(&handle, terminal(ExecutionStatus::Interrupted))
        .expect("finalize interrupted attempt");

    let assessment = interrupted.assess_replay(
        4,
        &BTreeMap::from([("niter".into(), serde_json::json!(2000))]),
    );
    assert_eq!(assessment.parameters, intent().parameters);
    assert_eq!(assessment.warnings.len(), 2);
}

#[test]
fn bypass_is_one_run_and_portable_export_excludes_receipts_and_products() {
    let (root, store) = project();
    let (handle, warning) =
        store.try_begin_attempt(RecordingPolicy::BypassOnce, request(None, None));
    assert!(handle.is_none());
    assert!(warning.is_none());
    assert!(!root.path().join("notebooks/default.md").exists());

    let handle = store
        .begin_attempt(request(None, None))
        .expect("record normal run");
    let receipt = store
        .finalize_attempt(&handle, terminal(ExecutionStatus::Failed))
        .expect("finalize failed attempt");
    assert_eq!(receipt.status, ExecutionStatus::Failed);
    fs::create_dir_all(root.path().join("products")).expect("products directory");
    fs::write(root.path().join("products/secret.image"), b"not portable").expect("product fixture");

    let portable = root.path().join("portable-export");
    store
        .export(&portable, ExportMode::Portable)
        .expect("portable export");
    assert!(portable.join("notebooks/default.md").is_file());
    assert!(!portable.join(".casa-rs").exists());
    assert!(!portable.join("products").exists());

    let advanced = root.path().join("advanced-export");
    store
        .export(&advanced, ExportMode::AdvancedWithReceipts)
        .expect("advanced export");
    assert!(
        advanced
            .join(format!(
                ".casa-rs/notebook-runs/{}/receipt.json",
                handle.run_id
            ))
            .is_file()
    );
    assert!(!advanced.join("products").exists());
}

#[test]
fn portable_export_copies_only_referenced_regular_assets_and_rejects_traversal() {
    let (root, store) = project();
    let notebook = store
        .create_named("Analysis.md", "Analysis")
        .expect("create notebook");
    fs::create_dir_all(root.path().join("notebooks/assets/figures")).expect("asset directory");
    fs::write(
        root.path().join("notebooks/assets/figures/plot.png"),
        b"plot",
    )
    .expect("referenced asset");
    fs::write(
        root.path().join("notebooks/assets/figures/private.png"),
        b"private",
    )
    .expect("unreferenced asset");
    let source = format!(
        "{}\n![science plot](assets/figures/plot.png)\n",
        notebook.document.source()
    );
    let saved = match store
        .save_notebook(&notebook, &source, ConflictResolution::Reject)
        .expect("save asset reference")
    {
        SaveResult::Saved(saved) => saved,
        other => panic!("unexpected save result: {other:?}"),
    };
    let exported = root.path().join("asset-export");
    store
        .export(&exported, ExportMode::Portable)
        .expect("export referenced asset");
    assert!(exported.join("notebooks/assets/figures/plot.png").is_file());
    assert!(
        !exported
            .join("notebooks/assets/figures/private.png")
            .exists()
    );

    let unsafe_source = format!(
        "{}\n![escape](assets/../outside.txt)\n",
        saved.document.source()
    );
    let unsafe_saved = match store
        .save_notebook(&saved, &unsafe_source, ConflictResolution::Reject)
        .expect("save third-party Markdown")
    {
        SaveResult::Saved(saved) => saved,
        other => panic!("unexpected save result: {other:?}"),
    };
    assert!(
        unsafe_saved
            .document
            .source()
            .contains("assets/../outside.txt")
    );
    let error = store
        .export(&root.path().join("unsafe-export"), ExportMode::Portable)
        .expect_err("export must reject asset traversal");
    assert!(
        error.to_string().contains("unsafe project-relative path"),
        "{error}"
    );
}
