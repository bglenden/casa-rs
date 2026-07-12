// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    fs,
    io::{self, BufRead, BufReader, Write},
    net::TcpListener,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use casa_notebook::{
    NotebookStore, TutorialAcquisitionApproval, TutorialAcquisitionPhase, TutorialArchiveFormat,
    TutorialCheckKind, TutorialCheckStatus, TutorialDataset, TutorialError, TutorialManifest,
    TutorialOptionalCheck, TutorialProject, TutorialReadChunk, TutorialSection,
    TutorialSourceResolution, TutorialUnpackPlan, TutorialUriHandler, TutorialUriRegistry,
};
use sha2::{Digest, Sha256};
use tar::{Builder, Header};
use tempfile::TempDir;

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn file_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

fn write_template(root: &Path, datasets: Vec<TutorialDataset>) -> casa_notebook::TutorialTemplate {
    fs::create_dir_all(root.join("assets")).expect("assets");
    fs::write(root.join("assets/diagram.txt"), "immutable diagram").expect("asset");
    fs::write(
        root.join("tutorial.md"),
        "# Portable tutorial\n\n![diagram](assets/diagram.txt)\n",
    )
    .expect("markdown");
    let manifest = TutorialManifest {
        schema_version: 1,
        tutorial_id: "portable-test".into(),
        title: "Portable test".into(),
        datasets,
        sections: vec![TutorialSection {
            id: "inspect".into(),
            title: "Inspect".into(),
            dataset_ids: vec!["science".into()],
            cell_ids: Vec::new(),
        }],
        regression: None,
    };
    fs::write(
        root.join("tutorial.toml"),
        toml::to_string_pretty(&manifest).expect("manifest TOML"),
    )
    .expect("manifest");
    TutorialProject::load_template(root).expect("load template")
}

fn dataset(uri: String, bytes: &[u8]) -> TutorialDataset {
    TutorialDataset {
        id: "science".into(),
        display_name: "Science input".into(),
        uri,
        destination: "data/science.bin".into(),
        expected_size_bytes: Some(bytes.len() as u64),
        sha256: Some(sha256(bytes)),
        unpack: None,
        checks: vec![TutorialOptionalCheck {
            id: "is-file".into(),
            label: "Verified source is a file".into(),
            kind: TutorialCheckKind::RegularFile,
            path: "".into(),
        }],
    }
}

fn fork(
    project_root: &Path,
    template: &casa_notebook::TutorialTemplate,
) -> casa_notebook::TutorialForkResult {
    TutorialProject::open(project_root)
        .expect("tutorial project")
        .fork_template(template, "Learner.md")
        .expect("fork template")
}

fn advance_ready(
    project: &TutorialProject,
    notebook_id: casa_notebook::NotebookId,
    generation: u64,
    chunk: u64,
) -> casa_notebook::TutorialDatasetLock {
    for _ in 0..32 {
        let state = project
            .advance_acquisition(notebook_id, "science", generation, chunk)
            .expect("advance acquisition");
        if state.phase == TutorialAcquisitionPhase::Ready {
            return state;
        }
    }
    panic!("acquisition did not become ready");
}

#[test]
fn template_forks_markdown_assets_and_managed_lock_without_mutating_source() {
    let project = TempDir::new().expect("project");
    let template_root = TempDir::new().expect("template");
    let source = template_root.path().join("source.bin");
    fs::write(&source, b"science").expect("source");
    let template = write_template(
        template_root.path(),
        vec![dataset(file_uri(&source), b"science")],
    );
    let original_markdown = fs::read_to_string(template_root.path().join("tutorial.md")).unwrap();

    let forked = fork(project.path(), &template);

    assert!(forked.notebook.document.source().contains(&format!(
        "<!-- casa-rs-notebook:v1 id={} -->",
        forked.notebook.entry.id
    )));
    assert!(
        forked
            .notebook
            .document
            .source()
            .contains(&format!("assets/{}/diagram.txt", forked.notebook.entry.id))
    );
    assert!(
        project
            .path()
            .join(format!(
                "notebooks/assets/{}/diagram.txt",
                forked.notebook.entry.id
            ))
            .is_file()
    );
    assert_eq!(
        fs::read_to_string(template_root.path().join("tutorial.md")).unwrap(),
        original_markdown
    );
    assert!(
        project
            .path()
            .join(format!(
                ".casa-rs/tutorials/{}/lock.toml",
                forked.notebook.entry.id
            ))
            .is_file()
    );
    assert_eq!(
        forked.lock.datasets[0].phase,
        TutorialAcquisitionPhase::Missing
    );
    assert!(!forked.lock.datasets[0].staged);
}

#[test]
fn one_shot_v0_migration_preserves_prose_tasks_and_regression_overlay() {
    let pack = TempDir::new().unwrap();
    fs::create_dir_all(pack.path().join("docs")).unwrap();
    fs::create_dir_all(pack.path().join("evidence")).unwrap();
    fs::write(
        pack.path().join("docs/index.md"),
        "# Legacy tutorial\n\nLearner prose.\n",
    )
    .unwrap();
    fs::write(pack.path().join("evidence/result.json"), "{\"rms\": 0.1}").unwrap();
    fs::write(
        pack.path().join("pack.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": "tutorial-pack.v0",
            "pack_id": "legacy-pack",
            "tutorial_id": "legacy-tutorial",
            "title": "Legacy tutorial",
            "declared_casa_version": "6.7",
            "inputs": [{
                "id": "science",
                "display_name": "Science input",
                "kind": "measurement_set",
                "registry_key": "science",
                "source_artifact_url": "https://example.invalid/science.ms",
                "filename": "science.ms",
                "size_bytes": 1234,
                "checksum_policy": "none",
                "pack_path": "data/science.ms",
                "materialization": "directory"
            }],
            "workspace": {"root":"workspace","native_path":"workspace/native","oracle_path":"workspace/oracle","scratch_path":"workspace/scratch"},
            "learner": {"docs_index":"docs/index.md","section_docs_path":"docs","screenshot_path":"screens","include_internal_evidence":false},
            "regression": {"evidence_path":"evidence","data_manifest":"","native_runs":"","oracle_runs":"","comparisons":"","timings":"","provider_provenance":"","review_path":"","review_record_schema":"","screenshot_specs_path":""},
            "sections": [{
                "id": "inspect",
                "sequence": 1,
                "title": "Inspect",
                "observable_result": "Visible data",
                "input_refs": ["science"],
                "tasks": ["msexplore"],
                "steps": [{
                    "id": "inspect-gui",
                    "surface": "gui",
                    "provider_kind": "native-rust",
                    "task_id": "msexplore",
                    "parameters": {"field": "TW Hya", "averaging": true}
                }],
                "review_checkpoint": {"required":false,"status":"not-required","record_path":""}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    let destination_parent = TempDir::new().unwrap();
    let destination = destination_parent.path().join("migrated");
    let migrated = TutorialProject::migrate_v0_template(pack.path(), &destination).unwrap();

    assert!(migrated.markdown.contains("Learner prose."));
    assert!(migrated.markdown.contains("kind=task"));
    assert_eq!(migrated.manifest.sections[0].cell_ids.len(), 1);
    assert_eq!(
        migrated.manifest.regression.as_ref().unwrap().path,
        Path::new("regression")
    );
    assert!(destination.join("regression/result.json").is_file());
    assert_eq!(migrated.manifest.datasets[0].sha256, None);
    let project = TempDir::new().unwrap();
    let forked = fork(project.path(), &migrated);
    let task = forked
        .notebook
        .document
        .cells()
        .iter()
        .find_map(|cell| cell.task.as_ref())
        .unwrap();
    assert_eq!(task.surface, "msexplore");
    assert_eq!(task.kind, "task");
    assert_eq!(task.parameters["field"].as_str(), Some("TW Hya"));
    assert!(matches!(
        TutorialProject::migrate_v0_template(pack.path(), &destination),
        Err(TutorialError::MigrationDestinationExists { .. })
    ));
}

#[test]
fn verified_file_acquisition_is_invisible_until_ready_and_records_checks() {
    let project_root = TempDir::new().expect("project");
    let template_root = TempDir::new().expect("template");
    let bytes = b"0123456789abcdef";
    let source = template_root.path().join("source.bin");
    fs::write(&source, bytes).expect("source");
    let template = write_template(
        template_root.path(),
        vec![dataset(file_uri(&source), bytes)],
    );
    let forked = fork(project_root.path(), &template);
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .expect("plan");
    let state = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            },
        )
        .expect("begin");
    assert!(!project_root.path().join("data/science.bin").exists());

    let ready = advance_ready(
        &project,
        forked.notebook.entry.id,
        state.current_generation,
        3,
    );

    assert!(ready.staged);
    assert_eq!(
        fs::read(project_root.path().join("data/science.bin")).unwrap(),
        bytes
    );
    assert_eq!(
        ready.attempts.last().unwrap().checks[0].status,
        TutorialCheckStatus::Passed,
        "{:?}",
        ready.attempts.last().unwrap().checks
    );
    assert_eq!(
        project
            .load_lock(forked.notebook.entry.id)
            .unwrap()
            .datasets[0]
            .phase,
        TutorialAcquisitionPhase::Ready
    );
    let receipts = NotebookStore::open(project_root.path())
        .unwrap()
        .receipts_for_notebook(forked.notebook.entry.id)
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].operation_id, "tutorial.acquire.science");
    assert_eq!(receipts[0].products[0].path, Path::new("data/science.bin"));
}

#[test]
fn missing_digest_requires_risk_approval_then_pins_computed_sha256() {
    let project_root = TempDir::new().expect("project");
    let template_root = TempDir::new().expect("template");
    let bytes = b"digestless";
    let source = template_root.path().join("source.bin");
    fs::write(&source, bytes).unwrap();
    let mut input = dataset(file_uri(&source), bytes);
    input.sha256 = None;
    let template = write_template(template_root.path(), vec![input]);
    let forked = fork(project_root.path(), &template);
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    assert!(plan.missing_digest);
    assert!(matches!(
        project.begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            }
        ),
        Err(TutorialError::MissingDigestApprovalRequired)
    ));
    let state = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: true,
                skipped_check_ids: vec!["is-file".into()],
            },
        )
        .unwrap();
    let ready = advance_ready(
        &project,
        forked.notebook.entry.id,
        state.current_generation,
        64,
    );
    assert_eq!(ready.pinned_sha256.as_deref(), Some(sha256(bytes).as_str()));
    assert_eq!(
        ready.attempts.last().unwrap().checks[0].status,
        TutorialCheckStatus::Skipped
    );
}

#[test]
fn cancellation_resume_and_restart_use_new_generations_and_reject_stale_callbacks() {
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let bytes = b"abcdefghijklmnopqrstuvwxyz";
    let source = template_root.path().join("source.bin");
    fs::write(&source, bytes).unwrap();
    let template = write_template(
        template_root.path(),
        vec![dataset(file_uri(&source), bytes)],
    );
    let forked = fork(project_root.path(), &template);
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    let first = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            },
        )
        .unwrap();
    project
        .advance_acquisition(
            forked.notebook.entry.id,
            "science",
            first.current_generation,
            5,
        )
        .unwrap();
    let cancelled = project
        .cancel_acquisition(
            forked.notebook.entry.id,
            "science",
            first.current_generation,
        )
        .unwrap();
    assert_eq!(cancelled.phase, TutorialAcquisitionPhase::Cancelled);
    assert!(!project_root.path().join("data/science.bin").exists());

    let resumed = project
        .resume_acquisition(forked.notebook.entry.id, "science")
        .unwrap();
    assert!(resumed.current_generation > first.current_generation);
    assert!(matches!(
        project.advance_acquisition(
            forked.notebook.entry.id,
            "science",
            first.current_generation,
            5
        ),
        Err(TutorialError::StaleGeneration { .. })
    ));
    let ready = advance_ready(
        &project,
        forked.notebook.entry.id,
        resumed.current_generation,
        5,
    );
    assert_eq!(ready.attempts.len(), 2);
    assert!(ready.staged);
}

#[test]
fn checksum_failure_and_destination_collision_never_replace_project_data() {
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let bytes = b"actual";
    let source = template_root.path().join("source.bin");
    fs::write(&source, bytes).unwrap();
    let mut input = dataset(file_uri(&source), bytes);
    input.sha256 = Some(sha256(b"different"));
    let template = write_template(template_root.path(), vec![input]);
    let forked = fork(project_root.path(), &template);
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    let first = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            },
        )
        .unwrap();
    let mut failure = None;
    for _ in 0..8 {
        match project.advance_acquisition(
            forked.notebook.entry.id,
            "science",
            first.current_generation,
            64,
        ) {
            Ok(_) => {}
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    assert!(matches!(
        failure,
        Some(TutorialError::ChecksumMismatch { .. })
    ));
    assert!(!project_root.path().join("data/science.bin").exists());
    assert_eq!(
        project
            .load_lock(forked.notebook.entry.id)
            .unwrap()
            .datasets[0]
            .phase,
        TutorialAcquisitionPhase::ChecksumFailed
    );
}

#[test]
fn existing_destination_is_a_distinct_failure_and_is_never_replaced() {
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let bytes = b"new bytes";
    let source = template_root.path().join("source.bin");
    fs::write(&source, bytes).unwrap();
    let template = write_template(
        template_root.path(),
        vec![dataset(file_uri(&source), bytes)],
    );
    let forked = fork(project_root.path(), &template);
    fs::create_dir_all(project_root.path().join("data")).unwrap();
    fs::write(project_root.path().join("data/science.bin"), b"keep me").unwrap();
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    let state = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            },
        )
        .unwrap();
    let mut failure = None;
    for _ in 0..8 {
        match project.advance_acquisition(
            forked.notebook.entry.id,
            "science",
            state.current_generation,
            64,
        ) {
            Ok(_) => {}
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    assert!(matches!(
        failure,
        Some(TutorialError::DestinationExists { .. })
    ));
    assert_eq!(
        fs::read(project_root.path().join("data/science.bin")).unwrap(),
        b"keep me"
    );
    assert_eq!(
        project
            .load_lock(forked.notebook.entry.id)
            .unwrap()
            .datasets[0]
            .phase,
        TutorialAcquisitionPhase::DestinationCollision
    );
}

fn tar_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut builder = Builder::new(&mut bytes);
        for (path, contents) in entries {
            let mut header = Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, path, *contents)
                .expect("append tar entry");
        }
        builder.finish().expect("finish tar");
    }
    bytes
}

#[test]
fn bounded_archive_materialization_rejects_links_and_limits_then_stages_safe_root() {
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let archive = tar_bytes(&[("science.ms/table.dat", b"table")]);
    let source = template_root.path().join("science.tar");
    fs::write(&source, &archive).unwrap();
    let mut input = dataset(file_uri(&source), &archive);
    input.destination = "data/science.ms".into();
    input.unpack = Some(TutorialUnpackPlan {
        format: TutorialArchiveFormat::Tar,
        archive_root: Some("science.ms".into()),
        max_entries: 4,
        max_expanded_bytes: 1024,
    });
    input.checks = vec![TutorialOptionalCheck {
        id: "is-ms".into(),
        label: "MeasurementSet structure".into(),
        kind: TutorialCheckKind::MeasurementSet,
        path: "".into(),
    }];
    let template = write_template(template_root.path(), vec![input]);
    let forked = fork(project_root.path(), &template);
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    let state = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            },
        )
        .unwrap();
    let ready = advance_ready(
        &project,
        forked.notebook.entry.id,
        state.current_generation,
        4096,
    );
    assert!(
        project_root
            .path()
            .join("data/science.ms/table.dat")
            .is_file()
    );
    assert_eq!(
        ready.attempts.last().unwrap().checks[0].status,
        TutorialCheckStatus::Passed
    );
}

#[test]
fn unsafe_archive_link_is_rejected_without_exposing_a_destination() {
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let mut malicious = Vec::new();
    {
        let mut builder = Builder::new(&mut malicious);
        let mut header = Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(0o777);
        header.set_link_name("../../escape").unwrap();
        header.set_cksum();
        builder
            .append_data(&mut header, "science.ms/link", io::empty())
            .unwrap();
        builder.finish().unwrap();
    }
    let source = template_root.path().join("bad.tar");
    fs::write(&source, &malicious).unwrap();
    let mut input = dataset(file_uri(&source), &malicious);
    input.destination = "data/science.ms".into();
    input.unpack = Some(TutorialUnpackPlan {
        format: TutorialArchiveFormat::Tar,
        archive_root: Some("science.ms".into()),
        max_entries: 4,
        max_expanded_bytes: 1024,
    });
    let template = write_template(template_root.path(), vec![input]);
    let forked = fork(project_root.path(), &template);
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    let state = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            },
        )
        .unwrap();
    let mut failure = None;
    for _ in 0..8 {
        match project.advance_acquisition(
            forked.notebook.entry.id,
            "science",
            state.current_generation,
            4096,
        ) {
            Ok(_) => {}
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    assert!(matches!(failure, Some(TutorialError::UnsafeArchive { .. })));
    assert!(!project_root.path().join("data/science.ms").exists());
    assert!(!project_root.path().join("escape").exists());
    assert_eq!(
        project
            .load_lock(forked.notebook.entry.id)
            .unwrap()
            .datasets[0]
            .phase,
        TutorialAcquisitionPhase::UnsafeArchive
    );
}

#[derive(Clone)]
struct FixtureHandler {
    bytes: Vec<u8>,
}

impl TutorialUriHandler for FixtureHandler {
    fn scheme(&self) -> &str {
        "fixture"
    }

    fn resolve(&self, _uri: &str) -> Result<TutorialSourceResolution, TutorialError> {
        Ok(TutorialSourceResolution {
            resolved_uri: "fixture://mirror/science".into(),
            redirects: vec!["fixture://mirror/science".into()],
            size_bytes: Some(self.bytes.len() as u64),
        })
    }

    fn read_chunk(
        &self,
        _uri: &str,
        offset: u64,
        limit: u64,
    ) -> Result<TutorialReadChunk, TutorialError> {
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.bytes.len());
        let end = start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(self.bytes.len());
        Ok(TutorialReadChunk {
            bytes: self.bytes[start..end].to_vec(),
            complete: end == self.bytes.len(),
        })
    }
}

#[derive(Clone)]
struct FlakyHandler {
    bytes: Vec<u8>,
    failed: Arc<AtomicBool>,
}

impl TutorialUriHandler for FlakyHandler {
    fn scheme(&self) -> &str {
        "flaky"
    }

    fn resolve(&self, uri: &str) -> Result<TutorialSourceResolution, TutorialError> {
        Ok(TutorialSourceResolution {
            resolved_uri: uri.into(),
            redirects: Vec::new(),
            size_bytes: Some(self.bytes.len() as u64),
        })
    }

    fn read_chunk(
        &self,
        uri: &str,
        offset: u64,
        limit: u64,
    ) -> Result<TutorialReadChunk, TutorialError> {
        if offset > 0 && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(TutorialError::Network {
                uri: uri.into(),
                detail: "fixture connection dropped".into(),
            });
        }
        FixtureHandler {
            bytes: self.bytes.clone(),
        }
        .read_chunk(uri, offset, limit)
    }
}

#[test]
fn network_failure_is_distinct_and_retry_resumes_the_partial_download() {
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let bytes = b"network source with multiple chunks";
    let input = dataset("flaky://catalog/science".into(), bytes);
    let template = write_template(template_root.path(), vec![input]);
    let forked = fork(project_root.path(), &template);
    let mut registry = TutorialUriRegistry::v1();
    registry.register(Arc::new(FlakyHandler {
        bytes: bytes.to_vec(),
        failed: Arc::new(AtomicBool::new(false)),
    }));
    let project = TutorialProject::with_registry(project_root.path(), registry).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    let initial = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            },
        )
        .unwrap();
    let partial = project
        .advance_acquisition(
            forked.notebook.entry.id,
            "science",
            initial.current_generation,
            5,
        )
        .unwrap();
    assert_eq!(partial.attempts.last().unwrap().downloaded_bytes, 5);
    assert!(matches!(
        project.advance_acquisition(
            forked.notebook.entry.id,
            "science",
            initial.current_generation,
            5,
        ),
        Err(TutorialError::Network { .. })
    ));
    let failed = project.load_lock(forked.notebook.entry.id).unwrap();
    assert_eq!(
        failed.datasets[0].phase,
        TutorialAcquisitionPhase::NetworkFailed
    );
    let retry = project
        .retry_acquisition(forked.notebook.entry.id, "science")
        .unwrap();
    assert_eq!(retry.attempts.last().unwrap().downloaded_bytes, 5);
    let ready = advance_ready(
        &project,
        forked.notebook.entry.id,
        retry.current_generation,
        5,
    );
    assert_eq!(ready.phase, TutorialAcquisitionPhase::Ready);
}

#[test]
fn insufficient_disk_is_rejected_before_an_attempt_and_offline_reopen_is_inert() {
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let source = template_root.path().join("source.bin");
    fs::write(&source, b"small").unwrap();
    let mut input = dataset(file_uri(&source), b"small");
    input.expected_size_bytes = Some(i64::MAX as u64);
    let template = write_template(template_root.path(), vec![input]);
    let forked = fork(project_root.path(), &template);
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    assert!(!plan.has_enough_disk());
    assert!(matches!(
        project.begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            }
        ),
        Err(TutorialError::InsufficientDisk { .. })
    ));
    fs::remove_file(source).unwrap();
    let reopened = TutorialProject::open(project_root.path())
        .unwrap()
        .load_lock(forked.notebook.entry.id)
        .unwrap();
    assert_eq!(
        reopened.datasets[0].phase,
        TutorialAcquisitionPhase::Missing
    );
    assert!(reopened.datasets[0].attempts.is_empty());
}

fn start_redirect_server(bytes: &'static [u8]) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut line = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            if request_line.contains(" /start ") {
                write!(
                    stream,
                    "HTTP/1.1 302 Found\r\nLocation: http://{address}/data\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            } else if request_line.starts_with("HEAD ") {
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    bytes.len()
                )
                .unwrap();
            } else {
                write!(
                    stream,
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 0-{}/{}\r\nConnection: close\r\n\r\n",
                    bytes.len(),
                    bytes.len() - 1,
                    bytes.len()
                )
                .unwrap();
                stream.write_all(bytes).unwrap();
            }
        }
    });
    (format!("http://{address}/start"), handle)
}

#[test]
fn built_in_http_handler_records_redirect_and_materializes_ranged_content() {
    let bytes = b"redirected http source";
    let (uri, server) = start_redirect_server(bytes);
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let template = write_template(template_root.path(), vec![dataset(uri, bytes)]);
    let forked = fork(project_root.path(), &template);
    let project = TutorialProject::open(project_root.path()).unwrap();
    let plan = project
        .plan_acquisition(forked.notebook.entry.id, "science", None)
        .unwrap();
    assert_eq!(plan.redirects.len(), 1);
    assert!(plan.resolved_uri.ends_with("/data"));
    let state = project
        .begin_acquisition(
            &plan,
            TutorialAcquisitionApproval {
                approval_sha256: plan.approval_sha256.clone(),
                allow_missing_digest: false,
                skipped_check_ids: Vec::new(),
            },
        )
        .unwrap();
    let ready = advance_ready(
        &project,
        forked.notebook.entry.id,
        state.current_generation,
        1024,
    );
    assert_eq!(ready.phase, TutorialAcquisitionPhase::Ready);
    server.join().unwrap();
}

#[test]
fn registry_keeps_unknown_schemes_inert_and_applies_user_source_override_policy() {
    let project_root = TempDir::new().unwrap();
    let template_root = TempDir::new().unwrap();
    let bytes = b"fixture source";
    let mut input = dataset("unknown://catalog/science".into(), bytes);
    input.expected_size_bytes = None;
    let template = write_template(template_root.path(), vec![input]);
    let forked = fork(project_root.path(), &template);
    let default_project = TutorialProject::open(project_root.path()).unwrap();
    assert!(matches!(
        default_project.plan_acquisition(forked.notebook.entry.id, "science", None),
        Err(TutorialError::UnknownScheme { .. })
    ));

    let mut registry = TutorialUriRegistry::v1();
    registry.register(Arc::new(FixtureHandler {
        bytes: bytes.to_vec(),
    }));
    let project = TutorialProject::with_registry(project_root.path(), registry).unwrap();
    let plan = project
        .plan_acquisition(
            forked.notebook.entry.id,
            "science",
            Some("fixture://user/science"),
        )
        .unwrap();
    assert_eq!(plan.requested_uri, "fixture://user/science");
    assert_eq!(plan.resolved_uri, "fixture://mirror/science");
    assert_eq!(plan.redirects, ["fixture://mirror/science"]);
}
