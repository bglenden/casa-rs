// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempDir};
use thiserror::Error;

use crate::{
    AttemptEvent, AttemptEventKind, CellId, CellKind, ExecutionReceipt, ExecutionStatus,
    LogReferences, NotebookDocument, NotebookId, NotebookParseError, ReceiptFinalization,
    RecordingRequest, RunId, Timestamp, receipt::RECEIPT_SCHEMA_VERSION,
};

const DEFAULT_NOTEBOOK: &str = "default.md";
const PENDING_ATTEMPT: &str = "attempt.json";
const RECEIPT_FILE: &str = "receipt.json";
const EVENTS_FILE: &str = "events.jsonl";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotebookEntry {
    pub id: NotebookId,
    pub filename: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NotebookSnapshot {
    pub entry: NotebookEntry,
    pub document: NotebookDocument,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NotebookConflict {
    pub base_hash: String,
    pub external: NotebookSnapshot,
    pub proposed_source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    Reject,
    KeepLocal,
    ReloadExternal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SaveResult {
    Saved(NotebookSnapshot),
    Reloaded(NotebookSnapshot),
    Conflict(Box<NotebookConflict>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordingPolicy {
    Record,
    BypassOnce,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingWarning {
    pub operation_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportMode {
    Portable,
    AdvancedWithReceipts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptHandle {
    pub run_id: RunId,
    pub notebook_id: NotebookId,
    pub cell_id: CellId,
    pub revision: u64,
    pub started_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingAttempt {
    handle: AttemptHandle,
    request: RecordingRequest,
}

#[derive(Debug, Clone)]
pub struct NotebookStore {
    project_root: PathBuf,
}

impl NotebookStore {
    pub fn open(project_root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let project_root = project_root.as_ref();
        if !project_root.is_absolute() {
            return Err(StoreError::ProjectRootMustBeAbsolute {
                path: project_root.to_owned(),
            });
        }
        let metadata = fs::metadata(project_root).map_err(|source| StoreError::Io {
            action: "inspect project root",
            path: project_root.to_owned(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(StoreError::ProjectRootNotDirectory {
                path: project_root.to_owned(),
            });
        }
        Ok(Self {
            project_root: project_root.to_owned(),
        })
    }

    #[must_use]
    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    pub fn list_notebooks(&self) -> Result<Vec<NotebookEntry>, StoreError> {
        let notebooks = self.notebooks_dir();
        let mut entries = Vec::new();
        let directory = match fs::read_dir(&notebooks) {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
            Err(source) => {
                return Err(StoreError::Io {
                    action: "list notebooks",
                    path: notebooks,
                    source,
                });
            }
        };
        for entry in directory {
            let entry = entry.map_err(|source| StoreError::Io {
                action: "read notebook directory entry",
                path: self.notebooks_dir(),
                source,
            })?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("md") {
                continue;
            }
            let snapshot = self.read_snapshot_path(&path)?;
            entries.push(snapshot.entry);
        }
        entries.sort_by(|left, right| left.filename.cmp(&right.filename));
        Ok(entries)
    }

    pub fn create_named(
        &self,
        filename: &str,
        title: &str,
    ) -> Result<NotebookSnapshot, StoreError> {
        validate_notebook_filename(filename)?;
        let _lock = self.lock_project()?;
        let path = self.notebooks_dir().join(filename);
        if path.exists() {
            return Err(StoreError::NotebookAlreadyExists { path });
        }
        let document = NotebookDocument::new(NotebookId::new(), title);
        atomic_write(&path, document.source().as_bytes())?;
        self.read_snapshot_path(&path)
    }

    /// Create the conventional project notebook on first use, or reopen it.
    pub fn ensure_default_notebook(&self) -> Result<NotebookSnapshot, StoreError> {
        let _lock = self.lock_project()?;
        self.ensure_default_notebook_locked()
    }

    pub fn open_notebook(&self, filename: &str) -> Result<NotebookSnapshot, StoreError> {
        validate_notebook_filename(filename)?;
        self.read_snapshot_path(&self.notebooks_dir().join(filename))
    }

    pub fn open_notebook_id(&self, id: NotebookId) -> Result<NotebookSnapshot, StoreError> {
        self.list_notebooks()?
            .into_iter()
            .find(|entry| entry.id == id)
            .map(|entry| self.open_notebook(&entry.filename))
            .transpose()?
            .ok_or(StoreError::NotebookIdNotFound { id })
    }

    pub fn save_notebook(
        &self,
        base: &NotebookSnapshot,
        proposed_source: &str,
        resolution: ConflictResolution,
    ) -> Result<SaveResult, StoreError> {
        let _lock = self.lock_project()?;
        let path = self.notebooks_dir().join(&base.entry.filename);
        let external = self.read_snapshot_path(&path)?;
        if external.content_hash != base.content_hash {
            match resolution {
                ConflictResolution::Reject => {
                    return Ok(SaveResult::Conflict(Box::new(NotebookConflict {
                        base_hash: base.content_hash.clone(),
                        external,
                        proposed_source: proposed_source.to_owned(),
                    })));
                }
                ConflictResolution::ReloadExternal => {
                    return Ok(SaveResult::Reloaded(external));
                }
                ConflictResolution::KeepLocal => {}
            }
        }
        let parsed = NotebookDocument::parse(proposed_source)?;
        if parsed.notebook_id() != base.entry.id {
            return Err(StoreError::NotebookIdentityChanged {
                expected: base.entry.id,
                actual: parsed.notebook_id(),
            });
        }
        atomic_write(&path, proposed_source.as_bytes())?;
        self.read_snapshot_path(&path).map(SaveResult::Saved)
    }

    pub fn begin_attempt(&self, request: RecordingRequest) -> Result<AttemptHandle, StoreError> {
        let _lock = self.lock_project()?;
        let mut snapshot = match request.notebook_id {
            Some(id) => self.open_notebook_id(id)?,
            None => self.ensure_default_notebook_locked()?,
        };
        let cell_id = match request.cell_id {
            Some(id) => {
                if snapshot.document.cell(id).is_none() {
                    return Err(StoreError::CellNotFound {
                        notebook_id: snapshot.entry.id,
                        cell_id: id,
                    });
                }
                id
            }
            None => {
                let id = CellId::new();
                if let Some(intent) = &request.task_intent {
                    snapshot.document.append_task_cell(id, intent)?;
                } else {
                    let body = format!(
                        "Recorded operation `{}`. Managed execution details are stored separately.\n",
                        request.operation_id
                    );
                    snapshot.document.append_cell(id, CellKind::Output, &body)?;
                }
                atomic_write(
                    &self.notebooks_dir().join(&snapshot.entry.filename),
                    snapshot.document.source().as_bytes(),
                )?;
                id
            }
        };
        let revision = self.next_revision_locked(snapshot.entry.id, cell_id)?;
        let handle = AttemptHandle {
            run_id: RunId::new(),
            notebook_id: snapshot.entry.id,
            cell_id,
            revision,
            started_at: Timestamp::now(),
        };
        let run_dir = self.run_dir(handle.run_id);
        fs::create_dir_all(&run_dir).map_err(|source| StoreError::Io {
            action: "create notebook run directory",
            path: run_dir.clone(),
            source,
        })?;
        let pending = PendingAttempt {
            handle: handle.clone(),
            request,
        };
        atomic_json(&run_dir.join(PENDING_ATTEMPT), &pending)?;
        self.append_event_locked(&AttemptEvent::started(handle.run_id))?;
        Ok(handle)
    }

    pub fn try_begin_attempt(
        &self,
        policy: RecordingPolicy,
        request: RecordingRequest,
    ) -> (Option<AttemptHandle>, Option<RecordingWarning>) {
        if policy == RecordingPolicy::BypassOnce {
            return (None, None);
        }
        let operation_id = request.operation_id.clone();
        match self.begin_attempt(request) {
            Ok(handle) => (Some(handle), None),
            Err(error) => (
                None,
                Some(RecordingWarning {
                    operation_id,
                    message: error.to_string(),
                }),
            ),
        }
    }

    pub fn append_event(&self, event: &AttemptEvent) -> Result<(), StoreError> {
        let _lock = self.lock_project()?;
        self.append_event_locked(event)
    }

    pub fn finalize_attempt(
        &self,
        handle: &AttemptHandle,
        finalization: ReceiptFinalization,
    ) -> Result<ExecutionReceipt, StoreError> {
        let _lock = self.lock_project()?;
        let run_dir = self.run_dir(handle.run_id);
        let receipt_path = run_dir.join(RECEIPT_FILE);
        if receipt_path.exists() {
            return Err(StoreError::ReceiptAlreadyFinalized {
                run_id: handle.run_id,
            });
        }
        let pending: PendingAttempt = read_json(&run_dir.join(PENDING_ATTEMPT))?;
        if pending.handle != *handle {
            return Err(StoreError::AttemptIdentityMismatch {
                run_id: handle.run_id,
            });
        }
        let stdout_path = (!finalization.stdout.is_empty()).then(|| PathBuf::from("stdout.log"));
        let stderr_path = (!finalization.stderr.is_empty()).then(|| PathBuf::from("stderr.log"));
        if let Some(path) = &stdout_path {
            atomic_write(&run_dir.join(path), &finalization.stdout)?;
        }
        if let Some(path) = &stderr_path {
            atomic_write(&run_dir.join(path), &finalization.stderr)?;
        }
        let receipt = ExecutionReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            run_id: handle.run_id,
            revision: handle.revision,
            notebook_id: handle.notebook_id,
            cell_id: handle.cell_id,
            initiating_surface: pending.request.initiating_surface,
            operation_id: pending.request.operation_id,
            started_at: handle.started_at,
            finished_at: finalization.finished_at,
            status: finalization.status,
            sparse_intent: pending.request.task_intent,
            resolved_parameters: pending.request.resolved_parameters,
            provider_contract_version: pending.request.provider_contract_version,
            run_safety: pending.request.run_safety,
            approvals: pending.request.approvals,
            affected_paths: finalization.affected_paths,
            products: finalization.products,
            artifacts: finalization.artifacts,
            logs: LogReferences {
                casa_log: finalization.casa_log,
                stdout: stdout_path,
                stderr: stderr_path,
                events: Some(PathBuf::from(EVENTS_FILE)),
            },
            diagnostics: finalization.diagnostics,
            replay_claim:
                "historical resolved values; validate current contract and defaults before rerun"
                    .to_owned(),
        };
        atomic_json(&receipt_path, &receipt)?;
        let terminal_kind = match receipt.status {
            ExecutionStatus::Succeeded => AttemptEventKind::Succeeded,
            ExecutionStatus::Failed => AttemptEventKind::Failed,
            ExecutionStatus::Cancelled => AttemptEventKind::Cancelled,
            ExecutionStatus::Interrupted => AttemptEventKind::Interrupted,
        };
        self.append_event_locked(&AttemptEvent {
            schema_version: RECEIPT_SCHEMA_VERSION,
            run_id: handle.run_id,
            timestamp: receipt.finished_at,
            kind: terminal_kind,
            message: None,
            fields: BTreeMap::new(),
        })?;
        fs::remove_file(run_dir.join(PENDING_ATTEMPT)).map_err(|source| StoreError::Io {
            action: "remove finalized attempt state",
            path: run_dir.join(PENDING_ATTEMPT),
            source,
        })?;
        Ok(receipt)
    }

    pub fn try_finalize_attempt(
        &self,
        handle: &AttemptHandle,
        finalization: ReceiptFinalization,
    ) -> Option<RecordingWarning> {
        self.finalize_attempt(handle, finalization)
            .err()
            .map(|error| RecordingWarning {
                operation_id: handle.run_id.to_string(),
                message: error.to_string(),
            })
    }

    pub fn recover_interrupted(&self) -> Result<Vec<ExecutionReceipt>, StoreError> {
        let runs = self.runs_dir();
        let directory = match fs::read_dir(&runs) {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(StoreError::Io {
                    action: "list notebook runs",
                    path: runs,
                    source,
                });
            }
        };
        let mut recovered = Vec::new();
        for entry in directory {
            let entry = entry.map_err(|source| StoreError::Io {
                action: "read notebook run entry",
                path: self.runs_dir(),
                source,
            })?;
            let run_dir = entry.path();
            if run_dir.join(PENDING_ATTEMPT).is_file() && !run_dir.join(RECEIPT_FILE).exists() {
                let pending: PendingAttempt = read_json(&run_dir.join(PENDING_ATTEMPT))?;
                recovered.push(self.finalize_attempt(
                    &pending.handle,
                    ReceiptFinalization {
                        status: ExecutionStatus::Interrupted,
                        finished_at: Timestamp::now(),
                        affected_paths: Vec::new(),
                        products: Vec::new(),
                        artifacts: Vec::new(),
                        diagnostics: vec![
                            "The prior process ended before recording a terminal event.".to_owned(),
                        ],
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                        casa_log: None,
                    },
                )?);
            }
        }
        Ok(recovered)
    }

    pub fn receipts_for_notebook(
        &self,
        notebook_id: NotebookId,
    ) -> Result<Vec<ExecutionReceipt>, StoreError> {
        let mut receipts = self.all_receipts()?;
        receipts.retain(|receipt| receipt.notebook_id == notebook_id);
        receipts.sort_by_key(|receipt| (receipt.cell_id, receipt.revision));
        Ok(receipts)
    }

    pub fn all_receipts(&self) -> Result<Vec<ExecutionReceipt>, StoreError> {
        let runs = self.runs_dir();
        let directory = match fs::read_dir(&runs) {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(StoreError::Io {
                    action: "list notebook receipts",
                    path: runs,
                    source,
                });
            }
        };
        let mut receipts = Vec::new();
        for entry in directory {
            let entry = entry.map_err(|source| StoreError::Io {
                action: "read notebook receipt entry",
                path: self.runs_dir(),
                source,
            })?;
            let receipt = entry.path().join(RECEIPT_FILE);
            if receipt.is_file() {
                receipts.push(read_json(&receipt)?);
            }
        }
        receipts.sort_by_key(|receipt: &ExecutionReceipt| receipt.started_at);
        Ok(receipts)
    }

    pub fn export(&self, destination: &Path, mode: ExportMode) -> Result<(), StoreError> {
        if destination.exists() {
            return Err(StoreError::ExportDestinationExists {
                path: destination.to_owned(),
            });
        }
        let parent = destination.parent().ok_or_else(|| StoreError::UnsafePath {
            path: destination.to_owned(),
        })?;
        fs::create_dir_all(parent).map_err(|source| StoreError::Io {
            action: "create export parent",
            path: parent.to_owned(),
            source,
        })?;
        let staging = TempDir::new_in(parent).map_err(|source| StoreError::Io {
            action: "create export staging directory",
            path: parent.to_owned(),
            source,
        })?;
        let export_notebooks = staging.path().join("notebooks");
        fs::create_dir_all(&export_notebooks).map_err(|source| StoreError::Io {
            action: "create export notebooks directory",
            path: export_notebooks.clone(),
            source,
        })?;
        let mut referenced_assets = BTreeSet::new();
        for entry in self.list_notebooks()? {
            let source_path = self.notebooks_dir().join(&entry.filename);
            let source = fs::read_to_string(&source_path).map_err(|source| StoreError::Io {
                action: "read notebook for export",
                path: source_path.clone(),
                source,
            })?;
            referenced_assets.extend(asset_references(&source)?);
            copy_regular_file(&source_path, &export_notebooks.join(&entry.filename))?;
        }
        for asset in referenced_assets {
            let source = self.notebooks_dir().join(&asset);
            let destination = export_notebooks.join(&asset);
            copy_regular_file(&source, &destination)?;
        }
        if mode == ExportMode::AdvancedWithReceipts && self.runs_dir().exists() {
            copy_tree_regular(
                &self.runs_dir(),
                &staging.path().join(".casa-rs/notebook-runs"),
            )?;
        }
        let staging_path = staging.keep();
        fs::rename(&staging_path, destination).map_err(|source| StoreError::Io {
            action: "publish notebook export",
            path: destination.to_owned(),
            source,
        })?;
        Ok(())
    }

    fn ensure_default_notebook_locked(&self) -> Result<NotebookSnapshot, StoreError> {
        let path = self.notebooks_dir().join(DEFAULT_NOTEBOOK);
        if !path.exists() {
            let document = NotebookDocument::new(NotebookId::new(), "CASA-RS notebook");
            atomic_write(&path, document.source().as_bytes())?;
        }
        self.read_snapshot_path(&path)
    }

    fn next_revision_locked(
        &self,
        notebook_id: NotebookId,
        cell_id: CellId,
    ) -> Result<u64, StoreError> {
        let finalized = self
            .all_receipts()?
            .into_iter()
            .filter(|receipt| receipt.notebook_id == notebook_id && receipt.cell_id == cell_id)
            .map(|receipt| receipt.revision)
            .max()
            .unwrap_or(0);
        let mut reserved = 0;
        let runs = self.runs_dir();
        if let Ok(directory) = fs::read_dir(&runs) {
            for entry in directory {
                let entry = entry.map_err(|source| StoreError::Io {
                    action: "read pending notebook run entry",
                    path: runs.clone(),
                    source,
                })?;
                let pending_path = entry.path().join(PENDING_ATTEMPT);
                if pending_path.is_file() {
                    let pending: PendingAttempt = read_json(&pending_path)?;
                    if pending.handle.notebook_id == notebook_id
                        && pending.handle.cell_id == cell_id
                    {
                        reserved = reserved.max(pending.handle.revision);
                    }
                }
            }
        }
        Ok(finalized.max(reserved) + 1)
    }

    fn append_event_locked(&self, event: &AttemptEvent) -> Result<(), StoreError> {
        let path = self.run_dir(event.run_id).join(EVENTS_FILE);
        let parent = path.parent().expect("events path has a parent");
        fs::create_dir_all(parent).map_err(|source| StoreError::Io {
            action: "create event directory",
            path: parent.to_owned(),
            source,
        })?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| StoreError::Io {
                action: "open attempt event log",
                path: path.clone(),
                source,
            })?;
        serde_json::to_writer(&mut file, event).map_err(|source| StoreError::Json {
            action: "serialize attempt event",
            path: path.clone(),
            source,
        })?;
        file.write_all(b"\n").map_err(|source| StoreError::Io {
            action: "append attempt event",
            path: path.clone(),
            source,
        })?;
        file.sync_data().map_err(|source| StoreError::Io {
            action: "sync attempt event",
            path,
            source,
        })
    }

    fn read_snapshot_path(&self, path: &Path) -> Result<NotebookSnapshot, StoreError> {
        let source = fs::read_to_string(path).map_err(|source| StoreError::Io {
            action: "read notebook",
            path: path.to_owned(),
            source,
        })?;
        let document = NotebookDocument::parse(source)?;
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| StoreError::UnsafePath {
                path: path.to_owned(),
            })?
            .to_owned();
        Ok(NotebookSnapshot {
            entry: NotebookEntry {
                id: document.notebook_id(),
                filename,
            },
            content_hash: content_hash(document.source().as_bytes()),
            document,
        })
    }

    fn lock_project(&self) -> Result<File, StoreError> {
        let managed = self.project_root.join(".casa-rs");
        fs::create_dir_all(&managed).map_err(|source| StoreError::Io {
            action: "create managed project directory",
            path: managed.clone(),
            source,
        })?;
        let path = managed.join("notebook.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| StoreError::Io {
                action: "open notebook project lock",
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive().map_err(|source| StoreError::Io {
            action: "lock notebook project",
            path,
            source,
        })?;
        Ok(file)
    }

    fn notebooks_dir(&self) -> PathBuf {
        self.project_root.join("notebooks")
    }

    fn runs_dir(&self) -> PathBuf {
        self.project_root.join(".casa-rs/notebook-runs")
    }

    fn run_dir(&self, run_id: RunId) -> PathBuf {
        self.runs_dir().join(run_id.to_string())
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("project root must be an explicit absolute path: {path}", path = .path.display())]
    ProjectRootMustBeAbsolute { path: PathBuf },
    #[error("project root is not a directory: {path}", path = .path.display())]
    ProjectRootNotDirectory { path: PathBuf },
    #[error("notebook filename must be one safe .md path component: {filename:?}")]
    InvalidNotebookFilename { filename: String },
    #[error("notebook already exists: {path}", path = .path.display())]
    NotebookAlreadyExists { path: PathBuf },
    #[error("notebook id {id} is not present in this project")]
    NotebookIdNotFound { id: NotebookId },
    #[error("cell {cell_id} is not present in notebook {notebook_id}")]
    CellNotFound {
        notebook_id: NotebookId,
        cell_id: CellId,
    },
    #[error("notebook identity changed from {expected} to {actual}")]
    NotebookIdentityChanged {
        expected: NotebookId,
        actual: NotebookId,
    },
    #[error("receipt {run_id} is already finalized and immutable")]
    ReceiptAlreadyFinalized { run_id: RunId },
    #[error("attempt state does not match handle {run_id}")]
    AttemptIdentityMismatch { run_id: RunId },
    #[error("export destination already exists: {path}", path = .path.display())]
    ExportDestinationExists { path: PathBuf },
    #[error("unsafe project-relative path: {path}", path = .path.display())]
    UnsafePath { path: PathBuf },
    #[error(transparent)]
    Markdown(#[from] NotebookParseError),
    #[error("{action} at {path}: {source}", path = .path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{action} at {path}: {source}", path = .path.display())]
    Json {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("publish atomic file at {path}: {source}", path = .path.display())]
    Persist {
        path: PathBuf,
        #[source]
        source: tempfile::PersistError,
    },
}

fn validate_notebook_filename(filename: &str) -> Result<(), StoreError> {
    let path = Path::new(filename);
    let safe = path.extension().and_then(|value| value.to_str()) == Some("md")
        && path.components().count() == 1
        && matches!(path.components().next(), Some(Component::Normal(_)));
    if safe {
        Ok(())
    } else {
        Err(StoreError::InvalidNotebookFilename {
            filename: filename.to_owned(),
        })
    }
}

fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<(), StoreError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|source| StoreError::Json {
        action: "serialize managed JSON",
        path: path.to_owned(),
        source,
    })?;
    atomic_write(path, &bytes)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, StoreError> {
    let bytes = fs::read(path).map_err(|source| StoreError::Io {
        action: "read managed JSON",
        path: path.to_owned(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| StoreError::Json {
        action: "parse managed JSON",
        path: path.to_owned(),
        source,
    })
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let parent = path.parent().ok_or_else(|| StoreError::UnsafePath {
        path: path.to_owned(),
    })?;
    fs::create_dir_all(parent).map_err(|source| StoreError::Io {
        action: "create atomic-write parent",
        path: parent.to_owned(),
        source,
    })?;
    let mut temporary = NamedTempFile::new_in(parent).map_err(|source| StoreError::Io {
        action: "create atomic-write temporary file",
        path: parent.to_owned(),
        source,
    })?;
    temporary
        .write_all(bytes)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| StoreError::Io {
            action: "write atomic temporary file",
            path: temporary.path().to_owned(),
            source,
        })?;
    temporary
        .persist(path)
        .map_err(|source| StoreError::Persist {
            path: path.to_owned(),
            source,
        })?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), StoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StoreError::Io {
            action: "sync containing directory",
            path: path.to_owned(),
            source,
        })
}

fn content_hash(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn asset_references(source: &str) -> Result<BTreeSet<PathBuf>, StoreError> {
    let mut assets = BTreeSet::new();
    let mut cursor = 0;
    while let Some(relative) = source[cursor..].find("](") {
        let start = cursor + relative + 2;
        let Some(end_offset) = source[start..].find(')') else {
            break;
        };
        let value = source[start..start + end_offset]
            .split_whitespace()
            .next()
            .unwrap_or("");
        let path = Path::new(value);
        if path.starts_with("assets") {
            validate_relative_path(path)?;
            assets.insert(path.to_owned());
        }
        cursor = start + end_offset + 1;
    }
    Ok(assets)
}

fn validate_relative_path(path: &Path) -> Result<(), StoreError> {
    let safe = !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if safe {
        Ok(())
    } else {
        Err(StoreError::UnsafePath {
            path: path.to_owned(),
        })
    }
}

fn copy_regular_file(source: &Path, destination: &Path) -> Result<(), StoreError> {
    let metadata = fs::symlink_metadata(source).map_err(|source_error| StoreError::Io {
        action: "inspect export source",
        path: source.to_owned(),
        source: source_error,
    })?;
    if !metadata.file_type().is_file() {
        return Err(StoreError::UnsafePath {
            path: source.to_owned(),
        });
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source_error| StoreError::Io {
            action: "create export directory",
            path: parent.to_owned(),
            source: source_error,
        })?;
    }
    fs::copy(source, destination).map_err(|source_error| StoreError::Io {
        action: "copy export file",
        path: source.to_owned(),
        source: source_error,
    })?;
    Ok(())
}

fn copy_tree_regular(source: &Path, destination: &Path) -> Result<(), StoreError> {
    fs::create_dir_all(destination).map_err(|source_error| StoreError::Io {
        action: "create export tree",
        path: destination.to_owned(),
        source: source_error,
    })?;
    for entry in fs::read_dir(source).map_err(|source_error| StoreError::Io {
        action: "read export tree",
        path: source.to_owned(),
        source: source_error,
    })? {
        let entry = entry.map_err(|source_error| StoreError::Io {
            action: "read export tree entry",
            path: source.to_owned(),
            source: source_error,
        })?;
        let file_type = entry.file_type().map_err(|source_error| StoreError::Io {
            action: "inspect export tree entry",
            path: entry.path(),
            source: source_error,
        })?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree_regular(&entry.path(), &target)?;
        } else if file_type.is_file() {
            copy_regular_file(&entry.path(), &target)?;
        } else {
            return Err(StoreError::UnsafePath { path: entry.path() });
        }
    }
    Ok(())
}
