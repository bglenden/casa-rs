// SPDX-License-Identifier: LGPL-3.0-or-later

//! Portable tutorial templates and verified project-local dataset acquisition.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use flate2::read::GzDecoder;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::Archive;
use tempfile::NamedTempFile;
use thiserror::Error;
use ureq::ResponseExt;

use crate::{
    ApprovalRecord, ArtifactReference, AttemptHandle, CellId, ConflictResolution, ExecutionStatus,
    NotebookDocument, NotebookId, NotebookSnapshot, NotebookStore, ReceiptFinalization,
    RecordingRequest, RunSafetyRecord, SaveResult, StoreError, TaskCellIntent, Timestamp,
};

/// Current portable `tutorial.toml` schema.
pub const TUTORIAL_MANIFEST_SCHEMA_VERSION: u32 = 1;
/// Current managed `.casa-rs/tutorials/<notebook-id>/lock.toml` schema.
pub const TUTORIAL_LOCK_SCHEMA_VERSION: u32 = 1;

const TUTORIAL_MANIFEST: &str = "tutorial.toml";
const TUTORIAL_MARKDOWN: &str = "tutorial.md";
const TUTORIAL_LOCK: &str = "lock.toml";
const ATTEMPT_ARCHIVE: &str = "download.part";
const MATERIALIZED: &str = "materialized";
const DOWNLOAD_CHUNK_LIMIT: u64 = 8 * 1024 * 1024;

#[derive(Deserialize)]
struct TutorialPackV0 {
    schema_version: String,
    tutorial_id: String,
    title: String,
    #[serde(default)]
    inputs: Vec<TutorialPackV0Input>,
    learner: TutorialPackV0Learner,
    regression: TutorialPackV0Regression,
    #[serde(default)]
    sections: Vec<TutorialPackV0Section>,
}

#[derive(Deserialize)]
struct TutorialPackV0Input {
    id: String,
    display_name: String,
    source_artifact_url: String,
    filename: String,
    checksum_policy: String,
    pack_path: PathBuf,
    #[serde(default)]
    size_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct TutorialPackV0Learner {
    docs_index: PathBuf,
}

#[derive(Deserialize)]
struct TutorialPackV0Regression {
    evidence_path: PathBuf,
}

#[derive(Deserialize)]
struct TutorialPackV0Section {
    id: String,
    title: String,
    #[serde(default)]
    input_refs: Vec<String>,
    #[serde(default)]
    steps: Vec<TutorialPackV0Step>,
}

#[derive(Deserialize)]
struct TutorialPackV0Step {
    surface: String,
    provider_kind: String,
    task_id: String,
    #[serde(default)]
    parameters: BTreeMap<String, serde_json::Value>,
}

/// Portable tutorial-template manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TutorialManifest {
    pub schema_version: u32,
    pub tutorial_id: String,
    pub title: String,
    #[serde(default)]
    pub datasets: Vec<TutorialDataset>,
    #[serde(default)]
    pub sections: Vec<TutorialSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regression: Option<TutorialRegressionOverlay>,
}

/// One tutorial dataset acquisition declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TutorialDataset {
    pub id: String,
    pub display_name: String,
    pub uri: String,
    pub destination: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unpack: Option<TutorialUnpackPlan>,
    #[serde(default)]
    pub checks: Vec<TutorialOptionalCheck>,
}

/// One ordered tutorial section and its stable notebook references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TutorialSection {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub dataset_ids: Vec<String>,
    #[serde(default)]
    pub cell_ids: Vec<CellId>,
}

/// Optional preserved regression-evidence overlay from a migrated tutorial.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TutorialRegressionOverlay {
    pub path: PathBuf,
}

/// Supported archive encodings for the v1 built-in materializer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TutorialArchiveFormat {
    Tar,
    TarGz,
}

/// Explicit bounded archive materialization plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TutorialUnpackPlan {
    pub format: TutorialArchiveFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_root: Option<PathBuf>,
    pub max_entries: u64,
    pub max_expanded_bytes: u64,
}

/// Built-in optional verification checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TutorialCheckKind {
    PathExists,
    RegularFile,
    Directory,
    MeasurementSet,
}

/// One skippable post-materialization verification check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TutorialOptionalCheck {
    pub id: String,
    pub label: String,
    pub kind: TutorialCheckKind,
    #[serde(default)]
    pub path: PathBuf,
}

/// Loaded immutable tutorial template.
#[derive(Debug, Clone)]
pub struct TutorialTemplate {
    pub root: PathBuf,
    pub manifest: TutorialManifest,
    pub markdown: String,
    pub content_sha256: String,
}

/// URI metadata shown before acquisition approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TutorialSourceResolution {
    pub resolved_uri: String,
    pub redirects: Vec<String>,
    pub size_bytes: Option<u64>,
}

/// One bounded source read used by resumable acquisition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TutorialReadChunk {
    pub bytes: Vec<u8>,
    pub complete: bool,
}

/// Pluggable scheme handler. Implementations never delegate to a shell or OS opener.
pub trait TutorialUriHandler: Send + Sync {
    fn scheme(&self) -> &str;
    fn resolve(&self, uri: &str) -> Result<TutorialSourceResolution, TutorialError>;
    fn read_chunk(
        &self,
        uri: &str,
        offset: u64,
        limit: u64,
    ) -> Result<TutorialReadChunk, TutorialError>;
}

/// Versioned scheme-to-handler registry. Unknown schemes remain inert.
#[derive(Clone)]
pub struct TutorialUriRegistry {
    version: u32,
    handlers: BTreeMap<String, Arc<dyn TutorialUriHandler>>,
}

impl TutorialUriRegistry {
    /// Construct the v1 registry with `file`, `http`, and `https` handlers.
    #[must_use]
    pub fn v1() -> Self {
        let mut registry = Self {
            version: 1,
            handlers: BTreeMap::new(),
        };
        registry.register(Arc::new(FileTutorialUriHandler));
        registry.register(Arc::new(HttpTutorialUriHandler::new("http")));
        registry.register(Arc::new(HttpTutorialUriHandler::new("https")));
        registry
    }

    /// Registry contract version persisted with acquisition state.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Install or replace one exact scheme handler.
    pub fn register(&mut self, handler: Arc<dyn TutorialUriHandler>) {
        self.handlers
            .insert(handler.scheme().to_ascii_lowercase(), handler);
    }

    fn handler(&self, uri: &str) -> Result<Arc<dyn TutorialUriHandler>, TutorialError> {
        let scheme = uri_scheme(uri)?;
        self.handlers
            .get(&scheme)
            .cloned()
            .ok_or(TutorialError::UnknownScheme { scheme })
    }
}

struct FileTutorialUriHandler;

impl TutorialUriHandler for FileTutorialUriHandler {
    fn scheme(&self) -> &str {
        "file"
    }

    fn resolve(&self, uri: &str) -> Result<TutorialSourceResolution, TutorialError> {
        let path = file_uri_path(uri)?;
        let metadata = fs::metadata(&path).map_err(|source| TutorialError::Io {
            action: "inspect file tutorial source",
            path: path.clone(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(TutorialError::SourceNotRegularFile { path });
        }
        Ok(TutorialSourceResolution {
            resolved_uri: uri.to_owned(),
            redirects: Vec::new(),
            size_bytes: Some(metadata.len()),
        })
    }

    fn read_chunk(
        &self,
        uri: &str,
        offset: u64,
        limit: u64,
    ) -> Result<TutorialReadChunk, TutorialError> {
        let path = file_uri_path(uri)?;
        let mut file = File::open(&path).map_err(|source| TutorialError::Io {
            action: "open file tutorial source",
            path: path.clone(),
            source,
        })?;
        let size = file
            .metadata()
            .map_err(|source| TutorialError::Io {
                action: "inspect file tutorial source",
                path: path.clone(),
                source,
            })?
            .len();
        file.seek(SeekFrom::Start(offset))
            .map_err(|source| TutorialError::Io {
                action: "seek file tutorial source",
                path: path.clone(),
                source,
            })?;
        let mut bytes = Vec::new();
        file.take(limit)
            .read_to_end(&mut bytes)
            .map_err(|source| TutorialError::Io {
                action: "read file tutorial source",
                path,
                source,
            })?;
        let read = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Ok(TutorialReadChunk {
            bytes,
            complete: offset.saturating_add(read) >= size,
        })
    }
}

struct HttpTutorialUriHandler {
    scheme: &'static str,
    agent: ureq::Agent,
}

impl HttpTutorialUriHandler {
    fn new(scheme: &'static str) -> Self {
        let agent: ureq::Agent = ureq::config::Config::builder()
            .save_redirect_history(true)
            .build()
            .into();
        Self { scheme, agent }
    }
}

impl TutorialUriHandler for HttpTutorialUriHandler {
    fn scheme(&self) -> &str {
        self.scheme
    }

    fn resolve(&self, uri: &str) -> Result<TutorialSourceResolution, TutorialError> {
        let response = self
            .agent
            .head(uri)
            .call()
            .map_err(|source| TutorialError::Network {
                uri: uri.to_owned(),
                detail: source.to_string(),
            })?;
        let resolved_uri = response.get_uri().to_string();
        let redirects = response
            .get_redirect_history()
            .unwrap_or_default()
            .iter()
            .skip(1)
            .map(ToString::to_string)
            .collect();
        let size_bytes = response
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        Ok(TutorialSourceResolution {
            resolved_uri,
            redirects,
            size_bytes,
        })
    }

    fn read_chunk(
        &self,
        uri: &str,
        offset: u64,
        limit: u64,
    ) -> Result<TutorialReadChunk, TutorialError> {
        let end = offset.saturating_add(limit.saturating_sub(1));
        let response = self
            .agent
            .get(uri)
            .header("Range", format!("bytes={offset}-{end}"))
            .call()
            .map_err(|source| TutorialError::Network {
                uri: uri.to_owned(),
                detail: source.to_string(),
            })?;
        if offset > 0 && response.status().as_u16() != 206 {
            return Err(TutorialError::ResumeUnsupported {
                uri: uri.to_owned(),
            });
        }
        let content_range_total = response
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.rsplit_once('/'))
            .and_then(|(_, total)| total.parse::<u64>().ok());
        let mut bytes = Vec::new();
        response
            .into_body()
            .into_reader()
            .take(limit)
            .read_to_end(&mut bytes)
            .map_err(|source| TutorialError::Network {
                uri: uri.to_owned(),
                detail: source.to_string(),
            })?;
        let read = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Ok(TutorialReadChunk {
            bytes,
            complete: content_range_total.is_some_and(|total| offset.saturating_add(read) >= total)
                || read < limit,
        })
    }
}

/// Persisted acquisition phase. Only `ready` implies staged project data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TutorialAcquisitionPhase {
    Missing,
    Downloading,
    Verifying,
    Unpacking,
    Checking,
    Materializing,
    Ready,
    Cancelled,
    NetworkFailed,
    ChecksumFailed,
    UnsafeArchive,
    DestinationCollision,
}

impl TutorialAcquisitionPhase {
    fn is_running(self) -> bool {
        matches!(
            self,
            Self::Downloading
                | Self::Verifying
                | Self::Unpacking
                | Self::Checking
                | Self::Materializing
        )
    }
}

/// How a persisted attempt generation began.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TutorialAttemptKind {
    Initial,
    Resume,
    Restart,
    Retry,
}

/// One exact pre-acquisition approval plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TutorialAcquisitionPlan {
    pub approval_sha256: String,
    pub registry_version: u32,
    pub notebook_id: NotebookId,
    pub dataset_id: String,
    pub scheme: String,
    pub requested_uri: String,
    pub resolved_uri: String,
    pub redirects: Vec<String>,
    pub expected_size_bytes: Option<u64>,
    pub resolved_size_bytes: Option<u64>,
    pub destination: PathBuf,
    pub expected_sha256: Option<String>,
    pub required_disk_bytes: u64,
    pub available_disk_bytes: u64,
    pub unpack: Option<TutorialUnpackPlan>,
    pub checks: Vec<TutorialOptionalCheck>,
    pub missing_digest: bool,
}

impl TutorialAcquisitionPlan {
    #[must_use]
    pub fn has_enough_disk(&self) -> bool {
        self.available_disk_bytes >= self.required_disk_bytes
    }
}

/// Approval bound to the exact plan hash and optional check choices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TutorialAcquisitionApproval {
    pub approval_sha256: String,
    #[serde(default)]
    pub allow_missing_digest: bool,
    #[serde(default)]
    pub skipped_check_ids: Vec<String>,
}

/// Persisted outcome for one optional check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TutorialCheckStatus {
    Passed,
    Failed,
    Skipped,
}

/// Persisted optional-check evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TutorialCheckOutcome {
    pub check_id: String,
    pub status: TutorialCheckStatus,
    pub detail: String,
}

/// One immutable-generation acquisition attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TutorialDatasetAttempt {
    pub generation: u64,
    pub kind: TutorialAttemptKind,
    pub phase: TutorialAcquisitionPhase,
    pub requested_uri: String,
    pub resolved_uri: String,
    pub redirects: Vec<String>,
    pub expected_size_bytes: Option<u64>,
    pub expected_sha256: Option<String>,
    pub approval_sha256: String,
    pub approved_missing_digest: bool,
    pub skipped_check_ids: Vec<String>,
    pub downloaded_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub computed_sha256: Option<String>,
    #[serde(default)]
    pub checks: Vec<TutorialCheckOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub started_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_handle: Option<AttemptHandle>,
}

/// Managed state for one tutorial dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TutorialDatasetLock {
    #[serde(flatten)]
    pub dataset: TutorialDataset,
    pub phase: TutorialAcquisitionPhase,
    pub staged: bool,
    pub current_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_sha256: Option<String>,
    #[serde(default)]
    pub attempts: Vec<TutorialDatasetAttempt>,
}

/// Rust-owned managed tutorial state for one learner notebook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TutorialLock {
    pub schema_version: u32,
    pub registry_version: u32,
    pub notebook_id: NotebookId,
    pub notebook_filename: String,
    pub tutorial_id: String,
    pub title: String,
    pub template_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regression: Option<TutorialRegressionOverlay>,
    pub sections: Vec<TutorialSection>,
    pub datasets: Vec<TutorialDatasetLock>,
}

/// Result of forking an immutable template into one editable learner notebook.
#[derive(Debug, Clone)]
pub struct TutorialForkResult {
    pub notebook: NotebookSnapshot,
    pub lock: TutorialLock,
}

/// Project-scoped tutorial persistence and acquisition service.
#[derive(Clone)]
pub struct TutorialProject {
    project_root: PathBuf,
    registry: TutorialUriRegistry,
}

impl TutorialProject {
    /// Open one explicit project root using the built-in v1 handler registry.
    pub fn open(project_root: impl AsRef<Path>) -> Result<Self, TutorialError> {
        Self::with_registry(project_root, TutorialUriRegistry::v1())
    }

    /// Open one explicit project root with an application-supplied registry.
    pub fn with_registry(
        project_root: impl AsRef<Path>,
        registry: TutorialUriRegistry,
    ) -> Result<Self, TutorialError> {
        let store = NotebookStore::open(project_root.as_ref()).map_err(TutorialError::Store)?;
        Ok(Self {
            project_root: store.project_root().to_owned(),
            registry,
        })
    }

    /// Load and validate an immutable portable template folder.
    pub fn load_template(path: impl AsRef<Path>) -> Result<TutorialTemplate, TutorialError> {
        let root = fs::canonicalize(path.as_ref()).map_err(|source| TutorialError::Io {
            action: "resolve tutorial template",
            path: path.as_ref().to_owned(),
            source,
        })?;
        if !root.is_dir() {
            return Err(TutorialError::TemplateNotDirectory { path: root });
        }
        let manifest_path = root.join(TUTORIAL_MANIFEST);
        let markdown_path = root.join(TUTORIAL_MARKDOWN);
        let manifest_source =
            fs::read_to_string(&manifest_path).map_err(|source| TutorialError::Io {
                action: "read tutorial manifest",
                path: manifest_path.clone(),
                source,
            })?;
        let manifest: TutorialManifest =
            toml::from_str(&manifest_source).map_err(|source| TutorialError::Manifest {
                path: manifest_path.clone(),
                detail: source.to_string(),
            })?;
        validate_manifest(&root, &manifest)?;
        let markdown = fs::read_to_string(&markdown_path).map_err(|source| TutorialError::Io {
            action: "read tutorial Markdown",
            path: markdown_path,
            source,
        })?;
        validate_template_cells(&manifest, &markdown)?;
        let content_sha256 = tutorial_template_hash(&manifest_source, &markdown, &root)?;
        Ok(TutorialTemplate {
            root,
            manifest,
            markdown,
            content_sha256,
        })
    }

    /// Convert one legacy `tutorial-pack.v0` folder into a new immutable v1 template.
    ///
    /// This is a one-shot conversion: the destination must not already exist and
    /// no runtime compatibility reader is retained after conversion.
    pub fn migrate_v0_template(
        pack_path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<TutorialTemplate, TutorialError> {
        let pack_path = pack_path.as_ref();
        let manifest_path = if pack_path.is_dir() {
            pack_path.join("pack.json")
        } else {
            pack_path.to_owned()
        };
        let pack_root =
            manifest_path
                .parent()
                .ok_or_else(|| TutorialError::UnsafeTemplatePath {
                    path: manifest_path.clone(),
                })?;
        let destination = destination.as_ref();
        if destination.exists() {
            return Err(TutorialError::MigrationDestinationExists {
                path: destination.to_owned(),
            });
        }
        let source = fs::read_to_string(&manifest_path).map_err(|source| TutorialError::Io {
            action: "read tutorial-pack v0 manifest",
            path: manifest_path.clone(),
            source,
        })?;
        let pack: TutorialPackV0 =
            serde_json::from_str(&source).map_err(|source| TutorialError::Manifest {
                path: manifest_path.clone(),
                detail: source.to_string(),
            })?;
        if pack.schema_version != "tutorial-pack.v0" {
            return Err(TutorialError::UnsupportedV0Schema {
                version: pack.schema_version,
            });
        }
        let parent = destination
            .parent()
            .ok_or_else(|| TutorialError::UnsafeTemplatePath {
                path: destination.to_owned(),
            })?;
        fs::create_dir_all(parent).map_err(|source| TutorialError::Io {
            action: "create migrated tutorial parent",
            path: parent.to_owned(),
            source,
        })?;
        let temporary = tempfile::Builder::new()
            .prefix(".tutorial-v1-")
            .tempdir_in(parent)
            .map_err(|source| TutorialError::Io {
                action: "create migrated tutorial staging directory",
                path: parent.to_owned(),
                source,
            })?;
        let staging = temporary.path();

        let docs_path = resolve_pack_relative(pack_root, &pack.learner.docs_index)?;
        let mut markdown = fs::read_to_string(&docs_path).map_err(|source| TutorialError::Io {
            action: "read tutorial-pack learner prose",
            path: docs_path,
            source,
        })?;
        let mut sections = Vec::with_capacity(pack.sections.len());
        for section in &pack.sections {
            let mut cell_ids = Vec::new();
            for step in section
                .steps
                .iter()
                .filter(|step| step.surface == "gui" && step.provider_kind == "native-rust")
            {
                let cell_id = CellId::new();
                let intent = TaskCellIntent {
                    format: 1,
                    surface: step.task_id.clone(),
                    kind: "task".to_owned(),
                    contract: 1,
                    parameters: step
                        .parameters
                        .iter()
                        .map(|(name, value)| {
                            json_to_toml(value)
                                .map(|value| (name.clone(), value))
                                .ok_or_else(|| TutorialError::V0Parameter {
                                    section_id: section.id.clone(),
                                    parameter: name.clone(),
                                })
                        })
                        .collect::<Result<_, _>>()?,
                };
                markdown.push_str(&format!(
                    "\n\n<!-- casa-rs-cell:v1 id={cell_id} kind=task -->\n{}<!-- /casa-rs-cell -->\n",
                    intent.to_markdown().map_err(TutorialError::Notebook)?
                ));
                cell_ids.push(cell_id);
            }
            sections.push(TutorialSection {
                id: section.id.clone(),
                title: section.title.clone(),
                dataset_ids: section.input_refs.clone(),
                cell_ids,
            });
        }
        fs::write(staging.join(TUTORIAL_MARKDOWN), markdown).map_err(|source| {
            TutorialError::Io {
                action: "write migrated tutorial Markdown",
                path: staging.join(TUTORIAL_MARKDOWN),
                source,
            }
        })?;

        let legacy_assets = pack_root.join("assets");
        if legacy_assets.is_dir() {
            copy_tree(&legacy_assets, &staging.join("assets"))?;
        }
        let regression_candidate = pack_root.join(&pack.regression.evidence_path);
        let regression = if regression_candidate.exists() {
            let regression_source =
                resolve_pack_relative(pack_root, &pack.regression.evidence_path)?;
            let regression_path = PathBuf::from("regression");
            if regression_source.is_dir() {
                copy_tree(&regression_source, &staging.join(&regression_path))?;
            } else {
                fs::create_dir_all(staging.join(&regression_path)).map_err(|source| {
                    TutorialError::Io {
                        action: "create migrated regression directory",
                        path: staging.join(&regression_path),
                        source,
                    }
                })?;
                fs::copy(
                    &regression_source,
                    staging.join(&regression_path).join("evidence"),
                )
                .map_err(|source| TutorialError::Io {
                    action: "copy migrated regression evidence",
                    path: regression_source,
                    source,
                })?;
            }
            Some(TutorialRegressionOverlay {
                path: regression_path,
            })
        } else {
            None
        };
        let manifest = TutorialManifest {
            schema_version: TUTORIAL_MANIFEST_SCHEMA_VERSION,
            tutorial_id: pack.tutorial_id,
            title: pack.title,
            datasets: pack
                .inputs
                .into_iter()
                .map(|input| {
                    let archive_format = if input.filename.ends_with(".tar.gz")
                        || input.filename.ends_with(".tgz")
                    {
                        Some(TutorialArchiveFormat::TarGz)
                    } else if input.filename.ends_with(".tar") {
                        Some(TutorialArchiveFormat::Tar)
                    } else {
                        None
                    };
                    let unpack = archive_format.map(|format| TutorialUnpackPlan {
                        format,
                        archive_root: input.pack_path.file_name().map(PathBuf::from),
                        max_entries: 100_000,
                        max_expanded_bytes: input
                            .size_bytes
                            .unwrap_or(1024 * 1024 * 1024)
                            .saturating_mul(10),
                    });
                    TutorialDataset {
                        id: input.id,
                        display_name: input.display_name,
                        uri: input.source_artifact_url,
                        destination: input.pack_path,
                        expected_size_bytes: input.size_bytes,
                        sha256: checksum_policy_sha256(&input.checksum_policy),
                        unpack,
                        checks: Vec::new(),
                    }
                })
                .collect(),
            sections,
            regression,
        };
        fs::write(
            staging.join(TUTORIAL_MANIFEST),
            toml::to_string_pretty(&manifest)
                .map_err(|source| TutorialError::Serialize(source.to_string()))?,
        )
        .map_err(|source| TutorialError::Io {
            action: "write migrated tutorial manifest",
            path: staging.join(TUTORIAL_MANIFEST),
            source,
        })?;
        let staging_path = temporary.keep();
        fs::rename(&staging_path, destination).map_err(|source| TutorialError::Io {
            action: "publish migrated tutorial template",
            path: destination.to_owned(),
            source,
        })?;
        Self::load_template(destination)
    }

    /// Fork one template into an editable notebook and managed tutorial lock.
    pub fn fork_template(
        &self,
        template: &TutorialTemplate,
        filename: &str,
    ) -> Result<TutorialForkResult, TutorialError> {
        let store = NotebookStore::open(&self.project_root).map_err(TutorialError::Store)?;
        let created = store
            .create_named(filename, &template.manifest.title)
            .map_err(TutorialError::Store)?;
        let notebook_id = created.entry.id;
        let result = self.finish_fork(&store, template, created);
        if result.is_err() {
            let _ = fs::remove_file(self.project_root.join("notebooks").join(filename));
            let _ = fs::remove_dir_all(
                self.project_root
                    .join("notebooks/assets")
                    .join(notebook_id.to_string()),
            );
            let _ = fs::remove_dir_all(self.tutorial_directory(notebook_id));
        }
        result
    }

    fn finish_fork(
        &self,
        store: &NotebookStore,
        template: &TutorialTemplate,
        created: NotebookSnapshot,
    ) -> Result<TutorialForkResult, TutorialError> {
        let notebook_id = created.entry.id;
        let assets_destination = self
            .project_root
            .join("notebooks/assets")
            .join(notebook_id.to_string());
        let lock_directory = self.tutorial_directory(notebook_id);
        let mut source = strip_notebook_marker(&template.markdown);
        source = rewrite_template_asset_links(&source, notebook_id);
        let source = format!(
            "<!-- casa-rs-notebook:v1 id={notebook_id} -->\n\n{}",
            source.trim_start()
        );
        NotebookDocument::parse(source.clone()).map_err(TutorialError::Notebook)?;

        if template.root.join("assets").exists() {
            copy_tree(&template.root.join("assets"), &assets_destination)?;
        }
        fs::create_dir_all(&lock_directory).map_err(|source| TutorialError::Io {
            action: "create tutorial managed directory",
            path: lock_directory.clone(),
            source,
        })?;
        atomic_write(
            &lock_directory.join(TUTORIAL_MANIFEST),
            toml::to_string_pretty(&template.manifest)
                .map_err(|source| TutorialError::Serialize(source.to_string()))?
                .as_bytes(),
        )?;

        let save = store
            .save_notebook(&created, &source, ConflictResolution::Reject)
            .map_err(TutorialError::Store)?;
        let notebook = match save {
            SaveResult::Saved(snapshot) => snapshot,
            SaveResult::Reloaded(_) | SaveResult::Conflict(_) => {
                return Err(TutorialError::ForkConflict);
            }
        };
        let lock = TutorialLock {
            schema_version: TUTORIAL_LOCK_SCHEMA_VERSION,
            registry_version: self.registry.version(),
            notebook_id,
            notebook_filename: notebook.entry.filename.clone(),
            tutorial_id: template.manifest.tutorial_id.clone(),
            title: template.manifest.title.clone(),
            template_sha256: template.content_sha256.clone(),
            regression: template.manifest.regression.clone(),
            sections: template.manifest.sections.clone(),
            datasets: template
                .manifest
                .datasets
                .iter()
                .cloned()
                .map(|dataset| TutorialDatasetLock {
                    dataset,
                    phase: TutorialAcquisitionPhase::Missing,
                    staged: false,
                    current_generation: 0,
                    pinned_sha256: None,
                    attempts: Vec::new(),
                })
                .collect(),
        };
        self.write_lock(&lock)?;
        Ok(TutorialForkResult { notebook, lock })
    }

    /// Reopen one managed learner tutorial.
    pub fn load_lock(&self, notebook_id: NotebookId) -> Result<TutorialLock, TutorialError> {
        let path = self.tutorial_directory(notebook_id).join(TUTORIAL_LOCK);
        let source = fs::read_to_string(&path).map_err(|source| TutorialError::Io {
            action: "read tutorial lock",
            path: path.clone(),
            source,
        })?;
        let lock: TutorialLock = toml::from_str(&source).map_err(|source| TutorialError::Lock {
            path,
            detail: source.to_string(),
        })?;
        if lock.schema_version != TUTORIAL_LOCK_SCHEMA_VERSION {
            return Err(TutorialError::UnsupportedLockVersion {
                version: lock.schema_version,
            });
        }
        Ok(lock)
    }

    /// List every managed learner tutorial in stable notebook-id order.
    pub fn list_locks(&self) -> Result<Vec<TutorialLock>, TutorialError> {
        let root = self.project_root.join(".casa-rs/tutorials");
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut locks = fs::read_dir(&root)
            .map_err(|source| TutorialError::Io {
                action: "list managed tutorials",
                path: root.clone(),
                source,
            })?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|value| value.parse().ok())
            })
            .map(|notebook_id| self.load_lock(notebook_id))
            .collect::<Result<Vec<_>, _>>()?;
        locks.sort_by_key(|lock| lock.notebook_id.to_string());
        Ok(locks)
    }

    /// Resolve one declared or user-supplied source into the exact approval facts.
    pub fn plan_acquisition(
        &self,
        notebook_id: NotebookId,
        dataset_id: &str,
        source_override: Option<&str>,
    ) -> Result<TutorialAcquisitionPlan, TutorialError> {
        let _guard = self.lock_project()?;
        let lock = self.load_lock(notebook_id)?;
        let dataset = lock
            .datasets
            .iter()
            .find(|entry| entry.dataset.id == dataset_id)
            .ok_or_else(|| TutorialError::DatasetNotFound {
                dataset_id: dataset_id.to_owned(),
            })?;
        let requested_uri = source_override.unwrap_or(&dataset.dataset.uri).to_owned();
        let handler = self.registry.handler(&requested_uri)?;
        let resolution = handler.resolve(&requested_uri)?;
        let expected_size_bytes = dataset.dataset.expected_size_bytes;
        let required_disk_bytes = required_disk_bytes(&dataset.dataset, &resolution);
        let available_disk_bytes =
            fs2::available_space(&self.project_root).map_err(|source| TutorialError::Io {
                action: "inspect tutorial project free space",
                path: self.project_root.clone(),
                source,
            })?;
        let mut plan = TutorialAcquisitionPlan {
            approval_sha256: String::new(),
            registry_version: self.registry.version(),
            notebook_id,
            dataset_id: dataset_id.to_owned(),
            scheme: uri_scheme(&requested_uri)?,
            requested_uri,
            resolved_uri: resolution.resolved_uri,
            redirects: resolution.redirects,
            expected_size_bytes,
            resolved_size_bytes: resolution.size_bytes,
            destination: dataset.dataset.destination.clone(),
            expected_sha256: dataset
                .pinned_sha256
                .clone()
                .or_else(|| dataset.dataset.sha256.clone()),
            required_disk_bytes,
            available_disk_bytes,
            unpack: dataset.dataset.unpack.clone(),
            checks: dataset.dataset.checks.clone(),
            missing_digest: dataset.pinned_sha256.is_none() && dataset.dataset.sha256.is_none(),
        };
        plan.approval_sha256 = acquisition_plan_hash(&plan)?;
        Ok(plan)
    }

    /// Begin an explicitly approved attempt without downloading any bytes yet.
    pub fn begin_acquisition(
        &self,
        plan: &TutorialAcquisitionPlan,
        approval: TutorialAcquisitionApproval,
    ) -> Result<TutorialDatasetLock, TutorialError> {
        self.begin_attempt(plan, approval, TutorialAttemptKind::Initial, false, true)
    }

    /// Resume a cancelled or network-failed partial download in a new generation.
    pub fn resume_acquisition(
        &self,
        notebook_id: NotebookId,
        dataset_id: &str,
    ) -> Result<TutorialDatasetLock, TutorialError> {
        self.resume_or_restart(notebook_id, dataset_id, TutorialAttemptKind::Resume, true)
    }

    /// Restart from byte zero in a new generation.
    pub fn restart_acquisition(
        &self,
        notebook_id: NotebookId,
        dataset_id: &str,
    ) -> Result<TutorialDatasetLock, TutorialError> {
        self.resume_or_restart(notebook_id, dataset_id, TutorialAttemptKind::Restart, false)
    }

    /// Retry a recoverable failure, resuming network failures and restarting integrity failures.
    pub fn retry_acquisition(
        &self,
        notebook_id: NotebookId,
        dataset_id: &str,
    ) -> Result<TutorialDatasetLock, TutorialError> {
        let lock = self.load_lock(notebook_id)?;
        let dataset = lock
            .datasets
            .iter()
            .find(|entry| entry.dataset.id == dataset_id)
            .ok_or_else(|| TutorialError::DatasetNotFound {
                dataset_id: dataset_id.to_owned(),
            })?;
        let resume = dataset.phase == TutorialAcquisitionPhase::NetworkFailed;
        if !matches!(
            dataset.phase,
            TutorialAcquisitionPhase::NetworkFailed
                | TutorialAcquisitionPhase::ChecksumFailed
                | TutorialAcquisitionPhase::UnsafeArchive
                | TutorialAcquisitionPhase::DestinationCollision
        ) {
            return Err(TutorialError::InvalidTransition {
                dataset_id: dataset_id.to_owned(),
                phase: dataset.phase,
                action: "retry",
            });
        }
        self.resume_or_restart(notebook_id, dataset_id, TutorialAttemptKind::Retry, resume)
    }

    fn begin_attempt(
        &self,
        plan: &TutorialAcquisitionPlan,
        approval: TutorialAcquisitionApproval,
        kind: TutorialAttemptKind,
        resume: bool,
        validate_plan_hash: bool,
    ) -> Result<TutorialDatasetLock, TutorialError> {
        let _guard = self.lock_project()?;
        let mut lock = self.load_lock(plan.notebook_id)?;
        let dataset = dataset_mut(&mut lock, &plan.dataset_id)?;
        validate_plan_dataset(plan, dataset)?;
        if plan.registry_version != self.registry.version()
            || (validate_plan_hash && plan.approval_sha256 != acquisition_plan_hash(plan)?)
            || approval.approval_sha256 != plan.approval_sha256
        {
            return Err(TutorialError::ApprovalMismatch);
        }
        if dataset.phase == TutorialAcquisitionPhase::Ready
            || (kind == TutorialAttemptKind::Initial && dataset.phase.is_running())
        {
            return Err(TutorialError::InvalidTransition {
                dataset_id: plan.dataset_id.clone(),
                phase: dataset.phase,
                action: "begin",
            });
        }
        if plan.missing_digest && !approval.allow_missing_digest {
            return Err(TutorialError::MissingDigestApprovalRequired);
        }
        let available_disk_bytes =
            fs2::available_space(&self.project_root).map_err(|source| TutorialError::Io {
                action: "recheck tutorial project free space",
                path: self.project_root.clone(),
                source,
            })?;
        if !plan.has_enough_disk() || available_disk_bytes < plan.required_disk_bytes {
            return Err(TutorialError::InsufficientDisk {
                required: plan.required_disk_bytes,
                available: available_disk_bytes.min(plan.available_disk_bytes),
            });
        }
        let known_checks: BTreeSet<&str> = dataset
            .dataset
            .checks
            .iter()
            .map(|check| check.id.as_str())
            .collect();
        if let Some(unknown) = approval
            .skipped_check_ids
            .iter()
            .find(|id| !known_checks.contains(id.as_str()))
        {
            return Err(TutorialError::UnknownCheck {
                check_id: unknown.clone(),
            });
        }
        let generation = dataset.current_generation.saturating_add(1);
        let attempt_directory = self.attempt_directory(plan.notebook_id, &plan.dataset_id);
        fs::create_dir_all(&attempt_directory).map_err(|source| TutorialError::Io {
            action: "create tutorial attempt directory",
            path: attempt_directory.clone(),
            source,
        })?;
        let part_path = attempt_directory.join(ATTEMPT_ARCHIVE);
        if !resume {
            remove_path_if_exists(&part_path)?;
            remove_path_if_exists(&attempt_directory.join(MATERIALIZED))?;
        }
        let downloaded_bytes = fs::metadata(&part_path)
            .map(|value| value.len())
            .unwrap_or(0);
        if dataset.phase.is_running()
            && let Ok(previous) = current_attempt_mut(dataset)
        {
            previous.phase = TutorialAcquisitionPhase::Cancelled;
            previous.error = Some("Superseded by a newer acquisition generation".to_owned());
            previous.finished_at = Some(Timestamp::now());
            self.finalize_current_receipt(
                dataset,
                ExecutionStatus::Cancelled,
                vec!["Superseded by a newer acquisition generation".to_owned()],
            )?;
        }
        let receipt_handle = NotebookStore::open(&self.project_root)
            .map_err(TutorialError::Store)?
            .begin_attempt(RecordingRequest {
                initiating_surface: "tutorial".to_owned(),
                operation_id: format!("tutorial.acquire.{}", plan.dataset_id),
                notebook_id: Some(plan.notebook_id),
                cell_id: None,
                task_intent: None,
                execution_input: None,
                provider_contract_version: plan.registry_version,
                resolved_parameters: BTreeMap::from([
                    (
                        "requested_uri".to_owned(),
                        plan.requested_uri.clone().into(),
                    ),
                    ("resolved_uri".to_owned(), plan.resolved_uri.clone().into()),
                    (
                        "destination".to_owned(),
                        plan.destination.to_string_lossy().into_owned().into(),
                    ),
                    (
                        "attempt_kind".to_owned(),
                        format!("{kind:?}").to_lowercase().into(),
                    ),
                ]),
                run_safety: RunSafetyRecord {
                    classification: "project_local_dataset_acquisition".to_owned(),
                    affected_paths: vec![plan.destination.clone()],
                },
                approvals: vec![ApprovalRecord {
                    kind: "tutorial_dataset_acquisition".to_owned(),
                    actor: "user".to_owned(),
                    timestamp: Timestamp::now(),
                    content_hash: Some(plan.approval_sha256.clone()),
                }],
            })
            .map_err(TutorialError::Store)?;
        let attempt = TutorialDatasetAttempt {
            generation,
            kind,
            phase: TutorialAcquisitionPhase::Downloading,
            requested_uri: plan.requested_uri.clone(),
            resolved_uri: plan.resolved_uri.clone(),
            redirects: plan.redirects.clone(),
            expected_size_bytes: plan.expected_size_bytes.or(plan.resolved_size_bytes),
            expected_sha256: plan.expected_sha256.clone(),
            approval_sha256: plan.approval_sha256.clone(),
            approved_missing_digest: approval.allow_missing_digest,
            skipped_check_ids: approval.skipped_check_ids,
            downloaded_bytes,
            computed_sha256: None,
            checks: Vec::new(),
            error: None,
            started_at: Timestamp::now(),
            finished_at: None,
            receipt_handle: Some(receipt_handle),
        };
        dataset.current_generation = generation;
        dataset.phase = TutorialAcquisitionPhase::Downloading;
        dataset.staged = false;
        dataset.attempts.push(attempt);
        let result = dataset.clone();
        self.write_lock(&lock)?;
        Ok(result)
    }

    fn resume_or_restart(
        &self,
        notebook_id: NotebookId,
        dataset_id: &str,
        kind: TutorialAttemptKind,
        resume: bool,
    ) -> Result<TutorialDatasetLock, TutorialError> {
        let lock = self.load_lock(notebook_id)?;
        let dataset = lock
            .datasets
            .iter()
            .find(|entry| entry.dataset.id == dataset_id)
            .ok_or_else(|| TutorialError::DatasetNotFound {
                dataset_id: dataset_id.to_owned(),
            })?;
        if kind == TutorialAttemptKind::Resume
            && !matches!(
                dataset.phase,
                TutorialAcquisitionPhase::Cancelled | TutorialAcquisitionPhase::NetworkFailed
            )
        {
            return Err(TutorialError::InvalidTransition {
                dataset_id: dataset_id.to_owned(),
                phase: dataset.phase,
                action: "resume",
            });
        }
        if dataset.phase == TutorialAcquisitionPhase::Ready {
            return Err(TutorialError::InvalidTransition {
                dataset_id: dataset_id.to_owned(),
                phase: dataset.phase,
                action: "restart",
            });
        }
        let previous = dataset
            .attempts
            .last()
            .ok_or_else(|| TutorialError::NoAttempt {
                dataset_id: dataset_id.to_owned(),
            })?;
        let expected_sha256 = dataset
            .pinned_sha256
            .clone()
            .or_else(|| previous.expected_sha256.clone());
        let plan = TutorialAcquisitionPlan {
            approval_sha256: previous.approval_sha256.clone(),
            registry_version: lock.registry_version,
            notebook_id,
            dataset_id: dataset_id.to_owned(),
            scheme: uri_scheme(&previous.requested_uri)?,
            requested_uri: previous.requested_uri.clone(),
            resolved_uri: previous.resolved_uri.clone(),
            redirects: previous.redirects.clone(),
            expected_size_bytes: previous.expected_size_bytes,
            resolved_size_bytes: previous.expected_size_bytes,
            destination: dataset.dataset.destination.clone(),
            expected_sha256: expected_sha256.clone(),
            required_disk_bytes: 0,
            available_disk_bytes: u64::MAX,
            unpack: dataset.dataset.unpack.clone(),
            checks: dataset.dataset.checks.clone(),
            missing_digest: expected_sha256.is_none(),
        };
        let approval = TutorialAcquisitionApproval {
            approval_sha256: previous.approval_sha256.clone(),
            allow_missing_digest: previous.approved_missing_digest,
            skipped_check_ids: previous.skipped_check_ids.clone(),
        };
        self.begin_attempt(&plan, approval, kind, resume, false)
    }

    /// Cancel the current generation without exposing incomplete project data.
    pub fn cancel_acquisition(
        &self,
        notebook_id: NotebookId,
        dataset_id: &str,
        generation: u64,
    ) -> Result<TutorialDatasetLock, TutorialError> {
        let _guard = self.lock_project()?;
        let mut lock = self.load_lock(notebook_id)?;
        let dataset = dataset_mut(&mut lock, dataset_id)?;
        ensure_generation(dataset, generation)?;
        if !dataset.phase.is_running() {
            return Err(TutorialError::InvalidTransition {
                dataset_id: dataset_id.to_owned(),
                phase: dataset.phase,
                action: "cancel",
            });
        }
        let attempt = current_attempt_mut(dataset)?;
        attempt.phase = TutorialAcquisitionPhase::Cancelled;
        attempt.finished_at = Some(Timestamp::now());
        dataset.phase = TutorialAcquisitionPhase::Cancelled;
        dataset.staged = false;
        self.finalize_current_receipt(dataset, ExecutionStatus::Cancelled, Vec::new())?;
        let result = dataset.clone();
        self.write_lock(&lock)?;
        Ok(result)
    }

    /// Advance exactly one bounded download chunk or one integrity/materialization phase.
    pub fn advance_acquisition(
        &self,
        notebook_id: NotebookId,
        dataset_id: &str,
        generation: u64,
        max_download_bytes: u64,
    ) -> Result<TutorialDatasetLock, TutorialError> {
        let _guard = self.lock_project()?;
        let mut lock = self.load_lock(notebook_id)?;
        let dataset = dataset_mut(&mut lock, dataset_id)?;
        ensure_generation(dataset, generation)?;
        let result = self.advance_dataset(notebook_id, dataset, max_download_bytes);
        if let Err(error) = &result {
            apply_attempt_failure(dataset, error);
            self.finalize_current_receipt(
                dataset,
                ExecutionStatus::Failed,
                vec![error.to_string()],
            )?;
        } else if dataset.phase == TutorialAcquisitionPhase::Ready {
            self.finalize_current_receipt(dataset, ExecutionStatus::Succeeded, Vec::new())?;
        }
        let snapshot = dataset.clone();
        self.write_lock(&lock)?;
        result.map(|()| snapshot)
    }

    fn finalize_current_receipt(
        &self,
        dataset: &mut TutorialDatasetLock,
        status: ExecutionStatus,
        diagnostics: Vec<String>,
    ) -> Result<(), TutorialError> {
        let handle = current_attempt_mut(dataset)?.receipt_handle.take();
        let Some(handle) = handle else {
            return Ok(());
        };
        let succeeded = status == ExecutionStatus::Succeeded;
        let destination = dataset.dataset.destination.clone();
        NotebookStore::open(&self.project_root)
            .map_err(TutorialError::Store)?
            .finalize_attempt(
                &handle,
                ReceiptFinalization {
                    status,
                    finished_at: Timestamp::now(),
                    affected_paths: succeeded.then(|| destination.clone()).into_iter().collect(),
                    products: succeeded
                        .then(|| ArtifactReference {
                            role: "tutorial_dataset".to_owned(),
                            path: destination,
                            media_type: None,
                        })
                        .into_iter()
                        .collect(),
                    artifacts: Vec::new(),
                    diagnostics,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                    casa_log: None,
                },
            )
            .map_err(TutorialError::Store)?;
        Ok(())
    }

    fn advance_dataset(
        &self,
        notebook_id: NotebookId,
        dataset: &mut TutorialDatasetLock,
        max_download_bytes: u64,
    ) -> Result<(), TutorialError> {
        let attempt_directory = self.attempt_directory(notebook_id, &dataset.dataset.id);
        let part_path = attempt_directory.join(ATTEMPT_ARCHIVE);
        let phase = dataset.phase;
        match phase {
            TutorialAcquisitionPhase::Downloading => {
                let limit = max_download_bytes.clamp(1, DOWNLOAD_CHUNK_LIMIT);
                let attempt = current_attempt_mut(dataset)?;
                let handler = self.registry.handler(&attempt.requested_uri)?;
                let chunk =
                    handler.read_chunk(&attempt.resolved_uri, attempt.downloaded_bytes, limit)?;
                if chunk.bytes.is_empty() && !chunk.complete {
                    return Err(TutorialError::UnexpectedEndOfSource {
                        uri: attempt.resolved_uri.clone(),
                    });
                }
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&part_path)
                    .map_err(|source| TutorialError::Io {
                        action: "append tutorial download",
                        path: part_path.clone(),
                        source,
                    })?;
                file.write_all(&chunk.bytes)
                    .and_then(|()| file.sync_all())
                    .map_err(|source| TutorialError::Io {
                        action: "persist tutorial download chunk",
                        path: part_path.clone(),
                        source,
                    })?;
                attempt.downloaded_bytes = attempt
                    .downloaded_bytes
                    .saturating_add(u64::try_from(chunk.bytes.len()).unwrap_or(u64::MAX));
                if let Some(expected) = attempt.expected_size_bytes {
                    if attempt.downloaded_bytes > expected {
                        return Err(TutorialError::SizeMismatch {
                            expected,
                            actual: attempt.downloaded_bytes,
                        });
                    }
                    if attempt.downloaded_bytes == expected {
                        attempt.phase = TutorialAcquisitionPhase::Verifying;
                        dataset.phase = TutorialAcquisitionPhase::Verifying;
                    } else if chunk.complete {
                        return Err(TutorialError::SizeMismatch {
                            expected,
                            actual: attempt.downloaded_bytes,
                        });
                    }
                } else if chunk.complete {
                    attempt.phase = TutorialAcquisitionPhase::Verifying;
                    dataset.phase = TutorialAcquisitionPhase::Verifying;
                }
            }
            TutorialAcquisitionPhase::Verifying => {
                let size = fs::metadata(&part_path)
                    .map_err(|source| TutorialError::Io {
                        action: "inspect downloaded tutorial source",
                        path: part_path.clone(),
                        source,
                    })?
                    .len();
                let has_unpack = dataset.dataset.unpack.is_some();
                let expected_size = current_attempt_mut(dataset)?.expected_size_bytes;
                if let Some(expected) = expected_size
                    && size != expected
                {
                    return Err(TutorialError::SizeMismatch {
                        expected,
                        actual: size,
                    });
                }
                let computed = file_sha256(&part_path)?;
                let expected_sha256 = current_attempt_mut(dataset)?.expected_sha256.clone();
                if let Some(expected) = &expected_sha256
                    && !expected.eq_ignore_ascii_case(&computed)
                {
                    return Err(TutorialError::ChecksumMismatch {
                        expected: expected.clone(),
                        actual: computed,
                    });
                }
                let approved_missing_digest = current_attempt_mut(dataset)?.approved_missing_digest;
                if expected_sha256.is_none() && !approved_missing_digest {
                    return Err(TutorialError::MissingDigestApprovalRequired);
                }
                if expected_sha256.is_none() {
                    dataset.pinned_sha256 = Some(computed.clone());
                }
                let next = if has_unpack {
                    TutorialAcquisitionPhase::Unpacking
                } else {
                    TutorialAcquisitionPhase::Checking
                };
                let attempt = current_attempt_mut(dataset)?;
                attempt.computed_sha256 = Some(computed);
                attempt.phase = next;
                dataset.phase = next;
            }
            TutorialAcquisitionPhase::Unpacking => {
                let materialized = attempt_directory.join(MATERIALIZED);
                remove_path_if_exists(&materialized)?;
                let plan = dataset
                    .dataset
                    .unpack
                    .as_ref()
                    .ok_or(TutorialError::MissingUnpackPlan)?;
                safely_extract_archive(&part_path, &materialized, plan)?;
                current_attempt_mut(dataset)?.phase = TutorialAcquisitionPhase::Checking;
                dataset.phase = TutorialAcquisitionPhase::Checking;
            }
            TutorialAcquisitionPhase::Checking => {
                if dataset.dataset.unpack.is_none() {
                    let materialized = attempt_directory.join(MATERIALIZED);
                    remove_path_if_exists(&materialized)?;
                    fs::copy(&part_path, &materialized).map_err(|source| TutorialError::Io {
                        action: "stage verified tutorial file",
                        path: materialized,
                        source,
                    })?;
                }
                let check_root = materialized_source(&attempt_directory, &dataset.dataset)?;
                let skipped: BTreeSet<String> = current_attempt_mut(dataset)?
                    .skipped_check_ids
                    .iter()
                    .cloned()
                    .collect();
                let outcomes = dataset
                    .dataset
                    .checks
                    .iter()
                    .map(|check| run_optional_check(check, &check_root, &skipped))
                    .collect();
                let attempt = current_attempt_mut(dataset)?;
                attempt.checks = outcomes;
                attempt.phase = TutorialAcquisitionPhase::Materializing;
                dataset.phase = TutorialAcquisitionPhase::Materializing;
            }
            TutorialAcquisitionPhase::Materializing => {
                let source = materialized_source(&attempt_directory, &dataset.dataset)?;
                let destination = self.project_root.join(&dataset.dataset.destination);
                if destination.exists() {
                    return Err(TutorialError::DestinationExists { path: destination });
                }
                let parent =
                    destination
                        .parent()
                        .ok_or_else(|| TutorialError::UnsafeProjectPath {
                            path: dataset.dataset.destination.clone(),
                        })?;
                fs::create_dir_all(parent).map_err(|source| TutorialError::Io {
                    action: "create tutorial dataset destination parent",
                    path: parent.to_owned(),
                    source,
                })?;
                fs::rename(&source, &destination).map_err(|source| TutorialError::Io {
                    action: "publish verified tutorial dataset",
                    path: destination,
                    source,
                })?;
                let attempt = current_attempt_mut(dataset)?;
                attempt.phase = TutorialAcquisitionPhase::Ready;
                attempt.finished_at = Some(Timestamp::now());
                dataset.phase = TutorialAcquisitionPhase::Ready;
                dataset.staged = true;
            }
            _ => {
                return Err(TutorialError::InvalidTransition {
                    dataset_id: dataset.dataset.id.clone(),
                    phase,
                    action: "advance",
                });
            }
        }
        Ok(())
    }

    fn tutorial_directory(&self, notebook_id: NotebookId) -> PathBuf {
        self.project_root
            .join(".casa-rs/tutorials")
            .join(notebook_id.to_string())
    }

    fn attempt_directory(&self, notebook_id: NotebookId, dataset_id: &str) -> PathBuf {
        self.tutorial_directory(notebook_id)
            .join("staging")
            .join(dataset_id)
    }

    fn write_lock(&self, lock: &TutorialLock) -> Result<(), TutorialError> {
        let path = self
            .tutorial_directory(lock.notebook_id)
            .join(TUTORIAL_LOCK);
        let source = toml::to_string_pretty(lock)
            .map_err(|source| TutorialError::Serialize(source.to_string()))?;
        atomic_write(&path, source.as_bytes())
    }

    fn lock_project(&self) -> Result<TutorialProjectGuard, TutorialError> {
        let directory = self.project_root.join(".casa-rs/tutorials");
        fs::create_dir_all(&directory).map_err(|source| TutorialError::Io {
            action: "create tutorial managed root",
            path: directory.clone(),
            source,
        })?;
        let path = directory.join("tutorial.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| TutorialError::Io {
                action: "open tutorial project lock",
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive().map_err(|source| TutorialError::Io {
            action: "lock tutorial project",
            path,
            source,
        })?;
        Ok(TutorialProjectGuard(file))
    }
}

struct TutorialProjectGuard(File);

impl Drop for TutorialProjectGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

fn validate_manifest(root: &Path, manifest: &TutorialManifest) -> Result<(), TutorialError> {
    if manifest.schema_version != TUTORIAL_MANIFEST_SCHEMA_VERSION {
        return Err(TutorialError::UnsupportedManifestVersion {
            version: manifest.schema_version,
        });
    }
    validate_identifier("tutorial", &manifest.tutorial_id)?;
    let mut dataset_ids = BTreeSet::new();
    for dataset in &manifest.datasets {
        validate_identifier("dataset", &dataset.id)?;
        if !dataset_ids.insert(dataset.id.as_str()) {
            return Err(TutorialError::DuplicateIdentifier {
                kind: "dataset",
                id: dataset.id.clone(),
            });
        }
        validate_project_path(&dataset.destination)?;
        let scheme = uri_scheme(&dataset.uri)?;
        if scheme == "file" {
            let _ = file_uri_path(&dataset.uri)?;
        }
        if let Some(digest) = &dataset.sha256
            && (digest.len() != 64 || !digest.bytes().all(|value| value.is_ascii_hexdigit()))
        {
            return Err(TutorialError::InvalidDigest {
                dataset_id: dataset.id.clone(),
            });
        }
        if let Some(unpack) = &dataset.unpack {
            if unpack.max_entries == 0 || unpack.max_expanded_bytes == 0 {
                return Err(TutorialError::InvalidArchiveBounds {
                    dataset_id: dataset.id.clone(),
                });
            }
            if let Some(archive_root) = &unpack.archive_root {
                validate_relative_path(archive_root, false)?;
            }
        }
        let mut check_ids = BTreeSet::new();
        for check in &dataset.checks {
            validate_identifier("check", &check.id)?;
            if !check_ids.insert(check.id.as_str()) {
                return Err(TutorialError::DuplicateIdentifier {
                    kind: "check",
                    id: check.id.clone(),
                });
            }
            validate_relative_path(&check.path, true)?;
        }
    }
    let mut section_ids = BTreeSet::new();
    for section in &manifest.sections {
        validate_identifier("section", &section.id)?;
        if !section_ids.insert(section.id.as_str()) {
            return Err(TutorialError::DuplicateIdentifier {
                kind: "section",
                id: section.id.clone(),
            });
        }
        if let Some(missing) = section
            .dataset_ids
            .iter()
            .find(|id| !dataset_ids.contains(id.as_str()))
        {
            return Err(TutorialError::UnknownSectionDataset {
                section_id: section.id.clone(),
                dataset_id: missing.clone(),
            });
        }
    }
    if let Some(regression) = &manifest.regression {
        validate_relative_path(&regression.path, false)?;
        let path = root.join(&regression.path);
        let resolved = fs::canonicalize(&path).map_err(|source| TutorialError::Io {
            action: "resolve tutorial regression overlay",
            path,
            source,
        })?;
        if !resolved.starts_with(root) {
            return Err(TutorialError::UnsafeTemplatePath { path: resolved });
        }
    }
    Ok(())
}

fn validate_template_cells(
    manifest: &TutorialManifest,
    markdown: &str,
) -> Result<(), TutorialError> {
    let notebook_id = NotebookId::new();
    let source = format!("<!-- casa-rs-notebook:v1 id={notebook_id} -->\n\n{markdown}");
    let document = NotebookDocument::parse(source).map_err(TutorialError::Notebook)?;
    let cells: BTreeSet<CellId> = document.cells().iter().map(|cell| cell.id).collect();
    for section in &manifest.sections {
        if let Some(cell_id) = section.cell_ids.iter().find(|id| !cells.contains(id)) {
            return Err(TutorialError::UnknownSectionCell {
                section_id: section.id.clone(),
                cell_id: *cell_id,
            });
        }
    }
    Ok(())
}

fn validate_identifier(kind: &'static str, value: &str) -> Result<(), TutorialError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(TutorialError::InvalidIdentifier {
            kind,
            id: value.to_owned(),
        });
    }
    Ok(())
}

fn validate_project_path(path: &Path) -> Result<(), TutorialError> {
    validate_relative_path(path, false)?;
    if matches!(
        path.components().next(),
        Some(Component::Normal(value)) if value == ".casa-rs" || value == "notebooks"
    ) {
        return Err(TutorialError::UnsafeProjectPath {
            path: path.to_owned(),
        });
    }
    Ok(())
}

fn validate_relative_path(path: &Path, allow_empty: bool) -> Result<(), TutorialError> {
    if (!allow_empty && path.as_os_str().is_empty())
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(TutorialError::UnsafeProjectPath {
            path: path.to_owned(),
        });
    }
    Ok(())
}

fn tutorial_template_hash(
    manifest_source: &str,
    markdown: &str,
    root: &Path,
) -> Result<String, TutorialError> {
    let mut digest = Sha256::new();
    hash_field(&mut digest, manifest_source.as_bytes());
    hash_field(&mut digest, markdown.as_bytes());
    let assets = root.join("assets");
    if assets.exists() {
        hash_tree(&assets, &assets, &mut digest)?;
    }
    let manifest: TutorialManifest =
        toml::from_str(manifest_source).map_err(|source| TutorialError::Manifest {
            path: root.join(TUTORIAL_MANIFEST),
            detail: source.to_string(),
        })?;
    if let Some(regression) = manifest.regression {
        hash_field(&mut digest, regression.path.as_os_str().as_encoded_bytes());
        let evidence = root.join(&regression.path);
        if evidence.is_dir() {
            hash_tree(&evidence, &evidence, &mut digest)?;
        } else {
            hash_field(
                &mut digest,
                &fs::read(&evidence).map_err(|source| TutorialError::Io {
                    action: "read tutorial regression evidence",
                    path: evidence,
                    source,
                })?,
            );
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn hash_tree(root: &Path, path: &Path, digest: &mut Sha256) -> Result<(), TutorialError> {
    let mut entries = fs::read_dir(path)
        .map_err(|source| TutorialError::Io {
            action: "read tutorial asset directory",
            path: path.to_owned(),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TutorialError::Io {
            action: "read tutorial asset entry",
            path: path.to_owned(),
            source,
        })?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path).map_err(|source| TutorialError::Io {
            action: "inspect tutorial asset",
            path: entry_path.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() {
            return Err(TutorialError::UnsafeTemplatePath { path: entry_path });
        }
        let relative = entry_path
            .strip_prefix(root)
            .expect("tutorial asset is below root");
        hash_field(digest, relative.as_os_str().as_encoded_bytes());
        if metadata.is_dir() {
            hash_tree(root, &entry_path, digest)?;
        } else if metadata.is_file() {
            hash_field(
                digest,
                &fs::read(&entry_path).map_err(|source| TutorialError::Io {
                    action: "read tutorial asset",
                    path: entry_path,
                    source,
                })?,
            );
        } else {
            return Err(TutorialError::UnsafeTemplatePath { path: entry_path });
        }
    }
    Ok(())
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(value);
}

fn strip_notebook_marker(source: &str) -> String {
    source
        .strip_prefix("<!-- casa-rs-notebook:v1 id=")
        .and_then(|remainder| remainder.find("-->\n").map(|end| &remainder[end + 4..]))
        .unwrap_or(source)
        .to_owned()
}

fn resolve_pack_relative(root: &Path, relative: &Path) -> Result<PathBuf, TutorialError> {
    validate_relative_path(relative, false)?;
    let canonical_root = fs::canonicalize(root).map_err(|source| TutorialError::Io {
        action: "resolve tutorial-pack root",
        path: root.to_owned(),
        source,
    })?;
    let candidate = root.join(relative);
    let resolved = fs::canonicalize(&candidate).map_err(|source| TutorialError::Io {
        action: "resolve tutorial-pack path",
        path: candidate,
        source,
    })?;
    if !resolved.starts_with(canonical_root) {
        return Err(TutorialError::UnsafeTemplatePath { path: resolved });
    }
    Ok(resolved)
}

fn json_to_toml(value: &serde_json::Value) -> Option<toml::Value> {
    match value {
        serde_json::Value::Bool(value) => Some(toml::Value::Boolean(*value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(toml::Value::Integer)
            .or_else(|| value.as_f64().map(toml::Value::Float)),
        serde_json::Value::String(value) => Some(toml::Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(json_to_toml)
            .collect::<Option<Vec<_>>>()
            .map(toml::Value::Array),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| json_to_toml(value).map(|value| (key.clone(), value)))
            .collect::<Option<toml::map::Map<_, _>>>()
            .map(toml::Value::Table),
        serde_json::Value::Null => None,
    }
}

fn checksum_policy_sha256(policy: &str) -> Option<String> {
    policy
        .split(|character: char| !character.is_ascii_hexdigit())
        .find(|part| part.len() == 64)
        .map(str::to_ascii_lowercase)
}

fn rewrite_template_asset_links(source: &str, notebook_id: NotebookId) -> String {
    let prefix = format!("assets/{notebook_id}/");
    source
        .replace("](assets/", &format!("]({prefix}"))
        .replace("]: assets/", &format!("]: {prefix}"))
        .replace("src=\"assets/", &format!("src=\"{prefix}"))
}

fn required_disk_bytes(dataset: &TutorialDataset, resolution: &TutorialSourceResolution) -> u64 {
    let download = dataset
        .expected_size_bytes
        .or(resolution.size_bytes)
        .unwrap_or(0);
    download.saturating_add(
        dataset
            .unpack
            .as_ref()
            .map_or(0, |plan| plan.max_expanded_bytes),
    )
}

fn acquisition_plan_hash(plan: &TutorialAcquisitionPlan) -> Result<String, TutorialError> {
    let mut value = plan.clone();
    value.approval_sha256.clear();
    let bytes = serde_json::to_vec(&value)
        .map_err(|source| TutorialError::Serialize(source.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn validate_plan_dataset(
    plan: &TutorialAcquisitionPlan,
    dataset: &TutorialDatasetLock,
) -> Result<(), TutorialError> {
    let expected_sha256 = dataset
        .pinned_sha256
        .clone()
        .or_else(|| dataset.dataset.sha256.clone());
    if plan.destination != dataset.dataset.destination
        || plan.unpack != dataset.dataset.unpack
        || plan.checks != dataset.dataset.checks
        || plan.expected_size_bytes != dataset.dataset.expected_size_bytes
        || plan.expected_sha256 != expected_sha256
        || plan.missing_digest != expected_sha256.is_none()
        || uri_scheme(&plan.requested_uri)? != plan.scheme
    {
        return Err(TutorialError::ApprovalMismatch);
    }
    Ok(())
}

fn dataset_mut<'a>(
    lock: &'a mut TutorialLock,
    dataset_id: &str,
) -> Result<&'a mut TutorialDatasetLock, TutorialError> {
    lock.datasets
        .iter_mut()
        .find(|entry| entry.dataset.id == dataset_id)
        .ok_or_else(|| TutorialError::DatasetNotFound {
            dataset_id: dataset_id.to_owned(),
        })
}

fn current_attempt_mut(
    dataset: &mut TutorialDatasetLock,
) -> Result<&mut TutorialDatasetAttempt, TutorialError> {
    let generation = dataset.current_generation;
    dataset
        .attempts
        .iter_mut()
        .find(|attempt| attempt.generation == generation)
        .ok_or_else(|| TutorialError::NoAttempt {
            dataset_id: dataset.dataset.id.clone(),
        })
}

fn ensure_generation(dataset: &TutorialDatasetLock, generation: u64) -> Result<(), TutorialError> {
    if dataset.current_generation != generation {
        return Err(TutorialError::StaleGeneration {
            dataset_id: dataset.dataset.id.clone(),
            expected: dataset.current_generation,
            actual: generation,
        });
    }
    Ok(())
}

fn apply_attempt_failure(dataset: &mut TutorialDatasetLock, error: &TutorialError) {
    let phase = match error {
        TutorialError::Network { .. }
        | TutorialError::ResumeUnsupported { .. }
        | TutorialError::UnexpectedEndOfSource { .. } => TutorialAcquisitionPhase::NetworkFailed,
        TutorialError::ChecksumMismatch { .. } | TutorialError::SizeMismatch { .. } => {
            TutorialAcquisitionPhase::ChecksumFailed
        }
        TutorialError::UnsafeArchive { .. }
        | TutorialError::ArchiveLimit { .. }
        | TutorialError::ArchiveCollision { .. }
        | TutorialError::MaterializedRootMissing { .. } => TutorialAcquisitionPhase::UnsafeArchive,
        TutorialError::DestinationExists { .. } => TutorialAcquisitionPhase::DestinationCollision,
        _ => dataset.phase,
    };
    dataset.phase = phase;
    dataset.staged = false;
    if let Ok(attempt) = current_attempt_mut(dataset) {
        attempt.phase = phase;
        attempt.error = Some(error.to_string());
        attempt.finished_at = Some(Timestamp::now());
    }
}

fn materialized_source(
    attempt_directory: &Path,
    dataset: &TutorialDataset,
) -> Result<PathBuf, TutorialError> {
    let materialized = attempt_directory.join(MATERIALIZED);
    let source = dataset
        .unpack
        .as_ref()
        .and_then(|plan| plan.archive_root.as_ref())
        .map_or(materialized.clone(), |root| materialized.join(root));
    if !source.exists() {
        return Err(TutorialError::MaterializedRootMissing { path: source });
    }
    Ok(source)
}

fn run_optional_check(
    check: &TutorialOptionalCheck,
    root: &Path,
    skipped: &BTreeSet<String>,
) -> TutorialCheckOutcome {
    if skipped.contains(&check.id) {
        return TutorialCheckOutcome {
            check_id: check.id.clone(),
            status: TutorialCheckStatus::Skipped,
            detail: "Skipped by user approval".to_owned(),
        };
    }
    let path = if check.path.as_os_str().is_empty() {
        root.to_owned()
    } else {
        root.join(&check.path)
    };
    let passed = match check.kind {
        TutorialCheckKind::PathExists => path.exists(),
        TutorialCheckKind::RegularFile => path.is_file(),
        TutorialCheckKind::Directory => path.is_dir(),
        TutorialCheckKind::MeasurementSet => path.is_dir() && path.join("table.dat").is_file(),
    };
    TutorialCheckOutcome {
        check_id: check.id.clone(),
        status: if passed {
            TutorialCheckStatus::Passed
        } else {
            TutorialCheckStatus::Failed
        },
        detail: if passed {
            format!("{} passed", check.label)
        } else {
            format!("{} failed at {}", check.label, path.display())
        },
    }
}

fn safely_extract_archive(
    archive_path: &Path,
    destination: &Path,
    plan: &TutorialUnpackPlan,
) -> Result<(), TutorialError> {
    fs::create_dir_all(destination).map_err(|source| TutorialError::Io {
        action: "create tutorial extraction staging",
        path: destination.to_owned(),
        source,
    })?;
    let file = File::open(archive_path).map_err(|source| TutorialError::Io {
        action: "open tutorial archive",
        path: archive_path.to_owned(),
        source,
    })?;
    let reader: Box<dyn Read> = match plan.format {
        TutorialArchiveFormat::Tar => Box::new(file),
        TutorialArchiveFormat::TarGz => Box::new(GzDecoder::new(file)),
    };
    let mut archive = Archive::new(reader);
    let mut entries = archive
        .entries()
        .map_err(|source| TutorialError::UnsafeArchive {
            detail: source.to_string(),
        })?;
    let mut count = 0_u64;
    let mut expanded = 0_u64;
    let mut paths = BTreeSet::new();
    for entry in &mut entries {
        let mut entry = entry.map_err(|source| TutorialError::UnsafeArchive {
            detail: source.to_string(),
        })?;
        count = count.saturating_add(1);
        if count > plan.max_entries {
            return Err(TutorialError::ArchiveLimit {
                detail: format!("entry count exceeds {}", plan.max_entries),
            });
        }
        let path = entry
            .path()
            .map_err(|source| TutorialError::UnsafeArchive {
                detail: source.to_string(),
            })?
            .into_owned();
        validate_relative_path(&path, false).map_err(|_| TutorialError::UnsafeArchive {
            detail: format!("unsafe archive path {}", path.display()),
        })?;
        if !paths.insert(path.to_string_lossy().to_lowercase()) {
            return Err(TutorialError::ArchiveCollision { path });
        }
        let entry_type = entry.header().entry_type();
        if !(entry_type.is_file() || entry_type.is_dir()) {
            return Err(TutorialError::UnsafeArchive {
                detail: format!("unsupported archive entry type for {}", path.display()),
            });
        }
        expanded = expanded.saturating_add(entry.header().size().unwrap_or(u64::MAX));
        if expanded > plan.max_expanded_bytes {
            return Err(TutorialError::ArchiveLimit {
                detail: format!("expanded bytes exceed {}", plan.max_expanded_bytes),
            });
        }
        let unpacked =
            entry
                .unpack_in(destination)
                .map_err(|source| TutorialError::UnsafeArchive {
                    detail: source.to_string(),
                })?;
        if !unpacked {
            return Err(TutorialError::UnsafeArchive {
                detail: format!("entry escaped extraction root: {}", path.display()),
            });
        }
    }
    if let Some(root) = &plan.archive_root
        && !destination.join(root).exists()
    {
        return Err(TutorialError::MaterializedRootMissing {
            path: destination.join(root),
        });
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), TutorialError> {
    let metadata = fs::symlink_metadata(source).map_err(|source_error| TutorialError::Io {
        action: "inspect tutorial asset source",
        path: source.to_owned(),
        source: source_error,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(TutorialError::UnsafeTemplatePath {
            path: source.to_owned(),
        });
    }
    if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|source| TutorialError::Io {
                action: "create tutorial asset parent",
                path: parent.to_owned(),
                source,
            })?;
        }
        fs::copy(source, destination).map_err(|source_error| TutorialError::Io {
            action: "copy tutorial asset",
            path: destination.to_owned(),
            source: source_error,
        })?;
        return Ok(());
    }
    if !metadata.is_dir() {
        return Err(TutorialError::UnsafeTemplatePath {
            path: source.to_owned(),
        });
    }
    fs::create_dir_all(destination).map_err(|source| TutorialError::Io {
        action: "create tutorial asset directory",
        path: destination.to_owned(),
        source,
    })?;
    for entry in fs::read_dir(source).map_err(|source_error| TutorialError::Io {
        action: "read tutorial asset directory",
        path: source.to_owned(),
        source: source_error,
    })? {
        let entry = entry.map_err(|source_error| TutorialError::Io {
            action: "read tutorial asset entry",
            path: source.to_owned(),
            source: source_error,
        })?;
        copy_tree(&entry.path(), &destination.join(entry.file_name()))?;
    }
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> Result<(), TutorialError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path).map_err(|source| TutorialError::Io {
                action: "remove tutorial staging directory",
                path: path.to_owned(),
                source,
            })
        }
        Ok(_) => fs::remove_file(path).map_err(|source| TutorialError::Io {
            action: "remove tutorial staging file",
            path: path.to_owned(),
            source,
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(TutorialError::Io {
            action: "inspect tutorial staging path",
            path: path.to_owned(),
            source,
        }),
    }
}

fn file_sha256(path: &Path) -> Result<String, TutorialError> {
    let mut file = File::open(path).map_err(|source| TutorialError::Io {
        action: "open tutorial source for SHA-256",
        path: path.to_owned(),
        source,
    })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| TutorialError::Io {
            action: "read tutorial source for SHA-256",
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<(), TutorialError> {
    let parent = path
        .parent()
        .ok_or_else(|| TutorialError::UnsafeProjectPath {
            path: path.to_owned(),
        })?;
    fs::create_dir_all(parent).map_err(|source| TutorialError::Io {
        action: "create tutorial managed parent",
        path: parent.to_owned(),
        source,
    })?;
    let mut temporary = NamedTempFile::new_in(parent).map_err(|source| TutorialError::Io {
        action: "create tutorial temporary file",
        path: parent.to_owned(),
        source,
    })?;
    temporary
        .write_all(contents)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| TutorialError::Io {
            action: "write tutorial temporary file",
            path: temporary.path().to_owned(),
            source,
        })?;
    temporary.persist(path).map_err(|error| TutorialError::Io {
        action: "publish tutorial managed file",
        path: path.to_owned(),
        source: error.error,
    })?;
    Ok(())
}

fn uri_scheme(uri: &str) -> Result<String, TutorialError> {
    let (scheme, _) = uri
        .split_once(':')
        .ok_or_else(|| TutorialError::InvalidUri {
            uri: uri.to_owned(),
        })?;
    if scheme.is_empty()
        || !scheme.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphabetic()
                || (index > 0 && (byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.')))
        })
    {
        return Err(TutorialError::InvalidUri {
            uri: uri.to_owned(),
        });
    }
    Ok(scheme.to_ascii_lowercase())
}

fn file_uri_path(uri: &str) -> Result<PathBuf, TutorialError> {
    let scheme = uri_scheme(uri)?;
    if scheme != "file" {
        return Err(TutorialError::InvalidUri {
            uri: uri.to_owned(),
        });
    }
    let remainder = uri
        .strip_prefix("file://")
        .ok_or_else(|| TutorialError::InvalidUri {
            uri: uri.to_owned(),
        })?;
    let path = if let Some(path) = remainder.strip_prefix("localhost/") {
        format!("/{path}")
    } else if remainder.starts_with('/') {
        percent_decode(remainder)?
    } else {
        return Err(TutorialError::InvalidUri {
            uri: uri.to_owned(),
        });
    };
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        return Err(TutorialError::InvalidUri {
            uri: uri.to_owned(),
        });
    }
    Ok(path)
}

fn percent_decode(value: &str) -> Result<String, TutorialError> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(TutorialError::InvalidUri {
                    uri: value.to_owned(),
                });
            }
            let high = hex_value(bytes[index + 1]).ok_or_else(|| TutorialError::InvalidUri {
                uri: value.to_owned(),
            })?;
            let low = hex_value(bytes[index + 2]).ok_or_else(|| TutorialError::InvalidUri {
                uri: value.to_owned(),
            })?;
            output.push((high << 4) | low);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|_| TutorialError::InvalidUri {
        uri: value.to_owned(),
    })
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

/// Tutorial-template, approval, integrity, and materialization failures.
#[derive(Debug, Error)]
pub enum TutorialError {
    #[error("tutorial project store failed: {0}")]
    Store(#[source] StoreError),
    #[error("tutorial notebook Markdown is invalid: {0}")]
    Notebook(#[source] crate::NotebookParseError),
    #[error("I/O error while attempting to {action} at {path}: {source}", path = .path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("tutorial template is not a directory: {path}", path = .path.display())]
    TemplateNotDirectory { path: PathBuf },
    #[error("invalid tutorial manifest {path}: {detail}", path = .path.display())]
    Manifest { path: PathBuf, detail: String },
    #[error("invalid tutorial lock {path}: {detail}", path = .path.display())]
    Lock { path: PathBuf, detail: String },
    #[error("unsupported tutorial manifest schema version {version}")]
    UnsupportedManifestVersion { version: u32 },
    #[error("unsupported tutorial lock schema version {version}")]
    UnsupportedLockVersion { version: u32 },
    #[error("unsupported legacy tutorial-pack schema {version}")]
    UnsupportedV0Schema { version: String },
    #[error("tutorial migration destination already exists: {path}", path = .path.display())]
    MigrationDestinationExists { path: PathBuf },
    #[error("legacy tutorial parameter {parameter} in section {section_id} cannot be represented")]
    V0Parameter {
        section_id: String,
        parameter: String,
    },
    #[error("invalid {kind} identifier {id:?}")]
    InvalidIdentifier { kind: &'static str, id: String },
    #[error("duplicate {kind} identifier {id:?}")]
    DuplicateIdentifier { kind: &'static str, id: String },
    #[error("section {section_id} references unknown dataset {dataset_id}")]
    UnknownSectionDataset {
        section_id: String,
        dataset_id: String,
    },
    #[error("section {section_id} references unknown notebook cell {cell_id}")]
    UnknownSectionCell { section_id: String, cell_id: CellId },
    #[error("unsafe project-relative path {path}", path = .path.display())]
    UnsafeProjectPath { path: PathBuf },
    #[error("unsafe tutorial template path {path}", path = .path.display())]
    UnsafeTemplatePath { path: PathBuf },
    #[error("dataset {dataset_id} has an invalid SHA-256")]
    InvalidDigest { dataset_id: String },
    #[error("dataset {dataset_id} has zero archive safety bounds")]
    InvalidArchiveBounds { dataset_id: String },
    #[error("invalid tutorial URI {uri:?}")]
    InvalidUri { uri: String },
    #[error("no tutorial URI handler is registered for scheme {scheme:?}")]
    UnknownScheme { scheme: String },
    #[error("file tutorial source is not a regular file: {path}", path = .path.display())]
    SourceNotRegularFile { path: PathBuf },
    #[error("network access failed for {uri}: {detail}")]
    Network { uri: String, detail: String },
    #[error("source does not support resumable range reads: {uri}")]
    ResumeUnsupported { uri: String },
    #[error("source ended unexpectedly: {uri}")]
    UnexpectedEndOfSource { uri: String },
    #[error("tutorial dataset {dataset_id} was not found")]
    DatasetNotFound { dataset_id: String },
    #[error("tutorial optional check {check_id} was not found")]
    UnknownCheck { check_id: String },
    #[error("acquisition approval does not match the exact current plan")]
    ApprovalMismatch,
    #[error("a missing dataset digest requires explicit risk approval")]
    MissingDigestApprovalRequired,
    #[error("insufficient disk: {required} bytes required, {available} bytes available")]
    InsufficientDisk { required: u64, available: u64 },
    #[error("tutorial dataset {dataset_id} has no acquisition attempt")]
    NoAttempt { dataset_id: String },
    #[error("stale tutorial generation for {dataset_id}: expected {expected}, got {actual}")]
    StaleGeneration {
        dataset_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("cannot {action} tutorial dataset {dataset_id} from phase {phase:?}")]
    InvalidTransition {
        dataset_id: String,
        phase: TutorialAcquisitionPhase,
        action: &'static str,
    },
    #[error("download size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("SHA-256 mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("dataset declares no unpack plan")]
    MissingUnpackPlan,
    #[error("unsafe archive: {detail}")]
    UnsafeArchive { detail: String },
    #[error("archive safety limit exceeded: {detail}")]
    ArchiveLimit { detail: String },
    #[error("archive contains a duplicate destination {path}", path = .path.display())]
    ArchiveCollision { path: PathBuf },
    #[error("expected materialized archive root is missing: {path}", path = .path.display())]
    MaterializedRootMissing { path: PathBuf },
    #[error("tutorial dataset destination already exists: {path}", path = .path.display())]
    DestinationExists { path: PathBuf },
    #[error("tutorial fork conflicted with an external notebook edit")]
    ForkConflict,
    #[error("serialize tutorial state: {0}")]
    Serialize(String),
}
