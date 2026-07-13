// SPDX-License-Identifier: LGPL-3.0-or-later

//! Agent-neutral scientific-assistant transcript and session-profile contracts.
//!
//! Provider protocols and credentials deliberately stay outside this persisted
//! boundary. A backend adapter may resume its own session using the opaque
//! binding stored here, while CASA-RS retains the durable scientific record.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::{AssistantMessageId, AssistantPinId, ConversationId, NotebookId, Timestamp};

pub const ASSISTANT_TRANSCRIPT_SCHEMA_VERSION: u32 = 2;
pub const ASSISTANT_PROFILE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantMessageRole {
    User,
    Assistant,
    Activity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantCitationKind {
    Document,
    Source,
    Run,
    Web,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantCitation {
    pub id: String,
    pub kind: AssistantCitationKind,
    pub label: String,
    pub locator: String,
    pub excerpt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_end: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantContextKind {
    Notebook,
    Tutorial,
    Task,
    Explorer,
    Python,
    Plot,
    History,
    Assistant,
    Corpus,
    Source,
    DataSemantics,
    ToolResult,
    Web,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantContextItem {
    pub id: String,
    pub kind: AssistantContextKind,
    pub label: String,
    pub summary: String,
    pub excerpt: String,
    pub byte_count: u64,
    pub content_sha256: String,
    pub untrusted_evidence: bool,
}

impl AssistantContextItem {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        kind: AssistantContextKind,
        label: impl Into<String>,
        summary: impl Into<String>,
        excerpt: impl Into<String>,
        untrusted_evidence: bool,
    ) -> Self {
        let excerpt = excerpt.into();
        Self {
            id: id.into(),
            kind,
            label: label.into(),
            summary: summary.into(),
            byte_count: excerpt.len() as u64,
            content_sha256: sha256(excerpt.as_bytes()),
            excerpt,
            untrusted_evidence,
        }
    }

    #[must_use]
    pub fn has_valid_hash(&self) -> bool {
        self.byte_count == self.excerpt.len() as u64
            && self.content_sha256 == sha256(self.excerpt.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantAttachment {
    pub kind: AssistantContextKind,
    pub identifier: String,
    pub label: String,
    pub primary: bool,
}

/// User-visible authority presets map onto backend-native permission controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantAuthorityPreset {
    /// Project context and corpus access; no project mutations.
    Explore,
    /// Project-scoped work with backend-native approval prompts.
    Work,
    /// Full backend authority, selected explicitly by the user.
    FullAccess,
}

/// Informational identity of the user-selected scientific Python.
///
/// This is provenance, not an executable pin or an authority receipt. Changing
/// it does not invalidate earlier conversations, notebook cells, or receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantPythonProvenance {
    pub selected_command: String,
    pub resolved_path: PathBuf,
    pub implementation: String,
    pub version: String,
    pub environment_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub casa_rs_version: Option<String>,
    #[serde(default)]
    pub packages: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantSessionProfile {
    pub profile_version: u32,
    pub backend_id: String,
    pub authority: AssistantAuthorityPreset,
    pub model: String,
    pub effort: String,
    /// Executable name or user-selected path; intentionally not hash-pinned.
    pub agent_command: String,
    /// Executable name or user-selected path used for ad-hoc Python work.
    pub python_command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python_provenance: Option<AssistantPythonProvenance>,
}

impl AssistantSessionProfile {
    #[must_use]
    pub fn codex_default() -> Self {
        Self {
            profile_version: ASSISTANT_PROFILE_VERSION,
            backend_id: "codex_app_server".to_owned(),
            authority: AssistantAuthorityPreset::Work,
            model: String::new(),
            effort: "medium".to_owned(),
            agent_command: "codex".to_owned(),
            python_command: "python3".to_owned(),
            python_provenance: None,
        }
    }

    fn validate(&self) -> Result<(), AssistantError> {
        if self.profile_version != ASSISTANT_PROFILE_VERSION {
            return Err(AssistantError::UnsupportedProfileVersion {
                actual: self.profile_version,
            });
        }
        if self.backend_id.trim().is_empty()
            || self.agent_command.trim().is_empty()
            || self.python_command.trim().is_empty()
        {
            return Err(AssistantError::InvalidProfile);
        }
        if self.python_provenance.as_ref().is_some_and(|provenance| {
            provenance.selected_command.trim().is_empty()
                || provenance.resolved_path.as_os_str().is_empty()
                || provenance.implementation.trim().is_empty()
                || provenance.version.trim().is_empty()
                || provenance.environment_label.trim().is_empty()
        }) {
            return Err(AssistantError::InvalidProfile);
        }
        Ok(())
    }
}

/// Opaque backend resume data. It is not a provider transcript or credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantBackendSession {
    pub backend_id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantActivityState {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantActivity {
    pub id: String,
    pub label: String,
    pub state: AssistantActivityState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// A typed, non-mutating recommendation that the GUI may open in the
/// canonical task surface. Applying it is always an explicit user action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantTaskSuggestion {
    pub id: String,
    pub task_id: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantPinReference {
    pub id: AssistantPinId,
    pub conversation_id: ConversationId,
    pub notebook_id: NotebookId,
    pub message_id: AssistantMessageId,
    pub representation: String,
    pub destination: String,
    pub snapshot_content: String,
    pub created_at: Timestamp,
    pub content_sha256: String,
}

impl AssistantPinReference {
    #[must_use]
    pub fn new(
        conversation_id: ConversationId,
        notebook_id: NotebookId,
        message_id: AssistantMessageId,
        representation: impl Into<String>,
        snapshot_content: impl Into<String>,
    ) -> Self {
        let snapshot_content = snapshot_content.into();
        Self {
            id: AssistantPinId::new(),
            conversation_id,
            notebook_id,
            message_id,
            representation: representation.into(),
            destination: "chronological_tail".to_owned(),
            content_sha256: sha256(snapshot_content.as_bytes()),
            snapshot_content,
            created_at: Timestamp::now(),
        }
    }

    #[must_use]
    pub fn has_valid_snapshot(&self) -> bool {
        self.destination == "chronological_tail"
            && self.content_sha256 == sha256(self.snapshot_content.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantMessage {
    pub id: AssistantMessageId,
    pub role: AssistantMessageRole,
    pub content: String,
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub citations: Vec<AssistantCitation>,
    #[serde(default)]
    pub used_context: Vec<AssistantContextItem>,
    #[serde(default)]
    pub activities: Vec<AssistantActivity>,
    #[serde(default)]
    pub task_suggestions: Vec<AssistantTaskSuggestion>,
    #[serde(default)]
    pub pins: Vec<AssistantPinReference>,
}

impl AssistantMessage {
    #[must_use]
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            id: AssistantMessageId::new(),
            role: AssistantMessageRole::User,
            content: content.into(),
            created_at: Timestamp::now(),
            agent_id: None,
            model: None,
            citations: Vec::new(),
            used_context: Vec::new(),
            activities: Vec::new(),
            task_suggestions: Vec::new(),
            pins: Vec::new(),
        }
    }

    #[must_use]
    pub fn assistant(
        content: impl Into<String>,
        agent_id: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            role: AssistantMessageRole::Assistant,
            agent_id: Some(agent_id.into()),
            model: Some(model.into()),
            ..Self::user(content)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationTranscript {
    pub schema_version: u32,
    pub id: ConversationId,
    pub title: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub profile: AssistantSessionProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_session: Option<AssistantBackendSession>,
    pub attachments: Vec<AssistantAttachment>,
    pub messages: Vec<AssistantMessage>,
    pub draft: String,
    #[serde(default)]
    pub selected_context_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scroll_anchor_message_id: Option<AssistantMessageId>,
}

impl ConversationTranscript {
    fn validate(&self) -> Result<(), AssistantError> {
        if self.schema_version != ASSISTANT_TRANSCRIPT_SCHEMA_VERSION {
            return Err(AssistantError::UnsupportedTranscriptVersion {
                actual: self.schema_version,
            });
        }
        self.profile.validate()?;
        if self
            .attachments
            .iter()
            .filter(|attachment| attachment.primary)
            .count()
            != 1
        {
            return Err(AssistantError::PrimaryAttachmentCount);
        }
        if self.backend_session.as_ref().is_some_and(|binding| {
            binding.backend_id != self.profile.backend_id || binding.session_id.trim().is_empty()
        }) {
            return Err(AssistantError::InvalidBackendSession);
        }
        for message in &self.messages {
            if message
                .used_context
                .iter()
                .any(|item| !item.has_valid_hash())
            {
                return Err(AssistantError::InvalidContextHash);
            }
            if message.pins.iter().any(|pin| !pin.has_valid_snapshot()) {
                return Err(AssistantError::InvalidPinSnapshot);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AssistantStore {
    project_root: PathBuf,
}

impl AssistantStore {
    pub fn open(project_root: impl AsRef<Path>) -> Result<Self, AssistantError> {
        let project_root = project_root.as_ref();
        if !project_root.is_absolute() {
            return Err(AssistantError::ProjectRootMustBeAbsolute {
                path: project_root.to_owned(),
            });
        }
        let metadata = fs::metadata(project_root).map_err(|source| AssistantError::Io {
            action: "inspect project root",
            path: project_root.to_owned(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(AssistantError::ProjectRootNotDirectory {
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

    pub fn create_conversation(
        &self,
        title: impl Into<String>,
        primary_attachment: AssistantAttachment,
        profile: AssistantSessionProfile,
    ) -> Result<ConversationTranscript, AssistantError> {
        profile.validate()?;
        let _lock = self.lock()?;
        let now = Timestamp::now();
        let transcript = ConversationTranscript {
            schema_version: ASSISTANT_TRANSCRIPT_SCHEMA_VERSION,
            id: ConversationId::new(),
            title: title.into(),
            created_at: now,
            updated_at: now,
            profile,
            backend_session: None,
            attachments: vec![AssistantAttachment {
                primary: true,
                ..primary_attachment
            }],
            messages: Vec::new(),
            draft: String::new(),
            selected_context_ids: Vec::new(),
            scroll_anchor_message_id: None,
        };
        transcript.validate()?;
        self.write_transcript(&transcript)?;
        Ok(transcript)
    }

    pub fn save_conversation(
        &self,
        transcript: &ConversationTranscript,
    ) -> Result<(), AssistantError> {
        transcript.validate()?;
        let _lock = self.lock()?;
        self.write_transcript(transcript)
    }

    pub fn load_conversation(
        &self,
        id: ConversationId,
    ) -> Result<ConversationTranscript, AssistantError> {
        let path = self.conversations_dir().join(format!("{id}.json"));
        let bytes = fs::read(&path).map_err(|source| AssistantError::Io {
            action: "read assistant transcript",
            path: path.clone(),
            source,
        })?;
        let transcript: ConversationTranscript =
            serde_json::from_slice(&bytes).map_err(AssistantError::Deserialize)?;
        if transcript.id != id {
            return Err(AssistantError::ConversationIdentityMismatch {
                expected: id,
                actual: transcript.id,
            });
        }
        transcript.validate()?;
        Ok(transcript)
    }

    pub fn list_conversations(&self) -> Result<Vec<ConversationTranscript>, AssistantError> {
        let directory = match fs::read_dir(self.conversations_dir()) {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(AssistantError::Io {
                    action: "list assistant transcripts",
                    path: self.conversations_dir(),
                    source,
                });
            }
        };
        let mut transcripts = Vec::new();
        for entry in directory {
            let path = entry
                .map_err(|source| AssistantError::Io {
                    action: "read assistant transcript entry",
                    path: self.conversations_dir(),
                    source,
                })?
                .path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&path).map_err(|source| AssistantError::Io {
                action: "read assistant transcript",
                path: path.clone(),
                source,
            })?;
            let transcript: ConversationTranscript =
                serde_json::from_slice(&bytes).map_err(AssistantError::Deserialize)?;
            transcript.validate()?;
            transcripts.push(transcript);
        }
        transcripts.sort_by_key(|transcript| transcript.updated_at);
        Ok(transcripts)
    }

    fn write_transcript(&self, transcript: &ConversationTranscript) -> Result<(), AssistantError> {
        let directory = self.conversations_dir();
        fs::create_dir_all(&directory).map_err(|source| AssistantError::Io {
            action: "create assistant transcript directory",
            path: directory.clone(),
            source,
        })?;
        let path = directory.join(format!("{}.json", transcript.id));
        let bytes = serde_json::to_vec_pretty(transcript).map_err(AssistantError::Serialize)?;
        atomic_write(&path, &bytes)
    }

    fn lock(&self) -> Result<File, AssistantError> {
        let managed = self.project_root.join(".casa-rs");
        fs::create_dir_all(&managed).map_err(|source| AssistantError::Io {
            action: "create managed project directory",
            path: managed.clone(),
            source,
        })?;
        let path = managed.join("assistant.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| AssistantError::Io {
                action: "open assistant project lock",
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive().map_err(|source| AssistantError::Io {
            action: "lock assistant project",
            path,
            source,
        })?;
        Ok(file)
    }

    fn conversations_dir(&self) -> PathBuf {
        self.project_root.join(".casa-rs/conversations")
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), AssistantError> {
    let parent = path.parent().ok_or_else(|| AssistantError::MissingParent {
        path: path.to_owned(),
    })?;
    let mut temporary = NamedTempFile::new_in(parent).map_err(|source| AssistantError::Io {
        action: "create assistant transcript temporary file",
        path: parent.to_owned(),
        source,
    })?;
    temporary
        .write_all(bytes)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| AssistantError::Io {
            action: "write assistant transcript temporary file",
            path: temporary.path().to_owned(),
            source,
        })?;
    temporary
        .persist(path)
        .map_err(|error| AssistantError::Io {
            action: "persist assistant transcript",
            path: path.to_owned(),
            source: error.error,
        })?;
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Debug, Error)]
pub enum AssistantError {
    #[error("assistant project root must be absolute: {path}")]
    ProjectRootMustBeAbsolute { path: PathBuf },
    #[error("assistant project root is not a directory: {path}")]
    ProjectRootNotDirectory { path: PathBuf },
    #[error("unsupported assistant transcript schema version {actual}")]
    UnsupportedTranscriptVersion { actual: u32 },
    #[error("unsupported assistant profile version {actual}")]
    UnsupportedProfileVersion { actual: u32 },
    #[error("assistant session profile is incomplete")]
    InvalidProfile,
    #[error("assistant backend session does not match its profile")]
    InvalidBackendSession,
    #[error("assistant conversation must have exactly one primary attachment")]
    PrimaryAttachmentCount,
    #[error("assistant context snapshot hash does not match its content")]
    InvalidContextHash,
    #[error("assistant notebook pin is not an immutable chronological-tail snapshot")]
    InvalidPinSnapshot,
    #[error("assistant conversation identity mismatch: expected {expected}, got {actual}")]
    ConversationIdentityMismatch {
        expected: ConversationId,
        actual: ConversationId,
    },
    #[error("assistant path has no parent: {path}")]
    MissingParent { path: PathBuf },
    #[error("failed to {action} at {path}: {source}")]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize assistant transcript: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to deserialize assistant transcript: {0}")]
    Deserialize(serde_json::Error),
}
