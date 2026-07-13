// SPDX-License-Identifier: LGPL-3.0-or-later

//! Provider-neutral assistant transcript, proposal, and sidecar protocol contracts.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::{
    AssistantMessageId, AssistantPinId, AssistantProposalId, ConversationId, NotebookId, Timestamp,
};

pub const ASSISTANT_TRANSCRIPT_SCHEMA_VERSION: u32 = 1;
pub const ASSISTANT_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantMessageRole {
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    Task,
    Explorer,
    Python,
    Plot,
    History,
    Corpus,
    Source,
    DataSemantics,
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
    pub provider_visible: bool,
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
        provider_visible: bool,
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
            provider_visible,
            untrusted_evidence,
        }
    }

    #[must_use]
    pub fn has_valid_hash(&self) -> bool {
        self.content_sha256 == sha256(self.excerpt.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantEgressManifest {
    pub provider: String,
    pub model: String,
    pub destination: String,
    pub items: Vec<AssistantContextItem>,
    pub estimated_bytes: u64,
}

impl AssistantEgressManifest {
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        destination: impl Into<String>,
        items: Vec<AssistantContextItem>,
    ) -> Self {
        let estimated_bytes = items
            .iter()
            .filter(|item| item.provider_visible)
            .map(|item| item.byte_count)
            .sum();
        Self {
            provider: provider.into(),
            model: model.into(),
            destination: destination.into(),
            items,
            estimated_bytes,
        }
    }

    #[must_use]
    pub fn validation_error(&self) -> Option<String> {
        if self.provider.trim().is_empty() || self.model.trim().is_empty() {
            return Some("provider and model must be visible in every egress manifest".to_owned());
        }
        if self.items.iter().any(|item| !item.has_valid_hash()) {
            return Some("egress item content hash does not match its exact excerpt".to_owned());
        }
        let expected: u64 = self
            .items
            .iter()
            .filter(|item| item.provider_visible)
            .map(|item| item.byte_count)
            .sum();
        (expected != self.estimated_bytes)
            .then(|| "egress byte estimate does not match visible context".to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantAttachment {
    pub kind: AssistantContextKind,
    pub identifier: String,
    pub label: String,
    pub primary: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(PartialOrd, Ord)]
pub enum AssistantProposalKind {
    Task,
    Python,
    Plot,
    Download,
    Note,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantExecutableIdentity {
    pub path: PathBuf,
    pub version: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistantExecutionBinding {
    pub operation_type: String,
    pub canonical_parameters: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_source: Option<String>,
    #[serde(default)]
    pub input_paths: Vec<PathBuf>,
    #[serde(default)]
    pub output_paths: Vec<PathBuf>,
    pub working_directory: PathBuf,
    pub executable: AssistantExecutableIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantAuthorityPolicy {
    pub layer: String,
    #[serde(default)]
    pub allowed_read_tools: BTreeSet<String>,
    #[serde(default)]
    pub allowed_mutations: BTreeSet<AssistantProposalKind>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssistantEffectivePolicy {
    layers: Vec<AssistantAuthorityPolicy>,
}

impl AssistantEffectivePolicy {
    #[must_use]
    pub fn intersect(layers: Vec<AssistantAuthorityPolicy>) -> Self {
        Self { layers }
    }

    #[must_use]
    pub fn permits_read_tool(&self, name: &str) -> bool {
        !self.layers.is_empty()
            && self
                .layers
                .iter()
                .all(|layer| layer.allowed_read_tools.contains(name))
    }

    #[must_use]
    pub fn permits_mutation(&self, kind: AssistantProposalKind) -> bool {
        !self.layers.is_empty()
            && self
                .layers
                .iter()
                .all(|layer| layer.allowed_mutations.contains(&kind))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantCredentialLease {
    pub provider: String,
    pub credential_type: String,
    pub secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantSidecarPolicy {
    pub provider_network_only: bool,
    pub project_filesystem: bool,
    pub shell: bool,
    pub python: bool,
    pub direct_host_tools: bool,
}

impl AssistantSidecarPolicy {
    #[must_use]
    pub const fn deny_by_default() -> Self {
        Self {
            provider_network_only: true,
            project_filesystem: false,
            shell: false,
            python: false,
            direct_host_tools: false,
        }
    }

    #[must_use]
    pub const fn is_constrained(&self) -> bool {
        self.provider_network_only
            && !self.project_filesystem
            && !self.shell
            && !self.python
            && !self.direct_host_tools
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantProposalState {
    Pending,
    Approved,
    Running,
    Succeeded,
    Failed,
    Rejected,
    Cancelled,
    Invalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantApproval {
    pub proposal_sha256: String,
    pub approved_at: Timestamp,
    pub authority: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantProposalDestination {
    pub surface: String,
    pub identifier: String,
    pub position: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantInsertionBinding {
    pub destination: AssistantProposalDestination,
    pub exact_content: String,
    pub content_sha256: String,
}

impl AssistantInsertionBinding {
    #[must_use]
    pub fn new(
        destination: AssistantProposalDestination,
        exact_content: impl Into<String>,
    ) -> Self {
        let exact_content = exact_content.into();
        let content_sha256 = sha256(exact_content.as_bytes());
        Self {
            destination,
            exact_content,
            content_sha256,
        }
    }

    #[must_use]
    pub fn has_valid_hash(&self) -> bool {
        self.content_sha256 == sha256(self.exact_content.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistantProposal {
    pub id: AssistantProposalId,
    pub kind: AssistantProposalKind,
    pub title: String,
    pub authority: String,
    pub payload: Value,
    pub execution: AssistantExecutionBinding,
    pub insertion: AssistantInsertionBinding,
    pub payload_sha256: String,
    pub state: AssistantProposalState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<AssistantApproval>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insertion_approval: Option<AssistantApproval>,
    #[serde(default)]
    pub affected_paths: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
}

impl AssistantProposal {
    pub fn new(
        kind: AssistantProposalKind,
        title: impl Into<String>,
        authority: impl Into<String>,
        payload: Value,
        execution: AssistantExecutionBinding,
        affected_paths: Vec<PathBuf>,
    ) -> Result<Self, AssistantError> {
        let exact_content =
            serde_json::to_string_pretty(&payload).map_err(AssistantError::Serialize)?;
        Self::new_with_insertion(
            kind,
            title,
            authority,
            payload,
            execution,
            AssistantInsertionBinding::new(
                AssistantProposalDestination {
                    surface: "conversation".to_owned(),
                    identifier: "pending".to_owned(),
                    position: "not_inserted".to_owned(),
                },
                exact_content,
            ),
            affected_paths,
        )
    }

    pub fn new_with_insertion(
        kind: AssistantProposalKind,
        title: impl Into<String>,
        authority: impl Into<String>,
        payload: Value,
        execution: AssistantExecutionBinding,
        insertion: AssistantInsertionBinding,
        affected_paths: Vec<PathBuf>,
    ) -> Result<Self, AssistantError> {
        if !insertion.has_valid_hash() {
            return Err(AssistantError::InsertionHashMismatch);
        }
        let mut proposal = Self {
            id: AssistantProposalId::new(),
            kind,
            title: title.into(),
            authority: authority.into(),
            payload,
            execution,
            insertion,
            payload_sha256: String::new(),
            state: AssistantProposalState::Pending,
            approval: None,
            insertion_approval: None,
            affected_paths,
            result: None,
        };
        proposal.payload_sha256 = proposal.computed_hash()?;
        Ok(proposal)
    }

    pub fn approve_insertion(
        &mut self,
        authority: impl Into<String>,
    ) -> Result<(), AssistantError> {
        self.ensure_hash()?;
        if !self.insertion.has_valid_hash() {
            return Err(AssistantError::InsertionHashMismatch);
        }
        self.insertion_approval = Some(AssistantApproval {
            proposal_sha256: self.payload_sha256.clone(),
            approved_at: Timestamp::now(),
            authority: authority.into(),
        });
        Ok(())
    }

    pub fn ensure_insertion_approved_exact(&mut self) -> Result<(), AssistantError> {
        let computed = self.computed_hash()?;
        let approved = self
            .insertion_approval
            .as_ref()
            .is_some_and(|approval| approval.proposal_sha256 == computed);
        if self.payload_sha256 != computed || !self.insertion.has_valid_hash() || !approved {
            self.state = AssistantProposalState::Invalidated;
            self.insertion_approval = None;
            self.approval = None;
            return Err(AssistantError::InsertionApprovalInvalidated {
                proposal_id: self.id,
            });
        }
        Ok(())
    }

    pub fn approve(&mut self, authority: impl Into<String>) -> Result<(), AssistantError> {
        self.ensure_hash()?;
        if self.state != AssistantProposalState::Pending {
            return Err(AssistantError::ProposalState {
                expected: AssistantProposalState::Pending,
                actual: self.state,
            });
        }
        self.state = AssistantProposalState::Approved;
        self.approval = Some(AssistantApproval {
            proposal_sha256: self.payload_sha256.clone(),
            approved_at: Timestamp::now(),
            authority: authority.into(),
        });
        Ok(())
    }

    pub fn reject(&mut self) -> Result<(), AssistantError> {
        if !matches!(
            self.state,
            AssistantProposalState::Pending | AssistantProposalState::Approved
        ) {
            return Err(AssistantError::ProposalState {
                expected: AssistantProposalState::Pending,
                actual: self.state,
            });
        }
        self.state = AssistantProposalState::Rejected;
        self.approval = None;
        self.insertion_approval = None;
        Ok(())
    }

    pub fn ensure_approved_exact(&mut self) -> Result<(), AssistantError> {
        let computed = self.computed_hash()?;
        let approved = self
            .approval
            .as_ref()
            .is_some_and(|approval| approval.proposal_sha256 == computed);
        if self.payload_sha256 != computed || !approved {
            self.state = AssistantProposalState::Invalidated;
            self.approval = None;
            self.insertion_approval = None;
            return Err(AssistantError::ApprovalInvalidated {
                proposal_id: self.id,
            });
        }
        if self.state != AssistantProposalState::Approved {
            return Err(AssistantError::ProposalNotApproved {
                proposal_id: self.id,
            });
        }
        Ok(())
    }

    fn ensure_hash(&self) -> Result<(), AssistantError> {
        let computed = self.computed_hash()?;
        if computed == self.payload_sha256 {
            Ok(())
        } else {
            Err(AssistantError::ProposalHashMismatch {
                proposal_id: self.id,
            })
        }
    }

    fn computed_hash(&self) -> Result<String, AssistantError> {
        let canonical = serde_json::to_vec(&(
            self.kind,
            self.title.as_str(),
            self.authority.as_str(),
            &self.payload,
            &self.execution,
            &self.insertion,
            &self.affected_paths,
        ))
        .map_err(AssistantError::Serialize)?;
        Ok(sha256(&canonical))
    }
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
        destination: impl Into<String>,
        snapshot_content: impl Into<String>,
    ) -> Self {
        let snapshot_content = snapshot_content.into();
        Self {
            id: AssistantPinId::new(),
            conversation_id,
            notebook_id,
            message_id,
            representation: representation.into(),
            destination: destination.into(),
            content_sha256: sha256(snapshot_content.as_bytes()),
            snapshot_content,
            created_at: Timestamp::now(),
        }
    }

    #[must_use]
    pub fn has_valid_snapshot(&self) -> bool {
        self.content_sha256 == sha256(self.snapshot_content.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistantMessage {
    pub id: AssistantMessageId,
    pub role: AssistantMessageRole,
    pub content: String,
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub citations: Vec<AssistantCitation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<AssistantEgressManifest>,
    #[serde(default)]
    pub proposals: Vec<AssistantProposal>,
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
            provider: None,
            model: None,
            citations: Vec::new(),
            egress: None,
            proposals: Vec::new(),
            pins: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationTranscript {
    pub schema_version: u32,
    pub id: ConversationId,
    pub title: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub provider: String,
    pub model: String,
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
        if self
            .attachments
            .iter()
            .filter(|attachment| attachment.primary)
            .count()
            != 1
        {
            return Err(AssistantError::PrimaryAttachmentCount);
        }
        for message in &self.messages {
            if let Some(egress) = &message.egress
                && let Some(message) = egress.validation_error()
            {
                return Err(AssistantError::InvalidEgress { message });
            }
            for proposal in &message.proposals {
                proposal.ensure_hash()?;
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
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<ConversationTranscript, AssistantError> {
        let _lock = self.lock()?;
        let now = Timestamp::now();
        let transcript = ConversationTranscript {
            schema_version: ASSISTANT_TRANSCRIPT_SCHEMA_VERSION,
            id: ConversationId::new(),
            title: title.into(),
            created_at: now,
            updated_at: now,
            provider: provider.into(),
            model: model.into(),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantProviderModel {
    pub id: String,
    pub label: String,
    pub context_window: u64,
    pub supports_images: bool,
    pub supports_tools: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantProviderOption {
    pub id: String,
    pub label: String,
    pub authentication: String,
    pub configured: bool,
    pub models: Vec<AssistantProviderModel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantProviderCatalog {
    pub protocol_version: u32,
    pub providers: Vec<AssistantProviderOption>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistantToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum AssistantProtocolRequest {
    Hello {
        request_id: String,
        protocol_version: u32,
        policy: AssistantSidecarPolicy,
    },
    Catalog {
        request_id: String,
    },
    Authenticate {
        request_id: String,
        provider: String,
    },
    AuthenticationResponse {
        request_id: String,
        prompt_id: String,
        value: String,
    },
    Turn {
        request_id: String,
        conversation_id: ConversationId,
        provider: String,
        model: String,
        messages: Vec<AssistantMessage>,
        egress: Box<AssistantEgressManifest>,
        tools: Vec<AssistantToolDefinition>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        credential: Option<AssistantCredentialLease>,
    },
    ToolResult {
        request_id: String,
        call_id: String,
        result: Value,
        is_error: bool,
    },
    Cancel {
        request_id: String,
    },
    Shutdown {
        request_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantProtocolError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum AssistantProtocolEvent {
    Ready {
        request_id: String,
        protocol_version: u32,
        adapter: String,
        adapter_version: String,
        policy: AssistantSidecarPolicy,
    },
    Catalog {
        request_id: String,
        catalog: AssistantProviderCatalog,
    },
    AuthenticationUrl {
        request_id: String,
        url: String,
        instructions: String,
    },
    AuthenticationPrompt {
        request_id: String,
        prompt_id: String,
        message: String,
        secret: bool,
    },
    AuthenticationComplete {
        request_id: String,
        provider: String,
        credential: AssistantCredentialLease,
    },
    CredentialUpdated {
        request_id: String,
        credential: AssistantCredentialLease,
    },
    TurnStarted {
        request_id: String,
    },
    TextDelta {
        request_id: String,
        delta: String,
    },
    ToolCall {
        request_id: String,
        call_id: String,
        name: String,
        arguments: Value,
    },
    TurnComplete {
        request_id: String,
        message: AssistantMessage,
    },
    Cancelled {
        request_id: String,
    },
    Error {
        request_id: String,
        error: AssistantProtocolError,
    },
}

#[derive(Debug, Error)]
pub enum AssistantError {
    #[error("assistant project root must be absolute: {path}")]
    ProjectRootMustBeAbsolute { path: PathBuf },
    #[error("assistant project root is not a directory: {path}")]
    ProjectRootNotDirectory { path: PathBuf },
    #[error("assistant transcript has unsupported schema version {actual}")]
    UnsupportedTranscriptVersion { actual: u32 },
    #[error("assistant transcript must have exactly one primary attachment")]
    PrimaryAttachmentCount,
    #[error("assistant egress manifest is invalid: {message}")]
    InvalidEgress { message: String },
    #[error("assistant proposal {proposal_id} payload hash does not match")]
    ProposalHashMismatch { proposal_id: AssistantProposalId },
    #[error("assistant proposal insertion content hash does not match")]
    InsertionHashMismatch,
    #[error("assistant proposal {proposal_id} insertion approval was invalidated")]
    InsertionApprovalInvalidated { proposal_id: AssistantProposalId },
    #[error("assistant proposal {proposal_id} approval was invalidated by a payload change")]
    ApprovalInvalidated { proposal_id: AssistantProposalId },
    #[error("assistant proposal {proposal_id} has not been approved")]
    ProposalNotApproved { proposal_id: AssistantProposalId },
    #[error("assistant proposal state mismatch: expected {expected:?}, got {actual:?}")]
    ProposalState {
        expected: AssistantProposalState,
        actual: AssistantProposalState,
    },
    #[error("assistant conversation identity mismatch: expected {expected}, got {actual}")]
    ConversationIdentityMismatch {
        expected: ConversationId,
        actual: ConversationId,
    },
    #[error("assistant JSON serialization failed: {0}")]
    Serialize(serde_json::Error),
    #[error("assistant JSON parsing failed: {0}")]
    Deserialize(serde_json::Error),
    #[error("failed to {action} at {path}: {source}")]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), AssistantError> {
    let parent = path.parent().expect("assistant managed file parent");
    fs::create_dir_all(parent).map_err(|source| AssistantError::Io {
        action: "create assistant managed directory",
        path: parent.to_owned(),
        source,
    })?;
    let mut temporary = NamedTempFile::new_in(parent).map_err(|source| AssistantError::Io {
        action: "create assistant temporary file",
        path: parent.to_owned(),
        source,
    })?;
    temporary
        .write_all(bytes)
        .map_err(|source| AssistantError::Io {
            action: "write assistant temporary file",
            path: temporary.path().to_owned(),
            source,
        })?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| AssistantError::Io {
            action: "sync assistant temporary file",
            path: temporary.path().to_owned(),
            source,
        })?;
    temporary
        .persist(path)
        .map_err(|error| AssistantError::Io {
            action: "persist assistant managed file",
            path: path.to_owned(),
            source: error.error,
        })?;
    Ok(())
}
