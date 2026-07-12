// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CellId, NotebookId, RunId, TaskCellIntent};

pub(crate) const RECEIPT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PythonExecutionAuthority {
    User,
    AiWorker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonEnvironmentIdentity {
    pub environment_id: String,
    pub interpreter: PathBuf,
    pub implementation: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub casa_rs_version: Option<String>,
    #[serde(default)]
    pub packages: BTreeMap<String, String>,
    pub fingerprint_sha256: String,
}

impl PythonEnvironmentIdentity {
    #[must_use]
    pub fn new(
        environment_id: impl Into<String>,
        interpreter: PathBuf,
        implementation: impl Into<String>,
        version: impl Into<String>,
        casa_rs_version: Option<String>,
        packages: BTreeMap<String, String>,
    ) -> Self {
        let mut identity = Self {
            environment_id: environment_id.into(),
            interpreter,
            implementation: implementation.into(),
            version: version.into(),
            casa_rs_version,
            packages,
            fingerprint_sha256: String::new(),
        };
        identity.fingerprint_sha256 = identity.computed_fingerprint();
        identity
    }

    #[must_use]
    pub fn has_valid_fingerprint(&self) -> bool {
        self.fingerprint_sha256 == self.computed_fingerprint()
    }

    fn computed_fingerprint(&self) -> String {
        let mut digest = Sha256::new();
        hash_field(&mut digest, self.environment_id.as_bytes());
        hash_field(&mut digest, self.interpreter.as_os_str().as_encoded_bytes());
        hash_field(&mut digest, self.implementation.as_bytes());
        hash_field(&mut digest, self.version.as_bytes());
        match &self.casa_rs_version {
            Some(version) => {
                digest.update([1]);
                hash_field(&mut digest, version.as_bytes());
            }
            None => digest.update([0]),
        }
        for (name, version) in &self.packages {
            hash_field(&mut digest, name.as_bytes());
            hash_field(&mut digest, version.as_bytes());
        }
        format!("{:x}", digest.finalize())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonExecutionInput {
    pub source: String,
    pub source_sha256: String,
    pub authority: PythonExecutionAuthority,
    #[serde(default)]
    pub input_references: Vec<PathBuf>,
    pub environment: PythonEnvironmentIdentity,
}

impl PythonExecutionInput {
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        authority: PythonExecutionAuthority,
        input_references: Vec<PathBuf>,
        environment: PythonEnvironmentIdentity,
    ) -> Self {
        let source = source.into();
        let source_sha256 = sha256(source.as_bytes());
        Self {
            source,
            source_sha256,
            authority,
            input_references,
            environment,
        }
    }

    #[must_use]
    pub fn has_valid_source_hash(&self) -> bool {
        self.source_sha256 == sha256(self.source.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub enum ExecutionInput {
    Python(PythonExecutionInput),
}

impl ExecutionInput {
    #[must_use]
    pub fn validation_error(&self) -> Option<&'static str> {
        match self {
            Self::Python(input) if !input.has_valid_source_hash() => {
                Some("Python source SHA-256 does not match the exact source")
            }
            Self::Python(input) if !input.environment.has_valid_fingerprint() => {
                Some("Python environment fingerprint does not match its identity fields")
            }
            Self::Python(_) => None,
        }
    }
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(value);
}

fn sha256(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    #[must_use]
    pub fn now() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        Self(u64::try_from(millis).unwrap_or(u64::MAX))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSafetyRecord {
    pub classification: String,
    #[serde(default)]
    pub affected_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRecord {
    pub kind: String,
    pub actor: String,
    pub timestamp: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactReference {
    pub role: String,
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogReferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub casa_log: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordingRequest {
    pub initiating_surface: String,
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notebook_id: Option<NotebookId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cell_id: Option<CellId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_intent: Option<TaskCellIntent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_input: Option<ExecutionInput>,
    pub provider_contract_version: u32,
    #[serde(default)]
    pub resolved_parameters: BTreeMap<String, serde_json::Value>,
    pub run_safety: RunSafetyRecord,
    #[serde(default)]
    pub approvals: Vec<ApprovalRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptFinalization {
    pub status: ExecutionStatus,
    pub finished_at: Timestamp,
    #[serde(default)]
    pub affected_paths: Vec<PathBuf>,
    #[serde(default)]
    pub products: Vec<ArtifactReference>,
    #[serde(default)]
    pub artifacts: Vec<ArtifactReference>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
    #[serde(default)]
    pub stdout: Vec<u8>,
    #[serde(default)]
    pub stderr: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub casa_log: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub schema_version: u32,
    pub run_id: RunId,
    pub revision: u64,
    pub notebook_id: NotebookId,
    pub cell_id: CellId,
    pub initiating_surface: String,
    pub operation_id: String,
    pub started_at: Timestamp,
    pub finished_at: Timestamp,
    pub status: ExecutionStatus,
    pub sparse_intent: Option<TaskCellIntent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_input: Option<ExecutionInput>,
    pub resolved_parameters: BTreeMap<String, serde_json::Value>,
    pub provider_contract_version: u32,
    pub run_safety: RunSafetyRecord,
    pub approvals: Vec<ApprovalRecord>,
    pub affected_paths: Vec<PathBuf>,
    pub products: Vec<ArtifactReference>,
    pub artifacts: Vec<ArtifactReference>,
    pub logs: LogReferences,
    pub diagnostics: Vec<String>,
    pub replay_claim: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayAssessment {
    pub parameters: BTreeMap<String, toml::Value>,
    pub warnings: Vec<String>,
}

impl ExecutionReceipt {
    #[must_use]
    pub fn assess_replay(
        &self,
        current_contract_version: u32,
        current_resolved_parameters: &BTreeMap<String, serde_json::Value>,
    ) -> ReplayAssessment {
        let mut warnings = Vec::new();
        if current_contract_version != self.provider_contract_version {
            warnings.push(format!(
                "provider contract changed from {} to {}; validate the typed parameter diff",
                self.provider_contract_version, current_contract_version
            ));
        }
        if current_resolved_parameters != &self.resolved_parameters {
            warnings.push(
                "current defaults or context resolve differently from this historical run"
                    .to_owned(),
            );
        }
        ReplayAssessment {
            parameters: self
                .sparse_intent
                .as_ref()
                .map(|intent| intent.parameters.clone())
                .unwrap_or_default(),
            warnings,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AttemptEventKind {
    Started,
    Progress,
    Approval,
    Diagnostic,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct AttemptEvent {
    pub schema_version: u32,
    pub run_id: RunId,
    pub timestamp: Timestamp,
    pub kind: AttemptEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub fields: BTreeMap<String, serde_json::Value>,
}

impl AttemptEvent {
    #[must_use]
    pub(crate) fn started(run_id: RunId) -> Self {
        Self {
            schema_version: RECEIPT_SCHEMA_VERSION,
            run_id,
            timestamp: Timestamp::now(),
            kind: AttemptEventKind::Started,
            message: None,
            fields: BTreeMap::new(),
        }
    }
}
