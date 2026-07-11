// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{CellId, NotebookId, RunId, TaskCellIntent};

pub(crate) const RECEIPT_SCHEMA_VERSION: u32 = 1;

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
