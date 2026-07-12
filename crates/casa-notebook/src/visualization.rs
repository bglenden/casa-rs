// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CellId, NotebookId, Timestamp};

pub const VISUALIZATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VisualizationReopenIntent {
    pub surface: String,
    pub contract_version: u32,
    pub parameters: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_toml: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VisualizationRenderMetadata {
    pub renderer: String,
    pub media_type: String,
    pub width: u32,
    pub height: u32,
    #[serde(default)]
    pub settings: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VisualizationRevision {
    pub revision: u64,
    pub created_at: Timestamp,
    pub asset_path: PathBuf,
    pub source_references: Vec<PathBuf>,
    pub reopen: VisualizationReopenIntent,
    pub render: VisualizationRenderMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VisualizationSnapshot {
    pub schema_version: u32,
    pub id: Uuid,
    pub notebook_id: NotebookId,
    pub cell_id: CellId,
    pub title: String,
    pub revisions: Vec<VisualizationRevision>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SaveVisualizationRequest {
    pub notebook_id: Option<NotebookId>,
    pub visualization_id: Option<Uuid>,
    pub title: String,
    pub source_asset: PathBuf,
    pub source_references: Vec<PathBuf>,
    pub reopen: VisualizationReopenIntent,
    pub render: VisualizationRenderMetadata,
}
