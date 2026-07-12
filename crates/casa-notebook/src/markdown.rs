// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, ops::Range, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CellId, NotebookId};

const NOTEBOOK_PREFIX: &str = "<!-- casa-rs-notebook:v1 id=";
const CELL_PREFIX: &str = "<!-- casa-rs-cell:v1 id=";
const CELL_END: &str = "<!-- /casa-rs-cell -->";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CellKind {
    Task,
    Python,
    Tutorial,
    AssistantPin,
    Output,
    Unknown(String),
}

impl CellKind {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Task => "task",
            Self::Python => "python",
            Self::Tutorial => "tutorial",
            Self::AssistantPin => "assistant-pin",
            Self::Output => "output",
            Self::Unknown(value) => value,
        }
    }
}

impl From<&str> for CellKind {
    fn from(value: &str) -> Self {
        match value {
            "task" => Self::Task,
            "python" => Self::Python,
            "tutorial" => Self::Tutorial,
            "assistant-pin" => Self::AssistantPin,
            "output" => Self::Output,
            other => Self::Unknown(other.to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskCellIntent {
    pub format: u32,
    pub surface: String,
    pub kind: String,
    pub contract: u32,
    pub parameters: BTreeMap<String, toml::Value>,
}

impl TaskCellIntent {
    pub fn parse(source: &str) -> Result<Self, NotebookParseError> {
        let fenced = fenced_toml(source).ok_or(NotebookParseError::MissingTaskTomlFence)?;
        let value = toml::from_str::<toml::Value>(fenced)
            .map_err(|source| NotebookParseError::InvalidTaskToml { source })?;
        let casars = value
            .get("casars")
            .and_then(toml::Value::as_table)
            .ok_or(NotebookParseError::MissingCasarsTable)?;
        let parameters = value
            .get("parameters")
            .and_then(toml::Value::as_table)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        Ok(Self {
            format: integer_field(casars, "format")?,
            surface: string_field(casars, "surface")?,
            kind: string_field(casars, "kind")?,
            contract: integer_field(casars, "contract")?,
            parameters,
        })
    }

    pub fn to_markdown(&self) -> Result<String, NotebookParseError> {
        let mut casars = toml::map::Map::new();
        casars.insert(
            "format".into(),
            toml::Value::Integer(i64::from(self.format)),
        );
        casars.insert("surface".into(), toml::Value::String(self.surface.clone()));
        casars.insert("kind".into(), toml::Value::String(self.kind.clone()));
        casars.insert(
            "contract".into(),
            toml::Value::Integer(i64::from(self.contract)),
        );
        let mut root = toml::map::Map::new();
        root.insert("casars".into(), toml::Value::Table(casars));
        root.insert(
            "parameters".into(),
            toml::Value::Table(self.parameters.clone().into_iter().collect()),
        );
        let rendered = toml::to_string(&toml::Value::Table(root))
            .map_err(|source| NotebookParseError::RenderTaskToml { source })?;
        Ok(format!("```toml\n{rendered}```\n"))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NotebookCell {
    pub id: CellId,
    pub kind: CellKind,
    pub full_range: Range<usize>,
    pub body_range: Range<usize>,
    pub task: Option<TaskCellIntent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NotebookDocument {
    source: String,
    notebook_id: NotebookId,
    cells: Vec<NotebookCell>,
}

impl NotebookDocument {
    pub fn parse(source: impl Into<String>) -> Result<Self, NotebookParseError> {
        let source = source.into();
        let notebook_id = parse_notebook_id(&source)?;
        let cells = parse_cells(&source)?;
        Ok(Self {
            source,
            notebook_id,
            cells,
        })
    }

    #[must_use]
    pub fn new(notebook_id: NotebookId, title: &str) -> Self {
        let source = format!("{NOTEBOOK_PREFIX}{notebook_id} -->\n\n# {title}\n");
        Self {
            source,
            notebook_id,
            cells: Vec::new(),
        }
    }

    #[must_use]
    pub const fn notebook_id(&self) -> NotebookId {
        self.notebook_id
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[must_use]
    pub fn cells(&self) -> &[NotebookCell] {
        &self.cells
    }

    #[must_use]
    pub fn cell(&self, id: CellId) -> Option<&NotebookCell> {
        self.cells.iter().find(|cell| cell.id == id)
    }

    pub fn append_task_cell(
        &mut self,
        id: CellId,
        intent: &TaskCellIntent,
    ) -> Result<(), NotebookParseError> {
        self.append_cell(id, CellKind::Task, &intent.to_markdown()?)
    }

    pub fn append_cell(
        &mut self,
        id: CellId,
        kind: CellKind,
        body: &str,
    ) -> Result<(), NotebookParseError> {
        if self.cell(id).is_some() {
            return Err(NotebookParseError::DuplicateCellId { id });
        }
        if !self.source.ends_with('\n') {
            self.source.push('\n');
        }
        self.source.push('\n');
        self.source
            .push_str(&format!("{CELL_PREFIX}{id} kind={} -->\n", kind.as_str()));
        self.source.push_str(body);
        if !body.ends_with('\n') {
            self.source.push('\n');
        }
        self.source.push_str(CELL_END);
        self.source.push('\n');
        *self = Self::parse(std::mem::take(&mut self.source))?;
        Ok(())
    }

    /// Replace one managed cell body while preserving its identity and neighbors.
    pub fn replace_cell_body(&mut self, id: CellId, body: &str) -> Result<(), NotebookParseError> {
        let cell = self
            .cell(id)
            .ok_or(NotebookParseError::CellNotFound { id })?;
        let mut replacement = body.to_owned();
        if !replacement.ends_with('\n') {
            replacement.push('\n');
        }
        self.source
            .replace_range(cell.body_range.clone(), &replacement);
        *self = Self::parse(std::mem::take(&mut self.source))?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum NotebookParseError {
    #[error("notebook v1 marker is missing")]
    MissingNotebookMarker,
    #[error("notebook marker is malformed")]
    MalformedNotebookMarker,
    #[error("invalid notebook id {value:?}: {source}")]
    InvalidNotebookId { value: String, source: uuid::Error },
    #[error("cell marker at byte {offset} is malformed")]
    MalformedCellMarker { offset: usize },
    #[error("cell {id} has no closing marker")]
    UnclosedCell { id: CellId },
    #[error("cell id {id} occurs more than once")]
    DuplicateCellId { id: CellId },
    #[error("cell {id} is not present")]
    CellNotFound { id: CellId },
    #[error("invalid cell id {value:?}: {source}")]
    InvalidCellId { value: String, source: uuid::Error },
    #[error("task cell has no fenced TOML block")]
    MissingTaskTomlFence,
    #[error("task cell TOML is invalid: {source}")]
    InvalidTaskToml { source: toml::de::Error },
    #[error("task cell is missing [casars]")]
    MissingCasarsTable,
    #[error("task cell [casars].{field} is missing or has the wrong type")]
    InvalidCasarsField { field: &'static str },
    #[error("task cell TOML could not be rendered: {source}")]
    RenderTaskToml { source: toml::ser::Error },
}

fn parse_notebook_id(source: &str) -> Result<NotebookId, NotebookParseError> {
    let start = source
        .find(NOTEBOOK_PREFIX)
        .ok_or(NotebookParseError::MissingNotebookMarker)?
        + NOTEBOOK_PREFIX.len();
    let rest = &source[start..];
    let end = rest
        .find(" -->")
        .ok_or(NotebookParseError::MalformedNotebookMarker)?;
    let value = &rest[..end];
    NotebookId::from_str(value).map_err(|source| NotebookParseError::InvalidNotebookId {
        value: value.to_owned(),
        source,
    })
}

fn parse_cells(source: &str) -> Result<Vec<NotebookCell>, NotebookParseError> {
    let mut cells = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = source[cursor..].find(CELL_PREFIX) {
        let marker_start = cursor + relative;
        let marker_end = source[marker_start..]
            .find(" -->")
            .map(|offset| marker_start + offset + 4)
            .ok_or(NotebookParseError::MalformedCellMarker {
                offset: marker_start,
            })?;
        let attributes = &source[marker_start + CELL_PREFIX.len()..marker_end - 4];
        let (id_text, kind_text) =
            attributes
                .split_once(" kind=")
                .ok_or(NotebookParseError::MalformedCellMarker {
                    offset: marker_start,
                })?;
        let id = CellId::from_str(id_text).map_err(|source| NotebookParseError::InvalidCellId {
            value: id_text.to_owned(),
            source,
        })?;
        if cells.iter().any(|cell: &NotebookCell| cell.id == id) {
            return Err(NotebookParseError::DuplicateCellId { id });
        }
        let body_start = source[marker_end..]
            .find('\n')
            .map_or(marker_end, |offset| marker_end + offset + 1);
        let body_tail = &source[body_start..];
        let body_end = body_tail
            .find(CELL_END)
            .map(|offset| body_start + offset)
            .ok_or(NotebookParseError::UnclosedCell { id })?;
        let full_end = body_end + CELL_END.len();
        let kind = CellKind::from(kind_text);
        let task = if kind == CellKind::Task {
            Some(TaskCellIntent::parse(&source[body_start..body_end])?)
        } else {
            None
        };
        cells.push(NotebookCell {
            id,
            kind,
            full_range: marker_start..full_end,
            body_range: body_start..body_end,
            task,
        });
        cursor = full_end;
    }
    Ok(cells)
}

fn fenced_toml(source: &str) -> Option<&str> {
    let fence = source.find("```toml")?;
    let content_start = source[fence..]
        .find('\n')
        .map(|offset| fence + offset + 1)?;
    let content_end = source[content_start..]
        .find("```")
        .map(|offset| content_start + offset)?;
    Some(&source[content_start..content_end])
}

fn integer_field(
    table: &toml::map::Map<String, toml::Value>,
    field: &'static str,
) -> Result<u32, NotebookParseError> {
    table
        .get(field)
        .and_then(toml::Value::as_integer)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(NotebookParseError::InvalidCasarsField { field })
}

fn string_field(
    table: &toml::map::Map<String, toml::Value>,
    field: &'static str,
) -> Result<String, NotebookParseError> {
    table
        .get(field)
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or(NotebookParseError::InvalidCasarsField { field })
}
