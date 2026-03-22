// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generic interactive browser support for casacore tables.
//!
//! The browser is schema-agnostic and can inspect arbitrary tables without
//! assuming MeasurementSet structure. It keeps enough typed state to drive a
//! long-lived subprocess session and future edit-mode extensions, while the v1
//! interaction model remains read-only.

use std::borrow::Cow;
use std::cmp::{max, min};
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

pub use casacore_tablebrowser_protocol::BrowserView as TableBrowserView;

use casacore_tablebrowser_protocol::{
    BrowserAddress, BrowserArrayElement, BrowserBreadcrumbEntry, BrowserCapabilities,
    BrowserCommand, BrowserComplex32Value, BrowserComplex64Value, BrowserFocus,
    BrowserInspectorSnapshot, BrowserInspectorTrailEntry, BrowserPrimitiveType,
    BrowserRecordFieldSummary, BrowserResponseEnvelope, BrowserScalarValue, BrowserSnapshot,
    BrowserValueKind, BrowserValueNode, BrowserViewport, ValuePathSegment,
};
use casacore_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use thiserror::Error;

use crate::{
    ArrayShapeContract, ColumnType, DataManagerInfo, Table, TableError, TableInfo, TableOptions,
};

const MIN_COLUMN_WIDTH: usize = 8;
const MAX_COLUMN_WIDTH: usize = 32;
const SAMPLE_ROW_COUNT: usize = 32;

/// One linked-table reference discovered while browsing a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkedTableRef {
    /// Human-friendly label derived from the target path.
    pub label: String,
    /// Original relative or absolute path stored in the keyword value.
    pub relative_path: String,
    /// Resolved filesystem path used when reopening the linked table.
    pub resolved_path: PathBuf,
    /// Description of where the reference was found.
    pub source: String,
    /// Whether the resolved path exists and can be opened as a table.
    pub openable: bool,
}

/// Errors surfaced by the generic table browser.
#[derive(Debug, Error)]
pub enum TableBrowserError {
    /// Table open or cell access failed.
    #[error("{0}")]
    Table(#[from] TableError),
    /// The browser command could not be applied in the current state.
    #[error("{0}")]
    InvalidCommand(String),
}

/// Stateful browser session for an arbitrary casacore table.
#[derive(Debug)]
pub struct TableBrowser {
    stack: Vec<OpenedTable>,
    view: TableBrowserView,
    focus: BrowserFocus,
    viewport: BrowserViewport,
    status_line: String,
    overview_scroll: usize,
    columns_selected: usize,
    columns_scroll: usize,
    keywords_selected: usize,
    keywords_scroll: usize,
    cells_row_selected: usize,
    cells_row_scroll: usize,
    cells_column_selected: usize,
    cells_column_offset: usize,
    subtables_selected: usize,
    subtables_scroll: usize,
    inspector_path: Vec<ValuePathSegment>,
    inspector_selected_child: usize,
    inspector_page_start: usize,
}

#[derive(Debug)]
struct OpenedTable {
    path: PathBuf,
    table: Table,
    columns: Vec<ColumnEntry>,
    keyword_entries: Vec<KeywordEntry>,
    linked_tables: Vec<LinkedTableRef>,
    overview_lines: Vec<String>,
}

#[derive(Debug, Clone)]
struct ColumnEntry {
    name: String,
    cell_header: String,
    width: usize,
    summary: String,
    detail_lines: Vec<String>,
    display_unit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum KeywordOwner {
    Table,
    Column(String),
}

#[derive(Debug, Clone)]
struct KeywordEntry {
    owner: KeywordOwner,
    path: Vec<String>,
    depth: usize,
    summary: String,
}

#[derive(Debug, Clone)]
enum ValueNodeRef<'a> {
    Undefined,
    Scalar(Cow<'a, ScalarValue>),
    Array(&'a ArrayValue),
    Record(&'a RecordValue),
    TableRef(&'a str),
}

#[derive(Debug, Clone)]
struct SelectionContext<'a> {
    address: BrowserAddress,
    owner_path: &'a Path,
    trail: Vec<BrowserInspectorTrailEntry>,
    node: ValueNodeRef<'a>,
}

impl TableBrowser {
    /// Open a browser rooted at `table_path`.
    pub fn open(table_path: impl AsRef<Path>) -> Result<Self, TableBrowserError> {
        let opened = OpenedTable::open(table_path.as_ref())?;
        let current_path = opened.path.display().to_string();
        Ok(Self {
            stack: vec![opened],
            view: TableBrowserView::Overview,
            focus: BrowserFocus::Main,
            viewport: BrowserViewport::default(),
            status_line: format!("Browsing {current_path}."),
            overview_scroll: 0,
            columns_selected: 0,
            columns_scroll: 0,
            keywords_selected: 0,
            keywords_scroll: 0,
            cells_row_selected: 0,
            cells_row_scroll: 0,
            cells_column_selected: 0,
            cells_column_offset: 0,
            subtables_selected: 0,
            subtables_scroll: 0,
            inspector_path: Vec::new(),
            inspector_selected_child: 0,
            inspector_page_start: 0,
        })
    }

    /// Apply a session command and return the updated snapshot.
    pub fn apply(&mut self, command: BrowserCommand) -> Result<BrowserSnapshot, TableBrowserError> {
        match command {
            BrowserCommand::OpenRoot { path, viewport } => {
                let opened = OpenedTable::open(Path::new(&path))?;
                self.stack.clear();
                self.stack.push(opened);
                self.view = TableBrowserView::Overview;
                self.focus = BrowserFocus::Main;
                self.viewport = viewport;
                self.reset_navigation_state();
                self.status_line = format!("Browsing {}.", self.current().path.display());
            }
            BrowserCommand::Resize { viewport } => {
                self.viewport = viewport;
            }
            BrowserCommand::CycleView { forward, viewport } => {
                self.apply_viewport(viewport);
                self.cycle_view(forward);
            }
            BrowserCommand::MoveUp { steps, viewport } => {
                self.apply_viewport(viewport);
                self.move_vertical(steps, false);
            }
            BrowserCommand::MoveDown { steps, viewport } => {
                self.apply_viewport(viewport);
                self.move_vertical(steps, true);
            }
            BrowserCommand::MoveLeft { steps, viewport } => {
                self.apply_viewport(viewport);
                self.move_horizontal(steps, false);
            }
            BrowserCommand::MoveRight { steps, viewport } => {
                self.apply_viewport(viewport);
                self.move_horizontal(steps, true);
            }
            BrowserCommand::PageUp { pages, viewport } => {
                self.apply_viewport(viewport);
                self.move_page(pages, false);
            }
            BrowserCommand::PageDown { pages, viewport } => {
                self.apply_viewport(viewport);
                self.move_page(pages, true);
            }
            BrowserCommand::Activate { viewport } => {
                self.apply_viewport(viewport);
                self.activate()?;
            }
            BrowserCommand::Back { viewport } => {
                self.apply_viewport(viewport);
                self.pop_table();
            }
            BrowserCommand::Escape { viewport } => {
                self.apply_viewport(viewport);
                self.escape();
            }
            BrowserCommand::GetSnapshot { viewport } => {
                self.apply_viewport(viewport);
            }
        }

        Ok(self.snapshot())
    }

    /// Render a protocol response envelope for the current browser state.
    pub fn handle_request(
        &mut self,
        request: casacore_tablebrowser_protocol::BrowserRequestEnvelope,
    ) -> BrowserResponseEnvelope {
        if request.version != casacore_tablebrowser_protocol::PROTOCOL_VERSION {
            return BrowserResponseEnvelope::error(
                "unsupported_version",
                format!(
                    "expected protocol version {}, received {}",
                    casacore_tablebrowser_protocol::PROTOCOL_VERSION,
                    request.version
                ),
            );
        }

        match self.apply(request.command) {
            Ok(snapshot) => BrowserResponseEnvelope::snapshot(snapshot),
            Err(error) => BrowserResponseEnvelope::error("browser_error", error.to_string()),
        }
    }

    /// Return the active browser view.
    pub fn view(&self) -> TableBrowserView {
        self.view
    }

    /// Set the active browser view.
    pub fn set_view(&mut self, view: TableBrowserView) {
        if self.view == view {
            return;
        }
        self.view = view;
        self.focus = BrowserFocus::Main;
        self.clear_inspector_state();
        self.clamp_all();
    }

    /// Cycle the active view left or right.
    pub fn cycle_view(&mut self, forward: bool) {
        const ALL: [TableBrowserView; 5] = [
            TableBrowserView::Overview,
            TableBrowserView::Columns,
            TableBrowserView::Keywords,
            TableBrowserView::Cells,
            TableBrowserView::Subtables,
        ];
        let index = ALL
            .iter()
            .position(|candidate| *candidate == self.view)
            .unwrap_or(0);
        let next = if forward {
            (index + 1) % ALL.len()
        } else if index == 0 {
            ALL.len() - 1
        } else {
            index - 1
        };
        self.set_view(ALL[next]);
    }

    /// Return the filesystem path of the currently open table.
    pub fn current_path(&self) -> &Path {
        &self.current().path
    }

    /// Return the number of columns in the currently opened table.
    pub fn column_count(&self) -> usize {
        self.current().columns.len()
    }

    /// Return the currently selected linked-table index, if any.
    pub fn selected_subtable_index(&self) -> Option<usize> {
        (!self.current().linked_tables.is_empty()).then_some(self.subtables_selected)
    }

    /// Select a linked table by index.
    pub fn select_subtable(&mut self, index: usize) -> bool {
        if index >= self.current().linked_tables.len() {
            return false;
        }
        self.subtables_selected = index;
        true
    }

    /// Select a cell by `(row, column)` for CLI inspection.
    pub fn select_cell(&mut self, row: usize, column: &str) -> Result<(), TableBrowserError> {
        let Some(index) = self
            .current()
            .columns
            .iter()
            .position(|entry| entry.name == column)
        else {
            return Err(TableBrowserError::InvalidCommand(format!(
                "unknown column {column:?}"
            )));
        };
        self.view = TableBrowserView::Cells;
        self.focus = BrowserFocus::Main;
        self.cells_row_selected = row.min(self.current().table.row_count().saturating_sub(1));
        self.cells_column_selected = index;
        self.clear_inspector_state();
        self.clamp_all();
        Ok(())
    }

    /// Select a cell by row and zero-based column index for CLI browsing.
    pub fn select_cell_position(
        &mut self,
        row: usize,
        column_index: usize,
    ) -> Result<(), TableBrowserError> {
        let Some(column_name) = self
            .current()
            .columns
            .get(column_index)
            .map(|column| column.name.clone())
        else {
            return Err(TableBrowserError::InvalidCommand(format!(
                "column index {column_index} is out of range"
            )));
        };
        self.select_cell(row, &column_name)
    }

    /// Select a table keyword path for CLI inspection.
    pub fn select_table_keyword(&mut self, path: &[String]) -> Result<(), TableBrowserError> {
        let Some(index) = self
            .current()
            .keyword_entries
            .iter()
            .position(|entry| matches!(entry.owner, KeywordOwner::Table) && entry.path == path)
        else {
            return Err(TableBrowserError::InvalidCommand(format!(
                "unknown table keyword path {}",
                path.join(".")
            )));
        };
        self.view = TableBrowserView::Keywords;
        self.focus = BrowserFocus::Main;
        self.keywords_selected = index;
        self.clear_inspector_state();
        self.clamp_all();
        Ok(())
    }

    /// Select a column keyword path for CLI inspection.
    pub fn select_column_keyword(
        &mut self,
        column: &str,
        path: &[String],
    ) -> Result<(), TableBrowserError> {
        let Some(index) = self.current().keyword_entries.iter().position(|entry| {
            matches!(&entry.owner, KeywordOwner::Column(owner) if owner == column)
                && entry.path == path
        }) else {
            return Err(TableBrowserError::InvalidCommand(format!(
                "unknown column keyword path {column}:{}",
                path.join(".")
            )));
        };
        self.view = TableBrowserView::Keywords;
        self.focus = BrowserFocus::Main;
        self.keywords_selected = index;
        self.clear_inspector_state();
        self.clamp_all();
        Ok(())
    }

    /// Open the currently selected linked table or selected inspector table ref.
    pub fn open_selected_subtable(&mut self) -> Result<(), TableBrowserError> {
        let target = if matches!(self.view, TableBrowserView::Subtables) {
            self.current()
                .linked_tables
                .get(self.subtables_selected)
                .map(|linked| linked.resolved_path.clone())
        } else {
            self.current_table_ref_target()
        };
        let Some(path) = target else {
            return Err(TableBrowserError::InvalidCommand(
                "no openable table reference is currently selected".to_string(),
            ));
        };
        if !path.exists() {
            return Err(TableBrowserError::InvalidCommand(format!(
                "linked table path does not exist: {}",
                path.display()
            )));
        }
        let opened = OpenedTable::open(&path)?;
        self.stack.push(opened);
        self.view = TableBrowserView::Overview;
        self.focus = BrowserFocus::Main;
        self.reset_navigation_state();
        self.status_line = format!("Browsing {}.", self.current().path.display());
        Ok(())
    }

    /// Pop back to the parent table, if any.
    pub fn pop_table(&mut self) -> bool {
        if self.stack.len() <= 1 {
            self.status_line = "Already at the root table.".to_string();
            return false;
        }
        self.stack.pop();
        self.view = TableBrowserView::Overview;
        self.focus = BrowserFocus::Main;
        self.reset_navigation_state();
        self.status_line = format!("Browsing {}.", self.current().path.display());
        true
    }

    /// Render the current session snapshot.
    pub fn snapshot(&self) -> BrowserSnapshot {
        let inspector = self
            .current_selection_context()
            .map(|context| self.render_inspector_snapshot(&context, self.inspector_height()));
        let content_lines = self.render_content_lines();
        BrowserSnapshot {
            capabilities: BrowserCapabilities { editable: false },
            view: self.view,
            focus: self.focus,
            table_path: self.current().path.display().to_string(),
            breadcrumb: self
                .stack
                .iter()
                .map(|opened| BrowserBreadcrumbEntry {
                    label: display_name_for_path(&opened.path),
                    path: opened.path.display().to_string(),
                })
                .collect(),
            viewport: self.viewport,
            status_line: self.status_line.clone(),
            content_lines,
            selected_address: self
                .current_selection_context()
                .map(|context| context.address),
            inspector,
        }
    }

    /// Render the main lines for a single view without appending inspector
    /// content. This is intended for bounded standalone CLI output.
    pub fn render_view_lines(
        &mut self,
        view: TableBrowserView,
        viewport: BrowserViewport,
    ) -> Vec<String> {
        self.set_view(view);
        self.viewport = viewport;
        self.clamp_all();
        let lines = match self.view {
            TableBrowserView::Overview => self.render_overview_lines(),
            TableBrowserView::Columns => self.render_columns_lines(),
            TableBrowserView::Keywords => self.render_keywords_lines(),
            TableBrowserView::Cells => self.render_cells_lines(),
            TableBrowserView::Subtables => self.render_subtables_lines(),
        };
        lines
            .into_iter()
            .take(viewport.height as usize)
            .map(|line| fit_text(&line, viewport.width as usize))
            .collect()
    }

    fn current(&self) -> &OpenedTable {
        self.stack
            .last()
            .expect("table browser always has an opened table")
    }

    fn apply_viewport(&mut self, viewport: Option<BrowserViewport>) {
        if let Some(viewport) = viewport {
            self.viewport = viewport;
        }
    }

    fn reset_navigation_state(&mut self) {
        self.overview_scroll = 0;
        self.columns_selected = 0;
        self.columns_scroll = 0;
        self.keywords_selected = 0;
        self.keywords_scroll = 0;
        self.cells_row_selected = 0;
        self.cells_row_scroll = 0;
        self.cells_column_selected = 0;
        self.cells_column_offset = 0;
        self.subtables_selected = 0;
        self.subtables_scroll = 0;
        self.clear_inspector_state();
    }

    fn clear_inspector_state(&mut self) {
        self.focus = BrowserFocus::Main;
        self.inspector_path.clear();
        self.inspector_selected_child = 0;
        self.inspector_page_start = 0;
    }

    fn main_height(&self) -> usize {
        self.viewport.height as usize
    }

    fn inspector_height(&self) -> usize {
        if !matches!(
            self.view,
            TableBrowserView::Cells
                | TableBrowserView::Keywords
                | TableBrowserView::Columns
                | TableBrowserView::Subtables
        ) {
            return 0;
        }
        let height = self.viewport.height as usize;
        if height < 8 {
            return 0;
        }
        let inspector = max(4, height / 3);
        inspector.min(height.saturating_sub(3))
    }

    fn clamp_all(&mut self) {
        self.columns_selected = self
            .columns_selected
            .min(self.current().columns.len().saturating_sub(1));
        self.keywords_selected = self
            .keywords_selected
            .min(self.current().keyword_entries.len().saturating_sub(1));
        self.cells_row_selected = self
            .cells_row_selected
            .min(self.current().table.row_count().saturating_sub(1));
        self.cells_column_selected = self
            .cells_column_selected
            .min(self.current().columns.len().saturating_sub(1));
        self.subtables_selected = self
            .subtables_selected
            .min(self.current().linked_tables.len().saturating_sub(1));
        self.ensure_main_selection_visible();
        self.clamp_inspector();
    }

    fn ensure_main_selection_visible(&mut self) {
        let main_height = self.main_height();
        match self.view {
            TableBrowserView::Overview => {
                let total = self.current().overview_lines.len();
                self.overview_scroll = self.overview_scroll.min(total.saturating_sub(main_height));
            }
            TableBrowserView::Columns => {
                self.columns_scroll = clamp_scroll_for_selection(
                    self.columns_scroll,
                    self.columns_selected,
                    main_height.saturating_sub(2),
                    self.current().columns.len(),
                );
            }
            TableBrowserView::Keywords => {
                self.keywords_scroll = clamp_scroll_for_selection(
                    self.keywords_scroll,
                    self.keywords_selected,
                    main_height.saturating_sub(2),
                    self.current().keyword_entries.len(),
                );
            }
            TableBrowserView::Cells => {
                let row_capacity = main_height.saturating_sub(3);
                self.cells_row_scroll = clamp_scroll_for_selection(
                    self.cells_row_scroll,
                    self.cells_row_selected,
                    row_capacity,
                    self.current().table.row_count(),
                );
                self.cells_column_offset = self.compute_column_offset();
            }
            TableBrowserView::Subtables => {
                self.subtables_scroll = clamp_scroll_for_selection(
                    self.subtables_scroll,
                    self.subtables_selected,
                    main_height.saturating_sub(2),
                    self.current().linked_tables.len(),
                );
            }
        }
    }

    fn compute_column_offset(&self) -> usize {
        let width = self.viewport.width as usize;
        if width == 0 {
            return self.cells_column_offset;
        }
        let columns = &self.current().columns;
        if columns.is_empty() {
            return 0;
        }
        let selected = self.cells_column_selected.min(columns.len() - 1);
        let mut offset = min(self.cells_column_offset, selected);
        while !cells_window_fits(columns, offset, selected, width) && offset < selected {
            offset += 1;
        }
        offset
    }

    fn clamp_inspector(&mut self) {
        let Some(context) = self.current_selection_context() else {
            self.clear_inspector_state();
            return;
        };
        match context.node {
            ValueNodeRef::Record(record) => {
                let total = record.fields().len();
                self.inspector_selected_child =
                    self.inspector_selected_child.min(total.saturating_sub(1));
                self.inspector_page_start = clamp_scroll_for_selection(
                    self.inspector_page_start,
                    self.inspector_selected_child,
                    self.inspector_page_size(),
                    total,
                );
            }
            ValueNodeRef::Array(array) => {
                let total = array_total_len(array);
                self.inspector_selected_child =
                    self.inspector_selected_child.min(total.saturating_sub(1));
                self.inspector_page_start = clamp_scroll_for_selection(
                    self.inspector_page_start,
                    self.inspector_selected_child,
                    self.inspector_page_size(),
                    total,
                );
            }
            ValueNodeRef::Undefined | ValueNodeRef::Scalar(_) | ValueNodeRef::TableRef(_) => {
                self.inspector_selected_child = 0;
                self.inspector_page_start = 0;
            }
        }
    }

    fn inspector_page_size(&self) -> usize {
        self.inspector_height().saturating_sub(3).max(1)
    }

    fn move_vertical(&mut self, steps: usize, forward: bool) {
        if self.focus == BrowserFocus::Inspector {
            self.move_inspector_vertical(steps, forward);
            return;
        }

        match self.view {
            TableBrowserView::Overview => {
                self.overview_scroll = adjust_index(self.overview_scroll, steps, forward);
                self.ensure_main_selection_visible();
            }
            TableBrowserView::Columns => {
                self.columns_selected = adjust_index(self.columns_selected, steps, forward);
                self.columns_selected = self
                    .columns_selected
                    .min(self.current().columns.len().saturating_sub(1));
                self.clear_inspector_state();
                self.ensure_main_selection_visible();
            }
            TableBrowserView::Keywords => {
                self.keywords_selected = adjust_index(self.keywords_selected, steps, forward);
                self.keywords_selected = self
                    .keywords_selected
                    .min(self.current().keyword_entries.len().saturating_sub(1));
                self.clear_inspector_state();
                self.ensure_main_selection_visible();
            }
            TableBrowserView::Cells => {
                self.cells_row_selected = adjust_index(self.cells_row_selected, steps, forward);
                self.cells_row_selected = self
                    .cells_row_selected
                    .min(self.current().table.row_count().saturating_sub(1));
                self.clear_inspector_state();
                self.ensure_main_selection_visible();
            }
            TableBrowserView::Subtables => {
                self.subtables_selected = adjust_index(self.subtables_selected, steps, forward);
                self.subtables_selected = self
                    .subtables_selected
                    .min(self.current().linked_tables.len().saturating_sub(1));
                self.clear_inspector_state();
                self.ensure_main_selection_visible();
            }
        }
    }

    fn move_horizontal(&mut self, steps: usize, forward: bool) {
        if self.focus == BrowserFocus::Inspector {
            self.move_inspector_horizontal(steps, forward);
            return;
        }

        if !matches!(self.view, TableBrowserView::Cells) {
            return;
        }
        self.cells_column_selected = adjust_index(self.cells_column_selected, steps, forward);
        self.cells_column_selected = self
            .cells_column_selected
            .min(self.current().columns.len().saturating_sub(1));
        self.clear_inspector_state();
        self.ensure_main_selection_visible();
    }

    fn move_page(&mut self, pages: usize, forward: bool) {
        let amount = pages.max(1);
        if self.focus == BrowserFocus::Inspector {
            let size = self.inspector_page_size();
            self.move_inspector_vertical(size.saturating_mul(amount), forward);
            return;
        }

        let page = self.main_height().saturating_sub(3).max(1);
        self.move_vertical(page.saturating_mul(amount), forward);
    }

    fn move_inspector_vertical(&mut self, steps: usize, forward: bool) {
        let Some(context) = self.current_selection_context() else {
            self.focus = BrowserFocus::Main;
            return;
        };
        match context.node {
            ValueNodeRef::Record(record) => {
                let total = record.fields().len();
                if total == 0 {
                    return;
                }
                self.inspector_selected_child =
                    adjust_index(self.inspector_selected_child, steps, forward).min(total - 1);
                self.clamp_inspector();
            }
            ValueNodeRef::Array(array) => {
                let total = array_total_len(array);
                if total == 0 {
                    return;
                }
                self.inspector_selected_child =
                    adjust_index(self.inspector_selected_child, steps, forward).min(total - 1);
                self.clamp_inspector();
            }
            ValueNodeRef::Undefined | ValueNodeRef::Scalar(_) | ValueNodeRef::TableRef(_) => {}
        }
    }

    fn move_inspector_horizontal(&mut self, steps: usize, forward: bool) {
        let Some(context) = self.current_selection_context() else {
            self.focus = BrowserFocus::Main;
            return;
        };
        match context.node {
            ValueNodeRef::Record(record) => {
                let total = record.fields().len();
                let page = self.inspector_page_size().saturating_mul(steps.max(1));
                if total == 0 {
                    return;
                }
                self.inspector_selected_child =
                    adjust_index(self.inspector_selected_child, page, forward).min(total - 1);
                self.clamp_inspector();
            }
            ValueNodeRef::Array(array) => {
                let total = array_total_len(array);
                let page = self.inspector_page_size().saturating_mul(steps.max(1));
                if total == 0 {
                    return;
                }
                self.inspector_selected_child =
                    adjust_index(self.inspector_selected_child, page, forward).min(total - 1);
                self.clamp_inspector();
            }
            ValueNodeRef::Undefined | ValueNodeRef::Scalar(_) | ValueNodeRef::TableRef(_) => {}
        }
    }

    fn activate(&mut self) -> Result<(), TableBrowserError> {
        let Some(context) = self.current_selection_context() else {
            return Ok(());
        };
        match (self.focus, context.node) {
            (_, ValueNodeRef::TableRef(_)) => {
                self.open_selected_subtable()?;
            }
            (BrowserFocus::Main, ValueNodeRef::Record(_) | ValueNodeRef::Array(_)) => {
                self.focus = BrowserFocus::Inspector;
                self.inspector_selected_child = 0;
                self.inspector_page_start = 0;
            }
            (BrowserFocus::Inspector, ValueNodeRef::Record(record)) => {
                let Some(field) = record.fields().get(self.inspector_selected_child) else {
                    return Ok(());
                };
                self.inspector_path.push(ValuePathSegment::RecordField {
                    name: field.name.clone(),
                });
                self.inspector_selected_child = 0;
                self.inspector_page_start = 0;
                self.clamp_inspector();
                if matches!(
                    self.current_selection_context().map(|ctx| ctx.node),
                    Some(ValueNodeRef::TableRef(_))
                ) {
                    self.open_selected_subtable()?;
                }
            }
            (BrowserFocus::Inspector, ValueNodeRef::Array(array)) => {
                if array_total_len(array) == 0 {
                    return Ok(());
                }
                self.inspector_path.push(ValuePathSegment::ArrayIndex {
                    flat_index: self.inspector_selected_child,
                });
                self.inspector_selected_child = 0;
                self.inspector_page_start = 0;
            }
            _ => {}
        }
        Ok(())
    }

    fn escape(&mut self) {
        if !self.inspector_path.is_empty() {
            self.inspector_path.pop();
            self.inspector_selected_child = 0;
            self.inspector_page_start = 0;
            self.focus = if matches!(
                self.view,
                TableBrowserView::Cells | TableBrowserView::Keywords
            ) {
                BrowserFocus::Inspector
            } else {
                BrowserFocus::Main
            };
            self.clamp_inspector();
        } else {
            self.focus = BrowserFocus::Main;
        }
    }

    fn current_table_ref_target(&self) -> Option<PathBuf> {
        let context = self.current_selection_context()?;
        match context.node {
            ValueNodeRef::TableRef(path) => {
                Some(resolve_linked_table_path(context.owner_path, path))
            }
            _ => None,
        }
    }

    fn current_selection_context(&self) -> Option<SelectionContext<'_>> {
        match self.view {
            TableBrowserView::Overview => None,
            TableBrowserView::Columns => {
                let column = self.current().columns.get(self.columns_selected)?;
                Some(SelectionContext {
                    address: BrowserAddress::Column {
                        table_path: self.current().path.display().to_string(),
                        column: column.name.clone(),
                    },
                    owner_path: &self.current().path,
                    trail: Vec::new(),
                    node: ValueNodeRef::Undefined,
                })
            }
            TableBrowserView::Keywords => self.keyword_selection_context(),
            TableBrowserView::Cells => self.cell_selection_context(),
            TableBrowserView::Subtables => {
                let linked = self.current().linked_tables.get(self.subtables_selected)?;
                Some(SelectionContext {
                    address: BrowserAddress::Subtable {
                        table_path: self.current().path.display().to_string(),
                        source: linked.source.clone(),
                        target_path: linked.resolved_path.display().to_string(),
                    },
                    owner_path: &self.current().path,
                    trail: Vec::new(),
                    node: ValueNodeRef::TableRef(linked.relative_path.as_str()),
                })
            }
        }
    }

    fn keyword_selection_context(&self) -> Option<SelectionContext<'_>> {
        let entry = self.current().keyword_entries.get(self.keywords_selected)?;
        let value = lookup_keyword_value(self.current(), entry)?;
        let (node, trail, value_path) = walk_value_node(
            value,
            self.owner_path_for_entry(entry),
            &self.inspector_path,
        )?;
        let address = match &entry.owner {
            KeywordOwner::Table => BrowserAddress::TableKeyword {
                table_path: self.current().path.display().to_string(),
                keyword_path: entry.path.clone(),
                value_path,
            },
            KeywordOwner::Column(column) => BrowserAddress::ColumnKeyword {
                table_path: self.current().path.display().to_string(),
                column: column.clone(),
                keyword_path: entry.path.clone(),
                value_path,
            },
        };
        Some(SelectionContext {
            address,
            owner_path: self.owner_path_for_entry(entry),
            trail,
            node,
        })
    }

    fn owner_path_for_entry<'a>(&'a self, _entry: &'a KeywordEntry) -> &'a Path {
        &self.current().path
    }

    fn cell_selection_context(&self) -> Option<SelectionContext<'_>> {
        let column = self.current().columns.get(self.cells_column_selected)?;
        let value = self
            .current()
            .table
            .cell(self.cells_row_selected, &column.name)
            .ok()
            .flatten();
        let (node, trail, value_path) =
            walk_optional_value_node(value, &self.current().path, &self.inspector_path)?;
        Some(SelectionContext {
            address: BrowserAddress::Cell {
                table_path: self.current().path.display().to_string(),
                row: self.cells_row_selected,
                column: column.name.clone(),
                value_path,
            },
            owner_path: &self.current().path,
            trail,
            node,
        })
    }

    fn render_content_lines(&self) -> Vec<String> {
        let lines = match self.view {
            TableBrowserView::Overview => self.render_overview_lines(),
            TableBrowserView::Columns => self.render_columns_lines(),
            TableBrowserView::Keywords => self.render_keywords_lines(),
            TableBrowserView::Cells => self.render_cells_lines(),
            TableBrowserView::Subtables => self.render_subtables_lines(),
        };

        lines
            .into_iter()
            .take(self.viewport.height as usize)
            .map(|line| fit_text(&line, self.viewport.width as usize))
            .collect()
    }

    fn render_overview_lines(&self) -> Vec<String> {
        let height = self.main_height();
        let mut lines = vec![format!(
            "Overview  view={}  breadcrumb={}",
            self.view.label(),
            self.stack
                .iter()
                .map(|opened| display_name_for_path(&opened.path))
                .collect::<Vec<_>>()
                .join(" / ")
        )];
        lines.extend(
            self.current()
                .overview_lines
                .iter()
                .skip(self.overview_scroll)
                .take(height.saturating_sub(1))
                .cloned(),
        );
        lines
    }

    fn render_columns_lines(&self) -> Vec<String> {
        let height = self.main_height();
        let mut lines = vec![format!(
            "Columns  selected={}/{}",
            self.columns_selected.saturating_add(1),
            self.current().columns.len()
        )];
        let visible = height.saturating_sub(1);
        for (index, entry) in self
            .current()
            .columns
            .iter()
            .enumerate()
            .skip(self.columns_scroll)
            .take(visible)
        {
            let marker = if index == self.columns_selected {
                ">"
            } else {
                " "
            };
            lines.push(format!("{marker} {:<18} {}", entry.name, entry.summary));
        }

        if height > 4 {
            lines.push(String::new());
            lines.push("-- Column Details --".to_string());
            if let Some(column) = self.current().columns.get(self.columns_selected) {
                let remaining = height.saturating_sub(lines.len());
                lines.extend(column.detail_lines.iter().take(remaining).cloned());
            }
        }
        lines
    }

    fn render_keywords_lines(&self) -> Vec<String> {
        let height = self.main_height();
        let mut lines = vec![format!(
            "Keywords  selected={}/{}  focus={:?}",
            self.keywords_selected.saturating_add(1),
            self.current().keyword_entries.len(),
            self.focus
        )];
        let visible = height.saturating_sub(1);
        for (index, entry) in self
            .current()
            .keyword_entries
            .iter()
            .enumerate()
            .skip(self.keywords_scroll)
            .take(visible)
        {
            let marker = if index == self.keywords_selected && self.focus == BrowserFocus::Main {
                ">"
            } else {
                " "
            };
            lines.push(format!(
                "{marker} {}{}{} = {}",
                keyword_owner_prefix(&entry.owner),
                "  ".repeat(entry.depth),
                entry_label(entry),
                entry.summary
            ));
        }
        lines
    }

    fn render_cells_lines(&self) -> Vec<String> {
        let width = self.viewport.width as usize;
        let height = self.main_height();
        let columns = &self.current().columns;
        let row_capacity = height.saturating_sub(2);
        let visible_columns = visible_cell_columns(columns, self.cells_column_offset, width);
        let visible_end = self.cells_row_scroll.saturating_add(row_capacity);
        let mut lines = vec![format!(
            "Cells  row={}/{}  col={}/{}  focus={:?}",
            self.cells_row_selected.saturating_add(1),
            self.current().table.row_count(),
            self.cells_column_selected.saturating_add(1),
            columns.len(),
            self.focus
        )];
        if columns.is_empty() {
            lines.push("<no columns>".to_string());
            return lines;
        }

        let mut header = String::from("row |");
        for &index in &visible_columns {
            let column = &columns[index];
            header.push(' ');
            header.push_str(&pad_or_truncate(&column.cell_header, column.width));
            header.push_str(" |");
        }
        lines.push(header);

        for row_index in self.cells_row_scroll..min(visible_end, self.current().table.row_count()) {
            let mut line =
                if row_index == self.cells_row_selected && self.focus == BrowserFocus::Main {
                    format!(">{row_index:>3} |")
                } else {
                    format!(" {row_index:>3} |")
                };
            for &index in &visible_columns {
                let column = &columns[index];
                let value = self
                    .current()
                    .table
                    .cell(row_index, &column.name)
                    .ok()
                    .flatten();
                let summary =
                    compact_optional_value_with_unit(value, column.display_unit.as_deref());
                let rendered = if row_index == self.cells_row_selected
                    && index == self.cells_column_selected
                {
                    highlight_selected_cell(&summary, column.width)
                } else {
                    pad_or_truncate(&summary, column.width)
                };
                line.push(' ');
                line.push_str(&rendered);
                line.push_str(" |");
            }
            lines.push(fit_text(&line, width));
        }
        lines
    }

    fn render_subtables_lines(&self) -> Vec<String> {
        let height = self.main_height();
        let mut lines = vec![format!(
            "Subtables  selected={}/{}",
            self.subtables_selected.saturating_add(1),
            self.current().linked_tables.len()
        )];
        let visible = height.saturating_sub(1);
        for (index, linked) in self
            .current()
            .linked_tables
            .iter()
            .enumerate()
            .skip(self.subtables_scroll)
            .take(visible)
        {
            let marker = if index == self.subtables_selected && self.focus == BrowserFocus::Main {
                ">"
            } else {
                " "
            };
            lines.push(format!(
                "{marker} [{}] {}  ({})",
                index, linked.label, linked.source
            ));
        }

        if height > 4 {
            lines.push(String::new());
            lines.push("-- Subtable Details --".to_string());
            if let Some(linked) = self.current().linked_tables.get(self.subtables_selected) {
                let remaining = height.saturating_sub(lines.len());
                let details = vec![
                    format!("Stored path: {}", linked.relative_path),
                    format!("Resolved path: {}", linked.resolved_path.display()),
                    format!("Source: {}", linked.source),
                    format!("Openable: {}", if linked.openable { "yes" } else { "no" }),
                    "Enter opens the selected linked table.".to_string(),
                ];
                lines.extend(details.into_iter().take(remaining));
            }
        }
        lines
    }

    fn render_inspector_snapshot(
        &self,
        context: &SelectionContext<'_>,
        inspector_height: usize,
    ) -> BrowserInspectorSnapshot {
        let title = match &context.address {
            BrowserAddress::Column { column, .. } => format!("Column {column}"),
            BrowserAddress::Cell { row, column, .. } => format!("Cell row={row} column={column}"),
            BrowserAddress::TableKeyword { keyword_path, .. } => {
                format!("Keyword {}", keyword_path.join("."))
            }
            BrowserAddress::ColumnKeyword {
                column,
                keyword_path,
                ..
            } => format!("Column keyword {column}:{}", keyword_path.join(".")),
            BrowserAddress::Subtable { target_path, .. } => format!("Subtable {target_path}"),
        };

        let node = self.build_value_node(context);
        let mut rendered_lines = vec![format!("-- Inspector ({:?}) --", self.focus)];
        rendered_lines.extend(render_trail(&context.trail));
        rendered_lines.extend(render_node_lines(&node));
        rendered_lines.truncate(inspector_height.max(1));

        BrowserInspectorSnapshot {
            title,
            trail: context.trail.clone(),
            node,
            rendered_lines,
        }
    }

    fn build_value_node(&self, context: &SelectionContext<'_>) -> BrowserValueNode {
        match &context.node {
            ValueNodeRef::Undefined => BrowserValueNode::Undefined,
            ValueNodeRef::Scalar(scalar) => BrowserValueNode::Scalar {
                value: scalar_to_protocol(scalar.as_ref()),
            },
            ValueNodeRef::Array(array) => {
                let total = array_total_len(array);
                let page_size = self.inspector_page_size();
                let start = self
                    .inspector_page_start
                    .min(total.saturating_sub(page_size));
                let end = min(total, start.saturating_add(page_size));
                let elements = array_elements(array, start, end, self.inspector_selected_child);
                BrowserValueNode::Array {
                    primitive: primitive_to_protocol(array.primitive_type()),
                    shape: array.shape().to_vec(),
                    total_elements: total,
                    page_start: start,
                    page_size,
                    elements,
                }
            }
            ValueNodeRef::Record(record) => {
                let total = record.fields().len();
                let page_size = self.inspector_page_size();
                let start = self
                    .inspector_page_start
                    .min(total.saturating_sub(page_size));
                let end = min(total, start.saturating_add(page_size));
                let fields = record
                    .fields()
                    .iter()
                    .enumerate()
                    .skip(start)
                    .take(end.saturating_sub(start))
                    .map(|(index, field)| BrowserRecordFieldSummary {
                        name: field.name.clone(),
                        kind: value_kind(&field.value),
                        summary: compact_value(&field.value),
                        expandable: matches!(field.value, Value::Record(_) | Value::Array(_)),
                        openable: matches!(field.value, Value::TableRef(_)),
                        selected: index == self.inspector_selected_child,
                    })
                    .collect();
                BrowserValueNode::Record {
                    total_fields: total,
                    page_start: start,
                    page_size,
                    fields,
                }
            }
            ValueNodeRef::TableRef(path) => {
                let resolved = resolve_linked_table_path(context.owner_path, path);
                BrowserValueNode::TableRef {
                    path: path.to_string(),
                    resolved_path: resolved.display().to_string(),
                    openable: resolved.exists(),
                }
            }
        }
    }
}

impl OpenedTable {
    fn open(path: &Path) -> Result<Self, TableBrowserError> {
        let table = Table::open(TableOptions::new(path))?;
        let path = table
            .path()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| path.to_path_buf());
        let columns = build_columns(&table)?;
        let keyword_entries = build_keyword_entries(&table);
        let linked_tables = collect_linked_tables(&table, &path);
        let overview_lines = build_overview_lines(&table, &columns, &linked_tables);
        Ok(Self {
            path,
            table,
            columns,
            keyword_entries,
            linked_tables,
            overview_lines,
        })
    }
}

fn build_overview_lines(
    table: &Table,
    columns: &[ColumnEntry],
    linked_tables: &[LinkedTableRef],
) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "Path: {}",
        table.path().map_or_else(
            || "<in-memory>".to_string(),
            |path| path.display().to_string()
        )
    ));
    lines.push(format!("Rows: {}", table.row_count()));
    lines.push(format!("Columns: {}", columns.len()));
    lines.push(format!("Linked tables: {}", linked_tables.len()));
    lines.push(String::new());
    lines.extend(table_info_lines(table.info()));

    if !table.data_manager_info().is_empty() {
        lines.push(String::new());
        lines.push("Data managers".to_string());
        for info in table.data_manager_info() {
            lines.push(format_data_manager_line(info));
        }
    }

    lines.push(String::new());
    lines.push("Columns".to_string());
    if columns.is_empty() {
        lines.push("  <none>".to_string());
    } else {
        for column in columns {
            lines.push(format!("  {} - {}", column.name, column.summary));
        }
    }
    lines
}

fn table_info_lines(info: &TableInfo) -> Vec<String> {
    vec![
        "Table info".to_string(),
        format!(
            "  Type: {}",
            if info.table_type.is_empty() {
                "<unset>"
            } else {
                &info.table_type
            }
        ),
        format!(
            "  SubType: {}",
            if info.sub_type.is_empty() {
                "<unset>"
            } else {
                &info.sub_type
            }
        ),
    ]
}

fn format_data_manager_line(info: &DataManagerInfo) -> String {
    format!(
        "  [{}] {} -> {}",
        info.seq_nr,
        info.dm_type,
        if info.columns.is_empty() {
            "<none>".to_string()
        } else {
            info.columns.join(", ")
        }
    )
}

fn build_columns(table: &Table) -> Result<Vec<ColumnEntry>, TableBrowserError> {
    let names = column_names(table)?;
    let sample_limit = table.row_count().min(SAMPLE_ROW_COUNT);
    let data_managers = table.data_manager_info();
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        let schema = table.schema().and_then(|schema| schema.column(&name));
        let type_label = schema.map(describe_schema_column_short);
        let unit_info = column_unit_info(table, &name);
        let cell_header =
            format_cell_header(&name, type_label.as_deref(), unit_info.heading.as_deref());
        let mut width = cell_header.chars().count().max(MIN_COLUMN_WIDTH);
        for row_index in 0..sample_limit {
            let value = table.cell(row_index, &name)?;
            let rendered =
                compact_optional_value_with_unit(value, unit_info.display_unit.as_deref());
            width = width.max(rendered.chars().count().min(MAX_COLUMN_WIDTH));
        }
        let summary = schema
            .map(describe_schema_column)
            .unwrap_or_else(|| "Dynamic".to_string());
        let keyword_count = table
            .column_keywords(&name)
            .map(|keywords| keywords.fields().len())
            .unwrap_or(0);
        let mut detail_lines = vec![
            format!("Name: {name}"),
            format!("Summary: {summary}"),
            format!("Column keywords: {keyword_count}"),
        ];
        if let Some(detail_line) = unit_info.detail_line.clone() {
            detail_lines.push(detail_line);
        }
        if let Some(schema) = schema {
            let options = schema.options();
            detail_lines.push(format!(
                "Flags: {}",
                render_column_flags(options.direct, options.undefined)
            ));
        }
        let managers = data_managers
            .iter()
            .filter(|info| info.columns.iter().any(|column| column == &name))
            .map(|info| format!("[{}] {}", info.seq_nr, info.dm_type))
            .collect::<Vec<_>>();
        detail_lines.push(format!(
            "Data managers: {}",
            if managers.is_empty() {
                "<none>".to_string()
            } else {
                managers.join(", ")
            }
        ));
        columns.push(ColumnEntry {
            name,
            cell_header,
            width: width.min(MAX_COLUMN_WIDTH),
            summary,
            detail_lines,
            display_unit: unit_info.display_unit,
        });
    }
    Ok(columns)
}

fn render_column_flags(direct: bool, undefined: bool) -> String {
    let mut flags = Vec::new();
    if direct {
        flags.push("direct");
    }
    if undefined {
        flags.push("undefined");
    }
    if flags.is_empty() {
        "<none>".to_string()
    } else {
        flags.join(", ")
    }
}

fn column_names(table: &Table) -> Result<Vec<String>, TableBrowserError> {
    if let Some(schema) = table.schema() {
        return Ok(schema
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect());
    }

    let mut names = BTreeSet::new();
    for row in table.rows()? {
        for field in row.fields() {
            names.insert(field.name.clone());
        }
    }
    Ok(names.into_iter().collect())
}

fn describe_schema_column(column: &crate::ColumnSchema) -> String {
    let mut line = String::new();
    match column.column_type() {
        ColumnType::Scalar => {
            let _ = write!(
                line,
                "Scalar {:?}",
                column
                    .data_type()
                    .expect("scalar columns always carry a data type")
            );
        }
        ColumnType::Array(contract) => {
            let _ = write!(
                line,
                "Array<{:?}> {:?}",
                column
                    .data_type()
                    .expect("array columns always carry a data type"),
                contract
            );
        }
        ColumnType::Record => line.push_str("Record"),
    }
    line
}

fn describe_schema_column_short(column: &crate::ColumnSchema) -> String {
    match column.column_type() {
        ColumnType::Scalar => column
            .data_type()
            .map(short_primitive_name)
            .unwrap_or("dyn")
            .to_string(),
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => format!(
            "{}[{}]",
            column
                .data_type()
                .map(short_primitive_name)
                .unwrap_or("dyn"),
            shape
                .iter()
                .map(|extent| extent.to_string())
                .collect::<Vec<_>>()
                .join("x")
        ),
        ColumnType::Array(ArrayShapeContract::Variable { ndim: Some(ndim) }) => format!(
            "{}[{}d]",
            column
                .data_type()
                .map(short_primitive_name)
                .unwrap_or("dyn"),
            ndim
        ),
        ColumnType::Array(ArrayShapeContract::Variable { ndim: None }) => format!(
            "{}[]",
            column
                .data_type()
                .map(short_primitive_name)
                .unwrap_or("dyn")
        ),
        ColumnType::Record => "record".to_string(),
    }
}

fn short_primitive_name(primitive: PrimitiveType) -> &'static str {
    match primitive {
        PrimitiveType::Bool => "bool",
        PrimitiveType::UInt8 => "u8",
        PrimitiveType::UInt16 => "u16",
        PrimitiveType::UInt32 => "u32",
        PrimitiveType::Int16 => "i16",
        PrimitiveType::Int32 => "i32",
        PrimitiveType::Int64 => "i64",
        PrimitiveType::Float32 => "f32",
        PrimitiveType::Float64 => "f64",
        PrimitiveType::Complex32 => "c32",
        PrimitiveType::Complex64 => "c64",
        PrimitiveType::String => "str",
    }
}

#[derive(Debug, Default)]
struct ColumnUnitInfo {
    heading: Option<String>,
    detail_line: Option<String>,
    display_unit: Option<String>,
}

fn column_unit_info(table: &Table, column: &str) -> ColumnUnitInfo {
    let Some(desc) = table.quantum_desc(column) else {
        return ColumnUnitInfo::default();
    };
    if let Some(unit_column) = desc.unit_column_name() {
        return ColumnUnitInfo {
            heading: Some(format!("[@{unit_column}]")),
            detail_line: Some(format!("Units column: {unit_column}")),
            display_unit: None,
        };
    }

    let units = desc.units();
    if units.is_empty() {
        return ColumnUnitInfo::default();
    }

    ColumnUnitInfo {
        heading: Some(format!("[{}]", summarize_units(units))),
        detail_line: Some(format!("Units: {}", units.join(", "))),
        display_unit: None,
    }
}

fn summarize_units(units: &[String]) -> String {
    if units.is_empty() {
        return String::new();
    }
    if units.windows(2).all(|window| window[0] == window[1]) {
        return units[0].clone();
    }
    units.join(",")
}

fn format_cell_header(name: &str, type_label: Option<&str>, unit_heading: Option<&str>) -> String {
    let mut header = name.to_string();
    if let Some(type_label) = type_label {
        header.push('<');
        header.push_str(type_label);
        header.push('>');
    }
    if let Some(unit_heading) = unit_heading {
        header.push_str(unit_heading);
    }
    header
}

fn build_keyword_entries(table: &Table) -> Vec<KeywordEntry> {
    let mut entries = Vec::new();
    push_keyword_entries(&mut entries, KeywordOwner::Table, &[], table.keywords());
    if let Some(schema) = table.schema() {
        for column in schema.columns() {
            if let Some(record) = table.column_keywords(column.name()) {
                push_keyword_entries(
                    &mut entries,
                    KeywordOwner::Column(column.name().to_string()),
                    &[],
                    record,
                );
            }
        }
    }
    entries
}

fn push_keyword_entries(
    entries: &mut Vec<KeywordEntry>,
    owner: KeywordOwner,
    prefix: &[String],
    record: &RecordValue,
) {
    for field in record.fields() {
        let mut path = prefix.to_vec();
        path.push(field.name.clone());
        entries.push(KeywordEntry {
            owner: owner.clone(),
            path: path.clone(),
            depth: path.len().saturating_sub(1),
            summary: compact_value(&field.value),
        });
        if let Value::Record(record) = &field.value {
            push_keyword_entries(entries, owner.clone(), &path, record);
        }
    }
}

fn lookup_keyword_value<'a>(opened: &'a OpenedTable, entry: &KeywordEntry) -> Option<&'a Value> {
    let record = match &entry.owner {
        KeywordOwner::Table => opened.table.keywords(),
        KeywordOwner::Column(column) => opened.table.column_keywords(column)?,
    };
    lookup_record_path(record, &entry.path)
}

fn lookup_record_path<'a>(record: &'a RecordValue, path: &[String]) -> Option<&'a Value> {
    let mut current_record = record;
    let mut current_value = None;
    for (index, segment) in path.iter().enumerate() {
        current_value = current_record.get(segment);
        let value = current_value?;
        if index + 1 < path.len() {
            let Value::Record(record) = value else {
                return None;
            };
            current_record = record;
        }
    }
    current_value
}

fn collect_linked_tables(table: &Table, owner_path: &Path) -> Vec<LinkedTableRef> {
    let mut discovered = Vec::new();
    let mut seen = BTreeSet::<PathBuf>::new();
    collect_record_links(
        table.keywords(),
        owner_path,
        "table keyword",
        &mut seen,
        &mut discovered,
    );
    if let Some(schema) = table.schema() {
        for column in schema.columns() {
            if let Some(keywords) = table.column_keywords(column.name()) {
                collect_record_links(
                    keywords,
                    owner_path,
                    &format!("column keyword {}", column.name()),
                    &mut seen,
                    &mut discovered,
                );
            }
        }
    }
    discovered.sort_by(|left, right| {
        left.label
            .cmp(&right.label)
            .then_with(|| left.source.cmp(&right.source))
    });
    discovered
}

fn collect_record_links(
    record: &RecordValue,
    owner_path: &Path,
    source_prefix: &str,
    seen: &mut BTreeSet<PathBuf>,
    discovered: &mut Vec<LinkedTableRef>,
) {
    for field in record.fields() {
        collect_value_links(
            &field.value,
            owner_path,
            &format!("{source_prefix}.{}", field.name),
            seen,
            discovered,
        );
    }
}

fn collect_value_links(
    value: &Value,
    owner_path: &Path,
    source: &str,
    seen: &mut BTreeSet<PathBuf>,
    discovered: &mut Vec<LinkedTableRef>,
) {
    match value {
        Value::Record(record) => collect_record_links(record, owner_path, source, seen, discovered),
        Value::TableRef(path) => {
            let resolved = resolve_linked_table_path(owner_path, path);
            if seen.insert(resolved.clone()) {
                discovered.push(LinkedTableRef {
                    label: display_name_for_path(&resolved),
                    relative_path: path.clone(),
                    openable: resolved.exists(),
                    resolved_path: resolved,
                    source: source.to_string(),
                });
            }
        }
        Value::Scalar(_) | Value::Array(_) => {}
    }
}

fn walk_optional_value_node<'a>(
    value: Option<&'a Value>,
    owner_path: &Path,
    inspector_path: &[ValuePathSegment],
) -> Option<(
    ValueNodeRef<'a>,
    Vec<BrowserInspectorTrailEntry>,
    Vec<ValuePathSegment>,
)> {
    match value {
        Some(value) => walk_value_node(value, owner_path, inspector_path),
        None => Some((ValueNodeRef::Undefined, Vec::new(), Vec::new())),
    }
}

fn walk_value_node<'a>(
    value: &'a Value,
    owner_path: &Path,
    inspector_path: &[ValuePathSegment],
) -> Option<(
    ValueNodeRef<'a>,
    Vec<BrowserInspectorTrailEntry>,
    Vec<ValuePathSegment>,
)> {
    let mut current = value;
    let mut trail = vec![BrowserInspectorTrailEntry {
        label: "root".to_string(),
        summary: compact_value(value),
    }];
    let mut applied = Vec::new();

    for segment in inspector_path {
        match segment {
            ValuePathSegment::RecordField { name } => {
                let Value::Record(record) = current else {
                    return None;
                };
                let next = record.get(name)?;
                trail.push(BrowserInspectorTrailEntry {
                    label: name.clone(),
                    summary: compact_value(next),
                });
                applied.push(ValuePathSegment::RecordField { name: name.clone() });
                current = next;
            }
            ValuePathSegment::ArrayIndex { flat_index } => {
                let Value::Array(array) = current else {
                    return None;
                };
                let scalar = scalar_at_flat_index_owned(array, *flat_index)?;
                trail.push(BrowserInspectorTrailEntry {
                    label: format!("#{flat_index}"),
                    summary: compact_scalar(&scalar),
                });
                applied.push(ValuePathSegment::ArrayIndex {
                    flat_index: *flat_index,
                });
                return Some((ValueNodeRef::Scalar(Cow::Owned(scalar)), trail, applied));
            }
        }
    }

    let node = match current {
        Value::Scalar(scalar) => ValueNodeRef::Scalar(Cow::Borrowed(scalar)),
        Value::Array(array) => ValueNodeRef::Array(array),
        Value::Record(record) => ValueNodeRef::Record(record),
        Value::TableRef(path) => {
            let _ = owner_path;
            ValueNodeRef::TableRef(path)
        }
    };
    Some((node, trail, applied))
}

fn render_trail(trail: &[BrowserInspectorTrailEntry]) -> Vec<String> {
    if trail.is_empty() {
        return vec!["Trail: <none>".to_string()];
    }
    vec![format!(
        "Trail: {}",
        trail
            .iter()
            .map(|entry| format!("{} ({})", entry.label, entry.summary))
            .collect::<Vec<_>>()
            .join(" -> ")
    )]
}

fn render_node_lines(node: &BrowserValueNode) -> Vec<String> {
    match node {
        BrowserValueNode::Undefined => {
            vec!["Type: undefined".to_string(), "Value: <undef>".to_string()]
        }
        BrowserValueNode::Scalar { value } => vec![
            format!("Type: {:?}", scalar_type_name(value)),
            format!("Value: {}", protocol_scalar_to_string(value)),
        ],
        BrowserValueNode::Array {
            primitive,
            shape,
            total_elements,
            page_start,
            page_size,
            elements,
        } => {
            let mut lines = vec![
                format!("Type: array<{primitive:?}> shape={shape:?}"),
                format!(
                    "Elements: {}..{} of {}",
                    page_start,
                    min(*total_elements, page_start.saturating_add(*page_size)),
                    total_elements
                ),
            ];
            if elements.is_empty() {
                lines.push("<empty>".to_string());
            } else {
                for element in elements {
                    let marker = if element.selected { ">" } else { " " };
                    lines.push(format!(
                        "{marker} {:?} = {}",
                        element.index,
                        protocol_scalar_to_string(&element.value)
                    ));
                }
            }
            lines
        }
        BrowserValueNode::Record {
            total_fields,
            page_start,
            page_size,
            fields,
        } => {
            let mut lines = vec![
                "Type: record".to_string(),
                format!(
                    "Fields: {}..{} of {}",
                    page_start,
                    min(*total_fields, page_start.saturating_add(*page_size)),
                    total_fields
                ),
            ];
            if fields.is_empty() {
                lines.push("<empty>".to_string());
            } else {
                for field in fields {
                    let marker = if field.selected { ">" } else { " " };
                    lines.push(format!("{marker} {} = {}", field.name, field.summary));
                }
            }
            lines
        }
        BrowserValueNode::TableRef {
            path,
            resolved_path,
            openable,
        } => vec![
            "Type: table_ref".to_string(),
            format!("Path: {path}"),
            format!("Resolved: {resolved_path}"),
            format!("Openable: {}", if *openable { "yes" } else { "no" }),
        ],
    }
}

fn entry_label(entry: &KeywordEntry) -> String {
    entry
        .path
        .last()
        .cloned()
        .unwrap_or_else(|| "<root>".to_string())
}

fn keyword_owner_prefix(owner: &KeywordOwner) -> String {
    match owner {
        KeywordOwner::Table => "table.".to_string(),
        KeywordOwner::Column(column) => format!("{column}."),
    }
}

fn resolve_linked_table_path(owner_path: &Path, relative: &str) -> PathBuf {
    let candidate = PathBuf::from(relative);
    if candidate.is_absolute() {
        return candidate;
    }

    let mut stripped = relative;
    let mut stripped_count = 0usize;
    while stripped.len() >= 2 && stripped.starts_with("./") {
        stripped = &stripped[2..];
        stripped_count += 1;
    }

    if stripped_count == 0 {
        let nested = owner_path.join(relative);
        if nested.exists() {
            return nested;
        }
        return owner_path.parent().unwrap_or(owner_path).join(relative);
    }
    if stripped_count == 1 {
        return owner_path.parent().unwrap_or(owner_path).join(stripped);
    }
    owner_path.join(stripped)
}

fn compact_optional_value_with_unit(value: Option<&Value>, unit: Option<&str>) -> String {
    let _ = unit;
    value
        .map(compact_value)
        .unwrap_or_else(|| "<undef>".to_string())
}

fn compact_value(value: &Value) -> String {
    match value {
        Value::Scalar(scalar) => compact_scalar(scalar),
        Value::Array(array) => compact_array(array),
        Value::Record(record) => format!("record{{{}}}", record.fields().len()),
        Value::TableRef(path) => format!("table({path})"),
    }
}

fn compact_scalar(scalar: &ScalarValue) -> String {
    match scalar {
        ScalarValue::Bool(value) => value.to_string(),
        ScalarValue::UInt8(value) => value.to_string(),
        ScalarValue::UInt16(value) => value.to_string(),
        ScalarValue::UInt32(value) => value.to_string(),
        ScalarValue::Int16(value) => value.to_string(),
        ScalarValue::Int32(value) => value.to_string(),
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::Float32(value) => format!("{value:.6}"),
        ScalarValue::Float64(value) => format!("{value:.6}"),
        ScalarValue::Complex32(value) => format!("{:.4}+{:.4}i", value.re, value.im),
        ScalarValue::Complex64(value) => format!("{:.4}+{:.4}i", value.re, value.im),
        ScalarValue::String(value) => format!("{value:?}"),
    }
}

fn compact_array(array: &ArrayValue) -> String {
    if array.ndim() <= 1 && array.len() <= 3 {
        return array_preview(array);
    }
    format!(
        "array<{:?}>{:?} {}",
        array.primitive_type(),
        array.shape(),
        array_preview(array)
    )
}

fn array_preview(array: &ArrayValue) -> String {
    match array {
        ArrayValue::Bool(values) => preview_iter(values.iter().map(|value| value.to_string())),
        ArrayValue::UInt8(values) => preview_iter(values.iter().map(|value| value.to_string())),
        ArrayValue::UInt16(values) => preview_iter(values.iter().map(|value| value.to_string())),
        ArrayValue::UInt32(values) => preview_iter(values.iter().map(|value| value.to_string())),
        ArrayValue::Int16(values) => preview_iter(values.iter().map(|value| value.to_string())),
        ArrayValue::Int32(values) => preview_iter(values.iter().map(|value| value.to_string())),
        ArrayValue::Int64(values) => preview_iter(values.iter().map(|value| value.to_string())),
        ArrayValue::Float32(values) => {
            preview_iter(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Float64(values) => {
            preview_iter(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Complex32(values) => preview_iter(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::Complex64(values) => preview_iter(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::String(values) => preview_iter(values.iter().map(|value| format!("{value:?}"))),
    }
}

fn preview_iter(values: impl Iterator<Item = String>) -> String {
    let mut preview = Vec::new();
    for value in values.take(4) {
        preview.push(value);
    }
    if preview.is_empty() {
        "[]".to_string()
    } else {
        format!(
            "[{}{}]",
            preview.join(", "),
            if preview.len() == 4 { ", ..." } else { "" }
        )
    }
}

fn highlight_selected_cell(text: &str, width: usize) -> String {
    if width <= 2 {
        return pad_or_truncate(text, width);
    }
    let decorated = format!(">{}<", truncate_chars(text, width.saturating_sub(2)));
    pad_or_truncate(&decorated, width)
}

fn cells_window_fits(
    columns: &[ColumnEntry],
    offset: usize,
    selected: usize,
    width: usize,
) -> bool {
    let visible = visible_cell_columns(columns, offset, width);
    !visible.is_empty() && visible.contains(&selected)
}

fn visible_cell_columns(columns: &[ColumnEntry], offset: usize, width: usize) -> Vec<usize> {
    if width == 0 {
        return Vec::new();
    }
    let mut used = 5usize;
    let mut visible = Vec::new();
    for (index, column) in columns.iter().enumerate().skip(offset) {
        let cell_width = column.width + 3;
        if !visible.is_empty() && used + cell_width > width {
            break;
        }
        visible.push(index);
        used = used.saturating_add(cell_width);
    }
    visible
}

fn adjust_index(index: usize, amount: usize, forward: bool) -> usize {
    if forward {
        index.saturating_add(amount.max(1))
    } else {
        index.saturating_sub(amount.max(1))
    }
}

fn clamp_scroll_for_selection(
    scroll: usize,
    selected: usize,
    viewport: usize,
    total: usize,
) -> usize {
    if total <= viewport || viewport == 0 {
        return 0;
    }
    let mut scroll = scroll.min(total.saturating_sub(viewport));
    if selected < scroll {
        scroll = selected;
    }
    let end = scroll.saturating_add(viewport);
    if selected >= end {
        scroll = selected.saturating_sub(viewport.saturating_sub(1));
    }
    scroll.min(total.saturating_sub(viewport))
}

fn pad_or_truncate(text: &str, width: usize) -> String {
    let truncated = truncate_chars(text, width);
    let padding = width.saturating_sub(truncated.chars().count());
    format!("{truncated}{}", " ".repeat(padding))
}

fn truncate_chars(text: &str, width: usize) -> String {
    let count = text.chars().count();
    if count <= width {
        return text.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    text.chars().take(width - 3).collect::<String>() + "..."
}

fn fit_text(text: &str, width: usize) -> String {
    truncate_chars(text, width)
}

fn display_name_for_path(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(ToString::to_string)
        .unwrap_or_else(|| path.display().to_string())
}

fn value_kind(value: &Value) -> BrowserValueKind {
    match value {
        Value::Scalar(_) => BrowserValueKind::Scalar,
        Value::Array(_) => BrowserValueKind::Array,
        Value::Record(_) => BrowserValueKind::Record,
        Value::TableRef(_) => BrowserValueKind::TableRef,
    }
}

fn primitive_to_protocol(primitive: PrimitiveType) -> BrowserPrimitiveType {
    match primitive {
        PrimitiveType::Bool => BrowserPrimitiveType::Bool,
        PrimitiveType::UInt8 => BrowserPrimitiveType::UInt8,
        PrimitiveType::UInt16 => BrowserPrimitiveType::UInt16,
        PrimitiveType::UInt32 => BrowserPrimitiveType::UInt32,
        PrimitiveType::Int16 => BrowserPrimitiveType::Int16,
        PrimitiveType::Int32 => BrowserPrimitiveType::Int32,
        PrimitiveType::Int64 => BrowserPrimitiveType::Int64,
        PrimitiveType::Float32 => BrowserPrimitiveType::Float32,
        PrimitiveType::Float64 => BrowserPrimitiveType::Float64,
        PrimitiveType::Complex32 => BrowserPrimitiveType::Complex32,
        PrimitiveType::Complex64 => BrowserPrimitiveType::Complex64,
        PrimitiveType::String => BrowserPrimitiveType::String,
    }
}

fn scalar_to_protocol(value: &ScalarValue) -> BrowserScalarValue {
    match value {
        ScalarValue::Bool(value) => BrowserScalarValue::Bool(*value),
        ScalarValue::UInt8(value) => BrowserScalarValue::UInt8(*value),
        ScalarValue::UInt16(value) => BrowserScalarValue::UInt16(*value),
        ScalarValue::UInt32(value) => BrowserScalarValue::UInt32(*value),
        ScalarValue::Int16(value) => BrowserScalarValue::Int16(*value),
        ScalarValue::Int32(value) => BrowserScalarValue::Int32(*value),
        ScalarValue::Int64(value) => BrowserScalarValue::Int64(*value),
        ScalarValue::Float32(value) => BrowserScalarValue::Float32(*value),
        ScalarValue::Float64(value) => BrowserScalarValue::Float64(*value),
        ScalarValue::Complex32(value) => BrowserScalarValue::Complex32(BrowserComplex32Value {
            re: value.re,
            im: value.im,
        }),
        ScalarValue::Complex64(value) => BrowserScalarValue::Complex64(BrowserComplex64Value {
            re: value.re,
            im: value.im,
        }),
        ScalarValue::String(value) => BrowserScalarValue::String(value.clone()),
    }
}

fn scalar_type_name(value: &BrowserScalarValue) -> BrowserPrimitiveType {
    match value {
        BrowserScalarValue::Bool(_) => BrowserPrimitiveType::Bool,
        BrowserScalarValue::UInt8(_) => BrowserPrimitiveType::UInt8,
        BrowserScalarValue::UInt16(_) => BrowserPrimitiveType::UInt16,
        BrowserScalarValue::UInt32(_) => BrowserPrimitiveType::UInt32,
        BrowserScalarValue::Int16(_) => BrowserPrimitiveType::Int16,
        BrowserScalarValue::Int32(_) => BrowserPrimitiveType::Int32,
        BrowserScalarValue::Int64(_) => BrowserPrimitiveType::Int64,
        BrowserScalarValue::Float32(_) => BrowserPrimitiveType::Float32,
        BrowserScalarValue::Float64(_) => BrowserPrimitiveType::Float64,
        BrowserScalarValue::Complex32(_) => BrowserPrimitiveType::Complex32,
        BrowserScalarValue::Complex64(_) => BrowserPrimitiveType::Complex64,
        BrowserScalarValue::String(_) => BrowserPrimitiveType::String,
    }
}

fn protocol_scalar_to_string(value: &BrowserScalarValue) -> String {
    match value {
        BrowserScalarValue::Bool(value) => value.to_string(),
        BrowserScalarValue::UInt8(value) => value.to_string(),
        BrowserScalarValue::UInt16(value) => value.to_string(),
        BrowserScalarValue::UInt32(value) => value.to_string(),
        BrowserScalarValue::Int16(value) => value.to_string(),
        BrowserScalarValue::Int32(value) => value.to_string(),
        BrowserScalarValue::Int64(value) => value.to_string(),
        BrowserScalarValue::Float32(value) => format!("{value:.6}"),
        BrowserScalarValue::Float64(value) => format!("{value:.6}"),
        BrowserScalarValue::Complex32(value) => format!("{:.4}+{:.4}i", value.re, value.im),
        BrowserScalarValue::Complex64(value) => format!("{:.4}+{:.4}i", value.re, value.im),
        BrowserScalarValue::String(value) => format!("{value:?}"),
    }
}

fn array_total_len(array: &ArrayValue) -> usize {
    array.shape().iter().product()
}

fn scalar_at_flat_index_owned(array: &ArrayValue, index: usize) -> Option<ScalarValue> {
    match array {
        ArrayValue::Bool(values) => values.iter().nth(index).copied().map(ScalarValue::Bool),
        ArrayValue::UInt8(values) => values.iter().nth(index).copied().map(ScalarValue::UInt8),
        ArrayValue::UInt16(values) => values.iter().nth(index).copied().map(ScalarValue::UInt16),
        ArrayValue::UInt32(values) => values.iter().nth(index).copied().map(ScalarValue::UInt32),
        ArrayValue::Int16(values) => values.iter().nth(index).copied().map(ScalarValue::Int16),
        ArrayValue::Int32(values) => values.iter().nth(index).copied().map(ScalarValue::Int32),
        ArrayValue::Int64(values) => values.iter().nth(index).copied().map(ScalarValue::Int64),
        ArrayValue::Float32(values) => values.iter().nth(index).copied().map(ScalarValue::Float32),
        ArrayValue::Float64(values) => values.iter().nth(index).copied().map(ScalarValue::Float64),
        ArrayValue::Complex32(values) => values
            .iter()
            .nth(index)
            .copied()
            .map(ScalarValue::Complex32),
        ArrayValue::Complex64(values) => values
            .iter()
            .nth(index)
            .copied()
            .map(ScalarValue::Complex64),
        ArrayValue::String(values) => values.iter().nth(index).cloned().map(ScalarValue::String),
    }
}

fn array_elements(
    array: &ArrayValue,
    start: usize,
    end: usize,
    selected: usize,
) -> Vec<BrowserArrayElement> {
    (start..end)
        .filter_map(|flat_index| {
            let scalar = scalar_at_flat_index_owned(array, flat_index)?;
            Some(BrowserArrayElement {
                flat_index,
                index: unravel_index(flat_index, array.shape()),
                value: scalar_to_protocol(&scalar),
                selected: flat_index == selected,
            })
        })
        .collect()
}

fn unravel_index(mut flat_index: usize, shape: &[usize]) -> Vec<usize> {
    if shape.is_empty() {
        return Vec::new();
    }
    let mut coords = vec![0; shape.len()];
    for (axis, extent) in shape.iter().enumerate().rev() {
        if *extent == 0 {
            coords[axis] = 0;
            continue;
        }
        coords[axis] = flat_index % extent;
        flat_index /= extent;
    }
    coords
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::time::{Duration, Instant};

    use super::*;

    use casacore_tablebrowser_protocol::{BrowserCommand, BrowserRequestEnvelope};
    use casacore_types::{
        ArrayD, Complex32, Complex64, RecordField, RecordValue, ScalarValue, Value,
    };
    use flate2::read::GzDecoder;
    use tempfile::tempdir;

    use crate::{ColumnOptions, ColumnSchema, TableSchema};

    #[test]
    fn browser_can_navigate_complex_values_and_linked_tables() {
        let temp = tempdir().expect("tempdir");
        let root = create_fixture_table(temp.path());
        let mut browser = TableBrowser::open(&root).expect("open browser");
        browser
            .apply(BrowserCommand::Resize {
                viewport: BrowserViewport::new(140, 28),
            })
            .expect("resize");

        browser.set_view(TableBrowserView::Cells);
        browser
            .select_cell(0, "complex_scalar")
            .expect("select complex cell");
        let scalar_snapshot = browser.snapshot();
        assert!(
            scalar_snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("complex_scalar"))
        );
        let scalar_inspector = scalar_snapshot.inspector.expect("scalar inspector");
        assert!(matches!(
            scalar_inspector.node,
            BrowserValueNode::Scalar {
                value: BrowserScalarValue::Complex32(_)
            }
        ));

        browser
            .select_cell(0, "complex_array")
            .expect("select complex array");
        browser
            .apply(BrowserCommand::Activate { viewport: None })
            .expect("activate array");
        let array_snapshot = browser.snapshot();
        let array_inspector = array_snapshot.inspector.expect("array inspector");
        assert!(matches!(
            array_inspector.node,
            BrowserValueNode::Array { .. }
        ));

        browser.set_view(TableBrowserView::Keywords);
        browser
            .select_table_keyword(&["NESTED".to_string(), "child".to_string()])
            .expect("select table keyword");
        let keyword_snapshot = browser.snapshot();
        assert!(matches!(
            keyword_snapshot.inspector.expect("keyword inspector").node,
            BrowserValueNode::TableRef { .. }
        ));

        browser.set_view(TableBrowserView::Subtables);
        assert_eq!(browser.selected_subtable_index(), Some(0));
        browser.open_selected_subtable().expect("open child table");
        assert!(browser.current_path().ends_with("child.tab"));
        assert!(browser.pop_table());
    }

    #[test]
    fn browser_request_envelope_round_trips_through_handle_request() {
        let temp = tempdir().expect("tempdir");
        let root = create_fixture_table(temp.path());
        let mut browser = TableBrowser::open(&root).expect("open browser");
        let response =
            browser.handle_request(BrowserRequestEnvelope::new(BrowserCommand::GetSnapshot {
                viewport: Some(BrowserViewport::new(120, 24)),
            }));
        match response.response {
            casacore_tablebrowser_protocol::BrowserResponse::Snapshot(snapshot) => {
                assert_eq!(snapshot.view, TableBrowserView::Overview);
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn browser_supports_full_value_matrix_and_cell_level_table_refs() {
        let temp = tempdir().expect("tempdir");
        let mut browser = create_type_matrix_browser(temp.path());
        browser
            .apply(BrowserCommand::Resize {
                viewport: BrowserViewport::new(180, 36),
            })
            .expect("resize");

        let scalar_expectations = [
            ("bool_scalar", BrowserPrimitiveType::Bool),
            ("u8_scalar", BrowserPrimitiveType::UInt8),
            ("u16_scalar", BrowserPrimitiveType::UInt16),
            ("u32_scalar", BrowserPrimitiveType::UInt32),
            ("i16_scalar", BrowserPrimitiveType::Int16),
            ("i32_scalar", BrowserPrimitiveType::Int32),
            ("i64_scalar", BrowserPrimitiveType::Int64),
            ("f32_scalar", BrowserPrimitiveType::Float32),
            ("f64_scalar", BrowserPrimitiveType::Float64),
            ("c32_scalar", BrowserPrimitiveType::Complex32),
            ("c64_scalar", BrowserPrimitiveType::Complex64),
            ("string_scalar", BrowserPrimitiveType::String),
        ];
        for (column, primitive) in scalar_expectations {
            browser
                .select_cell(0, column)
                .unwrap_or_else(|error| panic!("select scalar column {column}: {error}"));
            let snapshot = browser.snapshot();
            expect_scalar_node(
                snapshot.inspector.as_ref().expect("scalar inspector"),
                primitive,
            );
        }

        let array_expectations = [
            ("bool_array", BrowserPrimitiveType::Bool),
            ("u8_array", BrowserPrimitiveType::UInt8),
            ("u16_array", BrowserPrimitiveType::UInt16),
            ("u32_array", BrowserPrimitiveType::UInt32),
            ("i16_array", BrowserPrimitiveType::Int16),
            ("i32_array", BrowserPrimitiveType::Int32),
            ("i64_array", BrowserPrimitiveType::Int64),
            ("f32_array", BrowserPrimitiveType::Float32),
            ("f64_array", BrowserPrimitiveType::Float64),
            ("c32_array", BrowserPrimitiveType::Complex32),
            ("c64_array", BrowserPrimitiveType::Complex64),
            ("string_array", BrowserPrimitiveType::String),
        ];
        for (column, primitive) in array_expectations {
            browser
                .select_cell(0, column)
                .unwrap_or_else(|error| panic!("select array column {column}: {error}"));
            let snapshot = browser.snapshot();
            expect_array_node(
                snapshot.inspector.as_ref().expect("array inspector"),
                primitive,
            );
        }

        browser
            .select_cell(0, "undefined_scalar")
            .expect("select undefined");
        let undefined_snapshot = browser.snapshot();
        assert!(matches!(
            undefined_snapshot
                .inspector
                .expect("undefined inspector")
                .node,
            BrowserValueNode::Undefined
        ));

        browser
            .select_cell(0, "meta")
            .expect("select record column");
        let record_snapshot = browser.snapshot();
        let record_inspector = record_snapshot.inspector.expect("record inspector");
        assert!(matches!(
            record_inspector.node,
            BrowserValueNode::Record { .. }
        ));

        browser
            .apply(BrowserCommand::Activate { viewport: None })
            .expect("enter record inspector");
        let table_ref_snapshot = browser.snapshot();
        match table_ref_snapshot
            .inspector
            .expect("table ref selector")
            .node
        {
            BrowserValueNode::Record { fields, .. } => {
                assert_eq!(
                    fields.first().map(|field| field.name.as_str()),
                    Some("child_ref")
                );
                assert!(fields.first().is_some_and(|field| field.selected));
                assert!(fields.first().is_some_and(|field| field.openable));
            }
            other => panic!("unexpected record selector node {other:?}"),
        }

        browser
            .apply(BrowserCommand::Activate { viewport: None })
            .expect("open table ref from record cell");
        assert!(browser.current_path().ends_with("child.tab"));
        assert!(browser.pop_table());
    }

    #[test]
    fn browser_traverses_real_fixture_tables_and_cells() {
        let Some((_temp, root)) = unpack_fixture_ms("mssel_test_small.ms.tgz") else {
            return;
        };
        let mut browser = TableBrowser::open(&root).expect("open real fixture");
        browser
            .apply(BrowserCommand::Resize {
                viewport: BrowserViewport::new(160, 32),
            })
            .expect("resize");
        traverse_browser(&mut browser, &mut BTreeSet::new()).expect("traverse fixture");
    }

    #[test]
    fn browser_release_perf_targets_on_real_fixture() {
        if cfg!(debug_assertions) {
            eprintln!("[perf] skipping tablebrowser perf thresholds in debug mode");
            return;
        }

        let Some((_temp, root)) = unpack_fixture_ms("mssel_test_small.ms.tgz") else {
            return;
        };

        const INITIAL_OPEN_TARGET: Duration = Duration::from_millis(500);
        const STEADY_STATE_TARGET: Duration = Duration::from_millis(100);

        let start = Instant::now();
        let mut browser = TableBrowser::open(&root).expect("open real fixture browser");
        let open_elapsed = start.elapsed();

        let resize_elapsed = measure(|| {
            browser
                .apply(BrowserCommand::Resize {
                    viewport: BrowserViewport::new(160, 32),
                })
                .expect("resize");
        });

        browser.set_view(TableBrowserView::Cells);
        let scroll_down_elapsed = measure(|| {
            browser
                .apply(BrowserCommand::MoveDown {
                    steps: 1,
                    viewport: None,
                })
                .expect("move down");
        });
        let scroll_right_elapsed = measure(|| {
            browser
                .apply(BrowserCommand::MoveRight {
                    steps: 1,
                    viewport: None,
                })
                .expect("move right");
        });

        let inspector_expand_elapsed = select_first_expandable_cell(&mut browser)
            .map(|_| {
                measure(|| {
                    browser
                        .apply(BrowserCommand::Activate { viewport: None })
                        .expect("activate inspector");
                })
            })
            .unwrap_or_default();
        browser.escape();

        let openable_subtable = largest_openable_subtable_index(&mut browser);
        let subtable_open_elapsed = openable_subtable
            .map(|index| {
                browser.set_view(TableBrowserView::Subtables);
                assert!(browser.select_subtable(index));
                measure(|| {
                    browser
                        .open_selected_subtable()
                        .expect("open selected subtable")
                })
            })
            .unwrap_or_default();
        let subtable_back_elapsed = if openable_subtable.is_some() {
            measure(|| assert!(browser.pop_table()))
        } else {
            Duration::default()
        };

        eprintln!(
            "[perf] tablebrowser real fixture: open={open_elapsed:?} resize={resize_elapsed:?} \
scroll_down={scroll_down_elapsed:?} scroll_right={scroll_right_elapsed:?} \
inspector_expand={inspector_expand_elapsed:?} subtable_open={subtable_open_elapsed:?} \
subtable_back={subtable_back_elapsed:?}"
        );

        assert!(
            open_elapsed <= INITIAL_OPEN_TARGET,
            "initial open {open_elapsed:?} exceeded {INITIAL_OPEN_TARGET:?}"
        );
        for (label, elapsed) in [
            ("resize", resize_elapsed),
            ("scroll_down", scroll_down_elapsed),
            ("scroll_right", scroll_right_elapsed),
            ("inspector_expand", inspector_expand_elapsed),
            ("subtable_open", subtable_open_elapsed),
            ("subtable_back", subtable_back_elapsed),
        ] {
            if elapsed.is_zero() {
                continue;
            }
            assert!(
                elapsed <= STEADY_STATE_TARGET,
                "{label} {elapsed:?} exceeded {STEADY_STATE_TARGET:?}"
            );
        }
    }

    #[test]
    fn browser_cells_show_type_units_and_small_array_values() {
        let temp = tempdir().expect("tempdir");
        let root = create_quantum_fixture_table(temp.path());
        let mut browser = TableBrowser::open(&root).expect("open browser");
        browser
            .apply(BrowserCommand::Resize {
                viewport: BrowserViewport::new(180, 24),
            })
            .expect("resize");
        browser.set_view(TableBrowserView::Cells);

        let snapshot = browser.snapshot();
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("uvw<f64[3]>[m]"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("[1.0000, 2.0000"))
        );
        let inspector = snapshot.inspector.expect("array inspector");
        assert!(
            inspector
                .rendered_lines
                .iter()
                .any(|line| line.contains("[2] = 3.000000"))
        );

        browser.set_view(TableBrowserView::Columns);
        let columns_snapshot = browser.snapshot();
        assert!(
            columns_snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Units: m"))
        );
    }

    fn traverse_browser(
        browser: &mut TableBrowser,
        visited_tables: &mut BTreeSet<PathBuf>,
    ) -> Result<(), TableBrowserError> {
        let current = browser.current_path().to_path_buf();
        if !visited_tables.insert(current.clone()) {
            return Ok(());
        }

        browser.set_view(TableBrowserView::Columns);
        let column_count = browser.current().columns.len();
        for index in 0..column_count {
            browser.columns_selected = index;
            let snapshot = browser.snapshot();
            assert!(snapshot.selected_address.is_some());
        }

        browser.set_view(TableBrowserView::Keywords);
        let keyword_count = browser.current().keyword_entries.len();
        for index in 0..keyword_count {
            browser.keywords_selected = index;
            let snapshot = browser.snapshot();
            assert!(snapshot.selected_address.is_some());
            if let Some(inspector) = snapshot.inspector {
                match inspector.node {
                    BrowserValueNode::Record { total_fields, .. } => {
                        if total_fields > 0 {
                            browser
                                .apply(BrowserCommand::Activate { viewport: None })
                                .ok();
                            browser.escape();
                        }
                    }
                    BrowserValueNode::Array { total_elements, .. } => {
                        if total_elements > 0 {
                            browser
                                .apply(BrowserCommand::Activate { viewport: None })
                                .ok();
                            browser.escape();
                        }
                    }
                    BrowserValueNode::Scalar { .. }
                    | BrowserValueNode::TableRef { .. }
                    | BrowserValueNode::Undefined => {}
                }
            }
        }

        browser.set_view(TableBrowserView::Cells);
        let row_count = browser.current().table.row_count();
        let column_names = browser
            .current()
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        for row in 0..row_count {
            for column in &column_names {
                browser.select_cell(row, column)?;
                let snapshot = browser.snapshot();
                assert!(snapshot.selected_address.is_some());
                if let Some(inspector) = snapshot.inspector {
                    match inspector.node {
                        BrowserValueNode::Record { total_fields, .. } => {
                            if total_fields > 0 {
                                browser
                                    .apply(BrowserCommand::Activate { viewport: None })
                                    .ok();
                                browser.escape();
                            }
                        }
                        BrowserValueNode::Array { total_elements, .. } => {
                            if total_elements > 0 {
                                browser
                                    .apply(BrowserCommand::Activate { viewport: None })
                                    .ok();
                                browser.escape();
                            }
                        }
                        BrowserValueNode::TableRef { .. }
                        | BrowserValueNode::Scalar { .. }
                        | BrowserValueNode::Undefined => {}
                    }
                }
            }
        }

        browser.set_view(TableBrowserView::Subtables);
        let linked = browser.current().linked_tables.clone();
        for (index, linked_ref) in linked.iter().enumerate() {
            if !linked_ref.openable {
                continue;
            }
            browser.set_view(TableBrowserView::Subtables);
            browser.select_subtable(index);
            let before = browser.current_path().to_path_buf();
            browser.open_selected_subtable()?;
            assert_eq!(browser.current_path(), linked_ref.resolved_path.as_path());
            traverse_browser(browser, visited_tables)?;
            assert!(browser.pop_table());
            assert_eq!(browser.current_path(), before.as_path());
        }

        Ok(())
    }

    fn expect_scalar_node(inspector: &BrowserInspectorSnapshot, primitive: BrowserPrimitiveType) {
        match (&inspector.node, primitive) {
            (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::Bool(_),
                },
                BrowserPrimitiveType::Bool,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::UInt8(_),
                },
                BrowserPrimitiveType::UInt8,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::UInt16(_),
                },
                BrowserPrimitiveType::UInt16,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::UInt32(_),
                },
                BrowserPrimitiveType::UInt32,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::Int16(_),
                },
                BrowserPrimitiveType::Int16,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::Int32(_),
                },
                BrowserPrimitiveType::Int32,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::Int64(_),
                },
                BrowserPrimitiveType::Int64,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::Float32(_),
                },
                BrowserPrimitiveType::Float32,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::Float64(_),
                },
                BrowserPrimitiveType::Float64,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::Complex32(_),
                },
                BrowserPrimitiveType::Complex32,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::Complex64(_),
                },
                BrowserPrimitiveType::Complex64,
            )
            | (
                BrowserValueNode::Scalar {
                    value: BrowserScalarValue::String(_),
                },
                BrowserPrimitiveType::String,
            ) => {}
            other => panic!("unexpected scalar node {other:?}"),
        }
    }

    fn expect_array_node(inspector: &BrowserInspectorSnapshot, primitive: BrowserPrimitiveType) {
        match &inspector.node {
            BrowserValueNode::Array {
                primitive: node_primitive,
                shape,
                total_elements,
                ..
            } => {
                assert_eq!(*node_primitive, primitive);
                assert_eq!(shape, &vec![2, 2]);
                assert_eq!(*total_elements, 4);
            }
            other => panic!("unexpected array node {other:?}"),
        }
    }

    fn measure(operation: impl FnOnce()) -> Duration {
        let start = Instant::now();
        operation();
        start.elapsed()
    }

    fn select_first_expandable_cell(browser: &mut TableBrowser) -> Option<()> {
        let row_count = browser.current().table.row_count();
        let column_names = browser
            .current()
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        for row in 0..row_count {
            for column in &column_names {
                let value = match browser.current().table.cell(row, column) {
                    Ok(Some(value)) => value,
                    Ok(None) | Err(_) => continue,
                };
                if matches!(value, Value::Array(_) | Value::Record(_)) {
                    browser.select_cell(row, column).ok()?;
                    return Some(());
                }
            }
        }
        None
    }

    fn largest_openable_subtable_index(browser: &mut TableBrowser) -> Option<usize> {
        let linked = browser.current().linked_tables.clone();
        let mut best = None;
        let mut best_rows = 0usize;

        for (index, linked_ref) in linked.iter().enumerate() {
            if !linked_ref.openable {
                continue;
            }
            browser.set_view(TableBrowserView::Subtables);
            if !browser.select_subtable(index) {
                continue;
            }
            if browser.open_selected_subtable().is_err() {
                continue;
            }
            let row_count = browser.current().table.row_count();
            assert!(browser.pop_table(), "opened subtable should be poppable");
            if row_count > best_rows {
                best_rows = row_count;
                best = Some(index);
            }
        }

        best
    }

    fn create_fixture_table(root: &Path) -> PathBuf {
        let child_path = root.join("child.tab");
        let root_path = root.join("root.tab");

        let child_schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)])
            .expect("child schema");
        let mut child = Table::with_schema(child_schema);
        child
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(7)),
            )]))
            .expect("child row");
        child
            .save(TableOptions::new(&child_path))
            .expect("save child");

        let root_schema = TableSchema::new(vec![
            ColumnSchema::scalar("name", PrimitiveType::String),
            ColumnSchema::scalar("complex_scalar", PrimitiveType::Complex32),
            ColumnSchema::array_variable("complex_array", PrimitiveType::Complex64, Some(2)),
            ColumnSchema::record("meta"),
        ])
        .expect("root schema");
        let mut root_table = Table::with_schema(root_schema);
        let mut nested = RecordValue::default();
        nested.upsert("child", Value::table_ref("child.tab"));
        nested.upsert("flag", Value::Scalar(ScalarValue::Bool(true)));
        root_table
            .keywords_mut()
            .upsert("CHILD", Value::table_ref("child.tab"));
        root_table
            .keywords_mut()
            .upsert("NESTED", Value::Record(nested));
        root_table
            .add_row(RecordValue::new(vec![
                RecordField::new("name", Value::Scalar(ScalarValue::String("alpha".into()))),
                RecordField::new(
                    "complex_scalar",
                    Value::Scalar(ScalarValue::Complex32(Complex32::new(1.0, 2.0))),
                ),
                RecordField::new(
                    "complex_array",
                    Value::Array(ArrayValue::Complex64(
                        ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![
                                Complex64::new(1.0, -1.0),
                                Complex64::new(2.0, -2.0),
                                Complex64::new(3.0, -3.0),
                                Complex64::new(4.0, -4.0),
                            ],
                        )
                        .expect("array"),
                    )),
                ),
                RecordField::new(
                    "meta",
                    Value::Record(RecordValue::new(vec![
                        RecordField::new(
                            "note",
                            Value::Scalar(ScalarValue::String("hello".into())),
                        ),
                        RecordField::new("inner", Value::Scalar(ScalarValue::Int32(5))),
                    ])),
                ),
            ]))
            .expect("root row");
        root_table
            .save(TableOptions::new(&root_path))
            .expect("save root");
        root_path
    }

    fn create_quantum_fixture_table(root: &Path) -> PathBuf {
        let table_path = root.join("quantum.tab");
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "uvw",
            PrimitiveType::Float64,
            vec![3],
        )])
        .expect("quantum schema");
        let mut table = Table::with_schema(schema);
        let mut keywords = RecordValue::default();
        keywords.upsert(
            "QuantumUnits",
            Value::Array(ArrayValue::from_string_vec(vec!["m".to_string()])),
        );
        table.set_column_keywords("uvw", keywords);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "uvw",
                Value::Array(ArrayValue::from_f64_vec(vec![1.0, 2.0, 3.0])),
            )]))
            .expect("uvw row");
        table
            .save(TableOptions::new(&table_path))
            .expect("save quantum table");
        table_path
    }

    fn create_type_matrix_browser(root: &Path) -> TableBrowser {
        let child_path = root.join("child.tab");
        let root_path = root.join("type_matrix.tab");

        let child_schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)])
            .expect("child schema");
        let mut child = Table::with_schema(child_schema);
        child
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(99)),
            )]))
            .expect("child row");
        child
            .save(TableOptions::new(&child_path))
            .expect("save child");

        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("bool_scalar", PrimitiveType::Bool),
            ColumnSchema::scalar("u8_scalar", PrimitiveType::UInt8),
            ColumnSchema::scalar("u16_scalar", PrimitiveType::UInt16),
            ColumnSchema::scalar("u32_scalar", PrimitiveType::UInt32),
            ColumnSchema::scalar("i16_scalar", PrimitiveType::Int16),
            ColumnSchema::scalar("i32_scalar", PrimitiveType::Int32),
            ColumnSchema::scalar("i64_scalar", PrimitiveType::Int64),
            ColumnSchema::scalar("f32_scalar", PrimitiveType::Float32),
            ColumnSchema::scalar("f64_scalar", PrimitiveType::Float64),
            ColumnSchema::scalar("c32_scalar", PrimitiveType::Complex32),
            ColumnSchema::scalar("c64_scalar", PrimitiveType::Complex64),
            ColumnSchema::scalar("string_scalar", PrimitiveType::String),
            ColumnSchema::array_fixed("bool_array", PrimitiveType::Bool, vec![2, 2]),
            ColumnSchema::array_fixed("u8_array", PrimitiveType::UInt8, vec![2, 2]),
            ColumnSchema::array_fixed("u16_array", PrimitiveType::UInt16, vec![2, 2]),
            ColumnSchema::array_fixed("u32_array", PrimitiveType::UInt32, vec![2, 2]),
            ColumnSchema::array_fixed("i16_array", PrimitiveType::Int16, vec![2, 2]),
            ColumnSchema::array_fixed("i32_array", PrimitiveType::Int32, vec![2, 2]),
            ColumnSchema::array_fixed("i64_array", PrimitiveType::Int64, vec![2, 2]),
            ColumnSchema::array_fixed("f32_array", PrimitiveType::Float32, vec![2, 2]),
            ColumnSchema::array_fixed("f64_array", PrimitiveType::Float64, vec![2, 2]),
            ColumnSchema::array_fixed("c32_array", PrimitiveType::Complex32, vec![2, 2]),
            ColumnSchema::array_fixed("c64_array", PrimitiveType::Complex64, vec![2, 2]),
            ColumnSchema::array_fixed("string_array", PrimitiveType::String, vec![2, 2]),
            ColumnSchema::record("meta"),
        ])
        .expect("type matrix schema");
        let mut table = Table::with_schema(schema);

        let mut meta = RecordValue::default();
        meta.upsert("child_ref", Value::table_ref("child.tab"));
        meta.upsert(
            "nested",
            Value::Record(RecordValue::new(vec![RecordField::new(
                "answer",
                Value::Scalar(ScalarValue::Int32(42)),
            )])),
        );
        meta.upsert("flag", Value::Scalar(ScalarValue::Bool(true)));

        table
            .add_row(RecordValue::new(vec![
                RecordField::new("bool_scalar", Value::Scalar(ScalarValue::Bool(true))),
                RecordField::new("u8_scalar", Value::Scalar(ScalarValue::UInt8(8))),
                RecordField::new("u16_scalar", Value::Scalar(ScalarValue::UInt16(16))),
                RecordField::new("u32_scalar", Value::Scalar(ScalarValue::UInt32(32))),
                RecordField::new("i16_scalar", Value::Scalar(ScalarValue::Int16(-16))),
                RecordField::new("i32_scalar", Value::Scalar(ScalarValue::Int32(-32))),
                RecordField::new("i64_scalar", Value::Scalar(ScalarValue::Int64(-64))),
                RecordField::new("f32_scalar", Value::Scalar(ScalarValue::Float32(3.25))),
                RecordField::new("f64_scalar", Value::Scalar(ScalarValue::Float64(6.5))),
                RecordField::new(
                    "c32_scalar",
                    Value::Scalar(ScalarValue::Complex32(Complex32::new(1.0, -1.0))),
                ),
                RecordField::new(
                    "c64_scalar",
                    Value::Scalar(ScalarValue::Complex64(Complex64::new(2.0, -2.0))),
                ),
                RecordField::new(
                    "string_scalar",
                    Value::Scalar(ScalarValue::String("matrix".to_string())),
                ),
                RecordField::new(
                    "bool_array",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(vec![2, 2], vec![true, false, false, true])
                            .expect("bool array"),
                    )),
                ),
                RecordField::new(
                    "u8_array",
                    Value::Array(ArrayValue::UInt8(
                        ArrayD::from_shape_vec(vec![2, 2], vec![1, 2, 3, 4]).expect("u8 array"),
                    )),
                ),
                RecordField::new(
                    "u16_array",
                    Value::Array(ArrayValue::UInt16(
                        ArrayD::from_shape_vec(vec![2, 2], vec![1, 2, 3, 4]).expect("u16 array"),
                    )),
                ),
                RecordField::new(
                    "u32_array",
                    Value::Array(ArrayValue::UInt32(
                        ArrayD::from_shape_vec(vec![2, 2], vec![1, 2, 3, 4]).expect("u32 array"),
                    )),
                ),
                RecordField::new(
                    "i16_array",
                    Value::Array(ArrayValue::Int16(
                        ArrayD::from_shape_vec(vec![2, 2], vec![-1, -2, -3, -4])
                            .expect("i16 array"),
                    )),
                ),
                RecordField::new(
                    "i32_array",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2, 2], vec![-1, -2, -3, -4])
                            .expect("i32 array"),
                    )),
                ),
                RecordField::new(
                    "i64_array",
                    Value::Array(ArrayValue::Int64(
                        ArrayD::from_shape_vec(vec![2, 2], vec![-1, -2, -3, -4])
                            .expect("i64 array"),
                    )),
                ),
                RecordField::new(
                    "f32_array",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2, 2], vec![1.5, 2.5, 3.5, 4.5])
                            .expect("f32 array"),
                    )),
                ),
                RecordField::new(
                    "f64_array",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2, 2], vec![1.5, 2.5, 3.5, 4.5])
                            .expect("f64 array"),
                    )),
                ),
                RecordField::new(
                    "c32_array",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![
                                Complex32::new(1.0, -1.0),
                                Complex32::new(2.0, -2.0),
                                Complex32::new(3.0, -3.0),
                                Complex32::new(4.0, -4.0),
                            ],
                        )
                        .expect("c32 array"),
                    )),
                ),
                RecordField::new(
                    "c64_array",
                    Value::Array(ArrayValue::Complex64(
                        ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![
                                Complex64::new(1.0, -1.0),
                                Complex64::new(2.0, -2.0),
                                Complex64::new(3.0, -3.0),
                                Complex64::new(4.0, -4.0),
                            ],
                        )
                        .expect("c64 array"),
                    )),
                ),
                RecordField::new(
                    "string_array",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![
                                "one".to_string(),
                                "two".to_string(),
                                "three".to_string(),
                                "four".to_string(),
                            ],
                        )
                        .expect("string array"),
                    )),
                ),
                RecordField::new("meta", Value::Record(meta)),
            ]))
            .expect("type matrix row");

        let undefined_column = ColumnSchema::scalar("undefined_scalar", PrimitiveType::Float64)
            .with_options(ColumnOptions {
                direct: false,
                undefined: true,
            })
            .expect("undefined scalar column");
        table
            .add_column(undefined_column, None)
            .expect("add undefined column");
        table
            .keywords_mut()
            .upsert("CHILD", Value::table_ref("child.tab"));
        let columns = build_columns(&table).expect("build columns");
        let keyword_entries = build_keyword_entries(&table);
        let linked_tables = collect_linked_tables(&table, &root_path);
        let overview_lines = build_overview_lines(&table, &columns, &linked_tables);
        TableBrowser {
            stack: vec![OpenedTable {
                path: root_path.clone(),
                table,
                columns,
                keyword_entries,
                linked_tables,
                overview_lines,
            }],
            view: TableBrowserView::Overview,
            focus: BrowserFocus::Main,
            viewport: BrowserViewport::default(),
            status_line: format!("Browsing {}.", root_path.display()),
            overview_scroll: 0,
            columns_selected: 0,
            columns_scroll: 0,
            keywords_selected: 0,
            keywords_scroll: 0,
            cells_row_selected: 0,
            cells_row_scroll: 0,
            cells_column_selected: 0,
            cells_column_offset: 0,
            subtables_selected: 0,
            subtables_scroll: 0,
            inspector_path: Vec::new(),
            inspector_selected_child: 0,
            inspector_page_start: 0,
        }
    }

    fn unpack_fixture_ms(archive_name: &str) -> Option<(tempfile::TempDir, PathBuf)> {
        let archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../casacore-ms/tests/fixtures")
            .join(archive_name);
        if !archive_path.is_file() {
            return None;
        }
        let temp = tempdir().expect("tempdir");
        let archive_file = File::open(&archive_path).expect("open fixture archive");
        let decoder = GzDecoder::new(archive_file);
        let mut archive = tar::Archive::new(decoder);
        archive.unpack(temp.path()).expect("unpack fixture archive");
        let root = std::fs::read_dir(temp.path())
            .expect("read unpacked fixture dir")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.is_dir())
            .expect("fixture root directory");
        Some((temp, root))
    }
}
