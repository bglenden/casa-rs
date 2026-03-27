// SPDX-License-Identifier: LGPL-3.0-or-later
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use casacore_ms::listobs::cli::{
    UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiManagedOutputSchema, UiValueKind,
};
use casacore_ms::{ListObsSummary, ListObsUvCoverage};
use casacore_tablebrowser_protocol::{
    BrowserCommand, BrowserComplex32Value, BrowserComplex64Value, BrowserInspectorSnapshot,
    BrowserScalarValue, BrowserSnapshot, BrowserValueNode, BrowserView as TableBrowserView,
    BrowserViewport,
};
use casacore_types::quanta::{MvAngle, MvTime};
use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::Rect;
use ratatui_explorer::{FileExplorer, FileExplorerBuilder, Input as ExplorerInput};
use ratatui_graphics::{PanelProtocol, PanelRenderer, Picker, Resize};

use crate::browser_client::BrowserClient;
use crate::clipboard;
use crate::config::{ConfigStore, ThemeMode};
use crate::execution::{ExecutionEvent, ExecutionPlan, RunningProcess, spawn_process};
use crate::graphics::{UvPlotRenderInput, render_uv_plot, uv_plot_summary};
use crate::registry::RegistryApp;
use crate::ui::UiLayout;

const DENSE_SPINNER_FRAMES: &[&str] = &["|", "/", "-", "\\"];
const RICH_SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠴", "⠦"];
const DOUBLE_CLICK_WINDOW: Duration = Duration::from_millis(450);
const HORIZONTAL_SCROLL_STEP: i16 = 8;
const RESULT_TAB_COUNT: usize = 10;
const BROWSE_SUFFIX: &str = " [browse]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyProfile {
    Default,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Parameters,
    Result,
    Browser,
    Edit,
    PathChooser,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParameterAction {
    SelectPrevious,
    SelectNext,
    ChoicePrevious,
    ChoiceNext,
    Activate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResultAction {
    PreviousTab,
    NextTab,
    Scroll(i16),
    ScrollHorizontal(i16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrowserAction {
    CycleView { forward: bool },
    MoveLeft,
    MoveRight,
    MoveUp,
    MoveDown,
    PageUp,
    PageDown,
    Activate,
    Back,
    Escape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditAction {
    Cancel,
    Commit,
    DeleteBackward,
    Insert(char),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathChooserAction {
    Cancel,
    Confirm,
    SelectCurrent,
    Navigate(ExplorerInput),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppAction {
    Quit,
    BackToLauncher,
    ToggleTheme,
    ToggleParametersPane,
    CopySelection,
    ToggleFocus,
    StartRun,
    ToggleAdvanced,
    CancelSession,
    OpenPathChooser,
    ClearSelection,
    Parameter(ParameterAction),
    Result(ResultAction),
    Browser(BrowserAction),
    Edit(EditAction),
    PathChooser(PathChooserAction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PaneFocus {
    Parameters,
    Result,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FormSelection {
    Section(usize),
    Field(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ResultTab {
    Overview,
    Observations,
    Scans,
    Fields,
    Spws,
    Sources,
    Antennas,
    Uv,
    Stdout,
    Stderr,
}

impl ResultTab {
    pub(crate) const ALL: [Self; RESULT_TAB_COUNT] = [
        Self::Overview,
        Self::Observations,
        Self::Scans,
        Self::Fields,
        Self::Spws,
        Self::Sources,
        Self::Antennas,
        Self::Uv,
        Self::Stdout,
        Self::Stderr,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Observations => "Observations",
            Self::Scans => "Scans",
            Self::Fields => "Fields",
            Self::Spws => "SPWs",
            Self::Sources => "Sources",
            Self::Antennas => "Antennas",
            Self::Uv => "UV",
            Self::Stdout => "Stdout",
            Self::Stderr => "Stderr",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Overview => 0,
            Self::Observations => 1,
            Self::Scans => 2,
            Self::Fields => 3,
            Self::Spws => 4,
            Self::Sources => 5,
            Self::Antennas => 6,
            Self::Uv => 7,
            Self::Stdout => 8,
            Self::Stderr => 9,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FormRowView {
    pub target: FormSelection,
    pub text: String,
    pub kind: FormRowKind,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FormRowKind {
    Section { collapsed: bool },
    Field,
}

#[derive(Debug, Clone)]
pub(crate) struct TableView {
    pub header: String,
    pub rows: Vec<String>,
}

impl TableView {
    pub(crate) fn content_width(&self) -> usize {
        std::iter::once(self.header.chars().count())
            .chain(self.rows.iter().map(|row| row.chars().count()))
            .max()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ResultContent {
    Lines(Vec<String>),
    Table(TableView),
    Graphic(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BrowserTab {
    Overview,
    Columns,
    Keywords,
    Cells,
    Subtables,
}

impl BrowserTab {
    pub(crate) const ALL: [Self; 5] = [
        Self::Overview,
        Self::Columns,
        Self::Keywords,
        Self::Cells,
        Self::Subtables,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Columns => "Columns",
            Self::Keywords => "Keywords",
            Self::Cells => "Cells",
            Self::Subtables => "Subtables",
        }
    }

    fn from_view(view: TableBrowserView) -> Self {
        match view {
            TableBrowserView::Overview => Self::Overview,
            TableBrowserView::Columns => Self::Columns,
            TableBrowserView::Keywords => Self::Keywords,
            TableBrowserView::Cells => Self::Cells,
            TableBrowserView::Subtables => Self::Subtables,
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Overview => 0,
            Self::Columns => 1,
            Self::Keywords => 2,
            Self::Cells => 3,
            Self::Subtables => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputPane {
    Result,
    LeftOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSelectionMode {
    Pending,
    Dragging,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BufferPoint {
    row: usize,
    col: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OutputSelection {
    target: OutputPane,
    anchor: BufferPoint,
    cursor: BufferPoint,
    mode: OutputSelectionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VisibleTextRole {
    Plain,
    TableHeader,
    BrowserSeparator,
    BrowserSelectedCell,
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleTextLine {
    pub text: String,
    pub roles: Vec<VisibleTextRole>,
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleTextBuffer {
    pub area: Rect,
    pub lines: Vec<VisibleTextLine>,
}

#[derive(Debug)]
pub(crate) struct AppState {
    app: RegistryApp,
    config_store: ConfigStore,
    schema: Option<UiCommandSchema>,
    schema_error: Option<String>,
    fields: Vec<FormField>,
    sections: Vec<FormSection>,
    selected_form: FormSelection,
    show_advanced: bool,
    pane_focus: PaneFocus,
    edit_state: Option<EditState>,
    result: ResultState,
    active_result_tab: ResultTab,
    result_scrolls: [u16; RESULT_TAB_COUNT],
    result_hscrolls: [u16; RESULT_TAB_COUNT],
    running: Option<RunningState>,
    uv_loading: Option<UvCoverageLoadState>,
    uv_coverage: Option<ListObsUvCoverage>,
    uv_coverage_error: Option<String>,
    uv_plot_panel: Option<UvPlotPanelState>,
    last_listobs_uv_plan: Option<ExecutionPlan>,
    path_chooser: Option<PathChooserState>,
    browser_session: Option<BrowserSession>,
    spinner_frame: usize,
    dragging_divider: bool,
    dragging_result_scrollbar: bool,
    dragging_result_hscrollbar: bool,
    dragging_result_hscrollbar_grab: u16,
    output_selection: Option<OutputSelection>,
    cached_result_text_area: Option<Rect>,
    cached_left_output_area: Option<Rect>,
    last_click: Option<ClickState>,
    quit: bool,
    return_to_launcher: bool,
}

#[derive(Debug)]
struct RunningState {
    process: RunningProcess,
    renderer: Option<String>,
    file_output_path: Option<String>,
    uv_plan: Option<ExecutionPlan>,
    cancel_requested: bool,
}

#[derive(Debug)]
struct UvCoverageLoadState {
    process: RunningProcess,
    stdout: String,
    stderr: String,
    cancel_requested: bool,
}

#[derive(Debug)]
struct BrowserSession {
    client: BrowserClient,
    snapshot: BrowserSnapshot,
    viewport: BrowserViewport,
}

struct UvPlotPanelState {
    renderer: PanelRenderer<UvPlotRenderInput, String>,
    font_size: (u16, u16),
    request_key: Option<UvPlotRequestKey>,
    last_error: Option<String>,
    image_size: Option<(u32, u32)>,
}

#[derive(Debug)]
struct PathChooserState {
    field_index: usize,
    explorer: FileExplorer,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UvPlotRequestKey {
    area: Rect,
    theme_mode: ThemeMode,
    sample_count: usize,
    track_count: usize,
    max_abs_uv_bits: u64,
}

impl fmt::Debug for UvPlotPanelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UvPlotPanelState")
            .field("request_key", &self.request_key)
            .field("last_error", &self.last_error)
            .field("image_size", &self.image_size)
            .finish()
    }
}

#[derive(Debug, Default)]
struct ResultState {
    status_line: String,
    status_kind: StatusKind,
    stdout: String,
    stderr: String,
    structured: Option<ListObsSummary>,
    structured_error: Option<String>,
    file_output_path: Option<String>,
    exit_code: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum StatusKind {
    #[default]
    Info,
    Running,
    Ok,
    Error,
    Warning,
}

#[derive(Debug)]
struct EditState {
    field_index: usize,
    buffer: String,
}

#[derive(Debug)]
struct FormField {
    schema: UiArgumentSchema,
    value: FormValue,
}

#[derive(Debug)]
enum FormValue {
    Text(String),
    Toggle(bool),
    Choice { value: String, choices: Vec<String> },
}

#[derive(Debug)]
struct FormSection {
    name: String,
    field_indices: Vec<usize>,
    collapsed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClickTarget {
    Divider,
    ResultScrollbar,
    ResultHorizontalScrollbar,
    Pane(PaneFocus),
    Section(usize),
    Field(usize),
    Tab(ResultTab),
    BrowserTab(BrowserTab),
    PathChooserEntry(usize),
}

#[derive(Debug, Clone, Copy)]
struct ClickState {
    target: ClickTarget,
    at: Instant,
}

impl AppState {
    pub(crate) fn from_schema(app: RegistryApp, schema: UiCommandSchema) -> Self {
        Self::from_schema_with_config(app, schema, ConfigStore::load_default())
    }

    pub(crate) fn from_schema_with_config(
        app: RegistryApp,
        schema: UiCommandSchema,
        config_store: ConfigStore,
    ) -> Self {
        let ready_status_line = app.ready_status_line().to_string();
        let fields = schema
            .arguments
            .iter()
            .filter_map(FormField::from_schema)
            .collect::<Vec<_>>();
        let sections = build_sections(&fields);
        let selected_form = initial_form_selection(&sections, &fields, false);

        Self {
            app,
            config_store,
            schema: Some(schema),
            schema_error: None,
            fields,
            sections,
            selected_form,
            show_advanced: false,
            pane_focus: PaneFocus::Parameters,
            edit_state: None,
            result: ResultState {
                status_line: ready_status_line,
                status_kind: StatusKind::Info,
                ..ResultState::default()
            },
            active_result_tab: ResultTab::Overview,
            result_scrolls: [0; RESULT_TAB_COUNT],
            result_hscrolls: [0; RESULT_TAB_COUNT],
            running: None,
            uv_loading: None,
            uv_coverage: None,
            uv_coverage_error: None,
            uv_plot_panel: None,
            last_listobs_uv_plan: None,
            path_chooser: None,
            browser_session: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            output_selection: None,
            cached_result_text_area: None,
            cached_left_output_area: None,
            last_click: None,
            quit: false,
            return_to_launcher: false,
        }
    }

    pub(crate) fn schema_error(app: RegistryApp, error: String) -> Self {
        Self::schema_error_with_config(app, error, ConfigStore::load_default())
    }

    pub(crate) fn schema_error_with_config(
        app: RegistryApp,
        error: String,
        config_store: ConfigStore,
    ) -> Self {
        Self {
            app,
            config_store,
            schema: None,
            schema_error: Some(error.clone()),
            fields: Vec::new(),
            sections: Vec::new(),
            selected_form: FormSelection::Section(0),
            show_advanced: false,
            pane_focus: PaneFocus::Result,
            edit_state: None,
            result: ResultState {
                status_line: "Failed to load UI schema.".to_string(),
                status_kind: StatusKind::Error,
                stderr: format!("{error}\n"),
                structured_error: Some(error),
                ..ResultState::default()
            },
            active_result_tab: ResultTab::Stderr,
            result_scrolls: [0; RESULT_TAB_COUNT],
            result_hscrolls: [0; RESULT_TAB_COUNT],
            running: None,
            uv_loading: None,
            uv_coverage: None,
            uv_coverage_error: None,
            uv_plot_panel: None,
            last_listobs_uv_plan: None,
            path_chooser: None,
            browser_session: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            output_selection: None,
            cached_result_text_area: None,
            cached_left_output_area: None,
            last_click: None,
            quit: false,
            return_to_launcher: false,
        }
    }

    pub(crate) fn should_quit(&self) -> bool {
        self.quit
    }

    pub(crate) fn should_return_to_launcher(&self) -> bool {
        self.return_to_launcher
    }

    fn has_active_session(&self) -> bool {
        self.running.is_some() || self.browser_session.is_some() || self.uv_loading.is_some()
    }

    fn browser_session(&self) -> Option<&BrowserSession> {
        self.browser_session.as_ref()
    }

    pub(crate) fn path_chooser_active(&self) -> bool {
        self.path_chooser.is_some()
    }

    pub(crate) fn path_chooser_title(&self) -> Option<String> {
        let chooser = self.path_chooser.as_ref()?;
        let field = self.fields.get(chooser.field_index)?;
        Some(format!("Browse {}", field.schema.label))
    }

    pub(crate) fn path_chooser_cwd(&self) -> Option<String> {
        self.path_chooser
            .as_ref()
            .map(|chooser| chooser.explorer.cwd().display().to_string())
    }

    pub(crate) fn path_chooser_error(&self) -> Option<&str> {
        self.path_chooser
            .as_ref()
            .and_then(|chooser| chooser.last_error.as_deref())
    }

    pub(crate) fn path_chooser_entries(&self) -> Option<Vec<(String, bool)>> {
        let chooser = self.path_chooser.as_ref()?;
        let selected = chooser.explorer.selected_idx();
        Some(
            chooser
                .explorer
                .files()
                .iter()
                .enumerate()
                .map(|(index, file)| {
                    let icon = if file.is_dir { "▸" } else { " " };
                    (format!("{icon} {}", file.name), index == selected)
                })
                .collect(),
        )
    }

    fn selected_path_field_index(&self) -> Option<usize> {
        let FormSelection::Field(field_index) = self.selected_form else {
            return None;
        };
        self.fields
            .get(field_index)
            .filter(|field| field.is_path())
            .map(|_| field_index)
    }

    fn path_field_browse_hit(&self, field_index: usize, column: u16, layout: &UiLayout) -> bool {
        let Some(field) = self.fields.get(field_index) else {
            return false;
        };
        if !field.is_path() {
            return false;
        }
        let Some(row) = layout
            .form_rows
            .iter()
            .find(|row| row.target == FormSelection::Field(field_index))
        else {
            return false;
        };
        let text = field.render_line(self.edit_state.as_ref(), field_index);
        let browse_len = BROWSE_SUFFIX.chars().count() as u16;
        let text_end = row
            .rect
            .x
            .saturating_add(text.chars().count().min(row.rect.width as usize) as u16);
        let browse_start = text_end.saturating_sub(browse_len);
        column >= browse_start && column < text_end
    }

    fn open_path_chooser_for_selected_field(&mut self) {
        let Some(field_index) = self.selected_path_field_index() else {
            return;
        };
        self.open_path_chooser(field_index);
    }

    fn open_path_chooser(&mut self, field_index: usize) {
        self.prepare_path_chooser_field(field_index);
        let Some(field) = self.fields.get(field_index) else {
            return;
        };
        let start = chooser_start_path(field.text_value().as_deref());
        let start_is_dir = start.is_dir();
        let builder = if start_is_dir {
            FileExplorerBuilder::default().working_dir(start)
        } else {
            FileExplorerBuilder::default().working_file(start)
        };
        match builder.show_hidden(false).build() {
            Ok(mut explorer) => {
                if start_is_dir && explorer.current().name == "../" && explorer.files().len() > 1 {
                    explorer.set_selected_idx(1);
                }
                self.path_chooser = Some(PathChooserState {
                    field_index,
                    explorer,
                    last_error: None,
                });
            }
            Err(error) => {
                self.result.status_line = "Failed to open path chooser.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr.push_str(&format!("{error}\n"));
            }
        }
    }

    fn prepare_path_chooser_field(&mut self, field_index: usize) {
        let Some(edit_state) = self.edit_state.take() else {
            return;
        };
        if edit_state.field_index == field_index {
            if let Some(field) = self.fields.get_mut(field_index) {
                field.set_text(edit_state.buffer);
                self.clear_cached_uv_coverage();
            }
        } else {
            self.edit_state = Some(edit_state);
        }
    }

    fn close_path_chooser(&mut self) {
        self.path_chooser = None;
    }

    fn confirm_path_chooser(&mut self) {
        let Some((field_index, selected_path)) = self
            .path_chooser
            .as_ref()
            .map(|chooser| (chooser.field_index, chooser.explorer.current().path.clone()))
        else {
            return;
        };
        self.select_path_chooser_path(field_index, &selected_path);
    }

    fn select_current_path_chooser_entry(&mut self) {
        let Some((field_index, selected_path)) = self
            .path_chooser
            .as_ref()
            .map(|chooser| (chooser.field_index, chooser.explorer.current().path.clone()))
        else {
            return;
        };
        self.select_path_chooser_path(field_index, &selected_path);
    }

    fn select_path_chooser_path(&mut self, selected_field_index: usize, selected_path: &Path) {
        let value = absolute_display_path(selected_path);
        if let Some(field) = self.fields.get_mut(selected_field_index) {
            field.set_text(value.clone());
            self.clear_cached_uv_coverage();
        }
        self.close_path_chooser();
        self.result.status_line = format!("Selected path: {value}");
        self.result.status_kind = StatusKind::Ok;
    }

    fn cancel_path_chooser(&mut self) {
        self.close_path_chooser();
        self.result.status_line = "Path chooser canceled.".to_string();
        self.result.status_kind = StatusKind::Info;
    }

    fn apply_path_chooser_input(&mut self, input: ExplorerInput) {
        let Some(chooser) = self.path_chooser.as_mut() else {
            return;
        };
        chooser.last_error = None;
        if let Err(error) = chooser.explorer.handle(input) {
            chooser.last_error = Some(error.to_string());
        }
    }

    pub(crate) fn on_tick(&mut self) {
        self.spinner_frame = (self.spinner_frame + 1) % spinner_frames(self.theme_mode()).len();
        self.pump_uv_plot_panel();
    }

    fn key_profile(&self) -> KeyProfile {
        KeyProfile::Default
    }

    fn input_mode(&self) -> InputMode {
        if self.path_chooser.is_some() {
            InputMode::PathChooser
        } else if self.edit_state.is_some() {
            InputMode::Edit
        } else if self.browser_session.is_some() {
            InputMode::Browser
        } else {
            match self.pane_focus {
                PaneFocus::Parameters => InputMode::Parameters,
                PaneFocus::Result => InputMode::Result,
            }
        }
    }

    fn resolve_key_action(&self, key_event: KeyEvent) -> Option<AppAction> {
        if key_event.kind != KeyEventKind::Press {
            return None;
        }
        match self.key_profile() {
            KeyProfile::Default => self.resolve_default_key_action(key_event),
        }
    }

    fn resolve_default_key_action(&self, key_event: KeyEvent) -> Option<AppAction> {
        let mode = self.input_mode();

        if mode == InputMode::PathChooser {
            return match key_event.code {
                KeyCode::Esc => Some(AppAction::PathChooser(PathChooserAction::Cancel)),
                KeyCode::Enter if key_event.modifiers.is_empty() => {
                    Some(AppAction::PathChooser(PathChooserAction::Confirm))
                }
                KeyCode::Char(' ') if key_event.modifiers.is_empty() => {
                    Some(AppAction::PathChooser(PathChooserAction::SelectCurrent))
                }
                _ => chooser_input_from_key(key_event)
                    .map(PathChooserAction::Navigate)
                    .map(AppAction::PathChooser),
            };
        }

        match key_event.code {
            KeyCode::Char('q') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return Some(AppAction::Quit);
            }
            KeyCode::Char('t') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return Some(AppAction::ToggleTheme);
            }
            KeyCode::Char('x') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return Some(AppAction::CancelSession);
            }
            KeyCode::Char('b') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return Some(AppAction::BackToLauncher);
            }
            KeyCode::Char('p') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return Some(AppAction::ToggleParametersPane);
            }
            KeyCode::Char('y') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return Some(AppAction::CopySelection);
            }
            KeyCode::Char('c')
                if is_browser_copy_modifier(key_event.modifiers) && mode != InputMode::Edit =>
            {
                return Some(AppAction::CopySelection);
            }
            KeyCode::Char('r')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && !self.has_active_session() =>
            {
                return Some(AppAction::StartRun);
            }
            KeyCode::Char('a')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && !self.has_active_session() =>
            {
                return Some(AppAction::ToggleAdvanced);
            }
            KeyCode::Char('o')
                if key_event.modifiers == KeyModifiers::CONTROL
                    && mode != InputMode::Edit
                    && !self.has_active_session() =>
            {
                return Some(AppAction::OpenPathChooser);
            }
            KeyCode::Tab | KeyCode::BackTab if mode == InputMode::Browser => {
                return Some(AppAction::Browser(BrowserAction::CycleView {
                    forward: key_event.code == KeyCode::Tab,
                }));
            }
            KeyCode::Tab | KeyCode::BackTab => return Some(AppAction::ToggleFocus),
            _ => {}
        }

        if key_event.code == KeyCode::Esc && self.output_selection.is_some() {
            return Some(AppAction::ClearSelection);
        }

        match mode {
            InputMode::Parameters => resolve_parameter_action(key_event).map(AppAction::Parameter),
            InputMode::Result => resolve_result_action(key_event).map(AppAction::Result),
            InputMode::Browser => resolve_browser_action(key_event).map(AppAction::Browser),
            InputMode::Edit => resolve_edit_action(key_event).map(AppAction::Edit),
            InputMode::PathChooser => None,
        }
    }

    fn apply_action(&mut self, action: AppAction) {
        match action {
            AppAction::Quit => {
                if self.has_active_session() {
                    self.cancel_current();
                }
                self.quit = true;
            }
            AppAction::BackToLauncher => {
                if self.has_active_session() {
                    self.cancel_current();
                }
                self.return_to_launcher = true;
            }
            AppAction::ToggleTheme => self.toggle_theme(),
            AppAction::ToggleParametersPane => self.toggle_parameters_pane(),
            AppAction::CopySelection => self.copy_output_selection(),
            AppAction::ToggleFocus => self.toggle_focus(),
            AppAction::StartRun => {
                if !self.has_active_session() {
                    self.start_run();
                }
            }
            AppAction::ToggleAdvanced => self.toggle_advanced(),
            AppAction::CancelSession => self.cancel_current(),
            AppAction::OpenPathChooser => self.open_path_chooser_for_selected_field(),
            AppAction::ClearSelection => self.clear_output_selection(),
            AppAction::Parameter(action) => self.apply_parameter_action(action),
            AppAction::Result(action) => self.apply_result_action(action),
            AppAction::Browser(action) => self.apply_browser_action(action),
            AppAction::Edit(action) => self.apply_edit_action(action),
            AppAction::PathChooser(action) => self.apply_path_chooser_action(action),
        }
    }

    pub(crate) fn handle_key_event(&mut self, key_event: KeyEvent) {
        if let Some(action) = self.resolve_key_action(key_event) {
            self.apply_action(action);
        }
    }

    pub(crate) fn handle_paste(&mut self, text: String) {
        let pasted = text.trim_end_matches(['\r', '\n']).to_string();
        if pasted.is_empty() {
            return;
        }

        if let Some(edit_state) = self.edit_state.as_mut() {
            edit_state.buffer.push_str(&pasted);
            return;
        }

        let FormSelection::Field(field_index) = self.selected_form else {
            return;
        };
        let Some(field) = self.fields.get_mut(field_index) else {
            return;
        };
        if matches!(field.value, FormValue::Text(_)) {
            field.set_text(pasted);
        }
    }

    pub(crate) fn handle_mouse_event(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        self.cache_output_layout(layout);
        if self.path_chooser.is_some() {
            self.handle_path_chooser_mouse(mouse_event, layout);
            return;
        }
        match mouse_event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                self.handle_left_mouse_down(mouse_event, layout)
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.finalize_output_selection();
                self.dragging_divider = false;
                self.dragging_result_scrollbar = false;
                self.dragging_result_hscrollbar = false;
                self.dragging_result_hscrollbar_grab = 0;
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                self.handle_left_mouse_drag(mouse_event, layout)
            }
            MouseEventKind::ScrollUp => self.handle_mouse_scroll(mouse_event, layout, -3),
            MouseEventKind::ScrollDown => self.handle_mouse_scroll(mouse_event, layout, 3),
            MouseEventKind::ScrollLeft => {
                self.handle_mouse_hscroll(mouse_event, layout, -HORIZONTAL_SCROLL_STEP)
            }
            MouseEventKind::ScrollRight => {
                self.handle_mouse_hscroll(mouse_event, layout, HORIZONTAL_SCROLL_STEP)
            }
            _ => {}
        }
    }

    pub(crate) fn drain_execution_events(&mut self) {
        while let Some(running) = self.running.as_ref() {
            let event = running.process.try_recv();
            match event {
                Ok(ExecutionEvent::Stdout(chunk)) => self.result.stdout.push_str(&chunk),
                Ok(ExecutionEvent::Stderr(chunk)) => self.result.stderr.push_str(&chunk),
                Ok(ExecutionEvent::Exited(exit)) => {
                    self.finish_execution(exit.code, exit.success);
                    break;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    self.result.status_line =
                        "Execution channel disconnected unexpectedly.".to_string();
                    self.result.status_kind = StatusKind::Error;
                    self.running = None;
                    break;
                }
            }
        }
        self.drain_uv_loading_events();
    }

    pub(crate) fn app_category(&self) -> &str {
        self.app.category
    }

    pub(crate) fn app_name(&self) -> &str {
        self.app.display_name
    }

    pub(crate) fn footer_text(&self) -> &'static str {
        if self.edit_state.is_some() {
            "Tab pane  Enter save  Esc cancel  Bksp delete  p pane  t theme  q quit"
        } else if self.browser_session.is_some() {
            "Tab view  Arrows  PgUp/PgDn  Enter  y copy  Esc back/clear  Bksp parent table  p pane  b apps  x close  t theme  q quit"
        } else if self.running.is_some() {
            "Tab pane  h/l tabs  j/k scroll  [/] hscr  y copy  p pane  b apps  x cancel  t theme  q quit"
        } else {
            "Tab pane  h/l tabs  Arrows/jk  [/] hscr  y copy  a adv  p pane  b apps  r run  t theme  q quit"
        }
    }

    pub(crate) fn parameter_title(&self) -> String {
        let focus = if self.pane_focus == PaneFocus::Parameters {
            " [focus]"
        } else {
            ""
        };
        let title = match self.theme_mode() {
            ThemeMode::DenseAnsi => "Parameters",
            ThemeMode::RichPanel => "◈ Parameters",
        };
        if self.running.is_some() {
            format!(
                "{title} [{}] locked{}",
                spinner_frames(self.theme_mode())[self.spinner_frame],
                focus
            )
        } else if self.browser_session.is_some() {
            match self.theme_mode() {
                ThemeMode::DenseAnsi => format!("Inspector [live]{focus}"),
                ThemeMode::RichPanel => format!("◈ Inspector [live]{focus}"),
            }
        } else if self.schema_error.is_some() {
            format!("{title} (schema unavailable){focus}")
        } else {
            format!("{title}{focus}")
        }
    }

    pub(crate) fn result_title(&self) -> String {
        let focus = if self.pane_focus == PaneFocus::Result {
            " [focus]"
        } else {
            ""
        };
        let title = match self.theme_mode() {
            ThemeMode::DenseAnsi => "Result",
            ThemeMode::RichPanel => "◉ Result",
        };
        format!("{title}{focus}")
    }

    pub(crate) fn theme_mode(&self) -> ThemeMode {
        self.config_store.theme_mode()
    }

    pub(crate) fn pane_split_ratio(&self) -> f32 {
        self.config_store.pane_split_ratio()
    }

    pub(crate) fn parameters_pane_collapsed(&self) -> bool {
        self.pane_split_ratio() <= 0.0
    }

    pub(crate) fn pane_focus(&self) -> PaneFocus {
        self.pane_focus
    }

    pub(crate) fn form_rows(&self) -> Vec<FormRowView> {
        if self.browser_session.is_some() {
            return self.browser_inspector_rows();
        }

        let mut rows = Vec::new();
        for (section_index, section) in self.sections.iter().enumerate() {
            let visible_fields = section
                .field_indices
                .iter()
                .copied()
                .filter(|index| self.show_advanced || !self.fields[*index].schema.advanced)
                .collect::<Vec<_>>();
            if visible_fields.is_empty() {
                continue;
            }

            rows.push(FormRowView {
                target: FormSelection::Section(section_index),
                text: section.name.clone(),
                kind: FormRowKind::Section {
                    collapsed: section.collapsed,
                },
                selected: self.selected_form == FormSelection::Section(section_index),
            });

            if section.collapsed {
                continue;
            }

            for field_index in visible_fields {
                rows.push(FormRowView {
                    target: FormSelection::Field(field_index),
                    text: self.fields[field_index]
                        .render_line(self.edit_state.as_ref(), field_index),
                    kind: FormRowKind::Field,
                    selected: self.selected_form == FormSelection::Field(field_index),
                });
            }
        }

        if rows.is_empty() {
            rows.push(FormRowView {
                target: FormSelection::Section(0),
                text: "No editable parameters available.".to_string(),
                kind: FormRowKind::Field,
                selected: false,
            });
        }

        rows
    }

    pub(crate) fn active_result_tab(&self) -> ResultTab {
        self.active_result_tab
    }

    pub(crate) fn active_result_scroll(&self) -> u16 {
        self.result_scrolls[self.active_result_tab.index()]
    }

    pub(crate) fn active_result_hscroll(&self) -> u16 {
        self.result_hscrolls[self.active_result_tab.index()]
    }

    pub(crate) fn result_tabs(&self) -> &'static [ResultTab] {
        self.visible_result_tabs()
    }

    pub(crate) fn result_status_lines(&self) -> Vec<String> {
        let mut lines = vec![format!("Status: {}", self.result.status_line)];
        if let Some(code) = self.result.exit_code {
            lines.push(format!("Exit code: {code}"));
        } else if self.running.is_some() {
            lines.push("Exit code: running".to_string());
        } else {
            lines.push(String::new());
        }

        if let Some(path) = &self.result.file_output_path {
            lines.push(format!("Output: {path}"));
        } else if let Some(error) = &self.result.structured_error {
            lines.push(error.clone());
        } else if let Some(error) = &self.schema_error {
            lines.push(error.clone());
        } else if self.running.is_some() {
            lines.push("Structured output will appear when the subprocess exits.".to_string());
        } else if let Some(session) = self.browser_session() {
            lines.push(format!(
                "View: {}  Table: {}  Breadcrumb: {}",
                session.snapshot.view.label(),
                session.snapshot.table_path,
                session
                    .snapshot
                    .breadcrumb
                    .iter()
                    .map(|entry| entry.label.as_str())
                    .collect::<Vec<_>>()
                    .join(" / ")
            ));
        } else {
            lines.push(format!(
                "View: {}  Theme: {}  Verbose: {}",
                self.active_result_tab.label(),
                match self.theme_mode() {
                    ThemeMode::DenseAnsi => "dense_ansi",
                    ThemeMode::RichPanel => "rich_panel",
                },
                if self.verbose_enabled() { "on" } else { "off" }
            ));
        }
        lines
    }

    pub(crate) fn result_status_kind(&self) -> &'static str {
        match self.result.status_kind {
            StatusKind::Info => "info",
            StatusKind::Running => "running",
            StatusKind::Ok => "ok",
            StatusKind::Error => "error",
            StatusKind::Warning => "warning",
        }
    }

    pub(crate) fn browser_is_active(&self) -> bool {
        self.browser_session.is_some()
    }

    pub(crate) fn browser_tabs(&self) -> &'static [BrowserTab] {
        &BrowserTab::ALL
    }

    pub(crate) fn active_browser_tab_label(&self) -> Option<&'static str> {
        self.active_browser_tab().map(BrowserTab::label)
    }

    pub(crate) fn active_browser_scroll_metrics(
        &self,
        _viewport_height: u16,
    ) -> Option<(usize, usize)> {
        let session = self.browser_session()?;
        session
            .snapshot
            .vertical_metrics
            .map(|metrics| (metrics.total_items, metrics.viewport_items.max(1)))
    }

    pub(crate) fn active_browser_hscroll_metrics(
        &self,
        _viewport_width: u16,
    ) -> Option<(usize, usize)> {
        let session = self.browser_session()?;
        session
            .snapshot
            .horizontal_metrics
            .map(|metrics| (metrics.total_items, metrics.viewport_items.max(1)))
    }

    pub(crate) fn browser_inspector_lines(&self) -> Option<Vec<String>> {
        let session = self.browser_session()?;
        let inspector = session.snapshot.inspector.as_ref()?;
        Some(browser_inspector_lines(inspector))
    }

    fn browser_inspector_rows(&self) -> Vec<FormRowView> {
        let lines = self.browser_inspector_lines().unwrap_or_else(|| {
            vec![
                "Inspector".to_string(),
                String::new(),
                "No value selected.".to_string(),
            ]
        });

        lines
            .into_iter()
            .enumerate()
            .map(|(index, text)| FormRowView {
                target: FormSelection::Section(index),
                text,
                kind: FormRowKind::Field,
                selected: false,
            })
            .collect()
    }

    fn browser_main_content_lines(&self) -> Option<Vec<String>> {
        let session = self.browser_session()?;
        Some(browser_main_content_lines(&session.snapshot))
    }

    pub(crate) fn sync_browser_viewport(&mut self, width: u16, height: u16, inspector_height: u16) {
        let Some(current_viewport) = self
            .browser_session
            .as_ref()
            .map(|session| session.viewport)
        else {
            return;
        };
        let viewport = BrowserViewport::with_inspector_height(width, height, inspector_height);
        if viewport == current_viewport {
            return;
        }
        self.clear_output_selection();
        let Some(session) = self.browser_session.as_mut() else {
            return;
        };
        match session.client.request(BrowserCommand::Resize { viewport }) {
            Ok(snapshot) => {
                session.viewport = viewport;
                session.snapshot = snapshot.clone();
                self.result.status_line = snapshot.status_line;
                self.result.status_kind = StatusKind::Info;
            }
            Err(error) => {
                let stderr = session.client.stderr_text();
                let details = if stderr.trim().is_empty() {
                    format!("{error}\n")
                } else {
                    format!("{error}\n{stderr}")
                };
                if let Some(session) = self.browser_session.take() {
                    let _ = session.client.cancel();
                }
                self.report_browser_error(
                    "Browser session resize failed. Session closed.",
                    details,
                );
            }
        }
    }

    pub(crate) fn active_result_content(&self) -> ResultContent {
        match self.active_result_tab {
            ResultTab::Overview => ResultContent::Lines(self.overview_lines()),
            ResultTab::Observations => match self.result.structured.as_ref() {
                Some(summary) => ResultContent::Table(build_observations_table(summary)),
                None => {
                    ResultContent::Lines(vec!["No observation table available yet.".to_string()])
                }
            },
            ResultTab::Scans => match self.result.structured.as_ref() {
                Some(summary) => {
                    ResultContent::Table(build_scans_table(summary, self.listunfl_enabled()))
                }
                None => ResultContent::Lines(vec!["No scan table available yet.".to_string()]),
            },
            ResultTab::Fields => match self.result.structured.as_ref() {
                Some(summary) => {
                    ResultContent::Table(build_fields_table(summary, self.listunfl_enabled()))
                }
                None => ResultContent::Lines(vec!["No field table available yet.".to_string()]),
            },
            ResultTab::Spws => match self.result.structured.as_ref() {
                Some(summary) => ResultContent::Table(build_spws_table(summary)),
                None => ResultContent::Lines(vec![
                    "No spectral-window table available yet.".to_string(),
                ]),
            },
            ResultTab::Sources => match self.result.structured.as_ref() {
                Some(summary) => ResultContent::Table(build_sources_table(summary)),
                None => ResultContent::Lines(vec!["No source table available yet.".to_string()]),
            },
            ResultTab::Antennas => match self.result.structured.as_ref() {
                Some(summary) if self.verbose_enabled() => {
                    ResultContent::Table(build_antennas_table(summary))
                }
                Some(summary) => ResultContent::Lines(build_compact_antenna_lines(summary)),
                None => ResultContent::Lines(vec!["No antenna table available yet.".to_string()]),
            },
            ResultTab::Uv => ResultContent::Graphic(self.uv_tab_summary()),
            ResultTab::Stdout => ResultContent::Lines(raw_lines("stdout", &self.result.stdout)),
            ResultTab::Stderr => ResultContent::Lines(raw_lines("stderr", &self.result.stderr)),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_text_value(&mut self, id: &str, value: &str) {
        self.apply_startup_text_value(id, value.to_string())
            .expect("set text value in test");
    }

    #[cfg(test)]
    pub(crate) fn set_toggle_value(&mut self, id: &str, value: bool) {
        self.apply_startup_toggle_value(id, value)
            .expect("set toggle value in test");
    }

    pub(crate) fn apply_startup_text_value(
        &mut self,
        id: &str,
        value: String,
    ) -> Result<(), String> {
        let field = self
            .fields
            .iter_mut()
            .find(|field| field.schema.id == id)
            .ok_or_else(|| format!("unknown startup field {id:?} for {}", self.app.id))?;
        let result = field.apply_text_value(value);
        if result.is_ok() {
            self.clear_cached_uv_coverage();
        }
        result
    }

    pub(crate) fn apply_startup_toggle_value(
        &mut self,
        id: &str,
        value: bool,
    ) -> Result<(), String> {
        let field = self
            .fields
            .iter_mut()
            .find(|field| field.schema.id == id)
            .ok_or_else(|| format!("unknown startup field {id:?} for {}", self.app.id))?;
        let result = field.apply_toggle_value(value);
        if result.is_ok() {
            self.clear_cached_uv_coverage();
        }
        result
    }

    pub(crate) fn start_run_on_launch(&mut self) {
        self.start_run();
    }

    #[cfg(test)]
    pub(crate) fn set_active_result_tab(&mut self, tab: ResultTab) {
        self.activate_result_tab(tab);
    }

    fn activate_result_tab(&mut self, tab: ResultTab) {
        self.clear_output_selection();
        self.active_result_tab = tab;
        if self.active_result_tab == ResultTab::Uv {
            self.ensure_uv_coverage_started();
        }
    }

    #[cfg(test)]
    pub(crate) fn active_result_hscroll_for_test(&self) -> u16 {
        self.active_result_hscroll()
    }

    #[cfg(test)]
    pub(crate) fn set_result_for_test(&mut self, stdout: &str, stderr: &str) {
        self.result.stdout = stdout.to_string();
        self.result.stderr = stderr.to_string();
        self.result.status_line = "Test result".to_string();
        self.result.status_kind = StatusKind::Info;
    }

    fn uv_tab_summary(&self) -> String {
        if let Some(coverage) = self.uv_coverage.as_ref() {
            return uv_plot_summary(coverage);
        }
        if self.uv_loading.is_some() {
            return "Loading UV coverage...".to_string();
        }
        if let Some(error) = self.uv_coverage_error.as_ref() {
            return format!("UV coverage unavailable. {error}");
        }
        "Open the UV tab to load wavelength-space UV coverage.".to_string()
    }

    #[cfg(test)]
    pub(crate) fn start_run_for_test(&mut self) {
        self.start_run();
    }

    #[cfg(test)]
    pub(crate) fn cancel_for_test(&mut self) {
        self.cancel_current();
    }

    #[cfg(test)]
    pub(crate) fn uv_loading_active_for_test(&self) -> bool {
        self.uv_loading.is_some()
    }

    #[cfg(test)]
    pub(crate) fn wait_for_idle_for_test(&mut self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.running.is_some() && start.elapsed() < timeout {
            self.drain_execution_events();
            std::thread::sleep(Duration::from_millis(25));
        }
        self.drain_execution_events();
        self.running.is_none()
    }

    #[cfg(test)]
    pub(crate) fn wait_for_all_idle_for_test(&mut self, timeout: Duration) -> bool {
        let start = Instant::now();
        while (self.running.is_some() || self.uv_loading.is_some()) && start.elapsed() < timeout {
            self.drain_execution_events();
            std::thread::sleep(Duration::from_millis(25));
        }
        self.drain_execution_events();
        self.running.is_none() && self.uv_loading.is_none()
    }

    #[cfg(test)]
    pub(crate) fn is_running_for_test(&self) -> bool {
        self.running.is_some()
    }

    #[cfg(test)]
    pub(crate) fn status_line_for_test(&self) -> &str {
        &self.result.status_line
    }

    #[cfg(test)]
    pub(crate) fn should_return_to_launcher_for_test(&self) -> bool {
        self.return_to_launcher
    }

    #[cfg(test)]
    pub(crate) fn stderr_for_test(&self) -> &str {
        &self.result.stderr
    }

    #[cfg(test)]
    pub(crate) fn structured_for_test(&self) -> Option<&ListObsSummary> {
        self.result.structured.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn uv_coverage_for_test(&self) -> Option<&ListObsUvCoverage> {
        self.uv_coverage.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn file_output_path_for_test(&self) -> Option<&str> {
        self.result.file_output_path.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn theme_mode_for_test(&self) -> ThemeMode {
        self.theme_mode()
    }

    #[cfg(test)]
    pub(crate) fn pane_split_ratio_for_test(&self) -> f32 {
        self.pane_split_ratio()
    }

    #[cfg(test)]
    pub(crate) fn pane_focus_for_test(&self) -> PaneFocus {
        self.pane_focus
    }

    #[cfg(test)]
    pub(crate) fn section_collapsed_for_test(&self, name: &str) -> Option<bool> {
        self.sections
            .iter()
            .find(|section| section.name == name)
            .map(|section| section.collapsed)
    }

    #[cfg(test)]
    pub(crate) fn edit_buffer_for_test(&self) -> Option<&str> {
        self.edit_state.as_ref().map(|state| state.buffer.as_str())
    }

    #[cfg(test)]
    pub(crate) fn selected_form_text_for_test(&self) -> Option<String> {
        self.form_rows()
            .into_iter()
            .find(|row| row.selected)
            .map(|row| row.text)
    }

    #[cfg(test)]
    pub(crate) fn field_text_for_test(&self, id: &str) -> Option<String> {
        self.fields
            .iter()
            .find(|field| field.schema.id == id)
            .and_then(|field| field.text_value())
    }

    #[cfg(test)]
    pub(crate) fn prepare_graphics_for_test(&mut self, width: u16, height: u16) {
        let layout = crate::ui::compute_layout(Rect::new(0, 0, width, height), self);
        self.cache_output_layout(&layout);
        self.prepare_graphics(&layout);
    }

    pub(crate) fn cache_output_layout(&mut self, layout: &UiLayout) {
        let next_result = result_text_area(layout);
        let next_left = left_output_area(self, layout);
        if self.cached_result_text_area != Some(next_result)
            && self
                .output_selection
                .is_some_and(|selection| selection.target == OutputPane::Result)
        {
            self.clear_output_selection();
        }
        if self.cached_left_output_area != next_left
            && self
                .output_selection
                .is_some_and(|selection| selection.target == OutputPane::LeftOutput)
        {
            self.clear_output_selection();
        }
        self.cached_result_text_area = Some(next_result);
        self.cached_left_output_area = next_left;
    }

    pub(crate) fn visible_text_buffer(
        &self,
        target: OutputPane,
        layout: &UiLayout,
    ) -> Option<VisibleTextBuffer> {
        let area = match target {
            OutputPane::Result => result_text_area(layout),
            OutputPane::LeftOutput => left_output_area(self, layout)?,
        };
        self.visible_text_buffer_for_area(target, area)
    }

    pub(crate) fn output_selection_rect(
        &self,
        target: OutputPane,
    ) -> Option<(usize, usize, usize, usize)> {
        let selection = self.output_selection?;
        if selection.target != target || selection.mode != OutputSelectionMode::Dragging {
            return None;
        }
        Some(normalize_selection(selection))
    }

    pub(crate) fn prepare_graphics(&mut self, layout: &UiLayout) {
        if self.active_result_tab != ResultTab::Uv {
            return;
        }
        self.ensure_uv_coverage_started();
        self.ensure_uv_plot_requested(result_text_area(layout));
    }

    fn visible_text_buffer_for_area(
        &self,
        target: OutputPane,
        area: Rect,
    ) -> Option<VisibleTextBuffer> {
        if area.width == 0 || area.height == 0 {
            return None;
        }
        match target {
            OutputPane::Result => {
                if self.browser_is_active() {
                    let lines = self.browser_main_content_lines()?;
                    let browser_cells =
                        self.active_browser_tab_label() == Some(BrowserTab::Cells.label());
                    Some(VisibleTextBuffer {
                        area,
                        lines: lines
                            .into_iter()
                            .take(area.height as usize)
                            .map(|line| {
                                if browser_cells {
                                    browser_cells_visible_line(&line)
                                } else {
                                    VisibleTextLine::plain(line)
                                }
                            })
                            .collect(),
                    })
                } else {
                    let content = self.active_result_content();
                    match content {
                        ResultContent::Lines(lines) => Some(VisibleTextBuffer {
                            area,
                            lines: lines
                                .iter()
                                .skip(self.active_result_scroll() as usize)
                                .take(area.height as usize)
                                .map(|line| {
                                    VisibleTextLine::plain(slice_visible_text(
                                        line,
                                        self.active_result_hscroll() as usize,
                                        area.width as usize,
                                    ))
                                })
                                .collect(),
                        }),
                        ResultContent::Table(table) => {
                            let mut lines = Vec::new();
                            lines.push(VisibleTextLine::table_header(slice_visible_text(
                                &table.header,
                                self.active_result_hscroll() as usize,
                                area.width as usize,
                            )));
                            let body_height = area.height.saturating_sub(1) as usize;
                            lines.extend(
                                table
                                    .rows
                                    .iter()
                                    .skip(self.active_result_scroll() as usize)
                                    .take(body_height)
                                    .map(|row| {
                                        VisibleTextLine::plain(slice_visible_text(
                                            row,
                                            self.active_result_hscroll() as usize,
                                            area.width as usize,
                                        ))
                                    }),
                            );
                            Some(VisibleTextBuffer { area, lines })
                        }
                        ResultContent::Graphic(_) => None,
                    }
                }
            }
            OutputPane::LeftOutput => Some(VisibleTextBuffer {
                area,
                lines: self
                    .browser_inspector_lines()?
                    .into_iter()
                    .take(area.height as usize)
                    .map(VisibleTextLine::plain)
                    .collect(),
            }),
        }
    }

    fn clear_output_selection(&mut self) {
        self.output_selection = None;
    }

    fn clear_output_selection_for_target(&mut self, target: OutputPane) {
        if self
            .output_selection
            .is_some_and(|selection| selection.target == target)
        {
            self.output_selection = None;
        }
    }

    fn active_selected_text(&self) -> Option<String> {
        let selection = self.output_selection?;
        if selection.mode != OutputSelectionMode::Dragging {
            return None;
        }
        let area = match selection.target {
            OutputPane::Result => self.cached_result_text_area?,
            OutputPane::LeftOutput => self.cached_left_output_area?,
        };
        let buffer = self.visible_text_buffer_for_area(selection.target, area)?;
        let text = extract_selected_text(&buffer, selection);
        if text.is_empty() { None } else { Some(text) }
    }

    fn selection_point_at(
        &self,
        column: u16,
        row: u16,
        layout: &UiLayout,
    ) -> Option<(OutputPane, BufferPoint)> {
        if let Some(buffer) = self.visible_text_buffer(OutputPane::Result, layout)
            && rect_contains(buffer.area, column, row)
        {
            return Some((
                OutputPane::Result,
                clamp_point_to_buffer(&buffer, column, row),
            ));
        }
        if let Some(buffer) = self.visible_text_buffer(OutputPane::LeftOutput, layout)
            && rect_contains(buffer.area, column, row)
        {
            return Some((
                OutputPane::LeftOutput,
                clamp_point_to_buffer(&buffer, column, row),
            ));
        }
        None
    }

    fn clamped_selection_point(
        &self,
        target: OutputPane,
        column: u16,
        row: u16,
    ) -> Option<BufferPoint> {
        let area = match target {
            OutputPane::Result => self.cached_result_text_area?,
            OutputPane::LeftOutput => self.cached_left_output_area?,
        };
        let buffer = self.visible_text_buffer_for_area(target, area)?;
        Some(clamp_point_to_buffer(&buffer, column, row))
    }

    fn begin_output_selection(&mut self, target: OutputPane, point: BufferPoint) {
        self.output_selection = Some(OutputSelection {
            target,
            anchor: point,
            cursor: point,
            mode: OutputSelectionMode::Pending,
        });
    }

    fn update_output_selection(&mut self, column: u16, row: u16) -> bool {
        let Some(selection) = self.output_selection else {
            return false;
        };
        let Some(point) = self.clamped_selection_point(selection.target, column, row) else {
            return false;
        };
        let mode = if point == selection.anchor {
            selection.mode
        } else {
            OutputSelectionMode::Dragging
        };
        self.output_selection = Some(OutputSelection {
            cursor: point,
            mode,
            ..selection
        });
        true
    }

    fn finalize_output_selection(&mut self) {
        let Some(selection) = self.output_selection else {
            return;
        };
        if selection.mode == OutputSelectionMode::Pending {
            self.output_selection = None;
            return;
        }
        if let Some(text) = self.active_selected_text() {
            self.copy_text_to_clipboard(text, "selection");
        }
    }

    fn apply_parameter_action(&mut self, action: ParameterAction) {
        match action {
            ParameterAction::SelectPrevious => self.select_previous_form_item(),
            ParameterAction::SelectNext => self.select_next_form_item(),
            ParameterAction::ChoicePrevious => self.adjust_selected_choice(false),
            ParameterAction::ChoiceNext => self.adjust_selected_choice(true),
            ParameterAction::Activate => self.activate_selected_form_item(),
        }
    }

    fn apply_path_chooser_action(&mut self, action: PathChooserAction) {
        match action {
            PathChooserAction::Cancel => self.cancel_path_chooser(),
            PathChooserAction::Confirm => self.confirm_path_chooser(),
            PathChooserAction::SelectCurrent => self.select_current_path_chooser_entry(),
            PathChooserAction::Navigate(input) => self.apply_path_chooser_input(input),
        }
    }

    fn handle_path_chooser_mouse(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        let area = crate::ui::path_chooser_area(layout.body);
        let list_area = crate::ui::path_chooser_list_area(area);
        match mouse_event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if !rect_contains(area, mouse_event.column, mouse_event.row) {
                    self.cancel_path_chooser();
                    return;
                }
                if !rect_contains(list_area, mouse_event.column, mouse_event.row) {
                    return;
                }
                let Some(chooser) = self.path_chooser.as_mut() else {
                    return;
                };
                if chooser.explorer.files().is_empty() {
                    return;
                }
                let visible_height = list_area.height as usize;
                let row_offset = mouse_event.row.saturating_sub(list_area.y) as usize;
                let visible_start = chooser_visible_start(
                    chooser.explorer.selected_idx(),
                    chooser.explorer.files().len(),
                    visible_height,
                );
                let index = (visible_start + row_offset).min(chooser.explorer.files().len() - 1);
                chooser.explorer.set_selected_idx(index);
                let click_target = ClickTarget::PathChooserEntry(index);
                let double_click = self.last_click.is_some_and(|last| {
                    last.target == click_target && last.at.elapsed() <= DOUBLE_CLICK_WINDOW
                });
                self.last_click = Some(ClickState {
                    target: click_target,
                    at: Instant::now(),
                });
                let double_click_target = if double_click {
                    Some((
                        chooser.field_index,
                        chooser.explorer.current().path.clone(),
                        chooser.explorer.current().is_dir,
                    ))
                } else {
                    None
                };
                let _ = chooser;
                if let Some((field_index, path, is_dir)) = double_click_target {
                    if is_dir {
                        self.apply_path_chooser_input(ExplorerInput::Right);
                    } else {
                        self.select_path_chooser_path(field_index, &path);
                    }
                }
            }
            MouseEventKind::ScrollUp
                if rect_contains(area, mouse_event.column, mouse_event.row) =>
            {
                self.apply_path_chooser_input(ExplorerInput::Up);
            }
            MouseEventKind::ScrollDown
                if rect_contains(area, mouse_event.column, mouse_event.row) =>
            {
                self.apply_path_chooser_input(ExplorerInput::Down);
            }
            _ => {}
        }
    }

    fn apply_result_action(&mut self, action: ResultAction) {
        match action {
            ResultAction::PreviousTab => self.cycle_visible_result_tab(false),
            ResultAction::NextTab => self.cycle_visible_result_tab(true),
            ResultAction::Scroll(delta) => self.scroll_active_result(delta),
            ResultAction::ScrollHorizontal(delta) => self.scroll_active_result_horizontal(delta),
        }
    }

    fn apply_browser_action(&mut self, action: BrowserAction) {
        match action {
            BrowserAction::CycleView { forward } => {
                self.send_browser_command(BrowserCommand::CycleView {
                    forward,
                    viewport: None,
                });
            }
            BrowserAction::MoveLeft => {
                self.send_browser_command(BrowserCommand::MoveLeft {
                    steps: 1,
                    viewport: None,
                });
            }
            BrowserAction::MoveRight => {
                self.send_browser_command(BrowserCommand::MoveRight {
                    steps: 1,
                    viewport: None,
                });
            }
            BrowserAction::MoveUp => {
                self.send_browser_command(BrowserCommand::MoveUp {
                    steps: 1,
                    viewport: None,
                });
            }
            BrowserAction::MoveDown => {
                self.send_browser_command(BrowserCommand::MoveDown {
                    steps: 1,
                    viewport: None,
                });
            }
            BrowserAction::PageUp => self.send_browser_command(BrowserCommand::PageUp {
                pages: 1,
                viewport: None,
            }),
            BrowserAction::PageDown => self.send_browser_command(BrowserCommand::PageDown {
                pages: 1,
                viewport: None,
            }),
            BrowserAction::Activate => {
                self.send_browser_command(BrowserCommand::Activate { viewport: None })
            }
            BrowserAction::Back => {
                self.send_browser_command(BrowserCommand::Back { viewport: None })
            }
            BrowserAction::Escape => {
                self.send_browser_command(BrowserCommand::Escape { viewport: None })
            }
        }
    }

    fn activate_browser_tab(&mut self, tab: BrowserTab) {
        let Some(current) = self.active_browser_tab() else {
            return;
        };
        if current == tab {
            return;
        }

        let current_index = current.index();
        let target_index = tab.index();
        let (steps, forward) = if target_index >= current_index {
            (target_index - current_index, true)
        } else {
            (current_index - target_index, false)
        };
        for _ in 0..steps {
            self.send_browser_command(BrowserCommand::CycleView {
                forward,
                viewport: None,
            });
        }
    }

    fn copy_output_selection(&mut self) {
        let payload = self
            .active_selected_text()
            .map(|text| (text, "selection"))
            .or_else(|| {
                if self.browser_session.is_some() {
                    self.browser_clipboard_payload()
                } else if self.active_result_tab == ResultTab::Uv {
                    self.uv_coverage
                        .as_ref()
                        .map(|coverage| (uv_plot_summary(coverage), "uv coverage summary"))
                } else {
                    None
                }
            });
        let Some((text, label)) = payload else {
            self.result.status_line = "Nothing copyable is selected.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.copy_text_to_clipboard(text, label);
    }

    fn copy_text_to_clipboard(&mut self, text: String, label: &str) {
        match clipboard::copy_text(&text) {
            Ok(()) => {
                self.result.status_line = format!("Copied {label} to clipboard.");
                self.result.status_kind = StatusKind::Ok;
            }
            Err(error) => {
                self.result.status_line = "Failed to copy to clipboard.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
            }
        }
    }

    fn apply_edit_action(&mut self, action: EditAction) {
        let Some(edit_state) = self.edit_state.as_mut() else {
            return;
        };
        match action {
            EditAction::Cancel => self.edit_state = None,
            EditAction::Commit => {
                if let Some(field) = self.fields.get_mut(edit_state.field_index) {
                    field.set_text(edit_state.buffer.clone());
                }
                self.edit_state = None;
            }
            EditAction::DeleteBackward => {
                edit_state.buffer.pop();
            }
            EditAction::Insert(character) => {
                edit_state.buffer.push(character);
            }
        }
    }

    fn handle_left_mouse_down(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        if layout.in_divider(mouse_event.column, mouse_event.row) {
            self.clear_output_selection();
            self.dragging_divider = true;
            self.dragging_result_scrollbar = false;
            self.last_click = Some(ClickState {
                target: ClickTarget::Divider,
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_scrollbar(mouse_event.column, mouse_event.row) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            self.dragging_divider = false;
            self.dragging_result_scrollbar = true;
            self.dragging_result_hscrollbar = false;
            self.scroll_result_to_mouse(mouse_event.row, layout);
            self.last_click = Some(ClickState {
                target: ClickTarget::ResultScrollbar,
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_hscrollbar(mouse_event.column, mouse_event.row) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            self.dragging_divider = false;
            self.dragging_result_scrollbar = false;
            self.dragging_result_hscrollbar = true;
            self.dragging_result_hscrollbar_grab =
                self.result_hscrollbar_grab_offset(mouse_event.column, layout);
            self.scroll_result_horizontally_to_mouse(mouse_event.column, layout);
            self.last_click = Some(ClickState {
                target: ClickTarget::ResultHorizontalScrollbar,
                at: Instant::now(),
            });
            return;
        }

        if let Some(tab) = layout.result_tab_at(mouse_event.column, mouse_event.row) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            self.activate_result_tab(tab);
            self.last_click = Some(ClickState {
                target: ClickTarget::Tab(tab),
                at: Instant::now(),
            });
            return;
        }

        if let Some(tab) = layout.browser_tab_at(mouse_event.column, mouse_event.row) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            self.activate_browser_tab(tab);
            self.last_click = Some(ClickState {
                target: ClickTarget::BrowserTab(tab),
                at: Instant::now(),
            });
            return;
        }

        if let Some((target, point)) =
            self.selection_point_at(mouse_event.column, mouse_event.row, layout)
        {
            self.pane_focus = if target == OutputPane::Result {
                PaneFocus::Result
            } else {
                PaneFocus::Parameters
            };
            self.begin_output_selection(target, point);
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(self.pane_focus),
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Result;
            self.clear_output_selection_for_target(OutputPane::Result);
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Result),
                at: Instant::now(),
            });
            return;
        }

        if self.has_active_session() {
            return;
        }

        if let Some(target) = layout.form_target_at(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Parameters;
            let click_target = match target {
                FormSelection::Section(index) => ClickTarget::Section(index),
                FormSelection::Field(index) => ClickTarget::Field(index),
            };
            let double_click = self.last_click.is_some_and(|last| {
                last.target == click_target && last.at.elapsed() <= DOUBLE_CLICK_WINDOW
            });

            self.last_click = Some(ClickState {
                target: click_target,
                at: Instant::now(),
            });

            match target {
                FormSelection::Section(section_index) => {
                    self.selected_form = FormSelection::Section(section_index);
                    self.toggle_section(section_index);
                }
                FormSelection::Field(field_index) => {
                    self.selected_form = FormSelection::Field(field_index);
                    if self.path_field_browse_hit(field_index, mouse_event.column, layout) {
                        self.open_path_chooser(field_index);
                    } else if double_click {
                        self.enter_edit_mode(field_index);
                    }
                }
            }
            return;
        }

        if layout.in_form_block(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Parameters;
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Parameters),
                at: Instant::now(),
            });
        }
    }

    fn handle_left_mouse_drag(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        if self.dragging_divider {
            self.clear_output_selection();
            let relative = mouse_event
                .column
                .saturating_sub(layout.body.x)
                .min(layout.body.width.saturating_sub(1));
            let ratio = if relative <= 1 {
                0.0
            } else {
                f32::from(relative) / f32::from(layout.body.width.max(1))
            };
            self.config_store.set_pane_split_ratio(ratio);
            return;
        }
        if self.dragging_result_scrollbar {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.scroll_result_to_mouse(mouse_event.row, layout);
            return;
        }
        if self.dragging_result_hscrollbar {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.scroll_result_horizontally_to_mouse(mouse_event.column, layout);
            return;
        }
        if self.update_output_selection(mouse_event.column, mouse_event.row) {
            self.last_click = None;
        }
    }

    fn handle_mouse_scroll(&mut self, mouse_event: MouseEvent, layout: &UiLayout, delta: i16) {
        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            if self.browser_session.is_some() {
                self.scroll_active_browser(delta);
            } else {
                self.scroll_active_result(delta);
            }
            return;
        }
        if layout.in_form_block(mouse_event.column, mouse_event.row) && !self.has_active_session() {
            self.pane_focus = PaneFocus::Parameters;
            if delta.is_negative() {
                for _ in 0..delta.unsigned_abs() {
                    self.select_previous_form_item();
                }
            } else {
                for _ in 0..delta as u16 {
                    self.select_next_form_item();
                }
            }
        }
    }

    fn handle_mouse_hscroll(&mut self, mouse_event: MouseEvent, layout: &UiLayout, delta: i16) {
        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            if self.browser_session.is_some() {
                self.scroll_active_browser_horizontal(delta);
            } else {
                self.scroll_active_result_horizontal(delta);
            }
        }
    }

    fn toggle_theme(&mut self) {
        let next = self.theme_mode().toggle();
        self.config_store.set_theme_mode(next);
    }

    fn toggle_focus(&mut self) {
        self.pane_focus = match self.pane_focus {
            PaneFocus::Parameters => PaneFocus::Result,
            PaneFocus::Result => {
                if self.has_active_session() || self.sections.is_empty() {
                    PaneFocus::Result
                } else {
                    PaneFocus::Parameters
                }
            }
        };
    }

    fn toggle_advanced(&mut self) {
        self.show_advanced = !self.show_advanced;
        self.ensure_visible_form_selection();
    }

    fn visible_form_targets(&self) -> Vec<FormSelection> {
        self.form_rows().into_iter().map(|row| row.target).collect()
    }

    fn select_previous_form_item(&mut self) {
        let visible = self.visible_form_targets();
        if visible.is_empty() {
            return;
        }
        let position = visible
            .iter()
            .position(|target| *target == self.selected_form)
            .unwrap_or(0);
        let next = if position == 0 {
            visible.len() - 1
        } else {
            position - 1
        };
        self.selected_form = visible[next];
    }

    fn select_next_form_item(&mut self) {
        let visible = self.visible_form_targets();
        if visible.is_empty() {
            return;
        }
        let position = visible
            .iter()
            .position(|target| *target == self.selected_form)
            .unwrap_or(0);
        self.selected_form = visible[(position + 1) % visible.len()];
    }

    fn activate_selected_form_item(&mut self) {
        match self.selected_form {
            FormSelection::Section(section_index) => self.toggle_section(section_index),
            FormSelection::Field(field_index) => {
                let Some(field) = self.fields.get(field_index) else {
                    return;
                };
                match &field.value {
                    FormValue::Text(_) => self.enter_edit_mode(field_index),
                    FormValue::Toggle(_) => self.toggle_field(field_index),
                    FormValue::Choice { .. } => self.cycle_field_choice(field_index, true),
                }
            }
        }
    }

    fn toggle_section(&mut self, section_index: usize) {
        let Some(section) = self.sections.get_mut(section_index) else {
            return;
        };
        section.collapsed = !section.collapsed;
        self.selected_form = FormSelection::Section(section_index);
        self.ensure_visible_form_selection();
    }

    fn enter_edit_mode(&mut self, field_index: usize) {
        let Some(field) = self.fields.get(field_index) else {
            return;
        };
        if let FormValue::Text(value) = &field.value {
            self.edit_state = Some(EditState {
                field_index,
                buffer: value.clone(),
            });
        }
    }

    fn toggle_field(&mut self, field_index: usize) {
        if let Some(field) = self.fields.get_mut(field_index) {
            field.toggle();
        }
        self.sync_result_tab_visibility();
    }

    fn cycle_field_choice(&mut self, field_index: usize, forward: bool) {
        if let Some(field) = self.fields.get_mut(field_index) {
            field.cycle_choice(forward);
        }
    }

    fn adjust_selected_choice(&mut self, forward: bool) {
        if let FormSelection::Field(field_index) = self.selected_form {
            self.cycle_field_choice(field_index, forward);
        }
    }

    fn active_browser_tab(&self) -> Option<BrowserTab> {
        self.browser_session()
            .map(|session| BrowserTab::from_view(session.snapshot.view))
    }

    fn toggle_parameters_pane(&mut self) {
        self.clear_output_selection();
        let next = if self.parameters_pane_collapsed() {
            self.config_store.pane_restore_ratio()
        } else {
            0.0
        };
        self.config_store.set_pane_split_ratio(next);
        if next == 0.0 {
            self.pane_focus = PaneFocus::Result;
        }
    }

    pub(crate) fn active_browser_scroll(&self) -> u16 {
        let Some(session) = self.browser_session() else {
            return 0;
        };
        session
            .snapshot
            .vertical_metrics
            .map(|metrics| metrics.selected_index.min(u16::MAX as usize) as u16)
            .unwrap_or(0)
    }

    pub(crate) fn active_browser_hscroll(&self) -> u16 {
        let Some(session) = self.browser_session() else {
            return 0;
        };
        session
            .snapshot
            .horizontal_metrics
            .map(|metrics| metrics.selected_index.min(u16::MAX as usize) as u16)
            .unwrap_or(0)
    }

    fn scroll_active_browser(&mut self, delta: i16) {
        self.clear_output_selection_for_target(OutputPane::Result);
        if delta.is_negative() {
            self.send_browser_command(BrowserCommand::MoveUp {
                steps: delta.unsigned_abs() as usize,
                viewport: None,
            });
        } else {
            self.send_browser_command(BrowserCommand::MoveDown {
                steps: delta as usize,
                viewport: None,
            });
        }
    }

    fn set_active_browser_scroll(&mut self, scroll: usize) {
        let current = self.active_browser_scroll() as usize;
        if scroll > current {
            self.scroll_active_browser((scroll - current).min(i16::MAX as usize) as i16);
        } else if current > scroll {
            self.scroll_active_browser(-((current - scroll).min(i16::MAX as usize) as i16));
        }
    }

    fn scroll_active_browser_horizontal(&mut self, delta: i16) {
        self.clear_output_selection_for_target(OutputPane::Result);
        if delta.is_negative() {
            self.send_browser_command(BrowserCommand::MoveLeft {
                steps: delta.unsigned_abs() as usize,
                viewport: None,
            });
        } else {
            self.send_browser_command(BrowserCommand::MoveRight {
                steps: delta as usize,
                viewport: None,
            });
        }
    }

    fn set_active_browser_hscroll(&mut self, scroll: usize) {
        let current = self.active_browser_hscroll() as usize;
        if scroll > current {
            self.scroll_active_browser_horizontal((scroll - current).min(i16::MAX as usize) as i16);
        } else if current > scroll {
            self.scroll_active_browser_horizontal(
                -((current - scroll).min(i16::MAX as usize) as i16),
            );
        }
    }

    fn scroll_active_result(&mut self, delta: i16) {
        self.clear_output_selection_for_target(OutputPane::Result);
        let scroll = &mut self.result_scrolls[self.active_result_tab.index()];
        if delta.is_negative() {
            *scroll = scroll.saturating_sub(delta.unsigned_abs());
        } else {
            *scroll = scroll.saturating_add(delta as u16);
        }
    }

    fn set_active_result_scroll(&mut self, scroll: usize) {
        self.result_scrolls[self.active_result_tab.index()] = scroll.min(u16::MAX as usize) as u16;
    }

    fn scroll_active_result_horizontal(&mut self, delta: i16) {
        self.clear_output_selection_for_target(OutputPane::Result);
        let current = self.result_hscrolls[self.active_result_tab.index()] as usize;
        let next = if delta.is_negative() {
            current.saturating_sub(delta.unsigned_abs() as usize)
        } else {
            current.saturating_add(delta as usize)
        };
        self.set_active_result_hscroll(next);
    }

    fn set_active_result_hscroll(&mut self, scroll: usize) {
        self.result_hscrolls[self.active_result_tab.index()] = scroll.min(u16::MAX as usize) as u16;
    }

    fn scroll_result_to_mouse(&mut self, row: u16, layout: &UiLayout) {
        let Some(track) = layout.result_scrollbar else {
            return;
        };
        let metrics = if self.browser_session.is_some() {
            self.active_browser_scroll_metrics(track.height)
        } else {
            self.active_result_scroll_metrics(track.height)
        };
        let Some((content_length, viewport_length)) = metrics else {
            return;
        };
        if content_length <= viewport_length || track.height == 0 {
            if self.browser_session.is_some() {
                self.set_active_browser_scroll(0);
            } else {
                self.set_active_result_scroll(0);
            }
            return;
        }

        let max_scroll = content_length.saturating_sub(viewport_length);
        let row_offset = row
            .saturating_sub(track.y)
            .min(track.height.saturating_sub(1)) as usize;
        let denominator = track.height.saturating_sub(1) as usize;
        let scroll = if denominator == 0 {
            0
        } else {
            (row_offset * max_scroll + denominator / 2) / denominator
        };
        if self.browser_session.is_some() {
            self.set_active_browser_scroll(scroll);
        } else {
            self.set_active_result_scroll(scroll);
        }
    }

    fn scroll_result_horizontally_to_mouse(&mut self, column: u16, layout: &UiLayout) {
        let Some(track) = layout.result_hscrollbar else {
            return;
        };
        let metrics = if self.browser_session.is_some() {
            self.active_browser_hscroll_metrics(track.width)
        } else {
            self.active_result_hscroll_metrics(track.width)
        };
        let Some((content_width, viewport_width)) = metrics else {
            return;
        };
        if content_width <= viewport_width || track.width == 0 {
            if self.browser_session.is_some() {
                self.set_active_browser_hscroll(0);
            } else {
                self.set_active_result_hscroll(0);
            }
            return;
        }

        let max_scroll = content_width.saturating_sub(viewport_width);
        let thumb_length =
            scrollbar_thumb_length(track.width as usize, content_width, viewport_width);
        let max_thumb_offset = track.width as usize - thumb_length;
        let scroll = if max_thumb_offset == 0 {
            0
        } else {
            let column_offset = column.saturating_sub(track.x) as usize;
            let adjusted =
                column_offset.saturating_sub(self.dragging_result_hscrollbar_grab as usize);
            let thumb_offset = adjusted.min(max_thumb_offset);
            (thumb_offset * max_scroll + max_thumb_offset / 2) / max_thumb_offset
        };
        if self.browser_session.is_some() {
            self.set_active_browser_hscroll(scroll);
        } else {
            self.set_active_result_hscroll(scroll);
        }
    }

    fn active_result_scroll_metrics(&self, viewport_height: u16) -> Option<(usize, usize)> {
        let content = self.active_result_content();
        let viewport_length = match &content {
            ResultContent::Lines(_) => viewport_height as usize,
            ResultContent::Table(_) => viewport_height.saturating_sub(1) as usize,
            ResultContent::Graphic(_) => return None,
        };
        if viewport_length == 0 {
            return None;
        }
        let content_length = match content {
            ResultContent::Lines(lines) => lines.len(),
            ResultContent::Table(table) => table.rows.len(),
            ResultContent::Graphic(_) => return None,
        };
        Some((content_length, viewport_length))
    }

    fn active_result_hscroll_metrics(&self, viewport_width: u16) -> Option<(usize, usize)> {
        let content = self.active_result_content();
        let viewport_width = viewport_width as usize;
        if viewport_width == 0 {
            return None;
        }
        let content_width = match content {
            ResultContent::Lines(lines) => lines
                .iter()
                .map(|line| line.chars().count())
                .max()
                .unwrap_or(0),
            ResultContent::Table(table) => table.content_width(),
            ResultContent::Graphic(_) => return None,
        };
        Some((content_width, viewport_width))
    }

    fn result_hscrollbar_grab_offset(&self, column: u16, layout: &UiLayout) -> u16 {
        let Some(track) = layout.result_hscrollbar else {
            return 0;
        };
        let metrics = if self.browser_session.is_some() {
            self.active_browser_hscroll_metrics(track.width)
        } else {
            self.active_result_hscroll_metrics(track.width)
        };
        let Some((content_width, viewport_width)) = metrics else {
            return 0;
        };
        if content_width <= viewport_width || track.width == 0 {
            return 0;
        }

        let thumb_length =
            scrollbar_thumb_length(track.width as usize, content_width, viewport_width);
        let max_scroll = content_width.saturating_sub(viewport_width);
        let max_thumb_offset = track.width as usize - thumb_length;
        if max_scroll == 0 || max_thumb_offset == 0 {
            return 0;
        }

        let active_hscroll = if self.browser_session.is_some() {
            self.active_browser_hscroll()
        } else {
            self.active_result_hscroll()
        };
        let thumb_offset =
            (active_hscroll as usize * max_thumb_offset + max_scroll / 2) / max_scroll;
        let thumb_start = track.x as usize + thumb_offset;
        let thumb_end = thumb_start + thumb_length;
        let click = column as usize;
        if (thumb_start..thumb_end).contains(&click) {
            (click - thumb_start) as u16
        } else {
            (thumb_length / 2) as u16
        }
    }

    fn cycle_visible_result_tab(&mut self, forward: bool) {
        self.clear_output_selection_for_target(OutputPane::Result);
        let tabs = self.visible_result_tabs();
        let Some(position) = tabs.iter().position(|tab| *tab == self.active_result_tab) else {
            self.active_result_tab = ResultTab::Overview;
            return;
        };
        let next = if forward {
            (position + 1) % tabs.len()
        } else if position == 0 {
            tabs.len() - 1
        } else {
            position - 1
        };
        self.activate_result_tab(tabs[next]);
    }

    fn ensure_visible_form_selection(&mut self) {
        let visible = self.visible_form_targets();
        if visible.is_empty() {
            self.selected_form = FormSelection::Section(0);
            return;
        }
        if !visible.contains(&self.selected_form) {
            self.selected_form = visible[0];
        }
    }

    fn start_run(&mut self) {
        self.clear_output_selection();
        self.commit_edit_buffer();
        self.clear_cached_uv_coverage();
        self.last_listobs_uv_plan = None;

        if self.schema.is_none() {
            self.result.status_line = "Cannot run without a loaded UI schema.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.active_result_tab = ResultTab::Stderr;
            return;
        }

        if self.app.is_browser_session() {
            self.start_table_browser();
            return;
        }

        match self.build_execution_plan() {
            Ok(plan) => {
                let uv_plan = if self.app.id == "listobs" {
                    match self.build_uv_execution_plan() {
                        Ok(uv_plan) => Some(uv_plan),
                        Err(error) => {
                            self.result.status_line =
                                "Cannot prepare UV coverage request.".to_string();
                            self.result.status_kind = StatusKind::Error;
                            self.result.stderr = format!("{error}\n");
                            self.active_result_tab = ResultTab::Stderr;
                            return;
                        }
                    }
                } else {
                    None
                };

                match spawn_process(&plan) {
                    Ok(process) => {
                        self.result = ResultState {
                            status_line: format!("Running {}...", self.app.id),
                            status_kind: StatusKind::Running,
                            file_output_path: plan.file_output_path.clone(),
                            ..ResultState::default()
                        };
                        self.edit_state = None;
                        self.pane_focus = PaneFocus::Result;
                        self.active_result_tab = ResultTab::Overview;
                        self.result_scrolls = [0; RESULT_TAB_COUNT];
                        self.result_hscrolls = [0; RESULT_TAB_COUNT];
                        self.running = Some(RunningState {
                            process,
                            renderer: plan.renderer,
                            file_output_path: plan.file_output_path,
                            uv_plan,
                            cancel_requested: false,
                        });
                    }
                    Err(error) => {
                        self.result.status_line = format!("Failed to launch {}.", self.app.id);
                        self.result.status_kind = StatusKind::Error;
                        self.result.stderr = format!("{error}\n");
                        self.active_result_tab = ResultTab::Stderr;
                    }
                }
            }
            Err(error) => {
                self.result.status_line = "Cannot start command.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
                self.active_result_tab = ResultTab::Stderr;
            }
        }
    }

    fn start_table_browser(&mut self) {
        self.clear_output_selection();
        let Some(path) = self
            .field_text("table_path")
            .filter(|value| !value.trim().is_empty())
        else {
            self.result.status_line = "Table Path is required.".to_string();
            self.result.status_kind = StatusKind::Error;
            return;
        };

        let viewport = BrowserViewport::new(120, 24);
        match self
            .app
            .resolve_command()
            .and_then(|command| BrowserClient::spawn(&command))
        {
            Ok(client) => match client.request_startup(BrowserCommand::OpenRoot { path, viewport })
            {
                Ok(snapshot) => {
                    self.result = ResultState {
                        status_line: snapshot.status_line.clone(),
                        status_kind: StatusKind::Info,
                        ..ResultState::default()
                    };
                    self.edit_state = None;
                    self.pane_focus = PaneFocus::Result;
                    self.browser_session = Some(BrowserSession {
                        client,
                        snapshot,
                        viewport,
                    });
                }
                Err(error) => {
                    let _ = client.cancel();
                    self.report_browser_error(
                        "Failed to open table browser.",
                        format!("{error}\n"),
                    );
                }
            },
            Err(error) => {
                self.report_browser_error("Failed to launch table browser.", format!("{error}\n"));
            }
        }
    }

    fn send_browser_command(&mut self, command: BrowserCommand) {
        let result = {
            let Some(session) = self.browser_session.as_mut() else {
                return;
            };
            match session.client.request(command) {
                Ok(snapshot) => {
                    session.snapshot = snapshot.clone();
                    Ok(snapshot)
                }
                Err(error) => Err((error, session.client.stderr_text())),
            }
        };

        match result {
            Ok(snapshot) => {
                self.clear_output_selection();
                self.result.status_line = snapshot.status_line;
                self.result.status_kind = StatusKind::Info;
            }
            Err((error, stderr)) => {
                if let Some(session) = self.browser_session.take() {
                    let _ = session.client.cancel();
                }
                let details = if stderr.trim().is_empty() {
                    format!("{error}\n")
                } else {
                    format!("{error}\n{stderr}")
                };
                self.report_browser_error("Browser command failed. Session closed.", details);
            }
        }
    }

    fn report_browser_error(&mut self, status_line: &str, stderr: String) {
        self.result.status_line = status_line.to_string();
        self.result.status_kind = StatusKind::Error;
        self.result.stderr = stderr;
        self.active_result_tab = ResultTab::Stderr;
        self.pane_focus = PaneFocus::Result;
    }

    fn browser_clipboard_payload(&self) -> Option<(String, &'static str)> {
        let session = self.browser_session()?;
        let inspector = session.snapshot.inspector.as_ref()?;
        Some(copyable_browser_text(inspector))
    }

    fn clear_cached_uv_coverage(&mut self) {
        if let Some(loading) = self.uv_loading.take() {
            if !loading.cancel_requested {
                let _ = loading.process.cancel();
            }
        }
        self.uv_coverage = None;
        self.uv_coverage_error = None;
        self.uv_plot_panel = None;
    }

    fn ensure_uv_coverage_started(&mut self) {
        if self.app.id != "listobs"
            || self.result.structured.is_none()
            || self.uv_coverage.is_some()
            || self.uv_loading.is_some()
        {
            return;
        }

        let Some(plan) = self.last_listobs_uv_plan.clone() else {
            self.uv_coverage_error = Some(
                "UV coverage is only available for the most recent completed listobs run."
                    .to_string(),
            );
            return;
        };

        match spawn_process(&plan) {
            Ok(process) => {
                self.uv_loading = Some(UvCoverageLoadState {
                    process,
                    stdout: String::new(),
                    stderr: String::new(),
                    cancel_requested: false,
                });
                self.uv_coverage_error = None;
                self.result.status_line = "Loading UV coverage...".to_string();
                self.result.status_kind = StatusKind::Running;
            }
            Err(error) => {
                self.uv_coverage_error = Some(error.clone());
                self.result.status_line = "Failed to launch UV coverage loader.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr.push_str(&format!("{error}\n"));
            }
        }
    }

    fn build_uv_execution_plan(&self) -> Result<ExecutionPlan, String> {
        self.schema
            .as_ref()
            .ok_or_else(|| "missing command schema".to_string())?;

        let mut arguments = Vec::<OsString>::new();
        let force_selectdata = self.selection_inputs_present();
        for field in &self.fields {
            if matches!(
                field.schema.id.as_str(),
                "selectdata" | "output" | "listfile" | "overwrite" | "format"
            ) {
                continue;
            }
            field.append_arguments(&mut arguments)?;
        }
        self.append_effective_selectdata_argument(&mut arguments, force_selectdata)?;
        arguments.push("--format".into());
        arguments.push("json".into());
        arguments.push("--uv-coverage-json".into());

        Ok(ExecutionPlan {
            command: self.app.resolve_command()?,
            arguments,
            renderer: None,
            file_output_path: None,
        })
    }

    fn drain_uv_loading_events(&mut self) {
        loop {
            let event = match self.uv_loading.as_ref() {
                Some(loading) => loading.process.try_recv(),
                None => return,
            };
            match event {
                Ok(ExecutionEvent::Stdout(chunk)) => {
                    if let Some(loading) = self.uv_loading.as_mut() {
                        loading.stdout.push_str(&chunk);
                    }
                }
                Ok(ExecutionEvent::Stderr(chunk)) => {
                    if let Some(loading) = self.uv_loading.as_mut() {
                        loading.stderr.push_str(&chunk);
                    }
                }
                Ok(ExecutionEvent::Exited(exit)) => {
                    self.finish_uv_loading(exit.success);
                    break;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    self.uv_loading = None;
                    self.uv_coverage_error =
                        Some("UV coverage loader disconnected unexpectedly.".to_string());
                    self.result.status_line =
                        "UV coverage loader disconnected unexpectedly.".to_string();
                    self.result.status_kind = StatusKind::Error;
                    break;
                }
            }
        }
    }

    fn finish_uv_loading(&mut self, success: bool) {
        let Some(loading) = self.uv_loading.take() else {
            return;
        };

        if loading.cancel_requested {
            self.uv_coverage_error = Some("UV coverage load canceled.".to_string());
            self.result.status_line = "UV coverage load canceled.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        }

        if !success {
            self.uv_coverage_error =
                Some("UV coverage loader failed. See stderr for details.".to_string());
            self.result.status_line = "UV coverage load failed.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.result.stderr.push_str(&loading.stderr);
            return;
        }

        match serde_json::from_str::<ListObsUvCoverage>(&loading.stdout) {
            Ok(coverage) => {
                self.uv_coverage = Some(coverage);
                self.uv_coverage_error = None;
                self.result.status_line = "UV coverage loaded.".to_string();
                self.result.status_kind = StatusKind::Ok;
            }
            Err(error) => {
                self.uv_coverage_error = Some(format!("Failed to parse UV coverage JSON: {error}"));
                self.result.status_line = "UV coverage parsing failed.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr.push_str(&loading.stdout);
            }
        }
    }

    fn pump_uv_plot_panel(&mut self) {
        let Some(panel) = self.uv_plot_panel.as_mut() else {
            return;
        };
        match panel.renderer.pump() {
            Ok(changed) => {
                if changed {
                    panel.image_size = panel.renderer.image_size();
                }
            }
            Err(error) => {
                panel.last_error = Some(error.to_string());
                self.result.status_line = "UV plot rendering failed.".to_string();
                self.result.status_kind = StatusKind::Warning;
            }
        }
    }

    fn ensure_uv_plot_requested(&mut self, area: Rect) {
        let Some(coverage) = self.uv_coverage.clone() else {
            return;
        };
        if area.is_empty() {
            return;
        }
        let theme_mode = self.theme_mode();

        let panel = self.uv_plot_panel.get_or_insert_with(|| {
            let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
            let font_size = picker.font_size();
            let renderer = PanelRenderer::new(picker, Resize::Fit(None), |job| {
                render_uv_plot(job.max_pixel_width, job.max_pixel_height, &job.input)
            })
            .expect("panel renderer");
            UvPlotPanelState {
                renderer,
                font_size,
                request_key: None,
                last_error: None,
                image_size: None,
            }
        });

        let request_key = UvPlotRequestKey {
            area,
            theme_mode,
            sample_count: coverage.sample_count,
            track_count: coverage.tracks.len(),
            max_abs_uv_bits: coverage.max_abs_uv_lambda.to_bits(),
        };
        if panel.request_key == Some(request_key) {
            return;
        }

        let pixel_width = u32::from(area.width.max(1)) * u32::from(panel.font_size.0.max(1));
        let pixel_height = u32::from(area.height.max(1)) * u32::from(panel.font_size.1.max(1));
        if let Err(error) = panel.renderer.request(
            area,
            pixel_width.max(1),
            pixel_height.max(1),
            UvPlotRenderInput {
                coverage,
                theme_mode,
            },
        ) {
            panel.last_error = Some(error.to_string());
            self.result.status_line = "Failed to queue UV plot render.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        panel.request_key = Some(request_key);
    }

    pub(crate) fn uv_plot_protocol(&self) -> Option<&PanelProtocol> {
        self.uv_plot_panel
            .as_ref()
            .and_then(|panel| panel.renderer.protocol())
    }

    pub(crate) fn uv_plot_pending(&self) -> bool {
        self.uv_plot_panel
            .as_ref()
            .is_some_and(|panel| panel.renderer.is_pending())
    }

    pub(crate) fn uv_plot_last_error(&self) -> Option<&str> {
        self.uv_plot_panel
            .as_ref()
            .and_then(|panel| panel.last_error.as_deref())
    }

    fn build_execution_plan(&self) -> Result<ExecutionPlan, String> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| "missing command schema".to_string())?;

        let mut arguments = Vec::<OsString>::new();
        let force_selectdata = self.selection_inputs_present();
        for field in &self.fields {
            if field.schema.id == "selectdata" {
                continue;
            }
            field.append_arguments(&mut arguments)?;
        }
        self.append_effective_selectdata_argument(&mut arguments, force_selectdata)?;

        let output = self.field_text("output");
        let listfile = self.field_text("listfile");
        if output.as_deref().is_some_and(|value| !value.is_empty())
            && listfile.as_deref().is_some_and(|value| !value.is_empty())
        {
            return Err("Choose either --output or --listfile, not both.".to_string());
        }

        let file_output_path = output
            .filter(|value| !value.is_empty())
            .or_else(|| listfile.filter(|value| !value.is_empty()));

        if let Some(managed_output) = &schema.managed_output {
            inject_managed_arguments(&mut arguments, managed_output);
        }

        Ok(ExecutionPlan {
            command: self.app.resolve_command()?,
            arguments,
            renderer: schema
                .managed_output
                .as_ref()
                .map(|value| value.renderer.clone()),
            file_output_path,
        })
    }

    fn commit_edit_buffer(&mut self) {
        let Some(edit_state) = self.edit_state.take() else {
            return;
        };
        if let Some(field) = self.fields.get_mut(edit_state.field_index) {
            field.set_text(edit_state.buffer);
        }
    }

    fn field_text(&self, id: &str) -> Option<String> {
        self.fields
            .iter()
            .find(|field| field.schema.id == id)
            .and_then(|field| field.text_value())
    }

    fn bool_field_value(&self, id: &str) -> Option<bool> {
        self.fields
            .iter()
            .find(|field| field.schema.id == id)
            .and_then(|field| match &field.value {
                FormValue::Toggle(value) => Some(*value),
                _ => None,
            })
    }

    fn verbose_enabled(&self) -> bool {
        self.bool_field_value("verbose").unwrap_or(true)
    }

    fn listunfl_enabled(&self) -> bool {
        self.bool_field_value("listunfl").unwrap_or(false)
    }

    fn visible_result_tabs(&self) -> &'static [ResultTab] {
        const COMPACT: [ResultTab; 8] = [
            ResultTab::Overview,
            ResultTab::Observations,
            ResultTab::Fields,
            ResultTab::Spws,
            ResultTab::Antennas,
            ResultTab::Uv,
            ResultTab::Stdout,
            ResultTab::Stderr,
        ];
        if self.verbose_enabled() {
            &ResultTab::ALL
        } else {
            &COMPACT
        }
    }

    fn sync_result_tab_visibility(&mut self) {
        if !self.visible_result_tabs().contains(&self.active_result_tab) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.active_result_tab = ResultTab::Overview;
        }
    }

    fn selection_inputs_present(&self) -> bool {
        self.fields.iter().any(|field| {
            field.schema.group == "Selection"
                && field.schema.id != "selectdata"
                && match &field.value {
                    FormValue::Text(value) => !value.trim().is_empty(),
                    FormValue::Choice { value, .. } => !value.trim().is_empty(),
                    FormValue::Toggle(_) => false,
                }
        })
    }

    fn append_effective_selectdata_argument(
        &self,
        arguments: &mut Vec<OsString>,
        force_selectdata: bool,
    ) -> Result<(), String> {
        let Some(field) = self
            .fields
            .iter()
            .find(|field| field.schema.id == "selectdata")
        else {
            return Ok(());
        };
        let FormValue::Toggle(raw_value) = &field.value else {
            return Err("internal selectdata field mismatch".to_string());
        };
        let effective = *raw_value || force_selectdata;
        let UiArgumentParser::Toggle {
            true_flags,
            false_flags,
        } = &field.schema.parser
        else {
            return Err("internal selectdata parser mismatch".to_string());
        };
        match (effective, true_flags.first(), false_flags.first()) {
            (true, Some(flag), _) => arguments.push(OsString::from(flag)),
            (false, _, Some(flag)) => arguments.push(OsString::from(flag)),
            _ => {}
        }
        Ok(())
    }

    fn cancel_current(&mut self) {
        if let Some(session) = self.browser_session.take() {
            let _ = session.client.cancel();
            self.result.status_line = "Browser session closed.".to_string();
            self.result.status_kind = StatusKind::Info;
            self.pane_focus = PaneFocus::Parameters;
            return;
        }

        if let Some(loading) = self.uv_loading.as_mut() {
            if loading.cancel_requested {
                return;
            }
            match loading.process.cancel() {
                Ok(()) => {
                    loading.cancel_requested = true;
                    self.result.status_line =
                        "Cancel requested for UV coverage load...".to_string();
                    self.result.status_kind = StatusKind::Warning;
                }
                Err(error) => {
                    self.result.status_line = "Failed to cancel UV coverage load.".to_string();
                    self.result.status_kind = StatusKind::Error;
                    self.result.stderr.push_str(&format!("{error}\n"));
                    self.active_result_tab = ResultTab::Stderr;
                }
            }
            return;
        }

        let Some(running) = self.running.as_mut() else {
            return;
        };
        if running.cancel_requested {
            return;
        }
        match running.process.cancel() {
            Ok(()) => {
                running.cancel_requested = true;
                self.result.status_line = format!("Cancel requested for {}...", self.app.id);
                self.result.status_kind = StatusKind::Warning;
            }
            Err(error) => {
                self.result.status_line = "Failed to cancel running command.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr.push_str(&format!("{error}\n"));
                self.active_result_tab = ResultTab::Stderr;
            }
        }
    }

    fn finish_execution(&mut self, exit_code: Option<i32>, success: bool) {
        let Some(running) = self.running.take() else {
            return;
        };
        self.result.exit_code = exit_code;
        self.result.file_output_path = running.file_output_path.clone();

        if running.cancel_requested {
            self.last_listobs_uv_plan = None;
            self.result.status_line = "Execution canceled.".to_string();
            self.result.status_kind = StatusKind::Warning;
            self.result.structured = None;
            self.result.structured_error = Some(
                "The child process was canceled before a structured result was available."
                    .to_string(),
            );
            self.active_result_tab = if !self.result.stderr.is_empty() {
                ResultTab::Stderr
            } else {
                ResultTab::Stdout
            };
            return;
        }

        if success {
            self.result.status_line = "Execution completed successfully.".to_string();
            self.result.status_kind = StatusKind::Ok;
            if let Some(path) = running.file_output_path {
                self.last_listobs_uv_plan = None;
                self.result.structured = None;
                self.result.structured_error = None;
                self.result.file_output_path = Some(path);
                self.active_result_tab = ResultTab::Overview;
                return;
            }

            if matches!(running.renderer.as_deref(), Some("listobs-summary-v1")) {
                match serde_json::from_str::<ListObsSummary>(&self.result.stdout) {
                    Ok(summary) => {
                        self.last_listobs_uv_plan = running.uv_plan;
                        self.result.structured = Some(summary);
                        self.result.structured_error = None;
                        self.activate_result_tab(ResultTab::Overview);
                    }
                    Err(error) => {
                        self.last_listobs_uv_plan = None;
                        self.result.structured = None;
                        self.result.structured_error =
                            Some(format!("Failed to parse structured output: {error}"));
                        self.result.status_line =
                            "Execution completed, but structured rendering failed.".to_string();
                        self.result.status_kind = StatusKind::Warning;
                        self.active_result_tab = if !self.result.stdout.is_empty() {
                            ResultTab::Stdout
                        } else {
                            ResultTab::Stderr
                        };
                    }
                }
            } else {
                self.activate_result_tab(if !self.result.stdout.is_empty() {
                    ResultTab::Stdout
                } else {
                    ResultTab::Overview
                });
            }
        } else {
            self.last_listobs_uv_plan = None;
            self.result.status_line = "Execution failed.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.result.structured = None;
            self.result.structured_error = None;
            self.activate_result_tab(if !self.result.stderr.is_empty() {
                ResultTab::Stderr
            } else {
                ResultTab::Stdout
            });
        }
    }

    fn overview_lines(&self) -> Vec<String> {
        if let Some(summary) = &self.result.structured {
            let mut lines = Vec::new();
            lines.push("MeasurementSet".to_string());
            lines.push(format!(
                "Path: {}",
                summary
                    .measurement_set
                    .path
                    .clone()
                    .unwrap_or_else(|| "<in-memory>".to_string())
            ));
            lines.push(format!(
                "Rows: {}   Observations: {}   Fields: {}   SPWs: {}   Antennas: {}",
                summary.measurement_set.row_count,
                summary.measurement_set.observation_count,
                summary.measurement_set.field_count,
                summary.measurement_set.spectral_window_count,
                summary.measurement_set.antenna_count
            ));
            lines.push(format!(
                "Time range: {} -> {}",
                format_optional_mjd_timestamp(summary.measurement_set.start_mjd_seconds),
                format_optional_mjd_timestamp(summary.measurement_set.end_mjd_seconds)
            ));
            lines.push(String::new());
            lines.push("Badges".to_string());
            lines.push(format!(
                "MS_VERSION={}  TimeRef={}  Sources={}  DDIDs={}",
                format_optional_float(summary.measurement_set.ms_version),
                summary
                    .measurement_set
                    .time_reference
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                summary.measurement_set.source_count,
                summary.measurement_set.data_description_count
            ));
            lines.push(String::new());
            lines.push("Current tabs".to_string());
            lines.push(
                "Use h/l or Left/Right to switch between Overview, Observations, Fields, SPWs, Antennas, Stdout, and Stderr."
                    .to_string(),
            );
            return lines;
        }

        if let Some(path) = &self.result.file_output_path {
            return vec![
                "Overview".to_string(),
                format!("Output written to: {path}"),
                "Switch to Stdout or Stderr if the command emitted terminal output.".to_string(),
            ];
        }

        if self.running.is_some() {
            return vec![
                "Overview".to_string(),
                "The process is still running.".to_string(),
                "Structured result tabs will populate when the command exits.".to_string(),
            ];
        }

        if let Some(session) = self.browser_session() {
            return session.snapshot.content_lines.clone();
        }

        if let Some(error) = &self.result.structured_error {
            return vec![
                "Overview".to_string(),
                error.clone(),
                "Switch to Stdout or Stderr for raw process output.".to_string(),
            ];
        }

        vec![
            "Overview".to_string(),
            "No structured result available yet.".to_string(),
        ]
    }
}

fn spinner_frames(theme_mode: ThemeMode) -> &'static [&'static str] {
    match theme_mode {
        ThemeMode::DenseAnsi => DENSE_SPINNER_FRAMES,
        ThemeMode::RichPanel => RICH_SPINNER_FRAMES,
    }
}

impl FormField {
    fn from_schema(schema: &UiArgumentSchema) -> Option<Self> {
        if schema.hidden_in_tui || matches!(schema.parser, UiArgumentParser::Action { .. }) {
            return None;
        }

        let value = match &schema.parser {
            UiArgumentParser::Positional { .. } => {
                FormValue::Text(schema.default.clone().unwrap_or_default())
            }
            UiArgumentParser::Option { choices, .. } => {
                if choices.is_empty() {
                    FormValue::Text(schema.default.clone().unwrap_or_default())
                } else {
                    FormValue::Choice {
                        value: schema.default.clone().unwrap_or_else(|| choices[0].clone()),
                        choices: choices.clone(),
                    }
                }
            }
            UiArgumentParser::Toggle { .. } => {
                FormValue::Toggle(schema.default_bool().unwrap_or(false))
            }
            UiArgumentParser::Action { .. } => return None,
        };

        Some(Self {
            schema: schema.clone(),
            value,
        })
    }

    fn render_line(&self, edit_state: Option<&EditState>, field_index: usize) -> String {
        let value = match (&self.value, edit_state) {
            (_, Some(edit_state)) if edit_state.field_index == field_index => {
                format!("{}|", edit_state.buffer)
            }
            (FormValue::Text(value), _) if value.trim().is_empty() && self.schema.required => {
                "<required>".to_string()
            }
            (FormValue::Text(value), _) if value.trim().is_empty() => "<empty>".to_string(),
            (FormValue::Text(value), _) => value.clone(),
            (FormValue::Toggle(value), _) => {
                if *value {
                    "[x]".to_string()
                } else {
                    "[ ]".to_string()
                }
            }
            (FormValue::Choice { value, .. }, _) => value.clone(),
        };
        let mut rendered = format!("{:<18} {}", self.schema.label, value);
        if self.is_path() {
            rendered.push_str(BROWSE_SUFFIX);
        }
        rendered
    }

    fn is_path(&self) -> bool {
        self.schema.value_kind == UiValueKind::Path
    }

    fn append_arguments(&self, arguments: &mut Vec<OsString>) -> Result<(), String> {
        match (&self.schema.parser, &self.value) {
            (UiArgumentParser::Positional { .. }, FormValue::Text(value)) => {
                if self.schema.required && value.trim().is_empty() {
                    return Err(format!("{} is required.", self.schema.label));
                }
                if !value.trim().is_empty() {
                    arguments.push(path_argument_value(self.is_path(), value));
                }
            }
            (UiArgumentParser::Option { flags, .. }, FormValue::Text(value)) => {
                if !value.trim().is_empty() {
                    arguments.push(OsString::from(&flags[0]));
                    arguments.push(path_argument_value(self.is_path(), value));
                }
            }
            (UiArgumentParser::Option { flags, .. }, FormValue::Choice { value, .. }) => {
                arguments.push(OsString::from(&flags[0]));
                arguments.push(OsString::from(value));
            }
            (
                UiArgumentParser::Toggle {
                    true_flags,
                    false_flags,
                },
                FormValue::Toggle(value),
            ) => match (*value, true_flags.first(), false_flags.first()) {
                (true, Some(flag), _) => arguments.push(OsString::from(flag)),
                (false, _, Some(flag)) => arguments.push(OsString::from(flag)),
                _ => {}
            },
            _ => return Err(format!("internal argument mismatch for {}", self.schema.id)),
        }
        Ok(())
    }

    fn cycle_choice(&mut self, forward: bool) {
        let FormValue::Choice { value, choices } = &mut self.value else {
            return;
        };
        if choices.is_empty() {
            return;
        }
        let position = choices
            .iter()
            .position(|choice| choice == value)
            .unwrap_or(0);
        let next = if forward {
            (position + 1) % choices.len()
        } else if position == 0 {
            choices.len() - 1
        } else {
            position - 1
        };
        *value = choices[next].clone();
    }

    fn toggle(&mut self) {
        if let FormValue::Toggle(value) = &mut self.value {
            *value = !*value;
        }
    }

    fn set_text(&mut self, value: String) {
        let _ = self.apply_text_value(value);
    }

    fn apply_text_value(&mut self, value: String) -> Result<(), String> {
        match &mut self.value {
            FormValue::Text(current) => {
                *current = value;
                Ok(())
            }
            FormValue::Choice {
                value: current,
                choices,
            } => {
                if !choices.is_empty() && !choices.iter().any(|choice| choice == &value) {
                    return Err(format!(
                        "{} expects one of: {}",
                        self.schema.label,
                        choices.join(", ")
                    ));
                }
                *current = value;
                Ok(())
            }
            FormValue::Toggle(_) => {
                Err(format!("{} does not accept text input", self.schema.label))
            }
        }
    }

    fn apply_toggle_value(&mut self, value: bool) -> Result<(), String> {
        match &mut self.value {
            FormValue::Toggle(current) => {
                *current = value;
                Ok(())
            }
            _ => Err(format!("{} is not a toggle", self.schema.label)),
        }
    }

    fn text_value(&self) -> Option<String> {
        match &self.value {
            FormValue::Text(value) => Some(value.clone()),
            FormValue::Choice { value, .. } => Some(value.clone()),
            FormValue::Toggle(_) => None,
        }
    }
}

fn build_sections(fields: &[FormField]) -> Vec<FormSection> {
    let mut sections = Vec::<FormSection>::new();
    for (field_index, field) in fields.iter().enumerate() {
        if let Some(section) = sections
            .iter_mut()
            .find(|section| section.name == field.schema.group)
        {
            section.field_indices.push(field_index);
        } else {
            sections.push(FormSection {
                collapsed: !matches!(
                    field.schema.group.as_str(),
                    "Input" | "Presentation" | "Selection"
                ),
                name: field.schema.group.clone(),
                field_indices: vec![field_index],
            });
        }
    }
    sections
}

fn initial_form_selection(
    sections: &[FormSection],
    fields: &[FormField],
    show_advanced: bool,
) -> FormSelection {
    for (section_index, section) in sections.iter().enumerate() {
        let visible_fields = section
            .field_indices
            .iter()
            .copied()
            .filter(|index| show_advanced || !fields[*index].schema.advanced)
            .collect::<Vec<_>>();
        if visible_fields.is_empty() {
            continue;
        }
        if section.collapsed {
            return FormSelection::Section(section_index);
        }
        return FormSelection::Field(visible_fields[0]);
    }
    FormSelection::Section(0)
}

fn inject_managed_arguments(arguments: &mut Vec<OsString>, managed_output: &UiManagedOutputSchema) {
    for argument in &managed_output.inject_arguments {
        arguments.push(OsString::from(&argument.flag));
        arguments.push(OsString::from(&argument.value));
    }
}

fn raw_lines(label: &str, text: &str) -> Vec<String> {
    if text.trim().is_empty() {
        vec![format!("No {label} output.")]
    } else {
        text.lines().map(ToString::to_string).collect()
    }
}

fn build_observations_table(summary: &ListObsSummary) -> TableView {
    let header_cells = vec![
        "Obs".to_string(),
        "Telescope".to_string(),
        "Observer".to_string(),
        "Project".to_string(),
        "Start".to_string(),
        "End".to_string(),
    ];
    let mut widths = header_cells
        .iter()
        .map(|cell| cell.chars().count())
        .collect::<Vec<_>>();
    let row_cells = summary
        .observations
        .iter()
        .map(|observation| {
            let cells = vec![
                observation.observation_id.to_string(),
                truncate(&observation.telescope_name, 10),
                truncate(&observation.observer, 10),
                truncate(&observation.project, 12),
                format_optional_mjd_timestamp(observation.start_mjd_seconds),
                format_optional_mjd_timestamp(observation.end_mjd_seconds),
            ];
            for (index, cell) in cells.iter().enumerate() {
                widths[index] = widths[index].max(cell.chars().count());
            }
            cells
        })
        .collect::<Vec<_>>();
    let rows = row_cells
        .iter()
        .map(|cells| format_columns(cells, &widths))
        .collect();
    TableView {
        header: format_columns(&header_cells, &widths),
        rows,
    }
}

fn build_scans_table(summary: &ListObsSummary, listunfl: bool) -> TableView {
    let rows = summary
        .scans
        .iter()
        .map(|scan| {
            let start = MvTime::from_mjd_seconds(scan.start_mjd_seconds).format_dmy(1);
            let end = MvTime::from_mjd_seconds(scan.end_mjd_seconds).format_time(1);
            let field_names = if scan.field_names.is_empty() {
                scan.field_name.clone()
            } else {
                scan.field_names.join(",")
            };
            let intents = if scan.scan_intents.is_empty() {
                "-".to_string()
            } else {
                scan.scan_intents.join(",")
            };
            if listunfl {
                format!(
                    "{:<3} {:<3} {:<23} {:<10} {:<4} {:<4} {:<18} {:<7} {:<8} {:<7} {:<8} {}",
                    scan.observation_id,
                    scan.array_id,
                    start,
                    end,
                    scan.scan_number,
                    scan.field_id,
                    truncate(&field_names, 18),
                    scan.row_count,
                    format_float_compact(scan.unflagged_row_count.unwrap_or(0.0), 2),
                    format_i32_list(&scan.spectral_window_ids),
                    format_float_list(&scan.mean_interval_seconds_by_spw, 2),
                    truncate(&intents, 28),
                )
            } else {
                format!(
                    "{:<3} {:<3} {:<23} {:<10} {:<4} {:<4} {:<20} {:<7} {:<7} {:<8} {}",
                    scan.observation_id,
                    scan.array_id,
                    start,
                    end,
                    scan.scan_number,
                    scan.field_id,
                    truncate(&field_names, 20),
                    scan.row_count,
                    format_i32_list(&scan.spectral_window_ids),
                    format_float_list(&scan.mean_interval_seconds_by_spw, 2),
                    truncate(&intents, 32),
                )
            }
        })
        .collect();
    let header = if listunfl {
        format!(
            "{:<3} {:<3} {:<23} {:<10} {:<4} {:<4} {:<18} {:<7} {:<8} {:<7} {:<8} {}",
            "Obs",
            "Arr",
            "Start",
            "End",
            "Scan",
            "Fld",
            "Field",
            "Rows",
            "nUnfl",
            "Spws",
            "Int(s)",
            "Intent"
        )
    } else {
        format!(
            "{:<3} {:<3} {:<23} {:<10} {:<4} {:<4} {:<20} {:<7} {:<7} {:<8} {}",
            "Obs",
            "Arr",
            "Start",
            "End",
            "Scan",
            "Fld",
            "Field",
            "Rows",
            "Spws",
            "Int(s)",
            "Intent"
        )
    };
    TableView { header, rows }
}

fn build_fields_table(summary: &ListObsSummary, listunfl: bool) -> TableView {
    let rows = summary
        .fields
        .iter()
        .map(|field| {
            let ra = MvAngle::from_radians(field.phase_direction_radians[0])
                .normalized(0.0)
                .format_time(6);
            let dec = MvAngle::from_radians(field.phase_direction_radians[1]).format_angle_dig2(5);
            if listunfl {
                format!(
                    "{:<5} {:<5} {:<20} {:<16} {:<16} {:<14} {:<6} {:<7} {}",
                    field.field_id,
                    truncate(&field.code, 5),
                    truncate(&field.name, 20),
                    ra,
                    dec,
                    truncate(field.direction_reference.as_deref().unwrap_or("?"), 14),
                    field.source_id,
                    field.row_count,
                    format_float_compact(field.unflagged_row_count.unwrap_or(0.0), 2),
                )
            } else {
                format!(
                    "{:<5} {:<5} {:<20} {:<16} {:<16} {:<14} {:<6} {}",
                    field.field_id,
                    truncate(&field.code, 5),
                    truncate(&field.name, 20),
                    ra,
                    dec,
                    truncate(field.direction_reference.as_deref().unwrap_or("?"), 14),
                    field.source_id,
                    field.row_count,
                )
            }
        })
        .collect();
    let header = if listunfl {
        format!(
            "{:<5} {:<5} {:<20} {:<16} {:<16} {:<14} {:<6} {:<7} {}",
            "ID", "Code", "Name", "RA", "Dec", "Epoch", "SrcId", "nRows", "nUnflRows"
        )
    } else {
        format!(
            "{:<5} {:<5} {:<20} {:<16} {:<16} {:<14} {:<6} {}",
            "ID", "Code", "Name", "RA", "Dec", "Epoch", "SrcId", "nRows"
        )
    };
    TableView { header, rows }
}

fn build_spws_table(summary: &ListObsSummary) -> TableView {
    let rows = summary
        .spectral_windows
        .iter()
        .map(|spw| {
            format!(
                "{:<5} {:<8} {:<6} {:<7} {:<11} {:<12} {:<11} {:<12} {}",
                spw.spectral_window_id,
                truncate(&spw_display_name(spw.name.as_str()), 8),
                spw.num_channels,
                truncate(spw.frame.as_deref().unwrap_or("unknown"), 7),
                format!("{:.3}", spw.first_channel_frequency_hz / 1.0e6),
                format!("{:.3}", spw.channel_width_hz / 1.0e3),
                format!("{:.1}", spw.total_bandwidth_hz / 1.0e3),
                format!("{:.4}", spw.center_frequency_hz / 1.0e6),
                join_corrs(&spw.correlation_types),
            )
        })
        .collect();
    TableView {
        header: format!(
            "{:<5} {:<8} {:<6} {:<7} {:<11} {:<12} {:<11} {:<12} {}",
            "SpwID",
            "Name",
            "#Chans",
            "Frame",
            "Ch0(MHz)",
            "ChanWid(kHz)",
            "TotBW(kHz)",
            "CtrFreq(MHz)",
            "Corrs"
        ),
        rows,
    }
}

fn build_sources_table(summary: &ListObsSummary) -> TableView {
    let rows = summary
        .sources
        .iter()
        .map(|source| {
            format!(
                "{:<5} {:<20} {:<5} {:<14} {}",
                source.source_id,
                truncate(&source.name, 20),
                if source.spectral_window_id < 0 {
                    "any".to_string()
                } else {
                    source.spectral_window_id.to_string()
                },
                source
                    .rest_frequency_hz
                    .map(|value| format!("{:.6}", value / 1.0e6))
                    .unwrap_or_else(|| "-".to_string()),
                source
                    .system_velocity_m_s
                    .map(|value| format_float_compact(value / 1.0e3, 3))
                    .unwrap_or_else(|| "-".to_string()),
            )
        })
        .collect();
    TableView {
        header: format!(
            "{:<5} {:<20} {:<5} {:<14} {}",
            "ID", "Name", "SpwId", "RestFreq(MHz)", "SysVel(km/s)"
        ),
        rows,
    }
}

fn build_antennas_table(summary: &ListObsSummary) -> TableView {
    let rows = summary
        .antennas
        .iter()
        .map(|antenna| {
            format!(
                "{:<4} {:<6} {:<10} {:>5.1} {:<14} {:<13} {:>9.1} {:>9.1} {:>10.1} {:>14.3} {:>14.3} {:>14.3}",
                antenna.antenna_id,
                truncate(&antenna.name, 6),
                truncate(&antenna.station, 10),
                antenna.dish_diameter_m,
                MvAngle::from_radians(antenna.longitude_radians).format_angle(1),
                MvAngle::from_radians(antenna.latitude_radians).format_angle_dig2(1),
                antenna.offset_from_observatory_m[0],
                antenna.offset_from_observatory_m[1],
                antenna.offset_from_observatory_m[2],
                antenna.position_m[0],
                antenna.position_m[1],
                antenna.position_m[2],
            )
        })
        .collect();
    TableView {
        header: format!(
            "{:<4} {:<6} {:<10} {:<5} {:<14} {:<13} {:>9} {:>9} {:>10} {:>14} {:>14} {:>14}",
            "ID",
            "Name",
            "Station",
            "Diam.",
            "Long.",
            "Lat.",
            "East",
            "North",
            "Elev",
            "ITRF x",
            "ITRF y",
            "ITRF z"
        ),
        rows,
    }
}

fn build_compact_antenna_lines(summary: &ListObsSummary) -> Vec<String> {
    let mut lines = vec![format!(
        "Antennas: {} 'name'='station'",
        summary.antennas.len()
    )];
    if summary.antennas.is_empty() {
        return lines;
    }
    let mut line = String::new();
    let mut first_id = summary.antennas[0].antenna_id;
    let mut last_id = summary.antennas[0].antenna_id;
    let max_id = summary
        .antennas
        .iter()
        .map(|antenna| antenna.antenna_id)
        .max()
        .unwrap_or(first_id);

    for antenna in &summary.antennas {
        let fragment = format!("'{}'='{}'", antenna.name, antenna.station);
        let mut candidate = line.clone();
        if !candidate.is_empty() {
            candidate.push_str(", ");
        }
        candidate.push_str(&fragment);
        if candidate.len() > 55 && !line.is_empty() {
            lines.push(format!("ID={first_id}-{last_id}: {line}"));
            line = fragment;
            first_id = antenna.antenna_id;
        } else {
            line = candidate;
        }
        last_id = antenna.antenna_id;
        if antenna.antenna_id == max_id {
            lines.push(format!("ID={first_id}-{last_id}: {line}"));
        }
    }
    lines
}

fn format_optional_float(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_mjd_timestamp(value: Option<f64>) -> String {
    value
        .map(|value| MvTime::from_mjd_seconds(value).format_dmy(1))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_float_compact(value: f64, decimals: usize) -> String {
    let mut text = format!("{value:.decimals$}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn format_i32_list(values: &[i32]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn format_float_list(values: &[f64], decimals: usize) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format_float_compact(*value, decimals))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn join_corrs(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("{value:<4}"))
        .collect::<String>()
        .trim_end()
        .to_string()
}

impl VisibleTextLine {
    fn plain(text: String) -> Self {
        let roles = text.chars().map(|_| VisibleTextRole::Plain).collect();
        Self { text, roles }
    }

    fn table_header(text: String) -> Self {
        let roles = text.chars().map(|_| VisibleTextRole::TableHeader).collect();
        Self { text, roles }
    }
}

fn rect_contains(rect: Rect, column: u16, row: u16) -> bool {
    column >= rect.x
        && column < rect.x.saturating_add(rect.width)
        && row >= rect.y
        && row < rect.y.saturating_add(rect.height)
}

fn result_text_area(layout: &UiLayout) -> Rect {
    Rect {
        x: layout.result_content.x,
        y: layout.result_content.y,
        width: layout
            .result_content
            .width
            .saturating_sub(if layout.result_scrollbar.is_some() {
                1
            } else {
                0
            }),
        height: layout.result_content.height.saturating_sub(
            if layout.result_hscrollbar.is_some() {
                1
            } else {
                0
            },
        ),
    }
}

fn left_output_area(app: &AppState, layout: &UiLayout) -> Option<Rect> {
    if app.browser_is_active() && layout.form_inner.width > 0 && layout.form_inner.height > 0 {
        Some(layout.form_inner)
    } else {
        None
    }
}

fn normalize_selection(selection: OutputSelection) -> (usize, usize, usize, usize) {
    (
        selection.anchor.row.min(selection.cursor.row),
        selection.anchor.row.max(selection.cursor.row),
        selection.anchor.col.min(selection.cursor.col),
        selection.anchor.col.max(selection.cursor.col),
    )
}

fn clamp_point_to_buffer(buffer: &VisibleTextBuffer, column: u16, row: u16) -> BufferPoint {
    let relative_row = row.saturating_sub(buffer.area.y) as usize;
    let row = relative_row.min(buffer.lines.len().saturating_sub(1));
    let line_len = buffer
        .lines
        .get(row)
        .map(|line| line.text.chars().count())
        .unwrap_or(0);
    let relative_col = column.saturating_sub(buffer.area.x) as usize;
    let col = if line_len == 0 {
        0
    } else {
        relative_col.min(line_len.saturating_sub(1))
    };
    BufferPoint { row, col }
}

fn extract_selected_text(buffer: &VisibleTextBuffer, selection: OutputSelection) -> String {
    let (row_start, row_end, col_start, col_end) = normalize_selection(selection);
    (row_start..=row_end)
        .filter_map(|row| buffer.lines.get(row))
        .map(|line| {
            slice_chars(&line.text, col_start, col_end.saturating_add(1))
                .trim_end_matches(' ')
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn browser_cells_visible_line(raw_line: &str) -> VisibleTextLine {
    if !raw_line.contains('|') {
        return VisibleTextLine::plain(raw_line.to_string());
    }

    let is_header = raw_line.trim_start().starts_with("row ");
    let mut text = String::new();
    let mut roles = Vec::new();
    for (index, segment) in raw_line.split('|').enumerate() {
        if index > 0 {
            text.push('│');
            roles.push(VisibleTextRole::BrowserSeparator);
        }
        let (visible, role) = if let Some(selected) = strip_browser_selection_markers(segment) {
            (selected, VisibleTextRole::BrowserSelectedCell)
        } else if is_header {
            (segment.to_string(), VisibleTextRole::TableHeader)
        } else {
            (segment.to_string(), VisibleTextRole::Plain)
        };
        roles.extend(visible.chars().map(|_| role));
        text.push_str(&visible);
    }
    VisibleTextLine { text, roles }
}

fn strip_browser_selection_markers(segment: &str) -> Option<String> {
    let mut chars = segment.chars().collect::<Vec<_>>();
    let first = chars
        .iter()
        .position(|character| !character.is_whitespace())?;
    let last = chars
        .iter()
        .rposition(|character| !character.is_whitespace())?;
    if chars[first] != '>' || chars[last] != '<' {
        return None;
    }
    chars[first] = ' ';
    chars[last] = ' ';
    Some(chars.into_iter().collect())
}

fn slice_visible_text(text: &str, offset: usize, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let sliced = text.chars().skip(offset).collect::<String>();
    fit_visible_text(&sliced, width)
}

fn fit_visible_text(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if text.chars().count() <= width {
        return text.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let mut out = text.chars().take(width - 3).collect::<String>();
    out.push_str("...");
    out
}

fn slice_chars(text: &str, start: usize, end: usize) -> String {
    text.chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

fn is_browser_copy_modifier(modifiers: KeyModifiers) -> bool {
    modifiers == KeyModifiers::SUPER || modifiers == (KeyModifiers::CONTROL | KeyModifiers::SHIFT)
}

fn copyable_browser_text(inspector: &BrowserInspectorSnapshot) -> (String, &'static str) {
    match &inspector.node {
        BrowserValueNode::Undefined => ("<undefined>".to_string(), "undefined value"),
        BrowserValueNode::Scalar { value } => (render_browser_scalar(value), "value"),
        BrowserValueNode::TableRef { resolved_path, .. } => (resolved_path.clone(), "table path"),
        BrowserValueNode::Array {
            shape,
            total_elements,
            elements,
            ..
        } if elements.len() == *total_elements => (
            format!(
                "[{}]",
                elements
                    .iter()
                    .map(|element| render_browser_scalar(&element.value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            if shape.len() > 1 {
                "array value"
            } else {
                "value"
            },
        ),
        _ => (
            inspector.rendered_lines.join("\n").trim().to_string(),
            "inspector view",
        ),
    }
}

fn render_browser_scalar(value: &BrowserScalarValue) -> String {
    match value {
        BrowserScalarValue::Bool(value) => value.to_string(),
        BrowserScalarValue::UInt8(value) => value.to_string(),
        BrowserScalarValue::UInt16(value) => value.to_string(),
        BrowserScalarValue::UInt32(value) => value.to_string(),
        BrowserScalarValue::Int16(value) => value.to_string(),
        BrowserScalarValue::Int32(value) => value.to_string(),
        BrowserScalarValue::Int64(value) => value.to_string(),
        BrowserScalarValue::Float32(value) => format_browser_float(f64::from(*value)),
        BrowserScalarValue::Float64(value) => format_browser_float(*value),
        BrowserScalarValue::Complex32(value) => render_complex32(value),
        BrowserScalarValue::Complex64(value) => render_complex64(value),
        BrowserScalarValue::String(value) => format!("{value:?}"),
    }
}

fn render_complex32(value: &BrowserComplex32Value) -> String {
    format!(
        "{}{:+}i",
        format_browser_float(f64::from(value.re)),
        f64::from(value.im)
    )
}

fn render_complex64(value: &BrowserComplex64Value) -> String {
    format!("{}{:+}i", format_browser_float(value.re), value.im)
}

fn format_browser_float(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        let rendered = format!("{value:.15}");
        rendered
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

fn spw_display_name(name: &str) -> String {
    if name.is_empty() {
        "none".to_string()
    } else {
        name.to_string()
    }
}

fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let mut truncated = value.chars().take(width - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn format_columns(cells: &[String], widths: &[usize]) -> String {
    let mut line = String::new();
    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            line.push(' ');
        }
        line.push_str(cell);
        let padding = widths[index].saturating_sub(cell.chars().count());
        if padding > 0 {
            line.push_str(&" ".repeat(padding));
        }
    }
    line
}

fn scrollbar_thumb_length(
    track_length: usize,
    content_length: usize,
    viewport_length: usize,
) -> usize {
    if track_length == 0 || content_length == 0 {
        return 1;
    }
    let thumb = (track_length * viewport_length).div_ceil(content_length);
    thumb.clamp(1, track_length)
}

fn chooser_start_path(value: Option<&str>) -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return cwd;
    };
    let candidate = expand_tilde_path(raw);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        cwd.join(candidate)
    };
    if candidate.exists() {
        return candidate;
    }
    candidate
        .ancestors()
        .find(|path| path.exists())
        .map(Path::to_path_buf)
        .unwrap_or(cwd)
}

fn path_argument_value(is_path: bool, value: &str) -> OsString {
    if is_path {
        expand_tilde_path(value.trim()).into_os_string()
    } else {
        OsString::from(value)
    }
}

fn expand_tilde_path(raw: &str) -> PathBuf {
    expand_tilde_path_with_home(raw, home_dir_path())
}

fn expand_tilde_path_with_home(raw: &str, home: Option<&Path>) -> PathBuf {
    if raw == "~" {
        return home
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(raw));
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = home {
            return home.join(rest);
        }
    }
    PathBuf::from(raw)
}

fn home_dir_path() -> Option<&'static Path> {
    static HOME: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
    HOME.get_or_init(|| std::env::var_os("HOME").map(PathBuf::from))
        .as_deref()
}

fn absolute_display_path(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| {
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("."))
                    .join(path)
            }
        })
        .display()
        .to_string()
}

fn chooser_visible_start(selected: usize, total: usize, visible_height: usize) -> usize {
    if total == 0 || visible_height == 0 || total <= visible_height {
        return 0;
    }
    let half = visible_height / 2;
    selected
        .saturating_sub(half)
        .min(total.saturating_sub(visible_height))
}

fn chooser_input_from_key(key_event: KeyEvent) -> Option<ExplorerInput> {
    if !(key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT) {
        return None;
    }
    Some(match key_event.code {
        KeyCode::Up | KeyCode::Char('k') => ExplorerInput::Up,
        KeyCode::Down | KeyCode::Char('j') => ExplorerInput::Down,
        KeyCode::Left | KeyCode::Backspace | KeyCode::Char('h') => ExplorerInput::Left,
        KeyCode::Right | KeyCode::Char('l') => ExplorerInput::Right,
        KeyCode::Home => ExplorerInput::Home,
        KeyCode::End => ExplorerInput::End,
        KeyCode::PageUp => ExplorerInput::PageUp,
        KeyCode::PageDown => ExplorerInput::PageDown,
        _ => return None,
    })
}

fn resolve_parameter_action(key_event: KeyEvent) -> Option<ParameterAction> {
    match key_event.code {
        KeyCode::Up | KeyCode::Char('k')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(ParameterAction::SelectPrevious)
        }
        KeyCode::Down | KeyCode::Char('j')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(ParameterAction::SelectNext)
        }
        KeyCode::Left if key_event.modifiers.is_empty() => Some(ParameterAction::ChoicePrevious),
        KeyCode::Right if key_event.modifiers.is_empty() => Some(ParameterAction::ChoiceNext),
        KeyCode::Enter | KeyCode::Char(' ') if key_event.modifiers.is_empty() => {
            Some(ParameterAction::Activate)
        }
        _ => None,
    }
}

fn resolve_result_action(key_event: KeyEvent) -> Option<ResultAction> {
    match key_event.code {
        KeyCode::Left | KeyCode::Char('h')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(ResultAction::PreviousTab)
        }
        KeyCode::Right | KeyCode::Char('l')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(ResultAction::NextTab)
        }
        KeyCode::Up | KeyCode::Char('k')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(ResultAction::Scroll(-1))
        }
        KeyCode::Down | KeyCode::Char('j')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(ResultAction::Scroll(1))
        }
        KeyCode::PageUp if key_event.modifiers.is_empty() => Some(ResultAction::Scroll(-10)),
        KeyCode::PageDown if key_event.modifiers.is_empty() => Some(ResultAction::Scroll(10)),
        KeyCode::Char('[') if key_event.modifiers.is_empty() => {
            Some(ResultAction::ScrollHorizontal(-HORIZONTAL_SCROLL_STEP))
        }
        KeyCode::Char(']') if key_event.modifiers.is_empty() => {
            Some(ResultAction::ScrollHorizontal(HORIZONTAL_SCROLL_STEP))
        }
        KeyCode::Left if key_event.modifiers == KeyModifiers::CONTROL => {
            Some(ResultAction::ScrollHorizontal(-HORIZONTAL_SCROLL_STEP))
        }
        KeyCode::Right if key_event.modifiers == KeyModifiers::CONTROL => {
            Some(ResultAction::ScrollHorizontal(HORIZONTAL_SCROLL_STEP))
        }
        KeyCode::Char('v') if key_event.modifiers.is_empty() => Some(ResultAction::NextTab),
        _ => None,
    }
}

fn resolve_browser_action(key_event: KeyEvent) -> Option<BrowserAction> {
    match key_event.code {
        KeyCode::Left | KeyCode::Char('h')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(BrowserAction::MoveLeft)
        }
        KeyCode::Right | KeyCode::Char('l')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(BrowserAction::MoveRight)
        }
        KeyCode::Up | KeyCode::Char('k')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(BrowserAction::MoveUp)
        }
        KeyCode::Down | KeyCode::Char('j')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(BrowserAction::MoveDown)
        }
        KeyCode::PageUp if key_event.modifiers.is_empty() => Some(BrowserAction::PageUp),
        KeyCode::PageDown if key_event.modifiers.is_empty() => Some(BrowserAction::PageDown),
        KeyCode::Enter if key_event.modifiers.is_empty() => Some(BrowserAction::Activate),
        KeyCode::Backspace if key_event.modifiers.is_empty() => Some(BrowserAction::Back),
        KeyCode::Esc if key_event.modifiers.is_empty() => Some(BrowserAction::Escape),
        _ => None,
    }
}

fn resolve_edit_action(key_event: KeyEvent) -> Option<EditAction> {
    match key_event.code {
        KeyCode::Esc if key_event.modifiers.is_empty() => Some(EditAction::Cancel),
        KeyCode::Enter if key_event.modifiers.is_empty() => Some(EditAction::Commit),
        KeyCode::Backspace if key_event.modifiers.is_empty() => Some(EditAction::DeleteBackward),
        KeyCode::Char(character)
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(EditAction::Insert(character))
        }
        _ => None,
    }
}

fn browser_main_content_lines(snapshot: &BrowserSnapshot) -> Vec<String> {
    let mut lines = snapshot.content_lines.clone();
    let Some(inspector) = snapshot.inspector.as_ref() else {
        return lines;
    };
    let inspector_lines = &inspector.rendered_lines;
    if inspector_lines.is_empty() || lines.len() < inspector_lines.len() {
        return lines;
    }
    if lines.ends_with(inspector_lines) {
        let new_len = lines.len().saturating_sub(inspector_lines.len());
        lines.truncate(new_len);
        if lines.last().is_some_and(String::is_empty) {
            lines.pop();
        }
    }
    lines
}

fn browser_inspector_lines(inspector: &BrowserInspectorSnapshot) -> Vec<String> {
    let mut lines = vec![inspector.title.clone()];
    if !inspector.trail.is_empty() {
        lines.push(format!(
            "Path: {}",
            inspector
                .trail
                .iter()
                .map(|entry| entry.label.as_str())
                .collect::<Vec<_>>()
                .join(" / ")
        ));
    }
    if !lines.is_empty() {
        lines.push(String::new());
    }
    lines.extend(inspector.rendered_lines.iter().cloned());
    lines
}

#[cfg(test)]
mod tests {
    use super::{
        AppState, BrowserAction, BrowserSession, BrowserTab, BrowserValueNode, BufferPoint,
        FormSelection, FormValue, OutputPane, OutputSelection, OutputSelectionMode,
        PaneFocus, ParameterAction, ResultAction, ResultTab, RunningProcess,
        RunningState, StatusKind, UvCoverageLoadState, UvPlotPanelState, VisibleTextBuffer,
        VisibleTextLine, VisibleTextRole, browser_cells_visible_line, clamp_point_to_buffer,
        build_antennas_table, build_compact_antenna_lines, build_fields_table,
        build_observations_table, build_scans_table, build_sources_table, build_spws_table,
        copyable_browser_text, expand_tilde_path_with_home, extract_selected_text,
        fit_visible_text, format_float_compact, format_float_list, format_i32_list,
        format_optional_float, join_corrs, normalize_selection, render_browser_scalar,
        slice_chars, slice_visible_text, strip_browser_selection_markers,
    };
    use crate::execution::{ExecutionPlan, spawn_process};
    use crate::registry::{ResolvedCommand, listobs_app, tablebrowser_app};
    use casacore_tablebrowser_protocol::{
        BrowserArrayElement, BrowserBreadcrumbEntry, BrowserCapabilities, BrowserCommand,
        BrowserComplex32Value, BrowserComplex64Value, BrowserFocus, BrowserInspectorSnapshot,
        BrowserNavigationMetrics, BrowserResponseEnvelope, BrowserScalarValue, BrowserSnapshot,
        BrowserView as ProtocolBrowserView, BrowserViewport,
    };
    use casacore_ms::listobs::cli::command_schema;
    use casacore_ms::{
        ListObsOptions, ListObsSummary, ListObsUvCoverage, ListObsUvPoint, ListObsUvTrack,
    };
    use casacore_ms::listobs::{
        AntennaSummary, DataDescriptionSummary, FieldSummary, MeasurementSetInfo,
        ObservationSummary, PolarizationSummary, ScanSummary, SourceSummary,
        SpectralWindowSummary,
    };
    use image::DynamicImage;
    use ratatui::layout::Rect;
    use ratatui_graphics::{PanelRenderer, Picker, Resize};
    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;
    #[cfg(unix)]
    use tempfile::tempdir;

    #[cfg(unix)]
    use crate::browser_client::BrowserClient;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn test_app() -> AppState {
        AppState::from_schema(listobs_app(), command_schema("listobs"))
    }

    fn tablebrowser_test_app() -> AppState {
        let schema = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "command_id": "tablebrowser",
            "invocation_name": "tablebrowser",
            "display_name": "Table Browser",
            "category": "Tables",
            "summary": "browse arbitrary casacore tables",
            "usage": "tablebrowser <table-path>",
            "arguments": [
                {
                    "id": "table_path",
                    "label": "Table Path",
                    "order": 0,
                    "parser": {
                        "kind": "positional",
                        "metavar": "table-path"
                    },
                    "value_kind": "path",
                    "required": true,
                    "default": null,
                    "help": "Path to the casacore table root directory",
                    "group": "Input",
                    "advanced": false,
                    "hidden_in_tui": false
                }
            ],
            "managed_output": null
        }))
        .expect("tablebrowser schema");
        AppState::from_schema(tablebrowser_app(), schema)
    }

    fn sample_uv_coverage() -> ListObsUvCoverage {
        ListObsUvCoverage {
            schema_version: 1,
            options: ListObsOptions::default(),
            measurement_set_path: Some("/tmp/demo.ms".to_string()),
            axis_unit: "lambda".to_string(),
            mirrored_display: true,
            sample_count: 3,
            max_abs_uv_lambda: 1_200_000.0,
            tracks: vec![
                ListObsUvTrack {
                    antenna1: 0,
                    antenna2: 1,
                    field_id: 0,
                    spectral_window_id: 0,
                    center_frequency_hz: 1.4e9,
                    samples: vec![
                        ListObsUvPoint {
                            row: 0,
                            time_mjd_seconds: 0.0,
                            u_lambda: -600_000.0,
                            v_lambda: 300_000.0,
                            w_lambda: 0.0,
                        },
                        ListObsUvPoint {
                            row: 1,
                            time_mjd_seconds: 10.0,
                            u_lambda: 700_000.0,
                            v_lambda: -450_000.0,
                            w_lambda: 0.0,
                        },
                    ],
                },
                ListObsUvTrack {
                    antenna1: 2,
                    antenna2: 3,
                    field_id: 1,
                    spectral_window_id: 0,
                    center_frequency_hz: 1.4e9,
                    samples: vec![ListObsUvPoint {
                        row: 2,
                        time_mjd_seconds: 20.0,
                        u_lambda: 125_000.0,
                        v_lambda: 800_000.0,
                        w_lambda: 0.0,
                    }],
                },
            ],
        }
    }

    fn sample_listobs_summary() -> ListObsSummary {
        ListObsSummary {
            schema_version: 1,
            options: ListObsOptions::default(),
            measurement_set: MeasurementSetInfo {
                path: Some("/tmp/demo.ms".to_string()),
                ms_version: Some(2.0),
                row_count: 42,
                observation_count: 1,
                field_count: 1,
                spectral_window_count: 1,
                polarization_count: 1,
                data_description_count: 1,
                source_count: 1,
                antenna_count: 3,
                antenna_table_count: 3,
                time_reference: Some("UTC".to_string()),
                start_mjd_seconds: Some(0.0),
                end_mjd_seconds: Some(3600.0),
                total_elapsed_seconds: Some(3600.0),
            },
            observations: vec![ObservationSummary {
                observation_id: 0,
                telescope_name: "Very Large Array".to_string(),
                observer: "Long Observer".to_string(),
                project: "Project Alpha Beta".to_string(),
                release_date_mjd_seconds: 0.0,
                start_mjd_seconds: Some(0.0),
                end_mjd_seconds: Some(3600.0),
            }],
            scans: vec![ScanSummary {
                observation_id: 0,
                array_id: 1,
                scan_number: 7,
                row_count: 12,
                field_id: 0,
                field_name: "3C286".to_string(),
                field_ids: vec![0, 1],
                field_names: vec!["3C286".to_string(), "3C48".to_string()],
                data_description_ids: vec![0],
                spectral_window_ids: vec![0, 3],
                state_ids: vec![1, 2],
                scan_intents: vec!["CALIBRATE_BANDPASS".to_string()],
                start_mjd_seconds: 0.0,
                end_mjd_seconds: 120.0,
                mean_interval_seconds: 10.0,
                mean_interval_seconds_by_spw: vec![10.0, 20.25],
                unflagged_row_count: Some(11.5),
            }],
            fields: vec![FieldSummary {
                field_id: 0,
                name: "Long Source Name".to_string(),
                code: "CAL".to_string(),
                source_id: 9,
                row_count: 12,
                unflagged_row_count: Some(10.75),
                time_mjd_seconds: 0.0,
                direction_reference: Some("J2000".to_string()),
                phase_direction_radians: [1.0, 0.5],
            }],
            polarization_setups: vec![PolarizationSummary {
                polarization_id: 0,
                num_correlations: 2,
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            }],
            data_descriptions: vec![DataDescriptionSummary {
                data_description_id: 0,
                spectral_window_id: 0,
                polarization_id: 0,
                flagged: false,
            }],
            spectral_windows: vec![SpectralWindowSummary {
                spectral_window_id: 0,
                name: "none#LSRK#0".to_string(),
                num_channels: 64,
                frame: Some("LSRK".to_string()),
                first_channel_frequency_hz: 1.2e9,
                channel_width_hz: 1.25e5,
                reference_frequency_hz: 1.25e9,
                center_frequency_hz: 1.26e9,
                total_bandwidth_hz: 8.0e6,
                data_description_ids: vec![0],
                polarization_ids: vec![0],
                correlation_types: vec!["XX".to_string(), "YY".to_string(), "XY".to_string()],
            }],
            sources: vec![SourceSummary {
                source_id: 9,
                name: "3C286".to_string(),
                code: "C".to_string(),
                spectral_window_id: -1,
                calibration_group: 0,
                num_lines: 0,
                rest_frequency_hz: Some(1.420405751e9),
                system_velocity_m_s: Some(1234.0),
                time_mjd_seconds: 0.0,
                direction_radians: [1.0, 0.5],
            }],
            antennas: vec![
                AntennaSummary {
                    antenna_id: 0,
                    name: "ea01".to_string(),
                    station: "W01".to_string(),
                    antenna_type: "GROUND-BASED".to_string(),
                    mount: "ALT-AZ".to_string(),
                    dish_diameter_m: 25.0,
                    longitude_radians: 1.0,
                    latitude_radians: 0.5,
                    offset_from_observatory_m: [1.0, 2.0, 3.0],
                    position_m: [10.0, 20.0, 30.0],
                },
                AntennaSummary {
                    antenna_id: 1,
                    name: "ea02".to_string(),
                    station: "W02".to_string(),
                    antenna_type: "GROUND-BASED".to_string(),
                    mount: "ALT-AZ".to_string(),
                    dish_diameter_m: 25.0,
                    longitude_radians: 1.1,
                    latitude_radians: 0.55,
                    offset_from_observatory_m: [4.0, 5.0, 6.0],
                    position_m: [40.0, 50.0, 60.0],
                },
                AntennaSummary {
                    antenna_id: 2,
                    name: "ea03".to_string(),
                    station: "W03".to_string(),
                    antenna_type: "GROUND-BASED".to_string(),
                    mount: "ALT-AZ".to_string(),
                    dish_diameter_m: 25.0,
                    longitude_radians: 1.2,
                    latitude_radians: 0.6,
                    offset_from_observatory_m: [7.0, 8.0, 9.0],
                    position_m: [70.0, 80.0, 90.0],
                },
            ],
        }
    }

    fn exited_process() -> RunningProcess {
        spawn_process(&ExecutionPlan {
            command: ResolvedCommand::direct("sh"),
            arguments: vec!["-c".into(), "exit 0".into()],
            renderer: None,
            file_output_path: None,
        })
        .expect("spawn exited child")
    }

    fn running_state(renderer: Option<&str>, file_output_path: Option<&str>) -> RunningState {
        RunningState {
            process: exited_process(),
            renderer: renderer.map(str::to_string),
            file_output_path: file_output_path.map(str::to_string),
            uv_plan: None,
            cancel_requested: false,
        }
    }

    #[cfg(unix)]
    fn fake_browser_snapshot(status_line: &str, content_lines: Vec<String>) -> BrowserSnapshot {
        fake_browser_snapshot_with_metrics(status_line, content_lines, None, None)
    }

    #[cfg(unix)]
    fn fake_browser_snapshot_with_metrics(
        status_line: &str,
        content_lines: Vec<String>,
        vertical_metrics: Option<BrowserNavigationMetrics>,
        horizontal_metrics: Option<BrowserNavigationMetrics>,
    ) -> BrowserSnapshot {
        BrowserSnapshot {
            capabilities: BrowserCapabilities { editable: false },
            view: ProtocolBrowserView::Overview,
            focus: BrowserFocus::Main,
            table_path: "/tmp/fake.ms".to_string(),
            breadcrumb: vec![BrowserBreadcrumbEntry {
                label: "fake.ms".to_string(),
                path: "/tmp/fake.ms".to_string(),
            }],
            viewport: BrowserViewport::new(80, 24),
            status_line: status_line.to_string(),
            content_lines,
            vertical_metrics,
            horizontal_metrics,
            selected_address: None,
            inspector: None,
        }
    }

    #[cfg(unix)]
    fn write_browser_session_script(
        root: &Path,
        responses: &[String],
        stderr: Option<&str>,
    ) -> PathBuf {
        let path = root.join("app-browser.sh");
        let mut script = String::from("#!/bin/sh\ncount=0\nwhile IFS= read -r _line; do\n");
        if let Some(stderr) = stderr {
            script.push_str(&format!("  echo '{}' >&2\n", stderr.replace('\'', "'\\''")));
        }
        script.push_str("  count=$((count + 1))\n  case \"$count\" in\n");
        for (index, response) in responses.iter().enumerate() {
            let case_index = index + 1;
            script.push_str(&format!(
                "    {case_index}) printf '%s\\n' '{response}' ;;\n"
            ));
        }
        script.push_str("    *) exit 7 ;;\n  esac\ndone\n");
        fs::write(&path, script).expect("write browser script");
        let mut permissions = fs::metadata(&path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).expect("chmod");
        path
    }

    #[test]
    fn expands_bare_tilde_to_home() {
        assert_eq!(
            expand_tilde_path_with_home("~", Some(Path::new("/tmp/home"))),
            PathBuf::from("/tmp/home")
        );
    }

    #[test]
    fn expands_tilde_slash_to_home_relative_path() {
        assert_eq!(
            expand_tilde_path_with_home("~/data/file.ms", Some(Path::new("/tmp/home"))),
            PathBuf::from("/tmp/home/data/file.ms")
        );
    }

    #[test]
    fn leaves_non_tilde_paths_unchanged() {
        assert_eq!(
            expand_tilde_path_with_home("./relative/path", Some(Path::new("/tmp/home"))),
            PathBuf::from("./relative/path")
        );
    }

    #[test]
    fn uv_tab_summary_reports_prompt_loading_error_and_loaded_states() {
        let mut app = test_app();
        assert_eq!(
            app.uv_tab_summary(),
            "Open the UV tab to load wavelength-space UV coverage."
        );

        app.uv_loading = Some(UvCoverageLoadState {
            process: exited_process(),
            stdout: String::new(),
            stderr: String::new(),
            cancel_requested: false,
        });
        assert_eq!(app.uv_tab_summary(), "Loading UV coverage...");

        app.uv_loading = None;
        app.uv_coverage_error = Some("not available".to_string());
        assert_eq!(
            app.uv_tab_summary(),
            "UV coverage unavailable. not available"
        );

        app.uv_coverage = Some(sample_uv_coverage());
        let summary = app.uv_tab_summary();
        assert!(summary.contains("UV coverage in Mλ."));
        assert!(summary.contains("Tracks=2"));
    }

    #[test]
    fn finish_uv_loading_covers_cancel_failure_parse_and_success_paths() {
        let mut app = test_app();

        app.uv_loading = Some(UvCoverageLoadState {
            process: exited_process(),
            stdout: String::new(),
            stderr: String::new(),
            cancel_requested: true,
        });
        app.finish_uv_loading(true);
        assert_eq!(app.status_line_for_test(), "UV coverage load canceled.");
        assert_eq!(app.result.status_kind, StatusKind::Warning);

        app.uv_loading = Some(UvCoverageLoadState {
            process: exited_process(),
            stdout: String::new(),
            stderr: "uv stderr\n".to_string(),
            cancel_requested: false,
        });
        app.finish_uv_loading(false);
        assert_eq!(app.status_line_for_test(), "UV coverage load failed.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert!(app.stderr_for_test().contains("uv stderr"));

        app.uv_loading = Some(UvCoverageLoadState {
            process: exited_process(),
            stdout: "{".to_string(),
            stderr: String::new(),
            cancel_requested: false,
        });
        app.finish_uv_loading(true);
        assert_eq!(app.status_line_for_test(), "UV coverage parsing failed.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert_eq!(app.stderr_for_test(), "uv stderr\n{");

        let coverage = sample_uv_coverage();
        app.uv_loading = Some(UvCoverageLoadState {
            process: exited_process(),
            stdout: serde_json::to_string(&coverage).expect("serialize coverage"),
            stderr: String::new(),
            cancel_requested: false,
        });
        app.finish_uv_loading(true);
        assert_eq!(app.status_line_for_test(), "UV coverage loaded.");
        assert_eq!(app.result.status_kind, StatusKind::Ok);
        assert_eq!(app.uv_coverage_for_test(), Some(&coverage));
    }

    #[test]
    fn ensure_uv_plot_requested_generates_protocol_and_request_key() {
        let mut app = test_app();
        app.uv_coverage = Some(sample_uv_coverage());

        app.ensure_uv_plot_requested(Rect::new(0, 0, 20, 10));
        assert!(app.uv_plot_pending());
        for _ in 0..80 {
            app.pump_uv_plot_panel();
            if app.uv_plot_protocol().is_some() || app.uv_plot_last_error().is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        let panel = app.uv_plot_panel.as_ref().expect("plot panel");
        assert!(panel.request_key.is_some());
        assert!(
            app.uv_plot_protocol().is_some() || app.uv_plot_last_error().is_some(),
            "expected render output or a surfaced renderer error"
        );
        if app.uv_plot_protocol().is_some() {
            assert!(panel.image_size.is_some());
        } else {
            assert_eq!(app.status_line_for_test(), "UV plot rendering failed.");
            assert_eq!(app.result.status_kind, StatusKind::Warning);
        }

        let first_key = panel.request_key;
        app.ensure_uv_plot_requested(Rect::new(0, 0, 20, 10));
        assert_eq!(app.uv_plot_panel.as_ref().unwrap().request_key, first_key);

        app.result.status_line.clear();
        app.handle_key_event(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('t'),
            crossterm::event::KeyModifiers::NONE,
        ));
        app.ensure_uv_plot_requested(Rect::new(0, 0, 20, 10));
        assert_ne!(app.uv_plot_panel.as_ref().unwrap().request_key, first_key);
    }

    #[test]
    fn pump_uv_plot_panel_surfaces_renderer_errors() {
        let mut app = test_app();
        app.uv_coverage = Some(sample_uv_coverage());
        let picker = Picker::halfblocks();
        let font_size = picker.font_size();
        let renderer = PanelRenderer::new(picker, Resize::Fit(None), |_job| {
            Err::<DynamicImage, String>("boom".to_string())
        })
        .expect("panel renderer");
        app.uv_plot_panel = Some(UvPlotPanelState {
            renderer,
            font_size,
            request_key: None,
            last_error: None,
            image_size: None,
        });

        app.ensure_uv_plot_requested(Rect::new(0, 0, 18, 8));
        for _ in 0..80 {
            app.pump_uv_plot_panel();
            if app.uv_plot_last_error().is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(app.status_line_for_test(), "UV plot rendering failed.");
        assert_eq!(app.result.status_kind, StatusKind::Warning);
        assert!(
            app.uv_plot_last_error()
                .expect("plot error")
                .contains("boom")
        );
    }

    #[test]
    fn helper_formatters_cover_optional_lists_and_correlation_joining() {
        assert_eq!(format_optional_float(Some(1.23456)), "1.235");
        assert_eq!(format_optional_float(None), "n/a");
        assert_eq!(format_float_compact(12.3400, 3), "12.34");
        assert_eq!(format_float_compact(12.0, 3), "12");
        assert_eq!(format_i32_list(&[1, 2, 3]), "[1,2,3]");
        assert_eq!(format_float_list(&[1.25, 2.0, 3.5001], 3), "[1.25,2,3.5]");
        assert_eq!(join_corrs(&["XX".into(), "YY".into(), "XY".into()]), "XX  YY  XY");
    }

    #[test]
    fn browser_cell_helpers_preserve_selection_and_header_roles() {
        assert_eq!(
            strip_browser_selection_markers("  >value<  ").as_deref(),
            Some("   value   ")
        );
        assert!(strip_browser_selection_markers("value").is_none());

        let header = browser_cells_visible_line("row |colA|colB");
        assert_eq!(header.text, "row │colA│colB");
        assert!(header.roles.contains(&VisibleTextRole::TableHeader));
        assert!(header.roles.contains(&VisibleTextRole::BrowserSeparator));

        let selected = browser_cells_visible_line("  >42< | plain ");
        assert_eq!(selected.text, "   42  │ plain ");
        assert!(selected.roles.contains(&VisibleTextRole::BrowserSelectedCell));
    }

    #[test]
    fn visible_text_helpers_slice_and_trim_consistently() {
        assert_eq!(fit_visible_text("abcdef", 0), "");
        assert_eq!(fit_visible_text("abcdef", 2), "..");
        assert_eq!(fit_visible_text("abcdef", 5), "ab...");
        assert_eq!(slice_visible_text("abcdef", 2, 3), "...");
        assert_eq!(slice_visible_text("abcdef", 99, 3), "");
        assert_eq!(slice_chars("abcdef", 1, 4), "bcd");
    }

    #[test]
    fn selection_helpers_normalize_clamp_and_extract_text() {
        let buffer = VisibleTextBuffer {
            area: Rect::new(10, 5, 8, 3),
            lines: vec![
                VisibleTextLine::plain("alpha  ".to_string()),
                VisibleTextLine::plain("beta".to_string()),
                VisibleTextLine::plain(String::new()),
            ],
        };
        let selection = OutputSelection {
            target: OutputPane::Result,
            anchor: BufferPoint { row: 1, col: 3 },
            cursor: BufferPoint { row: 0, col: 1 },
            mode: OutputSelectionMode::Dragging,
        };
        assert_eq!(normalize_selection(selection), (0, 1, 1, 3));
        assert_eq!(
            clamp_point_to_buffer(&buffer, 50, 50),
            BufferPoint { row: 2, col: 0 }
        );
        assert_eq!(
            clamp_point_to_buffer(&buffer, 12, 5),
            BufferPoint { row: 0, col: 2 }
        );
        assert_eq!(extract_selected_text(&buffer, selection), "lph\neta");
    }

    #[test]
    fn browser_copy_helpers_render_scalars_arrays_and_fallback_views() {
        let undefined = BrowserInspectorSnapshot {
            title: "Undefined".into(),
            trail: Vec::new(),
            node: BrowserValueNode::Undefined,
            rendered_lines: vec!["<undefined>".into()],
        };
        assert_eq!(
            copyable_browser_text(&undefined),
            ("<undefined>".to_string(), "undefined value")
        );

        let scalar = BrowserInspectorSnapshot {
            title: "Scalar".into(),
            trail: Vec::new(),
            node: BrowserValueNode::Scalar {
                value: BrowserScalarValue::Complex64(BrowserComplex64Value { re: 1.5, im: -2.0 }),
            },
            rendered_lines: vec!["1.5-2i".into()],
        };
        assert_eq!(copyable_browser_text(&scalar), ("1.5-2i".to_string(), "value"));

        let table_ref = BrowserInspectorSnapshot {
            title: "Table".into(),
            trail: Vec::new(),
            node: BrowserValueNode::TableRef {
                path: "../SUB".into(),
                resolved_path: "/tmp/root/SUB".into(),
                openable: true,
            },
            rendered_lines: vec!["/tmp/root/SUB".into()],
        };
        assert_eq!(
            copyable_browser_text(&table_ref),
            ("/tmp/root/SUB".to_string(), "table path")
        );

        let full_array = BrowserInspectorSnapshot {
            title: "Array".into(),
            trail: Vec::new(),
            node: BrowserValueNode::Array {
                primitive: casacore_tablebrowser_protocol::BrowserPrimitiveType::Float64,
                shape: vec![2, 1],
                total_elements: 2,
                page_start: 0,
                page_size: 2,
                elements: vec![
                    BrowserArrayElement {
                        flat_index: 0,
                        index: vec![0, 0],
                        value: BrowserScalarValue::Float64(1.25),
                        selected: false,
                    },
                    BrowserArrayElement {
                        flat_index: 1,
                        index: vec![1, 0],
                        value: BrowserScalarValue::Float64(2.5),
                        selected: true,
                    },
                ],
            },
            rendered_lines: vec!["[1.25, 2.5]".into()],
        };
        assert_eq!(
            copyable_browser_text(&full_array),
            ("[1.25, 2.5]".to_string(), "array value")
        );

        let partial_array = BrowserInspectorSnapshot {
            title: "Paged".into(),
            trail: Vec::new(),
            node: BrowserValueNode::Array {
                primitive: casacore_tablebrowser_protocol::BrowserPrimitiveType::Int32,
                shape: vec![4],
                total_elements: 4,
                page_start: 0,
                page_size: 2,
                elements: vec![BrowserArrayElement {
                    flat_index: 0,
                    index: vec![0],
                    value: BrowserScalarValue::Int32(7),
                    selected: true,
                }],
            },
            rendered_lines: vec!["[7,".into(), " ...]".into()],
        };
        assert_eq!(
            copyable_browser_text(&partial_array),
            ("[7,\n ...]".to_string(), "inspector view")
        );
    }

    #[test]
    fn browser_scalar_rendering_formats_numbers_strings_and_complex_values() {
        assert_eq!(render_browser_scalar(&BrowserScalarValue::Bool(true)), "true");
        assert_eq!(render_browser_scalar(&BrowserScalarValue::Float32(1.25)), "1.25");
        assert_eq!(
            render_browser_scalar(&BrowserScalarValue::String("abc".into())),
            "\"abc\""
        );
        assert_eq!(
            render_browser_scalar(&BrowserScalarValue::Complex32(BrowserComplex32Value {
                re: 3.0,
                im: 4.5,
            })),
            "3.0+4.5i"
        );
    }

    #[test]
    fn browser_tab_labels_reflect_backend_view_mapping() {
        assert_eq!(BrowserTab::from_view(casacore_tablebrowser_protocol::BrowserView::Cells), BrowserTab::Cells);
        assert_eq!(BrowserTab::Cells.label(), "Cells");
    }

    #[test]
    fn build_execution_plan_applies_selection_and_output_rules() {
        let mut app = test_app();
        app.set_text_value("ms_path", "/tmp/input.ms");

        let base = app.build_execution_plan().expect("build execution plan");
        assert!(!base.arguments.is_empty());
        assert_eq!(base.renderer.as_deref(), Some("listobs-summary-v1"));

        app.set_text_value("field", "3C286");
        let selected = app.build_execution_plan().expect("selection plan");
        assert!(selected.arguments.iter().any(|arg| arg == "--selectdata"));

        app.set_text_value("output", "/tmp/out.txt");
        app.set_text_value("listfile", "/tmp/out.log");
        let error = app.build_execution_plan().expect_err("conflicting outputs should fail");
        assert!(error.contains("Choose either --output or --listfile"));
    }

    #[test]
    fn finish_execution_covers_cancel_file_output_parse_failure_and_plain_modes() {
        let mut app = test_app();

        app.running = Some(RunningState {
            cancel_requested: true,
            ..running_state(Some("listobs-summary-v1"), None)
        });
        app.result.stderr = "cancel stderr".into();
        app.finish_execution(Some(130), false);
        assert_eq!(app.status_line_for_test(), "Execution canceled.");
        assert_eq!(app.result.status_kind, StatusKind::Warning);
        assert_eq!(app.active_result_tab(), ResultTab::Stderr);
        assert!(app.result.structured_error.is_some());

        app.running = Some(running_state(Some("listobs-summary-v1"), Some("/tmp/out.txt")));
        app.result.stdout.clear();
        app.result.stderr.clear();
        app.finish_execution(Some(0), true);
        assert_eq!(app.status_line_for_test(), "Execution completed successfully.");
        assert_eq!(app.result.status_kind, StatusKind::Ok);
        assert_eq!(app.file_output_path_for_test(), Some("/tmp/out.txt"));
        assert_eq!(app.active_result_tab(), ResultTab::Overview);

        app.running = Some(running_state(Some("listobs-summary-v1"), None));
        app.result.stdout = "{".into();
        app.result.stderr.clear();
        app.finish_execution(Some(0), true);
        assert_eq!(
            app.status_line_for_test(),
            "Execution completed, but structured rendering failed."
        );
        assert_eq!(app.result.status_kind, StatusKind::Warning);
        assert!(app.result.structured_error.is_some());
        assert_eq!(app.active_result_tab(), ResultTab::Stdout);

        app.running = Some(running_state(None, None));
        app.result.stdout = "plain stdout".into();
        app.result.stderr.clear();
        app.finish_execution(Some(0), true);
        assert_eq!(app.result.status_kind, StatusKind::Ok);
        assert_eq!(app.active_result_tab(), ResultTab::Stdout);

        app.running = Some(running_state(None, None));
        app.result.stdout.clear();
        app.result.stderr = "plain stderr".into();
        app.finish_execution(Some(1), false);
        assert_eq!(app.status_line_for_test(), "Execution failed.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert_eq!(app.active_result_tab(), ResultTab::Stderr);
    }

    #[cfg(unix)]
    #[test]
    fn browser_session_success_and_failure_paths_update_state() {
        let temp = tempdir().expect("tempdir");
        let success_response = serde_json::to_string(&BrowserResponseEnvelope::snapshot(
            fake_browser_snapshot("Browser moved", vec!["new content".into()]),
        ))
        .expect("serialize snapshot");
        let script = write_browser_session_script(temp.path(), &[success_response], None);
        let client = BrowserClient::spawn(&ResolvedCommand::direct(script)).expect("spawn client");

        let mut app = test_app();
        app.browser_session = Some(BrowserSession {
            client,
            snapshot: fake_browser_snapshot("Old", vec!["old".into()]),
            viewport: BrowserViewport::new(80, 24),
        });
        app.output_selection = Some(OutputSelection {
            target: OutputPane::Result,
            anchor: BufferPoint { row: 0, col: 0 },
            cursor: BufferPoint { row: 0, col: 1 },
            mode: OutputSelectionMode::Dragging,
        });
        app.send_browser_command(BrowserCommand::GetSnapshot { viewport: None });
        assert!(app.browser_is_active());
        assert_eq!(app.status_line_for_test(), "Browser moved");
        assert_eq!(app.result.status_kind, StatusKind::Info);
        assert!(app.output_selection.is_none());

        let failure_script = write_browser_session_script(temp.path(), &[], Some("session stderr"));
        let failure_client =
            BrowserClient::spawn(&ResolvedCommand::direct(failure_script)).expect("spawn client");
        app.browser_session = Some(BrowserSession {
            client: failure_client,
            snapshot: fake_browser_snapshot("Old", vec!["old".into()]),
            viewport: BrowserViewport::new(80, 24),
        });
        app.send_browser_command(BrowserCommand::GetSnapshot { viewport: None });
        assert!(!app.browser_is_active());
        assert_eq!(
            app.status_line_for_test(),
            "Browser command failed. Session closed."
        );
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert!(app.stderr_for_test().contains("session stderr"));
        assert_eq!(app.active_result_tab(), ResultTab::Stderr);
        assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
    }

    #[cfg(unix)]
    #[test]
    fn cancel_current_closes_browser_session_and_restores_parameter_focus() {
        let temp = tempdir().expect("tempdir");
        let response = serde_json::to_string(&BrowserResponseEnvelope::snapshot(
            fake_browser_snapshot("Opened", vec!["content".into()]),
        ))
        .expect("serialize snapshot");
        let script = write_browser_session_script(temp.path(), &[response], None);
        let client = BrowserClient::spawn(&ResolvedCommand::direct(script)).expect("spawn client");

        let mut app = test_app();
        app.pane_focus = PaneFocus::Result;
        app.browser_session = Some(BrowserSession {
            client,
            snapshot: fake_browser_snapshot("Opened", vec!["content".into()]),
            viewport: BrowserViewport::new(80, 24),
        });

        app.cancel_current();
        assert!(!app.browser_is_active());
        assert_eq!(app.status_line_for_test(), "Browser session closed.");
        assert_eq!(app.result.status_kind, StatusKind::Info);
        assert_eq!(app.pane_focus_for_test(), PaneFocus::Parameters);
    }

    #[cfg(unix)]
    #[test]
    fn overview_lines_cover_file_running_error_and_browser_states() {
        let temp = tempdir().expect("tempdir");
        let mut app = test_app();
        app.result.file_output_path = Some("/tmp/output.txt".into());
        let file_lines = app.overview_lines();
        assert!(file_lines.iter().any(|line| line.contains("/tmp/output.txt")));

        app.result.file_output_path = None;
        app.running = Some(running_state(None, None));
        let running_lines = app.overview_lines();
        assert!(running_lines.iter().any(|line| line.contains("still running")));

        app.running = None;
        app.result.structured_error = Some("parse failed".into());
        let error_lines = app.overview_lines();
        assert!(error_lines.iter().any(|line| line.contains("parse failed")));

        app.result.structured_error = None;
        let response = serde_json::to_string(&BrowserResponseEnvelope::snapshot(
            fake_browser_snapshot("Browser overview", vec!["browser line".into()]),
        ))
        .expect("serialize snapshot");
        let script = write_browser_session_script(temp.path(), &[response], None);
        let client = BrowserClient::spawn(&ResolvedCommand::direct(script)).expect("spawn client");
        app.browser_session = Some(BrowserSession {
            client,
            snapshot: fake_browser_snapshot("Browser overview", vec!["browser line".into()]),
            viewport: BrowserViewport::new(80, 24),
        });
        let browser_lines = app.overview_lines();
        assert_eq!(browser_lines, vec!["browser line".to_string()]);
    }

    #[test]
    fn structured_renderers_cover_all_summary_tabs() {
        let summary = sample_listobs_summary();

        let observations = build_observations_table(&summary);
        assert!(observations.header.contains("Telescope"));
        assert!(observations.rows[0].contains("Very La"));
        assert!(observations.rows[0].contains("Long Ob"));

        let scans = build_scans_table(&summary, true);
        assert!(scans.header.contains("nUnfl"));
        assert!(scans.rows[0].contains("CALIBRATE_BANDPASS"));
        assert!(scans.rows[0].contains("[0,3]"));

        let fields = build_fields_table(&summary, true);
        assert!(fields.header.contains("nUnflRows"));
        assert!(fields.rows[0].contains("J2000"));

        let spws = build_spws_table(&summary);
        assert!(spws.header.contains("CtrFreq(MHz)"));
        assert!(spws.rows[0].contains("LSRK"));
        assert!(spws.rows[0].contains("XX  YY  XY"));

        let sources = build_sources_table(&summary);
        assert!(sources.rows[0].contains("any"));
        assert!(sources.rows[0].contains("1.234"));

        let antennas = build_antennas_table(&summary);
        assert!(antennas.header.contains("ITRF x"));
        assert!(antennas.rows[0].contains("ea01"));

        let compact = build_compact_antenna_lines(&summary);
        assert!(compact[0].contains("Antennas: 3"));
        assert!(compact.iter().any(|line| line.to_string().contains("'ea01'='W01'")));
    }

    #[test]
    fn start_run_reports_schema_build_and_launch_failures() {
        let mut app = test_app();
        app.schema = None;
        app.start_run();
        assert_eq!(app.status_line_for_test(), "Cannot run without a loaded UI schema.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert_eq!(app.active_result_tab(), ResultTab::Stderr);

        let mut app = test_app();
        app.set_text_value("ms_path", "/tmp/demo.ms");
        app.set_text_value("output", "/tmp/out.txt");
        app.set_text_value("listfile", "/tmp/out.log");
        app.start_run();
        assert_eq!(app.status_line_for_test(), "Cannot start command.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert!(app.stderr_for_test().contains("Choose either --output or --listfile"));

        let _guard = ENV_LOCK.lock().expect("env lock");
        let mut app = test_app();
        app.set_text_value("ms_path", "/tmp/demo.ms");
        unsafe {
            std::env::set_var("CASARS_LISTOBS_BIN", "/definitely/missing-listobs");
        }
        app.start_run();
        unsafe {
            std::env::remove_var("CASARS_LISTOBS_BIN");
        }
        assert_eq!(app.status_line_for_test(), "Failed to launch listobs.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert!(app.stderr_for_test().contains("spawn subprocess"));
        assert_eq!(app.active_result_tab(), ResultTab::Stderr);
    }

    #[test]
    fn table_browser_start_requires_path_and_reports_launch_failure() {
        let mut app = tablebrowser_test_app();
        app.start_run();
        assert_eq!(app.status_line_for_test(), "Table Path is required.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert!(!app.browser_is_active());

        let _guard = ENV_LOCK.lock().expect("env lock");
        let mut app = tablebrowser_test_app();
        app.set_text_value("table_path", "/tmp/demo.table");
        unsafe {
            std::env::set_var("CASARS_TABLEBROWSER_BIN", "/definitely/missing-tablebrowser");
        }
        app.start_run();
        unsafe {
            std::env::remove_var("CASARS_TABLEBROWSER_BIN");
        }
        assert_eq!(app.status_line_for_test(), "Failed to launch table browser.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert!(app.stderr_for_test().contains("spawn tablebrowser session"));
        assert_eq!(app.active_result_tab(), ResultTab::Stderr);
        assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
    }

    #[test]
    fn ensure_uv_coverage_started_covers_missing_plan_launch_failure_and_success() {
        let mut app = test_app();
        app.result.structured = Some(sample_listobs_summary());

        app.ensure_uv_coverage_started();
        assert_eq!(
            app.uv_coverage_error.as_deref(),
            Some("UV coverage is only available for the most recent completed listobs run.")
        );

        app.last_listobs_uv_plan = Some(ExecutionPlan {
            command: ResolvedCommand::direct("/definitely/missing-uv-loader"),
            arguments: Vec::new(),
            renderer: None,
            file_output_path: None,
        });
        app.ensure_uv_coverage_started();
        assert_eq!(app.status_line_for_test(), "Failed to launch UV coverage loader.");
        assert_eq!(app.result.status_kind, StatusKind::Error);
        assert!(app.stderr_for_test().contains("spawn subprocess"));

        let coverage = sample_uv_coverage();
        let json = serde_json::to_string(&coverage).expect("serialize coverage");
        app.result.stderr.clear();
        app.last_listobs_uv_plan = Some(ExecutionPlan {
            command: ResolvedCommand::direct("sh"),
            arguments: vec![
                "-c".into(),
                "printf '%s' \"$1\"".into(),
                "uv-loader".into(),
                json.into(),
            ],
            renderer: None,
            file_output_path: None,
        });
        app.ensure_uv_coverage_started();
        assert!(app.uv_loading_active_for_test());
        assert!(app.wait_for_all_idle_for_test(Duration::from_secs(2)));
        assert_eq!(app.status_line_for_test(), "UV coverage loaded.");
        assert_eq!(app.result.status_kind, StatusKind::Ok);
        assert_eq!(app.uv_coverage_for_test(), Some(&coverage));
    }

    #[test]
    fn cancel_current_covers_uv_and_running_cancel_paths() {
        let mut app = test_app();
        app.uv_loading = Some(UvCoverageLoadState {
            process: spawn_process(&ExecutionPlan {
                command: ResolvedCommand::direct("sh"),
                arguments: vec!["-c".into(), "sleep 5".into()],
                renderer: None,
                file_output_path: None,
            })
            .expect("spawn long-running uv loader"),
            stdout: String::new(),
            stderr: String::new(),
            cancel_requested: false,
        });
        app.cancel_current();
        assert_eq!(
            app.status_line_for_test(),
            "Cancel requested for UV coverage load..."
        );
        assert_eq!(app.result.status_kind, StatusKind::Warning);
        assert!(app.uv_loading.as_ref().is_some_and(|loading| loading.cancel_requested));

        let mut app = test_app();
        app.uv_loading = Some(UvCoverageLoadState {
            process: exited_process(),
            stdout: String::new(),
            stderr: String::new(),
            cancel_requested: false,
        });
        app.cancel_current();
        assert_eq!(
            app.status_line_for_test(),
            "Cancel requested for UV coverage load..."
        );
        assert!(app.uv_loading.as_ref().is_some_and(|loading| loading.cancel_requested));
        app.cancel_current();
        assert_eq!(
            app.status_line_for_test(),
            "Cancel requested for UV coverage load..."
        );

        let mut app = test_app();
        app.running = Some(running_state(None, None));
        app.cancel_current();
        assert_eq!(app.status_line_for_test(), "Cancel requested for listobs...");
        assert!(app.running.as_ref().is_some_and(|running| running.cancel_requested));
        app.cancel_current();
        assert_eq!(app.status_line_for_test(), "Cancel requested for listobs...");
    }

    #[test]
    fn parameter_and_result_actions_cover_dispatch_and_focus_helpers() {
        let mut app = test_app();
        let toggle_index = app
            .fields
            .iter()
            .position(|field| matches!(field.value, FormValue::Toggle(_)))
            .expect("toggle field");

        app.selected_form = FormSelection::Field(toggle_index);
        app.apply_parameter_action(ParameterAction::ChoiceNext);
        app.apply_parameter_action(ParameterAction::ChoicePrevious);

        app.apply_parameter_action(ParameterAction::SelectNext);
        assert_ne!(app.selected_form, FormSelection::Field(toggle_index));
        app.apply_parameter_action(ParameterAction::SelectPrevious);
        assert_eq!(app.selected_form, FormSelection::Field(toggle_index));

        let toggle_before = match app.fields[toggle_index].value {
            FormValue::Toggle(value) => value,
            _ => unreachable!("toggle field"),
        };
        app.selected_form = FormSelection::Field(toggle_index);
        app.apply_parameter_action(ParameterAction::Activate);
        let toggle_after = match app.fields[toggle_index].value {
            FormValue::Toggle(value) => value,
            _ => unreachable!("toggle field"),
        };
        assert_ne!(toggle_before, toggle_after);

        let collapsed_before = app.sections[0].collapsed;
        app.selected_form = FormSelection::Section(0);
        app.apply_parameter_action(ParameterAction::Activate);
        assert_ne!(collapsed_before, app.sections[0].collapsed);

        app.active_result_tab = ResultTab::Stdout;
        app.apply_result_action(ResultAction::Scroll(3));
        app.apply_result_action(ResultAction::ScrollHorizontal(4));
        assert_eq!(app.result_scrolls[ResultTab::Stdout.index()], 3);
        assert_eq!(app.result_hscrolls[ResultTab::Stdout.index()], 4);

        app.active_result_tab = ResultTab::Overview;
        app.apply_result_action(ResultAction::NextTab);
        assert_eq!(app.active_result_tab(), ResultTab::Observations);
        app.apply_result_action(ResultAction::PreviousTab);
        assert_eq!(app.active_result_tab(), ResultTab::Overview);

        app.toggle_advanced();
        assert!(app.show_advanced);
        app.toggle_parameters_pane();
        assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
        assert!(app.parameters_pane_collapsed());
        app.toggle_parameters_pane();
        assert!(!app.parameters_pane_collapsed());
        app.toggle_focus();
        assert_eq!(app.pane_focus_for_test(), PaneFocus::Parameters);
        app.toggle_focus();
        assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
    }

    #[test]
    fn input_modes_and_path_chooser_key_dispatch_cover_interactive_branches() {
        let mut app = test_app();
        assert_eq!(app.input_mode(), super::InputMode::Parameters);

        app.pane_focus = PaneFocus::Result;
        assert_eq!(app.input_mode(), super::InputMode::Result);

        app.edit_state = Some(super::EditState {
            field_index: 0,
            buffer: "draft".to_string(),
        });
        assert_eq!(app.input_mode(), super::InputMode::Edit);

        let temp = tempdir().expect("tempdir");
        let chooser_dir = temp.path().join("chooser-mode");
        std::fs::create_dir(&chooser_dir).expect("create chooser dir");
        std::fs::write(chooser_dir.join("picked.ms"), "").expect("create file");

        let field_index = app
            .fields
            .iter()
            .position(|field| field.schema.id == "ms_path")
            .expect("ms_path field");
        app.edit_state = Some(super::EditState {
            field_index,
            buffer: chooser_dir.to_string_lossy().to_string(),
        });
        app.selected_form = FormSelection::Field(field_index);
        app.open_path_chooser_for_selected_field();
        assert_eq!(app.input_mode(), super::InputMode::PathChooser);
        assert!(app
            .resolve_key_action(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Enter,
                crossterm::event::KeyModifiers::NONE,
            ))
            .is_some_and(|action| matches!(
                action,
                super::AppAction::PathChooser(super::PathChooserAction::Confirm)
            )));
        assert!(app
            .resolve_key_action(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char(' '),
                crossterm::event::KeyModifiers::NONE,
            ))
            .is_some_and(|action| matches!(
                action,
                super::AppAction::PathChooser(super::PathChooserAction::SelectCurrent)
            )));
        assert!(app
            .resolve_key_action(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Down,
                crossterm::event::KeyModifiers::NONE,
            ))
            .is_some_and(|action| matches!(
                action,
                super::AppAction::PathChooser(super::PathChooserAction::Navigate(
                    super::ExplorerInput::Down
                ))
            )));
        assert_eq!(
            app.resolve_key_action(crossterm::event::KeyEvent {
                code: crossterm::event::KeyCode::Esc,
                modifiers: crossterm::event::KeyModifiers::NONE,
                kind: crossterm::event::KeyEventKind::Release,
                state: crossterm::event::KeyEventState::NONE,
            }),
            None
        );

        let spinner_before = app.spinner_frame;
        app.on_tick();
        assert_eq!(
            app.spinner_frame,
            (spinner_before + 1) % super::spinner_frames(app.theme_mode()).len()
        );

        app.handle_key_event(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Down,
            crossterm::event::KeyModifiers::NONE,
        ));
        app.handle_key_event(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char(' '),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert!(app.path_chooser.is_none());
        assert!(app.status_line_for_test().starts_with("Selected path:"));
        assert_eq!(
            app.input_mode(),
            if app.pane_focus == PaneFocus::Parameters {
                super::InputMode::Parameters
            } else {
                super::InputMode::Result
            }
        );
    }

    #[cfg(unix)]
    #[test]
    fn browser_actions_and_path_chooser_cover_dispatch_heavy_branches() {
        let temp = tempdir().expect("tempdir");
        let responses = [
            ("cycle", ProtocolBrowserView::Columns),
            ("left", ProtocolBrowserView::Columns),
            ("right", ProtocolBrowserView::Columns),
            ("up", ProtocolBrowserView::Columns),
            ("down", ProtocolBrowserView::Columns),
            ("page-up", ProtocolBrowserView::Columns),
            ("page-down", ProtocolBrowserView::Columns),
            ("activate", ProtocolBrowserView::Columns),
            ("back", ProtocolBrowserView::Columns),
            ("escape", ProtocolBrowserView::Columns),
            ("scroll-down", ProtocolBrowserView::Columns),
            ("scroll-up", ProtocolBrowserView::Columns),
            ("hscroll-right", ProtocolBrowserView::Columns),
            ("hscroll-left", ProtocolBrowserView::Columns),
            ("keywords", ProtocolBrowserView::Keywords),
        ]
        .into_iter()
        .map(|(status, view)| {
            serde_json::to_string(&BrowserResponseEnvelope::snapshot(BrowserSnapshot {
                capabilities: BrowserCapabilities { editable: false },
                view,
                focus: BrowserFocus::Main,
                table_path: "/tmp/fake.ms".to_string(),
                breadcrumb: vec![BrowserBreadcrumbEntry {
                    label: "fake.ms".to_string(),
                    path: "/tmp/fake.ms".to_string(),
                }],
                viewport: BrowserViewport::new(80, 24),
                status_line: status.to_string(),
                content_lines: vec![status.to_string()],
                vertical_metrics: Some(BrowserNavigationMetrics {
                    selected_index: 2,
                    total_items: 20,
                    viewport_items: 5,
                }),
                horizontal_metrics: Some(BrowserNavigationMetrics {
                    selected_index: 1,
                    total_items: 10,
                    viewport_items: 4,
                }),
                selected_address: None,
                inspector: None,
            }))
            .expect("serialize snapshot")
        })
        .collect::<Vec<_>>();
        let script = write_browser_session_script(temp.path(), &responses, None);
        let client = BrowserClient::spawn(&ResolvedCommand::direct(script)).expect("spawn client");

        let mut app = test_app();
        app.browser_session = Some(BrowserSession {
            client,
            snapshot: fake_browser_snapshot_with_metrics(
                "cells",
                vec!["cells".into()],
                Some(BrowserNavigationMetrics {
                    selected_index: 2,
                    total_items: 20,
                    viewport_items: 5,
                }),
                Some(BrowserNavigationMetrics {
                    selected_index: 1,
                    total_items: 10,
                    viewport_items: 4,
                }),
            ),
            viewport: BrowserViewport::new(80, 24),
        });
        app.browser_session.as_mut().unwrap().snapshot.view = ProtocolBrowserView::Cells;

        assert_eq!(app.active_browser_scroll(), 2);
        assert_eq!(app.active_browser_hscroll(), 1);

        app.apply_browser_action(BrowserAction::CycleView { forward: true });
        app.apply_browser_action(BrowserAction::MoveLeft);
        app.apply_browser_action(BrowserAction::MoveRight);
        app.apply_browser_action(BrowserAction::MoveUp);
        app.apply_browser_action(BrowserAction::MoveDown);
        app.apply_browser_action(BrowserAction::PageUp);
        app.apply_browser_action(BrowserAction::PageDown);
        app.apply_browser_action(BrowserAction::Activate);
        app.apply_browser_action(BrowserAction::Back);
        app.apply_browser_action(BrowserAction::Escape);
        assert_eq!(app.status_line_for_test(), "escape");

        app.set_active_browser_scroll(5);
        assert_eq!(app.status_line_for_test(), "scroll-down");
        app.set_active_browser_scroll(0);
        assert_eq!(app.status_line_for_test(), "scroll-up");
        app.set_active_browser_hscroll(5);
        assert_eq!(app.status_line_for_test(), "hscroll-right");
        app.set_active_browser_hscroll(0);
        assert_eq!(app.status_line_for_test(), "hscroll-left");
        app.activate_browser_tab(BrowserTab::Keywords);
        assert_eq!(app.status_line_for_test(), "keywords");

        let chooser_dir = temp.path().join("chooser");
        std::fs::create_dir(&chooser_dir).expect("create chooser dir");
        std::fs::write(chooser_dir.join("picked.ms"), "").expect("create file");
        app.set_text_value("ms_path", chooser_dir.to_string_lossy().as_ref());
        let field_index = app
            .fields
            .iter()
            .position(|field| field.schema.id == "ms_path")
            .expect("ms_path field");
        app.edit_state = Some(super::EditState {
            field_index,
            buffer: chooser_dir.to_string_lossy().to_string(),
        });
        app.selected_form = FormSelection::Field(field_index);
        app.open_path_chooser_for_selected_field();
        assert!(app.path_chooser.is_some());
        assert!(app.edit_state.is_none());

        let layout = crate::ui::compute_layout(Rect::new(0, 0, 120, 30), &app);
        let chooser_area = crate::ui::path_chooser_area(layout.body);
        let list_area = crate::ui::path_chooser_list_area(chooser_area);
        app.handle_path_chooser_mouse(
            crossterm::event::MouseEvent {
                kind: crossterm::event::MouseEventKind::ScrollDown,
                column: list_area.x,
                row: list_area.y,
                modifiers: crossterm::event::KeyModifiers::NONE,
            },
            &layout,
        );
        app.handle_path_chooser_mouse(
            crossterm::event::MouseEvent {
                kind: crossterm::event::MouseEventKind::ScrollUp,
                column: list_area.x,
                row: list_area.y,
                modifiers: crossterm::event::KeyModifiers::NONE,
            },
            &layout,
        );
        app.apply_path_chooser_action(super::PathChooserAction::SelectCurrent);
        assert!(app.status_line_for_test().starts_with("Selected path:"));

        app.open_path_chooser(field_index);
        app.handle_path_chooser_mouse(
            crossterm::event::MouseEvent {
                kind: crossterm::event::MouseEventKind::Down(crossterm::event::MouseButton::Left),
                column: 0,
                row: 0,
                modifiers: crossterm::event::KeyModifiers::NONE,
            },
            &layout,
        );
        assert_eq!(app.status_line_for_test(), "Path chooser canceled.");
        assert!(app.path_chooser.is_none());
    }

    #[test]
    fn copy_output_selection_covers_warning_and_uv_summary_clipboard_paths() {
        let mut app = test_app();
        app.copy_output_selection();
        assert_eq!(app.status_line_for_test(), "Nothing copyable is selected.");
        assert_eq!(app.result.status_kind, StatusKind::Warning);

        let _guard = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::tempdir().expect("tempdir");
        let clipboard_path = temp.path().join("clipboard.txt");
        unsafe {
            std::env::set_var("CASARS_TEST_CLIPBOARD_FILE", &clipboard_path);
        }

        let mut app = test_app();
        app.active_result_tab = ResultTab::Uv;
        app.uv_coverage = Some(sample_uv_coverage());
        app.copy_output_selection();

        unsafe {
            std::env::remove_var("CASARS_TEST_CLIPBOARD_FILE");
        }

        assert_eq!(
            app.status_line_for_test(),
            "Copied uv coverage summary to clipboard."
        );
        assert_eq!(app.result.status_kind, StatusKind::Ok);
        let copied = std::fs::read_to_string(&clipboard_path).expect("read clipboard override");
        assert!(copied.contains("Tracks=2"));
        assert!(copied.contains("UV coverage in M"));
    }
}
