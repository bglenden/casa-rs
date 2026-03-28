// SPDX-License-Identifier: LGPL-3.0-or-later
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use casacore_imagebrowser_protocol::{
    ImageBrowserCommand, ImageBrowserFocus, ImageBrowserSnapshot, ImageBrowserView,
    ImageBrowserViewport,
};
use casacore_ms::listobs::cli::{
    UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiManagedOutputSchema, UiValueKind,
};
use casacore_ms::{
    ListObsOptions, ListObsPlotExportFormat, ListObsPlotKind, ListObsPlotPayload, ListObsPlotSpec,
    ListObsSummary, ListObsUvCoverage, build_listobs_plot_payload_from_summary,
    build_listobs_uv_plot_payload, export_listobs_plot,
};
use casacore_tablebrowser_protocol::{
    BrowserCommand, BrowserComplex32Value, BrowserComplex64Value, BrowserFocus,
    BrowserInspectorSnapshot, BrowserScalarValue, BrowserSnapshot, BrowserValueNode,
    BrowserView as TableBrowserView, BrowserViewport,
};
use casacore_types::measures::direction::{
    format_declination_labeled, format_right_ascension_labeled,
};
use casacore_types::quanta::{MvAngle, MvTime};
use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::Rect;
use ratatui_explorer::{FileExplorer, FileExplorerBuilder, Input as ExplorerInput};
use ratatui_graphics::{PanelProtocol, PanelRenderer, Picker, Resize};

use crate::browser_client::{BrowserClient, ImageBrowserClient};
use crate::clipboard;
use crate::config::{ConfigStore, ThemeMode};
use crate::execution::{ExecutionEvent, ExecutionPlan, RunningProcess, spawn_process};
use crate::graphics::{ListObsPlotRenderInput, plot_theme, render_plot_image};
use crate::registry::{BrowserAppKind, RegistryApp};
use crate::ui::UiLayout;

const DENSE_SPINNER_FRAMES: &[&str] = &["|", "/", "-", "\\"];
const RICH_SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠴", "⠦"];
const DOUBLE_CLICK_WINDOW: Duration = Duration::from_millis(450);
const HORIZONTAL_SCROLL_STEP: i16 = 8;
const IMAGE_PLANE_CELL_WIDTH: usize = 11;
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
    Activate,
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
enum BrowserRequest {
    Resize {
        width: u16,
        height: u16,
        inspector_height: u16,
    },
    SetFocus(BrowserPaneFocus),
    CycleView {
        forward: bool,
    },
    MoveLeft {
        steps: usize,
    },
    MoveRight {
        steps: usize,
    },
    MoveUp {
        steps: usize,
    },
    MoveDown {
        steps: usize,
    },
    SetImageCursor {
        x: usize,
        y: usize,
    },
    PageUp {
        pages: usize,
    },
    PageDown {
        pages: usize,
    },
    Activate,
    Back,
    Escape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditAction {
    Cancel,
    Commit,
    CommitAndNext,
    CommitAndPrevious,
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
    TogglePrimaryAuxPane,
    CopySelection,
    FocusNext,
    FocusPrevious,
    StartRun,
    ToggleAdvanced,
    CancelSession,
    OpenPathChooser,
    ClearSelection,
    ToggleHelp,
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
    Plots,
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
        Self::Plots,
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
            Self::Plots => "Plots",
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
            Self::Plots => 7,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlotPaneFocus {
    Catalog,
    Canvas,
    Controls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FocusTarget {
    ParametersPane,
    ResultPane,
    PlotCatalog,
    PlotCanvas,
    PlotControls,
    BrowserMain,
    BrowserInspector,
}

#[derive(Debug, Clone)]
pub(crate) struct PlotCatalogRowView {
    pub kind: ListObsPlotKind,
    pub label: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlotControlTarget {
    Option(&'static str),
    ExportPath,
    ExportWidth,
    ExportHeight,
    Refresh,
    ResetControls,
    CopyCli,
    ExportPng,
    ExportPdf,
}

#[derive(Debug, Clone)]
pub(crate) struct PlotControlRowView {
    pub target: PlotControlTarget,
    pub text: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BrowserTab {
    Overview,
    Columns,
    Keywords,
    Cells,
    Subtables,
    Plane,
    Metadata,
    Coordinates,
}

impl BrowserTab {
    pub(crate) const TABLE_ALL: [Self; 5] = [
        Self::Overview,
        Self::Columns,
        Self::Keywords,
        Self::Cells,
        Self::Subtables,
    ];

    pub(crate) const IMAGE_ALL: [Self; 3] = [Self::Metadata, Self::Coordinates, Self::Plane];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Columns => "Columns",
            Self::Keywords => "Keywords",
            Self::Cells => "Cells",
            Self::Subtables => "Subtables",
            Self::Plane => "Plane",
            Self::Metadata => "Metadata",
            Self::Coordinates => "Coordinates",
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

    fn from_image_view(view: ImageBrowserView) -> Self {
        match view {
            ImageBrowserView::Plane => Self::Plane,
            ImageBrowserView::Metadata => Self::Metadata,
            ImageBrowserView::Coordinates => Self::Coordinates,
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
    plot_workspace: PlotWorkspaceState,
    path_chooser: Option<PathChooserState>,
    browser_session: Option<BrowserSession>,
    spinner_frame: usize,
    dragging_divider: bool,
    dragging_result_scrollbar: bool,
    dragging_result_hscrollbar: bool,
    dragging_result_hscrollbar_grab: u16,
    output_selection: Option<OutputSelection>,
    show_help: bool,
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
    cancel_requested: bool,
}

#[derive(Debug)]
struct BrowserSession {
    root_path: String,
    kind: BrowserSessionKind,
}

#[derive(Debug)]
enum BrowserSessionKind {
    Table(TableBrowserSession),
    Image(ImageBrowserSessionState),
}

#[derive(Debug)]
struct TableBrowserSession {
    client: BrowserClient,
    snapshot: BrowserSnapshot,
    viewport: BrowserViewport,
}

#[derive(Debug)]
struct ImageBrowserSessionState {
    client: ImageBrowserClient,
    snapshot: ImageBrowserSnapshot,
    viewport: ImageBrowserViewport,
    hscroll: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserPaneFocus {
    Main,
    Inspector,
}

impl BrowserSession {
    fn kind(&self) -> BrowserAppKind {
        match self.kind {
            BrowserSessionKind::Table(_) => BrowserAppKind::Table,
            BrowserSessionKind::Image(_) => BrowserAppKind::Image,
        }
    }

    fn focus(&self) -> BrowserPaneFocus {
        match &self.kind {
            BrowserSessionKind::Table(session) => match session.snapshot.focus {
                BrowserFocus::Inspector => BrowserPaneFocus::Inspector,
                BrowserFocus::Main => BrowserPaneFocus::Main,
            },
            BrowserSessionKind::Image(session) => match session.snapshot.focus {
                ImageBrowserFocus::Inspector => BrowserPaneFocus::Inspector,
                ImageBrowserFocus::Content => BrowserPaneFocus::Main,
            },
        }
    }

    fn has_inspector(&self) -> bool {
        match &self.kind {
            BrowserSessionKind::Table(session) => session.snapshot.inspector.is_some(),
            BrowserSessionKind::Image(_) => true,
        }
    }

    fn active_tab(&self) -> BrowserTab {
        match &self.kind {
            BrowserSessionKind::Table(session) => BrowserTab::from_view(session.snapshot.view),
            BrowserSessionKind::Image(session) => {
                BrowserTab::from_image_view(session.snapshot.active_view)
            }
        }
    }

    fn tabs(&self) -> &'static [BrowserTab] {
        match self.kind() {
            BrowserAppKind::Table => &BrowserTab::TABLE_ALL,
            BrowserAppKind::Image => &BrowserTab::IMAGE_ALL,
        }
    }

    fn status_line(&self) -> &str {
        match &self.kind {
            BrowserSessionKind::Table(session) => &session.snapshot.status_line,
            BrowserSessionKind::Image(session) => &session.snapshot.status_line,
        }
    }

    fn vertical_metrics(&self) -> Option<(usize, usize)> {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .vertical_metrics
                .map(|metrics| (metrics.total_items, metrics.viewport_items.max(1))),
            BrowserSessionKind::Image(session) => Some((
                session.snapshot.navigation.total_items,
                session.snapshot.navigation.viewport_items.max(1),
            )),
        }
    }

    fn horizontal_metrics(&self, viewport_width: u16) -> Option<(usize, usize)> {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .horizontal_metrics
                .map(|metrics| (metrics.total_items, metrics.viewport_items.max(1))),
            BrowserSessionKind::Image(session) => {
                let viewport_width = viewport_width as usize;
                if viewport_width == 0 {
                    return None;
                }
                Some((
                    image_browser_content_width(&session.snapshot),
                    viewport_width,
                ))
            }
        }
    }

    fn active_scroll(&self) -> u16 {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .vertical_metrics
                .map(|metrics| metrics.selected_index.min(u16::MAX as usize) as u16)
                .unwrap_or(0),
            BrowserSessionKind::Image(session) => session
                .snapshot
                .navigation
                .selected_index
                .min(u16::MAX as usize) as u16,
        }
    }

    fn active_hscroll(&self) -> u16 {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .horizontal_metrics
                .map(|metrics| metrics.selected_index.min(u16::MAX as usize) as u16)
                .unwrap_or(0),
            BrowserSessionKind::Image(session) => session.hscroll,
        }
    }

    fn inspector_lines(&self) -> Option<Vec<String>> {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .inspector
                .as_ref()
                .map(browser_inspector_lines),
            BrowserSessionKind::Image(session) => Some(session.snapshot.inspector_lines.clone()),
        }
    }

    fn main_content_lines(&self) -> Vec<String> {
        match &self.kind {
            BrowserSessionKind::Table(session) => browser_main_content_lines(&session.snapshot),
            BrowserSessionKind::Image(session) => session.snapshot.content_lines.clone(),
        }
    }

    fn cells_view_active(&self) -> bool {
        matches!(
            &self.kind,
            BrowserSessionKind::Table(session) if session.snapshot.view == TableBrowserView::Cells
        )
    }

    fn clipboard_payload(&self) -> Option<(String, &'static str)> {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .inspector
                .as_ref()
                .map(copyable_browser_text),
            BrowserSessionKind::Image(session) => {
                let probe = session.snapshot.probe.as_ref()?;
                Some((render_image_probe(probe), "probe"))
            }
        }
    }

    fn cancel(self) -> Result<(), String> {
        match self.kind {
            BrowserSessionKind::Table(session) => session.client.cancel(),
            BrowserSessionKind::Image(session) => session.client.cancel(),
        }
    }
}

struct PlotPanelState {
    renderer: PanelRenderer<ListObsPlotRenderInput, String>,
    font_size: (u16, u16),
    request_key: Option<PlotRequestKey>,
    last_error: Option<String>,
    image_size: Option<(u32, u32)>,
}

#[derive(Debug)]
struct PathChooserState {
    field_index: usize,
    explorer: FileExplorer,
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlotRequestKey {
    area: Rect,
    theme_mode: ThemeMode,
    snapshot_generation: u64,
    plot_kind: ListObsPlotKind,
    spec_key: String,
    uv_sample_count: usize,
}

impl fmt::Debug for PlotPanelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlotPanelState")
            .field("request_key", &self.request_key)
            .field("last_error", &self.last_error)
            .field("image_size", &self.image_size)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct ListObsRunSnapshot {
    generation: u64,
    summary: ListObsSummary,
    path: Option<PathBuf>,
    options: ListObsOptions,
    dirty: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditTarget {
    FormField(usize),
    PlotExportPath,
    PlotExportWidth,
    PlotExportHeight,
}

#[derive(Debug)]
struct PlotWorkspaceState {
    focus: PlotPaneFocus,
    selected_plot: ListObsPlotKind,
    selected_control: usize,
    uv_spec: ListObsPlotSpec,
    antenna_spec: ListObsPlotSpec,
    scan_spec: ListObsPlotSpec,
    spw_spec: ListObsPlotSpec,
    snapshot: Option<ListObsRunSnapshot>,
    next_generation: u64,
    cached_uv_coverage: Option<(u64, ListObsUvCoverage)>,
    uv_error: Option<String>,
    panel: Option<PlotPanelState>,
    export_path: String,
    export_width: u32,
    export_height: u32,
}

impl PlotWorkspaceState {
    fn new() -> Self {
        let selected_plot = ListObsPlotKind::UvCoverage;
        Self {
            focus: PlotPaneFocus::Catalog,
            selected_plot,
            selected_control: 0,
            uv_spec: ListObsPlotSpec::new(ListObsPlotKind::UvCoverage),
            antenna_spec: ListObsPlotSpec::new(ListObsPlotKind::AntennaLayout),
            scan_spec: ListObsPlotSpec::new(ListObsPlotKind::ScanTimeline),
            spw_spec: ListObsPlotSpec::new(ListObsPlotKind::SpectralWindowCoverage),
            snapshot: None,
            next_generation: 1,
            cached_uv_coverage: None,
            uv_error: None,
            panel: None,
            export_path: default_plot_export_path(selected_plot, ListObsPlotExportFormat::Png),
            export_width: 1600,
            export_height: 900,
        }
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
    target: EditTarget,
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
    DividerToggle,
    Divider,
    ResultScrollbar,
    ResultHorizontalScrollbar,
    Pane(PaneFocus),
    Section(usize),
    Field(usize),
    Tab(ResultTab),
    PlotCatalog(ListObsPlotKind),
    PlotControl(PlotControlTarget),
    PlotCanvas,
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
            plot_workspace: PlotWorkspaceState::new(),
            path_chooser: None,
            browser_session: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            output_selection: None,
            show_help: false,
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
            plot_workspace: PlotWorkspaceState::new(),
            path_chooser: None,
            browser_session: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            output_selection: None,
            show_help: false,
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
        self.running.is_some() || self.browser_session.is_some()
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
        match edit_state.target {
            EditTarget::FormField(target_index) if target_index == field_index => {
                if let Some(field) = self.fields.get_mut(field_index) {
                    field.set_text(edit_state.buffer);
                    self.mark_plot_snapshot_dirty();
                }
            }
            _ => {
                self.edit_state = Some(edit_state);
            }
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
            self.mark_plot_snapshot_dirty();
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
        self.pump_plot_panel();
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
            if self.browser_uses_parameter_pane() && self.pane_focus == PaneFocus::Parameters {
                InputMode::Parameters
            } else {
                InputMode::Browser
            }
        } else {
            match self.pane_focus {
                PaneFocus::Parameters => InputMode::Parameters,
                PaneFocus::Result => InputMode::Result,
            }
        }
    }

    pub(crate) fn browser_uses_parameter_pane(&self) -> bool {
        self.browser_session()
            .is_some_and(|session| session.kind() == BrowserAppKind::Image)
    }

    fn browser_inspector_reachable(&self) -> bool {
        !self.browser_uses_parameter_pane()
            && !self.parameters_pane_collapsed()
            && self
                .browser_session()
                .is_some_and(BrowserSession::has_inspector)
    }

    fn focus_ring(&self) -> Vec<FocusTarget> {
        if self.browser_session.is_some() {
            let mut ring = Vec::new();
            if self.browser_uses_parameter_pane() && !self.parameters_pane_collapsed() {
                ring.push(FocusTarget::ParametersPane);
            }
            ring.push(FocusTarget::BrowserMain);
            if self.browser_inspector_reachable() {
                ring.push(FocusTarget::BrowserInspector);
            }
            return ring;
        }

        let mut ring = Vec::new();
        if !self.parameters_pane_collapsed() {
            ring.push(FocusTarget::ParametersPane);
        }
        if self.active_result_tab == ResultTab::Plots {
            ring.extend([
                FocusTarget::PlotCatalog,
                FocusTarget::PlotCanvas,
                FocusTarget::PlotControls,
            ]);
        } else {
            ring.push(FocusTarget::ResultPane);
        }
        ring
    }

    fn current_focus_target(&self) -> FocusTarget {
        if let Some(session) = self.browser_session() {
            if self.browser_uses_parameter_pane() && self.pane_focus == PaneFocus::Parameters {
                return FocusTarget::ParametersPane;
            }
            return match session.focus() {
                BrowserPaneFocus::Inspector if self.browser_inspector_reachable() => {
                    FocusTarget::BrowserInspector
                }
                _ => FocusTarget::BrowserMain,
            };
        }

        match self.pane_focus {
            PaneFocus::Parameters if !self.parameters_pane_collapsed() => {
                FocusTarget::ParametersPane
            }
            PaneFocus::Parameters | PaneFocus::Result => {
                if self.active_result_tab == ResultTab::Plots {
                    match self.plot_workspace.focus {
                        PlotPaneFocus::Catalog => FocusTarget::PlotCatalog,
                        PlotPaneFocus::Canvas => FocusTarget::PlotCanvas,
                        PlotPaneFocus::Controls => FocusTarget::PlotControls,
                    }
                } else {
                    FocusTarget::ResultPane
                }
            }
        }
    }

    fn set_focus_target(&mut self, target: FocusTarget) {
        match target {
            FocusTarget::ParametersPane => {
                if self.browser_uses_parameter_pane() || !self.parameters_pane_collapsed() {
                    self.pane_focus = PaneFocus::Parameters;
                }
            }
            FocusTarget::ResultPane => {
                self.pane_focus = PaneFocus::Result;
            }
            FocusTarget::PlotCatalog => {
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Catalog;
            }
            FocusTarget::PlotCanvas => {
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Canvas;
            }
            FocusTarget::PlotControls => {
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Controls;
            }
            FocusTarget::BrowserMain => {
                self.pane_focus = PaneFocus::Result;
                if self
                    .browser_session()
                    .is_some_and(|session| session.focus() != BrowserPaneFocus::Main)
                {
                    self.send_browser_command(BrowserRequest::SetFocus(BrowserPaneFocus::Main));
                }
            }
            FocusTarget::BrowserInspector => {
                if self.browser_inspector_reachable() {
                    self.pane_focus = PaneFocus::Parameters;
                    if self
                        .browser_session()
                        .is_some_and(|session| session.focus() != BrowserPaneFocus::Inspector)
                    {
                        self.send_browser_command(BrowserRequest::SetFocus(
                            BrowserPaneFocus::Inspector,
                        ));
                    }
                }
            }
        }
    }

    fn cycle_focus(&mut self, forward: bool) {
        let ring = self.focus_ring();
        if ring.is_empty() {
            return;
        }
        let current = self.current_focus_target();
        let index = ring
            .iter()
            .position(|target| *target == current)
            .unwrap_or(0);
        let next = if forward {
            (index + 1) % ring.len()
        } else if index == 0 {
            ring.len() - 1
        } else {
            index - 1
        };
        self.set_focus_target(ring[next]);
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
        let plots_active = mode == InputMode::Result && self.active_result_tab == ResultTab::Plots;

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

        if self.show_help {
            return match key_event.code {
                KeyCode::Esc | KeyCode::Char('?') if key_event.modifiers.is_empty() => {
                    Some(AppAction::ToggleHelp)
                }
                _ => None,
            };
        }

        match key_event.code {
            KeyCode::Char('?') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return Some(AppAction::ToggleHelp);
            }
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
                return Some(AppAction::TogglePrimaryAuxPane);
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
                    && (!self.has_active_session() || self.browser_uses_parameter_pane()) =>
            {
                return Some(AppAction::StartRun);
            }
            KeyCode::Char('a')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && (!self.has_active_session() || self.browser_uses_parameter_pane()) =>
            {
                return Some(AppAction::ToggleAdvanced);
            }
            KeyCode::Char('o')
                if key_event.modifiers == KeyModifiers::CONTROL
                    && mode != InputMode::Edit
                    && (!self.has_active_session() || self.browser_uses_parameter_pane()) =>
            {
                return Some(AppAction::OpenPathChooser);
            }
            KeyCode::Tab if mode == InputMode::Edit => {
                return Some(AppAction::Edit(EditAction::CommitAndNext));
            }
            KeyCode::BackTab if mode == InputMode::Edit => {
                return Some(AppAction::Edit(EditAction::CommitAndPrevious));
            }
            KeyCode::Tab => return Some(AppAction::FocusNext),
            KeyCode::BackTab => return Some(AppAction::FocusPrevious),
            KeyCode::Char('[') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return if mode == InputMode::Browser {
                    Some(AppAction::Browser(BrowserAction::CycleView {
                        forward: false,
                    }))
                } else {
                    Some(AppAction::Result(ResultAction::PreviousTab))
                };
            }
            KeyCode::Char(']') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return if mode == InputMode::Browser {
                    Some(AppAction::Browser(BrowserAction::CycleView {
                        forward: true,
                    }))
                } else {
                    Some(AppAction::Result(ResultAction::NextTab))
                };
            }
            KeyCode::Enter if plots_active && key_event.modifiers.is_empty() => {
                return Some(AppAction::Result(ResultAction::Activate));
            }
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
            AppAction::TogglePrimaryAuxPane => self.toggle_primary_aux_pane(),
            AppAction::CopySelection => self.copy_output_selection(),
            AppAction::FocusNext => self.cycle_focus(true),
            AppAction::FocusPrevious => self.cycle_focus(false),
            AppAction::StartRun => {
                if self.has_active_session() && self.browser_uses_parameter_pane() {
                    self.cancel_current();
                    self.start_run();
                } else if !self.has_active_session() {
                    self.start_run();
                }
            }
            AppAction::ToggleAdvanced => self.toggle_advanced(),
            AppAction::CancelSession => self.cancel_current(),
            AppAction::OpenPathChooser => self.open_path_chooser_for_selected_field(),
            AppAction::ClearSelection => self.clear_output_selection(),
            AppAction::ToggleHelp => self.show_help = !self.show_help,
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
    }

    pub(crate) fn app_category(&self) -> &str {
        self.app.category
    }

    pub(crate) fn app_name(&self) -> &str {
        self.app.display_name
    }

    pub(crate) fn footer_text(&self) -> String {
        let mut parts = vec!["Tab/Shift-Tab focus".to_string(), "t theme".to_string()];
        if self.edit_state.is_some() {
            parts.extend([
                "Enter save".to_string(),
                "Esc cancel".to_string(),
                "Bksp delete".to_string(),
            ]);
        } else if self.browser_session.is_some() {
            parts.push("[/] views".to_string());
            parts.push("Arrows/hjkl move".to_string());
            parts.push("PgUp/PgDn page".to_string());
            if self.browser_uses_parameter_pane() {
                parts.push("r reopen".to_string());
                parts.push("a adv".to_string());
                parts.push("^o browse".to_string());
            }
            if self
                .browser_session()
                .is_some_and(|session| session.kind() == BrowserAppKind::Table)
            {
                parts.push("Enter open".to_string());
                parts.push("Esc back".to_string());
                parts.push("Bksp parent table".to_string());
            }
            parts.push("y copy".to_string());
        } else if self.active_result_tab == ResultTab::Plots {
            parts.extend([
                "[/] tabs".to_string(),
                "Arrows/hjkl move".to_string(),
                "Enter activate".to_string(),
                "y copy".to_string(),
            ]);
        } else {
            parts.extend([
                "[/] tabs".to_string(),
                "Arrows/hjkl move".to_string(),
                "y copy".to_string(),
            ]);
            if self.running.is_none() {
                parts.push("a adv".to_string());
                parts.push("r run".to_string());
            } else {
                parts.push("x cancel".to_string());
            }
        }
        parts.extend([
            "p pane".to_string(),
            "? help".to_string(),
            "b apps".to_string(),
            "q quit".to_string(),
        ]);
        parts.join("  ")
    }

    pub(crate) fn help_visible(&self) -> bool {
        self.show_help
    }

    pub(crate) fn help_overlay_lines(&self) -> Vec<String> {
        let mut lines = vec![
            "Key Help".to_string(),
            String::new(),
            "Global: Tab/Shift-Tab focus  [/] primary views  ? help".to_string(),
            "Global: p primary pane  y copy  b apps  t theme  q quit".to_string(),
        ];
        if self.running.is_none() && self.browser_session.is_none() && self.edit_state.is_none() {
            lines.push("Global: r run  a advanced options".to_string());
        } else if self.running.is_some() {
            lines.push("Global: x cancel active process".to_string());
        } else if self.browser_session.is_some() {
            lines.push("Global: x close browser session".to_string());
        }
        lines.push(String::new());

        if self.edit_state.is_some() {
            lines.extend([
                "Edit: Enter save  Tab next field  Shift-Tab previous field".to_string(),
                "Edit: Esc cancel  Backspace delete".to_string(),
            ]);
            return lines;
        }

        match self.current_focus_target() {
            FocusTarget::ParametersPane => {
                lines.extend([
                    "Focus: Parameters pane".to_string(),
                    "Move: Up/Down or j/k".to_string(),
                    "Adjust: Left/Right".to_string(),
                    "Activate: Enter or Space".to_string(),
                ]);
            }
            FocusTarget::ResultPane => {
                lines.extend([
                    "Focus: Result pane".to_string(),
                    "Move: Up/Down or j/k".to_string(),
                    "Scroll horizontally: Left/Right or h/l".to_string(),
                ]);
            }
            FocusTarget::PlotCatalog => {
                lines.extend([
                    "Focus: Plot catalog".to_string(),
                    "Move: Up/Down or j/k".to_string(),
                    "Activate: Enter".to_string(),
                ]);
            }
            FocusTarget::PlotCanvas => {
                lines.extend([
                    "Focus: Plot canvas".to_string(),
                    "Canvas is passive in this wave.".to_string(),
                ]);
            }
            FocusTarget::PlotControls => {
                lines.extend([
                    "Focus: Plot controls".to_string(),
                    "Move: Up/Down or j/k".to_string(),
                    "Adjust: Left/Right or h/l".to_string(),
                    "Activate: Enter".to_string(),
                ]);
            }
            FocusTarget::BrowserMain => {
                lines.extend([
                    "Focus: Browser main pane".to_string(),
                    "Move: Arrows or h/j/k/l".to_string(),
                    "Page: PgUp/PgDn".to_string(),
                ]);
                if self
                    .browser_session()
                    .is_some_and(|session| session.kind() == BrowserAppKind::Table)
                {
                    lines.extend([
                        "Activate: Enter".to_string(),
                        "Back: Esc  Parent table: Backspace".to_string(),
                    ]);
                }
            }
            FocusTarget::BrowserInspector => {
                lines.extend([
                    "Focus: Browser inspector".to_string(),
                    "Move: Arrows or h/j/k/l".to_string(),
                    "Page: PgUp/PgDn".to_string(),
                ]);
                if self
                    .browser_session()
                    .is_some_and(|session| session.kind() == BrowserAppKind::Table)
                {
                    lines.extend(["Activate: Enter".to_string(), "Back: Esc".to_string()]);
                } else if self.browser_session().is_some_and(|session| {
                    matches!(&session.kind, BrowserSessionKind::Image(state) if state.snapshot.hidden_axis.is_some())
                }) {
                    lines.push("Adjust hidden axis: Left/Right".to_string());
                }
            }
        }
        lines
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
            if self.browser_uses_parameter_pane() {
                match self.theme_mode() {
                    ThemeMode::DenseAnsi => format!("Parameters [live]{focus}"),
                    ThemeMode::RichPanel => format!("◈ Parameters [live]{focus}"),
                }
            } else {
                match self.theme_mode() {
                    ThemeMode::DenseAnsi => format!("Inspector [live]{focus}"),
                    ThemeMode::RichPanel => format!("◈ Inspector [live]{focus}"),
                }
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
        if self.browser_session.is_some() && !self.browser_uses_parameter_pane() {
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
                "View: {}  Path: {}  Mode: {}",
                session.active_tab().label(),
                session.root_path,
                match session.kind() {
                    BrowserAppKind::Table => "tablebrowser",
                    BrowserAppKind::Image => "imexplore",
                }
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
        self.browser_session()
            .map(BrowserSession::tabs)
            .unwrap_or(&BrowserTab::TABLE_ALL)
    }

    pub(crate) fn active_browser_tab_label(&self) -> Option<&'static str> {
        self.active_browser_tab().map(BrowserTab::label)
    }

    pub(crate) fn active_browser_scroll_metrics(
        &self,
        _viewport_height: u16,
    ) -> Option<(usize, usize)> {
        self.browser_session()?.vertical_metrics()
    }

    pub(crate) fn active_browser_hscroll_metrics(
        &self,
        viewport_width: u16,
    ) -> Option<(usize, usize)> {
        self.browser_session()?.horizontal_metrics(viewport_width)
    }

    pub(crate) fn browser_inspector_lines(&self) -> Option<Vec<String>> {
        self.browser_session()?.inspector_lines()
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
        Some(self.browser_session()?.main_content_lines())
    }

    pub(crate) fn sync_browser_viewport(&mut self, width: u16, height: u16, inspector_height: u16) {
        let Some(current_viewport) =
            self.browser_session
                .as_ref()
                .map(|session| match &session.kind {
                    BrowserSessionKind::Table(state) => (
                        state.viewport.width,
                        state.viewport.height,
                        state.viewport.inspector_height,
                    ),
                    BrowserSessionKind::Image(state) => (
                        state.viewport.width,
                        state.viewport.height,
                        state.viewport.inspector_height,
                    ),
                })
        else {
            return;
        };
        let viewport = (width, height, inspector_height);
        if viewport == current_viewport {
            return;
        }
        self.clear_output_selection();
        self.send_browser_command(BrowserRequest::Resize {
            width,
            height,
            inspector_height,
        });
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
            ResultTab::Plots => ResultContent::Graphic(self.plots_tab_summary()),
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
            self.mark_plot_snapshot_dirty();
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
            self.mark_plot_snapshot_dirty();
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
        if self.active_result_tab == ResultTab::Plots {
            self.sync_plot_export_path_for_selected_plot();
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

    fn plots_tab_summary(&self) -> String {
        if let Some(error) = self.plot_workspace.uv_error.as_deref() {
            return format!("Plots unavailable. {error}");
        }
        if let Some(summary) = self.current_plot_summary() {
            return summary.to_string();
        }
        if self.plot_workspace.snapshot.is_some() {
            return "Select a plot from the catalog to render it.".to_string();
        }
        "Run listobs to populate the plot workspace.".to_string()
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
    pub(crate) fn plot_snapshot_dirty_for_test(&self) -> bool {
        self.plot_workspace
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.dirty)
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
        self.plot_workspace
            .cached_uv_coverage
            .as_ref()
            .map(|(_, coverage)| coverage)
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
    pub(crate) fn browser_focus_for_test(&self) -> Option<BrowserPaneFocus> {
        self.browser_session.as_ref().map(BrowserSession::focus)
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
        if self.active_result_tab != ResultTab::Plots {
            return;
        }
        self.ensure_plot_requested(layout);
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
                    let browser_session = self.browser_session()?;
                    let browser_cells = browser_session.cells_view_active();
                    let (image_plane_view, image_hscroll) = match &browser_session.kind {
                        BrowserSessionKind::Image(state) => (
                            state.snapshot.active_view == ImageBrowserView::Plane,
                            self.active_browser_hscroll() as usize,
                        ),
                        BrowserSessionKind::Table(_) => (false, 0),
                    };
                    Some(VisibleTextBuffer {
                        area,
                        lines: lines
                            .into_iter()
                            .take(area.height as usize)
                            .map(|line| {
                                if browser_cells {
                                    browser_cells_visible_line(&line)
                                } else if image_hscroll > 0 || image_plane_view {
                                    image_browser_visible_line(
                                        &line,
                                        image_plane_view,
                                        image_hscroll,
                                        area.width as usize,
                                    )
                                } else {
                                    VisibleTextLine::plain(slice_visible_text(
                                        &line,
                                        image_hscroll,
                                        area.width as usize,
                                    ))
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

    fn image_plane_click_target(
        &self,
        column: u16,
        row: u16,
        layout: &UiLayout,
    ) -> Option<(usize, usize)> {
        let area = result_text_area(layout);
        if !rect_contains(area, column, row) {
            return None;
        }
        let BrowserSessionKind::Image(state) = &self.browser_session()?.kind else {
            return None;
        };
        if state.snapshot.active_view != ImageBrowserView::Plane {
            return None;
        }

        let relative_row = usize::from(row.saturating_sub(area.y));
        if relative_row == 0 {
            return None;
        }
        let line = state.snapshot.content_lines.get(relative_row)?;
        let pipe_index = line.find('|')?;
        let pixel_y = line[..pipe_index].trim().parse::<usize>().ok()?;

        let absolute_col =
            self.active_browser_hscroll() as usize + usize::from(column.saturating_sub(area.x));
        let after_pipe = absolute_col.checked_sub(pipe_index + 1)?;
        if after_pipe == 0 {
            return None;
        }

        let chunk_offset = after_pipe - 1;
        let stride = IMAGE_PLANE_CELL_WIDTH + 1;
        let offset_in_chunk = chunk_offset % stride;
        if offset_in_chunk >= IMAGE_PLANE_CELL_WIDTH {
            return None;
        }

        let pixel_x = chunk_offset / stride;
        let max_x = image_plane_column_count(&state.snapshot)?;
        if pixel_x >= max_x {
            return None;
        }

        Some((pixel_x, pixel_y))
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
        if self.active_result_tab == ResultTab::Plots {
            match action {
                ResultAction::PreviousTab => {
                    if self.plot_workspace.focus == PlotPaneFocus::Controls {
                        self.adjust_selected_plot_control(false);
                    } else {
                        self.cycle_visible_result_tab(false);
                    }
                }
                ResultAction::NextTab => {
                    if self.plot_workspace.focus == PlotPaneFocus::Controls {
                        self.adjust_selected_plot_control(true);
                    } else {
                        self.cycle_visible_result_tab(true);
                    }
                }
                ResultAction::Scroll(delta) => self.scroll_active_plot_workspace(delta),
                ResultAction::ScrollHorizontal(delta) => {
                    if self.plot_workspace.focus == PlotPaneFocus::Controls {
                        self.adjust_selected_plot_control(delta >= 0);
                    }
                }
                ResultAction::Activate => self.activate_plot_workspace_selection(),
            }
            return;
        }
        match action {
            ResultAction::PreviousTab => self.cycle_visible_result_tab(false),
            ResultAction::NextTab => self.cycle_visible_result_tab(true),
            ResultAction::Scroll(delta) => self.scroll_active_result(delta),
            ResultAction::ScrollHorizontal(delta) => self.scroll_active_result_horizontal(delta),
            ResultAction::Activate => {}
        }
    }

    fn apply_browser_action(&mut self, action: BrowserAction) {
        match action {
            BrowserAction::CycleView { forward } => {
                self.send_browser_command(BrowserRequest::CycleView { forward });
            }
            BrowserAction::MoveLeft => {
                self.send_browser_command(BrowserRequest::MoveLeft { steps: 1 });
            }
            BrowserAction::MoveRight => {
                self.send_browser_command(BrowserRequest::MoveRight { steps: 1 });
            }
            BrowserAction::MoveUp => {
                self.send_browser_command(BrowserRequest::MoveUp { steps: 1 });
            }
            BrowserAction::MoveDown => {
                self.send_browser_command(BrowserRequest::MoveDown { steps: 1 });
            }
            BrowserAction::PageUp => self.send_browser_command(BrowserRequest::PageUp { pages: 1 }),
            BrowserAction::PageDown => {
                self.send_browser_command(BrowserRequest::PageDown { pages: 1 })
            }
            BrowserAction::Activate => self.send_browser_command(BrowserRequest::Activate),
            BrowserAction::Back => self.send_browser_command(BrowserRequest::Back),
            BrowserAction::Escape => self.send_browser_command(BrowserRequest::Escape),
        }
    }

    fn activate_browser_tab(&mut self, tab: BrowserTab) {
        let Some(current) = self.active_browser_tab() else {
            return;
        };
        if current == tab {
            return;
        }

        let tabs = self.browser_tabs();
        let current_index = tabs
            .iter()
            .position(|candidate| *candidate == current)
            .unwrap_or(0);
        let target_index = tabs
            .iter()
            .position(|candidate| *candidate == tab)
            .unwrap_or(0);
        let (steps, forward) = if target_index >= current_index {
            (target_index - current_index, true)
        } else {
            (current_index - target_index, false)
        };
        for _ in 0..steps {
            self.send_browser_command(BrowserRequest::CycleView { forward });
        }
    }

    fn copy_output_selection(&mut self) {
        let payload = self
            .active_selected_text()
            .map(|text| (text, "selection"))
            .or_else(|| {
                if self.browser_session.is_some() {
                    self.browser_clipboard_payload()
                } else if self.active_result_tab == ResultTab::Plots {
                    self.current_plot_summary()
                        .map(|summary| (summary, "plot summary"))
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
                let committed = self.edit_state.take().expect("edit state");
                self.commit_plot_or_field_edit(committed);
            }
            EditAction::CommitAndNext => {
                let committed = self.edit_state.take().expect("edit state");
                let target = committed.target;
                self.commit_plot_or_field_edit(committed);
                self.advance_after_edit(target, true);
            }
            EditAction::CommitAndPrevious => {
                let committed = self.edit_state.take().expect("edit state");
                let target = committed.target;
                self.commit_plot_or_field_edit(committed);
                self.advance_after_edit(target, false);
            }
            EditAction::DeleteBackward => {
                edit_state.buffer.pop();
            }
            EditAction::Insert(character) => {
                edit_state.buffer.push(character);
            }
        }
    }

    fn advance_after_edit(&mut self, target: EditTarget, forward: bool) {
        match target {
            EditTarget::FormField(field_index) => self.advance_form_edit(field_index, forward),
            EditTarget::PlotExportPath => {
                self.advance_plot_edit(PlotControlTarget::ExportPath, forward)
            }
            EditTarget::PlotExportWidth => {
                self.advance_plot_edit(PlotControlTarget::ExportWidth, forward)
            }
            EditTarget::PlotExportHeight => {
                self.advance_plot_edit(PlotControlTarget::ExportHeight, forward)
            }
        }
    }

    fn advance_form_edit(&mut self, field_index: usize, forward: bool) {
        let targets = self
            .visible_form_targets()
            .into_iter()
            .filter_map(|target| match target {
                FormSelection::Field(index) => Some(index),
                FormSelection::Section(_) => None,
            })
            .collect::<Vec<_>>();
        let Some(position) = targets
            .iter()
            .position(|candidate| *candidate == field_index)
        else {
            return;
        };
        let next = if forward {
            (position + 1) % targets.len()
        } else if position == 0 {
            targets.len() - 1
        } else {
            position - 1
        };
        let next_field = targets[next];
        self.selected_form = FormSelection::Field(next_field);
        self.pane_focus = PaneFocus::Parameters;
        if self
            .fields
            .get(next_field)
            .is_some_and(|field| matches!(field.value, FormValue::Text(_)))
        {
            self.enter_edit_mode(next_field);
        }
    }

    fn advance_plot_edit(&mut self, current: PlotControlTarget, forward: bool) {
        const EDITABLE_TARGETS: [PlotControlTarget; 3] = [
            PlotControlTarget::ExportPath,
            PlotControlTarget::ExportWidth,
            PlotControlTarget::ExportHeight,
        ];
        let Some(position) = EDITABLE_TARGETS
            .iter()
            .position(|target| *target == current)
        else {
            return;
        };
        let next = if forward {
            (position + 1) % EDITABLE_TARGETS.len()
        } else if position == 0 {
            EDITABLE_TARGETS.len() - 1
        } else {
            position - 1
        };
        let next_target = EDITABLE_TARGETS[next];
        if let Some(index) = self
            .plot_control_rows()
            .iter()
            .position(|row| row.target == next_target)
        {
            self.plot_workspace.selected_control = index;
            self.pane_focus = PaneFocus::Result;
            self.plot_workspace.focus = PlotPaneFocus::Controls;
            self.activate_plot_workspace_selection();
        }
    }

    fn handle_left_mouse_down(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        if layout.in_divider_toggle(mouse_event.column, mouse_event.row) {
            self.clear_output_selection();
            self.dragging_divider = false;
            self.dragging_result_scrollbar = false;
            self.toggle_primary_aux_pane();
            self.last_click = Some(ClickState {
                target: ClickTarget::DividerToggle,
                at: Instant::now(),
            });
            return;
        }

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

        if self.active_result_tab == ResultTab::Plots {
            if let Some(row) = layout.plot_catalog_at(mouse_event.column, mouse_event.row) {
                self.pane_focus = PaneFocus::Result;
                self.clear_output_selection_for_target(OutputPane::Result);
                self.plot_workspace.focus = PlotPaneFocus::Catalog;
                self.set_selected_plot(row.kind);
                self.last_click = Some(ClickState {
                    target: ClickTarget::PlotCatalog(row.kind),
                    at: Instant::now(),
                });
                return;
            }

            if let Some(target) = layout.plot_control_at(mouse_event.column, mouse_event.row) {
                self.pane_focus = PaneFocus::Result;
                self.clear_output_selection_for_target(OutputPane::Result);
                self.plot_workspace.focus = PlotPaneFocus::Controls;
                if let Some(index) = self
                    .plot_control_rows()
                    .iter()
                    .position(|row| row.target == target)
                {
                    self.plot_workspace.selected_control = index;
                }
                self.last_click = Some(ClickState {
                    target: ClickTarget::PlotControl(target),
                    at: Instant::now(),
                });
                match target {
                    PlotControlTarget::Refresh
                    | PlotControlTarget::ResetControls
                    | PlotControlTarget::CopyCli
                    | PlotControlTarget::ExportPng
                    | PlotControlTarget::ExportPdf => self.activate_plot_workspace_selection(),
                    PlotControlTarget::Option(_)
                    | PlotControlTarget::ExportPath
                    | PlotControlTarget::ExportWidth
                    | PlotControlTarget::ExportHeight => {}
                }
                return;
            }

            if layout.in_plot_canvas(mouse_event.column, mouse_event.row) {
                self.pane_focus = PaneFocus::Result;
                self.clear_output_selection_for_target(OutputPane::Result);
                self.plot_workspace.focus = PlotPaneFocus::Canvas;
                self.last_click = Some(ClickState {
                    target: ClickTarget::PlotCanvas,
                    at: Instant::now(),
                });
                return;
            }
        }

        if let Some((x, y)) =
            self.image_plane_click_target(mouse_event.column, mouse_event.row, layout)
        {
            self.set_focus_target(FocusTarget::BrowserMain);
            self.clear_output_selection_for_target(OutputPane::Result);
            self.send_browser_command(BrowserRequest::SetImageCursor { x, y });
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Result),
                at: Instant::now(),
            });
            return;
        }

        if let Some((target, point)) =
            self.selection_point_at(mouse_event.column, mouse_event.row, layout)
        {
            if self.browser_session.is_some() {
                match target {
                    OutputPane::Result => self.set_focus_target(FocusTarget::BrowserMain),
                    OutputPane::LeftOutput => self.set_focus_target(FocusTarget::BrowserInspector),
                }
            } else {
                self.pane_focus = if target == OutputPane::Result {
                    PaneFocus::Result
                } else {
                    PaneFocus::Parameters
                };
            }
            self.begin_output_selection(target, point);
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(self.pane_focus),
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            if self.browser_session.is_some() {
                self.set_focus_target(FocusTarget::BrowserMain);
            } else {
                self.pane_focus = PaneFocus::Result;
            }
            self.clear_output_selection_for_target(OutputPane::Result);
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Result),
                at: Instant::now(),
            });
            return;
        }

        if self.has_active_session() && !self.browser_uses_parameter_pane() {
            return;
        }

        if let Some(target) = layout.form_target_at(mouse_event.column, mouse_event.row) {
            if self.browser_uses_parameter_pane() {
                self.pane_focus = PaneFocus::Parameters;
            } else if self.browser_session.is_some() {
                self.set_focus_target(FocusTarget::BrowserInspector);
            } else {
                self.pane_focus = PaneFocus::Parameters;
            }
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
        if self.active_result_tab == ResultTab::Plots {
            if layout
                .plot_catalog_at(mouse_event.column, mouse_event.row)
                .is_some()
            {
                self.clear_output_selection_for_target(OutputPane::Result);
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Catalog;
                self.scroll_active_plot_workspace(delta);
                return;
            }
            if layout
                .plot_control_at(mouse_event.column, mouse_event.row)
                .is_some()
            {
                self.clear_output_selection_for_target(OutputPane::Result);
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Controls;
                self.scroll_active_plot_workspace(delta);
                return;
            }
            if layout.in_plot_canvas(mouse_event.column, mouse_event.row) {
                self.clear_output_selection_for_target(OutputPane::Result);
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Canvas;
                return;
            }
        }

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
        if self.active_result_tab == ResultTab::Plots {
            if layout
                .plot_control_at(mouse_event.column, mouse_event.row)
                .is_some()
            {
                self.clear_output_selection_for_target(OutputPane::Result);
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Controls;
                self.adjust_selected_plot_control(delta >= 0);
                return;
            }
            if layout
                .plot_catalog_at(mouse_event.column, mouse_event.row)
                .is_some()
            {
                self.clear_output_selection_for_target(OutputPane::Result);
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Catalog;
                return;
            }
            if layout.in_plot_canvas(mouse_event.column, mouse_event.row) {
                self.clear_output_selection_for_target(OutputPane::Result);
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Canvas;
                return;
            }
        }

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
                target: EditTarget::FormField(field_index),
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
        self.browser_session().map(BrowserSession::active_tab)
    }

    fn toggle_primary_aux_pane(&mut self) {
        if (self.browser_session.is_some() && !self.browser_uses_parameter_pane())
            || self.sections.is_empty()
        {
            return;
        }
        self.clear_output_selection();
        let next = if self.parameters_pane_collapsed() {
            self.config_store.pane_restore_ratio()
        } else {
            0.0
        };
        self.config_store.set_pane_split_ratio(next);
        if next == 0.0 {
            self.set_focus_target(if self.active_result_tab == ResultTab::Plots {
                FocusTarget::PlotCatalog
            } else {
                FocusTarget::ResultPane
            });
        } else if self.focus_ring().contains(&FocusTarget::ParametersPane) {
            self.pane_focus = PaneFocus::Parameters;
        }
    }

    pub(crate) fn active_browser_scroll(&self) -> u16 {
        self.browser_session()
            .map(BrowserSession::active_scroll)
            .unwrap_or(0)
    }

    pub(crate) fn active_browser_hscroll(&self) -> u16 {
        self.browser_session()
            .map(BrowserSession::active_hscroll)
            .unwrap_or(0)
    }

    fn scroll_active_browser(&mut self, delta: i16) {
        self.clear_output_selection_for_target(OutputPane::Result);
        if delta.is_negative() {
            self.send_browser_command(BrowserRequest::MoveUp {
                steps: delta.unsigned_abs() as usize,
            });
        } else {
            self.send_browser_command(BrowserRequest::MoveDown {
                steps: delta as usize,
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
        if let Some(BrowserSession {
            kind: BrowserSessionKind::Image(state),
            ..
        }) = self.browser_session.as_mut()
        {
            let max_scroll = image_browser_max_hscroll(&state.snapshot, state.viewport.width);
            let next = if delta.is_negative() {
                usize::from(state.hscroll).saturating_sub(delta.unsigned_abs() as usize)
            } else {
                usize::from(state.hscroll).saturating_add(delta as usize)
            };
            state.hscroll = next.min(max_scroll).min(u16::MAX as usize) as u16;
            return;
        }
        if delta.is_negative() {
            self.send_browser_command(BrowserRequest::MoveLeft {
                steps: delta.unsigned_abs() as usize,
            });
        } else {
            self.send_browser_command(BrowserRequest::MoveRight {
                steps: delta as usize,
            });
        }
    }

    fn set_active_browser_hscroll(&mut self, scroll: usize) {
        if let Some(BrowserSession {
            kind: BrowserSessionKind::Image(state),
            ..
        }) = self.browser_session.as_mut()
        {
            let max_scroll = image_browser_max_hscroll(&state.snapshot, state.viewport.width);
            state.hscroll = scroll.min(max_scroll).min(u16::MAX as usize) as u16;
            return;
        }
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

        if self.schema.is_none() {
            self.result.status_line = "Cannot run without a loaded UI schema.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.active_result_tab = ResultTab::Stderr;
            return;
        }

        if self.app.is_browser_session() {
            self.start_browser_session();
            return;
        }

        match self.build_execution_plan() {
            Ok(plan) => match spawn_process(&plan) {
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
                        cancel_requested: false,
                    });
                }
                Err(error) => {
                    self.result.status_line = format!("Failed to launch {}.", self.app.id);
                    self.result.status_kind = StatusKind::Error;
                    self.result.stderr = format!("{error}\n");
                    self.active_result_tab = ResultTab::Stderr;
                }
            },
            Err(error) => {
                self.result.status_line = "Cannot start command.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
                self.active_result_tab = ResultTab::Stderr;
            }
        }
    }

    fn start_browser_session(&mut self) {
        self.clear_output_selection();
        let Some(path_field) = self.app.browser_path_field_id() else {
            self.result.status_line =
                "Browser session is missing a startup path field.".to_string();
            self.result.status_kind = StatusKind::Error;
            return;
        };
        let Some(path) = self
            .field_text(path_field)
            .filter(|value| !value.trim().is_empty())
        else {
            self.result.status_line = format!(
                "{} is required.",
                self.fields
                    .iter()
                    .find(|field| field.schema.id == path_field)
                    .map(|field| field.schema.label.as_str())
                    .unwrap_or("Path")
            );
            self.result.status_kind = StatusKind::Error;
            return;
        };

        let Some(browser_kind) = self.app.browser_kind() else {
            self.result.status_line = "Selected app is not a browser session.".to_string();
            self.result.status_kind = StatusKind::Error;
            return;
        };

        match browser_kind {
            BrowserAppKind::Table => {
                let viewport = BrowserViewport::new(120, 24);
                match self
                    .app
                    .resolve_command()
                    .and_then(|command| BrowserClient::spawn(&command))
                {
                    Ok(client) => match client.request_startup(BrowserCommand::OpenRoot {
                        path: path.clone(),
                        viewport,
                    }) {
                        Ok(snapshot) => {
                            self.result = ResultState {
                                status_line: snapshot.status_line.clone(),
                                status_kind: StatusKind::Info,
                                ..ResultState::default()
                            };
                            self.edit_state = None;
                            self.pane_focus = PaneFocus::Result;
                            self.browser_session = Some(BrowserSession {
                                root_path: path,
                                kind: BrowserSessionKind::Table(TableBrowserSession {
                                    client,
                                    snapshot,
                                    viewport,
                                }),
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
                        self.report_browser_error(
                            "Failed to launch table browser.",
                            format!("{error}\n"),
                        );
                    }
                }
            }
            BrowserAppKind::Image => {
                let viewport = ImageBrowserViewport::new(120, 24);
                match self
                    .app
                    .resolve_command()
                    .and_then(|command| ImageBrowserClient::spawn(&command))
                {
                    Ok(client) => match client.request_startup(ImageBrowserCommand::OpenRoot {
                        path: path.clone(),
                        viewport,
                    }) {
                        Ok(snapshot) => {
                            self.result = ResultState {
                                status_line: snapshot.status_line.clone(),
                                status_kind: StatusKind::Info,
                                ..ResultState::default()
                            };
                            self.edit_state = None;
                            self.pane_focus = PaneFocus::Result;
                            self.browser_session = Some(BrowserSession {
                                root_path: path,
                                kind: BrowserSessionKind::Image(ImageBrowserSessionState {
                                    client,
                                    snapshot,
                                    viewport,
                                    hscroll: 0,
                                }),
                            });
                        }
                        Err(error) => {
                            let _ = client.cancel();
                            self.report_browser_error(
                                "Failed to open imexplore.",
                                format!("{error}\n"),
                            );
                        }
                    },
                    Err(error) => {
                        self.report_browser_error(
                            "Failed to launch imexplore.",
                            format!("{error}\n"),
                        );
                    }
                }
            }
        }
    }

    fn send_browser_command(&mut self, command: BrowserRequest) {
        let result = {
            let Some(session) = self.browser_session.as_mut() else {
                return;
            };
            match &mut session.kind {
                BrowserSessionKind::Table(state) => {
                    let request = match command {
                        BrowserRequest::Resize {
                            width,
                            height,
                            inspector_height,
                        } => Some(BrowserCommand::Resize {
                            viewport: BrowserViewport::with_inspector_height(
                                width,
                                height,
                                inspector_height,
                            ),
                        }),
                        BrowserRequest::SetFocus(BrowserPaneFocus::Main) => {
                            Some(BrowserCommand::SetFocus {
                                focus: BrowserFocus::Main,
                                viewport: None,
                            })
                        }
                        BrowserRequest::SetFocus(BrowserPaneFocus::Inspector) => {
                            Some(BrowserCommand::SetFocus {
                                focus: BrowserFocus::Inspector,
                                viewport: None,
                            })
                        }
                        BrowserRequest::CycleView { forward } => Some(BrowserCommand::CycleView {
                            forward,
                            viewport: None,
                        }),
                        BrowserRequest::MoveLeft { steps } => Some(BrowserCommand::MoveLeft {
                            steps,
                            viewport: None,
                        }),
                        BrowserRequest::MoveRight { steps } => Some(BrowserCommand::MoveRight {
                            steps,
                            viewport: None,
                        }),
                        BrowserRequest::MoveUp { steps } => Some(BrowserCommand::MoveUp {
                            steps,
                            viewport: None,
                        }),
                        BrowserRequest::MoveDown { steps } => Some(BrowserCommand::MoveDown {
                            steps,
                            viewport: None,
                        }),
                        BrowserRequest::PageUp { pages } => Some(BrowserCommand::PageUp {
                            pages,
                            viewport: None,
                        }),
                        BrowserRequest::PageDown { pages } => Some(BrowserCommand::PageDown {
                            pages,
                            viewport: None,
                        }),
                        BrowserRequest::Activate => {
                            Some(BrowserCommand::Activate { viewport: None })
                        }
                        BrowserRequest::Back => Some(BrowserCommand::Back { viewport: None }),
                        BrowserRequest::Escape => Some(BrowserCommand::Escape { viewport: None }),
                        BrowserRequest::SetImageCursor { .. } => None,
                    };
                    let Some(request) = request else {
                        return;
                    };
                    match state.client.request(request) {
                        Ok(snapshot) => {
                            if let BrowserRequest::Resize {
                                width,
                                height,
                                inspector_height,
                            } = command
                            {
                                state.viewport = BrowserViewport::with_inspector_height(
                                    width,
                                    height,
                                    inspector_height,
                                );
                            }
                            state.snapshot = snapshot;
                            Ok(())
                        }
                        Err(error) => Err((error, state.client.stderr_text())),
                    }
                }
                BrowserSessionKind::Image(state) => {
                    let request = match command {
                        BrowserRequest::Resize {
                            width,
                            height,
                            inspector_height,
                        } => Some(ImageBrowserCommand::Resize {
                            viewport: ImageBrowserViewport::with_inspector_height(
                                width,
                                height,
                                inspector_height,
                            ),
                        }),
                        BrowserRequest::SetFocus(BrowserPaneFocus::Main) => {
                            Some(ImageBrowserCommand::SetFocus {
                                focus: ImageBrowserFocus::Content,
                            })
                        }
                        BrowserRequest::SetFocus(BrowserPaneFocus::Inspector) => {
                            Some(ImageBrowserCommand::SetFocus {
                                focus: ImageBrowserFocus::Inspector,
                            })
                        }
                        BrowserRequest::CycleView { forward } => {
                            Some(ImageBrowserCommand::CycleView { forward })
                        }
                        BrowserRequest::MoveLeft { steps } => {
                            if state.snapshot.focus == ImageBrowserFocus::Inspector
                                && state.snapshot.hidden_axis.is_some()
                            {
                                Some(ImageBrowserCommand::StepHiddenAxis {
                                    delta: -(steps as i32),
                                })
                            } else if state.snapshot.active_view != ImageBrowserView::Plane {
                                state.hscroll = state.hscroll.saturating_sub(steps as u16);
                                None
                            } else {
                                Some(ImageBrowserCommand::MoveCursor {
                                    dx: -(steps as i32),
                                    dy: 0,
                                })
                            }
                        }
                        BrowserRequest::MoveRight { steps } => {
                            if state.snapshot.focus == ImageBrowserFocus::Inspector
                                && state.snapshot.hidden_axis.is_some()
                            {
                                Some(ImageBrowserCommand::StepHiddenAxis {
                                    delta: steps as i32,
                                })
                            } else if state.snapshot.active_view != ImageBrowserView::Plane {
                                let max_scroll = image_browser_max_hscroll(
                                    &state.snapshot,
                                    state.viewport.width,
                                );
                                state.hscroll = state
                                    .hscroll
                                    .saturating_add(steps as u16)
                                    .min(max_scroll.min(u16::MAX as usize) as u16);
                                None
                            } else {
                                Some(ImageBrowserCommand::MoveCursor {
                                    dx: steps as i32,
                                    dy: 0,
                                })
                            }
                        }
                        BrowserRequest::MoveUp { steps } => Some(ImageBrowserCommand::MoveCursor {
                            dx: 0,
                            dy: -(steps as i32),
                        }),
                        BrowserRequest::MoveDown { steps } => {
                            Some(ImageBrowserCommand::MoveCursor {
                                dx: 0,
                                dy: steps as i32,
                            })
                        }
                        BrowserRequest::SetImageCursor { x, y } => {
                            match image_plane_selected_pixel(&state.snapshot) {
                                Some((current_x, current_y)) => {
                                    let dx = x as i32 - current_x as i32;
                                    let dy = y as i32 - current_y as i32;
                                    if dx == 0 && dy == 0 {
                                        None
                                    } else {
                                        Some(ImageBrowserCommand::MoveCursor { dx, dy })
                                    }
                                }
                                None => None,
                            }
                        }
                        BrowserRequest::PageUp { pages } => Some(ImageBrowserCommand::MoveCursor {
                            dx: 0,
                            dy: -((pages as i32) * i32::from(state.viewport.height.max(1))),
                        }),
                        BrowserRequest::PageDown { pages } => {
                            Some(ImageBrowserCommand::MoveCursor {
                                dx: 0,
                                dy: (pages as i32) * i32::from(state.viewport.height.max(1)),
                            })
                        }
                        BrowserRequest::Activate
                        | BrowserRequest::Back
                        | BrowserRequest::Escape => None,
                    };
                    let Some(request) = request else {
                        return;
                    };
                    match state.client.request(request) {
                        Ok(snapshot) => {
                            if let BrowserRequest::Resize {
                                width,
                                height,
                                inspector_height,
                            } = command
                            {
                                state.viewport = ImageBrowserViewport::with_inspector_height(
                                    width,
                                    height,
                                    inspector_height,
                                );
                            }
                            state.snapshot = snapshot;
                            state.hscroll = state.hscroll.min(
                                image_browser_max_hscroll(&state.snapshot, state.viewport.width)
                                    .min(u16::MAX as usize) as u16,
                            );
                            Ok(())
                        }
                        Err(error) => Err((error, state.client.stderr_text())),
                    }
                }
            }
        };

        match result {
            Ok(()) => {
                self.clear_output_selection();
                self.pane_focus = match self.browser_session() {
                    Some(session)
                        if session.focus() == BrowserPaneFocus::Inspector
                            && self.browser_inspector_reachable() =>
                    {
                        PaneFocus::Parameters
                    }
                    _ => PaneFocus::Result,
                };
                self.result.status_line = self
                    .browser_session()
                    .map(|session| session.status_line().to_string())
                    .unwrap_or_else(|| "Browser session updated.".to_string());
                self.result.status_kind = StatusKind::Info;
            }
            Err((error, stderr)) => {
                if let Some(session) = self.browser_session.take() {
                    let _ = session.cancel();
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
        self.browser_session()?.clipboard_payload()
    }

    fn mark_plot_snapshot_dirty(&mut self) {
        if let Some(snapshot) = self.plot_workspace.snapshot.as_mut() {
            snapshot.dirty = true;
        }
    }

    fn record_plot_snapshot(&mut self, summary: ListObsSummary) {
        let generation = self.plot_workspace.next_generation;
        self.plot_workspace.next_generation += 1;
        self.plot_workspace.snapshot = Some(ListObsRunSnapshot {
            generation,
            path: summary.measurement_set.path.as_ref().map(PathBuf::from),
            options: summary.options.clone(),
            summary,
            dirty: false,
        });
        self.plot_workspace.cached_uv_coverage = None;
        self.plot_workspace.uv_error = None;
        self.plot_workspace.panel = None;
        self.sync_plot_export_path_for_selected_plot();
    }

    fn sync_plot_export_path_for_selected_plot(&mut self) {
        self.plot_workspace.export_path = default_plot_export_path(
            self.plot_workspace.selected_plot,
            ListObsPlotExportFormat::Png,
        );
    }

    fn clear_plot_render_cache(&mut self) {
        self.plot_workspace.panel = None;
    }

    fn current_plot_summary(&self) -> Option<String> {
        let snapshot = self.plot_workspace.snapshot.as_ref()?;
        let dirty_suffix = if snapshot.dirty {
            " Pending form edits will not affect plots until you rerun listobs."
        } else {
            ""
        };
        Some(format!(
            "{} from run {}.{}",
            self.plot_workspace.selected_plot.display_name(),
            snapshot.generation,
            dirty_suffix
        ))
    }

    fn selected_plot_spec(&self) -> &ListObsPlotSpec {
        match self.plot_workspace.selected_plot {
            ListObsPlotKind::UvCoverage => &self.plot_workspace.uv_spec,
            ListObsPlotKind::AntennaLayout => &self.plot_workspace.antenna_spec,
            ListObsPlotKind::ScanTimeline => &self.plot_workspace.scan_spec,
            ListObsPlotKind::SpectralWindowCoverage => &self.plot_workspace.spw_spec,
        }
    }

    fn selected_plot_spec_mut(&mut self) -> &mut ListObsPlotSpec {
        match self.plot_workspace.selected_plot {
            ListObsPlotKind::UvCoverage => &mut self.plot_workspace.uv_spec,
            ListObsPlotKind::AntennaLayout => &mut self.plot_workspace.antenna_spec,
            ListObsPlotKind::ScanTimeline => &mut self.plot_workspace.scan_spec,
            ListObsPlotKind::SpectralWindowCoverage => &mut self.plot_workspace.spw_spec,
        }
    }

    fn current_uv_coverage(
        &mut self,
        snapshot: &ListObsRunSnapshot,
    ) -> Result<ListObsUvCoverage, String> {
        if let Some((generation, coverage)) = self.plot_workspace.cached_uv_coverage.as_ref()
            && *generation == snapshot.generation
        {
            return Ok(coverage.clone());
        }
        let Some(path) = snapshot.path.as_ref() else {
            return Err(
                "UV coverage requires a disk-backed MeasurementSet from the last successful run."
                    .to_string(),
            );
        };
        match ListObsUvCoverage::from_path_with_options(path, &snapshot.options) {
            Ok(coverage) => {
                self.plot_workspace.cached_uv_coverage =
                    Some((snapshot.generation, coverage.clone()));
                self.plot_workspace.uv_error = None;
                Ok(coverage)
            }
            Err(error) => {
                let error = error.to_string();
                self.plot_workspace.uv_error = Some(error.clone());
                Err(error)
            }
        }
    }

    fn current_plot_payload(&mut self) -> Result<ListObsPlotPayload, String> {
        let Some(snapshot) = self.plot_workspace.snapshot.clone() else {
            return Err("Run listobs to populate the plot workspace.".to_string());
        };
        match self.plot_workspace.selected_plot {
            ListObsPlotKind::UvCoverage => {
                let coverage = self.current_uv_coverage(&snapshot)?;
                build_listobs_uv_plot_payload(&coverage, self.selected_plot_spec())
            }
            _ => build_listobs_plot_payload_from_summary(
                &snapshot.summary,
                self.selected_plot_spec(),
            ),
        }
    }

    fn pump_plot_panel(&mut self) {
        let Some(panel) = self.plot_workspace.panel.as_mut() else {
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
                self.result.status_line = "Plot rendering failed.".to_string();
                self.result.status_kind = StatusKind::Warning;
            }
        }
    }

    fn ensure_plot_requested(&mut self, layout: &UiLayout) {
        if self.active_result_tab != ResultTab::Plots {
            return;
        }
        let Some(area) = crate::ui::plot_canvas_area(layout) else {
            return;
        };
        if area.is_empty() {
            return;
        }
        let payload = match self.current_plot_payload() {
            Ok(payload) => payload,
            Err(error) => {
                self.result.status_line = "Plot payload unavailable.".to_string();
                self.result.status_kind = StatusKind::Warning;
                self.result.stderr = format!("{error}\n");
                return;
            }
        };
        let spec_key = self.selected_plot_spec().cli_assignments().join(";");
        let snapshot_generation = self
            .plot_workspace
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.generation)
            .unwrap_or_default();
        let uv_sample_count = self
            .plot_workspace
            .cached_uv_coverage
            .as_ref()
            .map(|(_, coverage)| coverage.sample_count)
            .unwrap_or(0);
        let theme_mode = self.theme_mode();

        let panel = self.plot_workspace.panel.get_or_insert_with(|| {
            let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
            let font_size = picker.font_size();
            let renderer = PanelRenderer::new(picker, Resize::Fit(None), |job| {
                render_plot_image(job.max_pixel_width, job.max_pixel_height, &job.input)
            })
            .expect("panel renderer");
            PlotPanelState {
                renderer,
                font_size,
                request_key: None,
                last_error: None,
                image_size: None,
            }
        });

        let request_key = PlotRequestKey {
            area,
            theme_mode,
            snapshot_generation,
            plot_kind: self.plot_workspace.selected_plot,
            spec_key,
            uv_sample_count,
        };
        if panel.request_key == Some(request_key.clone()) {
            return;
        }

        let pixel_width = u32::from(area.width.max(1)) * u32::from(panel.font_size.0.max(1));
        let pixel_height = u32::from(area.height.max(1)) * u32::from(panel.font_size.1.max(1));
        if let Err(error) = panel.renderer.request(
            area,
            pixel_width.max(1),
            pixel_height.max(1),
            ListObsPlotRenderInput {
                payload,
                theme_mode,
                terminal_cell_px: panel.font_size,
            },
        ) {
            panel.last_error = Some(error.to_string());
            self.result.status_line = "Failed to queue plot render.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        panel.request_key = Some(request_key);
    }

    pub(crate) fn plot_protocol(&self) -> Option<&PanelProtocol> {
        self.plot_workspace
            .panel
            .as_ref()
            .and_then(|panel| panel.renderer.protocol())
    }

    pub(crate) fn plot_pending(&self) -> bool {
        self.plot_workspace
            .panel
            .as_ref()
            .is_some_and(|panel| panel.renderer.is_pending())
    }

    pub(crate) fn plot_last_error(&self) -> Option<&str> {
        self.plot_workspace
            .panel
            .as_ref()
            .and_then(|panel| panel.last_error.as_deref())
    }

    pub(crate) fn plot_focus(&self) -> PlotPaneFocus {
        self.plot_workspace.focus
    }

    pub(crate) fn selected_plot_kind(&self) -> ListObsPlotKind {
        self.plot_workspace.selected_plot
    }

    pub(crate) fn plot_catalog_rows(&self) -> Vec<PlotCatalogRowView> {
        ListObsPlotKind::ALL
            .into_iter()
            .map(|kind| PlotCatalogRowView {
                kind,
                label: kind.display_name().to_string(),
                selected: kind == self.plot_workspace.selected_plot,
            })
            .collect()
    }

    pub(crate) fn plot_control_rows(&self) -> Vec<PlotControlRowView> {
        let spec = self.selected_plot_spec();
        let mut rows = plot_choice_descriptors(self.plot_workspace.selected_plot)
            .iter()
            .map(|descriptor| PlotControlRowView {
                target: PlotControlTarget::Option(descriptor.key),
                text: format!(
                    "{:<16} {}",
                    descriptor.label,
                    spec.option(descriptor.key).unwrap_or("<unset>")
                ),
                selected: false,
            })
            .collect::<Vec<_>>();
        rows.push(PlotControlRowView {
            target: PlotControlTarget::ExportPath,
            text: format!("{:<16} {}", "Export path", self.plot_workspace.export_path),
            selected: false,
        });
        rows.push(PlotControlRowView {
            target: PlotControlTarget::ExportWidth,
            text: format!(
                "{:<16} {}",
                "Export width", self.plot_workspace.export_width
            ),
            selected: false,
        });
        rows.push(PlotControlRowView {
            target: PlotControlTarget::ExportHeight,
            text: format!(
                "{:<16} {}",
                "Export height", self.plot_workspace.export_height
            ),
            selected: false,
        });
        for (target, label) in [
            (PlotControlTarget::Refresh, "Refresh"),
            (PlotControlTarget::ResetControls, "Reset Controls"),
            (PlotControlTarget::CopyCli, "Copy CLI"),
            (PlotControlTarget::ExportPng, "Export PNG"),
            (PlotControlTarget::ExportPdf, "Export PDF"),
        ] {
            rows.push(PlotControlRowView {
                target,
                text: label.to_string(),
                selected: false,
            });
        }
        if let Some(row) = rows.get_mut(self.plot_workspace.selected_control) {
            row.selected = true;
        }
        rows
    }

    pub(crate) fn plot_dirty_banner(&self) -> Option<&'static str> {
        self.plot_workspace.snapshot.as_ref().and_then(|snapshot| {
            snapshot
                .dirty
                .then_some("Plots reflect the last successful run. Re-run to apply form changes.")
        })
    }

    fn scroll_active_plot_workspace(&mut self, delta: i16) {
        match self.plot_workspace.focus {
            PlotPaneFocus::Catalog => {
                let all = ListObsPlotKind::ALL;
                let current = all
                    .iter()
                    .position(|kind| *kind == self.plot_workspace.selected_plot)
                    .unwrap_or(0) as i16;
                let next = (current + delta).clamp(0, all.len() as i16 - 1) as usize;
                self.set_selected_plot(all[next]);
            }
            PlotPaneFocus::Controls => {
                let row_count = self.plot_control_rows().len() as i16;
                let next = (self.plot_workspace.selected_control as i16 + delta)
                    .clamp(0, row_count.saturating_sub(1));
                self.plot_workspace.selected_control = next as usize;
            }
            PlotPaneFocus::Canvas => {}
        }
    }

    fn adjust_selected_plot_control(&mut self, forward: bool) {
        let Some(target) = self
            .plot_control_rows()
            .get(self.plot_workspace.selected_control)
            .map(|row| row.target)
        else {
            return;
        };
        if let PlotControlTarget::Option(key) = target {
            let Some(descriptor) = plot_choice_descriptors(self.plot_workspace.selected_plot)
                .iter()
                .find(|descriptor| descriptor.key == key)
            else {
                return;
            };
            let spec = self.selected_plot_spec_mut();
            let current = spec.option(key).unwrap_or(descriptor.choices[0]);
            let position = descriptor
                .choices
                .iter()
                .position(|choice| *choice == current)
                .unwrap_or(0);
            let next = if forward {
                (position + 1) % descriptor.choices.len()
            } else if position == 0 {
                descriptor.choices.len() - 1
            } else {
                position - 1
            };
            if spec.set_option(key, descriptor.choices[next]).is_ok() {
                self.clear_plot_render_cache();
            }
        }
    }

    fn activate_plot_workspace_selection(&mut self) {
        match self.plot_workspace.focus {
            PlotPaneFocus::Catalog => {}
            PlotPaneFocus::Canvas => {}
            PlotPaneFocus::Controls => {
                let Some(target) = self
                    .plot_control_rows()
                    .get(self.plot_workspace.selected_control)
                    .map(|row| row.target)
                else {
                    return;
                };
                match target {
                    PlotControlTarget::Option(_) => self.adjust_selected_plot_control(true),
                    PlotControlTarget::ExportPath => {
                        self.edit_state = Some(EditState {
                            target: EditTarget::PlotExportPath,
                            buffer: self.plot_workspace.export_path.clone(),
                        });
                    }
                    PlotControlTarget::ExportWidth => {
                        self.edit_state = Some(EditState {
                            target: EditTarget::PlotExportWidth,
                            buffer: self.plot_workspace.export_width.to_string(),
                        });
                    }
                    PlotControlTarget::ExportHeight => {
                        self.edit_state = Some(EditState {
                            target: EditTarget::PlotExportHeight,
                            buffer: self.plot_workspace.export_height.to_string(),
                        });
                    }
                    PlotControlTarget::Refresh => {
                        self.plot_workspace.cached_uv_coverage = None;
                        self.plot_workspace.uv_error = None;
                        self.clear_plot_render_cache();
                    }
                    PlotControlTarget::ResetControls => {
                        let selected_plot = self.plot_workspace.selected_plot;
                        *self.selected_plot_spec_mut() = ListObsPlotSpec::new(selected_plot);
                        self.clear_plot_render_cache();
                    }
                    PlotControlTarget::CopyCli => self.copy_current_plot_cli(),
                    PlotControlTarget::ExportPng => {
                        self.export_current_plot(ListObsPlotExportFormat::Png)
                    }
                    PlotControlTarget::ExportPdf => {
                        self.export_current_plot(ListObsPlotExportFormat::Pdf)
                    }
                }
            }
        }
    }

    fn commit_plot_or_field_edit(&mut self, edit_state: EditState) {
        match edit_state.target {
            EditTarget::FormField(field_index) => {
                if let Some(field) = self.fields.get_mut(field_index) {
                    field.set_text(edit_state.buffer);
                    self.mark_plot_snapshot_dirty();
                }
            }
            EditTarget::PlotExportPath => {
                self.plot_workspace.export_path = edit_state.buffer.trim().to_string();
            }
            EditTarget::PlotExportWidth => match edit_state.buffer.trim().parse::<u32>() {
                Ok(value) if value > 0 => self.plot_workspace.export_width = value,
                _ => {
                    self.result.status_line = "Plot width must be a positive integer.".to_string();
                    self.result.status_kind = StatusKind::Error;
                }
            },
            EditTarget::PlotExportHeight => match edit_state.buffer.trim().parse::<u32>() {
                Ok(value) if value > 0 => self.plot_workspace.export_height = value,
                _ => {
                    self.result.status_line = "Plot height must be a positive integer.".to_string();
                    self.result.status_kind = StatusKind::Error;
                }
            },
        }
    }

    fn copy_current_plot_cli(&mut self) {
        let Some(snapshot) = self.plot_workspace.snapshot.as_ref() else {
            self.result.status_line = "Run listobs before copying a plot CLI.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        let Some(path) = snapshot.path.as_ref() else {
            self.result.status_line =
                "The current plot is not backed by a disk MeasurementSet.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        let cli = self.build_current_plot_cli(path, ListObsPlotExportFormat::Png);
        self.copy_text_to_clipboard(cli, "plot CLI");
    }

    fn export_current_plot(&mut self, format: ListObsPlotExportFormat) {
        let payload = match self.current_plot_payload() {
            Ok(payload) => payload,
            Err(error) => {
                self.result.status_line = "Plot export failed.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
                return;
            }
        };
        let output_path = current_plot_output_path(
            &self.plot_workspace.export_path,
            self.plot_workspace.selected_plot,
            format,
        );
        match export_listobs_plot(
            &payload,
            plot_theme(self.theme_mode()),
            &output_path,
            format,
            self.plot_workspace.export_width,
            self.plot_workspace.export_height,
        ) {
            Ok(()) => {
                self.result.status_line = format!("Saved {}.", output_path.display());
                self.result.status_kind = StatusKind::Ok;
                self.plot_workspace.export_path = output_path.display().to_string();
            }
            Err(error) => {
                self.result.status_line = "Plot export failed.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
            }
        }
    }

    fn build_current_plot_cli(&self, path: &Path, format: ListObsPlotExportFormat) -> String {
        let mut args = Vec::<String>::new();
        let options = self
            .plot_workspace
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.options.clone())
            .unwrap_or_default();
        if !options.verbose {
            args.push("--no-verbose".to_string());
        }
        if !options.selectdata {
            args.push("--no-selectdata".to_string());
        }
        for (flag, value) in [
            ("--field", options.field.as_deref()),
            ("--spw", options.spw.as_deref()),
            ("--antenna", options.antenna.as_deref()),
            ("--scan", options.scan.as_deref()),
            ("--observation", options.observation.as_deref()),
            ("--array", options.array.as_deref()),
            ("--timerange", options.timerange.as_deref()),
            ("--uvrange", options.uvrange.as_deref()),
            ("--correlation", options.correlation.as_deref()),
            ("--intent", options.intent.as_deref()),
            ("--feed", options.feed.as_deref()),
        ] {
            if let Some(value) = value {
                args.push(flag.to_string());
                args.push(shell_quote(value));
            }
        }
        if options.listunfl {
            args.push("--listunfl".to_string());
        }
        if let Some(value) = options.cachesize_mb {
            args.push("--cachesize".to_string());
            args.push(value.to_string());
        }
        args.push("--plot".to_string());
        args.push(self.plot_workspace.selected_plot.as_str().to_string());
        for assignment in self.selected_plot_spec().cli_assignments() {
            args.push("--plot-option".to_string());
            args.push(shell_quote(&assignment));
        }
        args.push("--plot-output".to_string());
        args.push(shell_quote(
            &current_plot_output_path(
                &self.plot_workspace.export_path,
                self.plot_workspace.selected_plot,
                format,
            )
            .display()
            .to_string(),
        ));
        args.push("--plot-format".to_string());
        args.push(format.extension().to_string());
        args.push("--plot-width".to_string());
        args.push(self.plot_workspace.export_width.to_string());
        args.push("--plot-height".to_string());
        args.push(self.plot_workspace.export_height.to_string());
        args.push(shell_quote(&path.display().to_string()));
        format!("listobs {}", args.join(" "))
    }

    fn set_selected_plot(&mut self, kind: ListObsPlotKind) {
        if self.plot_workspace.selected_plot == kind {
            return;
        }
        self.plot_workspace.selected_plot = kind;
        self.plot_workspace.selected_control = 0;
        self.plot_workspace.focus = PlotPaneFocus::Catalog;
        self.clear_plot_render_cache();
        self.sync_plot_export_path_for_selected_plot();
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
        self.commit_plot_or_field_edit(edit_state);
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
            ResultTab::Plots,
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
            let _ = session.cancel();
            self.result.status_line = "Browser session closed.".to_string();
            self.result.status_kind = StatusKind::Info;
            self.pane_focus = PaneFocus::Parameters;
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
                self.result.structured = None;
                self.result.structured_error = None;
                self.result.file_output_path = Some(path);
                self.active_result_tab = ResultTab::Overview;
                return;
            }

            if matches!(running.renderer.as_deref(), Some("listobs-summary-v1")) {
                match serde_json::from_str::<ListObsSummary>(&self.result.stdout) {
                    Ok(summary) => {
                        self.record_plot_snapshot(summary.clone());
                        self.result.structured = Some(summary);
                        self.result.structured_error = None;
                        self.activate_result_tab(ResultTab::Overview);
                    }
                    Err(error) => {
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
            let tab_labels = self
                .visible_result_tabs()
                .iter()
                .map(|tab| tab.label())
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!(
                "Use [ and ] or click the tab strip to switch between {tab_labels}."
            ));
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
            return session.main_content_lines();
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
            (_, Some(edit_state)) if edit_state.target == EditTarget::FormField(field_index) => {
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
    if app.browser_is_active()
        && !app.browser_uses_parameter_pane()
        && layout.form_inner.width > 0
        && layout.form_inner.height > 0
    {
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

fn image_browser_visible_line(
    raw_line: &str,
    plane_view: bool,
    offset: usize,
    width: usize,
) -> VisibleTextLine {
    let line = if plane_view {
        image_plane_visible_line(raw_line)
    } else {
        VisibleTextLine::plain(raw_line.to_string())
    };
    slice_visible_line(&line, offset, width)
}

fn image_plane_visible_line(raw_line: &str) -> VisibleTextLine {
    let mut text = String::new();
    let mut roles = Vec::new();
    let mut in_selected_cell = false;

    for ch in raw_line.chars() {
        if ch == '[' {
            in_selected_cell = true;
        }
        let role = if in_selected_cell {
            VisibleTextRole::BrowserSelectedCell
        } else {
            VisibleTextRole::Plain
        };
        text.push(ch);
        roles.push(role);
        if ch == ']' {
            in_selected_cell = false;
        }
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

fn slice_visible_line(line: &VisibleTextLine, offset: usize, width: usize) -> VisibleTextLine {
    if width == 0 {
        return VisibleTextLine::plain(String::new());
    }

    let chars = line.text.chars().collect::<Vec<_>>();
    if offset >= chars.len() {
        return VisibleTextLine::plain(String::new());
    }

    let remaining = chars.len() - offset;
    if remaining <= width {
        return VisibleTextLine {
            text: chars[offset..].iter().collect(),
            roles: line.roles[offset..].to_vec(),
        };
    }

    if width <= 3 {
        return VisibleTextLine::plain(".".repeat(width));
    }

    let visible_len = width - 3;
    let mut text = chars[offset..offset + visible_len]
        .iter()
        .collect::<String>();
    text.push_str("...");
    let mut roles = line.roles[offset..offset + visible_len].to_vec();
    roles.extend(std::iter::repeat_n(VisibleTextRole::Plain, 3));
    VisibleTextLine { text, roles }
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

fn image_browser_content_width(snapshot: &ImageBrowserSnapshot) -> usize {
    snapshot
        .content_lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0)
}

fn image_plane_column_count(snapshot: &ImageBrowserSnapshot) -> Option<usize> {
    let header = snapshot.content_lines.first()?;
    let pipe_index = header.find('|')?;
    let right_width = header.get(pipe_index + 1..)?.chars().count();
    let stride = IMAGE_PLANE_CELL_WIDTH + 1;
    Some(right_width / stride)
}

fn image_plane_selected_pixel(snapshot: &ImageBrowserSnapshot) -> Option<(usize, usize)> {
    let header = snapshot.content_lines.first()?;
    let pipe_index = header.find('|')?;
    let stride = IMAGE_PLANE_CELL_WIDTH + 1;

    for line in snapshot.content_lines.iter().skip(1) {
        let Some(selected_index) = line.find('[') else {
            continue;
        };
        let y = line[..pipe_index].trim().parse::<usize>().ok()?;
        let after_pipe = selected_index.checked_sub(pipe_index + 1)?;
        let x = after_pipe.checked_sub(1)? / stride;
        return Some((x, y));
    }

    None
}

fn image_browser_max_hscroll(snapshot: &ImageBrowserSnapshot, viewport_width: u16) -> usize {
    let viewport_width = usize::from(viewport_width);
    image_browser_content_width(snapshot).saturating_sub(viewport_width)
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

fn render_image_probe(probe: &casacore_imagebrowser_protocol::ImageBrowserProbe) -> String {
    let mut lines = vec![
        format!("value: {}", probe.value),
        format!(
            "pixel: {}",
            probe
                .pixel_indices
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ];
    if probe.masked {
        lines.push("masked: true".to_string());
    }
    if !probe.finite {
        lines.push("finite: false".to_string());
    }
    for axis in &probe.world_axes {
        lines.push(format!(
            "{}: {}",
            axis.name,
            format_world_axis_probe_value(&axis.name, &axis.unit, axis.value)
        ));
    }
    lines.join("\n")
}

fn format_world_axis_probe_value(axis_name: &str, unit: &str, value: f64) -> String {
    if axis_name.eq_ignore_ascii_case("Right Ascension") || axis_name.eq_ignore_ascii_case("RA") {
        return format_right_ascension_labeled(value, 6);
    }
    if axis_name.eq_ignore_ascii_case("Declination") || axis_name.eq_ignore_ascii_case("DEC") {
        return format_declination_labeled(value, 5);
    }
    if unit.is_empty() {
        format!("{value} unitless")
    } else {
        format!("{value} {unit}")
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
            Some(ResultAction::ScrollHorizontal(-HORIZONTAL_SCROLL_STEP))
        }
        KeyCode::Right | KeyCode::Char('l')
            if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
        {
            Some(ResultAction::ScrollHorizontal(HORIZONTAL_SCROLL_STEP))
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
        KeyCode::Left if key_event.modifiers == KeyModifiers::CONTROL => {
            Some(ResultAction::ScrollHorizontal(-HORIZONTAL_SCROLL_STEP))
        }
        KeyCode::Right if key_event.modifiers == KeyModifiers::CONTROL => {
            Some(ResultAction::ScrollHorizontal(HORIZONTAL_SCROLL_STEP))
        }
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

#[derive(Debug, Clone, Copy)]
struct PlotChoiceDescriptor {
    key: &'static str,
    label: &'static str,
    choices: &'static [&'static str],
}

const UV_PLOT_CONTROLS: [PlotChoiceDescriptor; 3] = [
    PlotChoiceDescriptor {
        key: "draw_mode",
        label: "Draw mode",
        choices: &["tracks", "points"],
    },
    PlotChoiceDescriptor {
        key: "mirror",
        label: "Mirror",
        choices: &["on", "off"],
    },
    PlotChoiceDescriptor {
        key: "axis_extent",
        label: "Axis extent",
        choices: &["auto", "1000", "10000", "100000"],
    },
];

const ANTENNA_PLOT_CONTROLS: [PlotChoiceDescriptor; 3] = [
    PlotChoiceDescriptor {
        key: "labels",
        label: "Labels",
        choices: &["off", "id", "name"],
    },
    PlotChoiceDescriptor {
        key: "coordinates",
        label: "Coordinates",
        choices: &["offset", "absolute"],
    },
    PlotChoiceDescriptor {
        key: "size_by_diameter",
        label: "Size by dish",
        choices: &["on", "off"],
    },
];

const SCAN_PLOT_CONTROLS: [PlotChoiceDescriptor; 3] = [
    PlotChoiceDescriptor {
        key: "lanes",
        label: "Lane grouping",
        choices: &["scan", "field"],
    },
    PlotChoiceDescriptor {
        key: "color_by",
        label: "Color by",
        choices: &["field", "intent"],
    },
    PlotChoiceDescriptor {
        key: "labels",
        label: "Labels",
        choices: &["none", "scan", "field"],
    },
];

const SPW_PLOT_CONTROLS: [PlotChoiceDescriptor; 3] = [
    PlotChoiceDescriptor {
        key: "unit",
        label: "X unit",
        choices: &["ghz", "mhz"],
    },
    PlotChoiceDescriptor {
        key: "labels",
        label: "Labels",
        choices: &["on", "off"],
    },
    PlotChoiceDescriptor {
        key: "color_by",
        label: "Color by",
        choices: &["spw", "polarization"],
    },
];

fn plot_choice_descriptors(kind: ListObsPlotKind) -> &'static [PlotChoiceDescriptor] {
    match kind {
        ListObsPlotKind::UvCoverage => &UV_PLOT_CONTROLS,
        ListObsPlotKind::AntennaLayout => &ANTENNA_PLOT_CONTROLS,
        ListObsPlotKind::ScanTimeline => &SCAN_PLOT_CONTROLS,
        ListObsPlotKind::SpectralWindowCoverage => &SPW_PLOT_CONTROLS,
    }
}

fn default_plot_export_path(kind: ListObsPlotKind, format: ListObsPlotExportFormat) -> String {
    format!("{}.{}", kind.as_str(), format.extension())
}

fn current_plot_output_path(
    configured_path: &str,
    kind: ListObsPlotKind,
    format: ListObsPlotExportFormat,
) -> PathBuf {
    let trimmed = configured_path.trim();
    let mut path = if trimmed.is_empty() {
        PathBuf::from(default_plot_export_path(kind, format))
    } else {
        PathBuf::from(trimmed)
    };
    path.set_extension(format.extension());
    path
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        "''".to_string()
    } else if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "/._-:=~".contains(character))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

#[cfg(test)]
mod tests {
    use super::expand_tilde_path_with_home;
    use std::path::{Path, PathBuf};

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
}
