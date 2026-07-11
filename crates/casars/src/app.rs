// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "app/browser_manager.rs"]
mod browser_manager;
pub(crate) use browser_manager::BrowserManagerRowView;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use casa_calibration::{
    CalibrationPlotPreset, CalibrationPlotRequest, ManagedCalibrationOutput,
    build_calibration_plot_payload, load_apply_specs_from_callib, save_apply_specs_to_callib,
};
use casa_images::{
    ImageMovieBundleEngine, ImageMovieBundleRequest, ImageMovieOccurrence,
    ImageMoviePreparedBundle, ImageMoviePresentationCoordinator, ImageMoviePresentationPoll,
    ImageMovieRenderedBundle, ImageMovieSurfaceKind, ImageMovieSurfaceRequest,
};
use casa_ms::msexplore::cli::build_explore_spec_from_args;
use casa_ms::ui_schema::{UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiValueKind};
use casa_ms::{
    MeasurementSet, MeasurementSetSummary, MeasurementSetSummaryOptions, MsExportFormat,
    MsPlotPayload, MsPlotPreset, MsSelectionSpec, build_msexplore_payload_from_spec,
    export_msexplore_plot,
};
use casa_provider_contracts::{ParameterValue, SurfaceKind, builtin_surface_bundle};
use casa_task_runtime::{
    BaseSource, ManagedProfileKind, ManagedStateStore, ParameterProfile, ParameterSession,
    SessionLastState, TaskLastState, parse_profile, provider_parameter_applies,
    write_parameter_profile_atomic,
};
use casa_types::measures::direction::{
    angular_increment_arcseconds, format_declination_labeled, format_right_ascension_labeled,
};
use casa_types::quanta::{MvAngle, MvTime, Quantity, Unit};
use casars_imagebrowser_protocol::{
    ImageBackendPlaneCacheResult, ImageBackendTimingState, ImageBrowserCommand, ImageBrowserFocus,
    ImageBrowserParameters, ImageBrowserPreviewRequest, ImageBrowserProbe, ImageBrowserSnapshot,
    ImageBrowserView, ImageBrowserViewport, ImageDisplayAxisState,
    ImageMaskReference as ProtocolImageMaskReference, ImagePlaneContentMode, ImageProfilePayload,
    ImageRegionReference as ProtocolImageRegionReference,
};
use casars_imager::{ManagedImagingOutput, ManagedImagingStageTimings};
use casars_tablebrowser_protocol::{
    BrowserBookmark, BrowserCommand, BrowserComplex32Value, BrowserComplex64Value,
    BrowserContentMode, BrowserFocus, BrowserInspectorSnapshot, BrowserParameters,
    BrowserScalarValue, BrowserSnapshot, BrowserValueNode, BrowserView as TableBrowserView,
    BrowserViewport,
};
use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use image::{DynamicImage, RgbImage, RgbaImage};
use ratatui::layout::Rect;
use ratatui_explorer::{FileExplorer, FileExplorerBuilder, Input as ExplorerInput};
use ratatui_graphics::{
    PanelProtocol, PanelRenderPool, PanelRenderer, Picker, Resize,
    build_panel_protocol_from_rgba_owned,
};

use crate::browser_client::{BrowserClient, ImageBrowserClient};
use crate::calibration_workflow::{
    WorkflowCalibrationArtifactKind, WorkflowChainEntryRecord, WorkflowChainEntrySource,
    WorkflowChainSettingKind, WorkflowChainSettingRecord, WorkflowContextSettingKind,
    WorkflowProductActionKind, WorkflowProductRecord, WorkflowStageGuideKind, WorkflowStageId,
    calibration_stage_specs, parse_workflow_calwt_value, parse_workflow_gainfield_value,
    parse_workflow_interp_value, parse_workflow_spwmap_value,
    preferred_workflow_calibration_preset, suggested_output_table_path,
    workflow_calibration_catalog_entries, workflow_callib_apply_to_row,
    workflow_callib_setting_display_value, workflow_callib_setting_raw_value,
    workflow_chain_entries, workflow_preferred_diagnostic_preset_for_stage,
    workflow_product_metadata_from_report, workflow_products_display_groups,
    workflow_stage_action_label, workflow_stage_from_report, workflow_stage_goal,
    workflow_stage_hint, workflow_stage_output, workflow_stage_states,
};
use crate::clipboard;
use crate::config::{ConfigStore, ThemeMode};
use crate::execution::{ExecutionEvent, ExecutionPlan, RunningProcess, spawn_process};
use crate::graphics::{
    BrowserRenderTheme, ImagePlaneColormap, ImagePlaneOverlayMarker, ImagePlaneRenderInput,
    ImageSpectrumOverlaySeries, ImageSpectrumRenderInput, ImagingPlotPayload,
    ImagingPlotRenderInput, ImagingPlotSeries, MsExplorePlotRenderInput, PlotRenderInput,
    image_plane_layout, image_spectrum_layout, plot_theme, render_image_plane_image,
    render_image_spectrum_image, render_plot_image,
};
use crate::imaging_workflow::{
    ImagingDiagnosticKind, imaging_catalog_entries, imaging_preferred_diagnostic,
    imaging_products_display_groups,
};
use crate::movie_perf::{
    BackendTimingBreakdown, MovieFrameOutcome, MoviePerfContext, MoviePerfTracer,
    MoviePipelineState,
};
use crate::notebook_recording::NotebookRecording;
use crate::registry::{AppShellKind, BrowserAppKind, RegistryApp};
use crate::shell::{
    BrowserOverviewDisplay, InspectOverviewDisplay, render_browser_overview_lines,
    render_inspect_overview_lines,
};
use crate::terminal_picker;
use crate::ui::UiLayout;
use crate::workflow::{
    WorkflowArtifactGroupDisplay, WorkflowDetailDisplay, WorkflowDiagnosticSummaryDisplay,
    WorkflowOverviewDisplay, WorkflowProductRowDisplay, WorkflowProductSnapshot,
    WorkflowProductStatus, WorkflowRunSnapshot, WorkflowStageDisplay, WorkflowStageState,
    WorkflowValueDisplay, render_workflow_artifact_groups, render_workflow_detail_display,
    render_workflow_diagnostic_summary, render_workflow_overview_lines,
    render_workflow_product_row_display, render_workflow_stage_display,
    render_workflow_value_display, stale_descendant_product_indices,
};
use casa_notebook::ExecutionStatus as NotebookExecutionStatus;

const DENSE_SPINNER_FRAMES: &[&str] = &["|", "/", "-", "\\"];
const RICH_SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠴", "⠦"];
const DOUBLE_CLICK_WINDOW: Duration = Duration::from_millis(450);
const HORIZONTAL_SCROLL_STEP: i16 = 8;
const IMAGE_PLANE_CELL_WIDTH: usize = 11;
const IMAGE_MOVIE_DEFAULT_FPS: f64 = 1.0;
const IMAGE_PLANE_RENDER_CACHE_CAPACITY: usize = 32;
const IMAGE_SPECTRUM_RENDER_CACHE_CAPACITY: usize = 64;
const IMAGE_MOVIE_TARGET_RESIDENT_BYTES: u64 = 192 * 1024 * 1024;
const IMAGE_MOVIE_MIN_RENDER_SCALE: f32 = 0.35;
const IMAGE_MOVIE_RENDER_POOL_QUEUE_CAPACITY: usize = 12;
const IMAGE_MOVIE_PROTOCOL_POOL_QUEUE_CAPACITY: usize = 8;
const IMAGE_PLANE_TARGET_PIXELS_PER_SAMPLE: u32 = 2;
const IMAGE_PLANE_MIN_TARGET_IMAGE_PIXELS: u32 = 160;
const IMAGE_SPECTRUM_TARGET_PIXELS_PER_SAMPLE: u32 = 12;
const IMAGE_SPECTRUM_MIN_TARGET_PLOT_WIDTH: u32 = 256;
const IMAGE_SPECTRUM_TARGET_PLOT_HEIGHT: u32 = 192;
const IMEXPLORE_LIVE_PARAMETER_FIELD_IDS: [&str; 17] = [
    "image",
    "blc",
    "trc",
    "inc",
    "stretch",
    "autoscale",
    "clip_low",
    "clip_high",
    "fps",
    "view",
    "contentmode",
    "colormap",
    "movieaxis",
    "profileaxis",
    "loop",
    "region",
    "mask",
];
const RESULT_TAB_COUNT: usize = 17;
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
    ParameterProfilePath,
    ParameterSourceConfirmation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParameterAction {
    SelectPrevious,
    SelectNext,
    ChoicePrevious,
    ChoiceNext,
    Activate,
    PromoteWorkflowProduct,
    Delete,
    MoveUp,
    MoveDown,
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
    TogglePlaneMode,
    ToggleSpectrumPane,
    ToggleMovie,
    ZoomIn,
    ZoomOut,
    ResetViewWindow,
    PanLeft,
    PanRight,
    PanUp,
    PanDown,
    CycleColormap,
    ToggleInvert,
    StartRegionShape,
    ClearRegion,
    SaveRegionDefinition,
    LoadNextRegionDefinition,
    RenameRegionDefinition,
    DeleteRegionDefinition,
    WriteRegionMask,
    PinProbe,
    RemovePinnedProbe,
    CyclePinnedProbe { forward: bool },
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    AppendImageRegionVertex {
        x: usize,
        y: usize,
    },
    StepImageNonDisplayAxis {
        axis: usize,
        delta: i32,
    },
    StartImageRegionShape,
    CloseImageRegionShape,
    UndoImageRegionVertex,
    CancelImageRegionShape,
    ClearImageRegion,
    SaveImageRegionDefinition,
    LoadNextImageRegionDefinition,
    LoadImageRegionDefinition {
        name: String,
    },
    RenameImageRegionDefinition {
        name: String,
        new_name: String,
    },
    DeleteImageRegionDefinition {
        name: String,
    },
    SetImageDefaultMask {
        name: String,
    },
    UnsetImageDefaultMask,
    DeleteImageMask {
        name: String,
    },
    WriteImageRegionMask,
    SetImagePlaneContentMode {
        mode: ImagePlaneContentMode,
    },
    SetImageViewParameters {
        parameters: ImageBrowserParameters,
    },
    SetImageProfileAxis {
        axis: usize,
    },
    SetImageSelectionReferences {
        region: Option<ProtocolImageRegionReference>,
        mask: Option<ProtocolImageMaskReference>,
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
enum ParameterProfilePathAction {
    Cancel,
    Commit,
    DeleteBackward,
    Insert(char),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParameterSourceConfirmationAction {
    Cancel,
    Confirm,
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
    ToggleNotebookBypass,
    CancelSession,
    OpenPathChooser,
    OpenParameterSources,
    LoadParameterDefaults,
    LoadParameterLast,
    RevertParameters,
    ClearSelection,
    ToggleHelp,
    Parameter(ParameterAction),
    Result(ResultAction),
    Browser(BrowserAction),
    Edit(EditAction),
    PathChooser(PathChooserAction),
    ParameterProfilePath(ParameterProfilePathAction),
    ParameterSourceConfirmation(ParameterSourceConfirmationAction),
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
    SummaryView(SummaryDataView),
    BrowserView(BrowserTab),
    WorkflowContextSetting(WorkflowContextSettingKind),
    WorkflowStageGuide(WorkflowStageGuideKind),
    WorkflowStageAction,
    WorkflowProductAction(WorkflowProductActionKind),
    WorkflowChainEntry(usize),
    WorkflowChainSetting(usize, WorkflowChainSettingKind),
    WorkflowProduct(usize),
    WorkflowStage(WorkflowStageId),
    BrowserPane(BrowserPaneSelection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BrowserPaneSelection {
    Mode(ImageBrowserLeftPaneMode),
    SavedRegion(usize),
    Mask(usize),
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ResultTab {
    Overview,
    Data,
    Structure,
    Content,
    Inspector,
    Products,
    Diagnostics,
    History,
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
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Data => "Data",
            Self::Structure => "Structure",
            Self::Content => "Content",
            Self::Inspector => "Inspector",
            Self::Products => "Products",
            Self::Diagnostics => "Diagnostics",
            Self::History => "History",
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
            Self::Data => 1,
            Self::Structure => 2,
            Self::Content => 3,
            Self::Inspector => 4,
            Self::Products => 5,
            Self::Diagnostics => 6,
            Self::History => 7,
            Self::Observations => 8,
            Self::Scans => 9,
            Self::Fields => 10,
            Self::Spws => 11,
            Self::Sources => 12,
            Self::Antennas => 13,
            Self::Plots => 14,
            Self::Stdout => 15,
            Self::Stderr => 16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SummaryDataView {
    Observations,
    Scans,
    Fields,
    Spws,
    Sources,
    Antennas,
}

impl SummaryDataView {
    const ALL: [Self; 6] = [
        Self::Observations,
        Self::Scans,
        Self::Fields,
        Self::Spws,
        Self::Sources,
        Self::Antennas,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Observations => "Observations",
            Self::Scans => "Scans",
            Self::Fields => "Fields",
            Self::Spws => "SPWs",
            Self::Sources => "Sources",
            Self::Antennas => "Antennas",
        }
    }

    fn cycle(self, forward: bool) -> Self {
        let position = Self::ALL
            .iter()
            .position(|candidate| *candidate == self)
            .unwrap_or(0);
        if forward {
            Self::ALL[(position + 1) % Self::ALL.len()]
        } else if position == 0 {
            Self::ALL[Self::ALL.len() - 1]
        } else {
            Self::ALL[position - 1]
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
    Subsection,
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
    pub target: PlotCatalogTarget,
    pub label: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlotCatalogTarget {
    Preset(MsPlotPreset),
    Calibration(CalibrationPlotPreset),
    Imaging(ImagingDiagnosticKind),
    CustomPlot,
    PageSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlotControlTarget {
    Refresh,
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
    Spectrum,
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

    pub(crate) const IMAGE_ALL: [Self; 4] = [
        Self::Metadata,
        Self::Coordinates,
        Self::Plane,
        Self::Spectrum,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Columns => "Columns",
            Self::Keywords => "Keywords",
            Self::Cells => "Cells",
            Self::Subtables => "Subtables",
            Self::Plane => "Plane",
            Self::Spectrum => "Spectrum",
            Self::Metadata => "Metadata",
            Self::Coordinates => "Coordinates",
        }
    }

    fn shell_result_tab(self) -> ResultTab {
        match self {
            Self::Overview | Self::Columns | Self::Keywords | Self::Subtables => {
                ResultTab::Structure
            }
            Self::Cells | Self::Plane | Self::Spectrum => ResultTab::Content,
            Self::Metadata | Self::Coordinates => ResultTab::Structure,
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
            ImageBrowserView::Spectrum => Self::Spectrum,
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
    parameter_session: Option<ParameterSession>,
    pending_live_parameter_rollback: Option<ParameterSession>,
    parameter_edit_errors: BTreeMap<String, String>,
    parameter_workspace: PathBuf,
    #[cfg(test)]
    _test_parameter_workspace: tempfile::TempDir,
    save_last: bool,
    session_last_state: Option<SessionLastState>,
    parameter_profile_path_entry: Option<ParameterProfilePathEntryState>,
    pending_parameter_replacement: Option<ParameterDraftReplacement>,
    sections: Vec<FormSection>,
    selected_form: FormSelection,
    show_advanced: bool,
    pane_focus: PaneFocus,
    edit_state: Option<EditState>,
    result: ResultState,
    active_result_tab: ResultTab,
    result_scrolls: [u16; RESULT_TAB_COUNT],
    result_hscrolls: [u16; RESULT_TAB_COUNT],
    selected_summary_view: SummaryDataView,
    history_entries: Vec<HistoryEntry>,
    workflow_products: Vec<WorkflowProductRecord>,
    running: Option<RunningState>,
    plot_workspace: PlotWorkspaceState,
    path_chooser: Option<PathChooserState>,
    choice_picker: Option<ChoicePickerState>,
    browser_mode_picker: Option<ImageBrowserLeftPaneMode>,
    browser_session: Option<BrowserSession>,
    spinner_frame: usize,
    dragging_divider: bool,
    dragging_image_workspace_divider: bool,
    dragging_image_cursor: bool,
    dragging_image_profile: bool,
    dragging_result_scrollbar: bool,
    dragging_result_hscrollbar: bool,
    dragging_result_hscrollbar_grab: u16,
    output_selection: Option<OutputSelection>,
    show_help: bool,
    cached_result_text_area: Option<Rect>,
    cached_left_output_area: Option<Rect>,
    cached_browser_viewport_cells: Option<(u16, u16, u16)>,
    kitty_response_capture: Option<String>,
    kitty_movie_store_invalidated: bool,
    last_click: Option<ClickState>,
    pending_run_confirmation: bool,
    notebook_bypass_once: bool,
    movie_perf: MoviePerfTracer,
    quit: bool,
    return_to_launcher: bool,
}

#[derive(Debug)]
struct RunningState {
    process: RunningProcess,
    renderer: Option<String>,
    file_output_path: Option<String>,
    cancel_requested: bool,
    task_last_state: Option<TaskLastState>,
    notebook_recording: Option<NotebookRecording>,
}

#[derive(Debug)]
struct BrowserSession {
    root_path: String,
    kind: BrowserSessionKind,
}

#[derive(Debug)]
enum BrowserSessionKind {
    Table(Box<TableBrowserSession>),
    Image(Box<ImageBrowserSessionState>),
}

#[derive(Debug)]
struct TableBrowserSession {
    client: BrowserClient,
    snapshot: BrowserSnapshot,
    viewport: BrowserViewport,
}

#[derive(Debug, Clone)]
struct TableSessionStartupConfig {
    parameters: BrowserParameters,
    requires_configure: bool,
}

#[derive(Debug, Clone)]
struct ImageSessionStartupConfig {
    view_parameters: ImageBrowserParameters,
    view: ImageBrowserView,
    enforce_view: bool,
    content_mode: ImagePlaneContentMode,
    content_mode_explicit: bool,
    colormap: ImagePlaneColormap,
    movie_axis: String,
    profile_axis: String,
    fps: f64,
    looping: bool,
    region: ProtocolImageRegionReference,
    region_explicit: bool,
    mask: ProtocolImageMaskReference,
    mask_explicit: bool,
}

#[derive(Debug)]
struct ImageBrowserSessionState {
    client: ImageBrowserClient,
    snapshot: ImageBrowserSnapshot,
    viewport: ImageBrowserViewport,
    hscroll: u16,
    left_pane_mode: ImageBrowserLeftPaneMode,
    selected_saved_region_index: usize,
    selected_mask_index: usize,
    selected_non_display_axis: usize,
    pinned_probes: Vec<ImagePinnedProbe>,
    selected_pinned_probe_id: Option<u64>,
    next_pinned_probe_id: u64,
    restoring_selected_pinned_probe: bool,
    show_live_reticle: bool,
    plane_mode: ImagePlaneMode,
    plane_colormap: ImagePlaneColormap,
    plane_invert: bool,
    panel: Option<ImagePlanePanelState>,
    spectrum_panel: Option<ImageSpectrumPanelState>,
    snapshot_generation: u64,
    movie: ImageMovieState,
    movie_scheduler: Option<ImageMovieSchedulerState>,
    movie_frame_seq: Option<u64>,
    direct_movie_engine: ImageMovieBundleEngine,
}

#[derive(Debug, Clone)]
struct ImagePinnedProbe {
    id: u64,
    label: String,
    plane_pixel: (usize, usize),
    probe: ImageBrowserProbe,
    profile: Option<ImageProfilePayload>,
    non_display_axis_indices: Vec<(usize, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImagePlaneMode {
    Raster,
    Spreadsheet,
}

impl ImagePlaneMode {
    fn label(self) -> &'static str {
        match self {
            Self::Raster => "raster",
            Self::Spreadsheet => "spreadsheet",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ImageBrowserLeftPaneMode {
    Live,
    Regions,
    Masks,
}

impl ImageBrowserLeftPaneMode {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Live => "Live",
            Self::Regions => "Regions",
            Self::Masks => "Masks",
        }
    }

    pub(crate) fn all() -> [Self; 3] {
        [Self::Live, Self::Regions, Self::Masks]
    }

    pub(crate) fn cycle(self, forward: bool) -> Self {
        let all = Self::all();
        let index = all.iter().position(|mode| *mode == self).unwrap_or(0);
        if forward {
            all[(index + 1) % all.len()]
        } else {
            all[(index + all.len() - 1) % all.len()]
        }
    }
}

#[derive(Debug)]
struct ImageMovieState {
    playing: bool,
    fps: f64,
    frame_interval: Duration,
    last_advanced_at: Option<Instant>,
    looping: bool,
    direct_overlay: bool,
    terminal_looping: bool,
}

impl Default for ImageMovieState {
    fn default() -> Self {
        Self::with_fps(IMAGE_MOVIE_DEFAULT_FPS)
    }
}

impl ImageMovieState {
    fn with_fps(fps: f64) -> Self {
        Self {
            playing: false,
            fps,
            frame_interval: Duration::from_secs_f64(1.0 / fps),
            last_advanced_at: None,
            looping: false,
            direct_overlay: false,
            terminal_looping: false,
        }
    }

    fn set_fps(&mut self, fps: f64) {
        self.fps = fps;
        self.frame_interval = Duration::from_secs_f64(1.0 / fps);
    }
}

struct ImagePlanePanelState {
    picker: Picker,
    renderer: PanelRenderer<ImagePlaneRenderInput, String>,
    render_cache: Arc<Mutex<RenderImageCache<u64>>>,
    font_size: (u16, u16),
    display_key: Option<ImagePlaneRequestKey>,
    pending_request_key: Option<ImagePlaneRequestKey>,
    last_error: Option<String>,
    image_size: Option<(u32, u32)>,
    movie_protocol: Option<PanelProtocol>,
    movie_display_key: Option<ImagePlaneRequestKey>,
    movie_image_size: Option<(u32, u32)>,
}

struct ImageSpectrumPanelState {
    picker: Picker,
    renderer: PanelRenderer<ImageSpectrumRenderInput, String>,
    font_size: (u16, u16),
    display_key: Option<ImageSpectrumRequestKey>,
    pending_request_key: Option<ImageSpectrumRequestKey>,
    last_error: Option<String>,
    image_size: Option<(u32, u32)>,
    movie_protocol: Option<PanelProtocol>,
    movie_display_key: Option<ImageSpectrumRequestKey>,
    movie_image_size: Option<(u32, u32)>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
enum DirectImageMovieSurfacePayload {
    Plane(ImagePlaneRenderInput),
    Spectrum(ImageSpectrumRenderInput),
}

#[derive(Debug, Clone)]
pub(crate) struct ImageDirectMovieSurfaceInfo {
    pub kind: ImageMovieSurfaceKind,
    pub canvas: Rect,
    pub request_hash: u64,
    pub pixel_size: (u32, u32),
}

#[derive(Debug, Clone)]
pub(crate) struct ImageDirectMovieBundleInfo {
    pub movie_key: u64,
    pub axis: usize,
    pub axis_index: usize,
    pub axis_length: usize,
    pub fps: f64,
    pub surfaces: Vec<ImageDirectMovieSurfaceInfo>,
    prepared: ImageMoviePreparedBundle<DirectImageMovieSurfacePayload>,
}

#[derive(Debug, Clone)]
pub(crate) struct ImageDirectMovieBundle {
    pub rendered: ImageMovieRenderedBundle,
    pub cache_hit: bool,
}

impl ImageDirectMovieBundleInfo {
    pub(crate) fn surface(
        &self,
        kind: ImageMovieSurfaceKind,
    ) -> Option<&ImageDirectMovieSurfaceInfo> {
        self.surfaces.iter().find(|surface| surface.kind == kind)
    }

    pub(crate) fn plane_request_hash(&self) -> Option<u64> {
        self.surface(ImageMovieSurfaceKind::Plane)
            .map(|surface| surface.request_hash)
    }

    pub(crate) fn plane_surface(&self) -> Option<&ImageDirectMovieSurfaceInfo> {
        self.surface(ImageMovieSurfaceKind::Plane)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MovieOccurrenceKey {
    generation: u64,
    movie_axis: usize,
    axis_index: usize,
}

#[derive(Debug, Clone)]
struct CurrentImageSpectrumRenderRequest {
    request_key: ImageSpectrumRequestKey,
    pixel_width: u32,
    pixel_height: u32,
    input: ImageSpectrumRenderInput,
}

#[derive(Debug, Clone)]
struct MovieProtocolRenderJob {
    bundle: PreparedMovieBundle,
    plane_picker: Picker,
    spectrum_picker: Option<Picker>,
}

#[derive(Debug, Clone)]
struct PreparedMovieBundle {
    occurrence: MovieOccurrenceKey,
    snapshot: ImageBrowserSnapshot,
    plane_request: CurrentImagePlaneRenderRequest,
    spectrum_request: Option<CurrentImageSpectrumRenderRequest>,
    rendered: ImageMovieRenderedBundle,
    cache_hit: bool,
}

struct PreparedMoviePresentation {
    occurrence: MovieOccurrenceKey,
    snapshot: ImageBrowserSnapshot,
    plane_request: CurrentImagePlaneRenderRequest,
    plane_protocol: PanelProtocol,
    plane_image_size: (u32, u32),
    spectrum_request: Option<CurrentImageSpectrumRenderRequest>,
    spectrum_protocol: Option<PanelProtocol>,
    spectrum_image_size: Option<(u32, u32)>,
}

#[derive(Debug, Clone)]
struct ImageMovieSchedulerSpec {
    content_signature: u64,
    movie_axis: usize,
    axis_length: usize,
    next_due_index: usize,
    requested_fps: f64,
    theme_mode: ThemeMode,
    split_ratio: f32,
    viewport: ImageBrowserViewport,
    snapshot: ImageBrowserSnapshot,
    parameters: ImageBrowserParameters,
    plane_content_mode: ImagePlaneContentMode,
    show_live_reticle: bool,
    plane_colormap: ImagePlaneColormap,
    plane_invert: bool,
    pinned_probes: Vec<ImagePinnedProbe>,
    plane_font_size: (u16, u16),
    spectrum_font_size: Option<(u16, u16)>,
    spectrum_visible: bool,
    session_indices: Vec<(usize, usize)>,
}

struct ImageMovieSchedulerState {
    generation: u64,
    content_signature: u64,
    movie_axis: usize,
    session_indices: Vec<(usize, usize)>,
    presentations: ImageMoviePresentationCoordinator<PreparedMoviePresentation>,
    protocol_pool: PanelRenderPool<MovieProtocolRenderJob, PreparedMoviePresentation, String>,
    queue_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ImagePlaneRequestKey {
    area: Rect,
    theme_mode: ThemeMode,
    render_signature: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ImageSpectrumRequestKey {
    area: Rect,
    theme_mode: ThemeMode,
    render_signature: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ImageDirectMovieFrameInfo {
    pub movie_key: u64,
    pub canvas: Rect,
    pub axis: usize,
    pub axis_index: usize,
    pub axis_length: usize,
    pub fps: f64,
    pub render_request_key_hash: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ImageDirectMovieFrame {
    pub movie_key: u64,
    pub canvas: Rect,
    pub axis: usize,
    pub axis_index: usize,
    pub axis_length: usize,
    pub fps: f64,
    pub render_request_key_hash: u64,
    pub rendered_image: RgbaImage,
}

#[derive(Debug, Clone)]
struct CurrentImagePlaneRenderRequest {
    request_key: ImagePlaneRequestKey,
    pixel_width: u32,
    pixel_height: u32,
    input: ImagePlaneRenderInput,
}

struct ImagePlaneRenderRequestOptions<'a> {
    show_live_reticle: bool,
    colormap: ImagePlaneColormap,
    invert: bool,
    overlay_markers: &'a [ImagePlaneOverlayMarker],
    split_ratio: f32,
    theme_mode: ThemeMode,
    render_scale: f32,
    max_pixel_size: Option<(u32, u32)>,
}

struct ImageSpectrumRenderRequestOptions<'a> {
    overlay_profiles: &'a [ImageSpectrumOverlaySeries],
    split_ratio: f32,
    theme_mode: ThemeMode,
    render_scale: f32,
    max_pixel_size: Option<(u32, u32)>,
}

#[derive(Debug)]
struct RenderImageCache<K> {
    capacity: usize,
    values: HashMap<K, RgbImage>,
    order: VecDeque<K>,
}

impl<K> RenderImageCache<K>
where
    K: Clone + Eq + Hash,
{
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            values: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<RgbImage> {
        let value = self.values.get(key).cloned()?;
        self.touch(key);
        Some(value)
    }

    fn contains_key(&self, key: &K) -> bool {
        self.values.contains_key(key)
    }

    fn insert(&mut self, key: K, value: RgbImage) {
        if self.values.contains_key(&key) {
            self.values.insert(key.clone(), value);
            self.touch(&key);
            return;
        }
        self.values.insert(key.clone(), value);
        self.order.push_back(key);
        self.evict_if_needed();
    }

    fn touch(&mut self, key: &K) {
        if let Some(index) = self.order.iter().position(|existing| existing == key)
            && let Some(existing) = self.order.remove(index)
        {
            self.order.push_back(existing);
        }
    }

    fn evict_if_needed(&mut self) {
        while self.values.len() > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.values.remove(&oldest);
        }
    }
}

impl ImageMovieSchedulerState {
    fn new(
        content_signature: u64,
        movie_axis: usize,
        next_due_index: usize,
        fps: f64,
        session_indices: Vec<(usize, usize)>,
    ) -> Self {
        Self {
            generation: 1,
            content_signature,
            movie_axis,
            session_indices,
            presentations: ImageMoviePresentationCoordinator::new(next_due_index, fps),
            protocol_pool: PanelRenderPool::new(
                image_movie_render_worker_count(),
                IMAGE_MOVIE_PROTOCOL_POOL_QUEUE_CAPACITY,
                |job| render_movie_presentation(&job.input),
            )
            .expect("image movie protocol pool"),
            queue_capacity: IMAGE_MOVIE_PROTOCOL_POOL_QUEUE_CAPACITY,
        }
    }

    fn invalidate(
        &mut self,
        content_signature: u64,
        movie_axis: usize,
        next_due_index: usize,
        fps: f64,
        session_indices: Vec<(usize, usize)>,
    ) {
        self.generation = self.generation.saturating_add(1);
        self.content_signature = content_signature;
        self.movie_axis = movie_axis;
        self.session_indices = session_indices;
        self.presentations.invalidate(next_due_index, fps);
    }
}

fn image_movie_render_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|count| {
            let count = count.get();
            if count >= 8 { 3 } else { 2 }
        })
        .unwrap_or(2)
}

fn image_movie_pipeline_state(scheduler: &ImageMovieSchedulerState) -> MoviePipelineState {
    MoviePipelineState {
        render_queue_depth: 0,
        render_active_jobs: 0,
        protocol_queue_depth: scheduler.protocol_pool.queue_depth(),
        protocol_active_jobs: scheduler.protocol_pool.active_job_count(),
        ready_bundle_count: 0,
        ready_presentation_count: scheduler.presentations.ready_len(),
        bitmap_cache_bytes: 0,
    }
}

fn new_direct_image_movie_engine() -> ImageMovieBundleEngine {
    ImageMovieBundleEngine::new(
        configured_image_movie_target_resident_bytes(),
        IMAGE_MOVIE_MIN_RENDER_SCALE,
    )
}

fn render_movie_presentation(
    job: &MovieProtocolRenderJob,
) -> Result<PreparedMoviePresentation, String> {
    let bundle = job.bundle.clone();
    let PreparedMovieBundle {
        occurrence,
        snapshot,
        plane_request,
        spectrum_request,
        rendered,
        cache_hit: _,
    } = bundle;
    let plane_bitmap = rendered
        .surfaces
        .iter()
        .find(|surface| surface.spec.kind == ImageMovieSurfaceKind::Plane)
        .map(|surface| DynamicImage::ImageRgb8(surface.bitmap.clone()).to_rgba8())
        .ok_or_else(|| "movie bundle missing plane surface".to_string())?;
    let plane_prepared = build_panel_protocol_from_rgba_owned(
        &job.plane_picker,
        Resize::Scale(None),
        plane_request.request_key.area,
        plane_bitmap,
    )
    .map_err(|error| error.to_string())?;
    let (spectrum_request, spectrum_protocol, spectrum_image_size) =
        if let (Some(request), Some(bitmap), Some(picker)) = (
            spectrum_request.as_ref(),
            rendered
                .surfaces
                .iter()
                .find(|surface| surface.spec.kind == ImageMovieSurfaceKind::Spectrum)
                .map(|surface| DynamicImage::ImageRgb8(surface.bitmap.clone()).to_rgba8()),
            job.spectrum_picker.as_ref(),
        ) {
            let prepared = build_panel_protocol_from_rgba_owned(
                picker,
                Resize::Fit(None),
                request.request_key.area,
                bitmap,
            )
            .map_err(|error| error.to_string())?;
            (
                Some(request.clone()),
                Some(prepared.protocol),
                Some((prepared.image_width, prepared.image_height)),
            )
        } else {
            (None, None, None)
        };
    Ok(PreparedMoviePresentation {
        occurrence,
        snapshot,
        plane_request,
        plane_protocol: plane_prepared.protocol,
        plane_image_size: (plane_prepared.image_width, plane_prepared.image_height),
        spectrum_request,
        spectrum_protocol,
        spectrum_image_size,
    })
}

fn new_image_plane_panel_state() -> ImagePlanePanelState {
    let picker = terminal_picker();
    let font_size = picker.font_size();
    let render_image_cache = Arc::new(Mutex::new(RenderImageCache::new(
        IMAGE_PLANE_RENDER_CACHE_CAPACITY,
    )));
    let worker_cache = Arc::clone(&render_image_cache);
    let renderer = PanelRenderer::<ImagePlaneRenderInput, String>::new(
        picker.clone(),
        Resize::Scale(None),
        move |job| {
            if let Ok(mut cache) = worker_cache.lock()
                && let Some(image) = cache.get(&job.input.cache_key)
            {
                return Ok(image::DynamicImage::ImageRgb8(image));
            }
            let image =
                render_image_plane_image(job.max_pixel_width, job.max_pixel_height, &job.input)?;
            if let Ok(mut cache) = worker_cache.lock() {
                cache.insert(job.input.cache_key, image.to_rgb8());
            }
            Ok(image)
        },
    )
    .expect("image plane panel renderer");
    ImagePlanePanelState {
        picker,
        renderer,
        render_cache: render_image_cache,
        font_size,
        display_key: None,
        pending_request_key: None,
        last_error: None,
        image_size: None,
        movie_protocol: None,
        movie_display_key: None,
        movie_image_size: None,
    }
}

fn new_image_spectrum_panel_state() -> ImageSpectrumPanelState {
    let picker = terminal_picker();
    let font_size = picker.font_size();
    let render_image_cache = Arc::new(Mutex::new(RenderImageCache::new(
        IMAGE_SPECTRUM_RENDER_CACHE_CAPACITY,
    )));
    let worker_cache = Arc::clone(&render_image_cache);
    let renderer = PanelRenderer::<ImageSpectrumRenderInput, String>::new(
        picker.clone(),
        Resize::Scale(None),
        move |job| {
            if let Ok(mut cache) = worker_cache.lock()
                && let Some(image) = cache.get(&job.input.cache_key)
            {
                return Ok(image::DynamicImage::ImageRgb8(image));
            }
            let image =
                render_image_spectrum_image(job.max_pixel_width, job.max_pixel_height, &job.input)?;
            if let Ok(mut cache) = worker_cache.lock() {
                cache.insert(job.input.cache_key, image.to_rgb8());
            }
            Ok(image)
        },
    )
    .expect("image spectrum panel renderer");
    ImageSpectrumPanelState {
        picker,
        renderer,
        font_size,
        display_key: None,
        pending_request_key: None,
        last_error: None,
        image_size: None,
        movie_protocol: None,
        movie_display_key: None,
        movie_image_size: None,
    }
}

impl fmt::Debug for ImageMovieSchedulerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageMovieSchedulerState")
            .field("generation", &self.generation)
            .field("content_signature", &self.content_signature)
            .field("movie_axis", &self.movie_axis)
            .field("next_due_index", &self.presentations.next_due_index())
            .field("session_indices", &self.session_indices)
            .field("ready_presentation_count", &self.presentations.ready_len())
            .field(
                "in_flight_presentations",
                &self.presentations.in_flight_len(),
            )
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

impl fmt::Debug for ImagePlanePanelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImagePlanePanelState")
            .field("font_size", &self.font_size)
            .field("display_key", &self.display_key)
            .field("pending_request_key", &self.pending_request_key)
            .field("last_error", &self.last_error)
            .field("image_size", &self.image_size)
            .finish()
    }
}

impl fmt::Debug for ImageSpectrumPanelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageSpectrumPanelState")
            .field("font_size", &self.font_size)
            .field("display_key", &self.display_key)
            .field("pending_request_key", &self.pending_request_key)
            .field("last_error", &self.last_error)
            .field("image_size", &self.image_size)
            .finish()
    }
}

impl ImageSpectrumPanelState {
    fn has_visible_content(&self) -> bool {
        self.movie_protocol.is_some()
            || self.renderer.protocol().is_some()
            || self.image_size.is_some()
            || self.movie_image_size.is_some()
            || self.display_key.is_some()
            || self.pending_request_key.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserPaneFocus {
    Main,
    Inspector,
}

impl ImageBrowserSessionState {
    fn clamp_left_pane_selection(&mut self) {
        if let Some(active_name) = self.active_region_definition_name()
            && let Some(index) = self
                .snapshot
                .saved_region_names
                .iter()
                .position(|name| name == active_name)
        {
            self.selected_saved_region_index = index;
        } else {
            self.selected_saved_region_index = self
                .selected_saved_region_index
                .min(self.snapshot.saved_region_names.len().saturating_sub(1));
        }

        if let Some(default_mask_name) = self.snapshot.default_mask_name.as_deref()
            && let Some(index) = self
                .snapshot
                .mask_names
                .iter()
                .position(|name| name == default_mask_name)
        {
            self.selected_mask_index = index;
        } else {
            self.selected_mask_index = self
                .selected_mask_index
                .min(self.snapshot.mask_names.len().saturating_sub(1));
        }
    }

    fn selected_saved_region_name(&self) -> Option<&str> {
        self.snapshot
            .saved_region_names
            .get(self.selected_saved_region_index)
            .map(String::as_str)
    }

    fn selected_mask_name(&self) -> Option<&str> {
        self.snapshot
            .mask_names
            .get(self.selected_mask_index)
            .map(String::as_str)
    }

    fn raster_plane_active(&self) -> bool {
        self.snapshot.active_view == ImageBrowserView::Plane
            && self.plane_mode == ImagePlaneMode::Raster
            && self.snapshot.plane.is_some()
    }

    fn spreadsheet_plane_active(&self) -> bool {
        self.snapshot.active_view == ImageBrowserView::Plane
            && self.plane_mode == ImagePlaneMode::Spreadsheet
    }

    fn movie_available(&self) -> bool {
        self.snapshot.active_view == ImageBrowserView::Plane
            && self.selected_non_display_axis_state().is_some()
    }

    fn linked_profile_active(&self) -> bool {
        self.raster_plane_active() && self.snapshot.profile.is_some()
    }

    fn spectrum_workspace_visible(&self) -> bool {
        self.snapshot.active_view == ImageBrowserView::Plane
            && self.plane_mode == ImagePlaneMode::Raster
            && (self.snapshot.profile.is_some()
                || self
                    .spectrum_panel
                    .as_ref()
                    .is_some_and(ImageSpectrumPanelState::has_visible_content))
    }

    fn region_active(&self) -> bool {
        self.snapshot.region.is_some()
    }

    fn region_editing_active(&self) -> bool {
        self.snapshot
            .region
            .as_ref()
            .is_some_and(|region| region.editing)
    }

    fn active_region_definition_name(&self) -> Option<&str> {
        self.snapshot.active_region_definition_name.as_deref()
    }

    fn selected_non_display_axis_state(
        &self,
    ) -> Option<&casars_imagebrowser_protocol::ImageNonDisplayAxisState> {
        self.snapshot
            .non_display_axes
            .get(self.selected_non_display_axis)
            .or_else(|| self.snapshot.non_display_axes.first())
    }

    fn clamp_selected_non_display_axis(&mut self) {
        if self.snapshot.non_display_axes.is_empty() {
            self.selected_non_display_axis = 0;
        } else {
            self.selected_non_display_axis = self
                .selected_non_display_axis
                .min(self.snapshot.non_display_axes.len().saturating_sub(1));
        }
    }

    fn selected_pinned_probe_index(&self) -> Option<usize> {
        let selected_id = self.selected_pinned_probe_id?;
        self.pinned_probes
            .iter()
            .position(|probe| probe.id == selected_id)
    }

    fn selected_pinned_probe(&self) -> Option<&ImagePinnedProbe> {
        self.selected_pinned_probe_index()
            .and_then(|index| self.pinned_probes.get(index))
    }

    fn selected_pinned_probe_mut(&mut self) -> Option<&mut ImagePinnedProbe> {
        let index = self.selected_pinned_probe_index()?;
        self.pinned_probes.get_mut(index)
    }

    fn pin_from_snapshot(&mut self) -> bool {
        let Some(probe) = self.snapshot.probe.clone() else {
            return false;
        };
        let Some(cursor) = self.snapshot.plane_cursor.as_ref() else {
            return false;
        };
        let pinned = ImagePinnedProbe {
            id: self.next_pinned_probe_id,
            label: format!("P{}", self.next_pinned_probe_id),
            plane_pixel: (cursor.pixel_x, cursor.pixel_y),
            probe,
            profile: self.snapshot.profile.clone(),
            non_display_axis_indices: self
                .snapshot
                .non_display_axes
                .iter()
                .map(|axis| (axis.axis, axis.index))
                .collect(),
        };
        self.next_pinned_probe_id = self.next_pinned_probe_id.saturating_add(1);
        self.pinned_probes.push(pinned);
        true
    }

    fn remove_selected_pinned_probe(&mut self) -> bool {
        let Some(index) = self.selected_pinned_probe_index() else {
            return false;
        };
        self.pinned_probes.remove(index);
        self.selected_pinned_probe_id = None;
        true
    }

    fn cycle_selected_pinned_probe(&mut self, forward: bool) -> bool {
        if self.pinned_probes.is_empty() {
            return false;
        }
        let next_index = match self.selected_pinned_probe_index() {
            Some(index) if forward => (index + 1) % self.pinned_probes.len(),
            Some(0) => self.pinned_probes.len() - 1,
            Some(index) => index - 1,
            None => {
                if forward {
                    0
                } else {
                    self.pinned_probes.len() - 1
                }
            }
        };
        self.selected_pinned_probe_id = self.pinned_probes.get(next_index).map(|probe| probe.id);
        true
    }

    fn clear_selected_pinned_probe(&mut self) -> bool {
        self.selected_pinned_probe_id.take().is_some()
    }

    fn sync_selected_pinned_probe_from_snapshot(&mut self) {
        let snapshot_probe = self.snapshot.probe.clone();
        let snapshot_cursor = self.snapshot.plane_cursor.clone();
        let snapshot_profile = self.snapshot.profile.clone();
        let snapshot_axis_indices = self
            .snapshot
            .non_display_axes
            .iter()
            .map(|axis| (axis.axis, axis.index))
            .collect::<Vec<_>>();
        let Some(probe) = self.selected_pinned_probe_mut() else {
            return;
        };
        if let Some(snapshot_probe) = snapshot_probe {
            probe.probe = snapshot_probe;
        }
        if let Some(cursor) = snapshot_cursor.as_ref() {
            probe.plane_pixel = (cursor.pixel_x, cursor.pixel_y);
        }
        if snapshot_profile.is_some() {
            probe.profile = snapshot_profile;
        }
        probe.non_display_axis_indices = snapshot_axis_indices;
    }
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

    fn image_parameters(&self) -> Option<ImageBrowserParameters> {
        match &self.kind {
            BrowserSessionKind::Image(session) => Some(session.snapshot.parameters.clone()),
            BrowserSessionKind::Table(_) => None,
        }
    }

    fn image_snapshot(&self) -> Option<&ImageBrowserSnapshot> {
        match &self.kind {
            BrowserSessionKind::Image(session) => Some(&session.snapshot),
            BrowserSessionKind::Table(_) => None,
        }
    }

    fn vertical_metrics(&self) -> Option<(usize, usize)> {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .vertical_metrics
                .map(|metrics| (metrics.total_items, metrics.viewport_items.max(1))),
            BrowserSessionKind::Image(session) => (!session.raster_plane_active()).then_some((
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
                if session.raster_plane_active() {
                    return None;
                }
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
            BrowserSessionKind::Image(session) => {
                if session.raster_plane_active() {
                    0
                } else {
                    session
                        .snapshot
                        .navigation
                        .selected_index
                        .min(u16::MAX as usize) as u16
                }
            }
        }
    }

    fn active_hscroll(&self) -> u16 {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .horizontal_metrics
                .map(|metrics| metrics.selected_index.min(u16::MAX as usize) as u16)
                .unwrap_or(0),
            BrowserSessionKind::Image(session) => {
                if session.raster_plane_active() {
                    0
                } else {
                    session.hscroll
                }
            }
        }
    }

    fn inspector_lines(&self) -> Option<Vec<String>> {
        match &self.kind {
            BrowserSessionKind::Table(session) => session
                .snapshot
                .inspector
                .as_ref()
                .map(browser_inspector_lines),
            BrowserSessionKind::Image(session) => {
                let mut lines = session.snapshot.inspector_lines.clone();
                if !session.snapshot.non_display_axes.is_empty() {
                    lines.push("Non-display axes:".to_string());
                    for (index, axis) in session.snapshot.non_display_axes.iter().enumerate() {
                        let marker = if index == session.selected_non_display_axis {
                            ">"
                        } else {
                            " "
                        };
                        lines.push(format!(
                            "{marker} {} ({}): pixel {} [{}/{}]",
                            axis.label,
                            axis.axis,
                            axis.pixel,
                            axis.index,
                            axis.length.saturating_sub(1)
                        ));
                    }
                }
                if !session.pinned_probes.is_empty() {
                    lines.push(format!("Pinned probes: {}", session.pinned_probes.len()));
                    for probe in &session.pinned_probes {
                        let marker = if session.selected_pinned_probe_id == Some(probe.id) {
                            "*"
                        } else {
                            " "
                        };
                        lines.push(format!(
                            "{marker} {} x={} y={}",
                            probe.label, probe.plane_pixel.0, probe.plane_pixel.1
                        ));
                    }
                }
                lines.push(format!("Plane mode: {}", session.plane_mode.label()));
                lines.push(format!("Colormap: {}", session.plane_colormap.label()));
                lines.push(format!(
                    "Invert: {}",
                    if session.plane_invert { "on" } else { "off" }
                ));
                lines.push(format!(
                    "Reticle: {}",
                    if session.show_live_reticle {
                        "visible"
                    } else {
                        "hidden"
                    }
                ));
                if session.movie_available() {
                    lines.push(format!(
                        "Movie: {}",
                        if session.movie.playing {
                            "playing"
                        } else {
                            "paused"
                        }
                    ));
                    lines.push(format!(
                        "Movie FPS: {}",
                        trim_float_text(format!("{:.3}", session.movie.fps))
                    ));
                }
                Some(lines)
            }
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
    renderer: PanelRenderer<PlotRenderInput, String>,
    font_size: (u16, u16),
    request_key: Option<PlotRequestKey>,
    last_error: Option<String>,
    image_size: Option<(u32, u32)>,
}

#[derive(Debug)]
struct PathChooserState {
    target: PathChooserTarget,
    explorer: FileExplorer,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParameterProfilePathMode {
    Open,
    SaveAs,
}

impl ParameterProfilePathMode {
    const fn title(self) -> &'static str {
        match self {
            Self::Open => "Open Parameter Profile",
            Self::SaveAs => "Save Parameter Profile As",
        }
    }
}

#[derive(Debug)]
struct ParameterProfilePathEntryState {
    mode: ParameterProfilePathMode,
    buffer: String,
    error: Option<String>,
    replace_existing: bool,
}

#[derive(Debug, Clone)]
enum ParameterDraftReplacement {
    Defaults,
    Last,
    LastSuccessful,
    File {
        path: PathBuf,
        profile: ParameterProfile,
    },
    Revert,
}

impl ParameterDraftReplacement {
    fn label(&self) -> String {
        match self {
            Self::Defaults => "Defaults".to_string(),
            Self::Last => "Last".to_string(),
            Self::LastSuccessful => "Last Successful".to_string(),
            Self::File { path, .. } => format!("profile {}", path.display()),
            Self::Revert => "the selected source".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathChooserTarget {
    Field(usize),
    WorkflowImportChainTable,
    WorkflowChooseCallibrary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlotRequestKey {
    area: Rect,
    theme_mode: ThemeMode,
    snapshot_generation: u64,
    plot_signature: String,
    spec_key: String,
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
struct MeasurementSetRunSnapshot {
    generation: u64,
    summary: MeasurementSetSummary,
    path: Option<PathBuf>,
    options: MeasurementSetSummaryOptions,
    dirty: bool,
}

#[derive(Debug, Clone)]
enum CurrentPlotPayload {
    MsPlot(MsPlotPayload),
    Imaging(ImagingPlotPayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditTarget {
    FormField(usize),
    RenameImageRegionDefinition,
}

struct PlotWorkspaceState {
    focus: PlotPaneFocus,
    selected_control: usize,
    selected_catalog_target: Option<PlotCatalogTarget>,
    snapshot: Option<MeasurementSetRunSnapshot>,
    next_generation: u64,
    preview_invalidated: bool,
    placeholder_protocol: Option<PanelProtocol>,
    panel: Option<PlotPanelState>,
}

impl PlotWorkspaceState {
    fn new() -> Self {
        Self {
            focus: PlotPaneFocus::Catalog,
            selected_control: 0,
            selected_catalog_target: None,
            snapshot: None,
            next_generation: 1,
            preview_invalidated: false,
            placeholder_protocol: None,
            panel: None,
        }
    }
}

impl fmt::Debug for PlotWorkspaceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlotWorkspaceState")
            .field("focus", &self.focus)
            .field("selected_control", &self.selected_control)
            .field("selected_catalog_target", &self.selected_catalog_target)
            .field("snapshot", &self.snapshot)
            .field("next_generation", &self.next_generation)
            .field("preview_invalidated", &self.preview_invalidated)
            .field(
                "has_placeholder_protocol",
                &self.placeholder_protocol.is_some(),
            )
            .field("panel", &self.panel)
            .finish()
    }
}

#[derive(Debug, Default)]
struct ResultState {
    status_line: String,
    status_kind: StatusKind,
    stdout: String,
    stderr: String,
    structured: Option<StructuredResult>,
    structured_error: Option<String>,
    file_output_path: Option<String>,
    exit_code: Option<i32>,
}

#[derive(Debug, Clone)]
enum StructuredResult {
    MeasurementSetSummary(Box<MeasurementSetSummary>),
    Calibration(Box<ManagedCalibrationOutput>),
    Imaging(Box<ManagedImagingOutput>),
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

impl StatusKind {
    fn label(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Running => "running",
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Warning => "warning",
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticFormItem {
    Field(usize),
    SummaryView(SummaryDataView),
    BrowserView(BrowserTab),
    WorkflowStage(WorkflowStageId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChoicePickerEntry {
    value: String,
    label: String,
}

#[derive(Debug)]
struct ChoicePickerState {
    target: ChoicePickerTarget,
    title: String,
    entries: Vec<ChoicePickerEntry>,
    selected_filtered_index: usize,
    filter: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChoicePickerTarget {
    Field(usize),
    ParameterSource,
    WorkflowContextSetting(WorkflowContextSettingKind),
    WorkflowProductAction(WorkflowProductActionKind),
    WorkflowChainSetting(usize, WorkflowChainSettingKind),
}

impl ChoicePickerState {
    fn filtered_entries(&self) -> Vec<(usize, &ChoicePickerEntry)> {
        let needle = self.filter.trim();
        if needle.is_empty() {
            return self.entries.iter().enumerate().collect();
        }
        let needle_lower = needle.to_ascii_lowercase();
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                entry.label.to_ascii_lowercase().contains(&needle_lower)
                    || entry.value.to_ascii_lowercase().contains(&needle_lower)
            })
            .collect()
    }

    fn clamp_selection(&mut self) {
        let visible = self.filtered_entries();
        if visible.is_empty() {
            self.selected_filtered_index = 0;
        } else {
            self.selected_filtered_index = self.selected_filtered_index.min(visible.len() - 1);
        }
    }

    fn selected_entry(&self) -> Option<&ChoicePickerEntry> {
        let visible = self.filtered_entries();
        let (_, entry) = visible.get(self.selected_filtered_index)?;
        Some(*entry)
    }
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
    content: FormSectionContent,
    collapsed: bool,
}

#[derive(Debug)]
enum FormSectionContent {
    Items(Vec<StaticFormItem>),
}

#[derive(Debug, Clone)]
struct HistoryEntry {
    sequence: usize,
    stage: Option<WorkflowStageId>,
    title: String,
    status_kind: StatusKind,
    details: Vec<String>,
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
    WorkflowContextSetting(WorkflowContextSettingKind),
    WorkflowProductAction(WorkflowProductActionKind),
    WorkflowChainEntry(usize),
    WorkflowChainSetting(usize, WorkflowChainSettingKind),
    WorkflowProduct(usize),
    ResultTabs,
    PlotCatalog(PlotCatalogTarget),
    PlotControl(PlotControlTarget),
    PlotCanvas,
    PathChooserEntry(usize),
}

#[derive(Debug, Clone, Copy)]
struct ClickState {
    target: ClickTarget,
    at: Instant,
}

fn imager_summary_correlation_selector(value: Option<String>) -> Option<String> {
    let value = value?;
    let mut parts = Vec::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        let normalized = part
            .trim_matches('\'')
            .trim_matches('"')
            .to_ascii_uppercase();
        if !matches!(
            normalized.as_str(),
            "RR" | "RL" | "LR" | "LL" | "XX" | "XY" | "YX" | "YY"
        ) {
            return None;
        }
        parts.push(normalized);
    }
    (!parts.is_empty()).then(|| parts.join(","))
}

impl AppState {
    pub(crate) fn from_schema(app: RegistryApp, schema: UiCommandSchema) -> Self {
        Self::from_schema_with_config(app, schema, ConfigStore::load_default())
    }

    pub(crate) fn from_schema_with_config(
        app: RegistryApp,
        _schema: UiCommandSchema,
        config_store: ConfigStore,
    ) -> Self {
        let schema = match app.load_schema() {
            Ok(schema) => schema,
            Err(error) => return Self::schema_error_with_config(app, error, config_store),
        };
        let default_summary_view = default_summary_view_for_app(&app.id);
        let mut ready_status_line = if app.shell_kind() == AppShellKind::Workflow
            && app.id == "calibrate"
        {
            "Ready. Choose a stage, review Context and Products, then run the stage action or press r."
                .to_string()
        } else {
            app.ready_status_line().to_string()
        };
        #[cfg(test)]
        let test_parameter_workspace = tempfile::tempdir().expect("isolated TUI test workspace");
        #[cfg(test)]
        let parameter_workspace = test_parameter_workspace.path().to_owned();
        #[cfg(not(test))]
        let parameter_workspace = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let (parameter_session, parameter_warning) =
            load_interactive_parameter_session(&app.id, &parameter_workspace);
        if let Some(warning) = &parameter_warning {
            ready_status_line = warning.clone();
        }
        let mut fields = schema
            .arguments
            .iter()
            .filter_map(FormField::from_schema)
            .collect::<Vec<_>>();
        if let Some(session) = parameter_session.as_ref() {
            sync_form_fields_from_parameter_session(&mut fields, session);
        }
        let automatic_save_enabled = !cfg!(test);
        let session_last_state = app.is_browser_session().then(|| {
            SessionLastState::new(
                ManagedStateStore::for_workspace(&parameter_workspace),
                app.id.clone(),
                automatic_save_enabled,
                Duration::from_millis(350),
            )
        });
        let sections = build_sections(&app, &fields);
        let selected_form = initial_form_selection(&sections, &fields, false);

        let mut app_state = Self {
            app,
            config_store,
            schema: Some(schema),
            schema_error: None,
            fields,
            parameter_session,
            pending_live_parameter_rollback: None,
            parameter_edit_errors: BTreeMap::new(),
            parameter_workspace: parameter_workspace.clone(),
            #[cfg(test)]
            _test_parameter_workspace: test_parameter_workspace,
            save_last: automatic_save_enabled,
            session_last_state,
            parameter_profile_path_entry: None,
            pending_parameter_replacement: None,
            sections,
            selected_form,
            show_advanced: false,
            pane_focus: PaneFocus::Parameters,
            edit_state: None,
            result: ResultState {
                status_line: ready_status_line,
                status_kind: if parameter_warning.is_some() {
                    StatusKind::Warning
                } else {
                    StatusKind::Info
                },
                ..ResultState::default()
            },
            active_result_tab: ResultTab::Overview,
            result_scrolls: [0; RESULT_TAB_COUNT],
            result_hscrolls: [0; RESULT_TAB_COUNT],
            selected_summary_view: default_summary_view,
            history_entries: Vec::new(),
            workflow_products: Vec::new(),
            running: None,
            plot_workspace: PlotWorkspaceState::new(),
            path_chooser: None,
            choice_picker: None,
            browser_mode_picker: None,
            browser_session: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_image_workspace_divider: false,
            dragging_image_cursor: false,
            dragging_image_profile: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            output_selection: None,
            show_help: false,
            cached_result_text_area: None,
            cached_left_output_area: None,
            cached_browser_viewport_cells: None,
            kitty_response_capture: None,
            kitty_movie_store_invalidated: false,
            last_click: None,
            pending_run_confirmation: false,
            notebook_bypass_once: false,
            movie_perf: MoviePerfTracer::from_env(),
            quit: false,
            return_to_launcher: false,
        };
        if app_state.app.shell_kind() == AppShellKind::Workflow && app_state.app.id == "calibrate" {
            if matches!(
                app_state.field_text("mode").as_deref(),
                None | Some("apply")
            ) {
                app_state.set_current_workflow_stage(WorkflowStageId::InspectDataset);
            }
            let stage = app_state.current_workflow_stage();
            app_state.ensure_workflow_stage_defaults(None, stage);
        }
        app_state
    }

    pub(crate) fn schema_error(app: RegistryApp, error: String) -> Self {
        Self::schema_error_with_config(app, error, ConfigStore::load_default())
    }

    pub(crate) fn schema_error_with_config(
        app: RegistryApp,
        error: String,
        config_store: ConfigStore,
    ) -> Self {
        #[cfg(test)]
        let test_parameter_workspace = tempfile::tempdir().expect("isolated TUI test workspace");
        #[cfg(test)]
        let parameter_workspace = test_parameter_workspace.path().to_owned();
        #[cfg(not(test))]
        let parameter_workspace = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        Self {
            app,
            config_store,
            schema: None,
            schema_error: Some(error.clone()),
            fields: Vec::new(),
            parameter_session: None,
            pending_live_parameter_rollback: None,
            parameter_edit_errors: BTreeMap::new(),
            parameter_workspace,
            #[cfg(test)]
            _test_parameter_workspace: test_parameter_workspace,
            save_last: true,
            session_last_state: None,
            parameter_profile_path_entry: None,
            pending_parameter_replacement: None,
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
            selected_summary_view: SummaryDataView::Fields,
            history_entries: Vec::new(),
            workflow_products: Vec::new(),
            running: None,
            plot_workspace: PlotWorkspaceState::new(),
            path_chooser: None,
            choice_picker: None,
            browser_mode_picker: None,
            browser_session: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_image_workspace_divider: false,
            dragging_image_cursor: false,
            dragging_image_profile: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            output_selection: None,
            show_help: false,
            cached_result_text_area: None,
            cached_left_output_area: None,
            cached_browser_viewport_cells: None,
            kitty_response_capture: None,
            kitty_movie_store_invalidated: false,
            last_click: None,
            pending_run_confirmation: false,
            notebook_bypass_once: false,
            movie_perf: MoviePerfTracer::from_env(),
            quit: false,
            return_to_launcher: false,
        }
    }

    /// Select the explicit workspace and typed parameter draft for this tab.
    /// A caller-provided draft preserves its chosen base source; otherwise an
    /// interactive tab starts from managed Last when it is valid and falls
    /// back to current defaults with a visible warning.
    pub(crate) fn configure_parameter_runtime(
        &mut self,
        workspace: PathBuf,
        save_last: bool,
        session: Option<ParameterSession>,
    ) {
        self.parameter_profile_path_entry = None;
        self.pending_parameter_replacement = None;
        self.parameter_workspace = workspace;
        self.save_last = save_last;
        let (session, warning) = match session {
            Some(session) => (Some(session), None),
            None => load_interactive_parameter_session(&self.app.id, &self.parameter_workspace),
        };
        self.parameter_session = session;
        self.pending_live_parameter_rollback = None;
        self.parameter_edit_errors.clear();
        if let Some(session) = self.parameter_session.as_ref() {
            sync_form_fields_from_parameter_session(&mut self.fields, session);
        }
        self.session_last_state = self.app.is_browser_session().then(|| {
            SessionLastState::new(
                ManagedStateStore::for_workspace(&self.parameter_workspace),
                self.app.id.clone(),
                self.save_last,
                Duration::from_millis(350),
            )
        });
        if let Some(warning) = warning {
            self.result.status_line = warning;
            self.result.status_kind = StatusKind::Warning;
        }
    }

    fn record_session_opened(&mut self) {
        let report = match (
            self.session_last_state.as_mut(),
            self.parameter_session.as_ref(),
        ) {
            (Some(last), Some(session)) => last.opened(session),
            _ => return,
        };
        match report {
            Ok(report) => {
                if let Some(warning) = report.warning {
                    self.result.status_line = format!(
                        "{} Warning: could not save session Last: {warning}",
                        self.result.status_line
                    );
                    self.result.status_kind = StatusKind::Warning;
                    self.result
                        .stderr
                        .push_str(&format!("Automatic parameter save warning: {warning}\n"));
                }
            }
            Err(error) => {
                self.result.status_line = format!(
                    "{} Warning: could not render session Last: {error}",
                    self.result.status_line
                );
                self.result.status_kind = StatusKind::Warning;
            }
        }
    }

    fn queue_accepted_session_parameter_change(&mut self) {
        let result = match (
            self.session_last_state.as_mut(),
            self.parameter_session.as_ref(),
        ) {
            (Some(last), Some(session)) => last.accepted_durable_change(session, Instant::now()),
            _ => return,
        };
        if let Err(error) = result {
            self.result.status_line =
                format!("Session updated, but Last could not be prepared: {error}");
            self.result.status_kind = StatusKind::Warning;
        }
    }

    fn flush_session_last_if_due(&mut self) {
        let Some(last) = self.session_last_state.as_mut() else {
            return;
        };
        if let Some(warning) = last.flush_if_due(Instant::now()).warning {
            self.result.status_line =
                format!("Session active. Warning: could not save Last: {warning}");
            self.result.status_kind = StatusKind::Warning;
            self.result
                .stderr
                .push_str(&format!("Automatic parameter save warning: {warning}\n"));
        }
    }

    fn flush_session_last_on_close(&mut self) {
        let Some(last) = self.session_last_state.as_mut() else {
            return;
        };
        if let Some(warning) = last.flush().warning {
            self.result.stderr.push_str(&format!(
                "Automatic parameter save warning on close: {warning}\n"
            ));
        }
    }

    fn load_parameter_defaults(&mut self) {
        self.request_parameter_replacement(ParameterDraftReplacement::Defaults);
    }

    fn load_parameter_last(&mut self) {
        self.request_parameter_replacement(ParameterDraftReplacement::Last);
    }

    fn revert_parameter_edits(&mut self) {
        self.request_parameter_replacement(ParameterDraftReplacement::Revert);
    }

    fn open_parameter_source_picker(&mut self) {
        let Some(session) = self.parameter_session.as_ref() else {
            self.result.status_line = "Parameter sources are unavailable for this surface.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        let mut entries = vec![
            ChoicePickerEntry {
                value: "defaults".to_string(),
                label: "Defaults".to_string(),
            },
            ChoicePickerEntry {
                value: "last".to_string(),
                label: "Last".to_string(),
            },
        ];
        if session.bundle().surface.kind() == SurfaceKind::Task {
            entries.push(ChoicePickerEntry {
                value: "last_successful".to_string(),
                label: "Last Successful".to_string(),
            });
        }
        entries.extend([
            ChoicePickerEntry {
                value: "open".to_string(),
                label: "Open TOML...".to_string(),
            },
            ChoicePickerEntry {
                value: "save_as".to_string(),
                label: "Save As...".to_string(),
            },
            ChoicePickerEntry {
                value: "revert".to_string(),
                label: "Revert edits".to_string(),
            },
        ]);
        let current = match session.base_source() {
            BaseSource::Defaults => "defaults",
            BaseSource::Last => "last",
            BaseSource::LastSuccessful => "last_successful",
            BaseSource::File(_) => "open",
        };
        self.open_choice_picker_target(
            ChoicePickerTarget::ParameterSource,
            "Parameter Sources".to_string(),
            entries,
            current.to_string(),
        );
    }

    fn activate_parameter_source_menu_value(&mut self, value: &str) {
        match value {
            "defaults" => self.load_parameter_defaults(),
            "last" => self.load_parameter_last(),
            "last_successful" => {
                self.request_parameter_replacement(ParameterDraftReplacement::LastSuccessful)
            }
            "open" => self.open_parameter_profile_path_entry(ParameterProfilePathMode::Open),
            "save_as" => self.open_parameter_profile_path_entry(ParameterProfilePathMode::SaveAs),
            "revert" => self.revert_parameter_edits(),
            _ => {
                self.result.status_line = format!("Unknown parameter source action {value:?}.");
                self.result.status_kind = StatusKind::Error;
            }
        }
    }

    fn request_parameter_replacement(&mut self, replacement: ParameterDraftReplacement) {
        let Some(session) = self.parameter_session.as_ref() else {
            self.result.status_line = "Typed parameter session is unavailable.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        if session.is_dirty() {
            let label = replacement.label();
            self.pending_parameter_replacement = Some(replacement);
            self.result.status_line =
                format!("Parameters are modified. Confirm replacement with {label}.");
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        self.perform_parameter_replacement(replacement);
    }

    fn perform_parameter_replacement(&mut self, replacement: ParameterDraftReplacement) {
        self.pending_parameter_replacement = None;
        let result = match replacement {
            ParameterDraftReplacement::Defaults => self
                .replace_parameter_base(BaseSource::Defaults, None)
                .map(|()| "Parameter source replaced with Defaults.".to_string()),
            ParameterDraftReplacement::Last => self
                .replace_with_managed_parameter_profile(ManagedProfileKind::Last)
                .map(|()| "Parameter source replaced with Last.".to_string()),
            ParameterDraftReplacement::LastSuccessful => self
                .replace_with_managed_parameter_profile(ManagedProfileKind::LastSuccessful)
                .map(|()| "Parameter source replaced with Last Successful.".to_string()),
            ParameterDraftReplacement::File { path, profile } => self
                .replace_parameter_base(BaseSource::File(path.clone()), Some(&profile))
                .map(|()| format!("Opened parameter profile {}.", path.display())),
            ParameterDraftReplacement::Revert => self
                .revert_parameter_session()
                .map(|()| "Parameter edits reverted to the selected source.".to_string()),
        };
        match result {
            Ok(message) => {
                self.result.status_line = message;
                self.result.status_kind = StatusKind::Info;
            }
            Err(error) => {
                self.result.status_line = error;
                self.result.status_kind = StatusKind::Error;
            }
        }
    }

    fn replace_with_managed_parameter_profile(
        &mut self,
        kind: ManagedProfileKind,
    ) -> Result<(), String> {
        if kind == ManagedProfileKind::LastSuccessful
            && self
                .parameter_session
                .as_ref()
                .is_some_and(|session| session.bundle().surface.kind() == SurfaceKind::Session)
        {
            return Err(format!(
                "Session surface {} does not have Last Successful.",
                self.app.id
            ));
        }
        let label = match kind {
            ManagedProfileKind::Last => "Last",
            ManagedProfileKind::LastSuccessful => "Last Successful",
        };
        let source = ManagedStateStore::for_workspace(&self.parameter_workspace)
            .read(&self.app.id, kind)
            .map_err(|error| format!("Could not read {label}: {error}"))?
            .ok_or_else(|| format!("No {label} profile exists for {}.", self.app.id))?;
        let profile = parse_profile(&source)
            .map_err(|error| format!("{label} profile is invalid: {error}"))?;
        let base_source = match kind {
            ManagedProfileKind::Last => BaseSource::Last,
            ManagedProfileKind::LastSuccessful => BaseSource::LastSuccessful,
        };
        self.replace_parameter_base(base_source, Some(&profile))
    }

    fn replace_parameter_base(
        &mut self,
        source: BaseSource,
        profile: Option<&ParameterProfile>,
    ) -> Result<(), String> {
        {
            let session = self
                .parameter_session
                .as_mut()
                .ok_or_else(|| "Typed parameter session is unavailable.".to_string())?;
            session
                .replace_base(source, profile)
                .map_err(|error| format!("Could not replace parameter source: {error}"))?;
            sync_form_fields_from_parameter_session(&mut self.fields, session);
        }
        self.parameter_edit_errors.clear();
        self.refresh_parameter_field_metadata();
        Ok(())
    }

    fn revert_parameter_session(&mut self) -> Result<(), String> {
        {
            let session = self
                .parameter_session
                .as_mut()
                .ok_or_else(|| "Typed parameter session is unavailable.".to_string())?;
            session
                .revert()
                .map_err(|error| format!("Could not revert parameters: {error}"))?;
            sync_form_fields_from_parameter_session(&mut self.fields, session);
        }
        self.parameter_edit_errors.clear();
        self.refresh_parameter_field_metadata();
        Ok(())
    }

    fn confirm_parameter_source_replacement(&mut self) {
        let Some(replacement) = self.pending_parameter_replacement.take() else {
            return;
        };
        self.perform_parameter_replacement(replacement);
    }

    fn cancel_parameter_source_replacement(&mut self) {
        self.pending_parameter_replacement = None;
        self.result.status_line = "Parameter source replacement canceled.".to_string();
        self.result.status_kind = StatusKind::Info;
    }

    fn open_parameter_profile_path_entry(&mut self, mode: ParameterProfilePathMode) {
        self.choice_picker = None;
        self.path_chooser = None;
        self.parameter_profile_path_entry = Some(ParameterProfilePathEntryState {
            mode,
            buffer: String::new(),
            error: None,
            replace_existing: false,
        });
    }

    fn apply_parameter_profile_path_action(&mut self, action: ParameterProfilePathAction) {
        match action {
            ParameterProfilePathAction::Cancel => {
                self.parameter_profile_path_entry = None;
                self.result.status_line = "Parameter profile path entry canceled.".to_string();
                self.result.status_kind = StatusKind::Info;
            }
            ParameterProfilePathAction::Commit => self.commit_parameter_profile_path(),
            ParameterProfilePathAction::DeleteBackward => {
                if let Some(entry) = self.parameter_profile_path_entry.as_mut() {
                    entry.buffer.pop();
                    entry.error = None;
                    entry.replace_existing = false;
                }
            }
            ParameterProfilePathAction::Insert(ch) => {
                if let Some(entry) = self.parameter_profile_path_entry.as_mut() {
                    entry.buffer.push(ch);
                    entry.error = None;
                    entry.replace_existing = false;
                }
            }
        }
    }

    fn commit_parameter_profile_path(&mut self) {
        let Some(entry) = self.parameter_profile_path_entry.as_ref() else {
            return;
        };
        let mode = entry.mode;
        let raw_path = entry.buffer.trim();
        if raw_path.is_empty() {
            self.set_parameter_profile_path_error("Enter a TOML profile path.");
            return;
        }
        let candidate = expand_tilde_path(raw_path);
        let path = if candidate.is_absolute() {
            candidate
        } else {
            self.parameter_workspace.join(candidate)
        };
        match mode {
            ParameterProfilePathMode::Open => self.prepare_open_parameter_profile(path),
            ParameterProfilePathMode::SaveAs => {
                let replace_existing = self
                    .parameter_profile_path_entry
                    .as_ref()
                    .is_some_and(|entry| entry.replace_existing);
                if path.exists() && !replace_existing {
                    if let Some(entry) = self.parameter_profile_path_entry.as_mut() {
                        entry.error = Some(
                            "The destination exists. Press Enter again to replace it.".to_string(),
                        );
                        entry.replace_existing = true;
                    }
                    return;
                }
                self.save_parameter_profile_as(path);
            }
        }
    }

    fn prepare_open_parameter_profile(&mut self, path: PathBuf) {
        let source = match fs::read_to_string(&path) {
            Ok(source) => source,
            Err(error) => {
                self.set_parameter_profile_path_error(format!(
                    "Could not read {}: {error}",
                    path.display()
                ));
                return;
            }
        };
        let profile = match parse_profile(&source) {
            Ok(profile) => profile,
            Err(error) => {
                self.set_parameter_profile_path_error(format!(
                    "Invalid profile {}: {error}",
                    path.display()
                ));
                return;
            }
        };
        let validation = self
            .parameter_session
            .as_ref()
            .ok_or_else(|| "Typed parameter session is unavailable.".to_string())
            .and_then(|session| {
                ParameterSession::from_profile(
                    session.bundle().clone(),
                    BaseSource::File(path.clone()),
                    &profile,
                )
                .map(|_| ())
                .map_err(|error| error.to_string())
            });
        if let Err(error) = validation {
            self.set_parameter_profile_path_error(format!(
                "Profile {} does not match this surface: {error}",
                path.display()
            ));
            return;
        }
        self.parameter_profile_path_entry = None;
        self.request_parameter_replacement(ParameterDraftReplacement::File { path, profile });
    }

    fn save_parameter_profile_as(&mut self, path: PathBuf) {
        if let Some((name, error)) = self.parameter_edit_errors.iter().next() {
            self.set_parameter_profile_path_error(format!(
                "Cannot save while {name} is invalid: {error}"
            ));
            return;
        }
        let source = match self
            .parameter_session
            .as_ref()
            .ok_or_else(|| "Typed parameter session is unavailable.".to_string())
            .and_then(|session| session.render_sparse().map_err(|error| error.to_string()))
        {
            Ok(source) => source,
            Err(error) => {
                self.set_parameter_profile_path_error(format!("Could not render profile: {error}"));
                return;
            }
        };
        if let Err(error) = write_parameter_profile_atomic(&path, &source) {
            self.set_parameter_profile_path_error(format!(
                "Could not save {}: {error}",
                path.display()
            ));
            return;
        }
        let profile = match parse_profile(&source) {
            Ok(profile) => profile,
            Err(error) => {
                self.set_parameter_profile_path_error(format!(
                    "Saved profile could not be reloaded: {error}"
                ));
                return;
            }
        };
        if let Err(error) =
            self.replace_parameter_base(BaseSource::File(path.clone()), Some(&profile))
        {
            self.set_parameter_profile_path_error(error);
            return;
        }
        self.parameter_profile_path_entry = None;
        self.result.status_line = format!("Saved parameter profile as {}.", path.display());
        self.result.status_kind = StatusKind::Ok;
    }

    fn set_parameter_profile_path_error(&mut self, error: impl Into<String>) {
        if let Some(entry) = self.parameter_profile_path_entry.as_mut() {
            entry.error = Some(error.into());
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

    pub(crate) fn parameter_profile_path_entry_active(&self) -> bool {
        self.parameter_profile_path_entry.is_some()
    }

    pub(crate) fn parameter_profile_path_entry_title(&self) -> Option<&str> {
        self.parameter_profile_path_entry
            .as_ref()
            .map(|entry| entry.mode.title())
    }

    pub(crate) fn parameter_profile_path_entry_value(&self) -> Option<&str> {
        self.parameter_profile_path_entry
            .as_ref()
            .map(|entry| entry.buffer.as_str())
    }

    pub(crate) fn parameter_profile_path_entry_error(&self) -> Option<&str> {
        self.parameter_profile_path_entry
            .as_ref()
            .and_then(|entry| entry.error.as_deref())
    }

    pub(crate) fn parameter_source_confirmation_message(&self) -> Option<String> {
        self.pending_parameter_replacement
            .as_ref()
            .map(|replacement| {
                format!(
                    "The current parameter draft is modified. Discard those edits and load {}?",
                    replacement.label()
                )
            })
    }

    pub(crate) fn path_chooser_title(&self) -> Option<String> {
        let chooser = self.path_chooser.as_ref()?;
        Some(match chooser.target {
            PathChooserTarget::Field(field_index) => {
                let field = self.fields.get(field_index)?;
                format!("Browse {}", field.schema.label)
            }
            PathChooserTarget::WorkflowImportChainTable => {
                "Import Calibration Table Into Chain".to_string()
            }
            PathChooserTarget::WorkflowChooseCallibrary => "Choose Callibrary File".to_string(),
        })
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

    pub(crate) fn choice_picker_active(&self) -> bool {
        self.choice_picker.is_some()
    }

    pub(crate) fn choice_picker_title(&self) -> Option<&str> {
        self.choice_picker
            .as_ref()
            .map(|picker| picker.title.as_str())
    }

    pub(crate) fn choice_picker_filter(&self) -> Option<&str> {
        self.choice_picker
            .as_ref()
            .map(|picker| picker.filter.as_str())
    }

    pub(crate) fn choice_picker_entries(&self) -> Option<Vec<(String, bool)>> {
        let picker = self.choice_picker.as_ref()?;
        Some(
            picker
                .filtered_entries()
                .into_iter()
                .enumerate()
                .map(|(offset, (_, entry))| {
                    (
                        entry.label.clone(),
                        offset == picker.selected_filtered_index,
                    )
                })
                .collect(),
        )
    }

    pub(crate) fn choice_picker_field_index(&self) -> Option<usize> {
        self.choice_picker
            .as_ref()
            .and_then(|picker| match picker.target {
                ChoicePickerTarget::Field(field_index) => Some(field_index),
                _ => None,
            })
    }

    fn choice_picker_anchor_target(&self) -> Option<FormSelection> {
        self.choice_picker
            .as_ref()
            .map(|picker| match picker.target {
                ChoicePickerTarget::Field(field_index) => FormSelection::Field(field_index),
                ChoicePickerTarget::ParameterSource => self.selected_form,
                ChoicePickerTarget::WorkflowContextSetting(kind) => {
                    FormSelection::WorkflowContextSetting(kind)
                }
                ChoicePickerTarget::WorkflowProductAction(kind) => {
                    FormSelection::WorkflowProductAction(kind)
                }
                ChoicePickerTarget::WorkflowChainSetting(entry, kind) => {
                    FormSelection::WorkflowChainSetting(entry, kind)
                }
            })
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
        let text = self.render_field_line(field_index);
        let browse_len = BROWSE_SUFFIX.chars().count() as u16;
        let text_end = row
            .rect
            .x
            .saturating_add(text.chars().count().min(row.rect.width as usize) as u16);
        let browse_start = text_end.saturating_sub(browse_len);
        column >= browse_start && column < text_end
    }

    fn field_picker_entries(&self, field_index: usize) -> Option<Vec<ChoicePickerEntry>> {
        let field = self.fields.get(field_index)?;
        if field.is_path() {
            return None;
        }
        if let FormValue::Choice { choices, .. } = &field.value
            && !choices.is_empty()
        {
            return Some(
                choices
                    .iter()
                    .map(|choice| ChoicePickerEntry {
                        value: choice.clone(),
                        label: choice.clone(),
                    })
                    .collect(),
            );
        }
        dynamic_field_picker_entries(self, field.schema.id.as_str())
    }

    fn open_choice_picker(&mut self, field_index: usize) {
        let Some(entries) = self.field_picker_entries(field_index) else {
            return;
        };
        let title = self
            .fields
            .get(field_index)
            .map(|field| format!("Choose {}", field.schema.label))
            .unwrap_or_else(|| "Choose Value".to_string());
        let current = self
            .fields
            .get(field_index)
            .and_then(FormField::text_value)
            .unwrap_or_default();
        self.open_choice_picker_target(
            ChoicePickerTarget::Field(field_index),
            title,
            entries,
            current,
        );
    }

    fn open_choice_picker_target(
        &mut self,
        target: ChoicePickerTarget,
        title: String,
        entries: Vec<ChoicePickerEntry>,
        current: String,
    ) {
        if entries.is_empty() {
            return;
        }
        self.path_chooser = None;
        let selected_filtered_index = entries
            .iter()
            .position(|entry| entry.value == current)
            .unwrap_or(0);
        self.choice_picker = Some(ChoicePickerState {
            target,
            title,
            entries,
            selected_filtered_index,
            filter: String::new(),
        });
    }

    fn workflow_product_picker_entries(&self) -> Vec<ChoicePickerEntry> {
        self.workflow_products
            .iter()
            .filter(|product| product.stage != WorkflowStageId::Apply)
            .map(|product| ChoicePickerEntry {
                value: product.path.display().to_string(),
                label: format!(
                    "r{}  {}  [{} | {}]",
                    product.revision,
                    product.path.display(),
                    product.family,
                    product.status.label()
                ),
            })
            .collect()
    }

    fn workflow_context_picker_entries(
        &self,
        kind: WorkflowContextSettingKind,
    ) -> Vec<ChoicePickerEntry> {
        match kind {
            WorkflowContextSettingKind::ActiveFields => {
                let mut entries = vec![ChoicePickerEntry {
                    value: String::new(),
                    label: "<all fields>".to_string(),
                }];
                if let Some(summary) = self.current_structured_summary() {
                    entries.extend(summary.fields.iter().map(|field| ChoicePickerEntry {
                        value: field.field_id.to_string(),
                        label: format!("{}  {}", field.field_id, field.name),
                    }));
                }
                entries
            }
            WorkflowContextSettingKind::RefAnt => {
                dynamic_field_picker_entries(self, "refant").unwrap_or_default()
            }
            WorkflowContextSettingKind::FluxReferenceFields => {
                let mut entries = vec![ChoicePickerEntry {
                    value: String::new(),
                    label: "<unset>".to_string(),
                }];
                entries.extend(
                    dynamic_field_picker_entries(self, "reference_fields").unwrap_or_default(),
                );
                entries
            }
            WorkflowContextSettingKind::FluxTransferFields => {
                let mut entries = vec![ChoicePickerEntry {
                    value: String::new(),
                    label: "<unset>".to_string(),
                }];
                entries.extend(
                    dynamic_field_picker_entries(self, "transfer_fields").unwrap_or_default(),
                );
                entries
            }
        }
    }

    fn workflow_chain_setting_picker_entries(
        &self,
        kind: WorkflowChainSettingKind,
    ) -> Vec<ChoicePickerEntry> {
        match kind {
            WorkflowChainSettingKind::Gainfield => {
                let mut entries = vec![
                    ChoicePickerEntry {
                        value: String::new(),
                        label: "<default>".to_string(),
                    },
                    ChoicePickerEntry {
                        value: "nearest".to_string(),
                        label: "nearest".to_string(),
                    },
                ];
                if let Some(summary) = self.current_structured_summary() {
                    entries.extend(summary.fields.iter().map(|field| ChoicePickerEntry {
                        value: field.field_id.to_string(),
                        label: format!("{}  {}", field.field_id, field.name),
                    }));
                }
                entries
            }
            WorkflowChainSettingKind::Interp => ["nearest", "linear", "nearest,linear"]
                .into_iter()
                .map(|value| ChoicePickerEntry {
                    value: value.to_string(),
                    label: value.to_string(),
                })
                .collect(),
            WorkflowChainSettingKind::Spwmap => {
                let mut entries = vec![ChoicePickerEntry {
                    value: String::new(),
                    label: "<identity>".to_string(),
                }];
                if let Some(summary) = self.current_structured_summary() {
                    let spw_ids = summary
                        .spectral_windows
                        .iter()
                        .map(|spw| spw.spectral_window_id)
                        .collect::<Vec<_>>();
                    if !spw_ids.is_empty() {
                        if spw_ids.len() > 1 {
                            let exact = spw_ids
                                .iter()
                                .map(|spw| spw.to_string())
                                .collect::<Vec<_>>()
                                .join(",");
                            entries.push(ChoicePickerEntry {
                                value: exact.clone(),
                                label: format!("match selected SPWs ({exact})"),
                            });
                        }
                        entries.extend(spw_ids.iter().map(|spw_id| {
                            let map = if spw_ids.len() > 1 {
                                std::iter::repeat_n(spw_id.to_string(), spw_ids.len())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            } else {
                                spw_id.to_string()
                            };
                            ChoicePickerEntry {
                                value: map.clone(),
                                label: format!("all selected SPWs -> {spw_id} ({map})"),
                            }
                        }));
                    }
                }
                entries
            }
            WorkflowChainSettingKind::Calwt => ["false", "true"]
                .into_iter()
                .map(|value| ChoicePickerEntry {
                    value: value.to_string(),
                    label: value.to_string(),
                })
                .collect(),
        }
    }

    fn close_choice_picker(&mut self) {
        self.choice_picker = None;
    }

    fn commit_choice_picker(&mut self) {
        let Some((target, value, label)) = self.choice_picker.as_ref().and_then(|picker| {
            picker
                .selected_entry()
                .map(|entry| (picker.target, entry.value.clone(), entry.label.clone()))
        }) else {
            self.close_choice_picker();
            return;
        };
        if target == ChoicePickerTarget::ParameterSource {
            self.close_choice_picker();
            self.activate_parameter_source_menu_value(&value);
            return;
        }
        match target {
            ChoicePickerTarget::Field(field_index) => {
                if let Some(field) = self.fields.get_mut(field_index) {
                    field.set_text(value.clone());
                }
                if let Err(error) = self.sync_parameter_from_field(field_index) {
                    self.close_choice_picker();
                    self.result.status_line = error;
                    self.result.status_kind = StatusKind::Warning;
                    return;
                }
                self.apply_live_image_view_parameters_if_needed(field_index);
                self.mark_plot_snapshot_dirty();
            }
            ChoicePickerTarget::WorkflowContextSetting(kind) => {
                let _ = self.apply_startup_text_value(kind.field_id(), value.clone());
                self.selected_form = FormSelection::WorkflowContextSetting(kind);
            }
            ChoicePickerTarget::WorkflowProductAction(
                WorkflowProductActionKind::AddSolvedProduct,
            ) => {
                self.append_workflow_chain_path(value.clone());
            }
            ChoicePickerTarget::WorkflowProductAction(
                WorkflowProductActionKind::ImportChainTable,
            )
            | ChoicePickerTarget::WorkflowProductAction(
                WorkflowProductActionKind::ChooseCallibrary,
            ) => {}
            ChoicePickerTarget::WorkflowChainSetting(entry, kind) => {
                if let Err(error) =
                    self.apply_workflow_chain_setting_value(entry, kind, value.clone())
                {
                    self.close_choice_picker();
                    self.result.status_line = error;
                    self.result.status_kind = StatusKind::Warning;
                    return;
                }
            }
            ChoicePickerTarget::ParameterSource => unreachable!("handled above"),
        }
        self.close_choice_picker();
        self.result.status_line = format!("Selected {}.", label);
        self.result.status_kind = StatusKind::Ok;
    }

    fn cancel_choice_picker(&mut self) {
        self.close_choice_picker();
        self.result.status_line = "Choice picker canceled.".to_string();
        self.result.status_kind = StatusKind::Info;
    }

    fn activate_workflow_context_setting(&mut self, kind: WorkflowContextSettingKind) {
        self.selected_form = FormSelection::WorkflowContextSetting(kind);
        let entries = self.workflow_context_picker_entries(kind);
        if entries.is_empty() {
            self.result.status_line = format!(
                "No {} choices are available yet.",
                kind.label().to_ascii_lowercase()
            );
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        let current = self.field_text(kind.field_id()).unwrap_or_default();
        self.open_choice_picker_target(
            ChoicePickerTarget::WorkflowContextSetting(kind),
            format!("Choose {}", kind.label()),
            entries,
            current,
        );
    }

    fn cycle_choice_picker(&mut self, forward: bool) {
        let Some(picker) = self.choice_picker.as_mut() else {
            return;
        };
        let visible_len = picker.filtered_entries().len();
        if visible_len == 0 {
            picker.selected_filtered_index = 0;
            return;
        }
        if forward {
            picker.selected_filtered_index = (picker.selected_filtered_index + 1) % visible_len;
        } else if picker.selected_filtered_index == 0 {
            picker.selected_filtered_index = visible_len - 1;
        } else {
            picker.selected_filtered_index -= 1;
        }
    }

    fn extend_choice_picker_filter(&mut self, ch: char) {
        let Some(picker) = self.choice_picker.as_mut() else {
            return;
        };
        picker.filter.push(ch);
        picker.clamp_selection();
    }

    fn delete_choice_picker_filter_backward(&mut self) {
        let Some(picker) = self.choice_picker.as_mut() else {
            return;
        };
        picker.filter.pop();
        picker.clamp_selection();
    }

    fn open_path_chooser_for_selected_field(&mut self) {
        let Some(field_index) = self.selected_path_field_index() else {
            return;
        };
        self.open_path_chooser(field_index);
    }

    fn open_path_chooser(&mut self, field_index: usize) {
        let Some(field) = self.fields.get(field_index) else {
            return;
        };
        let start = chooser_start_path(field.text_value().as_deref());
        self.open_path_chooser_target(PathChooserTarget::Field(field_index), start);
    }

    fn open_path_chooser_target(&mut self, target: PathChooserTarget, start: PathBuf) {
        if let PathChooserTarget::Field(field_index) = target {
            self.prepare_path_chooser_field(field_index);
        }
        self.choice_picker = None;
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
                    target,
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
                let _ = self.sync_parameter_from_field(field_index);
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
        let Some((target, selected_path)) = self
            .path_chooser
            .as_ref()
            .map(|chooser| (chooser.target, chooser.explorer.current().path.clone()))
        else {
            return;
        };
        self.select_path_chooser_path(target, &selected_path);
    }

    fn select_current_path_chooser_entry(&mut self) {
        let Some((target, selected_path)) = self
            .path_chooser
            .as_ref()
            .map(|chooser| (chooser.target, chooser.explorer.current().path.clone()))
        else {
            return;
        };
        self.select_path_chooser_path(target, &selected_path);
    }

    fn select_path_chooser_path(&mut self, target: PathChooserTarget, selected_path: &Path) {
        let value = absolute_display_path(selected_path);
        match target {
            PathChooserTarget::Field(selected_field_index) => {
                if let Some(field) = self.fields.get_mut(selected_field_index) {
                    field.set_text(value.clone());
                    self.mark_plot_snapshot_dirty();
                }
                if let Err(error) = self.sync_parameter_from_field(selected_field_index) {
                    self.result.status_line = error;
                    self.result.status_kind = StatusKind::Warning;
                    return;
                }
            }
            PathChooserTarget::WorkflowImportChainTable => {
                self.append_workflow_chain_path(value.clone());
            }
            PathChooserTarget::WorkflowChooseCallibrary => {
                self.set_workflow_callibrary_path(value.clone());
            }
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
        self.flush_session_last_if_due();
        self.ensure_current_summary_snapshot_if_needed();
        self.pump_plot_panel();
        if self.image_movie_scheduler_enabled() {
            self.maybe_emit_movie_perf_summary();
            return;
        }
        self.advance_image_movie();
        self.pump_image_plane_panel();
        self.pump_image_spectrum_panel();
        self.maybe_emit_movie_perf_summary();
    }

    pub(crate) fn preferred_tick_rate(&self) -> Duration {
        const DEFAULT_TICK_RATE: Duration = Duration::from_millis(100);
        const MIN_MOVIE_TICK_RATE: Duration = Duration::from_millis(16);

        let Some(state) = self.image_browser_session_state() else {
            return DEFAULT_TICK_RATE;
        };
        if !state.movie.playing || state.movie.terminal_looping {
            return DEFAULT_TICK_RATE;
        }
        state
            .movie
            .frame_interval
            .max(MIN_MOVIE_TICK_RATE)
            .min(DEFAULT_TICK_RATE)
    }

    fn key_profile(&self) -> KeyProfile {
        KeyProfile::Default
    }

    fn input_mode(&self) -> InputMode {
        if self.parameter_profile_path_entry.is_some() {
            InputMode::ParameterProfilePath
        } else if self.pending_parameter_replacement.is_some() {
            InputMode::ParameterSourceConfirmation
        } else if self.path_chooser.is_some() {
            InputMode::PathChooser
        } else if self.edit_state.is_some() {
            InputMode::Edit
        } else if self.browser_session.is_some() {
            if self.browser_uses_parameter_pane() && self.pane_focus == PaneFocus::Parameters {
                InputMode::Parameters
            } else if self.pane_focus == PaneFocus::Result
                && !self.browser_result_uses_live_navigation()
            {
                InputMode::Result
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
        if self.result_tab_uses_plot_workspace() {
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
                if self.result_tab_uses_plot_workspace() {
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
        let plots_active = mode == InputMode::Result && self.result_tab_uses_plot_workspace();

        if mode == InputMode::ParameterProfilePath {
            return match key_event.code {
                KeyCode::Esc => Some(AppAction::ParameterProfilePath(
                    ParameterProfilePathAction::Cancel,
                )),
                KeyCode::Enter if key_event.modifiers.is_empty() => Some(
                    AppAction::ParameterProfilePath(ParameterProfilePathAction::Commit),
                ),
                KeyCode::Backspace if key_event.modifiers.is_empty() => Some(
                    AppAction::ParameterProfilePath(ParameterProfilePathAction::DeleteBackward),
                ),
                KeyCode::Char(ch)
                    if key_event.modifiers.is_empty()
                        || key_event.modifiers == KeyModifiers::SHIFT =>
                {
                    Some(AppAction::ParameterProfilePath(
                        ParameterProfilePathAction::Insert(ch),
                    ))
                }
                _ => None,
            };
        }

        if mode == InputMode::ParameterSourceConfirmation {
            return match key_event.code {
                KeyCode::Enter | KeyCode::Char('y') if key_event.modifiers.is_empty() => {
                    Some(AppAction::ParameterSourceConfirmation(
                        ParameterSourceConfirmationAction::Confirm,
                    ))
                }
                KeyCode::Esc | KeyCode::Char('n') if key_event.modifiers.is_empty() => {
                    Some(AppAction::ParameterSourceConfirmation(
                        ParameterSourceConfirmationAction::Cancel,
                    ))
                }
                _ => None,
            };
        }

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
            KeyCode::Char('c') if key_event.modifiers == KeyModifiers::CONTROL => {
                return Some(AppAction::Quit);
            }
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
            KeyCode::Char('v')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && !self.has_active_session() =>
            {
                return Some(AppAction::ToggleNotebookBypass);
            }
            KeyCode::Char('o')
                if key_event.modifiers == KeyModifiers::CONTROL
                    && mode != InputMode::Edit
                    && (!self.has_active_session() || self.browser_uses_parameter_pane()) =>
            {
                return Some(AppAction::OpenPathChooser);
            }
            KeyCode::Char('p')
                if key_event.modifiers == KeyModifiers::CONTROL && mode != InputMode::Edit =>
            {
                return Some(AppAction::OpenParameterSources);
            }
            KeyCode::Char('d')
                if key_event.modifiers == KeyModifiers::CONTROL && mode != InputMode::Edit =>
            {
                return Some(AppAction::LoadParameterDefaults);
            }
            KeyCode::Char('l')
                if key_event.modifiers == KeyModifiers::CONTROL && mode != InputMode::Edit =>
            {
                return Some(AppAction::LoadParameterLast);
            }
            KeyCode::Char('r')
                if key_event.modifiers == KeyModifiers::CONTROL && mode != InputMode::Edit =>
            {
                return Some(AppAction::RevertParameters);
            }
            KeyCode::Char('g')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::TogglePlaneMode));
            }
            KeyCode::Char('s')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::ToggleSpectrumPane));
            }
            KeyCode::Char('m')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::ToggleMovie));
            }
            KeyCode::Char('+') | KeyCode::Char('=')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::ZoomIn));
            }
            KeyCode::Char('-')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::ZoomOut));
            }
            KeyCode::Char('0')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::ResetViewWindow));
            }
            KeyCode::Char('H')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::PanLeft));
            }
            KeyCode::Char('L')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::PanRight));
            }
            KeyCode::Char('K')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::PanUp));
            }
            KeyCode::Char('J')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::PanDown));
            }
            KeyCode::Char('c')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::CycleColormap));
            }
            KeyCode::Char('i')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::ToggleInvert));
            }
            KeyCode::Char('R')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::StartRegionShape));
            }
            KeyCode::Char('D')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::ClearRegion));
            }
            KeyCode::Char('S')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::SaveRegionDefinition));
            }
            KeyCode::Char('O')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::LoadNextRegionDefinition));
            }
            KeyCode::Char('E')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::RenameRegionDefinition));
            }
            KeyCode::Delete
                if key_event.modifiers.is_empty()
                    && (mode == InputMode::Browser
                        || matches!(self.selected_form, FormSelection::BrowserPane(_)))
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::DeleteRegionDefinition));
            }
            KeyCode::Char('M')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::WriteRegionMask));
            }
            KeyCode::Char('P')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::PinProbe));
            }
            KeyCode::Char('u')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::RemovePinnedProbe));
            }
            KeyCode::Char('n')
                if key_event.modifiers.is_empty()
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::CyclePinnedProbe {
                    forward: true,
                }));
            }
            KeyCode::Char('N')
                if (key_event.modifiers.is_empty()
                    || key_event.modifiers == KeyModifiers::SHIFT)
                    && mode != InputMode::Edit
                    && self.image_browser_session_state().is_some() =>
            {
                return Some(AppAction::Browser(BrowserAction::CyclePinnedProbe {
                    forward: false,
                }));
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
                return if self.browser_session.is_some() {
                    Some(AppAction::Browser(BrowserAction::CycleView {
                        forward: false,
                    }))
                } else {
                    Some(AppAction::Result(ResultAction::PreviousTab))
                };
            }
            KeyCode::Char(']') if key_event.modifiers.is_empty() && mode != InputMode::Edit => {
                return if self.browser_session.is_some() {
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
            InputMode::PathChooser
            | InputMode::ParameterProfilePath
            | InputMode::ParameterSourceConfirmation => None,
        }
    }

    fn apply_action(&mut self, action: AppAction) {
        if !matches!(action, AppAction::StartRun) {
            self.pending_run_confirmation = false;
        }
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
            AppAction::ToggleNotebookBypass => {
                self.notebook_bypass_once = !self.notebook_bypass_once;
                self.result.status_line = if self.notebook_bypass_once {
                    "Notebook recording will be skipped for the next run only.".into()
                } else {
                    "Notebook recording is enabled for the next run.".into()
                };
                self.result.status_kind = StatusKind::Warning;
            }
            AppAction::CancelSession => self.cancel_current(),
            AppAction::OpenPathChooser => self.open_path_chooser_for_selected_field(),
            AppAction::OpenParameterSources => self.open_parameter_source_picker(),
            AppAction::LoadParameterDefaults => self.load_parameter_defaults(),
            AppAction::LoadParameterLast => self.load_parameter_last(),
            AppAction::RevertParameters => self.revert_parameter_edits(),
            AppAction::ClearSelection => self.clear_output_selection(),
            AppAction::ToggleHelp => self.show_help = !self.show_help,
            AppAction::Parameter(action) => self.apply_parameter_action(action),
            AppAction::Result(action) => self.apply_result_action(action),
            AppAction::Browser(action) => self.apply_browser_action(action),
            AppAction::Edit(action) => self.apply_edit_action(action),
            AppAction::PathChooser(action) => self.apply_path_chooser_action(action),
            AppAction::ParameterProfilePath(action) => {
                self.apply_parameter_profile_path_action(action)
            }
            AppAction::ParameterSourceConfirmation(action) => match action {
                ParameterSourceConfirmationAction::Cancel => {
                    self.cancel_parameter_source_replacement()
                }
                ParameterSourceConfirmationAction::Confirm => {
                    self.confirm_parameter_source_replacement()
                }
            },
        }
    }

    pub(crate) fn handle_key_event(&mut self, key_event: KeyEvent) {
        if self.consume_kitty_protocol_response_key(key_event) {
            return;
        }
        if key_event.code == KeyCode::Char('P')
            && key_event.modifiers == KeyModifiers::SHIFT
            && self.pane_focus == PaneFocus::Result
            && self.app.id == "calibrate"
            && self.promote_latest_workflow_product_from_current_report()
        {
            return;
        }
        if self.choice_picker.is_some() {
            self.handle_choice_picker_key(key_event);
            return;
        }
        if self.browser_mode_picker.is_some() {
            self.handle_browser_mode_picker_key(key_event);
            return;
        }
        let action = self.resolve_key_action(key_event);
        if self.image_movie_active() {
            crate::movie_debug_log(format!(
                "key event code={:?} modifiers={:?} kind={:?} action={:?}",
                key_event.code, key_event.modifiers, key_event.kind, action
            ));
        }
        if movie_input_fully_ignored_for_debug() && self.image_movie_active() {
            return;
        }
        if self.should_stop_image_movie_for_key(key_event, action.as_ref()) {
            crate::movie_debug_log(format!(
                "stop movie due to key event code={:?} modifiers={:?} kind={:?}",
                key_event.code, key_event.modifiers, key_event.kind
            ));
            self.stop_image_movie(
                false,
                format!(
                    "key event code={:?} modifiers={:?} kind={:?}",
                    key_event.code, key_event.modifiers, key_event.kind
                ),
            );
        }
        if let Some(action) = action {
            self.apply_action(action);
        }
    }

    pub(crate) fn handle_paste(&mut self, text: String) {
        let pasted = text.trim_end_matches(['\r', '\n']).to_string();
        if pasted.is_empty() {
            return;
        }

        if let Some(entry) = self.parameter_profile_path_entry.as_mut() {
            entry.buffer.push_str(&pasted);
            entry.error = None;
            entry.replace_existing = false;
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
        let _ = field;
        let _ = self.sync_parameter_from_field(field_index);
    }

    pub(crate) fn handle_mouse_event(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        self.cache_output_layout(layout);
        if self.image_movie_active() {
            crate::movie_debug_log(format!(
                "mouse event kind={:?} column={} row={} modifiers={:?}",
                mouse_event.kind, mouse_event.column, mouse_event.row, mouse_event.modifiers
            ));
        }
        if movie_input_fully_ignored_for_debug() && self.image_movie_active() {
            return;
        }
        if self.parameter_profile_path_entry.is_some()
            || self.pending_parameter_replacement.is_some()
        {
            return;
        }
        if self.choice_picker.is_some() {
            self.handle_choice_picker_mouse(mouse_event, layout);
            return;
        }
        if self.browser_mode_picker.is_some() {
            self.handle_browser_mode_picker_mouse(mouse_event, layout);
            return;
        }
        if self.should_stop_image_movie_for_mouse(mouse_event) {
            crate::movie_debug_log(format!(
                "stop movie due to mouse event kind={:?} column={} row={} modifiers={:?}",
                mouse_event.kind, mouse_event.column, mouse_event.row, mouse_event.modifiers
            ));
            self.stop_image_movie(
                false,
                format!(
                    "mouse event kind={:?} column={} row={} modifiers={:?}",
                    mouse_event.kind, mouse_event.column, mouse_event.row, mouse_event.modifiers
                ),
            );
        }
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
                self.dragging_image_workspace_divider = false;
                self.dragging_image_cursor = false;
                self.dragging_image_profile = false;
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

    fn consume_kitty_protocol_response_key(&mut self, key_event: KeyEvent) -> bool {
        if key_event.kind != KeyEventKind::Press {
            return false;
        }
        if let Some(capture) = self.kitty_response_capture.as_mut() {
            let Some(ch) = kitty_protocol_response_char(key_event) else {
                self.kitty_response_capture = None;
                return false;
            };
            capture.push(ch);
            if matches!(key_event.code, KeyCode::Char('\\'))
                && key_event.modifiers.contains(KeyModifiers::ALT)
            {
                if let Some((image_id, placement_id)) =
                    kitty_protocol_response_image_not_found(capture)
                    && image_id >= crate::KITTY_MOVIE_OVERLAY_IMAGE_ID_BASE
                {
                    self.kitty_movie_store_invalidated = true;
                    crate::movie_debug_log(format!(
                        "kitty movie cache miss image_id={} placement_id={} -> invalidate local stored-image cache",
                        image_id,
                        placement_id.unwrap_or(0)
                    ));
                }
                crate::movie_debug_log(format!("kitty protocol response: {capture}"));
                self.kitty_response_capture = None;
            }
            return true;
        }
        if self.image_browser_session_state().is_some()
            && matches!(key_event.code, KeyCode::Char('_'))
            && key_event.modifiers.contains(KeyModifiers::ALT)
        {
            self.kitty_response_capture = Some("_".to_string());
            return true;
        }
        false
    }

    pub(crate) fn take_kitty_movie_store_invalidated(&mut self) -> bool {
        std::mem::take(&mut self.kitty_movie_store_invalidated)
    }

    #[cfg(test)]
    pub(crate) fn kitty_movie_store_invalidated_for_test(&self) -> bool {
        self.kitty_movie_store_invalidated
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
        &self.app.category
    }

    pub(crate) fn app_name(&self) -> &str {
        &self.app.display_name
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
            if self.image_plane_has_linked_profile() {
                parts.push("s spectrum pane".to_string());
            }
            if self.image_browser_session_state().is_some() {
                parts.push("+/- zoom".to_string());
                parts.push("HJKL pan".to_string());
                parts.push("c map".to_string());
                parts.push("i invert".to_string());
                parts.push("R region".to_string());
                parts.push("S save".to_string());
                parts.push("O load".to_string());
                parts.push("E rename".to_string());
                parts.push("Del delete".to_string());
                parts.push("M mask".to_string());
                parts.push("P pin".to_string());
                parts.push("n/N probe".to_string());
            }
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
            } else if self
                .browser_session()
                .is_some_and(|session| session.kind() == BrowserAppKind::Image)
            {
                parts.push("Esc region/reticle".to_string());
            }
            parts.push("y copy".to_string());
        } else if self.result_tab_uses_plot_workspace() {
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
                parts.push(if self.notebook_bypass_once {
                    "v notebook: skip once".to_string()
                } else {
                    "v notebook: record".to_string()
                });
            } else {
                parts.push("x cancel".to_string());
            }
        }
        if self.parameter_session.is_some() && self.edit_state.is_none() {
            parts.push("^p sources".to_string());
            if self.current_focus_target() == FocusTarget::ParametersPane
                && matches!(self.selected_form, FormSelection::Field(_))
            {
                parts.push("Del reset".to_string());
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
            lines.push("Global: r run  a advanced options  v skip notebook once".to_string());
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
                if self.parameter_session.is_some() {
                    lines.extend([
                        "Sources: Ctrl-P Defaults, Last, Last Successful, Open, Save As, Revert"
                            .to_string(),
                        "Sources: Ctrl-D Defaults  Ctrl-L Last  Ctrl-R Revert".to_string(),
                        "Field: Delete reset to the current default".to_string(),
                    ]);
                }
                match self.selected_form {
                    FormSelection::WorkflowProductAction(_) => {
                        lines.push("Workflow action: Enter opens picker or chooser".to_string());
                    }
                    FormSelection::WorkflowProduct(_) => {
                        lines
                            .push("Workflow product: Shift-P promote into apply chain".to_string());
                    }
                    FormSelection::WorkflowChainEntry(_) => {
                        lines.push("Chain entry: Delete remove  Ctrl-K/Ctrl-J reorder".to_string());
                    }
                    FormSelection::WorkflowChainSetting(_, _) => {
                        lines.push("Chain setting: Enter opens picker".to_string());
                    }
                    _ => {}
                }
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
                } else if self
                    .browser_session()
                    .is_some_and(|session| session.kind() == BrowserAppKind::Image)
                {
                    lines.push("Plane view: g toggle raster/spreadsheet".to_string());
                    lines.push("Plane view: +/- zoom  0 reset view".to_string());
                    lines.push("Plane view: H/J/K/L pan view".to_string());
                    lines.push("Plane view: c cycle colormap  i invert".to_string());
                    lines.push(
                        "Display params: edit stretch/autoscale/clip_low/clip_high in Parameters"
                            .to_string(),
                    );
                    lines.push(
                        "Display stretch: percentile99 percentile95 minmax zscale manual"
                            .to_string(),
                    );
                    lines.push("Display autoscale: per_plane or frozen".to_string());
                    lines.push(
                        "Manual clip: set stretch=manual and both clip fields in image units"
                            .to_string(),
                    );
                    lines.push("Movie params: edit fps in Parameters (default 1)".to_string());
                    lines.push(
                        "Left pane: choose Live, Regions, or Masks with arrows or the mouse"
                            .to_string(),
                    );
                    lines.push(
                        "Regions: R start/add polygon  Enter close  Backspace undo  Esc cancel"
                            .to_string(),
                    );
                    lines.push(
                        "Regions: S save definition  O load next  E rename  Delete remove  M write default mask"
                            .to_string(),
                    );
                    lines.push("Regions: D clear active region".to_string());
                    lines.push(
                        "Masks: Enter set selected default mask  Delete remove selected mask"
                            .to_string(),
                    );
                    lines.push("Regions: polygons are stored in world coordinates".to_string());
                    lines.push("Spectrum view: follows the active plane cursor".to_string());
                    lines.push("Probes: P pin current  n/N cycle pinned  u remove".to_string());
                    lines.push(
                        "Probes: Esc return to the live cursor, then hide/show reticle".to_string(),
                    );
                    if self.image_plane_has_linked_profile() {
                        lines.push("Plane workspace: s collapse/expand spectrum".to_string());
                        lines.push("Plane workspace: drag divider to resize spectrum".to_string());
                        lines.push(
                            "Plane workspace: click chevron to collapse/expand spectrum"
                                .to_string(),
                        );
                    }
                    if self
                        .image_browser_session_state()
                        .is_some_and(|state| state.movie_available())
                    {
                        lines.push("Plane view: m play/pause movie".to_string());
                    }
                    if self.image_raster_plane_active() {
                        lines.push("Raster: click to select active pixel".to_string());
                        lines.push("Raster: mouse wheel zooms the plane".to_string());
                        lines
                            .push("Raster: click a pinned marker to select that probe".to_string());
                    }
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
                    matches!(&session.kind, BrowserSessionKind::Image(state) if !state.snapshot.non_display_axes.is_empty())
                }) {
                    if self
                        .image_browser_session_state()
                        .is_some_and(|state| state.snapshot.non_display_axes.len() > 1)
                    {
                        lines.push("Select non-display axis: Up/Down".to_string());
                    }
                    lines.push("Adjust selected axis: Left/Right".to_string());
                    lines.push("Use Tab to focus this pane, then arrows or h/j/k/l".to_string());
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
        let source = self.parameter_source_label();
        if self.running.is_some() {
            format!(
                "{title} [{}; {source}] locked{}",
                spinner_frames(self.theme_mode())[self.spinner_frame],
                focus
            )
        } else if self.browser_session.is_some() {
            if self.browser_uses_parameter_pane() {
                let mode = self
                    .image_browser_session_state()
                    .map(|state| state.left_pane_mode.label().to_ascii_lowercase())
                    .unwrap_or_else(|| "live".to_string());
                format!("{title} [{mode}; {source}]{focus}")
            } else {
                match self.theme_mode() {
                    ThemeMode::DenseAnsi => format!("Inspector [live]{focus}"),
                    ThemeMode::RichPanel => format!("◈ Inspector [live]{focus}"),
                }
            }
        } else if self.schema_error.is_some() {
            format!("{title} (schema unavailable){focus}")
        } else {
            format!("{title} [{source}]{focus}")
        }
    }

    fn parameter_source_label(&self) -> String {
        let Some(session) = self.parameter_session.as_ref() else {
            return "Unavailable".to_string();
        };
        let mut label = match session.base_source() {
            BaseSource::Defaults => "Defaults".to_string(),
            BaseSource::Last => "Last".to_string(),
            BaseSource::LastSuccessful => "Last Successful".to_string(),
            BaseSource::File(path) => {
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_owned)
                    .unwrap_or_else(|| path.display().to_string());
                format!("File: {name}")
            }
        };
        if session.is_dirty() {
            label.push_str(" *");
        }
        label
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

    pub(crate) fn image_workspace_split_ratio(&self) -> f32 {
        self.config_store.image_workspace_split_ratio()
    }

    pub(crate) fn parameters_pane_collapsed(&self) -> bool {
        self.pane_split_ratio() <= 0.0
    }

    pub(crate) fn image_spectrum_pane_collapsed(&self) -> bool {
        self.image_workspace_split_ratio() >= 1.0
    }

    pub(crate) fn pane_focus(&self) -> PaneFocus {
        self.pane_focus
    }

    pub(crate) fn form_rows(&self) -> Vec<FormRowView> {
        if self.image_browser_session_state().is_some() {
            return self.live_parameter_rows();
        }

        let mut rows = Vec::new();
        for (section_index, section) in self.sections.iter().enumerate() {
            let visible_items = self.visible_section_items(section);
            let context_rows = self.workflow_context_rows_for_section(section);
            let stage_rows = self.workflow_stage_parameter_rows_for_section(section);
            let product_action_rows = self.workflow_product_action_rows_for_section(section);
            let chain_rows = self.workflow_chain_rows_for_section(section);
            let product_rows = self.workflow_product_rows_for_section(section);
            let keep_section = self.keep_empty_workflow_products_section(section);
            let visible_items_empty = visible_items.is_empty();
            let context_rows_empty = context_rows.is_empty();
            let stage_rows_empty = stage_rows.is_empty();
            let product_action_rows_empty = product_action_rows.is_empty();
            let chain_rows_empty = chain_rows.is_empty();
            let product_rows_empty = product_rows.is_empty();
            if visible_items_empty
                && context_rows_empty
                && stage_rows_empty
                && product_action_rows_empty
                && chain_rows_empty
                && product_rows_empty
                && !keep_section
            {
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

            rows.extend(self.render_section_items_for_section(
                section,
                section_index,
                &visible_items,
            ));
            rows.extend(context_rows);
            rows.extend(stage_rows);
            rows.extend(product_action_rows);
            rows.extend(chain_rows);
            rows.extend(product_rows);
            if keep_section
                && visible_items_empty
                && context_rows_empty
                && stage_rows_empty
                && product_action_rows_empty
                && chain_rows_empty
                && product_rows_empty
            {
                rows.push(FormRowView {
                    target: FormSelection::Section(section_index),
                    text: "No chain entries or products yet.".to_string(),
                    kind: FormRowKind::Field,
                    selected: false,
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

    fn render_section_items_for_section(
        &self,
        section: &FormSection,
        section_index: usize,
        visible_items: &[StaticFormItem],
    ) -> Vec<FormRowView> {
        if !(self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Stage Parameters")
        {
            return visible_items
                .iter()
                .copied()
                .map(|item| self.render_section_item(item))
                .collect();
        }

        let mut rows = Vec::new();
        let mut last_group: Option<&str> = None;
        for item in visible_items.iter().copied() {
            if let StaticFormItem::Field(field_index) = item {
                let group = self.workflow_stage_parameter_subsection_title(field_index);
                if last_group != Some(group) {
                    rows.push(FormRowView {
                        target: FormSelection::Section(section_index),
                        text: group.to_string(),
                        kind: FormRowKind::Subsection,
                        selected: false,
                    });
                    last_group = Some(group);
                }
            }
            rows.push(self.render_section_item(item));
        }
        rows
    }

    fn live_parameter_rows(&self) -> Vec<FormRowView> {
        let mut rows = Vec::new();
        for (section_index, section) in self.sections.iter().enumerate() {
            let visible_items = self
                .visible_section_items(section)
                .into_iter()
                .filter(|item| {
                    matches!(
                        item,
                        StaticFormItem::Field(index)
                            if IMEXPLORE_LIVE_PARAMETER_FIELD_IDS
                                .contains(&self.fields[*index].schema.id.as_str())
                    )
                })
                .collect::<Vec<_>>();
            if visible_items.is_empty() {
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

            for item in visible_items {
                rows.push(self.render_section_item(item));
            }
        }
        rows
    }

    fn visible_section_items(&self, section: &FormSection) -> Vec<StaticFormItem> {
        match &section.content {
            FormSectionContent::Items(items) => items
                .iter()
                .copied()
                .filter(|item| match item {
                    StaticFormItem::Field(index) => {
                        self.parameter_session.as_ref().is_some_and(|session| {
                            let field_id = self.fields[*index].schema.id.as_str();
                            session
                                .states()
                                .get(field_id)
                                .is_some_and(|state| state.active)
                                && (!self.is_calibrate_workflow_stage_parameters(section)
                                    || provider_parameter_applies(session, field_id)
                                        .unwrap_or(false))
                        }) && (self.show_advanced || !self.fields[*index].schema.advanced)
                            && !self.workflow_context_owns_field(section, *index)
                    }
                    StaticFormItem::SummaryView(_)
                    | StaticFormItem::BrowserView(_)
                    | StaticFormItem::WorkflowStage(_) => true,
                })
                .collect(),
        }
    }

    fn workflow_context_owns_field(&self, section: &FormSection, field_index: usize) -> bool {
        self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Context"
            && matches!(
                self.fields[field_index].schema.id.as_str(),
                "field" | "refant" | "reference_fields" | "transfer_fields"
            )
    }

    fn workflow_context_rows_for_section(&self, section: &FormSection) -> Vec<FormRowView> {
        if !(self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Context")
        {
            return Vec::new();
        }
        self.workflow_context_displays()
            .into_iter()
            .map(|(kind, display)| FormRowView {
                target: FormSelection::WorkflowContextSetting(kind),
                text: render_workflow_value_display(&display),
                kind: FormRowKind::Field,
                selected: self.selected_form == FormSelection::WorkflowContextSetting(kind),
            })
            .collect()
    }

    fn workflow_context_displays(&self) -> Vec<(WorkflowContextSettingKind, WorkflowValueDisplay)> {
        [
            WorkflowContextSettingKind::ActiveFields,
            WorkflowContextSettingKind::RefAnt,
            WorkflowContextSettingKind::FluxReferenceFields,
            WorkflowContextSettingKind::FluxTransferFields,
        ]
        .into_iter()
        .map(|kind| {
            (
                kind,
                WorkflowValueDisplay {
                    label: kind.label().to_string(),
                    value: self.workflow_context_setting_display_value(kind),
                },
            )
        })
        .collect()
    }

    fn workflow_stage_parameter_rows_for_section(&self, section: &FormSection) -> Vec<FormRowView> {
        if !(self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Stage Parameters")
        {
            return Vec::new();
        }
        let stage = self.current_workflow_stage();
        let mut rows = self
            .workflow_stage_guide_displays(stage)
            .into_iter()
            .map(|(kind, display)| FormRowView {
                target: FormSelection::WorkflowStageGuide(kind),
                text: render_workflow_detail_display(&display),
                kind: FormRowKind::Field,
                selected: self.selected_form == FormSelection::WorkflowStageGuide(kind),
            })
            .collect::<Vec<_>>();
        rows.push(FormRowView {
            target: FormSelection::WorkflowStageAction,
            text: render_workflow_detail_display(&WorkflowDetailDisplay {
                label: "Action".to_string(),
                value: workflow_stage_action_label(stage).to_string(),
                indent: 0,
            }),
            kind: FormRowKind::Field,
            selected: self.selected_form == FormSelection::WorkflowStageAction,
        });
        rows
    }

    fn workflow_stage_parameter_subsection_title(&self, field_index: usize) -> &'static str {
        match self.fields[field_index].schema.group.as_str() {
            "Apply" => "Apply Settings",
            "Output" => "Output",
            "Inspect" => "Inspect",
            "Solve" => "Solve",
            "Solve Gain" => "Gain Solve",
            "Solve Bandpass" => "Bandpass Solve",
            "Fluxscale" => "Fluxscale",
            "Selection" => "Selection",
            "Input" => "Inputs",
            _ => "Parameters",
        }
    }

    fn workflow_stage_guide_displays(
        &self,
        stage: WorkflowStageId,
    ) -> Vec<(WorkflowStageGuideKind, WorkflowDetailDisplay)> {
        [
            (
                WorkflowStageGuideKind::Goal,
                WorkflowDetailDisplay {
                    label: WorkflowStageGuideKind::Goal.label().to_string(),
                    value: workflow_stage_goal(stage).to_string(),
                    indent: 0,
                },
            ),
            (
                WorkflowStageGuideKind::Produces,
                WorkflowDetailDisplay {
                    label: WorkflowStageGuideKind::Produces.label().to_string(),
                    value: workflow_stage_output(stage).to_string(),
                    indent: 0,
                },
            ),
            (
                WorkflowStageGuideKind::Hint,
                WorkflowDetailDisplay {
                    label: WorkflowStageGuideKind::Hint.label().to_string(),
                    value: workflow_stage_hint(stage).to_string(),
                    indent: 0,
                },
            ),
        ]
        .into_iter()
        .collect()
    }

    fn workflow_product_rows_for_section(&self, section: &FormSection) -> Vec<FormRowView> {
        if !(self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Products")
        {
            return Vec::new();
        }
        self.workflow_products
            .iter()
            .enumerate()
            .map(|(index, product)| {
                let display_name = product
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| product.path.display().to_string());
                FormRowView {
                    target: FormSelection::WorkflowProduct(index),
                    text: render_workflow_product_row_display(&WorkflowProductRowDisplay {
                        family: product.family.clone(),
                        revision: product.revision,
                        display_name,
                        stage_label: product.stage.label().to_string(),
                        status_label: product.status.label().to_string(),
                    }),
                    kind: FormRowKind::Field,
                    selected: self.selected_form == FormSelection::WorkflowProduct(index),
                }
            })
            .collect()
    }

    fn workflow_context_setting_display_value(&self, kind: WorkflowContextSettingKind) -> String {
        let mut current =
            self.non_empty_field_text(kind.field_id())
                .unwrap_or_else(|| match kind {
                    WorkflowContextSettingKind::ActiveFields => "<all fields>".to_string(),
                    WorkflowContextSettingKind::RefAnt => "<unset>".to_string(),
                    WorkflowContextSettingKind::FluxReferenceFields
                    | WorkflowContextSettingKind::FluxTransferFields => "<unset>".to_string(),
                });
        if kind == WorkflowContextSettingKind::ActiveFields
            && let Ok(field_id) = current.parse::<usize>()
            && let Some(summary) = self.current_structured_summary()
            && let Some(field) = summary
                .fields
                .iter()
                .find(|field| field.field_id == field_id)
        {
            current = field.name.clone();
        }
        if kind != WorkflowContextSettingKind::RefAnt {
            return current;
        }
        match self.workflow_recommended_refant_name() {
            Some(recommended) if current == "<unset>" => {
                format!("{current}  [suggested {recommended}]")
            }
            Some(recommended) if current == recommended => format!("{current}  [suggested]"),
            Some(recommended) => format!("{current}  [suggested {recommended}]"),
            None => current,
        }
    }

    fn workflow_recommended_refant_name(&self) -> Option<String> {
        let summary = self.current_structured_summary()?;
        summary
            .antennas
            .iter()
            .find(|antenna| !antenna.station.ends_with(":OUT"))
            .or_else(|| summary.antennas.first())
            .map(|antenna| antenna.name.clone())
    }

    fn workflow_product_action_rows_for_section(&self, section: &FormSection) -> Vec<FormRowView> {
        if !(self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Products")
        {
            return Vec::new();
        }
        [
            WorkflowProductActionKind::AddSolvedProduct,
            WorkflowProductActionKind::ImportChainTable,
            WorkflowProductActionKind::ChooseCallibrary,
        ]
        .into_iter()
        .map(|kind| FormRowView {
            target: FormSelection::WorkflowProductAction(kind),
            text: kind.label().to_string(),
            kind: FormRowKind::Field,
            selected: self.selected_form == FormSelection::WorkflowProductAction(kind),
        })
        .collect()
    }

    fn workflow_chain_rows_for_section(&self, section: &FormSection) -> Vec<FormRowView> {
        if !(self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Products")
        {
            return Vec::new();
        }
        let mut rows = Vec::new();
        for (index, entry) in self.workflow_chain_entries().into_iter().enumerate() {
            rows.push(FormRowView {
                target: FormSelection::WorkflowChainEntry(index),
                text: entry.label.clone(),
                kind: FormRowKind::Field,
                selected: self.selected_form == FormSelection::WorkflowChainEntry(index),
            });
            rows.extend(self.workflow_chain_detail_rows(index, &entry));
        }
        rows
    }

    fn workflow_chain_detail_rows(
        &self,
        entry: usize,
        record: &WorkflowChainEntryRecord,
    ) -> Vec<FormRowView> {
        match &record.source {
            WorkflowChainEntrySource::DirectTable => self
                .workflow_chain_setting_records(entry)
                .into_iter()
                .map(|setting| FormRowView {
                    target: FormSelection::WorkflowChainSetting(setting.entry, setting.kind),
                    text: setting.text,
                    kind: FormRowKind::Field,
                    selected: self.selected_form
                        == FormSelection::WorkflowChainSetting(setting.entry, setting.kind),
                })
                .collect(),
            WorkflowChainEntrySource::CallibraryFile { path } => vec![FormRowView {
                target: FormSelection::WorkflowChainEntry(entry),
                text: render_workflow_detail_display(&WorkflowDetailDisplay {
                    label: "source".to_string(),
                    value: path.display().to_string(),
                    indent: 2,
                }),
                kind: FormRowKind::Field,
                selected: false,
            }],
            WorkflowChainEntrySource::CallibrarySpec { spec, .. } => {
                let mut rows = [
                    WorkflowChainSettingKind::Gainfield,
                    WorkflowChainSettingKind::Interp,
                    WorkflowChainSettingKind::Spwmap,
                    WorkflowChainSettingKind::Calwt,
                ]
                .into_iter()
                .map(|kind| FormRowView {
                    target: FormSelection::WorkflowChainSetting(entry, kind),
                    text: render_workflow_detail_display(&WorkflowDetailDisplay {
                        label: kind.label().to_string(),
                        value: workflow_callib_setting_display_value(spec, kind),
                        indent: 2,
                    }),
                    kind: FormRowKind::Field,
                    selected: self.selected_form
                        == FormSelection::WorkflowChainSetting(entry, kind),
                })
                .collect::<Vec<_>>();
                if let Some(apply_to) = workflow_callib_apply_to_row(spec) {
                    rows.push(FormRowView {
                        target: FormSelection::WorkflowChainEntry(entry),
                        text: apply_to,
                        kind: FormRowKind::Field,
                        selected: false,
                    });
                }
                rows
            }
            WorkflowChainEntrySource::CallibraryError { message, .. } => vec![FormRowView {
                target: FormSelection::WorkflowChainEntry(entry),
                text: render_workflow_detail_display(&WorkflowDetailDisplay {
                    label: "error".to_string(),
                    value: message.clone(),
                    indent: 2,
                }),
                kind: FormRowKind::Field,
                selected: false,
            }],
        }
    }

    fn keep_empty_workflow_products_section(&self, section: &FormSection) -> bool {
        self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Products"
    }

    fn render_section_item(&self, item: StaticFormItem) -> FormRowView {
        match item {
            StaticFormItem::Field(field_index) => FormRowView {
                target: FormSelection::Field(field_index),
                text: self.render_field_line(field_index),
                kind: FormRowKind::Field,
                selected: self.selected_form == FormSelection::Field(field_index),
            },
            StaticFormItem::SummaryView(view) => FormRowView {
                target: FormSelection::SummaryView(view),
                text: format!("{:<18} {}", "View", view.label()),
                kind: FormRowKind::Field,
                selected: self.selected_form == FormSelection::SummaryView(view),
            },
            StaticFormItem::BrowserView(view) => FormRowView {
                target: FormSelection::BrowserView(view),
                text: format!("{:<18} {}", "View", view.label()),
                kind: FormRowKind::Field,
                selected: self.selected_form == FormSelection::BrowserView(view),
            },
            StaticFormItem::WorkflowStage(stage) => FormRowView {
                target: FormSelection::WorkflowStage(stage),
                text: self.workflow_stage_row_text(stage),
                kind: FormRowKind::Field,
                selected: self.selected_form == FormSelection::WorkflowStage(stage),
            },
        }
    }

    fn render_field_line(&self, field_index: usize) -> String {
        let Some(field) = self.fields.get(field_index) else {
            return String::new();
        };
        let mut rendered = field.render_line(self.edit_state.as_ref(), field_index);
        if field.is_path() {
            rendered.truncate(rendered.len().saturating_sub(BROWSE_SUFFIX.len()));
        }
        if !field.is_path()
            && self
                .field_picker_entries(field_index)
                .is_some_and(|entries| !entries.is_empty())
        {
            rendered.push_str(" [pick]");
        }
        if self.parameter_edit_errors.contains_key(&field.schema.id) {
            rendered.push_str(" [invalid]");
        } else if let Some(state) = self
            .parameter_session
            .as_ref()
            .and_then(|session| session.states().get(&field.schema.id))
        {
            let origin = match state.origin {
                casa_task_runtime::ParameterOrigin::Default => "default",
                casa_task_runtime::ParameterOrigin::BaseProfile => "base",
                casa_task_runtime::ParameterOrigin::Context => "context",
                casa_task_runtime::ParameterOrigin::Override => "override",
            };
            rendered.push_str(&format!(" [{origin}]"));
        }
        if field.is_path() {
            rendered.push_str(BROWSE_SUFFIX);
        }
        rendered
    }

    fn is_calibrate_workflow_stage_parameters(&self, section: &FormSection) -> bool {
        self.app.shell_kind() == AppShellKind::Workflow
            && self.app.id == "calibrate"
            && section.name == "Stage Parameters"
    }

    pub(crate) fn browser_parameter_summary_lines(&self) -> Vec<String> {
        let Some(state) = self.image_browser_session_state() else {
            return Vec::new();
        };
        match state.left_pane_mode {
            ImageBrowserLeftPaneMode::Live => self.browser_inspector_lines().unwrap_or_default(),
            ImageBrowserLeftPaneMode::Regions => {
                let selected = state
                    .selected_saved_region_name()
                    .map(|name| format!("Selected: {name}"))
                    .unwrap_or_else(|| "Selected: none".to_string());
                let active = state
                    .active_region_definition_name()
                    .map(|name| format!("Loaded: {name}"))
                    .unwrap_or_else(|| "Loaded: none".to_string());
                let mut lines = vec![selected, active];
                if let Some(stats) = state
                    .snapshot
                    .region
                    .as_ref()
                    .and_then(|region| region.stats.as_ref())
                {
                    lines.push(format!("Pixels: {}", stats.pixel_count));
                    lines.push(format!(
                        "Mean: {}",
                        format_numeric_value_with_unit(stats.mean, &stats.value_unit)
                    ));
                    lines.push(format!(
                        "Sigma: {}",
                        format_numeric_value_with_unit(stats.sigma, &stats.value_unit)
                    ));
                    lines.push(format!(
                        "Median: {}",
                        format_numeric_value_with_unit(stats.median, &stats.value_unit)
                    ));
                    lines.push(format!(
                        "Min / Max: {} / {}",
                        format_numeric_value_with_unit(stats.min, &stats.value_unit),
                        format_numeric_value_with_unit(stats.max, &stats.value_unit)
                    ));
                }
                lines.push("Click [ ] to load or clear a saved region".to_string());
                lines.push("Click the name or press E to rename it".to_string());
                lines.push("S save active region".to_string());
                lines.push("Delete remove selected region".to_string());
                lines.push("O cycle next saved region  M write mask".to_string());
                lines
            }
            ImageBrowserLeftPaneMode::Masks => {
                let selected = state
                    .selected_mask_name()
                    .map(|name| format!("Selected: {name}"))
                    .unwrap_or_else(|| "Selected: none".to_string());
                let default = state
                    .snapshot
                    .default_mask_name
                    .as_ref()
                    .map(|name| format!("Default: {name}"))
                    .unwrap_or_else(|| "Default: none".to_string());
                vec![
                    selected,
                    default,
                    "Click [ ] to set or clear the default mask".to_string(),
                    "Delete remove selected mask".to_string(),
                    "M writes a new mask from the active region".to_string(),
                ]
            }
        }
    }

    pub(crate) fn browser_parameter_summary_heading(&self) -> String {
        self.image_browser_session_state()
            .map(|state| state.left_pane_mode.label().to_string())
            .unwrap_or_else(|| "Live".to_string())
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
            match &session.kind {
                BrowserSessionKind::Table(_) => lines.push(format!(
                    "View: {}  Path: {}  Mode: tablebrowser",
                    session.active_tab().label(),
                    session.root_path,
                )),
                BrowserSessionKind::Image(state) => {
                    let mut detail = format!(
                        "View: {}  Path: {}  Mode: imexplore/{}",
                        session.active_tab().label(),
                        session.root_path,
                        state.plane_mode.label(),
                    );
                    if state.movie_available() {
                        detail.push_str(if state.movie.playing {
                            "  Movie: playing"
                        } else {
                            "  Movie: paused"
                        });
                    }
                    lines.push(detail);
                }
            }
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

    fn image_browser_session_state(&self) -> Option<&ImageBrowserSessionState> {
        match &self.browser_session()?.kind {
            BrowserSessionKind::Image(state) => Some(state),
            BrowserSessionKind::Table(_) => None,
        }
    }

    fn image_browser_session_state_mut(&mut self) -> Option<&mut ImageBrowserSessionState> {
        match &mut self.browser_session.as_mut()?.kind {
            BrowserSessionKind::Image(state) => Some(state),
            BrowserSessionKind::Table(_) => None,
        }
    }

    pub(crate) fn image_raster_plane_active(&self) -> bool {
        self.image_browser_session_state()
            .is_some_and(ImageBrowserSessionState::raster_plane_active)
    }

    pub(crate) fn image_movie_active(&self) -> bool {
        self.image_browser_session_state()
            .is_some_and(|state| state.movie.playing)
    }

    pub(crate) fn image_movie_terminal_looping_active(&self) -> bool {
        self.image_browser_session_state().is_some_and(|state| {
            state.movie.playing
                && state.movie.terminal_looping
                && state.raster_plane_active()
                && state.movie_available()
        })
    }

    pub(crate) fn image_movie_direct_overlay_active(&self) -> bool {
        self.image_browser_session_state().is_some_and(|state| {
            state.movie.playing
                && state.movie.direct_overlay
                && state.raster_plane_active()
                && state.movie_available()
        })
    }

    pub(crate) fn image_movie_fps(&self) -> Option<f64> {
        self.image_browser_session_state().and_then(|state| {
            if state.movie.playing {
                Some(state.movie.fps)
            } else {
                None
            }
        })
    }

    pub(crate) fn note_image_plane_presented(&mut self) {
        if self.image_movie_direct_overlay_active() || self.image_movie_scheduler_enabled() {
            return;
        }
        let Some(request_hash) = self
            .image_browser_session_state()
            .and_then(|state| state.panel.as_ref())
            .and_then(|panel| panel.display_key.as_ref())
            .map(hashed_render_request_key)
        else {
            return;
        };
        self.movie_perf.plane_presented(request_hash);
        self.movie_perf.startup_plane_presented(request_hash);
    }

    pub(crate) fn note_image_plane_direct_presented(&mut self, request_hash: u64) {
        self.movie_perf.plane_presented(request_hash);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn note_image_plane_direct_overlay_refresh(
        &mut self,
        request_hash: u64,
        canvas: Rect,
        pixel_size: (u32, u32),
        cache_hit: bool,
        render_duration: Duration,
        upload_duration: Duration,
        show_duration: Duration,
        refresh_duration: Duration,
        uploaded_bytes: usize,
        surface_count: usize,
    ) {
        let Some(context) = ({
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            Some(image_movie_perf_context_from_state(
                state,
                Some(canvas),
                Some(pixel_size),
                Some(request_hash),
            ))
        }) else {
            return;
        };
        self.movie_perf.direct_overlay_refresh_completed(
            request_hash,
            context,
            if cache_hit {
                MovieFrameOutcome::CacheHitRenderedImage
            } else {
                MovieFrameOutcome::CacheMiss
            },
            render_duration,
            upload_duration,
            show_duration,
            refresh_duration,
            uploaded_bytes,
            surface_count,
            cache_hit,
        );
    }

    fn maybe_emit_movie_perf_summary(&mut self) {
        let requested_fps = self.image_movie_fps().unwrap_or(IMAGE_MOVIE_DEFAULT_FPS);
        let pipeline = self
            .image_browser_session_state()
            .and_then(|state| state.movie_scheduler.as_ref())
            .map(image_movie_pipeline_state);
        self.movie_perf
            .maybe_emit_summary(self.image_movie_active(), requested_fps, pipeline);
    }

    pub(crate) fn browser_tabs(&self) -> &'static [BrowserTab] {
        self.browser_session()
            .map(BrowserSession::tabs)
            .unwrap_or(&BrowserTab::TABLE_ALL)
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub(crate) fn active_browser_tab_label(&self) -> Option<&'static str> {
        self.active_browser_tab().map(BrowserTab::label)
    }

    pub(crate) fn active_browser_tab_for_ui(&self) -> Option<BrowserTab> {
        self.active_browser_tab()
    }

    pub(crate) fn active_browser_scroll_metrics(
        &self,
        _viewport_height: u16,
    ) -> Option<(usize, usize)> {
        if !self.browser_result_uses_live_navigation() {
            return None;
        }
        self.browser_session()?.vertical_metrics()
    }

    pub(crate) fn active_browser_hscroll_metrics(
        &self,
        viewport_width: u16,
    ) -> Option<(usize, usize)> {
        if !self.browser_result_uses_live_navigation() {
            return None;
        }
        self.browser_session()?.horizontal_metrics(viewport_width)
    }

    pub(crate) fn browser_inspector_lines(&self) -> Option<Vec<String>> {
        self.browser_session()?.inspector_lines()
    }

    fn browser_main_content_lines(&self) -> Option<Vec<String>> {
        Some(self.browser_session()?.main_content_lines())
    }

    pub(crate) fn sync_browser_viewport(&mut self, width: u16, height: u16, inspector_height: u16) {
        if self.defer_image_browser_resize_during_divider_drag() {
            return;
        }
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

    fn image_plane_font_size(&self) -> (u16, u16) {
        self.image_browser_session_state()
            .and_then(|state| state.panel.as_ref().map(|panel| panel.font_size))
            .unwrap_or_else(|| terminal_picker().font_size())
    }

    fn defer_image_browser_resize_during_divider_drag(&self) -> bool {
        self.dragging_divider
            && self
                .browser_session
                .as_ref()
                .is_some_and(|session| matches!(session.kind, BrowserSessionKind::Image(_)))
    }

    fn defer_image_plane_render_during_divider_drag(&self) -> bool {
        (self.dragging_divider || self.dragging_image_workspace_divider)
            && self.image_raster_plane_active()
    }

    fn defer_image_spectrum_render_during_divider_drag(&self) -> bool {
        (self.dragging_divider || self.dragging_image_workspace_divider)
            && self.image_plane_has_linked_profile()
    }

    pub(crate) fn active_result_content(&self) -> ResultContent {
        match self.active_result_tab {
            ResultTab::Overview => ResultContent::Lines(self.overview_lines()),
            ResultTab::Data => self.data_result_content(),
            ResultTab::Structure => self.browser_structure_result_content(),
            ResultTab::Content => self.browser_content_result_content(),
            ResultTab::Inspector => self.browser_inspector_result_content(),
            ResultTab::Products => ResultContent::Lines(self.products_tab_lines()),
            ResultTab::Diagnostics => ResultContent::Graphic(self.plots_tab_summary()),
            ResultTab::History => ResultContent::Lines(self.history_tab_lines()),
            ResultTab::Observations => match self.current_structured_summary() {
                Some(summary) => ResultContent::Table(build_observations_table(summary)),
                None => {
                    ResultContent::Lines(vec!["No observation table available yet.".to_string()])
                }
            },
            ResultTab::Scans => match self.current_structured_summary() {
                Some(summary) => {
                    ResultContent::Table(build_scans_table(summary, self.listunfl_enabled()))
                }
                None => ResultContent::Lines(vec!["No scan table available yet.".to_string()]),
            },
            ResultTab::Fields => match self.current_structured_summary() {
                Some(summary) => {
                    ResultContent::Table(build_fields_table(summary, self.listunfl_enabled()))
                }
                None => ResultContent::Lines(vec!["No field table available yet.".to_string()]),
            },
            ResultTab::Spws => match self.current_structured_summary() {
                Some(summary) => ResultContent::Table(build_spws_table(summary)),
                None => ResultContent::Lines(vec![
                    "No spectral-window table available yet.".to_string(),
                ]),
            },
            ResultTab::Sources => match self.current_structured_summary() {
                Some(summary) => ResultContent::Table(build_sources_table(summary)),
                None => ResultContent::Lines(vec!["No source table available yet.".to_string()]),
            },
            ResultTab::Antennas => match self.current_structured_summary() {
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

    fn browser_structure_result_content(&self) -> ResultContent {
        if !self.browser_is_active() {
            return ResultContent::Lines(vec![
                "Structure".to_string(),
                "This application does not expose browser structure views.".to_string(),
            ]);
        }
        match self.browser_active_view_result_tab() {
            Some(ResultTab::Structure) => ResultContent::Lines(
                self.browser_main_content_lines()
                    .unwrap_or_else(|| vec!["No structure view available yet.".to_string()]),
            ),
            Some(ResultTab::Content) => ResultContent::Lines(vec![
                "Structure".to_string(),
                format!(
                    "The selected browser view ({}) is content-oriented. Choose a structure view from the left pane.",
                    self.active_browser_tab()
                        .map(BrowserTab::label)
                        .unwrap_or("unknown")
                ),
            ]),
            _ => ResultContent::Lines(vec![
                "Structure".to_string(),
                "Choose a browser view from the left pane to inspect structure.".to_string(),
            ]),
        }
    }

    fn browser_content_result_content(&self) -> ResultContent {
        if !self.browser_is_active() {
            return ResultContent::Lines(vec![
                "Content".to_string(),
                "This application does not expose browser content views.".to_string(),
            ]);
        }
        match self.browser_active_view_result_tab() {
            Some(ResultTab::Content) => ResultContent::Lines(
                self.browser_main_content_lines()
                    .unwrap_or_else(|| vec!["No content view available yet.".to_string()]),
            ),
            Some(ResultTab::Structure) => ResultContent::Lines(vec![
                "Content".to_string(),
                format!(
                    "The selected browser view ({}) is structure-oriented. Choose a content view from the left pane.",
                    self.active_browser_tab()
                        .map(BrowserTab::label)
                        .unwrap_or("unknown")
                ),
            ]),
            _ => ResultContent::Lines(vec![
                "Content".to_string(),
                "Choose a browser view from the left pane to inspect content.".to_string(),
            ]),
        }
    }

    fn browser_inspector_result_content(&self) -> ResultContent {
        if !self.browser_is_active() {
            return ResultContent::Lines(vec![
                "Inspector".to_string(),
                "This application does not expose a browser inspector.".to_string(),
            ]);
        }
        ResultContent::Lines(self.browser_inspector_lines().unwrap_or_else(|| {
            vec![
                "Inspector".to_string(),
                String::new(),
                "No value selected.".to_string(),
            ]
        }))
    }

    fn data_result_content(&self) -> ResultContent {
        match self.selected_summary_view {
            SummaryDataView::Observations => match self.current_structured_summary() {
                Some(summary) => ResultContent::Table(build_observations_table(summary)),
                None => {
                    ResultContent::Lines(vec!["No observation table available yet.".to_string()])
                }
            },
            SummaryDataView::Scans => match self.current_structured_summary() {
                Some(summary) => {
                    ResultContent::Table(build_scans_table(summary, self.listunfl_enabled()))
                }
                None => ResultContent::Lines(vec!["No scan table available yet.".to_string()]),
            },
            SummaryDataView::Fields => match self.current_structured_summary() {
                Some(summary) => {
                    ResultContent::Table(build_fields_table(summary, self.listunfl_enabled()))
                }
                None => ResultContent::Lines(vec!["No field table available yet.".to_string()]),
            },
            SummaryDataView::Spws => match self.current_structured_summary() {
                Some(summary) => ResultContent::Table(build_spws_table(summary)),
                None => ResultContent::Lines(vec![
                    "No spectral-window table available yet.".to_string(),
                ]),
            },
            SummaryDataView::Sources => match self.current_structured_summary() {
                Some(summary) => ResultContent::Table(build_sources_table(summary)),
                None => ResultContent::Lines(vec!["No source table available yet.".to_string()]),
            },
            SummaryDataView::Antennas => match self.current_structured_summary() {
                Some(summary) if self.verbose_enabled() => {
                    ResultContent::Table(build_antennas_table(summary))
                }
                Some(summary) => ResultContent::Lines(build_compact_antenna_lines(summary)),
                None => ResultContent::Lines(vec!["No antenna table available yet.".to_string()]),
            },
        }
    }

    fn history_tab_lines(&self) -> Vec<String> {
        if self.history_entries.is_empty() {
            return vec![
                "History".to_string(),
                "No runs have been recorded in this session yet.".to_string(),
            ];
        }
        let mut lines = vec!["History".to_string()];
        for (index, entry) in self.history_entries.iter().enumerate() {
            lines.push(format!(
                "{}. [{}] {}",
                entry.sequence.max(index + 1),
                entry.status_kind.label(),
                entry.title
            ));
            if let Some(stage) = entry.stage {
                lines.push(format!("   stage={} ({})", stage.label(), stage.key()));
            }
            lines.extend(entry.details.iter().map(|detail| format!("   {detail}")));
        }
        lines
    }

    fn products_tab_lines(&self) -> Vec<String> {
        if self.app.shell_kind() != AppShellKind::Workflow {
            return vec![
                "Products".to_string(),
                "This application does not expose workflow products.".to_string(),
            ];
        }
        render_workflow_artifact_groups(
            &self.workflow_products_display_groups(),
            "No workflow products have been configured or produced yet.",
        )
    }

    fn workflow_products_display_groups(&self) -> Vec<WorkflowArtifactGroupDisplay> {
        if self.app.id == "imager" {
            return self
                .current_imaging_report()
                .map(imaging_products_display_groups)
                .unwrap_or_default();
        }
        workflow_products_display_groups(
            &self.split_csv_field("gaintables"),
            self.non_empty_field_text("callib").as_deref(),
            &self.workflow_products,
        )
    }

    #[cfg(test)]
    pub(crate) fn set_text_value(&mut self, id: &str, value: &str) {
        self.apply_startup_text_value(id, value.to_string())
            .expect("set text value in test");
    }

    #[cfg(test)]
    pub(crate) fn set_text_value_and_apply(&mut self, id: &str, value: &str) {
        let field_index = self
            .fields
            .iter()
            .position(|field| field.schema.id == id)
            .expect("known test field");
        self.apply_startup_text_value(id, value.to_string())
            .expect("valid test parameter");
        self.apply_live_image_view_parameters_if_needed(field_index);
    }

    #[cfg(test)]
    pub(crate) fn set_toggle_value(&mut self, id: &str, value: bool) {
        self.apply_startup_toggle_value(id, value)
            .expect("set toggle value in test");
    }

    #[cfg(test)]
    pub(crate) fn set_toggle_value_and_apply(&mut self, id: &str, value: bool) {
        let field_index = self
            .fields
            .iter()
            .position(|field| field.schema.id == id)
            .expect("known test field");
        self.apply_startup_toggle_value(id, value)
            .expect("valid test parameter");
        self.apply_live_image_view_parameters_if_needed(field_index);
    }

    pub(crate) fn apply_startup_text_value(
        &mut self,
        id: &str,
        value: String,
    ) -> Result<(), String> {
        let field_index = self
            .fields
            .iter()
            .position(|field| field.schema.id == id)
            .ok_or_else(|| format!("unknown startup field {id:?} for {}", self.app.id))?;
        let result = self.fields[field_index].apply_text_value(value);
        if result.is_ok() {
            self.sync_parameter_from_field(field_index)?;
            self.mark_plot_snapshot_dirty();
        }
        result
    }

    pub(crate) fn apply_startup_toggle_value(
        &mut self,
        id: &str,
        value: bool,
    ) -> Result<(), String> {
        let field_index = self
            .fields
            .iter()
            .position(|field| field.schema.id == id)
            .ok_or_else(|| format!("unknown startup field {id:?} for {}", self.app.id))?;
        let result = self.fields[field_index].apply_toggle_value(value);
        if result.is_ok() {
            self.sync_parameter_from_field(field_index)?;
            self.mark_plot_snapshot_dirty();
        }
        result
    }

    fn sync_parameter_from_field(&mut self, field_index: usize) -> Result<(), String> {
        let Some(field) = self.fields.get(field_index) else {
            return Err("parameter field index is out of range".to_string());
        };
        let name = field.schema.id.clone();
        let live_parameter_rollback = (self.pending_live_parameter_rollback.is_none()
            && IMEXPLORE_LIVE_PARAMETER_FIELD_IDS.contains(&name.as_str())
            && self
                .browser_session()
                .is_some_and(|session| session.kind() == BrowserAppKind::Image))
        .then(|| self.parameter_session.clone())
        .flatten();
        let raw = match &field.value {
            FormValue::Toggle(value) => Some(ParameterValue::Bool(*value)),
            FormValue::Text(value) | FormValue::Choice { value, .. } => {
                if value.trim().is_empty() {
                    None
                } else {
                    let session = self
                        .parameter_session
                        .as_ref()
                        .ok_or_else(|| "typed parameter session is unavailable".to_string())?;
                    let binding = session
                        .bundle()
                        .surface
                        .bindings()
                        .iter()
                        .find(|binding| binding.name == name)
                        .ok_or_else(|| format!("unknown parameter {name:?}"))?;
                    let concept = session
                        .bundle()
                        .catalog
                        .concept(&binding.concept)
                        .ok_or_else(|| format!("missing concept for {name}"))?;
                    match crate::parameters_cli::parse_cli_value(value, &concept.value_domain) {
                        Ok(value) => Some(value),
                        Err(error) => {
                            self.parameter_edit_errors
                                .insert(name.clone(), error.clone());
                            return Err(error);
                        }
                    }
                }
            }
        };
        let session = self
            .parameter_session
            .as_mut()
            .ok_or_else(|| "typed parameter session is unavailable".to_string())?;
        let result = match raw {
            Some(value) => session.set(name.clone(), value),
            None => session.reset(&name),
        }
        .map_err(|error| error.to_string());
        match result {
            Ok(()) => {
                if self.pending_live_parameter_rollback.is_none() {
                    self.pending_live_parameter_rollback = live_parameter_rollback;
                }
                self.parameter_edit_errors.remove(&name);
                self.refresh_parameter_field_metadata();
                Ok(())
            }
            Err(error) => {
                self.parameter_edit_errors.insert(name, error.clone());
                Err(error)
            }
        }
    }

    fn refresh_parameter_field_metadata(&mut self) {
        let Some(session) = self.parameter_session.as_ref() else {
            return;
        };
        for field in &mut self.fields {
            if let Some(state) = session.states().get(&field.schema.id) {
                field.schema.required = state.required;
            }
        }
    }

    pub(crate) fn start_run_on_launch(&mut self) {
        if self.is_msexplore_app() {
            self.clear_output_selection();
            self.activate_result_tab(self.default_plot_result_tab());
            self.pane_focus = PaneFocus::Result;
            self.plot_workspace.preview_invalidated = true;
            self.result.status_line = if self.msexplore_form_has_plot_spec() {
                "Opening interactive msexplore preview.".to_string()
            } else {
                "Set a preset or axes to render an interactive preview.".to_string()
            };
            self.result.status_kind = StatusKind::Info;
            return;
        }
        self.start_run();
    }

    #[cfg(test)]
    pub(crate) fn set_active_result_tab(&mut self, tab: ResultTab) {
        self.activate_result_tab(tab);
    }

    fn activate_result_tab(&mut self, tab: ResultTab) {
        self.clear_output_selection();
        self.active_result_tab = tab;
        self.ensure_current_summary_snapshot_if_needed();
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
        if let Some(summary) = self.current_plot_summary() {
            return summary.to_string();
        }
        if self.plot_workspace.snapshot.is_some() {
            return "Select a plot from the catalog to render it.".to_string();
        }
        "Populate the plot workspace to preview plots.".to_string()
    }

    #[cfg(test)]
    pub(crate) fn start_run_for_test(&mut self) {
        self.pending_run_confirmation = self.requires_run_confirmation().unwrap_or(false);
        self.start_run();
    }

    #[cfg(test)]
    pub(crate) fn requires_run_confirmation_for_test(&self) -> Result<bool, String> {
        self.requires_run_confirmation()
    }

    #[cfg(test)]
    pub(crate) fn cancel_for_test(&mut self) {
        self.cancel_current();
    }

    #[cfg(test)]
    fn test_idle_timeout(timeout: Duration, under_tarpaulin: bool) -> Duration {
        if under_tarpaulin {
            timeout.saturating_mul(5)
        } else {
            timeout
        }
    }

    #[cfg(test)]
    fn under_tarpaulin_for_test() -> bool {
        std::env::var_os("TARPAULIN").is_some() || std::env::var_os("LLVM_PROFILE_FILE").is_some()
    }

    #[cfg(test)]
    pub(crate) fn wait_for_idle_for_test(&mut self, timeout: Duration) -> bool {
        let timeout = Self::test_idle_timeout(timeout, Self::under_tarpaulin_for_test());
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
    pub(crate) fn notebook_bypass_once_for_test(&self) -> bool {
        self.notebook_bypass_once
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
    pub(crate) fn field_text_for_test(&self, id: &str) -> Option<String> {
        self.field_text(id)
    }

    #[cfg(test)]
    pub(crate) fn parameter_value_for_test(&self, id: &str) -> Option<&ParameterValue> {
        self.session_parameter_value(id)
    }

    #[cfg(test)]
    pub(crate) fn clear_parameter_session_for_test(&mut self) {
        self.parameter_session = None;
    }

    #[cfg(test)]
    pub(crate) fn structured_for_test(&self) -> Option<&MeasurementSetSummary> {
        self.current_structured_summary()
    }

    #[cfg(test)]
    pub(crate) fn set_calibration_report_for_test(&mut self, report: ManagedCalibrationOutput) {
        let history_entry = Self::calibration_history_entry(&report, self.next_history_sequence());
        self.record_calibration_products(&report, history_entry.sequence);
        self.history_entries.push(history_entry);
        self.result.structured = Some(StructuredResult::Calibration(Box::new(report)));
        self.result.structured_error = None;
        self.active_result_tab = ResultTab::Overview;
        if let Some(report) = self.current_calibration_report().cloned() {
            self.apply_workflow_post_run_guidance(&report);
        } else {
            self.result.status_line = "Execution completed successfully.".to_string();
            self.result.status_kind = StatusKind::Ok;
        }
    }

    #[cfg(test)]
    pub(crate) fn file_output_path_for_test(&self) -> Option<&str> {
        self.result.file_output_path.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn execution_arguments_for_test(&self) -> Result<Vec<OsString>, String> {
        Ok(self.build_execution_plan()?.arguments)
    }

    #[cfg(test)]
    pub(crate) fn execution_stdin_for_test(&self) -> Result<Option<String>, String> {
        Ok(self.build_execution_plan()?.stdin)
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
    pub(crate) fn image_workspace_split_ratio_for_test(&self) -> f32 {
        self.image_workspace_split_ratio()
    }

    #[cfg(test)]
    pub(crate) fn pane_focus_for_test(&self) -> PaneFocus {
        self.pane_focus
    }

    #[cfg(test)]
    pub(crate) fn set_pane_focus_for_test(&mut self, focus: PaneFocus) {
        self.pane_focus = focus;
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
    pub(crate) fn select_form_field_for_test(&mut self, id: &str) -> bool {
        let Some(field_index) = self.fields.iter().position(|field| field.schema.id == id) else {
            return false;
        };
        self.selected_form = FormSelection::Field(field_index);
        true
    }

    #[cfg(test)]
    pub(crate) fn select_workflow_chain_entry_for_test(&mut self, index: usize) -> bool {
        if index >= self.workflow_chain_entries().len() {
            return false;
        }
        self.select_workflow_chain_entry(index);
        true
    }

    #[cfg(test)]
    pub(crate) fn select_workflow_product_for_test(&mut self, index: usize) -> bool {
        if index >= self.workflow_products.len() {
            return false;
        }
        self.select_workflow_product(index);
        true
    }

    #[cfg(test)]
    pub(crate) fn select_workflow_product_action_for_test(
        &mut self,
        kind: WorkflowProductActionKind,
    ) -> bool {
        self.selected_form = FormSelection::WorkflowProductAction(kind);
        true
    }

    #[cfg(test)]
    pub(crate) fn select_workflow_context_setting_for_test(
        &mut self,
        kind: WorkflowContextSettingKind,
    ) -> bool {
        self.selected_form = FormSelection::WorkflowContextSetting(kind);
        true
    }

    #[cfg(test)]
    pub(crate) fn select_workflow_chain_setting_for_test(
        &mut self,
        entry: usize,
        kind: WorkflowChainSettingKind,
    ) -> bool {
        if entry >= self.workflow_chain_entries().len() {
            return false;
        }
        self.selected_form = FormSelection::WorkflowChainSetting(entry, kind);
        true
    }

    #[cfg(test)]
    pub(crate) fn select_workflow_stage_for_test(&mut self, stage: WorkflowStageId) -> bool {
        self.selected_form = FormSelection::WorkflowStage(stage);
        true
    }

    #[cfg(test)]
    pub(crate) fn choice_picker_labels_for_test(&self) -> Vec<String> {
        self.choice_picker_entries()
            .unwrap_or_default()
            .into_iter()
            .map(|(label, _)| label)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn browser_focus_for_test(&self) -> Option<BrowserPaneFocus> {
        self.browser_session.as_ref().map(BrowserSession::focus)
    }

    #[cfg(test)]
    pub(crate) fn prepare_graphics_for_test(&mut self, width: u16, height: u16) {
        let layout = crate::ui::compute_layout(Rect::new(0, 0, width, height), self);
        self.cache_output_layout(&layout);
        self.prepare_graphics(&layout);
    }

    #[cfg(test)]
    pub(crate) fn image_plane_mode_label_for_test(&self) -> Option<&'static str> {
        self.image_browser_session_state()
            .map(|state| state.plane_mode.label())
    }

    #[cfg(test)]
    pub(crate) fn image_browser_snapshot_for_test(&self) -> Option<&ImageBrowserSnapshot> {
        self.image_browser_session_state()
            .map(|state| &state.snapshot)
    }

    #[cfg(test)]
    pub(crate) fn image_plane_image_size_for_test(&self) -> Option<(u32, u32)> {
        self.image_browser_session_state()?
            .panel
            .as_ref()
            .and_then(|panel| panel.image_size)
    }

    #[cfg(test)]
    pub(crate) fn movie_perf_json_path_for_test(&self) -> Option<&Path> {
        self.movie_perf.json_path()
    }

    #[cfg(test)]
    pub(crate) fn movie_perf_log_path_for_test(&self) -> Option<&Path> {
        self.movie_perf.log_path()
    }

    #[cfg(test)]
    pub(crate) fn image_spectrum_image_size_for_test(&self) -> Option<(u32, u32)> {
        self.image_browser_session_state()?
            .spectrum_panel
            .as_ref()
            .and_then(|panel| panel.image_size)
    }

    #[cfg(test)]
    pub(crate) fn clear_image_profile_for_test(&mut self) {
        if let Some(state) = self.image_browser_session_state_mut() {
            state.snapshot.profile = None;
        }
    }

    #[cfg(test)]
    pub(crate) fn seed_image_spectrum_content_for_test(&mut self, image_size: (u32, u32)) {
        if let Some(state) = self.image_browser_session_state_mut() {
            let panel = state
                .spectrum_panel
                .get_or_insert_with(new_image_spectrum_panel_state);
            panel.image_size = Some(image_size);
        }
    }

    #[cfg(test)]
    pub(crate) fn image_plane_font_size_for_test(&self) -> (u16, u16) {
        self.image_plane_font_size()
    }

    #[cfg(test)]
    pub(crate) fn image_movie_playing_for_test(&self) -> bool {
        self.image_movie_active()
    }

    #[cfg(test)]
    pub(crate) fn image_movie_configuration_for_test(&self) -> Option<(f64, bool)> {
        self.image_browser_session_state()
            .map(|state| (state.movie.fps, state.movie.looping))
    }

    #[cfg(test)]
    pub(crate) fn image_colormap_for_test(&self) -> Option<&'static str> {
        self.image_browser_session_state()
            .map(|state| image_colormap_parameter(state.plane_colormap))
    }

    #[cfg(test)]
    pub(crate) fn image_movie_axis_for_test(&self) -> Option<usize> {
        self.image_browser_session_state()?
            .selected_non_display_axis_state()
            .map(|axis| axis.axis)
    }

    #[cfg(test)]
    pub(crate) fn key_event_stops_movie_for_test(&self, key_event: KeyEvent) -> bool {
        let action = self.resolve_key_action(key_event);
        self.should_stop_image_movie_for_key(key_event, action.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn image_live_reticle_visible_for_test(&self) -> bool {
        self.image_browser_session_state()
            .map(|state| state.show_live_reticle)
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(crate) fn image_plane_cursor_sample_for_test(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
    ) -> Option<(usize, usize)> {
        self.current_image_plane_render_request(layout, font_size)
            .and_then(|request| request.input.cursor_sample)
    }

    #[cfg(test)]
    pub(crate) fn image_plane_request_key_hash_for_test(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
    ) -> Option<u64> {
        self.current_image_plane_render_request(layout, font_size)
            .map(|request| hashed_render_request_key(&request.request_key))
    }

    #[cfg(test)]
    pub(crate) fn image_plane_render_pixels_for_test(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
    ) -> Option<(u32, u32)> {
        self.current_image_plane_render_request(layout, font_size)
            .map(|request| (request.pixel_width, request.pixel_height))
    }

    #[cfg(test)]
    pub(crate) fn image_direct_plane_render_pixels_for_test(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
    ) -> Option<(u32, u32)> {
        self.current_image_direct_plane_render_request(layout, font_size)
            .map(|request| (request.pixel_width, request.pixel_height))
    }

    #[cfg(test)]
    pub(crate) fn image_spectrum_request_key_hash_for_test(
        &self,
        layout: &UiLayout,
    ) -> Option<u64> {
        let theme_mode = self.theme_mode();
        let split_ratio = self.image_workspace_split_ratio();
        let state = self.image_browser_session_state()?;
        let overlay_profiles = image_spectrum_overlay_series(state);
        let font_size = state
            .spectrum_panel
            .as_ref()
            .map(|panel| panel.font_size)
            .unwrap_or_else(|| terminal_picker().font_size());
        self.current_image_spectrum_render_request(
            layout,
            font_size,
            &state.snapshot,
            ImageSpectrumRenderRequestOptions {
                overlay_profiles: &overlay_profiles,
                split_ratio,
                theme_mode,
                render_scale: 1.0,
                max_pixel_size: None,
            },
        )
        .map(|request| hashed_render_request_key(&request.request_key))
    }

    #[cfg(test)]
    pub(crate) fn image_spectrum_render_pixels_for_test(
        &self,
        layout: &UiLayout,
    ) -> Option<(u32, u32)> {
        let theme_mode = self.theme_mode();
        let split_ratio = self.image_workspace_split_ratio();
        let state = self.image_browser_session_state()?;
        let overlay_profiles = image_spectrum_overlay_series(state);
        let font_size = state
            .spectrum_panel
            .as_ref()
            .map(|panel| panel.font_size)
            .unwrap_or_else(|| terminal_picker().font_size());
        self.current_image_spectrum_render_request(
            layout,
            font_size,
            &state.snapshot,
            ImageSpectrumRenderRequestOptions {
                overlay_profiles: &overlay_profiles,
                split_ratio,
                theme_mode,
                render_scale: 1.0,
                max_pixel_size: None,
            },
        )
        .map(|request| (request.pixel_width, request.pixel_height))
    }

    #[cfg(test)]
    pub(crate) fn image_plane_label_for_test(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
    ) -> Option<String> {
        self.current_image_plane_render_request(layout, font_size)
            .and_then(|request| request.input.plane_label)
    }

    #[cfg(test)]
    pub(crate) fn image_plane_invert_for_test(&self) -> Option<bool> {
        self.image_browser_session_state()
            .map(|state| state.plane_invert)
    }

    #[cfg(test)]
    pub(crate) fn image_pinned_probe_labels_for_test(&self) -> Vec<String> {
        self.image_browser_session_state()
            .map(|state| {
                state
                    .pinned_probes
                    .iter()
                    .map(|probe| probe.label.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn selected_image_pinned_probe_label_for_test(&self) -> Option<String> {
        self.image_browser_session_state().and_then(|state| {
            state
                .selected_pinned_probe()
                .map(|probe| probe.label.clone())
        })
    }

    pub(crate) fn cache_output_layout(&mut self, layout: &UiLayout) {
        let next_result = result_text_area(layout);
        let next_left = left_output_area(self, layout);
        self.cached_browser_viewport_cells = Some((
            layout.result_content.width,
            layout.result_content.height,
            layout.form_inner.height,
        ));
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

    fn browser_startup_viewport_cells(&self) -> (u16, u16, u16) {
        self.cached_browser_viewport_cells.unwrap_or((120, 24, 0))
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

    fn image_movie_scheduler_enabled(&self) -> bool {
        self.image_browser_session_state().is_some_and(|state| {
            state.movie.playing
                && !state.movie.terminal_looping
                && !state.movie.direct_overlay
                && state.raster_plane_active()
                && state.movie_available()
        })
    }

    fn ensure_image_plane_panel_state(&mut self) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        if state.panel.is_none() {
            state.panel = Some(new_image_plane_panel_state());
        }
    }

    fn ensure_image_spectrum_panel_state(&mut self) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        if state.spectrum_panel.is_none() {
            state.spectrum_panel = Some(new_image_spectrum_panel_state());
        }
    }

    fn clear_image_movie_panel_overrides(&mut self) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        if let Some(panel) = state.panel.as_mut() {
            panel.movie_protocol = None;
            panel.movie_display_key = None;
            panel.movie_image_size = None;
        }
        if let Some(panel) = state.spectrum_panel.as_mut() {
            panel.movie_protocol = None;
            panel.movie_display_key = None;
            panel.movie_image_size = None;
        }
    }

    fn current_image_movie_scheduler_spec(
        &mut self,
        layout: &UiLayout,
    ) -> Option<ImageMovieSchedulerSpec> {
        if !self.image_movie_scheduler_enabled() {
            return None;
        }
        self.ensure_image_plane_panel_state();
        let theme_mode = self.theme_mode();
        let split_ratio = self.image_workspace_split_ratio();
        let spectrum_visible = {
            let state = self.image_browser_session_state()?;
            state.snapshot.profile.is_some()
                && split_ratio < 0.999
                && crate::ui::image_spectrum_canvas_area(layout, true, split_ratio)
                    .is_some_and(|area| !area.is_empty())
        };
        if spectrum_visible {
            self.ensure_image_spectrum_panel_state();
        }
        let state = self.image_browser_session_state()?;
        let axis_state = state.selected_non_display_axis_state()?;
        let plane_font_size = state.panel.as_ref()?.font_size;
        let spectrum_font_size = if spectrum_visible {
            Some(state.spectrum_panel.as_ref()?.font_size)
        } else {
            None
        };
        let axis_length = axis_state.length.max(1);
        let session_indices = state
            .movie_scheduler
            .as_ref()
            .map(|scheduler| scheduler.session_indices.clone())
            .unwrap_or_else(|| {
                state
                    .snapshot
                    .non_display_axes
                    .iter()
                    .map(|axis| (axis.axis, axis.index))
                    .collect()
            });
        Some(ImageMovieSchedulerSpec {
            content_signature: image_movie_content_signature(
                state,
                layout,
                theme_mode,
                split_ratio,
                plane_font_size,
                spectrum_font_size,
                spectrum_visible,
            ),
            movie_axis: axis_state.axis,
            axis_length,
            next_due_index: (axis_state.index + 1) % axis_length,
            requested_fps: state.movie.fps,
            theme_mode,
            split_ratio,
            viewport: state.viewport,
            snapshot: state.snapshot.clone(),
            parameters: state.snapshot.parameters.clone(),
            plane_content_mode: match state.plane_mode {
                ImagePlaneMode::Raster => ImagePlaneContentMode::Raster,
                ImagePlaneMode::Spreadsheet => ImagePlaneContentMode::Spreadsheet,
            },
            show_live_reticle: state.show_live_reticle,
            plane_colormap: state.plane_colormap,
            plane_invert: state.plane_invert,
            pinned_probes: state.pinned_probes.clone(),
            plane_font_size,
            spectrum_font_size,
            spectrum_visible,
            session_indices,
        })
    }

    fn build_movie_prepared_bundle(
        &mut self,
        layout: &UiLayout,
        spec: &ImageMovieSchedulerSpec,
        occurrence: MovieOccurrenceKey,
        snapshot: ImageBrowserSnapshot,
    ) -> Option<PreparedMovieBundle> {
        let overlay_markers =
            image_plane_overlay_markers_for_snapshot(&snapshot, &spec.pinned_probes);
        let plane_request = self.image_plane_render_request_for_snapshot(
            layout,
            spec.plane_font_size,
            &snapshot,
            ImagePlaneRenderRequestOptions {
                show_live_reticle: spec.show_live_reticle,
                colormap: spec.plane_colormap,
                invert: spec.plane_invert,
                overlay_markers: &overlay_markers,
                split_ratio: spec.split_ratio,
                theme_mode: spec.theme_mode,
                render_scale: 1.0,
                max_pixel_size: None,
            },
        )?;
        let overlay_profiles = image_spectrum_overlay_series_for_pinned(&spec.pinned_probes);
        let spectrum_request = if spec.spectrum_visible {
            self.current_image_spectrum_render_request(
                layout,
                spec.spectrum_font_size?,
                &snapshot,
                ImageSpectrumRenderRequestOptions {
                    overlay_profiles: &overlay_profiles,
                    split_ratio: spec.split_ratio,
                    theme_mode: spec.theme_mode,
                    render_scale: 1.0,
                    max_pixel_size: None,
                },
            )
        } else {
            None
        };
        let mut surface_requests = vec![ImageMovieSurfaceRequest {
            kind: ImageMovieSurfaceKind::Plane,
            request_hash: hashed_render_request_key(&plane_request.request_key),
            cell_size: (
                plane_request.request_key.area.width,
                plane_request.request_key.area.height,
            ),
            pixel_size: (plane_request.pixel_width, plane_request.pixel_height),
            payload: DirectImageMovieSurfacePayload::Plane(plane_request.input.clone()),
        }];
        if let Some(request) = spectrum_request.as_ref() {
            surface_requests.push(ImageMovieSurfaceRequest {
                kind: ImageMovieSurfaceKind::Spectrum,
                request_hash: hashed_render_request_key(&request.request_key),
                cell_size: (
                    request.request_key.area.width,
                    request.request_key.area.height,
                ),
                pixel_size: (request.pixel_width, request.pixel_height),
                payload: DirectImageMovieSurfacePayload::Spectrum(request.input.clone()),
            });
        }
        let prepared = {
            let state = self.image_browser_session_state_mut()?;
            state
                .direct_movie_engine
                .prepare_bundle(&ImageMovieBundleRequest {
                    occurrence: ImageMovieOccurrence {
                        generation: occurrence.generation,
                        movie_key: spec.content_signature,
                        axis: occurrence.movie_axis,
                        axis_index: occurrence.axis_index,
                        axis_length: spec.axis_length,
                    },
                    requested_fps: spec.requested_fps,
                    surfaces: surface_requests,
                })
        };
        let (rendered, cache_hit) =
            {
                let state = self.image_browser_session_state_mut()?;
                state.direct_movie_engine.render_or_get_cached(
                &prepared,
                &mut |_, pixel_size: (u32, u32), payload: &DirectImageMovieSurfacePayload| {
                    match payload {
                        DirectImageMovieSurfacePayload::Plane(input) => {
                            render_image_plane_image(pixel_size.0, pixel_size.1, input)
                                .map(|image| image.to_rgb8())
                        }
                        DirectImageMovieSurfacePayload::Spectrum(input) => {
                            render_image_spectrum_image(pixel_size.0, pixel_size.1, input)
                                .map(|image| image.to_rgb8())
                        }
                    }
                },
            )
            .ok()?
            };
        Some(PreparedMovieBundle {
            occurrence,
            snapshot,
            plane_request,
            spectrum_request,
            rendered,
            cache_hit,
        })
    }

    fn apply_prepared_movie_presentation(&mut self, presentation: PreparedMoviePresentation) {
        let PreparedMoviePresentation {
            occurrence: _,
            snapshot,
            plane_request,
            plane_protocol,
            plane_image_size,
            spectrum_request,
            spectrum_protocol,
            spectrum_image_size,
        } = presentation;
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        let Some(panel) = state.panel.as_mut() else {
            return;
        };
        panel.movie_protocol = Some(plane_protocol);
        panel.movie_display_key = Some(plane_request.request_key.clone());
        panel.movie_image_size = Some(plane_image_size);
        panel.display_key = Some(plane_request.request_key.clone());
        panel.pending_request_key = None;
        panel.last_error = None;
        if let Some(panel) = state.spectrum_panel.as_mut() {
            if let (Some(request), Some(protocol), Some(image_size)) = (
                spectrum_request.as_ref(),
                spectrum_protocol,
                spectrum_image_size,
            ) {
                panel.movie_protocol = Some(protocol);
                panel.movie_display_key = Some(request.request_key.clone());
                panel.movie_image_size = Some(image_size);
                panel.display_key = Some(request.request_key.clone());
                panel.pending_request_key = None;
                panel.last_error = None;
            } else {
                panel.movie_protocol = None;
                panel.movie_display_key = None;
                panel.movie_image_size = None;
            }
        }
        state.snapshot = snapshot;
        state.clamp_selected_non_display_axis();
        state.hscroll = state.hscroll.min(
            image_browser_max_hscroll(&state.snapshot, state.viewport.width).min(u16::MAX as usize)
                as u16,
        );
    }

    pub(crate) fn prepare_graphics(&mut self, layout: &UiLayout) {
        if self.result_tab_uses_plot_workspace() {
            self.ensure_plot_requested(layout);
        }
        if self.defer_image_plane_render_during_divider_drag() {
            return;
        }
        if self.image_movie_scheduler_enabled() {
            self.prepare_image_movie(layout);
            return;
        }
        self.ensure_image_plane_requested(layout);
        self.ensure_image_spectrum_requested(layout);
    }

    fn prepare_image_movie(&mut self, layout: &UiLayout) {
        let Some(spec) = self.current_image_movie_scheduler_spec(layout) else {
            return;
        };
        let scheduler_context = self
            .image_browser_session_state()
            .map(|state| image_movie_perf_context_from_state(state, None, None, None))
            .unwrap_or_default();
        let mut invalidated = false;
        {
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            let scheduler = state.movie_scheduler.get_or_insert_with(|| {
                ImageMovieSchedulerState::new(
                    spec.content_signature,
                    spec.movie_axis,
                    spec.next_due_index,
                    spec.requested_fps,
                    spec.session_indices.clone(),
                )
            });
            if scheduler.content_signature != spec.content_signature
                || scheduler.movie_axis != spec.movie_axis
            {
                scheduler.invalidate(
                    spec.content_signature,
                    spec.movie_axis,
                    spec.next_due_index,
                    spec.requested_fps,
                    spec.session_indices.clone(),
                );
                invalidated = true;
            } else {
                scheduler.session_indices = spec.session_indices.clone();
            }
        }
        if invalidated {
            self.clear_image_movie_panel_overrides();
            let pipeline = self
                .image_browser_session_state()
                .and_then(|state| state.movie_scheduler.as_ref())
                .map(image_movie_pipeline_state);
            self.movie_perf.generation_invalidated(
                scheduler_context,
                "movie content changed",
                pipeline,
            );
        }

        self.queue_image_movie_presentations(layout, &spec);

        let protocol_drained = {
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            let Some(scheduler) = state.movie_scheduler.as_mut() else {
                return;
            };
            scheduler.protocol_pool.drain_ready(scheduler.generation)
        };

        if protocol_drained.stale_count > 0 {
            for _ in 0..protocol_drained.stale_count {
                self.movie_perf.frame_dropped(
                    None,
                    scheduler_context,
                    MovieFrameOutcome::StaleRenderDiscarded,
                    "movie protocol pool discarded stale completion",
                );
            }
        }

        for error in protocol_drained.errors {
            self.result.status_line = "Movie frame present prep failed.".into();
            self.result.status_kind = StatusKind::Warning;
            self.result.stderr = format!("{error}\n");
        }

        for ready in protocol_drained.ready {
            let presentation = ready.output;
            let request_hash = hashed_render_request_key(&presentation.plane_request.request_key);
            let (queue_depth, pipeline) = self
                .image_browser_session_state()
                .and_then(|state| state.movie_scheduler.as_ref())
                .map(|scheduler| {
                    (
                        scheduler.protocol_pool.queue_depth(),
                        Some(image_movie_pipeline_state(scheduler)),
                    )
                })
                .unwrap_or((0, None));
            self.movie_perf
                .bundle_ready(request_hash, queue_depth, pipeline);
            if let Some(state) = self.image_browser_session_state_mut()
                && let Some(scheduler) = state.movie_scheduler.as_mut()
            {
                scheduler
                    .presentations
                    .clear_in_flight(presentation.occurrence.axis_index);
                scheduler
                    .presentations
                    .mark_ready(presentation.occurrence.axis_index, presentation);
            }
        }

        let now = Instant::now();
        let mut due_presentation = None;
        let mut deadline_miss_note = None;
        if let Some(state) = self.image_browser_session_state_mut()
            && let Some(scheduler) = state.movie_scheduler.as_mut()
        {
            match scheduler.presentations.poll_due(
                now,
                state.movie.frame_interval,
                spec.axis_length,
            ) {
                ImageMoviePresentationPoll::Ready(presentation) => {
                    due_presentation = Some(presentation);
                }
                ImageMoviePresentationPoll::Missed { axis_index } => {
                    deadline_miss_note = Some(format!(
                        "movie occurrence {} missed deadline waiting for ready presentation",
                        axis_index
                    ));
                }
                ImageMoviePresentationPoll::NotDue => {}
            }
        }
        if let Some(note) = deadline_miss_note {
            let (queue_depth, pipeline) = self
                .image_browser_session_state()
                .and_then(|state| state.movie_scheduler.as_ref())
                .map(|scheduler| {
                    (
                        scheduler.protocol_pool.queue_depth(),
                        Some(image_movie_pipeline_state(scheduler)),
                    )
                })
                .unwrap_or((0, None));
            self.movie_perf
                .deadline_missed(scheduler_context, note, queue_depth, pipeline);
        }

        if let Some(presentation) = due_presentation {
            let request_hash = hashed_render_request_key(&presentation.plane_request.request_key);
            self.apply_prepared_movie_presentation(presentation);
            let pipeline = self
                .image_browser_session_state()
                .and_then(|state| state.movie_scheduler.as_ref())
                .map(image_movie_pipeline_state);
            self.movie_perf.bundle_presented(request_hash, pipeline);
        }
    }

    fn queue_image_movie_presentations(
        &mut self,
        layout: &UiLayout,
        spec: &ImageMovieSchedulerSpec,
    ) {
        let (render_worker_count, protocol_worker_count) = self
            .image_browser_session_state()
            .and_then(|state| state.movie_scheduler.as_ref())
            .map(|scheduler| {
                (
                    image_movie_render_worker_count(),
                    scheduler.protocol_pool.worker_count(),
                )
            })
            .unwrap_or((
                image_movie_render_worker_count(),
                image_movie_render_worker_count(),
            ));
        let lookahead_target = image_movie_lookahead_occurrences(
            spec.requested_fps,
            spec.axis_length,
            render_worker_count,
            protocol_worker_count,
        );
        for offset in 0..lookahead_target {
            let (generation, axis_index) = {
                let Some(state) = self.image_browser_session_state_mut() else {
                    return;
                };
                let Some(scheduler) = state.movie_scheduler.as_mut() else {
                    return;
                };
                let axis_index = scheduler
                    .presentations
                    .next_axis_index_for_offset(offset, spec.axis_length);
                if scheduler.presentations.contains_ready(axis_index)
                    || scheduler.presentations.contains_in_flight(axis_index)
                {
                    continue;
                }
                (scheduler.generation, axis_index)
            };
            let preview_request = build_image_movie_preview_request(spec, axis_index);
            let preview = {
                let Some(state) = self.image_browser_session_state_mut() else {
                    return;
                };
                state
                    .client
                    .request_preview(ImageBrowserCommand::PreviewOccurrence {
                        request: preview_request,
                    })
            };
            let preview = match preview {
                Ok(preview) => preview,
                Err(error) => {
                    self.stop_image_movie(
                        false,
                        format!("preview request failed: {}", error.message()),
                    );
                    self.report_browser_error(
                        "Movie preview failed.",
                        format!("{}\n", error.message()),
                    );
                    return;
                }
            };
            let occurrence = MovieOccurrenceKey {
                generation,
                movie_axis: spec.movie_axis,
                axis_index,
            };
            let Some(bundle) =
                self.build_movie_prepared_bundle(layout, spec, occurrence, *preview.snapshot)
            else {
                continue;
            };
            let request_hash = hashed_render_request_key(&bundle.plane_request.request_key);
            let context = self
                .image_browser_session_state()
                .map(|state| {
                    image_movie_perf_context_from_snapshot(
                        state,
                        &bundle.snapshot,
                        Some(request_hash),
                    )
                })
                .unwrap_or_default();
            let frame_seq = self.movie_perf.begin_frame(context);
            let backend_timing = bundle
                .snapshot
                .backend_timing
                .as_ref()
                .map(map_backend_timing);
            if let Some(frame_seq) = frame_seq {
                let (queue_depth, pipeline) = self
                    .image_browser_session_state()
                    .and_then(|state| state.movie_scheduler.as_ref())
                    .map(|scheduler| {
                        (
                            scheduler.protocol_pool.queue_depth(),
                            Some(image_movie_pipeline_state(scheduler)),
                        )
                    })
                    .unwrap_or((0, None));
                self.movie_perf
                    .preview_requested(frame_seq, context, queue_depth, pipeline);
                self.movie_perf
                    .preview_received(frame_seq, context, backend_timing, pipeline);
                self.movie_perf.bundle_render_requested(
                    frame_seq,
                    request_hash,
                    context,
                    queue_depth,
                    if bundle.cache_hit {
                        MovieFrameOutcome::CacheHitRenderedImage
                    } else {
                        map_backend_plane_outcome(bundle.snapshot.backend_timing.as_ref())
                    },
                    pipeline,
                );
            }

            let (plane_picker, spectrum_picker) = {
                let Some(state) = self.image_browser_session_state() else {
                    return;
                };
                let Some(plane_panel) = state.panel.as_ref() else {
                    return;
                };
                (
                    plane_panel.picker.clone(),
                    if bundle.spectrum_request.is_some() {
                        state
                            .spectrum_panel
                            .as_ref()
                            .map(|panel| panel.picker.clone())
                    } else {
                        None
                    },
                )
            };

            let submit_result = {
                let Some(state) = self.image_browser_session_state_mut() else {
                    return;
                };
                let Some(scheduler) = state.movie_scheduler.as_mut() else {
                    return;
                };
                scheduler.protocol_pool.submit(
                    generation,
                    hashed_movie_occurrence_key(occurrence),
                    MovieProtocolRenderJob {
                        bundle,
                        plane_picker,
                        spectrum_picker,
                    },
                )
            };

            match submit_result {
                Ok(_) => {
                    if let Some(state) = self.image_browser_session_state_mut()
                        && let Some(scheduler) = state.movie_scheduler.as_mut()
                    {
                        scheduler.presentations.mark_in_flight(axis_index);
                    }
                }
                Err(error) => {
                    self.movie_perf.frame_dropped(
                        frame_seq,
                        context,
                        MovieFrameOutcome::SkippedDueToPending,
                        format!("movie protocol submit failed: {error}"),
                    );
                    self.result.status_line = "Movie frame protocol prep failed.".into();
                    self.result.status_kind = StatusKind::Warning;
                    self.result.stderr = format!("{error}\n");
                    return;
                }
            }
        }
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
                if self.browser_result_uses_live_navigation() {
                    if self.image_raster_plane_active() {
                        return None;
                    }
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
        if state.raster_plane_active() {
            let canvas = crate::ui::image_plane_canvas_area_for_browser(
                layout,
                state.spectrum_workspace_visible(),
                self.image_workspace_split_ratio(),
            );
            return image_raster_click_target(state, column, row, canvas);
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

        let column = chunk_offset / stride;
        let max_x = image_plane_column_count(&state.snapshot)?;
        if column >= max_x {
            return None;
        }
        let pixel_x = image_plane_header_pixel(&state.snapshot, column)?;

        Some((pixel_x, pixel_y))
    }

    fn image_workspace_divider_toggle_hit(&self, column: u16, row: u16, layout: &UiLayout) -> bool {
        let Some(session) = self.browser_session() else {
            return false;
        };
        let BrowserSessionKind::Image(state) = &session.kind else {
            return false;
        };
        if !state.spectrum_workspace_visible() {
            return false;
        }
        crate::ui::image_workspace_divider_toggle_area(
            layout,
            true,
            self.image_workspace_split_ratio(),
        )
        .is_some_and(|rect| rect_contains(rect, column, row))
    }

    fn image_workspace_divider_hit(&self, column: u16, row: u16, layout: &UiLayout) -> bool {
        let Some(session) = self.browser_session() else {
            return false;
        };
        let BrowserSessionKind::Image(state) = &session.kind else {
            return false;
        };
        if !state.spectrum_workspace_visible() {
            return false;
        }
        crate::ui::image_workspace_divider_area(layout, true, self.image_workspace_split_ratio())
            .is_some_and(|rect| rect_contains(rect, column, row))
    }

    fn image_workspace_split_ratio_from_mouse(&self, row: u16, layout: &UiLayout) -> Option<f32> {
        let BrowserSessionKind::Image(state) = &self.browser_session()?.kind else {
            return None;
        };
        if !state.spectrum_workspace_visible() {
            return None;
        }
        let area = layout.result_content;
        if area.height < 4 {
            return None;
        }
        let available_canvas = area.height.saturating_sub(3);
        if available_canvas < 3 {
            return None;
        }
        let plane_canvas_height = row.saturating_sub(area.y.saturating_add(2));
        if plane_canvas_height >= available_canvas.saturating_sub(1) {
            return Some(1.0);
        }
        Some(f32::from(plane_canvas_height) / f32::from(available_canvas.max(1)))
    }

    fn image_raster_plane_canvas_hit(&self, column: u16, row: u16, layout: &UiLayout) -> bool {
        let Some(session) = self.browser_session() else {
            return false;
        };
        let BrowserSessionKind::Image(state) = &session.kind else {
            return false;
        };
        if !state.raster_plane_active() {
            return false;
        }
        let canvas = crate::ui::image_plane_canvas_area_for_browser(
            layout,
            state.spectrum_workspace_visible(),
            self.image_workspace_split_ratio(),
        );
        rect_contains(canvas, column, row)
    }

    fn image_spectrum_click_target(
        &self,
        column: u16,
        row: u16,
        layout: &UiLayout,
    ) -> Option<(usize, i32)> {
        let BrowserSessionKind::Image(state) = &self.browser_session()?.kind else {
            return None;
        };
        if !state.spectrum_workspace_visible() {
            return None;
        }
        let spectrum_area = crate::ui::image_spectrum_canvas_area(
            layout,
            state.spectrum_workspace_visible(),
            self.image_workspace_split_ratio(),
        )?;
        let plot_rect = image_spectrum_plot_rect(
            spectrum_area,
            state
                .spectrum_panel
                .as_ref()
                .map(|panel| panel.font_size)
                .unwrap_or((1, 1)),
        )?;
        if !rect_contains(plot_rect, column, row) {
            return None;
        }
        let profile = state.snapshot.profile.as_ref()?;
        let axis_state = state
            .snapshot
            .non_display_axes
            .iter()
            .find(|axis| axis.axis == profile.axis)
            .or_else(|| state.selected_non_display_axis_state())?;
        if profile.samples.is_empty() {
            return None;
        }
        let relative_x = usize::from(column.saturating_sub(plot_rect.x));
        let target_index = image_click_sample_index(
            relative_x,
            usize::from(plot_rect.width.max(1)),
            profile.samples.len(),
        );
        let delta = target_index as i32 - axis_state.index as i32;
        Some((axis_state.axis, delta))
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
            ParameterAction::PromoteWorkflowProduct => self.promote_selected_workflow_product(),
            ParameterAction::Delete => self.delete_selected_parameter_item(),
            ParameterAction::MoveUp => self.move_selected_workflow_chain_entry(false),
            ParameterAction::MoveDown => self.move_selected_workflow_chain_entry(true),
        }
    }

    fn delete_selected_parameter_item(&mut self) {
        if let FormSelection::WorkflowChainEntry(index) = self.selected_form {
            self.remove_workflow_chain_entry(index);
            return;
        }
        let FormSelection::Field(field_index) = self.selected_form else {
            return;
        };
        let Some(name) = self
            .fields
            .get(field_index)
            .map(|field| field.schema.id.clone())
        else {
            return;
        };
        let live_parameter_rollback = (self.pending_live_parameter_rollback.is_none()
            && IMEXPLORE_LIVE_PARAMETER_FIELD_IDS.contains(&name.as_str())
            && self
                .browser_session()
                .is_some_and(|session| session.kind() == BrowserAppKind::Image))
        .then(|| self.parameter_session.clone())
        .flatten();
        let Some(session) = self.parameter_session.as_mut() else {
            return;
        };
        match session.reset(&name) {
            Ok(()) => {
                if self.pending_live_parameter_rollback.is_none() {
                    self.pending_live_parameter_rollback = live_parameter_rollback;
                }
                self.parameter_edit_errors.remove(&name);
                sync_form_fields_from_parameter_session(&mut self.fields, session);
                self.result.status_line = format!("{name} reset to its current default.");
                self.result.status_kind = StatusKind::Info;
                self.apply_live_image_view_parameters_if_needed(field_index);
            }
            Err(error) => {
                self.result.status_line = format!("Could not reset {name}: {error}");
                self.result.status_kind = StatusKind::Error;
            }
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

    fn handle_choice_picker_key(&mut self, key_event: KeyEvent) {
        if key_event.kind != KeyEventKind::Press {
            return;
        }
        match key_event.code {
            KeyCode::Esc if key_event.modifiers.is_empty() => self.cancel_choice_picker(),
            KeyCode::Enter | KeyCode::Char(' ') if key_event.modifiers.is_empty() => {
                self.commit_choice_picker();
            }
            KeyCode::Up | KeyCode::Char('k')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_choice_picker(false);
            }
            KeyCode::Down | KeyCode::Char('j')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_choice_picker(true);
            }
            KeyCode::Left | KeyCode::Char('h')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_choice_picker(false);
            }
            KeyCode::Right | KeyCode::Char('l')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_choice_picker(true);
            }
            KeyCode::Backspace if key_event.modifiers.is_empty() => {
                self.delete_choice_picker_filter_backward();
            }
            KeyCode::Char(ch) if key_event.modifiers.is_empty() => {
                self.extend_choice_picker_filter(ch);
            }
            _ => {}
        }
    }

    fn handle_choice_picker_mouse(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        let anchor = layout
            .form_rows
            .iter()
            .find(|row| Some(row.target) == self.choice_picker_anchor_target())
            .map(|row| row.rect);
        let item_count = self
            .choice_picker_entries()
            .map(|entries| entries.len())
            .unwrap_or_default();
        let area = crate::ui::choice_picker_area(anchor, layout.form_block, item_count);
        let list_area = crate::ui::choice_picker_list_area(area);
        match mouse_event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if !rect_contains(area, mouse_event.column, mouse_event.row) {
                    self.cancel_choice_picker();
                    return;
                }
                if !rect_contains(list_area, mouse_event.column, mouse_event.row) {
                    return;
                }
                let Some(row_offset) =
                    popup_index_at(list_area, mouse_event.column, mouse_event.row, item_count)
                else {
                    return;
                };
                if let Some(picker) = self.choice_picker.as_mut() {
                    picker.selected_filtered_index = row_offset;
                }
                self.commit_choice_picker();
            }
            MouseEventKind::ScrollUp
                if rect_contains(area, mouse_event.column, mouse_event.row) =>
            {
                self.cycle_choice_picker(false);
            }
            MouseEventKind::ScrollDown
                if rect_contains(area, mouse_event.column, mouse_event.row) =>
            {
                self.cycle_choice_picker(true);
            }
            _ => {}
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
                        chooser.target,
                        chooser.explorer.current().path.clone(),
                        chooser.explorer.current().is_dir,
                    ))
                } else {
                    None
                };
                let _ = chooser;
                if let Some((target, path, is_dir)) = double_click_target {
                    if is_dir {
                        self.apply_path_chooser_input(ExplorerInput::Right);
                    } else {
                        self.select_path_chooser_path(target, &path);
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
        if self.result_tab_uses_plot_workspace() {
            match action {
                ResultAction::PreviousTab => {
                    self.cycle_visible_result_tab(false);
                }
                ResultAction::NextTab => {
                    self.cycle_visible_result_tab(true);
                }
                ResultAction::Scroll(delta) => self.scroll_active_plot_workspace(delta),
                ResultAction::ScrollHorizontal(_) => {}
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
            BrowserAction::TogglePlaneMode => self.toggle_image_plane_mode(),
            BrowserAction::ToggleSpectrumPane => self.toggle_image_spectrum_pane(),
            BrowserAction::ToggleMovie => self.toggle_image_movie(),
            BrowserAction::ZoomIn => self.zoom_image_view(true),
            BrowserAction::ZoomOut => self.zoom_image_view(false),
            BrowserAction::ResetViewWindow => self.reset_image_view_window(),
            BrowserAction::PanLeft => self.pan_image_view(-1, 0),
            BrowserAction::PanRight => self.pan_image_view(1, 0),
            BrowserAction::PanUp => self.pan_image_view(0, -1),
            BrowserAction::PanDown => self.pan_image_view(0, 1),
            BrowserAction::CycleColormap => self.cycle_image_plane_colormap(),
            BrowserAction::ToggleInvert => self.toggle_image_plane_invert(),
            BrowserAction::StartRegionShape => self.start_image_region_shape(),
            BrowserAction::ClearRegion => self.clear_image_region(),
            BrowserAction::SaveRegionDefinition => self.save_image_region_definition(),
            BrowserAction::LoadNextRegionDefinition => self.load_next_image_region_definition(),
            BrowserAction::RenameRegionDefinition => {
                if self
                    .image_browser_session_state()
                    .is_some_and(|state| state.left_pane_mode == ImageBrowserLeftPaneMode::Regions)
                {
                    self.rename_image_region_definition();
                }
            }
            BrowserAction::DeleteRegionDefinition => match self.image_browser_session_state() {
                Some(state) if state.left_pane_mode == ImageBrowserLeftPaneMode::Regions => {
                    self.delete_image_region_definition();
                }
                Some(state) if state.left_pane_mode == ImageBrowserLeftPaneMode::Masks => {
                    self.delete_selected_image_mask();
                }
                _ => {}
            },
            BrowserAction::WriteRegionMask => self.write_image_region_mask(),
            BrowserAction::PinProbe => self.pin_current_image_probe(),
            BrowserAction::RemovePinnedProbe => self.remove_selected_image_probe(),
            BrowserAction::CyclePinnedProbe { forward } => self.cycle_selected_image_probe(forward),
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
            BrowserAction::PageUp => {
                self.send_browser_command(BrowserRequest::PageUp { pages: 1 });
            }
            BrowserAction::PageDown => {
                self.send_browser_command(BrowserRequest::PageDown { pages: 1 });
            }
            BrowserAction::Activate => {
                if !self.close_image_region_shape() {
                    self.send_browser_command(BrowserRequest::Activate);
                }
            }
            BrowserAction::Back => {
                if !self.undo_image_region_vertex() {
                    self.send_browser_command(BrowserRequest::Back);
                }
            }
            BrowserAction::Escape => {
                if !self.cancel_image_region_shape()
                    && !self.clear_selected_image_probe()
                    && !self.toggle_image_live_reticle()
                {
                    self.send_browser_command(BrowserRequest::Escape);
                }
            }
        }
    }

    fn toggle_image_plane_mode(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        if state.snapshot.active_view != ImageBrowserView::Plane {
            self.result.status_line =
                "Plane mode toggle is only available in the Plane view.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        let next_mode = match state.plane_mode {
            ImagePlaneMode::Raster => ImagePlaneMode::Spreadsheet,
            ImagePlaneMode::Spreadsheet => ImagePlaneMode::Raster,
        };
        let content_mode = match next_mode {
            ImagePlaneMode::Raster => ImagePlaneContentMode::Raster,
            ImagePlaneMode::Spreadsheet => ImagePlaneContentMode::Spreadsheet,
        };
        if !self
            .send_browser_command(BrowserRequest::SetImagePlaneContentMode { mode: content_mode })
        {
            return;
        }
        if let Some(state) = self.image_browser_session_state_mut() {
            state.plane_mode = next_mode;
            if state.plane_mode == ImagePlaneMode::Spreadsheet {
                keep_image_plane_selection_visible(state);
            }
        }
        self.clear_output_selection_for_target(OutputPane::Result);
        self.result.status_line = format!("Plane view switched to {} mode.", next_mode.label());
        self.result.status_kind = StatusKind::Info;
    }

    fn toggle_image_movie(&mut self) {
        let Some(playing) = self
            .image_browser_session_state()
            .map(|state| state.movie.playing)
        else {
            crate::movie_debug_log("toggle movie ignored: no image browser session");
            return;
        };
        if playing {
            self.stop_image_movie(true, "toggle");
            return;
        }
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        if !state.movie_available() {
            crate::movie_debug_log(format!(
                "toggle movie blocked: movie_available=false view={:?} plane_mode={:?} non_display_axes={}",
                state.snapshot.active_view,
                state.plane_mode,
                state.snapshot.non_display_axes.len()
            ));
            self.result.status_line =
                "Movie mode is only available for Plane views with a selected non-display axis."
                    .into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        state.movie.playing = true;
        state.movie.direct_overlay = false;
        state.movie.terminal_looping = false;
        state.movie.last_advanced_at = Some(Instant::now());
        crate::movie_debug_log(format!(
            "toggle movie playing={} terminal_looping={} fps={} axis_state={}",
            state.movie.playing,
            state.movie.terminal_looping,
            state.movie.fps,
            state
                .selected_non_display_axis_state()
                .map(|axis| format!(
                    "axis={} index={} len={}",
                    axis.axis, axis.index, axis.length
                ))
                .unwrap_or_else(|| "none".to_string())
        ));
        let context = image_movie_perf_context_from_state(state, None, None, None);
        let _ = state;
        self.result.status_line = "Movie playback started.".into();
        self.result.status_kind = StatusKind::Info;
        self.movie_perf.movie_started(context);
    }

    fn start_image_region_shape(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        if state.snapshot.active_view != ImageBrowserView::Plane {
            self.result.status_line = "Regions are only editable in the Plane view.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        if let Some(state) = self.image_browser_session_state_mut() {
            state.show_live_reticle = false;
        }
        self.send_browser_command(BrowserRequest::StartImageRegionShape);
    }

    fn close_image_region_shape(&mut self) -> bool {
        let Some(state) = self.image_browser_session_state() else {
            return false;
        };
        if !state.region_editing_active() {
            return false;
        }
        self.send_browser_command(BrowserRequest::CloseImageRegionShape);
        true
    }

    fn undo_image_region_vertex(&mut self) -> bool {
        let Some(state) = self.image_browser_session_state() else {
            return false;
        };
        if !state.region_editing_active() {
            return false;
        }
        self.send_browser_command(BrowserRequest::UndoImageRegionVertex);
        true
    }

    fn cancel_image_region_shape(&mut self) -> bool {
        let Some(state) = self.image_browser_session_state() else {
            return false;
        };
        if !state.region_editing_active() {
            return false;
        }
        self.send_browser_command(BrowserRequest::CancelImageRegionShape);
        true
    }

    fn clear_image_region(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        if !state.region_active() {
            self.result.status_line = "No active region to clear.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        self.send_browser_command(BrowserRequest::ClearImageRegion);
    }

    fn save_image_region_definition(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        if !state.region_active() {
            self.result.status_line = "No active region to save.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        self.send_browser_command(BrowserRequest::SaveImageRegionDefinition);
    }

    fn load_next_image_region_definition(&mut self) {
        if self.image_browser_session_state().is_none() {
            return;
        }
        self.send_browser_command(BrowserRequest::LoadNextImageRegionDefinition);
    }

    fn select_image_browser_left_pane_mode(&mut self, mode: ImageBrowserLeftPaneMode) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        state.left_pane_mode = mode;
        state.clamp_left_pane_selection();
        self.browser_mode_picker = None;
        self.selected_form = FormSelection::BrowserPane(BrowserPaneSelection::Mode(mode));
    }

    fn write_image_region_mask(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        if !state.region_active() {
            self.result.status_line = "No active region to convert into a mask.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        self.send_browser_command(BrowserRequest::WriteImageRegionMask);
    }

    fn pin_current_image_probe(&mut self) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        if !state.pin_from_snapshot() {
            self.result.status_line = "No active probe available to pin yet.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        let label = state
            .pinned_probes
            .last()
            .map(|probe| probe.label.clone())
            .unwrap_or_else(|| "probe".to_string());
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.result.status_line = format!("Pinned {label}.");
        self.result.status_kind = StatusKind::Info;
    }

    fn remove_selected_image_probe(&mut self) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        let label = state
            .selected_pinned_probe()
            .map(|probe| probe.label.clone())
            .unwrap_or_else(|| "probe".to_string());
        if !state.remove_selected_pinned_probe() {
            self.result.status_line = "No pinned probe is currently selected.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        state.restoring_selected_pinned_probe = false;
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.result.status_line = format!("Removed {label}.");
        self.result.status_kind = StatusKind::Info;
    }

    fn clear_selected_image_probe(&mut self) -> bool {
        let Some(state) = self.image_browser_session_state_mut() else {
            return false;
        };
        if !state.clear_selected_pinned_probe() {
            return false;
        }
        state.restoring_selected_pinned_probe = false;
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.result.status_line = "Returned to the live cursor.".into();
        self.result.status_kind = StatusKind::Info;
        true
    }

    fn toggle_image_live_reticle(&mut self) -> bool {
        let Some(state) = self.image_browser_session_state_mut() else {
            return false;
        };
        state.show_live_reticle = !state.show_live_reticle;
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.result.status_line = if state.show_live_reticle {
            "Live reticle shown.".into()
        } else {
            "Live reticle hidden.".into()
        };
        self.result.status_kind = StatusKind::Info;
        true
    }

    fn cycle_selected_image_probe(&mut self, forward: bool) {
        let probe_id = {
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            if !state.cycle_selected_pinned_probe(forward) {
                self.result.status_line = "No pinned probes available yet.".into();
                self.result.status_kind = StatusKind::Warning;
                return;
            }
            state.selected_pinned_probe_id
        };
        let Some(probe_id) = probe_id else {
            return;
        };
        self.activate_image_pinned_probe(probe_id);
    }

    fn activate_image_pinned_probe(&mut self, probe_id: u64) -> bool {
        let target = {
            let Some(state) = self.image_browser_session_state_mut() else {
                return false;
            };
            let Some(target) = state
                .pinned_probes
                .iter()
                .find(|probe| probe.id == probe_id)
                .cloned()
            else {
                return false;
            };
            state.selected_pinned_probe_id = Some(probe_id);
            state.restoring_selected_pinned_probe = true;
            state.snapshot_generation = state.snapshot_generation.saturating_add(1);
            target
        };

        self.restore_image_browser_to_pinned_probe(&target);

        let Some(state) = self.image_browser_session_state_mut() else {
            return false;
        };
        state.restoring_selected_pinned_probe = false;
        state.sync_selected_pinned_probe_from_snapshot();
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.result.status_line = format!("Selected {}.", target.label);
        self.result.status_kind = StatusKind::Info;
        true
    }

    fn restore_image_browser_to_pinned_probe(&mut self, target: &ImagePinnedProbe) {
        let Some((current_x, current_y)) = self
            .image_browser_session_state()
            .and_then(|state| state.snapshot.plane_cursor.as_ref())
            .map(|cursor| (cursor.pixel_x, cursor.pixel_y))
        else {
            return;
        };
        if (current_x, current_y) != target.plane_pixel {
            self.send_browser_command(BrowserRequest::SetImageCursor {
                x: target.plane_pixel.0,
                y: target.plane_pixel.1,
            });
        }
        for (axis, target_index) in &target.non_display_axis_indices {
            let current_index = self
                .image_browser_session_state()
                .and_then(|state| {
                    state
                        .snapshot
                        .non_display_axes
                        .iter()
                        .find(|state_axis| state_axis.axis == *axis)
                        .map(|state_axis| state_axis.index)
                })
                .unwrap_or(*target_index);
            let delta = *target_index as i32 - current_index as i32;
            if delta != 0 {
                self.send_browser_command(BrowserRequest::StepImageNonDisplayAxis {
                    axis: *axis,
                    delta,
                });
            }
        }
    }

    fn select_image_pinned_probe_by_pixel(&mut self, pixel: (usize, usize)) -> bool {
        let selected_id = self.image_browser_session_state().and_then(|state| {
            state
                .pinned_probes
                .iter()
                .rev()
                .find(|probe| probe.plane_pixel == pixel)
                .map(|probe| probe.id)
        });
        selected_id
            .map(|probe_id| self.activate_image_pinned_probe(probe_id))
            .unwrap_or(false)
    }

    fn stop_image_movie(&mut self, update_status: bool, reason: impl Into<String>) {
        let Some(_) = self.image_browser_session_state() else {
            return;
        };
        if !self.image_movie_active() {
            return;
        }
        let reason = reason.into();
        let mut sync_steps = Vec::new();
        let context = {
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            if let Some(scheduler) = state.movie_scheduler.as_ref() {
                for axis_state in &state.snapshot.non_display_axes {
                    if let Some((_, session_index)) = scheduler
                        .session_indices
                        .iter()
                        .find(|(axis, _)| *axis == axis_state.axis)
                    {
                        let delta = axis_state.index as i32 - *session_index as i32;
                        if delta != 0 {
                            sync_steps.push((axis_state.axis, delta));
                        }
                    }
                }
            }
            crate::movie_debug_log(format!(
                "stop_image_movie update_status={} terminal_looping={} axis_state={} reason={}",
                update_status,
                state.movie.terminal_looping,
                state
                    .selected_non_display_axis_state()
                    .map(|axis| format!(
                        "axis={} index={} len={}",
                        axis.axis, axis.index, axis.length
                    ))
                    .unwrap_or_else(|| "none".to_string()),
                reason
            ));
            let context = image_movie_perf_context_from_state(state, None, None, None);
            state.movie.playing = false;
            state.movie.direct_overlay = false;
            state.movie.terminal_looping = false;
            state.movie.last_advanced_at = None;
            state.movie_frame_seq = None;
            state.movie_scheduler = None;
            context
        };
        self.clear_image_movie_panel_overrides();
        for (axis, delta) in sync_steps {
            self.send_browser_command(BrowserRequest::StepImageNonDisplayAxis { axis, delta });
        }
        if update_status {
            self.result.status_line = "Movie playback paused.".into();
            self.result.status_kind = StatusKind::Info;
        }
        self.movie_perf.movie_stopped(context, reason);
    }

    fn should_stop_image_movie_for_key(
        &self,
        key_event: KeyEvent,
        action: Option<&AppAction>,
    ) -> bool {
        !movie_stop_input_ignored_for_debug()
            && self.image_movie_active()
            && key_event.kind == KeyEventKind::Press
            && matches!(
                action,
                Some(action)
                    if !matches!(
                        action,
                        AppAction::Browser(BrowserAction::ToggleMovie)
                            | AppAction::Quit
                            | AppAction::BackToLauncher
                    )
            )
    }

    fn should_stop_image_movie_for_mouse(&self, mouse_event: MouseEvent) -> bool {
        !movie_stop_input_ignored_for_debug()
            && self.image_movie_active()
            && matches!(
                mouse_event.kind,
                MouseEventKind::Down(_)
                    | MouseEventKind::Drag(_)
                    | MouseEventKind::ScrollUp
                    | MouseEventKind::ScrollDown
                    | MouseEventKind::ScrollLeft
                    | MouseEventKind::ScrollRight
            )
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
                } else if self.result_tab_uses_plot_workspace() {
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
            EditTarget::RenameImageRegionDefinition => {}
        }
    }

    fn advance_form_edit(&mut self, field_index: usize, forward: bool) {
        let targets = self
            .visible_form_targets()
            .into_iter()
            .filter_map(|target| match target {
                FormSelection::Field(index) => Some(index),
                FormSelection::Section(_)
                | FormSelection::WorkflowStageGuide(_)
                | FormSelection::WorkflowStageAction
                | FormSelection::WorkflowContextSetting(_)
                | FormSelection::SummaryView(_)
                | FormSelection::BrowserView(_)
                | FormSelection::WorkflowProductAction(_)
                | FormSelection::WorkflowChainEntry(_)
                | FormSelection::WorkflowChainSetting(_, _)
                | FormSelection::WorkflowProduct(_)
                | FormSelection::WorkflowStage(_)
                | FormSelection::BrowserPane(_) => None,
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

    fn handle_left_mouse_down(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        self.dragging_image_workspace_divider = false;
        self.dragging_image_cursor = false;
        self.dragging_image_profile = false;
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
            match tab {
                crate::ui::TabHitTarget::Result(tab) => self.activate_result_tab(tab),
                crate::ui::TabHitTarget::Browser(tab) => {
                    self.activate_browser_tab(tab);
                    self.activate_result_tab(tab.shell_result_tab());
                }
            }
            self.last_click = Some(ClickState {
                target: ClickTarget::ResultTabs,
                at: Instant::now(),
            });
            return;
        }

        if self.image_workspace_divider_toggle_hit(mouse_event.column, mouse_event.row, layout) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.set_focus_target(FocusTarget::BrowserMain);
            self.dragging_divider = false;
            self.dragging_image_workspace_divider = false;
            self.toggle_image_spectrum_pane();
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Result),
                at: Instant::now(),
            });
            return;
        }

        if self.image_workspace_divider_hit(mouse_event.column, mouse_event.row, layout) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.set_focus_target(FocusTarget::BrowserMain);
            self.dragging_divider = false;
            self.dragging_image_workspace_divider = true;
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Result),
                at: Instant::now(),
            });
            return;
        }

        if self.result_tab_uses_plot_workspace() {
            if let Some(row) = layout.plot_catalog_at(mouse_event.column, mouse_event.row) {
                self.pane_focus = PaneFocus::Result;
                self.clear_output_selection_for_target(OutputPane::Result);
                self.plot_workspace.focus = PlotPaneFocus::Catalog;
                self.apply_plot_catalog_target(row.target);
                self.last_click = Some(ClickState {
                    target: ClickTarget::PlotCatalog(row.target),
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
                    | PlotControlTarget::CopyCli
                    | PlotControlTarget::ExportPng
                    | PlotControlTarget::ExportPdf => self.activate_plot_workspace_selection(),
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
            self.dragging_image_cursor = self
                .image_browser_session_state()
                .is_some_and(|state| !state.region_editing_active());
            if self.select_image_pinned_probe_by_pixel((x, y)) {
                self.last_click = Some(ClickState {
                    target: ClickTarget::Pane(PaneFocus::Result),
                    at: Instant::now(),
                });
                return;
            }
            if self
                .image_browser_session_state()
                .is_some_and(|state| state.region_editing_active())
            {
                self.send_browser_command(BrowserRequest::AppendImageRegionVertex { x, y });
            } else {
                self.send_browser_command(BrowserRequest::SetImageCursor { x, y });
            }
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Result),
                at: Instant::now(),
            });
            return;
        }

        if let Some((axis, delta)) =
            self.image_spectrum_click_target(mouse_event.column, mouse_event.row, layout)
        {
            self.set_focus_target(FocusTarget::BrowserMain);
            self.clear_output_selection_for_target(OutputPane::Result);
            self.dragging_image_profile = true;
            if delta != 0 {
                self.send_browser_command(BrowserRequest::StepImageNonDisplayAxis { axis, delta });
            }
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

        if layout.in_browser_mode_selector(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Parameters;
            self.open_browser_mode_picker();
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Parameters),
                at: Instant::now(),
            });
            return;
        }

        if let Some(target) = layout.form_target_at(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Parameters;
            let click_target = match target {
                FormSelection::Section(index) => ClickTarget::Section(index),
                FormSelection::Field(index) => ClickTarget::Field(index),
                FormSelection::WorkflowContextSetting(kind) => {
                    ClickTarget::WorkflowContextSetting(kind)
                }
                FormSelection::WorkflowProductAction(kind) => {
                    ClickTarget::WorkflowProductAction(kind)
                }
                FormSelection::WorkflowChainEntry(index) => ClickTarget::WorkflowChainEntry(index),
                FormSelection::WorkflowChainSetting(entry, kind) => {
                    ClickTarget::WorkflowChainSetting(entry, kind)
                }
                FormSelection::WorkflowProduct(index) => ClickTarget::WorkflowProduct(index),
                FormSelection::SummaryView(_)
                | FormSelection::BrowserView(_)
                | FormSelection::WorkflowStageGuide(_)
                | FormSelection::WorkflowStageAction
                | FormSelection::WorkflowStage(_) => ClickTarget::Pane(PaneFocus::Parameters),
                FormSelection::BrowserPane(_) => ClickTarget::Pane(PaneFocus::Parameters),
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
                FormSelection::SummaryView(view) => {
                    self.selected_form = FormSelection::SummaryView(view);
                    self.activate_selected_form_item();
                }
                FormSelection::WorkflowContextSetting(kind) => {
                    self.selected_form = FormSelection::WorkflowContextSetting(kind);
                    if double_click {
                        self.activate_selected_form_item();
                    }
                }
                FormSelection::WorkflowStageGuide(kind) => {
                    self.selected_form = FormSelection::WorkflowStageGuide(kind);
                }
                FormSelection::WorkflowStageAction => {
                    self.selected_form = FormSelection::WorkflowStageAction;
                    if double_click {
                        self.activate_selected_form_item();
                    }
                }
                FormSelection::BrowserView(view) => {
                    self.selected_form = FormSelection::BrowserView(view);
                    self.activate_selected_form_item();
                }
                FormSelection::WorkflowChainEntry(index) => {
                    self.selected_form = FormSelection::WorkflowChainEntry(index);
                    self.activate_selected_form_item();
                }
                FormSelection::WorkflowProductAction(kind) => {
                    self.selected_form = FormSelection::WorkflowProductAction(kind);
                    self.activate_selected_form_item();
                }
                FormSelection::WorkflowChainSetting(entry, kind) => {
                    self.selected_form = FormSelection::WorkflowChainSetting(entry, kind);
                    if double_click {
                        self.activate_selected_form_item();
                    }
                }
                FormSelection::WorkflowProduct(index) => {
                    self.selected_form = FormSelection::WorkflowProduct(index);
                    self.activate_selected_form_item();
                }
                FormSelection::WorkflowStage(stage) => {
                    self.selected_form = FormSelection::WorkflowStage(stage);
                    self.activate_selected_form_item();
                }
                FormSelection::Field(field_index) => {
                    self.selected_form = FormSelection::Field(field_index);
                    if self.path_field_browse_hit(field_index, mouse_event.column, layout) {
                        self.open_path_chooser(field_index);
                    } else if self
                        .field_picker_entries(field_index)
                        .is_some_and(|entries| !entries.is_empty())
                        && double_click
                    {
                        self.open_choice_picker(field_index);
                    } else if double_click {
                        self.enter_edit_mode(field_index);
                    }
                }
                FormSelection::BrowserPane(target) => {
                    self.selected_form = FormSelection::BrowserPane(target);
                    match target {
                        BrowserPaneSelection::SavedRegion(index) => {
                            if self.browser_pane_checkbox_hit(index, mouse_event.column, layout) {
                                self.activate_browser_pane_selection(target);
                            } else if let Some(state) = self.image_browser_session_state_mut() {
                                state.left_pane_mode = ImageBrowserLeftPaneMode::Regions;
                                state.selected_saved_region_index = index;
                                self.rename_image_region_definition();
                            }
                        }
                        BrowserPaneSelection::Mask(index) => {
                            if let Some(state) = self.image_browser_session_state_mut() {
                                state.left_pane_mode = ImageBrowserLeftPaneMode::Masks;
                                state.selected_mask_index = index;
                            }
                            self.activate_browser_pane_selection(target);
                        }
                        BrowserPaneSelection::Mode(mode) => {
                            self.selected_form =
                                FormSelection::BrowserPane(BrowserPaneSelection::Mode(mode));
                            self.open_browser_mode_picker();
                        }
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
        if self.dragging_image_workspace_divider {
            self.clear_output_selection_for_target(OutputPane::Result);
            if let Some(ratio) =
                self.image_workspace_split_ratio_from_mouse(mouse_event.row, layout)
            {
                self.config_store.set_image_workspace_split_ratio(ratio);
            }
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
        if self.dragging_image_profile {
            self.clear_output_selection_for_target(OutputPane::Result);
            if let Some((axis, delta)) =
                self.image_spectrum_click_target(mouse_event.column, mouse_event.row, layout)
                && delta != 0
            {
                self.set_focus_target(FocusTarget::BrowserMain);
                self.send_browser_command(BrowserRequest::StepImageNonDisplayAxis { axis, delta });
            }
            return;
        }
        if self.dragging_image_cursor {
            self.clear_output_selection_for_target(OutputPane::Result);
            if let Some((x, y)) =
                self.image_plane_click_target(mouse_event.column, mouse_event.row, layout)
            {
                self.set_focus_target(FocusTarget::BrowserMain);
                self.send_browser_command(BrowserRequest::SetImageCursor { x, y });
            }
            return;
        }
        if self.update_output_selection(mouse_event.column, mouse_event.row) {
            self.last_click = None;
        }
    }

    fn handle_mouse_scroll(&mut self, mouse_event: MouseEvent, layout: &UiLayout, delta: i16) {
        if self.image_raster_plane_canvas_hit(mouse_event.column, mouse_event.row, layout) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.set_focus_target(FocusTarget::BrowserMain);
            if delta.is_negative() {
                self.zoom_image_view(true);
            } else {
                self.zoom_image_view(false);
            }
            return;
        }

        if self.result_tab_uses_plot_workspace() {
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
        if self.result_tab_uses_plot_workspace() {
            if layout
                .plot_control_at(mouse_event.column, mouse_event.row)
                .is_some()
            {
                self.clear_output_selection_for_target(OutputPane::Result);
                self.pane_focus = PaneFocus::Result;
                self.plot_workspace.focus = PlotPaneFocus::Controls;
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
            FormSelection::SummaryView(view) => {
                self.selected_summary_view = view;
                self.activate_result_tab(ResultTab::Data);
            }
            FormSelection::WorkflowContextSetting(kind) => {
                self.activate_workflow_context_setting(kind)
            }
            FormSelection::WorkflowStageGuide(_) => {}
            FormSelection::WorkflowStageAction => self.start_run(),
            FormSelection::BrowserView(view) => {
                self.activate_browser_tab(view);
                self.activate_result_tab(view.shell_result_tab());
                self.result.status_line = format!("Browser view selected: {}.", view.label());
                self.result.status_kind = StatusKind::Info;
            }
            FormSelection::WorkflowChainEntry(index) => self.select_workflow_chain_entry(index),
            FormSelection::WorkflowProductAction(kind) => {
                self.activate_workflow_product_action(kind)
            }
            FormSelection::WorkflowChainSetting(entry, kind) => {
                self.activate_workflow_chain_setting(entry, kind)
            }
            FormSelection::WorkflowProduct(index) => self.select_workflow_product(index),
            FormSelection::WorkflowStage(stage) => {
                self.set_current_workflow_stage(stage);
                self.result.status_line = format!(
                    "Workflow stage selected: {}. {}",
                    stage.label(),
                    workflow_stage_hint(stage)
                );
                self.result.status_kind = StatusKind::Info;
            }
            FormSelection::Field(field_index) => {
                let Some(field) = self.fields.get(field_index) else {
                    return;
                };
                if self
                    .field_picker_entries(field_index)
                    .is_some_and(|entries| !entries.is_empty())
                {
                    self.open_choice_picker(field_index);
                    return;
                }
                match &field.value {
                    FormValue::Text(_) => self.enter_edit_mode(field_index),
                    FormValue::Toggle(_) => self.toggle_field(field_index),
                    FormValue::Choice { .. } => self.cycle_field_choice(field_index, true),
                }
            }
            FormSelection::BrowserPane(target) => self.activate_browser_pane_selection(target),
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
        match self.sync_parameter_from_field(field_index) {
            Ok(()) => self.apply_live_image_view_parameters_if_needed(field_index),
            Err(error) => {
                self.result.status_line = error;
                self.result.status_kind = StatusKind::Warning;
            }
        }
        self.sync_result_tab_visibility();
    }

    fn cycle_field_choice(&mut self, field_index: usize, forward: bool) {
        if let Some(field) = self.fields.get_mut(field_index) {
            field.cycle_choice(forward);
        }
        match self.sync_parameter_from_field(field_index) {
            Ok(()) => self.apply_live_image_view_parameters_if_needed(field_index),
            Err(error) => {
                self.result.status_line = error;
                self.result.status_kind = StatusKind::Warning;
            }
        }
    }

    fn adjust_selected_choice(&mut self, forward: bool) {
        match self.selected_form {
            FormSelection::Field(field_index) => {
                if self
                    .field_picker_entries(field_index)
                    .is_some_and(|entries| !entries.is_empty())
                {
                    self.open_choice_picker(field_index);
                } else {
                    self.cycle_field_choice(field_index, forward);
                }
            }
            FormSelection::SummaryView(view) => {
                self.selected_summary_view = view.cycle(forward);
                self.selected_form = FormSelection::SummaryView(self.selected_summary_view);
                self.activate_result_tab(ResultTab::Data);
            }
            FormSelection::BrowserView(view) => {
                let tabs = self.browser_tabs();
                if tabs.is_empty() {
                    return;
                }
                let position = tabs
                    .iter()
                    .position(|candidate| *candidate == view)
                    .unwrap_or(0);
                let next = if forward {
                    tabs[(position + 1) % tabs.len()]
                } else if position == 0 {
                    tabs[tabs.len() - 1]
                } else {
                    tabs[position - 1]
                };
                self.selected_form = FormSelection::BrowserView(next);
                self.activate_browser_tab(next);
                self.activate_result_tab(next.shell_result_tab());
            }
            FormSelection::WorkflowChainEntry(index) => {
                let entries = self.workflow_chain_entries();
                if entries.is_empty() {
                    return;
                }
                let next = if forward {
                    (index + 1) % entries.len()
                } else if index == 0 {
                    entries.len() - 1
                } else {
                    index - 1
                };
                self.selected_form = FormSelection::WorkflowChainEntry(next);
                self.select_workflow_chain_entry(next);
            }
            FormSelection::WorkflowProductAction(kind) => {
                self.selected_form = FormSelection::WorkflowProductAction(kind);
                match kind {
                    WorkflowProductActionKind::AddSolvedProduct => {
                        self.activate_workflow_product_action(kind);
                    }
                    WorkflowProductActionKind::ImportChainTable
                    | WorkflowProductActionKind::ChooseCallibrary => {}
                }
            }
            FormSelection::WorkflowContextSetting(kind) => {
                self.activate_workflow_context_setting(kind);
            }
            FormSelection::WorkflowStageGuide(_) => {}
            FormSelection::WorkflowStageAction => {}
            FormSelection::WorkflowChainSetting(entry, kind) => match kind {
                WorkflowChainSettingKind::Interp => {
                    self.open_workflow_chain_setting_picker(entry, kind);
                }
                WorkflowChainSettingKind::Calwt => {
                    self.open_workflow_chain_setting_picker(entry, kind);
                }
                WorkflowChainSettingKind::Gainfield | WorkflowChainSettingKind::Spwmap => {
                    self.open_workflow_chain_setting_picker(entry, kind);
                }
            },
            FormSelection::WorkflowProduct(index) => {
                if self.workflow_products.is_empty() {
                    return;
                }
                let next = if forward {
                    (index + 1) % self.workflow_products.len()
                } else if index == 0 {
                    self.workflow_products.len() - 1
                } else {
                    index - 1
                };
                self.selected_form = FormSelection::WorkflowProduct(next);
                self.select_workflow_product(next);
            }
            FormSelection::WorkflowStage(stage) => {
                let next = stage.cycle(forward);
                self.set_current_workflow_stage(next);
            }
            FormSelection::BrowserPane(BrowserPaneSelection::Mode(_)) => {
                if self.browser_mode_picker.is_none() {
                    self.open_browser_mode_picker();
                } else {
                    self.cycle_browser_mode_picker(forward);
                }
            }
            FormSelection::Section(_) | FormSelection::BrowserPane(_) => {}
        }
    }

    fn active_browser_tab(&self) -> Option<BrowserTab> {
        self.browser_session().map(BrowserSession::active_tab)
    }

    fn browser_active_view_result_tab(&self) -> Option<ResultTab> {
        self.active_browser_tab().map(BrowserTab::shell_result_tab)
    }

    pub(crate) fn browser_result_uses_live_navigation(&self) -> bool {
        matches!(
            self.active_result_tab,
            ResultTab::Structure | ResultTab::Content
        ) && self.browser_active_view_result_tab() == Some(self.active_result_tab)
    }

    pub(crate) fn browser_result_requires_special_renderer(&self) -> bool {
        self.browser_result_uses_live_navigation()
            && self.active_result_tab == ResultTab::Content
            && self.image_raster_plane_active()
    }

    fn sync_browser_shell_state(&mut self, force_result_tab: bool) {
        let Some(view) = self.active_browser_tab() else {
            return;
        };
        if !self.browser_uses_parameter_pane() {
            self.selected_form = FormSelection::BrowserView(view);
        }
        if force_result_tab
            || matches!(
                self.active_result_tab,
                ResultTab::Structure | ResultTab::Content
            )
        {
            self.active_result_tab = view.shell_result_tab();
        }
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
            self.set_focus_target(if self.result_tab_uses_plot_workspace() {
                FocusTarget::PlotCatalog
            } else {
                FocusTarget::ResultPane
            });
        } else if self.focus_ring().contains(&FocusTarget::ParametersPane) {
            self.pane_focus = PaneFocus::Parameters;
        }
    }

    fn toggle_image_spectrum_pane(&mut self) {
        if !self.image_plane_has_linked_profile() {
            return;
        }
        self.clear_output_selection_for_target(OutputPane::Result);
        let next = if self.image_spectrum_pane_collapsed() {
            self.config_store.image_workspace_restore_ratio()
        } else {
            1.0
        };
        self.config_store.set_image_workspace_split_ratio(next);
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
            if state.raster_plane_active() {
                return;
            }
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
            if state.raster_plane_active() {
                return;
            }
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

    fn keep_active_image_plane_selection_visible(&mut self) {
        if let Some(BrowserSession {
            kind: BrowserSessionKind::Image(state),
            ..
        }) = self.browser_session.as_mut()
        {
            keep_image_plane_selection_visible(state);
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
        let metrics = if self.browser_result_uses_live_navigation() {
            self.active_browser_scroll_metrics(track.height)
        } else {
            self.active_result_scroll_metrics(track.height)
        };
        let Some((content_length, viewport_length)) = metrics else {
            return;
        };
        if content_length <= viewport_length || track.height == 0 {
            if self.browser_result_uses_live_navigation() {
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
        let scroll = (row_offset * max_scroll + denominator / 2)
            .checked_div(denominator)
            .unwrap_or(0);
        if self.browser_result_uses_live_navigation() {
            self.set_active_browser_scroll(scroll);
        } else {
            self.set_active_result_scroll(scroll);
        }
    }

    fn scroll_result_horizontally_to_mouse(&mut self, column: u16, layout: &UiLayout) {
        let Some(track) = layout.result_hscrollbar else {
            return;
        };
        let metrics = if self.browser_result_uses_live_navigation() {
            self.active_browser_hscroll_metrics(track.width)
        } else {
            self.active_result_hscroll_metrics(track.width)
        };
        let Some((content_width, viewport_width)) = metrics else {
            return;
        };
        if content_width <= viewport_width || track.width == 0 {
            if self.browser_result_uses_live_navigation() {
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
        if self.browser_result_uses_live_navigation() {
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
        let metrics = if self.browser_result_uses_live_navigation() {
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

        let active_hscroll = if self.browser_result_uses_live_navigation() {
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
        self.choice_picker = None;
        self.path_chooser = None;
        self.commit_edit_buffer();

        if self.schema.is_none() {
            self.result.status_line = "Cannot run without a loaded UI schema.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.active_result_tab = ResultTab::Stderr;
            return;
        }

        if self.app.is_browser_session() {
            self.pending_run_confirmation = false;
            self.start_browser_session();
            return;
        }

        if self.app.id == "calibrate"
            && self.current_workflow_stage() == WorkflowStageId::InspectDataset
        {
            self.pending_run_confirmation = false;
            self.run_calibrate_dataset_summary_inline();
            return;
        }

        let requires_run_confirmation = match self.requires_run_confirmation() {
            Ok(required) => required,
            Err(error) => {
                self.pending_run_confirmation = false;
                self.result.status_line = "Cannot evaluate run safety.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
                self.active_result_tab = ResultTab::Stderr;
                return;
            }
        };
        if requires_run_confirmation && !self.pending_run_confirmation {
            self.pending_run_confirmation = true;
            self.result.status_line = format!(
                "{} may modify data or create products. Press r again to confirm.",
                self.app.display_name
            );
            self.result.status_kind = StatusKind::Warning;
            self.active_result_tab = ResultTab::Overview;
            return;
        }
        self.pending_run_confirmation = false;

        match self.build_execution_plan() {
            Ok(plan) => {
                let notebook_bypass_once = std::mem::take(&mut self.notebook_bypass_once);
                let mut task_last_state = TaskLastState::new(
                    ManagedStateStore::for_workspace(&self.parameter_workspace),
                    self.app.id.clone(),
                    self.save_last,
                );
                let attempted = match self.parameter_session.as_ref() {
                    Some(session) => task_last_state
                        .before_execution(session)
                        .map_err(|error| error.to_string()),
                    None => Err("typed parameter session is unavailable".to_string()),
                };
                let last_warning = match attempted {
                    Ok(report) => report.warning,
                    Err(error) => {
                        self.result.status_line = "Cannot save resolved task parameters.".into();
                        self.result.status_kind = StatusKind::Error;
                        self.result.stderr = format!("{error}\n");
                        self.active_result_tab = ResultTab::Stderr;
                        return;
                    }
                };
                let mut notebook_recording = self.parameter_session.as_ref().map(|session| {
                    NotebookRecording::begin(
                        self.parameter_workspace.clone(),
                        "tui",
                        &self.app.id,
                        session,
                        notebook_bypass_once,
                        requires_run_confirmation,
                    )
                });
                let notebook_warning = notebook_recording
                    .as_mut()
                    .and_then(NotebookRecording::take_warning);
                match spawn_process(&plan) {
                    Ok(process) => {
                        let mut run_warnings = Vec::new();
                        if let Some(warning) = &last_warning {
                            run_warnings.push(format!("could not save Last: {warning}"));
                        }
                        if let Some(warning) = &notebook_warning {
                            run_warnings.push(format!("notebook recording unavailable: {warning}"));
                        }
                        let status_line = if run_warnings.is_empty() {
                            format!("Running {}...", self.app.id)
                        } else {
                            format!(
                                "Running {}... Warning: {}",
                                self.app.id,
                                run_warnings.join("; ")
                            )
                        };
                        self.result = ResultState {
                            status_line,
                            status_kind: StatusKind::Running,
                            stderr: run_warnings
                                .into_iter()
                                .map(|warning| format!("Automatic run warning: {warning}\n"))
                                .collect(),
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
                            task_last_state: Some(task_last_state),
                            notebook_recording,
                        });
                    }
                    Err(error) => {
                        if let Some(recording) = notebook_recording.as_mut() {
                            let _ = recording.finalize(
                                NotebookExecutionStatus::Failed,
                                String::new(),
                                String::new(),
                                Vec::new(),
                                vec![error.clone()],
                            );
                        }
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

    fn requires_run_confirmation(&self) -> Result<bool, String> {
        self.parameter_session
            .as_ref()
            .ok_or_else(|| "typed parameter session is unavailable".to_string())?
            .required_run_safety()
            .map(|requirements| requirements.requires_interactive_confirmation())
            .map_err(|error| format!("evaluate {} run safety: {error}", self.app.id))
    }

    fn run_calibrate_dataset_summary_inline(&mut self) {
        let Some(ms_path) = self.non_empty_field_text("vis") else {
            self.result.status_line =
                "Enter a MeasurementSet path before running dataset summary.".to_string();
            self.result.status_kind = StatusKind::Warning;
            self.active_result_tab = ResultTab::Overview;
            return;
        };

        self.result = ResultState::default();
        self.active_result_tab = ResultTab::Overview;
        self.pane_focus = PaneFocus::Result;
        self.result_scrolls = [0; RESULT_TAB_COUNT];
        self.result_hscrolls = [0; RESULT_TAB_COUNT];
        self.ensure_current_summary_snapshot_if_needed();

        if let Some(error) = self.result.structured_error.clone() {
            self.result.status_line = "Dataset summary failed.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.record_history_entry(
                Some(WorkflowStageId::InspectDataset),
                "Dataset Summary Failed".to_string(),
                StatusKind::Error,
                vec![format!("MS: {ms_path}"), error],
            );
            return;
        }

        self.result.status_line = "MeasurementSet summary refreshed.".to_string();
        self.result.status_kind = StatusKind::Ok;
        self.record_history_entry(
            Some(WorkflowStageId::InspectDataset),
            "Dataset Summary".to_string(),
            StatusKind::Ok,
            vec![format!("MS: {ms_path}")],
        );
    }

    fn start_browser_session(&mut self) {
        self.clear_output_selection();
        if let Some((name, error)) = self.parameter_edit_errors.iter().next() {
            self.result.status_line = format!("Invalid parameter {name}: {error}");
            self.result.status_kind = StatusKind::Error;
            return;
        }
        let Some(session) = self.parameter_session.as_ref() else {
            self.result.status_line =
                "Cannot open session: the typed parameter contract is unavailable.".to_string();
            self.result.status_kind = StatusKind::Error;
            return;
        };
        if let Err(error) = session.render_sparse() {
            self.result.status_line = format!("Cannot open session: {error}");
            self.result.status_kind = StatusKind::Error;
            return;
        }
        let Some(path_field) = self.app.browser_path_field_id() else {
            self.result.status_line =
                "Browser session is missing a startup path field.".to_string();
            self.result.status_kind = StatusKind::Error;
            return;
        };
        let Some(path) = self
            .session_parameter_text(path_field)
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
                let startup = match self.table_session_startup_config() {
                    Ok(startup) => startup,
                    Err(error) => {
                        self.report_browser_error(
                            "Cannot open table browser with these startup parameters.",
                            format!("{error}\n"),
                        );
                        return;
                    }
                };
                let (width, height, _) = self.browser_startup_viewport_cells();
                let viewport = BrowserViewport::new(width, height);
                match self
                    .app
                    .resolve_command()
                    .and_then(|command| BrowserClient::spawn(&command))
                {
                    Ok(client) => match client.request_startup(BrowserCommand::OpenRoot {
                        path: path.clone(),
                        viewport,
                    }) {
                        Ok(snapshot) => match if startup.requires_configure {
                            client
                                .request_startup(BrowserCommand::Configure {
                                    parameters: startup.parameters.clone(),
                                })
                                .map_err(|error| error.message().to_string())
                        } else {
                            Ok(snapshot)
                        } {
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
                                    kind: BrowserSessionKind::Table(Box::new(
                                        TableBrowserSession {
                                            client,
                                            snapshot,
                                            viewport,
                                        },
                                    )),
                                });
                                self.result_scrolls = [0; RESULT_TAB_COUNT];
                                self.result_hscrolls = [0; RESULT_TAB_COUNT];
                                self.sync_browser_shell_state(true);
                                self.record_session_opened();
                            }
                            Err(error) => {
                                let stderr = client.stderr_text();
                                let _ = client.cancel();
                                self.report_browser_error(
                                    "Failed to apply table browser startup parameters.",
                                    if stderr.trim().is_empty() {
                                        format!("{error}\n")
                                    } else {
                                        format!("{error}\n{stderr}")
                                    },
                                );
                            }
                        },
                        Err(error) => {
                            let _ = client.cancel();
                            self.report_browser_error(
                                "Failed to open table browser.",
                                format!("{}\n", error.message()),
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
                let startup = match self.image_session_startup_config() {
                    Ok(startup) => startup,
                    Err(error) => {
                        self.report_browser_error(
                            "Cannot open imexplore with these startup parameters.",
                            format!("{error}\n"),
                        );
                        return;
                    }
                };
                let (width, height, inspector_height) = self.browser_startup_viewport_cells();
                let font_size = self.image_plane_font_size();
                let startup_context = image_startup_perf_context(
                    width,
                    height,
                    width.saturating_mul(font_size.0.max(1)),
                    height.saturating_mul(font_size.1.max(1)),
                );
                let viewport = ImageBrowserViewport::with_plane_pixels(
                    width,
                    height,
                    inspector_height,
                    width.saturating_mul(font_size.0.max(1)),
                    height.saturating_mul(font_size.1.max(1)),
                );
                self.movie_perf
                    .startup_started(startup_context, format!("imexplore path={path}"));
                match self
                    .app
                    .resolve_command()
                    .and_then(|command| ImageBrowserClient::spawn(&command))
                {
                    Ok(client) => {
                        self.movie_perf.startup_browser_open_requested(
                            startup_context,
                            format!("open_root path={path}"),
                        );
                        match client.request_startup(ImageBrowserCommand::OpenRoot {
                            path: path.clone(),
                            viewport,
                            parameters: Some(startup.view_parameters.clone()),
                        }) {
                            Ok(snapshot) => {
                                self.movie_perf.startup_browser_open_completed(
                                    startup_context,
                                    snapshot.backend_timing.as_ref().map(map_backend_timing),
                                    snapshot.active_view == ImageBrowserView::Plane
                                        && snapshot.profile.is_some(),
                                );
                                let (snapshot, selected_movie_axis) =
                                    match apply_image_startup_config(&client, snapshot, &startup) {
                                        Ok(configured) => configured,
                                        Err(error) => {
                                            let stderr = client.stderr_text();
                                            let _ = client.cancel();
                                            self.report_browser_error(
                                                "Failed to apply imexplore startup parameters.",
                                                if stderr.trim().is_empty() {
                                                    format!("{error}\n")
                                                } else {
                                                    format!("{error}\n{stderr}")
                                                },
                                            );
                                            return;
                                        }
                                    };
                                self.result = ResultState {
                                    status_line: snapshot.status_line.clone(),
                                    status_kind: StatusKind::Info,
                                    ..ResultState::default()
                                };
                                self.edit_state = None;
                                self.pane_focus = PaneFocus::Result;
                                let mut state = ImageBrowserSessionState {
                                    client,
                                    snapshot,
                                    viewport,
                                    hscroll: 0,
                                    left_pane_mode: ImageBrowserLeftPaneMode::Live,
                                    selected_saved_region_index: 0,
                                    selected_mask_index: 0,
                                    selected_non_display_axis: selected_movie_axis,
                                    pinned_probes: Vec::new(),
                                    selected_pinned_probe_id: None,
                                    next_pinned_probe_id: 1,
                                    restoring_selected_pinned_probe: false,
                                    show_live_reticle: true,
                                    plane_mode: image_plane_mode(startup.content_mode),
                                    plane_colormap: startup.colormap,
                                    plane_invert: false,
                                    panel: None,
                                    spectrum_panel: None,
                                    snapshot_generation: 1,
                                    movie: {
                                        let mut movie = ImageMovieState::with_fps(startup.fps);
                                        movie.looping = startup.looping;
                                        movie
                                    },
                                    movie_scheduler: None,
                                    movie_frame_seq: None,
                                    direct_movie_engine: new_direct_image_movie_engine(),
                                };
                                state.clamp_left_pane_selection();
                                self.browser_session = Some(BrowserSession {
                                    root_path: path,
                                    kind: BrowserSessionKind::Image(Box::new(state)),
                                });
                                self.result_scrolls = [0; RESULT_TAB_COUNT];
                                self.result_hscrolls = [0; RESULT_TAB_COUNT];
                                self.sync_browser_shell_state(true);
                                if let Some(parameters) = self
                                    .browser_session()
                                    .and_then(BrowserSession::image_parameters)
                                {
                                    if let Err(error) =
                                        self.sync_accepted_image_window_parameters(&parameters)
                                    {
                                        if let Some(session) = self.browser_session.take() {
                                            let _ = session.cancel();
                                        }
                                        self.report_browser_error(
                                            "Failed to record accepted imexplore startup parameters.",
                                            format!("{error}\n"),
                                        );
                                        return;
                                    }
                                }
                                self.record_session_opened();
                                self.keep_active_image_plane_selection_visible();
                                if std::env::var_os("CASARS_IMEXPLORE_AUTOSTART_MOVIE").is_some() {
                                    if let Some(fps_text) =
                                        std::env::var_os("CASARS_IMEXPLORE_AUTOSTART_FPS")
                                            .map(|value| value.to_string_lossy().into_owned())
                                    {
                                        crate::movie_debug_log(format!(
                                            "autostart movie fps override requested: {fps_text}"
                                        ));
                                        if let Some(field_index) = self
                                            .fields
                                            .iter()
                                            .position(|field| field.schema.id == "fps")
                                        {
                                            self.fields[field_index].set_text(fps_text);
                                            match self.sync_parameter_from_field(field_index) {
                                                Ok(()) => self
                                                    .apply_live_image_view_parameters_if_needed(
                                                        field_index,
                                                    ),
                                                Err(error) => {
                                                    self.result.status_line = error;
                                                    self.result.status_kind = StatusKind::Warning;
                                                }
                                            }
                                        }
                                    }
                                    crate::movie_debug_log(
                                        "autostart movie requested via CASARS_IMEXPLORE_AUTOSTART_MOVIE",
                                    );
                                    self.toggle_image_movie();
                                }
                            }
                            Err(error) => {
                                let _ = client.cancel();
                                self.report_browser_error(
                                    "Failed to open imexplore.",
                                    format!("{}\n", error.message()),
                                );
                            }
                        }
                    }
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

    fn send_browser_command(&mut self, command: BrowserRequest) -> bool {
        let accepted_command = command.clone();
        let movie_perf = &mut self.movie_perf;
        let mut sync_image_parameters = None::<ImageBrowserParameters>;
        let mut accepted_axis_selection = None::<usize>;
        let mut prior_axis_selection = None::<usize>;
        let result = {
            let Some(session) = self.browser_session.as_mut() else {
                return false;
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
                        BrowserRequest::SetImageCursor { .. }
                        | BrowserRequest::AppendImageRegionVertex { .. }
                        | BrowserRequest::StepImageNonDisplayAxis { .. }
                        | BrowserRequest::StartImageRegionShape
                        | BrowserRequest::CloseImageRegionShape
                        | BrowserRequest::UndoImageRegionVertex
                        | BrowserRequest::CancelImageRegionShape
                        | BrowserRequest::ClearImageRegion
                        | BrowserRequest::SaveImageRegionDefinition
                        | BrowserRequest::LoadNextImageRegionDefinition
                        | BrowserRequest::LoadImageRegionDefinition { .. }
                        | BrowserRequest::RenameImageRegionDefinition { .. }
                        | BrowserRequest::DeleteImageRegionDefinition { .. }
                        | BrowserRequest::SetImageDefaultMask { .. }
                        | BrowserRequest::UnsetImageDefaultMask
                        | BrowserRequest::DeleteImageMask { .. }
                        | BrowserRequest::WriteImageRegionMask
                        | BrowserRequest::SetImagePlaneContentMode { .. }
                        | BrowserRequest::SetImageViewParameters { .. }
                        | BrowserRequest::SetImageProfileAxis { .. }
                        | BrowserRequest::SetImageSelectionReferences { .. } => None,
                    };
                    let Some(request) = request else {
                        return false;
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
                    let font_size = state
                        .panel
                        .as_ref()
                        .map(|panel| panel.font_size)
                        .unwrap_or_else(|| terminal_picker().font_size());
                    let request = match command {
                        BrowserRequest::Resize {
                            width,
                            height,
                            inspector_height,
                        } => Some(ImageBrowserCommand::Resize {
                            viewport: ImageBrowserViewport::with_plane_pixels(
                                width,
                                height,
                                inspector_height,
                                width.saturating_mul(font_size.0.max(1)),
                                height.saturating_mul(font_size.1.max(1)),
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
                                && !state.snapshot.non_display_axes.is_empty()
                            {
                                state.selected_non_display_axis_state().map(|axis_state| {
                                    ImageBrowserCommand::StepNonDisplayAxis {
                                        axis: axis_state.axis,
                                        delta: -(steps as i32),
                                    }
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
                                && !state.snapshot.non_display_axes.is_empty()
                            {
                                state.selected_non_display_axis_state().map(|axis_state| {
                                    ImageBrowserCommand::StepNonDisplayAxis {
                                        axis: axis_state.axis,
                                        delta: steps as i32,
                                    }
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
                        BrowserRequest::MoveUp { steps } => {
                            if state.snapshot.focus == ImageBrowserFocus::Inspector
                                && state.snapshot.non_display_axes.len() > 1
                            {
                                prior_axis_selection = Some(state.selected_non_display_axis);
                                state.selected_non_display_axis =
                                    state.selected_non_display_axis.saturating_sub(steps);
                                state.selected_non_display_axis_state().map(|axis_state| {
                                    accepted_axis_selection = Some(axis_state.axis);
                                    ImageBrowserCommand::SetSelectedNonDisplayAxis {
                                        axis: axis_state.axis,
                                    }
                                })
                            } else {
                                Some(ImageBrowserCommand::MoveCursor {
                                    dx: 0,
                                    dy: -(steps as i32),
                                })
                            }
                        }
                        BrowserRequest::MoveDown { steps } => {
                            if state.snapshot.focus == ImageBrowserFocus::Inspector
                                && state.snapshot.non_display_axes.len() > 1
                            {
                                prior_axis_selection = Some(state.selected_non_display_axis);
                                state.selected_non_display_axis = state
                                    .selected_non_display_axis
                                    .saturating_add(steps)
                                    .min(state.snapshot.non_display_axes.len().saturating_sub(1));
                                state.selected_non_display_axis_state().map(|axis_state| {
                                    accepted_axis_selection = Some(axis_state.axis);
                                    ImageBrowserCommand::SetSelectedNonDisplayAxis {
                                        axis: axis_state.axis,
                                    }
                                })
                            } else {
                                Some(ImageBrowserCommand::MoveCursor {
                                    dx: 0,
                                    dy: steps as i32,
                                })
                            }
                        }
                        BrowserRequest::SetImageCursor { x, y } => {
                            Some(ImageBrowserCommand::SetCursor { x, y })
                        }
                        BrowserRequest::AppendImageRegionVertex { x, y } => {
                            Some(ImageBrowserCommand::AppendRegionVertex { x, y })
                        }
                        BrowserRequest::StepImageNonDisplayAxis { axis, delta } => {
                            Some(ImageBrowserCommand::StepNonDisplayAxis { axis, delta })
                        }
                        BrowserRequest::StartImageRegionShape => {
                            Some(ImageBrowserCommand::StartRegionShape)
                        }
                        BrowserRequest::CloseImageRegionShape => {
                            Some(ImageBrowserCommand::CloseRegionShape)
                        }
                        BrowserRequest::UndoImageRegionVertex => {
                            Some(ImageBrowserCommand::UndoRegionVertex)
                        }
                        BrowserRequest::CancelImageRegionShape => {
                            Some(ImageBrowserCommand::CancelRegionShape)
                        }
                        BrowserRequest::ClearImageRegion => Some(ImageBrowserCommand::ClearRegion),
                        BrowserRequest::SaveImageRegionDefinition => {
                            Some(ImageBrowserCommand::SaveRegionDefinition)
                        }
                        BrowserRequest::LoadNextImageRegionDefinition => {
                            Some(ImageBrowserCommand::LoadNextRegionDefinition)
                        }
                        BrowserRequest::LoadImageRegionDefinition { ref name } => {
                            Some(ImageBrowserCommand::LoadRegionDefinition { name: name.clone() })
                        }
                        BrowserRequest::RenameImageRegionDefinition {
                            ref name,
                            ref new_name,
                        } => Some(ImageBrowserCommand::RenameRegionDefinition {
                            name: name.clone(),
                            new_name: new_name.clone(),
                        }),
                        BrowserRequest::DeleteImageRegionDefinition { ref name } => {
                            Some(ImageBrowserCommand::DeleteRegionDefinition { name: name.clone() })
                        }
                        BrowserRequest::SetImageDefaultMask { ref name } => {
                            Some(ImageBrowserCommand::SetDefaultMask { name: name.clone() })
                        }
                        BrowserRequest::UnsetImageDefaultMask => {
                            Some(ImageBrowserCommand::UnsetDefaultMask)
                        }
                        BrowserRequest::DeleteImageMask { ref name } => {
                            Some(ImageBrowserCommand::DeleteMask { name: name.clone() })
                        }
                        BrowserRequest::WriteImageRegionMask => {
                            Some(ImageBrowserCommand::WriteRegionMask {
                                name: None,
                                set_default: true,
                            })
                        }
                        BrowserRequest::SetImagePlaneContentMode { mode } => {
                            Some(ImageBrowserCommand::SetPlaneContentMode { mode })
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
                        BrowserRequest::SetImageViewParameters { ref parameters } => {
                            Some(ImageBrowserCommand::SetViewWindow {
                                parameters: parameters.clone(),
                            })
                        }
                        BrowserRequest::SetImageProfileAxis { axis } => {
                            Some(ImageBrowserCommand::SetSelectedNonDisplayAxis { axis })
                        }
                        BrowserRequest::SetImageSelectionReferences {
                            ref region,
                            ref mask,
                        } => Some(ImageBrowserCommand::SetSelectionReferences {
                            region: region.clone(),
                            mask: mask.clone(),
                        }),
                        BrowserRequest::Activate
                        | BrowserRequest::Back
                        | BrowserRequest::Escape => None,
                    };
                    let Some(request) = request else {
                        return true;
                    };
                    let movie_frame_seq =
                        matches!(command, BrowserRequest::StepImageNonDisplayAxis { .. })
                            .then_some(state.movie_frame_seq)
                            .flatten();
                    if let Some(frame_seq) = movie_frame_seq {
                        movie_perf.browser_command_sent(frame_seq);
                    }
                    match state.client.request(request) {
                        Ok(snapshot) => {
                            if let BrowserRequest::Resize {
                                width,
                                height,
                                inspector_height,
                            } = command
                            {
                                state.viewport = ImageBrowserViewport::with_plane_pixels(
                                    width,
                                    height,
                                    inspector_height,
                                    width.saturating_mul(font_size.0.max(1)),
                                    height.saturating_mul(font_size.1.max(1)),
                                );
                            }
                            if let Some(frame_seq) = movie_frame_seq {
                                let context =
                                    image_movie_perf_context_from_snapshot(state, &snapshot, None);
                                let backend =
                                    snapshot.backend_timing.as_ref().map(map_backend_timing);
                                movie_perf.browser_snapshot_received(frame_seq, context, backend);
                                state.movie_frame_seq = Some(frame_seq);
                            }
                            state.snapshot = snapshot;
                            state.clamp_left_pane_selection();
                            state.clamp_selected_non_display_axis();
                            if !state.restoring_selected_pinned_probe {
                                state.sync_selected_pinned_probe_from_snapshot();
                            }
                            if let Some(scheduler) = state.movie_scheduler.as_mut() {
                                scheduler.session_indices = state
                                    .snapshot
                                    .non_display_axes
                                    .iter()
                                    .map(|axis| (axis.axis, axis.index))
                                    .collect();
                            }
                            state.snapshot_generation = state.snapshot_generation.saturating_add(1);
                            sync_image_parameters = Some(state.snapshot.parameters.clone());
                            if !state.movie_available() {
                                crate::movie_debug_log(format!(
                                    "stop movie due to snapshot movie_available=false active_view={:?} plane_mode={:?} non_display_axes={}",
                                    state.snapshot.active_view,
                                    state.plane_mode,
                                    state.snapshot.non_display_axes.len()
                                ));
                                movie_perf.movie_stopped(
                                    image_movie_perf_context_from_state(state, None, None, None),
                                    "snapshot movie_available=false",
                                );
                                state.movie.playing = false;
                                state.movie.last_advanced_at = None;
                                state.movie_frame_seq = None;
                            }
                            state.hscroll = state.hscroll.min(
                                image_browser_max_hscroll(&state.snapshot, state.viewport.width)
                                    .min(u16::MAX as usize) as u16,
                            );
                            keep_image_plane_selection_visible(state);
                            Ok(())
                        }
                        Err(error) => {
                            if let Some(frame_seq) = movie_frame_seq {
                                movie_perf.frame_dropped(
                                    Some(frame_seq),
                                    image_movie_perf_context_from_state(state, None, None, None),
                                    MovieFrameOutcome::CacheMiss,
                                    format!("browser request failed: {}", error.message()),
                                );
                                state.movie_frame_seq = None;
                            }
                            Err((error, state.client.stderr_text()))
                        }
                    }
                }
            }
        };

        match result {
            Ok(()) => {
                let durable_parameter_change = match self.sync_accepted_browser_parameters(
                    &accepted_command,
                    accepted_axis_selection,
                    sync_image_parameters.as_ref(),
                ) {
                    Ok(durable) => durable,
                    Err(error) => {
                        self.result.status_line = format!(
                            "Browser updated, but accepted parameters could not be recorded: {error}"
                        );
                        self.result.status_kind = StatusKind::Warning;
                        return true;
                    }
                };
                self.clear_output_selection();
                self.sync_browser_shell_state(true);
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
                if durable_parameter_change {
                    self.accept_live_parameter_changes();
                }
                true
            }
            Err((error, stderr)) => {
                if let Some(index) = prior_axis_selection
                    && let Some(state) = self.image_browser_session_state_mut()
                {
                    state.selected_non_display_axis = index;
                }
                if browser_request_may_change_durable_parameters(
                    &accepted_command,
                    accepted_axis_selection,
                ) {
                    self.rollback_rejected_live_parameter_change();
                }
                let keep_session = !error.is_transport()
                    && self
                        .browser_session()
                        .is_some_and(|session| session.kind() == BrowserAppKind::Image);
                if keep_session {
                    self.stop_image_movie(
                        false,
                        format!("browser request failed: {}", error.message()),
                    );
                }
                if !keep_session {
                    if let Some(session) = self.browser_session.take() {
                        let _ = session.cancel();
                    }
                }
                let details = if stderr.trim().is_empty() {
                    format!("{}\n", error.message())
                } else {
                    format!("{}\n{stderr}", error.message())
                };
                let status = if keep_session {
                    error.message().to_string()
                } else {
                    "Browser command failed. Session closed.".to_string()
                };
                self.report_browser_error(status, details);
                false
            }
        }
    }

    fn sync_accepted_browser_parameters(
        &mut self,
        command: &BrowserRequest,
        accepted_axis_selection: Option<usize>,
        image_parameters: Option<&ImageBrowserParameters>,
    ) -> Result<bool, String> {
        let table_view = self
            .browser_session
            .as_ref()
            .and_then(|session| match &session.kind {
                BrowserSessionKind::Table(state) => Some(state.snapshot.view),
                BrowserSessionKind::Image(_) => None,
            });
        let image_snapshot = self
            .image_browser_session_state()
            .map(|state| state.snapshot.clone());
        let mut durable = false;

        if let Some(axis) = accepted_axis_selection {
            self.set_accepted_parameter_value(
                "movieaxis",
                ParameterValue::String(axis.to_string()),
            )?;
            self.set_accepted_parameter_value(
                "profileaxis",
                ParameterValue::String(axis.to_string()),
            )?;
            durable = true;
        }

        match command {
            BrowserRequest::CycleView { .. } => {
                if let Some(view) = table_view.and_then(table_session_view_parameter) {
                    self.set_accepted_parameter_value(
                        "view",
                        ParameterValue::String(view.to_string()),
                    )?;
                    durable = true;
                } else if let Some(snapshot) = image_snapshot.as_ref() {
                    self.set_accepted_parameter_value(
                        "view",
                        ParameterValue::String(
                            image_session_view_parameter(snapshot.active_view).to_string(),
                        ),
                    )?;
                    durable = true;
                }
            }
            BrowserRequest::SetImageViewParameters { .. } => {
                if let Some(parameters) = image_parameters {
                    self.sync_accepted_image_window_parameters(parameters)?;
                    durable = true;
                }
            }
            BrowserRequest::SetImageProfileAxis { axis } => {
                self.set_accepted_parameter_value(
                    "profileaxis",
                    ParameterValue::String(axis.to_string()),
                )?;
                durable = true;
            }
            BrowserRequest::SetImagePlaneContentMode { mode } => {
                let value = match mode {
                    ImagePlaneContentMode::Raster => "raster",
                    ImagePlaneContentMode::Spreadsheet => "spreadsheet",
                };
                self.set_accepted_parameter_value(
                    "contentmode",
                    ParameterValue::String(value.to_string()),
                )?;
                durable = true;
            }
            BrowserRequest::SetImageSelectionReferences { region, mask } => {
                if let Some(region) = region {
                    self.set_accepted_parameter_value(
                        "region",
                        ParameterValue::String(image_region_reference_parameter(region)),
                    )?;
                    durable = true;
                }
                if let Some(mask) = mask {
                    self.set_accepted_parameter_value(
                        "mask",
                        ParameterValue::String(image_mask_reference_parameter(mask)),
                    )?;
                    durable = true;
                }
            }
            BrowserRequest::ClearImageRegion => {
                self.set_accepted_parameter_value(
                    "region",
                    ParameterValue::String("none".to_string()),
                )?;
                durable = true;
            }
            BrowserRequest::LoadImageRegionDefinition { name } => {
                self.set_accepted_parameter_value(
                    "region",
                    ParameterValue::String(format!("definition:{name}")),
                )?;
                durable = true;
            }
            BrowserRequest::SaveImageRegionDefinition
            | BrowserRequest::LoadNextImageRegionDefinition
            | BrowserRequest::RenameImageRegionDefinition { .. }
            | BrowserRequest::DeleteImageRegionDefinition { .. } => {
                let region = image_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.active_region_definition_name.as_ref())
                    .map(|name| format!("definition:{name}"))
                    .unwrap_or_else(|| "none".to_string());
                self.set_accepted_parameter_value("region", ParameterValue::String(region))?;
                durable = true;
            }
            BrowserRequest::SetImageDefaultMask { name } => {
                self.set_accepted_parameter_value("mask", ParameterValue::String(name.clone()))?;
                durable = true;
            }
            BrowserRequest::UnsetImageDefaultMask => {
                self.set_accepted_parameter_value(
                    "mask",
                    ParameterValue::String("none".to_string()),
                )?;
                durable = true;
            }
            BrowserRequest::DeleteImageMask { .. } | BrowserRequest::WriteImageRegionMask => {
                let mask = image_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.default_mask_name.clone())
                    .unwrap_or_else(|| "none".to_string());
                self.set_accepted_parameter_value("mask", ParameterValue::String(mask))?;
                durable = true;
            }
            _ => {}
        }

        if durable && let Some(session) = self.parameter_session.as_ref() {
            sync_form_fields_from_parameter_session(&mut self.fields, session);
        } else if let Some(parameters) = image_parameters {
            sync_image_parameter_fields(&mut self.fields, parameters);
        }
        Ok(durable)
    }

    fn report_browser_error(&mut self, status_line: impl Into<String>, stderr: String) {
        self.result.status_line = status_line.into();
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

    fn store_plot_snapshot(&mut self, summary: MeasurementSetSummary, reset_plot_cache: bool) {
        let generation = self.plot_workspace.next_generation;
        self.plot_workspace.next_generation += 1;
        self.plot_workspace.snapshot = Some(MeasurementSetRunSnapshot {
            generation,
            path: summary.measurement_set.path.as_ref().map(PathBuf::from),
            options: summary.options.clone(),
            summary,
            dirty: false,
        });
        if reset_plot_cache {
            self.plot_workspace.panel = None;
        }
    }

    fn record_plot_snapshot(&mut self, summary: MeasurementSetSummary) {
        self.store_plot_snapshot(summary, true);
    }

    fn record_msexplore_summary_snapshot(&mut self, summary: MeasurementSetSummary) {
        self.store_plot_snapshot(summary, false);
    }

    fn zoom_image_view(&mut self, zoom_in: bool) {
        let Some(parameters) = self
            .browser_session()
            .and_then(BrowserSession::image_snapshot)
            .and_then(|snapshot| image_zoom_parameters(snapshot, zoom_in))
        else {
            self.result.status_line = "Zoom controls are only available for plane views.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.send_browser_command(BrowserRequest::SetImageViewParameters { parameters });
        self.result.status_line = if zoom_in {
            "Zoomed in.".into()
        } else {
            "Zoomed out.".into()
        };
        self.result.status_kind = StatusKind::Info;
    }

    fn pan_image_view(&mut self, dx: i32, dy: i32) {
        let Some(parameters) = self
            .browser_session()
            .and_then(BrowserSession::image_snapshot)
            .and_then(|snapshot| image_pan_parameters(snapshot, dx, dy))
        else {
            self.result.status_line = "Pan controls are only available for plane views.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.send_browser_command(BrowserRequest::SetImageViewParameters { parameters });
        self.result.status_line = "Panned image view.".into();
        self.result.status_kind = StatusKind::Info;
    }

    fn reset_image_view_window(&mut self) {
        let Some(snapshot) = self
            .browser_session()
            .and_then(BrowserSession::image_snapshot)
        else {
            return;
        };
        if snapshot.active_view != ImageBrowserView::Plane {
            self.result.status_line = "Reset view is only available in the Plane view.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        self.send_browser_command(BrowserRequest::SetImageViewParameters {
            parameters: ImageBrowserParameters {
                blc: String::new(),
                trc: String::new(),
                inc: String::new(),
                stretch: snapshot.parameters.stretch.clone(),
                autoscale: snapshot.parameters.autoscale.clone(),
                clip_low: snapshot.parameters.clip_low.clone(),
                clip_high: snapshot.parameters.clip_high.clone(),
            },
        });
        self.result.status_line = "Reset image view.".into();
        self.result.status_kind = StatusKind::Info;
    }

    fn cycle_image_plane_colormap(&mut self) {
        let Some(colormap) = self.image_browser_session_state_mut().map(|state| {
            state.plane_colormap = state.plane_colormap.next();
            state.snapshot_generation = state.snapshot_generation.saturating_add(1);
            state.plane_colormap
        }) else {
            return;
        };
        if let Err(error) = self.set_accepted_parameter_value(
            "colormap",
            ParameterValue::String(image_colormap_parameter(colormap).to_string()),
        ) {
            self.result.status_line =
                format!("Colormap changed, but its parameter could not be recorded: {error}");
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        if let Some(session) = self.parameter_session.as_ref() {
            sync_form_fields_from_parameter_session(&mut self.fields, session);
        }
        self.accept_live_parameter_changes();
        self.result.status_line = format!("Colormap: {}.", colormap.label());
        self.result.status_kind = StatusKind::Info;
    }

    fn toggle_image_plane_invert(&mut self) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        state.plane_invert = !state.plane_invert;
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.result.status_line = if state.plane_invert {
            "Image colors inverted.".into()
        } else {
            "Image colors restored.".into()
        };
        self.result.status_kind = StatusKind::Info;
    }

    fn clear_plot_render_cache(&mut self) {
        self.plot_workspace.placeholder_protocol = self.build_blank_plot_protocol();
        self.plot_workspace.panel = None;
        self.plot_workspace.preview_invalidated = true;
    }

    pub(crate) fn is_msexplore_app(&self) -> bool {
        self.app.id == "msexplore"
    }

    fn msexplore_form_has_plot_spec(&self) -> bool {
        self.field_text("page_spec")
            .is_some_and(|value| !value.trim().is_empty())
            || self
                .field_text("preset")
                .is_some_and(|value| !value.trim().is_empty())
            || (self
                .field_text("x_axis")
                .is_some_and(|value| !value.trim().is_empty())
                && self
                    .field_text("y_axis")
                    .is_some_and(|value| !value.trim().is_empty()))
    }

    fn msexplore_plot_label(&self) -> String {
        if let Some(path) = self
            .field_text("page_spec")
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
        {
            return path
                .file_name()
                .and_then(|value| value.to_str())
                .map(|value| format!("Current Page ({value})"))
                .unwrap_or_else(|| "Current Page".to_string());
        }
        if let Some(preset) = self
            .field_text("preset")
            .filter(|value| !value.trim().is_empty())
        {
            return MsPlotPreset::parse(&preset)
                .map(|preset| preset.display_name().to_string())
                .unwrap_or_else(|_| {
                    preset
                        .split('_')
                        .map(|token| {
                            let mut chars = token.chars();
                            match chars.next() {
                                Some(first) => {
                                    first.to_ascii_uppercase().to_string() + chars.as_str()
                                }
                                None => String::new(),
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                });
        }
        if let (Some(y_axis), Some(x_axis)) = (
            self.field_text("y_axis")
                .filter(|value| !value.trim().is_empty()),
            self.field_text("x_axis")
                .filter(|value| !value.trim().is_empty()),
        ) {
            return format!("{y_axis} vs {x_axis}");
        }
        "Current Plot".to_string()
    }

    pub(crate) fn selected_plot_label(&self) -> String {
        if self.app.id == "calibrate" {
            let target = self
                .current_plot_catalog_target()
                .unwrap_or_else(|| self.default_calibration_plot_target());
            return match target {
                PlotCatalogTarget::Calibration(preset) => preset.display_name().to_string(),
                PlotCatalogTarget::Imaging(_)
                | PlotCatalogTarget::Preset(_)
                | PlotCatalogTarget::CustomPlot
                | PlotCatalogTarget::PageSpec => "Calibration Plot".to_string(),
            };
        }
        if self.app.id == "imager" {
            let report = match self.current_imaging_report() {
                Some(report) => report,
                None => return "Imaging Diagnostics".to_string(),
            };
            let target = self
                .current_plot_catalog_target()
                .unwrap_or(PlotCatalogTarget::Imaging(imaging_preferred_diagnostic(
                    report,
                )));
            return match target {
                PlotCatalogTarget::Imaging(kind) => kind.label().to_string(),
                PlotCatalogTarget::Calibration(_)
                | PlotCatalogTarget::Preset(_)
                | PlotCatalogTarget::CustomPlot
                | PlotCatalogTarget::PageSpec => "Imaging Diagnostics".to_string(),
            };
        }
        self.msexplore_plot_label()
    }

    fn current_plot_summary(&self) -> Option<String> {
        if self.app.id == "calibrate" {
            let target = self
                .current_plot_catalog_target()
                .unwrap_or_else(|| self.default_calibration_plot_target());
            let PlotCatalogTarget::Calibration(preset) = target else {
                return Some("Choose one calibration plot preset.".to_string());
            };
            if preset.uses_calibration_table() {
                return Some(render_workflow_diagnostic_summary(
                    &WorkflowDiagnosticSummaryDisplay {
                        description: preset.summary().to_string(),
                        source_path: self
                            .current_calibration_plot_table_path()
                            .map(|path| path.display().to_string()),
                        missing_source_message: Some(
                            "Choose or enter a calibration table to preview inspection plots."
                                .to_string(),
                        ),
                    },
                ));
            }
            let ms_path = self.field_text("vis").unwrap_or_default();
            return Some(render_workflow_diagnostic_summary(
                &WorkflowDiagnosticSummaryDisplay {
                    description: preset.summary().to_string(),
                    source_path: (!ms_path.trim().is_empty()).then(|| ms_path.trim().to_string()),
                    missing_source_message: Some(
                        "Enter a MeasurementSet path to preview corrected-data diagnostics."
                            .to_string(),
                    ),
                },
            ));
        }
        if self.app.id == "imager" {
            let report = self.current_imaging_report()?;
            let target = self
                .current_plot_catalog_target()
                .unwrap_or(PlotCatalogTarget::Imaging(imaging_preferred_diagnostic(
                    report,
                )));
            return Some(match target {
                PlotCatalogTarget::Imaging(ImagingDiagnosticKind::ResidualByChannel) => {
                    "Per-channel residual peak before and after deconvolution.".to_string()
                }
                PlotCatalogTarget::Imaging(ImagingDiagnosticKind::IterationsByChannel) => {
                    "Per-channel major-cycle and minor-iteration counts.".to_string()
                }
                PlotCatalogTarget::Imaging(kind) => {
                    format!(
                        "Preview the {} written by the latest imaging run.",
                        kind.label()
                    )
                }
                PlotCatalogTarget::Calibration(_)
                | PlotCatalogTarget::Preset(_)
                | PlotCatalogTarget::CustomPlot
                | PlotCatalogTarget::PageSpec => "Choose an imaging diagnostic.".to_string(),
            });
        }
        let ms_path = self.field_text("vis").unwrap_or_default();
        if ms_path.trim().is_empty() {
            return Some(
                "Enter a MeasurementSet path to preview the current msexplore form.".to_string(),
            );
        }
        if self.msexplore_form_has_plot_spec() {
            return Some(format!(
                "{} from the current msexplore form.",
                self.selected_plot_label()
            ));
        }
        Some(
            "Choose a preset, a page spec, or both x/y axes to preview the current msexplore form."
                .to_string(),
        )
    }

    fn current_structured_summary(&self) -> Option<&MeasurementSetSummary> {
        if self.is_msexplore_app() {
            return self
                .plot_workspace
                .snapshot
                .as_ref()
                .map(|snapshot| &snapshot.summary);
        }
        if matches!(self.app.id.as_str(), "calibrate" | "imager")
            && self.result.structured.is_none()
            && let Some(snapshot) = self.plot_workspace.snapshot.as_ref()
        {
            return Some(&snapshot.summary);
        }
        match self.result.structured.as_ref() {
            Some(StructuredResult::MeasurementSetSummary(summary)) => Some(summary.as_ref()),
            Some(StructuredResult::Calibration(_)) | Some(StructuredResult::Imaging(_)) | None => {
                None
            }
        }
    }

    fn current_calibration_report(&self) -> Option<&ManagedCalibrationOutput> {
        match self.result.structured.as_ref() {
            Some(StructuredResult::Calibration(report)) => Some(report.as_ref()),
            Some(StructuredResult::MeasurementSetSummary(_))
            | Some(StructuredResult::Imaging(_))
            | None => None,
        }
    }

    fn current_imaging_report(&self) -> Option<&ManagedImagingOutput> {
        match self.result.structured.as_ref() {
            Some(StructuredResult::Imaging(report)) => Some(report.as_ref()),
            Some(StructuredResult::MeasurementSetSummary(_))
            | Some(StructuredResult::Calibration(_))
            | None => None,
        }
    }

    fn next_history_sequence(&self) -> usize {
        self.history_entries.len() + 1
    }

    fn record_history_entry(
        &mut self,
        stage: Option<WorkflowStageId>,
        title: String,
        status_kind: StatusKind,
        details: Vec<String>,
    ) {
        self.history_entries.push(HistoryEntry {
            sequence: self.next_history_sequence(),
            stage,
            title,
            status_kind,
            details,
        });
    }

    fn record_calibration_products(
        &mut self,
        report: &ManagedCalibrationOutput,
        run_sequence: usize,
    ) {
        let product = workflow_product_metadata_from_report(report);
        let Some((path, stage, family, provenance)) = product else {
            return;
        };
        let dependency_revisions = self.active_workflow_dependency_revisions();
        for record in self.workflow_products.iter_mut().filter(|record| {
            record.stage == stage && record.status == WorkflowProductStatus::Active
        }) {
            record.status = WorkflowProductStatus::Superseded;
        }
        let revision = self
            .workflow_products
            .iter()
            .filter(|record| record.stage == stage)
            .count()
            + 1;
        self.workflow_products.push(WorkflowProductRecord {
            path,
            stage,
            family,
            revision,
            provenance,
            status: WorkflowProductStatus::Active,
            dependency_revisions,
            run_sequence,
        });
        let snapshots = self.workflow_product_snapshots();
        let stale_indices = stale_descendant_product_indices(
            &calibration_stage_specs(),
            &snapshots,
            stage.key(),
            revision,
        );
        for index in stale_indices {
            if let Some(product) = self.workflow_products.get_mut(index)
                && product.status == WorkflowProductStatus::Active
            {
                product.status = WorkflowProductStatus::Stale;
            }
        }
    }

    fn workflow_recommended_next_stage(&self) -> Option<WorkflowStageId> {
        self.workflow_stage_states()
            .into_iter()
            .find(|state| state.recommended)
            .and_then(|state| WorkflowStageId::from_key(state.id))
    }

    fn apply_workflow_post_run_guidance(&mut self, report: &ManagedCalibrationOutput) {
        if !(self.app.shell_kind() == AppShellKind::Workflow && self.app.id == "calibrate") {
            return;
        }

        match report {
            ManagedCalibrationOutput::SolveGain(report) => {
                self.apply_inspection_defaults_for_path(report.output_table.display().to_string());
                self.activate_result_tab(ResultTab::Diagnostics);
            }
            ManagedCalibrationOutput::SolveBandpass(report) => {
                self.apply_inspection_defaults_for_path(report.output_table.display().to_string());
                self.activate_result_tab(ResultTab::Diagnostics);
            }
            ManagedCalibrationOutput::FluxScale(report) => {
                self.apply_inspection_defaults_for_path(report.output_table.display().to_string());
                self.activate_result_tab(ResultTab::Diagnostics);
            }
            ManagedCalibrationOutput::Gencal(report) => {
                self.apply_inspection_defaults_for_path(report.output_table.display().to_string());
                self.activate_result_tab(ResultTab::Diagnostics);
            }
            ManagedCalibrationOutput::Stats(report) => {
                self.apply_inspection_defaults_for_path(report.path.display().to_string());
                self.activate_result_tab(ResultTab::Diagnostics);
            }
            ManagedCalibrationOutput::Apply(_) => {
                if let Some(preset) =
                    workflow_preferred_diagnostic_preset_for_stage(WorkflowStageId::Apply)
                {
                    self.plot_workspace.selected_catalog_target =
                        Some(PlotCatalogTarget::Calibration(preset));
                }
                self.activate_result_tab(ResultTab::Diagnostics);
            }
            ManagedCalibrationOutput::ExportCorrectedData(_) => {
                self.activate_result_tab(ResultTab::Overview);
            }
            ManagedCalibrationOutput::ContinuumSubtract(_) => {
                self.activate_result_tab(ResultTab::Overview);
            }
            ManagedCalibrationOutput::Summary(_) => {
                self.activate_result_tab(ResultTab::Overview);
            }
            ManagedCalibrationOutput::PlanApply(_) => {}
        }

        let Some(completed_stage) = workflow_stage_from_report(report) else {
            return;
        };
        let next_stage = self.workflow_recommended_next_stage();
        if let Some(next_stage) = next_stage
            && next_stage != completed_stage
        {
            self.set_current_workflow_stage(next_stage);
            self.result.status_line = format!(
                "{} completed. Recommended next stage: {}.",
                completed_stage.label(),
                next_stage.label()
            );
            self.result.status_kind = StatusKind::Ok;
            return;
        }

        self.result.status_line = format!("{} completed.", completed_stage.label());
        self.result.status_kind = StatusKind::Ok;
    }

    fn apply_imaging_post_run_guidance(&mut self, report: &ManagedImagingOutput) {
        if !(self.app.shell_kind() == AppShellKind::Workflow && self.app.id == "imager") {
            return;
        }
        self.plot_workspace.selected_catalog_target = Some(PlotCatalogTarget::Imaging(
            imaging_preferred_diagnostic(report),
        ));
        self.clear_plot_render_cache();
        self.activate_result_tab(ResultTab::Diagnostics);
        self.result.status_line = format!(
            "Imaging completed. {} artifacts recorded at {}.",
            report.artifacts.len(),
            report.request.imagename
        );
        self.result.status_kind = StatusKind::Ok;
    }

    fn apply_inspection_defaults_for_path(&mut self, path: String) {
        let _ = self.apply_startup_text_value("table_path", path.clone());
        let _ = self.apply_startup_text_value("summary_paths", path);
    }

    fn calibration_history_entry(
        report: &ManagedCalibrationOutput,
        sequence: usize,
    ) -> HistoryEntry {
        let (stage, title, details) = match report {
            ManagedCalibrationOutput::Apply(report) => (
                Some(WorkflowStageId::Apply),
                "Apply".to_string(),
                vec![
                    format!("rows updated={}", report.updated_row_count),
                    format!(
                        "measurement set={}",
                        report
                            .plan
                            .measurement_set_path
                            .as_ref()
                            .map(|path| path.display().to_string())
                            .unwrap_or_else(|| "<in-memory>".to_string())
                    ),
                ],
            ),
            ManagedCalibrationOutput::ExportCorrectedData(report) => (
                Some(WorkflowStageId::Apply),
                "Export Corrected Data".to_string(),
                vec![
                    format!("output={}", report.output_ms.display()),
                    format!("rows={}", report.row_count),
                ],
            ),
            ManagedCalibrationOutput::ContinuumSubtract(report) => (
                Some(WorkflowStageId::Apply),
                "Continuum Subtract".to_string(),
                vec![
                    format!("output={}", report.output_ms.display()),
                    format!("rows={}", report.row_count),
                    format!("fit_order={}", report.fit_order),
                ],
            ),
            ManagedCalibrationOutput::Summary(summaries) => (
                Some(WorkflowStageId::InspectDataset),
                "Inspect Dataset".to_string(),
                vec![format!("tables={}", summaries.len())],
            ),
            ManagedCalibrationOutput::PlanApply(plan) => (
                None,
                "Plan Apply".to_string(),
                vec![
                    format!("rows selected={}", plan.selected_row_count),
                    format!("tables={}", plan.calibration_tables.len()),
                ],
            ),
            ManagedCalibrationOutput::Stats(report) => (
                Some(WorkflowStageId::InspectResults),
                "Inspect Results".to_string(),
                vec![
                    format!("table={}", report.path.display()),
                    format!("axis={:?}", report.axis),
                ],
            ),
            ManagedCalibrationOutput::SolveGain(report) => (
                Some(WorkflowStageId::SolveGain),
                "Solve Gain".to_string(),
                vec![
                    format!("output={}", report.output_table.display()),
                    format!("rows={}", report.solution_row_count),
                ],
            ),
            ManagedCalibrationOutput::SolveBandpass(report) => (
                Some(WorkflowStageId::SolveBandpass),
                "Solve Bandpass".to_string(),
                vec![
                    format!("output={}", report.output_table.display()),
                    format!("rows={}", report.solution_row_count),
                ],
            ),
            ManagedCalibrationOutput::FluxScale(report) => (
                Some(WorkflowStageId::FluxScale),
                "Fluxscale".to_string(),
                vec![
                    format!("output={}", report.output_table.display()),
                    format!("fields={}", report.fields.len()),
                ],
            ),
            ManagedCalibrationOutput::Gencal(report) => (
                Some(WorkflowStageId::InspectResults),
                "Generate Prior Cal".to_string(),
                vec![
                    format!("output={}", report.output_table.display()),
                    format!("rows={}", report.row_count),
                    format!("subtype={}", report.table_subtype),
                ],
            ),
        };
        HistoryEntry {
            sequence,
            stage,
            title,
            status_kind: StatusKind::Ok,
            details,
        }
    }

    fn is_summary_tab(tab: ResultTab) -> bool {
        matches!(
            tab,
            ResultTab::Overview
                | ResultTab::Data
                | ResultTab::Observations
                | ResultTab::Scans
                | ResultTab::Fields
                | ResultTab::Spws
                | ResultTab::Sources
                | ResultTab::Antennas
        )
    }

    fn current_msexplore_summary_request(
        &self,
    ) -> Result<(PathBuf, MeasurementSetSummaryOptions), String> {
        let ms_path = self
            .field_text("vis")
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                "Enter a MeasurementSet path to populate the summary tabs.".to_string()
            })?;
        let selection_value =
            |id: &str| self.field_text(id).filter(|value| !value.trim().is_empty());
        let selection = MsSelectionSpec {
            selectdata: self.required_parameter_bool("selectdata")?,
            field: selection_value("field"),
            spw: selection_value("spw"),
            timerange: selection_value("timerange"),
            uvrange: selection_value("uvrange"),
            antenna: selection_value("antenna"),
            scan: selection_value("scan"),
            correlation: selection_value("correlation"),
            observation: selection_value("observation"),
            array: selection_value("array"),
            intent: selection_value("intent"),
            feed: selection_value("feed"),
            msselect: selection_value("msselect"),
        };
        Ok((ms_path, selection.to_summary_options()))
    }

    fn current_calibrate_summary_request(
        &self,
    ) -> Result<(PathBuf, MeasurementSetSummaryOptions), String> {
        let ms_path = self
            .field_text("vis")
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                "Enter a MeasurementSet path to populate the summary tabs.".to_string()
            })?;
        let selection_value =
            |id: &str| self.field_text(id).filter(|value| !value.trim().is_empty());
        let options = MeasurementSetSummaryOptions {
            verbose: self.verbose_enabled(),
            selectdata: self.required_parameter_bool("selectdata")?,
            field: selection_value("field"),
            spw: selection_value("spw"),
            antenna: selection_value("antenna"),
            scan: selection_value("scan"),
            observation: selection_value("observation"),
            array: selection_value("array"),
            timerange: selection_value("timerange"),
            uvrange: None,
            correlation: None,
            intent: None,
            msselect: selection_value("msselect"),
            feed: None,
            listunfl: self.listunfl_enabled(),
            cachesize_mb: None,
        };
        Ok((ms_path, options))
    }

    fn current_imager_summary_request(
        &self,
    ) -> Result<(PathBuf, MeasurementSetSummaryOptions), String> {
        let ms_path = self
            .field_text("vis")
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                "Enter a MeasurementSet path to populate the summary tabs.".to_string()
            })?;
        let selection_value =
            |id: &str| self.field_text(id).filter(|value| !value.trim().is_empty());
        let options = MeasurementSetSummaryOptions {
            verbose: self.verbose_enabled(),
            selectdata: true,
            field: selection_value("field"),
            spw: selection_value("spw"),
            antenna: None,
            scan: None,
            observation: None,
            array: None,
            timerange: None,
            uvrange: None,
            correlation: imager_summary_correlation_selector(self.field_text("polarization")),
            intent: None,
            msselect: None,
            feed: None,
            listunfl: self.listunfl_enabled(),
            cachesize_mb: None,
        };
        Ok((ms_path, options))
    }

    fn current_calibrate_selection_spec(&self) -> MsSelectionSpec {
        let selection_value =
            |id: &str| self.field_text(id).filter(|value| !value.trim().is_empty());
        MsSelectionSpec {
            // This non-fallible plot-preview path fails closed if its typed
            // session is unavailable; it never recreates the catalog default.
            selectdata: matches!(
                self.session_parameter_value("selectdata"),
                Some(ParameterValue::Bool(true))
            ),
            field: selection_value("field"),
            spw: selection_value("spw"),
            timerange: selection_value("timerange"),
            uvrange: None,
            antenna: selection_value("antenna"),
            scan: selection_value("scan"),
            correlation: None,
            array: selection_value("array"),
            observation: selection_value("observation"),
            intent: None,
            feed: None,
            msselect: selection_value("msselect"),
        }
    }

    fn current_calibration_plot_request(&self) -> CalibrationPlotRequest {
        CalibrationPlotRequest {
            measurement_set_path: self
                .field_text("vis")
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from),
            calibration_table_path: self.current_calibration_plot_table_path(),
            selection: self.current_calibrate_selection_spec(),
        }
    }

    fn current_calibration_plot_table_path(&self) -> Option<PathBuf> {
        self.first_path_value("table_path")
            .or_else(|| self.first_csv_path_value("gaintables"))
            .or_else(|| self.first_path_value("fluxscale_input"))
            .or_else(|| self.first_csv_path_value("summary_paths"))
    }

    fn default_calibration_plot_target(&self) -> PlotCatalogTarget {
        if self.current_calibration_plot_table_path().is_some() {
            PlotCatalogTarget::Calibration(preferred_workflow_calibration_preset(
                WorkflowCalibrationArtifactKind::GainLike,
            ))
        } else {
            PlotCatalogTarget::Calibration(preferred_workflow_calibration_preset(
                WorkflowCalibrationArtifactKind::CorrectedData,
            ))
        }
    }

    fn first_path_value(&self, field_id: &str) -> Option<PathBuf> {
        self.field_text(field_id)
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    }

    fn first_csv_path_value(&self, field_id: &str) -> Option<PathBuf> {
        self.field_text(field_id).and_then(|value| {
            value
                .split(',')
                .map(str::trim)
                .find(|entry| !entry.is_empty())
                .map(PathBuf::from)
        })
    }

    fn ensure_current_summary_snapshot_if_needed(&mut self) {
        if !Self::is_summary_tab(self.active_result_tab) {
            return;
        }
        let (path, options) = if self.is_msexplore_app() {
            match self.current_msexplore_summary_request() {
                Ok(request) => request,
                Err(error) => {
                    self.plot_workspace.snapshot = None;
                    self.result.structured_error = Some(error);
                    return;
                }
            }
        } else if self.app.id == "imager" {
            match self.current_imager_summary_request() {
                Ok(request) => request,
                Err(error) => {
                    self.plot_workspace.snapshot = None;
                    self.result.structured_error = Some(error);
                    return;
                }
            }
        } else if self.app.id == "calibrate" && self.result.structured.is_none() {
            match self.current_calibrate_summary_request() {
                Ok(request) => request,
                Err(error) => {
                    self.plot_workspace.snapshot = None;
                    self.result.structured_error = Some(error);
                    return;
                }
            }
        } else {
            return;
        };
        if let Some(snapshot) = self.plot_workspace.snapshot.as_mut()
            && snapshot.path.as_ref() == Some(&path)
            && snapshot.options == options
        {
            snapshot.dirty = false;
            self.result.structured_error = None;
            return;
        }
        match MeasurementSet::open(&path)
            .map_err(|error| format!("open MeasurementSet {}: {error}", path.display()))
            .and_then(|ms| {
                MeasurementSetSummary::from_ms_with_options(&ms, &options)
                    .map_err(|error| error.to_string())
            }) {
            Ok(summary) => {
                self.record_msexplore_summary_snapshot(summary);
                self.result.structured_error = None;
            }
            Err(error) => {
                self.plot_workspace.snapshot = None;
                self.result.structured_error = Some(error);
            }
        }
    }

    pub(crate) fn prime_idle_summary_for_launch(&mut self) {
        self.ensure_current_summary_snapshot_if_needed();
    }

    fn current_msexplore_plot_payload(&self) -> Result<MsPlotPayload, String> {
        let plan = self.build_execution_plan()?;
        let spec = build_explore_spec_from_args(plan.arguments)?;
        build_msexplore_payload_from_spec(&spec)
    }

    fn current_imaging_plot_payload(&self) -> Result<ImagingPlotPayload, String> {
        let report = self
            .current_imaging_report()
            .ok_or_else(|| "run imaging first".to_string())?;
        let target = self
            .current_plot_catalog_target()
            .unwrap_or(PlotCatalogTarget::Imaging(imaging_preferred_diagnostic(
                report,
            )));
        match target {
            PlotCatalogTarget::Imaging(ImagingDiagnosticKind::ResidualByChannel) => {
                let initial = report
                    .run
                    .channels
                    .iter()
                    .map(|channel| {
                        (
                            channel.channel_index,
                            channel.initial_residual_peak_jy_per_beam as f64,
                        )
                    })
                    .collect::<Vec<_>>();
                let final_values = report
                    .run
                    .channels
                    .iter()
                    .map(|channel| {
                        (
                            channel.channel_index,
                            channel.final_residual_peak_jy_per_beam as f64,
                        )
                    })
                    .collect::<Vec<_>>();
                Ok(ImagingPlotPayload::ChannelSeries {
                    title: "Residual Peak By Channel".to_string(),
                    y_label: "Jy / beam".to_string(),
                    series: vec![
                        ImagingPlotSeries {
                            label: "Initial".to_string(),
                            points: initial,
                            color_index: 0,
                        },
                        ImagingPlotSeries {
                            label: "Final".to_string(),
                            points: final_values,
                            color_index: 1,
                        },
                    ],
                })
            }
            PlotCatalogTarget::Imaging(ImagingDiagnosticKind::IterationsByChannel) => {
                Ok(ImagingPlotPayload::ChannelSeries {
                    title: "Cycle Counts By Channel".to_string(),
                    y_label: "Count".to_string(),
                    series: vec![
                        ImagingPlotSeries {
                            label: "Major cycles".to_string(),
                            points: report
                                .run
                                .channels
                                .iter()
                                .map(|channel| (channel.channel_index, channel.major_cycles as f64))
                                .collect(),
                            color_index: 2,
                        },
                        ImagingPlotSeries {
                            label: "Minor iterations".to_string(),
                            points: report
                                .run
                                .channels
                                .iter()
                                .map(|channel| {
                                    (channel.channel_index, channel.minor_iterations as f64)
                                })
                                .collect(),
                            color_index: 3,
                        },
                    ],
                })
            }
            PlotCatalogTarget::Imaging(kind) => {
                let artifact_kind = match kind {
                    ImagingDiagnosticKind::PsfPreview => "psf",
                    ImagingDiagnosticKind::ResidualPreview => "residual",
                    ImagingDiagnosticKind::ModelPreview => "model",
                    ImagingDiagnosticKind::ImagePreview => "image",
                    ImagingDiagnosticKind::AlphaPreview => "alpha",
                    ImagingDiagnosticKind::ResidualByChannel
                    | ImagingDiagnosticKind::IterationsByChannel => unreachable!(),
                };
                let artifact = report
                    .artifacts
                    .iter()
                    .find(|artifact| artifact.kind == artifact_kind)
                    .ok_or_else(|| format!("no artifact recorded for {artifact_kind}"))?;
                let preview = artifact
                    .preview_png_path
                    .as_ref()
                    .ok_or_else(|| format!("no preview image available for {}", artifact.label))?;
                Ok(ImagingPlotPayload::ArtifactPreview {
                    title: artifact.label.clone(),
                    image_path: PathBuf::from(preview),
                })
            }
            PlotCatalogTarget::Calibration(_)
            | PlotCatalogTarget::Preset(_)
            | PlotCatalogTarget::CustomPlot
            | PlotCatalogTarget::PageSpec => Err("choose an imaging diagnostic".to_string()),
        }
    }

    fn current_plot_payload(&self) -> Result<CurrentPlotPayload, String> {
        if self.app.id == "calibrate" {
            let target = self
                .current_plot_catalog_target()
                .unwrap_or_else(|| self.default_calibration_plot_target());
            let PlotCatalogTarget::Calibration(preset) = target else {
                return Err("choose one calibration plot preset".to_string());
            };
            return build_calibration_plot_payload(
                &self.current_calibration_plot_request(),
                preset,
            )
            .map(CurrentPlotPayload::MsPlot)
            .map_err(|error| error.to_string());
        }
        if self.app.id == "imager" {
            return self
                .current_imaging_plot_payload()
                .map(CurrentPlotPayload::Imaging);
        }
        self.current_msexplore_plot_payload()
            .map(CurrentPlotPayload::MsPlot)
    }

    fn current_plot_spec_key(&self) -> Result<String, String> {
        if self.app.id == "calibrate" {
            let target = self
                .current_plot_catalog_target()
                .unwrap_or_else(|| self.default_calibration_plot_target());
            let mut parts = vec![self.selected_plot_label()];
            if let Some(path) = self
                .field_text("vis")
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(format!("ms={}", path.trim()));
            }
            if let Some(path) = self.current_calibration_plot_table_path() {
                parts.push(format!("table={}", path.display()));
            }
            let selection = self.current_calibrate_selection_spec();
            for (label, value) in [
                ("field", selection.field),
                ("spw", selection.spw),
                ("timerange", selection.timerange),
                ("antenna", selection.antenna),
                ("scan", selection.scan),
                ("observation", selection.observation),
                ("array", selection.array),
                ("msselect", selection.msselect),
            ] {
                if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
                    parts.push(format!("{label}={value}"));
                }
            }
            parts.push(format!("target={target:?}"));
            return Ok(parts.join("\u{1f}"));
        }
        if self.app.id == "imager" {
            let report = self
                .current_imaging_report()
                .ok_or_else(|| "run imaging first".to_string())?;
            let target = self
                .current_plot_catalog_target()
                .unwrap_or(PlotCatalogTarget::Imaging(imaging_preferred_diagnostic(
                    report,
                )));
            return Ok(format!(
                "{}\u{1f}{}\u{1f}{target:?}",
                report.request.imagename, report.request.spectral_mode
            ));
        }
        self.build_execution_plan().map(|plan| {
            plan.arguments
                .iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("\u{1f}")
        })
    }

    fn pump_plot_panel(&mut self) {
        let plot_label = self.selected_plot_label();
        let was_pending = self.plot_workspace.preview_invalidated;
        let Some(panel) = self.plot_workspace.panel.as_mut() else {
            return;
        };
        match panel.renderer.pump() {
            Ok(changed) => {
                if changed {
                    panel.image_size = panel.renderer.image_size();
                    self.plot_workspace.preview_invalidated = false;
                    self.plot_workspace.placeholder_protocol = None;
                    if matches!(self.result.status_kind, StatusKind::Info)
                        && (self.result.status_line.contains("preview")
                            || self.result.status_line.contains("Rendering"))
                    {
                        self.result.status_line = format!("{plot_label} preview ready.");
                        self.result.status_kind = StatusKind::Ok;
                    }
                }
                if !changed
                    && was_pending
                    && panel.renderer.protocol().is_some()
                    && matches!(self.result.status_kind, StatusKind::Info)
                {
                    self.plot_workspace.preview_invalidated = false;
                    self.plot_workspace.placeholder_protocol = None;
                    self.result.status_line = format!("{plot_label} preview ready.");
                    self.result.status_kind = StatusKind::Ok;
                }
            }
            Err(error) => {
                panel.last_error = Some(error.to_string());
                self.plot_workspace.preview_invalidated = false;
                self.plot_workspace.placeholder_protocol = None;
                self.result.status_line = "Plot rendering failed.".to_string();
                self.result.status_kind = StatusKind::Warning;
            }
        }
    }

    fn build_blank_plot_protocol(&self) -> Option<PanelProtocol> {
        let panel = self.plot_workspace.panel.as_ref()?;
        let area = panel.request_key.as_ref()?.area;
        if area.is_empty() {
            return None;
        }
        let pixel_width = panel
            .image_size
            .map(|(width, _)| width)
            .unwrap_or_else(|| u32::from(area.width.max(1)) * u32::from(panel.font_size.0.max(1)));
        let pixel_height = panel
            .image_size
            .map(|(_, height)| height)
            .unwrap_or_else(|| u32::from(area.height.max(1)) * u32::from(panel.font_size.1.max(1)));
        let background = plot_theme(self.theme_mode()).background;
        let image = RgbaImage::from_pixel(
            pixel_width.max(1),
            pixel_height.max(1),
            image::Rgba([background[0], background[1], background[2], 255]),
        );
        let picker = terminal_picker();
        build_panel_protocol_from_rgba_owned(&picker, Resize::Fit(None), area, image)
            .ok()
            .map(|prepared| prepared.protocol)
    }

    fn pump_image_plane_panel(&mut self) {
        let context = self
            .image_browser_session_state()
            .map(|state| image_movie_perf_context_from_state(state, None, None, None));
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        let Some(panel) = state.panel.as_mut() else {
            return;
        };
        let stale_count = panel.renderer.take_stale_result_count();
        let pump_result = match panel.renderer.pump() {
            Ok(changed) => {
                if changed {
                    panel.image_size = panel.renderer.image_size();
                    let request_hash = panel
                        .pending_request_key
                        .as_ref()
                        .map(hashed_render_request_key)
                        .or_else(|| panel.display_key.as_ref().map(hashed_render_request_key));
                    let queue_depth = panel.renderer.queue_depth();
                    let panel_pending = panel.renderer.is_pending();
                    if let Some(request_key) = panel.pending_request_key.take() {
                        panel.display_key = Some(request_key);
                    }
                    Ok((request_hash, queue_depth, panel_pending))
                } else {
                    Ok((
                        None,
                        panel.renderer.queue_depth(),
                        panel.renderer.is_pending(),
                    ))
                }
            }
            Err(error) => {
                panel.pending_request_key = None;
                panel.last_error = Some(error.to_string());
                Err(())
            }
        };
        let _ = state;
        if stale_count > 0
            && let Some(context) = context
        {
            for _ in 0..stale_count {
                self.movie_perf.frame_dropped(
                    None,
                    context,
                    MovieFrameOutcome::StaleRenderDiscarded,
                    "panel renderer discarded stale completion",
                );
            }
        }
        if let Ok((Some(request_hash), queue_depth, panel_pending)) = pump_result {
            self.movie_perf
                .plane_render_completed(request_hash, queue_depth, panel_pending);
            self.movie_perf.startup_plane_render_completed(
                request_hash,
                queue_depth,
                panel_pending,
            );
        }
    }

    fn pump_image_spectrum_panel(&mut self) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        let Some(panel) = state.spectrum_panel.as_mut() else {
            return;
        };
        let mut startup_render_completed = None::<(u64, usize, bool)>;
        match panel.renderer.pump() {
            Ok(changed) => {
                if changed {
                    panel.image_size = panel.renderer.image_size();
                    if let Some(request_key) = panel.pending_request_key.take() {
                        startup_render_completed = Some((
                            hashed_render_request_key(&request_key),
                            panel.renderer.queue_depth(),
                            panel.renderer.is_pending(),
                        ));
                        panel.display_key = Some(request_key);
                    }
                }
            }
            Err(error) => {
                panel.pending_request_key = None;
                panel.last_error = Some(error.to_string());
            }
        }
        if let Some((request_hash, queue_depth, panel_pending)) = startup_render_completed {
            self.movie_perf.startup_spectrum_render_completed(
                request_hash,
                queue_depth,
                panel_pending,
            );
        }
    }

    fn current_image_plane_render_request(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
    ) -> Option<CurrentImagePlaneRenderRequest> {
        let theme_mode = self.theme_mode();
        let split_ratio = self.image_workspace_split_ratio();
        let state = self.image_browser_session_state()?;
        if !state.raster_plane_active() {
            return None;
        }
        self.image_plane_render_request_for_snapshot(
            layout,
            font_size,
            &state.snapshot,
            ImagePlaneRenderRequestOptions {
                show_live_reticle: state.show_live_reticle,
                colormap: state.plane_colormap,
                invert: state.plane_invert,
                overlay_markers: &image_plane_overlay_markers(state),
                split_ratio,
                theme_mode,
                render_scale: 1.0,
                max_pixel_size: None,
            },
        )
    }

    fn current_image_direct_plane_render_request(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
    ) -> Option<CurrentImagePlaneRenderRequest> {
        let theme_mode = self.theme_mode();
        let split_ratio = self.image_workspace_split_ratio();
        let state = self.image_browser_session_state()?;
        if !state.raster_plane_active() {
            return None;
        }
        self.image_plane_render_request_for_snapshot(
            layout,
            font_size,
            &state.snapshot,
            ImagePlaneRenderRequestOptions {
                show_live_reticle: state.show_live_reticle,
                colormap: state.plane_colormap,
                invert: state.plane_invert,
                overlay_markers: &image_plane_overlay_markers(state),
                split_ratio,
                theme_mode,
                render_scale: self.current_image_movie_plane_render_scale(),
                max_pixel_size: None,
            },
        )
    }

    fn image_plane_render_request_for_snapshot(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
        snapshot: &ImageBrowserSnapshot,
        options: ImagePlaneRenderRequestOptions<'_>,
    ) -> Option<CurrentImagePlaneRenderRequest> {
        let area = crate::ui::image_plane_canvas_area_for_browser(
            layout,
            snapshot.profile.is_some(),
            options.split_ratio,
        );
        if area.is_empty() {
            return None;
        }
        let raster = snapshot.plane.clone()?;
        let show_live_reticle = options.show_live_reticle && snapshot.region.is_none();
        let cursor = show_live_reticle
            .then(|| image_plane_sample_cursor(snapshot))
            .flatten();
        let sampled_shape = image_plane_sampled_shape(snapshot);
        let region_overlay_shapes = snapshot
            .region
            .as_ref()
            .map(|region| region.overlay_shapes.clone())
            .unwrap_or_default();
        let render_signature = image_plane_render_signature(
            snapshot,
            show_live_reticle,
            options.colormap,
            options.invert,
            options.overlay_markers,
            &region_overlay_shapes,
        );
        let request_key = ImagePlaneRequestKey {
            area,
            theme_mode: options.theme_mode,
            render_signature,
        };
        let scaled_pixel_width = scaled_movie_render_dimension(
            u32::from(area.width.max(1)) * u32::from(font_size.0.max(1)),
            options.render_scale,
        );
        let scaled_pixel_height = scaled_movie_render_dimension(
            u32::from(area.height.max(1)) * u32::from(font_size.1.max(1)),
            options.render_scale,
        );
        let max_pixel_size = combine_render_pixel_caps(
            options.max_pixel_size,
            adaptive_image_plane_render_pixel_cap(area, font_size, snapshot),
        );
        let (pixel_width, pixel_height) =
            clamp_render_dimensions(scaled_pixel_width, scaled_pixel_height, max_pixel_size);
        let cache_key = hashed_render_input_cache_key(&request_key, pixel_width, pixel_height);
        Some(CurrentImagePlaneRenderRequest {
            request_key,
            pixel_width: pixel_width.max(1),
            pixel_height: pixel_height.max(1),
            input: ImagePlaneRenderInput {
                cache_key,
                raster,
                plane_label: image_plane_frame_label(snapshot),
                cursor_sample: cursor,
                sampled_shape,
                display_axes: snapshot.display_axes.clone(),
                probe: snapshot.probe.clone(),
                overlay_markers: options.overlay_markers.to_vec(),
                region_overlay_shapes,
                display_aspect_ratio: image_plane_display_aspect_ratio(snapshot),
                colormap: options.colormap,
                invert: options.invert,
                theme_mode: browser_render_theme(options.theme_mode),
            },
        })
    }

    fn current_image_spectrum_render_request(
        &self,
        layout: &UiLayout,
        font_size: (u16, u16),
        snapshot: &ImageBrowserSnapshot,
        options: ImageSpectrumRenderRequestOptions<'_>,
    ) -> Option<CurrentImageSpectrumRenderRequest> {
        let area = crate::ui::image_spectrum_canvas_area(layout, true, options.split_ratio)?;
        if area.is_empty() {
            return None;
        }
        let profile = snapshot.profile.clone()?;
        let render_signature =
            image_spectrum_render_signature(&profile, options.overlay_profiles, options.theme_mode);
        let request_key = ImageSpectrumRequestKey {
            area,
            theme_mode: options.theme_mode,
            render_signature,
        };
        let scaled_pixel_width = scaled_movie_render_dimension(
            u32::from(area.width.max(1)) * u32::from(font_size.0.max(1)),
            options.render_scale,
        );
        let scaled_pixel_height = scaled_movie_render_dimension(
            u32::from(area.height.max(1)) * u32::from(font_size.1.max(1)),
            options.render_scale,
        );
        let max_pixel_size = combine_render_pixel_caps(
            options.max_pixel_size,
            adaptive_image_spectrum_render_pixel_cap(area, font_size, &profile),
        );
        let (pixel_width, pixel_height) =
            clamp_render_dimensions(scaled_pixel_width, scaled_pixel_height, max_pixel_size);
        let cache_key = hashed_render_input_cache_key(&request_key, pixel_width, pixel_height);
        Some(CurrentImageSpectrumRenderRequest {
            request_key,
            pixel_width: pixel_width.max(1),
            pixel_height: pixel_height.max(1),
            input: ImageSpectrumRenderInput {
                cache_key,
                profile,
                overlay_profiles: options.overlay_profiles.to_vec(),
                theme_mode: browser_render_theme(options.theme_mode),
            },
        })
    }

    fn current_image_movie_plane_render_scale(&self) -> f32 {
        self.image_browser_session_state()
            .map(image_movie_plane_render_scale_for_state)
            .unwrap_or(1.0)
    }

    fn ensure_image_plane_requested(&mut self, layout: &UiLayout) {
        if self.defer_image_plane_render_during_divider_drag() {
            return;
        }
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        if !state.raster_plane_active()
            || state.movie.terminal_looping
            || state.movie.direct_overlay
        {
            return;
        }
        let picker = terminal_picker();
        let font_size = state
            .panel
            .as_ref()
            .map(|panel| panel.font_size)
            .unwrap_or_else(|| picker.font_size());
        let Some(request) = self.current_image_plane_render_request(layout, font_size) else {
            return;
        };
        let mut perf_render_requested =
            None::<(u64, u64, MoviePerfContext, usize, bool, MovieFrameOutcome)>;
        let mut perf_render_completed = None::<(u64, usize, bool)>;
        let mut perf_drop = None::<(Option<u64>, MoviePerfContext, MovieFrameOutcome, String)>;
        let mut startup_render_requested = None::<(u64, MoviePerfContext, usize, bool)>;

        {
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            let request_key_hash = hashed_render_request_key(&request.request_key);
            let context = image_movie_perf_context_from_state(
                state,
                Some(request.request_key.area),
                Some((request.pixel_width, request.pixel_height)),
                Some(request_key_hash),
            );
            let frame_seq = state.movie_frame_seq;
            let backend_outcome = map_backend_plane_outcome(state.snapshot.backend_timing.as_ref());
            let panel = state.panel.get_or_insert_with(new_image_plane_panel_state);
            let render_cache_hit = panel
                .render_cache
                .lock()
                .map(|cache| cache.contains_key(&request.input.cache_key))
                .unwrap_or(false);
            let direct_overlay_cache_hit =
                state.movie.playing && state.movie.direct_overlay && render_cache_hit;
            let queue_depth = panel.renderer.queue_depth();
            let panel_pending = panel.renderer.is_pending();
            if panel.display_key == Some(request.request_key.clone()) || direct_overlay_cache_hit {
                if let Some(frame_seq) = frame_seq {
                    perf_render_requested = Some((
                        frame_seq,
                        request_key_hash,
                        context,
                        queue_depth,
                        panel_pending,
                        MovieFrameOutcome::CacheHitRenderedImage,
                    ));
                    perf_render_completed = Some((request_key_hash, queue_depth, panel_pending));
                }
            } else if panel.pending_request_key != Some(request.request_key.clone()) {
                if let Some(frame_seq) = frame_seq {
                    perf_render_requested = Some((
                        frame_seq,
                        request_key_hash,
                        context,
                        queue_depth,
                        panel_pending,
                        if render_cache_hit {
                            MovieFrameOutcome::CacheHitRenderedImage
                        } else {
                            backend_outcome
                        },
                    ));
                }
                let request_result = if state.movie.playing && state.movie.direct_overlay {
                    panel.renderer.request_render_only(
                        request.request_key.area,
                        request.pixel_width,
                        request.pixel_height,
                        request.input,
                    )
                } else {
                    panel.renderer.request(
                        request.request_key.area,
                        request.pixel_width,
                        request.pixel_height,
                        request.input,
                    )
                };
                if let Err(error) = request_result {
                    panel.last_error = Some(error.to_string());
                    if let Some(frame_seq) = frame_seq {
                        perf_drop = Some((
                            Some(frame_seq),
                            context,
                            backend_outcome,
                            format!("plane render request failed: {error}"),
                        ));
                    }
                } else {
                    startup_render_requested =
                        Some((request_key_hash, context, queue_depth, panel_pending));
                    panel.pending_request_key = Some(request.request_key);
                }
            } else if let Some(frame_seq) = frame_seq {
                perf_drop = Some((
                    Some(frame_seq),
                    context,
                    MovieFrameOutcome::SkippedDueToPending,
                    "plane render already pending for current request".to_string(),
                ));
                state.movie_frame_seq = None;
            }
        }

        if let Some((frame_seq, request_key_hash, context, queue_depth, panel_pending, outcome)) =
            perf_render_requested
        {
            self.movie_perf.plane_render_requested(
                frame_seq,
                request_key_hash,
                context,
                queue_depth,
                panel_pending,
                outcome,
            );
        }
        if let Some((request_key_hash, queue_depth, panel_pending)) = perf_render_completed {
            self.movie_perf
                .plane_render_completed(request_key_hash, queue_depth, panel_pending);
        }
        if let Some((request_key_hash, context, queue_depth, panel_pending)) =
            startup_render_requested
        {
            self.movie_perf.startup_plane_render_requested(
                request_key_hash,
                context,
                queue_depth,
                panel_pending,
            );
        }
        if let Some((frame_seq, context, outcome, note)) = perf_drop {
            self.movie_perf
                .frame_dropped(frame_seq, context, outcome, note);
        }
    }

    fn ensure_image_spectrum_requested(&mut self, layout: &UiLayout) {
        let theme_mode = self.theme_mode();
        if self.defer_image_spectrum_render_during_divider_drag() {
            return;
        }
        let split_ratio = self.image_workspace_split_ratio();
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        if !state.linked_profile_active() || state.movie.direct_overlay {
            return;
        }
        let overlay_profiles = image_spectrum_overlay_series(state);
        let font_size = state
            .spectrum_panel
            .as_ref()
            .map(|panel| panel.font_size)
            .unwrap_or_else(|| terminal_picker().font_size());
        let snapshot = state.snapshot.clone();
        let Some(request) = self.current_image_spectrum_render_request(
            layout,
            font_size,
            &snapshot,
            ImageSpectrumRenderRequestOptions {
                overlay_profiles: &overlay_profiles,
                split_ratio,
                theme_mode,
                render_scale: 1.0,
                max_pixel_size: None,
            },
        ) else {
            return;
        };
        let startup_render_requested = {
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            let request_key_hash = hashed_render_request_key(&request.request_key);
            let context = image_movie_perf_context_from_state(
                state,
                Some(request.request_key.area),
                Some((request.pixel_width, request.pixel_height)),
                Some(request_key_hash),
            );
            let panel = state
                .spectrum_panel
                .get_or_insert_with(new_image_spectrum_panel_state);
            let queue_depth = panel.renderer.queue_depth();
            let panel_pending = panel.renderer.is_pending();
            if panel.display_key == Some(request.request_key.clone())
                || panel.pending_request_key == Some(request.request_key.clone())
            {
                return;
            }
            // Do not cache cloned PanelProtocol values here: Kitty-backed protocols
            // transmit only once, so replaying a clone can produce blank frames.
            if let Err(error) = panel.renderer.request(
                request.request_key.area,
                request.pixel_width,
                request.pixel_height,
                request.input,
            ) {
                panel.last_error = Some(error.to_string());
                return;
            }
            panel.pending_request_key = Some(request.request_key);
            Some((request_key_hash, context, queue_depth, panel_pending))
        };
        if let Some((request_key_hash, context, queue_depth, panel_pending)) =
            startup_render_requested
        {
            self.movie_perf.startup_spectrum_render_requested(
                request_key_hash,
                context,
                queue_depth,
                panel_pending,
            );
        }
    }

    fn advance_image_movie(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        if !state.movie.playing || state.movie.terminal_looping || !state.movie_available() {
            return;
        }
        let now = Instant::now();
        if state
            .movie
            .last_advanced_at
            .is_some_and(|last| now.duration_since(last) < state.movie.frame_interval)
        {
            return;
        }
        let Some((axis, index, length)) = state
            .selected_non_display_axis_state()
            .map(|axis_state| (axis_state.axis, axis_state.index, axis_state.length))
        else {
            self.stop_image_movie(false, "movie axis unavailable");
            return;
        };
        if index + 1 >= length && !state.movie.looping {
            self.stop_image_movie(false, "movie reached the final frame");
            return;
        }
        let delta = if index + 1 < length {
            1
        } else {
            -((length.saturating_sub(1)) as i32)
        };
        let target_index = if delta > 0 {
            index.saturating_add(1)
        } else {
            0
        };
        let frame_context = MoviePerfContext {
            axis: Some(axis),
            axis_index: Some(target_index.min(length.saturating_sub(1))),
            axis_length: Some(length),
            render_request_key_hash: None,
            canvas_cell_size: Some((state.viewport.width, state.viewport.height)),
            canvas_pixel_size: Some((
                u32::from(state.viewport.plane_pixel_width),
                u32::from(state.viewport.plane_pixel_height),
            )),
            raster_mode: state.plane_mode == ImagePlaneMode::Raster,
            direct_overlay: state.movie.direct_overlay,
            terminal_looping: state.movie.terminal_looping,
            requested_fps_milli: Some((state.movie.fps * 1000.0).round() as u64),
        };
        let frame_seq = self.movie_perf.begin_frame(frame_context);
        if let Some(state) = self.image_browser_session_state_mut() {
            state.movie.last_advanced_at = Some(now);
            state.movie_frame_seq = frame_seq;
        }
        self.send_browser_command(BrowserRequest::StepImageNonDisplayAxis { axis, delta });
    }

    fn ensure_plot_requested(&mut self, layout: &UiLayout) {
        if !self.result_tab_uses_plot_workspace() {
            return;
        }
        let Some(area) = crate::ui::plot_canvas_area(layout) else {
            return;
        };
        if area.is_empty() {
            return;
        }
        let spec_key = match self.current_plot_spec_key() {
            Ok(key) => key,
            Err(error) => {
                self.plot_workspace.preview_invalidated = false;
                self.result.status_line = "Plot payload unavailable.".to_string();
                self.result.status_kind = StatusKind::Warning;
                self.result.stderr = format!("{error}\n");
                return;
            }
        };
        let snapshot_generation = self
            .plot_workspace
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.generation)
            .unwrap_or_default();
        let theme_mode = self.theme_mode();
        let request_key = PlotRequestKey {
            area,
            theme_mode,
            snapshot_generation,
            plot_signature: self.selected_plot_label(),
            spec_key,
        };
        if self
            .plot_workspace
            .panel
            .as_ref()
            .is_some_and(|panel| panel.request_key == Some(request_key.clone()))
        {
            return;
        }

        let payload = match self.current_plot_payload() {
            Ok(payload) => payload,
            Err(error) => {
                self.plot_workspace.preview_invalidated = false;
                self.result.status_line = "Plot payload unavailable.".to_string();
                self.result.status_kind = StatusKind::Warning;
                self.result.stderr = format!("{error}\n");
                return;
            }
        };

        let panel = self.plot_workspace.panel.get_or_insert_with(|| {
            let picker = terminal_picker();
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
        if panel.request_key == Some(request_key.clone()) {
            return;
        }

        let pixel_width = u32::from(area.width.max(1)) * u32::from(panel.font_size.0.max(1));
        let pixel_height = u32::from(area.height.max(1)) * u32::from(panel.font_size.1.max(1));
        if let Err(error) = panel.renderer.request(
            area,
            pixel_width.max(1),
            pixel_height.max(1),
            match payload {
                CurrentPlotPayload::MsPlot(payload) => {
                    PlotRenderInput::MsExplore(MsExplorePlotRenderInput {
                        payload,
                        theme_mode,
                        terminal_cell_px: panel.font_size,
                    })
                }
                CurrentPlotPayload::Imaging(payload) => {
                    PlotRenderInput::Imaging(ImagingPlotRenderInput {
                        payload,
                        theme_mode,
                    })
                }
            },
        ) {
            panel.last_error = Some(error.to_string());
            self.plot_workspace.preview_invalidated = false;
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

    pub(crate) fn plot_display_protocol(&self) -> Option<&PanelProtocol> {
        self.plot_protocol()
            .or(self.plot_workspace.placeholder_protocol.as_ref())
    }

    pub(crate) fn plot_pending(&self) -> bool {
        self.plot_workspace.preview_invalidated
            || self
                .plot_workspace
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

    pub(crate) fn image_plane_protocol(&self) -> Option<&PanelProtocol> {
        self.image_browser_session_state()?
            .panel
            .as_ref()
            .and_then(|panel| {
                panel
                    .movie_protocol
                    .as_ref()
                    .or_else(|| panel.renderer.protocol())
            })
    }

    pub(crate) fn image_plane_pending(&self) -> bool {
        self.image_browser_session_state()
            .and_then(|state| state.panel.as_ref())
            .is_some_and(|panel| panel.renderer.is_pending())
    }

    pub(crate) fn image_plane_last_error(&self) -> Option<&str> {
        self.image_browser_session_state()?
            .panel
            .as_ref()
            .and_then(|panel| panel.last_error.as_deref())
    }

    pub(crate) fn current_direct_image_movie_bundle_info(
        &self,
        layout: &UiLayout,
    ) -> Option<ImageDirectMovieBundleInfo> {
        let state = self.image_browser_session_state()?;
        if !state.movie.playing || !state.raster_plane_active() || !state.movie_available() {
            return None;
        }
        let axis_state = state.selected_non_display_axis_state()?;
        let split_ratio = self.image_workspace_split_ratio();
        let theme_mode = self.theme_mode();
        let plane_font_size = state
            .panel
            .as_ref()
            .map(|panel| panel.font_size)
            .unwrap_or_else(|| terminal_picker().font_size());
        let plane_request = self.image_plane_render_request_for_snapshot(
            layout,
            plane_font_size,
            &state.snapshot,
            ImagePlaneRenderRequestOptions {
                show_live_reticle: state.show_live_reticle,
                colormap: state.plane_colormap,
                invert: state.plane_invert,
                overlay_markers: &image_plane_overlay_markers(state),
                split_ratio,
                theme_mode,
                render_scale: 1.0,
                max_pixel_size: None,
            },
        )?;
        let mut surface_requests = vec![ImageMovieSurfaceRequest {
            kind: ImageMovieSurfaceKind::Plane,
            request_hash: hashed_render_request_key(&plane_request.request_key),
            cell_size: (
                plane_request.request_key.area.width,
                plane_request.request_key.area.height,
            ),
            pixel_size: (plane_request.pixel_width, plane_request.pixel_height),
            payload: DirectImageMovieSurfacePayload::Plane(plane_request.input.clone()),
        }];
        let mut surfaces = vec![ImageDirectMovieSurfaceInfo {
            kind: ImageMovieSurfaceKind::Plane,
            canvas: plane_request.request_key.area,
            request_hash: hashed_render_request_key(&plane_request.request_key),
            pixel_size: (plane_request.pixel_width, plane_request.pixel_height),
        }];
        if state.linked_profile_active() {
            let spectrum_font_size = state
                .spectrum_panel
                .as_ref()
                .map(|panel| panel.font_size)
                .unwrap_or_else(|| terminal_picker().font_size());
            let overlay_profiles = image_spectrum_overlay_series(state);
            if let Some(spectrum_request) = self.current_image_spectrum_render_request(
                layout,
                spectrum_font_size,
                &state.snapshot,
                ImageSpectrumRenderRequestOptions {
                    overlay_profiles: &overlay_profiles,
                    split_ratio,
                    theme_mode,
                    render_scale: 1.0,
                    max_pixel_size: None,
                },
            ) {
                surface_requests.push(ImageMovieSurfaceRequest {
                    kind: ImageMovieSurfaceKind::Spectrum,
                    request_hash: hashed_render_request_key(&spectrum_request.request_key),
                    cell_size: (
                        spectrum_request.request_key.area.width,
                        spectrum_request.request_key.area.height,
                    ),
                    pixel_size: (spectrum_request.pixel_width, spectrum_request.pixel_height),
                    payload: DirectImageMovieSurfacePayload::Spectrum(
                        spectrum_request.input.clone(),
                    ),
                });
                surfaces.push(ImageDirectMovieSurfaceInfo {
                    kind: ImageMovieSurfaceKind::Spectrum,
                    canvas: spectrum_request.request_key.area,
                    request_hash: hashed_render_request_key(&spectrum_request.request_key),
                    pixel_size: (spectrum_request.pixel_width, spectrum_request.pixel_height),
                });
            }
        }

        let movie_key = image_movie_animation_signature(
            &state.snapshot,
            state.show_live_reticle,
            state.plane_colormap,
            state.plane_invert,
            axis_state.axis,
            theme_mode,
        );
        let prepared = state
            .direct_movie_engine
            .prepare_bundle(&ImageMovieBundleRequest {
                occurrence: ImageMovieOccurrence {
                    generation: movie_key,
                    movie_key,
                    axis: axis_state.axis,
                    axis_index: axis_state.index,
                    axis_length: axis_state.length,
                },
                requested_fps: state.movie.fps,
                surfaces: surface_requests,
            });
        Some(ImageDirectMovieBundleInfo {
            movie_key,
            axis: axis_state.axis,
            axis_index: axis_state.index,
            axis_length: axis_state.length,
            fps: state.movie.fps,
            surfaces,
            prepared,
        })
    }

    pub(crate) fn current_direct_image_movie_bundle(
        &mut self,
        layout: &UiLayout,
    ) -> Option<ImageDirectMovieBundle> {
        let info = self.current_direct_image_movie_bundle_info(layout)?;
        let state = self.image_browser_session_state_mut()?;
        let (rendered, cache_hit) = state
            .direct_movie_engine
            .render_or_get_cached(
                &info.prepared,
                &mut |_, pixel_size: (u32, u32), payload: &DirectImageMovieSurfacePayload| {
                    match payload {
                        DirectImageMovieSurfacePayload::Plane(input) => {
                            render_image_plane_image(pixel_size.0, pixel_size.1, input)
                                .map(|image| image.to_rgb8())
                        }
                        DirectImageMovieSurfacePayload::Spectrum(input) => {
                            render_image_spectrum_image(pixel_size.0, pixel_size.1, input)
                                .map(|image| image.to_rgb8())
                        }
                    }
                },
            )
            .ok()?;
        Some(ImageDirectMovieBundle {
            rendered,
            cache_hit,
        })
    }

    pub(crate) fn current_direct_image_movie_frame_info(
        &self,
        layout: &UiLayout,
    ) -> Option<ImageDirectMovieFrameInfo> {
        let state = self.image_browser_session_state()?;
        if !state.movie.playing || !state.raster_plane_active() || !state.movie_available() {
            crate::movie_debug_log(format!(
                "direct frame unavailable: playing={} raster_active={} movie_available={} active_view={:?} plane_mode={:?}",
                state.movie.playing,
                state.raster_plane_active(),
                state.movie_available(),
                state.snapshot.active_view,
                state.plane_mode
            ));
            return None;
        }
        let Some(panel) = state.panel.as_ref() else {
            crate::movie_debug_log("direct frame unavailable: no panel state");
            return None;
        };
        let Some(request) = self.current_image_direct_plane_render_request(layout, panel.font_size)
        else {
            crate::movie_debug_log(format!(
                "direct frame unavailable: no render request area={}x{} font={}x{}",
                layout.result_content.width,
                layout.result_content.height,
                panel.font_size.0,
                panel.font_size.1
            ));
            return None;
        };
        let Some(axis_state) = state.selected_non_display_axis_state() else {
            crate::movie_debug_log("direct frame unavailable: no selected non-display axis");
            return None;
        };
        Some(ImageDirectMovieFrameInfo {
            movie_key: image_movie_animation_signature(
                &state.snapshot,
                state.show_live_reticle,
                state.plane_colormap,
                state.plane_invert,
                axis_state.axis,
                self.theme_mode(),
            ),
            canvas: request.request_key.area,
            axis: axis_state.axis,
            axis_index: axis_state.index,
            axis_length: axis_state.length,
            fps: state.movie.fps,
            render_request_key_hash: hashed_render_request_key(&request.request_key),
        })
    }

    pub(crate) fn current_direct_image_movie_frame(
        &self,
        layout: &UiLayout,
    ) -> Option<ImageDirectMovieFrame> {
        let frame_info = self.current_direct_image_movie_frame_info(layout)?;
        let state = self.image_browser_session_state()?;
        let panel = state.panel.as_ref()?;
        let request = self.current_image_direct_plane_render_request(layout, panel.font_size)?;
        let axis_state = state.selected_non_display_axis_state()?;
        let display_key_matches = panel.display_key.as_ref() == Some(&request.request_key);
        let rendered_image = panel
            .render_cache
            .lock()
            .ok()
            .and_then(|mut cache| cache.get(&request.input.cache_key))
            .map(DynamicImage::ImageRgb8)
            .map(|image| image.to_rgba8())
            .or_else(|| {
                if panel.display_key.as_ref() == Some(&request.request_key) {
                    panel.renderer.rendered_image().and_then(|image| {
                        (image.width() == request.pixel_width
                            && image.height() == request.pixel_height)
                            .then(|| image.clone())
                    })
                } else {
                    None
                }
            })
            .or_else(|| {
                match render_image_plane_image(
                    request.pixel_width,
                    request.pixel_height,
                    &request.input,
                ) {
                    Ok(image) => Some(image.to_rgba8()),
                    Err(error) => {
                        crate::movie_debug_log(format!(
                            "direct frame render error axis={} index={} request={}x{}: {}",
                            axis_state.axis,
                            axis_state.index,
                            request.pixel_width,
                            request.pixel_height,
                            error
                        ));
                        None
                    }
                }
            });
        let Some(rendered_image) = rendered_image else {
            crate::movie_debug_log(format!(
                "direct frame unavailable: render failed axis={} index={} request={}x{} display_key_match={}",
                axis_state.axis,
                axis_state.index,
                request.pixel_width,
                request.pixel_height,
                display_key_matches
            ));
            return None;
        };
        if crate::movie_debug_enabled() {
            let image_hash = {
                let mut hasher = DefaultHasher::new();
                rendered_image.as_raw().hash(&mut hasher);
                hasher.finish()
            };
            crate::movie_debug_log(format!(
                "direct frame ready axis={} index={} len={} request={}x{} image_hash={} display_key_match={} panel_pending={}",
                axis_state.axis,
                axis_state.index,
                axis_state.length,
                request.pixel_width,
                request.pixel_height,
                image_hash,
                display_key_matches,
                panel.renderer.is_pending()
            ));
        }
        Some(ImageDirectMovieFrame {
            movie_key: frame_info.movie_key,
            canvas: frame_info.canvas,
            axis: frame_info.axis,
            axis_index: frame_info.axis_index,
            axis_length: frame_info.axis_length,
            fps: frame_info.fps,
            render_request_key_hash: frame_info.render_request_key_hash,
            rendered_image,
        })
    }

    pub(crate) fn set_image_movie_terminal_looping(&mut self, looping: bool) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        state.movie.terminal_looping = looping;
        crate::movie_debug_log(format!(
            "set terminal looping={} playing={} axis_state={}",
            looping,
            state.movie.playing,
            state
                .selected_non_display_axis_state()
                .map(|axis| format!(
                    "axis={} index={} len={}",
                    axis.axis, axis.index, axis.length
                ))
                .unwrap_or_else(|| "none".to_string())
        ));
        if !looping {
            state.movie.last_advanced_at = Some(Instant::now());
        }
    }

    pub(crate) fn set_image_movie_direct_overlay(&mut self, active: bool) {
        let Some(context) = ({
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            if state.movie.direct_overlay == active {
                return;
            }
            state.movie.direct_overlay = active;
            if !active {
                state.movie.last_advanced_at = Some(Instant::now());
            }
            Some(image_movie_perf_context_from_state(state, None, None, None))
        }) else {
            return;
        };
        self.movie_perf.direct_overlay_changed(context, active);
    }

    pub(crate) fn sync_image_non_display_axis_index(&mut self, axis: usize, index: usize) {
        let current_index = self
            .image_browser_session_state()
            .and_then(|state| {
                state
                    .snapshot
                    .non_display_axes
                    .iter()
                    .find(|axis_state| axis_state.axis == axis)
                    .map(|axis_state| axis_state.index)
            })
            .unwrap_or(index);
        let delta = index as i32 - current_index as i32;
        if delta != 0 {
            self.send_browser_command(BrowserRequest::StepImageNonDisplayAxis { axis, delta });
        }
    }

    pub(crate) fn image_spectrum_protocol(&self) -> Option<&PanelProtocol> {
        self.image_browser_session_state()?
            .spectrum_panel
            .as_ref()
            .and_then(|panel| {
                panel
                    .movie_protocol
                    .as_ref()
                    .or_else(|| panel.renderer.protocol())
            })
    }

    pub(crate) fn image_spectrum_pending(&self) -> bool {
        self.image_browser_session_state()
            .and_then(|state| state.spectrum_panel.as_ref())
            .is_some_and(|panel| panel.renderer.is_pending())
    }

    pub(crate) fn image_spectrum_last_error(&self) -> Option<&str> {
        self.image_browser_session_state()?
            .spectrum_panel
            .as_ref()
            .and_then(|panel| panel.last_error.as_deref())
    }

    pub(crate) fn image_plane_has_linked_profile(&self) -> bool {
        self.image_browser_session_state()
            .is_some_and(ImageBrowserSessionState::spectrum_workspace_visible)
    }

    pub(crate) fn image_profile_title_line(&self) -> Option<String> {
        let profile = self
            .image_browser_session_state()?
            .snapshot
            .profile
            .as_ref()?;
        let kind = if profile.coord_type.eq_ignore_ascii_case("Spectral") {
            "Spectrum"
        } else {
            "Profile"
        };
        let axis_label = format_profile_axis_label(profile);
        let selected = profile
            .samples
            .get(profile.selected_sample_index)
            .map(|sample| format_profile_selected_label(sample, &profile.value_unit))
            .unwrap_or_else(|| "<none>".to_string());
        Some(format!("{kind}: {axis_label}  Selected: {selected}"))
    }

    pub(crate) fn plot_focus(&self) -> PlotPaneFocus {
        self.plot_workspace.focus
    }

    pub(crate) fn plot_catalog_rows(&self) -> Vec<PlotCatalogRowView> {
        let selected = self.current_plot_catalog_target();
        if self.app.id == "calibrate" {
            let selected_preset = match selected {
                Some(PlotCatalogTarget::Calibration(preset)) => Some(preset),
                _ => None,
            };
            return workflow_calibration_catalog_entries(selected_preset)
                .into_iter()
                .map(|entry| PlotCatalogRowView {
                    target: PlotCatalogTarget::Calibration(entry.target),
                    label: entry.label,
                    selected: entry.selected,
                })
                .collect();
        }
        if self.app.id == "imager" {
            let Some(report) = self.current_imaging_report() else {
                return Vec::new();
            };
            let selected_kind = match selected {
                Some(PlotCatalogTarget::Imaging(kind)) => kind,
                _ => imaging_preferred_diagnostic(report),
            };
            return imaging_catalog_entries(report, selected_kind)
                .into_iter()
                .map(|entry| PlotCatalogRowView {
                    target: PlotCatalogTarget::Imaging(entry.target),
                    label: entry.label,
                    selected: entry.selected,
                })
                .collect();
        }
        let mut rows = Vec::new();
        match selected {
            Some(PlotCatalogTarget::PageSpec) => rows.push(PlotCatalogRowView {
                target: PlotCatalogTarget::PageSpec,
                label: self.selected_plot_label(),
                selected: true,
            }),
            Some(PlotCatalogTarget::CustomPlot) => rows.push(PlotCatalogRowView {
                target: PlotCatalogTarget::CustomPlot,
                label: self.selected_plot_label(),
                selected: true,
            }),
            _ => {}
        }
        rows.extend(
            MsPlotPreset::ALL
                .into_iter()
                .map(|preset| PlotCatalogRowView {
                    target: PlotCatalogTarget::Preset(preset),
                    label: preset.display_name().to_string(),
                    selected: selected == Some(PlotCatalogTarget::Preset(preset)),
                }),
        );
        rows
    }

    pub(crate) fn plot_control_rows(&self) -> Vec<PlotControlRowView> {
        let mut rows = Vec::new();
        let catalog = if self.app.id == "calibrate" {
            vec![
                (PlotControlTarget::Refresh, "Refresh Preview"),
                (PlotControlTarget::ExportPng, "Export PNG"),
                (PlotControlTarget::ExportPdf, "Export PDF"),
            ]
        } else if self.app.id == "imager" {
            vec![
                (PlotControlTarget::Refresh, "Refresh Preview"),
                (PlotControlTarget::ExportPng, "Export PNG"),
            ]
        } else {
            vec![
                (PlotControlTarget::Refresh, "Refresh Preview"),
                (PlotControlTarget::CopyCli, "Copy CLI"),
                (PlotControlTarget::ExportPng, "Export PNG"),
                (PlotControlTarget::ExportPdf, "Export PDF"),
            ]
        };
        for (target, label) in catalog {
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
        None
    }

    fn scroll_active_plot_workspace(&mut self, delta: i16) {
        match self.plot_workspace.focus {
            PlotPaneFocus::Catalog => {
                let rows = self.plot_catalog_rows();
                if rows.is_empty() {
                    return;
                }
                let current = rows.iter().position(|row| row.selected).unwrap_or(0) as i16;
                let next = (current + delta).clamp(0, rows.len() as i16 - 1) as usize;
                self.apply_plot_catalog_target(rows[next].target);
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
                    PlotControlTarget::Refresh => self.clear_plot_render_cache(),
                    PlotControlTarget::CopyCli => self.copy_current_plot_cli(),
                    PlotControlTarget::ExportPng => self.export_current_plot(MsExportFormat::Png),
                    PlotControlTarget::ExportPdf => self.export_current_plot(MsExportFormat::Pdf),
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
                match self.sync_parameter_from_field(field_index) {
                    Ok(()) => self.apply_live_image_view_parameters_if_needed(field_index),
                    Err(error) => {
                        self.result.status_line = error;
                        self.result.status_kind = StatusKind::Warning;
                    }
                }
            }
            EditTarget::RenameImageRegionDefinition => {
                let new_name = edit_state.buffer.trim();
                if new_name.is_empty() {
                    self.result.status_line = "Saved region name cannot be empty.".to_string();
                    self.result.status_kind = StatusKind::Error;
                } else if let Some(name) = self
                    .image_browser_session_state()
                    .and_then(|state| state.selected_saved_region_name())
                    .map(str::to_string)
                {
                    self.send_browser_command(BrowserRequest::RenameImageRegionDefinition {
                        name,
                        new_name: new_name.to_string(),
                    });
                    self.result.status_line = format!("Renaming saved region to {new_name}...");
                    self.result.status_kind = StatusKind::Info;
                } else {
                    self.result.status_line = "No saved region selected.".to_string();
                    self.result.status_kind = StatusKind::Warning;
                }
            }
        }
    }

    fn copy_current_plot_cli(&mut self) {
        if matches!(self.app.id.as_str(), "calibrate" | "imager") {
            self.result.status_line =
                "Copy CLI is not available for this diagnostic workspace.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        match self.build_current_msexplore_plot_cli(MsExportFormat::Png) {
            Ok(cli) => self.copy_text_to_clipboard(cli, "plot CLI"),
            Err(error) => {
                self.result.status_line = "Copy plot CLI failed.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
            }
        }
    }

    fn export_current_plot(&mut self, format: MsExportFormat) {
        let payload = match self.current_plot_payload() {
            Ok(payload) => payload,
            Err(error) => {
                self.result.status_line = "Plot export failed.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
                return;
            }
        };
        let output_path = self.current_plot_output_path(format);
        let width = self.current_plot_export_width();
        let height = self.current_plot_export_height();
        let export_result = match payload {
            CurrentPlotPayload::MsPlot(payload) => export_msexplore_plot(
                &payload,
                plot_theme(self.theme_mode()),
                &output_path,
                format,
                width,
                height,
            ),
            CurrentPlotPayload::Imaging(payload) => match format {
                MsExportFormat::Png => render_plot_image(
                    width,
                    height,
                    &PlotRenderInput::Imaging(ImagingPlotRenderInput {
                        payload,
                        theme_mode: self.theme_mode(),
                    }),
                )
                .and_then(|image| image.save(&output_path).map_err(|error| error.to_string())),
                MsExportFormat::Pdf => {
                    Err("PDF export is not available for imaging diagnostics yet.".to_string())
                }
                MsExportFormat::Txt => {
                    Err("Text export is not available for imaging diagnostics.".to_string())
                }
            },
        };
        match export_result {
            Ok(()) => {
                self.result.status_line = format!("Saved {}.", output_path.display());
                self.result.status_kind = StatusKind::Ok;
            }
            Err(error) => {
                self.result.status_line = "Plot export failed.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
            }
        }
    }

    fn current_plot_output_path(&self, format: MsExportFormat) -> PathBuf {
        if self.app.id == "calibrate" {
            let mut path = self
                .field_text("output")
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    let slug = self
                        .selected_plot_label()
                        .to_ascii_lowercase()
                        .replace(':', "")
                        .replace(['/', ' '], "-");
                    PathBuf::from(format!("calibrate-{slug}.{}", format.extension()))
                });
            path.set_extension(format.extension());
            return path;
        }
        if self.app.id == "imager" {
            let slug = self
                .selected_plot_label()
                .to_ascii_lowercase()
                .replace(':', "")
                .replace(['/', ' '], "-");
            return PathBuf::from(format!("imager-{slug}.{}", format.extension()));
        }
        self.current_msexplore_output_path(format)
    }

    fn current_plot_export_width(&self) -> u32 {
        if matches!(self.app.id.as_str(), "calibrate" | "imager") {
            return 1600;
        }
        self.current_msexplore_export_width()
    }

    fn current_plot_export_height(&self) -> u32 {
        if matches!(self.app.id.as_str(), "calibrate" | "imager") {
            return 900;
        }
        self.current_msexplore_export_height()
    }

    fn current_msexplore_output_path(&self, format: MsExportFormat) -> PathBuf {
        let configured_path = self.field_text("plot_output").unwrap_or_default();
        let trimmed = configured_path.trim();
        let mut path = if trimmed.is_empty() {
            PathBuf::from(format!("msexplore-plot.{}", format.extension()))
        } else {
            PathBuf::from(trimmed)
        };
        path.set_extension(format.extension());
        path
    }

    fn current_msexplore_export_width(&self) -> u32 {
        self.field_text("plot_width")
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1600)
    }

    fn current_msexplore_export_height(&self) -> u32 {
        self.field_text("plot_height")
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(900)
    }

    fn build_current_msexplore_plot_cli(&self, format: MsExportFormat) -> Result<String, String> {
        let mut args = self.build_execution_plan()?.arguments;
        let output_path = self.current_msexplore_output_path(format);
        if !args.iter().any(|value| value == "--plot-output") {
            args.push(OsString::from("--plot-output"));
            args.push(output_path.into_os_string());
        }
        if !args.iter().any(|value| value == "--plot-format") {
            args.push(OsString::from("--plot-format"));
            args.push(OsString::from(format.extension()));
        }
        let rendered_args = args
            .iter()
            .map(|value| shell_quote(&value.to_string_lossy()))
            .collect::<Vec<_>>();
        Ok(format!("msexplore {}", rendered_args.join(" ")))
    }

    fn current_plot_catalog_target(&self) -> Option<PlotCatalogTarget> {
        if self.app.id == "calibrate" {
            return Some(
                self.plot_workspace
                    .selected_catalog_target
                    .unwrap_or_else(|| self.default_calibration_plot_target()),
            );
        }
        if self.app.id == "imager" {
            return self.plot_workspace.selected_catalog_target;
        }
        if self
            .field_text("page_spec")
            .is_some_and(|value| !value.trim().is_empty())
        {
            return Some(PlotCatalogTarget::PageSpec);
        }
        if let Some(preset) = self
            .field_text("preset")
            .filter(|value| !value.trim().is_empty())
            .and_then(|value| MsPlotPreset::parse(&value).ok())
        {
            return Some(PlotCatalogTarget::Preset(preset));
        }
        if self
            .field_text("x_axis")
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .field_text("y_axis")
                .is_some_and(|value| !value.trim().is_empty())
        {
            return Some(PlotCatalogTarget::CustomPlot);
        }
        None
    }

    fn apply_plot_catalog_target(&mut self, target: PlotCatalogTarget) {
        match target {
            PlotCatalogTarget::Calibration(preset) => {
                self.plot_workspace.selected_catalog_target =
                    Some(PlotCatalogTarget::Calibration(preset));
                self.clear_plot_render_cache();
            }
            PlotCatalogTarget::Imaging(kind) => {
                self.plot_workspace.selected_catalog_target =
                    Some(PlotCatalogTarget::Imaging(kind));
                self.clear_plot_render_cache();
            }
            PlotCatalogTarget::Preset(preset) => self.apply_msexplore_preset(preset),
            PlotCatalogTarget::CustomPlot | PlotCatalogTarget::PageSpec => {}
        }
    }

    fn apply_msexplore_preset(&mut self, preset: MsPlotPreset) {
        if !self.is_msexplore_app() {
            return;
        }
        if let Err(error) = self.apply_startup_text_value("preset", preset.as_str().to_string()) {
            self.result.status_line =
                format!("Could not select {}: {error}", preset.display_name());
            self.result.status_kind = StatusKind::Error;
            return;
        }
        for (id, value) in [
            ("page_spec", ""),
            ("x_axis", ""),
            ("y_axis", ""),
            ("y_axis2", ""),
        ] {
            if let Err(error) = self.apply_startup_text_value(id, value.to_string()) {
                self.result.status_line = format!(
                    "Selected {}, but could not reset {id}: {error}",
                    preset.display_name()
                );
                self.result.status_kind = StatusKind::Error;
                return;
            }
        }
        self.plot_workspace.focus = PlotPaneFocus::Catalog;
        self.plot_workspace.selected_control = 0;
        self.clear_plot_render_cache();
        self.result.status_line =
            format!("Selected {}. Rendering preview...", preset.display_name());
        self.result.status_kind = StatusKind::Info;
    }

    fn build_execution_plan(&self) -> Result<ExecutionPlan, String> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| "missing command schema".to_string())?;

        if let Some((name, error)) = self.parameter_edit_errors.iter().next() {
            return Err(format!("Invalid parameter {name}: {error}"));
        }
        let parameter_session = self
            .parameter_session
            .as_ref()
            .ok_or_else(|| "typed parameter session is unavailable".to_string())?;
        parameter_session
            .render_sparse()
            .map_err(|error| error.to_string())?;
        let invocation = crate::parameters_cli::project_task_invocation(parameter_session)?;
        let arguments = invocation
            .args
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();

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

        Ok(ExecutionPlan {
            command: self.app.resolve_command()?,
            arguments,
            stdin: invocation.stdin,
            working_directory: self.parameter_workspace.clone(),
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
            .map(|value| {
                if value.trim() == "none" {
                    String::new()
                } else {
                    value
                }
            })
    }

    fn non_empty_field_text(&self, id: &str) -> Option<String> {
        self.field_text(id).filter(|value| !value.trim().is_empty())
    }

    fn split_csv_field(&self, id: &str) -> Vec<String> {
        self.non_empty_field_text(id)
            .into_iter()
            .flat_map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn split_semicolon_field(&self, id: &str) -> Vec<String> {
        self.non_empty_field_text(id)
            .into_iter()
            .flat_map(|value| {
                value
                    .split(';')
                    .map(|item| item.trim().to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn workflow_chain_setting_values(&self, kind: WorkflowChainSettingKind) -> Vec<String> {
        let len = self.split_csv_field("gaintables").len();
        let mut values = match kind {
            WorkflowChainSettingKind::Gainfield => self.split_semicolon_field("gainfield"),
            WorkflowChainSettingKind::Interp => self.split_semicolon_field("interp"),
            WorkflowChainSettingKind::Spwmap => self.split_semicolon_field("spwmap"),
            WorkflowChainSettingKind::Calwt => self.split_csv_field("calwt"),
        };
        values.resize(len, String::new());
        values
    }

    fn workflow_chain_setting_raw_value(
        &self,
        entry: usize,
        kind: WorkflowChainSettingKind,
    ) -> String {
        match self
            .workflow_chain_entries()
            .get(entry)
            .map(|record| &record.source)
        {
            Some(WorkflowChainEntrySource::DirectTable) => self
                .workflow_chain_setting_values(kind)
                .get(entry)
                .cloned()
                .unwrap_or_default(),
            Some(WorkflowChainEntrySource::CallibrarySpec { spec, .. }) => {
                workflow_callib_setting_raw_value(spec, kind)
            }
            _ => String::new(),
        }
    }

    fn set_workflow_chain_setting_value(
        &mut self,
        entry: usize,
        kind: WorkflowChainSettingKind,
        value: String,
    ) {
        let len = self.split_csv_field("gaintables").len();
        if entry >= len {
            return;
        }
        let field_id = match kind {
            WorkflowChainSettingKind::Gainfield => "gainfield",
            WorkflowChainSettingKind::Interp => "interp",
            WorkflowChainSettingKind::Spwmap => "spwmap",
            WorkflowChainSettingKind::Calwt => "calwt",
        };
        let separator = match kind {
            WorkflowChainSettingKind::Calwt => ",",
            _ => ";",
        };
        let mut values = self.workflow_chain_setting_values(kind);
        if values.len() < len {
            values.resize(len, String::new());
        }
        values[entry] = value.trim().to_string();
        while values.last().is_some_and(|item| item.is_empty()) {
            values.pop();
        }
        let _ = self.apply_startup_text_value(field_id, values.join(separator));
    }

    fn set_workflow_callib_setting_value(
        &mut self,
        entry: usize,
        callib_path: &Path,
        spec_index: usize,
        kind: WorkflowChainSettingKind,
        value: String,
    ) -> Result<(), String> {
        let mut specs =
            load_apply_specs_from_callib(callib_path).map_err(|error| error.to_string())?;
        let Some(spec) = specs.get_mut(spec_index) else {
            return Err(format!(
                "callibrary entry {} no longer exists in {}",
                spec_index + 1,
                callib_path.display()
            ));
        };
        match kind {
            WorkflowChainSettingKind::Gainfield => {
                spec.gainfield = parse_workflow_gainfield_value(&value)?;
            }
            WorkflowChainSettingKind::Interp => {
                spec.interp = parse_workflow_interp_value(&value)?;
            }
            WorkflowChainSettingKind::Spwmap => {
                spec.spwmap = parse_workflow_spwmap_value(&value)?;
            }
            WorkflowChainSettingKind::Calwt => {
                spec.calwt = parse_workflow_calwt_value(&value)?;
            }
        }
        save_apply_specs_to_callib(callib_path, &specs).map_err(|error| error.to_string())?;
        self.selected_form = FormSelection::WorkflowChainSetting(entry, kind);
        Ok(())
    }

    fn apply_workflow_chain_setting_value(
        &mut self,
        entry: usize,
        kind: WorkflowChainSettingKind,
        value: String,
    ) -> Result<(), String> {
        let Some(source) = self
            .workflow_chain_entries()
            .get(entry)
            .map(|record| record.source.clone())
        else {
            return Err("workflow chain entry is no longer available".to_string());
        };
        match source {
            WorkflowChainEntrySource::DirectTable => {
                self.set_workflow_chain_setting_value(entry, kind, value);
                Ok(())
            }
            WorkflowChainEntrySource::CallibrarySpec {
                callib_path,
                spec_index,
                ..
            } => {
                self.set_workflow_callib_setting_value(entry, &callib_path, spec_index, kind, value)
            }
            WorkflowChainEntrySource::CallibraryFile { .. } => {
                Err("select a callibrary entry to edit its policies".to_string())
            }
            WorkflowChainEntrySource::CallibraryError { .. } => {
                Err("fix the callibrary parse error before editing entries".to_string())
            }
        }
    }

    fn workflow_chain_setting_records(&self, entry: usize) -> Vec<WorkflowChainSettingRecord> {
        [
            WorkflowChainSettingKind::Gainfield,
            WorkflowChainSettingKind::Interp,
            WorkflowChainSettingKind::Spwmap,
            WorkflowChainSettingKind::Calwt,
        ]
        .into_iter()
        .map(|kind| {
            let value = self.workflow_chain_setting_display_value(entry, kind);
            WorkflowChainSettingRecord {
                entry,
                kind,
                text: render_workflow_detail_display(&WorkflowDetailDisplay {
                    label: kind.label().to_string(),
                    value,
                    indent: 2,
                }),
            }
        })
        .collect()
    }

    fn workflow_chain_setting_display_value(
        &self,
        entry: usize,
        kind: WorkflowChainSettingKind,
    ) -> String {
        let value = self.workflow_chain_setting_raw_value(entry, kind);
        if value.is_empty() {
            match kind {
                WorkflowChainSettingKind::Gainfield => "<default>".to_string(),
                WorkflowChainSettingKind::Interp => "nearest".to_string(),
                WorkflowChainSettingKind::Spwmap => "<identity>".to_string(),
                WorkflowChainSettingKind::Calwt => "false".to_string(),
            }
        } else {
            value
        }
    }

    fn workflow_run_snapshots(&self) -> Vec<WorkflowRunSnapshot> {
        self.history_entries
            .iter()
            .filter_map(|entry| {
                entry.stage.map(|stage| WorkflowRunSnapshot {
                    stage_id: stage.key(),
                    sequence: entry.sequence,
                })
            })
            .collect()
    }

    fn workflow_product_snapshots(&self) -> Vec<WorkflowProductSnapshot> {
        self.workflow_products
            .iter()
            .map(|product| WorkflowProductSnapshot {
                stage_id: product.stage.key(),
                revision: product.revision,
                status: product.status,
                dependency_revisions: product.dependency_revisions.clone(),
            })
            .collect()
    }

    fn workflow_stage_states(&self) -> Vec<WorkflowStageState> {
        workflow_stage_states(
            &self.workflow_run_snapshots(),
            &self.workflow_product_snapshots(),
            self.non_empty_field_text("vis").is_some(),
        )
    }

    fn workflow_stage_state(&self, stage: WorkflowStageId) -> Option<WorkflowStageState> {
        self.workflow_stage_states()
            .into_iter()
            .find(|state| state.id == stage.key())
    }

    fn active_workflow_dependency_revisions(&self) -> BTreeMap<&'static str, usize> {
        let mut revisions = BTreeMap::new();
        for product in self
            .workflow_products
            .iter()
            .filter(|product| product.status == WorkflowProductStatus::Active)
        {
            revisions
                .entry(product.stage.key())
                .and_modify(|current: &mut usize| *current = (*current).max(product.revision))
                .or_insert(product.revision);
        }
        revisions
    }

    fn current_workflow_stage(&self) -> WorkflowStageId {
        WorkflowStageId::from_mode(
            self.field_text("mode")
                .as_deref()
                .unwrap_or(WorkflowStageId::InspectDataset.cli_mode()),
        )
    }

    fn set_current_workflow_stage(&mut self, stage: WorkflowStageId) {
        let previous_stage = self.current_workflow_stage();
        let _ = self.apply_startup_text_value("mode", stage.cli_mode().to_string());
        self.ensure_workflow_stage_defaults(Some(previous_stage), stage);
        self.selected_form = FormSelection::WorkflowStage(stage);
    }

    fn ensure_workflow_stage_defaults(
        &mut self,
        previous_stage: Option<WorkflowStageId>,
        stage: WorkflowStageId,
    ) {
        match stage {
            WorkflowStageId::SolveGain
            | WorkflowStageId::SolveBandpass
            | WorkflowStageId::FluxScale => {
                let current_output = self.non_empty_field_text("out_table");
                let previous_suggestion = previous_stage
                    .and_then(|previous| self.workflow_suggested_output_table_path(previous));
                let should_update = current_output.is_none()
                    || matches!(
                        (&current_output, &previous_suggestion),
                        (Some(current), Some(previous)) if current == previous
                    );
                if should_update
                    && let Some(path) = self.workflow_suggested_output_table_path(stage)
                {
                    let _ = self.apply_startup_text_value("out_table", path);
                }
            }
            WorkflowStageId::InspectResults => {
                if self.current_calibration_plot_table_path().is_none()
                    && let Some(path) = self.workflow_latest_inspectable_product_path()
                {
                    self.apply_inspection_defaults_for_path(path);
                }
            }
            WorkflowStageId::InspectDataset | WorkflowStageId::Apply => {}
        }

        if stage == WorkflowStageId::FluxScale
            && self.non_empty_field_text("fluxscale_input").is_none()
            && let Some(path) = self.workflow_latest_stage_product_path(WorkflowStageId::SolveGain)
        {
            let _ = self.apply_startup_text_value("fluxscale_input", path);
        }
    }

    fn workflow_suggested_output_table_path(&self, stage: WorkflowStageId) -> Option<String> {
        let base_path = match stage {
            WorkflowStageId::FluxScale => self
                .non_empty_field_text("fluxscale_input")
                .map(PathBuf::from)
                .or_else(|| self.non_empty_field_text("vis").map(PathBuf::from)),
            WorkflowStageId::SolveGain | WorkflowStageId::SolveBandpass => {
                self.non_empty_field_text("vis").map(PathBuf::from)
            }
            WorkflowStageId::InspectDataset
            | WorkflowStageId::Apply
            | WorkflowStageId::InspectResults => None,
        }?;
        suggested_output_table_path(stage, &base_path)
    }

    fn workflow_latest_stage_product_path(&self, stage: WorkflowStageId) -> Option<String> {
        self.workflow_products
            .iter()
            .filter(|product| {
                product.stage == stage && product.status == WorkflowProductStatus::Active
            })
            .max_by_key(|product| product.revision)
            .map(|product| product.path.display().to_string())
    }

    fn workflow_latest_inspectable_product_path(&self) -> Option<String> {
        self.workflow_products
            .iter()
            .filter(|product| {
                product.status == WorkflowProductStatus::Active
                    && matches!(
                        product.stage,
                        WorkflowStageId::SolveGain
                            | WorkflowStageId::SolveBandpass
                            | WorkflowStageId::FluxScale
                    )
            })
            .max_by_key(|product| product.run_sequence)
            .map(|product| product.path.display().to_string())
    }

    fn workflow_stage_row_text(&self, stage: WorkflowStageId) -> String {
        let selected = self.current_workflow_stage() == stage;
        let Some(state) = self.workflow_stage_state(stage) else {
            return render_workflow_stage_display(&WorkflowStageDisplay {
                label: stage.label().to_string(),
                status: crate::workflow::WorkflowStageStatus::Blocked,
                latest_revision: None,
                stale_revisions: 0,
                run_count: 0,
                recommended: false,
                selected,
            });
        };
        render_workflow_stage_display(&WorkflowStageDisplay {
            label: stage.label().to_string(),
            status: state.status,
            latest_revision: state.latest_revision,
            stale_revisions: state.stale_revisions,
            run_count: state.run_count,
            recommended: state.recommended,
            selected,
        })
    }

    fn workflow_chain_entries(&self) -> Vec<WorkflowChainEntryRecord> {
        workflow_chain_entries(
            &self.split_csv_field("gaintables"),
            self.non_empty_field_text("callib").as_deref(),
        )
    }

    fn workflow_products_section_index(&self) -> usize {
        self.sections
            .iter()
            .position(|section| section.name == "Products")
            .unwrap_or(0)
    }

    fn select_workflow_chain_entry(&mut self, index: usize) {
        let Some(entry) = self.workflow_chain_entries().get(index).cloned() else {
            return;
        };
        self.selected_form = FormSelection::WorkflowChainEntry(index);
        match entry.source {
            WorkflowChainEntrySource::DirectTable => {
                if let Some(path) = entry.inspect_path {
                    let path_text = path.display().to_string();
                    let _ = self.apply_startup_text_value("table_path", path_text.clone());
                    let _ = self.apply_startup_text_value("summary_paths", path_text.clone());
                    let preset = self
                        .workflow_products
                        .iter()
                        .find(|product| product.path == path)
                        .and_then(|product| {
                            workflow_preferred_diagnostic_preset_for_stage(product.stage)
                        })
                        .unwrap_or_else(|| {
                            preferred_workflow_calibration_preset(
                                WorkflowCalibrationArtifactKind::GainLike,
                            )
                        });
                    self.plot_workspace.selected_catalog_target =
                        Some(PlotCatalogTarget::Calibration(preset));
                    self.clear_plot_render_cache();
                    self.activate_result_tab(ResultTab::Diagnostics);
                    self.result.status_line = format!(
                        "Selected chain entry: {}. Enter inspects it. Delete removes it. Ctrl-K/Ctrl-J reorder.",
                        path.display()
                    );
                }
            }
            WorkflowChainEntrySource::CallibraryFile { path } => {
                self.activate_result_tab(ResultTab::Products);
                self.result.status_line = format!(
                    "Selected callibrary file: {}. Delete removes it.",
                    path.display()
                );
            }
            WorkflowChainEntrySource::CallibrarySpec { callib_path, .. } => {
                if let Some(path) = entry.inspect_path {
                    let path_text = path.display().to_string();
                    let _ = self.apply_startup_text_value("table_path", path_text.clone());
                    let _ = self.apply_startup_text_value("summary_paths", path_text.clone());
                    let preset = self
                        .workflow_products
                        .iter()
                        .find(|product| product.path == path)
                        .and_then(|product| {
                            workflow_preferred_diagnostic_preset_for_stage(product.stage)
                        })
                        .unwrap_or_else(|| {
                            preferred_workflow_calibration_preset(
                                WorkflowCalibrationArtifactKind::GainLike,
                            )
                        });
                    self.plot_workspace.selected_catalog_target =
                        Some(PlotCatalogTarget::Calibration(preset));
                    self.clear_plot_render_cache();
                    self.activate_result_tab(ResultTab::Diagnostics);
                    self.result.status_line = format!(
                        "Selected callibrary entry from {}: {}. Enter inspects it. Chain settings edit the file.",
                        callib_path.display(),
                        path.display()
                    );
                }
            }
            WorkflowChainEntrySource::CallibraryError { path, message } => {
                self.activate_result_tab(ResultTab::Products);
                self.result.status_line =
                    format!("Callibrary parse error for {}: {}", path.display(), message);
                self.result.status_kind = StatusKind::Warning;
                return;
            }
        }
        self.result.status_kind = StatusKind::Info;
    }

    fn remove_workflow_chain_entry(&mut self, index: usize) {
        let entries = self.workflow_chain_entries();
        let Some(entry) = entries.get(index).cloned() else {
            return;
        };
        match entry.source {
            WorkflowChainEntrySource::DirectTable => {}
            WorkflowChainEntrySource::CallibraryFile { .. } => {
                let _ = self.apply_startup_text_value("callib", String::new());
                self.result.status_line = "Removed callibrary entry.".to_string();
                self.result.status_kind = StatusKind::Info;
                self.selected_form = FormSelection::Section(self.workflow_products_section_index());
                self.activate_result_tab(ResultTab::Products);
                return;
            }
            WorkflowChainEntrySource::CallibrarySpec { callib_path, .. }
            | WorkflowChainEntrySource::CallibraryError {
                path: callib_path, ..
            } => {
                self.result.status_line = format!(
                    "Callibrary entries are managed by {}. Clear or replace the callibrary file to change them.",
                    callib_path.display()
                );
                self.result.status_kind = StatusKind::Warning;
                return;
            }
        }
        let mut gaintables = self.split_csv_field("gaintables");
        if index < gaintables.len() {
            let removed = gaintables.remove(index);
            let _ = self.apply_startup_text_value("gaintables", gaintables.join(","));
            for kind in [
                WorkflowChainSettingKind::Gainfield,
                WorkflowChainSettingKind::Interp,
                WorkflowChainSettingKind::Spwmap,
                WorkflowChainSettingKind::Calwt,
            ] {
                let mut values = self.workflow_chain_setting_values(kind);
                if index < values.len() {
                    values.remove(index);
                }
                let separator = if kind == WorkflowChainSettingKind::Calwt {
                    ","
                } else {
                    ";"
                };
                let field_id = match kind {
                    WorkflowChainSettingKind::Gainfield => "gainfield",
                    WorkflowChainSettingKind::Interp => "interp",
                    WorkflowChainSettingKind::Spwmap => "spwmap",
                    WorkflowChainSettingKind::Calwt => "calwt",
                };
                while values.last().is_some_and(|item| item.is_empty()) {
                    values.pop();
                }
                let _ = self.apply_startup_text_value(field_id, values.join(separator));
            }
            self.result.status_line = format!("Removed chain entry: {removed}.");
            self.result.status_kind = StatusKind::Info;
        }

        let updated_entries = self.workflow_chain_entries();
        if updated_entries.is_empty() {
            self.selected_form = FormSelection::Section(self.workflow_products_section_index());
            self.activate_result_tab(ResultTab::Products);
        } else {
            self.select_workflow_chain_entry(index.min(updated_entries.len().saturating_sub(1)));
        }
    }

    fn move_selected_workflow_chain_entry(&mut self, forward: bool) {
        let FormSelection::WorkflowChainEntry(index) = self.selected_form else {
            return;
        };
        let entries = self.workflow_chain_entries();
        if !matches!(
            entries.get(index).map(|entry| &entry.source),
            Some(WorkflowChainEntrySource::DirectTable)
        ) {
            self.result.status_line =
                "Only direct calibration-table chain entries can be reordered.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        let mut gaintables = self.split_csv_field("gaintables");
        if index >= gaintables.len() || gaintables.len() < 2 {
            return;
        }
        let target = if forward {
            (index + 1).min(gaintables.len() - 1)
        } else {
            index.saturating_sub(1)
        };
        if target == index {
            return;
        }
        gaintables.swap(index, target);
        let _ = self.apply_startup_text_value("gaintables", gaintables.join(","));
        for kind in [
            WorkflowChainSettingKind::Gainfield,
            WorkflowChainSettingKind::Interp,
            WorkflowChainSettingKind::Spwmap,
            WorkflowChainSettingKind::Calwt,
        ] {
            let mut values = self.workflow_chain_setting_values(kind);
            if target < values.len() && index < values.len() {
                values.swap(index, target);
                let separator = if kind == WorkflowChainSettingKind::Calwt {
                    ","
                } else {
                    ";"
                };
                let field_id = match kind {
                    WorkflowChainSettingKind::Gainfield => "gainfield",
                    WorkflowChainSettingKind::Interp => "interp",
                    WorkflowChainSettingKind::Spwmap => "spwmap",
                    WorkflowChainSettingKind::Calwt => "calwt",
                };
                while values.last().is_some_and(|item| item.is_empty()) {
                    values.pop();
                }
                let _ = self.apply_startup_text_value(field_id, values.join(separator));
            }
        }
        self.select_workflow_chain_entry(target);
        self.result.status_line = if forward {
            "Moved chain entry down.".to_string()
        } else {
            "Moved chain entry up.".to_string()
        };
        self.result.status_kind = StatusKind::Info;
    }

    fn promote_selected_workflow_product(&mut self) {
        let FormSelection::WorkflowProduct(index) = self.selected_form else {
            return;
        };
        let Some(product) = self.workflow_products.get(index).cloned() else {
            return;
        };
        if !matches!(
            product.stage,
            WorkflowStageId::SolveGain
                | WorkflowStageId::SolveBandpass
                | WorkflowStageId::FluxScale
        ) {
            self.result.status_line =
                "Selected workflow product cannot be promoted into the apply chain.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        }

        let product_path = product.path.display().to_string();
        let mut gaintables = self.split_csv_field("gaintables");
        if let Some(existing_index) = gaintables.iter().position(|path| path == &product_path) {
            self.select_workflow_chain_entry(existing_index);
            self.result.status_line = format!(
                "Workflow product already in chain: {}.",
                product.path.display()
            );
            self.result.status_kind = StatusKind::Info;
            return;
        }

        if self.non_empty_field_text("callib").is_some() {
            let _ = self.apply_startup_text_value("callib", String::new());
        }

        let replacement_index = gaintables.iter().position(|path| {
            let candidate = PathBuf::from(path);
            self.workflow_products
                .iter()
                .any(|record| record.path == candidate && record.stage == product.stage)
        });

        let target_index = if let Some(index) = replacement_index {
            gaintables[index] = product_path.clone();
            index
        } else {
            gaintables.push(product_path.clone());
            gaintables.len() - 1
        };

        let _ = self.apply_startup_text_value("gaintables", gaintables.join(","));
        if replacement_index.is_none() {
            for kind in [
                WorkflowChainSettingKind::Gainfield,
                WorkflowChainSettingKind::Interp,
                WorkflowChainSettingKind::Spwmap,
                WorkflowChainSettingKind::Calwt,
            ] {
                let default = match kind {
                    WorkflowChainSettingKind::Interp => "nearest".to_string(),
                    WorkflowChainSettingKind::Calwt => "false".to_string(),
                    _ => String::new(),
                };
                self.set_workflow_chain_setting_value(target_index, kind, default);
            }
        }
        self.select_workflow_chain_entry(target_index);
        self.result.status_line = if replacement_index.is_some() {
            format!(
                "Promoted workflow product into chain, replacing the prior {} table.",
                product.stage.label()
            )
        } else {
            format!(
                "Promoted workflow product into chain: {}.",
                product.path.display()
            )
        };
        self.result.status_kind = StatusKind::Info;
    }

    fn promote_latest_workflow_product_from_current_report(&mut self) -> bool {
        let Some(report) = self.current_calibration_report() else {
            return false;
        };
        let Some(stage) = workflow_stage_from_report(report) else {
            return false;
        };
        if !matches!(
            stage,
            WorkflowStageId::SolveGain
                | WorkflowStageId::SolveBandpass
                | WorkflowStageId::FluxScale
        ) {
            return false;
        }

        let product_path = match report {
            ManagedCalibrationOutput::SolveGain(report) => &report.output_table,
            ManagedCalibrationOutput::SolveBandpass(report) => &report.output_table,
            ManagedCalibrationOutput::FluxScale(report) => &report.output_table,
            ManagedCalibrationOutput::Gencal(report) => &report.output_table,
            ManagedCalibrationOutput::Apply(_)
            | ManagedCalibrationOutput::ContinuumSubtract(_)
            | ManagedCalibrationOutput::ExportCorrectedData(_)
            | ManagedCalibrationOutput::Summary(_)
            | ManagedCalibrationOutput::PlanApply(_)
            | ManagedCalibrationOutput::Stats(_) => return false,
        };

        let Some(index) =
            self.workflow_products
                .iter()
                .enumerate()
                .rev()
                .find_map(|(index, product)| {
                    (product.stage == stage && product.path == *product_path).then_some(index)
                })
        else {
            return false;
        };

        self.selected_form = FormSelection::WorkflowProduct(index);
        self.promote_selected_workflow_product();
        true
    }

    fn append_workflow_chain_path(&mut self, path: String) {
        let path = path.trim().to_string();
        if path.is_empty() {
            return;
        }
        let mut gaintables = self.split_csv_field("gaintables");
        if let Some(existing_index) = gaintables.iter().position(|candidate| candidate == &path) {
            self.select_workflow_chain_entry(existing_index);
            self.result.status_line = format!("Chain already contains {path}.");
            self.result.status_kind = StatusKind::Info;
            return;
        }
        if self.non_empty_field_text("callib").is_some() {
            let _ = self.apply_startup_text_value("callib", String::new());
        }
        gaintables.push(path.clone());
        let index = gaintables.len() - 1;
        let _ = self.apply_startup_text_value("gaintables", gaintables.join(","));
        for kind in [
            WorkflowChainSettingKind::Gainfield,
            WorkflowChainSettingKind::Interp,
            WorkflowChainSettingKind::Spwmap,
            WorkflowChainSettingKind::Calwt,
        ] {
            let default = match kind {
                WorkflowChainSettingKind::Interp => "nearest".to_string(),
                WorkflowChainSettingKind::Calwt => "false".to_string(),
                _ => String::new(),
            };
            self.set_workflow_chain_setting_value(index, kind, default);
        }
        self.select_workflow_chain_entry(index);
        self.result.status_line = format!("Added chain entry: {path}.");
        self.result.status_kind = StatusKind::Ok;
    }

    fn set_workflow_callibrary_path(&mut self, path: String) {
        let path = path.trim().to_string();
        if path.is_empty() {
            return;
        }
        let _ = self.apply_startup_text_value("gaintables", String::new());
        for field_id in ["gainfield", "interp", "spwmap", "calwt"] {
            let _ = self.apply_startup_text_value(field_id, String::new());
        }
        let _ = self.apply_startup_text_value("callib", path.clone());
        self.select_workflow_chain_entry(0);
        self.result.status_line = format!("Using callibrary: {path}.");
        self.result.status_kind = StatusKind::Ok;
    }

    fn select_workflow_product(&mut self, index: usize) {
        let Some(product) = self.workflow_products.get(index).cloned() else {
            return;
        };
        self.selected_form = FormSelection::WorkflowProduct(index);
        let product_path = product.path.display().to_string();
        match product.stage {
            WorkflowStageId::SolveGain
            | WorkflowStageId::SolveBandpass
            | WorkflowStageId::FluxScale => {
                let _ = self.apply_startup_text_value("table_path", product_path.clone());
                let _ = self.apply_startup_text_value("summary_paths", product_path.clone());
                if let Some(preset) = workflow_preferred_diagnostic_preset_for_stage(product.stage)
                {
                    self.plot_workspace.selected_catalog_target =
                        Some(PlotCatalogTarget::Calibration(preset));
                }
            }
            WorkflowStageId::Apply => {
                if let Some(preset) = workflow_preferred_diagnostic_preset_for_stage(product.stage)
                {
                    self.plot_workspace.selected_catalog_target =
                        Some(PlotCatalogTarget::Calibration(preset));
                }
            }
            WorkflowStageId::InspectDataset | WorkflowStageId::InspectResults => {}
        }
        self.clear_plot_render_cache();
        self.activate_result_tab(if self.app.shell_kind() == AppShellKind::Workflow {
            ResultTab::Diagnostics
        } else {
            ResultTab::Overview
        });
        self.result.status_line = format!(
            "Selected workflow product: {}. Enter inspects it. Shift-P promotes it into the chain.",
            product.path.display()
        );
        self.result.status_kind = StatusKind::Info;
    }

    fn activate_workflow_product_action(&mut self, kind: WorkflowProductActionKind) {
        self.selected_form = FormSelection::WorkflowProductAction(kind);
        match kind {
            WorkflowProductActionKind::AddSolvedProduct => {
                let entries = self.workflow_product_picker_entries();
                if entries.is_empty() {
                    self.result.status_line =
                        "No solved products available to add to the chain yet.".to_string();
                    self.result.status_kind = StatusKind::Warning;
                    return;
                }
                self.open_choice_picker_target(
                    ChoicePickerTarget::WorkflowProductAction(kind),
                    "Add Solved Product to Chain".to_string(),
                    entries,
                    String::new(),
                );
            }
            WorkflowProductActionKind::ImportChainTable => {
                let start_hint = self
                    .non_empty_field_text("table_path")
                    .or_else(|| self.non_empty_field_text("vis"));
                let start = chooser_start_path(start_hint.as_deref());
                self.open_path_chooser_target(PathChooserTarget::WorkflowImportChainTable, start);
            }
            WorkflowProductActionKind::ChooseCallibrary => {
                let start_hint = self
                    .non_empty_field_text("callib")
                    .or_else(|| self.non_empty_field_text("vis"));
                let start = chooser_start_path(start_hint.as_deref());
                self.open_path_chooser_target(PathChooserTarget::WorkflowChooseCallibrary, start);
            }
        }
    }

    fn activate_workflow_chain_setting(&mut self, entry: usize, kind: WorkflowChainSettingKind) {
        self.selected_form = FormSelection::WorkflowChainSetting(entry, kind);
        self.open_workflow_chain_setting_picker(entry, kind);
    }

    fn open_workflow_chain_setting_picker(&mut self, entry: usize, kind: WorkflowChainSettingKind) {
        let entries = self.workflow_chain_setting_picker_entries(kind);
        let current = self.workflow_chain_setting_raw_value(entry, kind);
        self.open_choice_picker_target(
            ChoicePickerTarget::WorkflowChainSetting(entry, kind),
            format!("Choose chain entry {} {}", entry + 1, kind.label()),
            entries,
            current,
        );
    }

    fn default_plot_result_tab(&self) -> ResultTab {
        match self.app.shell_kind() {
            AppShellKind::Workflow => ResultTab::Diagnostics,
            _ => ResultTab::Plots,
        }
    }

    pub(crate) fn result_tab_uses_plot_workspace(&self) -> bool {
        matches!(
            self.active_result_tab,
            ResultTab::Plots | ResultTab::Diagnostics
        )
    }

    fn current_image_browser_parameters(&self) -> Result<ImageBrowserParameters, String> {
        Ok(ImageBrowserParameters {
            blc: self.required_session_parameter_text("blc")?,
            trc: self.required_session_parameter_text("trc")?,
            inc: self.required_session_parameter_text("inc")?,
            stretch: self.required_session_parameter_text("stretch")?,
            autoscale: self.required_session_parameter_text("autoscale")?,
            clip_low: self.required_session_parameter_text("clip_low")?,
            clip_high: self.required_session_parameter_text("clip_high")?,
        })
    }

    fn session_parameter_value(&self, id: &str) -> Option<&ParameterValue> {
        self.parameter_session
            .as_ref()?
            .states()
            .get(id)?
            .value
            .as_ref()
    }

    fn session_parameter_text(&self, id: &str) -> Option<String> {
        self.session_parameter_value(id).map(parameter_value_text)
    }

    fn required_session_parameter_text(&self, id: &str) -> Result<String, String> {
        self.session_parameter_text(id)
            .ok_or_else(|| format!("resolved session parameter {id:?} is unavailable"))
    }

    fn required_session_parameter_is_explicit(&self, id: &str) -> Result<bool, String> {
        self.parameter_session
            .as_ref()
            .and_then(|session| session.states().get(id))
            .map(|state| state.explicit)
            .ok_or_else(|| format!("resolved session parameter {id:?} is unavailable"))
    }

    fn table_session_startup_config(&self) -> Result<TableSessionStartupConfig, String> {
        let view_text = self.required_session_parameter_text("view")?;
        let view = parse_table_session_view(&view_text)?;

        let rowstart = parameter_integer(self.session_parameter_value("rowstart"), "rowstart")?;
        let row_start = usize::try_from(rowstart)
            .map_err(|_| "tablebrowser rowstart must be non-negative".to_string())?;
        let nrow = parameter_integer(self.session_parameter_value("nrow"), "nrow")?;
        let row_count = usize::try_from(nrow)
            .ok()
            .filter(|count| *count > 0)
            .ok_or_else(|| "tablebrowser nrow must be greater than zero".to_string())?;

        let linked_table_text = self.required_session_parameter_text("linkedtable")?;
        let linked_table = (!parameter_reference_is_none(&linked_table_text))
            .then(|| linked_table_text.trim().to_string());

        let bookmark_text = self.required_session_parameter_text("bookmark")?;
        let bookmark = parse_table_bookmark(&bookmark_text)?;

        let content_mode_text = self.required_session_parameter_text("contentmode")?;
        let content_mode = parse_table_content_mode(&content_mode_text)?;
        let requires_configure = [
            "view",
            "bookmark",
            "rowstart",
            "nrow",
            "linkedtable",
            "contentmode",
        ]
        .into_iter()
        .try_fold(false, |any, name| {
            self.required_session_parameter_is_explicit(name)
                .map(|explicit| any || explicit)
        })?;

        Ok(TableSessionStartupConfig {
            parameters: BrowserParameters {
                view,
                row_start,
                row_count,
                linked_table,
                bookmark,
                content_mode,
            },
            requires_configure,
        })
    }

    fn image_session_startup_config(&self) -> Result<ImageSessionStartupConfig, String> {
        let view = parse_image_session_view(&self.required_session_parameter_text("view")?)?;
        let colormap = parse_image_colormap(&self.required_session_parameter_text("colormap")?)?;
        let content_mode =
            parse_image_content_mode(&self.required_session_parameter_text("contentmode")?)?;
        let movie_axis = self.required_session_parameter_text("movieaxis")?;
        let profile_axis = self.required_session_parameter_text("profileaxis")?;
        let fps = parameter_f64(self.session_parameter_value("fps"), "fps")?;
        if !fps.is_finite() || fps <= 0.0 {
            return Err("FPS must be a positive number.".to_string());
        }
        let looping = parameter_bool(self.session_parameter_value("loop"), "loop")?;
        let region =
            parse_image_region_reference(&self.required_session_parameter_text("region")?)?;
        let mask = parse_image_mask_reference(&self.required_session_parameter_text("mask")?)?;

        Ok(ImageSessionStartupConfig {
            view_parameters: self.current_image_browser_parameters()?,
            view,
            enforce_view: self.required_session_parameter_is_explicit("view")?,
            content_mode,
            content_mode_explicit: self.required_session_parameter_is_explicit("contentmode")?,
            colormap,
            movie_axis,
            profile_axis,
            fps,
            looping,
            region,
            region_explicit: self.required_session_parameter_is_explicit("region")?,
            mask,
            mask_explicit: self.required_session_parameter_is_explicit("mask")?,
        })
    }

    fn set_accepted_parameter_value(
        &mut self,
        name: &str,
        value: ParameterValue,
    ) -> Result<(), String> {
        let Some(session) = self.parameter_session.as_mut() else {
            return Err("typed parameter session is unavailable".to_string());
        };
        if session
            .states()
            .get(name)
            .and_then(|state| state.value.as_ref())
            == Some(&value)
        {
            return Ok(());
        }
        session
            .set(name.to_string(), value)
            .map_err(|error| error.to_string())
    }

    fn sync_accepted_image_window_parameters(
        &mut self,
        parameters: &ImageBrowserParameters,
    ) -> Result<(), String> {
        for (name, value) in [
            ("blc", parameters.blc.as_str()),
            ("trc", parameters.trc.as_str()),
            ("inc", parameters.inc.as_str()),
            ("stretch", parameters.stretch.as_str()),
            ("autoscale", parameters.autoscale.as_str()),
            ("clip_low", parameters.clip_low.as_str()),
            ("clip_high", parameters.clip_high.as_str()),
        ] {
            self.set_accepted_parameter_value(name, ParameterValue::String(value.to_string()))?;
        }
        if let Some(session) = self.parameter_session.as_ref() {
            sync_form_fields_from_parameter_session(&mut self.fields, session);
        }
        Ok(())
    }

    fn rollback_rejected_live_parameter_change(&mut self) {
        let Some(session) = self.pending_live_parameter_rollback.take() else {
            return;
        };
        self.parameter_session = Some(session);
        self.parameter_edit_errors.clear();
        if let Some(session) = self.parameter_session.as_ref() {
            sync_form_fields_from_parameter_session(&mut self.fields, session);
        }
    }

    fn accept_live_parameter_changes(&mut self) {
        self.pending_live_parameter_rollback = None;
        self.queue_accepted_session_parameter_change();
    }

    fn apply_live_image_view_parameters_if_needed(&mut self, field_index: usize) {
        let Some(field_id) = self
            .fields
            .get(field_index)
            .map(|field| field.schema.id.clone())
        else {
            return;
        };
        if !IMEXPLORE_LIVE_PARAMETER_FIELD_IDS.contains(&field_id.as_str()) {
            return;
        }
        if !self
            .browser_session()
            .is_some_and(|session| session.kind() == BrowserAppKind::Image)
        {
            return;
        }
        match field_id.as_str() {
            "image" => self.reject_live_parameter_edit(
                "Close the active imexplore session before changing Image Path.".to_string(),
            ),
            "blc" | "trc" | "inc" | "stretch" | "autoscale" | "clip_low" | "clip_high" => {
                match self.current_image_browser_parameters() {
                    Ok(parameters) => {
                        self.send_browser_command(BrowserRequest::SetImageViewParameters {
                            parameters,
                        });
                    }
                    Err(error) => self.reject_live_parameter_edit(error),
                }
            }
            "fps" => self.apply_live_image_movie_fps(),
            "view" => {
                let result = self
                    .session_parameter_text("view")
                    .ok_or_else(|| "imexplore view is unavailable".to_string())
                    .and_then(|value| parse_image_session_view(&value));
                match result {
                    Ok(view) => self.apply_live_image_view(view),
                    Err(error) => self.reject_live_parameter_edit(error),
                }
            }
            "contentmode" => {
                let result = self
                    .session_parameter_text("contentmode")
                    .ok_or_else(|| "imexplore content mode is unavailable".to_string())
                    .and_then(|value| parse_image_content_mode(&value));
                match result {
                    Ok(mode) => {
                        if self
                            .send_browser_command(BrowserRequest::SetImagePlaneContentMode { mode })
                            && let Some(state) = self.image_browser_session_state_mut()
                        {
                            state.plane_mode = image_plane_mode(mode);
                            state.snapshot_generation = state.snapshot_generation.saturating_add(1);
                        }
                    }
                    Err(error) => self.reject_live_parameter_edit(error),
                }
            }
            "colormap" => {
                let result = self
                    .session_parameter_text("colormap")
                    .ok_or_else(|| "imexplore colormap is unavailable".to_string())
                    .and_then(|value| parse_image_colormap(&value));
                match result {
                    Ok(colormap) => {
                        if let Some(state) = self.image_browser_session_state_mut() {
                            state.plane_colormap = colormap;
                            state.snapshot_generation = state.snapshot_generation.saturating_add(1);
                        }
                        self.accept_live_parameter_changes();
                    }
                    Err(error) => self.reject_live_parameter_edit(error),
                }
            }
            "movieaxis" => {
                let selector = match self.required_session_parameter_text("movieaxis") {
                    Ok(selector) => selector,
                    Err(error) => {
                        self.reject_live_parameter_edit(error);
                        return;
                    }
                };
                let Some(state) = self.image_browser_session_state() else {
                    return;
                };
                let resolved = resolve_image_axis_selector(&state.snapshot, &selector, "movieaxis");
                match resolved {
                    Ok(selected) => {
                        let index = selected.map(|(index, _)| index).unwrap_or(0);
                        if let Some(state) = self.image_browser_session_state_mut() {
                            state.selected_non_display_axis = index;
                            state.clamp_selected_non_display_axis();
                            state.snapshot_generation = state.snapshot_generation.saturating_add(1);
                        }
                        self.accept_live_parameter_changes();
                    }
                    Err(error) => self.reject_live_parameter_edit(error),
                }
            }
            "profileaxis" => {
                let selector = match self.required_session_parameter_text("profileaxis") {
                    Ok(selector) => selector,
                    Err(error) => {
                        self.reject_live_parameter_edit(error);
                        return;
                    }
                };
                let Some(state) = self.image_browser_session_state() else {
                    return;
                };
                let resolved =
                    resolve_image_axis_selector(&state.snapshot, &selector, "profileaxis");
                match resolved {
                    Ok(Some((_, axis))) => {
                        self.send_browser_command(BrowserRequest::SetImageProfileAxis { axis });
                    }
                    Ok(None) => {
                        self.accept_live_parameter_changes();
                    }
                    Err(error) => self.reject_live_parameter_edit(error),
                }
            }
            "loop" => match parameter_bool(self.session_parameter_value("loop"), "loop") {
                Ok(looping) => {
                    if let Some(state) = self.image_browser_session_state_mut() {
                        state.movie.looping = looping;
                    }
                    self.accept_live_parameter_changes();
                }
                Err(error) => self.reject_live_parameter_edit(error),
            },
            "region" => {
                let value = match self.required_session_parameter_text("region") {
                    Ok(value) => value,
                    Err(error) => {
                        self.reject_live_parameter_edit(error);
                        return;
                    }
                };
                match parse_image_region_reference(&value) {
                    Ok(region) => {
                        self.send_browser_command(BrowserRequest::SetImageSelectionReferences {
                            region: Some(region),
                            mask: None,
                        });
                    }
                    Err(error) => self.reject_live_parameter_edit(error),
                }
            }
            "mask" => {
                let value = match self.required_session_parameter_text("mask") {
                    Ok(value) => value,
                    Err(error) => {
                        self.reject_live_parameter_edit(error);
                        return;
                    }
                };
                match parse_image_mask_reference(&value) {
                    Ok(mask) => {
                        self.send_browser_command(BrowserRequest::SetImageSelectionReferences {
                            region: None,
                            mask: Some(mask),
                        });
                    }
                    Err(error) => self.reject_live_parameter_edit(error),
                }
            }
            _ => {}
        }
    }

    fn reject_live_parameter_edit(&mut self, error: String) {
        self.rollback_rejected_live_parameter_change();
        self.result.status_line = error;
        self.result.status_kind = StatusKind::Warning;
    }

    fn apply_live_image_view(&mut self, target: ImageBrowserView) {
        for _ in 0..4 {
            let Some(active) = self
                .image_browser_session_state()
                .map(|state| state.snapshot.active_view)
            else {
                return;
            };
            if active == target {
                self.accept_live_parameter_changes();
                return;
            }
            if !self.send_browser_command(BrowserRequest::CycleView { forward: true }) {
                return;
            }
        }
        self.reject_live_parameter_edit(format!(
            "imexplore backend did not expose requested view {}",
            target.label()
        ));
    }

    fn apply_live_image_movie_fps(&mut self) {
        let Ok(fps) = parameter_f64(self.session_parameter_value("fps"), "fps") else {
            self.result.status_line = "FPS must be a positive number.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        if !fps.is_finite() || fps <= 0.0 {
            self.result.status_line = "FPS must be a positive number.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        let context = self
            .image_browser_session_state()
            .map(|state| image_movie_perf_context_from_state(state, None, None, None))
            .unwrap_or_else(|| MoviePerfContext {
                requested_fps_milli: Some((fps * 1000.0).round() as u64),
                ..MoviePerfContext::default()
            });
        if let Some(state) = self.image_browser_session_state_mut() {
            state.movie.set_fps(fps);
        }
        self.result.status_line =
            format!("Movie FPS set to {}.", trim_float_text(format!("{fps:.3}")));
        self.result.status_kind = StatusKind::Info;
        self.movie_perf.fps_changed(context);
        self.accept_live_parameter_changes();
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

    fn required_parameter_bool(&self, id: &str) -> Result<bool, String> {
        match self.session_parameter_value(id) {
            Some(ParameterValue::Bool(value)) => Ok(*value),
            Some(value) => Err(format!(
                "resolved parameter {id:?} must be boolean, got {value:?}"
            )),
            None => Err(format!("resolved parameter {id:?} is unavailable")),
        }
    }

    fn verbose_enabled(&self) -> bool {
        self.bool_field_value("verbose").unwrap_or(true)
    }

    fn listunfl_enabled(&self) -> bool {
        self.bool_field_value("listunfl").unwrap_or(false)
    }

    fn visible_result_tabs(&self) -> &'static [ResultTab] {
        const CALIBRATION_LEGACY: [ResultTab; 4] = [
            ResultTab::Overview,
            ResultTab::Plots,
            ResultTab::Stdout,
            ResultTab::Stderr,
        ];
        const BROWSER: [ResultTab; 7] = [
            ResultTab::Overview,
            ResultTab::Structure,
            ResultTab::Content,
            ResultTab::Inspector,
            ResultTab::History,
            ResultTab::Stdout,
            ResultTab::Stderr,
        ];
        const INSPECT: [ResultTab; 6] = [
            ResultTab::Overview,
            ResultTab::Data,
            ResultTab::Plots,
            ResultTab::History,
            ResultTab::Stdout,
            ResultTab::Stderr,
        ];
        const WORKFLOW: [ResultTab; 7] = [
            ResultTab::Overview,
            ResultTab::Data,
            ResultTab::Products,
            ResultTab::Diagnostics,
            ResultTab::History,
            ResultTab::Stdout,
            ResultTab::Stderr,
        ];
        if self.current_calibration_report().is_some() {
            return if self.app.shell_kind() == AppShellKind::Workflow {
                &WORKFLOW
            } else {
                &CALIBRATION_LEGACY
            };
        }
        match self.app.shell_kind() {
            AppShellKind::Inspect => &INSPECT,
            AppShellKind::Workflow => &WORKFLOW,
            AppShellKind::Browser => &BROWSER,
        }
    }

    fn sync_result_tab_visibility(&mut self) {
        if !self.visible_result_tabs().contains(&self.active_result_tab) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.active_result_tab = ResultTab::Overview;
        }
    }

    fn cancel_current(&mut self) {
        if let Some(session) = self.browser_session.take() {
            self.flush_session_last_on_close();
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
        let Some(mut running) = self.running.take() else {
            return;
        };
        let completed_successfully = success && !running.cancel_requested;
        let notebook_status = if running.cancel_requested {
            NotebookExecutionStatus::Cancelled
        } else if success {
            NotebookExecutionStatus::Succeeded
        } else {
            NotebookExecutionStatus::Failed
        };
        let notebook_warning = running.notebook_recording.as_mut().and_then(|recording| {
            recording.finalize(
                notebook_status,
                self.result.stdout.clone(),
                self.result.stderr.clone(),
                running.file_output_path.iter().map(PathBuf::from).collect(),
                Vec::new(),
            )
        });
        let automatic_save_warning = running
            .task_last_state
            .as_mut()
            .and_then(|state| state.after_completion(completed_successfully).warning);
        if let Some(warning) = &automatic_save_warning {
            self.result
                .stderr
                .push_str(&format!("Automatic parameter save warning: {warning}\n"));
        }
        if let Some(warning) = &notebook_warning {
            self.result
                .stderr
                .push_str(&format!("Notebook recording warning: {warning}\n"));
        }
        let save_warning_suffix = automatic_save_warning
            .as_ref()
            .map(|warning| format!(" Parameter save warning: {warning}"))
            .unwrap_or_default();
        self.result.exit_code = exit_code;
        self.result.file_output_path = running.file_output_path.clone();

        if running.cancel_requested {
            self.result.status_line = format!("Execution canceled.{save_warning_suffix}");
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
            self.record_history_entry(
                None,
                "Canceled".to_string(),
                StatusKind::Warning,
                vec!["The child process was canceled before completion.".to_string()],
            );
            return;
        }

        if success {
            self.result.status_line =
                format!("Execution completed successfully.{save_warning_suffix}");
            self.result.status_kind = StatusKind::Ok;
            if let Some(path) = running.file_output_path {
                self.result.structured = None;
                self.result.structured_error = None;
                self.result.file_output_path = Some(path);
                self.active_result_tab = ResultTab::Overview;
                return;
            }

            if matches!(
                running.renderer.as_deref(),
                Some("measurementset-summary-v1")
            ) {
                match serde_json::from_str::<MeasurementSetSummary>(&self.result.stdout) {
                    Ok(summary) => {
                        self.record_plot_snapshot(summary.clone());
                        self.result.structured =
                            Some(StructuredResult::MeasurementSetSummary(Box::new(summary)));
                        self.result.structured_error = None;
                        self.activate_result_tab(ResultTab::Overview);
                        self.record_history_entry(
                            None,
                            "Inspect Dataset".to_string(),
                            StatusKind::Ok,
                            vec!["MeasurementSet summary refreshed.".to_string()],
                        );
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
            } else if matches!(running.renderer.as_deref(), Some("calibration-report-v1")) {
                match serde_json::from_str::<ManagedCalibrationOutput>(&self.result.stdout) {
                    Ok(report) => {
                        let history_entry =
                            Self::calibration_history_entry(&report, self.next_history_sequence());
                        self.record_calibration_products(&report, history_entry.sequence);
                        self.history_entries.push(history_entry);
                        self.result.structured =
                            Some(StructuredResult::Calibration(Box::new(report)));
                        self.result.structured_error = None;
                        self.activate_result_tab(ResultTab::Overview);
                        if let Some(report) = self.current_calibration_report().cloned() {
                            self.apply_workflow_post_run_guidance(&report);
                        }
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
            } else if matches!(running.renderer.as_deref(), Some("imager-run-v1")) {
                match serde_json::from_str::<ManagedImagingOutput>(&self.result.stdout) {
                    Ok(report) => {
                        self.record_history_entry(
                            None,
                            "Imaging Run".to_string(),
                            StatusKind::Ok,
                            imaging_history_details(&report),
                        );
                        self.result.structured =
                            Some(StructuredResult::Imaging(Box::new(report.clone())));
                        self.result.structured_error = None;
                        self.activate_result_tab(ResultTab::Overview);
                        self.apply_imaging_post_run_guidance(&report);
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
                self.record_history_entry(
                    None,
                    "Command".to_string(),
                    StatusKind::Ok,
                    vec![self.result.status_line.clone()],
                );
            }
        } else {
            self.result.status_line = format!("Execution failed.{save_warning_suffix}");
            self.result.status_kind = StatusKind::Error;
            self.result.structured = None;
            self.result.structured_error = None;
            self.activate_result_tab(if !self.result.stderr.is_empty() {
                ResultTab::Stderr
            } else {
                ResultTab::Stdout
            });
            self.record_history_entry(
                None,
                "Command Failed".to_string(),
                StatusKind::Error,
                vec![self.result.status_line.clone()],
            );
        }
    }

    fn overview_lines(&self) -> Vec<String> {
        if let Some(report) = self.current_imaging_report() {
            return build_imaging_overview_lines(report);
        }
        if let Some(summary) = self.current_structured_summary() {
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

        if self.app.shell_kind() == AppShellKind::Workflow {
            return render_workflow_overview_lines(&WorkflowOverviewDisplay {
                dataset_path: self
                    .non_empty_field_text("vis"),
                recommended_stage: self
                    .current_calibration_report()
                    .and_then(|_| {
                        self.workflow_stage_states()
                            .into_iter()
                            .find(|state| state.recommended)
                            .and_then(|state| WorkflowStageId::from_key(state.id))
                            .map(|stage| stage.label().to_string())
                    }),
                selected_stage: if self.app.id == "imager" {
                    self.field_text("deconvolver")
                        .unwrap_or_else(|| "hogbom".to_string())
                } else {
                    self.current_workflow_stage().label().to_string()
                },
                active_products: self
                    .current_imaging_report()
                    .map(|report| report.artifacts.iter().filter(|artifact| artifact.exists).count())
                    .unwrap_or_else(|| {
                        self.workflow_products
                            .iter()
                            .filter(|product| product.status == WorkflowProductStatus::Active)
                            .count()
                    }),
                stale_products: 0,
                total_products: self
                    .current_imaging_report()
                    .map(|report| report.artifacts.len())
                    .unwrap_or_else(|| self.workflow_products.len()),
                guidance:
                    "Use Context for dataset/selection, Products for revisions, Stages for the ordered workflow, and Diagnostics for recommended plots/stats."
                        .to_string(),
                latest_run_lines: self
                    .current_calibration_report()
                    .map(build_calibration_overview_lines)
                    .or_else(|| self.current_imaging_report().map(build_imaging_overview_lines))
                    .unwrap_or_default(),
            });
        }

        if self.app.shell_kind() == AppShellKind::Inspect {
            return render_inspect_overview_lines(&InspectOverviewDisplay {
                dataset_path: self.non_empty_field_text("vis"),
                current_view: self.selected_summary_view.label().to_string(),
                current_plot: Some(self.selected_plot_label()),
                tab_labels: self
                    .visible_result_tabs()
                    .iter()
                    .map(|tab| tab.label().to_string())
                    .collect(),
                guidance:
                    "Use Context for dataset selection, Views for summary tables, and Plots for the current interactive preset."
                        .to_string(),
            });
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
            return render_browser_overview_lines(&BrowserOverviewDisplay {
                root_path: session.root_path.clone(),
                active_view: session.active_tab().label().to_string(),
                shell_tab: session.active_tab().shell_result_tab().label().to_string(),
                status: session.status_line().to_string(),
                browser_kind: self.app.browser_kind().map(|kind| match kind {
                    BrowserAppKind::Table => "table".to_string(),
                    BrowserAppKind::Image => "image".to_string(),
                }),
                guidance:
                    "Use Structure and Content to inspect the selected browser view, and Inspector for the live value trail."
                        .to_string(),
            });
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

fn build_calibration_overview_lines(report: &ManagedCalibrationOutput) -> Vec<String> {
    match report {
        ManagedCalibrationOutput::Apply(report) => vec![
            "Calibration Apply".to_string(),
            format!(
                "MS: {}",
                report
                    .plan
                    .measurement_set_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string())
            ),
            format!(
                "Rows selected: {}   Rows updated: {}   Tables: {}",
                report.plan.selected_row_count,
                report.updated_row_count,
                report.plan.calibration_tables.len()
            ),
            format!(
                "Created CORRECTED_DATA: {}   Wrote MS: {}",
                yes_no(report.created_corrected_data_column),
                yes_no(report.wrote_measurement_set)
            ),
            format!(
                "Flagged rows: {}   Flagged samples: {}",
                report.flagged_row_count, report.flagged_sample_count
            ),
            format!(
                "Timing: total={}  planning={}  save={}",
                format_duration_ns(report.timings.total_ns),
                format_duration_ns(report.timings.planning_ns),
                format_duration_ns(report.timings.save_ns)
            ),
        ],
        ManagedCalibrationOutput::ExportCorrectedData(report) => vec![
            "Corrected Data Export".to_string(),
            format!("Input MS: {}", report.input_ms.display()),
            format!("Output MS: {}", report.output_ms.display()),
            format!(
                "Rows: {}   Copied: {} -> {}",
                report.row_count, report.source_column, report.output_column
            ),
        ],
        ManagedCalibrationOutput::ContinuumSubtract(report) => vec![
            "Continuum Subtraction".to_string(),
            format!("Input MS: {}", report.input_ms.display()),
            format!("Output MS: {}", report.output_ms.display()),
            format!(
                "Rows: {}   Fitted rows: {}   Skipped fits: {}",
                report.row_count, report.fitted_row_count, report.skipped_fit_count
            ),
            format!(
                "Source: {}   Output: {}   Fit order: {}",
                report.source_column, report.output_column, report.fit_order
            ),
        ],
        ManagedCalibrationOutput::Summary(summaries) => {
            let mut lines = vec![
                "Calibration Table Summary".to_string(),
                format!("Tables: {}", summaries.len()),
            ];
            for summary in summaries.iter().take(4) {
                lines.push(format!(
                    "{}  rows={}  family={:?}  subtype={}",
                    summary.path.display(),
                    summary.row_count,
                    summary.parameter_family,
                    summary.table_subtype
                ));
            }
            if summaries.len() > 4 {
                lines.push(format!("… {} more tables", summaries.len() - 4));
            }
            lines
        }
        ManagedCalibrationOutput::PlanApply(plan) => vec![
            "Calibration Apply Plan".to_string(),
            format!(
                "MS: {}",
                plan.measurement_set_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string())
            ),
            format!(
                "Rows selected: {}   Tables: {}   Requires CORRECTED_DATA: {}",
                plan.selected_row_count,
                plan.calibration_tables.len(),
                yes_no(plan.requires_corrected_data_column)
            ),
            format!(
                "Fields: {}   SPWs: {}   DDIDs: {}",
                plan.selected_field_ids.len(),
                plan.selected_data_spw_ids.len(),
                plan.selected_data_desc_ids.len()
            ),
        ],
        ManagedCalibrationOutput::Stats(report) => vec![
            "Calibration Stats".to_string(),
            format!("Table: {}", report.path.display()),
            format!(
                "Axis: {:?}   Column: {}   Rows: {}",
                report.axis,
                report.datacolumn.as_deref().unwrap_or("default"),
                report.row_count
            ),
            format!(
                "Points: {}   Flagged: {}   Total: {}",
                report.global.npts, report.global.flagged_npts, report.global.total_npts
            ),
            format!(
                "Mean: {:.6}   Median: {:.6}   RMS: {:.6}",
                report.global.mean, report.global.median, report.global.rms
            ),
        ],
        ManagedCalibrationOutput::SolveGain(report) => vec![
            "Gain Solve".to_string(),
            format!("Output: {}", report.output_table.display()),
            format!(
                "Type: {:?}   RefAnt: {}   Rows: {}",
                report.gain_type, report.refant_antenna_id, report.solution_row_count
            ),
            format!(
                "Fields: {}   SPWs: {}",
                report.field_ids.len(),
                report.spectral_window_ids.len()
            ),
        ],
        ManagedCalibrationOutput::SolveBandpass(report) => vec![
            "Bandpass Solve".to_string(),
            format!("Output: {}", report.output_table.display()),
            format!(
                "Subtype: {}   RefAnt: {}   Rows: {}",
                report.table_subtype, report.refant_antenna_id, report.solution_row_count
            ),
            format!(
                "SPWs: {}   Channels/row: {}",
                report.spectral_window_ids.len(),
                report.channel_count
            ),
        ],
        ManagedCalibrationOutput::FluxScale(report) => vec![
            "Fluxscale".to_string(),
            format!("Output: {}", report.output_table.display()),
            format!(
                "Transfer fields: {}   SPWs: {}",
                report.fields.len(),
                report.spw_ids.len()
            ),
        ],
        ManagedCalibrationOutput::Gencal(report) => vec![
            "Prior Calibration".to_string(),
            format!("Output: {}", report.output_table.display()),
            format!(
                "Type: {:?}   Subtype: {}   Rows: {}",
                report.caltype, report.table_subtype, report.row_count
            ),
            format!(
                "Antennas: {}   SPWs: {}",
                report.antenna_ids.len(),
                report.spectral_window_ids.len()
            ),
        ],
    }
}

fn build_imaging_overview_lines(report: &ManagedImagingOutput) -> Vec<String> {
    let mut lines = vec![
        "Imaging Run".to_string(),
        format!("MS: {}", report.request.measurement_set),
        format!("Image Prefix: {}", report.request.imagename),
        format!(
            "Mode: {}   Deconvolver: {}   Weighting: {}",
            report.request.spectral_mode, report.request.deconvolver, report.request.weighting
        ),
        format!(
            "W-term: {}   Dirty Only: {}   Gridded Samples: {}",
            report.request.w_term_mode,
            yes_no(report.request.dirty_only),
            report.run.gridded_samples
        ),
        format!(
            "Major Cycles: {}   Minor Iterations: {}   Stop: {}",
            report.run.major_cycles,
            report.run.minor_iterations,
            report.run.clean_stop_reason.as_deref().unwrap_or("n/a")
        ),
    ];
    if !report.run.channels.is_empty() {
        lines.push(format!(
            "Cube planes: {}   Taylor terms: {}",
            report.run.channels.len(),
            report.request.nterms
        ));
    }
    if !report.run.warnings.is_empty() {
        lines.push(String::new());
        lines.push("Warnings".to_string());
        lines.extend(
            report
                .run
                .warnings
                .iter()
                .take(4)
                .map(|warning| format!("  {warning}")),
        );
        if report.run.warnings.len() > 4 {
            lines.push(format!(
                "  … {} more warnings",
                report.run.warnings.len() - 4
            ));
        }
    }
    lines.push(String::new());
    lines.push(format!(
        "Frontend timing: {}",
        render_managed_stage_timing_summary(&report.run.frontend_timings)
    ));
    lines.push(format!(
        "Core timing: {}",
        render_managed_stage_timing_summary(&report.run.stage_timings)
    ));
    lines
}

fn imaging_history_details(report: &ManagedImagingOutput) -> Vec<String> {
    let mut details = vec![
        format!("prefix={}", report.request.imagename),
        format!("specmode={}", report.request.spectral_mode),
        format!("weighting={}", report.request.weighting),
        format!("deconvolver={}", report.request.deconvolver),
        format!("gridded_samples={}", report.run.gridded_samples),
        format!("major_cycles={}", report.run.major_cycles),
        format!("minor_iterations={}", report.run.minor_iterations),
    ];
    if let Some(stop) = &report.run.clean_stop_reason {
        details.push(format!("stop={stop}"));
    }
    if !report.run.channels.is_empty() {
        details.push(format!("channels={}", report.run.channels.len()));
    }
    details
}

fn render_managed_stage_timing_summary(timings: &ManagedImagingStageTimings) -> String {
    timings
        .values_ns
        .iter()
        .take(4)
        .map(|(label, ns)| format!("{label}={}", format_duration_ns(*ns)))
        .collect::<Vec<_>>()
        .join("  ")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn format_duration_ns(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{:.3}s", value as f64 / 1_000_000_000.0)
    } else if value >= 1_000_000 {
        format!("{:.3}ms", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.3}us", value as f64 / 1_000.0)
    } else {
        format!("{value}ns")
    }
}

fn spinner_frames(theme_mode: ThemeMode) -> &'static [&'static str] {
    match theme_mode {
        ThemeMode::DenseAnsi => DENSE_SPINNER_FRAMES,
        ThemeMode::RichPanel => RICH_SPINNER_FRAMES,
    }
}

fn browser_render_theme(theme_mode: ThemeMode) -> BrowserRenderTheme {
    match theme_mode {
        ThemeMode::DenseAnsi => BrowserRenderTheme::DenseAnsi,
        ThemeMode::RichPanel => BrowserRenderTheme::RichPanel,
    }
}

#[cfg(not(test))]
fn load_interactive_parameter_session(
    surface_id: &str,
    workspace: &Path,
) -> (Option<ParameterSession>, Option<String>) {
    let bundle = match builtin_surface_bundle(surface_id) {
        Ok(bundle) => bundle,
        Err(error) => {
            return (
                None,
                Some(format!(
                    "Parameter contract for {surface_id} is unavailable: {error}"
                )),
            );
        }
    };
    let defaults = || ParameterSession::defaults(bundle.clone());
    let store = ManagedStateStore::for_workspace(workspace);
    match store.read(surface_id, ManagedProfileKind::Last) {
        Ok(Some(source)) => match parse_profile(&source)
            .map_err(|error| error.to_string())
            .and_then(|profile| {
                ParameterSession::from_profile(bundle.clone(), BaseSource::Last, &profile)
                    .map_err(|error| error.to_string())
            }) {
            Ok(session) => (Some(session), None),
            Err(error) => (
                defaults().ok(),
                Some(format!(
                    "Last profile for {surface_id} is invalid; using Defaults. {error}"
                )),
            ),
        },
        Ok(None) => (defaults().ok(), None),
        Err(error) => (
            defaults().ok(),
            Some(format!(
                "Could not read Last profile for {surface_id}; using Defaults. {error}"
            )),
        ),
    }
}

#[cfg(test)]
fn load_interactive_parameter_session(
    surface_id: &str,
    _workspace: &Path,
) -> (Option<ParameterSession>, Option<String>) {
    match builtin_surface_bundle(surface_id)
        .and_then(|bundle| ParameterSession::defaults(bundle).map_err(|error| error.to_string()))
    {
        Ok(session) => (Some(session), None),
        Err(error) => (
            None,
            Some(format!(
                "Parameter contract for {surface_id} is unavailable: {error}"
            )),
        ),
    }
}

fn sync_form_fields_from_parameter_session(fields: &mut [FormField], session: &ParameterSession) {
    for field in fields {
        let Some(state) = session.states().get(&field.schema.id) else {
            continue;
        };
        field.schema.required = state.required;
        match (&mut field.value, state.value.as_ref()) {
            (FormValue::Toggle(current), Some(ParameterValue::Bool(value))) => *current = *value,
            (FormValue::Text(current), Some(value))
            | (FormValue::Choice { value: current, .. }, Some(value)) => {
                *current = parameter_value_text(value);
            }
            (FormValue::Text(current), None) | (FormValue::Choice { value: current, .. }, None) => {
                current.clear()
            }
            _ => {}
        }
    }
}

fn parameter_value_text(value: &ParameterValue) -> String {
    match value {
        ParameterValue::Bool(value) => value.to_string(),
        ParameterValue::Integer(value) => value.to_string(),
        ParameterValue::Float(value) => value.to_string(),
        ParameterValue::String(value) => value.clone(),
        ParameterValue::Array(values) => values
            .iter()
            .map(parameter_value_text)
            .collect::<Vec<_>>()
            .join(","),
        ParameterValue::Table(values) => serde_json::to_string(values).unwrap_or_default(),
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
                        value: schema.default.clone().unwrap_or_default(),
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
            (FormValue::Choice { value, .. }, _)
                if value.trim().is_empty() && self.schema.required =>
            {
                "<required>".to_string()
            }
            (FormValue::Choice { value, .. }, _) if value.trim().is_empty() => {
                "<empty>".to_string()
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

    fn cycle_choice(&mut self, forward: bool) {
        let FormValue::Choice { value, choices } = &mut self.value else {
            return;
        };
        if choices.is_empty() {
            return;
        }
        if value.trim().is_empty() {
            *value = if forward {
                choices[0].clone()
            } else {
                choices[choices.len() - 1].clone()
            };
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
                if value.trim().is_empty() && !self.schema.required {
                    *current = String::new();
                    return Ok(());
                }
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

fn build_sections(app: &RegistryApp, fields: &[FormField]) -> Vec<FormSection> {
    match app.shell_kind() {
        AppShellKind::Inspect => build_inspect_sections(&app.id, fields),
        AppShellKind::Workflow => build_workflow_sections(&app.id, fields),
        AppShellKind::Browser => build_browser_sections(app.browser_kind(), fields),
    }
}

fn build_inspect_sections(app_id: &str, fields: &[FormField]) -> Vec<FormSection> {
    if app_id != "msexplore" {
        return build_browser_sections(None, fields);
    }
    let context_ids = [
        "vis",
        "selectdata",
        "field",
        "spw",
        "antenna",
        "baseline",
        "scan",
        "observation",
        "array",
        "timerange",
        "uvrange",
        "correlation",
        "intent",
        "feed",
        "taql",
    ];
    let view_ids = [
        "preset",
        "page_spec",
        "x_axis",
        "y_axis",
        "y_axis2",
        "datacolumn",
        "color_by",
        "avgchannel",
        "avgtime",
        "avgscan",
        "avgfield",
        "avgbaseline",
        "avgantenna",
        "avgspw",
        "scalar",
        "freqframe",
        "restfreq",
        "veldef",
        "iteraxis",
        "gridrows",
        "gridcols",
        "xselfscale",
        "yselfscale",
        "xsharedaxis",
        "ysharedaxis",
        "title",
        "xlabel",
        "ylabel",
        "showlegend",
        "legendposition",
        "showmajorgrid",
        "showminorgrid",
        "headeritems",
        "max_points",
    ];
    let context = collect_field_items(fields, &context_ids);
    let views = SummaryDataView::ALL
        .into_iter()
        .map(StaticFormItem::SummaryView)
        .chain(collect_field_items(fields, &view_ids))
        .collect::<Vec<_>>();
    let used = context_ids
        .into_iter()
        .chain(view_ids)
        .collect::<std::collections::HashSet<_>>();
    let controls = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| !used.contains(field.schema.id.as_str()))
        .map(|(index, _)| StaticFormItem::Field(index))
        .collect::<Vec<_>>();
    vec![
        FormSection {
            name: "Context".to_string(),
            content: FormSectionContent::Items(context),
            collapsed: false,
        },
        FormSection {
            name: "Views".to_string(),
            content: FormSectionContent::Items(views),
            collapsed: false,
        },
        FormSection {
            name: "Controls".to_string(),
            content: FormSectionContent::Items(controls),
            collapsed: true,
        },
    ]
}

fn build_workflow_sections(app_id: &str, fields: &[FormField]) -> Vec<FormSection> {
    if app_id == "imager" {
        let context_ids = [
            "vis",
            "datacolumn",
            "field",
            "phasecenter_field",
            "phasecenter",
            "ddid",
            "spw",
            "channel_start",
            "channel_count",
            "polarization",
        ];
        let product_ids = ["imagename", "write_preview_pngs"];
        let stage_ids = [
            "specmode",
            "start",
            "width",
            "outframe",
            "veltype",
            "interpolation",
            "restfreq",
            "restoringbeam",
            "perchanweightdensity",
            "dirty_only",
            "niter",
            "threshold",
            "deconvolver",
            "nterms",
            "scales",
            "smallscalebias",
            "weighting",
            "robust",
            "uvtaper",
            "gridder",
            "wprojplanes",
            "usepointing",
            "write_pb",
            "pbcor",
            "pblimit",
            "wterm",
        ];
        let stage_parameters = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.schema.group == "Stage Parameters")
            .map(|(index, _)| StaticFormItem::Field(index))
            .collect::<Vec<_>>();
        return vec![
            FormSection {
                name: "Context".to_string(),
                content: FormSectionContent::Items(collect_field_items(fields, &context_ids)),
                collapsed: false,
            },
            FormSection {
                name: "Products".to_string(),
                content: FormSectionContent::Items(collect_field_items(fields, &product_ids)),
                collapsed: false,
            },
            FormSection {
                name: "Stages".to_string(),
                content: FormSectionContent::Items(collect_field_items(fields, &stage_ids)),
                collapsed: false,
            },
            FormSection {
                name: "Stage Parameters".to_string(),
                content: FormSectionContent::Items(stage_parameters),
                collapsed: false,
            },
        ];
    }
    if app_id != "calibrate" {
        return build_browser_sections(None, fields);
    }
    let context_ids = [
        "vis",
        "selectdata",
        "field",
        "refant",
        "reference_fields",
        "transfer_fields",
        "spw",
        "antenna",
        "scan",
        "observation",
        "array",
        "timerange",
        "msselect",
        "parang",
    ];
    let product_ids: [&str; 0] = [];
    let workflow_owned_ids = [
        "gaintables",
        "callib",
        "gainfield",
        "interp",
        "spwmap",
        "calwt",
    ];
    let stage_param_excluded = context_ids
        .into_iter()
        .chain(product_ids)
        .chain(workflow_owned_ids)
        .chain(["mode"])
        .collect::<std::collections::HashSet<_>>();
    let context = collect_field_items(fields, &context_ids);
    let products = collect_field_items(fields, &product_ids);
    let stages = WorkflowStageId::ALL
        .into_iter()
        .map(StaticFormItem::WorkflowStage)
        .collect::<Vec<_>>();
    let stage_parameters = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| !stage_param_excluded.contains(field.schema.id.as_str()))
        .map(|(index, _)| StaticFormItem::Field(index))
        .collect::<Vec<_>>();
    vec![
        FormSection {
            name: "Context".to_string(),
            content: FormSectionContent::Items(context),
            collapsed: false,
        },
        FormSection {
            name: "Products".to_string(),
            content: FormSectionContent::Items(products),
            collapsed: false,
        },
        FormSection {
            name: "Stages".to_string(),
            content: FormSectionContent::Items(stages),
            collapsed: false,
        },
        FormSection {
            name: "Stage Parameters".to_string(),
            content: FormSectionContent::Items(stage_parameters),
            collapsed: false,
        },
    ]
}

fn build_browser_sections(kind: Option<BrowserAppKind>, fields: &[FormField]) -> Vec<FormSection> {
    let context = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.schema.value_kind == UiValueKind::Path)
        .map(|(index, _)| StaticFormItem::Field(index))
        .collect::<Vec<_>>();
    let views = kind
        .map(|kind| match kind {
            BrowserAppKind::Table => BrowserTab::TABLE_ALL
                .into_iter()
                .map(StaticFormItem::BrowserView)
                .collect::<Vec<_>>(),
            BrowserAppKind::Image => BrowserTab::IMAGE_ALL
                .into_iter()
                .map(StaticFormItem::BrowserView)
                .collect::<Vec<_>>(),
        })
        .unwrap_or_default();
    let tools = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.schema.value_kind != UiValueKind::Path)
        .map(|(index, _)| StaticFormItem::Field(index))
        .collect::<Vec<_>>();
    vec![
        FormSection {
            name: "Context".to_string(),
            content: FormSectionContent::Items(context),
            collapsed: false,
        },
        FormSection {
            name: "Views".to_string(),
            content: FormSectionContent::Items(views),
            collapsed: false,
        },
        FormSection {
            name: "Tools".to_string(),
            content: FormSectionContent::Items(tools),
            collapsed: true,
        },
    ]
}

fn collect_field_items(fields: &[FormField], ids: &[&str]) -> Vec<StaticFormItem> {
    ids.iter()
        .filter_map(|id| {
            fields
                .iter()
                .position(|field| field.schema.id == *id)
                .map(StaticFormItem::Field)
        })
        .collect()
}

fn initial_form_selection(
    sections: &[FormSection],
    fields: &[FormField],
    show_advanced: bool,
) -> FormSelection {
    for (section_index, section) in sections.iter().enumerate() {
        let visible_items = match &section.content {
            FormSectionContent::Items(items) => items
                .iter()
                .copied()
                .filter(|item| match item {
                    StaticFormItem::Field(index) => {
                        show_advanced || !fields[*index].schema.advanced
                    }
                    StaticFormItem::SummaryView(_)
                    | StaticFormItem::BrowserView(_)
                    | StaticFormItem::WorkflowStage(_) => true,
                })
                .collect::<Vec<_>>(),
        };
        if visible_items.is_empty() {
            continue;
        }
        if section.collapsed {
            return FormSelection::Section(section_index);
        }
        return match visible_items[0] {
            StaticFormItem::Field(index) => FormSelection::Field(index),
            StaticFormItem::SummaryView(view) => FormSelection::SummaryView(view),
            StaticFormItem::BrowserView(view) => FormSelection::BrowserView(view),
            StaticFormItem::WorkflowStage(stage) => FormSelection::WorkflowStage(stage),
        };
    }
    FormSelection::Section(0)
}

fn default_summary_view_for_app(app_id: &str) -> SummaryDataView {
    match app_id {
        "calibrate" | "imager" => SummaryDataView::Fields,
        _ => SummaryDataView::Observations,
    }
}

fn popup_index_at(list_area: Rect, column: u16, row: u16, item_count: usize) -> Option<usize> {
    if !rect_contains(list_area, column, row) {
        return None;
    }
    let index = row.saturating_sub(list_area.y) as usize;
    (index < item_count).then_some(index)
}

fn dynamic_field_picker_entries(app: &AppState, field_id: &str) -> Option<Vec<ChoicePickerEntry>> {
    match field_id {
        "field" => {
            let summary = app.current_structured_summary()?;
            Some(
                summary
                    .fields
                    .iter()
                    .map(|field| ChoicePickerEntry {
                        value: field.field_id.to_string(),
                        label: format!("{}  {}", field.field_id, field.name),
                    })
                    .collect(),
            )
        }
        "spw" => {
            let summary = app.current_structured_summary()?;
            Some(
                summary
                    .spectral_windows
                    .iter()
                    .map(|spw| ChoicePickerEntry {
                        value: spw.spectral_window_id.to_string(),
                        label: format!(
                            "{}  {}  {:.6} GHz",
                            spw.spectral_window_id,
                            spw.name,
                            spw.center_frequency_hz / 1.0e9
                        ),
                    })
                    .collect(),
            )
        }
        "antenna" => {
            let summary = app.current_structured_summary()?;
            Some(
                summary
                    .antennas
                    .iter()
                    .map(|antenna| ChoicePickerEntry {
                        value: antenna.name.clone(),
                        label: format!(
                            "{}  {} ({})",
                            antenna.antenna_id, antenna.name, antenna.station
                        ),
                    })
                    .collect(),
            )
        }
        "refant" => {
            let summary = app.current_structured_summary()?;
            Some(
                summary
                    .antennas
                    .iter()
                    .map(|antenna| ChoicePickerEntry {
                        value: antenna.name.clone(),
                        label: format!("{} ({})", antenna.name, antenna.station),
                    })
                    .collect(),
            )
        }
        "scan" => {
            let summary = app.current_structured_summary()?;
            let mut seen = HashSet::new();
            Some(
                summary
                    .scans
                    .iter()
                    .filter(|scan| seen.insert(scan.scan_number))
                    .map(|scan| ChoicePickerEntry {
                        value: scan.scan_number.to_string(),
                        label: format!(
                            "{}  field={}  {}",
                            scan.scan_number, scan.field_id, scan.field_name
                        ),
                    })
                    .collect(),
            )
        }
        "observation" => {
            let summary = app.current_structured_summary()?;
            Some(
                summary
                    .observations
                    .iter()
                    .map(|observation| ChoicePickerEntry {
                        value: observation.observation_id.to_string(),
                        label: format!(
                            "{}  {}  {}",
                            observation.observation_id,
                            observation.telescope_name,
                            observation.project
                        ),
                    })
                    .collect(),
            )
        }
        "array" => {
            let summary = app.current_structured_summary()?;
            let arrays = summary
                .scans
                .iter()
                .map(|scan| scan.array_id)
                .collect::<BTreeSet<_>>();
            Some(
                arrays
                    .into_iter()
                    .map(|array_id| ChoicePickerEntry {
                        value: array_id.to_string(),
                        label: format!("Array {array_id}"),
                    })
                    .collect(),
            )
        }
        "intent" => {
            let summary = app.current_structured_summary()?;
            let intents = summary
                .scans
                .iter()
                .flat_map(|scan| scan.scan_intents.iter().cloned())
                .collect::<BTreeSet<_>>();
            Some(
                intents
                    .into_iter()
                    .map(|intent| ChoicePickerEntry {
                        value: intent.clone(),
                        label: intent,
                    })
                    .collect(),
            )
        }
        "reference_fields" | "transfer_fields" => {
            let summary = app.current_structured_summary()?;
            Some(
                summary
                    .fields
                    .iter()
                    .map(|field| ChoicePickerEntry {
                        value: field.name.clone(),
                        label: format!("{}  {}", field.field_id, field.name),
                    })
                    .collect(),
            )
        }
        "gaintables" | "summary_paths" | "table_path" | "fluxscale_input" => {
            let entries = app
                .workflow_products
                .iter()
                .filter(|product| product.stage != WorkflowStageId::Apply)
                .map(|product| ChoicePickerEntry {
                    value: product.path.display().to_string(),
                    label: format!(
                        "r{}  {}  [{} | {}]",
                        product.revision,
                        product.path.display(),
                        product.family,
                        product.status.label()
                    ),
                })
                .collect::<Vec<_>>();
            (!entries.is_empty()).then_some(entries)
        }
        _ => None,
    }
}

fn raw_lines(label: &str, text: &str) -> Vec<String> {
    if text.trim().is_empty() {
        vec![format!("No {label} output.")]
    } else {
        text.lines().map(ToString::to_string).collect()
    }
}

fn build_observations_table(summary: &MeasurementSetSummary) -> TableView {
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

fn build_scans_table(summary: &MeasurementSetSummary, listunfl: bool) -> TableView {
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

fn build_fields_table(summary: &MeasurementSetSummary, listunfl: bool) -> TableView {
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

fn build_spws_table(summary: &MeasurementSetSummary) -> TableView {
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

fn build_sources_table(summary: &MeasurementSetSummary) -> TableView {
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

fn build_antennas_table(summary: &MeasurementSetSummary) -> TableView {
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

fn build_compact_antenna_lines(summary: &MeasurementSetSummary) -> Vec<String> {
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
    text.chars().skip(offset).take(width).collect()
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

    VisibleTextLine {
        text: chars[offset..offset + width].iter().collect(),
        roles: line.roles[offset..offset + width].to_vec(),
    }
}

fn image_browser_content_width(snapshot: &ImageBrowserSnapshot) -> usize {
    snapshot
        .content_lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0)
}

fn sync_image_parameter_fields(fields: &mut [FormField], parameters: &ImageBrowserParameters) {
    for (id, value) in [
        ("blc", parameters.blc.as_str()),
        ("trc", parameters.trc.as_str()),
        ("inc", parameters.inc.as_str()),
        ("stretch", parameters.stretch.as_str()),
        ("autoscale", parameters.autoscale.as_str()),
        ("clip_low", parameters.clip_low.as_str()),
        ("clip_high", parameters.clip_high.as_str()),
    ] {
        if let Some(field) = fields.iter_mut().find(|field| field.schema.id == id) {
            let _ = field.apply_text_value(value.to_string());
        }
    }
}

fn parameter_reference_is_none(value: &str) -> bool {
    value.trim().is_empty() || value.trim().eq_ignore_ascii_case("none")
}

fn browser_request_may_change_durable_parameters(
    command: &BrowserRequest,
    accepted_axis_selection: Option<usize>,
) -> bool {
    accepted_axis_selection.is_some()
        || matches!(
            command,
            BrowserRequest::CycleView { .. }
                | BrowserRequest::SetImageViewParameters { .. }
                | BrowserRequest::SetImageProfileAxis { .. }
                | BrowserRequest::SetImageSelectionReferences { .. }
                | BrowserRequest::ClearImageRegion
                | BrowserRequest::SaveImageRegionDefinition
                | BrowserRequest::LoadNextImageRegionDefinition
                | BrowserRequest::LoadImageRegionDefinition { .. }
                | BrowserRequest::RenameImageRegionDefinition { .. }
                | BrowserRequest::DeleteImageRegionDefinition { .. }
                | BrowserRequest::SetImageDefaultMask { .. }
                | BrowserRequest::UnsetImageDefaultMask
                | BrowserRequest::DeleteImageMask { .. }
                | BrowserRequest::WriteImageRegionMask
        )
}

fn parameter_integer(value: Option<&ParameterValue>, parameter: &str) -> Result<i64, String> {
    match value {
        None => Err(format!(
            "resolved integer parameter {parameter:?} is unavailable"
        )),
        Some(ParameterValue::Integer(value)) => Ok(*value),
        Some(ParameterValue::String(value)) => value
            .trim()
            .parse::<i64>()
            .map_err(|_| format!("expected an integer parameter, got {value:?}")),
        Some(value) => Err(format!("expected an integer parameter, got {value:?}")),
    }
}

fn parameter_f64(value: Option<&ParameterValue>, parameter: &str) -> Result<f64, String> {
    match value {
        None => Err(format!(
            "resolved numeric parameter {parameter:?} is unavailable"
        )),
        Some(ParameterValue::Integer(value)) => Ok(*value as f64),
        Some(ParameterValue::Float(value)) => Ok(*value),
        Some(ParameterValue::String(value)) => value
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("expected a numeric parameter, got {value:?}")),
        Some(value) => Err(format!("expected a numeric parameter, got {value:?}")),
    }
}

fn parameter_bool(value: Option<&ParameterValue>, parameter: &str) -> Result<bool, String> {
    match value {
        None => Err(format!(
            "resolved boolean parameter {parameter:?} is unavailable"
        )),
        Some(ParameterValue::Bool(value)) => Ok(*value),
        Some(ParameterValue::String(value)) if value.trim().eq_ignore_ascii_case("true") => {
            Ok(true)
        }
        Some(ParameterValue::String(value)) if value.trim().eq_ignore_ascii_case("false") => {
            Ok(false)
        }
        Some(value) => Err(format!("expected a boolean parameter, got {value:?}")),
    }
}

fn parse_table_session_view(value: &str) -> Result<TableBrowserView, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "summary" => Ok(TableBrowserView::Overview),
        "columns" => Ok(TableBrowserView::Columns),
        "keywords" => Ok(TableBrowserView::Keywords),
        "rows" => Ok(TableBrowserView::Cells),
        _ => Err(format!(
            "unsupported tablebrowser view {value:?}; expected summary, columns, keywords, or rows"
        )),
    }
}

fn table_session_view_parameter(view: TableBrowserView) -> Option<&'static str> {
    match view {
        TableBrowserView::Overview => Some("summary"),
        TableBrowserView::Columns => Some("columns"),
        TableBrowserView::Keywords => Some("keywords"),
        TableBrowserView::Cells => Some("rows"),
        TableBrowserView::Subtables => None,
    }
}

fn parse_image_session_view(value: &str) -> Result<ImageBrowserView, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "plane" => Ok(ImageBrowserView::Plane),
        "spectrum" => Ok(ImageBrowserView::Spectrum),
        "metadata" => Ok(ImageBrowserView::Metadata),
        "coordinates" => Ok(ImageBrowserView::Coordinates),
        _ => Err(format!(
            "unsupported imexplore view {value:?}; expected plane, spectrum, metadata, or coordinates"
        )),
    }
}

fn image_session_view_parameter(view: ImageBrowserView) -> &'static str {
    match view {
        ImageBrowserView::Plane => "plane",
        ImageBrowserView::Spectrum => "spectrum",
        ImageBrowserView::Metadata => "metadata",
        ImageBrowserView::Coordinates => "coordinates",
    }
}

fn parse_image_colormap(value: &str) -> Result<ImagePlaneColormap, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "gray" | "grayscale" => Ok(ImagePlaneColormap::Grayscale),
        "viridis" => Ok(ImagePlaneColormap::Viridis),
        "inferno" => Ok(ImagePlaneColormap::Inferno),
        _ => Err(format!(
            "unsupported imexplore colormap {value:?}; expected gray, viridis, or inferno"
        )),
    }
}

fn parse_image_content_mode(value: &str) -> Result<ImagePlaneContentMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "raster" => Ok(ImagePlaneContentMode::Raster),
        "spreadsheet" => Ok(ImagePlaneContentMode::Spreadsheet),
        _ => Err(format!(
            "unsupported imexplore contentmode {value:?}; expected raster or spreadsheet"
        )),
    }
}

const fn image_plane_mode(mode: ImagePlaneContentMode) -> ImagePlaneMode {
    match mode {
        ImagePlaneContentMode::Raster => ImagePlaneMode::Raster,
        ImagePlaneContentMode::Spreadsheet => ImagePlaneMode::Spreadsheet,
    }
}

fn image_colormap_parameter(colormap: ImagePlaneColormap) -> &'static str {
    match colormap {
        ImagePlaneColormap::Grayscale => "gray",
        ImagePlaneColormap::Viridis => "viridis",
        ImagePlaneColormap::Inferno => "inferno",
    }
}

fn parse_table_bookmark(value: &str) -> Result<Option<BrowserBookmark>, String> {
    let value = value.trim();
    if parameter_reference_is_none(value) {
        return Ok(None);
    }
    let bookmark = if let Some(rest) = value.strip_prefix("cell:") {
        let mut parts = rest.splitn(2, ':');
        let row = parts.next().and_then(|row| row.parse::<usize>().ok());
        let column = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        match (row, column) {
            (Some(row), Some(column)) => Some(BrowserBookmark::Cell {
                row,
                column: column.to_string(),
            }),
            _ => None,
        }
    } else if let Some(rest) = value.strip_prefix("table-keyword:") {
        let path = parse_bookmark_path(rest);
        (!path.is_empty()).then_some(BrowserBookmark::TableKeyword { path })
    } else if let Some(rest) = value.strip_prefix("column-keyword:") {
        let mut parts = rest.splitn(2, ':');
        let column = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let path = parts.next().map(parse_bookmark_path).unwrap_or_default();
        column
            .filter(|_| !path.is_empty())
            .map(|column| BrowserBookmark::ColumnKeyword {
                column: column.to_string(),
                path,
            })
    } else if let Some(rest) = value.strip_prefix("subtable:") {
        let name = rest.trim();
        (!name.is_empty()).then(|| BrowserBookmark::Subtable {
            name: name.to_string(),
        })
    } else {
        None
    };
    bookmark.map(Some).ok_or_else(|| {
        format!(
            "invalid tablebrowser bookmark {value:?}; expected cell:ROW:COLUMN, table-keyword:PATH, column-keyword:COLUMN:PATH, or subtable:NAME"
        )
    })
}

fn parse_bookmark_path(value: &str) -> Vec<String> {
    value
        .split(['.', '/'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_table_content_mode(value: &str) -> Result<BrowserContentMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(BrowserContentMode::Auto),
        "compact" => Ok(BrowserContentMode::Compact),
        "detailed" => Ok(BrowserContentMode::Detailed),
        _ => Err(format!(
            "unsupported tablebrowser contentmode {value:?}; expected auto, compact, or detailed"
        )),
    }
}

fn looks_like_image_expression(value: &str) -> bool {
    value
        .chars()
        .any(|character| "[](){}&|=<>!*+;".contains(character))
}

fn parse_image_region_reference(value: &str) -> Result<ProtocolImageRegionReference, String> {
    let value = value.trim();
    if parameter_reference_is_none(value) {
        return Ok(ProtocolImageRegionReference::None);
    }
    if let Some(path) = value.strip_prefix("file:") {
        let path = path.trim();
        return if path.is_empty() {
            Err("imexplore region file reference cannot be empty".to_string())
        } else {
            Ok(ProtocolImageRegionReference::File {
                path: path.to_string(),
            })
        };
    }
    if let Some(name) = value.strip_prefix("definition:") {
        let name = name.trim();
        return if name.is_empty() {
            Err("imexplore saved region definition name cannot be empty".to_string())
        } else {
            Ok(ProtocolImageRegionReference::Definition {
                name: name.to_string(),
            })
        };
    }
    if looks_like_image_expression(value) {
        return Ok(ProtocolImageRegionReference::Expression {
            expression: value.to_string(),
        });
    }
    if value.contains('/')
        || value.contains('\\')
        || value.ends_with(".crtf")
        || value.ends_with(".reg")
        || value.ends_with(".region")
    {
        Ok(ProtocolImageRegionReference::File {
            path: value.to_string(),
        })
    } else {
        Ok(ProtocolImageRegionReference::Definition {
            name: value.to_string(),
        })
    }
}

fn parse_image_mask_reference(value: &str) -> Result<ProtocolImageMaskReference, String> {
    let value = value.trim();
    if parameter_reference_is_none(value) {
        return Ok(ProtocolImageMaskReference::None);
    }
    if let Some(name) = value.strip_prefix("name:") {
        let name = name.trim();
        return if name.is_empty() {
            Err("imexplore mask name cannot be empty".to_string())
        } else {
            Ok(ProtocolImageMaskReference::Name {
                name: name.to_string(),
            })
        };
    }
    if value.is_empty() {
        return Err("imexplore mask name cannot be empty".to_string());
    }
    if looks_like_image_expression(value) {
        return Ok(ProtocolImageMaskReference::Expression {
            expression: value.to_string(),
        });
    }
    Ok(ProtocolImageMaskReference::Name {
        name: value.to_string(),
    })
}

fn image_region_reference_parameter(reference: &ProtocolImageRegionReference) -> String {
    match reference {
        ProtocolImageRegionReference::None => "none".to_string(),
        ProtocolImageRegionReference::Definition { name } => format!("definition:{name}"),
        ProtocolImageRegionReference::File { path } => format!("file:{path}"),
        ProtocolImageRegionReference::Expression { expression } => expression.clone(),
    }
}

fn image_mask_reference_parameter(reference: &ProtocolImageMaskReference) -> String {
    match reference {
        ProtocolImageMaskReference::None => "none".to_string(),
        ProtocolImageMaskReference::Name { name } => name.clone(),
        ProtocolImageMaskReference::Expression { expression } => expression.clone(),
    }
}

fn resolve_image_axis_selector(
    snapshot: &ImageBrowserSnapshot,
    selector: &str,
    parameter: &str,
) -> Result<Option<(usize, usize)>, String> {
    let selector = selector.trim();
    if selector.is_empty() || selector.eq_ignore_ascii_case("auto") {
        return Ok(None);
    }
    let selected = selector
        .parse::<usize>()
        .ok()
        .and_then(|axis| {
            snapshot
                .non_display_axes
                .iter()
                .enumerate()
                .find(|(_, state)| state.axis == axis)
        })
        .or_else(|| {
            snapshot
                .non_display_axes
                .iter()
                .enumerate()
                .find(|(_, state)| state.label.eq_ignore_ascii_case(selector))
        });
    selected
        .map(|(index, state)| Some((index, state.axis)))
        .ok_or_else(|| {
            let available = snapshot
                .non_display_axes
                .iter()
                .map(|state| format!("{} ({})", state.label, state.axis))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "imexplore {parameter}={selector:?} does not identify a non-display axis; available axes: {}",
                if available.is_empty() {
                    "none"
                } else {
                    &available
                }
            )
        })
}

fn cycle_image_startup_view(
    client: &ImageBrowserClient,
    mut snapshot: ImageBrowserSnapshot,
    target: ImageBrowserView,
) -> Result<ImageBrowserSnapshot, String> {
    for _ in 0..4 {
        if snapshot.active_view == target {
            return Ok(snapshot);
        }
        snapshot = client
            .request_startup(ImageBrowserCommand::CycleView { forward: true })
            .map_err(|error| error.message().to_string())?;
    }
    Err(format!(
        "imexplore backend did not expose requested startup view {}",
        target.label()
    ))
}

fn apply_image_startup_config(
    client: &ImageBrowserClient,
    snapshot: ImageBrowserSnapshot,
    config: &ImageSessionStartupConfig,
) -> Result<(ImageBrowserSnapshot, usize), String> {
    let mut snapshot = if config.enforce_view {
        cycle_image_startup_view(client, snapshot, config.view)?
    } else {
        snapshot
    };

    if config.content_mode_explicit {
        snapshot = client
            .request_startup(ImageBrowserCommand::SetPlaneContentMode {
                mode: config.content_mode,
            })
            .map_err(|error| error.message().to_string())?;
    }

    if let Some((_, axis)) =
        resolve_image_axis_selector(&snapshot, &config.profile_axis, "profileaxis")?
    {
        snapshot = client
            .request_startup(ImageBrowserCommand::SetSelectedNonDisplayAxis { axis })
            .map_err(|error| error.message().to_string())?;
    }

    if config.region_explicit || config.mask_explicit {
        snapshot = client
            .request_startup(ImageBrowserCommand::SetSelectionReferences {
                region: config.region_explicit.then(|| config.region.clone()),
                mask: config.mask_explicit.then(|| config.mask.clone()),
            })
            .map_err(|error| error.message().to_string())?;
    }

    let selected_movie_axis =
        resolve_image_axis_selector(&snapshot, &config.movie_axis, "movieaxis")?
            .map(|(index, _)| index)
            .unwrap_or(0);
    Ok((snapshot, selected_movie_axis))
}

fn movie_stop_input_ignored_for_debug() -> bool {
    std::env::var_os("CASARS_IMEXPLORE_IGNORE_MOVIE_STOP_INPUT").is_some()
}

fn movie_input_fully_ignored_for_debug() -> bool {
    std::env::var_os("CASARS_IMEXPLORE_IGNORE_ALL_INPUT").is_some()
}

fn kitty_protocol_response_char(key_event: KeyEvent) -> Option<char> {
    match key_event.code {
        KeyCode::Char(ch) => Some(ch),
        _ => None,
    }
}

fn kitty_protocol_response_image_not_found(response: &str) -> Option<(u32, Option<u32>)> {
    let response = response.strip_suffix('\\').unwrap_or(response);
    let payload = response.strip_prefix("_G")?;
    let (fields, status) = payload.split_once(';')?;
    if !status.starts_with("ENOENT:") {
        return None;
    }
    let mut image_id = None;
    let mut placement_id = None;
    for field in fields.split(',') {
        if let Some(raw) = field.strip_prefix("i=") {
            image_id = raw.parse::<u32>().ok();
        } else if let Some(raw) = field.strip_prefix("p=") {
            placement_id = raw.parse::<u32>().ok();
        }
    }
    image_id.map(|id| (id, placement_id))
}

fn image_plane_column_count(snapshot: &ImageBrowserSnapshot) -> Option<usize> {
    let header = snapshot.content_lines.first()?;
    let pipe_index = header.find('|')?;
    let right_width = header.get(pipe_index + 1..)?.chars().count();
    let stride = IMAGE_PLANE_CELL_WIDTH + 1;
    Some(right_width / stride)
}

fn image_plane_header_pixel(snapshot: &ImageBrowserSnapshot, column: usize) -> Option<usize> {
    let header = snapshot.content_lines.first()?;
    let pipe_index = header.find('|')?;
    let stride = IMAGE_PLANE_CELL_WIDTH + 1;
    let start = pipe_index
        .checked_add(2)?
        .checked_add(column.checked_mul(stride)?)?;
    let text = slice_chars(header, start, start + IMAGE_PLANE_CELL_WIDTH);
    text.trim().parse::<usize>().ok()
}

fn image_plane_selected_span(snapshot: &ImageBrowserSnapshot) -> Option<(usize, usize)> {
    for line in snapshot.content_lines.iter().skip(1) {
        let Some(start) = line.find('[') else {
            continue;
        };
        let Some(end) = line[start..].find(']').map(|offset| start + offset + 1) else {
            continue;
        };
        return Some((start, end));
    }

    None
}

fn image_plane_sample_cursor(snapshot: &ImageBrowserSnapshot) -> Option<(usize, usize)> {
    let cursor = snapshot.plane_cursor.as_ref()?;
    Some((cursor.sampled_x, cursor.sampled_y))
}

fn image_plane_sampled_shape(snapshot: &ImageBrowserSnapshot) -> Option<(usize, usize)> {
    let display_x = snapshot.display_axes.first()?;
    let display_y = snapshot.display_axes.get(1)?;
    if display_x.sampled_len == 0 || display_y.sampled_len == 0 {
        return None;
    }
    Some((display_x.sampled_len, display_y.sampled_len))
}

fn scaled_movie_render_dimension(dimension: u32, render_scale: f32) -> u32 {
    let dimension = dimension.max(1);
    if render_scale >= 0.999 {
        return dimension;
    }
    let scaled = ((dimension as f32) * render_scale).round().max(1.0) as u32;
    if dimension <= 64 {
        scaled.min(dimension).max(1)
    } else {
        scaled.max(64).min(dimension)
    }
}

fn image_movie_plane_render_scale_for_state(state: &ImageBrowserSessionState) -> f32 {
    if !state.movie.playing {
        return 1.0;
    }
    let Some(axis_len) = state
        .selected_non_display_axis_state()
        .map(|axis| axis.length)
        .filter(|length| *length > 1)
    else {
        return 1.0;
    };
    let pixel_width = u64::from(state.viewport.plane_pixel_width.max(1));
    let pixel_height = u64::from(state.viewport.plane_pixel_height.max(1));
    let bytes_per_frame = pixel_width.saturating_mul(pixel_height).saturating_mul(4);
    let total_bytes = bytes_per_frame.saturating_mul(axis_len as u64);
    let target_bytes = configured_image_movie_target_resident_bytes();
    if total_bytes <= target_bytes || target_bytes == 0 {
        return 1.0;
    }
    ((target_bytes as f64 / total_bytes as f64).sqrt() as f32)
        .clamp(IMAGE_MOVIE_MIN_RENDER_SCALE, 1.0)
}

fn configured_image_movie_target_resident_bytes() -> u64 {
    std::env::var("CASARS_IMEXPLORE_MOVIE_TARGET_RESIDENT_MB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|mb| mb.saturating_mul(1024 * 1024))
        .unwrap_or(IMAGE_MOVIE_TARGET_RESIDENT_BYTES)
}

fn clamp_render_dimensions(
    pixel_width: u32,
    pixel_height: u32,
    max_pixel_size: Option<(u32, u32)>,
) -> (u32, u32) {
    let Some((max_width, max_height)) = max_pixel_size else {
        return (pixel_width.max(1), pixel_height.max(1));
    };
    let pixel_width = pixel_width.max(1);
    let pixel_height = pixel_height.max(1);
    if pixel_width <= max_width && pixel_height <= max_height {
        return (pixel_width, pixel_height);
    }
    let width_scale = (max_width.max(1) as f32) / (pixel_width as f32);
    let height_scale = (max_height.max(1) as f32) / (pixel_height as f32);
    let scale = width_scale.min(height_scale).min(1.0);
    (
        ((pixel_width as f32) * scale).round().max(1.0) as u32,
        ((pixel_height as f32) * scale).round().max(1.0) as u32,
    )
}

fn combine_render_pixel_caps(
    primary: Option<(u32, u32)>,
    secondary: Option<(u32, u32)>,
) -> Option<(u32, u32)> {
    match (primary, secondary) {
        (Some((left_width, left_height)), Some((right_width, right_height))) => Some((
            left_width.min(right_width).max(1),
            left_height.min(right_height).max(1),
        )),
        (Some(cap), None) | (None, Some(cap)) => Some((cap.0.max(1), cap.1.max(1))),
        (None, None) => None,
    }
}

fn adaptive_image_plane_render_pixel_cap(
    area: Rect,
    font_size: (u16, u16),
    snapshot: &ImageBrowserSnapshot,
) -> Option<(u32, u32)> {
    if area.is_empty() {
        return None;
    }
    let full_width = u32::from(area.width.max(1)) * u32::from(font_size.0.max(1));
    let full_height = u32::from(area.height.max(1)) * u32::from(font_size.1.max(1));
    let layout = image_plane_layout(
        full_width,
        full_height,
        image_plane_display_aspect_ratio(snapshot),
        snapshot.display_axes.len() >= 2,
    );
    if layout.image.width == 0 || layout.image.height == 0 {
        return None;
    }
    let (sampled_width, sampled_height) = image_plane_sampled_shape(snapshot).or_else(|| {
        snapshot
            .plane
            .as_ref()
            .map(|raster| (raster.width.max(1), raster.height.max(1)))
    })?;
    let target_image_width = plane_target_dimension(
        (sampled_width as u32).saturating_mul(IMAGE_PLANE_TARGET_PIXELS_PER_SAMPLE),
        IMAGE_PLANE_MIN_TARGET_IMAGE_PIXELS,
        layout.image.width.max(1),
    );
    let target_image_height = plane_target_dimension(
        (sampled_height as u32).saturating_mul(IMAGE_PLANE_TARGET_PIXELS_PER_SAMPLE),
        IMAGE_PLANE_MIN_TARGET_IMAGE_PIXELS,
        layout.image.height.max(1),
    );
    let width_scale = target_image_width as f32 / layout.image.width.max(1) as f32;
    let height_scale = target_image_height as f32 / layout.image.height.max(1) as f32;
    let scale = width_scale.min(height_scale).min(1.0);
    if scale >= 0.999 {
        return None;
    }
    Some((
        ((full_width as f32) * scale).round().max(1.0) as u32,
        ((full_height as f32) * scale).round().max(1.0) as u32,
    ))
}

fn adaptive_image_spectrum_render_pixel_cap(
    area: Rect,
    font_size: (u16, u16),
    profile: &ImageProfilePayload,
) -> Option<(u32, u32)> {
    if area.is_empty() {
        return None;
    }
    let full_width = u32::from(area.width.max(1)) * u32::from(font_size.0.max(1));
    let full_height = u32::from(area.height.max(1)) * u32::from(font_size.1.max(1));
    let layout = image_spectrum_layout(full_width, full_height);
    if layout.plot.width == 0 || layout.plot.height == 0 {
        return None;
    }
    let target_plot_width = plane_target_dimension(
        (profile.samples.len() as u32)
            .max(1)
            .saturating_mul(IMAGE_SPECTRUM_TARGET_PIXELS_PER_SAMPLE),
        IMAGE_SPECTRUM_MIN_TARGET_PLOT_WIDTH,
        layout.plot.width.max(1),
    );
    let target_plot_height = layout
        .plot
        .height
        .clamp(1, IMAGE_SPECTRUM_TARGET_PLOT_HEIGHT);
    let width_scale = target_plot_width as f32 / layout.plot.width.max(1) as f32;
    let height_scale = target_plot_height as f32 / layout.plot.height.max(1) as f32;
    let scale = width_scale.min(height_scale).min(1.0);
    if scale >= 0.999 {
        return None;
    }
    Some((
        ((full_width as f32) * scale).round().max(1.0) as u32,
        ((full_height as f32) * scale).round().max(1.0) as u32,
    ))
}

fn plane_target_dimension(desired: u32, min_target: u32, max_target: u32) -> u32 {
    let max_target = max_target.max(1);
    let min_target = min_target.min(max_target);
    desired.clamp(min_target, max_target)
}

fn image_movie_content_signature(
    state: &ImageBrowserSessionState,
    layout: &UiLayout,
    theme_mode: ThemeMode,
    split_ratio: f32,
    plane_font_size: (u16, u16),
    spectrum_font_size: Option<(u16, u16)>,
    spectrum_visible: bool,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    state.snapshot_generation.hash(&mut hasher);
    state.selected_non_display_axis.hash(&mut hasher);
    state
        .selected_non_display_axis_state()
        .map(|axis| axis.axis)
        .hash(&mut hasher);
    state.viewport.width.hash(&mut hasher);
    state.viewport.height.hash(&mut hasher);
    state.viewport.inspector_height.hash(&mut hasher);
    state.viewport.plane_pixel_width.hash(&mut hasher);
    state.viewport.plane_pixel_height.hash(&mut hasher);
    split_ratio.to_bits().hash(&mut hasher);
    spectrum_visible.hash(&mut hasher);
    plane_font_size.hash(&mut hasher);
    spectrum_font_size.hash(&mut hasher);
    result_text_area(layout).hash(&mut hasher);
    crate::ui::image_plane_canvas_area_for_browser(layout, spectrum_visible, split_ratio)
        .hash(&mut hasher);
    crate::ui::image_spectrum_canvas_area(layout, spectrum_visible, split_ratio).hash(&mut hasher);
    match theme_mode {
        ThemeMode::DenseAnsi => 0u8,
        ThemeMode::RichPanel => 1u8,
    }
    .hash(&mut hasher);
    hasher.finish()
}

fn image_movie_preview_indices(
    snapshot: &ImageBrowserSnapshot,
    movie_axis: usize,
    axis_index: usize,
) -> Vec<usize> {
    snapshot
        .non_display_axes
        .iter()
        .map(|axis| {
            if axis.axis == movie_axis {
                axis_index.min(axis.length.saturating_sub(1))
            } else {
                axis.index
            }
        })
        .collect()
}

fn build_image_movie_preview_request(
    spec: &ImageMovieSchedulerSpec,
    axis_index: usize,
) -> ImageBrowserPreviewRequest {
    ImageBrowserPreviewRequest {
        viewport: spec.viewport,
        parameters: spec.parameters.clone(),
        plane_content_mode: spec.plane_content_mode,
        non_display_indices: image_movie_preview_indices(
            &spec.snapshot,
            spec.movie_axis,
            axis_index,
        ),
        include_profile: spec.spectrum_visible,
    }
}

pub(crate) fn image_movie_lookahead_occurrences(
    requested_fps: f64,
    axis_length: usize,
    render_worker_count: usize,
    protocol_worker_count: usize,
) -> usize {
    let frame_interval = Duration::from_secs_f64(1.0 / requested_fps.max(0.001));
    let horizon = Duration::from_millis(150).max(frame_interval.mul_f64(1.5));
    let frame_count = ((horizon.as_secs_f64() / frame_interval.as_secs_f64()).ceil() as usize)
        .clamp(1, IMAGE_MOVIE_RENDER_POOL_QUEUE_CAPACITY.max(1));
    let worker_hint = render_worker_count.max(protocol_worker_count).clamp(1, 2);
    frame_count
        .max(worker_hint)
        .min(axis_length.max(1))
        .min(IMAGE_MOVIE_RENDER_POOL_QUEUE_CAPACITY.max(1))
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn image_movie_presentation_lookahead_occurrences(
    requested_fps: f64,
    axis_length: usize,
    protocol_worker_count: usize,
) -> usize {
    let frame_interval = Duration::from_secs_f64(1.0 / requested_fps.max(0.001));
    let horizon = Duration::from_millis(150).max(frame_interval.mul_f64(1.5));
    let frame_count = ((horizon.as_secs_f64() / frame_interval.as_secs_f64()).ceil() as usize)
        .clamp(1, IMAGE_MOVIE_PROTOCOL_POOL_QUEUE_CAPACITY.max(1));
    let worker_hint = protocol_worker_count.clamp(1, 2);
    frame_count
        .max(worker_hint)
        .min(axis_length.max(1))
        .min(IMAGE_MOVIE_PROTOCOL_POOL_QUEUE_CAPACITY.max(1))
}

fn image_plane_overlay_markers(state: &ImageBrowserSessionState) -> Vec<ImagePlaneOverlayMarker> {
    image_plane_overlay_markers_for_snapshot(&state.snapshot, &state.pinned_probes)
}

fn image_plane_overlay_markers_for_snapshot(
    snapshot: &ImageBrowserSnapshot,
    pinned_probes: &[ImagePinnedProbe],
) -> Vec<ImagePlaneOverlayMarker> {
    pinned_probes
        .iter()
        .enumerate()
        .filter_map(|(color_index, probe)| {
            image_pinned_probe_sample_from_snapshot(snapshot, probe).map(|sample| {
                ImagePlaneOverlayMarker {
                    sample,
                    color_index,
                }
            })
        })
        .collect()
}

fn image_spectrum_overlay_series(
    state: &ImageBrowserSessionState,
) -> Vec<ImageSpectrumOverlaySeries> {
    image_spectrum_overlay_series_for_pinned(&state.pinned_probes)
}

fn image_spectrum_overlay_series_for_pinned(
    pinned_probes: &[ImagePinnedProbe],
) -> Vec<ImageSpectrumOverlaySeries> {
    pinned_probes
        .iter()
        .enumerate()
        .filter_map(|(color_index, probe)| {
            probe
                .profile
                .clone()
                .map(|profile| ImageSpectrumOverlaySeries {
                    label: probe.label.clone(),
                    profile,
                    color_index,
                })
        })
        .collect()
}

fn image_plane_render_signature(
    snapshot: &ImageBrowserSnapshot,
    show_live_reticle: bool,
    colormap: ImagePlaneColormap,
    invert: bool,
    overlay_markers: &[ImagePlaneOverlayMarker],
    region_overlay_shapes: &[casars_imagebrowser_protocol::ImageRegionOverlayShapeState],
) -> u64 {
    let mut hasher = DefaultHasher::new();
    snapshot.parameters.blc.hash(&mut hasher);
    snapshot.parameters.trc.hash(&mut hasher);
    snapshot.parameters.inc.hash(&mut hasher);
    snapshot.parameters.stretch.hash(&mut hasher);
    snapshot.parameters.autoscale.hash(&mut hasher);
    snapshot.parameters.clip_low.hash(&mut hasher);
    snapshot.parameters.clip_high.hash(&mut hasher);
    show_live_reticle.hash(&mut hasher);
    match colormap {
        ImagePlaneColormap::Grayscale => 0u8,
        ImagePlaneColormap::Viridis => 1u8,
        ImagePlaneColormap::Inferno => 2u8,
    }
    .hash(&mut hasher);
    invert.hash(&mut hasher);
    if let Some(cursor) = snapshot.plane_cursor.as_ref() {
        cursor.sampled_x.hash(&mut hasher);
        cursor.sampled_y.hash(&mut hasher);
        cursor.pixel_x.hash(&mut hasher);
        cursor.pixel_y.hash(&mut hasher);
    }
    for axis in &snapshot.non_display_axes {
        axis.axis.hash(&mut hasher);
        axis.index.hash(&mut hasher);
        axis.pixel.hash(&mut hasher);
    }
    if let Some(plane) = snapshot.plane.as_ref() {
        plane.width.hash(&mut hasher);
        plane.height.hash(&mut hasher);
        plane.clip_min.to_bits().hash(&mut hasher);
        plane.clip_max.to_bits().hash(&mut hasher);
        plane.data_min.to_bits().hash(&mut hasher);
        plane.data_max.to_bits().hash(&mut hasher);
        plane.masked_or_non_finite_count.hash(&mut hasher);
        plane.no_finite_values.hash(&mut hasher);
        plane.value_unit.hash(&mut hasher);
        plane.pixels_u8.hash(&mut hasher);
    }
    image_plane_frame_label(snapshot).hash(&mut hasher);
    for marker in overlay_markers {
        marker.color_index.hash(&mut hasher);
        marker.sample.hash(&mut hasher);
    }
    for shape in region_overlay_shapes {
        shape.closed.hash(&mut hasher);
        for vertex in &shape.vertices {
            vertex.sampled_x.to_bits().hash(&mut hasher);
            vertex.sampled_y.to_bits().hash(&mut hasher);
        }
    }
    hasher.finish()
}

fn image_plane_frame_label(snapshot: &ImageBrowserSnapshot) -> Option<String> {
    let labels = snapshot
        .non_display_axes
        .iter()
        .map(|axis| {
            format!(
                "{} {}/{}",
                axis.label,
                axis.index,
                axis.length.saturating_sub(1)
            )
        })
        .collect::<Vec<_>>();
    (!labels.is_empty()).then(|| labels.join(" | "))
}

fn image_movie_animation_signature(
    snapshot: &ImageBrowserSnapshot,
    show_live_reticle: bool,
    colormap: ImagePlaneColormap,
    invert: bool,
    movie_axis: usize,
    theme_mode: ThemeMode,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    snapshot.parameters.blc.hash(&mut hasher);
    snapshot.parameters.trc.hash(&mut hasher);
    snapshot.parameters.inc.hash(&mut hasher);
    snapshot.parameters.stretch.hash(&mut hasher);
    snapshot.parameters.autoscale.hash(&mut hasher);
    snapshot.parameters.clip_low.hash(&mut hasher);
    snapshot.parameters.clip_high.hash(&mut hasher);
    movie_axis.hash(&mut hasher);
    show_live_reticle.hash(&mut hasher);
    match theme_mode {
        ThemeMode::DenseAnsi => 0u8,
        ThemeMode::RichPanel => 1u8,
    }
    .hash(&mut hasher);
    match colormap {
        ImagePlaneColormap::Grayscale => 0u8,
        ImagePlaneColormap::Viridis => 1u8,
        ImagePlaneColormap::Inferno => 2u8,
    }
    .hash(&mut hasher);
    invert.hash(&mut hasher);
    for axis in &snapshot.display_axes {
        axis.axis.hash(&mut hasher);
        axis.name.hash(&mut hasher);
        axis.unit.hash(&mut hasher);
        axis.blc.hash(&mut hasher);
        axis.trc.hash(&mut hasher);
        axis.inc.hash(&mut hasher);
        axis.sampled_len.hash(&mut hasher);
        axis.world_increment.map(f64::to_bits).hash(&mut hasher);
    }
    // Keep the direct-movie overlay key stable across ordinary frame stepping. Interactive
    // cursor and region edits already stop movie playback, so they should not invalidate the
    // cached terminal overlay while the movie is running.
    hasher.finish()
}

fn hashed_render_request_key<T>(key: &T) -> u64
where
    T: Hash,
{
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn hashed_render_input_cache_key<T>(key: &T, pixel_width: u32, pixel_height: u32) -> u64
where
    T: Hash,
{
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    pixel_width.hash(&mut hasher);
    pixel_height.hash(&mut hasher);
    hasher.finish()
}

fn hashed_movie_occurrence_key(key: MovieOccurrenceKey) -> u64 {
    hashed_render_request_key(&key)
}

fn image_spectrum_render_signature(
    profile: &ImageProfilePayload,
    overlay_profiles: &[ImageSpectrumOverlaySeries],
    theme_mode: ThemeMode,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    match theme_mode {
        ThemeMode::DenseAnsi => 0u8,
        ThemeMode::RichPanel => 1u8,
    }
    .hash(&mut hasher);
    hash_profile_payload(&mut hasher, profile);
    overlay_profiles.len().hash(&mut hasher);
    for overlay in overlay_profiles {
        overlay.label.hash(&mut hasher);
        overlay.color_index.hash(&mut hasher);
        hash_profile_payload(&mut hasher, &overlay.profile);
    }
    hasher.finish()
}

fn hash_profile_payload(hasher: &mut DefaultHasher, profile: &ImageProfilePayload) {
    profile.axis.hash(hasher);
    profile.axis_name.hash(hasher);
    profile.coord_type.hash(hasher);
    profile.axis_unit.hash(hasher);
    profile.value_unit.hash(hasher);
    profile.selected_sample_index.hash(hasher);
    profile.samples.len().hash(hasher);
    for sample in &profile.samples {
        sample.sample_index.hash(hasher);
        sample.pixel_index.hash(hasher);
        sample.value.to_bits().hash(hasher);
        sample.masked.hash(hasher);
        sample.finite.hash(hasher);
        if let Some(world_axis) = sample.world_axis.as_ref() {
            world_axis.name.hash(hasher);
            world_axis.unit.hash(hasher);
            world_axis.value.to_bits().hash(hasher);
        } else {
            0u8.hash(hasher);
        }
    }
}

fn image_pinned_probe_sample_from_snapshot(
    snapshot: &ImageBrowserSnapshot,
    probe: &ImagePinnedProbe,
) -> Option<(usize, usize)> {
    let display_x = snapshot.display_axes.first()?;
    let display_y = snapshot.display_axes.get(1)?;
    if probe.plane_pixel.0 < display_x.blc
        || probe.plane_pixel.0 > display_x.trc
        || probe.plane_pixel.1 < display_y.blc
        || probe.plane_pixel.1 > display_y.trc
    {
        return None;
    }
    let sample_x = sample_index_for_pixel(probe.plane_pixel.0, display_x.blc, display_x.inc);
    let sample_y = sample_index_for_pixel(probe.plane_pixel.1, display_y.blc, display_y.inc);
    (sample_x < display_x.sampled_len && sample_y < display_y.sampled_len)
        .then_some((sample_x, sample_y))
}

fn image_raster_click_target(
    state: &ImageBrowserSessionState,
    column: u16,
    row: u16,
    canvas: Rect,
) -> Option<(usize, usize)> {
    let font_size = state
        .panel
        .as_ref()
        .map(|panel| panel.font_size)
        .unwrap_or((1, 1));
    let draw_rect = image_plane_draw_rect(canvas, &state.snapshot, font_size)?;
    if !rect_contains(draw_rect, column, row) {
        return None;
    }
    let display_x = state.snapshot.display_axes.first()?;
    let display_y = state.snapshot.display_axes.get(1)?;
    if display_x.sampled_len == 0 || display_y.sampled_len == 0 {
        return None;
    }
    let relative_x = usize::from(column.saturating_sub(draw_rect.x));
    let relative_y = usize::from(row.saturating_sub(draw_rect.y));
    let sampled_x = image_click_sample_index(
        relative_x,
        usize::from(draw_rect.width.max(1)),
        display_x.sampled_len,
    );
    let sampled_y = image_click_sample_index(
        relative_y,
        usize::from(draw_rect.height.max(1)),
        display_y.sampled_len,
    );
    Some((
        display_x.blc + sampled_x * display_x.inc,
        display_y.blc + sampled_y * display_y.inc,
    ))
}

pub(crate) fn image_plane_draw_rect(
    canvas: Rect,
    snapshot: &ImageBrowserSnapshot,
    font_size: (u16, u16),
) -> Option<Rect> {
    if canvas.is_empty() {
        return None;
    }
    let font_width = u32::from(font_size.0.max(1));
    let font_height = u32::from(font_size.1.max(1));
    let geometry = image_plane_layout(
        u32::from(canvas.width.max(1)) * font_width,
        u32::from(canvas.height.max(1)) * font_height,
        image_plane_display_aspect_ratio(snapshot),
        snapshot.display_axes.len() >= 2,
    )
    .image;
    let start_x = geometry.x / font_width;
    let start_y = geometry.y / font_height;
    let end_x = div_ceil_u32(geometry.x + geometry.width, font_width);
    let end_y = div_ceil_u32(geometry.y + geometry.height, font_height);
    Some(Rect {
        x: canvas.x.saturating_add(start_x as u16),
        y: canvas.y.saturating_add(start_y as u16),
        width: (end_x.saturating_sub(start_x)).min(u32::from(canvas.width)) as u16,
        height: (end_y.saturating_sub(start_y)).min(u32::from(canvas.height)) as u16,
    })
}

pub(crate) fn image_spectrum_plot_rect(canvas: Rect, font_size: (u16, u16)) -> Option<Rect> {
    if canvas.is_empty() {
        return None;
    }
    let font_width = u32::from(font_size.0.max(1));
    let font_height = u32::from(font_size.1.max(1));
    let plot = image_spectrum_layout(
        u32::from(canvas.width.max(1)) * font_width,
        u32::from(canvas.height.max(1)) * font_height,
    )
    .plot;
    let start_x = plot.x / font_width;
    let start_y = plot.y / font_height;
    let end_x = div_ceil_u32(plot.x + plot.width, font_width);
    let end_y = div_ceil_u32(plot.y + plot.height, font_height);
    Some(Rect {
        x: canvas.x.saturating_add(start_x as u16),
        y: canvas.y.saturating_add(start_y as u16),
        width: (end_x.saturating_sub(start_x)).min(u32::from(canvas.width)) as u16,
        height: (end_y.saturating_sub(start_y)).min(u32::from(canvas.height)) as u16,
    })
}

fn image_click_sample_index(relative: usize, draw_len: usize, sampled_len: usize) -> usize {
    if draw_len == 0 || sampled_len == 0 {
        return 0;
    }
    let numerator = (relative.saturating_mul(2).saturating_add(1)).saturating_mul(sampled_len);
    (numerator / draw_len.saturating_mul(2)).min(sampled_len.saturating_sub(1))
}

fn sample_index_for_pixel(pixel: usize, blc: usize, inc: usize) -> usize {
    pixel.saturating_sub(blc) / inc.max(1)
}

fn div_ceil_u32(value: u32, divisor: u32) -> u32 {
    value.div_ceil(divisor.max(1))
}

fn image_plane_display_aspect_ratio(snapshot: &ImageBrowserSnapshot) -> Option<f64> {
    let x = snapshot.display_axes.first()?;
    let y = snapshot.display_axes.get(1)?;
    let x_span = x.trc.saturating_sub(x.blc).saturating_add(1).max(1) as f64;
    let y_span = y.trc.saturating_sub(y.blc).saturating_add(1).max(1) as f64;
    let (x_scale, y_scale) = image_plane_axis_scales(x, y);
    let aspect = (x_span * x_scale) / (y_span * y_scale);
    (aspect.is_finite() && aspect > 0.0).then_some(aspect)
}

fn image_plane_axis_scales(x: &ImageDisplayAxisState, y: &ImageDisplayAxisState) -> (f64, f64) {
    if is_direction_display_axis(&x.name) && is_direction_display_axis(&y.name) {
        return (
            x.world_increment
                .map(|increment| angular_increment_arcseconds(increment).value().abs())
                .filter(|value| *value > 0.0)
                .unwrap_or(1.0),
            y.world_increment
                .map(|increment| angular_increment_arcseconds(increment).value().abs())
                .filter(|value| *value > 0.0)
                .unwrap_or(1.0),
        );
    }
    if !x.unit.is_empty() && x.unit == y.unit {
        return (
            x.world_increment
                .map(f64::abs)
                .filter(|value| *value > 0.0)
                .unwrap_or(1.0),
            y.world_increment
                .map(f64::abs)
                .filter(|value| *value > 0.0)
                .unwrap_or(1.0),
        );
    }
    (1.0, 1.0)
}

fn is_direction_display_axis(name: &str) -> bool {
    name.eq_ignore_ascii_case("Right Ascension")
        || name.eq_ignore_ascii_case("RA")
        || name.eq_ignore_ascii_case("Declination")
        || name.eq_ignore_ascii_case("DEC")
}

fn image_movie_perf_context_from_state(
    state: &ImageBrowserSessionState,
    canvas: Option<Rect>,
    canvas_pixels: Option<(u32, u32)>,
    render_request_key_hash: Option<u64>,
) -> MoviePerfContext {
    let axis_state = state.selected_non_display_axis_state();
    MoviePerfContext {
        axis: axis_state.map(|axis| axis.axis),
        axis_index: axis_state.map(|axis| axis.index),
        axis_length: axis_state.map(|axis| axis.length),
        render_request_key_hash,
        canvas_cell_size: canvas.map(|area| (area.width, area.height)),
        canvas_pixel_size: canvas_pixels,
        raster_mode: state.plane_mode == ImagePlaneMode::Raster,
        direct_overlay: state.movie.direct_overlay,
        terminal_looping: state.movie.terminal_looping,
        requested_fps_milli: Some((state.movie.fps * 1000.0).round() as u64),
    }
}

fn image_startup_perf_context(
    width: u16,
    height: u16,
    plane_pixel_width: u16,
    plane_pixel_height: u16,
) -> MoviePerfContext {
    MoviePerfContext {
        axis: None,
        axis_index: None,
        axis_length: None,
        render_request_key_hash: None,
        canvas_cell_size: Some((width, height)),
        canvas_pixel_size: Some((
            u32::from(plane_pixel_width.max(1)),
            u32::from(plane_pixel_height.max(1)),
        )),
        raster_mode: true,
        direct_overlay: false,
        terminal_looping: false,
        requested_fps_milli: Some((IMAGE_MOVIE_DEFAULT_FPS * 1000.0).round() as u64),
    }
}

fn image_movie_perf_context_from_snapshot(
    state: &ImageBrowserSessionState,
    snapshot: &ImageBrowserSnapshot,
    render_request_key_hash: Option<u64>,
) -> MoviePerfContext {
    let axis_state = snapshot
        .non_display_axes
        .get(state.selected_non_display_axis)
        .or_else(|| snapshot.non_display_axes.first());
    MoviePerfContext {
        axis: axis_state.map(|axis| axis.axis),
        axis_index: axis_state.map(|axis| axis.index),
        axis_length: axis_state.map(|axis| axis.length),
        render_request_key_hash,
        canvas_cell_size: Some((state.viewport.width, state.viewport.height)),
        canvas_pixel_size: Some((
            u32::from(state.viewport.plane_pixel_width),
            u32::from(state.viewport.plane_pixel_height),
        )),
        raster_mode: state.plane_mode == ImagePlaneMode::Raster,
        direct_overlay: state.movie.direct_overlay,
        terminal_looping: state.movie.terminal_looping,
        requested_fps_milli: Some((state.movie.fps * 1000.0).round() as u64),
    }
}

fn map_backend_timing(timing: &ImageBackendTimingState) -> BackendTimingBreakdown {
    BackendTimingBreakdown {
        cached_plane_lookup_ns: timing.cached_plane_lookup_ns,
        plane_extract_ns: timing.plane_extract_ns,
        stat_collection_ns: timing.stat_collection_ns,
        histogram_ns: timing.histogram_ns,
        rasterize_ns: timing.rasterize_ns,
        total_plane_ns: timing.total_plane_ns,
        profile_cache_hits: timing.profile_cache_hits,
        profile_cache_misses: timing.profile_cache_misses,
        profile_extract_total_ns: timing.profile_extract_total_ns,
    }
}

fn map_backend_plane_outcome(timing: Option<&ImageBackendTimingState>) -> MovieFrameOutcome {
    match timing.map(|timing| timing.plane_cache_result) {
        Some(ImageBackendPlaneCacheResult::Hit | ImageBackendPlaneCacheResult::PrefetchHit) => {
            MovieFrameOutcome::CacheHitBackendPlane
        }
        _ => MovieFrameOutcome::CacheMiss,
    }
}

fn image_zoom_parameters(
    snapshot: &ImageBrowserSnapshot,
    zoom_in: bool,
) -> Option<ImageBrowserParameters> {
    if snapshot.active_view != ImageBrowserView::Plane || snapshot.display_axes.len() < 2 {
        return None;
    }
    let (mut blc, mut trc, inc) = image_snapshot_window(snapshot)?;
    let cursor = snapshot.plane_cursor.as_ref();
    for (display_index, axis_state) in snapshot.display_axes.iter().take(2).enumerate() {
        let axis = axis_state.axis;
        let full_len = *snapshot.shape.get(axis)?;
        let span = trc[axis].saturating_sub(blc[axis]).saturating_add(1).max(1);
        let target_span = if zoom_in {
            span.div_ceil(2).max(1)
        } else {
            span.saturating_mul(2).min(full_len.max(1))
        };
        let center_pixel = match (display_index, cursor) {
            (0, Some(cursor)) => cursor.pixel_x,
            (1, Some(cursor)) => cursor.pixel_y,
            _ => blc[axis].saturating_add(span / 2),
        }
        .min(full_len.saturating_sub(1));
        let new_blc = centered_window_start(center_pixel, target_span, full_len);
        blc[axis] = new_blc;
        trc[axis] = new_blc.saturating_add(target_span.saturating_sub(1));
    }
    Some(ImageBrowserParameters {
        blc: format_usize_axis_list(&blc),
        trc: format_usize_axis_list(&trc),
        inc: format_usize_axis_list(&inc),
        stretch: snapshot.parameters.stretch.clone(),
        autoscale: snapshot.parameters.autoscale.clone(),
        clip_low: snapshot.parameters.clip_low.clone(),
        clip_high: snapshot.parameters.clip_high.clone(),
    })
}

fn image_pan_parameters(
    snapshot: &ImageBrowserSnapshot,
    dx: i32,
    dy: i32,
) -> Option<ImageBrowserParameters> {
    if snapshot.active_view != ImageBrowserView::Plane || snapshot.display_axes.len() < 2 {
        return None;
    }
    let (mut blc, mut trc, inc) = image_snapshot_window(snapshot)?;
    for (display_index, axis_state) in snapshot.display_axes.iter().take(2).enumerate() {
        let axis = axis_state.axis;
        let full_len = *snapshot.shape.get(axis)?;
        let span = trc[axis].saturating_sub(blc[axis]).saturating_add(1).max(1);
        let step = (span / 6).max(1) as i32;
        let delta = match display_index {
            0 => dx.saturating_mul(step),
            1 => dy.saturating_mul(step),
            _ => 0,
        };
        let max_start = full_len.saturating_sub(span);
        let new_blc = (blc[axis] as i32 + delta).clamp(0, max_start as i32) as usize;
        blc[axis] = new_blc;
        trc[axis] = new_blc.saturating_add(span.saturating_sub(1));
    }
    Some(ImageBrowserParameters {
        blc: format_usize_axis_list(&blc),
        trc: format_usize_axis_list(&trc),
        inc: format_usize_axis_list(&inc),
        stretch: snapshot.parameters.stretch.clone(),
        autoscale: snapshot.parameters.autoscale.clone(),
        clip_low: snapshot.parameters.clip_low.clone(),
        clip_high: snapshot.parameters.clip_high.clone(),
    })
}

fn image_snapshot_window(
    snapshot: &ImageBrowserSnapshot,
) -> Option<(Vec<usize>, Vec<usize>, Vec<usize>)> {
    let shape_len = snapshot.shape.len();
    let blc = parse_usize_axis_list(&snapshot.parameters.blc, shape_len)?;
    let trc = parse_usize_axis_list(&snapshot.parameters.trc, shape_len)?;
    let inc = parse_usize_axis_list(&snapshot.parameters.inc, shape_len)?;
    Some((blc, trc, inc))
}

fn parse_usize_axis_list(text: &str, expected_len: usize) -> Option<Vec<usize>> {
    let values = text
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .filter(|part| !part.is_empty())
        .map(|part| part.parse::<usize>().ok())
        .collect::<Option<Vec<_>>>()?;
    (values.len() == expected_len).then_some(values)
}

fn format_usize_axis_list(values: &[usize]) -> String {
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn centered_window_start(center: usize, span: usize, full_len: usize) -> usize {
    let span = span.clamp(1, full_len.max(1));
    center
        .saturating_sub(span / 2)
        .min(full_len.saturating_sub(span))
}

fn image_browser_max_hscroll(snapshot: &ImageBrowserSnapshot, viewport_width: u16) -> usize {
    let viewport_width = usize::from(viewport_width);
    image_browser_content_width(snapshot).saturating_sub(viewport_width)
}

fn image_plane_visible_width(state: &ImageBrowserSessionState) -> usize {
    let viewport_width = usize::from(state.viewport.width.max(1));
    let needs_vscroll =
        state.snapshot.navigation.total_items > state.snapshot.navigation.viewport_items;
    viewport_width.saturating_sub(usize::from(needs_vscroll))
}

fn keep_image_plane_selection_visible(state: &mut ImageBrowserSessionState) {
    if !state.spreadsheet_plane_active() {
        return;
    }

    let Some((selected_start, selected_end)) = image_plane_selected_span(&state.snapshot) else {
        return;
    };
    let visible_width = image_plane_visible_width(state);
    if visible_width == 0 {
        return;
    }

    let current = usize::from(state.hscroll);
    let max_scroll = image_browser_max_hscroll(&state.snapshot, state.viewport.width);
    let next = if selected_start < current {
        selected_start
    } else if selected_end > current.saturating_add(visible_width) {
        selected_end.saturating_sub(visible_width)
    } else {
        current
    };
    state.hscroll = next.min(max_scroll).min(u16::MAX as usize) as u16;
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

fn render_image_probe(probe: &casars_imagebrowser_protocol::ImageBrowserProbe) -> String {
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
        format_numeric_value_with_unit(value, unit)
    }
}

fn format_numeric_value_with_unit(value: f64, unit: &str) -> String {
    format_quantity_auto(value, unit, 6).unwrap_or_else(|| format!("{value} {unit}"))
}

fn format_profile_axis_label(profile: &ImageProfilePayload) -> String {
    let unit = frequency_display_unit_for_profile(profile)
        .map(str::to_string)
        .unwrap_or_else(|| profile.axis_unit.clone());
    if unit.is_empty() {
        profile.axis_name.clone()
    } else {
        format!("{} [{unit}]", profile.axis_name)
    }
}

fn frequency_display_unit_for_profile(profile: &ImageProfilePayload) -> Option<&'static str> {
    let axis_unit = profile.axis_unit.as_str();
    let hz = Unit::new("Hz").ok()?;
    let axis = Unit::new(axis_unit).ok()?;
    if !axis.conformant(&hz) {
        return None;
    }
    let max_abs_hz = profile
        .samples
        .iter()
        .filter_map(|sample| {
            sample
                .world_axis
                .as_ref()
                .filter(|axis| axis.unit == axis_unit)
                .and_then(|axis| Quantity::new(axis.value, axis_unit).ok())
                .and_then(|quantity| quantity.get_value_in(&hz).ok())
                .map(f64::abs)
        })
        .fold(0.0, f64::max);
    Some(if max_abs_hz >= 1e9 {
        "GHz"
    } else if max_abs_hz >= 1e6 {
        "MHz"
    } else if max_abs_hz >= 1e3 {
        "kHz"
    } else {
        "Hz"
    })
}

fn format_profile_selected_label(
    sample: &casars_imagebrowser_protocol::ImageProfileSampleState,
    value_unit: &str,
) -> String {
    let world = sample
        .world_axis
        .as_ref()
        .map(|axis| format_world_axis_probe_value(&axis.name, &axis.unit, axis.value))
        .unwrap_or_else(|| format!("pixel {}", sample.pixel_index));
    let value = if sample.masked {
        "masked".to_string()
    } else if sample.finite && sample.value.is_finite() {
        if value_unit.is_empty() {
            format!("{:.6e}", sample.value)
        } else {
            format!("{:.6e} {value_unit}", sample.value)
        }
    } else {
        sample.value.to_string()
    };
    format!("{world} -> {value}")
}

fn format_quantity_auto(value: f64, unit: &str, decimals: usize) -> Option<String> {
    let quantity = Quantity::new(value, unit).ok()?;
    let scaled = quantity.auto_scaled().ok()?;
    if scaled.unit().name().is_empty() {
        Some(trim_float_text(format!("{:.*}", decimals, scaled.value())))
    } else {
        Some(format!(
            "{} {}",
            trim_float_text(format!("{:.*}", decimals, scaled.value())),
            scaled.unit().name()
        ))
    }
}

fn trim_float_text(mut text: String) -> String {
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" { "0".into() } else { text }
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
        KeyCode::Char('P') if key_event.modifiers == KeyModifiers::SHIFT => {
            Some(ParameterAction::PromoteWorkflowProduct)
        }
        KeyCode::Char('k') if key_event.modifiers == KeyModifiers::CONTROL => {
            Some(ParameterAction::MoveUp)
        }
        KeyCode::Char('j') if key_event.modifiers == KeyModifiers::CONTROL => {
            Some(ParameterAction::MoveDown)
        }
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
        KeyCode::Delete if key_event.modifiers.is_empty() => Some(ParameterAction::Delete),
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
    use casa_images::ImageMovieSurfaceKind;
    use casa_ms::ui_schema::UiCommandSchema;
    use casars_imagebrowser_protocol::{
        ImageBrowserAxisValue, ImageBrowserCapabilities, ImageBrowserFocus, ImageBrowserParameters,
        ImageBrowserSnapshot, ImageBrowserView, ImageBrowserViewport, ImageDisplayAxisState,
        ImageNavigationMetrics, ImageNonDisplayAxisState, ImagePlaneCursorState, ImagePlaneRaster,
        ImageProfilePayload, ImageProfileSampleState,
    };
    use ratatui::layout::Rect;

    use super::{
        AppState, BrowserSession, BrowserSessionKind, ConfigStore, FormField, FormSectionContent,
        ImageBrowserLeftPaneMode, ImageBrowserSessionState, ImageMovieState, ImagePlaneColormap,
        ImagePlaneMode, StaticFormItem, build_workflow_sections, centered_window_start,
        expand_tilde_path_with_home, image_pan_parameters, image_plane_draw_rect,
        image_zoom_parameters, new_direct_image_movie_engine,
    };
    use std::{
        path::{Path, PathBuf},
        time::Duration,
    };

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
    fn test_idle_timeout_is_unchanged_without_tarpaulin() {
        let timeout = Duration::from_secs(60);
        assert_eq!(AppState::test_idle_timeout(timeout, false), timeout);
    }

    #[test]
    fn test_idle_timeout_extends_under_tarpaulin() {
        let timeout = Duration::from_secs(60);
        assert_eq!(
            AppState::test_idle_timeout(timeout, true),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn llvm_profile_file_marks_tarpaulin_context_for_waits() {
        let _guard = crate::test_env_lock();
        let original_tarpaulin = std::env::var_os("TARPAULIN");
        let original_llvm_profile = std::env::var_os("LLVM_PROFILE_FILE");

        unsafe {
            std::env::remove_var("TARPAULIN");
            std::env::remove_var("LLVM_PROFILE_FILE");
        }
        assert!(!AppState::under_tarpaulin_for_test());

        unsafe {
            std::env::set_var("LLVM_PROFILE_FILE", "target/tarpaulin/%p.profraw");
        }
        assert!(AppState::under_tarpaulin_for_test());

        unsafe {
            match original_tarpaulin {
                Some(value) => std::env::set_var("TARPAULIN", value),
                None => std::env::remove_var("TARPAULIN"),
            }
            match original_llvm_profile {
                Some(value) => std::env::set_var("LLVM_PROFILE_FILE", value),
                None => std::env::remove_var("LLVM_PROFILE_FILE"),
            }
        }
    }

    fn plane_snapshot() -> ImageBrowserSnapshot {
        ImageBrowserSnapshot {
            status_line: "ready".into(),
            active_view: ImageBrowserView::Plane,
            focus: ImageBrowserFocus::Content,
            shape: vec![256, 256, 30],
            parameters: ImageBrowserParameters {
                blc: "0,0,0".into(),
                trc: "255,255,29".into(),
                inc: "1,1,1".into(),
                stretch: "percentile99".into(),
                autoscale: "per_plane".into(),
                clip_low: String::new(),
                clip_high: String::new(),
            },
            inspector_lines: vec!["Shape: [256, 256, 30]".into()],
            content_lines: Vec::new(),
            navigation: ImageNavigationMetrics {
                selected_index: 0,
                total_items: 0,
                viewport_items: 0,
            },
            plane: None,
            probe: None,
            profile: None,
            display_axes: vec![
                ImageDisplayAxisState {
                    axis: 0,
                    name: "Right Ascension".into(),
                    unit: "rad".into(),
                    blc: 0,
                    trc: 255,
                    inc: 1,
                    sampled_len: 256,
                    world_increment: Some(-1.0e-4),
                },
                ImageDisplayAxisState {
                    axis: 1,
                    name: "Declination".into(),
                    unit: "rad".into(),
                    blc: 0,
                    trc: 255,
                    inc: 1,
                    sampled_len: 256,
                    world_increment: Some(1.0e-4),
                },
            ],
            plane_cursor: Some(ImagePlaneCursorState {
                sampled_x: 128,
                sampled_y: 128,
                pixel_x: 128,
                pixel_y: 128,
            }),
            non_display_axes: Vec::new(),
            region: None,
            saved_region_names: Vec::new(),
            active_region_definition_name: None,
            region_reference: casars_imagebrowser_protocol::ImageRegionReference::None,
            mask_names: Vec::new(),
            default_mask_name: None,
            mask_reference: casars_imagebrowser_protocol::ImageMaskReference::None,
            backend_timing: None,
            capabilities: ImageBrowserCapabilities {
                renderable_plane: true,
                world_coords_available: true,
                pixel_only_mode: false,
                non_display_axis_selectors: false,
                mask_present: false,
                complex_unsupported: false,
            },
        }
    }

    fn plane_raster() -> ImagePlaneRaster {
        ImagePlaneRaster {
            width: 16,
            height: 16,
            pixels_u8: (0u8..=255).cycle().take(16 * 16).collect(),
            clip_min: 0.0,
            clip_max: 1.0,
            data_min: 0.0,
            data_max: 1.0,
            value_unit: "Jy/beam".into(),
            histogram_bins: vec![8; 16],
            masked_or_non_finite_count: 0,
            no_finite_values: false,
        }
    }

    fn spectrum_profile() -> ImageProfilePayload {
        ImageProfilePayload {
            axis: 2,
            axis_name: "Frequency".into(),
            axis_unit: "Hz".into(),
            value_unit: "Jy/beam".into(),
            coord_type: "Spectral".into(),
            selected_sample_index: 1,
            samples: vec![
                ImageProfileSampleState {
                    sample_index: 0,
                    pixel_index: 0,
                    value: 0.25,
                    masked: false,
                    finite: true,
                    world_axis: Some(ImageBrowserAxisValue {
                        name: "Frequency".into(),
                        unit: "Hz".into(),
                        value: 1.420e9,
                    }),
                },
                ImageProfileSampleState {
                    sample_index: 1,
                    pixel_index: 1,
                    value: 0.5,
                    masked: false,
                    finite: true,
                    world_axis: Some(ImageBrowserAxisValue {
                        name: "Frequency".into(),
                        unit: "Hz".into(),
                        value: 1.421e9,
                    }),
                },
                ImageProfileSampleState {
                    sample_index: 2,
                    pixel_index: 2,
                    value: 0.1,
                    masked: false,
                    finite: true,
                    world_axis: Some(ImageBrowserAxisValue {
                        name: "Frequency".into(),
                        unit: "Hz".into(),
                        value: 1.422e9,
                    }),
                },
            ],
        }
    }

    fn spectrum_profile_with_selected(selected_sample_index: usize) -> ImageProfilePayload {
        let mut profile = spectrum_profile();
        profile.selected_sample_index = selected_sample_index.min(profile.samples.len() - 1);
        profile
    }

    fn image_test_app(snapshot: ImageBrowserSnapshot) -> AppState {
        let temp = tempfile::tempdir().expect("tempdir");
        let schema = crate::registry::imexplore_app()
            .load_schema()
            .expect("load imexplore schema");
        let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
        let mut app =
            AppState::from_schema_with_config(crate::registry::imexplore_app(), schema, config);
        app.browser_session = Some(BrowserSession {
            root_path: "test.image".into(),
            kind: BrowserSessionKind::Image(Box::new(ImageBrowserSessionState {
                client: crate::browser_client::ImageBrowserClient::for_test(),
                snapshot,
                viewport: ImageBrowserViewport::with_plane_pixels(160, 48, 0, 1280, 768),
                hscroll: 0,
                left_pane_mode: ImageBrowserLeftPaneMode::Live,
                selected_saved_region_index: 0,
                selected_mask_index: 0,
                selected_non_display_axis: 0,
                pinned_probes: Vec::new(),
                selected_pinned_probe_id: None,
                next_pinned_probe_id: 1,
                restoring_selected_pinned_probe: false,
                show_live_reticle: true,
                plane_mode: ImagePlaneMode::Raster,
                plane_colormap: ImagePlaneColormap::Grayscale,
                plane_invert: false,
                panel: None,
                spectrum_panel: None,
                snapshot_generation: 1,
                movie: ImageMovieState::with_fps(10.0),
                movie_scheduler: None,
                movie_frame_seq: None,
                direct_movie_engine: new_direct_image_movie_engine(),
            })),
        });
        app
    }

    #[test]
    fn centered_window_start_clamps_to_image_bounds() {
        assert_eq!(centered_window_start(5, 16, 256), 0);
        assert_eq!(centered_window_start(250, 32, 256), 224);
        assert_eq!(centered_window_start(128, 64, 256), 96);
    }

    #[test]
    fn zoom_in_parameters_center_on_plane_cursor() {
        let snapshot = plane_snapshot();
        let parameters = image_zoom_parameters(&snapshot, true).expect("zoom parameters");
        assert_eq!(parameters.blc, "64,64,0");
        assert_eq!(parameters.trc, "191,191,29");
        assert_eq!(parameters.inc, "1,1,1");
    }

    #[test]
    fn pan_parameters_shift_window_without_touching_non_display_axes() {
        let mut snapshot = plane_snapshot();
        snapshot.parameters = ImageBrowserParameters {
            blc: "64,64,7".into(),
            trc: "191,191,7".into(),
            inc: "1,1,1".into(),
            stretch: "percentile99".into(),
            autoscale: "per_plane".into(),
            clip_low: String::new(),
            clip_high: String::new(),
        };
        let parameters = image_pan_parameters(&snapshot, 1, -1).expect("pan parameters");
        assert_eq!(parameters.blc, "85,43,7");
        assert_eq!(parameters.trc, "212,170,7");
        assert_eq!(parameters.inc, "1,1,1");
    }

    #[test]
    fn image_plane_draw_rect_reserves_space_for_axis_annotations() {
        let snapshot = plane_snapshot();
        let rect = image_plane_draw_rect(
            Rect {
                x: 0,
                y: 0,
                width: 80,
                height: 40,
            },
            &snapshot,
            (8, 16),
        )
        .expect("plane draw rect");
        assert!(rect.x > 0);
        assert!(rect.height < 40);
    }

    #[test]
    fn direct_movie_bundle_info_maps_plane_and_spectrum_surfaces() {
        let mut snapshot = plane_snapshot();
        snapshot.plane = Some(plane_raster());
        snapshot.profile = Some(spectrum_profile());
        snapshot.non_display_axes = vec![ImageNonDisplayAxisState {
            axis: 2,
            label: "Frequency".into(),
            index: 17,
            length: 63,
            pixel: 17,
        }];
        let mut app = image_test_app(snapshot);
        if let Some(state) = app.image_browser_session_state_mut() {
            state.movie.playing = true;
        }
        let layout = crate::ui::compute_layout(Rect::new(0, 0, 180, 52), &app);

        let info = app
            .current_direct_image_movie_bundle_info(&layout)
            .expect("direct movie bundle info");

        assert_eq!(info.axis_index, 17);
        assert_eq!(info.surfaces.len(), 2);
        assert!(info.surface(ImageMovieSurfaceKind::Plane).is_some());
        assert!(info.surface(ImageMovieSurfaceKind::Spectrum).is_some());
    }

    #[test]
    fn image_render_requests_scale_down_large_surface_pixels_to_content_budget() {
        let mut snapshot = plane_snapshot();
        snapshot.plane = Some(plane_raster());
        snapshot.profile = Some(spectrum_profile());
        snapshot.non_display_axes = vec![ImageNonDisplayAxisState {
            axis: 2,
            label: "Frequency".into(),
            index: 17,
            length: 63,
            pixel: 17,
        }];
        let app = image_test_app(snapshot);
        let layout = crate::ui::compute_layout(Rect::new(0, 0, 240, 80), &app);
        let font_size = app.image_plane_font_size_for_test();
        let plane_canvas = crate::ui::image_plane_canvas_area_for_browser(&layout, true, 0.75);
        let spectrum_canvas =
            crate::ui::image_spectrum_canvas_area(&layout, true, 0.75).expect("spectrum canvas");
        let full_plane_pixels = (
            u32::from(plane_canvas.width.max(1)) * u32::from(font_size.0.max(1)),
            u32::from(plane_canvas.height.max(1)) * u32::from(font_size.1.max(1)),
        );
        let full_spectrum_pixels = (
            u32::from(spectrum_canvas.width.max(1)) * u32::from(font_size.0.max(1)),
            u32::from(spectrum_canvas.height.max(1)) * u32::from(font_size.1.max(1)),
        );

        let plane_pixels = app
            .image_plane_render_pixels_for_test(&layout, font_size)
            .expect("plane render pixels");
        let spectrum_pixels = app
            .image_spectrum_render_pixels_for_test(&layout)
            .expect("spectrum render pixels");

        assert!(plane_pixels.0 < full_plane_pixels.0);
        assert!(plane_pixels.1 < full_plane_pixels.1);
        assert!(spectrum_pixels.0 < full_spectrum_pixels.0);
        assert!(spectrum_pixels.1 < full_spectrum_pixels.1);
    }

    #[test]
    fn direct_movie_bundle_cache_warms_on_repeat_request() {
        let mut snapshot = plane_snapshot();
        snapshot.plane = Some(plane_raster());
        snapshot.profile = Some(spectrum_profile());
        snapshot.non_display_axes = vec![ImageNonDisplayAxisState {
            axis: 2,
            label: "Frequency".into(),
            index: 9,
            length: 32,
            pixel: 9,
        }];
        let mut app = image_test_app(snapshot);
        if let Some(state) = app.image_browser_session_state_mut() {
            state.movie.playing = true;
        }
        let layout = crate::ui::compute_layout(Rect::new(0, 0, 180, 52), &app);

        let first = app
            .current_direct_image_movie_bundle(&layout)
            .expect("first direct movie bundle");
        let second = app
            .current_direct_image_movie_bundle(&layout)
            .expect("second direct movie bundle");

        assert!(!first.cache_hit);
        assert!(second.cache_hit);
        assert_eq!(
            first.rendered.surfaces.len(),
            second.rendered.surfaces.len()
        );
    }

    #[test]
    fn direct_movie_spectrum_bitmap_changes_with_selected_channel() {
        let mut snapshot = plane_snapshot();
        snapshot.plane = Some(plane_raster());
        snapshot.profile = Some(spectrum_profile_with_selected(0));
        snapshot.non_display_axes = vec![ImageNonDisplayAxisState {
            axis: 2,
            label: "Frequency".into(),
            index: 0,
            length: 3,
            pixel: 0,
        }];
        let mut app = image_test_app(snapshot);
        if let Some(state) = app.image_browser_session_state_mut() {
            state.movie.playing = true;
        }
        let layout = crate::ui::compute_layout(Rect::new(0, 0, 180, 52), &app);

        let first = app
            .current_direct_image_movie_bundle(&layout)
            .expect("first direct movie bundle");
        let first_spectrum = first
            .rendered
            .surfaces
            .iter()
            .find(|surface| surface.spec.kind == ImageMovieSurfaceKind::Spectrum)
            .expect("spectrum surface")
            .bitmap
            .clone();

        if let Some(state) = app.image_browser_session_state_mut() {
            state.snapshot.profile = Some(spectrum_profile_with_selected(2));
            state.snapshot.non_display_axes[0].index = 2;
            state.snapshot.non_display_axes[0].pixel = 2;
        }

        let second = app
            .current_direct_image_movie_bundle(&layout)
            .expect("second direct movie bundle");
        let second_spectrum = second
            .rendered
            .surfaces
            .iter()
            .find(|surface| surface.spec.kind == ImageMovieSurfaceKind::Spectrum)
            .expect("spectrum surface")
            .bitmap
            .clone();

        assert_ne!(first_spectrum.as_raw(), second_spectrum.as_raw());
    }

    #[test]
    fn scheduled_movie_bundle_spectrum_bitmap_changes_with_selected_channel() {
        let mut snapshot = plane_snapshot();
        snapshot.plane = Some(plane_raster());
        snapshot.profile = Some(spectrum_profile_with_selected(0));
        snapshot.non_display_axes = vec![ImageNonDisplayAxisState {
            axis: 2,
            label: "Frequency".into(),
            index: 0,
            length: 3,
            pixel: 0,
        }];
        let mut app = image_test_app(snapshot.clone());
        if let Some(state) = app.image_browser_session_state_mut() {
            state.movie.playing = true;
        }
        let layout = crate::ui::compute_layout(Rect::new(0, 0, 180, 52), &app);
        let spec = app
            .current_image_movie_scheduler_spec(&layout)
            .expect("movie scheduler spec");

        let first = app
            .build_movie_prepared_bundle(
                &layout,
                &spec,
                super::MovieOccurrenceKey {
                    generation: 1,
                    movie_axis: 2,
                    axis_index: 0,
                },
                snapshot.clone(),
            )
            .expect("first movie bundle");
        let first_spectrum = first
            .rendered
            .surfaces
            .iter()
            .find(|surface| surface.spec.kind == ImageMovieSurfaceKind::Spectrum)
            .expect("spectrum surface")
            .bitmap
            .clone();

        snapshot.profile = Some(spectrum_profile_with_selected(2));
        snapshot.non_display_axes[0].index = 2;
        snapshot.non_display_axes[0].pixel = 2;

        let second = app
            .build_movie_prepared_bundle(
                &layout,
                &spec,
                super::MovieOccurrenceKey {
                    generation: 1,
                    movie_axis: 2,
                    axis_index: 2,
                },
                snapshot,
            )
            .expect("second movie bundle");
        let second_spectrum = second
            .rendered
            .surfaces
            .iter()
            .find(|surface| surface.spec.kind == ImageMovieSurfaceKind::Spectrum)
            .expect("spectrum surface")
            .bitmap
            .clone();

        assert_ne!(first_spectrum.as_raw(), second_spectrum.as_raw());
    }

    #[test]
    fn imager_stage_parameters_exclude_meta_arguments() {
        let schema: UiCommandSchema = serde_json::from_value(casa_task_runtime::project_ui_schema(
            &casa_provider_contracts::builtin_surface_bundle("imager").unwrap(),
        ))
        .unwrap();
        let fields = schema
            .arguments
            .iter()
            .filter_map(FormField::from_schema)
            .collect::<Vec<_>>();
        let sections = build_workflow_sections("imager", &fields);
        let stages = sections
            .iter()
            .find(|section| section.name == "Stages")
            .expect("stages section");
        let FormSectionContent::Items(stage_items) = &stages.content;
        let stage_ids = stage_items
            .iter()
            .map(|item| match item {
                StaticFormItem::Field(index) => fields[*index].schema.id.as_str(),
                other => panic!("unexpected stages item: {other:?}"),
            })
            .collect::<Vec<_>>();
        let stage_parameters = sections
            .iter()
            .find(|section| section.name == "Stage Parameters")
            .expect("stage parameters section");
        let FormSectionContent::Items(items) = &stage_parameters.content;
        let ids = items
            .iter()
            .map(|item| match item {
                StaticFormItem::Field(index) => fields[*index].schema.id.as_str(),
                other => panic!("unexpected stage-parameters item: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert!(stage_ids.contains(&"dirty_only"));
        assert!(stage_ids.contains(&"niter"));
        assert!(stage_ids.contains(&"threshold"));
        assert!(ids.contains(&"imsize"));
        assert!(!ids.contains(&"ui_schema"));
        assert!(!ids.contains(&"help"));
    }
}
