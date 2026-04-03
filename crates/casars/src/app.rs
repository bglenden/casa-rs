// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "app/browser_manager.rs"]
mod browser_manager;
pub(crate) use browser_manager::BrowserManagerRowView;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use casacore_imagebrowser_protocol::{
    ImageBackendPlaneCacheResult, ImageBackendTimingState, ImageBrowserCommand, ImageBrowserFocus,
    ImageBrowserParameters, ImageBrowserPreviewRequest, ImageBrowserProbe, ImageBrowserSnapshot,
    ImageBrowserView, ImageBrowserViewport, ImageDisplayAxisState, ImagePlaneContentMode,
    ImageProfilePayload,
};
use casacore_ms::listobs::cli::{
    UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiManagedOutputSchema, UiValueKind,
};
use casacore_ms::msexplore::cli::build_explore_spec_from_args;
use casacore_ms::{
    ListObsOptions, ListObsPlotExportFormat, ListObsPlotKind, ListObsPlotPayload, ListObsPlotSpec,
    ListObsSummary, ListObsUvCoverage, MeasurementSet, MsExportFormat, MsPlotPayload, MsPlotPreset,
    build_listobs_plot_payload_from_summary, build_listobs_uv_plot_payload,
    build_listobs_visibility_plot_payload, build_msexplore_payload_from_spec, export_listobs_plot,
    export_msexplore_plot,
};
use casacore_tablebrowser_protocol::{
    BrowserCommand, BrowserComplex32Value, BrowserComplex64Value, BrowserFocus,
    BrowserInspectorSnapshot, BrowserScalarValue, BrowserSnapshot, BrowserValueNode,
    BrowserView as TableBrowserView, BrowserViewport,
};
use casacore_types::measures::direction::{
    angular_increment_arcseconds, format_declination_labeled, format_right_ascension_labeled,
};
use casacore_types::quanta::{MvAngle, MvTime, Quantity, Unit};
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
use crate::clipboard;
use crate::config::{ConfigStore, ThemeMode};
use crate::execution::{ExecutionEvent, ExecutionPlan, RunningProcess, spawn_process};
use crate::graphics::{
    ImagePlaneColormap, ImagePlaneOverlayMarker, ImagePlaneRenderInput, ImageSpectrumOverlaySeries,
    ImageSpectrumRenderInput, ListObsPlotRenderInput, MsExplorePlotRenderInput, PlotRenderInput,
    image_plane_layout, image_spectrum_layout, plot_theme, render_image_plane_image,
    render_image_spectrum_image, render_plot_image,
};
use crate::movie_perf::{
    BackendTimingBreakdown, MovieFrameOutcome, MoviePerfContext, MoviePerfTracer,
    MoviePipelineState,
};
use crate::registry::{BrowserAppKind, RegistryApp};
use crate::ui::UiLayout;

const DENSE_SPINNER_FRAMES: &[&str] = &["|", "/", "-", "\\"];
const RICH_SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠴", "⠦"];
const DOUBLE_CLICK_WINDOW: Duration = Duration::from_millis(450);
const HORIZONTAL_SCROLL_STEP: i16 = 8;
const IMAGE_PLANE_CELL_WIDTH: usize = 11;
const IMAGE_MOVIE_DEFAULT_FPS: f64 = 1.0;
const IMAGE_PLANE_RENDER_CACHE_CAPACITY: usize = 32;
const IMAGE_SPECTRUM_RENDER_CACHE_CAPACITY: usize = 64;
const IMAGE_MOVIE_BITMAP_CACHE_BYTES: usize = 512 * 1024 * 1024;
const IMAGE_MOVIE_RENDER_POOL_QUEUE_CAPACITY: usize = 12;
const IMAGE_MOVIE_PROTOCOL_POOL_QUEUE_CAPACITY: usize = 8;
const IMAGE_MOVIE_PROTOCOL_LOOKAHEAD_OCCURRENCES: usize = 4;
const IMEXPLORE_LIVE_PARAMETER_FIELD_IDS: [&str; 9] = [
    "image_path",
    "blc",
    "trc",
    "inc",
    "stretch",
    "autoscale",
    "clip_low",
    "clip_high",
    "fps",
];
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
    BrowserPane(BrowserPaneSelection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BrowserPaneSelection {
    Mode(ImageBrowserLeftPaneMode),
    SavedRegion(usize),
    Mask(usize),
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
    pub target: PlotCatalogTarget,
    pub label: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlotCatalogTarget {
    ListObs(ListObsPlotKind),
    MsExplorePreset(MsPlotPreset),
    MsExploreCustomPlot,
    MsExplorePageSpec,
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
    kitty_response_capture: Option<String>,
    kitty_movie_store_invalidated: bool,
    last_click: Option<ClickState>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MovieOccurrenceKey {
    generation: u64,
    movie_axis: usize,
    axis_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MovieBundleKey {
    occurrence: MovieOccurrenceKey,
    plane_signature: u64,
    spectrum_signature: Option<u64>,
}

#[derive(Debug, Clone)]
struct CurrentImageSpectrumRenderRequest {
    request_key: ImageSpectrumRequestKey,
    pixel_width: u32,
    pixel_height: u32,
    input: ImageSpectrumRenderInput,
}

#[derive(Debug, Clone)]
struct MovieBundleRenderJob {
    occurrence: MovieOccurrenceKey,
    bundle_key: MovieBundleKey,
    snapshot: ImageBrowserSnapshot,
    plane_request: CurrentImagePlaneRenderRequest,
    spectrum_request: Option<CurrentImageSpectrumRenderRequest>,
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
    bundle_key: MovieBundleKey,
    snapshot: ImageBrowserSnapshot,
    plane_request: CurrentImagePlaneRenderRequest,
    plane_bitmap: RgbaImage,
    spectrum_request: Option<CurrentImageSpectrumRenderRequest>,
    spectrum_bitmap: Option<RgbaImage>,
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

#[derive(Debug)]
struct MovieBitmapCache<K> {
    capacity_bytes: usize,
    total_bytes: usize,
    values: HashMap<K, PreparedMovieBundle>,
    order: VecDeque<K>,
}

struct ImageMovieSchedulerState {
    generation: u64,
    content_signature: u64,
    movie_axis: usize,
    next_due_index: usize,
    next_due_at: Instant,
    session_indices: Vec<(usize, usize)>,
    ready_bundles: BTreeMap<usize, MovieBundleKey>,
    ready_presentations: BTreeMap<usize, PreparedMoviePresentation>,
    in_flight_occurrences: HashSet<usize>,
    in_flight_presentations: HashSet<usize>,
    render_pool: PanelRenderPool<MovieBundleRenderJob, PreparedMovieBundle, String>,
    protocol_pool: PanelRenderPool<MovieProtocolRenderJob, PreparedMoviePresentation, String>,
    bitmap_cache: MovieBitmapCache<MovieBundleKey>,
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

impl<K> MovieBitmapCache<K>
where
    K: Clone + Eq + Hash,
{
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes: capacity_bytes.max(1),
            total_bytes: 0,
            values: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<PreparedMovieBundle> {
        let value = self.values.get(key).cloned()?;
        self.touch(key);
        Some(value)
    }

    fn insert(&mut self, key: K, value: PreparedMovieBundle) {
        let value_bytes = prepared_movie_bundle_bytes(&value);
        if let Some(previous) = self.values.insert(key.clone(), value) {
            self.total_bytes = self
                .total_bytes
                .saturating_sub(prepared_movie_bundle_bytes(&previous));
            self.total_bytes = self.total_bytes.saturating_add(value_bytes);
            self.touch(&key);
            self.evict_if_needed();
            return;
        }
        self.total_bytes = self.total_bytes.saturating_add(value_bytes);
        self.order.push_back(key);
        self.evict_if_needed();
    }

    fn clear(&mut self) {
        self.values.clear();
        self.order.clear();
        self.total_bytes = 0;
    }

    fn touch(&mut self, key: &K) {
        if let Some(index) = self.order.iter().position(|existing| existing == key)
            && let Some(existing) = self.order.remove(index)
        {
            self.order.push_back(existing);
        }
    }

    fn evict_if_needed(&mut self) {
        while self.total_bytes > self.capacity_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(previous) = self.values.remove(&oldest) {
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub(prepared_movie_bundle_bytes(&previous));
            }
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
            next_due_index,
            next_due_at: Instant::now() + Duration::from_secs_f64(1.0 / fps.max(0.001)),
            session_indices,
            ready_bundles: BTreeMap::new(),
            ready_presentations: BTreeMap::new(),
            in_flight_occurrences: HashSet::new(),
            in_flight_presentations: HashSet::new(),
            render_pool: PanelRenderPool::new(
                image_movie_render_worker_count(),
                IMAGE_MOVIE_RENDER_POOL_QUEUE_CAPACITY,
                |job| Ok(render_movie_bundle(&job.input)),
            )
            .expect("image movie render pool"),
            protocol_pool: PanelRenderPool::new(
                image_movie_render_worker_count(),
                IMAGE_MOVIE_PROTOCOL_POOL_QUEUE_CAPACITY,
                |job| render_movie_presentation(&job.input),
            )
            .expect("image movie protocol pool"),
            bitmap_cache: MovieBitmapCache::new(IMAGE_MOVIE_BITMAP_CACHE_BYTES),
            queue_capacity: IMAGE_MOVIE_RENDER_POOL_QUEUE_CAPACITY,
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
        self.next_due_index = next_due_index;
        self.next_due_at = Instant::now() + Duration::from_secs_f64(1.0 / fps.max(0.001));
        self.session_indices = session_indices;
        self.ready_bundles.clear();
        self.ready_presentations.clear();
        self.in_flight_occurrences.clear();
        self.in_flight_presentations.clear();
        self.bitmap_cache.clear();
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
        render_queue_depth: scheduler.render_pool.queue_depth(),
        render_active_jobs: scheduler.render_pool.active_job_count(),
        protocol_queue_depth: scheduler.protocol_pool.queue_depth(),
        protocol_active_jobs: scheduler.protocol_pool.active_job_count(),
        ready_bundle_count: scheduler.ready_bundles.len(),
        ready_presentation_count: scheduler.ready_presentations.len(),
        bitmap_cache_bytes: scheduler.bitmap_cache.total_bytes,
    }
}

fn prepared_movie_bundle_bytes(bundle: &PreparedMovieBundle) -> usize {
    let plane_bytes = bundle.plane_bitmap.as_raw().len();
    let spectrum_bytes = bundle
        .spectrum_bitmap
        .as_ref()
        .map(|bitmap| bitmap.as_raw().len())
        .unwrap_or_default();
    plane_bytes.saturating_add(spectrum_bytes)
}

fn render_movie_bundle(job: &MovieBundleRenderJob) -> PreparedMovieBundle {
    let plane_bitmap = render_image_plane_image(
        job.plane_request.pixel_width,
        job.plane_request.pixel_height,
        &job.plane_request.input,
    )
    .unwrap_or_else(|error| {
        let mut image = RgbaImage::new(
            job.plane_request.pixel_width.max(1),
            job.plane_request.pixel_height.max(1),
        );
        for pixel in image.pixels_mut() {
            *pixel = image::Rgba([0, 0, 0, 255]);
        }
        let _ = error;
        DynamicImage::ImageRgba8(image)
    })
    .to_rgba8();
    let spectrum_bitmap = job.spectrum_request.as_ref().map(|request| {
        render_image_spectrum_image(request.pixel_width, request.pixel_height, &request.input)
            .unwrap_or_else(|error| {
                let mut image =
                    RgbaImage::new(request.pixel_width.max(1), request.pixel_height.max(1));
                for pixel in image.pixels_mut() {
                    *pixel = image::Rgba([0, 0, 0, 255]);
                }
                let _ = error;
                DynamicImage::ImageRgba8(image)
            })
            .to_rgba8()
    });
    PreparedMovieBundle {
        occurrence: job.occurrence,
        bundle_key: job.bundle_key.clone(),
        snapshot: job.snapshot.clone(),
        plane_request: job.plane_request.clone(),
        plane_bitmap,
        spectrum_request: job.spectrum_request.clone(),
        spectrum_bitmap,
    }
}

fn render_movie_presentation(
    job: &MovieProtocolRenderJob,
) -> Result<PreparedMoviePresentation, String> {
    let bundle = job.bundle.clone();
    let PreparedMovieBundle {
        occurrence,
        bundle_key: _,
        snapshot,
        plane_request,
        plane_bitmap,
        spectrum_request,
        spectrum_bitmap,
    } = bundle;
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
            spectrum_bitmap,
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
    let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
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
    let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
    let font_size = picker.font_size();
    let render_image_cache = Arc::new(Mutex::new(RenderImageCache::new(
        IMAGE_SPECTRUM_RENDER_CACHE_CAPACITY,
    )));
    let worker_cache = Arc::clone(&render_image_cache);
    let renderer = PanelRenderer::<ImageSpectrumRenderInput, String>::new(
        picker.clone(),
        Resize::Fit(None),
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
            .field("next_due_index", &self.next_due_index)
            .field("next_due_at", &self.next_due_at)
            .field("session_indices", &self.session_indices)
            .field("ready_bundle_count", &self.ready_bundles.len())
            .field("in_flight_occurrences", &self.in_flight_occurrences)
            .field("queue_capacity", &self.queue_capacity)
            .field("bitmap_cache_bytes", &self.bitmap_cache.total_bytes)
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
    ) -> Option<&casacore_imagebrowser_protocol::ImageNonDisplayAxisState> {
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

#[derive(Debug, Clone)]
enum CurrentPlotPayload {
    ListObs(ListObsPlotPayload),
    MsExplore(MsPlotPayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditTarget {
    FormField(usize),
    PlotExportPath,
    PlotExportWidth,
    PlotExportHeight,
    RenameImageRegionDefinition,
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
    amplitude_time_spec: ListObsPlotSpec,
    phase_time_spec: ListObsPlotSpec,
    amplitude_uv_distance_spec: ListObsPlotSpec,
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
            amplitude_time_spec: ListObsPlotSpec::new(ListObsPlotKind::AmplitudeVsTime),
            phase_time_spec: ListObsPlotSpec::new(ListObsPlotKind::PhaseVsTime),
            amplitude_uv_distance_spec: ListObsPlotSpec::new(
                ListObsPlotKind::AmplitudeVsUvDistance,
            ),
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
    PlotCatalog(PlotCatalogTarget),
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
        let mut fields = schema
            .arguments
            .iter()
            .filter_map(FormField::from_schema)
            .collect::<Vec<_>>();
        seed_app_field_defaults(app.id, &mut fields);
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
            kitty_response_capture: None,
            kitty_movie_store_invalidated: false,
            last_click: None,
            movie_perf: MoviePerfTracer::from_env(),
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
            kitty_response_capture: None,
            kitty_movie_store_invalidated: false,
            last_click: None,
            movie_perf: MoviePerfTracer::from_env(),
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
            KeyCode::Char('o')
                if key_event.modifiers == KeyModifiers::CONTROL
                    && mode != InputMode::Edit
                    && (!self.has_active_session() || self.browser_uses_parameter_pane()) =>
            {
                return Some(AppAction::OpenPathChooser);
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
                    && mode != InputMode::Edit
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
        if self.consume_kitty_protocol_response_key(key_event) {
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
        if self.image_movie_active() {
            crate::movie_debug_log(format!(
                "mouse event kind={:?} column={} row={} modifiers={:?}",
                mouse_event.kind, mouse_event.column, mouse_event.row, mouse_event.modifiers
            ));
        }
        if movie_input_fully_ignored_for_debug() && self.image_movie_active() {
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
        if self.running.is_some() {
            format!(
                "{title} [{}] locked{}",
                spinner_frames(self.theme_mode())[self.spinner_frame],
                focus
            )
        } else if self.browser_session.is_some() {
            if self.browser_uses_parameter_pane() {
                let mode = self
                    .image_browser_session_state()
                    .map(|state| state.left_pane_mode.label().to_ascii_lowercase())
                    .unwrap_or_else(|| "live".to_string());
                match self.theme_mode() {
                    ThemeMode::DenseAnsi => format!("Parameters [{mode}]{focus}"),
                    ThemeMode::RichPanel => format!("◈ Parameters [{mode}]{focus}"),
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
        if self.browser_session.is_some() && !self.browser_uses_parameter_pane() {
            return self.browser_inspector_rows();
        }

        if self.image_browser_session_state().is_some() {
            return self.live_parameter_rows();
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

    fn live_parameter_rows(&self) -> Vec<FormRowView> {
        let mut rows = Vec::new();
        for (section_index, section) in self.sections.iter().enumerate() {
            let visible_fields = section
                .field_indices
                .iter()
                .copied()
                .filter(|index| self.show_advanced || !self.fields[*index].schema.advanced)
                .filter(|index| {
                    let field_id = self.fields[*index].schema.id.as_str();
                    IMEXPLORE_LIVE_PARAMETER_FIELD_IDS.contains(&field_id)
                })
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
        rows
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
    }

    pub(crate) fn note_image_plane_direct_presented(&mut self, request_hash: u64) {
        self.movie_perf.plane_presented(request_hash);
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
            .unwrap_or_else(|| {
                Picker::from_query_stdio()
                    .unwrap_or_else(|_| Picker::halfblocks())
                    .font_size()
            })
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
    pub(crate) fn set_text_value_and_apply(&mut self, id: &str, value: &str) {
        let field_index = self
            .fields
            .iter()
            .position(|field| field.schema.id == id)
            .expect("known test field");
        self.fields[field_index].set_text(value.to_string());
        self.apply_live_image_view_parameters_if_needed(field_index);
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
        if !self.is_msexplore_app()
            && let Some(error) = self.plot_workspace.uv_error.as_deref()
        {
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
    pub(crate) fn field_text_for_test(&self, id: &str) -> Option<String> {
        self.field_text(id)
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
    pub(crate) fn image_workspace_split_ratio_for_test(&self) -> f32 {
        self.image_workspace_split_ratio()
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

    fn build_movie_bundle_job(
        &self,
        layout: &UiLayout,
        spec: &ImageMovieSchedulerSpec,
        occurrence: MovieOccurrenceKey,
        snapshot: ImageBrowserSnapshot,
    ) -> Option<MovieBundleRenderJob> {
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
                render_scale: self.current_image_movie_plane_render_scale(),
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
                    render_scale: self.current_image_movie_spectrum_render_scale(),
                    max_pixel_size: None,
                },
            )
        } else {
            None
        };
        Some(MovieBundleRenderJob {
            occurrence,
            bundle_key: MovieBundleKey {
                occurrence,
                plane_signature: plane_request.request_key.render_signature,
                spectrum_signature: spectrum_request
                    .as_ref()
                    .map(|request| request.request_key.render_signature),
            },
            snapshot,
            plane_request,
            spectrum_request,
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
        if self.active_result_tab == ResultTab::Plots {
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

        let drained = {
            let Some(state) = self.image_browser_session_state_mut() else {
                return;
            };
            let Some(scheduler) = state.movie_scheduler.as_mut() else {
                return;
            };
            scheduler.render_pool.drain_ready(scheduler.generation)
        };

        if drained.stale_count > 0 {
            for _ in 0..drained.stale_count {
                self.movie_perf.frame_dropped(
                    None,
                    scheduler_context,
                    MovieFrameOutcome::StaleRenderDiscarded,
                    "movie render pool discarded stale completion",
                );
            }
        }

        for error in drained.errors {
            self.result.status_line = "Movie frame render failed.".into();
            self.result.status_kind = StatusKind::Warning;
            self.result.stderr = format!("{error}\n");
        }

        for ready in drained.ready {
            let bundle = ready.output;
            if let Some(state) = self.image_browser_session_state_mut()
                && let Some(scheduler) = state.movie_scheduler.as_mut()
            {
                let axis_index = bundle.occurrence.axis_index;
                let bundle_key = bundle.bundle_key.clone();
                scheduler.in_flight_occurrences.remove(&axis_index);
                scheduler.bitmap_cache.insert(bundle_key.clone(), bundle);
                scheduler.ready_bundles.insert(axis_index, bundle_key);
            }
        }

        self.queue_image_movie_presentations(&spec);

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
                    .in_flight_presentations
                    .remove(&presentation.occurrence.axis_index);
                scheduler
                    .ready_presentations
                    .insert(presentation.occurrence.axis_index, presentation);
            }
        }

        let now = Instant::now();
        let mut due_presentation = None;
        let mut deadline_miss_note = None;
        if let Some(state) = self.image_browser_session_state_mut()
            && let Some(scheduler) = state.movie_scheduler.as_mut()
            && now >= scheduler.next_due_at
        {
            if let Some(presentation) = scheduler
                .ready_presentations
                .remove(&scheduler.next_due_index)
            {
                due_presentation = Some(presentation);
                scheduler.next_due_index = (scheduler.next_due_index + 1) % spec.axis_length.max(1);
                scheduler.next_due_at = now + state.movie.frame_interval;
            } else {
                deadline_miss_note = Some(format!(
                    "movie occurrence {} missed deadline waiting for ready presentation",
                    scheduler.next_due_index
                ));
            }
        }
        if let Some(note) = deadline_miss_note {
            let (queue_depth, pipeline) = self
                .image_browser_session_state()
                .and_then(|state| state.movie_scheduler.as_ref())
                .map(|scheduler| {
                    (
                        scheduler.render_pool.queue_depth(),
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

        self.queue_image_movie_occurrences(layout, &spec);
    }

    fn queue_image_movie_occurrences(&mut self, layout: &UiLayout, spec: &ImageMovieSchedulerSpec) {
        let (render_worker_count, protocol_worker_count) = self
            .image_browser_session_state()
            .and_then(|state| state.movie_scheduler.as_ref())
            .map(|scheduler| {
                (
                    scheduler.render_pool.worker_count(),
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
            let (generation, next_due_index, already_ready, in_flight) = {
                let Some(state) = self.image_browser_session_state_mut() else {
                    return;
                };
                let Some(scheduler) = state.movie_scheduler.as_mut() else {
                    return;
                };
                let axis_index = (scheduler.next_due_index + offset) % spec.axis_length.max(1);
                (
                    scheduler.generation,
                    axis_index,
                    scheduler.ready_bundles.contains_key(&axis_index),
                    scheduler.in_flight_occurrences.contains(&axis_index),
                )
            };
            if already_ready || in_flight {
                continue;
            }

            let preview_request = build_image_movie_preview_request(spec, next_due_index);
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
                axis_index: next_due_index,
            };
            let Some(job) =
                self.build_movie_bundle_job(layout, spec, occurrence, *preview.snapshot)
            else {
                continue;
            };

            let request_hash = hashed_render_request_key(&job.plane_request.request_key);
            let context = self
                .image_browser_session_state()
                .map(|state| {
                    image_movie_perf_context_from_snapshot(state, &job.snapshot, Some(request_hash))
                })
                .unwrap_or_default();
            let frame_seq = self.movie_perf.begin_frame(context);
            let backend_timing = job.snapshot.backend_timing.as_ref().map(map_backend_timing);
            if let Some(frame_seq) = frame_seq {
                let (queue_depth, pipeline) = self
                    .image_browser_session_state()
                    .and_then(|state| state.movie_scheduler.as_ref())
                    .map(|scheduler| {
                        (
                            scheduler.render_pool.queue_depth(),
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
                    map_backend_plane_outcome(job.snapshot.backend_timing.as_ref()),
                    pipeline,
                );
            }

            let maybe_cached = {
                let Some(state) = self.image_browser_session_state_mut() else {
                    return;
                };
                let Some(scheduler) = state.movie_scheduler.as_mut() else {
                    return;
                };
                scheduler.bitmap_cache.get(&job.bundle_key).is_some()
            };
            if maybe_cached {
                if let Some(state) = self.image_browser_session_state_mut()
                    && let Some(scheduler) = state.movie_scheduler.as_mut()
                {
                    scheduler
                        .ready_bundles
                        .insert(next_due_index, job.bundle_key.clone());
                }
                continue;
            }

            let submit_result = {
                let Some(state) = self.image_browser_session_state_mut() else {
                    return;
                };
                let Some(scheduler) = state.movie_scheduler.as_mut() else {
                    return;
                };
                scheduler.render_pool.submit(
                    generation,
                    hashed_movie_bundle_key(&job.bundle_key),
                    job,
                )
            };
            match submit_result {
                Ok(_) => {
                    if let Some(state) = self.image_browser_session_state_mut()
                        && let Some(scheduler) = state.movie_scheduler.as_mut()
                    {
                        scheduler.in_flight_occurrences.insert(next_due_index);
                    }
                }
                Err(error) => {
                    self.movie_perf.frame_dropped(
                        frame_seq,
                        context,
                        MovieFrameOutcome::SkippedDueToPending,
                        format!("movie render submit failed: {error}"),
                    );
                    break;
                }
            }
        }
    }

    fn queue_image_movie_presentations(&mut self, spec: &ImageMovieSchedulerSpec) {
        let protocol_worker_count = self
            .image_browser_session_state()
            .and_then(|state| state.movie_scheduler.as_ref())
            .map(|scheduler| scheduler.protocol_pool.worker_count())
            .unwrap_or_else(image_movie_render_worker_count);
        let presentation_lookahead = spec.axis_length.min(
            IMAGE_MOVIE_PROTOCOL_LOOKAHEAD_OCCURRENCES
                .max(protocol_worker_count.saturating_mul(2))
                .max(1),
        );
        for offset in 0..presentation_lookahead {
            let (generation, axis_index, bundle) = {
                let Some(state) = self.image_browser_session_state_mut() else {
                    return;
                };
                let Some(scheduler) = state.movie_scheduler.as_mut() else {
                    return;
                };
                let axis_index = (scheduler.next_due_index + offset) % spec.axis_length.max(1);
                if scheduler.ready_presentations.contains_key(&axis_index)
                    || scheduler.in_flight_presentations.contains(&axis_index)
                {
                    continue;
                }
                let Some(bundle_key) = scheduler.ready_bundles.get(&axis_index).cloned() else {
                    continue;
                };
                let Some(bundle) = scheduler.bitmap_cache.get(&bundle_key) else {
                    continue;
                };
                (scheduler.generation, axis_index, bundle)
            };

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
                    hashed_movie_bundle_key(&bundle.bundle_key),
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
                        scheduler.in_flight_presentations.insert(axis_index);
                    }
                }
                Err(error) => {
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
                if self.browser_is_active() {
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
            BrowserAction::PageUp => self.send_browser_command(BrowserRequest::PageUp { pages: 1 }),
            BrowserAction::PageDown => {
                self.send_browser_command(BrowserRequest::PageDown { pages: 1 })
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
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        if state.snapshot.active_view != ImageBrowserView::Plane {
            self.result.status_line =
                "Plane mode toggle is only available in the Plane view.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        }
        state.plane_mode = match state.plane_mode {
            ImagePlaneMode::Raster => ImagePlaneMode::Spreadsheet,
            ImagePlaneMode::Spreadsheet => ImagePlaneMode::Raster,
        };
        let mode_label = state.plane_mode.label();
        if state.plane_mode == ImagePlaneMode::Spreadsheet {
            keep_image_plane_selection_visible(state);
        }
        let content_mode = match state.plane_mode {
            ImagePlaneMode::Raster => ImagePlaneContentMode::Raster,
            ImagePlaneMode::Spreadsheet => ImagePlaneContentMode::Spreadsheet,
        };
        let _ = state;
        self.send_browser_command(BrowserRequest::SetImagePlaneContentMode { mode: content_mode });
        self.clear_output_selection_for_target(OutputPane::Result);
        self.result.status_line = format!("Plane view switched to {mode_label} mode.");
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
            EditTarget::RenameImageRegionDefinition => {}
        }
    }

    fn advance_form_edit(&mut self, field_index: usize, forward: bool) {
        let targets = self
            .visible_form_targets()
            .into_iter()
            .filter_map(|target| match target {
                FormSelection::Field(index) => Some(index),
                FormSelection::Section(_) | FormSelection::BrowserPane(_) => None,
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

        if self.active_result_tab == ResultTab::Plots {
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

        if self.has_active_session() && !self.browser_uses_parameter_pane() {
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
                FormSelection::Field(field_index) => {
                    self.selected_form = FormSelection::Field(field_index);
                    if self.path_field_browse_hit(field_index, mouse_event.column, layout) {
                        self.open_path_chooser(field_index);
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
        self.sync_result_tab_visibility();
    }

    fn cycle_field_choice(&mut self, field_index: usize, forward: bool) {
        if let Some(field) = self.fields.get_mut(field_index) {
            field.cycle_choice(forward);
        }
        self.apply_live_image_view_parameters_if_needed(field_index);
    }

    fn adjust_selected_choice(&mut self, forward: bool) {
        match self.selected_form {
            FormSelection::Field(field_index) => self.cycle_field_choice(field_index, forward),
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
                                kind: BrowserSessionKind::Table(Box::new(TableBrowserSession {
                                    client,
                                    snapshot,
                                    viewport,
                                })),
                            });
                        }
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
                let font_size = self.image_plane_font_size();
                let viewport = ImageBrowserViewport::with_plane_pixels(
                    120,
                    24,
                    0,
                    120u16.saturating_mul(font_size.0.max(1)),
                    24u16.saturating_mul(font_size.1.max(1)),
                );
                match self
                    .app
                    .resolve_command()
                    .and_then(|command| ImageBrowserClient::spawn(&command))
                {
                    Ok(client) => match client.request_startup(ImageBrowserCommand::OpenRoot {
                        path: path.clone(),
                        viewport,
                        parameters: Some(self.current_image_browser_parameters()),
                    }) {
                        Ok(snapshot) => {
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
                                movie: ImageMovieState::with_fps(self.current_image_movie_fps()),
                                movie_scheduler: None,
                                movie_frame_seq: None,
                            };
                            state.clamp_left_pane_selection();
                            self.browser_session = Some(BrowserSession {
                                root_path: path,
                                kind: BrowserSessionKind::Image(Box::new(state)),
                            });
                            if let Some(parameters) = self
                                .browser_session()
                                .and_then(BrowserSession::image_parameters)
                            {
                                self.sync_image_parameter_fields(&parameters);
                            }
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
                                        self.apply_live_image_view_parameters_if_needed(
                                            field_index,
                                        );
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
        let movie_perf = &mut self.movie_perf;
        let mut sync_image_parameters = None::<ImageBrowserParameters>;
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
                        | BrowserRequest::SetImageViewParameters { .. } => None,
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
                    let font_size = state
                        .panel
                        .as_ref()
                        .map(|panel| panel.font_size)
                        .unwrap_or_else(|| {
                            Picker::from_query_stdio()
                                .unwrap_or_else(|_| Picker::halfblocks())
                                .font_size()
                        });
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
                                state.selected_non_display_axis =
                                    state.selected_non_display_axis.saturating_sub(steps);
                                state.selected_non_display_axis_state().map(|axis_state| {
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
                                state.selected_non_display_axis = state
                                    .selected_non_display_axis
                                    .saturating_add(steps)
                                    .min(state.snapshot.non_display_axes.len().saturating_sub(1));
                                state.selected_non_display_axis_state().map(|axis_state| {
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
                        BrowserRequest::Activate
                        | BrowserRequest::Back
                        | BrowserRequest::Escape => None,
                    };
                    let Some(request) = request else {
                        return;
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

        if let Some(parameters) = sync_image_parameters.as_ref() {
            sync_image_parameter_fields(&mut self.fields, parameters);
        }

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
            }
        }
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
        self.plot_workspace.export_path = if self.is_msexplore_app() {
            "msexplore-plot.png".to_string()
        } else {
            default_plot_export_path(
                self.plot_workspace.selected_plot,
                ListObsPlotExportFormat::Png,
            )
        };
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
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        state.plane_colormap = state.plane_colormap.next();
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.result.status_line = format!("Colormap: {}.", state.plane_colormap.label());
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
        self.plot_workspace.panel = None;
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
        if self.is_msexplore_app() {
            self.msexplore_plot_label()
        } else {
            self.plot_workspace.selected_plot.display_name().to_string()
        }
    }

    fn current_plot_summary(&self) -> Option<String> {
        if self.is_msexplore_app() {
            let ms_path = self.field_text("ms_path").unwrap_or_default();
            if ms_path.trim().is_empty() {
                return Some(
                    "Enter a MeasurementSet path to preview the current msexplore form."
                        .to_string(),
                );
            }
            if self.msexplore_form_has_plot_spec() {
                return Some(format!(
                    "{} from the current msexplore form.",
                    self.selected_plot_label()
                ));
            }
            return Some(
                "Choose a preset, a page spec, or both x/y axes to preview the current msexplore form."
                    .to_string(),
            );
        }
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
            ListObsPlotKind::AmplitudeVsTime => &self.plot_workspace.amplitude_time_spec,
            ListObsPlotKind::PhaseVsTime => &self.plot_workspace.phase_time_spec,
            ListObsPlotKind::AmplitudeVsUvDistance => {
                &self.plot_workspace.amplitude_uv_distance_spec
            }
        }
    }

    fn selected_plot_spec_mut(&mut self) -> &mut ListObsPlotSpec {
        match self.plot_workspace.selected_plot {
            ListObsPlotKind::UvCoverage => &mut self.plot_workspace.uv_spec,
            ListObsPlotKind::AntennaLayout => &mut self.plot_workspace.antenna_spec,
            ListObsPlotKind::ScanTimeline => &mut self.plot_workspace.scan_spec,
            ListObsPlotKind::SpectralWindowCoverage => &mut self.plot_workspace.spw_spec,
            ListObsPlotKind::AmplitudeVsTime => &mut self.plot_workspace.amplitude_time_spec,
            ListObsPlotKind::PhaseVsTime => &mut self.plot_workspace.phase_time_spec,
            ListObsPlotKind::AmplitudeVsUvDistance => {
                &mut self.plot_workspace.amplitude_uv_distance_spec
            }
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

    fn current_msexplore_plot_payload(&self) -> Result<MsPlotPayload, String> {
        let plan = self.build_execution_plan()?;
        let spec = build_explore_spec_from_args(plan.arguments)?;
        build_msexplore_payload_from_spec(&spec)
    }

    fn current_plot_payload(&mut self) -> Result<CurrentPlotPayload, String> {
        if self.is_msexplore_app() {
            return self
                .current_msexplore_plot_payload()
                .map(CurrentPlotPayload::MsExplore);
        }
        let Some(snapshot) = self.plot_workspace.snapshot.clone() else {
            return Err("Run listobs to populate the plot workspace.".to_string());
        };
        match self.plot_workspace.selected_plot {
            ListObsPlotKind::UvCoverage => {
                let coverage = self.current_uv_coverage(&snapshot)?;
                build_listobs_uv_plot_payload(&coverage, self.selected_plot_spec())
                    .map(CurrentPlotPayload::ListObs)
            }
            kind if kind.is_raw_visibility() => {
                let Some(path) = snapshot.path.as_ref() else {
                    return Err(
                        "Raw visibility plots require a disk-backed MeasurementSet from the last successful run."
                            .to_string(),
                    );
                };
                let ms = MeasurementSet::open(path).map_err(|error| error.to_string())?;
                build_listobs_visibility_plot_payload(
                    &ms,
                    &snapshot.options,
                    self.selected_plot_spec(),
                )
                .map(CurrentPlotPayload::ListObs)
            }
            _ => build_listobs_plot_payload_from_summary(
                &snapshot.summary,
                self.selected_plot_spec(),
            )
            .map(CurrentPlotPayload::ListObs),
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
        }
    }

    fn pump_image_spectrum_panel(&mut self) {
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        let Some(panel) = state.spectrum_panel.as_mut() else {
            return;
        };
        match panel.renderer.pump() {
            Ok(changed) => {
                if changed {
                    panel.image_size = panel.renderer.image_size();
                    if let Some(request_key) = panel.pending_request_key.take() {
                        panel.display_key = Some(request_key);
                    }
                }
            }
            Err(error) => {
                panel.pending_request_key = None;
                panel.last_error = Some(error.to_string());
            }
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
        let (pixel_width, pixel_height) = clamp_render_dimensions(
            scaled_pixel_width,
            scaled_pixel_height,
            options.max_pixel_size,
        );
        let cache_key = hashed_render_input_cache_key(&request_key, pixel_width, pixel_height);
        Some(CurrentImagePlaneRenderRequest {
            request_key,
            pixel_width: pixel_width.max(1),
            pixel_height: pixel_height.max(1),
            input: ImagePlaneRenderInput {
                cache_key,
                raster,
                cursor_sample: cursor,
                sampled_shape,
                display_axes: snapshot.display_axes.clone(),
                probe: snapshot.probe.clone(),
                overlay_markers: options.overlay_markers.to_vec(),
                region_overlay_shapes,
                display_aspect_ratio: image_plane_display_aspect_ratio(snapshot),
                colormap: options.colormap,
                invert: options.invert,
                theme_mode: options.theme_mode,
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
        let (pixel_width, pixel_height) = clamp_render_dimensions(
            scaled_pixel_width,
            scaled_pixel_height,
            options.max_pixel_size,
        );
        let cache_key = hashed_render_input_cache_key(&request_key, pixel_width, pixel_height);
        Some(CurrentImageSpectrumRenderRequest {
            request_key,
            pixel_width: pixel_width.max(1),
            pixel_height: pixel_height.max(1),
            input: ImageSpectrumRenderInput {
                cache_key,
                profile,
                overlay_profiles: options.overlay_profiles.to_vec(),
                theme_mode: options.theme_mode,
            },
        })
    }

    fn current_image_movie_plane_render_scale(&self) -> f32 {
        1.0
    }

    fn current_image_movie_spectrum_render_scale(&self) -> f32 {
        1.0
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
        let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
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
            .unwrap_or_else(|| {
                Picker::from_query_stdio()
                    .unwrap_or_else(|_| Picker::halfblocks())
                    .font_size()
            });
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
        let Some(state) = self.image_browser_session_state_mut() else {
            return;
        };
        let panel = state
            .spectrum_panel
            .get_or_insert_with(new_image_spectrum_panel_state);
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
        if self.active_result_tab != ResultTab::Plots {
            return;
        }
        let Some(area) = crate::ui::plot_canvas_area(layout) else {
            return;
        };
        if area.is_empty() {
            return;
        }
        let spec_key = if self.is_msexplore_app() {
            match self.build_execution_plan() {
                Ok(plan) => plan
                    .arguments
                    .iter()
                    .map(|value| value.to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("\u{1f}"),
                Err(error) => {
                    self.result.status_line = "Plot payload unavailable.".to_string();
                    self.result.status_kind = StatusKind::Warning;
                    self.result.stderr = format!("{error}\n");
                    return;
                }
            }
        } else {
            self.selected_plot_spec().cli_assignments().join(";")
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
            plot_kind: self.plot_workspace.selected_plot,
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
                self.result.status_line = "Plot payload unavailable.".to_string();
                self.result.status_kind = StatusKind::Warning;
                self.result.stderr = format!("{error}\n");
                return;
            }
        };

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
                CurrentPlotPayload::ListObs(payload) => {
                    PlotRenderInput::ListObs(ListObsPlotRenderInput {
                        payload,
                        theme_mode,
                        terminal_cell_px: panel.font_size,
                    })
                }
                CurrentPlotPayload::MsExplore(payload) => {
                    PlotRenderInput::MsExplore(MsExplorePlotRenderInput {
                        payload,
                        theme_mode,
                        terminal_cell_px: panel.font_size,
                    })
                }
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

    pub(crate) fn current_direct_image_movie_frame(
        &self,
        layout: &UiLayout,
    ) -> Option<ImageDirectMovieFrame> {
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
        Some(ImageDirectMovieFrame {
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

    #[cfg(test)]
    pub(crate) fn selected_plot_kind(&self) -> ListObsPlotKind {
        self.plot_workspace.selected_plot
    }

    pub(crate) fn plot_catalog_rows(&self) -> Vec<PlotCatalogRowView> {
        if self.is_msexplore_app() {
            let selected = self.current_plot_catalog_target();
            let mut rows = Vec::new();
            match selected {
                Some(PlotCatalogTarget::MsExplorePageSpec) => rows.push(PlotCatalogRowView {
                    target: PlotCatalogTarget::MsExplorePageSpec,
                    label: self.selected_plot_label(),
                    selected: true,
                }),
                Some(PlotCatalogTarget::MsExploreCustomPlot) => rows.push(PlotCatalogRowView {
                    target: PlotCatalogTarget::MsExploreCustomPlot,
                    label: self.selected_plot_label(),
                    selected: true,
                }),
                _ => {}
            }
            rows.extend(
                MsPlotPreset::ALL
                    .into_iter()
                    .map(|preset| PlotCatalogRowView {
                        target: PlotCatalogTarget::MsExplorePreset(preset),
                        label: preset.display_name().to_string(),
                        selected: selected == Some(PlotCatalogTarget::MsExplorePreset(preset)),
                    }),
            );
            return rows;
        }
        ListObsPlotKind::ALL
            .into_iter()
            .map(|kind| PlotCatalogRowView {
                target: PlotCatalogTarget::ListObs(kind),
                label: kind.display_name().to_string(),
                selected: kind == self.plot_workspace.selected_plot,
            })
            .collect()
    }

    pub(crate) fn plot_control_rows(&self) -> Vec<PlotControlRowView> {
        if self.is_msexplore_app() {
            let mut rows = Vec::new();
            for (target, label) in [
                (PlotControlTarget::Refresh, "Refresh Preview"),
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
            return rows;
        }
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
        if self.is_msexplore_app() {
            return None;
        }
        self.plot_workspace.snapshot.as_ref().and_then(|snapshot| {
            snapshot
                .dirty
                .then_some("Plots reflect the last successful run. Re-run to apply form changes.")
        })
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
                self.apply_live_image_view_parameters_if_needed(field_index);
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
        if self.is_msexplore_app() {
            match self.build_current_msexplore_plot_cli(MsExportFormat::Png) {
                Ok(cli) => self.copy_text_to_clipboard(cli, "plot CLI"),
                Err(error) => {
                    self.result.status_line = "Copy plot CLI failed.".to_string();
                    self.result.status_kind = StatusKind::Error;
                    self.result.stderr = format!("{error}\n");
                }
            }
            return;
        }
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
        let (output_path, width, height) = if self.is_msexplore_app() {
            let format = match format {
                ListObsPlotExportFormat::Png => MsExportFormat::Png,
                ListObsPlotExportFormat::Pdf => MsExportFormat::Pdf,
            };
            (
                self.current_msexplore_output_path(format),
                self.current_msexplore_export_width(),
                self.current_msexplore_export_height(),
            )
        } else {
            (
                current_plot_output_path(
                    &self.plot_workspace.export_path,
                    self.plot_workspace.selected_plot,
                    format,
                ),
                self.plot_workspace.export_width,
                self.plot_workspace.export_height,
            )
        };
        let export_result = match payload {
            CurrentPlotPayload::ListObs(payload) => export_listobs_plot(
                &payload,
                plot_theme(self.theme_mode()),
                &output_path,
                format,
                width,
                height,
            ),
            CurrentPlotPayload::MsExplore(payload) => export_msexplore_plot(
                &payload,
                plot_theme(self.theme_mode()),
                &output_path,
                match format {
                    ListObsPlotExportFormat::Png => MsExportFormat::Png,
                    ListObsPlotExportFormat::Pdf => MsExportFormat::Pdf,
                },
                width,
                height,
            ),
        };
        match export_result {
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
        if self.is_msexplore_app() {
            return;
        }
        if self.plot_workspace.selected_plot == kind {
            return;
        }
        self.plot_workspace.selected_plot = kind;
        self.plot_workspace.selected_control = 0;
        self.plot_workspace.focus = PlotPaneFocus::Catalog;
        self.clear_plot_render_cache();
        self.sync_plot_export_path_for_selected_plot();
    }

    fn current_plot_catalog_target(&self) -> Option<PlotCatalogTarget> {
        if !self.is_msexplore_app() {
            return Some(PlotCatalogTarget::ListObs(
                self.plot_workspace.selected_plot,
            ));
        }
        if self
            .field_text("page_spec")
            .is_some_and(|value| !value.trim().is_empty())
        {
            return Some(PlotCatalogTarget::MsExplorePageSpec);
        }
        if let Some(preset) = self
            .field_text("preset")
            .filter(|value| !value.trim().is_empty())
            .and_then(|value| MsPlotPreset::parse(&value).ok())
        {
            return Some(PlotCatalogTarget::MsExplorePreset(preset));
        }
        if self
            .field_text("x_axis")
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .field_text("y_axis")
                .is_some_and(|value| !value.trim().is_empty())
        {
            return Some(PlotCatalogTarget::MsExploreCustomPlot);
        }
        None
    }

    fn apply_plot_catalog_target(&mut self, target: PlotCatalogTarget) {
        match target {
            PlotCatalogTarget::ListObs(kind) => self.set_selected_plot(kind),
            PlotCatalogTarget::MsExplorePreset(preset) => self.apply_msexplore_preset(preset),
            PlotCatalogTarget::MsExploreCustomPlot | PlotCatalogTarget::MsExplorePageSpec => {}
        }
    }

    fn apply_msexplore_preset(&mut self, preset: MsPlotPreset) {
        if !self.is_msexplore_app() {
            return;
        }
        for (id, value) in [
            ("page_spec", ""),
            ("preset", preset.as_str()),
            ("x_axis", ""),
            ("y_axis", ""),
            ("y_axis2", ""),
        ] {
            let _ = self.apply_startup_text_value(id, value.to_string());
        }
        self.plot_workspace.focus = PlotPaneFocus::Catalog;
        self.plot_workspace.selected_control = 0;
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

    fn current_image_browser_parameters(&self) -> ImageBrowserParameters {
        ImageBrowserParameters {
            blc: self.field_text("blc").unwrap_or_default(),
            trc: self.field_text("trc").unwrap_or_default(),
            inc: self.field_text("inc").unwrap_or_default(),
            stretch: self
                .field_text("stretch")
                .unwrap_or_else(|| "percentile99".into()),
            autoscale: self
                .field_text("autoscale")
                .unwrap_or_else(|| "per_plane".into()),
            clip_low: self.field_text("clip_low").unwrap_or_default(),
            clip_high: self.field_text("clip_high").unwrap_or_default(),
        }
    }

    fn sync_image_parameter_fields(&mut self, parameters: &ImageBrowserParameters) {
        sync_image_parameter_fields(&mut self.fields, parameters);
    }

    fn apply_live_image_view_parameters_if_needed(&mut self, field_index: usize) {
        let Some(field) = self.fields.get(field_index) else {
            return;
        };
        if !IMEXPLORE_LIVE_PARAMETER_FIELD_IDS.contains(&field.schema.id.as_str()) {
            return;
        }
        if !self
            .browser_session()
            .is_some_and(|session| session.kind() == BrowserAppKind::Image)
        {
            return;
        }
        if field.schema.id == "fps" {
            self.apply_live_image_movie_fps();
            return;
        }
        self.send_browser_command(BrowserRequest::SetImageViewParameters {
            parameters: self.current_image_browser_parameters(),
        });
    }

    fn current_image_movie_fps(&self) -> f64 {
        self.field_text("fps")
            .and_then(|value| parse_image_movie_fps(&value).ok())
            .unwrap_or(IMAGE_MOVIE_DEFAULT_FPS)
    }

    fn apply_live_image_movie_fps(&mut self) {
        let fps_text = self.field_text("fps").unwrap_or_else(|| "1".into());
        let Ok(fps) = parse_image_movie_fps(&fps_text) else {
            self.result.status_line = "FPS must be a positive number.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
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

fn seed_app_field_defaults(app_id: &str, fields: &mut [FormField]) {
    if app_id != "msexplore" {
        return;
    }
    if let Some(field) = fields
        .iter_mut()
        .find(|field| field.schema.id == "showlegend")
    {
        let _ = field.apply_toggle_value(true);
    }
    if let Some(field) = fields
        .iter_mut()
        .find(|field| field.schema.id == "legendposition")
    {
        let _ = field.apply_text_value("exteriorRight".to_string());
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
                if self.schema.required && value.trim().is_empty() {
                    return Err(format!("{} is required.", self.schema.label));
                }
                if !value.trim().is_empty() {
                    arguments.push(OsString::from(&flags[0]));
                    arguments.push(OsString::from(value));
                }
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

fn parse_image_movie_fps(text: &str) -> Result<f64, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(IMAGE_MOVIE_DEFAULT_FPS);
    }
    let fps = trimmed
        .parse::<f64>()
        .map_err(|_| "FPS must be a positive number.".to_string())?;
    if !fps.is_finite() || fps <= 0.0 {
        return Err("FPS must be a positive number.".to_string());
    }
    Ok(fps)
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

fn image_movie_lookahead_occurrences(
    requested_fps: f64,
    axis_length: usize,
    render_worker_count: usize,
    protocol_worker_count: usize,
) -> usize {
    let frame_interval = Duration::from_secs_f64(1.0 / requested_fps.max(0.001));
    let horizon = Duration::from_millis(150).max(frame_interval.mul_f64(3.0));
    let frame_count = ((horizon.as_secs_f64() / frame_interval.as_secs_f64()).ceil() as usize)
        .clamp(1, IMAGE_MOVIE_RENDER_POOL_QUEUE_CAPACITY.max(1));
    let worker_floor = render_worker_count
        .max(protocol_worker_count)
        .saturating_mul(2)
        .max(IMAGE_MOVIE_PROTOCOL_LOOKAHEAD_OCCURRENCES);
    frame_count
        .max(worker_floor)
        .min(axis_length.max(1))
        .min(IMAGE_MOVIE_RENDER_POOL_QUEUE_CAPACITY.max(1))
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
    region_overlay_shapes: &[casacore_imagebrowser_protocol::ImageRegionOverlayShapeState],
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

fn hashed_movie_bundle_key(key: &MovieBundleKey) -> u64 {
    hashed_render_request_key(key)
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
    sample: &casacore_imagebrowser_protocol::ImageProfileSampleState,
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

const RAW_VISIBILITY_PLOT_CONTROLS: [PlotChoiceDescriptor; 2] = [
    PlotChoiceDescriptor {
        key: "data_column",
        label: "Data column",
        choices: &["data", "corrected", "model"],
    },
    PlotChoiceDescriptor {
        key: "color_by",
        label: "Color by",
        choices: &["field", "spw", "scan", "baseline", "correlation", "none"],
    },
];

fn plot_choice_descriptors(kind: ListObsPlotKind) -> &'static [PlotChoiceDescriptor] {
    match kind {
        ListObsPlotKind::UvCoverage => &UV_PLOT_CONTROLS,
        ListObsPlotKind::AntennaLayout => &ANTENNA_PLOT_CONTROLS,
        ListObsPlotKind::ScanTimeline => &SCAN_PLOT_CONTROLS,
        ListObsPlotKind::SpectralWindowCoverage => &SPW_PLOT_CONTROLS,
        ListObsPlotKind::AmplitudeVsTime
        | ListObsPlotKind::PhaseVsTime
        | ListObsPlotKind::AmplitudeVsUvDistance => &RAW_VISIBILITY_PLOT_CONTROLS,
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
    use casacore_imagebrowser_protocol::{
        ImageBrowserCapabilities, ImageBrowserFocus, ImageBrowserParameters, ImageBrowserSnapshot,
        ImageBrowserView, ImageDisplayAxisState, ImageNavigationMetrics, ImagePlaneCursorState,
    };
    use ratatui::layout::Rect;

    use super::{
        centered_window_start, expand_tilde_path_with_home, image_pan_parameters,
        image_plane_draw_rect, image_zoom_parameters,
    };
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
            mask_names: Vec::new(),
            default_mask_name: None,
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
}
