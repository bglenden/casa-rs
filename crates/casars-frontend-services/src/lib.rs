// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared frontend services exposed to Swift and Python through UniFFI.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_images::{AnyPagedImage, ImageBrowserSession, ImageInfo, ImagePixelType};
use casa_ms::columns::main_ids;
use casa_ms::plot::{
    AntennaLayoutPlotPayload, AntennaLayoutPoint, ScanTimelineBar, ScanTimelinePlotPayload,
    SpectralWindowCoverageBar, SpectralWindowCoveragePlotPayload, UvCoverageSeries,
};
use casa_ms::{
    MeasurementSet, MeasurementSetPlotPayload, MeasurementSetSummary,
    MeasurementSetSummaryOutputFormat, MsExploreSpec, MsPageExportRange, MsPlotData,
    MsPlotDataPanel, MsPlotPayload, MsPlotPreset, MsPlotSpec, MsScatterGridPayload,
    MsScatterPagePayload, MsScatterPlotPayload, MsScatterSeries, MsSelectionSpec,
    VisibilityDataColumn, build_msexplore_payload_from_spec,
};
use casa_notebook::{
    ASSISTANT_PROFILE_VERSION, ASSISTANT_TRANSCRIPT_SCHEMA_VERSION, AssistantAttachment,
    AssistantMessageId, AssistantPinReference, AssistantSessionProfile, AssistantStore,
    AttemptHandle, CORPUS_SCHEMA_VERSION, CellId, ConflictResolution, ConversationId,
    ConversationTranscript, CorpusDocumentInput, CorpusIndex, CorpusLayer, ExecutionInput,
    ExecutionReceipt, ExecutionStatus, ExportMode, NotebookDocument, NotebookId, NotebookStore,
    ProjectCorpusSource, PythonEnvironmentIdentity, PythonExecutionAuthority, PythonExecutionInput,
    ReceiptFinalization, RecordingPolicy, RecordingRequest, RunId, SaveResult,
    SaveVisualizationRequest, TaskCellIntent, Timestamp, TutorialAcquisitionApproval,
    TutorialProject, TutorialTemplate, VisualizationRenderMetadata, VisualizationReopenIntent,
    VisualizationSnapshot,
};
use casa_provider_contracts::{
    ParameterValue, ProviderInvocationAdaptation, RunProductKind, RunProductRole, RunSafetyClass,
    SurfaceContractBundle, SurfaceKind, builtin_application_catalog, builtin_surface_bundle,
};
use casa_tables::{ArrayShapeContract, ColumnType, Table, TableBrowser, TableOptions};
use casa_task_runtime::{
    BaseSource, DiagnosticCode, ManagedProfileKind, ManagedStateStore, OpenSessionRequest,
    ParameterRuntime, ParameterSession, ResolutionPatch, SessionLastCoordinator,
    TaskLastCoordinator, TaskOutputValue, decode_task_completion, project_provider_invocation,
    write_parameter_profile_atomic,
};
use casa_types::measures::direction::{
    angular_increment_arcseconds, declination_increment_arcseconds, format_declination_labeled,
    format_right_ascension_labeled,
};
use casa_types::{ArrayValue, PrimitiveType, ScalarValue, Value};
use casars_imagebrowser_protocol::ImageBrowserViewport;
use casars_imagebrowser_protocol::{
    ImageBrowserCommand, ImageBrowserFocus, ImageBrowserParameters, ImageBrowserPreviewRequest,
    ImagePlaneContentMode,
};
use casars_tablebrowser_protocol::{BrowserCommand, BrowserFocus, BrowserView, BrowserViewport};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const TUTORIAL_TASK_PARAMETER_AUDIT_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/tutorial-task-parameter-audit.json"
));
const MAX_PROJECT_SCAN_ENTRIES: usize = 512;
const MAX_PROJECT_SCAN_DEPTH: usize = 4;
const DEFAULT_GUI_MAX_PLOT_POINTS: u64 = 250_000;
const MAIN_SCALAR_CHUNK_ROWS: usize = 65_536;
const FRONTEND_POINT_PROVENANCE_LIMIT: usize = 8_000;
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;
#[cfg(test)]
const DEFAULT_PLOT_WIDTH: u32 = 960;
#[cfg(test)]
const DEFAULT_PLOT_HEIGHT: u32 = 600;

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ImageExplorerParameters {
    pub blc: String,
    pub trc: String,
    pub inc: String,
    pub stretch: String,
    pub autoscale: String,
    pub clip_low: String,
    pub clip_high: String,
}

impl From<ImageExplorerParameters> for ImageBrowserParameters {
    fn from(value: ImageExplorerParameters) -> Self {
        Self {
            blc: value.blc,
            trc: value.trc,
            inc: value.inc,
            stretch: value.stretch,
            autoscale: value.autoscale,
            clip_low: value.clip_low,
            clip_high: value.clip_high,
        }
    }
}

impl From<&ImageBrowserParameters> for ImageExplorerParameters {
    fn from(value: &ImageBrowserParameters) -> Self {
        Self {
            blc: value.blc.clone(),
            trc: value.trc.clone(),
            inc: value.inc.clone(),
            stretch: value.stretch.clone(),
            autoscale: value.autoscale.clone(),
            clip_low: value.clip_low.clone(),
            clip_high: value.clip_high.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum ImageExplorerRegionReference {
    None,
    Definition { name: String },
    File { path: String },
    Expression { expression: String },
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ImageExplorerCommand {
    pub command: String,
    pub x: Option<u64>,
    pub y: Option<u64>,
    pub name: Option<String>,
    pub new_name: Option<String>,
    pub set_default: Option<bool>,
    pub path: Option<String>,
    pub region: Option<ImageExplorerRegionReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ImageExplorerSnapshotRequest {
    pub dataset_path: String,
    pub selected_view: String,
    pub focus: String,
    pub plane_content_mode: String,
    pub parameters: ImageExplorerParameters,
    pub cursor_x: Option<u64>,
    pub cursor_y: Option<u64>,
    pub selected_profile_axis: Option<u64>,
    pub non_display_indices: Vec<u64>,
    pub commands: Vec<ImageExplorerCommand>,
    pub transient_commands: Vec<ImageExplorerCommand>,
    pub include_profile: bool,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerAxisValue {
    pub name: String,
    pub unit: String,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ImageExplorerCapabilities {
    pub renderable_plane: bool,
    pub world_coords_available: bool,
    pub pixel_only_mode: bool,
    pub non_display_axis_selectors: bool,
    pub mask_present: bool,
    pub complex_unsupported: bool,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerPlane {
    pub width: u64,
    pub height: u64,
    pub pixels_u8: Vec<u8>,
    pub clip_min: f64,
    pub clip_max: f64,
    pub data_min: f64,
    pub data_max: f64,
    pub value_unit: String,
    pub histogram_bins: Vec<u32>,
    pub masked_or_non_finite_count: u64,
    pub no_finite_values: bool,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerProfileSample {
    pub sample_index: u64,
    pub pixel_index: u64,
    pub value: f64,
    pub masked: bool,
    pub finite: bool,
    pub world_axis: Option<ImageExplorerAxisValue>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerProfile {
    pub axis: u64,
    pub axis_name: String,
    pub axis_unit: String,
    pub value_unit: String,
    pub coord_type: String,
    pub selected_sample_index: u64,
    pub samples: Vec<ImageExplorerProfileSample>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerRegionOverlayVertex {
    pub sampled_x: f64,
    pub sampled_y: f64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerRegionOverlayShape {
    pub vertices: Vec<ImageExplorerRegionOverlayVertex>,
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerRegionStats {
    pub pixel_count: u64,
    pub median: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub sigma: f64,
    pub rms: f64,
    pub sum: f64,
    pub value_unit: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerRegion {
    pub label: String,
    pub shape_count: u64,
    pub closed_shape_count: u64,
    pub editing: bool,
    pub active_shape_vertices: u64,
    pub overlay_shapes: Vec<ImageExplorerRegionOverlayShape>,
    pub stats: Option<ImageExplorerRegionStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ImageExplorerNavigation {
    pub selected_index: u64,
    pub total_items: u64,
    pub viewport_items: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerDisplayAxis {
    pub axis: u64,
    pub name: String,
    pub unit: String,
    pub blc: u64,
    pub trc: u64,
    pub inc: u64,
    pub sampled_len: u64,
    pub world_increment: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ImageExplorerPlaneCursor {
    pub sampled_x: u64,
    pub sampled_y: u64,
    pub pixel_x: u64,
    pub pixel_y: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ImageExplorerNonDisplayAxis {
    pub axis: u64,
    pub label: String,
    pub index: u64,
    pub length: u64,
    pub pixel: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerProbe {
    pub pixel_indices: Vec<u64>,
    pub pixel_axes: Vec<ImageExplorerAxisValue>,
    pub value: f64,
    pub masked: bool,
    pub finite: bool,
    pub world_axes: Vec<ImageExplorerAxisValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ImageExplorerBackendTiming {
    pub plane_cache_result: String,
    pub cached_plane_lookup_ns: u64,
    pub plane_extract_ns: u64,
    pub stat_collection_ns: u64,
    pub histogram_ns: u64,
    pub rasterize_ns: u64,
    pub total_plane_ns: u64,
    pub profile_cache_hits: u64,
    pub profile_cache_misses: u64,
    pub profile_extract_total_ns: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ImageExplorerSnapshot {
    pub status_line: String,
    pub active_view: String,
    pub focus: String,
    pub shape: Vec<u64>,
    pub parameters: ImageExplorerParameters,
    pub inspector_lines: Vec<String>,
    pub content_lines: Vec<String>,
    pub navigation: ImageExplorerNavigation,
    pub plane: Option<ImageExplorerPlane>,
    pub probe: Option<ImageExplorerProbe>,
    pub profile: Option<ImageExplorerProfile>,
    pub display_axes: Vec<ImageExplorerDisplayAxis>,
    pub plane_cursor: Option<ImageExplorerPlaneCursor>,
    pub non_display_axes: Vec<ImageExplorerNonDisplayAxis>,
    pub region: Option<ImageExplorerRegion>,
    pub saved_region_names: Vec<String>,
    pub active_region_definition_name: Option<String>,
    pub mask_names: Vec<String>,
    pub default_mask_name: Option<String>,
    pub backend_timing: Option<ImageExplorerBackendTiming>,
    pub capabilities: ImageExplorerCapabilities,
}

#[derive(Debug, Clone, Deserialize)]
struct TableBrowserSnapshotRequest {
    dataset_path: String,
    #[serde(default = "default_table_browser_width")]
    width: u16,
    #[serde(default = "default_table_browser_height")]
    height: u16,
    #[serde(default = "default_table_browser_inspector_height")]
    inspector_height: u16,
    #[serde(default)]
    selected_view: Option<String>,
    #[serde(default)]
    focus: Option<String>,
    #[serde(default)]
    commands: Vec<BrowserCommand>,
    #[serde(default)]
    transient_commands: Vec<BrowserCommand>,
}

#[derive(Debug, Clone, Deserialize)]
struct TableBrowserCellWindowRequest {
    dataset_path: String,
    #[serde(default)]
    row_start: u64,
    #[serde(default = "default_table_browser_cell_row_limit")]
    row_limit: u64,
    #[serde(default)]
    column_start: u64,
    #[serde(default = "default_table_browser_cell_column_limit")]
    column_limit: u64,
    #[serde(default)]
    column_options: Vec<TableBrowserCellColumnDisplayOption>,
}

#[derive(Debug, Clone, Deserialize)]
struct TableBrowserCellColumnDisplayOption {
    column_index: u64,
    #[serde(default)]
    array_inline_limit: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct TableBrowserCellValueRequest {
    dataset_path: String,
    row_index: u64,
    column_index: u64,
}

#[derive(Debug, Clone, Serialize)]
struct TableBrowserCellWindowSnapshot {
    table_path: String,
    row_count: u64,
    column_count: u64,
    row_start: u64,
    column_start: u64,
    columns: Vec<TableBrowserCellWindowColumn>,
    rows: Vec<TableBrowserCellWindowRow>,
}

#[derive(Debug, Clone, Serialize)]
struct TableBrowserCellWindowColumn {
    index: u64,
    name: String,
    header: String,
    summary: String,
    width: u64,
    keywords: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct TableBrowserCellWindowRow {
    index: u64,
    cells: Vec<TableBrowserCellWindowCell>,
}

#[derive(Debug, Clone, Serialize)]
struct TableBrowserCellWindowCell {
    column_index: u64,
    display: String,
    defined: bool,
}

const fn default_image_browser_width() -> u16 {
    120
}

const fn default_image_browser_height() -> u16 {
    36
}

const fn default_image_browser_inspector_height() -> u16 {
    10
}

const fn default_image_browser_plane_pixel_width() -> u16 {
    512
}

const fn default_image_browser_plane_pixel_height() -> u16 {
    384
}

const fn default_table_browser_width() -> u16 {
    120
}

const fn default_table_browser_height() -> u16 {
    32
}

const fn default_table_browser_inspector_height() -> u16 {
    10
}

const fn default_table_browser_cell_row_limit() -> u64 {
    1024
}

const fn default_table_browser_cell_column_limit() -> u64 {
    24
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum DatasetKind {
    MeasurementSet,
    Image,
    Table,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DatasetProbe {
    pub id: String,
    pub name: String,
    pub path: String,
    pub kind: DatasetKind,
    pub size_bytes: u64,
    pub modified_unix_seconds: Option<u64>,
    pub probed_unix_seconds: u64,
    pub logical_size: String,
    pub units: String,
    pub fields: Vec<String>,
    pub spectral_windows: Vec<String>,
    pub scans: Vec<String>,
    pub arrays: Vec<String>,
    pub observations: Vec<String>,
    pub antennas: Vec<String>,
    pub intents: Vec<String>,
    pub feeds: Vec<String>,
    pub correlations: Vec<String>,
    pub columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub subtables: Vec<String>,
    pub shape: Vec<u64>,
    pub notes: String,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ProjectProbe {
    pub name: String,
    pub root_path: String,
    pub datasets: Vec<DatasetProbe>,
    pub diagnostics: Vec<String>,
    pub scanned_entry_count: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum TaskProductRole {
    Primary,
    Auxiliary,
    Preview,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum TaskProductKind {
    MeasurementSet,
    CasaImage,
    CasaTable,
    FitsImage,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TaskParameterValue {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum SurfaceParameterValue {
    Bool { value: bool },
    Integer { value: i64 },
    Float { value: f64 },
    String { value: String },
    Array { values: Vec<SurfaceParameterValue> },
    Table { entries: Vec<SurfaceParameterEntry> },
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterEntry {
    pub name: String,
    pub value: SurfaceParameterValue,
}

impl From<ParameterValue> for SurfaceParameterValue {
    fn from(value: ParameterValue) -> Self {
        match value {
            ParameterValue::Bool(value) => Self::Bool { value },
            ParameterValue::Integer(value) => Self::Integer { value },
            ParameterValue::Float(value) => Self::Float { value },
            ParameterValue::String(value) => Self::String { value },
            ParameterValue::Array(values) => Self::Array {
                values: values.into_iter().map(Self::from).collect(),
            },
            ParameterValue::Table(entries) => Self::Table {
                entries: entries
                    .into_iter()
                    .map(|(name, value)| SurfaceParameterEntry {
                        name,
                        value: Self::from(value),
                    })
                    .collect(),
            },
        }
    }
}

impl From<SurfaceParameterValue> for ParameterValue {
    fn from(value: SurfaceParameterValue) -> Self {
        match value {
            SurfaceParameterValue::Bool { value } => Self::Bool(value),
            SurfaceParameterValue::Integer { value } => Self::Integer(value),
            SurfaceParameterValue::Float { value } => Self::Float(value),
            SurfaceParameterValue::String { value } => Self::String(value),
            SurfaceParameterValue::Array { values } => {
                Self::Array(values.into_iter().map(Self::from).collect())
            }
            SurfaceParameterValue::Table { entries } => Self::Table(
                entries
                    .into_iter()
                    .map(|entry| (entry.name, Self::from(entry.value)))
                    .collect(),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum SurfaceParameterType {
    Bool,
    Integer,
    Float,
    String,
    Path {
        resource_kind: Option<String>,
    },
    Choice {
        values: Vec<String>,
    },
    Quantity {
        dimension: String,
        canonical_unit: String,
        special_values: Vec<String>,
    },
    Array {
        elements: Vec<SurfaceParameterType>,
        min_items: u64,
        max_items: Option<u64>,
        allow_scalar: bool,
    },
    Table {
        fields: Vec<SurfaceParameterTypeField>,
    },
    Optional {
        values: Vec<SurfaceParameterType>,
        states: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterTypeField {
    pub name: String,
    pub value: SurfaceParameterType,
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum SurfaceParameterPredicate {
    Always,
    Never,
    IsSet {
        parameter: String,
    },
    Equals {
        parameter: String,
        value: SurfaceParameterValue,
    },
    Not {
        predicates: Vec<SurfaceParameterPredicate>,
    },
    All {
        predicates: Vec<SurfaceParameterPredicate>,
    },
    Any {
        predicates: Vec<SurfaceParameterPredicate>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceParameterDocumentation {
    pub summary: String,
    pub details: Option<String>,
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterConcept {
    pub id: String,
    pub semantic_revision: u64,
    pub casa_name: String,
    pub value_domain: SurfaceParameterType,
    pub unit_dimension: Option<String>,
    pub semantic_role: String,
    pub documentation: SurfaceParameterDocumentation,
    pub persistence_class: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterCatalog {
    pub schema_version: u64,
    pub concepts: Vec<SurfaceParameterConcept>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceParameterConceptReference {
    pub id: String,
    pub semantic_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceParameterPresentation {
    pub label: String,
    pub group: String,
    pub advanced: bool,
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceParameterCliProjection {
    pub positional: Option<u64>,
    pub flags: Vec<String>,
    pub false_flags: Vec<String>,
    pub metavar: Option<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterProviderProjection {
    pub field: String,
    pub adapter: String,
    pub emit_when: Option<SurfaceParameterPredicate>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterProjections {
    pub cli: Option<SurfaceParameterCliProjection>,
    pub provider: Option<SurfaceParameterProviderProjection>,
    pub presentation: SurfaceParameterPresentation,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceNarrowingConstraint {
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterBinding {
    pub name: String,
    pub concept: SurfaceParameterConceptReference,
    pub order: u64,
    pub refinements: Vec<SurfaceNarrowingConstraint>,
    pub context_role: Option<String>,
    pub surface_note: Option<String>,
    pub projections: SurfaceParameterProjections,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceExecutionProjection {
    pub invocation_name: String,
    pub fixed_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterDefinition {
    pub kind: String,
    pub id: String,
    pub contract_version: u64,
    pub display_name: String,
    pub category: String,
    pub summary: String,
    pub execution: SurfaceExecutionProjection,
    pub bindings: Vec<SurfaceParameterBinding>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterBundle {
    pub schema_version: u64,
    pub surface: SurfaceParameterDefinition,
    pub catalog: SurfaceParameterCatalog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum SurfaceParameterBaseSource {
    Defaults,
    Last,
    LastSuccessful,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum SurfaceParameterSourceRecord {
    Defaults,
    Last,
    LastSuccessful,
    File { path: String },
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterPatch {
    pub values: HashMap<String, SurfaceParameterValue>,
    pub unset: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterState {
    pub value: Option<SurfaceParameterValue>,
    pub origin: String,
    pub active: bool,
    pub required: bool,
    pub explicit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceParameterLocation {
    pub line: u64,
    pub column: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceParameterDiagnostic {
    pub level: String,
    pub code: String,
    pub message: String,
    pub parameter: Option<String>,
    pub location: Option<SurfaceParameterLocation>,
    pub suggestions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterSnapshot {
    pub schema_version: u64,
    pub surface_id: String,
    pub surface_kind: String,
    pub contract_version: u64,
    pub base_source: SurfaceParameterSourceRecord,
    pub dirty: bool,
    pub states: HashMap<String, SurfaceParameterState>,
    pub diagnostics: Vec<SurfaceParameterDiagnostic>,
    pub profile_toml: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TaskCompletionProduct {
    pub id: String,
    pub role: TaskProductRole,
    pub resource_kind: TaskProductKind,
    pub label: String,
    pub path: String,
    pub exists: bool,
    pub preview_path: Option<String>,
    pub preview_exists: bool,
    pub dataset: Option<DatasetProbe>,
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TaskCompletionProjection {
    pub surface_id: String,
    pub summary: String,
    pub products: Vec<TaskCompletionProduct>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct TaskContextOptionsEnvelope {
    pub schema_version: u64,
    pub dataset_path: String,
    pub dataset_kind: String,
    pub fields: Vec<String>,
    pub spectral_windows: Vec<String>,
    pub scans: Vec<String>,
    pub arrays: Vec<String>,
    pub observations: Vec<String>,
    pub antennas: Vec<String>,
    pub intents: Vec<String>,
    pub feeds: Vec<String>,
    pub correlations: Vec<String>,
    pub columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub subtables: Vec<String>,
    pub shape: Vec<u64>,
    pub defaults: HashMap<String, String>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct ApplicationCatalogEnvelope {
    pub schema_version: u64,
    pub applications: Vec<ApplicationCatalogEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct ApplicationCatalogEntry {
    pub id: String,
    pub kind: String,
    pub category: String,
    pub display_name: String,
    pub executable: String,
    pub cargo_package: String,
    pub override_env: String,
    pub shell_kind: String,
    pub interaction: String,
    pub browser_kind: Option<String>,
    pub dataset_kinds: Vec<String>,
    pub show_in_tui: bool,
    pub show_in_swift: bool,
    pub include_in_suite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct TaskUISchema {
    pub schema_version: u64,
    pub command_id: String,
    pub invocation_name: String,
    pub display_name: String,
    pub category: String,
    pub summary: String,
    pub usage: String,
    pub arguments: Vec<TaskUIArgument>,
    pub managed_output: Option<TaskUIManagedOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct TaskUIManagedOutput {
    pub decoder: String,
    pub stdout_format: String,
    pub inject_arguments: Vec<TaskUIInjectedArgument>,
    pub raw_stdout_available: bool,
    pub raw_stderr_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct TaskUIInjectedArgument {
    pub flag: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct TaskUIArgument {
    pub id: String,
    pub label: String,
    pub order: i64,
    pub parser: TaskUIArgumentParser,
    pub value_kind: String,
    pub required: bool,
    pub default: Option<String>,
    pub help: String,
    pub group: String,
    pub parameter_type: Option<String>,
    pub concept_id: Option<String>,
    pub concept_revision: Option<u64>,
    pub unit_dimension: Option<String>,
    pub context_role: Option<String>,
    pub advanced: bool,
    pub hidden_in_tui: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct TaskUIArgumentParser {
    pub kind: String,
    pub flags: Option<Vec<String>>,
    pub metavar: Option<String>,
    pub choices: Option<Vec<String>>,
    pub true_flags: Option<Vec<String>>,
    pub false_flags: Option<Vec<String>>,
    pub action: Option<String>,
    pub positional_metavar: Option<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetUvRangeProbe {
    pub min_meters: f64,
    pub max_meters: f64,
    pub min_kilolambda: f64,
    pub max_kilolambda: f64,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetTimeRangeProbe {
    pub min_seconds: f64,
    pub max_seconds: f64,
    pub row_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum MeasurementSetPlotPreset {
    UvCoverage,
    AntennaLayout,
    ScanTimeline,
    SpectralWindowCoverage,
    PhaseVsTime,
    AmplitudePhaseVsTimeStacked,
    WeightVsTime,
    SigmaVsTime,
    FlagVsTime,
    WeightSpectrumVsTime,
    SigmaSpectrumVsTime,
    FlagRowVsTime,
    ElevationVsTime,
    AzimuthVsTime,
    HourAngleVsTime,
    ParallacticAngleVsTime,
    AzimuthVsElevation,
    AmplitudeVsFrequency,
    AmplitudeVsChannel,
    PhaseVsChannel,
    PhaseVsFrequency,
    AmplitudeVsVelocity,
    PhaseVsVelocity,
    AmplitudeVsUvDistance,
    AmplitudeVsTime,
    RealVsImaginary,
}

#[cfg(test)]
impl MeasurementSetPlotPreset {
    fn all() -> &'static [Self] {
        &[
            Self::UvCoverage,
            Self::AntennaLayout,
            Self::ScanTimeline,
            Self::SpectralWindowCoverage,
            Self::AmplitudeVsTime,
            Self::PhaseVsTime,
            Self::AmplitudePhaseVsTimeStacked,
            Self::AmplitudeVsUvDistance,
            Self::WeightVsTime,
            Self::SigmaVsTime,
            Self::FlagVsTime,
            Self::WeightSpectrumVsTime,
            Self::SigmaSpectrumVsTime,
            Self::FlagRowVsTime,
            Self::ElevationVsTime,
            Self::AzimuthVsTime,
            Self::HourAngleVsTime,
            Self::ParallacticAngleVsTime,
            Self::AzimuthVsElevation,
            Self::AmplitudeVsChannel,
            Self::PhaseVsChannel,
            Self::AmplitudeVsFrequency,
            Self::PhaseVsFrequency,
            Self::AmplitudeVsVelocity,
            Self::PhaseVsVelocity,
            Self::RealVsImaginary,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetPlotRequest {
    pub dataset_path: String,
    pub preset: MeasurementSetPlotPreset,
    pub field: Option<String>,
    pub spectral_window: Option<String>,
    pub timerange: Option<String>,
    pub uvrange: Option<String>,
    pub antenna: Option<String>,
    pub scan: Option<String>,
    pub correlation: Option<String>,
    pub array: Option<String>,
    pub observation: Option<String>,
    pub intent: Option<String>,
    pub feed: Option<String>,
    pub msselect: Option<String>,
    pub data_column: String,
    pub color_by: Option<String>,
    pub avgchannel: Option<u64>,
    pub avgtime: Option<f64>,
    pub avgscan: bool,
    pub avgfield: bool,
    pub avgbaseline: bool,
    pub avgantenna: bool,
    pub avgspw: bool,
    pub scalar: bool,
    pub iteraxis: Option<String>,
    pub width: u32,
    pub height: u32,
    pub max_plot_points: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetSummaryRequest {
    pub dataset_path: String,
    pub format: String,
    pub field: Option<String>,
    pub spectral_window: Option<String>,
    pub timerange: Option<String>,
    pub uvrange: Option<String>,
    pub antenna: Option<String>,
    pub scan: Option<String>,
    pub correlation: Option<String>,
    pub array: Option<String>,
    pub observation: Option<String>,
    pub intent: Option<String>,
    pub feed: Option<String>,
    pub msselect: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MeasurementSetSummaryResult {
    pub dataset_path: String,
    pub format: String,
    pub summary_text: String,
    pub selection_summary: String,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotAxisMetadata {
    pub id: String,
    pub label: String,
    pub unit: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum PlotAxisScale {
    Linear,
    Log,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum PlotLayerKind {
    Scatter,
    Line,
    Interval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum PlotPayloadStrategy {
    PointCloud,
    Intervals,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentAxis {
    pub id: String,
    pub label: String,
    pub unit: String,
    pub lower: f64,
    pub upper: f64,
    pub scale: PlotAxisScale,
    pub lane_labels: Vec<String>,
    pub draws_on_trailing_edge: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PlotPointProvenance {
    pub row: u64,
    pub corr: u64,
    pub chan_start: u64,
    pub chan_end: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentLayer {
    pub id: String,
    pub title: String,
    pub kind: PlotLayerKind,
    pub x_axis_id: String,
    pub y_axis_id: String,
    pub x_values: Vec<f64>,
    pub y_values: Vec<f64>,
    pub point_labels: Vec<String>,
    pub point_symbol_sizes: Vec<f64>,
    pub interval_x_start: Vec<f64>,
    pub interval_x_end: Vec<f64>,
    pub interval_y: Vec<f64>,
    pub interval_height: Vec<f64>,
    pub interval_labels: Vec<String>,
    pub provenance: Vec<PlotPointProvenance>,
    pub color_group: String,
    pub symbol_size: f64,
    pub line_width: f64,
    pub opacity: f64,
    pub source_sample_count: u64,
    pub payload_strategy: PlotPayloadStrategy,
    pub provenance_summary: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentAnnotation {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentPanel {
    pub id: String,
    pub title: String,
    pub axes: Vec<PlotDocumentAxis>,
    pub layers: Vec<PlotDocumentLayer>,
    pub annotations: Vec<PlotDocumentAnnotation>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentPayload {
    pub id: String,
    pub title: String,
    pub subtitle: String,
    pub header_lines: Vec<String>,
    pub axes: Vec<PlotDocumentAxis>,
    pub layers: Vec<PlotDocumentLayer>,
    pub annotations: Vec<PlotDocumentAnnotation>,
    pub panels: Vec<PlotDocumentPanel>,
    pub show_legend: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PlotSeriesMetadata {
    pub label: String,
    pub color_group: String,
    pub point_count: u64,
    pub first_row: Option<u64>,
    pub last_row: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PlotSamplingDiagnostics {
    pub requested_max_points: u64,
    pub rendered_point_count: u64,
    pub series_count: u64,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PlotRenderProvenance {
    pub renderer: String,
    pub image_format: String,
    pub width: u32,
    pub height: u32,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetPlotResult {
    pub preset: MeasurementSetPlotPreset,
    pub preset_label: String,
    pub title: String,
    pub summary: String,
    pub dataset_path: String,
    pub data_column: String,
    pub selection_summary: String,
    pub x_axis: PlotAxisMetadata,
    pub y_axis: PlotAxisMetadata,
    pub series: Vec<PlotSeriesMetadata>,
    pub sampling: PlotSamplingDiagnostics,
    pub document: PlotDocumentPayload,
    pub render: PlotRenderProvenance,
    pub image_bytes: Vec<u8>,
}

#[derive(Debug, Error, uniffi::Error)]
pub enum FrontendServiceError {
    #[error("invalid path: {reason}")]
    InvalidPath { reason: String },
    #[error("I/O error: {reason}")]
    Io { reason: String },
    #[error("probe failed: {reason}")]
    Probe { reason: String },
    #[error("plot failed: {reason}")]
    Plot { reason: String },
    #[error("image explorer failed: {reason}")]
    ImageExplorer { reason: String },
    #[error("table explorer failed: {reason}")]
    TableExplorer { reason: String },
    #[error("parameter service failed: {reason}")]
    Parameters { reason: String },
    #[error("notebook service failed: {reason}")]
    Notebook { reason: String },
    #[error("tutorial service failed: {reason}")]
    Tutorial { reason: String },
    #[error("assistant service failed: {reason}")]
    Assistant { reason: String },
    #[error("assistant corpus service failed: {reason}")]
    Corpus { reason: String },
    #[error("task completion failed: {reason}")]
    TaskCompletion { reason: String },
}

type FrontendResult<T> = Result<T, FrontendServiceError>;

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Enum)]
pub enum NotebookValue {
    String { value: String },
    Number { value: f64 },
    Bool { value: bool },
    Array { values: Vec<NotebookValue> },
    Object { entries: Vec<NotebookValueEntry> },
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookValueEntry {
    pub name: String,
    pub value: NotebookValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookTaskIntent {
    pub format: u32,
    pub surface: String,
    pub kind: String,
    pub contract: u32,
    pub parameters: HashMap<String, NotebookValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct NotebookReceiptArtifact {
    pub role: String,
    pub path: String,
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct NotebookPythonEnvironmentIdentity {
    pub environment_id: String,
    pub interpreter: String,
    pub implementation: String,
    pub version: String,
    pub casa_rs_version: Option<String>,
    pub packages: HashMap<String, String>,
    pub fingerprint_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct NotebookPythonExecutionInput {
    pub source: String,
    pub source_sha256: String,
    pub authority: String,
    pub input_references: Vec<String>,
    pub environment: NotebookPythonEnvironmentIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct NotebookExecutionInput {
    pub kind: String,
    pub details: NotebookPythonExecutionInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct NotebookPythonOutputEvent {
    pub order: i64,
    pub channel: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookExecutionReceipt {
    pub schema_version: u32,
    pub run_id: String,
    pub revision: u64,
    pub notebook_id: String,
    pub cell_id: String,
    pub initiating_surface: String,
    pub operation_id: String,
    pub started_at: u64,
    pub finished_at: u64,
    pub status: String,
    pub sparse_intent: Option<NotebookTaskIntent>,
    pub execution_input: Option<NotebookExecutionInput>,
    pub ordered_outputs: Option<Vec<NotebookPythonOutputEvent>>,
    pub resolved_parameters: HashMap<String, NotebookValue>,
    pub provider_contract_version: u32,
    pub affected_paths: Vec<String>,
    pub products: Vec<NotebookReceiptArtifact>,
    pub artifacts: Vec<NotebookReceiptArtifact>,
    pub diagnostics: Vec<String>,
    pub replay_claim: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookCellState {
    pub id: String,
    pub kind: String,
    pub body: String,
    pub task_intent: Option<NotebookTaskIntent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookVisualizationReopenIntent {
    pub surface: String,
    pub contract_version: u32,
    pub parameters: HashMap<String, NotebookValue>,
    pub profile_toml: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookVisualizationRenderMetadata {
    pub renderer: String,
    pub media_type: String,
    pub width: u32,
    pub height: u32,
    pub settings: HashMap<String, NotebookValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookVisualizationRevision {
    pub revision: u64,
    pub created_at: u64,
    pub asset_path: String,
    pub source_references: Vec<String>,
    pub reopen: NotebookVisualizationReopenIntent,
    pub render: NotebookVisualizationRenderMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookVisualizationSnapshot {
    pub schema_version: u32,
    pub id: String,
    pub notebook_id: String,
    pub cell_id: String,
    pub title: String,
    pub revisions: Vec<NotebookVisualizationRevision>,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct NotebookDocumentProjection {
    pub id: String,
    pub filename: String,
    pub source: String,
    pub content_hash: String,
    pub cells: Vec<NotebookCellState>,
    pub receipts: Vec<NotebookExecutionReceipt>,
    pub visualizations: Vec<NotebookVisualizationSnapshot>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct ScientificNotebookProjectProjection {
    pub schema_version: u32,
    pub project_root: String,
    pub notebooks: Vec<NotebookDocumentProjection>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum NotebookSaveProjection {
    Saved {
        notebook: NotebookDocumentProjection,
    },
    Reloaded {
        notebook: NotebookDocumentProjection,
    },
    Conflict {
        base_hash: String,
        external: NotebookDocumentProjection,
        proposed_source: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum NotebookConflictResolution {
    Reject,
    KeepLocal,
    ReloadExternal,
}

impl From<NotebookConflictResolution> for ConflictResolution {
    fn from(value: NotebookConflictResolution) -> Self {
        match value {
            NotebookConflictResolution::Reject => Self::Reject,
            NotebookConflictResolution::KeepLocal => Self::KeepLocal,
            NotebookConflictResolution::ReloadExternal => Self::ReloadExternal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NotebookCreateRequest {
    pub project_root: String,
    pub filename: Option<String>,
    pub title: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NotebookSaveRequest {
    pub project_root: String,
    pub filename: String,
    pub base_hash: String,
    pub source: String,
    pub resolution: NotebookConflictResolution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum NotebookRecordingPolicy {
    Record,
    BypassOnce,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct NotebookRunSafetyRecord {
    pub classification: String,
    pub affected_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NotebookApprovalRecord {
    pub kind: String,
    pub actor: String,
    pub timestamp: u64,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct NotebookRecordingRequest {
    pub initiating_surface: String,
    pub operation_id: String,
    pub notebook_id: Option<String>,
    pub cell_id: Option<String>,
    pub task_intent: Option<NotebookTaskIntent>,
    pub execution_input: Option<NotebookExecutionInput>,
    pub provider_contract_version: u32,
    pub resolved_parameters: HashMap<String, NotebookValue>,
    pub run_safety: NotebookRunSafetyRecord,
    pub approvals: Vec<NotebookApprovalRecord>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct NotebookBeginRecordingRequest {
    pub project_root: String,
    pub policy: NotebookRecordingPolicy,
    pub request: NotebookRecordingRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NotebookAttemptHandle {
    pub run_id: String,
    pub revision: u64,
    pub notebook_id: String,
    pub cell_id: String,
    pub started_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NotebookBeginRecordingResult {
    pub handle: Option<NotebookAttemptHandle>,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NotebookReceiptFinalization {
    pub status: String,
    pub finished_at: u64,
    pub affected_paths: Vec<String>,
    pub products: Vec<NotebookReceiptArtifact>,
    pub artifacts: Vec<NotebookReceiptArtifact>,
    pub diagnostics: Vec<String>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub casa_log: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NotebookFinalizeRecordingRequest {
    pub project_root: String,
    pub handle: NotebookAttemptHandle,
    pub finalization: NotebookReceiptFinalization,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct NotebookSaveVisualizationRequest {
    pub notebook_id: Option<String>,
    pub visualization_id: Option<String>,
    pub title: String,
    pub source_asset: String,
    pub source_references: Vec<String>,
    pub reopen: NotebookVisualizationReopenIntent,
    pub render: NotebookVisualizationRenderMetadata,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct NotebookSaveVisualizationEnvelope {
    pub project_root: String,
    pub request: NotebookSaveVisualizationRequest,
}

impl From<NotebookRecordingPolicy> for RecordingPolicy {
    fn from(value: NotebookRecordingPolicy) -> Self {
        match value {
            NotebookRecordingPolicy::Record => Self::Record,
            NotebookRecordingPolicy::BypassOnce => Self::BypassOnce,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum NotebookExportMode {
    Portable,
    AdvancedWithReceipts,
}

impl From<NotebookExportMode> for ExportMode {
    fn from(value: NotebookExportMode) -> Self {
        match value {
            NotebookExportMode::Portable => Self::Portable,
            NotebookExportMode::AdvancedWithReceipts => Self::AdvancedWithReceipts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NotebookExportRequest {
    pub project_root: String,
    pub destination: String,
    pub mode: NotebookExportMode,
}

#[derive(Debug, Deserialize)]
struct AssistantCreateConversationRequest {
    project_root: String,
    title: String,
    primary_attachment: AssistantAttachment,
    profile: AssistantSessionProfile,
}

#[derive(Debug, Deserialize)]
struct AssistantConversationRequest {
    project_root: String,
    conversation_id: ConversationId,
}

#[derive(Debug, Deserialize)]
struct AssistantSaveConversationRequest {
    project_root: String,
    transcript: ConversationTranscript,
}

#[derive(Debug, Deserialize)]
struct AssistantCreatePinRequest {
    conversation_id: ConversationId,
    notebook_id: NotebookId,
    message_id: AssistantMessageId,
    representation: String,
    snapshot_content: String,
}

#[derive(Debug, Deserialize)]
struct AssistantCorpusIndexRequest {
    project_root: String,
    documents: Vec<CorpusDocumentInput>,
    #[serde(default)]
    remove_missing_layers: BTreeSet<CorpusLayer>,
    #[serde(default)]
    project_sources: Option<Vec<ProjectCorpusSource>>,
    #[serde(default)]
    failed_project_sources: BTreeSet<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct AssistantProjectCorpusPlanRequest {
    project_root: String,
    sources: Vec<ProjectCorpusSource>,
}

#[derive(Debug, Deserialize)]
struct AssistantCorpusSearchRequest {
    project_root: String,
    query: String,
    #[serde(default = "default_assistant_corpus_search_limit")]
    limit: usize,
    #[serde(default)]
    layers: BTreeSet<CorpusLayer>,
}

const fn default_assistant_corpus_search_limit() -> usize {
    8
}

// Protocol-plane bounds keep one MCP request/result finite. They do not cap
// indexed documents, source size, scientific data, or downloads.
const MAX_ASSISTANT_CORPUS_QUERY_BYTES: usize = 4_096;
const MAX_ASSISTANT_CORPUS_SEARCH_RESULTS: usize = 32;

#[derive(Debug, Serialize)]
struct AssistantProtocolProjection {
    profile_version: u32,
    transcript_schema_version: u32,
    corpus_schema_version: u32,
    retrieval_engine: &'static str,
    backend_session_binding: &'static str,
    authority_presets: Vec<&'static str>,
    project_mcp_tools: Vec<&'static str>,
}

#[derive(Debug, Deserialize)]
struct AssistantTaskSuggestionWire {
    kind: String,
    task_id: String,
    parameters: HashMap<String, String>,
    validated_patch: ResolutionPatch,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct AssistantTaskSuggestionProjection {
    pub task_id: String,
    pub parameters: HashMap<String, String>,
    pub validated_patch: SurfaceParameterPatch,
}

#[derive(Debug, Deserialize)]
struct TutorialForkRequest {
    project_root: String,
    template_path: String,
    filename: String,
}

#[derive(Debug, Deserialize)]
struct TutorialMigrateRequest {
    pack_path: String,
    destination: String,
}

#[derive(Debug, Deserialize)]
struct TutorialNotebookRequest {
    project_root: String,
    notebook_id: casa_notebook::NotebookId,
}

#[derive(Debug, Deserialize)]
struct TutorialPlanRequest {
    project_root: String,
    notebook_id: casa_notebook::NotebookId,
    dataset_id: String,
    source_override: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TutorialBeginRequest {
    project_root: String,
    plan: casa_notebook::TutorialAcquisitionPlan,
    approval: TutorialAcquisitionApproval,
}

#[derive(Debug, Deserialize)]
struct TutorialActionRequest {
    project_root: String,
    notebook_id: casa_notebook::NotebookId,
    dataset_id: String,
}

#[derive(Debug, Deserialize)]
struct TutorialGenerationActionRequest {
    project_root: String,
    notebook_id: casa_notebook::NotebookId,
    dataset_id: String,
    generation: u64,
    #[serde(default = "default_tutorial_download_chunk")]
    max_download_bytes: u64,
}

const fn default_tutorial_download_chunk() -> u64 {
    1024 * 1024
}

#[derive(Serialize)]
struct TutorialTemplateProjection<'a> {
    root: String,
    content_sha256: &'a str,
    manifest: &'a casa_notebook::TutorialManifest,
}

#[derive(Serialize)]
struct TutorialForkProjection {
    notebook: NotebookDocumentProjection,
    tutorial: casa_notebook::TutorialLock,
}

fn notebook_error(action: &str, error: impl std::fmt::Display) -> FrontendServiceError {
    FrontendServiceError::Notebook {
        reason: format!("{action}: {error}"),
    }
}

fn tutorial_error(action: &str, error: impl std::fmt::Display) -> FrontendServiceError {
    FrontendServiceError::Tutorial {
        reason: format!("{action}: {error}"),
    }
}

fn assistant_error(action: &str, error: impl std::fmt::Display) -> FrontendServiceError {
    FrontendServiceError::Assistant {
        reason: format!("{action}: {error}"),
    }
}

fn corpus_error(action: &str, error: impl std::fmt::Display) -> FrontendServiceError {
    FrontendServiceError::Corpus {
        reason: format!("{action}: {error}"),
    }
}

fn parse_tutorial_request<T: for<'de> Deserialize<'de>>(
    action: &str,
    request_json: &str,
) -> FrontendResult<T> {
    serde_json::from_str(request_json).map_err(|error| tutorial_error(action, error))
}

fn serialize_tutorial<T: Serialize>(action: &str, value: &T) -> FrontendResult<String> {
    serde_json::to_string(value).map_err(|error| tutorial_error(action, error))
}

fn notebook_value_from_json(value: &serde_json::Value) -> FrontendResult<NotebookValue> {
    match value {
        serde_json::Value::String(value) => Ok(NotebookValue::String {
            value: value.clone(),
        }),
        serde_json::Value::Number(value) => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(|value| NotebookValue::Number { value })
            .ok_or_else(|| notebook_error("project notebook value", "number is not finite")),
        serde_json::Value::Bool(value) => Ok(NotebookValue::Bool { value: *value }),
        serde_json::Value::Array(values) => Ok(NotebookValue::Array {
            values: values
                .iter()
                .map(notebook_value_from_json)
                .collect::<FrontendResult<Vec<_>>>()?,
        }),
        serde_json::Value::Object(values) => Ok(NotebookValue::Object {
            entries: values
                .iter()
                .map(|(name, value)| {
                    Ok(NotebookValueEntry {
                        name: name.clone(),
                        value: notebook_value_from_json(value)?,
                    })
                })
                .collect::<FrontendResult<Vec<_>>>()?,
        }),
        serde_json::Value::Null => Ok(NotebookValue::Null),
    }
}

fn notebook_value_into_json(value: NotebookValue) -> FrontendResult<serde_json::Value> {
    match value {
        NotebookValue::String { value } => Ok(serde_json::Value::String(value)),
        NotebookValue::Number { value } => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| notebook_error("convert notebook value", "number is not finite")),
        NotebookValue::Bool { value } => Ok(serde_json::Value::Bool(value)),
        NotebookValue::Array { values } => Ok(serde_json::Value::Array(
            values
                .into_iter()
                .map(notebook_value_into_json)
                .collect::<FrontendResult<Vec<_>>>()?,
        )),
        NotebookValue::Object { entries } => {
            let mut values = serde_json::Map::new();
            for entry in entries {
                if values.contains_key(&entry.name) {
                    return Err(notebook_error(
                        "convert notebook value",
                        format!("duplicate object member `{}`", entry.name),
                    ));
                }
                values.insert(entry.name, notebook_value_into_json(entry.value)?);
            }
            Ok(serde_json::Value::Object(values))
        }
        NotebookValue::Null => Ok(serde_json::Value::Null),
    }
}

fn notebook_value_from_toml(value: &toml::Value) -> FrontendResult<NotebookValue> {
    match value {
        toml::Value::String(value) => Ok(NotebookValue::String {
            value: value.clone(),
        }),
        toml::Value::Integer(value) => Ok(NotebookValue::Number {
            value: *value as f64,
        }),
        toml::Value::Float(value) if value.is_finite() => {
            Ok(NotebookValue::Number { value: *value })
        }
        toml::Value::Float(_) => Err(notebook_error(
            "project notebook task intent",
            "number is not finite",
        )),
        toml::Value::Boolean(value) => Ok(NotebookValue::Bool { value: *value }),
        toml::Value::Datetime(value) => Ok(NotebookValue::String {
            value: value.to_string(),
        }),
        toml::Value::Array(values) => Ok(NotebookValue::Array {
            values: values
                .iter()
                .map(notebook_value_from_toml)
                .collect::<FrontendResult<Vec<_>>>()?,
        }),
        toml::Value::Table(values) => Ok(NotebookValue::Object {
            entries: values
                .iter()
                .map(|(name, value)| {
                    Ok(NotebookValueEntry {
                        name: name.clone(),
                        value: notebook_value_from_toml(value)?,
                    })
                })
                .collect::<FrontendResult<Vec<_>>>()?,
        }),
    }
}

fn notebook_value_into_toml(value: NotebookValue) -> FrontendResult<toml::Value> {
    match value {
        NotebookValue::String { value } => Ok(toml::Value::String(value)),
        NotebookValue::Number { value } if !value.is_finite() => Err(notebook_error(
            "convert notebook task intent",
            "number is not finite",
        )),
        NotebookValue::Number { value }
            if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 =>
        {
            Ok(toml::Value::Integer(value as i64))
        }
        NotebookValue::Number { value } => Ok(toml::Value::Float(value)),
        NotebookValue::Bool { value } => Ok(toml::Value::Boolean(value)),
        NotebookValue::Array { values } => Ok(toml::Value::Array(
            values
                .into_iter()
                .map(notebook_value_into_toml)
                .collect::<FrontendResult<Vec<_>>>()?,
        )),
        NotebookValue::Object { entries } => {
            let mut values = toml::map::Map::new();
            for entry in entries {
                if values.contains_key(&entry.name) {
                    return Err(notebook_error(
                        "convert notebook task intent",
                        format!("duplicate table member `{}`", entry.name),
                    ));
                }
                values.insert(entry.name, notebook_value_into_toml(entry.value)?);
            }
            Ok(toml::Value::Table(values))
        }
        NotebookValue::Null => Err(notebook_error(
            "convert notebook task intent",
            "TOML task parameters cannot contain null",
        )),
    }
}

fn notebook_task_intent_projection(intent: &TaskCellIntent) -> FrontendResult<NotebookTaskIntent> {
    Ok(NotebookTaskIntent {
        format: intent.format,
        surface: intent.surface.clone(),
        kind: intent.kind.clone(),
        contract: intent.contract,
        parameters: intent
            .parameters
            .iter()
            .map(|(name, value)| Ok((name.clone(), notebook_value_from_toml(value)?)))
            .collect::<FrontendResult<HashMap<_, _>>>()?,
    })
}

fn notebook_task_intent_owner(intent: NotebookTaskIntent) -> FrontendResult<TaskCellIntent> {
    Ok(TaskCellIntent {
        format: intent.format,
        surface: intent.surface,
        kind: intent.kind,
        contract: intent.contract,
        parameters: intent
            .parameters
            .into_iter()
            .map(|(name, value)| Ok((name, notebook_value_into_toml(value)?)))
            .collect::<FrontendResult<BTreeMap<_, _>>>()?,
    })
}

fn notebook_execution_input_projection(input: &ExecutionInput) -> NotebookExecutionInput {
    match input {
        ExecutionInput::Python(input) => NotebookExecutionInput {
            kind: "python".to_owned(),
            details: NotebookPythonExecutionInput {
                source: input.source.clone(),
                source_sha256: input.source_sha256.clone(),
                authority: match input.authority {
                    PythonExecutionAuthority::User => "user",
                    PythonExecutionAuthority::AiWorker => "ai_worker",
                }
                .to_owned(),
                input_references: input
                    .input_references
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect(),
                environment: NotebookPythonEnvironmentIdentity {
                    environment_id: input.environment.environment_id.clone(),
                    interpreter: input.environment.interpreter.display().to_string(),
                    implementation: input.environment.implementation.clone(),
                    version: input.environment.version.clone(),
                    casa_rs_version: input.environment.casa_rs_version.clone(),
                    packages: input.environment.packages.clone().into_iter().collect(),
                    fingerprint_sha256: input.environment.fingerprint_sha256.clone(),
                },
            },
        },
    }
}

fn notebook_execution_input_owner(input: NotebookExecutionInput) -> FrontendResult<ExecutionInput> {
    if input.kind != "python" {
        return Err(notebook_error(
            "convert notebook execution input",
            format!("unsupported execution input kind `{}`", input.kind),
        ));
    }
    let authority = match input.details.authority.as_str() {
        "user" => PythonExecutionAuthority::User,
        "ai_worker" => PythonExecutionAuthority::AiWorker,
        value => {
            return Err(notebook_error(
                "convert notebook execution input",
                format!("unsupported Python authority `{value}`"),
            ));
        }
    };
    let execution = ExecutionInput::Python(PythonExecutionInput {
        source: input.details.source,
        source_sha256: input.details.source_sha256,
        authority,
        input_references: input
            .details
            .input_references
            .into_iter()
            .map(PathBuf::from)
            .collect(),
        environment: PythonEnvironmentIdentity {
            environment_id: input.details.environment.environment_id,
            interpreter: PathBuf::from(input.details.environment.interpreter),
            implementation: input.details.environment.implementation,
            version: input.details.environment.version,
            casa_rs_version: input.details.environment.casa_rs_version,
            packages: input.details.environment.packages.into_iter().collect(),
            fingerprint_sha256: input.details.environment.fingerprint_sha256,
        },
    });
    if let Some(reason) = execution.validation_error() {
        return Err(notebook_error("validate notebook execution input", reason));
    }
    Ok(execution)
}

fn notebook_artifact_projection(
    artifact: &casa_notebook::ArtifactReference,
) -> NotebookReceiptArtifact {
    NotebookReceiptArtifact {
        role: artifact.role.clone(),
        path: artifact.path.display().to_string(),
        media_type: artifact.media_type.clone(),
    }
}

fn notebook_artifact_owner(artifact: NotebookReceiptArtifact) -> casa_notebook::ArtifactReference {
    casa_notebook::ArtifactReference {
        role: artifact.role,
        path: PathBuf::from(artifact.path),
        media_type: artifact.media_type,
    }
}

fn notebook_visualization_projection(
    visualization: &VisualizationSnapshot,
) -> FrontendResult<NotebookVisualizationSnapshot> {
    Ok(NotebookVisualizationSnapshot {
        schema_version: visualization.schema_version,
        id: visualization.id.to_string(),
        notebook_id: visualization.notebook_id.to_string(),
        cell_id: visualization.cell_id.to_string(),
        title: visualization.title.clone(),
        revisions: visualization
            .revisions
            .iter()
            .map(|revision| {
                Ok(NotebookVisualizationRevision {
                    revision: revision.revision,
                    created_at: revision.created_at.0,
                    asset_path: revision.asset_path.display().to_string(),
                    source_references: revision
                        .source_references
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect(),
                    reopen: NotebookVisualizationReopenIntent {
                        surface: revision.reopen.surface.clone(),
                        contract_version: revision.reopen.contract_version,
                        parameters: revision
                            .reopen
                            .parameters
                            .iter()
                            .map(|(name, value)| {
                                Ok((name.clone(), notebook_value_from_json(value)?))
                            })
                            .collect::<FrontendResult<HashMap<_, _>>>()?,
                        profile_toml: revision.reopen.profile_toml.clone(),
                    },
                    render: NotebookVisualizationRenderMetadata {
                        renderer: revision.render.renderer.clone(),
                        media_type: revision.render.media_type.clone(),
                        width: revision.render.width,
                        height: revision.render.height,
                        settings: revision
                            .render
                            .settings
                            .iter()
                            .map(|(name, value)| {
                                Ok((name.clone(), notebook_value_from_json(value)?))
                            })
                            .collect::<FrontendResult<HashMap<_, _>>>()?,
                    },
                })
            })
            .collect::<FrontendResult<Vec<_>>>()?,
    })
}

fn notebook_visualization_request_owner(
    request: NotebookSaveVisualizationRequest,
) -> FrontendResult<SaveVisualizationRequest> {
    Ok(SaveVisualizationRequest {
        notebook_id: request
            .notebook_id
            .map(|value| value.parse::<NotebookId>())
            .transpose()
            .map_err(|error| notebook_error("parse visualization notebook ID", error))?,
        visualization_id: request
            .visualization_id
            .map(|value| value.parse())
            .transpose()
            .map_err(|error| notebook_error("parse visualization ID", error))?,
        title: request.title,
        source_asset: PathBuf::from(request.source_asset),
        source_references: request
            .source_references
            .into_iter()
            .map(PathBuf::from)
            .collect(),
        reopen: VisualizationReopenIntent {
            surface: request.reopen.surface,
            contract_version: request.reopen.contract_version,
            parameters: request
                .reopen
                .parameters
                .into_iter()
                .map(|(name, value)| Ok((name, notebook_value_into_json(value)?)))
                .collect::<FrontendResult<BTreeMap<_, _>>>()?,
            profile_toml: request.reopen.profile_toml,
        },
        render: VisualizationRenderMetadata {
            renderer: request.render.renderer,
            media_type: request.render.media_type,
            width: request.render.width,
            height: request.render.height,
            settings: request
                .render
                .settings
                .into_iter()
                .map(|(name, value)| Ok((name, notebook_value_into_json(value)?)))
                .collect::<FrontendResult<BTreeMap<_, _>>>()?,
        },
    })
}

fn notebook_status_name(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Succeeded => "succeeded",
        ExecutionStatus::Failed => "failed",
        ExecutionStatus::Cancelled => "cancelled",
        ExecutionStatus::Interrupted => "interrupted",
    }
}

fn notebook_status_owner(status: &str) -> FrontendResult<ExecutionStatus> {
    match status {
        "succeeded" => Ok(ExecutionStatus::Succeeded),
        "failed" => Ok(ExecutionStatus::Failed),
        "cancelled" => Ok(ExecutionStatus::Cancelled),
        "interrupted" => Ok(ExecutionStatus::Interrupted),
        value => Err(notebook_error(
            "convert notebook execution status",
            format!("unsupported status `{value}`"),
        )),
    }
}

fn notebook_receipt_projection(
    receipt: &ExecutionReceipt,
    ordered_outputs: Option<Vec<NotebookPythonOutputEvent>>,
) -> FrontendResult<NotebookExecutionReceipt> {
    Ok(NotebookExecutionReceipt {
        schema_version: receipt.schema_version,
        run_id: receipt.run_id.to_string(),
        revision: receipt.revision,
        notebook_id: receipt.notebook_id.to_string(),
        cell_id: receipt.cell_id.to_string(),
        initiating_surface: receipt.initiating_surface.clone(),
        operation_id: receipt.operation_id.clone(),
        started_at: receipt.started_at.0,
        finished_at: receipt.finished_at.0,
        status: notebook_status_name(receipt.status).to_owned(),
        sparse_intent: receipt
            .sparse_intent
            .as_ref()
            .map(notebook_task_intent_projection)
            .transpose()?,
        execution_input: receipt
            .execution_input
            .as_ref()
            .map(notebook_execution_input_projection),
        ordered_outputs,
        resolved_parameters: receipt
            .resolved_parameters
            .iter()
            .map(|(name, value)| Ok((name.clone(), notebook_value_from_json(value)?)))
            .collect::<FrontendResult<HashMap<_, _>>>()?,
        provider_contract_version: receipt.provider_contract_version,
        affected_paths: receipt
            .affected_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        products: receipt
            .products
            .iter()
            .map(notebook_artifact_projection)
            .collect(),
        artifacts: receipt
            .artifacts
            .iter()
            .map(notebook_artifact_projection)
            .collect(),
        diagnostics: receipt.diagnostics.clone(),
        replay_claim: receipt.replay_claim.clone(),
    })
}

fn notebook_cell_projections(
    document: &NotebookDocument,
) -> FrontendResult<Vec<NotebookCellState>> {
    document
        .cells()
        .iter()
        .map(|cell| {
            Ok(NotebookCellState {
                id: cell.id.to_string(),
                kind: cell.kind.as_str().to_owned(),
                body: document.source()[cell.body_range.clone()].to_owned(),
                task_intent: cell
                    .task
                    .as_ref()
                    .map(notebook_task_intent_projection)
                    .transpose()?,
            })
        })
        .collect()
}

fn notebook_recording_request_owner(
    request: NotebookRecordingRequest,
) -> FrontendResult<RecordingRequest> {
    Ok(RecordingRequest {
        initiating_surface: request.initiating_surface,
        operation_id: request.operation_id,
        notebook_id: request
            .notebook_id
            .map(|value| value.parse::<NotebookId>())
            .transpose()
            .map_err(|error| notebook_error("parse recording notebook ID", error))?,
        cell_id: request
            .cell_id
            .map(|value| value.parse::<CellId>())
            .transpose()
            .map_err(|error| notebook_error("parse recording cell ID", error))?,
        task_intent: request
            .task_intent
            .map(notebook_task_intent_owner)
            .transpose()?,
        execution_input: request
            .execution_input
            .map(notebook_execution_input_owner)
            .transpose()?,
        provider_contract_version: request.provider_contract_version,
        resolved_parameters: request
            .resolved_parameters
            .into_iter()
            .map(|(name, value)| Ok((name, notebook_value_into_json(value)?)))
            .collect::<FrontendResult<BTreeMap<_, _>>>()?,
        run_safety: casa_notebook::RunSafetyRecord {
            classification: request.run_safety.classification,
            affected_paths: request
                .run_safety
                .affected_paths
                .into_iter()
                .map(PathBuf::from)
                .collect(),
        },
        approvals: request
            .approvals
            .into_iter()
            .map(|approval| casa_notebook::ApprovalRecord {
                kind: approval.kind,
                actor: approval.actor,
                timestamp: Timestamp(approval.timestamp),
                content_hash: approval.content_hash,
            })
            .collect(),
    })
}

fn notebook_attempt_handle_projection(handle: &AttemptHandle) -> NotebookAttemptHandle {
    NotebookAttemptHandle {
        run_id: handle.run_id.to_string(),
        revision: handle.revision,
        notebook_id: handle.notebook_id.to_string(),
        cell_id: handle.cell_id.to_string(),
        started_at: handle.started_at.0,
    }
}

fn notebook_attempt_handle_owner(handle: NotebookAttemptHandle) -> FrontendResult<AttemptHandle> {
    Ok(AttemptHandle {
        run_id: handle
            .run_id
            .parse::<RunId>()
            .map_err(|error| notebook_error("parse recording run ID", error))?,
        notebook_id: handle
            .notebook_id
            .parse::<NotebookId>()
            .map_err(|error| notebook_error("parse recording notebook ID", error))?,
        cell_id: handle
            .cell_id
            .parse::<CellId>()
            .map_err(|error| notebook_error("parse recording cell ID", error))?,
        revision: handle.revision,
        started_at: Timestamp(handle.started_at),
    })
}

fn notebook_finalization_owner(
    finalization: NotebookReceiptFinalization,
) -> FrontendResult<ReceiptFinalization> {
    Ok(ReceiptFinalization {
        status: notebook_status_owner(&finalization.status)?,
        finished_at: Timestamp(finalization.finished_at),
        affected_paths: finalization
            .affected_paths
            .into_iter()
            .map(PathBuf::from)
            .collect(),
        products: finalization
            .products
            .into_iter()
            .map(notebook_artifact_owner)
            .collect(),
        artifacts: finalization
            .artifacts
            .into_iter()
            .map(notebook_artifact_owner)
            .collect(),
        diagnostics: finalization.diagnostics,
        stdout: finalization.stdout,
        stderr: finalization.stderr,
        casa_log: finalization.casa_log.map(PathBuf::from),
    })
}

#[derive(Debug, Deserialize)]
struct PersistedNotebookPythonOutputEvent {
    order: i64,
    channel: String,
    text: String,
}

fn notebook_projection(
    store: &NotebookStore,
    filename: &str,
) -> FrontendResult<NotebookDocumentProjection> {
    let snapshot = store
        .open_notebook(filename)
        .map_err(|error| notebook_error("open notebook", error))?;
    let receipts = store
        .receipts_for_notebook(snapshot.entry.id)
        .map_err(|error| notebook_error("load notebook receipts", error))?
        .into_iter()
        .map(|mut receipt| {
            let ordered_outputs = match receipt
                .artifacts
                .iter()
                .find(|artifact| artifact.role == "ordered_output")
            {
                None => None,
                Some(artifact) => {
                    let path = store.project_root().join(&artifact.path);
                    match fs::read(&path) {
                        Ok(contents) => match serde_json::from_slice::<
                            Vec<PersistedNotebookPythonOutputEvent>,
                        >(&contents)
                        {
                            Ok(outputs) => Some(
                                outputs
                                    .into_iter()
                                    .map(|output| NotebookPythonOutputEvent {
                                        order: output.order,
                                        channel: output.channel,
                                        text: output.text,
                                    })
                                    .collect(),
                            ),
                            Err(error) => {
                                receipt.diagnostics.push(format!(
                                    "ordered output artifact `{}` is malformed: {error}",
                                    artifact.path.display()
                                ));
                                None
                            }
                        },
                        Err(error) => {
                            receipt.diagnostics.push(format!(
                                "ordered output artifact `{}` could not be read: {error}",
                                artifact.path.display()
                            ));
                            None
                        }
                    }
                }
            };
            notebook_receipt_projection(&receipt, ordered_outputs)
        })
        .collect::<FrontendResult<Vec<_>>>()?;
    let visualizations = store
        .visualizations_for_notebook(snapshot.entry.id)
        .map_err(|error| notebook_error("load notebook visualizations", error))?;
    Ok(NotebookDocumentProjection {
        id: snapshot.entry.id.to_string(),
        filename: snapshot.entry.filename,
        source: snapshot.document.source().to_owned(),
        content_hash: snapshot.content_hash,
        cells: notebook_cell_projections(&snapshot.document)?,
        receipts,
        visualizations: visualizations
            .iter()
            .map(notebook_visualization_projection)
            .collect::<FrontendResult<Vec<_>>>()?,
    })
}

/// Parse one complete in-memory Markdown draft through the Rust-owned cell contract.
#[uniffi::export]
pub fn notebook_cells(source: String) -> FrontendResult<Vec<NotebookCellState>> {
    let document = NotebookDocument::parse(source)
        .map_err(|error| notebook_error("parse notebook draft cells", error))?;
    notebook_cell_projections(&document)
}

/// Project notebook list, complete Markdown sources, conflict tokens, and receipts.
#[uniffi::export]
pub fn notebook_project(
    project_root: String,
) -> FrontendResult<ScientificNotebookProjectProjection> {
    let store = NotebookStore::open(PathBuf::from(&project_root))
        .map_err(|error| notebook_error("open notebook project", error))?;
    store
        .recover_interrupted()
        .map_err(|error| notebook_error("recover interrupted notebook attempts", error))?;
    let notebooks = store
        .list_notebooks()
        .map_err(|error| notebook_error("list notebooks", error))?
        .into_iter()
        .map(|entry| notebook_projection(&store, &entry.filename))
        .collect::<FrontendResult<Vec<_>>>()?;
    Ok(ScientificNotebookProjectProjection {
        schema_version: 1,
        project_root,
        notebooks,
    })
}

/// Lazily create the default notebook or create a named Markdown notebook.
#[uniffi::export]
pub fn notebook_create(
    request: NotebookCreateRequest,
) -> FrontendResult<NotebookDocumentProjection> {
    let store = NotebookStore::open(PathBuf::from(&request.project_root))
        .map_err(|error| notebook_error("open notebook project", error))?;
    let snapshot = match request.filename {
        Some(filename) => store
            .create_named(&filename, &request.title)
            .map_err(|error| notebook_error("create named notebook", error))?,
        None => store
            .ensure_default_notebook()
            .map_err(|error| notebook_error("create default notebook", error))?,
    };
    notebook_projection(&store, &snapshot.entry.filename)
}

/// Atomically save one complete Markdown source with explicit conflict handling.
#[uniffi::export]
pub fn notebook_save(request: NotebookSaveRequest) -> FrontendResult<NotebookSaveProjection> {
    let store = NotebookStore::open(PathBuf::from(&request.project_root))
        .map_err(|error| notebook_error("open notebook project", error))?;
    let mut base = store
        .open_notebook(&request.filename)
        .map_err(|error| notebook_error("open notebook save base", error))?;
    base.content_hash = request.base_hash;
    let projection = match store
        .save_notebook(&base, &request.source, request.resolution.into())
        .map_err(|error| notebook_error("save notebook", error))?
    {
        SaveResult::Saved(snapshot) => NotebookSaveProjection::Saved {
            notebook: notebook_projection(&store, &snapshot.entry.filename)?,
        },
        SaveResult::Reloaded(snapshot) => NotebookSaveProjection::Reloaded {
            notebook: notebook_projection(&store, &snapshot.entry.filename)?,
        },
        SaveResult::Conflict(conflict) => NotebookSaveProjection::Conflict {
            base_hash: conflict.base_hash,
            external: notebook_projection(&store, &conflict.external.entry.filename)?,
            proposed_source: conflict.proposed_source,
        },
    };
    Ok(projection)
}

/// Begin best-effort project recording or apply the visible one-run bypass.
#[uniffi::export]
pub fn notebook_begin_recording(
    request: NotebookBeginRecordingRequest,
) -> FrontendResult<NotebookBeginRecordingResult> {
    let store = NotebookStore::open(PathBuf::from(&request.project_root))
        .map_err(|error| notebook_error("open notebook project", error))?;
    let (handle, warning) = store.try_begin_attempt(
        request.policy.into(),
        notebook_recording_request_owner(request.request)?,
    );
    Ok(NotebookBeginRecordingResult {
        handle: handle.as_ref().map(notebook_attempt_handle_projection),
        warning,
    })
}

/// Finalize exactly one immutable receipt revision.
#[uniffi::export]
pub fn notebook_finalize_recording(
    request: NotebookFinalizeRecordingRequest,
) -> FrontendResult<()> {
    let store = NotebookStore::open(PathBuf::from(&request.project_root))
        .map_err(|error| notebook_error("open notebook project", error))?;
    store
        .finalize_attempt(
            &notebook_attempt_handle_owner(request.handle)?,
            notebook_finalization_owner(request.finalization)?,
        )
        .map_err(|error| notebook_error("finalize notebook receipt", error))?;
    Ok(())
}

/// Copy one explicit explorer visualization into a new immutable notebook revision.
#[uniffi::export]
pub fn notebook_save_visualization(
    request: NotebookSaveVisualizationEnvelope,
) -> FrontendResult<NotebookVisualizationSnapshot> {
    let store = NotebookStore::open(PathBuf::from(&request.project_root))
        .map_err(|error| notebook_error("open notebook project", error))?;
    let snapshot = store
        .save_visualization(notebook_visualization_request_owner(request.request)?)
        .map_err(|error| notebook_error("save notebook visualization", error))?;
    notebook_visualization_projection(&snapshot)
}

/// Export portable Markdown/assets or explicitly include managed receipts.
#[uniffi::export]
pub fn notebook_export(request: NotebookExportRequest) -> FrontendResult<()> {
    let store = NotebookStore::open(PathBuf::from(request.project_root))
        .map_err(|error| notebook_error("open notebook project", error))?;
    store
        .export(Path::new(&request.destination), request.mode.into())
        .map_err(|error| notebook_error("export notebooks", error))
}

/// Describe the versioned agent-neutral persistence and project-MCP boundary.
#[uniffi::export]
pub fn assistant_protocol_info_json() -> FrontendResult<String> {
    serde_json::to_string(&AssistantProtocolProjection {
        profile_version: ASSISTANT_PROFILE_VERSION,
        transcript_schema_version: ASSISTANT_TRANSCRIPT_SCHEMA_VERSION,
        corpus_schema_version: CORPUS_SCHEMA_VERSION,
        retrieval_engine: "sqlite_fts5_unicode61",
        backend_session_binding: "opaque_adapter_session_id",
        authority_presets: vec!["explore", "work", "full_access"],
        project_mcp_tools: vec![
            "corpus.search",
            "source.search",
            "context.open_tabs",
            "task.schema",
            "data.describe",
            "web.fetch",
            "web.search",
        ],
    })
    .map_err(|error| assistant_error("serialize assistant protocol information", error))
}

/// Parse one trusted `task.suggest` MCP result into the generated application contract.
#[uniffi::export]
pub fn assistant_task_suggestion(
    tool_output: String,
) -> FrontendResult<AssistantTaskSuggestionProjection> {
    let suggestion: AssistantTaskSuggestionWire = serde_json::from_str(&tool_output)
        .map_err(|error| assistant_error("parse task suggestion", error))?;
    if suggestion.kind != "task_suggestion" {
        return Err(assistant_error(
            "parse task suggestion",
            format!("unexpected suggestion kind {:?}", suggestion.kind),
        ));
    }
    Ok(AssistantTaskSuggestionProjection {
        task_id: suggestion.task_id,
        parameters: suggestion.parameters,
        validated_patch: SurfaceParameterPatch {
            values: suggestion
                .validated_patch
                .values
                .into_iter()
                .map(|(name, value)| (name, value.into()))
                .collect(),
            unset: suggestion.validated_patch.unset.into_iter().collect(),
        },
    })
}

/// Construct one immutable notebook pin snapshot with transcript provenance.
#[uniffi::export]
pub fn assistant_create_pin_json(request_json: String) -> FrontendResult<String> {
    let request: AssistantCreatePinRequest = serde_json::from_str(&request_json)
        .map_err(|error| assistant_error("parse assistant pin request", error))?;
    let pin = AssistantPinReference::new(
        request.conversation_id,
        request.notebook_id,
        request.message_id,
        request.representation,
        request.snapshot_content,
    );
    serde_json::to_string(&pin).map_err(|error| assistant_error("serialize assistant pin", error))
}

/// List the provider-neutral visible conversations persisted for one project.
#[uniffi::export]
pub fn assistant_conversations_json(project_root: String) -> FrontendResult<String> {
    let store = AssistantStore::open(&project_root)
        .map_err(|error| assistant_error("open assistant project", error))?;
    let conversations = store
        .list_conversations()
        .map_err(|error| assistant_error("list assistant conversations", error))?;
    serde_json::to_string(&conversations)
        .map_err(|error| assistant_error("serialize assistant conversations", error))
}

/// Create one persistent conversation attached primarily to a task or notebook.
#[uniffi::export]
pub fn assistant_create_conversation_json(request_json: String) -> FrontendResult<String> {
    let request: AssistantCreateConversationRequest = serde_json::from_str(&request_json)
        .map_err(|error| assistant_error("parse assistant create request", error))?;
    let store = AssistantStore::open(&request.project_root)
        .map_err(|error| assistant_error("open assistant project", error))?;
    let conversation = store
        .create_conversation(request.title, request.primary_attachment, request.profile)
        .map_err(|error| assistant_error("create assistant conversation", error))?;
    serde_json::to_string(&conversation)
        .map_err(|error| assistant_error("serialize assistant conversation", error))
}

/// Load one persistent visible transcript without provider-specific envelopes.
#[uniffi::export]
pub fn assistant_load_conversation_json(request_json: String) -> FrontendResult<String> {
    let request: AssistantConversationRequest = serde_json::from_str(&request_json)
        .map_err(|error| assistant_error("parse assistant load request", error))?;
    let store = AssistantStore::open(&request.project_root)
        .map_err(|error| assistant_error("open assistant project", error))?;
    let conversation = store
        .load_conversation(request.conversation_id)
        .map_err(|error| assistant_error("load assistant conversation", error))?;
    serde_json::to_string(&conversation)
        .map_err(|error| assistant_error("serialize assistant conversation", error))
}

/// Atomically save one provider-neutral visible transcript.
#[uniffi::export]
pub fn assistant_save_conversation_json(request_json: String) -> FrontendResult<()> {
    let request: AssistantSaveConversationRequest = serde_json::from_str(&request_json)
        .map_err(|error| assistant_error("parse assistant save request", error))?;
    let store = AssistantStore::open(&request.project_root)
        .map_err(|error| assistant_error("open assistant project", error))?;
    store
        .save_conversation(&request.transcript)
        .map_err(|error| assistant_error("save assistant conversation", error))
}

/// Incrementally index trusted host-supplied corpus documents.
#[uniffi::export]
pub fn assistant_corpus_index_json(request_json: String) -> FrontendResult<String> {
    let request: AssistantCorpusIndexRequest = serde_json::from_str(&request_json)
        .map_err(|error| corpus_error("parse corpus index request", error))?;
    let index = CorpusIndex::open(&request.project_root)
        .map_err(|error| corpus_error("open corpus index", error))?;
    let report = if let Some(project_sources) = request.project_sources {
        index.index_documents_with_project_sources(
            &request.documents,
            &request.remove_missing_layers,
            &project_sources,
            &request.failed_project_sources,
        )
    } else {
        index.index_documents(&request.documents, &request.remove_missing_layers)
    }
    .map_err(|error| corpus_error("index corpus documents", error))?;
    serde_json::to_string(&report)
        .map_err(|error| corpus_error("serialize corpus index report", error))
}

/// Plan project-document extraction using metadata only.
#[uniffi::export]
pub fn assistant_project_corpus_plan_json(request_json: String) -> FrontendResult<String> {
    let request: AssistantProjectCorpusPlanRequest = serde_json::from_str(&request_json)
        .map_err(|error| corpus_error("parse project corpus plan request", error))?;
    let index = CorpusIndex::open(&request.project_root)
        .map_err(|error| corpus_error("open corpus index", error))?;
    let plan = index
        .plan_project_sources(&request.sources)
        .map_err(|error| corpus_error("plan project corpus extraction", error))?;
    serde_json::to_string(&plan)
        .map_err(|error| corpus_error("serialize project corpus plan", error))
}

/// Execute the bounded `corpus.search` operation exposed through project MCP.
#[uniffi::export]
pub fn assistant_corpus_search_json(request_json: String) -> FrontendResult<String> {
    let request: AssistantCorpusSearchRequest = serde_json::from_str(&request_json)
        .map_err(|error| corpus_error("parse corpus search request", error))?;
    if request.query.len() > MAX_ASSISTANT_CORPUS_QUERY_BYTES {
        return Err(corpus_error(
            "validate corpus search request",
            format!("query exceeds the {MAX_ASSISTANT_CORPUS_QUERY_BYTES}-byte host limit"),
        ));
    }
    let index = CorpusIndex::open(&request.project_root)
        .map_err(|error| corpus_error("open corpus index", error))?;
    let hits = index
        .search_layers(
            &request.query,
            request.limit.min(MAX_ASSISTANT_CORPUS_SEARCH_RESULTS),
            &request.layers,
        )
        .map_err(|error| corpus_error("search corpus", error))?;
    serde_json::to_string(&hits)
        .map_err(|error| corpus_error("serialize corpus search results", error))
}

fn tutorial_template_projection(template: &TutorialTemplate) -> TutorialTemplateProjection<'_> {
    TutorialTemplateProjection {
        root: template.root.to_string_lossy().into_owned(),
        content_sha256: &template.content_sha256,
        manifest: &template.manifest,
    }
}

/// Validate and preview one immutable portable tutorial template.
#[uniffi::export]
pub fn tutorial_template_json(template_path: String) -> FrontendResult<String> {
    let template = TutorialProject::load_template(&template_path)
        .map_err(|error| tutorial_error("load tutorial template", error))?;
    serialize_tutorial(
        "serialize tutorial template",
        &tutorial_template_projection(&template),
    )
}

/// One-shot conversion of `tutorial-pack.v0` into a portable v1 template.
#[uniffi::export]
pub fn tutorial_migrate_v0_json(request_json: String) -> FrontendResult<String> {
    let request: TutorialMigrateRequest =
        parse_tutorial_request("parse tutorial migration request", &request_json)?;
    let template = TutorialProject::migrate_v0_template(&request.pack_path, &request.destination)
        .map_err(|error| tutorial_error("migrate tutorial-pack v0", error))?;
    serialize_tutorial(
        "serialize migrated tutorial template",
        &tutorial_template_projection(&template),
    )
}

/// Fork one immutable template into an editable learner notebook and managed lock.
#[uniffi::export]
pub fn tutorial_fork_json(request_json: String) -> FrontendResult<String> {
    let request: TutorialForkRequest =
        parse_tutorial_request("parse tutorial fork request", &request_json)?;
    let template = TutorialProject::load_template(&request.template_path)
        .map_err(|error| tutorial_error("load tutorial template", error))?;
    let project = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?;
    let forked = project
        .fork_template(&template, &request.filename)
        .map_err(|error| tutorial_error("fork tutorial template", error))?;
    let store = NotebookStore::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial notebook store", error))?;
    serialize_tutorial(
        "serialize forked tutorial",
        &TutorialForkProjection {
            notebook: notebook_projection(&store, &forked.notebook.entry.filename)?,
            tutorial: forked.lock,
        },
    )
}

/// Reopen one learner tutorial entirely from Rust-owned project state.
#[uniffi::export]
pub fn tutorial_project_json(request_json: String) -> FrontendResult<String> {
    let request: TutorialNotebookRequest =
        parse_tutorial_request("parse tutorial project request", &request_json)?;
    let project = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?;
    let lock = project
        .load_lock(request.notebook_id)
        .map_err(|error| tutorial_error("load tutorial lock", error))?;
    let store = NotebookStore::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial notebook store", error))?;
    serialize_tutorial(
        "serialize tutorial project",
        &TutorialForkProjection {
            notebook: notebook_projection(&store, &lock.notebook_filename)?,
            tutorial: lock,
        },
    )
}

/// List every Rust-owned learner tutorial in one project.
#[uniffi::export]
pub fn tutorial_project_list_json(project_root: String) -> FrontendResult<String> {
    let project = TutorialProject::open(&project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?;
    let store = NotebookStore::open(&project_root)
        .map_err(|error| tutorial_error("open tutorial notebook store", error))?;
    let tutorials = project
        .list_locks()
        .map_err(|error| tutorial_error("list tutorial locks", error))?
        .into_iter()
        .map(|lock| {
            Ok(TutorialForkProjection {
                notebook: notebook_projection(&store, &lock.notebook_filename)?,
                tutorial: lock,
            })
        })
        .collect::<FrontendResult<Vec<_>>>()?;
    serialize_tutorial("serialize tutorial project list", &tutorials)
}

/// Resolve the exact source, redirect, integrity, disk, and extraction approval facts.
#[uniffi::export]
pub fn tutorial_plan_acquisition_json(request_json: String) -> FrontendResult<String> {
    let request: TutorialPlanRequest =
        parse_tutorial_request("parse tutorial acquisition plan request", &request_json)?;
    let plan = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?
        .plan_acquisition(
            request.notebook_id,
            &request.dataset_id,
            request.source_override.as_deref(),
        )
        .map_err(|error| tutorial_error("plan tutorial acquisition", error))?;
    serialize_tutorial("serialize tutorial acquisition plan", &plan)
}

/// Begin one exact explicitly approved acquisition generation.
#[uniffi::export]
pub fn tutorial_begin_acquisition_json(request_json: String) -> FrontendResult<String> {
    let request: TutorialBeginRequest =
        parse_tutorial_request("parse tutorial acquisition approval", &request_json)?;
    let state = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?
        .begin_acquisition(&request.plan, request.approval)
        .map_err(|error| tutorial_error("begin tutorial acquisition", error))?;
    serialize_tutorial("serialize tutorial acquisition state", &state)
}

fn tutorial_action(
    request_json: &str,
    action: &'static str,
    apply: impl FnOnce(
        &TutorialProject,
        casa_notebook::NotebookId,
        &str,
    ) -> Result<casa_notebook::TutorialDatasetLock, casa_notebook::TutorialError>,
) -> FrontendResult<String> {
    let request: TutorialActionRequest = parse_tutorial_request(action, request_json)?;
    let project = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?;
    let state = apply(&project, request.notebook_id, &request.dataset_id)
        .map_err(|error| tutorial_error(action, error))?;
    serialize_tutorial("serialize tutorial acquisition state", &state)
}

#[uniffi::export]
pub fn tutorial_resume_acquisition_json(request_json: String) -> FrontendResult<String> {
    tutorial_action(
        &request_json,
        "resume tutorial acquisition",
        |project, id, dataset| project.resume_acquisition(id, dataset),
    )
}

#[uniffi::export]
pub fn tutorial_restart_acquisition_json(request_json: String) -> FrontendResult<String> {
    tutorial_action(
        &request_json,
        "restart tutorial acquisition",
        |project, id, dataset| project.restart_acquisition(id, dataset),
    )
}

#[uniffi::export]
pub fn tutorial_retry_acquisition_json(request_json: String) -> FrontendResult<String> {
    tutorial_action(
        &request_json,
        "retry tutorial acquisition",
        |project, id, dataset| project.retry_acquisition(id, dataset),
    )
}

#[uniffi::export]
pub fn tutorial_cancel_acquisition_json(request_json: String) -> FrontendResult<String> {
    let request: TutorialGenerationActionRequest =
        parse_tutorial_request("parse tutorial cancellation", &request_json)?;
    let state = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?
        .cancel_acquisition(request.notebook_id, &request.dataset_id, request.generation)
        .map_err(|error| tutorial_error("cancel tutorial acquisition", error))?;
    serialize_tutorial("serialize tutorial acquisition state", &state)
}

/// Advance one bounded chunk/phase so the GUI can remain responsive and cancellation-aware.
#[uniffi::export]
pub fn tutorial_advance_acquisition_json(request_json: String) -> FrontendResult<String> {
    let request: TutorialGenerationActionRequest =
        parse_tutorial_request("parse tutorial acquisition advance", &request_json)?;
    let state = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?
        .advance_acquisition(
            request.notebook_id,
            &request.dataset_id,
            request.generation,
            request.max_download_bytes,
        )
        .map_err(|error| tutorial_error("advance tutorial acquisition", error))?;
    serialize_tutorial("serialize tutorial acquisition state", &state)
}

#[uniffi::export]
pub fn application_catalog() -> FrontendResult<ApplicationCatalogEnvelope> {
    let catalog =
        builtin_application_catalog().map_err(|reason| FrontendServiceError::Probe { reason })?;
    let applications = catalog
        .applications
        .iter()
        .map(|application| {
            let surface =
                application
                    .surface_bundle()
                    .map_err(|reason| FrontendServiceError::Probe {
                        reason: format!("resolve application {}: {reason}", application.id),
                    })?;
            let wire_string = |value: serde_json::Value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| FrontendServiceError::Probe {
                        reason: format!(
                            "application {} has a non-string enum projection",
                            application.id
                        ),
                    })
            };
            Ok(ApplicationCatalogEntry {
                id: application.id.clone(),
                kind: wire_string(serde_json::to_value(application.kind).map_err(|error| {
                    FrontendServiceError::Probe {
                        reason: format!("project application kind: {error}"),
                    }
                })?)?,
                display_name: surface
                    .as_ref()
                    .map(|bundle| bundle.surface.display_name())
                    .unwrap_or("CASA-RS")
                    .to_string(),
                category: surface
                    .as_ref()
                    .map(|bundle| bundle.surface.category())
                    .unwrap_or("Launcher")
                    .to_string(),
                executable: application.launch.executable.clone(),
                cargo_package: application.launch.cargo_package.clone(),
                override_env: application.launch.override_env.clone(),
                shell_kind: wire_string(serde_json::to_value(application.shell_kind).map_err(
                    |error| FrontendServiceError::Probe {
                        reason: format!("project shell kind: {error}"),
                    },
                )?)?,
                interaction: wire_string(serde_json::to_value(application.interaction).map_err(
                    |error| FrontendServiceError::Probe {
                        reason: format!("project interaction: {error}"),
                    },
                )?)?,
                browser_kind: application
                    .browser_kind
                    .map(|kind| serde_json::to_value(kind))
                    .transpose()
                    .map_err(|error| FrontendServiceError::Probe {
                        reason: format!("project browser kind: {error}"),
                    })?
                    .map(wire_string)
                    .transpose()?,
                dataset_kinds: application.dataset_kinds.clone(),
                show_in_tui: application.show_in_tui,
                show_in_swift: application.show_in_swift,
                include_in_suite: application.include_in_suite,
            })
        })
        .collect::<FrontendResult<Vec<_>>>()?;
    Ok(ApplicationCatalogEnvelope {
        schema_version: u64::from(catalog.schema_version),
        applications,
    })
}

#[uniffi::export]
pub fn task_context_options(dataset_path: String) -> FrontendResult<TaskContextOptionsEnvelope> {
    let path = PathBuf::from(&dataset_path);
    let probe = probe_dataset_path(&path)
        .map_err(|error| FrontendServiceError::Probe {
            reason: format!("{}: {error}", path.display()),
        })?
        .ok_or_else(|| FrontendServiceError::InvalidPath {
            reason: format!("{} is not a recognized CASA-rs dataset", path.display()),
        })?;

    let mut defaults = BTreeMap::new();
    insert_first_default(&mut defaults, "field", &probe.fields);
    insert_first_default(&mut defaults, "spectral_window", &probe.spectral_windows);
    insert_first_default(&mut defaults, "scan", &probe.scans);
    insert_first_default(&mut defaults, "antenna", &probe.antennas);
    insert_first_default(&mut defaults, "correlation", &probe.correlations);
    insert_first_default(&mut defaults, "data_column", &probe.data_columns);
    insert_first_default(&mut defaults, "column", &probe.columns);

    Ok(TaskContextOptionsEnvelope {
        schema_version: 1,
        dataset_path: probe.path,
        dataset_kind: match probe.kind {
            DatasetKind::MeasurementSet => "measurement_set",
            DatasetKind::Image => "image",
            DatasetKind::Table => "table",
        }
        .to_string(),
        fields: probe.fields,
        spectral_windows: probe.spectral_windows,
        scans: probe.scans,
        arrays: probe.arrays,
        observations: probe.observations,
        antennas: probe.antennas,
        intents: probe.intents,
        feeds: probe.feeds,
        correlations: probe.correlations,
        columns: probe.columns,
        data_columns: probe.data_columns,
        subtables: probe.subtables,
        shape: probe.shape,
        defaults: defaults.into_iter().collect(),
        diagnostics: probe.diagnostics,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct SurfaceParameterWriteResult {
    pub path: String,
    pub bytes_written: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_kind: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct SurfaceRunSafety {
    pub classes: Vec<SurfaceRunSafetyClass>,
    pub requires_interactive_confirmation: bool,
    pub requires_overwrite_confirmation: bool,
    pub requires_input_mutation_confirmation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, uniffi::Enum)]
pub enum SurfaceRunSafetyClass {
    ProductWrite,
    Overwrite,
    InputMutation,
}

impl From<RunSafetyClass> for SurfaceRunSafetyClass {
    fn from(value: RunSafetyClass) -> Self {
        match value {
            RunSafetyClass::ProductWrite => Self::ProductWrite,
            RunSafetyClass::Overwrite => Self::Overwrite,
            RunSafetyClass::InputMutation => Self::InputMutation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct SurfaceProviderInvocation {
    pub args: Vec<String>,
    pub stdin: Option<String>,
}

fn snake_case_debug(value: impl std::fmt::Debug) -> String {
    let source = format!("{value:?}");
    let mut output = String::with_capacity(source.len());
    for (index, character) in source.chars().enumerate() {
        if character.is_uppercase() {
            if index > 0 {
                output.push('_');
            }
            for lower in character.to_lowercase() {
                output.push(lower);
            }
        } else {
            output.push(character);
        }
    }
    output
}

fn surface_parameter_type(value: &casa_provider_contracts::ParameterType) -> SurfaceParameterType {
    use casa_provider_contracts::ParameterType;
    match value {
        ParameterType::Bool => SurfaceParameterType::Bool,
        ParameterType::Integer => SurfaceParameterType::Integer,
        ParameterType::Float => SurfaceParameterType::Float,
        ParameterType::String => SurfaceParameterType::String,
        ParameterType::Path { resource_kind } => SurfaceParameterType::Path {
            resource_kind: resource_kind.map(snake_case_debug),
        },
        ParameterType::Choice { values } => SurfaceParameterType::Choice {
            values: values.clone(),
        },
        ParameterType::Quantity {
            dimension,
            canonical_unit,
            special_values,
        } => SurfaceParameterType::Quantity {
            dimension: snake_case_debug(dimension),
            canonical_unit: canonical_unit.clone(),
            special_values: special_values.clone(),
        },
        ParameterType::Array {
            element,
            min_items,
            max_items,
            allow_scalar,
        } => SurfaceParameterType::Array {
            elements: vec![surface_parameter_type(element)],
            min_items: *min_items as u64,
            max_items: max_items.map(|value| value as u64),
            allow_scalar: *allow_scalar,
        },
        ParameterType::Table { fields } => SurfaceParameterType::Table {
            fields: fields
                .iter()
                .map(|(name, value)| SurfaceParameterTypeField {
                    name: name.clone(),
                    value: surface_parameter_type(value),
                })
                .collect(),
        },
        ParameterType::Optional { value, states } => SurfaceParameterType::Optional {
            values: vec![surface_parameter_type(value)],
            states: states.clone(),
        },
    }
}

fn surface_parameter_predicate(
    value: &casa_provider_contracts::Predicate,
) -> SurfaceParameterPredicate {
    use casa_provider_contracts::Predicate;
    match value {
        Predicate::Always => SurfaceParameterPredicate::Always,
        Predicate::Never => SurfaceParameterPredicate::Never,
        Predicate::IsSet { parameter } => SurfaceParameterPredicate::IsSet {
            parameter: parameter.clone(),
        },
        Predicate::Equals { parameter, value } => SurfaceParameterPredicate::Equals {
            parameter: parameter.clone(),
            value: value.clone().into(),
        },
        Predicate::Not { predicate } => SurfaceParameterPredicate::Not {
            predicates: vec![surface_parameter_predicate(predicate)],
        },
        Predicate::All { predicates } => SurfaceParameterPredicate::All {
            predicates: predicates.iter().map(surface_parameter_predicate).collect(),
        },
        Predicate::Any { predicates } => SurfaceParameterPredicate::Any {
            predicates: predicates.iter().map(surface_parameter_predicate).collect(),
        },
    }
}

fn surface_parameter_binding(
    value: &casa_provider_contracts::SurfaceParameterBinding,
) -> SurfaceParameterBinding {
    SurfaceParameterBinding {
        name: value.name.clone(),
        concept: SurfaceParameterConceptReference {
            id: value.concept.id.0.clone(),
            semantic_revision: value.concept.semantic_revision.0 as u64,
        },
        order: value.order as u64,
        refinements: value
            .refinements
            .iter()
            .map(|constraint| SurfaceNarrowingConstraint {
                kind: match constraint {
                    casa_provider_contracts::NarrowingConstraint::NumberRange { .. } => {
                        "number_range"
                    }
                    casa_provider_contracts::NarrowingConstraint::Length { .. } => "length",
                    casa_provider_contracts::NarrowingConstraint::AllowedValues { .. } => {
                        "allowed_values"
                    }
                    casa_provider_contracts::NarrowingConstraint::SelectorCapabilities {
                        ..
                    } => "selector_capabilities",
                    casa_provider_contracts::NarrowingConstraint::SquarePair => "square_pair",
                }
                .to_string(),
            })
            .collect(),
        context_role: value.context_role.map(snake_case_debug),
        surface_note: value.surface_note.clone(),
        projections: SurfaceParameterProjections {
            cli: value
                .projections
                .cli
                .as_ref()
                .map(|projection| SurfaceParameterCliProjection {
                    positional: projection.positional.map(|value| value as u64),
                    flags: projection.flags.clone(),
                    false_flags: projection.false_flags.clone(),
                    metavar: projection.metavar.clone(),
                }),
            provider: value.projections.provider.as_ref().map(|projection| {
                SurfaceParameterProviderProjection {
                    field: projection.field.clone(),
                    adapter: snake_case_debug(projection.adapter.clone()),
                    emit_when: projection
                        .emit_when
                        .as_ref()
                        .map(surface_parameter_predicate),
                }
            }),
            presentation: SurfaceParameterPresentation {
                label: value.projections.presentation.label.clone(),
                group: value.projections.presentation.group.clone(),
                advanced: value.projections.presentation.advanced,
                hidden: value.projections.presentation.hidden,
            },
        },
    }
}

fn surface_parameter_bundle(value: SurfaceContractBundle) -> SurfaceParameterBundle {
    let surface = SurfaceParameterDefinition {
        kind: value.surface.kind().to_string(),
        id: value.surface.id().to_string(),
        contract_version: value.surface.contract_version() as u64,
        display_name: value.surface.display_name().to_string(),
        category: value.surface.category().to_string(),
        summary: value.surface.summary().to_string(),
        execution: SurfaceExecutionProjection {
            invocation_name: value.surface.execution().invocation_name.clone(),
            fixed_args: value.surface.execution().fixed_args.clone(),
        },
        bindings: value
            .surface
            .bindings()
            .iter()
            .map(surface_parameter_binding)
            .collect(),
    };
    let catalog = SurfaceParameterCatalog {
        schema_version: value.catalog.schema_version as u64,
        concepts: value
            .catalog
            .concepts
            .into_iter()
            .map(|concept| SurfaceParameterConcept {
                id: concept.id.0,
                semantic_revision: concept.semantic_revision.0 as u64,
                casa_name: concept.casa_name,
                value_domain: surface_parameter_type(&concept.value_domain),
                unit_dimension: concept.unit_dimension.map(snake_case_debug),
                semantic_role: snake_case_debug(concept.semantic_role),
                documentation: SurfaceParameterDocumentation {
                    summary: concept.documentation.summary,
                    details: concept.documentation.details,
                    examples: concept.documentation.examples,
                },
                persistence_class: snake_case_debug(concept.persistence_class),
            })
            .collect(),
    };
    SurfaceParameterBundle {
        schema_version: value.schema_version as u64,
        surface,
        catalog,
    }
}

fn surface_parameter_snapshot(
    session: &ParameterSession,
) -> FrontendResult<SurfaceParameterSnapshot> {
    let profile_toml = match session.render_sparse() {
        Ok(profile_toml) => Some(profile_toml),
        Err(_)
            if session
                .diagnostics()
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::MissingRequired) =>
        {
            None
        }
        Err(error) => return Err(parameter_error("render sparse parameter profile", error)),
    };
    let base_source = match session.base_source() {
        BaseSource::Defaults => SurfaceParameterSourceRecord::Defaults,
        BaseSource::Last => SurfaceParameterSourceRecord::Last,
        BaseSource::LastSuccessful => SurfaceParameterSourceRecord::LastSuccessful,
        BaseSource::File(path) => SurfaceParameterSourceRecord::File {
            path: path.to_string_lossy().into_owned(),
        },
    };
    Ok(SurfaceParameterSnapshot {
        schema_version: session.bundle().schema_version as u64,
        surface_id: session.bundle().surface.id().to_string(),
        surface_kind: session.bundle().surface.kind().to_string(),
        contract_version: session.bundle().surface.contract_version() as u64,
        base_source,
        dirty: session.is_dirty(),
        states: session
            .states()
            .iter()
            .map(|(name, state)| {
                (
                    name.clone(),
                    SurfaceParameterState {
                        value: state.value.clone().map(Into::into),
                        origin: snake_case_debug(state.origin.clone()),
                        active: state.active,
                        required: state.required,
                        explicit: state.explicit,
                    },
                )
            })
            .collect(),
        diagnostics: session
            .diagnostics()
            .iter()
            .map(|diagnostic| SurfaceParameterDiagnostic {
                level: snake_case_debug(diagnostic.level),
                code: snake_case_debug(diagnostic.code),
                message: diagnostic.message.clone(),
                parameter: diagnostic.parameter.clone(),
                location: diagnostic
                    .location
                    .map(|location| SurfaceParameterLocation {
                        line: location.line as u64,
                        column: location.column as u64,
                    }),
                suggestions: diagnostic.suggestions.clone(),
            })
            .collect(),
        profile_toml,
    })
}

fn parameter_error(context: &str, error: impl std::fmt::Display) -> FrontendServiceError {
    FrontendServiceError::Parameters {
        reason: format!("{context}: {error}"),
    }
}

fn parameter_bundle(surface_id: &str) -> FrontendResult<SurfaceContractBundle> {
    builtin_surface_bundle(surface_id)
        .map_err(|error| parameter_error("load parameter surface", error))
}

fn parameter_session_from_source(
    surface_id: &str,
    source: &str,
    profile_toml: Option<&str>,
    profile_path: Option<PathBuf>,
    workspace: PathBuf,
) -> FrontendResult<ParameterSession> {
    let bundle = parameter_bundle(surface_id)?;
    let source = match source {
        "defaults" => BaseSource::Defaults,
        "file" => BaseSource::File(profile_path.unwrap_or_else(|| PathBuf::from("<memory>"))),
        "last" => BaseSource::Last,
        "last_successful" => BaseSource::LastSuccessful,
        other => {
            return Err(parameter_error(
                "resolve parameter source",
                format!(
                    "unknown parameter source {other:?}; expected defaults, file, last, or last_successful"
                ),
            ));
        }
    };
    ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle,
            workspace,
            source,
            profile_text: profile_toml.map(str::to_owned),
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch::default(),
            managed_save: true,
        })
        .map_err(|error| parameter_error("open parameter session", error))
}

fn typed_parameter_values(
    values: HashMap<String, SurfaceParameterValue>,
) -> BTreeMap<String, ParameterValue> {
    values
        .into_iter()
        .map(|(name, value)| (name, ParameterValue::from(value)))
        .collect()
}

fn typed_parameter_patch(patch: SurfaceParameterPatch) -> ResolutionPatch {
    ResolutionPatch {
        values: typed_parameter_values(patch.values),
        unset: patch.unset.into_iter().collect(),
    }
}

fn parameter_session_from_typed_values(
    surface_id: &str,
    values: HashMap<String, SurfaceParameterValue>,
) -> FrontendResult<ParameterSession> {
    ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle: parameter_bundle(surface_id)?,
            workspace: PathBuf::from("."),
            source: BaseSource::Defaults,
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch: ResolutionPatch {
                values: typed_parameter_values(values),
                unset: BTreeSet::new(),
            },
            managed_save: false,
        })
        .map_err(|error| parameter_error("resolve parameter values", error))
}

/// Rust-owned session persistence lifecycle used by generated frontends.
#[derive(uniffi::Object)]
pub struct ParameterSessionLifecycle {
    coordinator: SessionLastCoordinator,
}

/// Rust-owned task-attempt persistence lifecycle used by generated frontends.
#[derive(uniffi::Object)]
pub struct ParameterTaskLifecycle {
    coordinator: TaskLastCoordinator,
}

#[uniffi::export]
impl ParameterTaskLifecycle {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            coordinator: TaskLastCoordinator::new(),
        })
    }

    /// Capture and persist the exact validated snapshot immediately before execution.
    pub fn before_execution(
        &self,
        attempt_id: String,
        surface_id: String,
        workspace: String,
        values: HashMap<String, SurfaceParameterValue>,
        enabled: bool,
    ) -> FrontendResult<Vec<String>> {
        let session = parameter_session_from_typed_values(&surface_id, values)?;
        self.coordinator
            .before_execution(attempt_id, workspace, &surface_id, enabled, &session)
            .map_err(|error| parameter_error("record parameter task attempt", error))?;
        Ok(self.coordinator.take_warnings())
    }

    /// Complete an attempt, promoting its captured snapshot only on success.
    pub fn after_completion(&self, attempt_id: String, successful: bool) -> Vec<String> {
        self.coordinator.after_completion(&attempt_id, successful);
        self.coordinator.take_warnings()
    }
}

#[uniffi::export]
impl ParameterSessionLifecycle {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            coordinator: SessionLastCoordinator::new(
                ParameterRuntime::default().session_debounce(),
            ),
        })
    }

    /// Record one successfully opened session root.
    pub fn opened(
        &self,
        surface_id: String,
        workspace: String,
        values: HashMap<String, SurfaceParameterValue>,
        enabled: bool,
    ) -> FrontendResult<Vec<String>> {
        let session = parameter_session_from_typed_values(&surface_id, values)?;
        self.coordinator
            .opened(workspace, &surface_id, enabled, &session)
            .map_err(|error| parameter_error("record opened parameter session", error))?;
        Ok(self.coordinator.take_warnings())
    }

    /// Queue one backend-accepted durable session change.
    pub fn accepted_durable_change(
        &self,
        surface_id: String,
        workspace: String,
        values: HashMap<String, SurfaceParameterValue>,
        enabled: bool,
    ) -> FrontendResult<Vec<String>> {
        let session = parameter_session_from_typed_values(&surface_id, values)?;
        self.coordinator
            .accepted_durable_change(workspace, &surface_id, enabled, &session)
            .map_err(|error| parameter_error("record accepted parameter session change", error))?;
        Ok(self.coordinator.take_warnings())
    }

    /// Flush one destination on clean session close.
    pub fn flush(&self, surface_id: String, workspace: String) -> Vec<String> {
        self.coordinator.flush(workspace, &surface_id);
        self.coordinator.take_warnings()
    }

    /// Flush every destination on clean frontend shutdown.
    pub fn flush_all(&self) -> Vec<String> {
        self.coordinator.flush_all();
        self.coordinator.take_warnings()
    }

    /// Drain any asynchronous persistence warnings.
    pub fn take_warnings(&self) -> Vec<String> {
        self.coordinator.take_warnings()
    }
}

/// Return one self-contained surface definition and its referenced concepts.
#[uniffi::export]
pub fn parameter_surface_bundle(surface_id: String) -> FrontendResult<SurfaceParameterBundle> {
    Ok(surface_parameter_bundle(parameter_bundle(&surface_id)?))
}

/// Resolve the current defaults and UI state for one surface.
#[uniffi::export]
pub fn parameter_defaults(surface_id: String) -> FrontendResult<SurfaceParameterSnapshot> {
    let session =
        parameter_session_from_source(&surface_id, "defaults", None, None, PathBuf::from("."))?;
    surface_parameter_snapshot(&session)
}

/// Load and resolve an explicit sparse TOML profile supplied by the frontend.
#[uniffi::export]
pub fn parameter_load(
    surface_id: String,
    profile_toml: String,
    source_path: String,
) -> FrontendResult<SurfaceParameterSnapshot> {
    let session = parameter_session_from_source(
        &surface_id,
        "file",
        Some(&profile_toml),
        Some(PathBuf::from(source_path)),
        PathBuf::from("."),
    )?;
    surface_parameter_snapshot(&session)
}

/// Load Last or Last Successful from the managed store, if present.
#[uniffi::export]
pub fn parameter_last(
    surface_id: String,
    workspace: String,
    successful: bool,
) -> FrontendResult<Option<SurfaceParameterSnapshot>> {
    let bundle = parameter_bundle(&surface_id)?;
    if successful && bundle.surface.kind() == SurfaceKind::Session {
        return Err(parameter_error(
            "read managed parameter profile",
            format!("session surface {surface_id:?} does not have Last Successful"),
        ));
    }
    let (source, kind) = if successful {
        ("last_successful", ManagedProfileKind::LastSuccessful)
    } else {
        ("last", ManagedProfileKind::Last)
    };
    let workspace = PathBuf::from(workspace);
    let profile = ManagedStateStore::for_workspace(&workspace)
        .read(&surface_id, kind)
        .map_err(|error| parameter_error("read managed parameter profile", error))?;
    let Some(profile) = profile else {
        return Ok(None);
    };
    let session =
        parameter_session_from_source(&surface_id, source, Some(&profile), None, workspace)?;
    surface_parameter_snapshot(&session).map(Some)
}

/// Resolve typed context and explicit override patches over one selected base source.
#[uniffi::export]
pub fn parameter_resolve(
    surface_id: String,
    base_source: SurfaceParameterBaseSource,
    profile_toml: Option<String>,
    profile_path: Option<String>,
    context_patch: SurfaceParameterPatch,
    override_patch: SurfaceParameterPatch,
) -> FrontendResult<SurfaceParameterSnapshot> {
    let source = match base_source {
        SurfaceParameterBaseSource::Defaults => BaseSource::Defaults,
        SurfaceParameterBaseSource::File => BaseSource::File(
            profile_path
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("<memory>")),
        ),
        SurfaceParameterBaseSource::Last => BaseSource::Last,
        SurfaceParameterBaseSource::LastSuccessful => BaseSource::LastSuccessful,
    };
    let session = ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle: parameter_bundle(&surface_id)?,
            workspace: PathBuf::from("."),
            source,
            profile_text: profile_toml,
            context_patch: typed_parameter_patch(context_patch),
            override_patch: typed_parameter_patch(override_patch),
            managed_save: false,
        })
        .map_err(|error| parameter_error("resolve parameter session", error))?;
    surface_parameter_snapshot(&session)
}

/// Render typed resolved values as a sparse current-contract TOML profile.
#[uniffi::export]
pub fn parameter_render_toml(
    surface_id: String,
    values: HashMap<String, SurfaceParameterValue>,
) -> FrontendResult<String> {
    parameter_session_from_typed_values(&surface_id, values)?
        .render_sparse()
        .map_err(|error| parameter_error("render sparse parameter profile", error))
}

/// Evaluate catalog-owned run risks for one resolved task value set.
#[uniffi::export]
pub fn parameter_run_safety(
    surface_id: String,
    values: HashMap<String, SurfaceParameterValue>,
) -> FrontendResult<SurfaceRunSafety> {
    let session = parameter_session_from_typed_values(&surface_id, values)?;
    let safety = session
        .required_run_safety()
        .map_err(|error| parameter_error("evaluate task run safety", error))?;
    Ok(SurfaceRunSafety {
        classes: safety.classes().iter().copied().map(Into::into).collect(),
        requires_interactive_confirmation: safety.requires_interactive_confirmation(),
        requires_overwrite_confirmation: safety.requires_overwrite_confirmation(),
        requires_input_mutation_confirmation: safety.requires_input_mutation_confirmation(),
    })
}

/// Project one complete task invocation for CLI/TUI/GUI consumers.
#[uniffi::export]
pub fn parameter_provider_invocation(
    surface_id: String,
    values: HashMap<String, SurfaceParameterValue>,
) -> FrontendResult<SurfaceProviderInvocation> {
    let session = parameter_session_from_typed_values(&surface_id, values)?;
    session
        .render_sparse()
        .map_err(|error| parameter_error("validate provider invocation parameters", error))?;
    let invocation = project_provider_invocation(&session, |family, values, direct| match family {
        "simobserve" => {
            casa_ms::simulation_task::simobserve_provider_invocation(values, direct.args)
        }
        _ => Ok(ProviderInvocationAdaptation::direct(direct)),
    })
    .map_err(|error| parameter_error("project provider invocation", error))?;
    Ok(SurfaceProviderInvocation {
        args: invocation.args,
        stdin: invocation.stdin,
    })
}

/// Atomically save typed resolved values to an explicit sparse TOML profile.
#[uniffi::export]
pub fn parameter_save(
    surface_id: String,
    values: HashMap<String, SurfaceParameterValue>,
    destination_path: String,
) -> FrontendResult<SurfaceParameterWriteResult> {
    let profile = parameter_render_toml(surface_id, values)?;
    write_parameter_profile(Path::new(&destination_path), &profile)
}

#[uniffi::export]
pub fn task_ui_schema(surface_id: String) -> FrontendResult<TaskUISchema> {
    serde_json::from_value(casa_provider_contracts::project_ui_form(&parameter_bundle(
        &surface_id,
    )?))
    .map_err(|error| parameter_error("project typed task form", error))
}

fn write_parameter_profile(
    path: &Path,
    contents: &str,
) -> FrontendResult<SurfaceParameterWriteResult> {
    let outcome = write_parameter_profile_atomic(path, contents)
        .map_err(|error| parameter_error("save parameter profile", error))?;
    Ok(SurfaceParameterWriteResult {
        path: path.to_string_lossy().into_owned(),
        bytes_written: outcome.bytes_written as u64,
        managed_kind: None,
    })
}

#[uniffi::export]
pub fn tutorial_task_parameter_audit_json() -> String {
    TUTORIAL_TASK_PARAMETER_AUDIT_JSON.to_string()
}

fn insert_first_default(defaults: &mut BTreeMap<String, String>, key: &str, values: &[String]) {
    if let Some(value) = values.first().filter(|value| !value.is_empty()) {
        defaults.insert(key.to_string(), value.clone());
    }
}

#[uniffi::export]
pub fn probe_path(path: String) -> FrontendResult<Option<DatasetProbe>> {
    let path = PathBuf::from(path);
    probe_dataset_path(&path).map_err(|error| FrontendServiceError::Probe {
        reason: format!("{}: {error}", path.display()),
    })
}

/// Decode one successful provider result and probe only contract-declared products.
#[uniffi::export]
pub fn task_completion(
    surface_id: String,
    stdout: String,
    workspace: String,
    values: Vec<TaskParameterValue>,
) -> FrontendResult<TaskCompletionProjection> {
    let bundle = builtin_surface_bundle(&surface_id)
        .map_err(|reason| FrontendServiceError::TaskCompletion { reason })?;
    let values = values
        .into_iter()
        .map(|entry| TaskOutputValue {
            name: entry.name,
            value: entry.value,
        })
        .collect::<Vec<_>>();
    let completion = decode_task_completion(&bundle, &stdout, Path::new(&workspace), &values)
        .map_err(|error| FrontendServiceError::TaskCompletion {
            reason: error.to_string(),
        })?;
    let products = completion
        .products
        .into_iter()
        .map(|product| {
            let mut diagnostic = product.diagnostic;
            let dataset = if product.exists
                && matches!(
                    product.resource_kind,
                    RunProductKind::MeasurementSet
                        | RunProductKind::CasaImage
                        | RunProductKind::CasaTable
                )
            {
                match probe_dataset_path(&product.path) {
                    Ok(dataset) => {
                        if dataset.is_none() {
                            diagnostic = Some(format!(
                                "{} is a declared product but no supported dataset probe recognized it",
                                product.path.display()
                            ));
                        }
                        dataset
                    }
                    Err(error) => {
                        diagnostic = Some(format!(
                            "failed to probe declared product {}: {error}",
                            product.path.display()
                        ));
                        None
                    }
                }
            } else {
                None
            };
            TaskCompletionProduct {
                id: product.id,
                role: match product.role {
                    RunProductRole::Primary => TaskProductRole::Primary,
                    RunProductRole::Auxiliary => TaskProductRole::Auxiliary,
                    RunProductRole::Preview => TaskProductRole::Preview,
                },
                resource_kind: match product.resource_kind {
                    RunProductKind::MeasurementSet => TaskProductKind::MeasurementSet,
                    RunProductKind::CasaImage => TaskProductKind::CasaImage,
                    RunProductKind::CasaTable => TaskProductKind::CasaTable,
                    RunProductKind::FitsImage => TaskProductKind::FitsImage,
                    RunProductKind::File => TaskProductKind::File,
                },
                label: product.label,
                path: product.path.to_string_lossy().into_owned(),
                exists: product.exists,
                preview_path: product
                    .preview_path
                    .map(|path| path.to_string_lossy().into_owned()),
                preview_exists: product.preview_exists,
                dataset,
                diagnostic,
            }
        })
        .collect();
    Ok(TaskCompletionProjection {
        surface_id: completion.surface_id,
        summary: completion.summary,
        products,
        diagnostics: completion.diagnostics,
    })
}

#[uniffi::export]
pub fn probe_project(path: String) -> FrontendResult<ProjectProbe> {
    let root = PathBuf::from(path);
    let metadata = fs::metadata(&root).map_err(|error| FrontendServiceError::Io {
        reason: format!("metadata {}: {error}", root.display()),
    })?;
    if !metadata.is_dir() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} is not a directory", root.display()),
        });
    }

    let mut scan = ProjectScan::default();
    scan_path(&root, 0, &mut scan);
    let name = root
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("Project")
        .to_string();

    Ok(ProjectProbe {
        name,
        root_path: root.display().to_string(),
        datasets: scan.datasets,
        diagnostics: scan.diagnostics,
        scanned_entry_count: scan.scanned_entry_count as u64,
        truncated: scan.truncated,
    })
}

#[uniffi::export]
pub fn build_measurement_set_summary(
    request: MeasurementSetSummaryRequest,
) -> FrontendResult<MeasurementSetSummaryResult> {
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.is_dir() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!(
                "{} is not a MeasurementSet directory",
                dataset_path.display()
            ),
        });
    }

    let format = match request.format.trim().to_ascii_lowercase().as_str() {
        "" | "text" => MeasurementSetSummaryOutputFormat::Text,
        "json" => MeasurementSetSummaryOutputFormat::Json,
        other => {
            return Err(FrontendServiceError::Probe {
                reason: format!("unsupported MeasurementSet summary format {other:?}"),
            });
        }
    };
    let selection = MsSelectionSpec {
        field: normalized_optional(request.field.clone()),
        spw: normalized_optional(request.spectral_window.clone()),
        timerange: normalized_optional(request.timerange.clone()),
        uvrange: normalized_optional(request.uvrange.clone()),
        antenna: normalized_optional(request.antenna.clone()),
        scan: normalized_optional(request.scan.clone()),
        correlation: normalized_optional(request.correlation.clone()),
        array: normalized_optional(request.array.clone()),
        observation: normalized_optional(request.observation.clone()),
        intent: normalized_optional(request.intent.clone()),
        feed: normalized_optional(request.feed.clone()),
        msselect: normalized_optional(request.msselect.clone()),
        ..MsSelectionSpec::default()
    };
    let started = Instant::now();
    let ms = MeasurementSet::open(&dataset_path).map_err(|error| FrontendServiceError::Probe {
        reason: format!("open MeasurementSet {}: {error}", dataset_path.display()),
    })?;
    let summary = MeasurementSetSummary::from_ms_with_options(&ms, &selection.to_summary_options())
        .map_err(|error| FrontendServiceError::Probe {
            reason: format!(
                "summarize MeasurementSet {}: {error}",
                dataset_path.display()
            ),
        })?;
    let summary_text = summary
        .render(format)
        .map_err(|error| FrontendServiceError::Probe {
            reason: format!("render MeasurementSet summary: {error}"),
        })?;
    let elapsed = started.elapsed();
    Ok(MeasurementSetSummaryResult {
        dataset_path: dataset_path.display().to_string(),
        format: match format {
            MeasurementSetSummaryOutputFormat::Text => "text",
            MeasurementSetSummaryOutputFormat::Json => "json",
        }
        .to_string(),
        summary_text,
        selection_summary: measurement_set_summary_selection_summary(&request),
        diagnostics: vec![format!("timing: summary={} ms", elapsed.as_millis())],
    })
}

#[uniffi::export]
pub fn build_measurement_set_plot(
    request: MeasurementSetPlotRequest,
) -> FrontendResult<MeasurementSetPlotResult> {
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.is_dir() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!(
                "{} is not a MeasurementSet directory",
                dataset_path.display()
            ),
        });
    }

    let width = request.width.clamp(320, 4096);
    let height = request.height.clamp(240, 4096);
    let max_plot_points = if request.max_plot_points == 0 {
        DEFAULT_GUI_MAX_PLOT_POINTS
    } else {
        request.max_plot_points
    };
    let data_column = normalize_data_column(&request.data_column);
    let mut plot = MsPlotSpec::from_preset(ms_plot_preset(request.preset));
    plot.data_column =
        casa_ms::MsDataColumn::parse(&data_column).map_err(|error| FrontendServiceError::Plot {
            reason: format!("{}: {error}", dataset_path.display()),
        })?;
    if let Some(color_by) = normalized_optional(request.color_by.clone()) {
        plot.color_by =
            casa_ms::MsColorAxis::parse(&color_by).map_err(|error| FrontendServiceError::Plot {
                reason: format!("{}: {error}", dataset_path.display()),
            })?;
    }
    plot.averaging.avgchannel = request
        .avgchannel
        .map(usize::try_from)
        .transpose()
        .map_err(|_| FrontendServiceError::Plot {
            reason: "avgchannel exceeds platform usize".to_string(),
        })?;
    plot.averaging.avgtime = request.avgtime;
    plot.averaging.avgscan = request.avgscan;
    plot.averaging.avgfield = request.avgfield;
    plot.averaging.avgbaseline = request.avgbaseline;
    plot.averaging.avgantenna = request.avgantenna;
    plot.averaging.avgspw = request.avgspw;
    plot.averaging.scalar = request.scalar;
    plot.iteration.iteraxis = normalized_optional(request.iteraxis.clone())
        .map(|iteraxis| casa_ms::MsIterationAxis::parse(&iteraxis))
        .transpose()
        .map_err(|error| FrontendServiceError::Plot {
            reason: format!("{}: {error}", dataset_path.display()),
        })?;

    let selection = MsSelectionSpec {
        field: normalized_optional(request.field.clone()),
        spw: normalized_optional(request.spectral_window.clone()),
        timerange: normalized_optional(request.timerange.clone()),
        uvrange: normalized_optional(request.uvrange.clone()),
        antenna: normalized_optional(request.antenna.clone()),
        scan: normalized_optional(request.scan.clone()),
        correlation: normalized_optional(request.correlation.clone()),
        array: normalized_optional(request.array.clone()),
        observation: normalized_optional(request.observation.clone()),
        intent: normalized_optional(request.intent.clone()),
        feed: normalized_optional(request.feed.clone()),
        msselect: normalized_optional(request.msselect.clone()),
        ..MsSelectionSpec::default()
    };
    let spec = MsExploreSpec {
        ms_path: dataset_path.clone(),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection,
        header_items: vec![],
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points: usize::try_from(max_plot_points).unwrap_or(usize::MAX),
        plots: vec![plot],
    };

    let payload_started = Instant::now();
    let payload =
        build_frontend_msexplore_payload(&spec).map_err(|error| FrontendServiceError::Plot {
            reason: format!("{}: {error}", dataset_path.display()),
        })?;
    let payload_elapsed = payload_started.elapsed();
    let metadata_started = Instant::now();
    let mut metadata = plot_payload_metadata(&payload, request.preset, max_plot_points);
    let metadata_elapsed = metadata_started.elapsed();
    let document_started = Instant::now();
    let document = plot_document_payload(&payload, &metadata, request.preset);
    let document_elapsed = document_started.elapsed();
    metadata.sampling.diagnostics.push(format!(
        "timing: payload={} ms, metadata={} ms, document={} ms",
        payload_elapsed.as_millis(),
        metadata_elapsed.as_millis(),
        document_elapsed.as_millis()
    ));

    Ok(MeasurementSetPlotResult {
        preset: request.preset,
        preset_label: ms_plot_preset(request.preset).display_name().to_string(),
        title: metadata.title,
        summary: metadata.summary,
        dataset_path: dataset_path.display().to_string(),
        data_column,
        selection_summary: selection_summary(&request),
        x_axis: metadata.x_axis,
        y_axis: metadata.y_axis,
        series: metadata.series,
        sampling: metadata.sampling,
        document,
        render: PlotRenderProvenance {
            renderer: "casa-rs plot document".to_string(),
            image_format: "none".to_string(),
            width,
            height,
            source: "Rust casa-ms MeasurementSet payload converted to a structured plot document through UniFFI".to_string(),
        },
        image_bytes: Vec::new(),
    })
}

fn build_frontend_msexplore_payload(spec: &MsExploreSpec) -> Result<MsPlotPayload, String> {
    let [plot] = spec.plots.as_slice() else {
        return build_msexplore_payload_from_spec(spec);
    };
    match plot.preset {
        Some(MsPlotPreset::AntennaLayout)
            if spec.selection.spw.is_none() && spec.selection.correlation.is_none() =>
        {
            let ms = MeasurementSet::open(&spec.ms_path).map_err(|error| error.to_string())?;
            build_antenna_layout_payload_fast(&ms, spec.selection.field.as_deref()).map(|payload| {
                MsPlotPayload::ListObs(MeasurementSetPlotPayload::AntennaLayout(payload))
            })
        }
        Some(MsPlotPreset::ScanTimeline) if spec.selection.correlation.is_none() => {
            let ms = MeasurementSet::open(&spec.ms_path).map_err(|error| error.to_string())?;
            build_scan_timeline_payload_fast(
                &ms,
                spec.selection.field.as_deref(),
                spec.selection.spw.as_deref(),
            )
            .map(|payload| MsPlotPayload::ListObs(MeasurementSetPlotPayload::ScanTimeline(payload)))
        }
        Some(MsPlotPreset::SpectralWindowCoverage)
            if spec.selection.field.is_none() && spec.selection.correlation.is_none() =>
        {
            let ms = MeasurementSet::open(&spec.ms_path).map_err(|error| error.to_string())?;
            build_spectral_window_coverage_payload_fast(&ms, spec.selection.spw.as_deref()).map(
                |payload| {
                    MsPlotPayload::ListObs(MeasurementSetPlotPayload::SpectralWindowCoverage(
                        payload,
                    ))
                },
            )
        }
        _ => build_msexplore_payload_from_spec(spec),
    }
}

fn build_antenna_layout_payload_fast(
    ms: &MeasurementSet,
    field_selection: Option<&str>,
) -> Result<AntennaLayoutPlotPayload, String> {
    let selected_field = field_selection.and_then(parse_single_index_selector);
    let used_antennas = selected_field
        .map(|field| selected_antennas_for_field(ms, field))
        .transpose()?;
    let antenna = ms.antenna().map_err(|error| error.to_string())?;
    let mut points = Vec::new();
    let mut omitted = 0usize;
    for row in 0..antenna.row_count() {
        if used_antennas
            .as_ref()
            .is_some_and(|used| !used.contains(&row))
        {
            continue;
        }
        let position = antenna.position(row).map_err(|error| error.to_string())?;
        let x = position[0];
        let y = position[1];
        if !x.is_finite() || !y.is_finite() {
            omitted += 1;
            continue;
        }
        points.push(AntennaLayoutPoint {
            label: antenna.name(row).map_err(|error| error.to_string())?,
            x,
            y,
            marker_radius: ((antenna.dish_diameter(row).unwrap_or(15.0) / 3.0).round() as i32)
                .clamp(3, 12),
        });
    }
    if points.is_empty() {
        return Err("Antenna layout requires finite ANTENNA positions".to_string());
    }
    let suffix = if omitted == 0 {
        String::new()
    } else {
        format!(" ({} omitted without finite coordinates)", omitted)
    };
    Ok(AntennaLayoutPlotPayload {
        x_label: "ITRF X (m)".to_string(),
        y_label: "ITRF Y (m)".to_string(),
        labels_enabled: true,
        summary: format!("Antenna layout. Antennas={}{}", points.len(), suffix),
        antennas: points,
    })
}

fn selected_antennas_for_field(
    ms: &MeasurementSet,
    field_id: usize,
) -> Result<BTreeSet<usize>, String> {
    let main = ms.main_table();
    let field_column = main_ids::field_id(main);
    let antenna1 = main_ids::antenna1(main);
    let antenna2 = main_ids::antenna2(main);
    let mut antennas = BTreeSet::new();
    for row in 0..main.row_count() {
        if field_column.get(row).map_err(|error| error.to_string())? == field_id as i32 {
            let ant1 = antenna1.get(row).map_err(|error| error.to_string())?;
            let ant2 = antenna2.get(row).map_err(|error| error.to_string())?;
            if ant1 >= 0 {
                antennas.insert(ant1 as usize);
            }
            if ant2 >= 0 {
                antennas.insert(ant2 as usize);
            }
        }
    }
    Ok(antennas)
}

fn build_scan_timeline_payload_fast(
    ms: &MeasurementSet,
    field_selection: Option<&str>,
    spw_selection: Option<&str>,
) -> Result<ScanTimelinePlotPayload, String> {
    #[derive(Default)]
    struct ScanAccum {
        lane: usize,
        start: f64,
        end: f64,
        field_ids: BTreeSet<i32>,
        row_count: usize,
    }

    let selected_field = field_selection.and_then(parse_single_index_selector);
    let selected_spw = spw_selection.and_then(parse_single_spw_selector);
    let dd_to_spw = data_description_to_spw(ms)?;
    let field_names = field_names(ms)?;
    let main = ms.main_table();
    let row_numbers = (0..main.row_count()).collect::<Vec<_>>();
    let mut lane_lookup = BTreeMap::<i32, usize>::new();
    let mut scans = BTreeMap::<i32, ScanAccum>::new();

    for row_chunk in row_numbers.chunks(MAIN_SCALAR_CHUNK_ROWS) {
        let scan_values = selected_i32_values(main, "SCAN_NUMBER", row_chunk)?;
        let field_values = selected_i32_values(main, "FIELD_ID", row_chunk)?;
        let data_desc_values = selected_i32_values(main, "DATA_DESC_ID", row_chunk)?;
        let time_values = selected_f64_values(main, "TIME", row_chunk)?;
        for row_slot in 0..row_chunk.len() {
            let row_field = field_values[row_slot];
            if selected_field.is_some_and(|field| row_field != field as i32) {
                continue;
            }
            let row_dd = data_desc_values[row_slot];
            let row_spw = dd_to_spw.get(&row_dd).copied().unwrap_or(-1);
            if selected_spw.is_some_and(|spw| row_spw != spw as i32) {
                continue;
            }
            let scan = scan_values[row_slot];
            let row_time = time_values[row_slot];
            let next_lane = lane_lookup.len();
            let lane = *lane_lookup.entry(scan).or_insert(next_lane);
            let entry = scans.entry(scan).or_insert_with(|| ScanAccum {
                lane,
                start: row_time,
                end: row_time,
                ..ScanAccum::default()
            });
            entry.start = entry.start.min(row_time);
            entry.end = entry.end.max(row_time);
            entry.field_ids.insert(row_field);
            entry.row_count += 1;
        }
    }

    let mut lane_labels = vec![String::new(); lane_lookup.len()];
    for (scan, lane) in &lane_lookup {
        if let Some(label) = lane_labels.get_mut(*lane) {
            *label = format!("Scan {scan}");
        }
    }
    let mut bars = Vec::new();
    let mut start = f64::INFINITY;
    let mut end = f64::NEG_INFINITY;
    for (scan, entry) in scans {
        start = start.min(entry.start);
        end = end.max(entry.end);
        let field_label = entry
            .field_ids
            .iter()
            .next()
            .and_then(|field| field_names.get(field))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        bars.push(ScanTimelineBar {
            lane: entry.lane,
            start_mjd_seconds: entry.start,
            end_mjd_seconds: entry.end.max(entry.start + 1.0e-6),
            label: format!("Scan {scan}"),
            color_group: field_label,
        });
    }
    if !start.is_finite() || !end.is_finite() {
        start = 0.0;
        end = 1.0;
    }
    Ok(ScanTimelinePlotPayload {
        start_mjd_seconds: start,
        end_mjd_seconds: if end > start { end } else { start + 1.0 },
        bars,
        lane_labels,
        summary: format!("Scan timeline. Scans={}", lane_lookup.len()),
    })
}

fn selected_i32_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<i32>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Int32(value) => Ok(value),
        other => Err(format!(
            "plot service requires INT {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

fn selected_f64_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<f64>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Float64(value) => Ok(value),
        other => Err(format!(
            "plot service requires DOUBLE {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

fn selected_scalar_values<T>(
    table: &Table,
    column: &'static str,
    rows: &[usize],
    convert: impl Fn(ScalarValue) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    table
        .column_accessor(column)
        .map_err(|error| error.to_string())?
        .scalar_cells_owned_for_rows(rows)
        .map_err(|error| error.to_string())?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| {
            value
                .ok_or_else(|| format!("plot service requires {column} data for row {row}"))
                .and_then(&convert)
        })
        .collect()
}

fn build_spectral_window_coverage_payload_fast(
    ms: &MeasurementSet,
    spw_selection: Option<&str>,
) -> Result<SpectralWindowCoveragePlotPayload, String> {
    let spectral_windows = ms.spectral_window().map_err(|error| error.to_string())?;
    let selected_spw = spw_selection.and_then(parse_single_spw_selector);
    let mut bars = Vec::new();
    for row in 0..spectral_windows.row_count() {
        if selected_spw.is_some_and(|selected| selected != row) {
            continue;
        }
        let chan_freq = spectral_windows
            .chan_freq(row)
            .map_err(|error| error.to_string())?;
        let chan_width = spectral_windows.chan_width(row).unwrap_or_default();
        let (start, end) = spectral_window_frequency_bounds_ghz(
            &chan_freq,
            &chan_width,
            spectral_windows
                .ref_frequency(row)
                .map_err(|error| error.to_string())?,
            spectral_windows.total_bandwidth(row).unwrap_or(0.0),
        );
        let name = spectral_windows.name(row).unwrap_or_default();
        let label = if name.trim().is_empty() {
            format!("SPW {row}")
        } else {
            format!("SPW {row} {name}")
        };
        bars.push(SpectralWindowCoverageBar {
            spectral_window_id: row,
            lane: bars.len(),
            start,
            end,
            label,
            color_group: format!("spw-{row}"),
        });
    }
    Ok(SpectralWindowCoveragePlotPayload {
        x_label: "Frequency (GHz)".to_string(),
        summary: format!("Spectral window coverage. Windows={}", bars.len()),
        bars,
    })
}

fn parse_single_spw_selector(value: &str) -> Option<usize> {
    parse_single_index_selector(value)
}

fn parse_single_index_selector(value: &str) -> Option<usize> {
    let trimmed = value.trim();
    let without_prefix = trimmed.strip_prefix("spw ").unwrap_or(trimmed);
    without_prefix
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

fn data_description_to_spw(ms: &MeasurementSet) -> Result<BTreeMap<i32, i32>, String> {
    let data_description = ms.data_description().map_err(|error| error.to_string())?;
    let mut lookup = BTreeMap::new();
    for row in 0..data_description.row_count() {
        lookup.insert(
            row as i32,
            data_description
                .spectral_window_id(row)
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(lookup)
}

fn field_names(ms: &MeasurementSet) -> Result<BTreeMap<i32, String>, String> {
    let fields = ms.field().map_err(|error| error.to_string())?;
    let mut names = BTreeMap::new();
    for row in 0..fields.row_count() {
        names.insert(
            row as i32,
            fields.name(row).map_err(|error| error.to_string())?,
        );
    }
    Ok(names)
}

fn spectral_window_frequency_bounds_ghz(
    chan_freq_hz: &[f64],
    chan_width_hz: &[f64],
    ref_frequency_hz: f64,
    total_bandwidth_hz: f64,
) -> (f64, f64) {
    let mut bounds = None;
    for (index, frequency) in chan_freq_hz.iter().copied().enumerate() {
        let half_width = chan_width_hz
            .get(index)
            .copied()
            .or_else(|| chan_width_hz.first().copied())
            .unwrap_or(0.0)
            .abs()
            / 2.0;
        bounds = bounds_accumulator(bounds, frequency - half_width);
        bounds = bounds_accumulator(bounds, frequency + half_width);
    }
    let (lower, upper) = bounds.unwrap_or_else(|| {
        let half_width = total_bandwidth_hz.abs() / 2.0;
        (ref_frequency_hz - half_width, ref_frequency_hz + half_width)
    });
    (lower / 1.0e9, upper / 1.0e9)
}

fn required_image_command_field<T>(
    command: &str,
    field: &str,
    value: Option<T>,
) -> Result<T, String> {
    value.ok_or_else(|| format!("image explorer command `{command}` requires `{field}`"))
}

fn image_region_reference_owner(
    reference: ImageExplorerRegionReference,
) -> casars_imagebrowser_protocol::ImageRegionReference {
    match reference {
        ImageExplorerRegionReference::None => {
            casars_imagebrowser_protocol::ImageRegionReference::None
        }
        ImageExplorerRegionReference::Definition { name } => {
            casars_imagebrowser_protocol::ImageRegionReference::Definition { name }
        }
        ImageExplorerRegionReference::File { path } => {
            casars_imagebrowser_protocol::ImageRegionReference::File { path }
        }
        ImageExplorerRegionReference::Expression { expression } => {
            casars_imagebrowser_protocol::ImageRegionReference::Expression { expression }
        }
    }
}

fn image_explorer_command_owner(
    command: ImageExplorerCommand,
) -> Result<ImageBrowserCommand, String> {
    let name = command.command.as_str();
    Ok(match name {
        "start_region_shape" => ImageBrowserCommand::StartRegionShape,
        "append_region_vertex" => ImageBrowserCommand::AppendRegionVertex {
            x: required_image_command_field(name, "x", command.x)? as usize,
            y: required_image_command_field(name, "y", command.y)? as usize,
        },
        "close_region_shape" => ImageBrowserCommand::CloseRegionShape,
        "undo_region_vertex" => ImageBrowserCommand::UndoRegionVertex,
        "cancel_region_shape" => ImageBrowserCommand::CancelRegionShape,
        "clear_region" => ImageBrowserCommand::ClearRegion,
        "save_region_definition" => ImageBrowserCommand::SaveRegionDefinition,
        "load_next_region_definition" => ImageBrowserCommand::LoadNextRegionDefinition,
        "load_region_definition" => ImageBrowserCommand::LoadRegionDefinition {
            name: required_image_command_field(name, "name", command.name)?,
        },
        "delete_region_definition" => ImageBrowserCommand::DeleteRegionDefinition {
            name: required_image_command_field(name, "name", command.name)?,
        },
        "set_default_mask" => ImageBrowserCommand::SetDefaultMask {
            name: required_image_command_field(name, "name", command.name)?,
        },
        "unset_default_mask" => ImageBrowserCommand::UnsetDefaultMask,
        "delete_mask" => ImageBrowserCommand::DeleteMask {
            name: required_image_command_field(name, "name", command.name)?,
        },
        "write_region_mask" => ImageBrowserCommand::WriteRegionMask {
            name: command.name,
            set_default: command.set_default.unwrap_or(false),
        },
        "export_region_file" => ImageBrowserCommand::ExportRegionFile {
            path: required_image_command_field(name, "path", command.path)?,
        },
        "load_region_file" => ImageBrowserCommand::LoadRegionFile {
            path: required_image_command_field(name, "path", command.path)?,
        },
        "append_region_file" => ImageBrowserCommand::AppendRegionFile {
            path: required_image_command_field(name, "path", command.path)?,
        },
        "set_selection_references" => ImageBrowserCommand::SetSelectionReferences {
            region: command.region.map(image_region_reference_owner),
            mask: None,
        },
        other => return Err(format!("unknown image explorer command `{other}`")),
    })
}

fn image_axis_value_projection(
    value: &casars_imagebrowser_protocol::ImageBrowserAxisValue,
) -> ImageExplorerAxisValue {
    ImageExplorerAxisValue {
        name: value.name.clone(),
        unit: value.unit.clone(),
        value: value.value,
    }
}

fn image_explorer_snapshot_projection(
    snapshot: casars_imagebrowser_protocol::ImageBrowserSnapshot,
) -> ImageExplorerSnapshot {
    ImageExplorerSnapshot {
        status_line: snapshot.status_line,
        active_view: serde_plain_view_name(snapshot.active_view.label()),
        focus: snake_case_debug(snapshot.focus),
        shape: snapshot
            .shape
            .into_iter()
            .map(|value| value as u64)
            .collect(),
        parameters: (&snapshot.parameters).into(),
        inspector_lines: snapshot.inspector_lines,
        content_lines: snapshot.content_lines,
        navigation: ImageExplorerNavigation {
            selected_index: snapshot.navigation.selected_index as u64,
            total_items: snapshot.navigation.total_items as u64,
            viewport_items: snapshot.navigation.viewport_items as u64,
        },
        plane: snapshot.plane.map(|plane| ImageExplorerPlane {
            width: plane.width as u64,
            height: plane.height as u64,
            pixels_u8: plane.pixels_u8,
            clip_min: plane.clip_min,
            clip_max: plane.clip_max,
            data_min: plane.data_min,
            data_max: plane.data_max,
            value_unit: plane.value_unit,
            histogram_bins: plane.histogram_bins,
            masked_or_non_finite_count: plane.masked_or_non_finite_count as u64,
            no_finite_values: plane.no_finite_values,
        }),
        probe: snapshot.probe.map(|probe| ImageExplorerProbe {
            pixel_indices: probe
                .pixel_indices
                .into_iter()
                .map(|value| value as u64)
                .collect(),
            pixel_axes: probe
                .pixel_axes
                .iter()
                .map(image_axis_value_projection)
                .collect(),
            value: probe.value,
            masked: probe.masked,
            finite: probe.finite,
            world_axes: probe
                .world_axes
                .iter()
                .map(image_axis_value_projection)
                .collect(),
        }),
        profile: snapshot.profile.map(|profile| ImageExplorerProfile {
            axis: profile.axis as u64,
            axis_name: profile.axis_name,
            axis_unit: profile.axis_unit,
            value_unit: profile.value_unit,
            coord_type: profile.coord_type,
            selected_sample_index: profile.selected_sample_index as u64,
            samples: profile
                .samples
                .into_iter()
                .map(|sample| ImageExplorerProfileSample {
                    sample_index: sample.sample_index as u64,
                    pixel_index: sample.pixel_index as u64,
                    value: sample.value,
                    masked: sample.masked,
                    finite: sample.finite,
                    world_axis: sample.world_axis.as_ref().map(image_axis_value_projection),
                })
                .collect(),
        }),
        display_axes: snapshot
            .display_axes
            .into_iter()
            .map(|axis| ImageExplorerDisplayAxis {
                axis: axis.axis as u64,
                name: axis.name,
                unit: axis.unit,
                blc: axis.blc as u64,
                trc: axis.trc as u64,
                inc: axis.inc as u64,
                sampled_len: axis.sampled_len as u64,
                world_increment: axis.world_increment,
            })
            .collect(),
        plane_cursor: snapshot
            .plane_cursor
            .map(|cursor| ImageExplorerPlaneCursor {
                sampled_x: cursor.sampled_x as u64,
                sampled_y: cursor.sampled_y as u64,
                pixel_x: cursor.pixel_x as u64,
                pixel_y: cursor.pixel_y as u64,
            }),
        non_display_axes: snapshot
            .non_display_axes
            .into_iter()
            .map(|axis| ImageExplorerNonDisplayAxis {
                axis: axis.axis as u64,
                label: axis.label,
                index: axis.index as u64,
                length: axis.length as u64,
                pixel: axis.pixel as u64,
            })
            .collect(),
        region: snapshot.region.map(|region| ImageExplorerRegion {
            label: region.label,
            shape_count: region.shape_count as u64,
            closed_shape_count: region.closed_shape_count as u64,
            editing: region.editing,
            active_shape_vertices: region.active_shape_vertices as u64,
            overlay_shapes: region
                .overlay_shapes
                .into_iter()
                .map(|shape| ImageExplorerRegionOverlayShape {
                    vertices: shape
                        .vertices
                        .into_iter()
                        .map(|vertex| ImageExplorerRegionOverlayVertex {
                            sampled_x: vertex.sampled_x,
                            sampled_y: vertex.sampled_y,
                        })
                        .collect(),
                    closed: shape.closed,
                })
                .collect(),
            stats: region.stats.map(|stats| ImageExplorerRegionStats {
                pixel_count: stats.pixel_count as u64,
                median: stats.median,
                min: stats.min,
                max: stats.max,
                mean: stats.mean,
                sigma: stats.sigma,
                rms: stats.rms,
                sum: stats.sum,
                value_unit: stats.value_unit,
            }),
        }),
        saved_region_names: snapshot.saved_region_names,
        active_region_definition_name: snapshot.active_region_definition_name,
        mask_names: snapshot.mask_names,
        default_mask_name: snapshot.default_mask_name,
        backend_timing: snapshot
            .backend_timing
            .map(|timing| ImageExplorerBackendTiming {
                plane_cache_result: snake_case_debug(timing.plane_cache_result),
                cached_plane_lookup_ns: timing.cached_plane_lookup_ns,
                plane_extract_ns: timing.plane_extract_ns,
                stat_collection_ns: timing.stat_collection_ns,
                histogram_ns: timing.histogram_ns,
                rasterize_ns: timing.rasterize_ns,
                total_plane_ns: timing.total_plane_ns,
                profile_cache_hits: timing.profile_cache_hits,
                profile_cache_misses: timing.profile_cache_misses,
                profile_extract_total_ns: timing.profile_extract_total_ns,
            }),
        capabilities: ImageExplorerCapabilities {
            renderable_plane: snapshot.capabilities.renderable_plane,
            world_coords_available: snapshot.capabilities.world_coords_available,
            pixel_only_mode: snapshot.capabilities.pixel_only_mode,
            non_display_axis_selectors: snapshot.capabilities.non_display_axis_selectors,
            mask_present: snapshot.capabilities.mask_present,
            complex_unsupported: snapshot.capabilities.complex_unsupported,
        },
    }
}

#[uniffi::export]
pub fn build_image_explorer_snapshot(
    request: ImageExplorerSnapshotRequest,
) -> FrontendResult<ImageExplorerSnapshot> {
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let viewport = ImageBrowserViewport::with_plane_pixels(
        default_image_browser_width(),
        default_image_browser_height(),
        default_image_browser_inspector_height(),
        default_image_browser_plane_pixel_width(),
        default_image_browser_plane_pixel_height(),
    );
    let parameters: ImageBrowserParameters = request.parameters.clone().into();
    let mut session =
        ImageBrowserSession::open_with_parameters(&dataset_path, viewport, Some(&parameters))
            .map_err(|error| FrontendServiceError::ImageExplorer {
                reason: format!("open {}: {error}", dataset_path.display()),
            })?;
    apply_image_explorer_snapshot_request(&mut session, &request).map_err(|error| {
        FrontendServiceError::ImageExplorer {
            reason: format!("snapshot {}: {error}", dataset_path.display()),
        }
    })?;
    let mode = parse_image_plane_content_mode(Some(&request.plane_content_mode))
        .map_err(|reason| FrontendServiceError::ImageExplorer { reason })?;
    let snapshot = if !request.non_display_indices.is_empty() {
        session
            .preview_occurrence(&ImageBrowserPreviewRequest {
                viewport,
                parameters,
                plane_content_mode: mode,
                non_display_indices: request
                    .non_display_indices
                    .iter()
                    .copied()
                    .map(|index| index as usize)
                    .collect(),
                include_profile: request.include_profile,
            })
            .map(|payload| *payload.snapshot)
    } else if request.include_profile {
        session.snapshot()
    } else {
        session.handle_command(ImageBrowserCommand::GetSnapshot)
    }
    .map_err(|error| FrontendServiceError::ImageExplorer {
        reason: format!("snapshot {}: {error}", dataset_path.display()),
    })?;
    Ok(image_explorer_snapshot_projection(snapshot))
}

#[uniffi::export]
pub fn build_table_browser_snapshot_json(
    dataset_path: String,
    width: u16,
    height: u16,
    inspector_height: u16,
    view: Option<String>,
) -> FrontendResult<String> {
    let dataset_path = PathBuf::from(dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let mut browser =
        TableBrowser::open(&dataset_path).map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        })?;
    let viewport =
        BrowserViewport::with_inspector_height(width.max(20), height.max(8), inspector_height);
    if let Some(view) = view.as_deref() {
        browser.set_view(
            parse_table_browser_view(view)
                .map_err(|reason| FrontendServiceError::TableExplorer { reason })?,
        );
    }
    browser
        .apply(casars_tablebrowser_protocol::BrowserCommand::GetSnapshot {
            viewport: Some(viewport),
        })
        .map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("snapshot {}: {error}", dataset_path.display()),
        })
        .and_then(|snapshot| {
            serde_json::to_string(&snapshot).map_err(|error| FrontendServiceError::TableExplorer {
                reason: format!("encode snapshot {}: {error}", dataset_path.display()),
            })
        })
}

#[uniffi::export]
pub fn build_table_browser_snapshot_from_request_json(
    request_json: String,
) -> FrontendResult<String> {
    let request: TableBrowserSnapshotRequest =
        serde_json::from_str(&request_json).map_err(|error| {
            FrontendServiceError::TableExplorer {
                reason: format!("decode table browser snapshot request: {error}"),
            }
        })?;
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let mut browser =
        TableBrowser::open(&dataset_path).map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        })?;
    let viewport = BrowserViewport::with_inspector_height(
        request.width.max(20),
        request.height.max(8),
        request.inspector_height,
    );
    if let Some(view) = request.selected_view.as_deref() {
        browser.set_view(
            parse_table_browser_view(view)
                .map_err(|reason| FrontendServiceError::TableExplorer { reason })?,
        );
    }
    if let Some(focus) = parse_table_browser_focus(request.focus.as_deref())
        .map_err(|reason| FrontendServiceError::TableExplorer { reason })?
    {
        browser
            .apply(BrowserCommand::SetFocus {
                focus,
                viewport: Some(viewport),
            })
            .map_err(|error| FrontendServiceError::TableExplorer {
                reason: format!("set focus {}: {error}", dataset_path.display()),
            })?;
    }
    for command in request
        .commands
        .iter()
        .chain(request.transient_commands.iter())
        .cloned()
    {
        browser
            .apply(command)
            .map_err(|error| FrontendServiceError::TableExplorer {
                reason: format!("apply command {}: {error}", dataset_path.display()),
            })?;
    }
    browser
        .apply(BrowserCommand::GetSnapshot {
            viewport: Some(viewport),
        })
        .map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("snapshot {}: {error}", dataset_path.display()),
        })
        .and_then(|snapshot| {
            serde_json::to_string(&snapshot).map_err(|error| FrontendServiceError::TableExplorer {
                reason: format!("encode snapshot {}: {error}", dataset_path.display()),
            })
        })
}

#[uniffi::export]
pub fn build_table_browser_cell_window_json(request_json: String) -> FrontendResult<String> {
    let request: TableBrowserCellWindowRequest =
        serde_json::from_str(&request_json).map_err(|error| {
            FrontendServiceError::TableExplorer {
                reason: format!("decode table browser cell window request: {error}"),
            }
        })?;
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let table = Table::open(TableOptions::new(&dataset_path)).map_err(|error| {
        FrontendServiceError::TableExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        }
    })?;
    let snapshot = build_table_browser_cell_window(&table, &dataset_path, &request)?;
    serde_json::to_string(&snapshot).map_err(|error| FrontendServiceError::TableExplorer {
        reason: format!("encode cell window {}: {error}", dataset_path.display()),
    })
}

#[uniffi::export]
pub fn build_table_browser_cell_value_json(request_json: String) -> FrontendResult<String> {
    let request: TableBrowserCellValueRequest =
        serde_json::from_str(&request_json).map_err(|error| {
            FrontendServiceError::TableExplorer {
                reason: format!("decode table browser cell value request: {error}"),
            }
        })?;
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let table = Table::open(TableOptions::new(&dataset_path)).map_err(|error| {
        FrontendServiceError::TableExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        }
    })?;
    let column_names = table_browser_cell_column_names(&table)?;
    let column_name = column_names
        .get(request.column_index as usize)
        .ok_or_else(|| FrontendServiceError::TableExplorer {
            reason: format!(
                "column index {} is outside {} columns",
                request.column_index,
                column_names.len()
            ),
        })?;
    let row_index = request.row_index as usize;
    if row_index >= table.row_count() {
        return Err(FrontendServiceError::TableExplorer {
            reason: format!(
                "row index {} is outside {} rows",
                row_index,
                table.row_count()
            ),
        });
    }
    let value = table
        .cell_accessor(row_index, column_name)
        .and_then(|accessor| accessor.value())
        .map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!(
                "read {} row {} column {}: {error}",
                dataset_path.display(),
                row_index,
                column_name
            ),
        })?;
    serde_json::to_string(&format_table_browser_copy_value(value)).map_err(|error| {
        FrontendServiceError::TableExplorer {
            reason: format!("encode cell value {}: {error}", dataset_path.display()),
        }
    })
}

fn build_table_browser_cell_window(
    table: &Table,
    table_path: &Path,
    request: &TableBrowserCellWindowRequest,
) -> FrontendResult<TableBrowserCellWindowSnapshot> {
    let column_names = table_browser_cell_column_names(table)?;
    let columns = column_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let header = table_browser_cell_column_header(table, name);
            let width = table_browser_cell_column_width(&header);
            TableBrowserCellWindowColumn {
                index: index as u64,
                name: name.clone(),
                header,
                summary: table_browser_cell_column_summary(table, name),
                width: width as u64,
                keywords: table_browser_column_keyword_lines(table, name),
            }
        })
        .collect::<Vec<_>>();
    let column_display_options = request
        .column_options
        .iter()
        .map(|option| (option.column_index, option.array_inline_limit as usize))
        .collect::<BTreeMap<_, _>>();

    let row_count = table.row_count();
    let row_start = (request.row_start as usize).min(row_count);
    let row_limit = request.row_limit.clamp(1, 4096) as usize;
    let row_end = row_start.saturating_add(row_limit).min(row_count);
    let column_count = columns.len();
    let column_start = (request.column_start as usize).min(column_count);
    let column_limit = request.column_limit.clamp(1, 128) as usize;
    let column_end = column_start.saturating_add(column_limit).min(column_count);
    let visible_columns = &columns[column_start..column_end];
    let mut rows = Vec::with_capacity(row_end.saturating_sub(row_start));
    for row_index in row_start..row_end {
        let mut cells = Vec::with_capacity(visible_columns.len());
        for column in visible_columns {
            let array_inline_limit = column_display_options
                .get(&column.index)
                .copied()
                .unwrap_or(0);
            let (display, defined) =
                table_browser_cell_display(table, row_index, &column.name, array_inline_limit)
                    .map_err(|error| FrontendServiceError::TableExplorer {
                        reason: format!(
                            "read {} row {} column {}: {error}",
                            table_path.display(),
                            row_index,
                            column.name
                        ),
                    })?;
            cells.push(TableBrowserCellWindowCell {
                column_index: column.index,
                display,
                defined,
            });
        }
        rows.push(TableBrowserCellWindowRow {
            index: row_index as u64,
            cells,
        });
    }

    Ok(TableBrowserCellWindowSnapshot {
        table_path: table_path.display().to_string(),
        row_count: row_count as u64,
        column_count: column_count as u64,
        row_start: row_start as u64,
        column_start: column_start as u64,
        columns,
        rows,
    })
}

fn table_browser_cell_column_names(table: &Table) -> FrontendResult<Vec<String>> {
    if let Some(schema) = table.schema() {
        return Ok(schema
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect());
    }

    let mut names = BTreeSet::new();
    for row in table
        .rows()
        .map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("read schema-free table rows: {error}"),
        })?
    {
        for field in row.fields() {
            names.insert(field.name.clone());
        }
    }
    Ok(names.into_iter().collect())
}

fn table_browser_cell_column_header(table: &Table, name: &str) -> String {
    let Some(schema) = table.schema().and_then(|schema| schema.column(name)) else {
        return name.to_string();
    };
    format!("{name} {}", table_browser_cell_column_type_label(schema))
}

fn table_browser_cell_column_summary(table: &Table, name: &str) -> String {
    table
        .schema()
        .and_then(|schema| schema.column(name))
        .map(table_browser_cell_column_summary_label)
        .unwrap_or_else(|| "Dynamic".to_string())
}

fn table_browser_cell_column_width(header: &str) -> usize {
    header.chars().count().clamp(8, 32)
}

fn table_browser_cell_display(
    table: &Table,
    row_index: usize,
    name: &str,
    array_inline_limit: usize,
) -> Result<(String, bool), casa_tables::TableError> {
    if let Some(column) = table.schema().and_then(|schema| schema.column(name)) {
        match column.column_type() {
            ColumnType::Array(_) => {
                if array_inline_limit > 0
                    && let Some(display) = table_browser_expanded_array_display(
                        table,
                        row_index,
                        name,
                        column,
                        array_inline_limit,
                    )?
                {
                    return Ok((display, true));
                }
                return Ok((format_table_browser_schema_array(column), true));
            }
            ColumnType::Record => {
                return Ok(("record".to_string(), true));
            }
            ColumnType::Scalar => {}
        }
    }

    let value = table.cell_accessor(row_index, name)?.value()?;
    Ok((format_table_browser_cell_value(value), value.is_some()))
}

fn table_browser_expanded_array_display(
    table: &Table,
    row_index: usize,
    name: &str,
    column: &casa_tables::ColumnSchema,
    array_inline_limit: usize,
) -> Result<Option<String>, casa_tables::TableError> {
    let fixed_size = match column.column_type() {
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
            Some(shape.iter().copied().product::<usize>())
        }
        ColumnType::Array(ArrayShapeContract::Variable { .. }) => None,
        ColumnType::Scalar | ColumnType::Record => return Ok(None),
    };
    if let Some(size) = fixed_size
        && size > array_inline_limit
    {
        return Ok(None);
    }
    let Some(value) = table.cell_accessor(row_index, name)?.value()? else {
        return Ok(Some("<undef>".to_string()));
    };
    let Value::Array(array) = value else {
        return Ok(None);
    };
    if array.len() > array_inline_limit {
        return Ok(Some(table_browser_array_type_label(array)));
    }
    Ok(Some(format_table_browser_array_expanded(array)))
}

fn format_table_browser_schema_array(column: &casa_tables::ColumnSchema) -> String {
    let primitive = column
        .data_type()
        .map(table_browser_short_primitive_name)
        .unwrap_or("dyn");
    match column.column_type() {
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
            format!("{primitive}[{}]", table_browser_shape_label(shape))
        }
        ColumnType::Array(ArrayShapeContract::Variable { ndim: Some(ndim) }) => {
            format!("{primitive}[{ndim}d]")
        }
        ColumnType::Array(ArrayShapeContract::Variable { ndim: None }) => {
            format!("{primitive}[]")
        }
        ColumnType::Scalar | ColumnType::Record => primitive.to_string(),
    }
}

fn table_browser_shape_label(shape: &[usize]) -> String {
    if shape.is_empty() {
        return "scalar".to_string();
    }
    shape
        .iter()
        .map(|extent| extent.to_string())
        .collect::<Vec<_>>()
        .join("x")
}

fn table_browser_column_keyword_lines(table: &Table, name: &str) -> Vec<String> {
    let Some(record) = table.column_keywords(name) else {
        return Vec::new();
    };
    let mut lines = Vec::new();
    push_table_browser_keyword_lines(record, &[], &mut lines);
    lines
}

fn push_table_browser_keyword_lines(
    record: &casa_types::RecordValue,
    prefix: &[String],
    lines: &mut Vec<String>,
) {
    for field in record.fields() {
        if matches!(field.value, Value::TableRef(_)) {
            continue;
        }
        let mut path = prefix.to_vec();
        path.push(field.name.clone());
        match &field.value {
            Value::Record(record) => push_table_browser_keyword_lines(record, &path, lines),
            value => lines.push(format!(
                "{} = {}",
                path.join("."),
                format_table_browser_cell_value(Some(value))
            )),
        }
    }
}

fn table_browser_cell_column_type_label(column: &casa_tables::ColumnSchema) -> String {
    match column.column_type() {
        ColumnType::Scalar => column
            .data_type()
            .map(table_browser_short_primitive_name)
            .unwrap_or("dyn")
            .to_string(),
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => format!(
            "{}[{}]",
            column
                .data_type()
                .map(table_browser_short_primitive_name)
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
                .map(table_browser_short_primitive_name)
                .unwrap_or("dyn"),
            ndim
        ),
        ColumnType::Array(ArrayShapeContract::Variable { ndim: None }) => format!(
            "{}[]",
            column
                .data_type()
                .map(table_browser_short_primitive_name)
                .unwrap_or("dyn")
        ),
        ColumnType::Record => "record".to_string(),
    }
}

fn table_browser_cell_column_summary_label(column: &casa_tables::ColumnSchema) -> String {
    match column.column_type() {
        ColumnType::Scalar => format!(
            "Scalar {:?}",
            column
                .data_type()
                .expect("scalar columns always carry a data type")
        ),
        ColumnType::Array(contract) => format!(
            "Array<{:?}> {:?}",
            column
                .data_type()
                .expect("array columns always carry a data type"),
            contract
        ),
        ColumnType::Record => "Record".to_string(),
    }
}

fn table_browser_short_primitive_name(primitive: PrimitiveType) -> &'static str {
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

fn format_table_browser_cell_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::Scalar(scalar)) => format_table_browser_scalar(scalar),
        Some(Value::Array(array)) => format_table_browser_array(array),
        Some(Value::Record(record)) => format!("record{{{}}}", record.fields().len()),
        Some(Value::TableRef(path)) => format!("table({path})"),
        None => "<undef>".to_string(),
    }
}

fn format_table_browser_copy_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::Array(array)) => table_browser_array_all_values(array),
        other => format_table_browser_cell_value(other),
    }
}

fn format_table_browser_scalar(scalar: &ScalarValue) -> String {
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

fn format_table_browser_array(array: &ArrayValue) -> String {
    if array.ndim() <= 1 && array.len() <= 3 {
        return table_browser_array_preview(array);
    }
    format!(
        "{} {}",
        table_browser_array_type_label(array),
        table_browser_array_preview(array)
    )
}

fn format_table_browser_array_expanded(array: &ArrayValue) -> String {
    table_browser_array_all_values(array)
}

fn table_browser_array_type_label(array: &ArrayValue) -> String {
    format!(
        "{}[{}]",
        table_browser_short_primitive_name(array.primitive_type()),
        table_browser_shape_label(array.shape())
    )
}

fn table_browser_array_all_values(array: &ArrayValue) -> String {
    match array {
        ArrayValue::Bool(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt8(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt16(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt32(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int16(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int32(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int64(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Float32(values) => {
            table_browser_all_values(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Float64(values) => {
            table_browser_all_values(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Complex32(values) => table_browser_all_values(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::Complex64(values) => table_browser_all_values(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::String(values) => {
            table_browser_all_values(values.iter().map(|value| format!("{value:?}")))
        }
    }
}

fn table_browser_array_preview(array: &ArrayValue) -> String {
    match array {
        ArrayValue::Bool(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt8(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt16(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt32(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int16(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int32(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int64(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Float32(values) => {
            table_browser_preview_values(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Float64(values) => {
            table_browser_preview_values(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Complex32(values) => table_browser_preview_values(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::Complex64(values) => table_browser_preview_values(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::String(values) => {
            table_browser_preview_values(values.iter().map(|value| format!("{value:?}")))
        }
    }
}

fn table_browser_all_values(values: impl Iterator<Item = String>) -> String {
    format!("[{}]", values.collect::<Vec<_>>().join(", "))
}

fn table_browser_preview_values(values: impl Iterator<Item = String>) -> String {
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

fn image_snapshot_for_requested_view(
    session: &mut ImageBrowserSession,
    active_view: Option<&str>,
) -> Result<casars_imagebrowser_protocol::ImageBrowserSnapshot, casa_images::ImageError> {
    let Some(active_view) = active_view else {
        return session.snapshot();
    };
    let target = active_view.trim();
    if target.is_empty() {
        return session.snapshot();
    }
    for _ in 0..4 {
        let snapshot = session.snapshot()?;
        if snapshot.active_view.label().eq_ignore_ascii_case(target)
            || format!("{:?}", snapshot.active_view).eq_ignore_ascii_case(target)
            || serde_plain_view_name(snapshot.active_view.label()).eq_ignore_ascii_case(target)
        {
            return Ok(snapshot);
        }
        session.handle_command(
            casars_imagebrowser_protocol::ImageBrowserCommand::CycleView { forward: true },
        )?;
    }
    session.snapshot()
}

fn apply_image_explorer_snapshot_request(
    session: &mut ImageBrowserSession,
    request: &ImageExplorerSnapshotRequest,
) -> Result<(), casa_images::ImageError> {
    let mode = parse_image_plane_content_mode(Some(&request.plane_content_mode))
        .map_err(casa_images::ImageError::InvalidMetadata)?;
    session.handle_command(ImageBrowserCommand::SetPlaneContentMode { mode })?;
    if let Some(focus) = parse_image_browser_focus(Some(&request.focus))
        .map_err(casa_images::ImageError::InvalidMetadata)?
    {
        session.handle_command(ImageBrowserCommand::SetFocus { focus })?;
    }
    if let (Some(x), Some(y)) = (request.cursor_x, request.cursor_y) {
        session.handle_command(ImageBrowserCommand::SetCursor {
            x: x as usize,
            y: y as usize,
        })?;
    }
    if let Some(axis) = request.selected_profile_axis {
        session.handle_command(ImageBrowserCommand::SetSelectedNonDisplayAxis {
            axis: axis as usize,
        })?;
    }
    for command in request
        .commands
        .iter()
        .chain(request.transient_commands.iter())
        .cloned()
    {
        session.handle_command(
            image_explorer_command_owner(command)
                .map_err(casa_images::ImageError::InvalidMetadata)?,
        )?;
    }
    let _ = image_snapshot_for_requested_view(session, Some(&request.selected_view))?;
    Ok(())
}

fn parse_image_browser_focus(value: Option<&str>) -> Result<Option<ImageBrowserFocus>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" => Ok(None),
        "content" => Ok(Some(ImageBrowserFocus::Content)),
        "inspector" => Ok(Some(ImageBrowserFocus::Inspector)),
        other => Err(format!("unknown image browser focus {other:?}")),
    }
}

fn parse_image_plane_content_mode(value: Option<&str>) -> Result<ImagePlaneContentMode, String> {
    let Some(value) = value else {
        return Ok(ImagePlaneContentMode::Raster);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "raster" => Ok(ImagePlaneContentMode::Raster),
        "spreadsheet" => Ok(ImagePlaneContentMode::Spreadsheet),
        other => Err(format!("unknown image plane content mode {other:?}")),
    }
}

fn serde_plain_view_name(label: &str) -> String {
    label.replace(' ', "_").to_ascii_lowercase()
}

fn parse_table_browser_view(value: &str) -> Result<BrowserView, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "overview" => Ok(BrowserView::Overview),
        "columns" => Ok(BrowserView::Columns),
        "keywords" => Ok(BrowserView::Keywords),
        "cells" => Ok(BrowserView::Cells),
        "subtables" => Ok(BrowserView::Subtables),
        other => Err(format!("unknown table browser view {other:?}")),
    }
}

fn parse_table_browser_focus(value: Option<&str>) -> Result<Option<BrowserFocus>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" => Ok(None),
        "main" => Ok(Some(BrowserFocus::Main)),
        "inspector" => Ok(Some(BrowserFocus::Inspector)),
        other => Err(format!("unknown table browser focus {other:?}")),
    }
}

struct PayloadMetadata {
    title: String,
    summary: String,
    x_axis: PlotAxisMetadata,
    y_axis: PlotAxisMetadata,
    series: Vec<PlotSeriesMetadata>,
    sampling: PlotSamplingDiagnostics,
}

fn ms_plot_preset(preset: MeasurementSetPlotPreset) -> MsPlotPreset {
    match preset {
        MeasurementSetPlotPreset::UvCoverage => MsPlotPreset::UvCoverage,
        MeasurementSetPlotPreset::AntennaLayout => MsPlotPreset::AntennaLayout,
        MeasurementSetPlotPreset::ScanTimeline => MsPlotPreset::ScanTimeline,
        MeasurementSetPlotPreset::SpectralWindowCoverage => MsPlotPreset::SpectralWindowCoverage,
        MeasurementSetPlotPreset::PhaseVsTime => MsPlotPreset::PhaseVsTime,
        MeasurementSetPlotPreset::AmplitudePhaseVsTimeStacked => {
            MsPlotPreset::AmplitudePhaseVsTimeStacked
        }
        MeasurementSetPlotPreset::WeightVsTime => MsPlotPreset::WeightVsTime,
        MeasurementSetPlotPreset::SigmaVsTime => MsPlotPreset::SigmaVsTime,
        MeasurementSetPlotPreset::FlagVsTime => MsPlotPreset::FlagVsTime,
        MeasurementSetPlotPreset::WeightSpectrumVsTime => MsPlotPreset::WeightSpectrumVsTime,
        MeasurementSetPlotPreset::SigmaSpectrumVsTime => MsPlotPreset::SigmaSpectrumVsTime,
        MeasurementSetPlotPreset::FlagRowVsTime => MsPlotPreset::FlagRowVsTime,
        MeasurementSetPlotPreset::ElevationVsTime => MsPlotPreset::ElevationVsTime,
        MeasurementSetPlotPreset::AzimuthVsTime => MsPlotPreset::AzimuthVsTime,
        MeasurementSetPlotPreset::HourAngleVsTime => MsPlotPreset::HourAngleVsTime,
        MeasurementSetPlotPreset::ParallacticAngleVsTime => MsPlotPreset::ParallacticAngleVsTime,
        MeasurementSetPlotPreset::AzimuthVsElevation => MsPlotPreset::AzimuthVsElevation,
        MeasurementSetPlotPreset::AmplitudeVsFrequency => MsPlotPreset::AmplitudeVsFrequency,
        MeasurementSetPlotPreset::AmplitudeVsChannel => MsPlotPreset::AmplitudeVsChannel,
        MeasurementSetPlotPreset::PhaseVsChannel => MsPlotPreset::PhaseVsChannel,
        MeasurementSetPlotPreset::PhaseVsFrequency => MsPlotPreset::PhaseVsFrequency,
        MeasurementSetPlotPreset::AmplitudeVsVelocity => MsPlotPreset::AmplitudeVsVelocity,
        MeasurementSetPlotPreset::PhaseVsVelocity => MsPlotPreset::PhaseVsVelocity,
        MeasurementSetPlotPreset::AmplitudeVsUvDistance => MsPlotPreset::AmplitudeVsUvDistance,
        MeasurementSetPlotPreset::AmplitudeVsTime => MsPlotPreset::AmplitudeVsTime,
        MeasurementSetPlotPreset::RealVsImaginary => MsPlotPreset::RealVsImaginary,
    }
}

fn plot_payload_metadata(
    payload: &MsPlotPayload,
    preset: MeasurementSetPlotPreset,
    requested_max_points: u64,
) -> PayloadMetadata {
    match payload {
        MsPlotPayload::Scatter(payload) => scatter_metadata(payload, requested_max_points),
        MsPlotPayload::ScatterGrid(payload) => scatter_grid_metadata(payload, requested_max_points),
        MsPlotPayload::ScatterPage(payload) => scatter_page_metadata(payload, requested_max_points),
        MsPlotPayload::ListObs(payload) => listobs_metadata(payload, preset, requested_max_points),
    }
}

fn plot_document_payload(
    payload: &MsPlotPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    match payload {
        MsPlotPayload::Scatter(payload) => scatter_plot_document(payload, metadata, preset),
        MsPlotPayload::ScatterGrid(payload) => {
            scatter_grid_plot_document(payload, metadata, preset)
        }
        MsPlotPayload::ScatterPage(payload) => {
            scatter_page_plot_document(payload, metadata, preset)
        }
        MsPlotPayload::ListObs(payload) => listobs_plot_document(payload, metadata, preset),
    }
}

fn listobs_plot_document(
    payload: &MeasurementSetPlotPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    match payload {
        MeasurementSetPlotPayload::UvCoverage(payload) => {
            let axis_extent = payload.axis_extent_lambda / 1_000.0;
            let axes = vec![
                document_axis(
                    "u",
                    "u",
                    "kλ",
                    -axis_extent,
                    axis_extent,
                    PlotAxisScale::Linear,
                ),
                document_axis(
                    "v",
                    "v",
                    "kλ",
                    -axis_extent,
                    axis_extent,
                    PlotAxisScale::Linear,
                ),
            ];
            let mut grouped_layers = BTreeMap::<String, (Vec<f64>, Vec<f64>, usize)>::new();
            for track in &payload.tracks {
                let group = uv_coverage_group_label(&track.label);
                let entry = grouped_layers.entry(group).or_default();
                entry.2 += track.points.len();
                for (u_lambda, v_lambda) in &track.points {
                    entry.0.push(*u_lambda / 1_000.0);
                    entry.1.push(*v_lambda / 1_000.0);
                    if payload.mirror {
                        entry.0.push(-*u_lambda / 1_000.0);
                        entry.1.push(-*v_lambda / 1_000.0);
                    }
                }
            }
            let layers = grouped_layers
                .into_iter()
                .enumerate()
                .map(|(index, (group, (x_values, y_values, source_count)))| {
                    point_layer(PointLayerSpec {
                        id: format!("uv-coverage-{index}"),
                        title: group.clone(),
                        x_axis_id: "u",
                        y_axis_id: "v",
                        x_values,
                        y_values,
                        point_labels: Vec::new(),
                        point_symbol_sizes: Vec::new(),
                        provenance: Vec::new(),
                        color_group: group,
                        symbol_size: 2.5,
                        line_width: 1.0,
                        provenance_summary: format!(
                            "UV coverage samples from Rust msexplore payload, coalesced from {source_count} samples"
                        ),
                    })
                })
                .collect();
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                layers,
                Vec::new(),
                Vec::new(),
            )
        }
        MeasurementSetPlotPayload::VisibilityScatter(payload) => {
            let axes = axes_for_ranges(
                "x",
                &payload.x_label,
                "y",
                &payload.y_label,
                None,
                payload.fixed_y_bounds,
                payload
                    .series
                    .iter()
                    .flat_map(|series| series.points.iter().copied()),
            );
            let layers = payload
                .series
                .iter()
                .enumerate()
                .map(|(index, series)| {
                    let (x_values, y_values): (Vec<_>, Vec<_>) =
                        series.points.iter().copied().unzip();
                    point_layer(PointLayerSpec {
                        id: format!("visibility-series-{index}"),
                        title: series.label.clone(),
                        x_axis_id: "x",
                        y_axis_id: "y",
                        x_values,
                        y_values,
                        point_labels: Vec::new(),
                        point_symbol_sizes: Vec::new(),
                        provenance: Vec::new(),
                        color_group: series.color_group.clone(),
                        symbol_size: 2.5,
                        line_width: 1.0,
                        provenance_summary: "Visibility scatter points from Rust msexplore payload"
                            .to_string(),
                    })
                })
                .collect();
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                layers,
                Vec::new(),
                Vec::new(),
            )
        }
        MeasurementSetPlotPayload::AntennaLayout(payload) => {
            let x_values = payload
                .antennas
                .iter()
                .map(|antenna| antenna.x)
                .collect::<Vec<_>>();
            let y_values = payload
                .antennas
                .iter()
                .map(|antenna| antenna.y)
                .collect::<Vec<_>>();
            let point_labels = if payload.labels_enabled {
                payload
                    .antennas
                    .iter()
                    .map(|antenna| antenna.label.clone())
                    .collect()
            } else {
                Vec::new()
            };
            let point_symbol_sizes = payload
                .antennas
                .iter()
                .map(|antenna| f64::from(antenna.marker_radius.max(1)) * 2.0)
                .collect::<Vec<_>>();
            let axes = axes_for_ranges(
                "x",
                &payload.x_label,
                "y",
                &payload.y_label,
                None,
                None,
                x_values.iter().copied().zip(y_values.iter().copied()),
            );
            let layer = point_layer(PointLayerSpec {
                id: "antennas".to_string(),
                title: "Antennas".to_string(),
                x_axis_id: "x",
                y_axis_id: "y",
                x_values,
                y_values,
                point_labels,
                point_symbol_sizes,
                provenance: Vec::new(),
                color_group: "antenna".to_string(),
                symbol_size: 4.0,
                line_width: 1.0,
                provenance_summary: "ANTENNA table positions from Rust msexplore payload"
                    .to_string(),
            });
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                vec![layer],
                Vec::new(),
                Vec::new(),
            )
        }
        MeasurementSetPlotPayload::ScanTimeline(payload) => {
            let axes = vec![
                document_axis(
                    "time",
                    "Time",
                    "MJD seconds",
                    payload.start_mjd_seconds,
                    payload.end_mjd_seconds,
                    PlotAxisScale::Linear,
                ),
                document_lane_axis("scan", "Scan", payload.lane_labels.clone()),
            ];
            let layers = payload
                .bars
                .iter()
                .enumerate()
                .map(|(index, bar)| {
                    interval_layer(IntervalLayerSpec {
                        id: format!("scan-bar-{index}"),
                        title: nonempty_or(&bar.label, "Scan").to_string(),
                        x_axis_id: "time",
                        y_axis_id: "scan",
                        interval_x_start: vec![bar.start_mjd_seconds],
                        interval_x_end: vec![bar.end_mjd_seconds],
                        interval_y: vec![bar.lane as f64],
                        interval_height: vec![0.72],
                        interval_labels: vec![bar.label.clone()],
                        color_group: bar.color_group.clone(),
                        provenance_summary: "MAIN-table scan interval from Rust msexplore payload"
                            .to_string(),
                    })
                })
                .collect();
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                layers,
                Vec::new(),
                Vec::new(),
            )
        }
        MeasurementSetPlotPayload::SpectralWindowCoverage(payload) => {
            let lane_labels = payload
                .bars
                .iter()
                .map(|bar| format!("spw {}", bar.spectral_window_id))
                .collect::<Vec<_>>();
            let x_bounds = payload
                .bars
                .iter()
                .flat_map(|bar| [bar.start, bar.end])
                .fold(None, bounds_accumulator);
            let (lower, upper) = expanded_bounds(x_bounds.unwrap_or((0.0, 1.0)));
            let axes = vec![
                document_axis(
                    "frequency",
                    &payload.x_label,
                    label_unit(&payload.x_label).as_str(),
                    lower,
                    upper,
                    PlotAxisScale::Linear,
                ),
                document_lane_axis("spw", "Spectral window", lane_labels),
            ];
            let layers = payload
                .bars
                .iter()
                .map(|bar| {
                    interval_layer(IntervalLayerSpec {
                        id: format!("spw-bar-{}", bar.spectral_window_id),
                        title: nonempty_or(&bar.label, "Spectral window").to_string(),
                        x_axis_id: "frequency",
                        y_axis_id: "spw",
                        interval_x_start: vec![bar.start],
                        interval_x_end: vec![bar.end],
                        interval_y: vec![bar.lane as f64],
                        interval_height: vec![0.72],
                        interval_labels: vec![bar.label.clone()],
                        color_group: bar.color_group.clone(),
                        provenance_summary:
                            "SPECTRAL_WINDOW coverage interval from Rust msexplore payload"
                                .to_string(),
                    })
                })
                .collect();
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                layers,
                Vec::new(),
                Vec::new(),
            )
        }
    }
}

fn scatter_plot_document(
    payload: &MsScatterPlotPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    if payload.secondary_y_axis.is_none() {
        return shared_scatter_plot_document(
            &MsPlotPayload::Scatter(payload.clone()),
            metadata,
            preset,
        );
    }
    let axes = scatter_axes(
        "x",
        &payload.x_label,
        "y",
        &payload.y_label,
        payload.fixed_x_bounds,
        payload.fixed_y_bounds,
        payload
            .series
            .iter()
            .flat_map(|series| series.points.iter().copied()),
    );
    let mut layers = scatter_layers(&payload.series, "x", "y", payload.symbol_size_px);
    if let Some(secondary_label) = &payload.secondary_y_label {
        let secondary_axis = document_axis(
            "y-secondary",
            secondary_label,
            &label_unit(secondary_label),
            payload
                .secondary_fixed_y_bounds
                .unwrap_or_else(|| {
                    series_y_bounds(
                        payload
                            .series
                            .iter()
                            .filter(|series| Some(series.y_axis) == payload.secondary_y_axis)
                            .flat_map(|series| series.points.iter().map(|(_, y)| *y)),
                    )
                })
                .0,
            payload
                .secondary_fixed_y_bounds
                .unwrap_or_else(|| {
                    series_y_bounds(
                        payload
                            .series
                            .iter()
                            .filter(|series| Some(series.y_axis) == payload.secondary_y_axis)
                            .flat_map(|series| series.points.iter().map(|(_, y)| *y)),
                    )
                })
                .1,
            PlotAxisScale::Linear,
        );
        let mut axes = axes;
        axes.push(PlotDocumentAxis {
            draws_on_trailing_edge: true,
            ..secondary_axis
        });
        for layer in &mut layers {
            if payload
                .series
                .iter()
                .find(|series| series.label == layer.title)
                .is_some_and(|series| Some(series.y_axis) == payload.secondary_y_axis)
            {
                layer.y_axis_id = "y-secondary".to_string();
            }
        }
        return base_document(
            preset,
            metadata,
            payload.header_lines.clone(),
            axes,
            layers,
            Vec::new(),
            Vec::new(),
        );
    }
    base_document(
        preset,
        metadata,
        payload.header_lines.clone(),
        axes,
        layers,
        Vec::new(),
        Vec::new(),
    )
}

fn scatter_grid_plot_document(
    payload: &MsScatterGridPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    shared_scatter_plot_document(
        &MsPlotPayload::ScatterGrid(payload.clone()),
        metadata,
        preset,
    )
}

fn scatter_page_plot_document(
    payload: &MsScatterPagePayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    shared_scatter_plot_document(
        &MsPlotPayload::ScatterPage(payload.clone()),
        metadata,
        preset,
    )
}

fn shared_scatter_plot_document(
    payload: &MsPlotPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    let shared = MsPlotData::from_payload(payload)
        .expect("scatter payloads always project into the shared plot-data contract");
    let mut panels = shared
        .panels
        .iter()
        .map(shared_plot_panel)
        .collect::<Vec<_>>();
    if matches!(payload, MsPlotPayload::Scatter(_)) {
        let panel = panels.remove(0);
        return base_document(
            preset,
            metadata,
            shared.header_lines,
            panel.axes,
            panel.layers,
            panel.annotations,
            Vec::new(),
        );
    }
    base_document(
        preset,
        metadata,
        shared.header_lines,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        panels,
    )
}

fn shared_plot_panel(panel: &MsPlotDataPanel) -> PlotDocumentPanel {
    PlotDocumentPanel {
        id: panel.id.clone(),
        title: panel.title.clone(),
        axes: panel
            .axes
            .iter()
            .map(|axis| {
                document_axis(
                    &axis.id,
                    &axis.label,
                    &axis.unit,
                    axis.lower,
                    axis.upper,
                    PlotAxisScale::Linear,
                )
            })
            .collect(),
        layers: panel
            .series
            .iter()
            .enumerate()
            .map(|(index, series)| {
                point_layer(PointLayerSpec {
                    id: format!("series-{index}"),
                    title: series.label.clone(),
                    x_axis_id: "x",
                    y_axis_id: &series.y_axis_id,
                    x_values: series.x.clone(),
                    y_values: series.y.clone(),
                    point_labels: Vec::new(),
                    point_symbol_sizes: Vec::new(),
                    provenance: if series.x.len() <= FRONTEND_POINT_PROVENANCE_LIMIT {
                        series
                            .provenance
                            .iter()
                            .map(|point| PlotPointProvenance {
                                row: point.row as u64,
                                corr: point.corr as u64,
                                chan_start: point.chan_start as u64,
                                chan_end: point.chan_end as u64,
                            })
                            .collect()
                    } else {
                        Vec::new()
                    },
                    color_group: series.color_group.clone(),
                    symbol_size: panel.symbol_size,
                    line_width: 1.0,
                    provenance_summary: "Visibility samples from shared casa-ms plot data"
                        .to_owned(),
                })
            })
            .collect(),
        annotations: Vec::new(),
    }
}

fn base_document(
    preset: MeasurementSetPlotPreset,
    metadata: &PayloadMetadata,
    header_lines: Vec<String>,
    axes: Vec<PlotDocumentAxis>,
    layers: Vec<PlotDocumentLayer>,
    annotations: Vec<PlotDocumentAnnotation>,
    panels: Vec<PlotDocumentPanel>,
) -> PlotDocumentPayload {
    PlotDocumentPayload {
        id: format!("msexplore-{}", ms_plot_preset(preset).as_str()),
        title: metadata.title.clone(),
        subtitle: metadata.summary.clone(),
        header_lines,
        axes,
        layers,
        annotations,
        panels,
        show_legend: true,
    }
}

fn scatter_axes(
    x_id: &str,
    x_label: &str,
    y_id: &str,
    y_label: &str,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    points: impl Iterator<Item = (f64, f64)>,
) -> Vec<PlotDocumentAxis> {
    axes_for_ranges(
        x_id,
        x_label,
        y_id,
        y_label,
        fixed_x_bounds,
        fixed_y_bounds,
        points,
    )
}

fn axes_for_ranges(
    x_id: &str,
    x_label: &str,
    y_id: &str,
    y_label: &str,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    points: impl Iterator<Item = (f64, f64)>,
) -> Vec<PlotDocumentAxis> {
    let (x_bounds, y_bounds) = points.fold((None, None), |(x_bounds, y_bounds), (x, y)| {
        (
            bounds_accumulator(x_bounds, x),
            bounds_accumulator(y_bounds, y),
        )
    });
    let x_bounds = expanded_bounds(fixed_x_bounds.or(x_bounds).unwrap_or((0.0, 1.0)));
    let y_bounds = expanded_bounds(fixed_y_bounds.or(y_bounds).unwrap_or((0.0, 1.0)));
    vec![
        document_axis(
            x_id,
            x_label,
            &label_unit(x_label),
            x_bounds.0,
            x_bounds.1,
            PlotAxisScale::Linear,
        ),
        document_axis(
            y_id,
            y_label,
            &label_unit(y_label),
            y_bounds.0,
            y_bounds.1,
            PlotAxisScale::Linear,
        ),
    ]
}

fn scatter_layers(
    series: &[MsScatterSeries],
    x_axis_id: &str,
    y_axis_id: &str,
    symbol_size_px: Option<u32>,
) -> Vec<PlotDocumentLayer> {
    series
        .iter()
        .enumerate()
        .map(|(index, series)| {
            let (x_values, y_values): (Vec<_>, Vec<_>) = series.points.iter().copied().unzip();
            let provenance = if series.points.len() <= FRONTEND_POINT_PROVENANCE_LIMIT {
                series
                    .provenance
                    .iter()
                    .map(|point| PlotPointProvenance {
                        row: point.row as u64,
                        corr: point.corr as u64,
                        chan_start: point.chan_start as u64,
                        chan_end: point.chan_end as u64,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            point_layer(PointLayerSpec {
                id: format!("series-{index}"),
                title: series.label.clone(),
                x_axis_id,
                y_axis_id,
                x_values,
                y_values,
                point_labels: Vec::new(),
                point_symbol_sizes: Vec::new(),
                provenance,
                color_group: series.color_group.clone(),
                symbol_size: symbol_size_px.unwrap_or(3) as f64,
                line_width: 1.0,
                provenance_summary: "Visibility samples from Rust msexplore payload".to_string(),
            })
        })
        .collect()
}

struct PointLayerSpec<'a> {
    id: String,
    title: String,
    x_axis_id: &'a str,
    y_axis_id: &'a str,
    x_values: Vec<f64>,
    y_values: Vec<f64>,
    point_labels: Vec<String>,
    point_symbol_sizes: Vec<f64>,
    provenance: Vec<PlotPointProvenance>,
    color_group: String,
    symbol_size: f64,
    line_width: f64,
    provenance_summary: String,
}

fn point_layer(spec: PointLayerSpec<'_>) -> PlotDocumentLayer {
    let source_sample_count = spec.x_values.len() as u64;
    PlotDocumentLayer {
        id: spec.id,
        title: spec.title,
        kind: PlotLayerKind::Scatter,
        x_axis_id: spec.x_axis_id.to_string(),
        y_axis_id: spec.y_axis_id.to_string(),
        x_values: spec.x_values,
        y_values: spec.y_values,
        point_labels: spec.point_labels,
        point_symbol_sizes: spec.point_symbol_sizes,
        interval_x_start: Vec::new(),
        interval_x_end: Vec::new(),
        interval_y: Vec::new(),
        interval_height: Vec::new(),
        interval_labels: Vec::new(),
        provenance: spec.provenance,
        color_group: spec.color_group,
        symbol_size: spec.symbol_size,
        line_width: spec.line_width,
        opacity: 0.82,
        source_sample_count,
        payload_strategy: PlotPayloadStrategy::PointCloud,
        provenance_summary: spec.provenance_summary,
    }
}

struct IntervalLayerSpec<'a> {
    id: String,
    title: String,
    x_axis_id: &'a str,
    y_axis_id: &'a str,
    interval_x_start: Vec<f64>,
    interval_x_end: Vec<f64>,
    interval_y: Vec<f64>,
    interval_height: Vec<f64>,
    interval_labels: Vec<String>,
    color_group: String,
    provenance_summary: String,
}

fn interval_layer(spec: IntervalLayerSpec<'_>) -> PlotDocumentLayer {
    let source_sample_count = spec.interval_x_start.len() as u64;
    PlotDocumentLayer {
        id: spec.id,
        title: spec.title,
        kind: PlotLayerKind::Interval,
        x_axis_id: spec.x_axis_id.to_string(),
        y_axis_id: spec.y_axis_id.to_string(),
        x_values: Vec::new(),
        y_values: Vec::new(),
        point_labels: Vec::new(),
        point_symbol_sizes: Vec::new(),
        interval_x_start: spec.interval_x_start,
        interval_x_end: spec.interval_x_end,
        interval_y: spec.interval_y,
        interval_height: spec.interval_height,
        interval_labels: spec.interval_labels,
        provenance: Vec::new(),
        color_group: spec.color_group,
        symbol_size: 1.0,
        line_width: 1.0,
        opacity: 0.78,
        source_sample_count,
        payload_strategy: PlotPayloadStrategy::Intervals,
        provenance_summary: spec.provenance_summary,
    }
}

fn document_axis(
    id: &str,
    label: &str,
    unit: &str,
    lower: f64,
    upper: f64,
    scale: PlotAxisScale,
) -> PlotDocumentAxis {
    PlotDocumentAxis {
        id: id.to_string(),
        label: label.to_string(),
        unit: unit.to_string(),
        lower,
        upper,
        scale,
        lane_labels: Vec::new(),
        draws_on_trailing_edge: false,
    }
}

fn document_lane_axis(id: &str, label: &str, lane_labels: Vec<String>) -> PlotDocumentAxis {
    PlotDocumentAxis {
        id: id.to_string(),
        label: label.to_string(),
        unit: String::new(),
        lower: -0.5,
        upper: lane_labels.len().saturating_sub(1) as f64 + 0.5,
        scale: PlotAxisScale::Linear,
        lane_labels,
        draws_on_trailing_edge: false,
    }
}

fn series_y_bounds(values: impl Iterator<Item = f64>) -> (f64, f64) {
    expanded_bounds(values.fold(None, bounds_accumulator).unwrap_or((0.0, 1.0)))
}

fn bounds_accumulator(bounds: Option<(f64, f64)>, value: f64) -> Option<(f64, f64)> {
    if !value.is_finite() {
        return bounds;
    }
    Some(match bounds {
        Some((lower, upper)) => (lower.min(value), upper.max(value)),
        None => (value, value),
    })
}

fn expanded_bounds((lower, upper): (f64, f64)) -> (f64, f64) {
    if !lower.is_finite() || !upper.is_finite() {
        return (0.0, 1.0);
    }
    if (upper - lower).abs() < f64::EPSILON {
        let pad = lower.abs().max(1.0) * 0.05;
        return (lower - pad, upper + pad);
    }
    let pad = (upper - lower).abs() * 0.04;
    (lower - pad, upper + pad)
}

fn listobs_metadata(
    payload: &MeasurementSetPlotPayload,
    preset: MeasurementSetPlotPreset,
    requested_max_points: u64,
) -> PayloadMetadata {
    match payload {
        MeasurementSetPlotPayload::UvCoverage(payload) => {
            let rendered_point_count = payload
                .tracks
                .iter()
                .map(|track| track.points.len() as u64)
                .sum();
            let series = uv_coverage_series_metadata(&payload.tracks);
            let series_count = series.len() as u64;
            PayloadMetadata {
                title: "UV Coverage".to_string(),
                summary: payload.summary.clone(),
                x_axis: axis_metadata("u", "u (kλ)"),
                y_axis: axis_metadata("v", "v (kλ)"),
                series,
                sampling: PlotSamplingDiagnostics {
                    requested_max_points,
                    rendered_point_count,
                    series_count,
                    diagnostics: sampling_diagnostics(&payload.summary, rendered_point_count),
                },
            }
        }
        _ => PayloadMetadata {
            title: ms_plot_preset(preset).display_name().to_string(),
            summary: "Metadata-oriented MeasurementSet plot.".to_string(),
            x_axis: axis_metadata(
                ms_plot_preset(preset).as_str(),
                ms_plot_preset(preset).display_name(),
            ),
            y_axis: axis_metadata("metadata", "Metadata"),
            series: vec![],
            sampling: PlotSamplingDiagnostics {
                requested_max_points,
                rendered_point_count: 0,
                series_count: 0,
                diagnostics: vec!["metadata-oriented plot has no visibility series".to_string()],
            },
        },
    }
}

fn scatter_grid_metadata(
    payload: &MsScatterGridPayload,
    requested_max_points: u64,
) -> PayloadMetadata {
    let series = payload
        .panels
        .iter()
        .flat_map(|panel| panel.series.iter())
        .map(series_metadata)
        .collect::<Vec<_>>();
    let rendered_point_count = series.iter().map(|series| series.point_count).sum();
    PayloadMetadata {
        title: payload.title.clone(),
        summary: payload.summary.clone(),
        x_axis: axis_metadata(payload.x_axis.as_str(), &payload.x_label),
        y_axis: axis_metadata(payload.y_axis.as_str(), &payload.y_label),
        series,
        sampling: PlotSamplingDiagnostics {
            requested_max_points,
            rendered_point_count,
            series_count: payload
                .panels
                .iter()
                .map(|panel| panel.series.len() as u64)
                .sum(),
            diagnostics: sampling_diagnostics(&payload.summary, rendered_point_count),
        },
    }
}

fn scatter_page_metadata(
    payload: &MsScatterPagePayload,
    requested_max_points: u64,
) -> PayloadMetadata {
    let first_plot = payload.items.first().map(|item| &item.plot);
    let series = payload
        .items
        .iter()
        .flat_map(|item| item.plot.series.iter())
        .map(series_metadata)
        .collect::<Vec<_>>();
    let rendered_point_count = series.iter().map(|series| series.point_count).sum();
    PayloadMetadata {
        title: payload.title.clone(),
        summary: payload.summary.clone(),
        x_axis: first_plot
            .map(|plot| axis_metadata(plot.x_axis.as_str(), &plot.x_label))
            .unwrap_or_else(|| axis_metadata("unknown", "Unknown")),
        y_axis: first_plot
            .map(|plot| axis_metadata(plot.y_axis.as_str(), &plot.y_label))
            .unwrap_or_else(|| axis_metadata("unknown", "Unknown")),
        series,
        sampling: PlotSamplingDiagnostics {
            requested_max_points,
            rendered_point_count,
            series_count: payload
                .items
                .iter()
                .map(|item| item.plot.series.len() as u64)
                .sum(),
            diagnostics: sampling_diagnostics(&payload.summary, rendered_point_count),
        },
    }
}

fn scatter_metadata(payload: &MsScatterPlotPayload, requested_max_points: u64) -> PayloadMetadata {
    let series = payload
        .series
        .iter()
        .map(series_metadata)
        .collect::<Vec<_>>();
    let series_count = series.len() as u64;
    let rendered_point_count = series.iter().map(|series| series.point_count).sum();
    PayloadMetadata {
        title: payload.title.clone(),
        summary: payload.summary.clone(),
        x_axis: axis_metadata(payload.x_axis.as_str(), &payload.x_label),
        y_axis: axis_metadata(payload.y_axis.as_str(), &payload.y_label),
        series,
        sampling: PlotSamplingDiagnostics {
            requested_max_points,
            rendered_point_count,
            series_count,
            diagnostics: sampling_diagnostics(&payload.summary, rendered_point_count),
        },
    }
}

fn uv_coverage_series_metadata(tracks: &[UvCoverageSeries]) -> Vec<PlotSeriesMetadata> {
    let mut grouped = BTreeMap::<String, u64>::new();
    for track in tracks {
        *grouped
            .entry(uv_coverage_group_label(&track.label))
            .or_default() += track.points.len() as u64;
    }
    grouped
        .into_iter()
        .map(|(group, point_count)| PlotSeriesMetadata {
            label: group.clone(),
            color_group: group,
            point_count,
            first_row: None,
            last_row: None,
        })
        .collect()
}

fn uv_coverage_group_label(track_label: &str) -> String {
    let parts = track_label.split_whitespace().collect::<Vec<_>>();
    let field = parts
        .windows(2)
        .find_map(|window| (window[0] == "field").then_some(window[1]));
    let spw = parts
        .windows(2)
        .find_map(|window| (window[0] == "spw").then_some(window[1]));
    match (field, spw) {
        (Some(field), Some(spw)) => format!("field {field} spw {spw}"),
        (Some(field), None) => format!("field {field}"),
        (None, Some(spw)) => format!("spw {spw}"),
        (None, None) => "UV samples".to_string(),
    }
}

fn series_metadata(series: &MsScatterSeries) -> PlotSeriesMetadata {
    let first_row = series.provenance.first().map(|point| point.row as u64);
    let last_row = series.provenance.last().map(|point| point.row as u64);
    PlotSeriesMetadata {
        label: series.label.clone(),
        color_group: series.color_group.clone(),
        point_count: series.points.len() as u64,
        first_row,
        last_row,
    }
}

fn axis_metadata(id: &str, label: &str) -> PlotAxisMetadata {
    PlotAxisMetadata {
        id: id.to_string(),
        label: label.to_string(),
        unit: label_unit(label),
    }
}

fn label_unit(label: &str) -> String {
    label
        .rsplit_once('(')
        .and_then(|(_, suffix)| suffix.strip_suffix(')'))
        .unwrap_or("")
        .trim()
        .to_string()
}

fn nonempty_or<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

fn sampling_diagnostics(summary: &str, rendered_point_count: u64) -> Vec<String> {
    let mut diagnostics = Vec::new();
    if rendered_point_count == 0 {
        diagnostics.push("plot produced no drawable visibility points".to_string());
    }
    if summary.contains("Decimated points") || summary.contains("Downsampled plot") {
        diagnostics.push("point budget decimation was applied".to_string());
    }
    diagnostics
}

fn normalize_data_column(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "data".to_string();
    }
    trimmed.to_ascii_lowercase()
}

fn normalized_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "all")
}

fn measurement_set_summary_selection_summary(request: &MeasurementSetSummaryRequest) -> String {
    let mut parts = Vec::new();
    if let Some(field) = normalized_optional(request.field.clone()) {
        parts.push(format!("field {field}"));
    }
    if let Some(spw) = normalized_optional(request.spectral_window.clone()) {
        parts.push(format!("spw {spw}"));
    }
    if let Some(correlation) = normalized_optional(request.correlation.clone()) {
        parts.push(format!("correlation {correlation}"));
    }
    if let Some(scan) = normalized_optional(request.scan.clone()) {
        parts.push(format!("scan {scan}"));
    }
    if let Some(timerange) = normalized_optional(request.timerange.clone()) {
        parts.push(format!("timerange {timerange}"));
    }
    if parts.is_empty() {
        "all rows".to_string()
    } else {
        parts.join(", ")
    }
}

fn selection_summary(request: &MeasurementSetPlotRequest) -> String {
    let mut parts = vec![format!(
        "data column {}",
        normalize_data_column(&request.data_column)
    )];
    if let Some(field) = normalized_optional(request.field.clone()) {
        parts.push(format!("field {field}"));
    }
    if let Some(spw) = normalized_optional(request.spectral_window.clone()) {
        parts.push(format!("spw {spw}"));
    }
    if let Some(correlation) = normalized_optional(request.correlation.clone()) {
        parts.push(format!("correlation {correlation}"));
    }
    parts.join(", ")
}

#[derive(Default)]
struct ProjectScan {
    datasets: Vec<DatasetProbe>,
    diagnostics: Vec<String>,
    scanned_entry_count: usize,
    truncated: bool,
}

fn scan_path(path: &Path, depth: usize, scan: &mut ProjectScan) {
    if scan.scanned_entry_count >= MAX_PROJECT_SCAN_ENTRIES {
        scan.truncated = true;
        return;
    }
    scan.scanned_entry_count += 1;

    match probe_dataset_path(path) {
        Ok(Some(dataset)) => {
            scan.datasets.push(dataset);
            return;
        }
        Ok(None) => {}
        Err(error) => scan
            .diagnostics
            .push(format!("{}: {error}", path.display())),
    }

    if depth >= MAX_PROJECT_SCAN_DEPTH {
        return;
    }
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if !metadata.is_dir() {
        return;
    }

    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) => {
            scan.diagnostics
                .push(format!("read {}: {error}", path.display()));
            return;
        }
    };
    for entry in entries {
        if scan.scanned_entry_count >= MAX_PROJECT_SCAN_ENTRIES {
            scan.truncated = true;
            break;
        }
        match entry {
            Ok(entry) => scan_path(&entry.path(), depth + 1, scan),
            Err(error) => scan.diagnostics.push(format!("directory entry: {error}")),
        }
    }
}

fn probe_dataset_path(path: &Path) -> Result<Option<DatasetProbe>, String> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => return Err(format!("metadata failed: {error}")),
    };
    if !(metadata.is_dir() || metadata.is_file()) {
        return Ok(None);
    }

    if metadata.is_dir() {
        if let Some(probe) = probe_measurement_set(path, &metadata)? {
            return Ok(Some(probe));
        }
        if let Some(probe) = probe_image(path, &metadata)? {
            return Ok(Some(probe));
        }
        if let Some(probe) = probe_table(path, &metadata)? {
            return Ok(Some(probe));
        }
    }

    Ok(None)
}

fn probe_measurement_set(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<Option<DatasetProbe>, String> {
    let (size_bytes, diagnostics) = dataset_size_bytes(path, metadata);
    let ms = match MeasurementSet::open(path) {
        Ok(ms) => ms,
        Err(_) => return Ok(None),
    };
    let fields = match ms_field_labels(&ms) {
        Ok(fields) => fields,
        Err(_) => return Ok(None),
    };
    let spectral_windows = match ms_spectral_window_labels(&ms) {
        Ok(spectral_windows) => spectral_windows,
        Err(_) => return Ok(None),
    };
    let antennas = match ms_antenna_labels(&ms) {
        Ok(antennas) => antennas,
        Err(_) => return Ok(None),
    };
    let scans = Vec::new();
    let arrays = Vec::new();
    let observations = ms_observation_labels(&ms).unwrap_or_default();
    let intents = ms_intent_labels(&ms).unwrap_or_default();
    let feeds = ms_feed_labels(&ms).unwrap_or_default();
    let correlations = match ms_correlation_labels(&ms) {
        Ok(correlations) => correlations,
        Err(_) => return Ok(None),
    };
    let columns = table_columns(ms.main_table());
    let data_columns = visibility_data_columns(&columns);
    let subtables = ms_subtables(&ms);
    let row_count = ms.row_count();

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::MeasurementSet,
        size_bytes,
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format!(
            "{} rows, {} fields, {} spw, {} antennas",
            row_count,
            fields.len(),
            spectral_windows.len(),
            antennas.len()
        ),
        units: "Jy, Hz, seconds".to_string(),
        fields,
        spectral_windows,
        scans,
        arrays,
        observations,
        antennas,
        intents,
        feeds,
        correlations,
        columns,
        data_columns,
        subtables,
        shape: vec![row_count as u64],
        notes: "Recognized by opening the path as a MeasurementSet and reading subtable/schema overview metadata; MAIN-derived scan and array labels are deferred to explicit plot/range probes."
            .to_string(),
        diagnostics,
    }))
}

fn ms_field_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let fields = ms.field().map_err(|error| error.to_string())?;
    (0..fields.row_count())
        .map(|row| {
            fields
                .name(row)
                .map(|name| format!("{row}: {name}"))
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn ms_spectral_window_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let spectral_windows = ms.spectral_window().map_err(|error| error.to_string())?;
    (0..spectral_windows.row_count())
        .map(|row| {
            let num_chan = spectral_windows
                .num_chan(row)
                .map_err(|error| error.to_string())?;
            let chan_freq = spectral_windows.chan_freq(row).unwrap_or_default();
            let center_hz = if chan_freq.is_empty() {
                spectral_windows
                    .ref_frequency(row)
                    .map_err(|error| error.to_string())?
            } else {
                mean_or_zero(&chan_freq)
            };
            Ok(format!(
                "spw {}: {} chan, {:.6} GHz center",
                row,
                num_chan,
                center_hz / 1.0e9
            ))
        })
        .collect()
}

fn ms_antenna_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let antennas = ms.antenna().map_err(|error| error.to_string())?;
    (0..antennas.row_count())
        .map(|row| antennas.name(row).map_err(|error| error.to_string()))
        .collect()
}

fn ms_intent_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let state = ms.state().map_err(|error| error.to_string())?;
    let mut intents = BTreeSet::new();
    for row in 0..state.row_count() {
        let intent = state
            .string(row, "OBS_MODE")
            .map_err(|error| error.to_string())?;
        for item in intent
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            intents.insert(item.to_string());
        }
    }
    Ok(intents.into_iter().collect())
}

fn ms_observation_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let observations = ms.observation().map_err(|error| error.to_string())?;
    let mut labels = Vec::new();
    for row in 0..observations.row_count() {
        let project = observations
            .string(row, "PROJECT")
            .unwrap_or_default()
            .trim()
            .to_string();
        let telescope = observations
            .string(row, "TELESCOPE_NAME")
            .unwrap_or_default()
            .trim()
            .to_string();
        let detail = [project, telescope]
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(", ");
        if detail.is_empty() {
            labels.push(format!("observation {row}"));
        } else {
            labels.push(format!("observation {row}: {detail}"));
        }
    }
    Ok(labels)
}

fn ms_feed_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let feed = ms.feed().map_err(|error| error.to_string())?;
    let mut ids = BTreeSet::new();
    for row in 0..feed.row_count() {
        let id = feed
            .i32(row, "FEED_ID")
            .map_err(|error| error.to_string())?;
        if id >= 0 {
            ids.insert(id);
        }
    }
    Ok(ids.into_iter().map(|id| format!("feed {id}")).collect())
}

#[uniffi::export]
pub fn probe_measurement_set_uv_range(
    dataset_path: String,
) -> FrontendResult<MeasurementSetUvRangeProbe> {
    probe_measurement_set_uv_range_inner(&dataset_path).map_err(|error| {
        FrontendServiceError::Probe {
            reason: format!("{dataset_path}: {error}"),
        }
    })
}

#[uniffi::export]
pub fn probe_measurement_set_time_range(
    dataset_path: String,
) -> FrontendResult<MeasurementSetTimeRangeProbe> {
    probe_measurement_set_time_range_inner(&dataset_path).map_err(|error| {
        FrontendServiceError::Probe {
            reason: format!("{dataset_path}: {error}"),
        }
    })
}

fn probe_measurement_set_uv_range_inner(
    dataset_path: &str,
) -> Result<MeasurementSetUvRangeProbe, String> {
    let ms = MeasurementSet::open(Path::new(dataset_path)).map_err(|error| error.to_string())?;
    let table = ms.main_table();
    if table.row_count() == 0 {
        return Err("MeasurementSet has no MAIN rows".to_string());
    }
    let ddid_to_spw = data_description_to_spw(&ms)?;
    let spw_center_frequency_hz = spw_center_frequency_lookup(&ms)?;
    let row_numbers = (0..table.row_count()).collect::<Vec<_>>();
    let mut min_meters = f64::INFINITY;
    let mut max_meters = f64::NEG_INFINITY;
    let mut min_kilolambda = f64::INFINITY;
    let mut max_kilolambda = f64::NEG_INFINITY;
    let mut seen_rows = 0u64;

    for row_chunk in row_numbers.chunks(MAIN_SCALAR_CHUNK_ROWS) {
        let uvw_values = selected_uvw_values(table, "UVW", row_chunk)?;
        let ddids = selected_i32_values(table, "DATA_DESC_ID", row_chunk)?;
        for (uvw, ddid) in uvw_values.into_iter().zip(ddids) {
            let uv_meters = uvw[0].hypot(uvw[1]);
            if !uv_meters.is_finite() {
                continue;
            }
            min_meters = min_meters.min(uv_meters);
            max_meters = max_meters.max(uv_meters);
            if let Some(frequency_hz) = ddid_to_spw
                .get(&ddid)
                .and_then(|spw| spw_center_frequency_hz.get(spw))
                .copied()
                .filter(|frequency| *frequency > 0.0)
            {
                let kilo_lambda = uv_meters * frequency_hz / SPEED_OF_LIGHT_M_S / 1_000.0;
                min_kilolambda = min_kilolambda.min(kilo_lambda);
                max_kilolambda = max_kilolambda.max(kilo_lambda);
            }
            seen_rows += 1;
        }
    }

    if seen_rows == 0 || !min_meters.is_finite() || !max_meters.is_finite() {
        return Err("MeasurementSet UVW column did not contain finite UV distances".to_string());
    }
    if !min_kilolambda.is_finite() || !max_kilolambda.is_finite() {
        min_kilolambda = 0.0;
        max_kilolambda = 0.0;
    }

    Ok(MeasurementSetUvRangeProbe {
        min_meters,
        max_meters,
        min_kilolambda,
        max_kilolambda,
        row_count: seen_rows,
    })
}

fn probe_measurement_set_time_range_inner(
    dataset_path: &str,
) -> Result<MeasurementSetTimeRangeProbe, String> {
    let ms = MeasurementSet::open(Path::new(dataset_path)).map_err(|error| error.to_string())?;
    let table = ms.main_table();
    if table.row_count() == 0 {
        return Err("MeasurementSet has no MAIN rows".to_string());
    }
    let row_numbers = (0..table.row_count()).collect::<Vec<_>>();
    let mut min_seconds = f64::INFINITY;
    let mut max_seconds = f64::NEG_INFINITY;
    let mut seen_rows = 0u64;
    for row_chunk in row_numbers.chunks(MAIN_SCALAR_CHUNK_ROWS) {
        for seconds in selected_f64_values(table, "TIME", row_chunk)? {
            if !seconds.is_finite() {
                continue;
            }
            min_seconds = min_seconds.min(seconds);
            max_seconds = max_seconds.max(seconds);
            seen_rows += 1;
        }
    }
    if seen_rows == 0 || !min_seconds.is_finite() || !max_seconds.is_finite() {
        return Err("MeasurementSet TIME column did not contain finite seconds".to_string());
    }
    Ok(MeasurementSetTimeRangeProbe {
        min_seconds,
        max_seconds,
        row_count: seen_rows,
    })
}

fn spw_center_frequency_lookup(ms: &MeasurementSet) -> Result<BTreeMap<i32, f64>, String> {
    let spectral_windows = ms.spectral_window().map_err(|error| error.to_string())?;
    let mut lookup = BTreeMap::new();
    for row in 0..spectral_windows.row_count() {
        let chan_freq = spectral_windows.chan_freq(row).unwrap_or_default();
        let center_hz = if chan_freq.is_empty() {
            spectral_windows
                .ref_frequency(row)
                .map_err(|error| error.to_string())?
        } else {
            mean_or_zero(&chan_freq)
        };
        lookup.insert(row as i32, center_hz);
    }
    Ok(lookup)
}

fn selected_uvw_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<[f64; 3]>, String> {
    table
        .column_accessor(column)
        .map_err(|error| error.to_string())?
        .array_cells_owned(rows)
        .map_err(|error| error.to_string())?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| {
            let value = value
                .ok_or_else(|| format!("plot service requires {column} data for row {row}"))?;
            match value {
                casa_types::ArrayValue::Float64(values) => {
                    let slice = values.as_slice().ok_or_else(|| {
                        format!("plot service requires contiguous {column} f64[3] cells")
                    })?;
                    if slice.len() != 3 {
                        return Err(format!(
                            "plot service requires {column} cells with shape [3], found [{}]",
                            slice.len()
                        ));
                    }
                    Ok([slice[0], slice[1], slice[2]])
                }
                other => Err(format!(
                    "plot service requires DOUBLE array {column} cells, found {:?}",
                    other.primitive_type()
                )),
            }
        })
        .collect()
}

fn ms_correlation_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let mut labels = Vec::new();
    for row in 0..polarization.row_count() {
        labels.extend(
            polarization
                .corr_type(row)
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(stokes_name)
                .map(str::to_string),
        );
    }
    Ok(unique_sorted(labels))
}

fn stokes_name(code: i32) -> &'static str {
    match code {
        1 => "I",
        2 => "Q",
        3 => "U",
        4 => "V",
        5 => "RR",
        6 => "RL",
        7 => "LR",
        8 => "LL",
        9 => "XX",
        10 => "XY",
        11 => "YX",
        12 => "YY",
        _ => "??",
    }
}

fn mean_or_zero(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn probe_image(path: &Path, metadata: &fs::Metadata) -> Result<Option<DatasetProbe>, String> {
    let (size_bytes, mut diagnostics) = dataset_size_bytes(path, metadata);
    let image = match AnyPagedImage::open(path) {
        Ok(image) => image,
        Err(_) => return Ok(None),
    };
    let pixel_type = match image.pixel_type() {
        ImagePixelType::Float32 => "float32",
        ImagePixelType::Float64 => "float64",
        ImagePixelType::Complex32 => "complex64",
        ImagePixelType::Complex64 => "complex128",
    };
    let shape: Vec<u64> = image.shape().iter().map(|value| *value as u64).collect();
    let mask_names = image.mask_names();
    let region_names = image.region_names();
    let brightness_unit = image_units(&image).to_string();
    diagnostics.push(format!("Pixel type: {pixel_type}"));
    match image_info(&image) {
        Ok(info) => {
            if !info.object_name.is_empty() {
                diagnostics.push(format!("Object: {}", info.object_name));
            }
            diagnostics.push(format!("Image type: {}", info.image_type));
        }
        Err(error) => diagnostics.push(format!("Image info unavailable: {error}")),
    }
    diagnostics.extend(image_coordinate_diagnostics(&image));
    if !mask_names.is_empty() {
        diagnostics.push(format!("Masks: {}", mask_names.join(", ")));
    }
    if !region_names.is_empty() {
        diagnostics.push(format!("Regions: {}", region_names.join(", ")));
    }
    if let Some(default_mask) = image.default_mask_name() {
        diagnostics.push(format!("Default mask: {default_mask}"));
    }

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::Image,
        size_bytes,
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format_shape(&shape),
        units: brightness_unit,
        fields: vec![],
        spectral_windows: vec![],
        scans: vec![],
        arrays: vec![],
        observations: vec![],
        antennas: vec![],
        intents: vec![],
        feeds: vec![],
        correlations: vec![],
        columns: vec!["map".to_string()],
        data_columns: vec![],
        subtables: vec![],
        shape,
        notes: format!(
            "Recognized by opening the path as a casa-rs image; {} masks, {} regions.",
            mask_names.len(),
            region_names.len()
        ),
        diagnostics,
    }))
}

fn image_units(image: &AnyPagedImage) -> &str {
    match image {
        AnyPagedImage::Float32(image) => image.units(),
        AnyPagedImage::Float64(image) => image.units(),
        AnyPagedImage::Complex32(image) => image.units(),
        AnyPagedImage::Complex64(image) => image.units(),
    }
}

fn image_coordinates(image: &AnyPagedImage) -> &CoordinateSystem {
    match image {
        AnyPagedImage::Float32(image) => image.coordinates(),
        AnyPagedImage::Float64(image) => image.coordinates(),
        AnyPagedImage::Complex32(image) => image.coordinates(),
        AnyPagedImage::Complex64(image) => image.coordinates(),
    }
}

fn image_info(image: &AnyPagedImage) -> Result<ImageInfo, String> {
    match image {
        AnyPagedImage::Float32(image) => image.image_info(),
        AnyPagedImage::Float64(image) => image.image_info(),
        AnyPagedImage::Complex32(image) => image.image_info(),
        AnyPagedImage::Complex64(image) => image.image_info(),
    }
    .map_err(|error| error.to_string())
}

fn image_coordinate_diagnostics(image: &AnyPagedImage) -> Vec<String> {
    let coords = image_coordinates(image);
    let shape = image.shape();
    let mut diagnostics = Vec::new();
    diagnostics.extend(image_direction_diagnostics(coords, shape));
    diagnostics.extend(image_spectral_diagnostics(coords, shape));
    match image_info(image) {
        Ok(info) => diagnostics.extend(image_beam_diagnostics(&info)),
        Err(error) => diagnostics.push(format!("Image info unavailable: {error}")),
    }
    diagnostics
}

fn image_direction_diagnostics(coords: &CoordinateSystem, shape: &[usize]) -> Vec<String> {
    let mut diagnostics = Vec::new();
    let mut pixel_offset = 0usize;
    for index in 0..coords.n_coordinates() {
        let coord = coords.coordinate(index);
        let n_pixel_axes = coord.n_pixel_axes();
        if coord.coordinate_type() == CoordinateType::Direction && n_pixel_axes >= 2 {
            let increments = coord.increment();
            if increments.len() >= 2 {
                let ra_cell = angular_increment_arcseconds(increments[0]).value().abs();
                let dec_cell = declination_increment_arcseconds(increments[1])
                    .value()
                    .abs();
                diagnostics.push(format!(
                    "Cell size: {} x {} arcsec",
                    format_compact_float(ra_cell),
                    format_compact_float(dec_cell)
                ));
            }

            let Some(x_len) = shape.get(pixel_offset).copied() else {
                break;
            };
            let Some(y_len) = shape.get(pixel_offset + 1).copied() else {
                break;
            };
            let center_pixel = [
                center_pixel_coordinate(x_len),
                center_pixel_coordinate(y_len),
            ];
            if let Ok(world) = coord.to_world(&center_pixel) {
                if world.len() >= 2 {
                    diagnostics.push(format!(
                        "Center: RA {} Dec {}",
                        format_right_ascension_labeled(world[0], 3),
                        format_declination_labeled(world[1], 2)
                    ));
                }
            }
            break;
        }
        pixel_offset += n_pixel_axes;
    }
    diagnostics
}

fn image_spectral_diagnostics(coords: &CoordinateSystem, shape: &[usize]) -> Vec<String> {
    let mut diagnostics = Vec::new();
    let mut pixel_offset = 0usize;
    for index in 0..coords.n_coordinates() {
        let coord = coords.coordinate(index);
        let n_pixel_axes = coord.n_pixel_axes();
        if coord.coordinate_type() == CoordinateType::Spectral && n_pixel_axes >= 1 {
            let Some(channel_count) = shape.get(pixel_offset).copied() else {
                break;
            };
            if channel_count == 0 {
                break;
            }
            let center_pixel = [center_pixel_coordinate(channel_count)];
            let unit = coord
                .axis_units()
                .first()
                .cloned()
                .unwrap_or_else(|| "Hz".to_string());
            if let Ok(world) = coord.to_world(&center_pixel) {
                if let Some(center) = world.first().copied() {
                    diagnostics.push(format!(
                        "Cube center frequency: {}",
                        format_frequency_like_value(center, &unit)
                    ));
                }
            }
            let channel_separation = coord
                .increment()
                .first()
                .copied()
                .map(f64::abs)
                .unwrap_or_default();
            if channel_separation.is_finite() && channel_separation > 0.0 {
                diagnostics.push(format!(
                    "Total bandwidth: {}",
                    format_frequency_like_value(channel_separation * channel_count as f64, &unit)
                ));
                diagnostics.push(format!(
                    "Channel separation: {}",
                    format_frequency_like_value(channel_separation, &unit)
                ));
            }
            break;
        }
        pixel_offset += n_pixel_axes;
    }
    diagnostics
}

fn image_beam_diagnostics(info: &ImageInfo) -> Vec<String> {
    let beam_set = &info.beam_set;
    if let Some(beam) = beam_set.single_beam() {
        if beam.is_null() {
            return Vec::new();
        }
        return match (
            beam.major_in("arcsec"),
            beam.minor_in("arcsec"),
            beam.position_angle_in("deg"),
        ) {
            (Ok(major), Ok(minor), Ok(pa)) => vec![format!(
                "Beam: {} x {} arcsec, PA {} deg",
                format_compact_float(major),
                format_compact_float(minor),
                format_compact_float(pa)
            )],
            _ => Vec::new(),
        };
    }
    if beam_set.is_empty() {
        return Vec::new();
    }
    let (channels, stokes) = beam_set.shape();
    let mut diagnostics = vec![format!(
        "Beam: {} per-plane beams ({} chan x {} stokes)",
        beam_set.size(),
        channels,
        stokes
    )];
    if let Some(beam) = beam_set.median_area_beam() {
        if !beam.is_null() {
            if let (Ok(major), Ok(minor), Ok(pa)) = (
                beam.major_in("arcsec"),
                beam.minor_in("arcsec"),
                beam.position_angle_in("deg"),
            ) {
                diagnostics.push(format!(
                    "Median beam: {} x {} arcsec, PA {} deg",
                    format_compact_float(major),
                    format_compact_float(minor),
                    format_compact_float(pa)
                ));
            }
        }
    }
    diagnostics
}

fn center_pixel_coordinate(axis_len: usize) -> f64 {
    axis_len.saturating_sub(1) as f64 / 2.0
}

fn format_frequency_like_value(value: f64, unit: &str) -> String {
    if unit.eq_ignore_ascii_case("Hz") {
        let abs_value = value.abs();
        if abs_value >= 1.0e9 {
            format!("{} GHz", format_compact_float(value / 1.0e9))
        } else if abs_value >= 1.0e6 {
            format!("{} MHz", format_compact_float(value / 1.0e6))
        } else if abs_value >= 1.0e3 {
            format!("{} kHz", format_compact_float(value / 1.0e3))
        } else {
            format!("{} Hz", format_compact_float(value))
        }
    } else if unit.is_empty() {
        format_compact_float(value)
    } else {
        format!("{} {unit}", format_compact_float(value))
    }
}

fn format_compact_float(value: f64) -> String {
    let mut text = format!("{value:.6}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" { "0".to_string() } else { text }
}

fn probe_table(path: &Path, metadata: &fs::Metadata) -> Result<Option<DatasetProbe>, String> {
    let (size_bytes, diagnostics) = dataset_size_bytes(path, metadata);
    let table = match Table::open(TableOptions::new(path)) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let columns = table_columns(&table);
    if columns.is_empty() && table.row_count() == 0 && table.info().table_type.is_empty() {
        return Ok(None);
    }
    let table_type = if table.info().table_type.is_empty() {
        "casacore table".to_string()
    } else {
        table.info().table_type.clone()
    };

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::Table,
        size_bytes,
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format!("{} rows, {} columns", table.row_count(), columns.len()),
        units: table_type.clone(),
        fields: vec![],
        spectral_windows: vec![],
        scans: vec![],
        arrays: vec![],
        observations: vec![],
        antennas: vec![],
        intents: vec![],
        feeds: vec![],
        correlations: vec![],
        columns,
        data_columns: vec![],
        subtables: vec![],
        shape: vec![table.row_count() as u64],
        notes: format!("Recognized by opening the path as a {table_type}."),
        diagnostics,
    }))
}

fn table_columns(table: &Table) -> Vec<String> {
    table
        .schema()
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn visibility_data_columns(columns: &[String]) -> Vec<String> {
    VisibilityDataColumn::ALL
        .iter()
        .map(|column| column.name())
        .filter(|name| columns.iter().any(|column| column == *name))
        .map(str::to_string)
        .collect()
}

fn ms_subtables(ms: &MeasurementSet) -> Vec<String> {
    let mut ids = ms.subtable_ids();
    ids.sort();
    ids.into_iter()
        .map(|id| {
            let role = if id.is_required() {
                "required"
            } else {
                "optional"
            };
            format!("{} ({role})", id.name())
        })
        .collect()
}

fn dataset_size_bytes(path: &Path, metadata: &fs::Metadata) -> (u64, Vec<String>) {
    if metadata.is_file() {
        return (metadata.len(), vec![]);
    }
    if !metadata.is_dir() {
        return (metadata.len(), vec![]);
    }

    let mut diagnostics = Vec::new();
    match directory_size_bytes(path, 2_048) {
        Ok((size, truncated)) => {
            if truncated {
                diagnostics
                    .push("size estimate truncated after 2048 filesystem entries".to_string());
            }
            (size, diagnostics)
        }
        Err(error) => {
            diagnostics.push(format!("size estimate failed: {error}"));
            (metadata.len(), diagnostics)
        }
    }
}

fn directory_size_bytes(path: &Path, max_entries: usize) -> Result<(u64, bool), String> {
    let mut stack = vec![path.to_path_buf()];
    let mut seen_entries = 0usize;
    let mut total = 0u64;
    let mut truncated = false;

    while let Some(current) = stack.pop() {
        if seen_entries >= max_entries {
            truncated = true;
            break;
        }
        seen_entries += 1;

        let metadata = fs::metadata(&current)
            .map_err(|error| format!("metadata {}: {error}", current.display()))?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        } else if metadata.is_dir() {
            let entries = fs::read_dir(&current)
                .map_err(|error| format!("read {}: {error}", current.display()))?;
            for entry in entries {
                let entry = entry
                    .map_err(|error| format!("directory entry {}: {error}", current.display()))?;
                stack.push(entry.path());
            }
        }
    }

    Ok((total, truncated))
}

fn stable_id(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn path_name(path: &Path) -> String {
    if let Some(name) = path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
    {
        return name.to_string();
    }
    path.to_string_lossy().into_owned()
}

fn modified_unix_seconds(metadata: &fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn unique_sorted(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn format_shape(shape: &[u64]) -> String {
    if shape.is_empty() {
        return "scalar".to_string();
    }
    shape
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(" x ")
}

uniffi::setup_scaffolding!();

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    use casa_coordinates::{
        CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    };
    use casa_images::beam::{GaussianBeam, ImageBeamSet};
    use casa_images::{ImageInfo, ImageType, PagedImage};
    use casa_tables::{ColumnSchema, TableInfo, TableSchema};
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;
    use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use flate2::read::GzDecoder;
    use ndarray::ArrayD;
    use tempfile::TempDir;

    fn unpack_small_ms() -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let fixture = File::open("../casa-ms/tests/fixtures/mssel_test_small.ms.tgz")
            .expect("small MS fixture");
        let decoder = GzDecoder::new(fixture);
        let mut archive = tar::Archive::new(decoder);
        archive.unpack(dir.path()).expect("unpack MS fixture");
        let ms_path = dir.path().join("mssel_test_small.ms");
        (dir, ms_path)
    }

    fn make_table(path: &Path) {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        table.set_info(TableInfo {
            table_type: "Calibration Table".to_string(),
            sub_type: "G Jones".to_string(),
            readme: Vec::new(),
        });
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String("gain".to_string())),
                ),
            ]))
            .expect("row");
        table.save(TableOptions::new(path)).expect("save table");
    }

    fn image_explorer_request(path: &Path, selected_view: &str) -> ImageExplorerSnapshotRequest {
        ImageExplorerSnapshotRequest {
            dataset_path: path.display().to_string(),
            selected_view: selected_view.to_string(),
            focus: "content".to_string(),
            plane_content_mode: "raster".to_string(),
            parameters: ImageExplorerParameters {
                blc: String::new(),
                trc: String::new(),
                inc: String::new(),
                stretch: "percentile99".to_string(),
                autoscale: "per_plane".to_string(),
                clip_low: String::new(),
                clip_high: String::new(),
            },
            cursor_x: None,
            cursor_y: None,
            selected_profile_axis: None,
            non_display_indices: Vec::new(),
            commands: Vec::new(),
            transient_commands: Vec::new(),
            include_profile: true,
        }
    }

    fn image_explorer_command(command: &str) -> ImageExplorerCommand {
        ImageExplorerCommand {
            command: command.to_string(),
            x: None,
            y: None,
            name: None,
            new_name: None,
            set_default: None,
            path: None,
            region: None,
        }
    }

    #[test]
    fn probe_measurement_set_reads_real_metadata() {
        let (_dir, ms_path) = unpack_small_ms();

        let probe = probe_path(ms_path.display().to_string())
            .expect("probe")
            .expect("recognized");

        assert_eq!(probe.kind, DatasetKind::MeasurementSet);
        assert!(probe.logical_size.contains("rows"));
        assert!(!probe.fields.is_empty());
        assert!(!probe.spectral_windows.is_empty());
        assert!(!probe.antennas.is_empty());
        assert!(
            !probe.observations.is_empty(),
            "overview probe should use OBSERVATION subtable labels"
        );
        assert!(
            probe.scans.is_empty(),
            "overview probe should not scan MAIN.SCAN_NUMBER"
        );
        assert!(
            probe.arrays.is_empty(),
            "overview probe should not scan MAIN.ARRAY_ID"
        );
        assert!(probe.columns.iter().any(|column| column == "DATA"));
        assert_eq!(probe.data_columns, vec!["DATA"]);
        assert!(probe.notes.contains("subtable/schema overview"));
        assert!(
            probe
                .subtables
                .iter()
                .any(|subtable| subtable == "ANTENNA (required)")
        );
        assert!(probe.size_bytes > 0);
    }

    #[test]
    fn probe_measurement_set_uv_range_reads_main_uvw() {
        let (_dir, ms_path) = unpack_small_ms();

        let probe =
            probe_measurement_set_uv_range(ms_path.display().to_string()).expect("uv probe");

        assert!(probe.row_count > 0);
        assert!(probe.min_meters.is_finite());
        assert!(probe.max_meters >= probe.min_meters);
    }

    #[test]
    fn probe_measurement_set_time_range_reads_main_time() {
        let (_dir, ms_path) = unpack_small_ms();

        let probe =
            probe_measurement_set_time_range(ms_path.display().to_string()).expect("time probe");

        assert!(probe.row_count > 0);
        assert!(probe.min_seconds.is_finite());
        assert!(probe.max_seconds >= probe.min_seconds);
    }

    #[test]
    fn application_catalog_exposes_canonical_frontend_projection() {
        let catalog = application_catalog().expect("application catalog");
        let applications = catalog.applications;
        assert_eq!(applications.len(), 43);
        assert!(applications.iter().any(|application| {
            application.id == "imager"
                && application.kind == "task"
                && application.display_name == "Imager"
                && application.executable == "casars-imager"
        }));
        assert!(applications.iter().any(|application| {
            application.id == "casars"
                && application.kind == "launcher"
                && application.include_in_suite
        }));
    }

    #[test]
    fn task_context_options_are_grounded_in_dataset_probe() {
        let (_dir, ms_path) = unpack_small_ms();

        let options = task_context_options(ms_path.display().to_string()).expect("task options");

        assert_eq!(options.dataset_kind, "measurement_set");
        assert_eq!(options.dataset_path, ms_path.display().to_string());
        assert!(!options.spectral_windows.is_empty());
        assert_eq!(
            options.defaults["spectral_window"],
            options.spectral_windows[0]
        );
        assert_eq!(options.data_columns, ["DATA"]);
        assert_eq!(options.defaults["data_column"], "DATA");
        assert!(options.fields.iter().all(|label| label.contains(':')));
    }

    #[test]
    fn parameter_catalog_and_definitions_cover_task_and_session_surfaces() {
        let catalog =
            casa_provider_contracts::builtin_surface_catalog().expect("parameter catalog");
        let surfaces = &catalog.surfaces;
        assert_eq!(surfaces.len(), 42);
        assert_eq!(
            surfaces
                .iter()
                .filter(|surface| surface.kind() == SurfaceKind::Task)
                .count(),
            40
        );
        assert_eq!(
            surfaces
                .iter()
                .filter(|surface| surface.kind() == SurfaceKind::Session)
                .count(),
            2
        );

        let definition = surfaces
            .iter()
            .find(|surface| surface.id() == "imexplore")
            .expect("definition");
        assert_eq!(definition.id(), "imexplore");
        assert_eq!(definition.kind(), SurfaceKind::Session);
        assert!(definition.bindings().iter().any(|binding| {
            binding.name == "image" && binding.concept.id.as_str() == "imexplore.image"
        }));
    }

    #[test]
    fn task_form_is_the_runtime_projection_of_the_builtin_definition() {
        let schema = task_ui_schema("flagdata".to_string()).expect("flagdata schema");
        assert_eq!(schema.schema_version, 2);
        assert_eq!(schema.command_id, "flagdata");
        let arguments = schema.arguments;
        let vis = arguments
            .iter()
            .find(|argument| argument.id == "vis")
            .expect("vis projection");
        assert_eq!(vis.concept_id.as_deref(), Some("data.input.vis"));
        assert_eq!(vis.concept_revision, Some(1));
        assert_eq!(vis.context_role.as_deref(), Some("input_dataset"));
        assert_eq!(vis.parameter_type.as_deref(), Some("array"));
        assert_eq!(
            vis.parser
                .flags
                .as_ref()
                .and_then(|flags| flags.first())
                .map(String::as_str),
            Some("--vis")
        );

        let mode = arguments
            .iter()
            .find(|argument| argument.id == "mode")
            .expect("mode projection");
        assert_eq!(mode.concept_id.as_deref(), Some("flagdata.mode"));
        assert!(
            mode.parser
                .choices
                .as_ref()
                .is_some_and(|choices| choices.contains(&"summary".to_string()))
        );

        let session = task_ui_schema("imexplore".to_string()).expect("session schema");
        assert_eq!(session.command_id, "imexplore");
        assert!(session.arguments.iter().any(|argument| {
            argument.id == "image" && argument.concept_id.as_deref() == Some("imexplore.image")
        }));
    }

    #[test]
    fn ui_projection_preserves_ambiguous_concepts_instead_of_inferring_from_names() {
        for (surface_id, parameter, expected_concept) in [
            ("imager", "vis", "data.input.vis"),
            ("importvla", "vis", "data.output.vis"),
            ("imstat", "imagename", "image.input.imagename"),
            ("imager", "imagename", "image.output.imagename"),
            ("imager", "width", "imager.width"),
            ("impv", "width", "impv.width"),
            ("flagdata", "mode", "flagdata.mode"),
            ("imhead", "mode", "imhead.mode"),
        ] {
            let schema = task_ui_schema(surface_id.to_string()).expect("form projection");
            let argument = schema
                .arguments
                .iter()
                .find(|argument| argument.id == parameter)
                .unwrap_or_else(|| panic!("{surface_id} is missing {parameter}"));
            assert_eq!(
                argument.concept_id.as_deref(),
                Some(expected_concept),
                "{surface_id}.{parameter} must retain its reviewed concept"
            );
        }
    }

    #[test]
    fn parameter_resolution_preserves_context_and_override_origins() {
        let defaults = parameter_defaults("imager".to_string()).expect("defaults");
        assert_eq!(defaults.base_source, SurfaceParameterSourceRecord::Defaults);
        assert!(!defaults.dirty);
        assert_eq!(defaults.states["niter"].origin, "default");

        let context = SurfaceParameterPatch {
            values: HashMap::from([
                (
                    "vis".to_string(),
                    SurfaceParameterValue::String {
                        value: "input.ms".to_string(),
                    },
                ),
                (
                    "imagename".to_string(),
                    SurfaceParameterValue::String {
                        value: "products/image".to_string(),
                    },
                ),
            ]),
            unset: Vec::new(),
        };
        let overrides = SurfaceParameterPatch {
            values: HashMap::from([(
                "niter".to_string(),
                SurfaceParameterValue::Integer { value: 7 },
            )]),
            unset: Vec::new(),
        };
        let resolved = parameter_resolve(
            "imager".to_string(),
            SurfaceParameterBaseSource::Defaults,
            None,
            None,
            context,
            overrides,
        )
        .expect("resolved parameters");
        assert!(resolved.dirty);
        assert_eq!(resolved.states["vis"].origin, "context");
        assert_eq!(resolved.states["niter"].origin, "override");
        assert_eq!(
            resolved.states["niter"].value,
            Some(SurfaceParameterValue::Integer { value: 7 })
        );
        assert!(
            resolved
                .profile_toml
                .as_deref()
                .expect("profile TOML")
                .contains("niter = 7")
        );
    }

    #[test]
    fn parameter_run_safety_exposes_catalog_owned_frontend_gates() {
        let values = HashMap::from([(
            "savemodel".to_string(),
            SurfaceParameterValue::String {
                value: "modelcolumn".to_string(),
            },
        )]);
        let safety = parameter_run_safety("imager".to_string(), values).expect("imager safety");
        assert!(safety.requires_interactive_confirmation);
        assert!(!safety.requires_overwrite_confirmation);
        assert!(safety.requires_input_mutation_confirmation);
        assert!(
            safety
                .classes
                .contains(&SurfaceRunSafetyClass::ProductWrite)
        );
        assert!(
            safety
                .classes
                .contains(&SurfaceRunSafetyClass::InputMutation)
        );
    }

    #[test]
    fn parameter_provider_invocation_uses_simobserve_family_stdin() {
        let values = HashMap::from([(
            "request_kind".to_string(),
            SurfaceParameterValue::String {
                value: "family".to_string(),
            },
        )]);
        let invocation = parameter_provider_invocation("simobserve".to_string(), values)
            .expect("family invocation");
        assert_eq!(invocation.args, ["--json-run", "-"]);
        let request: serde_json::Value =
            serde_json::from_str(invocation.stdin.as_deref().expect("stdin JSON")).unwrap();
        assert_eq!(request["kind"], "family");
        assert_eq!(request["request"]["telescope"], "VLA");
        assert_eq!(request["request"]["output_ms"], "simobserve-family.ms");
        assert!(request["request"].get("model").is_none());
        assert!(request["request"].get("out").is_none());
    }

    #[test]
    fn one_profile_matches_runtime_ui_projection_and_typed_uniffi() {
        let source_path = PathBuf::from("profiles/imager.toml");
        let profile_toml = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../resources/test-profiles/imager-cross-surface.toml"
        ));
        let bundle = builtin_surface_bundle("imager").expect("builtin imager surface");
        let direct = ParameterRuntime::default()
            .open_session(OpenSessionRequest {
                bundle: bundle.clone(),
                workspace: PathBuf::from("."),
                source: BaseSource::File(source_path.clone()),
                profile_text: Some(profile_toml.to_string()),
                context_patch: ResolutionPatch::default(),
                override_patch: ResolutionPatch::default(),
                managed_save: false,
            })
            .expect("direct runtime resolution");

        assert_eq!(
            direct.states()["imsize"].value,
            Some(ParameterValue::Array(vec![
                ParameterValue::Integer(1024),
                ParameterValue::Integer(1024),
            ]))
        );
        assert_eq!(
            direct.states()["cell"].value,
            Some(ParameterValue::Array(vec![
                ParameterValue::String("1arcsec".to_string()),
                ParameterValue::String("1arcsec".to_string()),
            ]))
        );

        let uniffi_snapshot = parameter_load(
            "imager".to_string(),
            profile_toml.to_string(),
            source_path.to_string_lossy().into_owned(),
        )
        .expect("UniFFI profile resolution");
        assert_eq!(
            uniffi_snapshot.states["imsize"].value,
            Some(SurfaceParameterValue::Array {
                values: vec![
                    SurfaceParameterValue::Integer { value: 1024 },
                    SurfaceParameterValue::Integer { value: 1024 },
                ],
            })
        );
        assert_eq!(
            uniffi_snapshot.diagnostics.len(),
            direct.diagnostics().len()
        );
        let canonical_profile = uniffi_snapshot
            .profile_toml
            .as_deref()
            .expect("canonical profile TOML");
        assert!(canonical_profile.contains("imsize = [1024, 1024]"));
        assert!(!canonical_profile.contains("cell ="));

        let uniffi_ui = task_ui_schema("imager".to_string()).expect("UniFFI form projection");
        let arguments = uniffi_ui.arguments;
        for binding in bundle.surface.bindings().iter().filter(|binding| {
            matches!(
                binding.name.as_str(),
                "vis" | "imagename" | "imsize" | "cell" | "niter"
            )
        }) {
            let argument = arguments
                .iter()
                .find(|argument| argument.id == binding.name)
                .unwrap_or_else(|| panic!("UI projection missing {}", binding.name));
            assert_eq!(
                argument.concept_id.as_deref(),
                Some(binding.concept.id.as_str())
            );
            assert_eq!(
                argument.concept_revision,
                Some(u64::from(binding.concept.semantic_revision.0))
            );
        }
    }

    #[test]
    fn explicit_and_managed_parameter_profiles_round_trip() {
        let temp = tempfile::tempdir().expect("tempdir");
        let values = HashMap::from([
            (
                "vis".to_string(),
                SurfaceParameterValue::String {
                    value: "input.ms".to_string(),
                },
            ),
            (
                "imagename".to_string(),
                SurfaceParameterValue::String {
                    value: "products/image".to_string(),
                },
            ),
            (
                "niter".to_string(),
                SurfaceParameterValue::Integer { value: 42 },
            ),
        ]);
        let profile_path = temp.path().join("profiles/imager.toml");
        let save = parameter_save(
            "imager".to_string(),
            values.clone(),
            profile_path.to_string_lossy().into_owned(),
        )
        .expect("save profile");
        assert_eq!(save.path, profile_path.to_string_lossy());
        assert!(save.bytes_written > 0);
        assert_eq!(save.managed_kind, None);

        let profile_toml = fs::read_to_string(&profile_path).expect("read profile");
        assert!(profile_toml.contains("surface = \"imager\""));
        assert!(profile_toml.contains("niter = 42"));
        assert!(!profile_toml.contains("cell ="));
        let loaded = parameter_load(
            "imager".to_string(),
            profile_toml,
            profile_path.to_string_lossy().into_owned(),
        )
        .expect("load profile");
        assert!(matches!(
            loaded.base_source,
            SurfaceParameterSourceRecord::File { .. }
        ));
        assert_eq!(loaded.states["niter"].origin, "base_profile");
        assert_eq!(
            loaded.states["niter"].value,
            Some(SurfaceParameterValue::Integer { value: 42 })
        );

        let workspace = temp.path().join("workspace");
        let lifecycle = ParameterTaskLifecycle::new();
        lifecycle
            .before_execution(
                "attempt-1".to_string(),
                "imager".to_string(),
                workspace.to_string_lossy().into_owned(),
                values,
                true,
            )
            .expect("write Last through lifecycle");

        let last = parameter_last(
            "imager".to_string(),
            workspace.to_string_lossy().into_owned(),
            false,
        )
        .expect("load Last")
        .expect("Last exists");
        assert_eq!(last.base_source, SurfaceParameterSourceRecord::Last);
        assert_eq!(
            last.states["niter"].value,
            Some(SurfaceParameterValue::Integer { value: 42 })
        );

        assert!(
            lifecycle
                .after_completion("attempt-1".to_string(), true)
                .is_empty()
        );
        assert!(
            parameter_last(
                "imager".to_string(),
                workspace.to_string_lossy().into_owned(),
                true,
            )
            .expect("load Last Successful")
            .is_some()
        );

        let error = parameter_last(
            "imexplore".to_string(),
            workspace.to_string_lossy().into_owned(),
            true,
        )
        .expect_err("sessions do not have Last Successful");
        assert!(error.to_string().contains("does not have Last Successful"));
    }

    #[test]
    fn managed_parameter_lookup_distinguishes_missing_from_corrupt() {
        let workspace = tempfile::tempdir().unwrap();
        assert!(
            parameter_last(
                "imager".to_string(),
                workspace.path().to_string_lossy().into_owned(),
                false,
            )
            .unwrap()
            .is_none()
        );

        ManagedStateStore::for_workspace(workspace.path())
            .write("imager", ManagedProfileKind::Last, "not valid profile TOML")
            .unwrap();
        let error = parameter_last(
            "imager".to_string(),
            workspace.path().to_string_lossy().into_owned(),
            false,
        )
        .expect_err("corrupt Last must not be treated as missing");
        assert!(error.to_string().contains("open parameter session"));
    }

    #[test]
    fn measurement_set_summary_builds_listobs_text_for_gui() {
        let (_dir, ms_path) = unpack_small_ms();

        let result = build_measurement_set_summary(MeasurementSetSummaryRequest {
            dataset_path: ms_path.display().to_string(),
            format: "text".to_string(),
            field: None,
            spectral_window: None,
            timerange: None,
            uvrange: None,
            antenna: None,
            scan: None,
            correlation: None,
            array: None,
            observation: None,
            intent: None,
            feed: None,
            msselect: None,
        })
        .expect("build listobs-style summary");

        assert_eq!(result.format, "text");
        assert_eq!(result.selection_summary, "all rows");
        assert!(result.summary_text.contains("Observation"));
        assert!(result.summary_text.contains("Spectral Windows"));
    }

    #[test]
    fn measurement_set_plot_builds_typed_document_without_gui_png_work() {
        let (_dir, ms_path) = unpack_small_ms();

        assert_eq!(
            MeasurementSetPlotPreset::all().len(),
            MsPlotPreset::ALL.len()
        );
        for preset in MeasurementSetPlotPreset::all().iter().copied() {
            let plot = match build_measurement_set_plot(MeasurementSetPlotRequest {
                dataset_path: ms_path.display().to_string(),
                preset,
                field: None,
                spectral_window: None,
                timerange: None,
                uvrange: None,
                antenna: None,
                scan: None,
                correlation: None,
                array: None,
                observation: None,
                intent: None,
                feed: None,
                msselect: None,
                data_column: "DATA".to_string(),
                color_by: None,
                avgchannel: None,
                avgtime: None,
                avgscan: false,
                avgfield: false,
                avgbaseline: false,
                avgantenna: false,
                avgspw: false,
                scalar: false,
                iteraxis: None,
                width: DEFAULT_PLOT_WIDTH,
                height: DEFAULT_PLOT_HEIGHT,
                max_plot_points: 10_000,
            }) {
                Ok(plot) => plot,
                Err(FrontendServiceError::Plot { reason })
                    if matches!(
                        preset,
                        MeasurementSetPlotPreset::WeightSpectrumVsTime
                            | MeasurementSetPlotPreset::SigmaSpectrumVsTime
                    ) && (reason.contains("requires") || reason.contains("shape [0, 0]")) =>
                {
                    continue;
                }
                Err(error) => panic!("plot {preset:?}: {error}"),
            };

            assert_eq!(plot.preset, preset);
            assert_eq!(plot.data_column, "data");
            assert_eq!(plot.render.image_format, "none");
            assert_eq!(plot.render.width, DEFAULT_PLOT_WIDTH);
            assert_eq!(plot.render.height, DEFAULT_PLOT_HEIGHT);
            assert!(plot.image_bytes.is_empty());
            assert!(!plot.title.is_empty());
            assert!(!plot.x_axis.label.is_empty());
            assert!(!plot.y_axis.label.is_empty());
            assert_eq!(plot.document.title, plot.title);
            assert!(
                !plot.document.layers.is_empty() || !plot.document.panels.is_empty(),
                "plot document should expose manipulable layers or panels for {preset:?}"
            );
            match preset {
                MeasurementSetPlotPreset::UvCoverage => {
                    assert_eq!(
                        plot.document.layers.len() as u64,
                        plot.sampling.series_count
                    );
                    assert!(
                        plot.document.layers.len() <= 32,
                        "UV coverage should coalesce baseline tracks into coarse display groups"
                    );
                }
                MeasurementSetPlotPreset::AntennaLayout => {
                    let layer = plot.document.layers.first().expect("antenna layer");
                    assert_eq!(layer.x_values.len(), layer.point_labels.len());
                    assert_eq!(layer.x_values.len(), layer.point_symbol_sizes.len());
                }
                MeasurementSetPlotPreset::ScanTimeline
                | MeasurementSetPlotPreset::SpectralWindowCoverage => {
                    assert!(
                        plot.document
                            .layers
                            .iter()
                            .any(|layer| !layer.interval_labels.is_empty()),
                        "interval labels should survive document conversion for {preset:?}"
                    );
                    assert!(
                        plot.document
                            .layers
                            .iter()
                            .any(|layer| !layer.color_group.is_empty()),
                        "interval color groups should survive document conversion for {preset:?}"
                    );
                }
                _ => {}
            }
            if preset == MeasurementSetPlotPreset::AntennaLayout
                || preset == MeasurementSetPlotPreset::ScanTimeline
                || preset == MeasurementSetPlotPreset::SpectralWindowCoverage
            {
                assert!(plot.sampling.diagnostics.iter().any(|line| {
                    line.contains("metadata-oriented plot") || line.contains("no drawable")
                }));
            } else {
                assert!(!plot.series.is_empty());
                assert!(plot.sampling.rendered_point_count > 0);
            }
            assert_eq!(plot.sampling.requested_max_points, 10_000);
            assert!(plot.selection_summary.contains("data column data"));
        }
    }

    #[test]
    #[ignore = "diagnostic timing helper for local tutorial MeasurementSets"]
    fn twhya_measurement_set_plot_timing_diagnostic() {
        let ms_path = std::env::var("CASA_RS_TWHYA_MS")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from("/private/tmp/casa-rs-wave6-prof/twhya_calibrated.ms")
            });
        if !ms_path.is_dir() {
            eprintln!("skipping: {} is not staged", ms_path.display());
            return;
        }
        for preset in MeasurementSetPlotPreset::all().iter().copied() {
            let started = Instant::now();
            let plot = build_measurement_set_plot(MeasurementSetPlotRequest {
                dataset_path: ms_path.display().to_string(),
                preset,
                field: None,
                spectral_window: None,
                timerange: None,
                uvrange: None,
                antenna: None,
                scan: None,
                correlation: None,
                array: None,
                observation: None,
                intent: None,
                feed: None,
                msselect: None,
                data_column: "DATA".to_string(),
                color_by: None,
                avgchannel: None,
                avgtime: None,
                avgscan: false,
                avgfield: false,
                avgbaseline: false,
                avgantenna: false,
                avgspw: false,
                scalar: false,
                iteraxis: None,
                width: DEFAULT_PLOT_WIDTH,
                height: DEFAULT_PLOT_HEIGHT,
                max_plot_points: 250_000,
            })
            .unwrap_or_else(|error| panic!("plot {preset:?}: {error}"));
            eprintln!(
                "{preset:?}: total={} ms, points={}, layers={}, panels={}, diagnostics={:?}",
                started.elapsed().as_millis(),
                plot.sampling.rendered_point_count,
                plot.document.layers.len(),
                plot.document.panels.len(),
                plot.sampling.diagnostics
            );
        }

        let ms = MeasurementSet::open(&ms_path).expect("open timing MS");
        let row_count = ms.main_table().row_count();
        let target_rows = 250_000usize / (384 * 2);
        let rows = sampled_indices_for_test(row_count, target_rows);
        let scalars_started = Instant::now();
        let _times = selected_f64_values(ms.main_table(), "TIME", &rows).expect("time rows");
        let _fields = selected_i32_values(ms.main_table(), "FIELD_ID", &rows).expect("field rows");
        let _ddids =
            selected_i32_values(ms.main_table(), "DATA_DESC_ID", &rows).expect("ddid rows");
        let scalars_elapsed = scalars_started.elapsed();
        let flags_started = Instant::now();
        let flags = ms
            .main_table()
            .column_accessor("FLAG")
            .expect("FLAG")
            .array_cells_owned(&rows)
            .expect("flag rows");
        let flags_elapsed = flags_started.elapsed();
        let data_started = Instant::now();
        let data = ms
            .main_table()
            .column_accessor("DATA")
            .expect("DATA")
            .array_cells_owned(&rows)
            .expect("data rows");
        let data_elapsed = data_started.elapsed();
        let scan_started = Instant::now();
        let mut sample_count = 0usize;
        for cell in data {
            sample_count += match cell {
                Some(ArrayValue::Complex32(values)) => values.len(),
                Some(ArrayValue::Complex64(values)) => values.len(),
                _ => 0,
            };
        }
        let scan_elapsed = scan_started.elapsed();
        eprintln!(
            "selected row probe: rows={}, samples={}, scalar={} ms, FLAG={} ms ({} cells), DATA={} ms, sample-scan={} ms",
            rows.len(),
            sample_count,
            scalars_elapsed.as_millis(),
            flags_elapsed.as_millis(),
            flags.len(),
            data_elapsed.as_millis(),
            scan_elapsed.as_millis()
        );
    }

    fn sampled_indices_for_test(total: usize, target: usize) -> Vec<usize> {
        if target >= total {
            return (0..total).collect();
        }
        if target <= 1 {
            return vec![0];
        }
        let step = (total - 1) as f64 / (target - 1) as f64;
        (0..target)
            .map(|index| ((index as f64 * step).round() as usize).min(total - 1))
            .collect()
    }

    #[test]
    fn probe_project_discovers_ms_and_table_without_suffix_guessing() {
        let (dir, ms_path) = unpack_small_ms();
        let table_path = dir.path().join("derived_gain");
        make_table(&table_path);
        fs::write(dir.path().join("notes.txt"), "not a dataset").expect("notes");

        let project = probe_project(dir.path().display().to_string()).expect("project");

        assert!(
            project
                .datasets
                .iter()
                .any(|dataset| dataset.path == ms_path.display().to_string()
                    && dataset.kind == DatasetKind::MeasurementSet)
        );
        assert!(
            project
                .datasets
                .iter()
                .any(|dataset| dataset.path == table_path.display().to_string()
                    && dataset.kind == DatasetKind::Table)
        );
        assert!(
            !project
                .datasets
                .iter()
                .any(|dataset| dataset.name == "notes.txt")
        );
    }

    #[test]
    fn task_completion_uses_declared_product_and_exact_probe_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain.cal");
        make_table(&table_path);
        let completion = task_completion(
            "gaincal".to_string(),
            r#"{"kind":"solve_gain","report":{}}"#.to_string(),
            dir.path().display().to_string(),
            vec![TaskParameterValue {
                name: "output".to_string(),
                value: "gain.cal".to_string(),
            }],
        )
        .expect("typed completion");

        assert_eq!(completion.products.len(), 1);
        let product = &completion.products[0];
        assert_eq!(product.path, table_path.display().to_string());
        assert_eq!(product.resource_kind, TaskProductKind::CasaTable);
        assert_eq!(
            product.dataset.as_ref().map(|dataset| &dataset.kind),
            Some(&DatasetKind::Table)
        );
        assert!(product.diagnostic.is_none());
    }

    #[test]
    fn task_completion_retains_unrecognized_declared_dataset_with_diagnostic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let image_path = dir.path().join("mom0.image");
        std::fs::create_dir(&image_path).expect("empty image directory");
        let completion = task_completion(
            "immoments".to_string(),
            r#"{"kind":"immoments","result":{}}"#.to_string(),
            dir.path().display().to_string(),
            vec![TaskParameterValue {
                name: "outfile".to_string(),
                value: "mom0.image".to_string(),
            }],
        )
        .expect("typed completion");

        assert_eq!(completion.products.len(), 1);
        let product = &completion.products[0];
        assert!(product.exists);
        assert!(product.dataset.is_none());
        assert!(product.diagnostic.is_some());
    }

    #[test]
    fn probe_image_reports_science_units_and_observing_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("restored.image");
        let mut coords = CoordinateSystem::new();
        let cell_rad = (0.1_f64 / 3600.0).to_radians();
        coords.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, -0.5],
            [-cell_rad, cell_rad],
            [1.5, 1.5],
        )));
        coords.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            100.0e9,
            2.0e6,
            2.0,
            100.0e9,
        )));
        let mut image =
            PagedImage::<f32>::create(vec![4, 4, 5], coords, &path).expect("create image");
        image.set_units("Jy/beam").expect("set units");
        image
            .set_image_info(&ImageInfo {
                image_type: ImageType::Intensity,
                object_name: "TW Hya".to_string(),
                beam_set: ImageBeamSet::new(GaussianBeam::new(
                    (0.42_f64 / 3600.0).to_radians(),
                    (0.31_f64 / 3600.0).to_radians(),
                    12.0_f64.to_radians(),
                )),
            })
            .expect("set image info");
        image.save().expect("save image");

        let probe = probe_path(path.display().to_string())
            .expect("probe")
            .expect("recognized");

        assert_eq!(probe.kind, DatasetKind::Image);
        assert_eq!(probe.units, "Jy/beam");
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Pixel type: float32")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Object: TW Hya")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Image type: Intensity")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Cell size: 0.1 x 0.1 arcsec")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line.starts_with("Center: RA ") && line.contains(" Dec "))
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Cube center frequency: 100 GHz")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Total bandwidth: 10 MHz")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Channel separation: 2 MHz")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Beam: 0.42 x 0.31 arcsec, PA 12 deg")
        );

        let plane_snapshot = build_image_explorer_snapshot(image_explorer_request(&path, "plane"))
            .expect("image explorer plane snapshot");
        assert_eq!(plane_snapshot.active_view, "plane");
        assert!(plane_snapshot.capabilities.renderable_plane);
        let plane = plane_snapshot.plane.expect("renderable plane");
        assert!(plane.width > 0);
        assert!(plane.height > 0);

        let snapshot = build_image_explorer_snapshot(image_explorer_request(&path, "spectrum"))
            .expect("image explorer snapshot");
        assert_eq!(snapshot.active_view, "spectrum");
        assert!(snapshot.profile.is_some());

        let mut request = image_explorer_request(&path, "plane");
        request.focus = "inspector".to_string();
        request.plane_content_mode = "spreadsheet".to_string();
        request.parameters.blc = "0,0,0".to_string();
        request.parameters.trc = "3,3,0".to_string();
        request.parameters.inc = "1,1,1".to_string();
        request.cursor_x = Some(1);
        request.cursor_y = Some(1);
        let requested_snapshot =
            build_image_explorer_snapshot(request).expect("image explorer requested snapshot");
        assert_eq!(requested_snapshot.active_view, "plane");
        assert_eq!(requested_snapshot.focus, "inspector");
        let cursor = requested_snapshot.plane_cursor.expect("plane cursor");
        assert_eq!(cursor.pixel_x, 1);
        assert_eq!(cursor.pixel_y, 1);

        let region_path = path.with_extension("crtf");
        fs::write(
            &region_path,
            "#CRTFv0 CASA Region Text Format version 0\nbox[[1pix,1pix],[2pix,2pix]]\n",
        )
        .expect("write test region");
        let mut region_request = image_explorer_request(&path, "plane");
        let mut load_region = image_explorer_command("load_region_file");
        load_region.path = Some(region_path.display().to_string());
        region_request.commands.push(load_region);
        let region_snapshot = build_image_explorer_snapshot(region_request)
            .expect("image explorer region load snapshot");
        let region = region_snapshot.region.expect("loaded region");
        assert_eq!(region.label, "restored");
        assert_eq!(region.shape_count, 1);

        let mut append_region_request = image_explorer_request(&path, "plane");
        append_region_request
            .commands
            .push(image_explorer_command("start_region_shape"));
        for (x, y) in [(0, 0), (1, 0), (1, 1)] {
            let mut vertex = image_explorer_command("append_region_vertex");
            vertex.x = Some(x);
            vertex.y = Some(y);
            append_region_request.commands.push(vertex);
        }
        append_region_request
            .commands
            .push(image_explorer_command("close_region_shape"));
        let mut append_region = image_explorer_command("append_region_file");
        append_region.path = Some(region_path.display().to_string());
        append_region_request.commands.push(append_region);
        let appended_region_snapshot = build_image_explorer_snapshot(append_region_request)
            .expect("image explorer region append snapshot");
        assert_eq!(
            appended_region_snapshot
                .region
                .expect("appended region")
                .shape_count,
            2
        );
    }

    #[test]
    fn image_explorer_typed_commands_reject_unknown_or_incomplete_requests() {
        let unknown = image_explorer_command_owner(image_explorer_command("mystery"))
            .expect_err("unknown command must fail closed");
        assert!(unknown.contains("unknown image explorer command `mystery`"));

        let incomplete = image_explorer_command_owner(image_explorer_command("load_region_file"))
            .expect_err("missing path must fail closed");
        assert!(incomplete.contains("requires `path`"));
    }

    #[test]
    fn table_browser_snapshot_json_uses_tablebrowser_protocol() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let snapshot_json = build_table_browser_snapshot_json(
            table_path.display().to_string(),
            100,
            24,
            8,
            Some("columns".to_string()),
        )
        .expect("table browser snapshot");
        let snapshot: serde_json::Value =
            serde_json::from_str(&snapshot_json).expect("snapshot json");
        assert_eq!(snapshot["view"], "columns");
        assert_eq!(snapshot["capabilities"]["editable"], false);
        assert_eq!(snapshot["table_path"], table_path.display().to_string());
        assert!(
            snapshot["content_lines"]
                .as_array()
                .is_some_and(|lines| !lines.is_empty())
        );
    }

    #[test]
    fn table_browser_request_json_replays_navigation_commands() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let request_json = serde_json::json!({
            "dataset_path": table_path,
            "selected_view": "columns",
            "focus": "main",
            "commands": [
                { "command": "move_down", "steps": 1 }
            ]
        })
        .to_string();
        let snapshot_json = build_table_browser_snapshot_from_request_json(request_json)
            .expect("table browser requested snapshot");
        let snapshot: serde_json::Value =
            serde_json::from_str(&snapshot_json).expect("snapshot json");
        assert_eq!(snapshot["view"], "columns");
        assert_eq!(snapshot["focus"], "main");
        assert_eq!(
            snapshot["vertical_metrics"]["selected_index"]
                .as_u64()
                .expect("selected index"),
            1
        );
        assert!(snapshot["selected_address"].is_object());
        assert!(snapshot["inspector"].is_object());
    }

    #[test]
    fn table_browser_cell_window_json_returns_typed_scroll_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let request_json = serde_json::json!({
            "dataset_path": table_path,
            "row_start": 0,
            "row_limit": 4,
            "column_start": 1,
            "column_limit": 1
        })
        .to_string();
        let window_json =
            build_table_browser_cell_window_json(request_json).expect("table browser cell window");
        let window: serde_json::Value =
            serde_json::from_str(&window_json).expect("cell window json");

        assert_eq!(window["row_count"].as_u64(), Some(1));
        assert_eq!(window["column_count"].as_u64(), Some(2));
        assert_eq!(window["row_start"].as_u64(), Some(0));
        assert_eq!(window["column_start"].as_u64(), Some(1));
        assert_eq!(window["columns"][0]["name"], "id");
        assert_eq!(window["columns"][1]["header"], "name str");
        assert_eq!(window["rows"][0]["index"].as_u64(), Some(0));
        assert_eq!(
            window["rows"][0]["cells"][0]["column_index"].as_u64(),
            Some(1)
        );
        assert_eq!(window["rows"][0]["cells"][0]["display"], "\"gain\"");
        assert_eq!(window["rows"][0]["cells"][0]["defined"], true);
    }

    #[test]
    fn table_browser_cell_window_summarizes_schema_array_without_preview_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("array_table");
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "DATA",
            PrimitiveType::Float32,
            vec![2, 2],
        )])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        let mut keywords = RecordValue::default();
        keywords.upsert(
            "MEASINFO",
            Value::Record(RecordValue::new(vec![RecordField::new(
                "type",
                Value::Scalar(ScalarValue::String("visibility".to_string())),
            )])),
        );
        table.set_column_keywords("DATA", keywords);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DATA",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).expect("array"),
                )),
            )]))
            .expect("row");
        table
            .save(TableOptions::new(&table_path))
            .expect("save table");

        let request_json = serde_json::json!({
            "dataset_path": table_path,
            "row_start": 0,
            "row_limit": 1,
            "column_start": 0,
            "column_limit": 1,
            "column_options": [{
                "column_index": 0,
                "array_inline_limit": 4
            }]
        })
        .to_string();
        let window_json =
            build_table_browser_cell_window_json(request_json).expect("table browser cell window");
        let window: serde_json::Value =
            serde_json::from_str(&window_json).expect("cell window json");

        assert_eq!(
            window["columns"][0]["keywords"][0],
            "MEASINFO.type = \"visibility\""
        );
        assert_eq!(
            window["rows"][0]["cells"][0]["display"],
            "[1.0000, 2.0000, 3.0000, 4.0000]"
        );

        let copy_request = serde_json::json!({
            "dataset_path": table_path,
            "row_index": 0,
            "column_index": 0
        })
        .to_string();
        let copy_json =
            build_table_browser_cell_value_json(copy_request).expect("table browser cell value");
        let copy_value: String = serde_json::from_str(&copy_json).expect("cell value json");
        assert_eq!(copy_value, "[1.0000, 2.0000, 3.0000, 4.0000]");
    }

    #[test]
    fn notebook_typed_bridge_creates_saves_and_finalizes_a_receipt() {
        let project = tempfile::tempdir().expect("project");
        let project_root = project.path().canonicalize().expect("canonical project");
        let created = notebook_create(NotebookCreateRequest {
            project_root: project_root.display().to_string(),
            filename: Some("Analysis.md".to_owned()),
            title: "Analysis".to_owned(),
        })
        .expect("create notebook");
        let begin = notebook_begin_recording(NotebookBeginRecordingRequest {
            project_root: project_root.display().to_string(),
            policy: NotebookRecordingPolicy::Record,
            request: NotebookRecordingRequest {
                initiating_surface: "gui".to_owned(),
                operation_id: "imager".to_owned(),
                notebook_id: Some(created.id.clone()),
                cell_id: None,
                task_intent: Some(NotebookTaskIntent {
                    format: 1,
                    surface: "imager".to_owned(),
                    kind: "task".to_owned(),
                    contract: 1,
                    parameters: HashMap::from([(
                        "niter".to_owned(),
                        NotebookValue::Number { value: 4.0 },
                    )]),
                }),
                execution_input: None,
                provider_contract_version: 1,
                resolved_parameters: HashMap::from([(
                    "niter".to_owned(),
                    NotebookValue::Number { value: 4.0 },
                )]),
                run_safety: NotebookRunSafetyRecord {
                    classification: "product_write".to_owned(),
                    affected_paths: vec!["products/test.image".to_owned()],
                },
                approvals: Vec::new(),
            },
        })
        .expect("begin recording");
        let handle = begin.handle.expect("recording handle");
        notebook_finalize_recording(NotebookFinalizeRecordingRequest {
            project_root: project_root.display().to_string(),
            handle,
            finalization: NotebookReceiptFinalization {
                status: "succeeded".to_owned(),
                finished_at: 2,
                affected_paths: vec!["products/test.image".to_owned()],
                products: vec![NotebookReceiptArtifact {
                    role: "image".to_owned(),
                    path: "products/test.image".to_owned(),
                    media_type: None,
                }],
                artifacts: Vec::new(),
                diagnostics: Vec::new(),
                stdout: b"ok".to_vec(),
                stderr: Vec::new(),
                casa_log: None,
            },
        })
        .expect("finalize recording");

        let projection =
            notebook_project(project_root.display().to_string()).expect("reload notebook project");
        let receipt = &projection.notebooks[0].receipts[0];
        assert_eq!(receipt.status, "succeeded");
        assert_eq!(
            receipt
                .sparse_intent
                .as_ref()
                .and_then(|intent| intent.parameters.get("niter")),
            Some(&NotebookValue::Number { value: 4.0 })
        );
    }

    #[test]
    fn notebook_projection_includes_authored_task_cells_without_receipts() {
        let project = tempfile::tempdir().expect("project");
        let project_root = project.path().canonicalize().expect("canonical project");
        let created = notebook_create(NotebookCreateRequest {
            project_root: project_root.display().to_string(),
            filename: Some("Authored.md".to_owned()),
            title: "Authored".to_owned(),
        })
        .expect("create notebook");
        let cell_id = casa_notebook::CellId::new();
        let source = format!(
            "{}\n<!-- casa-rs-cell:v1 id={cell_id} kind=task -->\n```toml\n[casars]\nformat = 1\nsurface = \"imhead\"\nkind = \"task\"\ncontract = 1\n\n[parameters]\nmode = \"summary\"\n```\n<!-- /casa-rs-cell -->\n",
            created.source
        );
        notebook_save(NotebookSaveRequest {
            project_root: project_root.display().to_string(),
            filename: "Authored.md".to_owned(),
            base_hash: created.content_hash,
            source,
            resolution: NotebookConflictResolution::Reject,
        })
        .expect("save authored task cell");

        let projection =
            notebook_project(project_root.display().to_string()).expect("reload notebook project");
        let notebook = &projection.notebooks[0];
        assert!(notebook.receipts.is_empty());
        assert_eq!(notebook.cells[0].id, cell_id.to_string());
        assert_eq!(notebook.cells[0].kind, "task");
        assert!(notebook.cells[0].body.contains("[parameters]"));
        assert_eq!(
            notebook.cells[0].task_intent.as_ref().unwrap().surface,
            "imhead"
        );
        assert_eq!(
            notebook.cells[0]
                .task_intent
                .as_ref()
                .and_then(|intent| intent.parameters.get("mode")),
            Some(&NotebookValue::String {
                value: "summary".to_owned()
            })
        );
    }

    #[test]
    fn assistant_json_bridge_persists_provider_neutral_conversations() {
        let project = tempfile::tempdir().expect("project");
        let project_root = project.path().canonicalize().expect("canonical project");
        let created_json = assistant_create_conversation_json(
            serde_json::json!({
                "project_root": project_root,
                "title": "Calibration discussion",
                "primary_attachment": {
                    "kind": "notebook",
                    "identifier": "Analysis.md",
                    "label": "Analysis",
                    "primary": false
                },
                "profile": {
                    "profile_version": 1,
                    "backend_id": "codex_app_server",
                    "authority": "work",
                    "model": "gpt-test",
                    "effort": "medium",
                    "agent_command": "codex",
                    "python_command": "python3"
                }
            })
            .to_string(),
        )
        .expect("create assistant conversation");
        let mut created: serde_json::Value = serde_json::from_str(&created_json).unwrap();
        assert_eq!(created["attachments"][0]["primary"], true);
        created["draft"] = serde_json::Value::String("continue here".to_owned());

        assistant_save_conversation_json(
            serde_json::json!({
                "project_root": project_root,
                "transcript": created
            })
            .to_string(),
        )
        .expect("save assistant conversation");

        let conversations_json = assistant_conversations_json(project_root.display().to_string())
            .expect("list assistant conversations");
        let conversations: serde_json::Value = serde_json::from_str(&conversations_json).unwrap();
        assert_eq!(conversations[0]["draft"], "continue here");
        assert_eq!(
            conversations[0]["profile"]["backend_id"],
            "codex_app_server"
        );
        assert!(conversations[0].get("provider_envelope").is_none());

        let protocol_json = assistant_protocol_info_json().expect("assistant protocol info");
        let protocol: serde_json::Value = serde_json::from_str(&protocol_json).unwrap();
        assert_eq!(protocol["retrieval_engine"], "sqlite_fts5_unicode61");
        assert_eq!(protocol["authority_presets"][1], "work");
        assert_eq!(protocol["project_mcp_tools"][0], "corpus.search");
    }

    #[test]
    fn assistant_corpus_bridge_exposes_bounded_cited_search_not_sql() {
        let project = tempfile::tempdir().expect("project");
        let project_root = project.path().canonicalize().expect("canonical project");
        let report_json = assistant_corpus_index_json(
            serde_json::json!({
                "project_root": project_root,
                "documents": [{
                    "id": "rao:calibration",
                    "layer": "baseline",
                    "title": "Interferometric calibration",
                    "source_identity": "rao/calibration.md",
                    "content": "A complex gain calibrator constrains antenna based amplitudes and phases.",
                    "citation": {
                        "label": "Calibration guide",
                        "locator": "section 4",
                        "source_path": "rao/calibration.md",
                        "section": "Complex gains"
                    },
                    "redistribution_cleared": true
                }],
                "remove_missing_layers": ["baseline"]
            })
            .to_string(),
        )
        .expect("index assistant corpus");
        let report: serde_json::Value = serde_json::from_str(&report_json).unwrap();
        assert_eq!(report["indexed_documents"], 1);

        let hits_json = assistant_corpus_search_json(
            serde_json::json!({
                "project_root": project_root,
                "query": "antenna gain phase calibration",
                "limit": 1000
            })
            .to_string(),
        )
        .expect("search assistant corpus");
        let hits: serde_json::Value = serde_json::from_str(&hits_json).unwrap();
        assert_eq!(hits.as_array().unwrap().len(), 1);
        assert_eq!(hits[0]["citation"]["section"], "Complex gains");
        assert_eq!(hits[0]["untrusted_evidence"], true);

        let oversized = "x".repeat(MAX_ASSISTANT_CORPUS_QUERY_BYTES + 1);
        let error = assistant_corpus_search_json(
            serde_json::json!({
                "project_root": project_root,
                "query": oversized,
                "limit": 1
            })
            .to_string(),
        )
        .expect_err("oversized query must fail closed");
        assert!(error.to_string().contains("host limit"));
    }

    #[test]
    fn unrecognized_path_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("plain.dat");
        fs::write(&file, "plain").expect("write");

        assert!(
            probe_path(file.display().to_string())
                .expect("probe")
                .is_none()
        );
    }
}
