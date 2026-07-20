// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared frontend services exposed to Swift and Python through UniFFI.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_images::{AnyPagedImage, ImageInfo, ImagePixelType, parse_image_channel_selection};
use casa_ms::plot::UvCoverageSeries;
use casa_ms::{
    MeasurementSet, MeasurementSetPlotPayload, MeasurementSetSummary,
    MeasurementSetSummaryOutputFormat, MsExploreSpec, MsPageExportRange, MsPlotData,
    MsPlotDataPanel, MsPlotPayload, MsPlotPreset, MsPlotSpec, MsScatterGridPayload,
    MsScatterPagePayload, MsScatterPlotPayload, MsScatterSeries, MsSelection,
    MsSelectorEditContext, build_msexplore_payload_from_spec, validate_ms_selector_edit,
};
use casa_notebook::{
    ASSISTANT_PROFILE_VERSION, ASSISTANT_TRANSCRIPT_SCHEMA_VERSION, AssistantActivity,
    AssistantAttachment, AssistantCitation, AssistantContextItem, AssistantPinReference,
    AssistantPythonProvenance, AssistantSessionProfile, AssistantStore, AssistantTaskSuggestion,
    AttemptHandle, CORPUS_SCHEMA_VERSION, CellId, ConflictResolution, ConversationTranscript,
    CorpusDocumentInput, CorpusIndex, CorpusLayer, CorpusReconciliationScope, ExecutionInput,
    ExecutionReceipt, ExecutionStatus, ExportMode, NotebookDocument, NotebookId, NotebookStore,
    PreparedCorpusReconciliation, ProjectCorpusSource, ProjectSourceExtractionOutcome,
    ProjectSourceExtractionStatus, PythonEnvironmentIdentity, PythonExecutionAuthority,
    PythonExecutionInput, ReceiptFinalization, RecordingPolicy, RecordingRequest, RunId,
    SaveResult, SaveVisualizationRequest, TaskCellIntent, Timestamp, TutorialAcquisitionApproval,
    TutorialProject, TutorialTemplate, VisualizationRenderMetadata, VisualizationReopenIntent,
    VisualizationSnapshot,
};
use casa_provider_contracts::{
    NormalizationRule, ParameterValue, ProviderInvocationAdaptation, RunProductKind,
    RunProductRole, RunSafetyClass, SelectorGrammar, SurfaceContractBundle, SurfaceKind,
    builtin_application_catalog, builtin_surface_bundle, builtin_surface_catalog, project_ui_form,
};
use casa_tables::{ArrayShapeContract, ColumnType, Table, TableOptions};
use casa_task_runtime::{
    BaseSource, DiagnosticCode, ManagedProfileKind, ManagedStateStore, OpenSessionRequest,
    ParameterEditDiagnosticCode, ParameterEditSuggestion, ParameterRuntime, ParameterSession,
    ResolutionPatch, SessionLastCoordinator, TaskLastCoordinator, TaskOutputValue,
    decode_task_completion, parse_parameter_text, parse_profile, project_provider_invocation,
    render_documented_template, validate_parameter_edit, write_parameter_profile_atomic,
};
use casa_types::measures::direction::{
    angular_increment_arcseconds, declination_increment_arcseconds, format_declination_labeled,
    format_right_ascension_labeled,
};
use casa_types::{ArrayValue, PrimitiveType, ScalarValue, Value};
use casars::imagebrowser_runtime::ImageBrowserSession;
use casars::tablebrowser_runtime::TableBrowser;
use casars_imagebrowser_protocol::ImageBrowserViewport;
use casars_imagebrowser_protocol::{
    ImageBrowserCommand, ImageBrowserFocus, ImageBrowserParameters, ImageBrowserPreviewRequest,
    ImagePlaneContentMode,
};
use casars_tablebrowser_protocol::{BrowserCommand, BrowserFocus, BrowserView, BrowserViewport};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_PROJECT_SCAN_ENTRIES: usize = 512;
const MAX_PROJECT_SCAN_DEPTH: usize = 4;
const DEFAULT_GUI_MAX_PLOT_POINTS: u64 = 250_000;
const FRONTEND_POINT_PROVENANCE_LIMIT: usize = 8_000;
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

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum TableBrowserBookmark {
    Cell { row: u64, column: String },
    TableKeyword { path: Vec<String> },
    ColumnKeyword { column: String, path: Vec<String> },
    Subtable { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserParameters {
    pub view: String,
    pub row_start: u64,
    pub row_count: u64,
    pub linked_table: Option<String>,
    pub bookmark: Option<TableBrowserBookmark>,
    pub content_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum TableBrowserCommand {
    Configure { parameters: TableBrowserParameters },
    SetFocus { focus: String },
    CycleView { forward: bool },
    MoveUp { steps: u64 },
    MoveDown { steps: u64 },
    MoveLeft { steps: u64 },
    MoveRight { steps: u64 },
    PageUp { pages: u64 },
    PageDown { pages: u64 },
    Activate,
    Back,
    Escape,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserSnapshotRequest {
    pub dataset_path: String,
    pub width: u16,
    pub height: u16,
    pub inspector_height: u16,
    pub selected_view: String,
    pub focus: String,
    pub commands: Vec<TableBrowserCommand>,
    pub transient_commands: Vec<TableBrowserCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserCapabilities {
    pub editable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserBreadcrumb {
    pub label: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserViewport {
    pub width: u16,
    pub height: u16,
    pub inspector_height: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserNavigationMetrics {
    pub selected_index: u64,
    pub total_items: u64,
    pub viewport_items: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserValuePathSegment {
    pub segment: String,
    pub name: Option<String>,
    pub flat_index: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserSelectedAddress {
    pub kind: String,
    pub table_path: String,
    pub row: Option<u64>,
    pub column: Option<String>,
    pub keyword_path: Vec<String>,
    pub value_path: Vec<TableBrowserValuePathSegment>,
    pub source: Option<String>,
    pub target_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum TableBrowserScalarValue {
    Bool { value: bool },
    Int { value: i64 },
    Uint { value: u64 },
    Float { value: f64 },
    Complex { re: f64, im: f64 },
    String { value: String },
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct TableBrowserArrayElement {
    pub flat_index: u64,
    pub index: Vec<u64>,
    pub value: TableBrowserScalarValue,
    pub selected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserRecordFieldSummary {
    pub name: String,
    pub kind: String,
    pub summary: String,
    pub expandable: bool,
    pub openable: bool,
    pub selected: bool,
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum TableBrowserValueNode {
    Undefined,
    Scalar {
        value: TableBrowserScalarValue,
    },
    Array {
        primitive: String,
        shape: Vec<u64>,
        total_elements: u64,
        page_start: u64,
        page_size: u64,
        elements: Vec<TableBrowserArrayElement>,
    },
    Record {
        total_fields: u64,
        page_start: u64,
        page_size: u64,
        fields: Vec<TableBrowserRecordFieldSummary>,
    },
    TableRef {
        path: String,
        resolved_path: String,
        openable: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserInspectorTrailEntry {
    pub label: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct TableBrowserInspector {
    pub title: String,
    pub trail: Vec<TableBrowserInspectorTrailEntry>,
    pub node: TableBrowserValueNode,
    pub rendered_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct TableBrowserSnapshot {
    pub capabilities: TableBrowserCapabilities,
    pub view: String,
    pub focus: String,
    pub table_path: String,
    pub breadcrumb: Vec<TableBrowserBreadcrumb>,
    pub viewport: TableBrowserViewport,
    pub status_line: String,
    pub content_lines: Vec<String>,
    pub vertical_metrics: Option<TableBrowserNavigationMetrics>,
    pub horizontal_metrics: Option<TableBrowserNavigationMetrics>,
    pub selected_address: Option<TableBrowserSelectedAddress>,
    pub inspector: Option<TableBrowserInspector>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserCellWindowRequest {
    pub dataset_path: String,
    pub row_start: u64,
    pub row_limit: u64,
    pub column_start: u64,
    pub column_limit: u64,
    pub column_options: Vec<TableBrowserColumnDisplayOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserColumnDisplayOption {
    pub column_index: u64,
    pub array_inline_limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserCellValueRequest {
    pub dataset_path: String,
    pub row_index: u64,
    pub column_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserCellWindowSnapshot {
    pub table_path: String,
    pub row_count: u64,
    pub column_count: u64,
    pub row_start: u64,
    pub column_start: u64,
    pub columns: Vec<TableBrowserCellWindowColumn>,
    pub rows: Vec<TableBrowserCellWindowRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserCellWindowColumn {
    pub index: u64,
    pub name: String,
    pub header: String,
    pub summary: String,
    pub width: u64,
    pub keywords: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserCellWindowRow {
    pub index: u64,
    pub cells: Vec<TableBrowserCellWindowCell>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TableBrowserCellWindowCell {
    pub column_index: u64,
    pub display: String,
    pub defined: bool,
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

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterCatalogEnvelope {
    pub schema_version: u64,
    pub catalog: SurfaceParameterCatalog,
    pub surfaces: Vec<SurfaceParameterDefinition>,
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

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceParameterPythonProjection {
    pub name: String,
    pub type_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterProjections {
    pub cli: Option<SurfaceParameterCliProjection>,
    pub provider: Option<SurfaceParameterProviderProjection>,
    pub python: Option<SurfaceParameterPythonProjection>,
    pub presentation: SurfaceParameterPresentation,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SurfaceNarrowingConstraint {
    pub kind: String,
    pub values: Vec<String>,
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

/// One typed context suggestion supplied to canonical edit validation.
#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterEditSuggestion {
    pub label: String,
    pub value: SurfaceParameterValue,
}

/// Complete context for one cross-surface parameter edit.
#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterEditRequest {
    pub surface_id: String,
    pub parameter: String,
    pub text: String,
    pub dataset_path: Option<String>,
    pub spectral_window_id: Option<i32>,
    pub suggestions: Vec<SurfaceParameterEditSuggestion>,
}

/// Typed canonical result rendered by Swift and Python bindings.
#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct SurfaceParameterEditResult {
    pub parameter: String,
    pub normalized_value: Option<SurfaceParameterValue>,
    pub diagnostics: Vec<SurfaceParameterDiagnostic>,
    pub supported_capabilities: Vec<String>,
    pub suggestions: Vec<SurfaceParameterEditSuggestion>,
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
    pub renderer: String,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Enum)]
pub enum NotebookValue {
    String { value: String },
    Number { value: f64 },
    Bool { value: bool },
    Array { values: Vec<NotebookValue> },
    Object { entries: Vec<NotebookValueEntry> },
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct NotebookValueEntry {
    pub name: String,
    pub value: NotebookValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct NotebookTaskIntent {
    pub format: u32,
    pub surface: String,
    pub kind: String,
    pub contract: u32,
    pub parameters: HashMap<String, NotebookValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct NotebookReceiptArtifact {
    pub role: String,
    pub path: String,
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct NotebookPythonEnvironmentIdentity {
    pub environment_id: String,
    pub interpreter: String,
    pub implementation: String,
    pub version: String,
    pub casa_rs_version: Option<String>,
    pub packages: HashMap<String, String>,
    pub fingerprint_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct NotebookPythonExecutionInput {
    pub source: String,
    pub source_sha256: String,
    pub authority: String,
    pub input_references: Vec<String>,
    pub environment: NotebookPythonEnvironmentIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct NotebookExecutionInput {
    pub kind: String,
    pub details: NotebookPythonExecutionInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct NotebookPythonOutputEvent {
    pub order: i64,
    pub channel: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum AssistantAuthorityState {
    Explore,
    Work,
    FullAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantPythonProvenanceState {
    pub selected_command: String,
    pub resolved_path: String,
    pub implementation: String,
    pub version: String,
    pub environment_label: String,
    pub casa_rs_version: Option<String>,
    pub packages: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantSessionProfileState {
    pub profile_version: u32,
    pub backend_id: String,
    pub authority: AssistantAuthorityState,
    pub model: String,
    pub effort: String,
    pub agent_command: String,
    pub python_command: String,
    pub python_provenance: Option<AssistantPythonProvenanceState>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantBackendSessionState {
    pub backend_id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantAttachmentState {
    pub kind: String,
    pub identifier: String,
    pub label: String,
    pub primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantCitationState {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub locator: String,
    pub excerpt: String,
    pub source_path: Option<String>,
    pub page: Option<u32>,
    pub section: Option<String>,
    pub line_start: Option<u32>,
    pub line_end: Option<u32>,
    pub release: Option<String>,
    pub commit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantContextItemState {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub summary: String,
    pub excerpt: String,
    pub byte_count: u64,
    pub content_sha256: String,
    pub untrusted_evidence: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct AssistantContextTabProjection {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub summary: String,
    pub excerpt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct AssistantDataSemanticProjection {
    pub id: String,
    pub label: String,
    pub summary: String,
    pub semantics: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct AssistantNotebookReceiptsProjection {
    pub notebook_id: String,
    pub notebook: String,
    pub receipts: Vec<NotebookExecutionReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct AssistantContextResourcePlanProjection {
    pub schema_version: u32,
    pub corpus_text_units: u64,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct AssistantActionProjection {
    pub id: String,
    pub owner: String,
    pub effect: String,
    pub requires_user_interaction: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct AssistantContextProjectionState {
    pub schema_version: u32,
    pub session_nonce: String,
    pub open_tabs: Vec<AssistantContextTabProjection>,
    pub data_semantics: Vec<AssistantDataSemanticProjection>,
    pub receipts: Vec<AssistantNotebookReceiptsProjection>,
    pub resource_plan: AssistantContextResourcePlanProjection,
    pub action_catalog: Vec<AssistantActionProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantActivityState {
    pub id: String,
    pub label: String,
    pub state: String,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct AssistantTaskSuggestionState {
    pub id: String,
    pub task_id: String,
    pub parameters: HashMap<String, String>,
    pub validated_patch: Option<SurfaceParameterPatch>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantPinState {
    pub id: String,
    pub conversation_id: String,
    pub notebook_id: String,
    pub message_id: String,
    pub representation: String,
    pub destination: String,
    pub snapshot_content: String,
    pub created_at: u64,
    pub content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct AssistantMessageState {
    pub id: String,
    pub role: String,
    pub content: String,
    pub created_at: u64,
    pub agent_id: Option<String>,
    pub model: Option<String>,
    pub citations: Vec<AssistantCitationState>,
    pub used_context: Vec<AssistantContextItemState>,
    pub activities: Vec<AssistantActivityState>,
    pub task_suggestions: Vec<AssistantTaskSuggestionState>,
    pub pins: Vec<AssistantPinState>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct AssistantConversationState {
    pub schema_version: u32,
    pub id: String,
    pub title: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub profile: AssistantSessionProfileState,
    pub backend_session: Option<AssistantBackendSessionState>,
    pub attachments: Vec<AssistantAttachmentState>,
    pub messages: Vec<AssistantMessageState>,
    pub draft: String,
    pub selected_context_ids: Vec<String>,
    pub scroll_anchor_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantCreateConversationRequest {
    pub project_root: String,
    pub title: String,
    pub primary_attachment: AssistantAttachmentState,
    pub profile: AssistantSessionProfileState,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantConversationRequest {
    pub project_root: String,
    pub conversation_id: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct AssistantSaveConversationRequest {
    pub project_root: String,
    pub transcript: AssistantConversationState,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantCreatePinRequest {
    pub conversation_id: String,
    pub notebook_id: String,
    pub message_id: String,
    pub representation: String,
    pub snapshot_content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct AssistantCorpusCitationRequest {
    pub label: String,
    pub locator: String,
    pub source_path: Option<String>,
    pub page: Option<u32>,
    pub section: Option<String>,
    pub line_start: Option<u32>,
    pub line_end: Option<u32>,
    pub release: Option<String>,
    pub commit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantCorpusDocumentRequest {
    pub id: String,
    pub layer: String,
    pub title: String,
    pub source_identity: String,
    pub content: String,
    pub citation: AssistantCorpusCitationRequest,
    pub redistribution_cleared: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantProjectCorpusSourceRequest {
    pub relative_path: String,
    pub file_type: String,
    pub size_bytes: u64,
    pub modified_unix_ns: i64,
    pub status_changed_unix_ns: i64,
    pub file_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantCorpusIndexRequest {
    pub project_root: String,
    pub documents: Vec<AssistantCorpusDocumentRequest>,
    pub remove_missing_layers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantCorpusIndexReportState {
    pub schema_version: u32,
    pub retrieval_engine: String,
    pub indexed_documents: u64,
    pub unchanged_documents: u64,
    pub removed_documents: u64,
    pub chunk_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantPrepareCorpusReconciliationRequest {
    pub project_root: String,
    pub sources: Vec<AssistantProjectCorpusSourceRequest>,
    pub generation: u64,
    pub scope: AssistantCorpusReconciliationScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum AssistantCorpusReconciliationScope {
    AllLayers,
    ProjectDocuments,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantPreparedCorpusReconciliationState {
    pub schema_version: u32,
    pub generation: u64,
    pub scope: AssistantCorpusReconciliationScope,
    pub snapshot_digest: String,
    pub sources: Vec<AssistantProjectCorpusSourceRequest>,
    pub extract_paths: Vec<String>,
    pub unchanged_paths: Vec<String>,
    pub removed_paths: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum AssistantProjectSourceExtractionStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantProjectSourceExtractionOutcome {
    pub relative_path: String,
    pub status: AssistantProjectSourceExtractionStatus,
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantApplyCorpusReconciliationRequest {
    pub project_root: String,
    pub prepared: AssistantPreparedCorpusReconciliationState,
    pub documents: Vec<AssistantCorpusDocumentRequest>,
    pub remove_missing_layers: Vec<String>,
    pub outcomes: Vec<AssistantProjectSourceExtractionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantCorpusSearchRequest {
    pub project_root: String,
    pub query: String,
    pub limit: u64,
    pub layers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, uniffi::Record)]
pub struct AssistantCorpusSearchHitState {
    pub chunk_id: String,
    pub document_id: String,
    pub layer: String,
    pub title: String,
    pub text: String,
    pub score: f32,
    pub citation: AssistantCorpusCitationRequest,
    pub untrusted_evidence: bool,
}

// Protocol-plane bounds keep one MCP request/result finite. They do not cap
// indexed documents, source size, scientific data, or downloads.
const MAX_ASSISTANT_CORPUS_QUERY_BYTES: usize = 4_096;
const MAX_ASSISTANT_CORPUS_SEARCH_RESULTS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct AssistantProtocolProjection {
    pub profile_version: u32,
    pub transcript_schema_version: u32,
    pub corpus_schema_version: u32,
    pub retrieval_engine: String,
    pub backend_session_binding: String,
    pub authority_presets: Vec<String>,
    pub project_mcp_tools: Vec<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum TutorialAcquisitionPhase {
    Missing,
    Downloading,
    Verifying,
    Unpacking,
    Checking,
    Materializing,
    Ready,
    Cancelled,
    NetworkFailed,
    ChecksumFailed,
    UnsafeArchive,
    DestinationCollision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum TutorialPersistenceAction {
    Resume,
    Restart,
    Retry,
    Cancel,
    Advance,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialSectionState {
    pub id: String,
    pub title: String,
    pub dataset_ids: Vec<String>,
    pub cell_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialOptionalCheckState {
    pub id: String,
    pub label: String,
    pub kind: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialUnpackState {
    pub format: String,
    pub archive_root: Option<String>,
    pub max_entries: u64,
    pub max_expanded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialCheckOutcomeState {
    pub check_id: String,
    pub status: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialDatasetAttemptState {
    pub generation: u64,
    pub kind: String,
    pub phase: TutorialAcquisitionPhase,
    pub requested_uri: String,
    pub resolved_uri: String,
    pub redirects: Vec<String>,
    pub expected_size_bytes: Option<u64>,
    pub expected_sha256: Option<String>,
    pub approval_sha256: String,
    pub approved_missing_digest: bool,
    pub skipped_check_ids: Vec<String>,
    pub downloaded_bytes: u64,
    pub computed_sha256: Option<String>,
    pub checks: Vec<TutorialCheckOutcomeState>,
    pub error: Option<String>,
    pub started_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialDatasetState {
    pub id: String,
    pub display_name: String,
    pub uri: String,
    pub destination: String,
    pub expected_size_bytes: Option<u64>,
    pub sha256: Option<String>,
    pub unpack: Option<TutorialUnpackState>,
    pub checks: Vec<TutorialOptionalCheckState>,
    pub phase: TutorialAcquisitionPhase,
    pub staged: bool,
    pub current_generation: u64,
    pub pinned_sha256: Option<String>,
    pub attempts: Vec<TutorialDatasetAttemptState>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialLockState {
    pub schema_version: u32,
    pub registry_version: u32,
    pub notebook_id: String,
    pub notebook_filename: String,
    pub tutorial_id: String,
    pub title: String,
    pub template_sha256: String,
    pub sections: Vec<TutorialSectionState>,
    pub datasets: Vec<TutorialDatasetState>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct TutorialProjectProjection {
    pub notebook: NotebookDocumentProjection,
    pub tutorial: TutorialLockState,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialAcquisitionPlanState {
    pub approval_sha256: String,
    pub registry_version: u32,
    pub notebook_id: String,
    pub dataset_id: String,
    pub scheme: String,
    pub requested_uri: String,
    pub resolved_uri: String,
    pub redirects: Vec<String>,
    pub expected_size_bytes: Option<u64>,
    pub resolved_size_bytes: Option<u64>,
    pub destination: String,
    pub expected_sha256: Option<String>,
    pub required_disk_bytes: u64,
    pub available_disk_bytes: u64,
    pub unpack: Option<TutorialUnpackState>,
    pub checks: Vec<TutorialOptionalCheckState>,
    pub missing_digest: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialAcquisitionApprovalState {
    pub approval_sha256: String,
    pub allow_missing_digest: bool,
    pub skipped_check_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialTemplateState {
    pub root: String,
    pub content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialForkRequest {
    pub project_root: String,
    pub template_path: String,
    pub filename: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialMigrateRequest {
    pub pack_path: String,
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialPlanRequest {
    pub project_root: String,
    pub notebook_id: String,
    pub dataset_id: String,
    pub source_override: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialBeginRequest {
    pub project_root: String,
    pub plan: TutorialAcquisitionPlanState,
    pub approval: TutorialAcquisitionApprovalState,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TutorialActionRequest {
    pub action: TutorialPersistenceAction,
    pub project_root: String,
    pub notebook_id: String,
    pub dataset_id: String,
    pub generation: Option<u64>,
    pub max_download_bytes: Option<u64>,
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

fn tutorial_notebook_id(value: &str) -> FrontendResult<NotebookId> {
    value
        .parse()
        .map_err(|error| tutorial_error("parse tutorial notebook ID", error))
}

fn tutorial_phase_projection(
    phase: casa_notebook::TutorialAcquisitionPhase,
) -> TutorialAcquisitionPhase {
    match phase {
        casa_notebook::TutorialAcquisitionPhase::Missing => TutorialAcquisitionPhase::Missing,
        casa_notebook::TutorialAcquisitionPhase::Downloading => {
            TutorialAcquisitionPhase::Downloading
        }
        casa_notebook::TutorialAcquisitionPhase::Verifying => TutorialAcquisitionPhase::Verifying,
        casa_notebook::TutorialAcquisitionPhase::Unpacking => TutorialAcquisitionPhase::Unpacking,
        casa_notebook::TutorialAcquisitionPhase::Checking => TutorialAcquisitionPhase::Checking,
        casa_notebook::TutorialAcquisitionPhase::Materializing => {
            TutorialAcquisitionPhase::Materializing
        }
        casa_notebook::TutorialAcquisitionPhase::Ready => TutorialAcquisitionPhase::Ready,
        casa_notebook::TutorialAcquisitionPhase::Cancelled => TutorialAcquisitionPhase::Cancelled,
        casa_notebook::TutorialAcquisitionPhase::NetworkFailed => {
            TutorialAcquisitionPhase::NetworkFailed
        }
        casa_notebook::TutorialAcquisitionPhase::ChecksumFailed => {
            TutorialAcquisitionPhase::ChecksumFailed
        }
        casa_notebook::TutorialAcquisitionPhase::UnsafeArchive => {
            TutorialAcquisitionPhase::UnsafeArchive
        }
        casa_notebook::TutorialAcquisitionPhase::DestinationCollision => {
            TutorialAcquisitionPhase::DestinationCollision
        }
    }
}

fn tutorial_unpack_projection(value: casa_notebook::TutorialUnpackPlan) -> TutorialUnpackState {
    let format = match value.format {
        casa_notebook::TutorialArchiveFormat::Tar => "tar",
        casa_notebook::TutorialArchiveFormat::TarGz => "tar_gz",
    };
    TutorialUnpackState {
        format: format.to_owned(),
        archive_root: value.archive_root.map(|path| path.display().to_string()),
        max_entries: value.max_entries,
        max_expanded_bytes: value.max_expanded_bytes,
    }
}

fn tutorial_unpack_owner(
    value: TutorialUnpackState,
) -> FrontendResult<casa_notebook::TutorialUnpackPlan> {
    let format = match value.format.as_str() {
        "tar" => casa_notebook::TutorialArchiveFormat::Tar,
        "tar_gz" => casa_notebook::TutorialArchiveFormat::TarGz,
        other => {
            return Err(tutorial_error(
                "parse tutorial archive format",
                format!("unsupported format `{other}`"),
            ));
        }
    };
    Ok(casa_notebook::TutorialUnpackPlan {
        format,
        archive_root: value.archive_root.map(PathBuf::from),
        max_entries: value.max_entries,
        max_expanded_bytes: value.max_expanded_bytes,
    })
}

fn tutorial_check_kind_projection(kind: casa_notebook::TutorialCheckKind) -> String {
    match kind {
        casa_notebook::TutorialCheckKind::PathExists => "path_exists",
        casa_notebook::TutorialCheckKind::RegularFile => "regular_file",
        casa_notebook::TutorialCheckKind::Directory => "directory",
        casa_notebook::TutorialCheckKind::MeasurementSet => "measurement_set",
    }
    .to_owned()
}

fn tutorial_check_kind_owner(value: &str) -> FrontendResult<casa_notebook::TutorialCheckKind> {
    match value {
        "path_exists" => Ok(casa_notebook::TutorialCheckKind::PathExists),
        "regular_file" => Ok(casa_notebook::TutorialCheckKind::RegularFile),
        "directory" => Ok(casa_notebook::TutorialCheckKind::Directory),
        "measurement_set" => Ok(casa_notebook::TutorialCheckKind::MeasurementSet),
        other => Err(tutorial_error(
            "parse tutorial check kind",
            format!("unsupported kind `{other}`"),
        )),
    }
}

fn tutorial_check_projection(
    value: casa_notebook::TutorialOptionalCheck,
) -> TutorialOptionalCheckState {
    TutorialOptionalCheckState {
        id: value.id,
        label: value.label,
        kind: tutorial_check_kind_projection(value.kind),
        path: value.path.display().to_string(),
    }
}

fn tutorial_check_owner(
    value: TutorialOptionalCheckState,
) -> FrontendResult<casa_notebook::TutorialOptionalCheck> {
    Ok(casa_notebook::TutorialOptionalCheck {
        id: value.id,
        label: value.label,
        kind: tutorial_check_kind_owner(&value.kind)?,
        path: PathBuf::from(value.path),
    })
}

fn tutorial_attempt_projection(
    value: casa_notebook::TutorialDatasetAttempt,
) -> TutorialDatasetAttemptState {
    let kind = match value.kind {
        casa_notebook::TutorialAttemptKind::Initial => "initial",
        casa_notebook::TutorialAttemptKind::Resume => "resume",
        casa_notebook::TutorialAttemptKind::Restart => "restart",
        casa_notebook::TutorialAttemptKind::Retry => "retry",
    };
    TutorialDatasetAttemptState {
        generation: value.generation,
        kind: kind.to_owned(),
        phase: tutorial_phase_projection(value.phase),
        requested_uri: value.requested_uri,
        resolved_uri: value.resolved_uri,
        redirects: value.redirects,
        expected_size_bytes: value.expected_size_bytes,
        expected_sha256: value.expected_sha256,
        approval_sha256: value.approval_sha256,
        approved_missing_digest: value.approved_missing_digest,
        skipped_check_ids: value.skipped_check_ids,
        downloaded_bytes: value.downloaded_bytes,
        computed_sha256: value.computed_sha256,
        checks: value
            .checks
            .into_iter()
            .map(|check| TutorialCheckOutcomeState {
                check_id: check.check_id,
                status: match check.status {
                    casa_notebook::TutorialCheckStatus::Passed => "passed",
                    casa_notebook::TutorialCheckStatus::Failed => "failed",
                    casa_notebook::TutorialCheckStatus::Skipped => "skipped",
                }
                .to_owned(),
                detail: check.detail,
            })
            .collect(),
        error: value.error,
        started_at: value.started_at.0,
        finished_at: value.finished_at.map(|timestamp| timestamp.0),
    }
}

fn tutorial_dataset_projection(value: casa_notebook::TutorialDatasetLock) -> TutorialDatasetState {
    TutorialDatasetState {
        id: value.dataset.id,
        display_name: value.dataset.display_name,
        uri: value.dataset.uri,
        destination: value.dataset.destination.display().to_string(),
        expected_size_bytes: value.dataset.expected_size_bytes,
        sha256: value.dataset.sha256,
        unpack: value.dataset.unpack.map(tutorial_unpack_projection),
        checks: value
            .dataset
            .checks
            .into_iter()
            .map(tutorial_check_projection)
            .collect(),
        phase: tutorial_phase_projection(value.phase),
        staged: value.staged,
        current_generation: value.current_generation,
        pinned_sha256: value.pinned_sha256,
        attempts: value
            .attempts
            .into_iter()
            .map(tutorial_attempt_projection)
            .collect(),
    }
}

fn tutorial_lock_projection(value: casa_notebook::TutorialLock) -> TutorialLockState {
    TutorialLockState {
        schema_version: value.schema_version,
        registry_version: value.registry_version,
        notebook_id: value.notebook_id.to_string(),
        notebook_filename: value.notebook_filename,
        tutorial_id: value.tutorial_id,
        title: value.title,
        template_sha256: value.template_sha256,
        sections: value
            .sections
            .into_iter()
            .map(|section| TutorialSectionState {
                id: section.id,
                title: section.title,
                dataset_ids: section.dataset_ids,
                cell_ids: section
                    .cell_ids
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect(),
            })
            .collect(),
        datasets: value
            .datasets
            .into_iter()
            .map(tutorial_dataset_projection)
            .collect(),
    }
}

fn tutorial_plan_projection(
    value: casa_notebook::TutorialAcquisitionPlan,
) -> TutorialAcquisitionPlanState {
    TutorialAcquisitionPlanState {
        approval_sha256: value.approval_sha256,
        registry_version: value.registry_version,
        notebook_id: value.notebook_id.to_string(),
        dataset_id: value.dataset_id,
        scheme: value.scheme,
        requested_uri: value.requested_uri,
        resolved_uri: value.resolved_uri,
        redirects: value.redirects,
        expected_size_bytes: value.expected_size_bytes,
        resolved_size_bytes: value.resolved_size_bytes,
        destination: value.destination.display().to_string(),
        expected_sha256: value.expected_sha256,
        required_disk_bytes: value.required_disk_bytes,
        available_disk_bytes: value.available_disk_bytes,
        unpack: value.unpack.map(tutorial_unpack_projection),
        checks: value
            .checks
            .into_iter()
            .map(tutorial_check_projection)
            .collect(),
        missing_digest: value.missing_digest,
    }
}

fn tutorial_plan_owner(
    value: TutorialAcquisitionPlanState,
) -> FrontendResult<casa_notebook::TutorialAcquisitionPlan> {
    Ok(casa_notebook::TutorialAcquisitionPlan {
        approval_sha256: value.approval_sha256,
        registry_version: value.registry_version,
        notebook_id: tutorial_notebook_id(&value.notebook_id)?,
        dataset_id: value.dataset_id,
        scheme: value.scheme,
        requested_uri: value.requested_uri,
        resolved_uri: value.resolved_uri,
        redirects: value.redirects,
        expected_size_bytes: value.expected_size_bytes,
        resolved_size_bytes: value.resolved_size_bytes,
        destination: PathBuf::from(value.destination),
        expected_sha256: value.expected_sha256,
        required_disk_bytes: value.required_disk_bytes,
        available_disk_bytes: value.available_disk_bytes,
        unpack: value.unpack.map(tutorial_unpack_owner).transpose()?,
        checks: value
            .checks
            .into_iter()
            .map(tutorial_check_owner)
            .collect::<FrontendResult<Vec<_>>>()?,
        missing_digest: value.missing_digest,
    })
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
pub fn assistant_protocol_info() -> AssistantProtocolProjection {
    AssistantProtocolProjection {
        profile_version: ASSISTANT_PROFILE_VERSION,
        transcript_schema_version: ASSISTANT_TRANSCRIPT_SCHEMA_VERSION,
        corpus_schema_version: CORPUS_SCHEMA_VERSION,
        retrieval_engine: "sqlite_fts5_unicode61".to_string(),
        backend_session_binding: "opaque_adapter_session_id".to_string(),
        authority_presets: vec!["explore", "work", "full_access"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        project_mcp_tools: vec![
            "corpus.search",
            "source.search",
            "context.open_tabs",
            "task.schema",
            "data.describe",
            "web.fetch",
            "web.search",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    }
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

fn assistant_authority_owner(
    authority: AssistantAuthorityState,
) -> casa_notebook::AssistantAuthorityPreset {
    match authority {
        AssistantAuthorityState::Explore => casa_notebook::AssistantAuthorityPreset::Explore,
        AssistantAuthorityState::Work => casa_notebook::AssistantAuthorityPreset::Work,
        AssistantAuthorityState::FullAccess => casa_notebook::AssistantAuthorityPreset::FullAccess,
    }
}

fn assistant_authority_projection(
    authority: casa_notebook::AssistantAuthorityPreset,
) -> AssistantAuthorityState {
    match authority {
        casa_notebook::AssistantAuthorityPreset::Explore => AssistantAuthorityState::Explore,
        casa_notebook::AssistantAuthorityPreset::Work => AssistantAuthorityState::Work,
        casa_notebook::AssistantAuthorityPreset::FullAccess => AssistantAuthorityState::FullAccess,
    }
}

fn assistant_python_owner(value: AssistantPythonProvenanceState) -> AssistantPythonProvenance {
    AssistantPythonProvenance {
        selected_command: value.selected_command,
        resolved_path: PathBuf::from(value.resolved_path),
        implementation: value.implementation,
        version: value.version,
        environment_label: value.environment_label,
        casa_rs_version: value.casa_rs_version,
        packages: value.packages.into_iter().collect(),
    }
}

fn assistant_python_projection(value: AssistantPythonProvenance) -> AssistantPythonProvenanceState {
    AssistantPythonProvenanceState {
        selected_command: value.selected_command,
        resolved_path: value.resolved_path.display().to_string(),
        implementation: value.implementation,
        version: value.version,
        environment_label: value.environment_label,
        casa_rs_version: value.casa_rs_version,
        packages: value.packages.into_iter().collect(),
    }
}

fn assistant_profile_owner(value: AssistantSessionProfileState) -> AssistantSessionProfile {
    AssistantSessionProfile {
        profile_version: value.profile_version,
        backend_id: value.backend_id,
        authority: assistant_authority_owner(value.authority),
        model: value.model,
        effort: value.effort,
        agent_command: value.agent_command,
        python_command: value.python_command,
        python_provenance: value.python_provenance.map(assistant_python_owner),
    }
}

fn assistant_profile_projection(value: AssistantSessionProfile) -> AssistantSessionProfileState {
    AssistantSessionProfileState {
        profile_version: value.profile_version,
        backend_id: value.backend_id,
        authority: assistant_authority_projection(value.authority),
        model: value.model,
        effort: value.effort,
        agent_command: value.agent_command,
        python_command: value.python_command,
        python_provenance: value.python_provenance.map(assistant_python_projection),
    }
}

fn assistant_attachment_owner(
    value: AssistantAttachmentState,
) -> FrontendResult<AssistantAttachment> {
    Ok(AssistantAttachment {
        kind: serde_json::from_value(serde_json::Value::String(value.kind))
            .map_err(|error| assistant_error("parse assistant attachment kind", error))?,
        identifier: value.identifier,
        label: value.label,
        primary: value.primary,
    })
}

fn assistant_attachment_projection(value: AssistantAttachment) -> AssistantAttachmentState {
    AssistantAttachmentState {
        kind: snake_case_debug(value.kind),
        identifier: value.identifier,
        label: value.label,
        primary: value.primary,
    }
}

fn assistant_citation_owner(value: AssistantCitationState) -> FrontendResult<AssistantCitation> {
    Ok(AssistantCitation {
        id: value.id,
        kind: serde_json::from_value(serde_json::Value::String(value.kind))
            .map_err(|error| assistant_error("parse assistant citation kind", error))?,
        label: value.label,
        locator: value.locator,
        excerpt: value.excerpt,
        source_path: value.source_path.map(PathBuf::from),
        page: value.page,
        section: value.section,
        line_start: value.line_start,
        line_end: value.line_end,
        release: value.release,
        commit: value.commit,
    })
}

fn assistant_citation_projection(value: AssistantCitation) -> AssistantCitationState {
    AssistantCitationState {
        id: value.id,
        kind: snake_case_debug(value.kind),
        label: value.label,
        locator: value.locator,
        excerpt: value.excerpt,
        source_path: value.source_path.map(|path| path.display().to_string()),
        page: value.page,
        section: value.section,
        line_start: value.line_start,
        line_end: value.line_end,
        release: value.release,
        commit: value.commit,
    }
}

fn assistant_context_owner(
    value: AssistantContextItemState,
) -> FrontendResult<AssistantContextItem> {
    Ok(AssistantContextItem {
        id: value.id,
        kind: serde_json::from_value(serde_json::Value::String(value.kind))
            .map_err(|error| assistant_error("parse assistant context kind", error))?,
        label: value.label,
        summary: value.summary,
        excerpt: value.excerpt,
        byte_count: value.byte_count,
        content_sha256: value.content_sha256,
        untrusted_evidence: value.untrusted_evidence,
    })
}

fn assistant_context_projection(value: AssistantContextItem) -> AssistantContextItemState {
    AssistantContextItemState {
        id: value.id,
        kind: snake_case_debug(value.kind),
        label: value.label,
        summary: value.summary,
        excerpt: value.excerpt,
        byte_count: value.byte_count,
        content_sha256: value.content_sha256,
        untrusted_evidence: value.untrusted_evidence,
    }
}

fn assistant_activity_owner(value: AssistantActivityState) -> FrontendResult<AssistantActivity> {
    Ok(AssistantActivity {
        id: value.id,
        label: value.label,
        state: serde_json::from_value(serde_json::Value::String(value.state))
            .map_err(|error| assistant_error("parse assistant activity state", error))?,
        summary: value.summary,
    })
}

fn assistant_activity_projection(value: AssistantActivity) -> AssistantActivityState {
    AssistantActivityState {
        id: value.id,
        label: value.label,
        state: snake_case_debug(value.state),
        summary: value.summary,
    }
}

fn assistant_suggestion_owner(value: AssistantTaskSuggestionState) -> AssistantTaskSuggestion {
    AssistantTaskSuggestion {
        id: value.id,
        task_id: value.task_id,
        parameters: value.parameters.into_iter().collect(),
    }
}

fn assistant_suggestion_projection(value: AssistantTaskSuggestion) -> AssistantTaskSuggestionState {
    AssistantTaskSuggestionState {
        id: value.id,
        task_id: value.task_id,
        parameters: value.parameters.into_iter().collect(),
        validated_patch: None,
    }
}

fn assistant_pin_owner(value: AssistantPinState) -> FrontendResult<AssistantPinReference> {
    Ok(AssistantPinReference {
        id: value
            .id
            .parse()
            .map_err(|error| assistant_error("parse assistant pin ID", error))?,
        conversation_id: value
            .conversation_id
            .parse()
            .map_err(|error| assistant_error("parse assistant pin conversation ID", error))?,
        notebook_id: value
            .notebook_id
            .parse()
            .map_err(|error| assistant_error("parse assistant pin notebook ID", error))?,
        message_id: value
            .message_id
            .parse()
            .map_err(|error| assistant_error("parse assistant pin message ID", error))?,
        representation: value.representation,
        destination: value.destination,
        snapshot_content: value.snapshot_content,
        created_at: Timestamp(value.created_at),
        content_sha256: value.content_sha256,
    })
}

fn assistant_pin_projection(value: AssistantPinReference) -> AssistantPinState {
    AssistantPinState {
        id: value.id.to_string(),
        conversation_id: value.conversation_id.to_string(),
        notebook_id: value.notebook_id.to_string(),
        message_id: value.message_id.to_string(),
        representation: value.representation,
        destination: value.destination,
        snapshot_content: value.snapshot_content,
        created_at: value.created_at.0,
        content_sha256: value.content_sha256,
    }
}

fn assistant_message_owner(
    value: AssistantMessageState,
) -> FrontendResult<casa_notebook::AssistantMessage> {
    Ok(casa_notebook::AssistantMessage {
        id: value
            .id
            .parse()
            .map_err(|error| assistant_error("parse assistant message ID", error))?,
        role: serde_json::from_value(serde_json::Value::String(value.role))
            .map_err(|error| assistant_error("parse assistant message role", error))?,
        content: value.content,
        created_at: Timestamp(value.created_at),
        agent_id: value.agent_id,
        model: value.model,
        citations: value
            .citations
            .into_iter()
            .map(assistant_citation_owner)
            .collect::<FrontendResult<Vec<_>>>()?,
        used_context: value
            .used_context
            .into_iter()
            .map(assistant_context_owner)
            .collect::<FrontendResult<Vec<_>>>()?,
        activities: value
            .activities
            .into_iter()
            .map(assistant_activity_owner)
            .collect::<FrontendResult<Vec<_>>>()?,
        task_suggestions: value
            .task_suggestions
            .into_iter()
            .map(assistant_suggestion_owner)
            .collect(),
        pins: value
            .pins
            .into_iter()
            .map(assistant_pin_owner)
            .collect::<FrontendResult<Vec<_>>>()?,
    })
}

fn assistant_message_projection(value: casa_notebook::AssistantMessage) -> AssistantMessageState {
    AssistantMessageState {
        id: value.id.to_string(),
        role: snake_case_debug(value.role),
        content: value.content,
        created_at: value.created_at.0,
        agent_id: value.agent_id,
        model: value.model,
        citations: value
            .citations
            .into_iter()
            .map(assistant_citation_projection)
            .collect(),
        used_context: value
            .used_context
            .into_iter()
            .map(assistant_context_projection)
            .collect(),
        activities: value
            .activities
            .into_iter()
            .map(assistant_activity_projection)
            .collect(),
        task_suggestions: value
            .task_suggestions
            .into_iter()
            .map(assistant_suggestion_projection)
            .collect(),
        pins: value
            .pins
            .into_iter()
            .map(assistant_pin_projection)
            .collect(),
    }
}

fn assistant_transcript_owner(
    value: AssistantConversationState,
) -> FrontendResult<ConversationTranscript> {
    Ok(ConversationTranscript {
        schema_version: value.schema_version,
        id: value
            .id
            .parse()
            .map_err(|error| assistant_error("parse assistant conversation ID", error))?,
        title: value.title,
        created_at: Timestamp(value.created_at),
        updated_at: Timestamp(value.updated_at),
        profile: assistant_profile_owner(value.profile),
        backend_session: value.backend_session.map(|session| {
            casa_notebook::AssistantBackendSession {
                backend_id: session.backend_id,
                session_id: session.session_id,
            }
        }),
        attachments: value
            .attachments
            .into_iter()
            .map(assistant_attachment_owner)
            .collect::<FrontendResult<Vec<_>>>()?,
        messages: value
            .messages
            .into_iter()
            .map(assistant_message_owner)
            .collect::<FrontendResult<Vec<_>>>()?,
        draft: value.draft,
        selected_context_ids: value.selected_context_ids,
        scroll_anchor_message_id: value
            .scroll_anchor_message_id
            .map(|id| id.parse())
            .transpose()
            .map_err(|error| assistant_error("parse assistant scroll anchor", error))?,
    })
}

fn assistant_transcript_projection(value: ConversationTranscript) -> AssistantConversationState {
    AssistantConversationState {
        schema_version: value.schema_version,
        id: value.id.to_string(),
        title: value.title,
        created_at: value.created_at.0,
        updated_at: value.updated_at.0,
        profile: assistant_profile_projection(value.profile),
        backend_session: value
            .backend_session
            .map(|session| AssistantBackendSessionState {
                backend_id: session.backend_id,
                session_id: session.session_id,
            }),
        attachments: value
            .attachments
            .into_iter()
            .map(assistant_attachment_projection)
            .collect(),
        messages: value
            .messages
            .into_iter()
            .map(assistant_message_projection)
            .collect(),
        draft: value.draft,
        selected_context_ids: value.selected_context_ids,
        scroll_anchor_message_id: value.scroll_anchor_message_id.map(|id| id.to_string()),
    }
}

fn assistant_corpus_layer(value: &str) -> FrontendResult<CorpusLayer> {
    match value {
        "baseline" => Ok(CorpusLayer::Baseline),
        "project_document" => Ok(CorpusLayer::ProjectDocument),
        "release_source" => Ok(CorpusLayer::ReleaseSource),
        "live_source" => Ok(CorpusLayer::LiveSource),
        other => Err(FrontendServiceError::Corpus {
            reason: format!("unsupported corpus layer {other}"),
        }),
    }
}

fn assistant_corpus_citation_owner(
    value: AssistantCorpusCitationRequest,
) -> casa_notebook::CorpusCitation {
    casa_notebook::CorpusCitation {
        label: value.label,
        locator: value.locator,
        source_path: value.source_path.map(PathBuf::from),
        page: value.page,
        section: value.section,
        line_start: value.line_start,
        line_end: value.line_end,
        release: value.release,
        commit: value.commit,
    }
}

fn assistant_corpus_citation_projection(
    value: casa_notebook::CorpusCitation,
) -> AssistantCorpusCitationRequest {
    AssistantCorpusCitationRequest {
        label: value.label,
        locator: value.locator,
        source_path: value.source_path.map(|path| path.display().to_string()),
        page: value.page,
        section: value.section,
        line_start: value.line_start,
        line_end: value.line_end,
        release: value.release,
        commit: value.commit,
    }
}

fn assistant_project_source_owner(
    value: AssistantProjectCorpusSourceRequest,
) -> ProjectCorpusSource {
    ProjectCorpusSource {
        relative_path: PathBuf::from(value.relative_path),
        file_type: value.file_type,
        size_bytes: value.size_bytes,
        modified_unix_ns: value.modified_unix_ns,
        status_changed_unix_ns: value.status_changed_unix_ns,
        file_identity: value.file_identity,
    }
}

fn assistant_project_source_projection(
    value: ProjectCorpusSource,
) -> AssistantProjectCorpusSourceRequest {
    AssistantProjectCorpusSourceRequest {
        relative_path: value.relative_path.display().to_string(),
        file_type: value.file_type,
        size_bytes: value.size_bytes,
        modified_unix_ns: value.modified_unix_ns,
        status_changed_unix_ns: value.status_changed_unix_ns,
        file_identity: value.file_identity,
    }
}

fn assistant_corpus_documents_owner(
    documents: Vec<AssistantCorpusDocumentRequest>,
) -> FrontendResult<Vec<CorpusDocumentInput>> {
    documents
        .into_iter()
        .map(|document| {
            Ok(CorpusDocumentInput {
                id: document.id,
                layer: assistant_corpus_layer(&document.layer)?,
                title: document.title,
                source_identity: document.source_identity,
                content: document.content,
                citation: assistant_corpus_citation_owner(document.citation),
                redistribution_cleared: document.redistribution_cleared,
            })
        })
        .collect()
}

fn assistant_corpus_report_projection(
    report: casa_notebook::CorpusIndexReport,
) -> AssistantCorpusIndexReportState {
    AssistantCorpusIndexReportState {
        schema_version: report.schema_version,
        retrieval_engine: report.retrieval_engine,
        indexed_documents: report.indexed_documents as u64,
        unchanged_documents: report.unchanged_documents as u64,
        removed_documents: report.removed_documents as u64,
        chunk_count: report.chunk_count as u64,
    }
}

/// Construct one immutable notebook pin snapshot with transcript provenance.
#[uniffi::export]
pub fn assistant_create_pin(
    request: AssistantCreatePinRequest,
) -> FrontendResult<AssistantPinState> {
    let pin = AssistantPinReference::new(
        request
            .conversation_id
            .parse()
            .map_err(|error| assistant_error("parse assistant pin conversation ID", error))?,
        request
            .notebook_id
            .parse()
            .map_err(|error| assistant_error("parse assistant pin notebook ID", error))?,
        request
            .message_id
            .parse()
            .map_err(|error| assistant_error("parse assistant pin message ID", error))?,
        request.representation,
        request.snapshot_content,
    );
    Ok(assistant_pin_projection(pin))
}

/// List the provider-neutral visible conversations persisted for one project.
#[uniffi::export]
pub fn assistant_conversations(
    project_root: String,
) -> FrontendResult<Vec<AssistantConversationState>> {
    let store = AssistantStore::open(&project_root)
        .map_err(|error| assistant_error("open assistant project", error))?;
    let conversations = store
        .list_conversations()
        .map_err(|error| assistant_error("list assistant conversations", error))?;
    Ok(conversations
        .into_iter()
        .map(assistant_transcript_projection)
        .collect())
}

/// Create one persistent conversation attached primarily to a task or notebook.
#[uniffi::export]
pub fn assistant_create_conversation(
    request: AssistantCreateConversationRequest,
) -> FrontendResult<AssistantConversationState> {
    let store = AssistantStore::open(&request.project_root)
        .map_err(|error| assistant_error("open assistant project", error))?;
    let conversation = store
        .create_conversation(
            request.title,
            assistant_attachment_owner(request.primary_attachment)?,
            assistant_profile_owner(request.profile),
        )
        .map_err(|error| assistant_error("create assistant conversation", error))?;
    Ok(assistant_transcript_projection(conversation))
}

/// Load one persistent visible transcript without provider-specific envelopes.
#[uniffi::export]
pub fn assistant_load_conversation(
    request: AssistantConversationRequest,
) -> FrontendResult<AssistantConversationState> {
    let store = AssistantStore::open(&request.project_root)
        .map_err(|error| assistant_error("open assistant project", error))?;
    let conversation = store
        .load_conversation(
            request
                .conversation_id
                .parse()
                .map_err(|error| assistant_error("parse assistant conversation ID", error))?,
        )
        .map_err(|error| assistant_error("load assistant conversation", error))?;
    Ok(assistant_transcript_projection(conversation))
}

/// Atomically save one provider-neutral visible transcript.
#[uniffi::export]
pub fn assistant_save_conversation(
    request: AssistantSaveConversationRequest,
) -> FrontendResult<()> {
    let store = AssistantStore::open(&request.project_root)
        .map_err(|error| assistant_error("open assistant project", error))?;
    store
        .save_conversation(&assistant_transcript_owner(request.transcript)?)
        .map_err(|error| assistant_error("save assistant conversation", error))
}

/// Incrementally index trusted host-supplied corpus documents.
#[uniffi::export]
pub fn assistant_corpus_index(
    request: AssistantCorpusIndexRequest,
) -> FrontendResult<AssistantCorpusIndexReportState> {
    let documents = assistant_corpus_documents_owner(request.documents)?;
    let remove_missing_layers = request
        .remove_missing_layers
        .iter()
        .map(|layer| assistant_corpus_layer(layer))
        .collect::<FrontendResult<BTreeSet<_>>>()?;
    let index = CorpusIndex::open(&request.project_root)
        .map_err(|error| corpus_error("open corpus index", error))?;
    let report = index
        .index_documents(&documents, &remove_missing_layers)
        .map_err(|error| corpus_error("index corpus documents", error))?;
    Ok(assistant_corpus_report_projection(report))
}

/// Prepare one project-document reconciliation against an exact snapshot.
#[uniffi::export]
pub fn assistant_prepare_corpus_reconciliation(
    request: AssistantPrepareCorpusReconciliationRequest,
) -> FrontendResult<AssistantPreparedCorpusReconciliationState> {
    let index = CorpusIndex::open(&request.project_root)
        .map_err(|error| corpus_error("open corpus index", error))?;
    let prepared = index
        .prepare_reconciliation(
            &request
                .sources
                .into_iter()
                .map(assistant_project_source_owner)
                .collect::<Vec<_>>(),
            request.generation,
            match request.scope {
                AssistantCorpusReconciliationScope::AllLayers => {
                    CorpusReconciliationScope::AllLayers
                }
                AssistantCorpusReconciliationScope::ProjectDocuments => {
                    CorpusReconciliationScope::ProjectDocuments
                }
            },
        )
        .map_err(|error| corpus_error("prepare project corpus reconciliation", error))?;
    Ok(AssistantPreparedCorpusReconciliationState {
        schema_version: prepared.schema_version,
        generation: prepared.generation,
        scope: request.scope,
        snapshot_digest: prepared.snapshot_digest,
        sources: prepared
            .project_sources
            .into_iter()
            .map(assistant_project_source_projection)
            .collect(),
        extract_paths: prepared
            .extract_paths
            .into_iter()
            .map(|path| path.display().to_string())
            .collect(),
        unchanged_paths: prepared
            .unchanged_paths
            .into_iter()
            .map(|path| path.display().to_string())
            .collect(),
        removed_paths: prepared
            .removed_paths
            .into_iter()
            .map(|path| path.display().to_string())
            .collect(),
    })
}

/// Apply one prepared reconciliation atomically. Outcomes must exactly cover
/// the prepared extract paths; failed sources retain their last valid index.
#[uniffi::export]
pub fn assistant_apply_corpus_reconciliation(
    request: AssistantApplyCorpusReconciliationRequest,
) -> FrontendResult<AssistantCorpusIndexReportState> {
    let prepared = PreparedCorpusReconciliation {
        schema_version: request.prepared.schema_version,
        generation: request.prepared.generation,
        scope: match request.prepared.scope {
            AssistantCorpusReconciliationScope::AllLayers => CorpusReconciliationScope::AllLayers,
            AssistantCorpusReconciliationScope::ProjectDocuments => {
                CorpusReconciliationScope::ProjectDocuments
            }
        },
        snapshot_digest: request.prepared.snapshot_digest,
        project_sources: request
            .prepared
            .sources
            .into_iter()
            .map(assistant_project_source_owner)
            .collect(),
        extract_paths: request
            .prepared
            .extract_paths
            .into_iter()
            .map(PathBuf::from)
            .collect(),
        unchanged_paths: request
            .prepared
            .unchanged_paths
            .into_iter()
            .map(PathBuf::from)
            .collect(),
        removed_paths: request
            .prepared
            .removed_paths
            .into_iter()
            .map(PathBuf::from)
            .collect(),
    };
    let outcomes = request
        .outcomes
        .into_iter()
        .map(|outcome| ProjectSourceExtractionOutcome {
            relative_path: PathBuf::from(outcome.relative_path),
            status: match outcome.status {
                AssistantProjectSourceExtractionStatus::Succeeded => {
                    ProjectSourceExtractionStatus::Succeeded
                }
                AssistantProjectSourceExtractionStatus::Failed => {
                    ProjectSourceExtractionStatus::Failed
                }
            },
            diagnostic: outcome.diagnostic,
        })
        .collect::<Vec<_>>();
    let documents = assistant_corpus_documents_owner(request.documents)?;
    let remove_missing_layers = request
        .remove_missing_layers
        .iter()
        .map(|layer| assistant_corpus_layer(layer))
        .collect::<FrontendResult<BTreeSet<_>>>()?;
    let index = CorpusIndex::open(&request.project_root)
        .map_err(|error| corpus_error("open corpus index", error))?;
    let report = index
        .apply_prepared_reconciliation(&prepared, &documents, &remove_missing_layers, &outcomes)
        .map_err(|error| corpus_error("apply project corpus reconciliation", error))?;
    Ok(assistant_corpus_report_projection(report))
}

/// Execute the bounded `corpus.search` operation exposed through project MCP.
#[uniffi::export]
pub fn assistant_corpus_search(
    request: AssistantCorpusSearchRequest,
) -> FrontendResult<Vec<AssistantCorpusSearchHitState>> {
    if request.query.len() > MAX_ASSISTANT_CORPUS_QUERY_BYTES {
        return Err(corpus_error(
            "validate corpus search request",
            format!("query exceeds the {MAX_ASSISTANT_CORPUS_QUERY_BYTES}-byte host limit"),
        ));
    }
    let index = CorpusIndex::open(&request.project_root)
        .map_err(|error| corpus_error("open corpus index", error))?;
    let layers = request
        .layers
        .iter()
        .map(|layer| assistant_corpus_layer(layer))
        .collect::<FrontendResult<BTreeSet<_>>>()?;
    let hits = index
        .search_layers(
            &request.query,
            (request.limit as usize).min(MAX_ASSISTANT_CORPUS_SEARCH_RESULTS),
            &layers,
        )
        .map_err(|error| corpus_error("search corpus", error))?;
    Ok(hits
        .into_iter()
        .map(|hit| AssistantCorpusSearchHitState {
            chunk_id: hit.chunk_id,
            document_id: hit.document_id,
            layer: snake_case_debug(hit.layer),
            title: hit.title,
            text: hit.text,
            score: hit.score,
            citation: assistant_corpus_citation_projection(hit.citation),
            untrusted_evidence: hit.untrusted_evidence,
        })
        .collect())
}

fn tutorial_template_projection(template: TutorialTemplate) -> TutorialTemplateState {
    TutorialTemplateState {
        root: template.root.to_string_lossy().into_owned(),
        content_sha256: template.content_sha256,
    }
}

fn tutorial_project_projection(
    store: &NotebookStore,
    lock: casa_notebook::TutorialLock,
) -> FrontendResult<TutorialProjectProjection> {
    Ok(TutorialProjectProjection {
        notebook: notebook_projection(store, &lock.notebook_filename)?,
        tutorial: tutorial_lock_projection(lock),
    })
}

/// One-shot conversion of `tutorial-pack.v0` into a portable v1 template.
#[uniffi::export]
pub fn tutorial_migrate_v0(
    request: TutorialMigrateRequest,
) -> FrontendResult<TutorialTemplateState> {
    let template = TutorialProject::migrate_v0_template(&request.pack_path, &request.destination)
        .map_err(|error| tutorial_error("migrate tutorial-pack v0", error))?;
    Ok(tutorial_template_projection(template))
}

/// Fork one immutable template into an editable learner notebook and managed lock.
#[uniffi::export]
pub fn tutorial_fork(request: TutorialForkRequest) -> FrontendResult<TutorialProjectProjection> {
    let template = TutorialProject::load_template(&request.template_path)
        .map_err(|error| tutorial_error("load tutorial template", error))?;
    let project = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?;
    let forked = project
        .fork_template(&template, &request.filename)
        .map_err(|error| tutorial_error("fork tutorial template", error))?;
    let store = NotebookStore::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial notebook store", error))?;
    tutorial_project_projection(&store, forked.lock)
}

/// List every Rust-owned learner tutorial in one project.
#[uniffi::export]
pub fn tutorial_project_list(
    project_root: String,
) -> FrontendResult<Vec<TutorialProjectProjection>> {
    let project = TutorialProject::open(&project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?;
    let store = NotebookStore::open(&project_root)
        .map_err(|error| tutorial_error("open tutorial notebook store", error))?;
    project
        .list_locks()
        .map_err(|error| tutorial_error("list tutorial locks", error))?
        .into_iter()
        .map(|lock| tutorial_project_projection(&store, lock))
        .collect()
}

/// Resolve the exact source, redirect, integrity, disk, and extraction approval facts.
#[uniffi::export]
pub fn tutorial_plan_acquisition(
    request: TutorialPlanRequest,
) -> FrontendResult<TutorialAcquisitionPlanState> {
    let plan = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?
        .plan_acquisition(
            tutorial_notebook_id(&request.notebook_id)?,
            &request.dataset_id,
            request.source_override.as_deref(),
        )
        .map_err(|error| tutorial_error("plan tutorial acquisition", error))?;
    Ok(tutorial_plan_projection(plan))
}

/// Begin one exact explicitly approved acquisition generation.
#[uniffi::export]
pub fn tutorial_begin_acquisition(
    request: TutorialBeginRequest,
) -> FrontendResult<TutorialDatasetState> {
    let plan = tutorial_plan_owner(request.plan)?;
    let approval = TutorialAcquisitionApproval {
        approval_sha256: request.approval.approval_sha256,
        allow_missing_digest: request.approval.allow_missing_digest,
        skipped_check_ids: request.approval.skipped_check_ids,
    };
    let state = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?
        .begin_acquisition(&plan, approval)
        .map_err(|error| tutorial_error("begin tutorial acquisition", error))?;
    Ok(tutorial_dataset_projection(state))
}

/// Apply one typed acquisition transition. Generation-bound actions require an
/// exact generation; only bounded advance accepts a byte budget.
#[uniffi::export]
pub fn tutorial_acquisition_action(
    request: TutorialActionRequest,
) -> FrontendResult<TutorialDatasetState> {
    let notebook_id = tutorial_notebook_id(&request.notebook_id)?;
    let project = TutorialProject::open(&request.project_root)
        .map_err(|error| tutorial_error("open tutorial project", error))?;
    let state = match request.action {
        TutorialPersistenceAction::Resume => {
            if request.generation.is_some() || request.max_download_bytes.is_some() {
                return Err(tutorial_error(
                    "resume tutorial acquisition",
                    "resume does not accept generation or byte-budget fields",
                ));
            }
            project.resume_acquisition(notebook_id, &request.dataset_id)
        }
        TutorialPersistenceAction::Restart => {
            if request.generation.is_some() || request.max_download_bytes.is_some() {
                return Err(tutorial_error(
                    "restart tutorial acquisition",
                    "restart does not accept generation or byte-budget fields",
                ));
            }
            project.restart_acquisition(notebook_id, &request.dataset_id)
        }
        TutorialPersistenceAction::Retry => {
            if request.generation.is_some() || request.max_download_bytes.is_some() {
                return Err(tutorial_error(
                    "retry tutorial acquisition",
                    "retry does not accept generation or byte-budget fields",
                ));
            }
            project.retry_acquisition(notebook_id, &request.dataset_id)
        }
        TutorialPersistenceAction::Cancel => {
            if request.max_download_bytes.is_some() {
                return Err(tutorial_error(
                    "cancel tutorial acquisition",
                    "cancel does not accept a byte budget",
                ));
            }
            let generation = request.generation.ok_or_else(|| {
                tutorial_error(
                    "cancel tutorial acquisition",
                    "cancel requires an exact generation",
                )
            })?;
            project.cancel_acquisition(notebook_id, &request.dataset_id, generation)
        }
        TutorialPersistenceAction::Advance => {
            let generation = request.generation.ok_or_else(|| {
                tutorial_error(
                    "advance tutorial acquisition",
                    "advance requires an exact generation",
                )
            })?;
            let max_download_bytes = request.max_download_bytes.ok_or_else(|| {
                tutorial_error(
                    "advance tutorial acquisition",
                    "advance requires an explicit byte budget",
                )
            })?;
            project.advance_acquisition(
                notebook_id,
                &request.dataset_id,
                generation,
                max_download_bytes,
            )
        }
    }
    .map_err(|error| tutorial_error("apply tutorial acquisition action", error))?;
    Ok(tutorial_dataset_projection(state))
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
                    .map(serde_json::to_value)
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

/// Canonical agent-facing task schema assembled by the frontend contract owner.
/// MCP hosts serialize this projection but do not merge provider schemas.
pub fn assistant_task_schema(task_id: &str) -> FrontendResult<serde_json::Value> {
    let bundle = builtin_surface_bundle(task_id)
        .map_err(|error| parameter_error("load assistant task schema", error))?;
    let mut schema = project_ui_form(&bundle);
    let concepts = bundle
        .catalog
        .concepts
        .iter()
        .map(|concept| {
            serde_json::to_value(&concept.value_domain)
                .map(|value_domain| {
                    (
                        (
                            concept.id.as_str().to_owned(),
                            u64::from(concept.semantic_revision.0),
                        ),
                        value_domain,
                    )
                })
                .map_err(|error| parameter_error("project task value domain", error))
        })
        .collect::<FrontendResult<BTreeMap<_, _>>>()?;
    let predicates = bundle
        .surface
        .bindings()
        .iter()
        .map(|binding| {
            let active_when = serde_json::to_value(&binding.active_when)
                .map_err(|error| parameter_error("project active predicate", error))?;
            let required_when = serde_json::to_value(&binding.required_when)
                .map_err(|error| parameter_error("project required predicate", error))?;
            let value_domain = concepts
                .get(&(
                    binding.concept.id.as_str().to_owned(),
                    u64::from(binding.concept.semantic_revision.0),
                ))
                .cloned()
                .ok_or_else(|| {
                    parameter_error(
                        "assistant task schema",
                        format!("task contract omits value domain for {}", binding.name),
                    )
                })?;
            Ok((
                binding.name.clone(),
                (active_when, required_when, value_domain),
            ))
        })
        .collect::<FrontendResult<BTreeMap<_, _>>>()?;
    for argument in schema
        .get_mut("arguments")
        .and_then(serde_json::Value::as_array_mut)
        .into_iter()
        .flatten()
    {
        let Some(id) = argument.get("id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if let Some((active_when, required_when, value_domain)) = predicates.get(id) {
            argument["active_when"] = active_when.clone();
            argument["required_when"] = required_when.clone();
            argument["value_domain"] = value_domain.clone();
        }
    }
    Ok(schema)
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AssistantTaskSuggestionAction {
    pub kind: &'static str,
    pub task_id: String,
    pub parameters: BTreeMap<String, String>,
    pub validated_patch: ResolutionPatch,
}

/// Validate a non-mutating assistant task action through the canonical
/// parameter runtime. No MCP-local parser or session lifecycle is involved.
pub fn assistant_task_suggestion_action(
    task_id: &str,
    parameters: BTreeMap<String, String>,
    project_root: &Path,
) -> FrontendResult<AssistantTaskSuggestionAction> {
    let bundle = builtin_surface_bundle(task_id)
        .map_err(|error| parameter_error("load assistant task suggestion", error))?;
    let allowed = bundle
        .surface
        .bindings()
        .iter()
        .map(|binding| binding.name.as_str())
        .collect::<BTreeSet<_>>();
    let unknown = parameters
        .keys()
        .filter(|name| !allowed.contains(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(parameter_error(
            "validate assistant task suggestion",
            format!(
                "task.suggest contains unknown {task_id} parameters: {}",
                unknown.join(", ")
            ),
        ));
    }
    let mut values = BTreeMap::new();
    for (name, text) in &parameters {
        let binding = bundle
            .surface
            .bindings()
            .iter()
            .find(|binding| binding.name == *name)
            .expect("allowed binding exists");
        let concept = bundle.catalog.concept(&binding.concept).ok_or_else(|| {
            parameter_error(
                "validate assistant task suggestion",
                format!("task contract omits the value domain for {name}"),
            )
        })?;
        let typed = parse_parameter_text(text, &concept.value_domain).map_err(|error| {
            parameter_error(
                "validate assistant task suggestion",
                format!("invalid {task_id}.{name} value: {error}"),
            )
        })?;
        values.insert(name.clone(), typed);
    }
    let patch = ResolutionPatch {
        values,
        unset: BTreeSet::new(),
    };
    let session = ParameterRuntime::default()
        .open_session(OpenSessionRequest {
            bundle,
            workspace: project_root.to_path_buf(),
            source: BaseSource::Defaults,
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch: patch.clone(),
            managed_save: false,
        })
        .map_err(|error| {
            parameter_error(
                "validate assistant task suggestion",
                format!("parameters do not form a runnable {task_id} request: {error}"),
            )
        })?;
    let errors = session
        .diagnostics()
        .iter()
        .filter(|diagnostic| diagnostic.level == casa_task_runtime::DiagnosticLevel::Error)
        .map(|diagnostic| diagnostic.message.as_str())
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return Err(parameter_error(
            "validate assistant task suggestion",
            format!(
                "parameters do not form a runnable {task_id} request: {}",
                errors.join("; ")
            ),
        ));
    }
    Ok(AssistantTaskSuggestionAction {
        kind: "task_suggestion",
        task_id: task_id.to_owned(),
        parameters,
        validated_patch: patch,
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
                values: match constraint {
                    casa_provider_contracts::NarrowingConstraint::AllowedValues { values }
                    | casa_provider_contracts::NarrowingConstraint::SelectorCapabilities {
                        capabilities: values,
                    } => values.clone(),
                    _ => Vec::new(),
                },
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
            python: value.projections.python.as_ref().map(|projection| {
                SurfaceParameterPythonProjection {
                    name: projection.name.clone(),
                    type_hint: projection.type_hint.clone(),
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

fn surface_parameter_definition(
    value: &casa_provider_contracts::SurfaceDefinition,
) -> SurfaceParameterDefinition {
    SurfaceParameterDefinition {
        kind: value.kind().to_string(),
        id: value.id().to_string(),
        contract_version: value.contract_version() as u64,
        display_name: value.display_name().to_string(),
        category: value.category().to_string(),
        summary: value.summary().to_string(),
        execution: SurfaceExecutionProjection {
            invocation_name: value.execution().invocation_name.clone(),
            fixed_args: value.execution().fixed_args.clone(),
        },
        bindings: value
            .bindings()
            .iter()
            .map(surface_parameter_binding)
            .collect(),
    }
}

fn surface_parameter_catalog(
    value: &casa_provider_contracts::ParameterCatalog,
) -> SurfaceParameterCatalog {
    SurfaceParameterCatalog {
        schema_version: value.schema_version as u64,
        concepts: value
            .concepts
            .iter()
            .map(|concept| SurfaceParameterConcept {
                id: concept.id.0.clone(),
                semantic_revision: concept.semantic_revision.0 as u64,
                casa_name: concept.casa_name.clone(),
                value_domain: surface_parameter_type(&concept.value_domain),
                unit_dimension: concept.unit_dimension.as_ref().map(snake_case_debug),
                semantic_role: snake_case_debug(concept.semantic_role),
                documentation: SurfaceParameterDocumentation {
                    summary: concept.documentation.summary.clone(),
                    details: concept.documentation.details.clone(),
                    examples: concept.documentation.examples.clone(),
                },
                persistence_class: snake_case_debug(concept.persistence_class),
            })
            .collect(),
    }
}

fn surface_parameter_bundle(value: SurfaceContractBundle) -> SurfaceParameterBundle {
    SurfaceParameterBundle {
        schema_version: value.schema_version as u64,
        surface: surface_parameter_definition(&value.surface),
        catalog: surface_parameter_catalog(&value.catalog),
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

/// Parse, normalize, and validate one parameter edit through the generated boundary.
#[uniffi::export]
pub fn parameter_validate_edit(
    request: SurfaceParameterEditRequest,
) -> FrontendResult<SurfaceParameterEditResult> {
    let bundle = parameter_bundle(&request.surface_id)?;
    let grammar = selector_grammar_for_parameter(&bundle, &request.parameter);
    let mut suggestions = request
        .suggestions
        .into_iter()
        .map(|suggestion| ParameterEditSuggestion {
            label: suggestion.label,
            value: suggestion.value.into(),
        })
        .collect::<Vec<_>>();
    if request.text.trim().is_empty()
        && let Some(grammar) = grammar
    {
        suggestions.extend(domain_parameter_suggestions(
            grammar,
            request.dataset_path.as_deref(),
            request.spectral_window_id,
        ));
    }
    let mut result =
        validate_parameter_edit(&bundle, &request.parameter, &request.text, suggestions);

    let domain_edit = result
        .is_valid()
        .then(|| result.normalized_value.clone())
        .flatten()
        .zip(grammar);
    if let Some((value, grammar)) = domain_edit {
        validate_domain_parameter_edit(
            &mut result,
            grammar,
            &value,
            request.dataset_path.as_deref(),
            request.spectral_window_id,
        );
    }

    Ok(SurfaceParameterEditResult {
        parameter: result.parameter,
        normalized_value: result.normalized_value.map(Into::into),
        diagnostics: result
            .diagnostics
            .into_iter()
            .map(|diagnostic| SurfaceParameterDiagnostic {
                level: "error".to_string(),
                code: snake_case_debug(diagnostic.code),
                message: diagnostic.message,
                parameter: Some(diagnostic.parameter),
                location: None,
                suggestions: Vec::new(),
            })
            .collect(),
        supported_capabilities: result.supported_capabilities,
        suggestions: result
            .suggestions
            .into_iter()
            .map(|suggestion| SurfaceParameterEditSuggestion {
                label: suggestion.label,
                value: suggestion.value.into(),
            })
            .collect(),
    })
}

fn domain_parameter_suggestions(
    grammar: SelectorGrammar,
    dataset_path: Option<&str>,
    spectral_window_id: Option<i32>,
) -> Vec<ParameterEditSuggestion> {
    if grammar == SelectorGrammar::ImageChannels {
        return image_channel_suggestions(dataset_path);
    }
    let Some(path) = dataset_path else {
        return Vec::new();
    };
    let Ok(ms) = MeasurementSet::open(path) else {
        return Vec::new();
    };
    let Ok(context) = ms.probe_context() else {
        return Vec::new();
    };
    if grammar == SelectorGrammar::SpectralWindow
        && let Some(spw_id) = spectral_window_id
        && let Ok(spw_id) = usize::try_from(spw_id)
        && let Some(channels) = context
            .spectral_windows
            .iter()
            .find(|spw| spw.row == spw_id)
            .map(|spw| spw.channel_count)
    {
        return channel_range_suggestions(channels);
    }
    let pairs = match grammar {
        SelectorGrammar::Field => Some(
            context
                .fields
                .into_iter()
                .map(|field| {
                    (
                        format!("{}: {}", field.row, field.name),
                        field.row.to_string(),
                    )
                })
                .collect::<Vec<_>>(),
        ),
        SelectorGrammar::SpectralWindow => Some(
            context
                .spectral_windows
                .into_iter()
                .map(|spw| {
                    (
                        format!("spw {}: {} channels", spw.row, spw.channel_count),
                        spw.row.to_string(),
                    )
                })
                .collect::<Vec<_>>(),
        ),
        SelectorGrammar::Antenna => Some(
            context
                .antennas
                .into_iter()
                .map(|antenna| (antenna.name.clone(), antenna.name))
                .collect::<Vec<_>>(),
        ),
        SelectorGrammar::Observation => Some(
            context
                .observations
                .into_iter()
                .map(|observation| {
                    let detail = [observation.project, observation.telescope_name]
                        .into_iter()
                        .filter(|value| !value.is_empty())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let label = if detail.is_empty() {
                        format!("observation {}", observation.row)
                    } else {
                        format!("observation {}: {detail}", observation.row)
                    };
                    (label, observation.row.to_string())
                })
                .collect::<Vec<_>>(),
        ),
        SelectorGrammar::Intent => Some(
            context
                .intents
                .into_iter()
                .map(|label| (label.clone(), label))
                .collect::<Vec<_>>(),
        ),
        SelectorGrammar::Feed => Some(
            context
                .feed_ids
                .into_iter()
                .map(|id| (format!("feed {id}"), id.to_string()))
                .collect::<Vec<_>>(),
        ),
        SelectorGrammar::Correlation => Some(
            context
                .correlations
                .into_iter()
                .map(|label| (label.clone(), label))
                .collect::<Vec<_>>(),
        ),
        SelectorGrammar::TimeRange
        | SelectorGrammar::UvRange
        | SelectorGrammar::Scan
        | SelectorGrammar::Array
        | SelectorGrammar::MsSelect
        | SelectorGrammar::ImageBox
        | SelectorGrammar::ImageChannels
        | SelectorGrammar::ImageRegion
        | SelectorGrammar::Stokes => None,
    };
    pairs
        .unwrap_or_default()
        .into_iter()
        .map(|(label, value)| ParameterEditSuggestion {
            label,
            value: ParameterValue::String(value),
        })
        .collect()
}

fn image_channel_suggestions(dataset_path: Option<&str>) -> Vec<ParameterEditSuggestion> {
    let Some(path) = dataset_path else {
        return Vec::new();
    };
    let Ok(image) = AnyPagedImage::open(path) else {
        return Vec::new();
    };
    let spectral_axis = match &image {
        AnyPagedImage::Float32(image) => image.find_axis(CoordinateType::Spectral),
        AnyPagedImage::Float64(image) => image.find_axis(CoordinateType::Spectral),
        AnyPagedImage::Complex32(image) => image.find_axis(CoordinateType::Spectral),
        AnyPagedImage::Complex64(image) => image.find_axis(CoordinateType::Spectral),
    };
    let Some(axis) = spectral_axis else {
        return Vec::new();
    };
    let channels = image.shape()[axis];
    if channels == 0 {
        return Vec::new();
    }
    channel_range_suggestions(image.shape()[axis])
}

fn channel_range_suggestions(channels: usize) -> Vec<ParameterEditSuggestion> {
    if channels == 0 {
        return Vec::new();
    }
    let mut suggestions = vec![
        ("All channels".to_string(), String::new()),
        ("First channel".to_string(), "0".to_string()),
    ];
    if channels > 1 {
        suggestions.push(("All explicit".to_string(), format!("0~{}", channels - 1)));
    }
    if channels >= 16 {
        suggestions.push(("Every fourth".to_string(), format!("0~{}^4", channels - 1)));
    }
    suggestions
        .into_iter()
        .map(|(label, value)| ParameterEditSuggestion {
            label,
            value: ParameterValue::String(value),
        })
        .collect()
}

fn selector_grammar_for_parameter(
    bundle: &SurfaceContractBundle,
    parameter: &str,
) -> Option<SelectorGrammar> {
    let binding = bundle
        .surface
        .bindings()
        .iter()
        .find(|binding| binding.name == parameter)?;
    let concept = bundle.catalog.concept(&binding.concept)?;
    selector_grammar_from_rule(&concept.normalization)
}

fn selector_grammar_from_rule(rule: &NormalizationRule) -> Option<SelectorGrammar> {
    match rule {
        NormalizationRule::CasaSelector { grammar, .. } => Some(*grammar),
        NormalizationRule::Sequence { rules } => rules.iter().find_map(selector_grammar_from_rule),
        NormalizationRule::Identity
        | NormalizationRule::Trim
        | NormalizationRule::Lowercase
        | NormalizationRule::Path
        | NormalizationRule::Quantity { .. } => None,
    }
}

fn validate_domain_parameter_edit(
    result: &mut casa_task_runtime::ParameterEditResult,
    grammar: SelectorGrammar,
    value: &ParameterValue,
    dataset_path: Option<&str>,
    spectral_window_id: Option<i32>,
) {
    let ParameterValue::String(value) = value else {
        result.reject(
            ParameterEditDiagnosticCode::InvalidText,
            "selector edits must normalize to a string",
        );
        return;
    };
    if value.is_empty() {
        return;
    }

    let validation = match grammar {
        SelectorGrammar::ImageChannels => validate_image_channel_edit(dataset_path, value),
        SelectorGrammar::ImageBox | SelectorGrammar::ImageRegion | SelectorGrammar::Stokes => {
            return;
        }
        _ => validate_measurement_set_edit(dataset_path, grammar, value, spectral_window_id),
    };
    if let Err((code, message)) = validation {
        result.reject(code, message);
    }
}

fn validate_measurement_set_edit(
    dataset_path: Option<&str>,
    grammar: SelectorGrammar,
    value: &str,
    spectral_window_id: Option<i32>,
) -> Result<(), (ParameterEditDiagnosticCode, String)> {
    let path = dataset_path.ok_or_else(|| {
        (
            ParameterEditDiagnosticCode::DatasetUnavailable,
            "MeasurementSet context is required to validate this selector".to_string(),
        )
    })?;
    let ms = MeasurementSet::open(Path::new(path)).map_err(|error| {
        (
            ParameterEditDiagnosticCode::DatasetUnavailable,
            format!("open MeasurementSet selector context {path}: {error}"),
        )
    })?;
    validate_ms_selector_edit(
        &ms,
        grammar,
        value,
        MsSelectorEditContext { spectral_window_id },
    )
    .map_err(|error| classify_selector_error(error.to_string()))
}

fn validate_image_channel_edit(
    dataset_path: Option<&str>,
    value: &str,
) -> Result<(), (ParameterEditDiagnosticCode, String)> {
    let path = dataset_path.ok_or_else(|| {
        (
            ParameterEditDiagnosticCode::DatasetUnavailable,
            "image context is required to validate channel bounds".to_string(),
        )
    })?;
    let image = AnyPagedImage::open(path).map_err(|error| {
        (
            ParameterEditDiagnosticCode::DatasetUnavailable,
            format!("open image selector context {path}: {error}"),
        )
    })?;
    let spectral_axis = match &image {
        AnyPagedImage::Float32(image) => image.find_axis(CoordinateType::Spectral),
        AnyPagedImage::Float64(image) => image.find_axis(CoordinateType::Spectral),
        AnyPagedImage::Complex32(image) => image.find_axis(CoordinateType::Spectral),
        AnyPagedImage::Complex64(image) => image.find_axis(CoordinateType::Spectral),
    }
    .ok_or_else(|| {
        (
            ParameterEditDiagnosticCode::DatasetUnavailable,
            "image has no spectral axis for channel validation".to_string(),
        )
    })?;
    parse_image_channel_selection(value, image.shape()[spectral_axis])
        .map(|_| ())
        .map_err(|error| classify_selector_error(error.to_string()))
}

fn classify_selector_error(message: String) -> (ParameterEditDiagnosticCode, String) {
    let code = if message.contains("outside")
        || message.contains("exceeds")
        || message.contains("Shape mismatch")
    {
        ParameterEditDiagnosticCode::SelectorValueOutOfRange
    } else if message.contains("not present") || message.contains("did not match") {
        ParameterEditDiagnosticCode::UnknownSelectorValue
    } else {
        ParameterEditDiagnosticCode::InvalidSelectorSyntax
    };
    (code, message)
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

/// Return the complete checked parameter catalog for generated frontends.
#[uniffi::export]
pub fn parameter_catalog() -> FrontendResult<SurfaceParameterCatalogEnvelope> {
    let catalog = builtin_surface_catalog()
        .map_err(|error| parameter_error("load parameter catalog", error))?;
    Ok(SurfaceParameterCatalogEnvelope {
        schema_version: catalog.schema_version as u64,
        catalog: surface_parameter_catalog(&catalog.catalog),
        surfaces: catalog
            .surfaces
            .iter()
            .map(surface_parameter_definition)
            .collect(),
    })
}

/// Parse the canonical surface identity from one sparse profile.
#[uniffi::export]
pub fn parameter_profile_surface(profile_toml: String) -> FrontendResult<String> {
    parse_profile(&profile_toml)
        .map(|profile| profile.header.surface)
        .map_err(|error| parameter_error("parse parameter profile", error))
}

/// Render the catalog-owned commented reference template for one surface.
#[uniffi::export]
pub fn parameter_template_toml(surface_id: String) -> FrontendResult<String> {
    render_documented_template(&parameter_bundle(&surface_id)?)
        .map_err(|error| parameter_error("render parameter template", error))
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

/// Explicitly write Last or Last Successful for one workspace.
#[uniffi::export]
pub fn parameter_write_managed(
    surface_id: String,
    workspace: String,
    values: HashMap<String, SurfaceParameterValue>,
    successful: bool,
) -> FrontendResult<SurfaceParameterWriteResult> {
    let bundle = parameter_bundle(&surface_id)?;
    if successful && bundle.surface.kind() == SurfaceKind::Session {
        return Err(parameter_error(
            "write managed parameter profile",
            format!("session surface {surface_id:?} does not have Last Successful"),
        ));
    }
    let profile = parameter_render_toml(surface_id.clone(), values)?;
    let kind = if successful {
        ManagedProfileKind::LastSuccessful
    } else {
        ManagedProfileKind::Last
    };
    let outcome = ManagedStateStore::for_workspace(workspace)
        .write(&surface_id, kind, &profile)
        .map_err(|error| parameter_error("write managed parameter profile", error))?;
    Ok(SurfaceParameterWriteResult {
        path: outcome.path.to_string_lossy().into_owned(),
        bytes_written: profile.len() as u64,
        managed_kind: Some(if successful {
            "last_successful".to_string()
        } else {
            "last".to_string()
        }),
    })
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
    let selection = MsSelection {
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
        data_description: None,
        state: None,
        msselect: normalized_optional(request.msselect.clone()),
        ..MsSelection::default()
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

    let selection = MsSelection {
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
        data_description: None,
        state: None,
        msselect: normalized_optional(request.msselect.clone()),
        ..MsSelection::default()
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
        build_msexplore_payload_from_spec(&spec).map_err(|error| FrontendServiceError::Plot {
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

fn table_browser_bookmark_owner(
    bookmark: TableBrowserBookmark,
) -> casars_tablebrowser_protocol::BrowserBookmark {
    match bookmark {
        TableBrowserBookmark::Cell { row, column } => {
            casars_tablebrowser_protocol::BrowserBookmark::Cell {
                row: row as usize,
                column,
            }
        }
        TableBrowserBookmark::TableKeyword { path } => {
            casars_tablebrowser_protocol::BrowserBookmark::TableKeyword { path }
        }
        TableBrowserBookmark::ColumnKeyword { column, path } => {
            casars_tablebrowser_protocol::BrowserBookmark::ColumnKeyword { column, path }
        }
        TableBrowserBookmark::Subtable { name } => {
            casars_tablebrowser_protocol::BrowserBookmark::Subtable { name }
        }
    }
}

fn table_browser_parameters_owner(
    parameters: TableBrowserParameters,
) -> Result<casars_tablebrowser_protocol::BrowserParameters, String> {
    Ok(casars_tablebrowser_protocol::BrowserParameters {
        view: parse_table_browser_view(&parameters.view)?,
        row_start: parameters.row_start as usize,
        row_count: parameters.row_count as usize,
        linked_table: parameters.linked_table,
        bookmark: parameters.bookmark.map(table_browser_bookmark_owner),
        content_mode: parse_table_browser_content_mode(&parameters.content_mode)?,
    })
}

fn table_browser_command_owner(
    command: TableBrowserCommand,
    viewport: BrowserViewport,
) -> Result<BrowserCommand, String> {
    Ok(match command {
        TableBrowserCommand::Configure { parameters } => BrowserCommand::Configure {
            parameters: table_browser_parameters_owner(parameters)?,
        },
        TableBrowserCommand::SetFocus { focus } => BrowserCommand::SetFocus {
            focus: parse_table_browser_focus(Some(&focus))?
                .ok_or_else(|| "table browser focus cannot be empty".to_string())?,
            viewport: Some(viewport),
        },
        TableBrowserCommand::CycleView { forward } => BrowserCommand::CycleView {
            forward,
            viewport: Some(viewport),
        },
        TableBrowserCommand::MoveUp { steps } => BrowserCommand::MoveUp {
            steps: steps as usize,
            viewport: Some(viewport),
        },
        TableBrowserCommand::MoveDown { steps } => BrowserCommand::MoveDown {
            steps: steps as usize,
            viewport: Some(viewport),
        },
        TableBrowserCommand::MoveLeft { steps } => BrowserCommand::MoveLeft {
            steps: steps as usize,
            viewport: Some(viewport),
        },
        TableBrowserCommand::MoveRight { steps } => BrowserCommand::MoveRight {
            steps: steps as usize,
            viewport: Some(viewport),
        },
        TableBrowserCommand::PageUp { pages } => BrowserCommand::PageUp {
            pages: pages as usize,
            viewport: Some(viewport),
        },
        TableBrowserCommand::PageDown { pages } => BrowserCommand::PageDown {
            pages: pages as usize,
            viewport: Some(viewport),
        },
        TableBrowserCommand::Activate => BrowserCommand::Activate {
            viewport: Some(viewport),
        },
        TableBrowserCommand::Back => BrowserCommand::Back {
            viewport: Some(viewport),
        },
        TableBrowserCommand::Escape => BrowserCommand::Escape {
            viewport: Some(viewport),
        },
    })
}

fn table_browser_value_path_projection(
    segment: casars_tablebrowser_protocol::ValuePathSegment,
) -> TableBrowserValuePathSegment {
    match segment {
        casars_tablebrowser_protocol::ValuePathSegment::RecordField { name } => {
            TableBrowserValuePathSegment {
                segment: "record_field".to_string(),
                name: Some(name),
                flat_index: None,
            }
        }
        casars_tablebrowser_protocol::ValuePathSegment::ArrayIndex { flat_index } => {
            TableBrowserValuePathSegment {
                segment: "array_index".to_string(),
                name: None,
                flat_index: Some(flat_index as u64),
            }
        }
    }
}

fn table_browser_address_projection(
    address: casars_tablebrowser_protocol::BrowserAddress,
) -> TableBrowserSelectedAddress {
    use casars_tablebrowser_protocol::BrowserAddress;
    match address {
        BrowserAddress::Column { table_path, column } => TableBrowserSelectedAddress {
            kind: "column".to_string(),
            table_path,
            row: None,
            column: Some(column),
            keyword_path: Vec::new(),
            value_path: Vec::new(),
            source: None,
            target_path: None,
        },
        BrowserAddress::Cell {
            table_path,
            row,
            column,
            value_path,
        } => TableBrowserSelectedAddress {
            kind: "cell".to_string(),
            table_path,
            row: Some(row as u64),
            column: Some(column),
            keyword_path: Vec::new(),
            value_path: value_path
                .into_iter()
                .map(table_browser_value_path_projection)
                .collect(),
            source: None,
            target_path: None,
        },
        BrowserAddress::TableKeyword {
            table_path,
            keyword_path,
            value_path,
        } => TableBrowserSelectedAddress {
            kind: "table_keyword".to_string(),
            table_path,
            row: None,
            column: None,
            keyword_path,
            value_path: value_path
                .into_iter()
                .map(table_browser_value_path_projection)
                .collect(),
            source: None,
            target_path: None,
        },
        BrowserAddress::ColumnKeyword {
            table_path,
            column,
            keyword_path,
            value_path,
        } => TableBrowserSelectedAddress {
            kind: "column_keyword".to_string(),
            table_path,
            row: None,
            column: Some(column),
            keyword_path,
            value_path: value_path
                .into_iter()
                .map(table_browser_value_path_projection)
                .collect(),
            source: None,
            target_path: None,
        },
        BrowserAddress::Subtable {
            table_path,
            source,
            target_path,
        } => TableBrowserSelectedAddress {
            kind: "subtable".to_string(),
            table_path,
            row: None,
            column: None,
            keyword_path: Vec::new(),
            value_path: Vec::new(),
            source: Some(source),
            target_path: Some(target_path),
        },
    }
}

fn table_browser_scalar_projection(
    value: casars_tablebrowser_protocol::BrowserScalarValue,
) -> TableBrowserScalarValue {
    use casars_tablebrowser_protocol::BrowserScalarValue;
    match value {
        BrowserScalarValue::Bool(value) => TableBrowserScalarValue::Bool { value },
        BrowserScalarValue::UInt8(value) => TableBrowserScalarValue::Uint {
            value: value as u64,
        },
        BrowserScalarValue::UInt16(value) => TableBrowserScalarValue::Uint {
            value: value as u64,
        },
        BrowserScalarValue::UInt32(value) => TableBrowserScalarValue::Uint {
            value: value as u64,
        },
        BrowserScalarValue::Int16(value) => TableBrowserScalarValue::Int {
            value: value as i64,
        },
        BrowserScalarValue::Int32(value) => TableBrowserScalarValue::Int {
            value: value as i64,
        },
        BrowserScalarValue::Int64(value) => TableBrowserScalarValue::Int { value },
        BrowserScalarValue::Float32(value) => TableBrowserScalarValue::Float {
            value: value as f64,
        },
        BrowserScalarValue::Float64(value) => TableBrowserScalarValue::Float { value },
        BrowserScalarValue::Complex32(value) => TableBrowserScalarValue::Complex {
            re: value.re as f64,
            im: value.im as f64,
        },
        BrowserScalarValue::Complex64(value) => TableBrowserScalarValue::Complex {
            re: value.re,
            im: value.im,
        },
        BrowserScalarValue::String(value) => TableBrowserScalarValue::String { value },
    }
}

fn table_browser_value_node_projection(
    node: casars_tablebrowser_protocol::BrowserValueNode,
) -> TableBrowserValueNode {
    use casars_tablebrowser_protocol::BrowserValueNode;
    match node {
        BrowserValueNode::Undefined => TableBrowserValueNode::Undefined,
        BrowserValueNode::Scalar { value } => TableBrowserValueNode::Scalar {
            value: table_browser_scalar_projection(value),
        },
        BrowserValueNode::Array {
            primitive,
            shape,
            total_elements,
            page_start,
            page_size,
            elements,
        } => TableBrowserValueNode::Array {
            primitive: snake_case_debug(primitive),
            shape: shape.into_iter().map(|value| value as u64).collect(),
            total_elements: total_elements as u64,
            page_start: page_start as u64,
            page_size: page_size as u64,
            elements: elements
                .into_iter()
                .map(|element| TableBrowserArrayElement {
                    flat_index: element.flat_index as u64,
                    index: element
                        .index
                        .into_iter()
                        .map(|value| value as u64)
                        .collect(),
                    value: table_browser_scalar_projection(element.value),
                    selected: element.selected,
                })
                .collect(),
        },
        BrowserValueNode::Record {
            total_fields,
            page_start,
            page_size,
            fields,
        } => TableBrowserValueNode::Record {
            total_fields: total_fields as u64,
            page_start: page_start as u64,
            page_size: page_size as u64,
            fields: fields
                .into_iter()
                .map(|field| TableBrowserRecordFieldSummary {
                    name: field.name,
                    kind: snake_case_debug(field.kind),
                    summary: field.summary,
                    expandable: field.expandable,
                    openable: field.openable,
                    selected: field.selected,
                })
                .collect(),
        },
        BrowserValueNode::TableRef {
            path,
            resolved_path,
            openable,
        } => TableBrowserValueNode::TableRef {
            path,
            resolved_path,
            openable,
        },
    }
}

fn table_browser_metrics_projection(
    metrics: casars_tablebrowser_protocol::BrowserNavigationMetrics,
) -> TableBrowserNavigationMetrics {
    TableBrowserNavigationMetrics {
        selected_index: metrics.selected_index as u64,
        total_items: metrics.total_items as u64,
        viewport_items: metrics.viewport_items as u64,
    }
}

fn table_browser_snapshot_projection(
    snapshot: casars_tablebrowser_protocol::BrowserSnapshot,
) -> TableBrowserSnapshot {
    TableBrowserSnapshot {
        capabilities: TableBrowserCapabilities {
            editable: snapshot.capabilities.editable,
        },
        view: snake_case_debug(snapshot.view),
        focus: snake_case_debug(snapshot.focus),
        table_path: snapshot.table_path,
        breadcrumb: snapshot
            .breadcrumb
            .into_iter()
            .map(|entry| TableBrowserBreadcrumb {
                label: entry.label,
                path: entry.path,
            })
            .collect(),
        viewport: TableBrowserViewport {
            width: snapshot.viewport.width,
            height: snapshot.viewport.height,
            inspector_height: snapshot.viewport.inspector_height,
        },
        status_line: snapshot.status_line,
        content_lines: snapshot.content_lines,
        vertical_metrics: snapshot
            .vertical_metrics
            .map(table_browser_metrics_projection),
        horizontal_metrics: snapshot
            .horizontal_metrics
            .map(table_browser_metrics_projection),
        selected_address: snapshot
            .selected_address
            .map(table_browser_address_projection),
        inspector: snapshot.inspector.map(|inspector| TableBrowserInspector {
            title: inspector.title,
            trail: inspector
                .trail
                .into_iter()
                .map(|entry| TableBrowserInspectorTrailEntry {
                    label: entry.label,
                    summary: entry.summary,
                })
                .collect(),
            node: table_browser_value_node_projection(inspector.node),
            rendered_lines: inspector.rendered_lines,
        }),
    }
}

#[uniffi::export]
pub fn build_table_browser_snapshot(
    request: TableBrowserSnapshotRequest,
) -> FrontendResult<TableBrowserSnapshot> {
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
    if !request.selected_view.trim().is_empty() {
        browser.set_view(
            parse_table_browser_view(&request.selected_view)
                .map_err(|reason| FrontendServiceError::TableExplorer { reason })?,
        );
    }
    if let Some(focus) = parse_table_browser_focus(Some(&request.focus))
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
        let command = table_browser_command_owner(command, viewport)
            .map_err(|reason| FrontendServiceError::TableExplorer { reason })?;
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
        .map(table_browser_snapshot_projection)
}

#[uniffi::export]
pub fn build_table_browser_cell_window(
    request: TableBrowserCellWindowRequest,
) -> FrontendResult<TableBrowserCellWindowSnapshot> {
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
    build_table_browser_cell_window_projection(&table, &dataset_path, &request)
}

#[uniffi::export]
pub fn build_table_browser_cell_value(
    request: TableBrowserCellValueRequest,
) -> FrontendResult<String> {
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
    Ok(format_table_browser_copy_value(value))
}

fn build_table_browser_cell_window_projection(
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

fn parse_table_browser_content_mode(
    value: &str,
) -> Result<casars_tablebrowser_protocol::BrowserContentMode, String> {
    use casars_tablebrowser_protocol::BrowserContentMode;
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "auto" => Ok(BrowserContentMode::Auto),
        "compact" => Ok(BrowserContentMode::Compact),
        "detailed" => Ok(BrowserContentMode::Detailed),
        other => Err(format!("unknown table browser content mode {other:?}")),
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
    let context = match ms.probe_context() {
        Ok(context) => context,
        Err(_) => return Ok(None),
    };
    let fields: Vec<String> = context
        .fields
        .iter()
        .map(|field| format!("{}: {}", field.row, field.name))
        .collect();
    let spectral_windows: Vec<String> = context
        .spectral_windows
        .iter()
        .map(|spw| {
            format!(
                "spw {}: {} chan, {:.6} GHz center",
                spw.row,
                spw.channel_count,
                spw.center_frequency_hz / 1.0e9
            )
        })
        .collect();
    let antennas: Vec<String> = context
        .antennas
        .iter()
        .map(|antenna| antenna.name.clone())
        .collect();
    let scans = Vec::new();
    let arrays = Vec::new();
    let observations = context
        .observations
        .iter()
        .map(|observation| {
            let detail = [
                observation.project.as_str(),
                observation.telescope_name.as_str(),
            ]
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(", ");
            if detail.is_empty() {
                format!("observation {}", observation.row)
            } else {
                format!("observation {}: {detail}", observation.row)
            }
        })
        .collect();
    let intents = context.intents.clone();
    let feeds = context
        .feed_ids
        .iter()
        .map(|id| format!("feed {id}"))
        .collect();
    let correlations = context.correlations.clone();
    let columns = context.columns.clone();
    let data_columns = context.data_columns.clone();
    let subtables = context
        .subtables
        .iter()
        .map(|subtable| {
            let role = if subtable.required {
                "required"
            } else {
                "optional"
            };
            format!("{} ({role})", subtable.name)
        })
        .collect();
    let row_count = context.row_count;

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
    let probe = ms.probe_uv_range().map_err(|error| error.to_string())?;
    Ok(MeasurementSetUvRangeProbe {
        min_meters: probe.min_meters,
        max_meters: probe.max_meters,
        min_kilolambda: probe.min_kilolambda,
        max_kilolambda: probe.max_kilolambda,
        row_count: probe.row_count,
    })
}

fn probe_measurement_set_time_range_inner(
    dataset_path: &str,
) -> Result<MeasurementSetTimeRangeProbe, String> {
    let ms = MeasurementSet::open(Path::new(dataset_path)).map_err(|error| error.to_string())?;
    let probe = ms.probe_time_range().map_err(|error| error.to_string())?;
    Ok(MeasurementSetTimeRangeProbe {
        min_seconds: probe.min_seconds,
        max_seconds: probe.max_seconds,
        row_count: probe.row_count,
    })
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

    fn table_browser_request(path: &Path, selected_view: &str) -> TableBrowserSnapshotRequest {
        TableBrowserSnapshotRequest {
            dataset_path: path.display().to_string(),
            width: 100,
            height: 24,
            inspector_height: 8,
            selected_view: selected_view.to_string(),
            focus: "main".to_string(),
            commands: Vec::new(),
            transient_commands: Vec::new(),
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
    fn parameter_edit_projection_uses_the_canonical_measurement_set_selector_corpus() {
        let (_dir, ms_path) = unpack_small_ms();
        let cases = [
            ("timerange", "2020/01/01/00:00:00~2020/01/01/00:01:00"),
            ("uvrange", "0~1klambda"),
            ("spw", "0:0~0^1"),
            ("field", "0"),
            ("antenna", "*"),
            ("scan", "0~1"),
            ("timerange", ""),
        ];
        for (parameter, text) in cases {
            let result = parameter_validate_edit(SurfaceParameterEditRequest {
                surface_id: "msexplore".to_string(),
                parameter: parameter.to_string(),
                text: text.to_string(),
                dataset_path: Some(ms_path.display().to_string()),
                spectral_window_id: None,
                suggestions: Vec::new(),
            })
            .unwrap_or_else(|error| panic!("{parameter} {text:?}: {error}"));
            assert!(
                result.diagnostics.is_empty(),
                "{parameter} {text:?}: {:?}",
                result.diagnostics
            );
        }

        let invalid = parameter_validate_edit(SurfaceParameterEditRequest {
            surface_id: "msexplore".to_string(),
            parameter: "timerange".to_string(),
            text: "not-a-casa-time".to_string(),
            dataset_path: Some(ms_path.display().to_string()),
            spectral_window_id: None,
            suggestions: Vec::new(),
        })
        .expect("typed rejection result");
        assert_eq!(invalid.diagnostics.len(), 1);
        assert_eq!(invalid.diagnostics[0].code, "invalid_selector_syntax");
        assert_eq!(
            invalid.diagnostics[0].parameter.as_deref(),
            Some("timerange")
        );
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
        coords.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, -0.5],
            [-cell_rad, cell_rad],
            [1.5, 1.5],
        ));
        coords.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            100.0e9,
            2.0e6,
            2.0,
            100.0e9,
        ));
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
    fn table_browser_snapshot_uses_typed_protocol_projection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let snapshot = build_table_browser_snapshot(table_browser_request(&table_path, "columns"))
            .expect("table browser snapshot");
        assert_eq!(snapshot.view, "columns");
        assert!(!snapshot.capabilities.editable);
        assert_eq!(snapshot.table_path, table_path.display().to_string());
        assert!(!snapshot.content_lines.is_empty());
    }

    #[test]
    fn table_browser_typed_request_replays_navigation_commands() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let mut request = table_browser_request(&table_path, "columns");
        request
            .commands
            .push(TableBrowserCommand::MoveDown { steps: 1 });
        let snapshot =
            build_table_browser_snapshot(request).expect("table browser requested snapshot");
        assert_eq!(snapshot.view, "columns");
        assert_eq!(snapshot.focus, "main");
        assert_eq!(
            snapshot
                .vertical_metrics
                .expect("vertical metrics")
                .selected_index,
            1
        );
        assert!(snapshot.selected_address.is_some());
        assert!(snapshot.inspector.is_some());
    }

    #[test]
    fn table_browser_cell_window_returns_typed_scroll_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let window = build_table_browser_cell_window(TableBrowserCellWindowRequest {
            dataset_path: table_path.display().to_string(),
            row_start: 0,
            row_limit: 4,
            column_start: 1,
            column_limit: 1,
            column_options: Vec::new(),
        })
        .expect("table browser cell window");

        assert_eq!(window.row_count, 1);
        assert_eq!(window.column_count, 2);
        assert_eq!(window.row_start, 0);
        assert_eq!(window.column_start, 1);
        assert_eq!(window.columns[0].name, "id");
        assert_eq!(window.columns[1].header, "name str");
        assert_eq!(window.rows[0].index, 0);
        assert_eq!(window.rows[0].cells[0].column_index, 1);
        assert_eq!(window.rows[0].cells[0].display, "\"gain\"");
        assert!(window.rows[0].cells[0].defined);
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

        let window = build_table_browser_cell_window(TableBrowserCellWindowRequest {
            dataset_path: table_path.display().to_string(),
            row_start: 0,
            row_limit: 1,
            column_start: 0,
            column_limit: 1,
            column_options: vec![TableBrowserColumnDisplayOption {
                column_index: 0,
                array_inline_limit: 4,
            }],
        })
        .expect("table browser cell window");

        assert_eq!(
            window.columns[0].keywords[0],
            "MEASINFO.type = \"visibility\""
        );
        assert_eq!(
            window.rows[0].cells[0].display,
            "[1.0000, 2.0000, 3.0000, 4.0000]"
        );

        let copy_value = build_table_browser_cell_value(TableBrowserCellValueRequest {
            dataset_path: table_path.display().to_string(),
            row_index: 0,
            column_index: 0,
        })
        .expect("table browser cell value");
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
    fn assistant_typed_bridge_persists_provider_neutral_conversations() {
        let project = tempfile::tempdir().expect("project");
        let project_root = project.path().canonicalize().expect("canonical project");
        let mut created = assistant_create_conversation(AssistantCreateConversationRequest {
            project_root: project_root.display().to_string(),
            title: "Calibration discussion".to_owned(),
            primary_attachment: AssistantAttachmentState {
                kind: "notebook".to_owned(),
                identifier: "Analysis.md".to_owned(),
                label: "Analysis".to_owned(),
                primary: false,
            },
            profile: AssistantSessionProfileState {
                profile_version: 1,
                backend_id: "codex_app_server".to_owned(),
                authority: AssistantAuthorityState::Work,
                model: "gpt-test".to_owned(),
                effort: "medium".to_owned(),
                agent_command: "codex".to_owned(),
                python_command: "python3".to_owned(),
                python_provenance: None,
            },
        })
        .expect("create assistant conversation");
        assert!(created.attachments[0].primary);
        created.draft = "continue here".to_owned();

        assistant_save_conversation(AssistantSaveConversationRequest {
            project_root: project_root.display().to_string(),
            transcript: created,
        })
        .expect("save assistant conversation");

        let conversations = assistant_conversations(project_root.display().to_string())
            .expect("list assistant conversations");
        assert_eq!(conversations[0].draft, "continue here");
        assert_eq!(conversations[0].profile.backend_id, "codex_app_server");

        let protocol = assistant_protocol_info();
        assert_eq!(protocol.retrieval_engine, "sqlite_fts5_unicode61");
        assert_eq!(protocol.authority_presets[1], "work");
        assert_eq!(protocol.project_mcp_tools[0], "corpus.search");
    }

    #[test]
    fn assistant_corpus_bridge_exposes_bounded_cited_search_not_sql() {
        let project = tempfile::tempdir().expect("project");
        let project_root = project.path().canonicalize().expect("canonical project");
        let report = assistant_corpus_index(AssistantCorpusIndexRequest {
            project_root: project_root.display().to_string(),
            documents: vec![AssistantCorpusDocumentRequest {
                id: "rao:calibration".to_owned(),
                layer: "baseline".to_owned(),
                title: "Interferometric calibration".to_owned(),
                source_identity: "rao/calibration.md".to_owned(),
                content:
                    "A complex gain calibrator constrains antenna based amplitudes and phases."
                        .to_owned(),
                citation: AssistantCorpusCitationRequest {
                    label: "Calibration guide".to_owned(),
                    locator: "section 4".to_owned(),
                    source_path: Some("rao/calibration.md".to_owned()),
                    page: None,
                    section: Some("Complex gains".to_owned()),
                    line_start: None,
                    line_end: None,
                    release: None,
                    commit: None,
                },
                redistribution_cleared: true,
            }],
            remove_missing_layers: vec!["baseline".to_owned()],
        })
        .expect("index assistant corpus");
        assert_eq!(report.indexed_documents, 1);

        let hits = assistant_corpus_search(AssistantCorpusSearchRequest {
            project_root: project_root.display().to_string(),
            query: "antenna gain phase calibration".to_owned(),
            limit: 1000,
            layers: Vec::new(),
        })
        .expect("search assistant corpus");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].citation.section.as_deref(), Some("Complex gains"));
        assert!(hits[0].untrusted_evidence);

        let oversized = "x".repeat(MAX_ASSISTANT_CORPUS_QUERY_BYTES + 1);
        let error = assistant_corpus_search(AssistantCorpusSearchRequest {
            project_root: project_root.display().to_string(),
            query: oversized,
            limit: 1,
            layers: Vec::new(),
        })
        .expect_err("oversized query must fail closed");
        assert!(error.to_string().contains("host limit"));
    }

    #[test]
    fn tutorial_typed_action_rejects_fields_that_do_not_belong_to_transition() {
        let project = tempfile::tempdir().expect("project");
        let error = tutorial_acquisition_action(TutorialActionRequest {
            action: TutorialPersistenceAction::Resume,
            project_root: project.path().display().to_string(),
            notebook_id: NotebookId::new().to_string(),
            dataset_id: "science".to_owned(),
            generation: Some(1),
            max_download_bytes: None,
        })
        .expect_err("resume generation must fail closed");

        assert!(error.to_string().contains("does not accept generation"));
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
