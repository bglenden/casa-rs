// SPDX-License-Identifier: LGPL-3.0-or-later
//! UniFFI service layer of the casa-rs Python package.
//!
//! `casars.parameters` resolves, renders and saves task parameters through
//! these exports, `casars.tasks` decodes task completions and resolves the
//! application catalog, and `casars.data` builds typed MeasurementSet plot
//! documents. `scripts/generate-frontend-bindings.sh` regenerates the Python
//! module `casars._frontend` from this crate's shared library.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_ms::plot::UvCoverageSeries;
use casa_ms::{
    MeasurementSetPlotPayload, MeasurementSetSummaryOutputFormat, MsExploreSpec, MsPageExportRange,
    MsPlotData, MsPlotDataPanel, MsPlotPayload, MsPlotPreset, MsPlotSpec, MsScatterGridPayload,
    MsScatterPagePayload, MsScatterPlotPayload, MsScatterSeries, MsSelection,
    build_msexplore_payload_from_spec,
};
use casa_provider_contracts::{
    ParameterValue, ProviderInvocationAdaptation, RunProductKind, RunProductRole,
    SurfaceContractBundle, builtin_application_catalog, builtin_surface_bundle,
    builtin_surface_catalog,
};
use casa_task_runtime::{
    BaseSource, DiagnosticCode, ManagedProfileKind, ManagedStateStore, OpenSessionRequest,
    ParameterRuntime, ParameterSession, ResolutionPatch, TaskOutputValue, decode_task_completion,
    parse_profile, project_provider_invocation, render_documented_template,
    write_parameter_profile_atomic,
};
use serde::Serialize;
use thiserror::Error;

const DEFAULT_MAX_PLOT_POINTS: u64 = 250_000;
const FRONTEND_POINT_PROVENANCE_LIMIT: usize = 8_000;

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
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TaskCompletionProjection {
    pub surface_id: String,
    pub summary: String,
    pub products: Vec<TaskCompletionProduct>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct ApplicationCatalogEnvelope {
    pub schema_version: u64,
    pub applications: Vec<ApplicationCatalogEntry>,
}

/// One application-catalog entry: its identity and how to launch it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, uniffi::Record)]
pub struct ApplicationCatalogEntry {
    pub id: String,
    pub kind: String,
    pub category: String,
    pub display_name: String,
    pub executable: String,
    pub cargo_package: String,
    pub override_env: String,
    pub dataset_kinds: Vec<String>,
    pub include_in_suite: bool,
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
    pub max_plot_points: u64,
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

/// A typed MeasurementSet plot document and its metadata; Python renders it
/// with Matplotlib.
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
}

#[derive(Debug, Error, uniffi::Error)]
pub enum FrontendServiceError {
    #[error("invalid path: {reason}")]
    InvalidPath { reason: String },
    #[error("probe failed: {reason}")]
    Probe { reason: String },
    #[error("plot failed: {reason}")]
    Plot { reason: String },
    #[error("parameter service failed: {reason}")]
    Parameters { reason: String },
    #[error("task completion failed: {reason}")]
    TaskCompletion { reason: String },
}

type FrontendResult<T> = Result<T, FrontendServiceError>;

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
                dataset_kinds: application.dataset_kinds.clone(),
                include_in_suite: application.include_in_suite,
            })
        })
        .collect::<FrontendResult<Vec<_>>>()?;
    Ok(ApplicationCatalogEnvelope {
        schema_version: u64::from(catalog.schema_version),
        applications,
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
pub struct SurfaceProviderInvocation {
    pub protocol_name: Option<String>,
    pub protocol_version: Option<u32>,
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

/// Load Last or Last Successful from the managed store, if present.
#[uniffi::export]
pub fn parameter_last(
    surface_id: String,
    workspace: String,
    successful: bool,
) -> FrontendResult<Option<SurfaceParameterSnapshot>> {
    // Reject an unknown surface before looking for its managed state.
    parameter_bundle(&surface_id)?;
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

/// Project one complete task invocation for the CLI and Python.
#[uniffi::export]
pub fn parameter_provider_invocation(
    surface_id: String,
    values: HashMap<String, SurfaceParameterValue>,
) -> FrontendResult<SurfaceProviderInvocation> {
    let session = parameter_session_from_typed_values(&surface_id, values)?;
    session
        .render_sparse()
        .map_err(|error| parameter_error("validate provider invocation parameters", error))?;
    let provider_family = session.bundle().surface.provider_family().to_string();
    let invocation = project_provider_invocation(&session, |family, values, direct| match family {
        "imager" => casars_imager::imager_provider_invocation(values, direct.args),
        "simobserve" => casars_simulation::simobserve_provider_invocation(values, direct.args),
        _ => Ok(ProviderInvocationAdaptation::direct(direct)),
    })
    .map_err(|error| parameter_error("project provider invocation", error))?;
    let descriptor = match provider_family.as_str() {
        "imager" => Some(casars_imager::imager_protocol_descriptor()),
        "simobserve" => Some(casars_simulation::simobserve_protocol_descriptor()),
        _ => None,
    };
    let (protocol_name, protocol_version) = descriptor.map_or((None, None), |descriptor| {
        (
            Some(descriptor.protocol_name),
            Some(descriptor.protocol_version),
        )
    });
    Ok(SurfaceProviderInvocation {
        protocol_name,
        protocol_version,
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

/// Decode one successful provider result into its contract-declared products.
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
        .map(|product| TaskCompletionProduct {
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
            diagnostic: product.diagnostic,
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

    let max_plot_points = if request.max_plot_points == 0 {
        DEFAULT_MAX_PLOT_POINTS
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
    })
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

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
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

uniffi::setup_scaffolding!();

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::{self, File};

    use flate2::read::GzDecoder;
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

    fn defaults_snapshot(surface_id: &str) -> SurfaceParameterSnapshot {
        parameter_resolve(
            surface_id.to_string(),
            SurfaceParameterBaseSource::Defaults,
            None,
            None,
            SurfaceParameterPatch {
                values: HashMap::new(),
                unset: Vec::new(),
            },
            SurfaceParameterPatch {
                values: HashMap::new(),
                unset: Vec::new(),
            },
        )
        .expect("defaults")
    }

    fn profile_snapshot(
        surface_id: &str,
        profile_toml: &str,
        path: &Path,
    ) -> SurfaceParameterSnapshot {
        parameter_resolve(
            surface_id.to_string(),
            SurfaceParameterBaseSource::File,
            Some(profile_toml.to_string()),
            Some(path.to_string_lossy().into_owned()),
            SurfaceParameterPatch {
                values: HashMap::new(),
                unset: Vec::new(),
            },
            SurfaceParameterPatch {
                values: HashMap::new(),
                unset: Vec::new(),
            },
        )
        .expect("profile resolution")
    }

    #[test]
    fn application_catalog_exposes_canonical_frontend_projection() {
        let catalog = application_catalog().expect("application catalog");
        let applications = catalog.applications;
        assert_eq!(applications.len(), 28);
        assert!(
            applications
                .iter()
                .all(|application| application.id != "tablebrowser")
        );
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
    fn parameter_catalog_projects_every_task_surface() {
        let catalog = parameter_catalog().expect("parameter catalog");
        assert_eq!(catalog.surfaces.len(), 27);
        let imstat = parameter_surface_bundle("imstat".to_string()).expect("imstat bundle");
        assert_eq!(imstat.surface.kind, "task");
        assert!(imstat.surface.bindings.iter().any(|binding| {
            binding.name == "imagename" && binding.concept.id == "image.input.imagename"
        }));
        assert!(parameter_surface_bundle("imexplore".to_string()).is_err());
    }

    #[test]
    fn parameter_resolution_preserves_context_and_override_origins() {
        let defaults = defaults_snapshot("imager");
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
    fn parameter_provider_invocation_carries_casas_simobserve_parameters() {
        let values = HashMap::from([(
            "antennalist".to_string(),
            SurfaceParameterValue::String {
                value: "vla.a.cfg".to_string(),
            },
        )]);
        let invocation = parameter_provider_invocation("simobserve".to_string(), values)
            .expect("simobserve invocation");
        assert_eq!(
            invocation.args,
            ["--managed-output", "true", "--json-run", "-"]
        );
        let request: serde_json::Value =
            serde_json::from_str(invocation.stdin.as_deref().expect("stdin JSON")).unwrap();
        assert_eq!(request["kind"], "run");
        assert_eq!(request["request"]["antennalist"], "vla.a.cfg");
        assert_eq!(request["request"]["project"], "sim");
        assert_eq!(request["request"]["thermalnoise"], "tsys-atm");
        assert_eq!(request["request"]["mapsize"], serde_json::json!(["", ""]));
    }

    #[test]
    fn parameter_provider_invocation_carries_the_imager_parameters() {
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
                "parallel".to_string(),
                SurfaceParameterValue::Bool { value: false },
            ),
        ]);
        let invocation =
            parameter_provider_invocation("imager".to_string(), values).expect("imager invocation");
        assert_eq!(
            invocation.args,
            ["--managed-output", "true", "--json-run", "-"]
        );
        assert_eq!(
            invocation.protocol_name.as_deref(),
            Some("casa_imager_task")
        );
        assert_eq!(
            invocation.protocol_version,
            Some(casars_imager::IMAGER_TASK_PROTOCOL_VERSION)
        );
        let request: serde_json::Value =
            serde_json::from_str(invocation.stdin.as_deref().expect("stdin JSON")).unwrap();
        assert_eq!(request["kind"], "run");
        assert_eq!(request["request"]["vis"], serde_json::json!(["input.ms"]));
        assert_eq!(request["request"]["imagename"], "products/image");
        assert_eq!(request["request"]["parallel"], false);
    }

    #[test]
    fn one_profile_matches_runtime_and_typed_uniffi_resolution() {
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
                ParameterValue::Integer(12150),
                ParameterValue::Integer(12150),
            ]))
        );
        assert_eq!(
            direct.states()["cell"].value,
            Some(ParameterValue::Array(vec![
                ParameterValue::String("0.6arcsec".to_string()),
                ParameterValue::String("0.6arcsec".to_string()),
            ]))
        );

        let uniffi_snapshot = profile_snapshot("imager", profile_toml, &source_path);
        assert_eq!(
            uniffi_snapshot.states["imsize"].value,
            Some(SurfaceParameterValue::Array {
                values: vec![
                    SurfaceParameterValue::Integer { value: 12150 },
                    SurfaceParameterValue::Integer { value: 12150 },
                ],
            })
        );
        assert_eq!(
            uniffi_snapshot.diagnostics.len(),
            direct.diagnostics().len()
        );
        let invocation = parameter_provider_invocation(
            "imager".to_string(),
            uniffi_snapshot
                .states
                .iter()
                .filter_map(|(name, state)| state.value.clone().map(|value| (name.clone(), value)))
                .collect(),
        )
        .expect("Python-facing canonical provider invocation");
        let expected: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../resources/test-profiles/imager-cross-surface.expected.json"
        )))
        .expect("shared imager request baseline");
        let request: serde_json::Value = serde_json::from_str(
            invocation
                .stdin
                .as_deref()
                .expect("canonical imager request stdin"),
        )
        .expect("canonical imager request JSON");
        assert_eq!(request, expected["request"]);
        assert_eq!(
            invocation.protocol_name.as_deref(),
            Some("casa_imager_task")
        );
        assert_eq!(
            invocation.protocol_version,
            Some(casars_imager::IMAGER_TASK_PROTOCOL_VERSION)
        );
        let canonical_profile = uniffi_snapshot
            .profile_toml
            .as_deref()
            .expect("canonical profile TOML");
        assert!(canonical_profile.contains("imsize = [12150, 12150]"));
        assert!(canonical_profile.contains("cell = [\"0.6arcsec\", \"0.6arcsec\"]"));
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
        let loaded = profile_snapshot("imager", &profile_toml, &profile_path);
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
        let written = parameter_write_managed(
            "imager".to_string(),
            workspace.to_string_lossy().into_owned(),
            values.clone(),
            false,
        )
        .expect("write Last");
        assert!(written.path.ends_with("last.toml"), "{}", written.path);

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
            parameter_last(
                "imager".to_string(),
                workspace.to_string_lossy().into_owned(),
                true,
            )
            .expect("look up Last Successful")
            .is_none()
        );
        parameter_write_managed(
            "imager".to_string(),
            workspace.to_string_lossy().into_owned(),
            values,
            true,
        )
        .expect("write Last Successful");
        assert!(
            parameter_last(
                "imager".to_string(),
                workspace.to_string_lossy().into_owned(),
                true,
            )
            .expect("load Last Successful")
            .is_some()
        );

        assert!(
            parameter_last(
                "imexplore".to_string(),
                workspace.to_string_lossy().into_owned(),
                false,
            )
            .is_err(),
            "removed surfaces are unknown"
        );
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
    fn measurement_set_plot_builds_a_typed_document_for_every_preset() {
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
    fn task_completion_reports_the_declared_product() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain.cal");
        fs::create_dir(&table_path).expect("product directory");
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
        assert!(product.exists);
        assert!(product.diagnostic.is_none());
    }
}
