// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical calibration task request/result contracts shared by CLI, shell, and Python.

use std::path::PathBuf;

use casa_ms::{MsSelectionSpec, selection::MsSelection};
use casa_provider_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, TaskOperationDescriptor, TaskSemanticContract,
    derived_ui_schema_annotations, merged_components,
};
use schemars::{JsonSchema, schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::managed_output::CalibrationTaskResult;
use crate::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, BandpassSolveCombine,
    BandpassSolveRequest, BandpassType, CalibrationStatsAxis, CalibrationStatsRequest,
    FluxScaleRequest, GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainSolveRequest, GainType, RefAntSelector, calibration_stats, command_schema,
    execute_apply_from_path, export_corrected_data, fluxscale, plan_apply_from_path,
    solve_bandpass_from_path, solve_gain_from_path, summarize_tables,
};

/// Stable protocol name advertised by `calibrate --protocol-info`.
pub const CALIBRATION_TASK_PROTOCOL_NAME: &str = "casa_calibration_task";
/// Stable protocol version advertised by `calibrate --protocol-info`.
pub const CALIBRATION_TASK_PROTOCOL_VERSION: u32 = 1;

/// Version/compatibility information for the JSON task protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CalibrationProtocolInfo {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Binary version implementing the protocol.
    pub binary_version: String,
}

impl CalibrationProtocolInfo {
    /// Build the current calibration protocol descriptor.
    pub fn current() -> Self {
        Self {
            protocol_name: CALIBRATION_TASK_PROTOCOL_NAME.to_string(),
            protocol_version: CALIBRATION_TASK_PROTOCOL_VERSION,
            surface_kind: ProviderSurfaceKind::Task,
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// JSON-schema bundle for the public calibration task protocol.
#[derive(Debug, Clone, Serialize)]
pub struct CalibrationTaskSchemaBundle {
    /// Compatibility descriptor for the request/result schemas.
    pub protocol: CalibrationProtocolInfo,
    /// Canonical semantic task contract.
    pub semantic: TaskSemanticContract,
    /// Shared component schemas reusable across projections.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Derived projection metadata for UI and CLI consumers.
    pub projections: ProviderProjectionMetadata,
    /// JSON schema for [`CalibrationTaskRequest`].
    pub request_schema: RootSchema,
    /// JSON schema for [`CalibrationTaskResult`].
    pub result_schema: RootSchema,
}

impl CalibrationTaskSchemaBundle {
    /// Build the current request/result schema bundle.
    pub fn current() -> Self {
        let request_schema = schema_for!(CalibrationTaskRequest);
        let result_schema = schema_for!(CalibrationTaskResult);
        let ui_schema = serde_json::to_value(command_schema("calibrate"))
            .expect("serialize calibration ui schema projection");
        Self {
            protocol: CalibrationProtocolInfo::current(),
            semantic: TaskSemanticContract {
                request_schema: request_schema.clone(),
                result_schema: result_schema.clone(),
                operations: calibration_task_operations(),
            },
            components: merged_components([&request_schema, &result_schema]),
            annotations: derived_ui_schema_annotations(),
            projections: ProviderProjectionMetadata {
                cli: Some(ProviderCliProjection {
                    machine_actions: ProviderCliMachineActions {
                        ui_schema: Some("--ui-schema".to_string()),
                        json_schema: Some("--json-schema".to_string()),
                        protocol_info: Some("--protocol-info".to_string()),
                        json_run: Some("--json-run <SOURCE>".to_string()),
                        session: None,
                    },
                }),
                ui_schema: Some(ui_schema),
                python: None,
            },
            request_schema,
            result_schema,
        }
    }

    /// Return the launcher/TUI compatibility view projected from the bundle.
    pub fn ui_schema_projection(&self) -> Result<casa_ms::ui_schema::UiCommandSchema, String> {
        let value = self
            .projections
            .ui_schema
            .clone()
            .ok_or_else(|| "missing ui_schema projection".to_string())?;
        serde_json::from_value(value)
            .map_err(|error| format!("parse calibration ui schema: {error}"))
    }
}

fn calibration_task_operations() -> Vec<TaskOperationDescriptor> {
    vec![
        TaskOperationDescriptor {
            name: "summary".to_string(),
            request_kind: "summary".to_string(),
            result_kind: Some("summary".to_string()),
        },
        TaskOperationDescriptor {
            name: "stats".to_string(),
            request_kind: "stats".to_string(),
            result_kind: Some("stats".to_string()),
        },
        TaskOperationDescriptor {
            name: "plan_apply".to_string(),
            request_kind: "plan_apply".to_string(),
            result_kind: Some("plan_apply".to_string()),
        },
        TaskOperationDescriptor {
            name: "execute_apply".to_string(),
            request_kind: "execute_apply".to_string(),
            result_kind: Some("apply".to_string()),
        },
        TaskOperationDescriptor {
            name: "export_corrected_data".to_string(),
            request_kind: "export_corrected_data".to_string(),
            result_kind: Some("export_corrected_data".to_string()),
        },
        TaskOperationDescriptor {
            name: "solve_gain".to_string(),
            request_kind: "solve_gain".to_string(),
            result_kind: Some("solve_gain".to_string()),
        },
        TaskOperationDescriptor {
            name: "solve_bandpass".to_string(),
            request_kind: "solve_bandpass".to_string(),
            result_kind: Some("solve_bandpass".to_string()),
        },
        TaskOperationDescriptor {
            name: "flux_scale".to_string(),
            request_kind: "flux_scale".to_string(),
            result_kind: Some("flux_scale".to_string()),
        },
    ]
}

/// Request for summarizing one or more calibration tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SummaryTaskRequest {
    /// Calibration-table paths to summarize.
    pub paths: Vec<PathBuf>,
}

/// Request for computing statistics over one calibration table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StatsTaskRequest {
    /// Calibration-table path to inspect.
    pub path: PathBuf,
    /// Axis transform used to compute the statistics.
    pub axis: CalibrationStatsAxis,
    /// Complex datacolumn override when the axis transforms complex values.
    pub datacolumn: Option<String>,
    /// Whether flagged values should be included.
    pub use_flags: bool,
}

/// Request for planning an `applycal`-class operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PlanApplyTaskRequest {
    /// MeasurementSet root path.
    pub measurement_set: PathBuf,
    /// Structured MS selection controls.
    #[serde(default)]
    pub selection: MsSelectionSpec,
    /// Ordered calibration-table inputs.
    pub calibration_tables: Vec<ApplyCalibrationTableSpec>,
    /// Whether to apply parallactic-angle correction.
    #[serde(default)]
    pub parang: bool,
}

/// Request for executing an `applycal`-class operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExecuteApplyTaskRequest {
    /// MeasurementSet root path.
    pub measurement_set: PathBuf,
    /// Structured MS selection controls.
    #[serde(default)]
    pub selection: MsSelectionSpec,
    /// Ordered calibration-table inputs.
    pub calibration_tables: Vec<ApplyCalibrationTableSpec>,
    /// Apply/execution mode.
    pub apply_mode: ApplyMode,
    /// Whether to apply parallactic-angle correction.
    #[serde(default)]
    pub parang: bool,
}

/// Request for exporting corrected visibilities into an imaging-ready MS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExportCorrectedDataTaskRequest {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// Structured MS selection controls.
    #[serde(default)]
    pub selection: MsSelectionSpec,
}

/// Request for solving antenna gains.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SolveGainTaskRequest {
    /// MeasurementSet root path.
    pub measurement_set: PathBuf,
    /// Structured MS selection controls.
    #[serde(default)]
    pub selection: MsSelectionSpec,
    /// Output calibration-table path.
    pub output_table: PathBuf,
    /// Gain family to solve.
    pub gain_type: GainType,
    /// Solve mode.
    pub solve_mode: GainSolveMode,
    /// Solution interval.
    pub solve_interval: GainSolveInterval,
    /// Group-combine controls.
    #[serde(default)]
    pub combine: GainSolveCombine,
    /// Reference antenna selector.
    pub refant: RefAntSelector,
    /// Prior calibration tables to pre-apply while solving.
    #[serde(default)]
    pub prior_calibration_tables: Vec<ApplyCalibrationTableSpec>,
    /// Whether to apply parallactic-angle correction.
    #[serde(default)]
    pub parang: bool,
    /// Visibility model source used while solving.
    #[serde(default)]
    pub model_source: GainSolveModelSource,
    /// Whether to normalize average solution amplitudes to unity.
    #[serde(default)]
    pub normalize_average_amplitude: bool,
    /// Point-source Stokes model.
    #[serde(default = "default_smodel")]
    pub smodel: [f32; 4],
}

/// Request for solving bandpass terms.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SolveBandpassTaskRequest {
    /// MeasurementSet root path.
    pub measurement_set: PathBuf,
    /// Structured MS selection controls.
    #[serde(default)]
    pub selection: MsSelectionSpec,
    /// Output calibration-table path.
    pub output_table: PathBuf,
    /// Reference antenna selector.
    pub refant: RefAntSelector,
    /// Prior calibration tables to pre-apply while solving.
    #[serde(default)]
    pub prior_calibration_tables: Vec<ApplyCalibrationTableSpec>,
    /// Whether to apply parallactic-angle correction.
    #[serde(default)]
    pub parang: bool,
    /// Group-combine controls.
    #[serde(default)]
    pub combine: BandpassSolveCombine,
    /// Requested output bandpass family.
    pub band_type: BandpassType,
    /// Whether to normalize the average amplitude.
    #[serde(default)]
    pub normalize_average_amplitude: bool,
    /// Requested amplitude polynomial degree for `BPOLY`.
    #[serde(default = "default_polynomial_degree")]
    pub amplitude_degree: usize,
    /// Requested phase polynomial degree for `BPOLY`.
    #[serde(default = "default_polynomial_degree")]
    pub phase_degree: usize,
    /// Point-source Stokes model.
    #[serde(default = "default_smodel")]
    pub smodel: [f32; 4],
}

/// Canonical calibration task request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum CalibrationTaskRequest {
    /// Summarize one or more calibration tables.
    Summary(SummaryTaskRequest),
    /// Compute statistics over one calibration table.
    Stats(StatsTaskRequest),
    /// Plan an `applycal`-class operation without mutating the MS.
    PlanApply(PlanApplyTaskRequest),
    /// Execute an `applycal`-class operation.
    ExecuteApply(ExecuteApplyTaskRequest),
    /// Export `CORRECTED_DATA` into `DATA` in a new MS.
    ExportCorrectedData(ExportCorrectedDataTaskRequest),
    /// Solve antenna gains.
    SolveGain(SolveGainTaskRequest),
    /// Solve bandpass terms.
    SolveBandpass(SolveBandpassTaskRequest),
    /// Bootstrap flux density scaling.
    FluxScale(FluxScaleRequest),
}

impl CalibrationTaskRequest {
    /// Execute the request and return the canonical task result envelope.
    pub fn execute(&self) -> Result<CalibrationTaskResult, String> {
        match self {
            Self::Summary(request) => {
                if request.paths.is_empty() {
                    return Err("summary requires at least one calibration-table path".to_string());
                }
                let path_refs = request
                    .paths
                    .iter()
                    .map(PathBuf::as_path)
                    .collect::<Vec<_>>();
                summarize_tables(path_refs)
                    .map(CalibrationTaskResult::Summary)
                    .map_err(|error| error.to_string())
            }
            Self::Stats(request) => calibration_stats(
                &request.path,
                &CalibrationStatsRequest {
                    axis: request.axis.clone(),
                    datacolumn: request.datacolumn.clone(),
                    use_flags: request.use_flags,
                },
            )
            .map(CalibrationTaskResult::Stats)
            .map_err(|error| error.to_string()),
            Self::PlanApply(request) => plan_apply_from_path(
                &request.measurement_set,
                &ApplyPlanRequest {
                    selection: selection_from_spec(&request.selection)?,
                    apply_mode: ApplyMode::Trial,
                    parang: request.parang,
                    calibration_tables: request.calibration_tables.clone(),
                },
            )
            .map(CalibrationTaskResult::PlanApply)
            .map_err(|error| error.to_string()),
            Self::ExecuteApply(request) => execute_apply_from_path(
                &request.measurement_set,
                &ApplyPlanRequest {
                    selection: selection_from_spec(&request.selection)?,
                    apply_mode: request.apply_mode,
                    parang: request.parang,
                    calibration_tables: request.calibration_tables.clone(),
                },
            )
            .map(CalibrationTaskResult::Apply)
            .map_err(|error| error.to_string()),
            Self::ExportCorrectedData(request) => {
                export_corrected_data(&crate::ExportCorrectedDataRequest {
                    input_ms: request.input_ms.clone(),
                    output_ms: request.output_ms.clone(),
                    selection: selection_from_spec(&request.selection)?,
                })
                .map(CalibrationTaskResult::ExportCorrectedData)
                .map_err(|error| error.to_string())
            }
            Self::SolveGain(request) => solve_gain_from_path(
                &request.measurement_set,
                &GainSolveRequest {
                    selection: selection_from_spec(&request.selection)?,
                    output_table: request.output_table.clone(),
                    gain_type: request.gain_type,
                    solve_mode: request.solve_mode,
                    solve_interval: request.solve_interval,
                    combine: request.combine,
                    refant: request.refant.clone(),
                    prior_calibration_tables: request.prior_calibration_tables.clone(),
                    parang: request.parang,
                    model_source: request.model_source,
                    normalize_average_amplitude: request.normalize_average_amplitude,
                    smodel: request.smodel,
                },
            )
            .map(CalibrationTaskResult::SolveGain)
            .map_err(|error| error.to_string()),
            Self::SolveBandpass(request) => solve_bandpass_from_path(
                &request.measurement_set,
                &BandpassSolveRequest {
                    selection: selection_from_spec(&request.selection)?,
                    output_table: request.output_table.clone(),
                    refant: request.refant.clone(),
                    prior_calibration_tables: request.prior_calibration_tables.clone(),
                    parang: request.parang,
                    combine: request.combine,
                    band_type: request.band_type,
                    normalize_average_amplitude: request.normalize_average_amplitude,
                    amplitude_degree: request.amplitude_degree,
                    phase_degree: request.phase_degree,
                    smodel: request.smodel,
                },
            )
            .map(CalibrationTaskResult::SolveBandpass)
            .map_err(|error| error.to_string()),
            Self::FluxScale(request) => fluxscale(request)
                .map(CalibrationTaskResult::FluxScale)
                .map_err(|error| error.to_string()),
        }
    }
}

fn default_smodel() -> [f32; 4] {
    [1.0, 0.0, 0.0, 0.0]
}

fn default_polynomial_degree() -> usize {
    3
}

pub(crate) fn selection_from_spec(spec: &MsSelectionSpec) -> Result<MsSelection, String> {
    if !spec.selectdata {
        return Ok(MsSelection::new());
    }

    if let Some(value) = first_non_empty(&spec.uvrange) {
        return Err(format!(
            "selection field uvrange is not supported by calibrate tasks yet: {value}"
        ));
    }
    if let Some(value) = first_non_empty(&spec.correlation) {
        return Err(format!(
            "selection field correlation is not supported by calibrate tasks yet: {value}"
        ));
    }
    if let Some(value) = first_non_empty(&spec.intent) {
        return Err(format!(
            "selection field intent is not supported by calibrate tasks yet: {value}"
        ));
    }
    if let Some(value) = first_non_empty(&spec.feed) {
        return Err(format!(
            "selection field feed is not supported by calibrate tasks yet: {value}"
        ));
    }

    let mut selection = MsSelection::new();
    if let Some(field) = first_non_empty(&spec.field) {
        selection = selection.field(&parse_i32_list("field", field)?);
    }
    if let Some(spw) = first_non_empty(&spec.spw) {
        selection = selection.spw(&parse_i32_list("spw", spw)?);
    }
    if let Some(antenna) = first_non_empty(&spec.antenna) {
        selection = selection.antenna(&parse_i32_list("antenna", antenna)?);
    }
    if let Some(scan) = first_non_empty(&spec.scan) {
        selection = selection.scan(&parse_i32_list("scan", scan)?);
    }
    if let Some(observation) = first_non_empty(&spec.observation) {
        selection = selection.observation(&parse_i32_list("observation", observation)?);
    }
    if let Some(array) = first_non_empty(&spec.array) {
        selection = selection.array(&parse_i32_list("array", array)?);
    }
    if let Some(timerange) = first_non_empty(&spec.timerange) {
        let (start, end) = parse_time_range(timerange)?;
        selection = selection.time_range(start, end);
    }
    if let Some(msselect) = first_non_empty(&spec.msselect) {
        selection = selection.taql(msselect);
    }
    Ok(selection)
}

fn first_non_empty(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_i32_list(flag: &str, value: &str) -> Result<Vec<i32>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            segment.parse::<i32>().map_err(|error| {
                format!("failed to parse {flag} value {segment:?} as integer: {error}")
            })
        })
        .collect()
}

fn parse_time_range(value: &str) -> Result<(f64, f64), String> {
    let mut parts = value.split(':').map(str::trim);
    let start = parts
        .next()
        .ok_or_else(|| "timerange requires START:END".to_string())?;
    let end = parts
        .next()
        .ok_or_else(|| "timerange requires START:END".to_string())?;
    if parts.next().is_some() {
        return Err("timerange requires exactly one ':' separator".to_string());
    }
    let start = start
        .parse::<f64>()
        .map_err(|error| format!("failed to parse timerange start {start:?}: {error}"))?;
    let end = end
        .parse::<f64>()
        .map_err(|error| format!("failed to parse timerange end {end:?}: {error}"))?;
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use casa_provider_contracts::ProviderSurfaceKind;

    use super::{
        CALIBRATION_TASK_PROTOCOL_NAME, CALIBRATION_TASK_PROTOCOL_VERSION, CalibrationProtocolInfo,
        CalibrationTaskRequest, CalibrationTaskResult, CalibrationTaskSchemaBundle,
        SummaryTaskRequest,
    };
    use crate::{
        CalibrationColumnSummary, CalibrationIssueSeverity, CalibrationKeywordSummary,
        CalibrationParameterFamily, CalibrationSubtableSummary, CalibrationTableSummary,
        CalibrationValidationIssue,
    };

    fn sample_summary() -> CalibrationTableSummary {
        CalibrationTableSummary {
            path: PathBuf::from("phase.gcal"),
            table_type: "Calibration".into(),
            table_subtype: "G Jones".into(),
            row_count: 2,
            columns: vec!["TIME".into(), "CPARAM".into()],
            keywords: CalibrationKeywordSummary {
                par_type: Some("Complex".into()),
                vis_cal: Some("G Jones".into()),
                ms_name: Some("dataset.ms".into()),
                pol_basis: Some("Circular".into()),
                casa_version: Some("6.7.0".into()),
            },
            subtables: vec![CalibrationSubtableSummary {
                name: "FIELD".into(),
                stored_reference: Some("Table: FIELD".into()),
                resolved_path: Some(PathBuf::from("phase.gcal/FIELD")),
                exists: true,
                row_count: Some(1),
                open_error: None,
            }],
            parameter_family: CalibrationParameterFamily::Complex,
            parameter_column: CalibrationColumnSummary {
                parameter_column: Some("CPARAM".into()),
                parameter_primitive_type: Some("Complex".into()),
                first_cell_shape: Some(vec![2, 1]),
            },
            field_ids: vec![0],
            spectral_window_ids: vec![0],
            antenna1_ids: vec![0, 1],
            antenna2_ids: vec![0],
            observation_ids: vec![0],
            time_coverage: None,
            issues: vec![CalibrationValidationIssue {
                code: "warn/test".into(),
                severity: CalibrationIssueSeverity::Warning,
                message: "fixture warning".into(),
            }],
        }
    }

    #[test]
    fn protocol_info_matches_public_constants() {
        let info = CalibrationProtocolInfo::current();
        assert_eq!(info.protocol_name, CALIBRATION_TASK_PROTOCOL_NAME);
        assert_eq!(info.protocol_version, CALIBRATION_TASK_PROTOCOL_VERSION);
        assert_eq!(info.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(info.binary_version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn calibration_task_request_roundtrips_through_json() {
        let request = CalibrationTaskRequest::Summary(SummaryTaskRequest {
            paths: vec![PathBuf::from("phase.gcal"), PathBuf::from("bandpass.bcal")],
        });
        let encoded = serde_json::to_string(&request).expect("serialize request");
        let decoded =
            serde_json::from_str::<CalibrationTaskRequest>(&encoded).expect("deserialize request");
        assert_eq!(decoded, request);
    }

    #[test]
    fn calibration_task_result_roundtrips_through_json() {
        let result = CalibrationTaskResult::Summary(vec![sample_summary()]);
        let encoded = serde_json::to_string(&result).expect("serialize result");
        let decoded =
            serde_json::from_str::<CalibrationTaskResult>(&encoded).expect("deserialize result");
        assert_eq!(decoded, result);
    }

    #[test]
    fn schema_bundle_uses_current_protocol_and_definitions() {
        let bundle = CalibrationTaskSchemaBundle::current();
        assert_eq!(
            bundle.protocol.protocol_name,
            CALIBRATION_TASK_PROTOCOL_NAME
        );
        assert_eq!(
            bundle.protocol.protocol_version,
            CALIBRATION_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations.len(), 8);
        assert!(
            bundle
                .semantic
                .operations
                .iter()
                .any(|operation| operation.request_kind == "flux_scale")
        );
        assert!(bundle.components.contains_key("SummaryTaskRequest"));
        assert!(bundle.projections.ui_schema.is_some());
        assert!(
            bundle
                .request_schema
                .schema
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.title.as_deref())
                == Some("CalibrationTaskRequest")
        );
        assert!(
            bundle
                .request_schema
                .definitions
                .contains_key("SummaryTaskRequest")
        );
        assert!(
            bundle
                .result_schema
                .schema
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.title.as_deref())
                == Some("CalibrationTaskResult")
        );
        assert!(
            bundle
                .result_schema
                .definitions
                .contains_key("CalibrationTableSummary")
        );
        let ui_schema = bundle.ui_schema_projection().expect("ui schema projection");
        assert_eq!(ui_schema.command_id, "calibrate");
    }
}
