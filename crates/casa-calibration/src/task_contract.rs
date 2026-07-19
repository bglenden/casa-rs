// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical calibration task request/result contracts shared by CLI, shell, and Python.

use std::path::PathBuf;

use casa_ms::MsSelection;
use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskSemanticContract,
    builtin_surface_bundle, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

use crate::managed_output::CalibrationTaskResult;
use crate::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, BandpassSolveCombine,
    BandpassSolveRequest, BandpassType, CalibrationDataset, CalibrationSolveRequest,
    CalibrationSolveResult, CalibrationStatsAxis, CalibrationStatsRequest,
    ContinuumSubtractionDataColumn, ContinuumSubtractionRequest, FluxScaleRequest,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
    GainType, GencalRequest, RefAntSelector, calibration_stats, continuum_subtract,
    execute_apply_from_path, export_corrected_data, fluxscale, gencal, plan_apply_from_path,
    solve_calibration, summarize_tables,
};

/// Stable protocol name advertised by `calibrate --protocol-info`.
pub const CALIBRATION_TASK_PROTOCOL_NAME: &str = "casa_calibration_task";
/// Stable protocol version advertised by `calibrate --protocol-info`.
pub const CALIBRATION_TASK_PROTOCOL_VERSION: u32 = 1;

/// Build the current shared calibration protocol descriptor.
pub fn calibration_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        CALIBRATION_TASK_PROTOCOL_NAME,
        CALIBRATION_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

/// Build the current calibration schema bundle with the shared envelope.
pub fn calibration_task_schema_bundle() -> TaskProviderContract {
    let request_schema = schema_for!(CalibrationTaskRequest);
    let result_schema = schema_for!(CalibrationTaskResult);
    TaskProviderContract {
        protocol: calibration_protocol_descriptor(),
        semantic: TaskSemanticContract {
            request_schema: request_schema.clone(),
            result_schema: result_schema.clone(),
            operations: calibration_task_operations(),
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: serde_json::json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: Some("--json-run <SOURCE>".to_string()),
                    session: None,
                },
            }),
            python: None,
        },
        parameter_surfaces: [
            "calibrate",
            "uvcontsub",
            "applycal",
            "gaincal",
            "bandpass",
            "fluxscale",
            "gencal",
        ]
        .into_iter()
        .map(|surface| {
            builtin_surface_bundle(surface).unwrap_or_else(|error| {
                panic!("built-in calibration parameter surface {surface:?}: {error}")
            })
        })
        .collect(),
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
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
            name: "continuum_subtract".to_string(),
            request_kind: "continuum_subtract".to_string(),
            result_kind: Some("continuum_subtract".to_string()),
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
        TaskOperationDescriptor {
            name: "gencal".to_string(),
            request_kind: "gencal".to_string(),
            result_kind: Some("gencal".to_string()),
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

/// Request for generating one externally specified calibration table.
pub type GencalTaskRequest = GencalRequest;

/// Request for planning an `applycal`-class operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PlanApplyTaskRequest {
    /// MeasurementSet root path.
    pub measurement_set: PathBuf,
    /// Structured MS selection controls.
    #[serde(default)]
    pub selection: MsSelection,
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
    pub selection: MsSelection,
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
    pub selection: MsSelection,
}

/// Request for UV continuum subtraction into an imaging-ready MS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ContinuumSubtractionTaskRequest {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// CASA-style line-free channel selector, e.g. `0:0~500;900~1919`.
    pub fit_spw: String,
    /// Polynomial order fitted independently to real and imaginary visibilities.
    #[serde(default)]
    pub fit_order: usize,
    /// Input data column to subtract.
    #[serde(default)]
    pub data_column: ContinuumSubtractionDataColumn,
    /// Structured MS selection controls.
    #[serde(default)]
    pub selection: MsSelection,
}

/// Request for solving antenna gains.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SolveGainTaskRequest {
    /// MeasurementSet root path.
    pub measurement_set: PathBuf,
    /// Structured MS selection controls.
    #[serde(default)]
    pub selection: MsSelection,
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
    /// Minimum solution SNR required to keep a solved parameter unflagged.
    #[serde(default = "default_min_snr")]
    pub min_snr: f32,
    /// Minimum unflagged baselines per antenna required before solving.
    #[serde(default = "default_min_baselines_per_antenna")]
    pub min_baselines_per_antenna: usize,
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
    pub selection: MsSelection,
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
    /// Fit and subtract UV continuum into a new MS.
    ContinuumSubtract(ContinuumSubtractionTaskRequest),
    /// Solve antenna gains.
    SolveGain(SolveGainTaskRequest),
    /// Solve bandpass terms.
    SolveBandpass(SolveBandpassTaskRequest),
    /// Bootstrap flux density scaling.
    FluxScale(FluxScaleRequest),
    /// Generate a prior calibration table.
    Gencal(GencalTaskRequest),
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
                    selection: request.selection.clone(),
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
                    selection: request.selection.clone(),
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
                    selection: request.selection.clone(),
                })
                .map(CalibrationTaskResult::ExportCorrectedData)
                .map_err(|error| error.to_string())
            }
            Self::ContinuumSubtract(request) => continuum_subtract(&ContinuumSubtractionRequest {
                input_ms: request.input_ms.clone(),
                output_ms: request.output_ms.clone(),
                fit_spw: request.fit_spw.clone(),
                fit_order: request.fit_order,
                data_column: request.data_column,
                selection: request.selection.clone(),
            })
            .map(CalibrationTaskResult::ContinuumSubtract)
            .map_err(|error| error.to_string()),
            Self::SolveGain(request) => solve_calibration(
                CalibrationDataset::path(&request.measurement_set),
                CalibrationSolveRequest::Gain(GainSolveRequest {
                    selection: request.selection.clone(),
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
                    min_snr: request.min_snr,
                    min_baselines_per_antenna: request.min_baselines_per_antenna,
                    smodel: request.smodel,
                }),
            )
            .map(|result| match result {
                CalibrationSolveResult::Gain(report) => report,
                CalibrationSolveResult::Bandpass(_) => unreachable!("gain request result"),
            })
            .map(CalibrationTaskResult::SolveGain)
            .map_err(|error| error.to_string()),
            Self::SolveBandpass(request) => solve_calibration(
                CalibrationDataset::path(&request.measurement_set),
                CalibrationSolveRequest::Bandpass(BandpassSolveRequest {
                    selection: request.selection.clone(),
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
                }),
            )
            .map(|result| match result {
                CalibrationSolveResult::Bandpass(report) => report,
                CalibrationSolveResult::Gain(_) => unreachable!("bandpass request result"),
            })
            .map(CalibrationTaskResult::SolveBandpass)
            .map_err(|error| error.to_string()),
            Self::FluxScale(request) => fluxscale(request)
                .map(CalibrationTaskResult::FluxScale)
                .map_err(|error| error.to_string()),
            Self::Gencal(request) => gencal(request)
                .map(CalibrationTaskResult::Gencal)
                .map_err(|error| error.to_string()),
        }
    }
}

fn default_smodel() -> [f32; 4] {
    [1.0, 0.0, 0.0, 0.0]
}

fn default_min_snr() -> f32 {
    3.0
}

fn default_min_baselines_per_antenna() -> usize {
    4
}

fn default_polynomial_degree() -> usize {
    3
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use casa_provider_contracts::ProviderSurfaceKind;

    use super::{
        CALIBRATION_TASK_PROTOCOL_NAME, CALIBRATION_TASK_PROTOCOL_VERSION, CalibrationTaskRequest,
        CalibrationTaskResult, SummaryTaskRequest, calibration_protocol_descriptor,
        calibration_task_schema_bundle,
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
        let info = calibration_protocol_descriptor();
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
        let bundle = calibration_task_schema_bundle();
        bundle.validate().expect("shared provider envelope");
        assert_eq!(
            bundle.protocol.protocol_name,
            CALIBRATION_TASK_PROTOCOL_NAME
        );
        assert_eq!(
            bundle.protocol.protocol_version,
            CALIBRATION_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations.len(), 10);
        assert!(
            bundle
                .semantic
                .operations
                .iter()
                .any(|operation| operation.request_kind == "continuum_subtract")
        );
        assert!(
            bundle
                .semantic
                .operations
                .iter()
                .any(|operation| operation.request_kind == "flux_scale")
        );
        assert!(
            bundle
                .semantic
                .operations
                .iter()
                .any(|operation| operation.request_kind == "gencal")
        );
        assert!(bundle.components.contains_key("SummaryTaskRequest"));
        assert!(bundle.projections.cli.is_some());
        assert_eq!(
            bundle
                .parameter_surfaces
                .iter()
                .map(|surface| surface.surface.id())
                .collect::<Vec<_>>(),
            [
                "calibrate",
                "uvcontsub",
                "applycal",
                "gaincal",
                "bandpass",
                "fluxscale",
                "gencal",
            ]
        );
        assert!(
            bundle
                .parameter_surfaces
                .iter()
                .all(|surface| surface.validate().is_ok())
        );
        assert_eq!(
            serde_json::to_value(&bundle).unwrap()["parameter_surfaces"]
                .as_array()
                .unwrap()
                .len(),
            7
        );
        assert!(
            bundle
                .domain_schemas
                .request_schema
                .schema
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.title.as_deref())
                == Some("CalibrationTaskRequest")
        );
        assert!(
            bundle
                .domain_schemas
                .request_schema
                .definitions
                .contains_key("SummaryTaskRequest")
        );
        assert!(
            bundle
                .domain_schemas
                .result_schema
                .schema
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.title.as_deref())
                == Some("CalibrationTaskResult")
        );
        assert!(
            bundle
                .domain_schemas
                .result_schema
                .definitions
                .contains_key("CalibrationTableSummary")
        );
        let form = casa_provider_contracts::project_ui_form(&bundle.parameter_surfaces[0]);
        assert_eq!(form["command_id"], "calibrate");
    }
}
