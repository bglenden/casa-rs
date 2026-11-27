// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical calibration task request/result contracts shared by CLI, shell, and Python.

use std::path::PathBuf;

use casa_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata,
    ProviderProtocolDescriptor, ProviderSurfaceKind, TaskOperationDescriptor, TaskProviderContract,
    TaskProviderSchemas, TaskSemanticContract, builtin_surface_bundle, merged_components,
};
use casa_ms::MsSelection;
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

use crate::calibration::managed_output::CalibrationTaskResult;
use casa_calibration::{
    apply::{
        ApplyArgumentError, ApplyCalibrationTableSpec, ApplyExecutionOptions, ApplyMode,
        ApplyPlanRequest, plan_apply_from_path,
    },
    bandpass::{
        BandpassFrequencyInterval, BandpassSolveCombine, BandpassSolveCuts, BandpassSolveRequest,
        BandpassType,
    },
    fluxscale::FluxScaleRequest,
    gencal::GencalRequest,
    solve::{
        CalibrationDataset, CalibrationSolveRequest, CalibrationSolveResult, GainNormalization,
        GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
        GainSplineControls, GainType, RefAntSelector,
    },
    table::{CalibrationStatsAxis, CalibrationStatsRequest, calibration_stats, summarize_tables},
};

/// Stable protocol name advertised by `calibrate --protocol-info`.
pub const CALIBRATION_TASK_PROTOCOL_NAME: &str = "casa_calibration_task";
/// Stable protocol version advertised by `calibrate --protocol-info`.
pub const CALIBRATION_TASK_PROTOCOL_VERSION: u32 = 2;

/// The surfaces `calibrate` hosts, with the default surface first.
pub const CALIBRATION_SURFACES: [&str; 38] = [
    "calibrate",
    "applycal",
    "gaincal",
    "fringefit",
    "accor",
    "blcal",
    "pccor",
    "appendantab",
    "bandpass",
    "fluxscale",
    "gencal",
    "wvrgcal",
    "polcal",
    "polfromgain",
    "getantposalma",
    "getcalmodvla",
    "getephemtable",
    "getjyperkalma",
    "clearcal",
    "delmod",
    "uvsub",
    "ft",
    "setjy",
    "uvmodelfit",
    "smoothcal",
    "rerefant",
    "plotbandpass",
    "plotweather",
    "initweights",
    "statwt",
    "calstat",
    "listcal",
    "sdcal",
    "sdgaincal",
    "sdsmooth",
    "sdbaseline",
    "sdfit",
    "sdatmcor",
];

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
            front_end: None,
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: serde_json::json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: Some("--json-run <SOURCE>".to_string()),
                },
            }),
            python: None,
        },
        parameter_surfaces: CALIBRATION_SURFACES
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GencalTaskRequest {
    /// Input, output and parameters of the prior calibration table.
    #[serde(flatten)]
    pub prior: GencalRequest,
    /// Ordered CASA receptor selector. Empty applies the value to both receptors.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub polarization: String,
    /// Explicit CASA gain-curve `infile`, which uses unmodified antenna names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gaincurve_infile: Option<PathBuf>,
    /// Explicit CASA ALMA antenna-position JSON `infile`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub antpos_infile: Option<PathBuf>,
    /// Float CASA image of vertical TEC in longitude, latitude and time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tec_image: Option<PathBuf>,
    /// Optional updates to the EOPs recorded in the MeasurementSet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eop_infile: Option<PathBuf>,
    /// Request an automatic VLA lookup, with the optional cutoff in days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ant_pos_time_limit: Option<i64>,
    /// Fill the full antenna grid when importing SYSCAL. CASA defaults to true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uniform: Option<bool>,
}

impl GencalTaskRequest {
    pub(crate) fn generate(
        &self,
        mode: casa_calibration::output::CalibrationOutputMode,
    ) -> Result<casa_calibration::gencal::GencalReport, casa_calibration::gencal::GencalError> {
        casa_calibration::gencal::gencal_with_output_mode(
            &self.prior,
            &self.polarization,
            self.infile(),
            self.ant_pos_time_limit,
            self.uniform.unwrap_or(true),
            mode,
        )
    }

    pub(crate) fn infile(&self) -> Option<&std::path::Path> {
        match self.prior.caltype {
            casa_calibration::gencal::GencalType::Antpos => self.antpos_infile.as_deref(),
            casa_calibration::gencal::GencalType::Tecim => self.tec_image.as_deref(),
            casa_calibration::gencal::GencalType::Eop => self.eop_infile.as_deref(),
            _ => self.gaincurve_infile.as_deref(),
        }
    }
}

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
    /// Callibrary parse failure raised after CASA-compatible MS setup/selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callibrary_error: Option<String>,
    /// A traditional argument error raised after CASA-compatible setup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_error: Option<ApplyArgumentError>,
    /// Apply/execution mode.
    pub apply_mode: ApplyMode,
    /// Whether to apply parallactic-angle correction.
    #[serde(default)]
    pub parang: bool,
    /// Whether to save the flags to the next `applycal_N` flag version before
    /// a `calflag` run writes, as CASA does (default true).
    #[serde(default = "default_flagbackup")]
    pub flagbackup: bool,
}

impl ExecuteApplyTaskRequest {
    pub(crate) fn execute_apply(
        &self,
    ) -> Result<
        casa_calibration::apply::ApplyExecutionReport,
        Box<casa_calibration::apply::ApplyExecutionError>,
    > {
        let error = self
            .callibrary_error
            .as_deref()
            .map(casa_calibration::apply::ApplyPreparationError::Callibrary)
            .or(self
                .argument_error
                .as_ref()
                .map(casa_calibration::apply::ApplyPreparationError::Argument));
        casa_calibration::apply::execute_apply_with_preparation_error(
            &self.measurement_set,
            &ApplyPlanRequest {
                selection: self.selection.clone(),
                apply_mode: self.apply_mode,
                parang: self.parang,
                calibration_tables: self.calibration_tables.clone(),
            },
            ApplyExecutionOptions {
                flag_backup: self.flagbackup,
            },
            error,
        )
        .map_err(Box::new)
    }
}

fn default_flagbackup() -> bool {
    true
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
    /// Append compatible solutions instead of replacing the table.
    #[serde(default)]
    pub append: bool,
    /// Gain family to solve.
    pub gain_type: GainType,
    /// Explicit controls for gaincal's legacy spline solve.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spline_controls: Option<GainSplineControls>,
    /// Delay and rate search controls for the fringefit task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fringe_controls: Option<casa_calibration::solve::FringeControls>,
    /// Solve mode.
    pub solve_mode: GainSolveMode,
    /// Solution interval.
    pub solve_interval: GainSolveInterval,
    /// Positive averaging interval in seconds before the gain solve.
    #[serde(default)]
    pub preaverage_seconds: f64,
    /// Preserve each correlation's flags independently when gathering data.
    #[serde(default)]
    pub correlation_dependent_flags: bool,
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
    /// Statistic used when normalization is enabled.
    #[serde(default)]
    pub normalization_type: GainNormalization,
    /// Optional CASA robust weights and outlier rejection.
    #[serde(default)]
    pub robustness: casa_calibration::solve::GainSolveRobustness,
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
    /// Append compatible solutions instead of replacing the table.
    #[serde(default)]
    pub append: bool,
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
    /// Source of the visibility model.
    #[serde(default)]
    pub model_source: GainSolveModelSource,
    /// Time interval used to group channelized solutions.
    #[serde(default)]
    pub solve_interval: GainSolveInterval,
    /// Optional frequency interval used to group selected channels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frequency_interval: Option<BandpassFrequencyInterval>,
    /// Keep independent flags on each correlation during the solve.
    #[serde(default)]
    pub correlation_dependent_flags: bool,
    /// Point-source Stokes model.
    #[serde(default = "default_smodel")]
    pub smodel: [f32; 4],
    /// Per-channel cuts and the `BPOLY` edge mask, or none.
    #[serde(default)]
    pub cuts: Option<BandpassSolveCuts>,
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
    /// Normalize sampler amplitudes using antenna autocorrelations.
    SolveAccor(SolveGainTaskRequest),
    /// Solve a CASA baseline Mueller calibration.
    SolveBaseline {
        /// Shared visibility and calibration selections.
        gain: SolveGainTaskRequest,
        /// Keep a solution for each selected frequency bin.
        frequency_dependent: bool,
        /// Optional grouping of selected channels by count or bandwidth.
        frequency_interval: Option<BandpassFrequencyInterval>,
    },
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
        self.execute_with_output_mode(None)
    }

    pub(crate) fn execute_with_output_mode(
        &self,
        mode: Option<casa_calibration::output::CalibrationOutputMode>,
    ) -> Result<CalibrationTaskResult, String> {
        use casa_calibration::output::CalibrationOutputMode::{Append, Create, Replace, Update};
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
            Self::ExecuteApply(request) => request
                .execute_apply()
                .map(CalibrationTaskResult::Apply)
                .map_err(|error| error.to_string()),
            Self::ExportCorrectedData(request) => {
                casa_calibration::corrected_export::export_corrected_data_with_overwrite(
                    &casa_calibration::corrected_export::ExportCorrectedDataRequest {
                        input_ms: request.input_ms.clone(),
                        output_ms: request.output_ms.clone(),
                        selection: request.selection.clone(),
                    },
                    mode == Some(Replace),
                )
                .map(CalibrationTaskResult::ExportCorrectedData)
                .map_err(|error| error.to_string())
            }
            Self::SolveGain(request)
            | Self::SolveAccor(request)
            | Self::SolveBaseline { gain: request, .. } => {
                let gain = GainSolveRequest {
                    selection: request.selection.clone(),
                    output_table: request.output_table.clone(),
                    gain_type: request.gain_type,
                    solve_mode: request.solve_mode,
                    solve_interval: request.solve_interval,
                    preaverage_seconds: request.preaverage_seconds,
                    correlation_dependent_flags: request.correlation_dependent_flags,
                    combine: request.combine,
                    refant: request.refant.clone(),
                    prior_calibration_tables: request.prior_calibration_tables.clone(),
                    parang: request.parang,
                    model_source: request.model_source,
                    normalize_average_amplitude: request.normalize_average_amplitude,
                    normalization_type: request.normalization_type,
                    robustness: request.robustness.clone(),
                    min_snr: request.min_snr,
                    min_baselines_per_antenna: request.min_baselines_per_antenna,
                    smodel: request.smodel,
                };
                let solve = match (self, request.spline_controls, request.fringe_controls) {
                    (Self::SolveAccor(_), _, _) => CalibrationSolveRequest::Accor(gain),
                    (
                        Self::SolveBaseline {
                            frequency_dependent,
                            frequency_interval,
                            ..
                        },
                        _,
                        _,
                    ) => CalibrationSolveRequest::Baseline {
                        gain,
                        frequency_dependent: *frequency_dependent,
                        frequency_interval: *frequency_interval,
                    },
                    (_, _, Some(controls)) => CalibrationSolveRequest::Fringe { gain, controls },
                    (_, Some(controls), _) => {
                        CalibrationSolveRequest::GainSpline { gain, controls }
                    }
                    _ => CalibrationSolveRequest::Gain(gain),
                };
                casa_calibration::solve::solve_calibration_with_output_mode(
                    CalibrationDataset::path(&request.measurement_set),
                    solve,
                    mode.unwrap_or(if request.append { Append } else { Replace }),
                )
                .map(|result| match result {
                    CalibrationSolveResult::Gain(report) => report,
                    _ => unreachable!("gain request result"),
                })
                .map(CalibrationTaskResult::SolveGain)
                .map_err(|error| error.to_string())
            }
            Self::SolveBandpass(request) => {
                casa_calibration::solve::solve_calibration_with_output_mode(
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
                        model_source: request.model_source,
                        solve_interval: request.solve_interval,
                        frequency_interval: request.frequency_interval,
                        correlation_dependent_flags: request.correlation_dependent_flags,
                        smodel: request.smodel,
                        cuts: request.cuts,
                    }),
                    mode.unwrap_or(if request.append { Append } else { Replace }),
                )
                .map(|result| match result {
                    CalibrationSolveResult::Bandpass(report) => report,
                    _ => unreachable!("bandpass request result"),
                })
                .map(CalibrationTaskResult::SolveBandpass)
                .map_err(|error| error.to_string())
            }
            Self::FluxScale(request) => casa_calibration::fluxscale::fluxscale_with_output_mode(
                request,
                mode.unwrap_or(if request.append { Append } else { Create }),
            )
            .map(CalibrationTaskResult::FluxScale)
            .map_err(|error| error.to_string()),
            Self::Gencal(request) => request
                .generate(mode.unwrap_or(Update))
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

    use casa_contracts::ProviderSurfaceKind;

    use super::{
        CALIBRATION_TASK_PROTOCOL_NAME, CALIBRATION_TASK_PROTOCOL_VERSION, CalibrationTaskRequest,
        CalibrationTaskResult, SummaryTaskRequest, calibration_protocol_descriptor,
        calibration_task_schema_bundle,
    };
    use casa_calibration::table::{
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
        assert_eq!(bundle.semantic.operations.len(), 9);
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
                "applycal",
                "gaincal",
                "fringefit",
                "accor",
                "blcal",
                "pccor",
                "appendantab",
                "bandpass",
                "fluxscale",
                "gencal",
                "wvrgcal",
                "polcal",
                "polfromgain",
                "getantposalma",
                "getcalmodvla",
                "getephemtable",
                "getjyperkalma",
                "clearcal",
                "delmod",
                "uvsub",
                "ft",
                "setjy",
                "uvmodelfit",
                "smoothcal",
                "rerefant",
                "plotbandpass",
                "plotweather",
                "initweights",
                "statwt",
                "calstat",
                "listcal",
                "sdcal",
                "sdgaincal",
                "sdsmooth",
                "sdbaseline",
                "sdfit",
                "sdatmcor",
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
            bundle.parameter_surfaces.len()
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
    }
}
