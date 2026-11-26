// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-table compatibility support for `casa-rs`.
//!
//! This crate starts the calibration workflow substrate planned for
//! `casars`. The first wave focuses on reading and validating CASA-produced
//! calibration tables, normalising the metadata needed for later apply/solve
//! work, and emitting machine-readable summaries that can anchor slow parity
//! tests against CASA.
//!
//! The public API is intentionally narrow in the first wave:
//!
//! - [`summarize_table`] opens a calibration table written by CASA or Rust and
//!   returns a normalized [`CalibrationTableSummary`].
//! - [`summarize_tables`] batches the same operation for CLI callers.
//! - [`CalibrationTableSummary::supported_for_v1_apply`] indicates whether the
//!   table falls inside the initial `Complex`/`CPARAM` family targeted by the
//!   upcoming `applycal`-class workflow.
//!
//! The implementation is built on [`casa_tables::Table`] rather than a new
//! storage stack, so every on-disk assumption made here is exercised against
//! the same table reader/writer substrate used elsewhere in the repo.

mod accumulate;
mod antpos_vla;
pub mod appendantab;
pub mod attachment;
mod bandpass;
mod calanalysis;
mod calibrater_lifecycle;
pub mod calibration_service;
pub mod callib;
pub mod constants;
pub mod continuum;
pub mod corrected_export;
pub mod execute;
mod flagdata_model;
pub mod fluxscale;
pub mod ft;
pub mod gencal;
#[cfg(test)]
mod imaging_memory_test;
pub mod initweights;
pub mod interpolation;
mod legacy_spline;
pub mod listing;
mod lookup;
mod lookup_frequency;
mod model;
pub mod model_columns;
pub mod output;
mod output_merge;
pub mod paths;
pub mod pccor;
pub mod plan;
#[doc(hidden)]
pub mod plotms;
pub mod polfromgain;
pub mod rerefant;
mod selected_data;
pub mod session;
mod simulated;
pub mod single_dish;
pub use single_dish::preparation::atmosphere::SingleDishAtmosphere;
pub use single_dish::preparation::baseline::{
    SingleDishBaseline, SingleDishBaselineBasis, SingleDishBaselineDiagnostics,
    SingleDishBaselineMode, SingleDishLineFinder, SingleDishSinusoid,
};
pub use single_dish::preparation::import::{import_asap, import_nro};
pub use single_dish::preparation::line_fit::SingleDishLineFit;
pub use single_dish::preparation::{SingleDishSession, SingleDishSmoothing};
pub mod smooth;
mod solution_edit;
pub mod solve;
pub mod solve_stats;
mod stats;
pub mod statwt;
mod summary;
pub mod table_maintenance;
pub mod timing;
mod transform_calibration;
pub mod transform_models;
mod uvmodelfit;
mod uvsub;
pub mod weather;
mod weight_spectra;
mod writer;
mod wvr;

pub use flagdata_model::flagdata_model;
pub use ft::predictcomp::{
    PredictedComponent, predict_calibrator_component, predict_solar_component_2012,
};
pub use ft::settings::{
    ImagerGridder, ImagerLegacyAwOptions, ImagerLegacyPbWOptions, ImagerSingleDish,
    ImagerSingleDishKernel, ImagerSingleDishSetup, ImagerTransformOptions,
};
pub use ft::{
    ImagerDefinitionRequest, ImagerImageDefinition, ImagerPhaseCentre, ImagerPrediction,
    ImagerPredictionModel, ImagerSpectralAxis, ImagerVoltagePattern,
};
pub use statwt::{
    StatWtAlgorithm, StatWtChannelBin, StatWtCombine, StatWtDataColumn, StatWtMs, StatWtOptions,
    StatWtSummary, StatWtTimeBin, estimate_statwt, statwt_weight_range,
};
pub use wvr::{
    WvrAntennaReport, WvrError, WvrReport, WvrRequest, WvrSelectors, execute_wvr,
    execute_wvr_with_log, resolve_wvr_selectors,
};

pub use uvsub::{
    subtract_visibility_model, subtract_visibility_model_holding,
    subtract_visibility_model_selected, visibility_model_source_paths,
};

pub use accumulate::{CalibrationAccumulationRequest, accumulate_calibration};
pub use attachment::{
    AttachmentError, AttachmentMode, AttachmentOptions, CalibrationAttachment, ImagerAttachments,
    ImagerChannelSelection,
};
pub use calanalysis::{CalAnalysis, CalAnalysisFit, CalAnalysisIds, CalAnalysisQuery};
pub use calibrater_lifecycle::{CalibraterLifecycle, CalibraterLifecycleError};
pub use listing::CalibrationListingReport;
pub use output::CalibrationOutputMode;
pub use single_dish::tool::SingleDishSolveControls;
pub use table_maintenance::{create_calibration_table, update_calibration_table};
pub use transform_calibration::transform_callibrary;
pub use transform_models::{transform_model_input, visibility_model_input};

pub use bandpass::{
    BandpassFrequencyInterval, BandpassSolveCombine, BandpassSolveCuts, BandpassSolveError,
    BandpassSolveReport, BandpassSolveRequest, BandpassType,
};
pub use callib::{
    CallibError, CallibraryContextMap, CallibraryOptions, load_apply_specs_from_callib,
    load_apply_specs_from_callib_at, load_callibrary_record, parse_apply_specs,
    save_apply_specs_to_callib, validate_callibrary_record,
};
pub use corrected_export::{
    ExportCorrectedDataError, ExportCorrectedDataReport, ExportCorrectedDataRequest,
    export_corrected_data,
};
pub use execute::{
    ApplyExecutionError, ApplyExecutionOptions, ApplyExecutionReport, ApplyExecutionTimings,
    EvaluatedApplyRow, PlotWeights, evaluate_plot_rows, execute_apply, execute_apply_from_path,
    execute_apply_from_path_with_options, execute_apply_with_preparation_error,
    execute_single_dish_apply, legacy_bpoly_chebyshev_value,
};
pub use fluxscale::{
    FluxScaleError, FluxScaleFieldResult, FluxScaleOptions, FluxScaleReport, FluxScaleRequest,
    FluxScaleSpwResult, fluxscale,
};
pub use gencal::{
    AntposInputError, CalibrationSpecification, GencalError, GencalReport, GencalRequest,
    GencalType, gencal, gencal_with_output_mode,
};
pub use legacy_spline::LegacySplineTable;
pub use lookup::{
    CalibrationTableLayout, CalibrationTableLookup, ComplexSolution, FieldSelection,
    LOOKUP_KEPT_ROW_BYTES, LOOKUP_LOADING_ROW_BYTES, TimeInterpolation, TimeInterpolationOptions,
};
pub use lookup_frequency::FrequencyInterpolation;
pub use model::{
    CalibrationColumnSummary, CalibrationIssueSeverity, CalibrationKeywordSummary,
    CalibrationParameterFamily, CalibrationSubtableSummary, CalibrationTableSummary,
    CalibrationValidationIssue, TimeCoverageSummary,
};
pub use model_columns::{
    delete_calibration_model, initialize_hanning_scratch_holding, reset_calibration_set,
};
pub use plan::{
    ApplyArgumentError, ApplyCalibrationTablePlan, ApplyCalibrationTableSpec,
    ApplyChannelSelection, ApplyInterpolationMode, ApplyMode, ApplyPlan, ApplyPlanError,
    ApplyPlanRequest, ApplyPlanTimings, ApplyPreparationError, ApplyRowPlan, ApplySpwMapping,
    ApplyTableSelection, GainFieldSelector, ResolvedGainField, ResolvedNearestGainField,
    SpectralWindowPlan, plan_apply, plan_apply_from_path, plan_apply_with_timings,
};
pub use selected_data::SelectedData;
pub use session::{
    CalibrationDataset, CalibrationError, CalibrationSolveRequest, CalibrationSolveResult,
    solve_calibration, solve_calibration_with_output_mode,
};
pub use simulated::{
    SIMULATED_TABLE_ROW_BYTES, SIMULATED_TABLE_VALUE_BYTES, SimulatedJones, SimulatedNoiseSolution,
    SimulatedSolution, SimulatedTableMetadata, SolutionValues, check_simulated_table_destination,
    simulated_table_bytes, write_simulated_jones_table, write_simulated_noise_table,
};
pub use solve::fringe::{FringeControls, FringeCorrelation};
pub use solve::{
    GainNormalization, GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveMode,
    GainSolveModelSource, GainSolveReport, GainSolveRequest, GainSolveRobustness,
    GainSplineControls, GainType, RefAntSelector, point_model_stokes,
};
pub use solve_stats::{AntennaSolveStats, SolveCounts, SolveStats, WindowSolveStats};
pub use stats::{
    CalibrationIndexedStats, CalibrationStatsAxis, CalibrationStatsError, CalibrationStatsReport,
    CalibrationStatsRequest, CalibrationValueStats, calibration_stats,
};
pub use summary::{CalibrationTableError, summarize_table, summarize_tables};
pub use uvmodelfit::{
    VisibilityComponentFit, VisibilityFitRequest, VisibilityFitShape, fit_visibility_component,
};
pub use writer::CalibrationTableWriteError;
