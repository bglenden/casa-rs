// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration table schemas, lookup, statistics and publication.

pub use crate::accumulate::{CalibrationAccumulationRequest, accumulate_calibration};
pub use crate::calanalysis::{CalAnalysis, CalAnalysisFit, CalAnalysisIds, CalAnalysisQuery};
pub use crate::legacy_spline::LegacySplineTable;
pub use crate::lookup::{
    CalibrationTableLayout, CalibrationTableLookup, ComplexSolution, FieldSelection,
    LOOKUP_KEPT_ROW_BYTES, LOOKUP_LOADING_ROW_BYTES, TimeInterpolation, TimeInterpolationOptions,
};
pub use crate::lookup_frequency::FrequencyInterpolation;
pub use crate::model::{
    CalibrationColumnSummary, CalibrationIssueSeverity, CalibrationKeywordSummary,
    CalibrationParameterFamily, CalibrationSubtableSummary, CalibrationTableSummary,
    CalibrationValidationIssue, TimeCoverageSummary,
};
pub use crate::simulated::{
    SIMULATED_TABLE_ROW_BYTES, SIMULATED_TABLE_VALUE_BYTES, SimulatedJones, SimulatedNoiseSolution,
    SimulatedSolution, SimulatedTableMetadata, SolutionValues, check_simulated_table_destination,
    simulated_table_bytes, write_simulated_jones_table, write_simulated_noise_table,
};
pub use crate::stats::{
    CalibrationIndexedStats, CalibrationStatsAxis, CalibrationStatsError, CalibrationStatsReport,
    CalibrationStatsRequest, CalibrationValueStats, calibration_stats,
};
pub use crate::summary::{CalibrationTableError, summarize_table, summarize_tables};
pub use crate::table_maintenance::{create_calibration_table, update_calibration_table};
pub use crate::writer::CalibrationTableWriteError;
