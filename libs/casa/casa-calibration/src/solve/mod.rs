// SPDX-License-Identifier: LGPL-3.0-or-later
//! Limited `gaincal` support for the first solver wave.
//!
//! This module intentionally implements only a narrow, testable slice:
//!
//! - `gaintype=G|T|K|KCROSS`
//! - `calmode='p|a|ap'`
//! - `solint='inf'|'int'|<seconds>`
//! - explicit reference antenna
//! - point-source Stokes sky model (`smodel=[I,Q,U,V]`) or `MODEL_DATA`
//!
//! The acceptance contract is downstream behavior: the resulting caltable
//! should be CASA-compatible on disk and should yield corrected visibilities
//! close to CASA's own `gaincal` when applied.

pub(crate) mod accor;
pub(crate) mod additive_polynomial;
pub(crate) mod baseline;
#[cfg(test)]
mod batch_trial;
mod delay;
pub(crate) mod fringe;
pub mod grouping;
pub(crate) mod gspline;
pub(crate) mod kernel;
mod model;
mod normal;
#[cfg(unix)]
pub(crate) mod observation_gain;
mod trace;
pub(crate) mod writer;

pub use crate::calibrater_lifecycle::{CalibraterLifecycle, CalibraterLifecycleError};
pub use crate::selected_data::SelectedData;
pub use crate::session::resolve_casa_calibration_selection;
pub use crate::session::{
    CalibrationDataset, CalibrationError, CalibrationSolveRequest, CalibrationSolveResult,
    solve_calibration, solve_calibration_with_output_mode,
};
pub use crate::solve_stats::{AntennaSolveStats, SolveCounts, SolveStats, WindowSolveStats};
pub use fringe::{FringeControls, FringeCorrelation};

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::PathBuf;

use casa_core::{Complex32, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_ms::{ChannelSelection, MsError};
use casa_tables::{Table, TableError, TableOptions};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::session::prepare_calibration_solve;
use crate::solve_stats::{AntennaSolution, IntervalStages, SolveStatsBuilder, ToolSolveActivity};
use crate::{
    apply::{ApplyExecutionError, ApplyPlanError},
    bandpass::BandpassFrequencyInterval,
    output::CalibrationOutputMode,
};
use grouping::{SolveGroupOptions, build_solve_groups};
use kernel::solve_group;
use writer::write_gain_caltable;

/// Supported first-wave gain solve families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainType {
    /// Per-receptor complex gains.
    G,
    /// Polarization-collapsed scalar complex gains.
    T,
    /// Per-receptor delay in nanoseconds.
    K,
    /// Multi-band K delay table.
    KMbd,
    /// Shared cross-hand delay in nanoseconds.
    #[serde(rename = "KCROSS")]
    KCross,
    /// Direct KAntPos table with the antenna delay fit in its first two terms.
    KAntPos,
    /// Time-domain cubic-spline gains per receptor.
    #[serde(rename = "GSPLINE")]
    GSpline,
}

impl GainType {
    pub(crate) fn vis_cal(self) -> &'static str {
        match self {
            Self::G => "G Jones",
            Self::T => "T Jones",
            Self::K => "K Jones",
            Self::KMbd => "KMBD Jones",
            Self::KCross => "Kcross Jones",
            Self::KAntPos => "KAntPos Jones",
            Self::GSpline => "GSPLINE",
        }
    }

    /// The polarizations a solution holds: one gain per receptor for G, one
    /// for both for T.
    pub(crate) fn polarizations(self) -> usize {
        match self {
            Self::G | Self::K | Self::KMbd | Self::KCross | Self::GSpline => 2,
            Self::KAntPos => 3,
            Self::T => 1,
        }
    }

    pub(crate) fn is_delay(self) -> bool {
        matches!(self, Self::K | Self::KMbd | Self::KCross | Self::KAntPos)
    }
}

/// Controls for the GSPLINE mode of gaincal.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GainSplineControls {
    /// Requested interval between spline knots, in seconds.
    pub splinetime: f64,
    /// Number of points in each phase-unwrapping median.
    pub npointaver: i64,
    /// Phase difference that identifies a wrap, in degrees.
    pub phasewrap: f64,
}

impl Default for GainSplineControls {
    fn default() -> Self {
        Self {
            splinetime: 3600.,
            npointaver: 3,
            phasewrap: 180.,
        }
    }
}

/// Visibility and output-gain constraints for `gaincal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainSolveMode {
    /// Phase-only solve.
    Phase,
    /// Amplitude-only solve.
    Amplitude,
    /// Amplitude-and-phase solve.
    AmplitudePhase,
}

/// Statistic used by `gaincal` to normalize a window's solution power.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainNormalization {
    /// Arithmetic mean of the unflagged squared amplitudes.
    #[default]
    Mean,
    /// Median of the unflagged squared amplitudes.
    Median,
}

/// Reweighting and outlier rejection for a routine gain solve.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GainSolveRobustness {
    /// Reweight the samples by their complex residuals in three L1 passes.
    pub l1: bool,
    /// Successive rejection thresholds in multiples of each correlation's RMS.
    pub rms_thresholds: Vec<f32>,
}

impl GainSolveRobustness {
    /// Parse CASA's documented `solmode` and optional `rmsthresh` sequence.
    #[must_use]
    pub fn from_casa(mode: &str, thresholds: Vec<f64>) -> Option<Self> {
        let (l1, reject) = match mode {
            "" => (false, false),
            "L1" => (true, false),
            "R" => (false, true),
            "L1R" => (true, true),
            _ => return None,
        };
        let rms_thresholds = if !reject {
            Vec::new()
        } else if thresholds.is_empty() {
            vec![7.0, 5.0, 4.0, 3.5, 3.0, 2.8, 2.6, 2.4, 2.2, 2.5]
        } else {
            thresholds.into_iter().map(|value| value as f32).collect()
        };
        Some(Self { l1, rms_thresholds })
    }

    pub(crate) fn enabled(&self) -> bool {
        self.l1 || !self.rms_thresholds.is_empty()
    }
}

/// Visibility model source used by the gain solver.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainSolveModelSource {
    /// Use the point-source Stokes model from `smodel`.
    #[default]
    PointSource,
    /// Use per-sample visibilities from the MS `MODEL_DATA` column.
    ModelColumn,
    /// Select the field’s virtual model, MODEL_DATA, or the unit point model.
    FromMeasurementSet,
}

impl GainSolveModelSource {
    /// A stored model or a polarized point must be rotated on the model
    /// side of a diagonal Jones solve to respect the calibration order.
    pub(crate) fn needs_model_rotation(self, stokes: [f32; 4]) -> bool {
        self != Self::PointSource || stokes[1..].iter().any(|value| *value != 0.0)
    }
}

/// Supported first-wave gain solution intervals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum GainSolveInterval {
    /// Solve one solution per `(observation, field, spw, scan)` group.
    #[default]
    Infinite,
    /// Solve one solution per integration timestamp inside each scan.
    Integration,
    /// Solve one solution per contiguous time bucket of the given duration, in seconds.
    Seconds(f64),
}

impl GainSolveInterval {
    /// Read CASA's time component of `solint`, accepting quanta and seconds.
    #[must_use]
    pub fn from_casa(text: &str) -> Option<Self> {
        use casa_core::quanta::{Quantity, Unit};
        let upper = text.to_ascii_uppercase();
        if upper.is_empty() || upper.contains("INF") {
            return Some(Self::Infinite);
        }
        if upper.contains("INT") {
            return Some(Self::Integration);
        }
        let quantity = text.parse::<Quantity>().ok()?;
        let seconds = if quantity.unit().name().is_empty() {
            quantity.value()
        } else {
            quantity.get_value_in(&Unit::new("s").ok()?).ok()?
        };
        if !seconds.is_finite() {
            return None;
        }
        Some(if seconds > 0.0 {
            Self::Seconds(seconds)
        } else if seconds == 0.0 {
            Self::Infinite
        } else {
            Self::Integration
        })
    }
}

/// Data axes omitted as boundaries when forming `gaincal` solution intervals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GainSolveCombine {
    /// Extend solves across scan boundaries.
    pub scans: bool,
    /// Extend solves across field boundaries.
    pub fields: bool,
    /// Extend solves across observation boundaries.
    #[serde(default)]
    pub observations: bool,
    /// Solve jointly across selected spectral windows.
    #[serde(default)]
    pub spectral_windows: bool,
}

impl GainSolveCombine {
    /// Match combine axes by CASA's case-insensitive axis names.
    #[must_use]
    pub fn from_casa(value: &str) -> Self {
        let upper = value.to_ascii_uppercase();
        Self {
            scans: upper.contains("SCAN"),
            fields: upper.contains("FIELD"),
            observations: upper.contains("OBS"),
            spectral_windows: upper.contains("SPW"),
        }
    }
}

/// Reference antenna selector for solve requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum RefAntSelector {
    /// Solve without rereferencing solution phases across intervals.
    Unspecified,
    /// Exact antenna id.
    AntennaId(i32),
    /// Exact ANTENNA.NAME match.
    AntennaName(String),
    /// CASA selection in preference order with an optional strict reference.
    Expression {
        /// CASA's antenna selector and priority list.
        selection: String,
        /// Flag an interval if the first preferred antenna cannot reference it.
        strict: bool,
    },
}

/// Request for a limited `gaincal` solve.
#[derive(Debug, Clone)]
pub struct GainSolveRequest {
    /// MS selection applied before solving.
    pub selection: MsSelection,
    /// Output caltable path.
    pub output_table: PathBuf,
    /// Gain family to solve.
    pub gain_type: GainType,
    /// Solve mode.
    pub solve_mode: GainSolveMode,
    /// Solution interval.
    pub solve_interval: GainSolveInterval,
    /// Positive averaging interval in seconds before the gain solve.
    /// Nonpositive values average over the whole solution interval.
    pub preaverage_seconds: f64,
    /// Preserve each correlation's flags independently when gathering data.
    pub correlation_dependent_flags: bool,
    /// Axes to combine while forming solve groups.
    pub combine: GainSolveCombine,
    /// Reference antenna.
    pub refant: RefAntSelector,
    /// Prior calibration tables to apply on the fly before solving.
    pub prior_calibration_tables: Vec<crate::apply::ApplyCalibrationTableSpec>,
    /// Whether to apply parallactic-angle correction before solving.
    pub parang: bool,
    /// Visibility model source.
    pub model_source: GainSolveModelSource,
    /// Whether to normalize the solution power to unity.
    pub normalize_average_amplitude: bool,
    /// Statistic used when normalization is enabled.
    pub normalization_type: GainNormalization,
    /// Optional CASA robust weights and outlier rejection.
    pub robustness: GainSolveRobustness,
    /// Minimum solution SNR required to keep a solved parameter unflagged.
    pub min_snr: f32,
    /// Minimum unflagged baselines per antenna required before solving.
    pub min_baselines_per_antenna: usize,
    /// Point-source Stokes model used when `model_source` is `PointSource`.
    pub smodel: [f32; 4],
}

/// Solve summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GainSolveReport {
    /// Output caltable path.
    pub output_table: PathBuf,
    /// Gain family that was solved.
    pub gain_type: GainType,
    /// Resolved reference antenna id.
    pub refant_antenna_id: i32,
    /// Distinct fields represented in the solved table.
    pub field_ids: Vec<i32>,
    /// Distinct SPWs represented in the solved table.
    pub spectral_window_ids: Vec<i32>,
    /// Number of solution rows written.
    pub solution_row_count: usize,
    /// The data the solve selected, CASA's `selectvis`.
    #[serde(default)]
    pub selected_data: SelectedData,
    /// How far the solve's intervals got, CASA's `solvestats`.
    #[serde(default)]
    pub solve_stats: SolveStats,
}

/// Errors returned by the limited `gaincal` solver.
#[derive(Debug, Error)]
pub enum GainSolveError {
    /// Opening or traversing the shared observation boundary failed.
    #[error(transparent)]
    Observation(#[from] casa_observation::ObservationError),

    /// A selector could not be resolved against an observation's identities.
    #[error("invalid {parameter} selector: {reason}")]
    ObservationSelector {
        /// The public selector parameter.
        parameter: &'static str,
        /// The selector parser's diagnostic.
        reason: String,
    },

    /// A real opacity solve reached the unavailable fitting stage.
    #[error("The configured opacity type cannot solve a calibration table.")]
    OpacityCannotFit,
    /// A gather-only tool solve found data that would require fitting.
    #[error("The configured calibration family does not implement fitting.")]
    GatherOnlyCannotFit,
    /// Shared calibration-table persistence failed.
    #[error(transparent)]
    CalibrationTable(#[from] crate::table::CalibrationTableWriteError),
    /// Opening the MeasurementSet failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path that was being opened.
        path: String,
        /// Underlying error.
        #[source]
        source: MsError,
    },

    /// The selection produced no rows.
    #[error("gain solve selection produced no rows")]
    EmptySelection,

    /// Two data descriptions of the selection share a spectral window.
    /// CASA solves each description apart and writes each one's solutions
    /// under the window; casa-rs keys a solve by its window and does not
    /// take this layout yet.
    #[error(
        "DATA_DESC_IDs {data_desc_ids:?} share spectral window {spw_id}; casa-rs does not solve data descriptions that share a window yet (#979); select one of them"
    )]
    SharedSpectralWindow {
        /// The shared window.
        spw_id: i32,
        /// The selected data descriptions on it.
        data_desc_ids: Vec<i32>,
    },

    /// The point-source Stokes vector is invalid for the solve.
    #[error("unsupported smodel {smodel:?}")]
    UnsupportedSkyModel {
        /// Model vector passed by the caller.
        smodel: [f32; 4],
    },

    /// A virtual model could not be decoded or evaluated.
    #[error("cannot read virtual model: {0}")]
    VirtualModel(String),

    /// The solve requested `MODEL_DATA`, but the MS does not contain it.
    #[error("gain solve model_source=ModelColumn requires a MODEL_DATA column")]
    MissingModelColumn,

    /// The requested solve interval is unsupported.
    #[error("unsupported solve interval {solve_interval:?}; seconds values must be > 0")]
    UnsupportedSolveInterval {
        /// Interval requested by the caller.
        solve_interval: GainSolveInterval,
    },

    /// A selected delay spectrum cannot form a solution.
    #[error("delay solve: {0}")]
    Delay(String),

    /// The retained Tf factory uses the single-bin scalar T fitter.
    #[error("Tf Jones frequency configuration: {0}")]
    FrequencyScalar(String),

    /// Temporary samples for an iterative polarization solve could not be stored.
    #[error("polarization solve scratch I/O failed: {0}")]
    PolarizationScratch(#[source] std::io::Error),

    /// The cross-hand data cannot constrain both terms of the source fit.
    #[error("Source polarization solution is singular; try solving for D-terms only.")]
    SingularSourcePolarization,
    /// The generic leakage solve does not accept correlation-dependent flags.
    #[error("correlation-dependent flags are unsupported for an iterative leakage solve")]
    LeakageCorrelationFlagging,
    #[error("accumulated data entirely flagged")]
    /// No selected unflagged visibility can constrain the parallactic-angle solution.
    NoUnflaggedParallacticData,
    #[error(
        "the cross-hand phase and Q/U solve requires at least {required} unflagged time segments"
    )]
    /// Too few distinct parallactic-angle samples for the solve.
    InsufficientParallacticCoverage {
        /// Minimum number of independent samples required by the selected solve.
        required: usize,
    },
    #[error("GlinXphJones needs multiple SDBs")]
    /// Too few usable gain-phase segments for the cross-hand solution.
    InsufficientGainPhaseSegments,
    /// Old GlinXph only accepts one accumulated field/window buffer.
    #[error("GlinXphJones can't process multi-vb vbga.")]
    LegacyGainPhaseMultipleBuffers,

    /// Baseline calibration could not be prepared or persisted.
    #[error("{0}")]
    Baseline(String),

    /// A legacy ACCOR gather reached a fit that requires VI2.
    #[error("the legacy visibility iterator cannot fit ACCOR")]
    AccorNeedsModernIterator,

    /// The configured amplitude/phase mode cannot be applied to ACCOR solutions.
    #[error("enforceAPonSoln divide-by-zero error.")]
    AccorInvalidSolveMode,

    /// The reference antenna could not be resolved.
    #[error("failed to resolve reference antenna {selector}: {reason}")]
    ResolveRefAnt {
        /// Caller-visible selector.
        selector: String,
        /// Additional context.
        reason: String,
    },

    /// Planning prior on-the-fly calibration application failed.
    #[error("failed to plan prior calibration application: {source}")]
    PriorCalibrationPlan {
        /// Underlying apply-planning error.
        #[source]
        source: Box<ApplyPlanError>,
    },

    /// Evaluating prior on-the-fly calibration application failed.
    #[error("failed to evaluate prior calibration application: {source}")]
    PriorCalibrationApply {
        /// Underlying apply-execution error.
        #[source]
        source: Box<ApplyExecutionError>,
    },

    /// The MS polarization layout is outside the supported diagonal surface.
    #[error(
        "unsupported correlation layout for DATA_DESC_ID {data_desc_id}: {correlation_types:?}"
    )]
    UnsupportedCorrelationLayout {
        /// Data description id.
        data_desc_id: i32,
        /// Correlation names.
        correlation_types: Vec<String>,
    },

    /// A table/column cell had an unexpected shape.
    #[error("unsupported parameter shape in {path}: {shape:?}")]
    UnsupportedParameterShape {
        /// Logical source.
        path: String,
        /// Discovered shape.
        shape: Vec<usize>,
    },

    /// The solve failed because the selected data do not connect the reference
    /// antenna to at least one solved antenna.
    #[error(
        "gain solve could not determine a phase for antenna {antenna_id} in field={field_id} spw={spw_id}"
    )]
    UnsolvableAntenna {
        /// Antenna id.
        antenna_id: i32,
        /// Field id.
        field_id: i32,
        /// Spectral window id.
        spw_id: i32,
    },

    /// Persisting the output caltable failed.
    #[error("failed to save caltable {path}: {source}")]
    SaveCalibrationTable {
        /// Output path.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Copying an MS subtable into the caltable failed.
    #[error("failed to copy {subtable} subtable into {path}: {source}")]
    CopySubtable {
        /// Subtable name.
        subtable: String,
        /// Destination root.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Filesystem preparation for the output root failed.
    #[error("failed to prepare output path {path}: {reason}")]
    PrepareOutput {
        /// Output path.
        path: String,
        /// Error context.
        reason: String,
    },
}

/// Solve a limited `gaincal` request from an already-open MeasurementSet.
pub(crate) fn solve_gain(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    mode: crate::output::CalibrationOutputMode,
    activity: Option<&mut ToolSolveActivity>,
    legacy_iterator: bool,
) -> Result<GainSolveReport, GainSolveError> {
    if request.gain_type == GainType::GSpline {
        return gspline::solve(ms, request, GainSplineControls::default(), mode, None);
    }
    let solved =
        if legacy_iterator && matches!(request.gain_type, GainType::KMbd | GainType::KAntPos) {
            delay::solve(ms, request, activity, true)?
        } else {
            solve_gain_rows(ms, request, activity, mode)?
        };
    write_gain_caltable(ms, request, &solved, mode)
}

#[derive(Clone, Copy)]
pub(crate) struct TfSolveSettings {
    pub(crate) frequency: Option<BandpassFrequencyInterval>,
}

pub(crate) fn solve_tf(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    settings: TfSolveSettings,
    mode: crate::output::CalibrationOutputMode,
    activity: Option<&mut ToolSolveActivity>,
) -> Result<GainSolveReport, GainSolveError> {
    let (solved, spectrum) =
        solve_gain_rows_with_settings(ms, request, activity, Some(settings), None, false)?;
    let spectrum = spectrum.expect("Tf prepared its spectral grid");
    writer::write_tf_caltable(
        ms,
        request,
        &solved,
        mode,
        writer::GainChannelGrid {
            bin_widths: &spectrum.bin_widths,
            selected_rows: &spectrum.selected_rows,
        },
    )
}

struct TfSpectrum {
    bin_widths: BTreeMap<i32, usize>,
    selected_rows: Vec<grouping::SelectedSolveRow>,
}

fn tf_multi_bin_windows(
    ms: &MeasurementSet,
    rows: &[grouping::SelectedSolveRow],
    widths: &BTreeMap<i32, usize>,
) -> Result<BTreeSet<i32>, GainSolveError> {
    let fail = |error: MsError| GainSolveError::FrequencyScalar(error.to_string());
    let selections = rows
        .iter()
        .map(|row| (row.data_spw_id, row.channel_selection.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let spectral = ms.spectral_window().map_err(fail)?;
    let mut multiple = BTreeSet::new();
    for (spw, selection) in selections {
        let count = spectral.num_chan(spw as usize).map_err(fail)?;
        let selected = grouping::selected_channels(selection, count as usize)?.len();
        if selected > widths.get(&spw).copied().unwrap_or(1) {
            multiple.insert(spw);
        }
    }
    Ok(multiple)
}

/// A gain solve's solutions, before they are written.
pub(crate) struct SolvedGains {
    /// The solution rows, flagged ones included.
    pub(crate) rows: Vec<kernel::SolutionRow>,
    /// The reference antenna.
    pub(crate) refant_id: i32,
    /// The spectral windows an interval reached a solve in, whether or not
    /// it succeeded ([`grouping::SolveAccumulator::attempted`]).
    pub(crate) attempted_spw_ids: BTreeSet<i32>,
    /// The resolved channels used in each selected spectral window.
    pub(crate) channel_selections: BTreeMap<i32, ChannelSelection>,
    /// The data the solve selected.
    pub(crate) selected_data: SelectedData,
    /// How far its intervals got.
    pub(crate) solve_stats: SolveStats,
}

/// The solutions of [`solve_gain`], the resolved reference antenna and the
/// windows a solve reached, before the caltable is written.
pub(crate) fn solve_gain_rows(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    activity: Option<&mut ToolSolveActivity>,
    mode: CalibrationOutputMode,
) -> Result<SolvedGains, GainSolveError> {
    if request.gain_type.is_delay() {
        return delay::solve(ms, request, activity, false);
    }
    let legacy_g_receptors = legacy_gain_receptors(request, mode)?;
    solve_gain_rows_with_settings(ms, request, activity, None, None, legacy_g_receptors)
        .map(|(solved, _)| solved)
}

fn legacy_gain_receptors(
    request: &GainSolveRequest,
    mode: CalibrationOutputMode,
) -> Result<bool, GainSolveError> {
    if request.gain_type == GainType::G
        && mode == CalibrationOutputMode::Append
        && request.output_table.exists()
    {
        let output = Table::open(TableOptions::new(&request.output_table)).map_err(|error| {
            GainSolveError::PrepareOutput {
                path: request.output_table.display().to_string(),
                reason: error.to_string(),
            }
        })?;
        Ok(matches!(
            output.keywords().get(crate::constants::KEY_CASA_VERSION),
            Some(Value::Scalar(ScalarValue::String(tag)))
                if tag == crate::constants::LEGACY_NATIVE_G_VERSION
        ))
    } else {
        Ok(false)
    }
}

pub(crate) struct GatherOnlySolveSettings {
    pub(crate) parameters: usize,
    pub(crate) check_append: bool,
    pub(crate) legacy: bool,
    pub(crate) layout: crate::bandpass::GatherFrequencyLayout,
}

pub(crate) fn gather_only_tool_solve(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    activity: &mut ToolSolveActivity,
    settings: GatherOnlySolveSettings,
) -> Result<SolveStats, GainSolveError> {
    solve_gain_rows_with_settings(ms, request, Some(activity), None, Some(settings), false)
        .map(|(solved, _)| solved.solve_stats)
}

fn solve_point_stokes(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
) -> Result<[f32; 4], GainSolveError> {
    match request.model_source {
        GainSolveModelSource::PointSource => {
            validate_smodel(request.smodel, true)?;
            Ok(request.smodel)
        }
        GainSolveModelSource::ModelColumn
            if !ms
                .main_table()
                .schema()
                .is_some_and(|schema| schema.contains_column("MODEL_DATA")) =>
        {
            Err(GainSolveError::MissingModelColumn)
        }
        _ => Ok([1.0, 0.0, 0.0, 0.0]),
    }
}

fn solve_gain_rows_with_settings(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    activity: Option<&mut ToolSolveActivity>,
    tf: Option<TfSolveSettings>,
    gather_only: Option<GatherOnlySolveSettings>,
    legacy_g_receptors: bool,
) -> Result<(SolvedGains, Option<TfSpectrum>), GainSolveError> {
    let point_stokes = solve_point_stokes(ms, request)?;
    validate_solve_interval(request.solve_interval)?;
    // P Jones is later in CASA's Jones chain than G, so the rotation
    // corrupts the MeasurementSet model or a polarized explicit point
    // before a diagonal gain is fitted.
    let rotate_model = request.parang
        && request.gain_type == GainType::G
        && request.model_source.needs_model_rotation(request.smodel);
    let input_phase = crate::timing::Phase::start("gain_select_read_average");
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &request.refant,
        &request.prior_calibration_tables,
        request.parang,
        true,
        grouping::PriorCalibrationMode::Gain {
            scalar: request.gain_type == GainType::T,
            rotate_model,
            share_input_flags: !request.correlation_dependent_flags,
        },
    )?;
    let tf_widths = tf
        .map(|settings| {
            crate::bandpass::solution_frequency_bin_widths(ms, &context.rows, settings.frequency)
        })
        .transpose()
        .map_err(|error| GainSolveError::FrequencyScalar(error.to_string()))?;
    let multi_bin_windows = tf_widths
        .as_ref()
        .map(|widths| tf_multi_bin_windows(ms, &context.rows, widths))
        .transpose()?
        .unwrap_or_default();
    if let Some(settings) = &gather_only
        && (settings.check_append || request.combine.spectral_windows)
    {
        crate::bandpass::check_gather_frequency(
            ms,
            settings
                .check_append
                .then_some(request.output_table.as_path()),
            &context.rows,
            settings.layout,
            request.combine.spectral_windows,
        )
        .map_err(|error| GainSolveError::FrequencyScalar(error.to_string()))?;
    }
    let input_rows = if let Some(settings) = &gather_only {
        grouping::SolveInputRows::GatherOnlyTool {
            legacy: settings.legacy,
        }
    } else {
        grouping::SolveInputRows::gain(
            activity.is_some() && matches!(request.gain_type, GainType::G | GainType::T),
        )
    };
    // CalSolvingVi2 shares raw flags before applying priors. The solver
    // keeps flags introduced by a prior on their corresponding receptors
    // through reference selection, which may need another antenna.
    let correlation_dependent_flags = request.correlation_dependent_flags
        || (context.preapplied_rows.is_some()
            && gather_only.is_none()
            && matches!(request.gain_type, GainType::G | GainType::T));
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &BTreeMap::new(),
        SolveGroupOptions {
            input_rows,
            gain_type: request.gain_type,
            model_source: request.model_source,
            point_stokes,
            rotate_model,
            solve_interval: request.solve_interval,
            combine: request.combine,
            // VbAvg applies this boundary to either source of the interval.
            preaverage_max_span: preaverage_span(
                request.solve_interval,
                request.preaverage_seconds,
            ),
            // Polarized preapply combines raw flags before applying priors.
            // A newly flagged cross-hand must not invalidate parallel hands.
            correlation_dependent_flags: correlation_dependent_flags
                || context
                    .preapplied_rows
                    .as_ref()
                    .is_some_and(|rows| rows.polarized),
            robust_seed: request.robustness.enabled(),
            legacy_g_receptors,
            channelized: false,
            polynomial_bandpass: false,
        },
    )?;

    if groups.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }

    drop(input_phase);
    let kernel_phase = crate::timing::Phase::start("gain_solve");
    let mut solution_rows = Vec::new();
    let mut attempted_spw_ids = BTreeSet::new();
    let mut legacy_refant_slots = LegacyRefantSlots::new();
    let mut solve_stats = SolveStatsBuilder::new(
        gather_only.map_or_else(
            || request.gain_type.polarizations(),
            |settings| settings.parameters,
        ),
        spectral_window_ids(ms)?,
        &context.available_antennas,
    );
    for ((base_key, bucket_key), mut group) in groups {
        if (group.attempted || group.gather_attempted())
            && (multi_bin_windows.contains(&group.spw_id)
                || (request.combine.spectral_windows && !multi_bin_windows.is_empty()))
        {
            return Err(GainSolveError::FrequencyScalar(format!(
                "SPW {} has more than one frequency bin",
                group.spw_id,
            )));
        }
        if group.attempted {
            attempted_spw_ids.insert(group.spw_id);
        }
        record_legacy_refant_slots(&mut legacy_refant_slots, &group);
        group.finalize_for_solve(request.solve_mode, request.robustness.enabled());
        trace::trace_group(&base_key, &bucket_key, &group, request);
        let Some(group_rows) = solve_group_with_stats(
            group,
            &context.available_antennas,
            request,
            context.refant_id,
            correlation_dependent_flags,
            &mut solve_stats,
        ) else {
            continue;
        };
        trace::trace_solution_rows(
            &base_key,
            &bucket_key,
            &group_rows,
            request,
            context.refant_id,
        );
        solution_rows.extend(group_rows);
    }
    apply_requested_reference(
        ms,
        &mut solution_rows,
        &request.refant,
        &context,
        &legacy_refant_slots,
    )?;
    finish_gain_amplitudes(&mut solution_rows, request);
    tally_references(&solution_rows, &mut solve_stats);
    drop(kernel_phase);
    let mut channel_selections = BTreeMap::new();
    for row in &context.rows {
        if let Some(channels) = &row.channel_selection {
            channel_selections
                .entry(row.data_spw_id)
                .or_insert_with(|| channels.clone());
        }
    }
    solve_stats.track_activity(activity);
    let solved = SolvedGains {
        rows: solution_rows,
        refant_id: context.refant_id,
        attempted_spw_ids,
        channel_selections,
        selected_data: context.selected_data,
        solve_stats: solve_stats.finish(),
    };
    Ok((
        solved,
        tf_widths.map(|bin_widths| TfSpectrum {
            bin_widths,
            selected_rows: context.rows,
        }),
    ))
}

fn finish_gain_amplitudes(rows: &mut [kernel::SolutionRow], request: &GainSolveRequest) {
    // CASA enforces amplitudes on the final gains after referencing and
    // before normalizing across all intervals (`globalPostSolveTinker`).
    if request.solve_mode != GainSolveMode::AmplitudePhase {
        for row in rows.iter_mut() {
            for (gain, flagged) in row.gains.iter_mut().zip(&row.flags) {
                if *flagged {
                    *gain = Complex32::new(1.0, 0.0);
                } else if request.solve_mode == GainSolveMode::Amplitude {
                    let amplitude = gain.norm();
                    *gain = Complex32::new(if amplitude == 0.0 { 1.0 } else { amplitude }, 0.0);
                }
            }
        }
    }
    if request.solve_mode != GainSolveMode::Phase && request.normalize_average_amplitude {
        normalize_gain_solution_amplitudes(rows, request.normalization_type);
    }
}

/// Gather at the shorter positive user preaverage or solution interval.
pub(crate) fn preaverage_span(interval: GainSolveInterval, requested: f64) -> Option<f64> {
    let requested = requested as f32;
    let preaverage = (requested > f32::EPSILON && requested.is_finite()).then_some(requested);
    let seconds = match interval {
        GainSolveInterval::Infinite => preaverage,
        GainSolveInterval::Integration => None,
        GainSolveInterval::Seconds(seconds) => {
            let solint = seconds as f32;
            Some(preaverage.map_or(solint, |preavg| preavg.min(solint)))
        }
    };
    seconds.map(|seconds| f64::from(seconds) * 0.999)
}

/// Every spectral window of `ms`, by id.
pub(crate) fn spectral_window_ids(ms: &MeasurementSet) -> Result<Vec<i32>, GainSolveError> {
    let windows = ms
        .spectral_window()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    Ok((0..windows.row_count())
        .map(|row| i32::try_from(row).expect("window count fits in i32"))
        .collect())
}

fn apply_requested_reference(
    ms: &MeasurementSet,
    rows: &mut [kernel::SolutionRow],
    request: &RefAntSelector,
    context: &crate::session::CalibrationSolveContext,
    legacy_refant_slots: &LegacyRefantSlots,
) -> Result<(), GainSolveError> {
    let preferred = context.refant_id;
    if preferred < 0 {
        return Ok(());
    }
    let strict = matches!(request, RefAntSelector::Expression { strict: true, .. });
    let mut alternates = context.refant_alternates.clone();
    if !strict {
        let last_preferred = alternates.last().copied().unwrap_or(preferred);
        let mut nearby = refant_distance_order(ms, last_preferred)?;
        nearby.retain(|id| *id != preferred && !alternates.contains(id));
        alternates.extend(nearby);
    }
    apply_reference_antenna(rows, preferred, &alternates, strict, legacy_refant_slots);
    Ok(())
}

type LegacyRefantSlots = BTreeMap<i32, [bool; 2]>;

fn record_legacy_refant_slots(slots: &mut LegacyRefantSlots, group: &grouping::SolveAccumulator) {
    if group.legacy_parallel_hands != [false; 2] {
        let window = slots.entry(group.spw_id).or_default();
        for (known, observed) in window.iter_mut().zip(group.legacy_parallel_hands) {
            *known |= observed;
        }
    }
}

fn tally_references(rows: &[kernel::SolutionRow], stats: &mut SolveStatsBuilder) {
    let mut referenced = BTreeSet::new();
    for row in rows {
        if row.antenna_id == row.refant_id
            && row.flags.iter().any(|flagged| !flagged)
            && referenced.insert((row.spw_id, row.time_seconds.to_bits()))
        {
            stats.used_as_refant(row.spw_id, row.antenna_id);
        }
    }
}

/// Solve one complete group whose accumulation is finalized
/// ([`grouping::SolveAccumulator::finalize_for_solve`]). An interval CASA
/// cannot solve gets no rows (`Calibrater::genericGatherAndSolve` keeps a
/// solution only after one succeeds).
pub(crate) fn solve_closed_group(
    group: grouping::SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    request: &GainSolveRequest,
    refant_id: i32,
    correlation_dependent_flags: bool,
) -> Option<Vec<kernel::SolutionRow>> {
    solve_group(
        group,
        available_antennas,
        request.gain_type,
        request.solve_mode,
        request.min_snr,
        kernel::SolveGraphOptions {
            refant_id,
            min_baselines_per_antenna: request.min_baselines_per_antenna,
            group_seed: None,
            correlation_dependent_flags,
            defer_reference: true,
            robustness: request.robustness.enabled().then_some(&request.robustness),
        },
    )
}

fn solve_group_with_stats(
    group: grouping::SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    request: &GainSolveRequest,
    refant_id: i32,
    correlation_dependent_flags: bool,
    stats: &mut SolveStatsBuilder,
) -> Option<Vec<kernel::SolutionRow>> {
    stats.interval(&IntervalStages::of(
        &group,
        request.min_baselines_per_antenna,
    ));
    let spw_id = group.spw_id;
    let rows = solve_closed_group(
        group,
        available_antennas,
        request,
        refant_id,
        correlation_dependent_flags,
    )?;
    stats.record_success(spw_id, true);
    stats.solutions(
        spw_id,
        rows.iter().map(|row| AntennaSolution {
            antenna_id: row.antenna_id,
            passed: row.flags.iter().map(|flagged| !flagged).collect(),
        }),
    );
    Some(rows)
}

/// The antennas other than `refant_id`, nearest it first: the alternates
/// `SolvableVisJones::applyRefAnt` tries when the reference antenna is flagged.
pub(crate) fn refant_distance_order(
    ms: &MeasurementSet,
    refant_id: i32,
) -> Result<Vec<i32>, GainSolveError> {
    let antenna = ms
        .antenna()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    let positions = (0..antenna.row_count())
        .map(|row| antenna.position(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: "ANTENNA POSITION".to_string(),
            source,
        })?;
    Ok(nearest_first(&positions, refant_id))
}

/// The antennas other than `refant_id`, nearest it first, of antennas at
/// `positions` (ITRF metres, indexed by antenna ID).
pub(crate) fn nearest_first(positions: &[[f64; 3]], refant_id: i32) -> Vec<i32> {
    let origin = usize::try_from(refant_id)
        .ok()
        .and_then(|row| positions.get(row))
        .copied()
        .unwrap_or_default();
    let mut order = (0..positions.len())
        .filter(|row| i32::try_from(*row).ok() != Some(refant_id))
        .map(|row| {
            let distance = positions[row]
                .iter()
                .zip(origin)
                .map(|(value, origin)| (value - origin).powi(2))
                .sum::<f64>();
            (distance, row)
        })
        .collect::<Vec<_>>();
    order.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    order
        .into_iter()
        .map(|(_, row)| i32::try_from(row).expect("antenna row count fits in i32"))
        .collect()
}

/// Refer each solution interval's phases to a reference antenna, as
/// `SolvableVisJones::applyRefAnt` does with `refantmode='flex'`. Intervals
/// go in time order within each spectral window. The reference is the first
/// of: the preferred antenna, the one used last, then the others nearest the
/// preferred first, that has every receptor unflagged both here and in the
/// last referred interval (only the first receptor when the window's second
/// is never solved, or only the second for a legacy YY/LL-only window).
/// Its phase change since that interval is removed from
/// every antenna, so an alternate is held at constant phase. An interval
/// with no such antenna is flagged entirely and leaves the last referred
/// interval in place for the next.
pub(crate) fn apply_reference_antenna(
    rows: &mut [kernel::SolutionRow],
    preferred_refant_id: i32,
    alternates: &[i32],
    strict: bool,
    legacy_refant_slots: &LegacyRefantSlots,
) {
    // CASA `applyRefAnt` iterates by window and exact caltable time, even
    // where separate solve intervals contributed rows at the same time.
    let mut group_indices = BTreeMap::<_, Vec<_>>::new();
    for (index, row) in rows.iter().enumerate() {
        group_indices
            .entry((row.spw_id, row.time_seconds.to_bits()))
            .or_default()
            .push(index);
    }
    let mut receptor_counts = BTreeMap::<i32, usize>::new();
    for row in rows.iter() {
        let count = receptor_counts.entry(row.spw_id).or_insert(1);
        if row.flags.get(1).is_some_and(|flag| !*flag) {
            *count = 2;
        }
    }

    let mut choices = Vec::with_capacity(alternates.len() + 2);
    choices.extend([preferred_refant_id, preferred_refant_id]);
    choices.extend_from_slice(alternates);

    let mut previous = None::<Vec<usize>>;
    let mut last_spw = None::<i32>;
    for ((spw_id, _), current) in group_indices {
        if last_spw != Some(spw_id) {
            previous = None;
            last_spw = Some(spw_id);
        }
        let receptors = if legacy_refant_slots.get(&spw_id) == Some(&[false, true]) {
            1..2
        } else {
            0..receptor_counts[&spw_id]
        };
        let previous_indices = previous.as_ref().unwrap_or(&current);
        let candidates = if strict { &choices[..1] } else { &choices[..] };
        let Some((current_ref_index, previous_ref_index)) =
            find_flex_refant(rows, candidates, &current, previous_indices, receptors)
        else {
            for index in &current {
                rows[*index].flags.iter_mut().for_each(|flag| *flag = true);
                rows[*index].refant_id = -1;
            }
            continue;
        };

        let first = previous.is_none();
        let ref_phasor = rows[current_ref_index]
            .gains
            .iter()
            .zip(rows[previous_ref_index].gains.iter())
            .map(|(current_gain, previous_gain)| {
                let current_phase = phase_only(*current_gain);
                if first {
                    current_phase
                } else {
                    current_phase / phase_only(*previous_gain)
                }
            })
            .collect::<Vec<_>>();

        let current_refant = rows[current_ref_index].antenna_id;
        choices[1] = current_refant;
        for index in &current {
            for (gain, phasor) in rows[*index].gains.iter_mut().zip(ref_phasor.iter()) {
                if phasor.norm() > f32::EPSILON {
                    *gain /= *phasor;
                }
            }
            rows[*index].refant_id = current_refant;
        }
        previous = Some(current);
    }
}

fn find_flex_refant(
    rows: &[kernel::SolutionRow],
    choices: &[i32],
    current: &[usize],
    previous: &[usize],
    receptors: Range<usize>,
) -> Option<(usize, usize)> {
    let usable = |indices: &[usize], antenna_id: i32| {
        indices
            .iter()
            .copied()
            .find(|&index| rows[index].antenna_id == antenna_id)
            .filter(|&index| {
                rows[index]
                    .flags
                    .get(receptors.clone())
                    .is_some_and(|flags| flags.iter().all(|flag| !*flag))
            })
    };
    choices
        .iter()
        .find_map(|&antenna_id| Some((usable(current, antenna_id)?, usable(previous, antenna_id)?)))
}

pub(crate) fn phase_only(gain: Complex32) -> Complex32 {
    let norm = gain.norm();
    if norm > f32::EPSILON {
        gain / Complex32::new(norm, 0.0)
    } else {
        Complex32::new(1.0, 0.0)
    }
}

/// Divide every gain of each window by the root mean or median power of the window's
/// unflagged solutions, taken over all its intervals, antennas and
/// receptors together, as CASA's `solnorm` does
/// (`SolvableVisJones::normalize`, `SolvableVisCal::calcPowerNorm`). A
/// window with one unflagged solution or none stays, as `normSolnArray`
/// normalizes only more than one.
fn normalize_gain_solution_amplitudes(rows: &mut [kernel::SolutionRow], method: GainNormalization) {
    let mut power = BTreeMap::<i32, (f64, usize, Vec<f32>)>::new();
    for row in rows.iter() {
        let (sum, count, samples) = power.entry(row.spw_id).or_default();
        for (gain, flagged) in row.gains.iter().zip(&row.flags) {
            if !flagged {
                let power = gain.norm_sqr();
                match method {
                    GainNormalization::Mean => *sum += f64::from(power),
                    GainNormalization::Median => samples.push(power),
                }
                *count += 1;
            }
        }
    }
    let factors: BTreeMap<_, _> = power
        .into_iter()
        .filter_map(|(spw, (sum, count, mut samples))| {
            if count <= 1 {
                return None;
            }
            let factor = match method {
                GainNormalization::Mean => (sum / count as f64).sqrt() as f32,
                GainNormalization::Median => grouping::median_with_even_mean(&mut samples).sqrt(),
            };
            (factor.is_finite() && factor > 0.0).then_some((spw, factor))
        })
        .collect();
    for row in rows.iter_mut() {
        let Some(&factor) = factors.get(&row.spw_id) else {
            continue;
        };
        for gain in &mut row.gains {
            *gain /= Complex32::new(factor, 0.0);
        }
    }
}

pub(crate) fn get_i32(table: &Table, row: usize, column: &str) -> Result<i32, GainSolveError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
        .map_err(MsError::from)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: column.to_string(),
            source,
        })? {
        ScalarValue::Int32(value) => Ok(*value),
        ScalarValue::Int64(value) => {
            i32::try_from(*value).map_err(|_| GainSolveError::UnsupportedParameterShape {
                path: format!("{column} scalar"),
                shape: vec![row],
            })
        }
        _ => Err(GainSolveError::UnsupportedParameterShape {
            path: format!("{column} scalar"),
            shape: vec![row],
        }),
    }
}

pub(crate) fn get_f64(table: &Table, row: usize, column: &str) -> Result<f64, GainSolveError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
        .map_err(MsError::from)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: column.to_string(),
            source,
        })? {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Float32(value) => Ok(f64::from(*value)),
        _ => Err(GainSolveError::UnsupportedParameterShape {
            path: format!("{column} scalar"),
            shape: vec![row],
        }),
    }
}

pub(crate) fn validate_solve_interval(
    solve_interval: GainSolveInterval,
) -> Result<(), GainSolveError> {
    match solve_interval {
        GainSolveInterval::Infinite | GainSolveInterval::Integration => Ok(()),
        GainSolveInterval::Seconds(seconds) if seconds > 0.0 => Ok(()),
        _ => Err(GainSolveError::UnsupportedSolveInterval { solve_interval }),
    }
}

/// Convert the CASA point-model Stokes vector to the solver's single precision.
/// Short vectors are padded with zero. An explicit empty vector is a zero
/// model, as in `calibrater.setptmodel`, while tasks choose their stored model
/// before calling this conversion when `smodel` is empty.
pub fn point_model_stokes(values: &[f64]) -> Option<[f32; 4]> {
    let mut stokes = [0.0; 4];
    for (target, &value) in stokes.get_mut(..values.len())?.iter_mut().zip(values) {
        *target = value as f32;
    }
    stokes
        .iter()
        .all(|value| value.is_finite())
        .then_some(stokes)
}

pub(crate) fn validate_smodel(smodel: [f32; 4], polarized: bool) -> Result<(), GainSolveError> {
    if smodel.iter().any(|value| !value.is_finite())
        || (!polarized && smodel[1..].iter().any(|value| *value != 0.0))
    {
        return Err(GainSolveError::UnsupportedSkyModel { smodel });
    }
    Ok(())
}

pub(crate) fn correlation_receptors(code: i32) -> Option<(usize, usize)> {
    match code {
        5 | 9 => Some((0, 0)),
        6 | 10 => Some((0, 1)),
        7 | 11 => Some((1, 0)),
        8 | 12 => Some((1, 1)),
        _ => None,
    }
}

pub(crate) fn stokes_name(code: i32) -> &'static str {
    match code {
        5 => "RR",
        6 => "RL",
        7 => "LR",
        8 => "LL",
        9 => "XX",
        10 => "XY",
        11 => "YX",
        12 => "YY",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod solnorm_tests {
    use super::*;

    fn row(
        spw_id: i32,
        time_seconds: f64,
        gains: [f32; 2],
        flags: [bool; 2],
    ) -> kernel::SolutionRow {
        kernel::SolutionRow {
            time_seconds,
            field_id: 0,
            spw_id,
            antenna_id: 0,
            scan_number: 1,
            observation_id: 0,
            refant_id: 0,
            gains: gains
                .iter()
                .map(|&gain| Complex32::new(gain, 0.0))
                .collect(),
            flags: flags.to_vec(),
            param_errors: vec![0.0; 2],
            snrs: vec![0.0; 2],
            weights: vec![1.0; 2],
        }
    }

    #[test]
    fn reference_checks_the_legacy_physical_slots_without_relaxing_dual_receptor_solves() {
        let mut rows = [
            row(0, 0.0, [1.0, 2.0], [true, false]),
            row(1, 0.0, [1.0, 2.0], [true, false]),
        ];
        let slots = LegacyRefantSlots::from([(0, [false, true]), (1, [true, true])]);
        apply_reference_antenna(&mut rows, 0, &[], true, &slots);
        assert_eq!(rows[0].flags, [true, false]);
        assert_eq!(rows[0].refant_id, 0);
        assert_eq!(rows[1].flags, [true, true]);
        assert_eq!(rows[1].refant_id, -1);
    }

    /// CASA 6.7.6.14 normalizes a window's gains by one factor, the root
    /// mean power of its unflagged solutions over every interval and both
    /// receptors (probed: XX and YY solutions of mean power 0.43 and 1.51,
    /// together 1.00), not each interval or receptor by its own.
    #[test]
    fn solnorm_divides_each_window_by_one_factor_over_its_intervals_and_receptors() {
        let mut rows = vec![
            row(0, 0.0, [1.0, 3.0], [false, false]),
            row(0, 10.0, [2.0, 3.0], [false, false]),
            // A flagged solution is divided but does not count.
            row(0, 20.0, [100.0, 3.0], [true, false]),
            row(1, 0.0, [4.0, 4.0], [false, false]),
            // One unflagged solution is not normalized (normSolnArray).
            row(2, 0.0, [4.0, 5.0], [false, true]),
        ];
        normalize_gain_solution_amplitudes(&mut rows, GainNormalization::Mean);
        let factor = ((1.0_f32 + 9.0 + 4.0 + 9.0 + 9.0) / 5.0).sqrt();
        let amplitudes = |row: &kernel::SolutionRow| -> Vec<f32> {
            row.gains.iter().map(|gain| gain.norm()).collect()
        };
        let close = |left: &[f32], right: &[f32]| {
            left.iter()
                .zip(right)
                .all(|(left, right)| (left - right).abs() < 1e-5)
        };
        assert!(close(&amplitudes(&rows[0]), &[1.0 / factor, 3.0 / factor]));
        assert!(close(&amplitudes(&rows[1]), &[2.0 / factor, 3.0 / factor]));
        assert!(close(
            &amplitudes(&rows[2]),
            &[100.0 / factor, 3.0 / factor]
        ));
        assert!(close(&amplitudes(&rows[3]), &[1.0, 1.0]));
        assert!(close(&amplitudes(&rows[4]), &[4.0, 5.0]));
    }
}
