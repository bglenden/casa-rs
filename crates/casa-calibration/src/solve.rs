// SPDX-License-Identifier: LGPL-3.0-or-later
//! Limited `gaincal` support for the first solver wave.
//!
//! This module intentionally implements only a narrow, testable slice:
//!
//! - `gaintype=G|T`
//! - `calmode='p|ap'`
//! - `solint='inf'|'int'|<seconds>`
//! - explicit reference antenna
//! - point-source Stokes-I sky model (`smodel=[I,0,0,0]`) or `MODEL_DATA`
//!
//! The acceptance contract is downstream behavior: the resulting caltable
//! should be CASA-compatible on disk and should yield corrected visibilities
//! close to CASA's own `gaincal` when applied.

#[cfg(test)]
mod batch_trial;
pub(crate) mod grouping;
pub(crate) mod kernel;
mod trace;
pub(crate) mod writer;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_tables::{Table, TableError};
use casa_types::{Complex32, ScalarValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::selected_data::SelectedData;
use crate::session::prepare_calibration_solve;
use crate::solve_stats::{AntennaSolution, IntervalStages, SolveStats, SolveStatsBuilder};
use crate::{ApplyExecutionError, ApplyPlanError};
use grouping::{SolveGroupOptions, build_solve_groups, validate_smodel, validate_solve_interval};
use kernel::solve_group;
use writer::write_gain_caltable;

/// Supported first-wave gain solve families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainType {
    /// Per-receptor complex gains.
    G,
    /// Polarization-collapsed scalar complex gains.
    T,
}

impl GainType {
    fn vis_cal(self) -> &'static str {
        match self {
            Self::G => "G Jones",
            Self::T => "T Jones",
        }
    }

    /// The polarizations a solution holds: one gain per receptor for G, one
    /// for both for T.
    pub(crate) fn polarizations(self) -> usize {
        match self {
            Self::G => 2,
            Self::T => 1,
        }
    }
}

/// Supported first-wave solve modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainSolveMode {
    /// Phase-only solve.
    Phase,
    /// Amplitude-and-phase solve.
    AmplitudePhase,
}

/// Visibility model source used by the gain solver.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainSolveModelSource {
    /// Use the point-source Stokes-I model from `smodel`.
    #[default]
    PointSource,
    /// Use per-sample visibilities from the MS `MODEL_DATA` column.
    ModelColumn,
}

/// Supported first-wave gain solution intervals.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum GainSolveInterval {
    /// Solve one solution per `(observation, field, spw, scan)` group.
    Infinite,
    /// Solve one solution per integration timestamp inside each scan.
    Integration,
    /// Solve one solution per contiguous time bucket of the given duration, in seconds.
    Seconds(f64),
}

/// Supported first-wave `gaincal` combine axes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GainSolveCombine {
    /// Extend solves across scan boundaries.
    pub scans: bool,
    /// Extend solves across field boundaries.
    pub fields: bool,
}

/// Reference antenna selector for solve requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum RefAntSelector {
    /// Exact antenna id.
    AntennaId(i32),
    /// Exact ANTENNA.NAME match.
    AntennaName(String),
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
    /// Axes to combine while forming solve groups.
    pub combine: GainSolveCombine,
    /// Reference antenna.
    pub refant: RefAntSelector,
    /// Prior calibration tables to apply on the fly before solving.
    pub prior_calibration_tables: Vec<crate::ApplyCalibrationTableSpec>,
    /// Whether to apply parallactic-angle correction before solving.
    pub parang: bool,
    /// Visibility model source.
    pub model_source: GainSolveModelSource,
    /// Whether to normalize average solution amplitudes to unity.
    pub normalize_average_amplitude: bool,
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
    /// Shared calibration-table persistence failed.
    #[error(transparent)]
    CalibrationTable(#[from] crate::CalibrationTableWriteError),
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

    /// The solve requires a point-source Stokes-I model.
    #[error("unsupported smodel {smodel:?}; only [I,0,0,0] is supported in this wave")]
    UnsupportedSkyModel {
        /// Model vector passed by the caller.
        smodel: [f32; 4],
    },

    /// The solve requested `MODEL_DATA`, but the MS does not contain it.
    #[error("gain solve model_source=ModelColumn requires a MODEL_DATA column")]
    MissingModelColumn,

    /// The requested solve interval is unsupported.
    #[error("unsupported solve interval {solve_interval:?}; seconds values must be > 0")]
    UnsupportedSolveInterval {
        /// Interval requested by the caller.
        solve_interval: GainSolveInterval,
    },

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

    /// No solution interval could be solved, so there is no caltable to
    /// write; CASA logs "No output calibration table written." here.
    #[error(
        "gain solve found no solution in any interval (no usable data, too few baselines per antenna, or no convergence); no caltable written"
    )]
    NoSolutions,

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
) -> Result<GainSolveReport, GainSolveError> {
    let solved = solve_gain_rows(ms, request)?;
    write_gain_caltable(ms, request, &solved)
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
) -> Result<SolvedGains, GainSolveError> {
    if matches!(request.model_source, GainSolveModelSource::PointSource) {
        validate_smodel(request.smodel)?;
    } else if !ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column("MODEL_DATA"))
    {
        return Err(GainSolveError::MissingModelColumn);
    }
    validate_solve_interval(request.solve_interval)?;
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &request.refant,
        &request.prior_calibration_tables,
        request.parang,
    )?;
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        SolveGroupOptions {
            gain_type: request.gain_type,
            model_source: request.model_source,
            stokes_i: request.smodel[0],
            solve_interval: request.solve_interval,
            combine: request.combine,
            channelized: false,
        },
    )?;

    if groups.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }

    let mut solution_rows = Vec::new();
    let mut attempted_spw_ids = BTreeSet::new();
    let mut solve_stats = SolveStatsBuilder::new(
        request.gain_type.polarizations(),
        spectral_window_ids(ms)?,
        &context.available_antennas,
    );
    // Each solved interval's window and its rows in `solution_rows`.
    let mut intervals = Vec::new();
    for ((base_key, bucket_key), mut group) in groups {
        if group.attempted {
            attempted_spw_ids.insert(group.spw_id);
        }
        group.finalize_for_solve(request.solve_mode);
        trace::trace_group(&base_key, &bucket_key, &group, request);
        solve_stats.interval(&IntervalStages::of(
            &group,
            request.min_baselines_per_antenna,
        ));
        let spw_id = group.spw_id;
        let Some(group_rows) = solve_closed_group(
            group,
            &context.available_antennas,
            request,
            context.refant_id,
        ) else {
            continue;
        };
        solve_stats.solutions(
            spw_id,
            group_rows.iter().map(|row| AntennaSolution {
                antenna_id: row.antenna_id,
                passed: row.flags.iter().map(|flagged| !flagged).collect(),
            }),
        );
        trace::trace_solution_rows(
            &base_key,
            &bucket_key,
            &group_rows,
            request,
            context.refant_id,
        );
        let start = solution_rows.len();
        solution_rows.extend(group_rows);
        intervals.push((spw_id, start..solution_rows.len()));
    }
    if solution_rows.is_empty() {
        return Err(GainSolveError::NoSolutions);
    }
    apply_flex_refant(
        &mut solution_rows,
        context.refant_id,
        &refant_distance_order(ms, context.refant_id)?,
    );
    // CASA's solnorm follows the reference antenna, over every interval
    // (`SolvableVisJones::globalPostSolveTinker`).
    if matches!(request.solve_mode, GainSolveMode::AmplitudePhase)
        && request.normalize_average_amplitude
    {
        normalize_gain_solution_amplitudes(&mut solution_rows);
    }
    // The reference antenna each interval ended with, where it kept its
    // solution: flex flags an interval it found none for.
    for (spw_id, rows) in intervals {
        let rows = &solution_rows[rows];
        if let Some(refant) = rows
            .iter()
            .find(|row| row.antenna_id == row.refant_id && row.flags.iter().any(|flagged| !flagged))
        {
            solve_stats.used_as_refant(spw_id, refant.antenna_id);
        }
    }
    Ok(SolvedGains {
        rows: solution_rows,
        refant_id: context.refant_id,
        attempted_spw_ids,
        selected_data: context.selected_data,
        solve_stats: solve_stats.finish(),
    })
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

/// Solve one complete group whose accumulation is finalized
/// ([`grouping::SolveAccumulator::finalize_for_solve`]). An interval CASA
/// cannot solve gets no rows (`Calibrater::genericGatherAndSolve` keeps a
/// solution only after one succeeds).
pub(crate) fn solve_closed_group(
    group: grouping::SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    request: &GainSolveRequest,
    refant_id: i32,
) -> Option<Vec<kernel::SolutionRow>> {
    solve_group(
        group,
        available_antennas,
        request.gain_type,
        request.solve_mode,
        refant_id,
        request.min_snr,
        request.min_baselines_per_antenna,
    )
}

/// The antennas other than `refant_id`, nearest it first: the alternates
/// `SolvableVisJones::applyRefAnt` tries when the reference antenna is flagged.
fn refant_distance_order(ms: &MeasurementSet, refant_id: i32) -> Result<Vec<i32>, GainSolveError> {
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
/// is never solved). Its phase change since that interval is removed from
/// every antenna, so an alternate is held at constant phase. An interval
/// with no such antenna is flagged entirely and leaves the last referred
/// interval in place for the next.
pub(crate) fn apply_flex_refant(
    rows: &mut [kernel::SolutionRow],
    preferred_refant_id: i32,
    alternates: &[i32],
) {
    let mut group_indices = BTreeMap::<(i32, u64, i32), Vec<usize>>::new();
    for (index, row) in rows.iter().enumerate() {
        group_indices
            .entry((row.spw_id, row.time_seconds.to_bits(), row.field_id))
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
    for ((spw_id, _, _), current) in group_indices {
        if last_spw != Some(spw_id) {
            previous = None;
            choices[1] = preferred_refant_id;
            last_spw = Some(spw_id);
        }
        let receptors = receptor_counts[&spw_id];
        let previous_indices = previous.as_ref().unwrap_or(&current);
        let Some((current_ref_index, previous_ref_index)) =
            find_flex_refant(rows, &choices, &current, previous_indices, receptors)
        else {
            for index in &current {
                rows[*index].flags.iter_mut().for_each(|flag| *flag = true);
                rows[*index]
                    .gains
                    .iter_mut()
                    .for_each(|gain| *gain = Complex32::new(1.0, 0.0));
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
            for (gain, flag) in rows[*index].gains.iter_mut().zip(rows[*index].flags.iter()) {
                if *flag {
                    *gain = Complex32::new(1.0, 0.0);
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
    receptors: usize,
) -> Option<(usize, usize)> {
    let usable = |index: &usize, antenna_id: i32| {
        let row = &rows[*index];
        row.antenna_id == antenna_id && row.flags.iter().take(receptors).all(|flag| !*flag)
    };
    choices.iter().find_map(|&antenna_id| {
        let current_index = current
            .iter()
            .copied()
            .find(|index| usable(index, antenna_id))?;
        let previous_index = previous
            .iter()
            .copied()
            .find(|index| usable(index, antenna_id))?;
        Some((current_index, previous_index))
    })
}

fn phase_only(gain: Complex32) -> Complex32 {
    let norm = gain.norm();
    if norm > f32::EPSILON {
        gain / Complex32::new(norm, 0.0)
    } else {
        Complex32::new(1.0, 0.0)
    }
}

/// Divide every gain of each window by the root mean power of the window's
/// unflagged solutions, taken over all its intervals, antennas and
/// receptors together, as CASA's `solnorm` does
/// (`SolvableVisJones::normalize`, `SolvableVisCal::calcPowerNorm`).
fn normalize_gain_solution_amplitudes(rows: &mut [kernel::SolutionRow]) {
    let mut power = BTreeMap::<i32, (f64, usize)>::new();
    for row in rows.iter() {
        let (sum, count) = power.entry(row.spw_id).or_default();
        for (gain, flagged) in row.gains.iter().zip(&row.flags) {
            if !flagged {
                *sum += f64::from(gain.norm_sqr());
                *count += 1;
            }
        }
    }
    for row in rows.iter_mut() {
        let Some(&(sum, count)) = power.get(&row.spw_id) else {
            continue;
        };
        let factor = (sum / count.max(1) as f64).sqrt() as f32;
        if count == 0 || !(factor.is_finite() && factor > 0.0) {
            continue;
        }
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
        ];
        normalize_gain_solution_amplitudes(&mut rows);
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
    }
}
