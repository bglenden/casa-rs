// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channelized `B Jones` and legacy polynomial `BPOLY` bandpass solving.
//!
//! The acceptance contract is downstream behavior: the resulting caltable
//! should be CASA-compatible on disk and should yield corrected visibilities
//! close to CASA's own `bandpass` when applied after the same prior gains.

pub(crate) mod bpoly;
mod metadata;
pub(crate) use metadata::{
    GatherFrequencyLayout, bin_widths as solution_frequency_bin_widths, check_gather_frequency,
    write as write_solution_spectral_windows,
};
mod postprocess;

use crate::solve::control::SolveControl;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use casa_core::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_ms::MsError;
use casa_ms::schema::SubtableId;
use casa_ms::selection::MsSelection;
use casa_tables::{ColumnSchema, TableError, TableSchema};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
    COL_WEIGHT,
};
use crate::selected_data::SelectedData;
use crate::session::prepare_calibration_solve;
use crate::solve::grouping::{
    SelectedSolveRow, SolveAccumulator, SolveGroupOptions, SolveInputRows,
    build_solve_groups_with_control,
};
use crate::solve::kernel::{SolveGraphOptions, SolveStart, SolvedGraphs, solve_graphs};
use crate::solve::spectral_window_ids;
use crate::solve::{
    GainNormalization, GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainType, RefAntSelector,
};
use crate::solve_stats::{
    AntennaSolution, IntervalStages, SolveStats, SolveStatsBuilder, ToolSolveActivity,
};
use crate::writer::{CalibrationTableDescriptor, CalibrationTableWriter};
use bpoly::write_bpoly_caltable;
use postprocess::{normalize_bandpass_rows, reference_bandpass_rows};

/// Supported `bandpass` table families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum BandpassType {
    /// Channelized complex `B Jones` solutions stored in `CPARAM`.
    B,
    /// Legacy polynomial `BPOLY` bandpass solutions.
    BPoly,
}

/// Frequency width of a channelized `bandpass` solution bin.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum BandpassFrequencyInterval {
    /// Number of selected channels per solution.
    Channels(f64),
    /// Bandwidth divided by the first input channel's absolute width in each SPW.
    Hertz(f64),
}

impl BandpassFrequencyInterval {
    /// Read the frequency component of CASA's `solint`. The outer option
    /// denotes valid input, and the inner one denotes frequency binning.
    #[must_use]
    pub fn from_casa(text: &str) -> Option<Option<Self>> {
        use casa_core::quanta::{Quantity, Unit};
        let text = text.trim();
        let upper = text.to_ascii_uppercase();
        if upper.is_empty() || upper.contains("NONE") {
            return Some(None);
        }
        let channels = |count: f64| {
            count
                .is_finite()
                .then(|| (count > 1.0).then_some(Self::Channels(count)))
        };
        if let Some((count, _)) = upper.split_once("CH") {
            return channels(count.trim().parse().ok()?);
        }
        let quantity = text.parse::<Quantity>().ok()?;
        if quantity.unit().name().is_empty() {
            return channels(quantity.value());
        }
        let hertz = quantity.get_value_in(&Unit::new("Hz").ok()?).ok()?;
        hertz
            .is_finite()
            .then(|| (hertz > 0.).then_some(Self::Hertz(hertz)))
    }
}

/// Supported `bandpass(..., combine=...)` axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct BandpassSolveCombine {
    /// Extend solves across scan boundaries.
    pub scans: bool,
    /// Extend solves across field boundaries.
    pub fields: bool,
    /// Extend solves across observation boundaries.
    #[serde(default)]
    pub observations: bool,
    /// Solve compatible spectral windows together.
    #[serde(default)]
    pub spectral_windows: bool,
}

/// The cuts CASA's `bandpass` makes in each channel's solve
/// (`SolvableVisCal::verifyConstraints` and `applySNRThreshold`) and the
/// edge channels a `BPOLY` fit leaves out (`BJonesPoly::maskedChannel`).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BandpassSolveCuts {
    /// An antenna left with fewer baselines with data, once the antennas
    /// below this have been removed in turn, is not solved in the channel
    /// (`minblperant`).
    pub min_baselines_per_antenna: usize,
    /// A channel solution whose SNR is not above this is flagged
    /// (`minsnr`).
    pub min_snr: f32,
    /// Largest interior flagged gap to interpolate in a channelized B table.
    #[serde(default)]
    pub fill_gaps: usize,
    /// The percentage of a window's channels at each edge a `BPOLY` fit
    /// leaves out (`maskedge`).
    pub mask_edge_percent: f32,
    /// The number of central channels excluded from a polynomial fit.
    #[serde(default)]
    pub mask_center_channels: usize,
    /// Normalize each baseline's input spectrum by its weighted mean.
    #[serde(default)]
    pub normalize_visibilities: bool,
    /// Whether the BPOLY amplitude and phase polynomials are omitted.
    #[serde(default)]
    pub omit_polynomial: [bool; 2],
}

impl BandpassSolveCuts {
    /// CASA 6.7.6's defaults: four baselines, an SNR of three, and five
    /// percent of the channels at each edge.
    pub const CASA_DEFAULTS: Self = Self {
        min_baselines_per_antenna: 4,
        min_snr: 3.0,
        fill_gaps: 0,
        mask_edge_percent: 5.0,
        mask_center_channels: 0,
        normalize_visibilities: false,
        omit_polynomial: [false; 2],
    };

    /// Whether `channel` of `channel_count` lies in the edges a `BPOLY` fit
    /// leaves out: below `channel_count * fraction`, or above
    /// `channel_count * (1 - fraction)`, in single precision as
    /// `BJonesPoly::maskedChannel` masks them.
    fn masks_edge_channel(&self, channel: usize, channel_count: usize) -> bool {
        let fraction = (f64::from(self.mask_edge_percent) / 100.0) as f32;
        let (channel, count) = (channel as f32, channel_count as f32);
        channel < count * fraction || channel > count * (1.0 - fraction)
    }

    fn masks_polynomial_channel(&self, channel: usize, count: usize) -> bool {
        let center_count = self.mask_center_channels.min(count) as i64;
        let center_start = count as i64 / 2 - center_count / 2;
        let center_end = center_start + center_count;
        self.masks_edge_channel(channel, count)
            || (center_start..center_end).contains(&(channel as i64))
    }
}

/// Request for a limited `bandpass` solve.
#[derive(Debug, Clone)]
pub struct BandpassSolveRequest {
    /// MS selection applied before solving.
    pub selection: MsSelection,
    /// Output caltable path.
    pub output_table: PathBuf,
    /// Reference antenna.
    pub refant: RefAntSelector,
    /// Prior calibration tables to apply on the fly before solving.
    pub prior_calibration_tables: Vec<crate::apply::ApplyCalibrationTableSpec>,
    /// Whether to apply parallactic-angle correction before solving.
    pub parang: bool,
    /// Axes combined before solving.
    pub combine: BandpassSolveCombine,
    /// Requested output bandpass table family.
    pub band_type: BandpassType,
    /// Whether to normalize average amplitudes to unity.
    pub normalize_average_amplitude: bool,
    /// Requested amplitude polynomial degree for `BPOLY` output.
    pub amplitude_degree: usize,
    /// Requested phase polynomial degree for `BPOLY` output.
    pub phase_degree: usize,
    /// Visibility model source.
    pub model_source: GainSolveModelSource,
    /// Time interval used to group channelized solutions.
    pub solve_interval: GainSolveInterval,
    /// Optional frequency interval used to group selected channels.
    pub frequency_interval: Option<BandpassFrequencyInterval>,
    /// Keep independent flags on each correlation during the solve.
    pub correlation_dependent_flags: bool,
    /// Point-source Stokes model when requested.
    pub smodel: [f32; 4],
    /// The per-channel cuts and `BPOLY` edge mask, or none: every antenna
    /// with a baseline is solved, every solution kept and every channel
    /// fitted.
    pub cuts: Option<BandpassSolveCuts>,
}

/// Solve summary for a limited `bandpass` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BandpassSolveReport {
    /// Output caltable path.
    pub output_table: PathBuf,
    /// CASA-visible caltable subtype.
    pub table_subtype: String,
    /// Resolved reference antenna id.
    pub refant_antenna_id: i32,
    /// Distinct fields represented in the solved table.
    pub field_ids: Vec<i32>,
    /// Distinct SPWs represented in the solved table.
    pub spectral_window_ids: Vec<i32>,
    /// Number of solution rows written.
    pub solution_row_count: usize,
    /// Number of channels in each solved row.
    pub channel_count: usize,
    /// The data the solve selected, CASA's `selectvis`.
    #[serde(default)]
    pub selected_data: SelectedData,
    /// Selection failed before any prior tables were registered.
    #[serde(default)]
    pub selection_failed: bool,
    /// How far the solve's intervals got, CASA's `solvestats`.
    #[serde(default)]
    pub solve_stats: SolveStats,
}

/// Errors returned by the limited `bandpass` solver.
#[derive(Debug, Error)]
pub enum BandpassSolveError {
    /// Shared gain/bandpass solve lifecycle failed.
    #[error(transparent)]
    SharedSolve(#[from] crate::solve::GainSolveError),
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
    #[error("bandpass solve selection produced no rows")]
    EmptySelection,

    /// The solve requires a point-source Stokes-I model.
    #[error("unsupported smodel {smodel:?}; only [I,0,0,0] is supported in this wave")]
    UnsupportedSkyModel {
        /// Model vector passed by the caller.
        smodel: [f32; 4],
    },

    /// The selected output configuration is unsupported.
    #[error("unsupported bandpass solve configuration: {reason}")]
    UnsupportedConfiguration {
        /// Error context.
        reason: String,
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

    /// Fitting a legacy BPOLY row failed.
    #[error(
        "failed to fit BPOLY row for antenna {antenna_id} field={field_id} spw={spw_id}: {reason}"
    )]
    FitPolynomial {
        /// Antenna id.
        antenna_id: i32,
        /// Field id.
        field_id: i32,
        /// Spectral window id.
        spw_id: i32,
        /// Error context.
        reason: String,
    },

    /// The solve failed because the selected data do not connect the reference
    /// antenna to at least one solved antenna.
    #[error(
        "bandpass solve could not determine a solution for antenna {antenna_id} in field={field_id} spw={spw_id}"
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

/// Additional controls used by the direct calibrater's `setsolve(B)`.
#[derive(Clone, Copy)]
pub(crate) struct BandpassSolveControls {
    pub(crate) solve_mode: GainSolveMode,
    pub(crate) normalization: GainNormalization,
    pub(crate) preaverage_seconds: f64,
    /// VI2 reports the gather but cannot perform a BPOLY interval fit.
    pub(crate) modern_polynomial: bool,
}

impl Default for BandpassSolveControls {
    fn default() -> Self {
        Self {
            solve_mode: GainSolveMode::AmplitudePhase,
            normalization: GainNormalization::Mean,
            preaverage_seconds: -1.,
            modern_polynomial: false,
        }
    }
}

/// Solve a limited `bandpass` request from an already-open MeasurementSet.
pub(crate) fn solve_bandpass(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    mode: crate::output::CalibrationOutputMode,
    activity: Option<&mut ToolSolveActivity>,
    controls: BandpassSolveControls,
    control: SolveControl<'_>,
) -> Result<BandpassSolveReport, BandpassSolveError> {
    control.check()?;
    validate_bandpass_request(request)?;
    let use_casa_solve = request.band_type == BandpassType::B || request.cuts.is_some();
    let polynomial_bandpass = request.band_type == BandpassType::BPoly
        && request.cuts.is_some()
        && !controls.modern_polynomial;
    let rotate_model = request.parang && use_casa_solve;
    let input_phase = crate::timing::Phase::start("bandpass_select_read_average");
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &request.refant,
        &request.prior_calibration_tables,
        request.parang,
        use_casa_solve,
        if use_casa_solve {
            crate::solve::grouping::PriorCalibrationMode::Bandpass {
                share_input_flags: !request.correlation_dependent_flags && !polynomial_bandpass,
                rotate_model,
            }
        } else {
            crate::solve::grouping::PriorCalibrationMode::Data
        },
    );
    let context = match context {
        Ok(context) => context,
        Err(crate::solve::GainSolveError::ResolveRefAnt { .. })
            if use_casa_solve && matches!(request.refant, RefAntSelector::Expression { .. }) =>
        {
            return skipped_bandpass_report(ms, request);
        }
        Err(error) if use_casa_solve && selection_rejected(&error) => {
            return skipped_bandpass_report(ms, request);
        }
        Err(error) => return Err(error.into()),
    };
    let channel_bin_widths = metadata::bin_widths(ms, &context.rows, request.frequency_interval)?;
    // Preapply already shared the input flags when requested. Flags
    // introduced by a prior remain on the affected correlations.
    let correlation_dependent_flags = request.correlation_dependent_flags
        || (use_casa_solve && context.preapplied_rows.is_some());
    let groups = build_solve_groups_with_control(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &channel_bin_widths,
        SolveGroupOptions {
            input_rows: SolveInputRows::gain(
                activity.is_some()
                    && (request.band_type == BandpassType::B || controls.modern_polynomial),
            ),
            gain_type: GainType::G,
            model_source: request.model_source,
            point_stokes: request.smodel,
            rotate_model,
            solve_interval: request.solve_interval,
            combine: GainSolveCombine {
                scans: request.combine.scans,
                fields: request.combine.fields,
                observations: request.combine.observations,
                spectral_windows: request.combine.spectral_windows,
            },
            legacy_g_receptors: false,
            channelized: true,
            polynomial_bandpass,
            preaverage_max_span: crate::solve::preaverage_span(
                request.solve_interval,
                controls.preaverage_seconds,
            ),
            robust_seed: false,
            correlation_dependent_flags,
        },
        control,
    )?;

    if groups.is_empty() {
        return Err(BandpassSolveError::EmptySelection);
    }

    drop(input_phase);
    if request.band_type == BandpassType::BPoly && request.cuts.is_some() {
        if controls.modern_polynomial
            && let Some(activity) = activity
        {
            return bpoly::modern_gather(ms, request, context, groups.into_values(), activity);
        }
        return bpoly::solve_polynomial(
            ms,
            request,
            context,
            groups.into_values(),
            &channel_bin_widths,
            activity,
            (mode, control),
        );
    }
    let kernel_phase = crate::timing::Phase::start("bandpass_solve");
    let mut solution_rows = Vec::new();
    let mut solve_stats = SolveStatsBuilder::new(
        GainType::G.polarizations(),
        spectral_window_ids(ms)?,
        &context.available_antennas,
    );
    for mut group in groups.into_values() {
        control.check()?;
        match request.band_type {
            BandpassType::B => group.finalize(controls.solve_mode, false, true, control)?,
            BandpassType::BPoly => {
                group.finalize(GainSolveMode::AmplitudePhase, false, false, control)?
            }
        }
        let mut stages = IntervalStages::of(
            &group,
            request
                .cuts
                .map_or(0, |cuts| cuts.min_baselines_per_antenna),
        );
        if group.single_parallel_hand && request.band_type == BandpassType::B {
            stages.with_data[1] = stages.with_data[0].clone();
        }
        solve_stats.interval(&stages);
        let (mut rows, refants) = solve_bandpass_group(
            &group,
            &context.available_antennas,
            context.refant_id,
            request,
            controls.solve_mode,
            (
                correlation_dependent_flags,
                request.correlation_dependent_flags,
            ),
            control,
        )?;
        if request.band_type == BandpassType::BPoly && request.normalize_average_amplitude {
            normalize_bandpass_rows(&mut rows, group.solution_channels);
        }
        let solutions = rows
            .iter()
            .map(|row| AntennaSolution {
                antenna_id: row.antenna_id,
                passed: (0..2)
                    .map(|receptor| row.flags.iter().skip(receptor).step_by(2).any(|f| !f))
                    .collect(),
            })
            .collect::<Vec<_>>();
        if request.band_type == BandpassType::BPoly {
            for solution in &solutions {
                if refants.contains(&solution.antenna_id) && solution.passed.contains(&true) {
                    solve_stats.used_as_refant(group.spw_id, solution.antenna_id);
                }
            }
        }
        solve_stats.record_success(group.spw_id, !rows.is_empty());
        solve_stats.solutions(group.spw_id, solutions);
        solution_rows.extend(rows);
    }
    control.check()?;
    solve_stats.track_activity(activity);
    drop(kernel_phase);
    write_solved_bandpass(
        ms,
        request,
        context,
        &channel_bin_widths,
        controls,
        (solution_rows, solve_stats),
        (mode, control),
    )
}

fn write_solved_bandpass(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    context: crate::session::CalibrationSolveContext,
    channel_bin_widths: &BTreeMap<i32, usize>,
    controls: BandpassSolveControls,
    (mut solution_rows, mut solve_stats): (Vec<BandpassSolutionRow>, SolveStatsBuilder),
    (mode, control): (crate::output::CalibrationOutputMode, SolveControl<'_>),
) -> Result<BandpassSolveReport, BandpassSolveError> {
    control.check()?;
    let _write_phase = crate::timing::Phase::start("bandpass_write");
    if solution_rows.is_empty() {
        return Ok(empty_report(
            request,
            context.refant_id,
            context.selected_data,
            solve_stats.finish(),
        ));
    }
    let report = match request.band_type {
        BandpassType::B => {
            if context.refant_id >= 0 {
                let strict = matches!(
                    request.refant,
                    RefAntSelector::Expression { strict: true, .. }
                );
                let mut alternates = context.refant_alternates;
                if !strict {
                    let last = alternates.last().copied().unwrap_or(context.refant_id);
                    let mut nearby = crate::solve::refant_distance_order(ms, last)?;
                    nearby.retain(|id| *id != context.refant_id && !alternates.contains(id));
                    alternates.extend(nearby);
                }
                reference_bandpass_rows(
                    &mut solution_rows,
                    context.refant_id,
                    &alternates,
                    strict,
                    &mut solve_stats,
                );
            }
            postprocess::finish_bandpass_rows(&mut solution_rows, request, controls);
            write_bandpass_caltable(
                ms,
                request,
                context.refant_id,
                &solution_rows,
                channel_bin_widths,
                &context.rows,
                (mode, control),
            )
        }
        BandpassType::BPoly => write_bpoly_caltable(
            ms,
            request,
            context.refant_id,
            &solution_rows,
            mode,
            control,
        ),
    }?;
    Ok(BandpassSolveReport {
        selected_data: context.selected_data,
        solve_stats: solve_stats.finish(),
        ..report
    })
}

fn empty_report(
    request: &BandpassSolveRequest,
    refant_id: i32,
    selected_data: SelectedData,
    solve_stats: SolveStats,
) -> BandpassSolveReport {
    BandpassSolveReport {
        output_table: request.output_table.clone(),
        table_subtype: match request.band_type {
            BandpassType::B => "B Jones",
            BandpassType::BPoly => "BPOLY",
        }
        .to_string(),
        refant_antenna_id: refant_id,
        field_ids: Vec::new(),
        spectral_window_ids: Vec::new(),
        solution_row_count: 0,
        channel_count: 0,
        selected_data,
        selection_failed: false,
        solve_stats,
    }
}

fn skipped_bandpass_report(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
) -> Result<BandpassSolveReport, BandpassSolveError> {
    let select = |selection: &MsSelection| {
        prepare_calibration_solve(
            ms,
            selection,
            &RefAntSelector::Unspecified,
            &[],
            false,
            true,
            crate::solve::grouping::PriorCalibrationMode::Data,
        )
    };
    let (selection_failed, context) = match select(&request.selection) {
        Ok(context) => (false, context),
        Err(error) if selection_rejected(&error) => (true, select(&MsSelection::default())?),
        Err(error) => return Err(error.into()),
    };
    Ok(BandpassSolveReport {
        selection_failed,
        ..empty_report(request, -1, context.selected_data, SolveStats::default())
    })
}

fn selection_rejected(error: &crate::solve::GainSolveError) -> bool {
    matches!(
        error,
        crate::solve::GainSolveError::EmptySelection
            | crate::solve::GainSolveError::OpenMeasurementSet {
                source: MsError::VersionError(_) | MsError::InvalidInput(_),
                ..
            }
    )
}

fn validate_bandpass_request(request: &BandpassSolveRequest) -> Result<(), BandpassSolveError> {
    crate::solve::validate_smodel(
        request.smodel,
        request.band_type == BandpassType::B || request.cuts.is_some(),
    )?;
    crate::solve::validate_solve_interval(request.solve_interval)?;
    if request.band_type == BandpassType::BPoly
        && request.cuts.is_none()
        && request.frequency_interval.is_some()
    {
        return Err(BandpassSolveError::UnsupportedConfiguration {
            reason: "frequency intervals require bandtype='B'".to_string(),
        });
    }
    if request.band_type == BandpassType::BPoly
        && request.cuts.is_none()
        && matches!(
            request.refant,
            RefAntSelector::Unspecified | RefAntSelector::Expression { .. }
        )
    {
        return Err(BandpassSolveError::UnsupportedConfiguration {
            reason: "bandpass requires one reference antenna by ID or name".to_string(),
        });
    }
    if matches!(request.band_type, BandpassType::BPoly)
        && request.cuts.is_none()
        && (request.amplitude_degree > 31 || request.phase_degree > 31)
    {
        return Err(BandpassSolveError::UnsupportedConfiguration {
            reason: "BPOLY degree must be <= 31".to_string(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct BandpassSolutionRow {
    time_seconds: f64,
    interval_seconds: f64,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    refant_id: i32,
    scan_number: i32,
    observation_id: i32,
    channel_count: usize,
    gains: Vec<Complex32>,
    flags: Vec<bool>,
    param_errors: Vec<f32>,
    snrs: Vec<f32>,
}

#[derive(Debug, Clone)]
struct BPolySolutionRow {
    time_seconds: f64,
    interval_seconds: f64,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    scan_number: i32,
    observation_id: i32,
    valid_domain_hz: [f64; 2],
    scale_factor: Complex32,
    amp_coefficients: Vec<Vec<f64>>,
    phase_coefficients: Vec<Vec<f64>>,
}

/// Solve one bandpass interval channel by channel, both receptors of a
/// channel together as CASA's `VisCalSolver2` does for B. A channel CASA
/// cannot solve is flagged for every antenna, and an interval with no
/// solved channel gets no rows, as `Calibrater::genericGatherAndSolve` keeps
/// it only when one channel succeeds. `cuts` apply in each channel: an
/// antenna short of baselines is not solved, and a solution whose SNR is not
/// above `min_snr` is flagged.
fn solve_bandpass_group(
    group: &SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    refant_id: i32,
    request: &BandpassSolveRequest,
    solve_mode: GainSolveMode,
    (correlation_dependent_flags, independent_input_flags): (bool, bool),
    control: SolveControl<'_>,
) -> Result<(Vec<BandpassSolutionRow>, BTreeSet<i32>), BandpassSolveError> {
    let BandpassSolveRequest {
        combine,
        cuts,
        band_type,
        ..
    } = *request;
    let channel_count = group.solution_channels;
    // CASA solves the last channel first and starts each channel from the
    // last solution found (`Calibrater::genericGatherAndSolve`).
    let mut solved_channels = vec![None; channel_count];
    let mut start = None::<SolvedGraphs>;
    for (channel, graphs) in group.channel_graphs.iter().enumerate().rev() {
        control.check()?;
        #[cfg(feature = "solve-test-hooks")]
        control.observe(crate::solve_test_hooks::Checkpoint::BandpassChannelStarted(
            channel,
        ));
        let solved = solve_graphs(
            graphs,
            GainType::G,
            solve_mode,
            SolveGraphOptions {
                refant_id,
                min_baselines_per_antenna: cuts.map_or(0, |cuts| cuts.min_baselines_per_antenna),
                group_seed: None,
                correlation_dependent_flags,
                defer_reference: band_type == BandpassType::B,
                robustness: None,
                control,
            },
            if band_type == BandpassType::BPoly {
                start
                    .as_ref()
                    .map_or(SolveStart::Guess, SolveStart::LegacyChannel)
            } else if channel + 1 == channel_count {
                SolveStart::BandpassGuess {
                    scalar: group.single_parallel_hand,
                    independent_input_flags,
                }
            } else {
                SolveStart::PreviousChannel(start.as_ref())
            },
        );
        control.check()?;
        if solved.is_some() || band_type == BandpassType::B {
            start.clone_from(&solved);
        }
        solved_channels[channel] = solved;
        #[cfg(feature = "solve-test-hooks")]
        control.observe(crate::solve_test_hooks::Checkpoint::BandpassChannelFinished(channel));
    }
    // The antennas the channel solves referred their gains to.
    let refants = solved_channels
        .iter()
        .flatten()
        .map(|solved| solved.refant_id)
        .collect::<BTreeSet<_>>();
    if refants.is_empty() {
        return Ok((Vec::new(), refants));
    }
    // CASA keeps the previous formal errors and SNR when a channel fails
    // to solve, while flagging its gains. It traverses the channels backward.
    let mut channel_errors = solved_channels
        .iter()
        .rev()
        .scan(None, |previous, channel| {
            if let Some(channel) = channel {
                *previous = Some(channel);
            }
            Some(*previous)
        })
        .collect::<Vec<_>>();
    channel_errors.reverse();

    let mut antenna_ids = available_antennas.clone();
    antenna_ids.extend(group.antenna_ids.iter().copied());
    if refant_id >= 0 {
        antenna_ids.insert(refant_id);
    }
    let output_field_ids = if combine.fields && !combine.scans {
        group.field_ids.iter().copied().collect::<Vec<_>>()
    } else {
        vec![group.field_id]
    };
    let scan_number = group.scan_number();
    let interval_seconds = group.total_interval / group.sample_rows.max(1) as f64;

    let mut solution_rows = Vec::new();
    for field_id in output_field_ids {
        control.check()?;
        for antenna_id in antenna_ids.iter().copied() {
            control.check()?;
            let mut gains = Vec::with_capacity(2 * channel_count);
            let mut flags = Vec::with_capacity(2 * channel_count);
            let mut param_errors = Vec::with_capacity(2 * channel_count);
            let mut snrs = Vec::with_capacity(2 * channel_count);
            for (solved, estimates) in solved_channels.iter().zip(&channel_errors) {
                for receptor_index in 0..2 {
                    let receptor = solved
                        .as_ref()
                        .and_then(|solved| solved.receptors.get(receptor_index));
                    let snr = receptor.and_then(|fit| fit.snr.get(&antenna_id)).copied();
                    let solution = receptor.and_then(|fit| fit.gains.get(&antenna_id));
                    let solved = receptor.is_some_and(|fit| fit.solved.contains(&antenna_id));
                    let above_min_snr =
                        cuts.is_none_or(|cuts| snr.is_some_and(|snr| snr > cuts.min_snr));
                    gains.push(solution.copied().unwrap_or(Complex32::new(1.0, 0.0)));
                    flags.push(!solved || !above_min_snr);
                    let estimates = estimates.and_then(|fit| fit.receptors.get(receptor_index));
                    param_errors.push(
                        estimates
                            .and_then(|fit| fit.param_error.get(&antenna_id))
                            .copied()
                            .unwrap_or_default(),
                    );
                    snrs.push(
                        estimates
                            .and_then(|fit| fit.snr.get(&antenna_id))
                            .copied()
                            .unwrap_or_default(),
                    );
                }
            }
            solution_rows.push(BandpassSolutionRow {
                time_seconds: group.solution_time,
                interval_seconds,
                field_id,
                spw_id: group.spw_id,
                antenna_id,
                refant_id: -1,
                scan_number,
                observation_id: group.observation_id,
                channel_count,
                gains,
                flags,
                param_errors,
                snrs,
            });
        }
    }

    Ok((solution_rows, refants))
}

fn write_bandpass_caltable(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    refant_id: i32,
    rows: &[BandpassSolutionRow],
    channel_bin_widths: &BTreeMap<i32, usize>,
    selected_rows: &[SelectedSolveRow],
    (mode, control): (crate::output::CalibrationOutputMode, SolveControl<'_>),
) -> Result<BandpassSolveReport, BandpassSolveError> {
    control.check()?;
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::array_variable(COL_CPARAM, casa_core::PrimitiveType::Complex32, Some(2)),
        ColumnSchema::array_variable(COL_PARAMERR, casa_core::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_FLAG, casa_core::PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable(COL_SNR, casa_core::PrimitiveType::Float32, Some(2)),
        // CASA declares WEIGHT but leaves it undefined in bandpass solutions.
        ColumnSchema::array_variable(COL_WEIGHT, casa_core::PrimitiveType::Float32, Some(2)),
    ])
    .expect("valid bandpass schema");
    let mut writer = CalibrationTableWriter::with_mode(
        ms,
        CalibrationTableDescriptor {
            output: &request.output_table,
            schema,
            subtype: "B Jones",
            parameter_type: Some("Complex"),
            measurement_set_name: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        mode,
    )?;

    for (index, row) in rows.iter().enumerate() {
        if index.is_multiple_of(256) {
            control.check()?;
        }
        let receptor_count = 2;
        writer.append(RecordValue::new(vec![
            RecordField::new(
                COL_TIME,
                Value::Scalar(ScalarValue::Float64(row.time_seconds)),
            ),
            RecordField::new(
                COL_FIELD_ID,
                Value::Scalar(ScalarValue::Int32(row.field_id)),
            ),
            RecordField::new(
                COL_SPECTRAL_WINDOW_ID,
                Value::Scalar(ScalarValue::Int32(row.spw_id)),
            ),
            RecordField::new(
                COL_ANTENNA1,
                Value::Scalar(ScalarValue::Int32(row.antenna_id)),
            ),
            RecordField::new(
                COL_ANTENNA2,
                Value::Scalar(ScalarValue::Int32(row.refant_id)),
            ),
            // CASA writes every solution's INTERVAL as 0 (`SolvableVisCal::keepNCT`).
            RecordField::new(COL_INTERVAL, Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new(
                COL_SCAN_NUMBER,
                Value::Scalar(ScalarValue::Int32(row.scan_number)),
            ),
            RecordField::new(
                COL_OBSERVATION_ID,
                Value::Scalar(ScalarValue::Int32(row.observation_id)),
            ),
            RecordField::new(
                COL_CPARAM,
                Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[receptor_count, row.channel_count]).f(),
                        row.gains.clone(),
                    )
                    .expect("bandpass gains should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_PARAMERR,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[receptor_count, row.channel_count]).f(),
                        row.param_errors.clone(),
                    )
                    .expect("bandpass paramerr should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_FLAG,
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(
                        IxDyn(&[receptor_count, row.channel_count]).f(),
                        row.flags.clone(),
                    )
                    .expect("bandpass flags should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_SNR,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[receptor_count, row.channel_count]).f(),
                        row.snrs.clone(),
                    )
                    .expect("bandpass snr should reshape to receptor x channel"),
                )),
            ),
        ]))?;
    }

    writer.copy_subtables(&[
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
        (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
    ])?;
    metadata::write(
        &writer.output_path().join("SPECTRAL_WINDOW"),
        &rows.iter().map(|row| row.spw_id).collect(),
        channel_bin_widths,
        selected_rows,
        request.combine.spectral_windows,
    )?;
    writer.save_main_with_control(control)?;

    let field_ids = rows.iter().map(|row| row.field_id).collect::<BTreeSet<_>>();
    let spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    let channel_count = rows.first().map(|row| row.channel_count).unwrap_or(0);
    Ok(BandpassSolveReport {
        output_table: request.output_table.clone(),
        table_subtype: "B Jones".to_string(),
        refant_antenna_id: refant_id,
        field_ids: field_ids.iter().copied().collect(),
        spectral_window_ids: spw_ids.iter().copied().collect(),
        solution_row_count: rows.len(),
        channel_count,
        selected_data: SelectedData::default(),
        selection_failed: false,
        solve_stats: SolveStats::default(),
    })
}

#[cfg(test)]
mod tests;
