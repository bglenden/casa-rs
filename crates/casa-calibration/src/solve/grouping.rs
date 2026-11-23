// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection, bucketing, and accumulation for `gaincal` and `bandpass`.

pub(super) mod additive_polynomial;
mod inputs;
pub(crate) mod leakage;
mod legacy_baseline;
pub(crate) mod polynomial;
mod samples;
mod spectra;
mod time;
mod visibility_models;
mod weights;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_ms::ChannelSelection;
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_ms::{MsError, ResolvedMsSelectionRow};
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, ArrayView2};

use super::{
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainType, RefAntSelector, get_f64, get_i32,
};
use crate::execute::{EvaluatedApplyRow, PreappliedRows};
use crate::session::{resolve_calibration_selection, resolve_casa_calibration_selection};
use crate::solve::kernel::{RobustSample, SolveGraph};
pub(crate) use inputs::{PriorCalibrationMode, SolveInputBatch, load_preapplied_rows};
use inputs::{input_cell, read_input_batch};
pub(crate) use samples::median_with_even_mean;
use samples::{
    SolveAccumulationKey, SolveEdgeAccumulation, SolveRowInputs, SolveRowLayout, SolveRowLayouts,
    two_dimensional,
};
pub(super) use spectra::DelaySpectrum;
pub(crate) use time::SolveInputRows;
use time::{AveragedBaseline, FirstTBuffer, WeightedTime, bucketed_rows, first_t_buffers};
pub(crate) use visibility_models::VisibilityModels;
pub(super) use weights::VisibilityBuffer;
use weights::{SpectralWeights, solve_weights};

pub(crate) fn build_solve_groups(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    preapplied_rows: Option<&PreappliedRows>,
    channel_bin_widths: &BTreeMap<i32, usize>,
    mut options: SolveGroupOptions,
) -> Result<BTreeMap<(SolveBaseKey, SolveBucketKey), SolveAccumulator>, GainSolveError> {
    if preapplied_rows.is_some()
        && !options.channelized
        && matches!(options.gain_type, GainType::G | GainType::T)
        && matches!(
            options.input_rows,
            SolveInputRows::Interferometry | SolveInputRows::GainTool
        )
    {
        // CalSolvingVi2 shares the input flags before applying priors.
        // Flags added by a failed prior stay on the affected correlation.
        options.correlation_dependent_flags = true;
    }
    let mut groups = BTreeMap::<(SolveBaseKey, SolveBucketKey), SolveAccumulator>::new();
    let mut models = super::model::ModelInputs::new(ms, rows, options.model_source)?;
    let only_autocorrelations = options.input_rows == SolveInputRows::Autocorrelations;
    let baseline_solve = options.input_rows.is_baseline();
    let keep_autocorrelations = matches!(
        options.input_rows,
        SolveInputRows::GainTool | SolveInputRows::GatherOnlyTool { .. }
    ) || options.input_rows.is_accor();
    let seed_combined_g = !baseline_solve
        && !options.input_rows.is_accor()
        && options.gain_type == GainType::G
        && !options.channelized
        && !options.robust_seed
        && (options.combine.spectral_windows
            || options.combine.fields
            || options.combine.observations);
    let needs_first_buffer = options.input_rows.is_accor()
        || matches!(options.gain_type, GainType::T | GainType::KCross)
        || options.robust_seed
        || seed_combined_g;
    let legacy_spline = options.gain_type == GainType::GSpline;
    let include_autocorrelations = baseline_solve
        || keep_autocorrelations
        || legacy_spline
        || needs_first_buffer
        || models.needs_buffers()
        || options.combine.scans;
    let mut ordered = bucketed_rows(
        rows,
        options.solve_interval,
        options.combine,
        include_autocorrelations,
        options.legacy_intervals(),
        baseline_solve,
    );
    let seed_buffers = if !legacy_spline
        && (needs_first_buffer
            || options.combine.observations
            || options.combine.spectral_windows
            || options.combine.scans)
    {
        first_t_buffers(&ordered, options.preaverage_max_span)
    } else {
        BTreeMap::new()
    };
    models.bind_buffers(ms, &ordered, options.combine)?;
    if only_autocorrelations {
        ordered.retain(|(row, _, _)| row.antenna1 == row.antenna2);
    } else if include_autocorrelations
        && !legacy_spline
        && !baseline_solve
        && !keep_autocorrelations
    {
        ordered.retain(|(row, _, _)| row.antenna1 != row.antenna2);
    }
    models.phase_times =
        weights::chunk_phase_times(&ordered, options.combine, models.dynamic_fields());
    let forward_model = preapplied_rows.and_then(|rows| rows.model.as_ref());
    let mut layouts = SolveRowLayouts::new(
        ms,
        options.rotate_model && forward_model.is_none(),
        &models,
        channel_bin_widths,
    )?;
    layouts.forward_model = forward_model;
    let spectral_weights =
        if options.legacy_chan_selection() || options.gain_type == GainType::GSpline {
            SpectralWeights::default()
        } else {
            SpectralWeights::new(ms, &ordered, options.combine)?
        };
    let pipelined = overlap_inputs(ordered.len());
    if std::env::var_os("CASA_RS_CALIBRATION_TIMING").is_some() {
        eprintln!("calibration_workers {}", if pipelined { 2 } else { 1 });
    }
    let read_uncalibrated_data = preapplied_rows.is_none();
    let read = |batch: &[BucketedRow<'_>]| {
        let _phase = crate::timing::Phase::start("solve_read_decode");
        let indices = batch
            .iter()
            .map(|(row, _, _)| row.row_index)
            .collect::<Vec<_>>();
        read_input_batch(
            ms,
            &indices,
            (
                read_uncalibrated_data,
                batch
                    .iter()
                    .any(|(row, _, _)| models.uses_fourier(row.field_id)),
            ),
            batch
                .iter()
                .any(|(row, _, _)| models.reads_column(row.field_id)),
            batch
                .iter()
                .any(|(row, _, _)| models.uses_virtual_model(row.field_id)),
            batch
                .iter()
                .map(|row| spectral_weights.contains(row))
                .collect(),
            options.polynomial_bandpass && !channel_bin_widths.is_empty(),
        )
    };
    let mut consume = |batch: &[BucketedRow<'_>], inputs: SolveInputBatch| {
        let _accumulate_phase = crate::timing::Phase::start("solve_accumulate");
        for (index, &(row, base_key, bucket_key)) in batch.iter().enumerate() {
            let group = groups.entry((base_key, bucket_key)).or_insert_with(|| {
                let buffer = seed_buffers.get(&(base_key, bucket_key));
                let mut group = SolveAccumulator::new(
                    row.field_id,
                    buffer.map_or(row.data_spw_id, |first| first.spw_id),
                    buffer.map_or(row.observation_id, |first| first.observation_id),
                );
                group.first_t_buffer = buffer.copied();
                group.first_g_graphs = seed_combined_g.then(Box::default);
                group.direct_gain = options.input_rows.tracks_gather().then(Default::default);
                group
            });
            group.apply_bucket_identity(row, &options);
            let preapplied = preapplied_rows.and_then(|rows| rows.data.get(&row.row_index));
            group.observe(
                ms,
                row,
                preapplied,
                &options,
                &mut layouts,
                (&inputs, index),
            )?;
            if matches!(options.input_rows, SolveInputRows::GatherOnlyTool { .. })
                && (group.attempted || group.gather_attempted())
            {
                return Err(GainSolveError::GatherOnlyCannotFit);
            }
        }
        Ok::<_, GainSolveError>(())
    };
    if pipelined {
        std::thread::scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let producer_rows = &ordered;
            scope.spawn(move || {
                for batch in producer_rows.chunks(256) {
                    let input = read(batch);
                    let failed = input.is_err();
                    if sender.send(input).is_err() || failed {
                        break;
                    }
                }
            });
            // Drop the receiver before scope joins the reader on an error,
            // unblocking a producer that is waiting to send its next batch.
            (|| {
                for (batch, input) in ordered.chunks(256).zip(receiver) {
                    consume(batch, input?)?;
                }
                Ok::<_, GainSolveError>(())
            })()
        })?;
    } else {
        for batch in ordered.chunks(256) {
            consume(batch, read(batch)?)?;
        }
    }

    Ok(groups)
}

pub(super) type BucketedRow<'a> = (&'a SelectedSolveRow, SolveBaseKey, SolveBucketKey);

/// One reader and the calling accumulation thread, within the CPU budget.
fn overlap_inputs(rows: usize) -> bool {
    let available = std::thread::available_parallelism().map_or(1, usize::from);
    let requested = std::env::var("RAYON_NUM_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(available);
    rows > 256 && available.min(requested) >= 2
}

fn ms_read_error(ms: &MeasurementSet, source: casa_tables::TableError) -> GainSolveError {
    GainSolveError::OpenMeasurementSet {
        path: ms
            .path()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<in-memory>".to_string()),
        source: MsError::from(source),
    }
}

pub(crate) fn all_antenna_ids(ms: &MeasurementSet) -> Result<BTreeSet<i32>, GainSolveError> {
    let antenna = ms
        .antenna()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    Ok((0..antenna.row_count())
        .map(|row| i32::try_from(row).expect("antenna row count should fit in i32"))
        .collect())
}

pub(crate) fn resolve_refants(
    ms: &MeasurementSet,
    selector: &RefAntSelector,
) -> Result<Vec<i32>, GainSolveError> {
    match selector {
        RefAntSelector::Unspecified => Ok(Vec::new()),
        RefAntSelector::AntennaId(id) => Ok(vec![*id]),
        RefAntSelector::Expression { selection, .. } => ms
            .resolve_reference_antennas(selection)
            .map(|ids| ids.into_iter().map(|id| id as i32).collect())
            .map_err(|error| GainSolveError::ResolveRefAnt {
                selector: selection.clone(),
                reason: error.to_string(),
            }),
        RefAntSelector::AntennaName(name) => {
            let antenna = ms
                .antenna()
                .map_err(|error| GainSolveError::ResolveRefAnt {
                    selector: name.clone(),
                    reason: error.to_string(),
                })?;
            for row in 0..antenna.row_count() {
                if antenna
                    .name(row)
                    .map_err(|error| GainSolveError::ResolveRefAnt {
                        selector: name.clone(),
                        reason: error.to_string(),
                    })?
                    == *name
                {
                    return Ok(vec![row as i32]);
                }
            }
            Err(GainSolveError::ResolveRefAnt {
                selector: name.clone(),
                reason: "no ANTENNA.NAME match".to_string(),
            })
        }
    }
}

pub(crate) fn collect_selected_rows(
    ms: &MeasurementSet,
    selection: &MsSelection,
    full_selection: bool,
) -> Result<Vec<SelectedSolveRow>, GainSolveError> {
    let selected_rows = if full_selection {
        resolve_casa_calibration_selection(ms, selection)
    } else {
        resolve_calibration_selection(ms, selection)
    }
    .map_err(|error| GainSolveError::OpenMeasurementSet {
        path: ms
            .path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<in-memory>".to_string()),
        source: match error {
            casa_ms::MsSelectionError::Domain(source) => source,
            other => MsError::InvalidInput(other.to_string()),
        },
    })?;
    if selected_rows.selected_rows.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }

    let channels = &selected_rows.channel_selections;
    selected_rows
        .selected_rows
        .into_iter()
        .map(|row| {
            let channel_selection = if full_selection {
                channels.get(&row.spectral_window_id).cloned()
            } else {
                selection.channel_selection_for_spw(row.spectral_window_id)
            };
            selected_solve_row(ms, &row, channel_selection)
        })
        .collect()
}

fn selected_solve_row(
    ms: &MeasurementSet,
    row: &ResolvedMsSelectionRow,
    channel_selection: Option<ChannelSelection>,
) -> Result<SelectedSolveRow, GainSolveError> {
    Ok(SelectedSolveRow {
        row_index: row.row_index,
        array_id: get_i32(ms.main_table(), row.row_index, "ARRAY_ID")?,
        field_id: row.field_id,
        observation_id: get_i32(ms.main_table(), row.row_index, "OBSERVATION_ID")?,
        data_desc_id: row.data_desc_id,
        data_spw_id: row.spectral_window_id,
        channel_selection,
        antenna1: row.antenna1_id,
        antenna2: row.antenna2_id,
        time_seconds: row.time_mjd_seconds,
        time_centroid_seconds: get_f64(ms.main_table(), row.row_index, "TIME_CENTROID")?,
        interval_seconds: get_f64(ms.main_table(), row.row_index, "INTERVAL")?,
        scan_number: get_i32(ms.main_table(), row.row_index, "SCAN_NUMBER")?,
        flag_row: row.flag_row,
    })
}

pub(crate) fn correlation_types_for_ddid(
    ms: &MeasurementSet,
    data_desc_id: i32,
) -> Result<Vec<i32>, GainSolveError> {
    let dd = ms
        .data_description()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    let pol = ms
        .polarization()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    let row = usize::try_from(data_desc_id).map_err(|_| {
        GainSolveError::UnsupportedCorrelationLayout {
            data_desc_id,
            correlation_types: Vec::new(),
        }
    })?;
    let pol_id = dd
        .polarization_id(row)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    pol.corr_type(usize::try_from(pol_id).unwrap_or(usize::MAX))
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveGroupOptions {
    pub(crate) input_rows: SolveInputRows,
    pub(crate) gain_type: GainType,
    pub(crate) model_source: GainSolveModelSource,
    pub(crate) point_stokes: [f32; 4],
    pub(crate) rotate_model: bool,
    pub(crate) solve_interval: GainSolveInterval,
    pub(crate) combine: GainSolveCombine,
    /// Maximum time from the first sample in an average of one baseline.
    pub(crate) preaverage_max_span: Option<f64>,
    pub(crate) correlation_dependent_flags: bool,
    pub(crate) robust_seed: bool,
    /// Solve each data channel apart (`bandpass`) rather than the selected
    /// channels together (`gaincal`).
    pub(crate) channelized: bool,
    /// BPOLY uses the old visibility iterator and scalar row weights.
    pub(crate) polynomial_bandpass: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SelectedSolveRow {
    pub(crate) row_index: usize,
    pub(crate) array_id: i32,
    pub(crate) field_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) data_desc_id: i32,
    pub(crate) data_spw_id: i32,
    pub(crate) channel_selection: Option<ChannelSelection>,
    pub(crate) antenna1: i32,
    pub(crate) antenna2: i32,
    pub(crate) time_seconds: f64,
    pub(crate) time_centroid_seconds: f64,
    pub(crate) interval_seconds: f64,
    pub(crate) scan_number: i32,
    pub(crate) flag_row: bool,
}

/// What the accumulation reads of a sample's row: its identities, times and
/// scan, from a MAIN row or from any other source of samples.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveSampleRow {
    pub(crate) field_id: i32,
    pub(crate) data_desc_id: i32,
    pub(crate) antenna1: i32,
    pub(crate) antenna2: i32,
    pub(crate) time_seconds: f64,
    pub(crate) time_centroid_seconds: f64,
    pub(crate) interval_seconds: f64,
    pub(crate) scan_number: i32,
}

impl SolveSampleRow {
    fn of(row: &SelectedSolveRow) -> Self {
        Self {
            field_id: row.field_id,
            data_desc_id: row.data_desc_id,
            antenna1: row.antenna1,
            antenna2: row.antenna2,
            time_seconds: row.time_seconds,
            time_centroid_seconds: row.time_centroid_seconds,
            interval_seconds: row.interval_seconds,
            scan_number: row.scan_number,
        }
    }
}

/// One sample's arrays for [`SolveAccumulator::observe_selected_samples`], each
/// `[correlation, channel]` as casacore stores a MAIN cell: the data, the
/// flags the solve uses (row flags already folded in), the solve weights and
/// the model, if any.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveSamples<'a> {
    pub(crate) data: ArrayView2<'a, Complex32>,
    pub(crate) flags: ArrayView2<'a, bool>,
    pub(crate) weights: ArrayView2<'a, f32>,
    pub(crate) model: Option<ArrayView2<'a, Complex32>>,
    pub(crate) point_model: Option<&'a [Complex32]>,
}

#[derive(Clone, Copy)]
struct SelectedChannels<'a> {
    indices: &'a [usize],
    all_channels: bool,
    bin_width: Option<usize>,
    excluded: &'a [bool],
    bin_weights: Option<ArrayView2<'a, f32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SolveBaseKey {
    pub(crate) array_id: i32,
    /// Consecutive visits of one field in a baseline solve across scans.
    pub(crate) field_visit: usize,
    pub(crate) field_id: i32,
    pub(crate) spw_id: i32,
    pub(crate) data_desc_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) scan_number: i32,
}

impl SolveBaseKey {
    fn for_row(row: &SelectedSolveRow, combine: GainSolveCombine) -> Self {
        Self {
            array_id: row.array_id,
            field_visit: 0,
            field_id: if combine.fields { 0 } else { row.field_id },
            spw_id: if combine.spectral_windows {
                0
            } else {
                row.data_spw_id
            },
            data_desc_id: if combine.spectral_windows {
                0
            } else {
                row.data_desc_id
            },
            observation_id: if combine.observations {
                0
            } else {
                row.observation_id
            },
            scan_number: if combine.scans { 0 } else { row.scan_number },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum SolveBucketKey {
    Infinite,
    Integration { time_bits: u64, interval_bits: u64 },
    Seconds(u64),
}

/// The samples of one solution interval, collapsed per baseline,
/// correlation and solution channel, and the interval's metadata.
#[derive(Debug, Clone)]
pub(crate) struct SolveAccumulator {
    pub(super) fringe_buffers: BTreeSet<(i32, i32, u64)>,
    pub(crate) field_id: i32,
    pub(crate) spw_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) min_time: f64,
    pub(crate) max_time: f64,
    pub(crate) unique_time_sum: f64,
    pub(crate) unique_time_count: usize,
    pub(crate) total_interval: f64,
    pub(crate) sample_rows: usize,
    pub(crate) field_ids: BTreeSet<i32>,
    pub(crate) scan_numbers: BTreeSet<i32>,
    pub(crate) antenna_ids: BTreeSet<i32>,
    unique_time_bits: BTreeSet<u64>,
    /// Data channels per solution: 1 when the selected channels solve
    /// together, else the row's channel count.
    pub(crate) solution_channels: usize,
    /// Correlation counts of the buffers gathered for an M/Mf solution.
    pub(super) baseline_correlation_counts: BTreeSet<usize>,
    /// A single parallel hand in channelized G solves occupies parameter zero.
    pub(crate) single_parallel_hand: bool,
    pub(crate) polynomial: Option<polynomial::PolynomialAccumulation>,
    /// `[solution channel][receptor]`, filled by [`Self::finalize_for_solve`].
    pub(crate) channel_graphs: Vec<Vec<SolveGraph>>,
    /// The solution's TIME, set by [`Self::finalize_for_solve`].
    pub(crate) solution_time: f64,
    /// Whether a sample entered the solve: CASA attempts a solution for
    /// such an interval (`SDBList::Ok`, positive unflagged weight after the
    /// model division), and sets its window's gain-table channel then,
    /// whether or not the solve succeeds.
    pub(crate) attempted: bool,
    first_t_buffer: Option<FirstTBuffer>,
    pub(crate) first_g_graphs: Option<Box<[SolveGraph; 2]>>,
    direct_gain: Option<samples::DirectGainGather>,
    legacy_baselines: Option<legacy_baseline::LegacyMuellerGather>,
    pub(super) additive_polynomial: Option<additive_polynomial::Gather>,
    parallel_hands: usize,
    baseline_centroids: HashMap<AveragedBaseline, WeightedTime>,
    preaverage_starts: HashMap<AveragedBaseline, f64>,
    receptor_accumulations: Vec<HashMap<SolveAccumulationKey, Vec<SolveEdgeAccumulation>>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SolveEdgeStats {
    pub(crate) weighted_sample_power: f64,
    pub(crate) raw_weighted_sample_power: f64,
    pub(crate) sample_count: usize,
    pub(crate) collapsed_count: usize,
}

impl SolveEdgeStats {
    pub(crate) fn add(&mut self, other: Self) {
        self.weighted_sample_power += other.weighted_sample_power;
        self.raw_weighted_sample_power += other.raw_weighted_sample_power;
        self.sample_count += other.sample_count;
        self.collapsed_count += other.collapsed_count;
    }
}

impl SolveAccumulator {
    pub(crate) fn scan_number(&self) -> i32 {
        self.first_t_buffer.map_or_else(
            || self.scan_numbers.first().copied().unwrap_or(0),
            |first| first.scan_number,
        )
    }

    pub(crate) fn new(field_id: i32, spw_id: i32, observation_id: i32) -> Self {
        Self {
            fringe_buffers: BTreeSet::new(),
            field_id,
            spw_id,
            observation_id,
            min_time: f64::INFINITY,
            max_time: f64::NEG_INFINITY,
            unique_time_sum: 0.0,
            unique_time_count: 0,
            total_interval: 0.0,
            sample_rows: 0,
            field_ids: BTreeSet::new(),
            scan_numbers: BTreeSet::new(),
            antenna_ids: BTreeSet::new(),
            unique_time_bits: BTreeSet::new(),
            solution_channels: 1,
            baseline_correlation_counts: BTreeSet::new(),
            single_parallel_hand: false,
            polynomial: None,
            channel_graphs: Vec::new(),
            solution_time: 0.0,
            attempted: false,
            first_t_buffer: None,
            first_g_graphs: None,
            direct_gain: None,
            legacy_baselines: None,
            additive_polynomial: None,
            parallel_hands: 1,
            baseline_centroids: HashMap::new(),
            preaverage_starts: HashMap::new(),
            receptor_accumulations: Vec::new(),
        }
    }

    /// Add one MAIN row: its unflagged, positively weighted samples divided by
    /// the model, each weighted by its solve weight times |model|² as
    /// `VisEquation::divideCorrByModel` does, and its TIME_CENTROID.
    fn observe(
        &mut self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        preapplied_row: Option<&EvaluatedApplyRow>,
        options: &SolveGroupOptions,
        layouts: &mut SolveRowLayouts,
        batch: (&SolveInputBatch, usize),
    ) -> Result<(), GainSolveError> {
        let inputs = read_row_inputs(
            ms,
            row,
            preapplied_row,
            layouts.models.reads_column(row.field_id),
            batch,
        )?;
        let channel_count = inputs.data.shape()[1];
        let layout = match layouts.by_ddid.entry(row.data_desc_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => entry.insert(SolveRowLayout {
                correlations: correlation_types_for_ddid(ms, row.data_desc_id)?,
                channel_count: 0,
                selection: None,
                channels: vec![],
                excluded: vec![],
                all_channels: false,
            }),
        };
        if layout.channel_count != channel_count || layout.selection != row.channel_selection {
            layout.select_channels(row, channel_count, options.legacy_chan_selection())?;
        }
        if options.input_rows.is_baseline() {
            self.baseline_correlation_counts
                .insert(layout.correlations.len());
        }
        let raw_flags = if preapplied_row.is_some() && layouts.models.uses_fourier(row.field_id) {
            match input_cell(ms, row.row_index, batch, "FLAG")? {
                ArrayValue::Bool(flags) if flags.shape() == inputs.data.shape() => flags,
                other => return Err(unsupported_shape("FLAG", other)),
            }
        } else {
            inputs.flags.as_ref()
        };
        let mut predicted = layouts.models.predict(
            ms,
            row,
            &layout.correlations,
            raw_flags,
            batch.0.cell("UVW", batch.1),
        )?;
        let rotated = layouts
            .rotation
            .as_ref()
            .map(|state| {
                if predicted.is_none() {
                    predicted = inputs.model.cloned();
                }
                samples::rotate_model(
                    ms,
                    row,
                    state,
                    &layout.correlations,
                    options.point_stokes,
                    predicted.as_mut(),
                )
            })
            .transpose()?
            .flatten();
        let mut samples = inputs.samples();
        let model_flags = if let Some(forward) = layouts.forward_model {
            let model = predicted
                .take()
                .or_else(|| inputs.model.cloned())
                .unwrap_or_else(|| {
                    ArrayD::from_shape_fn(inputs.data.raw_dim(), |index| {
                        samples::point_visibility(
                            options.point_stokes,
                            layout.correlations[index[0]],
                            layout.correlations[0],
                        )
                    })
                });
            let (model, flags) = forward
                .corrupt(row.row_index, &layout.correlations, model, &inputs.flags)
                .map_err(|source| GainSolveError::PriorCalibrationApply {
                    source: Box::new(source),
                })?;
            predicted = Some(model);
            flags
        } else {
            None
        };
        if let Some(flags) = &model_flags {
            samples.flags = two_dimensional(flags);
        }
        if let Some(gains) = preapplied_row.and_then(|row| row.model_gains.as_ref()) {
            let model = predicted.as_ref().or(inputs.model);
            if model.is_some_and(|model| model.shape() != gains.shape()) {
                return Err(GainSolveError::UnsupportedParameterShape {
                    path: "<pre-applied sky model>".into(),
                    shape: model.expect("checked model").shape().to_vec(),
                });
            }
            predicted = Some(samples::corrupt_model(
                gains,
                model,
                rotated.as_deref(),
                &layout.correlations,
                options.point_stokes,
            ));
        }
        if let Some(predicted) = &predicted {
            if predicted.shape() != samples.data.shape() {
                return Err(GainSolveError::UnsupportedParameterShape {
                    path: "<virtual model visibility>".into(),
                    shape: predicted.shape().to_vec(),
                });
            }
            samples.model = Some(two_dimensional(predicted));
        }
        samples.point_model = rotated.as_deref();
        let bin_weights = match batch.0.polynomial_bin_weights(batch.1) {
            Some(ArrayValue::Float32(weights)) if weights.shape() == inputs.data.shape() => {
                Some(two_dimensional(weights))
            }
            Some(other) => return Err(unsupported_shape("WEIGHT_SPECTRUM", other)),
            None => None,
        };
        let channels = SelectedChannels {
            indices: &layout.channels,
            all_channels: layout.all_channels,
            bin_width: layouts.channel_bin_widths.get(&row.data_spw_id).copied(),
            excluded: &layout.excluded,
            bin_weights,
        };
        let row = SolveSampleRow::of(row);
        if options.input_rows == SolveInputRows::LegacyAdditivePolynomial {
            self.observe_metadata(&row, false);
            let gather = self
                .additive_polynomial
                .get_or_insert_with(Default::default);
            self.attempted |= gather.observe(&row, &samples, &layout.correlations, channels)?;
            return Ok(());
        }
        self.observe_selected_samples(&row, &samples, &layout.correlations, channels, options)
    }

    /// Accumulate one sample's `channels` (every channel for a channelized
    /// solve): the arithmetic of [`Self::observe`] on arrays from any source.
    /// The MSv2/MSv4 trial (`solve::batch_trial`) feeds it
    /// from `casa_observation::VisibilityBatch` views without copying them.
    #[cfg(test)]
    pub(crate) fn observe_samples(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlation_types: &[i32],
        channels: &[usize],
        options: &SolveGroupOptions,
    ) -> Result<(), GainSolveError> {
        self.observe_selected_samples(
            row,
            samples,
            correlation_types,
            SelectedChannels {
                indices: channels,
                all_channels: channels.iter().copied().eq(0..samples.data.ncols()),
                bin_width: None,
                excluded: &[],
                bin_weights: None,
            },
            options,
        )
    }

    fn observe_selected_samples(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlation_types: &[i32],
        channels: SelectedChannels<'_>,
        options: &SolveGroupOptions,
    ) -> Result<(), GainSolveError> {
        if options.input_rows == SolveInputRows::LegacyBaseline {
            return self.observe_legacy_baseline(
                row,
                samples,
                correlation_types,
                channels,
                options,
            );
        }
        if options.polynomial_bandpass {
            self.observe_polynomial(
                row,
                samples,
                correlation_types,
                channels,
                options.point_stokes,
            );
            return Ok(());
        }
        if options.channelized {
            self.update_solution_channels(channels, options.gain_type, samples.data.shape())?;
        }

        let baseline_solve = options.input_rows.is_baseline();
        let single_parallel_hand =
            !baseline_solve && options.gain_type == GainType::G && correlation_types.len() == 1;
        self.single_parallel_hand =
            single_parallel_hand && (self.sample_rows == 0 || self.single_parallel_hand);
        let graph_count = match options.gain_type {
            GainType::G | GainType::K | GainType::KMbd | GainType::KAntPos | GainType::GSpline => 2,
            GainType::T | GainType::KCross => {
                self.parallel_hands = if matches!(correlation_types.len(), 2 | 4) {
                    2
                } else {
                    1
                };
                1
            }
        };
        if self.receptor_accumulations.len() < graph_count {
            self.receptor_accumulations
                .resize_with(graph_count, HashMap::new);
        }
        self.observe_metadata(row, options.input_rows.is_fringe());
        let legacy_spline = options.gain_type == GainType::GSpline;
        let shared_flags = (legacy_spline || !options.correlation_dependent_flags)
            .then(|| samples::channel_flags(samples));
        let flagged = shared_flags.as_ref().is_some_and(|flags| {
            flags.iter().all(|flag| *flag)
                || legacy_spline && channels.indices.iter().all(|&channel| flags[channel])
        });
        let no_weighted_data = flagged || samples.weights.iter().all(|weight| *weight <= 0.0);
        if let Some(gather) = &mut self.direct_gain {
            gather.observe(row, samples, channels.indices, shared_flags.as_deref());
        }
        if legacy_spline && flagged {
            return Ok(());
        }
        // CASA gathers each field and data description in a separate buffer.
        // Keep their averages separate until enforcing the solve mode.
        let mut baseline = AveragedBaseline {
            antennas: (row.antenna1, row.antenna2),
            field_id: if options.channelized && !baseline_solve && !options.gain_type.is_delay() {
                0
            } else {
                row.field_id
            },
            data_desc_id: row.data_desc_id,
            start_bits: 0,
        };
        let start_bits = options.preaverage_max_span.map_or(0, |max_span| {
            let start = self
                .preaverage_starts
                .entry(baseline.time_bucket_key(legacy_spline))
                .or_insert(row.time_seconds);
            if row.time_seconds - *start > max_span {
                *start = row.time_seconds;
            }
            start.to_bits()
        });
        baseline.start_bits = start_bits;
        let solution_channels = self.solution_channels;
        let delay = options.gain_type.is_delay();
        let mut row_weight = 0.0_f64;
        for (corr_index, &correlation_type) in correlation_types.iter().enumerate() {
            let receptors =
                options
                    .input_rows
                    .receptors(correlation_types, corr_index, row.data_desc_id)?;
            let parallel_hand = receptors.0 == receptors.1;
            if no_weighted_data {
                continue;
            }
            let graph_index = if graph_count == 1
                || single_parallel_hand
                || delay && correlation_types.len() == 1
            {
                0
            } else {
                receptors.0
            };
            let point = if let Some(rotated) = samples.point_model {
                rotated[corr_index]
            } else {
                samples::point_visibility(
                    options.point_stokes,
                    correlation_type,
                    correlation_types[0],
                )
            };
            let normalization = |channel| {
                samples
                    .model
                    .map_or(point, |model| model[[corr_index, channel]])
            };
            if delay {
                row_weight += samples::centroid_weight(
                    samples,
                    corr_index,
                    channels,
                    shared_flags.as_deref(),
                    normalization,
                    true,
                );
            }
            if !parallel_hand && !baseline_solve && graph_count == 2 {
                // CASA can use cross-hand weights for the averaged baseline's
                // time even though only parallel hands enter these solves.
                if !delay && !legacy_spline {
                    let weight = samples::centroid_weight(
                        samples,
                        corr_index,
                        channels,
                        shared_flags.as_deref(),
                        normalization,
                        options.channelized,
                    );
                    row_weight += self.cross_hand_centroid_weight(weight, options.input_rows);
                }
                continue;
            }
            // Look the baseline/correlation up once per row; channels are
            // dense indices, not independent hash keys for every sample.
            let accumulations = self.receptor_accumulations[graph_index]
                .entry(SolveAccumulationKey {
                    baseline,
                    correlation_index: corr_index,
                    parallel_hand,
                })
                .or_insert_with(|| vec![SolveEdgeAccumulation::default(); solution_channels]);
            let accumulated_weight = if legacy_spline {
                let collapsed = samples::collapse_spline_channels(
                    samples,
                    corr_index,
                    channels.indices,
                    shared_flags.as_deref().expect("old iterator flags"),
                    point,
                );
                accumulations[0].merge(collapsed);
                f64::from(collapsed.weight)
            } else {
                samples::SampleAccumulation {
                    values: accumulations,
                    attempted: &mut self.attempted,
                    channelized: options.channelized,
                    keep_zero_data: baseline_solve
                        || options.input_rows.is_accor()
                        || self.direct_gain.is_some() && row.antenna1 == row.antenna2,
                    retain_zero_model: options.gain_type.is_delay()
                        || options.gain_type == GainType::T
                        || samples.model.is_some()
                        || point.norm_sqr() == 0.0,
                }
                .observe(
                    samples,
                    corr_index,
                    shared_flags.as_deref(),
                    channels,
                    point,
                )
            };
            if !delay {
                row_weight += accumulated_weight;
            }
        }
        if row_weight > 0.0 {
            let time = if legacy_spline {
                row.time_seconds
            } else {
                row.time_centroid_seconds
            };
            self.baseline_centroids
                .entry(baseline.time_bucket_key(legacy_spline))
                .or_default()
                .observe(time, row_weight);
        }
        Ok(())
    }

    /// Collapse each baseline's samples per correlation and solution channel
    /// into the solve graphs, and set the solution TIME.
    ///
    /// A phase-only solve sees each collapsed sample at unit amplitude with
    /// its weight times its squared amplitude. An amplitude-only solve
    /// keeps the collapsed magnitude and the original weight
    /// (`SolveDataBuffer::enforceAPonData`). TIME is
    /// `SDBList::aggregateTimeCentroid`: each baseline's TIME_CENTROID,
    /// averaged over its rows by row weight as time averaging does, weighted
    /// by the baseline's solve weight after that phase-only scaling.
    pub(crate) fn finalize_for_solve(&mut self, solve_mode: GainSolveMode, preserve_samples: bool) {
        self.finalize(solve_mode, preserve_samples, false);
    }

    pub(crate) fn finalize_bandpass(&mut self, solve_mode: GainSolveMode) {
        self.finalize(solve_mode, false, true);
    }

    fn finalize(
        &mut self,
        solve_mode: GainSolveMode,
        preserve_samples: bool,
        direct_centroid: bool,
    ) {
        self.channel_graphs = vec![
            vec![SolveGraph::default(); self.receptor_accumulations.len()];
            self.solution_channels
        ];
        let mut baseline_weights = HashMap::<AveragedBaseline, f64>::new();
        for (receptor, accumulations) in self.receptor_accumulations.iter().enumerate() {
            for (key, channels) in accumulations {
                for (channel_index, accumulation) in channels.iter().enumerate() {
                    if accumulation.weight <= 0.0 {
                        continue;
                    }
                    let averaged =
                        accumulation.weighted_sum / Complex32::new(accumulation.weight, 0.0);
                    let fixed_auto = self.direct_gain.is_some()
                        && key.baseline.antennas.0 == key.baseline.antennas.1;
                    let Some((sample, weight)) = samples::transform_solve_sample(
                        averaged,
                        accumulation.weight,
                        solve_mode,
                        fixed_auto,
                    ) else {
                        continue;
                    };
                    let model = accumulation.model_weight / accumulation.weight;
                    let model_weight = weight * model;
                    let in_first_buffer = self
                        .first_t_buffer
                        .is_some_and(|buffer| buffer.contains(key.baseline));
                    let robust_sample = RobustSample {
                        baseline: key.baseline.antennas,
                        correlation: key.correlation_index,
                        visibility: sample,
                        weight,
                        model,
                        in_first_buffer,
                    };
                    if fixed_auto {
                        // The CASA differentiator skips Jones terms and derivatives for
                        // autocorrelations. The residual and its weight still enter chi-square.
                        let graph = &mut self.channel_graphs[channel_index][receptor];
                        let residual = robust_sample.fixed_residual_power();
                        graph.add_fixed_residual(
                            key.baseline.antennas.0,
                            f64::from(residual) * f64::from(weight),
                        );
                        if preserve_samples {
                            graph.robust_samples.push(robust_sample);
                        }
                        *baseline_weights.entry(key.baseline).or_default() += f64::from(weight);
                        continue;
                    }
                    let edge_stats = SolveEdgeStats {
                        weighted_sample_power: f64::from(weight) * f64::from(sample.norm_sqr()),
                        raw_weighted_sample_power: accumulation.weighted_sample_power,
                        sample_count: accumulation.sample_count,
                        collapsed_count: 1,
                    };
                    self.channel_graphs[channel_index][receptor].accumulate(
                        key.baseline.antennas,
                        model_weight * model,
                        sample * model_weight,
                        edge_stats,
                    );
                    if in_first_buffer && let Some(graphs) = self.first_g_graphs.as_mut() {
                        graphs[receptor].accumulate(
                            key.baseline.antennas,
                            model_weight * model,
                            sample * model_weight,
                            edge_stats,
                        );
                    }
                    if preserve_samples {
                        self.channel_graphs[channel_index][receptor]
                            .robust_samples
                            .push(robust_sample);
                    }
                    *baseline_weights.entry(key.baseline).or_default() += f64::from(weight);
                }
            }
        }

        if direct_centroid {
            self.solution_time = self.bandpass_centroid(baseline_weights);
            return;
        }
        let mut solution_time = WeightedTime::default();
        for (baseline, weight) in baseline_weights {
            let Some(centroid) = self.baseline_centroids.get(&baseline) else {
                continue;
            };
            solution_time.observe_mean(*centroid, weight);
        }
        self.solution_time = solution_time
            .mean()
            .unwrap_or_else(|| self.aggregate_time());
    }

    fn bandpass_centroid(&self, weights: HashMap<AveragedBaseline, f64>) -> f64 {
        let mut baselines = weights.into_iter().collect::<Vec<_>>();
        baselines.sort_unstable_by_key(|(baseline, _)| {
            (
                baseline.field_id,
                baseline.data_desc_id,
                baseline.start_bits,
                baseline.antennas,
            )
        });
        // SDBList::aggregateTimeCentroid uses absolute epochs. Rounding at
        // an integration boundary decides which two solutions CASA blends.
        let mut sum = 0.0;
        let mut total = 0.0;
        for buffer in baselines.chunk_by(|(left, _), (right, _)| {
            (left.field_id, left.data_desc_id, left.start_bits)
                == (right.field_id, right.data_desc_id, right.start_bits)
        }) {
            let mut buffer_sum = 0.0;
            let mut buffer_total = 0.0;
            for (baseline, weight) in buffer {
                let Some(time) = self
                    .baseline_centroids
                    .get(baseline)
                    .and_then(|time| time.mean())
                else {
                    continue;
                };
                let weight = f64::from(*weight as f32);
                buffer_sum += weight * time;
                buffer_total += weight;
            }
            sum += buffer_sum;
            total += buffer_total;
        }
        if total > 0.0 {
            sum / total
        } else {
            self.aggregate_time()
        }
    }
    /// Add `other`'s accumulation, of the same group, to this one before
    /// [`Self::finalize_for_solve`], as if its samples had been observed
    /// here: the fragment work units of the MSv2/MSv4 trial accumulate apart
    /// and merge before the group closes. Sums change order, so results
    /// agree to rounding, not bitwise.
    #[cfg(test)]
    pub(crate) fn merge(&mut self, other: Self) {
        self.min_time = self.min_time.min(other.min_time);
        self.max_time = self.max_time.max(other.max_time);
        for bits in other.unique_time_bits {
            if self.unique_time_bits.insert(bits) {
                self.unique_time_sum += f64::from_bits(bits);
                self.unique_time_count += 1;
            }
        }
        self.total_interval += other.total_interval;
        self.sample_rows += other.sample_rows;
        self.field_ids.extend(other.field_ids);
        self.scan_numbers.extend(other.scan_numbers);
        self.antenna_ids.extend(other.antenna_ids);
        self.solution_channels = self.solution_channels.max(other.solution_channels);
        self.baseline_correlation_counts
            .extend(other.baseline_correlation_counts);
        self.attempted |= other.attempted;
        for (baseline, centroid) in other.baseline_centroids {
            self.baseline_centroids
                .entry(baseline)
                .or_default()
                .merge(centroid);
        }
        if self.receptor_accumulations.len() < other.receptor_accumulations.len() {
            self.receptor_accumulations
                .resize_with(other.receptor_accumulations.len(), HashMap::new);
        }
        for (mine, theirs) in self
            .receptor_accumulations
            .iter_mut()
            .zip(other.receptor_accumulations)
        {
            for (key, channels) in theirs {
                let entries = mine
                    .entry(key)
                    .or_insert_with(|| vec![SolveEdgeAccumulation::default(); channels.len()]);
                for (entry, accumulation) in entries.iter_mut().zip(channels) {
                    entry.merge(accumulation);
                }
            }
        }
    }

    /// Entries the accumulation's sets and maps hold: its state grows with
    /// the group's distinct times, antennas, baselines and baseline
    /// correlations, not with its samples.
    #[cfg(test)]
    pub(crate) fn state_entries(&self) -> usize {
        self.unique_time_bits.len()
            + self.scan_numbers.len()
            + self.antenna_ids.len()
            + self.baseline_centroids.len()
            + self
                .receptor_accumulations
                .iter()
                .flat_map(HashMap::values)
                .map(Vec::len)
                .sum::<usize>()
    }

    /// The mean of the interval's distinct TIMEs (`SDBList::aggregateTime`,
    /// CASA's TIME when no sample carries weight).
    pub(crate) fn aggregate_time(&self) -> f64 {
        if self.unique_time_count > 0 {
            self.unique_time_sum / self.unique_time_count as f64
        } else {
            (self.min_time + self.max_time) / 2.0
        }
    }
}

/// Read one MAIN row's data, flags, model and solve weights, from the row or
/// from its pre-applied calibration.
///
/// The weights are CASA's (`CalSolvingVi2::calibrateCurrentVB`, which calls
/// `VisBufferImpl2::resetWeightsUsingSigma`): 1/σ² from SIGMA_SPECTRUM when
/// the MeasurementSet has WEIGHT_SPECTRUM and SIGMA_SPECTRUM, otherwise from
/// SIGMA broadcast over channels; σ ≤ 0 gives no weight. WEIGHT and
/// WEIGHT_SPECTRUM are not read. Pre-applied `calwt` tables then scale them.
fn read_row_inputs<'a>(
    ms: &'a MeasurementSet,
    row: &SelectedSolveRow,
    preapplied_row: Option<&'a EvaluatedApplyRow>,
    read_model_column: bool,
    inputs: (&'a SolveInputBatch, usize),
) -> Result<SolveRowInputs<'a>, GainSolveError> {
    let (data, flags) = match preapplied_row {
        Some(preapplied) => (&preapplied.corrected_data, &preapplied.flags),
        None => (
            input_cell(ms, row.row_index, inputs, "DATA")?,
            input_cell(ms, row.row_index, inputs, "FLAG")?,
        ),
    };
    let ArrayValue::Complex32(data) = data else {
        return Err(unsupported_shape("DATA", data));
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err(unsupported_shape("FLAG", flags));
    };
    if data.ndim() != 2 || data.shape() != flags.shape() {
        return Err(GainSolveError::UnsupportedParameterShape {
            path: "<measurement-set row>".to_string(),
            shape: data.shape().to_vec(),
        });
    }
    let model = if read_model_column {
        let model = input_cell(ms, row.row_index, inputs, "MODEL_DATA")?;
        match model {
            ArrayValue::Complex32(model) if model.shape() == data.shape() => Some(model),
            other => return Err(unsupported_shape("MODEL_DATA", other)),
        }
    } else {
        None
    };
    let mut weights = solve_weights(ms, row.row_index, data.shape(), inputs)?;
    if let Some(factors) = preapplied_row.and_then(|row| row.weight_factors.as_ref()) {
        if factors.shape() != data.shape() {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<pre-applied weight factors>".to_string(),
                shape: factors.shape().to_vec(),
            });
        }
        weights = &weights
            .broadcast(data.raw_dim())
            .expect("checked weight shape")
            * factors;
    }
    Ok(SolveRowInputs {
        data,
        flags: solve_flags(row, flags),
        weights,
        model,
    })
}

fn main_cell<'a>(
    ms: &'a MeasurementSet,
    row_index: usize,
    column: &str,
) -> Result<&'a ArrayValue, GainSolveError> {
    ms.main_table()
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.array())
        .map_err(|source| ms_read_error(ms, source))
}

fn unsupported_shape(column: &str, value: &ArrayValue) -> GainSolveError {
    GainSolveError::UnsupportedParameterShape {
        path: format!("<measurement-set {column}>"),
        shape: value.shape().to_vec(),
    }
}

/// The channels of a window of `channel_count` channels that a solve
/// uses: those `selection` names, else all. The gain table's
/// `SPECTRAL_WINDOW` row collapses the same channels.
pub(crate) fn selected_channels(
    selection: Option<&ChannelSelection>,
    channel_count: usize,
) -> Result<Vec<usize>, GainSolveError> {
    match selection {
        Some(selection) => {
            selection
                .indices(channel_count)
                .map_err(|source| GainSolveError::OpenMeasurementSet {
                    path: "<measurement-set channel selection>".to_string(),
                    source,
                })
        }
        None => Ok((0..channel_count).collect()),
    }
}

/// The sample flags a solve uses for `row`: its `FLAG` cell, or every sample
/// flagged when the row's `FLAG_ROW` is set, whatever `FLAG` holds. CASA's
/// solve does the same: `CalSolvingVi2::calibrateCurrentVB` calls
/// `VisEquation::collapse2`, whose `divideCorrByModel` flags the whole cube
/// of such a row and zeroes its weights before G, T or B accumulates it.
pub(crate) fn solve_flags<'a>(
    row: &SelectedSolveRow,
    flags: &'a ArrayD<bool>,
) -> Cow<'a, ArrayD<bool>> {
    if row.flag_row {
        Cow::Owned(ArrayD::from_elem(flags.raw_dim(), true))
    } else {
        Cow::Borrowed(flags)
    }
}

#[cfg(test)]
mod tests;
