// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection, bucketing, and accumulation for `gaincal` and `bandpass`.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_ms::ChannelSelection;
use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, ArrayView2, Ix2};

use super::{
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainType, RefAntSelector, correlation_receptors, get_f64, get_i32, stokes_name,
};
use crate::ApplyCalibrationTableSpec;
use crate::execute::{EvaluatedApplyRow, evaluate_apply_rows};
use crate::plan::{ApplyPlanRequest, plan_apply};
use crate::session::resolve_calibration_selection;
use crate::solve::kernel::SolveGraph;

pub(crate) fn validate_solve_interval(
    solve_interval: GainSolveInterval,
) -> Result<(), GainSolveError> {
    match solve_interval {
        GainSolveInterval::Infinite | GainSolveInterval::Integration => Ok(()),
        GainSolveInterval::Seconds(seconds) if seconds > 0.0 => Ok(()),
        _ => Err(GainSolveError::UnsupportedSolveInterval { solve_interval }),
    }
}

pub(crate) fn validate_smodel(smodel: [f32; 4]) -> Result<(), GainSolveError> {
    if smodel[0] <= 0.0 || smodel[1] != 0.0 || smodel[2] != 0.0 || smodel[3] != 0.0 {
        return Err(GainSolveError::UnsupportedSkyModel { smodel });
    }
    Ok(())
}

pub(crate) fn load_preapplied_rows(
    ms: &MeasurementSet,
    selection: &MsSelection,
    prior_calibration_tables: &[ApplyCalibrationTableSpec],
    parang: bool,
) -> Result<Option<HashMap<usize, EvaluatedApplyRow>>, GainSolveError> {
    if prior_calibration_tables.is_empty() && !parang {
        return Ok(None);
    }
    let plan = plan_apply(
        ms,
        &ApplyPlanRequest {
            selection: selection.clone(),
            apply_mode: crate::ApplyMode::CalFlag,
            parang,
            calibration_tables: prior_calibration_tables.to_vec(),
        },
    )
    .map_err(|source| GainSolveError::PriorCalibrationPlan {
        source: Box::new(source),
    })?;
    evaluate_apply_rows(ms, &plan).map(Some).map_err(|source| {
        GainSolveError::PriorCalibrationApply {
            source: Box::new(source),
        }
    })
}

pub(crate) fn build_solve_groups(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    preapplied_rows: Option<&HashMap<usize, EvaluatedApplyRow>>,
    options: SolveGroupOptions,
) -> Result<BTreeMap<(SolveBaseKey, SolveBucketKey), SolveAccumulator>, GainSolveError> {
    let mut groups = BTreeMap::<(SolveBaseKey, SolveBucketKey), SolveAccumulator>::new();
    for (row, base_key, bucket_key) in bucketed_rows(rows, options.solve_interval, options.combine)
    {
        // A group takes its field from its earliest row, as CASA's
        // `SDBList::aggregateFld` takes the first buffer's.
        let group = groups.entry((base_key, bucket_key)).or_insert_with(|| {
            SolveAccumulator::new(row.field_id, row.data_spw_id, row.observation_id)
        });
        let preapplied = preapplied_rows.and_then(|rows| rows.get(&row.row_index));
        group.observe(ms, row, preapplied, &options)?;
    }

    Ok(groups)
}

/// Each cross-correlation row of `rows`, in solve order, with the group and
/// interval it is solved in.
///
/// CASA's iteration sorts by the axes `combine` keeps apart and then by
/// time, so the rows of a group come in time order whatever their fields
/// and scans, and a `solint` interval starts at its group's earliest time.
fn bucketed_rows(
    rows: &[SelectedSolveRow],
    solve_interval: GainSolveInterval,
    combine: GainSolveCombine,
) -> Vec<(&SelectedSolveRow, SolveBaseKey, SolveBucketKey)> {
    let mut keyed = rows
        .iter()
        .filter(|row| row.antenna1 != row.antenna2)
        .map(|row| {
            let base_key = SolveBaseKey {
                field_id: if combine.fields { 0 } else { row.field_id },
                spw_id: row.data_spw_id,
                observation_id: row.observation_id,
                scan_number: if combine.scans { 0 } else { row.scan_number },
            };
            (row, base_key)
        })
        .collect::<Vec<_>>();
    keyed.sort_by(|(a, a_key), (b, b_key)| {
        a_key
            .cmp(b_key)
            .then(a.time_seconds.total_cmp(&b.time_seconds))
            .then(a.interval_seconds.total_cmp(&b.interval_seconds))
            .then(a.row_index.cmp(&b.row_index))
    });

    let mut bucket_starts = BTreeMap::<SolveBaseKey, f64>::new();
    keyed
        .into_iter()
        .map(|(row, base_key)| {
            let bucket_key = match solve_interval {
                GainSolveInterval::Infinite => SolveBucketKey::Infinite,
                GainSolveInterval::Integration => SolveBucketKey::Integration {
                    time_bits: row.time_seconds.to_bits(),
                    interval_bits: row.interval_seconds.to_bits(),
                },
                GainSolveInterval::Seconds(seconds) => {
                    let start = *bucket_starts.entry(base_key).or_insert(row.time_seconds);
                    let index = ((row.time_seconds - start) / seconds).floor().max(0.0);
                    SolveBucketKey::Seconds(index as u64)
                }
            };
            (row, base_key, bucket_key)
        })
        .collect()
}

/// Refuses `rows` when two of their data descriptions share a spectral
/// window, whatever their polarizations: CASA solves each description
/// apart, where casa-rs's groups and tables are keyed by window.
pub(crate) fn refuse_shared_spectral_windows(
    rows: &[SelectedSolveRow],
) -> Result<(), GainSolveError> {
    let mut descriptions = BTreeMap::<i32, BTreeSet<i32>>::new();
    for row in rows {
        descriptions
            .entry(row.data_spw_id)
            .or_default()
            .insert(row.data_desc_id);
    }
    match descriptions.into_iter().find(|(_, ids)| ids.len() > 1) {
        Some((spw_id, ids)) => Err(GainSolveError::SharedSpectralWindow {
            spw_id,
            data_desc_ids: ids.into_iter().collect(),
        }),
        None => Ok(()),
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

pub(crate) fn resolve_refant(
    ms: &MeasurementSet,
    selector: &RefAntSelector,
) -> Result<i32, GainSolveError> {
    match selector {
        RefAntSelector::AntennaId(id) => Ok(*id),
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
                    return Ok(row as i32);
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
) -> Result<Vec<SelectedSolveRow>, GainSolveError> {
    let selected_rows = resolve_calibration_selection(ms, selection).map_err(|error| {
        GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source: match error {
                casa_ms::MsSelectionError::Domain(source) => source,
                other => MsError::InvalidInput(other.to_string()),
            },
        }
    })?;
    if selected_rows.selected_rows.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }

    selected_rows
        .selected_rows
        .into_iter()
        .map(|row| {
            Ok(SelectedSolveRow {
                row_index: row.row_index,
                field_id: row.field_id,
                observation_id: get_i32(ms.main_table(), row.row_index, "OBSERVATION_ID")?,
                data_desc_id: row.data_desc_id,
                data_spw_id: row.spectral_window_id,
                channel_selection: selection.channel_selection_for_spw(row.spectral_window_id),
                antenna1: row.antenna1_id,
                antenna2: row.antenna2_id,
                time_seconds: row.time_mjd_seconds,
                time_centroid_seconds: get_f64(ms.main_table(), row.row_index, "TIME_CENTROID")?,
                interval_seconds: get_f64(ms.main_table(), row.row_index, "INTERVAL")?,
                scan_number: get_i32(ms.main_table(), row.row_index, "SCAN_NUMBER")?,
                flag_row: row.flag_row,
            })
        })
        .collect()
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
    pub(crate) gain_type: GainType,
    pub(crate) model_source: GainSolveModelSource,
    pub(crate) stokes_i: f32,
    pub(crate) solve_interval: GainSolveInterval,
    pub(crate) combine: GainSolveCombine,
    /// Solve each data channel apart (`bandpass`) rather than the selected
    /// channels together (`gaincal`).
    pub(crate) channelized: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SelectedSolveRow {
    pub(crate) row_index: usize,
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

/// One sample's arrays for [`SolveAccumulator::observe_samples`], each
/// `[correlation, channel]` as casacore stores a MAIN cell: the data, the
/// flags the solve uses (row flags already folded in), the solve weights and
/// the model, if any.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveSamples<'a> {
    pub(crate) data: ArrayView2<'a, Complex32>,
    pub(crate) flags: ArrayView2<'a, bool>,
    pub(crate) weights: ArrayView2<'a, f32>,
    pub(crate) model: Option<ArrayView2<'a, Complex32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SolveBaseKey {
    pub(crate) field_id: i32,
    pub(crate) spw_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) scan_number: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SolveBucketKey {
    Infinite,
    Integration { time_bits: u64, interval_bits: u64 },
    Seconds(u64),
}

/// The samples of one solution interval, collapsed per baseline,
/// correlation and solution channel, and the interval's metadata.
#[derive(Debug, Clone)]
pub(crate) struct SolveAccumulator {
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
    /// `[solution channel][receptor]`, filled by [`Self::finalize_for_solve`].
    pub(crate) channel_graphs: Vec<Vec<SolveGraph>>,
    /// The solution's TIME, set by [`Self::finalize_for_solve`].
    pub(crate) solution_time: f64,
    /// Whether a sample entered the solve: CASA attempts a solution for
    /// such an interval (`SDBList::Ok`, positive unflagged weight after the
    /// model division), and sets its window's gain-table channel then,
    /// whether or not the solve succeeds.
    pub(crate) attempted: bool,
    baseline_centroids: HashMap<(i32, i32), WeightedTime>,
    receptor_accumulations: Vec<HashMap<SolveAccumulationKey, SolveEdgeAccumulation>>,
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

#[derive(Debug, Clone, Copy, Default)]
struct SolveEdgeAccumulation {
    weighted_sum: Complex32,
    weight: f32,
    weighted_sample_power: f64,
    sample_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SolveAccumulationKey {
    baseline: (i32, i32),
    correlation_index: usize,
    channel_index: usize,
}

/// A weighted mean of times.
#[derive(Debug, Clone, Copy, Default)]
struct WeightedTime {
    weighted_sum: f64,
    weight: f64,
}

/// One MAIN row's arrays for the solve, `[correlation, channel]`, checked
/// two-dimensional and of one shape.
struct SolveRowInputs<'a> {
    data: &'a ArrayD<Complex32>,
    flags: Cow<'a, ArrayD<bool>>,
    weights: ArrayD<f32>,
    model: Option<&'a ArrayD<Complex32>>,
}

impl SolveRowInputs<'_> {
    /// The arrays as [`SolveSamples`] views.
    fn samples(&self) -> SolveSamples<'_> {
        SolveSamples {
            data: two_dimensional(self.data),
            flags: two_dimensional(&self.flags),
            weights: two_dimensional(&self.weights),
            model: self.model.map(two_dimensional),
        }
    }
}

/// A MAIN cell checked two-dimensional, as a view.
fn two_dimensional<T>(cell: &ArrayD<T>) -> ArrayView2<'_, T> {
    cell.view()
        .into_dimensionality::<Ix2>()
        .expect("checked two-dimensional")
}

impl SolveAccumulator {
    pub(crate) fn new(field_id: i32, spw_id: i32, observation_id: i32) -> Self {
        Self {
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
            channel_graphs: Vec::new(),
            solution_time: 0.0,
            attempted: false,
            baseline_centroids: HashMap::new(),
            receptor_accumulations: Vec::new(),
        }
    }

    /// Add one MAIN row: its unflagged, positively weighted samples divided by
    /// the model, each weighted by its solve weight times |model|² as
    /// `VisEquation::divideCorrByModel` does, and its TIME_CENTROID.
    pub(crate) fn observe(
        &mut self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        preapplied_row: Option<&EvaluatedApplyRow>,
        options: &SolveGroupOptions,
    ) -> Result<(), GainSolveError> {
        let inputs = read_row_inputs(ms, row, preapplied_row, options.model_source)?;
        let correlation_types = correlation_types_for_ddid(ms, row.data_desc_id)?;
        let channel_count = inputs.data.shape()[1];
        let channels = if options.channelized {
            (0..channel_count).collect()
        } else {
            selected_channels(row.channel_selection.as_ref(), channel_count)?
        };
        self.observe_samples(
            &SolveSampleRow::of(row),
            &inputs.samples(),
            &correlation_types,
            &channels,
            options,
        )
    }

    /// Accumulate one sample's `channels` (every channel for a channelized
    /// solve): the arithmetic of [`Self::observe`] on arrays from any source.
    /// The MSv2/MSv4 trial (`docs/measurement-set/trial/README.md`) feeds it
    /// from `casa_observation::VisibilityBatch` views without copying them.
    pub(crate) fn observe_samples(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlation_types: &[i32],
        channels: &[usize],
        options: &SolveGroupOptions,
    ) -> Result<(), GainSolveError> {
        if options.channelized {
            let channel_count = samples.data.shape()[1];
            if self.sample_rows > 0 && self.solution_channels != channel_count {
                return Err(GainSolveError::UnsupportedParameterShape {
                    path: "<measurement-set DATA>".to_string(),
                    shape: samples.data.shape().to_vec(),
                });
            }
            self.solution_channels = channel_count;
        }

        let graph_count = match options.gain_type {
            GainType::G => 2,
            GainType::T => 1,
        };
        if self.receptor_accumulations.len() < graph_count {
            self.receptor_accumulations
                .resize_with(graph_count, HashMap::new);
        }
        self.observe_metadata(row);

        let mut row_weight = 0.0_f64;
        for (corr_index, &correlation_type) in correlation_types.iter().enumerate() {
            let Some(receptors) = correlation_receptors(correlation_type) else {
                return Err(GainSolveError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: correlation_types
                        .iter()
                        .map(|code| stokes_name(*code).to_string())
                        .collect(),
                });
            };
            if receptors.0 != receptors.1 {
                continue;
            }
            let graph_index = match options.gain_type {
                GainType::G => receptors.0,
                GainType::T => 0,
            };
            let mut channel_weights = Vec::with_capacity(channels.len());
            for &chan_index in channels {
                let weight = self.accumulate_sample(
                    graph_index,
                    row,
                    samples,
                    options,
                    corr_index,
                    chan_index,
                );
                channel_weights.push(weight);
            }
            row_weight += if options.channelized {
                f64::from(median_with_even_mean(&mut channel_weights))
            } else {
                channel_weights
                    .iter()
                    .map(|weight| f64::from(*weight))
                    .sum()
            };
        }
        if row_weight > 0.0 {
            let centroid = self
                .baseline_centroids
                .entry((row.antenna1, row.antenna2))
                .or_default();
            centroid.weighted_sum += row_weight * row.time_centroid_seconds;
            centroid.weight += row_weight;
        }
        Ok(())
    }

    fn observe_metadata(&mut self, row: &SolveSampleRow) {
        self.min_time = self.min_time.min(row.time_seconds);
        self.max_time = self.max_time.max(row.time_seconds);
        if self.unique_time_bits.insert(row.time_seconds.to_bits()) {
            self.unique_time_sum += row.time_seconds;
            self.unique_time_count += 1;
        }
        self.total_interval += row.interval_seconds;
        self.sample_rows += 1;
        self.field_ids.insert(row.field_id);
        self.scan_numbers.insert(row.scan_number);
        self.antenna_ids.insert(row.antenna1);
        self.antenna_ids.insert(row.antenna2);
    }

    /// Accumulate one sample if it can enter the solve; return its effective
    /// weight, zero when it is flagged or unusable.
    fn accumulate_sample(
        &mut self,
        graph_index: usize,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        options: &SolveGroupOptions,
        corr_index: usize,
        chan_index: usize,
    ) -> f32 {
        let weight = samples.weights[[corr_index, chan_index]];
        if weight <= 0.0 || solve_sample_flagged(&samples.flags, options.gain_type, chan_index) {
            return 0.0;
        }
        let model = match samples.model {
            Some(model_data) => model_data[[corr_index, chan_index]],
            None => Complex32::new(options.stokes_i, 0.0),
        };
        let model_norm_sqr = model.norm_sqr();
        if model_norm_sqr <= f32::EPSILON {
            return 0.0;
        }
        self.attempted = true;
        let sample = samples.data[[corr_index, chan_index]] / model;
        if sample.norm() <= f32::EPSILON {
            return 0.0;
        }
        let effective_weight = weight * model_norm_sqr;
        let accumulation = self.receptor_accumulations[graph_index]
            .entry(SolveAccumulationKey {
                baseline: (row.antenna1, row.antenna2),
                correlation_index: corr_index,
                channel_index: if options.channelized { chan_index } else { 0 },
            })
            .or_default();
        accumulation.weighted_sum += sample * Complex32::new(effective_weight, 0.0);
        accumulation.weight += effective_weight;
        accumulation.weighted_sample_power +=
            f64::from(effective_weight) * f64::from(sample.norm_sqr());
        accumulation.sample_count += 1;
        effective_weight
    }

    /// Collapse each baseline's samples per correlation and solution channel
    /// into the solve graphs, and set the solution TIME.
    ///
    /// A phase-only solve sees each collapsed sample at unit amplitude with
    /// its weight times its squared amplitude
    /// (`SolveDataBuffer::enforceAPonData`). TIME is
    /// `SDBList::aggregateTimeCentroid`: each baseline's TIME_CENTROID,
    /// averaged over its rows by row weight as time averaging does, weighted
    /// by the baseline's solve weight after that phase-only scaling.
    pub(crate) fn finalize_for_solve(&mut self, solve_mode: GainSolveMode) {
        self.channel_graphs = vec![
            vec![SolveGraph::default(); self.receptor_accumulations.len()];
            self.solution_channels
        ];
        let mut baseline_weights = HashMap::<(i32, i32), f64>::new();
        for (receptor, accumulations) in self.receptor_accumulations.iter().enumerate() {
            for (key, accumulation) in accumulations {
                if accumulation.weight <= f32::EPSILON {
                    continue;
                }
                let averaged = accumulation.weighted_sum / Complex32::new(accumulation.weight, 0.0);
                let (sample, weight) = match solve_mode {
                    GainSolveMode::Phase => {
                        let amplitude = averaged.norm();
                        if amplitude <= f32::EPSILON {
                            continue;
                        }
                        (
                            averaged / Complex32::new(amplitude, 0.0),
                            accumulation.weight * amplitude * amplitude,
                        )
                    }
                    GainSolveMode::AmplitudePhase => (averaged, accumulation.weight),
                };
                self.channel_graphs[key.channel_index][receptor].accumulate(
                    key.baseline,
                    weight,
                    sample * Complex32::new(weight, 0.0),
                    SolveEdgeStats {
                        weighted_sample_power: f64::from(weight) * f64::from(sample.norm_sqr()),
                        raw_weighted_sample_power: accumulation.weighted_sample_power,
                        sample_count: accumulation.sample_count,
                        collapsed_count: 1,
                    },
                );
                *baseline_weights.entry(key.baseline).or_default() += f64::from(weight);
            }
        }

        let mut solution_time = WeightedTime::default();
        for (baseline, weight) in baseline_weights {
            let Some(centroid) = self.baseline_centroids.get(&baseline) else {
                continue;
            };
            if centroid.weight > 0.0 {
                solution_time.weighted_sum += weight * centroid.weighted_sum / centroid.weight;
                solution_time.weight += weight;
            }
        }
        self.solution_time = if solution_time.weight > 0.0 {
            solution_time.weighted_sum / solution_time.weight
        } else {
            self.aggregate_time()
        };
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
        self.attempted |= other.attempted;
        for (baseline, centroid) in other.baseline_centroids {
            let mine = self.baseline_centroids.entry(baseline).or_default();
            mine.weighted_sum += centroid.weighted_sum;
            mine.weight += centroid.weight;
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
            for (key, accumulation) in theirs {
                let entry = mine.entry(key).or_default();
                entry.weighted_sum += accumulation.weighted_sum;
                entry.weight += accumulation.weight;
                entry.weighted_sample_power += accumulation.weighted_sample_power;
                entry.sample_count += accumulation.sample_count;
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
                .map(HashMap::len)
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
    model_source: GainSolveModelSource,
) -> Result<SolveRowInputs<'a>, GainSolveError> {
    let (data, flags) = match preapplied_row {
        Some(preapplied) => (&preapplied.corrected_data, &preapplied.flags),
        None => (
            main_cell(ms, row.row_index, "DATA")?,
            main_cell(ms, row.row_index, "FLAG")?,
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
    let model = match model_source {
        GainSolveModelSource::ModelColumn => {
            let model = main_cell(ms, row.row_index, "MODEL_DATA")?;
            match model {
                ArrayValue::Complex32(model) if model.shape() == data.shape() => Some(model),
                other => return Err(unsupported_shape("MODEL_DATA", other)),
            }
        }
        GainSolveModelSource::PointSource => None,
    };
    let mut weights = solve_weights(ms, row.row_index, data.shape())?;
    if let Some(factors) = preapplied_row.and_then(|row| row.weight_factors.as_ref()) {
        if factors.shape() != weights.shape() {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<pre-applied weight factors>".to_string(),
                shape: factors.shape().to_vec(),
            });
        }
        weights *= factors;
    }
    Ok(SolveRowInputs {
        data,
        flags: solve_flags(row, flags),
        weights,
        model,
    })
}

fn solve_weights(
    ms: &MeasurementSet,
    row_index: usize,
    shape: &[usize],
) -> Result<ArrayD<f32>, GainSolveError> {
    let has_column = |name| {
        ms.main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
    };
    let inverse_square = |sigma: f32| {
        if sigma > 0.0 {
            1.0 / (sigma * sigma)
        } else {
            0.0
        }
    };
    if has_column("WEIGHT_SPECTRUM") && has_column("SIGMA_SPECTRUM") {
        let sigma = main_cell(ms, row_index, "SIGMA_SPECTRUM")?;
        return match sigma {
            ArrayValue::Float32(sigma) if sigma.shape() == shape => Ok(sigma.mapv(inverse_square)),
            other => Err(unsupported_shape("SIGMA_SPECTRUM", other)),
        };
    }
    let sigma = main_cell(ms, row_index, "SIGMA")?;
    let ArrayValue::Float32(sigma) = sigma else {
        return Err(unsupported_shape("SIGMA", sigma));
    };
    if sigma.ndim() != 1 || sigma.shape()[0] != shape[0] {
        return Err(GainSolveError::UnsupportedParameterShape {
            path: "<measurement-set SIGMA>".to_string(),
            shape: sigma.shape().to_vec(),
        });
    }
    Ok(ArrayD::from_shape_fn(shape, |index| {
        inverse_square(sigma[[index[0]]])
    }))
}

fn main_cell<'a>(
    ms: &'a MeasurementSet,
    row_index: usize,
    column: &str,
) -> Result<&'a ArrayValue, GainSolveError> {
    ms.main_table()
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.array())
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source: MsError::from(source),
        })
}

fn unsupported_shape(column: &str, value: &ArrayValue) -> GainSolveError {
    GainSolveError::UnsupportedParameterShape {
        path: format!("<measurement-set {column}>"),
        shape: value.shape().to_vec(),
    }
}

/// The median, the mean of the middle two for an even count, as casacore's
/// `partialMedians(..., takeEvenMean=True)` that CASA uses for a row's
/// channel-independent weight.
fn median_with_even_mean(values: &mut [f32]) -> f32 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f32::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
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

/// Correlation-independent flags, as `CalSolvingVi2::corrIndepFlags` makes
/// them: a channel any correlation flags is flagged for all.
fn solve_sample_flagged(
    flags: &ArrayView2<'_, bool>,
    gain_type: GainType,
    chan_index: usize,
) -> bool {
    match gain_type {
        GainType::G | GainType::T => {
            (0..flags.shape()[0]).any(|corr_index| flags[[corr_index, chan_index]])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accumulator(receptors: usize) -> SolveAccumulator {
        let mut group = SolveAccumulator::new(0, 0, 0);
        group
            .receptor_accumulations
            .resize_with(receptors, HashMap::new);
        group
    }

    fn add(group: &mut SolveAccumulator, correlation_index: usize, sample: Complex32) {
        let accumulation = group.receptor_accumulations[0]
            .entry(SolveAccumulationKey {
                baseline: (0, 1),
                correlation_index,
                channel_index: 0,
            })
            .or_default();
        accumulation.weighted_sum += sample;
        accumulation.weight += 1.0;
        accumulation.weighted_sample_power += f64::from(sample.norm_sqr());
        accumulation.sample_count += 1;
    }

    #[test]
    fn finalize_for_phase_solve_collapses_before_unit_amplitude_normalization() {
        let mut group = accumulator(1);
        add(&mut group, 0, Complex32::new(2.0, 0.0));
        add(&mut group, 0, Complex32::new(0.0, 2.0));

        group.finalize_for_solve(GainSolveMode::Phase);

        let graph = &group.channel_graphs[0][0];
        assert!((graph.weights[&(0, 1)] - 4.0).abs() < 1.0e-6);
        let edge = graph.edges[&(0, 1)];
        let expected = 4.0_f32 / 2.0_f32.sqrt();
        assert!((edge.re - expected).abs() < 1.0e-6);
        assert!((edge.im - expected).abs() < 1.0e-6);
        assert_eq!(graph.stats[&(0, 1)].sample_count, 2);
    }

    #[test]
    fn finalize_for_phase_solve_keeps_correlations_separate_for_scalar_t() {
        let mut group = accumulator(1);
        add(&mut group, 0, Complex32::new(2.0, 0.0));
        add(&mut group, 3, Complex32::new(0.0, 2.0));

        group.finalize_for_solve(GainSolveMode::Phase);

        let graph = &group.channel_graphs[0][0];
        assert!((graph.weights[&(0, 1)] - 8.0).abs() < 1.0e-6);
        let edge = graph.edges[&(0, 1)];
        assert!((edge.re - 4.0).abs() < 1.0e-6);
        assert!((edge.im - 4.0).abs() < 1.0e-6);
        assert_eq!(graph.stats[&(0, 1)].sample_count, 2);
    }

    #[test]
    fn solution_time_weights_baseline_centroids_by_solve_weight() {
        // Baseline 0-1 (rows at 10 s and 12 s) carries three times the
        // weight of 0-2 (one row at 20 s).
        let mut group = accumulator(1);
        for (baseline, weight) in [((0, 1), 3.0_f32), ((0, 2), 1.0)] {
            group.receptor_accumulations[0].insert(
                SolveAccumulationKey {
                    baseline,
                    correlation_index: 0,
                    channel_index: 0,
                },
                SolveEdgeAccumulation {
                    weighted_sum: Complex32::new(weight, 0.0),
                    weight,
                    weighted_sample_power: f64::from(weight),
                    sample_count: 1,
                },
            );
        }
        group.baseline_centroids.insert(
            (0, 1),
            WeightedTime {
                weighted_sum: 10.0 + 12.0,
                weight: 2.0,
            },
        );
        group.baseline_centroids.insert(
            (0, 2),
            WeightedTime {
                weighted_sum: 20.0,
                weight: 1.0,
            },
        );

        group.finalize_for_solve(GainSolveMode::AmplitudePhase);

        assert!((group.solution_time - (3.0 * 11.0 + 20.0) / 4.0).abs() < 1.0e-12);
    }

    /// A cross-correlation row of field `field_id`, scan `scan_number` and
    /// data description `data_desc_id` (on window 0) at `time_seconds`.
    fn row(
        row_index: usize,
        field_id: i32,
        scan_number: i32,
        data_desc_id: i32,
        time_seconds: f64,
    ) -> SelectedSolveRow {
        SelectedSolveRow {
            row_index,
            field_id,
            observation_id: 0,
            data_desc_id,
            data_spw_id: 0,
            channel_selection: None,
            antenna1: 0,
            antenna2: 1,
            time_seconds,
            time_centroid_seconds: time_seconds,
            interval_seconds: 1.0,
            scan_number,
            flag_row: false,
        }
    }

    #[test]
    fn combined_intervals_start_at_the_earliest_row_whatever_the_field_order() {
        // Field 1 is observed first, in scan 1; field 0 thirty seconds
        // later, in scan 2. Sorting by field first would start the 20 s
        // intervals at field 0's scan and fold field 1's into its first.
        let rows = [130.0, 131.0, 132.0]
            .map(|time| (0, 2, time))
            .into_iter()
            .chain([100.0, 101.0, 102.0].map(|time| (1, 1, time)))
            .enumerate()
            .map(|(index, (field, scan, time))| row(index, field, scan, 0, time))
            .collect::<Vec<_>>();
        let buckets = |combine| {
            bucketed_rows(&rows, GainSolveInterval::Seconds(20.0), combine)
                .into_iter()
                .map(|(row, base, bucket)| (row.time_seconds, base.field_id, bucket))
                .collect::<Vec<_>>()
        };
        let combined = buckets(GainSolveCombine {
            scans: true,
            fields: true,
        });
        let times = combined.iter().map(|(time, ..)| *time).collect::<Vec<_>>();
        assert_eq!(times, [100.0, 101.0, 102.0, 130.0, 131.0, 132.0]);
        let intervals = combined
            .iter()
            .map(|(.., bucket)| *bucket)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            intervals,
            BTreeSet::from([SolveBucketKey::Seconds(0), SolveBucketKey::Seconds(1)])
        );
        // Kept apart, each field's intervals start at its own first row.
        for (time, field, bucket) in buckets(GainSolveCombine::default()) {
            assert_eq!(bucket, SolveBucketKey::Seconds(0), "{time} {field}");
        }
    }

    #[test]
    fn data_descriptions_sharing_a_window_are_refused() {
        // Two descriptions on window 0, as a duplicated description or one
        // holding the cross hands would give.
        let rows = [row(0, 0, 1, 0, 100.0), row(1, 0, 1, 1, 100.0)];
        let error = refuse_shared_spectral_windows(&rows).unwrap_err();
        assert!(
            matches!(
                &error,
                GainSolveError::SharedSpectralWindow { spw_id: 0, data_desc_ids }
                    if data_desc_ids == &[0, 1]
            ),
            "{error}"
        );
        assert!(error.to_string().contains("#979"), "{error}");
        assert!(refuse_shared_spectral_windows(&rows[..1]).is_ok());
    }

    #[test]
    fn median_with_even_mean_matches_casacore_partial_medians() {
        assert_eq!(median_with_even_mean(&mut []), 0.0);
        assert_eq!(median_with_even_mean(&mut [3.0, 0.0, 1.0]), 1.0);
        assert_eq!(median_with_even_mean(&mut [4.0, 0.0, 4.0, 4.0]), 4.0);
        assert_eq!(median_with_even_mean(&mut [2.0, 0.0]), 1.0);
    }
}
