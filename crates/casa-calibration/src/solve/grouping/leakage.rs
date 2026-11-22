// SPDX-License-Identifier: LGPL-3.0-or-later
//! Group polarized samples for the native polcal task.

mod channel_samples;
mod cross_hand;
mod fit;
mod general;
mod interval;
mod joint;
mod output;
mod parallactic;
mod parallactic_fit;
mod standard;

use super::*;
use crate::SelectedData;
use crate::execute::{ParallacticAngleState, coherency_order, load_parallactic_angle_state};
use crate::session::prepare_calibration_solve;
use crate::solve_stats::{
    AntennaSolution, IntervalStages, SolveStats, SolveStatsBuilder, ToolSolveActivity,
};
use std::path::PathBuf;

use interval::{IntervalFits, LeakageFitStages};
use joint::SourceFitMode;
use parallactic::BaselineSegment;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeakageSolveKind {
    D,
    Df,
    DQu,
    DfQu,
    DX,
    DfX,
    DGen,
    DfGen,
    DGenQu,
    DfGenQu,
    DGenX,
    DfGenX,
    Dlls,
    Dflls,
    Xj,
    Xf,
    PosAng,
    Xparang,
    Xfparang,
    XY,
    XYf,
}

impl LeakageSolveKind {
    pub(crate) fn subtype(self) -> &'static str {
        match self {
            Self::D | Self::DQu | Self::DX => "D Jones",
            Self::Df | Self::DfQu | Self::DfX => "Df Jones",
            Self::DGen | Self::DGenQu | Self::DGenX => "Dgen Jones",
            Self::DfGen | Self::DfGenQu | Self::DfGenX => "Dfgen Jones",
            Self::Dlls => "DLLS Jones",
            Self::Dflls => "DfLLS Jones",
            Self::Xj => "X Jones",
            Self::Xf => "Xf Jones",
            Self::PosAng => "PosAng Jones",
            Self::Xparang => "Xparang Jones",
            Self::Xfparang => "Xfparang Jones",
            Self::XY => "GlinXph Jones",
            Self::XYf => "GlinXphf Jones",
        }
    }

    pub(crate) fn channelized(self) -> bool {
        matches!(
            self,
            Self::Df
                | Self::DfQu
                | Self::DfX
                | Self::DfGen
                | Self::DfGenQu
                | Self::DfGenX
                | Self::Dflls
                | Self::Xf
                | Self::PosAng
                | Self::Xfparang
                | Self::XYf
        )
    }

    fn iterative(self) -> bool {
        !matches!(self, Self::Dlls | Self::Dflls) && !self.cross_hand()
    }

    pub(crate) fn cross_hand(self) -> bool {
        matches!(self, Self::Xj | Self::Xf | Self::PosAng) || self.parallactic()
    }

    fn parallactic(self) -> bool {
        matches!(self, Self::Xparang | Self::Xfparang) || self.gain_phase()
    }

    fn intensity_normalized(self) -> bool {
        matches!(self, Self::PosAng | Self::Xparang | Self::Xfparang)
    }

    pub(crate) fn gain_phase(self) -> bool {
        matches!(self, Self::XY | Self::XYf)
    }

    fn source(self) -> Option<SourceFitMode> {
        match self {
            Self::DQu | Self::DfQu | Self::DGenQu | Self::DfGenQu => {
                Some(SourceFitMode::Polarization)
            }
            Self::DX | Self::DfX | Self::DGenX | Self::DfGenX => Some(SourceFitMode::Angle),
            _ => None,
        }
    }

    fn algebra(self) -> standard::LeakageAlgebra {
        if matches!(
            self,
            Self::DGen | Self::DGenQu | Self::DGenX | Self::DfGen | Self::DfGenQu | Self::DfGenX
        ) {
            standard::LeakageAlgebra::General
        } else {
            standard::LeakageAlgebra::Linearized
        }
    }
}

#[derive(Clone)]
pub(crate) struct LeakageSolveRequest {
    pub(crate) output: PathBuf,
    pub(crate) selection: MsSelection,
    pub(crate) kind: LeakageSolveKind,
    pub(crate) model_source: GainSolveModelSource,
    pub(crate) stokes: [f32; 4],
    pub(crate) priors: Vec<ApplyCalibrationTableSpec>,
    pub(crate) parang_terms: usize,
    pub(crate) correlation_dependent_flags: bool,
    pub(crate) solve_mode: GainSolveMode,
    pub(crate) normalization: Option<crate::GainNormalization>,
    pub(crate) interval: GainSolveInterval,
    pub(crate) frequency_interval: Option<crate::BandpassFrequencyInterval>,
    pub(crate) combine: GainSolveCombine,
    pub(crate) preaverage_seconds: f64,
    pub(crate) min_baselines: usize,
    pub(crate) min_snr: f32,
    pub(crate) refant: RefAntSelector,
    pub(crate) append: bool,
}

pub(crate) struct LeakageSolution {
    pub(crate) spectral_window: i32,
    pub(crate) field: i32,
    pub(crate) observation: i32,
    pub(crate) scan: i32,
    pub(crate) time: f64,
    pub(crate) reference: i32,
    pub(crate) polarization: Option<Complex32>,
    /// Channel-major solutions with one pair of leakage terms per antenna.
    pub(crate) values: Vec<Vec<LeakageEstimate>>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct LeakageEstimate {
    gain: [Complex32; 2],
    valid: [bool; 2],
    error: [f32; 2],
    snr: [f32; 2],
}

pub(crate) struct PolarizationSolveOutcome {
    pub(crate) rows: usize,
    pub(crate) source: BTreeMap<String, BTreeMap<i32, Complex32>>,
    pub(crate) selected: SelectedData,
    pub(crate) gain_stats: Option<SolveStats>,
    pub(crate) solved_fields: BTreeSet<i32>,
    pub(crate) solved_windows: BTreeSet<i32>,
}

pub(crate) fn solve_polarization(
    ms: &MeasurementSet,
    request: &LeakageSolveRequest,
) -> Result<PolarizationSolveOutcome, String> {
    solve_polarization_with_activity(ms, request, None)
}

pub(crate) fn solve_polarization_with_activity(
    ms: &MeasurementSet,
    request: &LeakageSolveRequest,
    activity: Option<&mut ToolSolveActivity>,
) -> Result<PolarizationSolveOutcome, String> {
    let preparation = crate::timing::Phase::start("polarization_prepare");
    let error = |error: GainSolveError| error.to_string();
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &request.refant,
        &request.priors,
        request.parang_terms > 0,
        true,
        match request.kind {
            LeakageSolveKind::PosAng => PriorCalibrationMode::PositionAngle,
            kind if kind.cross_hand() => PriorCalibrationMode::CrossHand {
                share_input_flags: !request.correlation_dependent_flags,
            },
            _ => PriorCalibrationMode::Leakage {
                share_input_flags: !request.correlation_dependent_flags,
                parang_terms: request.parang_terms,
            },
        },
    )
    .map_err(error)?;
    let ordered = bucketed_rows(
        &context.rows,
        request.interval,
        request.combine,
        false,
        false,
        false,
    );
    let mut models = super::super::model::ModelInputs::new(ms, &context.rows, request.model_source)
        .map_err(error)?;
    models
        .bind_buffers(ms, &ordered, request.combine)
        .map_err(error)?;
    models.phase_times =
        weights::chunk_phase_times(&ordered, request.combine, models.dynamic_fields());
    let spectral_weights = SpectralWeights::new(ms, &ordered, request.combine).map_err(error)?;
    let parallactic_angles = request
        .kind
        .parallactic()
        .then(|| load_parallactic_angle_state(ms))
        .transpose()
        .map_err(|source| source.to_string())?;
    let channel_bin_widths = crate::bandpass::solution_frequency_bin_widths(
        ms,
        &context.rows,
        request
            .frequency_interval
            .filter(|_| request.kind.channelized()),
    )
    .map_err(|error| error.to_string())?;
    let mut windows = BTreeMap::new();
    for row in &context.rows {
        if let std::collections::btree_map::Entry::Vacant(entry) = windows.entry(row.data_spw_id) {
            let data = main_cell(ms, row.row_index, "DATA").map_err(error)?;
            if data.shape().len() != 2 {
                return Err(unsupported_shape("DATA", data).to_string());
            }
            let channels = selected_channels(row.channel_selection.as_ref(), data.shape()[1])
                .map_err(error)?;
            let bin_width = if request.kind.channelized() {
                channel_bin_widths
                    .get(&row.data_spw_id)
                    .copied()
                    .unwrap_or(1)
            } else {
                channels.len().max(1)
            };
            entry.insert(WindowChannels {
                channels,
                bin_width,
            });
        }
    }
    if request.combine.spectral_windows
        && let Some(first) = windows.values().next()
        && windows
            .values()
            .any(|window| window.count() != first.count())
    {
        return Err("Cannot combine spectral windows with different channel counts".into());
    }
    let setup = LeakageInputs {
        ms,
        request,
        models: &models,
        preapplied: context.preapplied_rows.as_ref(),
        spectral_weights,
        windows: &windows,
        parallactic_angles: parallactic_angles.as_ref(),
        tool: activity.is_some(),
    };
    let mut intervals = ordered
        .chunk_by(|a, b| a.1 == b.1 && a.2 == b.2)
        .collect::<Vec<_>>();
    intervals.sort_by(|a, b| {
        interval_window(a)
            .cmp(&interval_window(b))
            .then_with(|| a[0].0.time_seconds.total_cmp(&b[0].0.time_seconds))
    });
    let mut references = Vec::new();
    if context.refant_id >= 0 && !request.kind.cross_hand() {
        references.extend([context.refant_id, context.refant_id]);
        if !matches!(
            request.refant,
            RefAntSelector::Expression { strict: true, .. }
        ) {
            references.extend(context.refant_alternates.iter().copied());
            references.extend(
                super::super::refant_distance_order(
                    ms,
                    *references.last().expect("primary antenna"),
                )
                .map_err(error)?,
            );
        }
    }
    let mut previous: Option<usize> = None;
    let mut solutions: Vec<LeakageSolution> = Vec::new();
    let mut gain_stats = if request.kind.gain_phase() || setup.tool {
        let windows = crate::solve::spectral_window_ids(ms).map_err(error)?;
        Some(SolveStatsBuilder::new(
            2,
            windows,
            &context.available_antennas,
        ))
    } else {
        None
    };
    drop(preparation);
    let fitting = crate::timing::Phase::start("polarization_gather_and_fit");
    for rows in intervals {
        let previous_interval = previous
            .and_then(|index| solutions.get(index))
            .filter(|solution| solution.spectral_window == interval_window(rows));
        if let Some(solution) = setup
            .solve_interval(
                rows,
                context.available_antennas.len(),
                &references,
                previous_interval,
                gain_stats.as_mut(),
            )
            .map_err(error)?
        {
            if solution.reference >= 0 {
                references[1] = solution.reference;
                previous = Some(solutions.len());
            }
            solutions.push(solution);
        }
    }
    if request.kind.iterative() {
        interval::reference_iterative_solutions(&mut solutions, &mut references);
    }
    if setup.tool {
        interval::finish_tool_parameters(&mut solutions, request.solve_mode, request.normalization);
    }
    if let Some(stats) = &mut gain_stats
        && setup.tool
    {
        for solution in &solutions {
            if solution.reference >= 0 {
                stats.used_as_refant(solution.spectral_window, solution.reference);
            }
        }
        stats.track_activity(activity);
    }
    let bin_widths = windows
        .iter()
        .map(|(&id, window)| (id, window.bin_width))
        .collect();
    let mut source = BTreeMap::<String, BTreeMap<i32, Complex32>>::new();
    if request.kind == LeakageSolveKind::Xfparang {
        let fields = ms.field().map_err(|error| error.to_string())?;
        for interval in &solutions {
            if let Some(qu) = interval.polarization {
                let name = fields
                    .name(interval.field as usize)
                    .map_err(|error| error.to_string())?;
                source
                    .entry(name)
                    .or_default()
                    .insert(interval.spectral_window, qu);
            }
        }
    }
    drop(fitting);
    let _publication = crate::timing::Phase::start("polarization_write");
    let rows = output::write(ms, request, &context.rows, &solutions, bin_widths)?;
    Ok(PolarizationSolveOutcome {
        rows,
        source,
        selected: context.selected_data,
        gain_stats: gain_stats.map(SolveStatsBuilder::finish),
        solved_fields: solutions.iter().map(|interval| interval.field).collect(),
        solved_windows: solutions
            .iter()
            .map(|interval| interval.spectral_window)
            .collect(),
    })
}

struct LeakageInputs<'a> {
    ms: &'a MeasurementSet,
    request: &'a LeakageSolveRequest,
    models: &'a super::super::model::ModelInputs,
    preapplied: Option<&'a PreappliedRows>,
    spectral_weights: SpectralWeights,
    windows: &'a BTreeMap<i32, WindowChannels>,
    parallactic_angles: Option<&'a ParallacticAngleState>,
    tool: bool,
}

struct WindowChannels {
    channels: Vec<usize>,
    bin_width: usize,
}

impl WindowChannels {
    fn count(&self) -> usize {
        self.channels.len().div_ceil(self.bin_width)
    }
}

struct BaselineAverages {
    segment: BaselineSegment,
    bins: Vec<CoherencyMean>,
    centroid: WeightedTime,
}

#[derive(Default)]
struct GainIntervalData {
    antennas: BTreeSet<i32>,
    positive_weight: bool,
}

impl GainIntervalData {
    fn observe(&mut self, pair: [i32; 2], bins: &[CoherencyMean]) {
        if bins.iter().any(|bin| bin.unflagged) {
            self.antennas.extend(pair);
        }
        self.positive_weight |= bins.iter().any(|bin| bin.weights.iter().any(|&w| w > 0.0));
    }
}

impl BaselineAverages {
    fn finish_time(&mut self, solution_time: &mut WeightedTime, mode: GainSolveMode) {
        // SDBList reduces each baseline's Float weight spectrum before
        // converting its aggregate weight to Double.
        let weight = if mode == GainSolveMode::Phase {
            self.bins
                .iter()
                .flat_map(|bin| bin.data.into_iter().zip(bin.weights))
                .map(|(sum, weight)| {
                    if weight > 0.0 {
                        parallactic_fit::solve_sample(mode, sum / weight, weight).1
                    } else {
                        weight
                    }
                })
                .sum::<f32>()
        } else {
            self.bins.iter().flat_map(|bin| bin.weights).sum::<f32>()
        };
        let weight = f64::from(weight);
        if self.centroid.mean().is_some() {
            solution_time.observe_mean(self.centroid, weight);
        } else if weight > 0.0 {
            solution_time.observe(self.segment.start, weight);
        }
        self.centroid = WeightedTime::default();
    }
}

fn interval_window(rows: &[BucketedRow<'_>]) -> i32 {
    rows.iter()
        .map(|(row, _, _)| row.data_spw_id)
        .min()
        .expect("nonempty solve interval")
}

impl LeakageInputs<'_> {
    fn solve_interval(
        &self,
        rows: &[BucketedRow<'_>],
        antennas: usize,
        references: &[i32],
        previous: Option<&LeakageSolution>,
        gain_stats: Option<&mut SolveStatsBuilder>,
    ) -> Result<Option<LeakageSolution>, GainSolveError> {
        let first = rows[0].0;
        let spectral_window = interval_window(rows);
        let count = self.windows[&spectral_window].count();
        let kind = self.request.kind;
        let segment_centroid =
            kind.cross_hand() || (self.tool && self.request.solve_mode == GainSolveMode::Phase);
        let requested = if self.request.preaverage_seconds < 0.0 {
            match kind {
                LeakageSolveKind::PosAng => 30.0,
                LeakageSolveKind::Xj
                | LeakageSolveKind::Xf
                | LeakageSolveKind::XY
                | LeakageSolveKind::XYf => 0.0,
                _ => 300.0,
            }
        } else {
            self.request.preaverage_seconds
        };
        // For preavg=0 CASA averages throughout the enclosing solint.
        let span = (requested > f64::from(f32::EPSILON)).then_some(0.999 * requested);
        let circular = kind.intensity_normalized()
            && correlation_types_for_ddid(self.ms, first.data_desc_id)?.contains(&5);
        let mut fits = IntervalFits::new(count, antennas, kind, circular, self.request.solve_mode);
        let mut averages = BTreeMap::new();
        let mut time = WeightedTime::default();
        let mut gain_data = gain_stats.as_ref().map(|_| GainIntervalData::default());
        for batch in rows.chunks(256) {
            let indices = batch
                .iter()
                .map(|(row, _, _)| row.row_index)
                .collect::<Vec<_>>();
            let input = read_input_batch(
                self.ms,
                &indices,
                (
                    self.preapplied.is_none_or(|prior| prior.data.is_empty()),
                    batch
                        .iter()
                        .any(|(row, _, _)| self.models.uses_fourier(row.field_id)),
                ),
                batch
                    .iter()
                    .any(|(row, _, _)| self.models.reads_column(row.field_id)),
                batch
                    .iter()
                    .map(|row| self.spectral_weights.contains(row))
                    .collect(),
                false,
            )?;
            for (index, &(row, _, _)) in batch.iter().enumerate() {
                let pair = [row.antenna1, row.antenna2];
                let active = averages
                    .entry((row.field_id, row.data_desc_id, pair))
                    .or_insert_with(|| BaselineAverages {
                        segment: BaselineSegment {
                            field: row.field_id,
                            data_desc: row.data_desc_id,
                            spectral_window: row.data_spw_id,
                            start: row.time_seconds,
                            end: row.time_seconds,
                            emission: f64::INFINITY,
                        },
                        bins: vec![CoherencyMean::default(); count],
                        centroid: WeightedTime::default(),
                    });
                if span.is_some_and(|span| row.time_seconds - active.segment.start > span) {
                    if let Some(data) = &mut gain_data {
                        data.observe(pair, &active.bins);
                    }
                    if segment_centroid {
                        active.finish_time(&mut time, self.request.solve_mode);
                    }
                    let segment = BaselineSegment {
                        emission: row.time_seconds,
                        ..active.segment
                    };
                    fits.accumulate(pair, &mut active.bins, antennas, segment)?;
                    active.segment.start = row.time_seconds;
                }
                active.segment.end = row.time_seconds;
                let (spectral_weight, scalar_weight) = self.average_row(
                    row,
                    (&input, index),
                    &self.windows[&row.data_spw_id],
                    &mut active.bins,
                )?;
                if let Some(scalar_weight) = scalar_weight {
                    active
                        .centroid
                        .observe(row.time_centroid_seconds, scalar_weight);
                } else if segment_centroid {
                    active
                        .centroid
                        .observe(row.time_centroid_seconds, spectral_weight);
                } else {
                    time.observe(row.time_centroid_seconds, spectral_weight);
                }
            }
        }
        for ((_, _, pair), mut active) in averages {
            if let Some(data) = &mut gain_data {
                data.observe(pair, &active.bins);
            }
            if segment_centroid {
                active.finish_time(&mut time, self.request.solve_mode);
            }
            fits.accumulate(pair, &mut active.bins, antennas, active.segment)?;
        }
        let mut stages = self.tool.then(LeakageFitStages::default);
        let solution = if gain_data.as_ref().is_none_or(|data| data.positive_weight) {
            if self.tool && kind.iterative() && self.request.correlation_dependent_flags {
                return Err(GainSolveError::LeakageCorrelationFlagging);
            }
            fits.finish(
                antennas,
                references,
                previous,
                self.request,
                (first.field_id, spectral_window),
                self.parallactic_angles,
                stages.as_mut(),
            )?
        } else {
            None
        };
        if let Some((stats, data)) = gain_stats.zip(gain_data) {
            let expected = rows
                .iter()
                .filter(|(row, _, _)| row.antenna1 != row.antenna2)
                .flat_map(|(row, _, _)| [row.antenna1, row.antenna2])
                .collect::<BTreeSet<_>>();
            stats.interval(&IntervalStages {
                spw_id: spectral_window,
                antennas: &expected,
                expected_window: !self.tool || !expected.is_empty(),
                attempted_interval: self.tool.then_some(data.positive_weight),
                with_data: vec![data.antennas.clone(), data.antennas],
                constrained: stages.as_mut().map_or_else(BTreeSet::new, |stages| {
                    std::mem::take(&mut stages.constrained)
                }),
            });
            if let Some(stages) = stages {
                stats.record_success(spectral_window, solution.is_some());
                stats.solutions(
                    spectral_window,
                    stages
                        .passed
                        .into_iter()
                        .enumerate()
                        .map(|(id, passed)| AntennaSolution {
                            antenna_id: id as i32,
                            passed: passed.into(),
                        }),
                );
            }
        }
        Ok(
            solution.map(|(reference, values, polarization)| LeakageSolution {
                spectral_window,
                field: first.field_id,
                observation: first.observation_id,
                scan: first.scan_number,
                time: time.mean().unwrap_or(first.time_seconds),
                reference,
                values,
                polarization,
            }),
        )
    }

    fn average_row(
        &self,
        row: &SelectedSolveRow,
        batch: (&SolveInputBatch, usize),
        window: &WindowChannels,
        bins: &mut [CoherencyMean],
    ) -> Result<(f64, Option<f64>), GainSolveError> {
        let correlations = correlation_types_for_ddid(self.ms, row.data_desc_id)?;
        let order = coherency_order(row.data_desc_id, &correlations).map_err(prior_error)?;
        let prior = self
            .preapplied
            .and_then(|prior| prior.data_prior.as_ref())
            .map(|prior| {
                prior
                    .correct(
                        row.row_index,
                        &correlations,
                        input_cell(self.ms, row.row_index, batch, "DATA")?,
                        input_cell(self.ms, row.row_index, batch, "FLAG")?,
                    )
                    .map_err(prior_error)
            })
            .transpose()?
            .flatten();
        let input = read_row_inputs(
            self.ms,
            row,
            prior.as_ref(),
            self.models.reads_column(row.field_id),
            batch,
        )?;
        let raw_flags = if self.models.uses_fourier(row.field_id) {
            match input_cell(self.ms, row.row_index, batch, "FLAG")? {
                ArrayValue::Bool(flags) => flags,
                other => return Err(unsupported_shape("FLAG", other)),
            }
        } else {
            input.flags.as_ref()
        };
        let prediction = self
            .models
            .predict(self.ms, row, &correlations, raw_flags)?;
        let point_model = || {
            let first = correlations[order[0]];
            ArrayD::from_shape_fn(input.data.raw_dim(), |index| {
                samples::point_visibility(self.request.stokes, correlations[index[0]], first)
            })
        };
        let mut model = prediction
            .or_else(|| input.model.cloned())
            .unwrap_or_else(point_model);
        // For +QU, CASA normalizes the measured coherencies by the model's
        // complex Stokes I and applies P to a unity model to recover the
        // feed response to cross-hand source polarization.
        let intensity = matches!(
            self.request.kind.source(),
            Some(SourceFitMode::Polarization)
        )
        .then(|| {
            let intensity = (0..model.shape()[1])
                .map(|channel| (model[[order[0], channel]] + model[[order[3], channel]]) * 0.5)
                .collect::<Vec<_>>();
            model.fill(Complex32::ONE);
            intensity
        });
        let (model, flags) = match self.preapplied.and_then(|prior| prior.model.as_ref()) {
            Some(forward) => forward
                .corrupt(row.row_index, &correlations, model, &input.flags)
                .map_err(prior_error)?,
            None => (model, None),
        };
        let flags = flags.as_ref().unwrap_or(input.flags.as_ref());
        let samples = input.samples();
        let stokes_i = |channel| (model[[order[0], channel]] + model[[order[3], channel]]) * 0.5;
        // The X solve time combines baseline centroids using spectral
        // weights, while time averaging each baseline uses scalar weights.
        let scalar_weight = self.request.kind.cross_hand().then(|| {
            let normalized = SolveSamples {
                flags: samples::two_dimensional(flags),
                ..samples
            };
            let shared_flags = (!self.request.correlation_dependent_flags)
                .then(|| samples::channel_flags(&normalized));
            let channels = SelectedChannels {
                indices: &window.channels,
                all_channels: false,
                bin_width: (window.bin_width > 1).then_some(window.bin_width),
                excluded: &[],
                bin_weights: None,
            };
            order
                .into_iter()
                .map(|corr| {
                    samples::centroid_weight(
                        &normalized,
                        corr,
                        channels,
                        shared_flags.as_deref(),
                        |channel| {
                            if self.request.kind.intensity_normalized() {
                                stokes_i(channel)
                            } else {
                                model[[corr, channel]]
                            }
                        },
                        true,
                    )
                })
                .filter(|weight| weight.is_finite() && *weight > 0.0)
                .sum()
        });
        let mut total = 0.0;
        let retain_unweighted_model = self.request.kind.iterative()
            && matches!(self.request.interval, GainSolveInterval::Integration)
            && window.bin_width == 1;
        for (position, &channel) in window.channels.iter().enumerate() {
            if channel >= samples.data.shape()[1]
                || (!self.request.correlation_dependent_flags
                    && order.iter().any(|&corr| flags[[corr, channel]]))
                || order.iter().all(|&corr| flags[[corr, channel]])
            {
                continue;
            }
            let slot = position / window.bin_width;
            let mut data = order.map(|corr| samples.data[[corr, channel]]);
            let mut weights = order.map(|corr| {
                if flags[[corr, channel]] {
                    0.0
                } else {
                    samples.weights[[corr, channel]]
                }
            });
            let mut model = order.map(|corr| model[[corr, channel]]);
            if let Some(intensity) = &intensity {
                let scale = intensity[channel];
                if scale == Complex32::ZERO || !scale.is_finite() {
                    continue;
                }
                data = if scale.im == 0.0 {
                    data.map(|value| value / scale.re)
                } else {
                    data.map(|value| value / scale)
                };
                let amplitude = scale.norm();
                weights = weights.map(|weight| weight * (amplitude * amplitude));
            } else if self.request.kind.intensity_normalized() {
                let scale = (model[0] + model[3]) * 0.5;
                let power = scale.norm_sqr();
                if power > 0.0 {
                    data = data.map(|value| value / scale);
                    model = model.map(|value| value / scale);
                    weights = weights.map(|weight| weight * power);
                }
            } else if self.request.kind.cross_hand() {
                for (corr, model) in model.into_iter().enumerate() {
                    let amplitude = model.norm();
                    if amplitude > 0.0 {
                        let normalized = super::super::kernel::complex32_to_64(data[corr])
                            / super::super::kernel::complex32_to_64(model);
                        data[corr] = Complex32::new(normalized.re as f32, normalized.im as f32);
                        weights[corr] *= amplitude * amplitude;
                    }
                }
            }
            bins[slot].observe(data, model, weights, retain_unweighted_model);
            total += weights
                .into_iter()
                .filter(|weight| weight.is_finite() && *weight > 0.0)
                .map(f64::from)
                .sum::<f64>();
        }
        Ok((total, scalar_weight))
    }
}

fn prior_error(source: crate::ApplyExecutionError) -> GainSolveError {
    GainSolveError::PriorCalibrationApply {
        source: Box::new(source),
    }
}

#[derive(Default, Clone)]
struct CoherencyMean {
    data: [Complex32; 4],
    model: [Complex32; 4],
    weights: [f32; 4],
    unflagged: bool,
}

impl CoherencyMean {
    fn observe(
        &mut self,
        data: [Complex32; 4],
        model: [Complex32; 4],
        weights: [f32; 4],
        retain_unweighted_model: bool,
    ) {
        self.unflagged = true;
        for (corr, weight) in weights.into_iter().enumerate() {
            if weight > 0.0
                && weight.is_finite()
                && data[corr].is_finite()
                && model[corr].is_finite()
            {
                if self.weights[corr] == 0.0 {
                    self.model[corr] = Complex32::ZERO;
                }
                self.data[corr] += weight * data[corr];
                self.model[corr] += weight * model[corr];
                self.weights[corr] += weight;
            } else if retain_unweighted_model
                && self.weights[corr] == 0.0
                && model[corr].is_finite()
            {
                // The unaveraged model keeps its parallel hands even when
                // the corresponding observations have zero weight.
                self.model[corr] = model[corr];
            }
        }
    }

    fn finish(mut self, require_all: bool) -> Option<([Complex32; 4], [Complex32; 4], [f32; 4])> {
        if self.weights.iter().all(|&weight| weight <= 0.0) {
            return None;
        }
        for (corr, weight) in self.weights.into_iter().enumerate() {
            if weight > 0.0 {
                self.data[corr] /= weight;
                self.model[corr] /= weight;
            } else if require_all {
                return None;
            }
        }
        Some((self.data, self.model, self.weights))
    }
}
