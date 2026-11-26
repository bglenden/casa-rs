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
use crate::execute::{ParallacticAngleState, coherency_order, load_parallactic_angle_state};
use crate::session::prepare_calibration_solve;
use crate::solve_stats::{
    AntennaSolution, IntervalStages, SolveStats, SolveStatsBuilder, ToolSolveActivity,
};
use crate::{ApplyCalibrationTableSpec, SelectedData};
use casa_types::{ArrayValue, RecordField, RecordValue, Value};
use std::path::PathBuf;

use interval::{IntervalFits, LeakageFitStages};
use joint::SourceFitMode;
use parallactic::BaselineSegment;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LeakageSolveKind {
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
    XMueller,
    Xf,
    PosAng,
    Xparang,
    Xfparang,
    XY,
    XYf,
}

impl LeakageSolveKind {
    pub fn subtype(self) -> &'static str {
        match self {
            Self::D | Self::DQu | Self::DX => "D Jones",
            Self::Df | Self::DfQu | Self::DfX => "Df Jones",
            Self::DGen | Self::DGenQu | Self::DGenX => "Dgen Jones",
            Self::DfGen | Self::DfGenQu | Self::DfGenX => "Dfgen Jones",
            Self::Dlls => "DLLS Jones",
            Self::Dflls => "DfLLS Jones",
            Self::Xj => "X Jones",
            Self::XMueller => "X Mueller",
            Self::Xf => "Xf Jones",
            Self::PosAng => "PosAng Jones",
            Self::Xparang => "Xparang Jones",
            Self::Xfparang => "Xfparang Jones",
            Self::XY => "GlinXph Jones",
            Self::XYf => "GlinXphf Jones",
        }
    }

    pub fn channelized(self) -> bool {
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

    pub fn cross_hand(self) -> bool {
        matches!(self, Self::Xj | Self::XMueller | Self::Xf | Self::PosAng) || self.parallactic()
    }

    fn parameter_count(self) -> usize {
        if self.cross_hand() && !self.gain_phase() {
            1
        } else {
            2
        }
    }

    fn parallactic(self) -> bool {
        matches!(self, Self::Xparang | Self::Xfparang) || self.gain_phase()
    }

    fn intensity_normalized(self) -> bool {
        matches!(self, Self::PosAng | Self::Xparang | Self::Xfparang)
    }

    pub fn gain_phase(self) -> bool {
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
pub struct LeakageSolveRequest {
    pub output: PathBuf,
    pub selection: MsSelection,
    pub kind: LeakageSolveKind,
    pub model_source: GainSolveModelSource,
    pub stokes: [f32; 4],
    pub priors: Vec<ApplyCalibrationTableSpec>,
    pub parang_terms: usize,
    pub correlation_dependent_flags: bool,
    pub solve_mode: GainSolveMode,
    pub normalization: Option<crate::GainNormalization>,
    pub interval: GainSolveInterval,
    pub frequency_interval: Option<crate::BandpassFrequencyInterval>,
    pub combine: GainSolveCombine,
    pub preaverage_seconds: f64,
    pub min_baselines: usize,
    pub min_snr: f32,
    pub refant: RefAntSelector,
    pub append: bool,
}

impl LeakageSolveRequest {
    fn prior_calibration_mode(&self) -> PriorCalibrationMode {
        let share_input_flags = !self.correlation_dependent_flags;
        let parang_terms = self.parang_terms;
        match self.kind {
            LeakageSolveKind::PosAng => PriorCalibrationMode::PositionAngle {
                share_input_flags,
                parang_terms,
            },
            kind if kind.cross_hand() => PriorCalibrationMode::CrossHand {
                share_input_flags,
                parang_terms,
            },
            _ => PriorCalibrationMode::Leakage {
                share_input_flags,
                parang_terms,
            },
        }
    }
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

pub struct PolarizationSolveOutcome {
    pub rows: usize,
    pub source: BTreeMap<String, BTreeMap<i32, Complex32>>,
    pub selected: SelectedData,
    pub gain_stats: Option<SolveStats>,
    pub solved_fields: BTreeSet<i32>,
    pub solved_windows: BTreeSet<i32>,
}

pub fn solve_polarization(
    ms: &MeasurementSet,
    request: &LeakageSolveRequest,
) -> Result<PolarizationSolveOutcome, String> {
    solve_polarization_with_activity(ms, request, None, false)
}

pub(crate) fn solve_polarization_with_activity(
    ms: &MeasurementSet,
    request: &LeakageSolveRequest,
    activity: Option<&mut ToolSolveActivity>,
    legacy: bool,
) -> Result<PolarizationSolveOutcome, String> {
    if request.kind == LeakageSolveKind::XMueller && !legacy {
        return Err("Can't do selfGatherAndSolve for X Mueller".into());
    }
    let preparation = crate::timing::Phase::start("polarization_prepare");
    let error = |error: GainSolveError| error.to_string();
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &request.refant,
        &request.priors,
        request.parang_terms > 0,
        true,
        request.prior_calibration_mode(),
    )
    .map_err(error)?;
    let legacy_cross_hand = legacy && request.kind.cross_hand();
    let include_autocorrelations = request.kind.cross_hand()
        && (!request.kind.parallactic() || (activity.is_some() && request.kind.gain_phase()));
    let ordered = bucketed_rows(
        &context.rows,
        request.interval,
        request.combine,
        include_autocorrelations,
        legacy_cross_hand,
        false,
    );
    let mut models = super::super::model::ModelInputs::new(ms, &context.rows, request.model_source)
        .map_err(error)?;
    models
        .bind_buffers(ms, &ordered, request.combine)
        .map_err(error)?;
    models.phase_times =
        weights::chunk_phase_times(&ordered, request.combine, models.dynamic_fields());
    let spectral_weights = if legacy_cross_hand {
        SpectralWeights::default()
    } else {
        SpectralWeights::new(ms, &ordered, request.combine).map_err(error)?
    };
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
        legacy_cross_hand,
    };
    let intervals = polarization_intervals(&ordered, request.kind);
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
    let mut gain_stats = if request.kind.gain_phase()
        || (setup.tool && request.kind != LeakageSolveKind::XMueller)
    {
        let windows = crate::solve::spectral_window_ids(ms).map_err(error)?;
        Some(SolveStatsBuilder::new(
            request.kind.parameter_count(),
            windows,
            &context.available_antennas,
        ))
    } else {
        None
    };
    drop(preparation);
    let fitting = crate::timing::Phase::start("polarization_gather_and_fit");
    let mut solutions = fit_polarization_intervals(
        &setup,
        intervals,
        context.available_antennas.len(),
        &mut references,
        gain_stats.as_mut(),
    )
    .map_err(error)?;
    if request.kind.iterative() {
        interval::reference_iterative_solutions(&mut solutions, &mut references);
    }
    if setup.tool
        && !matches!(
            request.kind,
            LeakageSolveKind::PosAng | LeakageSolveKind::XMueller
        )
        && !request.kind.parallactic()
    {
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
    let source = source_polarization(ms, request.kind, setup.tool, &solutions)?;
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

fn polarization_intervals<'a>(
    ordered: &'a [BucketedRow<'a>],
    kind: LeakageSolveKind,
) -> Vec<(&'a [BucketedRow<'a>], i32)> {
    let mut groups = ordered.chunk_by(|a, b| a.1 == b.1 && a.2 == b.2).peekable();
    let mut intervals = Vec::new();
    while let Some(rows) = groups.next() {
        let scan = if kind == LeakageSolveKind::XMueller {
            // The self-gather reads the legacy buffer's scan after advancing
            // the iterator. Its final interval retains the last chunk's scan.
            groups.peek().map_or_else(
                || rows.last().expect("nonempty solve interval").0.scan_number,
                |next| next[0].0.scan_number,
            )
        } else {
            rows[0].0.scan_number
        };
        intervals.push((rows, scan));
    }
    if kind != LeakageSolveKind::XMueller {
        intervals.sort_by(|(a, _), (b, _)| {
            interval_window(a)
                .cmp(&interval_window(b))
                .then_with(|| a[0].0.time_seconds.total_cmp(&b[0].0.time_seconds))
        });
    }
    intervals
}

fn fit_polarization_intervals(
    setup: &LeakageInputs<'_>,
    intervals: Vec<(&[BucketedRow<'_>], i32)>,
    antennas: usize,
    references: &mut [i32],
    mut stats: Option<&mut SolveStatsBuilder>,
) -> Result<Vec<LeakageSolution>, GainSolveError> {
    let mueller = setup.request.kind == LeakageSolveKind::XMueller;
    let mut previous = None;
    let mut previous_mueller_time = None;
    let mut solutions: Vec<LeakageSolution> = Vec::new();
    for (rows, scan) in intervals {
        let previous_interval = previous
            .and_then(|index: usize| solutions.get(index))
            .filter(|solution| solution.spectral_window == interval_window(rows));
        let solution = setup.solve_interval(
            rows,
            scan,
            antennas,
            references,
            previous_interval,
            stats.as_deref_mut(),
        )?;
        if let Some(solution) = solution {
            if solution.reference >= 0 {
                references[1] = solution.reference;
                previous = Some(solutions.len());
            }
            if mueller {
                previous_mueller_time = Some(solution.time);
            }
            solutions.push(solution);
        } else if mueller {
            // A failed legacy sync keeps the previous TIME but publishes
            // this interval's flagged unity baseline parameters.
            let estimate = LeakageEstimate {
                gain: [Complex32::ONE, Complex32::ZERO],
                ..Default::default()
            };
            let first = rows[0].0;
            solutions.push(LeakageSolution {
                spectral_window: interval_window(rows),
                field: first.field_id,
                observation: first.observation_id,
                scan,
                time: previous_mueller_time.unwrap_or(0.0),
                reference: -1,
                polarization: None,
                values: vec![vec![estimate; antennas]],
            });
        }
    }
    if mueller && previous_mueller_time.is_none() {
        solutions.clear();
    }
    Ok(solutions)
}

fn source_polarization(
    ms: &MeasurementSet,
    kind: LeakageSolveKind,
    tool: bool,
    solutions: &[LeakageSolution],
) -> Result<BTreeMap<String, BTreeMap<i32, Complex32>>, String> {
    if !(kind == LeakageSolveKind::Xfparang || (tool && kind == LeakageSolveKind::Xparang)) {
        return Ok(BTreeMap::new());
    }
    let fields = ms.field().map_err(|error| error.to_string())?;
    let mut source = BTreeMap::<String, BTreeMap<i32, Complex32>>::new();
    for interval in solutions {
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
    Ok(source)
}

pub(crate) fn fractional_stokes_record(
    source: BTreeMap<String, BTreeMap<i32, Complex32>>,
) -> RecordValue {
    RecordValue::new(
        source
            .into_iter()
            .map(|(field, windows)| {
                let mut average = Complex32::ZERO;
                let vector = |qu: Complex32| {
                    Value::Array(ArrayValue::from_f64_vec(vec![
                        1.0,
                        f64::from(qu.re),
                        f64::from(qu.im),
                        0.0,
                    ]))
                };
                let mut entries = windows
                    .iter()
                    .map(|(id, &qu)| {
                        average += qu;
                        RecordField::new(format!("Spw{id}"), vector(qu))
                    })
                    .collect::<Vec<_>>();
                average /= windows.len() as f32;
                entries.sort_unstable_by(|left, right| left.name.cmp(&right.name));
                entries.push(RecordField::new("SpwAve", vector(average)));
                RecordField::new(field, Value::Record(RecordValue::new(entries)))
            })
            .collect(),
    )
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
    legacy_cross_hand: bool,
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
    legacy_xy_weights: Option<[f32; 4]>,
}

struct RowAveragingWeights {
    spectrum: f64,
    scalar: Option<f64>,
    legacy_xy: Option<[f32; 4]>,
}

#[derive(Default)]
struct GainIntervalData {
    antennas: BTreeSet<i32>,
    positive_weight: bool,
}

impl GainIntervalData {
    fn observe(&mut self, pair: [i32; 2], bins: &[CoherencyMean]) {
        if bins.iter().any(|bin| bin.unflagged != 0) {
            self.antennas.extend(pair);
        }
        self.positive_weight |= bins.iter().any(|bin| bin.weights.iter().any(|&w| w > 0.0));
    }

    fn record(
        self,
        stats: &mut SolveStatsBuilder,
        rows: &[BucketedRow<'_>],
        stages: Option<LeakageFitStages>,
        tool: bool,
        spectral_window: i32,
        solved: bool,
    ) {
        let expected = rows
            .iter()
            .filter(|(row, _, _)| row.antenna1 != row.antenna2)
            .flat_map(|(row, _, _)| [row.antenna1, row.antenna2])
            .collect::<BTreeSet<_>>();
        let (constrained, passed) = stages.map_or_else(
            || (BTreeSet::new(), None),
            |stages| (stages.constrained, Some(stages.passed)),
        );
        stats.interval(&IntervalStages {
            spw_id: spectral_window,
            antennas: &expected,
            expected_window: !tool || !expected.is_empty(),
            attempted_interval: tool.then_some(self.positive_weight),
            with_data: vec![self.antennas.clone(), self.antennas],
            constrained,
        });
        if let Some(passed) = passed {
            stats.record_success(spectral_window, solved);
            stats.solutions(
                spectral_window,
                passed
                    .into_iter()
                    .enumerate()
                    .map(|(id, passed)| AntennaSolution {
                        antenna_id: id as i32,
                        passed: passed.into(),
                    }),
            );
        }
    }
}

impl BaselineAverages {
    fn observe_legacy_xy_weights(&mut self, row: Option<[f32; 4]>) {
        if let Some(row) = row {
            let total = self.legacy_xy_weights.get_or_insert([0.0; 4]);
            for (sum, weight) in total.iter_mut().zip(row) {
                *sum += weight;
            }
        }
    }

    fn finish_legacy_xy_weights(&mut self) {
        if let Some(weights) = self.legacy_xy_weights.take() {
            for bin in &mut self.bins {
                if bin.unflagged != 0 {
                    // VisBuffAccumulator uses the scalar total even if some
                    // contributing time samples had this channel flagged.
                    bin.weights = weights;
                }
            }
        }
    }

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
    fn read_interval_batch(
        &self,
        rows: &[BucketedRow<'_>],
    ) -> Result<SolveInputBatch, GainSolveError> {
        let indices = rows
            .iter()
            .map(|(row, _, _)| row.row_index)
            .collect::<Vec<_>>();
        read_input_batch(
            self.ms,
            &indices,
            (
                self.preapplied.is_none_or(|prior| prior.data.is_empty()),
                rows.iter()
                    .any(|(row, _, _)| self.models.uses_fourier(row.field_id)),
            ),
            rows.iter()
                .any(|(row, _, _)| self.models.reads_column(row.field_id)),
            rows.iter()
                .any(|(row, _, _)| self.models.uses_virtual_model(row.field_id)),
            rows.iter()
                .map(|row| self.spectral_weights.contains(row))
                .collect(),
            false,
        )
    }

    fn preaverage_span(&self) -> Option<f64> {
        let requested = if self.request.preaverage_seconds < 0.0 {
            match self.request.kind {
                LeakageSolveKind::PosAng => 30.0,
                LeakageSolveKind::Xj | LeakageSolveKind::Xf if self.legacy_cross_hand => 300.0,
                kind if kind.gain_phase() && self.legacy_cross_hand => f64::INFINITY,
                LeakageSolveKind::Xj
                | LeakageSolveKind::Xf
                | LeakageSolveKind::XY
                | LeakageSolveKind::XYf => 0.0,
                _ => 300.0,
            }
        } else {
            self.request.preaverage_seconds
        };
        if self.legacy_cross_hand {
            // VisBuffAccumulator substitutes 0.1 seconds for a zero interval.
            Some(
                if requested < f64::EPSILON {
                    0.1
                } else {
                    requested
                } - f64::EPSILON,
            )
        } else {
            // VI2 averages throughout the enclosing solint when preavg=0.
            (requested > f64::from(f32::EPSILON)).then_some(0.999 * requested)
        }
    }

    fn solve_interval(
        &self,
        rows: &[BucketedRow<'_>],
        scan: i32,
        antennas: usize,
        references: &[i32],
        previous: Option<&LeakageSolution>,
        gain_stats: Option<&mut SolveStatsBuilder>,
    ) -> Result<Option<LeakageSolution>, GainSolveError> {
        let first = rows[0].0;
        let spectral_window = interval_window(rows);
        let count = self.windows[&spectral_window].count();
        let kind = self.request.kind;
        let mueller = kind == LeakageSolveKind::XMueller;
        let segment_centroid = !self.legacy_cross_hand
            && (kind.cross_hand()
                || (self.tool && self.request.solve_mode == GainSolveMode::Phase));
        let span = self.preaverage_span();
        let circular = kind.intensity_normalized()
            && correlation_types_for_ddid(self.ms, first.data_desc_id)?.contains(&5);
        let mut fits = IntervalFits::new(
            count,
            antennas,
            kind,
            circular,
            self.request.solve_mode,
            self.legacy_cross_hand,
        );
        let mut averages = BTreeMap::new();
        let mut time = WeightedTime::default();
        let mut legacy = self
            .legacy_cross_hand
            .then(cross_hand::LegacyCrossHandAveraging::default);
        let mut gain_data = gain_stats.as_ref().map(|_| GainIntervalData::default());
        for batch in rows.chunks(256) {
            let input = self.read_interval_batch(batch)?;
            for (index, &(row, _, _)) in batch.iter().enumerate() {
                let pair = [row.antenna1, row.antenna2];
                let start = legacy.as_mut().map_or(row.time_seconds, |legacy| {
                    legacy.start(row, span.expect("VI1 time averaging interval"), mueller)
                });
                let buffer = if mueller {
                    (0, 0)
                } else {
                    (row.field_id, row.data_desc_id)
                };
                let active = averages
                    .entry((buffer.0, buffer.1, pair))
                    .or_insert_with(|| BaselineAverages {
                        segment: BaselineSegment {
                            field: row.field_id,
                            data_desc: row.data_desc_id,
                            spectral_window: row.data_spw_id,
                            start,
                            end: row.time_seconds,
                            emission: f64::INFINITY,
                        },
                        bins: vec![CoherencyMean::default(); count],
                        centroid: WeightedTime::default(),
                        legacy_xy_weights: None,
                    });
                let flush = if legacy.is_some() {
                    start > active.segment.start
                } else {
                    span.is_some_and(|span| row.time_seconds - active.segment.start > span)
                };
                if flush {
                    active.finish_legacy_xy_weights();
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
                    if mueller || legacy.as_ref().is_none_or(|legacy| legacy.fits(segment)) {
                        fits.accumulate(
                            pair,
                            &mut active.bins,
                            antennas,
                            segment,
                            self.legacy_cross_hand,
                        )?;
                    } else {
                        active.bins.fill(CoherencyMean::default());
                    }
                    active.segment.start = start;
                }
                active.segment.end = row.time_seconds;
                let weights = self.average_row(
                    row,
                    (&input, index),
                    &self.windows[&row.data_spw_id],
                    &mut active.bins,
                )?;
                active.observe_legacy_xy_weights(weights.legacy_xy);
                if let Some(legacy) = &mut legacy
                    && active.bins.iter().any(|bin| bin.unflagged != 0)
                    && !legacy.observe(active.segment)
                    && kind.gain_phase()
                {
                    return Err(GainSolveError::LegacyGainPhaseMultipleBuffers);
                }
                if self.legacy_cross_hand {
                    let weight = weights.scalar.unwrap_or_default();
                    time.observe(row.time_seconds, weight);
                    if let Some(legacy) = &mut legacy
                        && kind.gain_phase()
                    {
                        legacy.observe_time(active.segment, row.time_seconds, weight);
                    }
                } else if let Some(scalar_weight) = weights.scalar {
                    active
                        .centroid
                        .observe(row.time_centroid_seconds, scalar_weight);
                } else if segment_centroid {
                    active
                        .centroid
                        .observe(row.time_centroid_seconds, weights.spectrum);
                } else {
                    time.observe(row.time_centroid_seconds, weights.spectrum);
                }
            }
        }
        for ((_, _, pair), mut active) in averages {
            active.finish_legacy_xy_weights();
            if let Some(data) = &mut gain_data {
                data.observe(pair, &active.bins);
            }
            if segment_centroid {
                active.finish_time(&mut time, self.request.solve_mode);
            }
            if mueller
                || legacy
                    .as_ref()
                    .is_none_or(|legacy| legacy.fits(active.segment))
            {
                fits.accumulate(
                    pair,
                    &mut active.bins,
                    antennas,
                    active.segment,
                    self.legacy_cross_hand,
                )?;
            }
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
                legacy.as_ref(),
                stages.as_mut(),
            )?
        } else {
            None
        };
        if let Some((stats, data)) = gain_stats.zip(gain_data) {
            data.record(
                stats,
                rows,
                stages,
                self.tool,
                spectral_window,
                solution.is_some(),
            );
        }
        Ok(
            solution.map(|(reference, values, polarization)| LeakageSolution {
                spectral_window,
                field: legacy
                    .and_then(|legacy| legacy.field())
                    .unwrap_or(first.field_id),
                observation: first.observation_id,
                scan,
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
    ) -> Result<RowAveragingWeights, GainSolveError> {
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
        let prediction = self.models.predict(
            self.ms,
            row,
            &correlations,
            raw_flags,
            batch.0.cell("UVW", batch.1),
        )?;
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
        // The X solve time combines baseline centroids using spectral
        // weights, while time averaging each baseline uses scalar weights.
        let mut legacy_weights = self.legacy_cross_hand.then(|| {
            let normalize = self.request.kind != LeakageSolveKind::XMueller;
            cross_hand::legacy_row_weights(
                &samples,
                flags,
                &model,
                order,
                &window.channels,
                normalize,
            )
        });
        let legacy_xy = self.legacy_cross_hand && self.request.kind.gain_phase();
        let legacy_bin_counts =
            if legacy_xy && self.request.kind.channelized() && window.bin_width > 1 {
                if let Some((weights, _)) = &mut legacy_weights {
                    // All output channels use the nominal channel averaging factor.
                    weights
                        .iter_mut()
                        .for_each(|weight| *weight *= window.bin_width as f32);
                }
                Some(cross_hand::legacy_bin_counts(
                    flags,
                    order,
                    &window.channels,
                    window.bin_width,
                ))
            } else {
                None
            };
        let scalar_weight = if let Some((weights, _)) = legacy_weights {
            Some(f64::from((weights[0] + weights[3]) * 0.5))
        } else if self.request.kind.cross_hand() {
            Some(self.cross_hand_centroid_weight(samples, flags, &model, order, window))
        } else {
            None
        };
        let mut total = 0.0;
        let retain_unweighted_model = self.request.kind.iterative()
            && matches!(self.request.interval, GainSolveInterval::Integration)
            && window.bin_width == 1;
        let share_flags = !self.request.correlation_dependent_flags || self.legacy_cross_hand;
        for (position, &channel) in window.channels.iter().enumerate() {
            if channel >= samples.data.shape()[1] {
                continue;
            }
            let unflagged = order
                .into_iter()
                .enumerate()
                .fold(0u8, |mask, (slot, corr)| {
                    mask | (u8::from(!flags[[corr, channel]]) << slot)
                });
            if unflagged == 0 || (share_flags && unflagged != 0b1111) {
                continue;
            }
            let slot = position / window.bin_width;
            let mut data = order.map(|corr| samples.data[[corr, channel]]);
            let mut weights = match legacy_weights {
                Some((weights, channels)) if !self.request.kind.channelized() => {
                    // Old freqAveCubes averages the row's channels before
                    // it contributes its scalar weight to the time average.
                    weights.map(|weight| weight / channels as f32)
                }
                Some((weights, _)) => legacy_bin_counts.as_ref().map_or(weights, |counts| {
                    weights.map(|weight| weight / counts[slot] as f32)
                }),
                None => order.map(|corr| {
                    if flags[[corr, channel]] {
                        0.0
                    } else {
                        samples.weights[[corr, channel]]
                    }
                }),
            };
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
            } else if self.request.kind.cross_hand()
                && self.request.kind != LeakageSolveKind::XMueller
            {
                for (corr, model) in model.into_iter().enumerate() {
                    let amplitude = model.norm();
                    if amplitude > 0.0 {
                        let normalized = super::super::kernel::complex32_to_64(data[corr])
                            / super::super::kernel::complex32_to_64(model);
                        data[corr] = Complex32::new(normalized.re as f32, normalized.im as f32);
                        if !self.legacy_cross_hand {
                            weights[corr] *= amplitude * amplitude;
                        }
                    } else if self.legacy_cross_hand {
                        data[corr] = Complex32::ZERO;
                    }
                }
            }
            bins[slot].observe(data, model, weights, unflagged, retain_unweighted_model);
            total += weights
                .into_iter()
                .filter(|weight| weight.is_finite() && *weight > 0.0)
                .map(f64::from)
                .sum::<f64>();
        }
        Ok(RowAveragingWeights {
            spectrum: total,
            scalar: scalar_weight,
            legacy_xy: legacy_weights
                .filter(|_| legacy_xy)
                .map(|(weights, _)| weights),
        })
    }

    fn cross_hand_centroid_weight(
        &self,
        samples: SolveSamples<'_>,
        flags: &ArrayD<bool>,
        model: &ArrayD<Complex32>,
        order: [usize; 4],
        window: &WindowChannels,
    ) -> f64 {
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
                            (model[[order[0], channel]] + model[[order[3], channel]]) * 0.5
                        } else {
                            model[[corr, channel]]
                        }
                    },
                    true,
                )
            })
            .filter(|weight| weight.is_finite() && *weight > 0.0)
            .sum()
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
    unflagged: u8,
}

impl CoherencyMean {
    fn observe(
        &mut self,
        data: [Complex32; 4],
        model: [Complex32; 4],
        weights: [f32; 4],
        unflagged: u8,
        retain_unweighted_model: bool,
    ) {
        self.unflagged |= unflagged;
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
