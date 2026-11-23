// SPDX-License-Identifier: LGPL-3.0-or-later
//! Interferometric phase, delay, rate and dispersion fitting.

mod correlations;
mod fit;
mod multiband;
mod reference;
mod search;

use std::collections::{BTreeMap, BTreeSet};
use std::f64::consts::TAU;

use casa_ms::MeasurementSet;
use casa_types::{Complex32, Complex64};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use self::fit::FitOptions;
use self::multiband::MultibandSearch;
use self::search::{FringePeak, FringeSearch, GridSpec};
use super::grouping::{
    PriorCalibrationMode, SelectedSolveRow, SolveAccumulator, SolveGroupOptions, SolveInputRows,
    build_solve_groups, correlation_types_for_ddid, selected_channels,
};
use super::kernel::SolutionRow;
use super::writer::{combined_window_frequency, write_new_style_gain_caltable};
use super::{
    GainSolveError, GainSolveMode, GainSolveModelSource, GainSolveReport, GainSolveRequest,
    GainType, SolvedGains,
};
use crate::CalibrationOutputMode;
use crate::session::prepare_calibration_solve;
use crate::solve_stats::{IntervalStages, SolveStatsBuilder, ToolSolveActivity};

/// Search and refinement controls for the VLBI fringe solver.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FringeControls {
    /// Restrict the FFT delay search in nanoseconds.
    pub delay_window: [f64; 2],
    /// Restrict the FFT rate search in seconds per second.
    pub rate_window: [f64; 2],
    /// Refine the reference-baseline estimates using all connected baselines.
    pub global_solve: bool,
    /// Maximum global refinement iterations.
    pub iterations: usize,
    /// Enable delay, rate and dispersion parameters, respectively.
    pub active_parameters: [bool; 3],
    /// Store zero rates after correcting phases to the calibration reference.
    pub zero_rates: bool,
    /// Optional combination of the two parallel-hand correlations.
    pub correlation_combination: FringeCorrelation,
    /// Place selected spectral windows on one FFT grid before searching.
    pub concatenate_windows: bool,
}

/// How a fringe fit uses the two parallel-hand visibility products.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum FringeCorrelation {
    /// Solve the two hands independently.
    Separate,
    /// Form Stokes I from two available hands with propagated variances.
    Stokes,
    /// Average available hands with their inverse variance weights.
    Parallel,
}

pub(crate) struct ToolOptions<'a> {
    pub(crate) active_phase: bool,
    pub(crate) legacy_iterator: bool,
    pub(crate) activity: Option<&'a mut ToolSolveActivity>,
}

#[derive(Clone, Copy)]
struct GroupFit {
    controls: FringeControls,
    solve_mode: GainSolveMode,
    active_phase: bool,
}

pub(crate) fn solve(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    controls: FringeControls,
    mode: CalibrationOutputMode,
    tool: Option<ToolOptions<'_>>,
) -> Result<GainSolveReport, GainSolveError> {
    let input = crate::timing::Phase::start("fringe_select_read");
    super::validate_solve_interval(request.solve_interval)?;
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &request.refant,
        &request.prior_calibration_tables,
        request.parang,
        true,
        PriorCalibrationMode::Delay {
            share_input_flags: !request.correlation_dependent_flags,
            rotate_model: request.parang,
        },
    )?;
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &BTreeMap::new(),
        SolveGroupOptions {
            input_rows: tool.as_ref().map_or(SolveInputRows::Fringe, |tool| {
                SolveInputRows::FringeTool {
                    legacy: tool.legacy_iterator,
                }
            }),
            gain_type: GainType::K,
            model_source: request.model_source,
            point_stokes: if request.model_source == GainSolveModelSource::PointSource {
                request.smodel
            } else {
                [1., 0., 0., 0.]
            },
            rotate_model: request.parang,
            solve_interval: request.solve_interval,
            combine: request.combine,
            preaverage_max_span: Some(0.),
            correlation_dependent_flags: request.correlation_dependent_flags
                || context.preapplied_rows.is_some(),
            robust_seed: false,
            legacy_g_receptors: false,
            channelized: true,
            polynomial_bandpass: false,
        },
    )?;
    let windows = windows(ms, &context.rows)?;
    let mut solved = SolvedGains {
        rows: Vec::new(),
        refant_id: context.refant_id,
        attempted_spw_ids: BTreeSet::new(),
        channel_selections: context
            .rows
            .iter()
            .filter_map(|row| {
                row.channel_selection
                    .as_ref()
                    .map(|channels| (row.data_spw_id, channels.clone()))
            })
            .collect(),
        selected_data: context.selected_data,
        solve_stats: Default::default(),
    };
    let references =
        std::iter::once(context.refant_id).chain(context.refant_alternates.iter().copied());
    if tool.is_none() && context.refant_id < 0 {
        return Err(error("No valid reference antenna supplied"));
    }
    let mut stats = if tool.is_some() {
        Some(SolveStatsBuilder::new(
            8,
            super::spectral_window_ids(ms)?,
            &context.available_antennas,
        ))
    } else {
        None
    };
    let fit = GroupFit {
        controls,
        solve_mode: tool
            .as_ref()
            .map_or(GainSolveMode::AmplitudePhase, |_| request.solve_mode),
        active_phase: tool.as_ref().is_none_or(|tool| tool.active_phase),
    };
    let combined_frequency = (request.combine.spectral_windows
        && solved.selected_data.spectral_windows.len() > 1)
        .then(|| combined_window_frequency(ms, &solved))
        .transpose()?;
    drop(input);
    let fitting = crate::timing::Phase::start("fringe_fit");
    for group in groups.into_values() {
        if let Some(stats) = &mut stats {
            let legacy = tool.as_ref().is_some_and(|tool| tool.legacy_iterator);
            let mut stages = IntervalStages::of(&group, 0);
            let parallel_data = stages.with_data.iter().any(|antennas| !antennas.is_empty());
            if !legacy && controls.correlation_combination != FringeCorrelation::Separate {
                // Both statistic slots describe the original parallel-hand
                // coverage after the correlation layer changes the weights.
                if let [first, second] = stages.with_data.as_mut_slice() {
                    first.append(second);
                    second.clone_from(first);
                }
                stages.attempted_interval = Some(match controls.correlation_combination {
                    FringeCorrelation::Stokes => correlations::has_stokes_data(&group, &windows),
                    _ => parallel_data,
                });
            }
            let attempted = stages.attempted_interval.unwrap_or(false);
            stats.interval(&stages);
            if !attempted {
                continue;
            }
            if legacy {
                return Err(error("Attempt to call un-implemented selfSolveOne()"));
            }
            if context.refant_id < 0 || !parallel_data {
                return Err(error("No valid reference antenna supplied"));
            }
            if controls.global_solve
                && !fit.active_phase
                && !controls.active_parameters.contains(&true)
            {
                return Err(error("No parameters specified!"));
            }
        }
        let antennas = &context.available_antennas;
        let rows = solve_group(
            &group,
            &windows,
            antennas,
            references.clone(),
            combined_frequency,
            request.min_snr,
            fit,
        )?;
        if !rows.is_empty() {
            solved.attempted_spw_ids.insert(group.spw_id);
            solved.rows.extend(rows);
            if let Some(stats) = &mut stats {
                stats.record_success(group.spw_id, true);
            }
        }
    }
    reference::connect_intervals(
        ms,
        &mut solved.rows,
        context.available_antennas.len(),
        references,
        matches!(
            request.refant,
            super::RefAntSelector::Expression { strict: true, .. }
        ),
    )?;
    if let Some(stats) = stats {
        stats.track_activity(tool.and_then(|tool| tool.activity));
        solved.solve_stats = stats.finish();
    }
    drop(fitting);
    write_new_style_gain_caltable(ms, request, &solved, mode, "Fringe Jones")
}

struct Window {
    spw: i32,
    selected_hz: Vec<f64>,
    spacing_hz: f64,
    edges_hz: [f32; 2],
    single_hand: bool,
}

fn windows(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
) -> Result<BTreeMap<i32, Window>, GainSolveError> {
    let spectral = ms.spectral_window().map_err(error)?;
    let mut windows = BTreeMap::new();
    for row in rows {
        let std::collections::btree_map::Entry::Vacant(entry) = windows.entry(row.data_desc_id)
        else {
            continue;
        };
        let hz = spectral
            .chan_freq(row.data_spw_id as usize)
            .map_err(error)?;
        let channels = selected_channels(row.channel_selection.as_ref(), hz.len())?;
        let selected_hz = channels
            .iter()
            .map(|&channel| hz[channel])
            .collect::<Vec<_>>();
        let edges = frequency_edges(&selected_hz);
        let last = selected_hz
            .len()
            .checked_sub(1)
            .ok_or_else(|| error("empty spectral window"))?;
        let spacing = if last > 0 {
            (selected_hz[last] - selected_hz[0]) / last as f64
        } else {
            0.
        };
        entry.insert(Window {
            spw: row.data_spw_id,
            selected_hz,
            spacing_hz: spacing,
            edges_hz: edges.map(|hz| hz as f32),
            single_hand: correlation_types_for_ddid(ms, row.data_desc_id)?.len() == 1,
        });
    }
    Ok(windows)
}

fn frequency_edges(hz: &[f64]) -> [f64; 2] {
    hz.iter()
        .copied()
        .fold([f64::INFINITY, f64::NEG_INFINITY], |[low, high], freq| {
            [low.min(freq), high.max(freq)]
        })
}

struct GroupAxis {
    grid: GridSpec,
    first_time: f64,
    first_frequency: f64,
    table_frequency: f64,
    grid_start_hz: f64,
}

fn solve_group(
    group: &SolveAccumulator,
    all_windows: &BTreeMap<i32, Window>,
    antennas: &BTreeSet<i32>,
    mut references: impl Iterator<Item = i32>,
    combined_frequency: Option<f64>,
    min_snr: f32,
    fit: GroupFit,
) -> Result<Vec<SolutionRow>, GainSolveError> {
    let GroupFit {
        controls,
        solve_mode,
        active_phase,
    } = fit;
    if !group.delay_spectra().any(|spectrum| {
        spectrum.parallel_hand && spectrum.channels().any(|(_, weight)| weight > 0.)
    }) {
        return Ok(Vec::new());
    }
    let descriptions = group
        .fringe_buffers
        .iter()
        .map(|&(description, _, _)| description)
        .collect::<BTreeSet<_>>();
    let axis = axis(group, all_windows, &descriptions, combined_frequency)?;
    let mut hands = samples(group, all_windows, &axis);
    let single_hand = descriptions
        .iter()
        .all(|description| all_windows[description].single_hand);
    let combined = controls.correlation_combination != FringeCorrelation::Separate;
    if combined && !single_hand {
        hands = [
            correlations::combine(hands, controls.correlation_combination),
            Vec::new(),
        ];
    }
    enforce_visibility_mode(&mut hands, solve_mode);
    if hands.iter().all(Vec::is_empty) {
        return Ok(Vec::new());
    }
    let reference_time = reference_time(&hands, axis.first_time);
    let Some(reference) = references.find_map(|antenna| {
        let antenna = usize::try_from(antenna).ok()?;
        hands
            .iter()
            .flatten()
            .any(|baseline| baseline.antennas.contains(&antenna))
            .then_some(antenna)
    }) else {
        return Err(error("No valid reference antenna supplied"));
    };
    let size = antennas.last().map_or(0, |&id| id as usize + 1);
    let mut search = if controls.concatenate_windows {
        GroupSearch::Concatenated(FringeSearch::new(axis.grid)?)
    } else {
        GroupSearch::Separate(MultibandSearch::new(
            axis.grid,
            descriptions.iter().map(|id| (*id, &all_windows[id])),
        )?)
    };
    let mut estimates = [Vec::new(), Vec::new()];
    let mut below_threshold = BTreeSet::new();
    let hands_present = if combined || single_hand { 1 } else { 2 };
    for (hand, (baselines, estimates)) in hands.iter().zip(&mut estimates).enumerate() {
        estimates.resize_with(size, || None);
        if hand >= hands_present {
            continue;
        }
        estimates[reference] = Some(FringePeak {
            parameters: FringeParameters::default(),
            snr: 999.,
            accepted: true,
            active: true,
        });
        for baseline in baselines
            .iter()
            .filter(|baseline| baseline.antennas.contains(&reference))
        {
            let [first, second] = baseline.antennas;
            let (antenna, sign) = if first == reference {
                (second, -1.)
            } else {
                (first, 1.)
            };
            estimates[antenna] = search
                .peak(&baseline.samples, sign, controls)?
                .map(|mut peak| {
                    if peak.snr < min_snr {
                        peak.accepted = false;
                        below_threshold.insert(antenna);
                    }
                    peak.active = !below_threshold.contains(&antenna);
                    peak
                });
        }
        let options = FitOptions {
            reference_antenna: reference,
            first_time_seconds: axis.first_time,
            first_frequency_hz: axis.first_frequency,
            active_parameters: [
                active_phase,
                controls.active_parameters[0],
                controls.active_parameters[1],
                controls.active_parameters[2],
            ],
            iterations: controls.iterations,
        };
        // CASA only transcribes a global fit when phase occupies a slot.
        if controls.global_solve
            && active_phase
            && fit::refine(baselines, estimates, &options).is_none()
        {
            for (antenna, estimate) in estimates.iter_mut().enumerate() {
                if antenna != reference
                    && let Some(estimate) = estimate
                    && estimate.active
                {
                    estimate.accepted = false;
                }
            }
        }
        for estimate in estimates.iter_mut().flatten() {
            let p = &mut estimate.parameters;
            if estimate.active {
                p.phase_rad += TAU
                    * (p.delay_ns * (axis.table_frequency - axis.first_frequency) * 1e-9
                        + p.rate * axis.table_frequency * (reference_time - axis.first_time));
            }
            if controls.zero_rates {
                p.rate = 0.;
            }
        }
    }
    if combined {
        estimates[1] = estimates[0].clone();
    }
    Ok(antennas
        .iter()
        .map(|&antenna| make_row(antenna, reference, group, &estimates, reference_time, fit))
        .collect())
}

fn enforce_visibility_mode(hands: &mut [Vec<FringeBaseline>; 2], mode: GainSolveMode) {
    if mode == GainSolveMode::AmplitudePhase {
        return;
    }
    for sample in hands
        .iter_mut()
        .flatten()
        .flat_map(|baseline| &mut baseline.samples)
    {
        let amplitude = sample.visibility.norm();
        if mode == GainSolveMode::Phase && amplitude > 0. {
            sample.visibility /= amplitude;
            sample.weight *= amplitude.powi(2);
        } else if mode == GainSolveMode::Amplitude && amplitude > 0. {
            // CASA divides by the rounded single-precision phasor using a
            // widened complex division. The remaining phase enters its SNR.
            let phase = sample.visibility / amplitude;
            let widen = |value: Complex32| Complex64::new(f64::from(value.re), f64::from(value.im));
            let corrected = widen(sample.visibility) / widen(phase);
            sample.visibility = Complex32::new(corrected.re as f32, corrected.im as f32);
        }
    }
}

enum GroupSearch {
    Concatenated(FringeSearch),
    Separate(MultibandSearch),
}

impl GroupSearch {
    fn peak(
        &mut self,
        samples: &[FringeSample],
        sign: f64,
        controls: FringeControls,
    ) -> Result<Option<FringePeak>, GainSolveError> {
        match self {
            Self::Concatenated(search) => {
                search.peak(samples, sign, controls.delay_window, controls.rate_window)
            }
            Self::Separate(search) => search.peak(samples, sign),
        }
    }
}

fn axis(
    group: &SolveAccumulator,
    all_windows: &BTreeMap<i32, Window>,
    descriptions: &BTreeSet<i32>,
    combined_frequency: Option<f64>,
) -> Result<GroupAxis, GainSolveError> {
    let mut times = group.integration_times().collect::<Vec<_>>();
    times.sort_unstable_by(f64::total_cmp);
    if times.len() < 2 {
        return Err(error("Can't do a 2-dimensional FFT on a single timestep"));
    }
    let windows = descriptions
        .iter()
        .map(|description| &all_windows[description]);
    let all_hz = windows
        .clone()
        .flat_map(|window| &window.selected_hz)
        .copied()
        .collect::<Vec<_>>();
    let [low, high] = frequency_edges(&all_hz);
    let nominal_spacing = windows
        .clone()
        .filter_map(|window| (window.spacing_hz != 0.).then_some(window.spacing_hz))
        .next_back()
        .unwrap_or_else(|| {
            ((high - low) / descriptions.len().saturating_sub(1).max(1) as f64).max(1.)
        });
    let start_hz = if nominal_spacing < 0. { high } else { low };
    let intervals = ((high - low) / nominal_spacing.abs()).round() as usize;
    let channels = intervals
        .checked_add(1)
        .ok_or_else(|| error("fringe FFT dimensions overflow"))?;
    let spacing = if intervals > 0 {
        ((high - low) / intervals as f64).copysign(nominal_spacing)
    } else {
        nominal_spacing
    };
    let (frequency_sum, frequency_count) = group
        .fringe_buffers
        .iter()
        .flat_map(|(description, _, _)| &all_windows[description].selected_hz)
        .fold((0., 0), |(sum, count), frequency| {
            (sum + frequency, count + 1)
        });
    let grid = GridSpec {
        channels,
        timestamps: times.len(),
        channel_spacing_hz: spacing,
        integration_seconds: (times[times.len() - 1] - times[0]) / (times.len() - 1) as f64,
        center_hz: frequency_sum / frequency_count as f64,
        padding: (8 / descriptions.len()).max(2),
    };
    Ok(GroupAxis {
        grid,
        first_time: times[0],
        first_frequency: all_hz[0],
        table_frequency: combined_frequency
            .unwrap_or_else(|| all_hz.iter().sum::<f64>() / all_hz.len() as f64),
        grid_start_hz: start_hz,
    })
}

fn reference_time(hands: &[Vec<FringeBaseline>; 2], origin: f64) -> f64 {
    let (offset, total) = hands
        .iter()
        .flatten()
        .flat_map(|baseline| &baseline.samples)
        .fold((0., 0.), |(offset, total), sample| {
            let weight = f64::from(sample.weight);
            (
                offset + weight * (sample.centroid_seconds - origin),
                total + weight,
            )
        });
    if total > 0. {
        origin + offset / total
    } else {
        origin
    }
}

fn samples(
    group: &SolveAccumulator,
    windows: &BTreeMap<i32, Window>,
    axis: &GroupAxis,
) -> [Vec<FringeBaseline>; 2] {
    let mut hands = [BTreeMap::<[usize; 2], Vec<_>>::new(), BTreeMap::new()];
    for spectrum in group
        .delay_spectra()
        .filter(|spectrum| spectrum.parallel_hand)
    {
        let window = &windows[&spectrum.buffer.0];
        let hand = if window.single_hand {
            0
        } else {
            spectrum.receptor
        };
        let time_seconds = f64::from_bits(spectrum.buffer.2);
        let integration =
            ((time_seconds - axis.first_time) / axis.grid.integration_seconds).round() as usize;
        let antennas = [spectrum.antennas.0 as usize, spectrum.antennas.1 as usize];
        let baseline = hands[hand].entry(antennas).or_default();
        // Separate channel selections are delivered as one packed buffer.
        // CASA aligns the buffer's first frequency on the concatenated grid.
        let reversed = window.spacing_hz != 0.
            && window.spacing_hz.is_sign_negative()
                != axis.grid.channel_spacing_hz.is_sign_negative();
        let last = window.selected_hz.len() - 1;
        let start = window.selected_hz[if reversed { last } else { 0 }];
        let grid_start =
            ((start - axis.grid_start_hz) / axis.grid.channel_spacing_hz).round() as usize;
        for (local_channel, ((sum, weight), &frequency_hz)) in
            spectrum.channels().zip(&window.selected_hz).enumerate()
        {
            let (visibility, weight) = if weight.is_finite() && weight > 0. && sum.is_finite() {
                (sum / weight, weight)
            } else {
                (Complex32::default(), 0.)
            };
            let offset = if reversed {
                last - local_channel
            } else {
                local_channel
            };
            let channel = grid_start
                .saturating_add(offset)
                .min(axis.grid.channels - 1);
            baseline.push(FringeSample {
                integration,
                channel,
                local_channel,
                data_description: spectrum.buffer.0,
                field_id: spectrum.buffer.1,
                time_seconds,
                centroid_seconds: spectrum.time_seconds,
                frequency_hz,
                window_edges_hz: window.edges_hz,
                visibility,
                weight,
            });
        }
    }
    hands.map(|baselines| {
        baselines
            .into_iter()
            .filter_map(|(antennas, mut samples)| {
                if !samples.iter().any(|sample| sample.weight > 0.) {
                    return None;
                }
                // Later buffers replace earlier cells when rounded integration
                // times or overlapping windows land on the same FFT bin.
                samples.sort_unstable_by_key(FringeSample::buffer_channel_key);
                Some(FringeBaseline { antennas, samples })
            })
            .collect()
    })
}

fn make_row(
    antenna: i32,
    reference: usize,
    group: &SolveAccumulator,
    estimates: &[Vec<Option<FringePeak>>; 2],
    time_seconds: f64,
    fit: GroupFit,
) -> SolutionRow {
    let controls = fit.controls;
    let mut row = SolutionRow {
        time_seconds,
        field_id: group.field_id,
        spw_id: group.spw_id,
        antenna_id: antenna,
        scan_number: group.scan_number(),
        observation_id: group.observation_id,
        refant_id: -1,
        gains: Vec::with_capacity(8),
        flags: Vec::with_capacity(8),
        param_errors: vec![1.; 8],
        snrs: Vec::with_capacity(8),
        weights: Vec::new(),
    };
    for hand in estimates {
        let peak = hand[antenna as usize].as_ref();
        let parameters = peak.map_or_else(FringeParameters::default, |peak| peak.parameters);
        row.gains.extend(
            parameters
                .values()
                .map(|value| Complex32::new(value as f32, 0.)),
        );
        row.flags
            .extend([peak.is_none_or(|peak| !peak.accepted); 4]);
        let active = [
            true,
            controls.active_parameters[0],
            controls.active_parameters[1],
            controls.active_parameters[2],
        ];
        row.snrs.extend(active.map(|active| {
            if !controls.global_solve
                || !fit.active_phase
                || active
                || antenna as usize == reference
                || peak.is_some_and(|peak| !peak.active)
            {
                peak.map_or(0., |peak| peak.snr)
            } else {
                0.
            }
        }));
    }
    row
}

fn error(error: impl ToString) -> GainSolveError {
    GainSolveError::Delay(error.to_string())
}

#[derive(Clone, Copy, Default)]
struct FringeParameters {
    phase_rad: f64,
    delay_ns: f64,
    rate: f64,
    dispersion: f64,
}

impl FringeParameters {
    fn values(self) -> [f64; 4] {
        [self.phase_rad, self.delay_ns, self.rate, self.dispersion]
    }

    fn from_values([phase_rad, delay_ns, rate, dispersion]: [f64; 4]) -> Self {
        Self {
            phase_rad,
            delay_ns,
            rate,
            dispersion,
        }
    }
}

#[derive(Clone, Copy)]
struct FringeSample {
    integration: usize,
    channel: usize,
    local_channel: usize,
    data_description: i32,
    field_id: i32,
    time_seconds: f64,
    centroid_seconds: f64,
    frequency_hz: f64,
    window_edges_hz: [f32; 2],
    visibility: Complex32,
    weight: f32,
}

impl FringeSample {
    fn buffer_channel_key(&self) -> (i32, i32, u64, usize) {
        (
            self.data_description,
            self.field_id,
            self.time_seconds.to_bits(),
            self.local_channel,
        )
    }

    fn weighted_unit_visibility(self) -> Complex32 {
        let amplitude = self.visibility.norm();
        let divisor = if amplitude < f32::EPSILON {
            1.
        } else {
            amplitude
        };
        (self.visibility / divisor) * self.weight
    }
}

struct FringeBaseline {
    antennas: [usize; 2],
    samples: Vec<FringeSample>,
}
