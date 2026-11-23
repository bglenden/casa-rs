// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antenna and cross-hand delays from the peak of a visibility spectrum.

mod multiband;

use std::collections::{BTreeMap, BTreeSet};

use casa_fft::Fft2;
use casa_ms::MeasurementSet;
use casa_types::Complex32;

use super::grouping::{
    DelaySpectrum, PriorCalibrationMode, SelectedSolveRow, SolveAccumulator, SolveGroupOptions,
    SolveInputRows, build_solve_groups, correlation_types_for_ddid, selected_channels,
};
use super::kernel::SolutionRow;
use super::{
    GainSolveError, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
    GainType, RefAntSelector, SolvedGains, spectral_window_ids, validate_smodel,
    validate_solve_interval,
};
use crate::session::prepare_calibration_solve;
use crate::solve_stats::{IntervalStages, SolveStatsBuilder, ToolSolveActivity};

pub(super) fn solve(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    mut activity: Option<&mut ToolSolveActivity>,
    legacy_iterator: bool,
) -> Result<SolvedGains, GainSolveError> {
    if matches!(request.refant, RefAntSelector::Unspecified) {
        return Err(delay_error("specify a reference antenna explicitly"));
    }
    validate_solve_interval(request.solve_interval)?;
    if request.model_source == GainSolveModelSource::PointSource {
        validate_smodel(request.smodel, true)?;
    }
    let cross_hand = request.gain_type == GainType::KCross;
    let antenna_position = request.gain_type == GainType::KAntPos;
    let legacy_multiband = legacy_iterator && request.gain_type == GainType::KMbd;
    let input_phase = crate::timing::Phase::start("gain_select_read_average");
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
    let windows = delay_windows(ms, &context.rows, cross_hand)?;
    let combined_output_spw = (legacy_iterator && request.combine.spectral_windows)
        .then(|| context.rows.iter().map(|row| row.data_spw_id).min())
        .flatten();
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &BTreeMap::new(),
        SolveGroupOptions {
            input_rows: if legacy_multiband {
                SolveInputRows::LegacyDelay
            } else {
                SolveInputRows::default()
            },
            gain_type: request.gain_type,
            model_source: request.model_source,
            point_stokes: if request.model_source == GainSolveModelSource::PointSource {
                request.smodel
            } else {
                [1., 0., 0., 0.]
            },
            rotate_model: request.parang,
            solve_interval: request.solve_interval,
            combine: request.combine,
            preaverage_max_span: averaging_span(request),
            // Prior evaluation has already combined raw input flags when
            // needed. New flags from priors stay per correlation.
            correlation_dependent_flags: request.correlation_dependent_flags
                || context.preapplied_rows.is_some(),
            robust_seed: false,
            channelized: true,
            polynomial_bandpass: false,
        },
    )?;
    if groups.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }
    drop(input_phase);
    let _kernel_phase = crate::timing::Phase::start("gain_solve");
    let selections = context
        .rows
        .iter()
        .filter_map(|row| {
            row.channel_selection
                .as_ref()
                .map(|selection| (row.data_spw_id, selection.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut transforms = BTreeMap::new();
    let mut rows = Vec::new();
    let mut attempted_spw_ids = BTreeSet::new();
    let parameters = request.gain_type.polarizations();
    let window_ids = spectral_window_ids(ms)?;
    if combined_output_spw.is_some()
        && let Some(activity) = activity.as_deref_mut()
    {
        activity.initialize(window_ids.len());
    }
    let mut stats = SolveStatsBuilder::new(parameters, window_ids, &context.available_antennas);
    for mut group in groups.into_values() {
        group.finalize_for_solve(request.solve_mode, false);
        let mut stages = IntervalStages::of(&group, 0);
        if cross_hand || antenna_position {
            // CASA reports data availability for the full output parameter
            // shape and bypasses the iterative solver's two thresholds.
            stages.with_data.resize(
                parameters,
                stages.with_data.first().cloned().unwrap_or_default(),
            );
        }
        stages.constrained.clear();
        stats.interval(&stages);
        if let Some(output_spw) = combined_output_spw
            && let Some(activity) = activity.as_deref_mut()
        {
            activity.count_mapped_interval(
                group.spw_id,
                output_spw,
                group.attempted,
                group.attempted,
            );
        }
        if !group.attempted {
            continue;
        }
        let output_spw = combined_output_spw.unwrap_or(group.spw_id);
        let solutions = group_delays(
            &group,
            request,
            context.refant_id,
            &windows,
            &mut transforms,
            legacy_multiband,
        )?;
        attempted_spw_ids.insert(output_spw);
        stats.record_success(output_spw, true);
        rows.extend(context.available_antennas.iter().map(|&antenna| {
            let delays = if cross_hand {
                solutions.get(&0).copied().unwrap_or_default()
            } else {
                solutions.get(&antenna).copied().unwrap_or_default()
            };
            let mut gains = Vec::with_capacity(parameters);
            let mut flags = Vec::with_capacity(parameters);
            for delay in delays {
                gains.push(Complex32::new(delay.unwrap_or_default(), 0.));
                flags.push(delay.is_none());
            }
            if antenna_position {
                // The inherited K solver fits the two parallel hands.
                // Its reference row defines the unused third term as zero.
                gains.push(Complex32::ZERO);
                flags.push(antenna != context.refant_id);
            }
            SolutionRow {
                time_seconds: group.solution_time,
                field_id: group.field_id,
                spw_id: output_spw,
                antenna_id: antenna,
                scan_number: group.scan_number(),
                observation_id: group.observation_id,
                refant_id: -1,
                gains,
                flags,
                param_errors: vec![0.; parameters],
                snrs: vec![0.; parameters],
                weights: Vec::new(),
            }
        }));
    }
    let tool_solve = activity.is_some();
    if combined_output_spw.is_none() {
        stats.track_activity(activity);
    }
    let mut solve_stats = stats.finish();
    if !cross_hand && !tool_solve {
        // task_gaincal.resultsDictGaintypeExcept copies the unflagged
        // counts at each level, regardless of the actual solved delays.
        for window in &mut solve_stats.windows {
            for counts in std::iter::once(&mut window.counts).chain(
                window
                    .antennas
                    .iter_mut()
                    .map(|antenna| &mut antenna.counts),
            ) {
                counts.above_minblperant.clone_from(&counts.data_unflagged);
                counts.above_minsnr.clone_from(&counts.data_unflagged);
            }
        }
    }
    Ok(SolvedGains {
        rows,
        refant_id: context.refant_id,
        attempted_spw_ids,
        channel_selections: selections,
        selected_data: context.selected_data,
        solve_stats,
    })
}

#[derive(Clone, Copy)]
struct DelayWindow {
    start_ghz: f64,
    spacing_ghz: f64,
    lowest_ghz: f64,
    highest_ghz: f64,
    channels: usize,
}

fn delay_windows(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    cross_hand: bool,
) -> Result<BTreeMap<i32, DelayWindow>, GainSolveError> {
    let spectral_windows = ms.spectral_window().map_err(delay_error)?;
    let mut windows = BTreeMap::new();
    for row in rows {
        let std::collections::btree_map::Entry::Vacant(entry) = windows.entry(row.data_desc_id)
        else {
            continue;
        };
        if cross_hand && correlation_types_for_ddid(ms, row.data_desc_id)?.len() != 4 {
            return Err(delay_error("KCROSS requires four correlations"));
        }
        let frequencies = spectral_windows
            .chan_freq(row.data_spw_id as usize)
            .map_err(delay_error)?;
        let indices = selected_channels(row.channel_selection.as_ref(), frequencies.len())?;
        let selected = indices
            .into_iter()
            .map(|channel| frequencies[channel])
            .collect::<Vec<_>>();
        let &[start, next, ..] = selected.as_slice() else {
            return Err(delay_error("at least two selected channels are needed"));
        };
        let spacing_ghz = (next - start) * 1e-9;
        if spacing_ghz == 0. || !spacing_ghz.is_finite() {
            return Err(delay_error(
                "delay channels need a nonzero frequency spacing",
            ));
        }
        entry.insert(DelayWindow {
            start_ghz: start * 1e-9,
            spacing_ghz,
            lowest_ghz: selected.iter().copied().fold(f64::INFINITY, f64::min) * 1e-9,
            highest_ghz: selected.iter().copied().fold(f64::NEG_INFINITY, f64::max) * 1e-9,
            channels: selected.len(),
        });
    }
    Ok(windows)
}

type DelaySolutions = BTreeMap<i32, [Option<f32>; 2]>;

fn group_delays(
    group: &SolveAccumulator,
    request: &GainSolveRequest,
    reference: i32,
    windows: &BTreeMap<i32, DelayWindow>,
    transforms: &mut BTreeMap<i32, DelayTransform>,
    legacy: bool,
) -> Result<DelaySolutions, GainSolveError> {
    let mut buffers = BTreeMap::<_, Vec<DelaySpectrum<'_>>>::new();
    for spectrum in group.delay_spectra() {
        buffers.entry(spectrum.buffer).or_default().push(spectrum);
    }
    let cross_hand = request.gain_type == GainType::KCross;
    let mode = request.solve_mode;
    if buffers.len() > 1 {
        if request.gain_type == GainType::KAntPos {
            return Err(delay_error(
                "the KAntPos fit cannot combine multiple buffers in a solution interval",
            ));
        }
        return multiband::solve(
            buffers
                .into_iter()
                .map(|((dd, _, _), spectra)| (windows[&dd], spectra))
                .collect(),
            reference,
            mode,
            cross_hand,
            legacy,
        );
    }
    let Some((&(dd, _, _), spectra)) = buffers.first_key_value() else {
        return Ok(BTreeMap::new());
    };
    let transform = match transforms.entry(dd) {
        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(DelayTransform::new(windows[&dd])?)
        }
    };
    if cross_hand {
        group_cross_hand(spectra, mode, transform)
    } else {
        group_antenna(spectra, reference, mode, transform)
    }
}

fn averaging_span(request: &GainSolveRequest) -> Option<f64> {
    let preaverage = request.preaverage_seconds as f32;
    let preaverage = (preaverage > f32::EPSILON && preaverage.is_finite()).then_some(preaverage);
    let interval = match request.solve_interval {
        GainSolveInterval::Infinite => preaverage,
        GainSolveInterval::Integration => None,
        GainSolveInterval::Seconds(seconds) => {
            Some(preaverage.map_or(seconds as f32, |pre| pre.min(seconds as f32)))
        }
    };
    interval.map(|seconds| f64::from(seconds) * 0.999)
}

fn group_antenna(
    spectra: &[DelaySpectrum<'_>],
    reference: i32,
    mode: GainSolveMode,
    transform: &mut DelayTransform,
) -> Result<BTreeMap<i32, [Option<f32>; 2]>, GainSolveError> {
    let mut results = BTreeMap::from([(reference, [Some(0.); 2])]);
    for spectrum in spectra.iter().filter(|spectrum| spectrum.parallel_hand) {
        let ((first, second), receptor) = (spectrum.antennas, spectrum.receptor);
        let (antenna, sign) = match (first == reference, second == reference) {
            (true, false) => (second, -1.),
            (false, true) => (first, 1.),
            _ => continue,
        };
        let values = spectrum
            .channels()
            .map(|(value, weight)| constrain(value, weight, mode).0);
        if let Some(delay) = transform.peak(values, false)? {
            results.entry(antenna).or_default()[receptor] = Some(sign * delay);
        }
    }
    Ok(results)
}

fn group_cross_hand(
    spectra: &[DelaySpectrum<'_>],
    mode: GainSolveMode,
    transform: &mut DelayTransform,
) -> Result<BTreeMap<i32, [Option<f32>; 2]>, GainSolveError> {
    let mut sum = vec![(Complex32::default(), 0.); transform.channels];
    // CASA accumulates both hands of each averaged baseline consecutively.
    // For a rotated Stokes I model, their opposite signs can cancel exactly.
    let mut cross_hands = spectra
        .iter()
        .filter(|spectrum| !spectrum.parallel_hand)
        .collect::<Vec<_>>();
    cross_hands.sort_unstable_by_key(|spectrum| (spectrum.antennas, spectrum.correlation));
    for spectrum in cross_hands {
        for ((total, weights), (value, weight)) in sum.iter_mut().zip(spectrum.channels()) {
            let (value, weight) = constrain(value, weight, mode);
            let value = if spectrum.correlation == 1 {
                value
            } else {
                value.conj()
            };
            *total += weight * value;
            *weights += weight;
        }
    }
    let values = sum.into_iter().map(|(total, weights)| {
        if weights > 0. {
            total / weights
        } else {
            Complex32::default()
        }
    });
    let delay = transform.peak(values, true)?;
    Ok(BTreeMap::from([(0, [delay, delay.map(|_| 0.)])]))
}

fn constrain(value: Complex32, weight: f32, mode: GainSolveMode) -> (Complex32, f32) {
    if weight <= 0. {
        return (Complex32::default(), 0.);
    }
    let mean = value / weight;
    let amplitude = mean.norm();
    match mode {
        GainSolveMode::AmplitudePhase => (mean, weight),
        GainSolveMode::Amplitude => (Complex32::new(amplitude, 0.), weight),
        GainSolveMode::Phase if amplitude > 0. => {
            (mean / amplitude, weight * amplitude * amplitude)
        }
        GainSolveMode::Phase => (Complex32::default(), 0.),
    }
}

struct DelayTransform {
    channels: usize,
    spacing_ghz: f32,
    fft: Fft2<f32>,
    padded: Vec<Complex32>,
}

impl DelayTransform {
    fn new(window: DelayWindow) -> Result<Self, GainSolveError> {
        let size = window.channels * 8;
        Ok(Self {
            channels: window.channels,
            spacing_ghz: window.spacing_ghz as f32,
            fft: Fft2::with_threads([size, 1], 1)
                .map_err(delay_error)?
                .with_estimated_plan(),
            padded: vec![Complex32::default(); size],
        })
    }

    fn peak(
        &mut self,
        samples: impl Iterator<Item = Complex32>,
        cross_hand: bool,
    ) -> Result<Option<f32>, GainSolveError> {
        let size = self.padded.len();
        let center = size / 2;
        let offset = (size - self.channels) / 2;
        self.padded.fill(Complex32::default());
        for (out, sample) in self.padded[offset..offset + self.channels]
            .iter_mut()
            .zip(samples)
        {
            *out = sample;
        }
        self.fft
            .transform(&mut self.padded, false)
            .map_err(delay_error)?;
        let amp = |bin: usize| self.padded[(bin + center) % size].norm();
        // K requires two neighbors and nonzero curvature. KCROSS can use
        // either endpoint and retains bin zero even when the sum is empty.
        let edge = usize::from(!cross_hand);
        let mut peak = edge;
        let mut middle = 0.;
        for bin in edge..size - edge {
            let amplitude = amp(bin);
            if amplitude > middle {
                peak = bin;
                middle = amplitude;
            }
        }
        if !cross_hand && middle == 0. {
            return Ok(None);
        }
        let sub_bin = if peak > 0 && peak + 1 < size {
            let left = amp(peak - 1);
            let right = amp(peak + 1);
            let curvature = left - 2. * middle + right;
            if curvature == 0. && !cross_hand {
                return Ok(None);
            }
            0.5 - (right - middle) / curvature
        } else {
            0.
        };
        Ok(Some(
            (peak as f32 + sub_bin - center as f32) / size as f32 / self.spacing_ghz,
        ))
    }
}

fn delay_error(error: impl ToString) -> GainSolveError {
    GainSolveError::Delay(error.to_string())
}
