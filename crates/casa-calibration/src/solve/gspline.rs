// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time-domain antenna gain solving for the legacy GSPLINE family.

mod basis;
mod diagnostics;
mod writer;

use std::collections::{BTreeMap, BTreeSet};
use std::f64::consts::TAU;

use casa_ms::{MeasurementSet, MsError};
use casa_types::ArrayValue;
use num_complex::Complex64;

use self::basis::SampleBasis;
use super::grouping::{
    PriorCalibrationMode, SelectedSolveRow, SolveAccumulator, SolveGroupOptions,
    build_solve_groups, selected_channels,
};
use super::normal::{NormalBand, Term};
use super::{
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainSolveReport, GainSolveRequest, GainSplineControls, GainType,
};
use crate::CalibrationOutputMode;
use crate::execute::PreappliedRows;
use crate::session::prepare_calibration_solve;

#[derive(Clone, Copy)]
pub(crate) struct ToolOptions<'a> {
    pub(crate) mode: &'a str,
    pub(crate) all_fields: bool,
}

pub(crate) fn solve(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    controls: GainSplineControls,
    mode: CalibrationOutputMode,
    tool: Option<ToolOptions<'_>>,
) -> Result<GainSolveReport, GainSolveError> {
    let tool_mode = tool.map(|settings| settings.mode);
    let all_fields = tool.is_some_and(|settings| settings.all_fields);
    let input = crate::timing::Phase::start("gspline_select_average");
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &request.refant,
        &request.prior_calibration_tables,
        request.parang,
        true,
        PriorCalibrationMode::GainSpline,
    )?;
    let preaverage = if request.preaverage_seconds > 0. && request.preaverage_seconds.is_finite() {
        request.preaverage_seconds
    } else {
        0.1
    };
    let selection = SolveGroupOptions {
        input_rows: Default::default(),
        gain_type: GainType::GSpline,
        model_source: request.model_source,
        point_stokes: if request.model_source == GainSolveModelSource::PointSource {
            request.smodel
        } else {
            [1., 0., 0., 0.]
        },
        rotate_model: false,
        solve_interval: GainSolveInterval::Infinite,
        // GSPLINE's old iterator sorts by array, field and data description.
        combine: GainSolveCombine {
            scans: true,
            observations: true,
            ..GainSolveCombine::default()
        },
        preaverage_max_span: Some(preaverage),
        correlation_dependent_flags: false,
        robust_seed: false,
        legacy_g_receptors: false,
        channelized: false,
        polynomial_bandpass: false,
    };
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &BTreeMap::new(),
        selection,
    )?;
    if groups.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }
    let fields = if all_fields {
        let count = ms
            .field()
            .map_err(|source| GainSolveError::OpenMeasurementSet {
                path: ms
                    .path()
                    .map_or_else(|| "<in-memory>".into(), |path| path.display().to_string()),
                source,
            })?
            .row_count();
        (0..count as i32).collect()
    } else {
        context
            .rows
            .iter()
            .map(|row| row.field_id)
            .collect::<BTreeSet<_>>()
    };
    let windows = context
        .rows
        .iter()
        .map(|row| row.data_spw_id)
        .collect::<BTreeSet<_>>();
    let descriptions = context
        .rows
        .iter()
        .map(|row| row.data_desc_id)
        .collect::<BTreeSet<_>>();
    let correlations = correlation_counts(ms, descriptions)?;
    let hands = correlations
        .values()
        .copied()
        .max()
        .unwrap_or(1)
        .clamp(1, 2);
    let (times, series) = time_series(groups.values(), &correlations, hands);
    drop(input);
    let fit_phase = crate::timing::Phase::start("gspline_fit");
    let fit_amplitude = tool_mode.map_or(request.solve_mode != GainSolveMode::Phase, |mode| {
        mode.contains('A')
    });
    let fit_phase_component = tool_mode
        .map_or(request.solve_mode != GainSolveMode::Amplitude, |mode| {
            mode.contains('P')
        });
    let poly_mode = match tool_mode.unwrap_or(match request.solve_mode {
        GainSolveMode::Amplitude => "A",
        GainSolveMode::Phase => "P",
        GainSolveMode::AmplitudePhase => "AP",
    }) {
        "A" => "AMP",
        "P" => "PHAS",
        "AP" => "PHASAMP",
        mode => mode,
    };
    let coefficients = fit(
        &times,
        &series,
        context.available_antennas.len(),
        &SplineKernelOptions {
            amplitude: fit_amplitude,
            phase: fit_phase_component,
            hands,
            spacing_seconds: controls.splinetime,
            phase_points: controls.npointaver.max(2) as usize,
            phase_wrap_radians: controls.phasewrap.to_radians(),
            reference_antenna: context.refant_id.max(0) as usize,
        },
    );
    drop(fit_phase);
    let output = crate::timing::Phase::start("gspline_write");
    let solution_row_count = if let Some(coefficients) = &coefficients {
        let frequency = reference_frequency(ms, &context.rows, context.preapplied_rows.as_ref())?;
        let solved_antennas = series
            .iter()
            .filter(|baseline| baseline.samples.iter().flatten().copied().any(usable))
            .flat_map(|baseline| baseline.antennas)
            .collect::<BTreeSet<_>>();
        let count = writer::write(
            ms,
            request,
            coefficients,
            (mode, all_fields),
            &fields,
            (context.refant_id, &solved_antennas, frequency),
            poly_mode,
        )?;
        diagnostics::write_logs(&request.output_table, &times, &series, coefficients, hands);
        count
    } else {
        0
    };
    drop(output);
    Ok(GainSolveReport {
        output_table: request.output_table.clone(),
        gain_type: GainType::GSpline,
        refant_antenna_id: context.refant_id,
        field_ids: fields.into_iter().collect(),
        spectral_window_ids: windows.into_iter().collect(),
        solution_row_count,
        selected_data: context.selected_data,
        solve_stats: Default::default(),
    })
}

fn reference_frequency(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    preapplied: Option<&PreappliedRows>,
) -> Result<f64, GainSolveError> {
    let ms_error = |source| GainSolveError::OpenMeasurementSet {
        path: ms.path().map_or_else(
            || "<in-memory>".to_owned(),
            |path| path.display().to_string(),
        ),
        source,
    };
    let spectral = ms.spectral_window().map_err(ms_error)?;
    // The old accumulator takes coordinates from the first usable averaged
    // buffer within each array, field and data-description chunk.
    let mut chunks = BTreeMap::<_, Vec<&SelectedSolveRow>>::new();
    for row in rows {
        chunks
            .entry((row.array_id, row.field_id, row.data_desc_id))
            .or_default()
            .push(row);
    }
    let mut sum = 0.;
    let mut usable_chunks = 0;
    for chunk in chunks.values_mut() {
        let first = chunk[0];
        let frequencies = spectral
            .chan_freq(first.data_spw_id as usize)
            .map_err(ms_error)?;
        let channels = selected_channels(first.channel_selection.as_ref(), frequencies.len())?;
        if channels.len() == 1 {
            sum += frequencies[channels[0]];
            usable_chunks += 1;
            continue;
        }
        if !chunk.is_sorted_by(|left, right| left.time_seconds <= right.time_seconds) {
            chunk.sort_by(|left, right| left.time_seconds.total_cmp(&right.time_seconds));
        }
        for buffer in chunk.chunk_by(|left, right| left.time_seconds == right.time_seconds) {
            let mut buffer_sum = 0.;
            let mut count = 0;
            for row in buffer.iter().filter(|row| !row.flag_row) {
                let flags = if let Some(applied) =
                    preapplied.and_then(|rows| rows.data.get(&row.row_index))
                {
                    &applied.flags
                } else {
                    ms.main_table()
                        .cell_accessor(row.row_index, "FLAG")
                        .and_then(|cell| cell.array())
                        .map_err(|error| ms_error(MsError::from(error)))?
                };
                let ArrayValue::Bool(flags) = flags else {
                    return Err(ms_error(MsError::InvalidInput(
                        "FLAG is not a boolean array".into(),
                    )));
                };
                for &channel in &channels {
                    if !flags
                        .index_axis(ndarray::Axis(1), channel)
                        .iter()
                        .any(|flagged| *flagged)
                    {
                        buffer_sum += frequencies[channel];
                        count += 1;
                    }
                }
            }
            if count > 0 {
                sum += buffer_sum / count as f64;
                usable_chunks += 1;
                break;
            }
        }
    }
    Ok(sum / usable_chunks.max(1) as f64)
}

fn correlation_counts(
    ms: &MeasurementSet,
    ids: BTreeSet<i32>,
) -> Result<BTreeMap<i32, usize>, GainSolveError> {
    let read = || {
        let descriptions = ms.data_description()?;
        let polarization = ms.polarization()?;
        ids.into_iter()
            .map(|id| {
                let row = descriptions.polarization_id(id as usize)?;
                let count = polarization.num_corr(row as usize)?;
                Ok((id, count as usize))
            })
            .collect::<casa_ms::MsResult<BTreeMap<_, _>>>()
    };
    read().map_err(|source: MsError| GainSolveError::OpenMeasurementSet {
        path: ms.path().map_or_else(
            || "<in-memory>".to_owned(),
            |path| path.display().to_string(),
        ),
        source,
    })
}

fn time_series<'a>(
    groups: impl Iterator<Item = &'a SolveAccumulator>,
    correlations: &BTreeMap<i32, usize>,
    hands: usize,
) -> (Vec<f64>, Vec<BaselineSeries>) {
    let mut timestamps = BTreeMap::<i64, (usize, f64)>::new();
    let mut baselines = BTreeMap::<[usize; 2], BTreeMap<i64, [WeightedVisibility; 2]>>::new();
    for (chunk, group) in groups.enumerate() {
        for spectrum in group
            .delay_spectra()
            .filter(|spectrum| spectrum.parallel_hand && spectrum.antennas.0 != spectrum.antennas.1)
        {
            let Some((sum, weight)) = spectrum.channels().next() else {
                continue;
            };
            if weight <= 0. {
                continue;
            }
            let timestamp = (spectrum.time_seconds * 10.).round() as i64;
            let first = timestamps
                .entry(timestamp)
                .or_insert((chunk, spectrum.time_seconds));
            // CASA retains the first chronological sample of the first chunk
            // contributing to a tenth-second bin, even when later samples
            // happen to round to the same bin.
            if first.0 == chunk {
                first.1 = first.1.min(spectrum.time_seconds);
            }
            let [first, second] = [spectrum.antennas.0, spectrum.antennas.1].map(|id| id as usize);
            let baseline = baselines.entry([first, second]).or_default();
            let samples = baseline.entry(timestamp).or_default();
            let update = |sample: &mut WeightedVisibility| {
                sample.sum += Complex64::new(f64::from(sum.re), f64::from(sum.im));
                sample.weight += f64::from(weight);
            };
            if correlations.get(&spectrum.buffer.0) == Some(&1) {
                for sample in &mut samples[..hands] {
                    update(sample);
                }
            } else {
                update(&mut samples[spectrum.receptor]);
            }
        }
    }
    let times = timestamps
        .values_mut()
        .enumerate()
        .map(|(index, (slot, time))| {
            *slot = index;
            *time
        })
        .collect::<Vec<_>>();
    let series = baselines
        .into_iter()
        .map(|(antennas, by_time)| {
            let mut samples = vec![[WeightedVisibility::default(); 2]; times.len()];
            for (time, value) in by_time {
                samples[timestamps[&time].0] = value;
            }
            BaselineSeries { antennas, samples }
        })
        .collect();
    (times, series)
}

#[derive(Clone, Copy, Default)]
pub(super) struct WeightedVisibility {
    pub(super) sum: Complex64,
    pub(super) weight: f64,
}

impl WeightedVisibility {
    fn value(self, phase: bool) -> f64 {
        if phase {
            self.sum.arg()
        } else {
            let amplitude = self.sum.norm() / self.weight;
            if amplitude > 0. { amplitude.ln() } else { 0. }
        }
    }
}

pub(super) struct BaselineSeries {
    pub(super) antennas: [usize; 2],
    pub(super) samples: Vec<[WeightedVisibility; 2]>,
}

pub(super) struct SplineKernelOptions {
    pub(super) amplitude: bool,
    pub(super) phase: bool,
    pub(super) hands: usize,
    pub(super) spacing_seconds: f64,
    pub(super) phase_points: usize,
    pub(super) phase_wrap_radians: f64,
    pub(super) reference_antenna: usize,
}

pub(super) struct SplineCoefficients {
    pub(super) knots: Vec<f64>,
    /// Receptor, then antenna, then cubic spline control point.
    pub(super) log_amplitude: Option<[Vec<Vec<f64>>; 2]>,
    pub(super) phase: Option<[Vec<Vec<f64>>; 2]>,
}

pub(super) fn fit(
    times: &[f64],
    baselines: &[BaselineSeries],
    antenna_count: usize,
    options: &SplineKernelOptions,
) -> Option<SplineCoefficients> {
    if baselines.iter().any(|baseline| {
        baseline.samples.len() != times.len()
            || baseline
                .antennas
                .iter()
                .any(|&antenna| antenna >= antenna_count)
            || baseline.antennas[0] == baseline.antennas[1]
    }) {
        return None;
    }
    let knots = basis::knots(times, options.spacing_seconds)?;
    let basis = times
        .iter()
        .map(|&time| basis::sample(&knots, time))
        .collect::<Option<Vec<_>>>()?;
    let size = (antenna_count, knots.len() - 4);
    let log_amplitude = if !options.amplitude {
        None
    } else {
        Some(fit_component(&basis, baselines, size, options, false)?)
    };
    let phase = if !options.phase {
        None
    } else {
        Some(fit_component(&basis, baselines, size, options, true)?)
    };
    Some(SplineCoefficients {
        knots,
        log_amplitude,
        phase,
    })
}

fn fit_component(
    basis: &[SampleBasis],
    baselines: &[BaselineSeries],
    (antenna_count, controls): (usize, usize),
    options: &SplineKernelOptions,
    phase: bool,
) -> Option<[Vec<Vec<f64>>; 2]> {
    let fit_hand = |hand: usize| {
        let active = antenna_indices(
            baselines,
            antenna_count,
            hand,
            phase.then_some(options.reference_antenna),
        )?;
        let count = active.iter().flatten().count();
        let size = controls.checked_mul(count)?;
        let mut normal = NormalBand::new(size, count.checked_mul(4)?)?;
        let mut values = vec![0.; basis.len()];
        let mut phases = Vec::new();
        for (index, baseline) in baselines.iter().enumerate() {
            let [first, second] = baseline.antennas;
            if active[first].is_none() && active[second].is_none() {
                continue;
            }
            for (value, samples) in values.iter_mut().zip(&baseline.samples) {
                *value = if usable(samples[hand]) {
                    samples[hand].value(phase)
                } else {
                    0.
                };
            }
            if phase {
                unwrap_phases(
                    &mut values,
                    options.phase_points,
                    options.phase_wrap_radians,
                );
            }
            for ((sample, value), &basis) in baseline.samples.iter().zip(&values).zip(basis) {
                if !usable(sample[hand]) {
                    continue;
                }
                let terms = basis_terms(basis, [active[first], active[second]], count, phase);
                normal.accumulate(&terms.0[..terms.1], *value, sample[hand].weight);
            }
            if phase {
                phases.push((index, values.clone()));
            }
        }
        normal.factor()?;
        let mut solved = normal.solve()?.to_vec();
        // The temporal unwrap seeds the linear fit. Refine it with circular
        // residuals, so baselines displaced by whole turns constrain the same
        // antenna phases. The weighted design matrix stays fixed.
        for _ in 1..100 {
            if !phase
                || !refine_phase(&mut normal, &mut solved, &phases, |index, time| {
                    let baseline = &baselines[index];
                    let antennas = baseline.antennas.map(|antenna| active[antenna]);
                    let terms = basis_terms(basis[time], antennas, count, true);
                    (terms, baseline.samples[time][hand])
                })?
            {
                break;
            }
        }
        Some(
            active
                .into_iter()
                .map(|index| {
                    index.map_or_else(
                        || vec![0.; controls],
                        |index| solved.iter().skip(index).step_by(count).copied().collect(),
                    )
                })
                .collect(),
        )
    };
    let first = fit_hand(0)?;
    let second = if options.hands == 1 {
        vec![vec![0.; controls]; antenna_count]
    } else {
        fit_hand(1)?
    };
    Some([first, second])
}

fn refine_phase(
    normal: &mut NormalBand,
    solution: &mut [f64],
    phases: &[(usize, Vec<f64>)],
    sample_at: impl Fn(usize, usize) -> (([Term; 8], usize), WeightedVisibility),
) -> Option<bool> {
    normal.clear_rhs();
    for (baseline, values) in phases {
        for (time, measured) in values.iter().enumerate() {
            let ((terms, size), sample) = sample_at(*baseline, time);
            if !usable(sample) {
                continue;
            }
            let terms = &terms[..size];
            let predicted = terms
                .iter()
                .map(|term| term.coefficient * solution[term.index])
                .sum::<f64>();
            normal.add_rhs(terms, (measured - predicted).sin(), sample.weight);
        }
    }
    let step = normal.solve()?;
    for (control, correction) in solution.iter_mut().zip(step) {
        *control += correction;
    }
    Some(step.iter().map(|value| value * value).sum::<f64>() > 1e-14)
}

fn antenna_indices(
    baselines: &[BaselineSeries],
    antenna_count: usize,
    hand: usize,
    reference: Option<usize>,
) -> Option<Vec<Option<usize>>> {
    let mut active = vec![false; antenna_count];
    for baseline in baselines {
        if baseline.samples.iter().any(|sample| usable(sample[hand])) {
            for antenna in baseline.antennas {
                *active.get_mut(antenna)? = true;
            }
        }
    }
    if let Some(reference) = reference {
        let present = active.get_mut(reference)?;
        if !*present {
            return None;
        }
        *present = false;
    }
    if !active.iter().any(|&present| present) {
        return None;
    }
    let mut next = 0;
    Some(
        active
            .into_iter()
            .map(|present| {
                present.then(|| {
                    let index = next;
                    next += 1;
                    index
                })
            })
            .collect(),
    )
}

fn usable(value: WeightedVisibility) -> bool {
    value.weight.is_finite() && value.weight > 0. && value.sum.is_finite()
}

fn basis_terms(
    basis: SampleBasis,
    antennas: [Option<usize>; 2],
    count: usize,
    phase: bool,
) -> ([Term; 8], usize) {
    let mut terms = [Term {
        index: 0,
        coefficient: 0.,
    }; 8];
    let mut size = 0;
    // CASA's stored baseline phase convention is antenna2 minus antenna1.
    let signs = [if phase { -1. } else { 1. }, 1.];
    for (antenna, sign) in antennas.into_iter().zip(signs) {
        if let Some(antenna) = antenna {
            for (offset, weight) in basis.weights.into_iter().enumerate() {
                terms[size] = Term {
                    index: (basis.start + offset) * count + antenna,
                    coefficient: sign * weight,
                };
                size += 1;
            }
        }
    }
    (terms, size)
}

fn unwrap_phases(values: &mut [f64], points: usize, threshold: f64) {
    let points = points.max(2);
    if values.len() <= points.saturating_mul(2) {
        return;
    }
    let end = values.len() - points;
    let mut scratch = vec![0.; points];
    for index in ((points..end).step_by(points / 2)).chain([end]) {
        let difference = phase_step(values, &mut scratch, index);
        remove_wrap(&mut values[index..], difference, threshold);
    }
    let difference = phase_step(values, &mut scratch, points);
    remove_wrap(&mut values[..points], -difference, threshold);
}

fn phase_step(values: &[f64], scratch: &mut [f64], index: usize) -> f64 {
    let width = scratch.len();
    let mut median = |slice: &[f64]| {
        scratch.copy_from_slice(slice);
        scratch.sort_unstable_by(f64::total_cmp);
        let lower = scratch[(width - 1) / 2];
        // casacore's default median averages the middle pair only up to 100.
        if width > 100 {
            lower
        } else {
            0.5 * (lower + scratch[width / 2])
        }
    };
    median(&values[index..index + width]) - median(&values[index - width..index])
}

#[cfg(test)]
mod tests {
    use super::phase_step;

    #[test]
    fn long_even_phase_window_uses_the_lower_median() {
        for (width, expected) in [(100, 1.), (102, 0.)] {
            let mut phases = vec![0.; width * 2];
            phases[width + width / 2..].fill(2.);
            assert_eq!(phase_step(&phases, &mut vec![0.; width], width), expected);
        }
    }
}

fn remove_wrap(samples: &mut [f64], difference: f64, threshold: f64) {
    let wraps = i32::from(difference < -threshold) - i32::from(difference > threshold);
    if wraps == 0 {
        return;
    }
    let offset = f64::from(wraps) * TAU;
    for sample in samples {
        *sample += offset;
    }
}
