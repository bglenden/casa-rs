// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weighted baseline responses, after the common model and prior calibration.

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::MeasurementSet;
use casa_types::Complex32;

use super::grouping::{
    PriorCalibrationMode, SolveAccumulator, SolveGroupOptions, SolveInputRows, build_solve_groups,
};
use super::kernel::SolutionRow;
use super::writer::{GainChannelGrid, write_baseline_caltable};
use super::{
    GainSolveError, GainSolveMode, GainSolveReport, GainSolveRequest, GainType, RefAntSelector,
    SolvedGains,
};
use crate::bandpass::solution_frequency_bin_widths;
use crate::session::prepare_calibration_solve;
use crate::{ApplyPlanError, BandpassFrequencyInterval, CalibrationOutputMode, SelectedData};

pub(crate) fn solve(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    frequency_dependent: bool,
    frequency_interval: Option<BandpassFrequencyInterval>,
    mode: CalibrationOutputMode,
) -> Result<GainSolveReport, GainSolveError> {
    super::validate_solve_interval(request.solve_interval)?;
    let input = crate::timing::Phase::start("blcal_select_read");
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &RefAntSelector::Unspecified,
        &request.prior_calibration_tables,
        request.parang,
        true,
        PriorCalibrationMode::Baseline,
    )
    .map_err(setup_error)?;
    let bin_widths = solution_frequency_bin_widths(ms, &context.rows, frequency_interval)
        .map_err(|error| GainSolveError::Baseline(error.to_string()))?;
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &bin_widths,
        SolveGroupOptions {
            input_rows: SolveInputRows::Baselines,
            gain_type: GainType::G,
            model_source: request.model_source,
            point_stokes: request.smodel,
            rotate_model: false,
            solve_interval: request.solve_interval,
            combine: request.combine,
            preaverage_max_span: None,
            correlation_dependent_flags: false,
            robust_seed: false,
            channelized: frequency_dependent,
            polynomial_bandpass: false,
        },
    )?;
    drop(input);
    let fitting = crate::timing::Phase::start("blcal_fit");
    let mut solved = SolvedGains {
        rows: Vec::new(),
        refant_id: -1,
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
        selected_data: SelectedData::of_rows(ms, context.rows.iter())?,
        solve_stats: Default::default(),
    };
    for group in groups.into_values().filter(|group| group.attempted) {
        solved.attempted_spw_ids.insert(group.spw_id);
        solved.rows.extend(solve_group(
            &group,
            &context.available_antennas,
            request.solve_mode,
        ));
    }
    if request.normalize_average_amplitude {
        normalize(&mut solved.rows, frequency_dependent);
    }
    drop(fitting);
    let channels = frequency_dependent.then_some(GainChannelGrid {
        bin_widths: &bin_widths,
        selected_rows: &context.rows,
    });
    write_baseline_caltable(ms, request, &solved, mode, channels)
}

fn setup_error(error: GainSolveError) -> GainSolveError {
    match &error {
        GainSolveError::EmptySelection => GainSolveError::Baseline(
            "Error in data selection specification: MSSelectionNullSelection : The selected table has zero rows."
                .into(),
        ),
        GainSolveError::PriorCalibrationPlan { source } => match source.as_ref() {
            ApplyPlanError::SummarizeCalibrationTable { path, .. }
                if !std::path::Path::new(path).exists() =>
            {
                GainSolveError::Baseline(format!("Table {path} does not exist."))
            }
            _ => error,
        },
        _ => error,
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Response {
    sum: Complex32,
    weight: f32,
}

fn solve_group(
    group: &SolveAccumulator,
    antennas: &BTreeSet<i32>,
    mode: GainSolveMode,
) -> Vec<SolutionRow> {
    let cells = group.solution_channels * 2;
    let mut baseline = BTreeMap::<(i32, i32), Vec<Response>>::new();
    let origin = group.min_time;
    let mut offset_sum = 0.0_f64;
    let mut total_weight = 0.0_f64;
    let mut spectra = group
        .delay_spectra()
        .filter(|spectrum| spectrum.parallel_hand)
        .collect::<Vec<_>>();
    spectra
        .sort_unstable_by_key(|spectrum| (spectrum.buffer, spectrum.antennas, spectrum.receptor));
    for spectrum in spectra {
        let (p, q) = spectrum.antennas;
        let responses = baseline
            .entry((p.min(q), p.max(q)))
            .or_insert_with(|| vec![Response::default(); cells]);
        let mut spectrum_weight = 0.0_f64;
        for (channel, (sum, weight)) in spectrum.channels().enumerate() {
            if weight <= 0.0 {
                continue;
            }
            let (sum, weight) = match mode {
                GainSolveMode::Phase => {
                    let amplitude = (sum / weight).norm();
                    if amplitude > 0. {
                        (sum * amplitude, weight * amplitude * amplitude)
                    } else {
                        (sum, weight)
                    }
                }
                GainSolveMode::Amplitude => (Complex32::new(sum.norm(), 0.), weight),
                GainSolveMode::AmplitudePhase => (sum, weight),
            };
            let response = &mut responses[2 * channel + spectrum.receptor];
            response.sum += if p > q { sum.conj() } else { sum };
            response.weight += weight;
            spectrum_weight += f64::from(weight);
        }
        offset_sum += (spectrum.time_seconds - origin) * spectrum_weight;
        total_weight += spectrum_weight;
    }
    let time = if total_weight > 0. {
        origin + offset_sum / total_weight
    } else {
        group.aggregate_time()
    };
    let scan_number = group.scan_number();
    antennas
        .iter()
        .flat_map(|&p| antennas.range(p..).map(move |&q| (p, q)))
        .map(|(p, q)| {
            let values = baseline
                .remove(&(p, q))
                .unwrap_or_else(|| vec![Response::default(); cells]);
            let (gains, flags) = values
                .into_iter()
                .map(|response| {
                    if response.weight > 0. {
                        (response.sum / response.weight, false)
                    } else {
                        (Complex32::ZERO, true)
                    }
                })
                .unzip();
            SolutionRow {
                time_seconds: time,
                field_id: group.field_id,
                spw_id: group.spw_id,
                antenna_id: p,
                scan_number,
                observation_id: group.observation_id,
                refant_id: q,
                gains,
                flags,
                param_errors: vec![0.; cells],
                snrs: vec![0.; cells],
                weights: Vec::new(),
            }
        })
        .collect()
}

fn normalize(rows: &mut [SolutionRow], frequency_dependent: bool) {
    if frequency_dependent {
        for row in rows {
            for receptor in 0..2 {
                if let Some(factor) = mean_channel_response(row, receptor) {
                    row.gains
                        .iter_mut()
                        .skip(receptor)
                        .step_by(2)
                        .for_each(|value| *value /= factor);
                }
            }
        }
    } else {
        let mut windows = BTreeMap::<i32, (f32, usize)>::new();
        for row in rows.iter() {
            let (sum, count) = windows.entry(row.spw_id).or_default();
            for (value, flagged) in row.gains.iter().zip(&row.flags) {
                if !flagged {
                    *sum += value.norm();
                    *count += 1;
                }
            }
        }
        for row in rows {
            let (sum, count) = windows[&row.spw_id];
            if count > 1 && sum > 0. {
                let factor = sum / count as f32;
                row.gains.iter_mut().for_each(|value| *value /= factor);
            }
        }
    }
}

/// Mf uses the mean amplitude and the unit-amplitude phase sum. With a
/// defined zero channel CASA's phase factor is undefined and leaves the hand
/// unnormalized.
fn mean_channel_response(row: &SolutionRow, receptor: usize) -> Option<Complex32> {
    let mut amplitude = 0.;
    let mut phase = Complex32::ZERO;
    let mut count = 0;
    for (value, flagged) in row.gains.iter().zip(&row.flags).skip(receptor).step_by(2) {
        if *flagged {
            continue;
        }
        let norm = value.norm();
        if norm == 0. || !norm.is_finite() {
            return None;
        }
        amplitude += norm;
        phase += *value / norm;
        count += 1;
    }
    let phase_norm = phase.norm();
    (count > 1 && phase_norm > 0.).then(|| (phase / phase_norm) * (amplitude / count as f32))
}
