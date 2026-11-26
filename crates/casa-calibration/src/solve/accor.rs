// SPDX-License-Identifier: LGPL-3.0-or-later
//! Normalize sampler amplitudes from antenna autocorrelation spectra.

use std::collections::{BTreeMap, BTreeSet};

use casa_core::Complex32;
use casa_ms::MeasurementSet;

use super::grouping::{
    PriorCalibrationMode, SolveAccumulator, SolveGroupOptions, SolveInputRows, build_solve_groups,
    correlation_types_for_ddid,
};
use super::kernel::SolutionRow;
use super::writer::write_new_style_gain_caltable;
use super::{
    GainSolveError, GainSolveMode, GainSolveReport, GainSolveRequest, GainType, RefAntSelector,
    SolvedGains,
};
use crate::solve_stats::{IntervalStages, SolveStatsBuilder, ToolSolveActivity};
use crate::{CalibrationOutputMode, SelectedData, session::prepare_calibration_solve};

pub(crate) fn solve(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    mode: CalibrationOutputMode,
    activity: Option<&mut ToolSolveActivity>,
    legacy_iterator: bool,
    invalid_mode: bool,
) -> Result<GainSolveReport, GainSolveError> {
    super::validate_solve_interval(request.solve_interval)?;
    let direct = activity.is_some();
    let mut selection = request.selection.clone();
    if !direct {
        selection.msselect = Some(match selection.msselect {
            Some(expression) => format!("({expression}) && ANTENNA1==ANTENNA2"),
            None => "ANTENNA1==ANTENNA2".into(),
        });
    }
    let input = crate::timing::Phase::start("accor_select_read");
    let context = prepare_calibration_solve(
        ms,
        &selection,
        if direct {
            &request.refant
        } else {
            &RefAntSelector::Unspecified
        },
        &request.prior_calibration_tables,
        request.parang,
        true,
        PriorCalibrationMode::Accor {
            share_input_flags: !request.correlation_dependent_flags,
        },
    )?;
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &BTreeMap::new(),
        SolveGroupOptions {
            input_rows: match (direct, legacy_iterator) {
                (true, true) => SolveInputRows::LegacyAccor,
                (true, false) => SolveInputRows::AccorTool,
                (false, _) => SolveInputRows::Autocorrelations,
            },
            gain_type: GainType::G,
            model_source: request.model_source,
            point_stokes: request.smodel,
            rotate_model: false,
            solve_interval: request.solve_interval,
            combine: request.combine,
            preaverage_max_span: direct
                .then(|| super::preaverage_span(request.solve_interval, request.preaverage_seconds))
                .flatten(),
            correlation_dependent_flags: request.correlation_dependent_flags,
            robust_seed: false,
            legacy_g_receptors: false,
            channelized: false,
            polynomial_bandpass: false,
        },
    )?;
    drop(input);
    let fitting = crate::timing::Phase::start("accor_fit");
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
        selected_data: if direct {
            context.selected_data.clone()
        } else {
            SelectedData::of_autocorrelations(ms, &context.rows)?
        },
        solve_stats: Default::default(),
    };
    let mut stats = if direct {
        Some(SolveStatsBuilder::new(
            2,
            super::spectral_window_ids(ms)?,
            &context.available_antennas,
        ))
    } else {
        None
    };
    let solve_mode = if direct {
        request.solve_mode
    } else {
        GainSolveMode::AmplitudePhase
    };
    for group in groups.into_values() {
        if let Some(stats) = &mut stats {
            stats.interval(&IntervalStages::of(&group, 0));
            if !group.gather_attempted() {
                continue;
            }
            if legacy_iterator {
                return Err(GainSolveError::AccorNeedsModernIterator);
            }
        }
        let rows = solve_group(ms, &group, &context.available_antennas, solve_mode)?;
        if !rows.is_empty() {
            solved.attempted_spw_ids.insert(group.spw_id);
            solved.rows.extend(rows);
            if let Some(stats) = &mut stats {
                stats.record_success(group.spw_id, true);
            }
        }
    }
    if let Some(mut stats) = stats {
        if invalid_mode && !solved.rows.is_empty() {
            return Err(GainSolveError::AccorInvalidSolveMode);
        }
        super::apply_requested_reference(ms, &mut solved.rows, &request.refant, &context)?;
        if solve_mode != GainSolveMode::AmplitudePhase {
            for row in &mut solved.rows {
                for (gain, &flagged) in row.gains.iter_mut().zip(&row.flags) {
                    if flagged || gain.re == 0. {
                        *gain = Complex32::new(1., 0.);
                    }
                }
            }
        }
        if solve_mode != GainSolveMode::Phase && request.normalize_average_amplitude {
            super::normalize_gain_solution_amplitudes(&mut solved.rows, request.normalization_type);
        }
        super::tally_references(&solved.rows, &mut stats);
        stats.track_activity(activity);
        solved.solve_stats = stats.finish();
    }
    drop(fitting);
    let request = GainSolveRequest {
        gain_type: GainType::G,
        ..request.clone()
    };
    write_new_style_gain_caltable(ms, &request, &solved, mode, "Accor Jones")
}

fn solve_group(
    ms: &MeasurementSet,
    group: &SolveAccumulator,
    antennas: &BTreeSet<i32>,
    mode: GainSolveMode,
) -> Result<Vec<SolutionRow>, GainSolveError> {
    // CASA collapses each input buffer before fitting only the first one.
    let Some(first) = group.first_input_buffer() else {
        return Ok(Vec::new());
    };
    let correlation_count = correlation_types_for_ddid(ms, first.1)?.len();
    let origin = group.min_time;
    let (offset, total) = group
        .delay_spectra()
        .fold((0., 0.), |(offset, total), spectrum| {
            let weight = spectrum
                .channels()
                .map(|(sum, weight)| {
                    if mode == GainSolveMode::Phase && weight > 0. {
                        let amplitude = (sum / weight).norm();
                        if amplitude > 0. {
                            return f64::from(weight * amplitude.powi(2));
                        }
                    }
                    f64::from(weight)
                })
                .sum::<f64>();
            (
                offset + weight * (spectrum.time_seconds - origin),
                total + weight,
            )
        });
    if !group.attempted && !group.gather_attempted() {
        return Ok(Vec::new());
    }
    let time = if total > 0. {
        origin + offset / total
    } else {
        group.aggregate_time()
    };
    let mut gains = BTreeMap::<i32, [Option<f32>; 2]>::new();
    for spectrum in group.delay_spectra().filter(|spectrum| {
        spectrum.parallel_hand
            && spectrum.antennas.0 == spectrum.antennas.1
            && group.in_first_input_buffer(spectrum.buffer)
    }) {
        let Some((sum, weight)) = spectrum.channels().next() else {
            continue;
        };
        if weight > 0. {
            let amplitude = (sum / weight).norm();
            if amplitude.is_finite() && amplitude > 0. {
                gains.entry(spectrum.antennas.0).or_default()[spectrum.receptor] =
                    Some(if mode == GainSolveMode::Phase {
                        1.
                    } else {
                        amplitude.sqrt()
                    });
            }
        }
    }
    Ok(antennas
        .iter()
        .map(|&antenna| {
            let [first, last] = gains.get(&antenna).copied().unwrap_or_default();
            let second = match correlation_count {
                2 | 4 => last,
                _ => first,
            };
            let second_value = if correlation_count == 1 {
                0.
            } else {
                second.unwrap_or(1.)
            };
            let values = [first.unwrap_or(1.), second_value];
            SolutionRow {
                time_seconds: time,
                field_id: group.field_id,
                spw_id: group.spw_id,
                antenna_id: antenna,
                scan_number: group.scan_number(),
                observation_id: group.observation_id,
                refant_id: -1,
                gains: values.map(|value| Complex32::new(value, 0.)).to_vec(),
                flags: vec![first.is_none(), second.is_none()],
                param_errors: vec![0.; 2],
                snrs: vec![0.; 2],
                weights: Vec::new(),
            }
        })
        .collect())
}
