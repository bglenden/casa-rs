// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weighted baseline responses, after the common model and prior calibration.

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::MeasurementSet;
use casa_types::Complex32;

use super::grouping::{
    PriorCalibrationMode, SelectedSolveRow, SolveAccumulator, SolveGroupOptions, SolveInputRows,
    build_solve_groups, median_with_even_mean, polynomial,
};
use super::kernel::SolutionRow;
use super::writer::{GainChannelGrid, write_baseline_caltable};
use super::{
    GainNormalization, GainSolveError, GainSolveMode, GainSolveModelSource, GainSolveReport,
    GainSolveRequest, GainType, RefAntSelector, SolvedGains,
};
use crate::bandpass::solution_frequency_bin_widths;
use crate::session::prepare_calibration_solve;
use crate::solve_stats::{IntervalStages, SolveStatsBuilder, ToolSolveActivity};
use crate::{ApplyPlanError, BandpassFrequencyInterval, CalibrationOutputMode, SelectedData};

pub(crate) enum Family {
    Multiplicative {
        frequency_dependent: bool,
        frequency_interval: Option<BandpassFrequencyInterval>,
    },
    Additive,
}

pub(crate) fn solve(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    family: Family,
    mode: CalibrationOutputMode,
    activity: Option<&mut ToolSolveActivity>,
    legacy_iterator: bool,
) -> Result<GainSolveReport, GainSolveError> {
    let additive = matches!(family, Family::Additive);
    if additive && !legacy_iterator {
        return Err(GainSolveError::Baseline(
            "Can't do selfGatherAndSolve for A Mueller".into(),
        ));
    }
    let (frequency_dependent, frequency_interval, subtype) = match family {
        Family::Additive => (false, None, "A Mueller"),
        Family::Multiplicative {
            frequency_dependent,
            frequency_interval,
        } => (
            frequency_dependent,
            frequency_interval,
            if frequency_dependent {
                "Mf Mueller"
            } else {
                "M Mueller"
            },
        ),
    };
    super::validate_solve_interval(request.solve_interval)?;
    let direct = activity.is_some();
    let legacy_gather = legacy_iterator && (direct || additive);
    // The legacy self-gather does not use the modern per-hand flag controls
    // or publish the generic solve activity.
    let track_gather = direct && !legacy_iterator;
    let corrdepflags = track_gather && request.correlation_dependent_flags;
    let input = crate::timing::Phase::start("blcal_select_read");
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &RefAntSelector::Unspecified,
        &request.prior_calibration_tables,
        request.parang,
        true,
        if additive {
            PriorCalibrationMode::Data
        } else {
            PriorCalibrationMode::Baseline {
                share_input_flags: !corrdepflags,
            }
        },
    )
    .map_err(setup_error)?;
    let bin_widths = solution_frequency_bin_widths(ms, &context.rows, frequency_interval)
        .map_err(|error| GainSolveError::Baseline(error.to_string()))?;
    let incompatible_legacy_windows = if legacy_gather && frequency_dependent {
        legacy_binned_windows(ms, &context.rows, &bin_widths)?
    } else {
        BTreeSet::new()
    };
    let preaverage = if additive {
        // CASA averages the whole solve interval for AMueller.
        None
    } else if legacy_gather {
        let requested = request.preaverage_seconds;
        (requested.is_finite() && requested >= 0.).then_some(if requested < f64::EPSILON {
            0.1
        } else {
            requested
        })
    } else if direct {
        super::preaverage_span(request.solve_interval, request.preaverage_seconds)
    } else {
        None
    };
    let (model_source, point_stokes) = if additive {
        // The legacy baseline gather with a unit model averages the calibrated DATA.
        (GainSolveModelSource::PointSource, [1., 0., 0., 0.])
    } else {
        (request.model_source, request.smodel)
    };
    let groups = build_solve_groups(
        ms,
        &context.rows,
        context.preapplied_rows.as_ref(),
        &bin_widths,
        SolveGroupOptions {
            input_rows: SolveInputRows::baseline(direct || additive, legacy_iterator),
            gain_type: GainType::G,
            model_source,
            point_stokes,
            rotate_model: false,
            solve_interval: request.solve_interval,
            combine: request.combine,
            preaverage_max_span: preaverage,
            correlation_dependent_flags: corrdepflags,
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
    let mut stats = if track_gather {
        Some(SolveStatsBuilder::new(
            2,
            super::spectral_window_ids(ms)?,
            &context.available_antennas,
        ))
    } else {
        None
    };
    for group in groups.into_values() {
        let attempted = if let Some(stats) = &mut stats {
            stats.interval(&IntervalStages::of(&group, 0));
            group.gather_attempted()
        } else {
            group.attempted
        };
        if !attempted {
            continue;
        }
        if incompatible_legacy_windows.contains(&group.spw_id) {
            return Err(GainSolveError::Baseline(
                "the legacy iterator cannot fit MF with frequency averaging".into(),
            ));
        }
        if group.baseline_correlation_counts.len() > 1 {
            return Err(GainSolveError::Baseline(
                "Error in Calibrater::solve.".into(),
            ));
        }
        solved.attempted_spw_ids.insert(group.spw_id);
        let antennas = &context.available_antennas;
        let mode = request.solve_mode;
        let rows = group
            .legacy_baseline_solutions(antennas, mode)
            .unwrap_or_else(|| solve_group(&group, antennas, mode));
        solved.rows.extend(rows);
        if let Some(stats) = &mut stats {
            stats.record_success(group.spw_id, true);
        }
    }
    if let Some(stats) = stats {
        stats.track_activity(activity);
        solved.solve_stats = stats.finish();
    }
    if request.normalize_average_amplitude {
        normalize(
            &mut solved.rows,
            frequency_dependent,
            request.normalization_type,
        );
    }
    drop(fitting);
    let channels = frequency_dependent.then_some(GainChannelGrid {
        bin_widths: &bin_widths,
        selected_rows: &context.rows,
    });
    write_baseline_caltable(ms, request, &solved, mode, subtype, channels, legacy_gather)
}

fn legacy_binned_windows(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    bin_widths: &BTreeMap<i32, usize>,
) -> Result<BTreeSet<i32>, GainSolveError> {
    if bin_widths.values().all(|&width| width <= 1) {
        return Ok(BTreeSet::new());
    }
    let spectral = ms
        .spectral_window()
        .map_err(|error| GainSolveError::Baseline(error.to_string()))?;
    let selections = rows
        .iter()
        .map(|row| (row.data_spw_id, row.channel_selection.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let mut incompatible = BTreeSet::new();
    for (window, selection) in selections {
        if bin_widths.get(&window).is_some_and(|&width| width > 1) {
            let count = spectral
                .num_chan(window as usize)
                .map_err(|error| GainSolveError::Baseline(error.to_string()))?;
            if polynomial::channel_selection(selection, count as usize)?
                .0
                .len()
                > 1
            {
                incompatible.insert(window);
            }
        }
    }
    Ok(incompatible)
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
    // SDBList::aggregateTimeCentroid uses all hands, even though
    // MMueller::solveOne fits only the parallel-hand responses.
    let mut spectra = group.delay_spectra().collect::<Vec<_>>();
    spectra
        .sort_unstable_by_key(|spectrum| (spectrum.buffer, spectrum.antennas, spectrum.receptor));
    for spectrum in spectra {
        let (p, q) = spectrum.antennas;
        let mut responses = spectrum.parallel_hand.then(|| {
            baseline
                .entry((p.min(q), p.max(q)))
                .or_insert_with(|| vec![Response::default(); cells])
        });
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
            if let Some(responses) = &mut responses {
                let response = &mut responses[2 * channel + spectrum.receptor];
                response.sum += if p > q { sum.conj() } else { sum };
                response.weight += weight;
            }
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

fn normalize(rows: &mut [SolutionRow], frequency_dependent: bool, method: GainNormalization) {
    if frequency_dependent {
        for row in rows {
            for receptor in 0..2 {
                if let Some(factor) = channel_normalization(row, receptor, method) {
                    row.gains
                        .iter_mut()
                        .skip(receptor)
                        .step_by(2)
                        .for_each(|value| *value /= factor);
                }
            }
        }
    } else {
        let mut windows = BTreeMap::<i32, ResponseNormalization>::new();
        for row in rows.iter() {
            let normalization = windows
                .entry(row.spw_id)
                .or_insert_with(|| ResponseNormalization::new(method));
            for (value, flagged) in row.gains.iter().zip(&row.flags) {
                if !flagged {
                    normalization.observe(value.norm());
                }
            }
        }
        let factors = windows
            .into_iter()
            .filter_map(|(spw, normalization)| Some((spw, normalization.factor()?)))
            .collect::<BTreeMap<_, _>>();
        for row in rows {
            if let Some(&factor) = factors.get(&row.spw_id) {
                row.gains.iter_mut().for_each(|value| *value /= factor);
            }
        }
    }
}

struct ResponseNormalization {
    count: usize,
    sum: f32,
    amplitudes: Option<Vec<f32>>,
}

impl ResponseNormalization {
    fn new(method: GainNormalization) -> Self {
        Self {
            count: 0,
            sum: 0.,
            amplitudes: (method == GainNormalization::Median).then(Vec::new),
        }
    }

    fn observe(&mut self, amplitude: f32) {
        self.count += 1;
        if let Some(amplitudes) = &mut self.amplitudes {
            amplitudes.push(amplitude);
        } else {
            self.sum += amplitude;
        }
    }

    fn factor(mut self) -> Option<f32> {
        if self.count < 2 {
            return None;
        }
        let factor = self
            .amplitudes
            .as_mut()
            .map_or(self.sum / self.count as f32, |amplitudes| {
                median_with_even_mean(amplitudes)
            });
        (factor > 0.).then_some(factor)
    }
}

/// Mf uses the amplitude statistic and the unit-amplitude phase sum. With a
/// defined zero channel CASA's phase factor is undefined and leaves the hand
/// unnormalized.
fn channel_normalization(
    row: &SolutionRow,
    receptor: usize,
    method: GainNormalization,
) -> Option<Complex32> {
    let mut normalization = ResponseNormalization::new(method);
    let mut phase = Complex32::ZERO;
    for (value, flagged) in row.gains.iter().zip(&row.flags).skip(receptor).step_by(2) {
        if *flagged {
            continue;
        }
        let norm = value.norm();
        if norm == 0. || !norm.is_finite() {
            return None;
        }
        normalization.observe(norm);
        phase += *value / norm;
    }
    let phase_norm = phase.norm();
    normalization
        .factor()
        .filter(|_| phase_norm > 0.)
        .map(|factor| (phase / phase_norm) * factor)
}
