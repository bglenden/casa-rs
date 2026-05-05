// SPDX-License-Identifier: LGPL-3.0-or-later
//! Numerical solve kernel for limited `gaincal`.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::f64::consts::PI;

use casa_types::Complex32;
use num_complex::Complex64;

use super::{GainSolveError, GainSolveMode, GainType};
use crate::solve::grouping::{SolveAccumulator, SolveEdgeStats};
use crate::solve::trace;

#[derive(Debug, Clone)]
pub(crate) struct SolutionRow {
    pub(crate) time_seconds: f64,
    pub(crate) interval_seconds: f64,
    pub(crate) field_id: i32,
    pub(crate) spw_id: i32,
    pub(crate) antenna_id: i32,
    pub(crate) scan_number: i32,
    pub(crate) observation_id: i32,
    pub(crate) refant_id: i32,
    pub(crate) gains: Vec<Complex32>,
    pub(crate) flags: Vec<bool>,
    pub(crate) param_errors: Vec<f32>,
    pub(crate) snrs: Vec<f32>,
    pub(crate) weights: Vec<f32>,
}

#[derive(Debug, Clone)]
pub(crate) struct SolvedPhaseGraph {
    pub(crate) gains: HashMap<i32, Complex32>,
    pub(crate) reachable: BTreeSet<i32>,
    pub(crate) hessian: HashMap<i32, f32>,
    pub(crate) param_error: HashMap<i32, f32>,
    pub(crate) snr: HashMap<i32, f32>,
    error_chi_square: f64,
    sample_count: usize,
    good_count: usize,
    snr_amplitude: HashMap<i32, f32>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveGraphOptions {
    pub(crate) refant_id: i32,
    pub(crate) min_baselines_per_antenna: usize,
}

struct PhaseSolveResult {
    gains: HashMap<i32, Complex32>,
    error_gains: HashMap<i32, Complex32>,
    snr_amplitude: HashMap<i32, f32>,
}

pub(crate) struct PhaseStepTrace {
    pub(crate) x0: f64,
    pub(crate) x1: f64,
    pub(crate) x2: f64,
    pub(crate) step: f64,
    pub(crate) opt_factor: f64,
    pub(crate) expanded: bool,
    pub(crate) iterations: usize,
}

pub(crate) struct PhaseSolverTraceEvent<'a> {
    pub(crate) iteration: usize,
    pub(crate) refant_id: i32,
    pub(crate) chi_square: f64,
    pub(crate) last_chi_square: f64,
    pub(crate) delta_chi_square: f64,
    pub(crate) fractional_delta: f64,
    pub(crate) convergence_count: i32,
    pub(crate) step: &'a PhaseStepTrace,
    pub(crate) antenna_ids: &'a BTreeSet<i32>,
    pub(crate) last_gains: &'a HashMap<i32, Complex64>,
    pub(crate) gains: &'a HashMap<i32, Complex64>,
    pub(crate) gradient: &'a HashMap<i32, Complex64>,
    pub(crate) hessian: &'a HashMap<i32, f64>,
    pub(crate) delta: &'a HashMap<i32, Complex64>,
}

pub(crate) fn solve_group(
    group: SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    gain_type: GainType,
    solve_mode: GainSolveMode,
    refant_id: i32,
    min_snr: f32,
    min_baselines_per_antenna: usize,
) -> Result<Vec<SolutionRow>, GainSolveError> {
    let averaged_time = group.aggregate_time_centroid();
    let averaged_interval = group.total_interval / group.sample_rows.max(1) as f64;
    let scan_number = *group.scan_numbers.iter().next().unwrap_or(&0);
    let solve_refant_id =
        choose_effective_refant(&group.receptor_graphs, &group.receptor_weights, refant_id)
            .unwrap_or(refant_id);
    let options = SolveGraphOptions {
        refant_id: solve_refant_id,
        min_baselines_per_antenna,
    };
    let mut solved = if matches!((gain_type, solve_mode), (GainType::G, GainSolveMode::Phase)) {
        solve_phase_graphs_like_casa(
            &group.receptor_graphs,
            &group.receptor_weights,
            &group.receptor_stats,
            options,
        )?
    } else {
        group
            .receptor_graphs
            .iter()
            .zip(group.receptor_weights.iter())
            .zip(group.receptor_stats.iter())
            .map(|((graph, weights), stats)| {
                solve_graph(graph, weights, stats, solve_mode, options)
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    apply_group_reduced_chi_square(&mut solved);

    let mut antenna_ids = available_antennas.clone();
    antenna_ids.extend(group.antenna_ids);
    antenna_ids.insert(refant_id);
    antenna_ids.insert(solve_refant_id);

    let solution_rows = antenna_ids
        .into_iter()
        .map(|antenna_id| {
            let gains = match gain_type {
                GainType::G => solved
                    .iter()
                    .map(|per_receptor| {
                        *per_receptor
                            .gains
                            .get(&antenna_id)
                            .unwrap_or(&Complex32::new(1.0, 0.0))
                    })
                    .collect::<Vec<_>>(),
                GainType::T => vec![
                    *solved[0]
                        .gains
                        .get(&antenna_id)
                        .unwrap_or(&Complex32::new(1.0, 0.0)),
                ],
            };
            let flags = match gain_type {
                GainType::G => solved
                    .iter()
                    .map(|per_receptor| {
                        let connected = antenna_id == solve_refant_id
                            || per_receptor.reachable.contains(&antenna_id);
                        let snr = per_receptor
                            .snr
                            .get(&antenna_id)
                            .copied()
                            .unwrap_or_default();
                        let param_error = per_receptor
                            .param_error
                            .get(&antenna_id)
                            .copied()
                            .unwrap_or_default();
                        !connected || below_min_snr(snr, min_snr) || !param_error.is_finite()
                    })
                    .collect(),
                GainType::T => {
                    let connected =
                        antenna_id == solve_refant_id || solved[0].reachable.contains(&antenna_id);
                    let snr = solved[0].snr.get(&antenna_id).copied().unwrap_or_default();
                    let param_error = solved[0]
                        .param_error
                        .get(&antenna_id)
                        .copied()
                        .unwrap_or_default();
                    vec![!connected || below_min_snr(snr, min_snr) || !param_error.is_finite()]
                }
            };
            let param_errors = match gain_type {
                GainType::G => solved
                    .iter()
                    .map(|per_receptor| {
                        per_receptor
                            .param_error
                            .get(&antenna_id)
                            .copied()
                            .unwrap_or_default()
                    })
                    .collect(),
                GainType::T => vec![
                    solved[0]
                        .param_error
                        .get(&antenna_id)
                        .copied()
                        .unwrap_or_default(),
                ],
            };
            let snrs = match gain_type {
                GainType::G => solved
                    .iter()
                    .map(|per_receptor| {
                        per_receptor
                            .snr
                            .get(&antenna_id)
                            .copied()
                            .unwrap_or_default()
                    })
                    .collect(),
                GainType::T => vec![solved[0].snr.get(&antenna_id).copied().unwrap_or_default()],
            };
            let weights = match gain_type {
                GainType::G => solved
                    .iter()
                    .map(|per_receptor| {
                        per_receptor
                            .hessian
                            .get(&antenna_id)
                            .copied()
                            .unwrap_or_default()
                    })
                    .collect(),
                GainType::T => vec![
                    solved[0]
                        .hessian
                        .get(&antenna_id)
                        .copied()
                        .unwrap_or_default(),
                ],
            };
            let gains = gains
                .into_iter()
                .zip(flags.iter())
                .map(|(gain, flag)| {
                    if *flag {
                        Complex32::new(1.0, 0.0)
                    } else {
                        gain
                    }
                })
                .collect();
            SolutionRow {
                time_seconds: averaged_time,
                interval_seconds: averaged_interval,
                field_id: group.field_id,
                spw_id: group.spw_id,
                antenna_id,
                scan_number,
                observation_id: group.observation_id,
                refant_id: solve_refant_id,
                gains,
                flags,
                param_errors,
                snrs,
                weights,
            }
        })
        .collect();

    Ok(solution_rows)
}

fn below_min_snr(snr: f32, min_snr: f32) -> bool {
    snr <= min_snr
}

fn choose_effective_refant(
    graphs: &[HashMap<(i32, i32), Complex32>],
    weights: &[HashMap<(i32, i32), f32>],
    preferred_refant_id: i32,
) -> Option<i32> {
    let mut connected = BTreeSet::new();
    let mut antenna_ids = BTreeSet::new();
    for (graph, weights) in graphs.iter().zip(weights.iter()) {
        for (&(antenna1, antenna2), weight) in weights {
            if *weight <= f32::EPSILON {
                continue;
            }
            if !graph.contains_key(&(antenna1, antenna2)) {
                continue;
            }
            antenna_ids.insert(antenna1);
            antenna_ids.insert(antenna2);
            if antenna1 == preferred_refant_id {
                connected.insert(antenna2);
            }
            if antenna2 == preferred_refant_id {
                connected.insert(antenna1);
            }
        }
    }
    if connected
        .iter()
        .any(|antenna_id| *antenna_id != preferred_refant_id)
    {
        return Some(preferred_refant_id);
    }
    antenna_ids.into_iter().next()
}

struct PreparedPhaseGraph {
    antenna_ids: BTreeSet<i32>,
    active_graph: HashMap<(i32, i32), Complex32>,
    active_weights: HashMap<(i32, i32), f32>,
    active_stats: HashMap<(i32, i32), SolveEdgeStats>,
    reachable: BTreeSet<i32>,
    active_antenna_ids: BTreeSet<i32>,
}

fn prepare_phase_graph(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    stats: &HashMap<(i32, i32), SolveEdgeStats>,
    options: SolveGraphOptions,
) -> Result<PreparedPhaseGraph, GainSolveError> {
    let refant_id = options.refant_id;
    let mut antenna_ids = BTreeSet::new();
    for (antenna1, antenna2) in graph.keys() {
        antenna_ids.insert(*antenna1);
        antenna_ids.insert(*antenna2);
    }
    antenna_ids.insert(refant_id);
    let constrained =
        baseline_constrained_antennas(graph, weights, options.min_baselines_per_antenna);
    let active_graph = graph
        .iter()
        .filter_map(|(&(antenna1, antenna2), value)| {
            (constrained.contains(&antenna1) && constrained.contains(&antenna2))
                .then_some(((antenna1, antenna2), *value))
        })
        .collect::<HashMap<_, _>>();
    let active_weights = weights
        .iter()
        .filter_map(|(&(antenna1, antenna2), weight)| {
            (constrained.contains(&antenna1) && constrained.contains(&antenna2))
                .then_some(((antenna1, antenna2), *weight))
        })
        .collect::<HashMap<_, _>>();
    let active_stats = stats
        .iter()
        .filter_map(|(&(antenna1, antenna2), stat)| {
            (constrained.contains(&antenna1) && constrained.contains(&antenna2))
                .then_some(((antenna1, antenna2), *stat))
        })
        .collect::<HashMap<_, _>>();
    let reachable = reachable_antennas(&active_graph, refant_id);

    let active_antenna_ids = antenna_ids
        .iter()
        .copied()
        .filter(|antenna_id| reachable.contains(antenna_id))
        .collect::<BTreeSet<_>>();

    Ok(PreparedPhaseGraph {
        antenna_ids,
        active_graph,
        active_weights,
        active_stats,
        reachable,
        active_antenna_ids,
    })
}

fn solve_phase_graphs_like_casa(
    graphs: &[HashMap<(i32, i32), Complex32>],
    weights: &[HashMap<(i32, i32), f32>],
    stats: &[HashMap<(i32, i32), SolveEdgeStats>],
    options: SolveGraphOptions,
) -> Result<Vec<SolvedPhaseGraph>, GainSolveError> {
    let prepared = graphs
        .iter()
        .zip(weights.iter())
        .zip(stats.iter())
        .map(|((graph, weights), stats)| prepare_phase_graph(graph, weights, stats, options))
        .collect::<Result<Vec<_>, _>>()?;
    let solved = solve_phase_lm_graphs_like_casa(&prepared, options.refant_id);
    Ok(prepared
        .iter()
        .zip(solved)
        .map(|(prepared, solved_phase)| {
            finish_phase_graph(prepared, solved_phase, options.refant_id)
        })
        .collect())
}

fn finish_phase_graph(
    prepared: &PreparedPhaseGraph,
    solved_phase: PhaseSolveResult,
    refant_id: i32,
) -> SolvedPhaseGraph {
    let mut gains = solved_phase.gains;
    for gain in gains.values_mut() {
        let norm = gain.norm();
        if norm > f32::EPSILON {
            *gain /= Complex32::new(norm, 0.0);
        }
    }
    rereference_gains(&mut gains, refant_id, GainSolveMode::Phase);

    let hessian = hessian(
        &prepared.active_graph,
        &prepared.active_weights,
        &solved_phase.error_gains,
        &prepared.reachable,
    );
    let error_chi_square = chi_square_for_errors(
        &prepared.active_graph,
        &prepared.active_weights,
        &prepared.active_stats,
        &solved_phase.error_gains,
    );
    let good_count = prepared
        .antenna_ids
        .iter()
        .filter(|antenna_id| prepared.reachable.contains(antenna_id))
        .count();
    let sample_count = undirected_collapsed_count(&prepared.active_stats);
    let degrees_of_freedom = (2 * sample_count.saturating_sub(good_count)).max(1);
    let reduced_chi_square = error_chi_square / degrees_of_freedom as f64;
    let param_error = param_errors(&hessian, reduced_chi_square);
    let snr = snrs(
        &gains,
        &prepared.reachable,
        &param_error,
        &solved_phase.snr_amplitude,
    );

    SolvedPhaseGraph {
        gains,
        reachable: prepared.reachable.clone(),
        hessian,
        param_error,
        snr,
        error_chi_square,
        sample_count,
        good_count,
        snr_amplitude: solved_phase.snr_amplitude,
    }
}

pub(crate) fn solve_graph(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    stats: &HashMap<(i32, i32), SolveEdgeStats>,
    solve_mode: GainSolveMode,
    options: SolveGraphOptions,
) -> Result<SolvedPhaseGraph, GainSolveError> {
    let refant_id = options.refant_id;
    let mut antenna_ids = BTreeSet::new();
    for (antenna1, antenna2) in graph.keys() {
        antenna_ids.insert(*antenna1);
        antenna_ids.insert(*antenna2);
    }
    antenna_ids.insert(refant_id);
    let constrained =
        baseline_constrained_antennas(graph, weights, options.min_baselines_per_antenna);
    let active_graph = graph
        .iter()
        .filter_map(|(&(antenna1, antenna2), value)| {
            (constrained.contains(&antenna1) && constrained.contains(&antenna2))
                .then_some(((antenna1, antenna2), *value))
        })
        .collect::<HashMap<_, _>>();
    let active_weights = weights
        .iter()
        .filter_map(|(&(antenna1, antenna2), weight)| {
            (constrained.contains(&antenna1) && constrained.contains(&antenna2))
                .then_some(((antenna1, antenna2), *weight))
        })
        .collect::<HashMap<_, _>>();
    let active_stats = stats
        .iter()
        .filter_map(|(&(antenna1, antenna2), stat)| {
            (constrained.contains(&antenna1) && constrained.contains(&antenna2))
                .then_some(((antenna1, antenna2), *stat))
        })
        .collect::<HashMap<_, _>>();
    let reachable = reachable_antennas(&active_graph, refant_id);

    let active_antenna_ids = antenna_ids
        .iter()
        .copied()
        .filter(|antenna_id| reachable.contains(antenna_id))
        .collect::<BTreeSet<_>>();
    let initial_phases = initial_phases(&active_graph, refant_id, &reachable);
    let mut gains = antenna_ids
        .iter()
        .copied()
        .map(|antenna_id| {
            let phase = *initial_phases.get(&antenna_id).unwrap_or(&0.0);
            (
                antenna_id,
                Complex32::new(phase.cos() as f32, phase.sin() as f32),
            )
        })
        .collect::<HashMap<_, _>>();

    let mut snr_amplitude = HashMap::new();
    let mut error_gains = None;
    if matches!(solve_mode, GainSolveMode::Phase) {
        let solved_phase = solve_phase_lm_like_casa(
            &active_graph,
            &active_weights,
            &active_stats,
            &active_antenna_ids,
            refant_id,
        );
        gains = solved_phase.gains;
        error_gains = Some(solved_phase.error_gains);
        snr_amplitude = solved_phase.snr_amplitude;
    } else {
        for _ in 0..512 {
            let mut changed = false;
            for antenna_id in active_antenna_ids.iter().copied() {
                if !reachable.contains(&antenna_id) || antenna_id == refant_id {
                    continue;
                }
                let mut accumulator = Complex32::new(0.0, 0.0);
                let mut total_weight = 0.0_f32;
                for other_id in active_antenna_ids.iter().copied() {
                    if antenna_id == other_id || !reachable.contains(&other_id) {
                        continue;
                    }
                    if let Some(edge) = active_graph.get(&(antenna_id, other_id)) {
                        accumulator += *edge * gains[&other_id];
                        total_weight += active_weights
                            .get(&(antenna_id, other_id))
                            .copied()
                            .unwrap_or_default()
                            * gains[&other_id].norm_sqr();
                    }
                }
                if total_weight <= f32::EPSILON {
                    continue;
                }
                let candidate = accumulator / Complex32::new(total_weight, 0.0);
                let norm = candidate.norm();
                if norm <= f32::EPSILON {
                    continue;
                }
                let updated = candidate / Complex32::new(norm, 0.0);
                let delta = updated * gains[&antenna_id].conj();
                let delta_complex = f64::from((updated - gains[&antenna_id]).norm());
                let delta_phase = f64::from(delta.im).atan2(f64::from(delta.re)).abs();
                if delta_phase > 1.0e-10 || delta_complex > 1.0e-7 {
                    changed = true;
                }
                gains.insert(antenna_id, updated);
            }
            rereference_gains(&mut gains, refant_id, solve_mode);
            if !changed {
                break;
            }
        }
    }

    if matches!(solve_mode, GainSolveMode::AmplitudePhase) {
        let amplitudes = solve_log_amplitudes(
            &active_graph,
            &active_weights,
            &active_antenna_ids,
            &reachable,
        );
        for &antenna_id in &active_antenna_ids {
            if let (Some(gain), Some(amplitude)) =
                (gains.get_mut(&antenna_id), amplitudes.get(&antenna_id))
            {
                *gain *= Complex32::new(*amplitude, 0.0);
            }
        }
        rereference_gains(&mut gains, refant_id, solve_mode);
    } else {
        for gain in gains.values_mut() {
            let norm = gain.norm();
            if norm > f32::EPSILON {
                *gain /= Complex32::new(norm, 0.0);
            }
        }
        rereference_gains(&mut gains, refant_id, solve_mode);
    }

    let error_gain_ref = error_gains.as_ref().unwrap_or(&gains);
    let hessian = hessian(&active_graph, &active_weights, error_gain_ref, &reachable);
    let error_chi_square = chi_square_for_errors(
        &active_graph,
        &active_weights,
        &active_stats,
        error_gain_ref,
    );
    let good_count = antenna_ids
        .iter()
        .filter(|antenna_id| reachable.contains(antenna_id))
        .count();
    let sample_count = undirected_collapsed_count(&active_stats);
    let degrees_of_freedom = (2 * sample_count.saturating_sub(good_count)).max(1);
    let reduced_chi_square = error_chi_square / degrees_of_freedom as f64;
    let param_error = param_errors(&hessian, reduced_chi_square);
    let snr = snrs(&gains, &reachable, &param_error, &snr_amplitude);

    Ok(SolvedPhaseGraph {
        gains,
        reachable,
        hessian,
        param_error,
        snr,
        error_chi_square,
        sample_count,
        good_count,
        snr_amplitude,
    })
}

fn apply_group_reduced_chi_square(solved: &mut [SolvedPhaseGraph]) {
    let error_chi_square = solved
        .iter()
        .map(|per_receptor| per_receptor.error_chi_square)
        .sum::<f64>();
    let sample_count = solved
        .iter()
        .map(|per_receptor| per_receptor.sample_count)
        .sum::<usize>();
    let good_count = solved
        .iter()
        .map(|per_receptor| per_receptor.good_count)
        .sum::<usize>();
    let degrees_of_freedom = (2 * sample_count.saturating_sub(good_count)).max(1);
    let reduced_chi_square = error_chi_square / degrees_of_freedom as f64;

    for per_receptor in solved {
        per_receptor.param_error = param_errors(&per_receptor.hessian, reduced_chi_square);
        per_receptor.snr = snrs(
            &per_receptor.gains,
            &per_receptor.reachable,
            &per_receptor.param_error,
            &per_receptor.snr_amplitude,
        );
    }
}

fn snrs(
    gains: &HashMap<i32, Complex32>,
    reachable: &BTreeSet<i32>,
    param_error: &HashMap<i32, f32>,
    snr_amplitude: &HashMap<i32, f32>,
) -> HashMap<i32, f32> {
    gains
        .iter()
        .map(|(antenna_id, gain)| {
            let error = param_error.get(antenna_id).copied().unwrap_or_default();
            let amplitude = snr_amplitude
                .get(antenna_id)
                .copied()
                .unwrap_or_else(|| gain.norm());
            let value = if error > 0.0 {
                amplitude / error
            } else if reachable.contains(antenna_id) {
                9_999_999.0
            } else {
                0.0
            };
            (*antenna_id, value)
        })
        .collect()
}

fn solve_phase_lm_like_casa(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    stats: &HashMap<(i32, i32), SolveEdgeStats>,
    antenna_ids: &BTreeSet<i32>,
    refant_id: i32,
) -> PhaseSolveResult {
    let mut gains = initial_casa_t_guess(graph, antenna_ids, refant_id);
    let mut last_gains = gains.clone();
    let mut last_chi_square = f64::MAX;
    let mut convergence_count = 0_i32;

    for iteration in 0..50 {
        let mut chi_square = chi_square_complex64(graph, weights, stats, &gains);
        let delta_chi_square = chi_square - last_chi_square;
        let fractional_delta = if chi_square > 0.0 {
            delta_chi_square / chi_square
        } else {
            0.0
        };

        if fractional_delta <= 0.001 {
            if delta_chi_square.abs() < 0.1 * chi_square {
                convergence_count += 1;
            }
            if convergence_count > 5 {
                break;
            }
        } else if delta_chi_square.abs() > 0.1 * chi_square {
            convergence_count = 0;
        } else {
            convergence_count = (convergence_count - 1).max(0);
        }

        if delta_chi_square <= 0.0 {
            last_chi_square = chi_square;
        } else {
            gains = last_gains.clone();
            chi_square = chi_square_complex64(graph, weights, stats, &gains);
        }

        let mut gradient = antenna_ids
            .iter()
            .copied()
            .map(|antenna_id| (antenna_id, Complex64::new(0.0, 0.0)))
            .collect::<HashMap<_, _>>();
        let mut hessian = antenna_ids
            .iter()
            .copied()
            .map(|antenna_id| (antenna_id, 0.0_f64))
            .collect::<HashMap<_, _>>();

        for (&(antenna1, antenna2), edge) in graph {
            if antenna1 > antenna2 {
                continue;
            }
            let weight = f64::from(
                weights
                    .get(&(antenna1, antenna2))
                    .copied()
                    .unwrap_or_default(),
            );
            if weight <= f64::EPSILON {
                continue;
            }
            let gain1 = gains
                .get(&antenna1)
                .copied()
                .unwrap_or_else(|| Complex64::new(1.0, 0.0));
            let gain2 = gains
                .get(&antenna2)
                .copied()
                .unwrap_or_else(|| Complex64::new(1.0, 0.0));
            let observed = complex32_to_64(*edge);
            let residual_sum = weight * gain1 * gain2.conj() - observed;
            let deriv1 = gain2.conj();
            let deriv2 = gain1;
            *gradient.entry(antenna1).or_default() += residual_sum * deriv1.conj();
            *hessian.entry(antenna1).or_default() += weight * deriv1.norm_sqr();
            *gradient.entry(antenna2).or_default() += deriv2 * residual_sum.conj();
            *hessian.entry(antenna2).or_default() += weight * deriv2.norm_sqr();
        }

        let mut delta = antenna_ids
            .iter()
            .copied()
            .map(|antenna_id| {
                let hessian = hessian.get(&antenna_id).copied().unwrap_or_default();
                let value = if hessian > f64::EPSILON {
                    -gradient
                        .get(&antenna_id)
                        .copied()
                        .unwrap_or_else(|| Complex64::new(0.0, 0.0))
                        / hessian
                        / 2.0
                } else {
                    Complex64::new(0.0, 0.0)
                };
                (antenna_id, value)
            })
            .collect::<HashMap<_, _>>();

        last_gains = gains.clone();
        let step_trace = optimize_phase_step(
            graph,
            weights,
            stats,
            &last_gains,
            &mut gains,
            &mut delta,
            chi_square,
        );
        for antenna_id in antenna_ids {
            if let Some(gain) = gains.get_mut(antenna_id) {
                *gain += delta
                    .get(antenna_id)
                    .copied()
                    .unwrap_or_else(|| Complex64::new(0.0, 0.0));
            }
        }
        trace::trace_phase_solver_iteration(PhaseSolverTraceEvent {
            iteration,
            refant_id,
            chi_square,
            last_chi_square,
            delta_chi_square,
            fractional_delta,
            convergence_count,
            step: &step_trace,
            antenna_ids,
            last_gains: &last_gains,
            gains: &gains,
            gradient: &gradient,
            hessian: &hessian,
            delta: &delta,
        });
    }

    rereference_complex64(&mut gains, refant_id, false);
    let error_gains = gains
        .iter()
        .map(|(antenna_id, gain)| (*antenna_id, complex64_to_32(*gain)))
        .collect::<HashMap<_, _>>();
    let snr_amplitude = gains
        .iter()
        .map(|(antenna_id, gain)| (*antenna_id, gain.norm() as f32))
        .collect::<HashMap<_, _>>();
    rereference_complex64(&mut gains, refant_id, true);
    let gains = gains
        .into_iter()
        .map(|(antenna_id, gain)| (antenna_id, complex64_to_32(gain)))
        .collect();
    PhaseSolveResult {
        gains,
        error_gains,
        snr_amplitude,
    }
}

fn solve_phase_lm_graphs_like_casa(
    prepared: &[PreparedPhaseGraph],
    refant_id: i32,
) -> Vec<PhaseSolveResult> {
    let mut gains = prepared
        .iter()
        .map(|graph| {
            initial_casa_t_guess(&graph.active_graph, &graph.active_antenna_ids, refant_id)
        })
        .collect::<Vec<_>>();
    let mut last_gains = gains.clone();
    let mut last_chi_square = f64::MAX;
    let mut convergence_count = 0_i32;

    for _iteration in 0..50 {
        let mut chi_square = chi_square_phase_graphs(prepared, &gains);
        let delta_chi_square = chi_square - last_chi_square;
        let fractional_delta = if chi_square > 0.0 {
            delta_chi_square / chi_square
        } else {
            0.0
        };

        if fractional_delta <= 0.001 {
            if delta_chi_square.abs() < 0.1 * chi_square {
                convergence_count += 1;
            }
            if convergence_count > 5 {
                break;
            }
        } else if delta_chi_square.abs() > 0.1 * chi_square {
            convergence_count = 0;
        } else {
            convergence_count = (convergence_count - 1).max(0);
        }

        if delta_chi_square <= 0.0 {
            last_chi_square = chi_square;
        } else {
            gains.clone_from(&last_gains);
            chi_square = chi_square_phase_graphs(prepared, &gains);
        }

        let mut deltas = Vec::with_capacity(prepared.len());
        for (graph, receptor_gains) in prepared.iter().zip(gains.iter()) {
            let (gradient, hessian) = phase_gradient_hessian(
                &graph.active_graph,
                &graph.active_weights,
                &graph.active_antenna_ids,
                receptor_gains,
            );
            let delta = graph
                .active_antenna_ids
                .iter()
                .copied()
                .map(|antenna_id| {
                    let hessian = hessian.get(&antenna_id).copied().unwrap_or_default();
                    let value = if hessian > f64::EPSILON {
                        -gradient
                            .get(&antenna_id)
                            .copied()
                            .unwrap_or_else(|| Complex64::new(0.0, 0.0))
                            / hessian
                            / 2.0
                    } else {
                        Complex64::new(0.0, 0.0)
                    };
                    (antenna_id, value)
                })
                .collect::<HashMap<_, _>>();
            deltas.push(delta);
        }

        last_gains = gains.clone();
        optimize_phase_step_graphs(prepared, &last_gains, &mut gains, &mut deltas, chi_square);
        for (receptor_gains, receptor_delta) in gains.iter_mut().zip(deltas.iter()) {
            for (antenna_id, gain) in receptor_gains {
                *gain += receptor_delta
                    .get(antenna_id)
                    .copied()
                    .unwrap_or_else(|| Complex64::new(0.0, 0.0));
            }
        }
    }

    gains
        .into_iter()
        .map(|mut receptor_gains| {
            rereference_complex64(&mut receptor_gains, refant_id, false);
            let error_gains = receptor_gains
                .iter()
                .map(|(antenna_id, gain)| (*antenna_id, complex64_to_32(*gain)))
                .collect::<HashMap<_, _>>();
            let snr_amplitude = receptor_gains
                .iter()
                .map(|(antenna_id, gain)| (*antenna_id, gain.norm() as f32))
                .collect::<HashMap<_, _>>();
            rereference_complex64(&mut receptor_gains, refant_id, true);
            let gains = receptor_gains
                .into_iter()
                .map(|(antenna_id, gain)| (antenna_id, complex64_to_32(gain)))
                .collect();
            PhaseSolveResult {
                gains,
                error_gains,
                snr_amplitude,
            }
        })
        .collect()
}

fn baseline_constrained_antennas(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    min_baselines_per_antenna: usize,
) -> BTreeSet<i32> {
    let mut active = BTreeSet::new();
    for (&(antenna1, antenna2), weight) in weights {
        if antenna1 != antenna2
            && *weight > f32::EPSILON
            && graph.contains_key(&(antenna1, antenna2))
        {
            active.insert(antenna1);
            active.insert(antenna2);
        }
    }
    if min_baselines_per_antenna == 0 {
        return active;
    }

    loop {
        let mut counts = HashMap::<i32, usize>::new();
        for (&(antenna1, antenna2), weight) in weights {
            if antenna1 >= antenna2
                || *weight <= f32::EPSILON
                || !active.contains(&antenna1)
                || !active.contains(&antenna2)
            {
                continue;
            }
            *counts.entry(antenna1).or_default() += 1;
            *counts.entry(antenna2).or_default() += 1;
        }

        let before = active.len();
        active.retain(|antenna_id| {
            counts.get(antenna_id).copied().unwrap_or_default() >= min_baselines_per_antenna
        });
        if active.len() == before {
            break;
        }
    }

    active
}

fn initial_casa_t_guess(
    graph: &HashMap<(i32, i32), Complex32>,
    antenna_ids: &BTreeSet<i32>,
    refant_id: i32,
) -> HashMap<i32, Complex64> {
    let mut gains = antenna_ids
        .iter()
        .copied()
        .map(|antenna_id| (antenna_id, Complex64::new(0.0, 0.0)))
        .collect::<HashMap<_, _>>();
    for antenna_id in antenna_ids {
        if *antenna_id == refant_id {
            continue;
        }
        if let Some(edge) = graph.get(&(refant_id, *antenna_id)) {
            let value = complex32_to_64(*edge);
            let norm = value.norm();
            if norm > f64::EPSILON {
                *gains.entry(*antenna_id).or_default() += value.conj() / norm;
            }
        } else if let Some(edge) = graph.get(&(*antenna_id, refant_id)) {
            let value = complex32_to_64(*edge);
            let norm = value.norm();
            if norm > f64::EPSILON {
                *gains.entry(*antenna_id).or_default() += value / norm;
            }
        }
    }
    for antenna_id in antenna_ids {
        let gain = gains.entry(*antenna_id).or_insert(Complex64::new(1.0, 0.0));
        if gain.norm() <= f64::EPSILON {
            *gain = Complex64::new(1.0, 0.0);
        }
    }
    gains.insert(refant_id, Complex64::new(1.0, 0.0));
    gains
}

fn optimize_phase_step(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    stats: &HashMap<(i32, i32), SolveEdgeStats>,
    last_gains: &HashMap<i32, Complex64>,
    gains: &mut HashMap<i32, Complex64>,
    delta: &mut HashMap<i32, Complex64>,
    current_chi_square: f64,
) -> PhaseStepTrace {
    let mut step = 1.0_f64;
    let mut iterations = 0_usize;
    let x0 = current_chi_square;
    apply_step(gains, last_gains, delta, 1.0);
    let mut x1 = chi_square_complex64(graph, weights, stats, gains);
    let x2;
    let expanded;

    if x1 < x0 {
        expanded = true;
        apply_step(gains, last_gains, delta, 2.0 * step);
        let mut trial_x2 = chi_square_complex64(graph, weights, stats, gains);
        while trial_x2 < x1 {
            iterations += 1;
            step *= 2.0;
            x1 = trial_x2;
            apply_step(gains, last_gains, delta, 2.0 * step);
            trial_x2 = chi_square_complex64(graph, weights, stats, gains);
        }
        x2 = trial_x2;
    } else {
        expanded = false;
        step *= 0.5;
        apply_step(gains, last_gains, delta, step);
        let mut trial_x2 = x1;
        x1 = chi_square_complex64(graph, weights, stats, gains);
        while x1 > x0 {
            iterations += 1;
            step *= 0.5;
            trial_x2 = x1;
            apply_step(gains, last_gains, delta, step);
            x1 = chi_square_complex64(graph, weights, stats, gains);
        }
        x2 = trial_x2;
    }

    let denominator = x0 - 2.0 * x1 + x2;
    let opt_factor = if denominator.abs() > 0.0 {
        step * (1.5 - (x2 - x1) / denominator)
    } else {
        0.0
    };

    gains.clone_from(last_gains);
    if opt_factor > 0.0 {
        for value in delta.values_mut() {
            *value *= opt_factor;
        }
    }
    PhaseStepTrace {
        x0,
        x1,
        x2,
        step,
        opt_factor,
        expanded,
        iterations,
    }
}

fn optimize_phase_step_graphs(
    prepared: &[PreparedPhaseGraph],
    last_gains: &[HashMap<i32, Complex64>],
    gains: &mut [HashMap<i32, Complex64>],
    deltas: &mut [HashMap<i32, Complex64>],
    current_chi_square: f64,
) -> PhaseStepTrace {
    let mut step = 1.0_f64;
    let mut iterations = 0_usize;
    let x0 = current_chi_square;
    apply_step_graphs(gains, last_gains, deltas, 1.0);
    let mut x1 = chi_square_phase_graphs(prepared, gains);
    let x2;
    let expanded;

    if x1 < x0 {
        expanded = true;
        apply_step_graphs(gains, last_gains, deltas, 2.0 * step);
        let mut trial_x2 = chi_square_phase_graphs(prepared, gains);
        while trial_x2 < x1 {
            iterations += 1;
            step *= 2.0;
            x1 = trial_x2;
            apply_step_graphs(gains, last_gains, deltas, 2.0 * step);
            trial_x2 = chi_square_phase_graphs(prepared, gains);
        }
        x2 = trial_x2;
    } else {
        expanded = false;
        step *= 0.5;
        apply_step_graphs(gains, last_gains, deltas, step);
        let mut trial_x2 = x1;
        x1 = chi_square_phase_graphs(prepared, gains);
        while x1 > x0 {
            iterations += 1;
            step *= 0.5;
            trial_x2 = x1;
            apply_step_graphs(gains, last_gains, deltas, step);
            x1 = chi_square_phase_graphs(prepared, gains);
        }
        x2 = trial_x2;
    }

    let denominator = x0 - 2.0 * x1 + x2;
    let opt_factor = if denominator.abs() > 0.0 {
        step * (1.5 - (x2 - x1) / denominator)
    } else {
        0.0
    };

    for (gain, last_gain) in gains.iter_mut().zip(last_gains.iter()) {
        gain.clone_from(last_gain);
    }
    if opt_factor > 0.0 {
        for receptor_delta in deltas {
            for value in receptor_delta.values_mut() {
                *value *= opt_factor;
            }
        }
    }
    PhaseStepTrace {
        x0,
        x1,
        x2,
        step,
        opt_factor,
        expanded,
        iterations,
    }
}

fn apply_step(
    gains: &mut HashMap<i32, Complex64>,
    base: &HashMap<i32, Complex64>,
    delta: &HashMap<i32, Complex64>,
    factor: f64,
) {
    for (antenna_id, gain) in gains {
        *gain = base
            .get(antenna_id)
            .copied()
            .unwrap_or_else(|| Complex64::new(1.0, 0.0))
            + factor
                * delta
                    .get(antenna_id)
                    .copied()
                    .unwrap_or_else(|| Complex64::new(0.0, 0.0));
    }
}

fn apply_step_graphs(
    gains: &mut [HashMap<i32, Complex64>],
    bases: &[HashMap<i32, Complex64>],
    deltas: &[HashMap<i32, Complex64>],
    factor: f64,
) {
    for ((receptor_gains, base), delta) in gains.iter_mut().zip(bases.iter()).zip(deltas.iter()) {
        apply_step(receptor_gains, base, delta, factor);
    }
}

fn chi_square_phase_graphs(
    prepared: &[PreparedPhaseGraph],
    gains: &[HashMap<i32, Complex64>],
) -> f64 {
    prepared
        .iter()
        .zip(gains.iter())
        .map(|(graph, gains)| {
            chi_square_complex64(
                &graph.active_graph,
                &graph.active_weights,
                &graph.active_stats,
                gains,
            )
        })
        .sum()
}

fn chi_square_complex64(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    stats: &HashMap<(i32, i32), SolveEdgeStats>,
    gains: &HashMap<i32, Complex64>,
) -> f64 {
    let mut total = 0.0_f64;
    for (&(antenna1, antenna2), stat) in stats {
        if antenna1 > antenna2 {
            continue;
        }
        let gain1 = gains
            .get(&antenna1)
            .copied()
            .unwrap_or_else(|| Complex64::new(1.0, 0.0));
        let gain2 = gains
            .get(&antenna2)
            .copied()
            .unwrap_or_else(|| Complex64::new(1.0, 0.0));
        let weight = f64::from(
            weights
                .get(&(antenna1, antenna2))
                .copied()
                .unwrap_or_default(),
        );
        if weight <= f64::EPSILON {
            continue;
        }
        let observed = graph
            .get(&(antenna1, antenna2))
            .copied()
            .map(complex32_to_64)
            .unwrap_or_else(|| Complex64::new(0.0, 0.0));
        let model = gain1 * gain2.conj();
        let cross = (model.conj() * observed).re;
        let contribution = stat.weighted_sample_power - 2.0 * cross + weight * model.norm_sqr();
        total += contribution.max(0.0);
    }
    total
}

fn phase_gradient_hessian(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    antenna_ids: &BTreeSet<i32>,
    gains: &HashMap<i32, Complex64>,
) -> (HashMap<i32, Complex64>, HashMap<i32, f64>) {
    let mut gradient = antenna_ids
        .iter()
        .copied()
        .map(|antenna_id| (antenna_id, Complex64::new(0.0, 0.0)))
        .collect::<HashMap<_, _>>();
    let mut hessian = antenna_ids
        .iter()
        .copied()
        .map(|antenna_id| (antenna_id, 0.0_f64))
        .collect::<HashMap<_, _>>();

    for (&(antenna1, antenna2), edge) in graph {
        if antenna1 > antenna2 {
            continue;
        }
        let weight = f64::from(
            weights
                .get(&(antenna1, antenna2))
                .copied()
                .unwrap_or_default(),
        );
        if weight <= f64::EPSILON {
            continue;
        }
        let gain1 = gains
            .get(&antenna1)
            .copied()
            .unwrap_or_else(|| Complex64::new(1.0, 0.0));
        let gain2 = gains
            .get(&antenna2)
            .copied()
            .unwrap_or_else(|| Complex64::new(1.0, 0.0));
        let observed = complex32_to_64(*edge);
        let residual_sum = weight * gain1 * gain2.conj() - observed;
        let deriv1 = gain2.conj();
        let deriv2 = gain1;
        *gradient.entry(antenna1).or_default() += residual_sum * deriv1.conj();
        *hessian.entry(antenna1).or_default() += weight * deriv1.norm_sqr();
        *gradient.entry(antenna2).or_default() += deriv2 * residual_sum.conj();
        *hessian.entry(antenna2).or_default() += weight * deriv2.norm_sqr();
    }

    (gradient, hessian)
}

fn rereference_complex64(gains: &mut HashMap<i32, Complex64>, refant_id: i32, phase_only: bool) {
    let ref_gain = gains
        .get(&refant_id)
        .copied()
        .unwrap_or_else(|| Complex64::new(1.0, 0.0));
    let ref_norm = ref_gain.norm();
    if ref_norm > f64::EPSILON {
        let anchor = ref_gain / ref_norm;
        for gain in gains.values_mut() {
            *gain /= anchor;
        }
    }
    if phase_only {
        for gain in gains.values_mut() {
            let norm = gain.norm();
            if norm > f64::EPSILON {
                *gain /= norm;
            }
        }
        gains.insert(refant_id, Complex64::new(1.0, 0.0));
    }
}

fn complex32_to_64(value: Complex32) -> Complex64 {
    Complex64::new(f64::from(value.re), f64::from(value.im))
}

fn complex64_to_32(value: Complex64) -> Complex32 {
    Complex32::new(value.re as f32, value.im as f32)
}

fn hessian(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    gains: &HashMap<i32, Complex32>,
    reachable: &BTreeSet<i32>,
) -> HashMap<i32, f32> {
    let mut hessian = HashMap::new();
    for &(antenna_id, other_id) in graph.keys() {
        if !reachable.contains(&antenna_id) || !reachable.contains(&other_id) {
            continue;
        }
        let weight = weights
            .get(&(antenna_id, other_id))
            .copied()
            .unwrap_or_default();
        if weight <= f32::EPSILON {
            continue;
        }
        let other_power = gains
            .get(&other_id)
            .map(|gain| gain.norm_sqr())
            .unwrap_or(1.0);
        *hessian.entry(antenna_id).or_insert(0.0) += weight * other_power;
    }
    hessian
}

fn chi_square_for_errors(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    stats: &HashMap<(i32, i32), SolveEdgeStats>,
    gains: &HashMap<i32, Complex32>,
) -> f64 {
    let mut total = 0.0_f64;
    for (&(antenna1, antenna2), stat) in stats {
        if antenna1 > antenna2 {
            continue;
        }
        let gain1 = gains
            .get(&antenna1)
            .copied()
            .unwrap_or_else(|| Complex32::new(1.0, 0.0));
        let gain2 = gains
            .get(&antenna2)
            .copied()
            .unwrap_or_else(|| Complex32::new(1.0, 0.0));
        let observed = graph
            .get(&(antenna1, antenna2))
            .copied()
            .unwrap_or_else(|| Complex32::new(0.0, 0.0));
        let weight = f64::from(
            weights
                .get(&(antenna1, antenna2))
                .copied()
                .unwrap_or_default(),
        );
        let model = gain1 * gain2.conj();
        let cross = (model.conj() * observed).re;
        let contribution = stat.weighted_sample_power - 2.0 * f64::from(cross)
            + weight * f64::from(model.norm_sqr());
        total += contribution.max(0.0);
    }
    total
}

fn undirected_collapsed_count(stats: &HashMap<(i32, i32), SolveEdgeStats>) -> usize {
    stats
        .iter()
        .filter_map(|(&(antenna1, antenna2), stat)| {
            (antenna1 < antenna2).then_some(stat.collapsed_count)
        })
        .sum()
}

fn param_errors(hessian: &HashMap<i32, f32>, reduced_chi_square: f64) -> HashMap<i32, f32> {
    hessian
        .iter()
        .map(|(antenna_id, hessian)| {
            let value = if *hessian > 0.0 && reduced_chi_square.is_finite() {
                (2.0 * reduced_chi_square / f64::from(*hessian)).sqrt() as f32
            } else {
                0.0
            };
            (*antenna_id, value)
        })
        .collect()
}

fn solve_log_amplitudes(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    antenna_ids: &BTreeSet<i32>,
    reachable: &BTreeSet<i32>,
) -> HashMap<i32, f32> {
    let mut log_amplitudes = antenna_ids
        .iter()
        .copied()
        .map(|antenna_id| (antenna_id, 0.0_f32))
        .collect::<HashMap<_, _>>();

    for _ in 0..256 {
        let mut max_delta = 0.0_f32;
        for antenna_id in antenna_ids.iter().copied() {
            if !reachable.contains(&antenna_id) {
                continue;
            }
            let mut numerator = 0.0_f32;
            let mut denominator = 0.0_f32;
            for other_id in antenna_ids.iter().copied() {
                if antenna_id == other_id || !reachable.contains(&other_id) {
                    continue;
                }
                let weight = weights
                    .get(&(antenna_id, other_id))
                    .copied()
                    .unwrap_or_default();
                if weight <= f32::EPSILON {
                    continue;
                }
                let Some(edge) = graph.get(&(antenna_id, other_id)) else {
                    continue;
                };
                let edge_amplitude = (*edge / Complex32::new(weight, 0.0)).norm();
                if edge_amplitude <= f32::EPSILON {
                    continue;
                }
                numerator += weight * (edge_amplitude.ln() - log_amplitudes[&other_id]);
                denominator += weight;
            }
            if denominator <= f32::EPSILON {
                continue;
            }
            let updated = numerator / denominator;
            max_delta = max_delta.max((updated - log_amplitudes[&antenna_id]).abs());
            log_amplitudes.insert(antenna_id, updated);
        }
        if max_delta <= 1.0e-6 {
            break;
        }
    }

    log_amplitudes
        .into_iter()
        .map(|(antenna_id, log_amplitude)| (antenna_id, log_amplitude.exp()))
        .collect()
}

fn rereference_gains(
    gains: &mut HashMap<i32, Complex32>,
    refant_id: i32,
    solve_mode: GainSolveMode,
) {
    let Some(ref_gain) = gains.get(&refant_id).copied() else {
        gains.insert(refant_id, Complex32::new(1.0, 0.0));
        return;
    };
    let ref_norm = ref_gain.norm();
    if ref_norm <= f32::EPSILON {
        gains.insert(refant_id, Complex32::new(1.0, 0.0));
        return;
    }
    let anchor = match solve_mode {
        GainSolveMode::Phase | GainSolveMode::AmplitudePhase => {
            ref_gain / Complex32::new(ref_norm, 0.0)
        }
    };
    for gain in gains.values_mut() {
        *gain /= anchor;
    }
    let anchored_ref = match solve_mode {
        GainSolveMode::Phase => Complex32::new(1.0, 0.0),
        GainSolveMode::AmplitudePhase => Complex32::new(ref_norm, 0.0),
    };
    gains.insert(refant_id, anchored_ref);
}

pub(crate) fn accumulate_edge(
    graph: &mut HashMap<(i32, i32), Complex32>,
    weights: &mut HashMap<(i32, i32), f32>,
    antenna1: i32,
    antenna2: i32,
    weight: f32,
    normalized: Complex32,
) {
    graph
        .entry((antenna1, antenna2))
        .and_modify(|value| *value += normalized)
        .or_insert(normalized);
    weights
        .entry((antenna1, antenna2))
        .and_modify(|value| *value += weight)
        .or_insert(weight);
    graph
        .entry((antenna2, antenna1))
        .and_modify(|value| *value += normalized.conj())
        .or_insert(normalized.conj());
    weights
        .entry((antenna2, antenna1))
        .and_modify(|value| *value += weight)
        .or_insert(weight);
}

pub(crate) fn accumulate_edge_with_stats(
    graph: &mut HashMap<(i32, i32), Complex32>,
    weights: &mut HashMap<(i32, i32), f32>,
    stats: &mut HashMap<(i32, i32), SolveEdgeStats>,
    baseline: (i32, i32),
    weight: f32,
    normalized: Complex32,
    edge_stats: SolveEdgeStats,
) {
    let (antenna1, antenna2) = baseline;
    accumulate_edge(graph, weights, antenna1, antenna2, weight, normalized);
    stats
        .entry((antenna1, antenna2))
        .and_modify(|value| {
            value.weighted_sample_power += edge_stats.weighted_sample_power;
            value.raw_weighted_sample_power += edge_stats.raw_weighted_sample_power;
            value.sample_count += edge_stats.sample_count;
            value.collapsed_count += edge_stats.collapsed_count;
        })
        .or_insert(edge_stats);
    stats
        .entry((antenna2, antenna1))
        .and_modify(|value| {
            value.weighted_sample_power += edge_stats.weighted_sample_power;
            value.raw_weighted_sample_power += edge_stats.raw_weighted_sample_power;
            value.sample_count += edge_stats.sample_count;
            value.collapsed_count += edge_stats.collapsed_count;
        })
        .or_insert(edge_stats);
}

fn reachable_antennas(graph: &HashMap<(i32, i32), Complex32>, refant_id: i32) -> BTreeSet<i32> {
    let mut reachable = BTreeSet::new();
    let mut queue = VecDeque::from([refant_id]);
    reachable.insert(refant_id);

    while let Some(antenna_id) = queue.pop_front() {
        for (from, to) in graph.keys() {
            if *from == antenna_id && reachable.insert(*to) {
                queue.push_back(*to);
            }
        }
    }

    reachable
}

fn initial_phases(
    graph: &HashMap<(i32, i32), Complex32>,
    refant_id: i32,
    reachable: &BTreeSet<i32>,
) -> HashMap<i32, f64> {
    let mut phases = HashMap::new();
    let mut queue = VecDeque::from([refant_id]);
    phases.insert(refant_id, 0.0);

    while let Some(antenna_id) = queue.pop_front() {
        let phase = phases[&antenna_id];
        for (from, to) in graph.keys() {
            if *from != antenna_id || !reachable.contains(to) || phases.contains_key(to) {
                continue;
            }
            let measurement = phase_angle(graph[&(*from, *to)]);
            phases.insert(*to, wrap_phase(phase - measurement));
            queue.push_back(*to);
        }
    }

    for antenna_id in reachable {
        phases.entry(*antenna_id).or_insert(0.0);
    }

    phases
}

fn phase_angle(value: Complex32) -> f64 {
    f64::from(value.im).atan2(f64::from(value.re))
}

fn wrap_phase(value: f64) -> f64 {
    let mut wrapped = value;
    while wrapped > PI {
        wrapped -= 2.0 * PI;
    }
    while wrapped <= -PI {
        wrapped += 2.0 * PI;
    }
    wrapped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_edge(
        graph: &mut HashMap<(i32, i32), Complex32>,
        weights: &mut HashMap<(i32, i32), f32>,
        stats: &mut HashMap<(i32, i32), SolveEdgeStats>,
        antenna1: i32,
        antenna2: i32,
        weight: f32,
        value: Complex32,
    ) {
        accumulate_edge_with_stats(
            graph,
            weights,
            stats,
            (antenna1, antenna2),
            weight,
            value * Complex32::new(weight, 0.0),
            SolveEdgeStats {
                weighted_sample_power: f64::from(weight) * f64::from(value.norm_sqr()),
                raw_weighted_sample_power: f64::from(weight) * f64::from(value.norm_sqr()),
                sample_count: 1,
                collapsed_count: 1,
            },
        );
    }

    #[test]
    fn phase_solve_errors_use_pre_phase_only_gain_amplitudes() {
        let mut graph = HashMap::new();
        let mut weights = HashMap::new();
        let mut stats = HashMap::new();
        let value = Complex32::new(0.5, 0.0);

        add_edge(&mut graph, &mut weights, &mut stats, 0, 1, 1.0, value);
        add_edge(&mut graph, &mut weights, &mut stats, 0, 2, 1.0, value);
        add_edge(&mut graph, &mut weights, &mut stats, 1, 2, 1.0, value);

        let solved = solve_graph(
            &graph,
            &weights,
            &stats,
            GainSolveMode::Phase,
            SolveGraphOptions {
                refant_id: 0,
                min_baselines_per_antenna: 0,
            },
        )
        .expect("solve phase graph");

        for gain in solved.gains.values() {
            assert!((gain.norm() - 1.0).abs() < 1.0e-5);
        }
        assert!(
            solved.hessian.values().any(|value| *value < 1.9),
            "phase-only diagnostics should use pre-normalization amplitudes, not unit gains"
        );
        assert!(solved.param_error.values().all(|value| value.is_finite()));
    }

    #[test]
    fn solve_group_flags_disconnected_antennas_instead_of_failing() {
        let mut graph = HashMap::new();
        let mut weights = HashMap::new();
        let mut stats = HashMap::new();

        add_edge(
            &mut graph,
            &mut weights,
            &mut stats,
            0,
            1,
            1.0,
            Complex32::new(1.0, 0.0),
        );
        add_edge(
            &mut graph,
            &mut weights,
            &mut stats,
            2,
            3,
            1.0,
            Complex32::new(1.0, 0.0),
        );

        let mut group = SolveAccumulator::new(12, 0, 0);
        group.min_time = 1.0;
        group.max_time = 1.0;
        group.unique_time_sum = 1.0;
        group.unique_time_count = 1;
        group.time_centroid_weighted_sum = 1.0;
        group.time_centroid_weight = 1.0;
        group.total_interval = 1.0;
        group.sample_rows = 1;
        group.scan_numbers.insert(9);
        group.antenna_ids.extend([0, 1, 2, 3]);
        group.receptor_graphs = vec![graph];
        group.receptor_weights = vec![weights];
        group.receptor_stats = vec![stats];

        let rows = solve_group(
            group,
            &BTreeSet::from([0, 1, 2, 3]),
            GainType::T,
            GainSolveMode::Phase,
            0,
            0.0,
            0,
        )
        .expect("solve should emit flagged rows for the disconnected component");

        let row_for = |antenna_id| {
            rows.iter()
                .find(|row| row.antenna_id == antenna_id)
                .expect("solution row")
        };
        assert!(!row_for(0).flags[0]);
        assert!(!row_for(1).flags[0]);
        assert!(row_for(2).flags[0]);
        assert!(row_for(3).flags[0]);
    }
}
