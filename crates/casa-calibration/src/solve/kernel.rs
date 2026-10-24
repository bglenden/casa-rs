// SPDX-License-Identifier: LGPL-3.0-or-later
//! Numerical solve kernel for `gaincal` and `bandpass`.
//!
//! Every solve is CASA's `VisCalSolver2::solve`: a Levenberg–Marquardt
//! minimisation of the weighted complex residual Σ w |V − g_i conj(g_j)|²
//! over antenna gains, with every receptor of one solution solved together
//! and a first guess from `GJones::guessPar` or `TJones::guessPar`. A
//! phase-only solve fits data already reduced to unit amplitude
//! (`SolveDataBuffer::enforceAPonData`) and normalises the gains afterwards,
//! as `SolvableVisCal::enforceAPonSoln` does; an amplitude-and-phase solve
//! keeps them. Within one solution the visibilities of each baseline are
//! collapsed to their weighted sum, which leaves the chi-square unchanged up
//! to a constant because the gains are constant over the solution.

use std::collections::{BTreeSet, HashMap, VecDeque};

use casa_types::Complex32;
use num_complex::Complex64;

use super::{GainSolveMode, GainType};
use crate::solve::grouping::{SolveAccumulator, SolveEdgeStats};
use crate::solve::trace;

/// `VisCalSolver2::maxIter_`: a solve that has not converged after this many
/// iterations fails.
const MAX_ITERATIONS: usize = 50;

#[derive(Debug, Clone)]
pub(crate) struct SolutionRow {
    pub(crate) time_seconds: f64,
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

/// One receptor's collapsed visibilities for one solution. For each directed
/// baseline (i, j) `edges` holds the weighted sum Σ w V_ij of its samples and
/// `weights` the weight sum Σ w; (j, i) holds the conjugate sum. `stats`
/// carries what the absolute chi-square needs (Σ w |V|²) and the sample
/// counts behind the parameter errors.
#[derive(Debug, Clone, Default)]
pub(crate) struct SolveGraph {
    pub(crate) edges: HashMap<(i32, i32), Complex32>,
    pub(crate) weights: HashMap<(i32, i32), f32>,
    pub(crate) stats: HashMap<(i32, i32), SolveEdgeStats>,
}

impl SolveGraph {
    /// Add one collapsed sample of baseline `(antenna1, antenna2)`: its
    /// weight, its weighted visibility and its statistics.
    pub(crate) fn accumulate(
        &mut self,
        (antenna1, antenna2): (i32, i32),
        weight: f32,
        weighted_sample: Complex32,
        edge_stats: SolveEdgeStats,
    ) {
        for (baseline, value) in [
            ((antenna1, antenna2), weighted_sample),
            ((antenna2, antenna1), weighted_sample.conj()),
        ] {
            *self.edges.entry(baseline).or_default() += value;
            *self.weights.entry(baseline).or_default() += weight;
            self.stats
                .entry(baseline)
                .and_modify(|stats| stats.add(edge_stats))
                .or_insert(edge_stats);
        }
    }

    fn weight(&self, baseline: (i32, i32)) -> f32 {
        self.weights.get(&baseline).copied().unwrap_or_default()
    }

    /// The baselines with positive weight between antennas of `antennas`.
    fn restricted_to(&self, antennas: &BTreeSet<i32>) -> Self {
        let keep = |&(antenna1, antenna2): &(i32, i32)| {
            antennas.contains(&antenna1)
                && antennas.contains(&antenna2)
                && self.weight((antenna1, antenna2)) > f32::EPSILON
        };
        Self {
            edges: self
                .edges
                .iter()
                .filter(|(baseline, _)| keep(baseline))
                .map(|(baseline, value)| (*baseline, *value))
                .collect(),
            weights: self
                .weights
                .iter()
                .filter(|(baseline, _)| keep(baseline))
                .map(|(baseline, value)| (*baseline, *value))
                .collect(),
            stats: self
                .stats
                .iter()
                .filter(|(baseline, _)| keep(baseline))
                .map(|(baseline, value)| (*baseline, *value))
                .collect(),
        }
    }

    /// Antennas with at least one positive-weight baseline.
    fn antennas(&self) -> BTreeSet<i32> {
        self.weights
            .iter()
            .filter(|((antenna1, antenna2), weight)| {
                antenna1 != antenna2 && **weight > f32::EPSILON
            })
            .map(|((antenna1, _), _)| *antenna1)
            .collect()
    }
}

/// One receptor of a converged solve.
#[derive(Debug, Clone)]
pub(crate) struct SolvedReceptor {
    /// Gains referred to the solve's reference antenna; phase-only solves
    /// have unit amplitudes.
    pub(crate) gains: HashMap<i32, Complex32>,
    /// Antennas with a solution: they kept enough baselines and connect to
    /// the reference antenna through baselines with data.
    pub(crate) solved: BTreeSet<i32>,
    pub(crate) hessian: HashMap<i32, f32>,
    pub(crate) param_error: HashMap<i32, f32>,
    pub(crate) snr: HashMap<i32, f32>,
    error_chi_square: f64,
    sample_count: usize,
    good_count: usize,
    snr_amplitude: HashMap<i32, f32>,
}

/// A converged solve of one solution's receptor graphs.
#[derive(Debug, Clone)]
pub(crate) struct SolvedGraphs {
    /// The antenna the gains are referred to: the requested one if it kept
    /// baselines, otherwise the lowest-numbered antenna that did.
    pub(crate) refant_id: i32,
    pub(crate) receptors: Vec<SolvedReceptor>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveGraphOptions {
    pub(crate) refant_id: i32,
    pub(crate) min_baselines_per_antenna: usize,
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

/// Solve one `gaincal` solution interval, or `None` where CASA keeps no
/// solution for it (see [`solve_graphs`]). Every antenna of the array or
/// the interval gets a row; antennas without a solution are flagged, as are
/// solutions at or below `min_snr`.
pub(crate) fn solve_group(
    group: SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    gain_type: GainType,
    solve_mode: GainSolveMode,
    refant_id: i32,
    min_snr: f32,
    min_baselines_per_antenna: usize,
) -> Option<Vec<SolutionRow>> {
    let scan_number = *group.scan_numbers.iter().next().unwrap_or(&0);
    let graphs = group.channel_graphs.first()?;
    let solved = solve_graphs(
        graphs,
        gain_type,
        solve_mode,
        SolveGraphOptions {
            refant_id,
            min_baselines_per_antenna,
        },
        None,
    )?;

    let mut antenna_ids = available_antennas.clone();
    antenna_ids.extend(group.antenna_ids.iter().copied());
    antenna_ids.insert(refant_id);
    let receptors = match gain_type {
        GainType::G => &solved.receptors[..],
        GainType::T => &solved.receptors[..1],
    };
    Some(
        antenna_ids
            .into_iter()
            .map(|antenna_id| {
                let mut row = SolutionRow {
                    time_seconds: group.solution_time,
                    field_id: group.field_id,
                    spw_id: group.spw_id,
                    antenna_id,
                    scan_number,
                    observation_id: group.observation_id,
                    refant_id: solved.refant_id,
                    gains: Vec::with_capacity(receptors.len()),
                    flags: Vec::with_capacity(receptors.len()),
                    param_errors: Vec::with_capacity(receptors.len()),
                    snrs: Vec::with_capacity(receptors.len()),
                    weights: Vec::with_capacity(receptors.len()),
                };
                for receptor in receptors {
                    let snr = receptor.snr.get(&antenna_id).copied().unwrap_or_default();
                    let param_error = receptor
                        .param_error
                        .get(&antenna_id)
                        .copied()
                        .unwrap_or_default();
                    let flagged = !receptor.solved.contains(&antenna_id)
                        || snr <= min_snr
                        || !param_error.is_finite();
                    row.gains.push(if flagged {
                        Complex32::new(1.0, 0.0)
                    } else {
                        receptor.gains[&antenna_id]
                    });
                    row.flags.push(flagged);
                    row.param_errors.push(param_error);
                    row.snrs.push(snr);
                    row.weights.push(
                        receptor
                            .hessian
                            .get(&antenna_id)
                            .copied()
                            .unwrap_or_default(),
                    );
                }
                row
            })
            .collect(),
    )
}

/// Solve the receptor graphs of one solution together, as
/// `VisCalSolver2::solve` does, or `None` where CASA records no solution:
/// no antenna keeps `min_baselines_per_antenna` baselines with data
/// (`SolvableVisCal::verifyConstraints`), or the solve has not converged
/// after [`MAX_ITERATIONS`]. CASA also fails a solve whose chi-square is
/// exactly zero; here an exact fit is a solution (`docs/CASA (C++) bugs.md`).
///
/// `start`, when given, seeds the antennas it solved in place of the first
/// guess, as CASA's B solve starts each channel from the one solved before.
pub(crate) fn solve_graphs(
    graphs: &[SolveGraph],
    gain_type: GainType,
    solve_mode: GainSolveMode,
    options: SolveGraphOptions,
    start: Option<&SolvedGraphs>,
) -> Option<SolvedGraphs> {
    let constrained = baseline_constrained_antennas(graphs, options.min_baselines_per_antenna);
    let refant_id = if constrained.contains(&options.refant_id) {
        options.refant_id
    } else {
        *constrained.first()?
    };
    let active = graphs
        .iter()
        .map(|graph| graph.restricted_to(&constrained))
        .collect::<Vec<_>>();
    let solved_antennas = active
        .iter()
        .map(|graph| {
            let with_data = graph.antennas();
            reachable_antennas(graph, refant_id)
                .into_iter()
                .filter(|antenna_id| with_data.contains(antenna_id))
                .collect::<BTreeSet<_>>()
        })
        .collect::<Vec<_>>();
    let mut first = first_guess(&active, &solved_antennas, gain_type, refant_id);
    if let Some(start) = start {
        for (guess, receptor) in first.iter_mut().zip(&start.receptors) {
            for (antenna_id, value) in guess.iter_mut() {
                if receptor.solved.contains(antenna_id) {
                    *value = complex32_to_64(receptor.gains[antenna_id]);
                }
            }
        }
    }
    let gains = solve_lm(&active, &solved_antennas, first, refant_id)?;
    let mut receptors = active
        .iter()
        .zip(solved_antennas)
        .zip(gains)
        .map(|((graph, solved), gains)| {
            finish_receptor(graph, solved, gains, solve_mode, refant_id)
        })
        .collect::<Vec<_>>();
    apply_group_reduced_chi_square(&mut receptors);
    Some(SolvedGraphs {
        refant_id,
        receptors,
    })
}

/// The antennas that keep at least `min_baselines_per_antenna` baselines
/// with data once antennas below the threshold, and their baselines, are
/// removed in turn (`SolvableVisCal::verifyConstraints`). A baseline counts
/// when any receptor has weight on it.
fn baseline_constrained_antennas(
    graphs: &[SolveGraph],
    min_baselines_per_antenna: usize,
) -> BTreeSet<i32> {
    let baselines = graphs
        .iter()
        .flat_map(|graph| graph.weights.iter())
        .filter(|((antenna1, antenna2), weight)| antenna1 < antenna2 && **weight > f32::EPSILON)
        .map(|(baseline, _)| *baseline)
        .collect::<BTreeSet<_>>();
    let mut active = baselines
        .iter()
        .flat_map(|&(antenna1, antenna2)| [antenna1, antenna2])
        .collect::<BTreeSet<_>>();
    loop {
        let mut counts = HashMap::<i32, usize>::new();
        for &(antenna1, antenna2) in &baselines {
            if active.contains(&antenna1) && active.contains(&antenna2) {
                *counts.entry(antenna1).or_default() += 1;
                *counts.entry(antenna2).or_default() += 1;
            }
        }
        let before = active.len();
        active.retain(|antenna_id| {
            counts.get(antenna_id).copied().unwrap_or_default() >= min_baselines_per_antenna.max(1)
        });
        if active.len() == before {
            return active;
        }
    }
}

/// CASA's first guess (`GJones::guessPar`, `TJones::guessPar`): each antenna
/// starts from its baseline to the reference antenna, conjugated (G) or as a
/// unit phasor (T), divided by the square root of those baselines' mean
/// amplitude, which is also the reference antenna's start. Antennas without
/// a baseline to the reference start there too. Phase-only data have unit
/// amplitude, so the guess is then the baselines' phases.
fn first_guess(
    graphs: &[SolveGraph],
    antennas: &[BTreeSet<i32>],
    gain_type: GainType,
    refant_id: i32,
) -> Vec<HashMap<i32, Complex64>> {
    let mut amplitude_sum = 0.0_f64;
    let mut amplitude_count = 0_usize;
    let mut guesses = graphs
        .iter()
        .zip(antennas)
        .map(|(graph, antennas)| {
            antennas
                .iter()
                .filter(|antenna_id| **antenna_id != refant_id)
                .filter_map(|&antenna_id| {
                    let weight = f64::from(graph.weight((refant_id, antenna_id)));
                    let edge = graph.edges.get(&(refant_id, antenna_id))?;
                    let mean = complex32_to_64(*edge) / weight;
                    let amplitude = mean.norm();
                    (weight > f64::EPSILON && amplitude > 0.0).then(|| {
                        amplitude_sum += amplitude;
                        amplitude_count += 1;
                        let guess = match gain_type {
                            GainType::G => mean.conj(),
                            GainType::T => mean.conj() / amplitude,
                        };
                        (antenna_id, guess)
                    })
                })
                .collect::<HashMap<_, _>>()
        })
        .collect::<Vec<_>>();
    let scale = if amplitude_count > 0 {
        (amplitude_sum / amplitude_count as f64).sqrt()
    } else {
        1.0
    };
    for (guess, antennas) in guesses.iter_mut().zip(antennas) {
        for value in guess.values_mut() {
            *value /= scale;
        }
        for antenna_id in antennas {
            guess
                .entry(*antenna_id)
                .or_insert(Complex64::new(scale, 0.0));
        }
        if antennas.contains(&refant_id) {
            guess.insert(refant_id, Complex64::new(scale, 0.0));
        }
    }
    guesses
}

/// The Levenberg–Marquardt iteration of `VisCalSolver2::solve`, with its
/// step size optimisation (`optStepSize2`) and convergence test
/// (`converged`): a step counts towards convergence when chi-square has not
/// risen by more than 0.1% and changed by less than 10%, and six such steps
/// converge. `None` when [`MAX_ITERATIONS`] pass without converging.
fn solve_lm(
    graphs: &[SolveGraph],
    antennas: &[BTreeSet<i32>],
    mut gains: Vec<HashMap<i32, Complex64>>,
    refant_id: i32,
) -> Option<Vec<HashMap<i32, Complex64>>> {
    let mut last_gains = gains.clone();
    let mut last_chi_square = f64::MAX;
    let mut convergence_count = 0_i32;

    for iteration in 0..=MAX_ITERATIONS {
        let mut chi_square = chi_square_graphs(graphs, &gains);
        if chi_square <= 0.0 {
            return Some(gains);
        }
        let delta_chi_square = chi_square - last_chi_square;
        let fractional_delta = delta_chi_square / chi_square;
        if fractional_delta <= 0.001 {
            if delta_chi_square.abs() < 0.1 * chi_square {
                convergence_count += 1;
            }
            if convergence_count > 5 {
                return Some(gains);
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
            chi_square = chi_square_graphs(graphs, &gains);
        }

        let mut gradients = Vec::with_capacity(graphs.len());
        let mut deltas = Vec::with_capacity(graphs.len());
        for ((graph, antennas), receptor_gains) in graphs.iter().zip(antennas).zip(&gains) {
            let (gradient, hessian) = gradient_hessian(graph, antennas, receptor_gains);
            let delta = antennas
                .iter()
                .map(|antenna_id| {
                    let hessian = hessian.get(antenna_id).copied().unwrap_or_default();
                    let value = if hessian > f64::EPSILON {
                        -gradient[antenna_id] / hessian / 2.0
                    } else {
                        Complex64::new(0.0, 0.0)
                    };
                    (*antenna_id, value)
                })
                .collect::<HashMap<_, _>>();
            gradients.push((gradient, hessian));
            deltas.push(delta);
        }

        last_gains.clone_from(&gains);
        let step = optimize_step(graphs, &last_gains, &mut gains, &mut deltas, chi_square);
        for (receptor_gains, receptor_delta) in gains.iter_mut().zip(&deltas) {
            for (antenna_id, gain) in receptor_gains.iter_mut() {
                *gain += receptor_delta.get(antenna_id).copied().unwrap_or_default();
            }
        }
        for (receptor, (antennas, (gradient, hessian))) in
            antennas.iter().zip(&gradients).enumerate()
        {
            trace::trace_phase_solver_iteration(PhaseSolverTraceEvent {
                iteration,
                refant_id,
                chi_square,
                last_chi_square,
                delta_chi_square,
                fractional_delta,
                convergence_count,
                step: &step,
                antenna_ids: antennas,
                last_gains: &last_gains[receptor],
                gains: &gains[receptor],
                gradient,
                hessian,
                delta: &deltas[receptor],
            });
        }
    }
    None
}

/// Refer one receptor's converged gains to the reference antenna's phase and
/// derive its diagnostics. Errors and SNRs come from the solved gains, before
/// a phase-only solve normalises them (`SolvableVisCal::formSolveSNR` runs
/// before `enforceAPonSoln`).
fn finish_receptor(
    graph: &SolveGraph,
    solved: BTreeSet<i32>,
    mut gains: HashMap<i32, Complex64>,
    solve_mode: GainSolveMode,
    refant_id: i32,
) -> SolvedReceptor {
    rereference_complex64(&mut gains, refant_id, false);
    let error_gains = gains
        .iter()
        .map(|(antenna_id, gain)| (*antenna_id, complex64_to_32(*gain)))
        .collect::<HashMap<_, _>>();
    let snr_amplitude = gains
        .iter()
        .map(|(antenna_id, gain)| (*antenna_id, gain.norm() as f32))
        .collect::<HashMap<_, _>>();
    if solve_mode == GainSolveMode::Phase {
        rereference_complex64(&mut gains, refant_id, true);
    }
    let gains = gains
        .into_iter()
        .map(|(antenna_id, gain)| (antenna_id, complex64_to_32(gain)))
        .collect::<HashMap<_, _>>();

    let hessian = hessian(graph, &error_gains, &solved);
    let error_chi_square = chi_square_for_errors(graph, &error_gains);
    let good_count = solved.len();
    let sample_count = undirected_collapsed_count(&graph.stats);
    let degrees_of_freedom = (2 * sample_count.saturating_sub(good_count)).max(1);
    let param_error = param_errors(&hessian, error_chi_square / degrees_of_freedom as f64);
    let snr = snrs(&gains, &solved, &param_error, &snr_amplitude);

    SolvedReceptor {
        gains,
        solved,
        hessian,
        param_error,
        snr,
        error_chi_square,
        sample_count,
        good_count,
        snr_amplitude,
    }
}

/// Errors from the reduced chi-square of all receptors together, as one
/// `VisCalSolver2::getErrors` covers every parameter of the solve.
fn apply_group_reduced_chi_square(solved: &mut [SolvedReceptor]) {
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
            &per_receptor.solved,
            &per_receptor.param_error,
            &per_receptor.snr_amplitude,
        );
    }
}

fn snrs(
    gains: &HashMap<i32, Complex32>,
    solved: &BTreeSet<i32>,
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
            } else if solved.contains(antenna_id) {
                9_999_999.0
            } else {
                0.0
            };
            (*antenna_id, value)
        })
        .collect()
}

/// `VisCalSolver2::optStepSize2`: scale the Gauss–Newton step by trying it,
/// doubling it while chi-square falls or halving it until it does, then
/// taking the vertex of the parabola through the last three chi-squares.
/// `gains` returns equal to `last_gains`; `deltas` returns scaled.
fn optimize_step(
    graphs: &[SolveGraph],
    last_gains: &[HashMap<i32, Complex64>],
    gains: &mut [HashMap<i32, Complex64>],
    deltas: &mut [HashMap<i32, Complex64>],
    current_chi_square: f64,
) -> PhaseStepTrace {
    let mut step = 1.0_f64;
    let mut iterations = 0_usize;
    let x0 = current_chi_square;
    apply_step(gains, last_gains, deltas, 1.0);
    let mut x1 = chi_square_graphs(graphs, gains);
    let expanded;

    let x2 = if x1 < x0 {
        expanded = true;
        apply_step(gains, last_gains, deltas, 2.0 * step);
        let mut trial_x2 = chi_square_graphs(graphs, gains);
        while trial_x2 < x1 {
            iterations += 1;
            step *= 2.0;
            x1 = trial_x2;
            apply_step(gains, last_gains, deltas, 2.0 * step);
            trial_x2 = chi_square_graphs(graphs, gains);
        }
        trial_x2
    } else {
        expanded = false;
        step *= 0.5;
        apply_step(gains, last_gains, deltas, step);
        let mut trial_x2 = x1;
        x1 = chi_square_graphs(graphs, gains);
        while x1 > x0 {
            iterations += 1;
            step *= 0.5;
            trial_x2 = x1;
            apply_step(gains, last_gains, deltas, step);
            x1 = chi_square_graphs(graphs, gains);
        }
        trial_x2
    };

    let denominator = x0 - 2.0 * x1 + x2;
    let opt_factor = if denominator.abs() > 0.0 {
        step * (1.5 - (x2 - x1) / denominator)
    } else {
        0.0
    };

    for (gain, last_gain) in gains.iter_mut().zip(last_gains) {
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
    gains: &mut [HashMap<i32, Complex64>],
    bases: &[HashMap<i32, Complex64>],
    deltas: &[HashMap<i32, Complex64>],
    factor: f64,
) {
    for ((receptor_gains, base), delta) in gains.iter_mut().zip(bases).zip(deltas) {
        for (antenna_id, gain) in receptor_gains.iter_mut() {
            *gain = base[antenna_id] + factor * delta.get(antenna_id).copied().unwrap_or_default();
        }
    }
}

fn chi_square_graphs(graphs: &[SolveGraph], gains: &[HashMap<i32, Complex64>]) -> f64 {
    graphs
        .iter()
        .zip(gains)
        .map(|(graph, gains)| chi_square_complex64(graph, gains))
        .sum()
}

fn chi_square_complex64(graph: &SolveGraph, gains: &HashMap<i32, Complex64>) -> f64 {
    let mut total = 0.0_f64;
    for (&(antenna1, antenna2), stat) in &graph.stats {
        if antenna1 > antenna2 {
            continue;
        }
        let weight = f64::from(graph.weight((antenna1, antenna2)));
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
        let observed = graph
            .edges
            .get(&(antenna1, antenna2))
            .copied()
            .map(complex32_to_64)
            .unwrap_or_default();
        let model = gain1 * gain2.conj();
        let cross = (model.conj() * observed).re;
        let contribution = stat.weighted_sample_power - 2.0 * cross + weight * model.norm_sqr();
        total += contribution.max(0.0);
    }
    total
}

/// The chi-square's gradient and diagonal Hessian with respect to each gain
/// of `antennas` (`VisCalSolver2::accGradHess2`).
fn gradient_hessian(
    graph: &SolveGraph,
    antennas: &BTreeSet<i32>,
    gains: &HashMap<i32, Complex64>,
) -> (HashMap<i32, Complex64>, HashMap<i32, f64>) {
    let mut gradient = antennas
        .iter()
        .map(|antenna_id| (*antenna_id, Complex64::new(0.0, 0.0)))
        .collect::<HashMap<_, _>>();
    let mut hessian = antennas
        .iter()
        .map(|antenna_id| (*antenna_id, 0.0_f64))
        .collect::<HashMap<_, _>>();

    for (&(antenna1, antenna2), edge) in &graph.edges {
        if antenna1 > antenna2 {
            continue;
        }
        let weight = f64::from(graph.weight((antenna1, antenna2)));
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
        let residual_sum = weight * gain1 * gain2.conj() - complex32_to_64(*edge);
        let deriv1 = gain2.conj();
        let deriv2 = gain1;
        if let Some(value) = gradient.get_mut(&antenna1) {
            *value += residual_sum * deriv1.conj();
            *hessian.entry(antenna1).or_default() += weight * deriv1.norm_sqr();
        }
        if let Some(value) = gradient.get_mut(&antenna2) {
            *value += deriv2 * residual_sum.conj();
            *hessian.entry(antenna2).or_default() += weight * deriv2.norm_sqr();
        }
    }

    (gradient, hessian)
}

/// Rotate every gain by the reference antenna's phase; with `phase_only`
/// also normalise each to unit amplitude.
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
        if let Some(gain) = gains.get_mut(&refant_id) {
            *gain = Complex64::new(1.0, 0.0);
        }
    }
}

fn complex32_to_64(value: Complex32) -> Complex64 {
    Complex64::new(f64::from(value.re), f64::from(value.im))
}

fn complex64_to_32(value: Complex64) -> Complex32 {
    Complex32::new(value.re as f32, value.im as f32)
}

fn hessian(
    graph: &SolveGraph,
    gains: &HashMap<i32, Complex32>,
    solved: &BTreeSet<i32>,
) -> HashMap<i32, f32> {
    let mut hessian = HashMap::new();
    for &(antenna_id, other_id) in graph.edges.keys() {
        if !solved.contains(&antenna_id) || !solved.contains(&other_id) {
            continue;
        }
        let weight = graph.weight((antenna_id, other_id));
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

fn chi_square_for_errors(graph: &SolveGraph, gains: &HashMap<i32, Complex32>) -> f64 {
    let mut total = 0.0_f64;
    for (&(antenna1, antenna2), stat) in &graph.stats {
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
            .edges
            .get(&(antenna1, antenna2))
            .copied()
            .unwrap_or_default();
        let weight = f64::from(graph.weight((antenna1, antenna2)));
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

fn reachable_antennas(graph: &SolveGraph, refant_id: i32) -> BTreeSet<i32> {
    let mut reachable = BTreeSet::from([refant_id]);
    let mut queue = VecDeque::from([refant_id]);
    while let Some(antenna_id) = queue.pop_front() {
        for (from, to) in graph.edges.keys() {
            if *from == antenna_id && reachable.insert(*to) {
                queue.push_back(*to);
            }
        }
    }
    reachable
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_edge(
        graph: &mut SolveGraph,
        antenna1: i32,
        antenna2: i32,
        weight: f32,
        value: Complex32,
    ) {
        graph.accumulate(
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

    fn options(min_baselines_per_antenna: usize) -> SolveGraphOptions {
        SolveGraphOptions {
            refant_id: 0,
            min_baselines_per_antenna,
        }
    }

    /// Visibilities g_i conj(g_j) of four antennas, plus `outlier` on 0-1.
    fn four_antenna_graph(gains: &[Complex32; 4], outlier: Complex32) -> SolveGraph {
        let mut graph = SolveGraph::default();
        for antenna1 in 0..4 {
            for antenna2 in antenna1 + 1..4 {
                let mut value = gains[antenna1 as usize] * gains[antenna2 as usize].conj();
                if (antenna1, antenna2) == (0, 1) {
                    value *= outlier;
                }
                add_edge(&mut graph, antenna1, antenna2, 1.0, value);
            }
        }
        graph
    }

    #[test]
    fn phase_solve_errors_use_pre_phase_only_gain_amplitudes() {
        let mut graph = SolveGraph::default();
        let value = Complex32::new(0.5, 0.0);
        add_edge(&mut graph, 0, 1, 1.0, value);
        add_edge(&mut graph, 0, 2, 1.0, value);
        add_edge(&mut graph, 1, 2, 1.0, value);

        let solved = solve_graphs(
            &[graph],
            GainType::T,
            GainSolveMode::Phase,
            options(0),
            None,
        )
        .expect("solve phase graph");
        let receptor = &solved.receptors[0];

        for gain in receptor.gains.values() {
            assert!((gain.norm() - 1.0).abs() < 1.0e-5);
        }
        assert!(
            receptor.hessian.values().any(|value| *value < 1.9),
            "phase-only diagnostics should use pre-normalization amplitudes, not unit gains"
        );
        assert!(receptor.param_error.values().all(|value| value.is_finite()));
    }

    const TRUTH: [Complex32; 4] = [
        Complex32::new(1.0, 0.0),
        Complex32::new(1.105_445_3, 0.467_302_7),
        Complex32::new(0.611_872_9, -0.515_386_2),
        Complex32::new(-0.355_853_6, 1.040_83),
    ];

    fn solve_amplitude_phase(graph: &SolveGraph) -> HashMap<i32, Complex64> {
        let solved = solve_graphs(
            std::slice::from_ref(graph),
            GainType::G,
            GainSolveMode::AmplitudePhase,
            options(0),
            None,
        )
        .expect("solve amplitude-phase graph");
        solved.receptors[0]
            .gains
            .iter()
            .map(|(antenna_id, gain)| (*antenna_id, complex32_to_64(*gain)))
            .collect()
    }

    #[test]
    fn amplitude_phase_solve_recovers_closing_gains() {
        let gains = solve_amplitude_phase(&four_antenna_graph(&TRUTH, Complex32::new(1.0, 0.0)));
        for (antenna_id, truth) in TRUTH.iter().enumerate() {
            let solved = complex64_to_32(gains[&(antenna_id as i32)]);
            assert!(
                (solved - truth).norm() < 1.0e-4,
                "antenna {antenna_id}: {solved} != {truth}"
            );
        }
    }

    #[test]
    fn amplitude_phase_solve_approaches_the_complex_least_squares_minimum() {
        // A non-closing outlier on baseline 0-1. The minimum of
        // Σ w |V - g_i conj(g_j)|² comes from coordinate descent run to
        // convergence. CASA's convergence test stops while chi-square still
        // falls by under 10% an iteration, here 3% above the minimum;
        // separate phase and log-amplitude fits end 2.8 times above it.
        let graph = four_antenna_graph(&TRUTH, Complex32::from_polar(3.0, 0.6));
        let solved = solve_amplitude_phase(&graph);
        let mut minimum = solved.clone();
        for _ in 0..20_000 {
            for antenna_id in 0..4 {
                let mut numerator = Complex64::new(0.0, 0.0);
                let mut denominator = 0.0;
                for other_id in (0..4).filter(|other_id| *other_id != antenna_id) {
                    let weight = f64::from(graph.weight((antenna_id, other_id)));
                    numerator +=
                        complex32_to_64(graph.edges[&(antenna_id, other_id)]) * minimum[&other_id];
                    denominator += weight * minimum[&other_id].norm_sqr();
                }
                minimum.insert(antenna_id, numerator / denominator);
            }
        }
        let solved_chi_square = chi_square_complex64(&graph, &solved);
        let minimum_chi_square = chi_square_complex64(&graph, &minimum);
        assert!(
            solved_chi_square <= minimum_chi_square * 1.05,
            "chi-square {solved_chi_square} is not within 5% of the minimum {minimum_chi_square}"
        );
    }

    #[test]
    fn solve_fails_when_no_antenna_keeps_enough_baselines() {
        // A chain 0-1-2-3: with two baselines required, pruning the ends in
        // turn removes every antenna, as `verifyConstraints` does.
        let mut graph = SolveGraph::default();
        for (antenna1, antenna2) in [(0, 1), (1, 2), (2, 3)] {
            add_edge(
                &mut graph,
                antenna1,
                antenna2,
                1.0,
                Complex32::new(1.0, 0.0),
            );
        }
        assert!(
            solve_graphs(
                &[graph.clone()],
                GainType::T,
                GainSolveMode::Phase,
                options(2),
                None
            )
            .is_none()
        );
        assert!(
            solve_graphs(
                &[graph],
                GainType::T,
                GainSolveMode::Phase,
                options(1),
                None
            )
            .is_some()
        );
    }

    #[test]
    fn solve_refers_to_another_antenna_when_the_refant_is_pruned() {
        // Antenna 0 has one baseline; antennas 1-3 form a triangle.
        let mut graph = SolveGraph::default();
        for (antenna1, antenna2) in [(0, 1), (1, 2), (1, 3), (2, 3)] {
            add_edge(
                &mut graph,
                antenna1,
                antenna2,
                1.0,
                Complex32::new(1.0, 0.0),
            );
        }
        let solved = solve_graphs(
            &[graph],
            GainType::T,
            GainSolveMode::Phase,
            options(2),
            None,
        )
        .expect("antennas 1-3 solve");
        assert_eq!(solved.refant_id, 1);
        assert_eq!(solved.receptors[0].solved, BTreeSet::from([1, 2, 3]));
    }

    #[test]
    fn solve_group_flags_disconnected_antennas_instead_of_failing() {
        let mut graph = SolveGraph::default();
        add_edge(&mut graph, 0, 1, 1.0, Complex32::new(1.0, 0.0));
        add_edge(&mut graph, 2, 3, 1.0, Complex32::new(1.0, 0.0));

        let mut group = SolveAccumulator::new(12, 0, 0);
        group.solution_time = 1.0;
        group.scan_numbers.insert(9);
        group.antenna_ids.extend([0, 1, 2, 3]);
        group.channel_graphs = vec![vec![graph]];

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
