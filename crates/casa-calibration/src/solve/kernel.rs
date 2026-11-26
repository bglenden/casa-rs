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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use casa_core::Complex32;
use num_complex::Complex64;

use super::{GainSolveMode, GainSolveRobustness, GainType};
use crate::solve::grouping::{SolveAccumulator, SolveEdgeStats};
use crate::solve::trace;

mod robust;

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
    pub(crate) robust_samples: Vec<RobustSample>,
    fixed_residuals: BTreeMap<i32, (f64, usize)>,
}

/// One unflagged, mode-transformed sample of a gathered buffer.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RobustSample {
    pub(crate) baseline: (i32, i32),
    pub(crate) correlation: usize,
    pub(crate) visibility: Complex32,
    pub(crate) weight: f32,
    /// Fraction of the averaged sample represented by a nonzero model.
    pub(crate) model: f32,
    pub(crate) in_first_buffer: bool,
}

impl SolveGraph {
    pub(crate) fn add_fixed_residual(&mut self, antenna: i32, chi_square: f64) {
        let (sum, count) = self.fixed_residuals.entry(antenna).or_default();
        *sum += chi_square;
        *count += 1;
    }

    fn fixed_chi_square(&self) -> f64 {
        self.fixed_residuals.values().map(|&(sum, _)| sum).sum()
    }

    fn fixed_sample_count(&self) -> usize {
        self.fixed_residuals.values().map(|&(_, count)| count).sum()
    }

    /// Add one collapsed sample of baseline `(antenna1, antenna2)`: its
    /// weight, its weighted visibility and its statistics.
    pub(crate) fn accumulate(
        &mut self,
        (antenna1, antenna2): (i32, i32),
        weight: f32,
        weighted_sample: Complex32,
        edge_stats: SolveEdgeStats,
    ) {
        let mut reverse_stats = edge_stats;
        if let Some(sample) = &mut reverse_stats.single_sample {
            sample.value = sample.value.conj();
        }
        for (baseline, value, stats) in [
            ((antenna1, antenna2), weighted_sample, edge_stats),
            ((antenna2, antenna1), weighted_sample.conj(), reverse_stats),
        ] {
            *self.edges.entry(baseline).or_default() += value;
            *self.weights.entry(baseline).or_default() += weight;
            self.stats
                .entry(baseline)
                .and_modify(|previous| previous.add(stats))
                .or_insert(stats);
        }
    }

    fn weight(&self, baseline: (i32, i32)) -> f32 {
        self.weights.get(&baseline).copied().unwrap_or_default()
    }

    /// The baselines between antennas that meet the solve constraints.
    fn restricted_to(&self, antennas: &BTreeSet<i32>) -> Self {
        let keep = |&(antenna1, antenna2): &(i32, i32)| {
            antennas.contains(&antenna1) && antennas.contains(&antenna2)
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
            fixed_residuals: self
                .fixed_residuals
                .iter()
                .filter(|(antenna, _)| antennas.contains(antenna))
                .map(|(&antenna, &value)| (antenna, value))
                .collect(),
            robust_samples: self
                .robust_samples
                .iter()
                .copied()
                .filter(|sample| keep(&sample.baseline))
                .collect(),
        }
    }

    /// Antennas with at least one positive-weight baseline.
    pub(crate) fn antennas(&self) -> BTreeSet<i32> {
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

pub(crate) enum SolveStart<'a> {
    Guess,
    BandpassGuess {
        scalar: bool,
        independent_input_flags: bool,
    },
    PreviousChannel(Option<&'a SolvedGraphs>),
    LegacyChannel(&'a SolvedGraphs),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveGraphOptions<'a> {
    pub(crate) refant_id: i32,
    pub(crate) min_baselines_per_antenna: usize,
    pub(crate) group_seed: Option<&'a SolveAccumulator>,
    pub(crate) correlation_dependent_flags: bool,
    pub(crate) defer_reference: bool,
    pub(crate) robustness: Option<&'a GainSolveRobustness>,
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
    min_snr: f32,
    options: SolveGraphOptions<'_>,
) -> Option<Vec<SolutionRow>> {
    let refant_id = options.refant_id;
    let scan_number = group.scan_number();
    let graphs = group.channel_graphs.first()?;
    let solved = solve_graphs(
        graphs,
        gain_type,
        solve_mode,
        SolveGraphOptions {
            group_seed: Some(&group),
            defer_reference: true,
            ..options
        },
        SolveStart::Guess,
    )?;

    let mut antenna_ids = available_antennas.clone();
    antenna_ids.extend(group.antenna_ids.iter().copied());
    if refant_id >= 0 {
        antenna_ids.insert(refant_id);
    }
    let receptors = match gain_type {
        GainType::G
        | GainType::K
        | GainType::KMbd
        | GainType::KCross
        | GainType::KAntPos
        | GainType::GSpline => &solved.receptors[..],
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
                    refant_id: if refant_id < 0 { -1 } else { solved.refant_id },
                    gains: Vec::with_capacity(receptors.len()),
                    flags: Vec::with_capacity(receptors.len()),
                    param_errors: Vec::with_capacity(receptors.len()),
                    snrs: Vec::with_capacity(receptors.len()),
                    weights: Vec::with_capacity(receptors.len()),
                };
                for (index, receptor) in receptors.iter().enumerate() {
                    let snr = receptor.snr.get(&antenna_id).copied().unwrap_or_default();
                    let param_error = receptor
                        .param_error
                        .get(&antenna_id)
                        .copied()
                        .unwrap_or_default();
                    let fitted = receptor.solved.contains(&antenna_id) && param_error.is_finite();
                    let flagged = !fitted || snr <= min_snr;
                    // CASA's AP solve retains the fit beneath the SNR flag.
                    // The amplitude- and phase-only paths publish unity there.
                    row.gains.push(
                        if fitted && (!flagged || solve_mode == GainSolveMode::AmplitudePhase) {
                            receptor.gains[&antenna_id]
                        } else if index == 1
                            && group.single_parallel_hand
                            && solve_mode == GainSolveMode::AmplitudePhase
                            && receptors[0].solved.contains(&antenna_id)
                        {
                            // An antenna solved from one hand has an empty
                            // second parameter in CASA's AP gain table.
                            Complex32::new(0.0, 0.0)
                        } else {
                            Complex32::new(1.0, 0.0)
                        },
                    );
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
/// CASA's B solve starts each channel from its predecessor's parameters.
/// Constraint failures leave unity for the affected antennas.
pub(crate) fn solve_graphs(
    graphs: &[SolveGraph],
    gain_type: GainType,
    solve_mode: GainSolveMode,
    options: SolveGraphOptions<'_>,
    start: SolveStart<'_>,
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
    let mut solved_antennas = active
        .iter()
        .map(|graph| {
            let with_data = graph.antennas();
            if options.correlation_dependent_flags {
                with_data
            } else {
                reachable_antennas(graph, refant_id)
                    .into_iter()
                    .filter(|antenna_id| with_data.contains(antenna_id))
                    .collect::<BTreeSet<_>>()
            }
        })
        .collect::<Vec<_>>();
    if solved_antennas.iter().all(BTreeSet::is_empty) {
        return None;
    }
    let bandpass_guess = if let SolveStart::BandpassGuess {
        scalar,
        independent_input_flags,
    } = start
    {
        Some(bandpass_first_guess(
            &active,
            &constrained,
            &solved_antennas,
            refant_id,
            scalar,
            independent_input_flags,
        ))
    } else {
        None
    };
    let mut first = if let Some(guess) = &bandpass_guess {
        guess
            .iter()
            .zip(&solved_antennas)
            .map(|(gains, antennas)| antennas.iter().map(|&id| (id, gains[&id])).collect())
            .collect()
    } else if let SolveStart::PreviousChannel(previous) = start {
        solved_antennas
            .iter()
            .enumerate()
            .map(|(index, antennas)| {
                let fit = previous.and_then(|previous| previous.receptors.get(index));
                antennas
                    .iter()
                    .map(|&antenna| {
                        let gain = fit
                            .and_then(|fit| fit.gains.get(&antenna))
                            .map_or(Complex64::new(1.0, 0.0), |gain| complex32_to_64(*gain));
                        (antenna, gain)
                    })
                    .collect()
            })
            .collect()
    } else if gain_type == GainType::T
        && let Some(seed) = options.group_seed
        && let Some(guess) =
            seed.scalar_initial_gains(&solved_antennas[0], options.refant_id, solve_mode)
    {
        vec![guess]
    } else if options.robustness.is_some() {
        let first_graphs = robust::first_graphs(&active);
        let first_reference = first_graphs
            .iter()
            .flat_map(SolveGraph::antennas)
            .min_by_key(|antenna| (*antenna != options.refant_id, *antenna))
            .unwrap_or(refant_id);
        first_guess(&first_graphs, &solved_antennas, gain_type, first_reference)
    } else if let Some(seed) = options
        .group_seed
        .and_then(|group| group.first_g_graphs.as_ref())
    {
        let first_reference = seed
            .iter()
            .flat_map(SolveGraph::antennas)
            .min_by_key(|antenna| (*antenna != options.refant_id, *antenna))
            .unwrap_or(refant_id);
        first_guess(
            seed.as_slice(),
            &solved_antennas,
            gain_type,
            first_reference,
        )
    } else {
        first_guess(&active, &solved_antennas, gain_type, refant_id)
    };
    if let SolveStart::LegacyChannel(previous) = start {
        for (guess, receptor) in first.iter_mut().zip(&previous.receptors) {
            for (antenna_id, value) in guess.iter_mut() {
                if receptor.solved.contains(antenna_id) {
                    *value = complex32_to_64(receptor.gains[antenna_id]);
                }
            }
        }
    }
    let (gains, active) = if let Some(robustness) = options.robustness {
        robust::solve(active, &mut solved_antennas, first, refant_id, robustness)?
    } else {
        (
            solve_lm(
                &active,
                &mut solved_antennas,
                first,
                refant_id,
                SolveIteration::Ordinary,
            )?,
            active,
        )
    };
    let mut receptors = active
        .iter()
        .zip(solved_antennas)
        .zip(gains)
        .map(|((graph, mut solved), gains)| {
            if options.robustness.is_some() {
                let with_weight = graph.antennas();
                solved.retain(|id| with_weight.contains(id));
            }
            finish_receptor(
                graph,
                solved,
                gains,
                solve_mode,
                if options.defer_reference || options.refant_id < 0 {
                    -1
                } else {
                    refant_id
                },
            )
        })
        .collect::<Vec<_>>();
    apply_group_reduced_chi_square(&mut receptors);
    if let SolveStart::PreviousChannel(Some(previous)) = start {
        // A constrained antenna with one unmeasured receptor keeps that
        // receptor's parameter. CASA flags it when its Hessian is zero.
        for (receptor, prior) in receptors.iter_mut().zip(&previous.receptors) {
            for &antenna in &constrained {
                if let Some(&gain) = prior.gains.get(&antenna) {
                    receptor.gains.entry(antenna).or_insert(gain);
                }
            }
        }
    }
    if let Some(guess) = bandpass_guess {
        for (receptor, initial) in receptors.iter_mut().zip(guess) {
            for (antenna, gain) in initial {
                receptor
                    .gains
                    .entry(antenna)
                    .or_insert_with(|| complex64_to_32(gain));
            }
        }
    }
    Some(SolvedGraphs {
        refant_id,
        receptors,
    })
}

fn bandpass_first_guess(
    graphs: &[SolveGraph],
    constrained: &BTreeSet<i32>,
    solved_antennas: &[BTreeSet<i32>],
    requested: i32,
    scalar: bool,
    independent_input_flags: bool,
) -> Vec<HashMap<i32, Complex64>> {
    if scalar {
        let mut seed = first_guess(&graphs[..1], &solved_antennas[..1], GainType::G, requested);
        seed.push(
            constrained
                .iter()
                .map(|&antenna| (antenna, Complex64::new(0.0, 0.0)))
                .collect(),
        );
        return seed;
    }
    if solved_antennas.iter().all(|ants| ants == constrained) {
        return first_guess(graphs, solved_antennas, GainType::G, requested);
    }
    // GJones::guessPar shares the phase reference and mean scale across
    // hands. With correlation-dependent flagging a surviving hand can seed it.
    let joint = graphs
        .iter()
        .flat_map(|graph| graph.weights.keys().copied())
        .filter(|&pair| {
            independent_input_flags || graphs.iter().all(|graph| graph.weight(pair) > 0.0)
        })
        .collect::<BTreeSet<_>>();
    let joined_antennas = joint
        .iter()
        .flat_map(|&(a, b)| [a, b])
        .collect::<BTreeSet<_>>();
    let Some(guess_ref) = joined_antennas
        .get(&requested)
        .or_else(|| joined_antennas.first())
    else {
        return graphs
            .iter()
            .map(|_| {
                constrained
                    .iter()
                    .map(|&antenna| (antenna, Complex64::new(0.3, 0.0)))
                    .collect()
            })
            .collect();
    };
    let mut joined = graphs.to_vec();
    for graph in &mut joined {
        graph.edges.retain(|pair, _| joint.contains(pair));
        graph.weights.retain(|pair, _| joint.contains(pair));
    }
    first_guess(
        &joined,
        &vec![constrained.clone(); graphs.len()],
        GainType::G,
        *guess_ref,
    )
}

/// The antennas that keep at least `min_baselines_per_antenna` baselines
/// with data once antennas below the threshold, and their baselines, are
/// removed in turn (`SolvableVisCal::verifyConstraints`). A baseline counts
/// when any receptor has weight on it.
pub(crate) fn baseline_constrained_antennas(
    graphs: &[SolveGraph],
    min_baselines_per_antenna: usize,
) -> BTreeSet<i32> {
    let baselines = graphs
        .iter()
        .flat_map(|graph| graph.stats.iter())
        .filter(|((antenna1, antenna2), stats)| antenna1 < antenna2 && stats.collapsed_count > 0)
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
            let seed = if antennas.contains(&refant_id) {
                refant_id
            } else {
                antennas.first().copied().unwrap_or(refant_id)
            };
            antennas
                .iter()
                .filter(|antenna_id| **antenna_id != seed)
                .filter_map(|&antenna_id| {
                    let weight = f64::from(graph.weight((seed, antenna_id)));
                    let edge = graph.edges.get(&(seed, antenna_id))?;
                    let mean = complex32_to_64(*edge) / weight;
                    let amplitude = mean.norm();
                    (weight > f64::EPSILON && amplitude > 0.0).then(|| {
                        amplitude_sum += amplitude;
                        amplitude_count += 1;
                        let guess = match gain_type {
                            GainType::G
                            | GainType::K
                            | GainType::KMbd
                            | GainType::KCross
                            | GainType::KAntPos
                            | GainType::GSpline => mean.conj(),
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
    }
    guesses
}

#[derive(Clone, Copy)]
enum SolveIteration {
    Ordinary,
    Rms,
    L1(f32),
}

/// The Levenberg–Marquardt iteration of `VisCalSolver2::solve`, with its
/// step size optimisation (`optStepSize2`) and convergence test
/// (`converged`): a step counts towards convergence when chi-square has not
/// risen by more than 0.1% and changed by less than 10%, and six such steps
/// converge. `None` when [`MAX_ITERATIONS`] pass without converging.
fn solve_lm(
    graphs: &[SolveGraph],
    antennas: &mut [BTreeSet<i32>],
    mut gains: Vec<HashMap<i32, Complex64>>,
    refant_id: i32,
    strategy: SolveIteration,
) -> Option<Vec<HashMap<i32, Complex64>>> {
    let mut last_gains = gains.clone();
    let mut last_chi_square = f64::MAX;
    let mut convergence_count = 0_i32;
    let robust_iteration = !matches!(strategy, SolveIteration::Ordinary);
    for iteration in 0..=MAX_ITERATIONS {
        let weighted = match strategy {
            SolveIteration::L1(clamp) => Some(robust::working_graphs(graphs, &gains, clamp)),
            _ => None,
        };
        let graphs = weighted.as_deref().unwrap_or(graphs);
        if robust_iteration && graphs.iter().all(|graph| graph.weights.is_empty()) {
            return None;
        }
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

        if delta_chi_square <= 0.0 || robust_iteration {
            last_chi_square = chi_square;
        } else {
            gains.clone_from(&last_gains);
            chi_square = chi_square_graphs(graphs, &gains);
        }

        let mut gradients = Vec::with_capacity(graphs.len());
        let mut deltas = Vec::with_capacity(graphs.len());
        for ((graph, antennas), receptor_gains) in
            graphs.iter().zip(antennas.iter_mut()).zip(&gains)
        {
            let (gradient, hessian) = gradient_hessian(graph, antennas, receptor_gains);
            if robust_iteration {
                // solveGradHess clears parOK permanently, but residualate2
                // and getErrors still use the last parameter value.
                antennas.retain(|id| hessian.get(id).is_some_and(|value| *value > 0.0));
            }
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
        let step = (!matches!(strategy, SolveIteration::L1(_)))
            .then(|| optimize_step(graphs, &last_gains, &mut gains, &mut deltas, chi_square));
        for (receptor_gains, receptor_delta) in gains.iter_mut().zip(&deltas) {
            for (antenna_id, gain) in receptor_gains.iter_mut() {
                *gain += receptor_delta.get(antenna_id).copied().unwrap_or_default();
            }
        }
        if let Some(step) = &step {
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
                    step,
                    antenna_ids: antennas,
                    last_gains: &last_gains[receptor],
                    gains: &gains[receptor],
                    gradient,
                    hessian,
                    delta: &deltas[receptor],
                });
            }
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
    let error_chi_square = if graph.robust_samples.is_empty() {
        chi_square_for_errors(graph, &error_gains)
    } else {
        robust::chi_square(graph, &gains)
    };
    if solve_mode == GainSolveMode::Phase {
        rereference_complex64(&mut gains, refant_id, true);
    }
    let gains = gains
        .into_iter()
        .map(|(antenna_id, gain)| (antenna_id, complex64_to_32(gain)))
        .collect::<HashMap<_, _>>();

    let hessian = hessian(graph, &error_gains, &solved);
    let good_count = solved.len();
    let sample_count = undirected_collapsed_count(&graph.stats) + graph.fixed_sample_count();
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
            let value = if !solved.contains(antenna_id) {
                0.0
            } else if error > 0.0 {
                amplitude / error
            } else {
                9_999_999.0
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
    if !graph.robust_samples.is_empty() {
        return robust::chi_square(graph, gains);
    }
    let mut total = graph.fixed_chi_square();
    for (&(antenna1, antenna2), stat) in &graph.stats {
        if antenna1 > antenna2 {
            continue;
        }
        let weight = f64::from(graph.weight((antenna1, antenna2)));
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

pub(super) fn complex32_to_64(value: Complex32) -> Complex64 {
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
        let included = if graph.robust_samples.is_empty() {
            solved.contains(&antenna_id) && solved.contains(&other_id)
        } else {
            gains.contains_key(&antenna_id) && gains.contains_key(&other_id)
        };
        if !included {
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
    let mut total = graph.fixed_chi_square();
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
        let weight = graph.weight((antenna1, antenna2));
        let model = gain1 * gain2.conj();
        let contribution = if let Some(sample) = stat.single_sample {
            // CASA subtracts the fitted visibility before squaring. Keep the
            // original averaged sample so rounding in the weighted sum does
            // not create or erase a small residual.
            let residual = sample.value - model * sample.model;
            f64::from(sample.weight) * f64::from(residual.norm_sqr())
        } else {
            let cross = (model.conj() * observed).re;
            stat.weighted_sample_power - 2.0 * f64::from(cross)
                + f64::from(weight) * f64::from(model.norm_sqr())
        };
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
    use crate::solve::grouping::CollapsedVisibility;

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
                single_sample: Some(CollapsedVisibility {
                    value,
                    weight,
                    model: 1.0,
                }),
            },
        );
    }

    #[test]
    fn singleton_error_uses_the_input_sample_before_weighting() {
        let observed = Complex32::new(1.45, 0.0);
        let mut graph = SolveGraph::default();
        add_edge(&mut graph, 0, 1, 3.0, observed);
        let mut gains = HashMap::from([(0, observed), (1, Complex32::new(1.0, 0.0))]);
        assert_eq!(chi_square_for_errors(&graph, &gains), 0.0);

        let next = f32::from_bits(observed.re.to_bits() + 1);
        gains.insert(0, Complex32::new(next, 0.0));
        let residual = f64::from(next - observed.re);
        assert_eq!(
            chi_square_for_errors(&graph, &gains),
            3.0 * residual * residual
        );
    }

    fn options(min_baselines_per_antenna: usize) -> SolveGraphOptions<'static> {
        SolveGraphOptions {
            refant_id: 0,
            min_baselines_per_antenna,
            group_seed: None,
            correlation_dependent_flags: false,
            defer_reference: false,
            robustness: None,
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
            SolveStart::Guess,
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
            SolveStart::Guess,
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
                SolveStart::Guess
            )
            .is_none()
        );
        assert!(
            solve_graphs(
                &[graph],
                GainType::T,
                GainSolveMode::Phase,
                options(1),
                SolveStart::Guess
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
            SolveStart::Guess,
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
            0.0,
            options(0),
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
