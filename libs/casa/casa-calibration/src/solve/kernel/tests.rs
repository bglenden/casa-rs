// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::solve::grouping::CollapsedVisibility;

fn add_edge(graph: &mut SolveGraph, antenna1: i32, antenna2: i32, weight: f32, value: Complex32) {
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
        control: Default::default(),
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
