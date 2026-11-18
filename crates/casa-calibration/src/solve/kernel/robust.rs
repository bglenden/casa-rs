// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reweight and reject averaged gaincal samples as `VisCalSolver2::solveL1R` does.

use std::collections::{BTreeSet, HashMap};

use num_complex::Complex64;

use super::{
    GainSolveRobustness, RobustSample, SolveEdgeStats, SolveGraph, SolveIteration, solve_lm,
};

const L1_CLAMPS: [f32; 3] = [5e-3, 5e-4, 5e-5];

type Gains = Vec<HashMap<i32, Complex64>>;

pub(super) fn first_graphs(graphs: &[SolveGraph]) -> Vec<SolveGraph> {
    graphs
        .iter()
        .map(|graph| {
            let mut first = SolveGraph::default();
            for sample in graph
                .robust_samples
                .iter()
                .filter(|sample| sample.in_first_buffer)
            {
                sample.accumulate(&mut first, sample.weight);
            }
            first
        })
        .collect()
}

pub(super) fn solve(
    nominal: Vec<SolveGraph>,
    antennas: &[BTreeSet<i32>],
    mut gains: Gains,
    refant_id: i32,
    options: &GainSolveRobustness,
) -> Option<(Gains, Vec<SolveGraph>)> {
    let mut selected = nominal.clone();
    if options.l1 {
        for gain in gains.iter_mut().flat_map(HashMap::values_mut) {
            *gain *= 1.0001;
        }
    }
    for pass in 0..=options.rms_thresholds.len() {
        if options.l1 {
            for clamp in L1_CLAMPS {
                gains = solve_lm(
                    &selected,
                    antennas,
                    gains,
                    refant_id,
                    SolveIteration::L1(clamp),
                )?;
            }
        } else {
            gains = solve_lm(&selected, antennas, gains, refant_id, SolveIteration::Rms)?;
        }
        if let Some(&threshold) = options.rms_thresholds.get(pass) {
            selected = reject_outliers(&nominal, &selected, &gains, threshold);
        }
    }
    if options.l1 {
        selected = l1_graphs(&selected, &gains, L1_CLAMPS[2]);
    }
    Some((gains, selected))
}

/// Recompute CASA's working weight spectrum from the current residuals.
pub(super) fn l1_graphs(graphs: &[SolveGraph], gains: &Gains, clamp: f32) -> Vec<SolveGraph> {
    graphs
        .iter()
        .zip(gains)
        .map(|(graph, gains)| {
            let mut weighted = SolveGraph::default();
            for sample in &graph.robust_samples {
                if let Some(power) = sample.residual_power(gains)
                    && power > 0.0
                {
                    let weight = sample.weight / (power + clamp).sqrt();
                    sample.accumulate(&mut weighted, weight);
                }
            }
            weighted
        })
        .collect()
}

fn reject_outliers(
    nominal: &[SolveGraph],
    selected: &[SolveGraph],
    gains: &Gains,
    threshold: f32,
) -> Vec<SolveGraph> {
    // CASA measures RMS separately for each correlation using the nominal
    // weights of samples that survived the previous pass.
    let mut moments = HashMap::<usize, (f64, f64)>::new();
    for (graph, gains) in selected.iter().zip(gains) {
        for sample in &graph.robust_samples {
            let Some(residual) = sample.residual_power(gains) else {
                continue;
            };
            let (power, weight) = moments.entry(sample.correlation).or_default();
            *power += f64::from(sample.weight * residual);
            *weight += f64::from(sample.weight);
        }
    }
    let cutoffs = moments
        .into_iter()
        .map(|(correlation, (power, weight))| {
            (correlation, threshold * (power / weight).sqrt() as f32)
        })
        .collect::<HashMap<_, _>>();
    nominal
        .iter()
        .zip(gains)
        .map(|(graph, gains)| {
            let mut accepted = SolveGraph::default();
            // Each rejection starts again from the original residual flags.
            for sample in &graph.robust_samples {
                let cutoff = cutoffs.get(&sample.correlation).copied().unwrap_or(0.0);
                if sample
                    .residual_power(gains)
                    .is_some_and(|power| power.sqrt() <= cutoff)
                {
                    sample.accumulate(&mut accepted, sample.weight);
                    accepted.robust_samples.push(*sample);
                }
            }
            accepted
        })
        .collect()
}

impl RobustSample {
    fn residual_power(self, gains: &HashMap<i32, Complex64>) -> Option<f32> {
        let model = gains.get(&self.baseline.0)? * gains.get(&self.baseline.1)?.conj();
        let observed = Complex64::new(f64::from(self.visibility.re), f64::from(self.visibility.im));
        Some((model - observed).norm_sqr() as f32)
    }

    fn accumulate(self, graph: &mut SolveGraph, weight: f32) {
        graph.accumulate(
            self.baseline,
            weight,
            weight * self.visibility,
            SolveEdgeStats {
                weighted_sample_power: f64::from(weight) * f64::from(self.visibility.norm_sqr()),
                raw_weighted_sample_power: f64::from(self.weight)
                    * f64::from(self.visibility.norm_sqr()),
                sample_count: 1,
                collapsed_count: 1,
            },
        );
    }
}
