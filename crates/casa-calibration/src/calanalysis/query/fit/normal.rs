// SPDX-License-Identifier: LGPL-3.0-or-later
//! Small normal equations with CASA's collinearity and initial-parameter rules.

use casa_numerics::invert_symmetric_positive_definite;

const MAX_TERMS: usize = 3;
type Coefficients = [f64; MAX_TERMS];
type Matrix = [Coefficients; MAX_TERMS];

pub(super) struct System {
    terms: usize,
    matrix: Matrix,
    known: Coefficients,
    sum_squares: f64,
}

pub(super) struct Solution {
    pub(super) coefficients: Vec<f64>,
    pub(super) covariance: Vec<Vec<f64>>,
    pub(super) chi_square: f64,
}

impl System {
    pub(super) fn new(terms: usize) -> Self {
        Self {
            terms,
            matrix: [[0.; MAX_TERMS]; MAX_TERMS],
            known: [0.; MAX_TERMS],
            sum_squares: 0.,
        }
    }

    pub(super) fn accumulate(&mut self, x: f64, y: f64, sigma: f64) -> Option<()> {
        let basis = [1., x, x * x];
        let weight = sigma.recip().powi(2);
        if !weight.is_finite() {
            return None;
        }
        // LinearFit starts with all scaled coefficients at one. In a
        // deficient fit, the minimum-norm update preserves that initial
        // polynomial's component in the numerical null space.
        let delta = y - basis[..self.terms].iter().sum::<f64>();
        self.sum_squares += weight * delta * delta;
        for (row, &value) in basis[..self.terms].iter().enumerate() {
            let weighted_value = weight * value;
            self.known[row] += weighted_value * delta;
            for (column, &source) in basis[..self.terms].iter().enumerate().skip(row) {
                self.matrix[row][column] += weighted_value * source;
                self.matrix[column][row] = self.matrix[row][column];
            }
        }
        Some(())
    }

    pub(super) fn solve(&self) -> Option<Solution> {
        let independent = self.independent();
        let rank = independent.len();
        let reduced = independent
            .iter()
            .flat_map(|&i| independent.iter().map(move |&j| self.matrix[i][j]))
            .collect::<Vec<_>>();
        let inverse = invert_symmetric_positive_definite(&reduced, rank)?;
        let distribute = self.distribute(&independent, &inverse)?;
        let correction = distribute
            .iter()
            .map(|row| {
                row.iter()
                    .zip(&independent)
                    .map(|(value, &index)| value * self.known[index])
                    .sum()
            })
            .collect::<Vec<f64>>();
        let covariance = if rank == self.terms {
            distribute
        } else {
            distribute
                .iter()
                .map(|row| {
                    distribute
                        .iter()
                        .map(|other| {
                            row.iter()
                                .zip(&independent)
                                .map(|(left, &i)| {
                                    left * other
                                        .iter()
                                        .zip(&independent)
                                        .map(|(right, &j)| self.matrix[i][j] * right)
                                        .sum::<f64>()
                                })
                                .sum::<f64>()
                        })
                        .collect()
                })
                .collect()
        };
        let chi_square = self.chi_square(&correction);
        let coefficients = correction.into_iter().map(|value| value + 1.).collect();
        Some(Solution {
            coefficients,
            covariance,
            chi_square,
        })
    }

    fn independent(&self) -> Vec<usize> {
        let mut normal = self.matrix;
        let mut terms = (0..self.terms).collect::<Vec<_>>();
        let mut rank = 0;
        while rank < terms.len() {
            let index = terms[rank];
            let original = self.matrix[index][index];
            let pivot = normal[index][index];
            // LSQFit compares the squared Schur pivot to the original
            // diagonal, using the LinearFitSVD collinearity factor 1e-8 squared.
            // Rank can change when uncertainty weights change.
            if original == 0. || pivot.powi(2) / original <= 1e-16 {
                terms.swap_remove(rank);
                continue;
            }
            for &row in &terms[rank + 1..] {
                for &column in &terms[rank + 1..] {
                    normal[row][column] -= normal[index][row] * normal[index][column] / pivot;
                }
            }
            rank += 1;
        }
        terms
    }

    fn distribute(&self, independent: &[usize], inverse: &[f64]) -> Option<Vec<Vec<f64>>> {
        let rank = independent.len();
        if rank == self.terms {
            return Some(inverse.chunks_exact(rank).map(<[f64]>::to_vec).collect());
        }
        // Write every original column as C_j in the independent basis.
        // The minimum-norm update is C^T (C C^T)^-1 A^-1 b, where A is
        // the independent normal matrix. Keep the same map for covariance.
        let basis = (0..self.terms)
            .map(|original| {
                (0..rank)
                    .map(|row| {
                        independent
                            .iter()
                            .enumerate()
                            .map(|(column, &index)| {
                                inverse[row * rank + column] * self.matrix[index][original]
                            })
                            .sum::<f64>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let gram = (0..rank)
            .flat_map(|row| {
                let basis = &basis;
                (0..rank).map(move |column| basis.iter().map(|term| term[row] * term[column]).sum())
            })
            .collect::<Vec<_>>();
        let normalized = invert_symmetric_positive_definite(&gram, rank)?;
        let map = basis
            .iter()
            .map(|term| {
                (0..rank)
                    .map(|column| {
                        (0..rank)
                            .map(|row| {
                                term[row]
                                    * (0..rank)
                                        .map(|inner| {
                                            normalized[row * rank + inner]
                                                * inverse[inner * rank + column]
                                        })
                                        .sum::<f64>()
                            })
                            .sum()
                    })
                    .collect()
            })
            .collect();
        Some(map)
    }

    fn chi_square(&self, correction: &[f64]) -> f64 {
        let mut explained = 0.;
        let mut error = 0.;
        for (row, (&value, &known)) in correction.iter().zip(&self.known).enumerate() {
            let model = self.matrix[row]
                .iter()
                .zip(correction)
                .map(|(matrix, solution)| matrix * solution)
                .sum::<f64>();
            explained += value * known;
            error += value * (model - known);
        }
        (self.sum_squares - explained + error).max(0.)
    }
}
