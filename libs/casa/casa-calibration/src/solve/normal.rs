// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weighted normal equations with a known lower bandwidth.

#[derive(Clone, Copy)]
pub(super) struct Term {
    pub(super) index: usize,
    pub(super) coefficient: f64,
}

/// Store only the nonzero lower band. The spline solver keeps the bandwidth
/// bounded by ordering its controls by knot, then antenna.
pub(super) struct NormalBand {
    lower: Vec<f64>,
    rhs: Vec<f64>,
    bandwidth: usize,
}

impl NormalBand {
    pub(super) fn new(size: usize, bandwidth: usize) -> Option<Self> {
        let cells = size.checked_mul(bandwidth)?;
        let mut lower = Vec::new();
        lower.try_reserve_exact(cells).ok()?;
        lower.resize(cells, 0.);
        let mut rhs = Vec::new();
        rhs.try_reserve_exact(size).ok()?;
        rhs.resize(size, 0.);
        Some(Self {
            lower,
            rhs,
            bandwidth,
        })
    }

    pub(super) fn accumulate(&mut self, terms: &[Term], value: f64, weight: f64) {
        for &term in terms {
            let weighted = term.coefficient * weight;
            self.rhs[term.index] += weighted * value;
            for &other in terms {
                if other.index <= term.index {
                    let index = self.cell(term.index, other.index);
                    self.lower[index] += weighted * other.coefficient;
                }
            }
        }
    }

    pub(super) fn factor(&mut self) -> Option<()> {
        for row in 0..self.rhs.len() {
            self.factor_row(row)?;
        }
        Some(())
    }

    pub(super) fn damped_step<'a>(
        &self,
        scratch: &'a mut Self,
        damping: f64,
    ) -> Option<(&'a [f64], f64)> {
        scratch.lower.copy_from_slice(&self.lower);
        scratch.rhs.copy_from_slice(&self.rhs);
        for diagonal in scratch.lower.iter_mut().step_by(self.bandwidth) {
            *diagonal += damping * if *diagonal > 0. { *diagonal } else { 1. };
        }
        scratch.factor()?;
        let step = scratch.solve()?;
        let reduction = step
            .iter()
            .zip(&self.rhs)
            .zip(self.lower.iter().step_by(self.bandwidth))
            .map(|((&delta, &rhs), &diagonal)| {
                let scale = if diagonal > 0. { diagonal } else { 1. };
                delta * (rhs + damping * scale * delta)
            })
            .sum::<f64>()
            * 0.5;
        Some((step, reduction))
    }

    pub(super) fn clear_rhs(&mut self) {
        self.rhs.fill(0.);
    }

    pub(super) fn add_rhs(&mut self, terms: &[Term], value: f64, weight: f64) {
        for term in terms {
            self.rhs[term.index] += term.coefficient * weight * value;
        }
    }

    pub(super) fn solve(&mut self) -> Option<&[f64]> {
        let size = self.rhs.len();
        for row in 0..size {
            let first = self.first_column(row);
            let preceding = (first..row)
                .map(|col| self.lower[self.cell(row, col)] * self.rhs[col])
                .sum::<f64>();
            self.rhs[row] = (self.rhs[row] - preceding) / self.lower[self.cell(row, row)];
        }
        for row in (0..size).rev() {
            let end = size.min(row.saturating_add(self.bandwidth));
            let following = (row + 1..end)
                .map(|col| self.lower[self.cell(col, row)] * self.rhs[col])
                .sum::<f64>();
            self.rhs[row] = (self.rhs[row] - following) / self.lower[self.cell(row, row)];
        }
        self.rhs
            .iter()
            .all(|value| value.is_finite())
            .then_some(&self.rhs)
    }

    fn factor_row(&mut self, row: usize) -> Option<()> {
        let first = self.first_column(row);
        for column in first..row {
            let index = self.cell(row, column);
            let previous = (first..column)
                .map(|i| self.lower[self.cell(row, i)] * self.lower[self.cell(column, i)])
                .sum::<f64>();
            self.lower[index] =
                (self.lower[index] - previous) / self.lower[self.cell(column, column)];
        }
        let index = self.cell(row, row);
        let original = self.lower[index];
        let previous = (first..row)
            .map(|column| self.lower[self.cell(row, column)].powi(2))
            .sum::<f64>();
        let pivot = original - previous;
        if !pivot.is_finite() || pivot <= 64. * f64::EPSILON * original.abs() {
            return None;
        }
        self.lower[index] = pivot.sqrt();
        Some(())
    }

    fn first_column(&self, row: usize) -> usize {
        row.saturating_sub(self.bandwidth - 1)
    }

    fn cell(&self, row: usize, column: usize) -> usize {
        row * self.bandwidth + row - column
    }
}
