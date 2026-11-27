// SPDX-License-Identifier: LGPL-3.0-or-later
//! Iterative D Jones solves and formal solution errors.

use num_complex::{Complex32, Complex64};

use crate::solve::kernel::complex32_to_64;

use super::LeakageEstimate;
use super::general;

#[derive(Clone, Copy)]
pub(super) enum LeakageAlgebra {
    Linearized,
    General,
}

pub(super) struct StandardSample {
    pub(super) pair: [usize; 2],
    pub(super) data: [Complex32; 4],
    pub(super) model: [Complex32; 4],
    pub(super) weights: [f32; 4],
}

/// Start at the preceding channel's solution. CASA traverses channels from
/// high to low, preserving that starting point until the next time interval.
pub(super) fn solve_channel(
    samples: &[StandardSample],
    gains: &mut Vec<[Complex32; 2]>,
    eligible: &[bool],
    min_snr: f32,
    algebra: LeakageAlgebra,
) -> Result<Vec<LeakageEstimate>, Vec<[bool; 2]>> {
    let mut valid = eligible
        .iter()
        .map(|&eligible| [eligible; 2])
        .collect::<Vec<_>>();
    let usable = samples
        .iter()
        .filter(|sample| sample.pair.iter().all(|&antenna| eligible[antenna]))
        .collect::<Vec<_>>();
    for (gain, &eligible) in gains.iter_mut().zip(eligible) {
        if !eligible {
            *gain = [Complex32::ONE; 2];
        }
    }
    if usable.is_empty() {
        return Err(valid);
    }
    let mut last_loss = f64::MAX;
    let mut stable_steps = 0_usize;
    let mut last_gains = gains.clone();
    let mut direction = vec![[Complex32::ZERO; 2]; gains.len()];
    let mut derivatives = Derivatives::new(gains.len());
    for _ in 0..=50 {
        let loss = chi_squared(&usable, gains, &valid, algebra);
        if !loss.is_finite() {
            gains.clone_from(&last_gains);
            return Err(vec![[false; 2]; gains.len()]);
        }
        if loss == 0.0 {
            return Err(valid);
        }
        let change = loss - last_loss;
        if (change / loss) as f32 <= 0.001 {
            stable_steps += usize::from(change.abs() < 0.1 * loss);
        } else if change.abs() > 0.1 * loss {
            stable_steps = 0;
        } else {
            stable_steps = stable_steps.saturating_sub(1);
        }
        if stable_steps > 5 {
            derivatives.accumulate(&usable, gains, &valid, algebra);
            return Ok(estimate(
                gains,
                &valid,
                &derivatives,
                &usable,
                loss,
                min_snr,
            ));
        }
        if change <= 0.0 {
            last_loss = loss;
            derivatives.accumulate(&usable, gains, &valid, algebra);
            for (antenna, deltas) in direction.iter_mut().enumerate() {
                for (receptor, delta) in deltas.iter_mut().enumerate() {
                    let hessian = derivatives.hessian[antenna][receptor];
                    valid[antenna][receptor] &= hessian > 0.0;
                    *delta = if valid[antenna][receptor] {
                        let gradient = derivatives.gradient[antenna][receptor] / hessian;
                        Complex32::new(gradient.re as f32, gradient.im as f32) * -0.5
                    } else {
                        Complex32::ZERO
                    };
                }
            }
        } else {
            gains.clone_from(&last_gains);
        }
        if !valid.iter().flatten().any(|&usable| usable) {
            return Err(valid);
        }
        let scale = match algebra {
            LeakageAlgebra::Linearized => step_scale(&usable, gains, &direction, &valid, loss),
            // CASA materializes full Jones matrices and refreshes them on
            // accepted parameter updates. Its trial steps see the same
            // matrices, keeping the nominal damped step for Dgen.
            LeakageAlgebra::General => 1.0,
        };
        last_gains.clone_from(gains);
        update(gains, &direction, scale);
    }
    Err(valid)
}

pub(super) fn eligible_antennas(
    samples: &[StandardSample],
    antennas: usize,
    minimum: usize,
) -> Vec<bool> {
    let mut neighbors = vec![std::collections::BTreeSet::new(); antennas];
    for sample in samples
        .iter()
        .filter(|sample| sample.weights.iter().any(|&weight| weight > 0.0))
    {
        let [left, right] = sample.pair;
        neighbors[left].insert(right);
        neighbors[right].insert(left);
    }
    let mut eligible = vec![true; antennas];
    let mut remaining = (0..antennas).collect::<Vec<_>>();
    while let Some(antenna) = remaining.pop() {
        if eligible[antenna] && neighbors[antenna].len() < minimum {
            eligible[antenna] = false;
            for neighbor in std::mem::take(&mut neighbors[antenna]) {
                neighbors[neighbor].remove(&antenna);
                remaining.push(neighbor);
            }
        }
    }
    eligible
}

struct Derivatives {
    gradient: Vec<[Complex64; 2]>,
    hessian: Vec<[f64; 2]>,
}

impl Derivatives {
    fn new(antennas: usize) -> Self {
        Self {
            gradient: vec![[Complex64::ZERO; 2]; antennas],
            hessian: vec![[0.0; 2]; antennas],
        }
    }

    fn accumulate(
        &mut self,
        samples: &[&StandardSample],
        gains: &[[Complex32; 2]],
        valid: &[[bool; 2]],
        algebra: LeakageAlgebra,
    ) {
        self.gradient.fill([Complex64::ZERO; 2]);
        self.hessian.fill([0.0; 2]);
        for sample in samples {
            let [left, right] = sample.pair;
            let (residual, partials) = sample.residual(gains, valid, algebra);
            let [xx, _, _, yy] = sample.model;
            for (side, antenna) in [left, right].into_iter().enumerate() {
                for receptor in 0..2 {
                    let derivative = match algebra {
                        LeakageAlgebra::Linearized => {
                            let mut derivative = partials[side];
                            // GenLinear adds to the partial coherency and
                            // retains its existing cross-hands.
                            derivative[1 + (receptor ^ side)] += [yy, xx][receptor];
                            derivative
                        }
                        LeakageAlgebra::General => {
                            general::derivative(partials[side], side, receptor)
                        }
                    };
                    for (corr, coefficient) in derivative.into_iter().enumerate() {
                        let weight = sample.weights[corr];
                        if weight <= 0.0 {
                            continue;
                        }
                        let gradient = residual[corr] * coefficient.conj();
                        self.gradient[antenna][receptor] += complex32_to_64(
                            if side == 0 { gradient } else { gradient.conj() } * weight,
                        );
                        self.hessian[antenna][receptor] +=
                            f64::from(weight * coefficient.norm_sqr());
                    }
                }
            }
        }
    }
}

impl StandardSample {
    fn residual(
        &self,
        gains: &[[Complex32; 2]],
        valid: &[[bool; 2]],
        algebra: LeakageAlgebra,
    ) -> ([Complex32; 4], [[Complex32; 4]; 2]) {
        let [left, right] = self.pair.map(|antenna| gains[antenna]);
        if matches!(algebra, LeakageAlgebra::General) {
            // CASA's differentiated coherencies use a zero matrix when
            // either of its two parameters is invalid. The raw parameter
            // values still carry forward to the next channel.
            let [left_valid, right_valid] = self.pair.map(|antenna| valid[antenna] == [true; 2]);
            let zero = [Complex32::ZERO; 4];
            let left_model = if left_valid {
                general::left(self.model, left)
            } else {
                zero
            };
            let (right_model, corrupted) = if right_valid {
                (
                    general::right(self.model, right),
                    general::right(left_model, right),
                )
            } else {
                (zero, zero)
            };
            let residual = std::array::from_fn(|corr| corrupted[corr] - self.data[corr]);
            return (residual, [right_model, left_model]);
        }
        let [xx, xy, yx, yy] = self.model;
        let zero = Complex32::ZERO;
        let [right_xy, right_yx] = [right[1].conj() * xx, right[0].conj() * yy];
        let left_model = [zero, xy + left[0] * yy, yx + left[1] * xx, zero];
        let right_model = [zero, xy + right_xy, yx + right_yx, zero];
        let residual = [
            zero,
            left_model[1] + right_xy - self.data[1],
            left_model[2] + right_yx - self.data[2],
            zero,
        ];
        (residual, [right_model, left_model])
    }
}

fn chi_squared(
    samples: &[&StandardSample],
    gains: &[[Complex32; 2]],
    valid: &[[bool; 2]],
    algebra: LeakageAlgebra,
) -> f64 {
    samples
        .iter()
        .map(|sample| {
            let (residual, _) = sample.residual(gains, valid, algebra);
            residual
                .into_iter()
                .zip(sample.weights)
                .map(|(value, weight)| f64::from(weight * value.norm_sqr()))
                .sum::<f64>()
        })
        .sum()
}

fn update(gains: &mut [[Complex32; 2]], direction: &[[Complex32; 2]], scale: f32) {
    for (gain, direction) in gains.iter_mut().zip(direction) {
        for (value, &delta) in gain.iter_mut().zip(direction) {
            *value += scale * delta;
        }
    }
}

fn step_scale(
    samples: &[&StandardSample],
    gains: &[[Complex32; 2]],
    direction: &[[Complex32; 2]],
    valid: &[[bool; 2]],
    loss: f64,
) -> f32 {
    let mut trial = gains.to_vec();
    let mut evaluate = |scale| {
        trial.copy_from_slice(gains);
        update(&mut trial, direction, scale);
        chi_squared(samples, &trial, valid, LeakageAlgebra::Linearized)
    };
    let mut step = 1.0;
    let mut mid = evaluate(step);
    let mut high;
    if mid < loss {
        high = evaluate(2.0 * step);
        while high < mid && step < f32::MAX / 4.0 {
            step *= 2.0;
            mid = high;
            high = evaluate(2.0 * step);
        }
    } else {
        high = mid;
        step *= 0.5;
        mid = evaluate(step);
        while mid > loss && step > f32::MIN_POSITIVE {
            step *= 0.5;
            high = mid;
            mid = evaluate(step);
        }
    }
    let curvature = loss - 2.0 * mid + high;
    let scale = f64::from(step) * (1.5 - (high - mid) / curvature);
    if scale.is_finite() && scale > 0.0 {
        scale as f32
    } else {
        1.0
    }
}

fn estimate(
    gains: &[[Complex32; 2]],
    valid: &[[bool; 2]],
    derivatives: &Derivatives,
    samples: &[&StandardSample],
    loss: f64,
    min_snr: f32,
) -> Vec<LeakageEstimate> {
    let observations = samples
        .iter()
        .flat_map(|sample| sample.weights)
        .filter(|weight| *weight > 0.0)
        .count();
    let parameters = valid.iter().flatten().filter(|&&usable| usable).count();
    let degrees = (2 * observations.saturating_sub(parameters)).max(1) as f64;
    let scale = 2.0 * loss / degrees;
    gains
        .iter()
        .enumerate()
        .map(|(antenna, &gain)| {
            let error = derivatives.hessian[antenna].map(|hessian| {
                if hessian > 0.0 {
                    (scale / hessian).sqrt() as f32
                } else {
                    0.0
                }
            });
            let snr = std::array::from_fn(|receptor| {
                if valid[antenna][receptor] && error[receptor] > 0.0 {
                    error[receptor].recip()
                } else {
                    0.0
                }
            });
            LeakageEstimate {
                gain,
                error,
                snr,
                valid: std::array::from_fn(|receptor| {
                    valid[antenna][receptor] && error[receptor] > 0.0 && snr[receptor] > min_snr
                }),
            }
        })
        .collect()
}
