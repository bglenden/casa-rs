// SPDX-License-Identifier: LGPL-3.0-or-later
//! Iterative linearized D Jones solves and formal solution errors.

use num_complex::{Complex32, Complex64};

use crate::solve::kernel::complex32_to_64;

use super::LeakageEstimate;

pub(super) struct StandardSample {
    pub(super) pair: [usize; 2],
    pub(super) cross_hands: [Complex32; 2],
    pub(super) model: [Complex32; 4],
    pub(super) weights: [f32; 2],
}

/// Start at the preceding channel's solution. CASA traverses channels from
/// high to low, preserving that starting point until the next time interval.
pub(super) fn solve_channel(
    samples: &[StandardSample],
    gains: &mut Vec<[Complex32; 2]>,
    min_baselines: usize,
    min_snr: f32,
) -> Option<Vec<LeakageEstimate>> {
    let eligible = eligible_antennas(samples, gains.len(), min_baselines);
    let valid = samples
        .iter()
        .filter(|sample| sample.pair.iter().all(|&antenna| eligible[antenna]))
        .collect::<Vec<_>>();
    for (gain, &eligible) in gains.iter_mut().zip(&eligible) {
        if !eligible {
            *gain = [Complex32::ONE; 2];
        }
    }
    if valid.is_empty() {
        return None;
    }
    let mut last_loss = f64::MAX;
    let mut stable_steps = 0_usize;
    let mut last_gains = gains.clone();
    let mut direction = vec![[Complex32::ZERO; 2]; gains.len()];
    let mut derivatives = Derivatives::new(gains.len());
    for _ in 0..=50 {
        let loss = chi_squared(&valid, gains);
        if !loss.is_finite() || loss == 0.0 {
            return None;
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
            derivatives.accumulate(&valid, gains);
            return Some(estimate(
                gains,
                &eligible,
                &derivatives,
                &valid,
                loss,
                min_snr,
            ));
        }
        if change <= 0.0 {
            last_loss = loss;
            derivatives.accumulate(&valid, gains);
            for (antenna, deltas) in direction.iter_mut().enumerate() {
                for (receptor, delta) in deltas.iter_mut().enumerate() {
                    let hessian = derivatives.hessian[antenna][receptor];
                    *delta = if eligible[antenna] && hessian > 0.0 {
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
        let scale = step_scale(&valid, gains, &direction, loss);
        last_gains.clone_from(gains);
        update(gains, &direction, scale);
    }
    None
}

fn eligible_antennas(samples: &[StandardSample], antennas: usize, minimum: usize) -> Vec<bool> {
    let mut neighbors = vec![std::collections::BTreeSet::new(); antennas];
    for sample in samples {
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

    fn accumulate(&mut self, samples: &[&StandardSample], gains: &[[Complex32; 2]]) {
        self.gradient.fill([Complex64::ZERO; 2]);
        self.hessian.fill([0.0; 2]);
        for sample in samples {
            let [left, right] = sample.pair;
            let (residual, partials) = sample.residual(gains);
            let [xx, _, _, yy] = sample.model;
            for (side, antenna) in [left, right].into_iter().enumerate() {
                for receptor in 0..2 {
                    let mut derivative = partials[side];
                    // GenLinear's differentiated Jones application adds to
                    // the partial coherency, including its existing cross-hands.
                    let hand = receptor ^ side;
                    derivative[hand] += [yy, xx][receptor];
                    for (corr, coefficient) in derivative.into_iter().enumerate() {
                        let weight = sample.weights[corr];
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
    fn residual(&self, gains: &[[Complex32; 2]]) -> ([Complex32; 2], [[Complex32; 2]; 2]) {
        let [left, right] = self.pair.map(|antenna| gains[antenna]);
        let [xx, xy, yx, yy] = self.model;
        let left_offset = [left[0] * yy, left[1] * xx];
        let right_offset = [right[1].conj() * xx, right[0].conj() * yy];
        let left_model = [xy + left_offset[0], yx + left_offset[1]];
        let right_model = [xy + right_offset[0], yx + right_offset[1]];
        let residual = std::array::from_fn(|hand| {
            left_model[hand] + right_offset[hand] - self.cross_hands[hand]
        });
        (residual, [right_model, left_model])
    }
}

fn chi_squared(samples: &[&StandardSample], gains: &[[Complex32; 2]]) -> f64 {
    samples
        .iter()
        .map(|sample| {
            let (residual, _) = sample.residual(gains);
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
    loss: f64,
) -> f32 {
    let mut trial = gains.to_vec();
    let mut evaluate = |scale| {
        trial.copy_from_slice(gains);
        update(&mut trial, direction, scale);
        chi_squared(samples, &trial)
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
        while mid > loss && step > 0.125 {
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
    eligible: &[bool],
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
    let parameters = 2 * eligible.iter().filter(|&&eligible| eligible).count();
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
            let snr = error.map(|error| if error > 0.0 { error.recip() } else { 0.0 });
            LeakageEstimate {
                gain,
                error,
                snr,
                valid: snr.map(|snr| eligible[antenna] && snr > min_snr),
            }
        })
        .collect()
}
