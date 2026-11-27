// SPDX-License-Identifier: LGPL-3.0-or-later
//! Track fringe phase using the solution rates before filtering the parameters.

use casa_core::ArrayValue;
use ndarray::ArrayD;

use super::{Smoother, smooth_series};

pub(super) fn apply(
    parameters: &mut [ArrayValue],
    flags: &mut [ArrayD<bool>],
    rows: &[(usize, f64)],
    windows: &[&[usize]],
    smoother: Smoother,
    reference_frequency: f64,
    ratesmooth: bool,
) -> Result<(), String> {
    if flags[0].shape() != [8, 1] {
        return Err("fringe smoothing requires two sets of four parameters at one channel".into());
    }
    let mut data = parameters
        .iter_mut()
        .map(|row| match row {
            ArrayValue::Float32(row) => Ok(row),
            _ => Err("fringe solutions must have a float32 FPARAM array".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let tau = std::f64::consts::TAU;
    for phase_index in [0, 4] {
        let rate_index = phase_index + 2;
        let phases = data
            .iter()
            .map(|row| row[[phase_index, 0]])
            .collect::<Vec<_>>();
        let rates = data
            .iter()
            .map(|row| row[[rate_index, 0]])
            .collect::<Vec<_>>();
        let valid_rates = flags
            .iter()
            .map(|row| !row[[rate_index, 0]])
            .collect::<Vec<_>>();
        let initial_rates = smooth_series(&rates, &valid_rates, windows, smoother);
        for (row, rate) in data.iter_mut().zip(initial_rates) {
            row[[rate_index, 0]] = rate;
        }
        let mut cycles = 0.0_f64;
        let mut unwrapped = vec![phases[0]];
        for index in 1..phases.len() {
            // CASA's fringe predictor stores the epoch and estimates in float32.
            let step = rows[index].1 as f32 - rows[index - 1].1 as f32;
            let previous = f64::from(phases[index - 1]) / tau;
            let current = f64::from(phases[index]) / tau;
            let predict = |rate: f32| {
                if ratesmooth {
                    f64::from(rate) * reference_frequency * f64::from(step) * 2.0
                } else {
                    0.0
                }
            };
            let forward = (previous + predict(rates[index - 1]) - current) as f32;
            let backward = (current - predict(rates[index]) - previous) as f32;
            let difference = (forward - backward) / 2.0;
            if difference > 0.5 {
                cycles += (f64::from(difference) + 0.5).floor();
            } else if difference < 0.5 {
                cycles -= (f64::from(difference.abs()) + 0.5).floor();
            }
            unwrapped.push((f64::from(phases[index]) + tau * cycles) as f32);
        }
        let valid_phase = flags
            .iter()
            .map(|row| !row[[phase_index, 0]])
            .collect::<Vec<_>>();
        let smoothed = smooth_series(&unwrapped, &valid_phase, windows, smoother);
        for (i, (row, selected)) in data.iter_mut().zip(windows).enumerate() {
            if selected.iter().any(|&index| valid_phase[index]) {
                row[[phase_index, 0]] = wrap_phase(smoothed[i]);
                flags[i][[phase_index, 0]] = false;
            } else {
                flags[i][[phase_index, 0]] = true;
            }
        }
    }
    for parameter in [1, 2, 3, 5, 6, 7] {
        let cell = [parameter, 0];
        let valid = flags.iter().map(|row| !row[cell]).collect::<Vec<_>>();
        let values = data.iter().map(|row| row[cell]).collect::<Vec<_>>();
        let smoothed = smooth_series(&values, &valid, windows, smoother);
        for (((row, flag), window), value) in data
            .iter_mut()
            .zip(flags.iter_mut())
            .zip(windows)
            .zip(smoothed)
        {
            row[cell] = value;
            flag[cell] = !window.iter().any(|&i| valid[i]);
        }
    }
    Ok(())
}

fn wrap_phase(phase: f32) -> f32 {
    let pi = std::f64::consts::PI;
    let tau = std::f64::consts::TAU;
    if !phase.is_finite() {
        return phase;
    }
    let mut wrapped = phase.abs();
    while f64::from(wrapped) > pi {
        let current = f64::from(wrapped);
        let next = (current - tau) as f32;
        if next == wrapped {
            // CASA's repeated subtraction makes no progress at this spacing.
            return ((f64::from(phase) + pi).rem_euclid(tau) - pi) as f32;
        }
        // The rounded subtraction has a constant step within a float32
        // exponent range. Leave a turn at the boundary for the finer spacing.
        let boundary = f64::from(f32::from_bits(wrapped.to_bits() & 0x7f80_0000));
        let step = current - f64::from(next);
        let turns = ((current - boundary) / step).floor() - 1.0;
        wrapped = if turns > 1.0 {
            (current - turns * step) as f32
        } else {
            next
        };
    }
    wrapped * phase.signum()
}
