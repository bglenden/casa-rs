// SPDX-License-Identifier: LGPL-3.0-or-later
//! Locate the peak of the baseband fringe packet used for phase unwrapping.

use std::f64::consts::TAU;

pub(super) fn estimate(
    values: &[f64],
    flags: &[bool],
    frequencies: &[f64],
) -> Result<Option<f64>, String> {
    let mut samples = frequencies
        .iter()
        .zip(values)
        .zip(flags)
        .filter_map(|((&frequency, &phase), &flag)| (!flag).then_some((frequency, phase)))
        .collect::<Vec<_>>();
    if samples.len() <= 1 {
        return Ok(None);
    }
    samples.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
    let spacing = samples
        .windows(2)
        .map(|pair| pair[1].0 - pair[0].0)
        .filter(|&spacing| spacing > 0.)
        .fold(f64::INFINITY, f64::min);
    let span = spacing.recip();
    let origin = samples[0].0;
    let bandwidth = samples[samples.len() - 1].0 - origin;
    if span == 0. || !span.is_finite() || bandwidth <= 0. {
        return Err("phase unwrapping needs distinct finite frequencies".into());
    }
    // Removing the common carrier rotates the complex fringe sum without
    // changing its power. The shortest period is set by the bandwidth.
    for (frequency, _) in &mut samples {
        *frequency -= origin;
    }
    let intervals = (8. * bandwidth * span).ceil();
    if !intervals.is_finite() || intervals >= usize::MAX as f64 {
        return Err("phase unwrapping has an invalid frequency grid".into());
    }
    Ok(Some(packet_peak(&samples, span, intervals as usize)))
}

fn packet_peak(samples: &[(f64, f64)], span: f64, intervals: usize) -> f64 {
    let start = -0.5 * span;
    let step = span / intervals as f64;
    let mut before = (start, power(samples, start));
    let mut last = (start + step, power(samples, start + step));
    let mut best = before;
    keep_peak(&mut best, refine(samples, before.0, last.0));
    for index in 2..=intervals {
        let delay = start + step * index as f64;
        let next = (delay, power(samples, delay));
        if last.1 > before.1 && last.1 >= next.1 {
            keep_peak(&mut best, refine(samples, before.0, next.0));
        }
        before = last;
        last = next;
    }
    keep_peak(&mut best, refine(samples, before.0, last.0));
    keep_peak(&mut best, last);
    best.0
}

fn keep_peak(best: &mut (f64, f64), candidate: (f64, f64)) {
    if candidate.1 > best.1 {
        *best = candidate;
    }
}

fn refine(samples: &[(f64, f64)], mut left: f64, mut right: f64) -> (f64, f64) {
    const FRACTION: f64 = 0.618_033_988_749_894_9;
    let mut lower = right - FRACTION * (right - left);
    let mut upper = left + FRACTION * (right - left);
    let mut low_power = power(samples, lower);
    let mut high_power = power(samples, upper);
    for _ in 0..32 {
        if low_power < high_power {
            left = lower;
            lower = upper;
            low_power = high_power;
            upper = left + FRACTION * (right - left);
            high_power = power(samples, upper);
        } else {
            right = upper;
            upper = lower;
            high_power = low_power;
            lower = right - FRACTION * (right - left);
            low_power = power(samples, lower);
        }
    }
    if low_power >= high_power {
        (lower, low_power)
    } else {
        (upper, high_power)
    }
}

fn power(samples: &[(f64, f64)], delay: f64) -> f64 {
    let mut real = 0.;
    let mut imaginary = 0.;
    for &(frequency, phase) in samples {
        let (sin, cos) = (TAU * frequency * delay - phase).sin_cos();
        real += cos;
        imaginary += sin;
    }
    real * real + imaginary * imaginary
}
