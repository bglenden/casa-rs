// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample rate-registered Fringe Jones solutions on visibility frequencies.

use super::*;

pub(super) fn sample(
    grid: &GainGrid,
    receptor: usize,
    channel: usize,
    context: &CalibrationSamplingContext<'_>,
) -> Result<GainSample, ApplyExecutionError> {
    let frequencies = context.data_channels.frequencies_hz;
    let frequency =
        *frequencies
            .get(channel)
            .ok_or_else(|| ApplyExecutionError::UnsupportedInterpolation {
                path: context.path.display().to_string(),
                interp: context.interp,
                reason: "Fringe Jones data channel is outside the frequency grid".into(),
            })?;
    let index = 4 * receptor;
    let phase = f64::from(grid.values[[index, 0]].re);
    let delay = f64::from(grid.values[[index + 1, 0]].re);
    let dispersive = grid.values[[index + 3, 0]].re;
    let phase =
        phase + std::f64::consts::TAU * delay * (frequency - context.cal_ref_frequency_hz) / 1e9;
    let correction = if dispersive == 0. {
        0.
    } else {
        let low = (frequencies
            .iter()
            .copied()
            .reduce(f64::min)
            .expect("visibility frequencies")
            / 1e9) as f32;
        let high = (frequencies
            .iter()
            .copied()
            .reduce(f64::max)
            .expect("visibility frequencies")
            / 1e9) as f32;
        let center = (frequency / 1e9) as f32;
        let basis = 1. / f64::from(center) + f64::from((center - low - high) / (low * high));
        f64::from(dispersive * (1e-3 * std::f64::consts::TAU * basis) as f32)
    };
    let (sine, cosine) = (phase + correction).sin_cos();
    Ok(GainSample {
        value: Complex32::new(cosine as f32, sine as f32),
        flagged: grid.flags[[index, 0]],
    })
}
