// SPDX-License-Identifier: LGPL-3.0-or-later
//! Evaluate an additive continuum polynomial over the selected data channels.

use super::*;
use crate::plan::SpectralWindowPlan;

pub(super) fn apply(
    grid: Option<&CalibrationGrid>,
    window: &SpectralWindowPlan,
    path: &Path,
    reversed: bool,
    forward: bool,
    legacy_corruption: bool,
    output: &mut ApplyBuffer<'_>,
) -> Result<(), ApplyExecutionError> {
    let invalid = |reason: &str| ApplyExecutionError::UnsupportedCalibrationTable {
        path: path.display().to_string(),
        reason: reason.into(),
    };
    let &[low, high] = window.channel_frequencies_hz.as_slice() else {
        return Err(invalid(
            "additive polynomial needs two frequency domain endpoints",
        ));
    };
    if !(low.is_finite() && high.is_finite() && low > 0. && low <= high) {
        return Err(invalid("invalid additive polynomial frequency domain"));
    }
    let grid = match grid {
        Some(CalibrationGrid::Complex(grid))
            if grid.channel_count == window.num_chan as usize
                && grid.receptor_count == output.correlation_types.len() =>
        {
            Some(grid)
        }
        Some(_) => {
            return Err(invalid(
                "additive coefficient shape differs from its data domain",
            ));
        }
        None => None,
    };
    let correlations = output.correlation_types.len();
    for correlation in 0..correlations {
        let hand = if reversed && correlations == 4 && matches!(correlation, 1 | 2) {
            3 - correlation
        } else {
            correlation
        };
        let usable = grid.filter(|grid| !grid.flag_at(hand, 0));
        for (channel, &frequency) in output.data_channels.frequencies_hz.iter().enumerate() {
            if let Some(grid) = usable {
                let x = if low == high {
                    0.
                } else {
                    (2. * (frequency - low) / (high - low) - 1.) as f32
                };
                let value = (0..grid.channel_count)
                    .rev()
                    .fold(Complex32::ZERO, |previous, term| {
                        previous * x + grid.value_at(hand, term)
                    });
                let value = if reversed { value.conj() } else { value };
                let cell = &mut output.corrected[[correlation, channel]];
                if forward {
                    *cell = value;
                } else {
                    *cell -= value;
                }
            } else if forward {
                if legacy_corruption {
                    output.corrected[[correlation, channel]] = Complex32::ZERO;
                }
            } else if output.propagate_flags && !output.flags[[correlation, channel]] {
                output.flags[[correlation, channel]] = true;
                *output.newly_flagged_samples += 1;
            }
        }
    }
    Ok(())
}
