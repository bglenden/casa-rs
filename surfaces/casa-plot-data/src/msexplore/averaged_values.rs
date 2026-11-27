// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn finalize_averaged_axis_value(
    axis: MsAxis,
    accumulator: &AveragedPointAccumulator,
    scalar_average: bool,
    avgtime_enabled: bool,
    use_x_samples: bool,
    reference_time_average: bool,
) -> Option<f64> {
    if axis.is_visibility_math() {
        let samples = if use_x_samples {
            &accumulator.x_visibility
        } else {
            &accumulator.y_visibility
        };
        return samples.project(axis, scalar_average, reference_time_average);
    }
    match axis {
        MsAxis::Time => {
            let samples = accumulator
                .numeric_axis_samples
                .iter()
                .find(|(candidate, _)| *candidate == axis)
                .map(|(_, samples)| samples)?;
            if avgtime_enabled {
                if samples.len() == 1 {
                    return Some(
                        samples[0]
                            - average_float_samples(&accumulator.time_interval_samples)? / 2.0,
                    );
                }
                let min_time = samples.iter().copied().min_by(f64::total_cmp)?;
                let max_time = samples.iter().copied().max_by(f64::total_cmp)?;
                return Some(min_time + (max_time - min_time) / 2.0);
            }
            average_float_samples(samples)
        }
        MsAxis::Flag => accumulator
            .has_flag_samples
            .then_some(if accumulator.any_flag_sample {
                1.0
            } else {
                0.0
            }),
        MsAxis::FlagRow => accumulator
            .has_flag_row_samples
            .then_some(if accumulator.any_flag_row { 1.0 } else { 0.0 }),
        _ => accumulator
            .numeric_axis_samples
            .iter()
            .find(|(candidate, _)| *candidate == axis)
            .and_then(|(_, samples)| average_float_samples(samples)),
    }
}
