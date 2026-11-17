// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA bandpass frequency resampling and full-band power normalization.

use super::*;

// CTPatchedInterp::resampleInFreq masks flagged channels, interpolates
// amplitude and unwrapped phase, and uses nearest valid values at the edges.
fn resample(grid: &GainGrid, input: &[f64], output: &[f64]) -> GainGrid {
    let shape = IxDyn(&[grid.receptor_count, output.len()]);
    let mut values = ArrayD::from_elem(shape.clone().f(), Complex32::new(0., 0.));
    let mut flags = ArrayD::from_elem(shape.f(), true);
    for receptor in 0..grid.receptor_count {
        let mut points = (0..grid.channel_count)
            .filter(|&channel| !grid.flag_at(receptor, channel))
            .map(|channel| (input[channel], grid.value_at(receptor, channel)))
            .collect::<Vec<_>>();
        points.sort_by(|left, right| left.0.total_cmp(&right.0));
        let mut phases = points.iter().map(|point| point.1.arg()).collect::<Vec<_>>();
        for index in 1..phases.len() {
            while phases[index] - phases[index - 1] > std::f32::consts::PI {
                phases[index] -= std::f32::consts::TAU;
            }
            while phases[index] - phases[index - 1] < -std::f32::consts::PI {
                phases[index] += std::f32::consts::TAU;
            }
        }
        for (channel, &frequency) in output.iter().enumerate() {
            let Some(value) = sample(&points, &phases, frequency) else {
                continue;
            };
            values[[receptor, channel]] = value;
            flags[[receptor, channel]] = false;
        }
    }
    GainGrid {
        receptor_count: grid.receptor_count,
        channel_count: output.len(),
        values,
        flags,
    }
}

fn sample(points: &[(f64, Complex32)], phases: &[f32], frequency: f64) -> Option<Complex32> {
    let first = points.first()?;
    let last = points.last().expect("nonempty points");
    if frequency <= first.0 {
        return Some(first.1);
    }
    if frequency >= last.0 {
        return Some(last.1);
    }
    let upper = points.partition_point(|point| point.0 < frequency);
    if points[upper].0 == frequency {
        return Some(points[upper].1);
    }
    let lower = upper - 1;
    let fraction = ((frequency - points[lower].0) / (points[upper].0 - points[lower].0)) as f32;
    let amplitude =
        points[lower].1.norm() + fraction * (points[upper].1.norm() - points[lower].1.norm());
    let phase = phases[lower] + fraction * (phases[upper] - phases[lower]);
    Some(Complex32::from_polar(amplitude, phase))
}

pub(super) fn prepare(
    grid: Arc<CalibrationGrid>,
    input: &[f64],
    output: &crate::plan::SpectralWindowPlan,
    row: &ApplyRowPlan,
    antenna_id: i32,
    cache: Option<&mut RowGeometryCache>,
    path: &Path,
) -> Result<Arc<CalibrationGrid>, ApplyExecutionError> {
    let CalibrationGrid::Complex(gain) = grid.as_ref() else {
        return Ok(grid);
    };
    if input.len() != gain.channel_count {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: path.display().to_string(),
            shape: gain.values.shape().to_vec(),
        });
    }
    let key = MaterializedGridKey {
        row: RowGeometryKey::new(row.time_seconds, row.field_id, antenna_id),
        kind: 4,
        data_spw_id: output.spw_id,
        grid_id: Arc::as_ptr(&grid) as usize,
    };
    if let Some(prepared) = cache
        .as_ref()
        .and_then(|cache| cache.materialized_grid(key))
    {
        return Ok(prepared);
    }
    let prepared = Arc::new(CalibrationGrid::Complex(resample(
        gain,
        input,
        &output.channel_frequencies_hz,
    )));
    if let Some(cache) = cache {
        cache.insert_materialized_grid(key, prepared.clone());
    }
    Ok(prepared)
}

// BJones::calcWtScale uses squared RMS amplitude of the unflagged full-band
// time-interpolated solution, before any selection/frequency interpolation.
fn mean_power(grid: &GainGrid, receptor: usize) -> f32 {
    let receptor = receptor.min(grid.receptor_count.saturating_sub(1));
    let mut total = 0_f64;
    let mut count = 0;
    for channel in 0..grid.channel_count {
        if !grid.flag_at(receptor, channel) {
            total += f64::from(grid.value_at(receptor, channel).norm_sqr());
            count += 1;
        }
    }
    if count == 0 {
        1.
    } else {
        (total / f64::from(count)) as f32
    }
}

pub(super) fn weight_factors(
    first: &CalibrationGrid,
    second: &CalibrationGrid,
    correlations: &[i32],
) -> Option<Vec<f32>> {
    let (CalibrationGrid::Complex(first), CalibrationGrid::Complex(second)) = (first, second)
    else {
        return None;
    };
    correlations
        .iter()
        .map(|&code| {
            let (left, right) = correlation_receptors(code)?;
            Some(mean_power(first, left) * mean_power(second, right))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_bandpass_bin_interpolates_amplitude_phase_and_uses_full_band_power() {
        // Two valid endpoints: amplitude 1/phase 0 and amplitude 3/phase pi/2.
        // The missing middle is amplitude 2/phase pi/4, independent of sideband.
        for descending in [false, true] {
            let mut frequencies = vec![1., 2., 3.];
            let mut samples = vec![
                Complex32::new(1., 0.),
                Complex32::new(100., 0.),
                Complex32::new(0., 3.),
            ];
            if descending {
                frequencies.reverse();
                samples.reverse();
            }
            let mut gain = GainGrid {
                receptor_count: 1,
                channel_count: 3,
                values: ArrayD::from_shape_vec(IxDyn(&[1, 3]), samples).unwrap(),
                flags: ArrayD::from_shape_vec(IxDyn(&[1, 3]), vec![false, true, false]).unwrap(),
            };
            let result = resample(&gain, &frequencies, &[0., 1., 2., 3., 4.]);
            let expected = [
                Complex32::new(1., 0.),
                Complex32::new(1., 0.),
                Complex32::new(2_f32.sqrt(), 2_f32.sqrt()),
                Complex32::new(0., 3.),
                Complex32::new(0., 3.),
            ];
            for (value, expected) in result.values.iter().zip(expected) {
                assert!((*value - expected).norm() < 1e-6);
            }
            assert!(result.flags.iter().all(|flag| !flag));
            assert_eq!(mean_power(&gain, 0), 5.);
            let mut other = gain.clone();
            other.values.mapv_inplace(|value| value * 2.);
            assert_eq!(
                weight_factors(
                    &CalibrationGrid::Complex(gain.clone()),
                    &CalibrationGrid::Complex(other),
                    &[9]
                ),
                Some(vec![100.])
            );
            gain.flags.fill(true);
            let missing = resample(&gain, &frequencies, &[2.]);
            assert!(missing.flags.iter().all(|flag| *flag));
            assert!(
                missing
                    .values
                    .iter()
                    .all(|value| *value == Complex32::new(0., 0.))
            );
            assert_eq!(mean_power(&gain, 0), 1.);
        }
    }
    #[test]
    fn bandpass_phase_wrap_interpolates_through_pi_instead_of_zero() {
        let values = vec![
            Complex32::from_polar(1., 170_f32.to_radians()),
            Complex32::new(100., 0.),
            Complex32::from_polar(1., -170_f32.to_radians()),
        ];
        let grid = GainGrid {
            receptor_count: 1,
            channel_count: 3,
            values: ArrayD::from_shape_vec(IxDyn(&[1, 3]), values).unwrap(),
            flags: ArrayD::from_shape_vec(IxDyn(&[1, 3]), vec![false, true, false]).unwrap(),
        };
        let result = resample(&grid, &[1., 2., 3.], &[2.]);
        assert!((result.values[[0, 0]] - Complex32::new(-1., 0.)).norm() < 1e-6);
        assert!(!result.flags[[0, 0]]);
    }
}
