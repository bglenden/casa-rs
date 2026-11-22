// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA bandpass frequency resampling and full-band power normalization.

use super::*;
use crate::lookup_frequency::relative_axis;

// Share CTPatchedInterp's curves, flag registration, phase unwrapping and
// short-axis rules with simulator application.
fn resample(
    grid: &GainGrid,
    input: &[f64],
    output: &[f64],
    mode: crate::FrequencyInterpolation,
    relative: bool,
    parameter_family: Option<TimeParameterFamily>,
) -> Result<GainGrid, ApplyExecutionError> {
    let (relative_input, relative_output);
    let (input, output) = if relative {
        relative_input = relative_axis(input);
        relative_output = relative_axis(output);
        (relative_input.as_slice(), relative_output.as_slice())
    } else {
        (input, output)
    };
    let solution = crate::ComplexSolution::of(grid);
    if matches!(
        parameter_family,
        Some(TimeParameterFamily::Sky | TimeParameterFamily::SpectralOpacity)
    ) {
        return solution
            .resample_real_array_frequency(input, output, mode)
            .map(crate::ComplexSolution::into_gain_grid);
    }
    if !matches!(
        parameter_family,
        Some(TimeParameterFamily::Tsys | TimeParameterFamily::PositionAngle)
    ) {
        return solution
            .resample_frequency(input, output, mode)
            .map(crate::ComplexSolution::into_gain_grid);
    }

    let mut sampled = solution
        .resample_real_frequency(input, output, mode)?
        .into_gain_grid();
    if matches!(parameter_family, Some(TimeParameterFamily::PositionAngle)) {
        return Ok(sampled);
    }
    // StandardTsys evaluates sqrt(Tsys) and reverses the Jones inversion.
    for (gain, flagged) in sampled.values.iter_mut().zip(&mut sampled.flags) {
        let tsys = gain.re;
        *flagged |= tsys <= 0.0 || tsys.is_nan();
        *gain = if *flagged {
            Complex32::new(1.0, 0.0)
        } else {
            Complex32::new(1.0 / tsys.sqrt(), 0.0)
        };
    }
    Ok(sampled)
}

pub(super) fn prepare(
    grid: Arc<CalibrationGrid>,
    input: &[f64],
    output: &crate::plan::SpectralWindowPlan,
    cache: Option<&mut RowGeometryCache>,
    table: &LoadedCalibrationTable,
    transient_grid: bool,
) -> Result<Arc<CalibrationGrid>, ApplyExecutionError> {
    let interpolation = table.interp;
    let path = &table.path;
    let (gain, opacity) = match grid.as_ref() {
        CalibrationGrid::Complex(gain) => (gain, false),
        CalibrationGrid::SpectralOpacity(gain) => (gain, true),
        _ => return Ok(grid),
    };
    if input.len() != gain.channel_count {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: path.display().to_string(),
            shape: gain.values.shape().to_vec(),
        });
    }
    let key = MaterializedGridKey {
        row: RowGeometryKey::new(0., -1, -1),
        kind: 4,
        data_spw_id: output.spw_id,
        grid_id: Arc::as_ptr(&grid) as usize,
    };
    let cache = cache.map(|cache| {
        if transient_grid {
            &mut cache.transient_frequency_grids
        } else {
            &mut cache.materialized_grids
        }
    });
    if let Some(prepared) = cache.as_ref().and_then(|grids| grids.get(&key)) {
        return Ok(prepared.clone());
    }
    let prepared = resample(
        gain,
        input,
        &output.channel_frequencies_hz,
        if matches!(table.time_parameter_family, Some(TimeParameterFamily::Sky)) {
            interpolation.sky_frequency_mode()
        } else {
            interpolation.frequency_mode()
        },
        matches!(
            interpolation,
            ApplyInterpolationMode::Casa {
                relative_frequency: true,
                ..
            }
        ),
        if opacity {
            Some(TimeParameterFamily::SpectralOpacity)
        } else {
            table.time_parameter_family
        },
    )?;
    let prepared = Arc::new(if opacity {
        CalibrationGrid::SpectralOpacity(prepared)
    } else {
        CalibrationGrid::Complex(prepared)
    });
    if let Some(cache) = cache {
        cache.insert(key, prepared.clone());
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
    temperature_channels: Option<DataChannelGrid<'_>>,
) -> Option<Vec<f32>> {
    let (CalibrationGrid::Complex(first), CalibrationGrid::Complex(second)) = (first, second)
    else {
        return None;
    };
    let scale = |grid: &GainGrid, receptor| {
        temperature_channels.map_or_else(
            || mean_power(grid, receptor),
            |channels| inverse_mean_tsys(grid, receptor, channels),
        )
    };
    (0..correlations.len())
        .map(|index| {
            let (left, right) = receptors(correlations, index)?;
            Some(scale(first, left) * scale(second, right))
        })
        .collect()
}

fn inverse_mean_tsys(grid: &GainGrid, receptor: usize, channels: DataChannelGrid<'_>) -> f32 {
    let receptor = receptor.min(grid.receptor_count.saturating_sub(1));
    let mut total = 0.0_f64;
    let mut count = 0;
    for index in 0..channels.frequencies_hz.len() {
        let channel = channels.original_index(index);
        let gain = f64::from(grid.value_at(receptor, channel).re);
        if !grid.flag_at(receptor, channel) && gain > 0.0 {
            // The interpolated temperature is the inverse square of this gain.
            total += 1.0 / (gain * gain);
            count += 1;
        }
    }
    if count == 0 {
        1.0
    } else {
        (f64::from(count) / total) as f32
    }
}

pub(super) fn receptors(correlations: &[i32], index: usize) -> Option<(usize, usize)> {
    let pair = correlation_receptors(correlations[index])?;
    // CASA uses the first receptor for single-correlation bandpass and
    // switched-power application, including a sole YY or LL product.
    Some(if correlations.len() == 1 {
        (0, 0)
    } else {
        pair
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_wide_bandpass_shares_payload_and_resampling_across_times() {
        check_constant_wide_table_shares_payload(None, 2.);
    }

    #[test]
    fn constant_wide_tsys_shares_payload_and_resampling_across_times() {
        check_constant_wide_table_shares_payload(
            Some(TimeParameterFamily::Tsys),
            2_f32.sqrt().recip(),
        );
    }

    fn check_constant_wide_table_shares_payload(
        family: Option<TimeParameterFamily>,
        expected: f32,
    ) {
        use crate::lookup::{TimePlan, TimeQuery};
        let real = family.is_some();
        let channels = 4096;
        let raw = Arc::new(CalibrationGrid::Complex(GainGrid {
            receptor_count: 2,
            channel_count: channels,
            values: ArrayD::from_elem(IxDyn(&[2, channels]), Complex32::new(2., 0.)),
            flags: ArrayD::from_elem(IxDyn(&[2, channels]), false),
        }));
        let table = LoadedCalibrationTable {
            baseline: false,
            path: "constant.bcal".into(),
            interp: ApplyInterpolationMode::Linear,
            supports_calwt: true,
            engine: None,
            antpos_tropo: None,
            ionosphere: None,
            sky_exposures: None,
            spline: None,
            time_parameter_family: family,
            phase_delay: false,
            callibrary: None,
            time_plans: HashMap::new(),
            missing_grids: HashMap::new(),
            solutions: HashMap::from([(
                (0, 0, 0),
                vec![CalibrationSolution {
                    time_seconds: 0.,
                    grid: raw.clone(),
                }],
            )]),
        };
        let options = crate::TimeInterpolationOptions::new(crate::TimeInterpolation::Linear);
        let context = TimePlan::apply_context(&table, options).unwrap();
        let plan = TimePlan::for_apply(&table, options, real, &context).unwrap();
        let input = (0..channels)
            .map(|channel| 1e9 + channel as f64 * 1e6)
            .collect::<Vec<_>>();
        let output = crate::SpectralWindowPlan {
            spw_id: 0,
            num_chan: channels as i32,
            ref_frequency_hz: 1e9,
            channel_frequencies_hz: input.iter().map(|frequency| frequency + 0.25e6).collect(),
        };
        let mut cache = RowGeometryCache::default();
        let mut first = None;
        for time in 0..1000 {
            let selected = plan
                .registered_grid(
                    &TimeQuery {
                        observation: 0,
                        scan: 1,
                        window: 0,
                        antenna: 0,
                        time: f64::from(time),
                        frequency_hz: None,
                    },
                    real,
                )
                .unwrap();
            assert!(Arc::ptr_eq(&selected, &raw));
            let sampled =
                prepare(selected, &input, &output, Some(&mut cache), &table, false).unwrap();
            if let Some(first) = &first {
                assert!(Arc::ptr_eq(first, &sampled));
            } else {
                first = Some(sampled);
            }
        }
        assert_eq!(cache.materialized_grids.len(), 1);
        for time in [1.0_f64, 2.0, 3.0] {
            let key = calibration_lookup_key(0, 0, 0, 0, time, ApplyInterpolationMode::Linear);
            let interpolated = cache
                .calibration_lookup(key, Some(time.to_bits()), || {
                    Ok(Some(fields::GridAtTime {
                        grid: Arc::new(raw.as_ref().clone()),
                        interpolated: true,
                    }))
                })
                .unwrap()
                .unwrap();
            prepare(
                interpolated.grid,
                &input,
                &output,
                Some(&mut cache),
                &table,
                true,
            )
            .unwrap();
            assert_eq!(cache.transient_lookups.len(), 1);
            assert_eq!(cache.transient_frequency_grids.len(), 1);
            assert_eq!(cache.materialized_grids.len(), 1);
        }
        let CalibrationGrid::Complex(gain) = first.unwrap().as_ref().clone() else {
            unreachable!()
        };
        assert!(
            gain.values
                .iter()
                .all(|value| (*value - Complex32::new(expected, 0.)).norm() < 1e-6)
        );
        assert!(gain.flags.iter().all(|flag| !*flag));
    }

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
            let result = resample(
                &gain,
                &frequencies,
                &[0., 1., 2., 3., 4.],
                crate::FrequencyInterpolation::Linear,
                false,
                None,
            )
            .unwrap();
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
                    &[9],
                    None,
                ),
                Some(vec![100.])
            );
            gain.flags.fill(true);
            let missing = resample(
                &gain,
                &frequencies,
                &[2.],
                crate::FrequencyInterpolation::Linear,
                false,
                None,
            )
            .unwrap();
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
        let result = resample(
            &grid,
            &[1., 2., 3.],
            &[2.],
            crate::FrequencyInterpolation::Linear,
            false,
            None,
        )
        .unwrap();
        assert!((result.values[[0, 0]] - Complex32::new(-1., 0.)).norm() < 1e-6);
        assert!(!result.flags[[0, 0]]);
    }
}
