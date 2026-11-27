// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn visibility_flags<'a>(
    column: &'a SelectedArrayColumn<'_>,
    row: usize,
    [correlations, channels]: [usize; 2],
) -> Result<ndarray::ArrayView2<'a, bool>, String> {
    let flags = match column.get(row)? {
        ArrayValue::Bool(values) => values.view().into_dimensionality::<Ix2>().map_err(|_| {
            "msexplore expects FLAG cells with shape [num_corr, num_chan]".to_string()
        })?,
        other => {
            return Err(format!(
                "msexplore requires BOOL flag cells, found {:?}",
                other.primitive_type()
            ));
        }
    };
    if flags.shape() != [correlations, channels] {
        return Err(format!(
            "visibility flag shape {:?} does not match data shape [{}, {}]",
            flags.shape(),
            correlations,
            channels,
        ));
    }
    Ok(flags)
}

pub(super) fn float_axis_values(
    array: &ArrayValue,
    expected_len: usize,
    column_name: &str,
) -> Result<Vec<f64>, String> {
    let values = match array {
        ArrayValue::Float32(values) => values
            .view()
            .into_dimensionality::<Ix1>()
            .map_err(|_| format!("msexplore expects {column_name} cells with shape [num_corr]"))?
            .iter()
            .map(|value| *value as f64)
            .collect::<Vec<_>>(),
        ArrayValue::Float64(values) => values
            .view()
            .into_dimensionality::<Ix1>()
            .map_err(|_| format!("msexplore expects {column_name} cells with shape [num_corr]"))?
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        other => {
            return Err(format!(
                "msexplore requires FLOAT {column_name} cells, found {:?}",
                other.primitive_type()
            ));
        }
    };
    if values.len() != expected_len {
        return Err(format!(
            "{column_name} shape [{}] does not match correlation count {expected_len}",
            values.len()
        ));
    }
    Ok(values)
}

#[derive(Clone, Copy, Default)]
pub(super) struct VisibilityBinSamples<'a> {
    pub(super) values: &'a [Complex64],
    pub(super) weights: &'a [f64],
    pub(super) projection: Option<(VisibilityColumnProjection, [Complex64; 2])>,
    pub(super) signed_amplitude: bool,
    pub(super) zero_invalid_weight: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum VisibilityColumnProjection {
    VectorResidual,
    ScalarResidual,
    VectorRatio,
    ScalarRatio,
}

impl VisibilityColumnProjection {
    pub(super) fn axis_value(
        self,
        axis: MsAxis,
        [observed, model]: [Complex64; 2],
        averaged_operands: bool,
    ) -> Option<f64> {
        let as_complex_float =
            |value: Complex64| casa_core::Complex32::new(value.re as f32, value.im as f32);
        match self {
            Self::VectorResidual => Some(Self::vector_axis_value(axis, observed - model)),
            Self::ScalarResidual if averaged_operands => {
                let observed = as_complex_float(observed);
                let model = as_complex_float(model);
                Some(f64::from(match axis {
                    MsAxis::Amplitude => observed.norm() - model.norm(),
                    MsAxis::Phase => Self::float_phase_degrees(observed.arg() - model.arg()),
                    MsAxis::Real => observed.re - model.re,
                    MsAxis::Imaginary => observed.im - model.im,
                    _ => unreachable!("visibility projection requires a visibility axis"),
                }))
            }
            Self::ScalarResidual => {
                let value = match axis {
                    MsAxis::Amplitude => observed.norm() - model.norm(),
                    MsAxis::Phase => (observed.arg() - model.arg()).to_degrees(),
                    MsAxis::Real => observed.re - model.re,
                    MsAxis::Imaginary => observed.im - model.im,
                    _ => unreachable!("visibility projection requires a visibility axis"),
                };
                value.is_finite().then_some(value)
            }
            Self::VectorRatio => {
                // CASA caches the complex float quotient before converting its axes.
                Some(Self::vector_axis_value(
                    axis,
                    Self::complex_quotient(observed, model),
                ))
            }
            Self::ScalarRatio => {
                let observed = as_complex_float(observed);
                let model = as_complex_float(model);
                Some(f64::from(match axis {
                    MsAxis::Amplitude => observed.norm() / model.norm(),
                    MsAxis::Phase => Self::float_phase_degrees(observed.arg() / model.norm()),
                    // CASA's scalar ratio uses component differences for these axes.
                    MsAxis::Real => observed.re - model.re,
                    MsAxis::Imaginary => observed.im - model.im,
                    _ => unreachable!("visibility projection requires a visibility axis"),
                }))
            }
        }
    }

    pub(super) fn complex_quotient(observed: Complex64, model: Complex64) -> Complex64 {
        if model == Complex64::new(0.0, 0.0) {
            // Match C++ complex division at a zero denominator for text export.
            let infinity = f64::INFINITY.copysign(model.re);
            Complex64::new(infinity * observed.re, infinity * observed.im)
        } else if observed.re.is_finite()
            && observed.im.is_finite()
            && (model.re.is_infinite() || model.im.is_infinite())
        {
            // C++ division keeps signed zero components for a finite value
            // over an infinite model, including an overflowed Float mean.
            let direction = |value: f64| {
                let magnitude = if value.is_infinite() {
                    1.0_f64
                } else {
                    0.0_f64
                };
                magnitude.copysign(value)
            };
            let model_direction = Complex64::new(direction(model.re), direction(model.im));
            observed * model_direction.conj() * 0.0
        } else {
            observed / model
        }
    }

    pub(super) fn vector_axis_value(axis: MsAxis, value: Complex64) -> f64 {
        let value = casa_core::Complex32::new(value.re as f32, value.im as f32);
        f64::from(match axis {
            MsAxis::Amplitude => value.norm(),
            MsAxis::Phase => Self::float_phase_degrees(value.arg()),
            MsAxis::Real => value.re,
            MsAxis::Imaginary => value.im,
            _ => unreachable!("visibility projection requires a visibility axis"),
        })
    }

    pub(super) fn float_phase_degrees(radians: f32) -> f32 {
        // The cache multiplies the Float array by 180 before dividing by pi.
        (radians * 180.0) / std::f32::consts::PI
    }
}

#[derive(Clone, Copy)]
pub(super) struct RowCoordinates {
    pub(super) time: f64,
    pub(super) interval: Option<f64>,
    pub(super) uvw: [f64; 3],
    pub(super) observatory_geometry: bool,
}

pub(super) fn uvw_coordinate(
    axis: MsAxis,
    uvw: [f64; 3],
    bin: &ChannelBin,
    spectrum: Option<&SpectralContext>,
) -> Option<f64> {
    let metres = uvw_metres_coordinate(axis, uvw);
    if axis.uses_wavelengths() {
        let frequency_hz = spectrum?.channel_frequencies_hz.get(bin.start)?;
        Some(metres_to_wavelengths(metres, *frequency_hz))
    } else {
        Some(metres)
    }
}

pub(super) fn metres_to_wavelengths(metres: f64, frequency_hz: f64) -> f64 {
    const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;
    metres / SPEED_OF_LIGHT_M_S * frequency_hz
}

pub(super) fn uvw_metres_coordinate(axis: MsAxis, [u, v, w]: [f64; 3]) -> f64 {
    match axis {
        MsAxis::UvDistance | MsAxis::UvWave => (u * u + v * v).sqrt(),
        MsAxis::U | MsAxis::UWave => u,
        MsAxis::V | MsAxis::VWave => v,
        MsAxis::W | MsAxis::WWave => w,
        _ => unreachable!("UVW coordinates require a UVW axis"),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compute_axis_value(
    axis: MsAxis,
    visibility: VisibilityBinSamples<'_>,
    flag_samples: &[bool],
    weight_spectrum_samples: &[f64],
    sigma_spectrum_samples: &[f64],
    flag_row_value: bool,
    row_weight: f64,
    row_sigma: f64,
    field_id_value: i32,
    antenna1_value: i32,
    scalar_average: bool,
    channel_bin: &ChannelBin,
    spectral_context: Option<&SpectralContext>,
    row: RowCoordinates,
    geometry_engine: Option<&MsCalEngine>,
) -> Result<Option<f64>, String> {
    if axis.is_visibility_math() {
        let weighted = axis == MsAxis::WeightedAmplitude;
        let axis = if weighted { MsAxis::Amplitude } else { axis };
        let apply_weight = |value: f64| {
            if weighted {
                weight_amplitude(value, row_weight)
            } else {
                value
            }
        };
        if let Some((projection, operands)) = visibility.projection {
            return Ok(projection
                .axis_value(axis, operands, channel_bin.averaged)
                .map(apply_weight));
        }
        let axis = if visibility.signed_amplitude && axis == MsAxis::Amplitude {
            MsAxis::Real
        } else {
            axis
        };
        return Ok(compute_weighted_visibility_math(
            axis,
            visibility.values,
            visibility.weights,
            scalar_average,
            visibility.zero_invalid_weight,
        )
        .map(apply_weight));
    }
    match axis {
        MsAxis::Time => Ok(Some(row.time)),
        MsAxis::Interval => Ok(row.interval),
        axis if axis.uses_uvw() => Ok(uvw_coordinate(axis, row.uvw, channel_bin, spectral_context)),
        MsAxis::Channel => Ok(Some(channel_bin.display_channel() as f64)),
        MsAxis::Frequency => {
            Ok(spectral_context.and_then(|spectrum| spectrum.mean_frequency_hz(channel_bin)))
        }
        MsAxis::Velocity => {
            let Some(spectral_context) = spectral_context else {
                return Ok(None);
            };
            let Some(mean_frequency_hz) = spectral_context.mean_frequency_hz(channel_bin) else {
                return Ok(None);
            };
            Ok(Some(
                velocity_ms_from_frequency_hz(
                    mean_frequency_hz,
                    spectral_context.rest_frequency_hz,
                    spectral_context.doppler_ref,
                )
                .map_err(|error| error.to_string())?
                    / 1000.0,
            ))
        }
        MsAxis::Weight => Ok(Some(row_weight)),
        MsAxis::Sigma => Ok(Some(row_sigma)),
        MsAxis::WeightSpectrum => Ok(average_float_samples(weight_spectrum_samples)),
        MsAxis::SigmaSpectrum => Ok(average_float_samples(sigma_spectrum_samples)),
        MsAxis::Flag => Ok(compute_flag_value(flag_samples)),
        MsAxis::FlagRow => Ok(Some(if flag_row_value { 1.0 } else { 0.0 })),
        MsAxis::Azimuth => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for azimuth axis".to_string()
            })?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for azimuth axis"))?;
            let antenna_id = usize::try_from(antenna1_value)
                .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for azimuth axis"))?;
            geometry_engine
                .azel(row.time, field_id, antenna_id)
                .map(|(azimuth, _elevation)| Some(normalize_signed_degrees(azimuth.to_degrees())))
                .map_err(|error| error.to_string())
        }
        MsAxis::Elevation => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for elevation axis".to_string()
            })?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for elevation axis"))?;
            let azel = if row.observatory_geometry {
                geometry_engine.azel_observatory(row.time, field_id)
            } else {
                let antenna_id = usize::try_from(antenna1_value)
                    .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for elevation axis"))?;
                geometry_engine.azel(row.time, field_id, antenna_id)
            };
            azel.map(|(_azimuth, elevation)| Some(elevation.to_degrees()))
                .map_err(|error| error.to_string())
        }
        MsAxis::HourAngle => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for hour angle axis".to_string()
            })?;
            let field_id = usize::try_from(field_id_value)
                .map_err(|_| format!("invalid FIELD_ID {field_id_value} for hour angle axis"))?;
            let antenna_id = usize::try_from(antenna1_value)
                .map_err(|_| format!("invalid ANTENNA1 {antenna1_value} for hour angle axis"))?;
            geometry_engine
                .hour_angle(row.time, field_id, antenna_id)
                .map(|hour_angle| Some(hour_angle * 12.0 / std::f64::consts::PI))
                .map_err(|error| error.to_string())
        }
        MsAxis::ParallacticAngle => {
            let geometry_engine = geometry_engine.ok_or_else(|| {
                "internal error: missing geometry engine for parallactic angle axis".to_string()
            })?;
            let field_id = usize::try_from(field_id_value).map_err(|_| {
                format!("invalid FIELD_ID {field_id_value} for parallactic angle axis")
            })?;
            let antenna_id = usize::try_from(antenna1_value).map_err(|_| {
                format!("invalid ANTENNA1 {antenna1_value} for parallactic angle axis")
            })?;
            geometry_engine
                .parallactic_angle(row.time, field_id, antenna_id)
                .map(|parallactic_angle| Some(parallactic_angle.to_degrees()))
                .map_err(|error| error.to_string())
        }
        _ => Ok(None),
    }
}

pub(super) fn weight_amplitude(amplitude: f64, weight: f64) -> f64 {
    f64::from(amplitude as f32 * weight as f32)
}

pub(super) fn average_float_samples(samples: &[f64]) -> Option<f64> {
    if samples.is_empty() {
        None
    } else {
        Some(samples.iter().copied().sum::<f64>() / samples.len() as f64)
    }
}

pub(super) fn normalize_signed_degrees(angle_degrees: f64) -> f64 {
    let wrapped = (angle_degrees + 180.0).rem_euclid(360.0) - 180.0;
    if wrapped == -180.0 { 180.0 } else { wrapped }
}

pub(super) fn compute_flag_value(flags: &[bool]) -> Option<f64> {
    if flags.is_empty() {
        None
    } else if flags.iter().any(|flag| *flag) {
        Some(1.0)
    } else {
        Some(0.0)
    }
}

pub(super) fn compute_visibility_math(
    axis: MsAxis,
    samples: &[Complex64],
    scalar_average: bool,
) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    if scalar_average {
        match axis {
            MsAxis::Amplitude => {
                Some(samples.iter().map(|value| value.norm()).sum::<f64>() / samples.len() as f64)
            }
            MsAxis::Phase => {
                let (sin_sum, cos_sum) =
                    samples
                        .iter()
                        .fold((0.0, 0.0), |(sin_sum, cos_sum), value| {
                            let angle = value.arg();
                            (sin_sum + angle.sin(), cos_sum + angle.cos())
                        });
                Some(sin_sum.atan2(cos_sum).to_degrees())
            }
            MsAxis::Real => {
                Some(samples.iter().map(|value| value.re).sum::<f64>() / samples.len() as f64)
            }
            MsAxis::Imaginary => {
                Some(samples.iter().map(|value| value.im).sum::<f64>() / samples.len() as f64)
            }
            _ => None,
        }
    } else {
        let average = samples.iter().copied().sum::<Complex64>() / samples.len() as f64;
        match axis {
            MsAxis::Amplitude => Some(average.norm()),
            MsAxis::Phase => Some(average.arg().to_degrees()),
            MsAxis::Real => Some(average.re),
            MsAxis::Imaginary => Some(average.im),
            _ => None,
        }
    }
}

pub(super) fn compute_weighted_visibility_math(
    axis: MsAxis,
    samples: &[Complex64],
    weights: &[f64],
    scalar_average: bool,
    zero_invalid_weight: bool,
) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    if weights.len() != samples.len() || weights.is_empty() {
        return compute_visibility_math(axis, samples, scalar_average);
    }
    let weight_sum = weights.iter().sum::<f64>();
    if zero_invalid_weight && (weight_sum <= 0.0 || weight_sum.is_nan()) {
        return Some(0.0);
    }
    if weight_sum <= 0.0 {
        return compute_visibility_math(axis, samples, scalar_average);
    }
    if scalar_average {
        match axis {
            MsAxis::Amplitude => Some(
                samples
                    .iter()
                    .zip(weights.iter())
                    .map(|(value, weight)| value.norm() * *weight)
                    .sum::<f64>()
                    / weight_sum,
            ),
            MsAxis::Phase => {
                let (sin_sum, cos_sum) = samples.iter().zip(weights.iter()).fold(
                    (0.0, 0.0),
                    |(sin_sum, cos_sum), (value, weight)| {
                        let angle = value.arg();
                        (
                            sin_sum + angle.sin() * *weight,
                            cos_sum + angle.cos() * *weight,
                        )
                    },
                );
                Some(sin_sum.atan2(cos_sum).to_degrees())
            }
            MsAxis::Real => Some(
                samples
                    .iter()
                    .zip(weights.iter())
                    .map(|(value, weight)| value.re * *weight)
                    .sum::<f64>()
                    / weight_sum,
            ),
            MsAxis::Imaginary => Some(
                samples
                    .iter()
                    .zip(weights.iter())
                    .map(|(value, weight)| value.im * *weight)
                    .sum::<f64>()
                    / weight_sum,
            ),
            _ => None,
        }
    } else {
        let average = weighted_visibility_mean(samples, weights, weight_sum);
        match axis {
            MsAxis::Amplitude => Some(average.norm()),
            MsAxis::Phase => Some(average.arg().to_degrees()),
            MsAxis::Real => Some(average.re),
            MsAxis::Imaginary => Some(average.im),
            _ => None,
        }
    }
}

pub(super) fn weighted_visibility_mean(
    samples: &[Complex64],
    weights: &[f64],
    weight_sum: f64,
) -> Complex64 {
    samples
        .iter()
        .zip(weights)
        .map(|(value, weight)| *value * *weight)
        .sum::<Complex64>()
        / weight_sum
}

pub(super) fn collect_bin_samples_into(
    grid: &ComplexGrid,
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_index: usize,
    bin: &ChannelBin,
    mask: plotms_ordinates::BinSampleMask,
    mut weights: Option<(&FloatGrid, &mut Vec<f64>)>,
    samples: &mut Vec<Complex64>,
) {
    samples.clear();
    if let Some((_, values)) = &mut weights {
        values.clear();
    }
    if corr_index >= grid.corr_count {
        return;
    }
    for channel in bin.channels() {
        if channel >= grid.chan_count
            || (mask == plotms_ordinates::BinSampleMask::Clear && flags[(corr_index, channel)])
        {
            continue;
        }
        let index = corr_index * grid.chan_count + channel;
        let value = grid.values[index];
        if mask == plotms_ordinates::BinSampleMask::FlaggedAverage
            || (value.re.is_finite() && value.im.is_finite())
        {
            samples.push(value);
            if let Some((spectrum, values)) = &mut weights {
                values.push(spectrum.values[index]);
            }
        }
    }
}

pub(super) fn collect_bin_flags_into(
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_index: usize,
    bin: &ChannelBin,
    out: &mut Vec<bool>,
) {
    out.clear();
    if corr_index >= flags.nrows() {
        return;
    }
    for chan_index in bin.channels() {
        if chan_index < flags.ncols() {
            out.push(flags[(corr_index, chan_index)]);
        }
    }
}

pub(super) fn collect_bin_float_samples_into(
    grid: &FloatGrid,
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_index: usize,
    bin: &ChannelBin,
    samples: &mut Vec<f64>,
) {
    samples.clear();
    if corr_index >= grid.corr_count {
        return;
    }
    for chan_index in bin.channels() {
        if chan_index >= grid.chan_count
            || chan_index >= flags.ncols()
            || flags[(corr_index, chan_index)]
        {
            continue;
        }
        let value = grid.values[corr_index * grid.chan_count + chan_index];
        if value.is_finite() {
            samples.push(value);
        }
    }
}

pub(super) fn select_correlation_slots(
    corr_count: usize,
    corr_types: &[i32],
    requested_corr_codes: Option<&[i32]>,
) -> Vec<(usize, String)> {
    let requested_corr_codes = requested_corr_codes.unwrap_or(&[]);
    let mut slots = Vec::new();
    for corr_index in 0..corr_count {
        let corr_label = corr_types
            .get(corr_index)
            .map(|code| casa_ms::stokes_name(*code).to_string())
            .unwrap_or_else(|| format!("corr-{corr_index}"));
        let corr_code = corr_types.get(corr_index).copied();
        if requested_corr_codes.is_empty()
            || corr_code.is_some_and(|code| requested_corr_codes.contains(&code))
        {
            slots.push((corr_index, corr_label));
        }
    }
    slots
}

pub(super) fn complex_grid_from_array(array: &ArrayValue) -> Result<ComplexGrid, String> {
    match array {
        ArrayValue::Complex32(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                "msexplore expects complex visibility cells with shape [num_corr, num_chan]"
                    .to_string()
            })?;
            Ok(ComplexGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values
                    .iter()
                    .map(|value| Complex64::new(value.re as f64, value.im as f64))
                    .collect(),
                model: None,
                signed_amplitude: false,
            })
        }
        ArrayValue::Complex64(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                "msexplore expects complex visibility cells with shape [num_corr, num_chan]"
                    .to_string()
            })?;
            Ok(ComplexGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values.iter().copied().collect(),
                model: None,
                signed_amplitude: false,
            })
        }
        other => Err(format!(
            "msexplore requires complex visibility data, found {:?}",
            other.primitive_type()
        )),
    }
}

pub(super) fn float_visibility_grid(array: &ArrayValue) -> Result<ComplexGrid, String> {
    let ArrayValue::Float32(values) = array else {
        return Err(format!(
            "msexplore requires FLOAT_DATA cells with type FLOAT, found {:?}",
            array.primitive_type()
        ));
    };
    let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
        "msexplore expects FLOAT_DATA cells with shape [num_corr, num_chan]".to_string()
    })?;
    Ok(ComplexGrid {
        corr_count: values.nrows(),
        chan_count: values.ncols(),
        values: values
            .iter()
            .map(|&value| Complex64::new(f64::from(value), 0.0))
            .collect(),
        model: None,
        signed_amplitude: true,
    })
}

pub(super) fn float_grid_from_array(
    array: &ArrayValue,
    column_name: &str,
) -> Result<FloatGrid, String> {
    match array {
        ArrayValue::Float32(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                format!("msexplore expects {column_name} cells with shape [num_corr, num_chan]")
            })?;
            Ok(FloatGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values.iter().map(|value| *value as f64).collect(),
            })
        }
        ArrayValue::Float64(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                format!("msexplore expects {column_name} cells with shape [num_corr, num_chan]")
            })?;
            Ok(FloatGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values.iter().copied().collect(),
            })
        }
        other => Err(format!(
            "msexplore requires FLOAT {column_name} data, found {:?}",
            other.primitive_type()
        )),
    }
}

pub(super) fn scalar_values_to_grid(values: &[f64], chan_count: usize) -> FloatGrid {
    FloatGrid {
        corr_count: values.len(),
        chan_count,
        values: values
            .iter()
            .flat_map(|value| std::iter::repeat_n(*value, chan_count))
            .collect(),
    }
}

pub(super) fn check_grid_shapes(left: &ComplexGrid, right: &ComplexGrid) -> Result<(), String> {
    if left.corr_count != right.corr_count || left.chan_count != right.chan_count {
        return Err(format!(
            "msexplore data columns have mismatched shapes [{}, {}] and [{}, {}]",
            left.corr_count, left.chan_count, right.corr_count, right.chan_count
        ));
    }
    Ok(())
}

pub(super) fn combine_grids<F>(
    left: ComplexGrid,
    right: ComplexGrid,
    mut combine: F,
) -> Result<ComplexGrid, String>
where
    F: FnMut(Complex64, Complex64) -> Complex64,
{
    check_grid_shapes(&left, &right)?;
    Ok(ComplexGrid {
        corr_count: left.corr_count,
        chan_count: left.chan_count,
        values: left
            .values
            .into_iter()
            .zip(right.values)
            .map(|(left, right)| combine(left, right))
            .collect(),
        model: None,
        signed_amplitude: false,
    })
}
