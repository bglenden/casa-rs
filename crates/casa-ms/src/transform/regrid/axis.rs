// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral axes for the MeasurementSet regrid task.

use casa_types::quanta::{Quantity, Unit};

use crate::spectral_selection::{
    CasaChannelFrequencies, RegridQuantity, RegridRequest, casa_data_channel_frequencies,
    parse_rest_frequency_hz,
};

use super::super::{MsRegridOptions, MsTransformError};

/// Grid arguments of the legacy in-place MeasurementSet spectral tools.
#[derive(Clone, Copy, Debug)]
pub enum LegacySpectralGrid {
    /// The mode, start and channel count established by `cvel()`.
    Cvel,
    /// The numeric band supplied to `regridspw(mode="chan")`.
    CenteredChannels {
        /// Centre expressed as an original channel index.
        centre: f64,
        /// Bandwidth in units of original channels.
        bandwidth: f64,
        /// Width of a full output channel in units of original channels.
        channel_width: f64,
    },
}

pub(super) fn output_axis(
    options: &MsRegridOptions,
    centres: &[f64],
    widths: &[f64],
    maximum_channels: usize,
) -> Result<CasaChannelFrequencies, MsTransformError> {
    let error = MsTransformError::Spectrum;
    let mode = options.mode.as_str();
    let length = centres.len();
    if length == 0 || length != widths.len() {
        return Err(error(
            "Regridding needs matching non-empty frequency and width arrays.".into(),
        ));
    }
    let count = usize::try_from(options.nchan)
        .ok()
        .filter(|&n| n > 0)
        .map(|channels| {
            channels
                .checked_mul(options.nspw.max(1))
                .filter(|&count| count <= i32::MAX as usize)
                .ok_or_else(|| {
                    error("Regridding channel count exceeds the MeasurementSet limit.".into())
                })
        })
        .transpose()?;
    let rest_frequency_hz = if options.restfreq.trim().is_empty() {
        if mode == "velocity" {
            return Err(MsTransformError::RegridParameter {
                parameter: "restfreq",
                reason: "Parameter restfreq must be set when mode='velocity'".into(),
            });
        }
        0.0
    } else {
        parse_rest_frequency_hz(&options.restfreq).map_err(|source| error(source.to_string()))?
    };
    let mut request = RegridRequest {
        quantity: RegridQuantity::Frequency,
        start: None,
        width: None,
        nchan: count,
        rest_frequency_hz,
    };
    match mode {
        "channel" => {
            let start = channel_arg(&options.start, 0);
            let step = channel_arg(&options.width, 1);
            let descending = centres.first() > centres.last();
            let first = if descending { length - 1 } else { 0 };
            let start = usize::try_from(start).ok();
            let selected = start.filter(|&value| value < length).unwrap_or(first);
            let mut width = widths[first].abs() * f64::from(step.unsigned_abs().max(1));
            let mut start_is_end = (step < 0) ^ descending;
            if start.is_some_and(|index| index >= length) {
                width = width.abs();
                start_is_end = false;
            }
            let edge = if start_is_end {
                centres[selected] + widths[selected].abs() / 2.
            } else {
                centres[selected] - widths[selected].abs() / 2.
            };
            request.start = start.map(|_| {
                if start_is_end {
                    edge - width / 2.
                } else {
                    edge + width / 2.
                }
            });
            request.width = Some(if start_is_end { -width } else { width });
        }
        "channel_b" => return channel_bounds(options, centres, widths, count),
        "frequency" => {
            request.start = quantity_arg("start", &options.start, "Hz", "0")?;
            request.width = quantity_arg("width", &options.width, "Hz", "1")?;
        }
        "velocity" => {
            request.quantity = match options.veltype.to_ascii_lowercase().as_str() {
                "optical" => RegridQuantity::OpticalVelocity,
                "radio" => RegridQuantity::RadioVelocity,
                _ => return Err(super::invalid_choice("veltype", &options.veltype)),
            };
            request.start = quantity_arg("start", &options.start, "m/s", "0")?;
            request.width = quantity_arg("width", &options.width, "m/s", "1")?;
        }
        _ => return Err(super::invalid_choice("mode", mode)),
    }
    casa_data_channel_frequencies(&request, centres, widths, maximum_channels)
        .map_err(|source| error(source.to_string()))
}

fn channel_arg(value: &str, default: i32) -> i32 {
    let value = value.trim();
    if value.is_empty() || value == "[]" {
        default
    } else {
        value.parse().unwrap_or_default()
    }
}

fn quantity_arg(
    parameter: &'static str,
    value: &str,
    unit: &str,
    default: &str,
) -> Result<Option<f64>, MsTransformError> {
    let value = value.trim();
    if value.is_empty() || value == "[]" || value == default {
        return Ok(None);
    }
    let unit = Unit::new(unit).expect("regridding uses a known frequency or velocity unit");
    let valid = Quantity::read(value)
        .ok()
        .and_then(|quantity| quantity.get_value_in(&unit).ok())
        .filter(|number| number.is_finite());
    valid.map(Some).ok_or_else(|| {
        let dimension = if unit.name() == "Hz" {
            "frequency"
        } else {
            "velocity"
        };
        MsTransformError::RegridParameter {
            parameter,
            reason: format!("Parameter {parameter} does not have valid {dimension} units"),
        }
    })
}

fn channel_bounds(
    options: &MsRegridOptions,
    frequencies: &[f64],
    widths: &[f64],
    count: Option<usize>,
) -> Result<CasaChannelFrequencies, MsTransformError> {
    let channel_count = frequencies.len() as i64;
    let step = channel_arg(&options.width, 1);
    let requested_step = i64::from(step.unsigned_abs().max(1));
    let requested_start = channel_arg(&options.start, 0);
    let descending = frequencies.first() > frequencies.last();
    let starts_high = (step < 0) ^ descending;
    let mut span = count.map_or(channel_count, |channels| channels as i64 * requested_step);
    let from = i64::from(requested_start);
    if from < 0 || from >= channel_count {
        return Err(MsTransformError::Spectrum(format!(
            "SPW start {requested_start} outside valid range 0–{}.",
            channel_count - 1
        )));
    }
    let from_ascending = if descending {
        channel_count - 1 - from
    } else {
        from
    };
    let centre = if starts_high {
        from_ascending - span / 2
    } else {
        from_ascending + span / 2
    };
    if centre - span / 2 < 0 {
        span = 2 * centre + 1;
    }
    if centre + span / 2 > channel_count {
        span = 2 * (channel_count - centre);
    }
    if span <= 0 {
        return Err(MsTransformError::Spectrum(
            "Requested channel bounds leave an empty SPW.".into(),
        ));
    }
    channel_groups(frequencies, widths, centre, span, requested_step.min(span))
}

/// Bound the legacy centre-based channel grid of SubMS::regridChanBounds.
/// A partial endpoint remains a channel instead of being dropped.
pub(super) fn centered_channel_bounds(
    frequencies: &[f64],
    widths: &[f64],
    centre: f64,
    bandwidth: f64,
    channel_width: f64,
) -> Result<CasaChannelFrequencies, MsTransformError> {
    let len = frequencies.len() as i64;
    let error = MsTransformError::Spectrum;
    if len == 0 || frequencies.len() != widths.len() {
        return Err(error(
            "Regridding needs matching non-empty frequency and width arrays.".into(),
        ));
    }
    let descending = frequencies.first() > frequencies.last();
    let centre = if centre < -1e30 {
        let midpoint = (frequencies[0] + frequencies[frequencies.len() - 1]) / 2.0;
        let midpoint_row = if descending {
            frequencies
                .iter()
                .rev()
                .position(|frequency| *frequency >= midpoint)
        } else {
            frequencies
                .iter()
                .position(|frequency| *frequency >= midpoint)
        };
        midpoint_row.ok_or_else(|| error("Invalid spectral window centre.".into()))? as i64
    } else if (0.0..len as f64).contains(&centre) {
        centre.floor() as i64
    } else {
        return Err(error(format!(
            "SPW center {centre} outside valid range 0–{}.",
            len - 1
        )));
    };
    let mut span = if bandwidth <= 0.0 {
        len
    } else {
        bandwidth.floor() as i64
    };
    if centre - span / 2 < 0 {
        span = 2 * centre + 1;
    }
    if centre + span / 2 > len {
        span = 2 * (len - centre);
    }
    if span <= 0 {
        return Err(error("Requested channel bounds leave an empty SPW.".into()));
    }
    let step = (channel_width.floor() as i64).clamp(1, span);
    channel_groups(frequencies, widths, centre, span, step)
}

fn channel_groups(
    frequencies: &[f64],
    widths: &[f64],
    centre: i64,
    span: i64,
    step: i64,
) -> Result<CasaChannelFrequencies, MsTransformError> {
    let channel_count = frequencies.len() as i64;
    let descending = frequencies.first() > frequencies.last();
    let phys = |index: i64| {
        if descending {
            (channel_count - 1 - index) as usize
        } else {
            index as usize
        }
    };
    let lower = centre - span / 2;
    let upper = (lower + span).min(channel_count);
    if lower < 0 || lower >= upper {
        return Err(MsTransformError::Spectrum(
            "Requested channel bounds leave an empty SPW.".into(),
        ));
    }
    let central_bin = if (span / step) % 2 == 1 {
        centre - step / 2
    } else {
        centre
    };
    let central_bin = central_bin.clamp(lower, upper);
    let mut bins = Vec::new();
    let mut right = central_bin;
    while right > lower {
        let left = lower.max(right - step);
        bins.push((left, right - 1));
        right = left;
    }
    bins.reverse();
    for left in (central_bin..upper).step_by(step as usize) {
        bins.push((left, upper.min(left + step) - 1));
    }
    if descending {
        bins.reverse();
    }
    let (centres_hz, steps_hz) = bins
        .into_iter()
        .map(|(low, high)| {
            let lo = frequencies[phys(low)] - widths[phys(low)].abs() / 2.;
            let hi = frequencies[phys(high)] + widths[phys(high)].abs() / 2.;
            ((hi + lo) / 2., hi - lo)
        })
        .unzip();
    Ok(CasaChannelFrequencies {
        centres_hz,
        steps_hz,
    })
}

#[cfg(test)]
mod tests {
    use super::centered_channel_bounds;

    #[test]
    fn a_centered_band_keeps_partial_endpoints_in_the_input_order() {
        let centres = [0., 1., 2., 3., 4., 5., 6.];
        let widths = [1.; 7];
        let ascending = centered_channel_bounds(&centres, &widths, -3e30, -1., 2.).unwrap();
        assert_eq!(ascending.centres_hz, [0.5, 2.5, 4.5, 6.]);
        assert_eq!(ascending.steps_hz, [2., 2., 2., 1.]);

        let descending =
            centered_channel_bounds(&[6., 5., 4., 3., 2., 1., 0.], &widths, -3e30, -1., 2.)
                .unwrap();
        assert_eq!(descending.centres_hz, [6., 4.5, 2.5, 0.5]);
        assert_eq!(descending.steps_hz, [1., 2., 2., 2.]);
    }

    #[test]
    fn the_vendored_regridspw_band_has_sixteen_two_channel_bins() {
        let channels = std::array::from_fn::<_, 63, _>(|slot| slot as f64);
        let axis = centered_channel_bounds(&channels, &[1.; 63], 24., 32., 2.).unwrap();
        assert_eq!(axis.centres_hz.len(), 16);
        assert_eq!(axis.centres_hz[0], 8.5);
        assert_eq!(axis.centres_hz[15], 38.5);
        assert_eq!(axis.steps_hz, [2.; 16]);
    }
}
