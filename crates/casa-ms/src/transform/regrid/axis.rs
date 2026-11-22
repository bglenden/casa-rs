// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral axes for the MeasurementSet regrid task.

use casa_types::quanta::{Quantity, Unit};

use crate::spectral_selection::{
    CasaChannelFrequencies, RegridQuantity, RegridRequest, casa_data_channel_frequencies,
    parse_rest_frequency_hz,
};

use super::super::{MsRegridOptions, MsTransformError};

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
    let phys = |index: i64| {
        if descending {
            (channel_count - 1 - index) as usize
        } else {
            index as usize
        }
    };
    let starts_high = (step < 0) ^ descending;
    let mut span = count.map_or(channel_count, |channels| channels as i64 * requested_step);
    let from = i64::from(requested_start);
    if from < 0 || from >= channel_count {
        return Err(MsTransformError::Spectrum(format!(
            "SPW start {requested_start} outside valid range 0–{}.",
            channel_count - 1
        )));
    }
    let centre = if starts_high {
        phys(from) as i64 - span / 2
    } else {
        phys(from) as i64 + span / 2
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
    let step = requested_step.min(span);
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
