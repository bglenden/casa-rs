// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn truncate_for_column(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }

    let mut chars = value.chars().collect::<Vec<_>>();
    if chars.len() > width {
        chars.truncate(width);
        if let Some(last) = chars.last_mut() {
            *last = '*';
        }
    }
    let mut truncated = chars.into_iter().collect::<String>();
    while truncated.chars().count() < width {
        truncated.push(' ');
    }
    truncated
}

pub(super) fn format_elapsed(value: Option<f64>) -> String {
    match value {
        Some(seconds) => format!("{seconds:.0} seconds"),
        None => "unknown".to_string(),
    }
}

pub(super) fn mean_or_zero(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

pub(super) fn spectral_window_frequency_bounds(
    chan_freq: &[f64],
    chan_width: &[f64],
) -> (f64, f64) {
    let mut min_frequency_hz = f64::INFINITY;
    let mut max_frequency_hz = f64::NEG_INFINITY;

    for (&frequency_hz, &width_hz) in chan_freq.iter().zip(chan_width.iter()) {
        if !frequency_hz.is_finite() || !width_hz.is_finite() {
            continue;
        }
        let edge_a = frequency_hz - width_hz / 2.0;
        let edge_b = frequency_hz + width_hz / 2.0;
        min_frequency_hz = min_frequency_hz.min(edge_a.min(edge_b));
        max_frequency_hz = max_frequency_hz.max(edge_a.max(edge_b));
    }

    if min_frequency_hz.is_finite() && max_frequency_hz.is_finite() {
        return (min_frequency_hz, max_frequency_hz);
    }

    if let Some(&center_frequency_hz) = chan_freq.first() {
        let width_hz = chan_width.first().copied().unwrap_or(0.0).abs();
        return (
            center_frequency_hz - width_hz / 2.0,
            center_frequency_hz + width_hz / 2.0,
        );
    }

    (0.0, 0.0)
}

pub(super) fn extract_first_f64_opt(value: Option<&ArrayValue>) -> MsResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        ArrayValue::Float64(values) => Ok(values.iter().next().copied()),
        other => Err(MsError::ColumnTypeMismatch {
            column: "array".to_string(),
            table: "listobs".to_string(),
            expected: "Float64 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

pub(super) fn join_or_blank(values: &[String]) -> String {
    values.join(", ")
}

pub(super) fn format_compact_antenna_lines(antennas: &[AntennaSummary]) -> Vec<String> {
    if antennas.is_empty() {
        return Vec::new();
    }

    let mut lines = Vec::new();
    let mut line = String::new();
    let mut first_id = antennas[0].antenna_id;
    let mut last_id = antennas[0].antenna_id;
    let max_id = antennas
        .iter()
        .map(|antenna| antenna.antenna_id)
        .max()
        .unwrap_or(first_id);

    for antenna in antennas {
        let fragment = format!("'{}'='{}'", antenna.name, antenna.station);
        let mut candidate = line.clone();
        if !candidate.is_empty() {
            candidate.push_str(", ");
        }
        candidate.push_str(&fragment);
        if candidate.len() > 55 && !line.is_empty() {
            lines.push(format!(
                "   ID={:>8}: {}",
                format!("{first_id}-{last_id}"),
                line
            ));
            line = fragment;
            first_id = antenna.antenna_id;
        } else {
            line = candidate;
        }
        last_id = antenna.antenna_id;
        if antenna.antenna_id == max_id {
            lines.push(format!(
                "   ID={:>8}: {}",
                format!("{first_id}-{last_id}"),
                line
            ));
        }
    }

    lines
}

pub(super) fn join_with_padding(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("{value:<4}"))
        .collect::<String>()
        .trim_end()
        .to_string()
}

pub(super) fn format_i32_bracket_list(values: &[i32]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(super) fn format_f64_bracket_list(values: &[f64]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format_float_compact(*value, 3))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Format a finite floating-point scalar with at most `decimals` fractional digits.
pub fn format_float_compact(value: f64, decimals: usize) -> String {
    let epsilon = 10f64.powi(-(decimals as i32)) / 2.0;
    let normalized = if value.abs() < epsilon { 0.0 } else { value };
    let mut formatted = format!("{normalized:.decimals$}");
    while formatted.contains('.') && formatted.ends_with('0') {
        formatted.pop();
    }
    if formatted.ends_with('.') {
        formatted.pop();
    }
    formatted
}

/// casacore's `Stokes::name` of a `CORR_TYPE` code, from `I` (1) to
/// `Pangle` (32); `??` for a code casacore does not define.
pub fn stokes_name(code: i32) -> &'static str {
    const NAMES: [&str; 32] = [
        "I",
        "Q",
        "U",
        "V",
        "RR",
        "RL",
        "LR",
        "LL",
        "XX",
        "XY",
        "YX",
        "YY",
        "RX",
        "RY",
        "LX",
        "LY",
        "XR",
        "XL",
        "YR",
        "YL",
        "PP",
        "PQ",
        "QP",
        "QQ",
        "RCircular",
        "LCircular",
        "Linear",
        "Ptotal",
        "Plinear",
        "PFtotal",
        "PFlinear",
        "Pangle",
    ];
    usize::try_from(code)
        .ok()
        .and_then(|code| code.checked_sub(1))
        .and_then(|index| NAMES.get(index))
        .copied()
        .unwrap_or("??")
}
