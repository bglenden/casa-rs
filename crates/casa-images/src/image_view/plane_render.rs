// SPDX-License-Identifier: LGPL-3.0-or-later
//! Plane rendering helpers for the read-only image browser backend.

use ndarray::Array2;

/// Timing breakdown for a single plane-raster build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct PlaneRenderTelemetry {
    pub plane_extract_ns: u64,
    pub stat_collection_ns: u64,
    pub histogram_ns: u64,
    pub rasterize_ns: u64,
    pub total_plane_ns: u64,
}

/// Backend-controlled stretch preset for 2D plane rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PlaneStretchPreset {
    Percentile99,
    Percentile95,
    MinMax,
    ZScale,
    Manual,
}

/// Autoscaling policy for plane rendering across cube stepping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PlaneAutoscaleMode {
    PerPlane,
    Frozen,
}

/// Normalized plane stretch settings applied by the image browser backend.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlaneStretchSettings {
    pub preset: PlaneStretchPreset,
    pub autoscale: PlaneAutoscaleMode,
    pub manual_clip: Option<(f64, f64)>,
}

impl Default for PlaneStretchSettings {
    fn default() -> Self {
        Self {
            preset: PlaneStretchPreset::Percentile99,
            autoscale: PlaneAutoscaleMode::PerPlane,
            manual_clip: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PlaneStats {
    pub finite_values: Vec<f64>,
    pub data_min: Option<f64>,
    pub data_max: Option<f64>,
    pub masked_or_non_finite_count: usize,
    pub no_finite_values: bool,
}

impl PlaneStats {
    pub(crate) fn clip_bounds_for(&self, stretch: &PlaneStretchSettings) -> Option<(f64, f64)> {
        let values = self.sorted_finite_values()?;
        let (min_value, max_value) = (*values.first()?, *values.last()?);
        match stretch.preset {
            PlaneStretchPreset::Percentile99 => Some(percentile_clip_bounds(&values, 0.01, 0.99)),
            PlaneStretchPreset::Percentile95 => Some(percentile_clip_bounds(&values, 0.05, 0.95)),
            PlaneStretchPreset::MinMax => Some((min_value, max_value)),
            PlaneStretchPreset::ZScale => zscale_like_clip_bounds(&values),
            PlaneStretchPreset::Manual => stretch.manual_clip,
        }
    }

    pub(crate) fn is_valid(&self, mask: Option<&Array2<bool>>, x: usize, y: usize) -> bool {
        mask.is_none_or(|data| data[[x, y]])
    }

    pub(crate) fn histogram_bins(&self, bins: usize) -> Vec<u32> {
        if bins == 0 || self.finite_values.is_empty() {
            return Vec::new();
        }
        let Some(min_value) = self.data_min else {
            return Vec::new();
        };
        let Some(max_value) = self.data_max else {
            return Vec::new();
        };
        let mut histogram = vec![0u32; bins];
        if (max_value - min_value).abs() < f64::EPSILON {
            histogram[bins / 2] = self.finite_values.len() as u32;
            return histogram;
        }
        for &value in &self.finite_values {
            let scaled = ((value - min_value) / (max_value - min_value)).clamp(0.0, 1.0);
            let index = ((scaled * bins.saturating_sub(1) as f64).round() as usize)
                .min(bins.saturating_sub(1));
            histogram[index] = histogram[index].saturating_add(1);
        }
        histogram
    }

    fn sorted_finite_values(&self) -> Option<Vec<f64>> {
        if self.finite_values.is_empty() {
            return None;
        }
        let mut values = self.finite_values.clone();
        values.sort_by(f64::total_cmp);
        Some(values)
    }
}

fn percentile_index(len: usize, percentile: f64) -> usize {
    ((len.saturating_sub(1)) as f64 * percentile).round() as usize
}

fn percentile_clip_bounds(values: &[f64], low_percentile: f64, high_percentile: f64) -> (f64, f64) {
    let low = percentile_index(values.len(), low_percentile);
    let high = percentile_index(values.len(), high_percentile);
    (values[low], values[high])
}

fn zscale_like_clip_bounds(values: &[f64]) -> Option<(f64, f64)> {
    let median = *values.get(values.len() / 2)?;
    let mut deviations = values
        .iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    let mad = *deviations.get(deviations.len() / 2)?;
    let sigma = (1.4826 * mad).max(f64::EPSILON);
    let min_value = *values.first()?;
    let max_value = *values.last()?;
    let clip_min = (median - 2.5 * sigma).max(min_value);
    let clip_max = (median + 2.5 * sigma).min(max_value);
    Some(if clip_min <= clip_max {
        (clip_min, clip_max)
    } else {
        (min_value, max_value)
    })
}

pub(crate) fn collect_plane_stats(plane: &Array2<f64>, mask: Option<&Array2<bool>>) -> PlaneStats {
    let mut finite_values = Vec::new();
    let mut data_min = None::<f64>;
    let mut data_max = None::<f64>;
    let mut masked_or_non_finite_count = 0usize;
    for x in 0..plane.shape()[0] {
        for y in 0..plane.shape()[1] {
            let value = plane[[x, y]];
            let valid_mask = mask.is_none_or(|mask_data| mask_data[[x, y]]);
            if !valid_mask || !value.is_finite() {
                masked_or_non_finite_count += 1;
                continue;
            }
            data_min = Some(match data_min {
                Some(current) => current.min(value),
                None => value,
            });
            data_max = Some(match data_max {
                Some(current) => current.max(value),
                None => value,
            });
            finite_values.push(value);
        }
    }
    let no_finite_values = finite_values.is_empty();
    PlaneStats {
        finite_values,
        data_min,
        data_max,
        masked_or_non_finite_count,
        no_finite_values,
    }
}

pub(crate) fn duration_ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn render_grid_cell(text: &str, width: usize, cursor: bool) -> String {
    if cursor && width >= 3 {
        let inner_width = width - 2;
        let inner = fit_cell_text(text, inner_width);
        format!("[{:>width$}]", inner, width = inner_width)
    } else {
        format!("{:>width$}", fit_cell_text(text, width), width = width)
    }
}

fn fit_cell_text(text: &str, width: usize) -> String {
    let chars = text.chars().count();
    if chars <= width {
        return text.to_string();
    }
    if width == 0 {
        return String::new();
    }
    if width == 1 {
        return "~".into();
    }
    let mut fitted = text.chars().take(width - 1).collect::<String>();
    fitted.push('~');
    fitted
}

pub(crate) fn plane_cell_text(
    plane: &Array2<f64>,
    mask: Option<&Array2<bool>>,
    x: usize,
    y: usize,
) -> String {
    if mask.is_some_and(|mask_data| !mask_data[[x, y]]) {
        return "masked".into();
    }
    format_plane_value(plane[[x, y]])
}

pub(crate) fn format_plane_value(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value == f64::INFINITY {
        return "Inf".into();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".into();
    }
    let abs = value.abs();
    let candidates = if abs >= 1.0e4 || (abs > 0.0 && abs < 1.0e-3) {
        [
            format!("{value:.5e}"),
            format!("{value:.4e}"),
            format!("{value:.3e}"),
            format!("{value:.2e}"),
        ]
    } else {
        [
            trim_float_text(format!("{value:.7}")),
            trim_float_text(format!("{value:.5}")),
            trim_float_text(format!("{value:.4}")),
            trim_float_text(format!("{value:.3}")),
        ]
    };
    candidates
        .into_iter()
        .min_by_key(|candidate| candidate.len())
        .unwrap_or_else(|| value.to_string())
}

pub(crate) fn trim_float_text(mut text: String) -> String {
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" { "0".into() } else { text }
}

pub(crate) fn sample_plane_axes<T: Clone>(
    plane: &Array2<T>,
    x_step: usize,
    y_step: usize,
) -> Array2<T> {
    let width = sampled_axis_len(0, plane.shape()[0].saturating_sub(1), x_step);
    let height = sampled_axis_len(0, plane.shape()[1].saturating_sub(1), y_step);
    Array2::from_shape_fn((width, height), |(x, y)| {
        plane[[x * x_step, y * y_step]].clone()
    })
}

fn sampled_axis_len(blc: usize, trc: usize, inc: usize) -> usize {
    (trc - blc) / inc + 1
}
