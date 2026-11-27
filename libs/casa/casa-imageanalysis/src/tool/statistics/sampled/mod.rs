// SPDX-License-Identifier: LGPL-3.0-or-later

//! Robust and per-plane sampled statistics computation (`ia.statistics`).

pub mod storage;

use casa_core::{ArrayValue, RecordField, Value};
use casa_images::{ImageInterface, ImagePixel};
use casa_lattices::{
    RobustStats, SamplePoint, StatisticsAlgorithm, StatsElement, compute_robust_stats,
};
use ndarray::{ArrayD, Dimension, IxDyn, ShapeBuilder};

use crate::analysis::imsubimage::ImageRegionSection;

use super::super::error::ImageToolResult;
use super::flux::FluxScale;
use super::{BASIC, QUANTILES, StatsOptions, add_extrema};
use storage::Rows;

// Rows use the same field order as BASIC followed by QUANTILES.
const COLS: usize = BASIC.len() + QUANTILES.len();

fn floats_with_shape(shape: Vec<usize>, values: Vec<f64>) -> Value {
    let array = ArrayD::from_shape_vec(IxDyn(&shape).f(), values)
        .expect("statistics vector length matches output shape");
    Value::Array(ArrayValue::Float64(array))
}

pub(super) fn collect<T: ImagePixel + StatsElement>(
    selection: &impl ImageInterface<T>,
    chunk: &ImageRegionSection,
    options: &StatsOptions<'_>,
    mask: Option<&ArrayD<bool>>,
    flux_scale: Option<&FluxScale>,
    fields: &mut Vec<RecordField>,
) -> ImageToolResult<Vec<usize>> {
    let rank = chunk.count.len();
    let display_axes = if options.axes.is_empty() {
        Vec::new()
    } else {
        (0..rank)
            .filter(|axis| !options.axes.contains(axis))
            .collect::<Vec<_>>()
    };
    let mut plane_shape = chunk.count.clone();
    let mut output_shape = Vec::new();
    for &axis in &display_axes {
        plane_shape[axis] = 1;
        if chunk.count[axis] > 1 {
            output_shape.push(chunk.count[axis]);
        }
    }
    if output_shape.is_empty() {
        output_shape.push(1);
    }
    let plane_count = output_shape.iter().product::<usize>();
    let mut rows = Rows::new(plane_count, options.disk)?;
    let mut has_samples = false;
    let mut extrema = [None, None];
    let mut start = vec![0; rank];
    let stride = vec![1; rank];
    let mut position = start.clone();
    let mut iterations = Vec::new();
    let chauvenet = matches!(options.algorithm, StatisticsAlgorithm::Chauvenet { .. });
    let biweight = matches!(options.algorithm, StatisticsAlgorithm::Biweight { .. });
    for mut plane in 0..plane_count {
        for &axis in &display_axes {
            start[axis] = plane % chunk.count[axis];
            plane /= chunk.count[axis];
        }
        let pixels = selection.get_slice(&start, &plane_shape, &stride)?;
        let mut sample = Vec::new();
        for (index, pixel) in pixels.reversed_axes().indexed_iter() {
            let value = pixel.to_f64_stats();
            if options
                .include
                .is_some_and(|[low, high]| !(low <= value && value <= high))
                || options
                    .exclude
                    .is_some_and(|[low, high]| value >= low && value <= high)
            {
                continue;
            }
            for (axis, &index) in index.slice().iter().rev().enumerate() {
                position[axis] = start[axis] + index;
            }
            if mask.is_some_and(|mask| !mask[IxDyn(&position)]) {
                continue;
            }
            let position = position
                .iter()
                .zip(&chunk.count)
                .rev()
                .fold(0_u64, |offset, (&pixel, &extent)| {
                    offset * extent as u64 + pixel as u64
                });
            sample.push(SamplePoint { value, position });
        }
        if sample.is_empty() && !biweight {
            rows.push([0.0; COLS])?;
            if chauvenet {
                iterations.push(0);
            }
            continue;
        }
        let result = compute_robust_stats(&sample, &options.algorithm)?;
        if let RobustStats::Sample(stats) = result
            && let Some(count) = stats.chauvenet_iterations
        {
            iterations.push(count);
        }
        let mut row = match result {
            RobustStats::Sample(stats) if stats.npts > 0.0 => {
                if display_axes.is_empty() {
                    extrema = [stats.minpos, stats.maxpos].map(|offset| {
                        offset.map(|mut offset| {
                            chunk
                                .count
                                .iter()
                                .map(|&extent| {
                                    let index = offset % extent as u64;
                                    offset /= extent as u64;
                                    index as usize
                                })
                                .collect()
                        })
                    });
                }
                [
                    stats.npts,
                    stats.sum,
                    stats.sumsq,
                    stats.min,
                    stats.max,
                    stats.mean,
                    stats.rms,
                    stats.sigma,
                    stats.median,
                    stats.medabsdevmed,
                    stats.q1,
                    stats.q3,
                    stats.quartile,
                ]
            }
            RobustStats::Sample(_) => [0.0; COLS],
            RobustStats::Biweight { location, scale } => {
                let mut row = [0.0; COLS];
                row[0] = sample.len() as f64;
                row[3] = sample
                    .iter()
                    .map(|point| point.value)
                    .fold(f64::INFINITY, f64::min);
                row[4] = sample
                    .iter()
                    .map(|point| point.value)
                    .fold(f64::NEG_INFINITY, f64::max);
                row[5] = location;
                row[7] = scale;
                row
            }
        };
        if options.single_precision {
            row[3] = f64::from(row[3] as f32);
            row[4] = f64::from(row[4] as f32);
        }
        has_samples |= row[0] > 0.0;
        rows.push(row)?;
    }
    rows.flush()?;
    let empty = !has_samples;
    for (column, &(name, _)) in BASIC.iter().chain(&QUANTILES).enumerate() {
        if biweight && !matches!(name, "npts" | "min" | "max" | "mean" | "sigma")
            || !options.robust && column >= BASIC.len()
        {
            continue;
        }
        let values = if empty {
            Value::Array(ArrayValue::from_f64_vec(Vec::new()))
        } else {
            floats_with_shape(output_shape.clone(), rows.column(column)?)
        };
        fields.push(RecordField::new(name, values));
    }
    if empty {
        fields.push(RecordField::new(
            "flux",
            Value::Array(ArrayValue::from_f64_vec(Vec::new())),
        ));
    } else if !biweight {
        if let Some(scale) = flux_scale {
            let mut values = rows.column(1)?;
            for (index, value) in values.iter_mut().enumerate() {
                *value *= scale.for_plane(index);
            }
            fields.push(RecordField::new(
                "flux",
                floats_with_shape(output_shape, values),
            ));
        }
        add_extrema(fields, selection.coordinates(), chunk, extrema)?;
    }
    Ok(iterations)
}
