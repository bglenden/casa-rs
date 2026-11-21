// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::{ImageInterface, ImagePixel};
use casa_lattices::{
    RobustStats, SamplePoint, StatisticsAlgorithm, StatsElement, compute_robust_stats,
};
use ndarray::{ArrayD, Dimension, IxDyn};
use pyo3::PyResult;

use crate::convert::value::CasaValue;

use super::flux::FluxScale;
use super::{BASIC, Chunk, QUANTILES, StatsOptions, add_extrema, raised};

mod storage;

use storage::Rows;

// Rows use the same field order as BASIC followed by QUANTILES.
const COLS: usize = BASIC.len() + QUANTILES.len();

pub(super) fn collect<T: ImagePixel + StatsElement>(
    selection: &impl ImageInterface<T>,
    chunk: &Chunk,
    options: &StatsOptions<'_>,
    mask: Option<&ArrayD<bool>>,
    flux_scale: Option<&FluxScale>,
    record: &mut Vec<(&'static str, CasaValue)>,
) -> PyResult<Vec<usize>> {
    let rank = chunk.size.len();
    let display_axes = if options.axes.is_empty() {
        Vec::new()
    } else {
        (0..rank)
            .filter(|axis| !options.axes.contains(axis))
            .collect::<Vec<_>>()
    };
    let mut plane_shape = chunk.size.clone();
    let mut output_shape = Vec::new();
    for &axis in &display_axes {
        plane_shape[axis] = 1;
        if chunk.size[axis] > 1 {
            output_shape.push(chunk.size[axis]);
        }
    }
    if output_shape.is_empty() {
        output_shape.push(1);
    }
    let plane_count = output_shape.iter().product::<usize>();
    let mut rows = Rows::new(plane_count, options.disk).map_err(raised)?;
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
            start[axis] = plane % chunk.size[axis];
            plane /= chunk.size[axis];
        }
        let pixels = selection
            .get_slice(&start, &plane_shape, &stride)
            .map_err(raised)?;
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
                .zip(&chunk.size)
                .rev()
                .fold(0_u64, |offset, (&pixel, &extent)| {
                    offset * extent as u64 + pixel as u64
                });
            sample.push(SamplePoint { value, position });
        }
        if sample.is_empty() && !biweight {
            rows.push([0.0; COLS]).map_err(raised)?;
            if chauvenet {
                iterations.push(0);
            }
            continue;
        }
        let result = compute_robust_stats(&sample, &options.algorithm).map_err(raised)?;
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
                                .size
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
        rows.push(row).map_err(raised)?;
    }
    rows.flush().map_err(raised)?;
    let empty = !has_samples;
    for (column, &(name, _)) in BASIC.iter().chain(&QUANTILES).enumerate() {
        if biweight && !matches!(name, "npts" | "min" | "max" | "mean" | "sigma")
            || !options.robust && column >= BASIC.len()
        {
            continue;
        }
        let values = if empty {
            CasaValue::floats(Vec::new())
        } else {
            CasaValue::Floats {
                shape: output_shape.clone(),
                values: rows.column(column).map_err(raised)?,
            }
        };
        record.push((name, values));
    }
    if empty {
        record.push(("flux", CasaValue::floats(Vec::new())));
    } else if !biweight {
        if let Some(scale) = flux_scale {
            let mut values = rows.column(1).map_err(raised)?;
            for (index, value) in values.iter_mut().enumerate() {
                *value *= scale.for_plane(index);
            }
            record.push((
                "flux",
                CasaValue::Floats {
                    shape: output_shape,
                    values,
                },
            ));
        }
        add_extrema(record, selection.coordinates(), chunk, extrema)?;
    }
    Ok(iterations)
}
