// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{LN_2, PI};

use casa_coordinates::CoordinateType;
use casa_core::{ArrayD, ArrayValue, Value, quanta::Quantity};
use casa_lattices::{TraversalCursorIter, TraversalSpec};
use ndarray::{IxDyn, ShapeBuilder, Zip};

use crate::analysis::axes::{StridedSection, direction_plane_axes};
use crate::analysis::imsubimage::fitprofile::fit_gaussian_profile;
use crate::analysis::imsubimage::read_window::PixelSections;
use crate::analysis::imsubimage::{ConvolutionOptions, SubimageOptions, SubimagePlan};
use crate::analysis::lel_mask::PreparedMask;
use crate::analysis::{
    GaussianConvolutionOptions, HistogramOptions, ImageGaussianConvolution, image_histogram,
};
use casa_images::bounded_read_cursor;
use casa_images::{ImageError, ImageInterface, TempImage};

use super::tool::{ImageMomentOptions, invalid, temp_image};

pub(super) struct AxisKernel {
    axis: usize,
    weights: Vec<(isize, f32)>,
}

pub(super) fn kernels(
    shape: &[usize],
    (axes, types, widths): (&[i64], &[String], &[f64]),
) -> Result<Vec<AxisKernel>, ImageError> {
    if axes.is_empty() || axes == [-1] {
        return Ok(Vec::new());
    }
    let mut selected = Vec::new();
    for &axis in axes {
        let axis = usize::try_from(axis)
            .ok()
            .filter(|&axis| axis < shape.len())
            .ok_or_else(|| invalid("Illegal smoothing axis"))?;
        if selected.contains(&axis) {
            return Err(invalid("A smoothing axis may be supplied only once"));
        }
        selected.push(axis);
    }
    if types.first().is_none_or(|name| name.is_empty()) {
        return Ok(Vec::new());
    }
    if types.len() != selected.len() {
        return Err(invalid(
            "Smoothing axes, kernel types and widths must conform",
        ));
    }
    selected
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(index, (axis, name))| {
            let width = widths
                .get(index)
                .copied()
                .or_else(|| name.starts_with(['H', 'h']).then_some(0.))
                .ok_or_else(|| invalid("A boxcar or Gaussian smoothing kernel requires a width"))?;
            Ok(AxisKernel {
                axis,
                weights: weights(shape[axis], name, width)?,
            })
        })
        .collect()
}

fn weights(length: usize, name: &str, width: f64) -> Result<Vec<(isize, f32)>, ImageError> {
    if length <= 1 {
        return Err(invalid(
            "Smoothing requires more than one sample along the axis",
        ));
    }
    let width = f64::from(width as f32);
    let radius = length as isize - 1;
    let name = name.to_ascii_uppercase();
    let mut result = Vec::new();
    if name.starts_with('H') {
        let half = length.div_ceil(2) as isize;
        let center = (length / 2) as isize;
        let middle = ((length - 1) / 2) as isize;
        let period = (length + 1) as f64;
        for x in 0..2 * half - 1 {
            let harmonic = (half - (x - middle).abs()) as f64;
            let weight = (1. - (2. * PI * harmonic / period).cos()) / period;
            result.push((x - center, weight as f32));
        }
    } else {
        if !width.is_finite() || width <= 0. || width > (isize::MAX / 16) as f64 {
            return Err(invalid(
                "Smoothing kernel width is outside the supported range",
            ));
        }
        if name.starts_with('B') {
            let pixels = (width + 0.5) as isize;
            if pixels == 0 {
                return Err(invalid("Boxcar kernel must be at least one pixel wide"));
            }
            let n = (length as isize).max(pixels + 1);
            let first = (n - pixels) / 2 - n / 2;
            let start = first.max(-radius);
            let finish = (first + pixels - 1).min(radius);
            result.extend((start..=finish).map(|offset| (offset, 1. / pixels as f32)));
        } else if name.starts_with('G') {
            let sigma = width / (8. * LN_2).sqrt();
            let n = (length as isize).max(2 * ((5. * sigma + 0.5) as isize + 1));
            let center = n / 2;
            let fraction = n as f64 / 2. - center as f64;
            let normalization = 1. / (sigma * (2. * PI).sqrt());
            let support = radius.min((12. * sigma + 1.).ceil() as isize);
            let start = (-center).max(-support);
            let finish = (n - center - 1).min(support);
            for offset in start..=finish {
                let z = (offset as f64 - fraction) / sigma;
                result.push((offset, (normalization * (-0.5 * z * z).exp()) as f32));
            }
        } else {
            return Err(invalid(format!("Unknown smoothing kernel {name}")));
        }
    }
    Ok(result)
}

pub(super) struct Sources {
    pub(super) replacement: Option<TempImage<f32>>,
    pub(super) plan: SubimagePlan,
    pub(super) smoothed: Option<TempImage<f32>>,
}

pub(super) fn prepare(
    image: &dyn ImageInterface<f32>,
    mut plan: SubimagePlan,
    axis: usize,
    kernels: &[AxisKernel],
    options: &ImageMomentOptions<'_>,
) -> Result<Sources, ImageError> {
    let mut replacement = None;
    let mut mask = options.mask;
    if image.find_axis(CoordinateType::Spectral) == Some(axis)
        && plan.image_info.beam_set.is_multi()
    {
        let selected = materialize(image, &plan, options, mask, false)?;
        let common = plan.image_info.beam_set.common_beam()?;
        let widths = [common.major, common.minor, common.position_angle]
            .map(|value| Quantity::new(value, "rad").expect("built-in angular unit"));
        let convolution = ImageGaussianConvolution::new(
            &selected,
            selected.coordinates().to_record(),
            &plan.image_info.to_record(),
            None,
            GaussianConvolutionOptions {
                widths,
                axes: direction_plane_axes(&selected)?
                    .map(|axis| axis as i64)
                    .to_vec(),
                scale: -1.,
                target_resolution: true,
            },
        )?;
        let geometry = convolution.section();
        let mut convolved = temp_image(geometry.shape.clone(), geometry.coordinates.clone(), 1)?;
        convolution.copy_to(
            &selected,
            ConvolutionOptions {
                scale: -1.,
                mask: None,
                stretch: false,
                destination: None,
            },
            &mut convolved,
        )?;
        plan = SubimagePlan::new(
            &convolved,
            None,
            SubimageOptions {
                region: None,
                dropdeg: false,
                keepaxes: &[],
            },
        )?;
        replacement = Some(convolved);
        mask = None;
    }
    let smoothed = if kernels.is_empty() {
        None
    } else {
        let image = replacement.as_ref().map_or(image, |image| image);
        let mut smoothed = materialize(image, &plan, options, mask, true)?;
        for kernel in kernels {
            convolve(&mut smoothed, kernel)?;
        }
        Some(smoothed)
    };
    Ok(Sources {
        replacement,
        plan,
        smoothed,
    })
}

fn materialize(
    image: &dyn ImageInterface<f32>,
    plan: &SubimagePlan,
    options: &ImageMomentOptions<'_>,
    mask: Option<&str>,
    zero_masked: bool,
) -> Result<TempImage<f32>, ImageError> {
    let mut result = temp_image(plan.shape.clone(), plan.coordinates.clone(), 1)?;
    if !zero_masked {
        result.set_units(image.units())?;
        result.set_image_info(&plan.image_info)?;
    }
    result.set_misc_info(image.misc_info())?;
    let selection = &plan.selection;
    let ones = vec![1; image.ndim()];
    let section = StridedSection {
        start: &selection.start,
        stride: selection.stride.as_deref().unwrap_or(&ones),
        shape: &plan.shape,
    };
    let windows = PixelSections {
        image,
        selection,
        mask: PreparedMask::new_for_outputs(
            mask,
            image.shape(),
            image.coordinates(),
            options.stretch,
            options.destinations.iter().copied(),
        )?,
    };
    let mut has_mask = false;
    for tile in section.cursors(bounded_read_cursor(plan.shape.clone())?) {
        let tile = tile?;
        let (mut values, valid) = windows.read(
            &section.source_start(&tile.position),
            &tile.shape,
            section.stride,
        )?;
        if let Some(valid) = valid {
            if !has_mask {
                result.make_mask("mask0", true, true)?;
                has_mask = true;
            }
            if zero_masked {
                Zip::from(&mut values)
                    .and(&valid)
                    .for_each(|value, &valid| {
                        if !valid {
                            *value = 0.;
                        }
                    });
            }
            result.put_mask_slice("mask0", &valid, &tile.position)?;
        }
        result.put_slice(&values, &tile.position)?;
    }
    Ok(result)
}

fn convolve(image: &mut TempImage<f32>, kernel: &AxisKernel) -> Result<(), ImageError> {
    let shape = image.shape().to_vec();
    let mut row = vec![1; shape.len()];
    let length = shape[kernel.axis];
    row[kernel.axis] = length;
    let stride = vec![1; shape.len()];
    let mut grid = shape;
    grid[kernel.axis] = 1;
    let mut result = Vec::with_capacity(length);
    for tile in TraversalCursorIter::new(grid, stride.clone(), TraversalSpec::tiles()) {
        let position = tile?.position;
        let source = image.get_slice(&position, &row)?;
        let source = source
            .as_slice_memory_order()
            .expect("one-dimensional image row");
        result.clear();
        for index in 0..length {
            let mut sum = 0_f64;
            for &(offset, weight) in &kernel.weights {
                if let Some(at) = index.checked_add_signed(-offset)
                    && let Some(&value) = source.get(at)
                {
                    sum += f64::from(value) * f64::from(weight);
                }
            }
            result.push(sum as f32);
        }
        let block = ArrayD::from_shape_vec(IxDyn(&row).f(), std::mem::take(&mut result))
            .expect("smoothing row shape");
        image.put_slice(&block, &position)?;
        result = block.into_raw_vec_and_offset().0;
    }
    Ok(())
}

pub(super) fn noise(
    image: &dyn ImageInterface<f32>,
    smoothed: Option<&TempImage<f32>>,
    region: Option<crate::analysis::imsubimage::SubimageRegion<'_>>,
    mask: Option<&str>,
    stretch: bool,
) -> Result<f32, ImageError> {
    let input = smoothed.map_or(image, |image| image);
    let bins = image_histogram(
        input,
        HistogramOptions {
            axes: &[-1],
            bins: 100,
            include: &[],
            cumulative: false,
            logarithmic: false,
            stretch,
            region: if smoothed.is_some() { None } else { region },
            mask: if smoothed.is_some() { None } else { mask },
        },
    )?;
    let column = |name| match bins.get(name) {
        Some(Value::Array(ArrayValue::Float64(array))) => array.as_slice().expect("one histogram"),
        _ => unreachable!("histogram record field"),
    };
    let values = column("values");
    let counts = column("counts");
    let peak = (0..counts.len())
        .reduce(|a, b| if counts[a] >= counts[b] { a } else { b })
        .expect("100 bins");
    let quarter = counts[peak] / 4.;
    let lower = (1..peak).rev().find(|&i| counts[i] < quarter);
    let upper = (peak + 1..counts.len()).find(|&i| counts[i] < quarter);
    let (start, end) = lower
        .zip(upper)
        .filter(|&(lo, hi)| hi - lo >= 3)
        .unwrap_or((0, counts.len() - 1));
    let seed_width = (end - start + 1) as f64 * (values[1] - values[0]) / 2.;
    let parameters = fit_gaussian_profile(
        (start..=end).map(|index| (values[index], counts[index] / counts[peak])),
        [1., values[peak], seed_width],
        None,
    );
    Ok(parameters.map_or(0., |fit| (fit[2].abs() / std::f64::consts::SQRT_2) as f32))
}
