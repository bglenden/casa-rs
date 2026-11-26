// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{ArrayD, Complex64};
use casa_fft::{Fft2, FftError};
use ndarray::{Axis, IxDyn, Slice, Zip};

use crate::ImageError;

const MAX_FFT_SAMPLES: usize = 1 << 19;
const MIN_DIRECT_PRODUCTS: usize = 1 << 16;
const MIN_FFT_KERNEL: usize = 32;

pub(super) fn limit_kernel_cursor(cursor: Vec<usize>) -> Vec<usize> {
    limit_cursor(cursor, |shape| {
        shape
            .iter()
            .try_fold(1usize, |total, &length| {
                total
                    .checked_mul(length)
                    .filter(|&n| n <= MAX_FFT_SAMPLES / 16)
            })
            .is_some()
    })
}

pub(super) fn limit_source_cursor(
    cursor: Vec<usize>,
    stride: &[usize],
    kernel: &[usize],
) -> Vec<usize> {
    if kernel.iter().product::<usize>() < MIN_FFT_KERNEL {
        return cursor;
    }
    limit_cursor(cursor, |shape| {
        let source: Vec<_> = shape
            .iter()
            .zip(stride)
            .map(|(&length, &step)| length.div_ceil(step))
            .collect();
        fft_shapes(&source, kernel).is_some()
    })
}

fn limit_cursor(mut cursor: Vec<usize>, fits: impl Fn(&[usize]) -> bool) -> Vec<usize> {
    while !fits(&cursor) {
        let Some(width) = cursor.iter_mut().max() else {
            break;
        };
        if *width == 1 {
            break;
        }
        *width = width.div_ceil(2);
    }
    cursor
}

/// Compute a full zero-padded linear convolution of two bounded blocks.
/// A singleton kernel axis is a batch axis and is not transformed.
pub(super) fn convolve_block(
    source: &ArrayD<f64>,
    kernel: &ArrayD<f64>,
) -> Result<Option<ArrayD<f64>>, ImageError> {
    if kernel.len() < MIN_FFT_KERNEL
        || source.len().saturating_mul(kernel.len()) < MIN_DIRECT_PRODUCTS
    {
        return Ok(None);
    }
    let Some(shapes) = fft_shapes(source.shape(), kernel.shape()) else {
        return Ok(None);
    };
    let BlockShapes {
        convolution,
        grid,
        kernel: kernel_grid,
        axes,
    } = shapes;
    if axes.iter().all(|&axis| source.shape()[axis] == 1) {
        return Ok(None);
    }

    let mut transformed_source = ArrayD::zeros(IxDyn(&grid));
    let mut transformed_kernel = ArrayD::zeros(IxDyn(&kernel_grid));
    embed(&mut transformed_source, source.view());
    let mut weights = kernel.view();
    for axis in kernel.ndim()..source.ndim() {
        weights = weights.insert_axis(Axis(axis));
    }
    embed(&mut transformed_kernel, weights);
    let mut plan = BlockTransform::new(&axes, &grid)?;
    plan.apply(&mut transformed_source, false)?;
    plan.apply(&mut transformed_kernel, false)?;
    let transfer = transformed_kernel
        .broadcast(IxDyn(&grid))
        .expect("kernel grid fits");
    Zip::from(&mut transformed_source)
        .and(transfer)
        .for_each(|value, &weight| *value *= weight);
    plan.apply(&mut transformed_source, true)?;
    let elements = axes.iter().map(|&i| grid[i]).product::<usize>() as f64;
    let answer = transformed_source
        .slice_each_axis(|axis| Slice::from(..convolution[axis.axis.index()]))
        .mapv(|value| value.re / elements);
    Ok(Some(answer))
}

struct BlockShapes {
    convolution: Vec<usize>,
    grid: Vec<usize>,
    kernel: Vec<usize>,
    axes: Vec<usize>,
}

fn fft_shapes(source: &[usize], kernel: &[usize]) -> Option<BlockShapes> {
    let mut convolution = source.to_vec();
    let mut grid = convolution.clone();
    let mut kernel_grid = vec![1; source.len()];
    let mut axes = Vec::new();
    for (axis, &width) in kernel.iter().enumerate() {
        if width > 1 {
            convolution[axis] = convolution[axis].checked_add(width - 1)?;
            let length = convolution[axis].checked_next_power_of_two()?;
            grid[axis] = length;
            kernel_grid[axis] = length;
            axes.push(axis);
        }
    }
    grid.iter().try_fold(1usize, |total, &length| {
        total.checked_mul(length).filter(|&n| n <= MAX_FFT_SAMPLES)
    })?;
    Some(BlockShapes {
        convolution,
        grid,
        kernel: kernel_grid,
        axes,
    })
}

fn embed(target: &mut ArrayD<Complex64>, input: ndarray::ArrayViewD<'_, f64>) {
    let destination =
        target.slice_each_axis_mut(|axis| Slice::from(..input.shape()[axis.axis.index()]));
    Zip::from(destination)
        .and(input)
        .for_each(|value, &pixel| *value = Complex64::new(pixel, 0.0));
}

fn fft_error(error: FftError) -> ImageError {
    ImageError::InvalidMetadata(format!("convolve FFT: {error}"))
}

struct BlockTransform {
    transforms: Vec<(Axis, Fft2<f64>)>,
    line: Vec<Complex64>,
}

impl BlockTransform {
    fn new(axes: &[usize], shape: &[usize]) -> Result<Self, ImageError> {
        let transforms = axes
            .iter()
            .map(|&axis| {
                let transform = Fft2::with_threads([1, shape[axis]], 1)
                    .map_err(fft_error)?
                    .with_estimated_plan();
                Ok((Axis(axis), transform))
            })
            .collect::<Result<Vec<_>, ImageError>>()?;
        let longest = axes
            .iter()
            .map(|&axis| shape[axis])
            .max()
            .expect("active axes");
        Ok(Self {
            transforms,
            line: vec![Complex64::default(); longest],
        })
    }

    fn apply(&mut self, grid: &mut ArrayD<Complex64>, inverse: bool) -> Result<(), ImageError> {
        for (axis, transform) in &mut self.transforms {
            for mut lane in grid.lanes_mut(*axis) {
                if let Some(values) = lane.as_slice_mut() {
                    transform.transform(values, inverse).map_err(fft_error)?;
                } else {
                    let line = &mut self.line[..lane.len()];
                    for (buffered, &value) in line.iter_mut().zip(&lane) {
                        *buffered = value;
                    }
                    transform.transform(line, inverse).map_err(fft_error)?;
                    for (value, &buffered) in lane.iter_mut().zip(line.iter()) {
                        *value = buffered;
                    }
                }
            }
        }
        Ok(())
    }
}
