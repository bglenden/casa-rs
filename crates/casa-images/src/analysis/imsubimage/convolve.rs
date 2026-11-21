// SPDX-License-Identifier: LGPL-3.0-or-later

mod fft;

use std::path::Path;

use casa_lattices::{TraversalCursorIter, TraversalSpec};
use casa_types::{ArrayD, RecordValue};
use ndarray::{Dimension, IxDyn, Slice, Zip};

use crate::image_expr::bounded_read_cursor;
use crate::{ImageError, ImageInterface, ImagePixel, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::lel_mask::PreparedMask;
use super::read_window::PixelSections;
use super::{SubimageOptions, SubimagePlan, SubimageRegion};

/// The real pixel precisions supported by general image convolution.
pub trait ConvolutionPixel: ImagePixel + Copy + Into<f64> {
    /// Round a sample to the image's storage precision.
    fn from_f64(value: f64) -> Self;
}

impl ConvolutionPixel for f32 {
    fn from_f64(value: f64) -> Self {
        value as Self
    }
}

impl ConvolutionPixel for f64 {
    fn from_f64(value: f64) -> Self {
        value
    }
}

/// A real-valued kernel array or a native image whose raw pixels are used.
pub enum ConvolutionKernel<'a, T: ConvolutionPixel> {
    /// A caller-supplied n-dimensional pixel array.
    Array(&'a ArrayD<T>),
    /// A named kernel image. Its pixel mask and coordinate grid are ignored.
    Image(&'a dyn ImageInterface<T>),
}

impl<T: ConvolutionPixel> ConvolutionKernel<'_, T> {
    fn shape(&self) -> &[usize] {
        match self {
            Self::Array(array) => array.shape(),
            Self::Image(image) => image.shape(),
        }
    }

    fn read(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        match self {
            Self::Image(image) => Ok(image.get_slice(start, shape, &vec![1; shape.len()])?),
            Self::Array(array) => Ok(array
                .slice_each_axis(|axis| slice(start[axis.axis.index()], shape[axis.axis.index()]))
                .to_owned()),
        }
    }

    fn sum(&self) -> Result<f64, ImageError> {
        let shape = self.shape();
        let mut sum = 0.0;
        for tile in tiles(shape, bounded_read_cursor(shape.to_vec())?) {
            let tile = tile?;
            for &value in &self.read(&tile.position, &tile.shape)? {
                sum += value.into();
            }
        }
        Ok(sum)
    }
}

/// A same-shape, zero-extended linear convolution of a selected image.
pub struct ImageConvolution {
    /// Shape, coordinates and stored metadata of the convolved selection.
    pub section: SubimagePlan,
}

impl ImageConvolution {
    /// Resolve the section in the original image and discard the restoring beam.
    ///
    /// # Errors
    /// The selected region or its coordinates cannot be interpreted.
    pub fn new<T: ConvolutionPixel>(
        image: &dyn ImageInterface<T>,
        record: RecordValue,
        region: Option<SubimageRegion<'_>>,
    ) -> Result<Self, ImageError> {
        let mut section = SubimagePlan::new(
            image,
            Some(record),
            SubimageOptions {
                region,
                dropdeg: false,
                keepaxes: &[],
            },
        )?;
        section.image_info.beam_set = Default::default();
        Ok(Self { section })
    }

    /// Convolve the selected pixels using an optional expression mask.
    ///
    /// # Errors
    /// The kernel or mask cannot be applied to the selected image, or input or
    /// output storage fails.
    pub fn copy_to<T: ConvolutionPixel>(
        &self,
        image: &dyn ImageInterface<T>,
        kernel: ConvolutionKernel<'_, T>,
        options: ConvolutionOptions<'_>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<(), ImageError> {
        let rank = kernel.shape().len();
        if rank == 0 || kernel.shape().contains(&0) {
            return Err(ImageError::CasaSelection(
                "Kernel array cannot be empty".into(),
            ));
        }
        let shape = &self.section.shape;
        if rank > shape.len() {
            return Err(ImageError::CasaSelection(
                "Kernel lattice has more axes than the image!".into(),
            ));
        }
        let divisor = (options.scale <= 0.0).then(|| kernel.sum()).transpose()?;
        let scaling = divisor.map_or(Scaling::Multiply(options.scale), Scaling::Divide);
        let mut center = kernel
            .shape()
            .iter()
            .map(|&width| width / 2)
            .collect::<Vec<_>>();
        center.resize(shape.len(), 0);
        let reach = &shape[..rank];
        let first = center[..rank]
            .iter()
            .zip(reach)
            .map(|(&mid, &len)| mid.saturating_sub(len - 1))
            .collect::<Vec<_>>();
        let support = kernel
            .shape()
            .iter()
            .zip(&center)
            .zip(&first)
            .zip(reach)
            .map(|(((&width, &mid), &low), &len)| width.min(mid.saturating_add(len)) - low)
            .collect::<Vec<_>>();
        let kernel_cursor = fft::limit_kernel_cursor(bounded_read_cursor(support.clone())?);
        // A zero divisor poisons an active FFT plane. With a fully
        // degenerate kernel the selected center coefficient is scalar.
        let invalid_normalization = divisor == Some(0.0) && support.iter().any(|&width| width > 1);

        let selection = &self.section.selection;
        let unit = vec![1; shape.len()];
        let input = StridedSection {
            start: &selection.start,
            shape,
            stride: selection.stride.as_deref().unwrap_or(&unit),
        };
        let reader = PixelSections {
            image,
            selection,
            mask: PreparedMask::new_for_output(
                options.mask,
                image.shape(),
                image.coordinates(),
                options.stretch,
                options.destination,
            )?,
        };
        self.section.copy_metadata(image, output)?;
        let cursor = bounded_read_cursor(input.dense_shape(shape))?
            .iter()
            .zip(input.stride)
            .map(|(&extent, &stride)| extent.div_ceil(stride).max(1))
            .collect::<Vec<_>>();
        let mut have_mask = false;
        for tile in tiles(shape, cursor) {
            let tile = tile?;
            let mut result = ArrayD::<f64>::zeros(IxDyn(&tile.shape));
            let (_, validity) = reader.read(
                &input.source_start(&tile.position),
                &tile.shape,
                input.stride,
            )?;
            if let Some(mask) = validity.filter(|mask| mask.iter().any(|&valid| !valid)) {
                if !have_mask {
                    output.create_filled_mask("mask0", true)?;
                    output.set_default_mask("mask0")?;
                    have_mask = true;
                }
                output.put_mask_slice("mask0", &mask, &tile.position)?;
            }

            if invalid_normalization {
                result.fill(f64::NAN);
                output.put_slice(&result.mapv(T::from_f64), &tile.position)?;
                continue;
            }

            for patch in tiles(&support, kernel_cursor.clone()) {
                let patch = patch?;
                let start = patch
                    .position
                    .iter()
                    .zip(&first)
                    .map(|(&i, &lo)| i + lo)
                    .collect::<Vec<_>>();
                let weights = kernel
                    .read(&start, &patch.shape)?
                    .mapv(|weight| scaling.apply::<T>(weight.into()));
                let delta = center
                    .iter()
                    .enumerate()
                    .map(|(axis, &mid)| {
                        start.get(axis).copied().unwrap_or_default() as isize - mid as isize
                    })
                    .collect::<Vec<_>>();
                let output_region = Window {
                    start: &tile.position,
                    shape: &tile.shape,
                };
                let Some((origin, size)) = output_region.halo(shape, &delta, &patch.shape) else {
                    continue;
                };
                let halo_section = StridedSection {
                    start: &input.source_start(&origin),
                    shape: &size,
                    stride: input.stride,
                };
                let halo_cursor = fft::limit_source_cursor(
                    bounded_read_cursor(halo_section.dense_shape(&size))?,
                    input.stride,
                    weights.shape(),
                );
                for piece in halo_section.cursors(halo_cursor) {
                    let piece = piece?;
                    let (data, mask) = reader.read(
                        &halo_section.source_start(&piece.position),
                        &piece.shape,
                        input.stride,
                    )?;
                    let mut data = data.mapv(|value| value.into());
                    if let Some(mask) = mask {
                        Zip::from(&mut data).and(&mask).for_each(|value, &valid| {
                            if !valid {
                                *value = 0.;
                            }
                        });
                    }
                    let data_start = piece
                        .position
                        .iter()
                        .zip(&origin)
                        .map(|(&i, &lo)| i + lo)
                        .collect::<Vec<_>>();
                    let data_region = Window {
                        start: &data_start,
                        shape: &piece.shape,
                    };
                    if let Some(convolved) = fft::convolve_block(&data, &weights)? {
                        let block = Window {
                            start: &data_start,
                            shape: convolved.shape(),
                        };
                        accumulate(
                            &mut result,
                            output_region,
                            &convolved,
                            block,
                            delta.iter().copied(),
                            1.,
                        );
                    } else {
                        for (index, &weight) in weights.indexed_iter() {
                            let offsets = delta.iter().enumerate().map(|(axis, &lo)| {
                                lo + index.slice().get(axis).copied().unwrap_or_default() as isize
                            });
                            accumulate(
                                &mut result,
                                output_region,
                                &data,
                                data_region,
                                offsets,
                                weight,
                            );
                        }
                    }
                }
            }
            output.put_slice(&result.mapv(T::from_f64), &tile.position)?;
        }
        Ok(())
    }
}

/// Scaling, optional mask and final native destination for a convolution.
pub struct ConvolutionOptions<'a> {
    /// A positive multiplier, or a nonpositive value for unit-sum scaling.
    pub scale: f64,
    /// The optional expression defining valid image pixels.
    pub mask: Option<&'a str>,
    /// Permit degenerate mask axes to extend across the image.
    pub stretch: bool,
    /// Final output path, for rejecting aliases of expression operands.
    pub destination: Option<&'a Path>,
}

enum Scaling {
    Multiply(f64),
    Divide(f64),
}

impl Scaling {
    fn apply<T: ConvolutionPixel>(&self, value: f64) -> f64 {
        let result = match *self {
            Self::Multiply(factor) => value * T::from_f64(factor).into(),
            Self::Divide(sum) => value / T::from_f64(sum).into(),
        };
        T::from_f64(result).into()
    }
}

fn accumulate(
    result: &mut ArrayD<f64>,
    destination: Window<'_>,
    input: &ArrayD<f64>,
    source: Window<'_>,
    offsets: impl Iterator<Item = isize>,
    weight: f64,
) {
    if let Some(overlap) = destination.overlap(source, offsets) {
        let source = input.slice_each_axis(|axis| overlap[axis.axis.index()].0);
        let target = result.slice_each_axis_mut(|axis| overlap[axis.axis.index()].1);
        Zip::from(target)
            .and(source)
            .for_each(|accum, &value| *accum += weight * value);
    }
}

fn tiles(shape: &[usize], cursor: Vec<usize>) -> TraversalCursorIter {
    TraversalCursorIter::new(shape.to_vec(), cursor, TraversalSpec::tiles())
}

fn slice(start: usize, length: usize) -> Slice {
    Slice::from(start..start + length)
}

#[derive(Clone, Copy)]
struct Window<'a> {
    start: &'a [usize],
    shape: &'a [usize],
}

impl Window<'_> {
    fn halo(
        self,
        image: &[usize],
        low: &[isize],
        kernel_shape: &[usize],
    ) -> Option<(Vec<usize>, Vec<usize>)> {
        let mut start = Vec::with_capacity(image.len());
        let mut shape = Vec::with_capacity(image.len());
        for (axis, &length) in image.iter().enumerate() {
            let width = kernel_shape.get(axis).copied().unwrap_or(1);
            let high = low[axis] + (width - 1) as isize;
            let first = (self.start[axis] as isize - high).max(0) as usize;
            let end =
                (self.start[axis] as isize + self.shape[axis] as isize - low[axis]).max(0) as usize;
            let end = end.min(length);
            if first >= end {
                return None;
            }
            start.push(first);
            shape.push(end - first);
        }
        Some((start, shape))
    }

    fn overlap(
        self,
        input: Self,
        shifts: impl Iterator<Item = isize>,
    ) -> Option<Vec<(Slice, Slice)>> {
        shifts
            .enumerate()
            .map(|(axis, shift)| {
                let aligned = input.start[axis] as isize + shift;
                let first = (self.start[axis] as isize).max(aligned);
                let last = ((self.start[axis] + self.shape[axis]) as isize)
                    .min(aligned + input.shape[axis] as isize);
                if first >= last {
                    return None;
                }
                let length = (last - first) as usize;
                Some((
                    slice((first - aligned) as usize, length),
                    slice(first as usize - self.start[axis], length),
                ))
            })
            .collect()
    }
}
