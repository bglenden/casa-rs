// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::{Add, Div, Mul, Sub};
use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_types::{ArrayD, RecordValue};
use ndarray::{Axis, IxDyn, Zip};

use crate::{ImageError, ImageInfo, ImageInterface, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::lel_mask::PreparedMask;
use super::decimate::{DecimationMethod, DecimationPixel};
use super::read_window::PixelSections;
use super::region::SubimageSelection;
use super::{SubimagePlan, SubimageRegion};

const MAX_TILE_SAMPLES: usize = 256 * 1024;

/// One-dimensional image smoothing kernels.
#[derive(Clone, Copy)]
pub enum SmoothingKernel {
    /// Average consecutive groups of the requested number of pixels.
    Boxcar(usize),
    /// Apply quarter, half, quarter weights on adjacent samples.
    Hanning,
}

/// Settings for a CASA boxcar or Hanning image product.
pub struct SmoothingOptions<'a> {
    /// Pixel axis, or a negative value to select the spectral axis.
    pub axis: i64,
    /// Kernel used along the chosen axis.
    pub kernel: SmoothingKernel,
    /// Optional decimation of the smoothed data.
    pub decimate: Option<DecimationMethod>,
    /// Region selected before smoothing.
    pub region: Option<SubimageRegion<'a>>,
    /// LEL expression combined with pixel validity.
    pub mask: Option<&'a str>,
    /// Stretch compatible singleton mask axes.
    pub stretch: bool,
    /// Final destination, if persistent, to protect named mask inputs.
    pub destination: Option<&'a Path>,
}

/// Scalar arithmetic required by the smoothing kernels.
pub trait SmoothingPixel:
    DecimationPixel
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<f32, Output = Self>
    + Div<f32, Output = Self>
{
}
impl<T> SmoothingPixel for T where
    T: DecimationPixel
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<f32, Output = T>
        + Div<f32, Output = T>
{
}

#[derive(Clone, Copy)]
struct Samples {
    first: usize,
    step: usize,
    count: usize,
}

impl Samples {
    fn describe(
        kernel: SmoothingKernel,
        length: usize,
        decimate: Option<DecimationMethod>,
    ) -> Result<Self, ImageError> {
        let width = match kernel {
            SmoothingKernel::Boxcar(0) => {
                return Err(selection_error("Boxcar width must be positive"));
            }
            SmoothingKernel::Boxcar(width) => width,
            SmoothingKernel::Hanning => 3,
        };
        if length < width {
            return Err(selection_error(format!(
                "The selected region of the image must have at least {width} pixels along the axis to be smoothed."
            )));
        }
        if let SmoothingKernel::Boxcar(width) = kernel
            && matches!(decimate, Some(DecimationMethod::Mean))
            && length / 2 < width
        {
            return Err(selection_error(
                "The convolution axis of selected image region must have length of at least 2*width when using the mean decimation function",
            ));
        }
        let (first, step, count) = match (kernel, decimate) {
            (SmoothingKernel::Boxcar(width), Some(DecimationMethod::Copy)) => {
                let count = length / width;
                (0, if count == 1 { 1 } else { width }, count)
            }
            (SmoothingKernel::Boxcar(width), Some(DecimationMethod::Mean)) => {
                (0, width, (length - width + 1) / width)
            }
            (SmoothingKernel::Boxcar(width), None) => (0, 1, length - width + 1),
            (SmoothingKernel::Hanning, Some(_)) => (1, 2, (length - 1) / 2),
            (SmoothingKernel::Hanning, None) => (0, 1, length),
        };
        Ok(Self { first, step, count })
    }

    fn centers(self) -> impl Iterator<Item = usize> {
        (0..self.count).map(move |index| self.first + self.step * index)
    }
}

/// A resolved input region and a smoothed output grid.
pub struct ImageSmoothing<'a> {
    /// Output geometry and metadata.
    pub section: SubimagePlan,
    input: SubimageSelection,
    axis: usize,
    samples: Samples,
    options: SmoothingOptions<'a>,
}

fn selection_error(text: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(text.into())
}

impl<'a> ImageSmoothing<'a> {
    /// Resolve the selected geometry without reading input pixels.
    ///
    /// # Errors
    /// The axis, region, smoothing width, or restoring-beam layout is invalid.
    pub fn new<T: SmoothingPixel>(
        image: &dyn ImageInterface<T>,
        coordinates: RecordValue,
        options: SmoothingOptions<'a>,
        beam_record: &RecordValue,
    ) -> Result<Self, ImageError> {
        let axis = if options.axis < 0 {
            image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
                selection_error("Axis not specified and image has no spectral coordinate")
            })?
        } else {
            options.axis as usize
        };
        if axis >= image.ndim() {
            return Err(selection_error(format!(
                "The axis number along which the smoothing is to occur must be less than the number of axes in the image which is {}",
                image.ndim(),
            )));
        }
        if ImageInfo::from_record(beam_record)?.beam_set.is_multi() {
            return Err(selection_error(
                "This application does not support images with multiple beams. Please convolve your image with a single beam and run this application using that image.",
            ));
        }
        let input = SubimageSelection::resolve_section(image, None, options.region, None, None)?;
        if input.has_multiple_regions {
            return Err(selection_error(
                "Only a single n-dimensional rectangular region is supported",
            ));
        }
        let mut shape = input.packed_shape();
        let samples = Samples::describe(options.kernel, shape[axis], options.decimate)?;
        shape[axis] = samples.count;
        let mut metadata = SubimageSelection::resolve_section(image, None, None, None, None)?;
        metadata.start.clone_from(&input.start);
        let mut step = input
            .stride
            .clone()
            .unwrap_or_else(|| vec![1; image.ndim()]);
        metadata.start[axis] += samples.first * step[axis];
        step[axis] = step[axis]
            .checked_mul(samples.step)
            .ok_or_else(|| selection_error("Smoothing stride exceeds the supported pixel range"))?;
        metadata.shape = shape
            .iter()
            .zip(&step)
            .map(|(&length, &step)| (length - 1) * step + 1)
            .collect();
        metadata.stride = step.iter().any(|&step| step != 1).then_some(step);
        let section = SubimagePlan::from_selection(
            image,
            Some(coordinates),
            metadata,
            false,
            &[],
            Some(beam_record),
        )?;
        Ok(Self {
            section,
            input,
            axis,
            samples,
            options,
        })
    }

    /// Pixel axis used by the kernel after resolving the spectral default.
    pub fn axis(&self) -> usize {
        self.axis
    }

    /// Stream tiles containing complete selected lines into an allocated output.
    ///
    /// # Errors
    /// Input I/O or mask evaluation failed, or a mean bin has no valid pixels.
    pub fn copy_to<T: SmoothingPixel>(
        &self,
        source: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<(), ImageError> {
        let reader = PixelSections {
            image: source,
            selection: &self.input,
            mask: PreparedMask::new_for_output(
                self.options.mask,
                source.shape(),
                source.coordinates(),
                self.options.stretch,
                self.options.destination,
            )?,
        };
        self.section.copy_metadata(source, output)?;
        let ones = vec![1; source.ndim()];
        let shape = self.input.packed_shape();
        let axis = self.axis;
        let input = StridedSection {
            start: &self.input.start,
            shape: &shape,
            stride: self.input.stride.as_deref().unwrap_or(&ones),
        };
        let mut lines = shape.clone();
        lines[axis] = 1;
        let positions = StridedSection {
            start: &vec![0; source.ndim()],
            shape: &lines,
            stride: &ones,
        };
        let mut line_budget = (MAX_TILE_SAMPLES / shape[axis]).max(1);
        let cursor = source
            .nice_cursor_shape()
            .into_iter()
            .enumerate()
            .map(|(index, pixels)| {
                let width = if axis == index {
                    1
                } else {
                    pixels
                        .div_ceil(input.stride[index])
                        .max(1)
                        .min(line_budget)
                        .min(shape[index])
                };
                line_budget /= width;
                width
            })
            .collect();
        let mut bins_have_data = vec![false; self.samples.count];
        let mut active_mask = None;
        for tile in positions.cursors(cursor) {
            let tile = tile?;
            let start = input.source_start(&tile.position);
            let mut input_shape = tile.shape;
            input_shape[axis] = shape[axis];
            let (pixels, valid) = reader.read(&start, &input_shape, input.stride)?;
            let valid = valid.unwrap_or_else(|| ArrayD::from_elem(IxDyn(&input_shape), true));
            let (values, validity) = self.smooth_tile(&pixels, &valid, &mut bins_have_data);
            output.put_slice(&values, &tile.position)?;
            if validity.iter().any(|&valid| !valid) {
                if active_mask.is_none() {
                    active_mask = Some(output.create_filled_pixel_mask(true)?);
                }
                output.put_mask_slice(
                    active_mask.as_deref().expect("active mask"),
                    &validity,
                    &tile.position,
                )?;
            }
        }
        if matches!(self.options.decimate, Some(DecimationMethod::Mean))
            && self.samples.step > 1
            && bins_have_data.contains(&false)
        {
            return Err(selection_error("All selected pixels are masked"));
        }
        Ok(())
    }

    fn smooth_tile<T: SmoothingPixel>(
        &self,
        input: &ArrayD<T>,
        input_valid: &ArrayD<bool>,
        bins_have_data: &mut [bool],
    ) -> (ArrayD<T>, ArrayD<bool>) {
        let axis = Axis(self.axis);
        let mut shape = input.shape().to_vec();
        shape[self.axis] = self.samples.count;
        let mut values = ArrayD::default(IxDyn(&shape));
        let mut valid = ArrayD::from_elem(IxDyn(&shape), true);
        Zip::from(values.lanes_mut(axis))
            .and(valid.lanes_mut(axis))
            .and(input.lanes(axis))
            .and(input_valid.lanes(axis))
            .for_each(|mut value_out, mut mask_out, value_in, mask_in| {
                let line: Vec<_> = value_in
                    .iter()
                    .zip(mask_in.iter())
                    .map(|(&pixel, &valid)| if valid { pixel } else { T::default() })
                    .collect();
                let mask: Vec<_> = mask_in.iter().copied().collect();
                let (smooth, valid) = smooth_line(
                    &line,
                    &mask,
                    self.options.kernel,
                    self.options.decimate,
                    self.samples,
                );
                for (slot, pixel) in value_out.iter_mut().zip(smooth) {
                    *slot = pixel;
                }
                for ((slot, found), present) in
                    mask_out.iter_mut().zip(&mut *bins_have_data).zip(valid)
                {
                    *slot = present;
                    *found |= present;
                }
            });
        (values, valid)
    }
}

fn smooth_line<T: SmoothingPixel>(
    pixels: &[T],
    valid: &[bool],
    kernel: SmoothingKernel,
    decimate: Option<DecimationMethod>,
    samples: Samples,
) -> (Vec<T>, Vec<bool>) {
    if let (SmoothingKernel::Boxcar(width), Some(DecimationMethod::Copy)) = (kernel, decimate) {
        let values = pixels
            .chunks_exact(width)
            .map(|row| row.iter().fold(T::default(), |sum, &pixel| sum + pixel) / width as f32)
            .collect();
        return (
            values,
            samples.centers().map(|index| valid[index]).collect(),
        );
    }
    let smooth = match kernel {
        SmoothingKernel::Boxcar(width) => {
            let mut windows = Vec::with_capacity(pixels.len() - width + 1);
            let mut total = pixels[..width]
                .iter()
                .fold(T::default(), |sum, &pixel| sum + pixel);
            windows.push(total / width as f32);
            for (&next, &leaving) in pixels[width..].iter().zip(pixels) {
                total = total + (next - leaving);
                windows.push(total / width as f32);
            }
            windows
        }
        SmoothingKernel::Hanning => {
            let mut windows = Vec::with_capacity(pixels.len());
            windows.push((pixels[0] + pixels[1]) * 0.5);
            windows.extend(
                pixels
                    .windows(3)
                    .map(|row| (row[0] + row[2]) * 0.25 + row[1] * 0.5),
            );
            let end = pixels.len() - 1;
            windows.push((pixels[end - 1] + pixels[end]) * 0.5);
            windows
        }
    };
    if matches!(decimate, Some(DecimationMethod::Mean)) {
        return samples
            .centers()
            .map(|start| {
                let mut total = T::Sum::default();
                let mut count = 0;
                let end = start + samples.step;
                for (&pixel, &valid) in smooth[start..end].iter().zip(&valid[start..end]) {
                    if valid {
                        total += pixel.widen();
                        count += 1;
                    }
                }
                if count == 0 {
                    (T::default(), false)
                } else {
                    (T::narrow(total / count as f64), true)
                }
            })
            .unzip();
    }
    samples
        .centers()
        .map(|index| (smooth[index], valid[index]))
        .unzip()
}
