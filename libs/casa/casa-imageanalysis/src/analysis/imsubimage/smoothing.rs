// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::{Add, Div, Mul, Range, Sub};
use std::path::Path;

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem, CoordinateType};
use casa_core::{ArrayD, ArrayValue, RecordValue, Value};
use ndarray::{Axis, IxDyn, Zip};

use casa_images::{ImageError, ImageInfo, ImageInterface, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::lel_mask::PreparedMask;
use super::decimate::{DecimationMethod, DecimationPixel, apply_decimation};
use super::read_window::PixelSections;
use super::region::{SubimageSelection, implicit_positional_axes};
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
        let length = shape[axis];
        let samples = Samples::describe(options.kernel, length, options.decimate)?;
        // Match the CASA sequence: smooth a cropped grid, then optionally
        // decimate its coordinates while retaining the cropped Stokes labels.
        shape[axis] = match (options.kernel, options.decimate) {
            (SmoothingKernel::Boxcar(width), Some(DecimationMethod::Copy)) => {
                (samples.count - 1) * width + 1
            }
            (SmoothingKernel::Boxcar(width), _) => length - width + 1,
            (SmoothingKernel::Hanning, Some(_)) => samples.count * 2,
            (SmoothingKernel::Hanning, None) => length,
        };
        if matches!(options.kernel, SmoothingKernel::Boxcar(_))
            && options.decimate.is_some()
            && shape[axis] > 1
            && let Some(axes) = implicit_positional_axes(image)
        {
            for index in axes {
                shape[index] = 1;
            }
        }
        let mut metadata = SubimageSelection::resolve_section(image, None, None, None, None)?;
        metadata.start.clone_from(&input.start);
        let stride = input
            .stride
            .clone()
            .unwrap_or_else(|| vec![1; image.ndim()]);
        metadata.start[axis] += samples.first * stride[axis];
        metadata.shape = shape
            .iter()
            .zip(&stride)
            .map(|(&length, &step)| (length - 1) * step + 1)
            .collect();
        metadata.stride.clone_from(&input.stride);
        let tabular = image
            .coordinates()
            .find_coordinate(CoordinateType::Tabular)
            .map(|_| coordinates.clone());
        let mut section = SubimagePlan::from_selection(
            image,
            Some(coordinates),
            metadata,
            false,
            &[],
            Some(beam_record),
        )?;
        apply_decimation(
            &mut section,
            tabular.as_ref(),
            axis,
            samples.step,
            options.decimate.unwrap_or(DecimationMethod::Copy),
        )?;
        if let SmoothingKernel::Boxcar(width) = options.kernel
            && width > 1
        {
            shift_boxcar_reference(&mut section, axis, width, samples.step)?;
        }
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

    /// Stream bounded tiles of selected samples into an allocated output.
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
        let mut lines = self.section.shape.clone();
        lines[axis] = 1;
        let positions = StridedSection {
            start: &vec![0; source.ndim()],
            shape: &lines,
            stride: &ones,
        };
        let dense_length = self.input.shape[axis];
        let mut line_budget = (MAX_TILE_SAMPLES / dense_length).max(1);
        let cursor = source
            .nice_cursor_shape()
            .into_iter()
            .enumerate()
            .map(|(index, pixels)| {
                let stride = input.stride[index];
                let width = if axis == index {
                    1
                } else {
                    pixels.min(line_budget).div_ceil(stride).min(lines[index])
                };
                line_budget = (line_budget / ((width - 1) * stride + 1)).max(1);
                width
            })
            .collect::<Vec<_>>();
        let mut active_mask = None;
        for tile in positions.cursors(cursor.clone()) {
            let tile = tile?;
            if dense_length > MAX_TILE_SAMPLES {
                let line = LineSections {
                    reader: &reader,
                    input: &input,
                    position: &tile.position,
                    axis,
                };
                self.copy_long_line(&line, output, &mut active_mask)?;
            } else {
                let start = input.source_start(&tile.position);
                let mut input_shape = tile.shape;
                input_shape[axis] = shape[axis];
                let (pixels, valid) = reader.read(&start, &input_shape, input.stride)?;
                let valid = valid.unwrap_or_else(|| ArrayD::from_elem(IxDyn(&input_shape), true));
                let (values, validity) = self.smooth_tile(&pixels, &valid);
                write_tile(output, &mut active_mask, &tile.position, &values, &validity)?;
            }
        }
        if active_mask.is_some()
            && matches!(self.options.decimate, Some(DecimationMethod::Mean))
            && self.samples.step > 1
        {
            self.check_mean_bins(output, &positions, &cursor)?;
        }
        Ok(())
    }

    fn check_mean_bins<T: SmoothingPixel>(
        &self,
        output: &dyn ImageInterface<T>,
        lines: &StridedSection<'_>,
        cursor: &[usize],
    ) -> Result<(), ImageError> {
        let axis = self.axis;
        let block = MAX_TILE_SAMPLES / cursor.iter().product::<usize>();
        for start in (0..self.samples.count).step_by(block) {
            let count = block.min(self.samples.count - start);
            let mut present = vec![false; count];
            for tile in lines.cursors(cursor.to_vec()) {
                let mut tile = tile?;
                tile.position[axis] = start;
                tile.shape[axis] = count;
                let Some(valid) =
                    output.get_mask_slice(&tile.position, &tile.shape, lines.stride)?
                else {
                    return Ok(());
                };
                for lane in valid.lanes(Axis(axis)) {
                    for (found, &valid) in present.iter_mut().zip(lane) {
                        *found |= valid;
                    }
                }
                if present.iter().all(|&found| found) {
                    break;
                }
            }
            if present.contains(&false) {
                return Err(selection_error("All selected pixels are masked"));
            }
        }
        Ok(())
    }

    fn copy_long_line<T: SmoothingPixel>(
        &self,
        line: &LineSections<'_, T>,
        output: &mut dyn MutableImageInterface<T>,
        active_mask: &mut Option<String>,
    ) -> Result<(), ImageError> {
        let axis = self.axis;
        let mut position = line.position.to_vec();
        let mut shape = vec![1; position.len()];
        let block = MAX_TILE_SAMPLES.min(self.samples.count);
        let mut values = Vec::with_capacity(block);
        let mut validity = Vec::with_capacity(block);
        let mut write = |pixel, valid| -> Result<(), ImageError> {
            values.push(pixel);
            validity.push(valid);
            let count = values.len();
            if count == block || position[axis] + count == self.samples.count {
                shape[axis] = count;
                let pixels = ArrayD::from_shape_vec(IxDyn(&shape), std::mem::take(&mut values))
                    .expect("one output line block");
                let valid = ArrayD::from_shape_vec(IxDyn(&shape), std::mem::take(&mut validity))
                    .expect("one output mask block");
                write_tile(output, active_mask, &position, &pixels, &valid)?;
                position[axis] += count;
            }
            Ok(())
        };
        let length = line.input.shape[axis];
        if let (SmoothingKernel::Boxcar(width), Some(DecimationMethod::Copy)) =
            (self.options.kernel, self.options.decimate)
        {
            let mut input = line.samples(0..self.samples.count * width);
            for _ in 0..self.samples.count {
                let (first, valid) = next_sample(&mut input)?;
                let total = input
                    .by_ref()
                    .take(width - 1)
                    .try_fold(first, |sum, sample| Ok::<_, ImageError>(sum + sample?.0))?;
                write(total / width as f32, valid)?;
            }
            return Ok(());
        }
        let Samples { first, step, count } = self.samples;
        let use_mean = matches!(self.options.decimate, Some(DecimationMethod::Mean));
        let end = first + (count - 1) * step + if use_mean { step } else { 1 };
        let mut mean_sum = T::Sum::default();
        let mut mean_count = 0;
        let mut emit = |index, pixel: T, valid| -> Result<(), ImageError> {
            if index < first {
                return Ok(());
            }
            let group_index = (index - first) % step;
            if use_mean {
                if valid {
                    mean_sum += pixel.widen();
                    mean_count += 1;
                }
                if group_index == step - 1 {
                    let present = mean_count != 0;
                    let value = if present {
                        T::narrow(mean_sum / mean_count as f64)
                    } else {
                        T::default()
                    };
                    write(value, present)?;
                    mean_sum = T::Sum::default();
                    mean_count = 0;
                }
            } else if group_index == 0 {
                write(pixel, valid)?;
            }
            Ok(())
        };
        match self.options.kernel {
            SmoothingKernel::Boxcar(width) => {
                let mut total = line
                    .samples(0..width)
                    .try_fold(T::default(), |sum, sample| {
                        Ok::<_, ImageError>(sum + sample?.0)
                    })?;
                let mut incoming = line.samples(width..width + end - 1);
                for (index, departing) in line.samples(0..end).enumerate() {
                    let (leaving, valid) = departing?;
                    emit(index, total / width as f32, valid)?;
                    if index + 1 < end {
                        total = total + (next_sample(&mut incoming)?.0 - leaving);
                    }
                }
            }
            SmoothingKernel::Hanning => {
                let mut input = line.samples(0..(end + 1).min(length));
                let (mut previous, valid) = next_sample(&mut input)?;
                let (mut current, mut current_valid) = next_sample(&mut input)?;
                emit(0, (previous + current) * 0.5, valid)?;
                for index in 1..end {
                    if index + 1 == length {
                        emit(index, (previous + current) * 0.5, current_valid)?;
                    } else {
                        let (next, next_valid) = next_sample(&mut input)?;
                        emit(
                            index,
                            hanning_interior(previous, current, next),
                            current_valid,
                        )?;
                        previous = current;
                        current = next;
                        current_valid = next_valid;
                    }
                }
            }
        }
        Ok(())
    }

    fn smooth_tile<T: SmoothingPixel>(
        &self,
        input: &ArrayD<T>,
        input_valid: &ArrayD<bool>,
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
                for (slot, present) in mask_out.iter_mut().zip(valid) {
                    *slot = present;
                }
            });
        (values, valid)
    }
}

fn shift_boxcar_reference(
    section: &mut SubimagePlan,
    axis: usize,
    width: usize,
    factor: usize,
) -> Result<(), ImageError> {
    let Some((index, local)) = section.coordinates.pixel_axis_owner(axis) else {
        return Ok(());
    };
    let coordinate = section.coordinates.coordinate(index);
    if matches!(
        coordinate,
        CoordinateModel::Stokes(_) | CoordinateModel::Quality(_)
    ) {
        return Ok(());
    }
    let (reference, increment) = match coordinate {
        CoordinateModel::Direction(direction) => direction.reference_and_increment_in_world_units(),
        _ => (coordinate.reference_value(), coordinate.increment()),
    };
    let center = (width - 1) as f64 / (2.0 * factor as f64);
    // The stored increment already includes the input stride and decimation.
    let mut record = section
        .coordinate_record
        .take()
        .unwrap_or_else(|| section.coordinates.to_record());
    let shift = increment[local] * center;
    section.coordinates = if let CoordinateModel::Tabular(table) = coordinate {
        let field = &section.coordinates.coordinates_in_record(&record)?[index].field;
        let Some(Value::Record(native)) = record.get_mut(field) else {
            return Err(selection_error(
                "Tabular coordinate is missing from its record",
            ));
        };
        // Shift the live reference and the evaluated samples by the same
        // offset. Keep the affine pixel reference from the selected grid.
        let live = table.clone().with_live_reference(native)?;
        let world = table
            .world_values()
            .iter()
            .map(|value| value + shift)
            .collect();
        native.upsert("worldvalues", ArrayValue::from_f64_vec(world).into());
        native.upsert(
            "crval",
            ArrayValue::from_f64_vec(vec![live.reference_value()[0] + shift]).into(),
        );
        CoordinateSystem::from_record(&record)?
    } else {
        CoordinateSystem::set_reference_value_in_record(
            &mut record,
            axis,
            reference[local] + shift,
        )?
        .0
    };
    section.coordinate_record = Some(record);
    Ok(())
}

fn write_tile<T: SmoothingPixel>(
    output: &mut dyn MutableImageInterface<T>,
    mask: &mut Option<String>,
    position: &[usize],
    values: &ArrayD<T>,
    valid: &ArrayD<bool>,
) -> Result<(), ImageError> {
    output.put_slice(values, position)?;
    if valid.iter().any(|&value| !value) {
        if mask.is_none() {
            *mask = Some(output.create_filled_pixel_mask(true)?);
        }
        output.put_mask_slice(mask.as_deref().expect("active mask"), valid, position)?;
    }
    Ok(())
}

struct LineSections<'a, T: SmoothingPixel> {
    reader: &'a PixelSections<'a, T>,
    input: &'a StridedSection<'a>,
    position: &'a [usize],
    axis: usize,
}

impl<T: SmoothingPixel> LineSections<'_, T> {
    fn samples(
        &self,
        range: Range<usize>,
    ) -> impl Iterator<Item = Result<(T, bool), ImageError>> + '_ {
        let axis = self.axis;
        let step = self.input.stride[axis];
        let block_size = MAX_TILE_SAMPLES.div_ceil(step);
        let mut position = self.input.source_start(self.position);
        let base = position[axis];
        let mut shape = vec![1; position.len()];
        let mut offset = range.start;
        let mut samples = Vec::new().into_iter();
        std::iter::from_fn(move || {
            if let Some(sample) = samples.next() {
                return Some(Ok(sample));
            }
            if offset == range.end {
                return None;
            }
            position[axis] = base + offset * step;
            shape[axis] = block_size.min(range.end - offset);
            offset += shape[axis];
            match self.reader.read(&position, &shape, self.input.stride) {
                Ok((pixels, valid)) => {
                    samples = match valid {
                        Some(mask) => pixels
                            .iter()
                            .zip(mask.iter())
                            .map(|(&value, &valid)| {
                                (if valid { value } else { T::default() }, valid)
                            })
                            .collect::<Vec<_>>(),
                        None => pixels.iter().map(|&value| (value, true)).collect(),
                    }
                    .into_iter();
                    samples.next().map(Ok)
                }
                Err(error) => Some(Err(error)),
            }
        })
    }
}

fn next_sample<T>(
    input: &mut impl Iterator<Item = Result<(T, bool), ImageError>>,
) -> Result<(T, bool), ImageError> {
    input.next().expect("sample within the input line")
}

fn hanning_interior<T: SmoothingPixel>(previous: T, current: T, next: T) -> T {
    (previous + next) * 0.25 + current * 0.5
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
                    .map(|row| hanning_interior(row[0], row[1], row[2])),
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
