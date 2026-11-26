// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::{AddAssign, Div};

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem, CoordinateType};
use casa_core::{ArrayD, ArrayValue, RecordValue, Value};
use ndarray::{Axis, IxDyn, Zip};
use num_complex::{Complex32, Complex64};

use crate::{ImageError, ImageInfo, ImageInterface, ImagePixel, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::lel_mask::PreparedMask;
use super::read_window::{PixelSections, sample_slice};
use super::region::{SubimageSelection, implicit_positional_axes};
use super::{SubimageOptions, SubimagePlan, SubimageRegion};

/// How to combine the selected input planes along the decimation axis.
#[derive(Clone, Copy)]
pub enum DecimationMethod {
    /// Keep the first input plane of each group, including a partial final group.
    Copy,
    /// Average the unmasked pixels of each complete group.
    Mean,
}

/// Single-precision image pixels that accumulate means in double precision.
pub trait DecimationPixel: ImagePixel {
    /// Double-precision sum of the input pixel values.
    type Sum: Copy + Default + AddAssign + Div<f64, Output = Self::Sum>;

    /// Widen a pixel before summing.
    fn widen(self) -> Self::Sum;

    /// Convert a completed mean back to image precision.
    fn narrow(value: Self::Sum) -> Self;
}

impl DecimationPixel for f32 {
    type Sum = f64;

    fn widen(self) -> Self::Sum {
        f64::from(self)
    }

    fn narrow(value: Self::Sum) -> Self {
        value as f32
    }
}

impl DecimationPixel for Complex32 {
    type Sum = Complex64;

    fn widen(self) -> Self::Sum {
        Complex64::new(f64::from(self.re), f64::from(self.im))
    }

    fn narrow(value: Self::Sum) -> Self {
        Self::new(value.re as f32, value.im as f32)
    }
}

/// Axis and input selection for image decimation.
pub struct ImageDecimationOptions<'a> {
    /// Zero-based image pixel axis to decimate.
    pub axis: usize,
    /// Positive decimation factor.
    pub factor: usize,
    /// Whether to sample planes or average complete groups.
    pub method: DecimationMethod,
    /// Input region before decimation.
    pub region: Option<SubimageRegion<'a>>,
    /// Optional lattice expression combined with the image validity mask.
    pub mask: Option<&'a str>,
    /// Stretch singleton mask dimensions if compatible with the image.
    pub stretch: bool,
}

/// A resolved input section and the output coordinates of an image decimation.
pub struct ImageDecimation<'a> {
    plan: SubimagePlan,
    options: ImageDecimationOptions<'a>,
}

impl<'a> ImageDecimation<'a> {
    /// Resolve decimation geometry without acquiring pixel data.
    ///
    /// # Errors
    /// Returns an error for invalid axes, factors, regions or input metadata.
    pub fn new<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        native_coordinates: RecordValue,
        options: ImageDecimationOptions<'a>,
        beam_record: &RecordValue,
    ) -> Result<Self, ImageError> {
        let ImageDecimationOptions {
            axis,
            factor,
            method,
            region,
            ..
        } = options;
        if axis >= image.ndim() {
            return Err(ImageError::CasaSelection(format!(
                "The axis number along which the decimation is to occur must be less than the number of axes in the image which is {}",
                image.ndim()
            )));
        }
        if factor == 0 || factor > image.shape()[axis] {
            return Err(ImageError::CasaSelection(if factor == 0 {
                "The decimation factor must be positive".into()
            } else {
                "The value of factor cannot be greater than the number of pixels along the specified axis".into()
            }));
        }
        if ImageInfo::from_record(beam_record)?.beam_set.is_multi() {
            return Err(ImageError::CasaSelection(
                "This application does not support images with multiple beams. Please convolve your image with a single beam and run this application using that image.".into(),
            ));
        }
        let tabular_source = image
            .coordinates()
            .find_coordinate(CoordinateType::Tabular)
            .map(|_| native_coordinates.clone());
        let implicit_axes = region
            .is_none()
            .then(|| implicit_positional_axes(image))
            .flatten();
        let mut plan = if let Some(axes) = implicit_axes {
            let mut selection = SubimageSelection::resolve_section(image, None, None, None, None)?;
            for axis in axes {
                selection.shape[axis] = 1;
            }
            SubimagePlan::from_selection(
                image,
                Some(native_coordinates),
                selection,
                false,
                &[],
                Some(beam_record),
            )?
        } else {
            SubimagePlan::new_with_beams(
                image,
                Some(native_coordinates),
                SubimageOptions {
                    region,
                    dropdeg: false,
                    keepaxes: &[],
                },
                Some(beam_record),
            )?
        };
        apply_decimation(&mut plan, tabular_source.as_ref(), axis, factor, method)?;
        Ok(Self { plan, options })
    }

    /// Geometry and metadata to use when allocating the output image.
    pub fn section(&self) -> &SubimagePlan {
        &self.plan
    }

    /// Copy sampled values or write masked means using bounded pixel tiles.
    /// Returns invalid output samples as an unnamed mask when decimating.
    ///
    /// # Errors
    /// Invalid product geometry, a fully masked averaging group, or I/O failure.
    pub fn copy_to<T: DecimationPixel>(
        &self,
        image: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        let ImageDecimationOptions {
            axis,
            factor,
            method,
            mask,
            stretch,
            ..
        } = self.options;
        if factor == 1 {
            self.plan.copy_to(image, output, mask, stretch)?;
            return Ok(None);
        }
        self.plan.copy_metadata(image, output)?;
        let selected = &self.plan.selection;
        let ones = vec![1; image.ndim()];
        let stride = selected.stride.as_deref().unwrap_or(&ones);
        let selected_shape = selected.packed_shape();
        let input = StridedSection {
            start: &selected.start,
            shape: &selected_shape,
            stride,
        };
        let reader = PixelSections {
            image,
            selection: selected,
            mask: PreparedMask::new(mask, image.shape(), image.coordinates(), stretch)?,
        };
        let mut out_step = stride.to_vec();
        out_step[axis] *= factor;
        let output_section = StridedSection {
            start: input.start,
            shape: &self.plan.shape,
            stride: &out_step,
        };
        let mut cursor_shape = image.nice_cursor_shape();
        let block_planes = cursor_shape[axis].div_ceil(stride[axis]).max(1);
        if matches!(method, DecimationMethod::Mean) {
            cursor_shape[axis] = 1;
        }
        let mut mask_out = None;
        let mut valid_bins = vec![false; self.plan.shape[axis]];
        for cursor in output_section.cursors(cursor_shape) {
            let cursor = cursor?;
            let origin = output_section.source_start(&cursor.position);
            let mut packed_start = cursor.position.clone();
            packed_start[axis] *= factor;
            let (values, validity) = match method {
                DecimationMethod::Copy => reader.read(&origin, &cursor.shape, &out_step)?,
                DecimationMethod::Mean => {
                    let bin = cursor.position[axis];
                    let (values, validity) = mean_tile(
                        &reader,
                        &input,
                        packed_start,
                        &cursor.shape,
                        axis,
                        factor,
                        block_planes,
                    )?;
                    valid_bins[bin] |= validity.iter().any(|&sample| sample);
                    (values, Some(validity))
                }
            };
            output.put_slice(&values, &cursor.position)?;
            if let Some(validity) = validity.filter(|mask| mask.iter().any(|&sample| !sample)) {
                let mask = mask_out
                    .get_or_insert_with(|| ArrayD::from_elem(IxDyn(&self.plan.shape), true));
                mask.slice_each_axis_mut(|slice| {
                    let i = slice.axis.index();
                    sample_slice(cursor.position[i], cursor.shape[i], 1)
                })
                .assign(&validity);
            }
        }
        if matches!(method, DecimationMethod::Mean) && valid_bins.contains(&false) {
            return Err(ImageError::CasaSelection(
                "All selected pixels are masked".into(),
            ));
        }
        Ok(mask_out)
    }
}

pub(super) fn apply_decimation(
    plan: &mut SubimagePlan,
    tabular_source: Option<&RecordValue>,
    axis: usize,
    factor: usize,
    method: DecimationMethod,
) -> Result<(), ImageError> {
    if factor > plan.shape[axis] {
        return Err(ImageError::CasaSelection(match method {
            DecimationMethod::Copy => {
                "LatticeStepper::setCursorShape: cursorShape <=0 or > latticeShape".into()
            }
            DecimationMethod::Mean => "TiledShape: shape has to be > 0".into(),
        }));
    }
    if factor > 1 || tabular_source.is_some() {
        let mut record = plan
            .coordinate_record
            .take()
            .unwrap_or_else(|| plan.coordinates.to_record());
        if factor > 1 && plan.coordinates.pixel_axis_type(axis) != Some(CoordinateType::Tabular) {
            let (index, local) = plan.coordinates.pixel_axis_owner(axis).ok_or_else(|| {
                ImageError::CasaSelection("Decimation axis has no coordinate".into())
            })?;
            let coordinate = plan.coordinates.coordinate(index);
            CoordinateSystem::set_reference_pixel_in_record(
                &mut record,
                axis,
                coordinate.reference_pixel()[local] / factor as f64,
            )?;
            let increments = match coordinate {
                CoordinateModel::Direction(direction) => {
                    direction.reference_and_increment_in_world_units().1
                }
                _ => coordinate.increment(),
            };
            CoordinateSystem::set_increment_in_record(
                &mut record,
                axis,
                increments[local] * factor as f64,
            )?;
        }
        if let Some(source) = tabular_source {
            adjust_tabular_record(source, &plan.selection, axis, factor, &mut record)?;
        }
        plan.shape[axis] = match method {
            DecimationMethod::Copy => plan.shape[axis].div_ceil(factor),
            DecimationMethod::Mean => plan.shape[axis] / factor,
        };
        plan.coordinates = CoordinateSystem::from_record(&record)?;
        plan.coordinate_record = Some(record);
    }
    Ok(())
}

fn adjust_tabular_record(
    source: &RecordValue,
    selection: &SubimageSelection,
    decimated_axis: usize,
    factor: usize,
    record: &mut RecordValue,
) -> Result<(), ImageError> {
    for placement in CoordinateSystem::record_coordinates(source)? {
        let CoordinateModel::Tabular(table) = placement.model(source)? else {
            continue;
        };
        let Some(axis) = placement.pixel_axes.first().copied().flatten() else {
            continue;
        };
        let start = selection.start[axis] as f64;
        let stride = selection.stride.as_ref().map_or(1, |stride| stride[axis]);
        let step = (if axis == decimated_axis {
            stride * factor
        } else {
            stride
        }) as f64;
        if start == 0.0 && step == 1.0 {
            continue;
        }
        let Some(Value::Record(native)) = record.get_mut(&placement.field) else {
            return Err(ImageError::CasaSelection(
                "Tabular coordinate is missing from its record".into(),
            ));
        };
        let reference_pixel = table.reference_pixel()[0];
        let reference_world = table.reference_value()[0];
        let shifted_pixel = (reference_pixel - start) / step;
        let increment = table.increment()[0] * step;
        // CASACore keeps the table's pixel samples and edits the affine
        // mapping around that lookup. Serialization evaluates the edited mapping.
        let world = table
            .world_values()
            .iter()
            .map(|&world| {
                reference_world
                    + (world - reference_world) * step
                    + (reference_pixel - shifted_pixel) * increment
            })
            .collect::<Vec<_>>();
        if !increment.is_finite() || world.iter().any(|value| !value.is_finite()) {
            return Err(ImageError::CasaSelection(
                "Decimation produces non-finite tabular coordinates".into(),
            ));
        }
        for (field, values) in [
            ("crpix", vec![shifted_pixel]),
            ("cdelt", vec![increment]),
            ("pixelvalues", table.pixel_values().to_vec()),
            ("worldvalues", world),
        ] {
            native.upsert(field, ArrayValue::from_f64_vec(values).into());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn mean_tile<T: DecimationPixel>(
    reader: &PixelSections<'_, T>,
    input: &StridedSection<'_>,
    mut packed_start: Vec<usize>,
    shape: &[usize],
    axis: usize,
    factor: usize,
    block_planes: usize,
) -> Result<(ArrayD<T>, ArrayD<bool>), ImageError> {
    let mut sums = ArrayD::<T::Sum>::default(IxDyn(shape));
    let mut counts = ArrayD::<usize>::zeros(IxDyn(shape));
    let mut block = shape.to_vec();
    for offset in (0..factor).step_by(block_planes) {
        block[axis] = block_planes.min(factor - offset);
        let origin = input.source_start(&packed_start);
        let (samples, validity) = reader.read(&origin, &block, input.stride)?;
        for (index, plane) in samples.axis_iter(Axis(axis)).enumerate() {
            let output = Zip::from(sums.index_axis_mut(Axis(axis), 0))
                .and(counts.index_axis_mut(Axis(axis), 0))
                .and(plane);
            if let Some(mask) = &validity {
                output.and(mask.index_axis(Axis(axis), index)).for_each(
                    |sum, count, &value, &valid| {
                        if valid {
                            *sum += value.widen();
                            *count += 1;
                        }
                    },
                );
            } else {
                output.for_each(|sum, count, &value| {
                    *sum += value.widen();
                    *count += 1;
                });
            }
        }
        packed_start[axis] += block[axis];
    }
    let values = Zip::from(&sums).and(&counts).map_collect(|&value, &count| {
        if count == 0 {
            T::default()
        } else {
            T::narrow(value / count as f64)
        }
    });
    Ok((values, counts.mapv(|count| count != 0)))
}
