// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::{AddAssign, Div};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_types::{ArrayD, RecordValue};
use ndarray::{Axis, IxDyn, Slice, Zip};

use crate::{ImageError, ImageInfo, ImageInterface, ImagePixel, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::lel_mask::evaluate_strided_mask;
use super::region::SubimageSelection;
use super::{SubimageOptions, SubimagePlan, SubimageRegion, and_opt_mask};

/// How to combine the selected input planes along the decimation axis.
#[derive(Clone, Copy)]
pub enum DecimationMethod {
    /// Keep the first input plane of each group, including a partial final group.
    Copy,
    /// Average the unmasked pixels of each complete group.
    Mean,
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
        let mut plan = if region.is_none()
            && image.ndim() > 2
            && image.find_axis(CoordinateType::Direction).is_none()
        {
            // CASA's implicit region selects a single pixel on two spatial
            // axes when a multidimensional image has no direction coordinate.
            let spectral = image.find_axis(CoordinateType::Spectral);
            let stokes = image.find_axis(CoordinateType::Stokes);
            let mut selection = SubimageSelection::resolve_section(image, None, None, None, None)?;
            for axis in (0..image.ndim())
                .filter(|&axis| Some(axis) != spectral && Some(axis) != stokes)
                .take(2)
            {
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
        if factor > plan.shape[axis] {
            return Err(ImageError::CasaSelection(match method {
                DecimationMethod::Copy => {
                    "LatticeStepper::setCursorShape: cursorShape <=0 or > latticeShape".into()
                }
                DecimationMethod::Mean => "TiledShape: shape has to be > 0".into(),
            }));
        }
        if factor > 1 {
            let mut record = plan
                .coordinate_record
                .take()
                .unwrap_or_else(|| plan.coordinates.to_record());
            let (coordinate, local) = (0..plan.coordinates.n_coordinates())
                .scan(0, |offset, index| {
                    let model = plan.coordinates.coordinate(index);
                    let start = *offset;
                    *offset += model.n_pixel_axes();
                    Some((model, start..*offset))
                })
                .find_map(|(model, range)| {
                    range.contains(&axis).then(|| (model, axis - range.start))
                })
                .ok_or_else(|| {
                    ImageError::CasaSelection("Decimation axis has no coordinate".into())
                })?;
            CoordinateSystem::set_reference_pixel_in_record(
                &mut record,
                axis,
                coordinate.reference_pixel()[local] / factor as f64,
            )?;
            let (coordinates, _) = CoordinateSystem::set_increment_in_record(
                &mut record,
                axis,
                coordinate.increment()[local] * factor as f64,
            )?;
            plan.shape[axis] = match method {
                DecimationMethod::Copy => plan.shape[axis].div_ceil(factor),
                DecimationMethod::Mean => plan.shape[axis] / factor,
            };
            plan.coordinates = coordinates;
            plan.coordinate_record = Some(record);
        }
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
    pub fn copy_to<T>(
        &self,
        image: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<Option<ArrayD<bool>>, ImageError>
    where
        T: ImagePixel + AddAssign + Div<f32, Output = T>,
    {
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
        let mut membership =
            evaluate_strided_mask(mask, image.shape(), image.coordinates(), stretch, &input)?;
        if selected.stride.is_none() {
            selected.apply_selection_masks(&mut membership);
        }
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
                DecimationMethod::Copy => {
                    let pixels = image.get_slice(&origin, &cursor.shape, &out_step)?;
                    let mut validity = image.get_mask_slice(&origin, &cursor.shape, &out_step)?;
                    if let Some(mask) = &membership {
                        let region = mask.slice_each_axis(|slice| {
                            let index = slice.axis.index();
                            let step = if index == axis { factor } else { 1 };
                            sample_slice(packed_start[index], cursor.shape[index], step)
                        });
                        and_opt_mask(&mut validity, region.to_owned());
                    }
                    (pixels, validity)
                }
                DecimationMethod::Mean => {
                    let bin = cursor.position[axis];
                    let (values, validity) = mean_tile(
                        image,
                        &input,
                        packed_start,
                        &cursor.shape,
                        axis,
                        factor,
                        block_planes,
                        membership.as_ref(),
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

fn sample_slice(start: usize, count: usize, step: usize) -> Slice {
    Slice::new(
        start as isize,
        Some((start + (count - 1) * step + 1) as isize),
        step as isize,
    )
}

#[allow(clippy::too_many_arguments)]
fn mean_tile<T>(
    image: &dyn ImageInterface<T>,
    input: &StridedSection<'_>,
    mut packed_start: Vec<usize>,
    shape: &[usize],
    axis: usize,
    factor: usize,
    block_planes: usize,
    membership: Option<&ArrayD<bool>>,
) -> Result<(ArrayD<T>, ArrayD<bool>), ImageError>
where
    T: ImagePixel + AddAssign + Div<f32, Output = T>,
{
    let mut sums = ArrayD::<T>::default(IxDyn(shape));
    let mut counts = ArrayD::<usize>::zeros(IxDyn(shape));
    let mut block = shape.to_vec();
    for offset in (0..factor).step_by(block_planes) {
        block[axis] = block_planes.min(factor - offset);
        let origin = input.source_start(&packed_start);
        let samples = image.get_slice(&origin, &block, input.stride)?;
        let mut validity = image.get_mask_slice(&origin, &block, input.stride)?;
        if let Some(membership) = membership {
            let region = membership.slice_each_axis(|slice| {
                let i = slice.axis.index();
                sample_slice(packed_start[i], block[i], 1)
            });
            and_opt_mask(&mut validity, region.to_owned());
        }
        for (index, plane) in samples.axis_iter(Axis(axis)).enumerate() {
            let output = Zip::from(sums.index_axis_mut(Axis(axis), 0))
                .and(counts.index_axis_mut(Axis(axis), 0))
                .and(plane);
            if let Some(mask) = &validity {
                output.and(mask.index_axis(Axis(axis), index)).for_each(
                    |sum, count, &value, &valid| {
                        if valid {
                            *sum += value;
                            *count += 1;
                        }
                    },
                );
            } else {
                output.for_each(|sum, count, &value| {
                    *sum += value;
                    *count += 1;
                });
            }
        }
        packed_start[axis] += block[axis];
    }
    Zip::from(&mut sums).and(&counts).for_each(|value, &count| {
        if count != 0 {
            *value = *value / count as f32;
        }
    });
    Ok((sums, counts.mapv(|count| count != 0)))
}
