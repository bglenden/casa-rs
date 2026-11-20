// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateType;
use casa_types::{ArrayD, RecordValue};
use ndarray::IxDyn;

use crate::image_expr::ImageExprValue;
use crate::image_expr_lel::{rebin_coordinate_record, rebin_reduce_slice};
use crate::{ImageError, ImageInterface, ImagePixel, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::lel_mask::PreparedMask;
use super::coords::{CoordReductionSpec, reduce_coordinates_and_beams};
use super::read_window::{PixelSections, sample_slice};
use super::region::SubimageSelection;
use super::{SubimageOptions, SubimagePlan, SubimageRegion, resolve_kept_axes};

const MAX_TILE_SAMPLES: usize = 256 * 1024;

/// Binning and selection options for a CASA image rebin.
pub struct ImageRebinOptions<'a> {
    /// Positive bin factors in source pixel-axis order.
    pub factors: &'a [usize],
    /// Region selected before binning.
    pub region: Option<SubimageRegion<'a>>,
    /// Optional LEL mask expression.
    pub mask: Option<&'a str>,
    /// Stretch compatible singleton mask dimensions.
    pub stretch: bool,
    /// Drop degenerate output axes.
    pub dropdeg: bool,
    /// Keep complete bins only.
    pub crop: bool,
}

/// The selected section and output geometry of a CASA image rebin.
pub struct ImageRebin<'a> {
    section: SubimagePlan,
    input_shape: Vec<usize>,
    binned_shape: Vec<usize>,
    factors: Vec<usize>,
    mask: Option<&'a str>,
    stretch: bool,
}

fn selection_error(text: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(text.into())
}

impl<'a> ImageRebin<'a> {
    /// Resolve the input section, bin factors and output metadata.
    ///
    /// # Errors
    /// A factor, axis, input region or output coordinate is invalid.
    pub fn new<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        coordinates: RecordValue,
        options: ImageRebinOptions<'a>,
        beams: &RecordValue,
    ) -> Result<Self, ImageError> {
        let ndim = image.ndim();
        if options.factors.is_empty() {
            return Err(selection_error("Rebinning factor vector cannot be empty"));
        }
        if options.factors.contains(&0) {
            return Err(selection_error("All factors must be positive"));
        }
        if options.factors.iter().all(|&factor| factor == 1) {
            return Err(selection_error(
                "All rebinning factors are 1, which means rebinning cannot occur",
            ));
        }
        if options.factors.len() > ndim {
            return Err(selection_error(
                "Factor vector length must be less than or equal to the number of input image axes",
            ));
        }
        let mut factors = options.factors.to_vec();
        factors.resize(ndim, 1);
        let spectral = image.find_axis(CoordinateType::Spectral);
        let stokes = image.find_axis(CoordinateType::Stokes);
        if stokes.is_some_and(|axis| factors[axis] > 1) {
            return Err(selection_error("A polarization axis cannot be rebinned"));
        }
        let positional: Vec<_> = (0..ndim)
            .filter(|&axis| Some(axis) != spectral && Some(axis) != stokes)
            .collect();
        let mut section = if options.region.is_none()
            && positional.len() > 2
            && image.find_axis(CoordinateType::Direction).is_none()
        {
            let mut selection = SubimageSelection::resolve_section(image, None, None, None, None)?;
            for axis in positional.into_iter().take(2) {
                selection.shape[axis] = 1;
            }
            SubimagePlan::from_selection(
                image,
                Some(coordinates),
                selection,
                false,
                &[],
                Some(beams),
            )?
        } else {
            SubimagePlan::new_with_beams(
                image,
                Some(coordinates),
                SubimageOptions {
                    region: options.region,
                    dropdeg: false,
                    keepaxes: &[],
                },
                Some(beams),
            )?
        };
        if options.dropdeg {
            for (axis, &size) in section.shape.iter().enumerate() {
                if image.shape()[axis] == 1 {
                    factors[axis] = 1;
                } else if size == 1 {
                    return Err(selection_error(
                        "Binning vector and lattice must have same dimension",
                    ));
                }
            }
        }
        if spectral.is_some_and(|axis| factors[axis] > 1) && section.image_info.beam_set.is_multi()
        {
            return Err(selection_error(
                "This image has multiple beams. The spectral axis cannot be rebinned",
            ));
        }
        let mut input_shape = section.shape.clone();
        if options.crop {
            for (size, &factor) in input_shape.iter_mut().zip(&factors) {
                *size -= *size % factor;
            }
            if input_shape.contains(&0) {
                let upper: Vec<_> = input_shape.iter().map(|&size| size as i128 - 1).collect();
                return Err(selection_error(format!(
                    "LCBox::LCBox - blc {:?} must be <= trc {upper:?}",
                    vec![0; ndim],
                )));
            }
        }
        let data_factors: Vec<_> = factors
            .iter()
            .zip(&input_shape)
            .map(|(&factor, &size)| factor.min(size))
            .collect();
        let binned_shape: Vec<_> = input_shape
            .iter()
            .zip(&data_factors)
            .map(|(&size, &factor)| size.div_ceil(factor))
            .collect();
        let kept_axes = resolve_kept_axes(&binned_shape, options.dropdeg, &[]).map_err(|_| {
            selection_error(
                "LatticeStepper::setCursorShape: cursorShape has no axes or more axes than lattice",
            )
        })?;
        let mut record = section
            .coordinate_record
            .take()
            .unwrap_or_else(|| section.coordinates.to_record());
        let mut coords = rebin_coordinate_record(&mut record, &factors)?;
        let mut native = Some(record);
        if kept_axes.len() != ndim {
            let spec = CoordReductionSpec {
                full_shape: &binned_shape,
                start: &vec![0; ndim],
                shape: &binned_shape,
                stride: None,
                kept_axes: &kept_axes,
                selected_stokes: None,
            };
            (coords, native, section.image_info) =
                reduce_coordinates_and_beams(&coords, native, spec, section.image_info)?;
        }
        section.shape = kept_axes.iter().map(|&axis| binned_shape[axis]).collect();
        section.kept_axes = kept_axes;
        section.coordinates = coords;
        section.coordinate_record = native;
        Ok(Self {
            section,
            input_shape,
            binned_shape,
            factors: data_factors,
            mask: options.mask,
            stretch: options.stretch,
        })
    }

    /// Output geometry and restoring beam metadata.
    pub fn section(&self) -> &SubimagePlan {
        &self.section
    }

    /// Source planes corresponding to the output for a beam-grid axis.
    pub fn beam_planes(&self, axis: Option<usize>, planes: usize) -> Vec<usize> {
        self.section.beam_planes(axis, planes)
    }

    /// Average selected pixels into an allocated output using bounded reads.
    /// Returns a validity mask when the input or selection uses one.
    ///
    /// # Errors
    /// A source read, mask calculation or output write fails.
    pub fn copy_to<T: ImageExprValue>(
        &self,
        image: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        self.section.copy_metadata(image, output)?;
        let ndim = image.ndim();
        let ones = vec![1; ndim];
        let selection = &self.section.selection;
        let steps = selection.stride.as_deref().unwrap_or(&ones);
        let input = StridedSection {
            start: &selection.start,
            shape: &self.input_shape,
            stride: steps,
        };
        let reader = PixelSections {
            image,
            selection,
            mask: PreparedMask::new(self.mask, image.shape(), image.coordinates(), self.stretch)?,
        };
        let binned = StridedSection {
            start: &vec![0; ndim],
            shape: &self.binned_shape,
            stride: &ones,
        };
        let kernel = self
            .factors
            .iter()
            .zip(steps)
            .map(|(&factor, &step)| (factor - 1) * step + 1)
            .fold(1_usize, usize::saturating_mul);
        let mut remaining = (MAX_TILE_SAMPLES / kernel).max(1);
        let output_cursor = image
            .nice_cursor_shape()
            .into_iter()
            .zip(&self.factors)
            .map(|(axis, &factor)| {
                let count = axis.div_ceil(factor).min(remaining).max(1);
                remaining /= count;
                count
            })
            .collect();
        let mut mask_out = None;
        for tile in binned.cursors(output_cursor) {
            let tile = tile?;
            let start = tile
                .position
                .iter()
                .zip(&self.factors)
                .map(|(&index, &factor)| index * factor)
                .collect::<Vec<_>>();
            let extent = tile
                .shape
                .iter()
                .zip(&self.factors)
                .zip(&start)
                .zip(&self.input_shape)
                .map(|(((&size, &factor), &start), &limit)| {
                    size.saturating_mul(factor).min(limit - start)
                })
                .collect::<Vec<_>>();
            let origin = input.source_start(&start);
            let (pixels, valid) = if kernel > MAX_TILE_SAMPLES {
                let (value, valid) = read_large_bin(&reader, &origin, &extent, steps)?;
                (
                    ArrayD::from_elem(IxDyn(&tile.shape), value),
                    valid.map(|valid| ArrayD::from_elem(IxDyn(&tile.shape), valid)),
                )
            } else {
                let (pixels, validity) = reader.read(&origin, &extent, steps)?;
                rebin_reduce_slice(
                    &pixels,
                    validity.as_ref(),
                    &self.factors,
                    &tile.shape,
                    &ones,
                )
            };
            let offset: Vec<_> = self
                .section
                .kept_axes
                .iter()
                .map(|&axis| tile.position[axis])
                .collect();
            output.put_slice(&self.section.squeeze(pixels), &offset)?;
            if let Some(valid) = valid {
                let valid = self.section.squeeze(valid);
                let mask = mask_out
                    .get_or_insert_with(|| ArrayD::from_elem(IxDyn(&self.section.shape), true));
                mask.slice_each_axis_mut(|slice| {
                    let axis = slice.axis.index();
                    sample_slice(offset[axis], valid.shape()[axis], 1)
                })
                .assign(&valid);
            }
        }
        Ok(mask_out)
    }
}

fn read_large_bin<T: ImageExprValue>(
    reader: &PixelSections<'_, T>,
    origin: &[usize],
    extent: &[usize],
    stride: &[usize],
) -> Result<(T, Option<bool>), ImageError> {
    let section = StridedSection {
        start: origin,
        shape: extent,
        stride,
    };
    let mut remaining = MAX_TILE_SAMPLES;
    // Retain CASA's axis-zero-fastest sum even when a bin spans many tiles.
    let dense_cursor = extent
        .iter()
        .zip(stride)
        .map(|(&size, &step)| {
            let dense = ((size - 1) * step + 1).min(remaining).max(1);
            remaining /= dense;
            dense
        })
        .collect();
    let mut sum = T::default_value();
    let mut count = 0;
    let mut masked = false;
    for tile in section.cursors(dense_cursor) {
        let tile = tile?;
        let start = section.source_start(&tile.position);
        let (values, valid) = reader.read(&start, &tile.shape, stride)?;
        masked |= valid.is_some();
        let valid = valid.as_ref().map(|mask| mask.view().reversed_axes());
        for (position, &value) in values.view().reversed_axes().indexed_iter() {
            if valid.as_ref().is_none_or(|mask| mask[position]) {
                sum = sum + value;
                count += 1;
            }
        }
    }
    if count > 0 {
        sum = sum / T::from_f64_lossy(count as f64);
    }
    Ok((sum, masked.then_some(count > 0)))
}
