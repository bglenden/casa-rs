// SPDX-License-Identifier: LGPL-3.0-or-later

mod kernel;
mod long_profile;

use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_core::{
    RecordValue,
    quanta::{Quantity, Unit},
};
use casa_lattices::{TraversalCursorIter, TraversalSpec};
use ndarray::{Axis, Zip};

use crate::{ImageError, ImageInterface, MutableImageInterface};

use super::super::{axes::StridedSection, lel_mask::PreparedMask};
use super::{SubimageOptions, SubimagePlan, SubimageRegion, read_window::PixelSections};
use kernel::{Kernel, LineConvolution, Weights};

const PROFILE_TILE_SAMPLES: usize = 256 * 1024;

/// Input section, per-axis kernels and scaling for CASA's separable convolution.
pub struct SepConvolutionOptions<'a> {
    /// Pixel axes in convolution order. An empty list or `[-1]` selects the first axes.
    pub axes: &'a [i64],
    /// Gaussian, boxcar, or Hanning for each width. A single empty name means all Gaussian.
    pub types: &'a [String],
    /// One width per axis, either in pixels or a conforming world unit.
    pub widths: &'a [Quantity],
    /// Negative for normalization, or a multiplier for each kernel.
    pub scale: f64,
    /// Region used both for pixels and for the returned coordinates.
    pub region: Option<SubimageRegion<'a>>,
    /// Extra LEL selection evaluated before convolution.
    pub mask: Option<&'a str>,
    /// Broadcast compatible singleton axes in the LEL mask.
    pub stretch: bool,
    /// Final destination, if writing a native image, for mask operand protection.
    pub destination: Option<&'a Path>,
}

/// Select a Float subimage and convolve it along successive pixel axes.
pub struct ImageSepConvolution<'a> {
    /// Output section and its image metadata.
    pub section: SubimagePlan,
    kernels: Vec<(usize, Weights)>,
    options: SepConvolutionOptions<'a>,
}

impl<'a> ImageSepConvolution<'a> {
    /// Resolve the selected grid and build the convolution kernels.
    ///
    /// # Errors
    /// The region, kernel dimensions, axes or width units do not describe a
    /// valid convolution of this Float image.
    pub fn new(
        source: &dyn ImageInterface<f32>,
        coordinates: RecordValue,
        beams: &RecordValue,
        options: SepConvolutionOptions<'a>,
    ) -> Result<Self, ImageError> {
        let section = SubimagePlan::new_with_beams(
            source,
            Some(coordinates),
            SubimageOptions {
                region: options.region,
                dropdeg: false,
                keepaxes: &[],
            },
            Some(beams),
        )?;
        let defaults = options.axes.is_empty() || options.axes == [-1];
        let gaussian = options.types == [""];
        let n = options.widths.len();
        if n == 0 {
            return Err(invalid("You have not specified any axes to convolve"));
        }
        if (!defaults && options.axes.len() != n) || (!gaussian && options.types.len() != n) {
            return Err(invalid(
                "You must give the same number of axes, kernels and widths",
            ));
        }
        let mut used = vec![false; section.shape.len()];
        let mut kernels = Vec::with_capacity(n);
        for (index, width) in options.widths.iter().enumerate() {
            let axis = if defaults {
                index
            } else {
                usize::try_from(options.axes[index])
                    .map_err(|_| invalid("Invalid convolution axis"))?
            };
            let selected = used
                .get_mut(axis)
                .ok_or_else(|| invalid("Convolution axis is outside the input image"))?;
            if *selected {
                return Err(invalid("You have already given this axis to be convolved"));
            }
            *selected = true;
            let kind = if gaussian {
                Kernel::Gaussian
            } else {
                Kernel::parse(&options.types[index])?
            };
            let width = pixel_width(&section.coordinates, axis, width)?;
            kernels.push((
                axis,
                kind.weights(section.shape[axis], width as f32, options.scale)?,
            ));
        }
        Ok(Self {
            section,
            kernels,
            options,
        })
    }

    /// Copy the selected Float data, mask and metadata, then convolve batches
    /// of independent profiles in place. Only the initial copy zeroes masked
    /// samples. Every subsequent axis uses the previous pass's raw values.
    ///
    /// # Errors
    /// The mask cannot be evaluated, an image section cannot be read or
    /// written, or the convolution plan cannot be initialized.
    pub fn copy_to(
        &self,
        source: &dyn ImageInterface<f32>,
        output: &mut dyn MutableImageInterface<f32>,
    ) -> Result<(), ImageError> {
        let selection = &self.section.selection;
        let reader = PixelSections {
            image: source,
            selection,
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
        let section = StridedSection {
            start: &selection.start,
            shape: &self.section.shape,
            stride: selection.stride.as_deref().unwrap_or(&ones),
        };
        let mut active_mask = None;
        for tile in section.cursors(source.nice_cursor_shape()) {
            let tile = tile?;
            let start = section.source_start(&tile.position);
            let (mut pixels, valid) = reader.read(&start, &tile.shape, section.stride)?;
            if let Some(valid) = valid {
                Zip::from(&mut pixels)
                    .and(&valid)
                    .for_each(|value, &valid| {
                        if !valid {
                            *value = 0.0;
                        }
                    });
                if active_mask.is_none() {
                    active_mask = Some(output.create_filled_pixel_mask(true)?);
                }
                output.put_mask_slice(
                    active_mask.as_deref().expect("mask was created"),
                    &valid,
                    &tile.position,
                )?;
            }
            output.put_slice(&pixels, &tile.position)?;
        }
        for (axis, weights) in &self.kernels {
            convolve_axis(output, *axis, weights, &ones)?;
        }
        Ok(())
    }
}

fn pixel_width(
    coords: &CoordinateSystem,
    axis: usize,
    width: &Quantity,
) -> Result<f64, ImageError> {
    if width.unit().name() == "pix" {
        return Ok(width.value());
    }
    coords
        .pixel_axis_to_world_axis(axis)
        .ok_or_else(|| invalid("Kernel axis has no corresponding world axis"))?;
    let (index, local) = coords
        .pixel_axis_owner(axis)
        .ok_or_else(|| invalid("Kernel axis has no owning coordinate"))?;
    let coord = coords.coordinate(index);
    let unit = Unit::new(&coord.axis_units()[local]).map_err(|error| invalid(error.to_string()))?;
    let physical = width.get_value_in(&unit).map_err(|error| {
        invalid(format!(
            "Specified width units are inconsistent with image axis unit: {error}"
        ))
    })?;
    Ok((physical / coord.increment()[local]).abs())
}

fn convolve_axis(
    image: &mut dyn MutableImageInterface<f32>,
    axis: usize,
    weights: &Weights,
    stride: &[usize],
) -> Result<(), ImageError> {
    let shape = image.shape().to_vec();
    let length = shape[axis];
    let halo = weights.halo();
    if length > PROFILE_TILE_SAMPLES && halo.iter().sum::<usize>() <= PROFILE_TILE_SAMPLES / 4 {
        return long_profile::convolve(image, axis, weights, halo, stride);
    }
    let mut profile_budget = (PROFILE_TILE_SAMPLES / length).max(1);
    let mut cursor = image.nice_cursor_shape();
    cursor[axis] = length;
    for (index, size) in cursor.iter_mut().enumerate() {
        if index != axis {
            *size = (*size).min(profile_budget);
            profile_budget /= *size;
        }
    }
    let mut convolution = LineConvolution::new(length, weights)?;
    for tile in TraversalCursorIter::new(shape, cursor, TraversalSpec::tiles()) {
        let tile = tile?;
        let mut values = image.get_slice(&tile.position, &tile.shape, stride)?;
        for line in values.lanes_mut(Axis(axis)) {
            convolution.apply(line)?;
        }
        image.put_slice(&values, &tile.position)?;
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
