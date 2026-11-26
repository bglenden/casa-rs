// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::{ArrayD, RecordValue};
use ndarray::IxDyn;

use crate::{ImageError, ImageInterface, MutableImageInterface};

use super::super::axes::{StridedSection, direction_plane_axes};
use super::super::lel_mask::PreparedMask;
use super::read_window::PixelSections;
use super::region::SubimageSelection;
use super::{SubimagePlan, SubimageRegion};

/// The border and source selection for direction-plane padding.
pub struct ImagePadOptions<'a> {
    /// Positive, equal border width at both ends of each direction axis.
    pub pixels: usize,
    /// The numeric value stored in the border.
    pub value: f32,
    /// Whether the border pixels are valid.
    pub padmask: bool,
    /// An optional CASA region selected before padding.
    pub region: Option<SubimageRegion<'a>>,
    /// Rectangular spatial selection.
    pub box_selection: Option<&'a str>,
    /// Spectral channel selection.
    pub channels: Option<&'a str>,
    /// Polarization selection.
    pub stokes: Option<&'a str>,
    /// A LEL expression combined with input validity.
    pub mask: Option<&'a str>,
    /// Stretch compatible singleton dimensions of the expression.
    pub stretch: bool,
    /// Final destination, if persistent, to protect named mask inputs.
    pub destination: Option<&'a Path>,
}

/// The resolved input section, padded shape, and shifted coordinates.
pub struct ImagePad<'a> {
    /// The final geometry and image metadata.
    pub section: SubimagePlan,
    placement: Vec<usize>,
    options: ImagePadOptions<'a>,
}

impl<'a> ImagePad<'a> {
    /// Resolve the input selection and padded output geometry.
    ///
    /// # Errors
    /// The width, direction plane, selection, or output coordinate system is invalid.
    pub fn new(
        image: &dyn ImageInterface<f32>,
        coordinates: RecordValue,
        options: ImagePadOptions<'a>,
        beam_record: &RecordValue,
    ) -> Result<Self, ImageError> {
        if options.pixels == 0 {
            return Err(ImageError::CasaSelection(
                "Value of npixels must be greater than zero".into(),
            ));
        }
        if image.find_axis(CoordinateType::Direction).is_none() {
            return Err(ImageError::CasaSelection(
                "Image does not have required coordinate Direction".into(),
            ));
        }
        let axes = direction_plane_axes(image)?;
        let selection = SubimageSelection::resolve_section(
            image,
            options.box_selection,
            options.region,
            options.channels.filter(|text| {
                !text.trim().eq_ignore_ascii_case("all")
                    && image.find_axis(CoordinateType::Spectral).is_some()
            }),
            options.stokes.filter(|text| {
                !text.trim().eq_ignore_ascii_case("all")
                    && image.find_axis(CoordinateType::Stokes).is_some()
            }),
        )?;
        if selection.has_multiple_regions {
            return Err(ImageError::CasaSelection(
                "Only a single n-dimensional rectangular region is supported".into(),
            ));
        }
        let mut section = SubimagePlan::from_selection(
            image,
            Some(coordinates),
            selection,
            false,
            &[],
            Some(beam_record),
        )?;
        let mut placement = vec![0; section.shape.len()];
        let too_large = || ImageError::CasaSelection("Padded image shape is too large".into());
        let both_edges = options.pixels.checked_mul(2).ok_or_else(too_large)?;
        for axis in axes {
            placement[axis] = options.pixels;
            section.shape[axis] = section.shape[axis]
                .checked_add(both_edges)
                .ok_or_else(too_large)?;
        }
        section
            .shape
            .iter()
            .try_fold(std::mem::size_of::<f32>(), |size, axis| {
                size.checked_mul(*axis)
                    .filter(|size| *size <= isize::MAX as usize)
            })
            .ok_or_else(too_large)?;
        let mut record = section
            .coordinate_record
            .take()
            .unwrap_or_else(|| section.coordinates.to_record());
        let original = section.coordinates.reference_pixel();
        for axis in axes {
            CoordinateSystem::set_reference_pixel_in_record(
                &mut record,
                axis,
                original[axis] + options.pixels as f64,
            )?;
        }
        section.coordinates = section.coordinates.restore_record_with_layout(&record)?;
        section.coordinate_record = Some(record);
        Ok(Self {
            section,
            placement,
            options,
        })
    }

    /// Source plane indices retained on a per-plane beam axis.
    pub fn beam_planes(&self, axis: Option<usize>, planes: usize) -> Vec<usize> {
        self.section.beam_planes(axis, planes)
    }

    /// Fill an allocated output and stream the interior values and mask into it.
    ///
    /// # Errors
    /// Returns an error for invalid mask expressions or input/output I/O.
    pub fn copy_to(
        &self,
        image: &dyn ImageInterface<f32>,
        output: &mut dyn MutableImageInterface<f32>,
    ) -> Result<(), ImageError> {
        let selection = &self.section.selection;
        let reader = PixelSections {
            image,
            selection,
            mask: PreparedMask::new_for_output(
                self.options.mask,
                image.shape(),
                image.coordinates(),
                self.options.stretch,
                self.options.destination,
            )?,
        };
        self.section.copy_metadata(image, output)?;
        output.set(self.options.value)?;
        let ones = vec![1; image.ndim()];
        let shape = selection.packed_shape();
        let stride = selection.stride.as_deref().unwrap_or(&ones);
        let interior = StridedSection {
            start: &selection.start,
            shape: &shape,
            stride,
        };
        let padmask = self.options.padmask;
        let mut mask_name = if padmask {
            None
        } else {
            Some(output.create_filled_pixel_mask(false)?)
        };
        for tile in interior.cursors(image.nice_cursor_shape()) {
            let tile = tile?;
            let input_origin = interior.source_start(&tile.position);
            let (pixels, validity) = reader.read(&input_origin, &tile.shape, stride)?;
            let output_origin: Vec<_> = tile
                .position
                .iter()
                .zip(&self.placement)
                .map(|(&index, &offset)| index + offset)
                .collect();
            output.put_slice(&pixels, &output_origin)?;
            if padmask
                && validity
                    .as_ref()
                    .is_none_or(|mask| mask.iter().all(|&valid| valid))
            {
                continue;
            }
            if mask_name.is_none() {
                mask_name = Some(output.create_filled_pixel_mask(true)?);
            }
            let validity = validity.unwrap_or_else(|| ArrayD::from_elem(IxDyn(&tile.shape), true));
            output.put_mask_slice(
                mask_name.as_deref().expect("active mask"),
                &validity,
                &output_origin,
            )?;
        }
        Ok(())
    }
}
