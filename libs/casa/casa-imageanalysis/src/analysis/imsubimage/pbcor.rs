// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_core::{ArrayD, RecordValue};
use ndarray::IxDyn;

use casa_images::{ImageError, ImageInterface, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::impbcor::{Correction, beam::BeamLayout, normalize_box, normalize_stokes};
use super::super::lel_mask::PreparedMask;
use super::read_window::PixelSections;
use super::region::SubimageSelection;
use super::{SubimagePlan, SubimageRegion};

/// The real-valued primary beam applied to an attached image.
#[derive(Clone, Copy)]
pub enum ImagePbcorBeam<'a> {
    /// A full-size or direction-plane-shaped floating-point array.
    Array {
        /// Stored samples with one element on each repeated axis.
        pixels: &'a ArrayD<f32>,
        /// Logical beam shape before broadcasting.
        shape: &'a [usize],
    },
    /// A coordinate-compatible image, including its validity mask.
    Image(&'a dyn ImageInterface<f32>),
}

/// Region and numerical options for correcting an attached image.
pub struct ImagePbcorOptions<'a> {
    /// CASA selector beginning with d (divide) or m (multiply).
    pub mode: &'a str,
    /// A nonnegative threshold masks low values for division and high values for multiplication.
    pub cutoff: f32,
    /// Optional CASA region.
    pub region: Option<SubimageRegion<'a>>,
    /// Spatial box.
    pub box_selection: Option<&'a str>,
    /// Spectral channels.
    pub channels: Option<&'a str>,
    /// Stokes planes.
    pub stokes: Option<&'a str>,
    /// LEL expression combined with the input and beam masks.
    pub mask: Option<&'a str>,
    /// Stretch compatible singleton dimensions of the expression.
    pub stretch: bool,
    /// Final destination, if persistent, to protect named mask inputs.
    pub destination: Option<&'a Path>,
}

/// Resolved primary-beam correction geometry and source views.
pub struct ImagePbcor<'a> {
    /// Output shape, coordinates and metadata.
    pub section: SubimagePlan,
    correction: Correction,
    beam: ImagePbcorBeam<'a>,
    layout: BeamLayout,
    options: ImagePbcorOptions<'a>,
}

impl<'a> ImagePbcor<'a> {
    /// Build a correction plan without reading image pixels.
    ///
    /// # Errors
    /// The input, beam, mode, selection or coordinate layout is unsupported.
    pub fn new(
        image: &dyn ImageInterface<f32>,
        coordinates: RecordValue,
        beam: ImagePbcorBeam<'a>,
        options: ImagePbcorOptions<'a>,
        beams: &RecordValue,
    ) -> Result<Self, ImageError> {
        let correction = Correction::parse(options.mode)?;
        let layout = match beam {
            ImagePbcorBeam::Array { pixels, shape } => {
                if pixels.ndim() != shape.len()
                    || pixels
                        .shape()
                        .iter()
                        .zip(shape)
                        .any(|(&have, &want)| have != 1 && have != want)
                {
                    return Err(ImageError::ShapeMismatch {
                        expected: shape.to_vec(),
                        got: pixels.shape().to_vec(),
                    });
                }
                BeamLayout::for_array(image, shape)?
            }
            ImagePbcorBeam::Image(beam) => BeamLayout::for_image(image, beam)?,
        };
        let spatial = options
            .box_selection
            .map(|text| normalize_box(image, text))
            .transpose()?;
        let stokes = normalize_stokes(image, options.stokes);
        let selected = SubimageSelection::resolve_section(
            image,
            spatial.as_deref(),
            options.region,
            options.channels.filter(|text| {
                !text.trim().eq_ignore_ascii_case("all")
                    && image.find_axis(CoordinateType::Spectral).is_some()
            }),
            stokes.as_deref(),
        )?;
        if selected.has_multiple_regions {
            return Err(ImageError::CasaSelection(
                "Only a single n-dimensional rectangular region is supported".into(),
            ));
        }
        let section = SubimagePlan::from_selection(
            image,
            Some(coordinates),
            selected,
            false,
            &[],
            Some(beams),
        )?;
        Ok(Self {
            section,
            correction,
            beam,
            layout,
            options,
        })
    }

    /// Return the selected source planes from the restoring beam grid.
    pub fn beam_planes(&self, axis: Option<usize>, planes: usize) -> Vec<usize> {
        self.section.beam_planes(axis, planes)
    }

    /// Correct selected pixels in bounded tiles and carry their combined mask.
    ///
    /// # Errors
    /// Mask evaluation, pixel I/O or writing the output metadata failed.
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
        let stride = selection.stride.as_deref().unwrap_or(&ones);
        let steps = StridedSection {
            start: &selection.start,
            shape: &self.section.shape,
            stride,
        };
        let mut active_mask: Option<String> = None;
        for tile in steps.cursors(source.nice_cursor_shape()) {
            let tile = tile?;
            let origin = steps.source_start(&tile.position);
            let section = StridedSection {
                start: &origin,
                shape: &tile.shape,
                stride,
            };
            let (mut values, valid) = reader.read(section.start, section.shape, section.stride)?;
            let mut valid = valid.unwrap_or_else(|| ArrayD::from_elem(IxDyn(&tile.shape), true));
            let (beam, mask) = match self.beam {
                ImagePbcorBeam::Array { pixels, .. } => {
                    (self.layout.read_array(pixels, &section), None)
                }
                ImagePbcorBeam::Image(image) => self.layout.read_image(image, &section)?,
            };
            self.correction.apply_to_window(
                &mut values,
                &mut valid,
                &beam,
                mask.as_ref(),
                self.options.cutoff,
            )?;
            output.put_slice(&values, &tile.position)?;
            if valid.iter().all(|&value| value) {
                continue;
            }
            if active_mask.is_none() {
                active_mask = Some(output.create_filled_pixel_mask(true)?);
            }
            output.put_mask_slice(
                active_mask.as_deref().expect("active mask"),
                &valid,
                &tile.position,
            )?;
        }
        Ok(())
    }
}
