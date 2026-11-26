// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateType;
use casa_core::{ArrayD, RecordValue};
use ndarray::Slice;

use crate::{ImageError, ImageInterface, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::lel_mask::evaluate_strided_mask;
use super::{SubimagePlan, SubimageRegion, and_opt_mask, region::SubimageSelection};

/// Selection and mask used to trim invalid outer planes of a float image.
pub struct ImageCropOptions<'a> {
    /// Axes to trim, or all axes if empty.
    pub axes: &'a [i64],
    /// Spatial box selection.
    pub box_selection: Option<&'a str>,
    /// CASA pixel or world region.
    pub region: Option<SubimageRegion<'a>>,
    /// Spectral selection.
    pub channels: Option<&'a str>,
    /// Stokes selection.
    pub stokes: Option<&'a str>,
    /// Optional lattice expression combined with the image mask.
    pub mask: Option<&'a str>,
    /// Stretch a compatible singleton mask axis.
    pub stretch: bool,
}

/// The clipped pixel geometry and mask for an image crop.
pub struct ImageCrop {
    /// Output geometry and coordinate metadata.
    pub section: SubimagePlan,
    mask: Option<ArrayD<bool>>,
}

impl ImageCrop {
    /// Resolve the selection and locate its first and last valid planes.
    ///
    /// # Errors
    /// An unsupported selection, axis, or expression, or a fully masked image region.
    pub fn new(
        image: &dyn ImageInterface<f32>,
        coordinates: Option<RecordValue>,
        options: ImageCropOptions<'_>,
        beam_record: Option<&RecordValue>,
    ) -> Result<Self, ImageError> {
        let axes = crop_axes(options.axes, image.ndim())?;
        let selected = SubimageSelection::resolve_section(
            image,
            options.box_selection.filter(|_| image.ndim() >= 2),
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
        if selected.has_multiple_regions {
            return Err(ImageError::CasaSelection(
                "Only a single n-dimensional rectangular region is supported".into(),
            ));
        }
        let stride = selected
            .stride
            .clone()
            .unwrap_or_else(|| vec![1; image.ndim()]);
        let shape = selected.packed_shape();
        let section = StridedSection {
            start: &selected.start,
            shape: &shape,
            stride: &stride,
        };
        let mut mask = image.get_mask_slice(section.start, section.shape, section.stride)?;
        if selected.stride.is_none() {
            selected.apply_selection_masks(&mut mask);
        }
        if let Some(expression) = evaluate_strided_mask(
            options.mask,
            image.shape(),
            image.coordinates(),
            options.stretch,
            &section,
        )? {
            and_opt_mask(&mut mask, expression);
        }
        let (start, end) = crop_bounds(mask.as_ref(), &shape, &axes)?;
        let mut cropped = SubimageSelection::resolve(image, None, None, None, None)?;
        cropped.start = section.source_start(&start);
        cropped.shape = start
            .iter()
            .zip(&end)
            .zip(&stride)
            .map(|((&start, &end), &step)| (end - start - 1) * step + 1)
            .collect();
        cropped.stride = selected.stride;
        let mask = mask.map(|mask| {
            if start.iter().all(|&axis| axis == 0) && end == shape {
                return mask;
            }
            mask.slice_each_axis(|axis| {
                let dim = axis.axis.index();
                Slice::from(start[dim]..end[dim])
            })
            .to_owned()
        });
        let section =
            SubimagePlan::from_selection(image, coordinates, cropped, false, &[], beam_record)?;
        Ok(Self { section, mask })
    }

    /// Source planes corresponding to the output for a beam-grid axis.
    pub fn beam_planes(&self, axis: Option<usize>, planes: usize) -> Vec<usize> {
        self.section.beam_planes(axis, planes)
    }

    /// Copy the resolved pixels, metadata and mask into an allocated image.
    ///
    /// # Errors
    /// The output shape differs from the plan, or source or destination I/O fails.
    pub fn copy_to(
        self,
        image: &dyn ImageInterface<f32>,
        output: &mut dyn MutableImageInterface<f32>,
    ) -> Result<(), ImageError> {
        self.section.copy_metadata(image, output)?;
        self.section.write_selection_mask(output, self.mask)?;
        let selection = &self.section.selection;
        let ones = vec![1; image.ndim()];
        let section = StridedSection {
            start: &selection.start,
            shape: &self.section.shape,
            stride: selection.stride.as_deref().unwrap_or(&ones),
        };
        for cursor in section.cursors(image.nice_cursor_shape()) {
            let cursor = cursor?;
            let source = section.source_start(&cursor.position);
            let pixels = image.get_slice(&source, &cursor.shape, section.stride)?;
            output.put_slice(&pixels, &cursor.position)?;
        }
        Ok(())
    }
}

fn crop_axes(requested: &[i64], count: usize) -> Result<Vec<usize>, ImageError> {
    if requested.is_empty() {
        return Ok((0..count).collect());
    }
    let mut axes = Vec::new();
    for &axis in requested {
        if axis < 0 {
            return Err(ImageError::CasaSelection(
                "All axes values must be >= 0".into(),
            ));
        }
        if axis as usize >= count {
            return Err(ImageError::CasaSelection(format!(
                "Illegal axis number {axis}. Image has only {count} dimensions"
            )));
        }
        axes.push(axis as usize);
    }
    axes.sort_unstable();
    axes.dedup();
    Ok(axes)
}

fn crop_bounds(
    mask: Option<&ArrayD<bool>>,
    shape: &[usize],
    axes: &[usize],
) -> Result<(Vec<usize>, Vec<usize>), ImageError> {
    let Some(mask) = mask else {
        return Ok((vec![0; shape.len()], shape.to_vec()));
    };
    let mut first = shape.to_vec();
    let mut last = vec![0; shape.len()];
    let mut found = false;
    for (pixel, &valid) in mask.indexed_iter() {
        if valid {
            found = true;
            for &axis in axes {
                first[axis] = first[axis].min(pixel[axis]);
                last[axis] = last[axis].max(pixel[axis] + 1);
            }
        }
    }
    if !found {
        return Err(ImageError::CasaSelection(
            "(Sub)image is completely masked.".into(),
        ));
    }
    let mut start = vec![0; shape.len()];
    let mut end = shape.to_vec();
    for &axis in axes {
        // CASA searches both edges only as far as the central plane.
        let halfway = shape[axis] / 2;
        if first[axis] > halfway || last[axis] + halfway < shape[axis] {
            return Err(ImageError::CasaSelection(
                "Failed AlwaysAssert minFound && maxFound".into(),
            ));
        }
        start[axis] = first[axis];
        end[axis] = last[axis];
    }
    Ok((start, end))
}
