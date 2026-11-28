// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection and output metadata for the public image tool's regrid.

use std::f64::consts::{PI, TAU};
use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::RecordValue;
use casa_lattices::{ScratchSpace, TempStoragePolicy};

use crate::analysis::axes::stokes_for_axis;
use crate::analysis::ensure_outfile_does_not_alias_input;
use crate::analysis::imsubimage::coords::beam_section_indices;
use crate::analysis::imsubimage::{SubimageOptions, SubimagePlan, SubimageRegion};
use crate::analysis::lel_mask::ImageToolExpressionInputs;
use crate::regrid::{ImageToolSampling, RegridTemplate, Regridded, TaskRegridOptions, regrid_task};
use casa_images::resample::{RegridMethod, RegridPixel};
use casa_images::{ImageError, ImageInfo, ImageInterface, TempImage};

use super::rotation::rotated_grid;

/// How the target grid is constructed from a coordinate record or selected image.
#[derive(Clone, Copy)]
pub enum ImageRegridTarget<'a> {
    /// Target CASA record. An empty target retains each source axis and length.
    Coordinates(Option<&'a RecordValue>),
    /// Rotate the selected direction or linear coordinate by this many radians.
    Rotation(f64),
}

/// Output coordinates, selection and sampling controls for the public image tools.
pub struct ImageRegridOptions<'a> {
    /// Destination pixel lengths, or the source shape when omitted.
    pub shape: &'a [i64],
    /// The coordinate record or rotation used to construct the target grid.
    pub target: ImageRegridTarget<'a>,
    /// Pixel axes to regrid, or the compatible nondegenerate axes by default.
    pub axes: &'a [i64],
    /// Input region read before interpolation.
    pub region: Option<SubimageRegion<'a>>,
    /// Lattice expression contributing to the input validity mask.
    pub mask: Option<&'a str>,
    /// Interpolation method as accepted by casacore.
    pub method: &'a str,
    /// Sampling interval for computing the plane's coordinate map.
    pub decimate: i64,
    /// Replicate by relative pixel position.
    pub replicate: bool,
    /// Convert direction and spectral reference frames during sampling.
    pub convert_reference: bool,
    /// Remove singleton axes from the input region before regridding.
    pub dropdeg: bool,
    /// Interpolate even when the two grids already match.
    pub force: bool,
    /// Interpolate the spectral coordinate in velocity space.
    pub as_velocity: bool,
    /// Broadcast a conformant mask over matching singleton axes.
    pub stretch: bool,
    /// Public output destination, used to guard named mask operands.
    pub outfile: Option<&'a Path>,
}

/// Regridded pixels and the native coordinate and restoring-beam metadata.
pub struct ImageRegridProduct<T> {
    /// Output values, validity and typed coordinate system.
    pub regridded: Regridded<T>,
    /// Output coordinate record for temporary attachments and native storage.
    pub coordinate_record: RecordValue,
    /// Image type and the selected subset of the input restoring beams.
    pub image_info: ImageInfo,
    /// Original channel and polarization indices for the output beams.
    pub beam_planes: [Vec<usize>; 2],
}

/// Regrid an open image onto a CASA coordinate record.
/// # Errors
/// An invalid output grid, unsupported region, unreadable mask or pixels, or
/// a destination whose grid has no overlap with the selected input.
pub fn regrid_image<T: RegridPixel>(
    input: &dyn ImageInterface<T>,
    native: RecordValue,
    beam_record: &RecordValue,
    options: ImageRegridOptions<'_>,
) -> Result<ImageRegridProduct<T>, ImageError> {
    let rotating = matches!(options.target, ImageRegridTarget::Rotation(_));
    if !rotating && is_pv_grid(input.coordinates()) {
        return Err(invalid("PV images are not supported by regrid"));
    }
    let shape_specified = options.shape.first().is_some_and(|&size| size >= 0);
    let shape: Vec<i64> = if !shape_specified {
        input
            .shape()
            .iter()
            .copied()
            .filter(|&size| !(rotating && options.dropdeg && size == 1))
            .map(|size| size as i64)
            .collect()
    } else {
        options.shape.to_vec()
    };
    let output_shape = shape
        .iter()
        .map(|&size| {
            usize::try_from(size)
                .ok()
                .filter(|&size| size > 0)
                .ok_or_else(|| invalid("output shape lengths must be positive"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let explicit_target = match options.target {
        ImageRegridTarget::Coordinates(Some(record)) => {
            Some(CoordinateSystem::from_record(record)?)
        }
        _ => None,
    };
    let info = ImageInfo::from_record(beam_record)?;
    if let (Some(output), Some(mask)) = (options.outfile, options.mask) {
        for operand in ImageToolExpressionInputs::resolve(mask)?.paths() {
            ensure_outfile_does_not_alias_input(output, operand)?;
        }
    }

    let selection = (options.region.is_some() || options.mask.is_some() || options.dropdeg)
        .then(|| {
            SubimagePlan::new_with_beams(
                input,
                Some(native.clone()),
                SubimageOptions {
                    region: options.region,
                    dropdeg: options.dropdeg,
                    keepaxes: &[],
                },
                Some(beam_record),
            )
        })
        .transpose()?;
    if let Some(selection) = &selection {
        selection.require_single_region()?;
    }
    let (selected_coords, selected_shape) = selection
        .as_ref()
        .map_or((input.coordinates(), input.shape()), |selection| {
            (&selection.coordinates, selection.shape.as_slice())
        });
    let ndim = selected_shape.len();
    if rotating && is_pv_grid(selected_coords) {
        return Err(invalid("PV images are not supported by rotate"));
    }
    let has_rotation_shape = rotating && !options.shape.is_empty() && options.shape != [-1];
    if output_shape.len() != ndim || (has_rotation_shape && options.shape.len() != ndim) {
        return Err(invalid(
            "The number of pixel axes in the output shape and Coordinate System must be the same",
        ));
    }
    let rotation_axes;
    let (target, axes) = match options.target {
        ImageRegridTarget::Coordinates(_) => (
            explicit_target.unwrap_or_else(CoordinateSystem::new),
            options.axes,
        ),
        ImageRegridTarget::Rotation(angle) => {
            let (target, axes) =
                rotated_grid(selected_coords, angle, selected_shape, &output_shape)?;
            rotation_axes = axes;
            (target, rotation_axes.as_slice())
        }
    };
    let all_axes = axes.is_empty() || axes == [-1];
    if info.beam_set.is_multi()
        && target.find_coordinate(CoordinateType::Spectral).is_some()
        && let Some(axis) = selected_coords
            .find_coordinate(CoordinateType::Spectral)
            .and_then(|index| selected_coords.coordinate_pixel_axes(index))
            .and_then(|axes| axes.into_iter().flatten().next())
        && (all_axes || axes.contains(&(axis as i64)))
    {
        return Err(invalid(
            "An image with multiple beams cannot be regridded along the spectral axis",
        ));
    }
    let selected = selection
        .as_ref()
        .map(|selection| {
            let mut image = TempImage::new(
                selection.shape.clone(),
                selection.coordinates.clone(),
                TempStoragePolicy::Auto {
                    memory_budget_bytes: 64 * 1024 * 1024,
                    scratch: ScratchSpace::SystemTemp,
                },
            )?;
            selection.copy_to(input, &mut image, options.mask, options.stretch)?;
            Ok::<_, ImageError>(image)
        })
        .transpose()?;
    let source = selected
        .as_ref()
        .map_or(input, |image| image as &dyn ImageInterface<T>);
    let template = RegridTemplate {
        shape: template_shape(source.coordinates(), &target, &output_shape),
        coordinates: target,
    };
    let regridded = regrid_task(
        source,
        &template,
        TaskRegridOptions {
            axes,
            shape: &shape,
            as_velocity: options.as_velocity,
            decimate: options.decimate.max(0),
            replicate: options.replicate,
            method: RegridMethod::from_image_tool(options.method)?,
            cancellation: None,
            image_tool: Some(ImageToolSampling {
                force: options.force,
                convert_reference: options.convert_reference,
                shape_specified,
                half_turn: matches!(options.target, ImageRegridTarget::Rotation(angle) if angle.rem_euclid(TAU) == PI),
            }),
        },
    )?;
    let calculated_record = regridded.coordinates.to_record();
    let coordinate_record = if calculated_record == source.coordinates().to_record() {
        if let Some(selection) = &selection {
            selection
                .coordinate_record
                .clone()
                .unwrap_or(calculated_record)
        } else {
            native
        }
    } else {
        calculated_record
    };
    let (image_info, beam_planes) = output_beams(input, &regridded, info, selection.as_ref())?;
    Ok(ImageRegridProduct {
        regridded,
        coordinate_record,
        image_info,
        beam_planes,
    })
}

fn is_pv_grid(coordinates: &CoordinateSystem) -> bool {
    coordinates
        .find_coordinate(CoordinateType::Spectral)
        .and_then(|index| coordinates.coordinate_pixel_axes(index))
        .is_some_and(|axes| axes.into_iter().any(|axis| axis.is_some()))
        && coordinates
            .find_coordinate(CoordinateType::Linear)
            .is_some_and(|index| {
                coordinates
                    .coordinate_pixel_axes(index)
                    .expect("existing coordinate")
                    .into_iter()
                    .zip(coordinates.coordinate(index).axis_names())
                    .any(|(axis, name)| axis.is_some() && name == "Offset")
            })
}

fn template_shape(
    input: &CoordinateSystem,
    target: &CoordinateSystem,
    shape: &[usize],
) -> Vec<usize> {
    let mut target_shape = vec![1; target.n_pixel_axes()];
    for index in 0..target.n_coordinates() {
        let Some(input_index) = input.find_coordinate(target.coordinate(index).coordinate_type())
        else {
            continue;
        };
        let input_axes = input
            .coordinate_pixel_axes(input_index)
            .expect("input coordinate exists");
        let target_axes = target
            .coordinate_pixel_axes(index)
            .expect("target coordinate exists");
        for (from, to) in input_axes.into_iter().zip(target_axes) {
            if let (Some(from), Some(to)) = (from, to) {
                target_shape[to] = shape[from];
            }
        }
    }
    target_shape
}

fn output_beams<T: RegridPixel>(
    input: &dyn ImageInterface<T>,
    product: &Regridded<T>,
    mut info: ImageInfo,
    selection: Option<&SubimagePlan>,
) -> Result<(ImageInfo, [Vec<usize>; 2]), ImageError> {
    let mut planes = if let Some(plan) = selection {
        let section = &plan.selection;
        beam_section_indices(
            &info.beam_set,
            input.coordinates(),
            input.ndim(),
            &section.start,
            &section.packed_shape(),
            section.stride.as_deref(),
        )
    } else {
        [
            (0..info.beam_set.n_channels()).collect(),
            (0..info.beam_set.n_stokes()).collect(),
        ]
    };
    if info.beam_set.is_multi() {
        let output_stokes = product
            .coordinates
            .find_coordinate(CoordinateType::Stokes)
            .and_then(|index| product.coordinates.coordinate_pixel_axes(index))
            .and_then(|axes| axes.into_iter().flatten().next());
        if let (Some(input_axis), Some(output_axis)) =
            (input.find_axis(CoordinateType::Stokes), output_stokes)
            && info.beam_set.n_stokes() > 1
        {
            let original = stokes_for_axis(input.coordinates(), input_axis)?;
            planes[1] = stokes_for_axis(&product.coordinates, output_axis)?
                .iter()
                .map(|pol| {
                    original
                        .iter()
                        .position(|value| value == pol)
                        .ok_or_else(|| invalid("output polarization has no input restoring beam"))
                })
                .collect::<Result<_, _>>()?;
        }
        info.beam_set = info.beam_set.subset(&planes[0], &planes[1])?;
    }
    Ok((info, planes))
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
