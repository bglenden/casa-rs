// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_components::ComponentList;
use casa_coordinates::CoordinateType;
use casa_types::RecordValue;
use ndarray::Dimension;

use crate::analysis::axes::direction_plane_axes;
use crate::image_expr::bounded_read_cursor;

use super::super::region::SubimageSelection;
use super::peaks::{self, PeakRanking};
use super::{
    ImageError, ImageInterface, PixelSections, StridedSection, SubimagePlan, SubimageRegion,
    components_record, fit_component, invalid, sky_plane,
};

/// Selects the image region and fit to apply around its strongest source.
pub struct MaxFitOptions<'a> {
    /// Image region, or the full image when absent.
    pub region: Option<SubimageRegion<'a>>,
    /// Fit a point source.
    pub point: bool,
    /// Half-width of a Gaussian fit window in selected pixels.
    pub width: i64,
    /// Allow negative sources, ranking both signs by magnitude.
    pub negative: bool,
    /// Include brightness and position diagnostics.
    pub list: bool,
}

/// A single source and the optional diagnostics for its selected position.
pub struct ImageMaxFit {
    /// CASA component-list record.
    pub record: RecordValue,
    /// Log messages to emit after releasing any image lock.
    pub messages: Vec<String>,
}

/// Find the strongest sky plane in a masked region and fit its brightest source.
///
/// # Errors
/// An invalid region, absent direction coordinate, multiple beams, failed
/// image read, or a region without a measurable source returns an image error.
pub fn fit_image_maximum(
    image: &dyn ImageInterface<f32>,
    coordinate_record: RecordValue,
    options: MaxFitOptions<'_>,
) -> Result<ImageMaxFit, ImageError> {
    if image.image_info()?.beam_set.has_multi_beam() {
        return Err(invalid(
            "This application does not support images with multiple beams",
        ));
    }
    let coords = image.coordinates();
    if coords.find_coordinate(CoordinateType::Direction).is_none() {
        return Err(invalid("The image must contain a direction coordinate"));
    }
    let direction_axes = direction_plane_axes(image)?;
    let mut selection =
        SubimageSelection::resolve_section(image, None, options.region, None, None)?;
    if let Some(axis) = image.find_axis(CoordinateType::Stokes) {
        // CASA searches the first Stokes plane included in the region.
        selection.shape[axis] = 1;
        if let Some(stokes) = &mut selection.selected_stokes {
            stokes.truncate(1);
        }
    }
    let unit = vec![1; image.ndim()];
    let origin = selection.start.clone();
    let packed = selection.packed_shape();
    let stride = selection.stride.as_deref().unwrap_or(&unit);
    let cut = StridedSection {
        start: &origin,
        shape: &packed,
        stride,
    };
    let position = brightest_pixel(image, &selection, &cut, options.negative)?;
    let absolute = cut.source_start(&position);
    for (axis, &pixel) in absolute.iter().enumerate() {
        if !direction_axes.contains(&axis) {
            selection.start[axis] = pixel;
            selection.shape[axis] = 1;
        }
    }
    let plan =
        SubimagePlan::from_selection(image, Some(coordinate_record), selection, true, &[], None)?;
    let sky = sky_plane(&plan, image.units())?;
    let selection = &plan.selection;
    let shape = selection.packed_shape();
    let section = StridedSection {
        start: &selection.start,
        shape: &shape,
        stride: selection.stride.as_deref().unwrap_or(&unit),
    };
    let reader = PixelSections {
        image,
        selection,
        mask: None,
    };
    let ranking = if options.negative {
        PeakRanking::Absolute
    } else {
        PeakRanking::Positive
    };
    let peak = peaks::search(&reader, &plan, &section, 1, 0.1, ranking)?
        .pop()
        .ok_or_else(|| invalid("No sources were found in the selected sky plane"))?;
    let width = if options.point { 0 } else { options.width };
    let (component, peak) = fit_component(&reader, &section, &plan, &sky, peak, width)?;
    let mut messages = Vec::new();
    if options.list {
        let mut selected_pixel = position
            .iter()
            .map(|&pixel| pixel as f64)
            .collect::<Vec<_>>();
        for (&axis, sky_pixel) in plan.kept_axes.iter().zip(peak.position) {
            selected_pixel[axis] = sky_pixel;
        }
        let source_pixel = selected_pixel
            .iter()
            .zip(origin)
            .zip(section.stride)
            .map(|((&pixel, origin), &step)| origin as f64 + pixel * step as f64)
            .collect::<Vec<_>>();
        let values = component.flux().values().map(|value| value.re);
        messages.push(format!(
            "Brightness     = {values:?} {}",
            component.flux().unit().name()
        ));
        messages.push(format!("World Axis Units: {:?}", coords.world_axis_units()));
        if let Ok(world) = coords.to_world(&source_pixel) {
            messages.push(format!("Absolute world = {world:?}"));
            let mut relative = world;
            if coords.convert_world_offsets(&mut relative, false).is_ok() {
                messages.push(format!("Relative world = {relative:?}"));
            }
        }
        let relative = offsets(&source_pixel, &coords.reference_pixel())
            .into_iter()
            .zip(section.stride)
            .map(|(offset, &step)| offset / step as f64)
            .collect::<Vec<_>>();
        messages.push(format!("Absolute pixel = {selected_pixel:?}"));
        messages.push(format!("Relative pixel = {relative:?}"));
    }
    let mut components = ComponentList::new();
    components.push(component);
    Ok(ImageMaxFit {
        record: components_record(components)?,
        messages,
    })
}

fn brightest_pixel(
    image: &dyn ImageInterface<f32>,
    selection: &SubimageSelection,
    section: &StridedSection<'_>,
    negative: bool,
) -> Result<Vec<usize>, ImageError> {
    let reader = PixelSections {
        image,
        selection,
        mask: None,
    };
    let mut maximum = f32::NEG_INFINITY;
    let mut position = None::<Vec<usize>>;
    for block in section.cursors(bounded_read_cursor(section.shape.to_vec())?) {
        let block = block?;
        let start = section.source_start(&block.position);
        if !selection.overlaps_window(&start, &section.dense_shape(&block.shape)) {
            continue;
        }
        let (values, mask) = reader.read(&start, &block.shape, section.stride)?;
        for (index, &value) in values.indexed_iter() {
            let magnitude = if negative { value.abs() } else { value };
            if !magnitude.is_finite()
                || magnitude < maximum
                || mask.as_ref().is_some_and(|mask| !mask[&index])
            {
                continue;
            }
            let candidate = block
                .position
                .iter()
                .zip(index.slice())
                .map(|(&base, &offset)| base + offset);
            if magnitude > maximum
                || position.as_ref().is_some_and(|previous| {
                    candidate.clone().rev().lt(previous.iter().rev().copied())
                })
            {
                maximum = magnitude;
                position = Some(candidate.collect());
            }
        }
    }
    position.ok_or_else(|| invalid("The selected region has no valid pixels"))
}

fn offsets(coordinate: &[f64], reference: &[f64]) -> Vec<f64> {
    coordinate
        .iter()
        .zip(reference)
        .map(|(&value, &reference)| value - reference)
        .collect()
}
