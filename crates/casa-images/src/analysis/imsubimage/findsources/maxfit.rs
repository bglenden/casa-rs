// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_components::ComponentList;
use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_core::{RecordValue, quanta::Unit};
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
    let coords = image.coordinates();
    if coords.find_coordinate(CoordinateType::Direction).is_none() {
        return Err(invalid("The image must contain a direction coordinate"));
    }
    let direction_axes = direction_plane_axes(image)?;
    let selection = SubimageSelection::resolve_section(image, None, options.region, None, None)?;
    if selection.has_multiple_regions {
        return Err(invalid(
            "Only a single n-dimensional rectangular region is supported",
        ));
    }
    let requested = SubimagePlan::from_selection(image, None, selection, true, &[], None)?;
    if requested.image_info.beam_set.has_multi_beam() {
        return Err(invalid(
            "This application does not support images with multiple beams",
        ));
    }
    let mut selection = requested.selection;
    let packed = selection.packed_shape();
    let mut sky_axes = direction_axes;
    sky_axes.sort_unstable();
    let [width, height] = sky_axes.map(|axis| packed[axis]);
    if width <= 1 || height <= 1 {
        return Err(invalid(
            "Source finding requires both nondegenerate sky axes",
        ));
    }
    if !peaks::admits_shape(width, height)? {
        return Err(invalid("No sources were found in the selected sky plane"));
    }
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
    let ranking = PeakRanking::Maximum {
        signed: options.negative,
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
        let pixel = requested
            .kept_axes
            .iter()
            .map(|&axis| selected_pixel[axis])
            .collect::<Vec<_>>();
        let coords = &requested.coordinates;
        let values = component.flux().values().map(|value| value.re);
        messages.push(format!(
            "Brightness     = {values:?} {}",
            component.flux().unit().name()
        ));
        let mut units = coords.world_axis_units();
        for (axis, unit) in units.iter_mut().enumerate() {
            if let Some((model, local)) = coords.world_axis_owner(axis)
                && let CoordinateModel::Direction(direction) = coords.coordinate(model)
            {
                *unit = direction.visible_world_units()[local].clone();
            }
        }
        messages.push(format!("World Axis Units: {units:?}"));
        if let Ok(world) = coords.to_world(&pixel) {
            let displayed = displayed_world(coords, &units, &world);
            messages.push(format!("Absolute world = {displayed:?}"));
            let mut relative = world;
            if coords.convert_world_offsets(&mut relative, false).is_ok() {
                let displayed = displayed_world(coords, &units, &relative);
                messages.push(format!("Relative world = {displayed:?}"));
            }
        }
        let relative = offsets(&pixel, &coords.reference_pixel());
        messages.push(format!("Absolute pixel = {pixel:?}"));
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
    let magnitude = |value: f32| if negative { value.abs() } else { value };
    let mut local = vec![0; section.shape.len()];
    for block in section.cursors(bounded_read_cursor(section.shape.to_vec())?) {
        let block = block?;
        let start = section.source_start(&block.position);
        if !selection.overlaps_window(&start, &section.dense_shape(&block.shape)) {
            continue;
        }
        let (values, mask) = reader.read(&start, &block.shape, section.stride)?;
        let mut keep = |magnitude: f32, local: &[usize]| {
            let candidate = block
                .position
                .iter()
                .zip(local)
                .map(|(&base, &offset)| base + offset);
            if magnitude > maximum
                || position.as_ref().is_some_and(|previous| {
                    magnitude == maximum
                        && candidate.clone().rev().lt(previous.iter().rev().copied())
                })
            {
                maximum = magnitude;
                position = Some(candidate.collect());
            }
        };
        // Native tiles and FITS sections commonly have axis zero contiguous.
        // In that order the first tied pixel already has the lowest subscript.
        let contiguous = values.view().reversed_axes();
        let contiguous_mask = mask.as_ref().map(|mask| mask.view().reversed_axes());
        if let Some(pixels) = contiguous.as_slice()
            && contiguous_mask
                .as_ref()
                .is_none_or(|mask| mask.is_standard_layout())
        {
            let valid = contiguous_mask.as_ref().and_then(|mask| mask.as_slice());
            let mut best = f32::NEG_INFINITY;
            let mut offset = 0;
            for (index, &pixel) in pixels.iter().enumerate() {
                let value = magnitude(pixel);
                if value > best && value.is_finite() && valid.is_none_or(|mask| mask[index]) {
                    best = value;
                    offset = index;
                }
            }
            if best.is_finite() {
                for (coordinate, &length) in local.iter_mut().zip(&block.shape) {
                    *coordinate = offset % length;
                    offset /= length;
                }
                keep(best, &local);
            }
            continue;
        }
        for (index, &value) in values.indexed_iter() {
            let value = magnitude(value);
            if value.is_finite() && mask.as_ref().is_none_or(|mask| mask[&index]) {
                keep(value, index.slice());
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

fn displayed_world(coordinates: &CoordinateSystem, units: &[String], world: &[f64]) -> Vec<f64> {
    world
        .iter()
        .zip(units)
        .enumerate()
        .map(|(axis, (&value, unit))| {
            if let Some((model, _)) = coordinates.world_axis_owner(axis)
                && matches!(coordinates.coordinate(model), CoordinateModel::Direction(_))
                && let Ok(unit) = Unit::new(unit)
            {
                value / unit.val().factor
            } else {
                value
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{any::Any, path::Path};

    use casa_coordinates::{DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate};
    use casa_core::{
        ArrayD,
        measures::{direction::DirectionRef, frequency::FrequencyRef},
    };
    use casa_lattices::{Lattice, LatticeError};

    use super::*;
    use crate::ImageInfo;

    struct ThinImage(CoordinateSystem);

    impl Lattice<f32> for ThinImage {
        fn shape(&self) -> &[usize] {
            &[4096, 4, 20_000]
        }

        fn get_at(&self, _: &[usize]) -> Result<f32, LatticeError> {
            panic!("a geometrically impossible fit should never read pixels")
        }

        fn get_slice(
            &self,
            _: &[usize],
            _: &[usize],
            _: &[usize],
        ) -> Result<ArrayD<f32>, LatticeError> {
            panic!("a geometrically impossible fit should never read pixels")
        }
    }

    impl ImageInterface<f32> for ThinImage {
        fn as_any(&self) -> Option<&dyn Any> {
            None
        }
        fn coordinates(&self) -> &CoordinateSystem {
            &self.0
        }
        fn units(&self) -> &str {
            ""
        }
        fn misc_info(&self) -> RecordValue {
            RecordValue::default()
        }
        fn image_info(&self) -> Result<ImageInfo, ImageError> {
            Ok(ImageInfo::default())
        }
        fn name(&self) -> Option<&Path> {
            None
        }
    }

    #[test]
    fn thin_sky_plane_is_refused_before_any_pixel_reads() {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::TAN),
            [0.; 2],
            [-1e-5, 1e-5],
            [0.; 2],
        ));
        coords.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.4e9,
            1e3,
            0.,
            1.4e9,
        ));
        let record = coords.to_record();
        let result = fit_image_maximum(
            &ThinImage(coords),
            record,
            MaxFitOptions {
                region: None,
                point: true,
                width: 5,
                negative: false,
                list: false,
            },
        );
        assert!(
            matches!(result, Err(ImageError::CasaSelection(message)) if message.contains("No sources were found"))
        );
    }
}
