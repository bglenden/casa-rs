// SPDX-License-Identifier: LGPL-3.0-or-later

mod median;
mod statistics;

use std::path::Path;

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_lattices::{TraversalCursorIter, TraversalSpec};
use casa_types::{ArrayD, RecordValue};
use ndarray::{Axis, Dimension, IxDyn, Zip};

use crate::{ImageError, ImageInterface, MutableImageInterface};

use super::super::axes::{StridedSection, direction_plane_axes};
use super::super::lel_mask::PreparedMask;
use super::super::statistics_flux_scale;
use super::decimate::{DecimationMethod, apply_decimation};
use super::read_window::PixelSections;
use super::region::{SubimageSelection, implicit_positional_axes};
use super::{SubimagePlan, SubimageRegion};
use median::{reduce, reduce_large};
use statistics::Summary;

pub use statistics::{CollapseFunction, CollapsePixel};

const MAX_TILE_SAMPLES: usize = 256 * 1024;

/// Parameters for a CASA image collapse over specified pixel axes.
pub struct ImageCollapseOptions<'a> {
    /// Statistic computed for each output cell.
    pub function: CollapseFunction,
    /// Axes to reduce to length one.
    pub axes: &'a [usize],
    /// Optional saved or textual region.
    pub region: Option<SubimageRegion<'a>>,
    /// Rectangular pixel bounds when supplied instead of a region.
    pub spatial: Option<&'a str>,
    /// Spectral channels selected before reducing.
    pub chans: Option<&'a str>,
    /// Stokes planes selected before reducing.
    pub stokes: Option<&'a str>,
    /// Additional lattice validity expression.
    pub mask: Option<&'a str>,
    /// Permit stretch across compatible mask axes.
    pub stretch: bool,
    /// Final destination, if any, to protect named mask inputs.
    pub destination: Option<&'a Path>,
}

/// The selected source and output metadata for an image collapse.
pub struct ImageCollapse<'a> {
    /// Output grid, native coordinate record, and selected beam metadata.
    pub section: SubimagePlan,
    options: ImageCollapseOptions<'a>,
    reduced: Vec<bool>,
    units: String,
    spectral: Option<usize>,
    stokes: Option<usize>,
    beam_pixels: f64,
}

pub(super) struct CollapsedProfile<T> {
    pub(super) values: Vec<T>,
    pub(super) mask: Vec<bool>,
    pub(super) counts: Vec<usize>,
}

struct ReducedTile<T> {
    values: ArrayD<T>,
    mask: ArrayD<bool>,
    counts: Vec<usize>,
}

fn invalid(reason: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(reason.into())
}

impl<'a> ImageCollapse<'a> {
    /// Resolve the selected region and build the retained grid.
    ///
    /// # Errors
    /// The axes, region, units, or coordinate record cannot describe this operation.
    pub fn new<T: CollapsePixel>(
        source: &dyn ImageInterface<T>,
        native: RecordValue,
        options: ImageCollapseOptions<'a>,
        beams: &RecordValue,
    ) -> Result<Self, ImageError> {
        let mut reduced = vec![false; source.ndim()];
        for &axis in options.axes {
            let flag = reduced.get_mut(axis).ok_or_else(|| {
                invalid(format!(
                    "Specified zero-based axis ({axis}) must be less than the number of axes ({})",
                    source.ndim()
                ))
            })?;
            if std::mem::replace(flag, true) {
                return Err(invalid("Collapse axes must be distinct"));
            }
        }
        let mut selection = SubimageSelection::resolve_section(
            source,
            options.spatial,
            options.region,
            options.chans,
            options.stokes,
        )?;
        if options.region.is_none()
            && options.spatial.is_none()
            && let Some(axes) = implicit_positional_axes(source)
        {
            for axis in axes {
                selection.shape[axis] = 1;
            }
        }
        let tabular = source
            .coordinates()
            .find_coordinate(CoordinateType::Tabular)
            .map(|_| native.clone());
        let mut section =
            SubimagePlan::from_selection(source, Some(native), selection, false, &[], Some(beams))?;
        if let Some(tabular) = &tabular {
            apply_decimation(&mut section, Some(tabular), 0, 1, DecimationMethod::Copy)?;
        }
        let units = if options.function == CollapseFunction::Flux {
            flux_units(source, &section, options.axes)?
        } else {
            source.units().to_owned()
        };
        let spectral = source.find_axis(CoordinateType::Spectral);
        let stokes = source.find_axis(CoordinateType::Stokes);
        let beam_pixels = if options.function == CollapseFunction::SqrtSumNpixBeam {
            if let Some(beam) = section.image_info.beam_set.single_beam() {
                statistics_flux_scale(&section.coordinates, "Jy/beam", Some(beam), None)
                    .filter(|&scale| scale > 0.0)
                    .map(|scale| 1.0 / scale)
                    .ok_or_else(|| invalid("Image has no direction coordinate for the beam area"))?
            } else {
                1.0
            }
        } else {
            1.0
        };
        if section.image_info.beam_set.is_multi()
            && [spectral, stokes]
                .into_iter()
                .flatten()
                .any(|axis| reduced[axis] && section.shape[axis] > 1)
        {
            let first = *section.image_info.beam_set.beam(0, 0);
            section.image_info.beam_set.set_all(first);
        }
        collapse_coordinates(&mut section, options.axes)?;
        Ok(Self {
            section,
            options,
            reduced,
            units,
            spectral,
            stokes,
            beam_pixels,
        })
    }

    /// Reduce valid pixels in bounded source sections and write the result.
    ///
    /// # Errors
    /// Output shape or I/O failed, or all selected source pixels are invalid.
    pub fn copy_to<T: CollapsePixel>(
        &self,
        source: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<(), ImageError> {
        self.section.copy_metadata(source, output)?;
        output.set_units_string(self.units.clone())?;
        let mut has_mask = false;
        self.visit_tiles(source, |position, results| {
            output.put_slice(&results.values, position)?;
            if results.mask.iter().any(|&present| !present) {
                if !has_mask {
                    output.create_filled_mask("mask0", true)?;
                    output.set_default_mask("mask0")?;
                    has_mask = true;
                }
                output.put_mask_slice("mask0", &results.mask, position)?;
            }
            Ok(())
        })
    }

    pub(super) fn collect_profile<T: CollapsePixel>(
        &self,
        source: &dyn ImageInterface<T>,
        axis: usize,
    ) -> Result<CollapsedProfile<T>, ImageError> {
        let length = self.section.shape[axis];
        let mut result = CollapsedProfile {
            values: vec![T::default(); length],
            mask: vec![false; length],
            counts: vec![0; length],
        };
        self.visit_tiles(source, |position, tile| {
            let start = position[axis];
            let end = start + tile.counts.len();
            for (slot, &value) in result.values[start..end].iter_mut().zip(&tile.values) {
                *slot = value;
            }
            for (slot, &value) in result.mask[start..end].iter_mut().zip(&tile.mask) {
                *slot = value;
            }
            result.counts[start..end].copy_from_slice(&tile.counts);
            Ok(())
        })?;
        Ok(result)
    }

    pub(super) fn profile_unit(&self) -> &str {
        &self.units
    }

    fn visit_tiles<T: CollapsePixel>(
        &self,
        source: &dyn ImageInterface<T>,
        mut visit: impl FnMut(&[usize], ReducedTile<T>) -> Result<(), ImageError>,
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
        let ones = vec![1; source.ndim()];
        let packed = selection.packed_shape();
        let stride = selection.stride.as_deref().unwrap_or(&ones);
        let groups = Groups {
            reader: &reader,
            section: StridedSection {
                start: &selection.start,
                shape: &packed,
                stride,
            },
            reduced: &self.reduced,
        };
        let group_size = self
            .reduced
            .iter()
            .zip(&packed)
            .filter_map(|(&reduce, &length)| reduce.then_some(length))
            .fold(1usize, usize::saturating_mul);
        let median = self.options.function.uses_median();
        let outer_budget = if median {
            (MAX_TILE_SAMPLES / group_size).max(1)
        } else {
            4096
        };
        let cursor = bounded_cursor(&self.section.shape, stride, outer_budget);
        let mut has_data = false;
        for tile in
            TraversalCursorIter::new(self.section.shape.clone(), cursor, TraversalSpec::tiles())
        {
            let tile = tile?;
            let values = self.reduce_tile(&groups, &tile.position, &tile.shape, group_size)?;
            has_data |= values.counts.iter().any(|&count| count != 0);
            visit(&tile.position, values)?;
        }
        if !has_data {
            return Err(invalid("All selected pixels are masked"));
        }
        Ok(())
    }

    fn reduce_tile<T: CollapsePixel>(
        &self,
        groups: &Groups<'_, T>,
        start: &[usize],
        shape: &[usize],
        group_size: usize,
    ) -> Result<ReducedTile<T>, ImageError> {
        let size = shape.iter().product();
        let function = self.options.function;
        let (values, validity, counts) = if function.uses_median() {
            if group_size > MAX_TILE_SAMPLES {
                debug_assert_eq!(size, 1);
                let (value, count) = reduce_large(function, |visit| {
                    groups.for_each(start, shape, |_, _, value| visit(value))
                })?;
                (
                    vec![value.unwrap_or_default()],
                    vec![value.is_some()],
                    vec![count],
                )
            } else {
                let mut by_group = vec![vec![None; group_size]; size];
                groups.for_each(start, shape, |index, position, sample| {
                    by_group[index][position] = Some(sample);
                    Ok(())
                })?;
                let (values, counts): (Vec<_>, Vec<_>) = by_group
                    .into_iter()
                    .map(|group| {
                        let samples: Vec<_> = group.into_iter().flatten().collect();
                        let count = samples.len();
                        (reduce(function, samples), count)
                    })
                    .unzip();
                let validity = values.iter().map(Option::is_some).collect();
                (
                    values.into_iter().map(Option::unwrap_or_default).collect(),
                    validity,
                    counts,
                )
            }
        } else {
            let mut cells = (0..size).map(|_| Summary::default()).collect::<Vec<_>>();
            let mut retained = self
                .reduced
                .iter()
                .enumerate()
                .filter_map(|(axis, &reduced)| (!reduced).then_some(axis));
            if !matches!(function, CollapseFunction::Min | CollapseFunction::Max)
                && let (Some(axis), None) = (retained.next(), retained.next())
            {
                groups.for_each_plane(start, shape, axis, |index, sample| {
                    cells[index].add(function, sample, 0)
                })?;
            } else {
                groups.for_each(start, shape, |index, position, sample| {
                    cells[index].add(function, sample, position);
                    Ok(())
                })?;
            }
            let counts = cells.iter().map(|cell| cell.count).collect();
            let (values, validity) = cells
                .iter()
                .enumerate()
                .map(|(index, cell)| {
                    let scale = if function == CollapseFunction::Flux {
                        self.flux_scale(groups.reader.image.units(), start, shape, index)?
                    } else {
                        1.0
                    };
                    Ok(cell.finish(function, scale, self.beam_pixels))
                })
                .collect::<Result<Vec<_>, ImageError>>()?
                .into_iter()
                .unzip();
            (values, validity, counts)
        };
        Ok(ReducedTile {
            values: ArrayD::from_shape_vec(IxDyn(shape), values)
                .expect("one result per output cell"),
            mask: ArrayD::from_shape_vec(IxDyn(shape), validity)
                .expect("one validity per output cell"),
            counts,
        })
    }

    fn flux_scale(
        &self,
        brightness: &str,
        start: &[usize],
        shape: &[usize],
        index: usize,
    ) -> Result<f64, ImageError> {
        let planes = |axis: Option<usize>| {
            axis.map_or(0, |axis| {
                let stride = shape[axis + 1..].iter().product::<usize>();
                start[axis] + (index / stride) % shape[axis]
            })
        };
        let beams = &self.section.image_info.beam_set;
        let beam =
            (!beams.is_empty()).then(|| *beams.beam(planes(self.spectral), planes(self.stokes)));
        statistics_flux_scale(&self.section.coordinates, brightness, beam, None)
            .ok_or_else(|| invalid("Unable to calculate flux density"))
    }
}

fn flux_units<T: CollapsePixel>(
    source: &dyn ImageInterface<T>,
    section: &SubimagePlan,
    axes: &[usize],
) -> Result<String, ImageError> {
    if source.find_axis(CoordinateType::Direction).is_none() {
        return Err(invalid(
            "Image has no direction coordinate. Cannot do flux density calculation.",
        ));
    }
    let brightness = source.units();
    if brightness.contains("beam") && section.image_info.beam_set.is_empty() {
        return Err(invalid(
            "Image has no beam. Cannot do flux density calculation.",
        ));
    }
    let direction = direction_plane_axes(source)?;
    for &axis in axes {
        if !direction.contains(&axis) && section.shape[axis] > 1 {
            return Err(invalid(format!(
                "Specified axis {axis} is not a direction axis but has length > 1. Cannot do flux density calculation."
            )));
        }
    }
    if brightness.contains('K') {
        Ok(format!("{brightness}.arcsec2"))
    } else if let Some((prefix, suffix)) = brightness.split_once("/beam") {
        Ok(format!("{prefix}{suffix}"))
    } else {
        Err(invalid("Unable to calculate flux density"))
    }
}

fn collapse_coordinates(section: &mut SubimagePlan, axes: &[usize]) -> Result<(), ImageError> {
    let coordinates = &section.coordinates;
    let record = section
        .coordinate_record
        .take()
        .unwrap_or_else(|| coordinates.to_record());
    let record = collapsed_coordinate_record(coordinates, &section.shape, record, axes)?;
    for &axis in axes {
        section.shape[axis] = 1;
    }
    section.coordinates = CoordinateSystem::from_record(&record)?;
    section.coordinate_record = Some(record);
    Ok(())
}

pub(super) fn collapsed_coordinate_record(
    coordinates: &CoordinateSystem,
    shape: &[usize],
    mut record: RecordValue,
    axes: &[usize],
) -> Result<RecordValue, ImageError> {
    let first = coordinates.to_world(&vec![0.0; shape.len()])?;
    let end = shape
        .iter()
        .map(|&length| (length - 1) as f64)
        .collect::<Vec<_>>();
    let last = coordinates.to_world(&end)?;
    let mut interior = None;
    for &axis in axes {
        let world = coordinates
            .pixel_axis_to_world_axis(axis)
            .ok_or_else(|| invalid("Collapsed pixel axis has no corresponding world axis"))?;
        let (index, local) = coordinates
            .pixel_axis_owner(axis)
            .ok_or_else(|| invalid("Collapsed pixel axis has no coordinate"))?;
        let model = coordinates.coordinate(index);
        let average = if let CoordinateModel::Direction(direction) = model {
            let midpoint = if direction.longitude_axis() == Some(local) {
                let circle = std::f64::consts::TAU;
                let half = std::f64::consts::PI;
                if interior.is_none() {
                    let center = end.iter().map(|&length| length * 0.5).collect::<Vec<_>>();
                    interior = Some(coordinates.to_world(&center)?);
                }
                let center = interior.as_ref().expect("interior longitude")[world];
                let offset = |longitude: f64| (longitude - center + half).rem_euclid(circle) - half;
                (center + 0.5 * (offset(first[world]) + offset(last[world]))).rem_euclid(circle)
            } else {
                0.5 * (first[world] + last[world])
            };
            let stored = direction.reference_and_increment_in_world_units().1[local];
            midpoint * (stored / model.increment()[local])
        } else {
            0.5 * (first[world] + last[world])
        };
        CoordinateSystem::set_reference_value_in_record(&mut record, axis, average)?;
        CoordinateSystem::set_reference_pixel_in_record(&mut record, axis, 0.0)?;
    }
    Ok(record)
}

fn bounded_cursor(shape: &[usize], stride: &[usize], mut budget: usize) -> Vec<usize> {
    shape
        .iter()
        .zip(stride)
        .map(|(&length, &step)| {
            let width = budget.div_ceil(step).min(length);
            budget = (budget / ((width - 1) * step + 1)).max(1);
            width
        })
        .collect()
}

struct Groups<'a, T: CollapsePixel> {
    reader: &'a PixelSections<'a, T>,
    section: StridedSection<'a>,
    reduced: &'a [bool],
}

impl<T: CollapsePixel> Groups<'_, T> {
    fn for_each(
        &self,
        start: &[usize],
        shape: &[usize],
        mut visit: impl FnMut(usize, usize, T) -> Result<(), ImageError>,
    ) -> Result<(), ImageError> {
        let mut group_step = 1;
        let group_weights: Vec<_> = shape
            .iter()
            .rev()
            .map(|&length| {
                let weight = group_step;
                group_step *= length;
                weight
            })
            .collect();
        let mut collapse_step = 1;
        let collapse_weights: Vec<_> = self
            .reduced
            .iter()
            .zip(self.section.shape)
            .map(|(&reduce, &length)| {
                if !reduce {
                    return 0;
                }
                let weight = collapse_step;
                collapse_step *= length;
                weight
            })
            .collect();
        self.visit_windows(start, shape, |tile_position, pixels, valid| {
            let pixels = pixels.reversed_axes();
            let valid = valid.map(ArrayD::reversed_axes);
            for (index, &pixel) in pixels.indexed_iter() {
                if valid.as_ref().is_some_and(|valid| !valid[&index]) {
                    continue;
                }
                let mut group = 0;
                let mut position = 0;
                for (axis, (&local, &tile_start)) in
                    index.slice().iter().rev().zip(tile_position).enumerate()
                {
                    if self.reduced[axis] {
                        position += (tile_start + local) * collapse_weights[axis];
                    } else {
                        group += local * group_weights[shape.len() - 1 - axis];
                    }
                }
                visit(group, position, pixel)?;
            }
            Ok(())
        })
    }

    fn for_each_plane(
        &self,
        start: &[usize],
        shape: &[usize],
        retained_axis: usize,
        mut visit: impl FnMut(usize, T),
    ) -> Result<(), ImageError> {
        self.visit_windows(start, shape, |tile_position, values, mask| {
            let axis = Axis(retained_axis);
            let offset = tile_position[retained_axis];
            for (index, plane) in values.axis_iter(axis).enumerate() {
                let group = offset + index;
                if let Some(mask) = &mask {
                    Zip::from(plane).and(mask.index_axis(axis, index)).for_each(
                        |&value, &valid| {
                            if valid {
                                visit(group, value);
                            }
                        },
                    );
                } else {
                    for &value in &plane {
                        visit(group, value);
                    }
                }
            }
            Ok(())
        })
    }

    fn visit_windows(
        &self,
        start: &[usize],
        shape: &[usize],
        mut visit: impl FnMut(&[usize], ArrayD<T>, Option<ArrayD<bool>>) -> Result<(), ImageError>,
    ) -> Result<(), ImageError> {
        let full = self.section.shape;
        let stride = self.section.stride;
        let request: Vec<_> = self
            .reduced
            .iter()
            .enumerate()
            .map(
                |(axis, &reduce)| {
                    if reduce { full[axis] } else { shape[axis] }
                },
            )
            .collect();
        let dense_outer: usize = shape
            .iter()
            .zip(stride)
            .map(|(&length, &step)| (length - 1) * step + 1)
            .product();
        let mut budget = MAX_TILE_SAMPLES / dense_outer;
        let cursor = self
            .reduced
            .iter()
            .enumerate()
            .map(|(axis, &reduce)| {
                if reduce {
                    let width = budget.div_ceil(stride[axis]).min(full[axis]);
                    budget = if width < full[axis] {
                        1
                    } else {
                        (budget / ((width - 1) * stride[axis] + 1)).max(1)
                    };
                    width
                } else {
                    shape[axis]
                }
            })
            .collect();
        for tile in TraversalCursorIter::new(request, cursor, TraversalSpec::tiles()) {
            let tile = tile?;
            let packed: Vec<_> = tile
                .position
                .iter()
                .zip(start)
                .map(|(&offset, &origin)| offset + origin)
                .collect();
            let source = self.section.source_start(&packed);
            if !self
                .reader
                .selection
                .overlaps_window(&source, &self.section.dense_shape(&tile.shape))
            {
                continue;
            }
            let (pixels, valid) = self.reader.read(&source, &tile.shape, stride)?;
            visit(&tile.position, pixels, valid)?;
        }
        Ok(())
    }
}
