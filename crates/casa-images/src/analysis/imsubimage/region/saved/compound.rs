// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compile compound world regions into crop bounds and pixel membership.

use casa_coordinates::CoordinateSystem;
use casa_types::RecordValue;
use ndarray::{Array2, ArrayD, Dimension, IxDyn};

use crate::ImageError;
use crate::saved_region::{compound_children, native_region_kind};

use super::super::{ParsedRegion, SavedRegionMask, resolve_spatial_and_saved_bounds};
use super::parse_saved_region;

#[derive(Clone, Copy)]
enum Operation {
    Union,
    Intersection,
    Difference,
    Complement,
}

struct Region {
    start: Vec<usize>,
    end: Vec<usize>,
    selection: Selection,
}

enum Selection {
    Primitive {
        spatial: Option<Array2<bool>>,
        polygon: Option<ArrayD<bool>>,
    },
    Compound(Operation, Vec<Region>),
}

pub(super) fn decode(
    name: &str,
    record: &RecordValue,
    coords: &CoordinateSystem,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<ParsedRegion, ImageError> {
    let region = compile(name, record, coords, full_shape, plane)?;
    let shape = region
        .start
        .iter()
        .zip(&region.end)
        .map(|(&start, &end)| end - start)
        .collect::<Vec<_>>();
    let mut pixel = region.start.clone();
    let mask = ArrayD::from_shape_fn(IxDyn(&shape), |local| {
        for ((global, &start), &index) in pixel.iter_mut().zip(&region.start).zip(local.slice()) {
            *global = start + index;
        }
        region.contains(&pixel, plane)
    });
    Ok(ParsedRegion {
        is_compound: true,
        shapes: Vec::new(),
        saved_bounds: Some((region.start, shape)),
        saved_mask: Some(SavedRegionMask::Cropped(mask)),
    })
}

fn compile(
    name: &str,
    record: &RecordValue,
    coords: &CoordinateSystem,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<Region, ImageError> {
    let class = native_region_kind(name, record)?;
    let operation = match class {
        "WCUnion" => Operation::Union,
        "WCIntersection" => Operation::Intersection,
        "WCDifference" => Operation::Difference,
        "WCComplement" => Operation::Complement,
        "WCBox" | "WCPolygon" | "WCEllipsoid" => {
            let leaf = parse_saved_region(name, record, coords, full_shape, plane)?;
            let shapes = leaf
                .shapes
                .iter()
                .map(|shape| &shape.spatial)
                .collect::<Vec<_>>();
            let resolved = resolve_spatial_and_saved_bounds(
                full_shape,
                full_shape.len(),
                plane,
                &shapes,
                leaf.saved_bounds.as_ref(),
                leaf.saved_mask.is_some(),
            )?;
            let polygon = leaf.saved_mask.map(|mask| match mask {
                SavedRegionMask::Whole(pixels) => pixels,
                SavedRegionMask::Cropped(_) => unreachable!("only compounds crop their mask"),
            });
            let end = resolved
                .start
                .iter()
                .zip(&resolved.shape)
                .map(|(start, size)| start + size)
                .collect();
            return Ok(Region {
                start: resolved.start,
                end,
                selection: Selection::Primitive {
                    spatial: resolved.spatial_mask,
                    polygon,
                },
            });
        }
        _ => {
            return Err(ImageError::InvalidMetadata(format!(
                "saved region '{name}' contains unsupported compound member {class:?}"
            )));
        }
    };
    let members = compound_children(name, record)?;
    let valid_count = match operation {
        Operation::Difference => members.len() == 2,
        Operation::Complement => members.len() == 1,
        Operation::Union | Operation::Intersection => !members.is_empty(),
    };
    if !valid_count {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{name}' has an invalid number of compound members"
        )));
    }
    let children = members
        .into_iter()
        .map(|child| compile(name, child, coords, full_shape, plane))
        .collect::<Result<Vec<_>, _>>()?;
    compound_region(name, operation, children, full_shape)
}

fn compound_region(
    name: &str,
    operation: Operation,
    children: Vec<Region>,
    full_shape: &[usize],
) -> Result<Region, ImageError> {
    // casacore's LC compounds define their bounds independently of the
    // result mask. Difference can remove an entire edge of its first child.
    let mut start = children[0].start.clone();
    let mut end = children[0].end.clone();
    match operation {
        Operation::Complement => {
            start.fill(0);
            end.copy_from_slice(full_shape);
        }
        Operation::Difference => {}
        Operation::Union | Operation::Intersection => {
            for child in &children[1..] {
                for axis in 0..start.len() {
                    if matches!(operation, Operation::Union) {
                        start[axis] = start[axis].min(child.start[axis]);
                        end[axis] = end[axis].max(child.end[axis]);
                    } else {
                        start[axis] = start[axis].max(child.start[axis]);
                        end[axis] = end[axis].min(child.end[axis]);
                    }
                }
            }
            if start.iter().zip(&end).any(|(start, end)| start >= end) {
                return Err(ImageError::CasaSelection(format!(
                    "saved region '{name}': regions do not overlap"
                )));
            }
        }
    }
    Ok(Region {
        start,
        end,
        selection: Selection::Compound(operation, children),
    })
}

impl Region {
    fn contains(&self, pixel: &[usize], [x, y]: [usize; 2]) -> bool {
        if pixel
            .iter()
            .zip(self.start.iter().zip(&self.end))
            .any(|(&index, (&start, &end))| index < start || index >= end)
        {
            return false;
        }
        let plane = [x, y];
        match &self.selection {
            Selection::Primitive { spatial, polygon } => {
                spatial
                    .as_ref()
                    .is_none_or(|mask| mask[[pixel[x] - self.start[x], pixel[y] - self.start[y]]])
                    && polygon.as_ref().is_none_or(|mask| mask[IxDyn(pixel)])
            }
            Selection::Compound(operation, children) => match operation {
                Operation::Union => children.iter().any(|child| child.contains(pixel, plane)),
                Operation::Intersection => {
                    children.iter().all(|child| child.contains(pixel, plane))
                }
                Operation::Difference => {
                    children[0].contains(pixel, plane) && !children[1].contains(pixel, plane)
                }
                Operation::Complement => !children[0].contains(pixel, plane),
            },
        }
    }
}
