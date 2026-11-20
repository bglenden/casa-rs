// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compile compound world regions into crop bounds and pixel membership.

use casa_coordinates::CoordinateSystem;
use casa_types::{RecordValue, Value};
use ndarray::{Array2, ArrayD, Dimension, IxDyn};

use crate::saved_region::{compound_children, native_region_kind};
use crate::{ImageError, PolygonRegion};

use super::super::{ParsedRegion, SavedRegionMask, resolve_spatial_and_saved_bounds};
use super::{decode_wc_box, decode_wc_box_axes, parse_saved_region, wc_pixel_axes};

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
    axes: Vec<usize>,
    selection: Selection,
}

enum Selection {
    Primitive {
        spatial: Option<([usize; 2], Array2<bool>)>,
    },
    Compound(Operation, Vec<Region>),
    Extension {
        child: Box<Region>,
        stretch: Vec<(usize, usize)>,
    },
    Concatenation {
        axis: usize,
        children: Vec<Region>,
    },
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
        region.contains(&mut pixel, plane)
    });
    Ok(ParsedRegion {
        is_compound: true,
        shapes: Vec::new(),
        saved_bounds: Some((region.start, shape)),
        saved_mask: Some(SavedRegionMask::Cropped(mask)),
        stride: None,
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
        "WCExtension" => return compile_extension(name, record, coords, full_shape, plane),
        "WCConcatenation" => return compile_concatenation(name, record, coords, full_shape, plane),
        "WCPolygon" => {
            let polygon = PolygonRegion::from_record(name, record, coords)?;
            let [x, y] = polygon.pixel_axes;
            let local = polygon.lattice_plane(coords, full_shape)?.ok_or_else(|| {
                ImageError::CasaSelection(format!(
                    "saved region '{name}' selects no pixels in image"
                ))
            })?;
            let mut start = vec![0; full_shape.len()];
            start[x] = local.start[0];
            start[y] = local.start[1];
            let mut end = full_shape.to_vec();
            end[x] = start[x] + local.mask.nrows();
            end[y] = start[y] + local.mask.ncols();
            let mut axes = vec![x, y];
            axes.sort_unstable();
            return Ok(Region {
                start,
                end,
                axes,
                selection: Selection::Primitive {
                    spatial: Some(([x, y], local.mask)),
                },
            });
        }
        "WCBox" | "WCEllipsoid" => {
            let (mut axes, _) = wc_pixel_axes(name, class, record, coords)?;
            axes.sort_unstable();
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
            let end = resolved
                .start
                .iter()
                .zip(&resolved.shape)
                .map(|(start, size)| start + size)
                .collect();
            return Ok(Region {
                start: resolved.start,
                end,
                axes,
                selection: Selection::Primitive {
                    spatial: resolved.spatial_mask.map(|pixels| (plane, pixels)),
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
    let (start, end) = match operation {
        Operation::Complement => (vec![0; full_shape.len()], full_shape.to_vec()),
        Operation::Difference => (children[0].start.clone(), children[0].end.clone()),
        Operation::Union | Operation::Intersection => {
            let (start, end) =
                combined_bounds(&children, matches!(operation, Operation::Intersection));
            if start.iter().zip(&end).any(|(start, end)| start >= end) {
                return Err(ImageError::CasaSelection(format!(
                    "saved region '{name}': regions do not overlap"
                )));
            }
            (start, end)
        }
    };
    let axes = (0..full_shape.len())
        .filter(|axis| children.iter().any(|child| child.axes.contains(axis)))
        .collect();
    Ok(Region {
        start,
        end,
        axes,
        selection: Selection::Compound(operation, children),
    })
}

fn combined_bounds(children: &[Region], intersection: bool) -> (Vec<usize>, Vec<usize>) {
    let mut start = children[0].start.clone();
    let mut end = children[0].end.clone();
    for child in &children[1..] {
        for axis in 0..start.len() {
            if intersection {
                start[axis] = start[axis].max(child.start[axis]);
                end[axis] = end[axis].min(child.end[axis]);
            } else {
                start[axis] = start[axis].min(child.start[axis]);
                end[axis] = end[axis].max(child.end[axis]);
            }
        }
    }
    (start, end)
}

fn compile_extension(
    name: &str,
    record: &RecordValue,
    coords: &CoordinateSystem,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<Region, ImageError> {
    let members = compound_children(name, record)?;
    let invalid = || {
        ImageError::InvalidMetadata(format!(
            "saved region '{name}': WCExtension requires a region and an extension WCBox"
        ))
    };
    let [original, extent] = members.as_slice() else {
        return Err(invalid());
    };
    if native_region_kind(name, extent)? != "WCBox" {
        return Err(invalid());
    }
    let child = compile(name, original, coords, full_shape, plane)?;
    let (box_axes, _) = wc_pixel_axes(name, "WCBox", extent, coords)?;
    let mut stretch = Vec::new();
    let mut start = child.start.clone();
    let mut end = child.end.clone();
    let belongs_to_child = |axis| child.axes.contains(&axis);
    // WCExtension splits the WCBox before converting world bounds. Its
    // shared axes stretch a one-pixel plane and the other axes extend it.
    for shared in [true, false] {
        let selected = |axis| belongs_to_child(axis) == shared;
        if !box_axes.iter().copied().any(selected) {
            continue;
        }
        let (low, high) = decode_wc_box_axes(name, extent, coords, full_shape, selected)?;
        for axis in box_axes.iter().copied().filter(|&axis| selected(axis)) {
            if shared {
                if end[axis] - start[axis] != 1 {
                    return Err(ImageError::CasaSelection(format!(
                        "saved region '{name}': a stretch axis does not have length 1"
                    )));
                }
                stretch.push((axis, start[axis]));
            }
            start[axis] = low[axis];
            end[axis] = high[axis] + 1;
        }
    }
    let mut axes = child.axes.clone();
    axes.extend(box_axes);
    axes.sort_unstable();
    axes.dedup();
    Ok(Region {
        start,
        end,
        axes,
        selection: Selection::Extension {
            child: Box::new(child),
            stretch,
        },
    })
}

fn compile_concatenation(
    name: &str,
    record: &RecordValue,
    coords: &CoordinateSystem,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<Region, ImageError> {
    let invalid = || {
        ImageError::InvalidMetadata(format!(
            "saved region '{name}': WCConcatenation requires a one-dimensional extendBox"
        ))
    };
    let Some(Value::Record(extent)) = record.get("box") else {
        return Err(invalid());
    };
    if native_region_kind(name, extent)? != "WCBox" {
        return Err(invalid());
    }
    let (box_axes, _) = wc_pixel_axes(name, "WCBox", extent, coords)?;
    let [axis] = box_axes.as_slice() else {
        return Err(invalid());
    };
    let axis = *axis;
    let (low, high) = decode_wc_box(name, extent, coords, full_shape)?;
    let members = compound_children(name, record)?;
    if members.len() != high[axis] - low[axis] + 1 {
        return Err(ImageError::CasaSelection(format!(
            "saved region '{name}': number of regions has to match the range specified in extendBox"
        )));
    }
    let children = members
        .into_iter()
        .map(|child| compile(name, child, coords, full_shape, plane))
        .collect::<Result<Vec<_>, _>>()?;
    let mut axes = children[0].axes.clone();
    if axes.contains(&axis) || children.iter().any(|child| child.axes != axes) {
        return Err(ImageError::CasaSelection(format!(
            "saved region '{name}': concatenated members must share axes distinct from the extendBox"
        )));
    }
    let (mut start, mut end) = combined_bounds(&children, false);
    start[axis] = low[axis];
    end[axis] = high[axis] + 1;
    axes.push(axis);
    axes.sort_unstable();
    Ok(Region {
        start,
        end,
        axes,
        selection: Selection::Concatenation { axis, children },
    })
}

impl Region {
    fn contains(&self, pixel: &mut [usize], [x, y]: [usize; 2]) -> bool {
        if pixel
            .iter()
            .zip(self.start.iter().zip(&self.end))
            .any(|(&index, (&start, &end))| index < start || index >= end)
        {
            return false;
        }
        let plane = [x, y];
        match &self.selection {
            Selection::Primitive { spatial } => spatial.as_ref().is_none_or(|([x, y], mask)| {
                mask[[pixel[*x] - self.start[*x], pixel[*y] - self.start[*y]]]
            }),
            Selection::Extension { child, stretch } => {
                contains_stretched(child, stretch, pixel, plane)
            }
            Selection::Concatenation { axis, children } => {
                let plane_index = pixel[*axis] - self.start[*axis];
                children[plane_index].contains(pixel, plane)
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

fn contains_stretched(
    child: &Region,
    stretch: &[(usize, usize)],
    pixel: &mut [usize],
    plane: [usize; 2],
) -> bool {
    let Some((&(axis, source), rest)) = stretch.split_first() else {
        return child.contains(pixel, plane);
    };
    let saved = std::mem::replace(&mut pixel[axis], source);
    let included = contains_stretched(child, rest, pixel, plane);
    pixel[axis] = saved;
    included
}
