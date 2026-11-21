// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_lattices::{ScratchSpace, TempStoragePolicy, TraversalCursorIter, TraversalSpec};
use casa_types::{ArrayD, RecordValue, Value};
use ndarray::{IxDyn, ShapeBuilder};

use crate::analysis::axes::StridedSection;
use crate::analysis::imsubimage::SubimageRegion;
use crate::analysis::imsubimage::coords::subset_beam_section;
use crate::analysis::imsubimage::read_window::PixelSections;
use crate::analysis::lel_mask::PreparedMask;
use crate::{ImageError, ImageInfo, ImageInterface, ImpvRequest, TempImage};

use super::{
    Plane, RegridMethod, direction, direction_plane_axes, geometry, output_coordinates, position,
    resolve_selection,
};

/// A position-velocity image and its CASA axis-map record.
pub struct ImagePvProduct {
    /// Masked Float image with offset and surviving non-direction axes.
    pub image: TempImage<f32>,
    /// Native axis maps for both temporary attachments and on-disk products.
    pub coordinate_record: RecordValue,
}

/// Create a position-velocity image from an attached Float image.
///
/// # Errors
/// Invalid endpoints, unit, region, width, mask, or failed image I/O.
pub fn generate_image_pv(
    image: &dyn ImageInterface<f32>,
    native: RecordValue,
    beam_record: &RecordValue,
    request: &ImpvRequest,
    region: Option<SubimageRegion<'_>>,
) -> Result<ImagePvProduct, ImageError> {
    let plane = direction_plane_axes(image)?;
    let direction = direction(image.coordinates())?;
    let width = position::width(&request.width, direction)?;
    let endpoints = position::endpoints(request, direction)?;
    let strip = geometry::Strip::new(
        direction,
        endpoints,
        plane.map(|axis| image.shape()[axis]),
        width,
    )?;
    let selection = resolve_selection(image, request, plane, region, true)?;
    if selection
        .stride
        .as_ref()
        .is_some_and(|stride| plane.iter().any(|&axis| stride[axis] != 1))
    {
        return Err(ImageError::CasaSelection(
            "You are not permitted to make a region selection in the direction coordinate".into(),
        ));
    }
    let (coordinates, coordinate_record, secondary) = output_coordinates(
        image,
        Some(native),
        &selection,
        plane,
        &strip,
        &request.unit,
    )?;
    let mut info = image.image_info()?;
    let packed = selection.packed_shape();
    info.beam_set = subset_beam_section(
        &ImageInfo::from_record(beam_record)?.beam_set,
        image.coordinates(),
        image.ndim(),
        &selection.start,
        &packed,
        selection.stride.as_deref(),
    )?;
    let mut misc = image.misc_info();
    misc.upsert("secondary_coordinates", Value::Record(secondary));
    let mut offsets = (0..strip.shape[0])
        .map(|step| {
            (0..strip.shape[1])
                .map(|across| strip.input_pixel(step, across))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let [x, y] = plane;
    let mut shape = packed.clone();
    shape[x] = strip.shape[0];
    shape.remove(y);
    let mut result = TempImage::<f32>::new(
        shape,
        coordinates,
        TempStoragePolicy::Auto {
            memory_budget_bytes: 64 * 1024 * 1024,
            scratch: ScratchSpace::SystemTemp,
        },
    )?;
    result.set_units(image.units())?;
    result.set_image_info(&info)?;
    result.set_misc_info(misc)?;

    let ones = vec![1; packed.len()];
    let section = StridedSection {
        start: &selection.start,
        shape: &packed,
        stride: selection.stride.as_deref().unwrap_or(&ones),
    };
    let windows = PixelSections {
        image,
        selection: &selection,
        mask: PreparedMask::new_for_outputs(
            request.mask.as_deref(),
            image.shape(),
            image.coordinates(),
            request.stretch,
            (!request.outfile.as_os_str().is_empty()).then_some(request.outfile.as_path()),
        )?,
    };
    let method = if strip.rotated() {
        RegridMethod::Cubic
    } else {
        RegridMethod::Nearest
    };
    let (origin, extent) =
        section_bounds(&offsets, plane.map(|axis| packed[axis]), strip.rotated());
    for point in offsets.iter_mut().flatten() {
        for axis in 0..2 {
            point[axis] -= origin[axis] as f64;
        }
    }
    let mut grid = packed.clone();
    grid[x] = 1;
    grid[y] = 1;
    let mut tile = ones.clone();
    tile[x] = extent[0];
    tile[y] = extent[1];
    let mut output_tile = vec![1; tile.len() - 1];
    output_tile[x - usize::from(y < x)] = strip.shape[0];
    let mut any_good = false;
    let mut has_mask = false;
    for plane in TraversalCursorIter::new(grid, ones, TraversalSpec::tiles()) {
        let mut plane_start = plane?.position;
        let mut output_start = plane_start.clone();
        output_start.remove(y);
        plane_start[x] = origin[0];
        plane_start[y] = origin[1];
        let (pixels, mask) =
            windows.read(&section.source_start(&plane_start), &tile, section.stride)?;
        let order: Vec<_> = [x, y]
            .into_iter()
            .chain((0..tile.len()).filter(|axis| *axis != x && *axis != y))
            .collect();
        let pixels = pixels.permuted_axes(order.clone()).reversed_axes();
        let pixels: Vec<_> = pixels.iter().map(|&value| f64::from(value)).collect();
        let mask = mask.map(|valid| {
            valid
                .permuted_axes(order)
                .reversed_axes()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        });
        let sky = Plane {
            data: &pixels,
            mask: mask.as_deref(),
            shape: extent,
        };
        let mut values = Vec::with_capacity(offsets.len());
        let mut good = Vec::with_capacity(offsets.len());
        for across in &offsets {
            let (sum, n) = across
                .iter()
                .filter_map(|&at| sky.sample(method, at))
                .fold((0., 0), |(sum, n), value| {
                    (sum + f64::from(value as f32), n + 1)
                });
            values.push(if n == 0 {
                0.
            } else {
                (sum / f64::from(n)) as f32
            });
            any_good |= n > 0;
            good.push(n > 0);
        }
        let values =
            ArrayD::from_shape_vec(IxDyn(&output_tile).f(), values).expect("PV row length");
        result.put_slice(&values, &output_start)?;
        if good.iter().any(|&valid| !valid) {
            if !has_mask {
                result.make_mask("mask0", true, true)?;
                has_mask = true;
            }
            let mask =
                ArrayD::from_shape_vec(IxDyn(&output_tile).f(), good).expect("PV mask row length");
            result.put_mask_slice("mask0", &mask, &output_start)?;
        }
    }
    if !any_good {
        return Err(ImageError::CasaSelection(
            "All selected pixels are masked".into(),
        ));
    }
    Ok(ImagePvProduct {
        image: result,
        coordinate_record,
    })
}

fn section_bounds(
    points: &[Vec<[f64; 2]>],
    shape: [usize; 2],
    cubic: bool,
) -> ([usize; 2], [usize; 2]) {
    let mut lower = [f64::INFINITY; 2];
    let mut upper = [f64::NEG_INFINITY; 2];
    for point in points.iter().flatten() {
        for axis in 0..2 {
            lower[axis] = lower[axis].min(point[axis]);
            upper[axis] = upper[axis].max(point[axis]);
        }
    }
    let halo = if cubic { 2. } else { 1. };
    let lower: [usize; 2] =
        std::array::from_fn(|axis| ((lower[axis].floor() - halo) as usize).min(shape[axis] - 1));
    let upper: [usize; 2] =
        std::array::from_fn(|axis| ((upper[axis].ceil() + halo) as usize).min(shape[axis] - 1));
    let extent = std::array::from_fn(|axis| upper[axis] + 1 - lower[axis]);
    (lower, extent)
}
