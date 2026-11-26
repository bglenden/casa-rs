// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::Range;

use casa_core::{ArrayD, RecordValue, Value};
use casa_lattices::{ScratchSpace, TempStoragePolicy, TraversalCursorIter, TraversalSpec};
use ndarray::{IxDyn, ShapeBuilder};

use crate::analysis::axes::StridedSection;
use crate::analysis::imsubimage::SubimageRegion;
use crate::analysis::imsubimage::coords::beam_section_indices;
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
    /// Channel and polarization indices for retaining the original beam records.
    pub beam_planes: [Vec<usize>; 2],
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
    let endpoints = position::endpoints(request, direction, false)?;
    let strip = geometry::Strip::new(
        direction,
        endpoints,
        plane.map(|axis| image.shape()[axis]),
        width,
        true,
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
    let beams = ImageInfo::from_record(beam_record)?.beam_set;
    let beam_planes = beam_section_indices(
        &beams,
        image.coordinates(),
        image.ndim(),
        &selection.start,
        &packed,
        selection.stride.as_deref(),
    );
    info.beam_set = if beams.is_multi() {
        beams.subset(&beam_planes[0], &beam_planes[1])?
    } else {
        beams
    };
    let mut misc = image.misc_info();
    misc.upsert("secondary_coordinates", Value::Record(secondary));
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
    let sampler = StripSampler {
        section,
        windows,
        strip,
        plane,
        order: [x, y]
            .into_iter()
            .chain((0..packed.len()).filter(|axis| *axis != x && *axis != y))
            .collect(),
    };
    sampler.copy_to(&mut result)?;
    Ok(ImagePvProduct {
        image: result,
        coordinate_record,
        beam_planes,
    })
}

// A square tile in strip coordinates maps to at most a sqrt(2)-wider
// square on the input plane. Tile both axes to bound memory for long or
// wide diagonal strips, including the cubic interpolation halo.
const STRIP_TILE: usize = 128;

struct StripSampler<'a> {
    section: StridedSection<'a>,
    windows: PixelSections<'a, f32>,
    strip: geometry::Strip,
    plane: [usize; 2],
    order: Vec<usize>,
}

impl StripSampler<'_> {
    fn copy_to(&self, result: &mut TempImage<f32>) -> Result<(), ImageError> {
        let [x, y] = self.plane;
        let offset_axis = x - usize::from(y < x);
        let mut grid = self.section.shape.to_vec();
        grid[x] = 1;
        grid[y] = 1;
        let mut shape = vec![1; grid.len() - 1];
        let planes =
            TraversalCursorIter::new(grid, vec![1; self.order.len()], TraversalSpec::tiles());
        let mut any_good = false;
        let mut has_mask = false;
        for plane in planes {
            let plane_start = plane?.position;
            let mut output_start = plane_start.clone();
            output_start.remove(y);
            for along in tile_ranges(self.strip.shape[0]) {
                output_start[offset_axis] = along.start;
                shape[offset_axis] = along.len();
                let samples = self.reduce_rows(&plane_start, along)?;
                let (values, valid): (Vec<_>, Vec<_>) = samples
                    .into_iter()
                    .map(|(sum, count)| {
                        let good = count != 0;
                        any_good |= good;
                        (
                            if good {
                                (sum / count as f64) as f32
                            } else {
                                0.
                            },
                            good,
                        )
                    })
                    .unzip();
                let values =
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), values).expect("PV row length");
                result.put_slice(&values, &output_start)?;
                if valid.iter().any(|&good| !good) {
                    if !has_mask {
                        result.make_mask("mask0", true, true)?;
                        has_mask = true;
                    }
                    let mask = ArrayD::from_shape_vec(IxDyn(&shape).f(), valid)
                        .expect("PV mask row length");
                    result.put_mask_slice("mask0", &mask, &output_start)?;
                }
            }
        }
        if !any_good {
            return Err(ImageError::CasaSelection(
                "All selected pixels are masked".into(),
            ));
        }
        Ok(())
    }

    fn reduce_rows(
        &self,
        plane_start: &[usize],
        along: Range<usize>,
    ) -> Result<Vec<(f64, usize)>, ImageError> {
        let [x, y] = self.plane;
        let sky_shape = self.plane.map(|axis| self.section.shape[axis]);
        let method = if self.strip.rotated() {
            RegridMethod::Cubic
        } else {
            RegridMethod::Nearest
        };
        let mut reduced = vec![(0., 0); along.len()];
        for across in tile_ranges(self.strip.shape[1]) {
            let points = along
                .clone()
                .flat_map(|step| {
                    across
                        .clone()
                        .map(move |width| self.strip.input_pixel(step, width))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (origin, extent) = section_bounds(&points, sky_shape, self.strip.rotated());
            let mut start = plane_start.to_vec();
            let mut shape = vec![1; plane_start.len()];
            start[x] = origin[0];
            start[y] = origin[1];
            shape[x] = extent[0];
            shape[y] = extent[1];
            let (pixels, mask) = self.windows.read(
                &self.section.source_start(&start),
                &shape,
                self.section.stride,
            )?;
            let pixels = pixels.permuted_axes(self.order.clone()).reversed_axes();
            let pixels: Vec<_> = pixels.iter().map(|&value| f64::from(value)).collect();
            let mask = mask.map(|valid| {
                valid
                    .permuted_axes(self.order.clone())
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
            for ((sum, count), row) in reduced.iter_mut().zip(points.chunks_exact(across.len())) {
                for &[px, py] in row {
                    let at = [px - origin[0] as f64, py - origin[1] as f64];
                    if let Some(sample) = sky.sample(method, at) {
                        *sum += f64::from(sample as f32);
                        *count += 1;
                    }
                }
            }
        }
        Ok(reduced)
    }
}

fn tile_ranges(length: usize) -> impl Iterator<Item = Range<usize>> {
    (0..length)
        .step_by(STRIP_TILE)
        .map(move |start| start..(start + STRIP_TILE).min(length))
}

fn section_bounds(points: &[[f64; 2]], shape: [usize; 2], cubic: bool) -> ([usize; 2], [usize; 2]) {
    let mut lower = [f64::INFINITY; 2];
    let mut upper = [f64::NEG_INFINITY; 2];
    for point in points {
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
