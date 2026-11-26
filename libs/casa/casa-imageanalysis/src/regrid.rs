// SPDX-License-Identifier: LGPL-3.0-or-later
//! Regridding an image's direction axes onto another grid, as casacore's
//! `ImageRegrid` regrids them for `imregrid` and the image tool's `regrid`.
//!
//! The output grid is a [`RegridTemplate`]: a coordinate system and a
//! shape, what `imregrid(template='get')` returns and what `simanalyze` and
//! `simutil.convimage` build from one image to regrid another onto. Each
//! output pixel of the direction axes is converted to world coordinates on
//! the template and back to an input pixel, and the input plane is sampled
//! there by casacore's `Interpolate2D` rules ([`RegridMethod`]):
//! - nearest: the nearest pixel within half a pixel (0.5001) of the image;
//! - linear: the 2 × 2 pixels from the position truncated toward zero, moved
//!   back by one at the far edge, so positions from −1 to the last pixel plus
//!   one are accepted and up to a pixel past an edge is extrapolated;
//! - cubic: Numerical Recipes' bicubic (`bcucof`) from finite-difference
//!   derivatives on the 4 × 4 pixels around the position, falling back to
//!   linear within a pixel of an edge.
//!
//! A pixel that cannot be sampled, because its position is off the image or
//! a pixel it needs is masked, is 0 and masked, as `regrid2DMatrix` leaves
//! it. A Jy/pixel image is scaled by the ratio of the output to the input
//! pixel area (`ImageRegrid::findScaleFactor`), so the flux is kept.
//!
//! casacore converts the positions on a coarser grid when `decimate` is above
//! 1 (10 by default): every `floor(n / (floor(n / decimate) − 1))`-th output
//! pixel, interpolating between them (`make2DCoordinateGrid`). casa-rs
//! converts every output pixel, as casacore does with `decimate=0`, so its
//! positions are exact.
//!
//! [`regrid`] regrids the direction axes alone, in the same frame.
//! [`regrid_along_frequency`] also regrids a spectral axis that differs, in
//! the same frame, for a tclean start model (the `spectral` module:
//! casacore's `regrid1D`, linear whatever the method, as
//! `SIImageStore::regridToModelImage` asks for one). For every other
//! coordinate:
//! - where all its axes have one pixel in both images, casacore does not
//!   regrid it and the output keeps the input's coordinate (CASA 6.7.6 keeps
//!   the input's channel and Stokes for a template at another frequency or
//!   Stokes), and so does casa-rs;
//! - otherwise the template's must be the input's: the same type, names,
//!   units, spectral frame and rest frequency or Stokes list, and the same
//!   world value at every pixel. A different one is refused rather than
//!   relabelled.

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
    ProjectionType,
};
use casa_core::ArrayD;

use ndarray::{IxDyn, ShapeBuilder};

use crate::plane::PlaneIndex;
use casa_images::{ImageError, ImageInterface, ImagePixel};

mod spectral;
mod task;

use casa_images::resample::{Plane, RegridMethod, RegridPixel};
pub(crate) use task::{
    ImageToolSampling, TaskRegridOptions, frame_from_template, regrid_direction_frame, regrid_task,
};

/// The coordinate system and shape an image is regridded onto.
#[derive(Clone, Debug)]
pub struct RegridTemplate {
    /// The output's coordinate system.
    pub coordinates: CoordinateSystem,
    /// The output's shape.
    pub shape: Vec<usize>,
}

impl RegridTemplate {
    /// `imregrid(imagename, template='get')`: `image`'s own coordinate
    /// system and shape.
    pub fn of<T: ImagePixel>(image: &impl ImageInterface<T>) -> Self {
        Self {
            coordinates: image.coordinates().clone(),
            shape: image.shape().to_vec(),
        }
    }
}

/// An image regridded onto a template.
#[derive(Clone, Debug)]
pub struct Regridded<T> {
    /// The pixels, in the template's shape (Fortran order).
    pub data: ArrayD<T>,
    /// Whether each pixel was sampled: false where it could not be, which
    /// holds 0, or beyond a spectral regrid's input, which holds the end
    /// channel's value as `regrid1D` leaves it.
    pub mask: ArrayD<bool>,
    /// The output's coordinate system: the template's, with each one-pixel
    /// coordinate other than the direction taken from the input.
    pub coordinates: CoordinateSystem,
    /// Whether that is the template's coordinate system unchanged.
    pub template_coordinates: bool,
    /// The Jy/pixel scale applied, 1 for other units.
    pub scale: f64,
}

/// The direction coordinate's index and pixel axes in longitude-latitude order.
fn direction_of(coordinates: &CoordinateSystem) -> Option<(usize, [usize; 2])> {
    let index = coordinates.find_coordinate(CoordinateType::Direction)?;
    let axes = coordinates.coordinate_pixel_axes(index)?;
    let [Some(longitude), Some(latitude)] = axes.as_slice() else {
        return None;
    };
    Some((index, [*longitude, *latitude]))
}

fn direction(coordinates: &CoordinateSystem, index: usize) -> &DirectionCoordinate {
    match coordinates.coordinate(index) {
        CoordinateModel::Direction(direction) => direction,
        _ => unreachable!("find_coordinate found a direction coordinate"),
    }
}

/// Whether two coordinates other than the direction are the same: type,
/// names, units, the spectral frame and rest frequency or the Stokes list,
/// and the world value at each of the `shape` pixels of their axes.
fn same_axes(a: &CoordinateModel, b: &CoordinateModel, shape: &[usize]) -> bool {
    same_axis_grid(a, b, shape, |pixel| {
        a.to_world(pixel).ok().zip(b.to_world(pixel).ok())
    })
}

fn same_axis_grid(
    a: &CoordinateModel,
    b: &CoordinateModel,
    shape: &[usize],
    mut worlds: impl FnMut(&[f64]) -> Option<(Vec<f64>, Vec<f64>)>,
) -> bool {
    let close = |x: f64, y: f64| (x - y).abs() <= 1e-12 * x.abs().max(y.abs());
    let identity = match (a, b) {
        (CoordinateModel::Spectral(a), CoordinateModel::Spectral(b)) => {
            a.frequency_ref() == b.frequency_ref() && close(a.rest_frequency(), b.rest_frequency())
        }
        (CoordinateModel::Stokes(a), CoordinateModel::Stokes(b)) => a.stokes() == b.stokes(),
        _ => true,
    };
    if !(identity
        && a.coordinate_type() == b.coordinate_type()
        && a.axis_names() == b.axis_names()
        && a.axis_units() == b.axis_units())
    {
        return false;
    }
    let pixels: usize = shape.iter().product();
    (0..pixels).all(|k| {
        let mut rest = k;
        let pixel: Vec<f64> = shape
            .iter()
            .map(|n| {
                let p = rest % n;
                rest /= n;
                p as f64
            })
            .collect();
        worlds(&pixel).is_some_and(|(x, y)| x.iter().zip(&y).all(|(p, q)| close(*p, *q)))
    })
}

/// A direction coordinate with its longitude and latitude pixel axes.
type DirectionAxes<'a> = (&'a DirectionCoordinate, [usize; 2]);

/// How an image is regridded onto a template: the two direction coordinates
/// and their pixel axes, the spectral pass when the spectral axes
/// differ, and the output's coordinate system.
struct AxesPlan<'a> {
    input: DirectionAxes<'a>,
    output: DirectionAxes<'a>,
    spectral: Option<spectral::SpectralPlan>,
    coordinates: CoordinateSystem,
    template_coordinates: bool,
}

/// The plan, once the two images are checked to differ only on the
/// direction axes, on coordinates of one pixel and, `along_frequency`, on
/// the spectral axis.
fn plan_axes<'a>(
    along_frequency: bool,
    input: &'a CoordinateSystem,
    input_shape: &[usize],
    template: &'a RegridTemplate,
) -> Result<AxesPlan<'a>, ImageError> {
    let refuse = |why: String| ImageError::InvalidMetadata(format!("regrid: {why}"));
    if input_shape.len() != template.shape.len()
        || input.n_coordinates() != template.coordinates.n_coordinates()
    {
        return Err(refuse(format!(
            "the input has shape {input_shape:?} and the template {:?}; casa-rs regrids images \
             with the same axes",
            template.shape
        )));
    }
    let (Some((in_index, in_axis)), Some((out_index, out_axis))) =
        (direction_of(input), direction_of(&template.coordinates))
    else {
        return Err(refuse("both images need a direction coordinate".into()));
    };
    let mut input_plane = in_axis;
    let mut output_plane = out_axis;
    input_plane.sort_unstable();
    output_plane.sort_unstable();
    if in_index != out_index || input_plane != output_plane {
        return Err(refuse(
            "the direction coordinates are in different places".into(),
        ));
    }
    let mut coordinates = template.coordinates.clone();
    let mut template_coordinates = true;
    let mut spectral = None;
    for k in 0..input.n_coordinates() {
        let ours = input.coordinate(k);
        let theirs = template.coordinates.coordinate(k);
        if k == in_index {
            continue;
        }
        let axes = input.coordinate_pixel_axes(k).expect("existing coordinate");
        if template.coordinates.coordinate_pixel_axes(k).as_ref() != Some(&axes) {
            return Err(refuse(
                "non-direction pixel axes occupy different positions".into(),
            ));
        }
        let visible: Vec<_> = axes.iter().copied().flatten().collect();
        if visible.is_empty() {
            continue;
        }
        let in_shape: Vec<_> = visible.iter().map(|&axis| input_shape[axis]).collect();
        let out_shape: Vec<_> = visible.iter().map(|&axis| template.shape[axis]).collect();
        let equivalent = || {
            if visible.len() == axes.len() {
                return same_axes(ours, theirs, &in_shape);
            }
            let mut input_pixel = input.local_reference_pixel(k);
            let mut target_pixel = template.coordinates.local_reference_pixel(k);
            let local = axes
                .iter()
                .enumerate()
                .filter_map(|(local, axis)| axis.map(|_| local));
            same_axis_grid(ours, theirs, &in_shape, |pixel| {
                for (local, &value) in local.clone().zip(pixel) {
                    input_pixel[local] = value;
                    target_pixel[local] = value;
                }
                ours.to_world(&input_pixel)
                    .ok()
                    .zip(theirs.to_world(&target_pixel).ok())
            })
        };
        let degenerate = in_shape.iter().chain(&out_shape).all(|n| *n == 1);
        if degenerate && ours.coordinate_type() == theirs.coordinate_type() {
            // Not regridded: the output keeps the input's coordinate.
            if !equivalent() {
                coordinates.replace_coordinate(k, ours.clone());
                template_coordinates = false;
            }
        } else if in_shape != out_shape || !equivalent() {
            if !along_frequency {
                return Err(refuse(format!(
                    "the {} axes differ between the input and the template; casa-rs regrids the \
                     direction axes only",
                    ours.coordinate_type()
                )));
            }
            spectral = Some(
                spectral::SpectralPlan::new(
                    (ours, in_shape[0]),
                    (theirs, out_shape[0]),
                    visible[0],
                )
                .map_err(refuse)?,
            );
        }
    }
    let input_direction = direction(input, in_index);
    let output_direction = direction(&template.coordinates, out_index);
    if input_direction.direction_ref() != output_direction.direction_ref() {
        return Err(refuse(format!(
            "the input's directions are {:?} and the template's {:?}; casa-rs does not convert \
             between direction frames",
            input_direction.direction_ref(),
            output_direction.direction_ref()
        )));
    }
    if input_direction.conversion_direction_ref() != output_direction.conversion_direction_ref() {
        return Err(refuse(
            "input and template direction conversion frames differ".into(),
        ));
    }
    Ok(AxesPlan {
        input: (input_direction, in_axis),
        output: (output_direction, out_axis),
        spectral,
        coordinates,
        template_coordinates,
    })
}

/// `ImageRegrid::findScaleFactor`: the output-to-input pixel area for a
/// Jy/pixel image, 1 otherwise.
fn jy_per_pixel_scale(
    units: &str,
    input: &DirectionCoordinate,
    output: &DirectionCoordinate,
) -> f64 {
    if !units.eq_ignore_ascii_case("Jy/pixel") {
        return 1.0;
    }
    let (inc_in, inc_out) = (input.increment(), output.increment());
    (inc_out[0] * inc_out[1] / inc_in[0] / inc_in[1]).abs()
}

/// Whether the spherical rotations describe one tangent plane. A
/// direction written through a CASA header can change by one representable
/// step. The resolved pole is the rotation, regardless of the LATPOLE
/// selector that chose it.
fn same_tangent_plane(
    a: &DirectionCoordinate,
    b: &DirectionCoordinate,
    rounding_radians: f64,
) -> bool {
    let same_rounded_value = |left: f64, right: f64| {
        left == right
            || ((left.next_up() == right || right.next_up() == left)
                && (left - right).abs() <= rounding_radians)
    };
    let same_pair = |left: &[f64], right: &[f64]| {
        left.iter()
            .zip(right)
            .all(|(&left, &right)| same_rounded_value(left, right))
    };
    same_pair(&a.reference_value(), &b.reference_value())
        && a.projection() == b.projection()
        && a.pc_matrix() == b.pc_matrix()
        && same_rounded_value(a.longpole(), b.longpole())
        && same_pair(&a.native_pole(), &b.native_pole())
}

/// Whether every output pixel of an `[nx, ny]` plane has a sky position,
/// shown by its four corners, for a zenithal projection with the identity
/// PC matrix. Those projections are valid on a disk about the reference
/// point in the intermediate plane, which the identity PC maps to the pixel
/// grid by an axis-aligned affine map: the grid's rectangle lies in the
/// disk when its corners do.
fn corners_cover_the_plane(output: &DirectionCoordinate, [nx, ny]: [usize; 2]) -> bool {
    let zenithal = matches!(
        output.projection().projection_type(),
        ProjectionType::SIN
            | ProjectionType::TAN
            | ProjectionType::ARC
            | ProjectionType::ZEA
            | ProjectionType::STG
    ) && output.projection().parameters().iter().all(|p| *p == 0.0);
    let (last_x, last_y) = (nx as f64 - 1.0, ny as f64 - 1.0);
    zenithal
        && [[0.0, 0.0], [last_x, 0.0], [0.0, last_y], [last_x, last_y]]
            .iter()
            .all(|corner| {
                output
                    .to_world(corner)
                    .is_ok_and(|world| world.iter().all(|v| v.is_finite()))
            })
}

/// The exact pixel mapping between direction coordinates on one tangent plane.
#[derive(Clone, Copy, Debug)]
pub struct DirectionAffine {
    input_reference: [f64; 2],
    output_reference: [f64; 2],
    increment_ratio: [f64; 2],
}

impl DirectionAffine {
    /// The source pixel at this output pixel.
    #[must_use]
    pub fn source_pixel(self, output: [f64; 2]) -> [f64; 2] {
        std::array::from_fn(|axis| {
            self.input_reference[axis]
                + (output[axis] - self.output_reference[axis]) * self.increment_ratio[axis]
        })
    }
}

/// The exact affine map when both direction coordinates share a tangent
/// plane, use identity PC matrices and the output is inside the projection.
/// A single rounding step in the stored rotation also preserves the map.
#[must_use]
pub fn direction_affine(
    input: &DirectionCoordinate,
    output: &DirectionCoordinate,
    shape: [usize; 2],
) -> Option<DirectionAffine> {
    let (input_increment, output_increment) = (input.increment(), output.increment());
    // Use the aligned image-mask pixel tolerance to bound the angular
    // difference on very fine grids as well.
    let rounding_radians = input_increment
        .iter()
        .chain(&output_increment)
        .map(|step| step.abs())
        .fold(f64::INFINITY, f64::min)
        * 1e-7;
    if input.direction_ref() == output.direction_ref()
        && input.conversion_direction_ref() == output.conversion_direction_ref()
        && same_tangent_plane(input, output, rounding_radians)
        && *input.pc_matrix() == ndarray::Array2::<f64>::eye(2)
        && corners_cover_the_plane(output, shape)
    {
        Some(DirectionAffine {
            input_reference: input.reference_pixel().try_into().ok()?,
            output_reference: output.reference_pixel().try_into().ok()?,
            increment_ratio: std::array::from_fn(|axis| {
                output_increment[axis] / input_increment[axis]
            }),
        })
    } else {
        None
    }
}

/// For each output pixel (first axis fastest), the input pixel at its
/// world position, or `None` where the conversion fails.
fn positions(
    input: &DirectionCoordinate,
    output: &DirectionCoordinate,
    [nx, ny]: [usize; 2],
) -> Vec<Option<[f64; 2]>> {
    if let Some(affine) = direction_affine(input, output, [nx, ny]) {
        return (0..ny)
            .flat_map(|j| (0..nx).map(move |i| [i as f64, j as f64]))
            .map(|output| Some(affine.source_pixel(output)))
            .collect();
    }
    let mut grid = Vec::with_capacity(nx * ny);
    for j in 0..ny {
        for i in 0..nx {
            let position = output
                .to_world(&[i as f64, j as f64])
                .and_then(|world| input.to_pixel(&world))
                .ok()
                .filter(|pixel| pixel.iter().all(|value| value.is_finite()))
                .map(|pixel| [pixel[0], pixel[1]]);
            grid.push(position);
        }
    }
    grid
}

/// Whether an image of `coordinates` and `shape` can be regridded onto
/// `template`: [`regrid`]'s check of the axes, without reading pixels.
///
/// # Errors
///
/// What [`regrid`] refuses before it reads the input: axes other than the
/// direction that differ where they have more than one pixel, or another
/// direction frame.
pub fn check_regrid(
    coordinates: &CoordinateSystem,
    shape: &[usize],
    template: &RegridTemplate,
) -> Result<(), ImageError> {
    plan_axes(false, coordinates, shape, template).map(drop)
}

/// Whether [`regrid_along_frequency`] can regrid an image of `coordinates`
/// and `shape` onto `template`, without reading a pixel: `true` when it
/// would regrid the spectral axis too, `false` for the direction axes alone.
///
/// # Errors
///
/// As [`regrid_along_frequency`], for all but reading.
pub fn check_regrid_along_frequency(
    coordinates: &CoordinateSystem,
    shape: &[usize],
    template: &RegridTemplate,
) -> Result<bool, ImageError> {
    plan_axes(true, coordinates, shape, template).map(|plan| plan.spectral.is_some())
}

/// Regrid `input`'s direction axes onto `template` by `method`.
///
/// # Errors
///
/// A template whose axes other than the direction differ from the input's,
/// where they have more than one pixel, or whose direction frame does, and a
/// failure to read the input.
pub fn regrid<T, I>(
    input: &I,
    template: &RegridTemplate,
    method: RegridMethod,
) -> Result<Regridded<T>, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
    I: ImageInterface<T>,
{
    regrid_with(false, input, template, method, false)
}

/// CASA `LinearMosaic` regrids the weighted pointing inside the output
/// subimage selected by the input's world-coordinate box.
pub(crate) fn regrid_mosaic_pointing(
    input: &impl ImageInterface<f32>,
    template: &RegridTemplate,
) -> Result<Regridded<f32>, ImageError> {
    regrid_with(false, input, template, RegridMethod::Linear, true)
}

/// Regrid `input`'s direction axes onto `template` by `method`, and its
/// spectral axis linearly where the template's differs, as
/// `SIImageStore::regridToModelImage` regrids a tclean start model.
///
/// # Errors
///
/// A template whose axes other than the direction and spectral ones differ
/// from the input's, where they have more than one pixel, whose direction
/// or spectral frame does, whose single channel a cube would be regridded
/// onto or whose channels a single channel would (casacore refuses the one
/// and reads past its array on the other), and a failure to read the input.
pub fn regrid_along_frequency<T, I>(
    input: &I,
    template: &RegridTemplate,
    method: RegridMethod,
) -> Result<Regridded<T>, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
    I: ImageInterface<T>,
{
    regrid_with(true, input, template, method, false)
}

fn regrid_with<T, I>(
    along_frequency: bool,
    input: &I,
    template: &RegridTemplate,
    method: RegridMethod,
    mosaic_pointing: bool,
) -> Result<Regridded<T>, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
    I: ImageInterface<T>,
{
    let input_shape = input.shape().to_vec();
    let plan = plan_axes(along_frequency, input.coordinates(), &input_shape, template)?;
    let ((input_direction, in_axis), (output_direction, out_axis)) = (plan.input, plan.output);
    let scale = jy_per_pixel_scale(input.units(), input_direction, output_direction);
    let in_plane = in_axis.map(|axis| input_shape[axis]);
    let out_plane = out_axis.map(|axis| template.shape[axis]);
    // ImageRegrid::regrid skips interpolation when shape and coordinates
    // agree. Sampling an identical masked grid erodes valid edge pixels
    // unnecessarily, which also changes feather's Fourier coefficients.
    if !along_frequency
        && input_shape == template.shape
        && in_axis == out_axis
        && plan.spectral.is_none()
        && same_axes(
            &CoordinateModel::Direction(input_direction.clone()),
            &CoordinateModel::Direction(output_direction.clone()),
            &in_plane,
        )
    {
        return Ok(Regridded {
            data: input.get()?,
            mask: input
                .default_mask()?
                .unwrap_or_else(|| ArrayD::from_elem(IxDyn(&input_shape).f(), true)),
            coordinates: plan.coordinates,
            template_coordinates: plan.template_coordinates,
            scale,
        });
    }
    let mut grid = positions(input_direction, output_direction, out_plane);
    let method = if mosaic_pointing
        && clip_pointing_grid(
            &mut grid,
            input_direction,
            output_direction,
            in_plane,
            out_plane,
        ) {
        // When the cropped output has the input's WCS and shape, CASA copies
        // directly. Sampling the matching pixel also avoids requiring valid
        // neighbours that contribute nothing to that pixel.
        RegridMethod::Nearest
    } else {
        method
    };
    let data = input.get()?;
    let mask = input.default_mask()?;
    let (mut out_data, mut out_mask) = sample_direction_planes(
        &data,
        mask.as_ref(),
        DirectionSampling {
            axes: [in_axis, out_axis],
            shape: out_plane,
            positions: &grid,
            method,
            scale,
        },
        1,
        |value: T, _| value.into(),
        |_, _, value| T::from(value as f32),
    );
    if let Some(spectral) = &plan.spectral {
        (out_data, out_mask) = spectral.apply(&out_data, &out_mask, &template.shape);
    }
    Ok(Regridded {
        data: out_data,
        mask: out_mask,
        coordinates: plan.coordinates,
        template_coordinates: plan.template_coordinates,
        scale,
    })
}

struct DirectionSampling<'a> {
    axes: [[usize; 2]; 2],
    shape: [usize; 2],
    positions: &'a [Option<[f64; 2]>],
    method: RegridMethod,
    scale: f64,
}

fn sample_direction_planes<T>(
    data: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    sampling: DirectionSampling<'_>,
    components: usize,
    read: impl Fn(T, usize) -> f64,
    write: impl Fn(T, usize, f64) -> T,
) -> (ArrayD<T>, ArrayD<bool>)
where
    T: ImagePixel + Copy,
{
    let DirectionSampling {
        axes: [in_axis, out_axis],
        shape: out_plane,
        positions,
        method,
        scale,
    } = sampling;
    let input_shape = data.shape();
    let in_plane = in_axis.map(|axis| input_shape[axis]);
    let mut direction_shape = input_shape.to_vec();
    for (axis, length) in out_axis.into_iter().zip(out_plane) {
        direction_shape[axis] = length;
    }
    let mut out_data = ArrayD::<T>::from_elem(IxDyn(&direction_shape).f(), T::default_value());
    let mut out_mask = ArrayD::<bool>::from_elem(IxDyn(&direction_shape).f(), false);
    let mut plane = vec![0.0f64; in_plane[0] * in_plane[1]];
    let mut plane_mask = mask.map(|_| vec![true; plane.len()]);
    let mut out_values = vec![T::default_value(); out_plane[0] * out_plane[1]];
    let mut out_valid = vec![false; out_values.len()];
    let planes = PlaneIndex::new(input_shape, in_axis);
    let out_planes = PlaneIndex::new(&direction_shape, out_axis);
    for plane_number in 0..planes.count() {
        if let (Some(values), Some(mask)) = (plane_mask.as_mut(), mask) {
            planes.read(plane_number, mask, values, |v| *v);
        }
        for component in 0..components {
            planes.read(plane_number, data, &mut plane, |v| read(*v, component));
            let view = Plane {
                data: &plane,
                mask: plane_mask.as_deref(),
                shape: in_plane,
            };
            for (k, at) in positions.iter().enumerate() {
                let sampled = at.and_then(|at| view.sample(method, at));
                let value = sampled.map_or(0.0, |value| scale * value);
                out_values[k] = write(out_values[k], component, value);
                out_valid[k] = sampled.is_some();
            }
        }
        out_planes.write(plane_number, &out_values, &mut out_data);
        out_planes.write(plane_number, &out_valid, &mut out_mask);
    }
    (out_data, out_mask)
}

fn clip_pointing_grid(
    grid: &mut [Option<[f64; 2]>],
    input: &DirectionCoordinate,
    output: &DirectionCoordinate,
    input_shape: [usize; 2],
    output_shape: [usize; 2],
) -> bool {
    // LinearMosaic::addOnToImage projects the two LCBox corners through a
    // WCBox and rounds them back to output pixels. An invalid or disjoint
    // box leaves ImageRegrid to restrict the full output by interpolation.
    let project = |point: [f64; 2]| {
        let world = input.to_world(&point).ok()?;
        let pixel = output.to_pixel(&world).ok()?;
        pixel
            .iter()
            .all(|p| p.is_finite())
            .then(|| [(pixel[0] + 0.5) as isize, (pixel[1] + 0.5) as isize])
    };
    let Some(start) = project([0.0, 0.0]) else {
        return false;
    };
    let Some(end) = project(input_shape.map(|n| n as f64 - 1.0)) else {
        return false;
    };
    let start = start.map(|p| p.max(0));
    let end = std::array::from_fn::<_, 2, _>(|i| end[i].min(output_shape[i] as isize - 1));
    if start[0] > end[0] || start[1] > end[1] {
        return false;
    }
    for (y, row) in grid.chunks_mut(output_shape[0]).enumerate() {
        if !(start[1]..=end[1]).contains(&(y as isize)) {
            row.fill(None);
        } else {
            row[..start[0] as usize].fill(None);
            row[end[0] as usize + 1..].fill(None);
        }
    }
    let region_shape = std::array::from_fn(|i| (end[i] - start[i] + 1) as usize);
    let centre = output.reference_pixel();
    region_shape == input_shape
        && same_axes(
            &CoordinateModel::Direction(input.clone()),
            &CoordinateModel::Direction(
                output.clone().with_reference_pixel([
                    centre[0] - start[0] as f64,
                    centre[1] - start[1] as f64,
                ]),
            ),
            &input_shape,
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regrid_shifts_longitude_on_an_interleaved_plane() {
        use casa_coordinates::{Projection, SpectralCoordinate};
        use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};

        let direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, 0.3],
            [-1e-4, 1e-4],
            [0.0, 0.0],
        );
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(direction.clone());
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.4e9,
            1e6,
            0.0,
            1.42e9,
        ));
        coordinates.transpose_axes(&[2, 0, 1], &[1, 2, 0]).unwrap();
        let shape = [3, 2, 5];
        let mut image = casa_images::TempImage::new(
            shape.to_vec(),
            coordinates.clone(),
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        let data = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
            (10 * index[0] + 100 * index[1] + index[2]) as f32
        });
        image.put_slice(&data, &[0; 3]).unwrap();
        coordinates.replace_coordinate(0, direction.with_reference_pixel([-1.0, 0.0]));
        let template = RegridTemplate {
            coordinates,
            shape: shape.to_vec(),
        };
        let result = regrid(&image, &template, RegridMethod::Nearest).unwrap();
        let task_result = task::regrid_task(
            &image,
            &template,
            task::TaskRegridOptions {
                axes: &[0, 2],
                shape: &[],
                as_velocity: false,
                decimate: 0,
                replicate: false,
                method: RegridMethod::Nearest,
                image_tool: None,
            },
        )
        .unwrap();
        for actual in [result, task_result] {
            for ((latitude, channel, longitude), &value) in actual
                .data
                .view()
                .into_dimensionality::<ndarray::Ix3>()
                .unwrap()
                .indexed_iter()
            {
                let good = longitude + 1 < shape[2];
                assert_eq!(actual.mask[[latitude, channel, longitude]], good);
                let expected = if good {
                    data[[latitude, channel, longitude + 1]]
                } else {
                    0.0
                };
                assert_eq!(value, expected);
            }
        }
    }

    #[test]
    fn identity_regrid_uses_replacements_for_hidden_local_pixels() {
        use casa_coordinates::{LinearCoordinate, Projection};
        use casa_core::measures::direction::DirectionRef;

        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, 0.3],
            [-1e-4, 1e-4],
            [0.0, 0.0],
        ));
        coordinates.add_coordinate(
            LinearCoordinate::new(2, vec![], vec![])
                .with_pc_matrix(ndarray::array![[1.0, 1.0], [1.0, 2.0]]),
        );
        coordinates.remove_pixel_axis(3, 2.0).unwrap();
        let shape = [5, 4, 4];
        let mut source = casa_images::TempImage::new(
            shape.to_vec(),
            coordinates,
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        let data =
            ArrayD::from_shape_fn(IxDyn(&shape), |at| (at[0] + 7 * at[1] + 37 * at[2]) as f32);
        source.put_slice(&data, &[0; 3]).unwrap();
        let target = RegridTemplate::of(&source);
        let result = regrid(&source, &target, RegridMethod::Linear).unwrap();
        assert_eq!(result.data, data);
        assert!(result.mask.iter().all(|&valid| valid));
    }

    #[test]
    fn affine_regrid_preserves_a_shared_plane_through_one_step_of_header_rounding() {
        use casa_coordinates::Projection;
        use casa_core::measures::direction::DirectionRef;

        let declination = 0.710_938_054_184_240_3_f64;
        let coordinate = |latitude, x, latitude_selector: f64| {
            DirectionCoordinate::new(
                DirectionRef::J2000,
                Projection::new(ProjectionType::SIN),
                [5.233_697_011_339_746, latitude],
                [-3.878_509_448_876_288e-5, 3.878_509_448_876_288e-5],
                [x, 16.0],
            )
            .with_longpole(std::f64::consts::PI)
            .with_latpole(latitude_selector)
        };
        let next = declination.next_up();
        let input = coordinate(next, 16.5, next);
        let output = coordinate(declination, 16.0, std::f64::consts::FRAC_PI_2);
        let mapping = direction_affine(&input, &output, [32, 32]).expect("one shared plane");
        assert_eq!(mapping.source_pixel([9.0, 21.0]), [9.5, 21.0]);
        let different = coordinate(next.next_up(), 16.5, next.next_up());
        assert!(direction_affine(&different, &output, [32, 32]).is_none());
        let fine_input = input.with_increment([-1e-12, 1e-12]);
        let fine_output = output.with_increment([-1e-12, 1e-12]);
        assert!(direction_affine(&fine_input, &fine_output, [32, 32]).is_none());
    }

    #[test]
    fn regrid_rejects_distinct_direction_conversion_frames() {
        use casa_coordinates::{Projection, ProjectionType};
        use casa_core::measures::direction::DirectionRef;
        let direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.3],
            [-1e-4, 1e-4],
            [2.0, 2.0],
        );
        let mut input = CoordinateSystem::new();
        input.add_coordinate(direction.clone());
        let mut output = CoordinateSystem::new();
        output.add_coordinate(
            direction
                .with_reference_pixel([2.5, 2.0])
                .with_conversion_direction_ref(DirectionRef::GALACTIC),
        );
        let template = RegridTemplate {
            coordinates: output,
            shape: vec![4, 4],
        };
        let error = plan_axes(false, &input, &[4, 4], &template)
            .err()
            .expect("frame mismatch");
        assert!(error.to_string().contains("conversion frames differ"));
    }

    #[test]
    fn positions_on_a_shared_tangent_plane_are_those_of_the_full_conversion() {
        use casa_coordinates::{Projection, ProjectionType};
        use casa_core::measures::direction::DirectionRef;
        let arcsec = std::f64::consts::PI / 648_000.0;
        let input = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [5.2, -0.4],
            [-2.0 * arcsec, 2.0 * arcsec],
            [10.0, 10.0],
        );
        let output = input
            .clone()
            .with_reference_pixel([30.0, 32.5])
            .with_increment([-0.5 * arcsec, 0.5 * arcsec]);
        assert!(same_tangent_plane(&input, &output, 0.0));
        let affine = positions(&input, &output, [60, 64]);
        for (k, at) in affine.iter().enumerate() {
            let (i, j) = ((k % 60) as f64, (k / 60) as f64);
            let world = output.to_world(&[i, j]).unwrap();
            let full = input.to_pixel(&world).unwrap();
            let at = at.unwrap();
            assert!((at[0] - full[0]).abs() < 1e-9 && (at[1] - full[1]).abs() < 1e-9);
        }
    }

    #[test]
    fn a_shared_tangent_plane_past_the_projections_domain_is_not_sampled() {
        // The #941 review's case: SIN at 0.05 rad a pixel, so the corners of
        // a 33 × 33 grid lie beyond the projection's disk and have no sky
        // position. The affine map would give them one; they stay unsampled.
        use casa_coordinates::{Projection, ProjectionType};
        use casa_core::measures::direction::DirectionRef;
        let input = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.3],
            [-0.05, 0.05],
            [16.0, 16.0],
        );
        let output = input.clone().with_reference_pixel([16.1, 16.0]);
        assert!(output.to_world(&[0.0, 0.0]).is_err());
        let grid = positions(&input, &output, [33, 33]);
        assert!(grid[0].is_none(), "{:?}", grid[0]);
        assert!(grid[16 + 16 * 33].is_some());
    }

    fn image_with_stokes(stokes: &[casa_coordinates::StokesType]) -> casa_images::TempImage<f32> {
        use casa_coordinates::{Projection, ProjectionType, StokesCoordinate};
        use casa_core::measures::direction::DirectionRef;
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.3],
            [-1e-5, 1e-5],
            [2.0, 2.0],
        ));
        coordinates.add_coordinate(StokesCoordinate::new(stokes.to_vec()));
        let mut image = casa_images::TempImage::<f32>::new(
            vec![4, 4, stokes.len()],
            coordinates,
            casa_lattices::TempStoragePolicy::Auto {
                memory_budget_bytes: 1 << 20,
                scratch: casa_lattices::ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        let data = ArrayD::from_elem(IxDyn(&[4, 4, stokes.len()]).f(), 42.0f32);
        image.put_slice(&data, &[0, 0, 0]).unwrap();
        image
    }

    #[test]
    fn a_template_relabelling_a_stokes_plane_is_refused() {
        // The #941 review's case: [I, Q] onto a template of [I, U] would put
        // Q's pixels in U's plane.
        use casa_coordinates::StokesType;
        let input = image_with_stokes(&[StokesType::I, StokesType::Q]);
        let template = RegridTemplate::of(&image_with_stokes(&[StokesType::I, StokesType::U]));
        let error = regrid(&input, &template, RegridMethod::Linear).unwrap_err();
        assert!(error.to_string().contains("Stokes axes differ"), "{error}");
        // The same axes regrid, onto the template's coordinates.
        let same = RegridTemplate::of(&input);
        let regridded = regrid(&input, &same, RegridMethod::Linear).unwrap();
        assert!(regridded.template_coordinates);
    }

    #[test]
    fn a_one_plane_axis_keeps_the_inputs_coordinate_as_casa_does() {
        // CASA 6.7.6 regrids an I plane onto a template labelled Q and keeps
        // I; casa-rs keeps the input's coordinate too, rather than relabel.
        use casa_coordinates::StokesType;
        let input = image_with_stokes(&[StokesType::I]);
        let template = RegridTemplate::of(&image_with_stokes(&[StokesType::Q]));
        let regridded = regrid(&input, &template, RegridMethod::Linear).unwrap();
        assert!(!regridded.template_coordinates);
        let CoordinateModel::Stokes(stokes) = regridded.coordinates.coordinate(1) else {
            panic!("a Stokes coordinate")
        };
        assert_eq!(stokes.stokes(), &[StokesType::I]);
    }
}
