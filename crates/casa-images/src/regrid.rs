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
//! casacore converts the positions on a grid decimated `decimate` times (10
//! by default) and interpolates between them. casa-rs converts every output
//! pixel, as casacore does with `decimate=0`: the positions are exact, and a
//! cubic regrid onto a small output, which casacore's decimation can mask
//! entirely, is regridded.
//!
//! Only the direction axes are regridded. Every other axis must be the same
//! in the input and the template, as it is in every regrid the simulation
//! tasks make; a different one is refused, as is a different direction frame.

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
};
use casa_types::ArrayD;
use ndarray::{IxDyn, ShapeBuilder};

use crate::{ImageError, ImageInterface, ImagePixel};

/// casacore's `Interpolate2D` methods that `imregrid` and the image tool's
/// `regrid` offer, Lanczos aside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegridMethod {
    /// The nearest pixel.
    Nearest,
    /// Bilinear interpolation.
    Linear,
    /// Bicubic interpolation (Numerical Recipes' `bcucof`).
    Cubic,
}

impl RegridMethod {
    /// The method `method` names, as casacore's
    /// `Interpolate2D::stringToMethod` reads it: by its first letter, of
    /// any case, with `la…` and `z…` for Lanczos.
    ///
    /// # Errors
    ///
    /// Lanczos, which casa-rs does not offer, or an unknown method.
    pub fn from_casa(method: &str) -> Result<Self, ImageError> {
        let upper = method.to_ascii_uppercase();
        match upper.as_bytes() {
            [b'N', ..] => Ok(Self::Nearest),
            [b'L', b'A', ..] | [b'Z', ..] => Err(ImageError::InvalidMetadata(format!(
                "interpolation method {method:?} (Lanczos) is not supported; use nearest, linear \
                 or cubic"
            ))),
            [b'L', ..] => Ok(Self::Linear),
            [b'C', ..] => Ok(Self::Cubic),
            _ => Err(ImageError::InvalidMetadata(format!(
                "Unknown interpolation method {method}"
            ))),
        }
    }
}

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
    /// Whether each pixel was sampled: false where it is 0 because it could
    /// not be.
    pub mask: ArrayD<bool>,
    /// The Jy/pixel scale applied, 1 for other units.
    pub scale: f64,
}

/// The direction coordinate of `coordinates`: its index and first pixel axis.
fn direction_of(coordinates: &CoordinateSystem) -> Option<(usize, usize)> {
    let index = coordinates.find_coordinate(CoordinateType::Direction)?;
    let first = (0..index)
        .map(|k| coordinates.coordinate(k).n_pixel_axes())
        .sum();
    Some((index, first))
}

fn direction(coordinates: &CoordinateSystem, index: usize) -> &DirectionCoordinate {
    match coordinates.coordinate(index) {
        CoordinateModel::Direction(direction) => direction,
        _ => unreachable!("find_coordinate found a direction coordinate"),
    }
}

/// Whether two coordinates other than the direction map pixels to the same
/// world values.
fn same_mapping(a: &CoordinateModel, b: &CoordinateModel) -> bool {
    let close = |x: &[f64], y: &[f64]| {
        x.len() == y.len()
            && x.iter()
                .zip(y)
                .all(|(p, q)| (p - q).abs() <= 1e-9 * p.abs().max(q.abs()).max(1e-30))
    };
    a.coordinate_type() == b.coordinate_type()
        && a.axis_names() == b.axis_names()
        && close(&a.reference_value(), &b.reference_value())
        && close(&a.reference_pixel(), &b.reference_pixel())
        && close(&a.increment(), &b.increment())
}

/// A direction coordinate and its first pixel axis.
type DirectionAxes<'a> = (&'a DirectionCoordinate, usize);

/// The input's and the template's direction coordinates, and their first
/// pixel axes, once the two are checked to differ only there.
fn check_axes<'a>(
    input: &'a CoordinateSystem,
    input_shape: &[usize],
    template: &'a RegridTemplate,
) -> Result<(DirectionAxes<'a>, DirectionAxes<'a>), ImageError> {
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
    if in_index != out_index {
        return Err(refuse(
            "the direction coordinates are in different places".into(),
        ));
    }
    for k in 0..input.n_coordinates() {
        if k == in_index {
            continue;
        }
        if !same_mapping(input.coordinate(k), template.coordinates.coordinate(k)) {
            return Err(refuse(format!(
                "the {} axes differ; casa-rs regrids the direction axes only",
                input.coordinate(k).coordinate_type()
            )));
        }
    }
    for (axis, (a, b)) in input_shape.iter().zip(&template.shape).enumerate() {
        if axis != in_axis && axis != in_axis + 1 && a != b {
            return Err(refuse(format!(
                "axis {axis} has length {a} in the input and {b} in the template; casa-rs \
                 regrids the direction axes only"
            )));
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
    Ok(((input_direction, in_axis), (output_direction, out_axis)))
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

/// Whether two direction coordinates share their tangent plane: the same
/// reference direction, projection, PC matrix and native poles, so that
/// their pixels differ only by reference pixel and increment.
fn same_tangent_plane(a: &DirectionCoordinate, b: &DirectionCoordinate) -> bool {
    a.reference_value() == b.reference_value()
        && a.projection() == b.projection()
        && a.pc_matrix() == b.pc_matrix()
        && a.longpole() == b.longpole()
        && a.latpole() == b.latpole()
}

/// For each output pixel of the direction plane (first axis fastest), the
/// input pixel at its world position, or `None` where the conversion fails.
///
/// On a shared tangent plane with the identity PC matrix, the intermediate
/// world coordinates are the same, so the mapping is exactly affine on each
/// axis: `crpix_in + (p − crpix_out) × cdelt_out / cdelt_in`. That is the
/// `simanalyze` and `simalma` total-power regrid, which keeps the reference
/// direction.
fn positions(
    input: &DirectionCoordinate,
    output: &DirectionCoordinate,
    [nx, ny]: [usize; 2],
) -> Vec<Option<[f64; 2]>> {
    if same_tangent_plane(input, output) && *input.pc_matrix() == ndarray::Array2::<f64>::eye(2) {
        let (in_pix, out_pix) = (input.reference_pixel(), output.reference_pixel());
        let (in_inc, out_inc) = (input.increment(), output.increment());
        let ratio = [out_inc[0] / in_inc[0], out_inc[1] / in_inc[1]];
        return (0..ny)
            .flat_map(|j| (0..nx).map(move |i| (i, j)))
            .map(|(i, j)| {
                Some([
                    in_pix[0] + (i as f64 - out_pix[0]) * ratio[0],
                    in_pix[1] + (j as f64 - out_pix[1]) * ratio[1],
                ])
            })
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

/// Regrid `input`'s direction axes onto `template` by `method`.
///
/// # Errors
///
/// A template whose axes other than the direction differ from the input's,
/// or whose direction frame does, and a failure to read the input.
pub fn regrid<T, I>(
    input: &I,
    template: &RegridTemplate,
    method: RegridMethod,
) -> Result<Regridded<T>, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
    I: ImageInterface<T>,
{
    let input_shape = input.shape().to_vec();
    let ((input_direction, in_axis), (output_direction, out_axis)) =
        check_axes(input.coordinates(), &input_shape, template)?;
    let scale = jy_per_pixel_scale(input.units(), input_direction, output_direction);
    let in_plane = [input_shape[in_axis], input_shape[in_axis + 1]];
    let out_plane = [template.shape[out_axis], template.shape[out_axis + 1]];
    let grid = positions(input_direction, output_direction, out_plane);
    let data = input.get()?;
    let mask = input.default_mask()?;
    let mut out_data = ArrayD::<T>::from_elem(IxDyn(&template.shape).f(), T::from(0.0));
    let mut out_mask = ArrayD::<bool>::from_elem(IxDyn(&template.shape).f(), false);
    let mut plane = vec![0.0f64; in_plane[0] * in_plane[1]];
    let mut plane_mask = mask.as_ref().map(|_| vec![true; plane.len()]);
    let mut out_values = vec![0.0f64; out_plane[0] * out_plane[1]];
    let mut out_valid = vec![false; out_values.len()];
    let planes = PlaneIndex::new(&input_shape, in_axis);
    let out_planes = PlaneIndex::new(&template.shape, out_axis);
    for plane_number in 0..planes.count {
        planes.read(plane_number, &data, &mut plane, |v| (*v).into());
        if let (Some(values), Some(mask)) = (plane_mask.as_mut(), mask.as_ref()) {
            planes.read(plane_number, mask, values, |v| *v);
        }
        let view = Plane {
            data: &plane,
            mask: plane_mask.as_deref(),
            shape: in_plane,
        };
        for (k, at) in grid.iter().enumerate() {
            let sampled = at.and_then(|at| view.sample(method, at));
            out_values[k] = sampled.map_or(0.0, |value| scale * value);
            out_valid[k] = sampled.is_some();
        }
        out_planes.write(plane_number, &out_values, &mut out_data, |v| {
            T::from(v as f32)
        });
        out_planes.write(plane_number, &out_valid, &mut out_mask, |v| v);
    }
    Ok(Regridded {
        data: out_data,
        mask: out_mask,
        scale,
    })
}

/// The direction planes of an array with its direction axes at
/// `axis, axis + 1`, numbered by the other axes (the first fastest), read and
/// written first axis fastest. A Fortran-ordered array with the direction
/// axes first, as images are, holds each plane contiguously.
struct PlaneIndex<'s> {
    shape: &'s [usize],
    axis: usize,
    others: Vec<usize>,
    count: usize,
}

impl<'s> PlaneIndex<'s> {
    fn new(shape: &'s [usize], axis: usize) -> Self {
        let others: Vec<usize> = (0..shape.len())
            .filter(|a| *a != axis && *a != axis + 1)
            .collect();
        let count = others.iter().map(|a| shape[*a]).product();
        Self {
            shape,
            axis,
            others,
            count,
        }
    }

    fn plane_len(&self) -> usize {
        self.shape[self.axis] * self.shape[self.axis + 1]
    }

    /// The plane's elements in memory order, when they are contiguous there.
    fn contiguous<'a, A>(&self, plane: usize, array: &'a ArrayD<A>) -> Option<&'a [A]> {
        let len = self.plane_len();
        (self.axis == 0)
            .then(|| array.as_slice_memory_order())
            .flatten()
            .filter(|_| array.t().is_standard_layout())
            .map(|all| &all[plane * len..(plane + 1) * len])
    }

    fn index(&self, plane: usize) -> Vec<usize> {
        let mut index = vec![0usize; self.shape.len()];
        let mut rest = plane;
        for a in &self.others {
            index[*a] = rest % self.shape[*a];
            rest /= self.shape[*a];
        }
        index
    }

    fn read<A, B>(&self, plane: usize, array: &ArrayD<A>, into: &mut [B], f: impl Fn(&A) -> B) {
        if let Some(values) = self.contiguous(plane, array) {
            for (out, value) in into.iter_mut().zip(values) {
                *out = f(value);
            }
            return;
        }
        let mut index = self.index(plane);
        let nx = self.shape[self.axis];
        for (k, out) in into.iter_mut().enumerate() {
            index[self.axis] = k % nx;
            index[self.axis + 1] = k / nx;
            *out = f(&array[IxDyn(&index)]);
        }
    }

    fn write<A: Copy, B>(
        &self,
        plane: usize,
        values: &[A],
        array: &mut ArrayD<B>,
        f: impl Fn(A) -> B,
    ) {
        let len = self.plane_len();
        if self.axis == 0 && array.t().is_standard_layout() {
            let all = array
                .as_slice_memory_order_mut()
                .expect("a standard-layout array is contiguous");
            for (out, value) in all[plane * len..(plane + 1) * len].iter_mut().zip(values) {
                *out = f(*value);
            }
            return;
        }
        let mut index = self.index(plane);
        let nx = self.shape[self.axis];
        for (k, value) in values.iter().enumerate() {
            index[self.axis] = k % nx;
            index[self.axis + 1] = k / nx;
            array[IxDyn(&index)] = f(*value);
        }
    }
}

/// One direction plane of the input, first axis fastest, with its mask
/// (true where good) when it has one.
struct Plane<'a> {
    data: &'a [f64],
    mask: Option<&'a [bool]>,
    shape: [usize; 2],
}

impl Plane<'_> {
    fn at(&self, i: usize, j: usize) -> f64 {
        self.data[i + j * self.shape[0]]
    }

    fn good(&self, i: usize, j: usize) -> bool {
        self.mask.is_none_or(|mask| mask[i + j * self.shape[0]])
    }

    fn sample(&self, method: RegridMethod, at: [f64; 2]) -> Option<f64> {
        match method {
            RegridMethod::Nearest => self.nearest(at),
            RegridMethod::Linear => self.linear(at),
            RegridMethod::Cubic => self.cubic(at),
        }
    }

    /// `Interpolate2D::interpNearest`.
    fn nearest(&self, [wi, wj]: [f64; 2]) -> Option<f64> {
        const HALF: f64 = 0.5001;
        let nearest = |w: f64, n: usize| -> Option<usize> {
            let max = n as f64 - 1.0;
            if n == 0 || w < -HALF || w > max + HALF {
                return None;
            }
            Some(if w <= 0.0 {
                0
            } else if w >= max {
                n - 1
            } else {
                (w + 0.5) as usize
            })
        };
        let i = nearest(wi, self.shape[0])?;
        let j = nearest(wj, self.shape[1])?;
        self.good(i, j).then(|| self.at(i, j))
    }

    /// `Interpolate2D::interpLinear`: the position truncated toward zero,
    /// moved back by one at the far edge.
    fn linear(&self, [wi, wj]: [f64; 2]) -> Option<f64> {
        let start = |w: f64, n: usize| -> Option<usize> {
            // casacore's `uInt(Int(w))`: a negative start wraps and fails.
            let truncated = w as i64;
            if truncated < 0 || n < 2 {
                return None;
            }
            let last = n - 1;
            let mut i = usize::try_from(truncated).ok()?;
            if i == last {
                i -= 1;
            }
            (i < last).then_some(i)
        };
        let i = start(wi, self.shape[0])?;
        let j = start(wj, self.shape[1])?;
        if !(self.good(i, j)
            && self.good(i + 1, j)
            && self.good(i, j + 1)
            && self.good(i + 1, j + 1))
        {
            return None;
        }
        let tt = wi - i as f64;
        let uu = wj - j as f64;
        Some(
            (1.0 - tt) * (1.0 - uu) * self.at(i, j)
                + tt * (1.0 - uu) * self.at(i + 1, j)
                + tt * uu * self.at(i + 1, j + 1)
                + (1.0 - tt) * uu * self.at(i, j + 1),
        )
    }

    /// `Interpolate2D::interpCubic`: bicubic on the 4 × 4 pixels from
    /// `[i−1, j−1]`, linear within a pixel of an edge.
    fn cubic(&self, at: [f64; 2]) -> Option<f64> {
        let (i, j) = (at[0] as i64, at[1] as i64);
        let (nx, ny) = (self.shape[0] as i64, self.shape[1] as i64);
        if i <= 0 || i >= nx - 2 || j <= 0 || j >= ny - 2 {
            return self.linear(at);
        }
        let (i, j) = (i as usize, j as usize);
        for jj in j - 1..=j + 2 {
            for ii in i - 1..=i + 2 {
                if !self.good(ii, jj) {
                    return None;
                }
            }
        }
        let d =
            |di: isize, dj: isize| self.at(i.wrapping_add_signed(di), j.wrapping_add_signed(dj));
        // Points 0..3 are [i, j], [i+1, j], [i+1, j+1] and [i, j+1].
        let y = [d(0, 0), d(1, 0), d(1, 1), d(0, 1)];
        let y1 = [
            (d(1, 0) - d(-1, 0)) / 2.0,
            (d(2, 0) - d(0, 0)) / 2.0,
            (d(2, 1) - d(0, 1)) / 2.0,
            (d(1, 1) - d(-1, 1)) / 2.0,
        ];
        let y2 = [
            (d(0, 1) - d(0, -1)) / 2.0,
            (d(1, 1) - d(1, -1)) / 2.0,
            (d(1, 2) - d(1, 0)) / 2.0,
            (d(0, 2) - d(0, 0)) / 2.0,
        ];
        let y12 = [
            (d(1, 1) + d(-1, -1) - d(-1, 1) - d(1, -1)) / 4.0,
            (d(2, 1) + d(0, -1) - d(0, 1) - d(2, -1)) / 4.0,
            (d(2, 2) + d(0, 0) - d(0, 2) - d(2, 0)) / 4.0,
            (d(1, 2) + d(-1, 0) - d(-1, 2) - d(1, 0)) / 4.0,
        ];
        let c = bicubic_coefficients(&y, &y1, &y2, &y12);
        let tt = at[0] - i as f64;
        let uu = at[1] - j as f64;
        let mut result = 0.0;
        for row in c.iter().rev() {
            result = tt * result + ((row[3] * uu + row[2]) * uu + row[1]) * uu + row[0];
        }
        Some(result)
    }
}

/// Numerical Recipes' `bcucof` (3.6), as casacore's `Interpolate2D::bcucof`
/// tabulates it.
fn bicubic_coefficients(
    y: &[f64; 4],
    y1: &[f64; 4],
    y2: &[f64; 4],
    y12: &[f64; 4],
) -> [[f64; 4]; 4] {
    #[rustfmt::skip]
    const WT: [[f64; 16]; 16] = [
        [1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
        [0., 0., 0., 0., 0., 0., 0., 0., 1., 0., 0., 0., 0., 0., 0., 0.],
        [-3., 0., 0., 3., 0., 0., 0., 0., -2., 0., 0., -1., 0., 0., 0., 0.],
        [2., 0., 0., -2., 0., 0., 0., 0., 1., 0., 0., 1., 0., 0., 0., 0.],
        [0., 0., 0., 0., 1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
        [0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 1., 0., 0., 0.],
        [0., 0., 0., 0., -3., 0., 0., 3., 0., 0., 0., 0., -2., 0., 0., -1.],
        [0., 0., 0., 0., 2., 0., 0., -2., 0., 0., 0., 0., 1., 0., 0., 1.],
        [-3., 3., 0., 0., -2., -1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
        [0., 0., 0., 0., 0., 0., 0., 0., -3., 3., 0., 0., -2., -1., 0., 0.],
        [9., -9., 9., -9., 6., 3., -3., -6., 6., -6., -3., 3., 4., 2., 1., 2.],
        [-6., 6., -6., 6., -4., -2., 2., 4., -3., 3., 3., -3., -2., -1., -1., -2.],
        [2., -2., 0., 0., 1., 1., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
        [0., 0., 0., 0., 0., 0., 0., 0., 2., -2., 0., 0., 1., 1., 0., 0.],
        [-6., 6., -6., 6., -3., -3., 3., 3., -4., 4., 2., -2., -2., -2., -1., -1.],
        [4., -4., 4., -4., 2., 2., -2., -2., 2., -2., -2., 2., 1., 1., 1., 1.],
    ];
    let mut x = [0.0; 16];
    x[..4].copy_from_slice(y);
    x[4..8].copy_from_slice(y1);
    x[8..12].copy_from_slice(y2);
    x[12..].copy_from_slice(y12);
    let mut c = [[0.0; 4]; 4];
    for (l, weights) in WT.iter().enumerate() {
        c[l / 4][l % 4] = weights.iter().zip(&x).map(|(w, v)| w * v).sum();
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plane(data: &[f64], shape: [usize; 2]) -> Plane<'_> {
        Plane {
            data,
            mask: None,
            shape,
        }
    }

    #[test]
    fn positions_on_a_shared_tangent_plane_are_those_of_the_full_conversion() {
        use casa_coordinates::{Projection, ProjectionType};
        use casa_types::measures::direction::DirectionRef;
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
        assert!(same_tangent_plane(&input, &output));
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
    fn methods_are_read_by_their_first_letter_as_casacore_reads_them() {
        assert_eq!(
            RegridMethod::from_casa("cubic").unwrap(),
            RegridMethod::Cubic
        );
        assert_eq!(
            RegridMethod::from_casa("LIN").unwrap(),
            RegridMethod::Linear
        );
        assert_eq!(RegridMethod::from_casa("n").unwrap(), RegridMethod::Nearest);
        assert!(RegridMethod::from_casa("lanczos").is_err());
        assert!(RegridMethod::from_casa("spline").is_err());
    }

    #[test]
    fn linear_accepts_a_pixel_beyond_each_edge_as_casacore_does() {
        let data: Vec<f64> = (0..12).map(f64::from).collect();
        let p = plane(&data, [4, 3]);
        // Inside: the bilinear value of the ramp i + 4j.
        assert!((p.linear([1.25, 0.5]).unwrap() - 3.25).abs() < 1e-12);
        // Within a pixel before the first and after the last: extrapolated.
        assert!((p.linear([-0.5, 0.0]).unwrap() + 0.5).abs() < 1e-12);
        assert!((p.linear([3.5, 1.0]).unwrap() - 7.5).abs() < 1e-12);
        // A pixel or more before the first, or past the last plus one.
        assert!(p.linear([-1.0, 0.0]).is_none());
        assert!(p.linear([4.0, 1.0]).is_none());
    }

    #[test]
    fn cubic_reproduces_a_quadratic_inside_and_is_linear_at_the_edges() {
        let shape = [6, 6];
        let f = |i: f64, j: f64| 0.5 * i * i - 0.25 * j + 0.1 * i * j;
        let data: Vec<f64> = (0..36).map(|k| f((k % 6) as f64, (k / 6) as f64)).collect();
        let p = plane(&data, shape);
        // Central differences are exact for a quadratic, and so is bicubic.
        assert!((p.cubic([2.3, 2.6]).unwrap() - f(2.3, 2.6)).abs() < 1e-12);
        // At i = 0 the linear value of the corner pixels.
        assert_eq!(p.cubic([0.4, 2.5]), p.linear([0.4, 2.5]));
    }

    #[test]
    fn a_masked_pixel_in_the_neighbourhood_masks_the_sample() {
        let data = vec![1.0; 36];
        let mut mask = vec![true; 36];
        mask[1 + 6] = false; // [1, 1]
        let p = Plane {
            data: &data,
            mask: Some(&mask),
            shape: [6, 6],
        };
        // Cubic at [2.5, 2.5] needs [1..=4, 1..=4], linear only [2..=3, 2..=3].
        assert!(p.cubic([2.5, 2.5]).is_none());
        assert_eq!(p.linear([2.5, 2.5]), Some(1.0));
        assert!(p.nearest([1.2, 0.9]).is_none());
    }
}
