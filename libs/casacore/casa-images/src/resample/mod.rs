// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pixel interpolation kernels for sampled image planes.
use crate::ImageError;
use num_complex::Complex32;
mod pixel;
pub use pixel::RegridPixel;
/// Interpolation kernels used for native regridding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegridMethod {
    /// The nearest pixel.
    Nearest,
    /// Bilinear interpolation.
    Linear,
    /// Bicubic interpolation (Numerical Recipes' `bcucof`).
    Cubic,
    /// Six-sample Lanczos kernel on each direction axis.
    Lanczos,
}

impl RegridMethod {
    /// Parse an image-tool method, including Lanczos interpolation.
    pub fn from_image_tool(method: &str) -> Result<Self, ImageError> {
        let upper = method.to_ascii_uppercase();
        if upper.starts_with("LA") || upper.starts_with('Z') {
            Ok(Self::Lanczos)
        } else {
            Self::from_casa(method)
        }
    }

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

/// Regrid a complex AW convolution plane around its integer centre pixel,
/// using CASA's cubic interpolation separately on the real and imaginary
/// parts (`SynthesisUtils::rotateComplexArray`). The matrix maps an output
/// offset from that centre to the corresponding input offset. Pixels with
/// no input sample are zero. The first axis is contiguous in both arrays.
///
/// # Panics
///
/// Panics when `input` does not have exactly `shape[0] * shape[1]` pixels.
#[must_use]
pub fn regrid_complex_plane(
    input: &[Complex32],
    shape: [usize; 2],
    input_from_output: [[f64; 2]; 2],
) -> Vec<Complex32> {
    let [width, height] = shape;
    assert_eq!(input.len(), width * height);
    let real: Vec<_> = input.iter().map(|value| f64::from(value.re)).collect();
    let imaginary: Vec<_> = input.iter().map(|value| f64::from(value.im)).collect();
    let real = Plane {
        data: &real,
        mask: None,
        shape,
    };
    let imaginary = Plane {
        data: &imaginary,
        mask: None,
        shape,
    };
    let [cx, cy] = [(width / 2) as f64, (height / 2) as f64];
    let [[xx, xy], [yx, yy]] = input_from_output;
    let mut output = Vec::with_capacity(input.len());
    for y in 0..height {
        let dy = y as f64 - cy;
        for x in 0..width {
            let dx = x as f64 - cx;
            let at = [cx + xx * dx + xy * dy, cy + yx * dx + yy * dy];
            output.push(Complex32::new(
                real.cubic(at).unwrap_or(0.0) as f32,
                imaginary.cubic(at).unwrap_or(0.0) as f32,
            ));
        }
    }
    output
}

/// One direction plane of the input, first axis fastest, with its mask
/// (true where good) when it has one.
pub struct Plane<'a> {
    /// Borrowed plane values in the input image's brightness unit, indexed with the first axis
    /// fastest.
    pub data: &'a [f64],
    /// Optional validity plane in the same order as the values. True marks samples usable in
    /// interpolation.
    pub mask: Option<&'a [bool]>,
    /// Extents of the first and second direction axes in pixels.
    pub shape: [usize; 2],
}

impl Plane<'_> {
    fn at(&self, i: usize, j: usize) -> f64 {
        self.data[i + j * self.shape[0]]
    }

    fn good(&self, i: usize, j: usize) -> bool {
        self.mask.is_none_or(|mask| mask[i + j * self.shape[0]])
    }

    /// Interpolate at fractional pixel coordinates, returning None outside the valid support.
    pub fn sample(&self, method: RegridMethod, at: [f64; 2]) -> Option<f64> {
        match method {
            RegridMethod::Nearest => self.nearest(at),
            RegridMethod::Linear => self.linear(at),
            RegridMethod::Cubic => self.cubic(at),
            RegridMethod::Lanczos => self.lanczos(at),
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

    fn linear(&self, at: [f64; 2]) -> Option<f64> {
        linear_sample(self.shape, at, |i, j| {
            self.good(i, j).then(|| self.at(i, j))
        })
    }

    fn lanczos(&self, [px, py]: [f64; 2]) -> Option<f64> {
        const RADIUS: isize = 3;
        let nx = self.shape[0] as isize;
        let ny = self.shape[1] as isize;
        let a = px.floor() as isize;
        let b = py.floor() as isize;
        let support_x = ((px - 2.) as isize).max(0)..=((px + 3.) as isize).min(nx - 1);
        let support_y = ((py - 2.) as isize).max(0)..=((py + 3.) as isize).min(ny - 1);
        for j in support_y {
            if support_x
                .clone()
                .any(|i| !self.good(i as usize, j as usize))
            {
                return None;
            }
        }
        if a < RADIUS || a >= nx - RADIUS || b < RADIUS || b >= ny - RADIUS {
            return Some(0.0);
        }
        let weight = |distance: f64| {
            let sinc = |x: f64| {
                let x = x * std::f64::consts::PI;
                if x == 0.0 { 1.0 } else { x.sin() / x }
            };
            if distance.abs() < RADIUS as f64 {
                sinc(distance) * sinc(distance / RADIUS as f64)
            } else {
                0.0
            }
        };
        let mut sum = 0.0;
        for i in a - RADIUS + 1..=a + RADIUS {
            let x = weight(px - i as f64);
            for j in b - RADIUS + 1..=b + RADIUS {
                sum += self.at(i as usize, j as usize) * x * weight(py - j as f64);
            }
        }
        Some(sum)
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

/// Bilinear sampling with casacore's `Interpolate2D::interpLinear` edge
/// convention. `sample` returns the input value or `None` for an invalid
/// cell. The output is valid only when all four neighbours are valid.
pub fn linear_sample(
    shape: [usize; 2],
    [wi, wj]: [f64; 2],
    sample: impl Fn(usize, usize) -> Option<f64>,
) -> Option<f64> {
    let start = |w: f64, n: usize| -> Option<usize> {
        // casacore truncates toward zero and moves the stencil back at
        // the far edge. A negative start wraps and fails.
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
    let i = start(wi, shape[0])?;
    let j = start(wj, shape[1])?;
    let tt = wi - i as f64;
    let uu = wj - j as f64;
    Some(
        (1.0 - tt) * (1.0 - uu) * sample(i, j)?
            + tt * (1.0 - uu) * sample(i + 1, j)?
            + tt * uu * sample(i + 1, j + 1)?
            + (1.0 - tt) * uu * sample(i, j + 1)?,
    )
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
    #[test]
    fn complex_regrid_rotates_both_parts_about_the_integer_centre() {
        let shape = [8, 8];
        let input: Vec<_> = (0..8)
            .flat_map(|y| {
                (0..8).map(move |x| Complex32::new((x * x + 2 * y * y) as f32, (x * y - x) as f32))
            })
            .collect();
        let output = regrid_complex_plane(&input, shape, [[0.8, -0.6], [0.6, 0.8]]);
        // (5,5) samples (4.2,5.4), where cubic interpolation recovers
        // these quadratic and bilinear polynomials exactly to Float rounding.
        let want = Complex32::new(75.96, 18.48);
        assert!((output[5 + 5 * 8] - want).norm() < 1.0e-5);
        assert_eq!(output[4 + 4 * 8], input[4 + 4 * 8]);
        assert_eq!(output[7 + 7 * 8], Complex32::default());
    }
    fn plane(data: &[f64], shape: [usize; 2]) -> Plane<'_> {
        Plane {
            data,
            mask: None,
            shape,
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
