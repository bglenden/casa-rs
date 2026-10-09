// SPDX-License-Identifier: LGPL-3.0-or-later
//! Dense cells from oversampled convolution functions: the one permutation
//! from CASA's `[x][y]` oversampled planes (fine pixel `k · sampling + off`
//! per tap) to the `[oy][ox][mueller][iy][ix]` tiles the kernels read.

use num_complex::Complex32;

use crate::convolution::DenseCell;

/// One oversampled convolution-function plane in CASA's layout: `side ×
/// side` values, x fastest, with the kernel origin at `centre`. Tap `k`
/// at fine offset `off` reads pixel `centre + k · sampling + off` along
/// each axis (`fmosaic.f` `xind`, `AWVisResampler` `iloc`).
#[derive(Clone, Copy)]
pub(crate) struct OversampledPlane<'a> {
    pub(crate) values: &'a [Complex32],
    pub(crate) side: usize,
    pub(crate) centre: [usize; 2],
    pub(crate) sampling: u16,
}

impl OversampledPlane<'_> {
    fn at(&self, x: usize, y: usize) -> Complex32 {
        self.values[y * self.side + x]
    }

    /// Pixel of tap `k` (counted from the kernel centre) at fine offset
    /// `off` along `axis`.
    fn pixel(&self, axis: usize, k: i64, off: i64) -> usize {
        let index = self.centre[axis] as i64 + k * i64::from(self.sampling) + off;
        usize::try_from(index).expect("tap inside the oversampled plane")
    }
}

/// The dense cell of `planes` (one oversampled plane per Mueller plane,
/// all of one `side`, `centre` and `sampling`) with `half_support` taps on
/// each side of the centre.
///
/// Fine-offset row `o` reads offset `off = o − sampling/2`, the
/// `nint((loc − pos) · sampling)` of CASA's `locuvw`, so the tiles are a
/// permutation of the oversampled plane, not a resampling.
pub(crate) fn dense_cell(planes: &[OversampledPlane<'_>], half_support: [u16; 2]) -> DenseCell {
    let first = planes.first().expect("a dense cell has a Mueller plane");
    let sampling = first.sampling;
    assert!(sampling.is_multiple_of(2), "oversampling must be even");
    let support = [2 * half_support[0] + 1, 2 * half_support[1] + 1];
    let [sx, sy] = [usize::from(support[0]), usize::from(support[1])];
    let rows = usize::from(sampling) + 1;
    let half = i64::from(sampling / 2);
    let mut data = Vec::with_capacity(rows * rows * planes.len() * sx * sy);
    for oy in 0..rows {
        let off_y = oy as i64 - half;
        for ox in 0..rows {
            let off_x = ox as i64 - half;
            for plane in planes {
                debug_assert_eq!(plane.sampling, sampling);
                for iy in 0..sy {
                    let ky = iy as i64 - i64::from(half_support[1]);
                    let y = plane.pixel(1, ky, off_y);
                    for ix in 0..sx {
                        let kx = ix as i64 - i64::from(half_support[0]);
                        let x = plane.pixel(0, kx, off_x);
                        data.push(plane.at(x, y));
                    }
                }
            }
        }
    }
    DenseCell {
        data: data.into_boxed_slice(),
        support,
        oversampling: sampling,
        mueller_planes: u8::try_from(planes.len()).expect("Mueller planes fit a byte"),
    }
}

/// The dense cell of one even-symmetric kernel stored as its positive
/// quadrant (`WPConvFunc`: `convFunc(abs(ix·sampling + off), abs(iy·sampling
/// + off))`), `quadrant` being `side × side` values, x fastest, with the
/// origin at `(0, 0)`.
///
/// A plane at `sampling` 1 (CASA's one-plane case, where `nint((loc −
/// pos) · 1)` is always zero) becomes a cell at oversampling 2 whose every
/// fine-offset row reads the integer taps.
pub(crate) fn dense_cell_from_quadrant(
    quadrant: &[Complex32],
    side: usize,
    sampling: u16,
    half_support: u16,
) -> DenseCell {
    let oversampling = if sampling == 1 { 2 } else { sampling };
    assert!(oversampling.is_multiple_of(2), "oversampling must be even");
    let support = 2 * half_support + 1;
    let taps = usize::from(support);
    let rows = usize::from(oversampling) + 1;
    let half = i64::from(oversampling / 2);
    let mut data = Vec::with_capacity(rows * rows * taps * taps);
    let pixel = |k: i64, off: i64| {
        if sampling == 1 {
            k.unsigned_abs() as usize
        } else {
            (k * i64::from(sampling) + off).unsigned_abs() as usize
        }
    };
    for oy in 0..rows {
        let off_y = oy as i64 - half;
        for ox in 0..rows {
            let off_x = ox as i64 - half;
            for iy in 0..taps {
                let y = pixel(iy as i64 - i64::from(half_support), off_y);
                for ix in 0..taps {
                    let x = pixel(ix as i64 - i64::from(half_support), off_x);
                    data.push(quadrant[y * side + x]);
                }
            }
        }
    }
    DenseCell {
        data: data.into_boxed_slice(),
        support: [support, support],
        oversampling,
        mueller_planes: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiles_permute_the_oversampled_plane_by_fine_offset() {
        // A 9×9 plane whose value encodes its pixel, centre (4, 4),
        // sampling 2: tap k at offset off reads pixel 4 + 2k + off.
        let side = 9;
        let values = (0..side * side)
            .map(|index| Complex32::new((index % side) as f32, (index / side) as f32))
            .collect::<Vec<_>>();
        let plane = OversampledPlane {
            values: &values,
            side,
            centre: [4, 4],
            sampling: 2,
        };
        let cell = dense_cell(&[plane], [1, 1]);
        assert_eq!(cell.support, [3, 3]);
        assert_eq!(cell.data.len(), 3 * 3 * 9);
        // Row (ox, oy) = (0, 2), the seventh tile: off_x = −1, off_y = +1;
        // tap (ix, iy) = (2, 0) → k = (+1, −1) → pixel (4 + 2 − 1, 4 − 2 +
        // 1) = (5, 3).
        let tile = &cell.data[6 * 9..][..9];
        assert_eq!(tile[2], Complex32::new(5.0, 3.0));
        // Centre row (1, 1), the fifth tile; its centre tap is the plane
        // centre.
        let tile = &cell.data[4 * 9..][..9];
        assert_eq!(tile[4], Complex32::new(4.0, 4.0));
    }

    #[test]
    fn quadrant_cells_mirror_negative_offsets() {
        let side = 6;
        let quadrant = (0..side * side)
            .map(|index| Complex32::new((index % side) as f32, (index / side) as f32))
            .collect::<Vec<_>>();
        let cell = dense_cell_from_quadrant(&quadrant, side, 2, 1);
        // Row (0, 1), the fourth tile: off_x = −1, off_y = 0; tap (0, 1):
        // k = (−1, 0) → |−2 − 1| = 3 along x, 0 along y.
        let tile = &cell.data[3 * 9..][..9];
        assert_eq!(tile[3], Complex32::new(3.0, 0.0));
        // Tap (2, 1): k = (+1, 0) → |2 − 1| = 1.
        assert_eq!(tile[5], Complex32::new(1.0, 0.0));
    }
}
