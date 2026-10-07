// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tap loops over one `[y][x]` block: spread (adjoint), gather (forward)
//! and the kernel norm.
//!
//! `tap' = (conjugate ? conj(t) : t) · e^{i(k_x g_x + k_y g_y)}`. The norm
//! is `Σ (conjugate ? conj(t) : t)` over the same taps without the pointing
//! ramp, which is how CASA's `AWVisResampler` accumulates it
//! (`faccumulateFromGrid`: `norm += wt` before the phase gradient).
//!
//! Every function takes the location the shared rounding rule produced and
//! trusts that the support lies inside the tile; the debug assertions name
//! that invariant.

use num_complex::{Complex, Complex32, Complex64};

use crate::accumulator::{GridScalar, Tile};
use crate::convolution::TapLayout;
use crate::geometry::CellLocation;

/// Add `value · tap'` over the support into `grid` through Mueller plane
/// `mueller`.
#[allow(clippy::too_many_arguments)]
pub(super) fn spread<T: GridScalar>(
    grid: &mut [Complex<T>],
    tile: Tile,
    location: CellLocation,
    taps: &TapLayout<'_>,
    mueller: u8,
    conjugate: bool,
    gradient: [f32; 2],
    value: Complex<T>,
) {
    match *taps {
        TapLayout::SeparableReal {
            rows,
            support,
            oversampling: _,
        } => {
            debug_assert_eq!(mueller, 0, "separable kernels have one Mueller plane");
            debug_assert_eq!(
                gradient,
                [0.0, 0.0],
                "separable kernels carry no phase ramp"
            );
            let support = usize::from(support);
            let rx = &rows[usize::from(location.ox) * support..][..support];
            let ry = &rows[usize::from(location.oy) * support..][..support];
            let (x0, y0) = block_origin(tile, location, [support / 2, support / 2]);
            match support {
                7 => spread_separable::<T, 7>(grid, tile.shape[0], x0, y0, rx, ry, value),
                _ => spread_separable_dyn(grid, tile.shape[0], x0, y0, rx, ry, value),
            }
        }
        TapLayout::Dense {
            data,
            support,
            oversampling,
            mueller_planes,
        } => {
            let tile_taps = dense_tile(
                data,
                support,
                oversampling,
                mueller_planes,
                location,
                mueller,
            );
            let [sx, sy] = [usize::from(support[0]), usize::from(support[1])];
            let (x0, y0) = block_origin(tile, location, [sx / 2, sy / 2]);
            let nx = tile.shape[0];
            for iy in 0..sy {
                let row = &mut grid[(y0 + iy) * nx + x0..][..sx];
                let tap_row = &tile_taps[iy * sx..][..sx];
                for (ix, (cell, tap)) in row.iter_mut().zip(tap_row).enumerate() {
                    let tap = dense_tap(*tap, conjugate, gradient, ix, iy, sx, sy);
                    let tap = Complex::new(T::from_f64(tap.re), T::from_f64(tap.im));
                    *cell = *cell + value * tap;
                }
            }
        }
    }
}

/// `Σ conj(tap') · grid` over the support through Mueller plane `mueller`.
pub(super) fn gather<T: GridScalar>(
    grid: &[Complex<T>],
    tile: Tile,
    location: CellLocation,
    taps: &TapLayout<'_>,
    mueller: u8,
    conjugate: bool,
    gradient: [f32; 2],
) -> Complex64 {
    match *taps {
        TapLayout::SeparableReal {
            rows,
            support,
            oversampling: _,
        } => {
            debug_assert_eq!(mueller, 0, "separable kernels have one Mueller plane");
            let support = usize::from(support);
            let rx = &rows[usize::from(location.ox) * support..][..support];
            let ry = &rows[usize::from(location.oy) * support..][..support];
            let (x0, y0) = block_origin(tile, location, [support / 2, support / 2]);
            let nx = tile.shape[0];
            let mut sum = Complex::<T>::default();
            for (iy, wy) in ry.iter().enumerate() {
                let row = &grid[(y0 + iy) * nx + x0..][..support];
                let mut row_sum = Complex::<T>::default();
                for (cell, wx) in row.iter().zip(rx) {
                    row_sum = row_sum + *cell * T::from_f32(*wx);
                }
                sum = sum + row_sum * T::from_f32(*wy);
            }
            Complex64::new(sum.re.into_f64(), sum.im.into_f64())
        }
        TapLayout::Dense {
            data,
            support,
            oversampling,
            mueller_planes,
        } => {
            let tile_taps = dense_tile(
                data,
                support,
                oversampling,
                mueller_planes,
                location,
                mueller,
            );
            let [sx, sy] = [usize::from(support[0]), usize::from(support[1])];
            let (x0, y0) = block_origin(tile, location, [sx / 2, sy / 2]);
            let nx = tile.shape[0];
            let mut sum = Complex64::default();
            for iy in 0..sy {
                let row = &grid[(y0 + iy) * nx + x0..][..sx];
                let tap_row = &tile_taps[iy * sx..][..sx];
                for (ix, (cell, tap)) in row.iter().zip(tap_row).enumerate() {
                    let tap = dense_tap(*tap, conjugate, gradient, ix, iy, sx, sy);
                    let cell = Complex64::new(cell.re.into_f64(), cell.im.into_f64());
                    sum += tap.conj() * cell;
                }
            }
            sum
        }
    }
}

/// `Σ (conjugate ? conj(t) : t)` over the support of Mueller plane
/// `mueller` at the sample's fine offset, without the pointing ramp: the
/// forward normalisation, whose magnitude `sumwt` accumulates.
pub(crate) fn norm(
    taps: &TapLayout<'_>,
    location: CellLocation,
    mueller: u8,
    conjugate: bool,
) -> Complex64 {
    match *taps {
        TapLayout::SeparableReal {
            rows,
            support,
            oversampling: _,
        } => {
            debug_assert_eq!(mueller, 0, "separable kernels have one Mueller plane");
            let support = usize::from(support);
            let axis_sum = |offset: u16| {
                rows[usize::from(offset) * support..][..support]
                    .iter()
                    .map(|tap| f64::from(*tap))
                    .sum::<f64>()
            };
            Complex64::new(axis_sum(location.ox) * axis_sum(location.oy), 0.0)
        }
        TapLayout::Dense {
            data,
            support,
            oversampling,
            mueller_planes,
        } => {
            let sum = dense_tile(
                data,
                support,
                oversampling,
                mueller_planes,
                location,
                mueller,
            )
            .iter()
            .map(|tap| Complex64::new(f64::from(tap.re), f64::from(tap.im)))
            .sum::<Complex64>();
            if conjugate { sum.conj() } else { sum }
        }
    }
}

/// Block-local `(x0, y0)` of the first tap.
fn block_origin(tile: Tile, location: CellLocation, half: [usize; 2]) -> (usize, usize) {
    let x0 = location.x - half[0] as i64 - tile.origin[0] as i64;
    let y0 = location.y - half[1] as i64 - tile.origin[1] as i64;
    debug_assert!(
        x0 >= 0
            && y0 >= 0
            && x0 as usize + 2 * half[0] < tile.shape[0]
            && y0 as usize + 2 * half[1] < tile.shape[1],
        "sample support lies outside the accumulator tile"
    );
    (x0 as usize, y0 as usize)
}

fn spread_separable<T: GridScalar, const S: usize>(
    grid: &mut [Complex<T>],
    nx: usize,
    x0: usize,
    y0: usize,
    rx: &[f32],
    ry: &[f32],
    value: Complex<T>,
) {
    let mut taps_x = [T::zero(); S];
    for (target, tap) in taps_x.iter_mut().zip(rx) {
        *target = T::from_f32(*tap);
    }
    for (iy, wy) in ry.iter().enumerate() {
        let scaled = value * T::from_f32(*wy);
        let row: &mut [Complex<T>; S] = (&mut grid[(y0 + iy) * nx + x0..][..S])
            .try_into()
            .expect("row has S cells");
        for (cell, tap) in row.iter_mut().zip(&taps_x) {
            *cell = *cell + scaled * *tap;
        }
    }
}

fn spread_separable_dyn<T: GridScalar>(
    grid: &mut [Complex<T>],
    nx: usize,
    x0: usize,
    y0: usize,
    rx: &[f32],
    ry: &[f32],
    value: Complex<T>,
) {
    let support = rx.len();
    for (iy, wy) in ry.iter().enumerate() {
        let scaled = value * T::from_f32(*wy);
        let row = &mut grid[(y0 + iy) * nx + x0..][..support];
        for (cell, tap) in row.iter_mut().zip(rx) {
            *cell = *cell + scaled * T::from_f32(*tap);
        }
    }
}

/// The `sy × sx` tile of one Mueller plane at the sample's fine offset.
fn dense_tile(
    data: &[Complex32],
    support: [u16; 2],
    oversampling: u16,
    mueller_planes: u8,
    location: CellLocation,
    mueller: u8,
) -> &[Complex32] {
    debug_assert!(mueller < mueller_planes, "Mueller plane outside the cell");
    let taps = usize::from(support[0]) * usize::from(support[1]);
    let rows = usize::from(oversampling) + 1;
    let offset = (usize::from(location.oy) * rows + usize::from(location.ox))
        * usize::from(mueller_planes)
        + usize::from(mueller);
    &data[offset * taps..][..taps]
}

fn dense_tap(
    tap: Complex32,
    conjugate: bool,
    gradient: [f32; 2],
    ix: usize,
    iy: usize,
    sx: usize,
    sy: usize,
) -> Complex64 {
    let mut tap = Complex64::new(f64::from(tap.re), f64::from(tap.im));
    if conjugate {
        tap = tap.conj();
    }
    if gradient != [0.0, 0.0] {
        let kx = ix as f64 - (sx / 2) as f64;
        let ky = iy as f64 - (sy / 2) as f64;
        let phase = kx * f64::from(gradient[0]) + ky * f64::from(gradient[1]);
        tap *= Complex64::from_polar(1.0, phase);
    }
    tap
}
