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
            let block = DenseBlock {
                nx: tile.shape[0],
                origin: [x0, y0],
                support: [sx, sy],
                taps: tile_taps,
                conjugate,
            };
            match PointingRamp::new(gradient, fine_fraction(location, oversampling), [sx, sy]) {
                Some(ramp) => spread_dense::<T, true>(grid, &block, ramp, value),
                None => spread_dense::<T, false>(grid, &block, PointingRamp::NONE, value),
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
            let sum = match support {
                7 => gather_separable::<T, 7>(grid, tile.shape[0], x0, y0, rx, ry),
                _ => gather_separable_dyn(grid, tile.shape[0], x0, y0, rx, ry),
            };
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
            let block = DenseBlock {
                nx: tile.shape[0],
                origin: [x0, y0],
                support: [sx, sy],
                taps: tile_taps,
                conjugate,
            };
            match PointingRamp::new(gradient, fine_fraction(location, oversampling), [sx, sy]) {
                Some(ramp) => gather_dense::<T, true>(grid, &block, ramp),
                None => gather_dense::<T, false>(grid, &block, PointingRamp::NONE),
            }
        }
    }
}

/// `Σ (conjugate ? conj(t) : t)` over the support of Mueller plane
/// `mueller` at the sample's fine offset, without the pointing ramp. The
/// adjoint passes its own conjugation (`w > 0`) and `sumwt` accumulates
/// the magnitude; the forward passes the opposite
/// (`accumulateFromGrid.inc` conjugates for `w ≤ 0` and sums the taps it
/// multiplies the grid by) and divides the gather by the result.
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

/// `Σ_y ry[y] · Σ_x rx[x] · grid[y][x]` over an `S × S` support, each row
/// summed in `x` order and the rows in `y` order, as
/// [`gather_separable_dyn`] sums them.
fn gather_separable<T: GridScalar, const S: usize>(
    grid: &[Complex<T>],
    nx: usize,
    x0: usize,
    y0: usize,
    rx: &[f32],
    ry: &[f32],
) -> Complex<T> {
    let mut taps_x = [T::zero(); S];
    for (target, tap) in taps_x.iter_mut().zip(rx) {
        *target = T::from_f32(*tap);
    }
    let mut sum = Complex::<T>::default();
    for (iy, wy) in ry.iter().enumerate() {
        let row: &[Complex<T>; S] = (&grid[(y0 + iy) * nx + x0..][..S])
            .try_into()
            .expect("row has S cells");
        let mut row_sum = Complex::<T>::default();
        for (cell, tap) in row.iter().zip(&taps_x) {
            row_sum = row_sum + *cell * *tap;
        }
        sum = sum + row_sum * T::from_f32(*wy);
    }
    sum
}

fn gather_separable_dyn<T: GridScalar>(
    grid: &[Complex<T>],
    nx: usize,
    x0: usize,
    y0: usize,
    rx: &[f32],
    ry: &[f32],
) -> Complex<T> {
    let support = rx.len();
    let mut sum = Complex::<T>::default();
    for (iy, wy) in ry.iter().enumerate() {
        let row = &grid[(y0 + iy) * nx + x0..][..support];
        let mut row_sum = Complex::<T>::default();
        for (cell, wx) in row.iter().zip(rx) {
            row_sum = row_sum + *cell * T::from_f32(*wx);
        }
        sum = sum + row_sum * T::from_f32(*wy);
    }
    sum
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

/// The sample's offset from its cell in cells, `(off − s/2)/s` per axis,
/// from its fine rows: the pointing ramp is anchored at the sample itself
/// (CASA reads the ramped kernel at `ix·sampling + off`), so every mode,
/// the PSF and weight image included, carries the same per-sample phase.
fn fine_fraction(location: CellLocation, oversampling: u16) -> [f64; 2] {
    let half = f64::from(oversampling) / 2.0;
    let fraction = |offset: u16| (f64::from(offset) - half) / f64::from(oversampling);
    [fraction(location.ox), fraction(location.oy)]
}

/// One sample's dense taps and the block cells they cover.
struct DenseBlock<'t> {
    /// Cells per block row.
    nx: usize,
    /// Block-local `[x0, y0]` of the first tap ([`block_origin`]).
    origin: [usize; 2],
    /// Taps per axis `[sx, sy]`.
    support: [usize; 2],
    /// The `sy × sx` taps at the sample's fine offset ([`dense_tile`]).
    taps: &'t [Complex32],
    /// Whether the taps are conjugated.
    conjugate: bool,
}

/// The pointing ramp `e^{i(k_x g_x + k_y g_y)}` over a support, with
/// `k = i − ⌊s/2⌋ + f` per axis at the sample's fine fraction `f`
/// ([`fine_fraction`]). The ramp is separable, so the tap loops walk it by
/// recurrence: one phasor at the support's first tap and one step per axis,
/// three evaluations per sample instead of one per tap, which cost about
/// three quarters of mosaic and AW gridding (#732). CASA's resampler
/// instead computes its phase-gradient array once per pointing
/// (`cachePhaseGrad`). Over a support the walk drifts from the direct
/// phasor by about 1e-14.
#[derive(Clone, Copy)]
struct PointingRamp {
    /// The ramp at the first tap of the current row.
    row: Complex64,
    /// The ramp's factor from one tap to the next along x, and from one
    /// row to the next along y.
    step: [Complex64; 2],
}

impl PointingRamp {
    /// The ramp of a kernel without one, which the unramped loops ignore.
    const NONE: Self = Self {
        row: Complex64::new(1.0, 0.0),
        step: [Complex64::new(1.0, 0.0); 2],
    };

    /// The ramp of `gradient` at `fraction` over `support` taps, or `None`
    /// when the gradient is zero.
    fn new(gradient: [f32; 2], fraction: [f64; 2], support: [usize; 2]) -> Option<Self> {
        if gradient == [0.0, 0.0] {
            return None;
        }
        let [gx, gy] = gradient.map(f64::from);
        let kx = fraction[0] - (support[0] / 2) as f64;
        let ky = fraction[1] - (support[1] / 2) as f64;
        Some(Self {
            row: Complex64::from_polar(1.0, kx * gx + ky * gy),
            step: [
                Complex64::from_polar(1.0, gx),
                Complex64::from_polar(1.0, gy),
            ],
        })
    }
}

/// A dense tap as stored, conjugated when `conjugate`, in `f64`.
fn oriented(tap: Complex32, conjugate: bool) -> Complex64 {
    let tap = Complex64::new(f64::from(tap.re), f64::from(tap.im));
    if conjugate { tap.conj() } else { tap }
}

/// Add `value · tap'` over `block`, each tap times `ramp` when `RAMPED`.
fn spread_dense<T: GridScalar, const RAMPED: bool>(
    grid: &mut [Complex<T>],
    block: &DenseBlock<'_>,
    mut ramp: PointingRamp,
    value: Complex<T>,
) {
    let ([x0, y0], [sx, sy]) = (block.origin, block.support);
    for iy in 0..sy {
        let row = &mut grid[(y0 + iy) * block.nx + x0..][..sx];
        let mut phasor = ramp.row;
        for (cell, tap) in row.iter_mut().zip(&block.taps[iy * sx..][..sx]) {
            let mut tap = oriented(*tap, block.conjugate);
            if RAMPED {
                tap *= phasor;
                phasor *= ramp.step[0];
            }
            *cell = *cell + value * Complex::new(T::from_f64(tap.re), T::from_f64(tap.im));
        }
        if RAMPED {
            ramp.row *= ramp.step[1];
        }
    }
}

/// `Σ conj(tap') · grid` over `block`, each tap times `ramp` when `RAMPED`.
fn gather_dense<T: GridScalar, const RAMPED: bool>(
    grid: &[Complex<T>],
    block: &DenseBlock<'_>,
    mut ramp: PointingRamp,
) -> Complex64 {
    let ([x0, y0], [sx, sy]) = (block.origin, block.support);
    let mut sum = Complex64::default();
    for iy in 0..sy {
        let row = &grid[(y0 + iy) * block.nx + x0..][..sx];
        let mut phasor = ramp.row;
        for (cell, tap) in row.iter().zip(&block.taps[iy * sx..][..sx]) {
            let mut tap = oriented(*tap, block.conjugate);
            if RAMPED {
                tap *= phasor;
                phasor *= ramp.step[0];
            }
            sum += tap.conj() * Complex64::new(cell.re.into_f64(), cell.im.into_f64());
        }
        if RAMPED {
            ramp.row *= ramp.step[1];
        }
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic pseudo-random value in `[-1, 1)` for index `i`.
    fn noise(i: usize) -> f64 {
        let mixed = (i as u64 ^ 0x9e37_79b9_7f4a_7c15).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        (mixed >> 11) as f64 / (1_u64 << 52) as f64 - 1.0
    }

    /// The seven-tap separable gather sums in the general gather's order,
    /// so it equals it bit for bit, in both precisions and at every offset
    /// of a block row.
    fn seven_taps_gather_as_the_general_gather<T: GridScalar>() {
        let nx = 13;
        let grid = (0..nx * nx)
            .map(|i| Complex::new(T::from_f64(noise(2 * i)), T::from_f64(noise(2 * i + 1))))
            .collect::<Vec<_>>();
        let taps = (0..14).map(|i| noise(1000 + i) as f32).collect::<Vec<_>>();
        let (rx, ry) = taps.split_at(7);
        for (x0, y0) in [(0, 0), (3, 5), (6, 6)] {
            let seven = gather_separable::<T, 7>(&grid, nx, x0, y0, rx, ry);
            let general = gather_separable_dyn(&grid, nx, x0, y0, rx, ry);
            assert_eq!(
                (seven.re.into_f64().to_bits(), seven.im.into_f64().to_bits()),
                (
                    general.re.into_f64().to_bits(),
                    general.im.into_f64().to_bits()
                ),
                "offset ({x0}, {y0})"
            );
        }
    }

    #[test]
    fn the_seven_tap_gather_equals_the_general_gather() {
        seven_taps_gather_as_the_general_gather::<f32>();
        seven_taps_gather_as_the_general_gather::<f64>();
    }

    /// The walked pointing ramp spreads and gathers what the ramp evaluated
    /// at each tap does, to 1e-12 (#732): odd supports of unequal sides,
    /// gradients beyond π per cell, fine fractions either side of the cell
    /// and both conjugations. A zero gradient has no ramp.
    #[test]
    fn the_walked_pointing_ramp_equals_the_ramp_at_each_tap() {
        let oversampling = 8;
        for (support, gradient, offsets, conjugate) in [
            ([7, 7], [0.37_f32, -0.21], [3_u16, 5], false),
            ([5, 11], [2.9, 4.6], [0, 8], true),
            ([31, 23], [-3.7, 0.05], [8, 1], false),
        ] {
            let [sx, sy] = support;
            let taps = (0..sx * sy)
                .map(|i| Complex32::new(noise(3 * i) as f32, noise(3 * i + 1) as f32))
                .collect::<Vec<_>>();
            let location = CellLocation {
                x: 0,
                y: 0,
                ox: offsets[0],
                oy: offsets[1],
            };
            let fraction = fine_fraction(location, oversampling);
            let direct = |ix: usize, iy: usize| {
                let kx = ix as f64 - (sx / 2) as f64 + fraction[0];
                let ky = iy as f64 - (sy / 2) as f64 + fraction[1];
                let phase = kx * f64::from(gradient[0]) + ky * f64::from(gradient[1]);
                oriented(taps[iy * sx + ix], conjugate) * Complex64::from_polar(1.0, phase)
            };
            let nx = sx + 3;
            let cells = nx * (sy + 4);
            let block = DenseBlock {
                nx,
                origin: [1, 2],
                support,
                taps: &taps,
                conjugate,
            };
            let ramp = PointingRamp::new(gradient, fraction, support).expect("a gradient");
            let cell = |ix: usize, iy: usize| (2 + iy) * nx + 1 + ix;

            let value = Complex64::new(0.6, -1.3);
            let mut spread = vec![Complex64::default(); cells];
            spread_dense::<f64, true>(&mut spread, &block, ramp, value);
            for iy in 0..sy {
                for ix in 0..sx {
                    let error = (spread[cell(ix, iy)] - value * direct(ix, iy)).norm();
                    assert!(
                        error <= 1e-12,
                        "{support:?}: spread tap ({ix}, {iy}) off by {error}"
                    );
                }
            }

            let grid = (0..cells)
                .map(|i| Complex64::new(noise(7 * i), noise(7 * i + 1)))
                .collect::<Vec<_>>();
            let expected = (0..sy)
                .flat_map(|iy| (0..sx).map(move |ix| (ix, iy)))
                .map(|(ix, iy)| direct(ix, iy).conj() * grid[cell(ix, iy)])
                .sum::<Complex64>();
            let error = (gather_dense::<f64, true>(&grid, &block, ramp) - expected).norm();
            assert!(
                error <= 1e-12 * (sx * sy) as f64,
                "{support:?}: gather off by {error}"
            );
        }
        assert!(PointingRamp::new([0.0, 0.0], [0.25, -0.5], [7, 7]).is_none());
    }
}
