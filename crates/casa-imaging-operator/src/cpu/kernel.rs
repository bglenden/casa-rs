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
            ..
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
            ..
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
            norms,
            oversampling,
            mueller_planes,
            ..
        } => {
            let sum = norms[dense_tile_index(oversampling, mueller_planes, location, mueller)];
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
    let taps = usize::from(support[0]) * usize::from(support[1]);
    let offset = dense_tile_index(oversampling, mueller_planes, location, mueller);
    &data[offset * taps..][..taps]
}

/// The index of the tile of Mueller plane `mueller` at the sample's fine
/// offset among a dense cell's tiles, and of its sum among the cell's
/// norms ([`DenseCell::new`](crate::convolution::DenseCell::new) sums them).
fn dense_tile_index(
    oversampling: u16,
    mueller_planes: u8,
    location: CellLocation,
    mueller: u8,
) -> usize {
    debug_assert!(mueller < mueller_planes, "Mueller plane outside the cell");
    let rows = usize::from(oversampling) + 1;
    (usize::from(location.oy) * rows + usize::from(location.ox)) * usize::from(mueller_planes)
        + usize::from(mueller)
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

/// Taps per row segment, over which the ramp's x factors are tabulated
/// ([`PointingRamp`]). Shorter tables cost less to fill per sample and
/// longer segments less per row: 32 is within a few percent of the best
/// of 16, 32 and 64 from 7 to 101 taps (#449).
const RAMP_SEGMENT: usize = 32;

/// The pointing ramp `e^{i(k_x g_x + k_y g_y)}` over a support, with
/// `k = i − ⌊s/2⌋ + f` per axis at the sample's fine fraction `f`
/// ([`fine_fraction`]). The ramp is separable, `e^{i k_x g_x} · e^{i k_y
/// g_y}`, so it is never evaluated per tap. CASA's resampler instead
/// computes its phase-gradient array once per pointing
/// (`cachePhaseGrad`). Three phasors are evaluated per sample, at the
/// support's first tap, one tap along x and one row along y, plus a
/// fourth for supports wider than [`RAMP_SEGMENT`]: the step from one
/// row segment to the next.
///
/// The loops tabulate `step_x^j` for the taps `j` of a segment once per
/// sample ([`RampSegment`]). Each row starts one row step on from the
/// last, each of its segments one segment step on, and a tap takes its
/// segment's start times its table entry. A tap's ramp is then two
/// multiplies that depend on no other tap. Walking the ramp tap by tap
/// instead chained each tap's multiply to the one before, and that chain
/// bounded the loops: they ran 2–3× slower from 29 taps up (#449).
///
/// The doubling shortens the dependency chain, not the error: squaring
/// doubles the relative error already in a power, and the step's own
/// rounding enters `step_x^j` `j` times whatever the method. So a tap's
/// factor carries about 2ε per tap of its place in the segment, as the walk
/// did, plus about 2ε per segment and per row before it, besides the error
/// of the three phasors themselves (#771 review: 13ε at `j = 31` by
/// doubling against 12.5ε by the walk). That is a few 1e-15 over a 31-tap
/// support, and about 1e-12 even 4,096 rows along. It is an estimate of
/// the drift, not a bound for arbitrary supports; the direct phasor's own
/// argument error grows with `|k·g|` instead.
#[derive(Clone, Copy)]
struct PointingRamp {
    /// The ramp at the first tap of the current row.
    row: Complex64,
    /// The ramp's factor from one tap to the next along x, and from one
    /// row to the next along y.
    step: [Complex64; 2],
    /// The factor from one row segment's first tap to the next's,
    /// `e^{i g_x · RAMP_SEGMENT}`; one when a row is a single segment.
    segment: Complex64,
}

impl PointingRamp {
    /// The ramp of a kernel without one, which the unramped loops ignore.
    const NONE: Self = Self {
        row: Complex64::new(1.0, 0.0),
        step: [Complex64::new(1.0, 0.0); 2],
        segment: Complex64::new(1.0, 0.0),
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
        let segment = if support[0] > RAMP_SEGMENT {
            Complex64::from_polar(1.0, gx * RAMP_SEGMENT as f64)
        } else {
            Complex64::new(1.0, 0.0)
        };
        Some(Self {
            row: Complex64::from_polar(1.0, kx * gx + ky * gy),
            step: [
                Complex64::from_polar(1.0, gx),
                Complex64::from_polar(1.0, gy),
            ],
            segment,
        })
    }
}

/// The ramp's x factors over one row segment: `step_x^j` for its first
/// `width` taps `j`, at most [`RAMP_SEGMENT`] ([`PointingRamp`]).
struct RampSegment {
    factors: [Complex64; RAMP_SEGMENT],
}

impl RampSegment {
    /// The factors by doubling: those filled so far times the next power
    /// `step_x^{2^k}`, so the fill is `log₂ width` multiplies deep rather
    /// than `width`, which a small support's sample would wait on. Its
    /// error is the walk's ([`PointingRamp`]).
    fn new(ramp: &PointingRamp, width: usize) -> Self {
        let width = width.min(RAMP_SEGMENT);
        let mut factors = [Complex64::new(1.0, 0.0); RAMP_SEGMENT];
        let (mut filled, mut power) = (1, ramp.step[0]);
        while filled < width {
            let more = filled.min(width - filled);
            for j in 0..more {
                factors[filled + j] = factors[j] * power;
            }
            filled += more;
            power *= power;
        }
        Self { factors }
    }
}

/// A dense tap as stored, conjugated when `conjugate`, in `f64`.
fn oriented(tap: Complex32, conjugate: bool) -> Complex64 {
    let tap = Complex64::new(f64::from(tap.re), f64::from(tap.im));
    if conjugate { tap.conj() } else { tap }
}

/// Add `value · tap'` over `block`, each tap times `ramp` when `RAMPED`:
/// a row segment's value carries the ramp at its first tap, and each tap
/// its table factor ([`PointingRamp`]).
fn spread_dense<T: GridScalar, const RAMPED: bool>(
    grid: &mut [Complex<T>],
    block: &DenseBlock<'_>,
    mut ramp: PointingRamp,
    value: Complex<T>,
) {
    let ([x0, y0], [sx, sy]) = (block.origin, block.support);
    let table = RAMPED.then(|| RampSegment::new(&ramp, sx));
    let wide = Complex64::new(value.re.into_f64(), value.im.into_f64());
    for iy in 0..sy {
        let row = &mut grid[(y0 + iy) * block.nx + x0..][..sx];
        let taps = &block.taps[iy * sx..][..sx];
        let Some(table) = &table else {
            for (cell, tap) in row.iter_mut().zip(taps) {
                let tap = oriented(*tap, block.conjugate);
                *cell = *cell + value * Complex::new(T::from_f64(tap.re), T::from_f64(tap.im));
            }
            continue;
        };
        let mut start = wide * ramp.row;
        for (cells, taps) in row.chunks_mut(RAMP_SEGMENT).zip(taps.chunks(RAMP_SEGMENT)) {
            for ((cell, tap), factor) in cells.iter_mut().zip(taps).zip(&table.factors) {
                let added = start * (oriented(*tap, block.conjugate) * *factor);
                *cell = *cell + Complex::new(T::from_f64(added.re), T::from_f64(added.im));
            }
            start *= ramp.segment;
        }
        ramp.row *= ramp.step[1];
    }
}

/// `Σ conj(tap') · grid` over `block`, each tap times `ramp` when `RAMPED`:
/// each row segment sums its taps times their table factors, then takes
/// the ramp at its first tap ([`PointingRamp`]).
fn gather_dense<T: GridScalar, const RAMPED: bool>(
    grid: &[Complex<T>],
    block: &DenseBlock<'_>,
    mut ramp: PointingRamp,
) -> Complex64 {
    let ([x0, y0], [sx, sy]) = (block.origin, block.support);
    let table = RAMPED.then(|| RampSegment::new(&ramp, sx));
    let mut sum = Complex64::default();
    for iy in 0..sy {
        let row = &grid[(y0 + iy) * block.nx + x0..][..sx];
        let taps = &block.taps[iy * sx..][..sx];
        let Some(table) = &table else {
            for (cell, tap) in row.iter().zip(taps) {
                let tap = oriented(*tap, block.conjugate);
                sum += tap.conj() * Complex64::new(cell.re.into_f64(), cell.im.into_f64());
            }
            continue;
        };
        let mut start = ramp.row;
        for (cells, taps) in row.chunks(RAMP_SEGMENT).zip(taps.chunks(RAMP_SEGMENT)) {
            let mut segment = Complex64::default();
            for ((cell, tap), factor) in cells.iter().zip(taps).zip(&table.factors) {
                let tap = oriented(*tap, block.conjugate) * *factor;
                segment += tap.conj() * Complex64::new(cell.re.into_f64(), cell.im.into_f64());
            }
            sum += start.conj() * segment;
            start *= ramp.segment;
        }
        ramp.row *= ramp.step[1];
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convolution::DenseCell;

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

    /// One case of [`the_tabulated_pointing_ramp_equals_the_ramp_at_each_tap`]:
    /// a dense kernel of `support` taps spread and gathered through
    /// [`spread`] and [`gather`] at fine offsets `offsets`, against the ramp
    /// evaluated at each tap, the conjugation applied by sign rather than
    /// through [`oriented`]. A zero gradient must be the unramped arithmetic
    /// exactly; otherwise each spread cell is within `spread_tolerance`
    /// and the gather within 1e-12 per tap.
    fn ramp_case<T: GridScalar>(
        (support, gradient, offsets, conjugate): ([usize; 2], [f32; 2], [u16; 2], bool),
        spread_tolerance: f64,
    ) {
        let oversampling = 4_u16;
        let [sx, sy] = support;
        let tiles = (usize::from(oversampling) + 1).pow(2);
        let data = (0..tiles * sx * sy)
            .map(|i| Complex32::new(noise(3 * i) as f32, noise(3 * i + 1) as f32))
            .collect::<Vec<_>>();
        let dense = DenseCell::new(
            data.clone().into_boxed_slice(),
            support.map(|s| u16::try_from(s).expect("support fits u16")),
            oversampling,
            1,
        );
        let taps = dense.layout();
        let tile = Tile {
            origin: [0, 0],
            shape: [sx + 4, sy + 6],
        };
        let location = CellLocation {
            x: (sx / 2 + 1) as i64,
            y: (sy / 2 + 2) as i64,
            ox: offsets[0],
            oy: offsets[1],
        };
        let cell = |ix: usize, iy: usize| (2 + iy) * tile.shape[0] + 1 + ix;
        // The offset's tile, x fastest, and its fractions (o − s/2)/s.
        let first = (usize::from(offsets[1]) * (usize::from(oversampling) + 1)
            + usize::from(offsets[0]))
            * sx
            * sy;
        let [fx, fy] = offsets
            .map(|o| (f64::from(o) - f64::from(oversampling) / 2.0) / f64::from(oversampling));
        let direct = |ix: usize, iy: usize| {
            let stored = data[first + iy * sx + ix];
            let sign = if conjugate { -1.0 } else { 1.0 };
            let tap = Complex64::new(f64::from(stored.re), sign * f64::from(stored.im));
            let kx = ix as f64 - (sx / 2) as f64 + fx;
            let ky = iy as f64 - (sy / 2) as f64 + fy;
            let phase = kx * f64::from(gradient[0]) + ky * f64::from(gradient[1]);
            if gradient == [0.0, 0.0] {
                tap
            } else {
                tap * Complex64::from_polar(1.0, phase)
            }
        };
        let label = format!("{support:?} {gradient:?} {offsets:?} {conjugate}");

        let value = Complex::new(T::from_f64(0.6), T::from_f64(-1.3));
        let mut spread_grid = vec![Complex::<T>::default(); tile.shape[0] * tile.shape[1]];
        spread::<T>(
            &mut spread_grid,
            tile,
            location,
            &taps,
            0,
            conjugate,
            gradient,
            value,
        );
        for (ix, iy) in (0..sy).flat_map(|iy| (0..sx).map(move |ix| (ix, iy))) {
            let got = spread_grid[cell(ix, iy)];
            let tap = direct(ix, iy);
            if gradient == [0.0, 0.0] {
                let unramped = value * Complex::new(T::from_f64(tap.re), T::from_f64(tap.im));
                assert!(got == unramped, "{label}: spread tap ({ix}, {iy}) changed");
            } else {
                let value = Complex64::new(value.re.into_f64(), value.im.into_f64());
                let got = Complex64::new(got.re.into_f64(), got.im.into_f64());
                let error = (got - value * tap).norm();
                assert!(
                    error <= spread_tolerance,
                    "{label}: spread tap ({ix}, {iy}) off by {error}"
                );
            }
        }

        let grid = (0..tile.shape[0] * tile.shape[1])
            .map(|i| Complex::new(T::from_f64(noise(7 * i)), T::from_f64(noise(7 * i + 1))))
            .collect::<Vec<_>>();
        let expected = (0..sy)
            .flat_map(|iy| (0..sx).map(move |ix| (ix, iy)))
            .map(|(ix, iy)| {
                let cell = grid[cell(ix, iy)];
                direct(ix, iy).conj() * Complex64::new(cell.re.into_f64(), cell.im.into_f64())
            })
            .fold(Complex64::default(), |sum, term| sum + term);
        let got = gather::<T>(&grid, tile, location, &taps, 0, conjugate, gradient);
        if gradient == [0.0, 0.0] {
            assert!(got == expected, "{label}: gather changed");
        } else {
            let error = (got - expected).norm();
            assert!(
                error <= 1e-12 * (sx * sy) as f64,
                "{label}: gather off by {error}"
            );
        }
    }

    /// The tabulated pointing ramp spreads and gathers what the ramp
    /// evaluated at each tap does, through [`spread`] and [`gather`] (#732,
    /// #449): odd supports of unequal sides up to 129 × 65, rows of one
    /// segment, one tap past it, and several segments
    /// ([`RAMP_SEGMENT`]), gradients beyond π per cell and zero on one
    /// axis, fine fractions either side of the cell, both conjugations and
    /// both grid precisions. A zero gradient takes the unramped loop, bit
    /// for bit.
    #[test]
    fn the_tabulated_pointing_ramp_equals_the_ramp_at_each_tap() {
        let cases = [
            ([7, 7], [0.37_f32, -0.21], [1_u16, 3], false),
            ([5, 11], [2.9, 4.6], [0, 4], true),
            ([31, 23], [-3.7, 0.05], [4, 1], false),
            ([33, 3], [1.7, -0.4], [3, 2], true),
            ([65, 9], [-2.6, 3.3], [0, 1], false),
            ([129, 65], [0.9, -2.2], [2, 2], true),
            ([9, 5], [0.0, 1.3], [3, 0], true),
            ([9, 5], [0.0, 0.0], [3, 0], true),
            ([9, 5], [0.0, 0.0], [1, 4], false),
        ];
        for case in cases {
            ramp_case::<f64>(case, 1e-12);
            ramp_case::<f32>(case, 1e-6);
        }
        assert!(PointingRamp::new([0.0, 0.0], [0.25, -0.5], [7, 7]).is_none());
        assert!(PointingRamp::new([-0.0, 0.0], [0.25, -0.5], [7, 7]).is_none());
    }

    /// A dense cell's norm read from its tiles' sums (#449) is the sum of
    /// the sample's tile in `f64` and tap order, conjugated or not, bit
    /// for bit: at every fine offset of two Mueller planes of an unequal
    /// support, the tile found from the layout's index rule.
    #[test]
    fn a_stored_dense_norm_is_the_sum_of_its_tile() {
        let (oversampling, mueller_planes, support) = (4_u16, 2_u8, [3_u16, 5]);
        let taps = usize::from(support[0]) * usize::from(support[1]);
        let rows = usize::from(oversampling) + 1;
        let data = (0..rows * rows * usize::from(mueller_planes) * taps)
            .map(|i| Complex32::new(noise(5 * i) as f32, noise(5 * i + 2) as f32))
            .collect::<Vec<_>>();
        let dense = DenseCell::new(
            data.clone().into_boxed_slice(),
            support,
            oversampling,
            mueller_planes,
        );
        let layout = dense.layout();
        for (oy, ox) in (0..rows).flat_map(|oy| (0..rows).map(move |ox| (oy, ox))) {
            let location = CellLocation {
                x: 0,
                y: 0,
                ox: ox as u16,
                oy: oy as u16,
            };
            for mueller in 0..mueller_planes {
                let first =
                    ((oy * rows + ox) * usize::from(mueller_planes) + usize::from(mueller)) * taps;
                let summed = data[first..][..taps]
                    .iter()
                    .map(|tap| Complex64::new(f64::from(tap.re), f64::from(tap.im)))
                    .sum::<Complex64>();
                for conjugate in [false, true] {
                    let expected = if conjugate { summed.conj() } else { summed };
                    let stored = norm(&layout, location, mueller, conjugate);
                    assert_eq!(
                        (stored.re.to_bits(), stored.im.to_bits()),
                        (expected.re.to_bits(), expected.im.to_bits()),
                        "offset ({ox}, {oy}), plane {mueller}, conjugate {conjugate}"
                    );
                }
            }
        }
    }
}
