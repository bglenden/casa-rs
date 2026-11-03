// SPDX-License-Identifier: LGPL-3.0-or-later
//! Where a placement's taps land on the grid under its set's [`GridEdge`]:
//! wholly, on a box of cells only, or not at all, and whether its weight
//! counts in `sumwt` ([`Reach`]). Both backends grid by it.
//!
//! A set that drops a sample whose support overruns the grid
//! ([`GridEdge::Whole`]) grids every placement the resampler keeps wholly.
//! W projection's sectioned edge ([`GridEdge::Sectioned`]) clips the data
//! and the PSF to the same sectors as the mosaic's, and every sample with a
//! tap on them weighs, by the kernel sum of those taps
//! ([`clipped_kernel_sum`](crate::clipped_kernel_sum)).
//! The mosaic's clipped edge ([`GridEdge::Clipped`]) follows `MosaicFT::put`:
//! - the data and the PSF go through `sectgmosd2` over the sectors
//!   `FTMachine::findGridSector` cuts, which cover every row and column of
//!   the grid but its last (the last sector is cut one cell short of the
//!   1-based grid's end): a support is clipped to that extent, and its weight
//!   counts when its anchor cell lies in it (`onmosgrid`'s `centin`);
//! - the weight image goes through `gmoswgtd` over the whole grid: the
//!   weight taps are placed at the uv origin, but only those whose offsets
//!   the sample's own support keeps on the grid (`onmosgrid` at the sample's
//!   uv cell), and every sample that reaches the grid counts.
//!
//! Prediction (`sectdmos2`) keeps the whole support on the whole grid
//! (`omos`), [`GridGeometry::fits`].

use crate::accumulator::Mode;
use crate::convolution::{ConvolutionFunctionSet, GridEdge};
use crate::geometry::{CellLocation, GridGeometry};
use crate::sample::Placement;

/// Grid cells `[x0, x1) × [y0, y1)`, `[[x0, x1], [y0, y1]]`, in grid
/// coordinates.
pub type CellBox = [[i64; 2]; 2];

/// How one placement's taps of a mode land on the grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reach {
    /// Every tap lands on the grid, and the weight counts in `sumwt`.
    Whole,
    /// Only the taps on `cells` are gridded; the weight counts in `sumwt`
    /// when `weighs`.
    Clipped {
        /// The cells the taps may land on.
        cells: CellBox,
        /// Whether the weight counts in `sumwt`.
        weighs: bool,
    },
    /// Nothing is gridded.
    Off,
}

impl Reach {
    /// The reach of `placement`'s taps of `mode`, `half` taps each side of
    /// `location` (the uv origin for [`Mode::Weight`]), located at
    /// `oversampling` fine offsets a cell, under `cf`'s [`GridEdge`] on
    /// `geometry`'s grid.
    #[must_use]
    pub fn of(
        cf: &dyn ConvolutionFunctionSet,
        geometry: &GridGeometry,
        (mode, placement): (Mode, &Placement),
        location: CellLocation,
        (half, oversampling): ([u16; 2], u16),
    ) -> Self {
        if cf.grid_edge() == GridEdge::Whole {
            return if geometry.fits(location, half) {
                Self::Whole
            } else {
                Self::Off
            };
        }
        let [nx, ny] = geometry.grid_shape().map(|n| n as i64);
        match mode {
            Mode::Data | Mode::Psf => {
                let extent = [[0, nx - 1], [0, ny - 1]];
                let support = support_box(location, half);
                match overlap(support, extent) {
                    None => Self::Off,
                    Some(cells) if cells == support => Self::Whole,
                    Some(cells) => Self::Clipped {
                        cells,
                        weighs: cf.grid_edge() == GridEdge::Sectioned || inside(location, extent),
                    },
                }
            }
            Mode::Weight => {
                let sample = geometry.locate(placement.u, placement.v, oversampling);
                let support = support_box(sample, half);
                match overlap(support, [[0, nx], [0, ny]]) {
                    None => Self::Off,
                    Some(cells) if cells == support => Self::Whole,
                    Some(cells) => {
                        let shift = [location.x - sample.x, location.y - sample.y];
                        Self::Clipped {
                            cells: [0, 1].map(|axis| cells[axis].map(|edge| edge + shift[axis])),
                            weighs: true,
                        }
                    }
                }
            }
        }
    }
}

/// `Σ (conjugate ? conj(t) : t)` over the taps of Mueller plane `mueller`
/// at `location`'s fine offset that land on `cells`: what a clipped
/// placement of a [`GridEdge::Sectioned`] set adds to `sumwt` per unit
/// weight (its real part, `wprojgrid.f` `sectgwgridd`'s `norm`), where a
/// whole one adds the kernel sum over its support.
#[must_use]
pub fn clipped_kernel_sum(
    taps: &crate::TapLayout<'_>,
    location: CellLocation,
    cells: CellBox,
    mueller: u8,
    conjugate: bool,
) -> num_complex::Complex64 {
    crate::cpu::kernel::clipped_norm(taps, location, cells, mueller, conjugate)
}

/// The cells of a support of `half` taps each side of `location`.
fn support_box(location: CellLocation, half: [u16; 2]) -> CellBox {
    let [hx, hy] = half.map(i64::from);
    [
        [location.x - hx, location.x + hx + 1],
        [location.y - hy, location.y + hy + 1],
    ]
}

/// The cells `a` and `b` share, if any.
fn overlap(a: CellBox, b: CellBox) -> Option<CellBox> {
    let cells = [0, 1].map(|axis| [a[axis][0].max(b[axis][0]), a[axis][1].min(b[axis][1])]);
    cells.iter().all(|[low, high]| low < high).then_some(cells)
}

/// Whether `location`'s cell lies in `cells`.
fn inside(location: CellLocation, cells: CellBox) -> bool {
    (cells[0][0]..cells[0][1]).contains(&location.x)
        && (cells[1][0]..cells[1][1]).contains(&location.y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boxes_overlap_in_the_cells_they_share() {
        let a = [[-3, 4], [10, 17]];
        assert_eq!(overlap(a, [[0, 63], [0, 63]]), Some([[0, 4], [10, 17]]));
        assert_eq!(overlap(a, [[4, 63], [0, 63]]), None);
        assert!(inside(
            CellLocation {
                x: 0,
                y: 62,
                ox: 0,
                oy: 0
            },
            [[0, 63], [0, 63]]
        ));
        assert!(!inside(
            CellLocation {
                x: 63,
                y: 0,
                ox: 0,
                oy: 0
            },
            [[0, 63], [0, 63]]
        ));
    }
}
