// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convolution-function sets: tap layouts, Mueller routing and the paired
//! image-domain correction.

use num_complex::Complex32;

use crate::sample::CfKey;

/// Taps of one convolution-function cell in the layout the kernels read.
///
/// Fine-offset rows are indexed by [`CellLocation`](crate::CellLocation)'s
/// `ox`/`oy`, which run over `0..=oversampling`, so every layout holds
/// `oversampling + 1` rows per axis and `oversampling` must be even.
#[derive(Clone, Copy, Debug)]
pub enum TapLayout<'a> {
    /// Separable real kernel (standard spheroidal). `rows[o * support + i]`
    /// is tap `i`, at grid offset `i − support/2` from the anchor, for fine
    /// offset row `o`; one fractional offset is one contiguous row
    /// (oversampling-major, Obit `ConvFunc`).
    SeparableReal {
        /// `(oversampling + 1) × support` tap values.
        rows: &'a [f32],
        /// Taps per axis (odd).
        support: u16,
        /// Fine offsets per cell (even).
        oversampling: u16,
    },
    /// Dense complex kernel (W, AW, mosaic) ordered
    /// `[oy][ox][mueller][iy][ix]`, x fastest, with one contiguous
    /// `mueller × sy × sx` tile per fractional offset. Tap `(ix, iy)` sits
    /// at grid offset `(ix − sx/2, iy − sy/2)` from the anchor.
    Dense {
        /// `(oversampling + 1)² × mueller_planes × sy × sx` tap values.
        data: &'a [Complex32],
        /// Taps per axis `[sx, sy]` (odd).
        support: [u16; 2],
        /// Fine offsets per cell (even).
        oversampling: u16,
        /// Mueller planes per tile; 1 for a scalar kernel.
        mueller_planes: u8,
    },
}

impl TapLayout<'_> {
    /// Taps on each side of the anchor, `[x, y]`.
    #[must_use]
    pub fn half_support(&self) -> [u16; 2] {
        match self {
            Self::SeparableReal { support, .. } => [support / 2, support / 2],
            Self::Dense { support, .. } => [support[0] / 2, support[1] / 2],
        }
    }

    /// Fine offsets per cell.
    #[must_use]
    pub fn oversampling(&self) -> u16 {
        match self {
            Self::SeparableReal { oversampling, .. } | Self::Dense { oversampling, .. } => {
                *oversampling
            }
        }
    }

    /// Mueller planes per cell.
    #[must_use]
    pub fn mueller_planes(&self) -> u8 {
        match self {
            Self::SeparableReal { .. } => 1,
            Self::Dense { mueller_planes, .. } => *mueller_planes,
        }
    }
}

/// Which kernel Mueller plane serves each (grid polarization, visibility
/// polarization) pair.
///
/// `direct` is the adjoint table for `w > 0`; `conjugate` its partner for
/// `w ≤ 0`. The kernel swaps the tables on the sign of `w` and on the
/// direction of the transform and conjugates the taps (HPG
/// `mueller_indexes` / `conjugate_mueller_indexes`). `None` skips a pair.
/// Both tables are `[grid pol][visibility pol]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MuellerRouting {
    /// Adjoint table for `w > 0`.
    pub direct: Vec<Vec<Option<u8>>>,
    /// Adjoint table for `w ≤ 0`.
    pub conjugate: Vec<Vec<Option<u8>>>,
}

impl MuellerRouting {
    /// Scalar routing: Mueller plane 0 for every pair `pol_map` joins
    /// (visibility polarization `p` feeds grid polarization `pol_map[p]`),
    /// with identical direct and conjugate tables.
    #[must_use]
    pub fn scalar(pol_map: &[Option<u8>], grid_pols: usize) -> Self {
        let direct = (0..grid_pols)
            .map(|gpol| {
                pol_map
                    .iter()
                    .map(|target| (*target == Some(gpol as u8)).then_some(0))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Self {
            conjugate: direct.clone(),
            direct,
        }
    }

    /// Number of grid polarizations.
    #[must_use]
    pub fn grid_pols(&self) -> usize {
        self.direct.len()
    }

    /// Number of visibility polarizations.
    #[must_use]
    pub fn visibility_pols(&self) -> usize {
        self.direct.first().map_or(0, Vec::len)
    }

    /// The table for one transform: the adjoint uses `direct` for `w > 0`,
    /// the forward transform swaps them.
    #[must_use]
    pub fn table(&self, w_positive: bool, forward: bool) -> &[Vec<Option<u8>>] {
        if w_positive != forward {
            &self.direct
        } else {
            &self.conjugate
        }
    }
}

/// Per-row context a kernel set keys its cells on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RowContext {
    /// Row time in seconds (MeasurementSet `TIME`).
    pub time_s: f64,
    /// Antenna pair.
    pub antennas: [u32; 2],
    /// Parallactic angle of each antenna in radians.
    pub parallactic_angle_rad: [f64; 2],
    /// Field (pointing) identifier.
    pub field: u32,
    /// Pointing offset from the image phase centre in radians `[Δl, Δm]`.
    pub pointing_offset_rad: [f64; 2],
}

/// Paired image-domain gridding correction: one real vector per grid axis,
/// applied as `x[gx] · y[gy]` on both sides of the operator (CASA divides
/// the model and the dirty image by the same spheroidal response).
#[derive(Clone, Debug, PartialEq)]
pub struct ImageCorrection {
    x: Box<[f64]>,
    y: Box<[f64]>,
}

impl ImageCorrection {
    /// Correction vectors over the padded grid axes.
    #[must_use]
    pub fn new(x: Vec<f64>, y: Vec<f64>) -> Self {
        Self {
            x: x.into_boxed_slice(),
            y: y.into_boxed_slice(),
        }
    }

    /// Correction along the grid x axis.
    #[must_use]
    pub fn x(&self) -> &[f64] {
        &self.x
    }

    /// Correction along the grid y axis.
    #[must_use]
    pub fn y(&self) -> &[f64] {
        &self.y
    }

    /// Correction at grid cell `(gx, gy)`.
    #[must_use]
    pub fn at(&self, gx: usize, gy: usize) -> f64 {
        self.x[gx] * self.y[gy]
    }
}

/// A kernel set: pure cell selection plus the taps, routing and correction
/// the kernels read. Implementations: the standard spheroidal set, W-planes,
/// the AW catalog and the mosaic primary beam.
pub trait ConvolutionFunctionSet: Send + Sync {
    /// Cell for one row at `freq_hz` and `w_lambda`. Pure; CASA-pinned
    /// rounding rules for w-planes, frequency and parallactic-angle cells
    /// live here.
    fn key(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> CfKey;

    /// Imaging taps of a cell.
    fn taps(&self, key: CfKey) -> TapLayout<'_>;

    /// `FT[PB²]` taps for the weight (sensitivity) image, placed at the uv
    /// origin; `None` for sets without a weight image.
    fn weight_taps(&self, key: CfKey) -> Option<TapLayout<'_>>;

    /// Mueller plane routing for every cell of the set.
    fn mueller(&self) -> &MuellerRouting;

    /// The paired image-domain correction.
    fn image_correction(&self) -> &ImageCorrection;
}
