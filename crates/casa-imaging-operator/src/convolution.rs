// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convolution-function sets: tap layouts, Mueller routing, the paired
//! image-domain correction and the per-set normalisation rule.

use std::sync::Arc;

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

/// Dense taps of one cell in the [`TapLayout::Dense`] order, owned so a
/// bounded cache can hand them out while it evicts others.
#[derive(Clone, Debug, PartialEq)]
pub struct DenseCell {
    /// `(oversampling + 1)² × mueller_planes × sy × sx` tap values.
    pub data: Box<[Complex32]>,
    /// Taps per axis `[sx, sy]` (odd).
    pub support: [u16; 2],
    /// Fine offsets per cell (even).
    pub oversampling: u16,
    /// Mueller planes per tile.
    pub mueller_planes: u8,
}

impl DenseCell {
    /// The cell as a tap layout.
    #[must_use]
    pub fn layout(&self) -> TapLayout<'_> {
        TapLayout::Dense {
            data: &self.data,
            support: self.support,
            oversampling: self.oversampling,
            mueller_planes: self.mueller_planes,
        }
    }

    /// Bytes of the tap values.
    #[must_use]
    pub fn bytes(&self) -> usize {
        std::mem::size_of_val(self.data.as_ref())
    }
}

/// A caller-owned slot that keeps one cell of a bounded cache alive while
/// the taps borrowed from it are in use.
///
/// Resident sets ignore it and lend their own storage. A set whose cells
/// come and go ([`AwCatalog`](crate::AwCatalog)) parks the cell here, so
/// the cache may drop its own reference without invalidating the borrow,
/// and a worker holds at most one cell per slot beyond the cache's bound.
#[derive(Clone, Debug, Default)]
pub struct CellHold(Option<Arc<DenseCell>>);

impl CellHold {
    /// An empty slot.
    #[must_use]
    pub const fn new() -> Self {
        Self(None)
    }

    /// Park `cell` and lend its taps.
    pub fn lend(&mut self, cell: Arc<DenseCell>) -> TapLayout<'_> {
        self.0.insert(cell).layout()
    }

    /// Release the parked cell.
    pub fn clear(&mut self) {
        self.0 = None;
    }
}

/// How a set's kernels are normalised: what a prediction divides by and
/// what `sumwt` accumulates, with `N` the sum of the w-conjugated taps a
/// visibility polarization uses at its fine offset, without the pointing
/// ramp (plan section 5.3; R1 on #650). CASA's machines differ, so the
/// rule is a property of the set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KernelNormalisation {
    /// Unit-sum taps: predictions are the gathered sums and `sumwt += W`
    /// (CASA `GridFT`, `MosaicFT`: `fgridft.f`, `fmosaic.f` add the weight
    /// without a kernel sum).
    UnitSum,
    /// Predictions are the gathered sums and `sumwt += W · Re N` (CASA
    /// `WProjectFT`: `wprojgrid.f` accumulates `norm += real(cwt)` and adds
    /// `weight · norm`; `dwgrid` does not divide).
    RealSum,
    /// A prediction divides once by `N` summed over its routed Mueller
    /// planes and `sumwt += W · |N|` (CASA `AWVisResampler::DataToGrid`,
    /// `GridToData` and `faccumulateFromGrid.f`).
    KernelSum,
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
    /// Antenna type of each antenna: an index into the kernel set's
    /// antenna classes (CASA `HetArrayConvFunc` dish classes, AW baseline
    /// types); 0 for a homogeneous array.
    pub antenna_types: [u8; 2],
    /// CASA's visibility polarization operator angle of each antenna in
    /// radians: the negative of the physical parallactic angle.
    pub parallactic_angle_rad: [f64; 2],
    /// Field (pointing) identifier.
    pub field: u32,
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

    /// Imaging taps of a cell. A set that pages cells parks the cell in
    /// `hold` and lends from it; resident sets lend their own storage.
    fn taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s>;

    /// The largest [`TapLayout::half_support`] of any cell `key` can
    /// return, `[x, y]`: the halo a tile needs so that every sample anchored
    /// inside it keeps its whole support in the tile.
    fn max_half_support(&self) -> [u16; 2];

    /// `FT[PB²]` taps for the weight (sensitivity) image, placed at the uv
    /// origin; `None` for sets without a weight image.
    fn weight_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> Option<TapLayout<'s>>;

    /// Mueller plane routing for every cell of the set.
    fn mueller(&self) -> &MuellerRouting;

    /// The paired image-domain correction.
    fn image_correction(&self) -> &ImageCorrection;

    /// The normalisation rule of the set's kernels.
    fn normalisation(&self) -> KernelNormalisation;

    /// Whether placements carry the pointing phase gradient
    /// (`Placement::gradient`): the kernels encode a pointing offset from
    /// the image centre as `e^{i(k·g)}` over the taps (mosaic, AW).
    fn pointing_ramp(&self) -> bool;
}
