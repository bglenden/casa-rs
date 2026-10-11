// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convolution-function sets: tap layouts, Mueller routing, the paired
//! image-domain correction and the per-set normalisation rule.

use std::sync::Arc;

use num_complex::{Complex32, Complex64};

use crate::geometry::CellLocation;
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

    /// The kernel norm `Σ (conjugate ? conj(t) : t)` of Mueller plane
    /// `mueller` at the fine offsets of `location`, summed in `f64` without
    /// the pointing ramp: the forward normalisation every backend divides
    /// by and whose magnitude `sumwt` accumulates (CASA
    /// `AWVisResampler::faccumulateFromGrid`).
    #[must_use]
    pub fn norm(&self, location: CellLocation, mueller: u8, conjugate: bool) -> Complex64 {
        crate::cpu::kernel::norm(self, location, mueller, conjugate)
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
pub struct CellHold {
    cell: Option<Arc<DenseCell>>,
    /// The cell's key and the set's slot (imaging, weight, prediction cell)
    /// when parked through [`Self::lend_keyed`]; a repeated key lends the
    /// parked cell again without touching the set's cache.
    keyed: Option<(CfKey, u8)>,
}

impl CellHold {
    /// An empty slot.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cell: None,
            keyed: None,
        }
    }

    /// Park `cell` and lend its taps.
    pub fn lend(&mut self, cell: Arc<DenseCell>) -> TapLayout<'_> {
        self.keyed = None;
        self.cell.insert(cell).layout()
    }

    /// Lend the taps of the cell parked for `key` in the set's `slot` (the
    /// set numbers its kinds of cell), fetching it with `load` when another
    /// cell is parked: rows of one key in sequence cost one fetch.
    pub fn lend_keyed(
        &mut self,
        key: CfKey,
        slot: u8,
        load: impl FnOnce() -> Arc<DenseCell>,
    ) -> TapLayout<'_> {
        if self.keyed != Some((key, slot)) || self.cell.is_none() {
            self.cell = Some(load());
            self.keyed = Some((key, slot));
        }
        self.cell.as_ref().expect("a parked cell").layout()
    }

    /// Release the parked cell.
    pub fn clear(&mut self) {
        self.cell = None;
        self.keyed = None;
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
/// polarization) pair, per direction of the transform and sign of `w`.
///
/// Four `[grid pol][visibility pol]` tables; `None` skips a pair. CASA
/// routes the two directions differently (`AWVisResampler`):
/// `getConvFunc_p` selects the cell by the sign of `w` (`mNdx` for
/// `w > 0`, `conjMNdx` otherwise; `GridToData` passes the two swapped),
/// then `DataToGrid` reads the visibility of the selected cell's own hand
/// (`muellerElement % nDataPol`) into the outer grid polarization, while
/// `GridToData` reads the model grid of that hand into the outer
/// visibility polarization. A forward table is therefore not a swap of an
/// adjoint one. Tap conjugation is separate from the routing: adjoint
/// taps are conjugated for `w > 0`, forward taps for `w ≤ 0`
/// (`accumulateToGrid.inc`, `accumulateFromGrid.inc`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MuellerRouting {
    /// Adjoint tables, indexed by `usize::from(w > 0)`.
    pub adjoint: [Vec<Vec<Option<u8>>>; 2],
    /// Forward tables, indexed by `usize::from(w > 0)`.
    pub forward: [Vec<Vec<Option<u8>>>; 2],
}

impl MuellerRouting {
    /// Scalar routing: Mueller plane 0 for every pair `pol_map` joins
    /// (visibility polarization `p` feeds grid polarization `pol_map[p]`),
    /// with identical tables for both directions and signs of w.
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
            adjoint: [direct.clone(), direct.clone()],
            forward: [direct.clone(), direct],
        }
    }

    /// Bytes a routing of at most four correlations and four grid
    /// polarizations holds at most.
    pub const MAXIMUM_BYTES: u64 =
        4 * (4 * size_of::<Vec<Option<u8>>>() as u64 + 16 * size_of::<Option<u8>>() as u64);

    /// Bytes the routing's tables hold, from their capacities.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.adjoint
            .iter()
            .chain(&self.forward)
            .map(|table| {
                table.capacity() * size_of::<Vec<Option<u8>>>()
                    + table
                        .iter()
                        .map(|row| row.capacity() * size_of::<Option<u8>>())
                        .sum::<usize>()
            })
            .sum::<usize>() as u64
    }

    /// Number of grid polarizations.
    #[must_use]
    pub fn grid_pols(&self) -> usize {
        self.adjoint[0].len()
    }

    /// Number of visibility polarizations.
    #[must_use]
    pub fn visibility_pols(&self) -> usize {
        self.adjoint[0].first().map_or(0, Vec::len)
    }

    /// The complete routing for one transform and sign of w.
    #[must_use]
    pub fn table(&self, w_positive: bool, forward: bool) -> &[Vec<Option<u8>>] {
        if forward {
            &self.forward[usize::from(w_positive)]
        } else {
            &self.adjoint[usize::from(w_positive)]
        }
    }
}

/// Per-row context a kernel set keys its cells on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RowContext {
    /// The row's MeasurementSet `UVW` w in metres before the phase-centre
    /// rotation, when the source rotated the row; `None` when the row's
    /// uvw are the MeasurementSet's. The AW set keys its prediction cell
    /// and parity on it ([`ConvolutionFunctionSet::prediction_w`]).
    pub original_w_m: Option<f64>,
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
    /// Spectral window of the row's data description; kernel sets with
    /// per-window frequency cells key on it.
    pub spectral_window: u32,
}

/// Image-domain gridding correction: one real vector per grid axis on each
/// side of the operator, applied as `x[gx] · y[gy]` to the image the
/// adjoint forms ([`Self::at`]) and to the model the forward transform
/// reads ([`Self::model_at`]).
///
/// CASA's `GridFT` divides the model and the dirty image by the same
/// response ([`Self::new`]); `MosaicFT` (`prepGridForDegrid`, `getImage`)
/// and `WProjectFT` (`initializeToVis`, `getImage`) multiply the model by
/// their sinc and divide the image by it, so the sides differ
/// ([`Self::split`]).
#[derive(Clone, Debug, PartialEq)]
pub struct ImageCorrection {
    x: Box<[f64]>,
    y: Box<[f64]>,
    model_x: Box<[f64]>,
    model_y: Box<[f64]>,
}

impl ImageCorrection {
    /// The same correction vectors on both sides, over the padded grid
    /// axes.
    #[must_use]
    pub fn new(x: Vec<f64>, y: Vec<f64>) -> Self {
        Self {
            model_x: x.clone().into_boxed_slice(),
            model_y: y.clone().into_boxed_slice(),
            x: x.into_boxed_slice(),
            y: y.into_boxed_slice(),
        }
    }

    /// Different vectors for the image side (`x`, `y`) and the model side
    /// (`model_x`, `model_y`).
    #[must_use]
    pub fn split(x: Vec<f64>, y: Vec<f64>, model_x: Vec<f64>, model_y: Vec<f64>) -> Self {
        Self {
            x: x.into_boxed_slice(),
            y: y.into_boxed_slice(),
            model_x: model_x.into_boxed_slice(),
            model_y: model_y.into_boxed_slice(),
        }
    }

    /// Bytes the four correction vectors of a grid of `grid` hold.
    #[must_use]
    pub const fn bytes(grid: [usize; 2]) -> u64 {
        (2 * (grid[0] + grid[1]) * size_of::<f64>()) as u64
    }

    /// Bytes the four correction vectors hold.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        ((self.x.len() + self.y.len() + self.model_x.len() + self.model_y.len()) * size_of::<f64>())
            as u64
    }

    /// Image-side correction along the grid x axis.
    #[must_use]
    pub fn x(&self) -> &[f64] {
        &self.x
    }

    /// Image-side correction along the grid y axis.
    #[must_use]
    pub fn y(&self) -> &[f64] {
        &self.y
    }

    /// Model-side correction along the grid x axis.
    #[must_use]
    pub fn model_x(&self) -> &[f64] {
        &self.model_x
    }

    /// Model-side correction along the grid y axis.
    #[must_use]
    pub fn model_y(&self) -> &[f64] {
        &self.model_y
    }

    /// Image-side correction at grid cell `(gx, gy)`.
    #[must_use]
    pub fn at(&self, gx: usize, gy: usize) -> f64 {
        self.x[gx] * self.y[gy]
    }

    /// Model-side correction at grid cell `(gx, gy)`.
    #[must_use]
    pub fn model_at(&self, gx: usize, gy: usize) -> f64 {
        self.model_x[gx] * self.model_y[gy]
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

    /// The w in wavelengths a prediction of `row`'s sample at `freq_hz`
    /// keys its cell and tap conjugation on: the image-frame `w_lambda` the
    /// gridding uses, for every set but AW. `AWVisResampler::GridToData`
    /// reads the MeasurementSet w before the phase-centre rotation
    /// (`vb_p->uvw()(2, irow)`) for the w-plane and the sign, while its u,
    /// v and phasor are the rotated ones (`sgrid` on `uvw_p`);
    /// `DataToGrid` reads the rotated w for both.
    fn prediction_w(&self, _row: &RowContext, _freq_hz: f64, w_lambda: f64) -> f64 {
        w_lambda
    }

    /// The paired image-domain correction.
    fn image_correction(&self) -> &ImageCorrection;

    /// The normalisation rule of the set's kernels.
    fn normalisation(&self) -> KernelNormalisation;

    /// Whether placements carry the pointing phase gradient
    /// (`Placement::gradient`): the kernels encode a pointing offset from
    /// the image centre as `e^{i(k·g)}` over the taps (mosaic, AW).
    fn pointing_ramp(&self) -> bool;

    /// The taps `Mode::Psf` grids a sample with at its uv position: the
    /// imaging taps for `GridFT`, `WProjectFT` and `MosaicFT`; the AW
    /// catalog grids its PSF with the weight cells (`AWProjectFT::
    /// findConvFunction` maps `cfwts2_p` when `makingPSF`).
    fn psf_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        self.taps(key, hold)
    }

    /// The taps a prediction gathers with: the gridding taps for every set
    /// but the AW catalog, whose `GridToData` reads the native-frequency
    /// cell (`CFBuffer::nearestFreqNdx(spw, chan)` without `conjBeams`)
    /// where `DataToGrid` read the conjugate one.
    fn prediction_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        self.taps(key, hold)
    }

    /// The half support a placement keyed `key` must keep inside the grid:
    /// the largest of every tap layout the key can lend.
    fn placement_half_support(&self, key: CfKey, hold: &mut CellHold) -> [u16; 2] {
        self.taps(key, hold).half_support()
    }

    /// Whether a sample at `w_lambda` is gridded at all. `WProjectFT`'s
    /// gridder drops a row whose plane index `nint(√(wScale·|w|))` lies
    /// beyond the last plane (`wprojgrid.f` `owp` tests the unclamped
    /// `loc(3)`); every other set accepts every `w` (the AW catalog clamps,
    /// `CFBuffer::nearestWNdx`).
    fn admits(&self, w_lambda: f64) -> bool {
        let _ = w_lambda;
        true
    }

    /// Whether the weight (sensitivity) image receives the image-side
    /// correction. `AWProjectFT::getWeightImage` divides its average PB by
    /// the sampling sinc as `getImage` does; `MosaicFT` publishes the
    /// transform of its gridded weight functions as `skyCoverage_p` with
    /// no correction at all, while its `getImage` divides the data and PSF
    /// by the sinc.
    fn corrects_weight_image(&self) -> bool {
        true
    }
}
