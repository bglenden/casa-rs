// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-worker grid storage shared by the CPU and device backends.

use std::ops::Range;

use casa_fft::FftScalar;
use num_complex::{Complex, Complex32, Complex64};
use num_traits::Float;

use crate::error::OperatorError;
use crate::geometry::GridGeometry;

/// Arithmetic precision of a grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GridPrecision {
    /// Interleaved `f32` complex cells.
    F32,
    /// Interleaved `f64` complex cells (default; taps and values stay `f32`).
    F64,
}

/// What a gridding pass accumulates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Mode {
    /// Weighted visibilities: the dirty or residual image terms.
    Data,
    /// Weights with unit visibility: the point-spread-function terms.
    Psf,
    /// Weights with the `FT[PB²]` taps at the uv origin: the sensitivity image.
    Weight,
}

/// The modes one accumulator holds; each present mode owns its own terms
/// and `sumwt`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ModeSet {
    /// Hold the data terms.
    pub data: bool,
    /// Hold the PSF terms.
    pub psf: bool,
    /// Hold the weight image.
    pub weight: bool,
}

impl ModeSet {
    /// Data terms only.
    pub const DATA: Self = Self {
        data: true,
        psf: false,
        weight: false,
    };
    /// PSF terms only.
    pub const PSF: Self = Self {
        data: false,
        psf: true,
        weight: false,
    };
    /// Data and PSF terms.
    pub const DATA_PSF: Self = Self {
        data: true,
        psf: true,
        weight: false,
    };
    /// Every mode.
    pub const ALL: Self = Self {
        data: true,
        psf: true,
        weight: true,
    };

    /// Whether `mode` is held.
    #[must_use]
    pub const fn contains(self, mode: Mode) -> bool {
        match mode {
            Mode::Data => self.data,
            Mode::Psf => self.psf,
            Mode::Weight => self.weight,
        }
    }
}

/// Half-open range of grid planes `[start, end)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlaneRange {
    /// First plane.
    pub start: u32,
    /// One past the last plane.
    pub end: u32,
}

impl PlaneRange {
    /// `[start, end)`.
    #[must_use]
    pub const fn new(start: u32, end: u32) -> Self {
        assert!(start <= end, "plane range runs backwards");
        Self { start, end }
    }

    /// The single plane `plane`.
    #[must_use]
    pub const fn single(plane: u32) -> Self {
        Self {
            start: plane,
            end: plane + 1,
        }
    }

    /// Number of planes.
    #[must_use]
    pub const fn len(self) -> usize {
        (self.end - self.start) as usize
    }

    /// Whether the range is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// Whether `plane` lies in the range.
    #[must_use]
    pub const fn contains(self, plane: u32) -> bool {
        plane >= self.start && plane < self.end
    }

    /// Local index of `plane` inside the range.
    #[must_use]
    pub const fn local(self, plane: u32) -> usize {
        assert!(self.contains(plane), "plane outside the accumulator range");
        (plane - self.start) as usize
    }
}

/// A rectangular window of the padded grid an accumulator covers, in grid
/// cells. A tile is the region a worker owns plus the kernel halo; every
/// sample routed to it must have its whole support inside.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Tile {
    /// First cell `[x, y]`.
    pub origin: [usize; 2],
    /// Extent `[nx, ny]`.
    pub shape: [usize; 2],
}

impl Tile {
    /// The whole padded grid.
    #[must_use]
    pub const fn full(grid_shape: [usize; 2]) -> Self {
        Self {
            origin: [0, 0],
            shape: grid_shape,
        }
    }

    /// Cells in the tile.
    #[must_use]
    pub const fn cells(self) -> usize {
        self.shape[0] * self.shape[1]
    }

    /// Whether `other` lies inside this tile.
    #[must_use]
    pub const fn contains(self, other: Tile) -> bool {
        other.origin[0] >= self.origin[0]
            && other.origin[1] >= self.origin[1]
            && other.origin[0] + other.shape[0] <= self.origin[0] + self.shape[0]
            && other.origin[1] + other.shape[1] <= self.origin[1] + self.shape[1]
    }
}

/// Scalar type of a grid: `f32` or `f64`.
pub trait GridScalar:
    Float + FftScalar + Default + std::fmt::Debug + Send + Sync + 'static
{
    /// The precision this scalar realises.
    const PRECISION: GridPrecision;
    /// Widen or keep an `f32`.
    fn from_f32(value: f32) -> Self;
    /// Narrow or keep an `f64`.
    fn from_f64(value: f64) -> Self;
    /// Widen or keep to `f64`.
    fn into_f64(self) -> f64;
    /// Narrow or keep to `f32`.
    fn into_f32(self) -> f32;
    /// The cells of a storage of this precision.
    ///
    /// Panics when the storage holds the other precision; callers dispatch
    /// on [`GridStorage::precision`] first.
    fn cells(storage: &GridStorage) -> &[Complex<Self>];
    /// Mutable cells of a storage of this precision.
    fn cells_mut(storage: &mut GridStorage) -> &mut [Complex<Self>];
}

impl GridScalar for f32 {
    const PRECISION: GridPrecision = GridPrecision::F32;
    fn from_f32(value: f32) -> Self {
        value
    }
    fn from_f64(value: f64) -> Self {
        value as f32
    }
    fn into_f64(self) -> f64 {
        f64::from(self)
    }
    fn into_f32(self) -> f32 {
        self
    }
    fn cells(storage: &GridStorage) -> &[Complex<Self>] {
        match storage {
            GridStorage::F32(cells) => cells,
            GridStorage::F64(_) => panic!("f32 kernel on an f64 grid"),
        }
    }
    fn cells_mut(storage: &mut GridStorage) -> &mut [Complex<Self>] {
        match storage {
            GridStorage::F32(cells) => cells,
            GridStorage::F64(_) => panic!("f32 kernel on an f64 grid"),
        }
    }
}

impl GridScalar for f64 {
    const PRECISION: GridPrecision = GridPrecision::F64;
    fn from_f32(value: f32) -> Self {
        f64::from(value)
    }
    fn from_f64(value: f64) -> Self {
        value
    }
    fn into_f64(self) -> f64 {
        self
    }
    fn into_f32(self) -> f32 {
        self as f32
    }
    fn cells(storage: &GridStorage) -> &[Complex<Self>] {
        match storage {
            GridStorage::F64(cells) => cells,
            GridStorage::F32(_) => panic!("f64 kernel on an f32 grid"),
        }
    }
    fn cells_mut(storage: &mut GridStorage) -> &mut [Complex<Self>] {
        match storage {
            GridStorage::F64(cells) => cells,
            GridStorage::F32(_) => panic!("f64 kernel on an f32 grid"),
        }
    }
}

/// Interleaved complex cells in one of the two precisions.
#[derive(Clone, Debug, PartialEq)]
pub enum GridStorage {
    /// `f32` cells.
    F32(Vec<Complex32>),
    /// `f64` cells.
    F64(Vec<Complex64>),
}

impl GridStorage {
    /// `len` zero cells of `precision`.
    #[must_use]
    pub fn zeros(precision: GridPrecision, len: usize) -> Self {
        match precision {
            GridPrecision::F32 => Self::F32(vec![Complex32::default(); len]),
            GridPrecision::F64 => Self::F64(vec![Complex64::default(); len]),
        }
    }

    /// Precision of the cells.
    #[must_use]
    pub const fn precision(&self) -> GridPrecision {
        match self {
            Self::F32(_) => GridPrecision::F32,
            Self::F64(_) => GridPrecision::F64,
        }
    }

    /// Number of cells.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::F32(cells) => cells.len(),
            Self::F64(cells) => cells.len(),
        }
    }

    /// Whether there are no cells.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set every cell to zero.
    pub fn fill_zero(&mut self) {
        match self {
            Self::F32(cells) => cells.fill(Complex32::default()),
            Self::F64(cells) => cells.fill(Complex64::default()),
        }
    }
}

/// Shape of an accumulator: which planes, polarizations, terms and window
/// it holds, over which geometry.
///
/// Cells are laid out `[plane][pol][term][y][x]`, x fastest, with the terms
/// of each held mode concatenated in the order data, PSF, weight. The same
/// layout is the device buffer, so a device copy is one memcpy.
#[derive(Clone, Debug, PartialEq)]
pub struct AccumulatorLayout {
    geometry: GridGeometry,
    planes: PlaneRange,
    pols: usize,
    data_terms: usize,
    psf_terms: usize,
    weight_terms: usize,
    tile: Tile,
}

impl AccumulatorLayout {
    /// Layout holding `modes` over `planes` × `pols` grid polarizations.
    ///
    /// `data_terms` is the number of Taylor terms (1 for other bases) and
    /// `psf_terms` is `2·data_terms − 1`; the weight image has one term.
    #[must_use]
    pub fn new(
        geometry: GridGeometry,
        planes: PlaneRange,
        pols: usize,
        modes: ModeSet,
        data_terms: usize,
        psf_terms: usize,
        tile: Option<Tile>,
    ) -> Self {
        assert!(pols > 0, "an accumulator needs at least one polarization");
        assert!(data_terms > 0 && psf_terms > 0, "term counts are positive");
        let full = Tile::full(geometry.grid_shape());
        let tile = tile.unwrap_or(full);
        assert!(full.contains(tile), "tile lies outside the padded grid");
        assert!(tile.cells() > 0, "tile is empty");
        Self {
            geometry,
            planes,
            pols,
            data_terms: if modes.data { data_terms } else { 0 },
            psf_terms: if modes.psf { psf_terms } else { 0 },
            weight_terms: usize::from(modes.weight),
            tile,
        }
    }

    /// The grid geometry.
    #[must_use]
    pub const fn geometry(&self) -> &GridGeometry {
        &self.geometry
    }

    /// Planes held.
    #[must_use]
    pub const fn planes(&self) -> PlaneRange {
        self.planes
    }

    /// Grid polarizations per plane.
    #[must_use]
    pub const fn pols(&self) -> usize {
        self.pols
    }

    /// Window covered.
    #[must_use]
    pub const fn tile(&self) -> Tile {
        self.tile
    }

    /// Whether the window is the whole padded grid.
    #[must_use]
    pub fn is_full_grid(&self) -> bool {
        self.tile == Tile::full(self.geometry.grid_shape())
    }

    /// Terms per (plane, polarization) over every held mode.
    #[must_use]
    pub const fn terms(&self) -> usize {
        self.data_terms + self.psf_terms + self.weight_terms
    }

    /// Term indices of `mode`, or `None` when the layout does not hold it.
    #[must_use]
    pub fn term_range(&self, mode: Mode) -> Option<Range<usize>> {
        let (start, count) = match mode {
            Mode::Data => (0, self.data_terms),
            Mode::Psf => (self.data_terms, self.psf_terms),
            Mode::Weight => (self.data_terms + self.psf_terms, self.weight_terms),
        };
        (count > 0).then_some(start..start + count)
    }

    /// Cells per `[y][x]` block.
    #[must_use]
    pub const fn block_cells(&self) -> usize {
        self.tile.cells()
    }

    /// Number of `[y][x]` blocks.
    #[must_use]
    pub const fn blocks(&self) -> usize {
        self.planes.len() * self.pols * self.terms()
    }

    /// Total cells.
    #[must_use]
    pub const fn cells(&self) -> usize {
        self.blocks() * self.block_cells()
    }

    /// Bytes of cell storage at `precision`.
    #[must_use]
    pub const fn bytes(&self, precision: GridPrecision) -> usize {
        let cell = match precision {
            GridPrecision::F32 => 8,
            GridPrecision::F64 => 16,
        };
        self.cells() * cell
    }

    /// Index of block `(plane_local, pol, term)`.
    #[must_use]
    pub const fn block_index(&self, plane_local: usize, pol: usize, term: usize) -> usize {
        (plane_local * self.pols + pol) * self.terms() + term
    }

    /// First cell of block `(plane_local, pol, term)`.
    #[must_use]
    pub const fn block_offset(&self, plane_local: usize, pol: usize, term: usize) -> usize {
        self.block_index(plane_local, pol, term) * self.block_cells()
    }
}

/// Per-worker grid: the cells of an [`AccumulatorLayout`] plus `sumwt`
/// (one `f64` per block) accumulated in the kernel.
#[derive(Clone, Debug, PartialEq)]
pub struct GridAccumulator {
    layout: AccumulatorLayout,
    storage: GridStorage,
    sumwt: Vec<f64>,
}

impl GridAccumulator {
    /// Zeroed accumulator for `layout` at `precision`.
    #[must_use]
    pub fn new(layout: AccumulatorLayout, precision: GridPrecision) -> Self {
        let storage = GridStorage::zeros(precision, layout.cells());
        let sumwt = vec![0.0; layout.blocks()];
        Self {
            layout,
            storage,
            sumwt,
        }
    }

    /// The layout.
    #[must_use]
    pub const fn layout(&self) -> &AccumulatorLayout {
        &self.layout
    }

    /// Cell precision.
    #[must_use]
    pub const fn precision(&self) -> GridPrecision {
        self.storage.precision()
    }

    /// The cells.
    #[must_use]
    pub const fn storage(&self) -> &GridStorage {
        &self.storage
    }

    /// `sumwt` per block, indexed by [`AccumulatorLayout::block_index`].
    #[must_use]
    pub fn sumwt(&self) -> &[f64] {
        &self.sumwt
    }

    /// `sumwt` of block `(plane_local, pol, term)`.
    #[must_use]
    pub fn sumwt_at(&self, plane_local: usize, pol: usize, term: usize) -> f64 {
        self.sumwt[self.layout.block_index(plane_local, pol, term)]
    }

    /// Cells of block `(plane_local, pol, term)` at precision `T`.
    #[must_use]
    pub fn block<T: GridScalar>(
        &self,
        plane_local: usize,
        pol: usize,
        term: usize,
    ) -> &[Complex<T>] {
        let offset = self.layout.block_offset(plane_local, pol, term);
        &T::cells(&self.storage)[offset..offset + self.layout.block_cells()]
    }

    /// Zero every cell and `sumwt`.
    pub fn reset(&mut self) {
        self.storage.fill_zero();
        self.sumwt.fill(0.0);
    }

    /// Add `other`'s cells and `sumwt` into this accumulator.
    ///
    /// Both must share geometry, planes, polarizations, terms and precision;
    /// `other`'s tile must lie inside this one.
    pub fn merge_from(&mut self, other: &GridAccumulator) -> Result<(), OperatorError> {
        let mine = &self.layout;
        let theirs = &other.layout;
        if mine.geometry != theirs.geometry {
            return Err(OperatorError::AccumulatorLayout { reason: "geometry" });
        }
        if mine.planes != theirs.planes
            || mine.pols != theirs.pols
            || mine.data_terms != theirs.data_terms
            || mine.psf_terms != theirs.psf_terms
            || mine.weight_terms != theirs.weight_terms
        {
            return Err(OperatorError::AccumulatorLayout {
                reason: "planes, polarizations or terms",
            });
        }
        if self.precision() != other.precision() {
            return Err(OperatorError::AccumulatorLayout {
                reason: "precision",
            });
        }
        if !mine.tile.contains(theirs.tile) {
            return Err(OperatorError::AccumulatorLayout {
                reason: "tile lies outside the target",
            });
        }
        match (&mut self.storage, &other.storage) {
            (GridStorage::F32(target), GridStorage::F32(source)) => {
                add_tiles(target, source, mine, theirs);
            }
            (GridStorage::F64(target), GridStorage::F64(source)) => {
                add_tiles(target, source, mine, theirs);
            }
            _ => unreachable!("precision checked"),
        }
        for (target, source) in self.sumwt.iter_mut().zip(&other.sumwt) {
            *target += source;
        }
        Ok(())
    }

    /// Layout, cells and `sumwt` for a kernel at precision `T`.
    pub(crate) fn parts_mut<T: GridScalar>(
        &mut self,
    ) -> (&AccumulatorLayout, &mut [Complex<T>], &mut [f64]) {
        (
            &self.layout,
            T::cells_mut(&mut self.storage),
            &mut self.sumwt,
        )
    }
}

fn add_tiles<T: GridScalar>(
    target: &mut [Complex<T>],
    source: &[Complex<T>],
    mine: &AccumulatorLayout,
    theirs: &AccumulatorLayout,
) {
    let dx = theirs.tile.origin[0] - mine.tile.origin[0];
    let dy = theirs.tile.origin[1] - mine.tile.origin[1];
    let [source_nx, source_ny] = theirs.tile.shape;
    let target_nx = mine.tile.shape[0];
    for block in 0..mine.blocks() {
        let target_block =
            &mut target[block * mine.block_cells()..(block + 1) * mine.block_cells()];
        let source_block =
            &source[block * theirs.block_cells()..(block + 1) * theirs.block_cells()];
        for y in 0..source_ny {
            let target_row = &mut target_block[(y + dy) * target_nx + dx..][..source_nx];
            let source_row = &source_block[y * source_nx..][..source_nx];
            for (cell, value) in target_row.iter_mut().zip(source_row) {
                *cell = *cell + *value;
            }
        }
    }
}
