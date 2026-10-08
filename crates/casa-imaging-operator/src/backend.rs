// SPDX-License-Identifier: LGPL-3.0-or-later
//! The backend contract: one dispatch over one sample block.

use std::any::Any;
use std::sync::OnceLock;

use num_complex::{Complex, Complex32};

use crate::accumulator::{
    AccumulatorLayout, GridAccumulator, GridPrecision, GridScalar, GridStorage, Mode,
};
use crate::convolution::ConvolutionFunctionSet;
use crate::error::OperatorError;
use crate::sample::SampleBlock;

/// Model grids ready for degridding: the forward FFT of the corrected,
/// polarization-expanded model, one full-grid block per
/// `(plane, grid pol, Taylor term)` in the operator's precision.
///
/// The grids are immutable. A device backend keeps its copy of them in the
/// grids themselves ([`PreparedModelGrids::device_copy`]), made once and
/// dropped with them.
pub struct PreparedModelGrids {
    layout: AccumulatorLayout,
    storage: GridStorage,
    device: OnceLock<Box<dyn Any + Send + Sync>>,
}

impl PreparedModelGrids {
    pub(crate) fn new(layout: AccumulatorLayout, storage: GridStorage) -> Self {
        debug_assert_eq!(storage.len(), layout.cells());
        debug_assert!(layout.is_full_grid());
        Self {
            layout,
            storage,
            device: OnceLock::new(),
        }
    }

    /// A device backend's copy of the grids, made by `copy` from the cells
    /// the first time any caller asks; later calls, from any thread, return
    /// the same copy. A failed copy is kept like a successful one, so a
    /// backend stores a `Result` when copying can fail.
    ///
    /// Panics when an earlier caller stored a copy of another type: one
    /// backend type serves a model.
    pub fn device_copy<C: Any + Send + Sync>(&self, copy: impl FnOnce(&GridStorage) -> C) -> &C {
        self.device
            .get_or_init(|| Box::new(copy(&self.storage)))
            .downcast_ref()
            .expect("one device backend type serves a model")
    }

    /// Layout of the grids (data terms only, full grid).
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
}

impl Clone for PreparedModelGrids {
    /// The clone holds no device copy.
    fn clone(&self) -> Self {
        Self::new(self.layout.clone(), self.storage.clone())
    }
}

impl PartialEq for PreparedModelGrids {
    fn eq(&self, other: &Self) -> bool {
        self.layout == other.layout && self.storage == other.storage
    }
}

impl std::fmt::Debug for PreparedModelGrids {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedModelGrids")
            .field("layout", &self.layout)
            .field("storage", &self.storage)
            .finish_non_exhaustive()
    }
}

/// What one dispatch does with a sample block.
pub enum Work<'a> {
    /// Accumulate the block into `acc` in `mode`.
    Grid {
        /// Data, PSF or weight terms.
        mode: Mode,
        /// Target accumulator; it must hold `mode`.
        acc: &'a mut GridAccumulator,
    },
    /// Predict raw visibilities (`npol` per placement, sample-major,
    /// phase-centre phasor applied) from `model`.
    Predict {
        /// Prepared model grids covering every plane in the block.
        model: &'a PreparedModelGrids,
        /// `npol × placements` predicted visibilities.
        out: &'a mut [Complex32],
    },
    /// Predict, subtract from the block and grid the residual data terms in
    /// one dispatch; optionally hand back the raw residual samples for a
    /// final pass that writes the corrected or model column.
    ResidualGrid {
        /// Prepared model grids covering every plane in the block.
        model: &'a PreparedModelGrids,
        /// Target accumulator; it must hold the data terms.
        acc: &'a mut GridAccumulator,
        /// `npol × placements` raw residual visibilities, when wanted.
        residual_out: Option<&'a mut [Complex32]>,
    },
}

/// A gridding backend: CPU or device. One implementation of the kernel
/// contract in plan section 5.3, selected by the runtime.
pub trait GridBackend: Send {
    /// Apply `work` to one block with kernel set `cf`; the work is complete
    /// when this returns.
    ///
    /// Every placement must lie on a plane the target holds and have its
    /// whole support inside the target's tile; the block's polarization
    /// count must equal the kernel set's visibility polarizations.
    fn apply(
        &mut self,
        block: &SampleBlock<'_>,
        cf: &dyn ConvolutionFunctionSet,
        work: Work<'_>,
    ) -> Result<(), OperatorError>;
}
