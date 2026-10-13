// SPDX-License-Identifier: LGPL-3.0-or-later
//! The model at native channels, as a backend forms it for a residual taken
//! at native channels ([`Work::PredictNative`](crate::Work::PredictNative),
//! [`Work::NativeResidualGrid`](crate::Work::NativeResidualGrid)).
//!
//! CASA predicts the model at its image frequencies, interpolates it to each
//! native channel (`FTMachine::interpolateFrequencyFromgrid`), subtracts it
//! there and interpolates the residual to its gridding frequencies
//! (`interpolateFrequencyTogrid`). Both interpolations are linear, so a
//! backend grids `W·e^{iφ}·interp(V) − W·e^{iφ}·interp(M)`: the observed
//! value the host places, less the model at the same native endpoints with
//! the same factors. The host computes each placement's observed value, its
//! native endpoints and the native terms of every row; the backend predicts,
//! interpolates the predictions to native cells and subtracts, without
//! handing the predictions back.

use std::any::Any;

use num_complex::Complex32;

/// No prediction or native cell: the term contributes zero.
pub const NO_CELL: u32 = u32::MAX;

/// The model at one native cell (`npol` values from `cell · npol`): the
/// predictions of a batch at the two output-channel points the native
/// channel lies between, `P_low + (P_high − P_low)·fraction` for each
/// unflagged polarization and zero for a flagged one. A missing prediction
/// ([`NO_CELL`]) is zero.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct NativeTerm {
    /// Native cell of the model.
    pub cell: u32,
    /// Batch index of the prediction at the lower point.
    pub low: u32,
    /// Batch index of the prediction at the upper point.
    pub high: u32,
    /// Position of the native channel between the two points.
    pub fraction: f32,
    /// Bit `p`: polarization `p` is flagged and its model stays zero.
    pub flags: u32,
}

/// The native endpoints of one placed sample: its value interpolates the
/// observed values at `cells` with `factors`, and the backend subtracts
/// `scale · Σ factors · M[cells]`, where `scale = W·e^{iφ}` is the factor the
/// placement's values already carry. A sample at one native channel has the
/// second cell [`NO_CELL`] and factor zero, so a flagged neighbour's value,
/// which may be NaN, never reaches it.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct NativeSource {
    /// Native cells of the two endpoints.
    pub cells: [u32; 2],
    /// Interpolation factor of each endpoint.
    pub factors: [f32; 2],
    /// The imaging weight times the phase-centre phasor, `W·e^{iφ}`.
    pub scale: Complex32,
}

/// Model cells a device backend allocated in memory it addresses, for one
/// block's native cells. The host never writes them; it reads them only
/// through [`Self::host_cells`], once the device's writes are done.
pub struct DeviceNativeCells {
    cells: std::ptr::NonNull<Complex32>,
    len: usize,
    owner: Box<dyn Any + Send + Sync>,
}

// SAFETY: the host only reads the cells, through `host_cells`, whose
// contract excludes concurrent device writes; the owner is `Send + Sync`.
unsafe impl Send for DeviceNativeCells {}
// SAFETY: as for `Send`.
unsafe impl Sync for DeviceNativeCells {}

impl DeviceNativeCells {
    /// `len` cells at `cells`, kept alive by `owner`.
    ///
    /// # Safety
    ///
    /// `cells` must point to `len` aligned `Complex32` values that stay valid
    /// for as long as `owner` lives, and only the device may write them.
    #[must_use]
    pub unsafe fn new(
        cells: std::ptr::NonNull<Complex32>,
        len: usize,
        owner: Box<dyn Any + Send + Sync>,
    ) -> Self {
        Self { cells, len, owner }
    }

    /// Number of cells.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether there are no cells.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The allocation that keeps the cells alive.
    #[must_use]
    pub fn owner(&self) -> &(dyn Any + Send + Sync) {
        self.owner.as_ref()
    }

    /// The cells, as the host reads them.
    ///
    /// # Safety
    ///
    /// Every device command that writes the cells must have completed, and
    /// none may be issued while the slice lives.
    #[must_use]
    pub unsafe fn host_cells(&self) -> &[Complex32] {
        // SAFETY: `new`'s contract and the caller's.
        unsafe { std::slice::from_raw_parts(self.cells.as_ptr(), self.len) }
    }
}

impl std::fmt::Debug for DeviceNativeCells {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeviceNativeCells")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

/// Where [`Work::PredictNative`](crate::Work::PredictNative) writes the
/// model at native cells.
pub enum NativeTarget<'a> {
    /// Host cells, `npol` per native cell, native cell `first` first; the
    /// terms address cells within them.
    Host {
        /// The cells.
        cells: &'a mut [Complex32],
        /// The native cell at index 0.
        first: u32,
    },
    /// Cells of the backend's device ([`DeviceNativeCells`]).
    Device(&'a DeviceNativeCells),
}

/// Where [`Work::NativeResidualGrid`](crate::Work::NativeResidualGrid)
/// reads the model at native cells.
#[derive(Clone, Copy)]
pub enum NativeModel<'a> {
    /// Host cells, `npol` per native cell, native cell `first` first.
    Host {
        /// The cells.
        cells: &'a [Complex32],
        /// The native cell at index 0.
        first: u32,
    },
    /// Cells of the backend's device ([`DeviceNativeCells`]).
    Device(&'a DeviceNativeCells),
}

/// Write (or with `add`, add) each term's model into `cells`, whose index 0
/// is native cell `first`, from `predicted` (`npol` per batch placement).
pub(crate) fn interpolate_terms(
    predicted: &[Complex32],
    npol: usize,
    terms: &[NativeTerm],
    cells: &mut [Complex32],
    first: u32,
    add: bool,
) {
    let at = |index: u32, pol: usize| {
        if index == NO_CELL {
            Complex32::default()
        } else {
            predicted[index as usize * npol + pol]
        }
    };
    for term in terms {
        let base = (term.cell - first) as usize * npol;
        for pol in 0..npol {
            let model = if (term.flags >> pol) & 1 == 1 {
                Complex32::default()
            } else if term.low == term.high {
                at(term.low, pol)
            } else {
                let (a, b) = (at(term.low, pol), at(term.high, pol));
                a + (b - a) * term.fraction
            };
            if add {
                cells[base + pol] += model;
            } else {
                cells[base + pol] = model;
            }
        }
    }
}

/// `values` (`W·V·e^{iφ}` per polarization) less `source`'s share of the
/// model in `cells` (index 0 is native cell `first`), into `out`.
pub(crate) fn residual_values(
    values: &[Complex32],
    source: &NativeSource,
    cells: &[Complex32],
    first: u32,
    out: &mut [Complex32],
) {
    let npol = values.len();
    for (pol, (out, value)) in out.iter_mut().zip(values).enumerate() {
        let mut model = Complex32::default();
        for (cell, factor) in source.cells.iter().zip(source.factors) {
            if *cell != NO_CELL {
                model += cells[(cell - first) as usize * npol + pol] * factor;
            }
        }
        *out = value - source.scale * model;
    }
}
