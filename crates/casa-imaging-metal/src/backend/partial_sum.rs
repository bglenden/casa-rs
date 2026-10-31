// SPDX-License-Identifier: LGPL-3.0-or-later

//! Local #991 experiment: reuse the f32 device grid for bounded partials.

use std::ptr::NonNull;

use casa_imaging_operator::DeviceCells;
use num_complex::{Complex32, Complex64};

/// One apply's master. Its f64 allocation is charged beside gridding by
/// `partial_grid_bytes`; it is gone before the wave's finishing phase.
pub(super) struct PartialGrid {
    cells: NonNull<Complex32>,
    master: Vec<Complex64>,
}

impl PartialGrid {
    /// # Safety
    ///
    /// The caller exclusively owns the enclosing accumulator until this
    /// value drops. Device work must be complete at each merge and drop;
    /// no other host reference to these cells may be used meanwhile.
    pub(super) unsafe fn new(cells: &DeviceCells) -> Self {
        let values = cells.cells(); // settles any earlier deferred work
        let master = values
            .iter()
            .map(|value| Complex64::new(f64::from(value.re), f64::from(value.im)))
            .collect();
        // SAFETY: new's caller holds the accumulator exclusively. The shared
        // values were last used above; this is the owner's original pointer,
        // not a pointer derived from that shared slice.
        let pointer = unsafe { cells.backend_pointer() };
        let mut result = Self {
            cells: pointer,
            master,
        };
        result.clear();
        result
    }

    fn clear(&mut self) {
        // SAFETY: new's exclusive ownership and completed-device contract;
        // the temporary slice does not outlive this host operation.
        unsafe { std::slice::from_raw_parts_mut(self.cells.as_ptr(), self.master.len()) }
            .fill(Complex32::new(0.0, 0.0));
    }

    pub(super) fn merge_and_clear(&mut self) {
        // SAFETY: the caller retired the grid-writing command before this
        // operation; new's ownership contract keeps the allocation alive.
        let partial = unsafe { std::slice::from_raw_parts(self.cells.as_ptr(), self.master.len()) };
        for (sum, value) in self.master.iter_mut().zip(partial) {
            *sum += Complex64::new(f64::from(value.re), f64::from(value.im));
        }
        self.clear();
    }
}

impl Drop for PartialGrid {
    fn drop(&mut self) {
        // SAFETY: run drains all commands before returning or unwinding;
        // new's exclusive ownership still holds while this guard drops.
        let cells =
            unsafe { std::slice::from_raw_parts_mut(self.cells.as_ptr(), self.master.len()) };
        for (cell, sum) in cells.iter_mut().zip(&self.master) {
            *cell = Complex32::new(sum.re as f32, sum.im as f32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_partials_preserve_small_terms_between_large_cancelling_terms() {
        let mut storage = vec![Complex32::new(0.0, 0.0)];
        let pointer = NonNull::new(storage.as_mut_ptr()).unwrap();
        // SAFETY: the boxed vector owns one initialized stable cell, reached
        // only through cells or this test's simulated backend access.
        let cells = unsafe { DeviceCells::new(pointer, 1, Box::new(storage)) };
        // SAFETY: this test owns cells exclusively; no device work runs.
        let mut partial = unsafe { PartialGrid::new(&cells) };
        for value in [1.0e8, 1.0, -1.0e8] {
            // SAFETY: no host view/device operation overlaps this simulated
            // completed kernel write, and the allocation still owns one cell.
            unsafe {
                cells
                    .backend_pointer()
                    .as_ptr()
                    .write(Complex32::new(value, -value))
            };
            partial.merge_and_clear();
        }
        drop(partial);
        assert_eq!(cells.cells(), &[Complex32::new(1.0, -1.0)]);
    }
}
