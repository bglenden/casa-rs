// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tile-shaped cursor navigator for cache-optimal full scans.

use crate::navigator::LatticeNavigator;

/// A navigator that steps through a lattice in tile-shaped chunks.
///
/// Corresponds to the C++ `TileStepper` class. The cursor shape matches
/// the tile shape, which maximises cache locality for full-scan
/// operations on tiled (paged) lattices.
///
/// This is a specialisation of [`LatticeStepper`](crate::LatticeStepper)
/// where the cursor shape equals the tile shape.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::{TileStepper, LatticeNavigator};
///
/// let stepper = TileStepper::new(vec![16, 16], vec![8, 8]);
/// assert_eq!(stepper.n_steps(), 4);
/// assert_eq!(stepper.cursor_shape(), &[8, 8]);
/// ```
pub struct TileStepper {
    inner: crate::LatticeStepper,
}

impl TileStepper {
    /// Creates a new `TileStepper` with the given lattice and tile shapes.
    pub fn new(lattice_shape: Vec<usize>, tile_shape: Vec<usize>) -> Self {
        Self {
            inner: crate::LatticeStepper::new(lattice_shape, tile_shape, None),
        }
    }
}

impl LatticeNavigator for TileStepper {
    fn lattice_shape(&self) -> &[usize] {
        self.inner.lattice_shape()
    }

    fn cursor_shape(&self) -> &[usize] {
        self.inner.cursor_shape()
    }

    fn position(&self) -> &[usize] {
        self.inner.position()
    }

    fn at_end(&self) -> bool {
        self.inner.at_end()
    }

    fn next(&mut self) -> bool {
        self.inner.next()
    }

    fn prev(&mut self) -> bool {
        self.inner.prev()
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn n_steps(&self) -> usize {
        self.inner.n_steps()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_stepper_basic() {
        let mut stepper = TileStepper::new(vec![16, 16], vec![8, 8]);
        assert_eq!(stepper.n_steps(), 4);

        let mut positions = vec![stepper.position().to_vec()];
        while stepper.next() {
            positions.push(stepper.position().to_vec());
        }
        assert_eq!(positions.len(), 4);
    }

    #[test]
    fn tile_stepper_with_hangover() {
        let stepper = TileStepper::new(vec![10, 10], vec![8, 8]);
        assert_eq!(stepper.n_steps(), 4); // 2×2 tiles, last ones smaller
    }
}
