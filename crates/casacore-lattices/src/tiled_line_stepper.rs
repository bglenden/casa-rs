// SPDX-License-Identifier: LGPL-3.0-or-later
//! Line-by-line navigator within tile groups for cache-optimal line access.

use crate::navigator::LatticeNavigator;

/// A navigator that iterates lines along a specified axis, ordered to
/// maximise cache locality within tile groups.
///
/// Corresponds to the C++ `TiledLineStepper` class. The cursor is a
/// single line (one axis spans the full extent or tile extent; all other
/// axes have extent 1). Lines are visited in an order that groups them
/// by tile, reducing the number of tile loads for paged lattices.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::{TiledLineStepper, LatticeNavigator};
///
/// // Iterate lines along axis 0 for a 4×4 lattice with 2×2 tiles:
/// let stepper = TiledLineStepper::new(vec![4, 4], vec![2, 2], 0);
/// assert_eq!(stepper.cursor_shape(), &[2, 1]); // line of length 2
/// ```
pub struct TiledLineStepper {
    lattice_shape: Vec<usize>,
    tile_shape: Vec<usize>,
    line_axis: usize,
    position: Vec<usize>,
    cursor_shape: Vec<usize>,
    at_end: bool,
}

impl TiledLineStepper {
    /// Creates a new `TiledLineStepper`.
    ///
    /// - `lattice_shape`: the overall lattice dimensions.
    /// - `tile_shape`: the tile size (for cache-optimal ordering).
    /// - `line_axis`: the axis along which lines are extracted.
    pub fn new(lattice_shape: Vec<usize>, tile_shape: Vec<usize>, line_axis: usize) -> Self {
        let ndim = lattice_shape.len();
        let mut cursor_shape = vec![1; ndim];
        cursor_shape[line_axis] = tile_shape[line_axis].min(lattice_shape[line_axis]);

        Self {
            lattice_shape,
            tile_shape,
            line_axis,
            position: vec![0; ndim],
            cursor_shape,
            at_end: false,
        }
    }

    /// Advances to the next line within the current tile group, or
    /// moves to the next tile group.
    fn advance(&mut self) -> bool {
        let ndim = self.lattice_shape.len();

        // First, try advancing within the current tile group on non-line axes.
        for axis in 0..ndim {
            if axis == self.line_axis {
                continue;
            }
            self.position[axis] += 1;
            if self.position[axis] < self.lattice_shape[axis] {
                self.update_cursor();
                return true;
            }
            // Check if we need to advance to next tile group on this axis.
            let tile_start = (self.position[axis] / self.tile_shape[axis]) * self.tile_shape[axis];
            // We've exhausted this axis within the tile group; wrap back
            // to tile start and carry to next axis.
            self.position[axis] = tile_start;
        }

        // All non-line axes exhausted within tile group. Advance the
        // tile group on the line axis.
        let line_pos = self.position[self.line_axis] + self.tile_shape[self.line_axis];
        if line_pos < self.lattice_shape[self.line_axis] {
            self.position[self.line_axis] = line_pos;
            // Reset non-line axes to start.
            for axis in 0..ndim {
                if axis != self.line_axis {
                    self.position[axis] = 0;
                }
            }
            self.update_cursor();
            return true;
        }

        // All tile groups on the line axis exhausted. Try advancing
        // the tile group on non-line axes.
        self.position[self.line_axis] = 0;
        for axis in 0..ndim {
            if axis == self.line_axis {
                continue;
            }
            let next_tile_start =
                ((self.position[axis] / self.tile_shape[axis]) + 1) * self.tile_shape[axis];
            if next_tile_start < self.lattice_shape[axis] {
                self.position[axis] = next_tile_start;
                // Reset all earlier non-line axes.
                for a2 in 0..axis {
                    if a2 != self.line_axis {
                        self.position[a2] = 0;
                    }
                }
                self.update_cursor();
                return true;
            }
            self.position[axis] = 0;
        }

        false
    }

    fn update_cursor(&mut self) {
        let ndim = self.lattice_shape.len();
        for axis in 0..ndim {
            if axis == self.line_axis {
                self.cursor_shape[axis] =
                    self.tile_shape[axis].min(self.lattice_shape[axis] - self.position[axis]);
            } else {
                self.cursor_shape[axis] = 1;
            }
        }
    }
}

impl LatticeNavigator for TiledLineStepper {
    fn lattice_shape(&self) -> &[usize] {
        &self.lattice_shape
    }

    fn cursor_shape(&self) -> &[usize] {
        &self.cursor_shape
    }

    fn position(&self) -> &[usize] {
        &self.position
    }

    fn at_end(&self) -> bool {
        self.at_end
    }

    fn next(&mut self) -> bool {
        if self.at_end {
            return false;
        }
        if !self.advance() {
            self.at_end = true;
            return false;
        }
        true
    }

    fn prev(&mut self) -> bool {
        // Simple implementation: not commonly used for TiledLineStepper.
        false
    }

    fn reset(&mut self) {
        let ndim = self.lattice_shape.len();
        self.position = vec![0; ndim];
        self.at_end = false;
        self.update_cursor();
    }

    fn n_steps(&self) -> usize {
        let ndim = self.lattice_shape.len();
        let mut steps = 1usize;
        for axis in 0..ndim {
            if axis == self.line_axis {
                steps *= self.lattice_shape[axis].div_ceil(self.tile_shape[axis]);
            } else {
                steps *= self.lattice_shape[axis];
            }
        }
        steps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_line_stepping() {
        let mut stepper = TiledLineStepper::new(vec![4, 4], vec![2, 2], 0);
        // Lines along axis 0: 2 tile groups on axis 0, 4 lines on axis 1
        // = 2 * 4 = 8 total steps
        assert_eq!(stepper.n_steps(), 8);

        let mut count = 1; // Include initial position
        while stepper.next() {
            count += 1;
        }
        assert_eq!(count, 8);
    }

    #[test]
    fn covers_all_elements() {
        let shape = vec![6, 4];
        let tile = vec![3, 2];
        let mut stepper = TiledLineStepper::new(shape.clone(), tile, 0);

        let mut visited = std::collections::HashSet::new();
        loop {
            let pos = stepper.position().to_vec();
            let cursor = stepper.cursor_shape().to_vec();
            for i in 0..cursor[0] {
                visited.insert((pos[0] + i, pos[1]));
            }
            if !stepper.next() {
                break;
            }
        }

        // All elements should be visited.
        for x in 0..shape[0] {
            for y in 0..shape[1] {
                assert!(visited.contains(&(x, y)), "missing ({x}, {y})");
            }
        }
    }

    #[test]
    fn reset_works() {
        let mut stepper = TiledLineStepper::new(vec![4, 4], vec![2, 2], 0);
        stepper.next();
        stepper.next();
        stepper.reset();
        assert_eq!(stepper.position(), &[0, 0]);
        assert!(!stepper.at_end());
    }
}
