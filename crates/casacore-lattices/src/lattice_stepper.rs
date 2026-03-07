// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sequential rectangular stepping navigator.

use crate::navigator::LatticeNavigator;

/// A navigator that steps through a lattice in rectangular chunks
/// along a configurable axis path.
///
/// Corresponds to the C++ `LatticeStepper` class. The cursor advances
/// sequentially through the lattice, with axis ordering determined by
/// the `axis_path`. By default, the fastest-varying axis (axis 0) is
/// iterated first.
///
/// Near lattice boundaries, the cursor shape is reduced (hangover
/// handling) so that no out-of-bounds access occurs.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::{LatticeStepper, LatticeNavigator};
///
/// let stepper = LatticeStepper::new(vec![10, 10], vec![5, 5], None);
/// assert_eq!(stepper.n_steps(), 4); // 2×2 steps
/// ```
pub struct LatticeStepper {
    lattice_shape: Vec<usize>,
    cursor_shape_requested: Vec<usize>,
    cursor_shape_current: Vec<usize>,
    position: Vec<usize>,
    axis_path: Vec<usize>,
    at_end: bool,
}

impl LatticeStepper {
    /// Creates a new `LatticeStepper`.
    ///
    /// - `lattice_shape`: the overall lattice dimensions.
    /// - `cursor_shape`: the desired chunk size along each axis.
    /// - `axis_path`: optional axis ordering (default: 0, 1, 2, ...).
    ///   The first axis in the path is iterated fastest.
    pub fn new(
        lattice_shape: Vec<usize>,
        cursor_shape: Vec<usize>,
        axis_path: Option<Vec<usize>>,
    ) -> Self {
        let ndim = lattice_shape.len();
        let axis_path = axis_path.unwrap_or_else(|| (0..ndim).collect());
        let position = vec![0; ndim];
        let cursor_shape_current = compute_hangover(&lattice_shape, &cursor_shape, &position);

        Self {
            lattice_shape,
            cursor_shape_requested: cursor_shape,
            cursor_shape_current,
            position,
            axis_path,
            at_end: false,
        }
    }
}

/// Computes the effective cursor shape at a given position, clamping
/// to the lattice boundary.
fn compute_hangover(
    lattice_shape: &[usize],
    cursor_shape: &[usize],
    position: &[usize],
) -> Vec<usize> {
    lattice_shape
        .iter()
        .zip(cursor_shape.iter())
        .zip(position.iter())
        .map(|((&ls, &cs), &pos)| cs.min(ls - pos))
        .collect()
}

impl LatticeNavigator for LatticeStepper {
    fn lattice_shape(&self) -> &[usize] {
        &self.lattice_shape
    }

    fn cursor_shape(&self) -> &[usize] {
        &self.cursor_shape_current
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

        // Advance along the axis path.
        for &axis in &self.axis_path.clone() {
            self.position[axis] += self.cursor_shape_requested[axis];
            if self.position[axis] < self.lattice_shape[axis] {
                self.cursor_shape_current = compute_hangover(
                    &self.lattice_shape,
                    &self.cursor_shape_requested,
                    &self.position,
                );
                return true;
            }
            // Wrap this axis and carry to the next.
            self.position[axis] = 0;
        }

        // All axes wrapped — we've completed the traversal.
        self.at_end = true;
        false
    }

    fn prev(&mut self) -> bool {
        if self.position.iter().all(|&p| p == 0) && !self.at_end {
            return false;
        }

        if self.at_end {
            self.at_end = false;
            // Set to last valid position.
            for axis in 0..self.lattice_shape.len() {
                let n_steps_axis =
                    self.lattice_shape[axis].div_ceil(self.cursor_shape_requested[axis]);
                self.position[axis] = (n_steps_axis - 1) * self.cursor_shape_requested[axis];
            }
            self.cursor_shape_current = compute_hangover(
                &self.lattice_shape,
                &self.cursor_shape_requested,
                &self.position,
            );
            return true;
        }

        for &axis in &self.axis_path.clone() {
            if self.position[axis] >= self.cursor_shape_requested[axis] {
                self.position[axis] -= self.cursor_shape_requested[axis];
                self.cursor_shape_current = compute_hangover(
                    &self.lattice_shape,
                    &self.cursor_shape_requested,
                    &self.position,
                );
                return true;
            }
            // Wrap this axis to its last position.
            let n_steps_axis = self.lattice_shape[axis].div_ceil(self.cursor_shape_requested[axis]);
            self.position[axis] = (n_steps_axis - 1) * self.cursor_shape_requested[axis];
        }

        false
    }

    fn reset(&mut self) {
        self.position = vec![0; self.lattice_shape.len()];
        self.at_end = false;
        self.cursor_shape_current = compute_hangover(
            &self.lattice_shape,
            &self.cursor_shape_requested,
            &self.position,
        );
    }

    fn n_steps(&self) -> usize {
        self.lattice_shape
            .iter()
            .zip(self.cursor_shape_requested.iter())
            .map(|(&ls, &cs)| ls.div_ceil(cs))
            .product()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_2d_stepping() {
        let mut stepper = LatticeStepper::new(vec![10, 10], vec![5, 5], None);
        assert_eq!(stepper.n_steps(), 4);
        assert_eq!(stepper.position(), &[0, 0]);
        assert_eq!(stepper.cursor_shape(), &[5, 5]);

        assert!(stepper.next());
        assert_eq!(stepper.position(), &[5, 0]);

        assert!(stepper.next());
        assert_eq!(stepper.position(), &[0, 5]);

        assert!(stepper.next());
        assert_eq!(stepper.position(), &[5, 5]);

        assert!(!stepper.next());
        assert!(stepper.at_end());
    }

    #[test]
    fn hangover_handling() {
        let mut stepper = LatticeStepper::new(vec![7, 7], vec![4, 4], None);
        assert_eq!(stepper.n_steps(), 4);
        assert_eq!(stepper.cursor_shape(), &[4, 4]);

        stepper.next();
        assert_eq!(stepper.position(), &[4, 0]);
        assert_eq!(stepper.cursor_shape(), &[3, 4]); // hangover on axis 0

        stepper.next();
        assert_eq!(stepper.position(), &[0, 4]);
        assert_eq!(stepper.cursor_shape(), &[4, 3]); // hangover on axis 1

        stepper.next();
        assert_eq!(stepper.position(), &[4, 4]);
        assert_eq!(stepper.cursor_shape(), &[3, 3]); // hangover on both
    }

    #[test]
    fn custom_axis_path() {
        let mut stepper = LatticeStepper::new(vec![6, 8], vec![3, 4], Some(vec![1, 0]));
        assert_eq!(stepper.n_steps(), 4);

        // With axis_path [1, 0], axis 1 varies fastest.
        stepper.next();
        assert_eq!(stepper.position(), &[0, 4]);

        stepper.next();
        assert_eq!(stepper.position(), &[3, 0]);

        stepper.next();
        assert_eq!(stepper.position(), &[3, 4]);
    }

    #[test]
    fn reverse_iteration() {
        let mut stepper = LatticeStepper::new(vec![6, 6], vec![3, 3], None);
        // Go to end.
        while stepper.next() {}
        assert!(stepper.at_end());

        // Go back.
        assert!(stepper.prev());
        assert_eq!(stepper.position(), &[3, 3]);
        assert!(stepper.prev());
        assert_eq!(stepper.position(), &[0, 3]);
        assert!(stepper.prev());
        assert_eq!(stepper.position(), &[3, 0]);
        assert!(stepper.prev());
        assert_eq!(stepper.position(), &[0, 0]);
        assert!(!stepper.prev());
    }

    #[test]
    fn reset() {
        let mut stepper = LatticeStepper::new(vec![4, 4], vec![2, 2], None);
        stepper.next();
        stepper.next();
        stepper.reset();
        assert_eq!(stepper.position(), &[0, 0]);
        assert!(!stepper.at_end());
    }

    #[test]
    fn single_step() {
        let stepper = LatticeStepper::new(vec![4, 4], vec![4, 4], None);
        assert_eq!(stepper.n_steps(), 1);
    }

    #[test]
    fn n_steps_1d() {
        let stepper = LatticeStepper::new(vec![100], vec![10], None);
        assert_eq!(stepper.n_steps(), 10);
    }
}
