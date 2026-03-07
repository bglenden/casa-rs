// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rectangular box region.

use ndarray::{ArrayD, IxDyn};

use crate::region::LCRegion;

/// A rectangular (box) region within a lattice.
///
/// Corresponds to the C++ `LCBox` class. Defined by bottom-left corner
/// (blc) and top-right corner (trc), inclusive on both ends.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::{LCBox, LCRegion};
///
/// let region = LCBox::new(vec![1, 1], vec![3, 3], vec![5, 5]);
/// assert!(region.contains(&[2, 2]));
/// assert!(!region.contains(&[0, 0]));
/// assert_eq!(region.bounding_box_shape(), vec![3, 3]);
/// ```
#[derive(Debug, Clone)]
pub struct LCBox {
    blc: Vec<usize>,
    trc: Vec<usize>,
    lattice_shape: Vec<usize>,
}

impl LCBox {
    /// Creates a box region from bottom-left and top-right corners (inclusive).
    pub fn new(blc: Vec<usize>, trc: Vec<usize>, lattice_shape: Vec<usize>) -> Self {
        Self {
            blc,
            trc,
            lattice_shape,
        }
    }

    /// Creates a box region covering the entire lattice.
    pub fn full(lattice_shape: Vec<usize>) -> Self {
        let blc = vec![0; lattice_shape.len()];
        let trc: Vec<usize> = lattice_shape.iter().map(|&s| s - 1).collect();
        Self {
            blc,
            trc,
            lattice_shape,
        }
    }
}

impl LCRegion for LCBox {
    fn lattice_shape(&self) -> Vec<usize> {
        self.lattice_shape.clone()
    }

    fn bounding_box_start(&self) -> Vec<usize> {
        self.blc.clone()
    }

    fn bounding_box_shape(&self) -> Vec<usize> {
        self.blc
            .iter()
            .zip(self.trc.iter())
            .map(|(&b, &t)| t - b + 1)
            .collect()
    }

    fn get_mask(&self) -> ArrayD<bool> {
        let shape = self.bounding_box_shape();
        ArrayD::from_elem(IxDyn(&shape), true)
    }

    fn contains(&self, position: &[usize]) -> bool {
        position
            .iter()
            .zip(self.blc.iter())
            .zip(self.trc.iter())
            .all(|((&p, &b), &t)| p >= b && p <= t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn box_contains() {
        let region = LCBox::new(vec![2, 2], vec![5, 5], vec![10, 10]);
        assert!(region.contains(&[3, 3]));
        assert!(region.contains(&[2, 2]));
        assert!(region.contains(&[5, 5]));
        assert!(!region.contains(&[1, 3]));
        assert!(!region.contains(&[3, 6]));
    }

    #[test]
    fn box_mask_all_true() {
        let region = LCBox::new(vec![0, 0], vec![2, 2], vec![5, 5]);
        let mask = region.get_mask();
        assert_eq!(mask.shape(), &[3, 3]);
        assert!(mask.iter().all(|&v| v));
    }

    #[test]
    fn full_box() {
        let region = LCBox::full(vec![4, 6]);
        assert_eq!(region.bounding_box_start(), vec![0, 0]);
        assert_eq!(region.bounding_box_shape(), vec![4, 6]);
    }
}
