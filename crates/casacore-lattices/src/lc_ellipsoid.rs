// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ellipsoidal region.

use ndarray::{ArrayD, Dimension, IxDyn};

use crate::region::LCRegion;

/// An ellipsoidal region within a lattice.
///
/// Corresponds to the C++ `LCEllipsoid` class. Defined by a center point
/// and semi-axes lengths. A pixel is inside if the sum of squared
/// normalized distances from the center is <= 1.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::{LCEllipsoid, LCRegion};
///
/// let region = LCEllipsoid::new(vec![5.0, 5.0], vec![3.0, 3.0], vec![10, 10]);
/// assert!(region.contains(&[5, 5])); // center
/// assert!(region.contains(&[3, 5])); // within semi-axis
/// assert!(!region.contains(&[0, 0])); // outside
/// ```
#[derive(Debug, Clone)]
pub struct LCEllipsoid {
    center: Vec<f64>,
    semi_axes: Vec<f64>,
    lattice_shape: Vec<usize>,
}

impl LCEllipsoid {
    /// Creates an ellipsoidal region.
    ///
    /// - `center`: the center of the ellipsoid (floating-point coordinates).
    /// - `semi_axes`: the semi-axis length along each dimension.
    /// - `lattice_shape`: the shape of the parent lattice.
    pub fn new(center: Vec<f64>, semi_axes: Vec<f64>, lattice_shape: Vec<usize>) -> Self {
        Self {
            center,
            semi_axes,
            lattice_shape,
        }
    }

    fn is_inside(&self, position: &[usize]) -> bool {
        let sum: f64 = position
            .iter()
            .zip(self.center.iter())
            .zip(self.semi_axes.iter())
            .map(|((&p, &c), &r)| {
                let d = (p as f64 - c) / r;
                d * d
            })
            .sum();
        sum <= 1.0
    }

    fn bounding_box(&self) -> (Vec<usize>, Vec<usize>) {
        let ndim = self.center.len();
        let mut blc = vec![0usize; ndim];
        let mut trc = vec![0usize; ndim];
        for axis in 0..ndim {
            blc[axis] = (self.center[axis] - self.semi_axes[axis]).floor().max(0.0) as usize;
            trc[axis] = ((self.center[axis] + self.semi_axes[axis]).ceil() as usize)
                .min(self.lattice_shape[axis] - 1);
        }
        (blc, trc)
    }
}

impl LCRegion for LCEllipsoid {
    fn lattice_shape(&self) -> Vec<usize> {
        self.lattice_shape.clone()
    }

    fn bounding_box_start(&self) -> Vec<usize> {
        self.bounding_box().0
    }

    fn bounding_box_shape(&self) -> Vec<usize> {
        let (blc, trc) = self.bounding_box();
        blc.iter()
            .zip(trc.iter())
            .map(|(&b, &t)| t - b + 1)
            .collect()
    }

    fn get_mask(&self) -> ArrayD<bool> {
        let (blc, _) = self.bounding_box();
        let bb_shape = self.bounding_box_shape();
        let mut mask = ArrayD::from_elem(IxDyn(&bb_shape), false);

        for (idx, val) in mask.indexed_iter_mut() {
            let pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(blc.iter())
                .map(|(&i, &b)| i + b)
                .collect();
            *val = self.is_inside(&pos);
        }

        mask
    }

    fn contains(&self, position: &[usize]) -> bool {
        self.is_inside(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ellipsoid_center_inside() {
        let region = LCEllipsoid::new(vec![5.0, 5.0], vec![3.0, 3.0], vec![10, 10]);
        assert!(region.contains(&[5, 5]));
    }

    #[test]
    fn ellipsoid_boundary() {
        let region = LCEllipsoid::new(vec![5.0, 5.0], vec![3.0, 3.0], vec![10, 10]);
        assert!(region.contains(&[2, 5])); // exactly on semi-axis
        assert!(region.contains(&[8, 5]));
    }

    #[test]
    fn ellipsoid_outside() {
        let region = LCEllipsoid::new(vec![5.0, 5.0], vec![2.0, 2.0], vec![10, 10]);
        assert!(!region.contains(&[0, 0]));
        assert!(!region.contains(&[9, 9]));
    }

    #[test]
    fn ellipsoid_mask() {
        let region = LCEllipsoid::new(vec![2.0, 2.0], vec![1.5, 1.5], vec![5, 5]);
        let mask = region.get_mask();
        // Center should be true.
        let (blc, _) = region.bounding_box();
        let center_in_mask: Vec<usize> = vec![2 - blc[0], 2 - blc[1]];
        assert!(mask[IxDyn(&center_in_mask)]);
    }
}
