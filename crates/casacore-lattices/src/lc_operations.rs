// SPDX-License-Identifier: LGPL-3.0-or-later
//! Set-algebra operations on lattice regions.

use ndarray::{ArrayD, Dimension, IxDyn};

use crate::region::LCRegion;

/// Complement (inversion) of a region.
///
/// Corresponds to C++ `LCComplement`. A pixel is inside the complement
/// if and only if it is outside the original region.
#[derive(Debug)]
pub struct LCComplement {
    inner: Box<dyn LCRegion>,
    lattice_shape: Vec<usize>,
}

impl LCComplement {
    /// Creates the complement of a region.
    pub fn new(region: Box<dyn LCRegion>) -> Self {
        let lattice_shape = region.lattice_shape();
        Self {
            inner: region,
            lattice_shape,
        }
    }
}

impl LCRegion for LCComplement {
    fn lattice_shape(&self) -> Vec<usize> {
        self.lattice_shape.clone()
    }

    fn bounding_box_start(&self) -> Vec<usize> {
        vec![0; self.lattice_shape.len()]
    }

    fn bounding_box_shape(&self) -> Vec<usize> {
        self.lattice_shape.clone()
    }

    fn get_mask(&self) -> ArrayD<bool> {
        // Full lattice mask, inverted.
        let mut mask = ArrayD::from_elem(IxDyn(&self.lattice_shape), true);
        let inner_mask = self.inner.get_mask();
        let inner_start = self.inner.bounding_box_start();

        for (idx, &val) in inner_mask.indexed_iter() {
            let pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(inner_start.iter())
                .map(|(&i, &s)| i + s)
                .collect();
            mask[IxDyn(&pos)] = !val;
        }
        mask
    }

    fn contains(&self, position: &[usize]) -> bool {
        !self.inner.contains(position)
    }
}

/// Intersection (AND) of two regions.
///
/// Corresponds to C++ `LCIntersection`.
#[derive(Debug)]
pub struct LCIntersection {
    a: Box<dyn LCRegion>,
    b: Box<dyn LCRegion>,
    lattice_shape: Vec<usize>,
}

impl LCIntersection {
    /// Creates the intersection of two regions.
    pub fn new(a: Box<dyn LCRegion>, b: Box<dyn LCRegion>) -> Self {
        let lattice_shape = a.lattice_shape();
        Self {
            a,
            b,
            lattice_shape,
        }
    }
}

impl LCRegion for LCIntersection {
    fn lattice_shape(&self) -> Vec<usize> {
        self.lattice_shape.clone()
    }

    fn bounding_box_start(&self) -> Vec<usize> {
        let a_start = self.a.bounding_box_start();
        let b_start = self.b.bounding_box_start();
        a_start
            .iter()
            .zip(b_start.iter())
            .map(|(&a, &b)| a.max(b))
            .collect()
    }

    fn bounding_box_shape(&self) -> Vec<usize> {
        let start = self.bounding_box_start();
        let a_end: Vec<usize> = self
            .a
            .bounding_box_start()
            .iter()
            .zip(self.a.bounding_box_shape().iter())
            .map(|(&s, &l)| s + l)
            .collect();
        let b_end: Vec<usize> = self
            .b
            .bounding_box_start()
            .iter()
            .zip(self.b.bounding_box_shape().iter())
            .map(|(&s, &l)| s + l)
            .collect();
        start
            .iter()
            .zip(a_end.iter())
            .zip(b_end.iter())
            .map(|((&s, &ae), &be)| {
                let end = ae.min(be);
                end.saturating_sub(s)
            })
            .collect()
    }

    fn get_mask(&self) -> ArrayD<bool> {
        let bb_shape = self.bounding_box_shape();
        let bb_start = self.bounding_box_start();
        let mut mask = ArrayD::from_elem(IxDyn(&bb_shape), false);

        for (idx, val) in mask.indexed_iter_mut() {
            let pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(bb_start.iter())
                .map(|(&i, &s)| i + s)
                .collect();
            *val = self.a.contains(&pos) && self.b.contains(&pos);
        }
        mask
    }

    fn contains(&self, position: &[usize]) -> bool {
        self.a.contains(position) && self.b.contains(position)
    }
}

/// Union (OR) of two regions.
///
/// Corresponds to C++ `LCUnion`.
#[derive(Debug)]
pub struct LCUnion {
    a: Box<dyn LCRegion>,
    b: Box<dyn LCRegion>,
    lattice_shape: Vec<usize>,
}

impl LCUnion {
    /// Creates the union of two regions.
    pub fn new(a: Box<dyn LCRegion>, b: Box<dyn LCRegion>) -> Self {
        let lattice_shape = a.lattice_shape();
        Self {
            a,
            b,
            lattice_shape,
        }
    }
}

impl LCRegion for LCUnion {
    fn lattice_shape(&self) -> Vec<usize> {
        self.lattice_shape.clone()
    }

    fn bounding_box_start(&self) -> Vec<usize> {
        let a_start = self.a.bounding_box_start();
        let b_start = self.b.bounding_box_start();
        a_start
            .iter()
            .zip(b_start.iter())
            .map(|(&a, &b)| a.min(b))
            .collect()
    }

    fn bounding_box_shape(&self) -> Vec<usize> {
        let start = self.bounding_box_start();
        let a_end: Vec<usize> = self
            .a
            .bounding_box_start()
            .iter()
            .zip(self.a.bounding_box_shape().iter())
            .map(|(&s, &l)| s + l)
            .collect();
        let b_end: Vec<usize> = self
            .b
            .bounding_box_start()
            .iter()
            .zip(self.b.bounding_box_shape().iter())
            .map(|(&s, &l)| s + l)
            .collect();
        start
            .iter()
            .zip(a_end.iter())
            .zip(b_end.iter())
            .map(|((&s, &ae), &be)| ae.max(be) - s)
            .collect()
    }

    fn get_mask(&self) -> ArrayD<bool> {
        let bb_shape = self.bounding_box_shape();
        let bb_start = self.bounding_box_start();
        let mut mask = ArrayD::from_elem(IxDyn(&bb_shape), false);

        for (idx, val) in mask.indexed_iter_mut() {
            let pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(bb_start.iter())
                .map(|(&i, &s)| i + s)
                .collect();
            *val = self.a.contains(&pos) || self.b.contains(&pos);
        }
        mask
    }

    fn contains(&self, position: &[usize]) -> bool {
        self.a.contains(position) || self.b.contains(position)
    }
}

/// Difference (A minus B) of two regions.
///
/// Corresponds to C++ `LCDifference`.
#[derive(Debug)]
pub struct LCDifference {
    a: Box<dyn LCRegion>,
    b: Box<dyn LCRegion>,
    lattice_shape: Vec<usize>,
}

impl LCDifference {
    /// Creates the difference A minus B.
    pub fn new(a: Box<dyn LCRegion>, b: Box<dyn LCRegion>) -> Self {
        let lattice_shape = a.lattice_shape();
        Self {
            a,
            b,
            lattice_shape,
        }
    }
}

impl LCRegion for LCDifference {
    fn lattice_shape(&self) -> Vec<usize> {
        self.lattice_shape.clone()
    }

    fn bounding_box_start(&self) -> Vec<usize> {
        self.a.bounding_box_start()
    }

    fn bounding_box_shape(&self) -> Vec<usize> {
        self.a.bounding_box_shape()
    }

    fn get_mask(&self) -> ArrayD<bool> {
        let bb_shape = self.bounding_box_shape();
        let bb_start = self.bounding_box_start();
        let mut mask = ArrayD::from_elem(IxDyn(&bb_shape), false);

        for (idx, val) in mask.indexed_iter_mut() {
            let pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(bb_start.iter())
                .map(|(&i, &s)| i + s)
                .collect();
            *val = self.a.contains(&pos) && !self.b.contains(&pos);
        }
        mask
    }

    fn contains(&self, position: &[usize]) -> bool {
        self.a.contains(position) && !self.b.contains(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LCBox;

    fn make_box(blc: Vec<usize>, trc: Vec<usize>) -> Box<dyn LCRegion> {
        Box::new(LCBox::new(blc, trc, vec![10, 10]))
    }

    #[test]
    fn complement() {
        let region = LCComplement::new(make_box(vec![2, 2], vec![5, 5]));
        assert!(!region.contains(&[3, 3]));
        assert!(region.contains(&[0, 0]));
        assert!(region.contains(&[9, 9]));
    }

    #[test]
    fn intersection() {
        let a = make_box(vec![1, 1], vec![6, 6]);
        let b = make_box(vec![4, 4], vec![8, 8]);
        let region = LCIntersection::new(a, b);
        assert!(region.contains(&[5, 5])); // in both
        assert!(!region.contains(&[2, 2])); // only in a
        assert!(!region.contains(&[7, 7])); // only in b
    }

    #[test]
    fn union() {
        let a = make_box(vec![0, 0], vec![3, 3]);
        let b = make_box(vec![6, 6], vec![9, 9]);
        let region = LCUnion::new(a, b);
        assert!(region.contains(&[1, 1]));
        assert!(region.contains(&[7, 7]));
        assert!(!region.contains(&[5, 5]));
    }

    #[test]
    fn difference() {
        let a = make_box(vec![0, 0], vec![6, 6]);
        let b = make_box(vec![3, 3], vec![6, 6]);
        let region = LCDifference::new(a, b);
        assert!(region.contains(&[1, 1])); // in a, not in b
        assert!(!region.contains(&[4, 4])); // in both
    }
}
