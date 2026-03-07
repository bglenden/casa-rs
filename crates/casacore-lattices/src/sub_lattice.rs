// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lattice view restricted by a region.

use ndarray::{ArrayD, Dimension, IxDyn};

use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::lattice::{Lattice, LatticeMut};
use crate::region::LCRegion;

/// A read-only view of a lattice restricted by a region.
///
/// Corresponds to the C++ `SubLattice<T>`. The sub-lattice presents a
/// view whose coordinate system is relative to the region's bounding
/// box. Pixels outside the region are returned as the default value
/// for type `T`.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::*;
/// use ndarray::{ArrayD, IxDyn};
///
/// let data = ArrayD::from_shape_fn(IxDyn(&[10, 10]), |idx| (idx[0] * 10 + idx[1]) as f64);
/// let lat = ArrayLattice::new(data);
/// let region = LCBox::new(vec![2, 3], vec![5, 7], vec![10, 10]);
/// let sub = SubLattice::new(&lat, Box::new(region));
///
/// assert_eq!(sub.shape(), &[4, 5]); // bounding box shape
/// assert_eq!(sub.get_at(&[0, 0]).unwrap(), 23.0); // lat[2, 3]
/// ```
pub struct SubLattice<'a, T: LatticeElement, L: Lattice<T>> {
    parent: &'a L,
    region: Box<dyn LCRegion>,
    bb_start: Vec<usize>,
    bb_shape: Vec<usize>,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: LatticeElement, L: Lattice<T>> SubLattice<'a, T, L> {
    /// Creates a sub-lattice view restricted by the given region.
    pub fn new(parent: &'a L, region: Box<dyn LCRegion>) -> Self {
        let bb_start = region.bounding_box_start();
        let bb_shape = region.bounding_box_shape();
        Self {
            parent,
            region,
            bb_start,
            bb_shape,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns the region defining this sub-lattice.
    pub fn region(&self) -> &dyn LCRegion {
        self.region.as_ref()
    }
}

impl<'a, T: LatticeElement, L: Lattice<T>> Lattice<T> for SubLattice<'a, T, L> {
    fn shape(&self) -> &[usize] {
        &self.bb_shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        if position.len() != self.bb_shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.bb_shape.len(),
                got: position.len(),
            });
        }

        // Translate to parent coordinates.
        let parent_pos: Vec<usize> = position
            .iter()
            .zip(self.bb_start.iter())
            .map(|(&p, &s)| p + s)
            .collect();

        if self.region.contains(&parent_pos) {
            self.parent.get_at(&parent_pos)
        } else {
            Ok(T::default_value())
        }
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        let ndim = self.bb_shape.len();
        if start.len() != ndim || shape.len() != ndim || stride.len() != ndim {
            return Err(LatticeError::NdimMismatch {
                expected: ndim,
                got: start.len(),
            });
        }

        // Build the result array.
        let mut result = ArrayD::from_elem(IxDyn(shape), T::default_value());

        for (idx, val) in result.indexed_iter_mut() {
            let sub_pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(start.iter())
                .zip(stride.iter())
                .map(|((&i, &s), &st)| s + i * st)
                .collect();

            let parent_pos: Vec<usize> = sub_pos
                .iter()
                .zip(self.bb_start.iter())
                .map(|(&p, &s)| p + s)
                .collect();

            if self.region.contains(&parent_pos) {
                *val = self.parent.get_at(&parent_pos)?;
            }
        }

        Ok(result)
    }
}

/// A mutable view of a lattice restricted by a region.
///
/// Like [`SubLattice`] but allows writes. Writes to pixels outside the
/// region are silently ignored.
pub struct SubLatticeMut<'a, T: LatticeElement, L: LatticeMut<T>> {
    parent: &'a mut L,
    region: Box<dyn LCRegion>,
    bb_start: Vec<usize>,
    bb_shape: Vec<usize>,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: LatticeElement, L: LatticeMut<T>> SubLatticeMut<'a, T, L> {
    /// Creates a mutable sub-lattice view.
    pub fn new(parent: &'a mut L, region: Box<dyn LCRegion>) -> Self {
        let bb_start = region.bounding_box_start();
        let bb_shape = region.bounding_box_shape();
        Self {
            parent,
            region,
            bb_start,
            bb_shape,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T: LatticeElement, L: LatticeMut<T>> Lattice<T> for SubLatticeMut<'a, T, L> {
    fn shape(&self) -> &[usize] {
        &self.bb_shape
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        if position.len() != self.bb_shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.bb_shape.len(),
                got: position.len(),
            });
        }
        let parent_pos: Vec<usize> = position
            .iter()
            .zip(self.bb_start.iter())
            .map(|(&p, &s)| p + s)
            .collect();
        if self.region.contains(&parent_pos) {
            self.parent.get_at(&parent_pos)
        } else {
            Ok(T::default_value())
        }
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        let ndim = self.bb_shape.len();
        if start.len() != ndim || shape.len() != ndim || stride.len() != ndim {
            return Err(LatticeError::NdimMismatch {
                expected: ndim,
                got: start.len(),
            });
        }

        let mut result = ArrayD::from_elem(IxDyn(shape), T::default_value());
        for (idx, val) in result.indexed_iter_mut() {
            let sub_pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(start.iter())
                .zip(stride.iter())
                .map(|((&i, &s), &st)| s + i * st)
                .collect();
            let parent_pos: Vec<usize> = sub_pos
                .iter()
                .zip(self.bb_start.iter())
                .map(|(&p, &s)| p + s)
                .collect();
            if self.region.contains(&parent_pos) {
                *val = self.parent.get_at(&parent_pos)?;
            }
        }
        Ok(result)
    }
}

impl<'a, T: LatticeElement, L: LatticeMut<T>> LatticeMut<T> for SubLatticeMut<'a, T, L> {
    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
        let parent_pos: Vec<usize> = position
            .iter()
            .zip(self.bb_start.iter())
            .map(|(&p, &s)| p + s)
            .collect();
        if self.region.contains(&parent_pos) {
            self.parent.put_at(value, &parent_pos)
        } else {
            Ok(()) // silently ignore writes outside region
        }
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        // Write each element individually, checking region membership.
        for (idx, val) in data.indexed_iter() {
            let sub_pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(start.iter())
                .map(|(&i, &s)| i + s)
                .collect();
            let parent_pos: Vec<usize> = sub_pos
                .iter()
                .zip(self.bb_start.iter())
                .map(|(&p, &s)| p + s)
                .collect();
            if self.region.contains(&parent_pos) {
                self.parent.put_at(val.clone(), &parent_pos)?;
            }
        }
        Ok(())
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        // Set only pixels within the region.
        for axis_sizes in IterPositions::new(&self.bb_shape) {
            let parent_pos: Vec<usize> = axis_sizes
                .iter()
                .zip(self.bb_start.iter())
                .map(|(&p, &s)| p + s)
                .collect();
            if self.region.contains(&parent_pos) {
                self.parent.put_at(value.clone(), &parent_pos)?;
            }
        }
        Ok(())
    }
}

/// Iterator over all positions in a given shape.
struct IterPositions {
    shape: Vec<usize>,
    current: Vec<usize>,
    done: bool,
}

impl IterPositions {
    fn new(shape: &[usize]) -> Self {
        let done = shape.contains(&0);
        Self {
            shape: shape.to_vec(),
            current: vec![0; shape.len()],
            done,
        }
    }
}

impl Iterator for IterPositions {
    type Item = Vec<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let result = self.current.clone();

        // Advance.
        for axis in 0..self.shape.len() {
            self.current[axis] += 1;
            if self.current[axis] < self.shape[axis] {
                return Some(result);
            }
            self.current[axis] = 0;
        }
        self.done = true;
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArrayLattice, LCBox};
    use ndarray::ArrayD;

    #[test]
    fn sub_lattice_read() {
        let data = ArrayD::from_shape_fn(IxDyn(&[10, 10]), |idx| (idx[0] * 10 + idx[1]) as f64);
        let lat = ArrayLattice::new(data);
        let region = LCBox::new(vec![2, 3], vec![5, 7], vec![10, 10]);
        let sub = SubLattice::new(&lat, Box::new(region));

        assert_eq!(sub.shape(), &[4, 5]);
        assert_eq!(sub.get_at(&[0, 0]).unwrap(), 23.0); // lat[2, 3]
        assert_eq!(sub.get_at(&[3, 4]).unwrap(), 57.0); // lat[5, 7]
    }

    #[test]
    fn sub_lattice_get_slice() {
        let data = ArrayD::from_shape_fn(IxDyn(&[10, 10]), |idx| (idx[0] * 10 + idx[1]) as f64);
        let lat = ArrayLattice::new(data);
        let region = LCBox::new(vec![0, 0], vec![4, 4], vec![10, 10]);
        let sub = SubLattice::new(&lat, Box::new(region));

        let slice = sub.get_slice(&[1, 1], &[2, 2], &[1, 1]).unwrap();
        assert_eq!(slice.shape(), &[2, 2]);
        assert_eq!(slice[IxDyn(&[0, 0])], 11.0); // lat[1, 1]
    }

    #[test]
    fn sub_lattice_mut_write() {
        let mut lat = ArrayLattice::<f64>::zeros(vec![10, 10]);
        let region = LCBox::new(vec![2, 2], vec![4, 4], vec![10, 10]);
        {
            let mut sub = SubLatticeMut::new(&mut lat, Box::new(region));
            sub.put_at(99.0, &[0, 0]).unwrap(); // writes to lat[2, 2]
            sub.put_at(88.0, &[2, 2]).unwrap(); // writes to lat[4, 4]
        }
        assert_eq!(lat.get_at(&[2, 2]).unwrap(), 99.0);
        assert_eq!(lat.get_at(&[4, 4]).unwrap(), 88.0);
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 0.0); // untouched
    }

    #[test]
    fn sub_lattice_mut_set() {
        let mut lat = ArrayLattice::<f64>::zeros(vec![6, 6]);
        let region = LCBox::new(vec![1, 1], vec![2, 2], vec![6, 6]);
        {
            let mut sub = SubLatticeMut::new(&mut lat, Box::new(region));
            sub.set(5.0).unwrap();
        }
        // Only [1..3, 1..3] should be set.
        assert_eq!(lat.get_at(&[1, 1]).unwrap(), 5.0);
        assert_eq!(lat.get_at(&[2, 2]).unwrap(), 5.0);
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 0.0);
        assert_eq!(lat.get_at(&[3, 3]).unwrap(), 0.0);
    }
}
