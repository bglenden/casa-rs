// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lattice iterators wrapping navigators as Rust `Iterator`s.

use ndarray::ArrayD;

use crate::element::LatticeElement;
use crate::lattice::{Lattice, LatticeMut};
use crate::navigator::LatticeNavigator;

/// A read-only iterator over chunks of a lattice.
///
/// Corresponds to the C++ `RO_LatticeIterator<T>`. Wraps a
/// [`LatticeNavigator`] and yields `ArrayD<T>` chunks by implementing
/// the standard Rust [`Iterator`] trait.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::*;
/// use ndarray::{ArrayD, IxDyn};
///
/// let data = ArrayD::from_shape_fn(IxDyn(&[6, 4]), |idx| (idx[0] * 4 + idx[1]) as f64);
/// let lat = ArrayLattice::new(data);
/// let stepper = LatticeStepper::new(vec![6, 4], vec![3, 2], None);
/// let chunks: Vec<_> = LatticeIter::new(&lat, stepper).collect();
/// assert_eq!(chunks.len(), 4); // 2×2 chunks
/// ```
pub struct LatticeIter<'a, T: LatticeElement, L: Lattice<T>, N: LatticeNavigator> {
    lattice: &'a L,
    navigator: N,
    started: bool,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: LatticeElement, L: Lattice<T>, N: LatticeNavigator> LatticeIter<'a, T, L, N> {
    /// Creates a new read-only lattice iterator.
    pub fn new(lattice: &'a L, navigator: N) -> Self {
        Self {
            lattice,
            navigator,
            started: false,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns a reference to the underlying navigator.
    pub fn navigator(&self) -> &N {
        &self.navigator
    }
}

impl<'a, T: LatticeElement, L: Lattice<T>, N: LatticeNavigator> Iterator
    for LatticeIter<'a, T, L, N>
{
    type Item = ArrayD<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            if self.navigator.at_end() {
                return None;
            }
        } else if !self.navigator.next() {
            return None;
        }

        let pos = self.navigator.position();
        let shape = self.navigator.cursor_shape();
        let stride = vec![1; pos.len()];

        self.lattice.get_slice(pos, shape, &stride).ok()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let total = self.navigator.n_steps();
        (0, Some(total))
    }
}

/// A mutable iterator over chunks of a lattice.
///
/// Corresponds to the C++ `LatticeIterator<T>`. Each call to `next()`
/// yields a [`LatticeChunk`] containing the data and its position.
/// Modified chunks can be written back using
/// [`LatticeChunk::write_back`].
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::*;
///
/// let mut lat = ArrayLattice::<f64>::zeros(vec![4, 4]);
/// let stepper = LatticeStepper::new(vec![4, 4], vec![2, 2], None);
/// let mut iter = LatticeIterMut::new(&mut lat, stepper);
///
/// while let Some(mut chunk) = iter.next_chunk() {
///     chunk.data.fill(1.0);
///     chunk.write_back(&mut iter).unwrap();
/// }
///
/// assert_eq!(iter.lattice().get_at(&[0, 0]).unwrap(), 1.0);
/// ```
pub struct LatticeIterMut<'a, T: LatticeElement, L: LatticeMut<T>, N: LatticeNavigator> {
    lattice: &'a mut L,
    navigator: N,
    started: bool,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: LatticeElement, L: LatticeMut<T>, N: LatticeNavigator> LatticeIterMut<'a, T, L, N> {
    /// Creates a new mutable lattice iterator.
    pub fn new(lattice: &'a mut L, navigator: N) -> Self {
        Self {
            lattice,
            navigator,
            started: false,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns a reference to the underlying lattice.
    pub fn lattice(&self) -> &L {
        self.lattice
    }

    /// Advances to the next chunk and returns it.
    pub fn next_chunk(&mut self) -> Option<LatticeChunk<T>> {
        if !self.started {
            self.started = true;
            if self.navigator.at_end() {
                return None;
            }
        } else if !self.navigator.next() {
            return None;
        }

        let pos = self.navigator.position().to_vec();
        let shape = self.navigator.cursor_shape();
        let stride = vec![1; pos.len()];

        let data = self.lattice.get_slice(&pos, shape, &stride).ok()?;
        Some(LatticeChunk {
            data,
            position: pos,
        })
    }

    /// Writes a modified chunk back to the lattice.
    pub fn write_chunk(&mut self, chunk: &LatticeChunk<T>) -> Result<(), crate::LatticeError> {
        self.lattice.put_slice(&chunk.data, &chunk.position)
    }
}

/// A chunk of typed data with its position, for [`LatticeIterMut`].
pub struct LatticeChunk<T> {
    /// The chunk data.
    pub data: ArrayD<T>,
    /// The origin position of this chunk in the lattice.
    pub position: Vec<usize>,
}

impl<T> LatticeChunk<T> {
    /// Writes this chunk back to the lattice via the iterator.
    pub fn write_back<L: LatticeMut<T>, N: LatticeNavigator>(
        &self,
        iter: &mut LatticeIterMut<'_, T, L, N>,
    ) -> Result<(), crate::LatticeError>
    where
        T: LatticeElement,
    {
        iter.write_chunk(self)
    }
}

/// Extension trait providing convenience iterator methods on lattices.
pub trait LatticeIterExt<T: LatticeElement>: Lattice<T> {
    /// Returns an iterator that yields lines along the specified axis.
    ///
    /// Each yielded chunk is a 1-D array (the line) at successive
    /// positions along the other axes.
    fn iter_lines(&self, axis: usize) -> LatticeIter<'_, T, Self, crate::TiledLineStepper>
    where
        Self: Sized,
    {
        let tile = self.nice_cursor_shape();
        let stepper = crate::TiledLineStepper::new(self.shape().to_vec(), tile, axis);
        LatticeIter::new(self, stepper)
    }

    /// Returns an iterator that yields tile-shaped chunks.
    fn iter_tiles(&self) -> LatticeIter<'_, T, Self, crate::TileStepper>
    where
        Self: Sized,
    {
        let tile = self.nice_cursor_shape();
        let stepper = crate::TileStepper::new(self.shape().to_vec(), tile);
        LatticeIter::new(self, stepper)
    }

    /// Returns an iterator that yields chunks of the specified shape.
    fn iter_chunks(
        &self,
        cursor_shape: Vec<usize>,
    ) -> LatticeIter<'_, T, Self, crate::LatticeStepper>
    where
        Self: Sized,
    {
        let stepper = crate::LatticeStepper::new(self.shape().to_vec(), cursor_shape, None);
        LatticeIter::new(self, stepper)
    }
}

// Blanket implementation for all lattice types.
impl<T: LatticeElement, L: Lattice<T>> LatticeIterExt<T> for L {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArrayLattice, LatticeStepper};
    use ndarray::IxDyn;

    #[test]
    fn iterate_chunks() {
        let data = ArrayD::from_shape_fn(IxDyn(&[6, 4]), |idx| (idx[0] * 4 + idx[1]) as f64);
        let lat = ArrayLattice::new(data);
        let stepper = LatticeStepper::new(vec![6, 4], vec![3, 2], None);
        let chunks: Vec<_> = LatticeIter::new(&lat, stepper).collect();
        assert_eq!(chunks.len(), 4);

        // First chunk: [0..3, 0..2]
        assert_eq!(chunks[0].shape(), &[3, 2]);
        assert_eq!(chunks[0][IxDyn(&[0, 0])], 0.0);
    }

    #[test]
    fn iterate_sum_equals_total() {
        let data = ArrayD::from_shape_fn(IxDyn(&[10, 10]), |idx| (idx[0] * 10 + idx[1]) as f64);
        let total: f64 = data.iter().sum();
        let lat = ArrayLattice::new(data);
        let stepper = LatticeStepper::new(vec![10, 10], vec![4, 4], None);
        let chunk_sum: f64 = LatticeIter::new(&lat, stepper)
            .map(|c| c.iter().sum::<f64>())
            .sum();
        assert_eq!(chunk_sum, total);
    }

    #[test]
    fn mutable_iteration() {
        let mut lat = ArrayLattice::<f64>::zeros(vec![6, 6]);
        let stepper = LatticeStepper::new(vec![6, 6], vec![3, 3], None);
        let mut iter = LatticeIterMut::new(&mut lat, stepper);

        while let Some(mut chunk) = iter.next_chunk() {
            chunk.data.fill(1.0);
            chunk.write_back(&mut iter).unwrap();
        }

        let result = iter.lattice().get().unwrap();
        assert!(result.iter().all(|&v| v == 1.0));
    }

    #[test]
    fn iter_lines_axis0() {
        let data = ArrayD::from_shape_fn(IxDyn(&[4, 3]), |idx| (idx[0] * 3 + idx[1]) as f64);
        let lat = ArrayLattice::new(data);
        let lines: Vec<_> = lat.iter_lines(0).collect();
        // Each line is along axis 0; should cover all axis-1 positions.
        assert!(!lines.is_empty());
        let total: f64 = lines.iter().flat_map(|l| l.iter()).sum();
        assert_eq!(total, (0..12).sum::<i32>() as f64);
    }

    #[test]
    fn iter_tiles_coverage() {
        let data = ArrayD::from_elem(IxDyn(&[8, 8]), 1.0f64);
        let lat = ArrayLattice::new(data);
        let total: f64 = lat.iter_tiles().flat_map(|c| c.into_iter()).sum();
        assert_eq!(total, 64.0);
    }

    #[test]
    fn iter_chunks_coverage() {
        let data = ArrayD::from_elem(IxDyn(&[10, 10]), 2.0f64);
        let lat = ArrayLattice::new(data);
        let total: f64 = lat
            .iter_chunks(vec![5, 5])
            .flat_map(|c| c.into_iter())
            .sum();
        assert_eq!(total, 200.0);
    }
}
