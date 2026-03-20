// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core lattice traits for read and write access.

use ndarray::ArrayD;

use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::traversal::{
    TraversalCacheHint, TraversalCacheScope, TraversalCursor, TraversalCursorIter, TraversalIter,
    TraversalSpec,
};

/// Read-only access to an N-dimensional lattice of typed elements.
///
/// Corresponds to the C++ `Lattice<T>` abstract base class (read-only
/// portion). A lattice is a regular N-dimensional array of elements of
/// type `T`, which may be stored in memory, on disk (tiled), or as a
/// computed expression.
///
/// The core methods are [`get`](Self::get) (retrieve the entire lattice),
/// [`get_slice`](Self::get_slice) (retrieve a rectangular sub-region),
/// and [`get_at`](Self::get_at) (retrieve a single element). Storage
/// characteristics are described by [`is_persistent`](Self::is_persistent),
/// [`is_paged`](Self::is_paged), and [`is_writable`](Self::is_writable).
///
/// # Relationship to C++ casacore
///
/// In C++ casacore, `LatticeBase` provides shape/ndim/nelements, while
/// `Lattice<T>` adds typed get/put methods. Here we unify them into a
/// single generic trait.
pub trait Lattice<T: LatticeElement> {
    /// Returns the shape of the lattice (size along each axis).
    ///
    /// The returned slice has length equal to [`ndim`](Self::ndim).
    fn shape(&self) -> &[usize];

    /// Returns the number of dimensions (axes) of the lattice.
    fn ndim(&self) -> usize {
        self.shape().len()
    }

    /// Returns the total number of elements in the lattice.
    ///
    /// Equal to the product of all shape dimensions.
    fn nelements(&self) -> usize {
        self.shape().iter().product()
    }

    /// Returns `true` if the lattice is backed by persistent storage.
    ///
    /// In-memory lattices (like [`ArrayLattice`](crate::ArrayLattice))
    /// return `false`; disk-backed lattices return `true`.
    fn is_persistent(&self) -> bool {
        false
    }

    /// Returns `true` if the lattice uses paged (tiled) storage.
    ///
    /// Paged lattices read/write data in tile-sized chunks for efficiency.
    fn is_paged(&self) -> bool {
        false
    }

    /// Returns `true` if the lattice supports write operations.
    fn is_writable(&self) -> bool {
        true
    }

    /// Retrieves a single element at the given N-dimensional position.
    ///
    /// Returns an error if `position` is out of bounds or has the wrong
    /// number of dimensions.
    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError>;

    /// Retrieves a rectangular sub-region of the lattice.
    ///
    /// The slice is defined by `start` (inclusive origin), `shape` (number
    /// of elements along each axis), and `stride` (step between elements).
    /// All three vectors must have length equal to [`ndim`](Self::ndim).
    ///
    /// The returned array has shape `shape`.
    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError>;

    /// Retrieves the entire lattice as an N-dimensional array.
    ///
    /// Equivalent to `get_slice` with start at the origin, the full shape,
    /// and unit stride.
    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        let ndim = self.ndim();
        let start = vec![0; ndim];
        let shape: Vec<usize> = self.shape().to_vec();
        let stride = vec![1; ndim];
        self.get_slice(&start, &shape, &stride)
    }

    /// Returns the advised maximum number of pixels for a cursor.
    ///
    /// Navigators use this to choose cursor shapes that balance memory
    /// usage against I/O efficiency. The default targets 1,048,576 pixels,
    /// which is about 4 MiB for `f32` and 8 MiB for `f64`, matching the
    /// range documented by C++ casacore.
    fn advised_max_pixels(&self) -> usize {
        1_048_576
    }

    /// Returns a cursor shape that is efficient for iterating.
    ///
    /// The returned shape respects [`advised_max_pixels`](Self::advised_max_pixels)
    /// and the actual lattice shape. Subclasses may override this to
    /// return tile-aligned shapes for paged lattices.
    fn nice_cursor_shape(&self) -> Vec<usize> {
        let max_pixels = self.advised_max_pixels();
        let shape = self.shape();
        if shape.is_empty() {
            return vec![];
        }

        let mut cursor = vec![1usize; shape.len()];
        let mut product = 1usize;

        for (i, &s) in shape.iter().enumerate() {
            let can_fit = max_pixels / product;
            if can_fit == 0 {
                break;
            }
            cursor[i] = s.min(can_fit);
            product *= cursor[i];
        }

        cursor
    }

    /// Enters a temporary traversal-specific cache-tuning scope, if supported.
    ///
    /// Paged lattices may use `hint` to size internal tile caches to better
    /// match the current traversal pattern. The returned scope restores any
    /// previous cache setting on drop.
    fn enter_traversal_cache_scope<'a>(
        &'a self,
        _hint: &TraversalCacheHint,
    ) -> Result<Option<Box<dyn TraversalCacheScope + 'a>>, LatticeError> {
        Ok(None)
    }
}

/// Write access to an N-dimensional lattice.
///
/// Extends [`Lattice<T>`] with mutation methods. Corresponds to the non-const
/// portion of C++ `Lattice<T>` (putSlice, set, etc.).
///
/// Not all lattice implementations are writable — for instance, expression
/// lattices are read-only. The [`Lattice::is_writable`] method indicates
/// whether mutation is supported.
pub trait LatticeMut<T: LatticeElement>: Lattice<T> {
    /// Sets a single element at the given position.
    ///
    /// Returns an error if `position` is out of bounds or the lattice
    /// is not writable.
    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError>;

    /// Writes a rectangular sub-region into the lattice.
    ///
    /// `data` is placed at the given `start` position. The data shape
    /// determines the extent of the region written. No stride is applied
    /// (contiguous write).
    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError>;

    /// Sets all elements of the lattice to `value`.
    ///
    /// Corresponds to C++ `Lattice<T>::set(value)`.
    fn set(&mut self, value: T) -> Result<(), LatticeError>;

    /// Executes `f` while a mutable traversal-specific cache hint is active.
    ///
    /// Paged backends can override this to temporarily retune internal tile
    /// caches for a known mutable access pattern, then restore the previous
    /// setting before returning.
    fn with_traversal_cache_hint_mut<R>(
        &mut self,
        _hint: &TraversalCacheHint,
        f: impl FnOnce(&mut Self) -> Result<R, LatticeError>,
    ) -> Result<R, LatticeError>
    where
        Self: Sized,
    {
        f(self)
    }

    /// Applies a function to every element of the lattice in place.
    ///
    /// This is a convenience method that reads the entire lattice, maps `f`
    /// over each element, and writes the result back. Subclasses may
    /// override for more efficient chunk-based application.
    fn apply(&mut self, f: impl Fn(&T) -> T) -> Result<(), LatticeError>
    where
        Self: Sized,
    {
        self.for_each_chunk_mut(default_chunk_spec(self), |data, _cursor| {
            data.mapv_inplace(|v| f(&v));
            Ok(())
        })
    }

    /// Copies all data from `source` into this lattice.
    ///
    /// The source lattice must have the same shape as this lattice.
    fn copy_data(&mut self, source: &dyn Lattice<T>) -> Result<(), LatticeError> {
        if self.shape() != source.shape() {
            return Err(LatticeError::ShapeMismatch {
                expected: self.shape().to_vec(),
                got: source.shape().to_vec(),
            });
        }
        let spec = copy_chunk_spec(self, source);
        for chunk in TraversalIter::new(source, spec) {
            let chunk = chunk?;
            self.put_slice(&chunk.data, &chunk.cursor.position)?;
        }
        Ok(())
    }

    /// Applies `f` to each chunk selected by `spec`, writing modified chunks
    /// back automatically after each successful callback.
    ///
    /// This provides a simpler mutable traversal API than the explicit
    /// iterator/write-back pattern while still operating chunk by chunk.
    fn for_each_chunk_mut(
        &mut self,
        spec: TraversalSpec,
        mut f: impl FnMut(&mut ArrayD<T>, &TraversalCursor) -> Result<(), LatticeError>,
    ) -> Result<(), LatticeError>
    where
        Self: Sized,
    {
        let hint = TraversalCursorIter::new(
            self.shape().to_vec(),
            self.nice_cursor_shape(),
            spec.clone(),
        )
        .cache_hint();
        let run = |this: &mut Self| -> Result<(), LatticeError> {
            let cursor_iter =
                TraversalCursorIter::new(this.shape().to_vec(), this.nice_cursor_shape(), spec);
            for cursor in cursor_iter {
                let cursor = cursor?;
                let stride = vec![1; cursor.position.len()];
                let mut data = this.get_slice(&cursor.position, &cursor.shape, &stride)?;
                f(&mut data, &cursor)?;
                this.put_slice(&data, &cursor.position)?;
            }
            Ok(())
        };
        if let Some(hint) = hint.as_ref() {
            self.with_traversal_cache_hint_mut(hint, run)
        } else {
            run(self)
        }
    }
}

fn default_chunk_spec<T: LatticeElement, L: Lattice<T> + ?Sized>(lattice: &L) -> TraversalSpec {
    if lattice.is_paged() || lattice.is_persistent() {
        TraversalSpec::tiles()
    } else {
        TraversalSpec::chunks(lattice.shape().to_vec())
    }
}

fn copy_chunk_spec<T: LatticeElement, D: Lattice<T> + ?Sized, S: Lattice<T> + ?Sized>(
    dest: &D,
    source: &S,
) -> TraversalSpec {
    if source.is_paged() || source.is_persistent() {
        TraversalSpec::tiles()
    } else if dest.is_paged() || dest.is_persistent() {
        TraversalSpec::chunks(dest.nice_cursor_shape())
    } else {
        TraversalSpec::chunks(source.shape().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use ndarray::{ArrayD, IxDyn};

    use super::*;
    use crate::ArrayLattice;

    struct CountingPagedArray<T: LatticeElement> {
        inner: ArrayLattice<T>,
        cursor_shape: Vec<usize>,
        get_slice_calls: Cell<usize>,
        put_slice_calls: Cell<usize>,
    }

    impl<T: LatticeElement> CountingPagedArray<T> {
        fn new(data: ArrayD<T>, cursor_shape: Vec<usize>) -> Self {
            Self {
                inner: ArrayLattice::new(data),
                cursor_shape,
                get_slice_calls: Cell::new(0),
                put_slice_calls: Cell::new(0),
            }
        }
    }

    impl<T: LatticeElement> Lattice<T> for CountingPagedArray<T> {
        fn shape(&self) -> &[usize] {
            self.inner.shape()
        }

        fn is_paged(&self) -> bool {
            true
        }

        fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
            self.inner.get_at(position)
        }

        fn get_slice(
            &self,
            start: &[usize],
            shape: &[usize],
            stride: &[usize],
        ) -> Result<ArrayD<T>, LatticeError> {
            self.get_slice_calls.set(self.get_slice_calls.get() + 1);
            self.inner.get_slice(start, shape, stride)
        }

        fn nice_cursor_shape(&self) -> Vec<usize> {
            self.cursor_shape.clone()
        }
    }

    impl<T: LatticeElement> LatticeMut<T> for CountingPagedArray<T> {
        fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
            self.inner.put_at(value, position)
        }

        fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
            self.put_slice_calls.set(self.put_slice_calls.get() + 1);
            self.inner.put_slice(data, start)
        }

        fn set(&mut self, value: T) -> Result<(), LatticeError> {
            self.inner.set(value)
        }
    }

    #[test]
    fn apply_uses_chunked_mutation_for_paged_lattices() {
        let data = ArrayD::from_shape_fn(IxDyn(&[4, 4]), |_| 1.0f32);
        let mut lat = CountingPagedArray::new(data, vec![2, 2]);
        lat.apply(|value| value * 3.0).unwrap();

        assert_eq!(lat.get_slice_calls.get(), 4);
        assert_eq!(lat.put_slice_calls.get(), 4);
        assert!(lat.inner.get().unwrap().iter().all(|&value| value == 3.0));
    }

    #[test]
    fn copy_data_uses_chunked_reads_for_paged_sources() {
        let src_data = ArrayD::from_shape_fn(IxDyn(&[4, 4]), |idx| (idx[0] * 4 + idx[1]) as f32);
        let src = CountingPagedArray::new(src_data.clone(), vec![2, 2]);
        let mut dst = ArrayLattice::<f32>::zeros(vec![4, 4]);
        dst.copy_data(&src).unwrap();

        assert_eq!(src.get_slice_calls.get(), 4);
        assert_eq!(dst.get().unwrap(), src_data);
    }
}
