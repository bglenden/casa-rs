// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core lattice traits for read and write access.

use ndarray::ArrayD;

use crate::element::LatticeElement;
use crate::error::LatticeError;

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
    /// usage against I/O efficiency. The default (512 KiB worth of
    /// elements) matches the C++ casacore heuristic.
    fn advised_max_pixels(&self) -> usize {
        // ~512 KiB at 8 bytes/element = 65536 elements
        65536
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

    /// Applies a function to every element of the lattice in place.
    ///
    /// This is a convenience method that reads the entire lattice, maps `f`
    /// over each element, and writes the result back. Subclasses may
    /// override for more efficient chunk-based application.
    fn apply(&mut self, f: impl Fn(&T) -> T) -> Result<(), LatticeError> {
        let mut data = self.get()?;
        data.mapv_inplace(|v| f(&v));
        let start = vec![0; self.ndim()];
        self.put_slice(&data, &start)
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
        let data = source.get()?;
        let start = vec![0; self.ndim()];
        self.put_slice(&data, &start)
    }
}
