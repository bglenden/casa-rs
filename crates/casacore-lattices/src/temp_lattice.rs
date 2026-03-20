// SPDX-License-Identifier: LGPL-3.0-or-later
//! Automatic memory/disk switching lattice.

use ndarray::ArrayD;

use crate::array_lattice::ArrayLattice;
use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::lattice::{Lattice, LatticeMut};
use crate::paged_array::PagedArray;
use crate::tiled_shape::TiledShape;
use crate::traversal::{TraversalCacheHint, TraversalCacheScope};

/// Default threshold in elements: lattices below this use in-memory storage.
///
/// Matches the C++ casacore `TempLattice` default of 2 MiB (at 8 bytes/element
/// this is 256 Ki elements).
const DEFAULT_THRESHOLD: usize = 256 * 1024;

/// A lattice that automatically chooses memory or scratch-disk storage.
///
/// Corresponds to the C++ `TempLattice<T>` class. If the lattice is small
/// enough (below `max_memory_elements`), data is kept in an
/// [`ArrayLattice`]. Otherwise, a scratch [`PagedArray`] is created on disk
/// in a temporary directory (enabling `temp_close()`/`reopen()` cycles).
///
/// `TempLattice` is always writable. It is persistent only when backed by
/// a scratch `PagedArray`, and it is paged only in that case.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::{TempLattice, Lattice, LatticeMut};
///
/// // Small: stays in memory.
/// let mut lat = TempLattice::<f64>::new(vec![4, 4], None).unwrap();
/// assert!(!lat.is_paged());
/// lat.set(3.0).unwrap();
/// assert_eq!(lat.get_at(&[0, 0]).unwrap(), 3.0);
/// ```
pub enum TempLattice<T: LatticeElement> {
    /// In-memory storage.
    Memory(ArrayLattice<T>),
    /// Scratch-disk storage backed by a temporary directory.
    Paged {
        /// The paged array storing pixel data.
        array: Box<PagedArray<T>>,
        /// Keeps the temp directory alive; cleaned up on drop.
        _dir: tempfile::TempDir,
    },
}

impl<T: LatticeElement> TempLattice<T> {
    /// Creates a new `TempLattice` with the given shape.
    ///
    /// If `max_memory_elements` is `None`, the default threshold
    /// (256 Ki elements) is used. If the total number of elements exceeds
    /// the threshold, a scratch [`PagedArray`] is created in a temporary
    /// directory on disk.
    pub fn new(
        shape: Vec<usize>,
        max_memory_elements: Option<usize>,
    ) -> Result<Self, LatticeError> {
        let nelements: usize = shape.iter().product();
        let threshold = max_memory_elements.unwrap_or(DEFAULT_THRESHOLD);

        if nelements <= threshold {
            Ok(Self::Memory(ArrayLattice::zeros(shape)))
        } else {
            let dir = tempfile::tempdir().map_err(|e| {
                LatticeError::Table(format!("failed to create scratch directory: {e}"))
            })?;
            let table_path = dir.path().join("TempLattice.table");
            let ts = TiledShape::new(shape);
            let pa = PagedArray::create(ts, &table_path)?;
            Ok(Self::Paged {
                array: Box::new(pa),
                _dir: dir,
            })
        }
    }

    /// Returns `true` if the lattice is using in-memory storage.
    pub fn is_in_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }

    /// Returns a reference to the inner `PagedArray`, if paged.
    fn paged_array(&self) -> Option<&PagedArray<T>> {
        match self {
            Self::Memory(_) => None,
            Self::Paged { array, .. } => Some(array),
        }
    }

    /// Returns a mutable reference to the inner `PagedArray`, if paged.
    fn paged_array_mut(&mut self) -> Option<&mut PagedArray<T>> {
        match self {
            Self::Memory(_) => None,
            Self::Paged { array, .. } => Some(array),
        }
    }

    /// Releases the backing table to free memory (paged variant only).
    ///
    /// For the `Memory` variant this is a no-op. For the `Paged` variant
    /// it delegates to [`PagedArray::temp_close`]. After calling this,
    /// subsequent reads/writes will transparently reopen from disk.
    pub fn temp_close(&mut self) -> Result<(), LatticeError> {
        match self.paged_array_mut() {
            Some(pa) => pa.temp_close(),
            None => Ok(()),
        }
    }

    /// Reopens a temp-closed paged lattice from disk.
    ///
    /// No-op for the `Memory` variant or if the paged variant is already open.
    pub fn reopen(&mut self) -> Result<(), LatticeError> {
        match self.paged_array_mut() {
            Some(pa) => pa.reopen(),
            None => Ok(()),
        }
    }

    /// Returns `true` if the backing storage has been temp-closed.
    pub fn is_temp_closed(&self) -> bool {
        self.paged_array().is_some_and(|pa| pa.is_temp_closed())
    }

    /// Returns the configured maximum tile-cache size in pixels.
    ///
    /// Returns `0` for in-memory lattices and for paged lattices with no
    /// explicit maximum.
    pub fn maximum_cache_size_pixels(&self) -> usize {
        self.paged_array()
            .map(PagedArray::maximum_cache_size_pixels)
            .unwrap_or(0)
    }

    /// Sets the maximum tile-cache size in pixels for the paged variant.
    ///
    /// No-op for in-memory lattices. A value of `0` removes the explicit
    /// maximum. Mirrors C++ `TempLattice::setMaximumCacheSize`.
    pub fn set_maximum_cache_size_pixels(
        &mut self,
        how_many_pixels: usize,
    ) -> Result<(), LatticeError> {
        match self.paged_array_mut() {
            Some(pa) => pa.set_maximum_cache_size_pixels(how_many_pixels),
            None => Ok(()),
        }
    }

    /// Sets the tile cache to hold approximately `how_many_tiles` tiles.
    ///
    /// No-op for in-memory lattices. A value of `0` removes the explicit
    /// maximum. Mirrors C++ `TempLattice::setCacheSizeInTiles`.
    pub fn set_cache_size_in_tiles(&mut self, how_many_tiles: usize) -> Result<(), LatticeError> {
        match self.paged_array_mut() {
            Some(pa) => pa.set_cache_size_in_tiles(how_many_tiles),
            None => Ok(()),
        }
    }
}

impl<T: LatticeElement> Lattice<T> for TempLattice<T> {
    fn shape(&self) -> &[usize] {
        match self {
            Self::Memory(l) => l.shape(),
            Self::Paged { array, .. } => array.shape(),
        }
    }

    fn is_persistent(&self) -> bool {
        match self {
            Self::Memory(l) => l.is_persistent(),
            Self::Paged { array, .. } => array.is_persistent(),
        }
    }

    fn is_paged(&self) -> bool {
        match self {
            Self::Memory(l) => l.is_paged(),
            Self::Paged { array, .. } => array.is_paged(),
        }
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        match self {
            Self::Memory(l) => l.get_at(position),
            Self::Paged { array, .. } => array.get_at(position),
        }
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        match self {
            Self::Memory(l) => l.get_slice(start, shape, stride),
            Self::Paged { array, .. } => array.get_slice(start, shape, stride),
        }
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        match self {
            Self::Memory(l) => l.get(),
            Self::Paged { array, .. } => array.get(),
        }
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        match self {
            Self::Memory(l) => l.nice_cursor_shape(),
            Self::Paged { array, .. } => array.nice_cursor_shape(),
        }
    }

    fn enter_traversal_cache_scope<'a>(
        &'a self,
        hint: &TraversalCacheHint,
    ) -> Result<Option<Box<dyn TraversalCacheScope + 'a>>, LatticeError> {
        match self {
            Self::Memory(_) => Ok(None),
            Self::Paged { array, .. } => array.enter_traversal_cache_scope(hint),
        }
    }
}

impl<T: LatticeElement> LatticeMut<T> for TempLattice<T> {
    fn with_traversal_cache_hint_mut<R>(
        &mut self,
        hint: &TraversalCacheHint,
        f: impl FnOnce(&mut Self) -> Result<R, LatticeError>,
    ) -> Result<R, LatticeError>
    where
        Self: Sized,
    {
        if self.is_in_memory() {
            return f(self);
        }
        let previous_cache_pixels = self.maximum_cache_size_pixels();
        let recommended_pixels = match self.paged_array() {
            Some(array) => {
                let recommended_tiles = crate::traversal::recommended_tile_cache_size(
                    array.shape(),
                    &array.nice_cursor_shape(),
                    hint,
                    None,
                )
                .max(1);
                recommended_tiles
                    .saturating_mul(array.nice_cursor_shape().iter().product::<usize>())
            }
            None => return f(self),
        };
        if previous_cache_pixels == recommended_pixels {
            return f(self);
        }
        self.set_maximum_cache_size_pixels(recommended_pixels)?;
        let result = f(self);
        let restore = self.set_maximum_cache_size_pixels(previous_cache_pixels);
        match (result, restore) {
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
        match self {
            Self::Memory(l) => l.put_at(value, position),
            Self::Paged { array, .. } => array.put_at(value, position),
        }
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        match self {
            Self::Memory(l) => l.put_slice(data, start),
            Self::Paged { array, .. } => array.put_slice(data, start),
        }
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        match self {
            Self::Memory(l) => l.set(value),
            Self::Paged { array, .. } => array.set(value),
        }
    }
}

impl<T: LatticeElement> std::fmt::Debug for TempLattice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory(l) => f.debug_tuple("TempLattice::Memory").field(l).finish(),
            Self::Paged { array, .. } => f.debug_tuple("TempLattice::Paged").field(array).finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_uses_memory() {
        let lat = TempLattice::<f64>::new(vec![4, 4], None).unwrap();
        assert!(lat.is_in_memory());
        assert!(!lat.is_paged());
    }

    #[test]
    fn large_uses_paged() {
        // Force paging with a threshold of 10 elements.
        let lat = TempLattice::<f64>::new(vec![10, 10], Some(10)).unwrap();
        assert!(!lat.is_in_memory());
        assert!(lat.is_paged());
    }

    #[test]
    fn memory_read_write() {
        let mut lat = TempLattice::<i32>::new(vec![3, 3], None).unwrap();
        lat.set(5).unwrap();
        assert_eq!(lat.get_at(&[1, 1]).unwrap(), 5);
    }

    #[test]
    fn paged_read_write() {
        let mut lat = TempLattice::<f64>::new(vec![4, 4], Some(1)).unwrap();
        lat.set(2.5).unwrap();
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 2.5);
    }

    #[test]
    fn threshold_boundary() {
        // Exactly at threshold: should be memory.
        let lat = TempLattice::<f64>::new(vec![10], Some(10)).unwrap();
        assert!(lat.is_in_memory());

        // One above threshold: should be paged.
        let lat = TempLattice::<f64>::new(vec![11], Some(10)).unwrap();
        assert!(!lat.is_in_memory());
    }

    #[test]
    fn temp_close_reopen_paged() {
        let mut lat = TempLattice::<f64>::new(vec![4, 4], Some(1)).unwrap();
        lat.set(3.0).unwrap();
        assert!(!lat.is_temp_closed());

        // temp_close releases the in-memory table.
        lat.temp_close().unwrap();
        assert!(lat.is_temp_closed());

        // Explicit reopen restores it.
        lat.reopen().unwrap();
        assert!(!lat.is_temp_closed());
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 3.0);
    }

    #[test]
    fn temp_close_paged_auto_reopens_on_read() {
        let mut lat = TempLattice::<f64>::new(vec![4, 4], Some(1)).unwrap();
        lat.set(7.0).unwrap();
        lat.temp_close().unwrap();
        assert!(lat.is_temp_closed());

        // Reading transparently reopens tiled payload access, but the backing
        // table remains temp-closed until an explicit reopen.
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 7.0);
        assert!(lat.is_temp_closed());
        lat.reopen().unwrap();
        assert!(!lat.is_temp_closed());
    }

    #[test]
    fn temp_close_memory_noop() {
        let mut lat = TempLattice::<f64>::new(vec![4, 4], None).unwrap();
        lat.set(5.0).unwrap();
        lat.temp_close().unwrap();
        assert!(!lat.is_temp_closed());
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 5.0);
    }

    #[test]
    fn paged_cache_size_controls_forward_to_paged_array() {
        let mut lat = TempLattice::<f32>::new(vec![16, 16, 16], Some(1)).unwrap();
        assert!(lat.is_paged());
        assert_eq!(lat.maximum_cache_size_pixels(), 0);

        lat.set_cache_size_in_tiles(2).unwrap();
        assert_eq!(lat.maximum_cache_size_pixels(), 2 * 16 * 16 * 16);

        lat.set_maximum_cache_size_pixels(512).unwrap();
        assert_eq!(lat.maximum_cache_size_pixels(), 512);
    }
}
