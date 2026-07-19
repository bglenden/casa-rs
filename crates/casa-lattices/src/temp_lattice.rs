// SPDX-License-Identifier: LGPL-3.0-or-later
//! Automatic memory/disk switching lattice.

use std::path::PathBuf;

use ndarray::ArrayD;

use crate::array_lattice::ArrayLattice;
use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::lattice::{Lattice, LatticeMut};
use crate::paged_array::PagedArray;
use crate::tiled_shape::TiledShape;
use crate::traversal::{TraversalCacheHint, TraversalCacheScope};

/// Location used when a temporary lattice is paged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScratchSpace {
    /// Create and automatically remove a unique system temporary directory.
    SystemTemp,
    /// Create and automatically remove a unique scratch subdirectory here.
    Directory(PathBuf),
}

/// Explicit byte-aware storage policy for temporary lattices and images.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TempStoragePolicy {
    Memory,
    Paged {
        scratch: ScratchSpace,
    },
    Auto {
        memory_budget_bytes: usize,
        scratch: ScratchSpace,
    },
}

/// Pure storage decision returned by [`TempStoragePolicy::plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempStoragePlan {
    shape_bytes: usize,
    paged: bool,
}

impl TempStoragePlan {
    pub fn shape_bytes(self) -> usize {
        self.shape_bytes
    }

    pub fn is_paged(self) -> bool {
        self.paged
    }
}

impl TempStoragePolicy {
    pub fn plan<T: LatticeElement>(
        &self,
        shape: &[usize],
    ) -> Result<TempStoragePlan, LatticeError> {
        if shape.is_empty() || shape.contains(&0) {
            return Err(LatticeError::TileLayout(format!(
                "temporary lattice shape must be non-empty and positive: {shape:?}"
            )));
        }
        let elements = shape.iter().try_fold(1usize, |product, &extent| {
            product.checked_mul(extent).ok_or_else(|| {
                LatticeError::TileLayout("temporary lattice element count overflows usize".into())
            })
        })?;
        let element_bytes = T::PRIMITIVE_TYPE.fixed_width_bytes().ok_or_else(|| {
            LatticeError::TileLayout(format!(
                "temporary storage has no fixed byte width for {:?}",
                T::PRIMITIVE_TYPE
            ))
        })?;
        let shape_bytes = elements.checked_mul(element_bytes).ok_or_else(|| {
            LatticeError::TileLayout("temporary lattice byte size overflows usize".into())
        })?;
        let paged = match self {
            Self::Memory => false,
            Self::Paged { .. } => true,
            Self::Auto {
                memory_budget_bytes,
                ..
            } => shape_bytes > *memory_budget_bytes,
        };
        Ok(TempStoragePlan { shape_bytes, paged })
    }

    fn scratch(&self) -> Option<&ScratchSpace> {
        match self {
            Self::Memory => None,
            Self::Paged { scratch } | Self::Auto { scratch, .. } => Some(scratch),
        }
    }
}

struct ScratchLease {
    _directory: tempfile::TempDir,
}

/// A lattice that automatically chooses memory or scratch-disk storage.
///
/// Corresponds to the C++ `TempLattice<T>` class. [`TempStoragePolicy`] makes
/// the storage choice explicit; its automatic mode compares checked storage
/// bytes with a caller-supplied memory budget.
///
/// `TempLattice` is always writable. It is persistent only when backed by
/// a scratch `PagedArray`, and it is paged only in that case.
///
/// # Examples
///
/// ```rust
/// use casa_lattices::{Lattice, LatticeMut, TempLattice, TempStoragePolicy};
///
/// // Small: stays in memory.
/// let mut lat = TempLattice::<f64>::new(vec![4, 4], TempStoragePolicy::Memory).unwrap();
/// assert!(!lat.is_paged());
/// lat.set(3.0).unwrap();
/// assert_eq!(lat.get_at(&[0, 0]).unwrap(), 3.0);
/// ```
pub struct TempLattice<T: LatticeElement> {
    storage: TempLatticeStorage<T>,
}

enum TempLatticeStorage<T: LatticeElement> {
    Memory(ArrayLattice<T>),
    Paged {
        array: Box<PagedArray<T>>,
        _scratch: ScratchLease,
    },
}

impl<T: LatticeElement> TempLattice<T> {
    /// Creates a new `TempLattice` with the given shape.
    ///
    /// The policy is explicit and measured in bytes; no element-count or
    /// element-width-dependent threshold is hidden in this constructor.
    pub fn new(shape: Vec<usize>, policy: TempStoragePolicy) -> Result<Self, LatticeError> {
        let plan = policy.plan::<T>(&shape)?;

        if !plan.is_paged() {
            Ok(Self {
                storage: TempLatticeStorage::Memory(ArrayLattice::zeros(shape)),
            })
        } else {
            let lease = match policy.scratch().expect("paged policy has scratch") {
                ScratchSpace::SystemTemp => {
                    let dir = tempfile::tempdir().map_err(|e| {
                        LatticeError::Table(format!("failed to create scratch directory: {e}"))
                    })?;
                    ScratchLease { _directory: dir }
                }
                ScratchSpace::Directory(directory) => {
                    std::fs::create_dir_all(directory).map_err(|e| {
                        LatticeError::Table(format!("failed to create scratch directory: {e}"))
                    })?;
                    let dir = tempfile::Builder::new()
                        .prefix("TempLattice-")
                        .tempdir_in(directory)
                        .map_err(|e| {
                            LatticeError::Table(format!(
                                "failed to create unique scratch directory: {e}"
                            ))
                        })?;
                    ScratchLease { _directory: dir }
                }
            };
            let table_path = lease._directory.path().join("TempLattice.table");
            let ts = TiledShape::new(shape, std::mem::size_of::<T>())?;
            let pa = PagedArray::create(ts, &table_path)?;
            Ok(Self {
                storage: TempLatticeStorage::Paged {
                    array: Box::new(pa),
                    _scratch: lease,
                },
            })
        }
    }

    /// Returns `true` if the lattice is using in-memory storage.
    pub fn is_in_memory(&self) -> bool {
        matches!(self.storage, TempLatticeStorage::Memory(_))
    }

    /// Returns a reference to the inner `PagedArray`, if paged.
    fn paged_array(&self) -> Option<&PagedArray<T>> {
        match &self.storage {
            TempLatticeStorage::Memory(_) => None,
            TempLatticeStorage::Paged { array, .. } => Some(array),
        }
    }

    /// Returns a mutable reference to the inner `PagedArray`, if paged.
    fn paged_array_mut(&mut self) -> Option<&mut PagedArray<T>> {
        match &mut self.storage {
            TempLatticeStorage::Memory(_) => None,
            TempLatticeStorage::Paged { array, .. } => Some(array),
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
    /// explicit request; persistent storage then uses its fixed default.
    pub fn maximum_cache_size_pixels(&self) -> usize {
        self.paged_array()
            .map(PagedArray::maximum_cache_size_pixels)
            .unwrap_or(0)
    }

    /// Sets the maximum tile-cache size in pixels for the paged variant.
    ///
    /// No-op for in-memory lattices. A value of `0` removes the explicit
    /// maximum request. Mirrors C++ `TempLattice::setMaximumCacheSize`.
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
    /// maximum request. Mirrors C++ `TempLattice::setCacheSizeInTiles`.
    pub fn set_cache_size_in_tiles(&mut self, how_many_tiles: usize) -> Result<(), LatticeError> {
        match self.paged_array_mut() {
            Some(pa) => pa.set_cache_size_in_tiles(how_many_tiles),
            None => Ok(()),
        }
    }
}

impl<T: LatticeElement> Lattice<T> for TempLattice<T> {
    fn shape(&self) -> &[usize] {
        match &self.storage {
            TempLatticeStorage::Memory(l) => l.shape(),
            TempLatticeStorage::Paged { array, .. } => array.shape(),
        }
    }

    fn is_persistent(&self) -> bool {
        match &self.storage {
            TempLatticeStorage::Memory(l) => l.is_persistent(),
            TempLatticeStorage::Paged { array, .. } => array.is_persistent(),
        }
    }

    fn is_paged(&self) -> bool {
        match &self.storage {
            TempLatticeStorage::Memory(l) => l.is_paged(),
            TempLatticeStorage::Paged { array, .. } => array.is_paged(),
        }
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        match &self.storage {
            TempLatticeStorage::Memory(l) => l.get_at(position),
            TempLatticeStorage::Paged { array, .. } => array.get_at(position),
        }
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        match &self.storage {
            TempLatticeStorage::Memory(l) => l.get_slice(start, shape, stride),
            TempLatticeStorage::Paged { array, .. } => array.get_slice(start, shape, stride),
        }
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        match &self.storage {
            TempLatticeStorage::Memory(l) => l.get(),
            TempLatticeStorage::Paged { array, .. } => array.get(),
        }
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        match &self.storage {
            TempLatticeStorage::Memory(l) => l.nice_cursor_shape(),
            TempLatticeStorage::Paged { array, .. } => array.nice_cursor_shape(),
        }
    }

    fn enter_traversal_cache_scope<'a>(
        &'a self,
        hint: &TraversalCacheHint,
    ) -> Result<Option<Box<dyn TraversalCacheScope + 'a>>, LatticeError> {
        match &self.storage {
            TempLatticeStorage::Memory(_) => Ok(None),
            TempLatticeStorage::Paged { array, .. } => array.enter_traversal_cache_scope(hint),
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
        match &mut self.storage {
            TempLatticeStorage::Memory(l) => l.put_at(value, position),
            TempLatticeStorage::Paged { array, .. } => array.put_at(value, position),
        }
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        match &mut self.storage {
            TempLatticeStorage::Memory(l) => l.put_slice(data, start),
            TempLatticeStorage::Paged { array, .. } => array.put_slice(data, start),
        }
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        match &mut self.storage {
            TempLatticeStorage::Memory(l) => l.set(value),
            TempLatticeStorage::Paged { array, .. } => array.set(value),
        }
    }
}

impl<T: LatticeElement> std::fmt::Debug for TempLattice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.storage {
            TempLatticeStorage::Memory(l) => f.debug_tuple("TempLattice::Memory").field(l).finish(),
            TempLatticeStorage::Paged { array, .. } => {
                f.debug_tuple("TempLattice::Paged").field(array).finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_uses_memory() {
        let lat = TempLattice::<f64>::new(vec![4, 4], TempStoragePolicy::Memory).unwrap();
        assert!(lat.is_in_memory());
        assert!(!lat.is_paged());
    }

    #[test]
    fn large_uses_paged() {
        // Explicit paging is independent of element type and shape size.
        let lat = TempLattice::<f64>::new(
            vec![10, 10],
            TempStoragePolicy::Paged {
                scratch: ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        assert!(!lat.is_in_memory());
        assert!(lat.is_paged());
    }

    #[test]
    fn memory_read_write() {
        let mut lat = TempLattice::<i32>::new(vec![3, 3], TempStoragePolicy::Memory).unwrap();
        lat.set(5).unwrap();
        assert_eq!(lat.get_at(&[1, 1]).unwrap(), 5);
    }

    #[test]
    fn paged_read_write() {
        let mut lat = TempLattice::<f64>::new(
            vec![4, 4],
            TempStoragePolicy::Paged {
                scratch: ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        lat.set(2.5).unwrap();
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 2.5);
    }

    #[test]
    fn byte_budget_boundary() {
        // Ten f64 values are exactly 80 bytes.
        let lat = TempLattice::<f64>::new(
            vec![10],
            TempStoragePolicy::Auto {
                memory_budget_bytes: 80,
                scratch: ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        assert!(lat.is_in_memory());

        // One byte less forces the same shape to scratch storage.
        let lat = TempLattice::<f64>::new(
            vec![10],
            TempStoragePolicy::Auto {
                memory_budget_bytes: 79,
                scratch: ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        assert!(!lat.is_in_memory());
    }

    #[test]
    fn byte_plan_rejects_overflow() {
        let error = TempStoragePolicy::Memory
            .plan::<f64>(&[usize::MAX, 2])
            .unwrap_err();
        assert!(error.to_string().contains("overflows usize"));
    }

    #[test]
    fn directory_scratch_is_unique_and_removed_on_drop() {
        let parent = tempfile::tempdir().unwrap();
        let policy = || TempStoragePolicy::Paged {
            scratch: ScratchSpace::Directory(parent.path().to_path_buf()),
        };

        {
            let mut first = TempLattice::<f32>::new(vec![4, 4], policy()).unwrap();
            let mut second = TempLattice::<f32>::new(vec![4, 4], policy()).unwrap();
            first.set(1.0).unwrap();
            second.set(2.0).unwrap();
            assert_eq!(first.get_at(&[0, 0]).unwrap(), 1.0);
            assert_eq!(second.get_at(&[0, 0]).unwrap(), 2.0);
            assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 2);
        }

        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn temp_close_reopen_paged() {
        let mut lat = TempLattice::<f64>::new(
            vec![4, 4],
            TempStoragePolicy::Paged {
                scratch: ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
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
        let mut lat = TempLattice::<f64>::new(
            vec![4, 4],
            TempStoragePolicy::Paged {
                scratch: ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
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
        let mut lat = TempLattice::<f64>::new(vec![4, 4], TempStoragePolicy::Memory).unwrap();
        lat.set(5.0).unwrap();
        lat.temp_close().unwrap();
        assert!(!lat.is_temp_closed());
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 5.0);
    }

    #[test]
    fn paged_cache_size_controls_forward_to_paged_array() {
        let mut lat = TempLattice::<f32>::new(
            vec![16, 16, 16],
            TempStoragePolicy::Paged {
                scratch: ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        assert!(lat.is_paged());
        assert_eq!(
            lat.maximum_cache_size_pixels(),
            casa_tables::DEFAULT_TILED_ARRAY_CACHE_BYTES / std::mem::size_of::<f32>()
        );

        lat.set_cache_size_in_tiles(2).unwrap();
        assert_eq!(lat.maximum_cache_size_pixels(), 2 * 16 * 16 * 16);

        lat.set_maximum_cache_size_pixels(512).unwrap();
        assert_eq!(lat.maximum_cache_size_pixels(), 512);
    }
}
