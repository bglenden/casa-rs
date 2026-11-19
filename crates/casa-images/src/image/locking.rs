// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::RefCell;
use std::path::Path;

use casa_tables::{LockMode, LockOptions, LockType, Table, TableOptions};

use super::{AnyPagedImage, ImageError, ImagePixel, ImagePixelType, PagedImage};

impl AnyPagedImage {
    /// Opens an existing image and retains a read lock on its table.
    ///
    /// Uses casacore-compatible locking and keeps the tiled payload lazy.
    /// The read lock is acquired before reading metadata. Applications can
    /// acquire a write lock before a mutation with [`PagedImage::lock`].
    pub fn open_with_lock(path: impl AsRef<Path>) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();
        let table = Table::open_with_lock(
            TableOptions::new(&path),
            LockOptions::new(LockMode::AutoLocking),
        )?;
        let tiled_io = PagedImage::<f32>::open_tiled_io(&path, 0)
            .ok()
            .map(RefCell::new);
        let kind = ImagePixelType::from_primitive_type(
            PagedImage::<f32>::map_column_primitive_type(&table, tiled_io.as_ref())?,
        )?;
        match kind {
            ImagePixelType::Float32 => {
                PagedImage::from_disk(path, table, tiled_io, 0).map(Self::Float32)
            }
            ImagePixelType::Float64 => {
                PagedImage::from_disk(path, table, tiled_io, 0).map(Self::Float64)
            }
            ImagePixelType::Complex32 => {
                PagedImage::from_disk(path, table, tiled_io, 0).map(Self::Complex32)
            }
            ImagePixelType::Complex64 => {
                PagedImage::from_disk(path, table, tiled_io, 0).map(Self::Complex64)
            }
        }
    }
}

impl<T: ImagePixel> PagedImage<T> {
    /// Acquires a casacore-compatible table lock on a locking-enabled image.
    ///
    /// See [`Table::lock`] for the meaning of `nattempts`. A new table
    /// generation refreshes the coordinates and invalidates image caches.
    pub fn lock(&mut self, kind: LockType, nattempts: u32) -> Result<bool, ImageError> {
        if !self.has_lock(kind) && !self.table.lock(kind, nattempts)? {
            return Ok(false);
        }
        let counter = self.table.locked_modify_counter()?;
        if self.cached_modify_counter != Some(counter) {
            self.refresh_image_cache()?;
            self.cached_modify_counter = Some(counter);
        }
        Ok(true)
    }

    /// Reports whether this handle holds the given lock.
    pub fn has_lock(&self, kind: LockType) -> bool {
        self.table.has_lock(kind)
    }

    /// Flushes a held write lock and releases this handle's table lock.
    pub fn unlock(&mut self) -> Result<(), ImageError> {
        if self.has_lock(LockType::Write) {
            self.save()?;
            self.table.publish_writes()?;
            self.cached_modify_counter = Some(self.table.locked_modify_counter()?);
        }
        if self.tiled_io.is_some() {
            self.table.unlock_metadata_only()?;
        } else {
            self.table.unlock()?;
        }
        Ok(())
    }

    /// Yields a held automatic lock if another process requested it.
    ///
    /// Checks the casacore request list at the table's inspection interval.
    pub fn release_requested_lock(&mut self) -> Result<(), ImageError> {
        if self.table.auto_lock_release_requested()? {
            self.unlock()?;
        }
        Ok(())
    }

    fn refresh_image_cache(&mut self) -> Result<(), ImageError> {
        let (coords, units, misc_info) = Self::read_table_metadata(&self.table)?;
        if let (Some(path), Some(tile)) = (&self.path, &mut self.tiled_io) {
            let refreshed = Self::open_tiled_io(path, self.max_cache_bytes.get())?;
            self.shape = refreshed.cube_shape().to_vec();
            self.tile_shape = refreshed.tile_shape().to_vec();
            *tile.get_mut() = refreshed;
        } else {
            self.shape = Self::map_column_shape(&self.table)?;
        }
        self.coords = coords;
        self.units = units;
        self.misc_info = misc_info;
        self.persistent_masks.get_mut().clear();
        Ok(())
    }
}
