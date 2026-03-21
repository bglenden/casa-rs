// SPDX-License-Identifier: LGPL-3.0-or-later
//! Disk-backed lattice using tiled table storage.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};

use casacore_tables::{
    ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema, TilePixel, TiledFileIO,
};
use casacore_types::{Complex32, Complex64, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn};

use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::lattice::{Lattice, LatticeMut};
use crate::tiled_shape::TiledShape;
use crate::traversal::{TraversalCacheHint, TraversalCacheScope, recommended_tile_cache_size};
use crate::value_bridge;

/// Column name used by `PagedArray` — matches C++ casacore convention.
const COLUMN_NAME: &str = "PagedArray";

/// A disk-backed N-dimensional lattice using tiled table storage.
///
/// Corresponds to the C++ `PagedArray<T>` class. Data is stored in a
/// one-row casacore table with a single array column named `"PagedArray"`,
/// using `TiledCellStMan` for tiled I/O.
///
/// # On-disk format
///
/// The table has:
/// - One column named `"PagedArray"` with fixed array shape
/// - One row containing the entire lattice as a single array cell
/// - `TiledCellStMan` storage manager with the specified tile shape
///
/// This format is fully compatible with C++ casacore's `PagedArray`.
///
/// # Examples
///
/// ```rust,no_run
/// use casacore_lattices::{PagedArray, TiledShape, Lattice, LatticeMut};
///
/// // Create a new 64x64 PagedArray on disk:
/// let ts = TiledShape::new(vec![64, 64]);
/// let mut pa = PagedArray::<f64>::create(ts, "/tmp/test_paged.table").unwrap();
/// pa.set(1.0).unwrap();
///
/// // Reopen:
/// let pa2 = PagedArray::<f64>::open("/tmp/test_paged.table").unwrap();
/// assert_eq!(pa2.shape(), &[64, 64]);
/// ```
pub struct PagedArray<T: LatticeElement> {
    /// Interior mutability for the table handle, enabling auto-reopen
    /// from `&self` read methods after `temp_close()`.
    table: RefCell<Option<Table>>,
    /// Direct tile-level access to the underlying `TiledCellStMan` payload.
    ///
    /// This mirrors C++ `PagedArray`'s direct slice access through the tiled
    /// array accessor instead of materializing the entire lattice for each
    /// unit-stride slice operation.
    tiled_io: RefCell<Option<TiledFileIO>>,
    shape: Vec<usize>,
    tile_shape: Vec<usize>,
    max_cache_bytes: Cell<usize>,
    path: Option<PathBuf>,
    _phantom: std::marker::PhantomData<T>,
}

struct PagedArrayTraversalCacheScope<'a, T: LatticeElement> {
    array: &'a PagedArray<T>,
    previous_cache_bytes: usize,
}

impl<T: LatticeElement> TraversalCacheScope for PagedArrayTraversalCacheScope<'_, T> {}

impl<T: LatticeElement> Drop for PagedArrayTraversalCacheScope<'_, T> {
    fn drop(&mut self) {
        let _ = self.array.set_cache_bytes_shared(self.previous_cache_bytes);
    }
}

/// Helper to map TableError to LatticeError.
fn table_err(e: casacore_tables::TableError) -> LatticeError {
    LatticeError::Table(e.to_string())
}

fn tiled_io_err(e: casacore_tables::StorageError) -> LatticeError {
    LatticeError::Table(e.to_string())
}

impl<T: LatticeElement> PagedArray<T> {
    fn supports_tiled_io() -> bool {
        matches!(
            T::PRIMITIVE_TYPE,
            PrimitiveType::Float32
                | PrimitiveType::Float64
                | PrimitiveType::Complex32
                | PrimitiveType::Complex64
        )
    }

    fn open_tiled_io(path: &Path, max_cache_bytes: usize) -> Result<TiledFileIO, LatticeError> {
        if max_cache_bytes == 0 {
            TiledFileIO::open(path, 0).map_err(tiled_io_err)
        } else {
            TiledFileIO::open_with_cache_limit(path, 0, max_cache_bytes).map_err(tiled_io_err)
        }
    }

    fn element_size_bytes() -> usize {
        T::PRIMITIVE_TYPE.fixed_width_bytes().unwrap_or(0)
    }

    fn tile_pixels(&self) -> usize {
        self.tile_shape.iter().product()
    }

    fn refresh_tiled_io(&mut self) -> Result<(), LatticeError> {
        if self.path.is_none() || !Self::supports_tiled_io() {
            return Ok(());
        }

        if let Some(ref mut tio) = *self.tiled_io.get_mut() {
            tio.flush().map_err(tiled_io_err)?;
        } else if self.table.get_mut().is_some() {
            self.flush()?;
        }
        let path = self.path.as_ref().expect("persistent path");
        let tiled_io = Self::open_tiled_io(path, self.max_cache_bytes.get())?;
        *self.tiled_io.get_mut() = Some(tiled_io);
        Ok(())
    }

    fn refresh_tiled_io_shared(&self) -> Result<(), LatticeError> {
        if self.path.is_none() || !Self::supports_tiled_io() {
            return Ok(());
        }

        if let Some(ref mut tio) = *self.tiled_io.borrow_mut() {
            tio.flush().map_err(tiled_io_err)?;
        } else if self.table.borrow().is_some() {
            self.flush()?;
        }
        let path = self.path.as_ref().expect("persistent path");
        let tiled_io = Self::open_tiled_io(path, self.max_cache_bytes.get())?;
        *self.tiled_io.borrow_mut() = Some(tiled_io);
        Ok(())
    }

    fn set_cache_bytes_shared(&self, max_cache_bytes: usize) -> Result<(), LatticeError> {
        self.max_cache_bytes.set(max_cache_bytes);
        self.refresh_tiled_io_shared()
    }

    fn cast_tiled_array<U: 'static>(data: ArrayD<U>) -> Result<ArrayD<T>, LatticeError> {
        let boxed: Box<dyn Any> = Box::new(data);
        boxed
            .downcast::<ArrayD<T>>()
            .map(|boxed| *boxed)
            .map_err(|_| LatticeError::Table("internal PagedArray tiled type mismatch".to_string()))
    }

    fn tiled_get_slice(
        tio: &mut TiledFileIO,
        start: &[usize],
        shape: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        match T::PRIMITIVE_TYPE {
            PrimitiveType::Float32 => {
                Self::cast_tiled_array(tio.get_slice::<f32>(start, shape).map_err(tiled_io_err)?)
            }
            PrimitiveType::Float64 => {
                Self::cast_tiled_array(tio.get_slice::<f64>(start, shape).map_err(tiled_io_err)?)
            }
            PrimitiveType::Complex32 => Self::cast_tiled_array(
                tio.get_slice::<Complex32>(start, shape)
                    .map_err(tiled_io_err)?,
            ),
            PrimitiveType::Complex64 => Self::cast_tiled_array(
                tio.get_slice::<Complex64>(start, shape)
                    .map_err(tiled_io_err)?,
            ),
            _ => Err(LatticeError::Table(
                "tiled I/O is not supported for this PagedArray element type".to_string(),
            )),
        }
    }

    fn tiled_get_all(tio: &mut TiledFileIO) -> Result<ArrayD<T>, LatticeError> {
        match T::PRIMITIVE_TYPE {
            PrimitiveType::Float32 => {
                Self::cast_tiled_array(tio.get_all::<f32>().map_err(tiled_io_err)?)
            }
            PrimitiveType::Float64 => {
                Self::cast_tiled_array(tio.get_all::<f64>().map_err(tiled_io_err)?)
            }
            PrimitiveType::Complex32 => {
                Self::cast_tiled_array(tio.get_all::<Complex32>().map_err(tiled_io_err)?)
            }
            PrimitiveType::Complex64 => {
                Self::cast_tiled_array(tio.get_all::<Complex64>().map_err(tiled_io_err)?)
            }
            _ => Err(LatticeError::Table(
                "tiled I/O is not supported for this PagedArray element type".to_string(),
            )),
        }
    }

    fn tiled_put_slice(
        tio: &mut TiledFileIO,
        data: &ArrayD<T>,
        start: &[usize],
    ) -> Result<(), LatticeError> {
        match T::PRIMITIVE_TYPE {
            PrimitiveType::Float32 => Self::tiled_put_slice_typed::<f32>(tio, data, start),
            PrimitiveType::Float64 => Self::tiled_put_slice_typed::<f64>(tio, data, start),
            PrimitiveType::Complex32 => Self::tiled_put_slice_typed::<Complex32>(tio, data, start),
            PrimitiveType::Complex64 => Self::tiled_put_slice_typed::<Complex64>(tio, data, start),
            _ => Err(LatticeError::Table(
                "tiled I/O is not supported for this PagedArray element type".to_string(),
            )),
        }
    }

    fn tiled_put_slice_typed<U: 'static + Clone + TilePixel>(
        tio: &mut TiledFileIO,
        data: &ArrayD<T>,
        start: &[usize],
    ) -> Result<(), LatticeError> {
        let typed = (data as &dyn Any)
            .downcast_ref::<ArrayD<U>>()
            .ok_or_else(|| {
                LatticeError::Table("internal PagedArray tiled type mismatch".to_string())
            })?;
        let fortran_view = typed.t();
        if let Some(slice) = fortran_view.as_slice() {
            tio.put_slice_fortran::<U>(slice, start, typed.shape())
                .map_err(tiled_io_err)?;
            return Ok(());
        }
        let contiguous = typed.as_standard_layout();
        let slice = contiguous.as_slice().expect("contiguous C-order data");
        tio.put_slice_c_order::<U>(slice, start, typed.shape())
            .map_err(tiled_io_err)
    }

    /// Creates a new `PagedArray` at the given path with the specified shape.
    ///
    /// The table is created immediately on disk. The array is initialized
    /// with zero/default values.
    pub fn create(tiled_shape: TiledShape, path: impl AsRef<Path>) -> Result<Self, LatticeError> {
        let path = path.as_ref();
        let shape = tiled_shape.shape().to_vec();
        let tile_shape = tiled_shape.tile_shape();
        let ndim = shape.len();

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            COLUMN_NAME,
            T::PRIMITIVE_TYPE,
            shape.clone(),
        )])
        .map_err(|e| LatticeError::Table(e.to_string()))?;

        let mut table = Table::with_schema(schema);

        // Add one row with a default-filled array.
        let data = ArrayD::from_elem(IxDyn(&shape), T::default_value());
        let array_value = value_bridge::to_array_value(&data);
        let row = RecordValue::new(vec![RecordField::new(
            COLUMN_NAME,
            Value::Array(array_value),
        )]);
        table.add_row(row).map_err(table_err)?;

        table
            .save(
                TableOptions::new(path)
                    .with_data_manager(DataManagerKind::TiledCellStMan)
                    .with_tile_shape(if ndim > 0 {
                        tile_shape.clone()
                    } else {
                        Vec::new()
                    }),
            )
            .map_err(table_err)?;

        let tiled_io = if Self::supports_tiled_io() {
            Some(Self::open_tiled_io(path, 0)?)
        } else {
            None
        };

        Ok(Self {
            table: RefCell::new(Some(table)),
            tiled_io: RefCell::new(tiled_io),
            shape,
            tile_shape,
            max_cache_bytes: Cell::new(0),
            path: Some(path.to_path_buf()),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Creates a scratch (temporary) `PagedArray` that is not persisted.
    ///
    /// The data lives in a memory-backed table. Equivalent to C++
    /// `PagedArray(shape)` with no path argument.
    pub fn new_scratch(tiled_shape: TiledShape) -> Result<Self, LatticeError> {
        let shape = tiled_shape.shape().to_vec();
        let tile_shape = tiled_shape.tile_shape();

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            COLUMN_NAME,
            T::PRIMITIVE_TYPE,
            shape.clone(),
        )])
        .map_err(|e| LatticeError::Table(e.to_string()))?;

        let mut table = Table::with_schema_memory(schema);

        let data = ArrayD::from_elem(IxDyn(&shape), T::default_value());
        let array_value = value_bridge::to_array_value(&data);
        let row = RecordValue::new(vec![RecordField::new(
            COLUMN_NAME,
            Value::Array(array_value),
        )]);
        table.add_row(row).map_err(table_err)?;

        Ok(Self {
            table: RefCell::new(Some(table)),
            tiled_io: RefCell::new(None),
            shape,
            tile_shape,
            max_cache_bytes: Cell::new(0),
            path: None,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Opens an existing `PagedArray` from disk.
    ///
    /// The table must have been created by a `PagedArray` (Rust or C++),
    /// with a single column named `"PagedArray"`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LatticeError> {
        let path = path.as_ref();
        let table = Table::open(TableOptions::new(path)).map_err(table_err)?;

        // Read the shape from the cell.
        let cell = table
            .cell(0, COLUMN_NAME)
            .map_err(table_err)?
            .ok_or_else(|| {
                LatticeError::Table("PagedArray column not found or no rows".to_string())
            })?;

        let shape = match cell {
            Value::Array(av) => av.shape().to_vec(),
            _ => {
                return Err(LatticeError::Table(
                    "PagedArray column is not an array".to_string(),
                ));
            }
        };

        let tiled_io = if Self::supports_tiled_io() {
            Some(Self::open_tiled_io(path, 0)?)
        } else {
            None
        };
        let (shape, tile_shape) = if let Some(ref tio) = tiled_io {
            (tio.cube_shape().to_vec(), tio.tile_shape().to_vec())
        } else {
            (shape.clone(), TiledShape::default_tile_shape(&shape))
        };

        Ok(Self {
            table: RefCell::new(Some(table)),
            tiled_io: RefCell::new(tiled_io),
            shape,
            tile_shape,
            max_cache_bytes: Cell::new(0),
            path: Some(path.to_path_buf()),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Returns the tile shape used by this `PagedArray`.
    pub fn tile_shape(&self) -> &[usize] {
        &self.tile_shape
    }

    /// Returns the filesystem path, if any.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Returns the configured maximum tile-cache size in pixels.
    ///
    /// A value of `0` means "no explicit maximum", matching C++ casacore's
    /// `PagedArray::maximumCacheSize()` convention.
    pub fn maximum_cache_size_pixels(&self) -> usize {
        let elem_size = Self::element_size_bytes();
        let max_cache_bytes = self.max_cache_bytes.get();
        if elem_size == 0 || max_cache_bytes == 0 {
            0
        } else {
            max_cache_bytes / elem_size
        }
    }

    /// Sets the maximum tile-cache size in pixels.
    ///
    /// A value of `0` removes the explicit limit. Persistent arrays reopen
    /// their tiled I/O handle so subsequent reads use the new cache policy.
    ///
    /// Mirrors C++ `PagedArray::setMaximumCacheSize`.
    pub fn set_maximum_cache_size_pixels(
        &mut self,
        how_many_pixels: usize,
    ) -> Result<(), LatticeError> {
        let elem_size = Self::element_size_bytes();
        let max_cache_bytes = if elem_size == 0 {
            0
        } else {
            how_many_pixels.saturating_mul(elem_size)
        };
        self.max_cache_bytes.set(max_cache_bytes);
        self.refresh_tiled_io()
    }

    /// Sets the cache size to hold approximately `how_many_tiles` tiles.
    ///
    /// A value of `0` removes the explicit maximum. Persistent arrays reopen
    /// their tiled I/O handle so subsequent reads use the new cache policy.
    ///
    /// Mirrors C++ `PagedArray::setCacheSizeInTiles`.
    pub fn set_cache_size_in_tiles(&mut self, how_many_tiles: usize) -> Result<(), LatticeError> {
        if how_many_tiles == 0 {
            return self.set_maximum_cache_size_pixels(0);
        }
        let pixels = self.tile_pixels().saturating_mul(how_many_tiles);
        self.set_maximum_cache_size_pixels(pixels)
    }

    /// Returns `true` if the array has been temp-closed.
    pub fn is_temp_closed(&self) -> bool {
        self.table.borrow().is_none()
    }

    /// Releases the in-memory table, flushing to disk first if persistent.
    ///
    /// For scratch (no-path) arrays this is a no-op. After `temp_close()`,
    /// subsequent reads/writes will auto-reopen from disk.
    pub fn temp_close(&mut self) -> Result<(), LatticeError> {
        if self.path.is_none() {
            return Ok(());
        }
        self.flush()?;
        *self.table.get_mut() = None;
        *self.tiled_io.get_mut() = None;
        Ok(())
    }

    /// Explicitly reopens a temp-closed array from disk.
    ///
    /// No-op if the table is already open or if this is a scratch array.
    pub fn reopen(&mut self) -> Result<(), LatticeError> {
        if (self.table.get_mut().is_some() && self.tiled_io.get_mut().is_some())
            || self.path.is_none()
        {
            return Ok(());
        }
        self.auto_reopen()
    }

    /// Ensures the table is open, reopening from disk if needed.
    ///
    /// Uses interior mutability so that `&self` read methods can
    /// transparently reopen after `temp_close()`.
    fn auto_reopen(&self) -> Result<(), LatticeError> {
        let needs_tiled_io = self.path.is_some() && Self::supports_tiled_io();
        if self.table.borrow().is_none() || (needs_tiled_io && self.tiled_io.borrow().is_none()) {
            let Some(path) = &self.path else {
                return Err(LatticeError::Table(
                    "cannot reopen scratch PagedArray after temp_close".to_string(),
                ));
            };
            if self.table.borrow().is_none() {
                let table = Table::open(TableOptions::new(path)).map_err(table_err)?;
                *self.table.borrow_mut() = Some(table);
            }
            if needs_tiled_io && self.tiled_io.borrow().is_none() {
                let tiled_io = Self::open_tiled_io(path, self.max_cache_bytes.get())?;
                *self.tiled_io.borrow_mut() = Some(tiled_io);
            }
        }
        Ok(())
    }

    fn auto_reopen_tiled_io(&self) -> Result<(), LatticeError> {
        if self.path.is_none() || !Self::supports_tiled_io() {
            return Ok(());
        }
        if self.tiled_io.borrow().is_none() {
            let path = self.path.as_ref().expect("persistent path");
            let tiled_io = Self::open_tiled_io(path, self.max_cache_bytes.get())?;
            *self.tiled_io.borrow_mut() = Some(tiled_io);
        }
        Ok(())
    }

    fn save_options(&self, path: &Path) -> TableOptions {
        TableOptions::new(path)
            .with_data_manager(DataManagerKind::TiledCellStMan)
            .with_tile_shape(self.tile_shape.clone())
    }

    /// Flushes changes to disk (for persistent arrays).
    pub fn flush(&self) -> Result<(), LatticeError> {
        if let Some(path) = &self.path {
            self.auto_reopen()?;
            if let Some(ref mut tio) = *self.tiled_io.borrow_mut() {
                tio.flush().map_err(tiled_io_err)?;
            } else if self.table.borrow().is_some() {
                self.table
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .save(self.save_options(path))
                    .map_err(table_err)?;
            }
        }
        Ok(())
    }

    /// Reads the full array, auto-reopening if temp-closed.
    fn read_full_array(&self) -> Result<ArrayD<T>, LatticeError> {
        self.auto_reopen()?;
        let table_ref = self.table.borrow();
        let table = table_ref.as_ref().unwrap();
        let cell = table
            .cell(0, COLUMN_NAME)
            .map_err(table_err)?
            .ok_or_else(|| LatticeError::Table("PagedArray cell not found".to_string()))?;
        match cell {
            Value::Array(av) => value_bridge::from_array_value(av.clone()),
            _ => Err(LatticeError::Table("unexpected scalar cell".to_string())),
        }
    }

    /// Writes the full array to the table cell.
    fn write_full_array(&mut self, data: &ArrayD<T>) -> Result<(), LatticeError> {
        self.auto_reopen()?;
        let array_value = value_bridge::to_array_value(data);
        self.table
            .get_mut()
            .as_mut()
            .unwrap()
            .set_cell(0, COLUMN_NAME, Value::Array(array_value))
            .map_err(table_err)
    }
}

impl<T: LatticeElement> Lattice<T> for PagedArray<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_persistent(&self) -> bool {
        self.path.is_some()
    }

    fn is_paged(&self) -> bool {
        true
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        if position.len() != self.shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.shape.len(),
                got: position.len(),
            });
        }
        for (&p, &s) in position.iter().zip(self.shape.iter()) {
            if p >= s {
                return Err(LatticeError::IndexOutOfBounds {
                    index: position.to_vec(),
                    shape: self.shape.clone(),
                });
            }
        }

        if self.path.is_some() {
            self.auto_reopen_tiled_io()?;
            if let Some(ref mut tio) = *self.tiled_io.borrow_mut() {
                let ones = vec![1; self.shape.len()];
                let arr = Self::tiled_get_slice(tio, position, &ones)?;
                return arr
                    .into_iter()
                    .next()
                    .ok_or_else(|| LatticeError::Table("empty tiled slice".to_string()));
            }
        }

        let arr = self.read_full_array()?;
        Ok(arr[IxDyn(position)].clone())
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        let ndim = self.shape.len();
        if start.len() != ndim || shape.len() != ndim || stride.len() != ndim {
            return Err(LatticeError::NdimMismatch {
                expected: ndim,
                got: start.len(),
            });
        }

        let is_unit_stride = stride.iter().all(|&s| s == 1);
        if self.path.is_some() && is_unit_stride {
            self.auto_reopen_tiled_io()?;
            if let Some(ref mut tio) = *self.tiled_io.borrow_mut() {
                return Self::tiled_get_slice(tio, start, shape);
            }
        }

        let arr = self.read_full_array()?;

        let slice_info: Vec<ndarray::SliceInfoElem> = start
            .iter()
            .zip(shape.iter())
            .zip(stride.iter())
            .map(|((&s, &n), &st)| {
                let end = if n == 0 { s } else { s + n * st };
                ndarray::SliceInfoElem::Slice {
                    start: s as isize,
                    end: Some(end as isize),
                    step: st as isize,
                }
            })
            .collect();

        let view = arr.slice(slice_info.as_slice());
        Ok(view.to_owned())
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        if self.path.is_some() {
            self.auto_reopen_tiled_io()?;
            if let Some(ref mut tio) = *self.tiled_io.borrow_mut() {
                return Self::tiled_get_all(tio);
            }
        }
        self.read_full_array()
    }

    fn advised_max_pixels(&self) -> usize {
        self.tile_shape.iter().product()
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        self.tile_shape.clone()
    }

    fn enter_traversal_cache_scope<'a>(
        &'a self,
        hint: &TraversalCacheHint,
    ) -> Result<Option<Box<dyn TraversalCacheScope + 'a>>, LatticeError> {
        if self.path.is_none() || !Self::supports_tiled_io() {
            return Ok(None);
        }
        let previous_cache_bytes = self.max_cache_bytes.get();
        let elem_size = Self::element_size_bytes().max(1);
        let recommended_tiles =
            recommended_tile_cache_size(&self.shape, &self.tile_shape, hint, None).max(1);
        let recommended_bytes = recommended_tiles
            .saturating_mul(self.tile_pixels())
            .saturating_mul(elem_size);
        if previous_cache_bytes == recommended_bytes {
            return Ok(None);
        }
        self.set_cache_bytes_shared(recommended_bytes)?;
        Ok(Some(Box::new(PagedArrayTraversalCacheScope {
            array: self,
            previous_cache_bytes,
        })))
    }
}

impl<T: LatticeElement> LatticeMut<T> for PagedArray<T> {
    fn with_traversal_cache_hint_mut<R>(
        &mut self,
        hint: &TraversalCacheHint,
        f: impl FnOnce(&mut Self) -> Result<R, LatticeError>,
    ) -> Result<R, LatticeError> {
        if self.path.is_none() || !Self::supports_tiled_io() {
            return f(self);
        }
        let previous_cache_bytes = self.max_cache_bytes.get();
        let elem_size = Self::element_size_bytes().max(1);
        let recommended_tiles =
            recommended_tile_cache_size(&self.shape, &self.tile_shape, hint, None).max(1);
        let recommended_bytes = recommended_tiles
            .saturating_mul(self.tile_pixels())
            .saturating_mul(elem_size);
        if previous_cache_bytes == recommended_bytes {
            return f(self);
        }

        self.set_cache_bytes_shared(recommended_bytes)?;
        let result = f(self);
        let restore = self.set_cache_bytes_shared(previous_cache_bytes);
        match (result, restore) {
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
        let data = ArrayD::from_elem(IxDyn(&vec![1; position.len()]), value);
        self.put_slice(&data, position)
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        let ndim = self.shape.len();
        if start.len() != ndim {
            return Err(LatticeError::NdimMismatch {
                expected: ndim,
                got: start.len(),
            });
        }
        let end: Vec<usize> = start
            .iter()
            .zip(data.shape().iter())
            .map(|(&s, &n)| s + n)
            .collect();
        for (&limit, &dim) in end.iter().zip(self.shape.iter()) {
            if limit > dim {
                return Err(LatticeError::ShapeMismatch {
                    expected: self.shape.clone(),
                    got: end,
                });
            }
        }

        if self.path.is_some() {
            self.auto_reopen()?;
            if let Some(ref mut tio) = *self.tiled_io.borrow_mut() {
                Self::tiled_put_slice(tio, data, start)?;
                return Ok(());
            }
        }

        let mut current = self.read_full_array()?;

        let slice_info: Vec<ndarray::SliceInfoElem> = start
            .iter()
            .zip(data.shape().iter())
            .map(|(&s, &n)| ndarray::SliceInfoElem::Slice {
                start: s as isize,
                end: Some((s + n) as isize),
                step: 1,
            })
            .collect();

        let mut view = current.slice_mut(slice_info.as_slice());
        view.assign(data);

        self.write_full_array(&current)
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        let data = ArrayD::from_elem(IxDyn(&self.shape), value);
        let start = vec![0; self.shape.len()];
        self.put_slice(&data, &start)
    }
}

impl<T: LatticeElement> std::fmt::Debug for PagedArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedArray")
            .field("shape", &self.shape)
            .field("tile_shape", &self.tile_shape)
            .field("max_cache_bytes", &self.max_cache_bytes.get())
            .field("path", &self.path)
            .field("element_type", &T::PRIMITIVE_TYPE)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LatticeIterExt, TraversalSpec};

    #[test]
    fn scratch_create_and_access() {
        let ts = TiledShape::new(vec![4, 4]);
        let mut pa = PagedArray::<f64>::new_scratch(ts).unwrap();

        assert_eq!(pa.shape(), &[4, 4]);
        assert!(!pa.is_persistent());
        assert!(pa.is_paged());

        // All values should be zero initially.
        assert_eq!(pa.get_at(&[0, 0]).unwrap(), 0.0);

        // Put and get.
        pa.put_at(3.125, &[1, 2]).unwrap();
        assert_eq!(pa.get_at(&[1, 2]).unwrap(), 3.125);
    }

    #[test]
    fn scratch_set_all() {
        let ts = TiledShape::new(vec![3, 3]);
        let mut pa = PagedArray::<i32>::new_scratch(ts).unwrap();
        pa.set(42).unwrap();
        let data = pa.get().unwrap();
        assert!(data.iter().all(|&v| v == 42));
    }

    #[test]
    fn scratch_get_slice() {
        let ts = TiledShape::new(vec![4, 4]);
        let mut pa = PagedArray::<f64>::new_scratch(ts).unwrap();

        let data = ArrayD::from_shape_fn(IxDyn(&[4, 4]), |idx| (idx[0] * 4 + idx[1]) as f64);
        pa.put_slice(&data, &[0, 0]).unwrap();

        let slice = pa.get_slice(&[1, 1], &[2, 2], &[1, 1]).unwrap();
        assert_eq!(slice.shape(), &[2, 2]);
        assert_eq!(slice[IxDyn(&[0, 0])], 5.0);
        assert_eq!(slice[IxDyn(&[1, 1])], 10.0);
    }

    #[test]
    fn create_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_paged.table");

        {
            let ts = TiledShape::new(vec![8, 8]);
            let mut pa = PagedArray::<f64>::create(ts, &path).unwrap();
            pa.set(2.5).unwrap();
            pa.flush().unwrap();
        }

        {
            let pa = PagedArray::<f64>::open(&path).unwrap();
            assert_eq!(pa.shape(), &[8, 8]);
            assert!(pa.is_persistent());
            let data = pa.get().unwrap();
            assert!(data.iter().all(|&v| v == 2.5));
        }
    }

    #[test]
    fn reopen_preserves_explicit_tile_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("explicit_tiles.table");
        let ts = TiledShape::with_tile_shape(vec![16, 16, 16], vec![8, 4, 2]).unwrap();

        let pa = PagedArray::<f32>::create(ts, &path).unwrap();
        assert_eq!(pa.tile_shape(), &[8, 4, 2]);
        pa.flush().unwrap();

        let reopened = PagedArray::<f32>::open(&path).unwrap();
        assert_eq!(reopened.tile_shape(), &[8, 4, 2]);
        assert_eq!(reopened.nice_cursor_shape(), vec![8, 4, 2]);
    }

    #[test]
    fn multiple_types() {
        let ts = TiledShape::new(vec![4]);
        let mut pa_f32 = PagedArray::<f32>::new_scratch(ts.clone()).unwrap();
        pa_f32.set(1.5f32).unwrap();
        assert_eq!(pa_f32.get_at(&[0]).unwrap(), 1.5f32);

        let mut pa_i64 = PagedArray::<i64>::new_scratch(ts).unwrap();
        pa_i64.set(100i64).unwrap();
        assert_eq!(pa_i64.get_at(&[0]).unwrap(), 100i64);
    }

    #[test]
    fn temp_close_and_reopen_persistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("close_reopen.table");
        let ts = TiledShape::new(vec![4, 4]);
        let mut pa = PagedArray::<f64>::create(ts, &path).unwrap();
        pa.set(7.5).unwrap();
        pa.flush().unwrap();

        pa.temp_close().unwrap();
        assert!(pa.is_temp_closed());

        // Reads reopen the tiled payload on demand, but leave the table handle
        // temp-closed until an explicit reopen.
        assert_eq!(pa.get_at(&[0, 0]).unwrap(), 7.5);
        assert!(pa.is_temp_closed());
        pa.reopen().unwrap();
        assert!(!pa.is_temp_closed());
    }

    #[test]
    fn temp_close_read_auto_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auto_reopen.table");
        let ts = TiledShape::new(vec![4, 4]);
        let mut pa = PagedArray::<f64>::create(ts, &path).unwrap();
        pa.set(3.0).unwrap();
        pa.flush().unwrap();

        pa.temp_close().unwrap();
        assert!(pa.is_temp_closed());

        // get() should transparently reopen tiled payload access.
        let data = pa.get().unwrap();
        assert!(data.iter().all(|&v| v == 3.0));
        assert!(pa.is_temp_closed());
        pa.reopen().unwrap();
        assert!(!pa.is_temp_closed());
    }

    #[test]
    fn persistent_plane_by_plane_round_trip_uses_tile_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plane_round_trip.table");
        let ts = TiledShape::with_tile_shape(vec![16, 16, 16], vec![8, 4, 2]).unwrap();
        let mut pa = PagedArray::<f32>::create(ts, &path).unwrap();

        for z in 0..16 {
            let plane = ArrayD::from_shape_fn(IxDyn(&[16, 16, 1]), |idx| {
                (idx[0] + idx[1] * 16 + z * 16 * 16) as f32
            });
            pa.put_slice(&plane, &[0, 0, z]).unwrap();
        }
        pa.flush().unwrap();

        let reopened = PagedArray::<f32>::open(&path).unwrap();
        assert_eq!(reopened.tile_shape(), &[8, 4, 2]);
        for z in 0..16 {
            let plane = reopened
                .get_slice(&[0, 0, z], &[16, 16, 1], &[1, 1, 1])
                .unwrap();
            for x in 0..16 {
                for y in 0..16 {
                    let expected = (x + y * 16 + z * 16 * 16) as f32;
                    assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }

    #[test]
    fn persistent_cache_size_in_tiles_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache_limit.table");
        let ts = TiledShape::with_tile_shape(vec![16, 16, 16], vec![8, 4, 2]).unwrap();
        let mut pa = PagedArray::<f32>::create(ts, &path).unwrap();

        assert_eq!(pa.maximum_cache_size_pixels(), 0);
        pa.set_cache_size_in_tiles(3).unwrap();
        assert_eq!(pa.maximum_cache_size_pixels(), 3 * 8 * 4 * 2);

        pa.temp_close().unwrap();
        pa.reopen().unwrap();
        assert_eq!(pa.maximum_cache_size_pixels(), 3 * 8 * 4 * 2);
    }

    #[test]
    fn traversal_cache_scope_restores_explicit_cache_setting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache_scope.table");
        let ts = TiledShape::with_tile_shape(vec![64, 64, 16], vec![8, 4, 2]).unwrap();
        let mut pa = PagedArray::<f32>::create(ts, &path).unwrap();
        pa.set_cache_size_in_tiles(1).unwrap();
        let original_pixels = pa.maximum_cache_size_pixels();

        {
            let mut iter = pa.traverse(TraversalSpec::lines(1));
            assert_eq!(pa.maximum_cache_size_pixels(), 16 * 8 * 4 * 2);
            let _ = iter.next().unwrap().unwrap();
        }

        assert_eq!(pa.maximum_cache_size_pixels(), original_pixels);
    }

    #[test]
    fn temp_close_scratch_is_noop() {
        let ts = TiledShape::new(vec![4]);
        let mut pa = PagedArray::<f32>::new_scratch(ts).unwrap();
        pa.set(1.0).unwrap();
        pa.temp_close().unwrap();
        assert!(!pa.is_temp_closed());
        assert_eq!(pa.get_at(&[0]).unwrap(), 1.0);
    }
}
