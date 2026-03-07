// SPDX-License-Identifier: LGPL-3.0-or-later
//! Disk-backed lattice using tiled table storage.

use std::cell::RefCell;
use std::path::{Path, PathBuf};

use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casacore_types::{RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn};

use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::lattice::{Lattice, LatticeMut};
use crate::tiled_shape::TiledShape;
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
    shape: Vec<usize>,
    tile_shape: Vec<usize>,
    path: Option<PathBuf>,
    _phantom: std::marker::PhantomData<T>,
}

/// Helper to map TableError to LatticeError.
fn table_err(e: casacore_tables::TableError) -> LatticeError {
    LatticeError::Table(e.to_string())
}

impl<T: LatticeElement> PagedArray<T> {
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

        Ok(Self {
            table: RefCell::new(Some(table)),
            shape,
            tile_shape,
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
            shape,
            tile_shape,
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
        let cell = table.cell(0, COLUMN_NAME).ok_or_else(|| {
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

        // Use default tile shape (actual tile shape is embedded in DM metadata).
        let tile_shape = TiledShape::default_tile_shape(&shape);

        Ok(Self {
            table: RefCell::new(Some(table)),
            shape,
            tile_shape,
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
        if let Some(ref table) = *self.table.borrow() {
            if let Some(path) = &self.path {
                table.save(self.save_options(path)).map_err(table_err)?;
            }
        }
        *self.table.get_mut() = None;
        Ok(())
    }

    /// Explicitly reopens a temp-closed array from disk.
    ///
    /// No-op if the table is already open or if this is a scratch array.
    pub fn reopen(&mut self) -> Result<(), LatticeError> {
        if self.table.get_mut().is_some() || self.path.is_none() {
            return Ok(());
        }
        self.auto_reopen()
    }

    /// Ensures the table is open, reopening from disk if needed.
    ///
    /// Uses interior mutability so that `&self` read methods can
    /// transparently reopen after `temp_close()`.
    fn auto_reopen(&self) -> Result<(), LatticeError> {
        if self.table.borrow().is_none() {
            if let Some(path) = &self.path {
                let table = Table::open(TableOptions::new(path)).map_err(table_err)?;
                *self.table.borrow_mut() = Some(table);
            } else {
                return Err(LatticeError::Table(
                    "cannot reopen scratch PagedArray after temp_close".to_string(),
                ));
            }
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
            if self.table.borrow().is_some() {
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
        self.read_full_array()
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        self.tile_shape.clone()
    }
}

impl<T: LatticeElement> LatticeMut<T> for PagedArray<T> {
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
        self.write_full_array(&data)
    }
}

impl<T: LatticeElement> std::fmt::Debug for PagedArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedArray")
            .field("shape", &self.shape)
            .field("tile_shape", &self.tile_shape)
            .field("path", &self.path)
            .field("element_type", &T::PRIMITIVE_TYPE)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Reads auto-reopen via interior mutability.
        assert_eq!(pa.get_at(&[0, 0]).unwrap(), 7.5);
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

        // get() (trait &self method) should transparently reopen.
        let data = pa.get().unwrap();
        assert!(data.iter().all(|&v| v == 3.0));
        assert!(!pa.is_temp_closed());
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
