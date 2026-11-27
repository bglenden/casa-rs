// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tiled storage managers: `TiledColumnStMan`, `TiledShapeStMan`, `TiledCellStMan`.
//!
//! These store multi-dimensional array data in rectangular tiles within
//! hypercubes. Each tile contains data for all bound columns, interleaved.
//!
//! # On-disk format
//!
//! Metadata is stored in the DM's header file (`table.fN`) as AipsIO
//! (always big-endian framing). Tile data lives in separate `table.fN_TSM0`,
//! `table.fN_TSM1`, ... files using raw bytes in the table's byte order.
//!
//! # C++ equivalents
//!
//! `TiledColumnStMan`, `TiledShapeStMan`, `TiledCellStMan`, `TiledStMan`,
//! `TSMCube`, `TSMFile`.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read as IoRead, Seek, SeekFrom, Write as IoWrite};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use casa_aipsio::{AipsIo, AipsOpenOption};
use casa_core::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::table::{
    SelectedArray1D, SelectedArray1DCells, SelectedArray1DCellsMut, SelectedArray1DShape,
    SelectedArray2D, SelectedArray2DCells, SelectedArray2DCellsMut, SelectedArray2DShape,
};

/// Maximum number of dimensions supported by stack-allocated arrays in
/// tile iteration loops.  casacore images are at most 5-D.
const MAX_NDIM: usize = 8;
const DEFAULT_TABLE_CACHE_BYTES: usize = 64 * 1024 * 1024;
/// Default explicit I/O buffer capacity for one sequential tiled-column writer.
pub const STREAMING_TILED_COLUMN_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const STREAMING_TILED_READ_AHEAD_BYTES: usize = 16 * 1024 * 1024;

#[cfg(test)]
thread_local! {
    static STREAMED_READ_COUNTS: std::cell::Cell<(usize, usize)> = const {
        std::cell::Cell::new((0, 0))
    };
}

#[derive(Clone, Debug, Default)]
struct CountingWriteStats {
    write_calls: usize,
    bytes_written: usize,
    max_write_bytes: usize,
    flush_calls: usize,
}

struct CountingFile {
    file: File,
    stats: CountingWriteStats,
}

impl CountingFile {
    fn create(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            file: File::create(path)?,
            stats: CountingWriteStats::default(),
        })
    }

    fn stats(&self) -> CountingWriteStats {
        self.stats.clone()
    }
}

impl IoWrite for CountingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(buf)?;
        self.stats.write_calls += 1;
        self.stats.bytes_written += written;
        self.stats.max_write_bytes = self.stats.max_write_bytes.max(written);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stats.flush_calls += 1;
        self.file.flush()
    }
}

/// The `tracing` target of the streamed tiled-write events (`debug`).
const STREAMED_TILED_TRACE: &str = "casa_tables::streamed_tiled_write";
/// The `tracing` target of the typed selected-row read profile (`debug`).
const TYPED_2D_READ_PROFILE: &str = "casa_tables::typed_2d_read_profile";

/// Whether the logging configuration takes the streamed tiled-write events.
fn trace_streamed_tiled_writes() -> bool {
    tracing::enabled!(target: STREAMED_TILED_TRACE, tracing::Level::DEBUG)
}

/// Whether the logging configuration takes the typed read profile; when it
/// does not, the reads take no clock.
fn typed_2d_read_profile_enabled() -> bool {
    tracing::enabled!(target: TYPED_2D_READ_PROFILE, tracing::Level::DEBUG)
}

fn duration_ms_f64(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

mod assemble_tiles;
mod columns;
mod file_cache;
mod file_io;
mod header;
mod read_session;
mod save_columns;
mod shared_cache;
mod sparse_rows;
mod streamed_complex;
mod streamed_primitive;
mod streamed_shape;
mod tile_bytes;
mod typed_1d;
mod typed_2d;
mod typed_array;

use assemble_tiles::*;
pub(crate) use columns::collect_tiled_scalar_columns;
pub(crate) use columns::load_tiled_column_rows;
pub(crate) use columns::load_tiled_columns;
use columns::*;
pub(crate) use file_cache::TiledFileIO;
pub use file_cache::TiledFileIoStats;
use file_cache::*;
pub(crate) use header::TiledReadMetadata;
pub(crate) use header::name_from_file;
pub(crate) use header::tiled_2d_channel_tile_width;
pub(crate) use header::tiled_bucket_bytes;
pub(crate) use header::tiled_tile_shape;
use header::*;
use read_session::*;
pub(crate) use save_columns::SingleColumnTiledSaveOptions;
pub(crate) use save_columns::save_empty_tiled_shape_column;
pub(crate) use save_columns::save_tiled_columns;
pub(crate) use save_columns::save_tiled_single_column_values;
pub(crate) use shared_cache::invalidate_shared_tile_cache_for_table;
#[cfg(test)]
pub(crate) use shared_cache::reset_table_cache_budget_for_tests;
pub use shared_cache::set_table_cache_budget_bytes;
#[cfg(test)]
pub(crate) use shared_cache::shared_tile_cache_entry_count;
pub use shared_cache::table_cache_budget_bytes;
use shared_cache::*;
pub(crate) use sparse_rows::save_tiled_columns_sparse_rows_in_place;
pub(crate) use sparse_rows::save_tiled_single_column_rows_in_place;
use sparse_rows::*;
pub use streamed_complex::StreamedTiledShapeComplex32Column;
pub use streamed_complex::StreamingTiledShapeComplex32Writer;
pub use streamed_complex::install_streamed_tiled_shape_complex32_column;
pub use streamed_primitive::StreamedTiledPrimitiveColumn;
pub use streamed_primitive::StreamedTiledPrimitiveType;
pub use streamed_primitive::StreamingTiledPrimitiveWriter;
pub use streamed_primitive::install_streamed_tiled_column_primitive_column;
pub use streamed_primitive::install_streamed_tiled_shape_primitive_column;
use streamed_primitive::*;
pub use streamed_shape::StreamedTiledShapeColumn;
pub use streamed_shape::StreamedTiledShapeCubeLayout;
pub use streamed_shape::StreamedTiledShapeValueType;
pub use streamed_shape::StreamingTiledShapeWriter;
pub use streamed_shape::install_streamed_tiled_column;
pub use streamed_shape::install_streamed_tiled_shape_column;
use streamed_shape::*;
use tile_bytes::*;
pub(crate) use typed_1d::fill_tiled_column_rows_1d_typed;
pub(crate) use typed_1d::load_tiled_column_rows_1d_typed;
use typed_1d::*;
pub(crate) use typed_2d::fill_tiled_column_rows_2d_channel_range_typed;
pub(crate) use typed_2d::load_tiled_column_rows_2d_channel_range_typed;
use typed_2d::*;
pub use typed_array::DEFAULT_TILED_ARRAY_CACHE_BYTES;
pub use typed_array::TiledArrayStorage;
pub use typed_array::TiledArrayStorageLayout;
use typed_array::*;

/// Pixel types supported by the generic tile I/O path.
///
/// Implemented for `bool`, `f32`, `f64`, `Complex32`, and `Complex64`.
/// `ELEM_SIZE` is the unpacked in-memory element size used by the tile cache;
/// `SWAP_SIZE` is the component size for byte-swapping (1 for bool, 4 for
/// f32/Complex32, 8 for f64/Complex64).
pub trait TilePixel: Copy + Default + 'static {
    /// Number of bytes occupied by one element on disk.
    const ELEM_SIZE: usize;
    /// Number of bytes in each independently byte-swapped component.
    const SWAP_SIZE: usize;
    /// casacore primitive type tag associated with this Rust element type.
    const STORAGE_TYPE: PrimitiveType;
}
impl TilePixel for bool {
    const ELEM_SIZE: usize = 1;
    const SWAP_SIZE: usize = 1;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::Bool;
}
impl TilePixel for u8 {
    const ELEM_SIZE: usize = 1;
    const SWAP_SIZE: usize = 1;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::UInt8;
}
impl TilePixel for i16 {
    const ELEM_SIZE: usize = 2;
    const SWAP_SIZE: usize = 2;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::Int16;
}
impl TilePixel for u16 {
    const ELEM_SIZE: usize = 2;
    const SWAP_SIZE: usize = 2;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::UInt16;
}
impl TilePixel for i32 {
    const ELEM_SIZE: usize = 4;
    const SWAP_SIZE: usize = 4;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::Int32;
}
impl TilePixel for u32 {
    const ELEM_SIZE: usize = 4;
    const SWAP_SIZE: usize = 4;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::UInt32;
}
impl TilePixel for i64 {
    const ELEM_SIZE: usize = 8;
    const SWAP_SIZE: usize = 8;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::Int64;
}
impl TilePixel for f32 {
    const ELEM_SIZE: usize = 4;
    const SWAP_SIZE: usize = 4;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::Float32;
}
impl TilePixel for f64 {
    const ELEM_SIZE: usize = 8;
    const SWAP_SIZE: usize = 8;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::Float64;
}
impl TilePixel for Complex32 {
    const ELEM_SIZE: usize = 8;
    const SWAP_SIZE: usize = 4;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::Complex32;
}
impl TilePixel for Complex64 {
    const ELEM_SIZE: usize = 16;
    const SWAP_SIZE: usize = 8;
    const STORAGE_TYPE: PrimitiveType = PrimitiveType::Complex64;
}

use super::canonical::*;
use super::data_file::DataFile;
use super::data_type::CasacoreDataType;
use super::table_control::{
    ColumnDescContents, DataManagerEntry, PlainColumnEntry, TableDescContents, read_iposition,
    read_record, write_iposition, write_record,
};
use super::{StorageError, StorageProfiler, TileLayoutPlanner};

pub(super) mod append;
mod band_writer;
#[cfg(test)]
mod casacore_tiled_data_tests;
#[cfg(test)]
mod casacore_tiled_order_tests;
mod selected_cells;
pub(crate) use selected_cells::cell_read_buffer_bytes;
use selected_cells::load_selected_rows_from_touched_cubes;
mod selected_tiles;
use selected_tiles::{SelectedTiles, copy_pixels};
pub(crate) mod shape_append;
mod shape_cube;
mod spec;
#[cfg(test)]
mod split_axis0_tests;
mod strided_slice;
pub(crate) use metadata_shapes::Packed2DSelection;
use shape_cube::{DirectShapeCubeWriter, ShapeCubeWorkingSet};
pub(crate) use spec::tiled_spec;
mod tile_columns;
use tile_columns::{
    TileColumns, bound_tile_layout, in_tile_order, rows_reached, tile_columns, tile_target,
    untiled_column_error, writer_tile_order,
};
mod undefined_column;
pub use undefined_column::install_undefined_tiled_shape_column;

mod metadata_shapes;
pub(crate) mod packed_update;

/// Construct the path to a TSM data file.
fn tsm_data_path(table_path: &Path, dm_seq_nr: u32, file_seq_nr: u32) -> std::path::PathBuf {
    table_path.join(format!("table.f{dm_seq_nr}_TSM{file_seq_nr}"))
}

/// Clone an existing tiled storage manager's header and payload files to a new
/// data-manager sequence number.
///
/// This mirrors the storage-side part of casacore `TableCopy::cloneColumn*`
/// for the common single-column tiled MeasurementSet data-column case. The
/// caller is responsible for updating `table.dat` to bind the new column to
/// `target_dm_seq_nr`.
pub(crate) fn clone_tiled_manager_files(
    table_path: &Path,
    source_dm_seq_nr: u32,
    target_dm_seq_nr: u32,
    target_dm_name: &str,
) -> Result<(), StorageError> {
    let source_header_path = table_path.join(format!("table.f{source_dm_seq_nr}"));
    let target_header_path = table_path.join(format!("table.f{target_dm_seq_nr}"));
    let (variant, mut header) = read_tiled_header(&source_header_path)?;

    if header.col_data_types.len() != 1 {
        return Err(StorageError::UnsupportedDataManager(format!(
            "tiled data manager {source_dm_seq_nr} has {} columns; clone_tiled_manager_files supports single-column managers",
            header.col_data_types.len()
        )));
    }

    for file_info in header.files.iter().flatten() {
        let source_data_path = tsm_data_path(table_path, source_dm_seq_nr, file_info.seq_nr);
        let target_data_path = tsm_data_path(table_path, target_dm_seq_nr, file_info.seq_nr);
        std::fs::copy(&source_data_path, &target_data_path)?;
    }

    header.seq_nr = target_dm_seq_nr;
    header.hypercolumn_name = target_dm_name.to_string();
    write_tiled_header(&target_header_path, &variant, &header)?;
    invalidate_shared_tile_cache_for_table(table_path);
    Ok(())
}

mod sparse_write_bytes;

fn checked_storage_product(values: &[usize], what: &str) -> Result<usize, StorageError> {
    values.iter().try_fold(1usize, |product, &value| {
        product.checked_mul(value).ok_or_else(|| {
            StorageError::FormatMismatch(format!("{what} overflow while planning tiled storage"))
        })
    })
}

impl Drop for TiledFileIO {
    fn drop(&mut self) {
        // Best-effort flush on drop.
        let _ = self.flush();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
