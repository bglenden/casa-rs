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
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Read as IoRead, Seek, SeekFrom, Write as IoWrite};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

use casa_aipsio::{AipsIo, AipsOpenOption};
use casa_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, Value,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

/// Maximum number of dimensions supported by stack-allocated arrays in
/// tile iteration loops.  casacore images are at most 5-D.
const MAX_NDIM: usize = 8;
const DEFAULT_TABLE_CACHE_BYTES: usize = 64 * 1024 * 1024;
const TABLE_CACHE_BUDGET_ENV: &str = "CASA_RS_TABLE_CACHE_BYTES";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SharedTileKey {
    table_path: PathBuf,
    dm_seq_nr: u32,
    cube_idx: usize,
    target_col_idx: usize,
    tile_index: usize,
}

#[derive(Clone)]
struct SharedTileEntry {
    data: Arc<[u8]>,
    bytes: usize,
    last_used: u64,
}

struct SharedTileCacheState {
    budget_bytes: usize,
    explicit_budget: Option<usize>,
    bytes_used: usize,
    clock: u64,
    entries: std::collections::HashMap<SharedTileKey, SharedTileEntry>,
}

impl SharedTileCacheState {
    fn new() -> Self {
        Self {
            budget_bytes: table_cache_budget_from_env(),
            explicit_budget: None,
            bytes_used: 0,
            clock: 0,
            entries: std::collections::HashMap::new(),
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    fn get(&mut self, key: &SharedTileKey) -> Option<Arc<[u8]>> {
        let tick = self.next_tick();
        let entry = self.entries.get_mut(key)?;
        entry.last_used = tick;
        Some(entry.data.clone())
    }

    fn set_budget(&mut self, budget_bytes: usize) {
        self.explicit_budget = Some(budget_bytes);
        self.budget_bytes = budget_bytes;
        self.evict_to_budget();
    }

    fn reset_for_tests(&mut self) {
        self.entries.clear();
        self.bytes_used = 0;
        self.clock = 0;
        self.explicit_budget = None;
        self.budget_bytes = table_cache_budget_from_env();
    }

    fn insert(&mut self, key: SharedTileKey, data: Arc<[u8]>) -> Arc<[u8]> {
        let bytes = data.len();
        if self.budget_bytes == 0 || bytes > self.budget_bytes {
            return data;
        }
        let tick = self.next_tick();
        self.bytes_used += bytes;
        self.entries.insert(
            key,
            SharedTileEntry {
                data: data.clone(),
                bytes,
                last_used: tick,
            },
        );
        self.evict_to_budget();
        data
    }

    fn evict_to_budget(&mut self) {
        while self.bytes_used > self.budget_bytes {
            let Some((evict_key, evict_bytes)) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, entry)| (key.clone(), entry.bytes))
            else {
                break;
            };
            self.entries.remove(&evict_key);
            self.bytes_used = self.bytes_used.saturating_sub(evict_bytes);
        }
    }
}

static SHARED_TILE_CACHE: LazyLock<Mutex<SharedTileCacheState>> =
    LazyLock::new(|| Mutex::new(SharedTileCacheState::new()));

fn table_cache_budget_from_env() -> usize {
    std::env::var(TABLE_CACHE_BUDGET_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_TABLE_CACHE_BYTES)
}

/// Returns the current process-wide shared table-read cache budget in bytes.
///
/// The initial value comes from `CASA_RS_TABLE_CACHE_BYTES` when set,
/// otherwise a bounded default is used.
pub fn table_cache_budget_bytes() -> usize {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .budget_bytes
}

/// Overrides the process-wide shared table-read cache budget in bytes.
///
/// The runtime override takes precedence over `CASA_RS_TABLE_CACHE_BYTES`
/// for the remainder of the process and immediately evicts excess cached data.
pub fn set_table_cache_budget_bytes(budget_bytes: usize) {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .set_budget(budget_bytes);
}

#[cfg(test)]
pub(crate) fn reset_table_cache_budget_for_tests() {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .reset_for_tests();
}

#[cfg(test)]
pub(crate) fn shared_tile_cache_entry_count() -> usize {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .entries
        .len()
}

#[cfg(test)]
fn shared_tile_cache_entry_count_for_table(table_path: &Path) -> usize {
    SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .entries
        .keys()
        .filter(|key| key.table_path == table_path)
        .count()
}

pub(crate) fn invalidate_shared_tile_cache_for_table(table_path: &Path) {
    let mut cache = SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned");
    let mut freed_bytes = 0usize;
    cache.entries.retain(|key, entry| {
        let keep = key.table_path != table_path;
        if !keep {
            freed_bytes += entry.bytes;
        }
        keep
    });
    cache.bytes_used = cache.bytes_used.saturating_sub(freed_bytes);
}

/// Pixel types supported by the generic tile I/O path.
///
/// Implemented for `bool`, `f32`, `f64`, `Complex32`, and `Complex64`.
/// `ELEM_SIZE` is the unpacked in-memory element size used by the tile cache;
/// `SWAP_SIZE` is the component size for byte-swapping (1 for bool, 4 for
/// f32/Complex32, 8 for f64/Complex64).
pub trait TilePixel: Copy + Default + 'static {
    const ELEM_SIZE: usize;
    const SWAP_SIZE: usize;
}
impl TilePixel for bool {
    const ELEM_SIZE: usize = 1;
    const SWAP_SIZE: usize = 1;
}
impl TilePixel for f32 {
    const ELEM_SIZE: usize = 4;
    const SWAP_SIZE: usize = 4;
}
impl TilePixel for f64 {
    const ELEM_SIZE: usize = 8;
    const SWAP_SIZE: usize = 8;
}
impl TilePixel for Complex32 {
    const ELEM_SIZE: usize = 8;
    const SWAP_SIZE: usize = 4;
}
impl TilePixel for Complex64 {
    const ELEM_SIZE: usize = 16;
    const SWAP_SIZE: usize = 8;
}

use super::canonical::*;
use super::data_type::CasacoreDataType;
use super::table_control::{
    ColumnDescContents, DataManagerEntry, read_iposition, read_record, write_iposition,
    write_record,
};
use super::{StorageError, StorageProfiler};

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Metadata for a TSM data file (maps to C++ `TSMFile`).
#[derive(Debug, Clone)]
struct TsmFileInfo {
    seq_nr: u32,
    length: i64,
}

/// Metadata for a single hypercube (maps to C++ `TSMCube`).
#[derive(Debug, Clone)]
struct TsmCubeInfo {
    values: RecordValue,
    extensible: bool,
    cube_shape: Vec<usize>,
    tile_shape: Vec<usize>,
    file_seq_nr: i32,
    file_offset: i64,
}

/// Common base-class header (maps to C++ `TiledStMan`).
#[derive(Debug, Clone)]
struct TiledStManHeader {
    big_endian: bool,
    seq_nr: u32,
    nrrow: u64,
    col_data_types: Vec<CasacoreDataType>,
    hypercolumn_name: String,
    max_cache_size: u64,
    nrdim: u32,
    files: Vec<Option<TsmFileInfo>>,
    cubes: Vec<TsmCubeInfo>,
}

/// Variant-specific data for each tiled storage manager type.
#[derive(Debug, Clone)]
enum TiledVariant {
    Column {
        default_tile_shape: Vec<i32>,
    },
    Shape {
        default_tile_shape: Vec<i32>,
        nr_used_row_map: u32,
        row_map: Vec<u32>,
        cube_map: Vec<u32>,
        pos_map: Vec<u32>,
    },
    Cell {
        default_tile_shape: Vec<i32>,
    },
    /// TiledDataStMan: user-controlled hypercube assignment.
    ///
    /// Unlike TiledShapeStMan (automatic shape-based grouping), the user
    /// explicitly assigns rows to hypercubes. Found in some older datasets.
    ///
    /// # C++ equivalent
    ///
    /// `TiledDataStMan` in `casacore/tables/DataMan/TiledDataStMan.h`.
    Data {
        default_tile_shape: Vec<i32>,
        nrrow_last: u64,
        row_map: Vec<u64>,
        cube_map: Vec<u32>,
        pos_map: Vec<u32>,
    },
}

// ---------------------------------------------------------------------------
// Tile element size (different from SSM canonical size)
// ---------------------------------------------------------------------------

/// Returns the unpacked element size used while reconstructing typed tile data.
fn tile_element_size(dt: CasacoreDataType) -> usize {
    match dt {
        CasacoreDataType::TpBool | CasacoreDataType::TpUChar | CasacoreDataType::TpChar => 1,
        CasacoreDataType::TpShort | CasacoreDataType::TpUShort => 2,
        CasacoreDataType::TpInt | CasacoreDataType::TpUInt | CasacoreDataType::TpFloat => 4,
        CasacoreDataType::TpDouble | CasacoreDataType::TpInt64 | CasacoreDataType::TpComplex => 8,
        CasacoreDataType::TpDComplex => 16,
        _ => 0,
    }
}

/// Returns the number of on-disk bytes a column occupies within one tile.
fn tile_storage_bytes(dt: CasacoreDataType, nrpixels: usize) -> usize {
    match dt {
        CasacoreDataType::TpBool => nrpixels.div_ceil(8),
        _ => nrpixels * tile_element_size(dt),
    }
}

/// Compute tile layout: per-column offsets within a tile and total bucket size.
///
/// C++ casacore stores tiled columns sequentially in schema order.
fn compute_tile_layout(
    col_data_types: &[CasacoreDataType],
    tile_shape: &[usize],
) -> (usize, Vec<usize>) {
    let nrpixels: usize = tile_shape.iter().product();

    let mut offsets = vec![0usize; col_data_types.len()];
    let mut offset = 0usize;
    for (col, &dt) in col_data_types.iter().enumerate() {
        offsets[col] = offset;
        offset += tile_storage_bytes(dt, nrpixels);
    }

    (offset, offsets)
}

fn read_tile_storage(src: &[u8], dt: CasacoreDataType, nrpixels: usize) -> Vec<u8> {
    match dt {
        CasacoreDataType::TpBool => read_bool_bits(src, 0, nrpixels)
            .into_iter()
            .map(|value| if value { 1 } else { 0 })
            .collect(),
        _ => src.to_vec(),
    }
}

fn write_tile_storage(dst: &mut [u8], dt: CasacoreDataType, unpacked: &[u8], nrpixels: usize) {
    match dt {
        CasacoreDataType::TpBool => write_bool_bits_from_bytes(dst, 0, &unpacked[..nrpixels]),
        _ => dst.copy_from_slice(unpacked),
    }
}

// ---------------------------------------------------------------------------
// Header read
// ---------------------------------------------------------------------------

/// Parse the AipsIO header file for a tiled storage manager.
///
/// Returns the variant-specific data and the common base header.
fn read_tiled_header(path: &Path) -> Result<(TiledVariant, TiledStManHeader), StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::Old)?;

    // The outermost AipsIO object is the derived-class envelope.
    let outer_type = io.get_next_type()?;
    let outer_version = io.getstart(&outer_type)?;

    match outer_type.as_str() {
        "TiledColumnStMan" => {
            // TiledColumnStMan: tileShape first, then base class.
            let default_tile_shape = read_iposition(&mut io)?;
            let header = read_tiled_stman_base(&mut io)?;
            io.getend()?;
            io.close()?;
            Ok((TiledVariant::Column { default_tile_shape }, header))
        }
        "TiledShapeStMan" => {
            // TiledShapeStMan: base class first, then variant-specific fields.
            let header = read_tiled_stman_base(&mut io)?;
            let default_tile_shape = read_iposition(&mut io)?;
            let nr_used_row_map = io.get_u32()?;
            let row_map = read_block_u32(&mut io)?;
            let cube_map = read_block_u32(&mut io)?;
            let pos_map = read_block_u32(&mut io)?;
            io.getend()?;
            io.close()?;
            Ok((
                TiledVariant::Shape {
                    default_tile_shape,
                    nr_used_row_map,
                    row_map,
                    cube_map,
                    pos_map,
                },
                header,
            ))
        }
        "TiledCellStMan" => {
            // TiledCellStMan: defaultTileShape first, then base class.
            let default_tile_shape = read_iposition(&mut io)?;
            let header = read_tiled_stman_base(&mut io)?;
            io.getend()?;
            io.close()?;
            Ok((TiledVariant::Cell { default_tile_shape }, header))
        }
        "TiledDataStMan" => {
            // TiledDataStMan (version 2): base class first, then
            // default_tile_shape, nrrowLast, rowMap, cubeMap, posMap.
            let header = read_tiled_stman_base(&mut io)?;
            let default_tile_shape = read_iposition(&mut io)?;
            let nrrow_last = if outer_version >= 2 {
                io.get_u64()?
            } else {
                io.get_u32()? as u64
            };
            let row_map = read_block_u64(&mut io)?;
            let cube_map = read_block_u32(&mut io)?;
            let pos_map = read_block_u32(&mut io)?;
            io.getend()?;
            io.close()?;
            Ok((
                TiledVariant::Data {
                    default_tile_shape,
                    nrrow_last,
                    row_map,
                    cube_map,
                    pos_map,
                },
                header,
            ))
        }
        other => Err(StorageError::FormatMismatch(format!(
            "unsupported tiled storage manager type: {other}"
        ))),
    }
}

/// Parse the `TiledStMan` base class AipsIO object.
///
/// Handles versions 1 (big-endian, 32-bit), 2 (explicit endian, 32-bit),
/// and 3 (explicit endian, 64-bit).
fn read_tiled_stman_base(io: &mut AipsIo) -> Result<TiledStManHeader, StorageError> {
    let version = io.getstart("TiledStMan")?;

    let big_endian = if version >= 2 {
        io.get_bool()?
    } else {
        true // v1 is always big-endian
    };

    let seq_nr = io.get_u32()?;

    let nrrow = if version >= 3 {
        io.get_u64()?
    } else {
        io.get_u32()? as u64
    };

    let ncol = io.get_u32()?;
    let mut col_data_types = Vec::with_capacity(ncol as usize);
    for _ in 0..ncol {
        let dt_i32 = io.get_i32()?;
        let dt = CasacoreDataType::from_i32(dt_i32).ok_or_else(|| {
            StorageError::FormatMismatch(format!("unknown TiledStMan column data type: {dt_i32}"))
        })?;
        col_data_types.push(dt);
    }

    let hypercolumn_name = io.get_string()?;

    let max_cache_size = if version >= 3 {
        io.get_u64()?
    } else {
        io.get_u32()? as u64
    };

    let nrdim = io.get_u32()?;

    // Read TSM file entries.
    let nr_files = if version >= 3 {
        io.get_u64()? as usize
    } else {
        io.get_u32()? as usize
    };

    let mut files = Vec::with_capacity(nr_files);
    for _ in 0..nr_files {
        let exists = io.get_bool()?;
        if exists {
            files.push(Some(read_tsm_file_info(io)?));
        } else {
            files.push(None);
        }
    }

    // Read TSM cube entries.
    let nr_cubes = if version >= 3 {
        io.get_u64()? as usize
    } else {
        io.get_u32()? as usize
    };

    let mut cubes = Vec::with_capacity(nr_cubes);
    for _ in 0..nr_cubes {
        cubes.push(read_tsm_cube_info(io)?);
    }

    io.getend()?;

    Ok(TiledStManHeader {
        big_endian,
        seq_nr,
        nrrow,
        col_data_types,
        hypercolumn_name,
        max_cache_size,
        nrdim,
        files,
        cubes,
    })
}

/// Read inline TSMFile serialization (no AipsIO start/end wrapper).
fn read_tsm_file_info(io: &mut AipsIo) -> Result<TsmFileInfo, StorageError> {
    let version = io.get_u32()?;
    let seq_nr = io.get_u32()?;
    let length = if version >= 2 {
        io.get_i64()?
    } else {
        io.get_u32()? as i64
    };
    Ok(TsmFileInfo { seq_nr, length })
}

/// Read inline TSMCube serialization (no AipsIO start/end wrapper).
fn read_tsm_cube_info(io: &mut AipsIo) -> Result<TsmCubeInfo, StorageError> {
    let version = io.get_u32()?;
    let values = read_record(io)?;
    let extensible = io.get_bool()?;
    let _nrdim = io.get_u32()?;
    let cube_shape_i32 = read_iposition(io)?;
    let tile_shape_i32 = read_iposition(io)?;
    let file_seq_nr = io.get_i32()?;
    let file_offset = if version >= 2 {
        io.get_i64()?
    } else {
        io.get_u32()? as i64
    };

    Ok(TsmCubeInfo {
        values,
        extensible,
        cube_shape: cube_shape_i32.iter().map(|&v| v as usize).collect(),
        tile_shape: tile_shape_i32.iter().map(|&v| v as usize).collect(),
        file_seq_nr,
        file_offset,
    })
}

/// Read a `Block<uInt>` AipsIO object (used by TiledShapeStMan row maps).
fn read_block_u32(io: &mut AipsIo) -> Result<Vec<u32>, StorageError> {
    let _version = io.getstart("Block")?;
    let count = io.get_u32()?;
    let mut values = vec![0u32; count as usize];
    for v in &mut values {
        *v = io.get_u32()?;
    }
    io.getend()?;
    Ok(values)
}

// ---------------------------------------------------------------------------
// Header write
// ---------------------------------------------------------------------------

/// Write the AipsIO header file for a tiled storage manager.
fn write_tiled_header(
    path: &Path,
    variant: &TiledVariant,
    header: &TiledStManHeader,
) -> Result<(), StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::New)?;

    match variant {
        TiledVariant::Column { default_tile_shape } => {
            io.putstart("TiledColumnStMan", 1)?;
            write_iposition(&mut io, default_tile_shape)?;
            write_tiled_stman_base(&mut io, header)?;
            io.putend()?;
        }
        TiledVariant::Shape {
            default_tile_shape,
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
        } => {
            io.putstart("TiledShapeStMan", 1)?;
            write_tiled_stman_base(&mut io, header)?;
            write_iposition(&mut io, default_tile_shape)?;
            io.put_u32(*nr_used_row_map)?;
            write_block_u32(&mut io, row_map)?;
            write_block_u32(&mut io, cube_map)?;
            write_block_u32(&mut io, pos_map)?;
            io.putend()?;
        }
        TiledVariant::Cell { default_tile_shape } => {
            io.putstart("TiledCellStMan", 1)?;
            write_iposition(&mut io, default_tile_shape)?;
            write_tiled_stman_base(&mut io, header)?;
            io.putend()?;
        }
        TiledVariant::Data {
            default_tile_shape,
            nrrow_last,
            row_map,
            cube_map,
            pos_map,
        } => {
            io.putstart("TiledDataStMan", 2)?;
            write_tiled_stman_base(&mut io, header)?;
            write_iposition(&mut io, default_tile_shape)?;
            io.put_u64(*nrrow_last)?;
            write_block_u64(&mut io, row_map)?;
            write_block_u32(&mut io, cube_map)?;
            write_block_u32(&mut io, pos_map)?;
            io.putend()?;
        }
    }

    io.close()?;
    Ok(())
}

/// Write the `TiledStMan` base class AipsIO object.
fn write_tiled_stman_base(io: &mut AipsIo, header: &TiledStManHeader) -> Result<(), StorageError> {
    // Choose version: v1 for big-endian with small tables, v2 for little-endian.
    let version = if header.big_endian { 1u32 } else { 2 };

    io.putstart("TiledStMan", version)?;

    if version >= 2 {
        io.put_bool(header.big_endian)?;
    }

    io.put_u32(header.seq_nr)?;
    io.put_u32(header.nrrow as u32)?;

    io.put_u32(header.col_data_types.len() as u32)?;
    for &dt in &header.col_data_types {
        io.put_i32(dt as i32)?;
    }

    io.put_string(&header.hypercolumn_name)?;
    io.put_u32(header.max_cache_size as u32)?;
    io.put_u32(header.nrdim)?;

    // TSM files.
    io.put_u32(header.files.len() as u32)?;
    for file in &header.files {
        match file {
            Some(f) => {
                io.put_bool(true)?;
                write_tsm_file_info(io, f)?;
            }
            None => {
                io.put_bool(false)?;
            }
        }
    }

    // TSM cubes.
    io.put_u32(header.cubes.len() as u32)?;
    for cube in &header.cubes {
        write_tsm_cube_info(io, cube)?;
    }

    io.putend()?;
    Ok(())
}

/// Write inline TSMFile serialization.
fn write_tsm_file_info(io: &mut AipsIo, file: &TsmFileInfo) -> Result<(), StorageError> {
    let version: u32 = if file.length > i32::MAX as i64 { 2 } else { 1 };
    io.put_u32(version)?;
    io.put_u32(file.seq_nr)?;
    if version >= 2 {
        io.put_i64(file.length)?;
    } else {
        io.put_u32(file.length as u32)?;
    }
    Ok(())
}

/// Write inline TSMCube serialization.
fn write_tsm_cube_info(io: &mut AipsIo, cube: &TsmCubeInfo) -> Result<(), StorageError> {
    let version: u32 = if cube.file_offset > i32::MAX as i64 {
        2
    } else {
        1
    };
    io.put_u32(version)?;
    write_record(io, &cube.values)?;
    io.put_bool(cube.extensible)?;
    io.put_u32(cube.cube_shape.len() as u32)?;
    let shape_i32: Vec<i32> = cube.cube_shape.iter().map(|&v| v as i32).collect();
    write_iposition(io, &shape_i32)?;
    let tile_i32: Vec<i32> = cube.tile_shape.iter().map(|&v| v as i32).collect();
    write_iposition(io, &tile_i32)?;
    io.put_i32(cube.file_seq_nr)?;
    if version >= 2 {
        io.put_i64(cube.file_offset)?;
    } else {
        io.put_u32(cube.file_offset as u32)?;
    }
    Ok(())
}

/// Write a `Block<uInt>` AipsIO object.
fn write_block_u32(io: &mut AipsIo, values: &[u32]) -> Result<(), StorageError> {
    io.putstart("Block", 1)?;
    io.put_u32(values.len() as u32)?;
    for &v in values {
        io.put_u32(v)?;
    }
    io.putend()?;
    Ok(())
}

/// Read a `Block<Int64>` AipsIO object (used by TiledDataStMan row maps).
fn read_block_u64(io: &mut AipsIo) -> Result<Vec<u64>, StorageError> {
    let _version = io.getstart("Block")?;
    let count = io.get_u32()?;
    let mut values = vec![0u64; count as usize];
    for v in &mut values {
        *v = io.get_u64()?;
    }
    io.getend()?;
    Ok(values)
}

/// Write a `Block<Int64>` AipsIO object.
fn write_block_u64(io: &mut AipsIo, values: &[u64]) -> Result<(), StorageError> {
    io.putstart("Block", 1)?;
    io.put_u32(values.len() as u32)?;
    for &v in values {
        io.put_u64(v)?;
    }
    io.putend()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tile data reconstruction
// ---------------------------------------------------------------------------

/// Convert a linear index to an N-dimensional position (column-major order).
fn linear_to_nd(mut linear: usize, dims: &[usize]) -> Vec<usize> {
    let mut pos = Vec::with_capacity(dims.len());
    for &d in dims {
        pos.push(linear % d);
        linear /= d;
    }
    pos
}

/// Convert an N-dimensional position to a linear index (column-major order).
fn nd_to_linear(pos: &[usize], dims: &[usize]) -> usize {
    let mut linear = 0;
    let mut stride = 1;
    for i in 0..dims.len() {
        linear += pos[i] * stride;
        stride *= dims[i];
    }
    linear
}

/// Copy tile data into the correct position in a reconstructed cube.
///
/// Both tile and cube use column-major (Fortran) order. Handles edge tiles
/// that extend beyond the cube boundary.
fn copy_tile_to_cube(
    tile_data: &[u8],
    tile_shape: &[usize],
    cube_data: &mut [u8],
    cube_shape: &[usize],
    cube_start: &[usize],
    actual_extent: &[usize],
    elem_size: usize,
) {
    let ndim = tile_shape.len();
    if ndim == 0 {
        return;
    }

    // Optimize: copy contiguous runs along the innermost dimension.
    let inner_bytes = actual_extent[0] * elem_size;

    if ndim == 1 {
        let src_off = 0;
        let dst_off = cube_start[0] * elem_size;
        cube_data[dst_off..dst_off + inner_bytes]
            .copy_from_slice(&tile_data[src_off..src_off + inner_bytes]);
        return;
    }

    // Iterate over all outer-dimension positions.
    let outer_dims: Vec<usize> = actual_extent[1..].to_vec();
    let outer_total: usize = outer_dims.iter().product();

    for outer_linear in 0..outer_total {
        let outer_pos = linear_to_nd(outer_linear, &outer_dims);

        // Compute tile linear offset for this outer position.
        let mut tile_off = 0;
        let mut stride = tile_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            tile_off += p * stride;
            stride *= tile_shape[d + 1];
        }

        // Compute cube linear offset.
        let mut cube_off = cube_start[0];
        stride = cube_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            cube_off += (cube_start[d + 1] + p) * stride;
            stride *= cube_shape[d + 1];
        }

        let src_start = tile_off * elem_size;
        let dst_start = cube_off * elem_size;
        cube_data[dst_start..dst_start + inner_bytes]
            .copy_from_slice(&tile_data[src_start..src_start + inner_bytes]);
    }
}

/// Reconstruct a single column's full cube data from tiled storage.
///
/// Returns raw bytes in column-major order, in the on-disk byte order.
#[allow(clippy::too_many_arguments)]
fn reconstruct_cube_column(
    file_data: &[u8],
    file_offset: usize,
    cube_shape: &[usize],
    tile_shape: &[usize],
    col_offset_in_tile: usize,
    bucket_size: usize,
    dt: CasacoreDataType,
    elem_size: usize,
) -> Result<Vec<u8>, StorageError> {
    let ndim = cube_shape.len();
    let nrpixels: usize = tile_shape.iter().product();
    let col_bytes_per_tile = tile_storage_bytes(dt, nrpixels);

    let tiles_per_dim: Vec<usize> = (0..ndim)
        .map(|i| cube_shape[i].div_ceil(tile_shape[i]))
        .collect();
    let nr_tiles: usize = tiles_per_dim.iter().product();

    let cube_nelem: usize = cube_shape.iter().product();
    let mut result = vec![0u8; cube_nelem * elem_size];

    for tile_idx in 0..nr_tiles {
        let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);

        let cube_start: Vec<usize> = (0..ndim).map(|d| tile_pos[d] * tile_shape[d]).collect();
        let actual_extent: Vec<usize> = (0..ndim)
            .map(|d| std::cmp::min(tile_shape[d], cube_shape[d] - cube_start[d]))
            .collect();

        let tile_data_start = file_offset + tile_idx * bucket_size + col_offset_in_tile;
        let tile_data_end = tile_data_start + col_bytes_per_tile;
        if tile_data_end > file_data.len() {
            return Err(StorageError::FormatMismatch(format!(
                "tile data out of bounds: need {tile_data_end} bytes but file has {}",
                file_data.len()
            )));
        }
        let tile_bytes =
            read_tile_storage(&file_data[tile_data_start..tile_data_end], dt, nrpixels);
        copy_tile_to_cube(
            &tile_bytes,
            tile_shape,
            &mut result,
            cube_shape,
            &cube_start,
            &actual_extent,
            elem_size,
        );
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Typed value extraction from raw bytes
// ---------------------------------------------------------------------------

/// Decode raw bytes (in the table's byte order) into a `Value::Array`.
fn decode_array_value(
    raw: &[u8],
    shape: &[usize],
    dt: CasacoreDataType,
    big_endian: bool,
) -> Result<Value, StorageError> {
    let nelem: usize = shape.iter().product();
    let av = match dt {
        CasacoreDataType::TpBool => {
            let vals: Vec<bool> = raw[..nelem].iter().map(|&b| b != 0).collect();
            ArrayValue::Bool(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpUChar => {
            let vals: Vec<u8> = raw[..nelem].to_vec();
            ArrayValue::UInt8(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpShort => {
            let vals = if big_endian {
                read_i16_slice_be(raw, nelem)
            } else {
                read_i16_slice_le(raw, nelem)
            };
            ArrayValue::Int16(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpUShort => {
            let vals = if big_endian {
                read_u16_slice_be(raw, nelem)
            } else {
                read_u16_slice_le(raw, nelem)
            };
            ArrayValue::UInt16(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpInt => {
            let vals = if big_endian {
                read_i32_slice_be(raw, nelem)
            } else {
                read_i32_slice_le(raw, nelem)
            };
            ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpUInt => {
            let vals = if big_endian {
                read_u32_slice_be(raw, nelem)
            } else {
                read_u32_slice_le(raw, nelem)
            };
            ArrayValue::UInt32(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpFloat => {
            let vals = if big_endian {
                read_f32_slice_be(raw, nelem)
            } else {
                read_f32_slice_le(raw, nelem)
            };
            ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpDouble => {
            let vals = if big_endian {
                read_f64_slice_be(raw, nelem)
            } else {
                read_f64_slice_le(raw, nelem)
            };
            ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpInt64 => {
            let vals = if big_endian {
                read_i64_slice_be(raw, nelem)
            } else {
                read_i64_slice_le(raw, nelem)
            };
            ArrayValue::Int64(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpComplex => {
            // Complex = (float, float) pairs
            let floats = if big_endian {
                read_f32_slice_be(raw, nelem * 2)
            } else {
                read_f32_slice_le(raw, nelem * 2)
            };
            let vals: Vec<Complex32> = (0..nelem)
                .map(|i| Complex32::new(floats[2 * i], floats[2 * i + 1]))
                .collect();
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpDComplex => {
            let doubles = if big_endian {
                read_f64_slice_be(raw, nelem * 2)
            } else {
                read_f64_slice_le(raw, nelem * 2)
            };
            let vals: Vec<Complex64> = (0..nelem)
                .map(|i| Complex64::new(doubles[2 * i], doubles[2 * i + 1]))
                .collect();
            ArrayValue::Complex64(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        other => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported tiled array element type: {other:?}"
            )));
        }
    };
    Ok(Value::Array(av))
}

/// Encode a `Value::Array` into raw bytes in the specified byte order.
fn encode_array_value(
    value: &Value,
    big_endian: bool,
) -> Result<(Vec<u8>, CasacoreDataType), StorageError> {
    let arr = match value {
        Value::Array(a) => a,
        _ => {
            return Err(StorageError::FormatMismatch(
                "expected array value for tiled column".to_string(),
            ));
        }
    };

    // Tile data is stored in Fortran (column-major) order. Most arrays read
    // from casacore tables already have that memory order, so use the raw
    // memory-order slice before falling back to a transposed contiguous copy.
    match arr {
        ArrayValue::Bool(a) => {
            let slice = array_memory_order_values(a);
            let data: Vec<u8> = slice.iter().map(|&b| if b { 1u8 } else { 0u8 }).collect();
            Ok((data, CasacoreDataType::TpBool))
        }
        ArrayValue::UInt8(a) => {
            let data = array_memory_order_values(a).as_ref().to_vec();
            Ok((data, CasacoreDataType::TpUChar))
        }
        ArrayValue::Int16(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 2];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_i16_be(&mut data[i * 2..], v);
                } else {
                    write_i16_le(&mut data[i * 2..], v);
                }
            }
            Ok((data, CasacoreDataType::TpShort))
        }
        ArrayValue::UInt16(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 2];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_u16_be(&mut data[i * 2..], v);
                } else {
                    write_u16_le(&mut data[i * 2..], v);
                }
            }
            Ok((data, CasacoreDataType::TpUShort))
        }
        ArrayValue::Int32(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 4];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_i32_be(&mut data[i * 4..], v);
                } else {
                    write_i32_le(&mut data[i * 4..], v);
                }
            }
            Ok((data, CasacoreDataType::TpInt))
        }
        ArrayValue::UInt32(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 4];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_u32_be(&mut data[i * 4..], v);
                } else {
                    write_u32_le(&mut data[i * 4..], v);
                }
            }
            Ok((data, CasacoreDataType::TpUInt))
        }
        ArrayValue::Float32(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 4];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_f32_be(&mut data[i * 4..], v);
                } else {
                    write_f32_le(&mut data[i * 4..], v);
                }
            }
            Ok((data, CasacoreDataType::TpFloat))
        }
        ArrayValue::Float64(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 8];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_f64_be(&mut data[i * 8..], v);
                } else {
                    write_f64_le(&mut data[i * 8..], v);
                }
            }
            Ok((data, CasacoreDataType::TpDouble))
        }
        ArrayValue::Int64(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 8];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_i64_be(&mut data[i * 8..], v);
                } else {
                    write_i64_le(&mut data[i * 8..], v);
                }
            }
            Ok((data, CasacoreDataType::TpInt64))
        }
        ArrayValue::Complex32(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 8];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_f32_be(&mut data[i * 8..], v.re);
                    write_f32_be(&mut data[i * 8 + 4..], v.im);
                } else {
                    write_f32_le(&mut data[i * 8..], v.re);
                    write_f32_le(&mut data[i * 8 + 4..], v.im);
                }
            }
            Ok((data, CasacoreDataType::TpComplex))
        }
        ArrayValue::Complex64(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 16];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_f64_be(&mut data[i * 16..], v.re);
                    write_f64_be(&mut data[i * 16 + 8..], v.im);
                } else {
                    write_f64_le(&mut data[i * 16..], v.re);
                    write_f64_le(&mut data[i * 16 + 8..], v.im);
                }
            }
            Ok((data, CasacoreDataType::TpDComplex))
        }
        ArrayValue::String(_) => Err(StorageError::FormatMismatch(
            "string arrays not supported in tiled storage".to_string(),
        )),
    }
}

fn array_memory_order_values<T: Clone>(array: &ArrayD<T>) -> Cow<'_, [T]> {
    if is_fortran_contiguous(array.shape(), array.strides())
        && let Some(slice) = array.as_slice_memory_order()
    {
        Cow::Borrowed(slice)
    } else {
        let fortran_ordered = array.t().as_standard_layout().into_owned();
        Cow::Owned(
            fortran_ordered
                .as_slice()
                .expect("contiguous after as_standard_layout")
                .to_vec(),
        )
    }
}

fn is_fortran_contiguous(shape: &[usize], strides: &[isize]) -> bool {
    let mut expected = 1isize;
    for (&dim, &stride) in shape.iter().zip(strides.iter()) {
        if dim > 1 && stride != expected {
            return false;
        }
        expected = expected.saturating_mul(dim.max(1) as isize);
    }
    true
}

// ---------------------------------------------------------------------------
// Load interface (read columns from tiled DM)
// ---------------------------------------------------------------------------

/// Load columns from a tiled storage manager into row records.
///
/// This is the main entry point called from `CompositeStorage::load_plain_table`.
pub(crate) fn load_tiled_columns(
    table_path: &Path,
    dm: &DataManagerEntry,
    all_col_descs: &[ColumnDescContents],
    bound_cols: &[(usize, &super::table_control::PlainColumnEntry)],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    let header_path = table_path.join(format!("table.f{}", dm.seq_nr));
    let (variant, header) = read_tiled_header(&header_path)?;

    // Collect column descriptors for the bound columns.
    let col_descs: Vec<&ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &all_col_descs[*desc_idx])
        .collect();

    match variant {
        TiledVariant::Column { .. } => load_tiled_column_stman(
            table_path,
            dm.seq_nr,
            &header,
            &col_descs,
            rows,
            undefined_cells,
            nrrow,
        ),
        TiledVariant::Shape {
            nr_used_row_map,
            ref row_map,
            ref cube_map,
            ref pos_map,
            ..
        } => load_tiled_shape_stman(
            table_path,
            dm.seq_nr,
            &header,
            &col_descs,
            rows,
            undefined_cells,
            nrrow,
            &ShapeRowMapping {
                nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
        ),
        TiledVariant::Cell { .. } => load_tiled_cell_stman(
            table_path,
            dm.seq_nr,
            &header,
            &col_descs,
            rows,
            undefined_cells,
            nrrow,
        ),
        TiledVariant::Data {
            ref row_map,
            ref cube_map,
            ref pos_map,
            ..
        } => load_tiled_data_stman(
            table_path,
            dm.seq_nr,
            &header,
            &col_descs,
            rows,
            undefined_cells,
            nrrow,
            row_map,
            cube_map,
            pos_map,
        ),
    }
}

pub(crate) fn load_tiled_column_rows(
    table_path: &Path,
    dm: &DataManagerEntry,
    all_col_descs: &[ColumnDescContents],
    bound_cols: &[(usize, &super::table_control::PlainColumnEntry)],
    target_desc_idx: usize,
    selected_rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let header_path = table_path.join(format!("table.f{}", dm.seq_nr));
    let (variant, header) = read_tiled_header(&header_path)?;
    let Some(target_col_idx) = bound_cols
        .iter()
        .position(|(desc_idx, _)| *desc_idx == target_desc_idx)
    else {
        return Err(StorageError::FormatMismatch(format!(
            "tiled column desc index {target_desc_idx} not bound to data manager {}",
            dm.seq_nr
        )));
    };
    if target_col_idx >= header.col_data_types.len() {
        return Err(StorageError::FormatMismatch(format!(
            "tiled column index {target_col_idx} out of range for data manager {}",
            dm.seq_nr
        )));
    }
    let col_desc = &all_col_descs[target_desc_idx];
    let dt = header.col_data_types[target_col_idx];
    let elem_size = tile_element_size(dt);
    if elem_size == 0 {
        return Ok(vec![None; selected_rows.len()]);
    }

    match variant {
        TiledVariant::Column { .. } => load_tiled_column_rows_column_variant(
            table_path,
            dm.seq_nr,
            &header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
        ),
        TiledVariant::Shape {
            nr_used_row_map,
            ref row_map,
            ref cube_map,
            ref pos_map,
            ..
        } => load_tiled_column_rows_shape_variant(
            table_path,
            dm.seq_nr,
            &header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
            &ShapeRowMapping {
                nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
        ),
        TiledVariant::Cell { .. } => load_tiled_column_rows_cell_variant(
            table_path,
            dm.seq_nr,
            &header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
        ),
        TiledVariant::Data {
            ref row_map,
            ref cube_map,
            ref pos_map,
            ..
        } => load_tiled_column_rows_data_variant(
            table_path,
            dm.seq_nr,
            &header,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            selected_rows,
            row_map,
            cube_map,
            pos_map,
        ),
    }
}

/// Load columns from a `TiledColumnStMan` (single hypercube for all rows).
fn load_tiled_column_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
    _undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    if header.cubes.is_empty() {
        return Ok(());
    }
    let cube = &header.cubes[0];
    if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
        return Ok(());
    }

    // Read the TSM data file.
    let tsm_file_name = tsm_data_path(table_path, dm_seq_nr, cube.file_seq_nr as u32);
    let file_data = std::fs::read(&tsm_file_name).map_err(|e| {
        StorageError::FormatMismatch(format!("cannot read {}: {e}", tsm_file_name.display()))
    })?;

    // Compute tile layout.
    let (bucket_size, col_offsets) = compute_tile_layout(&header.col_data_types, &cube.tile_shape);

    // Cell shape = cube_shape without the last dimension (row dimension).
    let cell_ndim = cube.cube_shape.len() - 1;
    let cell_shape: Vec<usize> = cube.cube_shape[..cell_ndim].to_vec();
    let cell_nelem: usize = cell_shape.iter().product();

    // For each column, reconstruct the full cube data, then extract per-row slices.
    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        if col_idx >= header.col_data_types.len() {
            break;
        }
        let dt = header.col_data_types[col_idx];
        let elem_size = tile_element_size(dt);
        if elem_size == 0 {
            continue;
        }

        let cube_raw = reconstruct_cube_column(
            &file_data,
            cube.file_offset as usize,
            &cube.cube_shape,
            &cube.tile_shape,
            col_offsets[col_idx],
            bucket_size,
            dt,
            elem_size,
        )?;

        // Extract per-row arrays.
        let row_bytes = cell_nelem * elem_size;
        for (row_idx, row) in rows.iter_mut().enumerate().take(nrrow) {
            let start = row_idx * row_bytes;
            let end = start + row_bytes;
            if end > cube_raw.len() {
                break;
            }
            let value =
                decode_array_value(&cube_raw[start..end], &cell_shape, dt, header.big_endian)?;
            row.push(RecordField::new(col_desc.col_name.clone(), value));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn load_tiled_column_rows_column_variant(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    if header.cubes.is_empty() {
        return Ok(vec![None; selected_rows.len()]);
    }
    let cube_idx = 0usize;
    let cube = &header.cubes[cube_idx];
    if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
        return Ok(vec![None; selected_rows.len()]);
    }
    let patches_by_cube = std::iter::once((
        cube_idx,
        selected_rows
            .iter()
            .enumerate()
            .map(|(out_idx, &row_idx)| SelectedCubeRow {
                out_idx,
                pos_in_cube: row_idx,
            })
            .collect(),
    ))
    .collect();
    load_selected_rows_from_touched_cubes(
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
        selected_rows.len(),
        patches_by_cube,
    )
}

/// Row-to-cube mapping tables extracted from `TiledShapeStMan` variant data.
struct ShapeRowMapping<'a> {
    nr_used_row_map: u32,
    row_map: &'a [u32],
    cube_map: &'a [u32],
    pos_map: &'a [u32],
}

/// Load columns from a `TiledShapeStMan` (one hypercube per unique shape).
#[allow(clippy::too_many_arguments)]
fn load_tiled_shape_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
    undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
    mapping: &ShapeRowMapping<'_>,
) -> Result<(), StorageError> {
    // Pre-read all TSM data files.
    let mut file_cache: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();

    // Pre-compute per-cube tile layouts and read TSM files.
    struct CubeLayout {
        bucket_size: usize,
        col_offsets: Vec<usize>,
    }
    let mut cube_layouts: Vec<Option<CubeLayout>> = Vec::with_capacity(header.cubes.len());

    for cube in &header.cubes {
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            cube_layouts.push(None);
            continue;
        }
        let (bucket_size, col_offsets) =
            compute_tile_layout(&header.col_data_types, &cube.tile_shape);
        cube_layouts.push(Some(CubeLayout {
            bucket_size,
            col_offsets,
        }));

        let fseq = cube.file_seq_nr as u32;
        if let std::collections::hash_map::Entry::Vacant(e) = file_cache.entry(fseq) {
            let path = tsm_data_path(table_path, dm_seq_nr, fseq);
            let data = std::fs::read(&path).map_err(|err| {
                StorageError::FormatMismatch(format!("cannot read {}: {err}", path.display()))
            })?;
            e.insert(data);
        }
    }

    // Pre-reconstruct per-cube per-column data.
    struct CubeColumnData {
        raw: Vec<u8>,
        cell_shape: Vec<usize>,
        cell_nelem: usize,
    }
    let mut cube_col_data: Vec<Vec<Option<CubeColumnData>>> =
        Vec::with_capacity(header.cubes.len());

    for (cube_idx, cube) in header.cubes.iter().enumerate() {
        let mut cols = Vec::with_capacity(col_descs.len());
        let layout = &cube_layouts[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() || layout.is_none() {
            for _ in col_descs {
                cols.push(None);
            }
            cube_col_data.push(cols);
            continue;
        }
        let layout = layout.as_ref().unwrap();
        let file_data = &file_cache[&(cube.file_seq_nr as u32)];

        let cell_ndim = cube.cube_shape.len() - 1;
        let cell_shape: Vec<usize> = cube.cube_shape[..cell_ndim].to_vec();
        let cell_nelem: usize = cell_shape.iter().product();

        for (col_idx, _) in col_descs.iter().enumerate() {
            if col_idx >= header.col_data_types.len() {
                cols.push(None);
                continue;
            }
            let dt = header.col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            if elem_size == 0 {
                cols.push(None);
                continue;
            }
            let raw = reconstruct_cube_column(
                file_data,
                cube.file_offset as usize,
                &cube.cube_shape,
                &cube.tile_shape,
                layout.col_offsets[col_idx],
                layout.bucket_size,
                dt,
                elem_size,
            )?;
            cols.push(Some(CubeColumnData {
                raw,
                cell_shape: cell_shape.clone(),
                cell_nelem,
            }));
        }
        cube_col_data.push(cols);
    }

    // Map rows to cubes and extract values.
    let n_intervals = mapping.nr_used_row_map as usize;
    if n_intervals != 0 {
        let row_map = &mapping.row_map[..n_intervals];
        let cube_map = &mapping.cube_map[..n_intervals];
        let pos_map = &mapping.pos_map[..n_intervals];
        for (row_idx, row) in rows.iter_mut().enumerate().take(nrrow) {
            // C++ TiledShapeStMan stores upper bounds for row intervals in rowMap.
            // The containing interval is the first entry whose upper bound is >= row.
            let interval = row_map.partition_point(|&rm| rm < row_idx as u32);
            if interval >= n_intervals {
                continue;
            }

            let cube_idx = cube_map[interval] as usize;
            if cube_idx == 0 || cube_idx >= cube_col_data.len() {
                continue; // dummy hypercube used for undefined rows
            }

            let diff = row_map[interval] as usize - row_idx;
            if diff > pos_map[interval] as usize {
                continue;
            }
            let Some(pos_in_cube) = (pos_map[interval] as usize).checked_sub(diff) else {
                return Err(StorageError::FormatMismatch(format!(
                    "invalid TiledShapeStMan row map for row {row_idx}: interval {interval} has pos {} < diff {diff}",
                    pos_map[interval]
                )));
            };

            for (col_idx, col_desc) in col_descs.iter().enumerate() {
                if let Some(ref ccd) = cube_col_data[cube_idx][col_idx] {
                    let dt = header.col_data_types[col_idx];
                    let elem_size = tile_element_size(dt);
                    let row_bytes = ccd.cell_nelem * elem_size;
                    let start = pos_in_cube * row_bytes;
                    let end = start + row_bytes;
                    if end <= ccd.raw.len() {
                        let value = decode_array_value(
                            &ccd.raw[start..end],
                            &ccd.cell_shape,
                            dt,
                            header.big_endian,
                        )?;
                        row.upsert(col_desc.col_name.clone(), value);
                    }
                }
            }
        }
    }

    for (row_idx, row) in rows.iter_mut().enumerate().take(nrrow) {
        for col_desc in col_descs {
            if row.get(&col_desc.col_name).is_some() {
                continue;
            }
            let value = if col_desc.is_record() {
                Value::Record(RecordValue::default())
            } else {
                let dt = CasacoreDataType::from_primitive_type(
                    col_desc.require_primitive_type()?,
                    false,
                );
                if let Some(set) = undefined_cells.get_mut(row_idx) {
                    set.insert(col_desc.col_name.clone());
                }
                super::make_undefined_array(dt, col_desc.nrdim.max(0) as usize)
            };
            row.push(RecordField::new(col_desc.col_name.clone(), value));
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct SelectedCubeRow {
    out_idx: usize,
    pos_in_cube: usize,
}

#[allow(clippy::too_many_arguments)]
fn load_tiled_column_rows_shape_variant(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
    mapping: &ShapeRowMapping<'_>,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let n_intervals = mapping.nr_used_row_map as usize;
    if n_intervals == 0 {
        return Ok(vec![None; selected_rows.len()]);
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SelectedCubeRow>> =
        std::collections::BTreeMap::new();
    for (out_idx, &row_idx) in selected_rows.iter().enumerate() {
        let interval = mapping.row_map[..n_intervals].partition_point(|&rm| rm < row_idx as u32);
        if interval >= n_intervals {
            continue;
        }
        let cube_idx = mapping.cube_map[interval] as usize;
        if cube_idx == 0 || cube_idx >= header.cubes.len() {
            continue;
        }
        let diff = mapping.row_map[interval] as usize - row_idx;
        if diff > mapping.pos_map[interval] as usize {
            continue;
        }
        let Some(pos_in_cube) = (mapping.pos_map[interval] as usize).checked_sub(diff) else {
            return Err(StorageError::FormatMismatch(format!(
                "invalid TiledShapeStMan row map for row {row_idx}: interval {interval} has pos {} < diff {diff}",
                mapping.pos_map[interval]
            )));
        };
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SelectedCubeRow {
                out_idx,
                pos_in_cube,
            });
    }
    load_selected_rows_from_touched_cubes(
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
        selected_rows.len(),
        patches_by_cube,
    )
}

/// Load columns from a `TiledDataStMan` (user-controlled hypercube assignment).
///
/// Uses binary search in `row_map` to find the chunk index for each row.
/// `cube_map[chunk]` gives the hypercube and `pos_map[chunk]` gives the
/// starting position in the last dimension.
///
/// # C++ equivalent
///
/// `TiledDataStMan::getArraySection()`.
#[allow(clippy::too_many_arguments)]
fn load_tiled_data_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
    _undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
    row_map: &[u64],
    cube_map: &[u32],
    pos_map: &[u32],
) -> Result<(), StorageError> {
    // Pre-read all TSM data files and compute per-cube layouts.
    let mut file_cache: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();

    struct CubeLayout {
        bucket_size: usize,
        col_offsets: Vec<usize>,
    }
    let mut cube_layouts: Vec<Option<CubeLayout>> = Vec::with_capacity(header.cubes.len());

    for cube in &header.cubes {
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            cube_layouts.push(None);
            continue;
        }
        let (bucket_size, col_offsets) =
            compute_tile_layout(&header.col_data_types, &cube.tile_shape);
        cube_layouts.push(Some(CubeLayout {
            bucket_size,
            col_offsets,
        }));

        let fseq = cube.file_seq_nr as u32;
        if let std::collections::hash_map::Entry::Vacant(e) = file_cache.entry(fseq) {
            let path = tsm_data_path(table_path, dm_seq_nr, fseq);
            let data = std::fs::read(&path).map_err(|err| {
                StorageError::FormatMismatch(format!("cannot read {}: {err}", path.display()))
            })?;
            e.insert(data);
        }
    }

    // Pre-reconstruct per-cube per-column data.
    struct CubeColumnData {
        raw: Vec<u8>,
        cell_shape: Vec<usize>,
        cell_nelem: usize,
    }
    let mut cube_col_data: Vec<Vec<Option<CubeColumnData>>> =
        Vec::with_capacity(header.cubes.len());

    for (cube_idx, cube) in header.cubes.iter().enumerate() {
        let mut cols = Vec::with_capacity(col_descs.len());
        let layout = &cube_layouts[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() || layout.is_none() {
            for _ in col_descs {
                cols.push(None);
            }
            cube_col_data.push(cols);
            continue;
        }
        let layout = layout.as_ref().unwrap();
        let file_data = &file_cache[&(cube.file_seq_nr as u32)];

        let cell_ndim = cube.cube_shape.len() - 1;
        let cell_shape: Vec<usize> = cube.cube_shape[..cell_ndim].to_vec();
        let cell_nelem: usize = cell_shape.iter().product();

        for (col_idx, _) in col_descs.iter().enumerate() {
            if col_idx >= header.col_data_types.len() {
                cols.push(None);
                continue;
            }
            let dt = header.col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            if elem_size == 0 {
                cols.push(None);
                continue;
            }
            let raw = reconstruct_cube_column(
                file_data,
                cube.file_offset as usize,
                &cube.cube_shape,
                &cube.tile_shape,
                layout.col_offsets[col_idx],
                layout.bucket_size,
                dt,
                elem_size,
            )?;
            cols.push(Some(CubeColumnData {
                raw,
                cell_shape: cell_shape.clone(),
                cell_nelem,
            }));
        }
        cube_col_data.push(cols);
    }

    // Map rows to cubes using binary search in row_map.
    let n_chunks = row_map.len();
    for (row_idx, row) in rows.iter_mut().enumerate().take(nrrow) {
        // Binary search: find the first chunk where row_map[chunk] > row_idx.
        let chunk = match row_map.binary_search(&(row_idx as u64)) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    continue; // row before first chunk
                }
                i - 1
            }
        };
        if chunk >= n_chunks {
            continue;
        }

        let cube_idx = cube_map[chunk] as usize;
        let chunk_start = row_map[chunk] as usize;
        let pos_in_cube = pos_map[chunk] as usize + (row_idx - chunk_start);

        if cube_idx >= cube_col_data.len() {
            continue;
        }

        for (col_idx, col_desc) in col_descs.iter().enumerate() {
            if let Some(ref ccd) = cube_col_data[cube_idx][col_idx] {
                let dt = header.col_data_types[col_idx];
                let elem_size = tile_element_size(dt);
                let row_bytes = ccd.cell_nelem * elem_size;
                let start = pos_in_cube * row_bytes;
                let end = start + row_bytes;
                if end <= ccd.raw.len() {
                    let value = decode_array_value(
                        &ccd.raw[start..end],
                        &ccd.cell_shape,
                        dt,
                        header.big_endian,
                    )?;
                    row.push(RecordField::new(col_desc.col_name.clone(), value));
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn load_tiled_column_rows_data_variant(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
    row_map: &[u64],
    cube_map: &[u32],
    pos_map: &[u32],
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SelectedCubeRow>> =
        std::collections::BTreeMap::new();
    let n_chunks = row_map.len();
    for (out_idx, &row_idx) in selected_rows.iter().enumerate() {
        let chunk = match row_map.binary_search(&(row_idx as u64)) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    continue;
                }
                i - 1
            }
        };
        if chunk >= n_chunks {
            continue;
        }
        let cube_idx = cube_map[chunk] as usize;
        if cube_idx >= header.cubes.len() {
            continue;
        }
        let chunk_start = row_map[chunk] as usize;
        let pos_in_cube = pos_map[chunk] as usize + (row_idx - chunk_start);
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SelectedCubeRow {
                out_idx,
                pos_in_cube,
            });
    }
    load_selected_rows_from_touched_cubes(
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
        selected_rows.len(),
        patches_by_cube,
    )
}

/// Load columns from a `TiledCellStMan` (one hypercube per row).
fn load_tiled_cell_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
    _undefined_cells: &mut [HashSet<String>],
    nrrow: usize,
) -> Result<(), StorageError> {
    let mut file_cache: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();

    for (row_idx, cube) in header.cubes.iter().enumerate().take(nrrow) {
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }

        let fseq = cube.file_seq_nr as u32;
        if let std::collections::hash_map::Entry::Vacant(e) = file_cache.entry(fseq) {
            let path = tsm_data_path(table_path, dm_seq_nr, fseq);
            let data = std::fs::read(&path).map_err(|err| {
                StorageError::FormatMismatch(format!("cannot read {}: {err}", path.display()))
            })?;
            e.insert(data);
        }
        let file_data = &file_cache[&fseq];

        let (bucket_size, col_offsets) =
            compute_tile_layout(&header.col_data_types, &cube.tile_shape);

        // For TiledCellStMan, the cube IS the cell (no extra row dimension).
        let cell_shape = &cube.cube_shape;

        for (col_idx, col_desc) in col_descs.iter().enumerate() {
            if col_idx >= header.col_data_types.len() {
                break;
            }
            let dt = header.col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            if elem_size == 0 {
                continue;
            }
            let raw = reconstruct_cube_column(
                file_data,
                cube.file_offset as usize,
                cell_shape,
                &cube.tile_shape,
                col_offsets[col_idx],
                bucket_size,
                dt,
                elem_size,
            )?;
            let value = decode_array_value(&raw, cell_shape, dt, header.big_endian)?;
            rows[row_idx].push(RecordField::new(col_desc.col_name.clone(), value));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn load_tiled_column_rows_cell_variant(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected_rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let mut outputs = vec![None; selected_rows.len()];
    let mut read_session = TileReadSession::default();

    for (out_idx, &row_idx) in selected_rows.iter().enumerate() {
        let Some(cube) = header.cubes.get(row_idx) else {
            continue;
        };
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        outputs[out_idx] = decode_tiled_cell_from_shared_tiles(
            table_path,
            dm_seq_nr,
            header,
            row_idx,
            cube,
            target_col_idx,
            col_desc,
            dt,
            elem_size,
            &mut read_session,
        )?;
    }
    Ok(outputs)
}

#[allow(clippy::too_many_arguments)]
fn load_selected_rows_from_touched_cubes(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    output_len: usize,
    patches_by_cube: std::collections::BTreeMap<usize, Vec<SelectedCubeRow>>,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let mut outputs = vec![None; output_len];
    let mut read_session = TileReadSession::default();

    for (cube_idx, patches) in patches_by_cube {
        let Some(cube) = header.cubes.get(cube_idx) else {
            continue;
        };
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        for selected in patches {
            outputs[selected.out_idx] = decode_selected_cube_row_from_shared_tiles(
                table_path,
                dm_seq_nr,
                header,
                cube_idx,
                cube,
                target_col_idx,
                col_desc,
                dt,
                elem_size,
                selected,
                &mut read_session,
            )?;
        }
    }

    Ok(outputs)
}

fn extract_selected_rows_from_cube_raw(
    cube_raw: &[u8],
    cube_shape: &[usize],
    selected_rows: impl IntoIterator<Item = SelectedCubeRow>,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    big_endian: bool,
    output_len: usize,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let cell_ndim = cube_shape.len().saturating_sub(1);
    let cell_shape: Vec<usize> = cube_shape[..cell_ndim].to_vec();
    let cell_nelem: usize = cell_shape.iter().product();
    let elem_size = tile_element_size(dt);
    let row_bytes = cell_nelem * elem_size;
    let mut outputs = vec![None; output_len];

    for selected in selected_rows {
        let start = selected.pos_in_cube * row_bytes;
        let end = start + row_bytes;
        if end > cube_raw.len() {
            continue;
        }
        let value = decode_array_value(&cube_raw[start..end], &cell_shape, dt, big_endian)?;
        match value {
            Value::Array(array) => outputs[selected.out_idx] = Some(array),
            other => {
                return Err(StorageError::FormatMismatch(format!(
                    "tiled array column {} decoded as non-array value {:?}",
                    col_desc.col_name, other
                )));
            }
        }
    }

    Ok(outputs)
}

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

// ---------------------------------------------------------------------------
// Save interface (write columns with tiled DM)
// ---------------------------------------------------------------------------

/// Save columns to a tiled storage manager.
///
/// Determines the appropriate variant from the `DataManagerKind` and writes
/// header + tile data files.
///
/// `dm_name` is the hypercolumn / data-manager name written into the
/// TiledStMan header. C++ uses this to look up the DM when opening the
/// table. Pass the column name (or an explicit name) to match C++ behaviour.
#[allow(clippy::too_many_arguments)]
pub(crate) fn save_tiled_columns(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_type_name: &str,
    all_col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    default_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::save_tiled_columns dm_seq={} type={} cols={} rows={}",
        dm_seq_nr,
        dm_type_name,
        all_col_descs.len(),
        rows.len()
    ));
    let result = match dm_type_name {
        "TiledColumnStMan" => save_tiled_column_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
            dm_name,
        ),
        "TiledShapeStMan" => save_tiled_shape_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
            dm_name,
        ),
        "TiledCellStMan" => save_tiled_cell_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
            dm_name,
        ),
        other => Err(StorageError::FormatMismatch(format!(
            "unknown tiled DM type: {other}"
        ))),
    };
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("dispatch_complete");
    }
    result
}

pub(crate) struct SingleColumnTiledSaveOptions<'a> {
    pub(crate) dm_type_name: &'a str,
    pub(crate) big_endian: bool,
    pub(crate) default_tile_shape: Option<&'a [usize]>,
    pub(crate) dm_name: &'a str,
}

pub(crate) fn save_tiled_single_column_values(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    options: SingleColumnTiledSaveOptions<'_>,
) -> Result<(), StorageError> {
    match options.dm_type_name {
        "TiledColumnStMan" => save_single_column_tiled_column_stman(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            options.big_endian,
            options.default_tile_shape,
            options.dm_name,
        ),
        "TiledShapeStMan" => save_single_column_tiled_shape_stman(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            options.big_endian,
            options.default_tile_shape,
            options.dm_name,
        ),
        _ => {
            let rows: Vec<RecordValue> = values
                .iter()
                .map(|value| match value {
                    Some(value) => RecordValue::new(vec![RecordField::new(
                        col_desc.col_name.clone(),
                        (*value).clone(),
                    )]),
                    None => RecordValue::default(),
                })
                .collect();
            save_tiled_columns(
                table_path,
                dm_seq_nr,
                options.dm_type_name,
                std::slice::from_ref(col_desc),
                &rows,
                options.big_endian,
                options.default_tile_shape,
                options.dm_name,
            )
        }
    }
}

type SparseArrayRowValues = Vec<(usize, Option<ArrayValue>)>;

#[derive(Clone, Copy, Debug)]
struct SparseTiledCubeRowPatch {
    global_row_idx: usize,
    pos_in_cube: usize,
}

pub(crate) fn save_tiled_columns_sparse_rows_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_type_name: &str,
    col_descs: &[ColumnDescContents],
    sparse_columns: &[Option<SparseArrayRowValues>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    if changed_rows.is_empty() {
        return Ok(true);
    }
    if col_descs.len() != sparse_columns.len() {
        return Ok(false);
    }
    match dm_type_name {
        "TiledColumnStMan" => save_tiled_column_group_rows_sparse_in_place(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            changed_rows,
        ),
        "TiledCellStMan" => save_tiled_cell_group_rows_sparse_in_place(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            changed_rows,
        ),
        "TiledDataStMan" => save_tiled_data_group_rows_sparse_in_place(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            changed_rows,
        ),
        "TiledShapeStMan" => save_tiled_shape_group_rows_sparse_in_place(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            changed_rows,
        ),
        _ => Ok(false),
    }
}

fn cube_with_single_row_axis(cube: &TsmCubeInfo) -> TsmCubeInfo {
    let mut cube_with_row_axis = cube.clone();
    cube_with_row_axis.cube_shape.push(1);
    cube_with_row_axis.tile_shape.push(1);
    cube_with_row_axis
}

fn save_tiled_column_group_rows_sparse_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    sparse_columns: &[Option<SparseArrayRowValues>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if !matches!(variant, TiledVariant::Column { .. })
        || header.col_data_types.len() != col_descs.len()
    {
        return Ok(false);
    }
    let Some(cube) = header.cubes.first() else {
        return Ok(false);
    };
    if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
        return Ok(false);
    }
    let row_axis = cube.cube_shape.len() - 1;
    let row_count = cube.cube_shape[row_axis];
    let row_patches: Vec<_> = changed_rows
        .iter()
        .copied()
        .filter(|&row_idx| row_idx < row_count)
        .map(|row_idx| SparseTiledCubeRowPatch {
            global_row_idx: row_idx,
            pos_in_cube: row_idx,
        })
        .collect();
    if row_patches.is_empty() {
        return Ok(true);
    }
    patch_tiled_cube_sparse_rows(
        table_path,
        dm_seq_nr,
        col_descs,
        sparse_columns,
        header.big_endian,
        &header.col_data_types,
        cube,
        &row_patches,
    )?;
    Ok(true)
}

fn save_tiled_cell_group_rows_sparse_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    sparse_columns: &[Option<SparseArrayRowValues>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if !matches!(variant, TiledVariant::Cell { .. })
        || header.col_data_types.len() != col_descs.len()
    {
        return Ok(false);
    }

    for &row_idx in changed_rows {
        let Some(cube) = header.cubes.get(row_idx) else {
            continue;
        };
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        let cube_with_row_axis = cube_with_single_row_axis(cube);
        patch_tiled_cube_sparse_rows(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            header.big_endian,
            &header.col_data_types,
            &cube_with_row_axis,
            &[SparseTiledCubeRowPatch {
                global_row_idx: row_idx,
                pos_in_cube: 0,
            }],
        )?;
    }

    Ok(true)
}

fn save_tiled_data_group_rows_sparse_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    sparse_columns: &[Option<SparseArrayRowValues>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    let TiledVariant::Data {
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        return Ok(false);
    };
    if header.col_data_types.len() != col_descs.len() || row_map.is_empty() {
        return Ok(false);
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SparseTiledCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &row_idx in changed_rows {
        let chunk = match row_map.binary_search(&(row_idx as u64)) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    continue;
                }
                i - 1
            }
        };
        let cube_idx = cube_map[chunk] as usize;
        if cube_idx >= header.cubes.len() {
            continue;
        }
        let chunk_start = row_map[chunk] as usize;
        let pos_in_cube = pos_map[chunk] as usize + (row_idx - chunk_start);
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SparseTiledCubeRowPatch {
                global_row_idx: row_idx,
                pos_in_cube,
            });
    }
    if patches_by_cube.is_empty() {
        return Ok(true);
    }

    for (cube_idx, row_patches) in patches_by_cube {
        let cube = &header.cubes[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        patch_tiled_cube_sparse_rows(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            header.big_endian,
            &header.col_data_types,
            cube,
            &row_patches,
        )?;
    }

    Ok(true)
}

fn save_tiled_shape_group_rows_sparse_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    sparse_columns: &[Option<SparseArrayRowValues>],
    changed_rows: &[usize],
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    let TiledVariant::Shape {
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        return Ok(false);
    };
    if header.col_data_types.len() != col_descs.len() {
        return Ok(false);
    }
    let n_intervals = nr_used_row_map as usize;
    if n_intervals == 0 {
        return Ok(false);
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SparseTiledCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &row_idx in changed_rows {
        let interval = row_map[..n_intervals].partition_point(|&rm| rm < row_idx as u32);
        if interval >= n_intervals {
            continue;
        }
        let cube_idx = cube_map[interval] as usize;
        if cube_idx == 0 || cube_idx >= header.cubes.len() {
            continue;
        }
        let diff = row_map[interval] as usize - row_idx;
        let Some(pos_in_cube) = (pos_map[interval] as usize).checked_sub(diff) else {
            return Err(StorageError::FormatMismatch(format!(
                "invalid TiledShapeStMan row map for row {row_idx}: interval {interval} has pos {} < diff {diff}",
                pos_map[interval]
            )));
        };
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SparseTiledCubeRowPatch {
                global_row_idx: row_idx,
                pos_in_cube,
            });
    }
    if patches_by_cube.is_empty() {
        return Ok(true);
    }

    for (cube_idx, row_patches) in patches_by_cube {
        let cube = &header.cubes[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        patch_tiled_cube_sparse_rows(
            table_path,
            dm_seq_nr,
            col_descs,
            sparse_columns,
            header.big_endian,
            &header.col_data_types,
            cube,
            &row_patches,
        )?;
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug)]
struct SingleColumnCubeRowPatch {
    global_row_idx: usize,
    pos_in_cube: usize,
}

pub(crate) fn save_tiled_single_column_rows_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    changed_rows: &[usize],
    options: SingleColumnTiledSaveOptions<'_>,
) -> Result<bool, StorageError> {
    if changed_rows.is_empty() {
        return Ok(true);
    }
    match options.dm_type_name {
        "TiledColumnStMan" => save_single_column_tiled_column_rows_in_place(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            changed_rows,
            options.big_endian,
        ),
        "TiledShapeStMan" => save_single_column_tiled_shape_rows_in_place(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            changed_rows,
            options.big_endian,
        ),
        _ => Ok(false),
    }
}

fn save_single_column_tiled_column_rows_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    changed_rows: &[usize],
    big_endian: bool,
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if !matches!(variant, TiledVariant::Column { .. }) || header.col_data_types.len() != 1 {
        return Ok(false);
    }
    let Some(cube) = header.cubes.first() else {
        return Ok(false);
    };
    if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
        return Ok(false);
    }
    let row_axis = cube.cube_shape.len() - 1;
    let row_count = cube.cube_shape[row_axis];
    let row_patches: Vec<_> = changed_rows
        .iter()
        .copied()
        .filter(|&row_idx| row_idx < values.len() && row_idx < row_count)
        .map(|row_idx| SingleColumnCubeRowPatch {
            global_row_idx: row_idx,
            pos_in_cube: row_idx,
        })
        .collect();
    if row_patches.is_empty() {
        return Ok(true);
    }
    patch_single_column_tiled_cube_rows(
        table_path,
        dm_seq_nr,
        col_desc,
        values,
        big_endian,
        header.col_data_types[0],
        cube,
        &row_patches,
    )?;
    Ok(true)
}

fn save_single_column_tiled_shape_rows_in_place(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    changed_rows: &[usize],
    big_endian: bool,
) -> Result<bool, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    let TiledVariant::Shape {
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        return Ok(false);
    };
    if header.col_data_types.len() != 1 {
        return Ok(false);
    }
    let n_intervals = nr_used_row_map as usize;
    if n_intervals == 0 {
        return Ok(false);
    }

    let mut patches_by_cube: std::collections::BTreeMap<usize, Vec<SingleColumnCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &row_idx in changed_rows {
        if row_idx >= values.len() {
            continue;
        }
        let interval = row_map[..n_intervals].partition_point(|&rm| rm < row_idx as u32);
        if interval >= n_intervals {
            continue;
        }
        let cube_idx = cube_map[interval] as usize;
        if cube_idx == 0 || cube_idx >= header.cubes.len() {
            continue;
        }
        let diff = row_map[interval] as usize - row_idx;
        let Some(pos_in_cube) = (pos_map[interval] as usize).checked_sub(diff) else {
            return Err(StorageError::FormatMismatch(format!(
                "invalid TiledShapeStMan row map for row {row_idx}: interval {interval} has pos {} < diff {diff}",
                pos_map[interval]
            )));
        };
        patches_by_cube
            .entry(cube_idx)
            .or_default()
            .push(SingleColumnCubeRowPatch {
                global_row_idx: row_idx,
                pos_in_cube,
            });
    }
    if patches_by_cube.is_empty() {
        return Ok(true);
    }

    for (cube_idx, row_patches) in patches_by_cube {
        let cube = &header.cubes[cube_idx];
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
            continue;
        }
        patch_single_column_tiled_cube_rows(
            table_path,
            dm_seq_nr,
            col_desc,
            values,
            big_endian,
            header.col_data_types[0],
            cube,
            &row_patches,
        )?;
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn patch_single_column_tiled_cube_rows(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    big_endian: bool,
    col_data_type: CasacoreDataType,
    cube: &TsmCubeInfo,
    row_patches: &[SingleColumnCubeRowPatch],
) -> Result<(), StorageError> {
    let ndim = cube.cube_shape.len();
    if ndim == 0 {
        return Ok(());
    }
    let row_axis = ndim - 1;
    if row_axis == 0 {
        return Err(StorageError::FormatMismatch(format!(
            "single-column tiled sparse save requires array cells for {}",
            col_desc.col_name
        )));
    }

    let cell_shape = &cube.cube_shape[..row_axis];
    let tile_cell_shape = &cube.tile_shape[..row_axis];
    let row_tile_len = cube.tile_shape[row_axis].max(1);
    let row_block_pixels: usize = tile_cell_shape.iter().product();
    let elem_size = tile_element_size(col_data_type);
    let expected_row_bytes = cell_shape.iter().product::<usize>() * elem_size;
    let row_block_bytes = row_block_pixels * elem_size;
    let nrpixels: usize = cube.tile_shape.iter().product();
    let (bucket_size, col_offsets) =
        compute_tile_layout(std::slice::from_ref(&col_data_type), &cube.tile_shape);
    let col_offset = col_offsets[0];
    let tiles_per_dim: Vec<usize> = cube
        .cube_shape
        .iter()
        .zip(cube.tile_shape.iter())
        .map(|(&cube_len, &tile_len)| cube_len.div_ceil(tile_len))
        .collect();
    let cell_tiles_per_dim = &tiles_per_dim[..row_axis];
    let cell_tile_total = cell_tiles_per_dim.iter().product::<usize>().max(1);

    let mut patches_by_row_tile: std::collections::BTreeMap<usize, Vec<SingleColumnCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &patch in row_patches {
        patches_by_row_tile
            .entry(patch.pos_in_cube / row_tile_len)
            .or_default()
            .push(patch);
    }

    let tsm_path = tsm_data_path(table_path, dm_seq_nr, cube.file_seq_nr as u32);
    let mut file = OpenOptions::new().read(true).write(true).open(&tsm_path)?;
    let tile_storage_len = tile_storage_bytes(col_data_type, nrpixels);
    for (row_tile_idx, row_tile_patches) in patches_by_row_tile {
        if row_tile_idx >= tiles_per_dim[row_axis] {
            continue;
        }
        for cell_tile_linear in 0..cell_tile_total {
            let cell_tile_pos = if cell_tiles_per_dim.is_empty() {
                Vec::new()
            } else {
                linear_to_nd(cell_tile_linear, cell_tiles_per_dim)
            };
            let mut tile_pos = cell_tile_pos.clone();
            tile_pos.push(row_tile_idx);
            let tile_idx = nd_to_linear(&tile_pos, &tiles_per_dim);
            let tile_start = cube.file_offset as usize + tile_idx * bucket_size + col_offset;
            let mut packed_tile = vec![0u8; tile_storage_len];
            file.seek(SeekFrom::Start(tile_start as u64))?;
            file.read_exact(&mut packed_tile)?;
            let mut unpacked_tile = read_tile_storage(&packed_tile, col_data_type, nrpixels);

            let cell_cube_start: Vec<usize> = (0..row_axis)
                .map(|dim| cell_tile_pos.get(dim).copied().unwrap_or(0) * cube.tile_shape[dim])
                .collect();
            let actual_cell_extent: Vec<usize> = (0..row_axis)
                .map(|dim| {
                    std::cmp::min(cube.tile_shape[dim], cell_shape[dim] - cell_cube_start[dim])
                })
                .collect();

            for patch in &row_tile_patches {
                let local_row = patch.pos_in_cube % row_tile_len;
                let row_slice_start = local_row * row_block_bytes;
                let row_slice_end = row_slice_start + row_block_bytes;
                let row_tile = &mut unpacked_tile[row_slice_start..row_slice_end];
                let encoded_row = match values.get(patch.global_row_idx).copied().flatten() {
                    Some(value) => {
                        let (encoded, dt) = encode_array_value(value, big_endian)?;
                        if dt != col_data_type {
                            return Err(StorageError::FormatMismatch(format!(
                                "tiled sparse save column {} expected {:?} but encoded {:?}",
                                col_desc.col_name, col_data_type, dt
                            )));
                        }
                        if encoded.len() != expected_row_bytes {
                            return Err(StorageError::FormatMismatch(format!(
                                "tiled sparse save column {} expected {expected_row_bytes} row bytes but encoded {}",
                                col_desc.col_name,
                                encoded.len()
                            )));
                        }
                        encoded
                    }
                    None => vec![0u8; expected_row_bytes],
                };
                copy_cube_to_tile(
                    &encoded_row,
                    cell_shape,
                    row_tile,
                    tile_cell_shape,
                    &cell_cube_start,
                    &actual_cell_extent,
                    elem_size,
                );
            }

            write_tile_storage(&mut packed_tile, col_data_type, &unpacked_tile, nrpixels);
            file.seek(SeekFrom::Start(tile_start as u64))?;
            file.write_all(&packed_tile)?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn patch_tiled_cube_sparse_rows(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    sparse_columns: &[Option<SparseArrayRowValues>],
    big_endian: bool,
    col_data_types: &[CasacoreDataType],
    cube: &TsmCubeInfo,
    row_patches: &[SparseTiledCubeRowPatch],
) -> Result<(), StorageError> {
    let ndim = cube.cube_shape.len();
    if ndim == 0 {
        return Ok(());
    }
    let row_axis = ndim - 1;
    if row_axis == 0 {
        return Err(StorageError::FormatMismatch(
            "tiled sparse save requires array-valued cells".to_string(),
        ));
    }

    struct SparseColumnPatch {
        col_data_type: CasacoreDataType,
        elem_size: usize,
        expected_row_bytes: usize,
        row_block_bytes: usize,
        tile_storage_len: usize,
        col_offset: usize,
        encoded_rows: std::collections::HashMap<usize, Vec<u8>>,
    }

    let cell_shape = &cube.cube_shape[..row_axis];
    let tile_cell_shape = &cube.tile_shape[..row_axis];
    let row_tile_len = cube.tile_shape[row_axis].max(1);
    let row_block_pixels: usize = tile_cell_shape.iter().product();
    let nrpixels: usize = cube.tile_shape.iter().product();
    let (bucket_size, col_offsets) = compute_tile_layout(col_data_types, &cube.tile_shape);
    let tiles_per_dim: Vec<usize> = cube
        .cube_shape
        .iter()
        .zip(cube.tile_shape.iter())
        .map(|(&cube_len, &tile_len)| cube_len.div_ceil(tile_len))
        .collect();
    let cell_tiles_per_dim = &tiles_per_dim[..row_axis];
    let cell_tile_total = cell_tiles_per_dim.iter().product::<usize>().max(1);

    let mut column_patches = Vec::new();
    for (col_idx, sparse_values) in sparse_columns.iter().enumerate() {
        let Some(sparse_values) = sparse_values else {
            continue;
        };
        if col_idx >= col_descs.len() || col_idx >= col_data_types.len() {
            return Ok(());
        }
        let col_data_type = col_data_types[col_idx];
        let elem_size = tile_element_size(col_data_type);
        if elem_size == 0 {
            return Err(StorageError::FormatMismatch(format!(
                "tiled sparse save does not support non-primitive array column {}",
                col_descs[col_idx].col_name
            )));
        }
        let expected_row_bytes = cell_shape.iter().product::<usize>() * elem_size;
        let mut encoded_rows = std::collections::HashMap::with_capacity(sparse_values.len());
        for (row_idx, value) in sparse_values {
            let encoded = match value {
                Some(value) => {
                    let wrapped = Value::Array(value.clone());
                    let (encoded, dt) = encode_array_value(&wrapped, big_endian)?;
                    if dt != col_data_type {
                        return Err(StorageError::FormatMismatch(format!(
                            "tiled sparse save column {} expected {:?} but encoded {:?}",
                            col_descs[col_idx].col_name, col_data_type, dt
                        )));
                    }
                    if encoded.len() != expected_row_bytes {
                        return Err(StorageError::FormatMismatch(format!(
                            "tiled sparse save column {} expected {expected_row_bytes} row bytes but encoded {}",
                            col_descs[col_idx].col_name,
                            encoded.len()
                        )));
                    }
                    encoded
                }
                None => vec![0u8; expected_row_bytes],
            };
            encoded_rows.insert(*row_idx, encoded);
        }
        if encoded_rows.is_empty() {
            continue;
        }
        column_patches.push(SparseColumnPatch {
            col_data_type,
            elem_size,
            expected_row_bytes,
            row_block_bytes: row_block_pixels * elem_size,
            tile_storage_len: tile_storage_bytes(col_data_type, nrpixels),
            col_offset: col_offsets[col_idx],
            encoded_rows,
        });
    }
    if column_patches.is_empty() {
        return Ok(());
    }

    let mut patches_by_row_tile: std::collections::BTreeMap<usize, Vec<SparseTiledCubeRowPatch>> =
        std::collections::BTreeMap::new();
    for &patch in row_patches {
        patches_by_row_tile
            .entry(patch.pos_in_cube / row_tile_len)
            .or_default()
            .push(patch);
    }

    let tsm_path = tsm_data_path(table_path, dm_seq_nr, cube.file_seq_nr as u32);
    let mut file = OpenOptions::new().read(true).write(true).open(&tsm_path)?;
    for (row_tile_idx, row_tile_patches) in patches_by_row_tile {
        if row_tile_idx >= tiles_per_dim[row_axis] {
            continue;
        }
        for cell_tile_linear in 0..cell_tile_total {
            let cell_tile_pos = if cell_tiles_per_dim.is_empty() {
                Vec::new()
            } else {
                linear_to_nd(cell_tile_linear, cell_tiles_per_dim)
            };
            let mut tile_pos = cell_tile_pos.clone();
            tile_pos.push(row_tile_idx);
            let tile_idx = nd_to_linear(&tile_pos, &tiles_per_dim);
            let tile_start = cube.file_offset as usize + tile_idx * bucket_size;
            let mut packed_bucket = vec![0u8; bucket_size];
            file.seek(SeekFrom::Start(tile_start as u64))?;
            file.read_exact(&mut packed_bucket)?;

            let cell_cube_start: Vec<usize> = (0..row_axis)
                .map(|dim| cell_tile_pos.get(dim).copied().unwrap_or(0) * cube.tile_shape[dim])
                .collect();
            let actual_cell_extent: Vec<usize> = (0..row_axis)
                .map(|dim| {
                    std::cmp::min(cube.tile_shape[dim], cell_shape[dim] - cell_cube_start[dim])
                })
                .collect();

            for column_patch in &column_patches {
                let src_start = column_patch.col_offset;
                let src_end = src_start + column_patch.tile_storage_len;
                let mut unpacked_tile = read_tile_storage(
                    &packed_bucket[src_start..src_end],
                    column_patch.col_data_type,
                    nrpixels,
                );
                for patch in &row_tile_patches {
                    let Some(encoded_row) = column_patch.encoded_rows.get(&patch.global_row_idx)
                    else {
                        continue;
                    };
                    debug_assert_eq!(encoded_row.len(), column_patch.expected_row_bytes);
                    let local_row = patch.pos_in_cube % row_tile_len;
                    let row_slice_start = local_row * column_patch.row_block_bytes;
                    let row_slice_end = row_slice_start + column_patch.row_block_bytes;
                    let row_tile = &mut unpacked_tile[row_slice_start..row_slice_end];
                    copy_cube_to_tile(
                        encoded_row,
                        cell_shape,
                        row_tile,
                        tile_cell_shape,
                        &cell_cube_start,
                        &actual_cell_extent,
                        column_patch.elem_size,
                    );
                }
                write_tile_storage(
                    &mut packed_bucket[src_start..src_end],
                    column_patch.col_data_type,
                    &unpacked_tile,
                    nrpixels,
                );
            }

            file.seek(SeekFrom::Start(tile_start as u64))?;
            file.write_all(&packed_bucket)?;
        }
    }

    Ok(())
}

fn format_shape(shape: &[usize]) -> String {
    let mut out = String::from("[");
    for (idx, value) in shape.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
    out.push(']');
    out
}

fn clamp_tile_shape_dims(tile_shape: &mut [usize]) {
    for dim in tile_shape {
        *dim = (*dim).max(1);
    }
}

fn encode_column_cube_values(
    values: &[Option<&Value>],
    cube_shape: &[usize],
    cell_nelem: usize,
    elem_size: usize,
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    let cube_nelem: usize = cube_shape.iter().product();
    let mut cube_bytes = vec![0u8; cube_nelem * elem_size];
    for (row_idx, value) in values.iter().enumerate() {
        if let Some(value) = value {
            let (encoded, _) = encode_array_value(value, big_endian)?;
            let start = row_idx * cell_nelem * elem_size;
            let end = start + cell_nelem * elem_size;
            if end <= cube_bytes.len() && encoded.len() == cell_nelem * elem_size {
                cube_bytes[start..end].copy_from_slice(&encoded);
            }
        }
    }
    Ok(cube_bytes)
}

#[allow(clippy::too_many_arguments)]
fn encode_single_column_shape_cube_direct(
    values: &[Option<&Value>],
    cell_shape: &[usize],
    tile_shape: &[usize],
    cell_nelem: usize,
    elem_size: usize,
    nr_tiles: usize,
    bucket_size: usize,
    col_offset: usize,
    col_data_type: CasacoreDataType,
    big_endian: bool,
) -> Result<Option<Vec<u8>>, StorageError> {
    if tile_shape.len() != cell_shape.len() + 1 {
        return Ok(None);
    }
    if tile_shape[..cell_shape.len()] != *cell_shape {
        return Ok(None);
    }
    let rows_per_tile = tile_shape[cell_shape.len()];
    if rows_per_tile == 0 {
        return Ok(None);
    }

    let row_bytes = cell_nelem * elem_size;
    let tile_nelem: usize = tile_shape.iter().product();
    let tile_storage_len = tile_storage_bytes(col_data_type, tile_nelem);
    let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
    for (pos_in_cube, value) in values.iter().enumerate() {
        let Some(value) = value else {
            continue;
        };
        let (encoded, encoded_type) = encode_array_value(value, big_endian)?;
        if encoded_type != col_data_type {
            return Ok(None);
        }
        let tile_idx = pos_in_cube / rows_per_tile;
        let row_in_tile = pos_in_cube % rows_per_tile;
        let tile_start = tile_idx * bucket_size + col_offset;
        if col_data_type == CasacoreDataType::TpBool {
            if encoded.len() != cell_nelem {
                return Ok(None);
            }
            let tile_end = tile_start + tile_storage_len;
            if tile_end > tsm_data.len() {
                return Ok(None);
            }
            write_bool_bits_from_bytes(
                &mut tsm_data[tile_start..tile_end],
                row_in_tile * cell_nelem,
                &encoded,
            );
        } else {
            if encoded.len() != row_bytes {
                return Ok(None);
            }
            let dst_start = tile_start + row_in_tile * row_bytes;
            let dst_end = dst_start + row_bytes;
            if dst_end > tsm_data.len() {
                return Ok(None);
            }
            tsm_data[dst_start..dst_end].copy_from_slice(&encoded);
        }
    }
    Ok(Some(tsm_data))
}

fn save_single_column_tiled_column_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::column dm_seq={} cols=1 rows={}",
        dm_seq_nr,
        values.len()
    ));
    let nrrow = values.len();
    if nrrow == 0 {
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types: vec![],
            hypercolumn_name: dm_name.to_string(),
            max_cache_size: 0,
            nrdim: 0,
            files: vec![],
            cubes: vec![],
        };
        let variant = TiledVariant::Column {
            default_tile_shape: vec![],
        };
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        return write_tiled_header(&header_path, &variant, &header);
    }

    let cell_shape: Vec<usize> = col_desc.shape.iter().map(|&s| s as usize).collect();
    let cell_ndim = cell_shape.len();

    let mut cube_shape = cell_shape.clone();
    cube_shape.push(nrrow);

    let mut tile_shape = if let Some(ts) = user_tile_shape {
        ts.to_vec()
    } else {
        default_tile_shape_for(&cell_shape, nrrow)
    };
    if tile_shape.len() == cell_ndim {
        let default_row_tile = nrrow.clamp(1, 32);
        tile_shape.push(default_row_tile);
    }
    clamp_tile_shape_dims(&mut tile_shape);
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "layout",
            Some(format!(
                "cube_shape={} tile_shape={}",
                format_shape(&cube_shape),
                format_shape(&tile_shape)
            )),
        );
    }

    let col_data_type =
        CasacoreDataType::from_primitive_type(col_desc.require_primitive_type()?, false);
    let elem_size = tile_element_size(col_data_type);
    let cell_nelem: usize = cell_shape.iter().product();

    let (bucket_size, col_offsets) =
        compute_tile_layout(std::slice::from_ref(&col_data_type), &tile_shape);
    let nrdim = (cell_ndim + 1) as u32;
    let tiles_per_dim: Vec<usize> = (0..nrdim as usize)
        .map(|d| cube_shape[d].div_ceil(tile_shape[d]))
        .collect();
    let nr_tiles: usize = tiles_per_dim.iter().product();
    let nrpixels: usize = tile_shape.iter().product();

    let cube_bytes =
        encode_column_cube_values(values, &cube_shape, cell_nelem, elem_size, big_endian)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "encode_cube_data",
            Some(format!(
                "tiles={} bucket_size={} nrpixels={}",
                nr_tiles, bucket_size, nrpixels
            )),
        );
    }

    let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
    for tile_idx in 0..nr_tiles {
        let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
        let cube_start: Vec<usize> = (0..nrdim as usize)
            .map(|d| tile_pos[d] * tile_shape[d])
            .collect();
        let actual_extent: Vec<usize> = (0..nrdim as usize)
            .map(|d| std::cmp::min(tile_shape[d], cube_shape[d] - cube_start[d]))
            .collect();

        let mut tile_col = vec![0u8; nrpixels * elem_size];
        copy_cube_to_tile(
            &cube_bytes,
            &cube_shape,
            &mut tile_col,
            &tile_shape,
            &cube_start,
            &actual_extent,
            elem_size,
        );

        let dst_start = tile_idx * bucket_size + col_offsets[0];
        write_tile_storage(
            &mut tsm_data[dst_start..dst_start + tile_storage_bytes(col_data_type, nrpixels)],
            col_data_type,
            &tile_col,
            nrpixels,
        );
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("assemble_tiles");
    }

    let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
    std::fs::write(&tsm_path, &tsm_data)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_tsm_file");
    }

    let default_ts_i32: Vec<i32> = tile_shape.iter().map(|&v| v as i32).collect();
    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types: vec![col_data_type],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: vec![Some(TsmFileInfo {
            seq_nr: 0,
            length: tsm_data.len() as i64,
        })],
        cubes: vec![TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape,
            file_seq_nr: 0,
            file_offset: 0,
        }],
    };
    let variant = TiledVariant::Column {
        default_tile_shape: default_ts_i32,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_header");
    }

    Ok(())
}

fn save_single_column_tiled_shape_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_desc: &ColumnDescContents,
    values: &[Option<&Value>],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::shape dm_seq={} cols=1 rows={}",
        dm_seq_nr,
        values.len()
    ));
    let nrrow = values.len();
    let col_data_type =
        CasacoreDataType::from_primitive_type(col_desc.require_primitive_type()?, false);

    if nrrow == 0 {
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types: vec![col_data_type],
            hypercolumn_name: dm_name.to_string(),
            max_cache_size: 0,
            nrdim: (col_desc.nrdim + 1) as u32,
            files: vec![],
            cubes: vec![TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![],
                tile_shape: vec![],
                file_seq_nr: -1,
                file_offset: 0,
            }],
        };
        let variant = TiledVariant::Shape {
            default_tile_shape: vec![],
            nr_used_row_map: 0,
            row_map: vec![],
            cube_map: vec![],
            pos_map: vec![],
        };
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        return write_tiled_header(&header_path, &variant, &header);
    }

    let mut shape_groups: Vec<(Vec<usize>, Vec<usize>)> = Vec::new();
    for (row_idx, value) in values.iter().enumerate() {
        let Some(value) = *value else {
            continue;
        };
        let shape = if let Value::Array(av) = value {
            array_shape(av)
        } else {
            vec![]
        };
        if let Some(group) = shape_groups
            .iter_mut()
            .find(|(candidate, _)| *candidate == shape)
        {
            group.1.push(row_idx);
        } else {
            shape_groups.push((shape, vec![row_idx]));
        }
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "group_shapes",
            Some(format!("shape_groups={}", shape_groups.len())),
        );
    }

    let mut cubes = vec![TsmCubeInfo {
        values: RecordValue::default(),
        extensible: false,
        cube_shape: vec![],
        tile_shape: vec![],
        file_seq_nr: -1,
        file_offset: 0,
    }];
    let mut all_files = Vec::new();
    let mut row_map_vec = Vec::new();
    let mut cube_map_vec = Vec::new();
    let mut pos_map_vec = Vec::new();

    let nrdim = if col_desc.nrdim > 0 {
        (col_desc.nrdim + 1) as u32
    } else {
        2
    };

    for (group_idx, (cell_shape, group_rows)) in shape_groups.iter().enumerate() {
        let cube_idx = group_idx + 1;
        let n_in_cube = group_rows.len();

        let mut cube_shape = cell_shape.clone();
        cube_shape.push(n_in_cube);

        let mut tile_shape = if let Some(ts) = user_tile_shape {
            ts.to_vec()
        } else {
            default_tile_shape_for(cell_shape, n_in_cube)
        };
        if tile_shape.len() == cell_shape.len() {
            let default_row_tile = n_in_cube.clamp(1, 32);
            tile_shape.push(default_row_tile);
        }
        clamp_tile_shape_dims(&mut tile_shape);

        let file_seq_nr = all_files.len() as u32;
        let (bucket_size, col_offsets) =
            compute_tile_layout(std::slice::from_ref(&col_data_type), &tile_shape);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(tile_shape.iter())
            .map(|(&cs, &ts)| cs.div_ceil(ts))
            .collect();
        let nr_tiles: usize = tiles_per_dim.iter().product();
        let nrpixels: usize = tile_shape.iter().product();
        let cell_nelem: usize = cell_shape.iter().product();
        let elem_size = tile_element_size(col_data_type);

        let group_values: Vec<Option<&Value>> =
            group_rows.iter().map(|&row_idx| values[row_idx]).collect();
        let tsm_data = if let Some(tsm_data) = encode_single_column_shape_cube_direct(
            &group_values,
            cell_shape,
            &tile_shape,
            cell_nelem,
            elem_size,
            nr_tiles,
            bucket_size,
            col_offsets[0],
            col_data_type,
            big_endian,
        )? {
            tsm_data
        } else {
            let cube_bytes = encode_column_cube_values(
                &group_values,
                &cube_shape,
                cell_nelem,
                elem_size,
                big_endian,
            )?;

            let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
            for tile_idx in 0..nr_tiles {
                let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
                let cube_start: Vec<usize> = cube_shape
                    .iter()
                    .zip(tile_pos.iter())
                    .zip(tile_shape.iter())
                    .map(|((_, &tp), &ts)| tp * ts)
                    .collect();
                let actual_extent: Vec<usize> = cube_shape
                    .iter()
                    .zip(cube_start.iter())
                    .zip(tile_shape.iter())
                    .map(|((&cs, &st), &ts)| std::cmp::min(ts, cs - st))
                    .collect();

                let mut tile_col = vec![0u8; nrpixels * elem_size];
                copy_cube_to_tile(
                    &cube_bytes,
                    &cube_shape,
                    &mut tile_col,
                    &tile_shape,
                    &cube_start,
                    &actual_extent,
                    elem_size,
                );
                let dst_start = tile_idx * bucket_size + col_offsets[0];
                write_tile_storage(
                    &mut tsm_data
                        [dst_start..dst_start + tile_storage_bytes(col_data_type, nrpixels)],
                    col_data_type,
                    &tile_col,
                    nrpixels,
                );
            }
            tsm_data
        };

        let tsm_path = tsm_data_path(table_path, dm_seq_nr, file_seq_nr);
        std::fs::write(&tsm_path, &tsm_data)?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "write_cube",
                Some(format!(
                    "cube={} shape={} rows={} tile_shape={} bytes={}",
                    cube_idx,
                    format_shape(&cube_shape),
                    n_in_cube,
                    format_shape(&tile_shape),
                    tsm_data.len()
                )),
            );
        }

        all_files.push(Some(TsmFileInfo {
            seq_nr: file_seq_nr,
            length: tsm_data.len() as i64,
        }));

        cubes.push(TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape,
            file_seq_nr: file_seq_nr as i32,
            file_offset: 0,
        });

        for (pos_in_cube, &row_idx) in group_rows.iter().enumerate() {
            row_map_vec.push(row_idx as u32);
            cube_map_vec.push(cube_idx as u32);
            pos_map_vec.push(pos_in_cube as u32);
        }
    }

    {
        let mut indices: Vec<usize> = (0..row_map_vec.len()).collect();
        indices.sort_by_key(|&idx| row_map_vec[idx]);
        let sorted_row: Vec<u32> = indices.iter().map(|&idx| row_map_vec[idx]).collect();
        let sorted_cube: Vec<u32> = indices.iter().map(|&idx| cube_map_vec[idx]).collect();
        let sorted_pos: Vec<u32> = indices.iter().map(|&idx| pos_map_vec[idx]).collect();
        row_map_vec = sorted_row;
        cube_map_vec = sorted_cube;
        pos_map_vec = sorted_pos;
    }

    {
        let mut merged_row = Vec::new();
        let mut merged_cube = Vec::new();
        let mut merged_pos = Vec::new();
        for idx in 0..row_map_vec.len() {
            if !merged_row.is_empty() {
                let last = merged_row.len() - 1;
                if merged_cube[last] == cube_map_vec[idx]
                    && merged_row[last] + 1 == row_map_vec[idx]
                    && merged_pos[last] + 1 == pos_map_vec[idx]
                {
                    merged_row[last] = row_map_vec[idx];
                    merged_pos[last] = pos_map_vec[idx];
                    continue;
                }
            }
            merged_row.push(row_map_vec[idx]);
            merged_cube.push(cube_map_vec[idx]);
            merged_pos.push(pos_map_vec[idx]);
        }
        row_map_vec = merged_row;
        cube_map_vec = merged_cube;
        pos_map_vec = merged_pos;
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail("row_map", Some(format!("intervals={}", row_map_vec.len())));
    }

    let default_ts_i32: Vec<i32> = if let Some(ts) = user_tile_shape {
        ts.iter().map(|&v| v as i32).collect()
    } else if let Some((first_shape, _)) = shape_groups.first() {
        default_tile_shape_for(first_shape, nrrow)
            .iter()
            .map(|&v| v as i32)
            .collect()
    } else {
        vec![]
    };

    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types: vec![col_data_type],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: all_files,
        cubes,
    };
    let variant = TiledVariant::Shape {
        default_tile_shape: default_ts_i32,
        nr_used_row_map: row_map_vec.len() as u32,
        row_map: row_map_vec,
        cube_map: cube_map_vec,
        pos_map: pos_map_vec,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_header");
    }

    Ok(())
}

/// Compute a reasonable default tile shape for a given cell shape and row count.
fn default_tile_shape_for(cell_shape: &[usize], nrow: usize) -> Vec<usize> {
    // Use the full cell shape with a row tile size that keeps tiles ~32KB.
    let cell_nelem: usize = cell_shape.iter().product();
    let target_elements: usize = 8192; // ~32KB for 4-byte elements
    let row_tile = target_elements
        .checked_div(cell_nelem)
        .unwrap_or(nrow)
        .max(1);
    let mut shape: Vec<usize> = cell_shape.iter().map(|&dim| dim.max(1)).collect();
    shape.push(row_tile.min(nrow).max(1));
    shape
}

/// Save with `TiledColumnStMan` (single hypercube, all rows same shape).
fn save_tiled_column_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::column dm_seq={} cols={} rows={}",
        dm_seq_nr,
        col_descs.len(),
        rows.len()
    ));
    let nrrow = rows.len();
    if nrrow == 0 || col_descs.is_empty() {
        // Write empty header.
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types: vec![],
            hypercolumn_name: dm_name.to_string(),
            max_cache_size: 0,
            nrdim: 0,
            files: vec![],
            cubes: vec![],
        };
        let variant = TiledVariant::Column {
            default_tile_shape: vec![],
        };
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        return write_tiled_header(&header_path, &variant, &header);
    }

    // Determine cell shape from the first column's schema.
    let cell_shape: Vec<usize> = col_descs[0].shape.iter().map(|&s| s as usize).collect();
    let cell_ndim = cell_shape.len();

    // Cube shape = cell_shape + [nrow].
    let mut cube_shape = cell_shape.clone();
    cube_shape.push(nrrow);

    let mut tile_shape: Vec<usize> = if let Some(ts) = user_tile_shape {
        ts.to_vec()
    } else {
        default_tile_shape_for(&cell_shape, nrrow)
    };

    // TiledColumnStMan tile shape must include the row dimension. If the user
    // supplied only cell dimensions, pad with a default row-tile size.
    if tile_shape.len() == cell_ndim {
        let default_row_tile = nrrow.clamp(1, 32);
        tile_shape.push(default_row_tile);
    }
    clamp_tile_shape_dims(&mut tile_shape);
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "layout",
            Some(format!(
                "cube_shape={} tile_shape={}",
                format_shape(&cube_shape),
                format_shape(&tile_shape)
            )),
        );
    }

    // Collect column data types.
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| {
            Ok(CasacoreDataType::from_primitive_type(
                c.require_primitive_type()?,
                false,
            ))
        })
        .collect::<Result<_, StorageError>>()?;

    let (bucket_size, col_offsets) = compute_tile_layout(&col_data_types, &tile_shape);

    // Build per-column cube data (all rows concatenated).
    let nrdim = (cell_ndim + 1) as u32;
    let tiles_per_dim: Vec<usize> = (0..nrdim as usize)
        .map(|d| cube_shape[d].div_ceil(tile_shape[d]))
        .collect();
    let nr_tiles: usize = tiles_per_dim.iter().product();
    let nrpixels: usize = tile_shape.iter().product();

    // Encode each column's full cube data (column-major, all rows).
    let mut col_cube_data: Vec<Vec<u8>> = Vec::with_capacity(col_descs.len());
    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        let dt = col_data_types[col_idx];
        let elem_size = tile_element_size(dt);
        let cube_nelem: usize = cube_shape.iter().product();
        let mut cube_bytes = vec![0u8; cube_nelem * elem_size];
        let cell_nelem: usize = cell_shape.iter().product();

        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(value) = row.get(&col_desc.col_name) {
                let (encoded, _) = encode_array_value(value, big_endian)?;
                let start = row_idx * cell_nelem * elem_size;
                let end = start + cell_nelem * elem_size;
                if end <= cube_bytes.len() && encoded.len() == cell_nelem * elem_size {
                    cube_bytes[start..end].copy_from_slice(&encoded);
                }
            }
        }
        col_cube_data.push(cube_bytes);
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "encode_cube_data",
            Some(format!(
                "tiles={} bucket_size={} nrpixels={}",
                nr_tiles, bucket_size, nrpixels
            )),
        );
    }

    // Build tile data for the TSM file.
    let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
    for tile_idx in 0..nr_tiles {
        let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
        let cube_start: Vec<usize> = (0..nrdim as usize)
            .map(|d| tile_pos[d] * tile_shape[d])
            .collect();
        let actual_extent: Vec<usize> = (0..nrdim as usize)
            .map(|d| std::cmp::min(tile_shape[d], cube_shape[d] - cube_start[d]))
            .collect();

        for (col_idx, _) in col_descs.iter().enumerate() {
            let dt = col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            let col_tile_bytes = tile_storage_bytes(dt, nrpixels);

            // Extract tile data from cube (reverse of copy_tile_to_cube).
            let mut tile_col = vec![0u8; nrpixels * elem_size];
            copy_cube_to_tile(
                &col_cube_data[col_idx],
                &cube_shape,
                &mut tile_col,
                &tile_shape,
                &cube_start,
                &actual_extent,
                elem_size,
            );

            let dst_start = tile_idx * bucket_size + col_offsets[col_idx];
            write_tile_storage(
                &mut tsm_data[dst_start..dst_start + col_tile_bytes],
                dt,
                &tile_col,
                nrpixels,
            );
        }
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("assemble_tiles");
    }

    // Write TSM data file.
    let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
    std::fs::write(&tsm_path, &tsm_data)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_tsm_file");
    }

    // Build and write header.
    let default_ts_i32: Vec<i32> = tile_shape.iter().map(|&v| v as i32).collect();
    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types,
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: vec![Some(TsmFileInfo {
            seq_nr: 0,
            length: tsm_data.len() as i64,
        })],
        cubes: vec![TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape,
            file_seq_nr: 0,
            file_offset: 0,
        }],
    };
    let variant = TiledVariant::Column {
        default_tile_shape: default_ts_i32,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_header");
    }

    Ok(())
}

/// Save with `TiledShapeStMan` (one hypercube per unique array shape).
fn save_tiled_shape_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let mut profiler = StorageProfiler::start(format!(
        "tiled::shape dm_seq={} cols={} rows={}",
        dm_seq_nr,
        col_descs.len(),
        rows.len()
    ));
    let nrrow = rows.len();
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| {
            Ok(CasacoreDataType::from_primitive_type(
                c.require_primitive_type()?,
                false,
            ))
        })
        .collect::<Result<_, StorageError>>()?;

    if nrrow == 0 || col_descs.is_empty() {
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types: col_data_types.clone(),
            hypercolumn_name: dm_name.to_string(),
            max_cache_size: 0,
            nrdim: if col_descs.is_empty() {
                0
            } else {
                (col_descs[0].nrdim + 1) as u32
            },
            files: vec![],
            cubes: vec![TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![],
                tile_shape: vec![],
                file_seq_nr: -1,
                file_offset: 0,
            }],
        };
        let variant = TiledVariant::Shape {
            default_tile_shape: vec![],
            nr_used_row_map: 0,
            row_map: vec![],
            cube_map: vec![],
            pos_map: vec![],
        };
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        return write_tiled_header(&header_path, &variant, &header);
    }

    // Group rows by array shape (using first column to determine shape).
    let first_col = &col_descs[0].col_name;
    let mut shape_groups: Vec<(Vec<usize>, Vec<usize>)> = Vec::new(); // (shape, row_indices)
    for (row_idx, row) in rows.iter().enumerate() {
        let shape = if let Some(Value::Array(av)) = row.get(first_col) {
            array_shape(av)
        } else {
            vec![]
        };
        if let Some(group) = shape_groups.iter_mut().find(|(s, _)| *s == shape) {
            group.1.push(row_idx);
        } else {
            shape_groups.push((shape, vec![row_idx]));
        }
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail(
            "group_shapes",
            Some(format!("shape_groups={}", shape_groups.len())),
        );
    }

    // Build cubes: cube 0 is dummy, cubes 1..N are real.
    let mut cubes = vec![TsmCubeInfo {
        values: RecordValue::default(),
        extensible: false,
        cube_shape: vec![],
        tile_shape: vec![],
        file_seq_nr: -1,
        file_offset: 0,
    }];
    let mut all_files = Vec::new();
    let mut row_map_vec = Vec::new();
    let mut cube_map_vec = Vec::new();
    let mut pos_map_vec = Vec::new();

    let nrdim = if col_descs[0].nrdim > 0 {
        (col_descs[0].nrdim + 1) as u32
    } else {
        2 // at least 2 for shape + row dim
    };

    for (group_idx, (cell_shape, group_rows)) in shape_groups.iter().enumerate() {
        let cube_idx = group_idx + 1; // cube 0 is dummy
        let n_in_cube = group_rows.len();

        let mut cube_shape = cell_shape.clone();
        cube_shape.push(n_in_cube);

        let mut tile_shape = if let Some(ts) = user_tile_shape {
            ts.to_vec()
        } else {
            default_tile_shape_for(cell_shape, n_in_cube)
        };
        if tile_shape.len() == cell_shape.len() {
            let default_row_tile = n_in_cube.clamp(1, 32);
            tile_shape.push(default_row_tile);
        }
        clamp_tile_shape_dims(&mut tile_shape);

        let file_seq_nr = all_files.len() as u32;

        // Build tile data for this cube.
        let (bucket_size, col_offsets) = compute_tile_layout(&col_data_types, &tile_shape);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(tile_shape.iter())
            .map(|(&cs, &ts)| cs.div_ceil(ts))
            .collect();
        let nr_tiles: usize = tiles_per_dim.iter().product();
        let nrpixels: usize = tile_shape.iter().product();

        // Encode column data for this cube.
        let cell_nelem: usize = cell_shape.iter().product();
        let mut col_cube_data: Vec<Vec<u8>> = Vec::with_capacity(col_descs.len());
        for (col_idx, col_desc) in col_descs.iter().enumerate() {
            let dt = col_data_types[col_idx];
            let elem_size = tile_element_size(dt);
            let cube_nelem: usize = cube_shape.iter().product();
            let mut cube_bytes = vec![0u8; cube_nelem * elem_size];

            for (pos, &row_idx) in group_rows.iter().enumerate() {
                if let Some(value) = rows[row_idx].get(&col_desc.col_name) {
                    let (encoded, _) = encode_array_value(value, big_endian)?;
                    let start = pos * cell_nelem * elem_size;
                    let end = start + cell_nelem * elem_size;
                    if end <= cube_bytes.len() && encoded.len() == cell_nelem * elem_size {
                        cube_bytes[start..end].copy_from_slice(&encoded);
                    }
                }
            }
            col_cube_data.push(cube_bytes);
        }

        let mut tsm_data = vec![0u8; nr_tiles * bucket_size];
        for tile_idx in 0..nr_tiles {
            let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
            let cube_start: Vec<usize> = cube_shape
                .iter()
                .zip(tile_pos.iter())
                .zip(tile_shape.iter())
                .map(|((_, &tp), &ts)| tp * ts)
                .collect();
            let actual_extent: Vec<usize> = cube_shape
                .iter()
                .zip(cube_start.iter())
                .zip(tile_shape.iter())
                .map(|((&cs, &st), &ts)| std::cmp::min(ts, cs - st))
                .collect();

            for (col_idx, _) in col_descs.iter().enumerate() {
                let dt = col_data_types[col_idx];
                let elem_size = tile_element_size(dt);
                let col_tile_bytes = tile_storage_bytes(dt, nrpixels);
                let mut tile_col = vec![0u8; nrpixels * elem_size];
                copy_cube_to_tile(
                    &col_cube_data[col_idx],
                    &cube_shape,
                    &mut tile_col,
                    &tile_shape,
                    &cube_start,
                    &actual_extent,
                    elem_size,
                );
                let dst_start = tile_idx * bucket_size + col_offsets[col_idx];
                write_tile_storage(
                    &mut tsm_data[dst_start..dst_start + col_tile_bytes],
                    dt,
                    &tile_col,
                    nrpixels,
                );
            }
        }

        let tsm_path = tsm_data_path(table_path, dm_seq_nr, file_seq_nr);
        std::fs::write(&tsm_path, &tsm_data)?;
        if let Some(profiler) = profiler.as_mut() {
            profiler.mark_with_detail(
                "write_cube",
                Some(format!(
                    "cube={} shape={} rows={} tile_shape={} bytes={}",
                    cube_idx,
                    format_shape(&cube_shape),
                    n_in_cube,
                    format_shape(&tile_shape),
                    tsm_data.len()
                )),
            );
        }

        all_files.push(Some(TsmFileInfo {
            seq_nr: file_seq_nr,
            length: tsm_data.len() as i64,
        }));

        cubes.push(TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape,
            file_seq_nr: file_seq_nr as i32,
            file_offset: 0,
        });

        // Build row map entries — one per row for now; merged and sorted below.
        for (pos_in_cube, &row_idx) in group_rows.iter().enumerate() {
            row_map_vec.push(row_idx as u32);
            cube_map_vec.push(cube_idx as u32);
            pos_map_vec.push(pos_in_cube as u32);
        }
    }

    // Sort entries by row number (C++ binary search requires sorted order).
    {
        let mut indices: Vec<usize> = (0..row_map_vec.len()).collect();
        indices.sort_by_key(|&i| row_map_vec[i]);
        let sorted_row: Vec<u32> = indices.iter().map(|&i| row_map_vec[i]).collect();
        let sorted_cube: Vec<u32> = indices.iter().map(|&i| cube_map_vec[i]).collect();
        let sorted_pos: Vec<u32> = indices.iter().map(|&i| pos_map_vec[i]).collect();
        row_map_vec = sorted_row;
        cube_map_vec = sorted_cube;
        pos_map_vec = sorted_pos;
    }

    // Merge adjacent intervals: same cube, contiguous rows and positions.
    // C++ format: rowMap[i] = last row, posMap[i] = last position in cube.
    {
        let mut merged_row = Vec::new();
        let mut merged_cube = Vec::new();
        let mut merged_pos = Vec::new();
        for i in 0..row_map_vec.len() {
            if !merged_row.is_empty() {
                let last = merged_row.len() - 1;
                if merged_cube[last] == cube_map_vec[i]
                    && merged_row[last] + 1 == row_map_vec[i]
                    && merged_pos[last] + 1 == pos_map_vec[i]
                {
                    // Extend: update to last row/pos of interval.
                    merged_row[last] = row_map_vec[i];
                    merged_pos[last] = pos_map_vec[i];
                    continue;
                }
            }
            merged_row.push(row_map_vec[i]);
            merged_cube.push(cube_map_vec[i]);
            merged_pos.push(pos_map_vec[i]);
        }
        row_map_vec = merged_row;
        cube_map_vec = merged_cube;
        pos_map_vec = merged_pos;
    }
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark_with_detail("row_map", Some(format!("intervals={}", row_map_vec.len())));
    }

    let default_ts_i32: Vec<i32> = if let Some(ts) = user_tile_shape {
        ts.iter().map(|&v| v as i32).collect()
    } else if let Some((first_shape, _)) = shape_groups.first() {
        default_tile_shape_for(first_shape, nrrow)
            .iter()
            .map(|&v| v as i32)
            .collect()
    } else {
        vec![]
    };

    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types,
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: all_files,
        cubes,
    };
    let variant = TiledVariant::Shape {
        default_tile_shape: default_ts_i32,
        nr_used_row_map: row_map_vec.len() as u32,
        row_map: row_map_vec,
        cube_map: cube_map_vec,
        pos_map: pos_map_vec,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    if let Some(profiler) = profiler.as_mut() {
        profiler.mark("write_header");
    }

    Ok(())
}

/// Save with `TiledCellStMan` (one hypercube per row).
fn save_tiled_cell_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    user_tile_shape: Option<&[usize]>,
    dm_name: &str,
) -> Result<(), StorageError> {
    let nrrow = rows.len();
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| {
            Ok(CasacoreDataType::from_primitive_type(
                c.require_primitive_type()?,
                false,
            ))
        })
        .collect::<Result<_, StorageError>>()?;

    let nrdim = if !col_descs.is_empty() && col_descs[0].nrdim > 0 {
        col_descs[0].nrdim as u32
    } else {
        1
    };

    let mut cubes = Vec::with_capacity(nrrow);
    let mut all_files: Vec<Option<TsmFileInfo>> = vec![Some(TsmFileInfo {
        seq_nr: 0,
        length: 0,
    })];
    let mut tsm_data = Vec::new(); // All non-extensible cubes share file 0.

    for row in rows.iter() {
        // Determine cell shape from the first column.
        let first_col = &col_descs[0].col_name;
        let cell_shape = if let Some(Value::Array(av)) = row.get(first_col) {
            array_shape(av)
        } else {
            cubes.push(TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![],
                tile_shape: vec![],
                file_seq_nr: -1,
                file_offset: 0,
            });
            continue;
        };

        if cell_shape.is_empty() {
            cubes.push(TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![],
                tile_shape: vec![],
                file_seq_nr: -1,
                file_offset: 0,
            });
            continue;
        }

        let cube_shape = cell_shape.clone();
        let mut tile_shape = if let Some(ts) = user_tile_shape {
            // Use only the cell dimensions (no row dim for TiledCellStMan).
            ts[..cell_shape.len().min(ts.len())].to_vec()
        } else {
            // Use full cell shape as tile shape.
            cell_shape.clone()
        };
        clamp_tile_shape_dims(&mut tile_shape);

        let (bucket_size, col_offsets) = compute_tile_layout(&col_data_types, &tile_shape);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(tile_shape.iter())
            .map(|(&cs, &ts)| cs.div_ceil(ts))
            .collect();
        let nr_tiles: usize = tiles_per_dim.iter().product();
        let nrpixels: usize = tile_shape.iter().product();
        let cell_nelem: usize = cell_shape.iter().product();

        let file_offset = tsm_data.len() as i64;
        let mut tile_data = vec![0u8; nr_tiles * bucket_size];

        // Pre-encode each column's data once (outside the tile loop).
        let encoded_cols: Vec<Vec<u8>> = col_descs
            .iter()
            .enumerate()
            .map(|(col_idx, col_desc)| {
                let dt = col_data_types[col_idx];
                let elem_size = tile_element_size(dt);
                let mut cube_bytes = vec![0u8; cell_nelem * elem_size];
                if let Some(value) = row.get(&col_desc.col_name) {
                    if let Ok((encoded, _)) = encode_array_value(value, big_endian) {
                        if encoded.len() == cube_bytes.len() {
                            cube_bytes = encoded;
                        }
                    }
                }
                cube_bytes
            })
            .collect();

        for tile_idx in 0..nr_tiles {
            let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
            let cube_start: Vec<usize> = cube_shape
                .iter()
                .zip(tile_pos.iter())
                .zip(tile_shape.iter())
                .map(|((_, &tp), &ts)| tp * ts)
                .collect();
            let actual_extent: Vec<usize> = cube_shape
                .iter()
                .zip(cube_start.iter())
                .zip(tile_shape.iter())
                .map(|((&cs, &st), &ts)| std::cmp::min(ts, cs - st))
                .collect();

            for (col_idx, _col_desc) in col_descs.iter().enumerate() {
                let dt = col_data_types[col_idx];
                let elem_size = tile_element_size(dt);
                let col_tile_bytes = tile_storage_bytes(dt, nrpixels);

                let mut tile_col = vec![0u8; nrpixels * elem_size];
                copy_cube_to_tile(
                    &encoded_cols[col_idx],
                    &cube_shape,
                    &mut tile_col,
                    &tile_shape,
                    &cube_start,
                    &actual_extent,
                    elem_size,
                );
                let dst_start = tile_idx * bucket_size + col_offsets[col_idx];
                write_tile_storage(
                    &mut tile_data[dst_start..dst_start + col_tile_bytes],
                    dt,
                    &tile_col,
                    nrpixels,
                );
            }
        }

        tsm_data.extend_from_slice(&tile_data);

        cubes.push(TsmCubeInfo {
            values: RecordValue::default(),
            extensible: false,
            cube_shape,
            tile_shape,
            file_seq_nr: 0,
            file_offset,
        });
    }

    // Write TSM data file.
    let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
    std::fs::write(&tsm_path, &tsm_data)?;
    if let Some(ref mut f) = all_files[0] {
        f.length = tsm_data.len() as i64;
    }

    let default_ts_i32: Vec<i32> = if let Some(ts) = user_tile_shape {
        ts.iter().map(|&v| v as i32).collect()
    } else {
        vec![]
    };

    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types,
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim,
        files: all_files,
        cubes,
    };
    let variant = TiledVariant::Cell {
        default_tile_shape: default_ts_i32,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;

    Ok(())
}

/// Copy data from a cube array into a tile (reverse of `copy_tile_to_cube`).
fn copy_cube_to_tile(
    cube_data: &[u8],
    cube_shape: &[usize],
    tile_data: &mut [u8],
    tile_shape: &[usize],
    cube_start: &[usize],
    actual_extent: &[usize],
    elem_size: usize,
) {
    let ndim = tile_shape.len();
    if ndim == 0 {
        return;
    }

    let inner_bytes = actual_extent[0] * elem_size;

    if ndim == 1 {
        let src_off = cube_start[0] * elem_size;
        let dst_off = 0;
        tile_data[dst_off..dst_off + inner_bytes]
            .copy_from_slice(&cube_data[src_off..src_off + inner_bytes]);
        return;
    }

    let outer_dims: Vec<usize> = actual_extent[1..].to_vec();
    let outer_total: usize = outer_dims.iter().product();

    for outer_linear in 0..outer_total {
        let outer_pos = linear_to_nd(outer_linear, &outer_dims);

        let mut tile_off = 0;
        let mut stride = tile_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            tile_off += p * stride;
            stride *= tile_shape[d + 1];
        }

        let mut cube_off = cube_start[0];
        stride = cube_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            cube_off += (cube_start[d + 1] + p) * stride;
            stride *= cube_shape[d + 1];
        }

        let src_start = cube_off * elem_size;
        let dst_start = tile_off * elem_size;
        tile_data[dst_start..dst_start + inner_bytes]
            .copy_from_slice(&cube_data[src_start..src_start + inner_bytes]);
    }
}

/// Get the shape of an ArrayValue as a Vec<usize>.
fn array_shape(av: &ArrayValue) -> Vec<usize> {
    match av {
        ArrayValue::Bool(a) => a.shape().to_vec(),
        ArrayValue::UInt8(a) => a.shape().to_vec(),
        ArrayValue::Int16(a) => a.shape().to_vec(),
        ArrayValue::UInt16(a) => a.shape().to_vec(),
        ArrayValue::Int32(a) => a.shape().to_vec(),
        ArrayValue::UInt32(a) => a.shape().to_vec(),
        ArrayValue::Float32(a) => a.shape().to_vec(),
        ArrayValue::Float64(a) => a.shape().to_vec(),
        ArrayValue::Int64(a) => a.shape().to_vec(),
        ArrayValue::Complex32(a) => a.shape().to_vec(),
        ArrayValue::Complex64(a) => a.shape().to_vec(),
        ArrayValue::String(a) => a.shape().to_vec(),
    }
}

// ---------------------------------------------------------------------------
// TiledFileIO — random-access tile I/O for on-disk TiledCellStMan
// ---------------------------------------------------------------------------

/// Flat tile cache: one contiguous byte buffer for all tiles.
/// Allocated lazily on first access, either zeroed (fresh image)
/// or filled from disk (open). Single allocation eliminates
/// per-tile malloc overhead.
struct FlatTileCache {
    /// Contiguous byte buffer: `data[i * tile_bytes .. (i+1) * tile_bytes]`.
    data: Vec<u8>,
    /// Per-tile dirty flag.
    dirty: Vec<bool>,
    tile_bytes: usize,
    nr_tiles: usize,
    allocated: bool,
}

/// LRU tile cache with a bounded number of slots.
///
/// Tiles are loaded on demand and evicted (flushing dirty tiles to disk)
/// when the cache is full. This is used when `max_cache_bytes` is set and
/// smaller than the total tile data, forcing real disk I/O.
struct LruTileCache {
    /// Fixed-size byte buffer: `max_slots × tile_bytes`.
    data: Vec<u8>,
    /// For each slot: the tile index it holds (`usize::MAX` = empty).
    slot_tile: Vec<usize>,
    /// Dirty flag per slot.
    slot_dirty: Vec<bool>,
    /// Reverse lookup: tile_index → slot (−1 = not cached).
    /// Sized to `nr_tiles` for O(1) direct-indexed access (like C++ `Block<Int>`).
    tile_to_slot: Vec<i32>,
    /// Access counter per slot for LRU eviction.
    slot_access: Vec<u64>,
    /// Monotonically increasing access counter.
    access_counter: u64,
    /// Maximum number of tile slots.
    max_slots: usize,
    /// Number of currently occupied slots.
    used_slots: usize,
    /// File handle kept open for reads and writes.
    file: std::fs::File,
}

enum TileCache {
    Flat(FlatTileCache),
    Lru(LruTileCache),
}

/// Random-access tile I/O for a `TiledCellStMan` data file.
///
/// Provides direct read/write of individual tiles without loading the entire
/// array into memory. Uses an in-memory tile cache (modeled on C++ casacore's
/// `BucketCache`) to avoid redundant disk I/O. Tiles are byte-swapped on
/// cache load/flush, so the hot path operates on native-endian data.
///
/// # C++ equivalent
///
/// `TSMCube` + `BucketCache` in casacore's tiled storage manager.
pub struct TiledFileIO {
    tsm_path: PathBuf,
    #[allow(dead_code)]
    header_path: PathBuf,
    table_path: PathBuf,
    pixel_type: PrimitiveType,
    cube_shape: Vec<usize>,
    tile_shape: Vec<usize>,
    tiles_per_dim: Vec<usize>,
    nr_tiles: usize,
    elem_size: usize,
    tile_bytes: usize,
    file_tile_bytes: usize,
    tile_nelem: usize,
    big_endian: bool,
    file_offset: usize,
    dm_seq_nr: u32,
    storage_data_type: CasacoreDataType,
    /// Precomputed Fortran-order strides for tile indexing.
    tile_strides: Vec<usize>,
    /// Precomputed Fortran-order strides for tile-grid indexing.
    tiles_per_dim_strides: Vec<usize>,
    /// Tile cache: flat (all-in-memory) or LRU (bounded).
    cache: TileCache,
    /// Whether this platform needs byte-swapping for the on-disk format.
    needs_swap: bool,
    /// Byte-swap component size (4 for f32/Complex32, 8 for f64/Complex64).
    swap_size: usize,
    /// Per-tile flag: `true` when the tile has been written to disk (via
    /// eviction or flush) and must be read back on cache miss. `false` for
    /// tiles that have never been written — these are zero-initialized
    /// without disk I/O.
    tile_on_disk: Vec<bool>,
    /// Persistent read file handle (used by flat cache bulk load).
    read_file: Option<std::fs::File>,
}

#[allow(clippy::too_many_arguments)]
fn read_tiled_file_tile(
    file: &mut std::fs::File,
    file_pos: u64,
    dst: &mut [u8],
    file_tile_bytes: usize,
    dt: CasacoreDataType,
    tile_nelem: usize,
    needs_swap: bool,
    swap_size: usize,
) -> Result<(), StorageError> {
    if file_tile_bytes == dst.len() {
        file.seek(SeekFrom::Start(file_pos))?;
        file.read_exact(dst)?;
        if needs_swap {
            swap_bytes_inplace(dst, swap_size);
        }
        return Ok(());
    }

    let mut packed = vec![0u8; file_tile_bytes];
    file.seek(SeekFrom::Start(file_pos))?;
    file.read_exact(&mut packed)?;
    let unpacked = read_tile_storage(&packed, dt, tile_nelem);
    debug_assert_eq!(unpacked.len(), dst.len());
    dst.copy_from_slice(&unpacked);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_tiled_file_tile(
    file: &mut std::fs::File,
    file_pos: u64,
    src: &[u8],
    file_tile_bytes: usize,
    dt: CasacoreDataType,
    tile_nelem: usize,
    needs_swap: bool,
    swap_size: usize,
) -> Result<(), StorageError> {
    if file_tile_bytes == src.len() {
        if needs_swap {
            let mut buf = src.to_vec();
            swap_bytes_inplace(&mut buf, swap_size);
            file.seek(SeekFrom::Start(file_pos))?;
            file.write_all(&buf)?;
        } else {
            file.seek(SeekFrom::Start(file_pos))?;
            file.write_all(src)?;
        }
        return Ok(());
    }

    let mut packed = vec![0u8; file_tile_bytes];
    write_tile_storage(&mut packed, dt, src, tile_nelem);
    file.seek(SeekFrom::Start(file_pos))?;
    file.write_all(&packed)?;
    Ok(())
}

#[derive(Default)]
struct TileReadSession {
    files: std::collections::HashMap<u32, std::fs::File>,
}

impl TileReadSession {
    fn file(
        &mut self,
        table_path: &Path,
        dm_seq_nr: u32,
        file_seq_nr: u32,
    ) -> Result<&mut std::fs::File, StorageError> {
        match self.files.entry(file_seq_nr) {
            std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let path = tsm_data_path(table_path, dm_seq_nr, file_seq_nr);
                let file = OpenOptions::new().read(true).open(&path).map_err(|err| {
                    StorageError::FormatMismatch(format!("cannot read {}: {err}", path.display()))
                })?;
                Ok(entry.insert(file))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn load_shared_column_tile(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube_idx: usize,
    cube: &TsmCubeInfo,
    target_col_idx: usize,
    bucket_size: usize,
    col_offset_in_tile: usize,
    dt: CasacoreDataType,
    tile_index: usize,
    session: &mut TileReadSession,
) -> Result<Arc<[u8]>, StorageError> {
    let key = SharedTileKey {
        table_path: table_path.to_path_buf(),
        dm_seq_nr,
        cube_idx,
        target_col_idx,
        tile_index,
    };
    if let Some(hit) = SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned")
        .get(&key)
    {
        return Ok(hit);
    }

    let tile_nelem: usize = cube.tile_shape.iter().product();
    let tile_bytes = tile_nelem * tile_element_size(dt);
    let file_tile_bytes = tile_storage_bytes(dt, tile_nelem);
    let swap_size = match dt {
        CasacoreDataType::TpComplex => 4,
        CasacoreDataType::TpDComplex => 8,
        _ => tile_element_size(dt),
    };
    let needs_swap = header.big_endian != cfg!(target_endian = "big");
    let file_pos =
        (cube.file_offset as usize + tile_index * bucket_size + col_offset_in_tile) as u64;
    let file = session.file(table_path, dm_seq_nr, cube.file_seq_nr as u32)?;
    let mut tile = vec![0u8; tile_bytes];
    read_tiled_file_tile(
        file,
        file_pos,
        &mut tile,
        file_tile_bytes,
        dt,
        tile_nelem,
        needs_swap,
        swap_size,
    )?;
    let data: Arc<[u8]> = Arc::from(tile);
    let mut cache = SHARED_TILE_CACHE
        .lock()
        .expect("shared tile cache lock poisoned");
    if let Some(hit) = cache.get(&key) {
        return Ok(hit);
    }
    Ok(cache.insert(key, data))
}

#[allow(clippy::too_many_arguments)]
fn reconstruct_cube_column_from_shared_tiles(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube_idx: usize,
    cube: &TsmCubeInfo,
    target_col_idx: usize,
    dt: CasacoreDataType,
    elem_size: usize,
    session: &mut TileReadSession,
) -> Result<Vec<u8>, StorageError> {
    let (bucket_size, col_offsets) = compute_tile_layout(&header.col_data_types, &cube.tile_shape);
    let ndim = cube.cube_shape.len();
    let tiles_per_dim: Vec<usize> = (0..ndim)
        .map(|i| cube.cube_shape[i].div_ceil(cube.tile_shape[i]))
        .collect();
    let nr_tiles: usize = tiles_per_dim.iter().product();
    let cube_nelem: usize = cube.cube_shape.iter().product();
    let mut result = vec![0u8; cube_nelem * elem_size];

    for tile_idx in 0..nr_tiles {
        let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);
        let cube_start: Vec<usize> = (0..ndim)
            .map(|d| tile_pos[d] * cube.tile_shape[d])
            .collect();
        let actual_extent: Vec<usize> = (0..ndim)
            .map(|d| std::cmp::min(cube.tile_shape[d], cube.cube_shape[d] - cube_start[d]))
            .collect();
        let tile = load_shared_column_tile(
            table_path,
            dm_seq_nr,
            header,
            cube_idx,
            cube,
            target_col_idx,
            bucket_size,
            col_offsets[target_col_idx],
            dt,
            tile_idx,
            session,
        )?;
        copy_tile_to_cube(
            tile.as_ref(),
            &cube.tile_shape,
            &mut result,
            &cube.cube_shape,
            &cube_start,
            &actual_extent,
            elem_size,
        );
    }

    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn decode_selected_cube_row_from_shared_tiles(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube_idx: usize,
    cube: &TsmCubeInfo,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    selected: SelectedCubeRow,
    session: &mut TileReadSession,
) -> Result<Option<ArrayValue>, StorageError> {
    let cell_ndim = cube.cube_shape.len().saturating_sub(1);
    let cell_shape: Vec<usize> = cube.cube_shape[..cell_ndim].to_vec();
    let cell_nelem: usize = cell_shape.iter().product();
    let cell_tile_shape = &cube.tile_shape[..cell_ndim];
    let cell_tiles_per_dim: Vec<usize> = cell_shape
        .iter()
        .zip(cell_tile_shape.iter())
        .map(|(&cell, &tile)| cell.div_ceil(tile))
        .collect();
    let row_tile = selected.pos_in_cube / cube.tile_shape[cell_ndim];
    let row_in_tile = selected.pos_in_cube % cube.tile_shape[cell_ndim];
    let row_tile_nelem: usize = cell_tile_shape.iter().product();
    let tiles_per_dim: Vec<usize> = cube
        .cube_shape
        .iter()
        .zip(cube.tile_shape.iter())
        .map(|(&shape, &tile)| shape.div_ceil(tile))
        .collect();
    let tile_grid_strides = fortran_order_strides(&tiles_per_dim);
    let (bucket_size, col_offsets) = compute_tile_layout(&header.col_data_types, &cube.tile_shape);

    if cell_tile_shape == cell_shape.as_slice() {
        let tile_index = row_tile * tile_grid_strides[cell_ndim];
        let tile = load_shared_column_tile(
            table_path,
            dm_seq_nr,
            header,
            cube_idx,
            cube,
            target_col_idx,
            bucket_size,
            col_offsets[target_col_idx],
            dt,
            tile_index,
            session,
        )?;
        let src_start = row_in_tile * row_tile_nelem * elem_size;
        let src_end = src_start + row_tile_nelem * elem_size;
        let value = decode_array_value(
            &tile[src_start..src_end],
            &cell_shape,
            dt,
            header.big_endian,
        )?;
        return match value {
            Value::Array(array) => Ok(Some(array)),
            other => Err(StorageError::FormatMismatch(format!(
                "tiled array column {} decoded as non-array value {:?}",
                col_desc.col_name,
                other.kind()
            ))),
        };
    }

    let mut raw = vec![0u8; cell_nelem * elem_size];

    for cell_tile_linear in 0..cell_tiles_per_dim.iter().product::<usize>() {
        let cell_tile_pos = linear_to_nd(cell_tile_linear, &cell_tiles_per_dim);
        let mut full_tile_pos = cell_tile_pos.clone();
        full_tile_pos.push(row_tile);
        let tile_index: usize = full_tile_pos
            .iter()
            .zip(tile_grid_strides.iter())
            .map(|(pos, stride)| pos * stride)
            .sum();
        let tile = load_shared_column_tile(
            table_path,
            dm_seq_nr,
            header,
            cube_idx,
            cube,
            target_col_idx,
            bucket_size,
            col_offsets[target_col_idx],
            dt,
            tile_index,
            session,
        )?;
        let src_start = row_in_tile * row_tile_nelem * elem_size;
        let src_end = src_start + row_tile_nelem * elem_size;
        let tile_row = &tile[src_start..src_end];
        let cell_start: Vec<usize> = cell_tile_pos
            .iter()
            .enumerate()
            .map(|(axis, &pos)| pos * cell_tile_shape[axis])
            .collect();
        let actual_extent: Vec<usize> = cell_shape
            .iter()
            .enumerate()
            .map(|(axis, &shape)| std::cmp::min(cell_tile_shape[axis], shape - cell_start[axis]))
            .collect();
        copy_tile_to_cube(
            tile_row,
            cell_tile_shape,
            &mut raw,
            &cell_shape,
            &cell_start,
            &actual_extent,
            elem_size,
        );
    }

    match decode_array_value(&raw, &cell_shape, dt, header.big_endian)? {
        Value::Array(array) => Ok(Some(array)),
        other => Err(StorageError::FormatMismatch(format!(
            "tiled array column {} decoded as non-array value {:?}",
            col_desc.col_name,
            other.kind()
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_tiled_cell_from_shared_tiles(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    cube_idx: usize,
    cube: &TsmCubeInfo,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    session: &mut TileReadSession,
) -> Result<Option<ArrayValue>, StorageError> {
    let raw = reconstruct_cube_column_from_shared_tiles(
        table_path,
        dm_seq_nr,
        header,
        cube_idx,
        cube,
        target_col_idx,
        dt,
        elem_size,
        session,
    )?;
    match decode_array_value(&raw, &cube.cube_shape, dt, header.big_endian)? {
        Value::Array(array) => Ok(Some(array)),
        other => Err(StorageError::FormatMismatch(format!(
            "tiled cell row '{}' expected array value, found {:?}",
            col_desc.col_name,
            other.kind()
        ))),
    }
}

impl TiledFileIO {
    /// Creates a new `TiledFileIO`, writing the TSM header and allocating
    /// a zeroed data file on disk.
    pub fn create(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        pixel_type: PrimitiveType,
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
    ) -> Result<Self, StorageError> {
        Self::create_impl(
            table_path, cube_shape, tile_shape, pixel_type, big_endian, dm_seq_nr, dm_name, 0,
        )
    }

    /// Creates a new `TiledFileIO` with an explicit cache size limit.
    ///
    /// When `max_cache_bytes > 0` and smaller than the total tile data,
    /// an LRU tile cache is used, forcing real disk I/O on eviction/load.
    #[allow(clippy::too_many_arguments)]
    pub fn create_with_cache_limit(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        pixel_type: PrimitiveType,
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
        max_cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        Self::create_impl(
            table_path,
            cube_shape,
            tile_shape,
            pixel_type,
            big_endian,
            dm_seq_nr,
            dm_name,
            max_cache_bytes,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_impl(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        pixel_type: PrimitiveType,
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
        max_cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        let dt = CasacoreDataType::from_primitive_type(pixel_type, false);
        let ndim = cube_shape.len();
        let elem_size = tile_element_size(dt);
        let tile_nelem: usize = tile_shape.iter().product();
        let tile_bytes = tile_nelem * elem_size;
        let file_tile_bytes = tile_storage_bytes(dt, tile_nelem);

        let tiles_per_dim: Vec<usize> = (0..ndim)
            .map(|d| cube_shape[d].div_ceil(tile_shape[d]))
            .collect();
        let nr_tiles: usize = tiles_per_dim.iter().product();
        let total_bytes = nr_tiles * file_tile_bytes;

        // Write zeroed TSM data file.
        let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
        {
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tsm_path)?;
            f.set_len(total_bytes as u64)?;
        }

        // Write AipsIO header file.
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        let tile_shape_i32: Vec<i32> = tile_shape.iter().map(|&v| v as i32).collect();
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 1,
            col_data_types: vec![dt],
            hypercolumn_name: dm_name.to_string(),
            max_cache_size: 0,
            nrdim: ndim as u32,
            files: vec![Some(TsmFileInfo {
                seq_nr: 0,
                length: total_bytes as i64,
            })],
            cubes: vec![TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: cube_shape.to_vec(),
                tile_shape: tile_shape.to_vec(),
                file_seq_nr: 0,
                file_offset: 0,
            }],
        };
        let variant = TiledVariant::Cell {
            default_tile_shape: tile_shape_i32,
        };
        write_tiled_header(&header_path, &variant, &header)?;

        let tile_strides = fortran_order_strides(tile_shape);
        let tiles_per_dim_strides = fortran_order_strides(&tiles_per_dim);
        let needs_swap = big_endian != cfg!(target_endian = "big");

        let total_data_bytes = nr_tiles * file_tile_bytes;
        let use_lru = max_cache_bytes > 0 && max_cache_bytes < total_data_bytes;
        let cache = if use_lru {
            let max_slots = max_cache_bytes / tile_bytes;
            let max_slots = max_slots.max(1);
            let file = OpenOptions::new().read(true).write(true).open(&tsm_path)?;
            TileCache::Lru(LruTileCache {
                data: vec![0u8; max_slots * tile_bytes],
                slot_tile: vec![usize::MAX; max_slots],
                slot_dirty: vec![false; max_slots],
                tile_to_slot: vec![-1i32; nr_tiles],
                slot_access: vec![0u64; max_slots],
                access_counter: 0,
                max_slots,
                used_slots: 0,
                file,
            })
        } else {
            TileCache::Flat(FlatTileCache {
                data: Vec::new(),
                dirty: vec![false; nr_tiles],
                tile_bytes,
                nr_tiles,
                allocated: false,
            })
        };

        let swap_size = match dt {
            CasacoreDataType::TpComplex => 4,
            CasacoreDataType::TpDComplex => 8,
            _ => elem_size,
        };

        Ok(Self {
            tsm_path,
            header_path,
            table_path: table_path.to_path_buf(),
            pixel_type,
            cube_shape: cube_shape.to_vec(),
            tile_shape: tile_shape.to_vec(),
            tiles_per_dim,
            nr_tiles,
            elem_size,
            tile_bytes,
            file_tile_bytes,
            tile_nelem,
            big_endian,
            file_offset: 0,
            dm_seq_nr,
            storage_data_type: dt,
            tile_strides,
            tiles_per_dim_strides,
            cache,
            needs_swap,
            swap_size,
            tile_on_disk: vec![false; nr_tiles],
            read_file: None,
        })
    }

    /// Opens an existing `TiledFileIO` by reading the TSM header file.
    pub fn open(table_path: &Path, dm_seq_nr: u32) -> Result<Self, StorageError> {
        Self::open_impl(table_path, dm_seq_nr, 0)
    }

    /// Opens an existing `TiledFileIO` with an explicit cache size limit.
    pub fn open_with_cache_limit(
        table_path: &Path,
        dm_seq_nr: u32,
        max_cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        Self::open_impl(table_path, dm_seq_nr, max_cache_bytes)
    }

    fn open_impl(
        table_path: &Path,
        dm_seq_nr: u32,
        max_cache_bytes: usize,
    ) -> Result<Self, StorageError> {
        let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
        let (_variant, header) = read_tiled_header(&header_path)?;

        if header.cubes.is_empty() {
            return Err(StorageError::FormatMismatch(
                "TiledFileIO: no cubes in header".to_string(),
            ));
        }
        let cube = &header.cubes[0];
        if cube.file_seq_nr < 0 {
            return Err(StorageError::FormatMismatch(
                "TiledFileIO: invalid file_seq_nr".to_string(),
            ));
        }
        if header.col_data_types.is_empty() {
            return Err(StorageError::FormatMismatch(
                "TiledFileIO: no column data types".to_string(),
            ));
        }

        let dt = header.col_data_types[0];
        let elem_size = tile_element_size(dt);
        let tile_nelem: usize = cube.tile_shape.iter().product();
        let tile_bytes = tile_nelem * elem_size;
        let file_tile_bytes = tile_storage_bytes(dt, tile_nelem);
        let ndim = cube.cube_shape.len();

        let tiles_per_dim: Vec<usize> = (0..ndim)
            .map(|d| cube.cube_shape[d].div_ceil(cube.tile_shape[d]))
            .collect();
        let nr_tiles: usize = tiles_per_dim.iter().product();

        let tsm_path = tsm_data_path(table_path, dm_seq_nr, cube.file_seq_nr as u32);

        let tile_strides = fortran_order_strides(&cube.tile_shape);
        let tiles_per_dim_strides = fortran_order_strides(&tiles_per_dim);
        let needs_swap = header.big_endian != cfg!(target_endian = "big");

        let swap_size = match dt {
            CasacoreDataType::TpComplex => 4,
            CasacoreDataType::TpDComplex => 8,
            _ => elem_size,
        };

        let total_data_bytes = nr_tiles * file_tile_bytes;
        let use_lru = max_cache_bytes > 0 && max_cache_bytes < total_data_bytes;
        let (cache, read_file) = if use_lru {
            let max_slots = (max_cache_bytes / tile_bytes).max(1);
            let file = OpenOptions::new().read(true).write(true).open(&tsm_path)?;
            (
                TileCache::Lru(LruTileCache {
                    data: vec![0u8; max_slots * tile_bytes],
                    slot_tile: vec![usize::MAX; max_slots],
                    slot_dirty: vec![false; max_slots],
                    tile_to_slot: vec![-1i32; nr_tiles],
                    slot_access: vec![0u64; max_slots],
                    access_counter: 0,
                    max_slots,
                    used_slots: 0,
                    file,
                }),
                None,
            )
        } else {
            let read_file = OpenOptions::new().read(true).open(&tsm_path).ok();
            (
                TileCache::Flat(FlatTileCache {
                    data: Vec::new(),
                    dirty: vec![false; nr_tiles],
                    tile_bytes,
                    nr_tiles,
                    allocated: false,
                }),
                read_file,
            )
        };

        Ok(Self {
            tsm_path,
            header_path,
            table_path: table_path.to_path_buf(),
            pixel_type: dt.to_primitive_type().ok_or_else(|| {
                StorageError::FormatMismatch(
                    "TiledFileIO: unsupported primitive type in header".to_string(),
                )
            })?,
            cube_shape: cube.cube_shape.clone(),
            tile_shape: cube.tile_shape.clone(),
            tiles_per_dim,
            nr_tiles,
            elem_size,
            tile_bytes,
            file_tile_bytes,
            tile_nelem,
            big_endian: header.big_endian,
            file_offset: cube.file_offset as usize,
            dm_seq_nr,
            storage_data_type: dt,
            tile_strides,
            tiles_per_dim_strides,
            cache,
            needs_swap,
            swap_size,
            tile_on_disk: vec![true; nr_tiles],
            read_file,
        })
    }

    /// Returns the cube shape.
    pub fn cube_shape(&self) -> &[usize] {
        &self.cube_shape
    }

    /// Returns the tile shape.
    pub fn tile_shape(&self) -> &[usize] {
        &self.tile_shape
    }

    /// Returns the first cube column's pixel type when it maps to a supported
    /// primitive type.
    pub fn pixel_type(&self) -> Option<PrimitiveType> {
        Some(self.pixel_type)
    }

    /// Whether the on-disk byte order is big-endian.
    pub fn big_endian(&self) -> bool {
        self.big_endian
    }

    /// Returns the DM sequence number.
    pub fn dm_seq_nr(&self) -> u32 {
        self.dm_seq_nr
    }

    /// Returns the path to the table directory.
    pub fn table_path(&self) -> &Path {
        &self.table_path
    }

    // -----------------------------------------------------------------------
    // Tile cache
    // -----------------------------------------------------------------------

    /// Ensures the flat cache is allocated (zeroed for fresh, loaded from disk otherwise).
    fn ensure_flat_cache_allocated(&mut self) -> Result<(), StorageError> {
        let flat = match &mut self.cache {
            TileCache::Flat(f) => f,
            _ => return Ok(()),
        };
        if flat.allocated {
            return Ok(());
        }
        let total_bytes = flat.nr_tiles * flat.tile_bytes;
        let all_fresh = self.tile_on_disk.iter().all(|&on| !on);
        if all_fresh {
            flat.data = vec![0u8; total_bytes];
        } else {
            let file_tile_bytes = self.file_tile_bytes;
            let needs_swap = self.needs_swap;
            let swap_size = self.swap_size;
            let dt = self.storage_data_type;
            let tile_nelem = self.tile_nelem;

            flat.data = vec![0u8; total_bytes];
            {
                let f: &mut std::fs::File;
                let mut temp_file;
                if let Some(ref mut rf) = self.read_file {
                    f = rf;
                } else {
                    temp_file = OpenOptions::new().read(true).open(&self.tsm_path)?;
                    f = &mut temp_file;
                }
                if file_tile_bytes == flat.tile_bytes {
                    f.seek(SeekFrom::Start(self.file_offset as u64))?;
                    f.read_exact(&mut flat.data)?;
                    if needs_swap {
                        swap_bytes_inplace(&mut flat.data, swap_size);
                    }
                } else {
                    for tile_index in 0..flat.nr_tiles {
                        let off = tile_index * flat.tile_bytes;
                        let file_pos = (self.file_offset + tile_index * file_tile_bytes) as u64;
                        read_tiled_file_tile(
                            f,
                            file_pos,
                            &mut flat.data[off..off + flat.tile_bytes],
                            file_tile_bytes,
                            dt,
                            tile_nelem,
                            needs_swap,
                            swap_size,
                        )?;
                    }
                }
            }
        }
        flat.allocated = true;
        Ok(())
    }

    /// Ensures a tile is available in the LRU cache, loading it from disk
    /// and evicting the least-recently-used tile if necessary.
    /// Returns the slot index holding the tile.
    fn ensure_lru_tile_loaded(&mut self, tile_index: usize) -> Result<usize, StorageError> {
        // Check if already loaded via direct-indexed Vec (O(1)).
        let cached_slot = match &self.cache {
            TileCache::Lru(lru) => lru.tile_to_slot[tile_index],
            _ => return Ok(0), // flat cache — not used
        };
        if cached_slot >= 0 {
            return Ok(cached_slot as usize);
        }

        let tile_bytes = self.tile_bytes;
        let file_tile_bytes = self.file_tile_bytes;
        let file_offset = self.file_offset;
        let needs_swap = self.needs_swap;
        let swap_size = self.swap_size;
        let dt = self.storage_data_type;
        let tile_nelem = self.tile_nelem;
        let tile_needs_read = self.tile_on_disk[tile_index];

        let lru = match &mut self.cache {
            TileCache::Lru(lru) => lru,
            _ => unreachable!(),
        };

        let slot = if lru.used_slots < lru.max_slots {
            let s = lru.used_slots;
            lru.used_slots += 1;
            s
        } else {
            // Try batch flush (dirty contiguous tiles → single write).
            if Self::try_batch_flush_lru_inner(
                lru,
                &mut self.tile_on_disk,
                tile_bytes,
                file_tile_bytes,
                file_offset,
                needs_swap,
                swap_size,
            )? {
                let s = lru.used_slots;
                lru.used_slots += 1;
                s
            }
            // Try batch load (clean contiguous tiles → single read prefetch).
            else if Self::try_batch_load_lru_inner(
                lru,
                &self.tile_on_disk,
                tile_index,
                self.nr_tiles,
                tile_bytes,
                file_tile_bytes,
                file_offset,
                needs_swap,
                swap_size,
            )? {
                // Tile is already loaded at slot 0 by the batch load.
                return Ok(0);
            } else {
                // Find least-recently-used slot.
                let mut min_slot = 0;
                let mut min_access = u64::MAX;
                for i in 0..lru.max_slots {
                    if lru.slot_access[i] < min_access {
                        min_access = lru.slot_access[i];
                        min_slot = i;
                    }
                }

                // Evict: write dirty tile to disk if needed.
                let old_tile = lru.slot_tile[min_slot];
                Self::flush_lru_slot_inner(
                    lru,
                    &mut self.tile_on_disk,
                    min_slot,
                    tile_bytes,
                    file_tile_bytes,
                    file_offset,
                    dt,
                    tile_nelem,
                    needs_swap,
                    swap_size,
                )?;
                if old_tile != usize::MAX {
                    lru.tile_to_slot[old_tile] = -1;
                }
                min_slot
            }
        };

        // Load tile data into the slot.
        let off = slot * tile_bytes;
        if !tile_needs_read {
            lru.data[off..off + tile_bytes].fill(0);
        } else {
            let file_pos = (file_offset + tile_index * file_tile_bytes) as u64;
            if file_tile_bytes == tile_bytes {
                #[cfg(unix)]
                lru.file
                    .read_exact_at(&mut lru.data[off..off + tile_bytes], file_pos)?;
                #[cfg(not(unix))]
                {
                    lru.file.seek(SeekFrom::Start(file_pos))?;
                    lru.file.read_exact(&mut lru.data[off..off + tile_bytes])?;
                }
                if needs_swap {
                    swap_bytes_inplace(&mut lru.data[off..off + tile_bytes], swap_size);
                }
            } else {
                read_tiled_file_tile(
                    &mut lru.file,
                    file_pos,
                    &mut lru.data[off..off + tile_bytes],
                    file_tile_bytes,
                    dt,
                    tile_nelem,
                    needs_swap,
                    swap_size,
                )?;
            }
        }

        // Update mappings.
        lru.slot_tile[slot] = tile_index;
        lru.tile_to_slot[tile_index] = slot as i32;
        lru.slot_dirty[slot] = false;
        lru.slot_access[slot] = lru.access_counter;
        lru.access_counter += 1;
        Ok(slot)
    }

    /// Attempts to batch-flush all dirty LRU tiles in a single write when
    /// they form a contiguous range of tile indices mapped to sequential slots.
    ///
    /// Returns `true` if the flush was performed (cache is now empty and ready
    /// for reuse), `false` if conditions weren't met (caller should fall back
    /// to per-tile LRU eviction).
    fn try_batch_flush_lru_inner(
        lru: &mut LruTileCache,
        tile_on_disk: &mut [bool],
        tile_bytes: usize,
        file_tile_bytes: usize,
        file_offset: usize,
        needs_swap: bool,
        swap_size: usize,
    ) -> Result<bool, StorageError> {
        if file_tile_bytes != tile_bytes {
            return Ok(false);
        }
        // All slots must be used and dirty, and tiles must be in sequential
        // slot order (slot i holds tile base+i) for a single contiguous write.
        let max_slots = lru.max_slots;
        let mut all_dirty = true;
        let base_tile = lru.slot_tile[0];
        let mut contiguous = base_tile != usize::MAX;
        for i in 0..max_slots {
            if !lru.slot_dirty[i] {
                all_dirty = false;
                break;
            }
            if lru.slot_tile[i] != base_tile + i {
                contiguous = false;
                break;
            }
        }
        if !all_dirty || !contiguous {
            return Ok(false);
        }

        // Single write of the entire cache buffer.
        let total_bytes = max_slots * tile_bytes;
        let file_pos = (file_offset + base_tile * tile_bytes) as u64;
        if needs_swap {
            let mut buf = lru.data[..total_bytes].to_vec();
            swap_bytes_inplace(&mut buf, swap_size);
            lru.file.seek(SeekFrom::Start(file_pos))?;
            lru.file.write_all(&buf)?;
        } else {
            lru.file.seek(SeekFrom::Start(file_pos))?;
            lru.file.write_all(&lru.data[..total_bytes])?;
        }

        // Mark all flushed tiles as on-disk and reset cache state.
        for i in 0..max_slots {
            tile_on_disk[base_tile + i] = true;
            lru.tile_to_slot[base_tile + i] = -1;
        }
        lru.slot_tile.fill(usize::MAX);
        lru.slot_dirty.fill(false);
        lru.used_slots = 0;
        lru.access_counter = 0;
        Ok(true)
    }

    /// Flush one dirty LRU slot and mark that tile as persisted on disk.
    #[allow(clippy::too_many_arguments)]
    fn flush_lru_slot_inner(
        lru: &mut LruTileCache,
        tile_on_disk: &mut [bool],
        slot: usize,
        tile_bytes: usize,
        file_tile_bytes: usize,
        file_offset: usize,
        dt: CasacoreDataType,
        tile_nelem: usize,
        needs_swap: bool,
        swap_size: usize,
    ) -> Result<(), StorageError> {
        let tile_idx = lru.slot_tile[slot];
        if tile_idx == usize::MAX || !lru.slot_dirty[slot] {
            return Ok(());
        }

        let off = slot * tile_bytes;
        let file_pos = (file_offset + tile_idx * file_tile_bytes) as u64;
        write_tiled_file_tile(
            &mut lru.file,
            file_pos,
            &lru.data[off..off + tile_bytes],
            file_tile_bytes,
            dt,
            tile_nelem,
            needs_swap,
            swap_size,
        )?;
        lru.slot_dirty[slot] = false;
        tile_on_disk[tile_idx] = true;
        Ok(())
    }

    /// Batch-loads a contiguous range of tiles into the LRU cache with a
    /// single read when the cache holds clean, contiguous tiles and the
    /// requested tile continues sequentially.
    ///
    /// Returns `true` if the batch load was performed (requested tile is now
    /// at slot 0), `false` if conditions weren't met.
    #[allow(clippy::too_many_arguments)]
    fn try_batch_load_lru_inner(
        lru: &mut LruTileCache,
        tile_on_disk: &[bool],
        tile_index: usize,
        nr_tiles: usize,
        tile_bytes: usize,
        file_tile_bytes: usize,
        file_offset: usize,
        needs_swap: bool,
        swap_size: usize,
    ) -> Result<bool, StorageError> {
        if file_tile_bytes != tile_bytes {
            return Ok(false);
        }
        let max_slots = lru.max_slots;

        // Check: current slots hold contiguous clean tiles, and the requested
        // tile continues right after them (sequential scan pattern).
        let base_tile = lru.slot_tile[0];
        if base_tile == usize::MAX || tile_index != base_tile + max_slots {
            return Ok(false);
        }
        for i in 0..max_slots {
            if lru.slot_dirty[i] || lru.slot_tile[i] != base_tile + i {
                return Ok(false);
            }
        }

        // All tiles on disk from tile_index onward? (Required for bulk read.)
        let load_count = max_slots.min(nr_tiles - tile_index);
        let all_on_disk = tile_on_disk[tile_index..tile_index + load_count]
            .iter()
            .all(|&d| d);
        if !all_on_disk {
            return Ok(false);
        }

        // Clear old mappings.
        for i in 0..max_slots {
            lru.tile_to_slot[base_tile + i] = -1;
        }

        // Single read of the contiguous tile range.
        let total_bytes = load_count * tile_bytes;
        let file_pos = (file_offset + tile_index * tile_bytes) as u64;
        lru.file.seek(SeekFrom::Start(file_pos))?;
        lru.file.read_exact(&mut lru.data[..total_bytes])?;
        if needs_swap {
            swap_bytes_inplace(&mut lru.data[..total_bytes], swap_size);
        }

        // Set up new mappings.
        for i in 0..load_count {
            lru.slot_tile[i] = tile_index + i;
            lru.tile_to_slot[tile_index + i] = i as i32;
            lru.slot_dirty[i] = false;
            lru.slot_access[i] = i as u64;
        }
        // Mark any remaining slots as empty (when load_count < max_slots).
        for i in load_count..max_slots {
            lru.slot_tile[i] = usize::MAX;
        }
        lru.used_slots = load_count;
        lru.access_counter = load_count as u64;
        Ok(true)
    }

    /// Gets a tile from the cache (read-only), returned as `&[u8]`.
    #[inline]
    fn get_cached_tile(&mut self, tile_index: usize) -> Result<&[u8], StorageError> {
        match &self.cache {
            TileCache::Flat(flat) => {
                if !flat.allocated {
                    self.ensure_flat_cache_allocated()?;
                }
                let tile_bytes = self.tile_bytes;
                let flat = match &self.cache {
                    TileCache::Flat(f) => f,
                    _ => unreachable!(),
                };
                let off = tile_index * tile_bytes;
                Ok(&flat.data[off..off + tile_bytes])
            }
            TileCache::Lru(_) => {
                let slot = self.ensure_lru_tile_loaded(tile_index)?;
                let tile_bytes = self.tile_bytes;
                let lru = match &mut self.cache {
                    TileCache::Lru(l) => l,
                    _ => unreachable!(),
                };
                lru.slot_access[slot] = lru.access_counter;
                lru.access_counter += 1;
                let off = slot * tile_bytes;
                Ok(&lru.data[off..off + tile_bytes])
            }
        }
    }

    /// Gets a mutable tile from the cache. Marks it dirty.
    #[inline]
    fn get_cached_tile_mut(&mut self, tile_index: usize) -> Result<&mut [u8], StorageError> {
        match &self.cache {
            TileCache::Flat(flat) => {
                if !flat.allocated {
                    self.ensure_flat_cache_allocated()?;
                }
                let tile_bytes = self.tile_bytes;
                let flat = match &mut self.cache {
                    TileCache::Flat(f) => f,
                    _ => unreachable!(),
                };
                flat.dirty[tile_index] = true;
                let off = tile_index * tile_bytes;
                Ok(&mut flat.data[off..off + tile_bytes])
            }
            TileCache::Lru(_) => {
                let slot = self.ensure_lru_tile_loaded(tile_index)?;
                let tile_bytes = self.tile_bytes;
                let lru = match &mut self.cache {
                    TileCache::Lru(l) => l,
                    _ => unreachable!(),
                };
                lru.slot_dirty[slot] = true;
                lru.slot_access[slot] = lru.access_counter;
                lru.access_counter += 1;
                let off = slot * tile_bytes;
                Ok(&mut lru.data[off..off + tile_bytes])
            }
        }
    }

    /// Flushes all dirty tiles to disk and clears the cache.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        match &mut self.cache {
            TileCache::Flat(flat) => {
                if !flat.allocated {
                    return Ok(());
                }
                let has_dirty = flat.dirty.iter().any(|&d| d);
                if has_dirty {
                    let mut f = std::io::BufWriter::new(
                        OpenOptions::new().write(true).open(&self.tsm_path)?,
                    );

                    if self.file_tile_bytes != self.tile_bytes || self.needs_swap {
                        let tile_bytes = self.tile_bytes;
                        let file_tile_bytes = self.file_tile_bytes;
                        let dt = self.storage_data_type;
                        let tile_nelem = self.tile_nelem;
                        for tile_index in 0..self.nr_tiles {
                            if !flat.dirty[tile_index] {
                                continue;
                            }
                            let off = tile_index * tile_bytes;
                            let offset = self.file_offset + tile_index * file_tile_bytes;
                            write_tiled_file_tile(
                                f.get_mut(),
                                offset as u64,
                                &flat.data[off..off + tile_bytes],
                                file_tile_bytes,
                                dt,
                                tile_nelem,
                                self.needs_swap,
                                self.swap_size,
                            )?;
                            self.tile_on_disk[tile_index] = true;
                        }
                    } else {
                        let tile_bytes = self.tile_bytes;
                        let mut run_start: Option<usize> = None;
                        for tile_index in 0..=self.nr_tiles {
                            let is_dirty = tile_index < self.nr_tiles && flat.dirty[tile_index];
                            if is_dirty && run_start.is_none() {
                                run_start = Some(tile_index);
                            } else if !is_dirty {
                                if let Some(start) = run_start {
                                    let byte_start = self.file_offset + start * tile_bytes;
                                    let src_start = start * tile_bytes;
                                    let src_end = tile_index * tile_bytes;
                                    f.seek(SeekFrom::Start(byte_start as u64))?;
                                    f.write_all(&flat.data[src_start..src_end])?;
                                    for written_tile in start..tile_index {
                                        self.tile_on_disk[written_tile] = true;
                                    }
                                    run_start = None;
                                }
                            }
                        }
                    }
                }
                flat.data.clear();
                flat.data.shrink_to_fit();
                flat.dirty.fill(false);
                flat.allocated = false;
            }
            TileCache::Lru(lru) => {
                let tile_bytes = self.tile_bytes;
                let file_tile_bytes = self.file_tile_bytes;
                let file_offset = self.file_offset;
                let needs_swap = self.needs_swap;
                let swap_size = self.swap_size;
                let dt = self.storage_data_type;
                let tile_nelem = self.tile_nelem;
                // Try batch flush first (single write if tiles are contiguous).
                if !Self::try_batch_flush_lru_inner(
                    lru,
                    &mut self.tile_on_disk,
                    tile_bytes,
                    file_tile_bytes,
                    file_offset,
                    needs_swap,
                    swap_size,
                )? {
                    // Fall back to per-tile flush.
                    for slot in 0..lru.used_slots {
                        Self::flush_lru_slot_inner(
                            lru,
                            &mut self.tile_on_disk,
                            slot,
                            tile_bytes,
                            file_tile_bytes,
                            file_offset,
                            dt,
                            tile_nelem,
                            needs_swap,
                            swap_size,
                        )?;
                    }
                    // Reset cache state.
                    for slot in 0..lru.used_slots {
                        let tile_idx = lru.slot_tile[slot];
                        if tile_idx != usize::MAX {
                            lru.tile_to_slot[tile_idx] = -1;
                        }
                    }
                    lru.slot_tile.fill(usize::MAX);
                    lru.slot_dirty.fill(false);
                    lru.used_slots = 0;
                    lru.access_counter = 0;
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Generic typed slice access
    // -----------------------------------------------------------------------

    /// Writes a rectangular slice from an ndarray into the tile file.
    ///
    /// Accepts any ndarray layout (C-order or Fortran-order).
    pub fn put_slice_ndarray<T: TilePixel>(
        &mut self,
        data: &ArrayD<T>,
        start: &[usize],
    ) -> Result<(), StorageError> {
        let shape: Vec<usize> = data.shape().to_vec();
        let fortran_view = data.t();
        let maybe_fortran_slice = fortran_view.as_slice();
        if let Some(f_data) = maybe_fortran_slice {
            return self.put_slice_fortran::<T>(f_data, start, &shape);
        }
        // Fallback: array is C-order. Use per-element scatter.
        let ndim = self.cube_shape.len();
        let tile_strides = self.tile_strides.clone();
        let tiles_per_dim_strides = self.tiles_per_dim_strides.clone();
        let tile_shape = self.tile_shape.clone();
        let cube_shape = self.cube_shape.clone();

        let mut tile_start = vec![0usize; ndim];
        let mut tile_end = vec![0usize; ndim];
        for d in 0..ndim {
            tile_start[d] = start[d] / tile_shape[d];
            tile_end[d] = (start[d] + shape[d] - 1) / tile_shape[d] + 1;
        }

        let mut tile_pos = tile_start.clone();
        loop {
            let tile_index = dot_product(&tile_pos, &tiles_per_dim_strides);
            let tile_origin: Vec<usize> = (0..ndim).map(|d| tile_pos[d] * tile_shape[d]).collect();

            let mut overlap_lo = vec![0usize; ndim];
            let mut overlap_hi = vec![0usize; ndim];
            let mut valid = true;
            for d in 0..ndim {
                let tile_end_d = std::cmp::min(tile_origin[d] + tile_shape[d], cube_shape[d]);
                overlap_lo[d] = std::cmp::max(start[d], tile_origin[d]);
                overlap_hi[d] = std::cmp::min(start[d] + shape[d], tile_end_d);
                if overlap_lo[d] >= overlap_hi[d] {
                    valid = false;
                    break;
                }
            }

            if valid {
                let tile_bytes = self.get_cached_tile_mut(tile_index)?;
                let tile_data = tile_as_typed_mut::<T>(tile_bytes);
                let inner_count = overlap_hi[0] - overlap_lo[0];

                let outer_dims: Vec<usize> =
                    (1..ndim).map(|d| overlap_hi[d] - overlap_lo[d]).collect();
                let outer_total: usize = outer_dims.iter().product();

                for outer_lin in 0..outer_total {
                    let mut tile_off = (overlap_lo[0] - tile_origin[0]) * tile_strides[0];
                    let mut remaining = outer_lin;
                    let mut nd_idx = vec![0usize; ndim];
                    for d in 1..ndim {
                        let ext = overlap_hi[d] - overlap_lo[d];
                        let coord = remaining % ext;
                        remaining /= ext;
                        tile_off += (overlap_lo[d] - tile_origin[d] + coord) * tile_strides[d];
                        nd_idx[d] = overlap_lo[d] - start[d] + coord;
                    }

                    let x_start = overlap_lo[0] - start[0];
                    for i in 0..inner_count {
                        nd_idx[0] = x_start + i;
                        tile_data[tile_off + i] = data[IxDyn(&nd_idx)];
                    }
                }
            }

            let mut carry = true;
            for d in 0..ndim {
                if carry {
                    tile_pos[d] += 1;
                    if tile_pos[d] < tile_end[d] {
                        carry = false;
                    } else {
                        tile_pos[d] = tile_start[d];
                    }
                }
            }
            if carry {
                break;
            }
        }
        Ok(())
    }

    /// Writes a rectangular slice of C-order (row-major) data into tiles.
    pub fn put_slice_c_order<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<(), StorageError> {
        let ndim = self.cube_shape.len();
        assert!(ndim <= MAX_NDIM, "ndim exceeds MAX_NDIM");
        let c_strides = c_order_strides(shape);

        // Pick inner axis: axis 0 gives contiguous tile writes (Fortran order),
        // but axis 0's C-stride may be too large at high resolutions.
        // Use axis 0 when the read footprint fits in L1 (~64KB); otherwise
        // fall back to the first non-singleton axis from the end (C-contiguous).
        let l1_threshold = 64 * 1024 / T::ELEM_SIZE; // ~64KB in elements
        let inner_axis = if c_strides[0] * self.tile_shape[0] <= l1_threshold {
            0
        } else {
            // Find the C-contiguous non-singleton axis (walk from end).
            let mut ia = ndim - 1;
            while ia > 0 && shape[ia] <= 1 {
                ia -= 1;
            }
            ia
        };
        let inner_c_stride = c_strides[inner_axis];
        let inner_t_stride = self.tile_strides[inner_axis];

        // Copy self fields into stack arrays before the tile loop to avoid
        // borrow conflicts with get_cached_tile_mut.
        let mut ts = [0usize; MAX_NDIM]; // tile_strides
        let mut tpds = [0usize; MAX_NDIM]; // tiles_per_dim_strides
        let mut tsh = [0usize; MAX_NDIM]; // tile_shape
        let mut csh = [0usize; MAX_NDIM]; // cube_shape
        ts[..ndim].copy_from_slice(&self.tile_strides[..ndim]);
        tpds[..ndim].copy_from_slice(&self.tiles_per_dim_strides[..ndim]);
        tsh[..ndim].copy_from_slice(&self.tile_shape[..ndim]);
        csh[..ndim].copy_from_slice(&self.cube_shape[..ndim]);

        let mut tile_start_pos = [0usize; MAX_NDIM];
        let mut tile_end_pos = [0usize; MAX_NDIM];
        let mut tile_origin = [0usize; MAX_NDIM];
        let mut overlap_lo = [0usize; MAX_NDIM];
        let mut overlap_hi = [0usize; MAX_NDIM];
        let mut outer_axes = [0usize; MAX_NDIM];
        let mut outer_dims = [0usize; MAX_NDIM];
        let mut n_outer = 0usize;

        for d in 0..ndim {
            tile_start_pos[d] = start[d] / tsh[d];
            tile_end_pos[d] = (start[d] + shape[d] - 1) / tsh[d] + 1;
            if d != inner_axis {
                outer_axes[n_outer] = d;
                n_outer += 1;
            }
        }

        let mut tile_pos = tile_start_pos;
        loop {
            let mut tile_index = 0usize;
            let mut valid = true;
            for d in 0..ndim {
                tile_origin[d] = tile_pos[d] * tsh[d];
                tile_index += tile_pos[d] * tpds[d];
                let tile_end_d = std::cmp::min(tile_origin[d] + tsh[d], csh[d]);
                overlap_lo[d] = std::cmp::max(start[d], tile_origin[d]);
                overlap_hi[d] = std::cmp::min(start[d] + shape[d], tile_end_d);
                if overlap_lo[d] >= overlap_hi[d] {
                    valid = false;
                    break;
                }
            }

            if valid {
                let inner_count = overlap_hi[inner_axis] - overlap_lo[inner_axis];
                let base_tile_off =
                    (overlap_lo[inner_axis] - tile_origin[inner_axis]) * inner_t_stride;
                let base_c_off = (overlap_lo[inner_axis] - start[inner_axis]) * inner_c_stride;

                let mut outer_total = 1usize;
                for oi in 0..n_outer {
                    outer_dims[oi] = overlap_hi[outer_axes[oi]] - overlap_lo[outer_axes[oi]];
                    outer_total *= outer_dims[oi];
                }

                let tile_bytes = self.get_cached_tile_mut(tile_index)?;
                let tile_data = tile_as_typed_mut::<T>(tile_bytes);

                let mut outer_coord = [0usize; MAX_NDIM];
                for _ in 0..outer_total {
                    let mut tile_off = base_tile_off;
                    let mut c_off = base_c_off;
                    for oi in 0..n_outer {
                        let d = outer_axes[oi];
                        tile_off += (overlap_lo[d] - tile_origin[d] + outer_coord[oi]) * ts[d];
                        c_off += (overlap_lo[d] - start[d] + outer_coord[oi]) * c_strides[d];
                    }

                    if inner_c_stride == 1 && inner_t_stride == 1 {
                        tile_data[tile_off..tile_off + inner_count]
                            .copy_from_slice(&data[c_off..c_off + inner_count]);
                    } else if inner_t_stride == 1 {
                        unsafe {
                            let tp = tile_data.as_mut_ptr().add(tile_off);
                            let dp = data.as_ptr().add(c_off);
                            for i in 0..inner_count {
                                *tp.add(i) = *dp.add(i * inner_c_stride);
                            }
                        }
                    } else if inner_c_stride == 1 {
                        unsafe {
                            let tp = tile_data.as_mut_ptr().add(tile_off);
                            let dp = data.as_ptr().add(c_off);
                            for i in 0..inner_count {
                                *tp.add(i * inner_t_stride) = *dp.add(i);
                            }
                        }
                    } else {
                        for i in 0..inner_count {
                            tile_data[tile_off + i * inner_t_stride] =
                                data[c_off + i * inner_c_stride];
                        }
                    }

                    let mut carry = true;
                    for oi in 0..n_outer {
                        if carry {
                            outer_coord[oi] += 1;
                            if outer_coord[oi] < outer_dims[oi] {
                                carry = false;
                            } else {
                                outer_coord[oi] = 0;
                            }
                        }
                    }
                }
            }

            let mut carry = true;
            for d in 0..ndim {
                if carry {
                    tile_pos[d] += 1;
                    if tile_pos[d] < tile_end_pos[d] {
                        carry = false;
                    } else {
                        tile_pos[d] = tile_start_pos[d];
                    }
                }
            }
            if carry {
                break;
            }
        }
        Ok(())
    }

    /// Writes a rectangular slice of Fortran-order data into the tile file.
    pub fn put_slice_fortran<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<(), StorageError> {
        let ndim = self.cube_shape.len();
        assert!(ndim <= MAX_NDIM, "ndim exceeds MAX_NDIM");
        let input_strides = fortran_order_strides(shape);

        // Copy self fields to stack arrays to avoid borrow conflicts.
        let mut ts = [0usize; MAX_NDIM];
        let mut tpds = [0usize; MAX_NDIM];
        let mut tsh = [0usize; MAX_NDIM];
        let mut csh = [0usize; MAX_NDIM];
        ts[..ndim].copy_from_slice(&self.tile_strides[..ndim]);
        tpds[..ndim].copy_from_slice(&self.tiles_per_dim_strides[..ndim]);
        tsh[..ndim].copy_from_slice(&self.tile_shape[..ndim]);
        csh[..ndim].copy_from_slice(&self.cube_shape[..ndim]);

        let mut tile_start = [0usize; MAX_NDIM];
        let mut tile_end = [0usize; MAX_NDIM];
        let mut tile_origin = [0usize; MAX_NDIM];
        let mut overlap_lo = [0usize; MAX_NDIM];
        let mut overlap_hi = [0usize; MAX_NDIM];

        for d in 0..ndim {
            tile_start[d] = start[d] / tsh[d];
            tile_end[d] = (start[d] + shape[d] - 1) / tsh[d] + 1;
        }

        let mut tile_pos = tile_start;
        loop {
            let mut tile_index = 0usize;
            let mut valid = true;
            for d in 0..ndim {
                tile_origin[d] = tile_pos[d] * tsh[d];
                tile_index += tile_pos[d] * tpds[d];
                let tile_end_d = std::cmp::min(tile_origin[d] + tsh[d], csh[d]);
                overlap_lo[d] = std::cmp::max(start[d], tile_origin[d]);
                overlap_hi[d] = std::cmp::min(start[d] + shape[d], tile_end_d);
                if overlap_lo[d] >= overlap_hi[d] {
                    valid = false;
                    break;
                }
            }

            if valid {
                let inner_count = overlap_hi[0] - overlap_lo[0];
                let base_tile_off = overlap_lo[0] - tile_origin[0];
                let base_input_off = (overlap_lo[0] - start[0]) * input_strides[0];

                let mut outer_dims = [0usize; MAX_NDIM];
                let mut outer_total = 1usize;
                for d in 1..ndim {
                    outer_dims[d - 1] = overlap_hi[d] - overlap_lo[d];
                    outer_total *= outer_dims[d - 1];
                }

                let tile_bytes = self.get_cached_tile_mut(tile_index)?;
                let tile_data = tile_as_typed_mut::<T>(tile_bytes);

                let mut outer_coord = [0usize; MAX_NDIM];
                for _ in 0..outer_total {
                    let mut tile_off = base_tile_off;
                    let mut input_off = base_input_off;
                    for d in 1..ndim {
                        tile_off += (overlap_lo[d] - tile_origin[d] + outer_coord[d - 1]) * ts[d];
                        input_off +=
                            (overlap_lo[d] - start[d] + outer_coord[d - 1]) * input_strides[d];
                    }

                    tile_data[tile_off..tile_off + inner_count]
                        .copy_from_slice(&data[input_off..input_off + inner_count]);

                    let mut carry = true;
                    for d in 0..ndim.saturating_sub(1) {
                        if carry {
                            outer_coord[d] += 1;
                            if outer_coord[d] < outer_dims[d] {
                                carry = false;
                            } else {
                                outer_coord[d] = 0;
                            }
                        }
                    }
                }
            }

            let mut carry = true;
            for d in 0..ndim {
                if carry {
                    tile_pos[d] += 1;
                    if tile_pos[d] < tile_end[d] {
                        carry = false;
                    } else {
                        tile_pos[d] = tile_start[d];
                    }
                }
            }
            if carry {
                break;
            }
        }

        Ok(())
    }

    /// Reads a rectangular slice from the tile file.
    ///
    /// Returns a Fortran-order `ArrayD<T>` of the given `shape`.
    pub fn get_slice<T: TilePixel>(
        &mut self,
        start: &[usize],
        shape: &[usize],
    ) -> Result<ArrayD<T>, StorageError> {
        let ndim = self.cube_shape.len();
        assert!(ndim <= MAX_NDIM, "ndim exceeds MAX_NDIM");
        let nelem: usize = shape.iter().product();
        let mut result = vec![T::default(); nelem];
        let result_strides = fortran_order_strides(shape);

        let inner_axis = {
            let (best, _) = shape[..ndim]
                .iter()
                .enumerate()
                .max_by_key(|&(_, &ext)| ext)
                .unwrap();
            if shape[best] > shape[0] { best } else { 0 }
        };
        let inner_t_stride = self.tile_strides[inner_axis];
        let inner_r_stride = result_strides[inner_axis];

        let mut ts = [0usize; MAX_NDIM];
        let mut tpds = [0usize; MAX_NDIM];
        let mut tsh = [0usize; MAX_NDIM];
        let mut csh = [0usize; MAX_NDIM];
        ts[..ndim].copy_from_slice(&self.tile_strides[..ndim]);
        tpds[..ndim].copy_from_slice(&self.tiles_per_dim_strides[..ndim]);
        tsh[..ndim].copy_from_slice(&self.tile_shape[..ndim]);
        csh[..ndim].copy_from_slice(&self.cube_shape[..ndim]);

        let mut tile_start = [0usize; MAX_NDIM];
        let mut tile_end_pos = [0usize; MAX_NDIM];
        let mut tile_origin = [0usize; MAX_NDIM];
        let mut overlap_lo = [0usize; MAX_NDIM];
        let mut overlap_hi = [0usize; MAX_NDIM];
        let mut outer_axes = [0usize; MAX_NDIM];
        let mut outer_dims = [0usize; MAX_NDIM];
        let mut n_outer = 0usize;

        for d in 0..ndim {
            tile_start[d] = start[d] / tsh[d];
            tile_end_pos[d] = (start[d] + shape[d] - 1) / tsh[d] + 1;
            if d != inner_axis {
                outer_axes[n_outer] = d;
                n_outer += 1;
            }
        }

        let mut tile_pos = tile_start;
        loop {
            let mut tile_index = 0usize;
            let mut valid = true;
            for d in 0..ndim {
                tile_origin[d] = tile_pos[d] * tsh[d];
                tile_index += tile_pos[d] * tpds[d];
                let tile_end_d = std::cmp::min(tile_origin[d] + tsh[d], csh[d]);
                overlap_lo[d] = std::cmp::max(start[d], tile_origin[d]);
                overlap_hi[d] = std::cmp::min(start[d] + shape[d], tile_end_d);
                if overlap_lo[d] >= overlap_hi[d] {
                    valid = false;
                    break;
                }
            }

            if valid {
                let inner_count = overlap_hi[inner_axis] - overlap_lo[inner_axis];
                let base_tile_off =
                    (overlap_lo[inner_axis] - tile_origin[inner_axis]) * inner_t_stride;
                let base_result_off = (overlap_lo[inner_axis] - start[inner_axis]) * inner_r_stride;

                let mut outer_total = 1usize;
                for oi in 0..n_outer {
                    outer_dims[oi] = overlap_hi[outer_axes[oi]] - overlap_lo[outer_axes[oi]];
                    outer_total *= outer_dims[oi];
                }

                let tile_bytes = self.get_cached_tile(tile_index)?;
                let tile_data = tile_as_typed::<T>(tile_bytes);

                let mut outer_coord = [0usize; MAX_NDIM];
                for _ in 0..outer_total {
                    let mut tile_off = base_tile_off;
                    let mut result_off = base_result_off;
                    for oi in 0..n_outer {
                        let d = outer_axes[oi];
                        tile_off += (overlap_lo[d] - tile_origin[d] + outer_coord[oi]) * ts[d];
                        result_off +=
                            (overlap_lo[d] - start[d] + outer_coord[oi]) * result_strides[d];
                    }

                    if inner_t_stride == 1 && inner_r_stride == 1 {
                        result[result_off..result_off + inner_count]
                            .copy_from_slice(&tile_data[tile_off..tile_off + inner_count]);
                    } else {
                        unsafe {
                            let rp = result.as_mut_ptr().add(result_off);
                            let tp = tile_data.as_ptr().add(tile_off);
                            for i in 0..inner_count {
                                *rp.add(i * inner_r_stride) = *tp.add(i * inner_t_stride);
                            }
                        }
                    }

                    let mut carry = true;
                    for oi in 0..n_outer {
                        if carry {
                            outer_coord[oi] += 1;
                            if outer_coord[oi] < outer_dims[oi] {
                                carry = false;
                            } else {
                                outer_coord[oi] = 0;
                            }
                        }
                    }
                }
            }

            let mut carry = true;
            for d in 0..ndim {
                if carry {
                    tile_pos[d] += 1;
                    if tile_pos[d] < tile_end_pos[d] {
                        carry = false;
                    } else {
                        tile_pos[d] = tile_start[d];
                    }
                }
            }
            if carry {
                break;
            }
        }

        ArrayD::from_shape_vec(IxDyn(shape).f(), result)
            .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))
    }

    /// Reads the full cube as a Fortran-order `ArrayD<T>`.
    pub fn get_all<T: TilePixel>(&mut self) -> Result<ArrayD<T>, StorageError> {
        let start = vec![0; self.cube_shape.len()];
        let shape = self.cube_shape.clone();
        self.get_slice::<T>(&start, &shape)
    }
}

impl Drop for TiledFileIO {
    fn drop(&mut self) {
        // Best-effort flush on drop.
        let _ = self.flush();
    }
}

/// Reinterpret a byte slice as a typed slice (read-only).
///
/// # Safety
/// The caller must ensure the byte slice is aligned for `T` and that
/// `bytes.len()` is a multiple of `T::ELEM_SIZE`.
#[inline]
fn tile_as_typed<T: TilePixel>(bytes: &[u8]) -> &[T] {
    debug_assert_eq!(bytes.len() % T::ELEM_SIZE, 0);
    debug_assert_eq!(
        (bytes.as_ptr() as usize) % std::mem::align_of::<T>(),
        0,
        "tile buffer misaligned for {}",
        std::any::type_name::<T>()
    );
    unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const T, bytes.len() / T::ELEM_SIZE) }
}

/// Reinterpret a byte slice as a typed mutable slice.
#[inline]
fn tile_as_typed_mut<T: TilePixel>(bytes: &mut [u8]) -> &mut [T] {
    debug_assert_eq!(bytes.len() % T::ELEM_SIZE, 0);
    debug_assert_eq!(
        (bytes.as_ptr() as usize) % std::mem::align_of::<T>(),
        0,
        "tile buffer misaligned for {}",
        std::any::type_name::<T>()
    );
    unsafe {
        std::slice::from_raw_parts_mut(bytes.as_mut_ptr() as *mut T, bytes.len() / T::ELEM_SIZE)
    }
}

/// Byte-swap in place: reverse bytes within each `component_size`-byte chunk.
fn swap_bytes_inplace(data: &mut [u8], component_size: usize) {
    match component_size {
        4 => {
            for chunk in data.chunks_exact_mut(4) {
                chunk.swap(0, 3);
                chunk.swap(1, 2);
            }
        }
        8 => {
            for chunk in data.chunks_exact_mut(8) {
                chunk.swap(0, 7);
                chunk.swap(1, 6);
                chunk.swap(2, 5);
                chunk.swap(3, 4);
            }
        }
        _ => {
            for chunk in data.chunks_exact_mut(component_size) {
                chunk.reverse();
            }
        }
    }
}

/// Compute C-order (row-major) strides for a given shape.
/// stride[last] = 1, stride[i] = product of shape[i+1..].
fn c_order_strides(shape: &[usize]) -> Vec<usize> {
    let ndim = shape.len();
    let mut strides = vec![1usize; ndim];
    for i in (0..ndim.saturating_sub(1)).rev() {
        strides[i] = strides[i + 1] * shape[i + 1];
    }
    strides
}

/// Compute Fortran-order (column-major) strides for a given shape.
/// stride[0] = 1, stride[i] = product of shape[0..i].
fn fortran_order_strides(shape: &[usize]) -> Vec<usize> {
    let ndim = shape.len();
    let mut strides = vec![1usize; ndim];
    for i in 1..ndim {
        strides[i] = strides[i - 1] * shape[i - 1];
    }
    strides
}

/// Dot product of two equal-length slices.
fn dot_product(a: &[usize], b: &[usize]) -> usize {
    a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};
    use ndarray::ArrayD;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    use crate::storage::TABLE_CONTROL_FILE;
    use crate::storage::table_control::{read_table_dat, write_table_dat};
    use crate::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};

    fn make_bool_tile_pattern(base: usize) -> Vec<bool> {
        (0..4).map(|i| (base + i) % 3 == 0).collect()
    }

    fn shared_table_cache_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("shared table cache test lock")
    }

    #[test]
    fn tile_element_sizes() {
        assert_eq!(tile_element_size(CasacoreDataType::TpBool), 1);
        assert_eq!(tile_element_size(CasacoreDataType::TpUChar), 1);
        assert_eq!(tile_element_size(CasacoreDataType::TpChar), 1);
        assert_eq!(tile_element_size(CasacoreDataType::TpShort), 2);
        assert_eq!(tile_element_size(CasacoreDataType::TpUShort), 2);
        assert_eq!(tile_element_size(CasacoreDataType::TpInt), 4);
        assert_eq!(tile_element_size(CasacoreDataType::TpUInt), 4);
        assert_eq!(tile_element_size(CasacoreDataType::TpFloat), 4);
        assert_eq!(tile_element_size(CasacoreDataType::TpDouble), 8);
        assert_eq!(tile_element_size(CasacoreDataType::TpInt64), 8);
        assert_eq!(tile_element_size(CasacoreDataType::TpComplex), 8);
        assert_eq!(tile_element_size(CasacoreDataType::TpDComplex), 16);
        // Unsupported types return 0.
        assert_eq!(tile_element_size(CasacoreDataType::TpString), 0);
        assert_eq!(tile_element_size(CasacoreDataType::TpTable), 0);
    }

    #[test]
    fn bool_tile_storage_is_bit_packed() {
        assert_eq!(tile_storage_bytes(CasacoreDataType::TpBool, 4), 1);
        assert_eq!(tile_storage_bytes(CasacoreDataType::TpBool, 9), 2);
    }

    #[test]
    fn table_cache_budget_api_respects_env_and_runtime_override() {
        let _guard = shared_table_cache_test_guard();
        let prior = std::env::var(TABLE_CACHE_BUDGET_ENV).ok();

        unsafe {
            std::env::set_var(TABLE_CACHE_BUDGET_ENV, "8192");
        }
        reset_table_cache_budget_for_tests();
        assert_eq!(table_cache_budget_bytes(), 8192);

        set_table_cache_budget_bytes(4096);
        assert_eq!(table_cache_budget_bytes(), 4096);

        match prior {
            Some(value) => unsafe {
                std::env::set_var(TABLE_CACHE_BUDGET_ENV, value);
            },
            None => unsafe {
                std::env::remove_var(TABLE_CACHE_BUDGET_ENV);
            },
        }
        reset_table_cache_budget_for_tests();
    }

    #[test]
    fn tiled_selected_row_reads_reuse_shared_tile_cache() {
        let _guard = shared_table_cache_test_guard();
        reset_table_cache_budget_for_tests();
        set_table_cache_budget_bytes(128);

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Float32,
            vec![2, 2],
        )])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        for row_idx in 0..6 {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![
                                row_idx as f32,
                                row_idx as f32 + 10.0,
                                row_idx as f32 + 20.0,
                                row_idx as f32 + 30.0,
                            ],
                        )
                        .expect("shape data"),
                    )),
                )]))
                .expect("push row");
        }

        let dir = tempdir().expect("tempdir");
        let root = dir.path().join("shared_tile_cache.table");
        std::fs::create_dir_all(&root).expect("create test dir");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::TiledShapeStMan)
                    .with_tile_shape(vec![2, 2, 2]),
            )
            .expect("save tiled-shape table");

        let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
        assert_eq!(shared_tile_cache_entry_count_for_table(&root), 0);
        reopened
            .get_array_cells_owned("data", &[0, 1, 4, 5])
            .expect("first tiled selected-row read");
        let cache_entries_after_first_read = shared_tile_cache_entry_count_for_table(&root);
        assert!(
            cache_entries_after_first_read > 0,
            "first tiled selected-row read should populate the shared tile cache"
        );

        reopened
            .get_array_cells_owned("data", &[0, 1, 4, 5])
            .expect("second tiled selected-row read");
        assert_eq!(
            shared_tile_cache_entry_count_for_table(&root),
            cache_entries_after_first_read,
            "repeated tiled selected-row reads should reuse the shared tile cache"
        );

        reset_table_cache_budget_for_tests();
    }

    #[test]
    fn bool_lru_eviction_round_trips_after_reopen() {
        let dir = tempdir().unwrap();
        let table_path = dir.path().join("bool_eviction.table");
        std::fs::create_dir_all(&table_path).unwrap();
        let mut io = TiledFileIO::create_with_cache_limit(
            &table_path,
            &[8],
            &[4],
            PrimitiveType::Bool,
            false,
            0,
            "bool_eviction",
            1,
        )
        .unwrap();

        let first_tile = make_bool_tile_pattern(0);
        let second_tile = make_bool_tile_pattern(4);
        io.put_slice_fortran(&first_tile, &[0], &[4]).unwrap();
        io.put_slice_fortran(&second_tile, &[4], &[4]).unwrap();
        io.flush().unwrap();
        drop(io);

        let mut reopened = TiledFileIO::open_with_cache_limit(&table_path, 0, 1).unwrap();
        let all = reopened.get_all::<bool>().unwrap();
        let expected: Vec<bool> = first_tile.into_iter().chain(second_tile).collect();
        assert_eq!(all.iter().copied().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn bool_lru_flush_keeps_tiles_readable_in_same_session() {
        let dir = tempdir().unwrap();
        let table_path = dir.path().join("bool_flush.table");
        std::fs::create_dir_all(&table_path).unwrap();
        let mut io = TiledFileIO::create_with_cache_limit(
            &table_path,
            &[8],
            &[4],
            PrimitiveType::Bool,
            false,
            0,
            "bool_flush",
            1,
        )
        .unwrap();

        let first_tile = make_bool_tile_pattern(0);
        let second_tile = make_bool_tile_pattern(4);
        io.put_slice_fortran(&first_tile, &[0], &[4]).unwrap();
        io.put_slice_fortran(&second_tile, &[4], &[4]).unwrap();

        io.flush().unwrap();

        let all = io.get_all::<bool>().unwrap();
        let expected: Vec<bool> = first_tile.into_iter().chain(second_tile).collect();
        assert_eq!(all.iter().copied().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn bool_flat_flush_keeps_tiles_readable_in_same_session() {
        let dir = tempdir().unwrap();
        let table_path = dir.path().join("bool_flat_flush.table");
        std::fs::create_dir_all(&table_path).unwrap();
        let mut io = TiledFileIO::create(
            &table_path,
            &[8],
            &[4],
            PrimitiveType::Bool,
            false,
            0,
            "bool_flat_flush",
        )
        .unwrap();

        let first_tile = make_bool_tile_pattern(0);
        let second_tile = make_bool_tile_pattern(4);
        io.put_slice_fortran(&first_tile, &[0], &[4]).unwrap();
        io.put_slice_fortran(&second_tile, &[4], &[4]).unwrap();

        io.flush().unwrap();

        let all = io.get_all::<bool>().unwrap();
        let expected: Vec<bool> = first_tile.into_iter().chain(second_tile).collect();
        assert_eq!(all.iter().copied().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn linear_to_nd_round_trip() {
        let dims = [3, 4, 2];
        for i in 0..24 {
            let pos = linear_to_nd(i, &dims);
            assert_eq!(nd_to_linear(&pos, &dims), i);
        }
    }

    #[test]
    fn compute_tile_layout_single_column() {
        let types = [CasacoreDataType::TpFloat];
        let tile = [4, 8];
        let (bucket, offsets) = compute_tile_layout(&types, &tile);
        assert_eq!(offsets[0], 0);
        assert_eq!(bucket, 4 * 8 * 4); // 128 bytes
    }

    #[test]
    fn compute_tile_layout_two_columns() {
        let types = [CasacoreDataType::TpFloat, CasacoreDataType::TpDouble];
        let tile = [2, 3];
        let nrpixels = 6;
        let (bucket, offsets) = compute_tile_layout(&types, &tile);
        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], nrpixels * 4);
        assert_eq!(bucket, nrpixels * 4 + nrpixels * 8);
    }

    #[test]
    fn copy_tile_to_cube_1d() {
        let tile = [1u8, 2, 3, 4];
        let mut cube = [0u8; 6];
        copy_tile_to_cube(&tile, &[4], &mut cube, &[6], &[1], &[4], 1);
        assert_eq!(cube, [0, 1, 2, 3, 4, 0]);
    }

    #[test]
    fn copy_tile_cube_round_trip_2d() {
        // Cube 4x6, tile 3x4.
        let cube_shape = [4, 6];
        let tile_shape = [3, 4];

        // Fill a cube with sequential values.
        let cube_nelem = 24;
        let cube_data: Vec<u8> = (0..cube_nelem as u8).collect();

        // Tile at grid position (0,0) covers cube[0..3, 0..4].
        let mut tile = [0u8; 12]; // 3*4=12
        copy_cube_to_tile(
            &cube_data,
            &cube_shape,
            &mut tile,
            &tile_shape,
            &[0, 0],
            &[3, 4],
            1,
        );

        let mut reconstructed = [0u8; 24];
        copy_tile_to_cube(
            &tile,
            &tile_shape,
            &mut reconstructed,
            &cube_shape,
            &[0, 0],
            &[3, 4],
            1,
        );

        // Only the [0..3, 0..4] region should match.
        for j in 0..4 {
            for i in 0..3 {
                let idx = i + j * 4;
                assert_eq!(reconstructed[idx], cube_data[idx], "mismatch at ({i},{j})");
            }
        }
    }

    #[test]
    fn sparse_partial_save_patches_tiled_data_rows_from_legacy_header() {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Float32,
            vec![2, 2],
        )])
        .expect("schema");

        let mut table = Table::with_schema(schema);
        for row_idx in 0..4 {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![2, 2],
                            vec![
                                row_idx as f32,
                                row_idx as f32 + 10.0,
                                row_idx as f32 + 20.0,
                                row_idx as f32 + 30.0,
                            ],
                        )
                        .expect("shape data"),
                    )),
                )]))
                .expect("push row");
        }

        let dir = tempdir().expect("tempdir");
        let table_path = dir.path().join("tiled-data-compat.table");
        std::fs::create_dir_all(&table_path).expect("create table dir");
        table
            .save(
                TableOptions::new(&table_path).with_data_manager(DataManagerKind::TiledColumnStMan),
            )
            .expect("save tiled-column table");

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        let mut table_dat = read_table_dat(&control_path).expect("read table.dat");
        for dm in &mut table_dat.column_set.data_managers {
            if dm.type_name == "TiledColumnStMan" {
                dm.type_name = "TiledDataStMan".to_string();
            }
        }
        for desc in &mut table_dat.table_desc.columns {
            if desc.data_manager_type == "TiledColumnStMan" {
                desc.data_manager_type = "TiledDataStMan".to_string();
            }
        }
        write_table_dat(&control_path, &table_dat).expect("rewrite table.dat");

        let header_path = table_path.join("table.f0");
        let (variant, header) = read_tiled_header(&header_path).expect("read tiled header");
        let TiledVariant::Column { default_tile_shape } = variant else {
            panic!("expected TiledColumnStMan header");
        };
        write_tiled_header(
            &header_path,
            &TiledVariant::Data {
                default_tile_shape,
                nrrow_last: header.nrrow,
                row_map: vec![0],
                cube_map: vec![0],
                pos_map: vec![0],
            },
            &header,
        )
        .expect("rewrite TiledDataStMan header");

        let mut reopened =
            Table::open(TableOptions::new(&table_path)).expect("open tiled-data table");
        assert_eq!(reopened.data_manager_info()[0].dm_type, "TiledDataStMan");

        reopened
            .column_accessor_mut("data")
            .expect("data column mut")
            .set_array_assuming_valid(
                2,
                ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![2, 2], vec![402.0, 412.0, 422.0, 432.0])
                        .expect("shape updated data"),
                ),
            )
            .expect("set tiled-data array cell lazily");

        reopened
            .save_selected_rows_in_place_assuming_valid(&["data"], &[2])
            .expect("sparse tiled-data partial save");

        let verify = Table::open(TableOptions::new(&table_path)).expect("reopen tiled-data table");
        let data = verify.column_accessor("data").expect("data column");
        assert_eq!(
            data.array_cell(0).expect("data row 0"),
            &ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![0.0, 10.0, 20.0, 30.0]).unwrap()
            )
        );
        assert_eq!(
            data.array_cell(2).expect("data row 2"),
            &ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![402.0, 412.0, 422.0, 432.0]).unwrap()
            )
        );
        assert_eq!(
            data.array_cell(3).expect("data row 3"),
            &ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![2, 2], vec![3.0, 13.0, 23.0, 33.0]).unwrap()
            )
        );
    }
}
