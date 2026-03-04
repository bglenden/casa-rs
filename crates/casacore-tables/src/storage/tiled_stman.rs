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

use std::path::Path;

use casacore_aipsio::{AipsIo, AipsOpenOption};
use casacore_types::{ArrayValue, Complex32, Complex64, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::StorageError;
use super::canonical::*;
use super::data_type::CasacoreDataType;
use super::table_control::{
    ColumnDescContents, DataManagerEntry, read_iposition, read_record, write_iposition,
    write_record,
};

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
}

// ---------------------------------------------------------------------------
// Tile element size (different from SSM canonical size)
// ---------------------------------------------------------------------------

/// Returns the external (on-disk) element size in bytes for tile data.
///
/// Unlike SSM bit-packing, tiled storage uses one full byte per Bool.
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

/// Compute tile layout: per-column offsets within a tile and total bucket size.
///
/// C++ casacore sorts columns by descending pixel size for alignment, then
/// assigns sequential offsets within the tile.
fn compute_tile_layout(
    col_data_types: &[CasacoreDataType],
    tile_shape: &[usize],
) -> (usize, Vec<usize>) {
    let nrpixels: usize = tile_shape.iter().product();

    let pixel_sizes: Vec<usize> = col_data_types
        .iter()
        .map(|dt| tile_element_size(*dt))
        .collect();

    // Sort column indices by descending pixel size (stable for determinism).
    let mut indices: Vec<usize> = (0..col_data_types.len()).collect();
    indices.sort_by(|&a, &b| pixel_sizes[b].cmp(&pixel_sizes[a]));

    let mut offsets = vec![0usize; col_data_types.len()];
    let mut offset = 0usize;
    for &col in &indices {
        offsets[col] = offset;
        offset += nrpixels * pixel_sizes[col];
    }

    (offset, offsets)
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
    let _outer_version = io.getstart(&outer_type)?;

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
fn reconstruct_cube_column(
    file_data: &[u8],
    file_offset: usize,
    cube_shape: &[usize],
    tile_shape: &[usize],
    col_offset_in_tile: usize,
    bucket_size: usize,
    elem_size: usize,
) -> Result<Vec<u8>, StorageError> {
    let ndim = cube_shape.len();
    let nrpixels: usize = tile_shape.iter().product();
    let col_bytes_per_tile = nrpixels * elem_size;

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
        let tile_bytes = &file_data[tile_data_start..tile_data_end];

        copy_tile_to_cube(
            tile_bytes,
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

    // IMPORTANT: We must iterate in memory (Fortran/column-major) order, not
    // the default logical (C/row-major) order that ndarray's `.iter()` uses.
    // Tile data is stored in column-major order, matching the array's memory layout.
    match arr {
        ArrayValue::Bool(a) => {
            let slice = a.as_slice_memory_order().expect("contiguous array");
            let data: Vec<u8> = slice.iter().map(|&b| if b { 1u8 } else { 0u8 }).collect();
            Ok((data, CasacoreDataType::TpBool))
        }
        ArrayValue::UInt8(a) => {
            let data: Vec<u8> = a
                .as_slice_memory_order()
                .expect("contiguous array")
                .to_vec();
            Ok((data, CasacoreDataType::TpUChar))
        }
        ArrayValue::Int16(a) => {
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
            let slice = a.as_slice_memory_order().expect("contiguous array");
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
        TiledVariant::Column { .. } => {
            load_tiled_column_stman(table_path, dm.seq_nr, &header, &col_descs, rows, nrrow)
        }
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
            nrrow,
            &ShapeRowMapping {
                nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
        ),
        TiledVariant::Cell { .. } => {
            load_tiled_cell_stman(table_path, dm.seq_nr, &header, &col_descs, rows, nrrow)
        }
    }
}

/// Load columns from a `TiledColumnStMan` (single hypercube for all rows).
fn load_tiled_column_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
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

/// Row-to-cube mapping tables extracted from `TiledShapeStMan` variant data.
struct ShapeRowMapping<'a> {
    nr_used_row_map: u32,
    row_map: &'a [u32],
    cube_map: &'a [u32],
    pos_map: &'a [u32],
}

/// Load columns from a `TiledShapeStMan` (one hypercube per unique shape).
fn load_tiled_shape_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
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
    for (row_idx, row) in rows.iter_mut().enumerate().take(nrrow) {
        // Find the interval containing this row.
        let interval = mapping.row_map[..n_intervals]
            .iter()
            .position(|&rm| rm >= row_idx as u32)
            .unwrap_or(n_intervals - 1);

        let cube_idx = mapping.cube_map[interval] as usize;
        let pos_in_cube =
            mapping.pos_map[interval] as usize - (mapping.row_map[interval] as usize - row_idx);

        if cube_idx >= cube_col_data.len() {
            continue; // undefined cell (cube 0 is dummy)
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

/// Load columns from a `TiledCellStMan` (one hypercube per row).
fn load_tiled_cell_stman(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    col_descs: &[&ColumnDescContents],
    rows: &mut [RecordValue],
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
                elem_size,
            )?;
            let value = decode_array_value(&raw, cell_shape, dt, header.big_endian)?;
            rows[row_idx].push(RecordField::new(col_desc.col_name.clone(), value));
        }
    }

    Ok(())
}

/// Construct the path to a TSM data file.
fn tsm_data_path(table_path: &Path, dm_seq_nr: u32, file_seq_nr: u32) -> std::path::PathBuf {
    table_path.join(format!("table.f{dm_seq_nr}_TSM{file_seq_nr}"))
}

// ---------------------------------------------------------------------------
// Save interface (write columns with tiled DM)
// ---------------------------------------------------------------------------

/// Save columns to a tiled storage manager.
///
/// Determines the appropriate variant from the `DataManagerKind` and writes
/// header + tile data files.
pub(crate) fn save_tiled_columns(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_type_name: &str,
    all_col_descs: &[ColumnDescContents],
    rows: &[RecordValue],
    big_endian: bool,
    default_tile_shape: Option<&[usize]>,
) -> Result<(), StorageError> {
    match dm_type_name {
        "TiledColumnStMan" => save_tiled_column_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
        ),
        "TiledShapeStMan" => save_tiled_shape_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
        ),
        "TiledCellStMan" => save_tiled_cell_stman(
            table_path,
            dm_seq_nr,
            all_col_descs,
            rows,
            big_endian,
            default_tile_shape,
        ),
        other => Err(StorageError::FormatMismatch(format!(
            "unknown tiled DM type: {other}"
        ))),
    }
}

/// Compute a reasonable default tile shape for a given cell shape and row count.
fn default_tile_shape_for(cell_shape: &[usize], nrow: usize) -> Vec<usize> {
    // Use the full cell shape with a row tile size that keeps tiles ~32KB.
    let cell_nelem: usize = cell_shape.iter().product();
    let target_elements = 8192; // ~32KB for 4-byte elements
    let row_tile = if cell_nelem > 0 {
        (target_elements / cell_nelem).max(1)
    } else {
        nrow
    };
    let mut shape: Vec<usize> = cell_shape.to_vec();
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
) -> Result<(), StorageError> {
    let nrrow = rows.len();
    if nrrow == 0 || col_descs.is_empty() {
        // Write empty header.
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types: vec![],
            hypercolumn_name: String::new(),
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

    let tile_shape: Vec<usize> = if let Some(ts) = user_tile_shape {
        ts.to_vec()
    } else {
        default_tile_shape_for(&cell_shape, nrrow)
    };

    // Collect column data types.
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| CasacoreDataType::from_primitive_type(c.primitive_type, false))
        .collect();

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
            let col_tile_bytes = nrpixels * elem_size;

            // Extract tile data from cube (reverse of copy_tile_to_cube).
            let mut tile_col = vec![0u8; col_tile_bytes];
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
            tsm_data[dst_start..dst_start + col_tile_bytes].copy_from_slice(&tile_col);
        }
    }

    // Write TSM data file.
    let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
    std::fs::write(&tsm_path, &tsm_data)?;

    // Build and write header.
    let default_ts_i32: Vec<i32> = tile_shape.iter().map(|&v| v as i32).collect();
    let header = TiledStManHeader {
        big_endian,
        seq_nr: dm_seq_nr,
        nrrow: nrrow as u64,
        col_data_types,
        hypercolumn_name: String::new(),
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
) -> Result<(), StorageError> {
    let nrrow = rows.len();
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| CasacoreDataType::from_primitive_type(c.primitive_type, false))
        .collect();

    if nrrow == 0 || col_descs.is_empty() {
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 0,
            col_data_types: col_data_types.clone(),
            hypercolumn_name: String::new(),
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

        let tile_shape = if let Some(ts) = user_tile_shape {
            ts.to_vec()
        } else {
            default_tile_shape_for(cell_shape, n_in_cube)
        };

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
                let col_tile_bytes = nrpixels * elem_size;
                let mut tile_col = vec![0u8; col_tile_bytes];
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
                tsm_data[dst_start..dst_start + col_tile_bytes].copy_from_slice(&tile_col);
            }
        }

        let tsm_path = tsm_data_path(table_path, dm_seq_nr, file_seq_nr);
        std::fs::write(&tsm_path, &tsm_data)?;

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
        hypercolumn_name: String::new(),
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
) -> Result<(), StorageError> {
    let nrrow = rows.len();
    let col_data_types: Vec<CasacoreDataType> = col_descs
        .iter()
        .map(|c| CasacoreDataType::from_primitive_type(c.primitive_type, false))
        .collect();

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
        let tile_shape = if let Some(ts) = user_tile_shape {
            // Use only the cell dimensions (no row dim for TiledCellStMan).
            ts[..cell_shape.len().min(ts.len())].to_vec()
        } else {
            // Use full cell shape as tile shape.
            cell_shape.clone()
        };

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

            for (col_idx, col_desc) in col_descs.iter().enumerate() {
                let dt = col_data_types[col_idx];
                let elem_size = tile_element_size(dt);
                let col_tile_bytes = nrpixels * elem_size;

                // Encode this cell's data.
                let mut cube_bytes = vec![0u8; cell_nelem * elem_size];
                if let Some(value) = row.get(&col_desc.col_name) {
                    let (encoded, _) = encode_array_value(value, big_endian)?;
                    if encoded.len() == cube_bytes.len() {
                        cube_bytes = encoded;
                    }
                }

                let mut tile_col = vec![0u8; col_tile_bytes];
                copy_cube_to_tile(
                    &cube_bytes,
                    &cube_shape,
                    &mut tile_col,
                    &tile_shape,
                    &cube_start,
                    &actual_extent,
                    elem_size,
                );
                let dst_start = tile_idx * bucket_size + col_offsets[col_idx];
                tile_data[dst_start..dst_start + col_tile_bytes].copy_from_slice(&tile_col);
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
        hypercolumn_name: String::new(),
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
        // Float32 (4 bytes) and Double (8 bytes).
        // Double should come first (larger pixel size).
        let types = [CasacoreDataType::TpFloat, CasacoreDataType::TpDouble];
        let tile = [2, 3];
        let nrpixels = 6;
        let (bucket, offsets) = compute_tile_layout(&types, &tile);
        // Double at offset 0, Float at offset 6*8=48.
        assert_eq!(offsets[1], 0); // Double
        assert_eq!(offsets[0], nrpixels * 8); // Float after Double
        assert_eq!(bucket, nrpixels * 8 + nrpixels * 4);
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
}
