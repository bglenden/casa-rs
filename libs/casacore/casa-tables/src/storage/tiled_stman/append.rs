// SPDX-License-Identifier: LGPL-3.0-or-later
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, PrimitiveType, RecordValue, Value};

use crate::storage::table_control::ColumnDescContents;

use super::{
    StorageError, TiledVariant, TsmCubeInfo, compute_tile_layout, read_tiled_header, tsm_data_path,
    write_tiled_header,
};

/// Stage an updated header; a fixed tiled column can extend its existing
/// file at the end, retaining all existing tiles. Variable shape tables
/// have no cube allocation for rows whose shape hasn't been set.
pub(in crate::storage) fn stage_append(
    path: &Path,
    destination: &Path,
    seq: u32,
    old_rows: u64,
    new_rows: u64,
    metadata: (&[ColumnDescContents], &RecordValue),
) -> Result<Option<(PathBuf, u64)>, StorageError> {
    let (columns, private_keywords) = metadata;
    let fixed_shape = columns
        .iter()
        .any(|column| column.is_array && !column.shape.is_empty());
    let name = format!("table.f{seq}");
    let (variant, mut header) = read_tiled_header(&path.join(&name))?;
    let invalid = || {
        StorageError::UnsupportedDataManager(format!(
            "{} has no supported append layout",
            variant.manager_name()
        ))
    };
    if header.nrrow != old_rows {
        return Err(invalid());
    }
    let extension = match &variant {
        TiledVariant::Shape { .. } if !fixed_shape => None,
        TiledVariant::Data { .. } => None,
        TiledVariant::Cell { .. } if !fixed_shape => {
            let new_rows = usize::try_from(new_rows).map_err(|_| invalid())?;
            header.cubes.resize_with(new_rows, || TsmCubeInfo {
                values: Default::default(),
                extensible: false,
                cube_shape: Vec::new(),
                tile_shape: Vec::new(),
                file_seq_nr: -1,
                file_offset: 0,
            });
            None
        }
        TiledVariant::Column { .. } => {
            let [cube] = header.cubes.as_mut_slice() else {
                return Err(invalid());
            };
            if !cube.extensible
                || cube.file_seq_nr < 0
                || cube.cube_shape.len() != cube.tile_shape.len()
                || cube.tile_shape.contains(&0)
            {
                return Err(invalid());
            }
            let tiles = |shape: &[usize]| {
                shape
                    .iter()
                    .zip(&cube.tile_shape)
                    .try_fold(1u64, |count, (length, tile)| {
                        count.checked_mul(length.div_ceil(*tile) as u64)
                    })
            };
            let old_tiles = tiles(&cube.cube_shape).ok_or_else(invalid)?;
            let last = cube.cube_shape.last_mut().ok_or_else(invalid)?;
            if *last as u64 != old_rows {
                return Err(invalid());
            }
            *last = usize::try_from(new_rows).map_err(|_| invalid())?;
            if let Some(coordinate) = row_coordinate(private_keywords, &header.hypercolumn_name) {
                let column = columns
                    .iter()
                    .find(|column| column.col_name == coordinate)
                    .ok_or_else(invalid)?;
                let values = extend_coordinate(
                    cube.values.get(coordinate),
                    column.require_primitive_type()?,
                    *last,
                )?;
                cube.values.upsert(coordinate, values);
            }
            let new_tiles = tiles(&cube.cube_shape).ok_or_else(invalid)?;
            if new_tiles == old_tiles {
                None
            } else {
                let file_id = cube.file_seq_nr as u32;
                let file = header
                    .files
                    .iter_mut()
                    .flatten()
                    .find(|file| file.seq_nr == file_id)
                    .ok_or_else(invalid)?;
                // The header lists every bound column, including coordinates.
                // Only the data columns occupy bytes inside each tile.
                let bound: Vec<&str> = columns
                    .iter()
                    .map(|column| column.col_name.as_str())
                    .collect();
                let types: Vec<_> = super::tile_columns::tile_order(
                    &header.hypercolumn_name,
                    &bound,
                    &header.col_data_types,
                    private_keywords,
                )?
                .into_iter()
                .map(|index| header.col_data_types[index])
                .collect();
                let tile_bytes = compute_tile_layout(&types, &cube.tile_shape).0 as u64;
                // Extending a cube inside an existing tile file may not
                // overwrite another cube's allocation.
                let old_end = old_tiles
                    .checked_mul(tile_bytes)
                    .and_then(|bytes| bytes.checked_add(cube.file_offset as u64))
                    .ok_or_else(invalid)?;
                if old_end != file.length as u64 {
                    return Err(invalid());
                }
                let len = new_tiles
                    .checked_mul(tile_bytes)
                    .and_then(|bytes| bytes.checked_add(cube.file_offset as u64))
                    .ok_or_else(invalid)?;
                file.length = i64::try_from(len).map_err(|_| invalid())?;
                Some((tsm_data_path(path, seq, file_id), len))
            }
        }
        _ => return Err(invalid()),
    };
    header.nrrow = new_rows;
    write_tiled_header(&destination.join(name), &variant, &header)?;
    Ok(extension)
}

fn row_coordinate<'a>(keywords: &'a RecordValue, group: &str) -> Option<&'a str> {
    let Value::Record(hypercolumn) = keywords.get(format!("Hypercolumn_{group}").as_str())? else {
        return None;
    };
    match hypercolumn.get("coord")? {
        Value::Array(ArrayValue::String(columns)) => columns
            .iter()
            .last()
            .map(String::as_str)
            .filter(|name| !name.is_empty()),
        _ => None,
    }
}

/// TSMCube::extendCoordinates with no explicitly supplied new coordinates.
fn extend_coordinate(
    value: Option<&Value>,
    kind: PrimitiveType,
    count: usize,
) -> Result<Value, StorageError> {
    let invalid = || {
        StorageError::FormatMismatch(format!(
            "invalid row coordinate in TiledColumnStMan for {kind:?}"
        ))
    };
    macro_rules! extend {
        ($kind:ident) => {{
            let mut cells = match value {
                Some(Value::Array(ArrayValue::$kind(array)))
                    if array.ndim() == 1 && array.len() <= count =>
                {
                    array.iter().cloned().collect::<Vec<_>>()
                }
                None => Vec::new(),
                _ => return Err(invalid()),
            };
            cells.resize_with(count, Default::default);
            ArrayValue::$kind(ndarray::Array1::from_vec(cells).into_dyn())
        }};
    }
    Ok(Value::Array(match kind {
        PrimitiveType::Bool => extend!(Bool),
        PrimitiveType::Int32 => extend!(Int32),
        PrimitiveType::UInt32 => extend!(UInt32),
        PrimitiveType::Float32 => extend!(Float32),
        PrimitiveType::Float64 => extend!(Float64),
        PrimitiveType::Complex32 => extend!(Complex32),
        PrimitiveType::Complex64 => extend!(Complex64),
        PrimitiveType::String => extend!(String),
        _ => return Err(invalid()),
    }))
}

/// Keep the source manager's cache in its replacement header, or apply a
/// supplied setting to a newly built manager.
pub(in crate::storage) fn set_cache(path: &Path, cache_size: u64) -> Result<(), StorageError> {
    let (variant, mut header) = read_tiled_header(path)?;
    header.max_cache_size = cache_size;
    std::fs::write(path, super::serialize_tiled_header(&variant, &header)?)?;
    Ok(())
}

pub(in crate::storage) fn cache_size(path: &Path) -> Result<u64, StorageError> {
    Ok(read_tiled_header(path)?.1.max_cache_size)
}

pub(in crate::storage) fn manager_name(path: &Path) -> Result<String, StorageError> {
    Ok(read_tiled_header(path)?.1.hypercolumn_name)
}
