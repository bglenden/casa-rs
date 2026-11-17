// SPDX-License-Identifier: LGPL-3.0-or-later
use std::path::{Path, PathBuf};

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
    fixed_shape: bool,
) -> Result<Option<(PathBuf, u64)>, StorageError> {
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
                || !cube.values.fields().is_empty()
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
                let tile_bytes =
                    compute_tile_layout(&header.col_data_types, &cube.tile_shape).0 as u64;
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
