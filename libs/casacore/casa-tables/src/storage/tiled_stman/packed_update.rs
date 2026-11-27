// SPDX-License-Identifier: LGPL-3.0-or-later
//! Replace consecutive packed array cells in whole TiledShapeStMan bands.

use std::ops::Range;

use super::*;

pub(crate) struct PackedArrayUpdate {
    file: File,
    offset: u64,
    cell_shape: [usize; 2],
    tile_shape: [usize; 3],
    data_type: CasacoreDataType,
    swap_bytes: usize,
}

pub(crate) fn prepare_packed_array_update(
    path: &Path,
    seq_nr: u32,
    rows: Range<usize>,
    cell_shape: [usize; 2],
    data_type: CasacoreDataType,
) -> Result<Option<PackedArrayUpdate>, StorageError> {
    let header_path = path.join(format!("table.f{seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    let TiledVariant::Shape {
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
        ..
    } = &variant
    else {
        return Ok(None);
    };
    if header.col_data_types != [data_type] || rows.end > u32::MAX as usize {
        return Ok(None);
    }
    let mapping = ShapeRowMapping {
        nr_used_row_map: *nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
    };
    let Some((cube_index, first)) = shape_row_location(&header, &mapping, rows.start)? else {
        return Ok(None);
    };
    let cube = &header.cubes[cube_index];
    let Ok(tile_shape) = <[usize; 3]>::try_from(cube.tile_shape.as_slice()) else {
        return Ok(None);
    };
    let Some(&cube_rows) = cube.cube_shape.get(2) else {
        return Ok(None);
    };
    let Some(last) = first.checked_add(rows.len()) else {
        return Ok(None);
    };
    if cube.cube_shape.len() != 3
        || cube.cube_shape[..2] != cell_shape
        || tile_shape.contains(&0)
        || cube.file_seq_nr < 0
        || cube.file_offset < 0
        || !first.is_multiple_of(tile_shape[2])
        || (last != cube_rows && !last.is_multiple_of(tile_shape[2]))
        || !cell_shape[0].is_multiple_of(tile_shape[0])
        || !cell_shape[1].is_multiple_of(tile_shape[1])
        || last > cube_rows
    {
        return Ok(None);
    }
    for (index, row) in rows.clone().enumerate() {
        if shape_row_location(&header, &mapping, row)? != Some((cube_index, first + index)) {
            return Ok(None);
        }
    }

    let tile_rows = tile_shape[2];
    checked_storage_product(
        &[
            cell_shape[0],
            cell_shape[1],
            tile_rows,
            tile_element_size(data_type),
        ],
        "unpacked tile band bytes",
    )?;
    let tiles_per_band = [cell_shape[0] / tile_shape[0], cell_shape[1] / tile_shape[1]];
    let tile_bytes = checked_tile_storage_bytes(data_type, &tile_shape)?;
    let band_bytes = checked_storage_product(
        &[tile_bytes, tiles_per_band[0], tiles_per_band[1]],
        "packed tile band bytes",
    )?;
    let base = cube.file_offset as u64;
    let start = base
        .checked_add(
            checked_storage_product(&[first / tile_rows, band_bytes], "tile offset")? as u64,
        );
    let end = base.checked_add(checked_storage_product(
        &[last.div_ceil(tile_rows), band_bytes],
        "tile extent",
    )? as u64);
    let (Some(offset), Some(end)) = (start, end) else {
        return Ok(None);
    };
    let file_seq = cube.file_seq_nr as u32;
    validate_cube_allocation(&header, cube_index, file_seq, data_type)?;
    let data = tsm_data_path(path, seq_nr, file_seq);
    let file = OpenOptions::new().read(true).write(true).open(data)?;
    if file.metadata()?.len() < end {
        return Err(StorageError::FormatMismatch(
            "packed tile update exceeds physical file length".into(),
        ));
    }
    Ok(Some(PackedArrayUpdate {
        file,
        offset,
        cell_shape,
        tile_shape,
        data_type,
        swap_bytes: if header.big_endian != cfg!(target_endian = "big")
            && data_type != CasacoreDataType::TpBool
        {
            size_of::<f32>()
        } else {
            1
        },
    }))
}

fn validate_cube_allocation(
    header: &TiledStManHeader,
    cube_index: usize,
    seq_nr: u32,
    data_type: CasacoreDataType,
) -> Result<(), StorageError> {
    let stored_len = header
        .files
        .iter()
        .flatten()
        .find(|file| file.seq_nr == seq_nr)
        .and_then(|file| u64::try_from(file.length).ok())
        .ok_or_else(|| StorageError::FormatMismatch("missing declared tiled file length".into()))?;
    let target = cube_extent(&header.cubes[cube_index], data_type)?;
    if target.end > stored_len {
        return Err(StorageError::FormatMismatch(
            "tiled cube exceeds declared file length".into(),
        ));
    }
    for (i, other) in header.cubes.iter().enumerate() {
        if i == cube_index || other.file_seq_nr as i64 != i64::from(seq_nr) {
            continue;
        }
        let other = cube_extent(other, data_type)?;
        if target.start < other.end && other.start < target.end {
            return Err(StorageError::FormatMismatch(
                "overlapping tiled cube allocations".into(),
            ));
        }
    }
    Ok(())
}

fn cube_extent(
    cube: &TsmCubeInfo,
    data_type: CasacoreDataType,
) -> Result<Range<u64>, StorageError> {
    check_tile_rank(cube)?;
    let invalid = || StorageError::FormatMismatch("invalid tiled cube allocation".into());
    let start = u64::try_from(cube.file_offset).map_err(|_| invalid())?;
    let mut bytes = checked_tile_storage_bytes(data_type, &cube.tile_shape)? as u64;
    for (&n, &tile) in cube.cube_shape.iter().zip(&cube.tile_shape) {
        if tile == 0 {
            return Err(invalid());
        }
        bytes = bytes
            .checked_mul(n.div_ceil(tile) as u64)
            .ok_or_else(invalid)?;
    }
    Ok(start..start.checked_add(bytes).ok_or_else(invalid)?)
}

fn checked_tile_storage_bytes(
    data_type: CasacoreDataType,
    shape: &[usize],
) -> Result<usize, StorageError> {
    let pixels = checked_storage_product(shape, "packed tile elements")?;
    if data_type == CasacoreDataType::TpBool {
        Ok(pixels.div_ceil(8))
    } else {
        checked_storage_product(&[pixels, tile_element_size(data_type)], "packed tile bytes")
    }
}

impl PackedArrayUpdate {
    pub(crate) fn write<T: TilePixel>(mut self, values: &[T]) -> Result<(), StorageError> {
        let [width, channels] = self.cell_shape;
        let [tile_width, tile_channels, tile_rows] = self.tile_shape;
        let elem_bytes = T::ELEM_SIZE;
        let row_bytes = width * channels * elem_bytes;
        let tile_row_bytes = tile_width * tile_channels * elem_bytes;
        let mut tile = vec![0; tile_rows * tile_row_bytes];
        let bool_tile = self.data_type == CasacoreDataType::TpBool;
        let mut packed = if bool_tile {
            vec![0; tile.len().div_ceil(8)]
        } else {
            Vec::new()
        };

        self.file.seek(SeekFrom::Start(self.offset))?;
        for band in tile_values_as_bytes(values).chunks(row_bytes * tile_rows) {
            let rows = band.len() / row_bytes;
            let partial = rows < tile_rows;
            for channel in (0..channels).step_by(tile_channels) {
                for sample in (0..width).step_by(tile_width) {
                    let bytes = if bool_tile { &mut packed } else { &mut tile };
                    if partial {
                        self.file.read_exact(bytes)?;
                        self.file.seek_relative(-(bytes.len() as i64))?;
                    }
                    let row_offset = (channel * width + sample) * elem_bytes;
                    for (src, dst) in band
                        .chunks_exact(row_bytes)
                        .zip(tile.chunks_exact_mut(tile_row_bytes))
                    {
                        if tile_width == width {
                            dst.copy_from_slice(&src[row_offset..row_offset + tile_row_bytes]);
                        } else {
                            copy_cube_to_tile(
                                src,
                                &self.cell_shape,
                                dst,
                                &[tile_width, tile_channels],
                                &[sample, channel],
                                &[tile_width, tile_channels],
                                elem_bytes,
                            );
                        }
                    }
                    let new_bytes = &mut tile[..rows * tile_row_bytes];
                    if bool_tile {
                        write_bool_bits_from_bytes(&mut packed, 0, new_bytes);
                        self.file.write_all(&packed)?;
                    } else {
                        if self.swap_bytes > 1 {
                            swap_bytes_inplace(new_bytes, self.swap_bytes);
                        }
                        self.file.write_all(&tile)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_cube_allocation_refuses_before_opening_the_payload() {
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path();
        let cube = TsmCubeInfo {
            values: RecordValue::default(),
            extensible: false,
            cube_shape: vec![2, 3, 2],
            tile_shape: vec![2, 3, 2],
            file_seq_nr: 0,
            file_offset: 0,
        };
        let header = TiledStManHeader {
            big_endian: false,
            seq_nr: 0,
            nrrow: 4,
            col_data_types: vec![CasacoreDataType::TpComplex],
            hypercolumn_name: String::new(),
            max_cache_size: 0,
            nrdim: 3,
            files: vec![Some(TsmFileInfo {
                seq_nr: 0,
                length: 192,
            })],
            cubes: vec![
                TsmCubeInfo {
                    file_seq_nr: -1,
                    ..cube.clone()
                },
                cube.clone(),
                TsmCubeInfo {
                    file_offset: 48,
                    ..cube
                },
            ],
        };
        let variant = TiledVariant::Shape {
            default_tile_shape: vec![2, 3, 2],
            nr_used_row_map: 2,
            row_map: vec![1, 3],
            cube_map: vec![1, 2],
            pos_map: vec![1, 1],
        };
        write_tiled_header(&path.join("table.f0"), &variant, &header).unwrap();
        let payload = tsm_data_path(path, 0, 0);
        let before = [0x41; 192];
        std::fs::write(&payload, before).unwrap();
        assert!(matches!(
            prepare_packed_array_update(path, 0, 0..2, [2, 3], CasacoreDataType::TpComplex),
            Err(StorageError::FormatMismatch(message)) if message.contains("overlapping")
        ));
        assert_eq!(std::fs::read(payload).unwrap(), before);
    }
}
