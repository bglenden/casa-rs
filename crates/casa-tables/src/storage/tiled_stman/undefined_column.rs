// SPDX-License-Identifier: LGPL-3.0-or-later
//! A `TiledShapeStMan` column whose every cell is undefined.
//!
//! casacore gives a `TiledShapeStMan` column a hypercube only when a row is
//! given a shape. A column no row has written, such as the FLAG_CATEGORY of a
//! MeasurementSet from CASA's simulator, has only the dummy hypercube 0, no
//! data file and an empty row map, and every cell reads as undefined.

use std::path::Path;

use casa_types::RecordValue;

use super::{
    StreamedTiledPrimitiveType, TiledStManHeader, TiledVariant, TsmCubeInfo,
    invalidate_shared_tile_cache_for_table, write_tiled_header,
};
use crate::storage::StorageError;

/// Install `dm_name` as a `TiledShapeStMan` column of `row_count` undefined
/// cells of rank `cell_rank`, laid out as casacore leaves a column no row
/// has written.
///
/// `default_tile_shape` (cell axes then rows) is the tile shape casacore
/// would give a hypercube created later, for example when a row is given a
/// shape.
///
/// # Errors
///
/// A tile shape whose rank is not `cell_rank + 1`, or a failure to write the
/// header.
pub fn install_undefined_tiled_shape_column(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_name: &str,
    row_count: usize,
    value_type: StreamedTiledPrimitiveType,
    cell_rank: usize,
    default_tile_shape: &[usize],
) -> Result<(), StorageError> {
    if default_tile_shape.len() != cell_rank + 1 {
        return Err(StorageError::FormatMismatch(format!(
            "default tile shape {default_tile_shape:?} does not fit cells of rank {cell_rank}"
        )));
    }
    std::fs::create_dir_all(table_path)?;
    let header = TiledStManHeader {
        big_endian: cfg!(target_endian = "big"),
        seq_nr: dm_seq_nr,
        nrrow: row_count as u64,
        col_data_types: vec![value_type.casacore_type()],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim: (cell_rank + 1) as u32,
        files: Vec::new(),
        cubes: vec![TsmCubeInfo {
            values: RecordValue::default(),
            extensible: false,
            cube_shape: Vec::new(),
            tile_shape: Vec::new(),
            file_seq_nr: -1,
            file_offset: 0,
        }],
    };
    let variant = TiledVariant::Shape {
        default_tile_shape: default_tile_shape
            .iter()
            .map(|&extent| extent as i32)
            .collect(),
        nr_used_row_map: 0,
        row_map: Vec::new(),
        cube_map: Vec::new(),
        pos_map: Vec::new(),
    };
    write_tiled_header(
        &table_path.join(format!("table.f{dm_seq_nr}")),
        &variant,
        &header,
    )?;
    invalidate_shared_tile_cache_for_table(table_path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::read_tiled_header;
    use super::*;

    #[test]
    fn an_undefined_column_has_only_the_dummy_cube_and_no_row_map() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let table_path = directory.path().join("main.table");
        install_undefined_tiled_shape_column(
            &table_path,
            3,
            "FLAG_CATEGORY",
            7,
            StreamedTiledPrimitiveType::Bool,
            3,
            &[2, 64, 1, 1024],
        )
        .expect("install");
        let (variant, header) =
            read_tiled_header(&table_path.join("table.f3")).expect("read header");
        assert_eq!(header.nrrow, 7);
        assert_eq!(header.nrdim, 4);
        assert!(header.files.is_empty());
        assert_eq!(header.cubes.len(), 1);
        assert!(header.cubes[0].cube_shape.is_empty());
        let TiledVariant::Shape {
            default_tile_shape,
            nr_used_row_map,
            row_map,
            ..
        } = variant
        else {
            panic!("expected a TiledShapeStMan header");
        };
        assert_eq!(default_tile_shape, vec![2, 64, 1, 1024]);
        assert_eq!(nr_used_row_map, 0);
        assert!(row_map.is_empty());
        assert!(
            !table_path.join("table.f3_TSM0").exists(),
            "an undefined column has no data file"
        );
    }

    #[test]
    fn a_tile_shape_of_the_wrong_rank_is_refused() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let error = install_undefined_tiled_shape_column(
            directory.path(),
            0,
            "FLAG_CATEGORY",
            1,
            StreamedTiledPrimitiveType::Bool,
            3,
            &[2, 64, 1024],
        )
        .expect_err("rank mismatch");
        assert!(error.to_string().contains("rank 3"), "{error}");
    }
}
