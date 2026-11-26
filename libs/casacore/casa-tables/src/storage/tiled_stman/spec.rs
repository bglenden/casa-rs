// SPDX-License-Identifier: LGPL-3.0-or-later
//! Storage-manager `SPEC` records for `TiledStMan` managers (#1028).

use std::borrow::Cow;

use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use super::super::StorageError;
use super::super::table_control::{ColumnDescContents, TableDatContents};
use super::{TiledVariant, compute_tile_layout, read_tiled_header, tile_columns};

fn i32_vec_value(values: &[impl Copy + TryInto<i32>]) -> Result<Value, StorageError> {
    let mut converted = Vec::with_capacity(values.len());
    for &value in values {
        converted.push(value.try_into().map_err(|_| {
            StorageError::FormatMismatch("tiled spec dimension exceeds i32".into())
        })?);
    }
    let len = converted.len();
    ArrayD::from_shape_vec(IxDyn(&[len]), converted)
        .map(ArrayValue::Int32)
        .map(Value::Array)
        .map_err(|error| StorageError::FormatMismatch(error.to_string()))
}

/// The `SPEC` record of a `TiledStMan` manager (`TiledStMan::dataManagerSpec`).
pub(crate) fn tiled_spec(
    table_path: &std::path::Path,
    dm_seq_nr: u32,
    table_dat: &TableDatContents,
) -> Result<RecordValue, StorageError> {
    let header_path = table_path.join(format!(
        "{}{dm_seq_nr}",
        super::super::TABLE_DATA_FILE_PREFIX
    ));
    let (variant, header) = read_tiled_header(&header_path)?;
    let bound_cols: Vec<&ColumnDescContents> = table_dat
        .column_set
        .columns
        .iter()
        .enumerate()
        .filter(|(_, pc)| pc.dm_seq_nr == dm_seq_nr)
        .filter_map(|(index, _)| table_dat.table_desc.columns.get(index))
        .collect();
    let tiles = tile_columns(
        &variant,
        Cow::Owned(header),
        &bound_cols,
        &table_dat.table_desc,
        0,
    )?;
    let header = &*tiles.header;
    let first_data_col = bound_cols
        .iter()
        .zip(&tiles.tile_index)
        .find_map(|(col, index)| index.map(|_| *col));

    let mut hypercubes = Vec::with_capacity(header.cubes.len());
    for (index, cube) in header
        .cubes
        .iter()
        .filter(|cube| !cube.cube_shape.is_empty())
        .enumerate()
    {
        let bucket_size = compute_tile_layout(&header.col_data_types, &cube.tile_shape).0;
        let bucket_size = i32::try_from(bucket_size)
            .map_err(|_| StorageError::FormatMismatch("tiled bucket size exceeds i32".into()))?;
        let cell_ndim = match first_data_col {
            Some(col) if !col.is_array => 0,
            Some(col) if col.nrdim > 0 => col.nrdim as usize,
            _ if matches!(variant, TiledVariant::Cell { .. }) => cube.cube_shape.len(),
            _ => cube.cube_shape.len().saturating_sub(1),
        };
        let cell_shape = &cube.cube_shape[..cell_ndim.min(cube.cube_shape.len())];
        hypercubes.push(RecordField::new(
            format!("*{}", index + 1),
            Value::Record(RecordValue::new(vec![
                RecordField::new("BucketSize", Value::Scalar(ScalarValue::Int32(bucket_size))),
                RecordField::new("CellShape", i32_vec_value(cell_shape)?),
                RecordField::new("CubeShape", i32_vec_value(&cube.cube_shape)?),
                RecordField::new("ID", Value::Record(cube.values.clone())),
                RecordField::new("TileShape", i32_vec_value(&cube.tile_shape)?),
            ])),
        ));
    }

    let max_cache_size = i32::try_from(header.max_cache_size)
        .map_err(|_| StorageError::FormatMismatch("tiled cache size exceeds i32".into()))?;
    let seq_nr = i32::try_from(dm_seq_nr)
        .map_err(|_| StorageError::FormatMismatch("tiled manager seq_nr exceeds i32".into()))?;
    let mut fields = vec![
        RecordField::new(
            "DEFAULTTILESHAPE",
            i32_vec_value(variant.default_tile_shape())?,
        ),
        RecordField::new("HYPERCUBES", Value::Record(RecordValue::new(hypercubes))),
    ];
    if let TiledVariant::Shape {
        nr_used_row_map, ..
    } = variant
    {
        let index_size = i32::try_from(nr_used_row_map)
            .map_err(|_| StorageError::FormatMismatch("tiled index size exceeds i32".into()))?;
        fields.push(RecordField::new(
            "IndexSize",
            Value::Scalar(ScalarValue::Int32(index_size)),
        ));
    }
    fields.push(RecordField::new(
        "MAXIMUMCACHESIZE",
        Value::Scalar(ScalarValue::Int32(max_cache_size)),
    ));
    fields.push(RecordField::new(
        "MaxCacheSize",
        Value::Scalar(ScalarValue::Int32(max_cache_size)),
    ));
    fields.push(RecordField::new(
        "SEQNR",
        Value::Scalar(ScalarValue::Int32(seq_nr)),
    ));
    Ok(RecordValue::new(fields))
}
