// SPDX-License-Identifier: LGPL-3.0-or-later
//! Which of a tiled manager's bound columns its tiles hold, and in which
//! order.
//!
//! A hypercolumn has data, coordinate and id columns, and all of them are
//! bound to its tiled manager. Only the data columns are stored in the
//! tiles; each hypercube keeps its id values and coordinate vectors in its
//! values record. The data columns are those the hypercolumn definition in
//! the table description lists, in its order; without a definition, every
//! bound column is a data column, in binding (table description) order.
//!
//! casacore lays a tile out over the data columns by descending tile pixel
//! size, with Bool (bit-packed) last. Its sort is not stable, whatever
//! `TiledStMan::setup` says: `GenSortIndirect` sorts ascending, breaking
//! ties by position, and then reverses the whole result for a descending
//! sort. Columns of equal pixel size therefore come out in descending
//! position in the data column list.
//!
//! casa-rs reads and writes every tiled manager's tiles this way (#730,
//! #755). Earlier casa-rs wrote a hypercolumn of several array columns in
//! binding order; such a table of mixed pixel sizes is not read compatibly
//! and needs rewriting, and casacore never could read it. casacore allows
//! id columns only in a `TiledDataStMan`; the other managers may have
//! coordinate columns, which casa-rs refuses to read.
//!
//! # C++ reference
//!
//! `TiledStMan::setup` (the data columns and the sort),
//! `GenSortIndirect::insSort` and `isAscending` (the tie order),
//! `TiledStMan::getLengthOffset` (the layout), `TSMDataColumn::TSMDataColumn`
//! (the pixel sizes), `TableDesc::hypercolumnDesc` (the definition, private
//! keyword `Hypercolumn_<name>`) and `ColumnSet::getFile` (binding order).

use std::borrow::Cow;
use std::cmp::Reverse;

use casa_types::{ArrayValue, RecordValue, Value};

use super::super::StorageError;
use super::super::data_type::CasacoreDataType;
use super::super::table_control::{ColumnDescContents, PlainColumnEntry, TableDescContents};
use super::{TiledStManHeader, TiledVariant, TsmCubeInfo, compute_tile_layout, tile_element_size};

/// Private keyword prefix of a hypercolumn definition (`theHyperPrefix`).
const HYPERCOLUMN_PREFIX: &str = "Hypercolumn_";

/// The tile columns of one tiled manager.
pub(super) struct TileColumns<'h> {
    /// The manager's header with `col_data_types` holding only the tile
    /// columns, in tile order. Tile layout functions take this header. It
    /// is borrowed when the tile order is the binding order.
    pub(super) header: Cow<'h, TiledStManHeader>,
    /// For each bound column, in binding order, its index among the tile
    /// columns, or `None` for a column kept in the hypercube values record.
    pub(super) tile_index: Vec<Option<usize>>,
}

impl TileColumns<'_> {
    /// The first bound column the tiles do not hold, if any.
    pub(super) fn first_untiled<'d>(
        &self,
        bound: &[&'d ColumnDescContents],
    ) -> Option<&'d ColumnDescContents> {
        self.tile_index
            .iter()
            .position(Option::is_none)
            .map(|column| bound[column])
    }
}

/// Split a tiled manager's bound columns, in binding order, into its tile
/// columns, in tile order, and the columns its hypercube values records
/// hold.
///
/// For a `TiledDataStMan` only: a row maps to the last hypercube axis, so a
/// data column must have exactly one axis fewer than the hypercube.
/// casacore spreads rows over every axis after a data column's cell axes
/// (`TiledDataStMan::getHypercube`), so a hypercolumn with more than one of
/// them is refused, not misread; so are scalar data columns, whose rows
/// casa-rs does not read. These refusals come before any read or write.
///
/// An owned `header` is reordered in place; a borrowed one is cloned only
/// when the tile order is not the binding order.
pub(super) fn tile_columns<'h>(
    variant: &TiledVariant,
    mut header: Cow<'h, TiledStManHeader>,
    bound: &[&ColumnDescContents],
    table_desc: &TableDescContents,
) -> Result<TileColumns<'h>, StorageError> {
    let data = matches!(variant, TiledVariant::Data { .. });
    // Earlier casa-rs wrote a manager with no rows as a header listing no
    // column types and no hypercube, whose tiles hold nothing (#755).
    if !data && header.col_data_types.is_empty() && header.cubes.is_empty() {
        return Ok(TileColumns {
            header,
            tile_index: (0..bound.len()).map(Some).collect(),
        });
    }
    if bound.len() != header.col_data_types.len() {
        return Err(StorageError::FormatMismatch(format!(
            "tiled manager {} binds {} columns but its header lists {}",
            header.seq_nr,
            bound.len(),
            header.col_data_types.len()
        )));
    }
    let names: Vec<&str> = bound.iter().map(|desc| desc.col_name.as_str()).collect();
    let order = tile_order(
        &header.hypercolumn_name,
        &names,
        &header.col_data_types,
        &table_desc.private_keywords,
    )?;
    if data {
        for &column in &order {
            check_data_column_axes(&header, bound[column])?;
        }
    }

    let mut tile_index = vec![None; bound.len()];
    for (tile, &column) in order.iter().enumerate() {
        tile_index[column] = Some(tile);
    }
    if !order.iter().copied().eq(0..bound.len()) {
        let tile_types = order
            .iter()
            .map(|&column| header.col_data_types[column])
            .collect();
        header.to_mut().col_data_types = tile_types;
    }
    Ok(TileColumns { header, tile_index })
}

/// Refuse a `TiledDataStMan` data column whose rows are not one hypercube
/// axis each: a scalar column, one of no fixed dimensionality, or one with
/// more than one row axis.
fn check_data_column_axes(
    header: &TiledStManHeader,
    column: &ColumnDescContents,
) -> Result<(), StorageError> {
    let refusal = match column.nrdim {
        0 => "a scalar data column; scalar tiled data is not supported",
        nrdim if nrdim < 0 => "a data column with no fixed dimensionality, which is not supported",
        nrdim if header.nrdim as i32 != nrdim + 1 => {
            "more than one row axis for its cells, which is not supported"
        }
        _ => return Ok(()),
    };
    Err(StorageError::FormatMismatch(format!(
        "TiledDataStMan hypercolumn {} ({} axes) has, in '{}', {refusal}",
        header.hypercolumn_name, header.nrdim, column.col_name
    )))
}

/// The bound columns in tile order: the descriptors a reader built for
/// binding order, rearranged to match the tile header's `col_data_types`.
/// Call it only when every bound column is a tile column.
pub(super) fn in_tile_order<'d>(
    tiles: &TileColumns<'_>,
    bound: &[&'d ColumnDescContents],
) -> Vec<&'d ColumnDescContents> {
    let mut ordered = vec![None; bound.len()];
    for (column, tile) in tiles.tile_index.iter().enumerate() {
        if let Some(tile) = tile {
            ordered[*tile] = Some(bound[column]);
        }
    }
    ordered.into_iter().flatten().collect()
}

/// The tile header a read of the column at `target_desc_idx` lays its tiles
/// out by, and that column's index among the tile columns, for a manager
/// whose bound columns are `bound_cols`.
pub(super) fn tile_target<'h>(
    variant: &TiledVariant,
    header: Cow<'h, TiledStManHeader>,
    table_desc: &TableDescContents,
    bound_cols: &[(usize, &PlainColumnEntry)],
    target_desc_idx: usize,
    dm_seq_nr: u32,
) -> Result<(Cow<'h, TiledStManHeader>, usize), StorageError> {
    let Some(bound_col_idx) = bound_cols
        .iter()
        .position(|(desc_idx, _)| *desc_idx == target_desc_idx)
    else {
        return Err(StorageError::FormatMismatch(format!(
            "tiled column desc index {target_desc_idx} not bound to data manager {dm_seq_nr}"
        )));
    };
    let bound: Vec<&ColumnDescContents> = bound_cols
        .iter()
        .map(|(desc_idx, _)| &table_desc.columns[*desc_idx])
        .collect();
    let tiles = tile_columns(variant, header, &bound, table_desc)?;
    match tiles.tile_index[bound_col_idx] {
        Some(tile) => Ok((tiles.header, tile)),
        None => Err(untiled_column_error(
            variant.manager_name(),
            &bound[bound_col_idx].col_name,
        )),
    }
}

/// The bound columns a tiled manager's tiles hold, as positions in binding
/// order, listed in tile order.
///
/// `names` and `types` are the bound columns in binding order; `hypercolumn`
/// names the manager's hypercolumn, whose definition, if the table has one,
/// is a field of `private_keywords`.
pub(super) fn tile_order(
    hypercolumn: &str,
    names: &[&str],
    types: &[CasacoreDataType],
    private_keywords: &RecordValue,
) -> Result<Vec<usize>, StorageError> {
    let mut order = match hypercolumn_data_columns(hypercolumn, private_keywords)? {
        Some(data) => data
            .iter()
            .map(|name| {
                names.iter().position(|bound| bound == name).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "hypercolumn {hypercolumn} data column '{name}' is not bound to its \
                         tiled manager"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        None => (0..names.len()).collect(),
    };
    // casacore's descending sort: by size, ties in descending list position.
    order.reverse();
    order.sort_by_key(|&column| Reverse(tile_pixel_size(types[column])));
    Ok(order)
}

/// The bucket size of a tile laid out over the tile columns `order` (from
/// [`tile_order`]) of the bound columns of `types`, and each bound column's
/// offset in it. Writers build tiles from bound columns with it.
pub(super) fn bound_tile_layout(
    types: &[CasacoreDataType],
    order: &[usize],
    tile_shape: &[usize],
) -> (usize, Vec<usize>) {
    let tile_types: Vec<CasacoreDataType> = order.iter().map(|&column| types[column]).collect();
    let (bucket_size, tile_offsets) = compute_tile_layout(&tile_types, tile_shape);
    let mut offsets = vec![0; types.len()];
    for (tile, &column) in order.iter().enumerate() {
        offsets[column] = tile_offsets[tile];
    }
    (bucket_size, offsets)
}

/// The tile order of the columns a casa-rs writer stores in one tiled
/// manager of hypercolumn `hypercolumn`, refusing a column the tiles would
/// not hold: casa-rs writes no id or coordinate columns.
pub(super) fn writer_tile_order(
    hypercolumn: &str,
    col_descs: &[ColumnDescContents],
    types: &[CasacoreDataType],
    private_keywords: &RecordValue,
) -> Result<Vec<usize>, StorageError> {
    let names: Vec<&str> = col_descs
        .iter()
        .map(|desc| desc.col_name.as_str())
        .collect();
    let order = tile_order(hypercolumn, &names, types, private_keywords)?;
    if let Some(column) = (0..names.len()).find(|column| !order.contains(column)) {
        return Err(StorageError::FormatMismatch(format!(
            "cannot write column '{}': it is an id or coordinate column of hypercolumn \
             {hypercolumn}",
            names[column]
        )));
    }
    Ok(order)
}

/// The data column names of hypercolumn `hypercolumn`'s definition, in its
/// order, or `None` when the table does not define it.
fn hypercolumn_data_columns(
    hypercolumn: &str,
    private_keywords: &RecordValue,
) -> Result<Option<Vec<String>>, StorageError> {
    let Some(definition) = private_keywords.get(&format!("{HYPERCOLUMN_PREFIX}{hypercolumn}"))
    else {
        return Ok(None);
    };
    let Value::Record(definition) = definition else {
        return Err(StorageError::FormatMismatch(format!(
            "hypercolumn {hypercolumn} definition is not a record"
        )));
    };
    match definition.get("data") {
        Some(Value::Array(ArrayValue::String(names))) => Ok(Some(names.iter().cloned().collect())),
        _ => Err(StorageError::FormatMismatch(format!(
            "hypercolumn {hypercolumn} definition has no data column names"
        ))),
    }
}

/// The refusal for reading `column`, which a tiled manager's tiles do not
/// hold.
pub(super) fn untiled_column_error(manager: &str, column: &str) -> StorageError {
    StorageError::FormatMismatch(format!(
        "column '{column}' is a {manager} id or coordinate column; reading coordinate columns is \
         not supported"
    ))
}

/// The cell of `column`, a column the hypercube's values record holds, in
/// any row of that hypercube: its id value.
///
/// A coordinate column's cell depends on the row's position along the
/// coordinate's axis; that is not supported and is refused rather than
/// read as the whole coordinate vector.
pub(super) fn hypercube_cell(cube: &TsmCubeInfo, column: &str) -> Result<Value, StorageError> {
    match cube.values.get(column) {
        Some(value @ Value::Scalar(_)) => Ok(value.clone()),
        Some(_) => Err(StorageError::FormatMismatch(format!(
            "TiledDataStMan coordinate column '{column}' is not supported"
        ))),
        None => Err(StorageError::FormatMismatch(format!(
            "TiledDataStMan hypercube has no value for its id column '{column}'"
        ))),
    }
}

/// casacore's `tilePixelSize`: a pixel's canonical size, 0 for Bool.
fn tile_pixel_size(data_type: CasacoreDataType) -> usize {
    match data_type {
        CasacoreDataType::TpBool => 0,
        other => tile_element_size(other),
    }
}

#[cfg(test)]
mod tests {
    use casa_types::{RecordField, ScalarValue};
    use ndarray::Array1;

    use super::*;

    fn column(name: &str, data_type: CasacoreDataType, nrdim: i32) -> ColumnDescContents {
        ColumnDescContents {
            class_name: String::new(),
            col_name: name.to_string(),
            comment: String::new(),
            data_manager_type: "TiledDataStMan".to_string(),
            data_manager_group: "TiledData".to_string(),
            data_type,
            option: 0,
            nrdim,
            shape: Vec::new(),
            max_length: 0,
            keywords: RecordValue::default(),
            is_array: nrdim > 0,
            primitive_type: None,
        }
    }

    fn strings(names: &[&str]) -> Value {
        Value::Array(ArrayValue::String(
            Array1::from_vec(names.iter().map(|name| name.to_string()).collect()).into_dyn(),
        ))
    }

    /// A table description defining hypercolumn `TiledData` with data
    /// columns `data` and id columns `id`, as `TableDesc::defineHypercolumn`
    /// stores it.
    fn defining(data: &[&str], id: &[&str]) -> TableDescContents {
        let definition = RecordValue::new(vec![
            RecordField::new("ndim", Value::Scalar(ScalarValue::UInt32(3))),
            RecordField::new("data", strings(data)),
            RecordField::new("coord", strings(&[])),
            RecordField::new("id", strings(id)),
        ]);
        TableDescContents {
            name: String::new(),
            version: String::new(),
            comment: String::new(),
            table_keywords: RecordValue::default(),
            private_keywords: RecordValue::new(vec![RecordField::new(
                "Hypercolumn_TiledData",
                Value::Record(definition),
            )]),
            columns: Vec::new(),
        }
    }

    fn undefined() -> TableDescContents {
        TableDescContents {
            private_keywords: RecordValue::default(),
            ..defining(&[], &[])
        }
    }

    fn header(types: &[CasacoreDataType]) -> TiledStManHeader {
        TiledStManHeader {
            big_endian: false,
            seq_nr: 0,
            nrrow: 2,
            col_data_types: types.to_vec(),
            hypercolumn_name: "TiledData".into(),
            max_cache_size: 0,
            nrdim: 3,
            files: Vec::new(),
            cubes: vec![TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: vec![2, 3, 2],
                tile_shape: vec![2, 3, 1],
                file_seq_nr: 0,
                file_offset: 0,
            }],
        }
    }

    const DATA: TiledVariant = TiledVariant::Data {
        nrrow_last: 0,
        row_map: Vec::new(),
        cube_map: Vec::new(),
        pos_map: Vec::new(),
    };

    #[test]
    fn id_columns_leave_the_tiles_and_data_columns_sort_by_pixel_size() {
        let bound = [
            column("flag", CasacoreDataType::TpBool, 2),
            column("id", CasacoreDataType::TpInt, 0),
            column("weight", CasacoreDataType::TpFloat, 2),
            column("data", CasacoreDataType::TpComplex, 2),
            column("count", CasacoreDataType::TpInt, 2),
        ];
        let types: Vec<_> = bound.iter().map(|desc| desc.data_type).collect();
        let bound: Vec<_> = bound.iter().collect();
        let desc = defining(&["flag", "weight", "data", "count"], &["id"]);
        let header = header(&types);
        let columns =
            tile_columns(&DATA, Cow::Borrowed(&header), &bound, &desc).expect("tile columns");

        // Complex, then the two 4-byte columns in descending definition
        // position (count after weight in it), then Bool.
        assert_eq!(
            columns.tile_index,
            [Some(3), None, Some(2), Some(0), Some(1)]
        );
        assert_eq!(
            columns.header.col_data_types,
            [
                CasacoreDataType::TpComplex,
                CasacoreDataType::TpInt,
                CasacoreDataType::TpFloat,
                CasacoreDataType::TpBool,
            ]
        );
        assert_eq!(
            columns
                .first_untiled(&bound)
                .map(|desc| desc.col_name.as_str()),
            Some("id")
        );
    }

    /// The two layouts Fable measured with casacore 3.7.1 on #756.
    #[test]
    fn equal_sizes_take_descending_definition_position_as_casacore_lays_them_out() {
        let float = CasacoreDataType::TpFloat;
        let order = |data: &[&str], names: &[&str], types: &[CasacoreDataType]| {
            tile_order(
                "TiledData",
                names,
                types,
                &defining(data, &[]).private_keywords,
            )
            .expect("order")
        };
        // Binding a, b; definition a, b: casacore's tile is b, a.
        assert_eq!(order(&["a", "b"], &["a", "b"], &[float, float]), [1, 0]);
        // Binding a, b, c, d (Float, Float, Complex, Int); definition
        // b, a, d, c: casacore's tile is c, d, a, b.
        let types = [
            float,
            float,
            CasacoreDataType::TpComplex,
            CasacoreDataType::TpInt,
        ];
        assert_eq!(
            order(&["b", "a", "d", "c"], &["a", "b", "c", "d"], &types),
            [2, 3, 0, 1]
        );
    }

    #[test]
    fn without_a_definition_equal_sizes_take_descending_binding_position() {
        let names = ["a", "b", "flag", "c"];
        let types = [
            CasacoreDataType::TpFloat,
            CasacoreDataType::TpFloat,
            CasacoreDataType::TpBool,
            CasacoreDataType::TpDouble,
        ];
        assert_eq!(
            tile_order("TiledData", &names, &types, &undefined().private_keywords).expect("order"),
            [3, 1, 0, 2]
        );
    }

    /// A field of a values record does not make a data column an id column:
    /// the definition decides.
    /// #755: the other managers take the same split and order. A
    /// TiledShapeStMan's coordinate column stays out of its tiles, and its
    /// data columns sort as a TiledDataStMan's do; an empty manager earlier
    /// casa-rs wrote, with no column types and no hypercube, lays out
    /// nothing.
    #[test]
    fn every_tiled_manager_takes_casacores_split_and_order() {
        let shape = TiledVariant::Shape {
            default_tile_shape: Vec::new(),
            nr_used_row_map: 0,
            row_map: Vec::new(),
            cube_map: Vec::new(),
            pos_map: Vec::new(),
        };
        let bound = [
            column("flag", CasacoreDataType::TpBool, 2),
            column("freq", CasacoreDataType::TpDouble, 1),
            column("data", CasacoreDataType::TpComplex, 2),
        ];
        let refs: Vec<&ColumnDescContents> = bound.iter().collect();
        let types = [
            CasacoreDataType::TpBool,
            CasacoreDataType::TpDouble,
            CasacoreDataType::TpComplex,
        ];
        let tiles = tile_columns(
            &shape,
            Cow::Owned(header(&types)),
            &refs,
            &defining(&["flag", "data"], &[]),
        )
        .expect("tiles");
        assert_eq!(tiles.tile_index, [Some(1), None, Some(0)]);
        assert_eq!(
            tiles.header.col_data_types,
            [CasacoreDataType::TpComplex, CasacoreDataType::TpBool]
        );
        assert_eq!(
            tiles
                .first_untiled(&refs)
                .map(|desc| desc.col_name.as_str()),
            Some("freq")
        );

        let empty = TiledStManHeader {
            col_data_types: Vec::new(),
            cubes: Vec::new(),
            ..header(&[])
        };
        let tiles =
            tile_columns(&shape, Cow::Borrowed(&empty), &refs, &undefined()).expect("tiles");
        assert_eq!(tiles.tile_index, [Some(0), Some(1), Some(2)]);
        assert!(matches!(tiles.header, Cow::Borrowed(_)));
    }

    #[test]
    fn a_values_field_named_like_a_data_column_leaves_it_in_the_tiles() {
        let a = column("a", CasacoreDataType::TpFloat, 2);
        let mut header = header(&[CasacoreDataType::TpFloat]);
        header.cubes[0].values = RecordValue::new(vec![RecordField::new(
            "a",
            Value::Scalar(ScalarValue::Float32(77.0)),
        )]);
        let tiles = tile_columns(&DATA, Cow::Borrowed(&header), &[&a], &defining(&["a"], &[]))
            .expect("tiles");
        assert_eq!(tiles.tile_index, [Some(0)]);
    }

    #[test]
    fn a_definition_naming_an_unbound_data_column_is_refused() {
        let desc = defining(&["data", "missing"], &[]);
        let types = [CasacoreDataType::TpFloat];
        assert!(tile_order("TiledData", &["data"], &types, &desc.private_keywords).is_err());
    }

    #[test]
    fn a_binding_that_disagrees_with_the_header_is_refused() {
        let header = header(&[CasacoreDataType::TpFloat]);
        let data = column("data", CasacoreDataType::TpFloat, 2);
        let id = column("id", CasacoreDataType::TpInt, 0);
        assert!(tile_columns(&DATA, Cow::Borrowed(&header), &[&data, &id], &undefined()).is_err());
    }

    #[test]
    fn data_columns_with_other_than_one_row_axis_are_refused() {
        // Three hypercube axes: one-dimensional cells leave two row axes,
        // and a scalar column three.
        let header = header(&[CasacoreDataType::TpFloat]);
        let desc = undefined();
        for nrdim in [1, 0, -1] {
            let data = column("data", CasacoreDataType::TpFloat, nrdim);
            assert!(
                tile_columns(&DATA, Cow::Borrowed(&header), &[&data], &desc).is_err(),
                "rank {nrdim}"
            );
        }
        let data = column("data", CasacoreDataType::TpFloat, 2);
        let tiles =
            tile_columns(&DATA, Cow::Borrowed(&header), &[&data], &desc).expect("one row axis");
        assert!(
            matches!(tiles.header, Cow::Borrowed(_)),
            "an identity order borrows"
        );
    }
}
