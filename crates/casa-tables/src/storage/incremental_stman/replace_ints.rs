// SPDX-License-Identifier: LGPL-3.0-or-later
//! Replace values without repacking the buckets when existing runs suffice.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use casa_types::Value;

use super::*;

pub(in crate::storage) fn stage_int32_replacement(
    original: &Path,
    staged: &Path,
    columns: &[ColumnDescContents],
    replacements: &BTreeMap<String, Vec<Option<Value>>>,
    rows: u64,
) -> Result<bool, StorageError> {
    let mut selected = Vec::with_capacity(columns.len());
    for column in columns {
        let values = replacements.get(&column.col_name).map(Vec::as_slice);
        if values.is_some() && column.data_type != CasacoreDataType::TpInt {
            return Ok(false);
        }
        selected.push(values);
    }

    let mut input = File::open(original)?;
    let header = parse_ism_header(&mut input)?;
    let index = parse_ism_index(&mut input, &header)?;
    if index.used == 0 {
        return Ok(false);
    }
    if index.rows.first() != Some(&0)
        || index.rows.get(index.used) != Some(&rows)
        || index.bucket_nrs.len() < index.used
    {
        return Err(invalid(
            "IncrementalStMan interval index does not cover the table",
        ));
    }
    let mut changed = BTreeMap::new();
    let mut preserved = BTreeSet::new();
    for interval in 0..index.used {
        let start =
            usize::try_from(index.rows[interval]).map_err(|_| invalid("ISM row overflow"))?;
        let end =
            usize::try_from(index.rows[interval + 1]).map_err(|_| invalid("ISM row overflow"))?;
        let bucket_number = index.bucket_nrs[interval];
        if start >= end || bucket_number >= header.nr_buckets {
            return Err(invalid("invalid IncrementalStMan interval"));
        }
        let raw = read_ism_bucket(&mut input, &header, bucket_number)?;
        let bucket = parse_ism_bucket(&raw, columns.len(), header.big_endian)?;
        let origin = ISM_HEADER_SIZE + u64::from(bucket_number) * u64::from(header.bucket_size) + 4;
        if !record_offsets(
            &bucket,
            &selected,
            start..end,
            origin,
            header.big_endian,
            &mut changed,
            &mut preserved,
        )? {
            return Ok(false);
        }
    }
    if changed.keys().any(|address| preserved.contains(address)) {
        return Ok(false);
    }

    fs::copy(original, staged)?;
    let mut output = File::options().write(true).open(staged)?;
    for (address, value) in changed {
        let words = if header.big_endian {
            value.to_be_bytes()
        } else {
            value.to_le_bytes()
        };
        output.seek(SeekFrom::Start(address))?;
        output.write_all(&words)?;
    }
    Ok(true)
}

fn record_offsets(
    bucket: &IsmBucket,
    selected: &[Option<&[Option<Value>]>],
    rows: std::ops::Range<usize>,
    origin: u64,
    big_endian: bool,
    changed: &mut BTreeMap<u64, i32>,
    preserved: &mut BTreeSet<u64>,
) -> Result<bool, StorageError> {
    if bucket.col_indices.len() != selected.len() {
        return Ok(false);
    }
    for (index, column) in bucket.col_indices.iter().zip(selected) {
        let Some(column) = column else {
            preserved.extend(
                index
                    .offsets
                    .iter()
                    .map(|&offset| origin + u64::from(offset)),
            );
            continue;
        };
        if index.row_nrs.first() != Some(&0) {
            return Ok(false);
        }
        for (entry, (&start, &offset)) in index.row_nrs.iter().zip(&index.offsets).enumerate() {
            let start = start as usize;
            if start >= rows.len() {
                // One physical bucket can cover a prefix of its logical rows.
                break;
            }
            let end = index
                .row_nrs
                .get(entry + 1)
                .map_or(rows.len(), |&row| rows.len().min(row as usize));
            let Some(values) = column.get(rows.start + start..rows.start + end) else {
                return Err(invalid("ISM replacement range is out of bounds"));
            };
            let Some((first @ Some(Value::Scalar(ScalarValue::Int32(value))), rest)) =
                values.split_first()
            else {
                return Ok(false);
            };
            if rest.iter().any(|other| other != first) {
                return Ok(false);
            }
            let bytes = bucket
                .data
                .get(offset as usize..offset as usize + 4)
                .ok_or_else(|| invalid("ISM replacement value offset is out of bounds"))?;
            let old = if big_endian {
                read_i32_be(bytes)
            } else {
                read_i32_le(bytes)
            };
            let address = origin + u64::from(offset);
            if *value == old {
                preserved.insert(address);
            } else if changed
                .insert(address, *value)
                .is_some_and(|prior| prior != *value)
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn invalid(message: &str) -> StorageError {
    StorageError::FormatMismatch(message.into())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        ColumnSchema, DataManagerKind, EndianFormat, Table, TableOptions, TableSchema,
        TableWriteLock,
    };
    use casa_types::{PrimitiveType, RecordField, RecordValue};

    #[test]
    fn encoded_replacement_preserves_other_cells_and_falls_back_when_a_run_splits() {
        let scratch = tempfile::tempdir().unwrap();
        for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
            let path = scratch.path().join(format!("{endian:?}"));
            let schema = TableSchema::new(vec![
                ColumnSchema::scalar("id", PrimitiveType::Int32),
                ColumnSchema::scalar("keep", PrimitiveType::Int32),
            ])
            .unwrap();
            let mut table = Table::with_schema(schema);
            for row in 0..8 {
                table
                    .add_row(RecordValue::new(vec![
                        RecordField::new("id", Value::Scalar(ScalarValue::Int32(row / 4))),
                        RecordField::new("keep", Value::Scalar(ScalarValue::Int32(10 + row))),
                    ]))
                    .unwrap();
            }
            table
                .save(
                    TableOptions::new(&path)
                        .with_data_manager(DataManagerKind::IncrementalStMan)
                        .with_endian_format(endian),
                )
                .unwrap();
            let mut lock = TableWriteLock::acquire_to_reread(&path, 1).unwrap();
            let table = Table::open(TableOptions::new(&path)).unwrap();
            let data = path.join("table.f0");
            let initial = fs::read(&data).unwrap();
            for (ids, encoded) in [
                ([4, 4, 4, 4, 5, 5, 5, 5], true),
                ([4, 5, 6, 7, 8, 9, 10, 11], false),
            ] {
                let requested = BTreeMap::from([(
                    "id".to_owned(),
                    ids.map(|id| Some(Value::Scalar(ScalarValue::Int32(id))))
                        .into(),
                )]);
                let target = scratch.path().join(format!("raw-{endian:?}-{encoded}"));
                let description =
                    crate::storage::table_control::read_table_dat(&path.join("table.dat")).unwrap();
                assert_eq!(
                    stage_int32_replacement(
                        &data,
                        &target,
                        &description.table_desc.columns,
                        &requested,
                        8
                    )
                    .unwrap(),
                    encoded
                );
                if encoded {
                    let staged_raw = fs::read(target).unwrap();
                    assert_eq!(initial.len(), staged_raw.len());
                    assert_eq!(
                        initial
                            .iter()
                            .zip(&staged_raw)
                            .filter(|(a, b)| a != b)
                            .count(),
                        2
                    );
                }
                let mut edit = table
                    .prepare_column_replacement(&requested, None, &mut lock)
                    .unwrap();
                edit.publish().unwrap();
                edit.synchronize().unwrap();
                let read = Table::open(TableOptions::new(&path))
                    .unwrap()
                    .scalar_columns_owned(&["id", "keep"])
                    .unwrap();
                assert_eq!(read["id"], ids.map(|id| Some(ScalarValue::Int32(id))));
                assert_eq!(
                    read["keep"],
                    (10..18)
                        .map(|id| Some(ScalarValue::Int32(id)))
                        .collect::<Vec<_>>()
                );
                edit.restore().unwrap();
                assert_eq!(fs::read(&data).unwrap(), initial);
            }
        }
    }
}
