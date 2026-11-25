// SPDX-License-Identifier: LGPL-3.0-or-later
//! Encode a scalar suffix without decoding and rewriting earlier buckets.

use casa_types::{RecordValue, Value};

use super::*;

pub(in crate::storage) fn stage_scalar_append(
    old_path: &Path,
    destination: &Path,
    columns: &[ColumnDescContents],
    manager: &[u8],
    records: &[RecordValue],
    old_rows: u64,
) -> Result<bool, StorageError> {
    let mut old = File::open(old_path)?;
    let mut header = parse_ism_header(&mut old)?;
    let prior = parse_ism_index(&mut old, &header)?;
    if prior.used == 0 {
        // A descriptor can precede initialization of its managed rows.
        // The whole-manager writer materializes the old implicit defaults.
        return Ok(false);
    }
    validate_index(&prior, old_rows, header.nr_buckets)?;
    let values = column_values(old_path, columns, manager, records, old_rows)?;
    let slices = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let parent = destination
        .parent()
        .ok_or_else(|| invalid("staging directory missing"))?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    write_columns::write_ism_column_sources_with_layout(
        temporary.path(),
        columns,
        &ColumnSources::materialized(records.len(), &slices),
        header.big_endian,
        Some(header.bucket_size),
        header.pers_cache_size,
    )?;
    let mut suffix = File::open(temporary.path())?;
    let suffix_header = parse_ism_header(&mut suffix)?;
    let suffix_index = parse_ism_index(&mut suffix, &suffix_header)?;
    validate_index(
        &suffix_index,
        records.len() as u64,
        suffix_header.nr_buckets,
    )?;

    let mut index_rows = prior.rows[..prior.used].to_vec();
    let mut buckets = prior.bucket_nrs[..prior.used].to_vec();
    for &row in &suffix_index.rows[..=suffix_index.used] {
        index_rows.push(
            old_rows
                .checked_add(row)
                .ok_or_else(|| invalid("ISM row count overflow"))?,
        );
    }
    for &bucket in &suffix_index.bucket_nrs[..suffix_index.used] {
        buckets.push(
            header
                .nr_buckets
                .checked_add(bucket)
                .ok_or_else(|| invalid("ISM bucket count overflow"))?,
        );
    }
    let previous_buckets = header.nr_buckets;
    header.nr_buckets = header
        .nr_buckets
        .checked_add(suffix_header.nr_buckets)
        .ok_or_else(|| invalid("ISM bucket count overflow"))?;
    let mut output = File::create(destination)?;
    output.write_all(&encode_ism_header(&header)?)?;
    copy_buckets(&mut old, &mut output, previous_buckets, header.bucket_size)?;
    copy_buckets(
        &mut suffix,
        &mut output,
        suffix_header.nr_buckets,
        suffix_header.bucket_size,
    )?;
    output.write_all(&encode_ism_index(&index_rows, &buckets, header.io_order)?)?;
    Ok(true)
}

fn column_values(
    old_path: &Path,
    columns: &[ColumnDescContents],
    manager: &[u8],
    records: &[RecordValue],
    old_rows: u64,
) -> Result<Vec<Vec<Option<ScalarValue>>>, StorageError> {
    let descriptors = columns.iter().collect::<Vec<_>>();
    let scalar = |record: &RecordValue, name: &str| match record.get(name) {
        Some(Value::Scalar(value)) => Some(value.clone()),
        _ => None,
    };
    let last = usize::try_from(old_rows - 1).map_err(|_| invalid("ISM row index exceeds usize"))?;
    columns
        .iter()
        .enumerate()
        .map(|(column, desc)| {
            let name = desc.col_name.as_str();
            let mut previous = if records[0].get(name).is_some() {
                None
            } else {
                read_ism_scalar_column_rows(
                    old_path,
                    manager,
                    &descriptors,
                    column,
                    &[last],
                    false,
                )?
                .and_then(|mut values| values.pop().flatten())
            };
            Ok(records
                .iter()
                .map(|record| {
                    if let Some(explicit) = scalar(record, name) {
                        previous = Some(explicit);
                    }
                    previous.clone()
                })
                .collect())
        })
        .collect()
}

fn copy_buckets(
    input: &mut File,
    output: &mut File,
    count: u32,
    size: u32,
) -> Result<(), StorageError> {
    input.seek(SeekFrom::Start(ISM_HEADER_SIZE))?;
    let bytes = u64::from(count) * u64::from(size);
    if std::io::copy(&mut input.take(bytes), output)? != bytes {
        return Err(invalid("truncated ISM bucket payload"));
    }
    Ok(())
}

fn validate_index(index: &IsmIndex, rows: u64, buckets: u32) -> Result<(), StorageError> {
    if index.used == 0
        || index.rows.first() != Some(&0)
        || index.rows.get(index.used) != Some(&rows)
        || index
            .bucket_nrs
            .get(..index.used)
            .is_none_or(|ids| ids.iter().any(|&id| id >= buckets))
        || index.rows[..=index.used]
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid(
            "IncrementalStMan append index does not cover the inputs",
        ));
    }
    Ok(())
}

fn invalid(reason: &str) -> StorageError {
    StorageError::FormatMismatch(reason.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnSchema, DataManagerKind, EndianFormat, Table, TableOptions, TableSchema};
    use casa_types::{PrimitiveType, RecordField};

    #[test]
    fn scalar_append_preserves_prior_buckets_and_inherits_omitted_values() {
        for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("scalars");
            let schema = TableSchema::new(vec![
                ColumnSchema::scalar("id", PrimitiveType::Int32),
                ColumnSchema::scalar("tag", PrimitiveType::String),
            ])
            .unwrap();
            let id = |value| RecordField::new("id", Value::Scalar(ScalarValue::Int32(value)));
            let tag = |value: &str| {
                RecordField::new("tag", Value::Scalar(ScalarValue::String(value.into())))
            };
            let mut original = Table::with_schema(schema);
            for row in 0..200 {
                original
                    .add_row(RecordValue::new(vec![id(row), tag("prefix")]))
                    .unwrap();
            }
            original
                .save(
                    TableOptions::new(&path)
                        .with_data_manager(DataManagerKind::IncrementalStMan)
                        .with_endian_format(endian),
                )
                .unwrap();

            let file = path.join("table.f0");
            let header = parse_ism_header(&mut File::open(&file).unwrap()).unwrap();
            let bucket_end =
                ISM_HEADER_SIZE as usize + header.nr_buckets as usize * header.bucket_size as usize;
            let before = std::fs::read(&file).unwrap();
            let mut table = Table::open(TableOptions::new(&path)).unwrap();
            table
                .append_records_in_place(&[
                    RecordValue::new(vec![id(804)]),
                    RecordValue::new(vec![tag("suffix")]),
                    RecordValue::default(),
                ])
                .unwrap();
            let after = std::fs::read(&file).unwrap();
            assert_eq!(
                &after[ISM_HEADER_SIZE as usize..bucket_end],
                &before[ISM_HEADER_SIZE as usize..bucket_end]
            );
            let rows = table.rows().unwrap();
            assert_eq!(&rows[..200], original.rows().unwrap());
            assert_eq!(rows[200], RecordValue::new(vec![id(804), tag("prefix")]));
            assert_eq!(rows[201], RecordValue::new(vec![id(804), tag("suffix")]));
            assert_eq!(rows[202], rows[201]);
        }
    }
}
