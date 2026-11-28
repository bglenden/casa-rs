// SPDX-License-Identifier: LGPL-3.0-or-later
//! The StandardStMan file writer for any mix of columns, with bounded
//! memory.
//!
//! The columns are written one after another, each over all its rows, which
//! fixes where their shared storage goes: indirect arrays and records are
//! appended to the array file (`table.fNi`) and long strings to the string
//! buckets in that column-major order. Each column's slice of a data bucket
//! is written at its place in the file as soon as it is full, and each
//! string bucket once the next one starts, even within one long string;
//! the index is generated as it is streamed into its buckets
//! ([`super::index`]). So the writer holds one slice, one string bucket,
//! one index bucket and the array file's buffer, whatever the row count and
//! however long a string, and while it stores a string array cell, that
//! cell's encoding (its elements' lengths and bytes). The file is laid out
//! as casacore's `SSMBase` reads it: the 512-byte header, the data buckets,
//! the string buckets, then the index buckets.

use std::borrow::Cow;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, RecordValue, Value};
use ndarray::{ArrayD, IxDyn};

use super::index::{StreamIndex, write_group_indexes};
use super::{
    CasacoreDataType, ColumnDescContents, ColumnSources, StManArrayFileWriter, StorageError,
    StringBuckets, bucket_offset, canonical_element_size, is_ssm_fixed_string, is_ssm_indirect,
    is_ssm_variable_string, new_ssm_bucket_layout_with_policy, serialize_ssm_dm_blob_mapped,
    ssm_cell_element_count, write_cell_to_bucket, write_i64_be, write_i64_le,
};

/// The cells a StandardStMan file is written from, by column and row.
pub(super) trait SsmCells {
    /// Rows to write.
    fn row_count(&self) -> usize;

    /// The cell of `column` in `row`, `None` when it is undefined.
    fn cell(&self, column: usize, row: usize) -> Result<Option<Cow<'_, Value>>, StorageError>;
}

impl SsmCells for ColumnSources<'_> {
    fn row_count(&self) -> usize {
        ColumnSources::row_count(self)
    }

    fn cell(&self, column: usize, row: usize) -> Result<Option<Cow<'_, Value>>, StorageError> {
        ColumnSources::cell(self, column, row)
    }
}

/// Row records as cells: each column's field, found at its index in the
/// record when `field_indices` gives one and by name otherwise.
pub(super) struct RowCells<'r> {
    rows: &'r [RecordValue],
    field_indices: Option<&'r [usize]>,
    names: Vec<&'r str>,
}

impl<'r> RowCells<'r> {
    pub(super) fn new(
        rows: &'r [RecordValue],
        field_indices: Option<&'r [usize]>,
        col_descs: &'r [ColumnDescContents],
    ) -> Self {
        Self {
            rows,
            field_indices,
            names: col_descs
                .iter()
                .map(|desc| desc.col_name.as_str())
                .collect(),
        }
    }
}

impl SsmCells for RowCells<'_> {
    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn cell(&self, column: usize, row: usize) -> Result<Option<Cow<'_, Value>>, StorageError> {
        let name = self.names[column];
        let record = self.rows.get(row).ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "SSM row writer missing row {row} for column {name}"
            ))
        })?;
        let fields = record.fields();
        let field = self
            .field_indices
            .and_then(|indices| indices.get(column))
            .and_then(|&index| fields.get(index))
            .filter(|field| field.name == name)
            .or_else(|| fields.iter().find(|field| field.name == name));
        Ok(field.map(|field| Cow::Borrowed(&field.value)))
    }
}

/// Bits a row of each column takes in a data bucket: the bits of its
/// elements; 12 bytes for a variable-length string's reference; 8 bytes for
/// an indirect cell's array-file offset.
fn column_bits(col_descs: &[ColumnDescContents]) -> Vec<usize> {
    col_descs
        .iter()
        .map(|desc| {
            if is_ssm_fixed_string(desc) {
                desc.max_length as usize * ssm_cell_element_count(desc) * 8
            } else if is_ssm_variable_string(desc) {
                96
            } else if is_ssm_indirect(desc) {
                64
            } else {
                ssm_cell_element_count(desc) * canonical_element_size(desc.data_type).1
            }
        })
        .collect()
}

/// Write a string bucket at its place in the file: bucket `number` of
/// buckets of `bucket_bytes` bytes.
fn write_string_bucket(
    file: &mut File,
    (number, bucket_bytes): (usize, usize),
    bucket: &[u8],
) -> Result<(), StorageError> {
    file.seek(SeekFrom::Start(bucket_offset(number, bucket_bytes)))?;
    file.write_all(bucket)?;
    Ok(())
}

/// Write a StandardStMan data file from `cells`, column by column, and
/// return the DM data blob for `table.dat`.
///
/// The bytes are those of a writer that assembles every bucket in memory
/// and writes the columns in order.
pub(super) fn write_ssm_file_columns(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    cells: &impl SsmCells,
    big_endian: bool,
    casacore_default_small: bool,
    dm_name: &str,
) -> Result<Vec<u8>, StorageError> {
    write_ssm_file_columns_with_layout(
        file_path,
        col_descs,
        cells,
        big_endian,
        casacore_default_small,
        dm_name,
        None,
    )
}

/// Plan one or more streams of data buckets. A manager can bind columns
/// whose combined widths exceed its configured bucket size, as long as
/// each column fits by itself (SSMBase::addColumn).
fn data_streams(
    bits: &[usize],
    nrrow: usize,
    size: usize,
    new_table_rows_per_bucket: Option<u32>,
) -> Result<Vec<StreamIndex>, StorageError> {
    let mut groups = vec![(0usize, 0usize)];
    for (index, &width) in bits.iter().enumerate() {
        let bytes = width.div_ceil(8);
        if bytes > size {
            return Err(StorageError::UnsupportedDataManager(
                "StandardStMan column does not fit the configured bucket size".into(),
            ));
        }
        let (_, used) = groups.last_mut().expect("initial stream");
        if *used + bytes > size {
            groups.push((index, bytes));
        } else {
            *used += bytes;
        }
    }
    let mut next_bucket = 0;
    Ok(groups
        .iter()
        .enumerate()
        .map(|(index, &(first, _))| {
            let last = groups
                .get(index + 1)
                .map_or(bits.len(), |&(start, _)| start);
            let bits = &bits[first..last];
            let rows_per_bucket = new_table_rows_per_bucket.unwrap_or_else(|| {
                if bits.iter().all(|&width| width == 0) {
                    return 32;
                }
                let fits = |rows: usize| {
                    bits.iter()
                        .map(|bits| (rows * bits).div_ceil(8))
                        .sum::<usize>()
                        <= size
                };
                let (mut low, mut high) = (1, 8 * size / bits.iter().sum::<usize>() + 1);
                while high - low > 1 {
                    let mid = low + (high - low) / 2;
                    if fits(mid) { low = mid } else { high = mid }
                }
                low as u32
            });
            let start = next_bucket;
            next_bucket += nrrow.div_ceil(rows_per_bucket as usize);
            StreamIndex {
                columns: last - first,
                rows_per_bucket,
                buckets: start..next_bucket,
            }
        })
        .collect())
}

pub(super) fn write_ssm_file_columns_with_layout(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    cells: &impl SsmCells,
    big_endian: bool,
    casacore_default_small: bool,
    dm_name: &str,
    inherited: Option<(Option<u32>, u32)>,
) -> Result<Vec<u8>, StorageError> {
    let nrrow = cells.row_count();
    let ncol = col_descs.len();
    // The shared array file of indirect columns (SSM's version 0, without
    // reference counts).
    let mut array_writer = if col_descs.iter().any(is_ssm_indirect) {
        let mut path = file_path.as_os_str().to_os_string();
        path.push("i");
        Some(StManArrayFileWriter::create(
            &PathBuf::from(path),
            big_endian,
            0,
        )?)
    } else {
        None
    };
    let bits = column_bits(col_descs);
    let (bucket, cache_size) = inherited.unwrap_or((None, 2));
    let (default_rows, bucket_size) = if let Some(size) = bucket {
        (None, size)
    } else {
        let (rows, bytes) = new_ssm_bucket_layout_with_policy(&bits, nrrow, casacore_default_small);
        (Some(rows), bytes)
    };
    let bucket_bytes = bucket_size as usize;
    let streams = data_streams(&bits, nrrow, bucket_bytes, default_rows)?;
    let nr_data_buckets = streams.last().map_or(0, |stream| stream.buckets.end);
    let mut column_offsets = Vec::with_capacity(ncol);
    let mut column_indexes = Vec::with_capacity(ncol);
    let mut first_column = 0;
    for (index, stream) in streams.iter().enumerate() {
        let mut offset = 0;
        for &column_bits in &bits[first_column..first_column + stream.columns] {
            column_offsets.push(offset);
            column_indexes.push(index as u32);
            offset += (stream.rows_per_bucket as usize * column_bits).div_ceil(8) as u32;
        }
        first_column += stream.columns;
    }

    let mut file = File::create(file_path)?;
    let mut strings = StringBuckets::after(nr_data_buckets);
    let mut slice = Vec::new();
    for (column, desc) in col_descs.iter().enumerate() {
        let indirect = is_ssm_indirect(desc);
        let nrelem = if indirect {
            1
        } else {
            ssm_cell_element_count(desc)
        };
        let stream = &streams[column_indexes[column] as usize];
        let bucket_rows = stream.rows_per_bucket as usize;
        for bucket in 0..stream.buckets.len() {
            slice.clear();
            slice.resize((bucket_rows * bits[column]).div_ceil(8), 0);
            let first_row = bucket * bucket_rows;
            for row in first_row..nrrow.min(first_row + bucket_rows) {
                let value = cells.cell(column, row)?;
                let row_in_bucket = row - first_row;
                if indirect {
                    let writer = array_writer
                        .as_mut()
                        .expect("indirect columns have an array file");
                    let offset = write_indirect_cell(writer, desc, value.as_deref())?;
                    let cell = &mut slice[row_in_bucket * 8..];
                    if big_endian {
                        write_i64_be(cell, offset);
                    } else {
                        write_i64_le(cell, offset);
                    }
                } else {
                    write_cell_to_bucket(
                        &mut slice,
                        0,
                        row_in_bucket,
                        desc,
                        desc.data_type,
                        nrelem,
                        value.as_deref(),
                        &mut |data| {
                            strings.stream(data, bucket_bytes, |number, bucket| {
                                write_string_bucket(
                                    &mut file,
                                    (nr_data_buckets + number, bucket_bytes),
                                    bucket,
                                )
                            })
                        },
                        nr_data_buckets,
                        big_endian,
                    )?;
                }
            }
            file.seek(SeekFrom::Start(
                bucket_offset(stream.buckets.start + bucket, bucket_bytes)
                    + u64::from(column_offsets[column]),
            ))?;
            file.write_all(&slice)?;
        }
    }
    // The open string bucket, the last.
    strings.release(|number, bucket| {
        write_string_bucket(&mut file, (nr_data_buckets + number, bucket_bytes), bucket)
    })?;
    if let Some(writer) = array_writer.as_mut() {
        writer.finish()?;
    }
    write_group_indexes(
        &mut file,
        nrrow,
        bucket_size,
        strings.len(),
        cache_size,
        &streams,
        big_endian,
    )?;
    serialize_ssm_dm_blob_mapped(dm_name, &column_offsets, &column_indexes)
}

/// Append an indirect cell to the array file and return its offset there:
/// a record as its serialized bytes, an array as itself, and 0 for an
/// undefined cell.
pub(super) fn write_indirect_cell(
    writer: &mut StManArrayFileWriter,
    desc: &ColumnDescContents,
    value: Option<&Value>,
) -> Result<i64, StorageError> {
    match value {
        Some(Value::Record(record)) if desc.is_record() => {
            let bytes = super::super::table_control::serialize_record_to_uchar(record)?;
            let payload = Value::Array(ArrayValue::UInt8(
                ArrayD::from_shape_vec(IxDyn(&[bytes.len()]), bytes).map_err(|err| {
                    StorageError::FormatMismatch(format!(
                        "SSM record column '{}': invalid byte payload shape: {err}",
                        desc.col_name
                    ))
                })?,
            ));
            writer.write_array(&payload, CasacoreDataType::TpUChar)
        }
        Some(value @ Value::Array(_)) if !desc.is_record() => writer.write_array(
            value,
            CasacoreDataType::from_primitive_type(desc.require_primitive_type()?, false),
        ),
        Some(Value::Array(_) | Value::Record(_)) => Err(StorageError::FormatMismatch(format!(
            "SSM indirect column '{}' received incompatible value type",
            desc.col_name
        ))),
        _ => Ok(0),
    }
}

#[cfg(test)]
mod tests {
    //! The column writer writes the bytes of the writer it replaced, which
    //! assembled every bucket in memory, for every kind of column.

    use casa_core::{Complex32, PrimitiveType, RecordField, ScalarValue};
    use ndarray::ArrayD;

    use super::*;
    use crate::schema::{ColumnOptions, ColumnSchema};

    fn schema_columns() -> Vec<ColumnSchema> {
        let direct = ColumnOptions {
            direct: true,
            undefined: false,
        };
        vec![
            ColumnSchema::scalar("I32", PrimitiveType::Int32),
            ColumnSchema::scalar("F64", PrimitiveType::Float64),
            ColumnSchema::scalar("FLAG", PrimitiveType::Bool),
            ColumnSchema::scalar("NAME", PrimitiveType::String),
            ColumnSchema::scalar("C32", PrimitiveType::Complex32),
            ColumnSchema::scalar("LONG_NAME", PrimitiveType::String),
            ColumnSchema::scalar("FIXED_NAME", PrimitiveType::String).with_max_length(6),
            ColumnSchema::array_fixed("DIRECT_F64", PrimitiveType::Float64, vec![2, 3])
                .with_options(direct)
                .expect("direct"),
            ColumnSchema::array_fixed("DIRECT_BOOL", PrimitiveType::Bool, vec![3])
                .with_options(direct)
                .expect("direct"),
            ColumnSchema::array_fixed("DIRECT_STRINGS", PrimitiveType::String, vec![2])
                .with_options(direct)
                .expect("direct"),
            ColumnSchema::array_fixed("FIXED_INDIRECT", PrimitiveType::Int32, vec![4]),
            ColumnSchema::array_variable("DIRECTION", PrimitiveType::Float64, Some(2)),
            ColumnSchema::array_variable("INTS", PrimitiveType::Int32, None),
            ColumnSchema::array_variable("FLAGS", PrimitiveType::Bool, Some(1)),
            ColumnSchema::array_variable("STRINGS", PrimitiveType::String, Some(1)),
            ColumnSchema::record("RECORD"),
        ]
    }

    fn array<T: Clone>(shape: &[usize], values: Vec<T>) -> ArrayD<T> {
        ArrayD::from_shape_vec(shape.to_vec(), values).expect("shape matches")
    }

    /// Row `row`: every column but some variable arrays and the record,
    /// which are left undefined in some rows; some names and strings are
    /// longer than a string bucket.
    fn row(row: usize) -> RecordValue {
        let r = row as i32;
        let long = |length: usize| "é-long".repeat(length / 7);
        let mut fields = vec![
            RecordField::new("I32", Value::Scalar(ScalarValue::Int32(r * 7 - 3))),
            RecordField::new("F64", Value::Scalar(ScalarValue::Float64(row as f64 / 3.0))),
            RecordField::new(
                "FLAG",
                Value::Scalar(ScalarValue::Bool(row.is_multiple_of(3))),
            ),
            RecordField::new(
                "NAME",
                Value::Scalar(ScalarValue::String(format!("n{}", row % 11))),
            ),
            RecordField::new(
                "C32",
                Value::Scalar(ScalarValue::Complex32(Complex32::new(r as f32, -1.5))),
            ),
            RecordField::new(
                "LONG_NAME",
                Value::Scalar(ScalarValue::String(if row % 250 == 7 {
                    long(70_000)
                } else {
                    format!("a longer name for row {row} {}", "x".repeat(row % 40))
                })),
            ),
            RecordField::new(
                "FIXED_NAME",
                Value::Scalar(ScalarValue::String(format!("f{row}"))),
            ),
            RecordField::new(
                "DIRECT_F64",
                Value::Array(ArrayValue::Float64(array(
                    &[2, 3],
                    (0..6).map(|i| (row * 6 + i) as f64).collect(),
                ))),
            ),
            RecordField::new(
                "DIRECT_BOOL",
                Value::Array(ArrayValue::Bool(array(
                    &[3],
                    vec![row.is_multiple_of(2), true, row.is_multiple_of(5)],
                ))),
            ),
            RecordField::new(
                "DIRECT_STRINGS",
                Value::Array(ArrayValue::String(array(
                    &[2],
                    vec![
                        format!("s{row}"),
                        if row % 300 == 11 {
                            long(45_000)
                        } else {
                            "a rather longer string".to_string()
                        },
                    ],
                ))),
            ),
            RecordField::new(
                "FIXED_INDIRECT",
                Value::Array(ArrayValue::Int32(array(&[4], vec![r, r + 1, r + 2, r + 3]))),
            ),
        ];
        if row % 4 != 1 {
            fields.push(RecordField::new(
                "DIRECTION",
                Value::Array(ArrayValue::Float64(array(&[2, 1], vec![0.5, r as f64]))),
            ));
        }
        if row % 6 != 2 {
            let count = row % 5;
            fields.push(RecordField::new(
                "INTS",
                Value::Array(ArrayValue::Int32(array(
                    &[count],
                    (0..count as i32).collect(),
                ))),
            ));
            fields.push(RecordField::new(
                "FLAGS",
                Value::Array(ArrayValue::Bool(array(
                    &[count + 1],
                    (0..=count).map(|i| i % 2 == row % 2).collect(),
                ))),
            ));
            fields.push(RecordField::new(
                "STRINGS",
                Value::Array(ArrayValue::String(array(
                    &[count],
                    (0..count).map(|i| format!("{row}:{i}")).collect(),
                ))),
            ));
        }
        if row % 7 == 3 {
            fields.push(RecordField::new(
                "RECORD",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "k",
                    Value::Scalar(ScalarValue::Int32(r)),
                )])),
            ));
        }
        RecordValue::new(fields)
    }

    fn files(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut files = std::fs::read_dir(dir)
            .expect("read dir")
            .map(|entry| {
                let entry = entry.expect("entry");
                (
                    entry.file_name().to_string_lossy().to_string(),
                    std::fs::read(entry.path()).expect("read file"),
                )
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    #[test]
    fn the_column_writer_writes_the_in_memory_writers_bytes() {
        let root = tempfile::tempdir().expect("tempdir");
        let schema_columns = schema_columns();
        let col_descs = schema_columns
            .iter()
            .map(ColumnDescContents::from_column_schema)
            .collect::<Vec<_>>();
        for (rows, big_endian) in [
            (0, false),
            (5, false),
            (32, true),
            (1500, false),
            (1500, true),
        ] {
            let records = (0..rows).map(row).collect::<Vec<_>>();
            let reference = root.path().join(format!("reference-{rows}-{big_endian}"));
            let streamed = root.path().join(format!("streamed-{rows}-{big_endian}"));
            std::fs::create_dir_all(&reference).expect("mkdir");
            std::fs::create_dir_all(&streamed).expect("mkdir");
            let reference_blob = super::super::write_ssm_file_in_memory(
                &reference.join("table.f0"),
                &col_descs,
                &records,
                None,
                None,
                big_endian,
            )
            .expect("in-memory writer");
            let streamed_blob = write_ssm_file_columns(
                &streamed.join("table.f0"),
                &col_descs,
                &RowCells::new(&records, None, &col_descs),
                big_endian,
                false,
                "SSM",
            )
            .expect("column writer");
            let mut overrides = crate::table::ColumnOverrides::for_row_count(rows);
            for desc in &col_descs {
                overrides.insert_values(
                    &desc.col_name,
                    records
                        .iter()
                        .map(|record| record.get(&desc.col_name).cloned())
                        .collect(),
                );
            }
            let sources = crate::storage::scalar_sources::override_columns_for_group(
                crate::table::DataManagerKind::StandardStMan,
                &col_descs,
                &overrides,
                rows,
                &[],
            )
            .expect("override sources")
            .expect("mixed materialized columns use sources");
            let overridden = root.path().join(format!("overridden-{rows}-{big_endian}"));
            std::fs::create_dir_all(&overridden).expect("mkdir");
            let overridden_blob = write_ssm_file_columns(
                &overridden.join("table.f0"),
                &col_descs,
                &sources,
                big_endian,
                false,
                "SSM",
            )
            .expect("override writer");
            assert_eq!(reference_blob, streamed_blob, "DM blob, {rows} rows");
            assert_eq!(reference_blob, overridden_blob, "override DM blob");
            assert_eq!(files(&reference), files(&overridden), "override bytes");
            let (reference, streamed) = (files(&reference), files(&streamed));
            assert_eq!(reference.len(), streamed.len(), "{rows} rows");
            for ((name, expected), (_, found)) in reference.iter().zip(&streamed) {
                assert!(
                    expected == found,
                    "{name} differs, {rows} rows, big_endian {big_endian}"
                );
            }
        }
    }

    #[test]
    fn missing_mixed_override_cells_keep_existing_row_values() {
        use crate::{ColumnOverrides, DataManagerKind, Table, TableOptions, TableSchema};
        let root = tempfile::tempdir().unwrap();
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("ID", PrimitiveType::Int32),
            ColumnSchema::array_variable("DATA", PrimitiveType::Int32, Some(1)),
        ])
        .unwrap();
        let original = ArrayValue::from_i32_vec(vec![3, 5]);
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("ID", Value::Scalar(ScalarValue::Int32(11))),
                RecordField::new("DATA", Value::Array(original.clone())),
            ]))
            .unwrap();
        let mut overrides = ColumnOverrides::for_row_count(1);
        // Hold the scalar unchanged to isolate the missing array cell's
        // fallback to the existing row.
        overrides.insert_values("ID", vec![Some(Value::Scalar(ScalarValue::Int32(11)))]);
        overrides.insert_values("DATA", vec![None]);
        table
            .prepare_write()
            .save_with_column_overrides(
                TableOptions::new(root.path()).with_data_manager(DataManagerKind::StandardStMan),
                &std::collections::HashMap::new(),
                &overrides,
            )
            .unwrap();
        let result = Table::open(TableOptions::new(root.path())).unwrap();
        assert_eq!(
            result
                .column_accessor("DATA")
                .unwrap()
                .array_cell(0)
                .unwrap(),
            &original
        );
        assert_eq!(
            result
                .column_accessor("ID")
                .unwrap()
                .scalar_cell(0)
                .unwrap(),
            &ScalarValue::Int32(11)
        );
    }

    /// The string buckets lie between the data and index buckets; one that
    /// links to a next bucket is full, every one states its used and unused
    /// bytes, and the header names the last, where casacore appends the
    /// next string (`SSMBase::itsLastStringBucket`).
    #[test]
    fn string_buckets_are_linked_and_the_header_names_the_last() {
        let root = tempfile::tempdir().expect("tempdir");
        let col_descs = schema_columns()
            .iter()
            .map(ColumnDescContents::from_column_schema)
            .collect::<Vec<_>>();
        let records = (0..1500).map(row).collect::<Vec<_>>();
        let path = root.path().join("table.f0");
        write_ssm_file_columns(
            &path,
            &col_descs,
            &RowCells::new(&records, None, &col_descs),
            false,
            false,
            "SSM",
        )
        .expect("column writer");
        let mut file = File::open(&path).expect("open");
        let header = super::super::parse_ssm_header(&mut file).expect("header");
        let indices = super::super::parse_ssm_indices(&mut file, &header).expect("index");
        // The data buckets are the index's; the string buckets follow.
        let first = indices[0].bucket_number.len() as i32;
        let last = header.last_string_bucket;
        assert_eq!(last, header.first_idx_bucket - 1);
        assert!(last > first, "several string buckets");
        let capacity = header.bucket_size as i32 - 16;
        let ints = |bucket: &[u8], at: usize| {
            i32::from_be_bytes(bucket[at..at + 4].try_into().expect("four bytes"))
        };
        let mut linked = 0;
        for number in first..=last {
            let bucket =
                super::super::read_bucket(&mut file, &header, number as u32).expect("bucket");
            let (used, unused, next) = (ints(&bucket, 4), ints(&bucket, 8), ints(&bucket, 12));
            assert_eq!(used + unused, capacity, "bucket {number}");
            if next >= 0 {
                assert_eq!(next, number + 1, "bucket {number} links the next");
                assert_eq!(used, capacity, "bucket {number} links on, so it is full");
                linked += 1;
            }
            if number == last {
                assert_eq!(next, -1, "the last string bucket links nowhere");
            }
        }
        assert!(linked >= 2, "long strings continue in linked buckets");
    }

    /// The writer of fixed-width scalar groups, which streams its index
    /// as the column writer does, writes the in-memory writer's bytes too.
    #[test]
    fn the_fixed_scalar_writer_writes_the_in_memory_writers_bytes() {
        let root = tempfile::tempdir().expect("tempdir");
        let col_descs = schema_columns()
            .iter()
            .filter(|schema| ["I32", "F64", "FLAG", "C32"].contains(&schema.name()))
            .map(ColumnDescContents::from_column_schema)
            .collect::<Vec<_>>();
        for (rows, big_endian) in [(0, false), (5, true), (20_000, false), (20_000, true)] {
            let records = (0..rows).map(row).collect::<Vec<_>>();
            let columns = col_descs
                .iter()
                .map(|desc| {
                    records
                        .iter()
                        .map(|record| match record.get(&desc.col_name) {
                            Some(Value::Scalar(scalar)) => Some(scalar.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let columns = columns.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let reference = root.path().join(format!("reference-{rows}-{big_endian}"));
            let streamed = root.path().join(format!("streamed-{rows}-{big_endian}"));
            let reference_blob = super::super::write_ssm_file_in_memory(
                &reference, &col_descs, &records, None, None, big_endian,
            )
            .expect("in-memory writer");
            let streamed_blob = super::super::write_ssm_fixed_scalar_sources_streaming(
                &streamed,
                &col_descs,
                &ColumnSources::materialized(rows, &columns),
                big_endian,
                "SSM",
            )
            .expect("fixed scalar writer");
            assert_eq!(reference_blob, streamed_blob, "DM blob, {rows} rows");
            assert!(
                std::fs::read(&reference).unwrap() == std::fs::read(&streamed).unwrap(),
                "{rows} rows, big_endian {big_endian}: files differ"
            );
        }
    }
}
