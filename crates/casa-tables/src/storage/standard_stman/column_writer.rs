// SPDX-License-Identifier: LGPL-3.0-or-later
//! The StandardStMan file writer for any mix of columns, with bounded
//! memory.
//!
//! The columns are written one after another, each over all its rows, which
//! fixes where their shared storage goes: indirect arrays and records are
//! appended to the array file (`table.fNi`) and long strings to the string
//! buckets in that column-major order. Each column's slice of a data bucket
//! is written at its place in the file as soon as it is full, and each
//! string bucket once the next one starts, so the writer holds one slice,
//! one string bucket and the array file's buffer whatever the row count.
//! The file is laid out as casacore's `SSMBase` reads it: the 512-byte
//! header, the data buckets, the string buckets, then the index buckets.

use std::borrow::Cow;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use casa_types::{ArrayValue, RecordValue, Value};
use ndarray::{ArrayD, IxDyn};

use super::{
    CasacoreDataType, ColumnDescContents, ColumnSources, SSM_HEADER_SIZE, StManArrayFileWriter,
    StorageError, canonical_element_size, is_ssm_fixed_string, is_ssm_indirect,
    is_ssm_variable_string, new_ssm_bucket_layout, serialize_ssm_dm_blob, serialize_ssm_header,
    serialize_ssm_index, ssm_cell_element_count, write_cell_to_bucket, write_i64_be, write_i64_le,
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

/// The string buckets, written to the file as each is finished.
///
/// String data is only ever appended to the last bucket, so the ones before
/// it are final; they are written at their place after the data buckets
/// and their memory released, leaving empty placeholders that keep the
/// bucket numbers.
struct StringBuckets {
    buckets: Vec<Vec<u8>>,
    written: usize,
    first_bucket: usize,
    bucket_bytes: usize,
}

impl StringBuckets {
    /// Write the finished buckets, and with `all` the last one too.
    fn write(&mut self, file: &mut File, all: bool) -> Result<(), StorageError> {
        let end = if all {
            self.buckets.len()
        } else {
            self.buckets.len().saturating_sub(1)
        };
        if self.written >= end {
            return Ok(());
        }
        file.seek(SeekFrom::Start(bucket_offset(
            self.first_bucket + self.written,
            self.bucket_bytes,
        )))?;
        for bucket in &mut self.buckets[self.written..end] {
            file.write_all(bucket)?;
            *bucket = Vec::new();
        }
        self.written = end;
        Ok(())
    }
}

/// File offset of bucket `bucket`.
fn bucket_offset(bucket: usize, bucket_bytes: usize) -> u64 {
    SSM_HEADER_SIZE + (bucket * bucket_bytes) as u64
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
    let (rows_per_bucket, bucket_size) = new_ssm_bucket_layout(&bits, nrrow);
    let bucket_rows = rows_per_bucket as usize;
    let bucket_bytes = bucket_size as usize;
    let mut column_offsets = Vec::with_capacity(ncol);
    let mut offset = 0u32;
    for &column_bits in &bits {
        column_offsets.push(offset);
        offset += (bucket_rows * column_bits).div_ceil(8) as u32;
    }
    let nr_data_buckets = nrrow.div_ceil(bucket_rows);

    let mut file = File::create(file_path)?;
    let mut strings = StringBuckets {
        buckets: Vec::new(),
        written: 0,
        first_bucket: nr_data_buckets,
        bucket_bytes,
    };
    let mut slice = Vec::new();
    for (column, desc) in col_descs.iter().enumerate() {
        let indirect = is_ssm_indirect(desc);
        let nrelem = if indirect {
            1
        } else {
            ssm_cell_element_count(desc)
        };
        for bucket in 0..nr_data_buckets {
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
                        &mut strings.buckets,
                        nr_data_buckets,
                        bucket_bytes,
                        big_endian,
                    );
                    strings.write(&mut file, false)?;
                }
            }
            file.seek(SeekFrom::Start(
                bucket_offset(bucket, bucket_bytes) + u64::from(column_offsets[column]),
            ))?;
            file.write_all(&slice)?;
        }
    }
    strings.write(&mut file, true)?;
    if let Some(writer) = array_writer.as_mut() {
        writer.finish()?;
    }
    let nr_string_buckets = strings.buckets.len();
    write_index_and_header(
        &mut file,
        IndexLayout {
            nrrow,
            ncol,
            rows_per_bucket,
            bucket_size,
            nr_data_buckets,
            nr_string_buckets,
        },
        big_endian,
    )?;
    serialize_ssm_dm_blob("SSM", &column_offsets, ncol)
}

/// Append an indirect cell to the array file and return its offset there:
/// a record as its serialized bytes, an array as itself, and 0 for an
/// undefined cell.
fn write_indirect_cell(
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

/// What the index and header of a StandardStMan file describe.
struct IndexLayout {
    nrrow: usize,
    ncol: usize,
    rows_per_bucket: u32,
    bucket_size: u32,
    nr_data_buckets: usize,
    nr_string_buckets: usize,
}

/// Write the index buckets after the data and string buckets, then the
/// header, and size the file to its buckets.
fn write_index_and_header(
    file: &mut File,
    layout: IndexLayout,
    big_endian: bool,
) -> Result<(), StorageError> {
    let IndexLayout {
        nrrow,
        ncol,
        rows_per_bucket,
        bucket_size,
        nr_data_buckets,
        nr_string_buckets,
    } = layout;
    let bucket_bytes = bucket_size as usize;
    let bucket_rows = rows_per_bucket as usize;
    let last_row = (0..nr_data_buckets)
        .map(|bucket| (((bucket + 1) * bucket_rows - 1).min(nrrow - 1)) as u64)
        .collect::<Vec<_>>();
    let bucket_number = (0..nr_data_buckets as u32).collect::<Vec<_>>();
    let index_data = serialize_ssm_index(
        nr_data_buckets as u32,
        rows_per_bucket,
        ncol as i32,
        &last_row,
        &bucket_number,
        big_endian,
    )?;
    // Each index bucket starts with a check number and the next bucket.
    let chain_overhead = 8usize;
    let data_per_index_bucket = bucket_bytes - chain_overhead;
    let nr_idx_buckets = index_data.len().div_ceil(data_per_index_bucket);
    let first_string_bucket = if nr_string_buckets > 0 {
        nr_data_buckets as i32
    } else {
        -1
    };
    let first_idx_bucket = nr_data_buckets + nr_string_buckets;
    let nr_buckets = first_idx_bucket + nr_idx_buckets;
    let header = serialize_ssm_header(
        bucket_size,
        nr_buckets as u32,
        0,
        -1,
        nr_idx_buckets as u32,
        first_idx_bucket as i32,
        0,
        first_string_bucket,
        index_data.len() as u32,
        1,
        big_endian,
    )?;

    file.seek(SeekFrom::Start(bucket_offset(
        first_idx_bucket,
        bucket_bytes,
    )))?;
    let mut bucket = vec![0u8; bucket_bytes];
    for (index, chunk) in index_data.chunks(data_per_index_bucket).enumerate() {
        bucket.fill(0);
        let next = if index + 1 < nr_idx_buckets {
            (first_idx_bucket + index + 1) as i32
        } else {
            -1
        };
        bucket[4..8].copy_from_slice(&next.to_be_bytes());
        bucket[chain_overhead..chain_overhead + chunk.len()].copy_from_slice(chunk);
        file.write_all(&bucket)?;
    }
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    // Bucket bytes no column or string wrote are zero.
    file.set_len(bucket_offset(nr_buckets, bucket_bytes))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! The column writer writes the bytes of the writer it replaced, which
    //! assembled every bucket in memory, for every kind of column.

    use casa_types::{Complex32, PrimitiveType, RecordField, ScalarValue};
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
    /// which are left undefined in some rows.
    fn row(row: usize) -> RecordValue {
        let r = row as i32;
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
                Value::Scalar(ScalarValue::String(format!(
                    "a longer name for row {row} {}",
                    "x".repeat(row % 40)
                ))),
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
                    vec![format!("s{row}"), "a rather longer string".to_string()],
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
            )
            .expect("column writer");
            assert_eq!(reference_blob, streamed_blob, "DM blob, {rows} rows");
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
}
