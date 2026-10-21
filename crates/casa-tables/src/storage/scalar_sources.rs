// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scalar column sources for StandardStMan and IncrementalStMan writers.
//!
//! A source is a column's values in row order: held in memory, generated, or
//! streamed from a construction file. The storage-manager writers read a
//! source a run of rows at a time as encoded cells, so a streamed column of
//! fixed-width values is decoded in blocks, without a [`ScalarValue`] per
//! cell. What a save holds for its streamed columns is bounded
//! ([`streamed_scalar_columns_resident_bytes`]).

use std::cell::RefCell;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::PathBuf;

use casa_types::ScalarValue;

use super::StorageError;
use super::data_type::CasacoreDataType;
use super::incremental_stman::ISM_SCALAR_BUCKET_BYTES_PER_COLUMN;
use super::standard_stman::DEFAULT_SSM_BUCKET_SIZE;
use super::table_control::ColumnDescContents;
use crate::table::{
    ColumnOverride, ColumnOverrides, GeneratedScalarColumn, GeneratedScalarValueRun,
    STREAMING_SCALAR_COLUMN_BUFFER_BYTES, StreamedScalarColumn, StreamedScalarType,
};

/// Cells a streamed source decodes per read, and a storage-manager writer
/// encodes per block.
pub(crate) const SCALAR_BLOCK_CELLS: usize = 4096;

// A block of Bool cells ends on a byte of packed bits.
const _: () = assert!(SCALAR_BLOCK_CELLS.is_multiple_of(8));

/// Bytes of the buffer a streamed source reads its construction file
/// through.
const READER_BUFFER_BYTES: usize = 64 * 1024;

/// The widest construction record, a validity byte and a `Float64`.
const MAX_RECORD_BYTES: usize = StreamedScalarType::Float64.record_bytes();

/// The widest encoded cell, a `Float64`.
const MAX_CELL_BYTES: usize = MAX_RECORD_BYTES - 1;

/// Heap a table save holds for each streamed scalar column while it writes
/// the column's storage-manager group: the source's read buffer and block
/// of construction records, the column's block of encoded cells (an
/// IncrementalStMan writer keeps one per column), and its share of the
/// group's IncrementalStMan bucket.
const SAVE_BYTES_PER_COLUMN: usize = READER_BUFFER_BYTES
    + SCALAR_BLOCK_CELLS * MAX_RECORD_BYTES
    + SCALAR_BLOCK_CELLS * MAX_CELL_BYTES
    + ISM_SCALAR_BUCKET_BYTES_PER_COLUMN;

/// Heap a table save holds once, for the group it is writing: a
/// StandardStMan bucket and the block of encoded cells it is filled from.
const SAVE_BYTES_PER_GROUP: usize =
    DEFAULT_SSM_BUCKET_SIZE as usize + SCALAR_BLOCK_CELLS * MAX_CELL_BYTES;

/// Heap that `column_count` streamed scalar columns hold at most: the
/// larger of what their construction writers hold while rows are appended
/// ([`STREAMING_SCALAR_COLUMN_BUFFER_BYTES`] each) and what a table save
/// holds while it encodes them into StandardStMan or IncrementalStMan
/// buckets. The writers are finished before the save opens the sources, so
/// the two are never held at once. A planner reserves this beside its other
/// buffers (ADR-0008); `None` if it overflows.
///
/// The bound excludes each manager's index of its buckets, a few bytes per
/// bucket written.
#[doc(hidden)]
pub fn streamed_scalar_columns_resident_bytes(column_count: usize) -> Option<usize> {
    let appending = column_count.checked_mul(STREAMING_SCALAR_COLUMN_BUFFER_BYTES)?;
    let saving = if column_count == 0 {
        0
    } else {
        column_count
            .checked_mul(SAVE_BYTES_PER_COLUMN)?
            .checked_add(SAVE_BYTES_PER_GROUP)?
    };
    Some(appending.max(saving))
}

pub(crate) enum ScalarColumnSource<'a> {
    Materialized(Vec<Option<ScalarValue>>),
    Generated(&'a GeneratedScalarColumn),
    Streamed(RefCell<StreamedScalarColumnReader>),
}

impl<'a> ScalarColumnSource<'a> {
    pub(crate) fn value(&self, row: usize) -> Result<Option<ScalarValue>, StorageError> {
        match self {
            Self::Materialized(values) => Ok(values.get(row).cloned().unwrap_or(None)),
            Self::Generated(column) => Ok(column.value(row)),
            Self::Streamed(reader) => reader.borrow_mut().value(row),
        }
    }

    pub(crate) fn scalar_runs(&self) -> Option<&'a [GeneratedScalarValueRun]> {
        match self {
            Self::Materialized(_) => None,
            Self::Generated(column) => column.scalar_runs(),
            Self::Streamed(_) => None,
        }
    }

    /// Append the fixed-width encodings of cells `rows` to `out`, if this
    /// source can produce them for `data_type` without decoding each value;
    /// see [`StreamedScalarColumnReader::read_encoded`]. Returns whether it
    /// did.
    fn encoded_cells(
        &self,
        rows: Range<usize>,
        data_type: CasacoreDataType,
        big_endian: bool,
        out: &mut Vec<u8>,
    ) -> Result<bool, StorageError> {
        let Self::Streamed(reader) = self else {
            return Ok(false);
        };
        let mut reader = reader.borrow_mut();
        if !reader.encodes_as(data_type) {
            return Ok(false);
        }
        reader.read_encoded(rows, big_endian, out)?;
        Ok(true)
    }
}

pub(crate) struct StreamedScalarColumnReader {
    reader: BufReader<File>,
    path: PathBuf,
    row_count: usize,
    scalar_type: StreamedScalarType,
    next_row: usize,
    /// Construction records of one block read, at most
    /// [`SCALAR_BLOCK_CELLS`].
    records: Vec<u8>,
}

impl StreamedScalarColumnReader {
    fn open(column: &StreamedScalarColumn) -> Result<Self, StorageError> {
        let file = File::open(column.path())?;
        let expected_bytes = column
            .row_count()
            .checked_mul(column.scalar_type().record_bytes())
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "streamed scalar source {} byte length overflow",
                    column.path().display()
                ))
            })?;
        let actual_bytes = usize::try_from(file.metadata()?.len()).map_err(|_| {
            StorageError::FormatMismatch(format!(
                "streamed scalar source {} is too large for this platform",
                column.path().display()
            ))
        })?;
        if actual_bytes != expected_bytes {
            return Err(StorageError::FormatMismatch(format!(
                "streamed scalar source {} has {actual_bytes} bytes, expected {expected_bytes}",
                column.path().display()
            )));
        }
        Ok(Self {
            reader: BufReader::with_capacity(READER_BUFFER_BYTES, file),
            path: column.path().to_path_buf(),
            row_count: column.row_count(),
            scalar_type: column.scalar_type(),
            next_row: 0,
            records: Vec::new(),
        })
    }

    fn value(&mut self, row: usize) -> Result<Option<ScalarValue>, StorageError> {
        self.seek_to(row..row + 1)?;
        let mut record = [0u8; 9];
        self.reader
            .read_exact(&mut record[..self.scalar_type.record_bytes()])?;
        self.next_row = row + 1;
        if record[0] == 0 {
            return Ok(None);
        }
        let value = match self.scalar_type {
            StreamedScalarType::Bool => ScalarValue::Bool(record[1] != 0),
            StreamedScalarType::Int32 => {
                ScalarValue::Int32(i32::from_le_bytes(record[1..5].try_into().unwrap()))
            }
            StreamedScalarType::Float32 => {
                ScalarValue::Float32(f32::from_le_bytes(record[1..5].try_into().unwrap()))
            }
            StreamedScalarType::Float64 => {
                ScalarValue::Float64(f64::from_le_bytes(record[1..9].try_into().unwrap()))
            }
        };
        Ok(Some(value))
    }

    /// Whether this source's values are of the casacore type `data_type`.
    fn encodes_as(&self, data_type: CasacoreDataType) -> bool {
        matches!(
            (self.scalar_type, data_type),
            (StreamedScalarType::Bool, CasacoreDataType::TpBool)
                | (StreamedScalarType::Int32, CasacoreDataType::TpInt)
                | (StreamedScalarType::Float32, CasacoreDataType::TpFloat)
                | (StreamedScalarType::Float64, CasacoreDataType::TpDouble)
        )
    }

    /// Append the encodings of cells `rows` to `out`, as the storage-manager
    /// writers encode a scalar of this type: the value in the table's byte
    /// order (a `Bool` as one 0 or 1 byte), and zeros for an undefined cell.
    /// `out` grows once, to exactly what the cells need.
    fn read_encoded(
        &mut self,
        rows: Range<usize>,
        big_endian: bool,
        out: &mut Vec<u8>,
    ) -> Result<(), StorageError> {
        self.seek_to(rows.clone())?;
        let record_bytes = self.scalar_type.record_bytes();
        out.reserve_exact(rows.len() * (record_bytes - 1));
        let undefined = [0u8; 8];
        let undefined = &undefined[..record_bytes - 1];
        let mut first = rows.start;
        while first < rows.end {
            let cells = (rows.end - first).min(SCALAR_BLOCK_CELLS);
            self.records.resize(cells * record_bytes, 0);
            self.reader.read_exact(&mut self.records)?;
            for record in self.records.chunks_exact(record_bytes) {
                let payload = &record[1..];
                if record[0] == 0 {
                    out.extend_from_slice(undefined);
                } else if self.scalar_type == StreamedScalarType::Bool {
                    out.push(u8::from(payload[0] != 0));
                } else if big_endian {
                    out.extend(payload.iter().rev());
                } else {
                    out.extend_from_slice(payload);
                }
            }
            first += cells;
        }
        self.next_row = rows.end;
        Ok(())
    }

    /// Position the reader at the first of `rows`, which must exist.
    fn seek_to(&mut self, rows: Range<usize>) -> Result<(), StorageError> {
        if rows.end > self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed scalar source {} row {} exceeds {} rows",
                self.path.display(),
                rows.end - 1,
                self.row_count
            )));
        }
        if rows.start != self.next_row {
            let offset = rows
                .start
                .checked_mul(self.scalar_type.record_bytes())
                .ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "streamed scalar source {} row offset overflow",
                        self.path.display()
                    ))
                })?;
            self.reader.seek(SeekFrom::Start(offset as u64))?;
        }
        Ok(())
    }
}

pub(crate) struct ScalarColumnSources<'a> {
    row_count: usize,
    columns: Vec<ScalarColumnSource<'a>>,
}

impl<'a> ScalarColumnSources<'a> {
    /// Sources holding `columns`, each of `row_count` values.
    pub(crate) fn materialized(row_count: usize, columns: &[&[Option<ScalarValue>]]) -> Self {
        Self {
            row_count,
            columns: columns
                .iter()
                .map(|values| ScalarColumnSource::Materialized(values.to_vec()))
                .collect(),
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn value(
        &self,
        column: usize,
        row: usize,
    ) -> Result<Option<ScalarValue>, StorageError> {
        self.columns[column].value(row)
    }

    pub(crate) fn scalar_runs(&self, column: usize) -> Option<&'a [GeneratedScalarValueRun]> {
        self.columns[column].scalar_runs()
    }

    /// Append the fixed-width encodings of `column`'s cells `rows` to `out`,
    /// if its source produces them without decoding each value (a streamed
    /// source of the column's type). Returns whether it did; otherwise the
    /// caller encodes [`Self::value`] cell by cell.
    pub(crate) fn encoded_cells(
        &self,
        column: usize,
        rows: Range<usize>,
        data_type: CasacoreDataType,
        big_endian: bool,
        out: &mut Vec<u8>,
    ) -> Result<bool, StorageError> {
        self.columns[column].encoded_cells(rows, data_type, big_endian, out)
    }
}

/// One column's encoded cells over a run of rows, reused from run to run.
#[derive(Default)]
pub(crate) struct EncodedCells {
    bytes: Vec<u8>,
    /// End of each cell in `bytes`; empty when every cell is `width` bytes.
    ends: Vec<usize>,
    width: usize,
}

impl EncodedCells {
    /// Encode `column`'s cells `rows`: in blocks when the source allows,
    /// otherwise by `encode` on each value.
    pub(crate) fn fill(
        &mut self,
        sources: &ScalarColumnSources<'_>,
        column: usize,
        rows: Range<usize>,
        data_type: CasacoreDataType,
        big_endian: bool,
        encode: impl Fn(Option<&ScalarValue>) -> Vec<u8>,
    ) -> Result<(), StorageError> {
        self.bytes.clear();
        self.ends.clear();
        let cells = rows.len();
        if sources.encoded_cells(column, rows.clone(), data_type, big_endian, &mut self.bytes)? {
            self.width = self.bytes.len() / cells.max(1);
            return Ok(());
        }
        for row in rows {
            let value = sources.value(column, row)?;
            self.bytes.extend_from_slice(&encode(value.as_ref()));
            self.ends.push(self.bytes.len());
        }
        Ok(())
    }

    /// The encoding of the `index`th cell of the run.
    pub(crate) fn cell(&self, index: usize) -> &[u8] {
        if self.ends.is_empty() {
            &self.bytes[index * self.width..(index + 1) * self.width]
        } else {
            let start = index
                .checked_sub(1)
                .map_or(0, |previous| self.ends[previous]);
            &self.bytes[start..self.ends[index]]
        }
    }

    /// Every cell's encoding, back to back.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub(super) fn scalar_override_columns_for_group<'a>(
    group_col_descs: &[ColumnDescContents],
    column_overrides: &'a ColumnOverrides,
    row_count: usize,
) -> Result<Option<ScalarColumnSources<'a>>, StorageError> {
    let mut columns = Vec::with_capacity(group_col_descs.len());
    for desc in group_col_descs {
        if desc.is_array || desc.is_record() {
            return Ok(None);
        }
        let Some(override_value) = column_overrides.get(&desc.col_name) else {
            return Ok(None);
        };
        match override_value {
            ColumnOverride::Values(values) => {
                let mut scalar_values = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        Some(casa_types::Value::Scalar(value)) => {
                            scalar_values.push(Some(value.clone()));
                        }
                        Some(other) => {
                            return Err(StorageError::FormatMismatch(format!(
                                "column override {} expected scalar values, found {:?}",
                                desc.col_name,
                                other.kind()
                            )));
                        }
                        None => scalar_values.push(None),
                    }
                }
                columns.push(ScalarColumnSource::Materialized(scalar_values));
            }
            ColumnOverride::GeneratedScalar(column) => {
                columns.push(ScalarColumnSource::Generated(column));
            }
            ColumnOverride::StreamedScalar(column) => {
                columns.push(ScalarColumnSource::Streamed(RefCell::new(
                    StreamedScalarColumnReader::open(column)?,
                )));
            }
            ColumnOverride::Deferred => return Ok(None),
        }
    }
    Ok(Some(ScalarColumnSources { row_count, columns }))
}
