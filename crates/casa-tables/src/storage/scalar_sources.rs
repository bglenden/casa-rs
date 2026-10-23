// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column sources for StandardStMan and IncrementalStMan writers.
//!
//! A source is a column's values in row order: held in memory, generated, or
//! streamed from a construction file. The storage-manager writers read a
//! source a run of rows at a time as encoded cells, so a streamed column of
//! fixed-width values is decoded in blocks, without a [`ScalarValue`] per
//! cell. Array columns have sources too (see `array_sources`): their
//! materialized cells or a [`GeneratedArrayColumn`](crate::table::GeneratedArrayColumn).
//! What a save holds for its streamed columns is bounded
//! ([`streamed_scalar_columns_resident_bytes`]).

use std::borrow::Cow;
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::PathBuf;

use casa_types::{RecordValue, ScalarValue, Value};

use super::StorageError;
use super::array_sources::GeneratedArraySource;
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

pub(crate) enum ColumnSource<'a> {
    Materialized(Vec<Option<ScalarValue>>),
    Generated(&'a GeneratedScalarColumn),
    Streamed(RefCell<StreamedScalarColumnReader>),
    /// The materialized cells of an array column.
    Cells(&'a [Option<Value>]),
    /// A generated array column, its cells checked as they are read.
    GeneratedArray(GeneratedArraySource<'a>),
    /// A column of row records, scalar or array.
    RowFields(RowFields<'a>),
}

impl<'a> ColumnSource<'a> {
    pub(crate) fn value(&self, row: usize) -> Result<Option<ScalarValue>, StorageError> {
        match self {
            Self::Materialized(values) => Ok(values.get(row).cloned().unwrap_or(None)),
            Self::Generated(column) => Ok(column.value(row)),
            Self::Streamed(reader) => reader.borrow_mut().value(row),
            Self::RowFields(fields) => match fields.cell(row) {
                None => Ok(None),
                Some(Value::Scalar(value)) => Ok(Some(value.clone())),
                Some(other) => Err(StorageError::FormatMismatch(format!(
                    "column {} row {row} holds {:?}, not a scalar",
                    fields.name,
                    other.kind()
                ))),
            },
            Self::Cells(_) | Self::GeneratedArray(_) => Err(StorageError::FormatMismatch(
                "an array column source has no scalar values".to_string(),
            )),
        }
    }

    /// The cell in `row`, `None` when it is undefined.
    pub(crate) fn cell(&self, row: usize) -> Result<Option<Cow<'a, Value>>, StorageError> {
        match self {
            Self::Cells(values) => Ok(values.get(row).and_then(Option::as_ref).map(Cow::Borrowed)),
            Self::GeneratedArray(source) => source.cell(row),
            Self::RowFields(fields) => Ok(fields.cell(row).map(Cow::Borrowed)),
            _ => Ok(self
                .value(row)?
                .map(|value| Cow::Owned(Value::Scalar(value)))),
        }
    }

    pub(crate) fn scalar_runs(&self) -> Option<&'a [GeneratedScalarValueRun]> {
        match self {
            Self::Generated(column) => column.scalar_runs(),
            Self::Materialized(_)
            | Self::Streamed(_)
            | Self::Cells(_)
            | Self::GeneratedArray(_)
            | Self::RowFields(_) => None,
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

/// A column of row records: in each row, the field of the column's name,
/// found at its index in the record when the save gives one and by name
/// otherwise; a row without the field is undefined.
pub(crate) struct RowFields<'a> {
    rows: &'a [RecordValue],
    field_index: Option<usize>,
    name: &'a str,
}

impl<'a> RowFields<'a> {
    /// Column `name` of `rows`, expected at `field_index` in each record.
    pub(crate) fn new(rows: &'a [RecordValue], field_index: Option<usize>, name: &'a str) -> Self {
        Self {
            rows,
            field_index,
            name,
        }
    }

    fn cell(&self, row: usize) -> Option<&'a Value> {
        let fields = self.rows.get(row)?.fields();
        self.field_index
            .and_then(|index| fields.get(index))
            .filter(|field| field.name == self.name)
            .or_else(|| fields.iter().find(|field| field.name == self.name))
            .map(|field| &field.value)
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

pub(crate) struct ColumnSources<'a> {
    row_count: usize,
    columns: Vec<ColumnSource<'a>>,
}

impl<'a> ColumnSources<'a> {
    /// Sources holding `columns`, each of `row_count` values.
    pub(crate) fn materialized(row_count: usize, columns: &[&[Option<ScalarValue>]]) -> Self {
        Self {
            row_count,
            columns: columns
                .iter()
                .map(|values| ColumnSource::Materialized(values.to_vec()))
                .collect(),
        }
    }

    /// Sources of `row_count` rows, one per column of the group.
    pub(super) fn new(row_count: usize, columns: Vec<ColumnSource<'a>>) -> Self {
        Self { row_count, columns }
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

    /// The cell of `column` in `row`, `None` when it is undefined.
    pub(crate) fn cell(
        &self,
        column: usize,
        row: usize,
    ) -> Result<Option<Cow<'a, Value>>, StorageError> {
        self.columns[column].cell(row)
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
    /// otherwise by `encode` on each cell.
    pub(crate) fn fill(
        &mut self,
        sources: &ColumnSources<'_>,
        column: usize,
        rows: Range<usize>,
        data_type: CasacoreDataType,
        big_endian: bool,
        encode: impl Fn(Option<&Value>) -> Vec<u8>,
    ) -> Result<(), StorageError> {
        self.bytes.clear();
        self.ends.clear();
        let cells = rows.len();
        if sources.encoded_cells(column, rows.clone(), data_type, big_endian, &mut self.bytes)? {
            self.width = self.bytes.len() / cells.max(1);
            return Ok(());
        }
        for row in rows {
            let value = sources.cell(column, row)?;
            self.bytes.extend_from_slice(&encode(value.as_deref()));
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
) -> Result<Option<ColumnSources<'a>>, StorageError> {
    let mut columns = Vec::with_capacity(group_col_descs.len());
    for desc in group_col_descs {
        if desc.is_array || desc.is_record() {
            return Ok(None);
        }
        let Some(override_value) = column_overrides.get(&desc.col_name) else {
            return Ok(None);
        };
        let Some(source) = scalar_column_source(desc, override_value)? else {
            return Ok(None);
        };
        columns.push(source);
    }
    Ok(Some(ColumnSources { row_count, columns }))
}

/// The source of scalar column `desc` from its override, or `None` for a
/// deferred column. A generated array override is refused.
pub(super) fn scalar_column_source<'a>(
    desc: &ColumnDescContents,
    override_value: &'a ColumnOverride,
) -> Result<Option<ColumnSource<'a>>, StorageError> {
    Ok(Some(match override_value {
        ColumnOverride::Values(values) => {
            let mut scalar_values = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(Value::Scalar(value)) => scalar_values.push(Some(value.clone())),
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
            ColumnSource::Materialized(scalar_values)
        }
        ColumnOverride::GeneratedScalar(column) => ColumnSource::Generated(column),
        ColumnOverride::GeneratedArray(_) => {
            return Err(StorageError::FormatMismatch(format!(
                "scalar column {} cannot take a generated array override",
                desc.col_name
            )));
        }
        ColumnOverride::StreamedScalar(column) => {
            ColumnSource::Streamed(RefCell::new(StreamedScalarColumnReader::open(column)?))
        }
        ColumnOverride::Deferred => return Ok(None),
    }))
}
