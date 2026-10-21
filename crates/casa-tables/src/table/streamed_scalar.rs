// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded construction sources for fixed-width scalar columns.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use casa_types::ScalarValue;

use super::TableError;

/// Fixed-width scalar type accepted by [`StreamingScalarColumnWriter`].
///
/// This construction-only representation is decoded into normal casacore
/// scalar values before the selected storage manager writes its data file. It
/// is not a persisted table format.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamedScalarType {
    Bool,
    Int32,
    Float32,
    Float64,
}

impl StreamedScalarType {
    pub(crate) const fn payload_bytes(self) -> usize {
        match self {
            Self::Bool => 1,
            Self::Int32 | Self::Float32 => 4,
            Self::Float64 => 8,
        }
    }

    pub(crate) const fn record_bytes(self) -> usize {
        1 + self.payload_bytes()
    }

    fn matches(self, value: &ScalarValue) -> bool {
        matches!(
            (self, value),
            (Self::Bool, ScalarValue::Bool(_))
                | (Self::Int32, ScalarValue::Int32(_))
                | (Self::Float32, ScalarValue::Float32(_))
                | (Self::Float64, ScalarValue::Float64(_))
        )
    }
}

/// A run of defined cells for a [`StreamingScalarColumnWriter`], in row
/// order: the columnar counterpart of pushing one cell at a time.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub enum StreamedScalarValues<'a> {
    Bool(&'a [bool]),
    Int32(&'a [i32]),
    Float32(&'a [f32]),
    Float64(&'a [f64]),
}

impl StreamedScalarValues<'_> {
    /// Number of cells.
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(values) => values.len(),
            Self::Int32(values) => values.len(),
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
        }
    }

    /// Whether the run holds no cells.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn scalar_type(&self) -> StreamedScalarType {
        match self {
            Self::Bool(_) => StreamedScalarType::Bool,
            Self::Int32(_) => StreamedScalarType::Int32,
            Self::Float32(_) => StreamedScalarType::Float32,
            Self::Float64(_) => StreamedScalarType::Float64,
        }
    }

    /// Write the construction records of cells `range` to the start of
    /// `out`, which must hold them; returns their length in bytes.
    fn encode(&self, range: std::ops::Range<usize>, out: &mut [u8]) -> usize {
        fn records<const N: usize>(values: impl Iterator<Item = [u8; N]>, out: &mut [u8]) -> usize {
            let mut length = 0;
            for (payload, record) in values.zip(out.chunks_exact_mut(N + 1)) {
                record[0] = 1;
                record[1..].copy_from_slice(&payload);
                length += N + 1;
            }
            length
        }
        match self {
            Self::Bool(values) => records(values[range].iter().map(|&v| [u8::from(v)]), out),
            Self::Int32(values) => records(values[range].iter().map(|v| v.to_le_bytes()), out),
            Self::Float32(values) => records(values[range].iter().map(|v| v.to_le_bytes()), out),
            Self::Float64(values) => records(values[range].iter().map(|v| v.to_le_bytes()), out),
        }
    }
}

/// Completed bounded scalar-column construction source.
///
/// The referenced file is a temporary input to table creation. The table
/// storage writer consumes it into StandardStMan or IncrementalStMan; callers
/// should then remove the temporary file before publishing the table.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct StreamedScalarColumn {
    path: PathBuf,
    row_count: usize,
    scalar_type: StreamedScalarType,
}

impl StreamedScalarColumn {
    /// Temporary construction file containing the column values.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of rows represented by the construction source.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Fixed scalar type represented by the construction source.
    pub fn scalar_type(&self) -> StreamedScalarType {
        self.scalar_type
    }
}

/// Append-only bounded writer for a fixed-width scalar column.
///
/// Values are written to a temporary construction file in row order. Each
/// record contains a validity byte followed by a fixed-width little-endian
/// payload, so undefined scalar cells remain representable.
#[doc(hidden)]
pub struct StreamingScalarColumnWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    expected_rows: usize,
    rows_written: usize,
    scalar_type: StreamedScalarType,
}

/// Resident buffer reserved by each [`StreamingScalarColumnWriter`].
#[doc(hidden)]
pub const STREAMING_SCALAR_COLUMN_BUFFER_BYTES: usize = 64 * 1024;

/// Cells a bulk append encodes per write, in a buffer on the stack.
const CHUNK_CELLS: usize = 256;

/// Bytes of [`CHUNK_CELLS`] records of the widest type.
const CHUNK_BYTES: usize = CHUNK_CELLS * StreamedScalarType::Float64.record_bytes();

impl StreamingScalarColumnWriter {
    /// Create a temporary scalar construction source.
    pub fn create(
        path: impl Into<PathBuf>,
        expected_rows: usize,
        scalar_type: StreamedScalarType,
    ) -> Result<Self, TableError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                TableError::Storage(format!(
                    "create scalar construction directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                TableError::Storage(format!(
                    "create scalar construction file {}: {error}",
                    path.display()
                ))
            })?;
        Ok(Self {
            path,
            writer: BufWriter::with_capacity(STREAMING_SCALAR_COLUMN_BUFFER_BYTES, file),
            expected_rows,
            rows_written: 0,
            scalar_type,
        })
    }

    /// Append one scalar cell, using `None` for an undefined cell.
    pub fn push(&mut self, value: Option<ScalarValue>) -> Result<(), TableError> {
        let record = self.record(value.as_ref())?;
        self.reserve_rows(1)?;
        self.write(&record[..self.scalar_type.record_bytes()])?;
        self.rows_written += 1;
        Ok(())
    }

    /// Append `values` as defined cells, in row order.
    pub fn push_values(&mut self, values: StreamedScalarValues<'_>) -> Result<(), TableError> {
        if values.scalar_type() != self.scalar_type {
            return Err(TableError::Storage(format!(
                "scalar construction source {} expected {:?}, found {:?} values",
                self.path.display(),
                self.scalar_type,
                values.scalar_type()
            )));
        }
        self.reserve_rows(values.len())?;
        let mut chunk = [0u8; CHUNK_BYTES];
        let mut first = 0;
        while first < values.len() {
            let end = values.len().min(first + CHUNK_CELLS);
            let length = values.encode(first..end, &mut chunk);
            self.write(&chunk[..length])?;
            first = end;
        }
        self.rows_written += values.len();
        Ok(())
    }

    /// Append `count` copies of one cell, using `None` for undefined cells.
    pub fn push_repeated(
        &mut self,
        value: Option<ScalarValue>,
        count: usize,
    ) -> Result<(), TableError> {
        let record = self.record(value.as_ref())?;
        let record = &record[..self.scalar_type.record_bytes()];
        self.reserve_rows(count)?;
        let mut chunk = [0u8; CHUNK_BYTES];
        for copy in chunk.chunks_exact_mut(record.len()).take(count) {
            copy.copy_from_slice(record);
        }
        let mut remaining = count;
        while remaining > 0 {
            let cells = remaining.min(CHUNK_CELLS);
            self.write(&chunk[..cells * record.len()])?;
            remaining -= cells;
        }
        self.rows_written += count;
        Ok(())
    }

    /// Finish the construction source after exactly the planned row count.
    pub fn finish(mut self) -> Result<StreamedScalarColumn, TableError> {
        if self.rows_written != self.expected_rows {
            return Err(TableError::Storage(format!(
                "scalar construction source {} has {} rows, expected {}",
                self.path.display(),
                self.rows_written,
                self.expected_rows
            )));
        }
        self.writer.flush().map_err(|error| {
            TableError::Storage(format!(
                "flush scalar construction file {}: {error}",
                self.path.display()
            ))
        })?;
        Ok(StreamedScalarColumn {
            path: self.path,
            row_count: self.rows_written,
            scalar_type: self.scalar_type,
        })
    }

    /// The construction record of one cell, validated against the column
    /// type; only the first [`StreamedScalarType::record_bytes`] are used.
    fn record(&self, value: Option<&ScalarValue>) -> Result<[u8; 9], TableError> {
        let mut record = [0u8; 9];
        let Some(value) = value else {
            return Ok(record);
        };
        if !self.scalar_type.matches(value) {
            return Err(TableError::Storage(format!(
                "scalar construction source {} expected {:?}, found {value:?}",
                self.path.display(),
                self.scalar_type
            )));
        }
        record[0] = 1;
        match value {
            ScalarValue::Bool(value) => record[1] = u8::from(*value),
            ScalarValue::Int32(value) => record[1..5].copy_from_slice(&value.to_le_bytes()),
            ScalarValue::Float32(value) => record[1..5].copy_from_slice(&value.to_le_bytes()),
            ScalarValue::Float64(value) => record[1..9].copy_from_slice(&value.to_le_bytes()),
            _ => unreachable!("scalar type was validated above"),
        }
        Ok(record)
    }

    fn reserve_rows(&self, rows: usize) -> Result<(), TableError> {
        if rows > self.expected_rows - self.rows_written {
            return Err(TableError::Storage(format!(
                "scalar construction source {} received more than {} rows",
                self.path.display(),
                self.expected_rows
            )));
        }
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), TableError> {
        self.writer.write_all(bytes).map_err(|error| {
            TableError::Storage(format!(
                "write scalar construction file {}: {error}",
                self.path.display()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// More cells than one bulk chunk, so the chunk boundary is exercised.
    const ROWS: usize = 2 * CHUNK_CELLS + 17;

    fn construction_bytes(
        dir: &Path,
        name: &str,
        scalar_type: StreamedScalarType,
        fill: impl FnOnce(&mut StreamingScalarColumnWriter),
    ) -> Vec<u8> {
        let mut writer = StreamingScalarColumnWriter::create(dir.join(name), ROWS, scalar_type)
            .expect("create writer");
        fill(&mut writer);
        let column = writer.finish().expect("finish writer");
        std::fs::read(column.path()).expect("read construction file")
    }

    /// Cell `row` of `values`, as a single push sends it.
    fn cell(values: StreamedScalarValues<'_>, row: usize) -> ScalarValue {
        match values {
            StreamedScalarValues::Bool(v) => ScalarValue::Bool(v[row]),
            StreamedScalarValues::Int32(v) => ScalarValue::Int32(v[row]),
            StreamedScalarValues::Float32(v) => ScalarValue::Float32(v[row]),
            StreamedScalarValues::Float64(v) => ScalarValue::Float64(v[row]),
        }
    }

    fn split(
        values: StreamedScalarValues<'_>,
        at: usize,
    ) -> (StreamedScalarValues<'_>, StreamedScalarValues<'_>) {
        match values {
            StreamedScalarValues::Bool(v) => (
                StreamedScalarValues::Bool(&v[..at]),
                StreamedScalarValues::Bool(&v[at..]),
            ),
            StreamedScalarValues::Int32(v) => (
                StreamedScalarValues::Int32(&v[..at]),
                StreamedScalarValues::Int32(&v[at..]),
            ),
            StreamedScalarValues::Float32(v) => (
                StreamedScalarValues::Float32(&v[..at]),
                StreamedScalarValues::Float32(&v[at..]),
            ),
            StreamedScalarValues::Float64(v) => (
                StreamedScalarValues::Float64(&v[..at]),
                StreamedScalarValues::Float64(&v[at..]),
            ),
        }
    }

    #[test]
    fn bulk_and_repeated_appends_write_the_records_of_single_pushes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let times = (0..ROWS).map(|row| row as f64 * 0.5).collect::<Vec<_>>();
        let antennas = (0..ROWS).map(|row| (row % 27) as i32).collect::<Vec<_>>();
        let flags = (0..ROWS).map(|row| row % 101 == 0).collect::<Vec<_>>();
        let gains = (0..ROWS).map(|row| row as f32 / 7.0).collect::<Vec<_>>();
        for (index, values) in [
            StreamedScalarValues::Float64(&times),
            StreamedScalarValues::Int32(&antennas),
            StreamedScalarValues::Bool(&flags),
            StreamedScalarValues::Float32(&gains),
        ]
        .into_iter()
        .enumerate()
        {
            let scalar_type = values.scalar_type();
            let single = construction_bytes(dir.path(), &format!("{index}.1"), scalar_type, |w| {
                for row in 0..ROWS {
                    w.push(Some(cell(values, row))).expect("push");
                }
            });
            let bulk = construction_bytes(dir.path(), &format!("{index}.n"), scalar_type, |w| {
                // Two uneven runs, so a run boundary falls inside a chunk.
                let (head, tail) = split(values, CHUNK_CELLS + 5);
                w.push_values(head).expect("push head");
                w.push_values(tail).expect("push tail");
            });
            assert_eq!(single.len(), ROWS * scalar_type.record_bytes());
            assert_eq!(single, bulk, "{scalar_type:?} bulk records");

            let constant = cell(values, 3);
            let single = construction_bytes(dir.path(), &format!("{index}.c1"), scalar_type, |w| {
                for row in 0..ROWS {
                    let defined = !row.is_multiple_of(5);
                    w.push(defined.then(|| constant.clone())).expect("push");
                }
            });
            let repeated =
                construction_bytes(dir.path(), &format!("{index}.cn"), scalar_type, |w| {
                    for row in (0..ROWS).step_by(5) {
                        w.push_repeated(None, 1).expect("undefined cell");
                        let run = (ROWS - row - 1).min(4);
                        w.push_repeated(Some(constant.clone()), run)
                            .expect("defined run");
                    }
                });
            assert_eq!(single, repeated, "{scalar_type:?} repeated records");
        }
    }

    #[test]
    fn appends_refuse_another_type_and_rows_past_the_plan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer =
            StreamingScalarColumnWriter::create(dir.path().join("t"), 4, StreamedScalarType::Int32)
                .expect("create writer");
        assert!(
            writer
                .push_values(StreamedScalarValues::Float64(&[1.0]))
                .is_err()
        );
        assert!(
            writer
                .push_repeated(Some(ScalarValue::Bool(true)), 2)
                .is_err()
        );
        writer
            .push_values(StreamedScalarValues::Int32(&[1, 2, 3]))
            .expect("three rows");
        assert!(
            writer
                .push_values(StreamedScalarValues::Int32(&[4, 5]))
                .is_err()
        );
        assert!(writer.push_repeated(None, 2).is_err());
        writer.push_repeated(None, 1).expect("fourth row");
        let column = writer.finish().expect("finish at the planned count");
        assert_eq!(column.row_count(), 4);
    }
}
