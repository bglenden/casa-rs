// SPDX-License-Identifier: LGPL-3.0-or-later
//! Private, invocation-local scratch for selected-row array updates.

use std::io::{self, BufReader, BufWriter, Read, Seek, Write};

use super::*;

/// Retain the first bounded batch as-is. Larger selections spill only the
/// subsequent calculated cells, in replay order, to an anonymous file.
#[derive(Default)]
pub(in crate::execute) struct StagedRows {
    first: Option<MeasurementSetMutationBatch>,
    spill: Option<BufWriter<File>>,
    columns: Vec<String>,
}

impl StagedRows {
    pub(super) fn push(&mut self, batch: MeasurementSetMutationBatch) -> io::Result<()> {
        if self.first.is_none() {
            self.columns = batch
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect();
            self.first = Some(batch);
            return Ok(());
        }
        let writer = match &mut self.spill {
            Some(writer) => writer,
            slot @ None => slot.insert(BufWriter::with_capacity(64 * 1024, tempfile::tempfile()?)),
        };
        for column in batch.columns {
            let MeasurementSetMutationColumnValues::Arrays(values) = column.values else {
                return Err(invalid("expected staged array column"));
            };
            for value in values {
                write_array(writer, &column.name, value)?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> io::Result<ReplayRows> {
        let reader = self
            .spill
            .map(|writer| {
                let mut file = writer.into_inner().map_err(|error| error.into_error())?;
                file.rewind()?;
                Ok::<_, io::Error>(BufReader::with_capacity(64 * 1024, file))
            })
            .transpose()?;
        Ok(ReplayRows {
            first: self.first,
            reader,
            columns: self.columns,
        })
    }
}

pub(super) struct ReplayRows {
    first: Option<MeasurementSetMutationBatch>,
    reader: Option<BufReader<File>>,
    columns: Vec<String>,
}

impl ReplayRows {
    pub(super) fn read(
        &mut self,
        rows: &[usize],
        staging_batch_rows: usize,
    ) -> io::Result<MeasurementSetMutationBatch> {
        let mut chunks = rows.chunks(staging_batch_rows);
        let first = chunks.next().expect("replay has selected rows");
        let mut batch = match self.first.take() {
            Some(batch) => batch,
            None => self.read_spill(first)?,
        };
        for chunk in chunks {
            let next = self.read_spill(chunk)?;
            batch.row_indices.extend(next.row_indices);
            for (column, next) in batch.columns.iter_mut().zip(next.columns) {
                let (
                    MeasurementSetMutationColumnValues::Arrays(arrays),
                    MeasurementSetMutationColumnValues::Arrays(next),
                ) = (&mut column.values, next.values)
                else {
                    return Err(invalid("expected staged array column"));
                };
                arrays.extend(next);
            }
        }
        Ok(batch)
    }

    fn read_spill(&mut self, rows: &[usize]) -> io::Result<MeasurementSetMutationBatch> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| invalid("missing staged data"))?;
        let columns = self
            .columns
            .iter()
            .map(|name| {
                let values = rows
                    .iter()
                    .map(|_| read_array(reader, name))
                    .collect::<io::Result<Vec<_>>>()?;
                Ok(MeasurementSetMutationColumnBatch {
                    name: name.clone(),
                    values: MeasurementSetMutationColumnValues::Arrays(values),
                })
            })
            .collect::<io::Result<_>>()?;
        Ok(MeasurementSetMutationBatch {
            row_indices: rows.to_vec(),
            columns,
        })
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn write_array(writer: &mut impl Write, name: &str, value: ArrayValue) -> io::Result<()> {
    let rank = if name == "WEIGHT" { 1 } else { 2 };
    if value.shape().len() != rank {
        return Err(invalid("unexpected staged array rank"));
    }
    for &dim in value.shape() {
        let dim = u32::try_from(dim).map_err(|_| invalid("staged array dimension exceeds u32"))?;
        writer.write_all(&dim.to_le_bytes())?;
    }
    // Reverse the axes so a Fortran-ordered cell can be encoded without
    // copying. The read side restores its first-axis-fastest layout.
    match (name, value) {
        ("CORRECTED_DATA", ArrayValue::Complex32(array)) => {
            let array = array.reversed_axes();
            write_values(
                writer,
                array
                    .as_standard_layout()
                    .as_slice()
                    .expect("standard layout"),
                |value| {
                    let [a, b, c, d] = value.re.to_le_bytes();
                    let [e, f, g, h] = value.im.to_le_bytes();
                    [a, b, c, d, e, f, g, h]
                },
            )
        }
        ("WEIGHT" | "WEIGHT_SPECTRUM", ArrayValue::Float32(array)) => {
            let array = array.reversed_axes();
            write_values(
                writer,
                array
                    .as_standard_layout()
                    .as_slice()
                    .expect("standard layout"),
                |value| value.to_le_bytes(),
            )
        }
        ("FLAG", ArrayValue::Bool(array)) => {
            let array = array.reversed_axes();
            write_values(
                writer,
                array
                    .as_standard_layout()
                    .as_slice()
                    .expect("standard layout"),
                |value| [u8::from(value)],
            )
        }
        _ => Err(invalid("unexpected staged array type")),
    }
}

fn write_values<T: Copy, const N: usize>(
    writer: &mut impl Write,
    values: &[T],
    encode: impl Fn(T) -> [u8; N],
) -> io::Result<()> {
    let mut encoded = [[0u8; N]; 1024];
    for chunk in values.chunks(encoded.len()) {
        for (value, slot) in chunk.iter().zip(&mut encoded) {
            *slot = encode(*value);
        }
        writer.write_all(encoded[..chunk.len()].as_flattened())?;
    }
    Ok(())
}

fn read_array(reader: &mut impl Read, name: &str) -> io::Result<ArrayValue> {
    let mut shape = [0; 2];
    let shape = if name == "WEIGHT" {
        &mut shape[..1]
    } else {
        &mut shape[..]
    };
    for dim in &mut *shape {
        let mut encoded = [0; 4];
        reader.read_exact(&mut encoded)?;
        *dim = u32::from_le_bytes(encoded) as usize;
    }
    match name {
        "CORRECTED_DATA" => read_values(reader, shape, |[a, b, c, d, e, f, g, h]| {
            Complex32::new(
                f32::from_le_bytes([a, b, c, d]),
                f32::from_le_bytes([e, f, g, h]),
            )
        })
        .map(ArrayValue::Complex32),
        "WEIGHT" | "WEIGHT_SPECTRUM" => {
            read_values(reader, shape, f32::from_le_bytes).map(ArrayValue::Float32)
        }
        "FLAG" => read_values(reader, shape, |[value]| value != 0).map(ArrayValue::Bool),
        _ => Err(invalid("unexpected staged column")),
    }
}

fn read_values<T, const N: usize>(
    reader: &mut impl Read,
    shape: &[usize],
    decode: impl Fn([u8; N]) -> T,
) -> io::Result<ArrayD<T>> {
    let mut count = shape
        .iter()
        .try_fold(1usize, |count, dim| count.checked_mul(*dim))
        .ok_or_else(|| invalid("staged array shape overflows"))?;
    let mut cells = Vec::new();
    cells.try_reserve_exact(count).map_err(io::Error::other)?;
    let mut encoded = [[0u8; N]; 1024];
    while count != 0 {
        let chunk = count.min(encoded.len());
        reader.read_exact(encoded[..chunk].as_flattened_mut())?;
        cells.extend(encoded[..chunk].iter().copied().map(&decode));
        count -= chunk;
    }
    ArrayD::from_shape_vec(IxDyn(shape).f(), cells).map_err(io::Error::other)
}
