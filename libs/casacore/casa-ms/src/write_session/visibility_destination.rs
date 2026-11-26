// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical writes for direct and CompressComplex visibility columns.

use std::ops::Range;

use casa_core::{ArrayValue, Complex32, RecordField, ScalarValue, Value};
use casa_tables::{ComplexCompression, ComplexScale, Table};

use super::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan,
};
use crate::{MsError, MsResult};

/// Physical write destination for a logical visibility column, including the
/// packed data and scale columns when casacore uses a CompressComplex engine.
pub struct VisibilityWriteDestination<'a> {
    name: &'a str,
    compression: Option<ComplexCompression>,
}

impl<'a> VisibilityWriteDestination<'a> {
    /// Resolve the on-disk codec and check that the engine supports writes.
    pub fn new(table: &Table, name: &'a str) -> MsResult<Self> {
        let compression = table.complex_compression(name)?;
        if let Some(compression) = &compression {
            compression.check_writable()?;
        }
        Ok(Self { name, compression })
    }

    /// Prepare to append visibility cells using the destination column’s existing storage manager.
    pub fn for_append(table: &Table, name: &'a str) -> MsResult<Self> {
        let destination = Self::new(table, name)?;
        if table.is_virtual_column(name) && destination.compression.is_none() {
            return Err(invalid(format!(
                "cannot append through virtual column {name:?}"
            )));
        }
        Ok(destination)
    }

    /// Append a visibility cell through the prepared destination and report a shape or storage error.
    pub fn append_cell(&self, value: ArrayValue, fields: &mut Vec<RecordField>) -> MsResult<()> {
        let Some(compression) = &self.compression else {
            fields.push(RecordField::new(self.name, Value::Array(value)));
            return Ok(());
        };
        let ArrayValue::Complex32(value) = value else {
            return Err(invalid("compressed visibility append expects Complex32"));
        };
        let (packed, (new_scale, new_offset)) = compression.encode_new_visibility_cell(&value)?;
        fields.push(RecordField::new(
            &compression.stored,
            Value::Array(ArrayValue::Int32(packed)),
        ));
        if let ComplexScale::PerRow { scale, offset, .. } = &compression.scale {
            fields.extend(
                [(scale, new_scale), (offset, new_offset)].map(|(name, value)| {
                    RecordField::new(name, Value::Scalar(ScalarValue::Float32(value)))
                }),
            );
        }
        Ok(())
    }

    /// Name of the logical visibility column.
    pub fn name(&self) -> &'a str {
        self.name
    }

    /// Columns needed by a selected-row mutation of this destination.
    pub fn plans(&self, elements: usize, create: bool) -> Vec<MeasurementSetWriteColumnPlan> {
        let mut columns = match &self.compression {
            None => vec![column_plan(self.name, elements, create)],
            Some(compression) => vec![column_plan(&compression.stored, elements, false)],
        };
        if let Some((scale, offset)) = self.automatic_scale_columns() {
            columns.extend([column_plan(scale, 1, false), column_plan(offset, 1, false)]);
        }
        columns
    }

    /// Encode one batch of complete visibility cells for physical writes.
    ///
    /// An optional channel range per row limits the encoded changes. The
    /// caller supplies each final full cell, with values outside a range
    /// already preserved.
    pub fn batch(
        &self,
        table: &Table,
        rows: &[usize],
        values: Vec<ArrayValue>,
        channels: Option<&[Option<Range<usize>>]>,
    ) -> MsResult<Vec<MeasurementSetMutationColumnBatch>> {
        if rows.len() != values.len() || channels.is_some_and(|ranges| ranges.len() != rows.len()) {
            return Err(invalid(
                "visibility batch rows, values and ranges do not agree",
            ));
        }
        let Some(compression) = &self.compression else {
            return Ok(vec![column_batch(
                self.name,
                MeasurementSetMutationColumnValues::Arrays(values),
            )]);
        };
        let packed = if channels.is_some_and(|ranges| ranges.iter().any(Option::is_some)) {
            table
                .column_accessor(&compression.stored)?
                .array_cells_owned_uncached(rows)?
        } else {
            vec![None; rows.len()]
        };
        let (scales, offsets) = match &compression.scale {
            ComplexScale::Fixed { scale, offset } => {
                (vec![*scale; rows.len()], vec![*offset; rows.len()])
            }
            ComplexScale::PerRow { scale, offset, .. } => (
                scalar_column(table, scale, rows)?,
                scalar_column(table, offset, rows)?,
            ),
        };
        let mut encoded = Vec::with_capacity(rows.len());
        let mut new_scale = Vec::with_capacity(rows.len());
        let mut new_offset = Vec::with_capacity(rows.len());
        for (index, ((value, original), previous)) in values
            .into_iter()
            .zip(packed)
            .zip(scales.into_iter().zip(offsets))
            .enumerate()
        {
            let ArrayValue::Complex32(value) = value else {
                return Err(invalid("compressed visibility write expects Complex32"));
            };
            let original = match original {
                Some(ArrayValue::Int32(original)) => Some(original),
                Some(_) => return Err(invalid("compressed visibility backing is not Int32")),
                None => None,
            };
            let range = channels.and_then(|ranges| ranges[index].clone());
            let (packed, (scale, offset)) =
                compression.encode_visibility_cell(&value, original, previous, range)?;
            encoded.push(ArrayValue::Int32(packed));
            new_scale.push(ScalarValue::Float32(scale));
            new_offset.push(ScalarValue::Float32(offset));
        }
        let mut columns = vec![column_batch(
            &compression.stored,
            MeasurementSetMutationColumnValues::Arrays(encoded),
        )];
        if let Some((scale, offset)) = self.automatic_scale_columns() {
            columns.extend([
                column_batch(
                    scale,
                    MeasurementSetMutationColumnValues::Scalars(new_scale),
                ),
                column_batch(
                    offset,
                    MeasurementSetMutationColumnValues::Scalars(new_offset),
                ),
            ]);
        }
        Ok(columns)
    }

    fn automatic_scale_columns(&self) -> Option<(&str, &str)> {
        match &self.compression.as_ref()?.scale {
            ComplexScale::PerRow {
                scale,
                offset,
                automatic: true,
            } => Some((scale, offset)),
            _ => None,
        }
    }
}

fn column_plan(name: &str, elements: usize, create: bool) -> MeasurementSetWriteColumnPlan {
    let (mode, storage_manager) = if create {
        (
            MeasurementSetColumnWriteMode::Create,
            MeasurementSetColumnStorage::TiledShape,
        )
    } else {
        (
            MeasurementSetColumnWriteMode::Replace,
            MeasurementSetColumnStorage::Persisted,
        )
    };
    MeasurementSetWriteColumnPlan {
        name: name.to_owned(),
        bytes_per_row: elements.saturating_mul(std::mem::size_of::<Complex32>()),
        mode,
        storage_manager,
        tile_shape: None,
        create_source_column: None,
    }
}

fn scalar_column(table: &Table, name: &str, rows: &[usize]) -> MsResult<Vec<f32>> {
    table
        .column_accessor(name)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .map(|value| match value {
            Some(ScalarValue::Float32(value)) => Ok(value),
            _ => Err(invalid(format!(
                "visibility scale column {name:?} is not Float32"
            ))),
        })
        .collect()
}

fn column_batch(
    name: &str,
    values: MeasurementSetMutationColumnValues,
) -> MeasurementSetMutationColumnBatch {
    MeasurementSetMutationColumnBatch {
        name: name.to_owned(),
        values,
    }
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
