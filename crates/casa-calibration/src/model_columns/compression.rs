// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical column writes for the compressed scratch visibility layout.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::write_session::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan,
};
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelectionRow};
use casa_tables::{ComplexCompression, ComplexScale, Table};
use casa_types::{ArrayValue, Complex32, ScalarValue};

use super::invalid;

pub(crate) struct ScratchDestination {
    pub(super) visibility: VisibilityDataColumn,
    compression: Option<ComplexCompression>,
}

impl ScratchDestination {
    pub(super) fn new(ms: &MeasurementSet, visibility: VisibilityDataColumn) -> MsResult<Self> {
        let compression = ms.main_table().complex_compression(visibility.name())?;
        if let Some(compression) = &compression {
            compression.check_writable()?;
        }
        Ok(Self {
            visibility,
            compression,
        })
    }

    pub(super) fn plans(
        &self,
        elements: usize,
        create: bool,
    ) -> Vec<MeasurementSetWriteColumnPlan> {
        let mut columns = match &self.compression {
            None => vec![column_plan(self.visibility.name(), elements, create)],
            Some(compression) => vec![column_plan(&compression.stored, elements, false)],
        };
        if let Some(ComplexScale::PerRow {
            scale,
            offset,
            automatic: true,
        }) = self
            .compression
            .as_ref()
            .map(|compression| &compression.scale)
        {
            columns.extend([column_plan(scale, 1, false), column_plan(offset, 1, false)]);
        }
        columns
    }

    pub(crate) fn batch(
        &self,
        ms: &MeasurementSet,
        row_indices: &[usize],
        rows: &[ResolvedMsSelectionRow],
        ranges: &BTreeMap<i32, Range<usize>>,
        values: Vec<ArrayValue>,
    ) -> MsResult<Vec<MeasurementSetMutationColumnBatch>> {
        let Some(compression) = &self.compression else {
            return Ok(vec![array_batch(self.visibility.name(), values)]);
        };
        let main = ms.main_table();
        let packed = if rows
            .iter()
            .any(|row| ranges.contains_key(&row.spectral_window_id))
        {
            main.column_accessor(&compression.stored)?
                .array_cells_owned_uncached(row_indices)?
        } else {
            vec![None; rows.len()]
        };
        let (scales, offsets) = match &compression.scale {
            ComplexScale::Fixed { scale, offset } => {
                (vec![*scale; rows.len()], vec![*offset; rows.len()])
            }
            ComplexScale::PerRow { scale, offset, .. } => (
                scalar_column(main, scale, row_indices)?,
                scalar_column(main, offset, row_indices)?,
            ),
        };
        let mut encoded = Vec::with_capacity(rows.len());
        let mut new_scale = Vec::with_capacity(rows.len());
        let mut new_offset = Vec::with_capacity(rows.len());
        for (((row, value), original), previous) in rows
            .iter()
            .zip(values)
            .zip(packed)
            .zip(scales.into_iter().zip(offsets))
        {
            let ArrayValue::Complex32(value) = value else {
                return Err(invalid(
                    "compressed scratch write expects complex visibilities",
                ));
            };
            let original = match original {
                Some(ArrayValue::Int32(original)) => Some(original),
                Some(_) => return Err(invalid("compressed scratch backing is not Int32")),
                None => None,
            };
            let range = ranges.get(&row.spectral_window_id).cloned();
            let (packed, (scale, offset)) =
                compression.encode_visibility_cell(&value, original, previous, range)?;
            encoded.push(ArrayValue::Int32(packed));
            new_scale.push(ScalarValue::Float32(scale));
            new_offset.push(ScalarValue::Float32(offset));
        }
        let mut columns = vec![array_batch(&compression.stored, encoded)];
        if let ComplexScale::PerRow {
            scale,
            offset,
            automatic: true,
        } = &compression.scale
        {
            columns.extend([
                scalar_batch(scale, new_scale),
                scalar_batch(offset, new_offset),
            ]);
        }
        Ok(columns)
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
                "scratch scale column {name:?} is not Float32"
            ))),
        })
        .collect()
}

fn array_batch(name: &str, values: Vec<ArrayValue>) -> MeasurementSetMutationColumnBatch {
    MeasurementSetMutationColumnBatch {
        name: name.to_owned(),
        values: MeasurementSetMutationColumnValues::Arrays(values),
    }
}

fn scalar_batch(name: &str, values: Vec<ScalarValue>) -> MeasurementSetMutationColumnBatch {
    MeasurementSetMutationColumnBatch {
        name: name.to_owned(),
        values: MeasurementSetMutationColumnValues::Scalars(values),
    }
}
