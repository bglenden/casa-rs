// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl MeasurementSetWritePlan {
    pub(crate) const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Return the ordered MAIN row numbers admitted by this mutation plan.
    pub fn selected_rows(&self) -> &[usize] {
        &self.selected_rows
    }

    /// Maximum rows accepted in one producer batch.
    pub const fn batch_rows(&self) -> usize {
        self.batch_rows
    }

    /// Maximum modeled writer-owned resident bytes.
    pub const fn maximum_resident_bytes(&self) -> usize {
        self.maximum_resident_bytes
    }

    /// Exact planned MAIN columns and their storage strategies.
    pub fn columns(&self) -> &[MeasurementSetWriteColumnPlan] {
        &self.columns
    }

    /// Plan the standard simulation/import visibility column family.
    pub fn visibility_creation(
        row_count: usize,
        correlation_count: usize,
        channel_count: usize,
        telescope_name: &str,
        resources: MeasurementSetWriteResources,
    ) -> Result<Self, MeasurementSetWriteError> {
        if correlation_count == 0 || channel_count == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "correlation_count and channel_count must be positive".to_string(),
            ));
        }
        if resources.maximum_live_batches == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "maximum_live_batches must be positive".to_string(),
            ));
        }
        let sample_count = correlation_count
            .checked_mul(channel_count)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let visibility_tile_shape =
            crate::ms::casa_visibility_tile_shape(correlation_count, channel_count, telescope_name);
        let weight_tile_shape = crate::ms::casa_weight_tile_shape(&visibility_tile_shape);
        let uvw_tile_shape = crate::ms::casa_uvw_tile_shape(&visibility_tile_shape);
        let scalar_columns = standard_main_scalar_column_plans();
        let scalar_bytes_per_row = scalar_columns.iter().try_fold(0usize, |total, column| {
            total
                .checked_add(column.bytes_per_row())
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?;
        // The scalar writers' buffers while rows stream, and the save's
        // finalization scratch after them.
        let scalar_buffer_bytes = streamed_scalar_columns_resident_bytes(scalar_columns.len())
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        // DATA complex32 + FLAG + one possible writer-owned FLAG copy,
        // UVW f64[3], WEIGHT f32[ncorr], SIGMA f32[ncorr], and fixed-width scalars.
        let bytes_per_row = sample_count
            .checked_mul(8 + 1 + 1)
            .and_then(|bytes| bytes.checked_add(3 * 8))
            .and_then(|bytes| bytes.checked_add(correlation_count.checked_mul(4 + 4)?))
            .and_then(|bytes| bytes.checked_add(scalar_bytes_per_row))
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let tile_rows = visibility_tile_shape.get(2).copied().unwrap_or(1).max(1);
        let fixed_bytes = |data_bands: usize| {
            visibility_writer_resident_bytes(
                (correlation_count, channel_count),
                [&visibility_tile_shape, &uvw_tile_shape, &weight_tile_shape],
                data_bands,
                resources.tiled_column_buffer_bytes,
            )?
            .checked_add(scalar_buffer_bytes)
            .ok_or(MeasurementSetWriteError::ByteOverflow)
        };
        // DATA's writer holds a second band of tiles, which it fills while
        // the first is written, when the budget still leaves each batch a
        // whole band of rows beside it.
        let band_fits = |fixed: usize| {
            resources
                .available_bytes
                .checked_sub(fixed)
                .is_some_and(|left| {
                    left / resources.maximum_live_batches
                        >= bytes_per_row.saturating_mul(tile_rows.min(row_count.max(1)))
                })
        };
        let data_bands = if band_fits(fixed_bytes(2)?) { 2 } else { 1 };
        let fixed_writer_bytes = fixed_bytes(data_bands)?;
        if fixed_writer_bytes > resources.available_bytes {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "fixed scalar and tiled column buffers require {fixed_writer_bytes} bytes; writer budget is {} bytes",
                resources.available_bytes
            )));
        }
        let streaming_budget = resources.available_bytes - fixed_writer_bytes;
        let per_batch_budget = streaming_budget / resources.maximum_live_batches;
        if row_count > 0 && per_batch_budget < bytes_per_row {
            return Err(MeasurementSetWriteError::InsufficientBudget {
                bytes_per_row,
                per_batch_budget,
            });
        }
        // Batches hold whole bands of tiles (one tile along the row axis,
        // every tile across the cell), so each batch completes its tiles; as
        // many bands as fit the target, and at least one when the budget
        // allows. The budget is a ceiling, not a target: a few tens of MB
        // keep the producer and the writer overlapping with little resident
        // memory.
        let budget_rows = per_batch_budget / bytes_per_row;
        let mut batch_rows = if budget_rows >= tile_rows {
            (TARGET_BATCH_BYTES / bytes_per_row / tile_rows).clamp(1, budget_rows / tile_rows)
                * tile_rows
        } else {
            budget_rows
        };
        batch_rows = batch_rows.min(row_count.max(1));
        let batch_bytes = batch_rows
            .checked_mul(bytes_per_row)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let queue_budget = streaming_budget.saturating_sub(batch_bytes);
        let queue_capacity = queue_budget
            .checked_div(batch_bytes)
            .unwrap_or(0)
            .min(resources.maximum_live_batches.saturating_sub(1));
        let maximum_resident_bytes = batch_bytes
            .checked_mul(queue_capacity.saturating_add(1))
            .and_then(|bytes| bytes.checked_add(fixed_writer_bytes))
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let mut columns = vec![
            MeasurementSetWriteColumnPlan {
                name: "DATA".to_string(),
                bytes_per_row: sample_count * 8,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: Some(visibility_tile_shape.clone()),
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "FLAG".to_string(),
                bytes_per_row: sample_count,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: Some(visibility_tile_shape.clone()),
                create_source_column: None,
            },
            // Every FLAG_CATEGORY cell is left undefined, as CASA's simulator
            // leaves it, so the column holds no payload.
            MeasurementSetWriteColumnPlan {
                name: "FLAG_CATEGORY".to_string(),
                bytes_per_row: 0,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: None,
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "UVW".to_string(),
                bytes_per_row: 3 * 8,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledColumn,
                tile_shape: Some(uvw_tile_shape.clone()),
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "WEIGHT".to_string(),
                bytes_per_row: correlation_count * 4,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: Some(weight_tile_shape.clone()),
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "SIGMA".to_string(),
                bytes_per_row: correlation_count * 4,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: Some(weight_tile_shape.clone()),
                create_source_column: None,
            },
        ];
        columns.extend(
            scalar_columns
                .iter()
                .map(|column| MeasurementSetWriteColumnPlan {
                    name: column.name.clone(),
                    bytes_per_row: column.bytes_per_row(),
                    mode: MeasurementSetColumnWriteMode::Create,
                    storage_manager: MeasurementSetColumnStorage::Standard,
                    tile_shape: None,
                    create_source_column: None,
                }),
        );
        Ok(Self {
            operation: MeasurementSetWriteOperation::Create,
            row_count,
            correlation_count,
            channel_count,
            bytes_per_row,
            batch_rows,
            batch_bytes,
            queue_capacity,
            maximum_resident_bytes,
            visibility_tile_shape: visibility_tile_shape.clone(),
            weight_tile_shape: weight_tile_shape.clone(),
            uvw_tile_shape: uvw_tile_shape.clone(),
            tiled_column_buffer_bytes: resources.tiled_column_buffer_bytes,
            data_bands,
            selected_rows: Vec::new(),
            columns,
            array_columns: Vec::new(),
            array_column_memory_budget_bytes: 0,
            scalar_columns,
        })
    }

    /// Plan a bounded mutation of explicit existing MAIN rows.
    pub fn selected_row_mutation(
        selected_rows: Vec<usize>,
        columns: Vec<MeasurementSetWriteColumnPlan>,
        resources: MeasurementSetWriteResources,
    ) -> Result<Self, MeasurementSetWriteError> {
        if resources.maximum_live_batches == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "maximum_live_batches must be positive".to_string(),
            ));
        }
        if columns.is_empty() && !selected_rows.is_empty() {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation requires at least one column".to_string(),
            ));
        }
        if let Some(column) = columns.iter().find(|column| {
            column.create_source_column.is_some()
                && column.mode != MeasurementSetColumnWriteMode::Create
        }) {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "column {} can clone a source only in create mode",
                column.name
            )));
        }
        if let Some(column) = columns.iter().find(|column| {
            (column.mode == MeasurementSetColumnWriteMode::Replace
                && column.storage_manager != MeasurementSetColumnStorage::Persisted)
                || (column.mode == MeasurementSetColumnWriteMode::Create
                    && column.storage_manager != MeasurementSetColumnStorage::TiledShape)
        }) {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "column {} has storage strategy {:?} for {:?} mode",
                column.name, column.storage_manager, column.mode
            )));
        }
        let mut unique_rows = selected_rows.clone();
        unique_rows.sort_unstable();
        unique_rows.dedup();
        if unique_rows.len() != selected_rows.len() {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation contains duplicate rows".to_string(),
            ));
        }
        let bytes_per_row = columns.iter().try_fold(0usize, |total, column| {
            total
                .checked_add(column.bytes_per_row)
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?;
        if !selected_rows.is_empty() && bytes_per_row == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation bytes_per_row must be positive".to_string(),
            ));
        }
        let per_batch_budget = resources.available_bytes / resources.maximum_live_batches;
        let mut batch_rows = per_batch_budget.checked_div(bytes_per_row).unwrap_or(0);
        if !selected_rows.is_empty() && batch_rows == 0 {
            return Err(MeasurementSetWriteError::InsufficientBudget {
                bytes_per_row,
                per_batch_budget,
            });
        }
        let tile_rows = columns
            .iter()
            .filter_map(|column| column.tile_shape.as_ref()?.last().copied())
            .filter(|rows| *rows > 0)
            .min()
            .unwrap_or(1);
        if batch_rows >= tile_rows {
            batch_rows -= batch_rows % tile_rows;
        }
        batch_rows = batch_rows.min(selected_rows.len().max(1));
        let batch_bytes = batch_rows
            .checked_mul(bytes_per_row)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let queue_capacity = resources
            .available_bytes
            .saturating_sub(batch_bytes)
            .checked_div(batch_bytes)
            .unwrap_or(0)
            .min(resources.maximum_live_batches.saturating_sub(1));
        let maximum_resident_bytes = batch_bytes
            .checked_mul(queue_capacity.saturating_add(1))
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        Ok(Self {
            operation: MeasurementSetWriteOperation::SelectedRowMutation,
            row_count: selected_rows.len(),
            correlation_count: 0,
            channel_count: 0,
            bytes_per_row,
            batch_rows,
            batch_bytes,
            queue_capacity,
            maximum_resident_bytes,
            visibility_tile_shape: Vec::new(),
            weight_tile_shape: Vec::new(),
            uvw_tile_shape: Vec::new(),
            tiled_column_buffer_bytes: 0,
            data_bands: 1,
            selected_rows,
            columns,
            array_columns: Vec::new(),
            array_column_memory_budget_bytes: 0,
            scalar_columns: Vec::new(),
        })
    }

    /// Plan canonical variable-shape MAIN columns for a new MeasurementSet.
    #[doc(hidden)]
    pub fn variable_array_creation(
        row_count: usize,
        array_columns: Vec<MeasurementSetArrayColumnPlan>,
        scalar_columns: Vec<MeasurementSetScalarColumnPlan>,
        resources: MeasurementSetWriteResources,
    ) -> Result<Self, MeasurementSetWriteError> {
        if resources.maximum_live_batches == 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "maximum_live_batches must be positive".to_string(),
            ));
        }
        if array_columns.is_empty() && scalar_columns.is_empty() && row_count > 0 {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "creation write requires at least one column".to_string(),
            ));
        }
        let mut names = HashSet::with_capacity(array_columns.len() + scalar_columns.len());
        for column in &array_columns {
            if !names.insert(column.name.as_str()) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "duplicate variable-array column {}",
                    column.name
                )));
            }
            if !matches!(
                column.storage_manager,
                MeasurementSetColumnStorage::TiledColumn | MeasurementSetColumnStorage::TiledShape
            ) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "array creation column {} requires a tiled creation manager, found {:?}",
                    column.name, column.storage_manager
                )));
            }
            let planned_rows = column.shapes.iter().try_fold(0usize, |total, shape| {
                total
                    .checked_add(shape.row_count)
                    .ok_or(MeasurementSetWriteError::ByteOverflow)
            })?;
            if planned_rows != row_count {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "column {} shape histogram has {planned_rows} rows; plan requires {row_count}",
                    column.name
                )));
            }
            if column.storage_manager == MeasurementSetColumnStorage::TiledColumn
                && (column.shapes.len() != 1
                    || column.shapes[0].row_count != row_count
                    || row_count == 0)
            {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "TiledColumnStMan column {} requires one defined shape for every row",
                    column.name
                )));
            }
        }
        for column in &scalar_columns {
            if !names.insert(column.name.as_str()) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "duplicate creation column {}",
                    column.name
                )));
            }
        }
        let array_bytes_per_row = array_columns.iter().try_fold(0usize, |total, column| {
            let element_bytes = match column.value_type {
                StreamedTiledShapeValueType::Bool => 1,
                StreamedTiledShapeValueType::Float32 | StreamedTiledShapeValueType::Int32 => 4,
                StreamedTiledShapeValueType::Float64 => 8,
                StreamedTiledShapeValueType::Complex32 => 8,
            };
            let column_bytes = column
                .shapes
                .iter()
                .try_fold(0usize, |maximum, shape| {
                    let elements = shape.cell_shape.iter().try_fold(1usize, |total, extent| {
                        total
                            .checked_mul(*extent)
                            .ok_or(MeasurementSetWriteError::ByteOverflow)
                    })?;
                    Ok::<_, MeasurementSetWriteError>(maximum.max(elements))
                })?
                .checked_mul(element_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            total
                .checked_add(column_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?;
        let scalar_bytes_per_row = scalar_columns.iter().try_fold(0usize, |total, column| {
            total
                .checked_add(column.bytes_per_row())
                .ok_or(MeasurementSetWriteError::ByteOverflow)
        })?;
        let bytes_per_row = array_bytes_per_row
            .checked_add(scalar_bytes_per_row)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        // The scalar writers' buffers while rows stream, and the save's
        // finalization scratch after them.
        let scalar_buffer_bytes = streamed_scalar_columns_resident_bytes(scalar_columns.len())
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        if scalar_buffer_bytes > resources.available_bytes {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "scalar column buffers require {scalar_buffer_bytes} bytes; writer budget is {} bytes",
                resources.available_bytes
            )));
        }
        let streaming_budget = resources.available_bytes - scalar_buffer_bytes;
        let batch_budget = streaming_budget / resources.maximum_live_batches;
        let mut batch_rows = batch_budget.checked_div(bytes_per_row).unwrap_or(0);
        if row_count > 0 && batch_rows == 0 {
            return Err(MeasurementSetWriteError::InsufficientBudget {
                bytes_per_row,
                per_batch_budget: batch_budget,
            });
        }
        batch_rows = batch_rows.min(row_count.max(1));
        let batch_bytes = batch_rows
            .checked_mul(bytes_per_row)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let writer_budget = streaming_budget.saturating_sub(batch_bytes);
        let array_column_memory_budget_bytes = if array_columns.is_empty() {
            0
        } else {
            writer_budget / array_columns.len()
        };
        if row_count > 0 && !array_columns.is_empty() && array_column_memory_budget_bytes == 0 {
            return Err(MeasurementSetWriteError::InsufficientBudget {
                bytes_per_row: 1,
                per_batch_budget: 0,
            });
        }
        let maximum_resident_bytes = array_column_memory_budget_bytes
            .checked_mul(array_columns.len())
            .and_then(|bytes| bytes.checked_add(batch_bytes))
            .and_then(|bytes| bytes.checked_add(scalar_buffer_bytes))
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        let mut columns = array_columns
            .iter()
            .map(|column| MeasurementSetWriteColumnPlan {
                name: column.name.clone(),
                bytes_per_row: column
                    .shapes
                    .iter()
                    .map(|shape| shape.cell_shape.iter().product::<usize>())
                    .max()
                    .unwrap_or(0)
                    * match column.value_type {
                        StreamedTiledShapeValueType::Bool => 1,
                        StreamedTiledShapeValueType::Float32
                        | StreamedTiledShapeValueType::Int32 => 4,
                        StreamedTiledShapeValueType::Float64
                        | StreamedTiledShapeValueType::Complex32 => 8,
                    },
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: column.storage_manager,
                tile_shape: column.shapes.first().map(|shape| shape.tile_shape.clone()),
                create_source_column: None,
            })
            .collect::<Vec<_>>();
        columns.extend(
            scalar_columns
                .iter()
                .map(|column| MeasurementSetWriteColumnPlan {
                    name: column.name.clone(),
                    bytes_per_row: column.bytes_per_row(),
                    mode: MeasurementSetColumnWriteMode::Create,
                    storage_manager: MeasurementSetColumnStorage::Standard,
                    tile_shape: None,
                    create_source_column: None,
                }),
        );
        Ok(Self {
            operation: MeasurementSetWriteOperation::Create,
            row_count,
            correlation_count: 0,
            channel_count: 0,
            bytes_per_row,
            batch_rows,
            batch_bytes,
            queue_capacity: 0,
            maximum_resident_bytes,
            visibility_tile_shape: Vec::new(),
            weight_tile_shape: Vec::new(),
            uvw_tile_shape: Vec::new(),
            tiled_column_buffer_bytes: 0,
            data_bands: 1,
            selected_rows: Vec::new(),
            columns,
            array_columns,
            array_column_memory_budget_bytes,
            scalar_columns,
        })
    }
    /// Set default tile extents for the planned tiled columns, by column name.
    pub fn with_array_default_tiles(
        mut self,
        shape_for_column: impl Fn(&str) -> Vec<usize>,
    ) -> Self {
        for column in &mut self.columns {
            if column.tile_shape.is_some() {
                column.tile_shape = Some(shape_for_column(&column.name));
            }
        }
        self
    }
}
