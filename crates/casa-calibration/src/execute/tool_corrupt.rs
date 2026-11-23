// SPDX-License-Identifier: LGPL-3.0-or-later
//! Forward application of a registered equation to physical MODEL_DATA.

use super::*;
use std::time::Duration;

pub(crate) struct ModelCorruption {
    pub(crate) legacy: bool,
    pub(crate) using_callibrary: bool,
}

pub(crate) fn execute_tool_corrupt(
    ms: &mut MeasurementSet,
    selected: &ResolvedMsSelection,
    lock: &mut TableWriteLock,
    request: &ApplyPlanRequest,
    correction: ToolCorrection,
    tracking: ToolActivityInput<'_>,
    operation: ModelCorruption,
) -> Result<(bool, ToolApplyActivity), ApplyExecutionError> {
    ms.data_column(VisibilityDataColumn::ModelData)
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: display_ms_path(ms),
            source,
        })?;
    let registration = RegisteredApplySelection {
        selected,
        has_library_registration: correction.has_library_registration,
        legacy_corruption: operation.legacy,
        legacy_libraries: if operation.legacy {
            tracking.libraries
        } else {
            &[]
        },
    };
    let (mut plan, _, legacy_scans) = plan_with_selection(ms, request, None, Some(registration))?;
    let activity = ToolApplyActivity::from_plan(
        &plan,
        tracking.names,
        tracking.libraries,
        correction.parang_terms,
        tracking.opacity,
        tracking.previous,
    );
    let mut chain = ApplyChain::load(ms, &plan, false)?;
    chain.arrange_for_tool(&plan, correction.parang_terms, tracking.additive);
    chain.legacy_corruption = operation.legacy;
    chain.legacy_scans = legacy_scans;
    chain.steps.reverse();
    let (all_calibrated, missing_library) = select_corruption_rows(
        ms,
        &mut plan,
        tracking.libraries,
        &chain.legacy_scans,
        operation,
    )?;
    if !plan.selected_rows.is_empty() {
        if correction.tableless_opacity {
            return Err(unresolved_opacity(ms));
        }
        write_corrupted_model(ms, &mut plan, &chain, lock)?;
    }
    if let Some(name) = missing_library {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: name,
            reason: "no library calibration is arranged for this legacy visibility buffer".into(),
        });
    }
    Ok((all_calibrated, activity))
}

fn select_corruption_rows(
    ms: &MeasurementSet,
    plan: &mut ApplyPlan,
    libraries: &[LibraryApplyRegistration],
    legacy_scans: &crate::plan::LegacyScanOverrides,
    operation: ModelCorruption,
) -> Result<(bool, Option<String>), ApplyExecutionError> {
    if operation.legacy && !libraries.is_empty() {
        let ordered =
            availability::rows_in_buffer_order(ms, std::mem::take(&mut plan.selected_rows))?;
        let mut all_calibrated = true;
        for row in ordered {
            let traditional_available = plan
                .calibration_tables
                .iter()
                .filter(|table| table.spec.callibrary.is_none())
                .all(|table| availability::table_available(&row, table));
            if !traditional_available {
                all_calibrated = false;
                continue;
            }
            if let Some(library) = libraries.iter().find(|library| {
                !library.tables.clone().any(|index| {
                    let slice = &plan.calibration_tables[index];
                    let scan = legacy_scans
                        .get(&(index, row.row_index))
                        .copied()
                        .unwrap_or(row.scan_number);
                    slice.applies_to_scan(&row, scan) && availability::table_available(&row, slice)
                })
            }) {
                return Ok((false, Some(library.name.clone())));
            }
            plan.selected_rows.push(row);
        }
        return Ok((all_calibrated, None));
    }
    if operation.using_callibrary && !operation.legacy {
        return Ok((true, None));
    }
    let mut rows = std::mem::take(&mut plan.selected_rows);
    let count = rows.len();
    rows.retain(|row| availability::available(row, plan));
    let all_calibrated = count == rows.len();
    plan.selected_rows = rows;
    Ok((all_calibrated, None))
}

fn write_corrupted_model(
    ms: &mut MeasurementSet,
    plan: &mut ApplyPlan,
    chain: &ApplyChain,
    lock: &mut TableWriteLock,
) -> Result<(), ApplyExecutionError> {
    let path = display_ms_path(ms);
    let model_column = VisibilityDataColumn::ModelData.name();
    let ms_error = |source| ApplyExecutionError::MutateMeasurementSet {
        path: path.clone(),
        source,
    };
    let write_error = |source: casa_ms::MeasurementSetWriteError| {
        ms_error(MsError::InvalidInput(source.to_string()))
    };
    let correlations = load_correlation_types_by_ddid(ms).map_err(ms_error)?;
    let cells = maximum_visibility_cells(plan, &correlations)?;
    let rows = availability::buffer_ordered_rows(ms, std::mem::take(&mut plan.selected_rows))?;
    let index = rows.iter().map(|(_, row)| row.row_index).collect();
    let write_plan = MeasurementSetWritePlan::selected_row_mutation(
        index,
        vec![MeasurementSetWriteColumnPlan {
            name: model_column.into(),
            bytes_per_row: cells.saturating_mul(std::mem::size_of::<Complex32>()),
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        }],
        MeasurementSetWriteResources::from_system_memory(2).map_err(write_error)?,
    )
    .map_err(write_error)?;
    let batch_rows = write_plan.batch_rows();
    let mut geometry = RowGeometryCache::default();
    let started = calibration_profile_enabled().then(Instant::now);
    let mut read_time = Duration::ZERO;
    let mut compute_time = Duration::ZERO;
    let mut write_time = Duration::ZERO;
    MeasurementSetWriteSession::mutate_selected_rows(
        ms,
        write_plan,
        Some(lock),
        write_error,
        |writer, ms| {
            let mut read_models = |ms: &MeasurementSet, indices: &[usize]| {
                let read_started = started.map(|_| Instant::now());
                let models = ms
                    .main_table()
                    .column_accessor(model_column)
                    .and_then(|column| column.array_cells_owned(indices))
                    .map_err(|error| ms_error(MsError::from(error)));
                if let Some(read_started) = read_started {
                    read_time += read_started.elapsed();
                }
                models
            };
            let mut publish = |writer: &mut MeasurementSetWriteSession,
                               ms: &mut MeasurementSet,
                               indices: &[usize],
                               values: Vec<ArrayValue>| {
                let write_started = started.map(|_| Instant::now());
                let mut values = values.into_iter();
                for indices in indices.chunks(batch_rows) {
                    writer
                        .write_mutation_batch(
                            ms,
                            MeasurementSetMutationBatch {
                                row_indices: indices.to_vec(),
                                columns: vec![MeasurementSetMutationColumnBatch {
                                    name: model_column.into(),
                                    values: MeasurementSetMutationColumnValues::Arrays(
                                        values.by_ref().take(indices.len()).collect(),
                                    ),
                                }],
                            },
                        )
                        .map_err(write_error)?;
                }
                if let Some(write_started) = write_started {
                    write_time += write_started.elapsed();
                }
                Ok::<_, ApplyExecutionError>(())
            };
            let mut buffers = rows.chunk_by(|first, next| first.0 == next.0).peekable();
            let mut remaining = rows.as_slice();
            while let Some(buffer) = buffers.next() {
                let mut row_count = buffer.len();
                while buffers
                    .peek()
                    .is_some_and(|buffer| row_count + buffer.len() <= batch_rows)
                {
                    row_count += buffers.next().expect("peeked buffer").len();
                }
                let batch;
                (batch, remaining) = remaining.split_at(row_count);
                let indices = batch
                    .iter()
                    .map(|(_, row)| row.row_index)
                    .collect::<Vec<_>>();
                // Read successive buffers together. On an I/O error, repeat the
                // reads buffer by buffer so the completed prefix is preserved.
                let mut prefetched = read_models(ms, &indices).ok().map(Vec::into_iter);
                let mut values = Vec::with_capacity(row_count);
                let mut offset = 0;
                for buffer in batch.chunk_by(|first, next| first.0 == next.0) {
                    let models = match &mut prefetched {
                        Some(models) => Ok(models.by_ref().take(buffer.len()).collect()),
                        None => read_models(ms, &indices[offset..offset + buffer.len()]),
                    };
                    let computed = models.and_then(|models| {
                        let compute_started = started.map(|_| Instant::now());
                        let result = buffer
                            .iter()
                            .zip(models)
                            .map(|((_, row), model)| {
                                let model = required_prefetched_array(
                                    model,
                                    row.row_index,
                                    model_column,
                                    &path,
                                )?;
                                corrupt_row_model(
                                    row,
                                    &model,
                                    &correlations,
                                    plan,
                                    chain,
                                    &mut geometry,
                                )
                            })
                            .collect::<Result<Vec<_>, _>>();
                        if let Some(compute_started) = compute_started {
                            compute_time += compute_started.elapsed();
                        }
                        result
                    });
                    match computed {
                        Ok(buffer_values) => values.extend(buffer_values),
                        Err(error) => {
                            publish(writer, ms, &indices[..offset], values)?;
                            return Err(error);
                        }
                    }
                    offset += buffer.len();
                }
                publish(writer, ms, &indices, values)?;
            }
            Ok(())
        },
    )?;
    if let Some(started) = started {
        log_calibration_profile(
            "corrupt_model_rows",
            started.elapsed().as_secs_f64(),
            Some(format!(
                "rows={} read={:.3}s compute={:.3}s write={:.3}s",
                rows.len(),
                read_time.as_secs_f64(),
                compute_time.as_secs_f64(),
                write_time.as_secs_f64()
            )),
        );
    }
    Ok(())
}

fn corrupt_row_model(
    row: &ApplyRowPlan,
    model: &ArrayValue,
    correlations: &HashMap<i32, Vec<i32>>,
    plan: &ApplyPlan,
    chain: &ApplyChain,
    geometry: &mut RowGeometryCache,
) -> Result<ArrayValue, ApplyExecutionError> {
    let layout = correlations.get(&row.data_desc_id).ok_or_else(|| {
        ApplyExecutionError::UnsupportedCorrelationLayout {
            data_desc_id: row.data_desc_id,
            correlation_types: Vec::new(),
        }
    })?;
    let result = apply_row(
        row,
        ExecutionRowInputs {
            correlation_types: layout,
            data: model,
            original_flags: None,
            original_weight: None,
            original_weight_spectrum: None,
            has_weight_spectrum: false,
            preserved_corrected_data: None,
            preserved_weight_spectrum: None,
            channel_grid: None,
            forward_model: true,
            skip_unavailable_terms: false,
            trial: false,
            parang_calwt: false,
        },
        plan,
        chain,
        Some(geometry),
        None,
        None,
    )?;
    Ok(result.corrected_data)
}
