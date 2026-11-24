// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibrater::correct2 skips unavailable SPWs, or only flags them in STRICT.

use super::*;

pub(super) fn available(row: &ApplyRowPlan, plan: &ApplyPlan) -> bool {
    plan.calibration_tables
        .iter()
        .filter(|table| table.applies_to(row))
        .all(|table| table_available(row, table))
}

pub(super) fn table_available(row: &ApplyRowPlan, table: &ApplyCalibrationTablePlan) -> bool {
    table
        .unavailable_row_indices
        .binary_search(&row.row_index)
        .is_err()
        && table
            .spw_mapping
            .iter()
            .find(|mapping| mapping.data_spw_id == row.data_spw_id)
            .is_some_and(|mapping| {
                table
                    .calibration_spectral_windows
                    .iter()
                    .any(|spw| spw.spw_id == mapping.calibration_spw_id)
            })
}

pub(super) fn execute(
    ms: &mut MeasurementSet,
    plan: ApplyPlan,
    input: ApplyInput<'_>,
    tracer: Option<&mut CalibrationPerfTracer>,
    mut lock: Option<&mut TableWriteLock>,
    tool: Option<&mut ToolApplyExecution<'_>>,
) -> Result<(ApplyExecutionReport, ExecuteApplyPlanTraceSummary), ApplyExecutionError> {
    let start = Instant::now();
    let strict =
        tool.as_ref().is_some_and(|run| run.correction.strict) || plan.apply_mode.is_strict();
    let unavailable = plan
        .selected_rows
        .iter()
        .filter(|row| !available(row, &plan))
        .cloned()
        .collect::<Vec<_>>();
    if unavailable.is_empty() {
        return execute_apply_available(ms, plan, input, tracer, lock, tool, None);
    }
    let mut usable = plan.clone();
    usable.selected_rows.retain(|row| available(row, &plan));
    usable.selected_row_count = usable.selected_rows.len();
    let usable_count = usable.selected_row_count;
    // An empty mutation still initializes a newly requested CORRECTED_DATA
    // from DATA, as CASA's open(addcorr=True) does before checking availability.
    let (mut report, mut trace) =
        execute_apply_available(ms, usable, input, None, lock.as_deref_mut(), tool, None)?;
    let flag_start = Instant::now();
    if strict {
        let (samples, rows) = flag_unavailable(ms, &plan, &unavailable, lock)?;
        report.flagged_sample_count += samples;
        report.flagged_row_count += rows;
        report.updated_row_count += unavailable.len();
    }
    let flag_ns = flag_start.elapsed().as_nanos() as u64;
    report.wrote_measurement_set =
        report.created_corrected_data_column || usable_count > 0 || strict;
    report.timings.row_writeback_ns += flag_ns;
    trace.row_writeback_ns += flag_ns;
    let total = start.elapsed().as_nanos() as u64;
    trace.execute_apply_plan_unattributed_ns +=
        total.saturating_sub(trace.execute_apply_plan_ns + flag_ns);
    trace.execute_apply_plan_ns = total;
    trace.selected_row_count = plan.selected_row_count;
    trace.updated_row_count = report.updated_row_count;
    trace.flagged_row_count = report.flagged_row_count;
    trace.flagged_sample_count = report.flagged_sample_count;
    report.plan = plan;
    if let Some(tracer) = tracer {
        tracer.emit_apply_plan_summary(&display_ms_path(ms), &report.plan, trace);
    }
    Ok((report, trace))
}

/// Publish earlier strict flags before a later group's correction can fail.
/// Keep the same MAIN lock and resolved row identities for the whole run.
pub(super) fn execute_strict_tool(
    ms: &mut MeasurementSet,
    mut plan: ApplyPlan,
    observed_column: &str,
    lock: &mut TableWriteLock,
    tool: &mut ToolApplyExecution<'_>,
) -> Result<(), ApplyExecutionError> {
    let mut chain = ApplyChain::load(ms, &plan, false)?;
    chain.arrange_for_tool(&plan, tool.correction.parang_terms, tool.additive);
    let ordered = rows_in_buffer_order(ms, std::mem::take(&mut plan.selected_rows))?
        .into_iter()
        .map(|row| (available(&row, &plan), row))
        .collect::<Vec<_>>();
    for group in ordered.chunk_by(|first, next| first.0 == next.0) {
        plan.selected_rows = group.iter().map(|(_, row)| row.clone()).collect();
        plan.selected_row_count = group.len();
        if !group[0].0 {
            flag_unavailable(ms, &plan, &plan.selected_rows, Some(lock))?;
        } else if tool.correction.tableless_opacity {
            return Err(unresolved_opacity(ms));
        } else if plan.apply_mode == ApplyMode::Trial {
            for_each_apply_row(
                ms,
                &plan,
                observed_column,
                false,
                false,
                PreviewWeightSource::UnitFactors,
                Some(&mut *tool),
                |_, _| {},
            )?;
        } else {
            execute_apply_available(
                ms,
                plan.clone(),
                ApplyInput {
                    observed_column,
                    use_row_cache: true,
                },
                None,
                Some(lock),
                Some(&mut *tool),
                Some(&chain),
            )?;
        }
    }
    Ok(())
}

pub(super) fn rows_in_buffer_order(
    ms: &MeasurementSet,
    rows: Vec<ApplyRowPlan>,
) -> Result<Vec<ApplyRowPlan>, ApplyExecutionError> {
    Ok(buffer_ordered_rows(ms, rows)?
        .into_iter()
        .map(|(_, row)| row)
        .collect())
}

/// The explicit VI iterator sort columns identify a single visibility buffer.
type VisibilityBufferKey = (i32, i32, i32, i32, u64);

pub(super) fn buffer_ordered_rows(
    ms: &MeasurementSet,
    rows: Vec<ApplyRowPlan>,
) -> Result<Vec<(VisibilityBufferKey, ApplyRowPlan)>, ApplyExecutionError> {
    let mut ordered = rows
        .into_iter()
        .map(|row| {
            let array = get_i32(ms.main_table(), row.row_index, "ARRAY_ID")?;
            let key = (
                array,
                row.scan_number,
                row.field_id,
                row.data_desc_id,
                row.time_seconds.to_bits(),
            );
            Ok((key, row))
        })
        .collect::<Result<Vec<_>, ApplyExecutionError>>()?;
    ordered.sort_by(|(key_a, a), (key_b, b)| {
        (key_a.0, key_a.1, key_a.2, key_a.3)
            .cmp(&(key_b.0, key_b.1, key_b.2, key_b.3))
            .then_with(|| a.time_seconds.total_cmp(&b.time_seconds))
            .then_with(|| a.row_index.cmp(&b.row_index))
    });
    Ok(ordered)
}

fn flag_unavailable(
    ms: &mut MeasurementSet,
    plan: &ApplyPlan,
    rows: &[ApplyRowPlan],
    lock: Option<&mut TableWriteLock>,
) -> Result<(usize, usize), ApplyExecutionError> {
    let path = display_ms_path(ms);
    let table_error = |source| ApplyExecutionError::MutateMeasurementSet {
        path: path.clone(),
        source: MsError::from(source),
    };
    let write_error =
        |source: casa_ms::MeasurementSetWriteError| ApplyExecutionError::MutateMeasurementSet {
            path: path.clone(),
            source: MsError::InvalidInput(source.to_string()),
        };
    let mut maximum = 0;
    for row in rows {
        let shape = ms
            .main_table()
            .cell_accessor(row.row_index, "FLAG")
            .and_then(|cell| cell.array_shape())
            .map_err(table_error)?;
        let shape = shape.ok_or_else(|| ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set FLAG>".into(),
            shape: vec![],
        })?;
        if shape.len() != 2 {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set FLAG>".into(),
                shape,
            });
        }
        maximum = maximum.max(shape.iter().product::<usize>());
    }
    let write_plan = MeasurementSetWritePlan::selected_row_mutation(
        rows.iter().map(|row| row.row_index).collect(),
        vec![MeasurementSetWriteColumnPlan {
            name: "FLAG".into(),
            bytes_per_row: maximum,
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        }],
        MeasurementSetWriteResources::from_system_memory(2).map_err(write_error)?,
    )
    .map_err(write_error)?;
    let batch_rows = write_plan.batch_rows();
    let (counts, _) = MeasurementSetWriteSession::mutate_selected_rows(
        ms,
        write_plan,
        lock,
        write_error,
        |writer, ms| {
            let (mut samples, mut fully_flagged) = (0, 0);
            for batch in rows.chunks(batch_rows) {
                let indices = batch.iter().map(|row| row.row_index).collect::<Vec<_>>();
                let values = ms
                    .main_table()
                    .column_accessor("FLAG")
                    .and_then(|column| column.array_cells_owned(&indices))
                    .map_err(table_error)?;
                let mut output = Vec::with_capacity(batch.len());
                for (row, value) in batch.iter().zip(values) {
                    let value = required_prefetched_array(value, row.row_index, "FLAG", &path)?;
                    let ArrayValue::Bool(mut flags) = value else {
                        return Err(ApplyExecutionError::UnsupportedParameterShape {
                            path: "<measurement-set FLAG>".into(),
                            shape: value.shape().to_vec(),
                        });
                    };
                    let mut changed = 0;
                    if let Some(selection) = plan.channel_selections.get(&row.data_spw_id) {
                        for &channel in &selection.indices {
                            if channel >= flags.shape()[1] {
                                return Err(ApplyExecutionError::UnsupportedParameterShape {
                                    path: "<measurement-set FLAG>".into(),
                                    shape: flags.shape().to_vec(),
                                });
                            }
                            for flag in flags.index_axis_mut(ndarray::Axis(1), channel) {
                                if !*flag {
                                    changed += 1;
                                }
                                *flag = true;
                            }
                        }
                    } else {
                        for flag in &mut flags {
                            if !*flag {
                                changed += 1;
                            }
                            *flag = true;
                        }
                    }
                    samples += changed;
                    fully_flagged += usize::from(changed > 0 && flags.iter().all(|flag| *flag));
                    output.push(ArrayValue::Bool(flags));
                }
                writer
                    .write_mutation_batch(
                        ms,
                        MeasurementSetMutationBatch {
                            row_indices: indices,
                            columns: vec![MeasurementSetMutationColumnBatch {
                                name: "FLAG".into(),
                                values: MeasurementSetMutationColumnValues::Arrays(output),
                            }],
                        },
                    )
                    .map_err(write_error)?;
            }
            Ok((samples, fully_flagged))
        },
    )?;
    Ok(counts)
}
