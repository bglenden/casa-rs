// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn evaluate_rows(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
    share_input_flags: bool,
    weight_source: PreviewWeightSource,
    control: SolveControl<'_>,
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    let mut rows = HashMap::new();
    for_each_apply_row_controlled(
        ms,
        plan,
        apply_observed_column(ms, false),
        false,
        share_input_flags,
        weight_source,
        (None, control),
        |index, row| {
            rows.insert(index, row);
        },
    )?;
    Ok(rows)
}

/// Evaluate one row at a time, allowing TRIAL validation without accumulating
/// corrected data or publishing any MS changes.
#[expect(
    clippy::too_many_arguments,
    reason = "preview and tool callers use the same row executor"
)]
pub(super) fn for_each_apply_row(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
    observed_column: &str,
    forward_model: bool,
    share_input_flags: bool,
    weight_source: PreviewWeightSource,
    tool: Option<&mut ToolApplyExecution<'_>>,
    consume: impl FnMut(usize, EvaluatedApplyRow),
) -> Result<(), ApplyExecutionError> {
    for_each_apply_row_controlled(
        ms,
        plan,
        observed_column,
        forward_model,
        share_input_flags,
        weight_source,
        (tool, SolveControl::default()),
        consume,
    )
}

/// Evaluate one row at a time, allowing TRIAL validation without accumulating
/// corrected data or publishing any MS changes.
#[expect(
    clippy::too_many_arguments,
    reason = "preview and tool callers use the same row executor"
)]
pub(super) fn for_each_apply_row_controlled(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
    observed_column: &str,
    forward_model: bool,
    share_input_flags: bool,
    weight_source: PreviewWeightSource,
    (mut tool, control): (Option<&mut ToolApplyExecution<'_>>, SolveControl<'_>),
    mut consume: impl FnMut(usize, EvaluatedApplyRow),
) -> Result<(), ApplyExecutionError> {
    let ms_path = display_ms_path(ms);
    let correlation_types_by_ddid = load_correlation_types_by_ddid(ms).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source,
        }
    })?;
    let mut chain = ApplyChain::load(ms, plan, forward_model)?;
    if let Some(run) = &tool {
        chain.arrange_for_tool(plan, run.correction.parang_terms, run.additive);
    }

    let any_calwt = applies_preview_weights(plan, &chain);
    let mut columns = vec!["FLAG"];
    if !forward_model {
        columns.push(observed_column);
    }
    let plot_weight_columns = match weight_source {
        PreviewWeightSource::UnitFactors => None,
        PreviewWeightSource::FromSigma { channel_weighting } => Some(weights::plot_weight_columns(
            ms.main_table(),
            plan.selected_rows.first().map(|row| row.row_index),
            &mut columns,
            channel_weighting,
        )?),
    };
    let read_error = |source| ApplyExecutionError::MutateMeasurementSet {
        path: ms_path.clone(),
        source: MsError::from(source),
    };
    let mut readers = PreviewReaders::new(ms.main_table(), &columns).map_err(read_error)?;
    let data_index = (!forward_model).then_some(1);
    let batch_rows = preview_batch_rows(plan, &correlation_types_by_ddid);
    #[cfg(feature = "solve-test-hooks")]
    let batch_rows = if crate::solve_test_hooks::small_batches() {
        batch_rows.min(32)
    } else {
        batch_rows
    };
    let mut geometry_cache = RowGeometryCache::default();
    // Only test checkpoints use the batch index.
    #[cfg_attr(
        not(feature = "solve-test-hooks"),
        expect(clippy::unused_enumerate_index)
    )]
    for (_batch_index, batch) in plan.selected_rows.chunks(batch_rows).enumerate() {
        check_preview(control)?;
        #[cfg(feature = "solve-test-hooks")]
        control.observe(crate::solve_test_hooks::Checkpoint::PriorBatchStarted(
            _batch_index,
        ));
        let indices: Vec<_> = batch.iter().map(|row| row.row_index).collect();
        let arrays = readers
            .selected
            .iter()
            .map(|column| {
                column
                    .as_ref()
                    .map(|column| column.array_cells_owned_uncached(&indices))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(read_error)?;
        for (offset, row) in batch.iter().enumerate() {
            let correlation_types = correlation_types_by_ddid
                .get(&row.data_desc_id)
                .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: Vec::new(),
                })?;
            if let Some(prepared) = readers.prepared.as_mut() {
                prepared.load(row.row_index).map_err(read_error)?;
            }
            let read = |slot: usize| {
                if let (Some(prepared), Some(prepared_slot)) =
                    (&readers.prepared, readers.prepared_slots[slot])
                {
                    return prepared.array_at(prepared_slot).map_err(read_error);
                }
                arrays[slot]
                    .as_ref()
                    .and_then(|cells| cells[offset].as_ref())
                    .ok_or_else(|| {
                        read_error(casa_tables::TableError::ColumnNotFound {
                            row_index: row.row_index,
                            column: columns[slot].to_owned(),
                        })
                    })
            };
            let input_flags = read(0)?;
            let shared;
            let original_flags =
                if share_input_flags && let Some(flags) = shared_channel_flags(input_flags) {
                    shared = flags;
                    &shared
                } else {
                    input_flags
                };
            let units;
            let data = if let Some(index) = data_index {
                read(index)?
            } else {
                units = ArrayValue::Complex32(ArrayD::from_elem(
                    IxDyn(original_flags.shape()).f(),
                    Complex32::new(1., 0.),
                ));
                &units
            };
            let complex_data;
            let data = if let ArrayValue::Float32(values) = data {
                complex_data = complex_visibility_from_float(values);
                &complex_data
            } else {
                data
            };
            let plot_weights = plot_weight_columns
                .map(|columns| columns.read(read, data.shape()))
                .transpose()?;
            if !availability::available(row, plan) {
                if plan.apply_mode == ApplyMode::Trial {
                    continue;
                }
                // VisEquation::collapse2 excludes data that cannot be pre-calibrated
                // from solving, independently of the table's calwt setting.
                let shape = IxDyn(data.shape());
                consume(
                    row.row_index,
                    EvaluatedApplyRow {
                        corrected_data: data.clone(),
                        flags: ArrayValue::Bool(ArrayD::from_elem(shape.clone(), true)),
                        model_gains: None,
                        weight_factors: Some(ArrayD::zeros(shape)),
                        plot_weights,
                    },
                );
                continue;
            }
            // Unit weights through the calwt update leave its factors.
            let unit_weight = (any_calwt && plot_weights.is_none()).then(|| {
                ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[correlation_types.len()]), 1.0))
            });

            let result = apply_row(
                row,
                ExecutionRowInputs {
                    correlation_types,
                    data,
                    original_flags: Some(original_flags),
                    original_weight: plot_weights
                        .as_ref()
                        .map(|weights| &weights.scalar)
                        .or(unit_weight.as_ref()),
                    original_weight_spectrum: plot_weights
                        .as_ref()
                        .and_then(|weights| weights.spectrum.as_ref()),
                    has_weight_spectrum: plot_weight_columns
                        .map_or(any_calwt, weights::PlotWeightColumns::has_spectrum),
                    preserved_corrected_data: None,
                    preserved_weight_spectrum: None,
                    channel_grid: None,
                    forward_model,
                    skip_unavailable_terms: false,
                    trial: false,
                    parang_calwt: false,
                },
                plan,
                &chain,
                Some(&mut geometry_cache),
                None,
                tool.as_deref_mut().map(|run| &mut *run.activity),
            )?;
            let (plot_weights, weight_factors) = match plot_weights {
                Some(weights) => (
                    Some(PlotWeights {
                        scalar: result.updated_weight.unwrap_or(weights.scalar),
                        spectrum: result.updated_weight_spectrum.or(weights.spectrum),
                    }),
                    None,
                ),
                None => (
                    None,
                    match result.updated_weight_spectrum {
                        Some(ArrayValue::Float32(factors)) => Some(factors),
                        _ => None,
                    },
                ),
            };
            consume(
                row.row_index,
                EvaluatedApplyRow {
                    corrected_data: result.corrected_data,
                    model_gains: None,
                    flags: result
                        .updated_flags
                        .unwrap_or_else(|| original_flags.clone()),
                    weight_factors,
                    plot_weights,
                },
            );
        }
        #[cfg(feature = "solve-test-hooks")]
        control.observe(crate::solve_test_hooks::Checkpoint::PriorBatchFinished(
            _batch_index,
        ));
    }

    check_preview(control)
}

fn check_preview(control: SolveControl<'_>) -> Result<(), ApplyExecutionError> {
    if control.is_cancelled() {
        return Err(ApplyExecutionError::Interrupted);
    }
    Ok(())
}

pub(super) struct PreviewReaders<'a> {
    pub(super) selected: Vec<Option<casa_tables::TableColumn<'a>>>,
    pub(super) prepared: Option<casa_tables::PreparedTableRow<'a>>,
    pub(super) prepared_slots: Vec<Option<usize>>,
}

impl<'a> PreviewReaders<'a> {
    fn new(
        main: &'a casa_tables::Table,
        columns: &[&str],
    ) -> Result<Self, casa_tables::TableError> {
        let selected = columns
            .iter()
            .map(|&name| {
                let supports_selection = main.data_manager_info().iter().any(|manager| {
                    matches!(
                        manager.dm_type.as_str(),
                        "StandardStMan"
                            | "TiledColumnStMan"
                            | "TiledShapeStMan"
                            | "TiledCellStMan"
                            | "TiledDataStMan"
                    ) && manager.columns.iter().any(|column| column == name)
                });
                supports_selection
                    .then(|| main.column_accessor(name))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Linked tables and other whole-column readers reuse one projection.
        let mut prepared_columns = Vec::new();
        let prepared_slots = selected
            .iter()
            .zip(columns)
            .map(|(reader, &name)| {
                reader.is_none().then(|| {
                    let slot = prepared_columns.len();
                    prepared_columns.push(name);
                    slot
                })
            })
            .collect();
        let prepared = (!prepared_columns.is_empty())
            .then(|| main.row_accessor().prepare(&prepared_columns))
            .transpose()?;
        Ok(Self {
            selected,
            prepared,
            prepared_slots,
        })
    }
}

pub(super) fn preview_batch_rows(plan: &ApplyPlan, correlations: &HashMap<i32, Vec<i32>>) -> usize {
    const BATCH_BYTES: usize = 8 * 1024 * 1024;
    const MAX_ROWS: usize = 256;
    let channels = plan
        .measurement_set_spectral_windows
        .iter()
        .filter_map(|window| usize::try_from(window.num_chan).ok())
        .max()
        .unwrap_or(0);
    let correlations = correlations.values().map(Vec::len).max().unwrap_or(0);
    // Bound storage for the data, flags and an optional sigma spectrum.
    let row_bytes = channels.saturating_mul(correlations).saturating_mul(16);
    (BATCH_BYTES / row_bytes.max(1)).clamp(1, MAX_ROWS)
}

pub(super) fn applies_preview_weights(plan: &ApplyPlan, chain: &ApplyChain) -> bool {
    plan.apply_mode.writes_corrected_data()
        && plan
            .calibration_tables
            .iter()
            .zip(&chain.tables)
            .any(|(table, loaded)| table.calwt && loaded.supports_calwt)
}

pub(super) fn shared_channel_flags(flags: &ArrayValue) -> Option<ArrayValue> {
    let ArrayValue::Bool(flags) = flags else {
        return None;
    };
    if flags.ndim() != 2 {
        return None;
    }
    let mut flags = flags.clone();
    for mut channel in flags.axis_iter_mut(ndarray::Axis(1)) {
        if channel.iter().any(|&flag| flag) {
            channel.fill(true);
        }
    }
    Some(ArrayValue::Bool(flags))
}
