// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Plan and execute calibration application against an on-disk MeasurementSet.
///
/// This is the mechanism alone and saves no flag backup; the `applycal` task
/// calls [`execute_apply_from_path_with_options`].
pub fn execute_apply_from_path(
    path: impl AsRef<Path>,
    request: &ApplyPlanRequest,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    execute_apply_from_path_with_options(path, request, ApplyExecutionOptions::default())
}

/// Plan and execute calibration application against an on-disk MeasurementSet
/// with the task-level `options`.
pub fn execute_apply_from_path_with_options(
    path: impl AsRef<Path>,
    request: &ApplyPlanRequest,
    options: ApplyExecutionOptions,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    execute_apply_from_path_inner(path.as_ref(), request, options, None, false)
}

/// Execute `applycal` with optional deferred preparation error checked after MS setup.
pub fn execute_apply_with_preparation_error(
    path: &Path,
    request: &ApplyPlanRequest,
    options: ApplyExecutionOptions,
    preparation_error: Option<crate::plan::ApplyPreparationError<'_>>,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    execute_apply_from_path_inner(path, request, options, preparation_error, true)
}

/// Execute single-dish calibration application against an on-disk MeasurementSet.
pub fn execute_single_dish_apply(
    path: &Path,
    request: &ApplyPlanRequest,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    execute_apply_from_path_inner(path, request, ApplyExecutionOptions::default(), None, true)
}

pub(super) fn execute_apply_from_path_inner(
    path: &Path,
    request: &ApplyPlanRequest,
    options: ApplyExecutionOptions,
    preparation_error: Option<crate::plan::ApplyPreparationError<'_>>,
    prefer_float: bool,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    let path = path.to_path_buf();
    let total_started_at = Instant::now();
    let mut perf_tracer = CalibrationPerfTracer::from_env();
    let (mut report, plan_trace_summary, pre_drop_total_ns) = {
        let open_started_at = Instant::now();
        let mut ms = MeasurementSet::open(&path).map_err(|source| {
            ApplyExecutionError::OpenMeasurementSet {
                path: path.display().to_string(),
                source,
            }
        })?;
        let observed_column = apply_observed_column(&ms, prefer_float);
        let open_measurement_set_ns = open_started_at.elapsed().as_nanos() as u64;
        let flag_backup_version = save_apply_flag_backup(&ms, request.apply_mode, options)?;
        let planning_started_at = Instant::now();
        let (plan, plan_timings) =
            match crate::plan::plan_apply_with_preparation_error(&ms, request, preparation_error) {
                Ok(planned) => planned,
                Err(error) => {
                    // CASA opens with addcorr=True before selectvis/setapply. Keep
                    // successful application on the existing single-write path;
                    // reproduce that initialization if planning rejects a request.
                    if request.apply_mode.writes_corrected_data() {
                        initialization::corrected_data(&mut ms, observed_column)?;
                    }
                    return Err(error.into());
                }
            };
        let planning_ns = planning_started_at.elapsed().as_nanos() as u64;
        if plan.apply_mode == ApplyMode::Trial {
            // Calibrater::correct2 still runs the calibration operators in
            // trial mode, including their input/basis validation.
            for_each_apply_row(
                &ms,
                &plan,
                observed_column,
                false,
                false,
                PreviewWeightSource::UnitFactors,
                None,
                |_, _| {},
            )?;
            return Ok(ApplyExecutionReport {
                plan,
                created_corrected_data_column: false,
                wrote_measurement_set: false,
                updated_row_count: 0,
                flagged_row_count: 0,
                flagged_sample_count: 0,
                flag_backup_version: None,
                timings: ApplyExecutionTimings {
                    planning_ns,
                    planning_selection_ns: plan_timings.selection_ns,
                    planning_selected_rows_ns: plan_timings.selected_rows_ns,
                    planning_measurement_set_spectral_windows_ns: plan_timings
                        .measurement_set_spectral_windows_ns,
                    planning_calibration_table_plans_ns: plan_timings.calibration_table_plans_ns,
                    open_measurement_set_ns,
                    total_ns: total_started_at.elapsed().as_nanos() as u64,
                    ..ApplyExecutionTimings::default()
                },
            });
        }

        let (mut report, trace_summary) = execute_apply_plan(
            &mut ms,
            plan,
            ApplyInput {
                observed_column,
                use_row_cache: false,
            },
            Some(&mut perf_tracer),
            None,
            None,
        )?;
        report.flag_backup_version = flag_backup_version;
        report.timings.planning_ns = planning_ns;
        report.timings.planning_selection_ns = plan_timings.selection_ns;
        report.timings.planning_selected_rows_ns = plan_timings.selected_rows_ns;
        report.timings.planning_measurement_set_spectral_windows_ns =
            plan_timings.measurement_set_spectral_windows_ns;
        report.timings.planning_calibration_table_plans_ns =
            plan_timings.calibration_table_plans_ns;
        report.timings.open_measurement_set_ns = open_measurement_set_ns;
        let pre_drop_total_ns = total_started_at.elapsed().as_nanos() as u64;
        if calibration_profile_enabled() {
            log_calibration_profile(
                "execute_apply_from_path.pre_drop",
                pre_drop_total_ns as f64 / 1_000_000_000.0,
                Some(format!(
                    "rows={} report_total_so_far={:.3}s",
                    report.updated_row_count,
                    pre_drop_total_ns as f64 / 1_000_000_000.0
                )),
            );
        }
        (report, trace_summary, pre_drop_total_ns)
    };
    let after_drop_ns = total_started_at.elapsed().as_nanos() as u64;
    let drop_ns = after_drop_ns.saturating_sub(pre_drop_total_ns);
    if calibration_profile_enabled() {
        log_calibration_profile(
            "execute_apply_from_path.drop",
            drop_ns as f64 / 1_000_000_000.0,
            Some(format!(
                "total_after_drop={:.3}s",
                after_drop_ns as f64 / 1_000_000_000.0
            )),
        );
    }
    report.timings.total_ns = after_drop_ns;
    perf_tracer.emit_apply_completed(
        &path.display().to_string(),
        &report,
        drop_ns,
        plan_trace_summary,
    );
    Ok(report)
}

pub(super) fn save_apply_flag_backup(
    ms: &MeasurementSet,
    mode: ApplyMode,
    options: ApplyExecutionOptions,
) -> Result<Option<String>, ApplyExecutionError> {
    if !options.flag_backup || !mode.writes_flags() {
        return Ok(None);
    }
    save_next_flag_backup(ms, "applycal")
        .map(Some)
        .map_err(|source| ApplyExecutionError::FlagBackup {
            path: display_ms_path(ms),
            source: Box::new(source),
        })
}

/// Plan and execute calibration application against an already-open MeasurementSet.
pub fn execute_apply(
    ms: &mut MeasurementSet,
    request: &ApplyPlanRequest,
) -> Result<ApplyExecutionReport, ApplyExecutionError> {
    let total_started_at = Instant::now();
    let mut perf_tracer = CalibrationPerfTracer::from_env();
    let planning_started_at = Instant::now();
    let (plan, plan_timings) = plan_apply_with_timings(ms, request)?;
    let planning_ns = planning_started_at.elapsed().as_nanos() as u64;
    if plan.apply_mode == ApplyMode::Trial {
        for_each_apply_row(
            ms,
            &plan,
            apply_observed_column(ms, false),
            false,
            false,
            PreviewWeightSource::UnitFactors,
            None,
            |_, _| {},
        )?;
        return Ok(ApplyExecutionReport {
            plan,
            created_corrected_data_column: false,
            wrote_measurement_set: false,
            updated_row_count: 0,
            flagged_row_count: 0,
            flagged_sample_count: 0,
            flag_backup_version: None,
            timings: ApplyExecutionTimings {
                planning_ns,
                planning_selection_ns: plan_timings.selection_ns,
                planning_selected_rows_ns: plan_timings.selected_rows_ns,
                planning_measurement_set_spectral_windows_ns: plan_timings
                    .measurement_set_spectral_windows_ns,
                planning_calibration_table_plans_ns: plan_timings.calibration_table_plans_ns,
                total_ns: total_started_at.elapsed().as_nanos() as u64,
                ..ApplyExecutionTimings::default()
            },
        });
    }
    let ms_path = display_ms_path(ms);
    let observed_column = apply_observed_column(ms, false);
    let (mut report, trace_summary) = execute_apply_plan(
        ms,
        plan,
        ApplyInput {
            observed_column,
            use_row_cache: true,
        },
        Some(&mut perf_tracer),
        None,
        None,
    )?;
    report.timings.planning_ns = planning_ns;
    report.timings.planning_selection_ns = plan_timings.selection_ns;
    report.timings.planning_selected_rows_ns = plan_timings.selected_rows_ns;
    report.timings.planning_measurement_set_spectral_windows_ns =
        plan_timings.measurement_set_spectral_windows_ns;
    report.timings.planning_calibration_table_plans_ns = plan_timings.calibration_table_plans_ns;
    report.timings.total_ns = total_started_at.elapsed().as_nanos() as u64;
    perf_tracer.emit_apply_completed(&ms_path, &report, 0, trace_summary);
    Ok(report)
}

#[derive(Clone, Copy)]
pub(crate) struct ToolCorrection {
    pub(crate) has_library_registration: bool,
    pub(crate) strict: bool,
    pub(crate) trial: bool,
    pub(crate) parang_terms: usize,
    pub(crate) parang_calwt: bool,
    pub(crate) tableless_opacity: bool,
}

pub(crate) struct ToolActivityInput<'a> {
    pub(crate) names: &'a [String],
    pub(crate) libraries: &'a [LibraryApplyRegistration],
    pub(crate) opacity: &'a [(&'static str, usize)],
    pub(crate) previous: ApplyTermCounts,
    pub(crate) additive: &'a additive::ToolState,
}

pub(super) fn unresolved_opacity(ms: &MeasurementSet) -> ApplyExecutionError {
    ApplyExecutionError::MutateMeasurementSet {
        path: display_ms_path(ms),
        source: MsError::InvalidInput("Error in TOpac::calcPar()".into()),
    }
}

pub(crate) fn has_usable_apply_rows(plan: &ApplyPlan) -> bool {
    plan.selected_rows
        .iter()
        .any(|row| availability::available(row, plan))
}

pub(super) struct ToolApplyExecution<'a> {
    pub(super) correction: ToolCorrection,
    pub(super) activity: &'a mut ToolApplyActivity,
    pub(super) additive: &'a additive::ToolState,
}

/// Correct the selection resolved under the same lock used for row writes.
pub(crate) fn execute_tool_apply(
    ms: &mut MeasurementSet,
    selected: &ResolvedMsSelection,
    lock: &mut TableWriteLock,
    request: &ApplyPlanRequest,
    correction: ToolCorrection,
    tracking: ToolActivityInput<'_>,
) -> Result<(bool, ToolApplyActivity), ApplyExecutionError> {
    let registration = RegisteredApplySelection {
        selected,
        has_library_registration: correction.has_library_registration,
        legacy_corruption: false,
        legacy_libraries: &[],
    };
    let (mut plan, _, _) = plan_with_selection(ms, request, None, Some(registration))?;
    if additive::in_plan(&plan) {
        plan.selected_rows =
            availability::rows_in_buffer_order(ms, std::mem::take(&mut plan.selected_rows))?;
    }
    let mut activity = ToolApplyActivity::from_plan(
        &plan,
        tracking.names,
        tracking.libraries,
        correction.parang_terms,
        tracking.opacity,
        tracking.previous,
    );
    let mut run = ToolApplyExecution {
        correction,
        activity: &mut activity,
        additive: tracking.additive,
    };
    let all_calibrated = plan
        .selected_rows
        .iter()
        .all(|row| availability::available(row, &plan));
    let observed_column = apply_observed_column(ms, false);
    if correction.strict && !all_calibrated {
        availability::execute_strict_tool(ms, plan, observed_column, lock, &mut run)?;
        return Ok((false, activity));
    }
    if correction.tableless_opacity && has_usable_apply_rows(&plan) {
        return Err(unresolved_opacity(ms));
    }
    if plan.apply_mode == ApplyMode::Trial {
        for_each_apply_row(
            ms,
            &plan,
            observed_column,
            false,
            false,
            PreviewWeightSource::UnitFactors,
            Some(&mut run),
            |_, _| {},
        )?;
    } else {
        execute_apply_plan(
            ms,
            plan,
            ApplyInput {
                observed_column,
                use_row_cache: true,
            },
            None,
            Some(lock),
            Some(&mut run),
        )?;
    }
    Ok((all_calibrated, activity))
}

#[derive(Clone, Copy)]
pub(super) struct ApplyInput<'a> {
    pub(super) observed_column: &'a str,
    /// An already-open MeasurementSet can contain unflushed array edits.
    pub(super) use_row_cache: bool,
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn execute_apply_available(
    ms: &mut MeasurementSet,
    plan: ApplyPlan,
    input: ApplyInput<'_>,
    perf_tracer: Option<&mut CalibrationPerfTracer>,
    lock: Option<&mut TableWriteLock>,
    mut tool: Option<&mut ToolApplyExecution<'_>>,
    retained_chain: Option<&ApplyChain>,
) -> Result<(ApplyExecutionReport, ExecuteApplyPlanTraceSummary), ApplyExecutionError> {
    let ms_path = display_ms_path(ms);
    let observed_column = input.observed_column;
    let execute_apply_plan_started_at = Instant::now();
    let calibration_load_started_at = Instant::now();
    let correction = tool.as_ref().map(|run| run.correction);
    let loaded;
    let chain = if let Some(chain) = retained_chain {
        chain
    } else {
        let mut chain = ApplyChain::load(ms, &plan, false)?;
        if let Some(tool) = &tool {
            chain.arrange_for_tool(&plan, tool.correction.parang_terms, tool.additive);
        }
        loaded = chain;
        &loaded
    };
    let trial = correction.is_some_and(|tool| tool.trial);
    let calibration_load_ns = calibration_load_started_at.elapsed().as_nanos() as u64;
    let ensure_corrected_data_started_at = Instant::now();
    if lock.is_some() && plan.requires_corrected_data_column && !plan.selected_rows.is_empty() {
        return Err(ApplyExecutionError::MutateMeasurementSet {
            path: ms_path,
            source: MsError::ColumnNotPresent(VisibilityDataColumn::CorrectedData.name().into()),
        });
    }
    let corrected_data_creation = if plan.apply_mode.writes_corrected_data() && lock.is_none() {
        ensure_corrected_data_column(ms, observed_column).map_err(|source| {
            ApplyExecutionError::CreateCorrectedData {
                path: ms_path.clone(),
                source,
            }
        })?
    } else {
        CorrectedDataCreation { created: false }
    };
    let created_corrected_data_column = corrected_data_creation.created;
    let ensure_corrected_data_ns = ensure_corrected_data_started_at.elapsed().as_nanos() as u64;

    let correlation_lookup_started_at = Instant::now();
    let correlation_types_by_ddid = load_correlation_types_by_ddid(ms).map_err(|source| {
        ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source,
        }
    })?;
    let correlation_lookup_ns = correlation_lookup_started_at.elapsed().as_nanos() as u64;
    let parang_calwt = correction.is_some_and(|tool| tool.parang_calwt)
        && plan.selected_rows.iter().all(|row| {
            correlation_types_by_ddid
                .get(&row.data_desc_id)
                .and_then(|types| types.first())
                == Some(&5)
        });
    let mut updated_row_count = 0;
    let mut flagged_row_count = 0;
    let mut flagged_sample_count = 0;
    let mut row_read_total_ns = 0_u64;
    let mut row_fetch_ns = 0_u64;
    let mut row_compute_ns = 0_u64;
    let mut row_writeback_ns = 0_u64;
    let any_calwt = plan.apply_mode.writes_corrected_data()
        && (parang_calwt
            || plan
                .calibration_tables
                .iter()
                .zip(&chain.tables)
                .any(|(table, loaded)| table.calwt && loaded.supports_calwt));
    let first_row = plan.selected_rows.first().map(|row| row.row_index);
    let has_weight_spectrum =
        any_calwt && weights::has_spectrum(ms.main_table(), first_row, "WEIGHT_SPECTRUM")?;
    let has_sigma_spectrum =
        has_weight_spectrum && weights::has_spectrum(ms.main_table(), first_row, "SIGMA_SPECTRUM")?;
    let mut changed_columns: Vec<&'static str> = if plan.apply_mode.writes_corrected_data() {
        vec![VisibilityDataColumn::CorrectedData.name()]
    } else {
        Vec::new()
    };
    let row_loop_started_at = Instant::now();
    let mut geometry_cache = RowGeometryCache::default();
    let mut row_compute_profile =
        calibration_profile_enabled().then(ApplyRowComputeProfile::default);
    if plan.apply_mode.writes_flags() {
        changed_columns.push("FLAG");
    }
    if any_calwt {
        changed_columns.push("WEIGHT");
        if has_weight_spectrum {
            changed_columns.push("WEIGHT_SPECTRUM");
        }
    }
    let maximum_data_elements = maximum_visibility_cells(&plan, &correlation_types_by_ddid)?;
    let maximum_weight_elements = plan
        .selected_rows
        .iter()
        .filter_map(|row| {
            correlation_types_by_ddid
                .get(&row.data_desc_id)
                .map(Vec::len)
        })
        .max()
        .unwrap_or(0);
    let write_columns = changed_columns
        .iter()
        .map(|column| MeasurementSetWriteColumnPlan {
            name: (*column).to_string(),
            bytes_per_row: match *column {
                "CORRECTED_DATA" => maximum_data_elements.saturating_mul(8),
                "FLAG" => maximum_data_elements,
                "WEIGHT" => maximum_weight_elements.saturating_mul(4),
                "WEIGHT_SPECTRUM" => maximum_data_elements.saturating_mul(4),
                _ => 0,
            },
            mode: if *column == VisibilityDataColumn::CorrectedData.name()
                && created_corrected_data_column
            {
                MeasurementSetColumnWriteMode::Create
            } else {
                MeasurementSetColumnWriteMode::Replace
            },
            storage_manager: if *column == VisibilityDataColumn::CorrectedData.name()
                && created_corrected_data_column
            {
                MeasurementSetColumnStorage::TiledShape
            } else {
                MeasurementSetColumnStorage::Persisted
            },
            tile_shape: None,
            create_source_column: (*column == VisibilityDataColumn::CorrectedData.name()
                && created_corrected_data_column
                && observed_column == VisibilityDataColumn::Data.name())
            .then(|| observed_column.to_string()),
        })
        .collect();
    let write_resources =
        MeasurementSetWriteResources::from_system_memory(2).map_err(|source| {
            ApplyExecutionError::MutateMeasurementSet {
                path: ms_path.clone(),
                source: MsError::InvalidInput(source.to_string()),
            }
        })?;
    let write_plan = MeasurementSetWritePlan::selected_row_mutation(
        plan.selected_rows.iter().map(|row| row.row_index).collect(),
        write_columns,
        write_resources,
    )
    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
        path: ms_path.clone(),
        source: MsError::InvalidInput(source.to_string()),
    })?;
    let batch_rows = write_plan.batch_rows().max(1);
    let write_error =
        |source: casa_ms::MeasurementSetWriteError| ApplyExecutionError::MutateMeasurementSet {
            path: ms_path.clone(),
            source: MsError::InvalidInput(source.to_string()),
        };

    let row_field_index_lookup_ns = 0;
    let (save_started_at, _) = MeasurementSetWriteSession::mutate_selected_rows(
        ms,
        write_plan,
        lock,
        write_error,
        |write_session, ms| {
            for planned_rows in plan.selected_rows.chunks(batch_rows) {
                let selected_row_indices = planned_rows
                    .iter()
                    .map(|row| row.row_index)
                    .collect::<Vec<_>>();
                let row_read_started_at = Instant::now();
                let row_fetch_started_at = Instant::now();
                let read_cells = |name: &str| {
                    ms.main_table()
                        .column_accessor(name)
                        .and_then(|column| {
                            if input.use_row_cache {
                                column.array_cells_owned(&selected_row_indices)
                            } else {
                                column.array_cells_owned_uncached(&selected_row_indices)
                            }
                        })
                        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                            path: ms_path.clone(),
                            source: MsError::from(source),
                        })
                };
                let data_values = read_cells(observed_column)?;
                let flag_values = (plan.apply_mode.writes_flags() || tool.is_some())
                    .then(|| read_cells("FLAG"))
                    .transpose()?;
                let weight_values = any_calwt.then(|| read_cells("SIGMA")).transpose()?;
                let weight_spectrum_values = has_sigma_spectrum
                    .then(|| read_cells("SIGMA_SPECTRUM"))
                    .transpose()?;
                let partial_channels = planned_rows
                    .iter()
                    .any(|row| plan.channel_selections.contains_key(&row.data_spw_id));
                let preserved_corrected_values = (partial_channels
                    && plan.apply_mode.writes_corrected_data()
                    && !created_corrected_data_column)
                    .then(|| read_cells("CORRECTED_DATA"))
                    .transpose()?;
                let preserved_spectrum_values = (partial_channels && has_weight_spectrum)
                    .then(|| read_cells("WEIGHT_SPECTRUM"))
                    .transpose()?;
                row_fetch_ns += row_fetch_started_at.elapsed().as_nanos() as u64;
                row_read_total_ns += row_read_started_at.elapsed().as_nanos() as u64;
                let mut data_values = data_values.into_iter();
                let mut flag_values = flag_values.map(Vec::into_iter);
                let mut weight_values = weight_values.map(Vec::into_iter);
                let mut weight_spectrum_values = weight_spectrum_values.map(Vec::into_iter);
                let mut preserved_corrected_values = preserved_corrected_values.map(Vec::into_iter);
                let mut preserved_spectrum_values = preserved_spectrum_values.map(Vec::into_iter);
                let mut corrected_batch = Vec::with_capacity(planned_rows.len());
                let mut flag_batch = Vec::with_capacity(planned_rows.len());
                let mut weight_batch = Vec::with_capacity(planned_rows.len());
                let mut weight_spectrum_batch = Vec::with_capacity(planned_rows.len());

                for row in planned_rows {
                    let data = required_prefetched_array(
                        data_values.next().flatten(),
                        row.row_index,
                        observed_column,
                        &ms_path,
                    )?;
                    let data = match data {
                        ArrayValue::Float32(values) => complex_visibility_from_float(&values),
                        data => data,
                    };
                    let prefetched = PrefetchedExecutionRowInputs {
                        trial,
                        parang_calwt,
                        preserved_corrected_data: preserved_corrected_values
                            .as_mut()
                            .and_then(|values| values.next().flatten()),
                        preserved_weight_spectrum: preserved_spectrum_values
                            .as_mut()
                            .and_then(|values| values.next().flatten()),
                        data,
                        original_flags: flag_values
                            .as_mut()
                            .map(|values| {
                                required_prefetched_array(
                                    values.next().flatten(),
                                    row.row_index,
                                    "FLAG",
                                    &ms_path,
                                )
                            })
                            .transpose()?,
                        original_weight: weight_values
                            .as_mut()
                            .map(|values| {
                                weights::from_sigma(
                                    required_prefetched_array(
                                        values.next().flatten(),
                                        row.row_index,
                                        "SIGMA",
                                        &ms_path,
                                    )?,
                                    "SIGMA",
                                )
                            })
                            .transpose()?,
                        original_weight_spectrum: weight_spectrum_values
                            .as_mut()
                            .map(|values| {
                                weights::from_sigma(
                                    required_prefetched_array(
                                        values.next().flatten(),
                                        row.row_index,
                                        "SIGMA_SPECTRUM",
                                        &ms_path,
                                    )?,
                                    "SIGMA_SPECTRUM",
                                )
                            })
                            .transpose()?,
                    };
                    let correlation_types = correlation_types_by_ddid
                        .get(&row.data_desc_id)
                        .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                            data_desc_id: row.data_desc_id,
                            correlation_types: Vec::new(),
                        })?;
                    let row_compute_started_at = Instant::now();
                    let result = apply_row_prefetched(
                        row,
                        correlation_types,
                        &prefetched,
                        any_calwt && has_weight_spectrum,
                        &plan,
                        chain,
                        Some(&mut geometry_cache),
                        row_compute_profile.as_mut(),
                        tool.as_deref_mut().map(|run| &mut *run.activity),
                    )?;
                    row_compute_ns += row_compute_started_at.elapsed().as_nanos() as u64;
                    if plan.apply_mode.writes_corrected_data() {
                        corrected_batch.push(result.corrected_data);
                    }
                    if plan.apply_mode.writes_flags() {
                        flag_batch.push(result.updated_flags.unwrap_or_else(|| {
                            prefetched
                                .original_flags
                                .expect("CalFlag rows always prefetch FLAG")
                        }));
                        if result.row_became_fully_flagged {
                            flagged_row_count += 1;
                        }
                        flagged_sample_count += result.newly_flagged_samples;
                    }
                    if any_calwt {
                        weight_batch.push(result.updated_weight.ok_or_else(|| {
                            ApplyExecutionError::MutateMeasurementSet {
                                path: ms_path.clone(),
                                source: MsError::InvalidInput(
                                    "calwt row did not produce WEIGHT output".to_string(),
                                ),
                            }
                        })?);
                        if has_weight_spectrum {
                            weight_spectrum_batch.push(result.updated_weight_spectrum.ok_or_else(
                                || {
                                    ApplyExecutionError::MutateMeasurementSet {
                                        path: ms_path.clone(),
                                        source: MsError::InvalidInput(
                                            "calwt row did not produce WEIGHT_SPECTRUM output"
                                                .to_string(),
                                        ),
                                    }
                                },
                            )?);
                        }
                    }
                    updated_row_count += 1;
                }

                let mut columns = if plan.apply_mode.writes_corrected_data() {
                    vec![MeasurementSetMutationColumnBatch {
                        name: VisibilityDataColumn::CorrectedData.name().to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(corrected_batch),
                    }]
                } else {
                    Vec::new()
                };
                if plan.apply_mode.writes_flags() {
                    columns.push(MeasurementSetMutationColumnBatch {
                        name: "FLAG".to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(flag_batch),
                    });
                }
                if any_calwt {
                    columns.push(MeasurementSetMutationColumnBatch {
                        name: "WEIGHT".to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(weight_batch),
                    });
                    if has_weight_spectrum {
                        columns.push(MeasurementSetMutationColumnBatch {
                            name: "WEIGHT_SPECTRUM".to_string(),
                            values: MeasurementSetMutationColumnValues::Arrays(
                                weight_spectrum_batch,
                            ),
                        });
                    }
                }
                let row_writeback_started_at = Instant::now();
                write_session
                    .write_mutation_batch(
                        ms,
                        MeasurementSetMutationBatch {
                            row_indices: selected_row_indices,
                            columns,
                        },
                    )
                    .map_err(write_error)?;
                row_writeback_ns += row_writeback_started_at.elapsed().as_nanos() as u64;
            }
            Ok(Instant::now())
        },
    )?;
    let row_loop_ns = save_started_at
        .duration_since(row_loop_started_at)
        .as_nanos() as u64;
    let save_ns = save_started_at.elapsed().as_nanos() as u64;
    let row_read_overhead_ns = row_read_total_ns.saturating_sub(row_fetch_ns);
    let execute_apply_plan_ns = execute_apply_plan_started_at.elapsed().as_nanos() as u64;
    let bucketed_ns = ensure_corrected_data_ns
        + correlation_lookup_ns
        + calibration_load_ns
        + row_field_index_lookup_ns
        + row_read_total_ns
        + row_compute_ns
        + row_writeback_ns
        + save_ns;
    let execute_apply_plan_unattributed_ns = execute_apply_plan_ns.saturating_sub(bucketed_ns);
    let trace_summary = ExecuteApplyPlanTraceSummary {
        selected_row_count: plan.selected_rows.len(),
        calibration_table_count: plan.calibration_tables.len(),
        parang: plan.parang,
        created_corrected_data_column,
        updated_row_count,
        flagged_row_count,
        flagged_sample_count,
        row_field_index_lookup_ns,
        ensure_corrected_data_ns,
        correlation_lookup_ns,
        calibration_load_ns,
        row_loop_ns,
        row_read_total_ns,
        row_fetch_ns,
        row_compute_ns,
        row_read_overhead_ns,
        row_writeback_ns,
        save_ns,
        execute_apply_plan_ns,
        execute_apply_plan_unattributed_ns,
    };
    if let Some(perf_tracer) = perf_tracer {
        perf_tracer.emit_apply_plan_summary(&ms_path, &plan, trace_summary);
    }
    if calibration_profile_enabled() {
        log_calibration_profile(
            "execute_apply_plan",
            execute_apply_plan_ns as f64 / 1_000_000_000.0,
            Some(format!(
                "rows={} row_loop={:.3}s bucketed={:.3}s unattributed={:.3}s ensure_corrected_data={:.3}s correlation_lookup={:.3}s calibration_load={:.3}s row_field_index_lookup={:.3}s row_read_total={:.3}s row_fetch={:.3}s row_compute={:.3}s row_read_overhead={:.3}s row_writeback={:.3}s save={:.3}s",
                plan.selected_rows.len(),
                row_loop_ns as f64 / 1_000_000_000.0,
                bucketed_ns as f64 / 1_000_000_000.0,
                execute_apply_plan_unattributed_ns as f64 / 1_000_000_000.0,
                ensure_corrected_data_ns as f64 / 1_000_000_000.0,
                correlation_lookup_ns as f64 / 1_000_000_000.0,
                calibration_load_ns as f64 / 1_000_000_000.0,
                row_field_index_lookup_ns as f64 / 1_000_000_000.0,
                row_read_total_ns as f64 / 1_000_000_000.0,
                row_fetch_ns as f64 / 1_000_000_000.0,
                row_compute_ns as f64 / 1_000_000_000.0,
                row_read_overhead_ns as f64 / 1_000_000_000.0,
                row_writeback_ns as f64 / 1_000_000_000.0,
                save_ns as f64 / 1_000_000_000.0
            )),
        );
        if let Some(profile) = row_compute_profile {
            log_calibration_profile(
                "apply_row_compute",
                row_compute_ns as f64 / 1_000_000_000.0,
                Some(profile.detail_string()),
            );
        }
    }

    Ok((
        ApplyExecutionReport {
            plan,
            created_corrected_data_column,
            wrote_measurement_set: true,
            updated_row_count,
            flagged_row_count,
            flagged_sample_count,
            flag_backup_version: None,
            timings: ApplyExecutionTimings {
                planning_ns: 0,
                open_measurement_set_ns: 0,
                row_field_index_lookup_ns,
                ensure_corrected_data_ns,
                correlation_lookup_ns,
                calibration_load_ns,
                row_compute_ns,
                row_writeback_ns,
                save_ns,
                total_ns: 0,
                ..ApplyExecutionTimings::default()
            },
        },
        trace_summary,
    ))
}
