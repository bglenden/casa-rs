// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared MeasurementSet flagging execution and sequential multi-agent programs.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use casa_core::ScalarValue;

use casa_ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_ms::selection::syntax::ChannelSelection;
use casa_ms::selection::{
    TimeSelection, UvDistanceSelection, compile_flagdata_agent_time, first_unflagged_time_row,
    flagdata_c_string,
};

use super::shared_average::{SharedAgentSelection, SharedAveraging, SharedFlagView};

use super::{
    AveragedAgentInput, ChangeSet, CorrelationSelection, FlagDataAction, FlagDataMode,
    FlagDataModelFactory, FlagDataReport, FlagDataRequest, FlagTimeSelection, FlaggingError,
    SelectedFlagRowPolicy, antint, apply_clip_flags, apply_quack_flags, apply_rflag_flags,
    apply_tfcrop_flags, check_cancelled, display_ms_path, elevation, extend, flagging_channels,
    is_calibration_table, load_i32_cells, persist_changed_flags, report_after,
    save_next_casa_flag_backup, save_next_flag_backup, selected_flag_row_policy, selected_rows,
    set_selected_flags, shadow, summary,
};

struct AgentSelection {
    rows: Vec<usize>,
    channels: Option<BTreeMap<i32, ChannelSelection>>,
    correlations: Option<CorrelationSelection>,
    averaged_time: Option<TimeSelection>,
    averaged_uv: Option<UvDistanceSelection>,
}

impl AgentSelection {
    fn shared(&self) -> SharedAgentSelection<'_> {
        SharedAgentSelection {
            rows: Some(&self.rows),
            channels: self.channels.as_ref(),
            correlations: self.correlations.as_ref(),
            time: self.averaged_time.as_ref(),
            uv: self.averaged_uv.as_ref(),
        }
    }
}

struct AntintPlan {
    selection: AgentSelection,
    affected: Option<Vec<usize>>,
}

/// Validate CASA's allowed time and channel averaging in a list of agents.
///
/// Each config provides the mode, channel-average switch, and time-average
/// switch, in list order.
///
/// # Errors
/// An agent requests averaging in an unsupported mode or alongside an agent
/// that cannot run with an averaged automatic agent.
pub fn validate_calibration_flag_averaging<'a>(
    configs: impl Iterator<Item = (&'a str, bool, bool)> + Clone,
) -> Result<(), FlaggingError> {
    validate_flag_averaging(configs, false)
}

/// Check whether a MeasurementSet list combines averaging with an unsupported mode.
///
/// # Errors
/// An agent requests averaging in an unsupported mode or list.
pub fn validate_ms_flag_averaging<'a>(
    configs: impl Iterator<Item = (&'a str, bool, bool)> + Clone,
) -> Result<(), FlaggingError> {
    validate_flag_averaging(configs, true)
}

fn validate_flag_averaging<'a>(
    configs: impl Iterator<Item = (&'a str, bool, bool)> + Clone,
    shared_averaging: bool,
) -> Result<(), FlaggingError> {
    let automatic = |mode: &str| matches!(mode, "clip" | "tfcrop" | "rflag");
    let incompatible = configs
        .clone()
        .map(|(mode, _, _)| mode)
        .find(|mode| !automatic(mode) && !matches!(*mode, "extend" | "display" | "antint"));
    let multiple_automatic = configs
        .clone()
        .filter(|(mode, _, _)| automatic(mode))
        .take(2)
        .count()
        > 1;
    for (mode, channelavg, timeavg) in configs {
        if !(channelavg || timeavg) {
            continue;
        }
        let (timeavg, channelavg) = (u8::from(timeavg), u8::from(channelavg));
        if !automatic(mode) {
            return Err(FlaggingError::CalibrationAgentConfiguration(format!(
                "Cannot use timeavg or channelavg in mode {mode}. timeavg={timeavg}, channelavg={channelavg}. These averaging options are only allowed for auto-flagging methods. Refusing this configuration."
            )));
        }
        if let Some(other) = incompatible {
            return Err(FlaggingError::CalibrationAgentConfiguration(format!(
                "Cannot use {mode} mode with timeavg=True or channelavg=True and additional modes other than extend, display, and antint. timeavg={timeavg}, channelavg={channelavg}, and the following mode has been set up: {other}. Refusing to accept this configuration."
            )));
        }
        if multiple_automatic && !shared_averaging {
            return Err(FlaggingError::CalibrationAgentConfiguration(
                "Averaging with multiple automatic flagging agents requires a shared averaged visibility iterator (#978).".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn run_ms_single(
    ms: &mut MeasurementSet,
    request: &FlagDataRequest,
    persist: bool,
    model_factory: Option<FlagDataModelFactory>,
) -> Result<FlagDataReport, FlaggingError> {
    let ms_path = display_ms_path(ms);
    tracing::info!(
        path = %ms_path,
        mode = ?request.mode,
        action = ?request.action,
        "flagdata started"
    );
    request.check_cancelled()?;
    // The CASA controller saves the working flags before selection. Typed
    // callers keep their existing selection-first backup behavior.
    let early_backup = persist
        && request.flagbackup
        && request.casa_row_selection
        && request.mode != FlagDataMode::Summary;
    let mut backup_version = early_backup
        .then(|| save_next_casa_flag_backup(ms, "flagdata"))
        .transpose()?;
    request.check_cancelled()?;
    let time_selection = FlagTimeSelection::Global;
    let selection = selected_rows(ms, request, time_selection)?;
    let selected = selection.rows;
    if selected.is_empty() {
        tracing::warn!(path = %ms_path, mode = ?request.mode, "flagdata selected no rows");
    }
    let channel_selections = flagging_channels(ms, request, selection.channel_ranges)?;
    let correlations = resolve_correlations(ms, request)?;
    if !request.display_column_is_scalar {
        return Err(FlaggingError::DisplayColumnVector);
    }
    if request.mode == FlagDataMode::Summary {
        return summary::report(
            ms,
            &selected,
            channel_selections.as_ref(),
            correlations.as_ref(),
            request.summary_options,
            request.cancellation.as_ref(),
        );
    }
    if persist && request.flagbackup && !early_backup && !selected.is_empty() {
        request.check_cancelled()?;
        backup_version = Some(save_next_flag_backup(ms, "flagdata")?);
    }
    let selection = AgentSelection {
        rows: selected,
        channels: channel_selections,
        correlations,
        averaged_time: None,
        averaged_uv: None,
    };
    let changes =
        stage_selected_ms_request(ms, &selection, request, !persist, model_factory, None)?;
    request.check_cancelled()?;
    let mut report_cancellation = request.cancellation.as_ref();
    if persist && !changes.changed_row_indices.is_empty() {
        persist_changed_flags(ms, &changes, &ms_path, report_cancellation)?;
        report_cancellation = None;
    }
    let report = report_after(
        ms,
        request,
        &selection.rows,
        changes,
        backup_version,
        time_selection,
        report_cancellation,
    )?;
    tracing::info!(
        path = %ms_path,
        mode = ?report.mode,
        selected_rows = report.selected_rows,
        changed_rows = report.changed_rows,
        changed_samples = report.changed_samples,
        "flagdata completed"
    );
    Ok(report)
}

/// Execute a sequence of MeasurementSet flagging agents on one opened table view.
/// Earlier agents' staged edits are visible to later summary and propagation
/// passes. CASA's antenna-integration prepass reads the original input flags.
/// When `write_flags` is `false`, staged flags are discarded without mutating
/// the MeasurementSet on disk; when `true`, changed rows are written once at
/// the end. When `local_selection` is `true` (multiple commands in CASA's
/// `mode='list'`), an agent whose local selection fails is skipped (`None`)
/// rather than aborting the list; single-command runs propagate selection errors.
/// Use the same cancellation token for the program's requests.
pub fn flagdata_ms_program(
    mut ms: MeasurementSet,
    requests: &[FlagDataRequest],
    write_flags: bool,
    local_selection: bool,
    model_factory: Option<FlagDataModelFactory>,
) -> Result<Vec<Option<FlagDataReport>>, FlaggingError> {
    let cancellation = requests
        .iter()
        .find_map(|request| request.cancellation.as_ref());
    check_cancelled(cancellation)?;
    if is_calibration_table(&ms) {
        return Err(FlaggingError::MutateFlags {
            path: display_ms_path(&ms),
            reason: "MeasurementSet flag commands cannot run on a calibration table".to_owned(),
        });
    }
    let ms_path = display_ms_path(&ms);
    let time_selection = if local_selection {
        let has_timerange = requests.iter().any(|request| {
            request.casa_row_selection
                && request.selection.selectdata
                && request
                    .selection
                    .timerange
                    .as_deref()
                    .is_some_and(|text| !text.is_empty() && !text.starts_with('\0'))
        });
        let initial_default_row = if has_timerange {
            first_unflagged_time_row(&ms).map_err(|source| FlaggingError::MutateFlags {
                path: ms_path.clone(),
                reason: format!("select rows: {source}"),
            })?
        } else {
            0
        };
        FlagTimeSelection::Agent {
            initial_default_row,
        }
    } else {
        FlagTimeSelection::GlobalAndAgent
    };
    let mut shared = if let Some(config) = SharedAveraging::from_requests(requests.iter()) {
        let time_chunk = requests
            .iter()
            .filter_map(|request| request.time_chunk)
            .max_by(f64::total_cmp);
        let combinescans = requests.iter().any(|request| request.combinescans);
        Some(SharedFlagView::new(
            &ms,
            config,
            time_chunk,
            combinescans,
            &ms_path,
            cancellation,
        )?)
    } else {
        None
    };
    // Scan the supplied FLAG view before any list agent stages changes.
    // Keep row indices for later application, never copies of FLAG arrays.
    let antint_plans: Vec<_> = requests
        .iter()
        .enumerate()
        .map(|(index, request)| {
            check_cancelled(cancellation)?;
            Ok((shared.is_none()
                && index > 0
                && local_selection
                && request.casa_row_selection
                && request.mode == FlagDataMode::Antint)
                .then(|| plan_antint(&ms, request, &ms_path, time_selection)))
        })
        .collect::<Result<_, FlaggingError>>()?;
    let mut reports = Vec::with_capacity(requests.len());
    let mut touched = BTreeSet::new();
    let mut changed_flag_row_rows = 0usize;
    for (request, antint_plan) in requests.iter().zip(antint_plans) {
        check_cancelled(cancellation)?;
        match stage_ms_program_request(
            &mut ms,
            request,
            ProgramAgentContext {
                local_selection,
                calculate: !write_flags,
                model_factory,
                time_selection,
            },
            antint_plan,
            shared.as_mut(),
        ) {
            Ok(Some((report, changed_rows, changed_flag_rows))) => {
                touched.extend(changed_rows);
                changed_flag_row_rows += changed_flag_rows;
                reports.push(Some(report));
            }
            Ok(None)
            | Err(
                FlaggingError::MissingClipColumn
                | FlaggingError::MissingTfcropColumn
                | FlaggingError::MissingRflagColumn
                | FlaggingError::MissingCorrelationSummary,
            ) => reports.push(None),
            Err(error) => return Err(error),
        }
    }
    check_cancelled(cancellation)?;
    if let Some(view) = shared
        && write_flags
    {
        let mut changes = view.flush(&mut ms)?;
        touched.extend(std::mem::take(&mut changes.changed_row_indices));
        changed_flag_row_rows += changes.changed_flag_row_rows;
    }
    check_cancelled(cancellation)?;
    if write_flags && !touched.is_empty() {
        let changes = ChangeSet {
            changed_row_indices: touched.into_iter().collect(),
            changed_flag_row_rows,
            ..ChangeSet::default()
        };
        persist_changed_flags(&mut ms, &changes, &ms_path, cancellation)?;
    }
    Ok(reports)
}

fn plan_antint(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
    path: &str,
    time_selection: FlagTimeSelection,
) -> Result<Option<AntintPlan>, FlaggingError> {
    let Some(selection) = select_agent(ms, request, true, time_selection, false)? else {
        return Ok(None);
    };
    if selection
        .correlations
        .as_ref()
        .is_some_and(|selection| !selection.has_active_products())
    {
        return Ok(None);
    }
    let affected = antint::affected_rows(
        ms,
        &selection.rows,
        &request.antint,
        path,
        request.cancellation.as_ref(),
    )?;
    Ok(Some(AntintPlan {
        selection,
        affected,
    }))
}

fn select_agent(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
    local_selection: bool,
    time_selection: FlagTimeSelection,
    shared_time: bool,
) -> Result<Option<AgentSelection>, FlaggingError> {
    let averages_time = shared_time
        || request.clip_time_bin.is_some()
            && matches!(
                request.mode,
                FlagDataMode::Clip | FlagDataMode::Tfcrop | FlagDataMode::Rflag
            );
    let mut averaged_time = None;
    let mut averaged_uv = None;
    let mut input = Cow::Borrowed(request);
    if request.casa_row_selection
        && averages_time
        && request.selection.selectdata
        && request
            .selection
            .timerange
            .as_deref()
            .is_some_and(|text| !text.is_empty() && !text.starts_with('\0'))
        && let FlagTimeSelection::Agent {
            initial_default_row,
        } = time_selection
    {
        match compile_flagdata_agent_time(ms, &request.selection, initial_default_row) {
            Ok(filter) => averaged_time = filter,
            Err(_) if local_selection => return Ok(None),
            Err(source) => {
                return Err(FlaggingError::MutateFlags {
                    path: display_ms_path(ms),
                    reason: format!("select rows: {source}"),
                });
            }
        }
        // The local agent filters the output TIME of the averaging iterator.
        // Every contributing integration must still reach the averaging step.
        input.to_mut().selection.timerange = None;
    }
    if shared_time
        && request.selection.selectdata
        && let Some(text) = request
            .selection
            .uvrange
            .as_deref()
            .map(flagdata_c_string)
            .filter(|text| !text.is_empty())
    {
        match UvDistanceSelection::parse(ms, text) {
            Ok(filter) => averaged_uv = Some(filter),
            Err(_) if local_selection => return Ok(None),
            Err(source) => {
                return Err(FlaggingError::MutateFlags {
                    path: display_ms_path(ms),
                    reason: format!("select rows: {source}"),
                });
            }
        }
        input.to_mut().selection.uvrange = None;
    }
    let input_time_selection = if averages_time {
        FlagTimeSelection::Global
    } else {
        time_selection
    };
    let selection = match selected_rows(ms, &input, input_time_selection) {
        Ok(selection) => selection,
        Err(_) if local_selection => return Ok(None),
        Err(error) => return Err(error),
    };
    let channels = match flagging_channels(ms, request, selection.channel_ranges) {
        Ok(channels) => channels,
        Err(_) if local_selection => return Ok(None),
        Err(error) => return Err(error),
    };
    let correlations = resolve_correlations(ms, request)?;
    if !request.display_column_is_scalar {
        return Err(FlaggingError::DisplayColumnVector);
    }
    Ok(Some(AgentSelection {
        rows: selection.rows,
        channels,
        correlations,
        averaged_time,
        averaged_uv,
    }))
}

struct ProgramAgentContext {
    local_selection: bool,
    calculate: bool,
    model_factory: Option<FlagDataModelFactory>,
    time_selection: FlagTimeSelection,
}

fn stage_ms_program_request(
    ms: &mut MeasurementSet,
    request: &FlagDataRequest,
    context: ProgramAgentContext,
    antint_plan: Option<Result<Option<AntintPlan>, FlaggingError>>,
    mut shared: Option<&mut SharedFlagView>,
) -> Result<Option<(FlagDataReport, Vec<usize>, usize)>, FlaggingError> {
    let ProgramAgentContext {
        local_selection,
        calculate,
        model_factory,
        time_selection,
    } = context;
    let (selection, affected) = if let Some(plan) = antint_plan {
        let Some(plan) = plan? else {
            return Ok(None);
        };
        (plan.selection, Some(plan.affected))
    } else {
        let Some(selection) = select_agent(
            ms,
            request,
            local_selection,
            time_selection,
            shared.as_ref().is_some_and(|view| view.averaged_in_time()),
        )?
        else {
            return Ok(None);
        };
        (selection, None)
    };
    if request.mode == FlagDataMode::Summary {
        return summary::report(
            ms,
            &selection.rows,
            selection.channels.as_ref(),
            selection.correlations.as_ref(),
            request.summary_options,
            request.cancellation.as_ref(),
        )
        .map(|report| Some((report, Vec::new(), 0)));
    }
    if selection
        .correlations
        .as_ref()
        .is_some_and(|selection| !selection.has_active_products())
    {
        return Ok(None);
    }
    let mut changes = if let Some(affected) = affected {
        antint::apply_affected(
            ms,
            affected,
            selection.channels.as_ref(),
            selection.correlations.as_ref(),
            request.cancellation.as_ref(),
        )?
    } else {
        stage_selected_ms_request(
            ms,
            &selection,
            request,
            calculate,
            model_factory,
            shared.as_deref_mut(),
        )?
    };
    if changes.agent_skipped {
        return Ok(None);
    }
    if let Some(view) = shared {
        if request.extendflags && matches!(request.mode, FlagDataMode::Tfcrop | FlagDataMode::Rflag)
        {
            let extension = view.extend_implicit(&display_ms_path(ms))?;
            changes.changed_samples += extension.changed_samples;
            changes
                .changed_row_indices
                .extend(extension.changed_row_indices);
            changes.changed_row_indices.sort_unstable();
            changes.changed_row_indices.dedup();
            changes.changed_rows = changes.changed_row_indices.len();
        }
        let (selected_rows, summary) = view.summarize(selection.shared());
        changes.selected_rows = Some(selected_rows);
        changes.summary = Some(summary);
    }
    let changed_rows = std::mem::take(&mut changes.changed_row_indices);
    let changed_flag_rows = changes.changed_flag_row_rows;
    let report = report_after(
        ms,
        request,
        &selection.rows,
        changes,
        None,
        time_selection,
        request.cancellation.as_ref(),
    )?;
    Ok(Some((report, changed_rows, changed_flag_rows)))
}

fn resolve_correlations(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
) -> Result<Option<CorrelationSelection>, FlaggingError> {
    let correlation = request
        .correlation
        .as_deref()
        .filter(|expression| !expression.is_empty() && !expression.starts_with('\0'))
        .or_else(|| (request.mode == FlagDataMode::Clip).then_some(""));
    let auto_correlations = matches!(request.mode, FlagDataMode::Tfcrop | FlagDataMode::Rflag)
        && request.casa_row_selection;
    match correlation.filter(|_| !auto_correlations) {
        Some(expression) => {
            CorrelationSelection::read(ms, request.mode, request.data_column, expression)
        }
        None => Ok(None),
    }
}

fn select_autocorrelations(
    ms: &MeasurementSet,
    rows: &[usize],
    path: &str,
) -> Result<Vec<usize>, FlaggingError> {
    let main = ms.main_table();
    let first = load_i32_cells(main, rows, "ANTENNA1", path)?;
    let second = load_i32_cells(main, rows, "ANTENNA2", path)?;
    let autocorrelations: Vec<_> = rows
        .iter()
        .copied()
        .zip(first)
        .zip(second)
        .filter_map(|((row, first), second)| (first == second).then_some(row))
        .collect();
    let Some(processor) = ms
        .subtable(SubtableId::Processor)
        .filter(|table| table.row_count() > 0 && !autocorrelations.is_empty())
    else {
        return Ok(autocorrelations);
    };
    let types = processor
        .column_accessor("TYPE")
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|error| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: error.to_string(),
        })?;
    let processor_ids = load_i32_cells(main, &autocorrelations, "PROCESSOR_ID", path)?;
    Ok(autocorrelations
        .into_iter()
        .zip(processor_ids)
        .filter_map(|(row, id)| {
            let index = usize::try_from(id).ok()?;
            matches!(types.get(index), Some(Some(ScalarValue::String(kind))) if kind == "CORRELATOR")
                .then_some(row)
        })
        .collect())
}

fn stage_selected_ms_request(
    ms: &mut MeasurementSet,
    selection: &AgentSelection,
    request: &FlagDataRequest,
    calculate: bool,
    model_factory: Option<FlagDataModelFactory>,
    shared: Option<&mut SharedFlagView>,
) -> Result<ChangeSet, FlaggingError> {
    request.check_cancelled()?;
    let selected_rows = &selection.rows;
    let channel_selections = selection.channels.as_ref();
    let correlations = selection.correlations.as_ref();
    let mut averaging = AveragedAgentInput {
        time: selection.averaged_time.as_ref(),
        uv: selection.averaged_uv.as_ref(),
        view: shared,
    };
    let ms_path = display_ms_path(ms);
    // The manual agent filters baselines after full MS selection, including
    // the rows that supply the default date for a partial time selector.
    let autocorrelation_rows = if request.autocorr
        && request.mode == FlagDataMode::Manual
        && request.action == FlagDataAction::Flag
    {
        Some(select_autocorrelations(ms, selected_rows, &ms_path)?)
    } else {
        None
    };
    if let Some(view) = averaging.view.as_deref_mut() {
        match request.mode {
            FlagDataMode::Extend => {
                return view.extend(selection.shared(), request, &ms_path);
            }
            FlagDataMode::Antint => {
                return view.antint(ms, selection.shared(), request, &ms_path);
            }
            _ => {}
        }
    }
    match request.mode {
        FlagDataMode::Manual => set_selected_flags(
            ms,
            autocorrelation_rows.as_deref().unwrap_or(selected_rows),
            channel_selections,
            correlations,
            request.action.value(),
            selected_flag_row_policy(correlations, SelectedFlagRowPolicy::ChangedSamples),
            request.cancellation.as_ref(),
        ),
        FlagDataMode::Clip => apply_clip_flags(
            ms,
            selected_rows,
            channel_selections,
            correlations,
            request,
            model_factory,
            averaging,
        ),
        FlagDataMode::Quack => {
            apply_quack_flags(ms, selected_rows, channel_selections, correlations, request)
        }
        FlagDataMode::Elevation => elevation::apply(
            ms,
            selected_rows,
            request.elevation_limits_deg,
            channel_selections,
            correlations,
            &ms_path,
            request.cancellation.as_ref(),
        ),
        FlagDataMode::Shadow => shadow::apply(
            ms,
            selected_rows,
            &request.shadow,
            channel_selections,
            correlations,
            &ms_path,
            request.cancellation.as_ref(),
        ),
        FlagDataMode::Antint => antint::apply(
            ms,
            selected_rows,
            &request.antint,
            channel_selections,
            correlations,
            &ms_path,
            request.cancellation.as_ref(),
        ),
        FlagDataMode::Tfcrop => apply_tfcrop_flags(
            ms,
            selected_rows,
            channel_selections,
            request,
            model_factory,
            averaging,
        ),
        FlagDataMode::Rflag => apply_rflag_flags(
            ms,
            selected_rows,
            channel_selections,
            request,
            calculate,
            model_factory,
            averaging,
        ),
        FlagDataMode::Extend => extend::apply(
            ms,
            selected_rows,
            channel_selections,
            correlations,
            request,
            &ms_path,
        ),
        FlagDataMode::Summary => Ok(ChangeSet::default()),
    }
}
