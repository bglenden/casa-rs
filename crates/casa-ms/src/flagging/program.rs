// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared MeasurementSet flagging execution and sequential multi-agent programs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_tables::{Table, TableOptions};
use casa_types::ScalarValue;

use crate::ms::MeasurementSet;
use crate::schema::SubtableId;
use crate::selection::syntax::ChannelSelection;

use super::{
    ChangeSet, CorrelationSelection, FlagDataAction, FlagDataMode, FlagDataModelFactory,
    FlagDataReport, FlagDataRequest, FlaggingError, SelectedFlagRowPolicy, antint,
    apply_clip_flags, apply_quack_flags, apply_rflag_flags, apply_tfcrop_flags, display_ms_path,
    elevation, extend, flagging_channels, is_calibration_table, load_i32_cells,
    persist_changed_flags, report_after, save_next_casa_flag_backup, save_next_flag_backup,
    selected_flag_row_policy, selected_rows, set_selected_flags, shadow, summary,
};

#[derive(Clone, Copy)]
struct AgentStage<'a> {
    calculate: bool,
    model_factory: Option<FlagDataModelFactory>,
    antint_flags: Option<&'a Table>,
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
        if multiple_automatic {
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
    // The CASA controller saves the working flags before selection. Typed
    // callers keep their existing selection-first backup behavior.
    let early_backup = persist
        && request.flagbackup
        && request.casa_row_selection
        && request.mode != FlagDataMode::Summary;
    let mut backup_version = early_backup
        .then(|| save_next_casa_flag_backup(ms, "flagdata"))
        .transpose()?;
    let selection = selected_rows(ms, request)?;
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
        );
    }
    if persist && request.flagbackup && !early_backup && !selected.is_empty() {
        backup_version = Some(save_next_flag_backup(ms, "flagdata")?);
    }
    let changes = stage_selected_ms_request(
        ms,
        &selected,
        channel_selections.as_ref(),
        correlations.as_ref(),
        request,
        AgentStage {
            calculate: !persist,
            model_factory,
            antint_flags: None,
        },
    )?;
    if persist && !changes.changed_row_indices.is_empty() {
        persist_changed_flags(ms, &changes, &ms_path)?;
    }
    let thresholds = changes.thresholds;
    let report = report_after(ms, request, &selected, changes, backup_version, thresholds)?;
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
pub fn flagdata_ms_program(
    mut ms: MeasurementSet,
    requests: &[FlagDataRequest],
    write_flags: bool,
    local_selection: bool,
    model_factory: Option<FlagDataModelFactory>,
) -> Result<Vec<Option<FlagDataReport>>, FlaggingError> {
    if is_calibration_table(&ms) {
        return Err(FlaggingError::MutateFlags {
            path: display_ms_path(&ms),
            reason: "MeasurementSet flag commands cannot run on a calibration table".to_owned(),
        });
    }
    let ms_path = display_ms_path(&ms);
    let original_antint_flags = if local_selection
        && requests
            .iter()
            .skip(1)
            .any(|request| request.casa_row_selection && request.mode == FlagDataMode::Antint)
    {
        ms.path().map(open_source_flags).transpose()?
    } else {
        None
    };
    let stage = AgentStage {
        calculate: !write_flags,
        model_factory,
        antint_flags: original_antint_flags.as_ref(),
    };
    let mut reports = Vec::with_capacity(requests.len());
    let mut touched = BTreeSet::new();
    let mut changed_flag_row_rows = 0usize;
    for request in requests {
        match stage_ms_program_request(&mut ms, request, local_selection, stage) {
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
    drop(original_antint_flags);
    if write_flags && !touched.is_empty() {
        let changes = ChangeSet {
            changed_row_indices: touched.into_iter().collect(),
            changed_flag_row_rows,
            ..ChangeSet::default()
        };
        persist_changed_flags(&mut ms, &changes, &ms_path)?;
    }
    Ok(reports)
}

fn open_source_flags(path: &Path) -> Result<Table, FlaggingError> {
    let options = TableOptions::new(path);
    #[cfg(unix)]
    let source = Table::open_with_lock(
        options,
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait).read_only(true),
    );
    #[cfg(not(unix))]
    let source = Table::open(options);
    source.map_err(|error| FlaggingError::MutateFlags {
        path: path.display().to_string(),
        reason: format!("open original antenna-integration flags: {error}"),
    })
}

fn stage_ms_program_request(
    ms: &mut MeasurementSet,
    request: &FlagDataRequest,
    local_selection: bool,
    stage: AgentStage<'_>,
) -> Result<Option<(FlagDataReport, Vec<usize>, usize)>, FlaggingError> {
    let selection = match selected_rows(ms, request) {
        Ok(selection) => selection,
        Err(_) if local_selection => return Ok(None),
        Err(error) => return Err(error),
    };
    let channel_selections = match flagging_channels(ms, request, selection.channel_ranges) {
        Ok(channels) => channels,
        Err(_) if local_selection => return Ok(None),
        Err(error) => return Err(error),
    };
    let correlations = resolve_correlations(ms, request)?;
    if !request.display_column_is_scalar {
        return Err(FlaggingError::DisplayColumnVector);
    }
    if request.mode == FlagDataMode::Summary {
        return summary::report(
            ms,
            &selection.rows,
            channel_selections.as_ref(),
            correlations.as_ref(),
            request.summary_options,
        )
        .map(|report| Some((report, Vec::new(), 0)));
    }
    if correlations
        .as_ref()
        .is_some_and(|selection| !selection.has_active_products())
    {
        return Ok(None);
    }
    let mut changes = stage_selected_ms_request(
        ms,
        &selection.rows,
        channel_selections.as_ref(),
        correlations.as_ref(),
        request,
        stage,
    )?;
    if changes.agent_skipped {
        return Ok(None);
    }
    let changed_rows = std::mem::take(&mut changes.changed_row_indices);
    let changed_flag_rows = changes.changed_flag_row_rows;
    let thresholds = changes.thresholds;
    let report = report_after(ms, request, &selection.rows, changes, None, thresholds)?;
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
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    request: &FlagDataRequest,
    stage: AgentStage<'_>,
) -> Result<ChangeSet, FlaggingError> {
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
    match request.mode {
        FlagDataMode::Manual => set_selected_flags(
            ms,
            autocorrelation_rows.as_deref().unwrap_or(selected_rows),
            channel_selections,
            correlations,
            request.action.value(),
            selected_flag_row_policy(correlations, SelectedFlagRowPolicy::ChangedSamples),
        ),
        FlagDataMode::Clip => apply_clip_flags(
            ms,
            selected_rows,
            channel_selections,
            correlations,
            request,
            stage.model_factory,
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
        ),
        FlagDataMode::Shadow => shadow::apply(
            ms,
            selected_rows,
            &request.shadow,
            channel_selections,
            correlations,
            &ms_path,
        ),
        FlagDataMode::Antint => antint::apply(
            ms,
            selected_rows,
            &request.antint,
            channel_selections,
            correlations,
            &ms_path,
            stage.antint_flags,
        ),
        FlagDataMode::Tfcrop => apply_tfcrop_flags(
            ms,
            selected_rows,
            channel_selections,
            request,
            stage.model_factory,
        ),
        FlagDataMode::Rflag => apply_rflag_flags(
            ms,
            selected_rows,
            channel_selections,
            request,
            stage.calculate,
            stage.model_factory,
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
