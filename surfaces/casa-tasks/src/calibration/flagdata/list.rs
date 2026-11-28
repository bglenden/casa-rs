// SPDX-License-Identifier: LGPL-3.0-or-later
//! Execute task flag commands against one calibration-table or MeasurementSet view.

use std::collections::BTreeMap;
use std::path::Path;

use crate::ms::{flagdata_empty_record, flagdata_named_summary_record, ms_task_success};
use crate::{ParameterSession, ResolutionPatch, TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::{CasaValue, ParameterValue};
use casa_flagging::{
    FlagDataColumn, FlagDataMode, FlaggingError, flagdata_calibration_program, flagdata_ms_program,
    save_next_casa_flag_backup, validate_calibration_flag_averaging, validate_ms_flag_averaging,
};
use casa_flagging::{FlagDataReport, FlagDataRequest, append_flag_commands};
use casa_ms::MeasurementSet;
use casa_mstransform::append_task_history;

use super::{
    channel_bin_widths, command, command_values::Assignments, data_column,
    flagdata_column_argument, flagdata_history, is_calibration_table, prepare_flagdata_request,
    rflag, shadow, time_chunk_argument, unknown,
};

#[path = "list_input.rs"]
mod input;

pub(crate) use input::tbuff_active;

pub(super) fn run(task: &TaskInvocation<'_>, action: &str) -> Result<TaskSuccess, TaskFailure> {
    if !matches!(action, "apply" | "calculate" | "none" | "") {
        return Err(unknown(
            "action",
            action,
            "'apply', 'calculate', 'none' or ''",
        ));
    }
    let path = task.path("vis")?;
    let cancellation = task.cancellation();
    let check_cancelled = || {
        if cancellation.is_some_and(|token| token.is_cancelled()) {
            return Err(TaskFailure::interrupted("flagdata interrupted"));
        }
        Ok(())
    };
    let calibration = is_calibration_table(&path);
    let write_flags = action == "apply";
    let display_option = task.required_text("display")?;
    let column_argument = flagdata_column_argument(task)?;
    let table = MeasurementSet::open(&path)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("flag-list-agent"))?;
    check_cancelled()?;
    let backup = if write_flags && task.flag("flagbackup")? {
        Some(
            save_next_casa_flag_backup(&table, "flagdata")
                .map_err(|error| TaskFailure::execution(error.to_string()).at("flag-list-agent"))?,
        )
    } else {
        None
    };
    check_cancelled()?;
    let top_time_chunk = time_chunk_argument(task)?;
    let commands = input::read(task)?;
    let save = task.flag("savepars")?;
    if matches!(action, "" | "none") {
        if save {
            save_commands(task, &path, &commands, false, true, calibration)?;
        } else {
            tracing::warn!("Parameter action='' is only meaningful with savepars=True.");
        }
        return ms_task_success(flagdata_empty_record(), &Vec::<FlagDataReport>::new());
    }
    if !calibration && !display_option.is_empty() && column_argument.is_none() {
        return Err(
            TaskFailure::execution("'list' object has no attribute 'upper'").at("datacolumn"),
        );
    }
    if matches!(display_option.as_str(), "data" | "report" | "both") {
        tracing::warn!(display = %display_option, "running flagdata with its headless provider");
    }
    if save && !display_option.is_empty() {
        tracing::warn!("Disabling savepars for the display");
    }
    let mut requests = Vec::new();
    let mut agents = Vec::new();
    let mut configured = Vec::new();
    for (index, values) in commands.iter().enumerate() {
        check_cancelled()?;
        let mode = match values.get("mode") {
            Some(ParameterValue::String(mode)) if !matches!(mode.as_str(), "" | "manualflag") => {
                mode
            }
            _ => "manual",
        };
        let enabled = |name| matches!(values.get(name), Some(ParameterValue::Bool(true)));
        let averaging = (mode, enabled("channelavg"), enabled("timeavg"));
        if mode == "display" {
            let column = match values.get("datacolumn") {
                Some(ParameterValue::String(column)) => column,
                None => "DATA",
                _ => continue,
            };
            if let Ok(column) = data_column(column, FlagDataMode::Clip, calibration)
                && data_column_available(&table, column, calibration)
            {
                configured.push(averaging);
            }
            continue;
        }
        if let Some(mut request) = prepare(task, values, action, calibration, top_time_chunk)? {
            request.cancellation = cancellation.cloned();
            let available = agent_column_available(&table, &request, calibration);
            if available
                && !(calibration
                    && matches!(request.mode, FlagDataMode::Elevation | FlagDataMode::Shadow))
            {
                configured.push(averaging);
            }
            if !available && (!calibration || request.mode == FlagDataMode::Antint) {
                continue;
            }
            requests.push(request);
            agents.push((index, values));
        }
    }
    let validation = if calibration {
        validate_calibration_flag_averaging(configured.into_iter())
    } else {
        validate_ms_flag_averaging(configured.into_iter())
    };
    validation.map_err(|error| TaskFailure::execution(error.to_string()).at("flag-list-agent"))?;
    let incremental_quack =
        |request: &FlagDataRequest| request.mode == FlagDataMode::Quack && request.quackincrement;
    if requests.iter().skip(1).any(incremental_quack) {
        (requests, agents) = requests
            .into_iter()
            .zip(agents)
            .enumerate()
            .filter_map(|(index, (request, agent))| {
                if index > 0 && incremental_quack(&request) {
                    tracing::warn!(
                        "Cannot have quackincrement=True in list mode. Agent quack will be ignored!"
                    );
                    None
                } else {
                    Some((request, agent))
                }
            })
            .unzip();
    }
    check_cancelled()?;
    let program = if calibration {
        flagdata_calibration_program(table, &requests, write_flags, commands.len() > 1)
    } else {
        flagdata_ms_program(
            table,
            &requests,
            write_flags,
            commands.len() > 1,
            Some(casa_calibration::models::flagdata_model),
        )
    };
    let mut reports = program.map_err(|error| map_program_error(error, calibration))?;
    if let Some(report) = reports.iter_mut().flatten().next() {
        report.backup_version = backup;
    }
    if !write_flags {
        for (report, &(index, values)) in reports.iter().zip(&agents) {
            if let Some(thresholds) = report
                .as_ref()
                .and_then(|report| report.rflag_report.as_ref())
            {
                let freqdev = matches!(values.get("timedev"), Some(ParameterValue::Array(_)))
                    .then(|| values.get("freqdev"))
                    .flatten();
                rflag::write_agent_files(
                    task,
                    thresholds,
                    &format!("Rflag_{index}"),
                    index,
                    [None, freqdev],
                )?;
            }
        }
    }
    if save && display_option.is_empty() {
        save_commands(task, &path, &commands, write_flags, false, calibration)?;
    }
    // CASA's task_flagdata.py appends HISTORY on apply before checking
    // summary_stats_list['nreport'], so all-skipped apply calls still log.
    if !calibration
        && write_flags
        && let Err(error) =
            append_task_history(&path, &flagdata_history(task, &path, &display_option))
    {
        tracing::error!(%error, "updating flagdata HISTORY");
    }
    if reports.iter().all(Option::is_none) {
        return Err(TaskFailure::execution("nreport").at("flag-list-no-report"));
    }
    let value = summary_value(&reports, &agents);
    ms_task_success(value, &reports.into_iter().flatten().collect::<Vec<_>>())
}

fn agent_column_available(
    table: &MeasurementSet,
    request: &FlagDataRequest,
    calibration: bool,
) -> bool {
    if !matches!(
        request.mode,
        FlagDataMode::Clip | FlagDataMode::Tfcrop | FlagDataMode::Rflag | FlagDataMode::Antint
    ) {
        return true;
    }
    data_column_available(table, request.data_column, calibration)
}

fn data_column_available(
    table: &MeasurementSet,
    column: FlagDataColumn,
    calibration: bool,
) -> bool {
    let has_column = |name| {
        table
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
    };
    if calibration {
        return matches!(
            column,
            FlagDataColumn::Cparam | FlagDataColumn::Fparam | FlagDataColumn::Snr
        ) && has_column(column.name());
    }
    let has_model = || has_column("MODEL_DATA") || !casa_ms::virtual_model_fields(table).is_empty();
    match column {
        FlagDataColumn::Residual => has_column("CORRECTED_DATA") && has_model(),
        FlagDataColumn::ResidualData => {
            (has_column("DATA") || has_column("FLOAT_DATA")) && has_model()
        }
        FlagDataColumn::ModelData => has_column("MODEL_DATA"),
        FlagDataColumn::Weight => true,
        column => has_column(column.name()),
    }
}

fn map_program_error(error: FlaggingError, calibration: bool) -> TaskFailure {
    match error {
        FlaggingError::Interrupted => TaskFailure::interrupted(error.to_string()),
        FlaggingError::CalibrationAntint => {
            TaskFailure::execution(error.to_string()).at("calibration-agent")
        }
        FlaggingError::InvalidRFlagScale(parameter) if !calibration => {
            TaskFailure::validation(error.to_string()).at(parameter)
        }
        FlaggingError::UnknownTfcropProduct(_) if !calibration => {
            TaskFailure::execution(error.to_string()).at("correlation")
        }
        FlaggingError::InvalidClipChannelBins if !calibration => {
            TaskFailure::execution(error.to_string()).at("chanbin")
        }
        error => TaskFailure::execution(error.to_string()).at("flag-list-agent"),
    }
}

fn summary_value(
    reports: &[Option<FlagDataReport>],
    agents: &[(usize, &Assignments)],
) -> CasaValue {
    let mut summaries = reports
        .iter()
        .zip(agents)
        .filter_map(|(report, &(index, command))| {
            let summary = report.as_ref()?.summary.as_ref()?;
            let name = match command.get("name") {
                Some(ParameterValue::String(name)) => name.clone(),
                _ => format!("Summary_{index}"),
            };
            Some(flagdata_named_summary_record(summary, &name))
        })
        .collect::<Vec<_>>();
    match summaries.len() {
        0 => flagdata_empty_record(),
        1 => summaries.pop().expect("one summary"),
        _ => CasaValue::dict(
            summaries
                .into_iter()
                .enumerate()
                .map(|(index, summary)| (format!("report{index}"), summary)),
        ),
    }
}

fn prepare(
    parent: &TaskInvocation<'_>,
    values: &Assignments,
    action: &str,
    calibration: bool,
    top_time_chunk: Option<f64>,
) -> Result<Option<FlagDataRequest>, TaskFailure> {
    let mode = match values.get("mode") {
        Some(ParameterValue::String(mode)) => mode.as_str(),
        _ => "manual",
    };
    if !matches!(
        mode,
        "" | "manualflag"
            | "manual"
            | "unflag"
            | "clip"
            | "quack"
            | "summary"
            | "extend"
            | "tfcrop"
            | "rflag"
            | "elevation"
            | "shadow"
            | "antint"
    ) {
        return Ok(None);
    }
    let mut values: BTreeMap<_, _> = values
        .iter()
        .filter(|(key, _)| key.as_str() != "reason")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if mode.is_empty() {
        values.insert(
            "mode".to_owned(),
            ParameterValue::String("manual".to_owned()),
        );
    }
    if let Some(ParameterValue::Array(range)) = values.get_mut("clipminmax") {
        if range.len() < 2 {
            return Err(
                TaskFailure::execution("list index out of range").at("flag-list-clip-range")
            );
        }
        range.truncate(2);
    }
    if matches!(values.get("timedev"), Some(ParameterValue::Array(_))) {
        if matches!(values.get("freqdev"), Some(ParameterValue::String(_))) {
            values.remove("freqdev");
        }
    } else {
        for name in ["timedev", "freqdev"] {
            if let Some(value) = values.get_mut(name)
                && !matches!(value, ParameterValue::Array(_))
            {
                *value = rflag::list_threshold_value(parent, value, name)?;
            }
        }
    }
    values.insert(
        "action".to_owned(),
        ParameterValue::String(action.to_owned()),
    );
    values.insert("flagbackup".to_owned(), ParameterValue::Bool(false));
    if let Some(vis) = parent.value("vis") {
        values.insert("vis".to_owned(), vis.clone());
    }
    let mut session = ParameterSession::defaults(parent.session().bundle().clone())
        .map_err(|_| input::error())?;
    session
        .apply_override_patch(ResolutionPatch {
            values,
            ..ResolutionPatch::default()
        })
        .map_err(|_| input::error())?;
    let task = parent.with_session(&session);
    let mode = task.required_text("mode")?;
    let column = flagdata_column_argument(&task)?.unwrap_or_default();
    let cmd_time_chunk = time_chunk_argument(&task)?;
    let effective_time_chunk = if !calibration && mode == "clip" {
        top_time_chunk
    } else {
        match (top_time_chunk, cmd_time_chunk) {
            (Some(top), Some(cmd)) => Some(top.max(cmd)),
            (top, cmd) => cmd.or(top),
        }
    };
    let prepared = prepare_flagdata_request(
        &task,
        &mode,
        action,
        column,
        channel_bin_widths(&task)?,
        calibration,
        effective_time_chunk,
    );
    let (mut request, _) = match prepared {
        Err(error) if error.parameter.as_deref() == Some("datacolumn") => return Ok(None),
        result => result?,
    };
    if mode == "quack" && request.quackmode == casa_flagging::QuackMode::Disabled {
        return Ok(None);
    }
    if mode == "antint" {
        let Ok(column) = data_column(column, FlagDataMode::Clip, calibration) else {
            return Ok(None);
        };
        request.data_column = column;
    }
    if calibration {
        if mode == "tfcrop"
            && matches!(task.session().given("correlation"), Some(ParameterValue::String(value)) if value.is_empty())
        {
            request.correlation = Some("SOL1".to_owned());
        }
    } else if mode == "shadow" {
        request.shadow = shadow::parameters(&task)?.options;
    }
    request.summary_options.minrel = parent.number("minrel")?;
    request.summary_options.maxrel = parent.number("maxrel")?;
    request.summary_options.minabs = parent.integer("minabs")?;
    request.summary_options.maxabs = parent.integer("maxabs")?;
    Ok(Some(request))
}

fn save_commands(
    task: &TaskInvocation<'_>,
    vis: &Path,
    commands: &[Assignments],
    write_flags: bool,
    save_only: bool,
    calibration: bool,
) -> Result<(), TaskFailure> {
    let outfile = task.required_file_text("outfile")?;
    let reason = task.required_text("cmdreason")?;
    if outfile.is_empty() {
        if calibration {
            if save_only {
                return Err(TaskFailure::execution(format!(
                    "Error opening FLAG_CMD table {}",
                    vis.join("FLAG_CMD").display(),
                ))
                .at("calibration-save-command"));
            }
            return Ok(());
        }
        let mut entries = Vec::new();
        for values in commands {
            let mode = match values.get("mode") {
                Some(ParameterValue::String(mode)) => mode.as_str(),
                _ => "",
            };
            if mode == "summary" {
                tracing::warn!(
                    "Commands with mode='summary' are not allowed in the FLAG_CMD table"
                );
                continue;
            }
            let entry_reason = if !reason.is_empty() {
                reason.clone()
            } else {
                match values.get("reason") {
                    Some(ParameterValue::String(value)) => value.clone(),
                    _ => String::new(),
                }
            };
            entries.push((input::command_line_without_reason(values), entry_reason));
        }
        return append_flag_commands(vis, &entries, write_flags)
            .map_err(|error| TaskFailure::execution(error.to_string()).at("outfile"));
    }
    let lines = commands
        .iter()
        .map(|values| input::command_line(values, &reason))
        .collect::<Vec<_>>();
    command::write_file(
        &outfile,
        &lines.join("\n"),
        task.flag("overwrite")?,
        save_only,
    )
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use crate::{ParameterRuntime, TaskCommandLine};

    use super::super::command_values::parse_assignments;
    use super::*;

    fn invoke<T>(
        args: &[&str],
        f: impl FnOnce(&TaskInvocation<'_>) -> Result<T, TaskFailure>,
    ) -> Result<T, TaskFailure> {
        let args = args.iter().map(OsString::from).collect::<Vec<_>>();
        let bundle = casa_contracts::builtin_surface_bundle("flagdata").unwrap();
        let line = TaskCommandLine::parse(&bundle, &args).unwrap();
        let session = ParameterRuntime::default()
            .open_session(line.open_session_request(bundle))
            .unwrap();
        f(&TaskInvocation::new(&session, Path::new(".")))
    }

    #[test]
    fn command_line_renders_with_and_without_reason() {
        let parsed = parse_assignments("mode='manual' spw='0:1' reason='science'").unwrap();
        assert_eq!(
            input::command_line(&parsed, ""),
            "mode='manual' spw='0:1' reason='science'"
        );
        assert_eq!(
            input::command_line(&parsed, "batch"),
            "mode='manual' spw='0:1' reason='batch'"
        );
        assert_eq!(
            input::command_line_without_reason(&parsed),
            "mode='manual' spw='0:1'"
        );
    }

    #[test]
    fn list_history_preserves_tbuff_inpfile_wrapping() {
        let plain = invoke(
            &["--vis", "in.ms", "--mode", "list", "--inpfile", "cmds.txt"],
            |task| Ok(flagdata_history(task, Path::new("in.ms"), "")),
        )
        .unwrap();
        assert!(
            plain
                .parameters
                .contains(&("inpfile".to_string(), "\"cmds.txt\"".to_string())),
            "{:?}",
            plain.parameters
        );

        let wrapped = invoke(
            &[
                "--vis",
                "in.ms",
                "--mode",
                "list",
                "--inpfile",
                "cmds.txt",
                "--tbuff",
                "5",
            ],
            |task| Ok(flagdata_history(task, Path::new("in.ms"), "")),
        )
        .unwrap();
        assert!(
            wrapped
                .parameters
                .contains(&("inpfile".to_string(), "['cmds.txt']".to_string())),
            "{:?}",
            wrapped.parameters
        );
    }

    #[test]
    fn prepare_resolves_ms_shadow_and_ntime_precedence() {
        let commands = [
            parse_assignments("mode='clip' clipminmax=[0.0,1.0]").unwrap(),
            parse_assignments("mode='extend' ntime=60.0").unwrap(),
            parse_assignments("mode='shadow' tolerance=1.5").unwrap(),
            parse_assignments("mode='tfcrop'").unwrap(),
        ];
        let mut prepared = Vec::new();
        invoke(
            &["--vis", "in.ms", "--mode", "list", "--ntime", "20"],
            |task| {
                let top = time_chunk_argument(task)?;
                for cmd in &commands {
                    prepared.push(prepare(task, cmd, "apply", false, top)?.unwrap());
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(prepared[0].time_chunk, Some(20.0));
        assert_eq!(prepared[1].time_chunk, Some(60.0));
        assert_eq!(prepared[2].shadow.tolerance_m, 1.5);
        assert_eq!(prepared[3].correlation, None);
    }
}
