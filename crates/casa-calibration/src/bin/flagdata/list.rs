// SPDX-License-Identifier: LGPL-3.0-or-later
//! Execute task flag commands against a single calibration-table view.

use std::collections::BTreeMap;
use std::path::Path;

use casa_ms::flagging::FlagDataMode;
use casa_ms::flagging::flagdata_calibration_program;
use casa_ms::{
    FlagDataReport, FlagDataRequest, flagdata_empty_record, flagdata_named_summary_record,
    ms_task_success,
};
use casa_provider_contracts::{CasaValue, ParameterValue};
use casa_task_runtime::{
    ParameterSession, ResolutionPatch, TaskFailure, TaskInvocation, TaskSuccess,
};

use super::{
    backup_before_failed_apply, channel_bin_widths, command, command_values::Assignments,
    data_column, flagdata_column_argument, is_calibration_table, not_yet, prepare_flagdata_request,
    rflag, time_chunk_argument,
};

#[path = "list_input.rs"]
mod input;

pub(super) fn run(task: &TaskInvocation<'_>, action: &str) -> Result<TaskSuccess, TaskFailure> {
    let path = task.path("vis")?;
    if !is_calibration_table(&path) {
        return Err(not_yet("mode", "list"));
    }
    let commands = input::read(task)?;
    let save = task.flag("savepars")?;
    if matches!(action, "" | "none") {
        if save {
            save_commands(task, &path, &commands, true)?;
        }
        return ms_task_success(flagdata_empty_record(), &Vec::<FlagDataReport>::new());
    }
    let write_flags = action == "apply";
    let mut requests = Vec::new();
    let mut agents = Vec::new();
    for (index, values) in commands.iter().enumerate() {
        if let Some(request) = prepare(task, values, action).or_else(|error| {
            backup_before_failed_apply(task, action)?;
            Err(error)
        })? {
            requests.push(request);
            agents.push((index, values));
        }
    }
    if requests.is_empty() {
        backup_before_failed_apply(task, action)?;
        return Err(TaskFailure::execution("nreport").at("flag-list-no-report"));
    }
    let reports = flagdata_calibration_program(
        &path,
        &requests,
        write_flags,
        task.flag("flagbackup")?,
        commands.len() > 1,
    )
    .map_err(|error| {
        let parameter = if matches!(error, casa_ms::flagging::FlaggingError::CalibrationAntint) {
            "calibration-agent"
        } else {
            "flag-list-agent"
        };
        TaskFailure::execution(error.to_string()).at(parameter)
    })?;
    if reports.iter().all(Option::is_none) {
        return Err(TaskFailure::execution("nreport").at("flag-list-no-report"));
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
                    thresholds,
                    &format!("Rflag_{index}"),
                    index,
                    [None, freqdev],
                )?;
            }
        }
    }
    if save && task.required_text("display")?.is_empty() {
        save_commands(task, &path, &commands, false)?;
    }
    let value = summary_value(&reports, &agents);
    ms_task_success(value, &reports.into_iter().flatten().collect::<Vec<_>>())
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
                *value = rflag::list_threshold_value(value, name)?;
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
    let task = TaskInvocation::new(&session, parent.workspace());
    let mode = task.required_text("mode")?;
    let column = flagdata_column_argument(&task)?.unwrap_or_default();
    let prepared = prepare_flagdata_request(
        &task,
        &mode,
        action,
        column,
        channel_bin_widths(&task)?,
        true,
        time_chunk_argument(&task)?,
    );
    let (mut request, _) = match prepared {
        Err(error) if error.parameter.as_deref() == Some("datacolumn") => return Ok(None),
        result => result?,
    };
    if mode == "quack" && request.quackmode == casa_ms::flagging::QuackMode::Disabled {
        return Ok(None);
    }
    if mode == "antint" {
        request.data_column = data_column(column, FlagDataMode::Clip, true).unwrap_or_default();
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
    save_only: bool,
) -> Result<(), TaskFailure> {
    let outfile = task.required_text("outfile")?;
    if outfile.is_empty() {
        if save_only {
            return Err(TaskFailure::execution(format!(
                "Error opening FLAG_CMD table {}",
                vis.join("FLAG_CMD").display(),
            ))
            .at("calibration-save-command"));
        }
        return Ok(());
    }
    let reason = task.required_text("cmdreason")?;
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
