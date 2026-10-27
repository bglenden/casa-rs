// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagdata` - native CASA-style MeasurementSet flagging, on the shared task
//! front end.

use std::process::ExitCode;

use casa_ms::selection::MsSelection;
use casa_ms::{
    FlagDataAction, FlagDataColumn, FlagDataMode, FlagDataRequest, QuackMode, flagdata_path,
    flagdata_task_schema_bundle, ms_task_success, parse_numeric_id_selector, task_selection_text,
};
use casa_task_runtime::{
    CasaLoggingConfig, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess,
};

fn main() -> ExitCode {
    TaskFrontEnd::new(
        "flagdata",
        flagdata_task_schema_bundle(),
        &["flagdata"],
        run,
    )
    .expect("the flagdata contract carries its surface")
    .main(
        std::env::args_os().skip(1),
        CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = flagdata_request(task)?;
    let summary = request.mode == FlagDataMode::Summary;
    let report = flagdata_path(task.path("vis")?, &request)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    // CASA's summary mode returns the summary; the other modes return None.
    if summary {
        ms_task_success(&report, &report)
    } else {
        ms_task_success(&(), &report)
    }
}

/// The resolved run as a [`FlagDataRequest`]; the parameters the surface
/// does not carry keep their defaults.
fn flagdata_request(task: &TaskInvocation<'_>) -> Result<FlagDataRequest, TaskFailure> {
    let mut selection = MsSelection::new();
    let spw = task_selection_text(task, "spw")?;
    if !spw.is_empty() {
        selection = selection
            .spw_selector(&spw)
            .map_err(|error| TaskFailure::validation(error.to_string()).at("spw"))?;
    }
    for name in ["field", "scan"] {
        let text = task_selection_text(task, name)?;
        if text.is_empty() {
            continue;
        }
        let ids = parse_numeric_id_selector(&text, name)
            .map_err(|error| TaskFailure::validation(error.to_string()).at(name))?;
        selection = match name {
            "field" => selection.field(&ids),
            _ => selection.scan(&ids),
        };
    }
    let antenna = task_selection_text(task, "antenna")?;
    if !antenna.is_empty() {
        selection = antenna_selection(selection, &antenna);
    }
    Ok(FlagDataRequest {
        selection,
        spw: (!spw.is_empty()).then_some(spw),
        mode: choice(task, "mode", mode)?,
        action: choice(task, "action", action)?,
        data_column: choice(task, "datacolumn", data_column)?,
        flagbackup: task.flag("flagbackup")?,
        clipzeros: task.flag("clipzeros")?,
        quackinterval: task.number("quackinterval")?,
        quackmode: choice(task, "quackmode", quackmode)?,
        timecutoff: task.number("timecutoff")?,
        freqcutoff: task.number("freqcutoff")?,
        timedev: optional_number(task, "timedev")?,
        freqdev: optional_number(task, "freqdev")?,
        extendflags: task.flag("extendflags")?,
        extendpols: task.flag("extendpols")?,
        ..FlagDataRequest::default()
    })
}

/// Antenna ids when every item is one, names otherwise.
fn antenna_selection(selection: MsSelection, value: &str) -> MsSelection {
    let parts = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    match parts
        .iter()
        .map(|part| part.parse::<i32>())
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(ids) => selection.antenna(&ids),
        Err(_) => selection.antenna_name(&parts),
    }
}

/// Read `name` and parse it with `parse`, naming it when it is refused.
fn choice<T>(
    task: &TaskInvocation<'_>,
    name: &str,
    parse: fn(&str) -> Option<T>,
) -> Result<T, TaskFailure> {
    let text = task.required_text(name)?;
    parse(&text.to_ascii_lowercase())
        .ok_or_else(|| TaskFailure::validation(format!("unsupported {name} {text:?}")).at(name))
}

/// A threshold held as text, absent when `none`.
fn optional_number(task: &TaskInvocation<'_>, name: &str) -> Result<Option<f64>, TaskFailure> {
    task.text(name)?
        .map(|text| {
            text.trim().parse::<f64>().map_err(|error| {
                TaskFailure::validation(format!("invalid {name} {text:?}: {error}")).at(name)
            })
        })
        .transpose()
}

fn mode(value: &str) -> Option<FlagDataMode> {
    Some(match value {
        "manual" => FlagDataMode::Manual,
        "clip" => FlagDataMode::Clip,
        "quack" => FlagDataMode::Quack,
        "tfcrop" => FlagDataMode::Tfcrop,
        "rflag" => FlagDataMode::Rflag,
        "extend" => FlagDataMode::Extend,
        "summary" => FlagDataMode::Summary,
        _ => return None,
    })
}

fn action(value: &str) -> Option<FlagDataAction> {
    match value {
        "flag" | "apply" => Some(FlagDataAction::Flag),
        "unflag" => Some(FlagDataAction::Unflag),
        _ => None,
    }
}

fn data_column(value: &str) -> Option<FlagDataColumn> {
    match value {
        "data" => Some(FlagDataColumn::Data),
        "corrected" | "corrected_data" => Some(FlagDataColumn::CorrectedData),
        _ => None,
    }
}

fn quackmode(value: &str) -> Option<QuackMode> {
    match value {
        "beg" => Some(QuackMode::Beg),
        "end" => Some(QuackMode::End),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_bundle_embeds_flagdata_parameter_contract() {
        let bundle = flagdata_task_schema_bundle();
        assert_eq!(bundle.protocol.protocol_name, "casa_ms_flagdata_task");
        assert_eq!(bundle.parameter_surfaces.len(), 1);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "flagdata");
        bundle.validate().expect("valid flagdata provider");
    }
}
