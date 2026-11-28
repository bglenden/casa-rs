// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagdata` - native CASA-style MeasurementSet flagging, on the shared task
//! front end. It takes CASA 6.7.6's values and returns what CASA's task
//! returns (#931): the summary dictionary in summary mode, `{}` otherwise.
//! The CASA values it does not implement yet are refused naming #978.

use std::path::Path;

use crate::ms::{
    flagdata_empty_record, flagdata_named_summary_record, flagdata_task_schema_bundle,
    ms_task_success, task_selection_text,
};
use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::{
    CasaFloat, CasaValue, DefaultSpec, FloatDtype, NumpyScalarType, ParameterValue,
    TaskFailureKind, python_repr,
};
use casa_core::quanta::Quantity;
use casa_flagging::{
    AntennaIntegrationOptions, RFlagThresholdReport, ShadowOptions, flagdata_path_with_model,
};
use casa_flagging::{
    FlagDataAction, FlagDataColumn, FlagDataMode, FlagDataReport, FlagDataRequest,
    FlagSummaryOptions, FlaggingError, QuackMode, append_flag_command, save_next_casa_flag_backup,
};
use casa_ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_mstransform::{TransformTaskHistory, append_task_history};

#[path = "automatic.rs"]
mod automatic;
#[path = "command.rs"]
mod command;
#[path = "command_values.rs"]
mod command_values;
#[path = "list.rs"]
mod list;
#[path = "python_literal.rs"]
mod python_literal;
#[path = "rflag.rs"]
mod rflag;
#[path = "shadow.rs"]
mod shadow;
#[path = "tfcrop.rs"]
mod tfcrop;

const STRING_SELECTORS: [&str; 9] = [
    "field",
    "spw",
    "array",
    "feed",
    "scan",
    "antenna",
    "uvrange",
    "timerange",
    "intent",
];

/// Front end of the `flagdata` binary.
pub fn flagdata_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "flagdata",
        flagdata_task_schema_bundle(),
        &["flagdata"],
        run,
    )
    .expect("the flagdata contract carries its surface")
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let action = task.required_text("action")?;
    let mode = task.required_text("mode")?;
    if mode == "list" {
        return list::run(task, &action);
    }
    let automatic_calibration = automatic::table_kind(task, &mode)?;
    tfcrop::integer_setting(task, "winsize")?;
    if mode == "antint" {
        antint_min_channel_fraction(task)?;
    }
    let vector_selector = selector_argument(task)?;
    let column = flagdata_column_argument(task)?;
    let time_chunk = time_chunk_argument(task).or_else(|error| {
        if error.kind == TaskFailureKind::Execution {
            backup_before_failed_apply(task, &action)?;
        }
        Err(error)
    })?;
    let display_option = task.required_text("display")?;
    if mode == "tfcrop" {
        tfcrop::validate_types(task)?;
    }
    if column.is_none() && matches!(mode.as_str(), "clip" | "tfcrop" | "rflag") {
        backup_before_failed_apply(task, &action)?;
        return Err(
            TaskFailure::execution("'list' object has no attribute 'upper'").at("datacolumn"),
        );
    }
    check_correlation_argument(task, &action)?;
    let save_parameters = task.flag("savepars")? && mode != "summary";
    let channel_bins = channel_bin_widths(task)?;
    let shadow = (mode == "shadow")
        .then(|| shadow::parameters(task))
        .transpose()
        .or_else(|error| {
            if error.kind == TaskFailureKind::Execution {
                backup_before_failed_apply(task, &action)?;
            }
            Err(error)
        })?;
    let shadow_command = shadow
        .as_ref()
        .and_then(|params| params.command_argument.as_deref());
    let calculate = match action.as_str() {
        "apply" => false,
        "calculate" => true,
        "" | "none" => {
            if save_parameters {
                let path = task.path("vis")?;
                MeasurementSet::open(&path).map_err(|_| {
                    TaskFailure::validation(format!(
                        "Cannot open MeasurementSet {}",
                        path.display()
                    ))
                    .at("vis")
                })?;
                save_flag_parameters(
                    task,
                    &path,
                    &mode,
                    &action,
                    time_chunk,
                    shadow_command,
                    None,
                )?;
            } else {
                tracing::warn!("Parameter action='' is only meaningful with savepars=True.");
            }
            return ms_task_success(flagdata_empty_record(), &None::<FlagDataReport>);
        }
        _ => {
            return Err(unknown(
                "action",
                &action,
                "'apply', 'calculate', 'none' or ''",
            ));
        }
    };
    if let Some(selector) = vector_selector {
        backup_before_failed_apply(task, &action)?;
        return Err(
            TaskFailure::execution(format!("argument _{selector} must be a string")).at(selector),
        );
    }
    if matches!(mode.as_str(), "tfcrop" | "rflag") {
        tfcrop::validate_agent_record(task, &mode).or_else(|error| {
            backup_before_failed_apply(task, &action)?;
            Err(error)
        })?;
    }
    let path = task.path("vis")?;
    let calibration = automatic_calibration.unwrap_or_else(|| is_calibration_table(&path));
    let (mut request, threshold_inputs) = prepare_flagdata_request(
        task,
        &mode,
        &action,
        column.unwrap_or_default(),
        channel_bins,
        calibration,
        time_chunk,
    )?;
    if let Some(parameters) = &shadow {
        request.shadow = parameters.options.clone();
    }
    request.display_column_is_scalar = display_option.is_empty() || column.is_some();
    if matches!(display_option.as_str(), "data" | "report" | "both") {
        tracing::warn!(display = %display_option, "running flagdata with its headless provider");
    }
    if save_parameters && !display_option.is_empty() {
        tracing::warn!("Disabling savepars for the display");
    }
    // The CASA task derives persistence from action, including when the
    // caller supplied its hidden writeflags argument explicitly.
    let report = flagdata_path_with_model(
        &path,
        &request,
        !calculate,
        Some(casa_calibration::models::flagdata_model),
    )
    .map_err(|error| match error {
        FlaggingError::Interrupted => TaskFailure::interrupted(error.to_string()),
        FlaggingError::MissingCorrelationSummary => {
            TaskFailure::execution(error.to_string()).at("correlation")
        }
        FlaggingError::CalibrationSelection(_) => {
            TaskFailure::execution(error.to_string()).at("calibration-selection")
        }
        FlaggingError::UnsupportedCalibrationAgent(_) | FlaggingError::CalibrationAntint => {
            TaskFailure::execution(error.to_string()).at("calibration-agent")
        }
        FlaggingError::MissingClipColumn
        | FlaggingError::MissingTfcropColumn
        | FlaggingError::MissingRflagColumn => {
            TaskFailure::validation(error.to_string()).at("datacolumn")
        }
        FlaggingError::InvalidRFlagScale(parameter) => {
            TaskFailure::validation(error.to_string()).at(parameter)
        }
        FlaggingError::InvalidAutomaticAverageVector(parameter) => {
            TaskFailure::execution(error.to_string()).at(parameter)
        }
        FlaggingError::UnknownTfcropProduct(_) => {
            TaskFailure::execution(error.to_string()).at("correlation")
        }
        FlaggingError::DisplayColumnVector => {
            TaskFailure::execution(error.to_string()).at("datacolumn")
        }
        FlaggingError::InvalidClipChannelBins => {
            TaskFailure::execution(error.to_string()).at("chanbin")
        }
        error => TaskFailure::execution(error.to_string()),
    })?;
    if calculate && let Some(report) = &report.rflag_report {
        rflag::write_files(task, report)?;
    }
    if save_parameters && display_option.is_empty() {
        save_flag_parameters(
            task,
            &path,
            &mode,
            &action,
            time_chunk,
            shadow_command,
            Some((&threshold_inputs, report.rflag_report.as_ref())),
        )?;
    }
    if !calculate
        && !calibration
        && request.mode != FlagDataMode::Summary
        && let Err(error) =
            append_task_history(&path, &flagdata_history(task, &path, &display_option))
    {
        tracing::error!(%error, "updating flagdata HISTORY");
    }
    let value = if let Some(summary) = &report.summary {
        flagdata_named_summary_record(summary, &task.required_text("name")?)
    } else if calculate && let Some(report) = &report.rflag_report {
        rflag_result(&report.timedev, &report.freqdev)
    } else {
        flagdata_empty_record()
    };
    ms_task_success(value, &Some(report))
}

fn prepare_flagdata_request(
    task: &TaskInvocation<'_>,
    mode: &str,
    action: &str,
    column: &str,
    channel_bins: Vec<u32>,
    calibration: bool,
    time_chunk: Option<f64>,
) -> Result<(FlagDataRequest, rflag::ThresholdInputs), TaskFailure> {
    let calculate = action == "calculate";
    let mut threshold_inputs = rflag::ThresholdInputs::default();
    let mut request = (|| {
        if mode == "rflag" {
            threshold_inputs = rflag::ThresholdInputs::read(task, calculate)?;
        }
        flagdata_request(task, column, channel_bins, calibration, &threshold_inputs)
    })()
    .or_else(|error| {
        let parameter = error.parameter.as_deref();
        if matches!(parameter, Some("datacolumn" | "field" | "spw"))
            || (matches!(
                parameter,
                Some(
                    "channelavg"
                        | "timeavg"
                        | "rflag-threshold-file"
                        | "rflag-threshold-path.is-directory"
                        | "timedev"
                        | "freqdev"
                )
            ) && error.kind == TaskFailureKind::Execution)
        {
            backup_before_failed_apply(task, action)?;
        }
        Err(error)
    })?;
    request.time_chunk = time_chunk.filter(|seconds| *seconds > 0.0 && seconds.is_finite());
    if !calculate && request.mode == FlagDataMode::Rflag {
        for (name, threshold, scale) in [
            ("timedevscale", request.timedev, request.timedevscale),
            ("freqdevscale", request.freqdev, request.freqdevscale),
        ] {
            if threshold.is_some_and(|value| value > 0.0) && scale <= 0.0 {
                return Err(TaskFailure::validation(format!(
                    "{name} must be positive when its threshold is positive"
                ))
                .at(name));
            }
        }
    }
    Ok((request, threshold_inputs))
}

fn rflag_result(time: &[[f64; 3]], frequency: &[[f64; 3]]) -> CasaValue {
    if time.is_empty() && frequency.is_empty() {
        return CasaValue::dict([
            ("type", CasaValue::string("list")),
            ("nreport", CasaValue::Int { value: 0 }),
        ]);
    }
    let array = |rows: &[[f64; 3]]| CasaValue::FloatArray {
        dtype: FloatDtype::Float64,
        shape: vec![rows.len(), 3],
        data: (0..3)
            .flat_map(|column| rows.iter().map(move |row| CasaFloat(row[column])))
            .collect(),
    };
    let report = CasaValue::dict([
        ("type", CasaValue::string("rflag")),
        ("name", CasaValue::string("Rflag")),
        ("timedev", array(time)),
        ("freqdev", array(frequency)),
    ]);
    CasaValue::dict([
        ("type", CasaValue::string("list")),
        ("nreport", CasaValue::Int { value: 1 }),
        ("report0", report),
    ])
}

fn save_flag_parameters(
    task: &TaskInvocation<'_>,
    vis: &Path,
    mode: &str,
    action: &str,
    time_chunk: Option<f64>,
    shadow_command: Option<&str>,
    rflag_run: Option<(&rflag::ThresholdInputs, Option<&RFlagThresholdReport>)>,
) -> Result<(), TaskFailure> {
    let outfile = task.required_file_text("outfile")?;
    if outfile.is_empty() && is_calibration_table(vis) {
        if matches!(action, "" | "none") {
            return Err(TaskFailure::execution(format!(
                "Error opening FLAG_CMD table {}",
                vis.join("FLAG_CMD").display(),
            ))
            .at("calibration-save-command"));
        }
        return Ok(());
    }
    let mut arguments = Vec::new();
    for name in STRING_SELECTORS {
        if let Some(text) = task.argument_text(name) {
            arguments.push(format!("{name}={text}"));
            continue;
        }
        if let Some(ParameterValue::Array(values)) = task.session().given(name) {
            let values = values
                .iter()
                .map(|value| match value {
                    ParameterValue::String(text) => selector_string_repr(text),
                    _ => unreachable!("selector types were validated"),
                })
                .collect::<Vec<_>>();
            arguments.push(format!("{name}=[{}]", values.join(", ")));
        } else {
            let text = task_selection_text(task, name)?;
            push_command_argument(&mut arguments, name, &ParameterValue::String(text));
        }
    }
    if let Some(observation) = observation_text(task)? {
        push_command_argument(
            &mut arguments,
            "observation",
            &ParameterValue::String(observation),
        );
    }
    let mode = match mode {
        "" | "manualflag" => "manual",
        mode => mode,
    };
    let fields: &[&str] = match mode {
        "manual" => &["autocorr"],
        "unflag" => &[],
        "clip" => &[
            "datacolumn",
            "clipoutside",
            "channelavg",
            "chanbin",
            "timeavg",
            "timebin",
            "clipzeros",
        ],
        "quack" => &["quackmode", "quackinterval", "quackincrement"],
        "elevation" => &["lowerlimit", "upperlimit"],
        "extend" => &[
            "ntime",
            "combinescans",
            "extendpols",
            "growtime",
            "growfreq",
            "growaround",
            "flagneartime",
            "flagnearfreq",
        ],
        "shadow" => &["tolerance", "addantenna"],
        "antint" => &["antint_ref_antenna", "minchanfrac", "verbose"],
        "tfcrop" => &[
            "ntime",
            "combinescans",
            "datacolumn",
            "timecutoff",
            "freqcutoff",
            "timefit",
            "freqfit",
            "maxnpieces",
            "flagdimension",
            "usewindowstats",
            "halfwin",
            "extendflags",
            "channelavg",
            "chanbin",
            "timeavg",
            "timebin",
        ],
        "rflag" => rflag::SAVED_FIELDS,
        _ => return Err(not_yet("savepars", mode)),
    };
    let (rflag_inputs, thresholds) = rflag_run.map_or((None, None), |(inputs, report)| {
        (
            Some(inputs),
            report.filter(|report| !report.timedev.is_empty() || !report.freqdev.is_empty()),
        )
    });
    let mut append_thresholds = Vec::new();
    for &field in fields {
        if mode == "rflag" && matches!(field, "timedev" | "freqdev") {
            if let Some((value, append)) =
                rflag::saved_threshold(task.value(field), field, action, rflag_inputs, thresholds)
            {
                if append {
                    push_command_argument(&mut append_thresholds, field, &value);
                } else {
                    push_command_argument(&mut arguments, field, &value);
                }
            }
        } else if field == "writeflags" {
            if thresholds.is_none() {
                push_command_argument(
                    &mut arguments,
                    field,
                    &ParameterValue::Bool(action == "apply"),
                );
            }
        } else if mode == "rflag" && field == "ntime" && time_chunk.unwrap_or_default() == 0.0 {
            continue;
        } else if field == "addantenna" {
            if let Some(text) = shadow_command {
                arguments.push(format!("addantenna={text}"));
            }
        } else if let Some(text) = task.argument_text(field) {
            arguments.push(format!("{field}={text}"));
        } else if field == "datacolumn" {
            let column = task.required_text(field)?.to_ascii_uppercase();
            push_command_argument(&mut arguments, field, &ParameterValue::String(column));
        } else if field == "ntime" {
            push_command_argument(
                &mut arguments,
                field,
                &ParameterValue::Float(time_chunk.unwrap_or(0.0)),
            );
        } else {
            let value = if field == "chanbin" {
                task.session()
                    .given(field)
                    .unwrap_or(&ParameterValue::Integer(1))
            } else {
                task.value(field)
                    .expect("flagdata has a literal default for the agent parameter")
            };
            let value = match (field, value) {
                ("extendpols", ParameterValue::ScalarArray(value)) => value,
                _ => value,
            };
            push_command_argument(&mut arguments, field, value);
        }
    }
    if mode == "clip" {
        if let Some(text) = task.argument_text("clipminmax") {
            arguments.push(format!("clipminmax={text}"));
        } else if let [minimum, maximum] = task.numbers("clipminmax")?.as_slice() {
            push_command_argument(
                &mut arguments,
                "clipminmax",
                &ParameterValue::Array(vec![
                    ParameterValue::Float(*minimum),
                    ParameterValue::Float(*maximum),
                ]),
            );
        }
    }
    push_command_argument(
        &mut arguments,
        "mode",
        &ParameterValue::String(mode.to_owned()),
    );
    let correlation = task_selection_text(task, "correlation")?.to_ascii_uppercase();
    push_command_argument(
        &mut arguments,
        "correlation",
        &ParameterValue::String(correlation),
    );
    arguments.extend(append_thresholds);

    let reason = task.required_text("cmdreason")?;
    if outfile.is_empty() {
        return append_flag_command(vis, &arguments.join(" "), &reason, action == "apply")
            .map_err(|error| TaskFailure::execution(error.to_string()).at("outfile"));
    }
    push_command_argument(&mut arguments, "reason", &ParameterValue::String(reason));
    let content = arguments.join(" ");
    command::write_file(
        &outfile,
        &content,
        task.flag("overwrite")?,
        matches!(action, "none" | ""),
    )
}

fn is_calibration_table(path: &Path) -> bool {
    std::fs::read_to_string(path.join("table.info"))
        .is_ok_and(|info| casa_tables::TableInfo::parse(&info).table_type == "Calibration")
}

fn push_command_argument(arguments: &mut Vec<String>, name: &str, value: &ParameterValue) {
    let text = match value {
        ParameterValue::String(text) if text.is_empty() => return,
        ParameterValue::String(text) => format!("'{}'", text.trim_matches('\'')),
        ParameterValue::NumpyScalar { value, .. } => python_repr(value),
        value => python_repr(value),
    };
    arguments.push(format!("{name}={text}"));
}

fn selector_string_repr(text: &str) -> String {
    use std::fmt::Write as _;

    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut result = String::from(quote);
    for character in text.chars() {
        match character {
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\\' => result.push_str("\\\\"),
            character if character == quote => {
                result.push('\\');
                result.push(character);
            }
            character if character.is_control() && u32::from(character) <= 0xff => {
                write!(result, "\\x{:02x}", u32::from(character)).expect("write to String");
            }
            character => result.push(character),
        }
    }
    result.push(quote);
    result
}

fn flagdata_history(task: &TaskInvocation<'_>, path: &Path, display: &str) -> TransformTaskHistory {
    let rflag = matches!(task.value("mode"), Some(ParameterValue::String(mode)) if mode == "rflag");
    let wrap_list_inpfile = matches!(task.value("mode"), Some(ParameterValue::String(mode)) if mode == "list")
        && list::tbuff_active(task);
    // CASA's display agent can change these locals before writing HISTORY.
    let float_display = !display.is_empty()
        && matches!(task.value("datacolumn"), Some(ParameterValue::String(column)) if column.eq_ignore_ascii_case("DATA"))
        && MeasurementSet::open(path).is_ok_and(|ms| {
            ms.main_table()
                .schema()
                .is_some_and(|schema| schema.contains_column("FLOAT_DATA"))
        });
    let mut bindings = task
        .session()
        .bundle()
        .surface
        .bindings()
        .iter()
        .filter(|binding| binding.projections.python.is_some())
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let parameters = bindings
        .into_iter()
        .filter_map(|binding| {
            let name = binding.name.as_str();
            let default = match &binding.default {
                DefaultSpec::Literal { value } => Some(value),
                DefaultSpec::Conditional { fallback, .. } => Some(fallback),
                DefaultSpec::Required => None,
            };
            let given = task.session().given(name);
            let value = if matches!(
                name,
                "vis" | "spw" | "chanbin" | "timedev" | "freqdev" | "outfile"
            ) {
                given.or(default).or_else(|| task.value(name))
            } else {
                task.value(name).or(default)
            }?;
            let text = match (name, value) {
                ("writeflags", _) => "True".to_string(),
                ("flagbackup", _) => "False".to_string(),
                ("savepars", _) if !display.is_empty() => "False".to_string(),
                ("datacolumn", _) if float_display => "\"FLOAT_DATA\"".to_string(),
                ("datacolumn", ParameterValue::Array(_)) => task
                    .argument_text("datacolumn")
                    .map_or_else(|| python_repr(value), str::to_owned),
                ("addantenna", ParameterValue::Table(_)) => task
                    .argument_text("addantenna")
                    .map_or_else(|| python_repr(value), str::to_owned),
                ("maxnpieces" | "halfwin" | "winsize", _) => task
                    .argument_text(name)
                    .map_or_else(|| python_repr(value), str::to_owned),
                ("spw", ParameterValue::String(value)) if given.is_none() && value == "none" => {
                    "\"\"".to_string()
                }
                ("timedev" | "freqdev", ParameterValue::String(value))
                    if rflag && value.is_empty() =>
                {
                    "[]".to_string()
                }
                (
                    "chanbin",
                    ParameterValue::NumpyScalar { value, .. } | ParameterValue::ScalarArray(value),
                ) => python_repr(value),
                (
                    "extendpols" | "combinescans" | "growaround" | "flagneartime" | "flagnearfreq"
                    | "verbose",
                    value,
                ) => task
                    .argument_text(name)
                    .map_or_else(|| python_repr(value), str::to_owned),
                ("vis", ParameterValue::Array(paths))
                    if let [ParameterValue::String(path)] = paths.as_slice() =>
                {
                    format!("\"{path}\"")
                }
                ("mode", ParameterValue::String(value)) if value == "manualflag" => {
                    "\"manual\"".to_string()
                }
                ("inpfile", ParameterValue::String(value)) if wrap_list_inpfile => {
                    format!("[{}]", selector_string_repr(value))
                }
                ("inpfile", ParameterValue::Array(items)) => format!(
                    "[{}]",
                    items
                        .iter()
                        .map(|item| match item {
                            ParameterValue::String(text) => selector_string_repr(text),
                            _ => python_repr(item),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                ("ntime", ParameterValue::String(value)) => {
                    format!("\"{}\"", value.split('\0').next().unwrap_or_default())
                }
                (_, ParameterValue::String(value)) => format!("\"{value}\""),
                (_, value) => python_repr(value),
            };
            Some((binding.name.clone(), text))
        })
        .collect();
    TransformTaskHistory {
        task: "flagdata".to_string(),
        parameters,
    }
}

fn check_correlation_argument(task: &TaskInvocation<'_>, action: &str) -> Result<(), TaskFailure> {
    match task.value("correlation") {
        None | Some(ParameterValue::String(_)) => Ok(()),
        Some(ParameterValue::Array(items))
            if items
                .iter()
                .all(|item| matches!(item, ParameterValue::String(_))) =>
        {
            // CASA accepts a string vector at parameter validation. A default
            // apply saves the original flags before calling .upper() on it.
            backup_before_failed_apply(task, action)?;
            Err(TaskFailure::execution("'list' object has no attribute 'upper'").at("correlation"))
        }
        _ => Err(
            TaskFailure::validation("correlation must be a string or string vector")
                .at("correlation"),
        ),
    }
}

fn selector_argument(task: &TaskInvocation<'_>) -> Result<Option<&'static str>, TaskFailure> {
    let mut first_vector = None;
    for name in STRING_SELECTORS {
        match task.value(name) {
            None | Some(ParameterValue::String(_)) => {}
            Some(ParameterValue::Array(items))
                if items
                    .iter()
                    .all(|item| matches!(item, ParameterValue::String(_))) =>
            {
                first_vector.get_or_insert(name);
            }
            _ => {
                return Err(TaskFailure::validation(format!(
                    "{name} must be a string or string vector"
                ))
                .at(name));
            }
        }
    }
    Ok(first_vector)
}

fn time_chunk_argument(task: &TaskInvocation<'_>) -> Result<Option<f64>, TaskFailure> {
    let seconds = match task.value("ntime") {
        None => return Ok(None),
        Some(ParameterValue::String(text)) if text == "scan" => return Ok(None),
        Some(ParameterValue::String(text)) => {
            let text = text.split('\0').next().unwrap_or_default();
            let quantity = Quantity::read(text).map_err(|_| {
                TaskFailure::execution(format!(
                    "Error in QuantumHolder::fromString with input string \"{text}\": Illegal input units or format\n in converting quantity"
                ))
                .at("ntime")
            })?;
            let seconds = match quantity.unit().name() {
                "" | "s" => quantity.value(),
                "min" => quantity.get_si_value(),
                _ => {
                    tracing::warn!("Cannot convert units of ntime. Will use default 0.0s");
                    return Ok(None);
                }
            };
            if seconds < 0.0 {
                tracing::warn!(%seconds, "Provided time interval is negative");
            }
            return Ok(Some(seconds));
        }
        Some(ParameterValue::Float(value)) => *value,
        Some(ParameterValue::Integer(value)) => *value as f64,
        Some(ParameterValue::Bool(value)) => f64::from(u8::from(*value)),
        _ => return Err(TaskFailure::validation("ntime must be a number or string").at("ntime")),
    };
    if seconds <= 0.0 {
        return Err(TaskFailure::execution("Parameter ntime cannot be < = 0").at("ntime"));
    }
    if seconds.is_nan() {
        tracing::warn!("Provided time interval is negative: nan");
    }
    Ok(Some(seconds))
}

fn backup_before_failed_apply(task: &TaskInvocation<'_>, action: &str) -> Result<(), TaskFailure> {
    if action == "apply" && task.required_text("mode")? != "summary" && task.flag("flagbackup")? {
        let ms = MeasurementSet::open(task.path("vis")?)
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
        save_next_casa_flag_backup(&ms, "flagdata")
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
    }
    Ok(())
}

fn flagdata_column_argument<'a>(
    task: &'a TaskInvocation<'_>,
) -> Result<Option<&'a str>, TaskFailure> {
    match task.value("datacolumn") {
        Some(ParameterValue::String(column)) => Ok(Some(column)),
        Some(ParameterValue::Array(items))
            if items
                .iter()
                .all(|item| matches!(item, ParameterValue::String(_))) =>
        {
            Ok(None)
        }
        _ => Err(
            TaskFailure::validation("datacolumn must be a string or string vector")
                .at("datacolumn"),
        ),
    }
}

/// The resolved run as a [`FlagDataRequest`]; the parameters the surface
/// does not carry keep their defaults.
fn flagdata_request(
    task: &TaskInvocation<'_>,
    column: &str,
    channel_bins: Vec<u32>,
    calibration: bool,
    thresholds: &rflag::ThresholdInputs,
) -> Result<FlagDataRequest, TaskFailure> {
    let (mode, action) = mode(&task.required_text("mode")?)?;
    let (automatic_enabled, automatic_average_vector) =
        automatic::controls(task, mode, calibration)?;
    let spw = task_selection_text(task, "spw")?;
    let correlation = task_selection_text(task, "correlation")?;
    let clipminmax = match task.numbers("clipminmax")?.as_slice() {
        [minimum, maximum] => Some([*minimum, *maximum]),
        _ => None,
    };
    let clipoutside = match task.value("clipoutside") {
        Some(ParameterValue::Bool(outside)) => Some(*outside),
        Some(ParameterValue::Array(items))
            if items
                .iter()
                .all(|item| matches!(item, ParameterValue::Bool(_))) =>
        {
            None
        }
        _ => {
            return Err(
                TaskFailure::validation("clipoutside must be a Boolean or Boolean vector")
                    .at("clipoutside"),
            );
        }
    };
    let (quackinterval, quackmode, quackincrement) = quack_options(task)?;
    if mode == FlagDataMode::Quack && quackmode == QuackMode::Disabled {
        tracing::warn!("quack mode and interval require supported scalar options");
    }
    let extendpols = match task.value("extendpols") {
        Some(ParameterValue::Bool(value)) => Some(*value),
        _ => None,
    };
    let (timedev, freqdev) = (&thresholds.time, &thresholds.frequency);
    Ok(FlagDataRequest {
        cancellation: task.cancellation().cloned(),
        selection: selection(task, &spw)?,
        casa_row_selection: true,
        spw: (!spw.is_empty()).then_some(spw),
        correlation: (!correlation.is_empty()).then_some(correlation),
        mode,
        action,
        autocorr: task.flag("autocorr")?,
        data_column: data_column(column, mode, calibration)?,
        display_column_is_scalar: true,
        flagbackup: task.flag("flagbackup")?,
        summary_options: FlagSummaryOptions {
            spwchan: task.flag("spwchan")?,
            spwcorr: task.flag("spwcorr")?,
            basecnt: task.flag("basecnt")?,
            fieldcnt: task.flag("fieldcnt")?,
            minrel: task.number("minrel")?,
            maxrel: task.number("maxrel")?,
            minabs: task.integer("minabs")?,
            maxabs: task.integer("maxabs")?,
        },
        clipzeros: task.flag("clipzeros")?,
        clipminmax,
        clipoutside,
        clip_channel_bins: if calibration && mode != FlagDataMode::Clip {
            None
        } else {
            channel_averaging_bins(task, mode, channel_bins)?
        },
        clip_time_bin: if calibration && mode != FlagDataMode::Clip {
            None
        } else {
            time_averaging_bin(task, mode)?
        },
        time_chunk: None,
        combinescans: task.flag("combinescans")?,
        quackinterval,
        quackmode,
        quackincrement,
        elevation_limits_deg: [task.number("lowerlimit")?, task.number("upperlimit")?],
        shadow: ShadowOptions::default(),
        antint: AntennaIntegrationOptions {
            reference_antenna: task.required_text("antint_ref_antenna")?,
            min_channel_fraction: antint_min_channel_fraction(task)?,
            verbose: task.flag("verbose")?,
        },
        timecutoff: task.number("timecutoff")?,
        freqcutoff: task.number("freqcutoff")?,
        tfcrop: if mode == FlagDataMode::Tfcrop {
            if automatic_enabled {
                tfcrop::parameters(task)?
            } else {
                None
            }
        } else {
            Some(casa_flagging::TfcropOptions::default())
        },
        rflag_calculate: None,
        timedevscale: task.number("timedevscale")?,
        freqdevscale: task.number("freqdevscale")?,
        spectralmax: task.number("spectralmax")?,
        spectralmin: task.number("spectralmin")?,
        rflag_window: if timedev.invalid || freqdev.invalid || !automatic_enabled {
            None
        } else if mode == FlagDataMode::Rflag {
            let width = match task.value("winsize") {
                // The task's agent record uses its default for NumPy integers.
                Some(ParameterValue::NumpyScalar {
                    dtype: NumpyScalarType::Int32 | NumpyScalarType::Int64,
                    ..
                }) => Some(3),
                _ => tfcrop::integer_setting(task, "winsize")?,
            };
            width
                .map(|width| {
                    usize::try_from(width)
                        .ok()
                        .filter(|&value| value > 0)
                        .ok_or_else(|| {
                            TaskFailure::validation("winsize must be positive").at("winsize")
                        })
                })
                .transpose()?
        } else {
            Some(3)
        },
        automatic_average_vector,
        timedev: timedev.scalar,
        freqdev: freqdev.scalar,
        timedev_by_field_spw: timedev.by_field_spw.clone(),
        freqdev_by_field_spw: freqdev.by_field_spw.clone(),
        extendflags: task.flag("extendflags")?,
        extendpols: extendpols.unwrap_or(false),
        extension_enabled: extendpols.is_some(),
        growaround: task.flag("growaround")?,
        flagneartime: task.flag("flagneartime")?,
        flagnearfreq: task.flag("flagnearfreq")?,
        growtime: task.number("growtime")?,
        growfreq: task.number("growfreq")?,
    })
}

fn antint_min_channel_fraction(task: &TaskInvocation<'_>) -> Result<f64, TaskFailure> {
    if let Some(ParameterValue::NumpyScalar { value, .. }) = task.value("minchanfrac") {
        match value.as_ref() {
            ParameterValue::Float(value) => return Ok(*value),
            ParameterValue::Integer(value) => return Ok(*value as f64),
            _ => {}
        }
    }
    task.number("minchanfrac")
}

fn time_averaging_bin(
    task: &TaskInvocation<'_>,
    mode: FlagDataMode,
) -> Result<Option<String>, TaskFailure> {
    if !matches!(
        mode,
        FlagDataMode::Clip | FlagDataMode::Tfcrop | FlagDataMode::Rflag
    ) {
        return Ok(None);
    }
    match automatic::average_switch(task, "timeavg")? {
        Some(false) => Ok(None),
        Some(true) => task.required_text("timebin").map(Some),
        None => {
            Err(TaskFailure::execution("RecordInterface::asBool - invalid data type").at("timeavg"))
        }
    }
}

fn channel_averaging_bins(
    task: &TaskInvocation<'_>,
    mode: FlagDataMode,
    channel_bins: Vec<u32>,
) -> Result<Option<Vec<u32>>, TaskFailure> {
    if !matches!(
        mode,
        FlagDataMode::Clip | FlagDataMode::Tfcrop | FlagDataMode::Rflag
    ) {
        return Ok(None);
    }
    match automatic::average_switch(task, "channelavg")? {
        Some(false) => Ok(None),
        Some(true) => Ok(Some(channel_bins)),
        None => Err(
            TaskFailure::execution("RecordInterface::asBool - invalid data type").at("channelavg"),
        ),
    }
}

fn channel_bin_widths(task: &TaskInvocation<'_>) -> Result<Vec<u32>, TaskFailure> {
    let invalid =
        || TaskFailure::validation("chanbin must be an integer or integer vector").at("chanbin");
    let width = |value: &ParameterValue| match value {
        // Negative widths and oversized positive widths both cover the
        // selected window. Avoid wraparound into smaller, active widths.
        ParameterValue::Integer(value) => Ok(u32::try_from(*value).unwrap_or(u32::MAX)),
        ParameterValue::Bool(value) => Ok(u32::from(*value)),
        _ => Err(invalid()),
    };
    match task.value("chanbin") {
        None | Some(ParameterValue::Bool(_)) => Ok(vec![1]),
        Some(ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Int32 | NumpyScalarType::Int64,
            ..
        }) => Ok(vec![1]),
        Some(ParameterValue::ScalarArray(value))
            if matches!(
                value.as_ref(),
                ParameterValue::Integer(_)
                    | ParameterValue::NumpyScalar {
                        dtype: NumpyScalarType::Int32 | NumpyScalarType::Int64,
                        ..
                    }
            ) =>
        {
            Ok(Vec::new())
        }
        Some(ParameterValue::Array(values))
            if values
                .iter()
                .all(|value| matches!(value, ParameterValue::Bool(_))) =>
        {
            Ok(vec![1])
        }
        Some(ParameterValue::Array(values)) => values.iter().map(width).collect(),
        Some(value) => Ok(vec![width(value)?]),
    }
}

/// The row and channel selection passed to the flagging engine.
fn selection(task: &TaskInvocation<'_>, spw: &str) -> Result<MsSelection, TaskFailure> {
    let field = task_selection_text(task, "field")?;
    if !field.is_empty() && field.trim().is_empty() {
        // The CASA lexer accepts a space token, even when followed by an
        // unrecognized non-ASCII byte. Other whitespace alone is an error.
        let prefix = field
            .split(|character: char| !character.is_ascii())
            .next()
            .unwrap_or_default();
        if prefix.is_empty() || !prefix.bytes().all(|byte| byte == b' ') {
            return Err(TaskFailure::execution(format!(
                "Field Expression: invalid whitespace selector {field:?}"
            ))
            .at("field"));
        }
    }
    let selector = |name| -> Result<Option<String>, TaskFailure> {
        let text = task_selection_text(task, name)?;
        Ok((!text.is_empty()).then_some(text))
    };
    let selection = MsSelection {
        field: (!field.is_empty()).then_some(field),
        spw: (!spw.is_empty()).then(|| spw.to_owned()),
        scan: selector("scan")?,
        antenna: selector("antenna")?,
        timerange: selector("timerange")?,
        observation: observation_text(task)?,
        array: selector("array")?,
        feed: selector("feed")?,
        intent: selector("intent")?,
        uvrange: selector("uvrange")?,
        ..MsSelection::default()
    };
    Ok(selection)
}

fn observation_text(task: &TaskInvocation<'_>) -> Result<Option<String>, TaskFailure> {
    let Some(value) = task.value("observation") else {
        return Ok(None);
    };
    let value = match value {
        ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Int32 | NumpyScalarType::Int64,
            value,
        } => value,
        value => value,
    };
    match value {
        ParameterValue::String(text) => Ok((!text.is_empty()).then(|| text.clone())),
        ParameterValue::Integer(_) | ParameterValue::Bool(_) => Ok(Some(python_repr(value))),
        _ => Err(
            TaskFailure::validation("observation must be a string or integer").at("observation"),
        ),
    }
}

/// CASA's `mode`, with the action its manual flagging takes: `manualflag`
/// is CASA's other spelling of `manual`, and `unflag` unflags what
/// `manual` would flag.
fn mode(text: &str) -> Result<(FlagDataMode, FlagDataAction), TaskFailure> {
    let flag = |mode| Ok((mode, FlagDataAction::Flag));
    match text.to_ascii_lowercase().as_str() {
        "manual" | "manualflag" => flag(FlagDataMode::Manual),
        "unflag" => Ok((FlagDataMode::Manual, FlagDataAction::Unflag)),
        "clip" => flag(FlagDataMode::Clip),
        "quack" => flag(FlagDataMode::Quack),
        "elevation" => flag(FlagDataMode::Elevation),
        "shadow" => flag(FlagDataMode::Shadow),
        "antint" => flag(FlagDataMode::Antint),
        "tfcrop" => flag(FlagDataMode::Tfcrop),
        "rflag" => flag(FlagDataMode::Rflag),
        "extend" => flag(FlagDataMode::Extend),
        "summary" => flag(FlagDataMode::Summary),
        "list" => Err(not_yet("mode", text)),
        _ => Err(unknown(
            "mode",
            text,
            "'manual', 'unflag', 'clip', 'quack', 'tfcrop', 'rflag', 'extend' or 'summary'",
        )),
    }
}

/// The visibility or weight source for the active flagging agent.
fn data_column(
    text: &str,
    mode: FlagDataMode,
    calibration: bool,
) -> Result<FlagDataColumn, TaskFailure> {
    if !matches!(
        mode,
        FlagDataMode::Clip | FlagDataMode::Tfcrop | FlagDataMode::Rflag
    ) {
        return Ok(FlagDataColumn::Data);
    }
    let auto_ms = matches!(mode, FlagDataMode::Tfcrop | FlagDataMode::Rflag) && !calibration;
    let accepts_ms_sources = mode == FlagDataMode::Clip || auto_ms;
    let source = if auto_ms {
        text.split_once('\0').map_or(text, |(head, _)| head)
    } else {
        text
    };
    match source.to_ascii_uppercase().as_str() {
        "DATA" => Ok(FlagDataColumn::Data),
        "CORRECTED" => Ok(FlagDataColumn::CorrectedData),
        "CORRECTED_DATA" if !auto_ms => Ok(FlagDataColumn::CorrectedData),
        "FLOAT_DATA" if accepts_ms_sources => Ok(FlagDataColumn::FloatData),
        "MODEL" if accepts_ms_sources => Ok(FlagDataColumn::ModelData),
        "RESIDUAL" if accepts_ms_sources => Ok(FlagDataColumn::Residual),
        "RESIDUAL_DATA" if accepts_ms_sources => Ok(FlagDataColumn::ResidualData),
        "WEIGHT" if accepts_ms_sources => Ok(FlagDataColumn::Weight),
        "WEIGHT_SPECTRUM" if accepts_ms_sources => Ok(FlagDataColumn::WeightSpectrum),
        "CPARAM" if calibration => Ok(FlagDataColumn::Cparam),
        "FPARAM" if calibration => Ok(FlagDataColumn::Fparam),
        "PARAMERR" if calibration => Ok(FlagDataColumn::ParamErr),
        "SNR" if calibration => Ok(FlagDataColumn::Snr),
        _ if mode == FlagDataMode::Tfcrop && !calibration => Err(TaskFailure::validation(
            "Failed to parse parameters for mode tfcrop",
        )
        .at("datacolumn")),
        _ if mode == FlagDataMode::Rflag && !calibration => Err(TaskFailure::validation(
            "Failed to parse parameters for mode rflag",
        )
        .at("datacolumn")),
        "MODEL" | "RESIDUAL" | "RESIDUAL_DATA" | "FLOAT_DATA" | "WEIGHT_SPECTRUM" | "WEIGHT"
        | "FPARAM" | "CPARAM" | "PARAMERR" | "SNR" => Err(not_yet("datacolumn", text)),
        _ if mode == FlagDataMode::Clip => Err(TaskFailure::validation(
            "Failed to parse parameters for mode clip",
        )
        .at("datacolumn")),
        _ => Err(unknown("datacolumn", text, "'DATA' or 'CORRECTED'")),
    }
}

fn quack_options(task: &TaskInvocation<'_>) -> Result<(f64, QuackMode, bool), TaskFailure> {
    let interval = match task.value("quackinterval") {
        Some(ParameterValue::Float(value)) => Some(*value),
        Some(ParameterValue::Integer(value)) => Some(*value as f64),
        Some(ParameterValue::Bool(value)) => Some(f64::from(u8::from(*value))),
        Some(ParameterValue::Array(items))
            if items.iter().all(|item| {
                matches!(
                    item,
                    ParameterValue::Float(_) | ParameterValue::Integer(_) | ParameterValue::Bool(_)
                )
            }) =>
        {
            None
        }
        _ => {
            return Err(TaskFailure::validation(
                "quackinterval must be a number or numeric vector",
            )
            .at("quackinterval"));
        }
    };
    let increment = match task.value("quackincrement") {
        Some(ParameterValue::Bool(value)) => Some(*value),
        Some(ParameterValue::Array(items))
            if items
                .iter()
                .all(|item| matches!(item, ParameterValue::Bool(_))) =>
        {
            None
        }
        _ => {
            return Err(TaskFailure::validation(
                "quackincrement must be a Boolean or Boolean vector",
            )
            .at("quackincrement"));
        }
    };
    let requested_mode = match task.value("quackmode") {
        Some(ParameterValue::String(text)) => quackmode(text),
        Some(ParameterValue::Array(items))
            if items
                .iter()
                .all(|item| matches!(item, ParameterValue::String(_))) =>
        {
            QuackMode::Disabled
        }
        _ => {
            return Err(
                TaskFailure::validation("quackmode must be a string or string vector")
                    .at("quackmode"),
            );
        }
    };
    // The wrapper admits correctly typed vectors, but the CASA agent
    // requires scalar options before it starts.
    let mode = if interval.is_some() && increment.is_some() {
        requested_mode
    } else {
        QuackMode::Disabled
    };
    Ok((
        interval.unwrap_or_default(),
        mode,
        increment.unwrap_or_default(),
    ))
}

fn quackmode(text: &str) -> QuackMode {
    match text {
        "beg" => QuackMode::Beg,
        "endb" => QuackMode::End,
        "end" => QuackMode::BeforeEnd,
        "tail" => QuackMode::Tail,
        _ => QuackMode::Disabled,
    }
}

/// The refusal of a CASA value casa-rs does not implement yet.
fn not_yet(name: &str, text: &str) -> TaskFailure {
    TaskFailure::validation(format!(
        "{name}={text:?} is not implemented by casa-rs yet (#978)"
    ))
    .at(name)
}

/// The refusal of a value CASA does not take either.
fn unknown(name: &str, text: &str, expected: &str) -> TaskFailure {
    TaskFailure::validation(format!(
        "unsupported {name} {text:?}; CASA's flagdata takes {expected}"
    ))
    .at(name)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use crate::{ParameterRuntime, TaskCommandLine};

    use super::*;

    /// The resolved `flagdata` command line, read by `read`.
    fn read<T>(
        args: &[&str],
        read: fn(&TaskInvocation<'_>) -> Result<T, TaskFailure>,
    ) -> Result<T, TaskFailure> {
        let args = args.iter().map(OsString::from).collect::<Vec<_>>();
        let bundle = casa_contracts::builtin_surface_bundle("flagdata").unwrap();
        let line = TaskCommandLine::parse(&bundle, &args).unwrap();
        let session = ParameterRuntime::default()
            .open_session(line.open_session_request(bundle))
            .unwrap();
        read(&TaskInvocation::new(&session, std::path::Path::new(".")))
    }

    /// The request a `flagdata` command line resolves to.
    fn request_for(args: &[&str]) -> Result<FlagDataRequest, TaskFailure> {
        read(args, |task| {
            let thresholds = if task.required_text("mode")? == "rflag" {
                rflag::ThresholdInputs::read(task, task.required_text("action")? == "calculate")?
            } else {
                rflag::ThresholdInputs::default()
            };
            flagdata_request(
                task,
                flagdata_column_argument(task)?.unwrap_or_default(),
                channel_bin_widths(task)?,
                false,
                &thresholds,
            )
        })
    }

    #[test]
    fn rflag_thresholds_are_read_as_numbers() {
        // Astra's #962 P2: every explicit threshold was refused as not text.
        let request = request_for(&[
            "--vis",
            "in.ms",
            "--mode",
            "rflag",
            "--timedev",
            "1.0",
            "--freqdev",
            "10",
        ])
        .unwrap();
        assert_eq!(request.mode, FlagDataMode::Rflag);
        assert_eq!((request.timedev, request.freqdev), (Some(1.0), Some(10.0)));

        let defaults = request_for(&["--vis", "in.ms", "--mode", "rflag"]).unwrap();
        assert_eq!((defaults.timedev, defaults.freqdev), (None, None));
    }

    #[test]
    fn the_defaults_are_casas() {
        let request = request_for(&["--vis", "in.ms"]).unwrap();
        assert_eq!(
            (request.mode, request.action, request.data_column),
            (
                FlagDataMode::Manual,
                FlagDataAction::Flag,
                FlagDataColumn::Data
            )
        );
        assert_eq!(request.quackinterval, 1.0);
        assert!(request.extendflags && request.extendpols);
        assert_eq!((request.growtime, request.growfreq), (50.0, 50.0));
        assert_eq!((request.timedevscale, request.freqdevscale), (5.0, 5.0));
        assert_eq!(
            read(&["--vis", "in.ms"], |task| task.required_text("action")).unwrap(),
            "apply"
        );
    }

    #[test]
    fn casas_values_are_read_with_casas_meanings() {
        let unflag = request_for(&["--vis", "in.ms", "--mode", "unflag"]).unwrap();
        assert_eq!(
            (unflag.mode, unflag.action),
            (FlagDataMode::Manual, FlagDataAction::Unflag)
        );
        let manualflag = request_for(&["--vis", "in.ms", "--mode", "manualflag"]).unwrap();
        assert_eq!(manualflag.mode, FlagDataMode::Manual);
        let request = request_for(&[
            "--vis",
            "in.ms",
            "--mode",
            "clip",
            "--datacolumn",
            "corrected",
            "--quackmode",
            "endb",
        ])
        .unwrap();
        assert_eq!(request.data_column, FlagDataColumn::CorrectedData);
        assert_eq!(request.quackmode, QuackMode::End);
        // A command line cannot give action=''; tests/flagging.rs runs it.
        assert!(read(&["--vis", "in.ms", "--action", "none"], run).is_ok());
    }

    #[test]
    fn elevation_limits_are_numbers_in_the_cli() {
        let request = request_for(&[
            "--vis",
            "in.ms",
            "--mode",
            "elevation",
            "--lowerlimit",
            "-5.5",
            "--upperlimit",
            "82",
        ])
        .unwrap();
        assert_eq!(request.mode, FlagDataMode::Elevation);
        assert_eq!(request.elevation_limits_deg, [-5.5, 82.0]);
    }

    #[test]
    fn clipoutside_keeps_its_cli_switches_alongside_vector_values() {
        for (switch, expected) in [("--clipoutside", true), ("--no-clipoutside", false)] {
            let request = request_for(&["--vis", "in.ms", "--mode", "clip", switch]).unwrap();
            assert_eq!(request.clipoutside, Some(expected));
        }
    }

    #[test]
    fn scan_edge_options_preserve_cli_switches_and_numeric_intervals() {
        let request = request_for(&[
            "--vis",
            "in.ms",
            "--mode",
            "quack",
            "--quackmode",
            "tail",
            "--quackinterval",
            "2.5",
            "--quackincrement",
        ])
        .unwrap();
        assert_eq!(request.quackmode, QuackMode::Tail);
        assert_eq!(request.quackinterval, 2.5);
        assert!(request.quackincrement);

        let request = request_for(&[
            "--vis",
            "in.ms",
            "--mode",
            "quack",
            "--quackmode",
            "end",
            "--no-quackincrement",
        ])
        .unwrap();
        assert_eq!(request.quackmode, QuackMode::BeforeEnd);
        assert!(!request.quackincrement);

        let error = request_for(&[
            "--vis",
            "in.ms",
            "--mode",
            "quack",
            "--quackinterval",
            "typo",
        ])
        .unwrap_err();
        assert_eq!(error.parameter.as_deref(), Some("quackinterval"));
    }

    #[test]
    fn model_column_resolves_for_rflag() {
        let request =
            request_for(&["--vis", "in.ms", "--mode", "rflag", "--datacolumn", "MODEL"]).unwrap();
        assert_eq!(request.data_column, FlagDataColumn::ModelData);
    }

    #[test]
    fn flagdata_history_preserves_winsize_text() {
        let default_history = read(&["--vis", "in.ms", "--mode", "rflag"], |task| {
            Ok(flagdata_history(task, Path::new("in.ms"), ""))
        })
        .unwrap();
        assert!(
            default_history
                .parameters
                .contains(&("winsize".to_string(), "3".to_string())),
            "{:?}",
            default_history.parameters
        );

        let updated_history = read(
            &["--vis", "in.ms", "--mode", "rflag", "--winsize", "5"],
            |task| Ok(flagdata_history(task, Path::new("in.ms"), "")),
        )
        .unwrap();
        assert!(
            updated_history
                .parameters
                .contains(&("winsize".to_string(), "5".to_string())),
            "{:?}",
            updated_history.parameters
        );
    }

    #[test]
    fn provider_bundle_embeds_flagdata_parameter_contract() {
        let bundle = flagdata_task_schema_bundle();
        assert_eq!(bundle.protocol.protocol_name, "casa_ms_flagdata_task");
        assert_eq!(bundle.parameter_surfaces.len(), 1);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "flagdata");
        bundle.validate().expect("valid flagdata provider");
    }
}
