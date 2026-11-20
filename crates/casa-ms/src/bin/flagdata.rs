// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagdata` - native CASA-style MeasurementSet flagging, on the shared task
//! front end. It takes CASA 6.7.6's values and returns what CASA's task
//! returns (#931): the summary dictionary in summary mode, `{}` otherwise.
//! The CASA values it does not implement yet are refused naming #978.

use std::process::ExitCode;

use casa_ms::selection::MsSelection;
use casa_ms::{
    FlagDataAction, FlagDataColumn, FlagDataMode, FlagDataReport, FlagDataRequest,
    FlagSummaryOptions, FlaggingError, MeasurementSet, QuackMode, calculate_flagdata_path,
    flagdata_empty_record, flagdata_named_summary_record, flagdata_path,
    flagdata_task_schema_bundle, ms_task_success, save_next_casa_flag_backup, task_selection_text,
};
use casa_provider_contracts::{NumpyScalarType, ParameterValue, TaskFailureKind, python_repr};
use casa_task_runtime::{
    CasaLoggingConfig, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess,
};

fn main() -> ExitCode {
    TaskFrontEnd::new_with_deferred_schema(
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
    let action = task.required_text("action")?;
    check_correlation_argument(task, &action)?;
    let calculate = match action.as_str() {
        "apply" => false,
        "calculate" => true,
        "" | "none" => {
            // CASA saves parameters for these actions when savepars is set.
            tracing::warn!("Parameter action='' is only meaningful with savepars=True.");
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
    let request = flagdata_request(task).or_else(|error| {
        let parameter = error.parameter.as_deref();
        if matches!(parameter, Some("datacolumn" | "field" | "spw"))
            || (matches!(parameter, Some("channelavg" | "timeavg"))
                && error.kind == TaskFailureKind::Execution)
        {
            backup_before_failed_apply(task, &action)?;
        }
        Err(error)
    })?;
    let path = task.path("vis")?;
    // The CASA task derives persistence from action, including when the
    // caller supplied its hidden writeflags argument explicitly.
    let report = if calculate {
        if !matches!(
            request.mode,
            FlagDataMode::Manual | FlagDataMode::Clip | FlagDataMode::Quack | FlagDataMode::Summary
        ) {
            return Err(not_yet("action", &action));
        }
        calculate_flagdata_path(path, &request)
    } else {
        flagdata_path(path, &request)
    }
    .map_err(|error| match error {
        FlaggingError::MissingCorrelationSummary => {
            TaskFailure::execution(error.to_string()).at("correlation")
        }
        FlaggingError::MissingClipColumn => {
            TaskFailure::validation(error.to_string()).at("datacolumn")
        }
        FlaggingError::InvalidClipChannelBins => {
            TaskFailure::execution(error.to_string()).at("chanbin")
        }
        error => TaskFailure::execution(error.to_string()),
    })?;
    let value = match &report.summary {
        Some(summary) => flagdata_named_summary_record(summary, &task.required_text("name")?),
        None => flagdata_empty_record(),
    };
    ms_task_success(value, &Some(report))
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

fn backup_before_failed_apply(task: &TaskInvocation<'_>, action: &str) -> Result<(), TaskFailure> {
    if action == "apply" && task.required_text("mode")? != "summary" && task.flag("flagbackup")? {
        let ms = MeasurementSet::open(task.path("vis")?)
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
        save_next_casa_flag_backup(&ms, "flagdata")
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
    }
    Ok(())
}

/// The resolved run as a [`FlagDataRequest`]; the parameters the surface
/// does not carry keep their defaults.
fn flagdata_request(task: &TaskInvocation<'_>) -> Result<FlagDataRequest, TaskFailure> {
    let (mode, action) = mode(&task.required_text("mode")?)?;
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
    Ok(FlagDataRequest {
        selection: selection(task, &spw)?,
        casa_row_selection: true,
        spw: (!spw.is_empty()).then_some(spw),
        correlation: (!correlation.is_empty()).then_some(correlation),
        mode,
        action,
        autocorr: task.flag("autocorr")?,
        data_column: data_column(&task.required_text("datacolumn")?, mode)?,
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
        clip_channel_bins: clip_channel_bins(task, mode)?,
        clip_time_bin: clip_time_bin(task, mode)?,
        quackinterval,
        quackmode,
        quackincrement,
        timecutoff: task.number("timecutoff")?,
        freqcutoff: task.number("freqcutoff")?,
        timedevscale: task.number("timedevscale")?,
        freqdevscale: task.number("freqdevscale")?,
        spectralmax: task.number("spectralmax")?,
        spectralmin: task.number("spectralmin")?,
        timedev: task.optional_number("timedev")?,
        freqdev: task.optional_number("freqdev")?,
        extendflags: task.flag("extendflags")?,
        extendpols: task.flag("extendpols")?,
        growtime: task.number("growtime")?,
        growfreq: task.number("growfreq")?,
    })
}

fn clip_time_bin(
    task: &TaskInvocation<'_>,
    mode: FlagDataMode,
) -> Result<Option<String>, TaskFailure> {
    if mode != FlagDataMode::Clip {
        return Ok(None);
    }
    match task.value("timeavg") {
        Some(ParameterValue::Bool(false)) => Ok(None),
        Some(ParameterValue::Bool(true)) => task.required_text("timebin").map(Some),
        Some(ParameterValue::Array(values))
            if values
                .iter()
                .all(|value| matches!(value, ParameterValue::Bool(_))) =>
        {
            Err(TaskFailure::execution("RecordInterface::asBool - invalid data type").at("timeavg"))
        }
        _ => Err(
            TaskFailure::validation("timeavg must be a Boolean or Boolean vector").at("timeavg"),
        ),
    }
}

fn clip_channel_bins(
    task: &TaskInvocation<'_>,
    mode: FlagDataMode,
) -> Result<Option<Vec<u32>>, TaskFailure> {
    if mode != FlagDataMode::Clip {
        return Ok(None);
    }
    match task.value("channelavg") {
        Some(ParameterValue::Bool(false)) => return Ok(None),
        Some(ParameterValue::Bool(true)) => {}
        Some(ParameterValue::Array(values))
            if values
                .iter()
                .all(|value| matches!(value, ParameterValue::Bool(_))) =>
        {
            return Err(
                TaskFailure::execution("RecordInterface::asBool - invalid data type")
                    .at("channelavg"),
            );
        }
        _ => {
            return Err(
                TaskFailure::validation("channelavg must be a Boolean or Boolean vector")
                    .at("channelavg"),
            );
        }
    }
    let width = |value: &ParameterValue| match value {
        // Negative widths and oversized positive widths both cover the
        // selected window. Avoid wraparound into smaller, active widths.
        ParameterValue::Integer(value) => Ok(u32::try_from(*value).unwrap_or(u32::MAX)),
        _ => Err(
            TaskFailure::validation("chanbin must be an integer or integer vector").at("chanbin"),
        ),
    };
    let widths = match task.value("chanbin") {
        Some(ParameterValue::Array(values)) => {
            values.iter().map(width).collect::<Result<_, _>>()?
        }
        Some(value) => vec![width(value)?],
        None => vec![1],
    };
    Ok(Some(widths))
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
        "tfcrop" => flag(FlagDataMode::Tfcrop),
        "rflag" => flag(FlagDataMode::Rflag),
        "extend" => flag(FlagDataMode::Extend),
        "summary" => flag(FlagDataMode::Summary),
        "list" | "shadow" | "elevation" | "antint" => Err(not_yet("mode", text)),
        _ => Err(unknown(
            "mode",
            text,
            "'manual', 'unflag', 'clip', 'quack', 'tfcrop', 'rflag', 'extend' or 'summary'",
        )),
    }
}

/// The visibility or weight source for the active flagging agent.
fn data_column(text: &str, mode: FlagDataMode) -> Result<FlagDataColumn, TaskFailure> {
    match text.to_ascii_uppercase().as_str() {
        "DATA" => Ok(FlagDataColumn::Data),
        "CORRECTED" | "CORRECTED_DATA" => Ok(FlagDataColumn::CorrectedData),
        "FLOAT_DATA" if mode == FlagDataMode::Clip => Ok(FlagDataColumn::FloatData),
        "MODEL" if mode == FlagDataMode::Clip => Ok(FlagDataColumn::ModelData),
        "RESIDUAL" if mode == FlagDataMode::Clip => Ok(FlagDataColumn::Residual),
        "RESIDUAL_DATA" if mode == FlagDataMode::Clip => Ok(FlagDataColumn::ResidualData),
        "WEIGHT" if mode == FlagDataMode::Clip => Ok(FlagDataColumn::Weight),
        "WEIGHT_SPECTRUM" if mode == FlagDataMode::Clip => Ok(FlagDataColumn::WeightSpectrum),
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

    use casa_task_runtime::{ParameterRuntime, TaskCommandLine};

    use super::*;

    /// The resolved `flagdata` command line, read by `read`.
    fn read<T>(
        args: &[&str],
        read: fn(&TaskInvocation<'_>) -> Result<T, TaskFailure>,
    ) -> Result<T, TaskFailure> {
        let args = args.iter().map(OsString::from).collect::<Vec<_>>();
        let bundle = casa_provider_contracts::builtin_surface_bundle("flagdata").unwrap();
        let line = TaskCommandLine::parse(&bundle, &args).unwrap();
        let session = ParameterRuntime::default()
            .open_session(line.open_session_request(bundle))
            .unwrap();
        read(&TaskInvocation::new(&session, std::path::Path::new(".")))
    }

    /// The request a `flagdata` command line resolves to.
    fn request_for(args: &[&str]) -> Result<FlagDataRequest, TaskFailure> {
        read(args, flagdata_request)
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
    fn casa_values_not_implemented_yet_name_978() {
        for (name, value) in [
            ("--mode", "shadow"),
            ("--mode", "list"),
            ("--datacolumn", "MODEL"),
        ] {
            let error = request_for(&["--vis", "in.ms", name, value]).unwrap_err();
            assert!(error.message.contains("#978"), "{name} {value}: {error:?}");
            assert_eq!(error.parameter.as_deref(), Some(&name[2..]));
        }
        let error = read(
            &["--vis", "in.ms", "--mode", "rflag", "--action", "calculate"],
            |task| run(task).map(|_| ()),
        )
        .unwrap_err();
        assert!(error.message.contains("#978"), "{error:?}");
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
