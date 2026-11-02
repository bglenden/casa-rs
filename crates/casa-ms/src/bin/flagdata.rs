// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagdata` - native CASA-style MeasurementSet flagging, on the shared task
//! front end. It takes CASA 6.7.6's values and returns what CASA's task
//! returns (#931): the summary dictionary in summary mode, `{}` otherwise.
//! The CASA values it does not implement yet are refused naming #978.

use std::process::ExitCode;

use casa_ms::selection::MsSelection;
use casa_ms::{
    FlagDataAction, FlagDataColumn, FlagDataMode, FlagDataReport, FlagDataRequest, QuackMode,
    flagdata_empty_record, flagdata_path, flagdata_summary_record, flagdata_task_schema_bundle,
    ms_task_success, parse_numeric_id_selector, task_selection_text,
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
    if !applies(task)? {
        // CASA's action '' or 'none' only saves the parameters, which is
        // savepars (#978); without it CASA warns and returns {}.
        tracing::warn!("Parameter action='' is only meaningful with savepars=True.");
        return ms_task_success(flagdata_empty_record(), &None::<FlagDataReport>);
    }
    let request = flagdata_request(task)?;
    let report = flagdata_path(task.path("vis")?, &request)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let value = report
        .summary
        .as_ref()
        .map_or_else(flagdata_empty_record, flagdata_summary_record);
    ms_task_success(value, &Some(report))
}

/// Whether CASA's `action` runs the flagging: `apply` does, `''` and
/// `none` do not, and `calculate`, which flags without writing, is #978.
fn applies(task: &TaskInvocation<'_>) -> Result<bool, TaskFailure> {
    let text = task.required_text("action")?;
    match text.as_str() {
        "apply" => Ok(true),
        "" | "none" => Ok(false),
        "calculate" => Err(not_yet("action", &text)),
        _ => Err(unknown(
            "action",
            &text,
            "'apply', 'calculate', 'none' or ''",
        )),
    }
}

/// The resolved run as a [`FlagDataRequest`]; the parameters the surface
/// does not carry keep their defaults.
fn flagdata_request(task: &TaskInvocation<'_>) -> Result<FlagDataRequest, TaskFailure> {
    let (mode, action) = mode(&task.required_text("mode")?)?;
    let spw = task_selection_text(task, "spw")?;
    Ok(FlagDataRequest {
        selection: selection(task, &spw)?,
        spw: (!spw.is_empty()).then_some(spw),
        mode,
        action,
        data_column: data_column(&task.required_text("datacolumn")?)?,
        flagbackup: task.flag("flagbackup")?,
        clipzeros: task.flag("clipzeros")?,
        quackinterval: task.number("quackinterval")?,
        quackmode: quackmode(&task.required_text("quackmode")?)?,
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

/// The row selection of `spw`, `field`, `scan` and `antenna`.
fn selection(task: &TaskInvocation<'_>, spw: &str) -> Result<MsSelection, TaskFailure> {
    let mut selection = MsSelection::new();
    if !spw.is_empty() {
        selection = selection
            .spw_selector(spw)
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
    Ok(selection)
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

/// CASA's `datacolumn`, which CASA reads in any case: casa-rs flags on
/// `DATA` and `CORRECTED` (also spelled `CORRECTED_DATA`); CASA's other
/// columns are #978.
fn data_column(text: &str) -> Result<FlagDataColumn, TaskFailure> {
    match text.to_ascii_uppercase().as_str() {
        "DATA" => Ok(FlagDataColumn::Data),
        "CORRECTED" | "CORRECTED_DATA" => Ok(FlagDataColumn::CorrectedData),
        "MODEL" | "RESIDUAL" | "RESIDUAL_DATA" | "FLOAT_DATA" | "WEIGHT_SPECTRUM" | "WEIGHT"
        | "FPARAM" | "CPARAM" | "PARAMERR" | "SNR" => Err(not_yet("datacolumn", text)),
        _ => Err(unknown("datacolumn", text, "'DATA' or 'CORRECTED'")),
    }
}

/// CASA's `quackmode`: `beg` flags the first `quackinterval` of each scan
/// and `endb` the last; `end` and `tail`, which flag all but those, are
/// #978.
fn quackmode(text: &str) -> Result<QuackMode, TaskFailure> {
    match text.to_ascii_lowercase().as_str() {
        "beg" => Ok(QuackMode::Beg),
        "endb" => Ok(QuackMode::End),
        "end" | "tail" => Err(not_yet("quackmode", text)),
        _ => Err(unknown("quackmode", text, "'beg', 'endb', 'end' or 'tail'")),
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
        assert!(read(&["--vis", "in.ms"], applies).unwrap());
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
        assert!(!read(&["--vis", "in.ms", "--action", "none"], applies).unwrap());
    }

    #[test]
    fn casa_values_not_implemented_yet_name_978() {
        for (name, value) in [
            ("--mode", "shadow"),
            ("--mode", "list"),
            ("--datacolumn", "MODEL"),
            ("--quackmode", "tail"),
        ] {
            let error = request_for(&["--vis", "in.ms", name, value]).unwrap_err();
            assert!(error.message.contains("#978"), "{name} {value}: {error:?}");
            assert_eq!(error.parameter.as_deref(), Some(&name[2..]));
        }
        let error = read(&["--vis", "in.ms", "--action", "calculate"], applies).unwrap_err();
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
