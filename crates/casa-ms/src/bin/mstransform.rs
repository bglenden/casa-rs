// SPDX-License-Identifier: LGPL-3.0-or-later
//! `mstransform` - tutorial-scoped MeasurementSet transform, the `mstransform`
//! and `split` task provider, on the shared task front end. Its first word
//! names the surface (`mstransform split ...`); `mstransform` runs otherwise.

use std::process::ExitCode;

use casa_ms::selection::MsSelection;
use casa_ms::{
    MsTransformError, MsTransformRequest, TransformDataColumn, ms_task_success, mstransform,
    mstransform_task_schema_bundle, parse_numeric_id_selector, parse_spw_selector,
    task_selection_text,
};
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{
    CasaLoggingConfig, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess,
};

fn main() -> ExitCode {
    TaskFrontEnd::new(
        "mstransform",
        mstransform_task_schema_bundle(),
        &["mstransform", "split"],
        run,
    )
    .expect("the transform contract carries mstransform and split")
    .main(
        std::env::args_os().skip(1),
        CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

/// Both surfaces run the one transform; CASA's tasks return None.
fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = transform_request(task)?;
    // The output the transform refuses before it starts is the argument's
    // fault; anything else failed the transform.
    let report = mstransform(&request).map_err(|error| match error {
        MsTransformError::OutputExists { .. } | MsTransformError::SameInputOutput { .. } => {
            TaskFailure::validation(error.to_string()).at("outputvis")
        }
        other => TaskFailure::execution(other.to_string()),
    })?;
    ms_task_success(CasaValue::None, &report)
}

fn transform_request(task: &TaskInvocation<'_>) -> Result<MsTransformRequest, TaskFailure> {
    let width = channel_width(task)?;
    // The selector's syntax is checked here, before the transform prepares
    // its output; the windows it names are checked against the input there.
    let spw = task_selection_text(task, "spw")?;
    if !spw.trim().is_empty() {
        parse_spw_selector(&spw).map_err(|error| {
            TaskFailure::validation(format!("invalid spw {spw:?}: {error}")).at("spw")
        })?;
    }
    Ok(MsTransformRequest {
        input_ms: task.path("vis")?,
        output_ms: task.path("outputvis")?,
        spw,
        width,
        data_column: data_column(&task.required_text("datacolumn")?)?,
        selection: selection(task)?,
        keep_flags: task.flag("keepflags")?,
    })
}

/// The channels averaged into each output channel: split's `width`, or
/// mstransform's `chanbin` when its `chanaverage` is set and 1 otherwise,
/// as CASA's tasks take them. CASA's mstransform refuses to average by one
/// channel; split's `width=1` averages nothing.
fn channel_width(task: &TaskInvocation<'_>) -> Result<usize, TaskFailure> {
    let (name, least) = if task.surface() == "split" {
        ("width", 1)
    } else if task.flag("chanaverage")? {
        ("chanbin", 2)
    } else {
        return Ok(1);
    };
    let width = task.integer(name)?;
    usize::try_from(width)
        .ok()
        .filter(|width| *width >= least)
        .ok_or_else(|| {
            let message = if least > 1 {
                format!("Parameter {name} must be > 1 to do channel averaging, found {width}")
            } else {
                format!("{name} must be at least 1, found {width}")
            };
            TaskFailure::validation(message).at(name)
        })
}

/// The row selection; the TaQL expression is split's `msselect`, a
/// casa-rs control, and mstransform's `taql`.
fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let ids = |name: &str| -> Result<Option<Vec<i32>>, TaskFailure> {
        let text = task_selection_text(task, name)?;
        if text.is_empty() {
            return Ok(None);
        }
        parse_numeric_id_selector(&text, name)
            .map(Some)
            .map_err(|error| TaskFailure::validation(error.to_string()).at(name))
    };
    let mut selection = MsSelection::default();
    if let Some(field) = ids("field")? {
        selection = selection.field(&field);
    }
    if let Some(scan) = ids("scan")? {
        selection = selection.scan(&scan);
    }
    if let Some(antenna) = ids("antenna")? {
        selection = selection.antenna(&antenna);
    }
    let timerange = task_selection_text(task, "timerange")?;
    if !timerange.is_empty() {
        let (start, end) = time_range(&timerange)
            .map_err(|message| TaskFailure::validation(message).at("timerange"))?;
        selection = selection.time_range(start, end);
    }
    let taql_name = if task.surface() == "split" {
        "msselect"
    } else {
        "taql"
    };
    let taql = task_selection_text(task, taql_name)?;
    if !taql.is_empty() {
        selection = selection.taql(&taql);
    }
    Ok(selection)
}

/// CASA's `datacolumn`, in any case: casa-rs copies `data` and `corrected`
/// (also spelled `corrected_data`); CASA's other choices are #978.
fn data_column(value: &str) -> Result<TransformDataColumn, TaskFailure> {
    let text = value.trim().to_ascii_lowercase();
    match text.as_str() {
        "data" => Ok(TransformDataColumn::Data),
        "corrected" | "corrected_data" => Ok(TransformDataColumn::CorrectedData),
        "model"
        | "data,model,corrected"
        | "float_data"
        | "lag_data"
        | "float_data,data"
        | "lag_data,data"
        | "all" => Err(TaskFailure::validation(format!(
            "datacolumn={value:?} is not implemented by casa-rs yet (#978)"
        ))
        .at("datacolumn")),
        _ => Err(TaskFailure::validation(format!(
            "unsupported datacolumn {value:?}; CASA takes 'corrected', 'data', 'model', \
             'data,model,corrected', 'float_data', 'lag_data', 'float_data,data', \
             'lag_data,data' or 'all'"
        ))
        .at("datacolumn")),
    }
}

fn time_range(value: &str) -> Result<(f64, f64), String> {
    let (start, end) = value
        .split_once('~')
        .ok_or_else(|| format!("timerange must be start~end MJD seconds, got {value:?}"))?;
    let start = start
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("invalid timerange start {start:?}: {error}"))?;
    let end = end
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("invalid timerange end {end:?}: {error}"))?;
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use casa_task_runtime::{ParameterRuntime, TaskCommandLine};

    use super::*;

    /// The request a `surface` command line resolves to.
    fn request_for(surface: &str, args: &[&str]) -> Result<MsTransformRequest, TaskFailure> {
        let args = ["--vis", "in.ms", "--outputvis", "out.ms"]
            .iter()
            .chain(args)
            .map(OsString::from)
            .collect::<Vec<_>>();
        let bundle = casa_provider_contracts::builtin_surface_bundle(surface).unwrap();
        let line = TaskCommandLine::parse(&bundle, &args).unwrap();
        let session = ParameterRuntime::default()
            .open_session(line.open_session_request(bundle))
            .unwrap();
        transform_request(&TaskInvocation::new(&session, std::path::Path::new(".")))
    }

    #[test]
    fn each_task_averages_channels_by_its_own_casa_parameters() {
        let width = |surface, args| request_for(surface, args).unwrap().width;
        assert_eq!(width("split", &["--width", "8"]), 8);
        assert_eq!(width("mstransform", &["--chanbin", "4"]), 1);
        assert_eq!(
            width("mstransform", &["--chanaverage", "--chanbin", "4"]),
            4
        );
        let error = request_for("mstransform", &["--chanaverage", "--chanbin", "0"]).unwrap_err();
        assert_eq!(error.parameter.as_deref(), Some("chanbin"));
        // CASA's mstransform: "Parameter chanbin must be > 1 to do channel
        // averaging"; split's width=1 is no averaging (#997 F8).
        let error = request_for("mstransform", &["--chanaverage"]).unwrap_err();
        assert_eq!(error.parameter.as_deref(), Some("chanbin"));
        assert!(error.message.contains("must be > 1"), "{}", error.message);
        assert_eq!(width("split", &["--width", "1"]), 1);
    }

    #[test]
    fn datacolumn_takes_casas_values_and_default() {
        for surface in ["split", "mstransform"] {
            let request = request_for(surface, &[]).unwrap();
            assert_eq!(request.data_column, TransformDataColumn::CorrectedData);
            let data = request_for(surface, &["--datacolumn", "data"]).unwrap();
            assert_eq!(data.data_column, TransformDataColumn::Data);
            let error = request_for(surface, &["--datacolumn", "model"]).unwrap_err();
            assert!(error.message.contains("#978"), "{error:?}");
        }
    }

    #[test]
    fn provider_bundle_embeds_transform_family_parameter_contracts() {
        let bundle = mstransform_task_schema_bundle();
        assert_eq!(bundle.protocol.protocol_name, "casa_ms_transform_task");
        assert_eq!(
            bundle
                .parameter_surfaces
                .iter()
                .map(|surface| surface.surface.id())
                .collect::<Vec<_>>(),
            ["mstransform", "split"]
        );
        bundle.validate().expect("valid transform provider");
    }
}
