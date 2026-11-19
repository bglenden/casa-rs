// SPDX-License-Identifier: LGPL-3.0-or-later
//! `mstransform` - tutorial-scoped MeasurementSet transform, the `mstransform`
//! and `split` task provider, on the shared task front end. Its first word
//! names the surface (`mstransform split ...`); `mstransform` runs otherwise.

use std::process::ExitCode;

use casa_ms::selection::MsSelection;
use casa_ms::{
    MsTransformError, MsTransformRequest, TransformDataColumn, ms_task_success, mstransform_task,
    mstransform_task_schema_bundle, task_selection_text,
};
use casa_provider_contracts::{CasaValue, NumpyScalarType, ParameterValue, python_repr};
use casa_task_runtime::{
    CasaLoggingConfig, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess,
};

fn main() -> ExitCode {
    TaskFrontEnd::new_with_deferred_schema(
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
    let report = mstransform_task(&request).map_err(|error| match error {
        MsTransformError::OutputExists { .. } | MsTransformError::SameInputOutput { .. } => {
            TaskFailure::validation(error.to_string()).at("outputvis")
        }
        MsTransformError::InvalidSpw { .. } => TaskFailure::validation(error.to_string()).at("spw"),
        MsTransformError::MissingDataColumn { path, column } => TaskFailure::execution(format!(
            "Desired column ({column}) not found in the input MS ({path})."
        )),
        other => TaskFailure::execution(other.to_string()),
    })?;
    ms_task_success(CasaValue::None, &report)
}

fn transform_request(task: &TaskInvocation<'_>) -> Result<MsTransformRequest, TaskFailure> {
    Ok(MsTransformRequest {
        input_ms: task.path("vis")?,
        output_ms: task.path("outputvis")?,
        spw: task_selection_text(task, "spw")?,
        width: channel_width(task)?,
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
    let selector = |name: &str| -> Result<Option<String>, TaskFailure> {
        let text = task_selection_text(task, name)?;
        Ok((!text.trim().is_empty()).then_some(text))
    };
    let taql_name = if task.surface() == "split" {
        "msselect"
    } else {
        "taql"
    };
    let mut selection = MsSelection {
        field: selector("field")?,
        scan: selector("scan")?,
        antenna: selector("antenna")?,
        timerange: selector("timerange")?,
        observation: task.value("observation").and_then(variant_selector_text),
        intent: task.text("intent")?.filter(|text| !text.is_empty()),
        correlation: variant_selector(task, "correlation")?,
        array: variant_selector(task, "array")?,
        uvrange: variant_selector(task, "uvrange")?,
        feed: variant_selector(task, "feed")?,
        msselect: selector(taql_name)?,
        ..MsSelection::default()
    };
    // Earlier releases accepted absolute MJD seconds as a numeric pair.
    // CASA reads bare numbers in its clock grammar as seconds within the minute.
    if let Some((start, end)) = selection.timerange.as_deref().and_then(absolute_time_range) {
        selection.timerange = None;
        selection = selection.time_range(start, end);
    }
    Ok(selection)
}

fn variant_selector(task: &TaskInvocation<'_>, name: &str) -> Result<Option<String>, TaskFailure> {
    let Some(value) = task.value(name) else {
        return Ok(None);
    };
    if task.surface() == "split" && matches!(name, "array" | "feed") {
        return Ok(variant_selector_text(value));
    }
    if task.surface() == "split" && name == "correlation" {
        match value {
            ParameterValue::EmptyArray { .. } => return Ok(None),
            ParameterValue::Array(values) => {
                let labels = values
                    .iter()
                    .map(|value| match value {
                        ParameterValue::String(text) => Ok(text.as_str()),
                        _ => Err(TaskFailure::validation("correlation expects strings").at(name)),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let text = labels.join(", ").to_uppercase();
                return Ok((!text.is_empty()).then_some(text));
            }
            _ => {}
        }
    }
    let record_type = match value {
        ParameterValue::String(text) => return Ok((!text.is_empty()).then(|| text.clone())),
        ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Int32 | NumpyScalarType::Int64,
            ..
        } => {
            return Ok(None);
        }
        ParameterValue::Bool(_) => "Bool",
        ParameterValue::Integer(_) => "Int",
        _ => "unknown",
    };
    Err(TaskFailure::execution(format!(
        "RecordRep::get_pointer - incorrect data type String   used for field {name} with type {record_type:<8}"
    )).at(name))
}

fn variant_selector_text(value: &ParameterValue) -> Option<String> {
    match value {
        ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Int32 | NumpyScalarType::Int64,
            value,
        }
        | ParameterValue::ScalarArray(value) => variant_selector_text(value),
        ParameterValue::String(text) => (!text.is_empty()).then(|| text.clone()),
        other => Some(python_repr(other)),
    }
}

/// Resolve CASA's visibility layout, including the existing corrected_data alias.
fn data_column(value: &str) -> Result<TransformDataColumn, TaskFailure> {
    let text = value.trim().to_ascii_lowercase();
    match text.as_str() {
        "data" => Ok(TransformDataColumn::Data),
        "corrected" | "corrected_data" => Ok(TransformDataColumn::CorrectedData),
        "model" => Ok(TransformDataColumn::ModelData),
        "data,model,corrected" => Ok(TransformDataColumn::DataModelCorrected),
        "float_data" => Ok(TransformDataColumn::FloatData),
        "lag_data" => Ok(TransformDataColumn::LagData),
        "float_data,data" => Ok(TransformDataColumn::FloatDataAndData),
        "lag_data,data" => Ok(TransformDataColumn::LagDataAndData),
        "all" => Ok(TransformDataColumn::All),
        _ => Err(TaskFailure::validation(format!(
            "unsupported datacolumn {value:?}; CASA takes 'corrected', 'data', 'model', \
             'data,model,corrected', 'float_data', 'lag_data', 'float_data,data', \
             'lag_data,data' or 'all'"
        ))
        .at("datacolumn")),
    }
}

fn absolute_time_range(value: &str) -> Option<(f64, f64)> {
    let (start, end) = value.split_once('~')?;
    let start = start.trim().parse::<f64>().ok()?;
    let end = end.trim().parse::<f64>().ok()?;
    (start.is_finite()
        && end.is_finite()
        && (start > f64::from(i32::MAX) || end > f64::from(i32::MAX)))
    .then_some((start, end))
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
            let model = request_for(surface, &["--datacolumn", "model"]).unwrap();
            assert_eq!(model.data_column, TransformDataColumn::ModelData);
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
