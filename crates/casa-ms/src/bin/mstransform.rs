// SPDX-License-Identifier: LGPL-3.0-or-later
//! `mstransform` - tutorial-scoped MeasurementSet transform, the `mstransform`
//! and `split` task provider, on the shared task front end. Its first word
//! names the surface (`mstransform split ...`); `mstransform` runs otherwise.

use std::process::ExitCode;

use casa_ms::selection::MsSelection;
use casa_ms::{
    MsTransformError, MsTransformRequest, MsTransformTaskOptions, TimeAverageOptions,
    TransformDataColumn, ms_task_success, mstransform_task, mstransform_task_schema_bundle,
    task_selection_text,
};
use casa_provider_contracts::{CasaValue, NumpyScalarType, ParameterValue, python_repr};
use casa_task_runtime::{
    CasaLoggingConfig, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess,
};
use casa_types::quanta::{Quantity, Unit};

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
    let (request, options) = transform_request(task)?;
    // The output the transform refuses before it starts is the argument's
    // fault; anything else failed the transform.
    let report = mstransform_task(&request, &options).map_err(|error| match error {
        MsTransformError::OutputExists { .. } | MsTransformError::SameInputOutput { .. } => {
            TaskFailure::validation(error.to_string()).at("outputvis")
        }
        MsTransformError::InvalidSpw { .. } => TaskFailure::validation(error.to_string()).at("spw"),
        MsTransformError::MissingDataColumn { path, column } => TaskFailure::execution(format!(
            "Desired column ({column}) not found in the input MS ({path})."
        )),
        MsTransformError::ChannelWidthCount => {
            let parameter = channel_parameter(task);
            TaskFailure::validation(format!(
                "Number of {parameter} is different from the number of spw"
            ))
            .at(parameter)
        }
        other => TaskFailure::execution(other.to_string()),
    })?;
    ms_task_success(CasaValue::None, &report)
}

fn transform_request(
    task: &TaskInvocation<'_>,
) -> Result<(MsTransformRequest, MsTransformTaskOptions), TaskFailure> {
    let (width, channel_widths) = channel_width(task)?;
    let request = MsTransformRequest {
        input_ms: task.path("vis")?,
        output_ms: task.path("outputvis")?,
        spw: task_selection_text(task, "spw")?,
        width,
        data_column: data_column(&task.required_text("datacolumn")?)?,
        selection: selection(task)?,
        keep_flags: task.flag("keepflags")?,
    };
    let options = MsTransformTaskOptions {
        use_weight_spectrum: task.surface() == "mstransform" && task.flag("usewtspectrum")?,
        channel_widths,
        time_averaging: time_average_options(task)?,
    };
    Ok((request, options))
}

fn time_average_options(
    task: &TaskInvocation<'_>,
) -> Result<Option<TimeAverageOptions>, TaskFailure> {
    let split = task.surface() == "split";
    if !split && !task.flag("timeaverage")? {
        return Ok(None);
    }
    let interval = task.required_text("timebin")?;
    let seconds = Unit::new("s").expect("seconds are a defined unit");
    let interval_seconds = interval
        .parse::<Quantity>()
        .and_then(|time| time.get_value_in(&seconds))
        .map_err(|error| TaskFailure::validation(error.to_string()).at("timebin"))?;
    if interval_seconds <= 0.0 || !interval_seconds.is_finite() {
        return if split {
            Ok(None)
        } else {
            Err(
                TaskFailure::validation("Parameter timebin must be > '0s' to do time averaging")
                    .at("timebin"),
            )
        };
    }
    let span = task
        .text(if split { "combine" } else { "timespan" })?
        .unwrap_or_default();
    Ok(Some(TimeAverageOptions {
        interval_seconds,
        span_scans: span.contains("scan"),
        span_states: span.contains("state"),
        span_fields: span.contains("field"),
        max_uvw_distance: if split {
            0.0
        } else {
            task.number("maxuvwdistance")?
        },
    }))
}

fn channel_parameter(task: &TaskInvocation<'_>) -> &'static str {
    if task.surface() == "split" {
        "width"
    } else {
        "chanbin"
    }
}

fn channel_width(task: &TaskInvocation<'_>) -> Result<(usize, Option<Vec<usize>>), TaskFailure> {
    let name = channel_parameter(task);
    let split = name == "width";
    if !split && !task.flag("chanaverage")? {
        return Ok((1, None));
    }
    let invalid_type = || TaskFailure::validation(format!("invalid {name} type")).at(name);
    let empty = || {
        let message = if split {
            "list index out of range"
        } else {
            "chanbin must contain at least one width"
        };
        TaskFailure::validation(message).at(name)
    };
    let value = task.value(name).ok_or_else(invalid_type)?;
    match value {
        ParameterValue::Array(values) if values.is_empty() => Err(empty()),
        ParameterValue::Array(values) => {
            let widths = values
                .iter()
                .map(|value| channel_integer(value).ok_or_else(invalid_type))
                .collect::<Result<Vec<_>, _>>()?;
            if split && !widths.iter().any(|&width| width > 1) {
                return Ok((1, None));
            }
            let widths = widths
                .into_iter()
                .map(|width| {
                    usize::try_from(width)
                        .ok()
                        .filter(|&width| width > 0)
                        .ok_or_else(|| {
                            TaskFailure::validation(format!(
                                "{name} must contain positive integers"
                            ))
                            .at(name)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((widths[0], Some(widths)))
        }
        ParameterValue::EmptyArray { .. } => Err(empty()),
        ParameterValue::String(text) if split => {
            let width = if text.bytes().all(|digit| digit.is_ascii_digit()) {
                text.parse::<usize>().unwrap_or(1)
            } else {
                1
            };
            Ok((width.max(1), None))
        }
        value => {
            // CLI flags for variant parameters are passed as text. The
            // Python task validates the accepted argument types separately.
            let width = match value {
                ParameterValue::String(text) => text.parse().ok(),
                value => channel_integer(value),
            }
            .ok_or_else(invalid_type)?;
            if !split && width <= 1 && !matches!(value, ParameterValue::NumpyScalar { .. }) {
                return Err(TaskFailure::validation(
                    "Parameter chanbin must be > 1 to do channel averaging",
                )
                .at(name));
            }
            Ok((usize::try_from(width).unwrap_or(1).max(1), None))
        }
    }
}

fn channel_integer(value: &ParameterValue) -> Option<i64> {
    match value {
        ParameterValue::Integer(value) => Some(*value),
        ParameterValue::Bool(value) => Some(i64::from(*value)),
        ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Int32 | NumpyScalarType::Int64,
            value,
        }
        | ParameterValue::ScalarArray(value) => channel_integer(value),
        _ => None,
    }
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
            .map(|(request, _)| request)
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
