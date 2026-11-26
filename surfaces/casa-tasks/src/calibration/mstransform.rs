// SPDX-License-Identifier: LGPL-3.0-or-later
//! `mstransform` - tutorial-scoped MeasurementSet transform, the `mstransform`
//! and `split` task provider, on the shared task front end. Its first word
//! names the surface (`mstransform split ...`); `mstransform` runs otherwise.

use std::{io::ErrorKind, num::NonZeroUsize};

use crate::ms::{ms_task_success, mstransform_task_schema_bundle, task_selection_text};
use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_calibration::{CallibError, transform_callibrary, transform_model_input};
use casa_contracts::{CasaValue, DefaultSpec, NumpyScalarType, ParameterValue, python_repr};
use casa_ms::selection::MsSelection;
use casa_ms::{
    ContinuumFitOptions, MsPartitionAxis, MsRegridOptions, MsTransformError,
    MsTransformMultiMsOptions, MsTransformRequest, MsTransformTaskOptions, MsTransformTiling,
    TimeAverageOptions, TransformDataColumn, TransformTaskHistory, mstransform_task,
};
use casa_types::quanta::{Quantity, Unit};

/// Front end of the `mstransform` binary, hosting `mstransform` and `split`.
pub fn mstransform_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "mstransform",
        mstransform_task_schema_bundle(),
        &["mstransform", "split"],
        run,
    )
    .expect("the transform contract carries mstransform and split")
}

/// Run the shared transform and project the public task's return value.
pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let (request, mut options) = transform_request(task)?;
    if request.input_ms != request.output_ms && !request.output_ms.exists() {
        let calibration = callibrary(task)?;
        // CASA builds a continuum iterator in place of the calibration
        // iterator when both task switches are enabled.
        if options.continuum.is_none() {
            options.input_view = calibration;
        }
        if task.surface() == "mstransform"
            && matches!(
                request.data_column,
                TransformDataColumn::ModelData
                    | TransformDataColumn::DataModelCorrected
                    | TransformDataColumn::All
            )
        {
            options.input_view = Some(transform_model_input(
                options.input_view.take(),
                task.flag("realmodelcol")?,
            ));
        }
    }
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
        MsTransformError::RegridParameter { parameter, reason }
        | MsTransformError::MultiMsParameter { parameter, reason } => {
            TaskFailure::validation(reason).at(parameter)
        }
        MsTransformError::ChannelWidthCount => {
            let parameter = channel_parameter(task);
            TaskFailure::validation(format!(
                "Number of {parameter} is different from the number of spw"
            ))
            .at(parameter)
        }
        other => TaskFailure::execution(other.to_string()),
    })?;
    let result = report
        .retained_mms
        .map_or(CasaValue::None, |value| CasaValue::Bool { value });
    ms_task_success(result, &report)
}

fn transform_request(
    task: &TaskInvocation<'_>,
) -> Result<(MsTransformRequest, MsTransformTaskOptions), TaskFailure> {
    let (width, channel_widths) = channel_width(task)?;
    let request = MsTransformRequest {
        input_ms: task.path("vis")?,
        output_ms: task.path("outputvis")?,
        spw: variant_selector(task, "spw")?.unwrap_or_default(),
        width,
        data_column: data_column(&task.required_text("datacolumn")?)?,
        selection: selection(task)?,
        keep_flags: task.flag("keepflags")?,
    };
    let (keep_mms, retained_scan_span) = if task.surface() == "split" {
        let keep_mms = task.flag("keepmms")?;
        let combine_scan =
            matches!(task.value("combine"), Some(ParameterValue::String(span)) if span == "scan");
        let span = if keep_mms && combine_scan {
            Quantity::read(&task.required_text("timebin")?)
                .ok()
                .map(|quantity| quantity.value())
        } else {
            None
        };
        (Some(keep_mms), span)
    } else {
        (None, None)
    };
    let options = MsTransformTaskOptions {
        use_weight_spectrum: task.surface() == "mstransform" && task.flag("usewtspectrum")?,
        hanning: task.surface() == "mstransform" && task.flag("hanning")?,
        combine_spws: task.surface() == "mstransform" && task.flag("combinespws")?,
        continuum: continuum_options(task)?,
        regrid: regrid_options(task)?,
        tiling: tile_layout(task)?,
        preserve_ids: task.surface() == "mstransform" && !task.flag("reindex")?,
        ddi_start: task
            .value("ddistart")
            .and_then(channel_integer)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or_default(),
        channel_widths,
        time_averaging: time_average_options(task)?,
        history: Some(task_history(task)),
        flag_command_widths: flag_command_widths(task),
        keep_mms,
        retained_scan_span,
        multi_ms: multi_ms_options(task)?,
        ..MsTransformTaskOptions::default()
    };
    Ok((request, options))
}

fn continuum_options(
    task: &TaskInvocation<'_>,
) -> Result<Option<ContinuumFitOptions>, TaskFailure> {
    if task.surface() != "mstransform" || !task.flag("douvcontsub")? {
        return Ok(None);
    }
    let nonnegative = |name| {
        usize::try_from(task.integer(name)?).map_err(|_| {
            TaskFailure::execution(format!("{name} must be nonnegative for continuum fitting"))
                .at(name)
        })
    };
    Ok(Some(ContinuumFitOptions {
        fit_spw: task.text("fitspw")?.unwrap_or_default(),
        order: nonnegative("fitorder")?,
        want_continuum: task.flag("want_cont")?,
        reweight: task.flag("denoising_lib")?,
        iterations: nonnegative("niter")?,
        threads: usize::try_from(task.integer("nthreads")?).unwrap_or(usize::MAX),
    }))
}

fn callibrary(
    task: &TaskInvocation<'_>,
) -> Result<Option<std::sync::Arc<dyn casa_ms::TransformInputView>>, TaskFailure> {
    if task.surface() != "mstransform" || !task.flag("docallib")? {
        return Ok(None);
    }
    let filename = task.text("callib")?.unwrap_or_default();
    transform_callibrary(&task.io_path(&filename), task.workspace()).map_err(|error| {
        let reason = match &error {
            CallibError::ReadFile { source, .. } if source.kind() == ErrorKind::NotFound => {
                "No such file or directory".into()
            }
            CallibError::ParseLine { line, .. } => {
                format!("Problem parsing cal library line (check for typos): {line}")
            }
            _ => error.to_string(),
        };
        TaskFailure::validation(reason).at("callib")
    })
}

fn multi_ms_options(
    task: &TaskInvocation<'_>,
) -> Result<Option<MsTransformMultiMsOptions>, TaskFailure> {
    if task.surface() != "mstransform" {
        return Ok(None);
    }
    let create = task.flag("createmms")?;
    let axis = match task.required_text("separationaxis")?.as_str() {
        "scan" | "SCAN" => MsPartitionAxis::Scan,
        "spw" | "SPW" => MsPartitionAxis::SpectralWindow,
        "auto" | "AUTO" => MsPartitionAxis::Automatic,
        "baseline" | "BASELINE" => MsPartitionAxis::Baseline,
        other => {
            return Err(
                TaskFailure::validation(format!("unallowed value {other}")).at("separationaxis")
            );
        }
    };
    let count = match task.value("numsubms") {
        Some(ParameterValue::String(text)) if text == "auto" => None,
        Some(value) => {
            let integer = channel_integer(value);
            let count = integer
                .and_then(|n| usize::try_from(n).ok())
                .and_then(NonZeroUsize::new);
            if create && count.is_none() {
                let message = match integer {
                    Some(0) => "float division by zero",
                    Some(_) => "numsubms must be positive.",
                    None => "'<' not supported between instances of 'str' and 'int'",
                };
                return Err(TaskFailure::validation(message).at("numsubms"));
            }
            count
        }
        None => None,
    };
    Ok(Some(MsTransformMultiMsOptions {
        create,
        axis,
        count,
        monolithic: task.flag("monolithic_processing")?,
        scan_span: matches!(task.value("timespan"), Some(ParameterValue::String(value)) if value == "scan"),
        multiple_spws: task
            .value("nspw")
            .and_then(channel_integer)
            .is_some_and(|count| count > 1),
    }))
}

fn tile_layout(task: &TaskInvocation<'_>) -> Result<Option<MsTransformTiling>, TaskFailure> {
    if task.surface() != "mstransform" {
        return Ok(None);
    }
    let invalid = |reason| TaskFailure::validation(reason).at("tileshape");
    let Some(ParameterValue::Array(values)) = task.value("tileshape") else {
        return Err(invalid("Parameter tileshape must have 1 or 3 elements."));
    };
    let values = values
        .iter()
        .map(|value| {
            channel_integer(value).ok_or_else(|| invalid("Tile extents must be integers."))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = match values.as_slice() {
        [0] => MsTransformTiling::Standard,
        [1] => MsTransformTiling::FastMosaic,
        [_] => {
            return Err(invalid(
                "When tileshape has one element, it should be either 0 or 1.",
            ));
        }
        &[corr, chan, rows]
            if [corr, chan, rows]
                .into_iter()
                .all(|n| (0..=i64::from(i32::MAX)).contains(&n)) =>
        {
            MsTransformTiling::Explicit([corr as usize, chan as usize, rows as usize])
        }
        [_, _, _] => return Err(invalid("Tile extents must be nonnegative 32-bit integers.")),
        _ => return Err(invalid("Parameter tileshape must have 1 or 3 elements.")),
    };
    Ok(Some(result))
}

fn regrid_options(task: &TaskInvocation<'_>) -> Result<Option<MsRegridOptions>, TaskFailure> {
    if task.surface() != "mstransform" || !task.flag("regridms")? {
        return Ok(None);
    }
    let integer = |name| {
        task.value(name)
            .and_then(channel_integer)
            .ok_or_else(|| TaskFailure::validation("expected an integer").at(name))
    };
    let variant = |name| {
        task.value(name)
            .and_then(variant_selector_text)
            .unwrap_or_default()
    };
    Ok(Some(MsRegridOptions {
        mode: task.required_text("mode")?,
        nchan: integer("nchan")?,
        start: variant("start"),
        width: variant("width"),
        nspw: usize::try_from(integer("nspw")?)
            .ok()
            .filter(|&count| count > 0)
            .ok_or_else(|| {
                TaskFailure::validation("expected a positive number of windows").at("nspw")
            })?,
        interpolation: task.required_text("interpolation")?,
        phasecenter: variant("phasecenter"),
        restfreq: task.required_text("restfreq")?,
        outframe: task.required_text("outframe")?,
        veltype: task.required_text("veltype")?,
        preaverage: task.flag("preaverage")?,
    }))
}

fn task_history(task: &TaskInvocation<'_>) -> TransformTaskHistory {
    let mut bindings = task
        .session()
        .bundle()
        .surface
        .bindings()
        .iter()
        .filter(|binding| {
            binding
                .projections
                .python
                .as_ref()
                .is_some_and(|python| !python.keyword_only)
        })
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let parameters = bindings
        .into_iter()
        .filter_map(|binding| {
            let value = task
                .session()
                .states()
                .get(&binding.name)
                .and_then(|state| state.value.as_ref())
                .or(match &binding.default {
                    DefaultSpec::Literal { value } => Some(value),
                    DefaultSpec::Conditional { fallback, .. } => Some(fallback),
                    DefaultSpec::Required => None,
                })?;
            let value = if matches!(binding.name.as_str(), "vis" | "outputvis")
                && let ParameterValue::Array(paths) = value
                && let [path] = paths.as_slice()
            {
                path
            } else {
                value
            };
            let printable = match value {
                ParameterValue::String(value)
                    if value == "none"
                        && matches!(
                            binding.name.as_str(),
                            "field" | "spw" | "scan" | "antenna" | "timerange"
                        ) =>
                {
                    r#""""#.into()
                }
                ParameterValue::String(value) => format!("\"{value}\""),
                ParameterValue::Array(values)
                    if task.surface() == "split" && binding.name == "correlation" =>
                {
                    let labels = values
                        .iter()
                        .filter_map(|label| match label {
                            ParameterValue::String(text) => Some(text.to_uppercase()),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    format!("\"{}\"", labels.join(", "))
                }
                ParameterValue::ScalarArray(value)
                    if matches!(
                        binding.name.as_str(),
                        "field" | "spw" | "scan" | "antenna" | "timerange"
                    ) =>
                {
                    match value.as_ref() {
                        ParameterValue::String(text) => text.clone(),
                        value => python_repr(value),
                    }
                }
                ParameterValue::NumpyScalar { value, .. } => python_repr(value),
                ParameterValue::EmptyArray { .. } => "[]".into(),
                value => python_repr(value),
            };
            Some((binding.name.clone(), printable))
        })
        .collect();
    TransformTaskHistory {
        task: task.surface().to_string(),
        parameters,
    }
}

fn time_average_options(
    task: &TaskInvocation<'_>,
) -> Result<Option<TimeAverageOptions>, TaskFailure> {
    let split = task.surface() == "split";
    if !split && !task.flag("timeaverage")? {
        return Ok(None);
    }
    let interval = task.required_text("timebin")?;
    let quantity = Quantity::read(&interval).map_err(|_| {
        TaskFailure::execution(format!(
            "Error in QuantumHolder::fromString with input string {interval:?}: Illegal input units or format"
        )).at("timebin")
    })?;
    let seconds = Unit::new("s").expect("seconds are a defined unit");
    let radians = Unit::new("rad").expect("radians are a defined unit");
    let interval_seconds = quantity
        .get_value_in(&seconds)
        .or_else(|_| {
            quantity
                .get_value_in(&radians)
                .map(|angle| angle * 43_200.0 / std::f64::consts::PI)
        })
        .unwrap_or_else(|_| quantity.get_si_value());
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
    let parameter = if split { "combine" } else { "timespan" };
    let span = match task.value(parameter) {
        None => "",
        Some(ParameterValue::String(span)) => span,
        Some(_) => return Err(TaskFailure::execution(
            "RecordRep::get_pointer - incorrect data type String   used for field timespan with type unknown",
        ).at(parameter)),
    };
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

fn flag_command_widths(task: &TaskInvocation<'_>) -> Option<Vec<i64>> {
    let value = task.value(channel_parameter(task))?;
    match value {
        ParameterValue::Array(widths) => widths.iter().map(channel_integer).collect(),
        value if task.surface() == "mstransform" => channel_integer(value).map(|width| vec![width]),
        _ => None,
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
        field: variant_selector(task, "field")?,
        scan: variant_selector(task, "scan")?,
        antenna: variant_selector(task, "antenna")?,
        timerange: variant_selector(task, "timerange")?,
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
    if let ParameterValue::String(raw) = value {
        if matches!(name, "field" | "spw" | "scan" | "antenna" | "timerange") {
            let text = task_selection_text(task, name)?;
            let present = if name == "spw" {
                !text.is_empty()
            } else {
                !text.trim().is_empty()
            };
            return Ok(present.then_some(text));
        }
        return Ok((!raw.is_empty()).then(|| raw.clone()));
    }
    if task.surface() == "split"
        && matches!(
            name,
            "array" | "feed" | "field" | "spw" | "scan" | "antenna"
        )
    {
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

    use crate::{ParameterRuntime, TaskCommandLine};

    use super::*;

    /// The request a `surface` command line resolves to.
    fn request_for(surface: &str, args: &[&str]) -> Result<MsTransformRequest, TaskFailure> {
        let args = ["--vis", "in.ms", "--outputvis", "out.ms"]
            .iter()
            .chain(args)
            .map(OsString::from)
            .collect::<Vec<_>>();
        let bundle = casa_contracts::builtin_surface_bundle(surface).unwrap();
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
