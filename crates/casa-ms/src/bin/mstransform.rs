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
    ms_task_success(&(), &report)
}

fn transform_request(task: &TaskInvocation<'_>) -> Result<MsTransformRequest, TaskFailure> {
    let width = task.integer("width")?;
    let width = usize::try_from(width)
        .ok()
        .filter(|width| *width >= 1)
        .ok_or_else(|| {
            TaskFailure::validation(format!("width must be at least 1, found {width}")).at("width")
        })?;
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
    let msselect = task_selection_text(task, "msselect")?;
    if !msselect.is_empty() {
        selection = selection.taql(&msselect);
    }
    Ok(selection)
}

fn data_column(value: &str) -> Result<TransformDataColumn, TaskFailure> {
    match value.trim().to_ascii_uppercase().as_str() {
        "DATA" => Ok(TransformDataColumn::Data),
        "CORRECTED" | "CORRECTED_DATA" => Ok(TransformDataColumn::CorrectedData),
        other => Err(TaskFailure::validation(format!(
            "unsupported datacolumn {other:?}; expected DATA or CORRECTED_DATA"
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
    use super::*;

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
