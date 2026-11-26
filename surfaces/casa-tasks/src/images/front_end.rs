// SPDX-License-Identifier: LGPL-3.0-or-later
//! The image task binaries on the shared task front end (#931).
//!
//! Each binary's `main` builds its front end here, passing the surfaces it
//! hosts and a callback that reads the resolved run into its typed request.
//! The front end owns the command line, `--json-run`, Last and logging.

use std::collections::BTreeMap;

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

pub(super) fn region_in_workspace(
    task: &TaskInvocation<'_>,
) -> Result<Option<String>, TaskFailure> {
    Ok(task.text("region")?.map(|region| {
        let path = task.io_path(&region);
        if task.native_options().is_some() && path.is_file() {
            path.to_string_lossy().into_owned()
        } else {
            region
        }
    }))
}
use casa_contracts::{CasaValue, ParameterValue, parameter_value_to_casacore};
use casa_core::{RecordValue, Value};
use casa_images::{ImageError, PbcorRegion};

use super::task_contract::{ImageAnalysisTaskResult, image_analysis_task_schema_bundle};

/// The front end of the image binary `program`, which hosts `surfaces`.
pub fn image_task_front_end<F>(program: &str, surfaces: &[&str], execute: F) -> TaskFrontEnd<F>
where
    F: Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>,
{
    TaskFrontEnd::new_with_deferred_schema(
        program,
        image_analysis_task_schema_bundle(),
        surfaces,
        execute,
    )
    .expect("the image-analysis contract carries every image surface")
}

/// A product-writing image task's reply: CASA's return value `None`, and
/// its summary as the result, which a command line prints as JSON.
pub fn image_task_success(
    result: Result<ImageAnalysisTaskResult, ImageError>,
) -> Result<TaskSuccess, TaskFailure> {
    image_task_returning(CasaValue::None, result)
}

/// A product-writing image task's reply that returns CASA's `value`, as
/// `immath` returns `True`.
pub fn image_task_returning(
    value: CasaValue,
    result: Result<ImageAnalysisTaskResult, ImageError>,
) -> Result<TaskSuccess, TaskFailure> {
    let result = result.map_err(|error| match error {
        ImageError::TaskArgumentType { parameter, message } => {
            TaskFailure::validation(message).at(parameter)
        }
        error => TaskFailure::execution(error.to_string()),
    })?;
    let text = serde_json::to_string_pretty(&result)
        .map_err(|error| TaskFailure::execution(format!("serialize the result: {error}")))?;
    Ok(TaskSuccess::new(value, &result)?.with_text(text + "\n"))
}

/// Convert a serialized table parameter dictionary into a casacore [`RecordValue`].
pub(super) fn table_fields_to_record(
    fields: &BTreeMap<String, ParameterValue>,
) -> Result<RecordValue, TaskFailure> {
    let value = parameter_value_to_casacore(&ParameterValue::Table(fields.clone()))
        .map_err(TaskFailure::execution)?;
    match value {
        Value::Record(record) => Ok(record),
        _ => Err(TaskFailure::execution("expected a casacore record")),
    }
}

/// Convert an optional `region` parameter value into [`PbcorRegion`].
pub(super) fn parse_region_parameter(
    value: Option<&ParameterValue>,
    allow_empty_array: bool,
) -> Result<Option<PbcorRegion>, TaskFailure> {
    match value {
        None => Ok(None),
        Some(ParameterValue::String(text)) if text.trim().is_empty() => Ok(None),
        Some(ParameterValue::String(text)) => Ok(Some(PbcorRegion::Text(text.clone()))),
        Some(ParameterValue::Table(fields)) if fields.is_empty() => Ok(None),
        Some(ParameterValue::Table(fields)) => {
            Ok(Some(PbcorRegion::Record(table_fields_to_record(fields)?)))
        }
        Some(ParameterValue::Array(items)) if allow_empty_array && items.is_empty() => Ok(None),
        _ => Err(TaskFailure::validation(
            "region must be CRTF text, a filename, or a CASA region record",
        )
        .at("region")),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use crate::{TaskFrontEnd, TaskSuccess};
    use casa_contracts::CasaValue;

    use super::image_task_front_end;
    use crate::images::image_analysis_task_schema_bundle;

    #[test]
    fn image_front_end_matches_the_runtime_contract() {
        let old = TaskFrontEnd::new_with_deferred_schema(
            "imexplore",
            image_analysis_task_schema_bundle(),
            &["imhead", "imstat"],
            |_| TaskSuccess::new(CasaValue::None, &()),
        )
        .unwrap();
        let current = image_task_front_end("imexplore", &["imhead", "imstat"], |_| {
            TaskSuccess::new(CasaValue::None, &())
        });
        for option in ["--help", "--json-schema", "--protocol-info", "--show"] {
            let args = [OsString::from("imhead"), OsString::from(option)];
            assert_eq!(current.dispatch(&args), old.dispatch(&args), "{option}");
        }
        let workspace = tempfile::tempdir().unwrap();
        let request = workspace.path().join("invalid.json");
        std::fs::write(
            &request,
            r#"{"surface":"imhead","values":{"imagename":123}}"#,
        )
        .unwrap();
        let args = [
            OsString::from("imhead"),
            OsString::from("--json-run"),
            request.into_os_string(),
        ];
        let refused = current.dispatch(&args);
        assert_ne!(refused.code, 0);
        assert_eq!(refused, old.dispatch(&args));
    }
}
