// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MeasurementSet task binaries on the shared task front end (#931).
//!
//! Each binary's `main` builds a [`TaskFrontEnd`] from its provider contract
//! and a callback that reads the resolved run into its typed request; these
//! are the reads and the reply the binaries share. The front end owns the
//! command line, `--json-run`, Last and logging.
//!
//! [`TaskFrontEnd`]: casa_task_runtime::TaskFrontEnd

use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;

use crate::FlagMerge;

/// A MeasurementSet task's reply: `value` is what CASA's task returns (`&()`
/// for `None`), and `result` the provider's report, which a command line
/// prints as JSON.
pub fn ms_task_success(
    value: &impl Serialize,
    result: &impl Serialize,
) -> Result<TaskSuccess, TaskFailure> {
    let text = serde_json::to_string_pretty(result)
        .map_err(|error| TaskFailure::execution(format!("serialize the result: {error}")))?;
    Ok(TaskSuccess::new(value, result)?.with_text(text + "\n"))
}

/// Read `name`, a flag-version merge: `replace`, `or` or `and`, any case.
pub fn task_flag_merge(task: &TaskInvocation<'_>, name: &str) -> Result<FlagMerge, TaskFailure> {
    let text = task.required_text(name)?;
    match text.to_ascii_lowercase().as_str() {
        "replace" => Ok(FlagMerge::Replace),
        "or" => Ok(FlagMerge::Or),
        "and" => Ok(FlagMerge::And),
        _ => Err(TaskFailure::validation(format!(
            "parameter {name:?} must be replace, or or and, found {text:?}"
        ))
        .at(name)),
    }
}

/// Read `name`, a CASA selection string; empty when unset.
pub fn task_selection_text(task: &TaskInvocation<'_>, name: &str) -> Result<String, TaskFailure> {
    Ok(task.text(name)?.unwrap_or_default())
}
