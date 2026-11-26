// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task adapter for `appendantab`.

use casa_calibration::appendantab::{AppendantabError, AppendantabRequest, run_appendantab};
use casa_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = AppendantabRequest {
        input: task.input_path("vis")?,
        output: task.path("outvis")?,
        antab: task.input_path("antab")?,
        overwrite: task.flag("overwrite")?,
        append_tsys: task.flag("append_tsys")?,
        append_gain: task.flag("append_gc")?,
    };
    let summary = run_appendantab(&request).map_err(|error| match error {
        AppendantabError::Message(message) => TaskFailure::execution(message),
        AppendantabError::At(parameter, message) => TaskFailure::execution(message).at(parameter),
    })?;
    casa_calibration::timing::report_process_memory();
    TaskSuccess::new(CasaValue::None, &summary)
}
