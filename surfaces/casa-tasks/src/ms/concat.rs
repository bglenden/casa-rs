// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical schema contract and task front end for `concat`.

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::{CasaValue, TaskProviderContract};
use casa_ms::concat::{ConcatReport, ConcatRequest, concat};

use super::front_end::ms_task_success;
use super::task_contract::one_shot_contract;

/// The canonical task contract on the shared CLI/Python envelope.
pub fn concat_task_schema_bundle() -> TaskProviderContract {
    one_shot_contract::<ConcatRequest, ConcatReport>("casa_ms_concat_task", &["concat"])
}

/// The command-line host; all scientific work is in [`casa_ms::concat::concat()`].
pub fn concat_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema("concat", concat_task_schema_bundle(), &["concat"], run)
        .expect("concat contract")
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let mut request: ConcatRequest = serde_json::from_value(serde_json::json!(task.plain_values()))
        .map_err(|error| TaskFailure::validation(error.to_string()))?;
    request
        .vis
        .iter_mut()
        .for_each(|path| *path = task.io_path(&*path));
    request.concatvis = task.io_path(&request.concatvis);
    let report = concat(&request).map_err(|error| TaskFailure::execution(error.to_string()))?;
    ms_task_success(CasaValue::None, &report)
}
