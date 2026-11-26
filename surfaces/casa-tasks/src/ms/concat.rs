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
    TaskFrontEnd::new_with_deferred_schema(
        "concat",
        concat_task_schema_bundle(),
        &["concat"],
        |task| {
            let request: ConcatRequest =
                serde_json::from_value(serde_json::json!(task.plain_values()))
                    .map_err(|e| TaskFailure::validation(e.to_string()))?;
            let report = concat(&request).map_err(|e| TaskFailure::execution(e.to_string()))?;
            ms_task_success(CasaValue::None, &report)
        },
    )
    .expect("concat contract")
}
