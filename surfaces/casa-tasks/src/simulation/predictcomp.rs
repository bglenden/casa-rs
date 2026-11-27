// SPDX-License-Identifier: LGPL-3.0-or-later
//! `predictcomp` task adapter and schema bundle.

use casa_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata,
    ProviderProtocolDescriptor, ProviderSurfaceKind, TaskOperationDescriptor, TaskProviderContract,
    TaskProviderSchemas, TaskSemanticContract, builtin_surface_bundle, merged_components,
};
use schemars::schema_for;
use serde_json::Map;

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

mod task;
pub(crate) use task::run;

/// Return `predictcomp`'s task provider schema bundle.
pub fn predictcomp_task_schema_bundle() -> TaskProviderContract {
    let request = schema_for!(Map<String, serde_json::Value>);
    let result = schema_for!(task::PredictcompReport);
    TaskProviderContract {
        protocol: ProviderProtocolDescriptor::new(
            "casa_predictcomp_task",
            1,
            ProviderSurfaceKind::Task,
            env!("CARGO_PKG_VERSION"),
        ),
        semantic: TaskSemanticContract {
            request_schema: request.clone(),
            result_schema: result.clone(),
            operations: vec![TaskOperationDescriptor {
                name: "run".into(),
                request_kind: "run".into(),
                result_kind: Some("run".into()),
            }],
            front_end: None,
        },
        components: merged_components([&request, &result]),
        annotations: serde_json::json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".into()),
                    protocol_info: Some("--protocol-info".into()),
                    json_run: Some("--json-run <SOURCE>".into()),
                },
            }),
            python: None,
        },
        parameter_surfaces: vec![
            builtin_surface_bundle("predictcomp").expect("predictcomp surface"),
        ],
        domain_schemas: TaskProviderSchemas {
            request_schema: request,
            result_schema: result,
        },
    }
}

/// The `predictcomp` command-line and Python task host.
pub fn predictcomp_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "predictcomp",
        predictcomp_task_schema_bundle(),
        &["predictcomp"],
        task::run,
    )
    .expect("the predictcomp contract carries its surface")
}
