// SPDX-License-Identifier: LGPL-3.0-or-later
//! `predictcomp`, the native calibrator component and visibility prediction task.

use std::process::ExitCode;

use casa_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata,
    ProviderProtocolDescriptor, ProviderSurfaceKind, TaskOperationDescriptor, TaskProviderContract,
    TaskProviderSchemas, TaskSemanticContract, builtin_surface_bundle, merged_components,
};
use casa_task_runtime::TaskFrontEnd;
use schemars::schema_for;
use serde_json::Map;

#[path = "predictcomp/task.rs"]
mod task;

fn main() -> ExitCode {
    TaskFrontEnd::new_with_deferred_schema(
        "predictcomp",
        schema_bundle(),
        &["predictcomp"],
        task::run,
    )
    .expect("the predictcomp contract carries its surface")
    .main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn schema_bundle() -> TaskProviderContract {
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
