// SPDX-License-Identifier: LGPL-3.0-or-later
//! Host projection for the native simanalyze application.
use super::{
    SimobserveRunReport, SimobserveRunTaskResult, SimobserveTaskParameters, artifact,
    simobserve_task_schema_bundle,
};
use crate::{CasaValue, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::{
    ProviderProtocolDescriptor, ProviderSurfaceKind, TaskProviderContract, builtin_surface_bundle,
};
use casa_simulation::simanalyze::{SimanalyzeRequest, simanalyze_with_controls};
use casa_simulation::{ExecutionControls, SessionResources};
use std::path::Path;
use std::time::Instant;

/// simanalyze's native task protocol.
pub fn simanalyze_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        "casa_simanalyze_task",
        1,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}
/// simanalyze uses the shared simulation request, report and artifact shapes.
pub fn simanalyze_task_schema_bundle() -> TaskProviderContract {
    let mut bundle = simobserve_task_schema_bundle();
    bundle.protocol = simanalyze_protocol_descriptor();
    bundle.parameter_surfaces =
        vec![builtin_surface_bundle("simanalyze").expect("built-in simanalyze surface")];
    bundle
}
/// The simanalyze command-line and Python task host.
pub fn simanalyze_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "simanalyze",
        simanalyze_task_schema_bundle(),
        &["simanalyze"],
        run,
    )
    .expect("simanalyze contract")
}
pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let started = Instant::now();
    let parameters = task.plain_values();
    let resources = SessionResources::from_controls(&ExecutionControls::default())
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let mut execution_parameters = parameters.clone();
    task.resolve_file_values(&mut execution_parameters, &["project", "logfile"]);
    if task.native_options().is_some() {
        let project =
            Path::new(execution_parameters["project"].as_str().unwrap_or_default()).to_path_buf();
        resolve_inputs(task, &project, &mut execution_parameters);
    }
    let request = SimanalyzeRequest {
        parameters: execution_parameters,
        resources,
    };
    let payload = super::plot_payload(task);
    let cancel = super::cancellation(task);
    let (outcome, plots) = super::with_figures(payload.as_deref(), |sink| {
        simanalyze_with_controls(&request, sink, &cancel)
    })
    .map_err(|error| super::simulation_failure(&cancel, error))?;
    let mut artifacts: Vec<_> = outcome
        .products
        .iter()
        .map(|path| artifact(&outcome.stem, path))
        .collect();
    for plot in &plots {
        if let Some(path) = &plot.output_file {
            let mut product = artifact(&outcome.stem, path);
            product.exists = false;
            product.render_pending = true;
            artifacts.push(product);
        }
    }
    let report = SimobserveRunTaskResult {
        request: SimobserveTaskParameters(parameters),
        run: SimobserveRunReport {
            stem: outcome.stem,
            directory: outcome.directory.display().to_string(),
            warnings: outcome.warnings,
            elapsed_ns: started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
        },
        artifacts,
        plots,
    };
    TaskSuccess::new(CasaValue::None, &report)
}

fn resolve_inputs(
    task: &TaskInvocation<'_>,
    project: &Path,
    parameters: &mut serde_json::Map<String, serde_json::Value>,
) {
    use serde_json::Value;
    let rebase = |value: &mut Value| {
        if let Value::String(names) = value {
            *names = names
                .split(',')
                .map(|name| super::project_fallback_path(task, project, name.trim()))
                .collect::<Vec<_>>()
                .join(",");
        }
    };
    for name in ["vis", "skymodel", "modelimage", "featherimage"] {
        if let Some(value) = parameters.get_mut(name) {
            match value {
                Value::Array(paths) => paths.iter_mut().for_each(rebase),
                _ => rebase(value),
            }
        }
    }
}
