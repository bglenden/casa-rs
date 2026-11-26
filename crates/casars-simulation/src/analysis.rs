// SPDX-License-Identifier: LGPL-3.0-or-later
//! Host projection for the native simanalyze application.
use crate::{SimobserveRunReport, SimobserveRunTaskResult, SimobserveTaskParameters, artifact};
use casa_contracts::{
    ProviderProtocolDescriptor, ProviderSurfaceKind, TaskProviderContract, builtin_surface_bundle,
};
use casa_simulation::simanalyze::{SimanalyzeRequest, simanalyze, simanalyze_with_plots};
use casa_simulation::{ExecutionControls, SessionResources};
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
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
    let mut bundle = crate::simobserve_task_schema_bundle();
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
fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let started = Instant::now();
    let parameters = task.plain_values();
    let resources = SessionResources::from_controls(&ExecutionControls::default())
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let request = SimanalyzeRequest {
        parameters: parameters.clone(),
        resources,
    };
    let outcome = match std::env::var_os("CASARS_PLOT_PAYLOAD") {
        Some(path) => simanalyze_with_plots(&request, Path::new(&path)),
        None => simanalyze(&request),
    }
    .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let mut artifacts: Vec<_> = outcome
        .products
        .iter()
        .map(|path| artifact(&outcome.stem, path))
        .collect();
    for plot in &outcome.plots {
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
        plots: outcome.plots,
    };
    TaskSuccess::new(casa_task_runtime::CasaValue::None, &report)
}
