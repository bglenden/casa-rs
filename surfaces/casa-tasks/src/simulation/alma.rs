// SPDX-License-Identifier: LGPL-3.0-or-later
//! Host projection for the native simalma application.
use super::{
    SimobserveRunReport, SimobserveRunTaskResult, SimobserveTaskParameters, artifact,
    simobserve_task_schema_bundle,
};
use crate::{CasaValue, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::{
    ProviderProtocolDescriptor, ProviderSurfaceKind, TaskProviderContract, builtin_surface_bundle,
};
use casa_plot_data::simulation::simalma_with_plots;
use casa_simulation::simalma::{SimalmaRequest, simalma};
use casa_simulation::{ExecutionControls, SessionResources, SimulatorOptions};
use std::time::Instant;

/// simalma's native task protocol.
pub fn simalma_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        "casa_simalma_task",
        1,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}
/// simalma uses the shared simulation request, report and artifact shapes.
pub fn simalma_task_schema_bundle() -> TaskProviderContract {
    let mut bundle = simobserve_task_schema_bundle();
    bundle.protocol = simalma_protocol_descriptor();
    bundle.parameter_surfaces =
        vec![builtin_surface_bundle("simalma").expect("built-in simalma surface")];
    bundle
}
/// The simalma command-line and Python task host.
pub fn simalma_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "simalma",
        simalma_task_schema_bundle(),
        &["simalma"],
        run,
    )
    .expect("simalma contract")
}
fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let started = Instant::now();
    let parameters = task.plain_values();
    let resources = SessionResources::from_controls(&ExecutionControls::default())
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let request = SimalmaRequest {
        parameters: parameters.clone(),
        options: SimulatorOptions::from_system(resources)
            .map_err(|error| TaskFailure::execution(error.to_string()))?,
        data_roots: super::data_roots().map_err(TaskFailure::execution)?,
    };
    let (outcome, plots) = match std::env::var_os("CASARS_PLOT_PAYLOAD") {
        Some(path) => simalma_with_plots(&request, std::path::Path::new(&path)),
        None => simalma(&request).map(|outcome| (outcome, Vec::new())),
    }
    .map_err(|error| TaskFailure::execution(error.to_string()))?;
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
