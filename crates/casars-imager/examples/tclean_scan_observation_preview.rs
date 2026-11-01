// SPDX-License-Identifier: LGPL-3.0-or-later
//! Temporary evidence-only front end; never install as a production provider.
use casa_provider_contracts::SurfaceDefinition;
use std::process::ExitCode;

#[global_allocator]
static ALLOCATOR: casa_imaging_application::ReturningAllocator =
    casa_imaging_application::ReturningAllocator;

fn main() -> ExitCode {
    let mut contract = casars_imager::imager_task_schema_bundle();
    let surface = contract
        .parameter_surfaces
        .iter_mut()
        .find(|bundle| bundle.surface.id() == "tclean")
        .expect("tclean surface");
    let SurfaceDefinition::Task(definition) = &mut surface.surface;
    let mut removed = Vec::new();
    for binding in &mut definition.bindings {
        if matches!(binding.name.as_str(), "scan" | "observation") {
            let marker = binding
                .unimplemented
                .take()
                .expect("production refusal must still exist");
            assert_eq!(marker.issue, 980);
            removed.push(binding.name.as_str());
        }
    }
    removed.sort_unstable();
    assert_eq!(removed, ["observation", "scan"]);
    // The same TaskInvocation -> run_task callback retains original given-value
    // provenance, residual statistics, CASA return construction and reply format.
    casa_task_runtime::TaskFrontEnd::new(
        "tclean-scan-observation-preview",
        contract,
        &["tclean"],
        casars_imager::run_task,
    )
    .expect("preview differs only by two local refusal markers")
    .main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
