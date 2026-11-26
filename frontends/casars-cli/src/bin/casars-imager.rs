// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars-imager` — native CASA-compatible imaging CLI shell.

use std::process::ExitCode;

use casa_tasks::imaging::{
    ImagingExecutionOptions, RUN_SUMMARY_VARIABLE, task_front_end_with_options,
};

/// Freed working buffers leave the process at once, so a run's footprint
/// follows the memory its phases admit.
#[global_allocator]
static ALLOCATOR: casa_imaging_application::ReturningAllocator =
    casa_imaging_application::ReturningAllocator;

fn main() -> ExitCode {
    if let Err(error) = casars_cli::interrupt::install() {
        eprintln!("casars-imager: install interrupt handler: {error}");
        return ExitCode::FAILURE;
    }
    let mut options = ImagingExecutionOptions::from_env();
    options.cancel = casars_cli::interrupt::token().clone();
    options.write_run_summary =
        std::env::var_os(RUN_SUMMARY_VARIABLE).is_some_and(|value| value == "1");
    task_front_end_with_options(options).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
