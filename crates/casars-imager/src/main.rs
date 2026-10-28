// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars-imager` — native CASA-compatible imaging, the `imager` task
//! provider, on the shared task front end.

use std::process::ExitCode;

/// Freed working buffers leave the process at once, so a run's footprint
/// follows the memory its phases admit.
#[global_allocator]
static ALLOCATOR: casa_imaging_application::ReturningAllocator =
    casa_imaging_application::ReturningAllocator;

fn main() -> ExitCode {
    casars_imager::task_front_end().main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
