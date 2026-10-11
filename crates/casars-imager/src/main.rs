// SPDX-License-Identifier: LGPL-3.0-or-later

/// Freed working buffers leave the process at once, so a run's footprint
/// follows the memory its phases admit.
#[global_allocator]
static ALLOCATOR: casa_imaging_application::ReturningAllocator =
    casa_imaging_application::ReturningAllocator;

fn main() {
    let (logging_guard, args) =
        match casa_logging::init_global_from_env_and_args(std::env::args_os().skip(1)) {
            Ok((guard, args)) => (guard, args),
            Err(error) => {
                eprintln!("Error: failed to initialize logging: {error}");
                std::process::exit(1);
            }
        };
    tracing::info!("casars-imager started");
    if let Err(error) = casars_imager::interrupt::install() {
        eprintln!("Error: failed to install the interrupt handler: {error}");
        std::process::exit(1);
    }
    if let Err(error) = casars_imager::run_with_cli_args(args) {
        // 128 + SIGINT, the shell's status for an interrupted command.
        let interrupted = casars_imager::interrupt::token().is_cancelled();
        if interrupted {
            tracing::warn!("casars-imager interrupted; nothing was published");
        } else {
            tracing::error!(casa.priority = "SEVERE", error = %error, "casars-imager failed");
        }
        eprintln!("Error: {error}");
        let _ = logging_guard.flush();
        std::process::exit(if interrupted { 130 } else { 1 });
    }
    tracing::info!("casars-imager completed");
    if let Err(error) = logging_guard.flush() {
        eprintln!("Error: failed to flush logging: {error}");
        std::process::exit(1);
    }
}
