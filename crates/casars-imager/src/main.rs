// SPDX-License-Identifier: LGPL-3.0-or-later

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
    if let Err(error) = casars_imager::run_with_cli_args(args) {
        tracing::error!(casa.priority = "SEVERE", error = %error, "casars-imager failed");
        eprintln!("Error: {error}");
        let _ = logging_guard.flush();
        std::process::exit(1);
    }
    tracing::info!("casars-imager completed");
    if let Err(error) = logging_guard.flush() {
        eprintln!("Error: failed to flush logging: {error}");
        std::process::exit(1);
    }
}
