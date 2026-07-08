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
    tracing::info!("importvla started");
    let code = casa_vla::cli::run_with_cli_args("importvla", args);
    if code == 0 {
        tracing::info!("importvla completed");
    } else {
        tracing::error!(
            casa.priority = "SEVERE",
            exit_code = code,
            "importvla failed"
        );
    }
    if let Err(error) = logging_guard.flush() {
        eprintln!("Error: failed to flush logging: {error}");
        std::process::exit(1);
    }
    std::process::exit(code);
}
