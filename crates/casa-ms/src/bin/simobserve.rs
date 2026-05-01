// SPDX-License-Identifier: LGPL-3.0-or-later

fn main() {
    if let Err(error) = casa_ms::simulation_task::run_with_cli_args(std::env::args_os().skip(1)) {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}
