// SPDX-License-Identifier: LGPL-3.0-or-later
//! `tsdimaging` — CASA's total-power imaging task CLI shell.

use std::process::ExitCode;

fn main() -> ExitCode {
    if let Err(error) = casars_cli::interrupt::install() {
        eprintln!("tsdimaging: install interrupt handler: {error}");
        return ExitCode::FAILURE;
    }
    let front_end = casa_tasks::imaging::single_dish::tsdimaging_front_end_with_cancel(
        casars_cli::interrupt::token().clone(),
    );
    casars_cli::logging::run_front_end(
        &front_end,
        std::env::args_os().skip(1),
        casars_cli::logging::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
