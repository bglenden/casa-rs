// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagmanager` CLI shell.

use std::process::ExitCode;

fn main() -> ExitCode {
    let front_end = casa_tasks::ms::flagmanager::task_front_end();
    casars_cli::logging::run_front_end(
        &front_end,
        std::env::args_os().skip(1),
        casars_cli::logging::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
