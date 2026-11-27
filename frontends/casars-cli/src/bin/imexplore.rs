// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` CLI shell.

use std::ffi::OsString;
use std::process::ExitCode;

use casars_cli::logging::{CasaLoggingConfig, run_front_end};

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let default_logging = !CasaLoggingConfig::output_was_configured(&args);
    let front_end = casa_tasks::images::imexplore::imexplore_front_end(default_logging);
    run_front_end(
        &front_end,
        args,
        CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
