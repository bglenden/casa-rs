// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` CLI shell.

use std::ffi::OsString;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let default_logging = !casa_task_runtime::CasaLoggingConfig::output_was_configured(&args);
    casa_tasks::images::imexplore::imexplore_front_end(default_logging).main(
        args,
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
