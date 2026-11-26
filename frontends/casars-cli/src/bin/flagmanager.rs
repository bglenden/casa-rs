// SPDX-License-Identifier: LGPL-3.0-or-later
//! `flagmanager` CLI shell.

use std::process::ExitCode;

fn main() -> ExitCode {
    casa_tasks::ms::flagmanager::task_front_end().main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
