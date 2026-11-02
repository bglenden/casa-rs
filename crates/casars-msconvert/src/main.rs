// SPDX-License-Identifier: LGPL-3.0-or-later
//! `msconvert` — convert between MeasurementSet v2 and MSv4, on the shared
//! task front end.

use std::process::ExitCode;

fn main() -> ExitCode {
    casars_msconvert::task_front_end().main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
