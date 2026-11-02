// SPDX-License-Identifier: LGPL-3.0-or-later
//! `msexplore` — generic MeasurementSet plotting and export, the `msexplore`
//! and `plotms` task provider, on the shared task front end. Its first word
//! names the surface (`msexplore plotms ...`); `msexplore` runs otherwise.

use std::process::ExitCode;

fn main() -> ExitCode {
    casa_ms::msexplore::cli::task_front_end().main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
