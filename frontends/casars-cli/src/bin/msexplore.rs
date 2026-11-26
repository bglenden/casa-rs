// SPDX-License-Identifier: LGPL-3.0-or-later
//! `msexplore` — generic MeasurementSet plotting and export, the `msexplore`
//! and `plotms` task provider, on the shared task front end. Its first word
//! names the surface (`msexplore plotms ...`); `msexplore` runs otherwise.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let payload = std::env::var_os("CASARS_PLOT_PAYLOAD").map(PathBuf::from);
    let front_end = casa_tasks::ms::msexplore::cli::task_front_end_with_plot_payload(payload);
    casars_cli::logging::run_front_end(
        &front_end,
        std::env::args_os().skip(1),
        casars_cli::logging::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
