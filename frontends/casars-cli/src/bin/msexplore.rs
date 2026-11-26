// SPDX-License-Identifier: LGPL-3.0-or-later
//! `msexplore` — generic MeasurementSet plotting and export, the `msexplore`
//! and `plotms` task provider, on the shared task front end. Its first word
//! names the surface (`msexplore plotms ...`); `msexplore` runs otherwise.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let payload = std::env::var_os("CASARS_PLOT_PAYLOAD").map(PathBuf::from);
    casa_tasks::ms::msexplore::cli::task_front_end_with_plot_payload(payload).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
