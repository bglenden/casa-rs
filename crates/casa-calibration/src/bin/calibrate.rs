// SPDX-License-Identifier: LGPL-3.0-or-later
//! `calibrate` — the calibration task provider, hosting `calibrate`,
//! `applycal`, `gaincal`, `bandpass`, `fluxscale` and `gencal` on the shared
//! task front end. Its first word names the surface (`calibrate gaincal
//! ...`); `calibrate` runs otherwise.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1).peekable();
    if args.peek().is_some_and(|arg| arg == "plotms") {
        let payload = std::env::var_os("CASARS_PLOT_PAYLOAD").map(PathBuf::from);
        return casa_ms::msexplore::cli::task_front_end_with_plot_sources(
            payload,
            Some(casa_calibration::plotms::load_calibration),
            Some(casa_calibration::plotms::load_models),
        )
        .main(
            args,
            casa_task_runtime::CasaLoggingConfig::from_env(),
            std::env::set_current_dir,
            &mut std::io::stdout(),
            std::io::stderr(),
        );
    }
    casa_calibration::task_front_end().main(
        args,
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
