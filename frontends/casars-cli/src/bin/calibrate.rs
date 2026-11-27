// SPDX-License-Identifier: LGPL-3.0-or-later
//! `calibrate` — the calibration task provider CLI shell.

use std::path::PathBuf;
use std::process::ExitCode;

use casars_cli::logging::{CasaLoggingConfig, run_front_end};

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1).peekable();
    if args.peek().is_some_and(|arg| arg == "plotms") {
        let payload = std::env::var_os("CASARS_PLOT_PAYLOAD").map(PathBuf::from);
        let front_end = casa_tasks::ms::msexplore::cli::task_front_end_with_plot_sources(
            payload,
            Some(casa_tasks::calibration::plotms::load_calibration),
            Some(casa_tasks::calibration::plotms::load_models),
        );
        return run_front_end(
            &front_end,
            args,
            CasaLoggingConfig::from_env(),
            std::env::set_current_dir,
            &mut std::io::stdout(),
            std::io::stderr(),
        );
    }
    let front_end = casa_tasks::calibration::task_front_end();
    run_front_end(
        &front_end,
        args,
        CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
