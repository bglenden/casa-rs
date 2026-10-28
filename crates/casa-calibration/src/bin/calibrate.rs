// SPDX-License-Identifier: LGPL-3.0-or-later
//! `calibrate` — the calibration task provider, hosting `calibrate`,
//! `applycal`, `gaincal`, `bandpass`, `fluxscale` and `gencal` on the shared
//! task front end. Its first word names the surface (`calibrate gaincal
//! ...`); `calibrate` runs otherwise.

use std::process::ExitCode;

fn main() -> ExitCode {
    casa_calibration::task_front_end().main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
