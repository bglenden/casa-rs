// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's total-power imaging task, with native Rust computation.
fn main() -> std::process::ExitCode {
    if let Err(error) = casars_imager::interrupt::install() {
        eprintln!("tsdimaging: install interrupt handler: {error}");
        return std::process::ExitCode::FAILURE;
    }
    casars_imager::single_dish::tsdimaging_front_end().main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}
