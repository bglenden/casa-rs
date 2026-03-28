// SPDX-License-Identifier: LGPL-3.0-or-later
//! `msexplore` — generic MeasurementSet plotting and export.

fn main() {
    std::process::exit(casacore_ms::msexplore::cli::run_env("msexplore"));
}
