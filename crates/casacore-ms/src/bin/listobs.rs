// SPDX-License-Identifier: LGPL-3.0-or-later
//! `listobs` — render a CASA-style MeasurementSet summary.

fn main() {
    std::process::exit(casacore_ms::listobs::cli::run_env("listobs"));
}
