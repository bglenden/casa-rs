// SPDX-License-Identifier: LGPL-3.0-or-later
//! `msinfo` — alias for the `listobs` MeasurementSet summary CLI.
//!
//! This binary is kept for continuity with earlier `casa-rs` releases. New
//! callers should prefer [`listobs`](crate::listobs), which provides the same
//! summary core with explicit output-format selection.

fn main() {
    std::process::exit(casacore_ms::listobs::cli::run_env("msinfo"));
}
