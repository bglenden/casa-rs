// SPDX-License-Identifier: LGPL-3.0-or-later

//! Explicitly initialize the versioned imaging-owner manifest on one MeasurementSet.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: initialize_imaging_owner MEASUREMENT_SET")?;
    let identity = casa_ms::initialize_measurement_set_owner_manifest(path)?;
    println!("{identity}");
    Ok(())
}
