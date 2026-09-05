// SPDX-License-Identifier: LGPL-3.0-or-later

//! Rewrite one MeasurementSet through the canonical casa-rs storage policy.

use casa_ms::{MsTransformRequest, TransformDataColumn, mstransform, selection::MsSelection};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let source = arguments
        .next()
        .ok_or("usage: rewrite_measurement_set SOURCE.ms DESTINATION.ms")?;
    let destination = arguments
        .next()
        .ok_or("usage: rewrite_measurement_set SOURCE.ms DESTINATION.ms")?;
    if arguments.next().is_some() {
        return Err("usage: rewrite_measurement_set SOURCE.ms DESTINATION.ms".into());
    }
    mstransform(&MsTransformRequest {
        input_ms: source.into(),
        output_ms: destination.clone().into(),
        spw: String::new(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })?;
    println!("{}", std::path::Path::new(&destination).display());
    Ok(())
}
