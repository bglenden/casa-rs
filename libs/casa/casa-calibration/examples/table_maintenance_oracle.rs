// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native entry points for the paired table-maintenance oracle.

use std::{env, path::Path};

use casa_calibration::table::{
    CalibrationAccumulationRequest, accumulate_calibration, create_calibration_table,
    update_calibration_table,
};
use casa_ms::{MeasurementSet, MsSelection};

fn main() -> Result<(), String> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [action, source, output, partype, caltype, single] if action == "create" => {
            let ms = MeasurementSet::open(source).map_err(|error| error.to_string())?;
            create_calibration_table(&ms, Path::new(output), partype, caltype, single == "true")
        }
        [action, caltable] if action == "update" => update_calibration_table(Path::new(caltable)),
        [
            action,
            source,
            input,
            increment,
            output,
            field,
            calfield,
            interp,
            t,
            spwmap,
        ] if action == "accumulate" => {
            let ms = MeasurementSet::open(source).map_err(|error| error.to_string())?;
            let request = CalibrationAccumulationRequest {
                table_in: input.into(),
                incremental: increment.into(),
                table_out: output.into(),
                field: field.clone(),
                calibration_field: calfield.clone(),
                interpolation: interp.clone(),
                interval_seconds: t.parse::<f64>().map_err(|error| error.to_string())?,
                spectral_window_map: spwmap
                    .split(',')
                    .map(|v| v.parse::<i32>())
                    .collect::<Result<_, _>>()
                    .map_err(|error| error.to_string())?,
            };
            accumulate_calibration(&ms, &MsSelection::default(), &request)
        }

        _ => Err("expected create or update arguments".into()),
    }
}
