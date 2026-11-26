// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task adapter for `pccor`.

use std::path::PathBuf;

use casa_calibration::pccor::{PccorError, PccorRequest, run_pccor};
use casa_contracts::{CasaFloat, CasaValue, FloatDtype};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;

#[derive(Serialize)]
struct Summary {
    output_table: PathBuf,
    rows_written: usize,
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let fringe = task.required_text("ff_table")?;
    let request = PccorRequest {
        vis: task.input_path("vis")?,
        output: task.path("pccor_caltable")?,
        refant: task.required_text("refant")?,
        timerange: task.required_text("timerange")?,
        scan: task.required_text("scan")?,
        spw: task.required_text("spw")?,
        antenna: task.required_text("antenna")?,
        cable: task.flag("cablecal_correction")?,
        fringe_table: (fringe != "none").then(|| PathBuf::from(fringe)),
    };
    let report = run_pccor(&request).map_err(|error| match error {
        PccorError::Selection(message) => TaskFailure::execution(message).at("selection"),
        PccorError::FringeTable(message) => TaskFailure::execution(message).at("ff_table"),
        PccorError::Index(message) => TaskFailure::execution(message).at("pccor_index"),
        _ => TaskFailure::execution(error.to_string()),
    })?;
    let vector = CasaValue::float64_vector;
    let antennas: Vec<(String, CasaValue)> = report
        .antennas
        .into_iter()
        .map(|antenna| {
            let windows: Vec<(String, CasaValue)> = antenna
                .windows
                .into_iter()
                .map(|window| {
                    let columns = window.time.len();
                    (
                        window.window.to_string(),
                        CasaValue::dict([
                            ("time", vector(window.time)),
                            ("scan", vector(window.scan)),
                            ("field_id", vector(window.field_id)),
                            ("phase", hand_array(columns, window.phase.into_iter())),
                            ("delay", hand_array(columns, window.delay.into_iter())),
                            ("ff_phase", vector(window.ff_phase)),
                            ("ff_delay", vector(window.ff_delay)),
                            ("pc_mean_phase", tone_array(&window.pc_mean_phase)),
                            ("pc_mean_delay", vector(window.pc_mean_delay)),
                            ("pc_refant_phase", tone_array(&window.pc_refant_phase)),
                            ("pc_refant_delay", vector(window.pc_refant_delay)),
                        ]),
                    )
                })
                .collect();
            (antenna.name, CasaValue::dict(windows))
        })
        .collect();
    TaskSuccess::new(
        CasaValue::dict(antennas),
        &Summary {
            output_table: request.output,
            rows_written: report.rows_written,
        },
    )
}

fn hand_array(columns: usize, values: impl Iterator<Item = f64>) -> CasaValue {
    CasaValue::FloatArray {
        dtype: FloatDtype::Float64,
        shape: vec![2, columns],
        data: values.map(CasaFloat).collect(),
    }
}

fn tone_array(hands: &[Vec<f64>; 2]) -> CasaValue {
    hand_array(
        hands[0].len(),
        hands[0]
            .iter()
            .zip(&hands[1])
            .flat_map(|(&first, &second)| [first, second]),
    )
}
