// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA weight initialization task surface.

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::CasaValue;
use std::path::Path;

use casa_calibration::apply::ApplyCalibrationTableSpec;
use casa_calibration::initweights::{Mode, initialize, initialize_tsys};

use casa_calibration::attachment::OpenedMs;

use super::multi_ms::partition_paths;

enum Weights {
    Ordinary(Mode),
    Tsys(Box<ApplyCalibrationTableSpec>, bool),
}

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let mode =
        match task.text("wtmode")?.as_deref() {
            Some(mode @ ("tsys" | "tinttsys")) => {
                Weights::Tsys(Box::new(read_tsys(task)?), mode == "tinttsys")
            }
            mode => Weights::Ordinary(mode.and_then(Mode::from_name).ok_or_else(|| {
                TaskFailure::validation("unknown initialization mode").at("wtmode")
            })?),
        };
    let spectrum = task.flag("dowtsp")?;
    if let Some(parts) = partition_paths(&path)? {
        for part in parts {
            run_one(&part, &mode, spectrum)?;
        }
    } else {
        run_one(&path, &mode, spectrum)?;
    }
    TaskSuccess::new(CasaValue::None, &())
}

fn run_one(path: &Path, mode: &Weights, spectrum: bool) -> Result<(), TaskFailure> {
    let mut state = OpenedMs::open(path)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    match mode {
        Weights::Ordinary(mode) => {
            initialize(&mut state.measurement_set, *mode, spectrum, &mut state.lock)
        }
        Weights::Tsys(spec, by_exposure) => initialize_tsys(
            &mut state.measurement_set,
            spec.as_ref().clone(),
            *by_exposure,
            spectrum,
            &mut state.lock,
        ),
    }
    .map_err(|error| TaskFailure::execution(error.to_string()))?;
    state
        .release()
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))
}

fn read_tsys(task: &TaskInvocation<'_>) -> Result<ApplyCalibrationTableSpec, TaskFailure> {
    let text = task.text("tsystable")?.unwrap_or_default();
    let path = task.workspace().join(&text);
    if text.is_empty() || !path.exists() {
        return Err(
            TaskFailure::execution(format!("Tsys calibration table {text} not found"))
                .at("tsystable.missing"),
        );
    }
    let mut spec = ApplyCalibrationTableSpec::new(path);
    spec.gainfield =
        super::read::parse_gainfield_value(&task.text("gainfield")?.unwrap_or_default());
    spec.interp = super::read::parse_interp_value(&task.text("interp")?.unwrap_or_default())
        .map_err(|message| TaskFailure::execution(message).at("interp"))?;
    spec.spwmap = task
        .integers("spwmap")?
        .into_iter()
        .map(|id| {
            i32::try_from(id).map_err(|_| {
                TaskFailure::execution("Tsys SPW index is outside the integer range").at("spwmap")
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(spec)
}
