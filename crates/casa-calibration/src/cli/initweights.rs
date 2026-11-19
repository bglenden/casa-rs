// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA weight initialization task surface.

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_tables::TableWriteLock;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::ApplyCalibrationTableSpec;
use crate::initweights::{Mode, initialize, initialize_tsys};

enum Weights {
    Ordinary(Mode),
    Tsys(ApplyCalibrationTableSpec, bool),
}

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let mode = match task.text("wtmode")?.as_deref() {
        Some("nyq") => Weights::Ordinary(Mode::Nyquist),
        Some("sigma") => Weights::Ordinary(Mode::Sigma),
        Some("weight") => Weights::Ordinary(Mode::Weight),
        Some("ones") => Weights::Ordinary(Mode::Ones),
        Some("delwtsp") => Weights::Ordinary(Mode::DeleteWeightSpectrum),
        Some("delsigsp") => Weights::Ordinary(Mode::DeleteSigmaSpectrum),
        Some(mode @ ("tsys" | "tinttsys")) => Weights::Tsys(read_tsys(task)?, mode == "tinttsys"),
        _ => return Err(TaskFailure::validation("unknown initialization mode").at("wtmode")),
    };
    let spectrum = task.flag("dowtsp")?;
    let mut lock = TableWriteLock::acquire_to_reread(&path, 0)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    let mut ms = MeasurementSet::open(&path)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    match mode {
        Weights::Ordinary(mode) => initialize(&mut ms, mode, spectrum, &mut lock),
        Weights::Tsys(spec, by_exposure) => {
            initialize_tsys(&mut ms, spec, by_exposure, spectrum, &mut lock)
        }
    }
    .map_err(|error| TaskFailure::execution(error.to_string()))?;
    drop(ms);
    lock.release()
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    TaskSuccess::new(CasaValue::None, &())
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
