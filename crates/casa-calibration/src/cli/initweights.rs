// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA weight initialization task surface.

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_tables::TableWriteLock;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::initweights::{Mode, initialize};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let mode = match task.text("wtmode")?.as_deref() {
        Some("nyq") => Mode::Nyquist,
        Some("sigma") => Mode::Sigma,
        Some("weight") => Mode::Weight,
        Some("ones") => Mode::Ones,
        Some("delwtsp") => Mode::DeleteWeightSpectrum,
        Some("delsigsp") => Mode::DeleteSigmaSpectrum,
        Some("tsys" | "tinttsys") => {
            return Err(
                TaskFailure::validation("Tsys weight initialization is tracked in #1524")
                    .at("wtmode"),
            );
        }
        _ => return Err(TaskFailure::validation("unknown initialization mode").at("wtmode")),
    };
    let spectrum = task.flag("dowtsp")?;
    let mut lock = TableWriteLock::acquire_to_reread(&path, 0)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    let mut ms = MeasurementSet::open(&path)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    initialize(&mut ms, mode, spectrum, &mut lock)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    drop(ms);
    lock.release()
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    TaskSuccess::new(CasaValue::None, &())
}
