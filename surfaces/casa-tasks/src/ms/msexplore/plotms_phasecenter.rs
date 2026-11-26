// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parse and validate the requested PlotMS phase center for CLI tasks.

use crate::{TaskFailure, TaskInvocation};
use casa_ms::derived::parse_phase_direction;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frame::MeasFrame;

pub(super) fn requested(task: &TaskInvocation<'_>) -> Result<Option<String>, TaskFailure> {
    if task.flag("transform")? {
        Ok(task.text("phasecenter")?.filter(|value| !value.is_empty()))
    } else {
        Ok(None)
    }
}

pub(super) fn parse(value: &str) -> Result<MDirection, TaskFailure> {
    let target = parse_phase_direction(value).ok_or_else(|| {
        TaskFailure::validation(format!(
            "Cannot interpret phasecenter {value:?} as a direction"
        ))
        .at("phasecenter")
    })?;
    match target.refer() {
        DirectionRef::J2000 => Ok(target),
        DirectionRef::GALACTIC => target
            .convert_to(DirectionRef::J2000, &MeasFrame::default())
            .map_err(|error| TaskFailure::validation(error.to_string()).at("phasecenter")),
        _ => Err(TaskFailure::validation(
            "PlotMS phasecenter targets outside J2000 and Galactic are pending (#1032)",
        )
        .at("phasecenter")),
    }
}
