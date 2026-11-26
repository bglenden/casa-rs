// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task arguments and fractional Stokes return value for `polfromgain`.

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::CasaValue;

use casa_calibration::polfromgain::polarization_from_gain;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let input = task.input_path("tablein")?;
    let destination = task.path("caltable")?;
    let output = (!destination.as_os_str().is_empty()).then_some(destination.as_path());
    if let Some(path) = output
        && path.symlink_metadata().is_ok()
    {
        return Err(TaskFailure::validation(format!(
            "Output caltable={} exists. Choose another name or delete it.",
            path.display()
        ))
        .at("caltable"));
    }
    let source = polarization_from_gain(
        &vis,
        &input,
        output,
        task.number("paoffset")?,
        task.number("minpacov")?,
    )
    .map_err(TaskFailure::execution)?;
    let record = CasaValue::dict(source.iter().map(|(field, windows)| {
        let values = CasaValue::dict(windows.iter().map(|(window, stokes)| {
            let items = stokes.iter().copied().map(CasaValue::float).collect();
            (window.as_str(), CasaValue::List { items })
        }));
        (field.as_str(), values)
    }));
    TaskSuccess::new(record, &source)
}
