// SPDX-License-Identifier: LGPL-3.0-or-later
//! Adapt uvmodelfit's installed CASA task surface to the shared fitter.

use casa_components::ComponentList;
use casa_contracts::CasaValue;
use casa_ms::MsSelection;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::{VisibilityFitRequest, VisibilityFitShape, fit_visibility_component};

use crate::attachment::OpenedMs;

use super::multi_ms::partition_paths;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let root = OpenedMs::open(&path).map_err(execution)?;
    let parts = partition_paths(&path)?
        .unwrap_or_default()
        .iter()
        .map(|path| OpenedMs::open(path)?.initialize_calibrater(false, true, true))
        .collect::<Result<Vec<_>, _>>()
        .map_err(execution)?;
    let root = if parts.is_empty() {
        root.initialize_calibrater(false, true, true)
            .map_err(execution)?
    } else {
        root.reload().map_err(execution)?
    };
    let ms = &root.measurement_set;
    let selected = root.resolve(&selection(task)?).map_err(selection_error)?;
    if selected.selected_rows.is_empty() {
        return Err(selection_error(
            "MSSelectionNullSelection : The selected table has zero rows.",
        ));
    }
    let shape = VisibilityFitShape::parse(&task.text("comptype")?.unwrap_or_default())
        .map_err(failed_fit)?;
    let parameters = task.numbers("sourcepar")?;
    let vary = task.flags("varypar")?;
    let fitted = fit_visibility_component(
        ms,
        &selected,
        VisibilityFitRequest {
            shape,
            initial: &parameters,
            vary: &vary,
            iterations: task.integer("niter")?,
        },
    )
    .map_err(failed_fit)?;
    if let Some(path) = task.text("outfile")?.filter(|path| !path.is_empty()) {
        let list = ComponentList::from(vec![fitted.component]);
        list.write_table(task.workspace().join(path))
            .map_err(failed_fit)?;
    }
    for part in parts {
        part.release().map_err(execution)?;
    }
    root.release().map_err(execution)?;
    let mut report = TaskSuccess::new(CasaValue::None, &fitted.parameters)?
        .with_output_line("Formal errors assume the visibility weights are correctly scaled.");
    let labels = [
        ("I", " Jy"),
        ("x", " arcsec"),
        ("y", " arcsec"),
        ("a", " arcsec"),
        ("r", ""),
        ("p", " deg"),
    ];
    for ((value, error), (label, unit)) in fitted
        .parameters
        .into_iter()
        .zip(fitted.formal_errors)
        .zip(labels)
    {
        report = report.with_output_line(format!("{label} = {value:.9e} +/- {error:.9e}{unit}"));
    }
    Ok(report)
}

fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let select = task.flag("selectdata")?;
    let text = |name| -> Result<Option<String>, TaskFailure> {
        Ok(task.text(name)?.filter(|value| !value.is_empty()))
    };
    let optional = |name| {
        if select { text(name) } else { Ok(None) }
    };
    Ok(MsSelection {
        field: text("field")?,
        spw: text("spw")?,
        timerange: optional("timerange")?,
        uvrange: select
            .then(|| super::casa_read::uv_selector_text(task.value("uvrange")))
            .flatten(),
        antenna: optional("antenna")?,
        scan: optional("scan")?,
        msselect: optional("msselect")?,
        ..MsSelection::default()
    })
}

fn failed_fit(error: impl std::fmt::Display) -> TaskFailure {
    tracing::error!("visibility model fit: {error}");
    TaskFailure::execution("Error in Calibrater::modelfit.")
}

fn selection_error(error: impl std::fmt::Display) -> TaskFailure {
    execution(format!("Error in data selection specification: {error}"))
}

fn execution(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}
