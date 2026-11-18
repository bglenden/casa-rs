// SPDX-License-Identifier: LGPL-3.0-or-later
//! Adapt uvmodelfit's installed CASA task surface to the shared fitter.

use casa_components::ComponentList;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::{MeasurementSet, MsSelection};
use casa_provider_contracts::CasaValue;
use casa_tables::TableWriteLock;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::model_columns::initialize_scratch_column;
use crate::session::resolve_casa_calibration_selection;
use crate::uvmodelfit::{ComponentType, FitRequest, fit_visibility_component};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let lock = TableWriteLock::acquire_to_reread(&path, 0).map_err(execution)?;
    let mut ms = MeasurementSet::open(&path).map_err(execution)?;
    let lock = initialize_scratch_column(&mut ms, VisibilityDataColumn::CorrectedData, lock)
        .map_err(execution)?;
    let selected = resolve_casa_calibration_selection(&ms, &selection(task)?).map_err(execution)?;
    let shape =
        ComponentType::parse(&task.text("comptype")?.unwrap_or_default()).map_err(failed_fit)?;
    let parameters = task.numbers("sourcepar")?;
    let vary = task.flags("varypar")?;
    let fitted = fit_visibility_component(
        &ms,
        &selected,
        FitRequest {
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
    drop(ms);
    lock.release().map_err(execution)?;
    TaskSuccess::new(CasaValue::None, &fitted.parameters)
}

fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let select = task.flag("selectdata")?;
    let text = |name| {
        let value = super::casa_read::selector_text(task.value(name))?;
        let stripped = value
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .unwrap_or(&value);
        (!stripped.is_empty()).then(|| stripped.to_owned())
    };
    let optional = |name| select.then(|| text(name)).flatten();
    Ok(MsSelection {
        field: text("field"),
        spw: text("spw"),
        timerange: optional("timerange"),
        uvrange: optional("uvrange"),
        antenna: optional("antenna"),
        scan: optional("scan"),
        msselect: optional("msselect"),
        ..MsSelection::default()
    })
}

fn failed_fit(error: impl std::fmt::Display) -> TaskFailure {
    tracing::error!("visibility model fit: {error}");
    TaskFailure::execution("Error in Calibrater::modelfit.")
}

fn execution(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}
