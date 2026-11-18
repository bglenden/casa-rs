// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA ft task selection, prediction and provenance.

use casa_components::ComponentList;
use casa_ms::{MeasurementSet, MsSelection, VirtualModelKind};
use casa_provider_contracts::CasaValue;
use casa_tables::TableWriteLock;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::ft::{model_entry, publish_models};
use crate::model_columns::initialize_model_column;
use crate::session::resolve_casa_calibration_selection;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let selection = selection(task)?;
    let scratch = task.flag("usescratch")?;
    if !scratch
        && [
            &selection.timerange,
            &selection.uvrange,
            &selection.antenna,
            &selection.scan,
            &selection.observation,
            &selection.intent,
        ]
        .into_iter()
        .any(Option::is_some)
    {
        return Err(TaskFailure::validation(
            "If any of the selection parameters timerange, uvrange, antenna, scan, observation or intent are not the default ones then usescratch must be set to True",
        )
        .at("usescratch"));
    }
    let path = task.input_path("vis")?;
    let mut lock = TableWriteLock::acquire_to_reread(&path, 0).map_err(execution)?;
    let mut ms = MeasurementSet::open(&path).map_err(execution)?;
    if scratch {
        lock = initialize_model_column(&mut ms, lock).map_err(execution)?;
    }
    let selected = resolve_casa_calibration_selection(&ms, &selection).map_err(execution)?;
    let models = task.texts("model").map_err(|_| {
        TaskFailure::validation(
            "The model image must be a string or a list of strings (or '' or [])",
        )
        .at("model")
    })?;
    let component = task.text("complist")?.unwrap_or_default();
    if (models.is_empty() || models == [""]) && component.is_empty() {
        return Err(TaskFailure::validation(
            "Please specify a model image or component list to ft",
        )
        .at("model"));
    }
    for model in &models {
        if !model.is_empty() && !task.workspace().join(model).exists() {
            return Err(
                TaskFailure::validation(format!("Model image {model} cannot be found")).at("model"),
            );
        }
    }
    if !component.is_empty() && !task.workspace().join(&component).exists() {
        return Err(
            TaskFailure::validation(format!("Componentlist {component} cannot be found"))
                .at("complist"),
        );
    }
    let terms = task.integer("nterms")?;
    validate_terms(terms, models.len())?;
    let incremental = task.flag("incremental")?;
    let mut entries = Vec::new();
    if !selected.selected_rows.is_empty() {
        if !component.is_empty() && !incremental {
            let list =
                ComponentList::read_table(task.workspace().join(&component)).map_err(execution)?;
            let container = list.to_record().map_err(execution)?;
            entries.push(
                model_entry(&ms, &selected, VirtualModelKind::ComponentList, container)
                    .map_err(execution)?,
            );
        }
        if models.iter().any(|name| !name.is_empty()) {
            return Err(
                TaskFailure::execution("Image model prediction is pending in #1520").at("model"),
            );
        }
        publish_models(
            &mut ms,
            &selected,
            &entries,
            scratch,
            incremental,
            &mut lock,
        )
        .map_err(execution)?;
    }
    drop(ms);
    lock.release().map_err(execution)?;
    super::history::record_task(task, &path);
    TaskSuccess::new(CasaValue::None, &())
}

fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let text = |name| {
        task.text(name)
            .map(|value| value.filter(|value| !value.is_empty()))
    };
    Ok(MsSelection {
        field: text("field")?,
        spw: text("spw")?,
        timerange: text("timerange")?,
        uvrange: super::casa_read::selector_text(task.value("uvrange")),
        antenna: text("antenna")?,
        scan: text("scan")?,
        observation: super::casa_read::selector_text(task.value("observation")),
        intent: text("intent")?,
        ..MsSelection::default()
    })
}

fn validate_terms(terms: i64, images: usize) -> Result<(), TaskFailure> {
    if terms < 1 {
        return Err(
            TaskFailure::validation("nterms must be greater than or equal to 1").at("nterms"),
        );
    }
    if terms > 1 && images != terms as usize {
        return Err(TaskFailure::validation(
            "For nterms>1, please provide a list of nterms model-image names",
        )
        .at("model"));
    }
    Ok(())
}

fn execution(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}
