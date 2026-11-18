// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA ft task selection, prediction and provenance.

use casa_components::ComponentList;
use casa_ms::virtual_model::put_virtual_model_holding;
use casa_ms::{MeasurementSet, MsSelection, ResolvedMsSelection, VirtualModelKind};
use casa_provider_contracts::CasaValue;
use casa_tables::TableWriteLock;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::quanta::Quantity;

use crate::ft::image::{ImageModel, TaylorTerm, prepare_image, reference_frequency};
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
    if !selected.selected_rows.is_empty() {
        let options = Prediction {
            scratch,
            incremental: task.flag("incremental")?,
            terms,
        };
        predict(
            task,
            &mut ms,
            &selected,
            (&models, &component),
            options,
            &mut lock,
        )?;
    }
    drop(ms);
    lock.release().map_err(execution)?;
    super::history::record_task(task, &path);
    TaskSuccess::new(CasaValue::None, &())
}

#[derive(Clone, Copy)]
struct Prediction {
    scratch: bool,
    incremental: bool,
    terms: i64,
}

fn predict(
    task: &TaskInvocation<'_>,
    ms: &mut MeasurementSet,
    selected: &ResolvedMsSelection,
    (models, component): (&[String], &str),
    options: Prediction,
    lock: &mut TableWriteLock,
) -> Result<(), TaskFailure> {
    let mut entries = Vec::new();
    if !component.is_empty() && !options.incremental {
        let list =
            ComponentList::read_table(task.workspace().join(component)).map_err(execution)?;
        let container = list.to_record().map_err(execution)?;
        entries.push(
            model_entry(ms, selected, VirtualModelKind::ComponentList, container)
                .map_err(execution)?,
        );
    }
    let mut images = image_models(task, ms, selected, models, options, entries.is_empty())?;
    if options.scratch {
        entries.extend(images.iter().map(|image| image.entry.clone()));
        publish_models(ms, selected, &entries, true, options.incremental, lock)
            .map_err(execution)?;
    } else {
        publish_models(ms, selected, &entries, false, options.incremental, lock)
            .map_err(execution)?;
        for (index, model) in images.iter_mut().enumerate() {
            put_virtual_model_holding(ms, &model.entry, index > 0 || options.incremental, lock)
                .map_err(execution)?;
            model.retain();
        }
    }
    Ok(())
}

fn image_models(
    task: &TaskInvocation<'_>,
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    models: &[String],
    options: Prediction,
    no_components: bool,
) -> Result<Vec<ImageModel>, TaskFailure> {
    let terms = options.terms;
    let zero = options.scratch && !options.incremental && no_components;
    let reference = if terms > 1 {
        let quantity = task.text("reffreq")?.unwrap_or_default();
        let explicit = if quantity.is_empty() {
            0.0
        } else {
            quantity
                .parse::<Quantity>()
                .map_err(|_| {
                    TaskFailure::execution(format!(
                        "In conversion of reffreq={quantity:?} to a numerical value"
                    ))
                    .at("reffreq")
                })?
                .get_si_value()
        };
        Some(if explicit == 0.0 {
            reference_frequency(&task.workspace().join(&models[0])).map_err(execution)?
        } else {
            explicit
        })
    } else {
        None
    };
    let mut images = Vec::new();
    for (index, path) in models
        .iter()
        .enumerate()
        .filter(|(_, path)| !path.is_empty())
    {
        let taylor = reference.map(|reference_hz| TaylorTerm {
            count: terms as i32,
            index: index as i32,
            reference_hz,
        });
        if let Some(model) = prepare_image(
            ms,
            selected,
            &task.workspace().join(path),
            taylor,
            zero && index == 0,
            options.scratch,
        )
        .map_err(execution)?
        {
            images.push(model);
        }
    }
    Ok(images)
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
