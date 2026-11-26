// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA ft task selection, prediction and provenance.

use casa_components::ComponentList;
use casa_contracts::{CasaValue, NumpyScalarType, ParameterValue};
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::{
    MeasurementSet, MsSelection, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::quanta::Quantity;

use crate::ft::image::{ImageModel, TaylorTerm, prepare_image, reference_frequency};
use crate::ft::virtual_publication::VirtualFtPublication;
use crate::ft::{model_entry, write_model_scratch};

use crate::attachment::OpenedMs;

const MODEL: VisibilityDataColumn = VisibilityDataColumn::ModelData;

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
    let mut ms = OpenedMs::open(&path).map_err(execution)?;
    let mut parts = super::multi_ms::partition_paths(&path)?
        .unwrap_or_default()
        .iter()
        .map(|path| {
            let opened = OpenedMs::open(path).map_err(execution)?;
            if scratch {
                opened.initialize(MODEL).map_err(execution)
            } else {
                Ok(opened)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if scratch && parts.is_empty() {
        ms = ms.initialize(MODEL).map_err(execution)?;
    }
    let selected = ms.resolve(&selection).map_err(execution)?;
    let request = read_model(task, scratch)?;
    if !selected.selected_rows.is_empty() && request.images_are_tables(task) {
        if parts.is_empty() || !scratch {
            let models = read_models(task, &ms.measurement_set, &selected, &request)?;
            if scratch {
                write_scratch(&mut ms, &selected, &request, models)?;
            } else {
                write_virtual(&mut ms, &mut parts, &request, models)?;
            }
        } else {
            write_partition_scratch(task, parts.as_mut_slice(), selected, &request)?;
        }
    }
    for part in parts {
        part.release().map_err(execution)?;
    }
    ms.release().map_err(execution)?;
    super::history::record_task(task, &path);
    TaskSuccess::new(CasaValue::None, &())
}

struct ModelRequest {
    images: Vec<String>,
    component: String,
    scratch: bool,
    incremental: bool,
    terms: i64,
    reference_hz: f64,
}

impl ModelRequest {
    fn images_are_tables(&self, task: &TaskInvocation<'_>) -> bool {
        if self
            .images
            .iter()
            .filter(|name| !name.is_empty())
            .any(|name| !task.workspace().join(name).join("table.dat").is_file())
        {
            // The legacy imager's ft returns false for an existing FITS
            // file or another path that cannot name a CASA image table.
            tracing::warn!("ft requires a CASA image table");
            return false;
        }
        true
    }
}

fn read_model(task: &TaskInvocation<'_>, scratch: bool) -> Result<ModelRequest, TaskFailure> {
    let names = match task.value("model") {
        Some(model @ ParameterValue::String(_)) => std::slice::from_ref(model),
        Some(ParameterValue::Array(models)) => models,
        _ => {
            return Err(TaskFailure::validation(
                "The model image must be a string or a list of strings (or '' or [])",
            )
            .at("model"));
        }
    };
    let component = task.text("complist")?.unwrap_or_default();
    let empty_names =
        names.is_empty() || matches!(names, [ParameterValue::String(name)] if name.is_empty());
    if empty_names && component.is_empty() {
        return Err(TaskFailure::validation(
            "Please specify a model image or component list to ft",
        )
        .at("model"));
    }
    let mut models = Vec::with_capacity(names.len());
    for name in names {
        let ParameterValue::String(model) = name else {
            return Err(TaskFailure::validation("Model image names must be strings").at("model"));
        };
        if !model.is_empty() && !task.workspace().join(model).exists() {
            return Err(
                TaskFailure::validation(format!("Model image {model} cannot be found")).at("model"),
            );
        }
        models.push(model.clone());
    }
    if !component.is_empty() && !task.workspace().join(&component).exists() {
        return Err(
            TaskFailure::validation(format!("Componentlist {component} cannot be found"))
                .at("complist"),
        );
    }
    let terms = task.integer("nterms")?;
    validate_terms(terms, models.len())?;
    let mut reference_hz = 0.0;
    if terms > 1
        && let Some(quantity) = task.text("reffreq")?.filter(|text| !text.is_empty())
    {
        reference_hz = quantity
            .parse::<Quantity>()
            .map_err(|_| {
                TaskFailure::execution(format!(
                    "In conversion of reffreq={quantity:?} to a numerical value"
                ))
                .at("reffreq")
            })?
            .get_si_value();
    }
    if terms > 1 && reference_hz == 0.0 {
        if models[0].is_empty() {
            return Err(TaskFailure::execution("File name is empty").at("model"));
        }
        let path = task.workspace().join(&models[0]);
        reference_hz = reference_frequency(&path).map_err(|error| {
            tracing::error!(image = %path.display(), %error, "could not read Taylor reference frequency");
            TaskFailure::execution(format!("Unable to open image {}", models[0])).at("model")
        })?;
    }
    if matches!(
        task.session().given("incremental"),
        Some(ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Bool,
            ..
        })
    ) {
        return Err(
            TaskFailure::execution("in method 'imager_ft', argument 4 of type 'bool'")
                .at("incremental"),
        );
    }
    Ok(ModelRequest {
        images: models,
        component,
        scratch,
        incremental: task.flag("incremental")?,
        terms,
        reference_hz,
    })
}

type Models = (Vec<VirtualModelEntry>, Vec<ImageModel>);

fn read_models(
    task: &TaskInvocation<'_>,
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    request: &ModelRequest,
) -> Result<Models, TaskFailure> {
    let mut entries = Vec::new();
    if !request.component.is_empty() && !request.incremental {
        let list = ComponentList::read_table(task.workspace().join(&request.component))
            .map_err(execution)?;
        let container = list.to_record().map_err(execution)?;
        entries.push(
            model_entry(ms, selected, VirtualModelKind::ComponentList, container)
                .map_err(execution)?,
        );
    }
    let images = image_models(task, ms, selected, request, entries.is_empty())?;
    Ok((entries, images))
}

fn write_scratch(
    ms: &mut OpenedMs,
    selected: &ResolvedMsSelection,
    request: &ModelRequest,
    (mut entries, images): Models,
) -> Result<(), TaskFailure> {
    entries.extend(images.iter().map(|image| image.entry.clone()));
    write_model_scratch(
        &mut ms.measurement_set,
        selected,
        &entries,
        request.incremental,
        true,
        None,
        &mut ms.lock,
    )
    .map_err(execution)
}

fn write_partition_scratch(
    task: &TaskInvocation<'_>,
    parts: &mut [OpenedMs],
    mut selected: ResolvedMsSelection,
    request: &ModelRequest,
) -> Result<(), TaskFailure> {
    let rows = std::mem::take(&mut selected.selected_rows);
    let mut rows = rows.into_iter().peekable();
    let mut start = 0;
    for part in parts {
        let end = start + part.measurement_set.main_table().row_count();
        while let Some(row) = rows.next_if(|row| row.row_index < end) {
            selected
                .selected_rows
                .push(casa_ms::ResolvedMsSelectionRow {
                    row_index: row.row_index - start,
                    ..row
                });
        }
        if !selected.selected_rows.is_empty() {
            let models = read_models(task, &part.measurement_set, &selected, request)?;
            write_scratch(part, &selected, request, models)?;
            selected.selected_rows.clear();
        }
        start = end;
    }
    if rows.next().is_some() {
        return Err(TaskFailure::execution(
            "selected Multi-MS row is outside its partitions",
        ));
    }
    Ok(())
}

fn write_virtual(
    root: &mut OpenedMs,
    parts: &mut [OpenedMs],
    request: &ModelRequest,
    (entries, images): Models,
) -> Result<(), TaskFailure> {
    let tables = std::iter::once(&root.measurement_set)
        .chain(parts.iter().map(|part| &part.measurement_set));
    let mut publication = VirtualFtPublication::new(
        tables,
        entries,
        images,
        request.incremental,
        !parts.is_empty(),
    )
    .map_err(execution)?;
    publication
        .publish(
            std::iter::once(root)
                .chain(parts)
                .map(|opened| (&mut opened.measurement_set, &mut opened.lock)),
        )
        .map_err(execution)
}

fn image_models(
    task: &TaskInvocation<'_>,
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    request: &ModelRequest,
    no_components: bool,
) -> Result<Vec<ImageModel>, TaskFailure> {
    let models = &request.images;
    let terms = request.terms;
    let zero = request.scratch && !request.incremental && no_components;
    let reference = (terms > 1).then_some(request.reference_hz);
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
            request.scratch,
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
        uvrange: text("uvrange")?,
        antenna: text("antenna")?,
        scan: text("scan")?,
        observation: text("observation")?,
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
