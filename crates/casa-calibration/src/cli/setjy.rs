// SPDX-License-Identifier: LGPL-3.0-or-later
//! Manual calibrator point models on the shared calibration task front end.

use std::collections::BTreeMap;

use casa_components::ComponentList;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::virtual_model::{
    VirtualModelPublication, selected_model_combinations, virtual_model_clear_ids,
};
use casa_ms::{
    MeasurementSet, MsSelection, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind,
};
use casa_provider_contracts::{
    CasaEntry, CasaFloat, CasaKey, CasaValue, FloatDtype, ParameterValue,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::ScalarValue;
use casa_types::measures::frequency::{CasaFrequencyFrame, FrequencyRef, MFrequency};
use casa_types::quanta::{Quantity, Unit};

use crate::attachment::OpenedMs;
use crate::ft::setjy::ManualFlux;
use crate::ft::write_model_scratch;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    if super::multi_ms::partition_paths(&path)?.is_some() {
        return Err(TaskFailure::validation("setjy on a Multi-MS is pending #1519").at("vis"));
    }
    let scratch = task.flag("usescratch")?;
    let mut opened = OpenedMs::open(&path).map_err(execution)?;
    let mut selection = selection(task)?;
    // The task first checks field, SPW and intent for virtual models.
    // The imager then applies time, scan and observation to the model selection.
    let time_filtered = ["timerange", "scan", "observation"].into_iter().any(
        |name| !matches!(task.value(name), Some(ParameterValue::String(value)) if value.is_empty()),
    );
    if !scratch && time_filtered {
        let initial = opened.resolve(&selection).map_err(execution)?;
        if initial.selected_rows.is_empty() {
            return empty_selection(task);
        }
    }
    add_time_selection(task, &mut selection)?;
    let mut selected = opened.resolve(&selection).map_err(execution)?;
    if selected.selected_rows.is_empty() {
        if !scratch && time_filtered {
            return TaskSuccess::new(flux_dictionary(Vec::new()), &());
        }
        return empty_selection(task);
    }
    let Some(manual) = manual_parameters(task)? else {
        return TaskSuccess::new(
            CasaValue::Dict {
                entries: Vec::new(),
            },
            &(),
        );
    };
    // Imager::sjy_make_visibilities selects all channels of each chosen SPW.
    selected.channel_selections.clear();
    let spws = requested_windows(&opened.measurement_set, selection.spw.as_deref())?;
    let (models, fluxes) = prepare(
        &opened.measurement_set,
        &selected,
        &manual,
        &spws,
        task.flag("scalebychan")?,
    )?;
    if scratch {
        opened = opened
            .initialize(VisibilityDataColumn::ModelData)
            .map_err(execution)?;
        write_model_scratch(
            &mut opened.measurement_set,
            &selected,
            &models,
            false,
            true,
            None,
            &mut opened.lock,
        )
        .map_err(execution)?;
    } else {
        // Fields can place records in different tables, depending on SOURCE_ID.
        for entries in
            models.chunk_by(|a, b| a.index_combination[0][0] == b.index_combination[0][0])
        {
            VirtualModelPublication::default()
                .replace_windows_holding(
                    &mut opened.measurement_set,
                    entries,
                    &mut opened.lock,
                    entries[0].index_combination[0][0],
                    &spws,
                )
                .map_err(execution)?;
        }
    }
    opened.release().map_err(execution)?;
    super::history::record_task(task, &path);
    TaskSuccess::new(fluxes, &())
}

fn empty_selection(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    if task.flag("ismms")? {
        return TaskSuccess::new(
            CasaValue::Dict {
                entries: Vec::new(),
            },
            &(),
        );
    }
    Err(execution(
        "No rows were selected. Please check your data selection",
    ))
}

fn manual_parameters(task: &TaskInvocation<'_>) -> Result<Option<ManualFlux>, TaskFailure> {
    let pending =
        |name| TaskFailure::validation("this setjy model configuration is pending #1519").at(name);
    if task.text("standard")?.as_deref() != Some("manual") {
        return Err(pending("standard"));
    }
    if task.flag("listmodels")? {
        return Err(pending("listmodels"));
    }
    if task.text("model")?.is_some_and(|model| !model.is_empty()) {
        return Err(pending("model"));
    }
    if !matches!(task.value("fluxdict"), Some(ParameterValue::Table(_))) {
        return Err(TaskFailure::validation("fluxdict must be a dictionary").at("fluxdict"));
    }
    let input = task.numbers("fluxdensity")?;
    if input.is_empty() || input.len() > 4 {
        return Err(
            TaskFailure::validation("manual setjy needs one to four Stokes flux values")
                .at("fluxdensity"),
        );
    }
    if input[0] < 0. {
        return Err(pending("fluxdensity"));
    }
    let mut flux = [0.; 4];
    flux[..input.len()].copy_from_slice(&input);
    let mut index = task.numbers("spix")?;
    if index.is_empty() {
        index.push(0.);
    }
    if index[0].abs() > 10. {
        let limit = if index[0] < -10. {
            "less than -10"
        } else {
            "greater than 10"
        };
        return Err(TaskFailure::validation(format!(
            "The spectral index is {limit}! Please check the spix parameter."
        ))
        .at("spix"));
    }
    let reference = task.text("reffreq")?.unwrap_or_else(|| "1GHz".into());
    let Some(reference) = reference_frequency(&reference) else {
        return Ok(None);
    };
    ManualFlux::new(
        flux,
        reference,
        index,
        task.numbers("polindex")?,
        task.numbers("polangle")?,
        task.number("rotmeas")?,
    )
    .map(Some)
    .map_err(execution)
}

fn reference_frequency(text: &str) -> Option<MFrequency> {
    let text = text.trim();
    let mut frame = FrequencyRef::LSRK;
    let mut quantity = text;
    if let Some((prefix, rest)) = text.split_once(char::is_whitespace)
        && let Ok(CasaFrequencyFrame::Defined(parsed)) = prefix.parse()
    {
        frame = parsed;
        quantity = rest.trim();
    }
    let quantity = quantity.parse::<Quantity>().ok()?;
    let hz = quantity.get_value_in(&Unit::new("Hz").expect("Hz")).ok()?;
    Some(MFrequency::new(hz, frame))
}

fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let text = |name| -> Result<Option<String>, TaskFailure> {
        Ok(task.text(name)?.filter(|value| !value.is_empty()))
    };
    Ok(MsSelection {
        field: text("field")?,
        spw: text("spw")?,
        intent: text("intent")?,
        ..MsSelection::default()
    })
}

fn add_time_selection(
    task: &TaskInvocation<'_>,
    selection: &mut MsSelection,
) -> Result<(), TaskFailure> {
    let text = |name, argument| match task.value(name) {
        Some(ParameterValue::String(s)) => Ok((!s.is_empty()).then(|| s.clone())),
        _ => Err(TaskFailure::validation(format!("argument {argument} must be a string")).at(name)),
    };
    selection.timerange = text("timerange", "_time")?;
    selection.scan = text("scan", "_scan")?;
    selection.observation = super::casa_read::selector_text(task.value("observation"));
    Ok(())
}

fn prepare(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    manual: &ManualFlux,
    spws: &[i32],
    by_channel: bool,
) -> Result<(Vec<VirtualModelEntry>, CasaValue), TaskFailure> {
    let mut groups = BTreeMap::<i32, BTreeMap<i32, Vec<usize>>>::new();
    for row in &selected.selected_rows {
        groups
            .entry(row.field_id)
            .or_default()
            .entry(row.spectral_window_id)
            .or_default()
            .push(row.row_index);
    }
    let engine = MsCalEngine::new(ms).map_err(execution)?;
    let field_table = ms.field().map_err(execution)?;
    let windows = ms.spectral_window().map_err(execution)?;
    let time = ms.main_table().column_accessor("TIME").map_err(execution)?;
    let ScalarValue::Float64(epoch) = time.scalar_cell(0).map_err(execution)? else {
        return Err(execution("expected a double-precision visibility epoch"));
    };
    let frame = engine
        .antenna_frame(*epoch, selected.selected_rows[0].antenna1_id as usize)
        .map_err(execution)?
        .with_position(engine.observatory_position().clone());
    let mut models = Vec::new();
    let mut returned = BTreeMap::new();
    for (field, windows_for_field) in groups.into_iter().rev() {
        let direction = engine
            .observation_direction_j2000(*epoch, field as usize)
            .map_err(execution)?;
        let frame = frame.clone().with_direction(direction.clone());
        let mut field_flux = BTreeMap::new();
        for &spw in spws {
            let all_hz = windows.chan_freq(spw as usize).map_err(execution)?;
            let frequency_frame = windows
                .frequency_reference(spw as usize)
                .map_err(execution)?;
            let num_channels = i32::try_from(all_hz.len()).map_err(execution)?;
            let planned = selected_frequencies(all_hz, frequency_frame, by_channel)?;
            let (component, [i, q, u, v]) = manual
                .component(direction.clone(), &planned, &frame)
                .map_err(execution)?;
            if let Some(rows) = windows_for_field.get(&spw) {
                let container = ComponentList::from(vec![component])
                    .to_record()
                    .map_err(execution)?;
                models.push(VirtualModelEntry {
                    kind: VirtualModelKind::ComponentList,
                    index_combination: selected_model_combinations(ms, rows)
                        .map_err(execution)?
                        .index_combination,
                    channel_selection: vec![[spw, 0, num_channels, 1]],
                    container,
                });
            }
            field_flux.insert(
                spw.to_string(),
                CasaValue::Dict {
                    entries: vec![entry(
                        "fluxd",
                        CasaValue::FloatArray {
                            dtype: FloatDtype::Float64,
                            shape: vec![4],
                            data: [i, q, u, v].map(CasaFloat).to_vec(),
                        },
                    )],
                },
            );
        }
        field_flux.insert(
            "fieldName".into(),
            CasaValue::String {
                value: field_table.name(field as usize).map_err(execution)?,
            },
        );
        returned.insert(
            field.to_string(),
            CasaValue::Dict {
                entries: field_flux
                    .into_iter()
                    .map(|(key, value)| entry(key, value))
                    .collect(),
            },
        );
    }
    Ok((
        models,
        flux_dictionary(
            returned
                .into_iter()
                .map(|(key, value)| entry(key, value))
                .collect(),
        ),
    ))
}

fn requested_windows(ms: &MeasurementSet, spw: Option<&str>) -> Result<Vec<i32>, TaskFailure> {
    if spw.is_some() {
        return virtual_model_clear_ids(ms, None, spw)
            .map(|(_, spws)| spws)
            .map_err(execution);
    }
    let count = ms.spectral_window().map_err(execution)?.row_count();
    Ok((0..i32::try_from(count).map_err(execution)?).collect())
}

fn flux_dictionary(mut entries: Vec<CasaEntry>) -> CasaValue {
    entries.push(entry(
        "format",
        CasaValue::String {
            value: "{field Id: {spw Id: {fluxd: [I,Q,U,V] in Jy}, 'fieldName':field name }}".into(),
        },
    ));
    CasaValue::Dict { entries }
}

fn selected_frequencies(
    mut frequencies: Vec<f64>,
    frame: FrequencyRef,
    by_channel: bool,
) -> Result<Vec<MFrequency>, TaskFailure> {
    if frequencies.is_empty() || frequencies.iter().any(|f| !f.is_finite() || *f <= 0.) {
        return Err(execution("setjy needs positive finite channel frequencies"));
    }
    if !by_channel {
        // casacore averages the two middle values only for even counts up to 100.
        let count = frequencies.len();
        let (_, &mut lower, upper) =
            frequencies.select_nth_unstable_by((count - 1) / 2, f64::total_cmp);
        let median = if count.is_multiple_of(2) && count <= 100 {
            0.5 * (lower + upper.iter().copied().fold(f64::INFINITY, f64::min))
        } else {
            lower
        };
        frequencies = vec![median];
    }
    Ok(frequencies
        .into_iter()
        .map(|hz| MFrequency::new(hz, frame))
        .collect())
}

fn entry(key: impl Into<String>, value: CasaValue) -> CasaEntry {
    CasaEntry {
        key: CasaKey::String(key.into()),
        value,
    }
}

fn execution(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}
