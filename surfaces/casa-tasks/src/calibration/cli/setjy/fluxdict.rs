// SPDX-License-Identifier: LGPL-3.0-or-later
//! Field and window selections from a fluxscale return dictionary.

use std::collections::{BTreeMap, BTreeSet};

use crate::{TaskFailure, TaskInvocation};
use casa_contracts::{CasaValue, ParameterValue};
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_ms::{MsSelection, ResolvedMsSelection};

use super::{
    FluxParameters, add_time_selection, execution, flux_dictionary, model_files, prepare,
    publish_models, record_manual_spectrum, requested_windows,
};
use casa_calibration::attachment::OpenedMs;
use casa_calibration::ft::setjy::ManualFlux;
use casa_calibration::ft::setjy::standards::fixed::FixedScale;
use casa_calibration::ft::setjy::standards::{FluxCatalog, FluxScale};
use casa_calibration::interpolation::Curve;
use casa_ms::schema::main_table::VisibilityDataColumn;

type Record = BTreeMap<String, ParameterValue>;

pub(super) fn run(
    task: &TaskInvocation<'_>,
    mut opened: OpenedMs,
    parts: &mut [OpenedMs],
    selection: MsSelection,
    selected: &ResolvedMsSelection,
    scratch: bool,
    imager_history: &mut Vec<String>,
) -> Result<CasaValue, TaskFailure> {
    let image = model_files::task_image(task)?;
    let dictionary = FluxDictionary::new(task.value("fluxdict"))?;
    let selected_fields = selected
        .selected_rows
        .iter()
        .map(|row| row.field_id)
        .collect::<BTreeSet<_>>();
    let fields = dictionary.fields(&selected_fields, task.argument_text("fluxdict"))?;
    let selected_windows = selection
        .spw
        .as_deref()
        .map(|spw| {
            requested_windows(&opened.measurement_set, Some(spw))
                .map(|windows| windows.into_iter().collect::<BTreeSet<_>>())
        })
        .transpose()?;
    let by_channel = task.flag("scalebychan")?;
    let mut combined = Vec::new();
    let mut scratch_ready = !scratch;
    for (id, record) in fields {
        let ParameterValue::Table(record) = record else {
            return Err(invalid(format!("fluxdict field {id} must be a dictionary")));
        };
        let index = spectral_index(record, id)?;
        let windows = dictionary.windows(record, selected_windows.as_ref())?;
        if windows.is_empty() {
            append_values(&mut combined, flux_dictionary(Vec::new()));
            continue;
        }
        let (flux, hz) = dictionary.reference(record, &windows)?;
        let Some(model) = model(task, flux, hz, index)? else {
            continue;
        };
        let mut field_request = selection.clone();
        if !scratch {
            add_time_selection(task, &mut field_request)?;
        }
        field_request.field = Some(id.to_string());
        field_request.spw = Some(
            windows
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
        let mut field_rows = opened.resolve(&field_request).map_err(execution)?;
        if field_rows.selected_rows.is_empty() {
            append_values(&mut combined, flux_dictionary(Vec::new()));
            continue;
        }
        field_rows.channel_selections.clear();
        let spws = requested_windows(&opened.measurement_set, field_request.spw.as_deref())?;
        let super::PreparedModels {
            models: prepared,
            fluxes: values,
            manual_samples,
            ..
        } = prepare(
            &opened.measurement_set,
            &field_rows,
            &model,
            &spws,
            by_channel,
            image.as_deref().map(|path| (path, scratch)),
            imager_history,
        )?;
        if !scratch_ready {
            opened = opened
                .initialize(VisibilityDataColumn::ModelData)
                .map_err(execution)?;
            scratch_ready = true;
        }
        publish_models(
            &mut opened,
            parts,
            &field_rows,
            prepared,
            &spws,
            scratch,
            false,
        )?;
        record_manual_spectrum(imager_history, &manual_samples);
        append_values(&mut combined, values);
    }
    opened.release().map_err(execution)?;
    Ok(CasaValue::Dict { entries: combined })
}

fn append_values(combined: &mut Vec<casa_contracts::CasaEntry>, values: CasaValue) {
    let CasaValue::Dict { entries } = values else {
        unreachable!()
    };
    for entry in entries {
        if let Some(existing) = combined
            .iter_mut()
            .find(|existing| existing.key == entry.key)
        {
            existing.value = entry.value;
        } else {
            combined.push(entry);
        }
    }
}

struct FluxDictionary<'a> {
    data: &'a Record,
    windows: Vec<i32>,
    frequencies: Vec<f64>,
}

impl<'a> FluxDictionary<'a> {
    fn new(value: Option<&'a ParameterValue>) -> Result<Self, TaskFailure> {
        let Some(ParameterValue::Table(data)) = value else {
            return Err(invalid("fluxdict must be a dictionary"));
        };
        if data.is_empty() {
            return Err(invalid("fluxdict is empty"));
        }
        let missing = ["freq", "spwID"]
            .into_iter()
            .filter(|&key| !data.contains_key(key))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(invalid(format!(
                "Input fluxdict is missing keywords:{} ",
                missing.join(" ")
            )));
        }
        let windows = numbers(&data["spwID"])
            .ok_or_else(|| invalid("fluxdict spwID must be an array of integers"))?
            .into_iter()
            .map(|id| {
                if id.fract() == 0. && (0. ..=f64::from(i32::MAX)).contains(&id) {
                    Ok(id as i32)
                } else {
                    Err(invalid(
                        "fluxdict spwID must contain nonnegative window IDs",
                    ))
                }
            })
            .collect::<Result<_, _>>()?;
        let frequencies = numbers(&data["freq"])
            .ok_or_else(|| invalid("fluxdict freq must be an array of frequencies"))?;
        Ok(Self {
            data,
            windows,
            frequencies,
        })
    }

    fn fields(
        &self,
        selected: &BTreeSet<i32>,
        order: Option<&str>,
    ) -> Result<Vec<(i32, &'a ParameterValue)>, TaskFailure> {
        let supplied = order.and_then(|text| serde_json::from_str::<Vec<String>>(text).ok());
        let mut visited = BTreeSet::new();
        let mut fields = Vec::new();
        for key in supplied.iter().flatten().chain(self.data.keys()) {
            let Ok(id) = key.parse::<i32>() else { continue };
            if !selected.contains(&id) || !visited.insert(key) {
                continue;
            }
            let Some(value) = self.data.get(key) else {
                continue;
            };
            fields.push((id, value));
        }
        if fields.is_empty() {
            return Err(invalid(
                "No field was found in fluxdict for the given data selection",
            ));
        }
        Ok(fields)
    }

    fn windows(
        &self,
        field: &Record,
        requested: Option<&BTreeSet<i32>>,
    ) -> Result<Vec<i32>, TaskFailure> {
        let mut valid = Vec::new();
        for &window in &self.windows {
            if requested.is_some_and(|ids| !ids.contains(&window)) {
                continue;
            }
            let flux = window_flux(field, window)?;
            if flux[0] > -1. {
                valid.push(window);
            }
        }
        Ok(valid)
    }

    fn reference(&self, field: &Record, windows: &[i32]) -> Result<([f64; 4], f64), TaskFailure> {
        let fitted = field
            .get("fitFluxd")
            .and_then(number)
            .ok_or_else(|| invalid("fluxdict fitFluxd must be numeric"))?;
        if fitted != 0. {
            let hz = field
                .get("fitRefFreq")
                .and_then(number)
                .ok_or_else(|| invalid("fluxdict fitRefFreq must be numeric"))?;
            return Ok(([fitted, 0., 0., 0.], hz));
        }
        let first = *windows
            .first()
            .ok_or_else(|| invalid("list index out of range"))?;
        let hz = *self
            .frequencies
            .get(first as usize)
            .ok_or_else(|| invalid(format!("fluxdict freq has no entry for window {first}")))?;
        Ok((window_flux(field, first)?, hz))
    }
}

fn spectral_index(field: &Record, id: i32) -> Result<Vec<f64>, TaskFailure> {
    let coefficients = field
        .get("spidx")
        .and_then(numbers)
        .ok_or_else(|| invalid("fluxdict spidx must be an array of numbers"))?;
    if coefficients.is_empty() {
        return Err(invalid("fluxdict spidx must include its intercept"));
    }
    let alpha = coefficients.get(1).copied().unwrap_or_default();
    if alpha.abs() > 10. {
        let relation = if alpha < -10. {
            "less than -10"
        } else {
            "greater than 10"
        };
        return Err(invalid(format!(
            "Field {id}: the spectral index is {relation}!"
        )));
    }
    Ok(coefficients.into_iter().skip(1).collect())
}

fn window_flux(field: &Record, window: i32) -> Result<[f64; 4], TaskFailure> {
    let Some(ParameterValue::Table(record)) = field.get(&window.to_string()) else {
        return Err(invalid(format!(
            "fluxdict has no record for window {window}"
        )));
    };
    let values = record
        .get("fluxd")
        .and_then(numbers)
        .filter(|values| !values.is_empty() && values.len() <= 4)
        .ok_or_else(|| {
            invalid(format!(
                "fluxdict window {window} must supply one to four Stokes flux values"
            ))
        })?;
    let mut flux = [0.; 4];
    flux[..values.len()].copy_from_slice(&values);
    Ok(flux)
}

fn model(
    task: &TaskInvocation<'_>,
    flux: [f64; 4],
    hz: f64,
    index: Vec<f64>,
) -> Result<Option<FluxParameters>, TaskFailure> {
    if flux[0] < 0. {
        let catalog =
            FluxCatalog::open(FluxScale::Fixed(FixedScale::PerleyButler2010)).map_err(execution)?;
        return Ok(Some(FluxParameters::Standard {
            catalog,
            interpolation: Curve::Nearest,
        }));
    }
    if !hz.is_finite() || hz <= 0. {
        return Ok(None);
    }
    let model = ManualFlux::new(
        flux,
        MFrequency::new(hz, FrequencyRef::LSRK),
        index,
        task.numbers("polindex")?,
        task.numbers("polangle")?,
        task.number("rotmeas")?,
    )
    .map_err(execution)?;
    Ok(Some(FluxParameters::Manual(model)))
}

fn numbers(value: &ParameterValue) -> Option<Vec<f64>> {
    match value {
        ParameterValue::Array(values) => values.iter().map(number).collect(),
        ParameterValue::EmptyArray { .. } => Some(Vec::new()),
        _ => None,
    }
}

fn number(value: &ParameterValue) -> Option<f64> {
    match value {
        ParameterValue::Float(value) => Some(*value),
        ParameterValue::Integer(value) => Some(*value as f64),
        ParameterValue::NumpyScalar { value, .. } | ParameterValue::ScalarArray(value) => {
            number(value)
        }
        _ => None,
    }
}

fn invalid(message: impl Into<String>) -> TaskFailure {
    TaskFailure::execution(message).at("fluxdict")
}
