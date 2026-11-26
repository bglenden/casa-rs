// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::TabularEdit;
use casa_coordinates::{
    Coordinate, CoordinateError, CoordinateModel, CoordinateSystem, CoordinateType,
};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Array2;
use numpy::{PyArrayDescrMethods, PyReadonlyArrayDyn, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::Variant;

use super::frame_metadata::{coordinate, missing};
use super::inspect::coordinate_type;
use super::{CoordinateState, PyCoordsys};

fn raised(message: impl ToString) -> PyErr {
    PyRuntimeError::new_err(message.to_string())
}

fn has_zero_row(matrix: &Array2<f64>) -> bool {
    matrix
        .rows()
        .into_iter()
        .any(|row| row.iter().all(|&value| value == 0.0))
}

fn singular_linear_transform() -> PyErr {
    raised("wcs wcsset_error: Linear transformation matrix is singular")
}

pub(in crate::tools) fn validate_restored_linear_coordinates(
    coordinates: &CoordinateSystem,
    record: &RecordValue,
) -> PyResult<()> {
    if !(0..coordinates.n_coordinates()).any(|index| {
        matches!(
            coordinates.coordinate(index),
            CoordinateModel::Linear(_) | CoordinateModel::System(_)
        )
    }) {
        return Ok(());
    }
    for (index, placement) in coordinates
        .coordinates_in_record(record)
        .map_err(raised)?
        .into_iter()
        .enumerate()
    {
        let mapped = coordinates.coordinate(index);
        let full;
        let model = if matches!(
            mapped,
            CoordinateModel::Linear(_) | CoordinateModel::System(_)
        ) && (placement.pixel_axes.len() != mapped.n_pixel_axes()
            || placement.world_axes.len() != mapped.n_world_axes())
        {
            // The compact model can omit the invalid axis.
            full = placement.model(record).map_err(raised)?;
            &full
        } else {
            mapped
        };
        match model {
            CoordinateModel::Linear(linear) => {
                if linear.increment().contains(&0.0) || has_zero_row(linear.pc_matrix()) {
                    return Err(singular_linear_transform());
                }
            }
            CoordinateModel::System(nested) => {
                if let Some(Value::Record(nested_record)) = record.get(&placement.field) {
                    validate_restored_linear_coordinates(nested, nested_record)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub(super) fn matrix_input(value: Option<&Bound<'_, PyAny>>) -> PyResult<Array2<f64>> {
    let invalid = || raised("The value array is not a double or integer array");
    let value = value.filter(|value| !value.is_none()).ok_or_else(invalid)?;
    let (mut shape, values) = if let Ok(array) = value.cast::<PyUntypedArray>() {
        if array.ndim() == 0 {
            return Err(raised("The value array is empty"));
        }
        if !matches!(array.dtype().kind(), b'i' | b'u' | b'f') {
            return Err(invalid());
        }
        let native = value.call_method1("astype", ("float64",))?;
        let native = native.extract::<PyReadonlyArrayDyn<'_, f64>>()?;
        (
            native.shape().to_vec(),
            native.as_array().t().iter().copied().collect(),
        )
    } else {
        match value.extract::<Variant>()? {
            Variant::Numbers(values) => (vec![values.len()], values),
            Variant::Array { shape, values } => (shape, values),
            Variant::Complex(_) => {
                return Err(raised("The value array is empty"));
            }
            _ => return Err(invalid()),
        }
    };
    if shape.len() == 1 {
        shape.push(1);
    }
    let [rows, cols] = shape.as_slice() else {
        return Err(raised(
            "The transform matrix must have at most two dimensions",
        ));
    };
    // CASA's displayed transform is the transpose of its native WCS PC.
    Array2::from_shape_vec((*cols, *rows), values).map_err(raised)
}

fn write_fields(target: &mut RecordValue, source: &RecordValue, fields: &[&str]) -> PyResult<()> {
    for &name in fields {
        let value = source
            .get(name)
            .ok_or_else(|| raised(format!("Missing coordinate field {name}")))?;
        target.upsert(name, value.clone());
    }
    Ok(())
}

fn edit_record(original: &RecordValue, changed: &CoordinateModel) -> PyResult<RecordValue> {
    let mut updated = original.clone();
    let native = changed.to_record();
    match changed {
        CoordinateModel::Spectral(spectral) => {
            let (key, fields) = if spectral.has_linear_mapping() {
                ("wcs", &["pc"][..])
            } else {
                (
                    "tabular",
                    &[
                        "pc",
                        "crval",
                        "cdelt",
                        "crpix",
                        "units",
                        "pixelvalues",
                        "worldvalues",
                    ][..],
                )
            };
            let Some(Value::Record(source)) = native.get(key) else {
                return Err(raised(format!("Missing spectral coordinate field {key}")));
            };
            if let Some(Value::Record(destination)) = updated.get(key) {
                let mut destination = destination.clone();
                write_fields(&mut destination, source, fields)?;
                updated.upsert(key, Value::Record(destination));
            } else {
                updated.upsert(key, Value::Record(source.clone()));
            }
        }
        CoordinateModel::Direction(_) => write_fields(&mut updated, &native, &["pc", "cdelt"])?,
        CoordinateModel::Tabular(_) => write_fields(
            &mut updated,
            &native,
            &["pc", "crval", "cdelt", "crpix", "worldvalues"],
        )?,
        _ => write_fields(&mut updated, &native, &["pc"])?,
    }
    Ok(updated)
}

pub(super) fn normalize_restored_matrix(
    original: &RecordValue,
    model: &CoordinateModel,
) -> PyResult<Option<RecordValue>> {
    let table = match model {
        CoordinateModel::Tabular(_) => original,
        CoordinateModel::Spectral(spectral) if !spectral.has_linear_mapping() => {
            recorded_coordinate(original, "tabular")?
        }
        _ => return Ok(None),
    };
    let matrix = match table.get("pc") {
        Some(Value::Array(ArrayValue::Float64(values))) => values.first().copied(),
        Some(Value::Array(ArrayValue::Float32(values))) => values.first().copied().map(f64::from),
        Some(Value::Scalar(ScalarValue::Float64(value))) => Some(*value),
        _ => None,
    };
    if matrix.is_some_and(|factor| factor != 1.0) {
        Ok(Some(edit_record(original, model)?))
    } else if matches!(model, CoordinateModel::Spectral(_)) {
        let edited = edit_record(original, model)?;
        let normalized = recorded_coordinate(&edited, "tabular")?;
        let changed = ["crpix", "crval", "cdelt"]
            .iter()
            .any(|field| table.get(field) != normalized.get(field));
        Ok(changed.then_some(edited))
    } else if matches!(model, CoordinateModel::Tabular(table) if !table.pixel_values().is_empty()) {
        let edited = edit_record(original, model)?;
        Ok((&edited != original).then_some(edited))
    } else {
        Ok(None)
    }
}

fn update_stored_coordinate(
    record: &mut RecordValue,
    field: &str,
    model: &CoordinateModel,
) -> PyResult<()> {
    let Some(Value::Record(original)) = record.get(field) else {
        return Err(raised("Missing coordinate record"));
    };
    let stored = edit_record(original, model)?;
    record.upsert(field, Value::Record(stored));
    Ok(())
}

// Live tabular transforms survive other edits to a CASA tool. Restoration
// from serialized samples resets the matrix and reference parameters.
fn live_tabular_factor(model: &CoordinateModel) -> Option<f64> {
    let factor = match model {
        CoordinateModel::Tabular(table) if !table.pixel_values().is_empty() => {
            return Some(table.linear_transform());
        }
        CoordinateModel::Tabular(tabular) => tabular.linear_transform(),
        CoordinateModel::Spectral(spectral) if !spectral.has_linear_mapping() => {
            spectral.linear_transform()
        }
        _ => return None,
    };
    (factor != 1.0).then_some(factor)
}

fn change_tabular_matrix(model: &CoordinateModel, factor: f64) -> PyResult<CoordinateModel> {
    match model {
        CoordinateModel::Tabular(tabular) => Ok(tabular
            .clone()
            .with_linear_transform(factor)
            .map_err(raised)?
            .into()),
        CoordinateModel::Spectral(spectral) => Ok(spectral
            .clone()
            .with_linear_transform(factor)
            .map_err(raised)?
            .into()),
        _ => Err(raised(
            "Coordinate no longer supports its tabular transform",
        )),
    }
}

fn live_tabular_model(model: &CoordinateModel, record: &RecordValue) -> PyResult<CoordinateModel> {
    match model {
        CoordinateModel::Tabular(tabular) => Ok(tabular
            .clone()
            .with_live_reference(record)
            .map_err(raised)?
            .into()),
        CoordinateModel::Spectral(spectral) => Ok(spectral
            .clone()
            .with_live_reference(record)
            .map_err(raised)?
            .into()),
        _ => Err(raised(
            "Coordinate no longer supports its tabular transform",
        )),
    }
}

fn recorded_coordinate<'a>(record: &'a RecordValue, field: &str) -> PyResult<&'a RecordValue> {
    match record.get(field) {
        Some(Value::Record(value)) => Ok(value),
        _ => Err(raised("Missing coordinate record")),
    }
}

impl CoordinateState {
    fn live_tabular_factors(&self) -> impl Iterator<Item = (usize, f64)> + '_ {
        (0..self.coordinates.n_coordinates()).filter_map(|index| {
            live_tabular_factor(self.coordinates.coordinate(index)).map(|factor| (index, factor))
        })
    }

    pub(super) fn editable_record(&self) -> PyResult<RecordValue> {
        let mut record = self.record.clone();
        let mut matrices = self.live_tabular_factors().peekable();
        if matrices.peek().is_none() {
            return Ok(record);
        }
        let placements = self
            .coordinates
            .coordinates_in_record(&record)
            .map_err(raised)?;
        for (index, _) in matrices {
            let original = self.coordinates.coordinate(index);
            let unscaled = change_tabular_matrix(original, 1.0)?;
            update_stored_coordinate(&mut record, &placements[index].field, &unscaled)?;
        }
        Ok(record)
    }

    pub(super) fn apply_edited_record(&mut self, record: RecordValue) -> PyResult<()> {
        let coordinates = self
            .coordinates
            .restore_record_with_layout(&record)
            .map_err(raised)?;
        self.apply_edited_coordinates(record, coordinates)
    }

    pub(super) fn edit_record_axis(
        &self,
        record: &mut RecordValue,
        axis: usize,
        edit: TabularEdit,
    ) -> PyResult<()> {
        if self.live_tabular_factors().next().is_some() {
            let placements = self
                .coordinates
                .coordinates_in_record(record)
                .map_err(raised)?;
            let pixel = matches!(edit, TabularEdit::ReferencePixel(_));
            if let Some((index, placement)) =
                placements.iter().enumerate().find(|(_, placement)| {
                    let axes = if pixel {
                        &placement.pixel_axes
                    } else {
                        &placement.world_axes
                    };
                    axes.contains(&Some(axis))
                })
                && live_tabular_factor(self.coordinates.coordinate(index)).is_some()
            {
                let native = recorded_coordinate(record, &placement.field)?;
                let base = placement.model(record).map_err(raised)?;
                let mut model = live_tabular_model(&base, native)?;
                match &mut model {
                    CoordinateModel::Tabular(table) => {
                        table.edit_live_reference(edit).map_err(raised)?
                    }
                    CoordinateModel::Spectral(spectral) => {
                        spectral.edit_live_reference(edit).map_err(raised)?
                    }
                    _ => {
                        return Err(raised(
                            "Coordinate no longer supports its tabular transform",
                        ));
                    }
                }
                return update_stored_coordinate(record, &placement.field, &model);
            }
        }
        match edit {
            TabularEdit::ReferencePixel(value) => {
                CoordinateSystem::set_reference_pixel_in_record(record, axis, value)
            }
            TabularEdit::ReferenceValue(value) => {
                CoordinateSystem::set_world_reference_value_in_record(record, axis, value)
            }
            TabularEdit::Increment(value) => {
                CoordinateSystem::set_world_increment_in_record(record, axis, value)
            }
        }
        .map(|_| ())
        .map_err(raised)
    }

    pub(super) fn apply_edited_coordinates(
        &mut self,
        mut record: RecordValue,
        mut coordinates: CoordinateSystem,
    ) -> PyResult<()> {
        let mut matrices = self.live_tabular_factors().peekable();
        if matrices.peek().is_some() {
            let previous = self
                .coordinates
                .coordinates_in_record(&self.record)
                .map_err(raised)?;
            let placements = coordinates.coordinates_in_record(&record).map_err(raised)?;
            for (index, factor) in &mut matrices {
                let field = &previous[index].field;
                let index = placements
                    .iter()
                    .position(|placement| &placement.field == field)
                    .ok_or_else(|| raised("Missing coordinate placement"))?;
                let native = recorded_coordinate(&record, field)?;
                let base = live_tabular_model(coordinates.coordinate(index), native)?;
                let changed = change_tabular_matrix(&base, factor)?;
                update_stored_coordinate(&mut record, field, &changed)?;
                coordinates.replace_coordinate(index, changed);
            }
        }
        drop(matrices);
        coordinates.retain_direction_mix_ranges(&self.coordinates);
        self.record = record;
        self.coordinates = coordinates;
        Ok(())
    }
}

fn apply_matrix(
    state: &mut CoordinateState,
    kind: CoordinateType,
    matrix: Array2<f64>,
) -> PyResult<bool> {
    let (model, placement) = coordinate(state, kind)?.ok_or_else(|| missing(kind))?;
    let placements = state
        .coordinates
        .coordinates_in_record(&state.record)
        .map_err(raised)?;
    let index = placements
        .iter()
        .position(|entry| entry.field == placement.field)
        .ok_or_else(|| raised("Missing coordinate placement"))?;
    let sliced = placement.pixel_axes.len() != model.n_pixel_axes()
        || placement.world_axes.len() != model.n_world_axes();
    let full = if sliced {
        placement.model(&state.record).map_err(raised)?
    } else {
        model.clone()
    };
    let count = full.n_world_axes();
    let valid_shape = if matches!(full, CoordinateModel::Tabular(_)) {
        matrix.len() == 1
    } else {
        matrix.dim() == (count, count)
    };
    // CASA returns true when the coordinate refuses a matrix of the wrong shape.
    if !valid_shape
        || matches!(
            full,
            CoordinateModel::Stokes(_) | CoordinateModel::Quality(_)
        )
    {
        return Ok(true);
    }
    if matrix.iter().any(|value| !value.is_finite()) {
        return Err(raised(
            "The linear transform matrix must contain finite values",
        ));
    }
    let changed = match full {
        CoordinateModel::Direction(direction) => direction
            .with_normalized_pc_matrix(matrix)
            .map_err(raised)?
            .into(),
        CoordinateModel::Linear(linear) => {
            if has_zero_row(&matrix) {
                return Err(singular_linear_transform());
            }
            linear.with_pc_matrix(matrix).into()
        }
        CoordinateModel::Spectral(spectral) => spectral
            .with_linear_transform(matrix[[0, 0]])
            .map_err(raised)?
            .into(),
        CoordinateModel::Tabular(tabular) => tabular
            .with_linear_transform(matrix[[0, 0]])
            .map_err(raised)?
            .into(),
        CoordinateModel::Stokes(_) | CoordinateModel::Quality(_) | CoordinateModel::System(_) => {
            return Ok(true);
        }
    };
    let mut record = state.record.clone();
    update_stored_coordinate(&mut record, &placement.field, &changed)?;
    let changed = if sliced {
        match state.coordinates.restore_record_with_layout(&record) {
            Ok(restored) => restored.coordinate(index).clone(),
            Err(CoordinateError::UnsupportedAxisLayout(_)) => {
                let mut record = state.editable_record()?;
                update_stored_coordinate(&mut record, &placement.field, &changed)?;
                let restored = CoordinateSystem::from_mapped_record(&record).map_err(raised)?;
                state.apply_edited_coordinates(record, restored)?;
                return Ok(true);
            }
            Err(error) => return Err(raised(error)),
        }
    } else {
        changed
    };
    state.coordinates.replace_coordinate(index, changed);
    state.record = record;
    Ok(true)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (r#type="", value=None), text_signature = "($self, type='', value=[])")]
    fn setlineartransform(
        &self,
        py: Python<'_>,
        r#type: &str,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        if r#type.is_empty() {
            return Err(raised("You must specify the coordinate type"));
        }
        let matrix = matrix_input(value)?;
        py.detach(|| apply_matrix(&mut self.state(), coordinate_type(r#type), matrix))
    }
}
