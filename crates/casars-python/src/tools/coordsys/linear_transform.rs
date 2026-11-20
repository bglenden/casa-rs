// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_types::{RecordValue, Value};
use ndarray::Array2;
use numpy::{PyArrayDescrMethods, PyReadonlyArrayDyn, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::Variant;

use super::frame_metadata::missing;
use super::inspect::coordinate_type;
use super::{CoordinateState, PyCoordsys};

fn raised(message: impl ToString) -> PyErr {
    PyRuntimeError::new_err(message.to_string())
}

fn matrix_input(value: Option<&Bound<'_, PyAny>>) -> PyResult<Array2<f64>> {
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
                ("tabular", &["pc", "pixelvalues", "worldvalues"][..])
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
        CoordinateModel::Tabular(_) => write_fields(&mut updated, &native, &["pc", "worldvalues"])?,
        _ => write_fields(&mut updated, &native, &["pc"])?,
    }
    Ok(updated)
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

// A tabular matrix survives edits to a live CASA tool. Its record instead
// stores the effective samples and rebuilds them with an identity matrix.
fn live_tabular_matrix(model: &CoordinateModel) -> Option<f64> {
    let factor = match model {
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

impl CoordinateState {
    fn tabular_matrices(&self) -> impl Iterator<Item = (usize, f64)> + '_ {
        (0..self.coordinates.n_coordinates()).filter_map(|index| {
            live_tabular_matrix(self.coordinates.coordinate(index)).map(|factor| (index, factor))
        })
    }

    pub(super) fn editable_record(&self) -> PyResult<RecordValue> {
        let mut record = self.record.clone();
        let mut matrices = self.tabular_matrices().peekable();
        if matrices.peek().is_none() {
            return Ok(record);
        }
        let placements = CoordinateSystem::record_coordinates(&record).map_err(raised)?;
        for (index, _) in matrices {
            let original = self.coordinates.coordinate(index);
            let unscaled = change_tabular_matrix(original, 1.0)?;
            update_stored_coordinate(&mut record, &placements[index].field, &unscaled)?;
        }
        Ok(record)
    }

    pub(super) fn apply_edited_record(&mut self, record: RecordValue) -> PyResult<()> {
        let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
        self.apply_edited_coordinates(record, coordinates)
    }

    pub(super) fn apply_edited_coordinates(
        &mut self,
        mut record: RecordValue,
        mut coordinates: CoordinateSystem,
    ) -> PyResult<()> {
        let mut matrices = self.tabular_matrices().peekable();
        if matrices.peek().is_some() {
            let placements = CoordinateSystem::record_coordinates(&record).map_err(raised)?;
            for (index, factor) in &mut matrices {
                let changed = change_tabular_matrix(coordinates.coordinate(index), factor)?;
                update_stored_coordinate(&mut record, &placements[index].field, &changed)?;
                coordinates.replace_coordinate(index, changed);
            }
        }
        drop(matrices);
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
    let index = state
        .coordinates
        .find_coordinate(kind)
        .ok_or_else(|| missing(kind))?;
    let placements = CoordinateSystem::record_coordinates(&state.record).map_err(raised)?;
    let placement = &placements[index];
    let model = state.coordinates.coordinate(index);
    let sliced = placement.pixel_axes.contains(&None);
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
    if !valid_shape || matches!(full, CoordinateModel::Stokes(_)) {
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
        CoordinateModel::Linear(linear) => linear.with_pc_matrix(matrix).into(),
        CoordinateModel::Spectral(spectral) => spectral
            .with_linear_transform(matrix[[0, 0]])
            .map_err(raised)?
            .into(),
        CoordinateModel::Tabular(tabular) => tabular
            .with_linear_transform(matrix[[0, 0]])
            .map_err(raised)?
            .into(),
        CoordinateModel::Stokes(_) => return Ok(true),
    };
    let mut record = state.record.clone();
    update_stored_coordinate(&mut record, &placement.field, &changed)?;
    let changed = if sliced {
        CoordinateSystem::from_record(&record)
            .map_err(raised)?
            .coordinate(index)
            .clone()
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
