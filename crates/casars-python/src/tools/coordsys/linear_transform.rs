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
            Variant::Number(_) | Variant::Bool(_) | Variant::Text(_) | Variant::Complex(_) => {
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
                ("tabular", &["pc", "worldvalues"][..])
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
    let Some(Value::Record(original)) = state.record.get(&placement.field) else {
        return Err(raised("Missing coordinate record"));
    };
    let stored = edit_record(original, &changed)?;
    let mut record = state.record.clone();
    record.upsert(&placement.field, Value::Record(stored));
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
