// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::TabularEdit;
use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem, RecordCoordinate};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::{PyOverflowError, PyRuntimeError, PySystemError};
use pyo3::prelude::*;

use crate::convert::Variant;
use crate::tools::table::values;

use super::ordering::{IntegerVector, integer_vector};
use super::transform_single::bool_vector;
use super::{CoordinateState, PyCoordsys, default_spectral_conversion};

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[derive(Default)]
struct SubimageShape(IntegerVector);

impl<'a, 'py> FromPyObject<'a, 'py> for SubimageShape {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Err(PySystemError::new_err(
                "<built-in function coordsys_subimage> returned a result with an exception set",
            ));
        }
        integer_vector(&value, "subimage", "newshape").map(Self)
    }
}

fn origin_input(input: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<f32>> {
    let invalid = || raised("Parameter neworigin is not a vector of pixel positions");
    let input = input.ok_or_else(invalid)?;
    if bool_vector(input)?.is_some() {
        return Err(invalid());
    }
    let array = input
        .cast::<PyUntypedArray>()
        .ok()
        .filter(|array| matches!(array.dtype().kind(), b'i' | b'u' | b'f'));
    let values = match array {
        Some(array) if array.ndim() == 0 => vec![input.call_method0("item")?.extract()?],
        Some(array) if array.is_empty() => Vec::new(),
        _ => match input.extract::<Variant>()? {
            Variant::Numbers(values) | Variant::Array { values, .. } => values,
            _ => return Err(invalid()),
        },
    };
    values
        .into_iter()
        .map(|value| {
            // The casacore subimage interface takes Float pixel offsets.
            let value = value as f32;
            if value.is_finite() {
                Ok(value)
            } else {
                Err(raised("Subimage origin positions must be finite"))
            }
        })
        .collect()
}

fn select_discrete<T: Copy>(
    labels: &[T],
    field: &str,
    record: &mut RecordValue,
    origin: f32,
    count: Option<i32>,
    describe: impl Fn(T) -> (&'static str, i32),
) -> PyResult<()> {
    let origin = (f64::from(origin) + 0.5) as i32;
    let start = usize::try_from(origin).map_err(|_| raised("Illegal origin shift"))?;
    let labels = labels
        .get(start..)
        .filter(|labels| !labels.is_empty())
        .ok_or_else(|| raised("Illegal origin shift"))?;
    let count = count
        .filter(|&count| count > 0)
        .map_or(labels.len(), |count| count as usize);
    let selected = labels
        .get(..count)
        .ok_or_else(|| raised("New shape is invalid"))?;
    record.upsert(
        field,
        Value::Array(ArrayValue::from_string_vec(
            selected
                .iter()
                .map(|&kind| describe(kind).0.to_owned())
                .collect(),
        )),
    );
    record.upsert(
        "crval",
        Value::Array(ArrayValue::from_f64_vec(vec![f64::from(
            describe(selected[0]).1,
        )])),
    );
    Ok(())
}

fn shift_reference_pixel(
    record: &mut RecordValue,
    placement: &RecordCoordinate,
    origin: &[f32],
) -> PyResult<()> {
    let references = record
        .get_mut("crpix")
        .ok_or_else(|| raised("Missing coordinate reference pixel"))?;
    match references {
        Value::Array(ArrayValue::Float64(values)) if values.len() == placement.pixel_axes.len() => {
            for (pixel, axis) in values.iter_mut().zip(&placement.pixel_axes) {
                if let Some(axis) = axis {
                    *pixel -= f64::from(origin[*axis]);
                }
            }
        }
        Value::Scalar(ScalarValue::Float64(value)) if placement.pixel_axes.len() == 1 => {
            if let Some(axis) = placement.pixel_axes[0] {
                *value -= f64::from(origin[axis]);
            }
        }
        _ => return Err(raised("Invalid coordinate reference pixel")),
    }
    Ok(())
}

fn shift_coordinate(
    model: &CoordinateModel,
    placement: &RecordCoordinate,
    stored: &mut RecordValue,
    origin: &[f32],
    shape: &[i32],
) -> PyResult<Option<f64>> {
    match model {
        CoordinateModel::Stokes(coordinate) => {
            if let Some(axis) = placement.pixel_axes[0] {
                select_discrete(
                    coordinate.stokes(),
                    "stokes",
                    stored,
                    origin[axis],
                    shape.get(axis).copied(),
                    |label| (label.name(), label.code()),
                )?;
            }
        }
        CoordinateModel::Quality(coordinate) => {
            if let Some(axis) = placement.pixel_axes[0] {
                select_discrete(
                    coordinate.values(),
                    "quality",
                    stored,
                    origin[axis],
                    shape.get(axis).copied(),
                    |label| (label.name(), label.code()),
                )?;
            }
        }
        CoordinateModel::Spectral(coordinate) => {
            let Some(axis) = placement.pixel_axes[0] else {
                return Ok(None);
            };
            if let Some(section) = coordinate
                .subimage_samples(origin[axis], shape.get(axis).copied())
                .map_err(raised)?
            {
                let mut record = CoordinateModel::from(section).to_record();
                if let Some(native_type) = stored.get("nativeType") {
                    record.upsert("nativeType", native_type.clone());
                }
                if record.get("conversion").is_none() {
                    let mut conversion = default_spectral_conversion();
                    if let Some(frame) = record.get("system") {
                        conversion.upsert("system", frame.clone());
                    }
                    record.upsert("conversion", Value::Record(conversion));
                }
                *stored = record;
                return coordinate.value_in_hz(1.0).map(Some).map_err(raised);
            } else if let Some(Value::Record(wcs)) = stored.get_mut("wcs") {
                shift_reference_pixel(wcs, placement, origin)?;
            } else {
                shift_reference_pixel(stored, placement, origin)?;
            }
        }
        CoordinateModel::Tabular(coordinate) => {
            if let Some(axis) = placement.pixel_axes[0] {
                let mut coordinate = coordinate.clone();
                let reference = coordinate.reference_pixel()[0] - f64::from(origin[axis]);
                coordinate
                    .edit_live_reference(TabularEdit::ReferencePixel(reference))
                    .map_err(raised)?;
                *stored = CoordinateModel::from(coordinate).to_record();
            }
        }
        CoordinateModel::System(system) => {
            let references = system.reference_pixel();
            for (local, (axis, reference)) in
                placement.pixel_axes.iter().zip(references).enumerate()
            {
                if let Some(axis) = axis {
                    let shifted = reference - f64::from(origin[*axis]);
                    CoordinateSystem::set_reference_pixel_in_record(stored, local, shifted)
                        .map_err(raised)?;
                }
            }
        }
        _ => shift_reference_pixel(stored, placement, origin)?,
    }
    Ok(None)
}

fn subimage_record(
    state: &CoordinateState,
    origin: &[f32],
    shape: &[i32],
) -> PyResult<RecordValue> {
    let rank = state.coordinates.n_pixel_axes();
    if origin.len() != rank {
        return Err(raised(
            "Failed AlwaysAssert originShift.nelements() == nPixelAxes() && pixincFac.nelements() == nPixelAxes()",
        ));
    }
    if !shape.is_empty() && shape.len() != rank {
        return Err(raised(
            "Failed AlwaysAssert nShape==0 || nShape==nPixelAxes()",
        ));
    }
    let mut record = state.record.clone();
    let placements = state
        .coordinates
        .coordinates_in_record(&record)
        .map_err(raised)?;
    for (index, placement) in placements.iter().enumerate() {
        let Some(Value::Record(stored)) = record.get_mut(&placement.field) else {
            return Err(raised("Missing coordinate record"));
        };
        let world_scale = shift_coordinate(
            state.coordinates.coordinate(index),
            placement,
            stored,
            origin,
            shape,
        )?;
        if let Some(scale) = world_scale.filter(|scale| *scale != 1.0) {
            let index = placement.field.trim_start_matches("spectral");
            let field = format!("worldreplace{index}");
            let Some(Value::Array(ArrayValue::Float64(values))) = record.get_mut(&field) else {
                return Err(raised(format!("Invalid coordinate field {field}")));
            };
            values.mapv_inplace(|value| value * scale);
        }
    }
    Ok(record)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (originshft=None, newshape=SubimageShape::default()), text_signature = "($self, originshft=[], newshape=[])")]
    fn subimage(
        &self,
        py: Python<'_>,
        originshft: Option<&Bound<'_, PyAny>>,
        newshape: SubimageShape,
    ) -> PyResult<Py<PyAny>> {
        let origin = origin_input(originshft)?;
        let record = py.detach(|| subimage_record(&self.state(), &origin, &newshape.0.values))?;
        if newshape.0.overflowed {
            let error = PySystemError::new_err(
                "<built-in function coordsys_subimage> returned a result with an exception set",
            );
            error.set_cause(
                py,
                Some(PyOverflowError::new_err(
                    "Python int too large to convert to C long",
                )),
            );
            return Err(error);
        }
        values::record(&record, Path::new(".")).into_python(py)
    }
}
