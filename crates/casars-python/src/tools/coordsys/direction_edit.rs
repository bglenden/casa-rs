// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::PI;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
};
use casa_types::{Value, quanta::casacore};
use ndarray::Array2;
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::{DoubleList, Variant, holder::DIRECTION_CODES};
use crate::tools::{LogLine, emit};

use super::conversion_frames::FrameName;
use super::frame_metadata::missing;
use super::inspect::numbered_coordinates;
use super::linear_transform::matrix_input;
use super::projection::direction_projection;
use super::replace::replace_at;
use super::rest_frequency::{quantity_tokens, text_quantities};
use super::transform_single::{WorldArgument, bool_vector};
use super::{CoordinateState, PyCoordsys};

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

enum Transform {
    Default,
    Invalid,
    Matrix(Array2<f64>),
}

fn capture_transform(value: Option<&Bound<'_, PyAny>>) -> PyResult<Transform> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(Transform::Default);
    };
    if let Some(unset) = bool_vector(value)? {
        return Ok(if unset {
            Transform::Default
        } else {
            Transform::Invalid
        });
    }
    let py = value.py();
    let array = py.import("numpy")?.call_method1("asarray", (value,))?;
    let array = array.cast::<PyUntypedArray>()?;
    if !matches!(array.dtype().char(), b'f' | b'd') || array.ndim() == 0 {
        return Ok(Transform::Invalid);
    }
    matrix_input(Some(array.as_any())).map(Transform::Matrix)
}

fn capture_poles(value: Option<&Bound<'_, PyAny>>) -> PyResult<[f64; 2]> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok([PI, 0.0]);
    };
    if bool_vector(value)? == Some(true) {
        return Ok([PI, 0.0]);
    }
    let invalid = || raised("pole parameter is not understood");
    let texts = match value.extract::<Variant>()? {
        Variant::Text(value) => quantity_tokens(&value),
        Variant::Texts(values) => values,
        _ => return Err(invalid()),
    };
    let [longitude, latitude] = text_quantities(&texts)?.try_into().map_err(|_| invalid())?;
    let rad = casacore::casacore_unit("rad").map_err(raised)?;
    Ok([
        casacore::get_value(&longitude, &rad),
        casacore::get_value(&latitude, &rad),
    ])
}

fn direction_value(
    value: WorldArgument,
    state: &CoordinateState,
    defaults: Vec<f64>,
) -> PyResult<[f64; 2]> {
    let values = if matches!(value, WorldArgument::Unset) {
        defaults
    } else {
        value.values_with_defaults(state, None, defaults)?
    };
    values
        .try_into()
        .map_err(|_| raised("Direction requires two world-axis values"))
}

fn capture_world_value(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<WorldArgument> {
    if let Some(value) = value
        && let Ok(array) = value.cast::<PyUntypedArray>()
        && array.ndim() == 0
    {
        // CASA treats a zero-dimensional array as a one-element coordinate
        // vector, including when the array holds an integer.
        let flattened = value.call_method1("reshape", (1,))?;
        return WorldArgument::capture(py, Some(&flattened));
    }
    if let Some(value) = value
        && value.is_instance(&py.import("numpy")?.getattr("generic")?)?
    {
        let dtype = value.getattr("dtype")?;
        let kind: char = dtype.getattr("kind")?.extract()?;
        let code: char = dtype.getattr("char")?.extract()?;
        if kind == 'i' {
            return Err(raised("Unrecognized format for world coordinate"));
        }
        if matches!(kind, 'u' | 'b') || matches!(code, 'g' | 'F' | 'G') {
            return Ok(WorldArgument::Unset);
        }
    }
    WorldArgument::capture(py, value)
}

struct DirectionEdit {
    frame: String,
    projection: String,
    parameters: Vec<f64>,
    pixels: Vec<f64>,
    reference: WorldArgument,
    increment: WorldArgument,
    transform: Transform,
    poles: [f64; 2],
}

fn set_direction(
    state: &mut CoordinateState,
    edit: DirectionEdit,
    log: &mut Vec<LogLine>,
) -> PyResult<bool> {
    let (index, _, placement) = numbered_coordinates(state)?
        .into_iter()
        .find(|(_, model, _)| matches!(model, CoordinateModel::Direction(_)))
        .ok_or_else(|| missing(CoordinateType::Direction))?;
    let CoordinateModel::Direction(original) = placement.model(&state.record).map_err(raised)?
    else {
        return Err(missing(CoordinateType::Direction));
    };
    let pc = match edit.transform {
        Transform::Default => Array2::eye(2),
        Transform::Invalid => {
            log.push(LogLine::warning("xform paramater invalid"));
            return Ok(false);
        }
        Transform::Matrix(matrix) => matrix,
    };
    let frame = if edit.frame.is_empty() {
        original.direction_ref()
    } else {
        DIRECTION_CODES
            .resolve(&edit.frame)
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| raised(format!("Invalid direction code '{}' given", edit.frame)))?
    };
    let name = if edit.projection.is_empty() {
        original.projection().name()
    } else {
        &edit.projection
    };
    let parameters = if edit.parameters == [-1.0] {
        original.projection().parameters().to_vec()
    } else {
        edit.parameters
    };
    let projection = direction_projection(name, parameters)?;
    projection
        .validate_conversion_parameters()
        .map_err(raised)?;
    let pixels = if edit.pixels == [-1.0] {
        [placement.pixel_axes[0], placement.pixel_axes[1]]
            .map(|axis| axis.map_or(-1.0, |axis| axis as f64))
    } else if let [longitude, latitude, ..] = edit.pixels.as_slice() {
        [*longitude, *latitude]
    } else {
        return Err(raised("Direction reference pixel requires two components"));
    };
    let mut local = CoordinateSystem::new().with_obs_info(state.coordinates.obs_info().clone());
    let old_reference = original.reference_value();
    let old_increment = original.increment();
    local.add_coordinate(original);
    let local = CoordinateState::new(local);
    let reference = direction_value(edit.reference, &local, old_reference)?;
    let increment = direction_value(edit.increment, &local, old_increment)?;
    if reference
        .iter()
        .chain(&increment)
        .chain(&pixels)
        .any(|value| !value.is_finite())
    {
        return Err(raised(
            "Direction reference values, increments and pixels must be finite",
        ));
    }
    if pc.shape() != [2, 2]
        || pc[[0, 0]] * pc[[1, 1]] == pc[[0, 1]] * pc[[1, 0]]
        || increment.contains(&0.0)
    {
        return Err(raised(
            "wcs wcsset_error: Linear transformation matrix is singular",
        ));
    }
    let changed = CoordinateModel::from(
        DirectionCoordinate::new(frame, projection, reference, increment, pixels)
            .with_normalized_pc_matrix(pc)
            .and_then(|model| model.with_wcs_poles(edit.poles[0], edit.poles[1]))
            .map_err(raised)?,
    );
    let record = Value::Record(changed.to_record());
    replace_at(state, index as i32, changed, record)?;
    Ok(true)
}

#[pymethods]
impl PyCoordsys {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (refcode=FrameName::default(), proj=FrameName::default(), projpar=DoubleList::of(&[-1.0]), refpix=DoubleList::of(&[-1.0]), refval=None, incr=None, xform=None, poles=None), text_signature = "($self, refcode='', proj='', projpar=[-1.0], refpix=[-1.0], refval=[], incr=[], xform=[], poles=[])")]
    fn setdirection(
        &self,
        py: Python<'_>,
        refcode: FrameName,
        proj: FrameName,
        projpar: DoubleList,
        refpix: DoubleList,
        refval: Option<&Bound<'_, PyAny>>,
        incr: Option<&Bound<'_, PyAny>>,
        xform: Option<&Bound<'_, PyAny>>,
        poles: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let edit = DirectionEdit {
            frame: refcode.into_name("refcode")?,
            projection: proj.into_name("proj")?,
            parameters: projpar.0,
            pixels: refpix.0,
            reference: capture_world_value(py, refval)?,
            increment: capture_world_value(py, incr)?,
            transform: capture_transform(xform)?,
            poles: capture_poles(poles)?,
        };
        let mut log = Vec::new();
        let result = py.detach(|| set_direction(&mut self.state(), edit, &mut log));
        emit(py, "casars.tools.coordsys", &log)?;
        result
    }
}
