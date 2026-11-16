// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare CASA component shape updates before mutating the list.

use casa_components::{ComponentShape, Ellipse, EllipseErrors, ShapeKind, ShapeType};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim};
use pyo3::exceptions::{PyOverflowError, PySystemError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple};
use std::collections::HashSet;

use super::{PyComponentList, setrefdir_args, supplied};
use crate::convert::value::CasaValue;

use crate::convert::{ConvertError, ConvertResult, Variant, quantity};
use crate::tools::LogLine;

pub(super) struct Request {
    pub(super) name: String,
    pub(super) axes: [Variant; 3],
    pub(super) errors: [Variant; 3],
}

pub(super) struct Prepared {
    pub(super) shape: ComponentShape,
    pub(super) units: Option<[Unit; 6]>,
}

/// Parse all parameters in CASA's order and report shape refusals as false.
pub(super) fn prepare(
    request: &Request,
    lines: &mut Vec<LogLine>,
) -> ConvertResult<Option<Prepared>> {
    let shape_type = match ShapeType::parse_casa(&request.name) {
        Ok(ShapeType::LimbDarkenedDisk) => {
            lines.push(LogLine::error(
                "Limbdarkeneddisk is not implemented yet. Shape not changed.",
            ));
            return Ok(None);
        }
        Ok(kind) => kind,
        Err(_) => {
            lines.push(LogLine::error("Could not translate the shape type to a known value. Known types are: Point, Gaussian, Disk. Shape not changed."));
            return Ok(None);
        }
    };
    let axes = [
        quantity(&request.axes[0])?,
        quantity(&request.axes[1])?,
        quantity(&request.axes[2])?,
    ];
    let empty = |value: &Variant| {
        value.is_empty_sequence() || matches!(value, Variant::Text(text) if text == "[]")
    };
    let errors: [Quantity; 3] = if request.errors.iter().all(|value| !empty(value)) {
        [
            quantity(&request.errors[0])?,
            quantity(&request.errors[1])?,
            quantity(&request.errors[2])?,
        ]
    } else {
        let rad = Unit::new("rad").expect("rad is a unit");
        std::array::from_fn(|_| Quantity::with_unit(0.0, rad.clone()))
    };
    let direction = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
    if shape_type == ShapeType::Point {
        return Ok(Some(Prepared {
            shape: ComponentShape::point(direction),
            units: None,
        }));
    }
    for (name, q) in ["majoraxis", "minoraxis", "positionangle"]
        .into_iter()
        .zip(&axes)
    {
        if q.unit().val().dim != UnitDim::basis(Dimension::Angle) {
            lines.push(LogLine::error(format!("Could not parse the shape parameters. The {name} field must have angular units. Shape not changed.")));
            return Ok(None);
        }
    }
    let [major, mut minor, angle] = axes.each_ref().map(Quantity::get_si_value);
    if let Some(message) = super::add::constructor_error(shape_type, major, minor, angle) {
        return Err(ConvertError::runtime(message));
    }
    if shape_type == ShapeType::Disk && (major - minor).abs() < 1e-17 {
        minor = major;
    }
    if [major, minor, angle].iter().any(|value| !value.is_finite()) {
        return Err(ConvertError::runtime(
            "non-finite component shape refused (#1053)",
        ));
    }
    for (name, q) in ["majoraxiserror", "minoraxiserror", "positionangleerror"]
        .into_iter()
        .zip(&errors)
    {
        if q.unit().val().dim != UnitDim::basis(Dimension::Angle) {
            lines.push(LogLine::error(format!("Could not parse the shape parameters. The {name} field must have angular units. Shape not changed.")));
            return Ok(None);
        }
    }
    let [major_error, minor_error, angle_error] = errors.each_ref().map(Quantity::get_si_value);
    if [major_error, minor_error, angle_error]
        .iter()
        .any(|value| !value.is_finite())
    {
        return Err(ConvertError::runtime(
            "non-finite component shape error refused (#1053)",
        ));
    }
    if [major_error, minor_error, angle_error]
        .iter()
        .any(|value| *value < 0.0)
    {
        return Err(ConvertError::runtime(
            "The errors must be non-negative angular quantities.",
        ));
    }
    let axes_value =
        Ellipse::new(major, minor, angle).map_err(|e| ConvertError::runtime(e.to_string()))?;
    let error_values = EllipseErrors {
        major_axis: major_error,
        minor_axis: minor_error,
        position_angle: angle_error,
    };
    let kind = match shape_type {
        ShapeType::Gaussian => ShapeKind::Gaussian {
            axes: axes_value,
            errors: error_values,
        },
        ShapeType::Disk => ShapeKind::Disk {
            axes: axes_value,
            errors: error_values,
        },
        _ => unreachable!("point and limb-darkened disk handled above"),
    };
    let shape =
        ComponentShape::new(direction, kind).map_err(|e| ConvertError::runtime(e.to_string()))?;
    let units = [
        axes[0].unit().clone(),
        axes[1].unit().clone(),
        axes[2].unit().clone(),
        errors[0].unit().clone(),
        errors[1].unit().clone(),
        errors[2].unit().clone(),
    ];
    Ok(Some(Prepared {
        shape,
        units: Some(units),
    }))
}

enum OptionalFrame<'py> {
    Value(Bound<'py, PyAny>),
    Leave(usize),
}

/// CASA converts, then ignores, this double vector; cycles crash the binding.
pub(super) fn optional_pending(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let ndarray = value.py().import("numpy")?.getattr("ndarray")?;
    if value.is_instance(&ndarray)? {
        return Ok(false);
    }
    let sequence = value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>();
    if !sequence {
        if value.is_instance_of::<PyInt>() {
            return Ok(value.extract::<i64>().is_err());
        }
        return Ok(!value.is_instance_of::<PyFloat>()
            && !value.is_instance_of::<PyString>()
            && !value.is_instance_of::<PyBytes>());
    }
    let mut active = HashSet::new();
    let mut stack = vec![OptionalFrame::Value(value.clone())];
    let mut pending = false;
    while let Some(frame) = stack.pop() {
        let object = match frame {
            OptionalFrame::Value(object) => object,
            OptionalFrame::Leave(identity) => {
                active.remove(&identity);
                continue;
            }
        };
        if object.is_instance_of::<PyList>() || object.is_instance_of::<PyTuple>() {
            let identity = object.as_ptr() as usize;
            if !active.insert(identity) {
                return Err(PyValueError::new_err(
                    "cyclic component double-vector argument (CASA crash: #1053)",
                ));
            }
            let items = object.try_iter()?.collect::<PyResult<Vec<_>>>()?;
            stack.push(OptionalFrame::Leave(identity));
            stack.extend(items.into_iter().rev().map(OptionalFrame::Value));
        } else if object.is_instance_of::<PyInt>() {
            pending |= object.extract::<i64>().is_err();
        }
    }
    Ok(pending)
}

#[pymethods]
impl PyComponentList {
    /// Replace one component's shape, preserving its direction and errors.
    #[pyo3(
        signature = (which, r#type = None, majoraxis = None, minoraxis = None, positionangle = None, majoraxiserror = None, minoraxiserror = None, positionangleerror = None, optionalparms = None, log = None),
        text_signature = "($self, which, type='Point', majoraxis=[], minoraxis=[], positionangle=[], majoraxiserror=[], minoraxiserror=[], positionangleerror=[], optionalparms=[0.0], log=True)"
    )]
    #[allow(clippy::too_many_arguments)]
    fn setshape(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] r#type: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] majoraxis: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] minoraxis: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] positionangle: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] majoraxiserror: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] minoraxiserror: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] positionangleerror: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] optionalparms: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] log: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (which, mut pending) = match which.extract::<i64>() {
            Ok(index) => (i64::from(index as i32), false),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => {
                (-1, true)
            }
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        let name: String = match r#type {
            None => "Point".to_string(),
            Some(value) if value.is_instance_of::<PyString>() => value.extract()?,
            Some(value) if value.is_instance_of::<PyBytes>() => {
                let bytes = value.cast::<PyBytes>()?.as_bytes();
                let end = bytes
                    .iter()
                    .position(|&byte| byte == 0)
                    .unwrap_or(bytes.len());
                PyBytes::new(py, &bytes[..end])
                    .call_method1("decode", ("utf-8",))?
                    .extract()?
            }
            Some(_) => return Err(PyTypeError::new_err("argument _type must be a string")),
        };
        let name = name.split('\0').next().unwrap_or_default().to_string();
        let mut convert = |arg| {
            let (value, has_pending) = setrefdir_args::argument(arg)?;
            pending |= has_pending;
            PyResult::Ok(value)
        };
        let axes = [
            convert(majoraxis)?,
            convert(minoraxis)?,
            convert(positionangle)?,
        ];
        let errors = [
            convert(majoraxiserror)?,
            convert(minoraxiserror)?,
            convert(positionangleerror)?,
        ];
        if let Some(value) = optionalparms {
            pending |= optional_pending(&value)?;
        }
        if log.is_some_and(|value| !value.is_instance_of::<PyBool>()) {
            return Err(PyTypeError::new_err(
                "in method 'componentlist_setshape', argument 11 of type 'bool'",
            ));
        }
        let request = Request { name, axes, errors };
        let result = self.call(py, move |state, lines| {
            state
                .setshape(which, &request, pending, lines)
                .map(CasaValue::Bool)
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_setshape> returned a result with an exception set",
            ));
        }
        Ok(result)
    }
}
