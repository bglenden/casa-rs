// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_core::measures::doppler::DopplerRef;
use casa_core::quanta::{Dimension, UnitDim, casacore};
use ndarray::{Array2, ShapeBuilder};
use numpy::{IntoPyArray, PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::{PyRuntimeError, PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use crate::convert::holder::{DOPPLER_CODES, doppler_ref};
use crate::convert::{StringList, Variant, value::CasaValue};

use super::PyCoordsys;
use super::conversion_frames::FrameName;
use super::doppler::{VelocityConversion, values as double_values};
use super::inspect::{axis_values, units as coordinate_units};
use super::transform_relative::OutputFrame;

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn vector_error(method: &str) -> PyErr {
    PySystemError::new_err(format!(
        "<built-in function coordsys_{method}> returned a result with an exception set"
    ))
}

enum AbsoluteFlags {
    Values(Vec<bool>),
    Text,
    Tuple,
}

impl AbsoluteFlags {
    fn default_flags() -> Self {
        Self::Values(vec![true])
    }

    fn values(self, argument: &str, method: &str) -> PyResult<Vec<bool>> {
        match self {
            Self::Values(values) => Ok(values),
            Self::Text => Err(PyTypeError::new_err(format!(
                "argument _{argument} must be a boolean"
            ))),
            Self::Tuple => Err(vector_error(method)),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for AbsoluteFlags {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
            return Ok(Self::Text);
        }
        if value.is_instance_of::<PyTuple>() {
            return Ok(Self::Tuple);
        }
        let flags = if value.is_instance_of::<PyList>() || value.is_instance_of::<PyUntypedArray>()
        {
            double_values(&value, "convert")?
                .into_iter()
                .map(|value| value != 0.0)
                .collect()
        } else if value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>() {
            vec![value.extract::<f64>()?.trunc() != 0.0]
        } else {
            Vec::new()
        };
        Ok(Self::Values(flags))
    }
}

enum MixShape {
    Values(Vec<i64>),
    Unsupported,
}

impl MixShape {
    fn default_shape() -> Self {
        Self::Values(vec![-1])
    }

    fn values(self, method: &str) -> PyResult<Vec<i64>> {
        match self {
            Self::Values(values) => Ok(values),
            Self::Unsupported => Err(vector_error(method)),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for MixShape {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
            return Err(PyTypeError::new_err("argument _shape must not be a string"));
        }
        if value.is_instance_of::<PyList>()
            || value.is_instance_of::<PyUntypedArray>()
            || value.is_instance_of::<PyInt>()
            || value.is_instance_of::<PyFloat>()
        {
            let values = double_values(&value, "convert")?;
            return Ok(Self::Values(
                values
                    .into_iter()
                    .map(|value| i64::from(value as i32))
                    .collect(),
            ));
        }
        Ok(Self::Unsupported)
    }
}

struct MixFormat {
    units: Vec<String>,
    absolute: Vec<bool>,
    doppler: DopplerRef,
}

impl MixFormat {
    fn capture(
        units: Option<&Bound<'_, PyAny>>,
        absolute: Vec<bool>,
        doppler: FrameName,
        argument: &str,
    ) -> PyResult<Self> {
        let name = doppler.into_name(argument)?;
        let doppler = DOPPLER_CODES
            .resolve(&name)
            .and_then(|code| doppler_ref(code).ok())
            .ok_or_else(|| raised("Illegal doppler"))?;
        let mut units = units
            .filter(|value| value.is_instance_of::<PyString>() || value.is_instance_of::<PyList>())
            .map(|value| value.extract::<StringList>().map(|value| value.0))
            .transpose()?
            .unwrap_or_default();
        if units.is_empty() {
            units.push("Native".into());
        }
        Ok(Self {
            units,
            absolute,
            doppler,
        })
    }

    fn prepare(self, coordinates: &CoordinateSystem) -> PyResult<Vec<AxisSetting>> {
        let count = coordinates.n_world_axes();
        let declared = axis_values(coordinates, true, coordinate_units);
        let units = if self.units == ["Native"] {
            declared.clone()
        } else {
            self.units
        };
        let flags = if self.absolute == [true] {
            vec![true; count]
        } else {
            self.absolute
        };
        if units.len() != count || flags.len() != count {
            return Err(raised("Inputs must all be of length nWorldAxes"));
        }
        units
            .into_iter()
            .zip(flags)
            .enumerate()
            .map(|(axis, (unit, absolute))| {
                let (index, _) = coordinates
                    .world_axis_owner(axis)
                    .expect("public world axis");
                let model = coordinates.coordinate(index);
                let units = axis_units(model, &declared[axis], &unit, self.doppler)?;
                Ok(AxisSetting { units, absolute })
            })
            .collect()
    }
}

enum AxisUnits {
    Pixel,
    World(f64),
    Velocity(VelocityConversion),
}

fn axis_units(
    model: &CoordinateModel,
    declared: &str,
    requested: &str,
    doppler: DopplerRef,
) -> PyResult<AxisUnits> {
    if requested == "pix" {
        return Ok(AxisUnits::Pixel);
    }
    let unit = casacore::casacore_unit(requested).map_err(raised)?;
    let speed = UnitDim::basis(Dimension::Length) / UnitDim::basis(Dimension::Time);
    if unit.val().dim == speed {
        match model {
            CoordinateModel::Spectral(spectral) => {
                return Ok(AxisUnits::Velocity(VelocityConversion::for_spectral_axis(
                    spectral, &unit, doppler,
                )?));
            }
            CoordinateModel::Linear(_) => (),
            _ => {
                return Err(raised(
                    "axis with km/s units is neither Spectral nor Linear",
                ));
            }
        }
    }
    if matches!(
        model,
        CoordinateModel::Stokes(_) | CoordinateModel::Quality(_)
    ) {
        return Ok(AxisUnits::World(1.0));
    }
    let native = if matches!(model, CoordinateModel::Direction(_)) {
        "rad"
    } else {
        declared
    };
    let native = casacore::casacore_unit(native).map_err(raised)?;
    if unit.val().dim != native.val().dim {
        return Err(raised("Units are not compatible dimensionally"));
    }
    Ok(AxisUnits::World(unit.val().factor / native.val().factor))
}

struct AxisSetting {
    units: AxisUnits,
    absolute: bool,
}

struct MixedTransform {
    inputs: Vec<AxisSetting>,
    outputs: Vec<AxisSetting>,
    world_axes: Vec<bool>,
    reference_pixel: Vec<f64>,
    reference_world: Vec<f64>,
    relative_world_reference: Vec<f64>,
}

impl MixedTransform {
    fn new(
        coordinates: &CoordinateSystem,
        count: usize,
        input: MixFormat,
        output: MixFormat,
    ) -> PyResult<Self> {
        if coordinates.n_pixel_axes() != coordinates.n_world_axes() {
            return Err(raised("Number of pixel and world axes differs"));
        }
        if count != coordinates.n_world_axes() {
            return Err(raised("Coordinates must all be of length nWorldAxes"));
        }
        if coordinates
            .find_coordinate(CoordinateType::System)
            .is_some()
        {
            return Err(raised(
                "Mixed conversions cannot use a nested coordinate system",
            ));
        }
        let inputs = input.prepare(coordinates)?;
        let outputs = output.prepare(coordinates)?;
        let world_axes = inputs
            .iter()
            .map(|axis| !matches!(axis.units, AxisUnits::Pixel))
            .collect();
        let reference_pixel = coordinates.reference_pixel();
        let reference_world = coordinates.reference_value();
        let mut relative_world_reference = reference_world.clone();
        coordinates
            .convert_world_offsets(&mut relative_world_reference, false)
            .map_err(raised)?;
        Ok(Self {
            inputs,
            outputs,
            world_axes,
            reference_pixel,
            reference_world,
            relative_world_reference,
        })
    }

    fn apply(&self, coordinates: &CoordinateSystem, values: &[f64]) -> PyResult<Vec<f64>> {
        let mut world = self.reference_world.clone();
        let mut pixels = self.reference_pixel.clone();
        let mut relative_world = self.relative_world_reference.clone();
        let mut relative_pixels = vec![0.0; pixels.len()];
        let mut has_relative_world = false;
        let mut has_relative_pixel = false;
        for (axis, (format, &value)) in self.inputs.iter().zip(values).enumerate() {
            match (&format.units, format.absolute) {
                (AxisUnits::Pixel, true) => pixels[axis] = value,
                (AxisUnits::Pixel, false) => {
                    relative_pixels[axis] = value;
                    has_relative_pixel = true;
                }
                (AxisUnits::World(scale), true) => world[axis] = value * scale,
                (AxisUnits::World(scale), false) => {
                    relative_world[axis] = value * scale;
                    has_relative_world = true;
                }
                (AxisUnits::Velocity(conversion), absolute) => {
                    let value = if absolute {
                        value
                    } else {
                        value + conversion.mixed_velocity(self.reference_world[axis])?
                    };
                    world[axis] = conversion.mixed_frequency(value)?;
                }
            }
        }
        if has_relative_pixel {
            OutputFrame::Absolute.apply(coordinates, &mut relative_pixels, false)?;
        }
        if has_relative_world {
            coordinates
                .convert_world_offsets(&mut relative_world, true)
                .map_err(raised)?;
        }
        for (axis, format) in self.inputs.iter().enumerate() {
            if !format.absolute {
                match format.units {
                    AxisUnits::Pixel => pixels[axis] = relative_pixels[axis],
                    AxisUnits::World(_) => world[axis] = relative_world[axis],
                    AxisUnits::Velocity(_) => (),
                }
            }
        }
        let pixel = coordinates
            .pixels_from_mixed(&world, &pixels, &self.world_axes)
            .map_err(raised)?;
        if self
            .outputs
            .iter()
            .all(|axis| axis.absolute && matches!(axis.units, AxisUnits::Pixel))
        {
            return Ok(pixel);
        }
        let mut relative_pixels = pixel.clone();
        OutputFrame::Relative.apply(coordinates, &mut relative_pixels, false)?;
        if self
            .outputs
            .iter()
            .all(|axis| !axis.absolute && matches!(axis.units, AxisUnits::Pixel))
        {
            return Ok(relative_pixels);
        }
        let world = coordinates.to_world(&pixel).map_err(raised)?;
        let mut relative_world = world.clone();
        if self
            .outputs
            .iter()
            .any(|axis| !axis.absolute && matches!(axis.units, AxisUnits::World(_)))
        {
            coordinates
                .convert_world_offsets(&mut relative_world, false)
                .map_err(raised)?;
        }
        self.outputs
            .iter()
            .enumerate()
            .map(|(axis, format)| {
                Ok(match (&format.units, format.absolute) {
                    (AxisUnits::Pixel, true) => pixel[axis],
                    (AxisUnits::Pixel, false) => relative_pixels[axis],
                    (AxisUnits::World(scale), true) => world[axis] / scale,
                    (AxisUnits::World(scale), false) => relative_world[axis] / scale,
                    (AxisUnits::Velocity(conversion), absolute) => {
                        let velocity = conversion.mixed_velocity(world[axis])?;
                        if absolute {
                            velocity
                        } else {
                            velocity - conversion.mixed_velocity(self.reference_world[axis])?
                        }
                    }
                })
            })
            .collect()
    }
}

fn install_range_from_shape(coordinates: &mut CoordinateSystem, shape: &[i64]) {
    if shape != [-1] && shape.len() == coordinates.n_pixel_axes() {
        // Casacore retains the previous ranges when the selected image
        // centre cannot be projected.
        let _ = coordinates.set_direction_mix_shape(shape);
    }
}

fn matrix_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Array2<f64>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let (shape, values) = match value.extract::<Variant>()? {
        Variant::Array { shape, values } => (shape, values),
        Variant::ComplexArray { shape, values } => {
            (shape, values.into_iter().map(|value| value.re).collect())
        }
        _ if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() => {
            let py = value.py();
            let options = PyDict::new(py);
            options.set_item("dtype", "object")?;
            let Ok(array) = py
                .import("numpy")?
                .call_method("asarray", (value,), Some(&options))
            else {
                return Ok(None);
            };
            (
                array.cast::<PyUntypedArray>()?.shape().to_vec(),
                double_values(&array, "convertmany")?,
            )
        }
        _ if value.is_instance_of::<PyUntypedArray>() => {
            let array = value.cast::<PyUntypedArray>()?;
            let kind = array.dtype().kind();
            if !matches!(kind, b'b' | b'i' | b'u' | b'f' | b'S' | b'U') {
                return Ok(None);
            }
            let mut values = double_values(value, "convertmany")?;
            if kind == b'U' {
                // CASA's Unicode-array typemap reads the first string and
                // leaves the remaining entries in the double vector at zero.
                values.get_mut(1..).unwrap_or_default().fill(0.0);
            }
            (array.shape().to_vec(), values)
        }
        _ => return Ok(None),
    };
    let [rows, columns] = shape.as_slice() else {
        return Ok(None);
    };
    Array2::from_shape_vec((*rows, *columns).f(), values)
        .map(Some)
        .map_err(raised)
}

#[pymethods]
impl PyCoordsys {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (coordin, absin=AbsoluteFlags::default_flags(), dopplerin=FrameName::radio_default(), unitsin=None, absout=AbsoluteFlags::default_flags(), dopplerout=FrameName::radio_default(), unitsout=None, shape=MixShape::default_shape()), text_signature = "($self, coordin, absin=[True], dopplerin='radio', unitsin=['Native'], absout=[True], dopplerout='radio', unitsout=['Native'], shape=[-1])")]
    fn convert(
        &self,
        py: Python<'_>,
        coordin: &Bound<'_, PyAny>,
        absin: AbsoluteFlags,
        dopplerin: FrameName,
        unitsin: Option<&Bound<'_, PyAny>>,
        absout: AbsoluteFlags,
        dopplerout: FrameName,
        unitsout: Option<&Bound<'_, PyAny>>,
        shape: MixShape,
    ) -> PyResult<Py<PyAny>> {
        let values = double_values(coordin, "convert")?;
        let input = MixFormat::capture(
            unitsin,
            absin.values("absin", "convert")?,
            dopplerin,
            "dopplerin",
        )?;
        let output = MixFormat::capture(
            unitsout,
            absout.values("absout", "convert")?,
            dopplerout,
            "dopplerout",
        )?;
        let shape = shape.values("convert")?;
        let result = py.detach(|| {
            let mut state = self.state();
            install_range_from_shape(&mut state.coordinates, &shape);
            let transform = MixedTransform::new(&state.coordinates, values.len(), input, output)?;
            transform.apply(&state.coordinates, &values)
        })?;
        CasaValue::floats(result).into_python(py)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (coordin=None, absin=AbsoluteFlags::default_flags(), dopplerin=FrameName::radio_default(), unitsin=None, absout=AbsoluteFlags::default_flags(), dopplerout=FrameName::radio_default(), unitsout=None, shape=MixShape::default_shape()), text_signature = "($self, coordin=[], absin=[True], dopplerin='radio', unitsin=['Native'], absout=[True], dopplerout='radio', unitsout=['Native'], shape=[-1])")]
    fn convertmany(
        &self,
        py: Python<'_>,
        coordin: Option<&Bound<'_, PyAny>>,
        absin: AbsoluteFlags,
        dopplerin: FrameName,
        unitsin: Option<&Bound<'_, PyAny>>,
        absout: AbsoluteFlags,
        dopplerout: FrameName,
        unitsout: Option<&Bound<'_, PyAny>>,
        shape: MixShape,
    ) -> PyResult<Py<PyAny>> {
        let values = matrix_argument(coordin)?;
        let input = MixFormat::capture(
            unitsin,
            absin.values("absin", "convertmany")?,
            dopplerin,
            "dopplerin",
        )?;
        let output = MixFormat::capture(
            unitsout,
            absout.values("absout", "convertmany")?,
            dopplerout,
            "dopplerout",
        )?;
        let shape = shape.values("convertmany")?;
        let result = py.detach(|| {
            let mut state = self.state();
            install_range_from_shape(&mut state.coordinates, &shape);
            let values = values
                .ok_or_else(|| raised("Failed AlwaysAssert coordIn.shape().nelements()==2"))?;
            let transform = MixedTransform::new(&state.coordinates, values.nrows(), input, output)?;
            let mut result = Array2::<f64>::zeros(values.dim().f());
            for (at, mut converted) in values.columns().into_iter().zip(result.columns_mut()) {
                let at = at.as_slice().expect("contiguous coordinate column");
                for (entry, value) in converted
                    .iter_mut()
                    .zip(transform.apply(&state.coordinates, at)?)
                {
                    *entry = value;
                }
            }
            Ok::<_, PyErr>(result)
        })?;
        Ok(result.into_pyarray(py).into_any().unbind())
    }
}
