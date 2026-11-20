// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_components::ShapeType;
use casa_coordinates::CoordinateSystem;
use casa_images::{BeamComponent, GaussianBeam};
use casa_types::RecordValue;
use casa_types::quanta::{Quantity, Unit, casacore};
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyList, PyString};

use crate::convert::value::CasaValue;
use crate::tools::{LogLine, componentlist::image_component_records, emit, table::casa_record};

use super::PyImageTool;
use super::beam_edit::beam_for_units;
use super::beam_view::BeamView;
use super::handle::{ImageHandle, raised};

fn supplied<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(Some(value.clone()))
}

fn plane_index(value: Option<Bound<'_, PyAny>>, name: &str) -> PyResult<(i64, bool)> {
    let Some(value) = value else {
        return Ok((-1, false));
    };
    if value.is_instance_of::<PyFloat>()
        || value.is_instance_of::<PyString>()
        || value.is_instance_of::<PyBytes>()
        || value.is_instance_of::<PyList>()
        || value.is_instance_of::<PyDict>()
    {
        return Err(PyTypeError::new_err(format!(
            "argument _{name} must be an integer"
        )));
    }
    // The original binding invokes image metadata validation even when
    // conversion sets a Python error and returns the sentinel -1.
    Ok(value
        .extract::<i64>()
        .map_or((-1, true), |index| (i64::from(index as i32), false)))
}

fn replace_shape(output: &mut CasaValue, fit: GaussianBeam) -> PyResult<()> {
    fn field<'a>(value: &'a mut CasaValue, name: &str) -> &'a mut CasaValue {
        let CasaValue::Record(fields) = value else {
            unreachable!("component record")
        };
        &mut fields
            .iter_mut()
            .find(|(key, _)| key == name)
            .expect("serialized component field")
            .1
    }
    let shape = field(output, "shape");
    let angle = fit.position_angle_in("deg").map_err(raised)?;
    for (key, value) in [
        (
            "majoraxis",
            CasaValue::quantity(fit.major_in("arcsec").map_err(raised)?, "arcsec"),
        ),
        (
            "minoraxis",
            CasaValue::quantity(fit.minor_in("arcsec").map_err(raised)?, "arcsec"),
        ),
        (
            "positionangle",
            CasaValue::quantity(if angle < 0.0 { angle + 180.0 } else { angle }, "deg"),
        ),
        ("majoraxiserror", CasaValue::quantity(0.0, "arcmin")),
        ("minoraxiserror", CasaValue::quantity(0.0, "arcmin")),
        ("positionangleerror", CasaValue::quantity(0.0, "deg")),
    ] {
        *field(shape, key) = value;
    }
    let error = field(field(shape, "direction"), "error");
    *field(error, "latitude") = CasaValue::quantity(0.0, "rad");
    *field(error, "longitude") = CasaValue::quantity(0.0, "rad");
    Ok(())
}

fn source_beam(axes: &[Quantity; 3], rad: &Unit) -> PyResult<GaussianBeam> {
    let [major, minor, pa] = axes;
    let radians = |quantity| casacore::get_conformant_value(quantity, rad).map_err(raised);
    let major_rad = radians(major)?;
    let minor_rad = radians(minor)?;
    let angle_rad = radians(pa)?;
    if angle_rad.abs() > std::f64::consts::TAU {
        return Err(raised(
            "Gaussian2DParam<T>::setPA(const T &pa) - PA must be in radians and between -2pi and 2pi",
        ));
    }
    // GaussianShape starts at one arcminute and stores an axial ratio.
    // setWidth narrows the minor axis first for a smaller source major axis.
    // Preserve both ratio operations and TwoSidedShape's unit round trip:
    // a rounding bit can change an unresolved source into a resolved fit.
    let default_width = Quantity::new(1.0, "arcmin").expect("built-in angular unit");
    let default_width = radians(&default_width)?;
    let minor_rad = if major_rad <= default_width {
        default_width * (minor_rad / default_width)
    } else {
        minor_rad
    };
    let minor_rad = major_rad * (minor_rad / major_rad);
    let in_record_units = |value, unit| {
        casacore::convert(&Quantity::with_unit(value, rad.clone()), Some(unit)).map_err(raised)
    };
    let angle_rad = angle_rad % std::f64::consts::PI;
    let angle_rad = if angle_rad < 0.0 {
        angle_rad + std::f64::consts::PI
    } else {
        angle_rad
    };
    let angle = in_record_units(angle_rad, pa.unit())?;
    let angle = casa_images::beam::wrap_position_angle(&angle).map_err(raised)?;
    Ok(GaussianBeam::new(
        radians(&in_record_units(major_rad, major.unit())?)?,
        radians(&in_record_units(minor_rad, minor.unit())?)?,
        radians(&angle)?,
    ))
}

fn deconvolved_record(
    image: &ImageHandle,
    beam_view: &BeamView,
    input: &RecordValue,
    channel: i64,
    polarization: i64,
    logs: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    image.check_single_precision("deconvolvecomponentlist")?;
    let components = image_component_records(input, logs)?;
    let missing_beam = || raised("Exception: This image does not have a restoring beam.");
    if beam_view.beam_set().map_err(raised)?.is_empty() {
        return Err(missing_beam());
    }
    if CoordinateSystem::first_direction_in_record(&image.coordinate_record())
        .map_err(raised)?
        .is_none()
    {
        return Err(raised(
            "Exception: This image does not contain a direction coordinate.",
        ));
    }
    let stored_beam = beam_for_units(beam_view, channel, polarization)?.ok_or_else(missing_beam)?;
    let rad = Unit::new("rad").expect("built-in radians unit");
    let radians =
        |quantity: &Quantity| casacore::get_conformant_value(quantity, &rad).map_err(raised);
    let beam = GaussianBeam::new(
        radians(stored_beam.component(BeamComponent::Major))?,
        radians(stored_beam.component(BeamComponent::Minor))?,
        radians(&stored_beam.wrapped_position_angle().map_err(raised)?)?,
    );
    let mut fields = vec![("nelements".into(), CasaValue::Int(components.len() as i64))];
    for (index, component) in components.enumerate() {
        let mut component = component?;
        match component.shape {
            ShapeType::Point => {}
            ShapeType::Gaussian => {
                let source = source_beam(
                    component
                        .gaussian_axes
                        .as_ref()
                        .expect("Gaussian record axes"),
                    &rad,
                )?;
                let (fit, _) = source.fitted_deconvolution(beam).ok_or_else(|| {
                    raised("Source may be only (slightly) resolved in one direction")
                })?;
                if fit.minor == 0.0 {
                    return Err(raised(
                        "Gaussian2DParam<T>::setAxialRatio(const T &axialRatio) - axialRatio must be between (0,1]",
                    ));
                }
                replace_shape(&mut component.value, fit)?;
            }
            other => {
                return Err(raised(format!(
                    "Cannot deconvolve components of type {}",
                    other.casa_name()
                )));
            }
        }
        fields.push((format!("component{index}"), component.value));
    }
    Ok(CasaValue::record(fields))
}

#[pymethods]
impl PyImageTool {
    /// Remove the selected restoring beam from the Gaussian components of a CASA list.
    #[pyo3(signature = (complist, channel = None, polarization = None), text_signature = "($self, complist, channel=-1, polarization=-1)")]
    fn deconvolvecomponentlist(
        &self,
        py: Python<'_>,
        complist: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] channel: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] polarization: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let dictionary = complist
            .cast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("_complist is not a dictionary"))?;
        let record = casa_record(py, Some(dictionary))?;
        let (channel, pending_channel) = plane_index(channel, "channel")?;
        let (polarization, pending_polarization) = plane_index(polarization, "polarization")?;
        let mut logs = Vec::new();
        let result = py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(CasaValue::empty_record());
            };
            deconvolved_record(
                &attachment.lock(),
                &attachment.beam_view(),
                &record,
                channel,
                polarization,
                &mut logs,
            )
        });
        emit(py, "casars.tools.image", &logs)?;
        let result = result?;
        if pending_channel || pending_polarization {
            return Err(PySystemError::new_err(
                "<built-in function image_deconvolvecomponentlist> returned a result with an exception set",
            ));
        }
        result.into_python(py)
    }
}
