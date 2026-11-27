// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.deconvolvecomponentlist` wrapper delegating to [`casa_imageanalysis::tool::beam_ops`].

use casa_components::ShapeType;
use casa_core::RecordValue;
use casa_imageanalysis::tool::attachment::AttachedImage;
use casa_imageanalysis::tool::beam_ops::{
    deconvolve_attached_beam_context, deconvolve_gaussian_source,
};
use casa_images::GaussianBeam;
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyList, PyString};

use crate::convert::value::CasaValue;
use crate::tools::{LogLine, componentlist::image_component_records, emit, table::casa_record};

use super::PyImageTool;
use super::handle::raised;

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

fn deconvolved_record(
    attachment: &AttachedImage,
    input: &RecordValue,
    channel: i64,
    polarization: i64,
    logs: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let beam =
        deconvolve_attached_beam_context(attachment, channel, polarization).map_err(raised)?;
    let components = image_component_records(input, logs)?;
    let mut fields = vec![("nelements".into(), CasaValue::Int(components.len() as i64))];
    for (index, component) in components.enumerate() {
        let mut component = component?;
        match component.shape {
            ShapeType::Point => {}
            ShapeType::Gaussian => {
                let axes = component
                    .gaussian_axes
                    .as_ref()
                    .expect("Gaussian record axes");
                let fit = deconvolve_gaussian_source(axes, beam).map_err(raised)?;
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
            deconvolved_record(attachment, &record, channel, polarization, &mut logs)
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
