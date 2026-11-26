// SPDX-License-Identifier: LGPL-3.0-or-later
//! Numerical simulation-planning diagnostics over native Rust calculations.

use casa_core::quanta::{Dimension, Unit, UnitDim};
use casa_simulation::simutil::beam::spheroidal_beam;
use casa_simulation::simutil::sensitivity::{SensitivitySpec, array_sensitivity, sensitivity};
use casa_simulation::{ExecutionControls, SessionResources, SimulatorOptions};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use super::{PySimutil, ops};
use crate::convert::Variant;
use crate::convert::value::CasaValue;

fn angle(value: Option<&Variant>, message: &str) -> PyResult<f64> {
    let invalid = || PyValueError::new_err(message.to_string());
    let quantity = ops::quantity(value.ok_or_else(invalid)?).map_err(|_| invalid())?;
    if quantity.unit().val().dim != UnitDim::basis(Dimension::Angle) {
        return Err(invalid());
    }
    quantity
        .get_value_in(&Unit::new("arcsec").expect("arcseconds"))
        .map_err(|_| invalid())
}

fn in_unit(value: &Variant, unit: &str) -> PyResult<f64> {
    ops::quantity(value)?
        .get_value_in(&Unit::new(unit).expect("diagnostic unit"))
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pymethods]
impl PySimutil {
    /// Theoretical two-polarization point-source sensitivity, Jy, from a
    /// synthetic track and the existing simulator's Tsys/noise models.
    #[pyo3(signature = (freq, bandwidth, etime, elevation, pwv = None, telescope = None, diam = None, nant = None, antennalist = None, doimnoise = None, integration = None, debug = None, method = None, tau0 = None, t_sky = None), text_signature = "($self, freq, bandwidth, etime, elevation, pwv=None, telescope=None, diam=None, nant=None, antennalist=None, doimnoise=None, integration=None, debug=None, method='tsys-atm', tau0=None, t_sky=None)")]
    #[allow(clippy::too_many_arguments)]
    fn sensitivity(
        &mut self,
        py: Python<'_>,
        freq: Variant,
        bandwidth: Variant,
        etime: Variant,
        elevation: Variant,
        pwv: Option<f64>,
        telescope: Option<String>,
        diam: Option<f64>,
        nant: Option<usize>,
        antennalist: Option<String>,
        doimnoise: Option<bool>,
        integration: Option<Variant>,
        debug: Option<bool>,
        method: Option<String>,
        tau0: Option<f64>,
        t_sky: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        let spec = SensitivitySpec {
            frequency_hz: in_unit(&freq, "Hz")?,
            bandwidth_hz: in_unit(&bandwidth, "Hz")?,
            time_s: in_unit(&etime, "s")?,
            elevation_rad: in_unit(&elevation, "rad")?,
            pwv_mm: pwv.unwrap_or(2.0),
            integration_s: integration
                .as_ref()
                .map(|value| in_unit(value, "s"))
                .transpose()?,
            method: method.unwrap_or_else(|| "tsys-atm".into()),
            tau0: tau0.unwrap_or(0.1),
            sky_k: t_sky.unwrap_or(200.0),
            debug: debug.unwrap_or(false),
            image_noise: doimnoise.unwrap_or(false),
        };
        if spec.elevation_rad < 3.0_f64.to_radians() {
            return Err(PyRuntimeError::new_err("Elevation < ALMA limit of 3 deg"));
        }
        if spec.method == "tsys-manual" && tau0.is_none() {
            return Err(PyValueError::new_err(
                "tau0 must be supplied for tsys-manual sensitivity",
            ));
        }
        let Some(antennalist) = antennalist else {
            for (missing, message) in [
                (telescope.is_none(), "Telescope name has not been set."),
                (diam.is_none(), "Antenna diameter has not been set."),
                (nant.is_none(), "Number of antennas has not been set."),
            ] {
                if missing {
                    return Err(PyRuntimeError::new_err(message));
                }
            }
            if doimnoise.unwrap_or(false) {
                return Err(PyValueError::new_err(
                    "An antenna configuration is required to calculate image noise.",
                ));
            }
            let telescope = telescope.expect("checked telescope");
            let value = py.detach(|| {
                let resources = SessionResources::from_controls(&ExecutionControls::default())
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                let options = SimulatorOptions::from_system(resources)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                if options
                    .measures
                    .observatory(&telescope)
                    .map_err(PyRuntimeError::new_err)?
                    .is_none()
                {
                    return Err(PyRuntimeError::new_err(
                        "Unknown telescope and no antenna list.",
                    ));
                }
                array_sensitivity(
                    &telescope,
                    diam.expect("checked diameter"),
                    nant.expect("checked antenna count"),
                    &spec,
                    resources,
                )
                .map_err(ops::raised)
            })?;
            return CasaValue::Float(value).into_python(py);
        };
        let mut path = std::path::PathBuf::from(&antennalist);
        if !path.exists() {
            let mut roots = Vec::new();
            if let Some(root) = std::env::var_os("CASA_RS_MEASURESPATH") {
                roots.push(std::path::PathBuf::from(root));
            }
            if let Ok(root) = std::env::var("CASAPATH")
                && let Some(root) = root.split_whitespace().next()
            {
                roots.push(std::path::Path::new(root).join("data"));
            }
            path = roots
                .iter()
                .map(|root| root.join("alma/simmos").join(&antennalist))
                .find(|candidate| candidate.exists())
                .ok_or_else(|| {
                    PyRuntimeError::new_err(format!("antennalist {antennalist} not found"))
                })?;
        }
        let (value, telescope) = py.detach(|| {
            let config = super::configuration(&path)?;
            let resources = SessionResources::from_controls(&ExecutionControls::default())
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let options = SimulatorOptions::from_system(resources)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let value = sensitivity(&config, &spec, options).map_err(ops::raised)?;
            Ok::<_, PyErr>((value, config.telescope))
        })?;
        self.keep_telescope(py, telescope);
        match value.image_rms_jy {
            Some(rms) => CasaValue::Tuple(vec![
                CasaValue::Float(value.theoretical_jy),
                CasaValue::Float(rms),
            ])
            .into_python(py),
            None => CasaValue::Float(value.theoretical_jy).into_python(py),
        }
    }

    /// FWHM of a Gaussian primary beam convolved with the SF kernel and
    /// optional pointing-spacing box; returns an arcsecond quantity.
    #[pyo3(name = "sfBeam1d", signature = (beam = None, cell = None, convsupport = None, sampling = None), text_signature = "($self, beam='', cell='', convsupport=-1, sampling='')")]
    fn sf_beam_1d(
        &self,
        py: Python<'_>,
        beam: Option<Variant>,
        cell: Option<Variant>,
        convsupport: Option<i64>,
        sampling: Option<Variant>,
    ) -> PyResult<Py<PyAny>> {
        let beam = angle(
            beam.as_ref(),
            "beam should be a quantity of antenna primary beam size (angle)",
        )?;
        let cell = angle(
            cell.as_ref(),
            "cell should be a quantity of image pixel size (angle)",
        )?;
        let sampling = sampling
            .as_ref()
            .filter(|value| !matches!(value, Variant::Text(text) if text.is_empty()));
        let sampling = sampling
            .map(|value| {
                angle(
                    Some(value),
                    "sampling should be a quantity of pointing spacing (angle)",
                )
            })
            .transpose()?;
        let width = py
            .detach(|| spheroidal_beam(beam, cell, convsupport.unwrap_or(-1), sampling))
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        CasaValue::quantity(width, "arcsec").into_python(py)
    }
}
