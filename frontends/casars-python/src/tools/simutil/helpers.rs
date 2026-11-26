// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simutil`'s small helpers, as CASA 6.7.6.14's `simutil.py` has them:
//! `msg` and the report file (`isreport`, `openreport`, `closereport`),
//! `dir_s2m`, `is4d`, `spectral` and `setcfg`.
//!
//! `msg` logs on the Python logger `casars.<origin>` (`casars.simutil` with
//! no origin) at the level CASA's log shows:
//! - `priority=None`, `'info'` and any priority CASA does not know: INFO;
//! - `'warn'`: WARNING. CASA passes a priority like `'warning'` on to its
//!   logger, which does not know it and shows INFO, so casa-rs logs INFO;
//! - `'debug'`, `'debug2'` and the like: DEBUG, below CASA's default
//!   filter;
//! - `'error'`: raises, a `RuntimeError` with the message, as every
//!   `simutil` refusal here is (CASA's is an `Exception`).
//!
//! With `verbose`, CASA shows every message at INFO, writes it to the open
//! report, and posts it a second time with ANSI colour codes; casa-rs logs
//! it once at INFO, without them, and writes the report as CASA does.
//!
//! The report is a Python file object, `self.report`, as in CASA, whose
//! tasks write to it. `isreport` stays true after `closereport`, as CASA's
//! does, and a verbose message then fails writing to the closed file.

use std::path::Path;

use casa_simulation::simutil::config::default_mount;
use casa_simulation::simutil::direction::{Directions, average_direction, direction_measure};
use casa_simulation::simutil::skymodel::{is_4d, spectral_size};
use numpy::IntoPyArray;
use pyo3::exceptions::{PyAttributeError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use super::{LOGGER, PySimutil, ops};
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

/// The log line CASA's `msg` posts for `priority` (upper-cased).
fn logged(message: &str, priority: &str, verbose: bool) -> LogLine {
    if verbose {
        return LogLine::info(message);
    }
    match priority {
        "WARN" => LogLine::warning(message),
        "DEBUG" => LogLine::debug(message),
        debug if debug.len() == 6 && debug.starts_with("DEBUG") => LogLine::debug(message),
        _ => LogLine::info(message),
    }
}

impl PySimutil {
    /// CASA's `simutil.msg`.
    fn post(
        &self,
        py: Python<'_>,
        message: &str,
        origin: Option<&str>,
        priority: Option<&str>,
    ) -> PyResult<()> {
        let priority = priority.map_or_else(|| "INFO".to_string(), str::to_uppercase);
        if priority == "ERROR" {
            return Err(PyRuntimeError::new_err(message.to_string()));
        }
        if self.verbose && self.isreport(py) {
            let line = match origin {
                Some(origin) => format!("[{origin}] {message}\n"),
                None => format!("{message}\n"),
            };
            let report = self.report.as_ref().expect("isreport saw a report");
            report.bind(py).call_method1("write", (line,))?;
        }
        let logger = match origin {
            Some(origin) => format!("casars.{origin}"),
            None => LOGGER.to_string(),
        };
        emit(py, &logger, &[logged(message, &priority, self.verbose)])
    }
}

#[pymethods]
impl PySimutil {
    /// Log `s` at `priority` (`info`, `warn`, `debug` or `error`, which
    /// raises), from `origin`; with `verbose`, also to the open report.
    #[pyo3(signature = (s, origin = None, priority = None), text_signature = "($self, s, origin=None, priority=None)")]
    fn msg(
        &self,
        py: Python<'_>,
        s: &str,
        origin: Option<&str>,
        priority: Option<&str>,
    ) -> PyResult<()> {
        self.post(py, s, origin, priority)
    }

    /// Whether a report file is open: `self.report` is the file named by
    /// `self.reportfile`.
    fn isreport(&self, py: Python<'_>) -> bool {
        let (Some(report), Some(reportfile)) = (&self.report, &self.reportfile) else {
            return false;
        };
        report
            .bind(py)
            .getattr("name")
            .and_then(|name| name.eq(reportfile.bind(py)))
            .unwrap_or(false)
    }

    /// Open `self.reportfile` as `self.report`, appending to a file that
    /// exists.
    fn openreport(&mut self, py: Python<'_>) -> PyResult<()> {
        let refused = || PyRuntimeError::new_err("Can't open reportfile because it's not defined");
        let Some(reportfile) = &self.reportfile else {
            return Err(refused());
        };
        let reportfile = reportfile.bind(py);
        let mode = match reportfile.extract::<std::path::PathBuf>() {
            Ok(path) if path.exists() => "a",
            _ => "w",
        };
        let report = py
            .import("builtins")?
            .call_method1("open", (reportfile, mode))
            .map_err(|_| refused())?;
        self.report = Some(report.unbind());
        Ok(())
    }

    /// Close the report file.
    fn closereport(&self, py: Python<'_>) -> PyResult<()> {
        let Some(report) = &self.report else {
            return Err(PyAttributeError::new_err(
                "'simutil' object has no attribute 'report'",
            ));
        };
        report.bind(py).call_method0("close")?;
        Ok(())
    }

    /// A direction string `'refcode lon lat'` (else `self.direction`; a
    /// list, its mean) as a direction measure: J2000 when it names no
    /// frame, degrees for angles without a unit.
    #[pyo3(signature = (direction = None), text_signature = "($self, direction=None)")]
    fn dir_s2m(&self, py: Python<'_>, direction: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let given = direction.map_or_else(|| self.direction.bind(py).clone(), Clone::clone);
        let direction = match ops::directions(&given)? {
            Directions::One(direction) => direction,
            Directions::List(directions) => {
                average_direction(&directions).map_err(ops::raised)?.centre
            }
        };
        if direction.split_whitespace().count() != 3 && self.verbose {
            self.post(
                py,
                &format!("assuming J2000 for {direction}"),
                Some("simutil.s2m"),
                None,
            )?;
        }
        let (refer, [longitude, latitude]) = direction_measure(&direction).map_err(ops::raised)?;
        CasaValue::record(vec![
            ("type", CasaValue::Text("direction".to_string())),
            ("refer", CasaValue::Text(refer.as_str().to_string())),
            ("m0", CasaValue::quantity(longitude, "rad")),
            ("m1", CasaValue::quantity(latitude, "rad")),
        ])
        .into_python(py)
    }

    /// Whether the image has CASA's four axes in its order: direction,
    /// Stokes, spectral.
    #[pyo3(signature = (image), text_signature = "($self, image)")]
    fn is4d(&self, py: Python<'_>, image: std::path::PathBuf) -> PyResult<bool> {
        py.detach(|| is_4d(Path::new(&image)))
            .map_err(ops::sky_model_raised)
    }

    /// The image's spectral size: `(nchan, width)`, the channel count as a
    /// one-element NumPy array and the width as a string (`'2000000.0Hz'`),
    /// or `0.0` for an image without a spectral axis.
    #[pyo3(signature = (image), text_signature = "($self, image)")]
    fn spectral<'py>(
        &self,
        py: Python<'py>,
        image: std::path::PathBuf,
    ) -> PyResult<Bound<'py, PyAny>> {
        let size = py
            .detach(|| spectral_size(Path::new(&image)))
            .map_err(ops::sky_model_raised)?;
        let Some(size) = size else {
            return Ok(0.0_f64.into_pyobject(py)?.into_any());
        };
        #[allow(clippy::cast_possible_wrap)]
        let channels = vec![size.channels as i64].into_pyarray(py).into_any();
        Ok(PyTuple::new(py, [channels, size.width.into_pyobject(py)?.into_any()])?.into_any())
    }

    /// Give the simulator `mysm` the configuration, as CASA's `setcfg`
    /// does: global coordinates, `mounttype` (else CASA's guess for the
    /// telescope) for every antenna, and `diam.tolist()` as the dish
    /// diameters. Returns the mount; a refused configuration closes `mysm`
    /// and raises `ValueError`.
    #[pyo3(
        signature = (mysm, telescope, x, y, z, diam, padnames, posobs, mounttype = None),
        text_signature = "($self, mysm, telescope, x, y, z, diam, padnames, posobs, mounttype=None)"
    )]
    // CASA's signature: one value each.
    #[allow(clippy::too_many_arguments)]
    fn setcfg(
        &self,
        py: Python<'_>,
        mysm: &Bound<'_, PyAny>,
        telescope: &str,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        z: &Bound<'_, PyAny>,
        diam: &Bound<'_, PyAny>,
        padnames: &Bound<'_, PyAny>,
        posobs: &Bound<'_, PyAny>,
        mounttype: Option<&str>,
    ) -> PyResult<String> {
        let mount = match mounttype {
            Some(mount) if !mount.is_empty() => mount.to_string(),
            _ => default_mount(telescope).to_string(),
        };
        let kwargs = PyDict::new(py);
        kwargs.set_item("telescopename", telescope)?;
        kwargs.set_item("x", x)?;
        kwargs.set_item("y", y)?;
        kwargs.set_item("z", z)?;
        kwargs.set_item("dishdiameter", diam.call_method0("tolist")?)?;
        kwargs.set_item("mount", PyList::new(py, [&mount])?)?;
        kwargs.set_item("padname", padnames)?;
        kwargs.set_item("coordsystem", "global")?;
        kwargs.set_item("referencelocation", posobs)?;
        if !mysm
            .call_method("setconfig", (), Some(&kwargs))?
            .is_truthy()?
        {
            mysm.call_method0("close")?;
            return Err(PyValueError::new_err("Error setting the configuration"));
        }
        Ok(mount)
    }
}
