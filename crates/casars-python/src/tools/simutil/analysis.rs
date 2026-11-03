// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simutil`'s analysis helpers, which CASA's `simanalyze` and `simalma` use
//! on a simulated image: `ismstp`, `cellsize`, `statim` and `convimage`,
//! over [`casa_simulation::simutil::analysis`], which records where its
//! results depart from CASA's.
//!
//! Each returns what CASA 6.7.6.14's returns:
//! - `ismstp`: a `bool`;
//! - `cellsize`: a list of two quantity records, in the image's direction
//!   units (radians);
//! - `statim`: `(min, max, rms, unit)`, the three values NumPy 0-d `float64`
//!   arrays, as CASA's `np.squeeze` leaves them;
//! - `convimage`: `None`, having written `<modelflat>.regrid` and
//!   `<modelflat>.regrid.conv`.
//!
//! `statim`'s plot is not drawn yet (#1025): with `plot=True`, CASA's
//! default, it says so and returns the statistics. `convimage` with a
//! `complist` is refused (#1053).

use std::path::PathBuf;

use casa_images::PagedImage;
use casa_simulation::simutil::analysis::{
    AnalysisError, cell_size, convimage, is_total_power, statim,
};
use numpy::IntoPyArray;
use pyo3::exceptions::{PyNotImplementedError, PyOSError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::{LOGGER, PySimutil};
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

/// A refusal CASA raises through `simutil.msg(..., priority='error')` is a
/// `RuntimeError` with its message; a file that cannot be read an `OSError`.
fn analysis_raised(error: AnalysisError) -> PyErr {
    match error {
        AnalysisError::Io { .. } => PyOSError::new_err(error.to_string()),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

#[pymethods]
impl PySimutil {
    /// Whether `s` is a total-power MeasurementSet: its first antenna's name
    /// holds `TP`, it has one antenna, or every UVW value is zero. With
    /// `halt`, a MeasurementSet that is not, or cannot be read, raises with
    /// CASA's message; otherwise either is `False`.
    #[pyo3(signature = (s, halt = false), text_signature = "($self, s, halt=False)")]
    fn ismstp(&self, py: Python<'_>, s: PathBuf, halt: bool) -> PyResult<bool> {
        let shown = s.display().to_string();
        match py.detach(|| is_total_power(&s)) {
            Ok(true) => Ok(true),
            Ok(false) if halt => Err(PyRuntimeError::new_err(format!(
                "input file '{shown}' is not a totalpower ms"
            ))),
            Err(_) if halt => Err(PyRuntimeError::new_err(format!(
                "can't understand the file '{shown}'"
            ))),
            _ => Ok(false),
        }
    }

    /// The image's cell on the sky: `[cellx, celly]`, quantity records in
    /// radians, positive. An image rotated with respect to latitude and
    /// longitude raises CASA's message.
    #[pyo3(signature = (image), text_signature = "($self, image)")]
    fn cellsize(&self, py: Python<'_>, image: PathBuf) -> PyResult<Py<PyAny>> {
        let [x, y] = py
            .detach(|| {
                let opened = PagedImage::<f32>::open(&image)?;
                cell_size(opened.coordinates())
            })
            .map_err(analysis_raised)?;
        CasaValue::List(vec![
            CasaValue::quantity(x, "rad"),
            CasaValue::quantity(y, "rad"),
        ])
        .into_python(py)
    }

    /// The image's minimum, maximum, the rms of its lower-left quarter and
    /// its brightness unit, as CASA's `statim` gives them. `plot`, CASA's
    /// default, draws nothing yet (#1025); `incell`, `disprange`, `bar` and
    /// `showstats` only shape that plot.
    #[pyo3(
        signature = (image, plot = true, incell = None, disprange = None, bar = true, showstats = true),
        text_signature = "($self, image, plot=True, incell=None, disprange=None, bar=True, showstats=True)"
    )]
    // CASA's signature: the plot's controls are parameters of their own.
    #[allow(unused_variables, clippy::too_many_arguments)]
    fn statim<'py>(
        &self,
        py: Python<'py>,
        image: PathBuf,
        plot: bool,
        incell: Option<&Bound<'py, PyAny>>,
        disprange: Option<&Bound<'py, PyAny>>,
        bar: bool,
        showstats: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        if plot {
            emit(
                py,
                LOGGER,
                &[LogLine::warning(
                    "statim's plot is not yet implemented in casa-rs (#1025): no figure is drawn",
                )],
            )?;
        }
        let stats = py.detach(|| statim(&image)).map_err(analysis_raised)?;
        let zero_d = |value: f64| ndarray::arr0(value).into_pyarray(py).into_any();
        PyTuple::new(
            py,
            [
                zero_d(stats.min),
                zero_d(stats.max),
                zero_d(stats.rms),
                stats.unit.into_pyobject(py)?.into_any(),
            ],
        )
    }

    /// Regrid the flat sky model `modelflat` onto the flat image `outflat`
    /// and convolve it with the image's restoring beam: `<modelflat>.regrid`
    /// and `<modelflat>.regrid.conv`, as CASA writes them. A `complist` is
    /// refused (#1053).
    #[pyo3(signature = (modelflat, outflat, complist = ""), text_signature = "($self, modelflat, outflat, complist='')")]
    fn convimage(
        &self,
        py: Python<'_>,
        modelflat: PathBuf,
        outflat: PathBuf,
        complist: &str,
    ) -> PyResult<()> {
        if !complist.is_empty() {
            return Err(PyNotImplementedError::new_err(
                "simutil.convimage with a complist is not yet implemented in casa-rs (#1053)",
            ));
        }
        py.detach(|| convimage(&modelflat, &outflat))
            .map(|_| ())
            .map_err(analysis_raised)
    }
}
