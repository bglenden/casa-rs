// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.simutil.simutil`: the part of CASA's `simutil` helper class
//! (`casatasks.private.simutil`) a `sm` script uses, over
//! [`casa_simulation::readantenna`].
//!
//! `simutil` is a Python class of CASA's tasks, not a CASA tool, so it is
//! not under `casars.tools`. Its module path is provisional: where CASA's
//! private task helpers live in casa-rs is open with the owner. ADR-0018
//! puts its computation in Rust, so the class is declared here like the
//! tools, with CASA's constructor signature.
//!
//! Only `readantenna` is declared; the rest of `simutil` (pointings,
//! sensitivities, sky-model handling) comes with `simobserve` in simulation
//! Phase 5. The constructor takes CASA's parameters and keeps none of them,
//! since `readantenna` uses none.
//!
//! Not being a CASA tool, the class is not in the object-surface bundle
//! (whose CASA tools are [`crate::tools::TOOLS`]); its Python tests compare
//! its signatures with CASA's installed class
//! (`python/tests/data/casa_simutil_signatures.json`).
//!
//! `readantenna(antab)` returns CASA's tuple `(x, y, z, d, padnames,
//! antnames, telescopename, posobs)`: ITRF x, y and z as lists of `float`,
//! the dish diameters as a NumPy array, the pad and antenna names as lists
//! of `str`, the telescope name and the observatory's WGS84 position
//! record. CASA's errors (`Exception` with CASA's message) are
//! `RuntimeError`s here, and a file that cannot be opened is an `OSError`.
//! [`casa_simulation::readantenna`] records where its reading differs from
//! CASA's; also, CASA's LOC files give the positions as NumPy `float64`
//! scalars, which here are `float`s.

use std::path::PathBuf;

use casa_simulation::readantenna::{ReadAntennaError, read_antenna_config};
use pyo3::exceptions::{PyFileNotFoundError, PyOSError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;

use crate::convert::Variant;
use crate::convert::holder::{CasaMeasure, Measure};
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

/// The Python logger `simutil`'s messages go to.
const LOGGER: &str = "casars.simutil";

/// CASA's `simutil` helper class.
#[pyclass(name = "simutil", module = "casars.simutil", frozen)]
pub(crate) struct PySimutil;

/// A configuration-file error as the Python exception CASA's raises.
fn raised(error: &ReadAntennaError) -> PyErr {
    match error {
        ReadAntennaError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound => {
            PyFileNotFoundError::new_err(error.to_string())
        }
        ReadAntennaError::Io { .. } => PyOSError::new_err(error.to_string()),
        _ => PyRuntimeError::new_err(error.to_string()),
    }
}

#[pymethods]
impl PySimutil {
    #[new]
    #[allow(unused_variables)]
    #[pyo3(
        signature = (
            direction = Variant::Text(String::new()),
            centerfreq = Variant::Empty,
            bandwidth = Variant::Empty,
            totaltime = Variant::Empty,
            verbose = false
        ),
        text_signature = "(direction='', centerfreq={'unit': 'GHz', 'value': 245.0}, bandwidth={'unit': 'GHz', 'value': 1.0}, totaltime={'unit': 'h', 'value': 0.0}, verbose=False)"
    )]
    fn new(
        direction: Variant,
        centerfreq: Variant,
        bandwidth: Variant,
        totaltime: Variant,
        verbose: bool,
    ) -> Self {
        Self
    }

    /// Read an antenna configuration file: `(x, y, z, d, padnames,
    /// antnames, telescopename, posobs)`.
    #[pyo3(signature = (antab = None), text_signature = "($self, antab=None)")]
    fn readantenna(&self, py: Python<'_>, antab: Option<PathBuf>) -> PyResult<Py<PyAny>> {
        let Some(path) = antab else {
            return Err(PyTypeError::new_err(
                "readantenna needs the antenna configuration file (antab)",
            ));
        };
        emit(
            py,
            LOGGER,
            &[LogLine::info(format!(
                "Reading antenna positions from '{}'",
                path.display()
            ))],
        )?;
        let config = py.detach(|| {
            let measures = casa_ms::open_measures_runtime()
                .map_err(|error| PyRuntimeError::new_err(format!("measures data: {error}")))?;
            read_antenna_config(&path, &*measures).map_err(|error| raised(&error))
        })?;
        let floats = |values: &[f64]| {
            CasaValue::List(values.iter().copied().map(CasaValue::Float).collect())
        };
        let texts = |values: &[String]| {
            CasaValue::List(values.iter().cloned().map(CasaValue::Text).collect())
        };
        let observatory = Measure::from_casa(&CasaMeasure::Position(config.observatory.clone()));
        CasaValue::Tuple(vec![
            floats(&config.x),
            floats(&config.y),
            floats(&config.z),
            CasaValue::floats(config.dish_diameter.clone()),
            texts(&config.pad_names),
            texts(&config.antenna_names),
            CasaValue::Text(config.telescope.clone()),
            observatory.to_value(),
        ])
        .into_python(py)
    }
}
