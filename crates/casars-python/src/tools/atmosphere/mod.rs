// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's atmospheric transmission tool.

mod profile;
mod quantity;
mod windows;

use std::sync::{Mutex, PoisonError};

use casa_atmosphere::AtmosphereSession;
use casa_provider_contracts::CasaValue;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::convert::reply_value::reply_value;
use crate::tools::{LogLine, emit};

const LOGGER: &str = "casars.tools.atmosphere";
const TYPE_NAMES: [&str; 5] = [
    "TROPICAL",
    "MIDLATSUMMER",
    "MIDLATWINTER",
    "SUBARTSUMMER",
    "SUBARTWINTER",
];

#[pyclass(name = "atmosphere", module = "casars.tools", frozen)]
#[derive(Default)]
pub(crate) struct PyAtmosphere {
    session: Mutex<AtmosphereSession>,
}

#[pymethods]
impl PyAtmosphere {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn close(&self, py: Python<'_>) -> bool {
        self.run(py, AtmosphereSession::close);
        true
    }

    fn done(&self, py: Python<'_>) -> bool {
        self.close(py)
    }

    #[pyo3(name = "getAtmVersion")]
    fn get_atm_version(&self) -> &'static str {
        "ATM-1_0_0"
    }

    #[pyo3(name = "listAtmosphereTypes")]
    fn list_atmosphere_types(&self) -> Vec<String> {
        TYPE_NAMES
            .iter()
            .enumerate()
            .map(|(index, name)| format!("{} - {name}", index + 1))
            .collect()
    }
}

impl PyAtmosphere {
    fn run<T: Send>(
        &self,
        py: Python<'_>,
        operation: impl FnOnce(&mut AtmosphereSession) -> T + Send,
    ) -> T {
        py.detach(|| {
            let mut session = self.session.lock().unwrap_or_else(PoisonError::into_inner);
            operation(&mut session)
        })
    }
}

fn tuple(py: Python<'_>, items: impl IntoIterator<Item = CasaValue>) -> PyResult<Py<PyAny>> {
    let values = items
        .into_iter()
        .map(|item| reply_value(py, &item))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyTuple::new(py, values)?.into_any().unbind())
}

fn warn(py: Python<'_>, message: &str) -> PyResult<()> {
    emit(py, LOGGER, &[LogLine::warning(message)])
}

fn error(message: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(message.to_string())
}
