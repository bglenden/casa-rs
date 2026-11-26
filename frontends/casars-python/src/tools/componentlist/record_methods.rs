// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python entry points for component records.
use super::{PyComponentList, records, supplied};
use crate::convert::value::CasaValue;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyBool;

#[pymethods]
impl PyComponentList {
    /// Order components by flux, angular distance from zero, or polarization.
    #[pyo3(signature=(criteria=None,log=None),text_signature="($self, criteria='Flux', log=True)")]
    fn sort(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=supplied)] criteria: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with=supplied)] log: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let criteria = super::setspectrum::text(criteria, "Flux", "criteria")?;
        if log.is_some_and(|v| !v.is_instance_of::<PyBool>()) {
            return Err(PyTypeError::new_err(
                "in method 'componentlist_sort', argument 3 of type 'bool'",
            ));
        }
        self.call(py, move |state, _| {
            state.sort(&criteria).map(CasaValue::Bool)
        })
    }

    /// Append a sky component, applying CASA's record defaults and validation.
    #[pyo3(signature=(thecomponent,iknow=None),text_signature="($self, thecomponent, iknow=True)")]
    fn add(
        &self,
        py: Python<'_>,
        thecomponent: &Bound<'_, PyAny>,
        #[pyo3(from_py_with=supplied)] iknow: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let record = records::argument(py, Some(thecomponent), "thecomponent")?;
        if iknow.is_some_and(|v| !v.is_instance_of::<PyBool>()) {
            return Err(PyTypeError::new_err(
                "in method 'componentlist_add', argument 3 of type 'bool'",
            ));
        }
        self.call(py, move |state, lines| {
            state.add_record(&record, lines).map(CasaValue::Bool)
        })
    }
    /// Replace the list from a CASA component-list record.
    #[pyo3(signature=(record=None),text_signature="($self, record={})")]
    fn fromrecord(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=supplied)] record: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let record = records::argument(py, record.as_ref(), "record")?;
        self.call(py, move |state, lines| {
            state.fromrecord(&record, lines).map(CasaValue::Bool)
        })
    }
}
