// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.tools.componentlist`: CASA's `componentlist` tool (`cl`) over
//! [`casa_components::ComponentList`].
//!
//! The class declares every method of CASA's installed binding with CASA's
//! names, parameter order and effective defaults ([`surface`]). The methods
//! CASA's simulation tasks call are implemented (`open`, `addcomponent`,
//! `rename`, `close`, `done`, `length`, `torecord`, `getspectrum`,
//! `getrefdir`), with the accessors `getcomponent`, `getfluxvalue`,
//! `getfluxunit`, `getlabel`, `shapetype`, `spectrumtype`, table keyword
//! reads and writes, and CASA's `iscomponentlist` placeholder. The surface records
//! the other implemented methods and refusals naming #1053.
//!
//! CASA's outcomes are kept ([`ops`] for the table, `add` for
//! `addcomponent`): what CASA raises is a `RuntimeError` with CASA's
//! message, and what it logs and carries on from (a shape `setshape`
//! refuses, `torecord` of an empty list, `close`) is logged on the Python
//! logger `casars.tools.componentlist`, CASA's SEVERE at ERROR and its WARN
//! at WARNING. Records are casacore's (`casa_components` `to_record`), as
//! `dict`s with NumPy arrays. As in CASA, `addcomponent` stores the flux as
//! Stokes whatever `polarization` says.
//!
//! Divergences, recorded in `docs/CASA (C++) bugs.md`: `addcomponent` keeps
//! the flux's double precision (`add`).

mod add;
mod keywords;
mod ops;
mod placeholder;
mod selection;
pub(crate) mod surface;

use std::ffi::CString;
use std::sync::{Mutex, MutexGuard, PoisonError};

use pyo3::exceptions::{PyOverflowError, PyRuntimeWarning, PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyString};

use crate::convert::value::CasaValue;
use crate::convert::{ConvertResult, DoubleList, Variant};
use crate::tools::{LogLine, emit, not_yet_implemented_in};
use add::AddComponent;
use ops::State;

/// The Python logger CASA's `cl` messages go to.
const LOGGER: &str = "casars.tools.componentlist";

/// Keep a supplied Python `None` distinct from an omitted argument.
fn supplied<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    value
        .extract::<Bound<'py, PyAny>>()
        .map(Some)
        .map_err(Into::into)
}

/// CASA's `componentlist` tool.
#[pyclass(name = "componentlist", module = "casars.tools", frozen)]
pub(crate) struct PyComponentList {
    state: Mutex<State>,
}

impl PyComponentList {
    fn refdir_placeholder(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
        precision: Option<&Bound<'_, PyAny>>,
        method: &'static str,
    ) -> PyResult<Py<PyAny>> {
        let index = which.extract::<i64>();
        let mut system_error = which.is_none()
            || index
                .as_ref()
                .err()
                .is_some_and(|e| e.is_instance_of::<PyOverflowError>(py));
        if index.is_err() && !system_error {
            return Err(PyTypeError::new_err("argument _which must be an integer"));
        }
        if unit.is_some_and(|value| !value.is_instance_of::<PyString>()) {
            return Err(PyTypeError::new_err("argument _unit must be a string"));
        }
        if let Some(precision) = precision {
            let extracted = precision.extract::<i64>();
            let precision_system_error = precision.is_none()
                || extracted
                    .as_ref()
                    .err()
                    .is_some_and(|e| e.is_instance_of::<PyOverflowError>(py));
            if extracted.is_err() && !precision_system_error {
                return Err(PyTypeError::new_err(
                    "argument _precision must be an integer",
                ));
            }
            system_error |= precision_system_error;
        }
        let result = self.call(py, |_, lines| {
            lines.push(LogLine::warning(format!("{method} not implemented yet")));
            Ok(CasaValue::Text(String::new()))
        })?;
        if system_error {
            return Err(PySystemError::new_err(format!(
                "<built-in function componentlist_{method}> returned a result with an exception set"
            )));
        }
        Ok(result)
    }

    /// CASA's unimplemented getters log a warning and return placeholders.
    fn getter_placeholder(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        method: &'static str,
        result: CasaValue,
    ) -> PyResult<Py<PyAny>> {
        let index = which.extract::<i64>();
        let system_error = which.is_none()
            || index
                .as_ref()
                .err()
                .is_some_and(|error| error.is_instance_of::<PyOverflowError>(py));
        if index.is_err() && !system_error {
            return Err(PyTypeError::new_err("argument _which must be an integer"));
        }
        let result = self.call(py, |_, lines| {
            lines.push(LogLine::warning(format!("{method} not implemented yet")));
            Ok(result)
        })?;
        if system_error {
            return Err(PySystemError::new_err(format!(
                "<built-in function componentlist_{method}> returned a result with an exception set"
            )));
        }
        Ok(result)
    }

    /// Runs `work` on the state with the GIL released, logs what it reports
    /// and returns its value as Python.
    fn call(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut State, &mut Vec<LogLine>) -> ConvertResult<CasaValue> + Send,
    ) -> PyResult<Py<PyAny>> {
        let mut lines = Vec::new();
        let result = py.detach(|| {
            casa_types::quanta::registry::global_registry()
                .with_locked_units(|| work(&mut self.lock(), &mut lines))
        });
        emit(py, LOGGER, &lines)?;
        result?.into_python(py)
    }

    /// The state lock, taken without the GIL (inside `Python::detach`).
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for PyComponentList {
    /// CASA's destructor writes a list attached for update. The write runs
    /// with the GIL released, as every method's does (ADR-0019), since it
    /// may wait for a lock another process holds; a failure is a
    /// `RuntimeWarning`, raised once the GIL is held again, as nothing can
    /// raise here.
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        Python::attach(|py| {
            if let Err(message) = py.detach(|| {
                casa_types::quanta::registry::global_registry()
                    .with_locked_units(|| state.release())
            }) {
                let category = py.get_type::<PyRuntimeWarning>();
                let message = CString::new(message).unwrap_or_default();
                if let Err(error) = PyErr::warn(py, category.as_any(), &message, 1) {
                    error.write_unraisable(py, None);
                }
            }
        });
    }
}

/// A method casa-rs does not implement yet, refused naming #1053, which
/// classifies them.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("componentlist", method, 1053)
}

#[pymethods]
#[allow(unused_variables)]
impl PyComponentList {
    #[new]
    fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (thecomponent, iknow = None), text_signature = "($self, thecomponent, iknow=True)")]
    fn add(
        &self,
        thecomponent: &Bound<'_, PyAny>,
        iknow: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("add")
    }

    /// Add a component, built by CASA's setters from the arguments.
    #[pyo3(
        signature = (
            flux = Variant::Empty, fluxunit = "Jy", polarization = "Stokes", dir = Variant::Empty,
            shape = "point", majoraxis = Variant::Empty, minoraxis = Variant::Empty,
            positionangle = Variant::Empty, freq = Variant::Empty, spectrumtype = "constant",
            index = Variant::Empty, optionalparms = DoubleList::of(&[0.0]), label = ""
        ),
        text_signature = "($self, flux=[], fluxunit='Jy', polarization='Stokes', dir=[], shape='point', majoraxis=[], minoraxis=[], positionangle=[], freq=[], spectrumtype='constant', index=[], optionalparms=[0.0], label='')"
    )]
    #[allow(clippy::too_many_arguments)]
    fn addcomponent(
        &self,
        py: Python<'_>,
        flux: Variant,
        fluxunit: &str,
        polarization: &str,
        dir: Variant,
        shape: &str,
        majoraxis: Variant,
        minoraxis: Variant,
        positionangle: Variant,
        freq: Variant,
        spectrumtype: &str,
        index: Variant,
        optionalparms: DoubleList,
        label: &str,
    ) -> PyResult<Py<PyAny>> {
        // CASA's binding ignores `optionalparms`.
        let request = AddComponent {
            flux,
            fluxunit: fluxunit.to_string(),
            polarization: polarization.to_string(),
            dir,
            shape: shape.to_string(),
            majoraxis,
            minoraxis,
            positionangle,
            freq,
            spectrumtype: spectrumtype.to_string(),
            index,
            label: label.to_string(),
        };
        self.call(py, move |state, lines| {
            state.addcomponent(&request, lines).map(CasaValue::Bool)
        })
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(
        signature = (filename, asciifile, refer = None, format = None, direction = None, spectrum = None, flux = None, log = None),
        text_signature = "($self, filename, asciifile, refer='J2000', format='ST', direction={}, spectrum={}, flux={}, log=True)"
    )]
    #[allow(clippy::too_many_arguments)]
    fn asciitocomponentlist(
        &self,
        filename: &Bound<'_, PyAny>,
        asciifile: &Bound<'_, PyAny>,
        refer: Option<&Bound<'_, PyAny>>,
        format: Option<&Bound<'_, PyAny>>,
        direction: Option<&Bound<'_, PyAny>>,
        spectrum: Option<&Bound<'_, PyAny>>,
        flux: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("asciitocomponentlist")
    }

    /// Release the list (writing a list opened for update or renamed) and
    /// start an empty one; with `log`, log CASA's warning.
    #[pyo3(signature = (log = true), text_signature = "($self, log=True)")]
    fn close(&self, py: Python<'_>, log: bool) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, lines| {
            let done = state.done()?;
            if log {
                lines.push(LogLine::warning("componentlist closed"));
            }
            Ok(CasaValue::Bool(done))
        })
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (list = None, which = None, log = None), text_signature = "($self, list=[], which=[-1], log=True)")]
    fn concatenate(
        &self,
        list: Option<&Bound<'_, PyAny>>,
        which: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("concatenate")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, polarization = None), text_signature = "($self, which, polarization='Stokes')")]
    fn convertfluxpol(
        &self,
        which: &Bound<'_, PyAny>,
        polarization: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertfluxpol")
    }

    /// Convert one or all components' flux values and errors to a Jy unit.
    #[pyo3(signature = (which, unit = None), text_signature = "($self, which, unit='Jy')")]
    fn convertfluxunit(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] unit: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (which, pending) = match which.extract::<i64>() {
            Ok(index) => (i64::from(index as i32), false),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => {
                (-1, true)
            }
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        let unit: String = match unit {
            None => "Jy".to_string(),
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
            Some(_) => return Err(PyTypeError::new_err("argument _unit must be a string")),
        };
        let unit = unit.split('\0').next().unwrap_or_default().to_string();
        let result = self.call(py, move |state, lines| {
            state
                .convertfluxunit(which, &unit, lines)
                .map(CasaValue::Bool)
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_convertfluxunit> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Change the reference-frequency unit used in one component's records.
    #[pyo3(signature = (which, unit = None), text_signature = "($self, which, unit='GHz')")]
    fn convertfrequnit(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] unit: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let which = match which.extract::<i64>() {
            Ok(index) => i64::from(index as i32),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => -1,
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        let unit: String = match unit {
            None => "GHz".to_string(),
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
            Some(_) => return Err(PyTypeError::new_err("argument _unit must be a string")),
        };
        let unit = unit.split('\0').next().unwrap_or_default().to_string();
        self.call(py, move |state, _| {
            state.convertfrequnit(which, &unit).map(CasaValue::Bool)
        })
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, frame), text_signature = "($self, which, frame)")]
    fn convertrefdir(
        &self,
        which: &Bound<'_, PyAny>,
        frame: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertrefdir")
    }

    /// CASA's placeholder: logs a warning and returns false without changing
    /// the component's shape or units.
    #[pyo3(
        signature = (which, majoraxis = None, minoraxis = None, positionangle = None),
        text_signature = "($self, which, majoraxis='arcmin', minoraxis='arcmin', positionangle='deg')"
    )]
    fn convertshape(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] majoraxis: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] minoraxis: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] positionangle: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let index = which.extract::<i64>();
        let system_error = which.is_none()
            || index
                .as_ref()
                .err()
                .is_some_and(|error| error.is_instance_of::<PyOverflowError>(py));
        if index.is_err() && !system_error {
            return Err(PyTypeError::new_err("argument _which must be an integer"));
        }
        for (name, value) in [
            ("majoraxis", majoraxis),
            ("minoraxis", minoraxis),
            ("positionangle", positionangle),
        ] {
            if value.is_some_and(|value| {
                !value.is_instance_of::<PyString>() && !value.is_instance_of::<PyBytes>()
            }) {
                return Err(PyTypeError::new_err(format!(
                    "argument _{name} must be a string"
                )));
            }
        }
        let result = self.call(py, |_, lines| {
            lines.push(LogLine::warning("convertshape not implemented yet"));
            Ok(CasaValue::Bool(false))
        })?;
        if system_error {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_convertshape> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Deselect component indices.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn deselect(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let (indices, pending) = selection::indices(which)?;
        let result = self.call(py, move |state, lines| {
            state.set_selection(&indices, false, lines)
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_deselect> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Release the list (writing a list opened for update or renamed) and
    /// start an empty one.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn done(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| state.done().map(CasaValue::Bool))
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, log = None), text_signature = "($self, which, log=True)")]
    fn edit(
        &self,
        which: &Bound<'_, PyAny>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("edit")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (record = None), text_signature = "($self, record={})")]
    fn fromrecord(&self, record: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("fromrecord")
    }

    /// Component `which`'s record.
    #[pyo3(signature = (which, iknow = false), text_signature = "($self, which, iknow=False)")]
    fn getcomponent(&self, py: Python<'_>, which: i64, iknow: bool) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| {
            ops::record(state.checked(which)?.to_record())
        })
    }

    /// The real parts of component `which`'s four flux errors.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfluxerror(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let which = match which.extract::<i64>() {
            Ok(index) => i64::from(index as i32),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => -1,
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        self.call(py, move |state, lines| match state.checked(which) {
            Ok(component) => Ok(CasaValue::floats(
                component
                    .flux()
                    .errors()
                    .iter()
                    .map(|value| value.re)
                    .collect(),
            )),
            Err(_) => {
                let message = format!(
                    "Exception: Index out of range. The component number must be between 0 and {}.",
                    state.length() - 1
                );
                lines.push(LogLine::error(format!("Exception Reported: {message}")));
                Err(crate::convert::ConvertError::runtime(message))
            }
        })
    }

    /// CASA's placeholder: logs a warning and returns an empty string.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfluxpol(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.getter_placeholder(py, which, "getfluxpol", CasaValue::Text(String::new()))
    }

    /// The name of component `which`'s flux unit.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfluxunit(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| {
            Ok(CasaValue::Text(
                state.checked(which)?.flux().unit().name().to_string(),
            ))
        })
    }

    /// The real parts of component `which`'s flux values.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfluxvalue(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| {
            let values = state.checked(which)?.flux().values();
            Ok(CasaValue::floats(
                values.iter().map(|value| value.re).collect(),
            ))
        })
    }

    /// CASA's placeholder: logs a warning and returns an empty record.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfreq(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.getter_placeholder(py, which, "getfreq", CasaValue::empty_record())
    }

    /// CASA's placeholder: logs a warning and returns an empty string.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfreqframe(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.getter_placeholder(py, which, "getfreqframe", CasaValue::Text(String::new()))
    }

    /// CASA's placeholder: logs a warning and returns an empty string.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfrequnit(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.getter_placeholder(py, which, "getfrequnit", CasaValue::Text(String::new()))
    }

    /// CASA's placeholder: logs a warning and returns zero.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfreqvalue(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.getter_placeholder(py, which, "getfreqvalue", CasaValue::Float(0.0))
    }

    /// Read a table keyword, including a dotted path into a nested record.
    #[pyo3(signature = (keyword = None), text_signature = "($self, keyword='metadata')")]
    fn getkeyword(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] keyword: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let keyword = keywords::argument(py, keyword.as_ref())?;
        self.call(py, move |state, _| {
            keywords::get(state.keyword_table_path()?, &keyword)
        })
    }

    /// Component `which`'s label.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getlabel(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| {
            Ok(CasaValue::Text(state.checked(which)?.label().to_string()))
        })
    }

    /// Component `which`'s reference direction, a `MeasureHolder` record.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getrefdir(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| state.getrefdir(which))
    }

    /// CASA's placeholder: logs a warning and returns an empty string.
    #[pyo3(signature = (which, unit = None, precision = None), text_signature = "($self, which, unit='deg', precision=6)")]
    fn getrefdirdec(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] unit: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] precision: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        self.refdir_placeholder(py, which, unit.as_ref(), precision.as_ref(), "getrefdirdec")
    }

    /// The reference frame of component `which`'s direction.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getrefdirframe(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let which = match which.extract::<i64>() {
            // CASA's binding passes a C int to the underlying tool.
            Ok(index) => i64::from(index as i32),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => -1,
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        self.call(py, move |state, lines| match state.checked(which) {
            Ok(component) => Ok(CasaValue::Text(
                component
                    .shape()
                    .reference_direction()
                    .refer()
                    .as_str()
                    .into(),
            )),
            Err(_) => {
                let message = format!(
                    "Exception: Index out of range. The component number must be between 0 and {}.",
                    state.length() - 1
                );
                lines.push(LogLine::error(format!("Exception Reported: {message}")));
                Err(crate::convert::ConvertError::runtime(message))
            }
        })
    }

    /// CASA's placeholder: logs a warning and returns an empty string.
    #[pyo3(signature = (which, unit = None, precision = None), text_signature = "($self, which, unit='deg', precision=6)")]
    fn getrefdirra(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] unit: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] precision: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        self.refdir_placeholder(py, which, unit.as_ref(), precision.as_ref(), "getrefdirra")
    }

    /// The component's shape parameters and their current units.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getshape(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let which = match which.extract::<i64>() {
            Ok(index) => i64::from(index as i32),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => -1,
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        self.call(py, move |state, _| state.getshape(which))
    }

    /// Component `which`'s spectral model, as its `toRecord` writes it.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getspectrum(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| {
            ops::record(state.unchecked(which)?.spectrum().to_record())
        })
    }

    /// Whether the attached table has the named top-level keyword.
    #[pyo3(signature = (keyword = None), text_signature = "($self, keyword='metadata')")]
    fn haskeyword(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] keyword: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let keyword = keywords::argument(py, keyword.as_ref())?;
        self.call(py, move |state, _| {
            keywords::has(state.keyword_table_path()?, &keyword)
        })
    }

    /// CASA's placeholder: logs a warning and returns an empty int64 array.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn indices(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |_, lines| {
            lines.push(LogLine::warning("indices not implemented yet"));
            Ok(CasaValue::Ints {
                shape: vec![0],
                values: Vec::new(),
            })
        })
    }

    /// CASA's placeholder logs a warning and returns false.
    #[pyo3(signature = (tool = None), text_signature = "($self, tool=[])")]
    fn iscomponentlist(
        &self,
        py: Python<'_>,
        tool: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let pending = tool
            .map(placeholder::pending_integer_overflow)
            .transpose()?
            .unwrap_or(false);
        let result = self.call(py, |_, lines| {
            lines.push(LogLine::warning("is_componentlist not implemented yet"));
            Ok(CasaValue::Bool(false))
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_iscomponentlist> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Check whether the selected components have physically possible fluxes.
    #[pyo3(signature = (which = None), text_signature = "($self, which=[-1])")]
    fn isphysical(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] which: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (indices, pending) = match which {
            Some(which) => selection::indices(&which)?,
            None => (vec![-1], false),
        };
        let result = self.call(py, move |state, lines| state.is_physical(&indices, lines))?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_isphysical> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// The number of components.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn length(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| Ok(CasaValue::Int(state.length())))
    }

    /// Read the component list `filename`, replacing (and releasing) the
    /// current one; with `nomodify=False`, `close` rewrites it.
    #[pyo3(
        signature = (filename = "", nomodify = false, log = true),
        text_signature = "($self, filename='', nomodify=False, log=True)"
    )]
    fn open(
        &self,
        py: Python<'_>,
        filename: &str,
        nomodify: bool,
        log: bool,
    ) -> PyResult<Py<PyAny>> {
        let filename = filename.to_string();
        self.call(py, move |state, _| {
            state.open(&filename, nomodify).map(CasaValue::Bool)
        })
    }

    /// Discard all removed components.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn purge(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| Ok(CasaValue::Bool(state.purge())))
    }

    /// Write a keyword to the attached table, as CASA's `putkeyword` does.
    #[pyo3(signature = (keyword = None, value = None), text_signature = "($self, keyword='metadata', value=[])")]
    fn putkeyword(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] keyword: Option<Bound<'_, PyAny>>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let name = keywords::argument(py, keyword.as_ref())?;
        let value = crate::tools::table::component_keyword_value(py, value)?;
        self.call(py, move |state, _| {
            keywords::put(state.keyword_table_path()?, &name, value)?;
            Ok(CasaValue::Bool(true))
        })?;
        Ok(py.None())
    }

    /// Append removed components in reverse removal order.
    #[pyo3(signature = (log = None), text_signature = "($self, log=True)")]
    fn recover(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] log: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if log.as_ref().is_some_and(|v| !v.is_instance_of::<PyBool>()) {
            return Err(PyTypeError::new_err(
                "in method 'componentlist_recover', argument 2 of type 'bool'",
            ));
        }
        self.call(py, |state, _| Ok(CasaValue::Bool(state.recover())))
    }

    /// Remove components, keeping them available for recovery.
    #[pyo3(signature = (which = None, log = None), text_signature = "($self, which=[-1], log=True)")]
    fn remove(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] which: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] log: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (indices, pending) = match which {
            Some(which) => selection::indices(&which)?,
            None => (vec![-1], false),
        };
        if log.as_ref().is_some_and(|v| !v.is_instance_of::<PyBool>()) {
            return Err(PyTypeError::new_err(
                "in method 'componentlist_remove', argument 3 of type 'bool'",
            ));
        }
        let result = self.call(py, move |state, lines| {
            state.remove(&indices, lines).map(CasaValue::Bool)
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_remove> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Attach the list to a new table `filename`, written at once (a list
    /// not yet attached), or move its table there.
    #[pyo3(signature = (filename, log = true), text_signature = "($self, filename, log=True)")]
    fn rename(&self, py: Python<'_>, filename: &str, log: bool) -> PyResult<Py<PyAny>> {
        let filename = filename.to_string();
        self.call(py, move |state, _| {
            state.rename(&filename).map(CasaValue::Bool)
        })
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, list, whichones = None), text_signature = "($self, which, list, whichones=[-1])")]
    fn replace(
        &self,
        which: &Bound<'_, PyAny>,
        list: &Bound<'_, PyAny>,
        whichones: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("replace")
    }

    /// CASA's placeholder warns and returns an empty float array.
    #[pyo3(
        signature = (direction = None, pixellatsize = None, pixellongsize = None, frequency = None),
        text_signature = "($self, direction=[], pixellatsize=[], pixellongsize=[], frequency=[])"
    )]
    fn sample(
        &self,
        py: Python<'_>,
        direction: Option<&Bound<'_, PyAny>>,
        pixellatsize: Option<&Bound<'_, PyAny>>,
        pixellongsize: Option<&Bound<'_, PyAny>>,
        frequency: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let mut pending = false;
        for value in [direction, pixellatsize, pixellongsize, frequency]
            .into_iter()
            .flatten()
        {
            pending |= placeholder::pending_integer_overflow(value)?;
        }
        let result = self.call(py, |_, lines| {
            lines.push(LogLine::warning("sample not implemented yet"));
            Ok(CasaValue::floats(Vec::new()))
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_sample> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Select component indices.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn select(&self, py: Python<'_>, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let (indices, pending) = selection::indices(which)?;
        let result = self.call(py, move |state, lines| {
            state.set_selection(&indices, true, lines)
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_select> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// The selected component indices in ascending order.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn selected(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| Ok(state.selected()))
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(
        signature = (which, value = None, unit = None, polarization = None, error = None, log = None),
        text_signature = "($self, which, value=[], unit='Jy', polarization='Stokes', error=[], log=True)"
    )]
    #[allow(clippy::too_many_arguments)]
    fn setflux(
        &self,
        which: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        unit: Option<&Bound<'_, PyAny>>,
        polarization: Option<&Bound<'_, PyAny>>,
        error: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setflux")
    }

    /// Change a component's reference frequency and its record unit.
    #[pyo3(signature = (which, value, unit = None, log = None), text_signature = "($self, which, value, unit='GHz', log=True)")]
    fn setfreq(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] unit: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] log: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let which = match which.extract::<i64>() {
            Ok(index) => i64::from(index as i32),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => -1,
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        // CASA's double typemap passes -1 through to the operation when
        // conversion fails, then reports a SystemError if it succeeded.
        let converted = value.extract::<f64>();
        let invalid_value = converted.is_err();
        let value = converted.unwrap_or(-1.0);
        let unit: String = match unit {
            None => "GHz".to_string(),
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
            Some(_) => return Err(PyTypeError::new_err("argument _unit must be a string")),
        };
        if log.is_some_and(|value| !value.is_instance_of::<PyBool>()) {
            return Err(PyTypeError::new_err(
                "in method 'componentlist_setfreq', argument 5 of type 'bool'",
            ));
        }
        let unit = unit.split('\0').next().unwrap_or_default().to_string();
        let result = self.call(py, move |state, _| {
            state.setfreq(which, value, &unit).map(CasaValue::Bool)
        })?;
        if invalid_value {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_setfreq> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Relabel one component's frequency frame without converting its value.
    #[pyo3(signature = (which, frame = None, log = None), text_signature = "($self, which, frame='LSRK', log=True)")]
    fn setfreqframe(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] frame: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] log: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (which, pending) = match which.extract::<i64>() {
            Ok(index) => (i64::from(index as i32), false),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => {
                (-1, true)
            }
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        let frame: String = match frame {
            None => "LSRK".to_string(),
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
            Some(_) => return Err(PyTypeError::new_err("argument _frame must be a string")),
        };
        if log.is_some_and(|value| !value.is_instance_of::<PyBool>()) {
            return Err(PyTypeError::new_err(
                "in method 'componentlist_setfreqframe', argument 4 of type 'bool'",
            ));
        }
        let frame = frame.split('\0').next().unwrap_or_default().to_string();
        let result = self.call(py, move |state, lines| {
            state
                .setfreqframe(which, &frame, lines)
                .map(CasaValue::Bool)
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_setfreqframe> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Replace the user label of one component.
    #[pyo3(signature = (which, value, log = None), text_signature = "($self, which, value, log=True)")]
    fn setlabel(
        &self,
        py: Python<'_>,
        which: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = supplied)] log: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let which = match which.extract::<i64>() {
            Ok(index) => i64::from(index as i32),
            Err(error) if which.is_none() || error.is_instance_of::<PyOverflowError>(py) => -1,
            Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
        };
        let value: String = if value.is_instance_of::<PyString>() {
            value.extract()?
        } else if value.is_instance_of::<PyBytes>() {
            let bytes = value.cast::<PyBytes>()?.as_bytes();
            let end = bytes
                .iter()
                .position(|&byte| byte == 0)
                .unwrap_or(bytes.len());
            PyBytes::new(py, &bytes[..end])
                .call_method1("decode", ("utf-8",))?
                .extract()?
        } else {
            return Err(PyTypeError::new_err("argument _value must be a string"));
        };
        if log.as_ref().is_some_and(|v| !v.is_instance_of::<PyBool>()) {
            return Err(PyTypeError::new_err(
                "in method 'componentlist_setlabel', argument 4 of type 'bool'",
            ));
        }
        // The CASA wrapper passes a NUL-terminated C string to casacore.
        let value = value.split('\0').next().unwrap_or_default().to_string();
        self.call(py, move |state, lines| {
            match state.setlabel(which, &value) {
                Ok(result) => Ok(CasaValue::Bool(result)),
                Err(error) => {
                    lines.push(LogLine::error(format!("Exception Reported: {error}")));
                    Err(error)
                }
            }
        })
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None, ra = None, dec = None, log = None), text_signature = "($self, which=1, ra=[], dec=[], log=True)")]
    fn setrefdir(
        &self,
        which: Option<&Bound<'_, PyAny>>,
        ra: Option<&Bound<'_, PyAny>>,
        dec: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setrefdir")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, frame, log = None), text_signature = "($self, which, frame, log=True)")]
    fn setrefdirframe(
        &self,
        which: &Bound<'_, PyAny>,
        frame: &Bound<'_, PyAny>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setrefdirframe")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(
        signature = (which, r#type = None, majoraxis = None, minoraxis = None, positionangle = None, majoraxiserror = None, minoraxiserror = None, positionangleerror = None, optionalparms = None, log = None),
        text_signature = "($self, which, type='Point', majoraxis=[], minoraxis=[], positionangle=[], majoraxiserror=[], minoraxiserror=[], positionangleerror=[], optionalparms=[0.0], log=True)"
    )]
    #[allow(clippy::too_many_arguments)]
    fn setshape(
        &self,
        which: &Bound<'_, PyAny>,
        r#type: Option<&Bound<'_, PyAny>>,
        majoraxis: Option<&Bound<'_, PyAny>>,
        minoraxis: Option<&Bound<'_, PyAny>>,
        positionangle: Option<&Bound<'_, PyAny>>,
        majoraxiserror: Option<&Bound<'_, PyAny>>,
        minoraxiserror: Option<&Bound<'_, PyAny>>,
        positionangleerror: Option<&Bound<'_, PyAny>>,
        optionalparms: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setshape")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(
        signature = (which, r#type = None, index = None, tabularfreqs = None, tabularflux = None, tabularframe = None),
        text_signature = "($self, which, type='spectral index', index=[], tabularfreqs=[100000000000.0], tabularflux=[1.0], tabularframe='LSRK')"
    )]
    #[allow(clippy::too_many_arguments)]
    fn setspectrum(
        &self,
        which: &Bound<'_, PyAny>,
        r#type: Option<&Bound<'_, PyAny>>,
        index: Option<&Bound<'_, PyAny>>,
        tabularfreqs: Option<&Bound<'_, PyAny>>,
        tabularflux: Option<&Bound<'_, PyAny>>,
        tabularframe: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setspectrum")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(
        signature = (which, r#type = None, index = None, tabularfreqs = None, tabulari = None, tabularq = None, tabularu = None, tabularv = None, reffreq = None, frame = None),
        text_signature = "($self, which, type='spectral index', index=[0.0], tabularfreqs=[100000000000.0], tabulari=[1.0], tabularq=[0.0], tabularu=[0.0], tabularv=[0.0], reffreq=[], frame='LSRK')"
    )]
    #[allow(clippy::too_many_arguments)]
    fn setstokesspectrum(
        &self,
        which: &Bound<'_, PyAny>,
        r#type: Option<&Bound<'_, PyAny>>,
        index: Option<&Bound<'_, PyAny>>,
        tabularfreqs: Option<&Bound<'_, PyAny>>,
        tabulari: Option<&Bound<'_, PyAny>>,
        tabularq: Option<&Bound<'_, PyAny>>,
        tabularu: Option<&Bound<'_, PyAny>>,
        tabularv: Option<&Bound<'_, PyAny>>,
        reffreq: Option<&Bound<'_, PyAny>>,
        frame: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setstokesspectrum")
    }

    /// Component `which`'s shape name (`Point`, `Gaussian`, `Disk`,
    /// `Limbdarkeneddisk`).
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn shapetype(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| {
            let component = state.unchecked(which)?;
            Ok(CasaValue::Text(
                component.shape().shape_type().casa_name().to_string(),
            ))
        })
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (howmany = None, log = None), text_signature = "($self, howmany=1, log=True)")]
    fn simulate(
        &self,
        howmany: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("simulate")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (criteria = None, log = None), text_signature = "($self, criteria='Flux', log=True)")]
    fn sort(
        &self,
        criteria: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("sort")
    }

    /// Component `which`'s spectral-model name (`Constant`, `Spectral
    /// Index`, `Tabular Spectrum`, `Power Logarithmic Polynomial`).
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn spectrumtype(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| {
            let component = state.unchecked(which)?;
            Ok(CasaValue::Text(
                component.spectrum().spectral_type().casa_name().to_string(),
            ))
        })
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None), text_signature = "($self, which=-1)")]
    fn summarize(&self, which: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("summarize")
    }

    /// The list's record: `nelements` and each component's as
    /// `component<i>`; `{}` with CASA's warning for an empty list.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn torecord(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| state.torecord(lines))
    }
}
