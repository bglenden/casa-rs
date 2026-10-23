// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.tools.componentlist`: CASA's `componentlist` tool (`cl`) over
//! [`casa_components::ComponentList`].
//!
//! The class declares every method of CASA's installed binding with CASA's
//! names, parameter order and effective defaults ([`surface`]). The methods
//! CASA's simulation tasks call are implemented (`open`, `addcomponent`,
//! `rename`, `close`, `done`, `length`, `torecord`, `getspectrum`,
//! `getrefdir`), with the accessors `getcomponent`, `getfluxvalue`,
//! `getfluxunit`, `getlabel`, `shapetype` and `spectrumtype`; the rest raise
//! `NotImplementedError`.
//!
//! CASA's outcomes are kept ([`ops`] for the table, `add` for
//! `addcomponent`): what CASA raises is a `RuntimeError` with CASA's
//! message, and what it logs and carries on from (a shape `setshape` cannot
//! make, `torecord` of an empty list, `close`) is logged on the Python
//! logger `casars.tools.componentlist`, CASA's SEVERE at ERROR and its WARN
//! at WARNING. Records are casacore's (`casa_components` `to_record`), as
//! `dict`s with NumPy arrays.
//!
//! Divergences, recorded in `docs/CASA (C++) bugs.md`: `addcomponent` keeps
//! the flux's polarisation and double precision (`add`), and records give a
//! shape's axes in arcmin and degrees, as CASA gives a component read from a
//! table, where CASA gives one it built in the units it was given.

mod add;
mod ops;
pub(crate) mod surface;

use std::ffi::CString;
use std::sync::{Mutex, MutexGuard, PoisonError};

use pyo3::exceptions::PyRuntimeWarning;
use pyo3::prelude::*;

use crate::convert::value::CasaValue;
use crate::convert::{ConvertResult, DoubleList, Variant};
use crate::tools::{LogLine, emit, not_yet_implemented};
use add::AddComponent;
use ops::State;

/// The Python logger CASA's `cl` messages go to.
const LOGGER: &str = "casars.tools.componentlist";

/// CASA's `componentlist` tool.
#[pyclass(name = "componentlist", module = "casars.tools", frozen)]
pub(crate) struct PyComponentList {
    state: Mutex<State>,
}

impl PyComponentList {
    /// Runs `work` on the state with the GIL released, logs what it reports
    /// and returns its value as Python.
    fn call(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut State, &mut Vec<LogLine>) -> ConvertResult<CasaValue> + Send,
    ) -> PyResult<Py<PyAny>> {
        let mut lines = Vec::new();
        let result = py.detach(|| work(&mut self.lock(), &mut lines));
        emit(py, LOGGER, &lines)?;
        result?.into_python(py)
    }

    /// The state lock, taken without the GIL (inside `Python::detach`).
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for PyComponentList {
    /// CASA's destructor writes a list attached for update; a failure is a
    /// `RuntimeWarning`, as nothing can raise here.
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        if let Err(message) = state.release() {
            Python::attach(|py| {
                let category = py.get_type::<PyRuntimeWarning>();
                let message = CString::new(message).unwrap_or_default();
                if let Err(error) = PyErr::warn(py, category.as_any(), &message, 1) {
                    error.write_unraisable(py, None);
                }
            });
        }
    }
}

/// A method a later phase implements.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented("componentlist", method)
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

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, unit = None), text_signature = "($self, which, unit='Jy')")]
    fn convertfluxunit(
        &self,
        which: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertfluxunit")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, unit = None), text_signature = "($self, which, unit='GHz')")]
    fn convertfrequnit(
        &self,
        which: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertfrequnit")
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

    /// Not yet implemented in casa-rs.
    #[pyo3(
        signature = (which, majoraxis = None, minoraxis = None, positionangle = None),
        text_signature = "($self, which, majoraxis='arcmin', minoraxis='arcmin', positionangle='deg')"
    )]
    fn convertshape(
        &self,
        which: &Bound<'_, PyAny>,
        majoraxis: Option<&Bound<'_, PyAny>>,
        minoraxis: Option<&Bound<'_, PyAny>>,
        positionangle: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertshape")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn deselect(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("deselect")
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

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfluxerror(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getfluxerror")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfluxpol(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getfluxpol")
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

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfreq(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getfreq")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfreqframe(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getfreqframe")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfrequnit(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getfrequnit")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getfreqvalue(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getfreqvalue")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (keyword = None), text_signature = "($self, keyword='metadata')")]
    fn getkeyword(&self, keyword: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("getkeyword")
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

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, unit = None, precision = None), text_signature = "($self, which, unit='deg', precision=6)")]
    fn getrefdirdec(
        &self,
        which: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getrefdirdec")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getrefdirframe(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getrefdirframe")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, unit = None, precision = None), text_signature = "($self, which, unit='deg', precision=6)")]
    fn getrefdirra(
        &self,
        which: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getrefdirra")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getshape(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getshape")
    }

    /// Component `which`'s spectral model, as its `toRecord` writes it.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn getspectrum(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, _| {
            ops::record(state.unchecked(which)?.spectrum().to_record())
        })
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (keyword = None), text_signature = "($self, keyword='metadata')")]
    fn haskeyword(&self, keyword: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("haskeyword")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn indices(&self) -> PyResult<Py<PyAny>> {
        stub("indices")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (tool = None), text_signature = "($self, tool=[])")]
    fn iscomponentlist(&self, tool: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("iscomponentlist")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None), text_signature = "($self, which=[-1])")]
    fn isphysical(&self, which: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("isphysical")
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

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn purge(&self) -> PyResult<Py<PyAny>> {
        stub("purge")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (keyword = None, value = None), text_signature = "($self, keyword='metadata', value=[])")]
    fn putkeyword(
        &self,
        keyword: Option<&Bound<'_, PyAny>>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putkeyword")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (log = None), text_signature = "($self, log=True)")]
    fn recover(&self, log: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("recover")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None, log = None), text_signature = "($self, which=[-1], log=True)")]
    fn remove(
        &self,
        which: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("remove")
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

    /// Not yet implemented in casa-rs.
    #[pyo3(
        signature = (direction = None, pixellatsize = None, pixellongsize = None, frequency = None),
        text_signature = "($self, direction=[], pixellatsize=[], pixellongsize=[], frequency=[])"
    )]
    fn sample(
        &self,
        direction: Option<&Bound<'_, PyAny>>,
        pixellatsize: Option<&Bound<'_, PyAny>>,
        pixellongsize: Option<&Bound<'_, PyAny>>,
        frequency: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("sample")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which), text_signature = "($self, which)")]
    fn select(&self, which: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("select")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn selected(&self) -> PyResult<Py<PyAny>> {
        stub("selected")
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

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, value, unit = None, log = None), text_signature = "($self, which, value, unit='GHz', log=True)")]
    fn setfreq(
        &self,
        which: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setfreq")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, frame = None, log = None), text_signature = "($self, which, frame='LSRK', log=True)")]
    fn setfreqframe(
        &self,
        which: &Bound<'_, PyAny>,
        frame: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setfreqframe")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which, value, log = None), text_signature = "($self, which, value, log=True)")]
    fn setlabel(
        &self,
        which: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        log: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setlabel")
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
