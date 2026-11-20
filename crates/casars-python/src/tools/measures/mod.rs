// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.tools.measures`: CASA's `measures` tool (`me`) over casa-types'
//! measures.
//!
//! The class declares every method of CASA's installed binding with CASA's
//! names, parameter order and effective defaults ([`surface`]); each method
//! delegates to a free function: the measures a script defines in `define`,
//! the frame and conversions in `frame`, and the record queries and
//! formatting in `inspect`.
//!
//! Measures are casacore's measure records (`dict`s from `MeasureHolder`),
//! and the tool keeps the frame `doframe` sets. CASA's outcomes are kept: a
//! failure CASA logs is logged on the Python logger `casars.tools.measures`
//! (SEVERE at ERROR, WARN at WARNING) and the method returns `{}` (or `''`,
//! or `False`); a non-`dict` where CASA takes a record is a `TypeError`.
//! Each call releases the GIL while it computes; the measures data (TAI−UTC,
//! Earth orientation, observatories, sources) load on first use.
//!
//! Divergences, each recorded where it is implemented:
//! - a conversion that needs a frame element `doframe` has not set fails
//!   with CASA's "Cannot convert due to missing frame information", where
//!   CASA's tool starts with a frame at MJD 0 at the geocentre and converts
//!   with it (`measure(dir, 'AZEL')` with no `doframe`);
//! - Undefined frequencies retain their reference, and conversions between
//!   Undefined and a physical frequency scale fail as in CASA.

mod arguments;
mod comet;
mod define;
mod frame;
mod geometry;
mod inspect;
mod offset;
mod record;
pub(super) use record::observation_epoch;
mod rise_set;
mod shift;
mod showframe;
mod spectral;
pub(crate) mod surface;
mod xvalue;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use casa_measures_data::{CometTable, PlanetaryTable};
use casa_types::measures::MeasuresProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::holder::Measure;
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant};
use crate::tools::{LogLine, emit};

/// The Python logger CASA's `me` messages go to.
const LOGGER: &str = "casars.tools.measures";

/// The frame `doframe` sets.
#[derive(Debug, Clone, Default)]
struct Frame {
    epoch: Option<FrameValue>,
    position: Option<FrameValue>,
    direction: Option<FrameValue>,
    radial_velocity: Option<FrameValue>,
    comet: Option<Arc<CometTable>>,
    // Formatted display of these frame elements, cleared when an element changes.
    rendered: Option<String>,
}

/// A scalar frame item, keeping its displayed and absolute coordinates.
#[derive(Debug, Clone)]
struct FrameValue {
    measure: Measure,
    absolute: Measure,
}

impl From<Measure> for FrameValue {
    fn from(measure: Measure) -> Self {
        Self {
            measure,
            absolute: measure,
        }
    }
}

/// The tool's state: the frame and the measures data.
#[derive(Debug, Default)]
pub(crate) struct State {
    frame: Frame,
    measures: Option<Arc<dyn MeasuresProvider>>,
    planets: Option<PlanetaryTable>,
    // The comet queries retain their table when done clears the coordinate frame.
    comet: Option<Arc<CometTable>>,
}

impl State {
    /// The measures data, loaded on first use.
    fn measures(&mut self) -> Result<Arc<dyn MeasuresProvider>, String> {
        if let Some(measures) = &self.measures {
            return Ok(Arc::clone(measures));
        }
        let measures =
            casa_ms::open_measures_runtime().map_err(|error| format!("measures data: {error}"))?;
        self.measures = Some(Arc::clone(&measures));
        Ok(measures)
    }
}

/// CASA's `measures` tool.
#[pyclass(name = "measures", module = "casars.tools", frozen)]
pub(crate) struct PyMeasures {
    state: Mutex<State>,
}

impl PyMeasures {
    /// Runs `work` on the state with the GIL released, logs what it reports
    /// and returns its value as Python.
    fn call(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut State, &mut Vec<LogLine>) -> PyResult<CasaValue> + Send,
    ) -> PyResult<Py<PyAny>> {
        let mut lines = Vec::new();
        let result = py.detach(|| {
            casa_types::quanta::registry::global_registry().with_locked_units(|| {
                let mut state = self.lock();
                work(&mut state, &mut lines)
            })
        });
        emit(py, LOGGER, &lines)?;
        result?.into_python(py)
    }

    /// The state lock, taken without the GIL (inside `Python::detach`).
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The measures data failed to load: CASA's tool always has them.
fn no_measures(error: String) -> PyErr {
    PyRuntimeError::new_err(error)
}

#[pymethods]
impl PyMeasures {
    #[new]
    fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    /// An epoch measure.
    #[pyo3(
        signature = (rf = "UTC", v0 = Variant::Empty, off = RecordArg::empty()),
        text_signature = "($self, rf='UTC', v0=[], off={})"
    )]
    fn epoch(&self, py: Python<'_>, rf: &str, v0: Variant, off: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            define::epoch(state, rf, &v0, &off, lines)
        })
    }

    /// A direction measure.
    #[pyo3(
        signature = (rf = "J2000", v0 = Variant::Empty, v1 = Variant::Empty, off = RecordArg::empty()),
        text_signature = "($self, rf='J2000', v0=[], v1=[], off={})"
    )]
    fn direction(
        &self,
        py: Python<'_>,
        rf: &str,
        v0: Variant,
        v1: Variant,
        off: RecordArg,
    ) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            define::direction(state, rf, &v0, &v1, &off, lines)
        })
    }

    /// A position measure.
    #[pyo3(
        signature = (rf = "WGS84", v0 = Variant::Empty, v1 = Variant::Empty, v2 = Variant::Empty, off = RecordArg::empty()),
        text_signature = "($self, rf='WGS84', v0=[], v1=[], v2=[], off={})"
    )]
    fn position(
        &self,
        py: Python<'_>,
        rf: &str,
        v0: Variant,
        v1: Variant,
        v2: Variant,
        off: RecordArg,
    ) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            define::position(state, rf, [&v0, &v1, &v2], &off, lines)
        })
    }

    /// A frequency measure.
    #[pyo3(
        signature = (rf = "LSRK", v0 = Variant::Empty, off = RecordArg::empty()),
        text_signature = "($self, rf='LSRK', v0=[], off={})"
    )]
    fn frequency(
        &self,
        py: Python<'_>,
        rf: &str,
        v0: Variant,
        off: RecordArg,
    ) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            define::frequency(state, rf, &v0, &off, lines)
        })
    }

    /// The position of an observatory.
    #[pyo3(signature = (name = "ALMA"), text_signature = "($self, name='ALMA')")]
    fn observatory(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            let measures = state.measures().map_err(no_measures)?;
            Ok(define::observatory(name, &*measures, lines))
        })
    }

    /// The direction of a source: a catalogue name, a frame (its pole), a
    /// direction string or a direction record.
    #[pyo3(signature = (name = Variant::Empty), text_signature = "($self, name=[])")]
    fn source(&self, py: Python<'_>, name: Variant) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            let measures = state.measures().map_err(no_measures)?;
            define::source(&name, &measures, lines)
        })
    }

    /// Set an epoch, position or direction in the frame.
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn doframe(&self, py: Python<'_>, v: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| Ok(frame::doframe(state, &v, lines)))
    }

    /// Set the frame's epoch to now.
    fn framenow(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| Ok(frame::framenow(state)))
    }

    /// Convert a measure to the reference code `rf`, with the frame.
    #[pyo3(signature = (v, rf, off = RecordArg::empty()), text_signature = "($self, v, rf, off={})")]
    fn measure(
        &self,
        py: Python<'_>,
        v: RecordArg,
        rf: &str,
        off: RecordArg,
    ) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            frame::measure(state, &v, rf, &off, lines)
        })
    }

    /// The angular separation of two directions.
    #[pyo3(signature = (m1, m2), text_signature = "($self, m1, m2)")]
    fn separation(&self, py: Python<'_>, m1: RecordArg, m2: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            frame::between(state, &m1, &m2, frame::Between::Separation, lines)
        })
    }

    /// The position angle of a direction from another.
    #[pyo3(signature = (m1, m2), text_signature = "($self, m1, m2)")]
    fn posangle(&self, py: Python<'_>, m1: RecordArg, m2: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            frame::between(state, &m1, &m2, frame::Between::PositionAngle, lines)
        })
    }

    /// A measure's reference code.
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn getref(&self, py: Python<'_>, v: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |_, lines| Ok(inspect::getref(&v, lines)))
    }

    /// A measure's kind (`'Epoch'`, `'Direction'`, ...).
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn gettype(&self, py: Python<'_>, v: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |_, lines| Ok(inspect::gettype(&v, lines)))
    }

    /// A measure record without its `type` and `refer`.
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn getvalue(&self, py: Python<'_>, v: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |_, lines| Ok(inspect::getvalue(&v, lines)))
    }

    /// Whether `v` is a measure record.
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn ismeasure(&self, py: Python<'_>, v: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |_, _| Ok(inspect::ismeasure(&v)))
    }

    /// A measure as text.
    #[pyo3(signature = (v, refcode = true), text_signature = "($self, v, refcode=True)")]
    fn show(&self, py: Python<'_>, v: RecordArg, refcode: bool) -> PyResult<Py<PyAny>> {
        self.call(py, |_, lines| Ok(inspect::show(&v, refcode, lines)))
    }

    /// A direction as text, in degrees.
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn dirshow(&self, py: Python<'_>, v: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |_, lines| Ok(inspect::dirshow(&v, lines)))
    }

    /// The reference codes of a measure's kind.
    #[pyo3(signature = (ms), text_signature = "($self, ms)")]
    fn listcodes(&self, py: Python<'_>, ms: RecordArg) -> PyResult<Py<PyAny>> {
        self.call(py, |_, lines| Ok(inspect::listcodes(&ms, lines)))
    }

    /// Clear the frame.
    fn done(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| Ok(frame::done(state)))
    }

    /// The tool type, `'measures'`.
    #[pyo3(name = "type")]
    fn tool_type(&self) -> &'static str {
        "measures"
    }

    /// Return the Cartesian extra values of a supported scalar measure.
    #[pyo3(signature = (a), text_signature = "($self, a)")]
    fn addxvalue(&self, py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let record = xvalue::read_record(a)?;
        self.call(py, move |_, lines| xvalue::addxvalue(&record, lines))
    }

    /// The rest lines of the measures data, their names joined by spaces
    /// in the table's order, as CASA gives them. CASA deprecates the method
    /// and says so; so does casa-rs.
    fn linelist(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            lines.push(LogLine::warning(
                "DEPRECATED. The linelist method has been deprecated and will be removed in the \
                 near future. Please use the spectral line (sl) tool and/or related tasks instead",
            ));
            let measures = state.measures().map_err(no_measures)?;
            let names = measures.spectral_line_names().map_err(no_measures)?;
            Ok(CasaValue::Text(names.join(" ")))
        })
    }

    /// The observatories of the measures data, by name, in the table's
    /// order: a list of `str`, as CASA gives them.
    fn obslist(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| {
            let measures = state.measures().map_err(no_measures)?;
            let names = measures.observatory_names().map_err(no_measures)?;
            Ok(CasaValue::List(
                names.into_iter().map(CasaValue::Text).collect(),
            ))
        })
    }

    /// The sources of the measures data, their names joined by spaces in
    /// the table's order, as CASA gives them.
    fn sourcelist(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| {
            let measures = state.measures().map_err(no_measures)?;
            let names = measures.source_names().map_err(no_measures)?;
            Ok(CasaValue::Text(names.join(" ")))
        })
    }

    /// The rest frequency of a spectral line the measures data names.
    #[pyo3(signature = (name = "HI"), text_signature = "($self, name='HI')")]
    fn spectralline(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            let measures = state.measures().map_err(no_measures)?;
            Ok(define::spectralline(name, &*measures, lines))
        })
    }
}
