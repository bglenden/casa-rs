// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.tools.quanta`: CASA's `quanta` tool (`qa`) over casa-types'
//! quanta.
//!
//! The class declares every method of CASA's installed binding with CASA's
//! names, parameter order and effective defaults ([`surface`]); the
//! methods casa-rs implements delegate to `ops`, and the rest raise
//! `NotImplementedError`. Quantities are casacore's quantity records,
//! `{'unit': ..., 'value': ...}`. The tool has no state; each call releases
//! the GIL while it computes. A failure CASA raises is a `RuntimeError`
//! with CASA's message (CASA also logs it at SEVERE; casa-rs only raises).

mod ops;
pub(crate) mod surface;

use pyo3::prelude::*;

use crate::convert::value::CasaValue;
use crate::convert::{ConvertResult, StringList, Variant};
use crate::tools::{LogLine, emit, not_yet_implemented};

/// The Python logger the tool's warnings go to.
const LOGGER: &str = "casars.tools.quanta";

/// CASA's `quanta` tool.
#[pyclass(name = "quanta", module = "casars.tools", frozen)]
pub(crate) struct PyQuanta;

/// Runs `work` with the GIL released and returns its value as Python.
fn call(
    py: Python<'_>,
    work: impl FnOnce() -> ConvertResult<CasaValue> + Send,
) -> PyResult<Py<PyAny>> {
    py.detach(work)?.into_python(py)
}

/// A method a later phase implements.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented("quanta", method)
}

#[pymethods]
impl PyQuanta {
    #[new]
    fn new() -> Self {
        Self
    }

    /// A quantity record from a string, a record, or numbers in `unitname`.
    #[pyo3(
        signature = (v = Variant::Empty, unitname = "", keepshape = false),
        text_signature = "($self, v=[], unitname='', keepshape=False)"
    )]
    fn quantity(
        &self,
        py: Python<'_>,
        v: Variant,
        unitname: &str,
        keepshape: bool,
    ) -> PyResult<Py<PyAny>> {
        call(py, || ops::quantity_of(&v, unitname, keepshape))
    }

    /// The same as `quantity`.
    #[pyo3(
        signature = (v = Variant::Empty, unitname = "", keepshape = false),
        text_signature = "($self, v=[], unitname='', keepshape=False)"
    )]
    fn unit(
        &self,
        py: Python<'_>,
        v: Variant,
        unitname: &str,
        keepshape: bool,
    ) -> PyResult<Py<PyAny>> {
        call(py, || ops::quantity_of(&v, unitname, keepshape))
    }

    /// The value (or values) of a quantity, as an array.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn getvalue(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::getvalue(&v))
    }

    /// The unit of a quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn getunit(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::getunit(&v))
    }

    /// A quantity in the unit of `outunit`, or in SI base units.
    #[pyo3(
        signature = (v = Variant::Empty, outunit = Variant::Empty),
        text_signature = "($self, v=[], outunit=[])"
    )]
    fn convert(&self, py: Python<'_>, v: Variant, outunit: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::convert(&v, &outunit))
    }

    /// A quantity in SI base units.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn canonical(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::canonical(&v))
    }

    /// A quantity in SI base units (as `canonical`).
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn canon(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::canonical(&v))
    }

    /// The sum of two quantities, in the first's unit.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn add(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::arithmetic(&v, &a, '+'))
    }

    /// The difference of two quantities, in the first's unit.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn sub(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::arithmetic(&v, &a, '-'))
    }

    /// The product of two quantities.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn mul(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::arithmetic(&v, &a, '*'))
    }

    /// The quotient of two quantities.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn div(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::arithmetic(&v, &a, '/'))
    }

    /// A quantity negated.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn neg(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::neg(&v))
    }

    /// The absolute value of a quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn abs(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::abs(&v))
    }

    /// Whether two quantities' units have the same dimensions.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn compare(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::compare(&v, &a))
    }

    /// Whether a string reads as a quantity.
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn check(&self, py: Python<'_>, v: &str) -> PyResult<Py<PyAny>> {
        call(py, || Ok(ops::check(v)))
    }

    /// Whether a value is a quantity string or record.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn isquantity(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || Ok(ops::isquantity(&v)))
    }

    /// A time or angle as a time string.
    #[pyo3(
        signature = (v = Variant::Empty, prec = 0, form = StringList::of(&[]), showform = false),
        text_signature = "($self, v=[], prec=0, form=[], showform=False)"
    )]
    #[allow(unused_variables)]
    fn time(
        &self,
        py: Python<'_>,
        v: Variant,
        prec: i64,
        form: StringList,
        showform: bool,
    ) -> PyResult<Py<PyAny>> {
        call(py, || ops::time(&v, prec, &form.0))
    }

    /// An angle or time as an angle string.
    #[pyo3(
        signature = (v = Variant::Empty, prec = 0, form = StringList::of(&[]), showform = false),
        text_signature = "($self, v=[], prec=0, form=[], showform=False)"
    )]
    #[allow(unused_variables)]
    fn angle(
        &self,
        py: Python<'_>,
        v: Variant,
        prec: i64,
        form: StringList,
        showform: bool,
    ) -> PyResult<Py<PyAny>> {
        call(py, || ops::angle(&v, prec, &form.0))
    }

    /// An angle as a time (days), or a time as it is.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn totime(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::totime(&v))
    }

    /// A time as an angle (radians), or an angle as it is.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn toangle(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::toangle(&v))
    }

    /// A time's calendar fields.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn splitdate(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::splitdate(&v))
    }

    /// End the tool: `True`.
    #[allow(unused_variables)]
    #[pyo3(signature = (kill = false), text_signature = "($self, kill=False)")]
    fn done(&self, kill: bool) -> bool {
        true
    }

    /// The tool type, `'quanta'`.
    #[pyo3(name = "type")]
    fn tool_type(&self) -> &'static str {
        "quanta"
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn acos(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("acos")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn asin(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("asin")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn atan(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("atan")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=[])")]
    fn atan2(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("atan2")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn ceil(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("ceil")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (cm = None), text_signature = "($self, cm=[])")]
    fn checkfreq(&self, cm: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("checkfreq")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v='pi')")]
    fn constants(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("constants")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, outunit = None), text_signature = "($self, v=[], outunit='km/s')")]
    fn convertdop(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        outunit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertdop")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, outunit = None), text_signature = "($self, v=[], outunit='Hz')")]
    fn convertfreq(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        outunit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertfreq")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn cos(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("cos")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (name, v = None), text_signature = "($self, name, v=[])")]
    fn define(&self, name: &Bound<'_, PyAny>, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("define")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=[])")]
    fn eq(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("eq")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn exp(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("exp")
    }

    /// Not yet implemented in casa-rs.
    fn fits(&self) -> PyResult<Py<PyAny>> {
        stub("fits")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn floor(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("floor")
    }

    /// An angle or time as text: `hms`, `dms`, `deg`, `rad` or `+deg`. Any
    /// other format gives an empty string and a warning, as in CASA.
    #[pyo3(
        signature = (v = Variant::Empty, format = "dms".to_string(), prec = 2),
        text_signature = "($self, v=[], format='dms', prec=2)"
    )]
    fn formxxx(&self, py: Python<'_>, v: Variant, format: String, prec: i64) -> PyResult<Py<PyAny>> {
        let written = py.detach(|| ops::formxxx(&v, &format, prec))?;
        if written.is_none() {
            emit(py, LOGGER, &[LogLine::warning(format!("Don't understand {format}"))])?;
        }
        CasaValue::Text(written.unwrap_or_default()).into_python(py)
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=[])")]
    fn ge(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("ge")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (t = None), text_signature = "($self, t='')")]
    fn getformat(&self, t: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("getformat")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=[])")]
    fn gt(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("gt")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn isangle(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("isangle")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=[])")]
    fn le(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("le")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn log(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("log")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn log10(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("log10")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=[])")]
    fn lt(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("lt")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v='all')")]
    fn map(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("map")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v='all')")]
    fn maprec(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("maprec")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=[])")]
    fn ne(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("ne")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=-0.5)")]
    fn norm(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("norm")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None, a = None), text_signature = "($self, v=[], a=1)")]
    fn pow(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        a: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("pow")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (t = None, v = None), text_signature = "($self, t='', v='F')")]
    fn setformat(
        &self,
        t: Option<&Bound<'_, PyAny>>,
        v: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setformat")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn sin(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("sin")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn sqrt(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("sqrt")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (v = None), text_signature = "($self, v=[])")]
    fn tan(&self, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("tan")
    }

    /// A quantity as text: the value with `prec` decimals, then the unit.
    #[pyo3(signature = (v = Variant::Empty, prec = 9), text_signature = "($self, v=[], prec=9)")]
    fn tos(&self, py: Python<'_>, v: Variant, prec: i64) -> PyResult<Py<PyAny>> {
        call(py, || ops::tos(&v, prec))
    }
}
