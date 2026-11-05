// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.tools.quanta`: CASA's `quanta` tool (`qa`) over casa-types'
//! quanta.
//!
//! The class declares every method of CASA's installed binding with CASA's
//! names, parameter order and effective defaults ([`surface`]); the
//! methods casa-rs implements delegate to `ops`, and the rest raise
//! `NotImplementedError` naming #1053. Quantities are casacore's quantity records,
//! `{'unit': ..., 'value': ...}`. The tool has no state; each call releases
//! the GIL while it computes. A failure CASA raises is a `RuntimeError`
//! with CASA's message (CASA also logs it at SEVERE; casa-rs only raises).

mod ops;
pub(crate) mod surface;

use pyo3::exceptions::{PyOverflowError, PySystemError, PyTypeError};
use pyo3::prelude::*;

use crate::convert::value::CasaValue;
use crate::convert::{ConvertResult, StringList, Variant};
use crate::tools::{LogLine, emit, not_yet_implemented_in};

/// The Python logger the tool's warnings go to.
const LOGGER: &str = "casars.tools.quanta";

/// Keep an explicitly supplied `None` distinct from an omitted argument.
fn supplied<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(Some(value.clone()))
}

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

/// A method casa-rs does not implement yet, refused naming #1053, which
/// classifies them.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("quanta", method, 1053)
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

    /// CASA's scalar acos of a quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn acos(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::trigonometry(&v, "acos"))
    }

    /// CASA's scalar asin of a quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn asin(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::trigonometry(&v, "asin"))
    }

    /// CASA's scalar atan of a quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn atan(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::trigonometry(&v, "atan"))
    }

    /// CASA's scalar two-argument arctangent, in radians.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn atan2(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::atan2(&v, &a))
    }

    /// Round a scalar quantity upward in its own unit.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn ceil(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::round_integer(&v, true))
    }

    /// Whether a scalar quantity can represent a frequency.
    #[pyo3(signature = (cm = Variant::Empty), text_signature = "($self, cm=[])")]
    fn checkfreq(&self, py: Python<'_>, cm: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::checkfreq(&cm))
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

    /// CASA's scalar cos of a quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn cos(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::trigonometry(&v, "cos"))
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (name, v = None), text_signature = "($self, name, v=[])")]
    fn define(&self, name: &Bound<'_, PyAny>, v: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("define")
    }

    /// CASA's scalar quantity comparison.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn eq(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::relational(&v, &a, "eq"))
    }

    /// CASA's scalar exp of a dimensionless quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn exp(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::logarithm(&v, "exp"))
    }

    /// Not yet implemented in casa-rs.
    fn fits(&self) -> PyResult<Py<PyAny>> {
        stub("fits")
    }

    /// Round a scalar quantity downward in its own unit.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn floor(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::round_integer(&v, false))
    }

    /// An angle or time as text: `hms`, `dms`, `deg`, `rad` or `+deg`. Any
    /// other format gives an empty string and a warning, as in CASA.
    #[pyo3(
        signature = (v = Variant::Empty, format = "dms".to_string(), prec = 2),
        text_signature = "($self, v=[], format='dms', prec=2)"
    )]
    fn formxxx(
        &self,
        py: Python<'_>,
        v: Variant,
        format: String,
        prec: i64,
    ) -> PyResult<Py<PyAny>> {
        let written = py.detach(|| ops::formxxx(&v, &format, prec))?;
        if written.is_none() {
            emit(
                py,
                LOGGER,
                &[LogLine::warning(format!("Don't understand {format}"))],
            )?;
        }
        CasaValue::Text(written.unwrap_or_default()).into_python(py)
    }

    /// CASA's scalar quantity comparison.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn ge(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::relational(&v, &a, "ge"))
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables)]
    #[pyo3(signature = (t = None), text_signature = "($self, t='')")]
    fn getformat(&self, t: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("getformat")
    }

    /// CASA's scalar quantity comparison.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn gt(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::relational(&v, &a, "gt"))
    }

    /// Whether a scalar quantity has angle or time dimensions.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn isangle(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::isangle(&v))
    }

    /// CASA's scalar quantity comparison.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn le(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::relational(&v, &a, "le"))
    }

    /// CASA's scalar log of a dimensionless quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn log(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::logarithm(&v, "log"))
    }

    /// CASA's scalar log10 of a dimensionless quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn log10(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::logarithm(&v, "log10"))
    }

    /// CASA's scalar quantity comparison.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn lt(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::relational(&v, &a, "lt"))
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

    /// CASA's scalar quantity comparison.
    #[pyo3(signature = (v = Variant::Empty, a = Variant::Empty), text_signature = "($self, v=[], a=[])")]
    fn ne(&self, py: Python<'_>, v: Variant, a: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::relational(&v, &a, "ne"))
    }

    /// Normalize an angle into a one-turn range, returning degrees.
    #[pyo3(signature = (v = Variant::Empty, a = None), text_signature = "($self, v=[], a=-0.5)")]
    fn norm(
        &self,
        py: Python<'_>,
        v: Variant,
        #[pyo3(from_py_with = supplied)] a: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (origin, pending) = match a {
            None => (-0.5, false),
            Some(value) => match value.extract::<f64>() {
                Ok(number) => (number, false),
                Err(_) => (-1.0, true),
            },
        };
        let result = call(py, || ops::normalize(&v, origin))?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function quanta_norm> returned a result with an exception set",
            ));
        }
        Ok(result)
    }

    /// Raise a scalar quantity to an integer power.
    #[pyo3(signature = (v = Variant::Empty, a = None), text_signature = "($self, v=[], a=1)")]
    fn pow(
        &self,
        py: Python<'_>,
        v: Variant,
        #[pyo3(from_py_with = supplied)] a: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (power, system_error) = match a {
            None => (1, false),
            Some(value) => match value.extract::<i64>() {
                Ok(number) => (number as i32, false),
                Err(error) if value.is_none() || error.is_instance_of::<PyOverflowError>(py) => {
                    (-1, true)
                }
                Err(_) => return Err(PyTypeError::new_err("argument _a must be an integer")),
            },
        };
        let result = call(py, || ops::power(&v, power))?;
        if system_error {
            return Err(PySystemError::new_err(
                "<built-in function quanta_pow> returned a result with an exception set",
            ));
        }
        Ok(result)
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

    /// CASA's scalar sin of a quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn sin(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::trigonometry(&v, "sin"))
    }

    /// Square root in CASA's canonical units.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn sqrt(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::sqrt(&v))
    }

    /// CASA's scalar tan of a quantity.
    #[pyo3(signature = (v = Variant::Empty), text_signature = "($self, v=[])")]
    fn tan(&self, py: Python<'_>, v: Variant) -> PyResult<Py<PyAny>> {
        call(py, || ops::trigonometry(&v, "tan"))
    }

    /// A quantity as text: the value with `prec` decimals, then the unit.
    #[pyo3(signature = (v = Variant::Empty, prec = 9), text_signature = "($self, v=[], prec=9)")]
    fn tos(&self, py: Python<'_>, v: Variant, prec: i64) -> PyResult<Py<PyAny>> {
        call(py, || ops::tos(&v, prec))
    }
}
