// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pointing arithmetic using the caller's Python numeric protocols.

use pyo3::class::basic::CompareOp;
use pyo3::prelude::*;

use super::PySimutil;

#[pymethods]
#[allow(non_snake_case)]
impl PySimutil {
    /// Rotate coordinates, preserving Python and NumPy arithmetic types.
    fn applyRotate<'py>(
        &self,
        x: &Bound<'py, PyAny>,
        y: &Bound<'py, PyAny>,
        tcos: &Bound<'py, PyAny>,
        tsin: &Bound<'py, PyAny>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        Ok((
            tcos.mul(x)?.sub(tsin.mul(y)?)?,
            tsin.mul(x)?.add(tcos.mul(y)?)?,
        ))
    }

    /// Whether each input number is even, with the input's comparison shape.
    fn isEven<'py>(&self, num: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        num.rem(2)?.rich_compare(0, CompareOp::Eq)
    }

    /// Return 1 if the pattern needs an extra row or column, otherwise 0.
    fn needsFiller(
        &self,
        length: &Bound<'_, PyAny>,
        spacing: &Bound<'_, PyAny>,
        bs: &Bound<'_, PyAny>,
        npoints: &Bound<'_, PyAny>,
    ) -> PyResult<i32> {
        let limit = spacing.mul(npoints.sub(1)?)?.add(bs)?;
        Ok(i32::from(
            length.rich_compare(limit, CompareOp::Gt)?.is_truthy()?,
        ))
    }
}
