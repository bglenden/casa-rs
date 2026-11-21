// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `coordsys` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1027.

use pyo3::prelude::*;

use super::{PyCoordsys, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyCoordsys {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (refcode = None, proj = None, projpar = None, refpix = None, refval = None, incr = None, xform = None, poles = None), text_signature = "($self, refcode='', proj='', projpar=[-1.0], refpix=[-1.0], refval=[], incr=[], xform=[], poles=[])")]
    fn setdirection(
        &self,
        refcode: Option<&Bound<'_, PyAny>>,
        proj: Option<&Bound<'_, PyAny>>,
        projpar: Option<&Bound<'_, PyAny>>,
        refpix: Option<&Bound<'_, PyAny>>,
        refval: Option<&Bound<'_, PyAny>>,
        incr: Option<&Bound<'_, PyAny>>,
        xform: Option<&Bound<'_, PyAny>>,
        poles: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setdirection")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (doppler = None, list = None), text_signature = "($self, doppler='RADIO', list=True)")]
    fn summary(
        &self,
        doppler: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("summary")
    }
}
