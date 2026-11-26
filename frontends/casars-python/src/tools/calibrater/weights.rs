// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public weight initialization on a live calibration tool.

use std::path::Path;

use pyo3::prelude::*;

use super::PyCalibrater;
use super::arguments::{strict_bool, supplied, text, window_map};

#[pymethods]
impl PyCalibrater {
    #[pyo3(signature = (wtmode = None, dowtsp = None, tsystable = None, gainfield = None, interp = None, spwmap = None), text_signature = "($self, wtmode='nyq', dowtsp=False, tsystable='', gainfield='', interp='', spwmap=[])")]
    #[allow(clippy::too_many_arguments)]
    fn initweights(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] wtmode: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] dowtsp: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] tsystable: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] gainfield: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] interp: Option<Bound<'_, PyAny>>,
        #[pyo3(from_py_with = supplied)] spwmap: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let wtmode = text(wtmode, "wtmode", "nyq")?;
        let dowtsp = dowtsp
            .as_ref()
            .map(|value| strict_bool(value, "initweights", 3))
            .transpose()?
            .unwrap_or(false);
        let tsystable = text(tsystable, "tsystable", "")?;
        let gainfield = text(gainfield, "gainfield", "")?;
        let interp = text(interp, "interp", "")?;
        let spwmap = window_map(spwmap, "initweights", &[])?;
        self.attached(py, |state| {
            state.init_weights(
                &wtmode,
                dowtsp,
                Path::new(&tsystable),
                &gainfield,
                &interp,
                spwmap,
            )
        })
    }
}
