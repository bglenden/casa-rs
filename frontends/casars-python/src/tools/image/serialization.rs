// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.torecord` / `ia.fromrecord` wrapper delegating to [`casa_imageanalysis::tool::serialization`].

use std::path::Path;

use casa_imageanalysis::tool::serialization::{image_from_record_with_history, image_to_record};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::tools::table::{casa_record, values};

use super::PyImageTool;
use super::handle::{external_open_check, raised};

#[pymethods]
impl PyImageTool {
    fn torecord(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        py.detach(|| -> PyResult<crate::convert::value::CasaValue> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(crate::convert::value::CasaValue::empty_record());
            };
            let record = image_to_record(attachment).map_err(raised)?;
            Ok(values::record(&record, Path::new(".")))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (record, outfile=""), text_signature = "($self, record, outfile='')")]
    fn fromrecord(
        &self,
        py: Python<'_>,
        record: &Bound<'_, PyDict>,
        outfile: &str,
    ) -> PyResult<bool> {
        let record = casa_record(py, Some(record))?;
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let attached = image_from_record_with_history(
                &record,
                outfile,
                true,
                state.history,
                external_open_check(),
            )
            .map_err(raised)?;
            state.image = Some(attached);
            Ok(true)
        })
    }
}
