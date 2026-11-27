// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 restoring-beam wrappers delegating to [`casa_imageanalysis::tool::beams`].

pub(super) use casa_imageanalysis::tool::beams::{
    beam_area_record, common_beam_record, restoring_beam_record,
};
use pyo3::prelude::*;

use crate::convert::value::CasaValue;

use super::PyImageTool;
use super::handle::raised;

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (channel=-1, polarization=-1, mbret="list"), text_signature = "($self, channel=-1, polarization=-1, mbret='list')")]
    fn restoringbeam(
        &self,
        py: Python<'_>,
        channel: i64,
        polarization: i64,
        mbret: &str,
    ) -> PyResult<Py<PyAny>> {
        py.detach(|| -> PyResult<CasaValue> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let handle = attachment.lock();
            let record = restoring_beam_record(
                &attachment.beam_view(),
                || handle.coordinate_record(),
                channel,
                polarization,
                mbret,
            )
            .map_err(raised)?;
            Ok(CasaValue::from_record(&record))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (channel=-1, polarization=-1), text_signature = "($self, channel=-1, polarization=-1)")]
    fn beamarea(&self, py: Python<'_>, channel: i64, polarization: i64) -> PyResult<Py<PyAny>> {
        py.detach(|| -> PyResult<CasaValue> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let record = beam_area_record(
                &attachment.lock(),
                &attachment.beam_view(),
                channel,
                polarization,
            )
            .map_err(raised)?;
            Ok(CasaValue::from_record(&record))
        })?
        .into_python(py)
    }

    fn commonbeam(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        py.detach(|| -> PyResult<CasaValue> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let record =
                common_beam_record(&attachment.lock(), &attachment.beam_view()).map_err(raised)?;
            Ok(CasaValue::from_record(&record))
        })?
        .into_python(py)
    }
}
