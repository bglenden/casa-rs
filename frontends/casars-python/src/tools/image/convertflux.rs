// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.convertflux` wrapper delegating to [`casa_imageanalysis::tool::convertflux`].

use casa_imageanalysis::tool::convertflux::convert_flux_quantity;
use pyo3::prelude::*;

use crate::convert::{Variant, quantity, value::CasaValue};

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (value=None, major=None, minor=None, r#type="Gaussian", topeak=true, channel=-1, polarization=-1), text_signature = "($self, value=[], major=[], minor=[], type='Gaussian', topeak=True, channel=-1, polarization=-1)")]
    fn convertflux(
        &self,
        py: Python<'_>,
        value: Option<Variant>,
        major: Option<Variant>,
        minor: Option<Variant>,
        r#type: &str,
        topeak: bool,
        channel: i64,
        polarization: i64,
    ) -> PyResult<Py<PyAny>> {
        let _ = r#type; // CASA always uses the Gaussian shape here.
        let (value, logs) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok((CasaValue::empty_record(), Vec::new()));
            };
            let image = attachment.lock();
            let (converted, logs) = convert_flux_quantity(
                &image,
                &attachment.beam_view(),
                quantity(&value.unwrap_or(Variant::Empty))?,
                [
                    quantity(&major.unwrap_or(Variant::Empty))?,
                    quantity(&minor.unwrap_or(Variant::Empty))?,
                ],
                topeak,
                (channel, polarization),
            )
            .map_err(raised)?;
            Ok((
                CasaValue::quantity(converted.value(), converted.unit().name()),
                logs,
            ))
        })?;
        emit_tool_logs(py, &logs)?;
        value.into_python(py)
    }
}
