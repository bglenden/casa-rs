// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.beamforconvolvedsize` / `ia.deconvolvefrombeam` wrapper delegating to [`casa_imageanalysis::tool::beam_ops`].

use casa_core::quanta::Quantity;
use casa_imageanalysis::tool::beam_ops::{beam_for_convolved_size, deconvolve_from_beam};
use pyo3::prelude::*;

use crate::convert::{Variant, read_quantity, value::CasaValue};

use super::PyImageTool;
use super::handle::raised;

fn quantities(argument: Option<Variant>, label: &str) -> PyResult<Vec<Quantity>> {
    let strings = match argument {
        Some(Variant::Text(value)) => {
            let separator = if value.contains(',') { ',' } else { ' ' };
            if value.contains(separator) {
                value
                    .split(separator)
                    .filter(|part| !part.is_empty() && *part != " ")
                    .map(str::to_owned)
                    .collect()
            } else {
                vec![value]
            }
        }
        Some(Variant::Texts(values)) => values,
        _ => return Err(raised(format!("Cannot understand {label} values"))),
    };
    strings
        .iter()
        .map(|value| {
            if value.contains("pix") {
                // CASA accepts this unit in the list reader. All beam
                // components that use it fail the angular-unit check.
                return Quantity::new(0.0, "pix").map_err(raised);
            }
            read_quantity(value, &format!(" In converting quantity {value}")).map_err(Into::into)
        })
        .collect()
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::unused_self)]
    #[pyo3(signature = (source=None, convolved=None), text_signature = "($self, source=[], convolved=[])")]
    fn beamforconvolvedsize(
        &self,
        py: Python<'_>,
        source: Option<Variant>,
        convolved: Option<Variant>,
    ) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            let source = quantities(source, "source")?;
            let convolved = quantities(convolved, "target")?;
            let record = beam_for_convolved_size(&source, &convolved).map_err(raised)?;
            Ok::<_, PyErr>(CasaValue::from_record(&record))
        })?
        .into_python(py)
    }

    #[allow(clippy::unused_self)]
    #[pyo3(signature = (source=None, beam=None), text_signature = "($self, source=[], beam=[])")]
    fn deconvolvefrombeam(
        &self,
        py: Python<'_>,
        source: Option<Variant>,
        beam: Option<Variant>,
    ) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            let source = quantities(source, "source")?;
            let beam = quantities(beam, "beam")?;
            let record = deconvolve_from_beam(&source, &beam).map_err(raised)?;
            Ok::<_, PyErr>(CasaValue::from_record(&record))
        })?
        .into_python(py)
    }
}
