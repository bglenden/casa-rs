// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.summary` wrapper delegating to [`casa_imageanalysis::tool::summary`].

use casa_imageanalysis::tool::summary::{SummaryOptions, summarize_image};
use pyo3::prelude::*;

use crate::convert::value::CasaValue;

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};

fn dimension_list(values: &[usize]) -> CasaValue {
    CasaValue::Ints {
        shape: vec![values.len()],
        values: values.iter().map(|&value| value as i64).collect(),
    }
}

fn text_list(values: Vec<String>) -> CasaValue {
    if values.iter().all(String::is_empty) {
        CasaValue::TextArray {
            shape: vec![values.len()],
            values,
        }
    } else {
        CasaValue::Texts(values)
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (doppler="RADIO", list=true, pixelorder=true, verbose=false), text_signature = "($self, doppler='RADIO', list=True, pixelorder=True, verbose=False)")]
    fn summary(
        &self,
        py: Python<'_>,
        doppler: &str,
        list: bool,
        pixelorder: bool,
        verbose: bool,
    ) -> PyResult<Py<PyAny>> {
        let (value, logs) = py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok((CasaValue::empty_record(), Vec::new()));
            };
            let options = SummaryOptions {
                doppler: doppler.to_owned(),
                list,
                pixel_order: pixelorder,
                velocity: false,
                optical: false,
                verbose,
            };
            let (mut report, logs) = summarize_image(attachment, &options).map_err(raised)?;
            if list {
                report.messages.clear();
            }
            let mut fields = vec![
                ("refpix", CasaValue::floats(report.refpix)),
                ("axisnames", text_list(report.axisnames)),
                ("axisunits", text_list(report.axisunits)),
                ("refval", CasaValue::floats(report.refval)),
                ("incr", CasaValue::floats(report.incr)),
                ("messages", text_list(report.messages)),
                ("ndim", CasaValue::Int(report.ndim as i64)),
                ("shape", dimension_list(&report.shape)),
                ("tileshape", dimension_list(&report.tileshape)),
                ("unit", CasaValue::Text(report.unit)),
                ("hasmask", CasaValue::Bool(report.hasmask)),
                ("defaultmask", CasaValue::Text(report.defaultmask)),
                ("masks", text_list(report.masks)),
                ("imagetype", CasaValue::Text(report.imagetype)),
            ];
            if let Some(beam) = &report.restoringbeam {
                fields.push(("restoringbeam", CasaValue::from_record(beam)));
            }
            if let Some(beams) = &report.perplanebeams {
                fields.push(("perplanebeams", CasaValue::from_record(beams)));
            }
            Ok::<_, PyErr>((CasaValue::record(fields), logs))
        })?;
        emit_tool_logs(py, &logs)?;
        value.into_python(py)
    }
}
