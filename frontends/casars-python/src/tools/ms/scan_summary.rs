// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::{ToolScanSummary, tool_scan_summary};
use pyo3::prelude::*;

use super::{PyMsTool, runtime_error};
use crate::convert::value::CasaValue;

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn getscansummary(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.attachment_info(py, |attachment| {
            let Some(attachment) = attachment else {
                return Ok(CasaValue::empty_record());
            };
            let link = attachment.working_metadata_link();
            let (snapshot, rows) = link.source_and_rows().map_err(runtime_error)?;
            let scans = tool_scan_summary(&snapshot.ms, rows.as_deref()).map_err(runtime_error)?;
            link.verify_directory().map_err(runtime_error)?;
            Ok(CasaValue::record(scans.into_iter().map(
                |(scan, subscans)| {
                    (
                        scan.to_string(),
                        CasaValue::record(
                            subscans.into_iter().enumerate().map(|(index, subscan)| {
                                (index.to_string(), subscan_record(subscan))
                            }),
                        ),
                    )
                },
            )))
        })?
        .into_python(py)
    }
}

fn subscan_record(scan: ToolScanSummary) -> CasaValue {
    CasaValue::record([
        ("BeginTime", CasaValue::Float(scan.begin_day)),
        ("DDIds", integer_array(scan.data_description_ids)),
        ("EndTime", CasaValue::Float(scan.end_day)),
        ("FieldId", CasaValue::Int(scan.field_id.into())),
        ("IntegrationTime", CasaValue::Float(scan.mean_exposure)),
        ("SpwIds", integer_array(scan.spectral_window_ids)),
        ("StateId", CasaValue::Int(scan.state_id.into())),
        ("nRow", CasaValue::Int(scan.row_count as i64)),
    ])
}

fn integer_array(ids: Vec<i32>) -> CasaValue {
    CasaValue::Ints {
        shape: vec![ids.len()],
        values: ids.into_iter().map(i64::from).collect(),
    }
}
