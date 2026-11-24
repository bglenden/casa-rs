// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use casa_ms::subtables::FieldDirectionColumn;
use casa_ms::{MeasurementSet, MeasurementSetScanSummary, MeasurementSetSummary, MsResult};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::selection_indices::selection_text;
use super::{PyMsTool, runtime_error, strict_bool};
use crate::convert::value::CasaValue;
use crate::tools::msmetadata::direction_record;
use crate::tools::{LogLine, emit};

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (verbose = false, listfile = None, listunfl = false, cachesize = 50.0, overwrite = false, wantreturn = true), text_signature = "($self, verbose=False, listfile='', listunfl=False, cachesize=50.0, overwrite=False, wantreturn=True)")]
    fn summary(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = strict_bool)] verbose: bool,
        #[pyo3(from_py_with = listfile_argument)] listfile: Option<PathBuf>,
        #[pyo3(from_py_with = strict_bool)] listunfl: bool,
        cachesize: f64,
        #[pyo3(from_py_with = strict_bool)] overwrite: bool,
        #[pyo3(from_py_with = strict_bool)] wantreturn: bool,
    ) -> PyResult<Py<PyAny>> {
        let listfile = listfile.filter(|file| !file.as_os_str().is_empty());
        let result = self.attachment_info(py, |attachment| {
            let Some(attachment) = attachment else {
                return Ok(None);
            };
            if let Some(path) = listfile.as_deref()
                && !overwrite
                && path.exists()
            {
                return Err(existing_report(path));
            }
            let link = attachment.working_metadata_link();
            let (snapshot, rows) = link.source_and_rows().map_err(runtime_error)?;
            let ms = &snapshot.ms;
            let mut summary = MeasurementSetSummary::from_tool_view(
                ms,
                rows.as_deref(),
                verbose,
                listunfl,
                cachesize,
            )
            .map_err(runtime_error)?;
            summary.measurement_set.path = Some(attachment.name.display().to_string());
            let record = if wantreturn {
                summary_record(ms, &summary).map_err(runtime_error)?
            } else {
                CasaValue::empty_record()
            };
            let report = summary.render_text();
            link.verify_directory().map_err(runtime_error)?;
            if let Some(path) = listfile.as_deref() {
                write_report(path, overwrite, &report)?;
            }
            Ok(Some((record, report)))
        })?;
        let Some((record, report)) = result else {
            return CasaValue::empty_record().into_python(py);
        };
        let line = if let Some(path) = listfile.as_deref() {
            LogLine::info(format!("Writing output to file: {}", path.display()))
        } else {
            LogLine::info(report)
        };
        emit(py, "casars.tools.ms", &[line])?;
        record.into_python(py)
    }
}

fn write_report(path: &Path, overwrite: bool, report: &str) -> PyResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(overwrite)
        .truncate(overwrite)
        .create_new(!overwrite)
        .open(path)
        .map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists && !overwrite {
                existing_report(path)
            } else {
                runtime_error(format!("cannot write {}: {error}", path.display()))
            }
        })?;
    file.write_all(report.as_bytes()).map_err(runtime_error)
}

fn existing_report(path: &Path) -> PyErr {
    runtime_error(format!(
        "File: {} already exists; delete it, choose another name, or set overwrite=true.",
        path.display()
    ))
}

fn summary_record(ms: &MeasurementSet, summary: &MeasurementSetSummary) -> MsResult<CasaValue> {
    let info = &summary.measurement_set;
    let Some((begin, end)) = info.start_mjd_seconds.zip(info.end_mjd_seconds) else {
        return Ok(CasaValue::empty_record());
    };
    let mut record = vec![
        (
            "numrecords".to_string(),
            CasaValue::Int(info.row_count as i64),
        ),
        ("IntegrationTime".to_string(), CasaValue::Float(end - begin)),
        ("BeginTime".to_string(), CasaValue::Float(begin / 86_400.0)),
        ("EndTime".to_string(), CasaValue::Float(end / 86_400.0)),
        (
            "timeref".to_string(),
            CasaValue::Text(info.time_reference.clone().unwrap_or_default()),
        ),
        (
            "nfields".to_string(),
            CasaValue::Int(info.field_count as i64),
        ),
    ];
    let field_table = ms.field()?;
    for field in &summary.fields {
        let direction = field_table.metadata_direction_at_seconds(
            field.field_id,
            FieldDirectionColumn::Phase,
            0.0,
        )?;
        record.push((
            format!("field_{}", field.field_id),
            CasaValue::record([
                ("name", CasaValue::Text(field.name.clone())),
                ("code", CasaValue::Text(field.code.clone())),
                ("direction", direction_record(direction)),
            ]),
        ));
    }

    let mut scans = BTreeMap::<i32, BTreeMap<usize, CasaValue>>::new();
    let mut previous = None;
    let mut subset = 0;
    for scan in summary.scans.iter().take(10_000) {
        let key = (scan.observation_id, scan.array_id, scan.scan_number);
        subset = if previous == Some(key) { subset + 1 } else { 0 };
        previous = Some(key);
        scans
            .entry(scan.scan_number)
            .or_default()
            .insert(subset, scan_record(scan));
    }
    record.extend(scans.into_iter().map(|(scan, fields)| {
        let subs = fields
            .into_iter()
            .map(|(index, value)| (index.to_string(), value));
        (format!("scan_{scan}"), CasaValue::record(subs))
    }));
    Ok(CasaValue::record(record))
}

fn scan_record(scan: &MeasurementSetScanSummary) -> CasaValue {
    CasaValue::record([
        (
            "BeginTime",
            CasaValue::Float(scan.start_mjd_seconds / 86_400.0),
        ),
        ("EndTime", CasaValue::Float(scan.end_mjd_seconds / 86_400.0)),
        ("scanId", CasaValue::Int(scan.scan_number.into())),
        ("FieldId", CasaValue::Int(scan.field_id.into())),
        ("FieldName", CasaValue::Text(scan.field_name.clone())),
        (
            "StateId",
            CasaValue::Int(i64::from(scan.state_ids.first().copied().unwrap_or(-1))),
        ),
        ("nRow", CasaValue::Int(scan.row_count as i64)),
        (
            "IntegrationTime",
            CasaValue::Float(scan.mean_interval_seconds),
        ),
        (
            "SpwIds",
            CasaValue::Ints {
                shape: vec![scan.spectral_window_ids.len()],
                values: scan
                    .spectral_window_ids
                    .iter()
                    .copied()
                    .map(i64::from)
                    .collect(),
            },
        ),
    ])
}

fn listfile_argument(value: &Bound<'_, PyAny>) -> PyResult<Option<PathBuf>> {
    #[cfg(unix)]
    if let Ok(bytes) = value.cast::<PyBytes>() {
        use std::os::unix::ffi::OsStringExt;
        let path = std::ffi::OsString::from_vec(bytes.as_bytes().to_vec());
        return Ok(Some(path.into()));
    }
    selection_text(value, "listfile").map(|path| Some(path.into()))
}
