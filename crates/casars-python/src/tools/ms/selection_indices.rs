// SPDX-License-Identifier: LGPL-3.0-or-later
//! Auxiliary index records from CASA MeasurementSet selection expressions.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_ms::MsSelection;
use casa_ms::selection::MsSelectionIndices;
use casa_tables::lexical_absolute;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyDict, PyDictMethods, PyString};

use super::metadata_link::MetadataSnapshot;
use super::visibility::missing_selection_integer;
use super::{PyMsTool, runtime_error, strict_bool};
use crate::convert::selector::listing_selector;
use crate::convert::value::CasaValue;

#[derive(Default)]
pub(super) struct SelectionState {
    indices: MsSelectionIndices,
    error: Option<String>,
}

impl SelectionState {
    pub(super) fn use_implicit_selection(&mut self) {
        self.indices.scan.clear();
        self.indices.observation.clear();
        self.indices.state.clear();
        self.error = None;
    }

    fn record(&self) -> PyResult<CasaValue> {
        match &self.error {
            None => Ok(index_record(&self.indices, true)),
            Some(error) => Err(runtime_error(error)),
        }
    }
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = None, onlyparse = false), text_signature = "($self, items={}, onlyparse=False)")]
    fn msselect(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = selection_dictionary)] items: Option<Bound<'_, PyDict>>,
        #[pyo3(from_py_with = strict_bool)] onlyparse: bool,
    ) -> PyResult<bool> {
        let request = match items.as_ref().map(selection_request).transpose() {
            Ok(request) => request.flatten(),
            Err(_) => return Ok(false),
        };
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(attached) = attached.as_mut() else {
                return Ok(false);
            };
            let request = request.unwrap_or_default();
            let snapshot = attached
                .metadata_link()
                .snapshot(None)
                .map_err(runtime_error)?;
            let ms = &snapshot.ms;
            let parsed =
                ms.parse_ms_tool_selection(&request, &mut attached.selection_state.indices);
            let parsed = match parsed {
                Ok(parsed) => parsed,
                Err(error) => {
                    attached.selection_state.error = Some(error.to_string());
                    return Ok(false);
                }
            };
            let has_expression = parsed.has_row_expression();
            let rows = (!onlyparse && has_expression).then(|| parsed.select_rows(ms));
            attached.selection_state.error = None;
            if let Some(rows) = rows {
                let Ok(mut rows) = rows else {
                    return Ok(false);
                };
                if let Some(previous) = &attached.selected_rows {
                    for row in &mut rows {
                        *row = previous[*row];
                    }
                }
                let nonempty = !rows.is_empty();
                attached.set_selected_rows(Some(rows))?;
                Ok(nonempty)
            } else {
                Ok(has_expression)
            }
        })
    }

    #[pyo3(signature = (datadescid = 0, reset = false), text_signature = "($self, datadescid=0, reset=False)")]
    fn selectinit(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = selectinit_id)] datadescid: i64,
        #[pyo3(from_py_with = strict_bool)] reset: bool,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(attached) = attached.as_mut() else {
                return Ok(false);
            };
            if reset {
                attached.reset_visibility_selection()?;
                return Ok(true);
            }
            if (datadescid as i32) < 0 {
                return Err(runtime_error(
                    "The data description id must be a list of positive integers",
                ));
            }
            attached.select_visibility_description(datadescid)
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn msselectedindices(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.attachment_info(py, |attached| {
            let attached = attached.ok_or_else(|| runtime_error("Table column TIME is unknown"))?;
            attached.selection_state.record()
        })?
        .into_python(py)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (vis = None, spw = None, field = None, baseline = None, time = None, scan = None, uvrange = None, observation = None, polarization = None, taql = None), text_signature = "($self, vis='', spw=[], field=[], baseline=[], time=[], scan=[], uvrange=[], observation=[], polarization=[], taql='')")]
    fn msseltoindex(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = selection_visibility)] vis: Option<PathBuf>,
        spw: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        baseline: Option<&Bound<'_, PyAny>>,
        time: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        observation: Option<&Bound<'_, PyAny>>,
        polarization: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = selection_taql)] taql: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let request = MsSelection {
            spw: selector(spw)?,
            field: selector(field)?,
            antenna: selector(baseline)?,
            timerange: selector(time)?,
            scan: selector(scan)?,
            uvrange: selector(uvrange)?,
            observation: selector(observation)?,
            correlation: selector(polarization)?,
            msselect: taql.filter(|text| !text.is_empty()),
            ..MsSelection::default()
        };
        let indices = py.detach(|| {
            let path = lexical_absolute(
                vis.as_deref()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new(".")),
            );
            let view = MetadataSnapshot::open(&path).map_err(runtime_error)?;
            let ms = &view.ms;
            if ms.ms_version() != Some(2.0) {
                return Err(runtime_error(
                    "These data are not in MSv2 format - use ms1toms2 to convert",
                ));
            }

            let mut indices = MsSelectionIndices::default();
            ms.parse_ms_tool_selection(&request, &mut indices)
                .map_err(runtime_error)?;
            Ok(indices)
        })?;
        index_record(&indices, false).into_python(py)
    }
}

fn selectinit_id(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if missing_selection_integer(value)? {
        return Ok(-1);
    }
    value
        .extract::<i64>()
        .map_err(|_| PyTypeError::new_err("argument _datadescid must be an integer"))
}

fn selector(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<String>> {
    let text = listing_selector(value)?;
    Ok((!text.is_empty()).then_some(text))
}

fn selection_dictionary<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyDict>>> {
    value
        .cast::<PyDict>()
        .map(|record| Some(record.clone()))
        .map_err(|_| PyTypeError::new_err("_items is not a dictionary"))
}

fn selection_visibility(value: &Bound<'_, PyAny>) -> PyResult<Option<PathBuf>> {
    #[cfg(unix)]
    if let Ok(bytes) = value.cast::<PyBytes>() {
        use std::os::unix::ffi::OsStringExt;
        let path = std::ffi::OsString::from_vec(bytes.as_bytes().to_vec());
        return Ok(Some(path.into()));
    }
    selection_text(value, "vis").map(|text| Some(text.into()))
}

fn selection_taql(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    selection_text(value, "taql").map(Some)
}

fn selection_text(value: &Bound<'_, PyAny>, parameter: &str) -> PyResult<String> {
    if value.is_instance_of::<PyString>() {
        value.extract()
    } else if value.is_instance_of::<PyBytes>() {
        value.call_method1("decode", ("utf-8",))?.extract()
    } else {
        Err(PyTypeError::new_err(format!(
            "argument _{parameter} must be a string"
        )))
    }
}

fn index_record(indices: &MsSelectionIndices, attached: bool) -> CasaValue {
    let mut fields = vec![
        ("field", vector(&indices.field)),
        ("scan", vector(&indices.scan)),
        ("antenna1", vector(&indices.antenna1)),
        ("antenna2", vector(&indices.antenna2)),
        ("baselines", matrix(indices.baselines.as_deref())),
        ("spw", vector(&indices.spw)),
        (
            "channel",
            matrix((!indices.channel.is_empty()).then_some(indices.channel.as_slice())),
        ),
        ("spwdd", vector(&indices.spwdd)),
        ("poldd", vector(&indices.poldd)),
        ("dd", vector(&indices.descriptions())),
    ];
    if attached {
        fields.extend([
            ("observationid", vector(&indices.observation)),
            ("stateid", vector(&indices.state)),
            ("feed1", vector(&[])),
            ("feed2", vector(&[])),
            ("feedpairs", vector(&[])),
        ]);
    } else {
        fields.push(("obsids", vector(&indices.observation)));
    }
    CasaValue::record(fields)
}

fn vector(values: &[i32]) -> CasaValue {
    CasaValue::Ints {
        shape: vec![values.len()],
        values: values.iter().copied().map(i64::from).collect(),
    }
}

fn matrix<const COLUMNS: usize>(rows: Option<&[[i32; COLUMNS]]>) -> CasaValue {
    let column_count = rows.map_or(0, |_| COLUMNS);
    let rows = rows.unwrap_or_default();
    CasaValue::Ints {
        shape: vec![rows.len(), column_count],
        values: (0..column_count)
            .flat_map(|col| rows.iter().map(move |row| i64::from(row[col])))
            .collect(),
    }
}

fn selection_request(items: &Bound<'_, PyDict>) -> PyResult<Option<MsSelection>> {
    let mut request = MsSelection::default();
    let mut named = false;
    let fields = items
        .iter()
        .map(|(key, value)| selection_text(&key, "items").map(|key| (key, value)))
        .collect::<PyResult<BTreeMap<_, _>>>()?;
    // CASA's dictionary is a sorted casac::record, so alias precedence
    // follows field names rather than Python's insertion order.
    for (key, value) in fields {
        let slot = match key.as_str() {
            "spw" => &mut request.spw,
            "time" => &mut request.timerange,
            "field" => &mut request.field,
            "baseline" | "antenna" => &mut request.antenna,
            "scan" => &mut request.scan,
            "scanintent" | "state" => &mut request.intent,
            "polarization" => &mut request.correlation,
            "uvdist" => &mut request.uvrange,
            "observation" => &mut request.observation,
            "array" => &mut request.array,
            "taql" => &mut request.msselect,
            _ => continue,
        };
        let value = selection_text(&value, &key)?;
        *slot = (!value.is_empty()).then_some(value);
        named |= slot.is_some();
    }
    Ok(named.then_some(request))
}
