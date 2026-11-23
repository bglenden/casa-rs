// SPDX-License-Identifier: LGPL-3.0-or-later
//! Auxiliary index records from CASA MeasurementSet selection expressions.

use std::path::Path;

use casa_ms::MsSelection;
use casa_ms::selection::MsSelectionIndices;
use casa_tables::lexical_absolute;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::metadata_link::MetadataSnapshot;

use super::{PyMsTool, runtime_error, strict_bool};
use crate::convert::selector::listing_selector;
use crate::convert::value::CasaValue;

#[derive(Default)]
pub(super) enum SelectionState {
    #[default]
    Empty,
    Parsed(Box<MsSelectionIndices>),
    Invalid(String),
}

impl SelectionState {
    fn record(&self) -> PyResult<CasaValue> {
        match self {
            Self::Empty => Ok(index_record(&MsSelectionIndices::default(), true)),
            Self::Parsed(indices) => Ok(index_record(indices, true)),
            Self::Invalid(error) => Err(runtime_error(error)),
        }
    }
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = None, onlyparse = false), text_signature = "($self, items={}, onlyparse=False)")]
    fn msselect(
        &self,
        py: Python<'_>,
        items: Option<&Bound<'_, PyDict>>,
        #[pyo3(from_py_with = strict_bool)] onlyparse: bool,
    ) -> PyResult<bool> {
        let request = match items.map(selection_request).transpose() {
            Ok(request) => request.flatten(),
            Err(_) => return Ok(false),
        };
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(attached) = attached.as_mut() else {
                return Ok(false);
            };
            let Some(request) = request else {
                attached.selection_state = SelectionState::Empty;
                return Ok(false);
            };
            let snapshot = attached
                .metadata_link()
                .snapshot(None)
                .map_err(runtime_error)?;
            let ms = &snapshot.ms;
            let selected = ms.parse_ms_tool_selection(&request).and_then(|parsed| {
                let has_expression = parsed.has_row_expression();
                let rows = if onlyparse {
                    parsed.validate_taql(ms)?;
                    None
                } else if has_expression {
                    Some(parsed.select_rows(ms)?)
                } else {
                    None
                };
                Ok((parsed.indices, has_expression, rows))
            });
            let (indices, has_expression, rows) = match selected {
                Ok(result) => result,
                Err(error) => {
                    attached.selection_state = SelectionState::Invalid(error.to_string());
                    return Ok(false);
                }
            };
            attached.selection_state = SelectionState::Parsed(Box::new(indices));
            if let Some(mut rows) = rows {
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

    #[pyo3(signature = (), text_signature = "($self)")]
    fn msselectedindices(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.attachment_info(py, |attached| {
            let attached = attached.ok_or_else(|| runtime_error("Table column TIME is unknown"))?;
            attached.selection_state.record()
        })?
        .into_python(py)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (vis = "", spw = None, field = None, baseline = None, time = None, scan = None, uvrange = None, observation = None, polarization = None, taql = ""), text_signature = "($self, vis='', spw=[], field=[], baseline=[], time=[], scan=[], uvrange=[], observation=[], polarization=[], taql='')")]
    fn msseltoindex(
        &self,
        py: Python<'_>,
        vis: &str,
        spw: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        baseline: Option<&Bound<'_, PyAny>>,
        time: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        observation: Option<&Bound<'_, PyAny>>,
        polarization: Option<&Bound<'_, PyAny>>,
        taql: &str,
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
            msselect: (!taql.is_empty()).then(|| taql.to_owned()),
            ..MsSelection::default()
        };
        let indices = py.detach(|| {
            let path = lexical_absolute(Path::new(if vis.is_empty() { "." } else { vis }));
            let view = MetadataSnapshot::open(&path).map_err(runtime_error)?;
            let ms = &view.ms;
            if ms.ms_version() != Some(2.0) {
                return Err(runtime_error(
                    "These data are not in MSv2 format - use ms1toms2 to convert",
                ));
            }

            let parsed = ms
                .parse_ms_tool_selection(&request)
                .map_err(runtime_error)?;
            parsed.validate_taql(ms).map_err(runtime_error)?;
            Ok(parsed.indices)
        })?;
        index_record(&indices, false).into_python(py)
    }
}

fn selector(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<String>> {
    let text = listing_selector(value)?;
    Ok((!text.is_empty()).then_some(text))
}

fn index_record(indices: &MsSelectionIndices, attached: bool) -> CasaValue {
    let mut fields = vec![
        ("field", vector(&indices.field)),
        ("scan", vector(&indices.scan)),
        ("antenna1", vector(&indices.antenna1)),
        ("antenna2", vector(&indices.antenna2)),
        ("baselines", matrix(&indices.baselines)),
        ("spw", vector(&indices.spw)),
        ("channel", matrix(&indices.channel)),
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

fn matrix<const COLUMNS: usize>(rows: &[[i32; COLUMNS]]) -> CasaValue {
    let column_count = if rows.is_empty() { 0 } else { COLUMNS };
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
    for (key, value) in items {
        let key = key.extract::<&str>()?;
        let slot = match key {
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
        let value = value.extract::<String>()?;
        *slot = (!value.is_empty()).then_some(value);
        named |= slot.is_some();
    }
    Ok(named.then_some(request))
}
