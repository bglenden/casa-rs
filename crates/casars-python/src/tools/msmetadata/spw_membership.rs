// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::{MeasurementSet, MsError, MsResult};
use numpy::{IntoPyArray, PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::PyMsMetadata;
use super::names::Indices;
use super::scan_queries::{FieldSelector, int_array, matching_scan};
use super::spectral::spw_row;
use crate::convert::{Variant, value::CasaValue};

fn checked_spw(ms: &MeasurementSet, id: i64) -> MsResult<i32> {
    spw_row(&ms.spectral_window()?, id).map(|id| id as i32)
}

fn unsigned_group_dict(
    py: Python<'_>,
    groups: BTreeMap<String, BTreeSet<i32>>,
) -> PyResult<Py<PyAny>> {
    let result = PyDict::new(py);
    for (key, ids) in groups {
        let values: Vec<u64> = ids.into_iter().map(|id| id as u64).collect();
        result.set_item(key, values.into_pyarray(py))?;
    }
    Ok(result.into_any().unbind())
}

fn window_names(spwids: Option<&Bound<'_, PyAny>>) -> PyResult<Option<BTreeSet<String>>> {
    let Some(spwids) = spwids.filter(|value| !value.is_none()) else {
        return Ok(None);
    };
    if let Ok(array) = spwids.cast::<PyUntypedArray>()
        && array.len() == 0
        && array.dtype().kind() != b'b'
    {
        return Ok(Some(BTreeSet::new()));
    }
    if matches!(Indices::capture(Some(spwids))?, Indices::BooleanVector) {
        return Ok(None);
    }
    match spwids.extract::<Variant>()? {
        Variant::Empty => Ok(None),
        Variant::Text(name) if name.is_empty() => Ok(None),
        Variant::Text(name) => Ok(Some(BTreeSet::from([name]))),
        Variant::Texts(names) => Ok(Some(names.into_iter().collect())),
        _ => Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Spectral window names must be a string or an array of strings",
        )),
    }
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (spw = -1, asnames = false), text_signature = "($self, spw=-1, asnames=False)")]
    fn fieldsforspw(&self, py: Python<'_>, spw: i64, asnames: bool) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            let spw = checked_spw(ms, spw)?;
            let ids = index
                .samples()
                .filter(|sample| sample.spw == spw)
                .map(|sample| sample.field)
                .collect::<BTreeSet<_>>();
            if asnames {
                let fields = ms.field()?;
                let values: Vec<String> = ids
                    .into_iter()
                    .map(|id| fields.name(id as usize))
                    .collect::<MsResult<BTreeSet<_>>>()?
                    .into_iter()
                    .collect();
                Ok(CasaValue::TextArray {
                    shape: vec![values.len()],
                    values,
                })
            } else {
                Ok(int_array(ids))
            }
        })?
        .into_python(py)
    }

    #[pyo3(signature = (field = FieldSelector::Name(String::new())), text_signature = "($self, field='')")]
    fn spwsforfield(&self, py: Python<'_>, field: FieldSelector) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            let fields = ms.field()?;
            let (ids, name) = match field {
                FieldSelector::Name(name) => {
                    let mut ids = BTreeSet::new();
                    for row in 0..fields.row_count() {
                        if fields.name(row)? == name {
                            ids.insert(row as i32);
                        }
                    }
                    (ids, Some(name))
                }
                FieldSelector::Id(id) if id < 0 => return Ok(int_array([])),
                FieldSelector::Id(id) if id < fields.row_count() as i64 => {
                    (BTreeSet::from([id as i32]), None)
                }
                FieldSelector::Id(id) => {
                    return Err(MsError::InvalidInput(format!(
                        "Field ID {id} is out of range"
                    )));
                }
                FieldSelector::Unsupported => {
                    return Err(MsError::InvalidInput(
                        "Unacceptable type for field parameter".into(),
                    ));
                }
            };
            let windows: BTreeSet<_> = index
                .samples()
                .filter(|sample| ids.contains(&sample.field))
                .map(|sample| sample.spw)
                .collect();
            if let Some(name) = name.filter(|_| windows.is_empty()) {
                return Err(MsError::InvalidInput(format!("Unknown field {name}")));
            }
            Ok(int_array(windows))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn spwsforfields(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let result = self.with_scan_index(py, |_, index| {
            let mut windows = BTreeMap::<i32, BTreeSet<i32>>::new();
            for sample in index.samples() {
                windows.entry(sample.field).or_default().insert(sample.spw);
            }
            Ok(windows
                .into_iter()
                .map(|(field, windows)| (field.to_string(), windows))
                .collect())
        })?;
        unsigned_group_dict(py, result)
    }

    #[pyo3(signature = (spwids = None), text_signature = "($self, spwids='')")]
    fn spwsfornames(
        &self,
        py: Python<'_>,
        spwids: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let selection = window_names(spwids)?;
        let windows = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let mut matching = BTreeMap::<String, Vec<i64>>::new();
            for row in 0..windows.row_count() {
                let name = windows.name(row)?;
                if selection
                    .as_ref()
                    .is_none_or(|selected| selected.contains(&name))
                {
                    matching.entry(name).or_default().push(row as i64);
                }
            }
            Ok(matching)
        })?;
        let result = PyDict::new(py);
        for (name, ids) in windows {
            if ids.len() == 1 {
                result.set_item(name, vec![ids[0] as u64].into_pyarray(py))?;
            } else {
                result.set_item(name, ids.into_pyarray(py))?;
            }
        }
        Ok(result.into_any().unbind())
    }

    #[pyo3(signature = (scan = -1, obsid = -1, arrayid = -1), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1)")]
    fn spwsforscan(
        &self,
        py: Python<'_>,
        scan: i64,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            if scan < 0 {
                return Err(MsError::InvalidInput("Scan must be nonnegative".into()));
            }
            let windows: BTreeSet<_> = matching_scan(index, ms, scan, obsid, arrayid)?
                .flat_map(|scan| scan.samples.iter())
                .map(|sample| sample.spw)
                .collect();
            Ok(int_array(windows))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (obsid = 0, arrayid = 0), text_signature = "($self, obsid=0, arrayid=0)")]
    fn spwsforscans(&self, py: Python<'_>, obsid: i64, arrayid: i64) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            index.check_scope(ms, obsid, arrayid, true)?;
            Ok(CasaValue::record(index.matching(obsid, arrayid).map(
                |(key, scan)| {
                    let windows: BTreeSet<_> =
                        scan.samples.iter().map(|sample| sample.spw).collect();
                    (key.number.to_string(), int_array(windows))
                },
            )))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (spw = -1, obsid = -1, arrayid = -1), text_signature = "($self, spw=-1, obsid=-1, arrayid=-1)")]
    fn scansforspw(
        &self,
        py: Python<'_>,
        spw: i64,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            let spw = checked_spw(ms, spw)?;
            index.check_scope(ms, obsid, arrayid, false)?;
            let scans: BTreeSet<_> = index
                .matching(obsid, arrayid)
                .filter(|(_, scan)| scan.samples.iter().any(|sample| sample.spw == spw))
                .map(|(key, _)| key.number)
                .collect();
            Ok(int_array(scans))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (obsid = 0, arrayid = 0), text_signature = "($self, obsid=0, arrayid=0)")]
    fn scansforspws(&self, py: Python<'_>, obsid: i64, arrayid: i64) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            index.check_scope(ms, obsid, arrayid, true)?;
            let count = ms.spectral_window()?.row_count();
            let mut scans = vec![BTreeSet::new(); count];
            for (key, scan) in index.matching(obsid, arrayid) {
                for sample in &scan.samples {
                    let set = scans.get_mut(sample.spw as usize).ok_or_else(|| {
                        MsError::InvalidInput(format!("Unknown spectral window ID {}", sample.spw))
                    })?;
                    set.insert(key.number);
                }
            }
            Ok(CasaValue::record(
                scans
                    .into_iter()
                    .enumerate()
                    .map(|(spw, scans)| (spw.to_string(), int_array(scans))),
            ))
        })?
        .into_python(py)
    }
}
