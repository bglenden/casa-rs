// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::{MeasurementSet, MsError, MsResult};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyComplex, PyComplexMethods, PyFloat, PyInt, PyList, PyString};

use super::PyMsMetadata;
use super::names::selector_text;
use super::scan_index::{Scan, ScanIndex, ScanKey};
use crate::convert::{nested_sequence_items, value::CasaValue};

struct ScanList(Vec<i64>);

fn scan_number_text(text: &str) -> PyResult<i64> {
    let text = text.trim_start();
    let sign = usize::from(text.starts_with(['+', '-']));
    let digits = text[sign..].bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return Ok(0);
    }
    text[..sign + digits]
        .parse()
        .map_err(|_| PySystemError::new_err("scan number is out of range"))
}

impl<'a, 'py> FromPyObject<'a, 'py> for ScanList {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let invalid = || PySystemError::new_err("scans must be a number, list or NumPy array");
        let number = |value: &Bound<'py, PyAny>| {
            if let Ok(text) = value.cast::<PyString>() {
                return scan_number_text(text.to_str()?);
            }
            if let Ok(bytes) = value.cast::<PyBytes>() {
                return scan_number_text(&String::from_utf8_lossy(bytes.as_bytes()));
            }
            if let Ok(value) = value.extract::<i64>() {
                return Ok(value);
            }
            let value = if let Ok(complex) = value.cast::<PyComplex>() {
                complex.real()
            } else {
                value.extract::<f64>().map_err(|_| invalid())?
            };
            if value.is_finite() && value >= i64::MIN as f64 && value < -(i64::MIN as f64) {
                Ok(value as i64)
            } else {
                Err(invalid())
            }
        };
        if object.is_instance_of::<PyString>() {
            return Err(PyTypeError::new_err("argument _scans must not be a string"));
        }
        if object.is_instance_of::<PyInt>() || object.is_instance_of::<PyFloat>() {
            return Ok(Self(vec![number(&object)?]));
        }
        let items = if let Ok(array) = object.cast::<PyUntypedArray>() {
            let dtype = array.dtype();
            if !matches!(dtype.kind(), b'b' | b'i' | b'u' | b'f' | b'c' | b'S' | b'U')
                || (dtype.kind() == b'f' && dtype.itemsize() == 2)
            {
                return Err(PyTypeError::new_err(
                    "unsupported NumPy scan-selector dtype",
                ));
            }
            array
                .call_method1("ravel", ("F",))?
                .call_method0("tolist")?
                .try_iter()?
                .collect::<PyResult<Vec<_>>>()?
        } else if object.is_instance_of::<PyList>() {
            if let Some((_, items)) = nested_sequence_items(&object)? {
                items
            } else {
                object.try_iter()?.collect::<PyResult<Vec<_>>>()?
            }
        } else {
            return Err(invalid());
        };
        items
            .iter()
            .map(number)
            .collect::<PyResult<Vec<_>>>()
            .map(Self)
    }
}

pub(super) enum FieldSelector {
    Name(String),
    Id(i64),
    Unsupported,
}

impl<'a, 'py> FromPyObject<'a, 'py> for FieldSelector {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Some(name) = selector_text(&object)? {
            return Ok(Self::Name(name));
        }
        let signed = if object.is_instance_of::<PyInt>() {
            !object.is_instance_of::<PyBool>()
        } else {
            object.is_instance(&object.py().import("numpy")?.getattr("signedinteger")?)?
        };
        if signed && let Ok(id) = object.extract::<i64>() {
            return Ok(Self::Id(id));
        }
        Ok(Self::Unsupported)
    }
}

pub(super) fn matching_scan<'a>(
    index: &'a ScanIndex,
    ms: &MeasurementSet,
    scan: i64,
    obsid: i64,
    arrayid: i64,
) -> MsResult<impl Iterator<Item = &'a Scan>> {
    index.check_scope(ms, obsid, arrayid, false)?;
    let matches = index
        .matching(obsid, arrayid)
        .filter(move |(key, _)| i64::from(key.number) == scan);
    let mut scans = matches.map(|(_, scan)| scan).peekable();
    if scans.peek().is_none() {
        return Err(MsError::InvalidInput("No matching scans found".into()));
    }
    Ok(scans)
}

fn field_ids(ms: &MeasurementSet, name: &str) -> MsResult<BTreeSet<i32>> {
    let fields = ms.field()?;
    let mut ids = BTreeSet::new();
    for row in 0..fields.row_count() {
        if fields.name(row)?.eq_ignore_ascii_case(name) {
            ids.insert(row as i32);
        }
    }
    if ids.is_empty() {
        return Err(MsError::InvalidInput(format!("Unknown field name {name}")));
    }
    Ok(ids)
}

pub(super) fn int_array(ids: impl IntoIterator<Item = i32>) -> CasaValue {
    let values: Vec<i64> = ids.into_iter().map(i64::from).collect();
    CasaValue::Ints {
        shape: vec![values.len()],
        values,
    }
}

fn fields_result(ms: &MeasurementSet, ids: BTreeSet<i32>, asnames: bool) -> MsResult<CasaValue> {
    if asnames {
        let fields = ms.field()?;
        let values = ids
            .into_iter()
            .map(|id| fields.name(id as usize))
            .collect::<MsResult<Vec<_>>>()?;
        Ok(CasaValue::TextArray {
            shape: vec![values.len()],
            values,
        })
    } else {
        Ok(int_array(ids))
    }
}

fn field_selections(
    ms: &MeasurementSet,
    index: &ScanIndex,
    scans: Vec<i64>,
    obsid: i64,
    arrayid: i64,
    asnames: bool,
    asmap: bool,
) -> MsResult<CasaValue> {
    let scan_numbers = scans.into_iter().collect::<BTreeSet<_>>();
    if scan_numbers.first().is_some_and(|&scan| scan < 0) {
        return Err(MsError::InvalidInput(
            "All scan numbers must be nonnegative".into(),
        ));
    }
    index.check_scope(ms, obsid, arrayid, asmap)?;
    let scan_matches =
        |key: &ScanKey| scan_numbers.is_empty() || scan_numbers.contains(&i64::from(key.number));
    let scoped = index.matching(obsid, arrayid);
    if asmap {
        let mut present = false;
        let mut entries = Vec::new();
        for (key, scan) in scoped {
            present = true;
            if scan_matches(key) {
                entries.push((
                    key.number.to_string(),
                    fields_result(ms, scan.fields.clone(), asnames)?,
                ));
            }
        }
        if !present {
            return Err(MsError::InvalidInput(
                "MS does not contain requested array and observation".into(),
            ));
        }
        return Ok(CasaValue::record(entries));
    }
    // The supplied observation and array select the real MAIN keys. CASA's
    // vector binding leaves these key fields uninitialized for this form.
    let matched: Vec<_> = scoped.filter(|(key, _)| scan_matches(key)).collect();
    if matched.is_empty()
        || (obsid >= 0
            && arrayid >= 0
            && !scan_numbers.is_empty()
            && matched.len() != scan_numbers.len())
    {
        return Err(MsError::InvalidInput("No matching scans found".into()));
    }
    let ids = matched
        .into_iter()
        .flat_map(|(_, scan)| scan.fields.iter().copied())
        .collect();
    fields_result(ms, ids, asnames)
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn narrays(&self, py: Python<'_>) -> PyResult<usize> {
        self.with_scan_index(py, |_, index| Ok(index.arrays))
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn nscans(&self, py: Python<'_>) -> PyResult<usize> {
        self.with_scan_index(py, |_, index| Ok(index.scans.len()))
    }

    #[pyo3(signature = (obsid = -1, arrayid = -1), text_signature = "($self, obsid=-1, arrayid=-1)")]
    fn scannumbers(&self, py: Python<'_>, obsid: i64, arrayid: i64) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            index.check_scope(ms, obsid, arrayid, false)?;
            let scans = index
                .matching(obsid, arrayid)
                .map(|(key, _)| key.number)
                .collect::<BTreeSet<_>>();
            Ok(int_array(scans))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (name = ""), text_signature = "($self, name='')")]
    fn fieldsforname(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let ids = if name.is_empty() {
            self.with_scan_index(py, |_, index| {
                let fields = index
                    .scans
                    .values()
                    .flat_map(|scan| scan.fields.iter().copied())
                    .collect::<BTreeSet<_>>();
                Ok(fields)
            })?
        } else {
            self.with_ms(py, |ms| field_ids(ms, name))?
        };
        int_array(ids).into_python(py)
    }

    #[pyo3(signature = (scan = -1, asnames = false, obsid = -1, arrayid = -1), text_signature = "($self, scan=-1, asnames=False, obsid=-1, arrayid=-1)")]
    fn fieldsforscan(
        &self,
        py: Python<'_>,
        scan: i64,
        asnames: bool,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            if scan < 0 {
                return Err(MsError::InvalidInput(
                    "Scan number must be nonnegative".into(),
                ));
            }
            let fields = matching_scan(index, ms, scan, obsid, arrayid)?
                .flat_map(|scan| scan.fields.iter().copied())
                .collect();
            fields_result(ms, fields, asnames)
        })?
        .into_python(py)
    }

    #[pyo3(signature = (scans = ScanList(Vec::new()), asnames = false, obsid = -1, arrayid = -1, asmap = false), text_signature = "($self, scans=[], asnames=False, obsid=-1, arrayid=-1, asmap=False)")]
    fn fieldsforscans(
        &self,
        py: Python<'_>,
        scans: ScanList,
        asnames: bool,
        obsid: i64,
        arrayid: i64,
        asmap: bool,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            field_selections(ms, index, scans.0, obsid, arrayid, asnames, asmap)
        })?
        .into_python(py)
    }

    #[pyo3(signature = (field = FieldSelector::Name(String::new()), obsid = -1, arrayid = -1), text_signature = "($self, field='', obsid=-1, arrayid=-1)")]
    fn scansforfield(
        &self,
        py: Python<'_>,
        field: FieldSelector,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            index.check_scope(ms, obsid, arrayid, false)?;
            let fields = match field {
                FieldSelector::Name(name) => field_ids(ms, &name)?,
                FieldSelector::Id(id) => {
                    if id >= ms.field()?.row_count() as i64 {
                        return Err(MsError::InvalidInput(format!(
                            "Field ID {id} is out of range"
                        )));
                    }
                    if id < 0 {
                        return Ok(int_array([]));
                    }
                    BTreeSet::from([id as i32])
                }
                FieldSelector::Unsupported => {
                    return Err(MsError::InvalidInput(
                        "Unsupported type for field parameter".into(),
                    ));
                }
            };
            let numbers = index
                .matching(obsid, arrayid)
                .filter(|(_, scan)| !scan.fields.is_disjoint(&fields))
                .map(|(key, _)| key.number)
                .collect::<BTreeSet<_>>();
            Ok(int_array(numbers))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (obsid = 0, arrayid = 0), text_signature = "($self, obsid=0, arrayid=0)")]
    fn scansforfields(&self, py: Python<'_>, obsid: i64, arrayid: i64) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            index.check_scope(ms, obsid, arrayid, true)?;
            let count = ms.field()?.row_count();
            let mut numbers = BTreeMap::<usize, BTreeSet<i32>>::new();
            for (key, scan) in index.matching(obsid, arrayid) {
                for &field in &scan.fields {
                    numbers
                        .entry(field as usize)
                        .or_default()
                        .insert(key.number);
                }
            }
            Ok(CasaValue::record((0..count).map(|id| {
                (
                    id.to_string(),
                    int_array(numbers.remove(&id).unwrap_or_default()),
                )
            })))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (scan = -1, obsid = -1, arrayid = -1), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1)")]
    fn antennasforscan(
        &self,
        py: Python<'_>,
        scan: i64,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            let ids = matching_scan(index, ms, scan, obsid, arrayid)?
                .flat_map(|scan| scan.antennas.iter().copied())
                .collect::<BTreeSet<_>>();
            Ok(int_array(ids))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (ac = false), text_signature = "($self, ac=False)")]
    fn nbaselines(&self, py: Python<'_>, ac: bool) -> PyResult<usize> {
        self.with_scan_index(py, |_, index| {
            Ok(index
                .baselines
                .iter()
                .filter(|[first, second]| ac || first != second)
                .count())
        })
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn baselines(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            let count = ms.antenna()?.row_count();
            let len = count
                .checked_mul(count)
                .ok_or_else(|| MsError::InvalidInput("Baseline matrix is too large".into()))?;
            let mut values = vec![false; len];
            for &[first, second] in &index.baselines {
                let (first, second) = (first as usize, second as usize);
                if first >= count || second >= count {
                    return Err(MsError::InvalidInput(
                        "Baseline has an invalid antenna ID".into(),
                    ));
                }
                values[first * count + second] = true;
                values[second * count + first] = true;
            }
            Ok(CasaValue::Bools {
                shape: vec![count, count],
                values,
            })
        })?
        .into_python(py)
    }
}
