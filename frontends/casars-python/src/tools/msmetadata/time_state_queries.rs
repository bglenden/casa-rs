// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;

use casa_ms::{MsError, MsResult};
use pyo3::prelude::*;

use super::PyMsMetadata;
use super::names::Indices;
use super::scan_index::Time;
use super::scan_queries::{int_array, matching_scan};
use crate::convert::value::CasaValue;

enum TimeSelector {
    Indices(Indices),
    UnsignedScalar(Indices),
    BooleanScalar,
}

impl<'a, 'py> FromPyObject<'a, 'py> for TimeSelector {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let numpy = value.py().import("numpy")?;
        if value.is_instance(&numpy.getattr("bool_")?)? {
            return Ok(Self::BooleanScalar);
        }
        let indices = Indices::capture(Some(&value))?;
        if value.is_instance(&numpy.getattr("unsignedinteger")?)? {
            Ok(Self::UnsignedScalar(indices))
        } else {
            Ok(Self::Indices(indices))
        }
    }
}

fn interval(center: f64, tolerance: f64) -> MsResult<RangeInclusive<f64>> {
    if tolerance < 0.0 {
        return Err(MsError::InvalidInput(
            "Tolerance cannot be less than zero".into(),
        ));
    }
    Ok(center - tolerance..=center + tolerance)
}

pub(super) fn times_array(times: BTreeSet<Time>) -> CasaValue {
    CasaValue::floats(times.into_iter().map(|time| time.0).collect())
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (time = -1.0, tol = 0.0), text_signature = "($self, time=-1.0, tol=0.0)")]
    fn fieldsfortimes(&self, py: Python<'_>, time: f64, tol: f64) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |_, index| {
            let range = interval(time, tol)?;
            let fields: BTreeSet<_> = index
                .samples()
                .filter(|sample| range.contains(&sample.time.0))
                .map(|sample| sample.field)
                .collect();
            Ok(int_array(fields))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (time = -1.0, tol = 0.0, obsid = -1, arrayid = -1), text_signature = "($self, time=-1.0, tol=0.0, obsid=-1, arrayid=-1)")]
    fn scansfortimes(
        &self,
        py: Python<'_>,
        time: f64,
        tol: f64,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            index.check_scope(ms, obsid, arrayid, false)?;
            let range = interval(time, tol)?;
            let scans: BTreeSet<_> = index
                .matching(obsid, arrayid)
                .filter(|(_, scan)| {
                    let start = scan
                        .samples
                        .first()
                        .expect("a scan has observations")
                        .time
                        .0;
                    let end = scan.samples.last().expect("a scan has observations").time.0;
                    end >= *range.start() && start <= *range.end()
                })
                .map(|(key, _)| key.number)
                .collect();
            Ok(int_array(scans))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (field = -1), text_signature = "($self, field=-1)")]
    fn timesforfield(&self, py: Python<'_>, field: i64) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            if field < 0 || field >= ms.field()?.row_count() as i64 {
                return Err(MsError::InvalidInput(format!(
                    "Field ID {field} is out of range"
                )));
            }
            let times = index
                .samples()
                .filter(|sample| i64::from(sample.field) == field)
                .map(|sample| sample.time)
                .collect();
            Ok(times_array(times))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (scan = -1, obsid = -1, arrayid = -1, perspw = false), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1, perspw=False)")]
    fn timesforscan(
        &self,
        py: Python<'_>,
        scan: i64,
        obsid: i64,
        arrayid: i64,
        perspw: bool,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            if scan < 0 {
                return Err(MsError::InvalidInput(
                    "Scan number must be nonnegative".into(),
                ));
            }
            let mut samples =
                matching_scan(index, ms, scan, obsid, arrayid)?.flat_map(|scan| &scan.samples);
            if perspw {
                let mut times = BTreeMap::<i32, BTreeSet<Time>>::new();
                for sample in &mut samples {
                    times.entry(sample.spw).or_default().insert(sample.time);
                }
                Ok(CasaValue::record(
                    times
                        .into_iter()
                        .map(|(spw, times)| (spw.to_string(), times_array(times))),
                ))
            } else {
                Ok(times_array(samples.map(|sample| sample.time).collect()))
            }
        })?
        .into_python(py)
    }

    #[pyo3(signature = (scans = TimeSelector::Indices(Indices::Scalar(0)), obsid = -1, arrayid = -1), text_signature = "($self, scans=0, obsid=-1, arrayid=-1)")]
    fn timesforscans(
        &self,
        py: Python<'_>,
        scans: TimeSelector,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            let scans = match scans {
                TimeSelector::Indices(Indices::Scalar(id)) => vec![id],
                TimeSelector::Indices(Indices::Vector(ids)) => ids,
                _ => {
                    return Err(MsError::InvalidInput(
                        "scans must either be an int or an array of ints".into(),
                    ));
                }
            };
            let requested: BTreeSet<_> = scans.into_iter().collect();
            if requested.first().is_some_and(|&value| value < 0) {
                return Err(MsError::InvalidInput(
                    "All scan numbers must be nonnegative".into(),
                ));
            }
            index.check_scope(ms, obsid, arrayid, false)?;
            let matched: Vec<_> = index
                .matching(obsid, arrayid)
                .filter(|(key, _)| {
                    requested.is_empty() || requested.contains(&i64::from(key.number))
                })
                .map(|(_, scan)| scan)
                .collect();
            if matched.is_empty()
                || (obsid >= 0
                    && arrayid >= 0
                    && !requested.is_empty()
                    && matched.len() != requested.len())
            {
                return Err(MsError::InvalidInput("No matching scans found".into()));
            }
            let times = matched
                .into_iter()
                .flat_map(|scan| &scan.samples)
                .map(|sample| sample.time)
                .collect();
            Ok(times_array(times))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (spw = TimeSelector::Indices(Indices::Scalar(-1))), text_signature = "($self, spw=-1)")]
    fn timesforspws(&self, py: Python<'_>, spw: TimeSelector) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            let count = ms.spectral_window()?.row_count();
            let ids = match spw {
                TimeSelector::Indices(Indices::Scalar(id)) if id < 0 => Indices::Empty,
                TimeSelector::Indices(Indices::BooleanVector) | TimeSelector::BooleanScalar => {
                    Indices::Empty
                }
                TimeSelector::Indices(selection) | TimeSelector::UnsignedScalar(selection) => {
                    selection
                }
            }
            .rows(count, "Spectral window ID")?;
            let collapsed = ids.len() == 1;
            let mut times: BTreeMap<_, BTreeSet<Time>> = ids
                .into_iter()
                .map(|id| (id as i32, BTreeSet::new()))
                .collect();
            for sample in index.samples() {
                if let Some(values) = times.get_mut(&sample.spw) {
                    values.insert(sample.time);
                }
            }
            if collapsed {
                return Ok(times_array(
                    times.pop_first().expect("one selected window").1,
                ));
            }
            Ok(CasaValue::record(
                times
                    .into_iter()
                    .map(|(spw, times)| (spw.to_string(), times_array(times))),
            ))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (state = -1, obsid = -1, arrayid = -1), text_signature = "($self, state=-1, obsid=-1, arrayid=-1)")]
    fn scansforstate(
        &self,
        py: Python<'_>,
        state: i64,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            if state < 0 {
                return Err(MsError::InvalidInput("State must be nonnegative".into()));
            }
            index.check_scope(ms, obsid, arrayid, false)?;
            if state >= ms.state()?.row_count().max(1) as i64 {
                return Err(MsError::InvalidInput(format!(
                    "State ID {state} is out of range"
                )));
            }
            let scans: BTreeSet<_> = index
                .matching(obsid, arrayid)
                .filter(|(_, scan)| {
                    scan.samples
                        .iter()
                        .any(|sample| i64::from(sample.state) == state)
                })
                .map(|(key, _)| key.number)
                .collect();
            Ok(int_array(scans))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (scan = -1, obsid = -1, arrayid = -1), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1)")]
    fn statesforscan(
        &self,
        py: Python<'_>,
        scan: i64,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            if scan < 0 {
                return Err(MsError::InvalidInput(
                    "Scan number must be nonnegative".into(),
                ));
            }
            index.check_scope(ms, obsid, arrayid, false)?;
            let states: BTreeSet<_> = index
                .matching(obsid, arrayid)
                .filter(|(key, _)| i64::from(key.number) == scan)
                .flat_map(|(_, scan)| &scan.samples)
                .map(|sample| sample.state)
                .collect();
            Ok(int_array(states))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (obsid = 0, arrayid = 0), text_signature = "($self, obsid=0, arrayid=0)")]
    fn statesforscans(&self, py: Python<'_>, obsid: i64, arrayid: i64) -> PyResult<Py<PyAny>> {
        self.with_scan_index(py, |ms, index| {
            index.check_scope(ms, obsid, arrayid, false)?;
            let mut states = BTreeMap::new();
            for (key, scan) in index.matching(obsid, arrayid) {
                states.entry(key.number.to_string()).or_insert_with(|| {
                    let ids: BTreeSet<_> = scan.samples.iter().map(|sample| sample.state).collect();
                    int_array(ids)
                });
            }
            Ok(CasaValue::record(states))
        })?
        .into_python(py)
    }
}
