// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;

use casa_ms::{MeasurementSet, MsError, MsResult};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use regex::Regex;

use super::PyMsMetadata;
use super::names::selector_text;
use super::scan_index::Sample;
use super::scan_queries::{FieldSelector, fields_result, int_array, matching_scan};
use super::spw_membership::checked_spw;
use super::time_state_queries::times_array;

#[derive(Default)]
struct Intent(String);

impl<'a, 'py> FromPyObject<'a, 'py> for Intent {
    type Error = PyErr;

    fn extract(object: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        selector_text(&object)?
            .map(Self)
            .ok_or_else(|| PyTypeError::new_err("argument _intent must be a string"))
    }
}

struct StateIntents {
    by_state: Vec<Vec<String>>,
}

impl StateIntents {
    fn read(ms: &MeasurementSet) -> MsResult<Self> {
        let states = ms.state()?;
        let by_state = (0..states.row_count())
            .map(|row| {
                let mode = states.string(row, "OBS_MODE")?;
                if mode.is_empty() {
                    Ok(Vec::new())
                } else {
                    Ok(mode.split(',').map(str::to_owned).collect())
                }
            })
            .collect::<MsResult<Vec<_>>>()?;
        Ok(Self { by_state })
    }

    fn for_samples<'a>(&'a self, samples: impl Iterator<Item = &'a Sample>) -> Vec<String> {
        samples
            .flat_map(|sample| {
                self.by_state
                    .get(sample.state as usize)
                    .into_iter()
                    .flatten()
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect()
    }

    fn selected_states(&self, text: &str, expand: bool) -> MsResult<Vec<bool>> {
        let regex = if expand {
            wildcard_pattern(text)?
        } else {
            None
        };
        Ok(self
            .by_state
            .iter()
            .map(|intents| {
                intents.iter().any(|intent| {
                    regex
                        .as_ref()
                        .map_or_else(|| intent == text, |regex| regex.is_match(intent))
                })
            })
            .collect())
    }
}

pub(super) fn wildcard_pattern(text: &str) -> MsResult<Option<Regex>> {
    if !text.contains('*') {
        return Ok(None);
    }
    let pattern = text
        .split('*')
        .map(regex::escape)
        .collect::<Vec<_>>()
        .join(".*");
    Regex::new(&format!(r"\A(?:{pattern})\z"))
        .map(Some)
        .map_err(|error| MsError::InvalidInput(error.to_string()))
}

fn matches_state(selected: &[bool], state: i32) -> bool {
    selected.get(state as usize).copied().unwrap_or(false)
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn intents(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.with_ms(py, |ms| {
            Ok(StateIntents::read(ms)?
                .by_state
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect())
        })
    }

    #[pyo3(signature = (field = FieldSelector::Id(0)), text_signature = "($self, field=0)")]
    fn intentsforfield(&self, py: Python<'_>, field: FieldSelector) -> PyResult<Vec<String>> {
        self.with_scan_index(py, |ms, index| {
            let fields = ms.field()?;
            let mut ids = BTreeSet::new();
            match field {
                FieldSelector::Name(name) => {
                    for row in 0..fields.row_count() {
                        if fields.name(row)? == name {
                            ids.insert(row as i32);
                        }
                    }
                }
                FieldSelector::Id(id) if id >= 0 && id < fields.row_count() as i64 => {
                    ids.insert(id as i32);
                }
                FieldSelector::Id(id) => {
                    return Err(MsError::InvalidInput(format!(
                        "Field ID {id} is out of range"
                    )));
                }
                FieldSelector::Unsupported => {
                    return Err(MsError::InvalidInput(
                        "Field must be a nonnegative integer or string".into(),
                    ));
                }
            }
            let intents = StateIntents::read(ms)?;
            Ok(intents.for_samples(index.samples().filter(|sample| ids.contains(&sample.field))))
        })
    }

    #[pyo3(signature = (scan = -1, obsid = -1, arrayid = -1), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1)")]
    fn intentsforscan(
        &self,
        py: Python<'_>,
        scan: i64,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Vec<String>> {
        self.with_scan_index(py, |ms, index| {
            if scan < 0 {
                return Err(MsError::InvalidInput(
                    "Scan number must be nonnegative".into(),
                ));
            }
            let intents = StateIntents::read(ms)?;
            let samples =
                matching_scan(index, ms, scan, obsid, arrayid)?.flat_map(|scan| &scan.samples);
            Ok(intents.for_samples(samples))
        })
    }

    #[pyo3(signature = (spw = -1), text_signature = "($self, spw=-1)")]
    fn intentsforspw(&self, py: Python<'_>, spw: i64) -> PyResult<Vec<String>> {
        self.with_scan_index(py, |ms, index| {
            let spw = checked_spw(ms, spw)?;
            let intents = StateIntents::read(ms)?;
            Ok(intents.for_samples(index.samples().filter(|sample| sample.spw == spw)))
        })
    }

    #[pyo3(signature = (intent = Intent::default(), asnames = false), text_signature = "($self, intent='', asnames=False)")]
    fn fieldsforintent(
        &self,
        py: Python<'_>,
        intent: Intent,
        asnames: bool,
    ) -> PyResult<Py<PyAny>> {
        let intent = intent.0.as_str();
        self.with_scan_index(py, |ms, index| {
            let intents = StateIntents::read(ms)?;
            if intent == "*" && intents.by_state.iter().all(Vec::is_empty) {
                return fields_result(ms, (0..ms.field()?.row_count() as i32).collect(), asnames);
            }
            let states = intents.selected_states(intent, true)?;
            let fields = index
                .samples()
                .filter(|sample| matches_state(&states, sample.state))
                .map(|sample| sample.field)
                .collect();
            fields_result(ms, fields, asnames)
        })?
        .into_python(py)
    }

    #[pyo3(signature = (intent = Intent::default(), obsid = -1, arrayid = -1), text_signature = "($self, intent='', obsid=-1, arrayid=-1)")]
    fn scansforintent(
        &self,
        py: Python<'_>,
        intent: Intent,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        let intent = intent.0.as_str();
        self.with_scan_index(py, |ms, index| {
            index.check_scope(ms, obsid, arrayid, false)?;
            let states = StateIntents::read(ms)?.selected_states(intent, true)?;
            if !intent.contains('*') && !states.iter().any(|&selected| selected) {
                return Err(MsError::InvalidInput(format!(
                    "Intent {intent} is not present in this dataset"
                )));
            }
            let scans: BTreeSet<_> = index
                .matching(obsid, arrayid)
                .filter(|(_, scan)| {
                    scan.samples
                        .iter()
                        .any(|sample| matches_state(&states, sample.state))
                })
                .map(|(key, _)| key.number)
                .collect();
            Ok(int_array(scans))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (intent = Intent::default()), text_signature = "($self, intent='')")]
    fn spwsforintent(&self, py: Python<'_>, intent: Intent) -> PyResult<Py<PyAny>> {
        let intent = intent.0.as_str();
        self.with_scan_index(py, |ms, index| {
            let states = StateIntents::read(ms)?.selected_states(intent, true)?;
            let windows: BTreeSet<_> = index
                .samples()
                .filter(|sample| matches_state(&states, sample.state))
                .map(|sample| sample.spw)
                .collect();
            Ok(int_array(windows))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (intent = Intent::default()), text_signature = "($self, intent='')")]
    fn timesforintent(&self, py: Python<'_>, intent: Intent) -> PyResult<Py<PyAny>> {
        let intent = intent.0.as_str();
        self.with_scan_index(py, |ms, index| {
            let states = StateIntents::read(ms)?.selected_states(intent, false)?;
            let times = index
                .samples()
                .filter(|sample| matches_state(&states, sample.state))
                .map(|sample| sample.time)
                .collect();
            Ok(times_array(times))
        })?
        .into_python(py)
    }
}
