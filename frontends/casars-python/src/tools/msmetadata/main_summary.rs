// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::{MeasurementSet, MsError, MsResult};
use pyo3::prelude::*;

use super::PyMsMetadata;
use super::main_scalar_batches::main_scalar_batches;
use super::scan_index::{ScanKey, Time};
use super::scan_queries::int_array;
use crate::convert::value::CasaValue;

#[derive(Default)]
struct TimeRows {
    count: usize,
    data_descriptions: BTreeSet<i32>,
}

#[derive(Default)]
struct SubScan {
    times: BTreeMap<Time, TimeRows>,
    antennas: BTreeSet<i32>,
    data_descriptions: BTreeSet<i32>,
    states: BTreeSet<i32>,
    rows: usize,
}

impl SubScan {
    fn record(self) -> CasaValue {
        let begin = self.times.first_key_value().expect("nonempty subscan").0.0;
        let end = self.times.last_key_value().expect("nonempty subscan").0.0;
        let mut fields = vec![
            ("antennas".into(), int_array(self.antennas)),
            ("begin time".into(), CasaValue::Float(begin)),
            (
                "data description IDs".into(),
                int_array(self.data_descriptions),
            ),
            ("end time".into(), CasaValue::Float(end)),
            ("nrows".into(), CasaValue::Int(self.rows as i64)),
            ("state IDs".into(), int_array(self.states)),
        ];
        fields.extend(
            self.times
                .into_iter()
                .enumerate()
                .map(|(index, (time, rows))| {
                    (
                        index.to_string(),
                        CasaValue::record([
                            ("data description IDs", int_array(rows.data_descriptions)),
                            ("nrows", CasaValue::Int(rows.count as i64)),
                            ("time", CasaValue::Float(time.0)),
                        ]),
                    )
                }),
        );
        CasaValue::record(fields)
    }
}

#[derive(Default)]
struct Scan {
    fields: BTreeMap<i32, SubScan>,
}

impl Scan {
    fn record(self) -> CasaValue {
        let antennas = self
            .fields
            .values()
            .flat_map(|field| field.antennas.iter().copied())
            .collect::<BTreeSet<_>>();
        let rows = self.fields.values().map(|field| field.rows).sum::<usize>();
        let mut result = vec![
            ("antennas".into(), int_array(antennas)),
            ("nrows".into(), CasaValue::Int(rows as i64)),
        ];
        result.extend(
            self.fields
                .into_iter()
                .map(|(id, field)| (format!("fieldID={id}"), field.record())),
        );
        CasaValue::record(result)
    }
}

fn text_array(values: Vec<String>) -> CasaValue {
    CasaValue::TextArray {
        shape: vec![values.len()],
        values,
    }
}

fn summary(ms: &MeasurementSet) -> MsResult<CasaValue> {
    let windows = ms.spectral_window()?;
    let names = (0..windows.row_count())
        .map(|id| windows.name(id))
        .collect::<MsResult<Vec<_>>>()?;
    let fields = ms.field()?;
    let field_names = (0..fields.row_count())
        .map(|id| fields.name(id))
        .collect::<MsResult<Vec<_>>>()?;
    let pol = ms.polarization()?;
    let correlations = (0..pol.row_count())
        .map(|id| pol.num_corr(id))
        .collect::<MsResult<Vec<_>>>()?;
    let descriptions = ms.data_description()?;
    let windows_per_description = (0..descriptions.row_count())
        .map(|id| descriptions.spectral_window_id(id))
        .collect::<MsResult<Vec<_>>>()?;
    let pols = (0..descriptions.row_count())
        .map(|id| descriptions.polarization_id(id))
        .collect::<MsResult<Vec<_>>>()?;

    let mut scan_data = BTreeMap::<ScanKey, Scan>::new();
    let mut time_min = f64::INFINITY;
    let mut time_max = f64::NEG_INFINITY;
    main_scalar_batches(
        ms,
        [
            "OBSERVATION_ID",
            "ARRAY_ID",
            "SCAN_NUMBER",
            "FIELD_ID",
            "ANTENNA1",
            "ANTENNA2",
            "DATA_DESC_ID",
            "STATE_ID",
        ],
        ["TIME"],
        false,
        1 << 16,
        |rows, [observation, array, scan, field, a1, a2, ddid, state], [time], _| {
            for i in 0..rows.len() {
                let time = time[i];
                if !time.is_finite() {
                    return Err(MsError::InvalidInput("MAIN.TIME must be finite".into()));
                }
                time_min = time_min.min(time);
                time_max = time_max.max(time);
                let key = ScanKey {
                    observation: observation[i],
                    array: array[i],
                    number: scan[i],
                };
                let subscan = scan_data
                    .entry(key)
                    .or_default()
                    .fields
                    .entry(field[i])
                    .or_default();
                subscan.rows += 1;
                subscan.antennas.extend([a1[i], a2[i]]);
                subscan.data_descriptions.insert(ddid[i]);
                subscan.states.insert(state[i]);
                let timestamp = subscan.times.entry(Time(time)).or_default();
                timestamp.count += 1;
                timestamp.data_descriptions.insert(ddid[i]);
            }
            Ok(())
        },
    )?;

    if ms.row_count() == 0 {
        return Err(MsError::InvalidInput(
            "Cannot summarize a MeasurementSet with no MAIN rows".into(),
        ));
    }
    let mut by_observation =
        vec![BTreeMap::<i32, Vec<(String, CasaValue)>>::new(); ms.observation()?.row_count()];
    for (key, value) in scan_data {
        by_observation
            .get_mut(key.observation as usize)
            .ok_or_else(|| {
                MsError::InvalidInput(format!("Unknown observation ID {}", key.observation))
            })?
            .entry(key.array)
            .or_default()
            .push((format!("scan={}", key.number), value.record()));
    }

    let mut record = vec![
        (
            "spectral windows".into(),
            CasaValue::record([("names", text_array(names))]),
        ),
        (
            "polarizations".into(),
            CasaValue::record([("n correlations", int_array(correlations))]),
        ),
        (
            "data descriptions".into(),
            CasaValue::record([
                ("spectral windows", int_array(windows_per_description)),
                ("polarization ids", int_array(pols)),
            ]),
        ),
        ("fields".into(), text_array(field_names)),
        ("nrows".into(), CasaValue::Int(ms.row_count() as i64)),
        ("begin time".into(), CasaValue::Float(time_min)),
        ("end time".into(), CasaValue::Float(time_max)),
    ];
    record.extend(by_observation.into_iter().enumerate().map(|(id, arrays)| {
        (
            format!("observationID={id}"),
            CasaValue::record(
                arrays
                    .into_iter()
                    .map(|(array, scans)| (format!("arrayID={array}"), CasaValue::record(scans))),
            ),
        )
    }));
    Ok(CasaValue::record(record))
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn summary(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_ms(py, summary)?.into_python(py)
    }
}
