// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::{MeasurementSet, MsError, MsObservation, MsResult, open_measures_runtime};
use casa_tables::table_measures::{MeasRefDesc, TableMeasDesc};
use casa_types::ArrayValue;
use casa_types::measures::{EpochRef, MEpoch, MPosition};
use pyo3::prelude::*;

use super::PyMsMetadata;
use super::names::checked_row;
use crate::convert::holder::{CasaMeasure, Measure};
use crate::convert::value::CasaValue;

pub(super) fn observatory_position(ms: &MeasurementSet, which: i64) -> MsResult<MPosition> {
    let observation = ms.observation()?;
    let row = checked_row(which, observation.row_count(), "Observation ID")?;
    let measures = open_measures_runtime()?;
    let mut position = None;
    // CASA validates every OBSERVATION telescope before returning a position.
    for index in 0..observation.row_count() {
        let name = observation.string(index, "TELESCOPE_NAME")?;
        if name.is_empty() {
            return Err(MsError::InvalidInput(
                "The name of the telescope is not stored in the measurement set".into(),
            ));
        }
        let current = MPosition::casa_observatory(&name, measures.as_ref()).ok_or_else(|| {
            MsError::InvalidInput(format!("Telescope {name} is not recognized by CASA"))
        })?;
        if index == row {
            position = Some(current);
        }
    }
    position.ok_or_else(|| MsError::InvalidInput("Observation ID out of range".into()))
}

fn time_range(observation: &MsObservation<'_>, row: usize) -> MsResult<CasaValue> {
    let table = observation.table();
    let description = TableMeasDesc::reconstruct(table, "TIME_RANGE").ok_or_else(|| {
        MsError::InvalidInput("OBSERVATION.TIME_RANGE has no epoch descriptor".into())
    })?;
    let MeasRefDesc::Fixed { refer } = description.ref_desc() else {
        return Err(MsError::InvalidInput(
            "OBSERVATION.TIME_RANGE requires a fixed epoch reference".into(),
        ));
    };
    let refer = refer.parse::<EpochRef>()?;
    let ArrayValue::Float64(range) = observation.array(row, "TIME_RANGE")? else {
        return Err(MsError::InvalidInput(
            "OBSERVATION.TIME_RANGE must be a floating array".into(),
        ));
    };
    let [begin, end] = range.as_slice().unwrap_or_default() else {
        return Err(MsError::InvalidInput(
            "OBSERVATION.TIME_RANGE requires two values".into(),
        ));
    };
    let measure = |seconds| {
        Measure::from_casa(&CasaMeasure::Epoch(MEpoch::from_mjd_seconds(
            seconds, refer,
        )))
        .to_value()
    };
    Ok(CasaValue::record([
        ("begin", measure(*begin)),
        ("end", measure(*end)),
    ]))
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (obsid = -1), text_signature = "($self, obsid=-1)")]
    fn schedule(&self, py: Python<'_>, obsid: i64) -> PyResult<Vec<String>> {
        self.with_ms(py, |ms| {
            let observation = ms.observation()?;
            let row = checked_row(obsid, observation.row_count(), "Observation ID")?;
            let ArrayValue::String(schedule) = observation.array(row, "SCHEDULE")? else {
                return Err(MsError::InvalidInput(
                    "OBSERVATION.SCHEDULE must be a string array".into(),
                ));
            };
            Ok(schedule.iter().cloned().collect())
        })
    }

    #[pyo3(signature = (obsid = -1), text_signature = "($self, obsid=-1)")]
    fn timerangeforobs(&self, py: Python<'_>, obsid: i64) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let observation = ms.observation()?;
            time_range(
                &observation,
                checked_row(obsid, observation.row_count(), "Observation ID")?,
            )
        })?
        .into_python(py)
    }

    #[pyo3(signature = (which = 0), text_signature = "($self, which=0)")]
    fn observatoryposition(&self, py: Python<'_>, which: i64) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            Ok(
                Measure::from_casa(&CasaMeasure::Position(observatory_position(ms, which)?))
                    .to_value(),
            )
        })?
        .into_python(py)
    }
}
