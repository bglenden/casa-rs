// SPDX-License-Identifier: LGPL-3.0-or-later
//! Position-switch sky spectra from autocorrelation OFF integrations.

use std::collections::BTreeMap;
use std::path::Path;

use casa_core::ArrayValue;
use casa_ms::MeasurementSet;
use casa_tables::RequiredScalarColumnValues;

use super::spectrum::{self, ReferenceTimestamp, SkySpectrum};
use crate::CalibrationOutputMode;
use crate::constants::*;

pub(super) const SUBTYPE: &str = "SDSKY_PS";

/// CASA groups position-switch reference rows by these columns even when
/// there is a gap between integrations belonging to the same group.
#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct ReferenceGroup {
    observation: i32,
    processor: i32,
    field: i32,
    antenna: i32,
    feed: i32,
    data_description: i32,
    scan: i32,
    state: i32,
}

/// Solve and write position-switch sky spectra (`SDSKY_PS`) from OFF integrations.
pub fn generate(
    ms: &MeasurementSet,
    rows: &[usize],
    destination: &Path,
    mode: CalibrationOutputMode,
) -> Result<usize, String> {
    let solutions = solve(ms, rows)?;
    spectrum::write(
        ms,
        destination,
        mode,
        SUBTYPE,
        ReferenceTimestamp::UnflaggedCells,
        solutions
            .into_iter()
            .map(|(group, spectrum)| (group.antenna, spectrum)),
    )
}

fn solve(
    ms: &MeasurementSet,
    rows: &[usize],
) -> Result<BTreeMap<ReferenceGroup, SkySpectrum>, String> {
    let main = ms.main_table();
    let columns = main
        .required_scalar_columns_owned_for_rows(
            &[
                COL_OBSERVATION_ID,
                "PROCESSOR_ID",
                COL_FIELD_ID,
                COL_ANTENNA1,
                COL_ANTENNA2,
                "FEED1",
                "FEED2",
                "DATA_DESC_ID",
                COL_SCAN_NUMBER,
                "STATE_ID",
                COL_TIME,
                "EXPOSURE",
            ],
            rows,
        )
        .map_err(error_text)?;
    let ids = |column: &str| match &columns[column] {
        RequiredScalarColumnValues::Int32(values) => Ok(values.as_slice()),
        _ => Err(format!("MeasurementSet {column} is not Int32.")),
    };
    let [
        observation,
        processor,
        field,
        antenna,
        other_antenna,
        feed,
        other_feed,
        description,
        scan,
        state,
    ] = [
        ids(COL_OBSERVATION_ID)?,
        ids("PROCESSOR_ID")?,
        ids(COL_FIELD_ID)?,
        ids(COL_ANTENNA1)?,
        ids(COL_ANTENNA2)?,
        ids("FEED1")?,
        ids("FEED2")?,
        ids("DATA_DESC_ID")?,
        ids(COL_SCAN_NUMBER)?,
        ids("STATE_ID")?,
    ];
    let (
        RequiredScalarColumnValues::Float64(times),
        RequiredScalarColumnValues::Float64(exposures),
    ) = (&columns[COL_TIME], &columns["EXPOSURE"])
    else {
        return Err("MeasurementSet TIME or EXPOSURE is not Float64.".into());
    };
    let (state_table, observations, descriptions, spectral) = (
        ms.state().map_err(error_text)?,
        ms.observation().map_err(error_text)?,
        ms.data_description().map_err(error_text)?,
        ms.spectral_window().map_err(error_text)?,
    );
    let off_state = (0..state_table.row_count())
        .map(|row| {
            state_table.string(row, "OBS_MODE").map(|mode| {
                mode.to_ascii_uppercase()
                    .starts_with("OBSERVE_TARGET#OFF_SOURCE")
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let alma = (0..observations.row_count())
        .map(|row| {
            observations
                .string(row, "TELESCOPE_NAME")
                .map(|name| name == "ALMA")
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let windows = (0..descriptions.row_count())
        .map(|row| descriptions.spectral_window_id(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let wvr = (0..spectral.row_count())
        .map(|row| spectral.num_chan(row).map(|channels| channels == 4))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let data_column = if main
        .schema()
        .is_some_and(|schema| schema.contains_column("FLOAT_DATA"))
    {
        "FLOAT_DATA"
    } else {
        "DATA"
    };
    let mut solutions = BTreeMap::new();
    for (index, &row) in rows.iter().enumerate() {
        if antenna[index] != other_antenna[index]
            || feed[index] != other_feed[index]
            || off_state.get(state[index] as usize) != Some(&true)
        {
            continue;
        }
        let spw = *windows
            .get(description[index] as usize)
            .ok_or("Invalid DATA_DESC_ID in reference data.")? as usize;
        let water_vapour = *wvr.get(spw).ok_or("Invalid reference spectral window.")?;
        if water_vapour && alma.get(observation[index] as usize) == Some(&true) {
            continue;
        }
        let group = ReferenceGroup {
            observation: observation[index],
            processor: processor[index],
            field: field[index],
            antenna: antenna[index],
            feed: feed[index],
            data_description: description[index],
            scan: scan[index],
            state: state[index],
        };
        let power = spectrum::read_power(main, row, data_column)?;
        let ArrayValue::Bool(flags) = main
            .cell_accessor(row, "FLAG")
            .and_then(|cell| cell.array())
            .map_err(error_text)?
        else {
            return Err("MeasurementSet FLAG is not Boolean.".into());
        };
        let time = times[index];
        let exposure = exposures[index];
        if !time.is_finite() || !exposure.is_finite() {
            return Err("Invalid reference integration time or exposure.".into());
        }
        solutions
            .entry(group)
            .or_insert_with(|| SkySpectrum::new(spw, &power))
            .add(&power, flags, time, exposure)?;
    }
    if solutions.is_empty() {
        return Err("No reference integration found in user-selected data. Please double-check your data selection criteria.".into());
    }
    Ok(solutions)
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
