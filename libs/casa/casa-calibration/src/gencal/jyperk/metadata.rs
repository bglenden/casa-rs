// SPDX-License-Identifier: LGPL-3.0-or-later
//! MeasurementSet metadata used by ALMA's interpolation and model-fit endpoints.

use std::collections::{BTreeMap, BTreeSet};

use casa_core::measures::{
    epoch::{EpochRef, MEpoch},
    frame::MeasFrame,
};
use casa_core::quanta::{
    Quantity,
    format::{FITS, format_time},
};
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::{MeasurementSet, MsError, MsResult, subtables::SubTable};
use casa_tables::RequiredScalarColumnValues::{Float64, Int32};

pub(super) struct QueryMetadata {
    pub(super) date: String,
    pub(super) temperature: String,
    pub(super) antennas: Vec<Antenna>,
    pub(super) windows: Vec<Window>,
}

pub(super) struct Antenna {
    pub(super) name: String,
    pub(super) elevation: f64,
}

pub(super) struct Window {
    pub(super) id: usize,
    pub(super) band: i32,
    pub(super) baseband: i32,
    pub(super) frequency: f64,
}

pub(super) fn number(value: f64) -> String {
    if value.is_nan() {
        "nan".into()
    } else {
        format!("{value:?}")
    }
}

pub(super) fn select_windows(ms: &MeasurementSet, selector: &str) -> MsResult<Vec<usize>> {
    let selector = if selector.is_empty() { "*" } else { selector };
    Ok(ms.resolve_spw_selector(selector)?.spw_ids().to_vec())
}

impl QueryMetadata {
    pub(super) fn load(ms: &MeasurementSet, selected: &[usize], selector: &str) -> MsResult<Self> {
        let spw = ms.spectral_window()?;
        let active = select_windows(ms, "*")?;
        let names = (0..spw.row_count())
            .map(|row| spw.name(row))
            .collect::<MsResult<Vec<_>>>()?;
        let windows = if selector.is_empty() || selector == "*" {
            selected
                .iter()
                .copied()
                .filter(|&id| !names[id].starts_with("WVR"))
                .collect::<Vec<_>>()
        } else {
            selected.to_vec()
        };
        let utc = observation_start(ms)?;
        let temperature = temperature(ms)?;
        let windows = windows
            .iter()
            .map(|&id| {
                let frequency = mean_frequency(&spw, id)?;
                let band = match receiver_band(&names[id]) {
                    Some(band) => band,
                    None => nearest_band(&spw, &names, &active, frequency)?,
                };
                let baseband = match spw.table().cell_accessor(id, "BBC_NO")?.scalar()? {
                    ScalarValue::Int32(value) => *value,
                    _ => {
                        return Err(MsError::InvalidInput(
                            "SPECTRAL_WINDOW BBC_NO must be an integer".into(),
                        ));
                    }
                };
                Ok(Window {
                    id,
                    band,
                    baseband,
                    frequency,
                })
            })
            .collect::<MsResult<Vec<_>>>()?;
        let antenna = ms.antenna()?;
        let elevations = mean_elevations(ms, &names, antenna.row_count())?;
        let antennas = elevations
            .into_iter()
            .enumerate()
            .map(|(id, elevation)| {
                Ok(Antenna {
                    name: antenna.name(id)?,
                    elevation,
                })
            })
            .collect::<MsResult<Vec<_>>>()?;
        Ok(Self {
            date: format_time(utc, FITS, 0),
            temperature,
            antennas,
            windows,
        })
    }
}

fn observation_start(ms: &MeasurementSet) -> MsResult<f64> {
    let observation = ms.observation()?;
    let ArrayValue::Float64(range) = observation.array(0, "TIME_RANGE")? else {
        return Err(MsError::InvalidInput(
            "OBSERVATION TIME_RANGE must contain doubles".into(),
        ));
    };
    let time = *range
        .first()
        .ok_or_else(|| MsError::InvalidInput("OBSERVATION TIME_RANGE is empty".into()))?;
    let keywords = observation.table().column_keywords("TIME_RANGE");
    let unit = match keywords.and_then(|keys| keys.get("QuantumUnits")) {
        Some(Value::Array(ArrayValue::String(units))) => units.first().map_or("s", String::as_str),
        _ => "s",
    };
    let reference = match keywords.and_then(|keys| keys.get("MEASINFO")) {
        Some(Value::Record(info)) => match info.get("Ref") {
            Some(Value::Scalar(ScalarValue::String(reference))) => reference.parse()?,
            _ => EpochRef::UTC,
        },
        _ => EpochRef::UTC,
    };
    let quantity =
        Quantity::new(time, unit).map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let epoch = MEpoch::from_quantity(&quantity, reference)?;
    Ok(epoch
        .convert_to(EpochRef::UTC, &MeasFrame::new())?
        .value()
        .as_mjd())
}

fn temperature(ms: &MeasurementSet) -> MsResult<String> {
    let weather = ms.weather()?;
    let mut count = 0;
    let mut total = 0.0;
    for row in 0..weather.row_count() {
        if !weather.bool(row, "TEMPERATURE_FLAG")? {
            total += match weather
                .table()
                .cell_accessor(row, "TEMPERATURE")?
                .scalar()?
            {
                ScalarValue::Float32(value) => f64::from(*value),
                ScalarValue::Float64(value) => *value,
                _ => {
                    return Err(MsError::InvalidInput(
                        "WEATHER TEMPERATURE must be real".into(),
                    ));
                }
            };
            count += 1;
        }
    }
    // NumPy's masked mean of an entirely flagged (or empty) column prints --.
    Ok(if count == 0 {
        "--".into()
    } else {
        number(total / f64::from(count))
    })
}

fn receiver_band(name: &str) -> Option<i32> {
    let (_, suffix) = name.split_once("ALMA_RB_")?;
    suffix.split_once('#')?.0.parse().ok()
}

fn mean_frequency(
    windows: &casa_ms::subtables::spectral_window::MsSpectralWindow<'_>,
    id: usize,
) -> MsResult<f64> {
    let channels = windows.chan_freq(id)?;
    Ok(channels.iter().sum::<f64>() / channels.len() as f64)
}

fn nearest_band(
    windows: &casa_ms::subtables::spectral_window::MsSpectralWindow<'_>,
    names: &[String],
    active: &[usize],
    frequency: f64,
) -> MsResult<i32> {
    let mut nearest = None::<(f64, i32)>;
    for &id in active {
        if let Some(band) = receiver_band(&names[id]) {
            let difference = (frequency - mean_frequency(windows, id)?).abs();
            if nearest.is_none_or(|(best, _)| difference < best) {
                nearest = Some((difference, band));
            }
        }
    }
    nearest.map(|(_, band)| band).ok_or_else(|| {
        MsError::InvalidInput(
            "No ALMA receiver band can be inferred from the spectral windows".into(),
        )
    })
}

fn mean_elevations(ms: &MeasurementSet, names: &[String], antennas: usize) -> MsResult<Vec<f64>> {
    let state = ms.state()?;
    let mut target_states = BTreeSet::new();
    for row in 0..state.row_count() {
        let mode = state.string(row, "OBS_MODE")?;
        if mode
            .split(',')
            .any(|intent| intent == "OBSERVE_TARGET#ON_SOURCE")
        {
            target_states.insert(row as i32);
        }
    }
    let columns = ms.main_table().required_scalar_columns_owned(&[
        "ANTENNA1",
        "ANTENNA2",
        "DATA_DESC_ID",
        "STATE_ID",
        "TIME",
    ])?;
    let (Int32(first), Int32(second), Int32(dd), Int32(state), Float64(times)) = (
        &columns["ANTENNA1"],
        &columns["ANTENNA2"],
        &columns["DATA_DESC_ID"],
        &columns["STATE_ID"],
        &columns["TIME"],
    ) else {
        return Err(MsError::InvalidInput(
            "Invalid MAIN column types for Jy/K elevation query".into(),
        ));
    };
    let selected = dd
        .iter()
        .zip(state)
        .filter(|(_, state)| target_states.contains(state))
        .map(|(&id, _)| id)
        .collect::<BTreeSet<_>>();
    let spw = ms.spectral_window()?;
    let descriptions = ms.data_description()?;
    let mut first_description = BTreeMap::new();
    let mut science = BTreeSet::new();
    for id in 0..descriptions.row_count() {
        let window = descriptions.spectral_window_id(id)? as usize;
        first_description.entry(window).or_insert(id as i32);
        if selected.contains(&(id as i32))
            && let Some(name) = names.get(window)
            && !name.contains("WVR")
            && (spw.num_chan(window)? != 1 || name.contains("FULL_RES"))
        {
            science.insert(window);
        }
    }
    let window = science.first().ok_or_else(|| {
        MsError::InvalidInput("No TDM/FDM spectral window with on-source target intent".into())
    })?;
    let data_description = first_description[window];
    let rows = (0..dd.len())
        .filter(|&row| {
            first[row] == second[row]
                && dd[row] == data_description
                && target_states.contains(&state[row])
        })
        .map(|row| (row, first[row], times[row]))
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(vec![f64::NAN; antennas]);
    }
    summarize_elevations(ms, antennas, &rows)
}

fn summarize_elevations(
    ms: &MeasurementSet,
    antennas: usize,
    rows: &[(usize, i32, f64)],
) -> MsResult<Vec<f64>> {
    let directions = ms.pointing()?.metadata_directions(rows)?;
    let mut sums = vec![(0.0, 0); antennas];
    for (&(_, antenna, _), direction) in rows.iter().zip(directions) {
        if !direction.refer().as_str().starts_with("AZEL") {
            return Err(MsError::InvalidInput(
                "Jy/K elevation requires POINTING in AZEL coordinates".into(),
            ));
        }
        let (sum, count) = sums.get_mut(antenna as usize).ok_or_else(|| {
            MsError::InvalidInput("Invalid antenna in Jy/K elevation query".into())
        })?;
        *sum += direction.latitude_rad().to_degrees();
        *count += 1;
    }
    Ok(sums
        .into_iter()
        .map(|(sum, count)| sum / f64::from(count))
        .collect())
}
