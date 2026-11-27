// SPDX-License-Identifier: LGPL-3.0-or-later
//! Turn ANTAB gain and temperature records into MeasurementSet subtable rows.

use std::collections::HashMap;

use casa_core::ArrayValue;
use casa_ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use time::Date;

use super::parse::{Antab, Header};

pub(super) struct Metadata {
    pub(super) names: Vec<String>,
    pub(super) spw_ids: Vec<i32>,
    pub(super) frequencies: Vec<Vec<f64>>,
    pub(super) times: [f64; 2],
    pub(super) gain_time: f64,
    pub(super) gain_interval: f64,
}

pub(super) struct Gain {
    pub(super) antenna: i32,
    pub(super) kind: &'static str,
    pub(super) frequencies: Option<[f64; 2]>,
    pub(super) coefficients: Vec<f32>,
    pub(super) sensitivities: [f32; 2],
}

pub(super) struct Tsys {
    pub(super) antenna: i32,
    pub(super) time: f64,
    pub(super) window: i32,
    pub(super) temperatures: [f32; 2],
}

#[derive(Debug, thiserror::Error)]
pub(super) enum GainError {
    #[error("{0}")]
    Missing(&'static str),
    #[error("{0}")]
    Invalid(String),
}

impl Metadata {
    pub(super) fn new(ms: &MeasurementSet) -> Result<Self, String> {
        let ant = ms.antenna().map_err(message)?;
        let names = (0..ant.row_count())
            .map(|row| ant.station(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(message)?;
        let spw = ms.spectral_window().map_err(message)?;
        let frequencies = (0..spw.row_count())
            .map(|row| spw.chan_freq(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(message)?;
        let dd = ms.data_description().map_err(message)?;
        let spw_ids = (0..dd.row_count())
            .map(|row| dd.spectral_window_id(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(message)?;
        let final_row = ms
            .main_table()
            .row_count()
            .checked_sub(1)
            .ok_or("MeasurementSet contains no visibility rows")?;
        let times = [0, final_row].map(|row| crate::solve::get_f64(ms.main_table(), row, "TIME"));
        let [first, last] = times;
        let observation = ms
            .subtable(SubtableId::Observation)
            .ok_or("missing OBSERVATION")?;
        let mut gain_time = 0.;
        let mut gain_interval = 0.;
        for row in 0..observation.row_count() {
            let ArrayValue::Float64(range) = observation
                .row_accessor()
                .array_cell(row, "TIME_RANGE")
                .map_err(message)?
            else {
                return Err("invalid OBSERVATION TIME_RANGE".into());
            };
            let &[open, close] = range.as_slice_memory_order().unwrap_or_default() else {
                return Err("invalid OBSERVATION TIME_RANGE".into());
            };
            if row == 0 {
                gain_interval = close - open;
            }
            gain_time += open + close;
        }
        gain_time /= (2 * observation.row_count()) as f64;
        Ok(Self {
            names,
            spw_ids,
            frequencies,
            times: [first.map_err(message)?, last.map_err(message)?],
            gain_time,
            gain_interval,
        })
    }

    fn antenna(&self, header: &Header, reserved: &[&str]) -> Option<i32> {
        let name = header.antenna(reserved)?;
        self.names
            .iter()
            .position(|station| station == name)
            .map(|index| index as i32)
    }
}

pub(super) fn gains(input: &Antab<'_>, metadata: &Metadata) -> Result<Vec<Gain>, GainError> {
    let mut output = Vec::new();
    for header in &input.gains {
        let Some(antenna) = metadata.antenna(
            header,
            &["EQUAT", "ALTAZ", "ELEV", "GCNRAO", "TABLE", "RCP", "LCP"],
        ) else {
            continue;
        };
        let kind = if header.has("ELEV") || header.has("EQUAT") {
            "POWER(EL)"
        } else if header.has("ALTAZ") {
            "POWER(ZA)"
        } else {
            continue;
        };
        let sensitivities = numbers(header, "DPFU")?;
        let sensitivities = match sensitivities.as_slice() {
            [single] => [*single; 2],
            [first, second, ..] => [*first, *second],
            [] => return Err(GainError::Missing("DPFU")),
        };
        let coefficients = numbers(header, "POLY")?;
        let frequencies = header
            .values("FREQ")
            .and_then(|values| values.first().zip(values.get(1)))
            .map(|(first, last)| {
                Ok::<_, String>([
                    first.parse().map_err(message)?,
                    last.parse().map_err(message)?,
                ])
            })
            .transpose()
            .map_err(GainError::Invalid)?;
        output.push(Gain {
            antenna,
            kind,
            frequencies,
            coefficients,
            sensitivities,
        });
    }
    Ok(output)
}

fn numbers(header: &Header, name: &'static str) -> Result<Vec<f32>, GainError> {
    header
        .values(name)
        .ok_or(GainError::Missing(name))?
        .iter()
        .map(|item| {
            item.parse()
                .map_err(|error| GainError::Invalid(message(error)))
        })
        .collect()
}

pub(super) fn temperatures(input: &Antab<'_>, metadata: &Metadata) -> Result<Vec<Tsys>, String> {
    let [first, last] = metadata.times;
    let first_day = 2_400_001 + (first / 86400.) as i32;
    let year = Date::from_julian_day(first_day).map_err(message)?.year();
    let mut output = Vec::new();
    for (header, lines) in &input.tsys {
        let Some(antenna) = metadata.antenna(header, &["SRC/SYS"]) else {
            continue;
        };
        let map = index_map(header, &metadata.spw_ids)?;
        let offset: f64 = header
            .values("TIMEOFF")
            .and_then(|entries| entries.first())
            .map_or(Ok(0.), |number| number.parse().map_err(message))?;
        for line in lines {
            let mut tokens = line.split_whitespace();
            let (Some(day), Some(clock)) = (tokens.next(), tokens.next()) else {
                continue;
            };
            let day =
                Date::from_ordinal_date(year, day.parse().map_err(message)?).map_err(message)?;
            let time = f64::from(day.to_julian_day() - 2_400_001) * 86400.
                + clock_seconds(clock)?
                + offset;
            if time < first || time > last {
                continue;
            }
            let values: Vec<_> = tokens.collect();
            for &window in &metadata.spw_ids {
                let temperatures = ['R', 'L'].map(|hand| {
                    map.get(&(hand, window))
                        .and_then(|&column| values.get(column))
                        .and_then(|value| value.parse::<f64>().ok())
                        .filter(|&value| value != 999.9)
                        .unwrap_or(-999.9) as f32
                });
                output.push(Tsys {
                    antenna,
                    time,
                    window,
                    temperatures,
                });
            }
        }
    }
    Ok(output)
}

fn index_map(header: &Header, selected: &[i32]) -> Result<HashMap<(char, i32), usize>, String> {
    let mut map = HashMap::new();
    for key in ["INDEX", "INDEX2"] {
        let Some(labels) = header.values(key) else {
            continue;
        };
        for (column, label) in labels.iter().enumerate() {
            for part in label.split('|') {
                let mut chars = part.chars();
                let hand = chars.next().ok_or("empty antenna temperature index")?;
                if hand == 'X' {
                    continue;
                }
                let windows = chars.as_str();
                let (first, last) = windows.split_once(':').unwrap_or((windows, windows));
                let first = first.parse::<i32>().map_err(message)?.saturating_sub(1);
                let last = last.parse::<i32>().map_err(message)?.saturating_sub(1);
                for spw in selected
                    .iter()
                    .copied()
                    .filter(|window| (first..=last).contains(window))
                {
                    map.insert((hand, spw), column);
                }
            }
        }
    }
    Ok(map)
}

fn clock_seconds(clock: &str) -> Result<f64, String> {
    let numbers = clock
        .split(':')
        .map(str::parse::<f64>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(message)?;
    let seconds = match numbers.as_slice() {
        [hours] => (hours * 3600.).trunc(),
        [hours, minutes] => hours * 3600. + minutes.trunc() * 60. + (minutes.fract() * 60.).trunc(),
        [hours, minutes, seconds] => hours * 3600. + minutes * 60. + seconds.trunc(),
        _ => return Err(format!("invalid ANTAB clock {clock:?}")),
    };
    if !(0. ..86402.).contains(&seconds) {
        return Err(format!("invalid ANTAB clock {clock:?}"));
    }
    Ok(seconds)
}

pub(super) fn message(error: impl std::fmt::Display) -> String {
    error.to_string()
}
