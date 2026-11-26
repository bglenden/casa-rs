// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical coordinates and telemetry for single-dish atmospheric correction.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_ms::{MeasurementSet, SubtableId};
use casa_tables::{Table, TableOptions};
use casa_types::{
    ArrayValue, ScalarValue, Value,
    measures::{MPosition, PositionRef},
};

use super::{error_text, model::GroundConditions};

const ON_SOURCE: &str = "OBSERVE_TARGET#ON_SOURCE";
const OFF_SOURCE: &str = "OBSERVE_TARGET#OFF_SOURCE";

/// Pre-scanned MAIN-table antenna, SPW, and intent metadata for `sdatmcor`.
pub struct MainRows {
    /// Observed antenna IDs in `MAIN`.
    pub antennas: BTreeSet<i32>,
    /// Observed spectral window IDs in `MAIN`.
    pub windows: BTreeSet<i32>,
    /// Science spectral window IDs matching `ON_SOURCE`.
    pub science: BTreeSet<i32>,
    /// Per-row `ON_SOURCE` flags.
    pub on_source: Vec<bool>,
    off_source: Vec<f64>,
}

impl MainRows {
    /// Scan `MAIN`, `STATE`, `DATA_DESCRIPTION`, and `SPECTRAL_WINDOW` once.
    pub fn read(ms: &MeasurementSet) -> Result<Self, String> {
        let source = ms.main_table();
        let states = ms.state().map_err(error_text)?;
        let intents = (0..states.row_count())
            .map(|row| states.string(row, "OBS_MODE").map_err(error_text))
            .collect::<Result<Vec<_>, _>>()?;
        let antenna = integers(source, "ANTENNA1")?;
        let descriptions = integers(source, "DATA_DESC_ID")?;
        let state = integers(source, "STATE_ID")?;
        let times = numbers(source, "TIME")?;
        let description = ms.data_description().map_err(error_text)?;
        let windows_by_description = (0..description.row_count())
            .map(|row| description.spectral_window_id(row).map_err(error_text))
            .collect::<Result<Vec<_>, _>>()?;
        let spw = ms.spectral_window().map_err(error_text)?;
        let science_like = (0..spw.row_count())
            .map(|window| {
                let name = spw.name(window).map_err(error_text)?;
                Ok(!name.contains("WVR")
                    && (spw.num_chan(window).map_err(error_text)? > 1 || name.contains("FULL_RES")))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut windows = BTreeSet::new();
        let mut science = BTreeSet::new();
        let mut on_source = Vec::with_capacity(source.row_count());
        let mut off_source = Vec::new();
        for ((description, state), time) in descriptions.into_iter().zip(state).zip(times) {
            let window = *windows_by_description
                .get(description as usize)
                .ok_or_else(|| format!("MAIN refers to missing data description {description}."))?;
            windows.insert(window);
            let mode = intents.get(state as usize).map_or("", String::as_str);
            on_source.push(mode.starts_with(ON_SOURCE));
            if science_like.get(window as usize).copied().unwrap_or(false)
                && has_intent(mode, ON_SOURCE)
            {
                science.insert(window);
            }
            if has_intent(mode, OFF_SOURCE) {
                off_source.push(time);
            }
        }
        off_source.sort_by(f64::total_cmp);
        off_source.dedup();
        let off_source = off_source
            .chunk_by(|left, right| right - left <= 1.0)
            .map(|samples| (samples[0] + samples[samples.len() - 1]) * 0.5)
            .collect();
        Ok(Self {
            antennas: antenna.into_iter().collect(),
            windows,
            science,
            on_source,
            off_source,
        })
    }

    pub(super) fn elevations(&self, pointing: &Pointing, time: f64) -> Result<[f64; 2], String> {
        let after = self.off_source.partition_point(|value| *value < time);
        let on = pointing.elevation(time);
        let off = match (
            after.checked_sub(1).and_then(|i| self.off_source.get(i)),
            self.off_source.get(after),
        ) {
            (Some(&previous), Some(&next)) => interpolate(
                time,
                [previous, next],
                [pointing.elevation(previous), pointing.elevation(next)],
            ),
            (Some(&off), None) | (None, Some(&off)) => pointing.elevation(off),
            (None, None) => return Err("No valid OFF_SOURCE observation.".into()),
        };
        Ok([on, off])
    }
}

fn has_intent(mode: &str, wanted: &str) -> bool {
    mode.split(',').any(|intent| intent.trim() == wanted)
}

/// WGS84 altitude in metres of antenna `antenna`.
pub fn site_altitude(ms: &MeasurementSet, antenna: i32) -> Result<f64, String> {
    let [x, y, z] = ms
        .antenna()
        .and_then(|antennas| antennas.position(antenna as usize))
        .map_err(error_text)?;
    MPosition::new_itrf(x, y, z)
        .convert_to(PositionRef::WGS84)
        .map(|position| position.values()[2])
        .map_err(error_text)
}

pub(super) struct Pointing {
    times: Vec<f64>,
    elevations: Vec<f64>,
}

impl Pointing {
    pub(super) fn read(ms: &MeasurementSet, reference: i32) -> Result<Self, String> {
        if reference < 0 || reference as usize >= ms.antenna().map_err(error_text)?.row_count() {
            return Err(format!("ERROR: reference antenna {reference} is invalid."));
        }
        let table = ms
            .subtable(SubtableId::Pointing)
            .ok_or("POINTING table is missing.")?;
        let direction = table
            .column_keywords("DIRECTION")
            .and_then(|keywords| keywords.get("MEASINFO"))
            .and_then(|value| match value {
                Value::Record(record) => Some(record),
                _ => None,
            })
            .and_then(|measure| measure.get("Ref"));
        let Some(Value::Scalar(ScalarValue::String(direction))) = direction else {
            return Err("DIRECTION has no reference. Reference should be AZEL-like.".into());
        };
        if !direction.starts_with("AZEL") {
            return Err(format!(
                "Direction reference {direction} is not supported. Reference should be AZEL-like."
            ));
        }
        let selected = integers(table, "ANTENNA_ID")?
            .into_iter()
            .enumerate()
            .filter_map(|(row, id)| (id == reference).then_some(row))
            .collect::<Vec<_>>();
        let time = table.column_accessor("TIME").map_err(error_text)?;
        let direction = table.column_accessor("DIRECTION").map_err(error_text)?;
        let mut samples = Vec::with_capacity(selected.len());
        for rows in selected.chunks(4096) {
            let times = time.scalar_cells_owned_for_rows(rows).map_err(error_text)?;
            let directions = direction
                .array_cells_owned_uncached(rows)
                .map_err(error_text)?;
            for (time, direction) in times.into_iter().zip(directions) {
                let Some(ScalarValue::Float64(time)) = time else {
                    return Err("POINTING has an invalid TIME cell.".into());
                };
                let Some(ArrayValue::Float64(direction)) = direction else {
                    return Err("POINTING has an invalid DIRECTION cell.".into());
                };
                let elevation = match direction.shape() {
                    [2, count] if *count > 0 => direction[[1, 0]],
                    [2] => direction[[1]],
                    _ => {
                        return Err("POINTING DIRECTION must carry longitude and elevation.".into());
                    }
                };
                samples.push((time, elevation));
            }
        }
        if samples.is_empty() {
            return Err("No pointing samples for the reference antenna.".into());
        }
        samples.sort_by(|left, right| left.0.total_cmp(&right.0));
        samples.dedup_by_key(|sample| sample.0);
        let (times, elevations) = samples.into_iter().unzip();
        Ok(Self { times, elevations })
    }

    fn elevation(&self, time: f64) -> f64 {
        if self.times.len() == 1 {
            return self.elevations[0];
        }
        let right = self
            .times
            .partition_point(|sample| *sample < time)
            .clamp(1, self.times.len() - 1);
        interpolate(
            time,
            [self.times[right - 1], self.times[right]],
            [self.elevations[right - 1], self.elevations[right]],
        )
    }
}

fn interpolate(time: f64, [left, right]: [f64; 2], [first, last]: [f64; 2]) -> f64 {
    first + (time - left) / (right - left) * (last - first)
}

pub(super) struct Weather {
    times: Vec<f64>,
    values: Vec<GroundConditions>,
}

impl Weather {
    pub(super) fn read(path: &Path) -> Result<Self, String> {
        let atm = Table::open(TableOptions::new(path.join("ASDM_CALATMOSPHERE")))
            .map_err(|_| "No Atmosphere measurements. Unable to apply offline ATM correction.")?;
        let mut records = weather_records(&atm)?;
        if records.is_empty() {
            return Err(
                "No Atmosphere measurements. Unable to apply offline ATM correction.".into(),
            );
        }
        let wvr = Table::open(TableOptions::new(path.join("ASDM_CALWVR")))
            .map_err(|_| "No radiometer measurements for atmospheric correction.")?;
        let radiometer = water_records(&wvr)?;
        if radiometer.is_empty() {
            return Err("No radiometer measurements for atmospheric correction.".into());
        }
        records.sort_by(|left, right| left.0.total_cmp(&right.0));
        let (times, values) = records
            .chunk_by(|left, right| left.0 == right.0)
            .map(|group| {
                let time = group[0].0;
                let water = nearest_water(&radiometer, time);
                (
                    time,
                    GroundConditions {
                        temperature_k: median(group.iter().map(|row| row.1[0]).collect()),
                        pressure_hpa: median(group.iter().map(|row| row.1[1]).collect()) / 100.,
                        humidity_percent: median(group.iter().map(|row| row.1[2]).collect()),
                        water_m: water,
                    },
                )
            })
            .unzip();
        Ok(Self { times, values })
    }

    pub(super) fn preceding(&self, time: f64) -> Option<GroundConditions> {
        self.times
            .partition_point(|value| *value <= time)
            .checked_sub(1)
            .map(|index| self.values[index])
    }
}

fn weather_records(table: &Table) -> Result<Vec<(f64, [f64; 3])>, String> {
    Ok(numbers(table, "startValidTime")?
        .into_iter()
        .zip(numbers(table, "groundTemperature")?)
        .zip(numbers(table, "groundPressure")?)
        .zip(numbers(table, "groundRelHumidity")?)
        .map(|(((time, temperature), pressure), humidity)| {
            (time, [temperature, pressure, humidity])
        })
        .collect())
}

fn water_records(table: &Table) -> Result<Vec<(f64, f64)>, String> {
    let mut values = numbers(table, "startValidTime")?
        .into_iter()
        .zip(numbers(table, "water")?)
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.0.total_cmp(&right.0));
    Ok(values)
}

fn nearest_water(radiometer: &[(f64, f64)], time: f64) -> f64 {
    let position = radiometer.partition_point(|(t, _)| *t < time);
    let before = position.checked_sub(1).map(|i| radiometer[i].0);
    let after = radiometer.get(position).map(|&(t, _)| t);
    let minimum = before
        .into_iter()
        .chain(after)
        .map(|t| (t - time).abs())
        .min_by(f64::total_cmp)
        .expect("nonempty radiometer samples");
    let left = radiometer.partition_point(|(t, _)| *t < time - minimum);
    let right = radiometer.partition_point(|(t, _)| *t <= time + minimum);
    median(
        radiometer[left..right]
            .iter()
            .map(|&(_, water)| water)
            .collect(),
    )
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_unstable_by(f64::total_cmp);
    (values[(values.len() - 1) / 2] + values[values.len() / 2]) * 0.5
}

/// Infer the default reference antenna from `FLAG_CMD` mount-off-source counts or the first active antenna.
pub fn default_antenna(ms: &MeasurementSet, rows: &MainRows) -> Result<i32, String> {
    let antennas = ms.antenna().map_err(error_text)?;
    let mut scores = BTreeMap::new();
    if let Some(commands) = ms.subtable(SubtableId::FlagCmd) {
        for row in 0..commands.row_count() {
            if text(commands, row, "REASON")? != "Mount_is_off_source" {
                continue;
            }
            let command = text(commands, row, "COMMAND")?;
            let (name, duration) = mount_flag(command)?;
            let score = scores.entry(name).or_insert((0.0, 0usize));
            score.0 += duration;
            score.1 += 1;
        }
    }
    let mut candidates = Vec::new();
    for &id in &rows.antennas {
        let name = antennas.name(id as usize).map_err(error_text)?;
        let (duration, count) = scores.get(name.as_str()).copied().unwrap_or_default();
        candidates.push((duration, count, id));
    }
    candidates
        .into_iter()
        .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)))
        .map(|entry| entry.2)
        .ok_or_else(|| "No Antenna was found.".into())
}

fn mount_flag(command: &str) -> Result<(&str, f64), String> {
    let value = |key: &str| {
        command
            .split_ascii_whitespace()
            .find_map(|component| component.strip_prefix(key))
            .map(|value| value.trim_matches(['\'', '"']))
            .ok_or_else(|| format!("Malformed Mount_is_off_source flag command: {command}"))
    };
    let antenna = value("antenna=")?.trim_matches(['&', '*']);
    let range = value("timerange=").or_else(|_| value("time="))?;
    let (start, end) = range
        .split_once('~')
        .ok_or("Malformed mount flag time range.")?;
    let time = |text| {
        casa_types::quanta::Quantity::read(text)
            .map(|value| value.get_si_value())
            .map_err(error_text)
    };
    Ok((antenna, (time(end)? - time(start)?).abs()))
}

fn text<'a>(table: &'a Table, row: usize, name: &str) -> Result<&'a str, String> {
    match table
        .cell_accessor(row, name)
        .and_then(|cell| cell.scalar())
        .map_err(error_text)?
    {
        ScalarValue::String(text) => Ok(text),
        _ => Err(format!("{name} must be a string column.")),
    }
}

fn integers(table: &Table, name: &str) -> Result<Vec<i32>, String> {
    scalar_column(table, name, |value| match value {
        ScalarValue::Int32(value) => Some(value),
        _ => None,
    })
}

fn numbers(table: &Table, name: &str) -> Result<Vec<f64>, String> {
    scalar_column(table, name, |value| match value {
        ScalarValue::Float64(value) if value.is_finite() => Some(value),
        ScalarValue::Float32(value) if value.is_finite() => Some(f64::from(value)),
        _ => None,
    })
}

fn scalar_column<T>(
    table: &Table,
    name: &str,
    convert: impl Fn(ScalarValue) -> Option<T>,
) -> Result<Vec<T>, String> {
    let column = table.column_accessor(name).map_err(error_text)?;
    let mut values = Vec::with_capacity(table.row_count());
    for from in (0..table.row_count()).step_by(4096) {
        let rows = (from..(from + 4096).min(table.row_count())).collect::<Vec<_>>();
        for cell in column
            .scalar_cells_owned_for_rows(&rows)
            .map_err(error_text)?
        {
            values.push(
                cell.and_then(&convert)
                    .ok_or_else(|| format!("Missing or invalid {name} cell."))?,
            );
        }
    }
    Ok(values)
}
