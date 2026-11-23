// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sky spectra from successive edge integrations of a total-power map.

use std::collections::BTreeMap;
use std::path::Path;

use casa_ms::MeasurementSet;
use casa_tables::RequiredScalarColumnValues::{Bool, Float64, Int32};
use casa_types::ArrayValue;

use super::spectrum::{self, ReferenceTimestamp, SkySpectrum};
use crate::CalibrationOutputMode;
use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_FIELD_ID, COL_INTERVAL, COL_OBSERVATION_ID, COL_SCAN_NUMBER,
    COL_TIME,
};

mod mask;
mod otf;
mod raster;

#[derive(Clone, Copy)]
pub(super) enum Selection {
    Otf { fraction: f64 },
    Raster { fraction: f64, noff: i64 },
}

pub(super) struct Integration {
    pub(super) row: usize,
    pub(super) field: i32,
    pub(super) antenna: i32,
    pub(super) other_antenna: i32,
    pub(super) feed: i32,
    pub(super) other_feed: i32,
    pub(super) description: i32,
    pub(super) window: usize,
    pub(super) state: i32,
    pub(super) scan: i32,
    pub(super) observation: i32,
    pub(super) time: f64,
    pub(super) interval: f64,
    pub(super) exposure: f64,
    pub(super) flagged: bool,
}

pub(super) fn generate(
    ms: &MeasurementSet,
    selected: &[usize],
    destination: &Path,
    mode: CalibrationOutputMode,
    selection: Selection,
) -> Result<usize, String> {
    let descriptions = ms.data_description().map_err(error_text)?;
    let windows = (0..descriptions.row_count())
        .map(|row| descriptions.spectral_window_id(row).map(|id| id as usize))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let spectral = ms.spectral_window().map_err(error_text)?;
    let channels = (0..spectral.row_count())
        .map(|row| spectral.num_chan(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let integrations = read_integrations(ms, selected, &windows)?;
    let (subtype, reference_data) = match selection {
        Selection::Otf { fraction } => (
            "SDSKY_OTF",
            otf::reference_rows(ms, &integrations, fraction)?,
        ),
        Selection::Raster { fraction, noff } => {
            let time_ranges =
                raster::edge_windows(ms, &integrations, &windows, &channels, fraction, noff)?;
            let references = integrations
                .iter()
                .filter(|sample| {
                    sample.antenna == sample.other_antenna
                        && time_ranges
                            .get(time_ranges.partition_point(|range| range[1] < sample.time))
                            .is_some_and(|range| range[0] <= sample.time)
                })
                .collect();
            ("SDSKY_RASTER", references)
        }
    };
    let solutions = accumulate(ms, reference_data, &channels)?;
    spectrum::write(
        ms,
        destination,
        mode,
        subtype,
        ReferenceTimestamp::AllIntegrations,
        solutions,
    )
}

pub(super) fn read_integrations(
    ms: &MeasurementSet,
    rows: &[usize],
    windows: &[usize],
) -> Result<Vec<Integration>, String> {
    let columns = ms
        .main_table()
        .required_scalar_columns_owned_for_rows(
            &[
                COL_FIELD_ID,
                COL_ANTENNA1,
                COL_ANTENNA2,
                "FEED1",
                "FEED2",
                "DATA_DESC_ID",
                "STATE_ID",
                COL_SCAN_NUMBER,
                COL_OBSERVATION_ID,
                COL_TIME,
                COL_INTERVAL,
                "EXPOSURE",
                "FLAG_ROW",
            ],
            rows,
        )
        .map_err(error_text)?;
    let ids = |column: &str| match &columns[column] {
        Int32(values) => Ok(values.as_slice()),
        _ => Err(format!("MeasurementSet {column} is not Int32.")),
    };
    let numbers = |column: &str| match &columns[column] {
        Float64(values) => Ok(values.as_slice()),
        _ => Err(format!("MeasurementSet {column} is not Float64.")),
    };
    let field = ids(COL_FIELD_ID)?;
    let antenna = ids(COL_ANTENNA1)?;
    let other_antenna = ids(COL_ANTENNA2)?;
    let feed = ids("FEED1")?;
    let other_feed = ids("FEED2")?;
    let description = ids("DATA_DESC_ID")?;
    let state = ids("STATE_ID")?;
    let scan = ids(COL_SCAN_NUMBER)?;
    let observation = ids(COL_OBSERVATION_ID)?;
    let time = numbers(COL_TIME)?;
    let interval = numbers(COL_INTERVAL)?;
    let exposure = numbers("EXPOSURE")?;
    let Bool(flagged) = &columns["FLAG_ROW"] else {
        return Err("MeasurementSet FLAG_ROW is not Boolean.".into());
    };
    rows.iter()
        .enumerate()
        .map(|(index, &row)| {
            let window = *windows
                .get(description[index] as usize)
                .ok_or("Invalid DATA_DESC_ID in reference data.")?;
            if !time[index].is_finite()
                || !interval[index].is_finite()
                || !exposure[index].is_finite()
            {
                return Err("Invalid reference time, interval or exposure.".into());
            }
            Ok(Integration {
                row,
                field: field[index],
                antenna: antenna[index],
                other_antenna: other_antenna[index],
                feed: feed[index],
                other_feed: other_feed[index],
                description: description[index],
                window,
                state: state[index],
                scan: scan[index],
                observation: observation[index],
                time: time[index],
                interval: interval[index],
                exposure: exposure[index],
                flagged: flagged[index],
            })
        })
        .collect()
}

fn accumulate<'a>(
    ms: &MeasurementSet,
    reference_data: impl IntoIterator<Item = &'a Integration>,
    channels: &[i32],
) -> Result<Vec<(i32, SkySpectrum)>, String> {
    let mut by_field = BTreeMap::<_, Vec<_>>::new();
    for sample in reference_data {
        if channels
            .get(sample.window)
            .copied()
            .ok_or("Invalid reference spectral window.")?
            != 4
        {
            by_field
                .entry((
                    sample.field,
                    sample.antenna,
                    sample.feed,
                    sample.description,
                ))
                .or_default()
                .push(sample);
        }
    }
    let main = ms.main_table();
    let observed = crate::model_columns::observed_column(ms);
    let mut solutions = Vec::new();
    for ((_, antenna, _, _), mut group) in by_field {
        group.sort_by(|left, right| left.time.total_cmp(&right.time));
        let mut accumulated = None;
        for (index, sample) in group.iter().enumerate() {
            if sample.flagged {
                continue;
            }
            let power = spectrum::read_power(main, sample.row, observed)?;
            let ArrayValue::Bool(flags) = main
                .cell_accessor(sample.row, "FLAG")
                .and_then(|cell| cell.array())
                .map_err(error_text)?
            else {
                return Err("MeasurementSet FLAG is not Boolean.".into());
            };
            accumulated
                .get_or_insert_with(|| SkySpectrum::new(sample.window, &power))
                .add(&power, flags, sample.time, sample.exposure)?;
            let closes_group = group.get(index + 1).is_none_or(|next| {
                2. * (next.time - sample.time) / (sample.interval + next.interval) > 1.1
            });
            if closes_group && let Some(spectrum) = accumulated.take() {
                solutions.push((antenna, spectrum));
            }
        }
    }
    if solutions.is_empty() {
        return Err("No usable reference integrations in selected scan edges.".into());
    }
    Ok(solutions)
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
