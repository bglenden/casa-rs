// SPDX-License-Identifier: LGPL-3.0-or-later
//! Optional standard MS subtables populated by the FITS-IDI reader.

use casa_core::{ScalarValue, Value};
use fitsio::FitsFile;
use fitsio::hdu::FitsHdu;

use super::super::invalid;
use super::super::uvfits_ms::insert;
use super::metadata::{Geometry, IdiTable};
use crate::column_def::{ColumnKind, column_schema};
use crate::ms::apply_column_metadata;
use crate::schema::{self, SubtableId};
use crate::subtable_rows::{f, f32_array, i, row_from_defs, s, subtable_mut};
use crate::{MeasurementSet, MsResult};

const SECONDS_PER_DAY: f64 = 86_400.0;

pub(super) fn system_temperatures(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    geometry: &Geometry,
) -> MsResult<()> {
    let mut table = IdiTable::new(fits, hdu)?;
    let bands = table.key::<i32>("NO_BAND")?.unwrap_or(1);
    let bands = usize::try_from(bands)
        .ok()
        .filter(|&value| value > 0)
        .ok_or_else(|| invalid("Invalid FITS-IDI temperature NO_BAND"))?;
    let antennas = antenna_ids(&mut table, geometry)?;
    let times = table.column::<f64>("TIME", 1)?;
    let intervals = table.column::<f32>("TIME_INTERVAL", 1)?;
    let first = table.column_or_replicate::<f32>("TSYS_1", bands)?;
    let second = table
        .has("TSYS_2")
        .then(|| table.column_or_replicate::<f32>("TSYS_2", bands))
        .transpose()?;
    add_optional_columns(ms, SubtableId::SysCal, &["TSYS"])?;
    for (row, antenna) in antennas.into_iter().enumerate() {
        for band in 0..bands {
            let offset = row * bands + band;
            let mut temperatures = vec![first[offset]];
            if let Some(second) = &second {
                temperatures.push(second[offset]);
            }
            insert_with_optional(
                ms,
                SubtableId::SysCal,
                &[
                    ("ANTENNA_ID", i(antenna)),
                    ("FEED_ID", i(0)),
                    ("SPECTRAL_WINDOW_ID", i(band as i32)),
                    (
                        "TIME",
                        f(geometry.reference_time + times[row] * SECONDS_PER_DAY),
                    ),
                    ("INTERVAL", f(f64::from(intervals[row]) * SECONDS_PER_DAY)),
                    ("TSYS", f32_array(&temperatures, vec![temperatures.len()])),
                ],
            )?;
        }
    }
    Ok(())
}

pub(super) fn weather(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    geometry: &Geometry,
) -> MsResult<()> {
    let mut table = IdiTable::new(fits, hdu)?;
    let antennas = antenna_ids(&mut table, geometry)?;
    let times = table.column::<f64>("TIME", 1)?;
    let intervals = table.column::<f32>("TIME_INTERVAL", 1)?;
    let mut measurements = [
        ("DEW_POINT", table.column::<f32>("DEWPOINT", 1)?),
        ("TEMPERATURE", table.column::<f32>("TEMPERATURE", 1)?),
        ("WIND_DIRECTION", table.column::<f32>("WIND_DIRECTION", 1)?),
        ("WIND_SPEED", table.column::<f32>("WIND_VELOCITY", 1)?),
        ("PRESSURE", table.column::<f32>("PRESSURE", 1)?),
    ]
    .to_vec();
    for (_, temperatures) in &mut measurements[..2] {
        temperatures
            .iter_mut()
            .for_each(|value| *value = (f64::from(*value) + 273.15) as f32);
    }
    measurements[2]
        .1
        .iter_mut()
        .for_each(|value| *value = f64::from(*value).to_radians() as f32);
    for (in_name, out_name) in [("WVR_H2O", "H2O"), ("IONOS_ELECTRON", "IONOS_ELECTRON")] {
        if table.has(in_name) {
            measurements.push((out_name, table.column::<f32>(in_name, 1)?));
        }
    }
    let columns = measurements
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    add_optional_columns(ms, SubtableId::Weather, &columns)?;
    for (row, antenna) in antennas.into_iter().enumerate() {
        let mut values = vec![
            ("ANTENNA_ID", i(antenna)),
            (
                "TIME",
                f(geometry.reference_time + times[row] * SECONDS_PER_DAY),
            ),
            ("INTERVAL", f(f64::from(intervals[row]) * SECONDS_PER_DAY)),
        ];
        values.extend(
            measurements
                .iter()
                .map(|(name, col)| (*name, Value::Scalar(ScalarValue::Float32(col[row])))),
        );
        insert_with_optional(ms, SubtableId::Weather, &values)?;
    }
    Ok(())
}

fn insert_with_optional(
    ms: &mut MeasurementSet,
    id: SubtableId,
    values: &[(&str, Value)],
) -> MsResult<()> {
    let mut row = row_from_defs(schema::required_columns(id), values);
    for (name, value) in values {
        if row.get(name).is_none() {
            row.upsert(*name, value.clone());
        }
    }
    subtable_mut(ms, id)?.add_row(row)?;
    Ok(())
}

fn add_optional_columns(ms: &mut MeasurementSet, id: SubtableId, names: &[&str]) -> MsResult<()> {
    let table = subtable_mut(ms, id)?;
    for def in schema::optional_columns(id)
        .iter()
        .filter(|column| names.contains(&column.name))
    {
        if table
            .schema()
            .is_some_and(|schema| !schema.contains_column(def.name))
        {
            let default = matches!(def.column_kind, ColumnKind::Scalar)
                .then_some(Value::Scalar(ScalarValue::Float32(0.0)));
            table.add_column(column_schema(def), default)?;
        }
        apply_column_metadata(table, &[*def]);
    }
    Ok(())
}

pub(super) fn antenna_ids(table: &mut IdiTable<'_>, geometry: &Geometry) -> MsResult<Vec<i32>> {
    table
        .column::<i32>("ANTENNA_NO", 1)?
        .into_iter()
        .map(|number| lookup_antenna(geometry, number))
        .collect()
}

fn lookup_antenna(geometry: &Geometry, number: i32) -> MsResult<i32> {
    geometry
        .antennas
        .get(&number)
        .copied()
        .ok_or_else(|| invalid(format!("Missing FITS-IDI antenna {number}")))
}

pub(super) fn flag_commands(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    geometry: &Geometry,
    spectral_windows: usize,
) -> MsResult<()> {
    let mut table = IdiTable::new(fits, hdu)?;
    let bands = table.count("NO_BAND")?;
    let corr_count = table.count("NO_STKD")?;
    let first_stokes = table.required_key::<i32>("STK_1")?;
    let labels = ["RR", "LL", "RL", "LR", "XX", "YY", "XY", "YX"];
    let start = usize::try_from(-i64::from(first_stokes) - 1).map_err(invalid)?;
    let corr = labels
        .get(start..start.saturating_add(corr_count))
        .filter(|_| corr_count <= 4)
        .ok_or_else(|| invalid("Invalid FITS-IDI flag correlation range"))?;
    let sources = table.column::<i32>("SOURCE_ID", 1)?;
    let arrays = table.column::<i32>("ARRAY", 1)?;
    let antennas = table.column::<i32>("ANTS", 2)?;
    let frequencies = table.column::<i32>("FREQID", 1)?;
    let times = table.column::<f32>("TIMERANG", 2)?;
    let band_flags = table.column_or_replicate::<i32>("BANDS", bands)?;
    let channels = table.column::<i32>("CHANS", 2)?;
    let flags = table.column::<i32>("PFLAGS", 4)?;
    let reasons = table.column::<String>("REASON", 1)?;
    let severities = table.column::<i32>("SEVERITY", 1)?;
    for row in 0..table.rows {
        if arrays[row] != 0 {
            continue;
        }
        let antenna = [antennas[2 * row], antennas[2 * row + 1]];
        let antenna = antenna.map(|number| {
            (number != 0)
                .then(|| lookup_antenna(geometry, number))
                .transpose()
        });
        let [first, second] = antenna;
        let [first, second] = [first?, second?];
        let mut terms = Vec::new();
        if let Some(a) = first {
            match second {
                Some(b) => terms.push(format!("antenna='{a}&{b}'")),
                None => terms.push(format!("antenna='{a}'")),
            }
        }
        if sources[row] != 0 {
            terms.push(format!("field='{}'", sources[row] - 1));
        }
        let lo = channels[2 * row];
        let hi = channels[2 * row + 1];
        if lo > hi || lo < -1 {
            return Err(invalid("Invalid FITS-IDI flag channel range"));
        }
        let requested =
            (frequencies[row] > 0).then_some(frequencies[row].saturating_sub(1) as usize);
        let selected_bands = &band_flags[row * bands..(row + 1) * bands];
        if requested.is_some() || selected_bands.contains(&0) || lo > 0 {
            let windows = (0..spectral_windows)
                .filter(|&window| {
                    requested.is_none_or(|freq| window / bands == freq)
                        && selected_bands[window % bands] != 0
                })
                .map(|window| window.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let windows = if requested.is_none() && !selected_bands.contains(&0) {
                "*"
            } else {
                &windows
            };
            let channel = if lo > 0 {
                format!(":{}~{}", lo - 1, hi - 1)
            } else {
                String::new()
            };
            terms.push(format!("spw='{windows}{channel}'"));
        }
        let flagged = &flags[4 * row..4 * row + corr_count];
        if flagged.contains(&0) {
            let names = corr
                .iter()
                .zip(flagged)
                .filter_map(|(name, &flag)| (flag != 0).then_some(*name))
                .collect::<Vec<_>>()
                .join(",");
            terms.push(format!("correlation='{names}'"));
        }
        let (first, last) = (times[2 * row], times[2 * row + 1]);
        insert(
            ms,
            SubtableId::FlagCmd,
            &[
                (
                    "TIME",
                    f(geometry.reference_time + f64::from((first + last) / 2.0) * SECONDS_PER_DAY),
                ),
                ("INTERVAL", f(f64::from(last - first) * SECONDS_PER_DAY)),
                ("TYPE", s("FLAG")),
                ("REASON", s(reasons[row].trim_end())),
                ("SEVERITY", i(severities[row])),
                ("COMMAND", s(&terms.join(" "))),
            ],
        )?;
    }
    Ok(())
}
