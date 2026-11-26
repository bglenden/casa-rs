// SPDX-License-Identifier: LGPL-3.0-or-later
//! AIPS weather, system-temperature and gain-curve extensions.

use std::collections::{BTreeMap, BTreeSet};

use casa_core::measures::MPosition;
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_tables::Table;
use fitsio::FitsFile;
use fitsio::tables::ColumnDataType as Type;

use super::export_layout::{OutputLayout, f64_cell, has_column, i32_cell, subtable};
use super::export_metadata::{ExportHeader, create_table, measure_ref, put};
use super::invalid;
use super::uvfits_export::fits_date;
use crate::{MeasurementSet, MsResult, SubtableId};

const DAY: f64 = 86_400.0;

pub(super) fn write_weather(
    fits: &mut FitsFile,
    ms: &MeasurementSet,
    meta: &ExportHeader,
) -> MsResult<()> {
    let Some(table) = ms
        .subtable(SubtableId::Weather)
        .filter(|table| table.row_count() > 0)
    else {
        return Ok(());
    };
    let fields = [
        ("TEMPERATURE", "TEMPERATURE", "CENTIGRADE"),
        ("PRESSURE", "PRESSURE", "MILLIBAR"),
        ("DEW_POINT", "DEWPOINT", "CENTIGRADE"),
        ("WIND_SPEED", "WIND_VELOCITY", "M/SEC"),
        ("WIND_DIRECTION", "WIND_DIRECTION", "DEGREES"),
        ("H2O", "WVR_H2O", "m-2"),
        ("IONOS_ELECTRON", "IONOS_ELECTRON", "m-2"),
    ];
    let mut descriptors = vec![
        ("TIME", Type::Double, 1, "DAYS"),
        ("TIME_INTERVAL", Type::Float, 1, "DAYS"),
        ("ANTENNA_NO", Type::Int, 1, ""),
        ("SUBARRAY", Type::Int, 1, ""),
    ];
    descriptors.extend(
        fields
            .iter()
            .map(|&(_, name, unit)| (name, Type::Float, 1, unit)),
    );
    let hdu = create_table(fits, "AIPS WX", &descriptors)?;
    hdu.write_key(fits, "EXTVER", 1).map_err(invalid)?;
    hdu.write_key(fits, "OBSCODE", "").map_err(invalid)?;
    hdu.write_key(fits, "RDATE", fits_date(meta.time, false))
        .map_err(invalid)?;
    hdu.write_key(fits, "TABREV", 3).map_err(invalid)?;
    let origin = (meta.time / DAY).floor() * DAY;
    let mut times = Vec::with_capacity(table.row_count());
    let mut intervals = Vec::with_capacity(table.row_count());
    let mut antennas = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        times.push((f64_cell(table, row, "TIME")? - origin) / DAY);
        intervals.push((f64_cell(table, row, "INTERVAL")? / DAY) as f32);
        let antenna = i32_cell(table, row, "ANTENNA_ID")?;
        antennas.push(if antenna == -1 {
            0
        } else {
            antenna_number(meta, antenna)?
        });
    }
    put(&hdu, fits, "TIME", &times)?;
    put(&hdu, fits, "TIME_INTERVAL", &intervals)?;
    put(&hdu, fits, "ANTENNA_NO", &antennas)?;
    put(&hdu, fits, "SUBARRAY", &vec![0i32; table.row_count()])?;
    for (column, name, _) in fields {
        let values = (0..table.row_count())
            .map(|row| weather_cell(table, row, column))
            .collect::<MsResult<Vec<_>>>()?;
        put(&hdu, fits, name, &values)?;
    }
    Ok(())
}

fn weather_cell(table: &Table, row: usize, column: &str) -> MsResult<f32> {
    if !has_column(table, column) {
        return Ok(0.0);
    }
    let ScalarValue::Float32(value) = table.cell_accessor(row, column)?.scalar()? else {
        return Err(invalid(format!("Invalid weather column {column}")));
    };
    let value = f64::from(*value);
    match column {
        "TEMPERATURE" | "DEW_POINT" => Ok((value - 273.15) as f32),
        "PRESSURE" => convert_unit(table, column, value, "hPa", "mbar"),
        "WIND_DIRECTION" => convert_unit(table, column, value, "rad", "deg"),
        _ => Ok(value as f32),
    }
}

fn convert_unit(
    table: &Table,
    column: &str,
    value: f64,
    default: &str,
    output: &str,
) -> MsResult<f32> {
    let input = match table
        .column_keywords(column)
        .and_then(|keywords| keywords.get("QuantumUnits"))
    {
        Some(Value::Array(ArrayValue::String(units))) => {
            units.iter().next().map_or(default, String::as_str)
        }
        _ => default,
    };
    let value = Quantity::new(value, input).map_err(invalid)?;
    Ok(value
        .get_value_in(&Unit::new(output).map_err(invalid)?)
        .map_err(invalid)? as f32)
}

struct SysCal {
    antenna: i32,
    time: f64,
    interval: f64,
    window: i32,
    system_temperature: Vec<f32>,
}

pub(super) fn write_system_calibration(
    fits: &mut FitsFile,
    ms: &MeasurementSet,
    layout: &OutputLayout,
    meta: &ExportHeader,
) -> MsResult<()> {
    let Some(table) = ms
        .subtable(SubtableId::SysCal)
        .filter(|table| table.row_count() > 0 && has_column(table, "TSYS"))
    else {
        return Ok(());
    };
    let rows = read_syscal(table, layout)?;
    let Some(first) = rows.first() else {
        return Ok(());
    };
    if !matches!(first.system_temperature.len(), 1 | 2)
        || rows
            .iter()
            .any(|row| row.system_temperature.len() != first.system_temperature.len())
    {
        return Err(invalid("Irregular number of polarizations in SYSCAL"));
    }
    write_temperature(fits, layout, meta, &rows, first.system_temperature.len())?;
    if regular_gain_times(&rows) {
        write_gain(fits, ms, layout, meta, &rows)?;
    }
    Ok(())
}

fn regular_gain_times(rows: &[SysCal]) -> bool {
    let mut counts = rows.chunk_by(|a, b| a.antenna == b.antenna).map(|group| {
        1 + group
            .windows(2)
            .filter(|pair| pair[0].time != pair[1].time)
            .count()
    });
    counts
        .next()
        .is_some_and(|first| counts.all(|count| count == first))
}

#[cfg(test)]
#[test]
fn gain_curve_checks_distinct_calibration_times_per_antenna() {
    let sample = |antenna, time, window| SysCal {
        antenna,
        time,
        window,
        interval: 1.0,
        system_temperature: vec![100.0],
    };
    let mut rows = vec![sample(0, 0.0, 1), sample(0, 1.0, 1), sample(1, 0.0, 1)];
    assert!(!regular_gain_times(&rows));
    rows.push(sample(1, 0.0, 2));
    assert!(!regular_gain_times(&rows));
    rows.push(sample(1, 1.0, 1));
    assert!(regular_gain_times(&rows));
}

fn read_syscal(table: &Table, layout: &OutputLayout) -> MsResult<Vec<SysCal>> {
    let antennas = layout
        .rows
        .iter()
        .flat_map(|row| row.antenna)
        .collect::<BTreeSet<_>>();
    let windows = layout
        .windows
        .iter()
        .map(|window| window.id)
        .zip(1..)
        .collect::<BTreeMap<_, _>>();
    let first_time = f64_cell(table, 0, "TIME")?;
    let mut skip = 0;
    for row in 1..table.row_count() {
        let time = f64_cell(table, row, "TIME")?;
        if time != first_time {
            skip = if time < first_time { row } else { 0 };
            break;
        }
    }
    let mut rows = Vec::new();
    for row in skip..table.row_count() {
        let antenna = i32_cell(table, row, "ANTENNA_ID")?;
        let window = i32_cell(table, row, "SPECTRAL_WINDOW_ID")?;
        let Some(&window) = windows.get(&window).filter(|_| antennas.contains(&antenna)) else {
            continue;
        };
        let ArrayValue::Float32(temperatures) = table.cell_accessor(row, "TSYS")?.array()? else {
            return Err(invalid("Invalid system temperature in SYSCAL"));
        };
        rows.push(SysCal {
            antenna,
            time: f64_cell(table, row, "TIME")?,
            interval: f64_cell(table, row, "INTERVAL")?,
            window,
            system_temperature: temperatures.iter().copied().collect(),
        });
    }
    rows.sort_by(|a, b| {
        a.antenna
            .cmp(&b.antenna)
            .then(a.time.total_cmp(&b.time))
            .then(a.window.cmp(&b.window))
    });
    Ok(rows)
}

fn write_temperature(
    fits: &mut FitsFile,
    layout: &OutputLayout,
    meta: &ExportHeader,
    rows: &[SysCal],
    hands: usize,
) -> MsResult<()> {
    let ifs = if layout.combine {
        layout.windows.len()
    } else {
        1
    };
    if !rows.len().is_multiple_of(ifs) {
        return Err(invalid("Irregular IFs in SYSCAL"));
    }
    let mut descriptors = vec![
        ("TIME", Type::Float, 1, "DAYS"),
        ("TIME INTERVAL", Type::Float, 1, "DAYS"),
        ("SOURCE ID", Type::Int, 1, ""),
        ("ANTENNA NO.", Type::Int, 1, ""),
        ("SUBARRAY", Type::Int, 1, ""),
        ("FREQ ID", Type::Int, 1, ""),
        ("TSYS 1", Type::Float, ifs, "KELVINS"),
        ("TANT 1", Type::Float, ifs, "KELVINS"),
    ];
    if hands == 2 {
        descriptors.extend([
            ("TSYS 2", Type::Float, ifs, "KELVINS"),
            ("TANT 2", Type::Float, ifs, "KELVINS"),
        ]);
    }
    let hdu = create_table(fits, "AIPS TY", &descriptors)?;
    for (name, value) in [
        ("EXTVER", 1),
        ("NO_IF", ifs as i32),
        ("NO_POL", hands as i32),
        ("REVISION", 10),
    ] {
        hdu.write_key(fits, name, value).map_err(invalid)?;
    }
    let origin = (meta.time / DAY).floor() * DAY;
    let mut times = Vec::new();
    let mut intervals = Vec::new();
    let mut antennas = Vec::new();
    let mut windows = Vec::new();
    for group in rows.chunks_exact(ifs) {
        let first = &group[0];
        if group
            .iter()
            .any(|row| row.time != first.time || row.antenna != first.antenna)
        {
            return Err(invalid("Irregular times or antennas in SYSCAL"));
        }
        if layout.combine
            && group
                .iter()
                .enumerate()
                .any(|(index, row)| row.window != index as i32 + 1)
        {
            return Err(invalid(
                "Missing or repeated SYSCAL spectral window in a combined IF",
            ));
        }
        times.push(((first.time - origin) / DAY) as f32);
        intervals.push((first.interval / DAY) as f32);
        antennas.push(antenna_number(meta, first.antenna)?);
        windows.push(first.window);
    }
    for name in ["SOURCE ID", "SUBARRAY"] {
        put(&hdu, fits, name, &vec![1i32; times.len()])?;
    }
    put(&hdu, fits, "TIME", &times)?;
    put(&hdu, fits, "TIME INTERVAL", &intervals)?;
    put(&hdu, fits, "ANTENNA NO.", &antennas)?;
    put(&hdu, fits, "FREQ ID", &windows)?;
    for hand in 0..hands {
        let values = rows
            .iter()
            .map(|row| row.system_temperature[hand])
            .collect::<Vec<_>>();
        put(&hdu, fits, &format!("TSYS {}", hand + 1), &values)?;
        put(
            &hdu,
            fits,
            &format!("TANT {}", hand + 1),
            &vec![0f32; rows.len()],
        )?;
    }
    Ok(())
}

fn write_gain(
    fits: &mut FitsFile,
    ms: &MeasurementSet,
    layout: &OutputLayout,
    meta: &ExportHeader,
    rows: &[SysCal],
) -> MsResult<()> {
    let ifs = if layout.combine {
        layout.windows.len()
    } else {
        1
    };
    let windows = if layout.combine {
        1
    } else {
        layout.windows.len()
    };
    let hands = i32_cell(subtable(ms, SubtableId::Feed)?, 0, "NUM_RECEPTORS")?.clamp(1, 2);
    let fields = [
        "TYPE", "NTERM", "X_TYP", "Y_TYP", "X_VAL", "Y_VAL", "GAIN", "SENS",
    ];
    let mut descriptors = vec![
        ("ANTENNA_NO".to_owned(), Type::Int, 1, ""),
        ("SUBARRAY".to_owned(), Type::Int, 1, ""),
        ("FREQ ID".to_owned(), Type::Int, 1, ""),
    ];
    for hand in 1..=hands {
        for (index, field) in fields.iter().enumerate() {
            let kind = if index < 4 { Type::Int } else { Type::Float };
            let width = if matches!(*field, "Y_VAL" | "GAIN") {
                2 * ifs
            } else {
                ifs
            };
            let unit = match *field {
                "Y_VAL" => "DEGREES",
                "SENS" => "K/JY",
                _ => "",
            };
            descriptors.push((format!("{field}_{hand}"), kind, width, unit));
        }
    }
    let hdu = create_table(
        fits,
        "AIPS GC",
        &descriptors
            .iter()
            .map(|(name, kind, count, unit)| (name.as_str(), *kind, *count, *unit))
            .collect::<Vec<_>>(),
    )?;
    for (name, value) in [
        ("EXTVER", 1),
        ("NO_POL", hands),
        ("NO_STKD", layout.correlations.len() as i32),
        ("STK_1", -5),
        ("NO_BAND", ifs as i32),
        ("NO_CHAN", layout.input_channels as i32),
        ("NO_TABS", 2),
        ("TABREV", 2),
    ] {
        hdu.write_key(fits, name, value).map_err(invalid)?;
    }
    for (name, value) in [
        ("REF_FREQ", layout.reference),
        ("CHAN_BW", layout.step.abs()),
        ("REF_PIXL", 1.0 + layout.pixel as f64),
    ] {
        hdu.write_key(fits, name, value).map_err(invalid)?;
    }
    hdu.write_key(fits, "OBSCODE", "").map_err(invalid)?;
    let (antennas, ids, values) = gain_cells(ms, layout, meta, rows, windows, ifs)?;
    put(&hdu, fits, "ANTENNA_NO", &antennas)?;
    put(&hdu, fits, "SUBARRAY", &vec![1i32; antennas.len()])?;
    put(&hdu, fits, "FREQ ID", &ids)?;
    let size = ifs * antennas.len();
    for hand in 1..=hands {
        for (name, value) in [("TYPE", 1), ("NTERM", 2), ("X_TYP", 0), ("Y_TYP", 3)] {
            put(&hdu, fits, &format!("{name}_{hand}"), &vec![value; size])?;
        }
        for (name, value, width) in [("X_VAL", 0.0f32, 1), ("GAIN", 1.0, 2), ("SENS", 1.0, 1)] {
            put(
                &hdu,
                fits,
                &format!("{name}_{hand}"),
                &vec![value; size * width],
            )?;
        }
        put(&hdu, fits, &format!("Y_VAL_{hand}"), &values)?;
    }
    Ok(())
}

type GainCells = (Vec<i32>, Vec<i32>, Vec<f32>);

fn gain_cells(
    ms: &MeasurementSet,
    layout: &OutputLayout,
    meta: &ExportHeader,
    rows: &[SysCal],
    windows: usize,
    ifs: usize,
) -> MsResult<GainCells> {
    let start_hour_angle = start_hour_angle(ms, layout, meta)?;
    let first = &rows[0];
    let last = rows
        .iter()
        .take_while(|row| row.antenna == first.antenna)
        .last()
        .unwrap_or(first);
    let hour_angle = |time: f64| {
        (start_hour_angle + 360.0 * (366.25 / 365.25) * (time - meta.time) / DAY) as f32
    };
    let span = [hour_angle(first.time), hour_angle(last.time)];
    let unique = rows.iter().map(|row| row.antenna).collect::<BTreeSet<_>>();
    let mut antennas = Vec::new();
    let mut ids = Vec::new();
    let mut values = Vec::new();
    for antenna in unique {
        let number = antenna_number(meta, antenna)?;
        for window in 1..=windows {
            antennas.push(number);
            ids.push(window as i32);
            for _ in 0..ifs {
                values.extend(span);
            }
        }
    }
    Ok((antennas, ids, values))
}

fn start_hour_angle(
    ms: &MeasurementSet,
    layout: &OutputLayout,
    meta: &ExportHeader,
) -> MsResult<f64> {
    let first = layout
        .rows
        .iter()
        .min_by_key(|row| row.index)
        .expect("UVFITS export has rows");
    let table = subtable(ms, SubtableId::Field)?;
    let ArrayValue::Float64(direction) = table
        .cell_accessor(first.field as usize, "DELAY_DIR")?
        .array()?
    else {
        return Err(invalid("Invalid delay direction in FIELD"));
    };
    let angles = [direction.get([0, 0]), direction.get([1, 0])];
    let [Some(&ra), Some(&dec)] = angles else {
        return Err(invalid("Invalid delay direction shape in FIELD"));
    };
    let reference = measure_ref(table, "DELAY_DIR")
        .parse::<DirectionRef>()
        .unwrap_or(DirectionRef::J2000);
    let position = meta
        .measures
        .as_ref()
        .and_then(|runtime| MPosition::casa_observatory(&meta.telescope, runtime.as_ref()))
        .unwrap_or_else(|| MPosition::new_itrf(0.0, 0.0, 0.0));
    let frame = meta
        .frame(meta.time, meta.epoch_reference)?
        .with_position(position);
    let direction = MDirection::from_angles(ra, dec, reference)
        .convert_to(DirectionRef::HADEC, &frame)
        .map_err(invalid)?;
    let value = direction.as_angles().0;
    Ok(value.sin().atan2(value.cos()).to_degrees())
}

fn antenna_number(meta: &ExportHeader, id: i32) -> MsResult<i32> {
    meta.antenna_numbers
        .get(id as usize)
        .copied()
        .ok_or_else(|| invalid("Invalid UVFITS antenna identifier"))
}
