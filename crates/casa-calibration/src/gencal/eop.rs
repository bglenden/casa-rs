// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibrate a MeasurementSet from the change between recorded and updated EOPs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, RecordField, RecordValue, Value};
use casa_ms::MeasurementSet;
use casa_ms::derived::engine::MsCalEngine;
use casa_tables::RequiredScalarColumnValues::{Float64, Int32};
use casa_tables::{Table, TableOptions};
use sofars::{
    erst::era00,
    pnp::{c2i00a, c2tcio, pom00, sp00},
    ts::{taitt, taiutc, utctai},
};

use super::{
    GencalError, GencalReport, GencalRequest, PriorCalRow, f32_column, open_measurement_set,
    refuse_prior_input_overlap, write_prior_caltable,
};
use crate::constants::{COL_PARAMERR, COL_WEIGHT};
use crate::output::{CalibrationOutput, CalibrationOutputMode};

const DAY: f64 = 86400.;
const JD_AT_MJD_ZERO: f64 = 2400000.5;
const ARCSECOND: f64 = std::f64::consts::PI / (180. * 3600.);
const DELTA: f64 = 60.;

#[derive(Clone, Copy)]
struct Eop {
    mjd: f64,
    xp_radians: f64,
    yp_radians: f64,
    ut1_utc: f64,
}

pub(super) fn generate(
    request: &GencalRequest,
    source: Option<&Path>,
    mode: CalibrationOutputMode,
) -> Result<GencalReport, GencalError> {
    let ms = open_measurement_set(&request.measurement_set)?;
    let source = explicit_source(source);
    let installed = if source.is_none() {
        installed_source()
    } else {
        None
    };
    if let Some(path) = source.or(installed.as_deref()) {
        refuse_prior_input_overlap(request, path)?;
    }
    let recorded = request.measurement_set.join("EARTH_ORIENTATION");
    refuse_prior_input_overlap(request, &recorded)?;
    let old = recorded_eop(&request.measurement_set);
    let rows = if let Some(old) = old {
        build_rows(&ms, &old, source, installed.as_deref())?
    } else {
        tracing::warn!(vis = %request.measurement_set.display(), "No recorded Earth orientation");
        Vec::new()
    };
    if rows.is_empty() {
        return Ok(GencalReport {
            output_table: request.output_table.clone(),
            caltype: request.caltype,
            table_subtype: "Fringe Jones".into(),
            row_count: 0,
            spectral_window_ids: Vec::new(),
            antenna_ids: Vec::new(),
        });
    }
    let mode = if mode == CalibrationOutputMode::Update {
        CalibrationOutputMode::Replace
    } else {
        mode
    };
    let output = CalibrationOutput::prepare(&request.output_table, mode)?;
    write_prior_caltable(&ms, request, "Fringe Jones", &rows, output, mode, true)
}

fn explicit_source(source: Option<&Path>) -> Option<&Path> {
    source.filter(|path| !path.as_os_str().is_empty() && *path != Path::new("None"))
}

fn installed_source() -> Option<PathBuf> {
    let runtime = casa_measures::MeasuresRuntime::open_discovered(Default::default()).ok()?;
    Some(runtime.root().join("geodetic/IERSeop2000"))
}

pub(super) fn input_paths(ms: &Path, source: Option<&Path>) -> Vec<PathBuf> {
    let mut paths = vec![ms.join("EARTH_ORIENTATION")];
    paths.extend(
        explicit_source(source)
            .map(Path::to_path_buf)
            .or_else(installed_source),
    );
    paths
}

fn recorded_eop(path: &Path) -> Option<BTreeMap<i32, Vec<Eop>>> {
    let table = Table::open(TableOptions::new(path.join("EARTH_ORIENTATION"))).ok()?;
    let scalars = table
        .required_scalar_columns_owned(&["TIME", "OBSERVATION_ID", "UT1_UTC"])
        .ok()?;
    let (Float64(times), Int32(observations), Float64(dut1)) = (
        &scalars["TIME"],
        &scalars["OBSERVATION_ID"],
        &scalars["UT1_UTC"],
    ) else {
        return None;
    };
    let mut result = BTreeMap::<i32, Vec<Eop>>::new();
    let mut invalid_observations = BTreeSet::new();
    for (index, &time) in times.iter().enumerate() {
        let observation = observations[index];
        if let Some(row) = recorded_sample(&table, index, time, dut1[index]) {
            result.entry(observation).or_default().push(row);
        } else {
            invalid_observations.insert(observation);
        }
    }
    for observation in invalid_observations {
        result.remove(&observation);
        tracing::warn!(
            observation,
            "Invalid recorded Earth orientation for observation"
        );
    }
    for rows in result.values_mut() {
        sort_epochs(rows);
    }
    Some(result)
}

fn recorded_sample(table: &Table, row: usize, time: f64, ut1_utc: f64) -> Option<Eop> {
    let ArrayValue::Float64(pm) = table.cell_accessor(row, "PM").ok()?.array().ok()? else {
        return None;
    };
    let xp_radians = *pm.get(ndarray::IxDyn(&[0]))?;
    let yp_radians = *pm.get(ndarray::IxDyn(&[1]))?;
    [time, ut1_utc, xp_radians, yp_radians]
        .into_iter()
        .all(f64::is_finite)
        .then_some(Eop {
            mjd: time / DAY,
            xp_radians,
            yp_radians,
            ut1_utc,
        })
}

fn updated_eop(
    source: Option<&Path>,
    installed: Option<&Path>,
    start: f64,
    end: f64,
) -> Result<Vec<Eop>, GencalError> {
    let mut result = if let Some(path) = source {
        let text = std::fs::read_to_string(path).map_err(|source| GencalError::EopFile {
            path: path.to_path_buf(),
            source,
        })?;
        parse_file(&text, start, end).unwrap_or_default()
    } else if let Some(path) = installed {
        read_runtime(path, start, end).unwrap_or_default()
    } else {
        Vec::new()
    };
    sort_epochs(&mut result);
    Ok(result)
}

fn read_runtime(path: &Path, start: f64, end: f64) -> Option<Vec<Eop>> {
    let table = Table::open(TableOptions::new(path)).ok()?;
    let columns = table
        .required_scalar_columns_owned(&["MJD", "x", "y", "dUT1"])
        .ok()?;
    let (Float64(mjd), Float64(x), Float64(y), Float64(dut1)) = (
        &columns["MJD"],
        &columns["x"],
        &columns["y"],
        &columns["dUT1"],
    ) else {
        return None;
    };
    Some(
        (0..mjd.len())
            .filter(|&i| mjd[i] > start - 0.5 && mjd[i] < end + 0.5)
            .map(|i| Eop {
                mjd: mjd[i],
                xp_radians: x[i] * ARCSECOND,
                yp_radians: y[i] * ARCSECOND,
                ut1_utc: dut1[i],
            })
            .collect(),
    )
}

fn parse_file(text: &str, start: f64, end: f64) -> Option<Vec<Eop>> {
    let mut lines = text.lines();
    let header = lines.next()?;
    let usno = if header.starts_with("EOP-MOD Ver 2.0") {
        Some((10..17, 18..25, 26..35))
    } else if header.starts_with("EOP-MOD Ver 2.1") {
        Some((10..18, 19..27, 28..39))
    } else {
        None
    };
    let mut result = Vec::new();
    for line in lines {
        if line.starts_with('#')
            || (usno.is_none() && (line.is_empty() || line.starts_with(char::is_whitespace)))
        {
            continue;
        }
        let read = |range: std::ops::Range<usize>| {
            line.get(range.start..range.end.min(line.len()))?
                .trim()
                .parse::<f64>()
                .ok()
        };
        let mjd = if usno.is_some() {
            read(0..9)? - JD_AT_MJD_ZERO
        } else {
            f64::from(line.get(12..19)?.trim().parse::<i32>().ok()?)
        };
        if !(start..=end).contains(&mjd) {
            continue;
        }
        let entry = if let Some((x, y, dut1)) = &usno {
            let tai_jd = mjd + JD_AT_MJD_ZERO;
            let (day, fraction) = taiutc(tai_jd, 0.).ok()?;
            let utc_tai = (fraction + (day - tai_jd)) * DAY;
            Eop {
                mjd: fraction + (day - JD_AT_MJD_ZERO),
                xp_radians: read(x.clone())? / 10. * ARCSECOND,
                yp_radians: read(y.clone())? / 10. * ARCSECOND,
                ut1_utc: read(dut1.clone())? / 1e6 - utc_tai,
            }
        } else {
            Eop {
                mjd,
                xp_radians: read(20..30)? * ARCSECOND,
                yp_radians: read(31..41)? * ARCSECOND,
                ut1_utc: read(42..53)?,
            }
        };
        if [entry.mjd, entry.xp_radians, entry.yp_radians, entry.ut1_utc]
            .into_iter()
            .any(|value| !value.is_finite())
        {
            return None;
        }
        result.push(entry);
    }
    Some(result)
}

fn sort_epochs(rows: &mut Vec<Eop>) {
    rows.sort_by(|a, b| a.mjd.total_cmp(&b.mjd));
    rows.dedup_by(|a, b| a.mjd == b.mjd);
}

#[derive(Default)]
struct Scan {
    antennas: BTreeSet<i32>,
    fields: BTreeSet<i32>,
    windows: BTreeSet<i32>,
    times: BTreeSet<u64>,
}

fn scans(ms: &MeasurementSet) -> Result<BTreeMap<(i32, i32), Scan>, GencalError> {
    let columns = ms
        .main_table()
        .required_scalar_columns_owned(&[
            "ANTENNA1",
            "ANTENNA2",
            "FIELD_ID",
            "DATA_DESC_ID",
            "OBSERVATION_ID",
            "SCAN_NUMBER",
            "TIME",
        ])
        .map_err(invalid)?;
    let (
        Int32(ant1),
        Int32(ant2),
        Int32(fields),
        Int32(descriptions),
        Int32(obs),
        Int32(scans),
        Float64(times),
    ) = (
        &columns["ANTENNA1"],
        &columns["ANTENNA2"],
        &columns["FIELD_ID"],
        &columns["DATA_DESC_ID"],
        &columns["OBSERVATION_ID"],
        &columns["SCAN_NUMBER"],
        &columns["TIME"],
    )
    else {
        return Err(invalid("invalid MAIN columns for Earth orientation"));
    };
    let dd = ms.data_description().map_err(invalid)?;
    let windows = (0..dd.row_count())
        .map(|row| dd.spectral_window_id(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid)?;
    let mut result = BTreeMap::<_, Scan>::new();
    for row in 0..times.len() {
        let window = windows
            .get(descriptions[row] as usize)
            .ok_or_else(|| invalid("invalid DATA_DESC_ID"))?;
        let scan = result.entry((obs[row], scans[row])).or_default();
        scan.antennas.extend([ant1[row], ant2[row]]);
        scan.fields.insert(fields[row]);
        scan.windows.insert(*window);
        scan.times.insert(times[row].to_bits());
    }
    Ok(result)
}

fn sample_times(scan: &Scan) -> Vec<f64> {
    let mut values = scan
        .times
        .iter()
        .map(|&bits| f64::from_bits(bits))
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let mut knots = Vec::new();
    for &time in &values {
        if knots.last().is_none_or(|last| time - last > DELTA) {
            knots.push(time);
        }
    }
    if values.last() != knots.last() {
        knots.extend(values.last());
    }
    knots
}

fn phase_directions(ms: &MeasurementSet) -> Result<Vec<[f64; 3]>, GencalError> {
    let fields = ms.field().map_err(invalid)?;
    let mut engine = None;
    (0..fields.row_count())
        .map(|row| {
            let (lon, lat) = if fields
                .ephemeris_id(row)
                .map_err(invalid)?
                .is_some_and(|id| id >= 0)
            {
                let engine = match &mut engine {
                    Some(engine) => engine,
                    empty => empty.insert(MsCalEngine::for_calibration(ms).map_err(invalid)?),
                };
                let time = fields.time(row).map_err(invalid)?;
                engine
                    .reference_phase_direction(row, time)
                    .map_err(invalid)?
                    .as_angles()
            } else {
                let ArrayValue::Float64(array) = fields.phase_dir(row).map_err(invalid)? else {
                    return Err(invalid("invalid PHASE_DIR"));
                };
                let lon = *array
                    .get(ndarray::IxDyn(&[0, 0]))
                    .ok_or_else(|| invalid("invalid PHASE_DIR"))?;
                let lat = *array
                    .get(ndarray::IxDyn(&[1, 0]))
                    .ok_or_else(|| invalid("invalid PHASE_DIR"))?;
                (lon, lat)
            };
            Ok([lat.cos() * lon.cos(), lat.cos() * lon.sin(), lat.sin()])
        })
        .collect()
}

fn build_rows(
    ms: &MeasurementSet,
    recorded: &BTreeMap<i32, Vec<Eop>>,
    source: Option<&Path>,
    installed: Option<&Path>,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let scans = scans(ms)?;
    let antennas = ms.antenna().map_err(invalid)?;
    let positions = (0..antennas.row_count())
        .map(|row| antennas.position(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid)?;
    let directions = phase_directions(ms)?;
    let spectral = ms.spectral_window().map_err(invalid)?;
    let frequency = (0..spectral.row_count())
        .map(|row| super::spw_center_frequency_hz(&spectral, row))
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = Vec::new();
    for (&observation, old) in recorded {
        let Some((first, last)) = old.first().zip(old.last()) else {
            continue;
        };
        let updated = updated_eop(source, installed, first.mjd, last.mjd)?;
        if old.len() < 2 || updated.len() < 2 {
            tracing::warn!(
                observation,
                start_mjd = first.mjd,
                end_mjd = last.mjd,
                "Insufficient Earth orientation samples for observation"
            );
            continue;
        }
        for (_, scan) in scans.range((observation, i32::MIN)..=(observation, i32::MAX)) {
            let times = sample_times(scan);
            let rotations = times
                .iter()
                .map(|&time| {
                    let at = |t| difference(old, &updated, t);
                    Ok([at(time - DELTA)?, at(time)?, at(time + DELTA)?])
                })
                .collect::<Result<Vec<_>, GencalError>>()?;
            for &antenna in &scan.antennas {
                let pos = positions
                    .get(antenna as usize)
                    .ok_or_else(|| invalid("unknown ANTENNA_ID"))?;
                for &field in &scan.fields {
                    let ray = directions
                        .get(field as usize)
                        .ok_or_else(|| invalid("unknown FIELD_ID"))?;
                    for (&time, transform) in times.iter().zip(&rotations) {
                        let [before, delay, after] =
                            transform.map(|matrix| delay_for(&matrix, pos, ray));
                        let rate = (after - before) / (2. * DELTA);
                        for &window in &scan.windows {
                            let hz = frequency
                                .get(window as usize)
                                .ok_or_else(|| invalid("unknown spectral window"))?;
                            rows.push(row(
                                (antenna, field, window, observation),
                                time,
                                *hz,
                                delay,
                                rate,
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(rows)
}

type Matrix = [[f64; 3]; 3];

fn difference(old: &[Eop], updated: &[Eop], time: f64) -> Result<Matrix, GencalError> {
    let mjd = time / DAY;
    let tai = utctai(JD_AT_MJD_ZERO, mjd).map_err(|_| invalid("invalid UTC epoch"))?;
    let (tt1, tt2) = taitt(tai.0, tai.1).map_err(|_| invalid("invalid TAI epoch"))?;
    // The precession-nutation matrix and TIO locator depend only on TT.
    let rc2i = c2i00a(tt1, tt2);
    let sp = sp00(tt1, tt2);
    let transform = |samples: &[Eop]| {
        let end = samples
            .partition_point(|sample| sample.mjd <= mjd)
            .clamp(1, samples.len() - 1);
        let (a, b) = (samples[end - 1], samples[end]);
        let alpha = (mjd - a.mjd) / (b.mjd - a.mjd);
        let delta_ut1 = b.ut1_utc - a.ut1_utc;
        let ut1 = a.ut1_utc + alpha * (delta_ut1 - delta_ut1.round_ties_even());
        let xp = a.xp_radians + alpha * (b.xp_radians - a.xp_radians);
        let yp = a.yp_radians + alpha * (b.yp_radians - a.yp_radians);
        let era = era00(JD_AT_MJD_ZERO + mjd, ut1 / DAY);
        c2tcio(&rc2i, era, &pom00(xp, yp, sp))
    };
    let a = transform(old);
    let b = transform(updated);
    Ok(std::array::from_fn(|i| {
        std::array::from_fn(|j| b[i][j] - a[i][j])
    }))
}

fn delay_for(rot: &Matrix, pos: &[f64; 3], ray: &[f64; 3]) -> f64 {
    rot.iter()
        .zip(pos)
        .map(|(line, &distance)| {
            distance
                * line
                    .iter()
                    .zip(ray)
                    .map(|(value, direction)| value * direction)
                    .sum::<f64>()
        })
        .sum::<f64>()
        / 299_792_458.
}

fn row(
    (antenna, field, window, observation): (i32, i32, i32, i32),
    time: f64,
    hz: f64,
    delay: f64,
    rate: f64,
) -> PriorCalRow {
    let phase = (std::f64::consts::TAU * delay * hz) as f32;
    let ns = (delay * 1.0e9) as f32;
    let cells = [phase, ns, rate as f32, 0., phase, ns, rate as f32, 0.];
    PriorCalRow {
        time_seconds: time,
        interval_seconds: 0.,
        field_id: field,
        spw_id: window,
        antenna_id: antenna,
        observation_id: observation,
        scan_number: -1,
        parameters: f32_column(&cells),
        snr_value: 1.,
        preserved: Some(RecordValue::new(vec![
            RecordField::new(COL_PARAMERR, Value::Array(f32_column(&[-1.; 8]))),
            RecordField::new(COL_WEIGHT, Value::Array(f32_column(&[1.; 8]))),
        ])),
    }
}

fn invalid(error: impl std::fmt::Display) -> GencalError {
    GencalError::InvalidRequest(format!("Earth orientation calibration: {error}"))
}
