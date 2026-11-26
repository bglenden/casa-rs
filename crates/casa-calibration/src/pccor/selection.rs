// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antenna, spectral-window, and scan selections for pulse calibration.

use std::collections::BTreeSet;

use casa_core::quanta::Quantity;
use casa_ms::MeasurementSet;
use casa_tables::RequiredScalarColumnValues::{Float64, Int32};
use time::Date;

use super::{PccorError, PccorRequest};

pub(super) struct Selection {
    pub(super) antenna_names: Vec<String>,
    pub(super) antennas: Vec<i32>,
    pub(super) reference: i32,
    pub(super) windows: Vec<i32>,
    pub(super) scans: Vec<Scan>,
    pub(super) interval: [f64; 2],
}

pub(super) struct Scan {
    pub(super) start: f64,
    pub(super) number: i32,
    pub(super) field: i32,
}

impl Selection {
    pub(super) fn read(ms: &MeasurementSet, request: &PccorRequest) -> Result<Self, PccorError> {
        let antenna = ms.antenna()?;
        let antenna_names = (0..antenna.row_count())
            .map(|row| antenna.name(row))
            .collect::<Result<Vec<_>, _>>()?;
        let antennas = select_ids(
            &request.antenna,
            antenna_names.len() as i32,
            "antennas",
            |name| antenna_id(name, &antenna_names),
        )?;
        let reference = antenna_id(&request.refant, &antenna_names)?;
        if !antennas.contains(&reference) {
            return Err(PccorError::Execution(
                "Reference antenna must be included in processing antennas in PCCOR".into(),
            ));
        }
        let windows = select_windows(&request.spw, ms.spectral_window()?.row_count() as i32)?;
        let (scans, final_time) = read_scans(ms)?;
        let interval = solution_interval(request, &scans, final_time)?;
        Ok(Self {
            antenna_names,
            antennas,
            reference,
            windows,
            scans,
            interval,
        })
    }

    pub(super) fn scan_at(&self, time: f64) -> Option<&Scan> {
        self.scans.iter().rfind(|scan| time >= scan.start)
    }

    pub(super) fn pairs(&self) -> BTreeSet<(i32, i32)> {
        self.antennas
            .iter()
            .flat_map(|&antenna| self.windows.iter().map(move |&window| (antenna, window)))
            .collect()
    }
}

fn antenna_id(name: &str, antenna_names: &[String]) -> Result<i32, PccorError> {
    antenna_names
        .iter()
        .position(|candidate| candidate == name)
        .map(|index| index as i32)
        .ok_or_else(|| {
            PccorError::Selection(format!("Antenna {name} is not available in this dataset"))
        })
}

fn select_windows(text: &str, count: i32) -> Result<Vec<i32>, PccorError> {
    let parse = |text: &str| {
        text.trim()
            .parse::<i32>()
            .map_err(|_| PccorError::Selection(format!("{text} is not a valid SPW specification")))
    };
    if !text.contains('!')
        && let Some((start, tail)) = text.split_once('~')
    {
        let stop = tail.split_once('~').map_or(tail, |(stop, _)| stop);
        return Ok((parse(start)?.max(0)..=parse(stop)?.min(count - 1)).collect());
    }
    select_ids(text, count, "SPWs", |part| {
        let id = parse(part)?;
        if (0..count).contains(&id) {
            return Ok(id);
        }
        Err(PccorError::Selection(format!(
            "{part} is not a valid SPW specification"
        )))
    })
}

fn select_ids(
    text: &str,
    count: i32,
    axis: &str,
    parse: impl Fn(&str) -> Result<i32, PccorError>,
) -> Result<Vec<i32>, PccorError> {
    if text == "none" {
        return Ok((0..count).collect());
    }
    if !text.contains('!') {
        return text.split(',').map(|part| parse(part.trim())).collect();
    }
    let mut selected: Vec<_> = (0..count).collect();
    for part in text.split(',') {
        let Some(excluded) = part.strip_prefix('!') else {
            return Err(PccorError::Selection(format!(
                "Mixing of {axis} to include and {axis} to exclude in the same list is not allowed",
            )));
        };
        let id = parse(excluded.trim())?;
        let index = selected
            .iter()
            .position(|selected| *selected == id)
            .ok_or_else(|| PccorError::Selection(format!("{excluded} is already excluded")))?;
        selected.remove(index);
    }
    Ok(selected)
}

fn read_scans(ms: &MeasurementSet) -> Result<(Vec<Scan>, f64), PccorError> {
    let mut columns =
        ms.main_table()
            .required_scalar_columns_owned(&["TIME", "SCAN_NUMBER", "FIELD_ID"])?;
    let (Some(Float64(times)), Some(Int32(numbers)), Some(Int32(fields))) = (
        columns.remove("TIME"),
        columns.remove("SCAN_NUMBER"),
        columns.remove("FIELD_ID"),
    ) else {
        return Err(PccorError::Execution(
            "Invalid MeasurementSet scan columns".into(),
        ));
    };
    let mut scans = Vec::new();
    let mut current = 0;
    for ((&start, number), field) in times.iter().zip(numbers).zip(fields) {
        if number != current {
            scans.push(Scan {
                start,
                number,
                field,
            });
            current = number;
        }
    }
    let final_time = times
        .last()
        .copied()
        .filter(|_| !scans.is_empty())
        .ok_or_else(|| PccorError::Execution("MeasurementSet contains no scans".into()))?;
    Ok((scans, final_time))
}

fn solution_interval(
    request: &PccorRequest,
    scans: &[Scan],
    final_time: f64,
) -> Result<[f64; 2], PccorError> {
    let scan = (request.scan != "none")
        .then(|| parse_scan(&request.scan, scans, final_time))
        .transpose()?;
    let range = (request.timerange != "none")
        .then(|| parse_timerange(&request.timerange, scans[0].start))
        .transpose()?;
    if let (Some([scan_start, scan_stop]), Some([start, stop])) = (scan, range)
        && (stop < scan_start || start > scan_stop)
    {
        return Err(PccorError::Selection(
            "timerange is incompatible with scan times".into(),
        ));
    }
    let [start, stop] = range.or(scan).ok_or_else(|| {
        PccorError::Selection("Scan and timerange cannot be both undefined".into())
    })?;
    if stop < scans[0].start || start > final_time {
        return Err(PccorError::Selection(
            "Timerange is outside observation times".into(),
        ));
    }
    Ok([start, stop])
}

fn parse_scan(text: &str, scans: &[Scan], final_time: f64) -> Result<[f64; 2], PccorError> {
    let number = text
        .trim()
        .parse::<i32>()
        .map_err(|_| PccorError::Selection(format!("{text} is not a valid single scan number")))?;
    let index = scans
        .iter()
        .position(|scan| scan.number == number)
        .ok_or_else(|| {
            PccorError::Selection(format!("Selected scan {number} is not present in the ms"))
        })?;
    Ok([
        scans[index].start,
        scans.get(index + 1).map_or(final_time, |scan| scan.start),
    ])
}

fn parse_timerange(text: &str, first: f64) -> Result<[f64; 2], PccorError> {
    let Some((start, stop)) = text.split_once('~').filter(|(_, stop)| !stop.contains('~')) else {
        return Err(PccorError::Selection(
            "timerange must contain one and only one <~> to separate start and finish times".into(),
        ));
    };
    // PCCOR sends both bounds through measures. The nested fringefit still
    // uses the MeasurementSet expression grammar for its own selection.
    let date = if start.contains('/') {
        start.split('/').take(3).collect::<Vec<_>>().join("/")
    } else {
        let first_day = Date::from_julian_day(2_400_001 + (first / 86400.).floor() as i32)
            .map_err(|error| PccorError::Selection(error.to_string()))?;
        format!(
            "{}/{:02}/{:02}",
            first_day.year(),
            u8::from(first_day.month()),
            first_day.day()
        )
    };
    let seconds = |bound: &str, label: &str| {
        let qualified = if bound.contains('/') {
            bound.to_owned()
        } else {
            format!("{date}/{bound}")
        };
        Quantity::read(&qualified)
            .ok()
            .filter(|value| value.unit().name() == "d" && value.value().is_finite())
            .map(|value| value.value() * 86400.)
            .ok_or_else(|| PccorError::Selection(format!("{label} time in timerange is invalid")))
    };
    Ok([seconds(start, "Start")?, seconds(stop, "Stop")?])
}
