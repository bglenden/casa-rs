// SPDX-License-Identifier: LGPL-3.0-or-later
//! The VLA baseline-correction archive and antenna move history.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use casa_core::{ArrayValue, ScalarValue, Value, quanta::time::MvTime};
use casa_ms::{MeasurementSet, schema::SubtableId};

use super::{GencalError, GencalRequest, get_string};

const ARCHIVE: &str = "www.vla.nrao.edu/cgi-bin/evlais_blines.cgi?Year=";
const FIRST_EVLA_YEAR: i32 = 2010;
const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];

struct Antenna {
    number: i32,
    name: String,
    station: String,
    offset: [f64; 3],
    moved_after_observation: bool,
}

struct Correction<'a> {
    moved: Option<MvTime>,
    posted: MvTime,
    number: i32,
    station: &'a str,
    offset: [f64; 3],
}

pub(super) fn resolve(
    ms: &MeasurementSet,
    request: &GencalRequest,
    time_limit: i64,
) -> Result<Option<GencalRequest>, GencalError> {
    let observation = ms
        .subtable(SubtableId::Observation)
        .ok_or_else(|| invalid("missing OBSERVATION subtable"))?;
    match observation.row_count() {
        0 => return Ok(None),
        1 => {}
        _ => return Err(GencalError::AutomaticPositionInput(
            "The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()".to_string(),
        )),
    }
    let telescope = get_string(observation, 0, "TELESCOPE_NAME")?;
    if telescope != "VLA" && telescope != "EVLA" {
        return Ok(None);
    }
    let start = observation_start(ms)?;
    if start.year() < FIRST_EVLA_YEAR {
        return Ok(None);
    }
    let mut antennas = load_antennas(ms)?;
    let agent = crate::calibration_service::agent(Duration::from_secs(30)).map_err(invalid)?;
    let query = |scheme, year| agent.get(format!("{scheme}://{ARCHIVE}{year}")).call();
    let scheme = match query("http", FIRST_EVLA_YEAR) {
        Ok(_) => "http",
        // Some institutional proxies permit CONNECT only for HTTPS. The
        // archive serves the same data through the encrypted endpoint.
        Err(ureq::Error::Io(_)) if query("https", FIRST_EVLA_YEAR).is_ok() => "https",
        Err(_) => return Ok(None),
    };
    let unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(invalid)?
        .as_secs_f64();
    let year = MvTime::from_mjd_seconds(unix_seconds + 40_587.0 * 86_400.0).year();
    // CASA's archive comparison is based on its whole-second YMD string.
    let observed = MvTime::from_mjd_seconds(start.as_mjd_seconds().round());
    for correction_year in FIRST_EVLA_YEAR..=year {
        let failure = |source| GencalError::VlaArchive {
            url: format!("{scheme}://{ARCHIVE}{correction_year}"),
            source: Box::new(source),
        };
        let mut response = query(scheme, correction_year).map_err(failure)?;
        let text = response.body_mut().read_to_string().map_err(failure)?;
        accumulate(&mut antennas, observed, time_limit, correction_year, &text)?;
    }
    let mut selected = Vec::new();
    let mut offsets = Vec::new();
    for antenna in antennas {
        if antenna.offset != [0.0; 3] {
            selected.push(antenna.name);
            offsets.extend(antenna.offset);
        }
    }
    if selected.is_empty() && affected_tropo_epoch(start) {
        // CASA generates a zero-offset product to carry the correction keyword.
        selected.push("0".into());
        offsets.extend([0.0; 3]);
    }
    if selected.is_empty() {
        return Ok(None);
    }
    let mut resolved = request.clone();
    resolved.antenna = selected.join(",");
    resolved.parameter = offsets;
    Ok(Some(resolved))
}

pub(super) fn observation_start(ms: &MeasurementSet) -> Result<MvTime, GencalError> {
    let observation = ms
        .subtable(SubtableId::Observation)
        .ok_or_else(|| invalid("missing OBSERVATION subtable"))?;
    let value = observation
        .cell_accessor(0, "TIME_RANGE")
        .and_then(|cell| cell.array())
        .map_err(invalid)?;
    let ArrayValue::Float64(times) = value else {
        return Err(invalid("OBSERVATION.TIME_RANGE must contain UTC times"));
    };
    let start = times
        .iter()
        .next()
        .copied()
        .filter(|time| time.is_finite())
        .ok_or_else(|| invalid("missing observation start time"))?;
    Ok(MvTime::from_mjd_seconds(start))
}

pub(super) fn affected_tropo_epoch(time: MvTime) -> bool {
    let mjd = time.as_mjd_days();
    mjd > 57_609.0 && mjd < 57_707.0
}

pub(super) fn tropo_keyword(
    ms: &MeasurementSet,
    request: &GencalRequest,
    mode: crate::output::CalibrationOutputMode,
) -> Result<Option<Value>, GencalError> {
    if crate::antpos_vla::only_vla_observations(ms).map_err(invalid)?
        && affected_tropo_epoch(observation_start(ms)?)
    {
        return Ok(Some(Value::Scalar(ScalarValue::Float64(1.0))));
    }
    if mode == crate::output::CalibrationOutputMode::Update && request.output_table.exists() {
        let previous = crate::output_merge::open(&request.output_table)?;
        return Ok(previous
            .keywords()
            .get(crate::antpos_vla::TROPO_KEYWORD)
            .cloned());
    }
    Ok(None)
}

fn load_antennas(ms: &MeasurementSet) -> Result<Vec<Antenna>, GencalError> {
    let antennas = ms.antenna().map_err(invalid)?;
    (0..antennas.row_count())
        .map(|row| {
            let name = antennas.name(row).map_err(invalid)?;
            let number = name
                .get(2..)
                .and_then(|suffix| suffix.parse().ok())
                .ok_or_else(|| invalid(format!("invalid EVLA antenna name {name:?}")))?;
            let station = antennas.station(row).map_err(invalid)?;
            Ok(Antenna {
                number,
                name,
                station,
                offset: [0.0; 3],
                moved_after_observation: false,
            })
        })
        .collect()
}

fn accumulate(
    antennas: &mut [Antenna],
    observed: MvTime,
    time_limit: i64,
    year: i32,
    text: &str,
) -> Result<(), GencalError> {
    let observed = observed.as_mjd_days();
    for line in text.lines().filter(|line| {
        !line.starts_with(['<', ';']) && MONTHS.iter().any(|month| line.contains(month))
    }) {
        let correction = parse_correction(year, line)?;
        let Some(antenna) = antennas
            .iter_mut()
            .find(|antenna| antenna.number == correction.number)
        else {
            continue;
        };
        if antenna.moved_after_observation {
            continue;
        }
        if let Some(moved) = correction.moved {
            if moved.as_mjd_days() > observed {
                antenna.moved_after_observation = true;
                continue;
            }
            antenna.offset = [0.0; 3];
        }
        let posted = correction.posted.as_mjd_days();
        if posted > observed
            && correction.station == antenna.station
            && (time_limit <= 0 || posted.floor() - observed.floor() < time_limit as f64)
        {
            for (total, delta) in antenna.offset.iter_mut().zip(correction.offset) {
                *total += delta;
            }
        }
    }
    Ok(())
}

fn parse_correction(year: i32, line: &str) -> Result<Correction<'_>, GencalError> {
    let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
    let parsed = || {
        let (moved, fields) = if fields.len() == 9 {
            (Some(parse_date(year, fields[0], "00:00")?), &fields[1..])
        } else {
            (None, fields.as_slice())
        };
        let [_, posted, clock, number, station, x, y, z] = fields else {
            return None;
        };
        Some(Correction {
            moved,
            posted: parse_date(year, posted, clock)?,
            number: number.parse().ok()?,
            station,
            offset: [x.parse().ok()?, y.parse().ok()?, z.parse().ok()?],
        })
    };
    parsed().ok_or_else(|| invalid(format!("invalid VLA baseline correction row: {line}")))
}

fn parse_date(year: i32, date: &str, clock: &str) -> Option<MvTime> {
    let month = MONTHS.iter().position(|month| date.starts_with(month))? as u32 + 1;
    let day = date.get(3..)?.parse().ok()?;
    let (hour, minute) = clock.split_once(':')?;
    // Historic archive rows can label posting times past midnight (25:51).
    // CASA adds that elapsed time to the posted calendar day.
    let day = MvTime::from_ymd_hms_utc(year, month, day, 0, 0, 0.0)?;
    let hours: f64 = hour.parse::<i32>().ok()?.into();
    let minutes: f64 = minute.parse::<i32>().ok()?.into();
    Some(MvTime::from_mjd_days(
        day.as_mjd_days() + (hours * 60.0 + minutes) / 1440.0,
    ))
}

fn invalid(detail: impl std::fmt::Display) -> GencalError {
    GencalError::InvalidRequest(format!("VLA baseline corrections: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_matching_posted_changes_before_next_move_and_time_limit() {
        let observed = parse_date(2010, "JUL17", "12:00").unwrap();
        let mut antennas = [Antenna {
            number: 1,
            name: "ea01".into(),
            station: "W09".into(),
            offset: [0.0; 3],
            moved_after_observation: false,
        }];
        let lines = "
            JUL16 JUL16 JUL16 11:00 1 W09 1 1 1
            JUL17 JUL18 21:44 1 W09 0 .003 0
            JUL17 JUL18 21:44 1 W08 1 1 1
            JUL17 JUL19 21:44 1 W09 1 1 1
            JUL20 JUL20 JUL20 21:44 1 W09 1 1 1
            JUL17 JUL21 21:44 1 W09 1 1 1";
        accumulate(&mut antennas, observed, 2, 2010, lines).unwrap();
        assert_eq!(antennas[0].offset, [0.0, 0.003, 0.0]);
        assert!(antennas[0].moved_after_observation);
    }

    #[test]
    fn parses_the_live_archives_posting_times_past_midnight() {
        let extended = parse_date(2016, "SEP26", "25:51").unwrap();
        assert_eq!(extended, parse_date(2016, "SEP27", "01:51").unwrap());
    }
}
