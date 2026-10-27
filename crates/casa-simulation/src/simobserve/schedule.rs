// SPDX-License-Identifier: LGPL-3.0-or-later
//! The calibrator and the observing schedule: `refdate`, `hourangle` and
//! `totaltime` read as CASA reads them, and the passes over the map laid out
//! in time, the calibrator observed before the first and after each.

use std::path::PathBuf;

use casa_components::{
    ComponentList, ComponentShape, Flux, SkyComponent, SpectralKind, SpectralModel,
};
use casa_types::measures::direction::MDirection;
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

use super::pointings::Pointings;
use super::{Run, SimobserveError, python, remove};
use crate::simutil::direction::{direction_measure, is_direction};
use crate::simutil::skymodel::SkyModelError;
use crate::simutil::{SimutilError, qa};

/// The frequency `componentlist.addcomponent` gives a component when none
/// is given, LSRK.
const ADDCOMPONENT_FREQUENCY_HZ: f64 = 1.42e9;

/// The phase calibrator observed between passes over the map.
pub(super) struct Calibrator {
    /// `caldirection`.
    pub(super) direction: String,
    /// `<stem>.cal.cclist`: the calibrator as a point of `calflux`.
    pub(super) list: PathBuf,
}

/// The calibrator, when `calflux` is positive, `caldirection` is set and
/// the array is an interferometer; its component list is written.
pub(super) fn calibrator(run: &mut Run<'_>) -> Result<Option<Calibrator>, SimobserveError> {
    let parameters = run.parameters;
    if !qa::is_quantity(&parameters.calflux) {
        return Err(SimutilError::Quantity(parameters.calflux.clone()).into());
    }
    let flux_jy = qa::value_in(&qa::read(&parameters.calflux)?, "Jy")?;
    let mut direction = parameters.caldirection.clone();
    if direction.chars().count() < 4 {
        direction.clear();
    }
    if !(flux_jy > 0. && !direction.is_empty() && run.uvmode) {
        return Ok(None);
    }
    let path = run.file(".cal.cclist");
    remove(&path)?;
    if !is_direction(&direction) {
        return Err(SimutilError::Direction(direction).into());
    }
    let (refer, [longitude, latitude]) = direction_measure(&direction)?;
    let mut list = ComponentList::new();
    list.push(
        SkyComponent::new(
            Flux::stokes_jy(flux_jy, 0., 0., 0.),
            ComponentShape::point(MDirection::from_angles(longitude, latitude, refer)),
            SpectralModel::new(
                MFrequency::new(ADDCOMPONENT_FREQUENCY_HZ, FrequencyRef::LSRK),
                SpectralKind::Constant,
            ),
        )
        .with_label("phase calibrator"),
    );
    list.write_table(&path).map_err(SkyModelError::from)?;
    run.wrote(&path);
    Ok(Some(Calibrator {
        direction,
        list: path,
    }))
}

/// One pointing of a pass: the map's field `field` from `start_s` to
/// `stop_s`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct Target {
    pub(super) field: usize,
    pub(super) start_s: f64,
    pub(super) stop_s: f64,
}

/// A step of the schedule, as the task calls the session.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Step {
    /// `observemany` of the map's pointings.
    Targets(Vec<Target>),
    /// `observe` of the calibrator.
    Calibrator {
        /// The scan's start, s.
        start_s: f64,
        /// Its stop, s.
        stop_s: f64,
    },
}

/// The observation in time: the reference epoch and the steps.
#[derive(Debug, Clone)]
pub(super) struct Schedule {
    /// `refdate` at 00:00 UTC.
    pub(super) reference: MEpoch,
    /// The steps, in time order, in seconds from the transit of the first
    /// field at the reference date.
    pub(super) steps: Vec<Step>,
}

/// The schedule CASA's observing loop makes of `pointings` from
/// `refdate`, `hourangle` and `totaltime`, with a calibrator when
/// `calibrator`; but a pass the time ends inside is observed as far as the
/// time goes, with or without one (see the module documentation of
/// [`super`]).
pub(super) fn schedule(
    run: &Run<'_>,
    pointings: &Pointings,
    calibrator: bool,
) -> Result<Schedule, SimobserveError> {
    let parameters = run.parameters;
    let reference = reference_date(&parameters.refdate, &parameters.hourangle)?;
    let offset_s = hour_angle(&parameters.hourangle)?;
    tracing::info!(
        "You desire an hour angle of {} hours",
        python(offset_s / 3600.)
    );
    let calibrator_s = qa::value_in(&qa::read(&pointings.integration)?, "s")?;
    let times = &pointings.times_s;
    let fields = pointings.pointings.len();
    let total_s = total_time(
        &parameters.totaltime,
        times,
        calibrator.then_some(calibrator_s),
    )?;
    let all_scans_s = times.iter().fold(0., |sum, time| sum + time);
    if total_s < all_scans_s {
        tracing::warn!(
            "Not all pointings in the mosaic will be observed - check mosaic setup and exposure \
             time parameters!"
        );
    }
    let pass_s = times[..fields].iter().fold(0., |sum, time| sum + time)
        + if calibrator { calibrator_s } else { 0. };
    if total_s > 0. && pass_s <= 0. {
        return Err(SimobserveError::raised(format!(
            "a pass over the {fields} pointings takes {}s, so totaltime {} never ends",
            python(pass_s),
            parameters.totaltime
        )));
    }
    let mut start_s = -total_s / 2.0 + offset_s;
    let first_s = start_s;
    let mut steps = Vec::new();
    if calibrator {
        steps.push(Step::Calibrator {
            start_s,
            stop_s: start_s + calibrator_s,
        });
        start_s += calibrator_s;
    }
    let mut targets = Vec::new();
    let mut field = 0;
    while start_s - first_s < total_s {
        let stop_s = start_s + times[field];
        targets.push(Target {
            field,
            start_s,
            stop_s,
        });
        field += 1;
        start_s = stop_s;
        if field == fields {
            if calibrator {
                let stop_s = start_s + calibrator_s;
                steps.push(Step::Targets(std::mem::take(&mut targets)));
                steps.push(Step::Calibrator { start_s, stop_s });
                start_s = stop_s;
            }
            field = 0;
        }
    }
    if !targets.is_empty() {
        steps.push(Step::Targets(targets));
    }
    Ok(Schedule { reference, steps })
}

/// `refdate` at 00:00 UTC: its date, `(\d*/\d+/\d+)`, with any time after
/// it (`[/:\d]*`) discarded in favour of `hourangle`.
fn reference_date(refdate: &str, hourangle: &str) -> Result<MEpoch, SimobserveError> {
    let (date, time) = split_date(refdate)
        .ok_or_else(|| SimobserveError::raised(format!("Invalid reference date {refdate}")))?;
    if time.chars().count() > 1 {
        tracing::info!(
            "Discarding time part of refdate, '{time}', in favor of hourangle parameter = \
             {hourangle}"
        );
    }
    let days = qa::value_in(&qa::read(&format!("{date}/00:00:00"))?, "d")?;
    Ok(MEpoch::from_mjd(days, EpochRef::UTC))
}

/// The groups Python's `re.match('(\d*/\d+/\d+)([/:\d]*)', refdate)`
/// captures. Each digit run is greedy, and none can give a digit back.
fn split_date(text: &str) -> Option<(String, String)> {
    let chars: Vec<char> = text.chars().collect();
    let digits = |from: usize| {
        chars[from..]
            .iter()
            .take_while(|c| c.is_ascii_digit())
            .count()
    };
    let mut end = digits(0);
    for least in [1, 1] {
        if chars.get(end) != Some(&'/') {
            return None;
        }
        let run = digits(end + 1);
        if run < least {
            return None;
        }
        end += 1 + run;
    }
    let time = chars[end..]
        .iter()
        .take_while(|c| c.is_ascii_digit() || **c == '/' || **c == ':')
        .count();
    Some((
        chars[..end].iter().collect(),
        chars[end..end + time].iter().collect(),
    ))
}

/// `hourangle` in seconds: 0 for `transit`, else as hours when a number,
/// or as the time it is; CASA's message for anything else.
fn hour_angle(hourangle: &str) -> Result<f64, SimobserveError> {
    if hourangle == "transit" {
        return Ok(0.);
    }
    let mut offset_s = None;
    let as_hours = format!("{hourangle}h");
    if qa::is_quantity(&as_hours) {
        let hours = qa::read(&as_hours)?;
        if qa::conforms(&hours, "s") {
            offset_s = Some(qa::value_in(&hours, "s")?);
        }
    }
    // casacore converts an angle to a time at 360° a day, and keeps any
    // other unit's dimensions (a number becomes `s.s-1`, not a time).
    if qa::is_quantity(hourangle) {
        let seconds = qa::convert(&qa::read(hourangle)?, "s")?;
        if qa::conforms(&seconds, "s") {
            offset_s = Some(seconds.value());
        }
    }
    offset_s.ok_or_else(|| {
        SimobserveError::raised(format!(
            "Cannot interpret your hourangle parameter {hourangle} as a time quantity e.g. '5h', \
             30min'"
        ))
    })
}

/// `totaltime` in seconds: a time, or with no unit a number of passes over
/// the map (the calibrator's scan, `calibrator_s`, included in each).
fn total_time(
    totaltime: &str,
    times_s: &[f64],
    calibrator_s: Option<f64>,
) -> Result<f64, SimobserveError> {
    let not_a_time = || {
        SimobserveError::raised(format!(
            "totaltime {totaltime} does not appear to represent a time interval (use \
             's','min','h'; not 'sec','m','hr')"
        ))
    };
    if !qa::is_quantity(totaltime) {
        return Err(not_a_time());
    }
    let total = qa::read(totaltime)?;
    if total.value() < 0. {
        return Err(SimobserveError::raised(
            "Negative totaltime is not allowed.",
        ));
    }
    if total.unit().name().is_empty() {
        let pass_s = times_s.iter().fold(0., |sum, time| sum + time) + calibrator_s.unwrap_or(0.);
        let passes: f64 = totaltime.trim().parse().unwrap_or(total.value());
        let total_s = passes * pass_s;
        tracing::info!("Total observing time = {}s.", python(total_s));
        return Ok(total_s);
    }
    if !qa::conforms(&total, "s") {
        return Err(not_a_time());
    }
    Ok(qa::value_in(&total, "s")?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_dates_split_as_python_splits_them() {
        let split = |text: &str| split_date(text);
        let owned = |date: &str, time: &str| Some((date.to_string(), time.to_string()));
        assert_eq!(split("2014/01/01"), owned("2014/01/01", ""));
        assert_eq!(
            split("2014/01/01/12:00:00"),
            owned("2014/01/01", "/12:00:00")
        );
        assert_eq!(split("/1/2x"), owned("/1/2", ""));
        assert_eq!(split("2014-01-01"), None);
        assert_eq!(split("2014/01/"), None);
        let epoch = reference_date("2014/01/01/12:00:00", "transit").expect("a date");
        assert_eq!(epoch.value().as_mjd(), 56_658.0);
    }

    #[test]
    fn hour_angles_read_as_casa_reads_them() {
        let seconds = |text: &str| hour_angle(text).expect("an hour angle");
        assert_eq!(seconds("transit"), 0.);
        assert_eq!(seconds("1.5"), 5400.);
        assert_eq!(seconds("-2h"), -7200.);
        assert_eq!(seconds("30min"), 1800.);
        assert_eq!(seconds("30deg"), 7200.);
        assert!(hour_angle("soon").is_err());
    }

    #[test]
    fn total_times_are_times_or_passes() {
        let times = [10., 10., 10.];
        let total = |text: &str, calibrator| total_time(text, &times, calibrator).expect("a time");
        assert_eq!(total("7200s", None), 7200.);
        assert_eq!(total("2h", Some(10.)), 7200.);
        assert_eq!(total("2", None), 60.);
        assert_eq!(total("2", Some(10.)), 80.);
        assert!(total_time("-1s", &times, None).is_err());
        assert!(total_time("3m", &times, None).is_err());
        assert!(total_time("", &times, None).is_err());
    }
}
