// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.settimes` and `sm.observe`: the schedule of one scan.
//!
//! `observe` records a scan; the session writes every scan's rows at
//! `close`. The bookkeeping follows `NewMSSimulator::observe`: scan numbers
//! continue from the last scan, an OBSERVATION row is added when asked for
//! or when there is none, STATE rows are shared by identical states, and the
//! integration count is `max(1, Int(0.5 + (stop − start)/Tint))`.
//!
//! **Times honour the requested UTC** (plan decision 5). CASA adds the
//! reference time in TAI to start and stop and stores the result in a TIME
//! column labelled UTC, so its rows are TAI − UTC (37 s) late. casa-rs adds
//! the reference time in UTC.

use std::sync::Arc;

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{MEpoch, MPosition, MeasFrame, MeasuresProvider};

use super::error::{SimulatorError, SimulatorResult};

/// The STATE of a scan: `observe`'s `state_*` arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct StateSpec {
    /// SIG (`state_sig`).
    pub sig: bool,
    /// REF (`state_ref`).
    pub reference: bool,
    /// CAL (`state_cal`), K.
    pub cal: f64,
    /// LOAD (`state_load`), K.
    pub load: f64,
    /// SUB_SCAN (`state_sub_scan`).
    pub sub_scan: i32,
    /// OBS_MODE (`state_obs_mode`).
    pub obs_mode: String,
}

/// The arguments of `sm.observe`.
#[derive(Debug, Clone, PartialEq)]
pub struct ObserveSpec {
    /// The field, by name (`sourcename`).
    pub source: String,
    /// The spectral window, by name (`spwname`).
    pub spectral_window: String,
    /// Start (`starttime`), seconds after the reference time, or after the
    /// time the source transits in hour-angle mode.
    pub start_s: f64,
    /// Stop (`stoptime`), seconds, as `start_s`.
    pub stop_s: f64,
    /// Add an OBSERVATION row (`add_observation`); one is added anyway when
    /// there is none.
    pub add_observation: bool,
    /// The scan's state.
    pub state: StateSpec,
    /// OBSERVER (`observer`).
    pub observer: String,
    /// PROJECT (`project`).
    pub project: String,
}

/// `settimes` and its effects.
#[derive(Debug, Clone)]
pub(crate) struct Times {
    pub(crate) integration_s: f64,
    pub(crate) use_hour_angle: bool,
    /// The reference epoch, UTC.
    pub(crate) reference: MEpoch,
    /// Whether `settimes` was called.
    pub(crate) set: bool,
    /// The hour-angle offset, computed at the first `observe` after
    /// `settimes`; seconds.
    pub(crate) hour_angle_offset_s: Option<f64>,
}

impl Times {
    /// CASA's defaults before `settimes`: 10 s integrations in hour-angle
    /// mode, referred to the time the session was opened.
    pub(crate) fn defaults(now: MEpoch) -> Self {
        Self {
            integration_s: 10.0,
            use_hour_angle: true,
            reference: now,
            set: false,
            hour_angle_offset_s: None,
        }
    }

    /// The reference epoch as UTC seconds (MJD × 86400).
    pub(crate) fn reference_utc_s(&self) -> f64 {
        self.reference.value().as_mjd() * 86_400.0
    }
}

/// One OBSERVATION row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Observation {
    pub(crate) telescope: String,
    pub(crate) time_range_s: [f64; 2],
    pub(crate) observer: String,
    pub(crate) project: String,
}

/// One scan's rows, as `observe` planned them.
#[derive(Debug, Clone)]
pub(crate) struct Scan {
    pub(crate) scan_number: i32,
    pub(crate) field_id: usize,
    pub(crate) spectral_window_id: usize,
    /// The scan's start (CASA's `Tstart`), UTC seconds.
    pub(crate) start_s: f64,
    pub(crate) integration_s: f64,
    pub(crate) integration_count: usize,
    pub(crate) observation_id: i32,
    pub(crate) state_id: usize,
    /// Antennas and feeds defined when `observe` was called.
    pub(crate) antenna_count: usize,
    /// The J2000 phase centre, radians.
    pub(crate) phase_center_rad: [f64; 2],
    /// Autocorrelation rows are written when the weight is positive.
    pub(crate) autocorrelation_weight: f32,
    pub(crate) elevation_limit_rad: f64,
    pub(crate) shadow_fraction_limit: f64,
}

impl Scan {
    /// Baselines per integration, autocorrelations included when written.
    pub(crate) fn baseline_count(&self) -> usize {
        let n = self.antenna_count;
        if self.autocorrelation_weight > 0.0 {
            n * (n + 1) / 2
        } else {
            n * n.saturating_sub(1) / 2
        }
    }

    /// MAIN rows of the scan.
    pub(crate) fn row_count(&self) -> usize {
        self.baseline_count() * self.integration_count
    }

    /// TIME of integration `index`: CASA accumulates `Time += Tint` from
    /// the start and stores the integration's centre.
    pub(crate) fn integration_times_s(&self) -> impl Iterator<Item = f64> + '_ {
        let mut time = self.start_s;
        (0..self.integration_count).map(move |_| {
            let centre = time + self.integration_s / 2.0;
            time += self.integration_s;
            centre
        })
    }
}

/// CASA's integration count for one pointing: `max(1, Int(0.5 + span/Tint))`,
/// with C++'s truncation toward zero.
pub(crate) fn integration_count(start_s: f64, stop_s: f64, integration_s: f64) -> usize {
    let count = (0.5 + (stop_s - start_s) / integration_s).trunc();
    if count >= 1.0 { count as usize } else { 1 }
}

/// `NewMSSimulator`'s hour-angle offset: minus the hour angle of `direction`
/// at the reference epoch and the first antenna, as seconds, converting
/// hours of hour angle at 3600 seconds each (no sidereal factor).
pub(crate) fn hour_angle_offset_s(
    direction_rad: [f64; 2],
    reference: &MEpoch,
    first_antenna_itrf_m: [f64; 3],
    measures: Arc<dyn MeasuresProvider>,
) -> SimulatorResult<f64> {
    let [x, y, z] = first_antenna_itrf_m;
    let frame = MeasFrame::new()
        .with_epoch(reference.clone())
        .with_position(MPosition::new_itrf(x, y, z))
        .with_measures(measures);
    let hadec = MDirection::from_angles(direction_rad[0], direction_rad[1], DirectionRef::J2000)
        .convert_to(DirectionRef::HADEC, &frame)
        .map_err(|error| {
            SimulatorError::refused(format!("hour angle of the first source: {error}"))
        })?;
    let mut hour_angle = hadec.as_angles().0;
    // casacore's MVDirection longitude is in (−π, π].
    if hour_angle > std::f64::consts::PI {
        hour_angle -= std::f64::consts::TAU;
    } else if hour_angle <= -std::f64::consts::PI {
        hour_angle += std::f64::consts::TAU;
    }
    Ok(-hour_angle * (3600.0 * 180.0 / std::f64::consts::PI / 15.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_counts_round_like_casa() {
        assert_eq!(integration_count(0.0, 0.0, 10.0), 1);
        assert_eq!(integration_count(-600.0, -300.0, 10.0), 30);
        assert_eq!(integration_count(0.0, 14.0, 10.0), 1);
        assert_eq!(integration_count(0.0, 15.0, 10.0), 2);
        assert_eq!(integration_count(10.0, 0.0, 10.0), 1);
    }
}
