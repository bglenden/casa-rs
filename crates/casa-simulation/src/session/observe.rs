// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.settimes`, `sm.observe` and `sm.observemany`: the schedule of scans.
//!
//! `observe` records a scan, and `observemany` a run of pointings, each
//! pointing a scan of its own; the session writes every scan's rows at
//! `close`. The bookkeeping follows `NewMSSimulator::observe`, whose
//! one-source form is `observemany` with one pointing and no direction:
//! - scan numbers continue from the last scan, and a run of pointings of one
//!   field shares one;
//! - an OBSERVATION row is added when asked for or when there is none;
//! - STATE rows are shared by identical states;
//! - each pointing's integration count is
//!   `max(1, Int(0.5 + (stop − start)/Tint))`.
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

/// One pointing of `sm.observemany`: a field, by name, observed from
/// `start_s` to `stop_s`, its phase centre moved to `direction` when one is
/// given.
#[derive(Debug, Clone)]
pub struct Pointing {
    /// The field, by name (an element of `sourcenames`).
    pub source: String,
    /// Start, seconds, as [`ObserveSpec::start_s`] (an element of
    /// `starttimes`).
    pub start_s: f64,
    /// Stop, seconds (an element of `stoptimes`).
    pub stop_s: f64,
    /// The pointing's phase centre (an element of `directions`). `None`
    /// keeps the field's, or the previous pointing's when the field is the
    /// same; casatools passes the pole for an empty direction, which
    /// `NewMSSimulator` reads as none.
    pub direction: Option<MDirection>,
}

/// The arguments of `sm.observemany`: its pointings, observed in order, and
/// the window, OBSERVATION and STATE of them all.
#[derive(Debug, Clone)]
pub struct ObserveManySpec {
    /// The pointings, in time order (CASA does not check it).
    pub pointings: Vec<Pointing>,
    /// The spectral window, by name (`spwname`).
    pub spectral_window: String,
    /// Add an OBSERVATION row (`add_observation`); one is added anyway when
    /// there is none.
    pub add_observation: bool,
    /// The pointings' state.
    pub state: StateSpec,
    /// OBSERVER (`observer`).
    pub observer: String,
    /// PROJECT (`project`).
    pub project: String,
}

impl From<&ObserveSpec> for ObserveManySpec {
    /// `sm.observe`'s arguments as the one pointing `NewMSSimulator`'s
    /// `observe` makes of them, with no direction.
    fn from(spec: &ObserveSpec) -> Self {
        Self {
            pointings: vec![Pointing {
                source: spec.source.clone(),
                start_s: spec.start_s,
                stop_s: spec.stop_s,
                direction: None,
            }],
            spectral_window: spec.spectral_window.clone(),
            add_observation: spec.add_observation,
            state: spec.state.clone(),
            observer: spec.observer.clone(),
            project: spec.project.clone(),
        }
    }
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

/// One scan: a run of MAIN rows of one field and spectral window, which the
/// DATA steps cover as a whole. Its rows are either planned by `observe`,
/// for `close` to generate, or already on disk in the MeasurementSet
/// `openfromms` opened.
#[derive(Debug, Clone)]
pub(crate) struct Scan {
    pub(crate) scan_number: i32,
    pub(crate) field_id: usize,
    pub(crate) spectral_window_id: usize,
    /// The J2000 phase centre, radians: the field's PHASE_DIR, on which
    /// CASA's `VisBuffer` phases the DATA `predict` writes.
    pub(crate) phase_center_rad: [f64; 2],
    /// The J2000 direction the antennas point at, radians: the phase centre
    /// unless `observemany` moved the pointing. `NewMSSimulator` computes
    /// the rows' UVW and the elevation limit there and writes it to
    /// POINTING, but keeps the field's FIELD row, so a moved pointing's
    /// DATA is phased on its field (#884).
    pub(crate) pointing_rad: [f64; 2],
    pub(crate) rows: ScanRows,
}

/// Where a scan's rows come from.
#[derive(Debug, Clone)]
pub(crate) enum ScanRows {
    /// Rows `observe` planned, which `close` generates.
    Planned(PlannedRows),
    /// MAIN rows already in the MeasurementSet `openfromms` opened.
    OnDisk(std::ops::Range<usize>),
}

/// The rows of a scan `observe` planned.
#[derive(Debug, Clone)]
pub(crate) struct PlannedRows {
    /// The scan's start (CASA's `Tstart`), UTC seconds.
    pub(crate) start_s: f64,
    pub(crate) integration_s: f64,
    pub(crate) integration_count: usize,
    pub(crate) observation_id: i32,
    pub(crate) state_id: usize,
    /// Antennas and feeds defined when `observe` was called.
    pub(crate) antenna_count: usize,
    /// Autocorrelation rows are written when the weight is positive.
    pub(crate) autocorrelation_weight: f32,
    pub(crate) elevation_limit_rad: f64,
    pub(crate) shadow_fraction_limit: f64,
}

impl Scan {
    /// MAIN rows of the scan.
    pub(crate) fn row_count(&self) -> usize {
        match &self.rows {
            ScanRows::Planned(planned) => planned.row_count(),
            ScanRows::OnDisk(rows) => rows.len(),
        }
    }

    /// The scan's planned rows, `None` for rows already on disk.
    pub(crate) fn planned(&self) -> Option<&PlannedRows> {
        match &self.rows {
            ScanRows::Planned(planned) => Some(planned),
            ScanRows::OnDisk(_) => None,
        }
    }

    /// The scan's MAIN rows on disk, `None` for planned rows.
    pub(crate) fn on_disk(&self) -> Option<std::ops::Range<usize>> {
        match &self.rows {
            ScanRows::OnDisk(rows) => Some(rows.clone()),
            ScanRows::Planned(_) => None,
        }
    }
}

impl PlannedRows {
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

/// A scan whose rows `observe` planned, as `close` generates them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PlannedScan<'a> {
    pub(crate) scan: &'a Scan,
    pub(crate) rows: &'a PlannedRows,
}

impl<'a> PlannedScan<'a> {
    /// `scans` with their planned rows, `None` when any scan's rows are on
    /// disk.
    pub(crate) fn of_all(scans: &'a [Scan]) -> Option<Vec<Self>> {
        scans
            .iter()
            .map(|scan| scan.planned().map(|rows| Self { scan, rows }))
            .collect()
    }
}

impl super::Simulator {
    /// `NewMSSimulator::observe`'s bookkeeping for `spec`'s pointings, each
    /// planned as a [`Scan`] of its own:
    /// - a new scan number at the first pointing and whenever the field
    ///   changes, so consecutive pointings of one field share one;
    /// - each pointing's phase centre is its direction when it has one,
    ///   else its field's at a change of field, else the previous
    ///   pointing's;
    /// - the hour-angle offset is the first field's, and the OBSERVATION
    ///   row spans the first pointing's start to the last one's stop.
    ///
    /// CASA adds every pointing's MAIN rows before it looks up the fields
    /// after the first, so a missing one raises with rows left behind;
    /// casa-rs looks every field up first and plans nothing.
    pub(super) fn plan_pointings(&mut self, spec: &ObserveManySpec) -> SimulatorResult<()> {
        let spectral_window_id = self.observable_window(&spec.spectral_window)?;
        let Some((first, last)) = spec.pointings.first().zip(spec.pointings.last()) else {
            return Err(SimulatorError::refused(
                "observemany: no pointings to observe",
            ));
        };
        if self.fields.is_empty() {
            return Err(SimulatorError::refused("Field information not yet defined"));
        }
        let field_of = |name: &str| self.fields.iter().position(|field| field.name == name);
        let first_field = field_of(&first.source).ok_or_else(|| {
            SimulatorError::refused(format!("Field named {} not yet defined", first.source))
        })?;
        let mut field_ids = Vec::with_capacity(spec.pointings.len());
        for (index, pointing) in spec.pointings.iter().enumerate() {
            field_ids.push(field_of(&pointing.source).ok_or_else(|| {
                SimulatorError::refused(format!(
                    "   Field {index}: {} not yet defined",
                    pointing.source
                ))
            })?);
        }
        let directions = spec
            .pointings
            .iter()
            .map(|pointing| {
                pointing
                    .direction
                    .as_ref()
                    .map(|direction| self.j2000_rad(direction, "pointing direction"))
                    .transpose()
            })
            .collect::<SimulatorResult<Vec<_>>>()?;
        let first_direction_rad = self.fields[first_field].direction_rad;
        tracing::info!(
            "First source: {} @ {first_direction_rad:?}",
            self.fields[first_field].name
        );

        let integration_s = self.times.integration_s;
        if integration_s <= 0.0 {
            return Err(SimulatorError::refused(
                "observe: the integration time must be positive",
            ));
        }
        if self.times.use_hour_angle && self.times.hour_angle_offset_s.is_none() {
            self.times.hour_angle_offset_s = Some(hour_angle_offset_s(
                first_direction_rad,
                &self.times.reference,
                self.antennas[0].position_itrf_m,
                self.options.measures.clone(),
            )?);
        }
        let offset_s = if self.times.use_hour_angle {
            self.times.hour_angle_offset_s.unwrap_or(0.0)
        } else {
            0.0
        };
        let reference_s = self.times.reference_utc_s();
        let utc_s = move |seconds: f64| seconds + reference_s + offset_s;
        let (start_s, stop_s) = (utc_s(first.start_s), utc_s(last.stop_s));
        tracing::info!("Full time range: {start_s} -- {stop_s} UTC with int = {integration_s}");

        let observation_id = self.observation_for(spec, [start_s, stop_s]);
        let state_id = match self.states.iter().rposition(|state| *state == spec.state) {
            Some(existing) => existing,
            None => {
                self.states.push(spec.state.clone());
                self.states.len() - 1
            }
        };
        let mut scan_number = self.scans.last().map_or(0, |scan| scan.scan_number);
        let mut pointing_rad = first_direction_rad;
        let mut integrations = 0;
        for (index, pointing) in spec.pointings.iter().enumerate() {
            let field_id = field_ids[index];
            if index == 0 || pointing.source != spec.pointings[index - 1].source {
                scan_number += 1;
                pointing_rad = self.fields[field_id].direction_rad;
            }
            if let Some(direction) = directions[index] {
                pointing_rad = direction;
            }
            let start_s = utc_s(pointing.start_s);
            let integration_count =
                integration_count(start_s, utc_s(pointing.stop_s), integration_s);
            integrations += integration_count;
            self.scans.push(Scan {
                scan_number,
                field_id,
                spectral_window_id,
                phase_center_rad: self.fields[field_id].direction_rad,
                pointing_rad,
                rows: ScanRows::Planned(PlannedRows {
                    start_s,
                    integration_s,
                    integration_count,
                    observation_id,
                    state_id,
                    antenna_count: self.antennas.len(),
                    autocorrelation_weight: self.autocorrelation_weight,
                    elevation_limit_rad: self.elevation_limit_rad,
                    shadow_fraction_limit: self.shadow_fraction_limit,
                }),
            });
        }
        tracing::info!("Calculating a total of {integrations} integrations");
        Ok(())
    }

    /// The checks `NewMSSimulator::observe` makes before its fields, and
    /// the default feeds it sets: antennas, feeds as one per antenna, and
    /// the spectral window named `name`, whose index it returns.
    fn observable_window(&mut self, name: &str) -> SimulatorResult<usize> {
        if self.antennas.is_empty() {
            return Err(SimulatorError::refused(
                "Antenna information not yet defined",
            ));
        }
        if !self.feeds_set && self.feeds.is_empty() {
            tracing::warn!("Feeds have not been set - using default perfect R L");
            self.feeds = super::feeds::perfect_feeds("perfect R L", self.antennas.len());
        }
        if !self.times.set {
            tracing::warn!("Times have not been set - using defaults");
            tracing::warn!("     Times will be interpreted as hour angles for first source");
        }
        if self.feeds.len() != self.antennas.len() {
            return Err(SimulatorError::refused(
                "Feed table format is incompatible with existing code of NewMSSimulator::observe",
            ));
        }
        if self.spectral_windows.is_empty() {
            return Err(SimulatorError::refused(
                "Spectral window information not yet defined",
            ));
        }
        self.spectral_windows
            .iter()
            .position(|window| window.name == name)
            .ok_or_else(|| {
                SimulatorError::refused(format!("Spectral window named {name} not yet defined"))
            })
    }

    /// The OBSERVATION_ID of a planned observation spanning `time_range_s`:
    /// a new OBSERVATION row when `spec` asks for one or there is none,
    /// numbered one past the largest OBSERVATION_ID in MAIN so far, else 0.
    fn observation_for(&mut self, spec: &ObserveManySpec, time_range_s: [f64; 2]) -> i32 {
        if !spec.add_observation && !self.observations.is_empty() {
            return 0;
        }
        self.observations.push(Observation {
            telescope: self.telescope.clone(),
            time_range_s,
            observer: spec.observer.clone(),
            project: spec.project.clone(),
        });
        self.scans
            .iter()
            .filter_map(|scan| scan.planned().map(|rows| rows.observation_id))
            .max()
            .map_or(0, |largest| largest + 1)
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
