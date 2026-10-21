// SPDX-License-Identifier: LGPL-3.0-or-later
//! The simulator session: CASA's `sm` tool in Rust.
//!
//! A [`Simulator`] holds what the `sm` setters describe, as CASA's
//! `Simulator` and `NewMSSimulator` would have written it to the subtables,
//! and the scans `observe` plans. It writes nothing until
//! [`Simulator::close`], which creates the MeasurementSet in one streamed
//! pass (plan §4, deferred single-pass execution), so no reader can see an
//! intermediate state.
//!
//! The setters keep CASA's semantics, including order-dependent ones:
//! `setfield` records the reference time and first spectral window in force
//! when it is called, `setconfig` and `setfeed` append, and `observe` reads
//! the limits, autocorrelation weight and antennas defined at that moment.
//! Where CASA crashes, writes garbage or corrupts data, the session refuses
//! or diverges, as recorded in `docs/CASA (C++) bugs.md`.

pub mod array;
pub mod error;
mod feeds;
pub mod fields;
pub mod observe;
pub mod spectral;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_ms::MeasurementSetWriteResources;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MeasFrame, MeasuresProvider};

use crate::write;
use array::{Antenna, ArrayConfig};
use error::{SimulatorError, SimulatorResult};
use feeds::Feed;
use fields::Field;
use observe::{Observation, ObserveSpec, Scan, StateSpec, Times};
use spectral::{SpectralWindow, SpectralWindowSpec};

/// Controls CASA's `sm` lacks: the session's measures data, memory and the
/// clock that stands for "now".
#[derive(Clone)]
pub struct SimulatorOptions {
    /// Measures data (Earth orientation, observatories).
    pub measures: Arc<dyn MeasuresProvider>,
    /// The write session's memory and buffers.
    pub write_resources: MeasurementSetWriteResources,
    /// The time the session opens, UTC: CASA's reference time until
    /// `settimes`.
    pub now: MEpoch,
}

impl SimulatorOptions {
    /// The production options: the discovered measures data, a write budget
    /// from the machine's physical memory (two live batches), and the system
    /// clock as "now".
    ///
    /// # Errors
    ///
    /// No measures data was found, or the memory budget could not be read.
    pub fn from_system() -> SimulatorResult<Self> {
        let measures = casa_ms::open_measures_runtime()
            .map_err(|error| SimulatorError::raised(format!("measures data: {error}")))?;
        let write_resources = MeasurementSetWriteResources::from_system_memory(2)
            .map_err(|error| SimulatorError::raised(format!("memory budget: {error}")))?;
        Ok(Self {
            measures,
            write_resources,
            now: system_now(),
        })
    }
}

/// The system clock as a UTC epoch.
fn system_now() -> MEpoch {
    /// MJD of the Unix epoch, 1970-01-01.
    const UNIX_EPOCH_MJD: f64 = 40_587.0;
    let since_unix_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64());
    MEpoch::from_mjd(UNIX_EPOCH_MJD + since_unix_s / 86_400.0, EpochRef::UTC)
}

// A session moves between threads, so the Python tool can release the GIL
// while it writes (ADR-0019).
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<Simulator>();
};

/// A simulator session writing one new MeasurementSet.
pub struct Simulator {
    path: PathBuf,
    options: SimulatorOptions,
    telescope: String,
    antennas: Vec<Antenna>,
    feeds: Vec<Feed>,
    /// Whether `setfeed` was called (CASA's `feedsHaveBeenSet`).
    feeds_set: bool,
    spectral_windows: Vec<SpectralWindow>,
    fields: Vec<Field>,
    times: Times,
    elevation_limit_rad: f64,
    shadow_fraction_limit: f64,
    autocorrelation_weight: f32,
    observations: Vec<Observation>,
    states: Vec<StateSpec>,
    scans: Vec<Scan>,
}

impl Simulator {
    /// `sm.open(ms)`: a session that will create `path`, replacing any table
    /// there, at [`close`](Self::close).
    pub fn open(path: impl AsRef<Path>, options: SimulatorOptions) -> Self {
        let times = Times::defaults(options.now.clone());
        Self {
            path: path.as_ref().to_path_buf(),
            options,
            // NewMSSimulator's telescope until setconfig.
            telescope: "Unknown".to_string(),
            antennas: Vec::new(),
            feeds: Vec::new(),
            feeds_set: false,
            spectral_windows: Vec::new(),
            fields: Vec::new(),
            times,
            elevation_limit_rad: 8.0_f64.to_radians(),
            shadow_fraction_limit: casa_ms::shadowing::CASA_SHADOW_FRACTION_LIMIT,
            autocorrelation_weight: 1.0,
            observations: Vec::new(),
            states: Vec::new(),
            scans: Vec::new(),
        }
    }

    /// `sm.name()`: the MeasurementSet's path, as given to `open`.
    pub fn name(&self) -> &Path {
        &self.path
    }

    /// The number of scans `observe` has planned. They are written only at
    /// [`close`](Self::close); a session dropped without it writes nothing.
    pub fn scan_count(&self) -> usize {
        self.scans.len()
    }

    /// `sm.setconfig(...)`: append the antennas.
    ///
    /// # Errors
    ///
    /// Coordinates or per-antenna lists of different lengths, or
    /// `longlat` coordinates.
    pub fn set_config(&mut self, config: &ArrayConfig) -> SimulatorResult<()> {
        let antennas = config.antennas()?;
        match config.coordinate_system {
            array::CoordinateSystem::Global => {
                tracing::info!("Using global coordinates for the antennas");
            }
            _ => tracing::info!("Using local coordinates for the antennas"),
        }
        self.telescope.clone_from(&config.telescope);
        self.antennas.extend(antennas);
        tracing::info!("Added rows to ANTENNA table");
        Ok(())
    }

    /// `sm.setfeed(mode, x, y, pol)`: append one perfect feed per antenna.
    ///
    /// # Errors
    ///
    /// A mode other than `perfect R L`, `perfect X Y` or `list` (CASA
    /// returns `False`), no antennas yet, or several feeds per antenna,
    /// which casa-rs does not write yet.
    pub fn set_feed(
        &mut self,
        mode: &str,
        x: &[f64],
        y: &[f64],
        pol: &[String],
    ) -> SimulatorResult<()> {
        let feeds = feeds::feeds(mode, x, y, pol, self.antennas.len())?;
        self.feeds.extend(feeds);
        self.feeds_set = true;
        tracing::info!("Added rows to FEED table");
        Ok(())
    }

    /// `sm.setspwindow(...)`: add a spectral window and its polarization.
    ///
    /// # Errors
    ///
    /// No channels (CASA returns `False`).
    pub fn set_spectral_window(&mut self, spec: &SpectralWindowSpec) -> SimulatorResult<()> {
        let window = SpectralWindow::new(spec)?;
        tracing::info!(
            "Creating new spectral window {}, ID {}",
            window.name,
            self.spectral_windows.len() + 1
        );
        self.spectral_windows.push(window);
        Ok(())
    }

    /// `sm.setfield(sourcename, sourcedirection, calcode, distance)`: add a
    /// field and its source.
    ///
    /// # Errors
    ///
    /// An empty name (CASA returns `False`), or a direction that cannot be
    /// converted to J2000 without an epoch and position.
    pub fn set_field(
        &mut self,
        name: &str,
        direction: &MDirection,
        calcode: &str,
        distance_m: f64,
    ) -> SimulatorResult<()> {
        if name.is_empty() {
            return Err(SimulatorError::refused("must provide a source name"));
        }
        let j2000 = if direction.refer() == DirectionRef::J2000 {
            direction.clone()
        } else {
            let frame = MeasFrame::new().with_measures(self.options.measures.clone());
            direction
                .convert_to(DirectionRef::J2000, &frame)
                .map_err(|error| {
                    SimulatorError::raised(format!(
                        "Could not convert source direction to a J2000 Direction Measure: {error}"
                    ))
                })?
        };
        let (ra, dec) = j2000.as_angles();
        // casacore's MVDirection longitude is in (−π, π].
        let ra = ra.sin().atan2(ra.cos());
        self.fields.push(Field {
            name: name.to_string(),
            code: calcode.to_string(),
            direction_rad: [ra, dec],
            distance_m,
            source_time_s: self.times.reference_utc_s(),
            rest_frequency_hz: self
                .spectral_windows
                .first()
                .map(|window| window.start_frequency_hz),
        });
        Ok(())
    }

    /// `sm.settimes(integrationtime, usehourangle, referencetime)`.
    ///
    /// # Errors
    ///
    /// A negative integration time (CASA's assertion raises).
    pub fn set_times(
        &mut self,
        integration_s: f64,
        use_hour_angle: bool,
        reference: MEpoch,
    ) -> SimulatorResult<()> {
        if integration_s < 0.0 {
            return Err(SimulatorError::raised(
                "Failed AlwaysAssert qIntTime.getValue(\"s\")>=0",
            ));
        }
        tracing::info!("Times");
        tracing::info!("     Integration time {integration_s}s");
        if use_hour_angle {
            tracing::info!("     Times will be interpreted as hour angles for first source");
        }
        self.times = Times {
            integration_s,
            use_hour_angle,
            reference,
            set: true,
            hour_angle_offset_s: None,
        };
        Ok(())
    }

    /// `sm.setlimits(shadowlimit, elevationlimit)`.
    pub fn set_limits(&mut self, shadow_fraction_limit: f64, elevation_limit_rad: f64) {
        self.shadow_fraction_limit = shadow_fraction_limit;
        self.elevation_limit_rad = elevation_limit_rad;
    }

    /// `sm.setauto(autocorrwt)`: autocorrelation rows are written when the
    /// weight is positive, with WEIGHT multiplied by it.
    pub fn set_auto(&mut self, autocorrelation_weight: f32) {
        self.autocorrelation_weight = autocorrelation_weight;
    }

    /// `sm.observe(...)`: plan one scan.
    ///
    /// # Errors
    ///
    /// No antennas, spectral window or field, or none of the given names
    /// (CASA logs the error and returns `False`).
    pub fn observe(&mut self, spec: &ObserveSpec) -> SimulatorResult<()> {
        self.plan_scan(spec)
    }

    /// `sm.close()` / `sm.done()`: write the MeasurementSet.
    ///
    /// # Errors
    ///
    /// A failure to create or write the MeasurementSet.
    pub fn close(self) -> SimulatorResult<()> {
        write::write_measurement_set(&self)
    }

    /// The `observe` bookkeeping; see [`observe`](self::observe).
    fn plan_scan(&mut self, spec: &ObserveSpec) -> SimulatorResult<()> {
        if self.antennas.is_empty() {
            return Err(SimulatorError::refused(
                "Antenna information not yet defined",
            ));
        }
        if !self.feeds_set && self.feeds.is_empty() {
            tracing::warn!("Feeds have not been set - using default perfect R L");
            self.feeds = feeds::perfect_feeds("perfect R L", self.antennas.len());
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
        let spectral_window_id = self
            .spectral_windows
            .iter()
            .position(|window| window.name == spec.spectral_window)
            .ok_or_else(|| {
                SimulatorError::refused(format!(
                    "Spectral window named {} not yet defined",
                    spec.spectral_window
                ))
            })?;
        if self.fields.is_empty() {
            return Err(SimulatorError::refused("Field information not yet defined"));
        }
        let field_id = self
            .fields
            .iter()
            .position(|field| field.name == spec.source)
            .ok_or_else(|| {
                SimulatorError::refused(format!("Field named {} not yet defined", spec.source))
            })?;
        let field = &self.fields[field_id];
        tracing::info!("First source: {} @ {:?}", field.name, field.direction_rad);

        let integration_s = self.times.integration_s;
        if integration_s <= 0.0 {
            return Err(SimulatorError::refused(
                "observe: the integration time must be positive",
            ));
        }
        if self.times.use_hour_angle && self.times.hour_angle_offset_s.is_none() {
            self.times.hour_angle_offset_s = Some(observe::hour_angle_offset_s(
                field.direction_rad,
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
        let start_s = spec.start_s + reference_s + offset_s;
        let stop_s = spec.stop_s + reference_s + offset_s;
        tracing::info!("Full time range: {start_s} -- {stop_s} UTC with int = {integration_s}");
        let integration_count = observe::integration_count(start_s, stop_s, integration_s);

        let observation_id = if spec.add_observation || self.observations.is_empty() {
            self.observations.push(Observation {
                telescope: self.telescope.clone(),
                time_range_s: [start_s, stop_s],
                observer: spec.observer.clone(),
                project: spec.project.clone(),
            });
            // One more than the largest OBSERVATION_ID in MAIN so far.
            self.scans
                .iter()
                .map(|scan| scan.observation_id)
                .max()
                .map_or(0, |largest| largest + 1)
        } else {
            0
        };
        let state_id = match self.states.iter().rposition(|state| *state == spec.state) {
            Some(existing) => existing,
            None => {
                self.states.push(spec.state.clone());
                self.states.len() - 1
            }
        };
        let scan_number = self.scans.last().map_or(0, |scan| scan.scan_number) + 1;
        let scan = Scan {
            scan_number,
            field_id,
            spectral_window_id,
            start_s,
            integration_s,
            integration_count,
            observation_id,
            state_id,
            antenna_count: self.antennas.len(),
            phase_center_rad: field.direction_rad,
            autocorrelation_weight: self.autocorrelation_weight,
            elevation_limit_rad: self.elevation_limit_rad,
            shadow_fraction_limit: self.shadow_fraction_limit,
        };
        tracing::info!(
            "Calculating a total of {} integrations",
            scan.integration_count
        );
        self.scans.push(scan);
        Ok(())
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn options(&self) -> &SimulatorOptions {
        &self.options
    }

    pub(crate) fn telescope(&self) -> &str {
        &self.telescope
    }

    pub(crate) fn antennas(&self) -> &[Antenna] {
        &self.antennas
    }

    pub(crate) fn feeds(&self) -> &[Feed] {
        &self.feeds
    }

    pub(crate) fn spectral_windows(&self) -> &[SpectralWindow] {
        &self.spectral_windows
    }

    pub(crate) fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub(crate) fn observations(&self) -> &[Observation] {
        &self.observations
    }

    pub(crate) fn states(&self) -> &[StateSpec] {
        &self.states
    }

    pub(crate) fn scans(&self) -> &[Scan] {
        &self.scans
    }
}
