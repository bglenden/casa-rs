// SPDX-License-Identifier: LGPL-3.0-or-later
//! The simulator session: CASA's `sm` tool in Rust.
//!
//! A [`Simulator`] holds what the `sm` setters describe, as CASA's
//! `Simulator` and `NewMSSimulator` would have written it to the subtables,
//! and the scans `observe` plans. It writes nothing until
//! [`Simulator::close`], which creates the MeasurementSet in one streamed
//! pass (plan §4, deferred single-pass execution), so no reader can see an
//! intermediate state. The steps that change DATA (`predict`, `corrupt`) are
//! recorded in a plan of which scans each covers, and applied as `close`
//! writes each row (`plan`, `prediction`, `corruption`); `predict` prepares
//! its model's grids when it is called.
//!
//! A session [`Simulator::open_from_ms`] opens on an existing MeasurementSet
//! (`sm.openfromms`) reads its state back, takes its rows as scans already
//! observed, and patches their DATA in place at `close` (`from_ms`).
//!
//! The session runs within its [`SessionResources`]: `predict` admits its
//! model's grids, and `close` what it holds, against the imaging runtime's
//! resource policy, and `close` computes each write batch's geometry,
//! prediction and noise on the runtime's worker team, so the MeasurementSet
//! is bitwise the same for any budget or worker count. A model is predicted
//! on the CPU or the host's Metal device ([`SessionResources`]), whose
//! predictions agree with the CPU's to `1e-4` of the largest; noise is
//! drawn on the CPU on either.
//!
//! The setters keep CASA's semantics, including order-dependent ones:
//! `setfield` records the reference time and first spectral window in force
//! when it is called, `setconfig` and `setfeed` append, and `observe` reads
//! the limits, autocorrelation weight and antennas defined at that moment.
//! Where CASA crashes, writes garbage or corrupts data, the session refuses
//! or diverges, as recorded in `docs/CASA (C++) bugs.md`.

mod apply;
pub mod array;
mod corruption;
pub mod error;
mod feeds;
pub mod fields;
mod from_ms;
mod gains;
mod mosaic;
pub mod observe;
pub(crate) mod plan;
pub(crate) mod prediction;
pub mod resources;
mod single_dish;
pub mod spectral;
#[cfg(test)]
mod tests;
mod troposphere;
mod tsys_atm;

pub use apply::ApplySpec;
pub use corruption::{AtmosphereSpec, DEFAULT_SEED, NoiseSpec, TsysParameters};
pub use gains::{GainSpec, LeakageSpec};
pub use prediction::{PredictSpec, PredictionOptions, VoltagePatternSpec};
pub use resources::{BackendRequest, ExecutionControls, SessionResources, UnknownBackend};
pub use troposphere::TroposphereSpec;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_imaging_operator::PaddingFactor;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MeasFrame, MeasuresProvider};

use crate::corrupt::jones::HeldTerm;
use crate::predict::SingleDishKernel;
use crate::write;
use array::{Antenna, ArrayConfig};
use error::{SimulatorError, SimulatorResult};
use feeds::Feed;
use fields::Field;
use observe::{Observation, ObserveManySpec, ObserveSpec, Scan, StateSpec, Times};
use plan::{DataPlan, NoiseTerm};
use spectral::{SpectralWindow, SpectralWindowSpec};

/// Controls CASA's `sm` lacks: the session's measures data, its resources
/// and the clock that stands for "now".
#[derive(Clone)]
pub struct SimulatorOptions {
    /// Measures data (Earth orientation, observatories).
    pub measures: Arc<dyn MeasuresProvider>,
    /// The host, memory budget, workers and backend `close` runs with.
    pub resources: SessionResources,
    /// The time the session opens, UTC: CASA's reference time until
    /// `settimes`.
    pub now: MEpoch,
}

impl SimulatorOptions {
    /// The production options: the discovered measures data, `resources`,
    /// and the system clock as "now".
    ///
    /// # Errors
    ///
    /// No measures data was found.
    pub fn from_system(resources: SessionResources) -> SimulatorResult<Self> {
        let measures = casa_ms::open_measures_runtime()
            .map_err(|error| SimulatorError::raised(format!("measures data: {error}")))?;
        Ok(Self {
            measures,
            resources,
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
    /// The path given, which `name` reports.
    name: PathBuf,
    /// That path made absolute when the session was opened: the
    /// MeasurementSet `close` writes, whatever the working directory is by
    /// then, as CASA's simulator holds the table it opened.
    destination: PathBuf,
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
    /// The seed of noise terms set from now on (`setseed`).
    seed: i32,
    /// The noise terms `setnoise` set, until `reset`.
    noise_terms: Vec<NoiseTerm>,
    /// The antenna-based terms `setgain`, `setleakage` and `setapply` set,
    /// in the order they were set, until `reset`.
    jones_terms: Vec<apply::JonesSource>,
    /// The calibration tables `close` writes, each with its term (a
    /// `table` given to `setgain` or `setleakage`); `reset` keeps them.
    jones_tables: Vec<(PathBuf, Arc<HeldTerm>)>,
    /// `predict`'s `GridFT` grid padding (`setoptions`).
    padding: PaddingFactor,
    /// `setoptions(ftmachine='wproject', wprojplanes=…)`: the planes
    /// `predict` projects through, as given, or `None` for `GridFT`.
    w_projection_planes: Option<i64>,
    /// `setoptions(ftmachine='mosaic')`: `predict` degrids a model image
    /// through `MosaicFTNew`.
    mosaic: bool,
    /// `setoptions(ftmachine='sd', gridfunction=…)`: the kernel `predict`
    /// samples total power with, or `None` for an interferometric
    /// FTMachine.
    single_dish: Option<SingleDishKernel>,
    /// Whether `predict` applies the primary beam (`setvp(dovp)`).
    voltage_pattern: bool,
    /// Whether that beam squints (`setvp(dosquint)`), as CASA's VLA and
    /// EVLA patterns then do for a component list.
    squint: bool,
    /// The DATA steps (`predict`, `corrupt`), applied at `close`.
    data_plan: DataPlan,
    /// The MeasurementSet `openfromms` opened, whose rows `close` patches;
    /// `None` when `close` creates the MeasurementSet.
    opened: Option<from_ms::OpenedMs>,
}

impl Simulator {
    /// `sm.open(ms)`: a session that will create `path`, replacing any table
    /// there, at [`close`](Self::close).
    pub fn open(path: impl AsRef<Path>, options: SimulatorOptions) -> Self {
        let times = Times::defaults(options.now.clone());
        let path = path.as_ref().to_path_buf();
        Self {
            // Without a working directory a relative path stays as given.
            destination: std::path::absolute(&path).unwrap_or_else(|_| path.clone()),
            name: path,
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
            seed: DEFAULT_SEED,
            noise_terms: Vec::new(),
            jones_terms: Vec::new(),
            jones_tables: Vec::new(),
            padding: PaddingFactor::SIMULATOR,
            w_projection_planes: None,
            mosaic: false,
            single_dish: None,
            voltage_pattern: false,
            squint: false,
            data_plan: DataPlan::default(),
            opened: None,
        }
    }

    /// `sm.name()`: the MeasurementSet's path, as given to `open`; after
    /// `openfromms`, the absolute path, as CASA's `Table::tableName` gives
    /// it.
    pub fn name(&self) -> &Path {
        &self.name
    }

    /// The number of scans: those `observe` has planned, or the runs of
    /// MAIN rows of one scan, field and spectral window in the
    /// MeasurementSet `openfromms` opened. DATA is written only at
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
        self.refuse_after_openfromms("setconfig")?;
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
        self.refuse_after_openfromms("setfeed")?;
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
        self.refuse_after_openfromms("setspwindow")?;
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
        self.refuse_after_openfromms("setfield")?;
        if name.is_empty() {
            return Err(SimulatorError::refused("must provide a source name"));
        }
        let direction_rad = self.j2000_rad(direction, "source direction")?;
        self.fields.push(Field {
            name: name.to_string(),
            code: calcode.to_string(),
            direction_rad,
            distance_m,
            source_time_s: self.times.reference_utc_s(),
            rest_frequency_hz: self
                .spectral_windows
                .first()
                .map(|window| window.reference_frequency_hz),
        });
        Ok(())
    }

    /// `sm.setmosaicfield(sourcename, calcode, fieldcenter, xmosp, ymosp,
    /// mosspacing, distance)`: add an `nx × ny` grid of fields, named
    /// `{name}_{k}` with `k` counting down each column (y fastest) and then
    /// across, `spacing_rad` apart in latitude and in longitude at the
    /// centre's latitude, centred on `centre` and in its frame.
    ///
    /// **Divergence** (plan §4, `docs/CASA (C++) bugs.md`).
    /// - CASA tests an odd count as `nx/2 != floor(nx/2)` in integer
    ///   arithmetic, so every grid takes its even-count offsets. Those count
    ///   from zero where one was meant, so CASA's grids sit off their centre.
    /// - Over a pole, CASA sets the longitude to `|π − α|`, where crossing
    ///   the pole moves it by π.
    ///
    /// casa-rs centres each grid on `centre`: offsets `(i − (n − 1)/2)` times
    /// the spacing. It crosses a pole as the sphere does.
    ///
    /// # Errors
    ///
    /// As [`set_field`](Self::set_field), for any of the fields.
    pub fn set_mosaic_field(
        &mut self,
        name: &str,
        calcode: &str,
        centre: &MDirection,
        (nx, ny): (usize, usize),
        spacing_rad: f64,
        distance_m: f64,
    ) -> SimulatorResult<()> {
        use std::f64::consts::{FRAC_PI_2, PI, TAU};
        self.refuse_after_openfromms("setmosaicfield")?;
        let (ra0, dec0) = centre.as_angles();
        let offset = |index: usize, count: usize| index as f64 - (count as f64 - 1.0) / 2.0;
        let mut k = 0;
        for i in 0..nx {
            for j in 0..ny {
                let mut ra = ra0 + offset(i, nx) * spacing_rad / dec0.cos();
                let mut dec = dec0 + offset(j, ny) * spacing_rad;
                if ra > TAU {
                    ra -= TAU;
                }
                if dec.abs() > FRAC_PI_2 {
                    dec = dec.signum() * (PI - dec.abs());
                    ra = (ra + PI).rem_euclid(TAU);
                }
                let direction = MDirection::from_angles(ra, dec, centre.refer());
                self.set_field(&format!("{name}_{k}"), &direction, calcode, distance_m)?;
                k += 1;
            }
        }
        Ok(())
    }

    /// `sm.settimes(integrationtime, usehourangle, referencetime)`.
    ///
    /// A reference epoch in another time scale is converted to UTC, which
    /// the session works in.
    ///
    /// # Errors
    ///
    /// A negative integration time (CASA's assertion raises), or a
    /// reference epoch that does not convert to UTC.
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
        let reference = if reference.refer() == EpochRef::UTC {
            reference
        } else {
            let frame = MeasFrame::new().with_measures(self.options.measures.clone());
            reference
                .convert_to(EpochRef::UTC, &frame)
                .map_err(|error| {
                    SimulatorError::raised(format!(
                        "Could not convert the {} reference time to UTC: {error}",
                        reference.refer()
                    ))
                })?
        };
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

    /// `sm.observe(...)`: plan one scan, as `observemany` plans one pointing
    /// with no direction.
    ///
    /// # Errors
    ///
    /// No antennas, spectral window or field, or none of the given names
    /// (CASA logs the error and returns `False`).
    pub fn observe(&mut self, spec: &ObserveSpec) -> SimulatorResult<()> {
        self.refuse_after_openfromms("observe")?;
        self.plan_pointings(&ObserveManySpec::from(spec))
    }

    /// `sm.observemany(...)`: plan `spec`'s pointings in order, a scan for
    /// each run of pointings of one field, each pointing's phase centre its
    /// direction where given ([`Pointing`](observe::Pointing)).
    ///
    /// # Errors
    ///
    /// As [`observe`](Self::observe), for any pointing's field; no
    /// pointings; or a direction that cannot be converted to J2000 without
    /// an epoch and position.
    pub fn observe_many(&mut self, spec: &ObserveManySpec) -> SimulatorResult<()> {
        self.refuse_after_openfromms("observemany")?;
        self.plan_pointings(spec)
    }

    /// `sm.setdata(spwid, fieldid, msselect)`: select the rows the next
    /// `predict` and `corrupt` act on, until the next `observe`, which drops
    /// CASA's selection.
    ///
    /// CASA's selection works on the rows observed so far:
    /// - their FIELD_ID must be among `field_ids`, when there are any;
    /// - their DATA_DESC_ID must be one of the windows `spectral_window_ids`
    ///   names, when it names any (casa-rs's data description `w` is
    ///   window `w`);
    /// - then the TaQL `msselect` applies;
    /// - a selection that keeps no row reverts to every row, with a warning.
    ///
    /// casa-rs takes a selection that keeps every row, as `simobserve`'s
    /// `setdata(fieldid=range(nfld))` does. The following steps then act on
    /// every row observed so far, as without `setdata`.
    ///
    /// # Errors
    ///
    /// Not yet implemented: a selection that keeps some but not all of the
    /// rows, or a non-blank `msselect`.
    pub fn set_data(
        &mut self,
        spectral_window_ids: &[i64],
        field_ids: &[i64],
        ms_select: &str,
    ) -> SimulatorResult<()> {
        tracing::info!("Selecting data");
        if !ms_select.trim().is_empty() {
            return Err(SimulatorError::not_yet_implemented(format!(
                "setdata(msselect='{ms_select}') is not yet implemented in casa-rs (#841)"
            )));
        }
        let windows: Vec<usize> = (0..self.spectral_windows.len())
            .filter(|window| spectral_window_ids.contains(&(*window as i64)))
            .collect();
        if field_ids.is_empty() && windows.is_empty() {
            return Ok(());
        }
        let selected = |scan: &Scan| {
            (field_ids.is_empty() || field_ids.contains(&(scan.field_id as i64)))
                && (windows.is_empty() || windows.contains(&scan.spectral_window_id))
        };
        let kept = self.scans.iter().filter(|scan| selected(scan)).count();
        if kept == 0 {
            tracing::warn!("Selection is empty: reverting to original MeasurementSet");
            return Ok(());
        }
        if kept == self.scans.len() {
            return Ok(());
        }
        Err(SimulatorError::not_yet_implemented(format!(
            "setdata selecting {kept} of the {} scans observed so far is not yet implemented in \
             casa-rs (simulation Phase 2); a selection of them all is",
            self.scans.len()
        )))
    }

    /// `sm.close()` / `sm.done()`: write the MeasurementSet, or, after
    /// `openfromms`, patch the rows the DATA steps rewrite in place; then
    /// write the calibration tables `setgain` and `setleakage` named.
    ///
    /// # Errors
    ///
    /// A failure to create, read or write the MeasurementSet or a
    /// calibration table.
    pub fn close(self) -> SimulatorResult<()> {
        if self.opened.is_some() {
            write::patch_measurement_set(&self)?;
        } else {
            write::write_measurement_set(&self)?;
        }
        self.write_jones_tables()
    }

    /// `direction` in J2000, as `[ra, dec]` radians with the longitude in
    /// (−π, π] as casacore's `MVDirection` keeps it; `what` names it in the
    /// error.
    fn j2000_rad(&self, direction: &MDirection, what: &str) -> SimulatorResult<[f64; 2]> {
        let j2000 = if direction.refer() == DirectionRef::J2000 {
            direction.clone()
        } else {
            let frame = MeasFrame::new().with_measures(self.options.measures.clone());
            direction
                .convert_to(DirectionRef::J2000, &frame)
                .map_err(|error| {
                    SimulatorError::raised(format!(
                        "Could not convert {what} to a J2000 Direction Measure: {error}"
                    ))
                })?
        };
        let (ra, dec) = j2000.as_angles();
        Ok([ra.sin().atan2(ra.cos()), dec])
    }

    /// The absolute path of the MeasurementSet `close` writes.
    pub(crate) fn path(&self) -> &Path {
        &self.destination
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

    pub(crate) fn data_plan(&self) -> &DataPlan {
        &self.data_plan
    }
}
