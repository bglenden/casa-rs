// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setnoise(mode='tsys-atm')`: the atmosphere the noise's Tsys comes
//! from, built at `setnoise` (#841 Phase 4c). The model and where it
//! departs from CASA are the `corrupt::tsys_atm` module's.
//!
//! As CASA's `AtmosCorruptor::initAtm`, `setnoise` builds the atmosphere
//! for every spectral window defined then; a scan in a window defined later
//! is refused at `corrupt`. Each window's channels are independent and are
//! computed on the session's workers. What the windows keep (each
//! channel's line of sight) is admitted, with what building the largest
//! window holds, before anything is built, and stays charged while the
//! term does.

use casa_atmosphere::{AbsorptionProfile, AtmosphereProfile, ChannelTotals};
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{Demand, admit};

use super::Simulator;
use super::corruption::{LARGEST_ZENITH_TSYS_K, NoiseSpec};
use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::TsysAtm;
use crate::corrupt::tsys_atm::{self, WindowAtmosphere};

/// The phase the atmosphere of a `tsys-atm` term is admitted as.
const ATMOSPHERE_PHASE: &str = "the atmosphere of a tsys-atm noise term";

/// Channels a worker computes at a time.
const CHANNELS_PER_JOB: usize = 64;

impl Simulator {
    /// The `tsys-atm` model of `spec`, its atmosphere built for every
    /// spectral window, checked as CASA checks it. `pwv_mm` is the water
    /// column after `setnoise`'s default.
    pub(super) fn tsys_atm(&self, spec: &NoiseSpec, pwv_mm: f64) -> SimulatorResult<TsysAtm> {
        let parameters = spec.atmosphere.resolved(spec.tsys.ground_k);
        log_defaults(spec);
        tracing::info!("Initializing ATM");
        tracing::info!(
            "altitude={}m, Pground={}mb, Tground={}K, humidity= {}%, water scale height={}m",
            parameters.altitude_m,
            parameters.ground_pressure_hpa,
            parameters.ground_temperature_k,
            parameters.relative_humidity_percent,
            parameters.water_vapour_scale_height_km * 1000.0
        );
        let atmosphere = AtmosphereProfile::new(&parameters).map_err(|error| {
            SimulatorError::refused(format!("Caught exception: error in ATM setup - {error}"))
        })?;
        let layers = atmosphere.layers().len();
        let kept: u64 = self
            .spectral_windows
            .iter()
            .map(|window| WindowAtmosphere::bytes(window.channel_count, layers))
            .sum();
        let building = self
            .spectral_windows
            .iter()
            .map(|window| building_bytes(window.channel_count, layers))
            .max()
            .unwrap_or(0);
        let resources = &self.options.resources;
        let mut reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: ATMOSPHERE_PHASE,
                memory: kept + building,
            },
        )
        .map_err(SimulatorError::admission)?;
        let team = WorkerTeam::new(resources.cores().workers.max(1))
            .map_err(|error| SimulatorError::raised(format!("the worker team: {error}")))?;
        // In single precision, as the corruptor holds them.
        let pwv_m = tsys_atm::single(pwv_mm) * 1e-3;
        let coupling = (
            tsys_atm::single(spec.tsys.spillover_efficiency),
            tsys_atm::single(spec.tsys.ground_k),
        );
        let mut windows = Vec::with_capacity(self.spectral_windows.len());
        for spectral_window in &self.spectral_windows {
            let channels = spectral_window.channel_frequencies_hz();
            let grid = tsys_atm::window_grid(&channels)
                .map_err(|error| SimulatorError::refused(format!("Caught exception: {error}")))?;
            let totals = window_totals(&team, &atmosphere, &channels)?;
            let profile = AbsorptionProfile::from_channels(atmosphere.clone(), grid, totals)
                .map_err(|error| SimulatorError::raised(format!("the atmosphere: {error}")))?;
            let window = WindowAtmosphere::new(&profile, pwv_m, coupling).map_err(|error| {
                SimulatorError::refused(format!("Caught exception: error in ATM setup - {error}"))
            })?;
            windows.push(Some(window));
        }
        reservation.retain(kept);
        let model = TsysAtm::new(&spec.tsys, windows, reservation);
        check_zenith_tsys(&model, spec, pwv_mm)?;
        Ok(model)
    }
}

/// CASA's check of the zenith Tsys at the centre of the first window,
/// logged as CASA logs it. A Tsys that is not a number is refused too.
fn check_zenith_tsys(model: &TsysAtm, spec: &NoiseSpec, pwv_mm: f64) -> SimulatorResult<()> {
    if !model.covers(0) {
        return Err(SimulatorError::raised(
            "tsys-atm noise needs a spectral window",
        ));
    }
    let window = model.window(0);
    let centre = window.channels() / 2;
    let zenith = model.tsys(window, centre, 1.0);
    tracing::info!(
        "Zenith Tsys at center of first Spectral Window = {zenith} tground={} spillover={}",
        spec.tsys.ground_k as f32,
        spec.tsys.spillover_efficiency as f32
    );
    tracing::info!(
        " pwv={} tau={}(ch{centre})",
        pwv_mm,
        window.zenith_opacity(centre)
    );
    if zenith > LARGEST_ZENITH_TSYS_K || zenith <= 0.0 || zenith.is_nan() {
        return Err(SimulatorError::refused(format!(
            "Caught exception: error in ATM setup - Tsys {zenith} poorly defined - check \
                 inputs"
        )));
    }
    Ok(())
}

/// What building a window of `channels` channels holds beside what it
/// keeps: its channel frequencies, the profile of its channels' totals
/// (grid and the atmosphere's copy included), and the jobs' vectors with
/// the one they are gathered into.
fn building_bytes(channels: usize, layers: usize) -> u64 {
    AbsorptionProfile::<ChannelTotals>::shape_bytes(1, channels, layers)
        + (channels * (size_of::<f64>() + 2 * size_of::<ChannelTotals>())) as u64
}

/// Each channel's totals of `atmosphere` at `channels`, computed on `team`
/// in jobs of [`CHANNELS_PER_JOB`] channels.
fn window_totals(
    team: &WorkerTeam,
    atmosphere: &AtmosphereProfile,
    channels: &[f64],
) -> SimulatorResult<Vec<ChannelTotals>> {
    let mut jobs: Vec<(&[f64], Vec<ChannelTotals>)> = channels
        .chunks(CHANNELS_PER_JOB)
        .map(|chunk| (chunk, Vec::new()))
        .collect();
    team.for_each_mut(&mut jobs, |_, (chunk, totals)| {
        *totals = tsys_atm::channel_totals(atmosphere, chunk);
        Ok::<_, SimulatorError>(())
    })?;
    let mut totals = Vec::with_capacity(channels.len());
    for (_, job) in jobs {
        totals.extend(job);
    }
    Ok(totals)
}

/// CASA's log lines for the site values `setnoise` fills in.
fn log_defaults(spec: &NoiseSpec) {
    let site = &spec.atmosphere;
    if site.ground_pressure_hpa <= 100.0 {
        tracing::info!("User has not set ground pressure, using 560mb");
    }
    if site.relative_humidity_percent <= 0.0 {
        tracing::info!("User has not set ground relative humidity, using 20%");
    }
    if site.altitude_m <= 0.0 {
        tracing::info!("User has not set site altitude, using 5000m");
    }
    if site.water_scale_height_m <= 100.0 {
        tracing::info!("User has not set water scale height, using 2km");
    }
}
