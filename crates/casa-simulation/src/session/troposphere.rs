// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.settrop`: tropospheric phase, which `corrupt` applies (#841 Phase
//! 4d). The model and where it departs from CASA are the
//! `corrupt::troposphere` module's.
//!
//! As CASA's `Simulator::settrop`, the call simulates its term over the
//! integrations observed so far, with the seed in force (`sm.setseed`), the
//! atmosphere evaluated for every spectral window defined then, and adds it
//! to the terms every later `corrupt` applies, until `reset`. Each window's
//! channels are independent and are computed on the session's workers, as
//! are a screen's transform's. What the term keeps (a solution per
//! integration and window, an excess per slot and antenna, and each
//! window's delays) is admitted with what forming it holds (the largest
//! window's atmosphere, or the screen and its transform) before anything is
//! formed, and stays charged while the term lives.

use casa_atmosphere::{AbsorptionProfile, AtmosphereProfile, ChannelTotals, ProfileParameters};
use casa_imaging_runtime::pass::WorkerTeam;
use num_complex::Complex32;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use super::tsys_atm::{building_bytes, window_totals};
use crate::corrupt::jones::{Slots, TermBytes, tropospheric};
use crate::corrupt::troposphere::{
    FEWEST_SERIES_SLOTS, ScreenLayout, TroposphericPhase, antenna_series, screen_excess, wet_delays,
};
use crate::corrupt::tsys_atm::{self, single};

/// CASA's screens' slot width when `simint` is not positive, seconds
/// (`TJones::createCorruptor`).
const DEFAULT_SLOT_S: f64 = 10.0;

/// The narrowest slot CASA makes, seconds.
const NARROWEST_SLOT_S: f64 = 0.1;

/// The largest fluctuation `TJones::createCorruptor` keeps, as a fraction
/// of the mean water vapour.
const LARGEST_DELTA_PWV: f64 = 1.0;

/// The arguments of `sm.settrop`.
#[derive(Debug, Clone, PartialEq)]
pub struct TroposphereSpec {
    /// `mode`: `individual`, a series of its own above each antenna, or
    /// `screen`, one screen blown over the array; `test` (CASA's debugging
    /// ramp) is not implemented.
    pub mode: String,
    /// `table`: the `Tf Jones` calibration table the simulated phases are
    /// written to, or `''` for none. CASA writes it at once; casa-rs at
    /// `close`.
    pub table: String,
    /// `pwv`: the mean precipitable water vapour, mm.
    pub water_vapour_mm: f64,
    /// `deltapwv`: the fluctuations' root-mean-square as a fraction of
    /// `pwv`; at most 1.
    pub delta_pwv: f64,
    /// `beta`: the fluctuations' spectral exponent (fractional Brownian
    /// motion).
    pub beta: f64,
    /// `windspeed`: the speed `screen` mode blows its screen at, m/s.
    pub windspeed_m_s: f64,
    /// `simint`: the screens' time step, s; not positive for CASA's 10 s.
    pub simint_s: f64,
}

impl Default for TroposphereSpec {
    /// `sm.settrop`'s defaults.
    fn default() -> Self {
        Self {
            mode: "screen".to_string(),
            table: String::new(),
            water_vapour_mm: 3.0,
            delta_pwv: 0.15,
            beta: 1.1,
            windspeed_m_s: 7.0,
            simint_s: -1.0,
        }
    }
}

/// `settrop`'s refusals of a term CASA cannot form: `create_corrupt`'s
/// exception, as `settrop` logs it.
fn not_formed(reason: &str) -> SimulatorError {
    SimulatorError::refused(format!(
        "Caught exception: Error in Simulator::create_corrupt: {reason}"
    ))
}

impl Simulator {
    /// `sm.settrop(...)`: simulate the troposphere's phase over the rows
    /// observed so far and add it to the terms `corrupt` applies.
    ///
    /// # Errors
    ///
    /// - An unknown mode is refused as CASA's "unsupported mode";
    ///   `mode='test'` is not yet implemented.
    /// - No rows observed yet; a `pwv` that is not positive; integrations
    ///   spanning less than one slot (CASA's `setEvenSlots` "logic
    ///   problem"): refused.
    /// - What CASA's checks let through into undefined phases: an argument
    ///   that is not a finite number in single precision; in `individual`
    ///   mode, integrations spanning fewer than six slots, on which a
    ///   series is flat; in `screen` mode, a negative `windspeed`, a dish of
    ///   no size, or a screen too large to make: refused.
    /// - A `table` path that cannot be made absolute, or that is, lies
    ///   inside or contains the session's MeasurementSet: raised.
    /// - The term's memory does not fit the session's resource policy: an
    ///   admission error, before anything is formed.
    pub fn set_trop(&mut self, spec: &TroposphereSpec) -> SimulatorResult<()> {
        let screen = match spec.mode.as_str() {
            "individual" => false,
            "screen" => true,
            "test" => {
                return Err(SimulatorError::not_yet_implemented(
                    "settrop(mode='test'), CASA's debugging ramp, is not implemented in casa-rs",
                ));
            }
            other => {
                return Err(SimulatorError::refused(format!(
                    "Caught exception: unsupported mode {other} in settrop()"
                )));
            }
        };
        check_numbers(spec, screen)?;
        let table = self.table_path(&spec.table)?;
        self.require_rows()?;
        tracing::info!("Creating TF Calibration structure for data corruption.");
        // As the corruptor holds them, in single precision.
        let pwv_mm = single(spec.water_vapour_mm);
        if pwv_mm <= 0.0 {
            return Err(not_formed(
                "AtmCorruptor attempted initialization with undefined PWV",
            ));
        }
        let mut scale = single(spec.delta_pwv);
        if scale > LARGEST_DELTA_PWV {
            tracing::warn!(" decreasing PWV fluctuation magnitude to 100% of the mean PWV ");
            scale = LARGEST_DELTA_PWV;
        }
        tracing::info!(" PWV fluctuations = {scale} of mean PWV which is {pwv_mm}mm ");
        let slot_s = slot_width(spec.simint_s);
        let (per_window, span) = self.integration_counts();
        let [start, stop] = span.unwrap_or([0.0, 0.0]);
        if (stop - start) / slot_s < 1.0 {
            return Err(not_formed(
                "logic problem Corruptor::setEvenSlots called before start/stopTime set",
            ));
        }
        let slots = Slots::spanning(span, slot_s);
        if !screen && slots.count < FEWEST_SERIES_SLOTS {
            return Err(SimulatorError::refused(format!(
                "Caught exception: settrop(mode='individual') needs integrations spanning at \
                 least {FEWEST_SERIES_SLOTS} slots of {slot_s} s for its series to vary; these \
                 span {}",
                slots.count
            )));
        }
        let windspeed_m_s = single(spec.windspeed_m_s);
        let layout = if screen {
            let positions: Vec<[f64; 3]> = self
                .antennas
                .iter()
                .map(|antenna| antenna.position_itrf_m)
                .collect();
            let diameters: Vec<f64> = self
                .antennas
                .iter()
                .map(|antenna| antenna.diameter_m)
                .collect();
            Some(
                ScreenLayout::new(&positions, &diameters, (windspeed_m_s, stop - start))
                    .map_err(|reason| not_formed(&reason))?,
            )
        } else {
            None
        };
        let antennas = self.antennas.len();
        let workers = self.options.resources.cores().workers.max(1);
        let bytes = self.troposphere_bytes((&per_window, &slots), layout.as_ref(), workers);
        let reservation = self.admit_term(bytes)?;
        let delays = self.wet_delays(pwv_mm * 1e-3)?;
        let beta = single(spec.beta);
        let excess = match &layout {
            None => antenna_series(self.seed, (beta, scale), slots.count, antennas),
            Some(layout) => {
                tracing::info!(
                    "creating new fBM of size {},{} (may take a few minutes) ...",
                    layout.shape[1],
                    layout.shape[0]
                );
                screen_excess(
                    self.seed,
                    (beta, scale),
                    layout,
                    (&slots, windspeed_m_s),
                    workers,
                )
                .map_err(|error| SimulatorError::raised(format!("the screen: {error}")))?
            }
        };
        let chunks = self.integration_chunks(&per_window);
        let term = tropospheric(
            antennas,
            (chunks, &slots),
            TroposphericPhase::new(excess, delays),
        );
        self.hold_term(term, (reservation, bytes), table);
        Ok(())
    }

    /// What a tropospheric term over integrations counted `per_window` on
    /// `slots` keeps, and what forming it holds: building the largest
    /// window's atmosphere, then the excess, through `layout`'s screen and
    /// its transform on `workers` threads, or one series' transform.
    fn troposphere_bytes(
        &self,
        (per_window, slots): (&[usize], &Slots),
        layout: Option<&ScreenLayout>,
        workers: usize,
    ) -> TermBytes {
        let channels: Vec<usize> = self
            .spectral_windows
            .iter()
            .map(|window| window.channel_count)
            .collect();
        let layers = AtmosphereProfile::new(&ProfileParameters::default())
            .map_or(0, |atmosphere| atmosphere.layers().len());
        let atmosphere = channels
            .iter()
            .map(|&channels| building_bytes(channels, layers))
            .max()
            .unwrap_or(0);
        let excess = layout.map_or_else(
            || TermBytes::fbm(&[], slots.count, 0).forming,
            |layout| {
                let elements = layout.spectrum_len();
                (elements * size_of::<Complex32>()) as u64
                    + casa_fft::plan_pair_bytes(workers) as u64
                    + casa_fft::planning_bytes::<f32>(elements)
            },
        );
        TermBytes::troposphere(
            (per_window, &channels),
            (slots.count, self.antennas.len()),
            atmosphere.max(excess),
        )
    }

    /// Each spectral window's zenith wet phase delay per channel, radians,
    /// for a water column of `water_column_m` at `settrop`'s site (CASA's
    /// simulator defaults, which `settrop` fixes), at each channel's
    /// frequency.
    fn wet_delays(&self, water_column_m: f64) -> SimulatorResult<Vec<Option<Vec<f64>>>> {
        let atmosphere_error = |error: casa_atmosphere::AtmosphereError| {
            not_formed(&format!("error in ATM setup - {error}"))
        };
        let atmosphere =
            AtmosphereProfile::new(&ProfileParameters::default()).map_err(atmosphere_error)?;
        tracing::info!("Initializing ATM");
        let team = WorkerTeam::new(self.options.resources.cores().workers.max(1))
            .map_err(|error| SimulatorError::raised(format!("the worker team: {error}")))?;
        let mut delays = Vec::with_capacity(self.spectral_windows.len());
        for spectral_window in &self.spectral_windows {
            let channels = spectral_window.channel_frequencies_hz();
            let grid = tsys_atm::window_grid(&channels)
                .map_err(|error| SimulatorError::refused(format!("Caught exception: {error}")))?;
            let totals = window_totals(&team, &atmosphere, &channels)?;
            let profile: AbsorptionProfile<ChannelTotals> =
                AbsorptionProfile::from_channels(atmosphere.clone(), grid, totals)
                    .map_err(|error| SimulatorError::raised(format!("the atmosphere: {error}")))?;
            delays.push(Some(wet_delays(&profile, water_column_m)));
        }
        Ok(delays)
    }
}

/// Refuse what CASA would turn into undefined screens: an argument that is
/// not a number, or is infinite in the single precision CASA's corruptor
/// holds it in (CASA's checks let it through, and its phases come out NaN;
/// #899 review), and in `screen` mode a negative `windspeed`, which would
/// blow the screen off its west end (CASA's screen shrinks and its antennas
/// read pixels before its first). A `deltapwv` past single precision is
/// held to 1, as any above 1 is.
fn check_numbers(spec: &TroposphereSpec, screen: bool) -> SimulatorResult<()> {
    for (name, value, held) in [
        ("pwv", spec.water_vapour_mm, single(spec.water_vapour_mm)),
        ("deltapwv", spec.delta_pwv, spec.delta_pwv),
        ("beta", spec.beta, single(spec.beta)),
        ("windspeed", spec.windspeed_m_s, single(spec.windspeed_m_s)),
        ("simint", spec.simint_s, single(spec.simint_s)),
    ] {
        if !held.is_finite() {
            return Err(SimulatorError::refused(format!(
                "Caught exception: settrop's {name} must be a finite number in single \
                 precision, not {value}"
            )));
        }
    }
    if screen && spec.windspeed_m_s < 0.0 {
        return Err(SimulatorError::refused(format!(
            "Caught exception: settrop's windspeed must not be negative (the screen is blown \
             east), not {}",
            spec.windspeed_m_s
        )));
    }
    Ok(())
}

/// The screens' slot width for `simint_s`: CASA's 10 s when it is not
/// positive, else `simint_s` and at least 0.1 s (`TJones::createCorruptor`).
fn slot_width(simint_s: f64) -> f64 {
    if simint_s <= 0.0 {
        return DEFAULT_SLOT_S;
    }
    if simint_s < NARROWEST_SLOT_S {
        tracing::warn!(
            " Requested phase screen time granularity ({simint_s} s) is too small! Will use the \
             minimum permitted value: {NARROWEST_SLOT_S} s."
        );
    }
    simint_s.max(NARROWEST_SLOT_S)
}
