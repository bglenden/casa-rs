// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setnoise(mode='tsys-atm')`: thermal noise whose σ comes from a
//! system temperature the atmosphere model gives, channel by channel (CASA's
//! `AtmosCorruptor` with ATM, #841 Phase 4c).
//!
//! # What CASA computes
//!
//! As for `tsys-manual` ([`super::tsys`]), `Simulator::setnoise` adds a
//! unit `ANoise` and a `T` term flagged `NoiseScale`, here a `Tf Jones`
//! (type "TF NOISE"), which depends on frequency: `SolvableVisCal` asks the
//! corruptor for every channel of the window (`setFocusChan`), so σ is
//! per channel. Antenna `i`'s `T` in channel `c` is `D_i / f / √Tsys(A_i, c)`
//! with `f` as for `tsys-manual`, so a row of antennas `i` and `j` gets
//! σ_ij(c) = c·a/√(Δν t) · √(Tsys(A_i, c) Tsys(A_j, c)) / (D_i D_j), and
//!
//! ```text
//! Tsys(A, c) = (hν/k) · e^(τ A) · (1/(e^(hν/k / T_ebb(A)) − 1) + T_rx/(hν/k)) · (1 + g)
//! ```
//!
//! (`AtmosCorruptor::tsys` in its ATM branch), with τ the zenith opacity
//! (ATM's dry opacity plus its wet opacity for `pwv`) and `T_ebb` the sky's
//! equivalent blackbody temperature at airmass `A`, the receiver coupled to
//! the sky by `spillefficiency` with the rest of its beam on the ground at
//! `tground` (`SkyStatus::getTebbSky(…, pwv, A, spilleff, tground)`). hν/k
//! is 4.80145·10⁻¹¹ K/Hz times the channel's frequency.
//!
//! The atmosphere is ATM's (`casa-atmosphere`) for CASA's simulator site
//! defaults: `altitude` 5000 m, `pground` 560 hPa, `tground` 270 K,
//! `relhum` 20 %, `waterheight` 2 km, a −5.6 K/km lapse rate, 10 hPa steps,
//! top 48 km and ATM's tropical reference atmosphere; `setnoise` replaces an
//! unset `pwv` (≤ 0) with 1 mm, `pground` ≤ 100 hPa with 560, `relhum` ≤ 0
//! with 20, `altitude` ≤ 0 with 5000 m and `waterheight` ≤ 100 m with 2 km.
//!
//! Measured against CASA 6.7.6 (probe of 64-channel windows over the
//! 183 GHz and 325 GHz water lines, `tools/simulation`), every channel's
//! noise variance agrees with this formula through `casa-atmosphere` to the
//! probe's statistics (1 %), once CASA's frequency mapping below is used.
//!
//! # Where casa-rs departs
//!
//! Recorded in `docs/CASA (C++) bugs.md`:
//! - CASA's ATM grid for a window is centred on its REF_FREQUENCY, the first
//!   channel's frequency: ATM channel `i` sits at `REF + (i − n/2)·Δν`
//!   (`n/2 + 1` for windows after the first), so Tsys(c) is the atmosphere's
//!   half a band below channel `c`. casa-rs evaluates the atmosphere at each
//!   channel's own frequency (CHAN_FREQ).
//! - For a window of one channel after the first, CASA reads its channel map
//!   past its end and the term's `T` comes out bad: those rows get unscaled,
//!   1 Jy noise. casa-rs evaluates the atmosphere at the channel's frequency
//!   as for the first window.
//! - As for `tsys-manual`, casa-rs takes each row's own airmasses and
//!   EXPOSURE, where CASA caches airmasses for 100 s and uses the first
//!   row's EXPOSURE.

use casa_atmosphere::{
    AbsorptionProfile, AtmosphereProfile, AtmosphereType, ChannelTotals, ProfileParameters, Sky,
    SkyColumn, SpectralGrid,
};
use casa_imaging_runtime::Reservation;

use super::tsys::TsysParameters;

/// h/k, K/Hz, as `AtmosCorruptor::tsys` writes it.
const PLANCK_OVER_BOLTZMANN: f64 = 4.801_45e-11;

/// The sky background ATM's `SkyStatus` takes, K.
const BACKGROUND_K: f64 = 2.73;

/// The site and atmosphere arguments of `setnoise(mode='tsys-atm')`, as
/// the user gives them: unset values (at or below CASA's thresholds) take
/// CASA's defaults ([`AtmosphereSpec::resolved`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtmosphereSpec {
    /// `pground`: the ground pressure, hPa (mbar); 560 when at most 100.
    pub ground_pressure_hpa: f64,
    /// `relhum`: the ground relative humidity, %; 20 when not positive.
    pub relative_humidity_percent: f64,
    /// `altitude`: the site's altitude, m; 5000 when not positive.
    pub altitude_m: f64,
    /// `waterheight`: the water vapour scale height, m; 2 km when at most
    /// 100 m.
    pub water_scale_height_m: f64,
}

impl Default for AtmosphereSpec {
    /// Every value unset, as `sm.setnoise`'s defaults leave them.
    fn default() -> Self {
        Self {
            ground_pressure_hpa: 0.0,
            relative_humidity_percent: 0.0,
            altitude_m: 0.0,
            water_scale_height_m: 0.0,
        }
    }
}

impl AtmosphereSpec {
    /// The ATM profile parameters `Simulator::setnoise` and
    /// `AtmosCorruptor::initAtm` make of these values, the ground at
    /// `ground_k` (`tground`): each in single precision, as the corruptor
    /// reads them (`asFloat`).
    pub(crate) fn resolved(&self, ground_k: f64) -> ProfileParameters {
        let pick = |value: f64, unset: bool, default: f64| {
            if unset { default } else { single(value) }
        };
        ProfileParameters {
            altitude_m: pick(self.altitude_m, self.altitude_m <= 0.0, 5000.0),
            ground_pressure_hpa: pick(
                self.ground_pressure_hpa,
                self.ground_pressure_hpa <= 100.0,
                560.0,
            ),
            ground_temperature_k: single(ground_k),
            lapse_rate_k_per_km: -5.6,
            relative_humidity_percent: pick(
                self.relative_humidity_percent,
                self.relative_humidity_percent <= 0.0,
                20.0,
            ),
            water_vapour_scale_height_km: pick(
                self.water_scale_height_m / 1000.0,
                self.water_scale_height_m <= 100.0,
                2.0,
            ),
            pressure_step_hpa: 10.0,
            top_altitude_km: 48.0,
            atmosphere: AtmosphereType::Tropical,
        }
    }
}

/// `value` rounded to single precision, as CASA's corruptor holds its
/// parameters.
pub(crate) fn single(value: f64) -> f64 {
    f64::from(value as f32)
}

/// A spectral window's atmosphere for the noise: each channel's line of
/// sight, zenith opacity (single precision, as CASA holds it) and hν/k.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WindowAtmosphere {
    columns: Vec<SkyColumn>,
    opacity: Vec<f32>,
    h_nu_over_k: Vec<f64>,
}

impl WindowAtmosphere {
    /// The heap a window of `channels` channels keeps for an atmosphere of
    /// `layers` layers.
    pub(crate) fn bytes(channels: usize, layers: usize) -> u64 {
        channels as u64
            * (size_of::<SkyColumn>() as u64
                + SkyColumn::heap_bytes(layers)
                + size_of::<f32>() as u64
                + size_of::<f64>() as u64)
    }

    /// The window of `profile`'s channels, the sky seen through `pwv_m` of
    /// water with `spillover` of the beam on the sky and the rest on the
    /// ground at `ground_k`, each as the caller holds it ([`single`]).
    pub(crate) fn new(
        profile: &AbsorptionProfile<ChannelTotals>,
        pwv_m: f64,
        (spillover, ground_k): (f64, f64),
    ) -> Result<Self, casa_atmosphere::AtmosphereError> {
        let sky = Sky {
            water_column_m: pwv_m,
            airmass: 1.0,
            sky_coupling: spillover,
            spill_temperature_k: ground_k,
            background_temperature_k: BACKGROUND_K,
        };
        let channels = profile.channels().len();
        let mut columns = Vec::with_capacity(channels);
        let mut opacity = Vec::with_capacity(channels);
        let mut h_nu_over_k = Vec::with_capacity(channels);
        for (channel, frequency_hz) in profile.grid().frequencies_hz().enumerate() {
            columns.push(profile.sky_column(channel, &sky)?);
            // `AtmosCorruptor::opac`: dry plus wet, each held as a `Float`.
            opacity.push(
                profile.dry_opacity(channel) as f32 + profile.wet_opacity(channel, pwv_m) as f32,
            );
            h_nu_over_k.push(PLANCK_OVER_BOLTZMANN * frequency_hz);
        }
        Ok(Self {
            columns,
            opacity,
            h_nu_over_k,
        })
    }

    /// The window's channels.
    pub(crate) fn channels(&self) -> usize {
        self.columns.len()
    }

    /// Channel `channel`'s zenith opacity.
    pub(crate) fn zenith_opacity(&self, channel: usize) -> f32 {
        self.opacity[channel]
    }
}

/// The `tsys-atm` model as CASA's corruptor holds it: single-precision
/// parameters, and each spectral window's atmosphere, with the memory
/// admitted for what it keeps.
#[derive(Debug)]
pub(crate) struct TsysAtm {
    /// `senscoeff() · amp()` in single precision: c · a, Jy m² per kelvin.
    jansky_per_kelvin: f32,
    receiver_k: f32,
    /// 1 + g.
    sidebands: f64,
    /// By spectral window id; `None` for a window defined after `setnoise`.
    windows: Vec<Option<WindowAtmosphere>>,
    _reservation: Reservation,
}

impl TsysAtm {
    /// The model of `parameters` over `windows` (by spectral window id),
    /// holding `reservation`.
    pub(crate) fn new(
        parameters: &TsysParameters,
        windows: Vec<Option<WindowAtmosphere>>,
        reservation: Reservation,
    ) -> Self {
        let manual = super::tsys::TsysManual::new(parameters);
        Self {
            jansky_per_kelvin: manual.jansky_per_kelvin(),
            receiver_k: parameters.receiver_k as f32,
            sidebands: manual.sidebands(),
            windows,
            _reservation: reservation,
        }
    }

    /// Whether the model has spectral window `window`'s atmosphere.
    pub(crate) fn covers(&self, window: usize) -> bool {
        self.windows.get(window).is_some_and(Option::is_some)
    }

    /// Tsys in channel `channel` of `window` at airmass `airmass`, K, in
    /// single precision (`AtmosCorruptor::tsys`).
    pub(crate) fn tsys(&self, window: &WindowAtmosphere, channel: usize, airmass: f32) -> f32 {
        let h_nu_over_k = window.h_nu_over_k[channel];
        let tau = f64::from(window.opacity[channel] * airmass);
        let tebb = window.columns[channel].tebb(f64::from(airmass));
        let radiance = tau.exp()
            * (1.0 / ((h_nu_over_k / tebb).exp() - 1.0) + f64::from(self.receiver_k) / h_nu_over_k);
        (h_nu_over_k * radiance * self.sidebands) as f32
    }

    /// Spectral window `window`'s atmosphere.
    ///
    /// # Panics
    ///
    /// When the model does not cover the window ([`Self::covers`]).
    pub(crate) fn window(&self, window: usize) -> &WindowAtmosphere {
        self.windows[window]
            .as_ref()
            .expect("corrupt refuses a window the tsys-atm term does not cover")
    }

    /// Each channel's `1/T` for an antenna of diameter `diameter_m` at
    /// airmass `airmass`, in a window whose channels are
    /// `channel_bandwidth_hz` wide, for an integration of `exposure_s`,
    /// into `scales` (one a channel): the antenna's factor of σ (`s` in
    /// [`super::tsys`]), 1 where CASA applies a bad `T` as 1.
    pub(crate) fn antenna_scales(
        &self,
        window: &WindowAtmosphere,
        (diameter_m, airmass): (f64, f32),
        (channel_bandwidth_hz, exposure_s): (f64, f64),
        scales: &mut [f32],
    ) {
        let factor =
            (f64::from(self.jansky_per_kelvin) / (channel_bandwidth_hz * exposure_s).sqrt()).sqrt();
        for (channel, scale) in scales.iter_mut().enumerate() {
            let tsys = self.tsys(window, channel, airmass);
            let jones = (diameter_m / factor / f64::from(tsys.sqrt())) as f32;
            *scale = if jones.is_nan() || jones == 0.0 {
                1.0
            } else {
                1.0 / jones
            };
        }
    }
}

/// A grid of one window of the channels at `frequencies_hz`.
pub(crate) fn window_grid(
    frequencies_hz: &[f64],
) -> Result<SpectralGrid, casa_atmosphere::AtmosphereError> {
    let mut grid = SpectralGrid::new();
    grid.add_channels(frequencies_hz, 0)?;
    Ok(grid)
}

/// Each channel's totals of `atmosphere` at `frequencies_hz`: one
/// independent computation per channel, which a caller spreads over its
/// workers.
pub(crate) fn channel_totals(
    atmosphere: &AtmosphereProfile,
    frequencies_hz: &[f64],
) -> Vec<ChannelTotals> {
    frequencies_hz
        .iter()
        .map(|&frequency_hz| casa_atmosphere::absorption::channel_totals(atmosphere, frequency_hz))
        .collect()
}

#[cfg(test)]
mod tests {
    use casa_atmosphere::ProfileParameters;

    use super::AtmosphereSpec;

    /// `sm.setnoise`'s Python defaults (`relhum=20`, the quantities `[]`)
    /// resolve to the site CASA logs for them ("altitude=5000m,
    /// Pground=560mb, Tground=270K, humidity= 20%, water scale
    /// height=2000m"), which `casa-atmosphere`'s defaults are and the
    /// Phase 4c probe ran with.
    #[test]
    fn setnoise_defaults_resolve_to_casas_site() {
        let python = AtmosphereSpec {
            relative_humidity_percent: 20.0,
            ..AtmosphereSpec::default()
        };
        assert_eq!(python.resolved(270.0), ProfileParameters::default());
        assert_eq!(
            AtmosphereSpec::default().resolved(270.0),
            ProfileParameters::default()
        );
    }

    /// Values at CASA's thresholds are unset; values past them are held in
    /// single precision, as the corruptor reads them (`asFloat`), the water
    /// scale height in kilometres.
    #[test]
    fn site_values_past_casas_thresholds_are_taken_in_single_precision() {
        let thresholds = AtmosphereSpec {
            ground_pressure_hpa: 100.0,
            relative_humidity_percent: 0.0,
            altitude_m: 0.0,
            water_scale_height_m: 100.0,
        };
        assert_eq!(thresholds.resolved(270.0), ProfileParameters::default());
        let given = AtmosphereSpec {
            ground_pressure_hpa: 555.1,
            relative_humidity_percent: 33.3,
            altitude_m: 5058.7,
            water_scale_height_m: 1234.5,
        };
        let resolved = given.resolved(269.9);
        let single = |value: f64| f64::from(value as f32);
        assert_eq!(resolved.ground_pressure_hpa, single(555.1));
        assert_eq!(resolved.relative_humidity_percent, single(33.3));
        assert_eq!(resolved.altitude_m, single(5058.7));
        assert_eq!(resolved.water_vapour_scale_height_km, single(1.2345));
        assert_eq!(resolved.ground_temperature_k, single(269.9));
        assert_ne!(resolved.ground_pressure_hpa, 555.1);
    }
}
