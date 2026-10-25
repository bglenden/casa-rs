// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMSkyStatus.cc (`RT`, `RTRJ` and their accessors),
// (c) Instituto de Estructura de la Materia, Pardo et al.,
// LGPL-2.1-or-later.
//! The sky's brightness through the atmosphere (ATM's `SkyStatus`, forward
//! radiative transfer only).
//!
//! Along a line of sight of airmass m, layer j of opacity τⱼ (its dry
//! opacity plus its wet opacity scaled to the water column) at temperature
//! Tⱼ adds B(Tⱼ)·e^(−m Σ_{i<j} τᵢ)·(1 − e^(−m τⱼ)) to the radiance, where
//! B(T) = 1/(e^(hν/kT) − 1) is the Planck occupation. The background at
//! T_bg is attenuated by the whole column. A receiver with sky coupling η
//! sees η times that plus (1 − η)·B(T_spill) from its spillover. The
//! equivalent blackbody temperature is the T with B(T) equal to the
//! radiance, T_ebb = (hν/k) / ln(1 + 1/radiance); the Rayleigh–Jeans
//! temperature is (hν/k)·radiance.
//!
//! ATM's `SkyStatus` keeps a user water column, airmass and background
//! temperature as state and offers dozens of overloads that default some
//! of them. Here every condition is an explicit field of [`Sky`]. The
//! water-vapour retrievals from radiometers and FTS spectra, and the
//! sky-coupling retrievals, are not ported.

use crate::AtmosphereError;
use crate::absorption::{AbsorptionChannel, AbsorptionProfile};

/// h/k in K/GHz, as ATM's radiative transfer writes it.
const H_OVER_K: f64 = 0.04799274551;

/// The conditions a sky brightness is computed for.
///
/// ATM's defaults differ by accessor. Its `SkyStatus` starts with a 1 mm
/// water column (CASA's `at` tool resets it to the profile's own column),
/// airmass 1 and a 2.73 K background. The per-channel `getTebbSky`
/// overloads default to coupling 1 and a 100 K spillover; the window
/// averages `getAverageTebbSky` to coupling 1 and the ground temperature.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sky {
    /// Zenith precipitable water column, metres.
    pub water_column_m: f64,
    /// Airmass of the line of sight (1 at the zenith).
    pub airmass: f64,
    /// Fraction of the beam on the sky (the rest sees the spillover).
    pub sky_coupling: f64,
    /// Temperature of the spillover, kelvin.
    pub spill_temperature_k: f64,
    /// Temperature of the sky background beyond the atmosphere, kelvin.
    pub background_temperature_k: f64,
}

impl Sky {
    /// The conditions of ATM's per-channel `getTebbSky(spwid, nc, wh2o)`:
    /// the zenith, perfect coupling, a 100 K spillover and a 2.73 K
    /// background.
    #[must_use]
    pub fn zenith(water_column_m: f64) -> Self {
        Self {
            water_column_m,
            airmass: 1.0,
            sky_coupling: 1.0,
            spill_temperature_k: 100.0,
            background_temperature_k: 2.73,
        }
    }

    /// ATM's checks before radiative transfer: a non-negative water column,
    /// an airmass of at least 1 and a spillover between 0 and 350 K, and
    /// (for a single channel) a coupling between 0 and 1.
    fn validate(&self, check_coupling: bool) -> Result<(), AtmosphereError> {
        let invalid = |what: String| Err(AtmosphereError::InvalidParameter(what));
        if self.water_column_m.is_nan() || self.water_column_m < 0.0 {
            return invalid(format!(
                "water column {} m is negative",
                self.water_column_m
            ));
        }
        if check_coupling && !(0.0..=1.0).contains(&self.sky_coupling) {
            return invalid(format!(
                "sky coupling {} is outside 0 to 1",
                self.sky_coupling
            ));
        }
        if self.airmass.is_nan() || self.airmass < 1.0 {
            return invalid(format!("airmass {} is below 1", self.airmass));
        }
        if !(0.0..=350.0).contains(&self.spill_temperature_k) {
            return invalid(format!(
                "spillover temperature {} K is outside 0 to 350 K",
                self.spill_temperature_k
            ));
        }
        Ok(())
    }
}

/// The Planck occupation 1/(e^(hν/kT) − 1) at `frequency_ghz`.
fn occupation(frequency_ghz: f64, temperature_k: f64) -> f64 {
    1.0 / ((H_OVER_K * frequency_ghz / temperature_k).exp() - 1.0)
}

impl<C: AbsorptionChannel> AbsorptionProfile<C> {
    /// The radiance (Planck occupation) the receiver sees in `channel`:
    /// ATM's `RT` before it converts to a temperature.
    fn radiance(&self, channel: usize, sky: &Sky) -> f64 {
        let column = &self.channels()[channel];
        let frequency_ghz = 1.0e-9 * column.frequency_hz();
        let ratio = sky.water_column_m / self.ground_water_column_m();
        let airmass = sky.airmass;
        let mut radiance = 0.0;
        let mut opacity = 0.0;
        for (j, layer) in self.atmosphere().layers().iter().enumerate() {
            let totals = column.layer_totals(j);
            let tau =
                (totals.wet_absorption() * ratio + totals.dry_absorption()) * layer.thickness_m;
            radiance += occupation(frequency_ghz, layer.temperature_k)
                * (-opacity * airmass).exp()
                * (1.0 - (-airmass * tau).exp());
            opacity += tau;
        }
        sky.sky_coupling
            * (radiance
                + occupation(frequency_ghz, sky.background_temperature_k)
                    * (-opacity * airmass).exp())
            + occupation(frequency_ghz, sky.spill_temperature_k) * (1.0 - sky.sky_coupling)
    }

    /// The sky's equivalent blackbody temperature in `channel`, kelvin
    /// (ATM's `getTebbSky`).
    ///
    /// # Errors
    ///
    /// [`AtmosphereError::InvalidParameter`] for a negative water column,
    /// an airmass below 1, a coupling outside 0 to 1 or a spillover
    /// outside 0 to 350 K, where ATM returns −999 K.
    pub fn tebb_sky(&self, channel: usize, sky: &Sky) -> Result<f64, AtmosphereError> {
        sky.validate(true)?;
        Ok(self.tebb(channel, sky))
    }

    fn tebb(&self, channel: usize, sky: &Sky) -> f64 {
        let frequency_ghz = 1.0e-9 * self.channels()[channel].frequency_hz();
        H_OVER_K * frequency_ghz / (1.0 + 1.0 / self.radiance(channel, sky)).ln()
    }

    /// The sky's Rayleigh–Jeans temperature in `channel`, kelvin (ATM's
    /// `getTrjSky`).
    ///
    /// # Errors
    ///
    /// As [`AbsorptionProfile::tebb_sky`].
    pub fn trj_sky(&self, channel: usize, sky: &Sky) -> Result<f64, AtmosphereError> {
        sky.validate(true)?;
        Ok(self.trj(channel, sky))
    }

    fn trj(&self, channel: usize, sky: &Sky) -> f64 {
        let frequency_ghz = 1.0e-9 * self.channels()[channel].frequency_hz();
        H_OVER_K * frequency_ghz * self.radiance(channel, sky)
    }

    /// The equivalent blackbody temperature averaged over spectral window
    /// `window`, kelvin (ATM's `getAverageTebbSky`): the mean of the
    /// channels' temperatures. Like ATM, it does not check the coupling.
    ///
    /// # Errors
    ///
    /// As [`AbsorptionProfile::tebb_sky`], less the coupling check.
    ///
    /// # Panics
    ///
    /// If `window` is not a window of the grid.
    pub fn average_tebb_sky(&self, window: usize, sky: &Sky) -> Result<f64, AtmosphereError> {
        sky.validate(false)?;
        Ok(self.channel_mean(window, |channel| self.tebb(channel, sky)))
    }

    /// The Rayleigh–Jeans temperature averaged over spectral window
    /// `window`, kelvin (ATM's `getAverageTrjSky`).
    ///
    /// # Errors
    ///
    /// As [`AbsorptionProfile::average_tebb_sky`].
    ///
    /// # Panics
    ///
    /// If `window` is not a window of the grid.
    pub fn average_trj_sky(&self, window: usize, sky: &Sky) -> Result<f64, AtmosphereError> {
        sky.validate(false)?;
        Ok(self.channel_mean(window, |channel| self.trj(channel, sky)))
    }

    /// Channel `channel`'s line of sight under `sky`'s conditions, prepared
    /// for the sky temperature at many airmasses ([`SkyColumn`]); `sky`'s
    /// airmass is not used.
    ///
    /// # Errors
    ///
    /// As [`AbsorptionProfile::tebb_sky`], less the airmass.
    pub fn sky_column(&self, channel: usize, sky: &Sky) -> Result<SkyColumn, AtmosphereError> {
        Sky {
            airmass: 1.0,
            ..*sky
        }
        .validate(true)?;
        let column = &self.channels()[channel];
        let frequency_ghz = 1.0e-9 * column.frequency_hz();
        let ratio = sky.water_column_m / self.ground_water_column_m();
        let (opacity, occupations) = self
            .atmosphere()
            .layers()
            .iter()
            .enumerate()
            .map(|(j, layer)| {
                let totals = column.layer_totals(j);
                (
                    (totals.wet_absorption() * ratio + totals.dry_absorption()) * layer.thickness_m,
                    occupation(frequency_ghz, layer.temperature_k),
                )
            })
            .unzip();
        Ok(SkyColumn {
            frequency_ghz,
            opacity,
            occupation: occupations,
            sky_coupling: sky.sky_coupling,
            background: occupation(frequency_ghz, sky.background_temperature_k),
            spill: occupation(frequency_ghz, sky.spill_temperature_k),
        })
    }

    /// ATM's `RT` over a window: each channel's value divided by the
    /// channel count, summed.
    fn channel_mean(&self, window: usize, value: impl Fn(usize) -> f64) -> f64 {
        let first = self
            .grid()
            .flat_index(window, 0)
            .expect("a spectral window of the grid");
        let count = self.grid().windows()[window].num_channels();
        (first..first + count).fold(0.0, |sum, channel| sum + value(channel) / count as f64)
    }
}

/// One channel's line of sight under fixed conditions (water column,
/// coupling, spillover and background), prepared for the sky temperature
/// at many airmasses ([`AbsorptionProfile::sky_column`]).
///
/// It keeps each layer's zenith opacity and Planck occupation, so a
/// temperature costs one exponential per layer: the transmission through
/// the layers below is carried as a product. That agrees with
/// [`AbsorptionProfile::tebb_sky`] to rounding. A caller evaluating many
/// lines of sight per channel (the simulator's `tsys-atm` noise, one per
/// antenna and integration) prepares a column once per channel.
#[derive(Clone, Debug, PartialEq)]
pub struct SkyColumn {
    frequency_ghz: f64,
    /// Each layer's zenith opacity, from the ground up.
    opacity: Vec<f64>,
    /// Each layer's Planck occupation.
    occupation: Vec<f64>,
    sky_coupling: f64,
    /// The background's occupation.
    background: f64,
    /// The spillover's occupation.
    spill: f64,
}

impl SkyColumn {
    /// The heap bytes a column of `layers` layers keeps.
    #[must_use]
    pub const fn heap_bytes(layers: usize) -> u64 {
        (2 * layers * size_of::<f64>()) as u64
    }

    /// The zenith opacity of the whole column.
    #[must_use]
    pub fn zenith_opacity(&self) -> f64 {
        self.opacity.iter().sum()
    }

    /// The radiance (Planck occupation) seen at `airmass`.
    fn radiance(&self, airmass: f64) -> f64 {
        let mut radiance = 0.0;
        let mut transmission = 1.0;
        for (&tau, &occupation) in self.opacity.iter().zip(&self.occupation) {
            let layer = (-airmass * tau).exp();
            radiance += occupation * transmission * (1.0 - layer);
            transmission *= layer;
        }
        self.sky_coupling * (radiance + self.background * transmission)
            + self.spill * (1.0 - self.sky_coupling)
    }

    /// The sky's equivalent blackbody temperature at `airmass`, kelvin, as
    /// [`AbsorptionProfile::tebb_sky`] gives it. The airmass is not checked
    /// (ATM refuses one below 1).
    #[must_use]
    pub fn tebb(&self, airmass: f64) -> f64 {
        H_OVER_K * self.frequency_ghz / (1.0 + 1.0 / self.radiance(airmass)).ln()
    }
}
