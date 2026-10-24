// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setnoise(mode='tsys-manual')`: thermal noise whose σ comes from a
//! system temperature, CASA's `AtmosCorruptor` in its manual mode.
//!
//! # What CASA computes
//!
//! `Simulator::setnoise` adds two terms for `tsys-manual`: an `ANoise` of
//! unit amplitude, whose draws are those of `simplenoise` with σ = 1 Jy, and
//! a `T` Jones term flagged `NoiseScale`, which `VisEquation::collapseForSim`
//! applies in correct mode to the noise alone, not to the model. Antenna
//! `i`'s `T` is (`AtmosCorruptor::simPar`, `CalCorruptor.cc`)
//!
//! ```text
//! T_i = D_i / f / sqrt(Tsys(A_i)),   f = sqrt(c · a / sqrt(Δν · t)),
//! ```
//!
//! so a row of antennas `i` and `j` gets noise of σ = s_i s_j per real and
//! imaginary part, s = 1/T:
//!
//! ```text
//! σ_ij = c · a / sqrt(Δν · t) · sqrt(Tsys(A_i) · Tsys(A_j)) / (D_i · D_j).
//! ```
//!
//! - `D` is the dish diameter (ANTENNA's DISH_DIAMETER), metres.
//! - `a = 8 k · 10²³ · 10⁻⁴ / (η_a η_q π)` (`AtmosCorruptor::initialize`),
//!   `k` = 1.38062·10⁻¹⁶ erg/K: `2k/(η_a η_q A)` in Jy per kelvin for a dish
//!   of area `A = π D²/4`, with `antefficiency` η_a and `correfficiency` η_q.
//! - `c` is `senscoeff`, or 1/√2 when it is not positive.
//! - `Δν` is the row's spectral window's |TOTAL_BANDWIDTH| / NUM_CHAN: one
//!   width for every channel.
//! - `t` is the integration time, EXPOSURE.
//! - `A_i = 1/sin(e_i)` is the airmass at antenna `i`'s elevation `e_i` of
//!   the field's phase centre (`VisIter::azel`), or 1000 below the horizon.
//! - `Tsys(A) = (hν/k) · R · (1 + g)` (`AtmosCorruptor::tsys`, the branch
//!   without frequency dependence that `tsys-manual` takes), with
//!   `τ = τ₀ A` and
//!   `R = η_s R(T_cmb) + e^τ (η_s (1 − e^−τ) R(T_atm) + (1 − η_s) R(T_gnd)
//!   + T_rx / (hν/k))`, `R(T) = 1/(e^(hν/kT) − 1)`: `tau` τ₀,
//!   `spillefficiency` η_s, `tcmb`, `tatmos` (250 K when it is at most
//!   10), `tground` and `trx`, `rxtype` g (0 when negative). ν is the
//!   window's REF_FREQUENCY, in single precision as CASA holds it: the
//!   focus channel of a `T` without frequency dependence is channel 0.
//!   CASA writes `hν/k` with two constants, 4.80145·10⁻¹¹ K/Hz in `tsys` and
//!   4.799274551·10⁻¹¹ in the radiances (`setFocusChan`); both are kept.
//!
//! Measured against CASA 6.7.6 (`tools/simulation`, #841 Phase 3), per-row
//! variances agree with this formula to 4·10⁻⁴ over 2·10⁷ draws, for 12 m
//! and 7 m dishes together, autocorrelations, non-default parameters
//! (`rxtype=1`, `senscoeff`, `tatmos`, …) and two spectral windows.
//!
//! A `T` that is not finite and positive does not scale: CASA's
//! `JonesScal::invert` marks a zero or NaN `T` bad and `setMatByOk` applies
//! a bad `T` as 1. That happens when Tsys overflows single precision (the
//! airmass 1000 below the horizon) or is not a number; that antenna's scale
//! is then 1, and a `T` of +∞ (Tsys 0) scales by 0.
//!
//! The arithmetic follows CASA's precisions (single-precision parameters,
//! airmass, Tsys and `T`; double-precision intermediates), so σ agrees with
//! CASA's to about 10⁻⁷ relatively; the draws differ anyway.
//!
//! # Where casa-rs departs
//!
//! Recorded in `docs/CASA (C++) bugs.md`: CASA caches each antenna's airmass
//! for 100 s of solution times, evaluates the `T` of rows observed after
//! `setnoise` at the nearest earlier solution, uses the EXPOSURE of the
//! MeasurementSet's first row for every row, applies an antenna without an
//! unflagged row as 1, and multiplies every noise term by every Tsys term's
//! scale. casa-rs computes each row's σ from its own integration's
//! elevations and its own EXPOSURE, whatever the flags, and each term adds
//! its own noise.

/// CASA's Boltzmann constant in erg/K (`AtmosCorruptor::initialize`).
const BOLTZMANN_ERG_PER_K: f64 = 1.380_62e-16;

/// h/k, K/Hz, as `AtmosCorruptor::tsys` writes it.
const PLANCK_OVER_BOLTZMANN_TSYS: f64 = 4.801_45e-11;

/// h/k, K/Hz, as `AtmosCorruptor::setFocusChan` writes it for the radiances.
const PLANCK_OVER_BOLTZMANN_RADIANCE: f64 = 0.047_992_745_51 * 1e-9;

/// The airmass CASA gives an antenna whose elevation is not positive.
const BELOW_HORIZON_AIRMASS: f32 = 1000.0;

/// An atmosphere temperature at or below this, K, is replaced by
/// [`DEFAULT_ATMOSPHERE_K`] (`Simulator::setnoise`).
const UNSET_ATMOSPHERE_K: f32 = 10.0;

/// The atmosphere temperature `setnoise` uses when `tatmos` is unset.
const DEFAULT_ATMOSPHERE_K: f32 = 250.0;

/// The arguments of `setnoise(mode='tsys-manual')`, as `Simulator::setnoise`
/// passes them to the corruptor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TsysParameters {
    /// `tau`: the zenith opacity.
    pub tau: f64,
    /// `tatmos`: the atmosphere's temperature, K; 250 K when at most 10.
    pub atmosphere_k: f64,
    /// `antefficiency`: the aperture efficiency.
    pub antenna_efficiency: f64,
    /// `spillefficiency`: the forward (spillover) efficiency.
    pub spillover_efficiency: f64,
    /// `correfficiency`: the correlator efficiency.
    pub correlator_efficiency: f64,
    /// `trx`: the receiver temperature, K.
    pub receiver_k: f64,
    /// `tground`: the ground's temperature, K.
    pub ground_k: f64,
    /// `tcmb`: the cosmic background's temperature, K.
    pub cmb_k: f64,
    /// `senscoeff`: the sensitivity constant; 1/√2 when not positive.
    pub sensitivity: f64,
    /// `rxtype`: 0 for a sideband-separating receiver, 1 for a double
    /// sideband one, whose Tsys is doubled; negative is 0.
    pub receiver_type: i64,
}

impl Default for TsysParameters {
    /// CASA's `sm.setnoise` defaults.
    fn default() -> Self {
        Self {
            tau: 0.1,
            atmosphere_k: 250.0,
            antenna_efficiency: 0.8,
            spillover_efficiency: 0.85,
            correlator_efficiency: 0.88,
            receiver_k: 50.0,
            ground_k: 270.0,
            cmb_k: 2.73,
            sensitivity: 0.0,
            receiver_type: 0,
        }
    }
}

/// The `tsys-manual` model as CASA's corruptor holds it: single-precision
/// parameters, with the defaults `setnoise` fills in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct TsysManual {
    /// τ₀, CASA's `tauscale`.
    tau: f32,
    atmosphere_k: f32,
    ground_k: f32,
    cmb_k: f32,
    receiver_k: f32,
    spillover: f32,
    /// `senscoeff() · amp()`, both single precision, multiplied in single
    /// precision as `simPar` does: c · a, Jy m² per kelvin.
    jansky_per_kelvin: f32,
    /// 1 + g.
    sidebands: f64,
}

/// What Tsys depends on at one frequency: hν/k and the radiances
/// `1/(e^(hν/kT) − 1)` of the background, the ground and the atmosphere.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Band {
    /// hν/k, K, as `tsys` computes it.
    h_nu_over_k: f64,
    cmb: f64,
    ground: f64,
    atmosphere: f64,
}

/// What a spectral window gives its rows' noise.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WindowNoise {
    /// REF_FREQUENCY, Hz.
    pub(crate) reference_frequency_hz: f64,
    /// |TOTAL_BANDWIDTH| / NUM_CHAN, Hz.
    pub(crate) channel_bandwidth_hz: f64,
}

/// What one row gives its noise: its antennas' dishes and airmasses, and
/// its integration time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RowNoise {
    /// DISH_DIAMETER of ANTENNA1 and ANTENNA2, metres.
    pub(crate) diameters_m: [f64; 2],
    /// Their airmasses ([`airmass`]).
    pub(crate) airmass: [f32; 2],
    /// EXPOSURE, seconds.
    pub(crate) exposure_s: f64,
}

/// The airmass CASA gives an antenna at elevation `elevation_rad`:
/// `1/sin(e)` with `e` and its sine in single precision, held in single
/// precision; 1000 when `e` is not positive.
pub(crate) fn airmass(elevation_rad: f64) -> f32 {
    let elevation = elevation_rad as f32;
    if elevation > 0.0 {
        (1.0 / f64::from(elevation.sin())) as f32
    } else {
        BELOW_HORIZON_AIRMASS
    }
}

impl TsysManual {
    /// The model of `parameters`, held as `setnoise` and the corruptor hold
    /// them.
    pub(crate) fn new(parameters: &TsysParameters) -> Self {
        let float = |value: f64| value as f32;
        let atmosphere_k = float(parameters.atmosphere_k);
        let sensitivity = if parameters.sensitivity > 0.0 {
            float(parameters.sensitivity)
        } else {
            float(std::f64::consts::FRAC_1_SQRT_2)
        };
        // `8 k 1e23 1e-4 / (η_a η_q π)`: the efficiencies multiply in
        // single precision, then everything in double, held in single.
        let efficiencies =
            float(parameters.antenna_efficiency) * float(parameters.correlator_efficiency);
        let amplitude = (8.0 * BOLTZMANN_ERG_PER_K * 1e23 * 1e-4
            / (f64::from(efficiencies) * std::f64::consts::PI)) as f32;
        let receiver_type = if parameters.receiver_type >= 0 {
            parameters.receiver_type as i32
        } else {
            0
        };
        Self {
            tau: float(parameters.tau),
            atmosphere_k: if atmosphere_k > UNSET_ATMOSPHERE_K {
                atmosphere_k
            } else {
                DEFAULT_ATMOSPHERE_K
            },
            ground_k: float(parameters.ground_k),
            cmb_k: float(parameters.cmb_k),
            receiver_k: float(parameters.receiver_k),
            spillover: float(parameters.spillover_efficiency),
            jansky_per_kelvin: sensitivity * amplitude,
            sidebands: 1.0 + f64::from(receiver_type),
        }
    }

    /// The zenith opacity τ₀ the model holds.
    pub(crate) fn tau(&self) -> f32 {
        self.tau
    }

    /// The band at frequency `frequency_hz`.
    fn band(&self, frequency_hz: f64) -> Band {
        let h_nu_over_k = PLANCK_OVER_BOLTZMANN_RADIANCE * frequency_hz;
        let radiance =
            |temperature_k: f32| 1.0 / ((h_nu_over_k / f64::from(temperature_k)).exp() - 1.0);
        Band {
            h_nu_over_k: PLANCK_OVER_BOLTZMANN_TSYS * frequency_hz,
            cmb: radiance(self.cmb_k),
            ground: radiance(self.ground_k),
            atmosphere: radiance(self.atmosphere_k),
        }
    }

    /// Tsys at airmass `airmass` in `band`, K, in single precision
    /// (`AtmosCorruptor::tsys`).
    fn tsys(&self, band: &Band, airmass: f32) -> f32 {
        let tau = f64::from(self.tau * airmass);
        let spillover = f64::from(self.spillover);
        let radiance = spillover * band.cmb
            + tau.exp()
                * (spillover * (1.0 - (-tau).exp()) * band.atmosphere
                    + (1.0 - spillover) * band.ground
                    + f64::from(self.receiver_k) / band.h_nu_over_k);
        (band.h_nu_over_k * radiance * self.sidebands) as f32
    }

    /// The zenith Tsys `setnoise` checks and logs, K: at airmass 1, at the
    /// centre of a window of `channels` channels from `reference_hz` over
    /// `total_bandwidth_hz` (channel ⌊n/2⌋ as `setFocusChan` places it,
    /// from the single-precision reference and width CASA holds).
    pub(crate) fn zenith_tsys(
        &self,
        reference_hz: f64,
        total_bandwidth_hz: f64,
        channels: usize,
    ) -> f32 {
        let resolution = f64::from(total_bandwidth_hz as f32) / channels.max(1) as f64;
        let centre = f64::from(reference_hz as f32) + (channels / 2) as f64 * resolution;
        self.tsys(&self.band(centre), 1.0)
    }

    /// The model for the rows of `window`.
    pub(crate) fn for_window(&self, window: &WindowNoise) -> WindowTsys {
        WindowTsys {
            model: *self,
            band: self.band(f64::from(window.reference_frequency_hz as f32)),
            channel_bandwidth_hz: window.channel_bandwidth_hz,
        }
    }
}

/// The `tsys-manual` model for the rows of one spectral window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WindowTsys {
    model: TsysManual,
    band: Band,
    channel_bandwidth_hz: f64,
}

impl WindowTsys {
    /// σ of each real and imaginary draw of `row`, Jy: s_i s_j.
    pub(crate) fn sigma(&self, row: &RowNoise) -> f32 {
        let factor = (f64::from(self.model.jansky_per_kelvin)
            / (self.channel_bandwidth_hz * row.exposure_s).sqrt())
        .sqrt();
        let scale = |diameter_m: f64, airmass: f32| {
            let tsys = self.model.tsys(&self.band, airmass);
            let jones = (diameter_m / factor / f64::from(tsys.sqrt())) as f32;
            // A zero or NaN `T` is bad, and a bad `T` applies as 1.
            if jones.is_nan() || jones == 0.0 {
                1.0
            } else {
                1.0 / jones
            }
        };
        scale(row.diameters_m[0], row.airmass[0]) * scale(row.diameters_m[1], row.airmass[1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(diameters_m: [f64; 2], airmass: [f32; 2], exposure_s: f64) -> RowNoise {
        RowNoise {
            diameters_m,
            airmass,
            exposure_s,
        }
    }

    const BAND6: WindowNoise = WindowNoise {
        reference_frequency_hz: 230e9,
        channel_bandwidth_hz: 15.625e6,
    };

    /// CASA's formula in double precision throughout, from the module's
    /// equations.
    fn reference_sigma(parameters: &TsysParameters, window: &WindowNoise, row: &RowNoise) -> f64 {
        let nu = window.reference_frequency_hz;
        let h_nu_k = 4.80145e-11 * nu;
        let radiance = |t: f64| 1.0 / ((0.04799274551e-9 * nu / t).exp() - 1.0);
        let spill = parameters.spillover_efficiency;
        let tsys = |a: f64| {
            let tau = parameters.tau * a;
            let r = spill * radiance(parameters.cmb_k)
                + tau.exp()
                    * (spill * (1.0 - (-tau).exp()) * radiance(parameters.atmosphere_k)
                        + (1.0 - spill) * radiance(parameters.ground_k)
                        + parameters.receiver_k / h_nu_k);
            h_nu_k * r * (1.0 + parameters.receiver_type.max(0) as f64)
        };
        let c = if parameters.sensitivity > 0.0 {
            parameters.sensitivity
        } else {
            std::f64::consts::FRAC_1_SQRT_2
        };
        let a = 8.0 * 1.38062e-16 * 1e23 * 1e-4
            / (parameters.antenna_efficiency
                * parameters.correlator_efficiency
                * std::f64::consts::PI);
        c * a / (window.channel_bandwidth_hz * row.exposure_s).sqrt()
            * (tsys(f64::from(row.airmass[0])) * tsys(f64::from(row.airmass[1]))).sqrt()
            / (row.diameters_m[0] * row.diameters_m[1])
    }

    #[test]
    fn sigma_is_casas_formula_to_single_precision() {
        let defaults = TsysParameters::default();
        let other = TsysParameters {
            tau: 0.8,
            atmosphere_k: 260.0,
            antenna_efficiency: 0.7,
            spillover_efficiency: 0.9,
            correlator_efficiency: 0.95,
            receiver_k: 80.0,
            ground_k: 280.0,
            cmb_k: 2.725,
            sensitivity: 0.5,
            receiver_type: 1,
        };
        for parameters in [defaults, other] {
            let model = TsysManual::new(&parameters).for_window(&BAND6);
            for row in [
                row([12.0, 12.0], [1.0, 1.0], 10.0),
                row([12.0, 7.0], [1.3, 2.1], 2.0),
                row([7.0, 7.0], [5.7, 5.7], 30.0),
            ] {
                let sigma = f64::from(model.sigma(&row));
                let expected = reference_sigma(&parameters, &BAND6, &row);
                assert!(
                    (sigma / expected - 1.0).abs() < 2e-6,
                    "{parameters:?} {row:?}: {sigma} against {expected}"
                );
            }
        }
    }

    #[test]
    fn sigma_scales_as_the_formula_says() {
        let model = TsysManual::new(&TsysParameters::default());
        let sigma =
            |window: &WindowNoise, row: &RowNoise| f64::from(model.for_window(window).sigma(row));
        let base = row([12.0, 12.0], [1.2, 1.2], 10.0);
        let at = sigma(&BAND6, &base);
        // σ ∝ 1/(D_i D_j), 1/√t and 1/√Δν.
        let smaller = sigma(&BAND6, &row([12.0, 6.0], [1.2, 1.2], 10.0));
        assert!((smaller / at - 2.0).abs() < 1e-6);
        let longer = sigma(&BAND6, &row([12.0, 12.0], [1.2, 1.2], 40.0));
        assert!((longer / at - 0.5).abs() < 1e-6);
        let wider = WindowNoise {
            channel_bandwidth_hz: 4.0 * BAND6.channel_bandwidth_hz,
            ..BAND6
        };
        assert!((sigma(&wider, &base) / at - 0.5).abs() < 1e-6);
        // More airmass, more noise; one antenna's airmass enters as √Tsys.
        let lower = sigma(&BAND6, &row([12.0, 12.0], [2.0, 2.0], 10.0));
        let half = sigma(&BAND6, &row([12.0, 12.0], [1.2, 2.0], 10.0));
        assert!(lower > at);
        assert!((half * half / (at * lower) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn airmass_is_one_over_sine_and_1000_below_the_horizon() {
        assert_eq!(airmass(std::f64::consts::FRAC_PI_2), 1.0);
        assert!((airmass(30f64.to_radians()) - 2.0).abs() < 1e-6);
        assert_eq!(airmass(0.0), 1000.0);
        assert_eq!(airmass(-0.1), 1000.0);
    }

    /// Below the horizon Tsys overflows single precision for τ₀ above about
    /// 0.09, and CASA applies the bad `T` as 1: the antenna does not scale.
    #[test]
    fn an_overflowing_tsys_applies_as_one() {
        let model = TsysManual::new(&TsysParameters {
            tau: 0.2,
            ..TsysParameters::default()
        })
        .for_window(&BAND6);
        let good = row([12.0, 12.0], [1.5, 1.5], 10.0);
        let one_below = row([12.0, 12.0], [1.5, airmass(-0.2)], 10.0);
        let both_below = row([12.0, 12.0], [airmass(-0.2), airmass(-0.2)], 10.0);
        let s = model.sigma(&good).sqrt();
        assert_eq!(model.sigma(&both_below), 1.0);
        assert!((model.sigma(&one_below) / s - 1.0).abs() < 1e-6);
    }

    #[test]
    fn unset_parameters_take_casas_values() {
        let unset = TsysManual::new(&TsysParameters {
            atmosphere_k: 5.0,
            sensitivity: -1.0,
            receiver_type: -3,
            ..TsysParameters::default()
        });
        let set = TsysManual::new(&TsysParameters {
            atmosphere_k: 250.0,
            sensitivity: std::f64::consts::FRAC_1_SQRT_2,
            receiver_type: 0,
            ..TsysParameters::default()
        });
        assert_eq!(unset, set);
        // A double-sideband receiver doubles Tsys, so σ.
        let double = TsysManual::new(&TsysParameters {
            receiver_type: 1,
            ..TsysParameters::default()
        });
        let r = row([12.0, 12.0], [1.1, 1.1], 10.0);
        let ratio = double.for_window(&BAND6).sigma(&r) / set.for_window(&BAND6).sigma(&r);
        assert!((ratio - 2.0).abs() < 1e-6);
    }

    /// CASA logs 193.986 K for the zenith Tsys of τ₀ = 0.3 and the defaults
    /// at the centre of a 64-channel window from 230 GHz over 1 GHz (the
    /// Phase 3 probe); at 230 GHz itself it is 193.993 K.
    #[test]
    fn the_zenith_tsys_is_casas() {
        let model = TsysManual::new(&TsysParameters {
            tau: 0.3,
            ..TsysParameters::default()
        });
        let tsys = model.zenith_tsys(230e9, 1e9, 64);
        assert!((tsys - 193.986).abs() < 0.001, "{tsys}");
    }
}
