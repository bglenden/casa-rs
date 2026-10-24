// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMProfile.cc, (c) Instituto de Estructura de la Materia,
// Pardo et al., LGPL-2.1-or-later.
//! The layered atmosphere above a site, built from ground weather.
//!
//! ATM describes the atmosphere as a stack of horizontal layers. Each has a
//! thickness and mean pressure, temperature, water-vapour density and number
//! densities of the minor gases. [`AtmosphereProfile::new`] builds the stack
//! the way ATM's `AtmProfile` does from the ground pressure, temperature and
//! relative humidity at the site:
//!
//! - In the troposphere the pressure falls in steps that grow by a constant
//!   factor (ATM fixes it at 1.075), the height of each step follows from the
//!   hypsometric equation with height-dependent gravity, the temperature
//!   follows the tropospheric lapse rate, and the water-vapour density falls
//!   exponentially with the water-vapour scale height.
//! - Above the tropopause the levels come from the reference atmosphere
//!   chosen by [`AtmosphereType`].
//! - The minor gases (O3, N2O, CO, NO2, SO2, HCl, HCN) are interpolated from
//!   the US Standard 1976 family of reference profiles at each layer's mid
//!   height, with ATM's empirical corrections for Chajnantor.
//!
//! Each layer's mean temperature is the mean of its boundary temperatures;
//! its mean pressure and water-vapour density are geometric means. The
//! column of precipitable water is the sum of density times thickness
//! ([`AtmosphereProfile::ground_water_column_m`]).

mod minor;
mod tables;

use crate::AtmosphereError;
use minor::{MinorGases, minor_gases};
use tables::{MKATMPROFILE_HX as HX, MKATMPROFILE_PX as PX, MKATMPROFILE_TX as TX};

/// ATM's reference atmospheres, which set the profile above the tropopause
/// and the minor-gas abundances.
///
/// The discriminants are ATM's `typeAtm` numbers. CASA's `at` tool accepts
/// 1 to 5. The Chajnantor variants (11 to 14) are the tropical atmosphere
/// with ATM's seasonal and diurnal ozone corrections for the ALMA site.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AtmosphereType {
    /// Tropical (ATM's default for ALMA).
    Tropical = 1,
    /// Mid-latitude summer.
    MidlatitudeSummer = 2,
    /// Mid-latitude winter.
    MidlatitudeWinter = 3,
    /// Subarctic summer.
    SubarcticSummer = 4,
    /// Subarctic winter.
    SubarcticWinter = 5,
    /// US Standard Atmosphere 1976.
    UsStandard1976 = 6,
    /// Tropical, with the Chajnantor winter daytime ozone profile.
    ChajnantorWinterDay = 11,
    /// Tropical, with the Chajnantor winter night-time ozone profile.
    ChajnantorWinterNight = 12,
    /// Tropical, with the Chajnantor summer daytime ozone profile.
    ChajnantorSummerDay = 13,
    /// Tropical, with the Chajnantor summer night-time ozone profile.
    ChajnantorSummerNight = 14,
}

impl AtmosphereType {
    /// The type for ATM's `typeAtm` number, if ATM defines one.
    #[must_use]
    pub fn from_number(number: u32) -> Option<Self> {
        Some(match number {
            1 => Self::Tropical,
            2 => Self::MidlatitudeSummer,
            3 => Self::MidlatitudeWinter,
            4 => Self::SubarcticSummer,
            5 => Self::SubarcticWinter,
            6 => Self::UsStandard1976,
            11 => Self::ChajnantorWinterDay,
            12 => Self::ChajnantorWinterNight,
            13 => Self::ChajnantorSummerDay,
            14 => Self::ChajnantorSummerNight,
            _ => return None,
        })
    }

    /// The reference profile (1 to 6), the seasonal/diurnal ozone model
    /// (1 to 4), and the pressure in hPa below which the tropospheric steps
    /// give way to the reference levels.
    fn model(self) -> (usize, u32, f64) {
        match self {
            Self::Tropical => (1, 1, 125.0),
            Self::ChajnantorWinterDay => (1, 1, 125.0),
            Self::ChajnantorWinterNight => (1, 2, 125.0),
            Self::ChajnantorSummerDay => (1, 3, 125.0),
            Self::ChajnantorSummerNight => (1, 4, 125.0),
            Self::MidlatitudeSummer => (2, 1, 100.0),
            Self::MidlatitudeWinter => (3, 1, 100.0),
            Self::SubarcticSummer => (4, 1, 100.0),
            Self::SubarcticWinter => (5, 1, 100.0),
            Self::UsStandard1976 => (6, 1, 100.0),
        }
    }
}

/// The ground weather and model choices an [`AtmosphereProfile`] is built
/// from, in the units ATM's `AtmProfile` constructor takes.
///
/// [`Default`] gives CASA's `at.initAtmProfile` defaults, which are also the
/// simulator's: the ALMA site at 5,000 m, 560 hPa, 270 K, 20% humidity.
#[derive(Clone, Debug, PartialEq)]
pub struct ProfileParameters {
    /// Site altitude above sea level, metres.
    pub altitude_m: f64,
    /// Ground pressure, hPa (mbar).
    pub ground_pressure_hpa: f64,
    /// Ground temperature, kelvin.
    pub ground_temperature_k: f64,
    /// Tropospheric temperature lapse rate, K/km (negative when the
    /// temperature falls with height).
    pub lapse_rate_k_per_km: f64,
    /// Ground relative humidity, percent.
    pub relative_humidity_percent: f64,
    /// Water-vapour scale height, kilometres.
    pub water_vapour_scale_height_km: f64,
    /// Primary pressure step, hPa. ATM replaces a step outside 5 to 12.5 hPa
    /// with 10 hPa.
    pub pressure_step_hpa: f64,
    /// Top of the profile above sea level, kilometres.
    pub top_altitude_km: f64,
    /// The reference atmosphere.
    pub atmosphere: AtmosphereType,
}

impl Default for ProfileParameters {
    fn default() -> Self {
        Self {
            altitude_m: 5000.0,
            ground_pressure_hpa: 560.0,
            ground_temperature_k: 270.0,
            lapse_rate_k_per_km: -5.6,
            relative_humidity_percent: 20.0,
            water_vapour_scale_height_km: 2.0,
            pressure_step_hpa: 10.0,
            top_altitude_km: 48.0,
            atmosphere: AtmosphereType::Tropical,
        }
    }
}

/// One horizontal layer of an [`AtmosphereProfile`].
///
/// Pressures are in hPa, temperatures in kelvin, water-vapour densities in
/// kg/m³ and the minor gases in molecules/m³. "Bottom" and "top" are the
/// layer's boundary values; the unqualified values are the layer means.
#[derive(Clone, Debug, PartialEq)]
pub struct Layer {
    /// Thickness, metres.
    pub thickness_m: f64,
    /// Mean temperature.
    pub temperature_k: f64,
    /// Temperature at the bottom boundary.
    pub bottom_temperature_k: f64,
    /// Temperature at the top boundary.
    pub top_temperature_k: f64,
    /// Mean pressure (geometric mean of the boundaries).
    pub pressure_hpa: f64,
    /// Pressure at the bottom boundary.
    pub bottom_pressure_hpa: f64,
    /// Pressure at the top boundary.
    pub top_pressure_hpa: f64,
    /// Mean water-vapour mass density.
    pub water_vapour_kg_m3: f64,
    /// Water-vapour mass density at the bottom boundary.
    pub bottom_water_vapour_kg_m3: f64,
    /// Water-vapour mass density at the top boundary.
    pub top_water_vapour_kg_m3: f64,
    /// Ozone number density.
    pub o3_per_m3: f64,
    /// Carbon monoxide number density.
    pub co_per_m3: f64,
    /// Nitrous oxide number density.
    pub n2o_per_m3: f64,
    /// Nitrogen dioxide number density.
    pub no2_per_m3: f64,
    /// Sulphur dioxide number density.
    pub so2_per_m3: f64,
    /// Hydrogen chloride number density.
    pub hcl_per_m3: f64,
    /// Hydrogen cyanide number density.
    pub hcn_per_m3: f64,
}

/// The layered atmosphere above a site (ATM's `AtmProfile`).
#[derive(Clone, Debug, PartialEq)]
pub struct AtmosphereProfile {
    parameters: ProfileParameters,
    layers: Vec<Layer>,
    tropopause_layer: usize,
    tropopause_temperature_k: f64,
    tropopause_altitude_m: f64,
}

impl AtmosphereProfile {
    /// Builds the profile for `parameters` as ATM's `AtmProfile` constructor
    /// does.
    ///
    /// # Errors
    ///
    /// [`AtmosphereError::InvalidParameter`] if a parameter is not finite,
    /// the ground pressure, temperature or scale height is not positive, or
    /// the top of the profile is not above the site.
    pub fn new(parameters: &ProfileParameters) -> Result<Self, AtmosphereError> {
        let mut parameters = parameters.clone();
        validate(&parameters)?;
        if !(5.0..=12.5).contains(&parameters.pressure_step_hpa) {
            parameters.pressure_step_hpa = 10.0;
        }
        let levels = Levels::build(&parameters);
        Ok(levels.into_profile(parameters))
    }

    /// As [`AtmosphereProfile::new`], then replaces the mean temperature of
    /// each layer that contains user-given levels with their mean, as ATM's
    /// `AtmProfile` constructor with a user temperature profile does.
    ///
    /// `levels` are (altitude above sea level in metres, temperature in
    /// kelvin). A level on a boundary between two layers counts in both.
    ///
    /// # Errors
    ///
    /// As [`AtmosphereProfile::new`].
    pub fn with_temperature_levels(
        parameters: &ProfileParameters,
        levels: &[(f64, f64)],
    ) -> Result<Self, AtmosphereError> {
        let mut profile = Self::new(parameters)?;
        let mut bottom = profile.parameters.altitude_m;
        for layer in &mut profile.layers {
            let top = bottom + layer.thickness_m;
            let (sum, count) = levels
                .iter()
                .filter(|(altitude, _)| bottom <= *altitude && top >= *altitude)
                .fold((0.0, 0.0), |(sum, count), (_, temperature)| {
                    (sum + temperature, count + 1.0)
                });
            if sum > 0.0 {
                layer.temperature_k = sum / count;
            }
            bottom = top;
        }
        Ok(profile)
    }

    /// The parameters the profile was built from, after ATM's corrections
    /// (an out-of-range pressure step becomes 10 hPa).
    #[must_use]
    pub fn parameters(&self) -> &ProfileParameters {
        &self.parameters
    }

    /// The layers, from the ground up.
    #[must_use]
    pub fn layers(&self) -> &[Layer] {
        &self.layers
    }

    /// The index of the last tropospheric layer.
    #[must_use]
    pub fn tropopause_layer(&self) -> usize {
        self.tropopause_layer
    }

    /// The temperature at the tropopause, kelvin.
    #[must_use]
    pub fn tropopause_temperature_k(&self) -> f64 {
        self.tropopause_temperature_k
    }

    /// The altitude of the tropopause above sea level, metres.
    #[must_use]
    pub fn tropopause_altitude_m(&self) -> f64 {
        self.tropopause_altitude_m
    }

    /// The zenith column of precipitable water above the site, metres
    /// (ATM's `getGroundWH2O`).
    #[must_use]
    pub fn ground_water_column_m(&self) -> f64 {
        let column_mm = self.layers.iter().fold(0.0, |sum, layer| {
            sum + layer.water_vapour_kg_m3 * layer.thickness_m
        });
        column_mm * 1e-3
    }
}

fn validate(parameters: &ProfileParameters) -> Result<(), AtmosphereError> {
    let values = [
        ("altitude_m", parameters.altitude_m),
        ("ground_pressure_hpa", parameters.ground_pressure_hpa),
        ("ground_temperature_k", parameters.ground_temperature_k),
        ("lapse_rate_k_per_km", parameters.lapse_rate_k_per_km),
        (
            "relative_humidity_percent",
            parameters.relative_humidity_percent,
        ),
        (
            "water_vapour_scale_height_km",
            parameters.water_vapour_scale_height_km,
        ),
        ("pressure_step_hpa", parameters.pressure_step_hpa),
        ("top_altitude_km", parameters.top_altitude_km),
    ];
    if let Some((name, _)) = values.iter().find(|(_, value)| !value.is_finite()) {
        return Err(AtmosphereError::InvalidParameter(format!(
            "{name} is not finite"
        )));
    }
    for (name, value) in [
        ("ground_pressure_hpa", parameters.ground_pressure_hpa),
        ("ground_temperature_k", parameters.ground_temperature_k),
        (
            "water_vapour_scale_height_km",
            parameters.water_vapour_scale_height_km,
        ),
    ] {
        if value <= 0.0 {
            return Err(AtmosphereError::InvalidParameter(format!(
                "{name} must be positive, not {value}"
            )));
        }
    }
    if parameters.top_altitude_km * 1000.0 <= parameters.altitude_m {
        return Err(AtmosphereError::InvalidParameter(format!(
            "top_altitude_km {} is not above the site at {} m",
            parameters.top_altitude_km, parameters.altitude_m
        )));
    }
    Ok(())
}

/// Absolute humidity in g/m³ for a temperature (K), relative humidity (%)
/// and pressure (hPa): ATM's `rwat`.
fn absolute_humidity_g_m3(temperature_k: f64, humidity_percent: f64, pressure_hpa: f64) -> f64 {
    if pressure_hpa <= 0.0 || temperature_k <= 0.0 || humidity_percent <= 0.0 {
        return 0.0;
    }
    let t = temperature_k;
    let saturation = 6.105 * (25.22 / t * (t - 273.0) - 5.31 * (t / 273.0).ln()).exp();
    let factor = 1.0 - (1.0 - humidity_percent / 100.0) * saturation / pressure_hpa;
    let partial = saturation * humidity_percent / 100.0 / factor;
    partial * 216.502 / t
}

/// The level boundaries of `mkAtmProfile` before they become layers:
/// pressure (hPa), temperature (K), height above sea level (m) and water
/// vapour (g/m³) at each level.
struct Levels {
    pressure: Vec<f64>,
    temperature: Vec<f64>,
    height: Vec<f64>,
    water: Vec<f64>,
    tropopause: usize,
    reference: usize,
    ozone_model: u32,
}

const EARTH_RADIUS_KM: f64 = 6371.2;
const SURFACE_GRAVITY: f64 = 9.80665;
// ATM fixes the factor between consecutive pressure steps (05JUN2024).
const PRESSURE_STEP_FACTOR: f64 = 1.075;

impl Levels {
    /// The level loop of ATM's `mkAtmProfile`.
    fn build(parameters: &ProfileParameters) -> Self {
        let (reference, ozone_model, mut pressure_limit) = parameters.atmosphere.model();
        let (px, tx) = (&PX[reference - 1], &TX[reference - 1]);
        let altitude_km = parameters.altitude_m / 1000.0;
        let scale_height_km = parameters.water_vapour_scale_height_km;
        let top_m = parameters.top_altitude_km * 1000.0;
        let step = parameters.pressure_step_hpa;

        let ground_water = absolute_humidity_g_m3(
            parameters.ground_temperature_k,
            parameters.relative_humidity_percent,
            parameters.ground_pressure_hpa,
        );
        let sea_level_water = ground_water * (altitude_km / scale_height_km).exp();
        let water_at =
            |height_m: f64| sea_level_water * (-height_m / (1000.0 * scale_height_km)).exp();

        let mut levels = Self {
            pressure: vec![parameters.ground_pressure_hpa],
            temperature: vec![parameters.ground_temperature_k],
            height: vec![altitude_km * 1000.0],
            water: vec![ground_water],
            tropopause: 0,
            reference,
            ozone_model,
        };
        let mut gravity = SURFACE_GRAVITY;
        let mut troposphere = true;
        let mut reference_level = 0usize;
        let mut i = 0usize;
        loop {
            i += 1;
            let next = levels.pressure[i - 1] - step * PRESSURE_STEP_FACTOR.powf((i - 1) as f64);
            if next <= pressure_limit {
                if troposphere {
                    // The reference level nearest to, and well below, the
                    // next tropospheric pressure.
                    let mut nearest = 20000.0;
                    for (k, &level) in px.iter().enumerate() {
                        if next.abs() > 1.05 * level && (next - level).abs() <= nearest {
                            reference_level = k;
                            nearest = (next - level).abs();
                        }
                    }
                    let k = reference_level.saturating_sub(1);
                    levels.pressure.push(px[k]);
                    levels.temperature.push(tx[k]);
                    let dh = levels.step_height(i, gravity);
                    levels.height.push(levels.height[i - 1] + dh);
                } else {
                    reference_level += 1;
                    levels.pressure.push(px[reference_level - 1]);
                    levels.temperature.push(tx[reference_level - 1]);
                    if reference_level < HX.len() {
                        let dh = (HX[reference_level] - HX[reference_level - 1]) * 1000.0;
                        levels.height.push(dh + levels.height[i - 1]);
                    }
                }
                // ATM reads past the heights when the reference levels run
                // out; that level's water is never used.
                levels
                    .water
                    .push(levels.height.get(i).map_or(f64::NAN, |&h| water_at(h)));
                if troposphere {
                    levels.tropopause = i - 1;
                    troposphere = false;
                }
                if reference_level >= HX.len() || levels.height[i] > top_m {
                    break;
                }
            } else {
                levels.pressure.push(next);
                gravity = SURFACE_GRAVITY
                    * (1.0 + (levels.height[i - 1] / 1000.0) / EARTH_RADIUS_KM).powf(-2.0);
                let dh = levels.step_height(i, gravity);
                levels.height.push(levels.height[i - 1] + dh);
                levels
                    .temperature
                    .push(levels.temperature[i - 1] + parameters.lapse_rate_k_per_km * dh / 1000.0);
                levels.water.push(water_at(levels.height[i]));
                if levels.temperature[i] <= tx[4] && levels.pressure[i] <= px[4] {
                    pressure_limit = levels.pressure[i];
                }
                if levels.height[i] > top_m {
                    break;
                }
            }
        }
        levels.pressure.truncate(i);
        levels.temperature.truncate(i);
        levels.height.truncate(i);
        levels.water.truncate(i);
        levels
    }

    /// The hypsometric height of the step from level `i - 1` to level `i`.
    fn step_height(&self, i: usize, gravity: f64) -> f64 {
        let water_kg_m3 = self.water[i - 1] / 1000.0;
        288.6948
            * self.temperature[i - 1]
            * (1.0 + 0.61 * water_kg_m3 / 1000.0)
            * (self.pressure[i - 1] / self.pressure[i]).ln()
            / gravity
    }

    /// The layers between consecutive levels, as the end of `mkAtmProfile`
    /// makes them.
    fn into_profile(self, parameters: ProfileParameters) -> AtmosphereProfile {
        let count = self.pressure.len() - 1;
        let mut layers: Vec<Layer> = Vec::with_capacity(count);
        let mut mid_height_km = parameters.altitude_m / 1000.0;
        for j in 0..count {
            let thickness_m = self.height[j + 1] - self.height[j];
            mid_height_km += thickness_m / 2.0e3;
            let water_vapour_kg_m3 = if mid_height_km < 40.0 || j == 0 {
                1.0e-3 * ((self.water[j + 1].ln() + self.water[j].ln()) / 2.0).exp()
            } else {
                // ATM keeps the mesospheric water constant (12FEB2024).
                layers[j - 1].water_vapour_kg_m3
            };
            let MinorGases {
                o3,
                n2o,
                co,
                no2,
                so2,
                hcl,
                hcn,
            } = minor_gases(mid_height_km, self.reference, self.ozone_model);
            layers.push(Layer {
                thickness_m,
                temperature_k: (self.temperature[j + 1] + self.temperature[j]) / 2.0,
                bottom_temperature_k: self.temperature[j],
                top_temperature_k: self.temperature[j + 1],
                pressure_hpa: ((self.pressure[j + 1].ln() + self.pressure[j].ln()) / 2.0).exp(),
                bottom_pressure_hpa: self.pressure[j],
                top_pressure_hpa: self.pressure[j + 1],
                water_vapour_kg_m3,
                bottom_water_vapour_kg_m3: 1.0e-3 * self.water[j],
                top_water_vapour_kg_m3: 1.0e-3 * self.water[j + 1],
                o3_per_m3: 1.0e6 * o3,
                co_per_m3: 1.0e6 * co,
                n2o_per_m3: 1.0e6 * n2o,
                no2_per_m3: 1.0e6 * no2,
                so2_per_m3: 1.0e6 * so2,
                hcl_per_m3: 1.0e6 * hcl,
                hcn_per_m3: 1.0e6 * hcn,
            });
            mid_height_km += thickness_m / 2.0e3;
        }
        AtmosphereProfile {
            tropopause_temperature_k: self.temperature[self.tropopause],
            tropopause_altitude_m: self.height[self.tropopause],
            tropopause_layer: self.tropopause,
            parameters,
            layers,
        }
    }
}
