// SPDX-License-Identifier: LGPL-3.0-or-later
//! Atmosphere profile construction and CASA's profile return records.

use std::fmt::Write;

use casa_atmosphere::{AtmosphereProfile, AtmosphereType, Layer, ProfileParameters};
use casa_contracts::CasaValue;
use pyo3::prelude::*;

use super::quantity::{self, AtmosphereQuantity, QuantityArg};
use super::{PyAtmosphere, TYPE_NAMES, error, tuple, warn};
use crate::convert::reply_value::reply_value;

const NEED_PROFILE: &str = "Please initialize atmospheric profile with initAtmProfile.";
const PROFILE_STEP_FACTOR: f64 = 1.075;

struct Weather {
    altitude: AtmosphereQuantity,
    temperature: AtmosphereQuantity,
    pressure: AtmosphereQuantity,
    humidity: f64,
    lapse_rate: AtmosphereQuantity,
    scale_height: AtmosphereQuantity,
}

impl Weather {
    fn read(
        altitude: QuantityArg,
        temperature: QuantityArg,
        pressure: QuantityArg,
        humidity: f64,
        lapse_rate: QuantityArg,
        scale_height: QuantityArg,
    ) -> PyResult<Self> {
        let humidity_fraction = humidity / 100.0;
        Ok(Self {
            altitude: altitude.read("altitude", (5000., "m"))?,
            temperature: temperature.read("temperature", (270., "K"))?,
            pressure: pressure.read("pressure", (560., "mbar"))?,
            humidity: if humidity_fraction <= 0.0 {
                0.1
            } else {
                humidity_fraction * 100.0
            },
            lapse_rate: lapse_rate.read("dTem_dh", (-5.6, "K/km"))?,
            scale_height: scale_height.read("h0", (2., "km"))?,
        })
    }

    fn parameters(&self) -> PyResult<ProfileParameters> {
        Ok(ProfileParameters {
            altitude_m: self.altitude.value_in("m")?,
            ground_temperature_k: self.temperature.value_in("K")?,
            ground_pressure_hpa: self.pressure.value_in("mbar")?,
            relative_humidity_percent: self.humidity,
            lapse_rate_k_per_km: self.lapse_rate.value_in("K/km")?,
            water_vapour_scale_height_km: self.scale_height.value_in("km")?,
            ..ProfileParameters::default()
        })
    }
}

#[pymethods]
#[allow(non_snake_case)]
impl PyAtmosphere {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(name = "initAtmProfile", signature = (altitude=QuantityArg::Omitted, temperature=QuantityArg::Omitted, pressure=QuantityArg::Omitted, maxAltitude=QuantityArg::Omitted, humidity=20., dTem_dh=QuantityArg::Omitted, dP=QuantityArg::Omitted, dPm=1.2, h0=QuantityArg::Omitted, atmType=1, layerBoundaries=Vec::new(), layerTemperature=Vec::new()), text_signature = "($self, altitude={'value': 5000.0, 'unit': 'm'}, temperature={'value': 270.0, 'unit': 'K'}, pressure={'value': 560.0, 'unit': 'mbar'}, maxAltitude={'value': 48.0, 'unit': 'km'}, humidity=20.0, dTem_dh={'value': -5.6, 'unit': 'K/km'}, dP={'value': 10.0, 'unit': 'mbar'}, dPm=1.2, h0={'value': 2.0, 'unit': 'km'}, atmType=1, layerBoundaries=[], layerTemperature=[])")]
    fn init_atm_profile(
        &self,
        py: Python<'_>,
        altitude: QuantityArg,
        temperature: QuantityArg,
        pressure: QuantityArg,
        maxAltitude: QuantityArg,
        humidity: f64,
        dTem_dh: QuantityArg,
        dP: QuantityArg,
        dPm: f64,
        h0: QuantityArg,
        atmType: i64,
        layerBoundaries: Vec<f64>,
        layerTemperature: Vec<f64>,
    ) -> PyResult<Py<PyAny>> {
        let weather = Weather::read(altitude, temperature, pressure, humidity, dTem_dh, h0)?;
        let top = maxAltitude.read("maxAltitude", (48., "km"))?;
        let step = dP.read("dP", (10., "mbar"))?;
        let atmosphere = u32::try_from(atmType)
            .ok()
            .filter(|kind| (1..=5).contains(kind))
            .and_then(AtmosphereType::from_number)
            .ok_or_else(|| error("Exception: atmType not in permissive range.."))?;
        let parameters = ProfileParameters {
            top_altitude_km: top.value_in("km")?,
            pressure_step_hpa: step.value_in("mbar")?,
            atmosphere,
            ..weather.parameters()?
        };
        if layerBoundaries.len() != layerTemperature.len() {
            return Err(error(
                "Exception: Size of user-defined layer boundaries and temperature does not match..",
            ));
        }
        let levels = layerBoundaries
            .into_iter()
            .zip(layerTemperature)
            .collect::<Vec<_>>();
        let report = self
            .run(py, |session| {
                let profile = AtmosphereProfile::with_temperature_levels(&parameters, &levels)?;
                let report = initialized_report(
                    &parameters,
                    dPm,
                    !levels.is_empty(),
                    profile.layers().len(),
                );
                session.set_profile(profile);
                Ok::<_, casa_atmosphere::AtmosphereError>(report)
            })
            .map_err(error)?;
        tuple(
            py,
            [
                CasaValue::string(report),
                weather.altitude.into_record(),
                weather.temperature.into_record(),
                weather.pressure.into_record(),
                top.into_record(),
                weather.lapse_rate.into_record(),
                step.into_record(),
                weather.scale_height.into_record(),
            ],
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(name = "updateAtmProfile", signature = (altitude=QuantityArg::Omitted, temperature=QuantityArg::Omitted, pressure=QuantityArg::Omitted, humidity=20., dTem_dh=QuantityArg::Omitted, h0=QuantityArg::Omitted), text_signature = "($self, altitude={'value': 5000.0, 'unit': 'm'}, temperature={'value': 270.0, 'unit': 'K'}, pressure={'value': 560.0, 'unit': 'mbar'}, humidity=20.0, dTem_dh={'value': -5.6, 'unit': 'K/km'}, h0={'value': 2.0, 'unit': 'km'})")]
    fn update_atm_profile(
        &self,
        py: Python<'_>,
        altitude: QuantityArg,
        temperature: QuantityArg,
        pressure: QuantityArg,
        humidity: f64,
        dTem_dh: QuantityArg,
        h0: QuantityArg,
    ) -> PyResult<Py<PyAny>> {
        let weather = Weather::read(altitude, temperature, pressure, humidity, dTem_dh, h0)?;
        let parameters = weather.parameters()?;
        let status = self
            .run(py, |session| {
                let attached = session.profile().is_some();
                let changed = session.update_profile(&parameters)?;
                Ok::<_, casa_atmosphere::AtmosphereError>((attached, changed))
            })
            .map_err(error)?;
        match status {
            (false, _) => warn(py, NEED_PROFILE)?,
            (true, false) => warn(py, "Atmospheric profile update failed!")?,
            _ => (),
        }
        tuple(
            py,
            [
                CasaValue::string(updated_report(&parameters)),
                weather.altitude.into_record(),
                weather.temperature.into_record(),
                weather.pressure.into_record(),
                weather.lapse_rate.into_record(),
                weather.scale_height.into_record(),
            ],
        )
    }

    #[pyo3(name = "getBasicAtmParms")]
    fn get_basic_atm_parms(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reply = self.run(py, |session| session.profile().map(basic_parameters));
        if reply.is_none() {
            warn(py, NEED_PROFILE)?;
        }
        tuple(py, reply.unwrap_or_else(empty_parameters))
    }

    #[pyo3(name = "getNumLayers")]
    fn get_num_layers(&self, py: Python<'_>) -> PyResult<i64> {
        match self.run(py, |session| {
            session.profile().map(|p| p.layers().len() as i64)
        }) {
            Some(layers) => Ok(layers),
            None => {
                warn(py, NEED_PROFILE)?;
                Ok(-1)
            }
        }
    }

    #[pyo3(name = "getGroundWH2O")]
    fn get_ground_water(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reply = self.run(py, |session| {
            session
                .profile()
                .map(AtmosphereProfile::ground_water_column_m)
        });
        if reply.is_none() {
            warn(py, NEED_PROFILE)?;
        }
        reply_value(
            py,
            &reply.map_or_else(quantity::empty, |water| {
                quantity::scalar("mm", 1000. * water)
            }),
        )
    }

    #[pyo3(name = "getProfile")]
    fn get_profile(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reply = self.run(py, |session| session.profile().map(layer_parameters));
        if reply.is_none() {
            warn(py, NEED_PROFILE)?;
        }
        tuple(py, reply.unwrap_or_else(empty_layers))
    }
}

fn atmosphere_name(atmosphere: AtmosphereType) -> &'static str {
    match atmosphere {
        AtmosphereType::Tropical => TYPE_NAMES[0],
        AtmosphereType::MidlatitudeSummer => TYPE_NAMES[1],
        AtmosphereType::MidlatitudeWinter => TYPE_NAMES[2],
        AtmosphereType::SubarcticSummer => TYPE_NAMES[3],
        AtmosphereType::SubarcticWinter => TYPE_NAMES[4],
        _ => "",
    }
}

fn basic_parameters(profile: &AtmosphereProfile) -> Vec<CasaValue> {
    let p = profile.parameters();
    vec![
        CasaValue::string(current_report(p, profile.layers().len())),
        quantity::scalar("m", p.altitude_m),
        quantity::scalar("K", p.ground_temperature_k),
        quantity::scalar("mbar", p.ground_pressure_hpa),
        quantity::scalar("km", p.top_altitude_km),
        CasaValue::float(p.relative_humidity_percent),
        quantity::scalar("K/km", p.lapse_rate_k_per_km),
        quantity::scalar("mbar", p.pressure_step_hpa),
        CasaValue::float(PROFILE_STEP_FACTOR),
        quantity::scalar("km", p.water_vapour_scale_height_km),
        CasaValue::string(atmosphere_name(p.atmosphere)),
    ]
}

fn empty_parameters() -> Vec<CasaValue> {
    vec![
        CasaValue::string(""),
        quantity::scalar("m", 0.),
        quantity::scalar("K", 0.),
        quantity::scalar("mbar", 0.),
        quantity::scalar("km", 0.),
        CasaValue::float(0.),
        quantity::scalar("K/km", 0.),
        quantity::scalar("mbar", 0.),
        CasaValue::float(0.),
        quantity::scalar("km", 0.),
        CasaValue::string(""),
    ]
}

fn water_number(layer: &Layer) -> f64 {
    layer.water_vapour_kg_m3 * 6.023e23 * 1000.0 / 18.0
}

fn layer_parameters(profile: &AtmosphereProfile) -> Vec<CasaValue> {
    let layers = profile.layers();
    let mut summary = format!(
        "Number of layers returned: {}\nLayer parameters: \n",
        layers.len()
    );
    for layer in layers {
        writeln!(summary, " P: {} mb  T: {} K  Thickness: {} m  WaterVapor: {} kg m-3  WaterVapor: {} m-3  CO: {} m-3  O3: {} m-3  N2O: {} m-3",
                 number(layer.pressure_hpa), number(layer.temperature_k), number(layer.thickness_m),
                 number(layer.water_vapour_kg_m3), number(water_number(layer)), number(layer.co_per_m3),
                 number(layer.o3_per_m3), number(layer.n2o_per_m3)).expect("writing to String");
    }
    vec![
        CasaValue::string(summary),
        quantity::vector("m", layers.iter().map(|l| l.thickness_m)),
        quantity::vector("K", layers.iter().map(|l| l.temperature_k)),
        quantity::vector("kg m-3", layers.iter().map(|l| l.water_vapour_kg_m3)),
        quantity::vector("m-3", layers.iter().map(water_number)),
        quantity::vector("mb", layers.iter().map(|l| l.pressure_hpa)),
        quantity::vector("m-3", layers.iter().map(|l| l.o3_per_m3)),
        quantity::vector("m-3", layers.iter().map(|l| l.co_per_m3)),
        quantity::vector("m-3", layers.iter().map(|l| l.n2o_per_m3)),
    ]
}

fn empty_layers() -> Vec<CasaValue> {
    let units = ["m", "K", "kg.m-3", "m-3", "Pa", "m-3", "m-3", "m-3"];
    std::iter::once(CasaValue::string(""))
        .chain(units.map(|unit| quantity::scalar(unit, 0.)))
        .collect()
}

fn starting_lines(p: &ProfileParameters, pressure_unit: &str) -> String {
    format!(
        "Ground temperature T:         {} K\nGround pressure P:            {} {pressure_unit}\nRelative humidity rh:         {} %\nScale height h0:              {} km\n",
        number(p.ground_temperature_k),
        number(p.ground_pressure_hpa),
        number(p.relative_humidity_percent),
        number(p.water_vapour_scale_height_km)
    )
}

fn initialized_report(p: &ProfileParameters, factor: f64, custom: bool, layers: usize) -> String {
    format!(
        "BASIC ATMOSPHERIC PARAMETERS TO GENERATE REFERENCE ATMOSPHERIC PROFILE\n  \n{}Pressure step dp:             {} mb\nAltitude alti:                {} m\nAttitude top atm profile:     {} km\nPressure step factor:         {} \nTropospheric lapse rate:      {} K/km\nAtmospheric type:             {}\nUser-defined temperature profile: {}\n\nBuilt atmospheric profile with {layers} layers.\n\n",
        starting_lines(p, "mb"),
        number(p.pressure_step_hpa),
        number(p.altitude_m),
        number(p.top_altitude_km),
        number(factor),
        number(p.lapse_rate_k_per_km),
        atmosphere_name(p.atmosphere),
        if custom { "ON" } else { "OFF" }
    )
}

fn updated_report(p: &ProfileParameters) -> String {
    format!(
        "UPDATED BASIC ATMOSPHERIC PARAMETERS TO GENERATE REFERENCE ATMOSPHERIC PROFILE\n  \n{}Altitude alti:                {} m\nTropospheric lapse rate:      {} K/km\n\n",
        starting_lines(p, "mb"),
        number(p.altitude_m),
        number(p.lapse_rate_k_per_km)
    )
}

fn current_report(p: &ProfileParameters, layers: usize) -> String {
    format!(
        "CURRENT ATMOSPHERIC PARAMETERS OF REFERENCE ATMOSPHERIC PROFILE\n  \n{}Pressure step dp:             {} mbar\nAltitude alti:                {} m\nAttitude top atm profile      {} km\nPressure step factor          {} \nTropospheric lapse rate       {} K/km\nAtmospheric type:             {}\n\nAtmospheric profile has {layers} layers.\n",
        starting_lines(p, "mbar"),
        number(p.pressure_step_hpa),
        number(p.altitude_m),
        number(p.top_altitude_km),
        number(PROFILE_STEP_FACTOR),
        number(p.lapse_rate_k_per_km),
        atmosphere_name(p.atmosphere)
    )
}

// C++ ostream's default format has six significant digits and trims zeros.
fn number(value: f64) -> String {
    let scientific = format!("{value:.5e}");
    let Some((mantissa, exponent)) = scientific.split_once('e') else {
        return scientific;
    };
    let exponent: i32 = exponent.parse().expect("Rust-generated exponent");
    let trim = |input: String| {
        if input.contains('.') {
            input.trim_end_matches('0').trim_end_matches('.').to_owned()
        } else {
            input
        }
    };
    if (-4..6).contains(&exponent) {
        let precision = (5 - exponent) as usize;
        trim(format!("{value:.precision$}"))
    } else {
        format!("{}e{exponent:+03}", trim(mantissa.to_owned()))
    }
}
