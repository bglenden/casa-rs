// SPDX-License-Identifier: LGPL-3.0-or-later
//! User sky state and equivalent-blackbody and Rayleigh–Jeans temperatures.

use casa_atmosphere::{AtmosphereError, AtmosphereSpectrum, Sky};
use casa_provider_contracts::CasaValue;
use pyo3::prelude::*;

use super::absorption::{INVALID_CHANNEL, channel_index, channel_range};
use super::quantity::{self, QuantityArg};
use super::windows::NEED_WINDOWS;
use super::{PyAtmosphere, tuple, warn};
use crate::convert::reply_value::reply_value;

#[derive(Clone, Copy)]
enum Temperature {
    EquivalentBlackbody,
    RayleighJeans,
}

impl Temperature {
    fn at_channel(self, spectrum: &AtmosphereSpectrum, channel: usize, sky: &Sky) -> f64 {
        let value = match self {
            Self::EquivalentBlackbody => spectrum.absorption.tebb_sky(channel, sky),
            Self::RayleighJeans => spectrum.absorption.trj_sky(channel, sky),
        };
        value.unwrap_or(INVALID_CHANNEL)
    }

    fn average(
        self,
        spectrum: &AtmosphereSpectrum,
        window: usize,
        sky: &Sky,
    ) -> Result<f64, AtmosphereError> {
        match self {
            Self::EquivalentBlackbody => spectrum.absorption.average_tebb_sky(window, sky),
            Self::RayleighJeans => spectrum.absorption.average_trj_sky(window, sky),
        }
    }
}

#[pymethods]
impl PyAtmosphere {
    #[pyo3(name="setUserWH2O", signature=(wh2o=QuantityArg::Omitted), text_signature="($self, wh2o={'value': 0.0, 'unit': 'mm'})")]
    fn set_user_water(&self, py: Python<'_>, wh2o: QuantityArg) -> PyResult<bool> {
        let water = wh2o.read("wh2o", (0., "mm"))?.water_m()?;
        self.set_sky(py, |sky| sky.water_column_m = water)
    }

    #[pyo3(name = "getUserWH2O")]
    fn get_user_water(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.sky_quantity(py, "mm", |sky| 1000. * sky.water_column_m)
    }

    #[pyo3(name = "setAirMass")]
    fn set_air_mass(&self, py: Python<'_>, airmass: f64) -> PyResult<bool> {
        self.set_sky(py, |sky| sky.airmass = airmass)
    }

    #[pyo3(name = "getAirMass")]
    fn get_air_mass(&self, py: Python<'_>) -> PyResult<f64> {
        Ok(self.read_sky(py, |sky| sky.airmass)?.unwrap_or(-1.))
    }

    #[pyo3(name="setSkyBackgroundTemperature", signature=(tbgr=QuantityArg::Omitted), text_signature="($self, tbgr={'value': 2.73, 'unit': 'K'})")]
    fn set_sky_background_temperature(&self, py: Python<'_>, tbgr: QuantityArg) -> PyResult<bool> {
        let background = tbgr.read("tbgr", (2.73, "K"))?.sky_kelvin()?;
        self.set_sky(py, |sky| sky.background_temperature_k = background)
    }

    #[pyo3(name = "getSkyBackgroundTemperature")]
    fn get_sky_background_temperature(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.sky_quantity(py, "K", |sky| sky.background_temperature_k)
    }

    #[pyo3(name="getAverageTebbSky", signature=(spwid=0, wh2o=QuantityArg::Omitted), text_signature="($self, spwid=0, wh2o={'value': -1.0, 'unit': 'mm'})")]
    fn get_average_tebb_sky(
        &self,
        py: Python<'_>,
        spwid: i64,
        wh2o: QuantityArg,
    ) -> PyResult<Py<PyAny>> {
        self.average_temperature(py, spwid, wh2o, Temperature::EquivalentBlackbody)
    }

    #[pyo3(name="getTebbSky", signature=(nc=-1, spwid=0, wh2o=QuantityArg::Omitted), text_signature="($self, nc=-1, spwid=0, wh2o={'value': -1.0, 'unit': 'mm'})")]
    fn get_tebb_sky(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
        wh2o: QuantityArg,
    ) -> PyResult<Py<PyAny>> {
        self.channel_temperature(py, nc, spwid, wh2o, Temperature::EquivalentBlackbody)
    }

    #[pyo3(name="getTebbSkySpec", signature=(spwid=0, wh2o=QuantityArg::Omitted), text_signature="($self, spwid=0, wh2o={'value': -1.0, 'unit': 'mm'})")]
    fn get_tebb_sky_spec(
        &self,
        py: Python<'_>,
        spwid: i64,
        wh2o: QuantityArg,
    ) -> PyResult<Py<PyAny>> {
        self.temperature_spectrum(py, spwid, wh2o, Temperature::EquivalentBlackbody)
    }

    #[pyo3(name="getAverageTrjSky", signature=(spwid=0, wh2o=QuantityArg::Omitted), text_signature="($self, spwid=0, wh2o={'value': -1.0, 'unit': 'mm'})")]
    fn get_average_trj_sky(
        &self,
        py: Python<'_>,
        spwid: i64,
        wh2o: QuantityArg,
    ) -> PyResult<Py<PyAny>> {
        self.average_temperature(py, spwid, wh2o, Temperature::RayleighJeans)
    }

    #[pyo3(name="getTrjSky", signature=(nc=-1, spwid=0, wh2o=QuantityArg::Omitted), text_signature="($self, nc=-1, spwid=0, wh2o={'value': -1.0, 'unit': 'mm'})")]
    fn get_trj_sky(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
        wh2o: QuantityArg,
    ) -> PyResult<Py<PyAny>> {
        self.channel_temperature(py, nc, spwid, wh2o, Temperature::RayleighJeans)
    }

    #[pyo3(name="getTrjSkySpec", signature=(spwid=0, wh2o=QuantityArg::Omitted), text_signature="($self, spwid=0, wh2o={'value': -1.0, 'unit': 'mm'})")]
    fn get_trj_sky_spec(
        &self,
        py: Python<'_>,
        spwid: i64,
        wh2o: QuantityArg,
    ) -> PyResult<Py<PyAny>> {
        self.temperature_spectrum(py, spwid, wh2o, Temperature::RayleighJeans)
    }
}

impl PyAtmosphere {
    fn set_sky(&self, py: Python<'_>, change: impl FnOnce(&mut Sky) + Send) -> PyResult<bool> {
        let applied = self.run(py, |session| session.sky_mut().map(change).is_some());
        if !applied {
            warn(py, NEED_WINDOWS)?;
        }
        Ok(applied)
    }

    fn read_sky(
        &self,
        py: Python<'_>,
        read: impl FnOnce(&Sky) -> f64 + Send,
    ) -> PyResult<Option<f64>> {
        let result = self.run(py, |session| {
            session.spectrum().map(|spectrum| read(&spectrum.sky))
        });
        if result.is_none() {
            warn(py, NEED_WINDOWS)?;
        }
        Ok(result)
    }

    fn sky_quantity(
        &self,
        py: Python<'_>,
        unit: &str,
        read: impl FnOnce(&Sky) -> f64 + Send,
    ) -> PyResult<Py<PyAny>> {
        let value = self.read_sky(py, read)?;
        reply_value(
            py,
            &value.map_or_else(quantity::empty, |value| quantity::scalar(unit, value)),
        )
    }

    fn average_temperature(
        &self,
        py: Python<'_>,
        spwid: i64,
        wh2o: QuantityArg,
        kind: Temperature,
    ) -> PyResult<Py<PyAny>> {
        let water = override_water(wh2o)?;
        let value = self.with_spectrum(py, |spectrum| {
            channel_range(spectrum, spwid)?;
            Ok(kind
                .average(spectrum, spwid as usize, &effective_sky(spectrum, water))
                .unwrap_or(INVALID_CHANNEL))
        })?;
        reply_value(py, &quantity::scalar("K", value))
    }

    fn channel_temperature(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
        wh2o: QuantityArg,
        kind: Temperature,
    ) -> PyResult<Py<PyAny>> {
        let water = override_water(wh2o)?;
        let value = self.with_spectrum(py, |spectrum| {
            Ok(
                channel_index(spectrum, spwid, nc)?.map_or(INVALID_CHANNEL, |channel| {
                    kind.at_channel(spectrum, channel, &effective_sky(spectrum, water))
                }),
            )
        })?;
        reply_value(py, &quantity::scalar("K", value))
    }

    fn temperature_spectrum(
        &self,
        py: Python<'_>,
        spwid: i64,
        wh2o: QuantityArg,
        kind: Temperature,
    ) -> PyResult<Py<PyAny>> {
        let water = override_water(wh2o)?;
        let values = self.with_spectrum(py, |spectrum| {
            let sky = effective_sky(spectrum, water);
            Ok(channel_range(spectrum, spwid)?
                .map(|channel| kind.at_channel(spectrum, channel, &sky))
                .collect::<Vec<_>>())
        })?;
        tuple(
            py,
            [
                CasaValue::Int {
                    value: values.len() as i64,
                },
                quantity::vector("K", values),
            ],
        )
    }
}

fn override_water(value: QuantityArg) -> PyResult<Option<f64>> {
    let value = value.read("wh2o", (-1., "mm"))?;
    if value.scalar()? == -1. {
        Ok(None)
    } else {
        value.water_m().map(Some)
    }
}

fn effective_sky(spectrum: &AtmosphereSpectrum, water: Option<f64>) -> Sky {
    Sky {
        water_column_m: water.unwrap_or(spectrum.sky.water_column_m),
        ..spectrum.sky
    }
}
