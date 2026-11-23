// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA window geometry, selection and channel coordinates.

use casa_atmosphere::{AtmosphereSpectrum, SpectralGrid, SpectralWindow};
use casa_provider_contracts::CasaValue;
use pyo3::prelude::*;

use super::quantity::{self, AtmosphereQuantity, QuantityArg};
use super::{PyAtmosphere, error, warn};
use crate::convert::reply_value::reply_value;

pub(super) const NEED_WINDOWS: &str = "Please set spectral window(s) with initSpectralWindow.";

struct Window {
    channels: usize,
    reference: usize,
    frequency: f64,
    separation: f64,
}

impl Window {
    fn read(
        center: &AtmosphereQuantity,
        width: &AtmosphereQuantity,
        resolution: &AtmosphereQuantity,
        index: usize,
    ) -> PyResult<Self> {
        let nominal_separation = resolution.values[index];
        let separation = resolution.frequency_hz(index)?;
        let count = if nominal_separation == 0. {
            1.
        } else {
            (width.index_in(index, &resolution.unit)? / nominal_separation).ceil()
        };
        if !(1.0..=f64::from(i32::MAX)).contains(&count) {
            return Err(error("Invalid spectral window channel count"));
        }
        let channels = count as usize;
        let reference = (channels - 1) / 2;
        let mut frequency = center.frequency_hz(index)?;
        if channels.is_multiple_of(2) {
            frequency -= separation / 2.;
        }
        Ok(Self {
            channels,
            reference,
            frequency,
            separation,
        })
    }
}

#[pymethods]
#[allow(non_snake_case)]
impl PyAtmosphere {
    #[pyo3(name = "initSpectralWindow", signature = (nbands=1, fCenter=QuantityArg::Omitted, fWidth=QuantityArg::Omitted, fRes=QuantityArg::Omitted), text_signature = "($self, nbands=1, fCenter={'value': 90.0, 'unit': 'GHz'}, fWidth={'value': 0.64, 'unit': 'GHz'}, fRes={'value': 0.0, 'unit': 'GHz'})")]
    fn init_spectral_window(
        &self,
        py: Python<'_>,
        nbands: i64,
        fCenter: QuantityArg,
        fWidth: QuantityArg,
        fRes: QuantityArg,
    ) -> PyResult<i64> {
        let center = fCenter.read("fCenter", (90., "GHz"))?;
        let width = fWidth.read("fWidth", (0.64, "GHz"))?;
        let resolution = fRes.read("fRes", (0., "GHz"))?;
        let nbands = usize::try_from(nbands)
            .ok()
            .filter(|&count| count > 0)
            .ok_or_else(|| error("Exception: nbands should be > 0.."))?;
        if !self.run(py, |session| session.profile().is_some()) {
            warn(
                py,
                "Initialize atmospheric profile with initAtmProfile first.",
            )?;
            return Ok(-1);
        }
        if [
            center.values.len(),
            width.values.len(),
            resolution.values.len(),
        ] != [nbands; 3]
        {
            warn(py, "Dimensions of fCenter, fWidth and fRes != nbands!")?;
            return Ok(-1);
        }
        let mut grid = SpectralGrid::new();
        for index in 0..nbands {
            let window = Window::read(&center, &width, &resolution, index)?;
            grid.add_regular(
                window.channels,
                window.reference,
                window.frequency,
                window.separation,
            )
            .map_err(error)?;
        }
        let count = grid.windows()[0].num_channels() as i64;
        if !self.run(py, |session| session.set_grid(grid)) {
            return Ok(-1);
        }
        Ok(count)
    }

    #[pyo3(name = "addSpectralWindow", signature = (fCenter=QuantityArg::Omitted, fWidth=QuantityArg::Omitted, fRes=QuantityArg::Omitted), text_signature = "($self, fCenter={'value': 350.0, 'unit': 'GHz'}, fWidth={'value': 0.008, 'unit': 'GHz'}, fRes={'value': 0.002, 'unit': 'GHz'})")]
    fn add_spectral_window(
        &self,
        py: Python<'_>,
        fCenter: QuantityArg,
        fWidth: QuantityArg,
        fRes: QuantityArg,
    ) -> PyResult<i64> {
        let center = fCenter.read("fCenter", (350., "GHz"))?;
        let width = fWidth.read("fWidth", (0.008, "GHz"))?;
        let resolution = fRes.read("fRes", (0.002, "GHz"))?;
        if !self.run(py, |session| session.spectrum().is_some()) {
            warn(
                py,
                "Initialize spectral window with initSpectralWindow first.",
            )?;
            return Ok(-1);
        }
        if resolution.scalar()? == 0. {
            warn(py, "Resolution of band cannot be 0,0 GHz!")?;
            return Ok(-1);
        }
        center.scalar()?;
        width.scalar()?;
        let window = Window::read(&center, &width, &resolution, 0)?;
        self.run(py, |session| {
            session.append_window(
                window.channels,
                window.reference,
                window.frequency,
                window.separation,
            )
        })
        .map(|result| result.map_or(-1, |_| window.channels as i64))
        .map_err(error)
    }

    #[pyo3(name = "getNumSpectralWindows")]
    fn get_num_spectral_windows(&self, py: Python<'_>) -> PyResult<i64> {
        let count = self.run(py, |session| {
            session
                .spectrum()
                .map(|s| s.absorption.grid().windows().len() as i64)
        });
        if count.is_none() {
            warn(py, NEED_WINDOWS)?;
        }
        Ok(count.unwrap_or(-1))
    }

    #[pyo3(name = "getNumChan", signature = (spwid=0))]
    fn get_num_chan(&self, py: Python<'_>, spwid: i64) -> PyResult<i64> {
        self.read_window(py, spwid, |window| Ok(window.num_channels() as i64))
            .map(|n| n.unwrap_or(-1))
    }

    #[pyo3(name = "getRefChan", signature = (spwid=0))]
    fn get_ref_chan(&self, py: Python<'_>, spwid: i64) -> PyResult<i64> {
        self.read_window(py, spwid, |window| Ok(window.reference_channel() as i64))
            .map(|n| n.unwrap_or(-1))
    }

    #[pyo3(name = "getRefFreq", signature = (spwid=0))]
    fn get_ref_freq(&self, py: Python<'_>, spwid: i64) -> PyResult<Py<PyAny>> {
        self.window_quantity(py, spwid, "GHz", |window| {
            Ok(window.reference_frequency_hz() / 1e9)
        })
    }

    #[pyo3(name = "getChanSep", signature = (spwid=0))]
    fn get_chan_sep(&self, py: Python<'_>, spwid: i64) -> PyResult<Py<PyAny>> {
        self.window_quantity(py, spwid, "MHz", |window| {
            Ok(window.channel_separation_hz() / 1e6)
        })
    }

    #[pyo3(name = "getChanFreq", signature = (chanNum=0, spwid=0))]
    fn get_chan_freq(&self, py: Python<'_>, chanNum: i64, spwid: i64) -> PyResult<Py<PyAny>> {
        self.window_quantity(py, spwid, "GHz", |window| {
            window
                .frequencies_hz()
                .get(chanNum as usize)
                .map(|&frequency| frequency / 1e9)
                .ok_or("Exception: Channel ID out of range..")
        })
    }

    #[pyo3(name = "getSpectralWindow", signature = (spwid=0))]
    fn get_spectral_window(&self, py: Python<'_>, spwid: i64) -> PyResult<Py<PyAny>> {
        let spectrum = self.read_window(py, spwid, |window| {
            Ok(quantity::vector(
                "Hz",
                window.frequencies_hz().iter().copied(),
            ))
        })?;
        reply_value(py, &spectrum.unwrap_or_else(quantity::empty))
    }

    #[pyo3(name = "getChanNum", signature = (freq, spwid=0))]
    fn get_chan_num(&self, py: Python<'_>, freq: QuantityArg, spwid: i64) -> PyResult<f64> {
        let frequency = freq.read("freq", (0., "Hz"))?.frequency_hz(0)?;
        self.read_window(py, spwid, |window| {
            Ok(if window.num_channels() == 1 {
                1.
            } else {
                (frequency - window.reference_frequency_hz()) / window.channel_separation_hz()
            })
        })
        .map(|value| value.unwrap_or(-1.))
    }

    #[pyo3(name = "getBandwidth", signature = (spwid=0))]
    fn get_bandwidth(&self, py: Python<'_>, spwid: i64) -> PyResult<Py<PyAny>> {
        self.window_quantity(py, spwid, "GHz", |window| {
            Ok((window.max_frequency_hz() - window.min_frequency_hz()) / 1e9)
        })
    }

    #[pyo3(name = "getMinFreq", signature = (spwid=0))]
    fn get_min_freq(&self, py: Python<'_>, spwid: i64) -> PyResult<Py<PyAny>> {
        self.window_quantity(py, spwid, "GHz", |window| {
            Ok(window.min_frequency_hz() / 1e9)
        })
    }

    #[pyo3(name = "getMaxFreq", signature = (spwid=0))]
    fn get_max_freq(&self, py: Python<'_>, spwid: i64) -> PyResult<Py<PyAny>> {
        self.window_quantity(py, spwid, "GHz", |window| {
            Ok(window.max_frequency_hz() / 1e9)
        })
    }
}

pub(super) fn spectral_window(
    spectrum: &AtmosphereSpectrum,
    spwid: i64,
) -> Result<&SpectralWindow, &'static str> {
    spectrum
        .absorption
        .grid()
        .windows()
        .get(spwid as usize)
        .ok_or("Exception: Spw ID out of range..")
}

impl PyAtmosphere {
    fn read_window<T: Send>(
        &self,
        py: Python<'_>,
        spwid: i64,
        read: impl FnOnce(&SpectralWindow) -> Result<T, &'static str> + Send,
    ) -> PyResult<Option<T>> {
        let result = self.run(py, |session| {
            session
                .spectrum()
                .map(|s| spectral_window(s, spwid).and_then(read))
        });
        match result {
            Some(result) => result.map(Some).map_err(error),
            None => {
                warn(py, NEED_WINDOWS)?;
                Ok(None)
            }
        }
    }

    fn window_quantity(
        &self,
        py: Python<'_>,
        spwid: i64,
        unit: &str,
        read: impl FnOnce(&SpectralWindow) -> Result<f64, &'static str> + Send,
    ) -> PyResult<Py<PyAny>> {
        let value = self.read_window(py, spwid, read)?;
        reply_value(
            py,
            &value.map_or_else(quantity::empty, |value| quantity::scalar(unit, value)),
        )
    }
}
