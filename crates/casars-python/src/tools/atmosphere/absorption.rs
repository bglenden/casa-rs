// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opacity, delay, path and layer absorption returned by the CASA tool.

use std::ops::Range;

use casa_atmosphere::{AtmosphereSpectrum, absorption::Absorber};
use casa_provider_contracts::CasaValue;
use pyo3::prelude::*;

use super::quantity;
use super::windows::spectral_window;
use super::{PyAtmosphere, error, tuple};
use crate::convert::reply_value::reply_value;

pub(super) const NO_WINDOW: &str = "Exception: Spectral window is not defined yet.";
pub(super) const INVALID_CHANNEL: f64 = -999.0;

#[pymethods]
impl PyAtmosphere {
    #[pyo3(name = "getDryOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_dry_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<f64> {
        self.channel_value(py, nc, spwid, |s, c| s.absorption.dry_opacity(c))
    }

    #[pyo3(name = "getDryContOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_dry_cont_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<f64> {
        self.channel_value(py, nc, spwid, |s, c| s.absorption.dry_continuum_opacity(c))
    }

    #[pyo3(name = "getO2LinesOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_o2_lines_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<f64> {
        self.channel_value(py, nc, spwid, |s, c| {
            s.absorption.absorber_opacity(c, Absorber::O2Lines)
        })
    }

    #[pyo3(name = "getO3LinesOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_o3_lines_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<f64> {
        self.channel_value(py, nc, spwid, |s, c| {
            s.absorption.absorber_opacity(c, Absorber::O3Lines)
        })
    }

    #[pyo3(name = "getCOLinesOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_co_lines_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<f64> {
        self.channel_value(py, nc, spwid, |s, c| {
            s.absorption.absorber_opacity(c, Absorber::CoLines)
        })
    }

    #[pyo3(name = "getN2OLinesOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_n2_o_lines_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<f64> {
        self.channel_value(py, nc, spwid, |s, c| {
            s.absorption.absorber_opacity(c, Absorber::N2oLines)
        })
    }

    #[pyo3(name = "getH2OLinesOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_h2_o_lines_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<f64> {
        self.channel_value(py, nc, spwid, |s, c| {
            s.absorption.h2o_lines_opacity(c, s.sky.water_column_m)
        })
    }

    #[pyo3(name = "getH2OContOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_h2_o_cont_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<f64> {
        self.channel_value(py, nc, spwid, |s, c| {
            s.absorption.h2o_continuum_opacity(c, s.sky.water_column_m)
        })
    }

    #[pyo3(name = "getWetOpacity", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_wet_opacity(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "neper", |s, c| {
            s.absorption.wet_opacity(c, s.sky.water_column_m)
        })
    }

    #[pyo3(name = "getDispersivePhaseDelay", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_dispersive_phase_delay(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "deg", |s, c| {
            s.absorption
                .dispersive_h2o_phase_delay_deg(c, s.sky.water_column_m)
        })
    }

    #[pyo3(name = "getDispersiveWetPhaseDelay", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_dispersive_wet_phase_delay(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "deg", |s, c| {
            s.absorption
                .dispersive_h2o_phase_delay_deg(c, s.absorption.ground_water_column_m())
        })
    }

    #[pyo3(name = "getNonDispersiveWetPhaseDelay", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_non_dispersive_wet_phase_delay(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "deg", |s, c| {
            s.absorption
                .non_dispersive_h2o_phase_delay_deg(c, s.absorption.ground_water_column_m())
        })
    }

    #[pyo3(name = "getNonDispersiveDryPhaseDelay", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_non_dispersive_dry_phase_delay(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "deg", |s, c| {
            s.absorption.non_dispersive_dry_phase_delay_deg(c)
        })
    }

    #[pyo3(name = "getNonDispersivePhaseDelay", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_non_dispersive_phase_delay(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "deg", |s, c| {
            s.absorption
                .non_dispersive_h2o_phase_delay_deg(c, s.sky.water_column_m)
        })
    }

    #[pyo3(name = "getDispersivePathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_dispersive_path_length(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption
                .dispersive_h2o_path_length_m(c, s.sky.water_column_m)
        })
    }

    #[pyo3(name = "getDispersiveWetPathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_dispersive_wet_path_length(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption
                .dispersive_h2o_path_length_m(c, s.absorption.ground_water_column_m())
        })
    }

    #[pyo3(name = "getNonDispersiveWetPathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_non_dispersive_wet_path_length(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption
                .non_dispersive_h2o_path_length_m(c, s.absorption.ground_water_column_m())
        })
    }

    #[pyo3(name = "getNonDispersiveDryPathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_non_dispersive_dry_path_length(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption.non_dispersive_dry_path_length_m(c)
        })
    }

    #[pyo3(name = "getNonDispersivePathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_non_dispersive_path_length(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption
                .non_dispersive_h2o_path_length_m(c, s.sky.water_column_m)
        })
    }

    #[pyo3(name = "getO2LinesPathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_o2_lines_path_length(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption.absorber_path_length_m(c, Absorber::O2Lines)
        })
    }

    #[pyo3(name = "getO3LinesPathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_o3_lines_path_length(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption.absorber_path_length_m(c, Absorber::O3Lines)
        })
    }

    #[pyo3(name = "getCOLinesPathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_co_lines_path_length(&self, py: Python<'_>, nc: i64, spwid: i64) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption.absorber_path_length_m(c, Absorber::CoLines)
        })
    }

    #[pyo3(name = "getN2OLinesPathLength", signature = (nc=-1, spwid=0), text_signature = "($self, nc=-1, spwid=0)")]
    fn get_n2_o_lines_path_length(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.channel_quantity(py, nc, spwid, "m", |s, c| {
            s.absorption.absorber_path_length_m(c, Absorber::N2oLines)
        })
    }

    #[pyo3(name = "getAbsH2OLines", signature = (nl, nf=0, spwid=0))]
    fn get_abs_h2_o_lines(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            Absorber::H2oLines.of(s.absorption.layer(c, l)).im
        })
    }

    #[pyo3(name = "getAbsH2OCont", signature = (nl, nf=0, spwid=0))]
    fn get_abs_h2_o_cont(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            Absorber::H2oContinuum.of(s.absorption.layer(c, l)).im
        })
    }

    #[pyo3(name = "getAbsO2Lines", signature = (nl, nf=0, spwid=0))]
    fn get_abs_o2_lines(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            Absorber::O2Lines.of(s.absorption.layer(c, l)).im
        })
    }

    #[pyo3(name = "getAbsDryCont", signature = (nl, nf=0, spwid=0))]
    fn get_abs_dry_cont(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            Absorber::DryContinuum.of(s.absorption.layer(c, l)).im
        })
    }

    #[pyo3(name = "getAbsO3Lines", signature = (nl, nf=0, spwid=0))]
    fn get_abs_o3_lines(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            Absorber::O3Lines.of(s.absorption.layer(c, l)).im
        })
    }

    #[pyo3(name = "getAbsCOLines", signature = (nl, nf=0, spwid=0))]
    fn get_abs_co_lines(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            Absorber::CoLines.of(s.absorption.layer(c, l)).im
        })
    }

    #[pyo3(name = "getAbsN2OLines", signature = (nl, nf=0, spwid=0))]
    fn get_abs_n2_o_lines(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            Absorber::N2oLines.of(s.absorption.layer(c, l)).im
        })
    }

    #[pyo3(name = "getAbsTotalDry", signature = (nl, nf=0, spwid=0))]
    fn get_abs_total_dry(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            s.absorption.layer_absorption(c, l).0
        })
    }

    #[pyo3(name = "getAbsTotalWet", signature = (nl, nf=0, spwid=0))]
    fn get_abs_total_wet(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
    ) -> PyResult<Py<PyAny>> {
        self.layer_quantity(py, nl, nf, spwid, |s, c, l| {
            s.absorption.layer_absorption(c, l).1
        })
    }

    #[pyo3(name = "getDryOpacitySpec", signature = (spwid=0))]
    fn get_dry_opacity_spec(&self, py: Python<'_>, spwid: i64) -> PyResult<Py<PyAny>> {
        let values = self.spectral_values(py, spwid, |s, c| s.absorption.dry_opacity(c))?;
        tuple(
            py,
            [
                CasaValue::Int {
                    value: values.len() as i64,
                },
                CasaValue::float64_vector(values),
            ],
        )
    }

    #[pyo3(name = "getWetOpacitySpec", signature = (spwid=0))]
    fn get_wet_opacity_spec(&self, py: Python<'_>, spwid: i64) -> PyResult<Py<PyAny>> {
        let values = self.spectral_values(py, spwid, |s, c| {
            s.absorption.wet_opacity(c, s.sky.water_column_m)
        })?;
        tuple(
            py,
            [
                CasaValue::Int {
                    value: values.len() as i64,
                },
                quantity::vector("neper", values),
            ],
        )
    }
}

impl PyAtmosphere {
    pub(super) fn with_spectrum<T: Send>(
        &self,
        py: Python<'_>,
        read: impl FnOnce(&AtmosphereSpectrum) -> Result<T, &'static str> + Send,
    ) -> PyResult<T> {
        self.run(py, |session| read(session.spectrum().ok_or(NO_WINDOW)?))
            .map_err(error)
    }

    pub(super) fn channel_value(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
        read: impl FnOnce(&AtmosphereSpectrum, usize) -> f64 + Send,
    ) -> PyResult<f64> {
        self.with_spectrum(py, |spectrum| {
            let index = channel_index(spectrum, spwid, nc)?;
            Ok(index.map_or(INVALID_CHANNEL, |channel| read(spectrum, channel)))
        })
    }

    fn channel_quantity(
        &self,
        py: Python<'_>,
        nc: i64,
        spwid: i64,
        unit: &str,
        read: impl FnOnce(&AtmosphereSpectrum, usize) -> f64 + Send,
    ) -> PyResult<Py<PyAny>> {
        let value = self.channel_value(py, nc, spwid, read)?;
        reply_value(py, &quantity::scalar(unit, value))
    }

    fn spectral_values(
        &self,
        py: Python<'_>,
        spwid: i64,
        read: impl Fn(&AtmosphereSpectrum, usize) -> f64 + Send,
    ) -> PyResult<Vec<f64>> {
        self.with_spectrum(py, move |spectrum| {
            Ok(channel_range(spectrum, spwid)?
                .map(|channel| read(spectrum, channel))
                .collect())
        })
    }

    fn layer_quantity(
        &self,
        py: Python<'_>,
        nl: i64,
        nf: i64,
        spwid: i64,
        read: impl FnOnce(&AtmosphereSpectrum, usize, usize) -> f64 + Send,
    ) -> PyResult<Py<PyAny>> {
        let layer = usize::try_from(nl).map_err(|_| error("Failed AlwaysAssert value>=0"))?;
        let absorption = self.with_spectrum(py, |spectrum| {
            let window = spectral_window(spectrum, spwid)?;
            if nf < 0 || nf as usize >= window.num_channels() {
                return Err("Exception: Channel ID out of range..");
            }
            let channel = spectrum
                .absorption
                .grid()
                .flat_index(spwid as usize, nf as usize)
                .expect("validated channel");
            if layer >= spectrum.absorption.atmosphere().layers().len() {
                return Err("Exception: Layer ID out of range..");
            }
            Ok(read(spectrum, channel, layer))
        })?;
        reply_value(py, &quantity::scalar("m-1", absorption))
    }
}

pub(super) fn channel_index(
    spectrum: &AtmosphereSpectrum,
    spwid: i64,
    channel: i64,
) -> Result<Option<usize>, &'static str> {
    let window = spectral_window(spectrum, spwid)?;
    let index = usize::try_from(channel).unwrap_or_else(|_| window.reference_channel());
    Ok(spectrum.absorption.grid().flat_index(spwid as usize, index))
}

pub(super) fn channel_range(
    spectrum: &AtmosphereSpectrum,
    spwid: i64,
) -> Result<Range<usize>, &'static str> {
    let window = spectral_window(spectrum, spwid)?;
    let start = spectrum
        .absorption
        .grid()
        .flat_index(spwid as usize, 0)
        .expect("configured window has channels");
    Ok(start..start + window.num_channels())
}
