// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::measures::record::frequency_to_record;
use casa_core::quanta::{Dimension, Quantity, Unit, UnitDim, casacore, constants};
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::columns::frequency_columns::ChanFreqColumn;
use casa_ms::{MsError, MsResult, MsSpectralWindow, SubTable};
use numpy::IntoPyArray;
use pyo3::prelude::*;

use super::{PyMsMetadata, Source};
use crate::convert::value::CasaValue;

pub(super) fn spw_row(spw: &MsSpectralWindow<'_>, id: i64) -> MsResult<usize> {
    let count = spw.row_count();
    usize::try_from(id)
        .ok()
        .filter(|&row| row < count)
        .ok_or_else(|| {
            MsError::InvalidInput(format!(
                "Spectral window ID {id} out of range, must be nonnegative and less than {count}"
            ))
        })
}

pub(super) fn water_vapor_windows(spw: &MsSpectralWindow<'_>) -> MsResult<Vec<usize>> {
    let mut all = Vec::new();
    let mut nominal = Vec::new();
    for row in 0..spw.row_count() {
        let name = spw.name(row)?;
        if name.contains("WVR") {
            all.push(row);
            if name == "WVR#NOMINAL" {
                nominal.push(row);
            }
        }
    }
    Ok(if nominal.is_empty() { all } else { nominal })
}

fn column_unit(spw: &MsSpectralWindow<'_>, column: &str) -> MsResult<Unit> {
    let unit = spw
        .table()
        .column_keywords(column)
        .and_then(|keywords| keywords.get("QuantumUnits"))
        .and_then(|units| match units {
            Value::Array(ArrayValue::String(units)) => units.first().map(String::as_str),
            Value::Scalar(ScalarValue::String(unit)) => Some(unit.as_str()),
            _ => None,
        })
        .unwrap_or("Hz");
    casacore::casacore_unit(unit).map_err(|error| MsError::InvalidInput(error.to_string()))
}

fn convert_units(
    values: &mut [f64],
    source: Unit,
    requested: &str,
    conformant: bool,
) -> MsResult<()> {
    let target = casacore::casacore_unit(requested)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    if source.val().dim == target.val().dim {
        let inverse = target.val().factor / source.val().factor;
        if inverse != 1.0 {
            for value in values {
                *value /= inverse;
            }
        }
    } else {
        if conformant {
            let time = UnitDim::basis(Dimension::Time);
            let angle = UnitDim::basis(Dimension::Angle);
            let length = UnitDim::basis(Dimension::Length);
            let frequency = time.pow(-1);
            let dimensions = (source.val().dim, target.val().dim);
            if ![
                (time, angle),
                (angle, time),
                (length, frequency),
                (frequency, length),
            ]
            .contains(&dimensions)
            {
                return Err(MsError::InvalidInput(format!(
                    "From/to units not consistent. Cannot convert {} to {}",
                    source.name(),
                    target.name()
                )));
            }
        }
        for value in values {
            *value = casacore::get_value(&Quantity::with_unit(*value, source.clone()), &target);
        }
    }
    Ok(())
}

fn mean_frequency(windows: &MsSpectralWindow<'_>, row: usize) -> MsResult<f64> {
    let values = windows.chan_freq(row)?;
    if values.is_empty() {
        return Err(MsError::InvalidInput("No channel frequencies".into()));
    }
    Ok(values.iter().sum::<f64>() / values.len() as f64)
}

fn channel_resolution(
    windows: &MsSpectralWindow<'_>,
    row: usize,
    column: &str,
    mut values: Vec<f64>,
    unit: &str,
    as_velocity: bool,
) -> MsResult<Vec<f64>> {
    let mut source = column_unit(windows, column)?;
    let hz =
        casacore::casacore_unit("Hz").map_err(|error| MsError::InvalidInput(error.to_string()))?;
    if as_velocity && source.val().dim == hz.val().dim {
        let mut frequency = mean_frequency(windows, row)?;
        if frequency > 0.0 {
            convert_units(
                std::slice::from_mut(&mut frequency),
                column_unit(windows, "CHAN_FREQ")?,
                "Hz",
                false,
            )?;
            convert_units(&mut values, source, "Hz", true)?;
            for value in &mut values {
                *value = *value / frequency * (constants::c().value() / 1000.0);
            }
            source = casacore::casacore_unit("km/s")
                .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        }
    }
    let unit = if unit.is_empty() {
        if as_velocity { "km/s" } else { "Hz" }
    } else {
        unit
    };
    convert_units(&mut values, source, unit, true)?;
    Ok(values)
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (includewvr = true), text_signature = "($self, includewvr=True)")]
    fn nspw(&self, py: Python<'_>, includewvr: bool) -> PyResult<usize> {
        self.with_attachment(py, |attached| {
            let known = attached.spectral_windows;
            let (total, excluded) = if includewvr {
                let total = match &attached.source {
                    Source::Linked(link) => {
                        link.verify_directory()?;
                        match known {
                            Some(total) => total,
                            None => link.spectral_window_count()?,
                        }
                    }
                    Source::Open(snapshot) => snapshot.ms.spectral_window()?.row_count(),
                };
                (total, 0)
            } else {
                attached.query_snapshot(|ms| {
                    let windows = ms.spectral_window()?;
                    let total = known.unwrap_or_else(|| windows.row_count());
                    let excluded = water_vapor_windows(&windows)?
                        .into_iter()
                        .filter(|&id| id < total)
                        .count();
                    Ok((total, excluded))
                })?
            };
            if total > 0 {
                attached.spectral_windows = Some(total);
            }
            Ok(total - excluded)
        })
    }

    #[pyo3(signature = (complement = false), text_signature = "($self, complement=False)")]
    fn wvrspws(&self, py: Python<'_>, complement: bool) -> PyResult<Py<PyAny>> {
        let ids = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let wvr = water_vapor_windows(&windows)?;
            let selected = if complement {
                (0..windows.row_count())
                    .filter(|index| wvr.binary_search(index).is_err())
                    .collect::<Vec<_>>()
            } else {
                wvr
            };
            Ok(selected.into_iter().map(|id| id as i64).collect::<Vec<_>>())
        })?;
        Ok(ids.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (spw), text_signature = "($self, spw)")]
    fn nchan(&self, py: Python<'_>, spw: i64) -> PyResult<usize> {
        self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let row = spw_row(&windows, spw)?;
            let widths = windows.table().cell_accessor(row, "CHAN_WIDTH")?.array()?;
            Ok(widths.len())
        })
    }

    #[pyo3(signature = (spw, unit = "Hz"), text_signature = "($self, spw, unit='Hz')")]
    fn chanfreqs(&self, py: Python<'_>, spw: i64, unit: &str) -> PyResult<Py<PyAny>> {
        let frequencies = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let mut values = windows.chan_freq(spw_row(&windows, spw)?)?;
            convert_units(
                &mut values,
                column_unit(&windows, "CHAN_FREQ")?,
                unit,
                false,
            )?;
            Ok(values)
        })?;
        Ok(frequencies.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (spw, unit = "Hz"), text_signature = "($self, spw, unit='Hz')")]
    fn chanwidths(&self, py: Python<'_>, spw: i64, unit: &str) -> PyResult<Py<PyAny>> {
        let widths = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let mut values = windows.chan_width(spw_row(&windows, spw)?)?;
            convert_units(
                &mut values,
                column_unit(&windows, "CHAN_WIDTH")?,
                unit,
                true,
            )?;
            Ok(values)
        })?;
        Ok(widths.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (spw, unit = "", asvel = false), text_signature = "($self, spw, unit='', asvel=False)")]
    fn chaneffbws(&self, py: Python<'_>, spw: i64, unit: &str, asvel: bool) -> PyResult<Py<PyAny>> {
        let values = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let row = spw_row(&windows, spw)?;
            channel_resolution(
                &windows,
                row,
                "EFFECTIVE_BW",
                windows.effective_bw(row)?,
                unit,
                asvel,
            )
        })?;
        Ok(values.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (spw, unit = "", asvel = false), text_signature = "($self, spw, unit='', asvel=False)")]
    fn chanres(&self, py: Python<'_>, spw: i64, unit: &str, asvel: bool) -> PyResult<Py<PyAny>> {
        let values = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let row = spw_row(&windows, spw)?;
            channel_resolution(
                &windows,
                row,
                "RESOLUTION",
                windows.resolution(row)?,
                unit,
                asvel,
            )
        })?;
        Ok(values.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (spw, unit = "Hz"), text_signature = "($self, spw, unit='Hz')")]
    fn meanfreq(&self, py: Python<'_>, spw: i64, unit: &str) -> PyResult<f64> {
        self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let mut mean = mean_frequency(&windows, spw_row(&windows, spw)?)?;
            convert_units(
                std::slice::from_mut(&mut mean),
                column_unit(&windows, "CHAN_FREQ")?,
                unit,
                false,
            )?;
            Ok(mean)
        })
    }

    #[pyo3(signature = (spw), text_signature = "($self, spw)")]
    fn sideband(&self, py: Python<'_>, spw: i64) -> PyResult<i32> {
        self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            Ok(if windows.net_sideband(spw_row(&windows, spw)?)? == 2 {
                1
            } else {
                -1
            })
        })
    }

    #[pyo3(signature = (spw = -1), text_signature = "($self, spw=-1)")]
    fn reffreq(&self, py: Python<'_>, spw: i64) -> PyResult<Py<PyAny>> {
        let frequency = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let row = spw_row(&windows, spw)?;
            let frequency = ChanFreqColumn::new(windows.table()).get_ref_frequency(row)?;
            Ok(frequency_to_record(&frequency))
        })?;
        CasaValue::from_record(&frequency).into_python(py)
    }
}
