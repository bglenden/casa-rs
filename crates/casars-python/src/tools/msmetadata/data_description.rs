// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;

use casa_ms::{MsDataDescription, MsError, MsResult};
use casa_types::ArrayValue;
use numpy::IntoPyArray;
use pyo3::prelude::*;

use super::PyMsMetadata;
use crate::convert::value::CasaValue;

fn checked_id(id: i64, count: usize, name: &str) -> MsResult<usize> {
    usize::try_from(id)
        .ok()
        .filter(|&row| row < count)
        .ok_or_else(|| MsError::InvalidInput(format!("{name} {id} out of range (size {count})")))
}

impl PyMsMetadata {
    fn data_description_value(
        &self,
        py: Python<'_>,
        ddid: i64,
        value: impl Fn(&MsDataDescription<'_>, usize) -> MsResult<i32> + Send,
    ) -> PyResult<Py<PyAny>> {
        if ddid >= 0 {
            let id = self.with_ms(py, move |ms| {
                let descriptions = ms.data_description()?;
                let row = checked_id(ddid, descriptions.row_count(), "Data description ID")?;
                Ok(i64::from(value(&descriptions, row)? as u32))
            })?;
            CasaValue::Int(id).into_python(py)
        } else {
            let ids = self.with_ms(py, move |ms| {
                let descriptions = ms.data_description()?;
                (0..descriptions.row_count())
                    .map(|row| value(&descriptions, row).map(|id| u64::from(id as u32)))
                    .collect::<MsResult<Vec<_>>>()
            })?;
            Ok(ids.into_pyarray(py).into_any().unbind())
        }
    }
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (spw = -1, pol = -1), text_signature = "($self, spw=-1, pol=-1)")]
    fn datadescids(&self, py: Python<'_>, spw: i64, pol: i64) -> PyResult<Py<PyAny>> {
        let ids = self.with_ms(py, |ms| {
            if spw >= 0 {
                checked_id(spw, ms.spectral_window()?.row_count(), "Spectral window ID")?;
            }
            if pol >= 0 {
                checked_id(pol, ms.polarization()?.row_count(), "Polarization ID")?;
            }
            let descriptions = ms.data_description()?;
            let mut latest_rows = BTreeMap::new();
            for row in 0..descriptions.row_count() {
                let window = i64::from(descriptions.spectral_window_id(row)?);
                let polarization = i64::from(descriptions.polarization_id(row)?);
                if (spw < 0 || spw == window) && (pol < 0 || pol == polarization) {
                    latest_rows.insert((window, polarization), row as i64);
                }
            }
            let mut ids: Vec<_> = latest_rows.into_values().collect();
            ids.sort_unstable();
            Ok(ids)
        })?;
        Ok(ids.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (ddid = -1), text_signature = "($self, ddid=-1)")]
    fn spwfordatadesc(&self, py: Python<'_>, ddid: i64) -> PyResult<Py<PyAny>> {
        self.data_description_value(py, ddid, |description, row| {
            description.spectral_window_id(row)
        })
    }

    #[pyo3(signature = (ddid = -1), text_signature = "($self, ddid=-1)")]
    fn polidfordatadesc(&self, py: Python<'_>, ddid: i64) -> PyResult<Py<PyAny>> {
        self.data_description_value(py, ddid, |description, row| {
            description.polarization_id(row)
        })
    }

    #[pyo3(signature = (polid = -1), text_signature = "($self, polid=-1)")]
    fn ncorrforpol(&self, py: Python<'_>, polid: i64) -> PyResult<Py<PyAny>> {
        let value = self.with_ms(py, |ms| {
            let polarizations = ms.polarization()?;
            let count = polarizations.row_count();
            if polid >= 0 {
                let row = checked_id(polid, count, "Polarization ID")?;
                Ok(CasaValue::Int(i64::from(polarizations.num_corr(row)?)))
            } else {
                let values = (0..count)
                    .map(|row| polarizations.num_corr(row).map(i64::from))
                    .collect::<MsResult<Vec<_>>>()?;
                Ok(CasaValue::Ints {
                    shape: vec![values.len()],
                    values,
                })
            }
        })?;
        value.into_python(py)
    }

    #[pyo3(signature = (pol = -1), text_signature = "($self, pol=-1)")]
    fn corrtypesforpol(&self, py: Python<'_>, pol: i64) -> PyResult<Py<PyAny>> {
        let values = self.with_ms(py, |ms| {
            let polarizations = ms.polarization()?;
            let row = checked_id(pol, polarizations.row_count(), "Polarization ID")?;
            Ok(polarizations
                .corr_type(row)?
                .into_iter()
                .map(i64::from)
                .collect::<Vec<_>>())
        })?;
        Ok(values.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (pol = -1), text_signature = "($self, pol=-1)")]
    fn corrprodsforpol(&self, py: Python<'_>, pol: i64) -> PyResult<Py<PyAny>> {
        let result = self.with_ms(py, |ms| {
            let polarizations = ms.polarization()?;
            let row = checked_id(pol, polarizations.row_count(), "Polarization ID")?;
            let ArrayValue::Int32(values) = polarizations.corr_product(row)? else {
                return Err(MsError::InvalidInput(
                    "CORR_PRODUCT is not an Int32 array".into(),
                ));
            };
            Ok(CasaValue::Ints {
                shape: values.shape().to_vec(),
                values: values
                    .view()
                    .reversed_axes()
                    .iter()
                    .copied()
                    .map(i64::from)
                    .collect(),
            })
        })?;
        result.into_python(py)
    }
}
