// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's read-only calibration table analysis tool.

pub(crate) mod surface;

use std::sync::{Mutex, PoisonError};

use casa_calibration::{CalAnalysis, CalAnalysisIds::*};
use numpy::PyArray1;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::tools::{LogLine, emit, not_yet_implemented_in};

const LOGGER: &str = "casars.tools.calanalysis";

#[pyclass(name = "calanalysis", module = "casars.tools", frozen)]
#[derive(Default)]
pub(crate) struct PyCalAnalysis {
    attachment: Mutex<Option<CalAnalysis>>,
}

#[pymethods]
impl PyCalAnalysis {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    #[pyo3(signature = (caltable = ""))]
    fn open(&self, py: Python<'_>, caltable: &str) -> PyResult<bool> {
        let outcome = py.detach(|| {
            let mut attachment = self
                .attachment
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(table) = &*attachment {
                return Err(format!("File {} is already open", table.name));
            }
            *attachment = Some(CalAnalysis::open(caltable.into())?);
            Ok(())
        });
        match outcome {
            Ok(()) => Ok(true),
            Err(error) => {
                emit(py, LOGGER, &[LogLine::warning(error)])?;
                Ok(false)
            }
        }
    }

    fn close(&self, py: Python<'_>) -> bool {
        py.detach(|| {
            self.attachment
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .is_some()
        })
    }

    fn calname(&self, py: Python<'_>) -> PyResult<String> {
        self.metadata(py, |table| Ok(table.name.clone()))
    }

    fn msname(&self, py: Python<'_>) -> PyResult<String> {
        self.metadata(py, |table| Ok(table.ms_name.clone()))
    }

    fn viscal(&self, py: Python<'_>) -> PyResult<String> {
        self.metadata(py, |table| Ok(table.vis_cal.clone()))
    }

    fn partype(&self, py: Python<'_>) -> PyResult<String> {
        self.metadata(py, |table| Ok(table.parameter_type.clone()))
    }

    fn polbasis(&self, py: Python<'_>) -> PyResult<String> {
        self.metadata(py, |table| Ok(table.polarization_basis.clone()))
    }

    fn numfield(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.count(Fields)))
    }

    #[pyo3(signature = (name = true))]
    fn field(&self, py: Python<'_>, name: bool) -> PyResult<Vec<String>> {
        self.metadata(py, |table| table.labels(Fields, name))
    }

    fn numantenna(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.count(Antennas)))
    }

    #[pyo3(signature = (name = true))]
    fn antenna(&self, py: Python<'_>, name: bool) -> PyResult<Vec<String>> {
        self.metadata(py, |table| table.labels(Antennas, name))
    }

    fn numantenna1(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.count(FirstAntennas)))
    }

    #[pyo3(signature = (name = true))]
    fn antenna1(&self, py: Python<'_>, name: bool) -> PyResult<Vec<String>> {
        self.metadata(py, |table| table.labels(FirstAntennas, name))
    }

    fn numantenna2(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.count(SecondAntennas)))
    }

    #[pyo3(signature = (name = true))]
    fn antenna2(&self, py: Python<'_>, name: bool) -> PyResult<Vec<String>> {
        self.metadata(py, |table| table.labels(SecondAntennas, name))
    }

    fn numfeed(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.feeds.len()))
    }

    fn feed(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.metadata(py, |table| Ok(table.feeds.clone()))
    }

    fn numtime(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.times.len()))
    }

    fn time<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<f64>>> {
        let times = self.metadata(py, |table| Ok(table.times.clone()))?;
        Ok(PyArray1::from_vec(py, times))
    }

    fn numspw(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.count(SpectralWindows)))
    }

    #[pyo3(signature = (name = true))]
    fn spw(&self, py: Python<'_>, name: bool) -> PyResult<Vec<String>> {
        self.metadata(py, |table| table.labels(SpectralWindows, name))
    }

    fn numchannel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<i64>>> {
        let channels = self.metadata(py, |table| Ok(table.channel_counts.clone()))?;
        Ok(PyArray1::from_vec(py, channels))
    }

    fn freq<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let frequencies = self.metadata(py, |table| Ok(table.frequencies.clone()))?;
        let record = PyDict::new(py);
        for (index, frequency) in frequencies.into_iter().enumerate() {
            record.set_item(index.to_string(), PyArray1::from_vec(py, frequency))?;
        }
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (field = None, antenna = None, timerange = None, spw = None, feed = None, axis = "TIME", ap = "AMPLITUDE", norm = false, unwrap = false, jumpmax = 0.), text_signature = "($self, field=[], antenna=[], timerange=[], spw=[], feed=[], axis='TIME', ap='AMPLITUDE', norm=False, unwrap=False, jumpmax=0.0)")]
    fn get(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        antenna: Option<&Bound<'_, PyAny>>,
        timerange: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        feed: Option<&Bound<'_, PyAny>>,
        axis: &str,
        ap: &str,
        norm: bool,
        unwrap: bool,
        jumpmax: f64,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            field, antenna, timerange, spw, feed, axis, ap, norm, unwrap, jumpmax,
        );
        not_yet_implemented_in("calanalysis", "get", 1529)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (field = None, antenna = None, timerange = None, spw = None, feed = None, axis = "TIME", ap = "AMPLITUDE", norm = false, unwrap = false, jumpmax = 0., order = "AVERAGE", r#type = "LSQ", weight = false), text_signature = "($self, field=[], antenna=[], timerange=[], spw=[], feed=[], axis='TIME', ap='AMPLITUDE', norm=False, unwrap=False, jumpmax=0.0, order='AVERAGE', type='LSQ', weight=False)")]
    fn fit(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        antenna: Option<&Bound<'_, PyAny>>,
        timerange: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        feed: Option<&Bound<'_, PyAny>>,
        axis: &str,
        ap: &str,
        norm: bool,
        unwrap: bool,
        jumpmax: f64,
        order: &str,
        r#type: &str,
        weight: bool,
    ) -> PyResult<Py<PyAny>> {
        let _ = (
            field, antenna, timerange, spw, feed, axis, ap, norm, unwrap, jumpmax, order, r#type,
            weight,
        );
        not_yet_implemented_in("calanalysis", "fit", 1529)
    }
}

impl PyCalAnalysis {
    fn metadata<T: Default + Send>(
        &self,
        py: Python<'_>,
        read: impl FnOnce(&CalAnalysis) -> Result<T, String> + Send,
    ) -> PyResult<T> {
        let result = py.detach(|| {
            self.attachment
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .map(read)
        });
        match result {
            Some(result) => result.map_err(PyRuntimeError::new_err),
            None => {
                emit(py, LOGGER, &[LogLine::warning("Open caltable first")])?;
                Ok(T::default())
            }
        }
    }
}
