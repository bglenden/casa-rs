// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's read-only calibration table analysis tool.

pub(crate) mod surface;

use std::sync::{Mutex, PoisonError};

use casa_calibration::{CalAnalysis, CalAnalysisIds::*};
use numpy::PyArray1;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict};

use crate::tools::{LogLine, emit, not_yet_implemented_in};

const LOGGER: &str = "casars.tools.calanalysis";

struct TableName(Option<String>);

impl<'a, 'py> FromPyObject<'a, 'py> for TableName {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(Self(value.extract().ok()))
    }
}

struct NameArgument(Option<bool>);

impl<'a, 'py> FromPyObject<'a, 'py> for NameArgument {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let name = value.cast::<PyBool>().ok().map(|flag| flag.is_true());
        Ok(Self(name))
    }
}

impl NameArgument {
    fn value(self, method: &str) -> PyResult<bool> {
        self.0.ok_or_else(|| {
            PyTypeError::new_err(format!(
                "in method 'calanalysis_{method}', argument 2 of type 'bool'"
            ))
        })
    }
}

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

    #[pyo3(signature = (caltable = TableName(Some(String::new()))), text_signature = "($self, caltable='')")]
    fn open(&self, py: Python<'_>, caltable: TableName) -> PyResult<bool> {
        let caltable = caltable
            .0
            .ok_or_else(|| PyTypeError::new_err("argument _caltable must be a string"))?;
        let outcome = py.detach(|| {
            let mut attachment = self
                .attachment
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(table) = &*attachment {
                return Err(format!("File {} is already open", table.name));
            }
            *attachment = Some(CalAnalysis::open(caltable)?);
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

    #[pyo3(signature = (name = NameArgument(Some(true))), text_signature = "($self, name=True)")]
    fn field(&self, py: Python<'_>, name: NameArgument) -> PyResult<Vec<String>> {
        let name = name.value("field")?;
        self.metadata(py, |table| table.labels(Fields, name))
    }

    fn numantenna(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.count(Antennas)))
    }

    #[pyo3(signature = (name = NameArgument(Some(true))), text_signature = "($self, name=True)")]
    fn antenna(&self, py: Python<'_>, name: NameArgument) -> PyResult<Vec<String>> {
        let name = name.value("antenna")?;
        self.metadata(py, |table| table.labels(Antennas, name))
    }

    fn numantenna1(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.count(FirstAntennas)))
    }

    #[pyo3(signature = (name = NameArgument(Some(true))), text_signature = "($self, name=True)")]
    fn antenna1(&self, py: Python<'_>, name: NameArgument) -> PyResult<Vec<String>> {
        let name = name.value("antenna1")?;
        self.metadata(py, |table| table.labels(FirstAntennas, name))
    }

    fn numantenna2(&self, py: Python<'_>) -> PyResult<usize> {
        self.metadata(py, |table| Ok(table.count(SecondAntennas)))
    }

    #[pyo3(signature = (name = NameArgument(Some(true))), text_signature = "($self, name=True)")]
    fn antenna2(&self, py: Python<'_>, name: NameArgument) -> PyResult<Vec<String>> {
        let name = name.value("antenna2")?;
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

    #[pyo3(signature = (name = NameArgument(Some(true))), text_signature = "($self, name=True)")]
    fn spw(&self, py: Python<'_>, name: NameArgument) -> PyResult<Vec<String>> {
        let name = name.value("spw")?;
        self.metadata(py, |table| table.labels(SpectralWindows, name))
    }

    fn numchannel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyArray1<i64>>> {
        let channels = self.metadata(py, |table| Ok(table.channel_counts.clone()))?;
        Ok(PyArray1::from_vec(py, channels))
    }

    fn freq<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let frequencies = self.metadata(py, |table| Ok(table.frequencies.clone()))?;
        let mut entries = frequencies
            .into_iter()
            .enumerate()
            .map(|(index, frequency)| (index.to_string(), frequency))
            .collect::<Vec<_>>();
        // CASA converts its record with the decimal keys in lexical order.
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let record = PyDict::new(py);
        for (key, frequency) in entries {
            record.set_item(key, PyArray1::from_vec(py, frequency))?;
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
