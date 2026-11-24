// SPDX-License-Identifier: LGPL-3.0-or-later
//! Statistical weight estimation on the attached MeasurementSet.

use casa_calibration::{
    StatWtAlgorithm, StatWtChannelBin, StatWtCombine, StatWtDataColumn, StatWtMs, StatWtOptions,
    StatWtTimeBin, estimate_statwt, statwt_weight_range,
};
use casa_ms::MsSelection;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyDictMethods, PyList, PyListMethods, PyString};

use super::{PyMsTool, runtime_error, strict_bool};

#[pymethods]
impl PyMsTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (combine = "", timebin = None, slidetimebin = false, chanbin = None, minsamp = 2,
        statalg = "classic", fence = -1.0, center = "mean", lside = true, zscore = -1.0, maxiter = -1,
        fitspw = "", excludechans = false, wtrange = None, preview = false, datacolumn = "corrected"),
        text_signature = "($self, combine='', timebin=1, slidetimebin=False, chanbin='spw', minsamp=2, statalg='classic', fence=-1.0, center='mean', lside=True, zscore=-1.0, maxiter=-1, fitspw='', excludechans=False, wtrange=[], preview=False, datacolumn='corrected')")]
    fn statwt<'py>(
        &self,
        py: Python<'py>,
        combine: &str,
        timebin: Option<&Bound<'py, PyAny>>,
        #[pyo3(from_py_with = strict_bool)] slidetimebin: bool,
        chanbin: Option<&Bound<'py, PyAny>>,
        minsamp: i64,
        statalg: &str,
        fence: f64,
        center: &str,
        #[pyo3(from_py_with = strict_bool)] lside: bool,
        zscore: f64,
        maxiter: i64,
        fitspw: &str,
        #[pyo3(from_py_with = strict_bool)] excludechans: bool,
        wtrange: Option<Vec<f64>>,
        #[pyo3(from_py_with = strict_bool)] preview: bool,
        datacolumn: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        let options = StatWtOptions {
            data_column: StatWtDataColumn::parse(datacolumn).map_err(runtime_error)?,
            algorithm: StatWtAlgorithm::parse(statalg, fence, center, lside, zscore, maxiter)
                .map_err(runtime_error)?,
            minimum_samples: usize::try_from(minsamp).unwrap_or(0),
            combine: StatWtCombine::parse(combine),
            time_bin: read_bin(
                timebin,
                StatWtTimeBin::Timestamps(1),
                StatWtTimeBin::Timestamps,
                StatWtTimeBin::duration,
            )?,
            sliding: slidetimebin,
            channel_bin: read_bin(
                chanbin,
                StatWtChannelBin::Window,
                StatWtChannelBin::Channels,
                StatWtChannelBin::parse,
            )?,
            fit_spw: fitspw.to_owned(),
            exclude_channels: excludechans,
            weight_range: statwt_weight_range(wtrange.as_deref().unwrap_or_default())
                .map_err(runtime_error)?,
        };
        let report = py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(attached) = attached.as_mut() else {
                return Ok(None);
            };
            if !preview && !attached.writable {
                return Err(runtime_error("MeasurementSet is not writable"));
            }
            let mut transient = None;
            let locks = if preview {
                None
            } else if !attached.write_locks.is_empty() {
                Some(&mut attached.write_locks)
            } else {
                Some(
                    transient
                        .insert(StatWtMs::write_locks(&attached.sources).map_err(runtime_error)?),
                )
            };
            let mut ms = StatWtMs::open(&attached.path, false).map_err(runtime_error)?;
            let mut selected = ms.select(&MsSelection::default()).map_err(runtime_error)?;
            if let Some(rows) = &attached.selected_rows {
                selected.retain(|row| rows.binary_search(&row.row_index).is_ok());
            }
            let report =
                estimate_statwt(&mut ms, &selected, &options, locks).map_err(runtime_error)?;
            drop(ms);
            if let Some(locks) = transient {
                for lock in locks.into_values() {
                    lock.release().map_err(runtime_error)?;
                }
            }
            Ok(Some(report))
        })?;
        let result = PyDict::new(py);
        if let Some(report) = report {
            result.set_item("mean", report.mean)?;
            result.set_item("variance", report.variance)?;
        }
        Ok(result)
    }
}

fn read_bin<T>(
    value: Option<&Bound<'_, PyAny>>,
    default: T,
    count: impl FnOnce(usize) -> T,
    quantity: impl FnOnce(&str) -> casa_ms::MsResult<T>,
) -> PyResult<T> {
    let Some(value) = value else {
        return Ok(default);
    };
    if let Ok(text) = value.cast::<PyString>() {
        return quantity(text.to_str()?).map_err(runtime_error);
    }
    if let Ok(list) = value.cast::<PyList>()
        && list.is_empty()
    {
        return Ok(default);
    }
    if !value.is_instance_of::<PyBool>()
        && let Ok(size) = value.extract::<usize>()
    {
        return Ok(count(size));
    }
    Err(runtime_error(
        "statwt bin requires an integer count or a string quantity",
    ))
}
