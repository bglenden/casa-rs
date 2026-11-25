// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel, polarization, and visibility reads on the legacy selector view.

use casa_coordinates::StokesType;
use casa_ms::tool_visibility::{
    LegacyVisibilityAxes, VisibilityChannels, VisibilityPolarization, VisibilityReadOptions,
};
use pyo3::prelude::*;

use super::legacy_selection::deprecation;
use super::metadata_link::compose_rows;
use super::visibility::{channel_argument, integer_argument, requested_strings, sampled_rows};
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};
use crate::convert::value::CasaValue;

#[derive(Default)]
pub(super) struct LegacyAxes {
    channels: Option<VisibilityChannels>,
    polarization: Option<VisibilityPolarization>,
    labels: Option<Vec<String>>,
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (items = None, ifraxis = false, ifraxisgap = 0, increment = 1, average = false), text_signature = "($self, items=[], ifraxis=False, ifraxisgap=0, increment=1, average=False)")]
    fn getdataold(
        &self,
        py: Python<'_>,
        items: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = strict_bool)] ifraxis: bool,
        #[pyo3(from_py_with = gap_argument)] ifraxisgap: i64,
        #[pyo3(from_py_with = increment_argument)] increment: i64,
        #[pyo3(from_py_with = strict_bool)] average: bool,
    ) -> PyResult<Py<PyAny>> {
        let items = requested_strings(items, "items")?;
        deprecation(py, "getdataold", "getdata")?;
        let options = VisibilityReadOptions {
            average,
            baseline_gap: ifraxis.then_some(ifraxisgap as i32),
            ..VisibilityReadOptions::default()
        };
        let result = py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(CasaValue::empty_record());
            };
            ms.read_legacy_visibility(&items, increment, options)
        })?;
        result.into_python(py)
    }

    #[pyo3(signature = (nchan = 1, start = 0, width = 1, inc = 1), text_signature = "($self, nchan=1, start=0, width=1, inc=1)")]
    fn selectchannelold(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = nchan_argument)] nchan: i64,
        #[pyo3(from_py_with = start_argument)] start: i64,
        #[pyo3(from_py_with = width_argument)] width: i64,
        #[pyo3(from_py_with = channel_increment)] inc: i64,
    ) -> PyResult<bool> {
        deprecation(py, "selectchannelold", "selectchannel")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(false);
            };
            if !ms.prepare_legacy_filter()? {
                return Ok(false);
            }
            ms.working_metadata_link()
                .verify_directory()
                .map_err(runtime_error)?;
            let channels = ms.legacy_selection.channel_count();
            let [nchan, start, width, inc] =
                [nchan, start, width, inc].map(|value| i64::from(value as i32));
            let Some(selection) = VisibilityChannels::new(nchan, start, width, inc, channels)
            else {
                return Ok(false);
            };
            ms.legacy_selection.axes.channels = Some(selection);
            Ok(true)
        })
    }

    #[pyo3(signature = (wantedpol = None), text_signature = "($self, wantedpol=[])")]
    fn selectpolarizationold(
        &self,
        py: Python<'_>,
        wantedpol: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let wanted = requested_strings(wantedpol, "wantedpol")?;
        deprecation(py, "selectpolarizationold", "selectpolarization")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(false);
            };
            ms.initialize_legacy_selection(None)?;
            if !ms.prepare_legacy_filter()? {
                return Ok(false);
            }
            let mut types = Vec::with_capacity(wanted.len());
            for name in &wanted {
                let Some(kind) =
                    StokesType::from_name(name).filter(|&kind| kind != StokesType::Undefined)
                else {
                    return Ok(false);
                };
                if types.contains(&kind) {
                    return Ok(false);
                }
                types.push(kind);
            }
            if !types.is_empty() {
                let (description_count, codes) = ms.legacy_correlation_layout()?;
                ms.legacy_selection.axes.polarization = Some(
                    VisibilityPolarization::legacy_selected(description_count, &codes, types),
                );
            }
            ms.legacy_selection.axes.labels = Some(wanted);
            Ok(true)
        })
    }
}

fn nchan_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "nchan", "ms_selectchannelold")
}

fn start_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "start", "ms_selectchannelold")
}

fn width_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "width", "ms_selectchannelold")
}

fn channel_increment(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    channel_argument(value, "inc", "ms_selectchannelold")
}

fn gap_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    integer_argument(value, "ifraxisgap", "ms_getdataold")
}

fn increment_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    integer_argument(value, "increment", "ms_getdataold")
}

impl AttachedMs {
    fn read_legacy_visibility(
        &mut self,
        items: &[String],
        increment: i64,
        options: VisibilityReadOptions<'_>,
    ) -> PyResult<CasaValue> {
        if !self.prepare_legacy_filter()? || items.is_empty() {
            return Ok(CasaValue::empty_record());
        }
        let link = self.working_metadata_link();
        link.verify_directory().map_err(runtime_error)?;
        let base = link.selected_rows().map_err(runtime_error)?;
        let rows = compose_rows(base.as_deref(), self.legacy_selection.rows());
        let count = if let Some(rows) = &rows {
            rows.len()
        } else {
            link.row_count().map_err(runtime_error)?
        };
        let stride = sampled_rows(count, count, i64::from(increment as i32))?;
        let projected = compose_rows(rows.as_deref(), Some(&stride));
        let axes = &self.legacy_selection.axes;
        let snapshot = items
            .iter()
            .any(|name| name.eq_ignore_ascii_case("axis_info"))
            .then(|| link.snapshot(None))
            .transpose()
            .map_err(runtime_error)?;
        let main = self.visibility.main(&self.path)?;
        let options = VisibilityReadOptions {
            baseline_pairs: self.legacy_selection.baselines.as_deref(),
            channels: axes.channels,
            polarization: axes.polarization.as_ref(),
            legacy_axes: Some(LegacyVisibilityAxes {
                descriptions: self.legacy_selection.descriptions(),
                correlation_labels: axes.labels.as_deref(),
            }),
            ..options
        };
        let record = main.read_rows(
            projected.as_deref().expect("sampled rows are present"),
            items,
            snapshot.as_ref().map(|snapshot| &snapshot.ms),
            options,
        )?;
        link.verify_directory().map_err(runtime_error)?;
        Ok(record)
    }
}
