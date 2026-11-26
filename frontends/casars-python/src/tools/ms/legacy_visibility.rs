// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel, polarization, and visibility reads on the legacy selector view.

use std::collections::BTreeSet;
use std::sync::Arc;

use casa_coordinates::StokesType;
use casa_ms::tool_visibility::{
    LegacyVisibilityAxes, LegacyVisibilityWriteGrid, VisibilityChannels, VisibilityPolarization,
    VisibilityReadOptions,
};
use pyo3::prelude::*;

use super::legacy_selection::{LegacySelection, deprecation};
use super::metadata_link::compose_rows;
use super::visibility::{
    VisibilityMain, channel_argument, integer_argument, requested_strings, sampled_rows,
};
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};
use crate::convert::value::CasaValue;

#[derive(Default)]
pub(super) struct LegacyAxes {
    pub(super) channels: Option<VisibilityChannels>,
    pub(super) polarization: Option<Arc<VisibilityPolarization>>,
    pub(super) noncanonical_complex_polarization: bool,
    labels: Option<Vec<String>>,
    pub(super) baseline_read: Option<LegacyBaselineRead>,
}

impl LegacySelection {
    pub(super) fn visibility_options(&self) -> VisibilityReadOptions<'_> {
        VisibilityReadOptions {
            baseline_pairs: self.baselines.as_deref(),
            channels: self.axes.channels,
            polarization: self.axes.polarization.as_deref(),
            legacy_axes: Some(LegacyVisibilityAxes {
                descriptions: self.descriptions(),
                correlation_labels: self.axes.labels.as_deref(),
            }),
            ..VisibilityReadOptions::default()
        }
    }
}

pub(super) struct LegacyBaselineRead {
    main: Arc<VisibilityMain>,
    working_rows: Option<Arc<[usize]>>,
    selected_rows: Option<Arc<[usize]>>,
    baselines: Option<Vec<(i32, i32)>>,
    rows: Vec<usize>,
    gap: i32,
}

impl LegacyBaselineRead {
    pub(super) fn has_current_descriptor(&self) -> bool {
        self.main.verify_write_layout(&BTreeSet::new()).is_ok()
    }

    pub(super) fn refresh_descriptor(&mut self, main: Arc<VisibilityMain>) {
        self.main = main;
    }

    pub(super) fn write_grid(
        &self,
        working_rows: Option<&[usize]>,
        selection: &LegacySelection,
    ) -> Option<LegacyVisibilityWriteGrid<'_>> {
        (self.working_rows.as_deref() == working_rows
            && self.selected_rows.as_deref() == selection.rows()
            && self.baselines.as_deref() == selection.baselines.as_deref()
            && self.has_current_descriptor())
        .then_some(LegacyVisibilityWriteGrid {
            rows: &self.rows,
            baseline_pairs: self.baselines.as_deref(),
            gap: self.gap,
        })
    }
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
                let polarization =
                    VisibilityPolarization::legacy_selected(description_count, &codes, types);
                ms.legacy_selection.axes.noncanonical_complex_polarization =
                    polarization.legacy_noncanonical_complex_write(codes.len());
                ms.legacy_selection.axes.polarization = Some(Arc::new(polarization));
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
        let projected = match rows {
            Some(rows) => stride.into_iter().map(|row| rows[row]).collect(),
            None => stride,
        };
        let positioned = self.legacy_selection.positioned_iteration();
        let snapshot = if items
            .iter()
            .any(|name| name.eq_ignore_ascii_case("axis_info"))
        {
            if let Some(snapshot) = positioned.and_then(|iteration| iteration.metadata.as_ref()) {
                Some(Arc::clone(snapshot))
            } else {
                Some(link.snapshot(None).map_err(runtime_error)?)
            }
        } else {
            None
        };
        let main = if let Some(iteration) = positioned {
            iteration.main.verify_iterator_layout()?;
            Arc::clone(&iteration.main)
        } else {
            self.visibility.current_main(&self.path)?
        };
        let options = VisibilityReadOptions {
            average: options.average,
            baseline_gap: options.baseline_gap,
            ..self.legacy_selection.visibility_options()
        };
        let record = main.read_rows(
            &projected,
            items,
            snapshot.as_ref().map(|snapshot| &snapshot.ms),
            options,
        )?;
        link.verify_directory().map_err(runtime_error)?;
        self.legacy_selection.axes.baseline_read = options
            .baseline_gap
            .filter(|_| !options.average)
            .map(|gap| LegacyBaselineRead {
                main,
                working_rows: base,
                selected_rows: self.legacy_selection.shared_rows(),
                baselines: self.legacy_selection.baselines.clone(),
                rows: projected,
                gap,
            });
        Ok(record)
    }
}
