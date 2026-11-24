// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::MsSelection;
use casa_ms::tool_statistics::{ToolStatisticsOptions, tool_statistics};
use pyo3::prelude::*;

use super::owned_record::visibility_record;
use super::selection_indices::selection_text;
use super::{PyMsTool, runtime_error, strict_bool};
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

#[derive(Default)]
struct StatisticsText(String);

impl<'a, 'py> FromPyObject<'a, 'py> for StatisticsText {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        selection_text(&value, "statistics").map(Self)
    }
}

impl StatisticsText {
    fn selector(self) -> Option<String> {
        (!self.0.is_empty()).then_some(self.0)
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyMsTool {
    #[pyo3(
        signature = (
            column = StatisticsText::default(), complex_value = StatisticsText::default(),
            useflags = true, useweights = false, spw = StatisticsText::default(),
            field = StatisticsText::default(), baseline = StatisticsText::default(),
            uvrange = StatisticsText::default(), time = StatisticsText::default(),
            correlation = StatisticsText::default(), scan = StatisticsText::default(),
            intent = StatisticsText::default(), array = StatisticsText::default(),
            obs = StatisticsText::default(), reportingaxes = StatisticsText::default(),
            timeaverage = false, timebin = StatisticsText("0s".into()),
            timespan = StatisticsText::default(), maxuvwdistance = 0.0, doquantiles = true
        ),
        text_signature = "($self, column='', complex_value='', useflags=True, useweights=False, spw='', field='', baseline='', uvrange='', time='', correlation='', scan='', intent='', array='', obs='', reportingaxes='', timeaverage=False, timebin='0s', timespan='', maxuvwdistance=0.0, doquantiles=True)"
    )]
    fn statistics(
        &self,
        py: Python<'_>,
        column: StatisticsText,
        complex_value: StatisticsText,
        #[pyo3(from_py_with = strict_bool)] useflags: bool,
        #[pyo3(from_py_with = strict_bool)] useweights: bool,
        spw: StatisticsText,
        field: StatisticsText,
        baseline: StatisticsText,
        uvrange: StatisticsText,
        time: StatisticsText,
        correlation: StatisticsText,
        scan: StatisticsText,
        intent: StatisticsText,
        array: StatisticsText,
        obs: StatisticsText,
        reportingaxes: StatisticsText,
        #[pyo3(from_py_with = strict_bool)] timeaverage: bool,
        timebin: StatisticsText,
        timespan: StatisticsText,
        maxuvwdistance: f64,
        #[pyo3(from_py_with = strict_bool)] doquantiles: bool,
    ) -> PyResult<Py<PyAny>> {
        let selection = MsSelection {
            spw: spw.selector(),
            field: field.selector(),
            antenna: baseline.selector(),
            uvrange: uvrange.selector(),
            timerange: time.selector(),
            correlation: correlation.selector(),
            scan: scan.selector(),
            intent: intent.selector(),
            array: array.selector(),
            observation: obs.selector(),
            ..Default::default()
        };
        self.reduce_statistics(
            py,
            ToolStatisticsOptions {
                column: column.0,
                complex_value: complex_value.0,
                use_flags: useflags,
                use_weights: useweights,
                selection,
                reporting_axes: reportingaxes.0,
                time_average: timeaverage,
                time_bin: timebin.0,
                time_span: timespan.0,
                max_uvw_distance: maxuvwdistance,
                quantiles: doquantiles,
                legacy: false,
            },
        )
    }

    #[pyo3(
        signature = (
            column = StatisticsText::default(), complex_value = StatisticsText::default(),
            useflags = true, spw = StatisticsText::default(), field = StatisticsText::default(),
            baseline = StatisticsText::default(), uvrange = StatisticsText::default(),
            time = StatisticsText::default(), correlation = StatisticsText::default(),
            scan = StatisticsText::default(), array = StatisticsText::default(),
            obs = StatisticsText::default()
        ),
        text_signature = "($self, column='', complex_value='', useflags=True, spw='', field='', baseline='', uvrange='', time='', correlation='', scan='', array='', obs='')"
    )]
    fn statisticsold(
        &self,
        py: Python<'_>,
        column: StatisticsText,
        complex_value: StatisticsText,
        #[pyo3(from_py_with = strict_bool)] useflags: bool,
        spw: StatisticsText,
        field: StatisticsText,
        baseline: StatisticsText,
        uvrange: StatisticsText,
        time: StatisticsText,
        correlation: StatisticsText,
        scan: StatisticsText,
        array: StatisticsText,
        obs: StatisticsText,
    ) -> PyResult<Py<PyAny>> {
        let selection = MsSelection {
            spw: spw.selector(),
            field: field.selector(),
            antenna: baseline.selector(),
            uvrange: uvrange.selector(),
            timerange: time.selector(),
            correlation: correlation.selector(),
            scan: scan.selector(),
            array: array.selector(),
            observation: obs.selector(),
            ..Default::default()
        };
        emit(
            py,
            "casars.tools.ms",
            &[LogLine::warning(
                "The use of ms::statisticsold() is deprecated. Use ms::statistics() instead.",
            )],
        )?;
        self.reduce_statistics(
            py,
            ToolStatisticsOptions {
                column: column.0,
                complex_value: complex_value.0,
                use_flags: useflags,
                use_weights: false,
                selection,
                reporting_axes: String::new(),
                time_average: false,
                time_bin: String::new(),
                time_span: String::new(),
                max_uvw_distance: 0.0,
                quantiles: true,
                legacy: true,
            },
        )
    }
}

impl PyMsTool {
    fn reduce_statistics(
        &self,
        py: Python<'_>,
        options: ToolStatisticsOptions,
    ) -> PyResult<Py<PyAny>> {
        let result = self.attachment_info(py, move |attachment| {
            let Some(attachment) = attachment else {
                return Ok(CasaValue::empty_record());
            };
            let link = attachment.working_metadata_link();
            let snapshot = link.snapshot(None).map_err(runtime_error)?;
            let record = tool_statistics(&snapshot.ms, &options).map_err(runtime_error)?;
            link.verify_directory().map_err(runtime_error)?;
            Ok(visibility_record(record))
        })?;
        result.into_python(py)
    }
}
