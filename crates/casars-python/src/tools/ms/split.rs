// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::{
    selection::MsSelection,
    transform::{LegacyMsTool, LegacyTimeOptions, MsTransformTaskOptions},
};
use pyo3::prelude::*;

use crate::convert::Variant;

use super::partition::{
    all_if_minus_one, combine_argument, intent_argument, obs_argument, optional_expression,
    output_argument, taql_argument, tile_layout, time_argument, time_average, visibility_columns,
    whichcol_argument,
};
use super::{PyMsTool, selection_indices::selection_text};

#[pymethods]
impl PyMsTool {
    /// Materialize and compact a selected MeasurementSet, optionally averaging its visibilities.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outputms = String::new(), field = None, spw = None, step = vec![1], baseline = None, timebin = None, time = String::new(), scan = None, uvrange = None, taql = String::new(), whichcol = "DATA".to_owned(), tileshape = None, subarray = None, combine = String::new(), correlation = String::new(), intent = String::new(), obs = String::new()), text_signature = "($self, outputms='', field=[], spw=[], step=[1], baseline=[], timebin=[], time='', scan=[], uvrange=[], taql='', whichcol='DATA', tileshape=[], subarray=[], combine='', correlation='', intent='', obs='')")]
    fn split(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = output_argument)] outputms: String,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        step: Vec<i32>,
        baseline: Option<&Bound<'_, PyAny>>,
        timebin: Option<Variant>,
        #[pyo3(from_py_with = time_argument)] time: String,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = taql_argument)] taql: String,
        #[pyo3(from_py_with = whichcol_argument)] whichcol: String,
        tileshape: Option<Variant>,
        subarray: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = combine_argument)] combine: String,
        #[pyo3(from_py_with = correlation_argument)] correlation: String,
        #[pyo3(from_py_with = intent_argument)] intent: String,
        #[pyo3(from_py_with = obs_argument)] obs: String,
    ) -> PyResult<bool> {
        let spw = all_if_minus_one(spw)?.unwrap_or_else(|| "*".into());
        let field = all_if_minus_one(field)?;
        let history = format!(
            "fieldids={} spwids={spw} step={step:?} which='{whichcol}'",
            field.as_deref().unwrap_or("")
        );
        let selection = MsSelection {
            field,
            antenna: optional_expression(baseline)?,
            timerange: (!time.is_empty()).then_some(time),
            scan: optional_expression(scan)?,
            uvrange: optional_expression(uvrange)?,
            msselect: (!taql.is_empty()).then_some(taql),
            array: optional_expression(subarray)?,
            correlation: (!correlation.is_empty()).then_some(correlation),
            intent: (!intent.is_empty()).then_some(intent),
            observation: (!obs.is_empty()).then_some(obs),
            ..MsSelection::default()
        };
        let options = MsTransformTaskOptions {
            visibility_columns: visibility_columns(&whichcol)?,
            channel_widths: (!step.is_empty())
                .then(|| step.iter().map(|&width| width.max(1) as usize).collect()),
            legacy_tool: Some(LegacyMsTool::Split(LegacyTimeOptions {
                combine_observations: combine.to_ascii_lowercase().contains("obs"),
            })),
            time_averaging: time_average(timebin.as_ref(), &combine)?,
            tiling: tile_layout(tileshape.as_ref())?,
            ..MsTransformTaskOptions::default()
        };
        self.write_subms(py, outputms, spw, selection, options, Some(history))
    }
}

fn correlation_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "correlation")
}
