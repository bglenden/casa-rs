// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::Arc;

use casa_calibration::continuum::{
    ContinuumScratchOptions, LegacyContinuumTransform, fit_continuum_scratch_holding,
};
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::transform::{LegacyMsTool, LegacyTimeOptions, MsTransformTaskOptions};
use casa_ms::{MeasurementSet, MsSelection};
use casa_tables::TableWriteLock;
use casa_types::quanta::{Quantity, Unit};
use pyo3::prelude::*;

use super::partition::{
    all_if_minus_one, combine_argument, intent_argument, obs_argument, optional_expression,
    output_argument, visibility_columns, whichcol_argument,
};
use super::selection_indices::selection_text;
use super::{PyMsTool, runtime_error};

#[pymethods]
impl PyMsTool {
    /// Fit the selected continuum using correlation-specific visibility weights.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (field = None, fitspw = None, spw = None, solint = None, fitorder = 0, mode = "subtract".to_owned()), text_signature = "($self, field=[], fitspw=[], spw=[], solint=[], fitorder=0, mode='subtract')")]
    fn continuumsub(
        &self,
        py: Python<'_>,
        field: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        solint: Option<&Bound<'_, PyAny>>,
        fitorder: i64,
        #[pyo3(from_py_with = mode_argument)] mode: String,
    ) -> PyResult<bool> {
        let options = scratch_options(field, fitspw, spw, solint, fitorder, mode, false)?;
        py.detach(|| self.fit_continuum_scratch(&options))
    }

    /// Fit the continuum with the legacy visibility iterator's flag handling.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (field = None, fitspw = None, spw = None, solint = None, fitorder = 0, mode = "subtract".to_owned()), text_signature = "($self, field=[], fitspw=[], spw=[], solint=[], fitorder=0, mode='subtract')")]
    fn continuumsubold(
        &self,
        py: Python<'_>,
        field: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        solint: Option<&Bound<'_, PyAny>>,
        fitorder: i64,
        #[pyo3(from_py_with = mode_argument)] mode: String,
    ) -> PyResult<bool> {
        let options = scratch_options(field, fitspw, spw, solint, fitorder, mode, true)?;
        py.detach(|| self.fit_continuum_scratch(&options))
    }

    /// Write a selected MeasurementSet after removing a frequency polynomial.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outputms = String::new(), fitspw = None, fitorder = 1, combine = String::new(), spw = None, unionspw = None, field = None, scan = None, intent = String::new(), correlation = String::new(), obs = String::new(), whichcol = "CORRECTED_DATA".to_owned()), text_signature = "($self, outputms='', fitspw=[], fitorder=1, combine='', spw='*', unionspw='*', field='', scan='', intent='', correlation='', obs='', whichcol='CORRECTED_DATA')")]
    fn contsub(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = output_argument)] outputms: String,
        fitspw: Option<&Bound<'_, PyAny>>,
        fitorder: i64,
        #[pyo3(from_py_with = combine_argument)] combine: String,
        spw: Option<&Bound<'_, PyAny>>,
        unionspw: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = intent_argument)] intent: String,
        #[pyo3(from_py_with = correlation_argument)] correlation: String,
        #[pyo3(from_py_with = obs_argument)] obs: String,
        #[pyo3(from_py_with = whichcol_argument)] whichcol: String,
    ) -> PyResult<bool> {
        let union = all_if_minus_one(unionspw)?.unwrap_or_else(|| "*".into());
        let output = all_if_minus_one(spw)?.unwrap_or_else(|| "*".into());
        let fields = all_if_minus_one(field)?;
        let selection = MsSelection {
            field: fields.clone(),
            scan: optional_expression(scan)?,
            intent: nonempty(intent),
            correlation: nonempty(correlation.to_ascii_uppercase()),
            observation: nonempty(obs),
            ..Default::default()
        };
        let Some(columns) = visibility_columns(&whichcol)? else {
            return Ok(false);
        };
        let [column] = columns.as_slice() else {
            return Ok(false);
        };
        if !matches!(
            column,
            OptionalMainColumn::Data
                | OptionalMainColumn::CorrectedData
                | OptionalMainColumn::ModelData
        ) {
            return Ok(false);
        }
        let parameters = format!(
            "fieldids={} spwids={output} whichcol='{whichcol}'",
            fields.unwrap_or_default()
        );
        let view = LegacyContinuumTransform {
            column: column.name(),
            order: usize::try_from(fitorder)
                .map_err(|_| runtime_error("The continuum fitter must first be initialized."))?,
            fit_spw: all_if_minus_one(fitspw)?.unwrap_or_default(),
            union: MsSelection {
                spw: Some(union.clone()),
                ..selection.clone()
            },
            combine: combine.to_ascii_lowercase(),
        };
        let options = MsTransformTaskOptions {
            visibility_columns: Some(columns),
            input_view: Some(Arc::new(view)),
            legacy_output_spw: (output != "*").then_some(output),
            legacy_tool: Some(LegacyMsTool::Partition(LegacyTimeOptions {
                combine_observations: false,
            })),
            ..Default::default()
        };
        self.write_subms(
            py,
            outputms,
            union,
            selection,
            options,
            ("contsub", Some(parameters)),
        )
    }
}

impl PyMsTool {
    fn fit_continuum_scratch(&self, options: &ContinuumScratchOptions) -> PyResult<bool> {
        let mut attachment = self.attached.lock().map_err(runtime_error)?;
        let attached = attachment
            .as_mut()
            .ok_or_else(|| runtime_error("Table column DATA_DESC_ID is unknown"))?;
        if !attached.writable {
            return Err(runtime_error("MeasurementSet is not writable"));
        }
        for path in &attached.sources {
            let mut temporary;
            let lock = if let Some(lock) = attached.write_locks.get_mut(path) {
                lock
            } else {
                temporary = TableWriteLock::acquire_to_reread(path, 0).map_err(runtime_error)?;
                &mut temporary
            };
            let mut ms = MeasurementSet::open(path).map_err(runtime_error)?;
            fit_continuum_scratch_holding(&mut ms, options, lock).map_err(runtime_error)?;
        }
        Ok(true)
    }
}

fn scratch_options(
    field: Option<&Bound<'_, PyAny>>,
    fitspw: Option<&Bound<'_, PyAny>>,
    spw: Option<&Bound<'_, PyAny>>,
    solint: Option<&Bound<'_, PyAny>>,
    fitorder: i64,
    mode: String,
    old_iterator: bool,
) -> PyResult<ContinuumScratchOptions> {
    Ok(ContinuumScratchOptions {
        field: optional_expression(field)?,
        fit_spw: optional_expression(fitspw)?.unwrap_or_default(),
        apply_spw: optional_expression(spw)?.unwrap_or_default(),
        solution_interval: solution_interval(
            optional_expression(solint)?.as_deref().unwrap_or_default(),
        )?,
        order: usize::try_from(fitorder)
            .map_err(|_| runtime_error("fitorder must be nonnegative"))?,
        mode,
        old_iterator,
    })
}

fn solution_interval(text: &str) -> PyResult<f64> {
    let folded = text.to_ascii_uppercase();
    if text.is_empty() || folded.contains("INF") {
        return Ok(0.0);
    }
    if folded.contains("INT") {
        return Ok(-1.0);
    }
    let quantity =
        Quantity::read(text).map_err(|_| runtime_error("Unrecognized units for solint."))?;
    let seconds = Unit::new("s").expect("seconds");
    Ok(f64::from(
        quantity
            .get_value_in(&seconds)
            .unwrap_or_else(|_| quantity.value()) as f32,
    ))
}

fn mode_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "mode")
}

fn correlation_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "correlation")
}

fn nonempty(text: String) -> Option<String> {
    (!text.is_empty()).then_some(text)
}
