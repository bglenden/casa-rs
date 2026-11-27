// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's original fixed-integration statistical reweighter.

use casa_calibration::statwt::StatWtMs;
use casa_calibration::statwt::{LegacyStatWtOptions, reweight_legacy};
use casa_ms::MsSelection;
use pyo3::prelude::*;

use crate::convert::{Variant, quantity};

use super::partition::{
    all_if_minus_one, combine_argument, intent_argument, obs_argument, optional_expression,
};
use super::selection_indices::selection_text;
use super::{PyMsTool, runtime_error, strict_bool};

#[pymethods]
impl PyMsTool {
    /// Estimate raw or centered visibility scatter per integration and baseline.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (dorms = false, byantenna = true, sepacs = true, fitspw = None, fitcorr = None, combine = String::new(), timebin = None, minsamp = 3, field = None, spw = None, antenna = None, timerange = String::new(), scan = None, intent = String::new(), array = None, correlation = String::new(), obs = String::new(), datacolumn = "corrected_data".to_owned()), text_signature = "($self, dorms=False, byantenna=True, sepacs=True, fitspw=[], fitcorr=[], combine='', timebin=[], minsamp=3, field=[], spw=[], antenna='', timerange='', scan=[], intent='', array=[], correlation='', obs='', datacolumn='corrected_data')")]
    fn oldstatwt(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = strict_bool)] dorms: bool,
        #[pyo3(from_py_with = strict_bool)] byantenna: bool,
        #[pyo3(from_py_with = strict_bool)] sepacs: bool,
        fitspw: Option<&Bound<'_, PyAny>>,
        fitcorr: Option<Variant>,
        #[pyo3(from_py_with = combine_argument)] combine: String,
        timebin: Option<Variant>,
        minsamp: i64,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        antenna: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = timerange_argument)] timerange: String,
        scan: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = intent_argument)] intent: String,
        array: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = correlation_argument)] correlation: String,
        #[pyo3(from_py_with = obs_argument)] obs: String,
        #[pyo3(from_py_with = datacolumn_argument)] datacolumn: String,
    ) -> PyResult<bool> {
        // The original algorithm leaves these switches unused. It parses
        // the time quantum and always fits over one integration at a time.
        let _ = (byantenna, sepacs, fitcorr);
        let selection = MsSelection {
            field: all_if_minus_one(field)?,
            spw: all_if_minus_one(spw)?,
            antenna: optional_expression(antenna)?,
            timerange: nonempty(timerange),
            scan: optional_expression(scan)?,
            intent: nonempty(intent),
            array: optional_expression(array)?,
            correlation: nonempty(correlation.to_ascii_uppercase()),
            observation: nonempty(obs),
            ..Default::default()
        };
        let options = LegacyStatWtOptions {
            rms: dorms,
            minimum_samples: minsamp as u32,
            data_column: datacolumn,
            fit_spw: all_if_minus_one(fitspw)?.unwrap_or_default(),
            combine,
            selection,
        };
        py.detach(|| self.reweight_legacy(&options, timebin.as_ref()))
    }
}

impl PyMsTool {
    fn reweight_legacy(
        &self,
        options: &LegacyStatWtOptions,
        timebin: Option<&Variant>,
    ) -> PyResult<bool> {
        let mut attached = self.attached.lock().map_err(runtime_error)?;
        let ms = attached
            .as_mut()
            .ok_or_else(|| runtime_error("Table ... Null table object does not exist"))?;
        if !ms.writable {
            return Err(runtime_error("MeasurementSet is not writable"));
        }
        if let Some(bin) = timebin {
            quantity(bin)?;
        }
        let link = ms.working_metadata_link();
        let view = link.working_view().map_err(runtime_error)?;
        let path = view.path();
        let mut physical = StatWtMs::open_legacy(path).map_err(runtime_error)?;
        let sources = physical.legacy_sources().map_err(runtime_error)?;
        let mut transient = None;
        let locks = if sources.iter().all(|path| ms.write_locks.contains_key(path)) {
            &mut ms.write_locks
        } else {
            let locks = StatWtMs::write_locks(&sources).map_err(runtime_error)?;
            physical = StatWtMs::open_legacy(path).map_err(runtime_error)?;
            transient.insert(locks)
        };
        link.verify_directory().map_err(runtime_error)?;
        let result = reweight_legacy(&mut physical, options, locks);
        drop(physical);
        if let Some(locks) = transient {
            for lock in locks.into_values() {
                lock.release().map_err(runtime_error)?;
            }
        }
        result.map_err(runtime_error)
    }
}

fn nonempty(text: String) -> Option<String> {
    (!text.is_empty()).then_some(text)
}

fn timerange_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "timerange")
}

fn correlation_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "correlation")
}

fn datacolumn_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "datacolumn")
}
