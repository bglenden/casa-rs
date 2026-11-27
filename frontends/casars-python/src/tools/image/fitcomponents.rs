// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.fitcomponents` wrapper delegating to [`casa_imageanalysis::tool::ops_fit::fit_components`].

use std::path::Path;

use casa_core::RecordValue;
use casa_core::quanta::Quantity;
use casa_imageanalysis::analysis::ComponentFitQuantity;
use casa_imageanalysis::tool::ops_fit::{
    FitComponentsHistory, FitComponentsRequest, fit_components,
};
use pyo3::prelude::*;
use pyo3::types::PyBool;

use crate::convert::{DoubleList, Variant, quantity};

use super::PyImageTool;
use super::beam_edit::input_history;
use super::data_compat::record_to_py;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};

enum MeasureArgument {
    Numeric(f64),
    Quantity(Quantity),
}

impl MeasureArgument {
    fn value(&self) -> ComponentFitQuantity<'_> {
        match self {
            Self::Numeric(value) => ComponentFitQuantity::Numeric(*value),
            Self::Quantity(value) => ComponentFitQuantity::Quantity(value),
        }
    }
}

#[pymethods]
impl PyImageTool {
    /// Fit Gaussian sky components and write optional model and residual images.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (r#box="", region=None, chans=None, stokes="", mask=None, includepix=DoubleList(vec![-1.]), excludepix=DoubleList(vec![-1.]), residual="", model="", estimates="", logfile="", append=true, newestimates="", complist="", overwrite=false, dooff=false, offset=0., fixoffset=false, stretch=false, rms=None, noisefwhm=None, summary=""), text_signature = "($self, box='', region=[], chans=[], stokes='', mask='', includepix=[-1.0], excludepix=[-1.0], residual='', model='', estimates='', logfile='', append=True, newestimates='', complist='', overwrite=False, dooff=False, offset=0.0, fixoffset=False, stretch=False, rms='', noisefwhm='', summary='')")]
    fn fitcomponents<'py>(
        &self,
        py: Python<'py>,
        r#box: &str,
        region: Option<&Bound<'py, PyAny>>,
        chans: Option<&Bound<'py, PyAny>>,
        stokes: &str,
        mask: Option<&Bound<'py, PyAny>>,
        includepix: DoubleList,
        excludepix: DoubleList,
        residual: &str,
        model: &str,
        estimates: &str,
        logfile: &str,
        #[pyo3(from_py_with = require_bool)] append: bool,
        newestimates: &str,
        complist: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] dooff: bool,
        offset: f64,
        #[pyo3(from_py_with = require_bool)] fixoffset: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        rms: Option<&Bound<'py, PyAny>>,
        noisefwhm: Option<&Bound<'py, PyAny>>,
        summary: &str,
    ) -> PyResult<Py<PyAny>> {
        if py.detach(|| self.state().image.is_none()) {
            return record_to_py(py, RecordValue::default());
        }
        let region: RegionArgument =
            region.map_or_else(|| Ok(RegionArgument::default()), |arg| arg.extract())?;
        let (channels, channels_history) = channel_argument(chans)?;
        let (rms, rms_history) = measure_argument(rms, "rms")?;
        let (noise, noise_history) = measure_argument(noisefwhm, "noisefwhm")?;
        let mask = mask_argument(mask)?;
        let region_history = if region.selection().is_none() {
            "[]".to_owned()
        } else {
            region.history()
        };
        let record = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(source) = &state.image else {
                return Ok(RecordValue::default());
            };
            let request = FitComponentsRequest {
                box_selection: nonempty(r#box),
                region: region.selection(),
                chans: nonempty(&channels),
                stokes: nonempty(stokes),
                mask: Some(&mask),
                stretch,
                includepix: &includepix.0,
                excludepix: &excludepix.0,
                residual,
                model,
                estimates: nonempty(estimates).map(Path::new),
                logfile,
                append,
                newestimates,
                complist,
                summary,
                overwrite,
                fit_zero_level: dooff,
                zero_level: (offset, fixoffset),
                rms: rms.as_ref().map(MeasureArgument::value),
                noise_fwhm: noise.as_ref().map(MeasureArgument::value),
                history: FitComponentsHistory {
                    region: &region_history,
                    channels: &channels_history,
                    rms: &rms_history,
                    noise_fwhm: &noise_history,
                },
                record_history: state.history,
            };
            fit_components(source, request, external_open_check()).map_err(raised)
        })?;
        record_to_py(py, record)
    }
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn channel_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<(String, String)> {
    let Some(value) = value else {
        return Ok((String::new(), "[]".into()));
    };
    if let Ok(text) = value.extract::<String>() {
        let history = format!("{text:?}");
        return Ok((text, history));
    }
    if !value.is_instance_of::<PyBool>()
        && let Ok(channel) = value.extract::<i64>()
    {
        let text = channel.to_string();
        return Ok((text.clone(), text));
    }
    if value.extract::<Variant>()?.is_empty_sequence()
        || value.try_iter().is_ok_and(|mut values| {
            values.all(|item| item.is_ok_and(|item| item.is_instance_of::<PyBool>()))
        })
    {
        return Ok((String::new(), "[]".into()));
    }
    Err(raised(
        "Unsupported type for chans. chans must be either an integer or a string",
    ))
}

fn measure_argument(
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
) -> PyResult<(Option<MeasureArgument>, String)> {
    let Some(value) = value else {
        return Ok((None, r#""""#.into()));
    };
    let value = value.extract::<Variant>()?;
    let history = input_history(&value);
    let parsed = match &value {
        Variant::Empty => None,
        Variant::Text(text) if text.is_empty() => None,
        Variant::Number(value) => Some(MeasureArgument::Numeric(*value)),
        Variant::Text(_) | Variant::Record(_) => {
            Some(MeasureArgument::Quantity(quantity(&value).map_err(raised)?))
        }
        _ => return Err(raised(format!("Unsupported data type for {name}"))),
    };
    Ok((parsed, history))
}
