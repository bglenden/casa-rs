// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.statistics` wrapper delegating to [`casa_imageanalysis::tool::statistics`].

use std::path::Path;

use casa_imageanalysis::analysis::SubimageRegion;
use casa_imageanalysis::tool::statistics::{
    StatisticsRequest, image_statistics, parse_cursor_axes, parse_pixel_range,
};
use pyo3::prelude::*;

use crate::convert::{DoubleList, IntList, Variant, value::CasaValue};

use super::PyImageTool;
use super::handle::{emit_tool_logs, raised};
use super::region_read::read_region_argument;
use super::subimage::Region;

fn stats_mask(value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    match value.map(|value| value.extract::<Variant>()).transpose()? {
        None | Some(Variant::Empty | Variant::Bool(false)) => Ok(String::new()),
        Some(Variant::Text(text)) if matches!(text.as_str(), "false" | "[]") => Ok(String::new()),
        Some(Variant::Text(text)) => Ok(text),
        Some(Variant::Bool(true)) => Ok("true".into()),
        _ => Err(raised("Mask is not understood, try a valid LEL string")),
    }
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (axes=None, region=None, mask=None, includepix=None, excludepix=None, list=false, force=false, disk=false, robust=false, verbose=false, stretch=false, logfile="", append=true, algorithm="classic", fence=-1.0, center="mean", lside=true, zscore=-1.0, maxiter=-1, clmethod="auto", niter=3), text_signature = "($self, axes=[-1], region='', mask='', includepix=[-1.0], excludepix=[-1.0], list=False, force=False, disk=False, robust=False, verbose=False, stretch=False, logfile='', append=True, algorithm='classic', fence=-1.0, center='mean', lside=True, zscore=-1.0, maxiter=-1, clmethod='auto', niter=3)")]
    fn statistics(
        &self,
        py: Python<'_>,
        axes: Option<IntList>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        includepix: Option<DoubleList>,
        excludepix: Option<DoubleList>,
        list: bool,
        force: bool,
        disk: bool,
        robust: bool,
        verbose: bool,
        stretch: bool,
        logfile: &str,
        append: bool,
        algorithm: &str,
        fence: f64,
        center: &str,
        lside: bool,
        zscore: f64,
        maxiter: i64,
        clmethod: &str,
        niter: i64,
    ) -> PyResult<Py<PyAny>> {
        let _ = force;
        let region = read_region_argument(py, region, "statistics");
        let expression = stats_mask(mask);
        let (value, logs) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((CasaValue::empty_record(), Vec::new()));
            };
            let include = parse_pixel_range(includepix.as_ref().map(|v| v.0.as_slice()), "include")
                .map_err(raised)?;
            let exclude = parse_pixel_range(excludepix.as_ref().map(|v| v.0.as_slice()), "exclude")
                .map_err(raised)?;
            let region = region?;
            let mask = expression?;
            let rank = attachment.lock().shape().len();
            let raw_axes = axes.map_or_else(Vec::new, |arg| arg.0);
            let parsed_axes = parse_cursor_axes(&raw_axes, rank).map_err(raised)?;
            let request = StatisticsRequest {
                axes: Some(parsed_axes),
                region: region
                    .as_ref()
                    .map(Region::as_region)
                    .or(None::<SubimageRegion<'_>>),
                mask: (!mask.is_empty()).then_some(mask.as_str()),
                stretch,
                includepix: include,
                excludepix: exclude,
                list,
                robust,
                logfile: (!logfile.is_empty()).then(|| Path::new(logfile)),
                append,
                algorithm,
                fence,
                center,
                lside,
                zscore,
                maxiter: maxiter as i32,
                clmethod,
                niter: niter as i32,
                disk,
                verbose,
            };
            let result = image_statistics(attachment, &request).map_err(raised)?;
            Ok((CasaValue::from_record(&result.record), result.logs))
        })?;
        emit_tool_logs(py, &logs)?;
        value.into_python(py)
    }
}
