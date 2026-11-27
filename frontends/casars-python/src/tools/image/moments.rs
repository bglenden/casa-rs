// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.moments` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::moments_image`].

use casa_core::measures::doppler::DopplerRef;
use casa_imageanalysis::tool::ops_transform::moments_image;
use pyo3::prelude::*;

use crate::convert::{DoubleList, IntList, StringList, Variant};
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Collapse masked image profiles and return the first requested moment image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (moments=None, axis=-10, region=None, mask=None, method=None, smoothaxes=None, smoothtypes=None, smoothwidths=None, includepix=None, excludepix=None, peaksnr=3., stddev=0., doppler="RADIO", outfile="", smoothout="", overwrite=false, drop=true, stretch=false), text_signature = "($self, moments=[0], axis=-10, region={}, mask='', method=[], smoothaxes=[-1], smoothtypes=[], smoothwidths=[0.0], includepix=[-1.0], excludepix=[-1.0], peaksnr=3.0, stddev=0.0, doppler='RADIO', outfile='', smoothout='', overwrite=False, drop=True, stretch=False)")]
    fn moments(
        &self,
        py: Python<'_>,
        moments: Option<IntList>,
        axis: i64,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        method: Option<StringList>,
        smoothaxes: Option<IntList>,
        smoothtypes: Option<&Bound<'_, PyAny>>,
        smoothwidths: Option<DoubleList>,
        includepix: Option<DoubleList>,
        excludepix: Option<DoubleList>,
        peaksnr: f64,
        stddev: f64,
        doppler: &str,
        outfile: &str,
        smoothout: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] drop: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let moments = moments
            .map_or_else(|| vec![0], |values| values.0)
            .into_iter()
            .map(i32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(raised)?;
        let region: RegionArgument =
            region.map_or_else(|| Ok(Default::default()), |value| value.extract())?;
        let mask = mask_argument(mask)?;
        let methods = method.map_or_else(Vec::new, |value| value.0);
        let axes = smoothaxes.map_or_else(|| vec![-1], |value| value.0);
        let kernels = match smoothtypes
            .map(|value| value.extract::<Variant>())
            .transpose()?
        {
            Some(Variant::Text(name)) => name
                .split(|character: char| character == ',' || character.is_ascii_whitespace())
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect(),
            Some(Variant::Texts(names)) => names,
            _ => Vec::new(),
        };
        let widths = smoothwidths
            .filter(|value| !value.0.is_empty())
            .map_or_else(|| vec![0.], |value| value.0);
        let include = includepix.map_or_else(|| vec![-1.], |value| value.0);
        let exclude = excludepix.map_or_else(|| vec![-1.], |value| value.0);
        let parsed = if doppler.eq_ignore_ascii_case("TRUE") {
            Ok(DopplerRef::BETA)
        } else {
            doppler.parse::<DopplerRef>()
        };
        let logs = (!doppler.is_empty() && parsed.is_err())
            .then(|| LogLine::warning(format!("Illegal velocity type {doppler}. Using RADIO")))
            .into_iter()
            .collect::<Vec<_>>();
        let history = format!(
            "ia.moments(moments={moments:?}, axis={axis}, region={}, mask={mask:?}, method={methods:?}, smoothaxes={axes:?}, smoothtypes={kernels:?}, smoothwidths={widths:?}, includepix={include:?}, excludepix={exclude:?}, peaksnr={peaksnr}, stddev={stddev}, doppler={doppler:?}, outfile={outfile:?}, smoothout={smoothout:?}, overwrite={}, drop={}, stretch={})",
            region.history(),
            python_bool(overwrite),
            python_bool(drop),
            python_bool(stretch),
        );
        let output = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(input) = &state.image else {
                return Ok(None);
            };
            moments_image(
                input,
                &moments,
                axis,
                region.selection(),
                Some(&mask),
                stretch,
                drop,
                &methods,
                (&axes, &kernels, &widths),
                &include,
                &exclude,
                peaksnr as f32,
                stddev as f32,
                parsed.unwrap_or(DopplerRef::RADIO),
                outfile,
                smoothout,
                overwrite,
                state.history.then_some(history),
                external_open_check(),
            )
            .map(Some)
            .map_err(raised)
        })?;
        if output.is_some() {
            emit(py, "casars.tools.image", &logs)?;
        }
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}

fn python_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}
