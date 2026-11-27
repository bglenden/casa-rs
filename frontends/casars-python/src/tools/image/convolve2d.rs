// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.convolve2d` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::convolve_2d_gaussian`].

use casa_core::quanta::Quantity;
use casa_imageanalysis::analysis::GaussianConvolutionOptions;
use casa_imageanalysis::tool::ops_transform::convolve_2d_gaussian;
use pyo3::prelude::*;

use crate::convert::{IntList, Variant, quantity};

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};

fn parsed_widths(widths: &[Variant; 3], beam: &Variant) -> PyResult<[Quantity; 3]> {
    let parse = |value: &Variant| quantity(value).map_err(raised);
    let Variant::Record(beam) = beam else {
        if beam.is_empty_sequence() {
            return Ok([parse(&widths[0])?, parse(&widths[1])?, parse(&widths[2])?]);
        }
        return Err(raised("beam must be a record"));
    };
    if beam.is_empty() {
        return Ok([parse(&widths[0])?, parse(&widths[1])?, parse(&widths[2])?]);
    }
    if widths
        .iter()
        .any(|field| !matches!(field, Variant::Text(value) if value.is_empty()))
    {
        return Err(raised(
            "major, minor, and/or pa may not be specified if beam is specified",
        ));
    }
    if beam.len() != 3 {
        return Err(raised("If given, beam must have exactly three fields"));
    }
    let major = beam
        .get("major")
        .ok_or_else(|| raised("Beam must have a 'major' field"))?;
    let minor = beam
        .get("minor")
        .ok_or_else(|| raised("Beam must have a 'minor' field"))?;
    let angle = beam
        .get("pa")
        .or_else(|| beam.get("positionangle"))
        .ok_or_else(|| raised("Beam must have a 'positionangle' or 'pa' field"))?;
    for field in [major, minor, angle] {
        if !matches!(field, Variant::Text(_) | Variant::Record(_)) {
            return Err(raised("Unsupported data type for beam"));
        }
    }
    Ok([parse(major)?, parse(minor)?, parse(angle)?])
}

fn check_kernel(kind_raw: &str, beam: &Variant, targetres: bool) -> PyResult<()> {
    let kind = kind_raw.to_ascii_lowercase();
    let gaussian = !kind.is_empty() && "gaussian".starts_with(&kind);
    let has_beam = matches!(beam, Variant::Record(record) if !record.is_empty());
    if !gaussian && has_beam {
        return Err(raised("beam can only be given with a gaussian kernel"));
    }
    if gaussian {
        return Ok(());
    }
    if !kind.is_empty() && "boxcar".starts_with(&kind) {
        if targetres {
            return Err(raised(
                "targetres can only be true for a Gaussian convolving kernel",
            ));
        }
        return Err(raised("Boxcar kernel not yet implemented"));
    }
    if !kind.is_empty() && "hanning".starts_with(&kind) {
        return Err(raised("The kernel type HANNING is not supported"));
    }
    Err(raised(format!("Illegal kernel type {kind_raw}.")))
}

#[pymethods]
impl PyImageTool {
    /// Convolve two image axes with a Gaussian or to a requested resolution.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", axes=IntList::of(&[0, 1]), r#type="gaussian", major=None, minor=None, pa=None, scale=-1.0, region=RegionArgument::default(), mask=None, overwrite=false, stretch=false, targetres=false, beam=None), text_signature = "($self, outfile='', axes=[0, 1], type='gaussian', major='0deg', minor='0deg', pa='0deg', scale=-1.0, region={}, mask='', overwrite=False, stretch=False, targetres=False, beam={})")]
    fn convolve2d(
        &self,
        py: Python<'_>,
        outfile: &str,
        axes: IntList,
        r#type: &str,
        major: Option<Variant>,
        minor: Option<Variant>,
        pa: Option<Variant>,
        scale: f64,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        #[pyo3(from_py_with = require_bool)] targetres: bool,
        beam: Option<Variant>,
    ) -> PyResult<Py<Self>> {
        let mask = mask_argument(mask)?;
        let widths_arg =
            [major, minor, pa].map(|width| width.unwrap_or_else(|| Variant::Text("0deg".into())));
        let beam_arg = beam.unwrap_or(Variant::Empty);
        let output = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(source) = state.image.as_ref() else {
                return Ok(None);
            };
            check_kernel(r#type, &beam_arg, targetres)?;
            let widths = parsed_widths(&widths_arg, &beam_arg)?;
            let geometry = GaussianConvolutionOptions {
                axes: axes.0.clone(),
                widths,
                scale,
                target_resolution: targetres,
            };
            let history_command = state.history.then(|| {
                let width_strs = widths_arg.each_ref().map(|v| match v {
                    Variant::Text(t) => format!("{t:?}"),
                    v => v.describe(),
                });
                let [mj, mn, p] = width_strs;
                let axes_str = axes
                    .0
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                let region_hist = region.history();
                format!(
                    "ia.convolve2d(outfile={outfile:?}, axes=[{axes_str}], type={type:?}, major={mj}, minor={mn}, pa={p}, scale={scale:.6}, region={region_hist}, mask={mask:?}, overwrite={overwrite}, stretch={stretch}, targetres={targetres}, beam={{...}})",
                    type = r#type
                )
            });
            convolve_2d_gaussian(
                source,
                outfile,
                region.selection(),
                &mask,
                geometry,
                scale,
                overwrite,
                stretch,
                history_command,
                external_open_check(),
            )
            .map(Some)
            .map_err(raised)
        })?;
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}
