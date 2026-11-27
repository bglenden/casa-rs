// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.boxcar` / `ia.hanning` wrappers delegating to [`casa_imageanalysis::tool::ops_transform::smooth_axis`].

use std::path::Path;

use casa_imageanalysis::analysis::{DecimationMethod, SmoothingKernel, SmoothingOptions};
use casa_imageanalysis::tool::ops_transform::smooth_axis;
use pyo3::prelude::*;
use pyo3::types::PyString;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};

struct SmoothingMask {
    value: String,
    history: String,
}

impl SmoothingMask {
    fn boxcar_default() -> Self {
        Self {
            value: String::new(),
            history: "[]".into(),
        }
    }

    fn hanning_default() -> Self {
        Self {
            value: String::new(),
            history: r#""""#.into(),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for SmoothingMask {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let mask = mask_argument((!value.is_none()).then_some(&value))?;
        let history = if value.is_instance_of::<PyString>() {
            format!("{mask:?}")
        } else {
            "[]".into()
        };
        Ok(Self {
            value: mask,
            history,
        })
    }
}

impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    fn run_smoothing(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: RegionArgument,
        mask: SmoothingMask,
        kernel: SmoothingKernel,
        axis: i64,
        drop: bool,
        dmethod: &str,
        overwrite: bool,
        stretch: bool,
    ) -> PyResult<Py<Self>> {
        let output = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let attached = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Unable to create image"))?;
            let decimate = if drop {
                match dmethod.as_bytes().first().map(u8::to_ascii_lowercase) {
                    Some(b'c') => Some(DecimationMethod::Copy),
                    Some(b'm') => Some(DecimationMethod::Mean),
                    _ => {
                        return Err(raised(
                            "Value of dmethod must be either 'm'(ean) or 'c'(opy)",
                        ));
                    }
                }
            } else {
                None
            };
            let options = SmoothingOptions {
                axis,
                kernel,
                decimate,
                region: region.selection(),
                mask: (!mask.value.is_empty()).then_some(mask.value.as_str()),
                stretch,
                destination: (!outfile.is_empty()).then(|| Path::new(outfile)),
            };
            let region_hist = region.history();
            let mask_hist = &mask.history;
            let build_cmd = |resolved_axis: usize| match kernel {
                SmoothingKernel::Boxcar(width) => format!(
                    "ia.boxcar(outfile={outfile:?}, region={region_hist}, mask={mask_hist}, axis={resolved_axis}, width={width}, drop={drop}, dmethod={dmethod:?}, overwrite={overwrite}, stretch={stretch})"
                ),
                SmoothingKernel::Hanning => format!(
                    "ia.hanning(outfile={outfile:?}, region={region_hist}, mask={mask_hist}, axis={resolved_axis}, drop={drop}, overwrite={overwrite}, stretch={stretch}, dmethod={dmethod:?})"
                ),
            };
            smooth_axis(
                attached,
                outfile,
                overwrite,
                options,
                state.history.then_some(&build_cmd as &dyn Fn(usize) -> String),
                external_open_check(),
            )
            .map_err(raised)
        })?;
        Py::new(py, Self::attached(output))
    }
}

#[pymethods]
impl PyImageTool {
    /// Smooth an axis over consecutive windows, optionally decimating.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=RegionArgument::default(), mask=SmoothingMask::boxcar_default(), axis=-1, width=2, drop=true, dmethod="copy", overwrite=false, stretch=false), text_signature = "($self, outfile='', region={}, mask=[], axis=-1, width=2, drop=True, dmethod='copy', overwrite=False, stretch=False)")]
    fn boxcar(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: RegionArgument,
        mask: SmoothingMask,
        axis: i64,
        width: i64,
        #[pyo3(from_py_with = require_bool)] drop: bool,
        dmethod: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let width = usize::try_from(width).map_err(|_| raised("Boxcar width must be positive"))?;
        self.run_smoothing(
            py,
            outfile,
            region,
            mask,
            SmoothingKernel::Boxcar(width),
            axis,
            drop,
            dmethod,
            overwrite,
            stretch,
        )
    }

    /// Apply the three-point Hanning kernel and optionally decimate.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=RegionArgument::default(), mask=SmoothingMask::hanning_default(), axis=-10, drop=true, overwrite=false, stretch=false, dmethod="copy"), text_signature = "($self, outfile='', region={}, mask='', axis=-10, drop=True, overwrite=False, stretch=False, dmethod='copy')")]
    fn hanning(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: RegionArgument,
        mask: SmoothingMask,
        axis: i64,
        #[pyo3(from_py_with = require_bool)] drop: bool,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        dmethod: &str,
    ) -> PyResult<Py<Self>> {
        self.run_smoothing(
            py,
            outfile,
            region,
            mask,
            SmoothingKernel::Hanning,
            axis,
            drop,
            dmethod,
            overwrite,
            stretch,
        )
    }
}
