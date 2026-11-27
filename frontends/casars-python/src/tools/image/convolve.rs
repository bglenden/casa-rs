// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.convolve` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::convolve_image`].

use std::path::PathBuf;

use casa_core::ArrayD;
use casa_imageanalysis::tool::ops_transform::{ConvolveKernelInput, convolve_image};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::convert::Variant;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::pixels::snapshot_array;
use super::subimage::{RegionArgument, mask_argument, require_bool};

const INVALID_KERNEL: &str = "kernel is not understood, try using an array or an image";

enum KernelArgument {
    File {
        path: String,
        history: String,
    },
    Array {
        pixels: ArrayD<f64>,
        history: String,
    },
    Unsupported,
}

impl KernelArgument {
    fn capture(py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let Some(value) = value else {
            return Ok(Self::Unsupported);
        };
        let converted = if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            if value.len()? == 0 {
                return Ok(Self::Unsupported);
            }
            match py.import("numpy")?.call_method1("asarray", (value,)) {
                Ok(array) => Some(array),
                Err(_) => return Ok(Self::Unsupported),
            }
        } else {
            None
        };
        let raw = converted.as_ref().unwrap_or(value);
        if let Ok(array) = raw.cast::<PyUntypedArray>()
            && array.ndim() != 0
        {
            let dtype = array.dtype();
            match (dtype.kind(), dtype.itemsize()) {
                (b'f', 4 | 8) => {
                    let pixels = snapshot_array(py, array)?;
                    let history = if pixels.len() > 100 {
                        "\"(...)\"".into()
                    } else {
                        let values = pixels
                            .view()
                            .reversed_axes()
                            .iter()
                            .map(|value| format!("{value:.6}"))
                            .collect::<Vec<_>>();
                        format!("[{}]", values.join(","))
                    };
                    return Ok(Self::Array { pixels, history });
                }
                (b'i' | b'u', _) => {
                    let integers = snapshot_array::<i64>(py, array)?;
                    let history = if integers.len() > 100 {
                        "\"(...)\"".into()
                    } else {
                        let values = integers
                            .view()
                            .reversed_axes()
                            .iter()
                            .map(i64::to_string)
                            .collect::<Vec<_>>();
                        format!("[{}]", values.join(","))
                    };
                    let pixels = integers.mapv(|value| f64::from(value as i32));
                    return Ok(Self::Array { pixels, history });
                }
                _ => {}
            }
        }
        Ok(match value.extract::<Variant>()? {
            Variant::Text(path) => {
                let history = format!("{path:?}");
                Self::File { path, history }
            }
            argument @ Variant::Texts(_) => {
                let path = argument.describe();
                Self::File {
                    history: path.clone(),
                    path,
                }
            }
            _ => Self::Unsupported,
        })
    }
}

#[pymethods]
impl PyImageTool {
    /// Linearly convolve a selected real image with an array or native image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", kernel=None, scale=-1.0, region=RegionArgument::default(), mask=None, overwrite=false, stretch=false), text_signature = "($self, outfile='', kernel=[], scale=-1.0, region={}, mask='', overwrite=False, stretch=False)")]
    fn convolve(
        &self,
        py: Python<'_>,
        outfile: &str,
        kernel: Option<&Bound<'_, PyAny>>,
        scale: f64,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let kernel = KernelArgument::capture(py, kernel)?;
        let mask = mask_argument(mask)?;
        let output = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(source) = state.image.as_ref() else {
                return Ok(None);
            };
            let (kernel_in, kernel_history) = match kernel {
                KernelArgument::File { path, history } => {
                    (ConvolveKernelInput::ImagePath(PathBuf::from(path)), history)
                }
                KernelArgument::Array { pixels, history } => {
                    (ConvolveKernelInput::Array(pixels), history)
                }
                KernelArgument::Unsupported => return Err(raised(INVALID_KERNEL)),
            };
            let history_command = state.history.then(|| {
                let region_hist = region.history();
                format!(
                    "ia.convolve(outfile={outfile:?}, kernel={kernel_history}, scale={scale:.6}, region={region_hist}, vmask={mask:?}, overwrite={overwrite}, stretch={stretch})"
                )
            });
            convolve_image(
                source,
                outfile,
                &kernel_in,
                scale,
                region.selection(),
                &mask,
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
