// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.pbcor` wrapper delegating to [`casa_imageanalysis::tool::ops_spatial::pbcor_image`].

use std::path::{Path, PathBuf};

use casa_core::ArrayD;
use casa_imageanalysis::analysis::ImagePbcorOptions;
use casa_imageanalysis::tool::ops_spatial::{PbcorInput, pbcor_image};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyInt, PyList, PySlice, PyString, PyTuple};

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::pixels::{Pixels, snapshot_array};
use super::subimage::{RegionArgument, require_bool};

enum BeamArgument {
    File(String),
    Pixels {
        data: ArrayD<f32>,
        shape: Vec<usize>,
        history: String,
    },
    Unsupported(&'static str),
}

impl Default for BeamArgument {
    fn default() -> Self {
        Self::File(String::new())
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for BeamArgument {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(text) = value.cast::<PyString>() {
            return Ok(Self::File(text.to_str()?.to_owned()));
        }
        if let Ok(bytes) = value.cast::<PyBytes>() {
            return Ok(Self::File(
                std::str::from_utf8(bytes.as_bytes())
                    .map_err(raised)?
                    .to_owned(),
            ));
        }
        let py = value.py();
        let converted = if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            if value.len()? == 0 {
                return Ok(Self::Unsupported("boolvec"));
            }
            Some(py.import("numpy")?.call_method1("asarray", (&*value,))?)
        } else {
            None
        };
        let raw = converted.as_ref().unwrap_or(&value);
        if let Ok(array) = raw.cast::<PyUntypedArray>() {
            let dtype = array.dtype();
            return match (dtype.kind(), dtype.itemsize()) {
                (b'f', 4 | 8) if array.ndim() != 0 => capture_float_beam(py, array),
                (b'f', 4 | 8) => Ok(Self::Unsupported("double")),
                (b'i' | b'u', _) => Ok(Self::Unsupported("intvec")),
                (b'c', _) => Ok(Self::Unsupported("complexvec")),
                (b'S' | b'U', _) => Ok(Self::Unsupported("stringvec")),
                _ => Ok(Self::Unsupported("boolvec")),
            };
        }
        let kind = if value.is_instance_of::<PyDict>() {
            "record"
        } else if value.is_instance_of::<PyFloat>() {
            "double"
        } else if value.is_instance_of::<PyInt>() {
            "int"
        } else {
            "boolvec"
        };
        Ok(Self::Unsupported(kind))
    }
}

fn capture_float_beam(py: Python<'_>, array: &Bound<'_, PyUntypedArray>) -> PyResult<BeamArgument> {
    let shape = array.shape().to_vec();
    let history = if array.len() > 100 {
        "\"(...)\"".into()
    } else {
        Pixels::capture(py, Some(array.as_any()), false)?.history()
    };
    let compact = if array.strides().contains(&0) {
        let slices = shape.iter().zip(array.strides()).map(|(&length, &stride)| {
            PySlice::new(py, 0, if stride == 0 { 1 } else { length as isize }, 1)
        });
        Some(
            array
                .get_item(PyTuple::new(py, slices)?)?
                .cast_into::<PyUntypedArray>()?,
        )
    } else {
        None
    };
    let data = snapshot_array(py, compact.as_ref().unwrap_or(array))?;
    Ok(BeamArgument::Pixels {
        data,
        shape,
        history,
    })
}

fn text_option(text: &str) -> Option<&str> {
    (!text.is_empty()).then_some(text)
}

#[pymethods]
impl PyImageTool {
    /// Multiply or divide a selected image by an aligned primary beam.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (pbimage=BeamArgument::default(), outfile="", overwrite=false, r#box="", region=RegionArgument::default(), chans="", stokes="", mask="", mode="divide", cutoff=-1.0, stretch=false), text_signature = "($self, pbimage='', outfile='', overwrite=False, box='', region={}, chans='', stokes='', mask='', mode='divide', cutoff=-1.0, stretch=False)")]
    fn pbcor(
        &self,
        py: Python<'_>,
        pbimage: BeamArgument,
        outfile: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        r#box: &str,
        region: RegionArgument,
        chans: &str,
        stokes: &str,
        mask: &str,
        mode: &str,
        cutoff: f32,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let output = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let source = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Unable to create image"))?;
            region.validate_selectors(r#box, chans, stokes)?;
            let (pb_input, array_history) = match pbimage {
                BeamArgument::File(name) => (PbcorInput::ImagePath(PathBuf::from(name)), None),
                BeamArgument::Pixels {
                    data,
                    shape,
                    history,
                } => (PbcorInput::Array { data, shape }, Some(history)),
                BeamArgument::Unsupported(kind) => {
                    return Err(raised(format!("Unsupported type {kind} for pbimage")));
                }
            };
            let options = ImagePbcorOptions {
                mode,
                cutoff,
                region: region.selection(),
                box_selection: text_option(r#box),
                channels: text_option(chans),
                stokes: text_option(stokes),
                mask: text_option(mask),
                stretch,
                destination: text_option(outfile).map(Path::new),
            };
            let history_command = state.history.then_some(()).and(array_history).map(|hist| {
                let region_hist = region.history();
                format!(
                    "ia.pbcor(pbimage={hist}, outfile={outfile:?}, overwrite={overwrite}, box={box:?}, region={region_hist}, chans={chans:?}, stokes={stokes:?}, mask={mask:?}, mode={mode:?}, cutoff={cutoff:.6}, stretch={stretch})",
                    box = r#box
                )
            });
            pbcor_image(
                source,
                &pb_input,
                outfile,
                overwrite,
                options,
                history_command,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        Py::new(py, Self::attached(output))
    }
}
