// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `image` tool (ia): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0017, #1025). A method casa-rs does not implement
//! yet raises `NotImplementedError` naming #1027 (`stubs.rs`).

mod add_axes;
mod attachment;
mod beam_deconvolution;
mod beam_edit;
mod beam_rotate;
mod beams;
mod bounding_box;
mod calc;
mod collapse;
mod concatenate;
mod conformance;
mod continuum;
mod convertflux;
mod convolve;
mod convolve2d;
mod coordinate_conversion;
mod copy;
mod create;
mod crop;
mod data_compat;
mod decimate;
mod decompose;
mod deconvolve_components;
mod deviation;
mod fft;
mod findsources;
mod fitcomponents;
mod fitprofile;
mod fits_export;
mod fits_header;
mod fits_import;
mod getprofile;
mod handle;
mod histograms;
mod imagecalc;
mod insert;
mod lifecycle;
mod locking;
mod makecomplex;
mod mask_expression;
mod masks;
mod metadata;
mod modify;
mod moments;
mod noise;
mod pad;
mod pbcor;
mod pixel_edit;
mod pixels;
mod point;
mod put_region;
mod pv;
mod rebin;
mod records;
mod region_read;
mod regrid;
mod replace_masked;
mod rotate;
mod sepconvolve;
mod serialization;
mod slice;
mod smoothing;
mod statistics;
mod subimage;
mod summary;
pub(crate) mod surface;
mod transpose;
mod two_point;

use std::sync::{Mutex, MutexGuard, PoisonError};

use pyo3::prelude::*;

use crate::tools::not_yet_implemented_in;
use attachment::AttachedImage;

/// The issue that records the methods casa-rs refuses.
const ISSUE: u32 = 1027;

/// A method casa-rs does not implement yet.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("image", method, ISSUE)
}

/// CASA's `image` tool.
#[pyclass(name = "image", module = "casars.tools", frozen)]
pub(crate) struct PyImageTool {
    state: Mutex<ImageState>,
}

struct ImageState {
    image: Option<AttachedImage>,
    history: bool,
}

impl PyImageTool {
    fn attached(image: AttachedImage) -> Self {
        Self {
            state: Mutex::new(ImageState {
                image: Some(image),
                history: true,
            }),
        }
    }

    fn attached_handle(handle: handle::ImageHandle) -> Self {
        Self::attached(AttachedImage::from_handle(handle))
    }

    fn state(&self) -> MutexGuard<'_, ImageState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for PyImageTool {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        if let Some(image) = state.image.take() {
            // The last attachment may flush tiles and release table locks.
            let _ = Python::try_attach(|py| py.detach(|| drop(image)));
        }
    }
}

#[pymethods]
impl PyImageTool {
    #[new]
    fn new() -> Self {
        Self {
            state: Mutex::new(ImageState {
                image: None,
                history: true,
            }),
        }
    }
}
