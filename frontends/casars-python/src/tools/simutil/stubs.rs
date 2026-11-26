// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `simutil` methods casa-rs does not implement yet: each has CASA
//! 6.7.6.14's signature and refuses, naming #1053, which classifies them.

use pyo3::prelude::*;

use super::PySimutil;
use crate::tools::not_yet_implemented_in;

/// The issue that records the `simutil` methods casa-rs refuses.
const ISSUE: u32 = 1053;

fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("simutil", method, ISSUE)
}

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PySimutil {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (mstoimage, imagename, cleanmode, psfmode, cell, imsize, imcenter, interactive, niter, threshold, weighting, outertaper, pbcor, stokes, sourcefieldlist = None, modelimage = None, mask = None, dryrun = None), text_signature = "($self, mstoimage, imagename, cleanmode, psfmode, cell, imsize, imcenter, interactive, niter, threshold, weighting, outertaper, pbcor, stokes, sourcefieldlist='', modelimage='', mask=[], dryrun=False)")]
    fn imclean(
        &self,
        mstoimage: &Bound<'_, PyAny>,
        imagename: &Bound<'_, PyAny>,
        cleanmode: &Bound<'_, PyAny>,
        psfmode: &Bound<'_, PyAny>,
        cell: &Bound<'_, PyAny>,
        imsize: &Bound<'_, PyAny>,
        imcenter: &Bound<'_, PyAny>,
        interactive: &Bound<'_, PyAny>,
        niter: &Bound<'_, PyAny>,
        threshold: &Bound<'_, PyAny>,
        weighting: &Bound<'_, PyAny>,
        outertaper: &Bound<'_, PyAny>,
        pbcor: &Bound<'_, PyAny>,
        stokes: &Bound<'_, PyAny>,
        sourcefieldlist: Option<&Bound<'_, PyAny>>,
        modelimage: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dryrun: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("imclean")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (mstoimage, imagename, gridder, deconvolver, cell, imsize, imdirection, interactive, niter, threshold, weighting, outertaper, pbcor, stokes, modelimage = None, mask = None, dryrun = None), text_signature = "($self, mstoimage, imagename, gridder, deconvolver, cell, imsize, imdirection, interactive, niter, threshold, weighting, outertaper, pbcor, stokes, modelimage='', mask=[], dryrun=False)")]
    fn imtclean(
        &self,
        mstoimage: &Bound<'_, PyAny>,
        imagename: &Bound<'_, PyAny>,
        gridder: &Bound<'_, PyAny>,
        deconvolver: &Bound<'_, PyAny>,
        cell: &Bound<'_, PyAny>,
        imsize: &Bound<'_, PyAny>,
        imdirection: &Bound<'_, PyAny>,
        interactive: &Bound<'_, PyAny>,
        niter: &Bound<'_, PyAny>,
        threshold: &Bound<'_, PyAny>,
        weighting: &Bound<'_, PyAny>,
        outertaper: &Bound<'_, PyAny>,
        pbcor: &Bound<'_, PyAny>,
        stokes: &Bound<'_, PyAny>,
        modelimage: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dryrun: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("imtclean")
    }
}
