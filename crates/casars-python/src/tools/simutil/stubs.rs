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
    #[pyo3(signature = (show = None, filename = None), text_signature = "($self, show=True, filename='')")]
    fn endfig(
        &self,
        show: Option<&Bound<'_, PyAny>>,
        filename: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("endfig")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (date, usehourangle = None, direction = None, telescope = None, ms = None, cofa = None), text_signature = "($self, date, usehourangle=True, direction=None, telescope=None, ms=None, cofa=None)")]
    fn ephemeris(
        &self,
        date: &Bound<'_, PyAny>,
        usehourangle: Option<&Bound<'_, PyAny>>,
        direction: Option<&Bound<'_, PyAny>>,
        telescope: Option<&Bound<'_, PyAny>>,
        ms: Option<&Bound<'_, PyAny>>,
        cofa: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("ephemeris")
    }

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

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (multi = None, show = None), text_signature = "($self, multi=0, show=True)")]
    fn newfig(
        &self,
        multi: Option<&Bound<'_, PyAny>>,
        show: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("newfig")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nextfig(&self) -> PyResult<Py<PyAny>> {
        stub("nextfig")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (x, y, z, d, name), text_signature = "($self, x, y, z, d, name)")]
    fn plotants(
        &self,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        z: &Bound<'_, PyAny>,
        d: &Bound<'_, PyAny>,
        name: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("plotants")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (freq, bandwidth, etime, elevation, pwv = None, telescope = None, diam = None, nant = None, antennalist = None, doimnoise = None, integration = None, debug = None, method = None, tau0 = None, t_sky = None), text_signature = "($self, freq, bandwidth, etime, elevation, pwv=None, telescope=None, diam=None, nant=None, antennalist=None, doimnoise=None, integration=None, debug=None, method='tsys-atm', tau0=None, t_sky=None)")]
    fn sensitivity(
        &self,
        freq: &Bound<'_, PyAny>,
        bandwidth: &Bound<'_, PyAny>,
        etime: &Bound<'_, PyAny>,
        elevation: &Bound<'_, PyAny>,
        pwv: Option<&Bound<'_, PyAny>>,
        telescope: Option<&Bound<'_, PyAny>>,
        diam: Option<&Bound<'_, PyAny>>,
        nant: Option<&Bound<'_, PyAny>>,
        antennalist: Option<&Bound<'_, PyAny>>,
        doimnoise: Option<&Bound<'_, PyAny>>,
        integration: Option<&Bound<'_, PyAny>>,
        debug: Option<&Bound<'_, PyAny>>,
        method: Option<&Bound<'_, PyAny>>,
        tau0: Option<&Bound<'_, PyAny>>,
        t_sky: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("sensitivity")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (beam = None, cell = None, convsupport = None, sampling = None), text_signature = "($self, beam='', cell='', convsupport=-1, sampling='')")]
    fn sfBeam1d(
        &self,
        beam: Option<&Bound<'_, PyAny>>,
        cell: Option<&Bound<'_, PyAny>>,
        convsupport: Option<&Bound<'_, PyAny>>,
        sampling: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("sfBeam1d")
    }
}
