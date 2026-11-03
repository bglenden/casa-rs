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
    #[pyo3(signature = (x, y, tcos, tsin), text_signature = "($self, x, y, tcos, tsin)")]
    fn applyRotate(
        &self,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        tcos: &Bound<'_, PyAny>,
        tsin: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("applyRotate")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn closereport(&self) -> PyResult<Py<PyAny>> {
        stub("closereport")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (direction = None), text_signature = "($self, direction=None)")]
    fn dir_s2m(&self, direction: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("dir_s2m")
    }

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
    #[pyo3(signature = (width, height, angle, spacing, pb), text_signature = "($self, width, height, angle, spacing, pb)")]
    fn getBaseTriangularTiling(
        &self,
        width: &Bound<'_, PyAny>,
        height: &Bound<'_, PyAny>,
        angle: &Bound<'_, PyAny>,
        spacing: &Bound<'_, PyAny>,
        pb: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("getBaseTriangularTiling")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (datumcode, verbose = None), text_signature = "($self, datumcode, verbose=False)")]
    fn getdatum(
        &self,
        datumcode: &Bound<'_, PyAny>,
        verbose: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getdatum")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (width, height, angle, spacing, pb), text_signature = "($self, width, height, angle, spacing, pb)")]
    fn getShiftTriangularTiling(
        &self,
        width: &Bound<'_, PyAny>,
        height: &Bound<'_, PyAny>,
        angle: &Bound<'_, PyAny>,
        spacing: &Bound<'_, PyAny>,
        pb: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("getShiftTriangularTiling")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (width, height, angle, spacing), text_signature = "($self, width, height, angle, spacing)")]
    fn getTrianglePoints(
        &self,
        width: &Bound<'_, PyAny>,
        height: &Bound<'_, PyAny>,
        angle: &Bound<'_, PyAny>,
        spacing: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("getTrianglePoints")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (longlength, latlength, angle, spacing, pb), text_signature = "($self, longlength, latlength, angle, spacing, pb)")]
    fn getTriangularTiling(
        &self,
        longlength: &Bound<'_, PyAny>,
        latlength: &Bound<'_, PyAny>,
        angle: &Bound<'_, PyAny>,
        spacing: &Bound<'_, PyAny>,
        pb: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("getTriangularTiling")
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
    #[pyo3(signature = (image), text_signature = "($self, image)")]
    fn is4d(&self, image: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("is4d")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (num), text_signature = "($self, num)")]
    fn isEven(&self, num: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("isEven")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn isreport(&self) -> PyResult<Py<PyAny>> {
        stub("isreport")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (x, y, z, obsname), text_signature = "($self, x, y, z, obsname)")]
    fn itrf2locname(
        &self,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        z: &Bound<'_, PyAny>,
        obsname: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("itrf2locname")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (lat, longitude, alt, locx = None, locy = None, locz = None), text_signature = "($self, lat, longitude, alt, locx=0.0, locy=0.0, locz=0.0)")]
    fn locxyz2itrf(
        &self,
        lat: &Bound<'_, PyAny>,
        longitude: &Bound<'_, PyAny>,
        alt: &Bound<'_, PyAny>,
        locx: Option<&Bound<'_, PyAny>>,
        locy: Option<&Bound<'_, PyAny>>,
        locz: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("locxyz2itrf")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (lon, lat, elevation, datum), text_signature = "($self, lon, lat, elevation, datum)")]
    fn long2xyz(
        &self,
        lon: &Bound<'_, PyAny>,
        lat: &Bound<'_, PyAny>,
        elevation: &Bound<'_, PyAny>,
        datum: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("long2xyz")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (s, origin = None, priority = None), text_signature = "($self, s, origin=None, priority=None)")]
    fn msg(
        &self,
        s: &Bound<'_, PyAny>,
        origin: Option<&Bound<'_, PyAny>>,
        priority: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("msg")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (length, spacing, bs, npoints), text_signature = "($self, length, spacing, bs, npoints)")]
    fn needsFiller(
        &self,
        length: &Bound<'_, PyAny>,
        spacing: &Bound<'_, PyAny>,
        bs: &Bound<'_, PyAny>,
        npoints: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("needsFiller")
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
    #[pyo3(signature = (), text_signature = "($self)")]
    fn openreport(&self) -> PyResult<Py<PyAny>> {
        stub("openreport")
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
    #[pyo3(signature = (mysm, telescope, x, y, z, diam, padnames, posobs, mounttype = None), text_signature = "($self, mysm, telescope, x, y, z, diam, padnames, posobs, mounttype=None)")]
    fn setcfg(
        &self,
        mysm: &Bound<'_, PyAny>,
        telescope: &Bound<'_, PyAny>,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        z: &Bound<'_, PyAny>,
        diam: &Bound<'_, PyAny>,
        padnames: &Bound<'_, PyAny>,
        posobs: &Bound<'_, PyAny>,
        mounttype: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setcfg")
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

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (image), text_signature = "($self, image)")]
    fn spectral(&self, image: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("spectral")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (sf, orlim, er, esq, rf), text_signature = "($self, sf, orlim, er, esq, rf)")]
    fn tconpc(
        &self,
        sf: &Bound<'_, PyAny>,
        orlim: &Bound<'_, PyAny>,
        er: &Bound<'_, PyAny>,
        esq: &Bound<'_, PyAny>,
        rf: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("tconpc")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (n, e, eps, cm, fe, sf, so, r, v0, v2, v4, v6, r#fn, er, esq), text_signature = "($self, n, e, eps, cm, fe, sf, so, r, v0, v2, v4, v6, fn, er, esq)")]
    fn tmgeod(
        &self,
        n: &Bound<'_, PyAny>,
        e: &Bound<'_, PyAny>,
        eps: &Bound<'_, PyAny>,
        cm: &Bound<'_, PyAny>,
        fe: &Bound<'_, PyAny>,
        sf: &Bound<'_, PyAny>,
        so: &Bound<'_, PyAny>,
        r: &Bound<'_, PyAny>,
        v0: &Bound<'_, PyAny>,
        v2: &Bound<'_, PyAny>,
        v4: &Bound<'_, PyAny>,
        v6: &Bound<'_, PyAny>,
        r#fn: &Bound<'_, PyAny>,
        er: &Bound<'_, PyAny>,
        esq: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("tmgeod")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (east, north, zone, datum, nors), text_signature = "($self, east, north, zone, datum, nors)")]
    fn utm2long(
        &self,
        east: &Bound<'_, PyAny>,
        north: &Bound<'_, PyAny>,
        zone: &Bound<'_, PyAny>,
        datum: &Bound<'_, PyAny>,
        nors: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("utm2long")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (easting, northing, elevation, zone, datum, nors), text_signature = "($self, easting, northing, elevation, zone, datum, nors)")]
    fn utm2xyz(
        &self,
        easting: &Bound<'_, PyAny>,
        northing: &Bound<'_, PyAny>,
        elevation: &Bound<'_, PyAny>,
        zone: &Bound<'_, PyAny>,
        datum: &Bound<'_, PyAny>,
        nors: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("utm2xyz")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (x, y, z, datum), text_signature = "($self, x, y, z, datum)")]
    fn xyz2long(
        &self,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        z: &Bound<'_, PyAny>,
        datum: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("xyz2long")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (x, y, z, datum), text_signature = "($self, x, y, z, datum)")]
    fn xyz2long_old(
        &self,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        z: &Bound<'_, PyAny>,
        datum: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("xyz2long_old")
    }
}
