// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `image` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1027.

use pyo3::prelude::*;

use super::{PyImageTool, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyImageTool {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, direction = None, spectral = None, stokes = None, linear = None, tabular = None, overwrite = None, silent = None), text_signature = "($self, outfile='', direction=False, spectral=False, stokes='', linear=False, tabular=False, overwrite=False, silent=False)")]
    fn adddegaxes(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        direction: Option<&Bound<'_, PyAny>>,
        spectral: Option<&Bound<'_, PyAny>>,
        stokes: Option<&Bound<'_, PyAny>>,
        linear: Option<&Bound<'_, PyAny>>,
        tabular: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        silent: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("adddegaxes")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (r#type = None, pars = None, region = None, zero = None, seeds = None), text_signature = "($self, type='normal', pars=[0.0, 1.0], region={}, zero=False, seeds=[])")]
    fn addnoise(
        &self,
        r#type: Option<&Bound<'_, PyAny>>,
        pars: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        zero: Option<&Bound<'_, PyAny>>,
        seeds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("addnoise")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (channel = None, polarization = None), text_signature = "($self, channel=-1, polarization=-1)")]
    fn beamarea(
        &self,
        channel: Option<&Bound<'_, PyAny>>,
        polarization: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("beamarea")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (source = None, convolved = None), text_signature = "($self, source=[], convolved=[])")]
    fn beamforconvolvedsize(
        &self,
        source: Option<&Bound<'_, PyAny>>,
        convolved: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("beamforconvolvedsize")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (region = None), text_signature = "($self, region={})")]
    fn boundingbox(&self, region: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("boundingbox")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, region = None, mask = None, axis = None, width = None, drop = None, dmethod = None, overwrite = None, stretch = None), text_signature = "($self, outfile='', region={}, mask=[], axis=-1, width=2, drop=True, dmethod='copy', overwrite=False, stretch=False)")]
    fn boxcar(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        axis: Option<&Bound<'_, PyAny>>,
        width: Option<&Bound<'_, PyAny>>,
        drop: Option<&Bound<'_, PyAny>>,
        dmethod: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("boxcar")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn brightnessunit(&self) -> PyResult<Py<PyAny>> {
        stub("brightnessunit")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixels, verbose = None), text_signature = "($self, pixels, verbose=True)")]
    fn calc(
        &self,
        pixels: &Bound<'_, PyAny>,
        verbose: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("calc")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (mask = None, name = None, asdefault = None), text_signature = "($self, mask='', name='', asdefault=True)")]
    fn calcmask(
        &self,
        mask: Option<&Bound<'_, PyAny>>,
        name: Option<&Bound<'_, PyAny>>,
        asdefault: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("calcmask")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn close(&self) -> PyResult<Py<PyAny>> {
        stub("close")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (function = None, axes = None, outfile = None, region = None, r#box = None, chans = None, stokes = None, mask = None, overwrite = None, stretch = None), text_signature = "($self, function='', axes=[], outfile='', region='', box='', chans='', stokes='', mask='', overwrite=False, stretch=False)")]
    fn collapse(
        &self,
        function: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        r#box: Option<&Bound<'_, PyAny>>,
        chans: Option<&Bound<'_, PyAny>>,
        stokes: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("collapse")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn commonbeam(&self) -> PyResult<Py<PyAny>> {
        stub("commonbeam")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outline = None, outcont = None, region = None, channels = None, pol = None, fitorder = None, overwrite = None), text_signature = "($self, outline='', outcont='continuumsub.im', region={}, channels=[-1], pol='', fitorder=0, overwrite=False)")]
    fn continuumsub(
        &self,
        outline: Option<&Bound<'_, PyAny>>,
        outcont: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        channels: Option<&Bound<'_, PyAny>>,
        pol: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("continuumsub")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None, major = None, minor = None, r#type = None, topeak = None, channel = None, polarization = None), text_signature = "($self, value=[], major=[], minor=[], type='Gaussian', topeak=True, channel=-1, polarization=-1)")]
    fn convertflux(
        &self,
        value: Option<&Bound<'_, PyAny>>,
        major: Option<&Bound<'_, PyAny>>,
        minor: Option<&Bound<'_, PyAny>>,
        r#type: Option<&Bound<'_, PyAny>>,
        topeak: Option<&Bound<'_, PyAny>>,
        channel: Option<&Bound<'_, PyAny>>,
        polarization: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertflux")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, kernel = None, scale = None, region = None, mask = None, overwrite = None, stretch = None), text_signature = "($self, outfile='', kernel=[], scale=-1.0, region={}, mask='', overwrite=False, stretch=False)")]
    fn convolve(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        kernel: Option<&Bound<'_, PyAny>>,
        scale: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convolve")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, axes = None, r#type = None, major = None, minor = None, pa = None, scale = None, region = None, mask = None, overwrite = None, stretch = None, targetres = None, beam = None), text_signature = "($self, outfile='', axes=[0, 1], type='gaussian', major='0deg', minor='0deg', pa='0deg', scale=-1.0, region={}, mask='', overwrite=False, stretch=False, targetres=False, beam={})")]
    fn convolve2d(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        r#type: Option<&Bound<'_, PyAny>>,
        major: Option<&Bound<'_, PyAny>>,
        minor: Option<&Bound<'_, PyAny>>,
        pa: Option<&Bound<'_, PyAny>>,
        scale: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        targetres: Option<&Bound<'_, PyAny>>,
        beam: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convolve2d")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixel = None, dframe = None, sframe = None), text_signature = "($self, pixel=[-1.0], dframe='cl', sframe='cl')")]
    fn coordmeasures(
        &self,
        pixel: Option<&Bound<'_, PyAny>>,
        dframe: Option<&Bound<'_, PyAny>>,
        sframe: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("coordmeasures")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (axes = None), text_signature = "($self, axes=[-1])")]
    fn coordsys(&self, axes: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("coordsys")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, axes = None, overwrite = None, region = None, r#box = None, chans = None, stokes = None, mask = None, stretch = None, wantreturn = None), text_signature = "($self, outfile='', axes=[], overwrite=False, region={}, box='', chans='', stokes='', mask='', stretch=False, wantreturn=True)")]
    fn crop(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        r#box: Option<&Bound<'_, PyAny>>,
        chans: Option<&Bound<'_, PyAny>>,
        stokes: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        wantreturn: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("crop")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, axis = None, factor = None, method = None, region = None, mask = None, overwrite = None, stretch = None), text_signature = "($self, outfile='', axis=0, factor=1, method='copy', region='', mask='', overwrite=False, stretch=False)")]
    fn decimate(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        axis: Option<&Bound<'_, PyAny>>,
        factor: Option<&Bound<'_, PyAny>>,
        method: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("decimate")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (region = None, mask = None, simple = None, threshold = None, ncontour = None, minrange = None, naxis = None, fit = None, maxrms = None, maxretry = None, maxiter = None, convcriteria = None, stretch = None), text_signature = "($self, region={}, mask='', simple=False, threshold=-1.0, ncontour=11, minrange=1, naxis=2, fit=True, maxrms=-1.0, maxretry=-1, maxiter=256, convcriteria=0.0001, stretch=False)")]
    fn decompose(
        &self,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        simple: Option<&Bound<'_, PyAny>>,
        threshold: Option<&Bound<'_, PyAny>>,
        ncontour: Option<&Bound<'_, PyAny>>,
        minrange: Option<&Bound<'_, PyAny>>,
        naxis: Option<&Bound<'_, PyAny>>,
        fit: Option<&Bound<'_, PyAny>>,
        maxrms: Option<&Bound<'_, PyAny>>,
        maxretry: Option<&Bound<'_, PyAny>>,
        maxiter: Option<&Bound<'_, PyAny>>,
        convcriteria: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("decompose")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (complist, channel = None, polarization = None), text_signature = "($self, complist, channel=-1, polarization=-1)")]
    fn deconvolvecomponentlist(
        &self,
        complist: &Bound<'_, PyAny>,
        channel: Option<&Bound<'_, PyAny>>,
        polarization: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("deconvolvecomponentlist")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (source = None, beam = None), text_signature = "($self, source=[], beam=[])")]
    fn deconvolvefrombeam(
        &self,
        source: Option<&Bound<'_, PyAny>>,
        beam: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("deconvolvefrombeam")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, region = None, mask = None, overwrite = None, stretch = None, grid = None, anchor = None, xlength = None, ylength = None, interp = None, stattype = None, statalg = None, zscore = None, maxiter = None), text_signature = "($self, outfile='', region='', mask='', overwrite=False, stretch=False, grid=[1, 1], anchor='ref', xlength='1pix', ylength='1pix', interp='cubic', stattype='sigma', statalg='classic', zscore=-1.0, maxiter=-1)")]
    fn deviation(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        grid: Option<&Bound<'_, PyAny>>,
        anchor: Option<&Bound<'_, PyAny>>,
        xlength: Option<&Bound<'_, PyAny>>,
        ylength: Option<&Bound<'_, PyAny>>,
        interp: Option<&Bound<'_, PyAny>>,
        stattype: Option<&Bound<'_, PyAny>>,
        statalg: Option<&Bound<'_, PyAny>>,
        zscore: Option<&Bound<'_, PyAny>>,
        maxiter: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("deviation")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (enable = None), text_signature = "($self, enable=True)")]
    fn dohistory(&self, enable: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("dohistory")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (remove = None, verbose = None), text_signature = "($self, remove=False, verbose=True)")]
    fn done(
        &self,
        remove: Option<&Bound<'_, PyAny>>,
        verbose: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("done")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (real = None, imag = None, amp = None, phase = None, axes = None, region = None, mask = None, stretch = None, complex = None), text_signature = "($self, real='', imag='', amp='', phase='', axes=[-1], region={}, mask='', stretch=False, complex='')")]
    fn fft(
        &self,
        real: Option<&Bound<'_, PyAny>>,
        imag: Option<&Bound<'_, PyAny>>,
        amp: Option<&Bound<'_, PyAny>>,
        phase: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        complex: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fft")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (nmax = None, cutoff = None, region = None, mask = None, point = None, width = None, negfind = None), text_signature = "($self, nmax=20, cutoff=0.1, region={}, mask='', point=True, width=5, negfind=False)")]
    fn findsources(
        &self,
        nmax: Option<&Bound<'_, PyAny>>,
        cutoff: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        point: Option<&Bound<'_, PyAny>>,
        width: Option<&Bound<'_, PyAny>>,
        negfind: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("findsources")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (r#box = None, region = None, chans = None, stokes = None, mask = None, includepix = None, excludepix = None, residual = None, model = None, estimates = None, logfile = None, append = None, newestimates = None, complist = None, overwrite = None, dooff = None, offset = None, fixoffset = None, stretch = None, rms = None, noisefwhm = None, summary = None), text_signature = "($self, box='', region=[], chans=[], stokes='', mask='', includepix=[-1.0], excludepix=[-1.0], residual='', model='', estimates='', logfile='', append=True, newestimates='', complist='', overwrite=False, dooff=False, offset=0.0, fixoffset=False, stretch=False, rms='', noisefwhm='', summary='')")]
    fn fitcomponents(
        &self,
        r#box: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        chans: Option<&Bound<'_, PyAny>>,
        stokes: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        includepix: Option<&Bound<'_, PyAny>>,
        excludepix: Option<&Bound<'_, PyAny>>,
        residual: Option<&Bound<'_, PyAny>>,
        model: Option<&Bound<'_, PyAny>>,
        estimates: Option<&Bound<'_, PyAny>>,
        logfile: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        newestimates: Option<&Bound<'_, PyAny>>,
        complist: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        dooff: Option<&Bound<'_, PyAny>>,
        offset: Option<&Bound<'_, PyAny>>,
        fixoffset: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        rms: Option<&Bound<'_, PyAny>>,
        noisefwhm: Option<&Bound<'_, PyAny>>,
        summary: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fitcomponents")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (r#box = None, region = None, chans = None, stokes = None, axis = None, mask = None, ngauss = None, poly = None, estimates = None, minpts = None, multifit = None, model = None, residual = None, amp = None, amperr = None, center = None, centererr = None, fwhm = None, fwhmerr = None, integral = None, integralerr = None, stretch = None, logresults = None, pampest = None, pcenterest = None, pfwhmest = None, pfix = None, gmncomps = None, gmampcon = None, gmcentercon = None, gmfwhmcon = None, gmampest = None, gmcenterest = None, gmfwhmest = None, gmfix = None, spxtype = None, spxest = None, spxfix = None, div = None, spxsol = None, spxerr = None, logfile = None, append = None, pfunc = None, goodamprange = None, goodcenterrange = None, goodfwhmrange = None, sigma = None, outsigma = None, planes = None), text_signature = "($self, box='', region='', chans='', stokes='', axis=-1, mask='', ngauss=1, poly=-1, estimates='', minpts=1, multifit=False, model='', residual='', amp='', amperr='', center='', centererr='', fwhm='', fwhmerr='', integral='', integralerr='', stretch=False, logresults=True, pampest=[], pcenterest=[], pfwhmest=[], pfix='', gmncomps=0, gmampcon=[], gmcentercon=[], gmfwhmcon=[], gmampest=[0.0], gmcenterest=[0.0], gmfwhmest=[0.0], gmfix='', spxtype='', spxest=[], spxfix=[], div=[], spxsol='', spxerr='', logfile='', append=True, pfunc='', goodamprange=[0.0], goodcenterrange=[0.0], goodfwhmrange=[0.0], sigma='', outsigma='', planes=[])")]
    fn fitprofile(
        &self,
        r#box: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        chans: Option<&Bound<'_, PyAny>>,
        stokes: Option<&Bound<'_, PyAny>>,
        axis: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        ngauss: Option<&Bound<'_, PyAny>>,
        poly: Option<&Bound<'_, PyAny>>,
        estimates: Option<&Bound<'_, PyAny>>,
        minpts: Option<&Bound<'_, PyAny>>,
        multifit: Option<&Bound<'_, PyAny>>,
        model: Option<&Bound<'_, PyAny>>,
        residual: Option<&Bound<'_, PyAny>>,
        amp: Option<&Bound<'_, PyAny>>,
        amperr: Option<&Bound<'_, PyAny>>,
        center: Option<&Bound<'_, PyAny>>,
        centererr: Option<&Bound<'_, PyAny>>,
        fwhm: Option<&Bound<'_, PyAny>>,
        fwhmerr: Option<&Bound<'_, PyAny>>,
        integral: Option<&Bound<'_, PyAny>>,
        integralerr: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        logresults: Option<&Bound<'_, PyAny>>,
        pampest: Option<&Bound<'_, PyAny>>,
        pcenterest: Option<&Bound<'_, PyAny>>,
        pfwhmest: Option<&Bound<'_, PyAny>>,
        pfix: Option<&Bound<'_, PyAny>>,
        gmncomps: Option<&Bound<'_, PyAny>>,
        gmampcon: Option<&Bound<'_, PyAny>>,
        gmcentercon: Option<&Bound<'_, PyAny>>,
        gmfwhmcon: Option<&Bound<'_, PyAny>>,
        gmampest: Option<&Bound<'_, PyAny>>,
        gmcenterest: Option<&Bound<'_, PyAny>>,
        gmfwhmest: Option<&Bound<'_, PyAny>>,
        gmfix: Option<&Bound<'_, PyAny>>,
        spxtype: Option<&Bound<'_, PyAny>>,
        spxest: Option<&Bound<'_, PyAny>>,
        spxfix: Option<&Bound<'_, PyAny>>,
        div: Option<&Bound<'_, PyAny>>,
        spxsol: Option<&Bound<'_, PyAny>>,
        spxerr: Option<&Bound<'_, PyAny>>,
        logfile: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        pfunc: Option<&Bound<'_, PyAny>>,
        goodamprange: Option<&Bound<'_, PyAny>>,
        goodcenterrange: Option<&Bound<'_, PyAny>>,
        goodfwhmrange: Option<&Bound<'_, PyAny>>,
        sigma: Option<&Bound<'_, PyAny>>,
        outsigma: Option<&Bound<'_, PyAny>>,
        planes: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fitprofile")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (retstr = None, exclude = None), text_signature = "($self, retstr=False, exclude=[])")]
    fn fitsheader(
        &self,
        retstr: Option<&Bound<'_, PyAny>>,
        exclude: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fitsheader")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, pixels = None, csys = None, linear = None, overwrite = None, log = None, r#type = None), text_signature = "($self, outfile='', pixels=[], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn fromarray(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        pixels: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyAny>>,
        linear: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
        r#type: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromarray")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, shape = None, cl = None, csys = None, overwrite = None, log = None, cache = None), text_signature = "($self, outfile='', shape=[], cl='', csys={}, overwrite=False, log=True, cache=True)")]
    fn fromcomplist(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
        cl: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
        cache: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromcomplist")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, infile = None, whichrep = None, whichhdu = None, zeroblanks = None, overwrite = None), text_signature = "($self, outfile='', infile='', whichrep=0, whichhdu=0, zeroblanks=False, overwrite=False)")]
    fn fromfits(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        infile: Option<&Bound<'_, PyAny>>,
        whichrep: Option<&Bound<'_, PyAny>>,
        whichhdu: Option<&Bound<'_, PyAny>>,
        zeroblanks: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromfits")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, infile = None, region = None, mask = None, dropdeg = None, overwrite = None), text_signature = "($self, outfile='', infile='', region=[], mask='', dropdeg=False, overwrite=False)")]
    fn fromimage(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        infile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromimage")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (record, outfile = None), text_signature = "($self, record, outfile='')")]
    fn fromrecord(
        &self,
        record: &Bound<'_, PyAny>,
        outfile: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromrecord")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, shape = None, csys = None, linear = None, overwrite = None, log = None, r#type = None), text_signature = "($self, outfile='', shape=[0], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn fromshape(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyAny>>,
        linear: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
        r#type: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromshape")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (blc = None, trc = None, inc = None, axes = None, list = None, dropdeg = None, getmask = None), text_signature = "($self, blc=[-1], trc=[-1], inc=[1], axes=[-1], list=False, dropdeg=False, getmask=False)")]
    fn getchunk(
        &self,
        blc: Option<&Bound<'_, PyAny>>,
        trc: Option<&Bound<'_, PyAny>>,
        inc: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        getmask: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getchunk")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (axis = None, function = None, region = None, mask = None, unit = None, stretch = None, spectype = None, restfreq = None, frame = None, logfile = None), text_signature = "($self, axis=-1, function='mean', region={}, mask='', unit='', stretch=False, spectype='default', restfreq='', frame='', logfile='')")]
    fn getprofile(
        &self,
        axis: Option<&Bound<'_, PyAny>>,
        function: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        unit: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        spectype: Option<&Bound<'_, PyAny>>,
        restfreq: Option<&Bound<'_, PyAny>>,
        frame: Option<&Bound<'_, PyAny>>,
        logfile: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getprofile")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (region = None, axes = None, mask = None, list = None, dropdeg = None, getmask = None, stretch = None), text_signature = "($self, region={}, axes=[-1], mask='', list=False, dropdeg=False, getmask=False, stretch=False)")]
    fn getregion(
        &self,
        region: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        getmask: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getregion")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (x, y, axes = None, coord = None, npts = None, method = None), text_signature = "($self, x, y, axes=[0, 1], coord=[-1], npts=0, method='linear')")]
    fn getslice(
        &self,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        axes: Option<&Bound<'_, PyAny>>,
        coord: Option<&Bound<'_, PyAny>>,
        npts: Option<&Bound<'_, PyAny>>,
        method: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getslice")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, region = None, mask = None, axis = None, drop = None, overwrite = None, stretch = None, dmethod = None), text_signature = "($self, outfile='', region={}, mask='', axis=-10, drop=True, overwrite=False, stretch=False, dmethod='copy')")]
    fn hanning(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        axis: Option<&Bound<'_, PyAny>>,
        drop: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        dmethod: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("hanning")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn haslock(&self) -> PyResult<Py<PyAny>> {
        stub("haslock")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (axes = None, region = None, mask = None, nbins = None, includepix = None, cumu = None, log = None, stretch = None), text_signature = "($self, axes=[-1], region={}, mask='', nbins=25, includepix=[-1.0], cumu=False, log=False, stretch=False)")]
    fn histograms(
        &self,
        axes: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        nbins: Option<&Bound<'_, PyAny>>,
        includepix: Option<&Bound<'_, PyAny>>,
        cumu: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("histograms")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (list = None), text_signature = "($self, list=True)")]
    fn history(&self, list: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("history")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, pixels = None, overwrite = None, imagemd = None, prec = None), text_signature = "($self, outfile='', pixels='', overwrite=False, imagemd='', prec='float')")]
    fn imagecalc(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        pixels: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        imagemd: Option<&Bound<'_, PyAny>>,
        prec: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("imagecalc")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, infiles = None, axis = None, relax = None, tempclose = None, overwrite = None, reorder = None, mode = None), text_signature = "($self, outfile='', infiles=[], axis=-1, relax=False, tempclose=True, overwrite=False, reorder=False, mode='paged')")]
    fn imageconcat(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        infiles: Option<&Bound<'_, PyAny>>,
        axis: Option<&Bound<'_, PyAny>>,
        relax: Option<&Bound<'_, PyAny>>,
        tempclose: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        reorder: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("imageconcat")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (infile = None, region = None, locate = None, verbose = None), text_signature = "($self, infile='', region={}, locate=[-1.0], verbose=False)")]
    fn insert(
        &self,
        infile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        locate: Option<&Bound<'_, PyAny>>,
        verbose: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("insert")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (other), text_signature = "($self, other)")]
    fn isconform(&self, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("isconform")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn isopen(&self) -> PyResult<Py<PyAny>> {
        stub("isopen")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn ispersistent(&self) -> PyResult<Py<PyAny>> {
        stub("ispersistent")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (writelock = None, nattempts = None), text_signature = "($self, writelock=False, nattempts=0)")]
    fn lock(
        &self,
        writelock: Option<&Bound<'_, PyAny>>,
        nattempts: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("lock")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (v = None, shape = None), text_signature = "($self, v=0.0, shape=[0])")]
    fn makearray(
        &self,
        v: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("makearray")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile, imag, region = None, overwrite = None), text_signature = "($self, outfile, imag, region={}, overwrite=False)")]
    fn makecomplex(
        &self,
        outfile: &Bound<'_, PyAny>,
        imag: &Bound<'_, PyAny>,
        region: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("makecomplex")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, overwrite = None), text_signature = "($self, outfile='', overwrite=False)")]
    fn maketestimage(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("maketestimage")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (op = None, name = None), text_signature = "($self, op='default', name=[])")]
    fn maskhandler(
        &self,
        op: Option<&Bound<'_, PyAny>>,
        name: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("maskhandler")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (region = None, point = None, width = None, negfind = None, list = None), text_signature = "($self, region={}, point=True, width=5, negfind=False, list=True)")]
    fn maxfit(
        &self,
        region: Option<&Bound<'_, PyAny>>,
        point: Option<&Bound<'_, PyAny>>,
        width: Option<&Bound<'_, PyAny>>,
        negfind: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("maxfit")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn miscinfo(&self) -> PyResult<Py<PyAny>> {
        stub("miscinfo")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (model, region = None, mask = None, subtract = None, list = None, stretch = None), text_signature = "($self, model, region={}, mask='', subtract=True, list=True, stretch=False)")]
    fn modify(
        &self,
        model: &Bound<'_, PyAny>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        subtract: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("modify")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (moments = None, axis = None, region = None, mask = None, method = None, smoothaxes = None, smoothtypes = None, smoothwidths = None, includepix = None, excludepix = None, peaksnr = None, stddev = None, doppler = None, outfile = None, smoothout = None, overwrite = None, drop = None, stretch = None), text_signature = "($self, moments=[0], axis=-10, region={}, mask='', method=[], smoothaxes=[-1], smoothtypes=[], smoothwidths=[0.0], includepix=[-1.0], excludepix=[-1.0], peaksnr=3.0, stddev=0.0, doppler='RADIO', outfile='', smoothout='', overwrite=False, drop=True, stretch=False)")]
    fn moments(
        &self,
        moments: Option<&Bound<'_, PyAny>>,
        axis: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        method: Option<&Bound<'_, PyAny>>,
        smoothaxes: Option<&Bound<'_, PyAny>>,
        smoothtypes: Option<&Bound<'_, PyAny>>,
        smoothwidths: Option<&Bound<'_, PyAny>>,
        includepix: Option<&Bound<'_, PyAny>>,
        excludepix: Option<&Bound<'_, PyAny>>,
        peaksnr: Option<&Bound<'_, PyAny>>,
        stddev: Option<&Bound<'_, PyAny>>,
        doppler: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        smoothout: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        drop: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("moments")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (strippath = None), text_signature = "($self, strippath=False)")]
    fn name(&self, strippath: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("name")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (infile), text_signature = "($self, infile)")]
    fn newimage(&self, infile: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("newimage")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, pixels = None, csys = None, linear = None, overwrite = None, log = None, r#type = None), text_signature = "($self, outfile='', pixels=[], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn newimagefromarray(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        pixels: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyAny>>,
        linear: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
        r#type: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("newimagefromarray")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (infile), text_signature = "($self, infile)")]
    fn newimagefromfile(&self, infile: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("newimagefromfile")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, infile = None, whichrep = None, whichhdu = None, zeroblanks = None, overwrite = None), text_signature = "($self, outfile='', infile='', whichrep=0, whichhdu=0, zeroblanks=False, overwrite=False)")]
    fn newimagefromfits(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        infile: Option<&Bound<'_, PyAny>>,
        whichrep: Option<&Bound<'_, PyAny>>,
        whichhdu: Option<&Bound<'_, PyAny>>,
        zeroblanks: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("newimagefromfits")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (infile = None, outfile = None, region = None, mask = None, dropdeg = None, overwrite = None), text_signature = "($self, infile='', outfile='', region={}, mask='', dropdeg=False, overwrite=False)")]
    fn newimagefromimage(
        &self,
        infile: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("newimagefromimage")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, shape = None, csys = None, linear = None, overwrite = None, log = None, r#type = None), text_signature = "($self, outfile='', shape=[0], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn newimagefromshape(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyAny>>,
        linear: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
        r#type: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("newimagefromshape")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (infile, cache = None), text_signature = "($self, infile, cache=True)")]
    fn open(
        &self,
        infile: &Bound<'_, PyAny>,
        cache: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("open")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, npixels = None, value = None, padmask = None, overwrite = None, region = None, r#box = None, chans = None, stokes = None, mask = None, stretch = None, wantreturn = None), text_signature = "($self, outfile='', npixels=1, value=0.0, padmask=False, overwrite=False, region={}, box='', chans='', stokes='', mask='', stretch=False, wantreturn=True)")]
    fn pad(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        npixels: Option<&Bound<'_, PyAny>>,
        value: Option<&Bound<'_, PyAny>>,
        padmask: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        r#box: Option<&Bound<'_, PyAny>>,
        chans: Option<&Bound<'_, PyAny>>,
        stokes: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        wantreturn: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("pad")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pbimage = None, outfile = None, overwrite = None, r#box = None, region = None, chans = None, stokes = None, mask = None, mode = None, cutoff = None, stretch = None), text_signature = "($self, pbimage='', outfile='', overwrite=False, box='', region={}, chans='', stokes='', mask='', mode='divide', cutoff=-1.0, stretch=False)")]
    fn pbcor(
        &self,
        pbimage: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        r#box: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        chans: Option<&Bound<'_, PyAny>>,
        stokes: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
        cutoff: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("pbcor")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn pixeltype(&self) -> PyResult<Py<PyAny>> {
        stub("pixeltype")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixel = None), text_signature = "($self, pixel=[-1])")]
    fn pixelvalue(&self, pixel: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("pixelvalue")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixels = None, blc = None, inc = None, list = None, locking = None, replicate = None), text_signature = "($self, pixels=[], blc=[-1], inc=[1], list=False, locking=True, replicate=False)")]
    fn putchunk(
        &self,
        pixels: Option<&Bound<'_, PyAny>>,
        blc: Option<&Bound<'_, PyAny>>,
        inc: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        locking: Option<&Bound<'_, PyAny>>,
        replicate: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putchunk")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixels = None, pixelmask = None, region = None, list = None, usemask = None, locking = None, replicate = None), text_signature = "($self, pixels=[], pixelmask=[], region={}, list=False, usemask=True, locking=True, replicate=False)")]
    fn putregion(
        &self,
        pixels: Option<&Bound<'_, PyAny>>,
        pixelmask: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        usemask: Option<&Bound<'_, PyAny>>,
        locking: Option<&Bound<'_, PyAny>>,
        replicate: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putregion")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, start = None, end = None, center = None, length = None, pa = None, width = None, unit = None, overwrite = None, region = None, chans = None, stokes = None, mask = None, stretch = None, wantreturn = None), text_signature = "($self, outfile='', start=[], end=[], center=[], length=[], pa=[], width=1, unit='arcsec', overwrite=False, region={}, chans='', stokes='', mask='', stretch=False, wantreturn=True)")]
    fn pv(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        center: Option<&Bound<'_, PyAny>>,
        length: Option<&Bound<'_, PyAny>>,
        pa: Option<&Bound<'_, PyAny>>,
        width: Option<&Bound<'_, PyAny>>,
        unit: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        chans: Option<&Bound<'_, PyAny>>,
        stokes: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        wantreturn: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("pv")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile, bin, region = None, mask = None, dropdeg = None, overwrite = None, stretch = None, crop = None), text_signature = "($self, outfile, bin, region='', mask='', dropdeg=False, overwrite=False, stretch=False, crop=False)")]
    fn rebin(
        &self,
        outfile: &Bound<'_, PyAny>,
        bin: &Bound<'_, PyAny>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        crop: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("rebin")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, shape = None, csys = None, axes = None, region = None, mask = None, method = None, decimate = None, replicate = None, doref = None, dropdeg = None, overwrite = None, force = None, asvelocity = None, stretch = None), text_signature = "($self, outfile='', shape=[-1], csys={}, axes=[-1], region={}, mask='', method='linear', decimate=10, replicate=False, doref=True, dropdeg=False, overwrite=False, force=False, asvelocity=False, stretch=False)")]
    fn regrid(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        method: Option<&Bound<'_, PyAny>>,
        decimate: Option<&Bound<'_, PyAny>>,
        replicate: Option<&Bound<'_, PyAny>>,
        doref: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        force: Option<&Bound<'_, PyAny>>,
        asvelocity: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("regrid")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (done = None, verbose = None), text_signature = "($self, done=False, verbose=True)")]
    fn remove(
        &self,
        done: Option<&Bound<'_, PyAny>>,
        verbose: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("remove")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (file), text_signature = "($self, file)")]
    fn removefile(&self, file: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("removefile")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (name, overwrite = None), text_signature = "($self, name, overwrite=False)")]
    fn rename(
        &self,
        name: &Bound<'_, PyAny>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("rename")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixels = None, region = None, mask = None, update = None, list = None, stretch = None), text_signature = "($self, pixels=[], region={}, mask='', update=False, list=False, stretch=False)")]
    fn replacemaskedpixels(
        &self,
        pixels: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        update: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("replacemaskedpixels")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (channel = None, polarization = None, mbret = None), text_signature = "($self, channel=-1, polarization=-1, mbret='list')")]
    fn restoringbeam(
        &self,
        channel: Option<&Bound<'_, PyAny>>,
        polarization: Option<&Bound<'_, PyAny>>,
        mbret: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("restoringbeam")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, shape = None, pa = None, region = None, mask = None, method = None, decimate = None, replicate = None, dropdeg = None, overwrite = None, stretch = None), text_signature = "($self, outfile='', shape=[-1], pa=[], region={}, mask='', method='cubic', decimate=0, replicate=False, dropdeg=False, overwrite=False, stretch=False)")]
    fn rotate(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
        pa: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        method: Option<&Bound<'_, PyAny>>,
        decimate: Option<&Bound<'_, PyAny>>,
        replicate: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("rotate")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (angle = None), text_signature = "($self, angle=[])")]
    fn rotatebeam(&self, angle: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("rotatebeam")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, axes = None, types = None, widths = None, scale = None, region = None, mask = None, overwrite = None, stretch = None), text_signature = "($self, outfile='', axes=[-1], types=[''], widths=[], scale=-1.0, region={}, mask='', overwrite=False, stretch=False)")]
    fn sepconvolve(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        types: Option<&Bound<'_, PyAny>>,
        widths: Option<&Bound<'_, PyAny>>,
        scale: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("sepconvolve")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixels = None, pixelmask = None, region = None, list = None), text_signature = "($self, pixels=[], pixelmask=-1, region={}, list=False)")]
    fn set(
        &self,
        pixels: Option<&Bound<'_, PyAny>>,
        pixelmask: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("set")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (unit), text_signature = "($self, unit)")]
    fn setbrightnessunit(&self, unit: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("setbrightnessunit")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (csys), text_signature = "($self, csys)")]
    fn setcoordsys(&self, csys: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("setcoordsys")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (origin = None, history = None), text_signature = "($self, origin='', history=[])")]
    fn sethistory(
        &self,
        origin: Option<&Bound<'_, PyAny>>,
        history: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("sethistory")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (info), text_signature = "($self, info)")]
    fn setmiscinfo(&self, info: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("setmiscinfo")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (major = None, minor = None, pa = None, beam = None, remove = None, log = None, channel = None, polarization = None, imagename = None), text_signature = "($self, major=[], minor=[], pa='Not specified', beam={}, remove=False, log=True, channel=-1, polarization=-1, imagename='')")]
    fn setrestoringbeam(
        &self,
        major: Option<&Bound<'_, PyAny>>,
        minor: Option<&Bound<'_, PyAny>>,
        pa: Option<&Bound<'_, PyAny>>,
        beam: Option<&Bound<'_, PyAny>>,
        remove: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
        channel: Option<&Bound<'_, PyAny>>,
        polarization: Option<&Bound<'_, PyAny>>,
        imagename: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setrestoringbeam")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn shape(&self) -> PyResult<Py<PyAny>> {
        stub("shape")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (axes = None, region = None, mask = None, includepix = None, excludepix = None, list = None, force = None, disk = None, robust = None, verbose = None, stretch = None, logfile = None, append = None, algorithm = None, fence = None, center = None, lside = None, zscore = None, maxiter = None, clmethod = None, niter = None), text_signature = "($self, axes=[-1], region='', mask='', includepix=[-1.0], excludepix=[-1.0], list=False, force=False, disk=False, robust=False, verbose=False, stretch=False, logfile='', append=True, algorithm='classic', fence=-1.0, center='mean', lside=True, zscore=-1.0, maxiter=-1, clmethod='auto', niter=3)")]
    fn statistics(
        &self,
        axes: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        includepix: Option<&Bound<'_, PyAny>>,
        excludepix: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        force: Option<&Bound<'_, PyAny>>,
        disk: Option<&Bound<'_, PyAny>>,
        robust: Option<&Bound<'_, PyAny>>,
        verbose: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        logfile: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        algorithm: Option<&Bound<'_, PyAny>>,
        fence: Option<&Bound<'_, PyAny>>,
        center: Option<&Bound<'_, PyAny>>,
        lside: Option<&Bound<'_, PyAny>>,
        zscore: Option<&Bound<'_, PyAny>>,
        maxiter: Option<&Bound<'_, PyAny>>,
        clmethod: Option<&Bound<'_, PyAny>>,
        niter: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("statistics")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, region = None, mask = None, dropdeg = None, overwrite = None, list = None, stretch = None, wantreturn = None, keepaxes = None), text_signature = "($self, outfile='', region='', mask='', dropdeg=False, overwrite=False, list=True, stretch=False, wantreturn=True, keepaxes=[])")]
    fn subimage(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        wantreturn: Option<&Bound<'_, PyAny>>,
        keepaxes: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("subimage")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (doppler = None, list = None, pixelorder = None, verbose = None), text_signature = "($self, doppler='RADIO', list=True, pixelorder=True, verbose=False)")]
    fn summary(
        &self,
        doppler: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
        pixelorder: Option<&Bound<'_, PyAny>>,
        verbose: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("summary")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, velocity = None, optical = None, bitpix = None, minpix = None, maxpix = None, region = None, mask = None, overwrite = None, dropdeg = None, deglast = None, dropstokes = None, stokeslast = None, wavelength = None, airwavelength = None, stretch = None, history = None), text_signature = "($self, outfile='', velocity=False, optical=True, bitpix=-32, minpix=1.0, maxpix=-1.0, region={}, mask='', overwrite=False, dropdeg=False, deglast=False, dropstokes=False, stokeslast=True, wavelength=False, airwavelength=False, stretch=False, history=True)")]
    fn tofits(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        velocity: Option<&Bound<'_, PyAny>>,
        optical: Option<&Bound<'_, PyAny>>,
        bitpix: Option<&Bound<'_, PyAny>>,
        minpix: Option<&Bound<'_, PyAny>>,
        maxpix: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        dropdeg: Option<&Bound<'_, PyAny>>,
        deglast: Option<&Bound<'_, PyAny>>,
        dropstokes: Option<&Bound<'_, PyAny>>,
        stokeslast: Option<&Bound<'_, PyAny>>,
        wavelength: Option<&Bound<'_, PyAny>>,
        airwavelength: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
        history: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("tofits")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None), text_signature = "($self, value=[])")]
    fn topixel(&self, value: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("topixel")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn torecord(&self) -> PyResult<Py<PyAny>> {
        stub("torecord")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None, format = None, dovelocity = None), text_signature = "($self, value=[], format='n', dovelocity=True)")]
    fn toworld(
        &self,
        value: Option<&Bound<'_, PyAny>>,
        format: Option<&Bound<'_, PyAny>>,
        dovelocity: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("toworld")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, order = None), text_signature = "($self, outfile='', order=[])")]
    fn transpose(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        order: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("transpose")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, region = None, mask = None, axes = None, method = None, overwrite = None, stretch = None), text_signature = "($self, outfile='', region={}, mask='', axes=[-1], method='structurefunction', overwrite=False, stretch=False)")]
    fn twopointcorrelation(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        axes: Option<&Bound<'_, PyAny>>,
        method: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        stretch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("twopointcorrelation")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn r#type(&self) -> PyResult<Py<PyAny>> {
        stub("type")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn unlock(&self) -> PyResult<Py<PyAny>> {
        stub("unlock")
    }
}
