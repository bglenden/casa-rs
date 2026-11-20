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
    #[pyo3(signature = (value = None), text_signature = "($self, value=[])")]
    fn topixel(&self, value: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("topixel")
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
}
