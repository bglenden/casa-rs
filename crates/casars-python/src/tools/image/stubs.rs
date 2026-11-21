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
