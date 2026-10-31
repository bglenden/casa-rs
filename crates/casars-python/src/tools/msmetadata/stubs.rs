// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `msmetadata` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1029.

use pyo3::prelude::*;

use super::{PyMsMetadata, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyMsMetadata {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (chavg = None, fdm = None, sqld = None, tdm = None, wvr = None, complement = None), text_signature = "($self, chavg=False, fdm=False, sqld=False, tdm=False, wvr=False, complement=False)")]
    fn almaspws(
        &self,
        chavg: Option<&Bound<'_, PyAny>>,
        fdm: Option<&Bound<'_, PyAny>>,
        sqld: Option<&Bound<'_, PyAny>>,
        tdm: Option<&Bound<'_, PyAny>>,
        wvr: Option<&Bound<'_, PyAny>>,
        complement: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("almaspws")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (antenna = None), text_signature = "($self, antenna=-1)")]
    fn antennadiameter(&self, antenna: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("antennadiameter")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (name = None, mindiameter = None, maxdiameter = None, obsid = None), text_signature = "($self, name='', mindiameter='0m', maxdiameter='1pc', obsid=-1)")]
    fn antennaids(
        &self,
        name: Option<&Bound<'_, PyAny>>,
        mindiameter: Option<&Bound<'_, PyAny>>,
        maxdiameter: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("antennaids")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (antennaids = None), text_signature = "($self, antennaids=-1)")]
    fn antennanames(&self, antennaids: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("antennanames")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None), text_signature = "($self, which=0)")]
    fn antennaoffset(&self, which: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("antennaoffset")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None), text_signature = "($self, which=0)")]
    fn antennaposition(&self, which: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("antennaposition")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scan = None, obsid = None, arrayid = None), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1)")]
    fn antennasforscan(
        &self,
        scan: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("antennasforscan")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None, obsid = None), text_signature = "($self, which=-1, obsid=-1)")]
    fn antennastations(
        &self,
        which: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("antennastations")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw = None), text_signature = "($self, spw=-1)")]
    fn bandwidths(&self, spw: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("bandwidths")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw), text_signature = "($self, spw)")]
    fn baseband(&self, spw: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("baseband")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn baselines(&self) -> PyResult<Py<PyAny>> {
        stub("baselines")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn chanavgspws(&self) -> PyResult<Py<PyAny>> {
        stub("chanavgspws")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw, unit = None, asvel = None), text_signature = "($self, spw, unit='', asvel=False)")]
    fn chaneffbws(
        &self,
        spw: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
        asvel: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("chaneffbws")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw, unit = None), text_signature = "($self, spw, unit='Hz')")]
    fn chanfreqs(
        &self,
        spw: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("chanfreqs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw, unit = None, asvel = None), text_signature = "($self, spw, unit='', asvel=False)")]
    fn chanres(
        &self,
        spw: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
        asvel: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("chanres")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw, unit = None), text_signature = "($self, spw, unit='Hz')")]
    fn chanwidths(
        &self,
        spw: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("chanwidths")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn close(&self) -> PyResult<Py<PyAny>> {
        stub("close")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw = None), text_signature = "($self, spw=-1)")]
    fn corrbit(&self, spw: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("corrbit")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pol = None), text_signature = "($self, pol=-1)")]
    fn corrprodsforpol(&self, pol: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("corrprodsforpol")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pol = None), text_signature = "($self, pol=-1)")]
    fn corrtypesforpol(&self, pol: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("corrtypesforpol")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw = None, pol = None), text_signature = "($self, spw=-1, pol=-1)")]
    fn datadescids(
        &self,
        spw: Option<&Bound<'_, PyAny>>,
        pol: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("datadescids")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn done(&self) -> PyResult<Py<PyAny>> {
        stub("done")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn effexposuretime(&self) -> PyResult<Py<PyAny>> {
        stub("effexposuretime")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scan = None, spwid = None, polid = None, obsid = None, arrayid = None), text_signature = "($self, scan=0, spwid=0, polid=-1, obsid=0, arrayid=0)")]
    fn exposuretime(
        &self,
        scan: Option<&Bound<'_, PyAny>>,
        spwid: Option<&Bound<'_, PyAny>>,
        polid: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("exposuretime")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn fdmspws(&self) -> PyResult<Py<PyAny>> {
        stub("fdmspws")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn fieldnames(&self) -> PyResult<Py<PyAny>> {
        stub("fieldnames")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (intent = None, asnames = None), text_signature = "($self, intent='', asnames=False)")]
    fn fieldsforintent(
        &self,
        intent: Option<&Bound<'_, PyAny>>,
        asnames: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fieldsforintent")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (name = None), text_signature = "($self, name='')")]
    fn fieldsforname(&self, name: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("fieldsforname")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scan = None, asnames = None, obsid = None, arrayid = None), text_signature = "($self, scan=-1, asnames=False, obsid=-1, arrayid=-1)")]
    fn fieldsforscan(
        &self,
        scan: Option<&Bound<'_, PyAny>>,
        asnames: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fieldsforscan")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scans = None, asnames = None, obsid = None, arrayid = None, asmap = None), text_signature = "($self, scans=[], asnames=False, obsid=-1, arrayid=-1, asmap=False)")]
    fn fieldsforscans(
        &self,
        scans: Option<&Bound<'_, PyAny>>,
        asnames: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
        asmap: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fieldsforscans")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (source = None, asnames = None), text_signature = "($self, source=-1, asnames=False)")]
    fn fieldsforsource(
        &self,
        source: Option<&Bound<'_, PyAny>>,
        asnames: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fieldsforsource")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (asnames = None), text_signature = "($self, asnames=False)")]
    fn fieldsforsources(&self, asnames: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("fieldsforsources")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw = None, asnames = None), text_signature = "($self, spw=-1, asnames=False)")]
    fn fieldsforspw(
        &self,
        spw: Option<&Bound<'_, PyAny>>,
        asnames: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fieldsforspw")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (time = None, tol = None), text_signature = "($self, time=-1.0, tol=0.0)")]
    fn fieldsfortimes(
        &self,
        time: Option<&Bound<'_, PyAny>>,
        tol: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fieldsfortimes")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn intents(&self) -> PyResult<Py<PyAny>> {
        stub("intents")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None), text_signature = "($self, field=0)")]
    fn intentsforfield(&self, field: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("intentsforfield")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scan = None, obsid = None, arrayid = None), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1)")]
    fn intentsforscan(
        &self,
        scan: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("intentsforscan")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw = None), text_signature = "($self, spw=-1)")]
    fn intentsforspw(&self, spw: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("intentsforspw")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw, unit = None), text_signature = "($self, spw, unit='Hz')")]
    fn meanfreq(
        &self,
        spw: &Bound<'_, PyAny>,
        unit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("meanfreq")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn name(&self) -> PyResult<Py<PyAny>> {
        stub("name")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (fieldids = None), text_signature = "($self, fieldids='')")]
    fn namesforfields(&self, fieldids: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("namesforfields")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spwids = None), text_signature = "($self, spwids='')")]
    fn namesforspws(&self, spwids: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("namesforspws")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nantennas(&self) -> PyResult<Py<PyAny>> {
        stub("nantennas")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn narrays(&self) -> PyResult<Py<PyAny>> {
        stub("narrays")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (ac = None), text_signature = "($self, ac=False)")]
    fn nbaselines(&self, ac: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("nbaselines")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw), text_signature = "($self, spw)")]
    fn nchan(&self, spw: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("nchan")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (polid = None), text_signature = "($self, polid=-1)")]
    fn ncorrforpol(&self, polid: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("ncorrforpol")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nfields(&self) -> PyResult<Py<PyAny>> {
        stub("nfields")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nobservations(&self) -> PyResult<Py<PyAny>> {
        stub("nobservations")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (autoc = None, flagged = None), text_signature = "($self, autoc=True, flagged=True)")]
    fn nrows(
        &self,
        autoc: Option<&Bound<'_, PyAny>>,
        flagged: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("nrows")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nscans(&self) -> PyResult<Py<PyAny>> {
        stub("nscans")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nsources(&self) -> PyResult<Py<PyAny>> {
        stub("nsources")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (includewvr = None), text_signature = "($self, includewvr=True)")]
    fn nspw(&self, includewvr: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("nspw")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nstates(&self) -> PyResult<Py<PyAny>> {
        stub("nstates")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn observatorynames(&self) -> PyResult<Py<PyAny>> {
        stub("observatorynames")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None), text_signature = "($self, which=0)")]
    fn observatoryposition(&self, which: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("observatoryposition")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn observers(&self) -> PyResult<Py<PyAny>> {
        stub("observers")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msfile = None, maxcache = None), text_signature = "($self, msfile='', maxcache=50.0)")]
    fn open(
        &self,
        msfile: Option<&Bound<'_, PyAny>>,
        maxcache: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("open")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (fieldid = None, epoch = None), text_signature = "($self, fieldid=0, epoch={})")]
    fn phasecenter(
        &self,
        fieldid: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("phasecenter")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (rownum = None, interpolate = None, initialrow = None), text_signature = "($self, rownum=0, interpolate=False, initialrow=0)")]
    fn pointingdirection(
        &self,
        rownum: Option<&Bound<'_, PyAny>>,
        interpolate: Option<&Bound<'_, PyAny>>,
        initialrow: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("pointingdirection")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (ddid = None), text_signature = "($self, ddid=-1)")]
    fn polidfordatadesc(&self, ddid: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("polidfordatadesc")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn projects(&self) -> PyResult<Py<PyAny>> {
        stub("projects")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn propermotions(&self) -> PyResult<Py<PyAny>> {
        stub("propermotions")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None, epoch = None), text_signature = "($self, field=0, epoch={})")]
    fn refdir(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("refdir")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw = None), text_signature = "($self, spw=-1)")]
    fn reffreq(&self, spw: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("reffreq")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (sourceid = None, spw = None), text_signature = "($self, sourceid=0, spw=0)")]
    fn restfreqs(
        &self,
        sourceid: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("restfreqs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spwids = None), text_signature = "($self, spwids='')")]
    fn rxbands(&self, spwids: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("rxbands")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None, arrayid = None), text_signature = "($self, obsid=-1, arrayid=-1)")]
    fn scannumbers(
        &self,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("scannumbers")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None, obsid = None, arrayid = None), text_signature = "($self, field='', obsid=-1, arrayid=-1)")]
    fn scansforfield(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("scansforfield")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None, arrayid = None), text_signature = "($self, obsid=0, arrayid=0)")]
    fn scansforfields(
        &self,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("scansforfields")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (intent = None, obsid = None, arrayid = None), text_signature = "($self, intent='', obsid=-1, arrayid=-1)")]
    fn scansforintent(
        &self,
        intent: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("scansforintent")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw = None, obsid = None, arrayid = None), text_signature = "($self, spw=-1, obsid=-1, arrayid=-1)")]
    fn scansforspw(
        &self,
        spw: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("scansforspw")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None, arrayid = None), text_signature = "($self, obsid=0, arrayid=0)")]
    fn scansforspws(
        &self,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("scansforspws")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (state = None, obsid = None, arrayid = None), text_signature = "($self, state=-1, obsid=-1, arrayid=-1)")]
    fn scansforstate(
        &self,
        state: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("scansforstate")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (time = None, tol = None, obsid = None, arrayid = None), text_signature = "($self, time=-1.0, tol=0.0, obsid=-1, arrayid=-1)")]
    fn scansfortimes(
        &self,
        time: Option<&Bound<'_, PyAny>>,
        tol: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("scansfortimes")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None), text_signature = "($self, obsid=-1)")]
    fn schedule(&self, obsid: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("schedule")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw), text_signature = "($self, spw)")]
    fn sideband(&self, spw: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("sideband")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourcedirs(&self) -> PyResult<Py<PyAny>> {
        stub("sourcedirs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None), text_signature = "($self, field=-1)")]
    fn sourceidforfield(&self, field: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("sourceidforfield")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourceidsfromsourcetable(&self) -> PyResult<Py<PyAny>> {
        stub("sourceidsfromsourcetable")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourcenames(&self) -> PyResult<Py<PyAny>> {
        stub("sourcenames")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourcetimes(&self) -> PyResult<Py<PyAny>> {
        stub("sourcetimes")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (ddid = None), text_signature = "($self, ddid=-1)")]
    fn spwfordatadesc(&self, ddid: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("spwfordatadesc")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (baseband = None, sqldmode = None), text_signature = "($self, baseband=-1, sqldmode='include')")]
    fn spwsforbaseband(
        &self,
        baseband: Option<&Bound<'_, PyAny>>,
        sqldmode: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("spwsforbaseband")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None), text_signature = "($self, field='')")]
    fn spwsforfield(&self, field: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("spwsforfield")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn spwsforfields(&self) -> PyResult<Py<PyAny>> {
        stub("spwsforfields")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (intent = None), text_signature = "($self, intent='')")]
    fn spwsforintent(&self, intent: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("spwsforintent")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spwids = None), text_signature = "($self, spwids='')")]
    fn spwsfornames(&self, spwids: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("spwsfornames")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scan = None, obsid = None, arrayid = None), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1)")]
    fn spwsforscan(
        &self,
        scan: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("spwsforscan")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None, arrayid = None), text_signature = "($self, obsid=0, arrayid=0)")]
    fn spwsforscans(
        &self,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("spwsforscans")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scan = None, obsid = None, arrayid = None), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1)")]
    fn statesforscan(
        &self,
        scan: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("statesforscan")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None, arrayid = None), text_signature = "($self, obsid=0, arrayid=0)")]
    fn statesforscans(
        &self,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("statesforscans")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spwids = None), text_signature = "($self, spwids='')")]
    fn subwindows(&self, spwids: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("subwindows")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn summary(&self) -> PyResult<Py<PyAny>> {
        stub("summary")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn tdmspws(&self) -> PyResult<Py<PyAny>> {
        stub("tdmspws")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None), text_signature = "($self, obsid=-1)")]
    fn timerangeforobs(&self, obsid: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("timerangeforobs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None), text_signature = "($self, field=-1)")]
    fn timesforfield(&self, field: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("timesforfield")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (intent = None), text_signature = "($self, intent='')")]
    fn timesforintent(&self, intent: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("timesforintent")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scan = None, obsid = None, arrayid = None, perspw = None), text_signature = "($self, scan=-1, obsid=-1, arrayid=-1, perspw=False)")]
    fn timesforscan(
        &self,
        scan: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
        perspw: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("timesforscan")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scans = None, obsid = None, arrayid = None), text_signature = "($self, scans=0, obsid=-1, arrayid=-1)")]
    fn timesforscans(
        &self,
        scans: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("timesforscans")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spw = None), text_signature = "($self, spw=-1)")]
    fn timesforspws(&self, spw: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("timesforspws")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (sourceid = None, spw = None), text_signature = "($self, sourceid=0, spw=0)")]
    fn transitions(
        &self,
        sourceid: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("transitions")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (complement = None), text_signature = "($self, complement=False)")]
    fn wvrspws(&self, complement: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("wvrspws")
    }
}
