// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `ms` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1030.

use pyo3::prelude::*;

use super::{PyMsTool, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyMsTool {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (id = None, ephemerisname = None, comment = None, field = None), text_signature = "($self, id=-1, ephemerisname='', comment='', field=[])")]
    fn addephemeris(
        &self,
        id: Option<&Bound<'_, PyAny>>,
        ephemerisname: Option<&Bound<'_, PyAny>>,
        comment: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("addephemeris")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (abspath = None), text_signature = "($self, abspath='')")]
    fn asdmref(&self, abspath: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("asdmref")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msfile = None, freqtol = None, dirtol = None, weightscale = None, handling = None, destmsfile = None, respectname = None), text_signature = "($self, msfile='', freqtol=[], dirtol=[], weightscale=1.0, handling=0, destmsfile='', respectname=False)")]
    fn concatenate(
        &self,
        msfile: Option<&Bound<'_, PyAny>>,
        freqtol: Option<&Bound<'_, PyAny>>,
        dirtol: Option<&Bound<'_, PyAny>>,
        weightscale: Option<&Bound<'_, PyAny>>,
        handling: Option<&Bound<'_, PyAny>>,
        destmsfile: Option<&Bound<'_, PyAny>>,
        respectname: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("concatenate")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None, fitspw = None, spw = None, solint = None, fitorder = None, mode = None), text_signature = "($self, field=[], fitspw=[], spw=[], solint=[], fitorder=0, mode='subtract')")]
    fn continuumsub(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        solint: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("continuumsub")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None, fitspw = None, spw = None, solint = None, fitorder = None, mode = None), text_signature = "($self, field=[], fitspw=[], spw=[], solint=[], fitorder=0, mode='subtract')")]
    fn continuumsubold(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        solint: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("continuumsubold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outputms = None, fitspw = None, fitorder = None, combine = None, spw = None, unionspw = None, field = None, scan = None, intent = None, correlation = None, obs = None, whichcol = None), text_signature = "($self, outputms='', fitspw=[], fitorder=1, combine='', spw='*', unionspw='*', field='', scan='', intent='', correlation='', obs='', whichcol='CORRECTED_DATA')")]
    fn contsub(
        &self,
        outputms: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        combine: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        unionspw: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        intent: Option<&Bound<'_, PyAny>>,
        correlation: Option<&Bound<'_, PyAny>>,
        obs: Option<&Bound<'_, PyAny>>,
        whichcol: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("contsub")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outputTableName, tables, subtables, nomodify = None, lock = None, copysubtables = None, omitsubtables = None), text_signature = "($self, outputTableName, tables, subtables, nomodify=True, lock=False, copysubtables=False, omitsubtables=[])")]
    fn createmultims(
        &self,
        outputTableName: &Bound<'_, PyAny>,
        tables: &Bound<'_, PyAny>,
        subtables: &Bound<'_, PyAny>,
        nomodify: Option<&Bound<'_, PyAny>>,
        lock: Option<&Bound<'_, PyAny>>,
        copysubtables: Option<&Bound<'_, PyAny>>,
        omitsubtables: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("createmultims")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (mode = None, nchan = None, start = None, width = None, interp = None, phasec = None, restfreq = None, outframe = None, veltype = None, hanning = None), text_signature = "($self, mode='channel', nchan=-1, start=[], width=[], interp='linear', phasec=[], restfreq=[], outframe='', veltype='radio', hanning=True)")]
    fn cvel(
        &self,
        mode: Option<&Bound<'_, PyAny>>,
        nchan: Option<&Bound<'_, PyAny>>,
        start: Option<&Bound<'_, PyAny>>,
        width: Option<&Bound<'_, PyAny>>,
        interp: Option<&Bound<'_, PyAny>>,
        phasec: Option<&Bound<'_, PyAny>>,
        restfreq: Option<&Bound<'_, PyAny>>,
        outframe: Option<&Bound<'_, PyAny>>,
        veltype: Option<&Bound<'_, PyAny>>,
        hanning: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("cvel")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (spwids = None, fieldids = None, obstime = None, mode = None, nchan = None, start = None, width = None, phasec = None, restfreq = None, outframe = None, veltype = None, verbose = None), text_signature = "($self, spwids=[0], fieldids=[0], obstime='', mode='channel', nchan=-1, start=[], width=[], phasec=[], restfreq=[], outframe='', veltype='radio', verbose=True)")]
    fn cvelfreqs(
        &self,
        spwids: Option<&Bound<'_, PyAny>>,
        fieldids: Option<&Bound<'_, PyAny>>,
        obstime: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
        nchan: Option<&Bound<'_, PyAny>>,
        start: Option<&Bound<'_, PyAny>>,
        width: Option<&Bound<'_, PyAny>>,
        phasec: Option<&Bound<'_, PyAny>>,
        restfreq: Option<&Bound<'_, PyAny>>,
        outframe: Option<&Bound<'_, PyAny>>,
        veltype: Option<&Bound<'_, PyAny>>,
        verbose: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("cvelfreqs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msfile = None, fitsfile = None, nomodify = None, lock = None, obstype = None, host = None, forcenewserver = None, antnamescheme = None), text_signature = "($self, msfile='', fitsfile='', nomodify=True, lock=False, obstype=0, host='', forcenewserver=False, antnamescheme='old')")]
    fn fromfits(
        &self,
        msfile: Option<&Bound<'_, PyAny>>,
        fitsfile: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
        lock: Option<&Bound<'_, PyAny>>,
        obstype: Option<&Bound<'_, PyAny>>,
        host: Option<&Bound<'_, PyAny>>,
        forcenewserver: Option<&Bound<'_, PyAny>>,
        antnamescheme: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromfits")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msfile = None, fitsfile = None, nomodify = None, lock = None, obstype = None), text_signature = "($self, msfile='', fitsfile='', nomodify=True, lock=False, obstype=0)")]
    fn fromfitsidi(
        &self,
        msfile: Option<&Bound<'_, PyAny>>,
        fitsfile: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
        lock: Option<&Bound<'_, PyAny>>,
        obstype: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromfitsidi")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (items = None, ifraxis = None, ifraxisgap = None, increment = None, average = None), text_signature = "($self, items=[], ifraxis=False, ifraxisgap=0, increment=1, average=False)")]
    fn getdataold(
        &self,
        items: Option<&Bound<'_, PyAny>>,
        ifraxis: Option<&Bound<'_, PyAny>>,
        ifraxisgap: Option<&Bound<'_, PyAny>>,
        increment: Option<&Bound<'_, PyAny>>,
        average: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getdataold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (datacolumn = None), text_signature = "($self, datacolumn='corrected')")]
    fn hanningsmooth(&self, datacolumn: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("hanningsmooth")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn iterendold(&self) -> PyResult<Py<PyAny>> {
        stub("iterendold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columns = None, interval = None, maxrows = None, adddefaultsortcolumns = None), text_signature = "($self, columns=[], interval=0.0, maxrows=0, adddefaultsortcolumns=True)")]
    fn iterinitold(
        &self,
        columns: Option<&Bound<'_, PyAny>>,
        interval: Option<&Bound<'_, PyAny>>,
        maxrows: Option<&Bound<'_, PyAny>>,
        adddefaultsortcolumns: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("iterinitold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn iternextold(&self) -> PyResult<Py<PyAny>> {
        stub("iternextold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn iteroriginold(&self) -> PyResult<Py<PyAny>> {
        stub("iteroriginold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (fitsfile), text_signature = "($self, fitsfile)")]
    fn listfits(&self, fitsfile: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("listfits")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (items = None, ifraxis = None, ifraxisgap = None, increment = None, average = None), text_signature = "($self, items=[], ifraxis=False, ifraxisgap=0, increment=1, average=False)")]
    fn ngetdata(
        &self,
        items: Option<&Bound<'_, PyAny>>,
        ifraxis: Option<&Bound<'_, PyAny>>,
        ifraxisgap: Option<&Bound<'_, PyAny>>,
        increment: Option<&Bound<'_, PyAny>>,
        average: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("ngetdata")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn niterend(&self) -> PyResult<Py<PyAny>> {
        stub("niterend")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columns = None, interval = None, maxrows = None, adddefaultsortcolumns = None), text_signature = "($self, columns=[], interval=0.0, maxrows=0, adddefaultsortcolumns=True)")]
    fn niterinit(
        &self,
        columns: Option<&Bound<'_, PyAny>>,
        interval: Option<&Bound<'_, PyAny>>,
        maxrows: Option<&Bound<'_, PyAny>>,
        adddefaultsortcolumns: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("niterinit")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn niternext(&self) -> PyResult<Py<PyAny>> {
        stub("niternext")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn niterorigin(&self) -> PyResult<Py<PyAny>> {
        stub("niterorigin")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (selected = None), text_signature = "($self, selected=False)")]
    fn nrowold(&self, selected: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("nrowold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (dorms = None, byantenna = None, sepacs = None, fitspw = None, fitcorr = None, combine = None, timebin = None, minsamp = None, field = None, spw = None, antenna = None, timerange = None, scan = None, intent = None, array = None, correlation = None, obs = None, datacolumn = None), text_signature = "($self, dorms=False, byantenna=True, sepacs=True, fitspw=[], fitcorr=[], combine='', timebin=[], minsamp=3, field=[], spw=[], antenna='', timerange='', scan=[], intent='', array=[], correlation='', obs='', datacolumn='corrected_data')")]
    fn oldstatwt(
        &self,
        dorms: Option<&Bound<'_, PyAny>>,
        byantenna: Option<&Bound<'_, PyAny>>,
        sepacs: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        fitcorr: Option<&Bound<'_, PyAny>>,
        combine: Option<&Bound<'_, PyAny>>,
        timebin: Option<&Bound<'_, PyAny>>,
        minsamp: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        antenna: Option<&Bound<'_, PyAny>>,
        timerange: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        intent: Option<&Bound<'_, PyAny>>,
        array: Option<&Bound<'_, PyAny>>,
        correlation: Option<&Bound<'_, PyAny>>,
        obs: Option<&Bound<'_, PyAny>>,
        datacolumn: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("oldstatwt")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outputms = None, field = None, spw = None, baseline = None, timebin = None, time = None, scan = None, uvrange = None, taql = None, whichcol = None, tileshape = None, subarray = None, combine = None, intent = None, obs = None), text_signature = "($self, outputms='', field=[], spw=[], baseline=[], timebin=[], time='', scan=[], uvrange=[], taql='', whichcol='DATA', tileshape=[], subarray=[], combine='', intent='', obs='')")]
    fn partition(
        &self,
        outputms: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        baseline: Option<&Bound<'_, PyAny>>,
        timebin: Option<&Bound<'_, PyAny>>,
        time: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        taql: Option<&Bound<'_, PyAny>>,
        whichcol: Option<&Bound<'_, PyAny>>,
        tileshape: Option<&Bound<'_, PyAny>>,
        subarray: Option<&Bound<'_, PyAny>>,
        combine: Option<&Bound<'_, PyAny>>,
        intent: Option<&Bound<'_, PyAny>>,
        obs: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("partition")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (items = None), text_signature = "($self, items={})")]
    fn putdataold(&self, items: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("putdataold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (items = None, useflags = None, blocksize = None), text_signature = "($self, items=[], useflags=True, blocksize=10)")]
    fn rangeold(
        &self,
        items: Option<&Bound<'_, PyAny>>,
        useflags: Option<&Bound<'_, PyAny>>,
        blocksize: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("rangeold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outframe = None, mode = None, restfreq = None, interpolation = None, start = None, center = None, bandwidth = None, chanwidth = None, hanning = None), text_signature = "($self, outframe='LSRK', mode='chan', restfreq=-3e+30, interpolation='LINEAR', start=-3e+30, center=-3e+30, bandwidth=-1.0, chanwidth=-1.0, hanning=True)")]
    fn regridspw(
        &self,
        outframe: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
        restfreq: Option<&Bound<'_, PyAny>>,
        interpolation: Option<&Bound<'_, PyAny>>,
        start: Option<&Bound<'_, PyAny>>,
        center: Option<&Bound<'_, PyAny>>,
        bandwidth: Option<&Bound<'_, PyAny>>,
        chanwidth: Option<&Bound<'_, PyAny>>,
        hanning: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("regridspw")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (nchan = None, start = None, width = None, inc = None), text_signature = "($self, nchan=1, start=0, width=1, inc=1)")]
    fn selectchannelold(
        &self,
        nchan: Option<&Bound<'_, PyAny>>,
        start: Option<&Bound<'_, PyAny>>,
        width: Option<&Bound<'_, PyAny>>,
        inc: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("selectchannelold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (datadescid = None, reset = None), text_signature = "($self, datadescid=0, reset=False)")]
    fn selectinitold(
        &self,
        datadescid: Option<&Bound<'_, PyAny>>,
        reset: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("selectinitold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (items = None), text_signature = "($self, items={})")]
    fn selectold(&self, items: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("selectold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (wantedpol = None), text_signature = "($self, wantedpol=[])")]
    fn selectpolarizationold(&self, wantedpol: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("selectpolarizationold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msselect = None), text_signature = "($self, msselect='')")]
    fn selecttaqlold(&self, msselect: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("selecttaqlold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outputms = None, field = None, spw = None, step = None, baseline = None, timebin = None, time = None, scan = None, uvrange = None, taql = None, whichcol = None, tileshape = None, subarray = None, combine = None, correlation = None, intent = None, obs = None), text_signature = "($self, outputms='', field=[], spw=[], step=[1], baseline=[], timebin=[], time='', scan=[], uvrange=[], taql='', whichcol='DATA', tileshape=[], subarray=[], combine='', correlation='', intent='', obs='')")]
    fn split(
        &self,
        outputms: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        step: Option<&Bound<'_, PyAny>>,
        baseline: Option<&Bound<'_, PyAny>>,
        timebin: Option<&Bound<'_, PyAny>>,
        time: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        taql: Option<&Bound<'_, PyAny>>,
        whichcol: Option<&Bound<'_, PyAny>>,
        tileshape: Option<&Bound<'_, PyAny>>,
        subarray: Option<&Bound<'_, PyAny>>,
        combine: Option<&Bound<'_, PyAny>>,
        correlation: Option<&Bound<'_, PyAny>>,
        intent: Option<&Bound<'_, PyAny>>,
        obs: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("split")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msfile = None, freqtol = None, dirtol = None, respectname = None), text_signature = "($self, msfile='', freqtol=[], dirtol=[], respectname=False)")]
    fn testconcatenate(
        &self,
        msfile: Option<&Bound<'_, PyAny>>,
        freqtol: Option<&Bound<'_, PyAny>>,
        dirtol: Option<&Bound<'_, PyAny>>,
        respectname: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("testconcatenate")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (fitsfile = None, column = None, field = None, spw = None, baseline = None, time = None, scan = None, uvrange = None, taql = None, writesyscal = None, multisource = None, combinespw = None, writestation = None, padwithflags = None, overwrite = None), text_signature = "($self, fitsfile='', column='corrected', field=[], spw=[], baseline=[], time='', scan=[], uvrange=[], taql='', writesyscal=False, multisource=False, combinespw=False, writestation=False, padwithflags=False, overwrite=False)")]
    fn tofits(
        &self,
        fitsfile: Option<&Bound<'_, PyAny>>,
        column: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        baseline: Option<&Bound<'_, PyAny>>,
        time: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        taql: Option<&Bound<'_, PyAny>>,
        writesyscal: Option<&Bound<'_, PyAny>>,
        multisource: Option<&Bound<'_, PyAny>>,
        combinespw: Option<&Bound<'_, PyAny>>,
        writestation: Option<&Bound<'_, PyAny>>,
        padwithflags: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("tofits")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msfile = None, auxfilename = None, freqtol = None, dirtol = None, weightscale = None, respectname = None), text_signature = "($self, msfile='', auxfilename='', freqtol=[], dirtol=[], weightscale=1.0, respectname=True)")]
    fn virtconcatenate(
        &self,
        msfile: Option<&Bound<'_, PyAny>>,
        auxfilename: Option<&Bound<'_, PyAny>>,
        freqtol: Option<&Bound<'_, PyAny>>,
        dirtol: Option<&Bound<'_, PyAny>>,
        weightscale: Option<&Bound<'_, PyAny>>,
        respectname: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("virtconcatenate")
    }
}
