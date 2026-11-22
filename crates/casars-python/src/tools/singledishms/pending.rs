// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public signatures for the remaining operations in CAL-20.

use pyo3::prelude::*;

use super::PySingleDishMs;
use crate::tools::not_yet_implemented_in;

#[pymethods]
#[allow(unused_variables, clippy::too_many_arguments, clippy::unused_self)]
impl PySingleDishMs {
    #[pyo3(signature = (config = None, datacolumn = None, outfile = None), text_signature = "($self, config={}, datacolumn='data', outfile='')")]
    fn atmcor(
        &self,
        config: Option<&Bound<'_, PyAny>>,
        datacolumn: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "atmcor", 1532)
    }

    #[pyo3(signature = (datacolumn = None, spw = None, pol = None, timebin = None, timespan = None, polaverage = None, fitfunc = None, nfit = None, linefinding = None, threshold = None, avg_limit = None, minwidth = None, edge = None, tempfile = None, tempoutfile = None), text_signature = "($self, datacolumn='data', spw=[], pol=[], timebin='', timespan='', polaverage='', fitfunc='gaussian', nfit='0', linefinding=False, threshold=5.0, avg_limit=4, minwidth=4, edge=[0, 0], tempfile='', tempoutfile='')")]
    fn fit_line(
        &self,
        datacolumn: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        pol: Option<&Bound<'_, PyAny>>,
        timebin: Option<&Bound<'_, PyAny>>,
        timespan: Option<&Bound<'_, PyAny>>,
        polaverage: Option<&Bound<'_, PyAny>>,
        fitfunc: Option<&Bound<'_, PyAny>>,
        nfit: Option<&Bound<'_, PyAny>>,
        linefinding: Option<&Bound<'_, PyAny>>,
        threshold: Option<&Bound<'_, PyAny>>,
        avg_limit: Option<&Bound<'_, PyAny>>,
        minwidth: Option<&Bound<'_, PyAny>>,
        edge: Option<&Bound<'_, PyAny>>,
        tempfile: Option<&Bound<'_, PyAny>>,
        tempoutfile: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "fit_line", 1532)
    }

    #[pyo3(signature = (infile = None, outfile = None, parallel = None), text_signature = "($self, infile='', outfile='', parallel=False)")]
    fn importasap(
        &self,
        infile: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        parallel: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "importasap", 1532)
    }

    #[pyo3(signature = (infile = None, outfile = None, parallel = None), text_signature = "($self, infile='', outfile='', parallel=False)")]
    fn importnro(
        &self,
        infile: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        parallel: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "importnro", 1532)
    }

    #[pyo3(signature = (datacolumn = None, outfile = None, bloutput = None, dosubtract = None, spw = None, updateweight = None, sigmavalue = None, npiece = None, clip_threshold_sigma = None, num_fitting_max = None, linefinding = None, threshold = None, avg_limit = None, minwidth = None, edge = None), text_signature = "($self, datacolumn='data', outfile='', bloutput='', dosubtract=True, spw=[], updateweight=False, sigmavalue='stddev', npiece=5, clip_threshold_sigma=3.0, num_fitting_max=1, linefinding=False, threshold=5.0, avg_limit=4, minwidth=4, edge=[0, 0])")]
    fn subtract_baseline_cspline(
        &self,
        datacolumn: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        bloutput: Option<&Bound<'_, PyAny>>,
        dosubtract: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        updateweight: Option<&Bound<'_, PyAny>>,
        sigmavalue: Option<&Bound<'_, PyAny>>,
        npiece: Option<&Bound<'_, PyAny>>,
        clip_threshold_sigma: Option<&Bound<'_, PyAny>>,
        num_fitting_max: Option<&Bound<'_, PyAny>>,
        linefinding: Option<&Bound<'_, PyAny>>,
        threshold: Option<&Bound<'_, PyAny>>,
        avg_limit: Option<&Bound<'_, PyAny>>,
        minwidth: Option<&Bound<'_, PyAny>>,
        edge: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "subtract_baseline_cspline", 1532)
    }

    #[pyo3(signature = (datacolumn = None, outfile = None, bloutput = None, dosubtract = None, spw = None, updateweight = None, sigmavalue = None, addwn = None, rejwn = None, applyfft = None, fftmethod = None, fftthresh = None, clip_threshold_sigma = None, num_fitting_max = None, linefinding = None, threshold = None, avg_limit = None, minwidth = None, edge = None), text_signature = "($self, datacolumn='data', outfile='', bloutput='', dosubtract=True, spw=[], updateweight=False, sigmavalue='stddev', addwn='0', rejwn='', applyfft=False, fftmethod='fft', fftthresh=[], clip_threshold_sigma=3.0, num_fitting_max=1, linefinding=False, threshold=5.0, avg_limit=4, minwidth=4, edge=[0, 0])")]
    fn subtract_baseline_sinusoid(
        &self,
        datacolumn: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        bloutput: Option<&Bound<'_, PyAny>>,
        dosubtract: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        updateweight: Option<&Bound<'_, PyAny>>,
        sigmavalue: Option<&Bound<'_, PyAny>>,
        addwn: Option<&Bound<'_, PyAny>>,
        rejwn: Option<&Bound<'_, PyAny>>,
        applyfft: Option<&Bound<'_, PyAny>>,
        fftmethod: Option<&Bound<'_, PyAny>>,
        fftthresh: Option<&Bound<'_, PyAny>>,
        clip_threshold_sigma: Option<&Bound<'_, PyAny>>,
        num_fitting_max: Option<&Bound<'_, PyAny>>,
        linefinding: Option<&Bound<'_, PyAny>>,
        threshold: Option<&Bound<'_, PyAny>>,
        avg_limit: Option<&Bound<'_, PyAny>>,
        minwidth: Option<&Bound<'_, PyAny>>,
        edge: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "subtract_baseline_sinusoid", 1532)
    }

    #[pyo3(signature = (datacolumn = None, outfile = None, bloutput = None, dosubtract = None, spw = None, updateweight = None, sigmavalue = None, blparam = None, verbose = None), text_signature = "($self, datacolumn='data', outfile='', bloutput='', dosubtract=True, spw=[], updateweight=False, sigmavalue='stddev', blparam='', verbose=False)")]
    fn subtract_baseline_variable(
        &self,
        datacolumn: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        bloutput: Option<&Bound<'_, PyAny>>,
        dosubtract: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        updateweight: Option<&Bound<'_, PyAny>>,
        sigmavalue: Option<&Bound<'_, PyAny>>,
        blparam: Option<&Bound<'_, PyAny>>,
        verbose: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "subtract_baseline_variable", 1532)
    }
}
