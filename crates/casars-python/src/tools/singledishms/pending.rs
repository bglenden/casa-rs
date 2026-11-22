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
}
