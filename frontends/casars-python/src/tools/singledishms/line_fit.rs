// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;
use std::sync::PoisonError;

use casa_calibration::{SingleDishLineFinder, SingleDishLineFit};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::{IntList, Variant};
use crate::tools::calibrater::selector;

use super::PySingleDishMs;

#[pymethods]
impl PySingleDishMs {
    #[pyo3(signature = (datacolumn = "data", spw = Variant::Empty, pol = Variant::Empty, timebin = "", timespan = "", polaverage = "", fitfunc = "gaussian", nfit = "0", linefinding = false, threshold = 5.0, avg_limit = 4, minwidth = 4, edge = None, tempfile = "", tempoutfile = ""), text_signature = "($self, datacolumn='data', spw=[], pol=[], timebin='', timespan='', polaverage='', fitfunc='gaussian', nfit='0', linefinding=False, threshold=5.0, avg_limit=4, minwidth=4, edge=[0, 0], tempfile='', tempoutfile='')")]
    #[allow(clippy::too_many_arguments)]
    fn fit_line(
        &self,
        py: Python<'_>,
        datacolumn: &str,
        spw: Variant,
        pol: Variant,
        timebin: &str,
        timespan: &str,
        polaverage: &str,
        fitfunc: &str,
        nfit: &str,
        linefinding: bool,
        threshold: f64,
        avg_limit: i64,
        minwidth: i64,
        edge: Option<IntList>,
        tempfile: &str,
        tempoutfile: &str,
    ) -> PyResult<bool> {
        let spw = selector(&spw).unwrap_or_default();
        // The CASA tool uses the correlation from set_selection.
        drop(pol);
        py.detach(|| {
            self.session
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .fit_line(SingleDishLineFit {
                    datacolumn,
                    spw: &spw,
                    timebin,
                    timespan,
                    polaverage,
                    fitfunc,
                    nfit,
                    finder: linefinding.then(|| {
                        SingleDishLineFinder::new(
                            threshold,
                            avg_limit,
                            minwidth,
                            edge.as_ref().map_or(&[], |edge| edge.0.as_slice()),
                        )
                    }),
                    tempfile: Path::new(tempfile),
                    tempoutfile: Path::new(tempoutfile),
                })
        })
        .map_err(PyRuntimeError::new_err)
    }
}
