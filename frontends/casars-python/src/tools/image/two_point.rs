// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.twopointcorrelation` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::two_point_correlation`].

use std::path::Path;

use casa_imageanalysis::analysis::TwoPointOptions;
use casa_imageanalysis::tool::ops_transform::two_point_correlation;
use pyo3::prelude::*;

use crate::convert::IntList;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::records::numeric_axis_vector;
use super::subimage::{RegionArgument, mask_argument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Write the Float or Complex structure function of a selected image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=RegionArgument::default(), mask=None, axes=IntList::of(&[-1]), method="structurefunction", overwrite=false, stretch=false), text_signature = "($self, outfile='', region={}, mask='', axes=[-1], method='structurefunction', overwrite=False, stretch=False)")]
    fn twopointcorrelation(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = numeric_axis_vector)] axes: IntList,
        method: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<bool> {
        let mask = mask_argument(mask);
        py.detach(|| {
            let state = self.state();
            let Some(input) = state.image.as_ref() else {
                return Ok(false);
            };
            let mask = mask?;
            let destination = (!outfile.is_empty()).then(|| Path::new(outfile));
            let options = TwoPointOptions {
                axes: &axes.0,
                method,
                region: region.selection(),
                mask: (!mask.is_empty()).then_some(mask.as_str()),
                stretch,
                destination,
            };
            let history_command = state.history.then(|| {
                let region_hist = region.history();
                format!(
                    "ia.twopointcorrelation(outfile={outfile:?}, region={region_hist}, mask={mask:?}, axes={:?}, method={method:?}, overwrite={overwrite}, stretch={stretch})",
                    axes.0,
                )
            });
            two_point_correlation(
                input,
                outfile,
                overwrite,
                options,
                history_command,
                external_open_check(),
            )
            .map(|_| true)
            .map_err(raised)
        })
    }
}
