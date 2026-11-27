// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.getprofile` wrapper delegating to [`casa_imageanalysis::tool::ops_fit::get_profile`].

use std::path::Path;

use casa_imageanalysis::analysis::{CollapseFunction, ProfileFrequencyFrame, ReadProfileOptions};
use casa_imageanalysis::tool::ops_fit::{get_profile, spectral_axis_index};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{Variant, quantity};
use crate::tools::coordsys::profile_frequency_frame;

use super::PyImageTool;
use super::data_compat::record_to_py;
use super::handle::raised;
use super::subimage::{RegionArgument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Reduce the selected image perpendicular to an axis and return the profile.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (axis=-1, function="mean", region=None, mask="", unit="", stretch=false, spectype="default", restfreq=None, frame="", logfile=""), text_signature = "($self, axis=-1, function='mean', region={}, mask='', unit='', stretch=False, spectype='default', restfreq='', frame='', logfile='')")]
    fn getprofile<'py>(
        &self,
        py: Python<'py>,
        axis: i64,
        function: &str,
        region: Option<&Bound<'py, PyAny>>,
        mask: &str,
        unit: &str,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        spectype: &str,
        restfreq: Option<Variant>,
        frame: &str,
        logfile: &str,
    ) -> PyResult<Py<PyAny>> {
        let axis_u = usize::try_from(axis).map_err(|_| raised("Axis must be greater than 0"))?;
        let rest_frequency = match restfreq {
            None | Some(Variant::Empty) => None,
            Some(Variant::Text(text)) if text.trim().is_empty() => None,
            Some(value) => Some(quantity(&value).map_err(raised)?),
        };
        let function = CollapseFunction::parse(function).map_err(raised)?;
        let region: RegionArgument =
            region.map_or_else(|| Ok(RegionArgument::default()), |value| value.extract())?;
        let frame = frame.trim();
        let path = (!logfile.is_empty()).then(|| Path::new(logfile));
        let (record, counts) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let attachment = state
                .image
                .as_ref()
                .ok_or_else(|| raised("No image attached to tool"))?;
            let spectral_frame = if !frame.is_empty()
                && !unit.to_ascii_lowercase().starts_with("pix")
                && spectral_axis_index(attachment) == Some(axis_u)
            {
                let handle = attachment.lock();
                profile_frequency_frame(handle.coordinates(), frame)?
                    .map(|(reference, context)| ProfileFrequencyFrame { reference, context })
            } else {
                None
            };
            let options = ReadProfileOptions {
                axis: axis_u,
                function,
                region: region.selection(),
                mask: (!mask.is_empty()).then_some(mask),
                stretch,
                unit,
                spectral_type: spectype,
                rest_frequency: rest_frequency.as_ref(),
                spectral_frame,
                logfile: path,
            };
            let profile = get_profile(attachment, options).map_err(raised)?;
            Ok(profile.into_record_and_counts())
        })?;
        let result = record_to_py(py, record)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("dtype", "uint64")?;
        let npix = py
            .import("numpy")?
            .call_method("asarray", (counts,), Some(&kwargs))?;
        result.bind(py).cast::<PyDict>()?.set_item("npix", npix)?;
        Ok(result)
    }
}
