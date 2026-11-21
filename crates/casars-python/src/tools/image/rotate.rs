// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::analysis::{ImageRegridOptions, ImageRegridTarget};
use pyo3::prelude::*;

use crate::convert::{IntList, Variant, quantity, value_in};

use super::PyImageTool;
use super::handle::raised;
use super::regrid::{publish_regridded_image, regrid_indices};
use super::subimage::{Region, mask_argument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Rotate the selected sky or two-axis linear grid by a position angle.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", shape=IntList(vec![-1]), pa=None, region=None, mask=None, method="cubic", decimate=0, replicate=false, dropdeg=false, overwrite=false, stretch=false), text_signature = "($self, outfile='', shape=[-1], pa=[], region={}, mask='', method='cubic', decimate=0, replicate=False, dropdeg=False, overwrite=False, stretch=False)")]
    fn rotate(
        &self,
        py: Python<'_>,
        outfile: &str,
        #[pyo3(from_py_with = regrid_indices)] shape: IntList,
        pa: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        method: &str,
        decimate: i64,
        #[pyo3(from_py_with = require_bool)] replicate: bool,
        #[pyo3(from_py_with = require_bool)] dropdeg: bool,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let angle = pa.map_or(Ok(0.), |value| rotation_angle(value.extract()?));
        let pa = pa.map_or_else(
            || Ok("[]".into()),
            |value| Ok::<_, PyErr>(value.repr()?.to_str()?.to_owned()),
        )?;
        let region = Region::capture(py, region);
        let mask = mask_argument(mask);
        let attachment = py.detach(|| {
            let state = self.state();
            let input = state.image.as_ref().ok_or_else(|| raised("Unable to create image"))?;
            let region = region?;
            let mask = mask?;
            let options = ImageRegridOptions {
                shape: &shape.0,
                target: ImageRegridTarget::Rotation(angle?),
                axes: &[],
                region: region.as_ref().map(Region::as_region),
                mask: (!mask.is_empty()).then_some(mask.as_str()),
                method, decimate, replicate, convert_reference: false, dropdeg,
                force: false, as_velocity: false, stretch,
                outfile: (!outfile.is_empty()).then(|| Path::new(outfile)),
            };
            let history = state.history.then(|| {
                let region = region.as_ref().map_or_else(|| "{}".into(), Region::history);
                ("rotate", format!(
                    "ia.rotate(outfile={outfile:?}, shape={:?}, pa={pa}, region={region}, mask={mask:?}, method={method:?}, decimate={decimate}, replicate={replicate}, dropdeg={dropdeg}, overwrite={overwrite}, stretch={stretch})",
                    shape.0,
                ))
            });
            publish_regridded_image(input, outfile, overwrite, options, history)
        })?;
        Py::new(py, Self::attached(attachment))
    }
}

fn rotation_angle(value: Variant) -> PyResult<f64> {
    if matches!(
        value,
        Variant::Bool(_)
            | Variant::Numbers(_)
            | Variant::Complexes(_)
            | Variant::Array { .. }
            | Variant::Other(_)
    ) {
        return Err(raised("Empty QuantumHolder argument for asQuantumDouble"));
    }
    let angle = quantity(&value)?;
    // The CASA rotation obtains radians with Quantum::getValue, which
    // scales nonconformant units by their SI factor.
    let radians =
        value_in(&angle, "rad").unwrap_or_else(|_| angle.value() * angle.unit().val().factor);
    if !radians.is_finite() {
        return Err(raised("rotation angle must be finite"));
    }
    Ok(radians)
}
