// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.tofits` wrapper delegating to [`casa_imageanalysis::tool::fits`].

use casa_imageanalysis::analysis::ImageFitsExportOptions;
use casa_imageanalysis::tool::fits::export_attached_to_fits;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::raised;
use super::subimage::{Region, mask_argument};

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", velocity=false, optical=true, bitpix=-32, minpix=1.0, maxpix=-1.0, region=None, mask=None, overwrite=false, dropdeg=false, deglast=false, dropstokes=false, stokeslast=true, wavelength=false, airwavelength=false, stretch=false, history=true), text_signature = "($self, outfile='', velocity=False, optical=True, bitpix=-32, minpix=1.0, maxpix=-1.0, region={}, mask='', overwrite=False, dropdeg=False, deglast=False, dropstokes=False, stokeslast=True, wavelength=False, airwavelength=False, stretch=False, history=True)")]
    fn tofits(
        &self,
        py: Python<'_>,
        outfile: &str,
        velocity: bool,
        optical: bool,
        bitpix: i32,
        minpix: f64,
        maxpix: f64,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        overwrite: bool,
        dropdeg: bool,
        deglast: bool,
        dropstokes: bool,
        stokeslast: bool,
        wavelength: bool,
        airwavelength: bool,
        stretch: bool,
        history: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let region = Region::argument(py, region, "tofits")?;
        let mask = mask_argument(mask)?;
        let options = ImageFitsExportOptions {
            region: region.as_ref().map(Region::as_region),
            mask: (!mask.is_empty()).then_some(mask.as_str()),
            stretch,
            deglast,
            wavelength,
            airwavelength,
            beam_record: None,
            operand_paths: &[],
        };
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            export_attached_to_fits(
                attachment, outfile, velocity, optical, bitpix, minpix, maxpix, overwrite, dropdeg,
                dropstokes, stokeslast, history, options,
            )
            .map_err(raised)
        })
    }
}
