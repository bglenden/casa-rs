// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_imageanalysis::analysis::{
    ExportFitsRequest, ImageFitsExportOptions, export_attached_fits, validate_attached_fits_target,
};
use casa_images::image_history_entries;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised};
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
        if outfile.is_empty() {
            return Err(raised("fitsfile must be specified"));
        }
        if matches!(outfile, "." | "..") {
            return Err(raised(format!("Invalid fitsfile name {outfile}")));
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
            let mut handle = attachment.lock();
            if !matches!(*handle, ImageHandle::Float(_)) {
                return Err(raised(
                    "Only writing float-valued images to FITS is supported",
                ));
            }
            let request = ExportFitsRequest {
                imagename: handle.path().map_or_else(PathBuf::new, Path::to_path_buf),
                fitsimage: PathBuf::from(outfile),
                velocity,
                optical,
                bitpix,
                minpix,
                maxpix,
                overwrite,
                dropstokes,
                stokeslast,
                history,
                dropdeg,
            };
            let operand_paths = attachment.operand_paths();
            validate_attached_fits_target(
                &request,
                handle
                    .path()
                    .into_iter()
                    .chain(operand_paths.iter().map(PathBuf::as_path)),
            )
            .map_err(raised)?;
            attachment.require_pixel_read_lock(&mut handle)?;
            let ImageHandle::Float(storage) = &*handle else {
                unreachable!("image storage stays fixed while locked");
            };
            let beams = attachment.beam_view();
            let options = ImageFitsExportOptions {
                beam_record: Some(beams.record()),
                operand_paths,
                ..options
            };
            let entries = if history {
                match storage {
                    Storage::Paged(_) => {
                        image_history_entries(&request.imagename).map_err(raised)?
                    }
                    Storage::Temp { image, .. } => image
                        .history_rows()
                        .map_err(raised)?
                        .iter()
                        .map(|row| row.entry.clone())
                        .collect(),
                }
            } else {
                Vec::new()
            };
            let coordinates = handle.coordinate_record();
            match storage {
                Storage::Paged(image) => {
                    export_attached_fits(&request, image.as_ref(), coordinates, &entries, options)
                }
                Storage::Temp {
                    image,
                    fits: Some(fits),
                    ..
                } => {
                    let view = fits.image_view(image);
                    export_attached_fits(&request, &view, coordinates, &entries, options)
                }
                Storage::Temp { image, .. } => {
                    export_attached_fits(&request, image.as_ref(), coordinates, &entries, options)
                }
            }
            .map_err(raised)?;
            Ok(true)
        })
    }
}
