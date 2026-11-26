// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_core::RecordValue;
use casa_images::{
    ImageInterface,
    analysis::{ImageRebin, ImageRebinOptions, ensure_outfile_does_not_alias_input},
    image::image_history_rows,
    image_expr::ImageExprValue,
};
use pyo3::prelude::*;

use crate::convert::IntList;

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::beam_view::BeamView;
use super::beams::subset_beam_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::{Region, mask_argument};

fn copy<T: ImageExprValue>(
    input: &dyn ImageInterface<T>,
    coordinates: RecordValue,
    options: ImageRebinOptions<'_>,
    beams: &BeamView,
    path: Option<&Path>,
) -> PyResult<Storage<T>> {
    let rebin = ImageRebin::new(input, coordinates, options, beams.record()).map_err(raised)?;
    let section = rebin.section();
    let mut info = section.image_info().to_record();
    let source_beams = beams.beam_set().map_err(raised)?;
    if source_beams.is_multi() {
        let channels = rebin.beam_planes(
            input.find_axis(CoordinateType::Spectral),
            source_beams.n_channels(),
        );
        let stokes = rebin.beam_planes(
            input.find_axis(CoordinateType::Stokes),
            source_beams.n_stokes(),
        );
        subset_beam_record(
            &mut info,
            Some(beams.record()),
            &source_beams,
            &channels,
            &stokes,
        )
        .map_err(raised)?;
    } else {
        beams.apply_to(&mut info);
    }
    let mut product = Storage::create(
        section.shape.clone(),
        section.coordinates.clone(),
        path,
        None,
    )
    .map_err(raised)?;
    with_storage!(&mut product, output => rebin.copy_to(input, output.as_mut())).map_err(raised)?;
    with_storage!(&mut product, output => output.set_image_info_record(info)).map_err(raised)?;
    if let Some(record) = &section.coordinate_record {
        match &mut product {
            Storage::Paged(image) => image
                .set_coordinate_record(record.clone())
                .map_err(raised)?,
            Storage::Temp {
                record: coordinates,
                ..
            } => coordinates.clone_from(record),
        }
    }
    Ok(product)
}

fn rebin_storage<T: ImageExprValue>(
    source: &Storage<T>,
    coordinates: RecordValue,
    options: ImageRebinOptions<'_>,
    beams: &BeamView,
    path: Option<&Path>,
) -> PyResult<Storage<T>> {
    with_storage!(source, image => copy(image.as_ref(), coordinates, options, beams, path))
}

#[pymethods]
impl PyImageTool {
    /// Average selected pixels in bins and return the rebinned image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile, bin, region=None, mask=None, dropdeg=false, overwrite=false, stretch=false, crop=false), text_signature = "($self, outfile, bin, region='', mask='', dropdeg=False, overwrite=False, stretch=False, crop=False)")]
    fn rebin(
        &self,
        py: Python<'_>,
        outfile: &str,
        bin: IntList,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: bool,
        overwrite: bool,
        stretch: bool,
        crop: bool,
    ) -> PyResult<Py<Self>> {
        let region = Region::capture(py, region);
        let mask = mask_argument(mask);
        let product = py.detach(|| {
            let state = self.state();
            let attachment = state.image.as_ref().ok_or_else(|| raised("Unable to create image"))?;
            let region = region?;
            let mask = mask?;
            let mut live_coordinates = None;
            let mut product = publish_image(outfile, overwrite, |path| {
                let mut handle = attachment.lock();
                match &*handle {
                    ImageHandle::Double(_) => return Err(raised("rebin does not support images with double precision pixel values.")),
                    ImageHandle::DComplex(_) => return Err(raised("rebin does not support images with complex<double> precision pixel values.")),
                    _ => {}
                }
                let factors = bin.0.iter().map(|&n| {
                    usize::try_from(n).map_err(|_| raised("All factors must be positive"))
                }).collect::<PyResult<Vec<_>>>()?;
                if !outfile.is_empty() {
                    for source in handle.path().into_iter().chain(attachment.operand_paths().iter().map(|path| path.as_path())) {
                        ensure_outfile_does_not_alias_input(Path::new(outfile), source).map_err(raised)?;
                    }
                }
                attachment.require_pixel_read_lock(&mut handle)?;
                let beams = attachment.beam_view().clone();
                let coordinates = handle.coordinate_record();
                let options = ImageRebinOptions {
                    factors: &factors,
                    region: region.as_ref().map(Region::as_region),
                    mask: (!mask.is_empty()).then_some(mask.as_str()),
                    stretch,
                    dropdeg,
                    crop,
                };
                let mut output = match &*handle {
                    ImageHandle::Float(Storage::Temp { image, fits: Some(fits), .. }) => {
                        ImageHandle::Float(copy(&fits.image_view(image), coordinates, options, &beams, path)?)
                    }
                    ImageHandle::Float(source) => ImageHandle::Float(rebin_storage(source, coordinates, options, &beams, path)?),
                    ImageHandle::Complex(source) => ImageHandle::Complex(rebin_storage(source, coordinates, options, &beams, path)?),
                    _ => unreachable!("double precision is checked before rebinning"),
                };
                let history = with_pixels!(&*handle, source => match source {
                    Storage::Paged(image) => image_history_rows(image.name().expect("native image path")).map_err(raised)?,
                    Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
                });
                with_pixels!(&mut output, dest => with_storage!(dest, image => image.append_history_entries(&history))).map_err(raised)?;
                if state.history {
                    let region = region.as_ref().map_or_else(|| "\"\"".into(), Region::history);
                    let factors = factors.iter().map(usize::to_string).collect::<Vec<_>>().join(",");
                    output.record_history("rebin", format!("ia.rebin(outfile={outfile:?}, bin=[{factors}], region={region}, mask={mask:?}, dropdeg={dropdeg}, overwrite={overwrite}, stretch={stretch}, crop={crop})"))?;
                }
                if path.is_some() {
                    live_coordinates = AttachedImage::capture_live_coordinates(&output)?;
                }
                output.save().map_err(raised)?;
                attachment.release_requested_lock(&mut handle)?;
                Ok(output)
            })?;
            if let Some((record, system)) = live_coordinates {
                product.keep_live_coordinates(record, system);
            }
            Ok::<_, PyErr>(product)
        })?;
        Py::new(py, Self::attached(product))
    }
}
