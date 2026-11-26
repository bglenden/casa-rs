// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_core::RecordValue;
use casa_imageanalysis::analysis::{
    DecimationMethod, DecimationPixel, ImageDecimation, ImageDecimationOptions,
    ensure_outfile_does_not_alias_input,
};
use casa_images::ImageInterface;
use casa_images::image_history_rows;
use pyo3::prelude::*;

use super::PyImageTool;
use super::beam_view::BeamView;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::Region;

fn copy<T: DecimationPixel>(
    input: &dyn ImageInterface<T>,
    coordinates: RecordValue,
    options: ImageDecimationOptions<'_>,
    beams: &BeamView,
    path: Option<&Path>,
) -> PyResult<Storage<T>> {
    let decimation =
        ImageDecimation::new(input, coordinates, options, beams.record()).map_err(raised)?;
    let section = decimation.section();
    let mut product = Storage::create(
        section.shape.clone(),
        section.coordinates.clone(),
        path,
        None,
    )
    .map_err(raised)?;
    let mask = with_storage!(&mut product, output => decimation.copy_to(input, output.as_mut()))
        .map_err(raised)?;
    if let Some(mask) = mask {
        match &mut product {
            Storage::Paged(image) => {
                image.put_mask("mask0", &mask).map_err(raised)?;
                image.set_default_mask("mask0").map_err(raised)?;
            }
            Storage::Temp { image, .. } => image.set_anonymous_mask(mask).map_err(raised)?,
        }
    }
    let mut info = section.image_info().to_record();
    beams.apply_to(&mut info);
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

fn decimate_storage<T: DecimationPixel>(
    source: &Storage<T>,
    coordinates: RecordValue,
    options: ImageDecimationOptions<'_>,
    beams: &BeamView,
    path: Option<&Path>,
) -> PyResult<Storage<T>> {
    with_storage!(source, image => copy(image.as_ref(), coordinates, options, beams, path))
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", axis=0, factor=1, method="copy", region=None, mask="", overwrite=false, stretch=false), text_signature = "($self, outfile='', axis=0, factor=1, method='copy', region='', mask='', overwrite=False, stretch=False)")]
    fn decimate(
        &self,
        py: Python<'_>,
        outfile: &str,
        axis: i64,
        factor: i64,
        method: &str,
        region: Option<&Bound<'_, PyAny>>,
        mask: &str,
        overwrite: bool,
        stretch: bool,
    ) -> PyResult<Py<Self>> {
        let region = Region::capture(py, region);
        let product = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            let region = region?;
            publish_image(outfile, overwrite, |path| {
                let mut handle = attachment.lock();
                if matches!(&*handle, ImageHandle::Double(_) | ImageHandle::DComplex(_)) {
                    return Err(raised("decimate does not support images with double precision pixel values"));
                }
                let axis = usize::try_from(axis).map_err(|_| raised("The value of axis cannot be negative"))?;
                let factor = usize::try_from(factor).map_err(|_| raised("The value of factor cannot be negative"))?;
                let function = match method.as_bytes().first().map(u8::to_ascii_lowercase) {
                    Some(b'c') => DecimationMethod::Copy,
                    Some(b'm') => DecimationMethod::Mean,
                    _ => return Err(raised(format!("Unsupported decimation method {method}"))),
                };
                if !outfile.is_empty() {
                    for source in handle.path().into_iter().chain(attachment.operand_paths().iter().map(PathBuf::as_path)) {
                        ensure_outfile_does_not_alias_input(Path::new(outfile), source).map_err(raised)?;
                    }
                }
                attachment.require_pixel_read_lock(&mut handle)?;
                let beams = attachment.beam_view().clone();
                let coordinates = handle.coordinate_record();
                let options = ImageDecimationOptions {
                    axis, factor, method: function,
                    region: region.as_ref().map(Region::as_region),
                    mask: (!mask.is_empty()).then_some(mask),
                    stretch,
                };
                let mut output = match &*handle {
                    ImageHandle::Float(Storage::Temp { image, fits: Some(fits), .. }) => {
                        ImageHandle::Float(copy(&fits.image_view(image), coordinates, options, &beams, path)?)
                    }
                    ImageHandle::Float(source) => ImageHandle::Float(decimate_storage(source, coordinates, options, &beams, path)?),
                    ImageHandle::Complex(source) => ImageHandle::Complex(decimate_storage(source, coordinates, options, &beams, path)?),
                    _ => unreachable!("double precision is checked before decimation"),
                };
                let history = with_pixels!(&*handle, source => match source {
                    Storage::Paged(image) => image_history_rows(image.name().expect("native image path")).map_err(raised)?,
                    Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
                });
                with_pixels!(&mut output, dest => with_storage!(dest, image => image.append_history_entries(&history))).map_err(raised)?;
                if state.history {
                    let region = region.as_ref().map_or_else(|| "\"\"".into(), Region::history);
                    output.record_history("decimate", format!("ia.decimate(outfile={outfile:?}, axis={axis}, factor={factor}, method={method:?}, region={region}, mask={mask:?}, overwrite={overwrite}, stretch={stretch})"))?;
                }
                if factor > 1 {
                    let operation = match function {
                        DecimationMethod::Copy => "Directly copying every i*nth plane in input to plane i in output.",
                        DecimationMethod::Mean => "Averaging every i to i*(n-1) planes in the input image to form plane i in the output image.",
                    };
                    let source = handle.path().map_or_else(|| "Temporary_Image".into(), |path| path.display().to_string());
                    output.append_history_at("::ImageDecimator::decimate", &[
                        format!("Decimated axis {axis} by keeping only every nth plane, where n={factor}. {operation}"),
                        format!("Original {source} size => {:?}", handle.shape()),
                        format!("New {outfile} size => {:?}", output.shape()),
                    ])?;
                }
                output.save().map_err(raised)?;
                attachment.release_requested_lock(&mut handle)?;
                Ok(output)
            }).map(Some)
        })?;
        Py::new(py, product.map_or_else(Self::new, Self::attached))
    }
}
