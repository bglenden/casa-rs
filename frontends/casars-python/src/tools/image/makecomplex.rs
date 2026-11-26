// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_core::{RecordValue, Value};
use casa_images::analysis::{SubimageOptions, SubimagePlan, ensure_outfile_does_not_alias_input};
use casa_images::image::image_history_rows;
use casa_images::{ImageInterface, ImagePixel};
use num_complex::Complex;
use pyo3::prelude::*;

use super::PyImageTool;
use super::beams::{image_info, image_info_record, plane_axes, stored_beam_record};
use super::conformance::coordinates_conform;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::Region;

fn combine<T>(
    real: &dyn ImageInterface<T>,
    imaginary: &dyn ImageInterface<T>,
    real_record: &RecordValue,
    region: Option<&Region>,
    output_path: Option<&Path>,
    wrap: impl FnOnce(Storage<Complex<T>>) -> ImageHandle,
) -> PyResult<(ImageHandle, Vec<usize>)>
where
    T: ImagePixel + Copy,
    Complex<T>: ImagePixel + Copy,
{
    let selection = SubimageOptions {
        region: region.map(Region::as_region),
        dropdeg: false,
        keepaxes: &[],
    };
    let plan = SubimagePlan::new(real, Some(real_record.clone()), selection).map_err(raised)?;
    let mut storage = Storage::create(
        plan.shape.clone(),
        plan.coordinates.clone(),
        output_path,
        None,
    )
    .map_err(raised)?;
    with_storage!(&mut storage, output => plan.make_complex_to(real, imaginary, output.as_mut(), region.map(Region::as_region)))
        .map_err(raised)?;
    let mut result = wrap(storage);
    let origin = plan.bounds().0.to_vec();
    if let Some(record) = plan.coordinate_record {
        result.set_coordinate_record(record)?;
    }
    Ok((result, origin))
}

fn preserve_beam_records(
    real: &ImageHandle,
    output: &mut ImageHandle,
    coordinates: &RecordValue,
    origin: &[usize],
) -> PyResult<()> {
    let Some(stored) = image_info_record(real) else {
        return Ok(());
    };
    let source = image_info(real).map_err(raised)?.beam_set;
    let result = image_info(output).map_err(raised)?.beam_set;
    let mut record = stored.clone();
    record.remove("restoringbeam");
    record.remove("perplanebeams");
    let (spectral, stokes) = plane_axes(coordinates)?;
    let offset = |axis: Option<usize>, length: usize| {
        axis.and_then(|axis| origin.get(axis).copied())
            .filter(|_| length > 1)
            .unwrap_or(0)
    };
    let first_channel = offset(spectral, source.n_channels());
    let first_stokes = offset(stokes, source.n_stokes());
    let beam = |channel: usize, stokes: usize| {
        let channel = (first_channel + channel).min(source.n_channels().saturating_sub(1));
        let stokes = (first_stokes + stokes).min(source.n_stokes().saturating_sub(1));
        Value::Record(stored_beam_record(Some(stored), &source, channel, stokes))
    };
    if result.is_single() {
        record.upsert("restoringbeam", beam(0, 0));
    } else if result.is_multi() {
        let mut grid = result.to_record();
        for polarization in 0..result.n_stokes() {
            for channel in 0..result.n_channels() {
                let index = polarization * result.n_channels() + channel;
                grid.upsert(format!("*{index}"), beam(channel, polarization));
            }
        }
        record.upsert("perplanebeams", Value::Record(grid));
    }
    with_pixels!(output, storage => with_storage!(storage, image => image.set_image_info_record(record))).map_err(raised)
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile, imag, region=None, overwrite=false), text_signature = "($self, outfile, imag, region={}, overwrite=False)")]
    fn makecomplex(
        &self,
        py: Python<'_>,
        outfile: &str,
        imag: &str,
        region: Option<&Bound<'_, PyAny>>,
        overwrite: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let region = Region::argument(py, region, "makecomplex")?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(false);
            };
            let product = publish_image(outfile, overwrite, |path| {
                let mut real = attachment.lock();
                if !matches!(*real, ImageHandle::Float(_) | ImageHandle::Double(_)) {
                    return Err(raised("The attached image must be float valued"));
                }
                attachment.require_pixel_read_lock(&mut real)?;
                let imaginary = ImageHandle::open_read(Path::new(imag)).map_err(raised)?;
                if !matches!(imaginary, ImageHandle::Float(_) | ImageHandle::Double(_)) {
                    return Err(raised(format!("{imag} does not have supported real valued pixels")));
                }
                if real.kind() != imaginary.kind() {
                    return Err(raised("Real and imaginary images do not have the same precision"));
                }
                if !outfile.is_empty() {
                    for source in [real.path(), imaginary.path()].into_iter().flatten() {
                        ensure_outfile_does_not_alias_input(Path::new(outfile), source)
                            .map_err(raised)?;
                    }
                }
                if real.shape() != imaginary.shape() {
                    return Err(raised("Image shapes are not identical"));
                }
                let real_record = real.coordinate_record();
                if !coordinates_conform(&real_record, &imaginary.coordinate_record(), false)? {
                    return Err(raised("Image Coordinate systems are not conformant"));
                }
                let (mut output, origin) = match (&*real, &imaginary) {
                    (ImageHandle::Float(left), ImageHandle::Float(right)) => {
                        left.with_float_view(|left| right.with_float_view(|right| {
                            combine(left, right, &real_record, region.as_ref(), path, ImageHandle::Complex)
                        }))?
                    }
                    (ImageHandle::Double(left), ImageHandle::Double(right)) => {
                        with_storage!(left, left => with_storage!(right, right => {
                            combine(left.as_ref(), right.as_ref(), &real_record, region.as_ref(), path, ImageHandle::DComplex)
                        }))?
                    }
                    _ => unreachable!("real images with matching precision"),
                };
                preserve_beam_records(&real, &mut output, &real_record, &origin)?;
                let rows = with_pixels!(&*real, storage => match storage {
                    Storage::Paged(image) => image_history_rows(image.name().expect("persistent image")),
                    Storage::Temp { image, .. } => Ok(image.history_rows().map_err(raised)?.into_owned()),
                })
                .map_err(raised)?;
                with_pixels!(&mut output, storage => with_storage!(storage, image => image.append_history_entries(&rows)))
                    .map_err(raised)?;
                if state.history {
                    let region = region.as_ref().map_or_else(|| "{}".into(), Region::history);
                    output.record_history("makecomplex", format!(
                        "ia.makecomplex(outfile={outfile:?}, imag={imag:?}, region={region}, overwrite={overwrite})"
                    ))?;
                }
                output.save().map_err(raised)?;
                Ok(output)
            })?;
            drop(product);
            Ok(true)
        })
    }
}
