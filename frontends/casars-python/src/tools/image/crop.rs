// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_core::RecordValue;
use casa_images::{
    ImageInterface,
    analysis::{ImageCrop, ImageCropOptions, ensure_outfile_does_not_alias_input},
    image::image_history_rows,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::convert::IntList;

use super::PyImageTool;
use super::beam_view::BeamView;
use super::beams::subset_beam_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::Region;

fn copy(
    image: &dyn ImageInterface<f32>,
    coordinates: RecordValue,
    options: ImageCropOptions<'_>,
    beams: &BeamView,
    path: Option<&Path>,
) -> PyResult<Storage<f32>> {
    let mut crop =
        ImageCrop::new(image, Some(coordinates), options, Some(beams.record())).map_err(raised)?;
    let record = crop.section.coordinate_record.take();
    let beam_set = beams.beam_set().map_err(raised)?;
    let mut info = crop.section.image_info().to_record();
    if beam_set.is_multi() {
        let channels = crop.beam_planes(
            image.find_axis(CoordinateType::Spectral),
            beam_set.n_channels(),
        );
        let polarizations =
            crop.beam_planes(image.find_axis(CoordinateType::Stokes), beam_set.n_stokes());
        subset_beam_record(
            &mut info,
            Some(beams.record()),
            &beam_set,
            &channels,
            &polarizations,
        )
        .map_err(raised)?;
    } else {
        beams.apply_to(&mut info);
    }
    let mut product = Storage::create(
        crop.section.shape.clone(),
        crop.section.coordinates.clone(),
        path,
        None,
    )
    .map_err(raised)?;
    with_storage!(&mut product, image_out => crop.copy_to(image, image_out.as_mut()))
        .map_err(raised)?;
    with_storage!(&mut product, image_out => image_out.set_image_info_record(info))
        .map_err(raised)?;
    if let Some(record) = record {
        match &mut product {
            Storage::Paged(image) => image.set_coordinate_record(record).map_err(raised)?,
            Storage::Temp {
                record: coordinates,
                ..
            } => *coordinates = record,
        }
    }
    Ok(product)
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", axes=None, overwrite=false, region=None, r#box="", chans="", stokes="", mask="", stretch=false, wantreturn=true), text_signature = "($self, outfile='', axes=[], overwrite=False, region={}, box='', chans='', stokes='', mask='', stretch=False, wantreturn=True)")]
    fn crop(
        &self,
        py: Python<'_>,
        outfile: &str,
        axes: Option<IntList>,
        overwrite: bool,
        region: Option<&Bound<'_, PyAny>>,
        r#box: &str,
        chans: &str,
        stokes: &str,
        mask: &str,
        stretch: bool,
        wantreturn: bool,
    ) -> PyResult<Py<Self>> {
        let record_argument = region.is_none_or(|value| value.is_instance_of::<PyDict>());
        let omitted_history = match region {
            _ if record_argument => "{...}",
            Some(value) if value.is_instance_of::<PyString>() => "\"\"",
            _ => "[]",
        };
        let region = Region::capture(py, region);
        let result = py.detach(|| {
            let state = self.state();
            let Some(attached) = state.image.as_ref() else {
                return Ok(None);
            };
            let region = region?;
            publish_image(outfile, overwrite, |path| {
                let mut original = attached.lock();
                if !matches!(&*original, ImageHandle::Float(_)) {
                    return Err(raised("This method only supports Float valued images"));
                }
                if record_argument || region.is_some() {
                    if !r#box.is_empty() {
                        return Err(raised("box and regionPtr cannot be simultaneously specified"));
                    }
                    if !chans.is_empty() || !stokes.is_empty() {
                        return Err(raised(
                            "regionPtr and regionName, chans, and/or stokes cannot be simultaneously specified",
                        ));
                    }
                }
                if !outfile.is_empty() && let Some(source) = original.path() {
                    ensure_outfile_does_not_alias_input(Path::new(outfile), source).map_err(raised)?;
                }
                attached.require_pixel_read_lock(&mut original)?;
                let coordinates = original.coordinate_record();
                let ImageHandle::Float(source) = &*original else {
                    unreachable!("only float images accept crop")
                };
                let axes = axes.as_ref().map_or(&[][..], |axes| axes.0.as_slice());
                let options = ImageCropOptions {
                    axes,
                    box_selection: (!r#box.is_empty()).then_some(r#box),
                    region: region.as_ref().map(Region::as_region),
                    channels: (!chans.is_empty()).then_some(chans),
                    stokes: (!stokes.is_empty()).then_some(stokes),
                    mask: (!mask.is_empty()).then_some(mask),
                    stretch,
                };
                let beams = attached.beam_view().clone();
                let mut cropped = match source {
                    Storage::Temp { image, fits: Some(fits), .. } => {
                        copy(&fits.image_view(image), coordinates, options, &beams, path)
                    }
                    _ => with_storage!(source, image => copy(image.as_ref(), coordinates, options, &beams, path)),
                }?;
                let history = match source {
                    Storage::Paged(image) => image_history_rows(image.name().expect("native image path")).map_err(raised)?,
                    Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
                };
                with_storage!(&mut cropped, image => image.append_history_entries(&history)).map_err(raised)?;
                let mut cropped = ImageHandle::Float(cropped);
                if state.history {
                    let axes = axes.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
                    let region = region.as_ref().map_or_else(|| omitted_history.into(), Region::history);
                    cropped.record_history("crop", format!("ia.crop(outfile={outfile:?}, axes=[{axes}], overwrite={overwrite}, region={region}, box={box:?}, chans={chans:?}, stokes={stokes:?}, mask={mask:?}, stretch={stretch}, wantreturn={wantreturn})", box = r#box))?;
                }
                let source = original.path().map_or_else(
                    || "Temporary_Image".into(),
                    |path| path.display().to_string(),
                );
                cropped.append_history_at(
                    "::ImageCropper::_reportOldNewImageShapes",
                    &[
                        format!("Original {source} size => {:?}", original.shape()),
                        format!("New {outfile} size => {:?}", cropped.shape()),
                    ],
                )?;
                cropped.save().map_err(raised)?;
                attached.release_requested_lock(&mut original)?;
                Ok(cropped)
            }).map(Some)
        })?;
        match result {
            Some(attached) if wantreturn => Py::new(py, Self::attached(attached)),
            other => {
                py.detach(|| drop(other));
                Py::new(py, Self::new())
            }
        }
    }
}
