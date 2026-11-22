// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_images::{
    ImageInterface,
    analysis::{ImagePad, ImagePadOptions, ensure_outfile_does_not_alias_input},
    image::image_history_rows,
};
use casa_types::RecordValue;
use pyo3::prelude::*;

use super::PyImageTool;
use super::beam_view::BeamView;
use super::beams::subset_beam_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::{RegionArgument, require_bool};

fn copy(
    image: &dyn ImageInterface<f32>,
    coordinates: RecordValue,
    options: ImagePadOptions<'_>,
    beams: &BeamView,
    path: Option<&Path>,
) -> PyResult<ImageHandle> {
    let mut pad = ImagePad::new(image, coordinates, options, beams.record()).map_err(raised)?;
    let info = {
        let source = beams.beam_set().map_err(raised)?;
        let mut info = pad.section.image_info().to_record();
        if source.is_multi() {
            let channels = pad.beam_planes(
                image.find_axis(CoordinateType::Spectral),
                source.n_channels(),
            );
            let polarizations =
                pad.beam_planes(image.find_axis(CoordinateType::Stokes), source.n_stokes());
            subset_beam_record(
                &mut info,
                Some(beams.record()),
                &source,
                &channels,
                &polarizations,
            )
            .map_err(raised)?;
        } else {
            beams.apply_to(&mut info);
        }
        info
    };
    let record = pad
        .section
        .coordinate_record
        .take()
        .expect("padded coordinate record");
    let mut output = Storage::create(
        pad.section.shape.clone(),
        pad.section.coordinates.clone(),
        path,
        None,
    )
    .map_err(raised)?;
    with_storage!(&mut output, dest => pad.copy_to(image, dest.as_mut())).map_err(raised)?;
    with_storage!(&mut output, dest => dest.set_image_info_record(info)).map_err(raised)?;
    let mut output = ImageHandle::Float(output);
    output.set_coordinate_record(record)?;
    Ok(output)
}

#[pymethods]
impl PyImageTool {
    /// Select an image region and pad the two direction axes.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", npixels=1, value=0.0, padmask=false, overwrite=false, region=RegionArgument::default(), r#box="", chans="", stokes="", mask="", stretch=false, wantreturn=true), text_signature = "($self, outfile='', npixels=1, value=0.0, padmask=False, overwrite=False, region={}, box='', chans='', stokes='', mask='', stretch=False, wantreturn=True)")]
    fn pad(
        &self,
        py: Python<'_>,
        outfile: &str,
        npixels: i64,
        value: f64,
        #[pyo3(from_py_with = require_bool)] padmask: bool,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        region: RegionArgument,
        r#box: &str,
        chans: &str,
        stokes: &str,
        mask: &str,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        #[pyo3(from_py_with = require_bool)] wantreturn: bool,
    ) -> PyResult<Py<Self>> {
        let output = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else { return Ok(None) };
            if !matches!(&*attachment.lock(), ImageHandle::Float(_)) {
                return Err(raised("This method only supports Float valued images"));
            }
            let pixels = usize::try_from(npixels).ok().filter(|&count| count > 0)
                .ok_or_else(|| raised("Value of npixels must be greater than zero"))?;
            region.validate_selectors(r#box, chans, stokes)?;
            if !outfile.is_empty() && !overwrite && Path::new(outfile).symlink_metadata().is_ok() {
                return Err(raised(format!(
                    "Replaceable flag is false and there is already a file or directory named {outfile} so the output image will not be written"
                )));
            }
            let output = publish_image(outfile, overwrite, |path| {
                let mut input = attachment.lock();
                if !outfile.is_empty() {
                    for source in input.path().into_iter().chain(attachment.operand_paths().iter().map(|path| path.as_path())) {
                        ensure_outfile_does_not_alias_input(Path::new(outfile), source).map_err(raised)?;
                    }
                }
                attachment.require_pixel_read_lock(&mut input)?;
                let staged: PyResult<ImageHandle> = (|| {
                    let ImageHandle::Float(source) = &*input else {
                        unreachable!("checked float pixels")
                    };
                    let options = ImagePadOptions {
                        pixels,
                        value: value as f32,
                        padmask,
                        region: region.selection(),
                        box_selection: (!r#box.is_empty()).then_some(r#box),
                        channels: (!chans.is_empty()).then_some(chans),
                        stokes: (!stokes.is_empty()).then_some(stokes),
                        mask: (!mask.is_empty()).then_some(mask),
                        stretch,
                        destination: (!outfile.is_empty()).then(|| Path::new(outfile)),
                    };
                    let coordinates = input.coordinate_record();
                    let beams = attachment.beam_view().clone();
                    let history = match source {
                        Storage::Paged(image) => image_history_rows(image.name().expect("native image path")).map_err(raised)?,
                        Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
                    };
                    let mut output = match source {
                        Storage::Temp { image, fits: Some(fits), .. } => {
                            copy(&fits.image_view(image), coordinates, options, &beams, path)
                        }
                        _ => with_storage!(source, image => copy(image.as_ref(), coordinates, options, &beams, path)),
                    }?;
                    let ImageHandle::Float(dest) = &mut output else {
                        unreachable!("padding produces float pixels")
                    };
                    with_storage!(dest, image => image.append_history_entries(&history)).map_err(raised)?;
                    if state.history {
                        let selected = region.history();
                        let value = if value.is_nan() { "nan".into() } else { format!("{value:.6}") };
                        output.record_history("pad", format!("ia.pad(outfile={outfile:?}, npixels={npixels}, value={value}, padmask={padmask}, overwrite={overwrite}, region={selected}, box={box:?}, chans={chans:?}, stokes={stokes:?}, mask={mask:?}, stretch={stretch}, wantreturn={wantreturn})", box = r#box))?;
                    }
                    let source_name = input.path().map_or_else(
                        || "Temporary_Image".into(),
                        |path| path.display().to_string(),
                    );
                    output.append_history_at(
                        "::ImagePadder::_reportOldNewImageShapes",
                        &[
                            format!("Original {source_name} size => {:?}", input.shape()),
                            format!("New {outfile} size => {:?}", output.shape()),
                        ],
                    )?;
                    output.save().map_err(raised)?;
                    Ok(output)
                })();
                let released = attachment.release_requested_lock(&mut input);
                let result = staged?;
                released?;
                Ok(result)
            })?;
            Ok::<_, PyErr>(wantreturn.then_some(output))
        })?;
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}
