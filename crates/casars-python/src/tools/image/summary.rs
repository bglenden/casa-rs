// SPDX-License-Identifier: LGPL-3.0-or-later

mod coordinates;
mod plane_beams;

use std::fmt::Write;

use casa_coordinates::CoordinateSystem;
use casa_coordinates::display::format_stream_value;
use casa_images::{ImageInterface, ImageType};
use casa_types::measures::doppler::DopplerRef;
use pyo3::prelude::*;

use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::beam_view::BeamView;
use super::beams::{image_info, restoring_beam};
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

fn dimension_list(values: &[usize]) -> CasaValue {
    CasaValue::Ints {
        shape: vec![values.len()],
        values: values.iter().map(|&value| value as i64).collect(),
    }
}

fn text_list(values: Vec<String>) -> CasaValue {
    if values.iter().all(String::is_empty) {
        CasaValue::TextArray {
            shape: vec![values.len()],
            values,
        }
    } else {
        CasaValue::Texts(values)
    }
}

fn mask_description(masks: &[String], default: &str, has_mask: bool) -> String {
    match masks {
        [] if has_mask => "Parent is masked".into(),
        [] => "None".into(),
        [only] if only == default => default.into(),
        _ => {
            let other = masks
                .iter()
                .filter(|name| name.as_str() != default)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            format!("{default} [{other}]")
        }
    }
}

fn describe(
    image: &ImageHandle,
    beam_view: &BeamView,
    doppler: DopplerRef,
    list: bool,
    pixel_order: bool,
    verbose: bool,
    logs: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let shape = image.shape();
    let tile = if let Some(fits) = image.fits() {
        fits.tile_shape.clone()
    } else {
        with_pixels!(image, storage => match storage {
            Storage::Paged(image) => image.tile_shape().to_vec(),
            Storage::Temp { image, .. } if image.is_in_memory() => image.shape().to_vec(),
            Storage::Temp { image, .. } => {
                let lattice: &dyn ImageInterface<_> = image.as_ref();
                lattice.nice_cursor_shape()
            }
        })
    };
    let (has_mask, default, masks, units) = with_pixels!(image, storage =>
    with_storage!(storage, image => (
        image.has_pixel_mask(),
        image.default_mask_name().unwrap_or_default(),
        image.mask_names(),
        image.units().to_owned(),
    )));
    let has_mask = image.fits().map_or(has_mask, |fits| fits.has_mask);
    let regions = with_pixels!(image, storage => match storage {
        Storage::Paged(image) => image.region_names(),
        Storage::Temp { .. } => Vec::new(),
    });
    let name = image
        .path()
        .and_then(|path| path.file_name())
        .map_or_else(|| "Temporary_Image".into(), |name| name.to_string_lossy());
    let kind = image.image_type_name();
    let info = image_info(image).map_err(raised)?;
    let image_type = match info.image_type {
        ImageType::Undefined => ImageType::Intensity,
        other => other,
    }
    .to_string();
    let masks_text = mask_description(&masks, &default, has_mask);
    let regions_text = if regions.is_empty() {
        "None".into()
    } else {
        regions.join(", ")
    };
    let mut header = format!(
        "\nImage name       : {name}\nObject name      : {}\nImage type       : {kind}\nImage quantity   : {image_type}\nPixel mask(s)    : {masks_text}\nRegion(s)        : {regions_text}",
        info.object_name
    );
    if !units.is_empty() {
        write!(header, "\nImage units      : {units}").unwrap();
    }
    let record = image.coordinate_record();
    let coords = CoordinateSystem::from_record(&record).map_err(raised)?;
    let mut fields = coordinates::fields(&record, pixel_order)?;
    let beams = beam_view.beam_set().map_err(raised)?;
    if let Some(beam) = beams.single_beam() {
        let width_unit = if beam.minor_in("deg").map_err(raised)? < 1.0 {
            "arcsec"
        } else {
            "deg"
        };
        let major = format_stream_value(beam.major_in(width_unit).map_err(raised)?);
        let minor = format_stream_value(beam.minor_in(width_unit).map_err(raised)?);
        let pa = format_stream_value(plane_beams::normalized_angle(&beam)?);
        write!(
            header,
            "\nRestoring Beam   : {major} {width_unit}, {minor} {width_unit}, {pa} deg"
        )
        .unwrap();
        fields.push((
            "restoringbeam",
            restoring_beam(beam_view, &coords, -1, -1, "list")?,
        ));
    } else if beams.is_multi() {
        plane_beams::append(&mut header, &record, &coords, &beams, list && verbose)?;
        fields.push((
            "perplanebeams",
            restoring_beam(beam_view, &coords, -1, -1, "list")?,
        ));
    }

    let mut messages = vec![
        header,
        coordinates::report(&record, &coords, shape, &tile, doppler)?,
    ];
    if list {
        logs.extend(messages.drain(..).map(LogLine::info));
    }
    fields.extend([
        ("messages", text_list(messages)),
        ("ndim", CasaValue::Int(shape.len() as i64)),
        ("shape", dimension_list(shape)),
        ("tileshape", dimension_list(&tile)),
        ("unit", CasaValue::Text(units)),
        ("hasmask", CasaValue::Bool(has_mask)),
        ("defaultmask", CasaValue::Text(default)),
        ("masks", text_list(masks)),
        ("imagetype", CasaValue::Text(image_type)),
    ]);
    Ok(CasaValue::record(fields))
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (doppler="RADIO", list=true, pixelorder=true, verbose=false), text_signature = "($self, doppler='RADIO', list=True, pixelorder=True, verbose=False)")]
    fn summary(
        &self,
        py: Python<'_>,
        doppler: &str,
        list: bool,
        pixelorder: bool,
        verbose: bool,
    ) -> PyResult<Py<PyAny>> {
        let mut logs = Vec::new();
        let result = py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(CasaValue::empty_record());
            };
            let doppler = if doppler.eq_ignore_ascii_case("TRUE") {
                DopplerRef::BETA
            } else {
                doppler.parse().unwrap_or_else(|_| {
                    logs.push(LogLine::warning("Illegal velocity type, using RADIO"));
                    DopplerRef::RADIO
                })
            };
            let mut image = attachment.lock();
            attachment.require_lock(&mut image, casa_tables::LockType::Read)?;
            describe(
                &image,
                &attachment.beam_view(),
                doppler,
                list,
                pixelorder,
                verbose,
                &mut logs,
            )
        });
        emit(py, "casars.tools.image", &logs)?;
        result?.into_python(py)
    }
}
