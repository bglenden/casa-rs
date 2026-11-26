// SPDX-License-Identifier: LGPL-3.0-or-later
//! Template acquisition and persistent products for the `imregrid` task.

mod rotation;
mod tool;

pub use tool::{ImageRegridOptions, ImageRegridProduct, ImageRegridTarget, regrid_image};

use std::io;
use std::path::{Path, PathBuf};

use super::ensure_outfile_does_not_alias_input;
use crate::regrid::{
    RegridMethod, RegridPixel, RegridTemplate, Regridded, TaskRegridOptions, frame_from_template,
    regrid_direction_frame, regrid_task,
};
use crate::{
    AnyPagedImage, ImageBeamSet, ImageError, ImageInfo, ImagePixel, ImageRegridSummary,
    ImregridOutcome, ImregridRequest, ImregridTemplate, PagedImage,
};
use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_tables::publication::{
    exchange_directories, lock_table_tree_for_replacement, rename_without_replacing,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::DirectionRef;
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

/// Return an image's coordinate template, or regrid its pixels onto a template.
///
/// A dictionary template holds the two fields returned by `template='get'`.
/// A template image supplies its coordinates and dimensions in the same form.
///
/// # Errors
/// Returns an error for unreadable images, incompatible templates, unsupported
/// pixel or coordinate types, and failed output publication.
pub fn imregrid(request: &ImregridRequest) -> Result<ImregridOutcome, ImageError> {
    let input = AnyPagedImage::open(&request.imagename)?;
    if matches!(&request.template, ImregridTemplate::Image(path) if path.as_os_str().eq_ignore_ascii_case("get"))
    {
        let shape = input
            .shape()
            .iter()
            .map(|&length| i32::try_from(length))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("image shape exceeds the range of CASA's template record"))?;
        let mut coordinates = coordinate_record(&input);
        // casacore's coordsys.torecord() returns the spectral WCS string
        // without the fixed-width padding found in stored image records.
        for field in coordinates.fields_mut() {
            if field.name.starts_with("spectral")
                && let Value::Record(spectral) = &mut field.value
                && let Some(Value::Record(wcs)) = spectral.get_mut("wcs")
                && let Some(Value::Scalar(ScalarValue::String(ctype))) = wcs.get_mut("ctype")
            {
                ctype.truncate(ctype.trim_end_matches('\0').len());
            }
        }
        let template = Value::Record(RecordValue::new(vec![
            casa_types::RecordField::new("csys", Value::Record(coordinates)),
            casa_types::RecordField::new("shap", Value::Array(ArrayValue::from_i32_vec(shape))),
        ]));
        return Ok(ImregridOutcome::Template(template));
    }
    let template = read_template(&request.template)?;
    match input {
        AnyPagedImage::Float32(image) => write_product(&image, request, &template),
        AnyPagedImage::Float64(image) => write_product(&image, request, &template),
        AnyPagedImage::Complex32(image) => write_product(&image, request, &template),
        AnyPagedImage::Complex64(image) => write_product(&image, request, &template),
    }
    .map(ImregridOutcome::Product)
}

fn image_coordinates<T: ImagePixel>(image: &PagedImage<T>) -> RecordValue {
    match image.table().keywords().get("coords") {
        Some(Value::Record(record)) => record.clone(),
        _ => image.coordinates().to_record(),
    }
}

fn coordinate_record(image: &AnyPagedImage) -> RecordValue {
    match image {
        AnyPagedImage::Float32(image) => image_coordinates(image),
        AnyPagedImage::Float64(image) => image_coordinates(image),
        AnyPagedImage::Complex32(image) => image_coordinates(image),
        AnyPagedImage::Complex64(image) => image_coordinates(image),
    }
}

enum Template {
    Grid(RegridTemplate),
    Frame(DirectionRef),
}

fn read_template(template: &ImregridTemplate) -> Result<Template, ImageError> {
    let (shape, record) = match template {
        ImregridTemplate::Image(path) => {
            if let Some(reference) = path.to_str().and_then(frame_from_template) {
                return Ok(Template::Frame(reference));
            }
            if !path.exists() {
                return Err(ImageError::TaskArgumentType {
                    parameter: "template",
                    message: format!("Cannot read template image {}", path.display()),
                });
            }
            let image = AnyPagedImage::open(path)?;
            (image.shape().to_vec(), coordinate_record(&image))
        }
        ImregridTemplate::Record { csys, shape } => {
            if shape.contains(&0) {
                return Err(invalid("template shape must contain positive integers"));
            }
            (shape.clone(), csys.clone())
        }
    };
    let coordinates = CoordinateSystem::from_record(&record)?;
    if shape.len() != coordinates.n_pixel_axes() {
        return Err(invalid(
            "template shape must match the number of pixel axes",
        ));
    }
    Ok(Template::Grid(RegridTemplate { coordinates, shape }))
}

fn write_product<T: RegridPixel>(
    input: &PagedImage<T>,
    request: &ImregridRequest,
    template: &Template,
) -> Result<ImageRegridSummary, ImageError> {
    let output = match (&request.template, request.output.as_os_str().is_empty()) {
        (ImregridTemplate::Image(_), true) => {
            let mut name = request.imagename.as_os_str().to_os_string();
            name.push(".regridded");
            PathBuf::from(name)
        }
        _ => request.output.clone(),
    };
    // The same-frame CASA path uses image.subimage with its overwrite default.
    let same_frame = match template {
        Template::Frame(reference) => input
            .coordinates()
            .find_coordinate(CoordinateType::Direction)
            .is_some_and(|index| {
                matches!(input.coordinates().coordinate(index),
                CoordinateModel::Direction(direction) if direction.direction_ref() == *reference)
            }),
        Template::Grid(_) => false,
    };
    let overwrite = request.overwrite && !same_frame;
    let destination = if output.as_os_str().is_empty() {
        None
    } else {
        let path = std::path::absolute(&output).map_err(output_io)?;
        check_output(request, &path, overwrite)?;
        Some(path)
    };
    let method = RegridMethod::from_casa(&request.interpolation)?;
    let (regridded, beam_rotation) = match template {
        Template::Grid(grid) => {
            let record_shape;
            let shape = if matches!(request.template, ImregridTemplate::Record { .. }) {
                record_shape = grid
                    .shape
                    .iter()
                    .map(|&length| length as i64)
                    .collect::<Vec<_>>();
                &record_shape
            } else {
                &request.shape
            };
            (
                regrid_task(
                    input,
                    grid,
                    TaskRegridOptions {
                        axes: &request.axes,
                        shape,
                        as_velocity: request.asvelocity,
                        decimate: request.decimate,
                        replicate: request.replicate,
                        method,
                        image_tool: None,
                    },
                )?,
                0.0,
            )
        }
        Template::Frame(reference) => {
            regrid_direction_frame(input, *reference, method, request.decimate)?
        }
    };
    let Regridded {
        data,
        mask,
        coordinates,
        ..
    } = regridded;
    let shape = data.shape().to_vec();
    let valid_pixels = mask.iter().filter(|&&bit| bit).count();
    if let Some(destination) = destination {
        let input_info = input.image_info()?;
        let info = output_image_info(
            input_info.clone(),
            input.coordinates(),
            &coordinates,
            beam_rotation,
        );
        let stage = tempfile::tempdir_in(destination.parent().ok_or(ImageError::NotPersistent)?)
            .map_err(output_io)?;
        let product = stage.path().join("image");
        let mut image = PagedImage::<T>::create(shape.clone(), coordinates, &product)?;
        image.set_units(input.units())?;
        if info == input_info
            && let Some(Value::Record(record)) = input.table().keywords().get("imageinfo")
        {
            image.set_image_info_record(record.clone())?;
        } else {
            image.set_image_info(&info)?;
        }
        image.set_misc_info(input.misc_info())?;
        image.put_slice(&data, &vec![0; shape.len()])?;
        if valid_pixels < data.len() {
            image.put_mask("mask0", &mask)?;
            image.set_default_mask("mask0")?;
        }
        image.append_history_entries(&crate::image::image_history_rows(&request.imagename)?)?;
        image.append_history_at(
            "::imregrid",
            &[
                concat!("version: casa-rs ", env!("CARGO_PKG_VERSION")).into(),
                format!(
                    "imregrid(imagename={:?}, output={:?}, interpolation={:?})",
                    request.imagename, output, request.interpolation
                ),
            ],
        )?;
        image.save()?;
        drop(image);
        match rename_without_replacing(&product, &destination) {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && overwrite => {
                check_output(request, &destination, overwrite)?;
                let _locks = lock_table_tree_for_replacement(&destination)?;
                exchange_directories(&product, &destination).map_err(output_io)?;
            }
            result => result.map_err(output_io)?,
        }
    }
    Ok(ImageRegridSummary {
        imagename: request.imagename.display().to_string(),
        template: match &request.template {
            ImregridTemplate::Image(path) => path.display().to_string(),
            ImregridTemplate::Record { .. } => "coordinate record".into(),
        },
        output: output.display().to_string(),
        shape,
        valid_pixels,
    })
}

fn output_image_info(
    mut info: ImageInfo,
    source: &CoordinateSystem,
    output: &CoordinateSystem,
    rotation: f64,
) -> ImageInfo {
    if info.beam_set.is_empty() {
        return info;
    }
    let stokes = |system: &CoordinateSystem| {
        system
            .find_coordinate(CoordinateType::Stokes)
            .and_then(|index| match system.coordinate(index) {
                CoordinateModel::Stokes(coordinate) => Some(coordinate.stokes().to_vec()),
                _ => None,
            })
    };
    let selections = if info.beam_set.n_stokes() > 1 {
        stokes(source).zip(stokes(output)).map(|(source, target)| {
            target
                .iter()
                .filter_map(|value| source.iter().position(|candidate| candidate == value))
                .collect::<Vec<_>>()
        })
    } else {
        None
    };
    if rotation == 0.0
        && selections
            .as_ref()
            .is_none_or(|planes| planes.len() == info.beam_set.n_stokes())
    {
        return info;
    }
    let planes = selections.unwrap_or_else(|| (0..info.beam_set.n_stokes()).collect());
    info.beam_set = ImageBeamSet::from_grid(
        (0..info.beam_set.n_channels())
            .map(|channel| {
                planes
                    .iter()
                    .map(|&pol| {
                        let mut beam = *info.beam_set.beam(channel, pol);
                        beam.position_angle += rotation;
                        beam
                    })
                    .collect()
            })
            .collect(),
    );
    info
}

fn check_output(
    request: &ImregridRequest,
    destination: &Path,
    overwrite: bool,
) -> Result<(), ImageError> {
    ensure_outfile_does_not_alias_input(destination, &request.imagename)?;
    if let ImregridTemplate::Image(path) = &request.template {
        ensure_outfile_does_not_alias_input(destination, path)?;
    }
    match destination.symlink_metadata() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(output_io(error)),
        Ok(_) if !overwrite => Err(invalid(format!(
            "imregrid output already exists: {}",
            destination.display()
        ))),
        Ok(metadata)
            if metadata.is_dir()
                && Table::open_metadata_only(TableOptions::new(destination))
                    .is_ok_and(|table| table.info().table_type == "Image") =>
        {
            Ok(())
        }
        Ok(_) => Err(invalid(format!(
            "{} is not a CASA image directory",
            destination.display()
        ))),
    }
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::InvalidMetadata(message.into())
}

fn output_io(error: io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}
