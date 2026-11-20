// SPDX-License-Identifier: LGPL-3.0-or-later
//! Position-velocity images extracted from a shared image selection.

mod geometry;
mod position;

use std::io;
use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
    LinearCoordinate,
};
use casa_provider_contracts::{ParameterValue, parameter_value_to_casacore};
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{Table, TableOptions};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayD, ArrayValue, RecordValue, Value};
use ndarray::{IxDyn, ShapeBuilder};

use super::axes::{
    direction_plane_axes, legacy_subimage_coordinates_with_stokes, stokes_for_axis,
    subimage_coordinates_with_stokes,
};
use super::imsubimage::{
    SubimageRegion,
    coords::{drop_axes_from_legacy_record, subset_beam_set},
    region::SubimageSelection,
};
use super::planes::unravel_index;
use super::{combined_selection_mask, ensure_outfile_does_not_alias_input};
use crate::regrid::{Plane, RegridMethod};
use crate::{ImageError, ImagePixel, ImpvRequest, PagedImage, PvImageSummary};

pub(super) fn run<T>(
    image: &PagedImage<T>,
    request: &ImpvRequest,
) -> Result<PvImageSummary, ImageError>
where
    T: ImagePixel + Into<f64> + Copy,
{
    let destination = std::path::absolute(&request.outfile).map_err(output_io)?;
    if request.outfile.as_os_str().is_empty() {
        return Err(ImageError::CasaSelection(
            "outfile must be specified".into(),
        ));
    }
    check_destination(&destination, request)?;
    let plane = direction_plane_axes(image)?;
    let direction = direction(image.coordinates())?;
    let width = position::width(&request.width, direction)?;
    let endpoints = position::endpoints(request, direction)?;
    let strip = geometry::Strip::new(direction, endpoints, plane.map(|i| image.shape()[i]), width)?;
    let selection = selection(image, request, plane)?;
    let mut mask =
        combined_selection_mask(image, &selection, request.mask.as_deref(), request.stretch)?;
    selection.apply_selection_masks(&mut mask);
    let input = image.get_slice(&selection.start, &selection.shape)?;
    let (data, valid) = sample(&input, mask.as_ref(), plane, &strip)?;
    if !valid.iter().any(|&bit| bit) {
        return Err(ImageError::CasaSelection(
            "All selected pixels are masked".into(),
        ));
    }
    let (coordinates, record, secondary) =
        output_coordinates(image, &selection, plane, &strip, &request.unit)?;
    let mut info = image.image_info()?;
    info.beam_set = subset_beam_set(
        &info.beam_set,
        image.coordinates(),
        image.ndim(),
        &selection.start,
        &selection.shape,
    )?;
    let mut misc = image.misc_info();
    misc.upsert("secondary_coordinates", Value::Record(secondary));
    let stage = tempfile::tempdir_in(destination.parent().ok_or(ImageError::NotPersistent)?)
        .map_err(output_io)?;
    let product = stage.path().join("image");
    let shape = data.shape().to_vec();
    let mut output = PagedImage::<f32>::create(shape.clone(), coordinates, &product)?;
    output.set_coords_record_keyword(record);
    output.set_units(image.units())?;
    output.set_image_info(&info)?;
    output.set_misc_info(misc)?;
    output.put_slice(&data, &vec![0; shape.len()])?;
    if valid.iter().any(|bit| !bit) {
        output.put_mask("mask0", &valid)?;
        output.set_default_mask("mask0")?;
    }
    output.append_history_entries(&crate::image::image_history_rows(&request.imagename)?)?;
    output.append_history_at("::impv", &[format!(
        "impv(imagename={:?}, outfile={:?}, mode={:?}, start={:?}, end={:?}, width={width}, unit={:?})",
        request.imagename.display().to_string(), request.outfile.display().to_string(), request.mode,
        request.start, request.end, request.unit,
    )])?;
    output.save()?;
    drop(output);
    match rename_without_replacing(&product, &destination) {
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && request.overwrite => {
            check_destination(&destination, request)?;
            exchange_directories(&product, &destination).map_err(output_io)?;
        }
        result => result.map_err(output_io)?,
    }
    Ok(PvImageSummary {
        imagename: request.imagename.display().to_string(),
        outfile: request.outfile.display().to_string(),
        shape,
        path_pixels: strip.shape[0],
        width,
        units: image.units().to_owned(),
    })
}

fn selection<T: ImagePixel>(
    image: &PagedImage<T>,
    request: &ImpvRequest,
    plane: [usize; 2],
) -> Result<SubimageSelection, ImageError> {
    let record = match request.region.as_ref() {
        Some(ParameterValue::Table(fields)) if !fields.is_empty() => {
            let Value::Record(record) =
                parameter_value_to_casacore(request.region.as_ref().expect("region is set"))
                    .map_err(|e| ImageError::CasaSelection(e.into()))?
            else {
                unreachable!("a parameter table converts to a record")
            };
            Some(record)
        }
        Some(ParameterValue::String(name))
            if !name.contains(['[', ':', '\n']) && !Path::new(name).exists() =>
        {
            image.get_region_record(name.trim()).ok()
        }
        _ => None,
    };
    let region = if let Some(record) = &record {
        Some(SubimageRegion::Record(record))
    } else {
        match request.region.as_ref() {
            Some(ParameterValue::String(text)) if !text.trim().is_empty() => {
                Some(SubimageRegion::Text(text))
            }
            _ => None,
        }
    };
    let chans = request
        .chans
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.eq_ignore_ascii_case("all") && !s.eq_ignore_ascii_case("none"));
    let stokes = request
        .stokes
        .as_deref()
        .map(str::trim)
        .map(|s| s.trim_start_matches('[').trim_end_matches(']').trim())
        .filter(|s| !s.eq_ignore_ascii_case("all") && !s.eq_ignore_ascii_case("none"))
        .map(|s| s.replace(';', ","));
    let resolved = SubimageSelection::resolve(image, None, region, chans, stokes.as_deref())?;
    if resolved.has_multiple_regions {
        return Err(ImageError::CasaSelection(
            "Only a single n-dimensional rectangular region is supported".into(),
        ));
    }
    if resolved.shape[plane[0]] != image.shape()[plane[0]]
        || resolved.shape[plane[1]] != image.shape()[plane[1]]
    {
        return Err(ImageError::CasaSelection(
            "You are not permitted to make a region selection in the direction coordinate".into(),
        ));
    }
    Ok(resolved)
}

fn sample<T>(
    input: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    plane: [usize; 2],
    strip: &geometry::Strip,
) -> Result<(ArrayD<f32>, ArrayD<bool>), ImageError>
where
    T: Into<f64> + Copy,
{
    let [x, y] = plane;
    let plane_shape = [input.shape()[x], input.shape()[y]];
    let input_axes: Vec<_> = (0..input.ndim())
        .filter(|axis| !plane.contains(axis))
        .collect();
    let plane_keys: Vec<_> = input_axes.iter().map(|&axis| input.shape()[axis]).collect();
    let offsets = (0..strip.shape[0])
        .map(|i| {
            (0..strip.shape[1])
                .map(|j| strip.input_pixel(i, j))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut out_shape = input.shape().to_vec();
    out_shape[x] = strip.shape[0];
    out_shape.remove(y);
    let mut result = ArrayD::zeros(IxDyn(&out_shape).f());
    let mut valid = ArrayD::from_elem(IxDyn(&out_shape).f(), false);
    let mut pixels = Vec::with_capacity(plane_shape[0] * plane_shape[1]);
    let mut good = Vec::with_capacity(pixels.capacity());
    let mut index = vec![0; input.ndim()];
    let method = if strip.rotated() {
        RegridMethod::Cubic
    } else {
        RegridMethod::Nearest
    };
    for i in 0..plane_keys.iter().product() {
        for (axis, pixel) in input_axes.iter().zip(unravel_index(i, &plane_keys)) {
            index[*axis] = pixel;
        }
        pixels.clear();
        good.clear();
        for j in 0..plane_shape[1] {
            index[y] = j;
            for k in 0..plane_shape[0] {
                index[x] = k;
                pixels.push(input[IxDyn(&index)].into());
                if let Some(mask) = mask {
                    good.push(mask[IxDyn(&index)]);
                }
            }
        }
        let plane = Plane {
            data: &pixels,
            mask: mask.map(|_| good.as_slice()),
            shape: plane_shape,
        };
        let mut output_index = index.clone();
        output_index.remove(y);
        for (i, section) in offsets.iter().enumerate() {
            let (sum, n) = section
                .iter()
                .filter_map(|&at| plane.sample(method, at))
                .fold((0.0, 0), |(sum, n), value| {
                    (sum + f64::from(value as f32), n + 1)
                });
            if n > 0 {
                output_index[x] = i;
                result[IxDyn(&output_index)] = (sum / f64::from(n)) as f32;
                valid[IxDyn(&output_index)] = true;
            }
        }
    }
    Ok((result, valid))
}

fn output_coordinates<T: ImagePixel>(
    image: &PagedImage<T>,
    selection: &SubimageSelection,
    plane: [usize; 2],
    strip: &geometry::Strip,
    unit: &str,
) -> Result<(CoordinateSystem, RecordValue, RecordValue), ImageError> {
    let unit = Unit::new(unit)
        .map_err(|_| ImageError::CasaSelection("PV offset unit must be angular".into()))?;
    let collapsed = strip.collapsed_direction()?;
    let increment = Quantity::new(strip.offset_increment(&collapsed)?, "rad")
        .and_then(|q| q.get_value_in(&unit))
        .map_err(|e| ImageError::CasaSelection(e.to_string()))?;
    let mut selected_stokes = selection.selected_stokes.clone();
    if selected_stokes.is_none()
        && let Some(axis) = selection.stokes_axis
        && selection.shape[axis] != image.shape()[axis]
    {
        let stokes = stokes_for_axis(image.coordinates(), axis)?;
        let start = selection.start[axis];
        selected_stokes = Some(stokes[start..start + selection.shape[axis]].to_vec());
    }
    let selected_stokes = selected_stokes.as_deref();
    let mut record = match legacy_subimage_coordinates_with_stokes(
        image,
        &selection.start,
        &selection.shape,
        selected_stokes,
    ) {
        Some(record) => record,
        None => subimage_coordinates_with_stokes(
            image.coordinates(),
            &selection.start,
            &selection.shape,
            selected_stokes,
        )?
        .to_record(),
    };
    let direction = CoordinateSystem::record_coordinates(&record)?
        .into_iter()
        .find(|entry| entry.field.starts_with("direction"))
        .ok_or_else(|| {
            ImageError::InvalidMetadata(
                "PV direction coordinate missing from the image record".into(),
            )
        })?;
    record.upsert(&direction.field, coordinate_record(collapsed, "direction0"));
    let secondary = record.clone();
    let suffix = direction.field.trim_start_matches("direction");
    let linear = LinearCoordinate::new(2, vec!["Offset".into(); 2], vec![unit.name().into(); 2])
        .with_increment(vec![increment; 2])
        .with_reference_pixel(vec![((strip.shape[0] - 1) / 2) as f64; 2]);
    record.remove(&direction.field);
    record.upsert(
        format!("linear{suffix}"),
        coordinate_record(linear, "linear0"),
    );
    let kept: Vec<_> = (0..image.ndim()).filter(|&axis| axis != plane[1]).collect();
    let mut record = drop_axes_from_legacy_record(&record, image.ndim(), &kept)
        .ok_or_else(|| ImageError::InvalidMetadata("cannot remove the collapsed PV axis".into()))?;
    let offset = kept
        .iter()
        .position(|&axis| axis == plane[0])
        .expect("offset axis kept") as i32;
    // casacore sets the removed linear axis to its own second axis index.
    for kind in ["pixelmap", "worldmap"] {
        record.upsert(
            format!("{kind}{suffix}"),
            Value::Array(ArrayValue::from_i32_vec(vec![offset, -2])),
        );
    }
    Ok((CoordinateSystem::from_record(&record)?, record, secondary))
}

fn coordinate_record(coordinate: impl Into<CoordinateModel>, name: &str) -> Value {
    let mut system = CoordinateSystem::new();
    system.add_coordinate(coordinate);
    system
        .to_record()
        .get(name)
        .cloned()
        .expect("first coordinate was stored")
}

fn direction(coordinates: &CoordinateSystem) -> Result<&DirectionCoordinate, ImageError> {
    if coordinates
        .find_coordinate(CoordinateType::Spectral)
        .is_none()
    {
        return Err(ImageError::CasaSelection(
            "impv requires a spectral axis".into(),
        ));
    }
    let Some(CoordinateModel::Direction(direction)) = coordinates
        .find_coordinate(CoordinateType::Direction)
        .map(|i| coordinates.coordinate(i))
        .filter(|coordinate| coordinate.n_pixel_axes() == 2)
    else {
        return Err(ImageError::CasaSelection(
            "impv requires both direction axes".into(),
        ));
    };
    let increment = direction.increment();
    let (x, y) = (increment[0].abs(), increment[1].abs());
    if (x - y).abs() > 2e-10 * x.max(y) {
        return Err(ImageError::CasaSelection(
            "impv requires square direction pixels".into(),
        ));
    }
    Ok(direction)
}

fn check_destination(destination: &Path, request: &ImpvRequest) -> Result<(), ImageError> {
    ensure_outfile_does_not_alias_input(destination, &request.imagename)?;
    if request
        .imagename
        .canonicalize()
        .ok()
        .zip(
            destination
                .parent()
                .and_then(|parent| parent.canonicalize().ok()),
        )
        .is_some_and(|(source, parent)| parent.starts_with(source))
    {
        return Err(ImageError::CasaSelection(
            "outfile must be outside the input image".into(),
        ));
    }
    match destination.symlink_metadata() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(output_io(error)),
        Ok(_) if !request.overwrite => Err(ImageError::CasaSelection(format!(
            "PV output already exists: {}",
            destination.display()
        ))),
        Ok(metadata)
            if metadata.is_dir()
                && Table::open_metadata_only(TableOptions::new(destination))
                    .is_ok_and(|table| table.info().table_type == "Image") =>
        {
            Ok(())
        }
        Ok(_) => Err(ImageError::CasaSelection(format!(
            "{} is not a CASA image directory",
            destination.display()
        ))),
    }
}

fn output_io(error: io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}
