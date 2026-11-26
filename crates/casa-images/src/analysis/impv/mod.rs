// SPDX-License-Identifier: LGPL-3.0-or-later
//! Position-velocity images extracted from a shared image selection.

mod geometry;
mod position;
mod tool;

pub use tool::{ImagePvProduct, generate_image_pv};

use super::PbcorRegion;

use std::io;
use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
    LinearCoordinate,
};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayD, ArrayValue, RecordValue, Value};
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{Table, TableOptions};
use ndarray::{IxDyn, ShapeBuilder};

use super::axes::{
    direction_plane_axes, replace_stokes_in_record, section_record_reference_pixels,
    stokes_for_axis, subimage_coordinate_record, subimage_coordinates_with_stokes,
};
use super::imsubimage::{
    SubimageRegion,
    coords::{drop_axes_from_legacy_record, subset_beam_set},
    region::SubimageSelection,
};
use super::{combined_selection_mask, ensure_outfile_does_not_alias_input};
use crate::plane::PlaneIndex;
use crate::regrid::{Plane, RegridMethod};
use crate::{ImageError, ImageInterface, ImagePixel, ImpvRequest, PagedImage, PvImageSummary};

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
    let endpoints = position::endpoints(request, direction, true)?;
    // Keep the task's original independently rounded transverse limits.
    let strip = geometry::Strip::new(
        direction,
        endpoints,
        plane.map(|i| image.shape()[i]),
        width,
        false,
    )?;
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
    let native = match image.table().keywords().get("coords") {
        Some(Value::Record(record)) => Some(record.clone()),
        _ => None,
    };
    let (coordinates, record, secondary) =
        output_coordinates(image, native, &selection, plane, &strip, &request.unit)?;
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
    output.append_history_at("::impv", &[
        concat!("version: casa-rs ", env!("CARGO_PKG_VERSION")).into(),
        format!(
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
        Some(PbcorRegion::Record(record)) if !record.fields().is_empty() => Some(record.clone()),
        Some(PbcorRegion::Text(name))
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
            Some(PbcorRegion::Text(text)) if !text.trim().is_empty() => {
                Some(SubimageRegion::Text(text))
            }
            _ => None,
        }
    };
    resolve_selection(image, request, plane, region, false)
}

fn resolve_selection<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    request: &ImpvRequest,
    plane: [usize; 2],
    region: Option<SubimageRegion<'_>>,
    allow_strides: bool,
) -> Result<SubimageSelection, ImageError> {
    let chans = request.chans.as_deref().map(str::trim).filter(|s| {
        !s.eq_ignore_ascii_case("all") && (allow_strides || !s.eq_ignore_ascii_case("none"))
    });
    let stokes = request
        .stokes
        .as_deref()
        .map(str::trim)
        .map(|s| s.trim_start_matches('[').trim_end_matches(']').trim())
        .filter(|s| {
            !s.eq_ignore_ascii_case("all") && (allow_strides || !s.eq_ignore_ascii_case("none"))
        })
        .map(|s| s.replace(';', ","));
    let resolved = if allow_strides {
        SubimageSelection::resolve_section(image, None, region, chans, stokes.as_deref())?
    } else {
        SubimageSelection::resolve(image, None, region, chans, stokes.as_deref())?
    };
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
    let planes = PlaneIndex::new(input.shape(), plane);
    let offsets = strip.input_offsets()?;
    let mut out_shape = input.shape().to_vec();
    out_shape[x] = strip.shape[0];
    out_shape.remove(y);
    let offset_axis = x - usize::from(y < x);
    let mut result = ArrayD::zeros(IxDyn(&out_shape).f());
    let mut valid = ArrayD::from_elem(IxDyn(&out_shape).f(), false);
    let mut pixels = vec![0.0; plane_shape[0] * plane_shape[1]];
    let mut good = mask.map(|_| vec![false; pixels.len()]);
    let method = if strip.rotated() {
        RegridMethod::Cubic
    } else {
        RegridMethod::Nearest
    };
    for i in 0..planes.count() {
        planes.read(i, input, &mut pixels, |value| (*value).into());
        if let (Some(mask), Some(good)) = (mask, &mut good) {
            planes.read(i, mask, good, |value| *value);
        }
        let plane = Plane {
            data: &pixels,
            mask: good.as_deref(),
            shape: plane_shape,
        };
        let mut output_index: Vec<_> = (0..input.ndim())
            .filter(|&axis| axis != y)
            .map(|axis| planes.pixel(i, axis))
            .collect();
        for (i, section) in offsets.iter().enumerate() {
            if let Some(value) = collapse_width(&plane, method, section) {
                output_index[offset_axis] = i;
                result[IxDyn(&output_index)] = value;
                valid[IxDyn(&output_index)] = true;
            }
        }
    }
    Ok((result, valid))
}

fn collapse_width(plane: &Plane<'_>, method: RegridMethod, points: &[[f64; 2]]) -> Option<f32> {
    let (sum, count) = points
        .iter()
        .filter_map(|&at| plane.sample(method, at))
        .fold((0., 0), |(sum, count), value| {
            (sum + f64::from(value as f32), count + 1)
        });
    (count != 0).then(|| (sum / f64::from(count)) as f32)
}

fn output_coordinates<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    native: Option<RecordValue>,
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
        && let Some(axis) = image.find_axis(CoordinateType::Stokes)
        && selection.packed_shape()[axis] != image.shape()[axis]
    {
        let stokes = stokes_for_axis(image.coordinates(), axis)?;
        let start = selection.start[axis];
        let step = selection.stride.as_ref().map_or(1, |stride| stride[axis]);
        selected_stokes = Some(
            stokes[start..start + selection.shape[axis]]
                .iter()
                .copied()
                .step_by(step)
                .collect(),
        );
    }
    let selected_stokes = selected_stokes.as_deref();
    let native = native.filter(|record| {
        record
            .fields()
            .iter()
            .any(|field| field.name.starts_with("pixelmap"))
    });
    let mut record = if let Some(stride) = selection.stride.as_deref() {
        let mut record = native.unwrap_or_else(|| image.coordinates().to_record());
        section_record_reference_pixels(
            &mut record,
            &selection.start,
            &selection.packed_shape(),
            Some(stride),
        )?;
        if let Some(stokes) = selected_stokes {
            replace_stokes_in_record(&mut record, stokes)?;
        }
        record
    } else {
        match native.and_then(|record| {
            subimage_coordinate_record(record, &selection.start, &selection.shape, selected_stokes)
        }) {
            Some(record) => record,
            None => subimage_coordinates_with_stokes(
                image.coordinates(),
                &selection.start,
                &selection.shape,
                selected_stokes,
            )?
            .to_record(),
        }
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
    let pixel_offset = kept
        .iter()
        .position(|&axis| axis == plane[0])
        .expect("offset axis kept");
    let offset = CoordinateSystem::record_coordinates(&record)?
        .into_iter()
        .find(|entry| {
            entry.field.starts_with("linear") && entry.pixel_axes == [Some(pixel_offset), None]
        })
        .ok_or_else(|| {
            ImageError::InvalidMetadata("PV offset axis missing from the image record".into())
        })?;
    let world_offset = offset.world_axes[0]
        .ok_or_else(|| ImageError::InvalidMetadata("PV offset has no world axis".into()))?;
    let suffix = offset.field.trim_start_matches("linear");
    // The removed local axis is encoded as -2. The surviving pixel and
    // world axes can occupy independently transposed positions.
    for (kind, active) in [("pixelmap", pixel_offset), ("worldmap", world_offset)] {
        record.upsert(
            format!("{kind}{suffix}"),
            Value::Array(ArrayValue::from_i32_vec(vec![active as i32, -2])),
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

#[cfg(test)]
mod tests {
    use casa_coordinates::{DirectionCoordinate, Projection, ProjectionType};
    use casa_core::measures::direction::DirectionRef;
    use ndarray::IxDyn;

    use super::{ArrayD, geometry, sample};

    #[test]
    fn pv_offset_stays_on_the_longitude_axis_after_latitude_is_removed() {
        let direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, 0.0],
            [-1e-5, 1e-5],
            [0.0, 0.0],
        );
        let strip =
            geometry::Strip::new(&direction, [[2.0, 3.0], [7.0, 3.0]], [10, 8], 1, false).unwrap();
        let pixels = ArrayD::from_shape_fn(IxDyn(&[8, 10, 2]), |index| {
            (50 * index[0] + index[1] + 500 * index[2]) as f32
        });
        let (result, mask) = sample(&pixels, None, [1, 0], &strip).unwrap();
        assert_eq!(result.shape(), [6, 2]);
        let expected = ArrayD::from_shape_fn(IxDyn(&[6, 2]), |index| {
            (152 + index[0] + 500 * index[1]) as f32
        });
        assert_eq!(result, expected);
        assert!(mask.iter().all(|valid| *valid));
    }
}
