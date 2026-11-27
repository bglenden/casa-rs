// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn imhead_typed<T>(
    path: &Path,
    image: &PagedImage<T>,
    pixel_type: String,
) -> Result<ImageHeaderSummary, ImageError>
where
    T: ImagePixel,
{
    let info = image.image_info()?;
    let default_mask = image.default_mask_name();
    let has_mask = default_mask.is_some();
    Ok(ImageHeaderSummary {
        imagename: path.display().to_string(),
        pixel_type,
        shape: image.shape().to_vec(),
        tile_shape: image.tile_shape().to_vec(),
        units: image.units().to_string(),
        object_name: info.object_name.clone(),
        observer: image.coordinates().obs_info().observer.clone(),
        telescope: image.coordinates().obs_info().telescope.clone(),
        observation_date: casa_core::quanta::format::format_time(
            image
                .coordinates()
                .obs_info()
                .date
                .as_ref()
                .map_or(0.0, |epoch| epoch.value().as_mjd()),
            casa_core::quanta::format::YMD,
            12,
        ),
        direction_reference: (0..image.coordinates().n_coordinates()).find_map(|index| match image
            .coordinates()
            .coordinate(index)
        {
            casa_coordinates::CoordinateModel::Direction(direction) => {
                Some(direction.direction_ref().as_str().to_string())
            }
            _ => None,
        }),
        direction_projection: (0..image.coordinates().n_coordinates()).find_map(
            |index| match image.coordinates().coordinate(index) {
                casa_coordinates::CoordinateModel::Direction(direction) => {
                    Some(projection_header::projection_header(direction))
                }
                _ => None,
            },
        ),
        spectral: spectral_header(image)?.map(Box::new),
        image_type: header_image_type(image)?.to_string(),
        default_mask,
        has_mask,
        masks: image.mask_names(),
        regions: image.region_names(),
        restoring_beam: info
            .beam_set
            .single_beam()
            .and_then(|beam| format_beam(beam, image.restoring_beam_record()))
            .map(Box::new),
        has_multiple_beams: info.beam_set.is_multi(),
        per_plane_beams: per_plane_beam_summary(image, &info.beam_set)?.map(Box::new),
        extrema: None,
        axes: axis_summaries(image.coordinates(), image.shape(), false),
        world_axes: axis_summaries(image.coordinates(), image.shape(), true),
    })
}

pub(super) fn per_plane_beam_summary<T: ImagePixel>(
    image: &PagedImage<T>,
    beam_set: &ImageBeamSet,
) -> Result<Option<PerPlaneBeamSummary>, ImageError> {
    summarize_beam_set(
        image.coordinates(),
        image.shape(),
        beam_set,
        image.per_plane_beam_record(),
    )
}

pub(super) fn summarize_beam_set(
    coordinates: &CoordinateSystem,
    shape: &[usize],
    beam_set: &ImageBeamSet,
    beam_record: Option<&RecordValue>,
) -> Result<Option<PerPlaneBeamSummary>, ImageError> {
    if beam_set.single_beam().is_some_and(|beam| beam.is_null()) {
        return Err(ImageError::InvalidMetadata(
            "Invalid per plane beam found".into(),
        ));
    }
    if !beam_set.is_multi() {
        return Ok(None);
    }
    let channels = beam_set.n_channels();
    let stokes = beam_set.n_stokes();
    let mut image_channels = 1;
    let mut image_stokes = 1;
    for index in 0..coordinates.n_coordinates() {
        let coordinate = coordinates.coordinate(index);
        let extent = || {
            coordinates
                .coordinate_pixel_axes(index)
                .and_then(|axes| axes.into_iter().flatten().next())
                .and_then(|axis| shape.get(axis).copied())
                .unwrap_or(1)
        };
        match coordinate.coordinate_type() {
            CoordinateType::Spectral => image_channels = extent(),
            CoordinateType::Stokes => image_stokes = extent(),
            _ => {}
        }
    }
    if channels != 1 && channels != image_channels {
        return Err(ImageError::InvalidMetadata(
            "Number of channels is not consistent".into(),
        ));
    }
    if stokes != 1 && stokes != image_stokes {
        return Err(ImageError::InvalidMetadata(
            "Number of polarizations is not consistent".into(),
        ));
    }
    let mut beams = Vec::with_capacity(channels * stokes);
    let mut areas = Vec::with_capacity(channels * stokes);
    let area_unit = casa_core::quanta::Unit::new("arcsec2")
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
    for polarization in 0..stokes {
        for channel in 0..channels {
            let index = polarization * channels + channel;
            let beam = *beam_set.beam(channel, polarization);
            let raw = beam_record
                .and_then(|record| record.get(&format!("*{index}")))
                .and_then(|value| match value {
                    Value::Record(record) => Some(record),
                    _ => None,
                });
            let formatted = format_beam(beam, raw).ok_or_else(|| {
                ImageError::InvalidMetadata("Invalid per plane beam found".into())
            })?;
            // CASA multiplies the stored quantities before converting their
            // product to arcsec². That order affects ties in the median.
            let major_unit = casa_core::quanta::Unit::new(&formatted.major.unit)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
            let minor_unit = casa_core::quanta::Unit::new(&formatted.minor.unit)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
            let ratio =
                area_unit.val().factor / (major_unit.val().factor * minor_unit.val().factor);
            let area = std::f64::consts::PI / (4.0 * 2.0_f64.ln())
                * (formatted.major.value * formatted.minor.value / ratio);
            beams.push(formatted);
            areas.push((index, area));
        }
    }
    areas.sort_by(|left, right| left.1.total_cmp(&right.1));
    let median_index = areas[areas.len() / 2].0;
    Ok(Some(PerPlaneBeamSummary {
        channels,
        stokes,
        beams,
        median_index,
    }))
}

pub(super) fn spectral_header<T: ImagePixel>(
    image: &PagedImage<T>,
) -> Result<Option<SpectralHeaderSummary>, ImageError> {
    let Some(spectral) = image.first_spectral_coordinate()? else {
        return Ok(None);
    };
    Ok(Some(spectral_summary(&spectral)))
}

pub(super) fn spectral_summary(
    spectral: &casa_coordinates::SpectralCoordinate,
) -> SpectralHeaderSummary {
    let unit = spectral
        .axis_units()
        .into_iter()
        .next()
        .unwrap_or_else(|| "Hz".into());
    // The stored record's rest frequencies are already in its axis unit.
    SpectralHeaderSummary {
        reference_frame: if spectral.has_undefined_frame() {
            "Undefined".into()
        } else {
            spectral.frequency_ref().as_str().into()
        },
        unit,
        rest_frequency: spectral.rest_frequency(),
        rest_frequencies: spectral.rest_frequencies().to_vec(),
    }
}

pub(super) fn format_beam(
    beam: GaussianBeam,
    record: Option<&RecordValue>,
) -> Option<RestoringBeamSummary> {
    if beam.is_null() {
        return None;
    }
    let major_arcsec = beam.major_in("arcsec").ok()?;
    let minor_arcsec = beam.minor_in("arcsec").ok()?;
    let position_angle_deg = beam.position_angle_in("deg").ok()?;
    let fallback = |value, unit: &str| BeamQuantity {
        value,
        unit: unit.into(),
    };
    let major = record
        .and_then(|r| beam_quantity(r, "major"))
        .unwrap_or_else(|| fallback(major_arcsec, "arcsec"));
    let minor = record
        .and_then(|r| beam_quantity(r, "minor"))
        .unwrap_or_else(|| fallback(minor_arcsec, "arcsec"));
    let position_angle = record
        .and_then(|r| beam_quantity(r, "positionangle"))
        .unwrap_or_else(|| fallback(position_angle_deg, "deg"));
    let quantity =
        casa_core::quanta::Quantity::new(position_angle.value, &position_angle.unit).ok()?;
    let wrapped = casa_images::wrap_position_angle(&quantity).ok()?;
    let wrapped_position_angle = BeamQuantity {
        value: wrapped.value(),
        unit: wrapped.unit().name().into(),
    };
    Some(RestoringBeamSummary {
        major_arcsec,
        minor_arcsec,
        position_angle_deg,
        major,
        minor,
        position_angle,
        wrapped_position_angle,
    })
}

pub(super) fn beam_quantity(record: &RecordValue, key: &str) -> Option<BeamQuantity> {
    let Value::Record(quantity) = record.get(key)? else {
        return None;
    };
    let quantity = casa_images::quantity_from_record(quantity).ok()?;
    Some(BeamQuantity {
        value: quantity.value(),
        unit: casa_core::quanta::unit::normalize_casa_unit_name(quantity.unit().name()),
    })
}

pub(super) fn axis_summaries(
    coordinates: &CoordinateSystem,
    shape: &[usize],
    world: bool,
) -> Vec<ImageAxisSummary> {
    let mut summaries = Vec::new();
    for coord_idx in 0..coordinates.n_coordinates() {
        let coord = coordinates.coordinate(coord_idx);
        let mut refs = coord.reference_value();
        let refpix = coord.reference_pixel();
        let mut increments = coord.increment();
        let names = coord.axis_names();
        let mut units = coord.axis_units();
        // A direction coordinate presents its values in the units it is
        // stored in, as casacore's does, though it computes in radians.
        if let casa_coordinates::CoordinateModel::Direction(direction) = coord {
            (refs, increments) = direction.reference_and_increment_in_world_units();
            units = direction.visible_world_units();
        }
        let pixel_axes = coordinates
            .coordinate_pixel_axes(coord_idx)
            .unwrap_or_default();
        let world_axes;
        let axes = if world {
            world_axes = coordinates
                .coordinate_world_axes(coord_idx)
                .unwrap_or_default();
            &world_axes
        } else {
            &pixel_axes
        };
        for (axis_in_coord, &axis) in axes.iter().enumerate() {
            let Some(axis) = axis else {
                continue;
            };
            summaries.push(ImageAxisSummary {
                axis,
                coordinate_type: format!("{:?}", coord.coordinate_type()),
                name: names
                    .get(axis_in_coord)
                    .cloned()
                    .unwrap_or_else(|| format!("Axis{axis}")),
                shape: pixel_axes
                    .get(axis_in_coord)
                    .copied()
                    .flatten()
                    .and_then(|pixel| shape.get(pixel))
                    .copied()
                    .unwrap_or(0),
                reference_value: *refs.get(axis_in_coord).unwrap_or(&0.0),
                reference_pixel: *refpix.get(axis_in_coord).unwrap_or(&0.0),
                increment: *increments.get(axis_in_coord).unwrap_or(&0.0),
                unit: units.get(axis_in_coord).cloned().unwrap_or_default(),
                stokes: match coord {
                    casa_coordinates::CoordinateModel::Stokes(stokes) => {
                        stokes.stokes().iter().map(ToString::to_string).collect()
                    }
                    _ => Vec::new(),
                },
            });
        }
    }
    summaries.sort_unstable_by_key(|axis| axis.axis);
    if world {
        return summaries;
    }
    for (axis, &extent) in shape.iter().enumerate().skip(summaries.len()) {
        summaries.push(ImageAxisSummary {
            axis,
            coordinate_type: "Linear".to_string(),
            name: format!("Axis{axis}"),
            shape: extent,
            reference_value: 0.0,
            reference_pixel: 0.0,
            increment: 1.0,
            unit: String::new(),
            stokes: Vec::new(),
        });
    }
    summaries
}
