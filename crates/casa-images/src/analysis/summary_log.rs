// SPDX-License-Identifier: LGPL-3.0-or-later
//! Human-readable image and coordinate summaries for image metadata clients.

use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate,
};

use super::{
    ImageHeaderSummary, PerPlaneBeamSummary, RestoringBeamSummary,
    projection_header::cpp_number_precision,
};
use crate::{AnyPagedImage, ImageError};

/// Describe the image and its coordinate system for a logging sink.
///
/// Verbose output lists every per-plane restoring beam. Otherwise, large
/// beam grids are summarized by the minimum, maximum, and median area for
/// each polarization. This is a human-readable representation, while the
/// corresponding CASA task return record is built separately.
///
/// # Errors
///
/// Returns an error if the image cannot be opened.
pub fn imhead_summary_log(
    path: impl AsRef<Path>,
    summary: &ImageHeaderSummary,
    verbose: bool,
) -> Result<Vec<String>, ImageError> {
    let path = path.as_ref();
    if path.is_file() {
        let image = crate::fits_read::PrimaryHdu::open(path)?;
        let coordinates = &image.coordinates;
        let spectral = coordinates
            .find_coordinate(CoordinateType::Spectral)
            .and_then(|index| match coordinates.coordinate(index) {
                CoordinateModel::Spectral(spectral) => Some(spectral.as_ref()),
                _ => None,
            });
        return Ok(format_summary(
            summary,
            coordinates,
            spectral,
            verbose,
            "FITSImage",
        ));
    }
    let image = AnyPagedImage::open(path)?;
    let (coordinates, spectral) = match &image {
        AnyPagedImage::Float32(image) => (image.coordinates(), image.first_spectral_coordinate()?),
        AnyPagedImage::Float64(image) => (image.coordinates(), image.first_spectral_coordinate()?),
        AnyPagedImage::Complex32(image) => {
            (image.coordinates(), image.first_spectral_coordinate()?)
        }
        AnyPagedImage::Complex64(image) => {
            (image.coordinates(), image.first_spectral_coordinate()?)
        }
    };
    Ok(format_summary(
        summary,
        coordinates,
        spectral.as_ref(),
        verbose,
        "PagedImage",
    ))
}

fn format_summary(
    header: &ImageHeaderSummary,
    coords: &CoordinateSystem,
    first_spectral: Option<&SpectralCoordinate>,
    verbose: bool,
    image_type: &str,
) -> Vec<String> {
    let mut lines = vec![
        format!("Image name: {}", header.imagename),
        format!("Object: {}", header.object_name),
        format!(
            "Image type: {image_type}; quantity: {}; pixel type: {}",
            header.image_type, header.pixel_type
        ),
        format!(
            "Masks: {:?}; default mask: {}",
            header.masks,
            header.default_mask.as_deref().unwrap_or("None")
        ),
        format!("Regions: {:?}", header.regions),
        format!("Image units: {}", header.units),
    ];
    if let Some(beam) = &header.restoring_beam {
        lines.push(format!("Restoring beam: {}", beam_text(beam)));
    }
    if let Some(beams) = &header.per_plane_beams {
        append_beams(&mut lines, header, first_spectral, beams, verbose);
    }
    if let Some(index) = coords.find_coordinate(CoordinateType::Direction)
        && let CoordinateModel::Direction(direction) = coords.coordinate(index)
    {
        lines.push(format!(
            "Direction reference: {:?}; conversion frame: {:?}; projection: {}",
            direction.direction_ref(),
            direction.conversion_direction_ref(),
            header.direction_projection.as_deref().unwrap_or("unknown")
        ));
    }
    if let Some(spectral) = &header.spectral {
        let conversion = first_spectral
            .and_then(SpectralCoordinate::conversion_frequency_ref)
            .map(|frame| format!("; conversion frame: {frame:?}"))
            .unwrap_or_default();
        lines.push(format!(
            "Spectral reference: {}{conversion}; velocity type: RADIO; active rest frequency: {} {}; rest frequencies: {:?} {}",
            spectral.reference_frame, spectral.rest_frequency, spectral.unit,
            spectral.rest_frequencies, spectral.unit,
        ));
    }
    let obs = coords.obs_info();
    if !obs.pointing_center_initial {
        lines.push(format!(
            "Pointing center: {:?} rad",
            obs.pointing_center_rad
        ));
    }
    lines.push(format!(
        "Telescope: {}; observer: {}; observation date: {}",
        header.telescope, header.observer, header.observation_date
    ));
    if let Some(position) = &obs.telescope_position {
        let units = if position.refer().as_str() == "ITRF" {
            "m, m, m"
        } else {
            "rad, rad, m"
        };
        lines.push(format!(
            "Telescope position: {:?} ({}, {units})",
            position.values(),
            position.refer()
        ));
    }
    for axis in &header.axes {
        let tile = header.tile_shape.get(axis.axis).copied().unwrap_or(0);
        let spectral = spectral_at_axis(coords, axis.axis);
        let mapping = spectral
            .filter(|coordinate| !coordinate.has_linear_mapping())
            .map_or("", |_| " (tabular)");
        lines.push(format!(
            "Axis {}: {} {}{mapping}; shape {}, tile {}, reference {} at pixel {}, increment {} {}",
            axis.axis,
            axis.coordinate_type,
            axis.name,
            axis.shape,
            tile,
            axis.reference_value,
            axis.reference_pixel,
            axis.increment,
            axis.unit
        ));
        if !axis.stokes.is_empty() {
            lines.push(format!("  Polarizations: {}", axis.stokes.join(", ")));
        }
        if let Some(spectral) = spectral
            && spectral.rest_frequency() > 0.0
        {
            let velocity = 299_792.458 * (1.0 - axis.reference_value / spectral.rest_frequency());
            let increment = spectral
                .to_world(&[axis.reference_pixel + 0.5])
                .ok()
                .zip(spectral.to_world(&[axis.reference_pixel - 0.5]).ok())
                .map(|(high, low)| -299_792.458 * (high[0] - low[0]) / spectral.rest_frequency());
            let increment = increment.map_or_else(
                || "unavailable".to_string(),
                |value| format!("{value} km/s"),
            );
            lines.push(format!(
                "  Radio velocity: {velocity} km/s at pixel {}; increment {increment}",
                axis.reference_pixel
            ));
        }
    }
    lines
}

fn spectral_at_axis(coords: &CoordinateSystem, axis: usize) -> Option<&SpectralCoordinate> {
    let mut offset = 0;
    for index in 0..coords.n_coordinates() {
        let coordinate = coords.coordinate(index);
        if offset == axis
            && let CoordinateModel::Spectral(spectral) = coordinate
        {
            return Some(spectral);
        }
        offset += coordinate.n_pixel_axes();
    }
    None
}

fn beam_text(beam: &RestoringBeamSummary) -> String {
    format!(
        "{} arcsec x {} arcsec, position angle {} {}",
        cpp_number_precision(beam.major_arcsec, 8),
        cpp_number_precision(beam.minor_arcsec, 8),
        cpp_number_precision(beam.wrapped_position_angle.value, 8),
        beam.wrapped_position_angle.unit
    )
}

fn append_beams(
    lines: &mut Vec<String>,
    header: &ImageHeaderSummary,
    spectral: Option<&SpectralCoordinate>,
    grid: &PerPlaneBeamSummary,
    verbose: bool,
) {
    lines.push(format!(
        "Restoring beams: {} channels, {} polarizations",
        grid.channels, grid.stokes
    ));
    let labels = header
        .axes
        .iter()
        .find(|axis| axis.coordinate_type == "Stokes")
        .map(|axis| &axis.stokes);
    for pol in 0..grid.stokes {
        let label = labels
            .and_then(|labels| labels.get(pol))
            .cloned()
            .unwrap_or_else(|| pol.to_string());
        let channels: Vec<usize> = if verbose || grid.channels <= 3 || spectral.is_none() {
            (0..grid.channels).collect()
        } else {
            let mut ordered: Vec<usize> = (0..grid.channels).collect();
            ordered.sort_by(|&a, &b| {
                let area = |c: usize| {
                    let beam = &grid.beams[pol * grid.channels + c];
                    beam.major_arcsec * beam.minor_arcsec
                };
                area(a).total_cmp(&area(b))
            });
            vec![
                ordered[grid.channels - 1],
                ordered[0],
                ordered[grid.channels / 2],
            ]
        };
        for (rank, channel) in channels.into_iter().enumerate() {
            let kind = if !verbose && grid.channels > 3 && spectral.is_some() {
                ["maximum", "minimum", "median"][rank]
            } else {
                ""
            };
            let frequency = spectral
                .and_then(|coord| {
                    coord
                        .to_world(&[channel as f64])
                        .ok()
                        .and_then(|v| v.first().copied())
                        .map(|value| {
                            let unit = coord.axis_units().first().cloned().unwrap_or_default();
                            let velocity = if coord.rest_frequency() > 0.0 {
                                format!(
                                    ", radio velocity {} km/s",
                                    299_792.458 * (1.0 - value / coord.rest_frequency())
                                )
                            } else {
                                String::new()
                            };
                            format!(", frequency {value} {unit}{velocity}")
                        })
                })
                .unwrap_or_default();
            lines.push(format!(
                "  {kind} channel {channel}, polarization {label}{frequency}: {}",
                beam_text(&grid.beams[pol * grid.channels + channel])
            ));
        }
    }
}
