// SPDX-License-Identifier: LGPL-3.0-or-later
//! Coordinate and statistic columns for imstat's per-plane log.

use std::fmt::Write;

use crate::ImstatPlanesSummary;
use casa_coordinates::{CoordinateModel, CoordinateType};
use casa_core::cancellation::Cancellation;
use casa_images::{ImageError, ImageInterface, ImagePixel};

pub(super) fn plane_table<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    stats: &ImstatPlanesSummary,
    axes: &[usize],
    positions: &[Vec<usize>],
    iterations: &[Option<usize>],
    cancellation: Option<&Cancellation>,
) -> Result<String, ImageError> {
    let coordinates = image.coordinates();
    let names = image.axis_names();
    let types = image.axis_types();
    let mut columns: Vec<usize> = (0..axes.len()).collect();
    columns.sort_by_key(|&slot| match types[axes[slot]] {
        CoordinateType::Spectral => 1,
        CoordinateType::Stokes => 2,
        _ => 0,
    });
    let spectral = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .and_then(|index| {
            let CoordinateModel::Spectral(spectral) = coordinates.coordinate(index) else {
                return None;
            };
            let axis = image.find_axis(CoordinateType::Spectral)?;
            axes.contains(&axis).then_some((
                axis,
                spectral,
                spectral.rest_frequency_hz().ok().filter(|&rest| rest > 0.0),
            ))
        });
    let has_iterations = iterations.iter().any(Option::is_some);
    let world_columns = axes
        .iter()
        .map(|&axis| {
            let (index, local) = coordinates.pixel_axis_owner(axis).ok_or_else(|| {
                ImageError::InvalidMetadata(format!(
                    "imstat: display axis {axis} has no coordinate"
                ))
            })?;
            let offset = (0..index)
                .map(|i| coordinates.coordinate(i).n_world_axes())
                .sum::<usize>();
            Ok(offset + local)
        })
        .collect::<Result<Vec<_>, ImageError>>()?;
    let statistic_names = if stats.biweight {
        &["Npts", "Mean", "Std_dev", "Minimum", "Maximum"][..]
    } else {
        &[
            "Npts", "Sum", "Mean", "Rms", "Std_dev", "Minimum", "Maximum",
        ][..]
    };
    let mut out = String::new();
    if let Some((_, _, Some(_))) = spectral {
        let _ = writeln!(out, "#VELOCITY column unit = km/s");
    }
    for &slot in &columns {
        let axis = axes[slot];
        let name = if spectral
            .is_some_and(|(spectral_axis, _, rest)| spectral_axis == axis && rest.is_some())
        {
            "VELOCITY"
        } else {
            &names[axis]
        };
        let _ = write!(out, "{name:>16} {:>17} ", format!("{name}(Plane)"));
    }
    for name in statistic_names {
        let _ = write!(out, "{name:>14}");
    }
    if stats.flux.is_some() {
        let _ = write!(out, "{:>14}", "Flux");
    }
    if has_iterations {
        let _ = write!(out, "{:>8}", "N Iter");
    }
    out.push('\n');
    let mut pixel: Vec<_> = stats.blc.iter().map(|&index| index as f64).collect();
    let original_strides = positions
        .iter()
        .map(Vec::len)
        .scan(1, |stride, length| {
            let previous = *stride;
            *stride *= length;
            Some(previous)
        })
        .collect::<Vec<_>>();
    for row in 0..stats.npts.len() {
        super::check_imstat_cancelled(cancellation)?;
        let mut remainder = row;
        let mut plane = 0;
        for &slot in columns.iter().rev() {
            let index = remainder % positions[slot].len();
            remainder /= positions[slot].len();
            pixel[axes[slot]] = positions[slot][index] as f64;
            plane += index * original_strides[slot];
        }
        if stats.npts[plane] == 0.0 {
            continue;
        }
        let world = casa_coordinates::format_world_display(coordinates, &pixel)?;
        let fields: Vec<_> = world.split(", ").collect();
        for &slot in &columns {
            let axis = axes[slot];
            let field = world_columns[slot];
            let spectral_value;
            let value: &str = if let Some((spectral_axis, coordinate, rest)) = spectral
                && spectral_axis == axis
            {
                let frequency = coordinate.world_frequency_hz(pixel[axis])?;
                spectral_value = rest.map_or_else(
                    || format!("{frequency}Hz"),
                    |rest| {
                        format!(
                            "{:.5}",
                            super::SPEED_OF_LIGHT_KM_S * (1.0 - frequency / rest)
                        )
                    },
                );
                &spectral_value
            } else {
                fields.get(field).copied().ok_or_else(|| {
                    ImageError::InvalidMetadata(format!(
                        "imstat: no world value for display axis {axis}"
                    ))
                })?
            };
            let _ = write!(out, "{value:>16} {:>17} ", pixel[axis] as usize);
        }
        let mut values = vec![stats.npts[plane]];
        if !stats.biweight {
            values.push(stats.sum[plane]);
        }
        values.push(stats.mean[plane]);
        if !stats.biweight {
            values.push(stats.rms[plane]);
        }
        values.extend([stats.sigma[plane], stats.min[plane], stats.max[plane]]);
        if let Some(flux) = &stats.flux {
            values.push(flux[plane]);
        }
        for value in values {
            let _ = write!(out, "{value:>14.6e}");
        }
        if has_iterations {
            let _ = write!(out, "{:>8}", iterations[plane].unwrap_or_default());
        }
        out.push('\n');
    }
    Ok(out)
}
