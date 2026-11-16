// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA image-header value selection, shared by image task providers.

use crate::{ImageError, ImageHeaderSummary};
use casa_provider_contracts::CasaValue;
use std::path::Path;

/// A failure selecting a CASA image-header value.
#[derive(Debug, thiserror::Error)]
pub enum ImheadGetError {
    /// The requested keyword has not been implemented.
    #[error("imhead get of hdkey {0:?} is not implemented by casa-rs yet (#977)")]
    UnsupportedKey(String),
    /// CASA rejects the requested metadata operation.
    #[error("{0}")]
    Execution(String),
    /// The image or its metadata could not be read.
    #[error("{0}")]
    Image(#[source] ImageError),
}

/// Return a CASA `imhead(mode="get")` value from an image header.
///
/// `path` must name the image from which `summary` was computed. For extrema
/// keys the summary must include the extrema computed by
/// [`crate::imhead_extrema`]. Unknown and unsupported fields return an error.
///
/// # Errors
///
/// Returns an error for unavailable metadata, invalid axes, unsupported keys,
/// or a failure reading a user-defined field from the image.
pub fn imhead_get_value(
    summary: &ImageHeaderSummary,
    path: &Path,
    key: &str,
) -> Result<CasaValue, ImheadGetError> {
    let lower = key.to_ascii_lowercase();
    let value = match lower.as_str() {
        "bunit" => CasaValue::string(&summary.units),
        "imtype" => CasaValue::string(&summary.image_type),
        "object" => CasaValue::string(&summary.object_name),
        "date-obs" | "epoch" => CasaValue::string(&summary.observation_date),
        "observer" => CasaValue::string(&summary.observer),
        "telescope" => CasaValue::string(&summary.telescope),
        "equinox" => CasaValue::string(summary.direction_reference.as_deref().unwrap_or("")),
        "projection" => CasaValue::string(summary.direction_projection.as_deref().unwrap_or("")),
        "reffreqtype" => {
            CasaValue::string(summary.spectral.as_ref().map_or("", |s| &s.reference_frame))
        }
        "restfreq" => {
            let spectral = summary.spectral.as_ref().ok_or_else(|| {
                execution("Image has no spectral axis so there is no rest frequency")
            })?;
            CasaValue::dict([
                ("unit", CasaValue::string(&spectral.unit)),
                ("value", CasaValue::float(spectral.rest_frequency)),
            ])
        }
        "masks" => CasaValue::str_vector(summary.masks.clone()),
        "shape" => CasaValue::int64_vector(summary.shape.iter().map(|&n| n as i64)),
        "datamin" | "datamax" | "minpixpos" | "maxpixpos" | "minpos" | "maxpos" => {
            let extrema = summary
                .extrema
                .as_ref()
                .ok_or_else(|| execution(format!("RecordInterface: field {lower} is unknown")))?;
            match lower.as_str() {
                "datamin" => CasaValue::float(extrema.min),
                "datamax" => CasaValue::float(extrema.max),
                "minpixpos" => {
                    CasaValue::int64_vector(extrema.min_position.iter().map(|&n| n as i64))
                }
                "maxpixpos" => {
                    CasaValue::int64_vector(extrema.max_position.iter().map(|&n| n as i64))
                }
                "minpos" => CasaValue::string(&extrema.min_world),
                _ => CasaValue::string(&extrema.max_world),
            }
        }
        "beammajor" | "beamminor" | "beampa" | "bmaj" | "bmin" | "bpa" => {
            let beam = summary.restoring_beam.as_ref().ok_or_else(|| {
                execution(if summary.has_multiple_beams {
                    "This image has multiple beams."
                } else {
                    "This image has no beam(s)."
                })
            })?;
            let value = match lower.as_str() {
                "beammajor" | "bmaj" => &beam.major,
                "beamminor" | "bmin" => &beam.minor,
                _ => &beam.wrapped_position_angle,
            };
            CasaValue::dict([
                ("unit", CasaValue::string(&value.unit)),
                ("value", CasaValue::float(value.value)),
            ])
        }
        _ if crate::analysis::imhead_is_custom_key(key) => {
            return crate::analysis::imhead_custom_value(path, key).map_err(ImheadGetError::Image);
        }
        _ => return axis_header_value(summary, key, &lower),
    };
    Ok(value)
}

fn axis_header_value(
    summary: &ImageHeaderSummary,
    key: &str,
    lower: &str,
) -> Result<CasaValue, ImheadGetError> {
    if let Some((prefix, suffix)) = lower
        .get(..5)
        .zip(lower.get(5..))
        .filter(|(prefix, _)| ["cdelt", "crpix", "crval", "ctype", "cunit"].contains(prefix))
    {
        let index = crate::analysis::imhead_axis_index(suffix);
        let axis = index
            .and_then(|index| summary.axes.get(index))
            .ok_or_else(|| execution(format!("imhead: invalid axis keyword {key:?}")))?;
        let quantity = |value| {
            CasaValue::dict([
                ("unit", CasaValue::string(&axis.unit)),
                ("value", CasaValue::float(value)),
            ])
        };
        return match prefix {
            "cdelt" => Ok(quantity(axis.increment)),
            "crpix" => Ok(CasaValue::float(axis.reference_pixel)),
            "crval" if axis.coordinate_type == "Stokes" => {
                Ok(CasaValue::str_vector(axis.stokes.clone()))
            }
            "crval" => Ok(quantity(axis.reference_value)),
            "ctype" => Ok(CasaValue::string(&axis.name)),
            "cunit" => Ok(CasaValue::string(&axis.unit)),
            _ => Err(unimplemented_header_key(key)),
        };
    }
    Err(unimplemented_header_key(key))
}

fn unimplemented_header_key(key: &str) -> ImheadGetError {
    ImheadGetError::UnsupportedKey(key.to_string())
}

fn execution(message: impl Into<String>) -> ImheadGetError {
    ImheadGetError::Execution(message.into())
}
