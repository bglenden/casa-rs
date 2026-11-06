// SPDX-License-Identifier: LGPL-3.0-or-later
//! Metadata from a read-only FITS image.

use std::path::Path;

use casa_coordinates::{CoordinateModel, CoordinateType};
use fitsio::images::ImageType as FitsImageType;

use super::{
    ImageExtrema, ImageHeaderSummary, ImageType, axis_summaries, format_beam, projection_header,
    spectral_summary, summarize_beam_set, world_header,
};
use crate::ImageError;
use crate::fits_read::{PrimaryHdu, image_info_from_fits_header};

/// Read an unconsumed FITS header field as image miscellaneous metadata.
pub(super) fn custom_value(
    path: &Path,
    key: &str,
) -> Result<casa_provider_contracts::CasaValue, ImageError> {
    use casa_coordinates::{FitsHeader, FitsValue};
    use casa_provider_contracts::CasaValue;
    let upper = key.to_ascii_uppercase();
    let internal = [
        "SIMPLE", "BITPIX", "NAXIS", "EXTEND", "BLOCKED", "BLANK", "BSCALE", "BZERO", "XTENSION",
        "PCOUNT", "GCOUNT", "ORIGIN", "RADESYS", "EQUINOX", "SPECSYS", "RESTFRQ", "TELESCOP",
        "OBSERVER", "DATE-OBS", "MJD-OBS", "TIMESYS", "OBJECT", "BUNIT", "IMTYPE", "BMAJ", "BMIN",
        "BPA", "LONPOLE", "LATPOLE", "CROTA2",
    ];
    let axis_keyword = upper.strip_prefix("NAXIS").is_some_and(|suffix| {
        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
    });
    let matrix_keyword = ["PC", "CD"].iter().any(|prefix| {
        upper
            .strip_prefix(prefix)
            .and_then(|suffix| suffix.split_once('_'))
            .is_some_and(|(row, col)| {
                !row.is_empty()
                    && !col.is_empty()
                    && row.bytes().all(|byte| byte.is_ascii_digit())
                    && col.bytes().all(|byte| byte.is_ascii_digit())
            })
    });
    if internal.contains(&upper.as_str()) || axis_keyword || matrix_keyword {
        return Err(ImageError::InvalidMetadata(format!(
            "Unknown keyword {key}"
        )));
    }
    let cards = crate::fits_read::read_cards(path)?;
    let references = cards.iter().map(String::as_str).collect::<Vec<_>>();
    let header = FitsHeader::from_cards(&references);
    match header.get(&upper) {
        Some(FitsValue::String(value)) => Ok(CasaValue::string(value)),
        Some(FitsValue::Integer(value)) if i32::try_from(*value).is_ok() => {
            Ok(CasaValue::Int { value: *value })
        }
        Some(FitsValue::Float(value)) => Ok(CasaValue::float(*value)),
        Some(FitsValue::Logical(_)) => Err(ImageError::InvalidMetadata(
            "Unhandled data type Bool for user defined type. Send us a bug report".into(),
        )),
        _ => Err(ImageError::InvalidMetadata(format!(
            "Unknown keyword {key}"
        ))),
    }
}

/// FITSImage uses TiledFileAccess's 32768-pixel tile shape.
fn tile_shape(shape: &[usize]) -> Vec<usize> {
    let mut remaining = 32768_f32;
    let mut result = vec![1; shape.len()];
    for (axis, &length) in shape.iter().enumerate() {
        if length as f32 <= remaining {
            result[axis] = length;
            remaining /= length as f32;
        } else {
            let ideal = (remaining + 0.5) as usize;
            if length % ideal == 0 {
                result[axis] = ideal;
            } else {
                for offset in 1..ideal.min(length - ideal + 1) {
                    if length % (ideal - offset) == 0 {
                        result[axis] = ideal - offset;
                        break;
                    }
                    if length % (ideal + offset) == 0 {
                        result[axis] = ideal + offset;
                        break;
                    }
                }
            }
            break;
        }
    }
    result
}

pub(super) fn summary(path: &Path) -> Result<ImageHeaderSummary, ImageError> {
    let mut primary = PrimaryHdu::open(path)?;
    let beam_data = primary.beam_set()?;
    let mut info = image_info_from_fits_header(&primary.header);
    if let Some((beams, _)) = &beam_data {
        info.beam_set = beams.clone();
    }
    let coords = &primary.coordinates;
    let beam_records = beam_data.as_ref().map(|(_, records)| records);
    let per_plane_beams = summarize_beam_set(coords, &primary.shape, &info.beam_set, beam_records)?;
    let direction =
        coords
            .find_coordinate(CoordinateType::Direction)
            .and_then(|index| match coords.coordinate(index) {
                CoordinateModel::Direction(direction) => Some(direction),
                _ => None,
            });
    let spectral = coords
        .find_coordinate(CoordinateType::Spectral)
        .and_then(|index| match coords.coordinate(index) {
            CoordinateModel::Spectral(spectral) => Some(spectral_summary(spectral)),
            _ => None,
        });
    Ok(ImageHeaderSummary {
        imagename: path.display().to_string(),
        pixel_type: "Float32".into(),
        shape: primary.shape.clone(),
        tile_shape: tile_shape(&primary.shape),
        units: primary.header.get_string("BUNIT").unwrap_or("").to_string(),
        object_name: if primary.header.get_string("OBJECT") == Some("") {
            " ".into()
        } else {
            info.object_name
        },
        observer: coords.obs_info().observer.clone(),
        telescope: coords.obs_info().telescope.clone(),
        observation_date: casa_types::quanta::format::format_time(
            coords
                .obs_info()
                .date
                .as_ref()
                .map_or(0.0, |epoch| epoch.value().as_mjd()),
            casa_types::quanta::format::YMD,
            12,
        ),
        direction_reference: direction
            .map(|direction| direction.direction_ref().as_str().to_string()),
        direction_projection: direction.map(projection_header::projection_header),
        spectral: spectral.map(Box::new),
        image_type: if info.image_type == ImageType::Undefined {
            ImageType::Intensity
        } else {
            info.image_type
        }
        .to_string(),
        default_mask: None,
        has_mask: matches!(
            primary.image_type,
            FitsImageType::Float | FitsImageType::Double
        ) || primary.header.get_int("BLANK").is_some(),
        masks: Vec::new(),
        regions: Vec::new(),
        restoring_beam: info
            .beam_set
            .single_beam()
            .and_then(|beam| format_beam(beam, None))
            .map(Box::new),
        has_multiple_beams: info.beam_set.is_multi(),
        per_plane_beams: per_plane_beams.map(Box::new),
        extrema: None,
        axes: axis_summaries(coords, &primary.shape),
    })
}

/// Scan the pixels in bounded chunks, ignoring FITS blanks and NaNs.
pub(super) fn extrema(path: &Path) -> Result<Option<ImageExtrema>, ImageError> {
    let mut image = PrimaryHdu::open(path)?;
    let total = image
        .shape
        .iter()
        .try_fold(1usize, |value, length| value.checked_mul(*length))
        .ok_or_else(|| ImageError::InvalidMetadata("FITS image shape overflows".into()))?;
    let mut min: Option<(f32, usize)> = None;
    let mut max: Option<(f32, usize)> = None;
    for start in (0..total).step_by(1_048_576) {
        let (pixels, mask) = image.read_pixels(start, (total - start).min(1_048_576))?;
        for (offset, (&value, &good)) in pixels.iter().zip(&mask).enumerate() {
            if !good {
                continue;
            }
            let index = start + offset;
            if min.is_none_or(|(current, _)| value < current) {
                min = Some((value, index));
            }
            if max.is_none_or(|(current, _)| value > current) {
                max = Some((value, index));
            }
        }
    }
    let (Some((min, min_index)), Some((max, max_index))) = (min, max) else {
        return Ok(None);
    };
    let position = |mut index: usize| {
        image
            .shape
            .iter()
            .map(|length| {
                let coordinate = index % length;
                index /= length;
                coordinate
            })
            .collect::<Vec<_>>()
    };
    let min_position = position(min_index);
    let max_position = position(max_index);
    let mut axis = 0;
    let mut spectral_axis = None;
    for index in 0..image.coordinates.n_coordinates() {
        let coordinate = image.coordinates.coordinate(index);
        if coordinate.coordinate_type() == CoordinateType::Spectral && spectral_axis.is_none() {
            spectral_axis = Some(axis);
        }
        axis += coordinate.n_pixel_axes();
    }
    Ok(Some(ImageExtrema {
        min: f64::from(min),
        max: f64::from(max),
        min_world: world_header::format_position(&image.coordinates, &min_position, spectral_axis)?,
        max_world: world_header::format_position(&image.coordinates, &max_position, spectral_axis)?,
        min_position,
        max_position,
    }))
}
