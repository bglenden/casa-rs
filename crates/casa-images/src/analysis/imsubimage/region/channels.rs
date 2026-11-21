// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel ranges for image regions.

use casa_coordinates::CoordinateType;

use crate::analysis::parse_image_channel_selection;
use crate::{ImageError, ImageInterface, ImagePixel};

pub(super) fn resolve_channels<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    full_shape: &[usize],
    chans_arg: Option<&str>,
    crtf_chans: Option<&[usize]>,
    is_crtf: bool,
    start: &mut [usize],
    shape: &mut [usize],
) -> Result<(Option<usize>, Option<Vec<usize>>), ImageError> {
    let arg_trimmed = chans_arg.map(str::trim).filter(|s| !s.is_empty());
    if arg_trimmed.is_none() && crtf_chans.is_none() {
        return Ok((None, None));
    }
    let axis = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
        ImageError::InvalidMetadata("channel selection requires a spectral axis".into())
    })?;
    let mut indices = match (arg_trimmed, crtf_chans) {
        (Some(text), None) => parse_image_channel_selection(text, full_shape[axis])?,
        (None, Some(c)) => c.to_vec(),
        (Some(text), Some(c)) => {
            let from_arg = parse_image_channel_selection(text, full_shape[axis])?;
            let intersected: Vec<usize> =
                from_arg.into_iter().filter(|idx| c.contains(idx)).collect();
            if intersected.is_empty() {
                return Err(ImageError::CasaSelection(
                    "channel selection and CRTF range have empty intersection".into(),
                ));
            }
            intersected
        }
        (None, None) => unreachable!(),
    };
    indices.sort_unstable();
    indices.dedup();
    if is_crtf && arg_trimmed.is_some() && indices.windows(2).any(|pair| pair[1] - pair[0] != 1) {
        return Err(ImageError::CasaSelection(
            "Overriding spectral specification must be limited to a single channel range".into(),
        ));
    }
    // Intersect with any saved-region bounds already recorded on this axis.
    let prev_lo = start[axis];
    let prev_hi = start[axis] + shape[axis] - 1;
    let bounded: Vec<usize> = indices
        .into_iter()
        .filter(|&c| c >= prev_lo && c <= prev_hi)
        .collect();
    if bounded.is_empty() {
        return Err(ImageError::CasaSelection(
            "selected channels lie outside saved region".into(),
        ));
    }
    start[axis] = bounded[0];
    shape[axis] = bounded[bounded.len() - 1] - bounded[0] + 1;
    Ok((Some(axis), Some(bounded)))
}
