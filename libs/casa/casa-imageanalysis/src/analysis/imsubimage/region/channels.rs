// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel ranges for image regions.

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::quanta::{Quantity, Unit};

use crate::analysis::{ChannelSpan, parse_image_channel_spans};
use casa_images::{ImageError, ImageInterface, ImagePixel};

use super::{channel_position, crtf_spectral_bounds};

pub(super) fn resolve_channels<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    full_shape: &[usize],
    chans_arg: Option<&str>,
    crtf_chans: Option<&[usize]>,
    is_crtf: bool,
    start: &mut [usize],
    shape: &mut [usize],
) -> Result<(Option<usize>, Vec<ChannelSpan>), ImageError> {
    let arg_trimmed = chans_arg.map(str::trim).filter(|s| !s.is_empty());
    if arg_trimmed.is_none() && crtf_chans.is_none() {
        return Ok((None, Vec::new()));
    }
    let axis = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
        ImageError::InvalidMetadata("channel selection requires a spectral axis".into())
    })?;
    let requested = arg_trimmed
        .map(|text| channel_spans(text, image.coordinates(), axis, full_shape[axis]))
        .transpose()?;
    let mut spans = match crtf_chans {
        None => requested.expect("one channel selection"),
        Some(channels) => channels
            .iter()
            .copied()
            .filter(|&channel| {
                requested
                    .as_ref()
                    .is_none_or(|requested| requested.iter().any(|span| span.contains(channel)))
            })
            .map(|channel| ChannelSpan {
                first: channel,
                last: channel,
                step: 1,
            })
            .collect(),
    };
    if spans.is_empty() && arg_trimmed.is_some() && crtf_chans.is_some() {
        return Err(ImageError::CasaSelection(
            "channel selection and CRTF range have empty intersection".into(),
        ));
    }
    spans.sort_unstable_by_key(|span| span.first);
    if is_crtf && arg_trimmed.is_some() && !spans.is_empty() && has_gaps(&spans) {
        return Err(ImageError::CasaSelection(
            "Overriding spectral specification must be limited to a single channel range".into(),
        ));
    }
    // Intersect with any saved-region bounds already recorded on this axis.
    let first = start[axis];
    let last = first + shape[axis] - 1;
    spans = spans
        .into_iter()
        .filter_map(|span| span.clipped(first, last))
        .collect();
    spans.sort_unstable_by_key(|span| span.first);
    let Some(first) = spans.first() else {
        return Err(ImageError::CasaSelection(
            "selected channels lie outside saved region".into(),
        ));
    };
    start[axis] = first.first;
    shape[axis] = upper_bound(&spans) - first.first + 1;
    if !has_gaps(&spans) {
        spans.clear();
    }
    Ok((Some(axis), spans))
}

fn channel_spans(
    text: &str,
    coordinates: &CoordinateSystem,
    axis: usize,
    length: usize,
) -> Result<Vec<ChannelSpan>, ImageError> {
    let Some((_, range)) = text
        .split_once('=')
        .filter(|(key, _)| key.trim() == "range")
    else {
        return parse_image_channel_spans(text, length);
    };
    let range = range.trim();
    let invalid = || ImageError::CasaSelection(format!("invalid spectral range {text:?}"));
    let (lower, upper) = range
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .and_then(|inner| inner.split_once(','))
        .ok_or_else(invalid)?;
    // CASA requires both range endpoints to have conforming units. Its
    // legacy integer selector above is the form for unitless channel bounds.
    let endpoint = |text: &str| -> Result<bool, ImageError> {
        let text = text.trim();
        if let Some(value) = channel_position(text) {
            value
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
                .ok_or_else(invalid)?;
            return Ok(true);
        }
        let frequency = Quantity::read(text).map_err(|_| invalid())?;
        let hz = Unit::new("Hz").expect("built-in frequency unit");
        frequency
            .get_value_in(&hz)
            .ok()
            .filter(|value| value.is_finite())
            .ok_or_else(invalid)?;
        Ok(false)
    };
    if endpoint(lower)? != endpoint(upper)? {
        return Err(invalid());
    }
    let [first, last] = crtf_spectral_bounds(range, coordinates, axis, length, None)?;
    Ok(vec![ChannelSpan {
        first,
        last,
        step: 1,
    }])
}

fn upper_bound(spans: &[ChannelSpan]) -> usize {
    spans
        .iter()
        .map(|span| span.last)
        .max()
        .expect("one channel span")
}

fn has_gaps(spans: &[ChannelSpan]) -> bool {
    if spans.len() == 1 {
        return spans[0].step > 1 && spans[0].first != spans[0].last;
    }
    if spans.iter().all(|span| span.step == 1) {
        let mut end = spans[0].last;
        for span in &spans[1..] {
            if span.first > end.saturating_add(1) {
                return true;
            }
            end = end.max(span.last);
        }
        return false;
    }
    (spans[0].first..=upper_bound(spans))
        .any(|channel| !spans.iter().any(|span| span.contains(channel)))
}
