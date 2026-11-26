// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::ImageError;

/// Parse a CASA image-channel selector and resolve it against one spectral axis.
pub fn parse_image_channel_selection(
    text: &str,
    axis_len: usize,
) -> Result<Vec<usize>, ImageError> {
    let mut indices = parse_image_channel_spans(text, axis_len)?
        .into_iter()
        .flat_map(ChannelSpan::indices)
        .collect::<Vec<_>>();
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

#[derive(Clone, Copy)]
pub(in crate::analysis) struct ChannelSpan {
    pub(in crate::analysis) first: usize,
    pub(in crate::analysis) last: usize,
    pub(in crate::analysis) step: usize,
}

impl ChannelSpan {
    pub(in crate::analysis) fn indices(self) -> impl Iterator<Item = usize> {
        (self.first..=self.last).step_by(self.step)
    }

    pub(in crate::analysis) fn contains(self, channel: usize) -> bool {
        (self.first..=self.last).contains(&channel)
            && (channel - self.first).is_multiple_of(self.step)
    }

    pub(in crate::analysis) fn clipped(self, first: usize, last: usize) -> Option<Self> {
        let lower = first.max(self.first);
        let upper = last.min(self.last);
        if lower > upper {
            return None;
        }
        let offset = lower - self.first;
        let first = self
            .first
            .checked_add(offset.div_ceil(self.step).checked_mul(self.step)?)?;
        (first <= upper).then(|| Self {
            first,
            last: first + (upper - first) / self.step * self.step,
            step: self.step,
        })
    }
}

pub(in crate::analysis) fn parse_image_channel_spans(
    text: &str,
    axis_len: usize,
) -> Result<Vec<ChannelSpan>, ImageError> {
    let mut spans = Vec::new();
    for segment in text
        .split([',', ';'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let (range, step) = match segment.split_once('^') {
            Some((range, stride)) => {
                let step = stride.trim().parse::<usize>().map_err(|error| {
                    ImageError::InvalidMetadata(format!(
                        "invalid channel stride {segment:?}: {error}"
                    ))
                })?;
                if step == 0 {
                    return Err(ImageError::InvalidMetadata(format!(
                        "channel stride must be positive: {segment:?}"
                    )));
                }
                (range.trim(), step)
            }
            None => (segment, 1),
        };
        let span = if let Some((lhs, rhs)) = range.split_once('~') {
            let parse = |bound: &str| {
                bound.trim().parse::<usize>().map_err(|error| {
                    ImageError::InvalidMetadata(format!(
                        "invalid channel range {segment:?}: {error}"
                    ))
                })
            };
            let first = parse(lhs)?;
            let end = parse(rhs)?;
            if end < first {
                return Err(ImageError::InvalidMetadata(format!(
                    "channel range end precedes start: {segment:?}"
                )));
            }
            Some(ChannelSpan {
                first,
                last: first + (end - first) / step * step,
                step,
            })
        } else {
            if step != 1 {
                return Err(ImageError::InvalidMetadata(format!(
                    "channel stride requires a range: {segment:?}"
                )));
            }
            if let Some((op, number)) = channel_comparison(range) {
                let limit = number.parse::<usize>().map_err(|_| {
                    ImageError::CasaSelection(format!(
                        "In channel specification, {number} is not an integer in {segment}."
                    ))
                })?;
                let (first, limit) = match op {
                    ">" => (limit.saturating_add(1), axis_len),
                    ">=" => (limit, axis_len),
                    "<" => (0, limit.min(axis_len)),
                    _ => (0, limit.saturating_add(1).min(axis_len)),
                };
                (first < limit).then(|| ChannelSpan {
                    first,
                    last: limit - 1,
                    step,
                })
            } else {
                let first = range.parse::<usize>().map_err(|error| {
                    ImageError::InvalidMetadata(format!("invalid channel {segment:?}: {error}"))
                })?;
                Some(ChannelSpan {
                    first,
                    last: first,
                    step,
                })
            }
        };
        spans.extend(span);
    }
    if spans.is_empty() || spans.iter().any(|span| span.last >= axis_len) {
        return Err(ImageError::ShapeMismatch {
            expected: vec![axis_len],
            got: spans
                .into_iter()
                .flat_map(ChannelSpan::indices)
                .take(1024)
                .collect(),
        });
    }
    Ok(spans)
}

fn channel_comparison(segment: &str) -> Option<(&'static str, &str)> {
    for op in [">=", "<=", ">", "<"] {
        if let Some(number) = segment.strip_prefix(op) {
            return Some((op, number.trim()));
        }
    }
    None
}
