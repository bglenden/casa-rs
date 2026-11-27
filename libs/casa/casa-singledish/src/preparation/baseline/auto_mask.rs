// SPDX-License-Identifier: LGPL-3.0-or-later
//! Automatic minimum-width line masks for single-dish baselines.
//!
//! The binning and the clipped-MAD search follow CASA 6.7.6.14's
//! SingleDish/LineFinder.cc and LineFindingUtils.cc.

use std::ops::RangeInclusive;

/// CASA's median absolute deviation line detection parameters.
#[derive(Clone, Copy)]
pub struct SingleDishLineFinder {
    threshold: f32,
    average_limit: i64,
    min_width: i64,
    edges: [i64; 2],
    symmetric_edges: bool,
}

type Channels = RangeInclusive<usize>;
const BIN_STATISTICS_FAILED: &str = "Failed AlwaysAssert status == LIBSAKURA_SYMBOL(Status_kOK)";

impl SingleDishLineFinder {
    /// Build a line finder from CASA's threshold, averaging width, minimum
    /// line width and edge-channel controls. One edge value applies at both
    /// ends. An empty list leaves both edges eligible. Controls are checked
    /// when the finder processes an eligible spectrum.
    pub fn new(threshold: f64, average_limit: i64, min_width: i64, edges: &[i64]) -> Self {
        let left = edges.first().copied().unwrap_or(0);
        let right = edges.get(1).copied().unwrap_or(left);
        Self {
            threshold: threshold as f32,
            average_limit,
            min_width,
            edges: [left, right],
            symmetric_edges: edges.len() == 1,
        }
    }

    pub(super) fn exclude_lines(
        self,
        spectrum: &[f32],
        included: &mut [bool],
    ) -> Result<(), String> {
        for range in self.find_lines(spectrum, included)? {
            included[range].fill(false);
        }
        Ok(())
    }

    pub(in crate::preparation) fn find_lines(
        self,
        spectrum: &[f32],
        included: &mut [bool],
    ) -> Result<Vec<RangeInclusive<usize>>, String> {
        let channels = spectrum.len();
        let min_width = usize::try_from(self.min_width)
            .ok()
            .filter(|&width| width != 0)
            .ok_or("Failed AlwaysAssert minwidth > 0")?;
        let average_limit = usize::try_from(self.average_limit)
            .map_err(|_| "Failed AlwaysAssert avg_limit >= 0")?;
        if min_width >= channels {
            return Err("Failed AlwaysAssert maxwidth > static_cast<size_t>(minwidth)".into());
        }
        let edge = |value| {
            usize::try_from(value).map_err(|_| {
                if self.symmetric_edges {
                    "Failed AlwaysAssert edge[0] >= 0"
                } else {
                    "Failed AlwaysAssert edge[0] >= 0 && edge[1] >= 0"
                }
            })
        };
        let [left, right] = [edge(self.edges[0])?, edge(self.edges[1])?];
        let edges = left
            .checked_add(right)
            .ok_or("Invalid line finder edge channels.")?;
        if edges >= channels.saturating_sub(2) {
            return Err(
                "Failed AlwaysAssert edge.first+edge.second<num_data-min_valid_elem".into(),
            );
        }
        included[..left].fill(false);
        included[channels - right..].fill(false);
        if included.iter().filter(|&&valid| valid).count() < 2 + edges {
            return Err(
                "Failed AlwaysAssert num_valid_elem >= min_valid_elem+edge.first+edge.second"
                    .into(),
            );
        }

        let mut factor = 1;
        let mut lines = Vec::new();
        loop {
            let offset = factor / 2;
            let (binned, valid) = bin_spectrum(spectrum, included, factor, offset)?;
            lines.extend(self.detect_level(&binned, &valid, channels, factor, offset, min_width)?);
            let Some(next) = factor.checked_mul(4) else {
                break;
            };
            if next > average_limit {
                break;
            }
            factor = next;
        }
        Ok(merge_overlapping(lines))
    }

    fn detect_level(
        self,
        binned: &[f32],
        valid: &[bool],
        channels: usize,
        factor: usize,
        offset: usize,
        min_width: usize,
    ) -> Result<Vec<Channels>, String> {
        let mut search = valid.to_vec();
        let mut best = Vec::new();
        let mut best_threshold = f32::MAX;
        let mut best_channels = 0;
        for _ in 0..10 {
            // An averaging level can have no fully unmasked bins even when
            // the original spectrum has enough channels for its fit.
            if search.iter().filter(|&&valid| valid).take(2).count() < 2 {
                break;
            }
            let median = masked_median(binned, &search, 1.);
            let deviation = binned
                .iter()
                .map(|sample| (sample - median).abs())
                .collect::<Vec<_>>();
            let mad = masked_median(&deviation, &search, 0.8);
            let threshold =
                (f64::from(self.threshold) * (factor as f64).sqrt() * f64::from(mad)) as f32;
            if threshold >= best_threshold {
                break;
            }
            if !threshold.is_finite() {
                return Err(BIN_STATISTICS_FAILED.into());
            }
            let narrowest = (min_width / factor).max(1);
            let mut detected = detect_ranges(&deviation, valid, threshold, narrowest);
            extend_wings(&mut detected, &deviation, valid, mad);
            let widest = (channels / factor).max(1);
            let detected = merge_ranges(detected, widest);
            let ranges = detected
                .into_iter()
                .map(|range| {
                    range.start() * factor + offset..=range.end() * factor + (factor - 1) + offset
                })
                .filter(|range| (min_width..=channels).contains(&range.clone().count()))
                .collect::<Vec<_>>();

            // CASA updates its current search with the de-binned indices, clipped
            // to the length of the binned array.
            for range in &ranges {
                if *range.start() < search.len() {
                    let end = (*range.end()).min(search.len() - 1);
                    search[*range.start()..=end].fill(false);
                }
            }
            let found = ranges
                .iter()
                .map(|range| range.end() - range.start() + 1)
                .sum();
            if found <= best_channels {
                break;
            }
            best_channels = found;
            best_threshold = threshold;
            best = ranges;
        }
        Ok(best)
    }
}

fn masked_median(values: &[f32], mask: &[bool], fraction: f32) -> f32 {
    let mut samples = values
        .iter()
        .zip(mask)
        .filter_map(|(&value, &valid)| valid.then_some(value))
        .collect::<Vec<_>>();
    let count = (samples.len() as f32 * fraction) as usize;
    if count == 0 {
        return f32::NAN;
    }
    samples.sort_unstable_by(f32::total_cmp);
    (samples[(count - 1) / 2] + samples[count / 2]) / 2.
}

fn bin_spectrum(
    spectrum: &[f32],
    included: &[bool],
    factor: usize,
    offset: usize,
) -> Result<(Vec<f32>, Vec<bool>), String> {
    if offset + factor >= spectrum.len() {
        return Err(BIN_STATISTICS_FAILED.into());
    }
    Ok(spectrum[offset..]
        .chunks_exact(factor)
        .zip(included[offset..].chunks_exact(factor))
        .map(|(values, mask)| {
            let valid = mask.iter().all(|&value| value);
            let value = if valid {
                values.iter().sum::<f32>() / factor as f32
            } else {
                0.
            };
            (value, valid)
        })
        .unzip())
}

fn detect_ranges(
    deviation: &[f32],
    valid: &[bool],
    threshold: f32,
    min_width: usize,
) -> Vec<Channels> {
    let mut result = Vec::new();
    let mut start = None;
    for (index, line) in deviation
        .iter()
        .zip(valid)
        .map(|(&value, &valid)| valid && value >= threshold)
        .chain([false])
        .enumerate()
    {
        if line {
            start.get_or_insert(index);
        } else if let Some(first) = start.take()
            && index - first >= min_width
        {
            result.push(first..=index - 1);
        }
    }
    result
}

fn extend_wings(ranges: &mut [Channels], deviation: &[f32], valid: &[bool], mad: f32) {
    let sign = |index: usize| deviation[index].partial_cmp(&mad);
    for range in ranges {
        let (mut left, mut right) = (range.start().to_owned(), range.end().to_owned());
        let left_sign = sign(left);
        let right_sign = sign(right);
        while left > 0 && valid[left - 1] && sign(left - 1) == left_sign {
            left -= 1;
        }
        while right + 1 < valid.len() && valid[right + 1] && sign(right + 1) == right_sign {
            right += 1;
        }
        *range = left..=right;
    }
}

fn merge_overlapping(mut ranges: Vec<Channels>) -> Vec<Channels> {
    ranges.sort_by_key(|range| *range.start());
    let mut overlapping: Vec<Channels> = Vec::new();
    for range in ranges {
        if let Some(previous) = overlapping.last_mut()
            && range.start() <= previous.end()
        {
            *previous = *previous.start()..=*(previous.end().max(range.end()));
        } else {
            overlapping.push(range);
        }
    }
    overlapping
}

fn merge_ranges(ranges: Vec<Channels>, max_width: usize) -> Vec<Channels> {
    let mut result: Vec<Channels> = Vec::new();
    for range in merge_overlapping(ranges)
        .into_iter()
        .filter(|range| range.end() - range.start() < max_width)
    {
        if let Some(previous) = result.last_mut() {
            let current_width = range.end() - range.start();
            let previous_width = previous.end() - previous.start();
            let gap = range.start() - previous.end();
            if gap < current_width.min(previous_width) / 4
                && range.end() - previous.start() < max_width
            {
                *previous = *previous.start()..=*range.end();
                continue;
            }
        }
        result.push(range);
    }
    result
}
