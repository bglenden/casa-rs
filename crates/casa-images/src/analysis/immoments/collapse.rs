// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's moment calculations over valid samples of an image profile.

use casa_core::ArrayD;
use ndarray::{Axis, IxDyn, ShapeBuilder};

use super::axis::ProfileCoordinates;
use super::window::{Method, Selection};
use crate::analysis::imsubimage::fitprofile::selection::Lane;
use crate::{ImageError, ImmomentsRequest};

#[derive(Clone, Copy)]
pub(in crate::analysis) enum PixelClip {
    All,
    Include([f64; 2]),
    Exclude([f64; 2]),
}

impl PixelClip {
    pub(super) fn from_tool(include: &[f64], exclude: &[f64]) -> Result<Self, ImageError> {
        fn range(values: &[f64]) -> Result<Option<[f64; 2]>, ImageError> {
            let range = match values {
                [] | [-1.] => return Ok(None),
                [value] => [-value.abs(), value.abs()],
                [a, b] => [a.min(*b), a.max(*b)],
                _ => {
                    return Err(ImageError::CasaSelection(
                        "A pixel range accepts at most two values".into(),
                    ));
                }
            };
            Ok(Some(range.map(|value| f64::from(value as f32))))
        }
        match (range(include)?, range(exclude)?) {
            (None, None) => Ok(Self::All),
            (Some(range), None) => Ok(Self::Include(range)),
            (None, Some(range)) => Ok(Self::Exclude(range)),
            _ => Err(ImageError::CasaSelection(
                "Only one of includepix or excludepix may be specified, not both".into(),
            )),
        }
    }

    pub(super) fn from_request(request: &ImmomentsRequest) -> Result<Self, ImageError> {
        match (request.includepix, request.excludepix) {
            (None, None) => Ok(Self::All),
            (Some(range), None) => Ok(Self::Include(range)),
            (None, Some(range)) => Ok(Self::Exclude(range)),
            _ => Err(ImageError::CasaSelection(
                "Only one of includepix or excludepix may be specified, not both".into(),
            )),
        }
    }

    pub(super) fn permits_median_coordinate(self) -> bool {
        match self {
            Self::All => false,
            Self::Include([lower, upper]) | Self::Exclude([lower, upper]) => lower * upper >= 0.0,
        }
    }

    #[cfg(test)]
    pub(in crate::analysis) fn from_include(range: Option<[f64; 2]>) -> Self {
        range.map_or(Self::All, Self::Include)
    }

    fn keeps(self, value: f64) -> bool {
        match self {
            Self::All => true,
            Self::Include([lower, upper]) => value >= lower && value <= upper,
            Self::Exclude([lower, upper]) => value <= lower || value >= upper,
        }
    }
}

pub(super) struct CollapsedMoment {
    pub(super) values: ArrayD<f32>,
    pub(super) valid: ArrayD<bool>,
}

pub(super) fn collapse_moments<T>(
    input: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axis: usize,
    coordinates: &ProfileCoordinates,
    scale: f64,
    clip: PixelClip,
    moments: &[i32],
) -> Vec<CollapsedMoment>
where
    T: Into<f64> + Copy,
{
    let mut shape = input.shape().to_vec();
    shape.remove(axis);
    let mut cursor = vec![0; shape.len()];
    let mut result: Vec<_> = moments
        .iter()
        .map(|_| CollapsedMoment {
            values: ArrayD::zeros(IxDyn(&shape).f()),
            valid: ArrayD::from_elem(IxDyn(&shape).f(), false),
        })
        .collect();
    let mut samples = Vec::with_capacity(input.shape()[axis]);
    let mut mask_lanes = mask.map(|mask| mask.lanes(Axis(axis)).into_iter());
    for (mut offset, lane) in input.lanes(Axis(axis)).into_iter().enumerate() {
        for (pixel, &length) in cursor.iter_mut().zip(&shape).rev() {
            *pixel = offset % length;
            offset /= length;
        }
        let mask = mask_lanes.as_mut().and_then(Iterator::next);
        let positions = coordinates.at(&cursor);
        samples.clear();
        for (i, &value) in lane.iter().enumerate() {
            let value = value.into();
            if mask.as_ref().is_none_or(|mask| mask[i]) && clip.keeps(value) {
                samples.push(Sample {
                    value,
                    position: positions[i],
                });
            }
        }
        if samples.is_empty() {
            continue;
        }
        let sum = samples.iter().map(|sample| sample.value).sum::<f64>();
        let index = IxDyn(&cursor);
        for (&moment, output) in moments.iter().zip(&mut result) {
            if let Some(value) = calculate(&samples, sum, scale, moment) {
                output.values[index.clone()] = value as f32;
                output.valid[index.clone()] = true;
            }
        }
    }
    for output in &mut result {
        output.values = std::mem::take(&mut output.values).insert_axis(Axis(axis));
        output.valid = std::mem::take(&mut output.valid).insert_axis(Axis(axis));
    }
    result
}

struct Sample {
    value: f64,
    position: f64,
}

pub(super) struct MomentSamples {
    pub(super) method: Method,
    pub(super) clip: PixelClip,
    pub(super) peak_snr: f32,
    pub(super) noise: f32,
    pub(super) scale: f64,
    samples: Vec<Sample>,
    sum: f64,
}

impl MomentSamples {
    pub(super) fn new(
        method: Method,
        clip: PixelClip,
        peak_snr: f32,
        noise: f32,
        scale: f64,
    ) -> Self {
        Self {
            method,
            clip,
            peak_snr,
            noise,
            scale,
            samples: Vec::new(),
            sum: 0.,
        }
    }

    pub(super) fn extract(
        &mut self,
        original: &Lane,
        smoothed: Option<&Lane>,
        mut coordinate: impl FnMut(usize) -> Result<f64, ImageError>,
    ) -> Result<(), ImageError> {
        self.samples.clear();
        let guide = smoothed.unwrap_or(original);
        let selected = Selection::new(
            self.method,
            &guide.values,
            &guide.valid,
            self.peak_snr,
            self.noise,
        );
        for (index, (&value, &valid)) in original.values.iter().zip(&original.valid).enumerate() {
            if !valid {
                continue;
            }
            let value = match &selected {
                Selection::All if self.clip.keeps(f64::from(guide.values[index])) => value,
                Selection::Window(window) if window.contains(&index) => value,
                Selection::Model([amplitude, center, width, level]) => {
                    let z = (index as f64 - center) / width;
                    (level + amplitude * (-4. * std::f64::consts::LN_2 * z * z).exp()) as f32
                }
                _ => continue,
            };
            self.samples.push(Sample {
                value: value.into(),
                position: coordinate(index)?,
            });
        }
        self.sum = self.samples.iter().map(|sample| sample.value).sum();
        Ok(())
    }

    pub(super) fn measure(&self, moment: i32) -> Option<f64> {
        if self.samples.is_empty() {
            None
        } else {
            calculate(&self.samples, self.sum, self.scale, moment)
        }
    }
}

fn calculate(samples: &[Sample], sum: f64, scale: f64, moment: i32) -> Option<f64> {
    let n = samples.len() as f64;
    let sum_squares = || {
        samples
            .iter()
            .map(|sample| sample.value * sample.value)
            .sum::<f64>()
    };
    let weighted_mean = || {
        samples
            .iter()
            .map(|sample| sample.position * sample.value)
            .sum::<f64>()
            / sum
    };
    match moment {
        -1 => Some(sum / n),
        0 => Some(sum * scale),
        1 if sum != 0.0 => Some(weighted_mean()),
        2 if sum != 0.0 => {
            let mean = weighted_mean() as f32;
            let squares = samples
                .iter()
                .map(|sample| sample.value * sample.position * sample.position)
                .sum::<f64>();
            let variance = f64::from(((squares / sum - f64::from(mean * mean)) as f32).abs());
            (variance > 0.0).then(|| variance.sqrt())
        }
        3 => {
            let mut values: Vec<_> = samples.iter().map(|sample| sample.value).collect();
            // casacore averages the middle pair for short, even profiles.
            // Above 100 samples it uses the lower middle value.
            let mid = (values.len() - 1) / 2;
            let average_middle = values.len() <= 100 && values.len().is_multiple_of(2);
            let (_, middle, upper) = values.select_nth_unstable_by(mid, f64::total_cmp);
            let median = if average_middle {
                (*middle
                    + upper
                        .iter()
                        .copied()
                        .min_by(f64::total_cmp)
                        .expect("an even profile has an upper middle sample"))
                    / 2.0
            } else {
                *middle
            };
            Some(median)
        }
        4 => Some(median_coordinate(samples)),
        5 if n > 1.0 => {
            let variance = (sum_squares() - sum * sum / n) / (n - 1.0);
            (variance as f32 > 0.0).then(|| variance.sqrt())
        }
        6 => Some((sum_squares() / n).sqrt()),
        7 => {
            let mean = f64::from((sum / n) as f32);
            Some(
                samples
                    .iter()
                    .map(|sample| (sample.value - mean).abs())
                    .sum::<f64>()
                    / n,
            )
        }
        8 | 9 => {
            // CASA keeps the first location when values tie.
            let maximum = samples.iter().reduce(|best, sample| {
                if sample.value > best.value {
                    sample
                } else {
                    best
                }
            })?;
            Some(if moment == 8 {
                maximum.value
            } else {
                maximum.position
            })
        }
        10 | 11 => {
            let minimum = samples.iter().reduce(|best, sample| {
                if sample.value < best.value {
                    sample
                } else {
                    best
                }
            })?;
            Some(if moment == 10 {
                minimum.value
            } else {
                minimum.position
            })
        }
        _ => None,
    }
}

fn median_coordinate(samples: &[Sample]) -> f64 {
    // MomentClip accumulates Float values, adding the absolute value of the
    // previous cumulative value. The first bin at half the maximum is used.
    let mut cumulative = (samples[0].value as f32).abs();
    let mut maximum = cumulative;
    for sample in &samples[1..] {
        cumulative = sample.value as f32 + cumulative.abs();
        maximum = maximum.max(cumulative);
    }
    let half = maximum / 2.0;
    cumulative = (samples[0].value as f32).abs();
    if cumulative >= half {
        return samples[0].position;
    }
    for sample in &samples[1..] {
        cumulative = sample.value as f32 + cumulative.abs();
        if cumulative >= half {
            return sample.position;
        }
    }
    samples[0].position
}

#[cfg(test)]
pub(in crate::analysis) fn collapse_moment<T: Into<f64> + Copy>(
    input: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axis: usize,
    coordinates: &ProfileCoordinates,
    scale: f64,
    clip: PixelClip,
    moment: i32,
) -> (ArrayD<f32>, ArrayD<bool>) {
    let result = collapse_moments(input, mask, axis, coordinates, scale, clip, &[moment]).remove(0);
    (result.values, result.valid)
}
