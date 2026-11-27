// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time segments and baseline flag propagation for the phase and Q/U solve.

use std::collections::{BTreeMap, HashMap};

use crate::execute::ParallacticAngleState;

use super::channel_samples::{ChannelSamples, ScratchSample};
use super::cross_hand::LegacyCrossHandAveraging;
use super::parallactic_fit::{Segment, SegmentChannel, solve, solve_sample};
use super::{CoherencyMean, Complex32, GainSolveError, GainSolveMode, prior_error};

#[derive(Clone, Copy)]
pub(super) struct BaselineSegment {
    pub(super) field: i32,
    pub(super) data_desc: i32,
    pub(super) spectral_window: i32,
    pub(super) start: f64,
    pub(super) end: f64,
    pub(super) emission: f64,
}

struct BaselineFlags {
    antennas: [i32; 2],
    unflagged: Vec<u64>,
}

struct SegmentRows {
    id: usize,
    context: BaselineSegment,
    flags: Vec<BaselineFlags>,
    usable: bool,
}

pub(super) struct ParallacticAverages {
    circular: bool,
    gain_phase: bool,
    legacy: bool,
    mode: GainSolveMode,
    segments: BTreeMap<(u64, i32, i32), SegmentRows>,
    rows: ChannelSamples<PhaseSample>,
}

impl ParallacticAverages {
    pub(super) fn new(
        channels: usize,
        circular: bool,
        gain_phase: bool,
        mode: GainSolveMode,
        legacy: bool,
    ) -> Self {
        Self {
            circular,
            gain_phase,
            legacy,
            mode,
            segments: BTreeMap::new(),
            rows: ChannelSamples::new(channels),
        }
    }

    pub(super) fn add(
        &mut self,
        antennas: [i32; 2],
        context: BaselineSegment,
        channels: &mut [CoherencyMean],
    ) -> Result<(), GainSolveError> {
        let id = self.segments.len();
        let key_time = if self.legacy && self.gain_phase {
            context.start
        } else {
            context.emission
        };
        let segment = self
            .segments
            .entry((key_time.to_bits(), context.field, context.data_desc))
            .or_insert_with(|| SegmentRows {
                id,
                context,
                flags: Vec::new(),
                usable: false,
            });
        let mut unflagged = vec![0; channels.len().div_ceil(64)];
        let legacy_phase_normalization = (self.legacy && self.mode == GainSolveMode::Phase)
            .then(|| super::cross_hand::legacy_phase_normalization(channels));
        for (channel, mean) in channels.iter_mut().enumerate() {
            let mean = std::mem::take(mean);
            if mean.unflagged != 0 {
                unflagged[channel / 64] |= 1 << (channel % 64);
                segment.usable = true;
            }
            if (!self.gain_phase || antennas[0] != antennas[1])
                && let Some((data, model, weights)) = mean.finish(false)
                && (weights[1] > 0.0 || weights[2] > 0.0)
            {
                let [(xy, wx), (yx, wy)] = [1, 2].map(|corr| {
                    let (value, weight) = solve_sample(self.mode, data[corr], weights[corr]);
                    let weight = legacy_phase_normalization
                        .map_or(weight, |normalization| normalization[corr] * weights[corr]);
                    (value, weight)
                });
                let sample = PhaseSample {
                    segment: segment.id as u32,
                    antennas,
                    data: [xy, yx],
                    model: [model[1], model[2]],
                    weights: if self.legacy && self.gain_phase {
                        // VI1 weights both cross-hands by the sum of their weights.
                        [0.5 * (wx + wy); 2]
                    } else {
                        [wx, wy]
                    },
                };
                self.rows
                    .push(channel, sample)
                    .map_err(GainSolveError::PolarizationScratch)?;
            }
        }
        segment.flags.push(BaselineFlags {
            antennas,
            unflagged,
        });
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        angles: &ParallacticAngleState,
        legacy_times: Option<&LegacyCrossHandAveraging>,
    ) -> Result<(Vec<Option<Complex32>>, Complex32), GainSolveError> {
        let channels = self.rows.len();
        let mut positions = vec![None; self.segments.len()];
        let mut usable = self.segments.into_values().filter(|segment| segment.usable);
        let first = usable
            .next()
            .ok_or(GainSolveError::NoUnflaggedParallacticData)?;
        // CASA starts with the baselines in the first usable segment,
        // then extends their channel flags through all usable segments.
        let mut accepted = first
            .flags
            .iter()
            .map(|row| (row.antennas, row.unflagged.clone()))
            .collect::<HashMap<_, _>>();
        let mut samples = Vec::new();
        for segment in std::iter::once(first).chain(usable) {
            positions[segment.id] = Some(samples.len());
            if !self.gain_phase {
                for row in segment.flags {
                    if let Some(accepted) = accepted.get_mut(&row.antennas) {
                        for (keep, unflagged) in accepted.iter_mut().zip(row.unflagged) {
                            *keep &= unflagged;
                        }
                    }
                }
            }
            let context = segment.context;
            let time = legacy_times
                .and_then(|clock| clock.time(context))
                .unwrap_or(context.start + 0.5 * (context.end - context.start));
            let angle = if self.circular {
                0.0
            } else {
                angles
                    .feed_parallactic_angle(time, context.field, 0, 0, context.spectral_window)
                    .map_err(prior_error)? as f32
            };
            samples.push(Segment {
                angle: f64::from(angle),
                channels: vec![SegmentChannel::default(); channels],
            });
        }
        let required = if self.circular || self.gain_phase {
            2
        } else {
            3
        };
        if self.legacy && self.gain_phase && samples.len() < required {
            return Ok((vec![None; channels], Complex32::ZERO));
        }
        if samples.len() < required {
            if self.gain_phase {
                return Err(GainSolveError::InsufficientGainPhaseSegments);
            }
            return Err(GainSolveError::InsufficientParallacticCoverage { required });
        }
        (0..channels).try_for_each(|channel| {
            let word = channel / 64;
            let bit = 1 << (channel % 64);
            self.rows
                .drain(channel, |row| {
                    if let Some(segment) = positions[row.segment as usize]
                        && (self.gain_phase
                            || accepted
                                .get(&row.antennas)
                                .is_some_and(|words| words[word] & bit != 0))
                    {
                        samples[segment].channels[channel].observe(
                            row.data,
                            row.model,
                            row.weights,
                        );
                    }
                })
                .map_err(GainSolveError::PolarizationScratch)
        })?;
        if self.legacy
            && self.gain_phase
            && !samples
                .iter()
                .flat_map(|segment| &segment.channels)
                .any(SegmentChannel::has_weight)
        {
            return Ok((vec![None; channels], Complex32::ZERO));
        }
        solve(&samples, channels, self.circular, !self.gain_phase)
            .map_err(|_| GainSolveError::SingularSourcePolarization)
    }
}

#[derive(Debug, PartialEq)]
struct PhaseSample {
    segment: u32,
    antennas: [i32; 2],
    data: [Complex32; 2],
    model: [Complex32; 2],
    weights: [f32; 2],
}

impl ScratchSample for PhaseSample {
    const BYTES: usize = 52;

    fn write_to(&self, bytes: &mut [u8]) {
        let words = [
            self.segment,
            self.antennas[0] as u32,
            self.antennas[1] as u32,
        ]
        .into_iter()
        .chain(
            self.data
                .into_iter()
                .chain(self.model)
                .flat_map(|value| [value.re.to_bits(), value.im.to_bits()]),
        )
        .chain(self.weights.map(f32::to_bits));
        for (destination, source) in bytes.as_chunks_mut::<4>().0.iter_mut().zip(words) {
            *destination = source.to_le_bytes();
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let words = bytes.as_chunks::<4>().0;
        let word = |index| u32::from_le_bytes(words[index]);
        let float = |index| f32::from_bits(word(index));
        let complex = |index| Complex32::new(float(index), float(index + 1));
        Self {
            segment: word(0),
            antennas: [word(1) as i32, word(2) as i32],
            data: [complex(3), complex(5)],
            model: [complex(7), complex(9)],
            weights: [float(11), float(12)],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_sample_scratch_format_keeps_segment_baseline_and_cross_hands() {
        let sample = PhaseSample {
            segment: 0x1234,
            antennas: [1, 2],
            data: [Complex32::new(1.5, -0.75), Complex32::new(-3.0, 4.0)],
            model: [Complex32::new(2.75, -1.0), Complex32::new(1.125, 0.25)],
            weights: [3.125, 0.0],
        };
        let mut bytes = [0; PhaseSample::BYTES];
        sample.write_to(&mut bytes);
        assert_eq!(
            &bytes[..16],
            &[0x34, 0x12, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 192, 63]
        );
        assert_eq!(PhaseSample::from_bytes(&bytes), sample);
    }
}
