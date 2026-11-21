// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bound the retained preaverages for iterative channelized solves.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use num_complex::Complex32;

use super::standard::StandardSample;

const SAMPLE_BUFFER_BYTES: usize = 32 * 1024 * 1024;
const ENCODED_BYTES: usize = 64;

pub(super) struct ChannelSamples {
    pending: Vec<Vec<StandardSample>>,
    capacity: usize,
    buffered: usize,
    spill: Option<Spill>,
}

struct Spill {
    file: File,
    offset: u64,
    runs: Vec<Vec<(u64, usize)>>,
}

impl ChannelSamples {
    pub(super) fn new(channels: usize) -> Self {
        Self::with_capacity(channels, SAMPLE_BUFFER_BYTES / size_of::<StandardSample>())
    }

    fn with_capacity(channels: usize, capacity: usize) -> Self {
        Self {
            pending: (0..channels).map(|_| Vec::new()).collect(),
            capacity,
            buffered: 0,
            spill: None,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.buffered == 0 && self.spill.is_none()
    }

    pub(super) fn push(&mut self, channel: usize, sample: StandardSample) -> io::Result<()> {
        self.pending[channel].push(sample);
        self.buffered += 1;
        if self.buffered >= self.capacity {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.spill.is_none() {
            self.spill = Some(Spill {
                file: tempfile::tempfile()?,
                offset: 0,
                runs: vec![Vec::new(); self.pending.len()],
            });
        }
        let spill = self.spill.as_mut().expect("scratch is open");
        let mut writer = BufWriter::new(&mut spill.file);
        for (channel, pending) in self.pending.iter_mut().enumerate() {
            if !pending.is_empty() {
                spill.runs[channel].push((spill.offset, pending.len()));
                spill.offset += (pending.len() * ENCODED_BYTES) as u64;
                for sample in std::mem::take(pending) {
                    writer.write_all(&encode(&sample))?;
                }
            }
        }
        writer.flush()?;
        self.buffered = 0;
        Ok(())
    }

    /// Take one channel's averaged samples in their original order, including
    /// any batches written to the unnamed scratch file.
    pub(super) fn take(&mut self, channel: usize) -> io::Result<Vec<StandardSample>> {
        let pending = std::mem::take(&mut self.pending[channel]);
        let Some(spill) = self.spill.as_mut() else {
            return Ok(pending);
        };
        let runs = std::mem::take(&mut spill.runs[channel]);
        let mut samples =
            Vec::with_capacity(pending.len() + runs.iter().map(|(_, count)| count).sum::<usize>());
        let mut reader = BufReader::new(&mut spill.file);
        for (offset, count) in runs {
            reader.seek(SeekFrom::Start(offset))?;
            let mut bytes = [0; ENCODED_BYTES];
            for _ in 0..count {
                reader.read_exact(&mut bytes)?;
                samples.push(decode(bytes));
            }
        }
        samples.extend(pending);
        Ok(samples)
    }
}

// Scratch uses little-endian words and no platform-dependent padding. MS
// antenna IDs originate as signed 32-bit integers.
fn encode(sample: &StandardSample) -> [u8; ENCODED_BYTES] {
    let mut bytes = [0; ENCODED_BYTES];
    let words = sample
        .pair
        .map(|antenna| antenna as u32)
        .into_iter()
        .chain(
            sample
                .cross_hands
                .into_iter()
                .chain(sample.model)
                .flat_map(|value| [value.re.to_bits(), value.im.to_bits()]),
        )
        .chain(sample.weights.map(f32::to_bits));
    for (destination, source) in bytes.as_chunks_mut::<4>().0.iter_mut().zip(words) {
        *destination = source.to_le_bytes();
    }
    bytes
}

fn decode(bytes: [u8; ENCODED_BYTES]) -> StandardSample {
    let words: [u32; ENCODED_BYTES / 4] =
        std::array::from_fn(|index| u32::from_le_bytes(bytes.as_chunks::<4>().0[index]));
    let complex = |index: usize| {
        Complex32::new(
            f32::from_bits(words[index]),
            f32::from_bits(words[index + 1]),
        )
    };
    StandardSample {
        pair: [words[0] as usize, words[1] as usize],
        cross_hands: [complex(2), complex(4)],
        model: [complex(6), complex(8), complex(10), complex(12)],
        weights: [f32::from_bits(words[14]), f32::from_bits(words[15])],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(index: usize) -> StandardSample {
        StandardSample {
            pair: [index, 0x10203],
            cross_hands: [Complex32::new(1.5, -0.75), Complex32::new(2.0, -3.0)],
            model: [
                Complex32::new(4.0, -5.0),
                Complex32::new(-6.0, 7.0),
                Complex32::new(8.0, 9.0),
                Complex32::new(-10.0, -11.0),
            ],
            weights: [12.0, 13.0],
        }
    }

    #[test]
    fn spilled_preaverages_keep_channel_and_insertion_order() {
        let mut channels = ChannelSamples::with_capacity(3, 4);
        for index in 0..11 {
            channels.push(index % 3, sample(index)).unwrap();
        }
        assert!(channels.spill.is_some());
        for channel in (0..3).rev() {
            let actual = channels.take(channel).unwrap();
            let expected = (channel..11).step_by(3).map(|index| encode(&sample(index)));
            assert!(actual.iter().map(encode).eq(expected));
        }
        let encoded = encode(&sample(0x1234));
        assert_eq!(
            &encoded[..16],
            &[0x34, 0x12, 0, 0, 3, 2, 1, 0, 0, 0, 192, 63, 0, 0, 64, 191]
        );
        assert_eq!(encode(&decode(encoded)), encoded);
    }
}
