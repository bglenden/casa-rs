// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bound the retained preaverages for channelized polarization solves.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use num_complex::Complex32;

use super::standard::StandardSample;

const SAMPLE_BUFFER_BYTES: usize = 32 * 1024 * 1024;
const ENCODED_BYTES: usize = 88;

pub(super) trait ScratchSample {
    const BYTES: usize;

    fn write_to(&self, bytes: &mut [u8]);
    fn from_bytes(bytes: &[u8]) -> Self;
}

pub(super) struct ChannelSamples<S = StandardSample> {
    pending: Vec<Vec<S>>,
    capacity: usize,
    buffered: usize,
    spill: Option<Spill>,
}

struct Spill {
    file: File,
    offset: u64,
    runs: Vec<Vec<(u64, usize)>>,
}

impl<S: ScratchSample> ChannelSamples<S> {
    pub(super) fn new(channels: usize) -> Self {
        Self::with_capacity(channels, SAMPLE_BUFFER_BYTES / size_of::<S>())
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

    pub(super) fn push(&mut self, channel: usize, sample: S) -> io::Result<()> {
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
        let mut bytes = vec![0; S::BYTES];
        for (channel, pending) in self.pending.iter_mut().enumerate() {
            if !pending.is_empty() {
                spill.runs[channel].push((spill.offset, pending.len()));
                spill.offset += (pending.len() * S::BYTES) as u64;
                for sample in std::mem::take(pending) {
                    sample.write_to(&mut bytes);
                    writer.write_all(&bytes)?;
                }
            }
        }
        writer.flush()?;
        self.buffered = 0;
        Ok(())
    }

    /// Take one channel's averaged samples in their original order, including
    /// any batches written to the unnamed scratch file.
    pub(super) fn take(&mut self, channel: usize) -> io::Result<Vec<S>> {
        let Some(spill) = &self.spill else {
            return Ok(std::mem::take(&mut self.pending[channel]));
        };
        let count = self.pending[channel].len()
            + spill.runs[channel]
                .iter()
                .map(|(_, count)| count)
                .sum::<usize>();
        let mut samples = Vec::with_capacity(count);
        self.drain(channel, |sample| samples.push(sample))?;
        Ok(samples)
    }

    pub(super) fn drain(&mut self, channel: usize, mut visit: impl FnMut(S)) -> io::Result<()> {
        if let Some(spill) = self.spill.as_mut() {
            let runs = std::mem::take(&mut spill.runs[channel]);
            let mut reader = BufReader::new(&mut spill.file);
            let mut bytes = vec![0; S::BYTES];
            for (offset, count) in runs {
                reader.seek(SeekFrom::Start(offset))?;
                for _ in 0..count {
                    reader.read_exact(&mut bytes)?;
                    visit(S::from_bytes(&bytes));
                }
            }
        }
        std::mem::take(&mut self.pending[channel])
            .into_iter()
            .for_each(visit);
        Ok(())
    }
}

impl ScratchSample for StandardSample {
    const BYTES: usize = ENCODED_BYTES;

    fn write_to(&self, bytes: &mut [u8]) {
        bytes.copy_from_slice(&encode(self));
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        decode(
            bytes
                .try_into()
                .expect("standard polarization sample length"),
        )
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
                .data
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
        data: std::array::from_fn(|corr| complex(2 + 2 * corr)),
        model: std::array::from_fn(|corr| complex(10 + 2 * corr)),
        weights: std::array::from_fn(|corr| f32::from_bits(words[18 + corr])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(index: usize) -> StandardSample {
        StandardSample {
            pair: [index, 0x10203],
            data: [
                Complex32::new(1.5, -0.75),
                Complex32::new(2.0, -3.0),
                Complex32::new(-0.125, -1.25),
                Complex32::new(2.75, 3.25),
            ],
            model: [
                Complex32::new(4.0, -5.0),
                Complex32::new(-6.0, 7.0),
                Complex32::new(8.0, 9.0),
                Complex32::new(-10.0, -11.0),
            ],
            weights: [12.0, 13.0, 14.0, 15.0],
        }
    }

    #[test]
    fn spilled_preaverages_keep_channel_and_insertion_order() {
        let mut channels = ChannelSamples::<StandardSample>::with_capacity(3, 4);
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
