// SPDX-License-Identifier: LGPL-3.0-or-later

//! Lossless row framing for the existing ordered weighting coverage stream.
//!
//! This changes its private encoding, not the fields or checks at the source
//! boundary. Each sample starts with a new-row (1) or same-row (0) tag. New rows
//! carry the MS/row/DDID/SPW identity and optional native lattice; every sample
//! carries channel, correlation, output frequency and a length-prefixed spectral
//! value vector. An end (2) tag precedes the terminal sample count.
//!
//! Reuse compares encoded bytes, including float bits and option tags. The inline
//! row key survives chunk boundaries and is cloned/adopted with the SHA state.
//! No source array is retained, reread or separately verified by this encoding.

use super::{
    COVERAGE_DOMAIN, LogicalIdentity, WeightingGenerationId, WeightingReplayCoverageId,
    WeightingSampleValue,
};
use crate::{spectral_sampling::NativeRowSpectralGeometry, weighting::WeightingSpectralValue};
use casa_imaging_model::SelectedSampleAddress;
use sha2::{Digest, Sha256};

const STREAM_VERSION: u32 = 6;
const COVERAGE_HASH_CHUNK_BYTES: usize = 256;
// Tag + MS + row + DDID + SPW + full optional native geometry.
const ROW_HEADER_BYTES: usize = 1 + 32 + 8 + 4 + 4 + 1 + 16 + 12 + 1 + 12 + 1 + 16;

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoverageProofWork {
    pub(crate) bytes: u64,
    pub(crate) hash_calls: u64,
}

impl CoverageProofWork {
    fn checked_add(self, other: Self) -> Self {
        Self {
            bytes: self
                .bytes
                .checked_add(other.bytes)
                .expect("coverage proof byte count fits u64"),
            hash_calls: self
                .hash_calls
                .checked_add(other.hash_calls)
                .expect("coverage proof hash-call count fits u64"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CoverageEncoder {
    pub(super) hasher: Option<Sha256>,
    derived: Option<WeightingReplayCoverageId>,
    pub(super) work: CoverageProofWork,
    previous_row: [u8; ROW_HEADER_BYTES],
    previous_row_len: usize,
}

impl CoverageEncoder {
    pub(crate) fn checkpoint_token(&self) -> [u8; 32] {
        if let Some(hasher) = &self.hasher {
            hasher.clone().finalize().into()
        } else {
            let mut hasher = Sha256::new();
            hasher.update(b"casa-rs-derived-weighting-checkpoint");
            hasher.update(
                self.derived
                    .expect("derived coverage has a proof")
                    .as_bytes(),
            );
            hasher.finalize().into()
        }
    }

    pub(crate) fn new() -> Self {
        let mut encoder = Self {
            hasher: Some(Sha256::new()),
            derived: None,
            work: CoverageProofWork {
                bytes: 0,
                hash_calls: 0,
            },
            previous_row: [0; ROW_HEADER_BYTES],
            previous_row_len: 0,
        };
        encoder.update(COVERAGE_DOMAIN);
        encoder.update(&STREAM_VERSION.to_be_bytes());
        encoder
    }

    pub(crate) fn derived(coverage: WeightingReplayCoverageId) -> Self {
        Self {
            hasher: None,
            derived: Some(coverage),
            work: CoverageProofWork {
                bytes: 0,
                hash_calls: 0,
            },
            previous_row: [0; ROW_HEADER_BYTES],
            previous_row_len: 0,
        }
    }

    pub(super) fn update(&mut self, bytes: &[u8]) {
        self.work.bytes = self
            .work
            .bytes
            .checked_add(u64::try_from(bytes.len()).expect("coverage proof chunk fits u64"))
            .expect("coverage proof byte count fits u64");
        self.work.hash_calls = self
            .work
            .hash_calls
            .checked_add(1)
            .expect("coverage proof hash-call count fits u64");
        self.hasher
            .as_mut()
            .expect("encoded coverage owns a hasher")
            .update(bytes);
    }

    pub(crate) fn push(&mut self, weighted: &WeightingSampleValue) {
        let sample = weighted.selected();
        self.push_parts(
            sample.address,
            sample.row_spectral_geometry,
            sample.output_frame_frequency_hz,
            &weighted.spectral_values,
        );
    }

    pub(crate) fn push_parts(
        &mut self,
        address: SelectedSampleAddress,
        geometry: Option<NativeRowSpectralGeometry>,
        output_frame_frequency_hz: f64,
        spectral_values: &[WeightingSpectralValue],
    ) {
        if self.derived.is_some() {
            return;
        }
        let mut chunk = [0_u8; COVERAGE_HASH_CHUNK_BYTES];
        let mut used = 1;
        chunk[0] = 1;
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &address.measurement_set.identity().as_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &address.physical_row.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &address.data_description_id.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &address.spectral_window_id.to_be_bytes(),
        );
        match geometry {
            Some(geometry) => {
                append_coverage_bytes(self, &mut chunk, &mut used, &[1]);
                append_coverage_bytes(
                    self,
                    &mut chunk,
                    &mut used,
                    &(geometry.selected_channels() as u128).to_be_bytes(),
                );
                let first = geometry.first();
                append_coverage_bytes(self, &mut chunk, &mut used, &first.0.to_be_bytes());
                append_coverage_bytes(
                    self,
                    &mut chunk,
                    &mut used,
                    &first.1.to_bits().to_be_bytes(),
                );
                if let Some(second) = geometry.second() {
                    append_coverage_bytes(self, &mut chunk, &mut used, &[1]);
                    append_coverage_bytes(self, &mut chunk, &mut used, &second.0.to_be_bytes());
                    append_coverage_bytes(
                        self,
                        &mut chunk,
                        &mut used,
                        &second.1.to_bits().to_be_bytes(),
                    );
                } else {
                    append_coverage_bytes(self, &mut chunk, &mut used, &[0]);
                }
                match geometry.lattice_first_pair_hz {
                    Some(pair) => {
                        append_coverage_bytes(self, &mut chunk, &mut used, &[1]);
                        for frequency in pair {
                            append_coverage_bytes(
                                self,
                                &mut chunk,
                                &mut used,
                                &frequency.to_bits().to_be_bytes(),
                            );
                        }
                    }
                    None => append_coverage_bytes(self, &mut chunk, &mut used, &[0]),
                }
            }
            None => append_coverage_bytes(self, &mut chunk, &mut used, &[0]),
        }
        debug_assert!(used <= ROW_HEADER_BYTES);
        if self.previous_row_len == used && self.previous_row[..used] == chunk[..used] {
            chunk[0] = 0;
            used = 1;
        } else {
            self.previous_row[..used].copy_from_slice(&chunk[..used]);
            self.previous_row_len = used;
        }
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &address.channel_index.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &address.correlation_index.to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &output_frame_frequency_hz.to_bits().to_be_bytes(),
        );
        append_coverage_bytes(
            self,
            &mut chunk,
            &mut used,
            &(spectral_values.len() as u64).to_be_bytes(),
        );
        for value in spectral_values {
            append_coverage_bytes(
                self,
                &mut chunk,
                &mut used,
                &value.contribution.output_channel().to_be_bytes(),
            );
            append_coverage_bytes(
                self,
                &mut chunk,
                &mut used,
                &value.contribution.factor().to_bits().to_be_bytes(),
            );
            append_coverage_bytes(
                self,
                &mut chunk,
                &mut used,
                &value
                    .contribution
                    .evaluation_frequency_hz()
                    .to_bits()
                    .to_be_bytes(),
            );
            append_coverage_bytes(
                self,
                &mut chunk,
                &mut used,
                &value.imaging_weight.to_bits().to_be_bytes(),
            );
        }
        self.update(&chunk[..used]);
    }

    pub(crate) fn adopt(&mut self, checkpoint: &Self) {
        self.clone_from(checkpoint);
    }

    /// Complete one independently prepared row using the unchanged field
    /// encoding. The native coordinator combines these digests in source order.
    pub(crate) fn finish_row(mut self, sample_count: u64) -> ([u8; 32], CoverageProofWork) {
        self.update(&[2]);
        self.update(&sample_count.to_be_bytes());
        (
            self.hasher
                .take()
                .expect("row coverage owns its encoder")
                .finalize()
                .into(),
            self.work,
        )
    }

    pub(crate) fn finish(
        mut self,
        generation: WeightingGenerationId,
        sample_count: u64,
    ) -> (WeightingReplayCoverageId, CoverageProofWork) {
        if let Some(coverage) = self.derived {
            return (coverage, self.work);
        }
        self.update(&[2]);
        self.update(&sample_count.to_be_bytes());
        let content_work = self.work;
        let content = self
            .hasher
            .take()
            .expect("encoded coverage owns a hasher")
            .finalize();
        let mut identity = Self::new();
        identity.update(&generation.as_bytes());
        identity.update(&content);
        let work = content_work.checked_add(identity.work);
        (
            WeightingReplayCoverageId(LogicalIdentity::from_sha256(
                identity
                    .hasher
                    .take()
                    .expect("coverage identity owns a hasher")
                    .finalize()
                    .into(),
            )),
            work,
        )
    }
}

#[inline]
fn append_coverage_bytes(
    encoder: &mut CoverageEncoder,
    chunk: &mut [u8; COVERAGE_HASH_CHUNK_BYTES],
    used: &mut usize,
    bytes: &[u8],
) {
    debug_assert!(bytes.len() <= chunk.len());
    if chunk.len() - *used < bytes.len() {
        encoder.update(&chunk[..*used]);
        *used = 0;
    }
    let end = *used + bytes.len();
    chunk[*used..end].copy_from_slice(bytes);
    *used = end;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::weighting::{WeightingSpectralValue, selected_sample_tests::native_row_sample};
    use casa_imaging_model::{MeasurementSetIdentity, SelectedSpectralContribution};

    fn sample(channel: u32, row: u64) -> WeightingSampleValue {
        let mut sample = native_row_sample(channel, row);
        sample.spectral_values = [
            WeightingSpectralValue {
                contribution: SelectedSpectralContribution::new(channel, 0.25, 100.0).unwrap(),
                imaging_weight: 2.0,
            },
            WeightingSpectralValue {
                contribution: SelectedSpectralContribution::new(channel + 1, 0.75, 200.0).unwrap(),
                imaging_weight: 3.0,
            },
        ]
        .into_iter()
        .collect();
        sample
    }

    fn changed_after_prefix(a: &WeightingSampleValue, b: &WeightingSampleValue) {
        let mut same = CoverageEncoder::new();
        same.push(a);
        let mut changed = same.clone();
        same.push(a);
        changed.push(b);
        assert_ne!(same.checkpoint_token(), changed.checkpoint_token());
    }

    #[test]
    fn row_frames_match_an_explicit_unambiguous_stream() {
        let first = native_row_sample(0, 0);
        let second = native_row_sample(1, 0);
        let mut bytes = COVERAGE_DOMAIN.to_vec();
        bytes.extend(6_u32.to_be_bytes());
        bytes.push(1); // New row.
        bytes.extend([1; 32]);
        bytes.extend(0_u64.to_be_bytes());
        bytes.extend(0_i32.to_be_bytes());
        bytes.extend(0_u32.to_be_bytes());
        bytes.push(1); // Native geometry present.
        bytes.extend(3_u128.to_be_bytes());
        bytes.extend(0_u32.to_be_bytes());
        bytes.extend(100_f64.to_bits().to_be_bytes());
        bytes.push(1); // Second selected channel present.
        bytes.extend(1_u32.to_be_bytes());
        bytes.extend(200_f64.to_bits().to_be_bytes());
        bytes.push(1); // Original lattice pair present.
        bytes.extend(100_f64.to_bits().to_be_bytes());
        bytes.extend(200_f64.to_bits().to_be_bytes());
        bytes.extend(0_u32.to_be_bytes());
        bytes.extend(0_u32.to_be_bytes());
        bytes.extend(100_f64.to_bits().to_be_bytes());
        bytes.extend(0_u64.to_be_bytes()); // No spectral values.

        let mut coverage = CoverageEncoder::new();
        coverage.push(&first);
        assert_eq!(
            coverage.checkpoint_token(),
            <[u8; 32]>::from(Sha256::digest(&bytes))
        );
        bytes.push(0); // Same complete row context, another channel.
        bytes.extend(1_u32.to_be_bytes());
        bytes.extend(0_u32.to_be_bytes());
        bytes.extend(200_f64.to_bits().to_be_bytes());
        bytes.extend(0_u64.to_be_bytes());
        coverage.push(&second);
        assert_eq!(
            coverage.checkpoint_token(),
            <[u8; 32]>::from(Sha256::digest(&bytes))
        );
        assert_eq!(coverage.work.bytes, bytes.len() as u64);
        let generation = WeightingGenerationId(LogicalIdentity::from_sha256([7; 32]));
        assert_ne!(
            coverage.clone().finish(generation, 2).0,
            coverage.clone().finish(generation, 3).0
        );
        bytes.push(2); // End of samples, then exact count.
        bytes.extend(2_u64.to_be_bytes());
        let mut identity = Sha256::new();
        identity.update(COVERAGE_DOMAIN);
        identity.update(6_u32.to_be_bytes());
        identity.update(generation.as_bytes());
        identity.update(Sha256::digest(&bytes));
        let (id, work) = coverage.finish(generation, 2);
        assert_eq!(id.as_bytes(), <[u8; 32]>::from(identity.finalize()));
        assert_eq!(
            work.bytes,
            (bytes.len() + COVERAGE_DOMAIN.len() + 4 + 64) as u64
        );
        assert_eq!(work.hash_calls, 10);
    }

    #[test]
    fn row_framing_preserves_every_existing_covered_field() {
        let base = sample(0, 0);
        type Mutation = (&'static str, fn(&mut WeightingSampleValue));
        let mutations: &[Mutation] = &[
            ("MS", |s| {
                s.sample.address.measurement_set =
                    MeasurementSetIdentity::new(LogicalIdentity::from_sha256([9; 32]))
            }),
            ("row", |s| s.sample.address.physical_row += 1),
            ("DDID", |s| s.sample.address.data_description_id += 1),
            ("SPW", |s| s.sample.address.spectral_window_id += 1),
            ("channel", |s| s.sample.address.channel_index += 1),
            ("correlation", |s| s.sample.address.correlation_index += 1),
            ("output frequency", |s| {
                s.sample.output_frame_frequency_hz += 1.0
            }),
            ("geometry absent", |s| s.sample.row_spectral_geometry = None),
            ("geometry count", |s| {
                s.sample.row_spectral_geometry.as_mut().unwrap().channels += 1
            }),
            ("first channel", |s| {
                s.sample.row_spectral_geometry.as_mut().unwrap().first.0 += 1
            }),
            ("first frequency", |s| {
                s.sample.row_spectral_geometry.as_mut().unwrap().first.1 += 1.0
            }),
            ("second absent", |s| {
                s.sample.row_spectral_geometry.as_mut().unwrap().second = None
            }),
            ("second channel", |s| {
                s.sample
                    .row_spectral_geometry
                    .as_mut()
                    .unwrap()
                    .second
                    .as_mut()
                    .unwrap()
                    .0 += 1
            }),
            ("second frequency", |s| {
                s.sample
                    .row_spectral_geometry
                    .as_mut()
                    .unwrap()
                    .second
                    .as_mut()
                    .unwrap()
                    .1 += 1.0
            }),
            ("lattice absent", |s| {
                s.sample
                    .row_spectral_geometry
                    .as_mut()
                    .unwrap()
                    .lattice_first_pair_hz = None
            }),
            ("lattice first", |s| {
                s.sample
                    .row_spectral_geometry
                    .as_mut()
                    .unwrap()
                    .lattice_first_pair_hz
                    .as_mut()
                    .unwrap()[0] += 1.0
            }),
            ("lattice second", |s| {
                s.sample
                    .row_spectral_geometry
                    .as_mut()
                    .unwrap()
                    .lattice_first_pair_hz
                    .as_mut()
                    .unwrap()[1] += 1.0
            }),
            ("spectral channel", |s| {
                s.spectral_values[0].contribution =
                    SelectedSpectralContribution::new(9, 0.25, 100.0).unwrap()
            }),
            ("spectral factor", |s| {
                s.spectral_values[0].contribution =
                    SelectedSpectralContribution::new(0, 0.5, 100.0).unwrap()
            }),
            ("spectral frequency", |s| {
                s.spectral_values[0].contribution =
                    SelectedSpectralContribution::new(0, 0.25, 101.0).unwrap()
            }),
            ("imaging weight", |s| {
                s.spectral_values[0].imaging_weight += 1.0
            }),
            ("spectral count", |s| {
                s.spectral_values.pop();
            }),
            ("spectral order", |s| s.spectral_values.swap(0, 1)),
        ];
        for (name, mutate) in mutations {
            let mut changed = base.clone();
            mutate(&mut changed);
            changed_after_prefix(&base, &changed);
            eprintln!("coverage mutation preserved: {name}");
        }
        let float_fields: &[fn(&mut WeightingSampleValue, f64)] = &[
            |s, v| s.sample.output_frame_frequency_hz = v,
            |s, v| s.sample.row_spectral_geometry.as_mut().unwrap().first.1 = v,
            |s, v| {
                s.sample
                    .row_spectral_geometry
                    .as_mut()
                    .unwrap()
                    .second
                    .as_mut()
                    .unwrap()
                    .1 = v
            },
            |s, v| {
                s.sample
                    .row_spectral_geometry
                    .as_mut()
                    .unwrap()
                    .lattice_first_pair_hz
                    .as_mut()
                    .unwrap()[0] = v
            },
            |s, v| {
                s.sample
                    .row_spectral_geometry
                    .as_mut()
                    .unwrap()
                    .lattice_first_pair_hz
                    .as_mut()
                    .unwrap()[1] = v
            },
            |s, v| s.spectral_values[0].imaging_weight = v,
        ];
        for set in float_fields {
            let mut positive = base.clone();
            set(&mut positive, 0.0);
            let mut negative = positive.clone();
            set(&mut negative, -0.0);
            changed_after_prefix(&positive, &negative);
        }
    }

    #[test]
    fn row_framing_is_chunk_independent_and_binds_sample_order() {
        let samples: Vec<_> = (0..12)
            .map(|i| {
                let mut s = sample(i % 3, u64::from(i / 3));
                let value = s.spectral_values[0];
                s.spectral_values =
                    std::iter::repeat_n(value, [0, 4, 9, 17][i as usize % 4]).collect();
                s
            })
            .collect();
        let mut whole = CoverageEncoder::new();
        for s in &samples {
            whole.push(s);
        }
        for split in 0..=samples.len() {
            let mut prefix = CoverageEncoder::new();
            for s in &samples[..split] {
                prefix.push(s);
            }
            let mut resumed = CoverageEncoder::new();
            resumed.adopt(&prefix);
            for s in &samples[split..] {
                resumed.push(s);
            }
            assert_eq!(resumed.checkpoint_token(), whole.checkpoint_token());
            assert_eq!(resumed.work.bytes, whole.work.bytes);
            assert_eq!(resumed.work.hash_calls, whole.work.hash_calls);
        }
        let mut reordered = samples.clone();
        reordered.swap(0, 1);
        let mut changed = CoverageEncoder::new();
        for s in &reordered {
            changed.push(s);
        }
        assert_ne!(changed.checkpoint_token(), whole.checkpoint_token());
    }

    #[test]
    fn repeated_row_hash_bytes_are_bounded_by_rows_not_sample_count() {
        let mut coverage = CoverageEncoder::new();
        let start = coverage.work.bytes;
        for row in 0..2 {
            for channel in 0..512 {
                for correlation in 0..2 {
                    let mut s = sample(channel, row);
                    s.sample.row_spectral_geometry.as_mut().unwrap().channels = 512;
                    s.sample.address.correlation_index = correlation;
                    coverage.push(&s);
                }
            }
        }
        let samples = 2 * 512 * 2;
        let repeated_sample_bytes = 1 + 4 + 4 + 8 + 8 + 2 * 28;
        let bytes = coverage.work.bytes - start;
        assert_eq!(
            bytes,
            2 * (ROW_HEADER_BYTES as u64 - 1) + samples * repeated_sample_bytes
        );
        let old_bytes = samples * (32 + 8 + 4 + 4 + 4 + 4 + 8 + 58 + 2 * 28 + 1);
        assert!(bytes * 2 < old_bytes);
        assert_eq!(coverage.work.hash_calls, samples + 2);
        eprintln!(
            "coverage_samples={samples} old_projection_bytes={old_bytes} framed_bytes={bytes}"
        );
    }
}
