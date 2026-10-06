// SPDX-License-Identifier: LGPL-3.0-or-later

//! Preparation seam tests live under weighting to construct its private fixtures.
use super::*;
use crate::spectral_operator::accept_polarization_input;
use crate::streaming_cube::input::{NativeBlock, NativeInput, NativeLayout};
use casa_imaging_model::{CorrelationType, FiniteValuePolicy};
use num_complex::Complex64;
use std::io;

fn samples(rows: u64) -> Vec<WeightingSampleValue> {
    let mut result = Vec::new();
    for row in 0..rows {
        for channel in 0..3 {
            for corr in 0..2 {
                let mut weighted =
                    super::selected_sample_tests::native_row_sample(channel, row * 2 + 17);
                let sample = &mut weighted.sample;
                sample.address.channel_index *= 2;
                sample.address.correlation_index = corr * 2;
                sample.address.correlation_type = if corr == 0 {
                    CorrelationType::CircularRr
                } else {
                    CorrelationType::CircularLl
                };
                sample.starts_correlation_group = corr == 0;
                sample.ends_correlation_group = corr == 1;
                sample.correlation_group_size = 2;
                let start = 1e9 + row as f64 * 1e5;
                let step = if row % 2 == 0 { 1e6 } else { -1e6 };
                sample.output_frame_frequency_hz = start + channel as f64 * step;
                sample.row_spectral_geometry = Some(NativeRowSpectralGeometry {
                    channels: 3,
                    first: (0, start),
                    second: Some((2, start + step)),
                    lattice_first_pair_hz: Some([start - step, start]),
                });
                sample.visibility = if corr == 0 {
                    SelectedVisibilitySample::Float32(channel as f32 * 0.7 - 0.0)
                } else {
                    SelectedVisibilitySample::Complex32([row as f32 + 0.25, -0.0])
                };
                sample.channel_flag = channel == 1 && corr == 1;
                sample.parallel_hand_group_flag = channel == 2;
                sample.input_weight_group_flag = row % 2 == 1;
                sample.row_flag = row == 2;
                result.push(weighted);
            }
        }
    }
    result
}

fn layout(samples: &[WeightingSampleValue]) -> NativeLayout {
    NativeLayout::new(
        samples[0].selected().address(),
        vec![0, 2, 4],
        smallvec::smallvec![
            (0, CorrelationType::CircularRr),
            (2, CorrelationType::CircularLl)
        ],
    )
    .unwrap()
}

#[test]
fn preparation_carries_split_rows_and_correlations_without_extra_payload_buffers() {
    let samples = samples(5);
    for chunk_size in [1, 2, 3, 5, 6, 7, 13, 30] {
        let layout = layout(&samples);
        let channel_pointer = layout.channels.as_ptr();
        let block = NativeBlock::new(2, 3, 2).unwrap();
        let pointers = (
            block.values.as_ptr(),
            block.frequencies_hz.as_ptr(),
            block.metadata.as_ptr(),
        );
        let capacity = block.capacity_bytes();
        let mut input =
            NativeInput::new(layout, block, FiniteValuePolicy::FlagInputRejectGenerated).unwrap();
        let mut offset = 0;
        let mut emitted_rows = Vec::new();
        let mut emit = |block: &NativeBlock| {
            assert_eq!(
                (
                    block.values.as_ptr(),
                    block.frequencies_hz.as_ptr(),
                    block.metadata.as_ptr()
                ),
                pointers
            );
            assert_eq!(block.capacity_bytes(), capacity);
            emitted_rows.push(block.metadata.len());
            for row in 0..block.metadata.len() {
                let first = samples[offset + row * 6].selected();
                assert_eq!(block.metadata[row].physical_row, first.address.physical_row);
                assert_eq!(
                    block.metadata[row].original_pair_hz,
                    first
                        .row_spectral_geometry()
                        .unwrap()
                        .first_pair_hz()
                        .unwrap()
                );
                for channel in 0..3 {
                    assert_eq!(
                        block.frequencies_hz[row * 3 + channel],
                        samples[offset + row * 6 + channel * 2]
                            .sample
                            .output_frame_frequency_hz
                    );
                }
            }
            for (index, weighted) in samples[offset..offset + block.values.len()]
                .iter()
                .enumerate()
            {
                let sample = weighted.selected();
                let expected = match sample.visibility {
                    SelectedVisibilitySample::Float32(value) => {
                        Complex64::new(f64::from(value), 0.0)
                    }
                    SelectedVisibilitySample::Complex32([re, im]) => {
                        Complex64::new(f64::from(re), f64::from(im))
                    }
                };
                assert_eq!(block.values[index].re.to_bits(), expected.re.to_bits());
                assert_eq!(block.values[index].im.to_bits(), expected.im.to_bits());
                assert_eq!(
                    block.weights[index],
                    weighted.source_imaging_weight.unwrap()
                );
                assert_eq!(
                    block.flags[index],
                    !accept_polarization_input(sample, FiniteValuePolicy::FlagInputRejectGenerated)
                        .unwrap()
                );
                assert_eq!(
                    block.weight_flags[index],
                    sample.input_weight_group_flag
                        || sample.parallel_hand_group_flag
                        || sample.row_flag
                );
            }
            offset += block.values.len();
            Ok(())
        };
        for chunk in samples.chunks(chunk_size) {
            input.push(chunk, &mut emit).unwrap();
        }
        let (rows, layout) = input.finish(&mut emit).unwrap();
        assert_eq!(rows, 5);
        assert_eq!(layout.channels.as_ptr(), channel_pointer);
        assert_eq!(layout.address, samples[0].selected().address());
        assert_eq!(emitted_rows, [2, 2, 1]);
        assert_eq!(offset, samples.len());
    }
}

#[test]
fn preparation_preserves_finite_policy_and_nearest_weight_flag_distinction() {
    let mut samples = samples(1);
    samples[0].sample.visibility = SelectedVisibilitySample::Complex32([f32::NAN, -0.0]);
    samples[2].sample.raw_input_weight = -1.0;
    samples[4].sample.raw_input_weight = f32::INFINITY;
    let mut input = NativeInput::new(
        layout(&samples),
        NativeBlock::new(1, 3, 2).unwrap(),
        FiniteValuePolicy::FlagInputRejectGenerated,
    )
    .unwrap();
    input
        .push(&samples, |block| {
            assert!(block.values[0].re.is_nan());
            assert_eq!(block.values[0].im.to_bits(), (-0.0f64).to_bits());
            assert!(block.flags[0] && block.flags[2] && block.flags[4]);
            assert!(!block.weight_flags[0] && !block.weight_flags[2]);
            assert!(block.weight_flags[4]);
            Ok(())
        })
        .unwrap();
    input
        .finish(|_| panic!("full block already emitted"))
        .unwrap();
    let mut reject = NativeInput::new(
        layout(&samples),
        NativeBlock::new(1, 3, 2).unwrap(),
        FiniteValuePolicy::RejectAll,
    )
    .unwrap();
    assert!(
        reject
            .push(&samples, |_| panic!("invalid input emitted"))
            .is_err()
    );
    assert!(reject.finish(|_| Ok(())).is_err());
}

#[test]
fn preparation_rejects_incomplete_mixed_or_misordered_input_and_propagates_sink_errors() {
    let samples = samples(2);
    let new = || {
        NativeInput::new(
            layout(&samples),
            NativeBlock::new(2, 3, 2).unwrap(),
            FiniteValuePolicy::FlagInputRejectGenerated,
        )
        .unwrap()
    };
    for length in [0, 1, 5, 7, 11] {
        let mut input = new();
        input.push(&samples[..length], |_| Ok(())).unwrap();
        assert!(input.finish(|_| Ok(())).is_err());
    }
    for mutation in 0..7 {
        let mut changed = samples.clone();
        match mutation {
            0 => changed[1].sample.address.spectral_window_id += 1,
            1 => changed[2].sample.address.channel_index += 1,
            2 => changed[1].sample.address.correlation_index = 0,
            3 => changed[1].sample.address.physical_row += 1,
            4 => changed[6].sample.address.physical_row = changed[0].sample.address.physical_row,
            5 => changed[0].sample.row_spectral_geometry = None,
            _ => changed[0].source_imaging_weight = None,
        }
        let mut input = new();
        assert!(input.push(&changed, |_| Ok(())).is_err());
        assert!(input.push(&samples, |_| Ok(())).is_err());
        assert!(input.finish(|_| Ok(())).is_err());
    }
    let mut input = new();
    let error = input
        .push(&samples, |_| Err(io::Error::from_raw_os_error(28)))
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(28));
    assert!(input.finish(|_| Ok(())).is_err());
}

#[test]
fn shared_native_buffer_rejects_overflow_and_layout_mismatch() {
    assert!(NativeBlock::new(usize::MAX, 2, 2).is_err());
    assert!(NativeBlock::new(1, usize::MAX, 4).is_err());
    assert!(NativeBlock::new(1, 2, 5).is_err());
    let samples = samples(1);
    assert!(
        NativeInput::new(
            layout(&samples),
            NativeBlock::new(1, 3, 1).unwrap(),
            FiniteValuePolicy::RejectAll
        )
        .is_err()
    );
    let mut block = NativeBlock::new(1, 3, 2).unwrap();
    block.values.pop();
    assert!(NativeInput::new(layout(&samples), block, FiniteValuePolicy::RejectAll).is_err());
    let layout = layout(&samples);
    assert!(NativeLayout::new(layout.address, vec![0, 2, 1], layout.correlations.clone()).is_err());
}
