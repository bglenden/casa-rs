// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::MuellerMatrix;
use casa_imaging_model::{
    CorrelationType, DirectionCoordinateSpec, DirectionFrame, FrequencyFrame, LogicalIdentity,
    MeasurementSetIdentity, Projection, SkyDirection,
};
use ndarray::{Array2, s};

/// CPU spatial double that also records each dispatch's per-batch tap counts.
struct Backend {
    convolution: StandardConvolution,
    grids: [Array3<Complex32>; 4],
    dispatches: Vec<Vec<usize>>,
}

impl CubeSpatialBackend for Backend {
    fn initialize(
        &mut self,
        shape: [usize; 2],
        weights: &[[f32; 7]],
        fields: [usize; 4],
        model: &[Complex32],
    ) -> Result<(), SpectralOperatorError> {
        assert_eq!(weights, self.convolution.float_weights());
        self.grids = fields.map(|planes| Array3::zeros((planes, shape[0], shape[1])));
        self.grids[3].as_slice_mut().unwrap().copy_from_slice(model);
        Ok(())
    }
    fn degrid(
        &mut self,
        batches: &mut [SpatialPredictionBatch],
    ) -> Result<(), SpectralOperatorError> {
        self.dispatches
            .push(batches.iter().map(|b| b.taps.len()).collect());
        for batch in batches {
            for (tap, value) in batch.taps.iter().zip(&mut batch.values) {
                *value = self
                    .convolution
                    .degrid_float(&self.grids[3].index_axis(Axis(0), batch.plane), unpack(tap));
            }
        }
        Ok(())
    }
    fn grid(&mut self, batches: &[SpatialGridBatch]) -> Result<(), SpectralOperatorError> {
        self.dispatches
            .push(batches.iter().map(|b| b.taps.len()).collect());
        for batch in batches {
            let index = match batch.field {
                SpatialField::Dirty => 0,
                SpatialField::Residual => 1,
                SpatialField::Psf => 2,
                SpatialField::Model => 3,
            };
            for tap in &batch.taps {
                self.convolution.grid_float(
                    &mut self.grids[index].index_axis_mut(Axis(0), batch.plane),
                    unpack(tap),
                    Complex32::new(tap.value[0], tap.value[1]),
                );
            }
        }
        Ok(())
    }
    fn download(
        &mut self,
        _: SpatialField,
        _: usize,
        _: &mut [Complex32],
    ) -> Result<(), SpectralOperatorError> {
        unreachable!()
    }
}

fn unpack(tap: &SpatialTap) -> crate::spectral_operator::SampleTaps {
    use crate::spectral_operator::{SampleTaps, TapSpan};
    SampleTaps {
        x: TapSpan {
            start: tap.x as usize,
            weight_index: tap.x_weights as usize,
        },
        y: TapSpan {
            start: tap.y as usize,
            weight_index: tap.y_weights as usize,
        },
    }
}

#[test]
fn batched_spatial_preparation_matches_cpu_for_flags_phase_and_nonzero_model() {
    let output = [1e9, 1.002e9, 1.004e9, 1.006e9];
    let mut block = NativeBlock::new(4, 12, 2).unwrap();
    let mut layout = None;
    for (r, shift) in [-0.6e6, -0.6e6, 0.1e6, 0.6e6].into_iter().enumerate() {
        let input = Input::new(
            (0..12)
                .map(|ch| 0.996e9 + shift + ch as f64 * 1e6)
                .collect(),
        );
        let row = input.row(0..12);
        layout.get_or_insert_with(|| {
            NativeLayout::new(
                row.address,
                input.channels.clone(),
                smallvec::smallvec![
                    (0, CorrelationType::CircularRr),
                    (1, CorrelationType::CircularLl)
                ],
            )
            .unwrap()
        });
        block.metadata[r] = super::super::input::RowMetadata {
            physical_row: r as u64,
            uvw_m: row.uvw_m,
            phase_shift_m: row.phase_shift_m,
            original_pair_hz: row.original_pair_hz,
        };
        block.frequencies_hz[r * 12..(r + 1) * 12].copy_from_slice(row.frequencies_hz);
        block.values[r * 24..(r + 1) * 24].copy_from_slice(row.values);
        block.weights[r * 24..(r + 1) * 24].copy_from_slice(row.weights);
        block.flags[r * 24..(r + 1) * 24].copy_from_slice(row.flags);
        block.weight_flags[r * 24..(r + 1) * 24].copy_from_slice(row.weight_flags);
    }
    let layout = layout.unwrap();
    let raw = model();
    let polarization = polarization();
    for phase in [BandPhase::InitialZero, BandPhase::Full, BandPhase::Residual] {
        let make = || {
            let mut w = BandWorkspace::new(
                geometry(),
                0..4,
                (0..4).collect(),
                PreparedFft::new([10, 10], 7690, 1).unwrap(),
                phase,
                None,
            );
            if phase != BandPhase::InitialZero {
                w.prepare_model(raw.view()).unwrap();
            }
            w
        };
        let mut cpu = make();
        let mut candidate = make();
        let mut backend = Backend {
            convolution: StandardConvolution::new(&geometry()),
            grids: std::array::from_fn(|_| Array3::zeros((0, 10, 10))),
            dispatches: Vec::new(),
        };
        backend
            .initialize(
                [10, 10],
                &candidate.convolution.float_weights(),
                [
                    candidate.dirty.len_of(Axis(0)),
                    candidate.residual.len_of(Axis(0)),
                    candidate.psf.len_of(Axis(0)),
                    candidate.forward.len_of(Axis(0)),
                ],
                candidate.forward.as_slice().unwrap(),
            )
            .unwrap();
        // Repeated refills accumulate into the same resident grids.
        for _ in 0..2 {
            cpu.consume_block(
                block.view().unwrap(),
                &layout,
                0..12,
                0..12,
                &output,
                &polarization,
            )
            .unwrap();
            candidate
                .consume_spatial(
                    block.view().unwrap(),
                    &layout,
                    0..12,
                    &output,
                    &polarization,
                    &mut backend,
                )
                .unwrap();
        }
        assert_close(backend.grids[0].iter().copied(), cpu.dirty.iter().copied());
        assert_close(
            backend.grids[1].iter().copied(),
            cpu.residual.iter().copied(),
        );
        assert_close(backend.grids[2].iter().copied(), cpu.psf.iter().copied());
        assert_eq!(candidate.mapped, cpu.mapped);
        assert_eq!(candidate.sum_weight, cpu.sum_weight);
    }
}

#[test]
fn spatial_memory_includes_initialization_for_empty_source() {
    let plan = BandPlan {
        geometry: geometry(),
        core: 0..4,
        total_channels: 4,
        fine_per_output: 1,
        single_channel: None,
        phase: BandPhase::InitialZero,
        support: BandSupport {
            native: 0..0,
            model: Vec::new(),
        },
    };
    assert!(plan.spatial_host_bytes(0).unwrap() >= BandPlan::spatial_weight_bytes());
    assert_eq!(plan.spatial_request_capacity(0).unwrap(), 0);
}

#[test]
fn spatial_request_capacity_counts_fine_samples_of_coarse_output_planes() {
    // 1 MHz native channels under 2 MHz outputs: two CASA fine samples per
    // row and coarse plane, all bracketed by the unflagged in-grid native row.
    let output = [1e9, 1.002e9, 1.004e9, 1.006e9];
    let mut input = Input::new((0..12).map(|ch| 0.996e9 + ch as f64 * 1e6).collect());
    input.weights.fill(1.0);
    input.flags.fill(false);
    input.weight_flags.fill(false);
    let row = input.row(0..12);
    let layout = NativeLayout::new(
        row.address,
        input.channels.clone(),
        smallvec::smallvec![
            (0, CorrelationType::CircularRr),
            (1, CorrelationType::CircularLl),
        ],
    )
    .unwrap();
    let rows = 3;
    let mut block = NativeBlock::new(rows, 12, 2).unwrap();
    for r in 0..rows {
        block.metadata[r] = super::super::input::RowMetadata {
            physical_row: r as u64,
            uvw_m: row.uvw_m,
            phase_shift_m: row.phase_shift_m,
            original_pair_hz: row.original_pair_hz,
        };
        block.frequencies_hz[r * 12..(r + 1) * 12].copy_from_slice(row.frequencies_hz);
        block.values[r * 24..(r + 1) * 24].copy_from_slice(row.values);
        block.weights[r * 24..(r + 1) * 24].copy_from_slice(row.weights);
        block.flags[r * 24..(r + 1) * 24].copy_from_slice(row.flags);
        block.weight_flags[r * 24..(r + 1) * 24].copy_from_slice(row.weight_flags);
    }
    let raw = model();
    let polarization = polarization();
    for (phase, fields) in [
        (BandPhase::InitialZero, 2),
        (BandPhase::Full, 3),
        (BandPhase::Residual, 1),
    ] {
        let mut plan = BandPlan {
            geometry: geometry(),
            core: 0..4,
            total_channels: 4,
            fine_per_output: 1,
            single_channel: None,
            phase,
            support: BandSupport {
                native: 0..0,
                model: Vec::new(),
            },
        };
        BandPlan::observe_all(std::slice::from_mut(&mut plan), &block, &output).unwrap();
        assert_eq!(plan.fine_per_output, 2);
        assert_eq!(plan.support.model, [0, 1, 2, 3]);
        let capacity = plan.spatial_request_capacity(rows).unwrap();
        // Host preparation retains at least every packed request it can emit.
        let many = 1 << 12;
        assert!(
            plan.spatial_host_bytes(many).unwrap()
                >= plan.spatial_request_capacity(many).unwrap() * size_of::<SpatialTap>()
        );
        let mut workspace = BandWorkspace::new(
            geometry(),
            plan.core(),
            plan.support.model.clone(),
            PreparedFft::new([10, 10], 7690, 1).unwrap(),
            phase,
            None,
        );
        if phase != BandPhase::InitialZero {
            workspace.prepare_model(raw.view()).unwrap();
        }
        let mut backend = Backend {
            convolution: StandardConvolution::new(&geometry()),
            grids: std::array::from_fn(|_| Array3::zeros((0, 10, 10))),
            dispatches: Vec::new(),
        };
        backend
            .initialize(
                [10, 10],
                &workspace.convolution.float_weights(),
                [
                    workspace.dirty.len_of(Axis(0)),
                    workspace.residual.len_of(Axis(0)),
                    workspace.psf.len_of(Axis(0)),
                    workspace.forward.len_of(Axis(0)),
                ],
                workspace.forward.as_slice().unwrap(),
            )
            .unwrap();
        workspace
            .consume_spatial(
                block.view().unwrap(),
                &layout,
                plan.native_range(),
                &output,
                &polarization,
                &mut backend,
            )
            .unwrap();
        let grid = backend.dispatches.last().unwrap();
        assert_eq!(grid.len(), fields * 4);
        assert!(
            grid.iter().all(|&taps| taps == rows * 2),
            "{phase:?}: {grid:?}"
        );
        for dispatch in &backend.dispatches {
            assert!(dispatch.iter().sum::<usize>() <= capacity);
        }
        if phase != BandPhase::Residual {
            // Contributions, not native predictions, bound these phases.
            assert_eq!(grid.iter().sum::<usize>(), capacity);
        }
    }
}

fn geometry() -> SpectralOperatorGeometry {
    SpectralOperatorGeometry {
        image_shape: [8, 8],
        grid_shape: [10, 10],
        image_blc: [2, 2],
        reference_pixel: [4.0; 2],
        increment_rad: [-0.002, 0.002],
        direction: DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [4.0; 2],
            [-0.002, 0.002],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 0.0],
        ),
    }
}

fn model() -> Array3<Complex64> {
    Array3::from_shape_fn((4, 8, 8), |(channel, x, y)| {
        let pixel = (x * 8 + y) as f64;
        Complex64::new(
            channel as f64 * 0.2 + pixel * 0.003,
            channel as f64 * -0.04 + pixel * 0.001,
        )
    })
}

fn polarization() -> PolarizationOperator {
    PolarizationOperator::compile(
        &[PolarizationCoordinate::StokesI],
        &[CorrelationType::CircularRr, CorrelationType::CircularLl],
        [0.0; 2],
        MuellerMatrix::identity(),
    )
    .unwrap()
}

struct Input {
    frequencies: Vec<f64>,
    channels: Vec<u32>,
    values: Array2<Complex32>,
    weights: Array2<f32>,
    flags: Array2<bool>,
    weight_flags: Array2<bool>,
}

impl Input {
    fn new(frequencies: Vec<f64>) -> Self {
        let count = frequencies.len();
        Self {
            frequencies,
            channels: (0..count as u32).map(|channel| channel * 2).collect(),
            values: Array2::from_shape_fn((count, 2), |(ch, corr)| {
                Complex32::new(0.25 + ch as f32 * 0.12, -0.8 + corr as f32 * 0.2)
            }),
            weights: Array2::from_shape_fn((count, 2), |(ch, corr)| {
                if ch == 2 {
                    0.0
                } else {
                    0.3 + ch as f32 * 0.17 + corr as f32 * 0.11
                }
            }),
            flags: Array2::from_shape_fn((count, 2), |(ch, corr)| ch == 3 && corr == 1),
            weight_flags: Array2::from_shape_fn((count, 2), |(ch, _)| ch == 5),
        }
    }

    fn row(&self, native: Range<usize>) -> VisibilityRow<'_> {
        let frequency = self.frequencies[0];
        let samples = native.start * 2..native.end * 2;
        VisibilityRow {
            address: SelectedSampleAddress {
                measurement_set: MeasurementSetIdentity::new(LogicalIdentity::from_sha256([1; 32])),
                physical_row: 17,
                data_description_id: 0,
                spectral_window_id: 2,
                channel_index: 0,
                frequency_centre_hz: frequency,
                frequency_lower_hz: frequency - 1e6,
                frequency_upper_hz: frequency + 1e6,
                channel_width_hz: 2e6,
                frequency_frame: FrequencyFrame::Topocentric,
                polarization_id: 1,
                correlation_index: 0,
                correlation_type: CorrelationType::CircularRr,
            },
            uvw_m: [7.0, -3.0, 0.0],
            phase_shift_m: 0.017,
            original_pair_hz: [self.frequencies[0], self.frequencies[1]],
            channels: &self.channels[native.clone()],
            frequencies_hz: &self.frequencies[native.clone()],
            correlations: 2,
            values: &self.values.as_slice().unwrap()[samples.clone()],
            weights: &self.weights.as_slice().unwrap()[samples.clone()],
            flags: &self.flags.as_slice().unwrap()[samples.clone()],
            weight_flags: &self.weight_flags.as_slice().unwrap()[samples],
        }
    }
}

fn workspace(
    core: Range<usize>,
    model_channels: Vec<usize>,
    model: &Array3<Complex64>,
) -> BandWorkspace {
    let mut band = BandWorkspace::new(
        geometry(),
        core,
        model_channels,
        PreparedFft::new([10, 10], 7690, 1).unwrap(),
        BandPhase::Full,
        None,
    );
    band.prepare_model(model.view()).unwrap();
    band
}

fn full_band(input: &Input, output: &[f64], model: &Array3<Complex64>) -> BandWorkspace {
    let mut band = workspace(0..4, (0..4).collect(), model);
    let polarization = polarization();
    let input_row = input.row(0..input.channels.len());
    let mut row = band.begin_row(&input_row, output, &polarization).unwrap();
    row.push(0..input.channels.len()).unwrap();
    row.finish().unwrap();
    band
}

fn assert_close<T: Copy + Into<f64>>(
    actual: impl IntoIterator<Item = num_complex::Complex<T>>,
    expected: impl IntoIterator<Item = num_complex::Complex<T>>,
) {
    let actual = actual.into_iter().collect::<Vec<_>>();
    let expected = expected.into_iter().collect::<Vec<_>>();
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.into_iter().zip(expected) {
        let difference =
            (actual.re.into() - expected.re.into()).hypot(actual.im.into() - expected.im.into());
        let scale = expected.re.into().hypot(expected.im.into()).max(1.0);
        assert!(
            difference <= 2e-6 * scale,
            "difference={difference}, scale={scale}"
        );
    }
}

#[test]
fn single_output_uses_native_frequencies_and_ignores_neighbour_flags() {
    use super::super::input::RowMetadata;
    for increment in [2e6, -2e6, 8e6] {
        let single = CasaSingleChannel {
            centre_hz: 1e9,
            increment_hz: increment,
        };
        assert!(single.contains(1e9 - increment * 0.5));
        assert!(!single.contains(1e9 + increment * 0.5));
        for centre in [0.998e9, 1e9, 1.002e9] {
            let single = CasaSingleChannel {
                centre_hz: centre,
                ..single
            };
            let mut input = Input::new(vec![0.998e9, 1e9, 1.002e9]);
            input.weights.fill(1.0);
            for ch in 0..3 {
                input
                    .flags
                    .row_mut(ch)
                    .fill(!single.contains(input.frequencies[ch]));
            }
            let expected_range = single.native_window(&input.frequencies);
            let mut block = NativeBlock::new(1, 3, 2).unwrap();
            block.frequencies_hz.copy_from_slice(&input.frequencies);
            block.metadata[0] = RowMetadata {
                original_pair_hz: [0.998e9, 1e9],
                ..RowMetadata::default()
            };
            let mut plan = BandPlan {
                geometry: geometry(),
                core: 0..1,
                total_channels: 1,
                fine_per_output: 1,
                single_channel: Some(single),
                phase: BandPhase::Full,
                support: BandSupport {
                    native: 0..0,
                    model: vec![],
                },
            };
            assert_eq!(
                BandPlan::observe_all(std::slice::from_mut(&mut plan), &block, &[centre]).unwrap(),
                0
            );
            assert_eq!(plan.support.native, expected_range);
            assert_eq!(plan.support.model, [0]);
            let model = model().slice(s![0..1, .., ..]).to_owned();
            let make = || {
                let mut band = BandWorkspace::new(
                    geometry(),
                    0..1,
                    vec![0],
                    PreparedFft::new([10, 10], 7690, 1).unwrap(),
                    BandPhase::Full,
                    Some(single),
                );
                band.prepare_model(model.view()).unwrap();
                band
            };
            let mut actual = make();
            let mut expected = make();
            let polarization = polarization();
            actual
                .consume_single_row(input.row(0..3), single, &[centre], &polarization)
                .unwrap();
            let row = input.row(0..3);
            for channel in expected_range {
                let frequency = input.frequencies[channel];
                let taps = expected
                    .convolution
                    .taps([
                        row.uvw_m[0] * frequency / SPEED_OF_LIGHT_M_PER_S,
                        row.uvw_m[1] * frequency / SPEED_OF_LIGHT_M_PER_S,
                    ])
                    .unwrap();
                let predicted = expected
                    .convolution
                    .degrid_float(&expected.forward.index_axis(Axis(0), 0), taps);
                let predicted = widen(predicted) * phase(row.phase_shift_m, frequency).conj();
                let observed =
                    (widen(input.values[(channel, 0)]) + widen(input.values[(channel, 1)])) / 2.0;
                expected
                    .grid_sample(0, frequency, &row, observed, predicted, 2.0)
                    .unwrap();
            }
            for (actual, expected) in actual
                .dirty
                .iter()
                .chain(actual.residual.iter())
                .chain(actual.psf.iter())
                .zip(
                    expected
                        .dirty
                        .iter()
                        .chain(expected.residual.iter())
                        .chain(expected.psf.iter()),
                )
            {
                assert!((*actual - *expected).norm() <= 1e-14 * expected.norm().max(1.0));
            }
            assert!((actual.sum_weight[0] - expected.sum_weight[0]).abs() < 1e-14);
            assert!(actual.residual.iter().any(|value| value.norm() != 0.0));
            // The retained native window can contain just one contributing channel.
            let mut selected = make();
            selected
                .consume_single_row(
                    row.window(plan.support.native).unwrap(),
                    single,
                    &[centre],
                    &polarization,
                )
                .unwrap();
            assert_eq!(selected.residual, actual.residual);
        }
    }
}

#[test]
fn preparation_support_matches_row_reference_with_one_pair_sweep_for_all_bands() {
    use super::super::input::RowMetadata;
    // Include gaps, reversed axes, row shifts, and wholly unmapped rows.
    for (step, shift) in [(1.0, 0.0), (1.0, 0.25), (2.75, 0.31), (4.0, 0.0)] {
        for reverse_native in [false, true] {
            for reverse_output in [false, true] {
                let mut output: Vec<_> = (0..4)
                    .map(|ch| 1e9 + (shift + ch as f64 * step) * 1e6)
                    .collect();
                if reverse_output {
                    output.reverse();
                }
                let mut block = NativeBlock::new(4, 18, 2).unwrap();
                for (row, offset) in [-0.6e6, 0.1e6, 0.6e6, 100e6].into_iter().enumerate() {
                    let native = &mut block.frequencies_hz[row * 18..(row + 1) * 18];
                    for (index, frequency) in native.iter_mut().enumerate() {
                        let channel = index + usize::from(index >= 5) + usize::from(index >= 10);
                        *frequency = 0.997e9 + offset + channel as f64 * 1e6;
                    }
                    if reverse_native {
                        native.reverse();
                    }
                    block.metadata[row] = RowMetadata {
                        physical_row: row as u64,
                        uvw_m: [0.0; 3],
                        phase_shift_m: 0.0,
                        original_pair_hz: [native[0], native[1]],
                    };
                }
                for depth in [1, 2, 4] {
                    let mut bands: Vec<_> = (0..4)
                        .step_by(depth)
                        .map(|start| BandPlan {
                            geometry: geometry(),
                            core: start..start + depth,
                            total_channels: 4,
                            fine_per_output: 1,
                            single_channel: None,
                            phase: BandPhase::Full,
                            support: BandSupport {
                                native: 0..0,
                                model: Vec::new(),
                            },
                        })
                        .collect();
                    let visits = BandPlan::observe_all(&mut bands, &block, &output).unwrap();
                    assert_eq!(visits, 4 * 17, "pair work must not grow with band count");
                    for band in &bands {
                        let mut expected = BandSupport {
                            native: 0..0,
                            model: Vec::new(),
                        };
                        for (row, metadata) in block.metadata.iter().enumerate() {
                            expected.include(
                                BandSupport::compile(
                                    &output,
                                    band.core.clone(),
                                    &block.frequencies_hz[row * 18..(row + 1) * 18],
                                    metadata.original_pair_hz,
                                )
                                .unwrap(),
                            );
                        }
                        assert_eq!(band.support, expected);
                        assert!(band.support.model.capacity() <= band.total_channels);
                    }
                    let retained: Vec<_> = bands.iter().map(|band| band.support.clone()).collect();
                    BandPlan::observe_all(&mut bands, &block, &output).unwrap();
                    assert_eq!(
                        bands
                            .iter()
                            .map(|band| band.support.clone())
                            .collect::<Vec<_>>(),
                        retained
                    );
                }
            }
        }
    }
}

#[test]
fn preparation_reuses_only_identical_spectral_rows_without_losing_support() {
    let output = [1.0e9, 1.001e9, 1.002e9, 1.003e9];
    let mut block = NativeBlock::new(8, 6, 2).unwrap();
    for row in 0..8 {
        for channel in 0..6 {
            block.frequencies_hz[row * 6 + channel] = 0.999e9 + channel as f64 * 1.0e6;
        }
        block.metadata[row].original_pair_hz = [0.999e9, 1.0e9];
        block.metadata[row].physical_row = row as u64;
        block.metadata[row].uvw_m = [row as f64, 3.0, -2.0];
    }
    // Both an interior frequency change and a global interpolation-pair change
    // must invalidate reuse; different baseline geometry must not.
    block.frequencies_hz[3 * 6 + 3] += 0.25e6;
    block.metadata[5].original_pair_hz[1] += 0.1e6;
    let mut bands: Vec<_> = (0..4)
        .map(|channel| BandPlan {
            geometry: geometry(),
            core: channel..channel + 1,
            total_channels: 4,
            fine_per_output: 1,
            single_channel: None,
            phase: BandPhase::Full,
            support: BandSupport {
                native: 0..0,
                model: Vec::new(),
            },
        })
        .collect();
    let visits = BandPlan::observe_all(&mut bands, &block, &output).unwrap();
    assert_eq!(visits, 5 * 5);
    for band in &bands {
        let mut expected = BandSupport {
            native: 0..0,
            model: Vec::new(),
        };
        for (row, metadata) in block.metadata.iter().enumerate() {
            expected.include(
                BandSupport::compile(
                    &output,
                    band.core.clone(),
                    &block.frequencies_hz[row * 6..(row + 1) * 6],
                    metadata.original_pair_hz,
                )
                .unwrap(),
            );
        }
        assert_eq!(band.support, expected);
    }
}

#[test]
fn exact_zero_model_planes_skip_forward_work_without_losing_halo_terms() {
    let output = vec![1e9, 1.001e9, 1.002e9, 1.003e9];
    let input = Input::new((-2..7).map(|ch| 1e9 + ch as f64 * 1e6).collect());
    let polarization = polarization();
    let mut sparse = Array3::zeros((4, 8, 8));
    sparse[(1, 3, 4)] = Complex64::new(1e-20, -1e-21);
    let mut band = workspace(0..4, vec![0, 1, 2, 3], &sparse);
    assert_eq!(band.forward_nonzero, [false, true, false, false]);
    let halo = workspace(0..1, vec![0, 1], &sparse);
    let row = input.row(0..input.channels.len());
    let mut unskipped = workspace(0..4, vec![0, 1, 2, 3], &sparse);
    // Exercise the zero-valued degrid terms that the sparse path skips.
    unskipped.forward_nonzero.fill(true);
    for &frequency in &input.frequencies {
        assert_close(
            band.predict_native(&row, frequency, &output, &polarization)
                .unwrap(),
            unskipped
                .predict_native(&row, frequency, &output, &polarization)
                .unwrap(),
        );
    }
    for &frequency in &output[..2] {
        let predicted = band
            .predict_native(&row, frequency, &output, &polarization)
            .unwrap();
        assert_close(
            halo.predict_native(&row, frequency, &output, &polarization)
                .unwrap(),
            predicted.iter().copied(),
        );
        if frequency == output[0] {
            assert!(predicted.iter().all(|value| value.norm() == 0.0));
        } else {
            assert!(predicted.iter().any(|value| value.norm() > 0.0));
        }
    }
    assert!(
        !band
            .prepare_plane(1, |_, _| Complex64::new(-0.0, 0.0))
            .unwrap()
    );
    assert_eq!(band.forward_nonzero, [false; 4]);
    assert!(
        band.forward
            .iter()
            .all(|value| *value == Complex32::default())
    );
    assert!(
        band.prepare_plane(1, |x, y| if (x, y) == (3, 4) {
            Complex64::new(1e-20, 0.0)
        } else {
            Complex64::default()
        })
        .unwrap()
    );
    assert!(matches!(
        band.prepare_plane(2, |_, _| Complex64::new(f64::NAN, 0.0)),
        Err(SpectralOperatorError::GeneratedNonfinite)
    ));
    let missing = workspace(0..4, vec![1], &sparse);
    assert!(matches!(
        missing.predict_native(&row, output[0], &output, &polarization),
        Err(SpectralOperatorError::IncompleteSpectralHalo)
    ));
}

#[test]
fn nonzero_native_prediction_and_band_grids_are_partition_and_chunk_invariant() {
    // Direct, fractional-wide and wide-grid cases, including reversed axes.
    for (step, shift) in [(1.0, 0.0), (1.0, 0.25), (2.75, 0.31), (4.0, 0.0)] {
        for native_direction in [1.0, -1.0] {
            for output_direction in [1.0, -1.0] {
                let mut output: Vec<_> = (0..4)
                    .map(|ch| 1e9 + (shift + ch as f64 * step) * 1e6)
                    .collect();
                if output_direction < 0.0 {
                    output.reverse();
                }
                let mut native: Vec<_> = (-3..17).map(|ch| 1e9 + ch as f64 * 1e6).collect();
                if native_direction < 0.0 {
                    native.reverse();
                }
                let input = Input::new(native);
                let model = model();
                let reference = full_band(&input, &output, &model);
                assert!(reference.residual.iter().any(|value| value.norm() > 0.0));
                let polarization = polarization();
                for depth in [1, 2, 4] {
                    for start in (0..4).step_by(depth) {
                        let core = start..start + depth;
                        let support = BandSupport::compile(
                            &output,
                            core.clone(),
                            &input.frequencies,
                            [input.frequencies[0], input.frequencies[1]],
                        )
                        .unwrap();
                        assert!(!support.native.is_empty());
                        let mut band = workspace(core.clone(), support.model, &model);
                        let count = support.native.len();
                        let input_row = input.row(support.native.clone());
                        for &frequency in input_row.frequencies_hz {
                            let predicted = band
                                .predict_native(&input_row, frequency, &output, &polarization)
                                .unwrap();
                            assert!(!predicted.spilled());
                            let expected = reference
                                .predict_native(&input_row, frequency, &output, &polarization)
                                .unwrap();
                            assert_close(predicted, expected);
                        }
                        let mut row = band.begin_row(&input_row, &output, &polarization).unwrap();
                        // End a chunk inside the row, then resume the exact cursor.
                        row.push(0..1).unwrap();
                        row.push(1..count).unwrap();
                        row.finish().unwrap();
                        for (local, global) in core.enumerate() {
                            assert_close(
                                band.dirty.index_axis(Axis(0), local).iter().copied(),
                                reference.dirty.index_axis(Axis(0), global).iter().copied(),
                            );
                            assert_close(
                                band.residual.index_axis(Axis(0), local).iter().copied(),
                                reference
                                    .residual
                                    .index_axis(Axis(0), global)
                                    .iter()
                                    .copied(),
                            );
                            assert_close(
                                band.psf.index_axis(Axis(0), local).iter().copied(),
                                reference.psf.index_axis(Axis(0), global).iter().copied(),
                            );
                            assert_eq!(
                                band.sum_weight[local].to_bits(),
                                reference.sum_weight[global].to_bits()
                            );
                            assert_eq!(band.mapped[local], reference.mapped[global]);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn closure_includes_neighbors_and_excludes_unrelated_model_planes() {
    let output = [1e9, 1.001e9, 1.002e9, 1.003e9];
    let mut input = Input::new((0..8).map(|ch| 1e9 - 0.25e6 + ch as f64 * 0.7e6).collect());
    // This test isolates model support; the matrix above covers flags/zero weights.
    input.weights.fill(1.0);
    input.flags.fill(false);
    input.weight_flags.fill(false);
    let support = BandSupport::compile(
        &output,
        1..2,
        &input.frequencies,
        [input.frequencies[0], input.frequencies[1]],
    )
    .unwrap();
    assert_eq!(support.model, [0, 1, 2]);
    let polarization = polarization();
    let evaluate = |model: &Array3<Complex64>, channels: Vec<usize>| {
        let mut band = workspace(1..2, channels, model);
        let input_row = input.row(support.native.clone());
        let mut row = band.begin_row(&input_row, &output, &polarization)?;
        row.push(0..support.native.len())?;
        row.finish()?;
        Ok::<_, SpectralOperatorError>(band.residual)
    };
    let original = model();
    let expected = evaluate(&original, support.model.clone()).unwrap();
    assert!(expected.iter().any(|value| value.norm() > 0.0));
    let mut changed = original.clone();
    changed
        .index_axis_mut(Axis(0), 3)
        .fill(Complex64::new(1000.0, -300.0));
    assert_close(
        evaluate(&changed, support.model.clone()).unwrap(),
        expected.iter().copied(),
    );
    changed
        .index_axis_mut(Axis(0), 2)
        .fill(Complex64::new(1000.0, -300.0));
    assert_ne!(evaluate(&changed, support.model.clone()).unwrap(), expected);
    assert!(matches!(
        evaluate(&original, vec![1]),
        Err(SpectralOperatorError::IncompleteSpectralHalo)
    ));
}

#[test]
fn compact_views_are_zero_copy_and_reject_bad_shape_or_partial_rows() {
    let input = Input::new(vec![1e9, 1.001e9, 1.002e9, 1.003e9]);
    let output = input.frequencies.clone();
    let polarization = polarization();
    let row = input.row(1..3);
    assert_eq!(
        row.values.as_ptr(),
        input.values.as_slice().unwrap()[2..].as_ptr()
    );
    row.validate(2).unwrap();
    assert!(row.validate(1).is_err());
    for field in 0..4 {
        let mut malformed = input.row(1..3);
        match field {
            0 => malformed.values = &malformed.values[..3],
            1 => malformed.weights = &malformed.weights[..3],
            2 => malformed.flags = &malformed.flags[..3],
            _ => malformed.weight_flags = &malformed.weight_flags[..3],
        }
        assert!(malformed.validate(2).is_err());
    }
    let mut band = workspace(0..4, vec![0, 1, 2, 3], &model());
    let allocation = band.forward.as_ptr();
    band.prepare_model(model().view()).unwrap();
    assert_eq!(band.forward.as_ptr(), allocation);
    let input_row = input.row(0..4);
    let mut row = band.begin_row(&input_row, &output, &polarization).unwrap();
    row.push(0..2).unwrap();
    assert!(row.push(3..4).is_err());
    assert!(matches!(
        row.finish(),
        Err(SpectralOperatorError::IncompleteCoverage)
    ));
    assert!(matches!(
        band.begin_row(&input_row, &output, &polarization)
            .unwrap()
            .finish(),
        Err(SpectralOperatorError::IncompleteCoverage)
    ));
    let (mut first, mut second) = band.dirty.view_mut().split_at(Axis(0), 2);
    first.fill(Complex32::new(1.0, 0.0));
    second.fill(Complex32::new(2.0, 0.0));
    assert!(
        band.dirty
            .slice(s![0..2, .., ..])
            .iter()
            .all(|v| v.re == 1.0)
    );
    assert!(
        band.dirty
            .slice(s![2..4, .., ..])
            .iter()
            .all(|v| v.re == 2.0)
    );
}

#[test]
fn flat_row_windows_borrow_all_payloads_at_each_correlation_width() {
    let input = Input::new(vec![1e9, 1.001e9, 1.002e9, 1.003e9]);
    for correlations in 1..=4 {
        let layout = NativeLayout::new(
            input.row(0..4).address,
            input.channels.clone(),
            (0..correlations)
                .map(|index| (index as u32, CorrelationType::CircularRr))
                .collect(),
        )
        .unwrap();
        let mut block = NativeBlock::new(2, 4, correlations).unwrap();
        for (index, value) in block.values.iter_mut().enumerate() {
            *value = Complex32::new(index as f32, -(index as f32));
        }
        let row = block.row(&layout, 1, 0..4).unwrap().window(1..4).unwrap();
        let nested = row.window(1..2).unwrap();
        nested.validate(correlations).unwrap();
        let start = 6 * correlations;
        assert_eq!(nested.values, &block.values[start..start + correlations]);
        assert_eq!(nested.values.as_ptr(), block.values[start..].as_ptr());
        assert_eq!(nested.weights.as_ptr(), block.weights[start..].as_ptr());
        assert_eq!(nested.flags.as_ptr(), block.flags[start..].as_ptr());
        assert_eq!(
            nested.weight_flags.as_ptr(),
            block.weight_flags[start..].as_ptr()
        );
        assert_eq!(nested.channels, &layout.channels[2..3]);
        let view = block.view().unwrap();
        let rows = view.rows(&layout, 0..4, 1..4).unwrap();
        let mut direct = rows.row(1);
        direct.restrict(1..2).unwrap();
        direct.validate(correlations).unwrap();
        assert_eq!(direct.address, nested.address);
        assert_eq!(direct.values.as_ptr(), nested.values.as_ptr());
        assert_eq!(direct.weights.as_ptr(), nested.weights.as_ptr());
        assert_eq!(direct.flags.as_ptr(), nested.flags.as_ptr());
        assert_eq!(direct.weight_flags.as_ptr(), nested.weight_flags.as_ptr());
        assert_eq!(
            direct.frequencies_hz.as_ptr(),
            nested.frequencies_hz.as_ptr()
        );
        assert_eq!(direct.channels, nested.channels);
        assert!(view.rows(&layout, 0..3, 0..3).is_err());
        assert!(view.rows(&layout, 1..5, 0..4).is_err());
        assert!(view.rows(&layout, 0..4, 0..0).is_err());
        assert!(view.rows(&layout, 0..4, 1..5).is_err());
        assert!(direct.restrict(0..2).is_err());
        assert!(block.row(&layout, 2, 0..4).is_err());
    }
}

#[test]
fn new_numeric_owner_cannot_import_historical_containers_or_io() {
    let source = include_str!("band.rs");
    for forbidden in [
        "SpectralSlabOperator",
        "NativeSpectralGroup",
        "CasaResampledGroup",
        "WeightingSampleValue",
        "ReducedRecordKey",
        "RecordRole",
        "StandardRecordScratch",
        "GriddedNormalCompilationPlan",
        "std::fs",
        "std::io",
        "Arc<",
        "Mutex<",
    ] {
        assert!(
            !source.contains(forbidden),
            "new numeric owner depends on {forbidden}"
        );
    }
}

#[test]
fn stored_native_buffer_is_borrowed_directly_by_the_band_kernel() {
    use super::super::input::RowMetadata;
    let input = Input::new((0..6).map(|ch| 1e9 + ch as f64 * 2e6).collect());
    let output = [1.001e9, 1.003e9, 1.005e9, 1.007e9];
    let original = input.row(0..6);
    let layout = NativeLayout::new(
        original.address,
        input.channels.clone(),
        smallvec::smallvec![
            (0, CorrelationType::CircularRr),
            (1, CorrelationType::CircularLl)
        ],
    )
    .unwrap();
    let mut block = NativeBlock::new(1, 6, 2).unwrap();
    block.metadata[0] = RowMetadata {
        physical_row: original.address.physical_row,
        uvw_m: original.uvw_m,
        phase_shift_m: original.phase_shift_m,
        original_pair_hz: original.original_pair_hz,
    };
    block.frequencies_hz.copy_from_slice(&input.frequencies);
    block
        .values
        .copy_from_slice(input.values.as_slice().unwrap());
    block
        .weights
        .copy_from_slice(input.weights.as_slice().unwrap());
    block.flags.copy_from_slice(input.flags.as_slice().unwrap());
    block
        .weight_flags
        .copy_from_slice(input.weight_flags.as_slice().unwrap());
    let row = block.row(&layout, 0, 0..6).unwrap();
    assert_eq!(row.values.as_ptr(), block.values.as_ptr());
    assert_eq!(row.weights.as_ptr(), block.weights.as_ptr());
    assert_eq!(row.frequencies_hz.as_ptr(), block.frequencies_hz.as_ptr());
    let borrowed = NativeBlockView::new(
        &block.metadata,
        &block.frequencies_hz,
        input.values.as_slice().unwrap(),
        &block.weights,
        &block.flags,
        &block.weight_flags,
        6,
        2,
    )
    .unwrap();
    let source_row = borrowed.row(&layout, 0, 0..6).unwrap();
    assert_eq!(source_row.values.as_ptr(), input.values.as_ptr());
    assert!(
        NativeBlockView::new(
            &block.metadata,
            &block.frequencies_hz,
            input.values.as_slice().unwrap(),
            &block.weights[..block.weights.len() - 1],
            &block.flags,
            &block.weight_flags,
            6,
            2,
        )
        .is_err()
    );
    let model = model();
    let mut expected = workspace(0..4, (0..4).collect(), &model);
    let mut actual = workspace(0..4, (0..4).collect(), &model);
    let mut source_borrowed = workspace(0..4, (0..4).collect(), &model);
    let polarization = polarization();
    for (workspace, row) in [
        (&mut expected, original),
        (&mut actual, row),
        (&mut source_borrowed, source_row),
    ] {
        let mut accumulator = workspace.begin_row(&row, &output, &polarization).unwrap();
        accumulator.push(0..6).unwrap();
        accumulator.finish().unwrap();
    }
    assert_eq!(actual.dirty, expected.dirty);
    assert_eq!(actual.residual, expected.residual);
    assert_eq!(actual.psf, expected.psf);
    assert_eq!(actual.sum_weight, expected.sum_weight);
    assert_eq!(actual.mapped, expected.mapped);
    assert_eq!(source_borrowed.dirty, expected.dirty);
    assert_eq!(source_borrowed.residual, expected.residual);
    assert_eq!(source_borrowed.psf, expected.psf);
    assert!(block.row(&layout, 1, 0..6).is_err());
    assert!(block.row(&layout, 0, 1..6).is_err());
}

#[path = "../../tests/support/streaming_cube.rs"]
#[allow(dead_code)]
mod fixture;

#[derive(Debug, Default)]
struct ModelReads {
    ranges: std::sync::Mutex<Vec<Range<usize>>>,
    fail: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct ObservedModelStorage {
    samples: std::sync::RwLock<Box<[casa_imaging_model::ModelSample]>>,
    reads: std::sync::Arc<ModelReads>,
}

impl crate::ModelSampleStorage for ObservedModelStorage {
    fn sample_count(&self) -> usize {
        self.samples.read().unwrap().len()
    }
    fn read(
        &self,
        start: usize,
        destination: &mut [casa_imaging_model::ModelSample],
    ) -> Result<(), crate::ModelLifecycleError> {
        self.reads
            .ranges
            .lock()
            .unwrap()
            .push(start..start + destination.len());
        if self.reads.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::ModelLifecycleError::Storage(
                "injected model read failure".into(),
            ));
        }
        destination
            .copy_from_slice(&self.samples.read().unwrap()[start..start + destination.len()]);
        Ok(())
    }
    fn write(
        &mut self,
        start: usize,
        samples: &[casa_imaging_model::ModelSample],
    ) -> Result<(), crate::ModelLifecycleError> {
        self.samples.get_mut().unwrap()[start..start + samples.len()].copy_from_slice(samples);
        Ok(())
    }

    fn apply_updates(
        &self,
        updates: &[crate::ModelSampleUpdate],
        precision: casa_imaging_model::NumericPrecision,
        bound: f64,
    ) -> Result<f64, crate::ModelLifecycleError> {
        crate::ModelSampleStorage::apply_updates(&self.samples, updates, precision, bound)
    }
}

impl crate::ModelStorageFactory for std::sync::Arc<ModelReads> {
    fn create(
        &self,
        count: usize,
    ) -> Result<Box<dyn crate::ModelSampleStorage>, crate::ModelLifecycleError> {
        Ok(Box::new(ObservedModelStorage {
            samples: std::sync::RwLock::new(
                vec![casa_imaging_model::ModelSample::invalid(); count].into(),
            ),
            reads: self.clone(),
        }))
    }
}

fn generation(
    window: usize,
    values: impl Fn(usize, usize, usize) -> casa_imaging_model::ModelSample,
) -> (ModelGeneration, std::sync::Arc<ModelReads>) {
    generation_with_origin(window, Some(values))
}

fn generation_with_origin(
    window: usize,
    values: Option<impl Fn(usize, usize, usize) -> casa_imaging_model::ModelSample>,
) -> (ModelGeneration, std::sync::Arc<ModelReads>) {
    use casa_imaging_model::*;
    let problem = fixture::problem(SpectralSamplingLaw::IDENTITY);
    let reads = std::sync::Arc::new(ModelReads::default());
    let lifecycle = crate::ModelLifecycle::bind(
        crate::ExecutableModelProblem::from_compiled(problem).unwrap(),
        ModelExecutionAttemptId::new(LogicalIdentity::from_sha256([71; 32])),
        1,
        crate::ModelStoragePlan::new(std::sync::Arc::new(reads.clone()), window).unwrap(),
    )
    .unwrap();
    let Some(values) = values else {
        return (lifecycle.initial_empty().unwrap(), reads);
    };
    let mut samples = Vec::new();
    for channel in 0..4 {
        for y in 0..8 {
            for x in 0..8 {
                samples.push(values(channel, x, y));
            }
        }
    }
    let generation = lifecycle
        .mint_generation(
            samples,
            crate::ModelGenerationOrigin::Ingested {
                source: LogicalIdentity::from_sha256([72; 32]),
                reprojection: None,
            },
        )
        .unwrap();
    (generation, reads)
}

fn real_model(channel: usize, x: usize, y: usize) -> casa_imaging_model::ModelSample {
    casa_imaging_model::ModelSample::valid(
        casa_imaging_model::ModelValue::new(
            0.2 * channel as f64 + 0.01 * x as f64 - 0.03 * y as f64,
        )
        .unwrap(),
    )
}

#[test]
fn spatial_tap_preserves_checked_integer_projection_and_overflow_errors() {
    use crate::spectral_operator::{SampleTaps, TapSpan};
    let taps = SampleTaps {
        x: TapSpan {
            start: 7,
            weight_index: 11,
        },
        y: TapSpan {
            start: 13,
            weight_index: 17,
        },
    };
    let value = Complex32::new(0.25, -0.5);
    let actual = SpatialTap::new(taps, value).unwrap();
    assert_eq!(
        [actual.x, actual.y, actual.x_weights, actual.y_weights],
        [7, 13, 11, 17]
    );
    assert_eq!(actual.value, [value.re, value.im]);
    let maximum = SampleTaps {
        x: TapSpan {
            start: u32::MAX as usize,
            weight_index: u32::MAX as usize,
        },
        y: TapSpan {
            start: u32::MAX as usize,
            weight_index: u32::MAX as usize,
        },
    };
    assert!(SpatialTap::new(maximum, value).is_ok());
    #[cfg(target_pointer_width = "64")]
    for field in 0..4 {
        let mut invalid = taps;
        let index = match field {
            0 => &mut invalid.x.start,
            1 => &mut invalid.y.start,
            2 => &mut invalid.x.weight_index,
            _ => &mut invalid.y.weight_index,
        };
        *index = u32::MAX as usize + 1;
        assert!(matches!(
            SpatialTap::new(invalid, value),
            Err(SpectralOperatorError::ResidencyOverflow)
        ));
    }
}

#[test]
fn connected_residual_reuses_coarse_predictions_with_bounded_refill_storage() {
    let (model, _) = generation(64, real_model);
    let output = [1e9, 1.002e9, 1.004e9, 1.006e9];
    let input = Input::new((0..12).map(|ch| 0.996e9 + ch as f64 * 1e6).collect());
    let row = input.row(0..12);
    let layout = NativeLayout::new(
        row.address,
        input.channels.clone(),
        smallvec::smallvec![
            (0, CorrelationType::CircularRr),
            (1, CorrelationType::CircularLl),
        ],
    )
    .unwrap();
    let metadata = [super::super::input::RowMetadata {
        physical_row: 0,
        uvw_m: row.uvw_m,
        phase_shift_m: row.phase_shift_m,
        original_pair_hz: row.original_pair_hz,
    }];
    let block = NativeBlockView::new(
        &metadata,
        row.frequencies_hz,
        row.values,
        row.weights,
        row.flags,
        row.weight_flags,
        12,
        2,
    )
    .unwrap();
    let bands: Vec<_> = (0..4)
        .map(|ch| BandPlan {
            geometry: geometry(),
            core: ch..ch + 1,
            total_channels: 4,
            fine_per_output: 2,
            single_channel: None,
            phase: BandPhase::Residual,
            support: BandSupport {
                native: 0..12,
                model: (0..4).collect(),
            },
        })
        .collect();
    let wave = BandPlan::residual_wave(&bands).unwrap();
    assert_eq!(wave.support.model, [0, 1, 2, 3]);
    let capacity = wave.residual_capacities(1).unwrap();
    let job = wave.prepare(&model, None).unwrap();
    use bytemuck::Zeroable;
    let mut predictions = vec![ResidualPrediction::zeroed(); capacity[0] + 1];
    let mut native = vec![NativePrediction::zeroed(); capacity[1] + 1];
    let mut samples = vec![ResidualSample::zeroed(); capacity[2] + 1];
    predictions[capacity[0]].padding = 0xdead;
    native[capacity[1]].indices = [0xdead; 2];
    samples[capacity[2]].plane = 0xdead;
    let pointers = (predictions.as_ptr(), native.as_ptr(), samples.as_ptr());
    for _ in 0..2 {
        let mut refill = ResidualRefill {
            predictions: &mut predictions[..capacity[0]],
            native: &mut native[..capacity[1]],
            samples: &mut samples[..capacity[2]],
            counts: [usize::MAX; 3],
            requested_predictions: u64::MAX,
        };
        job.prepare_residual_refill(block, &layout, 0..12, &output, &mut refill)
            .unwrap();
        assert!(refill.requested_predictions > refill.counts[0] as u64);
        let planes: std::collections::BTreeSet<_> = refill.predictions[..refill.counts[0]]
            .iter()
            .map(|v| v.plane)
            .collect();
        assert_eq!(
            planes.len(),
            refill.counts[0],
            "one gather per row/coarse plane"
        );
        assert!(refill.counts[2] <= capacity[2]);
        for sample in &refill.samples[..refill.counts[2]] {
            assert!((sample.left as usize) < refill.counts[1]);
            assert!((sample.right as usize) < refill.counts[1]);
            assert!((sample.nearest_flags & ((1 << 30) - 1)) < refill.counts[1] as u32);
        }
        assert_eq!(
            pointers,
            (
                refill.predictions.as_ptr(),
                refill.native.as_ptr(),
                refill.samples.as_ptr()
            )
        );
    }
    assert_eq!(predictions[capacity[0]].padding, 0xdead);
    assert_eq!(native[capacity[1]].indices, [0xdead; 2]);
    assert_eq!(samples[capacity[2]].plane, 0xdead);
    for field in 0..3 {
        let mut bounded = capacity;
        bounded[field] = 0;
        let mut undersized = ResidualRefill {
            predictions: &mut predictions[..bounded[0]],
            native: &mut native[..bounded[1]],
            samples: &mut samples[..bounded[2]],
            counts: [0; 3],
            requested_predictions: 0,
        };
        assert!(matches!(
            job.prepare_residual_refill(block, &layout, 0..12, &output, &mut undersized),
            Err(SpectralOperatorError::ResidencyOverflow)
        ));
    }
    let mut mismatch = bands.clone();
    mismatch[1].geometry.increment_rad[0] *= 2.0;
    assert!(matches!(
        BandPlan::residual_wave(&mismatch),
        Err(SpectralOperatorError::ProblemMismatch)
    ));
}

#[test]
fn band_memory_accounts_for_actual_phase_buffers_and_completed_ownership() {
    use std::mem::size_of;
    let (empty, _) = generation_with_origin(
        64,
        None::<fn(usize, usize, usize) -> casa_imaging_model::ModelSample>,
    );
    let (model, _) = generation(64, real_model);
    for depth in [1, 2, 4] {
        for phase in [BandPhase::InitialZero, BandPhase::Full] {
            let plan = BandPlan {
                geometry: geometry(),
                core: 0..depth,
                total_channels: 4,
                fine_per_output: 1,
                single_channel: None,
                phase,
                support: BandSupport {
                    native: 0..6,
                    model: (0..4).collect(),
                },
            };
            let memory = plan.memory().unwrap();
            assert_eq!(
                memory.resident_bytes() + memory.transition_bytes(),
                memory.peak_bytes()
            );
            assert!(memory.resident_bytes() >= memory.accumulation_bytes);
            assert!(memory.resident_bytes() >= memory.retained_bytes);
            let generation = if phase == BandPhase::InitialZero {
                &empty
            } else {
                &model
            };
            let job = plan.clone().prepare(generation, None).unwrap();
            assert_eq!(job.native_range, plan.native_range());
            let w = &job.workspace;
            let grids = [&w.forward, &w.dirty, &w.residual, &w.psf];
            let payload = grids
                .iter()
                .map(|grid| grid.len() * size_of::<Complex32>())
                .sum::<usize>()
                + w.sum_weight.capacity() * size_of::<f64>()
                + w.mapped.capacity() * size_of::<u64>()
                + w.model_channels.capacity() * size_of::<usize>()
                + w.forward_nonzero.capacity();
            let fft = fft_resident_complex_values_for_shape(geometry().grid_shape).unwrap()
                * size_of::<Complex32>();
            let convolution = StandardConvolution::dynamic_bytes(geometry().grid_shape).unwrap();
            assert_eq!(
                memory.accumulation_bytes,
                payload + fft + convolution + size_of::<BandPlan>() + size_of::<EpochBand<'_>>()
            );
            let (normal, fft_state) = job.complete(generation).unwrap();
            let BandResult::Initial(normal) = normal else {
                panic!("initial normal required")
            };
            assert_eq!(
                memory.retained_bytes,
                normal.cube_owned_bytes(true).unwrap() - size_of::<SpectralOperatorPrimitives>()
                    + size_of::<(BandResult, PreparedFft<f32>)>()
                    + fft
            );
            let refresh = plan.residual_refresh();
            let memory = refresh.memory().unwrap();
            let residual_image = depth * 64 * size_of::<f32>();
            assert!(
                memory.preparation_bytes
                    >= memory.accumulation_bytes
                        + 64 * size_of::<casa_imaging_model::ModelSample>()
            );
            let (updated, _) = refresh
                .prepare(&model, Some(fft_state))
                .unwrap()
                .complete(&model)
                .unwrap();
            let BandResult::Residual(updated) = updated else {
                panic!("residual-only result required")
            };
            assert_eq!(updated.values.len() * size_of::<f32>(), residual_image);
            let residual_grid = depth
                * geometry().grid_shape[0]
                * geometry().grid_shape[1]
                * size_of::<Complex32>();
            let completion_with_grid = size_of::<BandPlan>()
                + size_of::<EpochBand<'_>>()
                + fft
                + convolution
                + residual_grid
                + residual_image;
            let completion_with_result = residual_image
                + size_of::<(BandResult, PreparedFft<f32>)>()
                + fft
                + size_of::<BandPlan>()
                + size_of::<EpochBand<'_>>();
            assert_eq!(
                memory.completion_bytes,
                completion_with_grid.max(completion_with_result),
                "residual completion must exclude prediction/support owners"
            );
            assert_eq!(
                memory.retained_bytes,
                residual_image + size_of::<(BandResult, PreparedFft<f32>)>() + fft
            );
        }
    }
}

#[test]
fn band_memory_scales_from_shapes_and_rejects_overflow_without_allocating() {
    let mut plan = BandPlan {
        geometry: geometry(),
        core: 0..1,
        total_channels: 16_384,
        fine_per_output: 1,
        single_channel: None,
        phase: BandPhase::InitialZero,
        support: BandSupport {
            native: 0..0,
            model: Vec::new(),
        },
    };
    for (image, grid, depths) in [
        (8, 10, [1, 2, 4]),
        (512, 640, [1, 16, 64]),
        (4096, 5120, [1, 8, 32]),
    ] {
        plan.geometry.image_shape = [image; 2];
        plan.geometry.grid_shape = [grid; 2];
        let mut previous = 0;
        for depth in depths {
            plan.core = 0..depth;
            let memory = plan.memory().unwrap();
            assert!(memory.peak_bytes() > previous);
            assert!(
                memory.accumulation_bytes
                    >= 2 * depth * grid * grid * std::mem::size_of::<Complex32>()
            );
            previous = memory.peak_bytes();
        }
    }
    plan.geometry.grid_shape = [usize::MAX, 2];
    assert!(matches!(
        plan.memory(),
        Err(SpectralOperatorError::ResidencyOverflow)
    ));
    plan.geometry = geometry();
    plan.core = 0..0;
    assert!(matches!(
        plan.memory(),
        Err(SpectralOperatorError::InvalidSlab)
    ));
    plan.core = 0..1;
    let initial = plan.memory().unwrap();
    plan.phase = BandPhase::Residual;
    let residual = plan.memory().unwrap();
    assert!(residual.accumulation_bytes < initial.accumulation_bytes);
    assert!(residual.retained_bytes < initial.retained_bytes);
}

#[test]
fn model_epoch_reads_only_support_planes_with_correct_axes_and_invalid_support() {
    use casa_imaging_model::ModelSample;
    let value = |channel, x, y| {
        if (channel, x, y) == (2, 1, 5) {
            ModelSample::invalid()
        } else {
            real_model(channel, x, y)
        }
    };
    let (model, reads) = generation(64, value);
    let raw = Array3::from_shape_fn((4, 8, 8), |(channel, x, y)| {
        let sample = value(channel, x, y);
        Complex64::new(sample.value().value(), 0.0)
    });
    let expected = workspace(1..2, vec![0, 2], &raw);
    let owned = BandWorkspace::new(
        geometry(),
        1..2,
        vec![0, 2],
        PreparedFft::new([10, 10], 7690, 1).unwrap(),
        BandPhase::Full,
        None,
    );
    let job = EpochBand::prepare(owned, &model, 0..8).unwrap();
    assert_close(
        job.workspace.forward.iter().copied(),
        expected.forward.iter().copied(),
    );
    assert_eq!(*reads.ranges.lock().unwrap(), [0..64, 128..192]);
    assert!(
        model.read_samples(0..256).is_err(),
        "no whole cube read fits this plan"
    );
    let output = [1e9, 1.001e9, 1.002e9, 1.003e9];
    let input = Input::new((0..8).map(|ch| 1e9 - 0.25e6 + ch as f64 * 0.7e6).collect());
    let support = BandSupport::compile(
        &output,
        1..2,
        &input.frequencies,
        [input.frequencies[0], input.frequencies[1]],
    )
    .unwrap();
    let prepared = BandWorkspace::new(
        geometry(),
        1..2,
        support.model.clone(),
        PreparedFft::new([10, 10], 7690, 1).unwrap(),
        BandPhase::Full,
        None,
    );
    let mut job = EpochBand::prepare(prepared, &model, support.native.clone()).unwrap();
    let mut expected = workspace(1..2, support.model, &raw);
    let polarization = polarization();
    for band in [&mut job.workspace, &mut expected] {
        let input_row = input.row(support.native.clone());
        let mut row = band.begin_row(&input_row, &output, &polarization).unwrap();
        row.push(0..support.native.len()).unwrap();
        row.finish().unwrap();
    }
    assert_close(
        job.workspace.residual.iter().copied(),
        expected.residual.iter().copied(),
    );
}

#[test]
fn delayed_band_cannot_complete_into_another_model_epoch_and_model_io_errors_propagate() {
    let (model, reads) = generation(64, real_model);
    let (other, _) = generation(64, real_model);
    let new = || {
        BandWorkspace::new(
            geometry(),
            0..1,
            vec![0],
            PreparedFft::new([10, 10], 7690, 1).unwrap(),
            BandPhase::Full,
            None,
        )
    };
    let job = EpochBand::prepare(new(), &model, 0..8).unwrap();
    std::thread::scope(|scope| {
        let (send, release) = std::sync::mpsc::sync_channel(0);
        let expected = &other;
        let delayed = scope.spawn(move || {
            release.recv().unwrap();
            job.complete(expected).map(|_| ())
        });
        send.send(()).unwrap();
        assert!(matches!(
            delayed.join().unwrap(),
            Err(SpectralOperatorError::ModelMismatch)
        ));
    });
    reads.fail.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(
        matches!(EpochBand::prepare(new(), &model, 0..8), Err(SpectralOperatorError::ModelAccess(crate::ModelLifecycleError::Storage(message))) if message == "injected model read failure")
    );
    let (too_small, _) = generation(63, real_model);
    assert!(matches!(
        EpochBand::prepare(new(), &too_small, 0..8),
        Err(SpectralOperatorError::ModelAccess(_))
    ));
}

#[test]
fn completed_epoch_images_preserve_partitioned_fields_and_model_binding() {
    let (model, _) = generation(64, real_model);
    let output = [1e9, 1.001e9, 1.002e9, 1.003e9];
    let input = Input::new((0..8).map(|ch| 1e9 - 0.25e6 + ch as f64 * 0.7e6).collect());
    let polarization = polarization();
    let complete = |core: Range<usize>, partitioned: bool| {
        let support = if partitioned {
            BandSupport::compile(
                &output,
                core.clone(),
                &input.frequencies,
                [input.frequencies[0], input.frequencies[1]],
            )
            .unwrap()
        } else {
            BandSupport {
                native: 0..8,
                model: (0..4).collect(),
            }
        };
        let plan = BandPlan {
            geometry: geometry(),
            core,
            total_channels: 4,
            fine_per_output: 1,
            single_channel: None,
            phase: BandPhase::Full,
            support,
        };
        let mut job = plan.prepare(&model, None).unwrap();
        let count = job.native_range.len();
        let input_row = input.row(job.native_range.clone());
        let mut row = job
            .workspace
            .begin_row(&input_row, &output, &polarization)
            .unwrap();
        if partitioned {
            for channel in 0..count {
                row.push(channel..channel + 1).unwrap();
            }
        } else {
            row.push(0..count).unwrap();
        }
        row.finish().unwrap();
        let (result, _) = job.complete(&model).unwrap();
        let BandResult::Initial(result) = result else {
            panic!("initial normal required")
        };
        result
    };
    let expected = complete(0..4, false);
    assert!(
        expected
            .cube_real
            .as_ref()
            .unwrap()
            .dirty
            .iter()
            .any(|value| *value != 0.0)
    );
    for depth in [1, 2] {
        for start in (0..4).step_by(depth) {
            let core = start..start + depth;
            let pixels = start * 64..(start + depth) * 64;
            let actual = complete(core.clone(), true);
            let actual_real = actual.cube_real.as_ref().unwrap();
            let expected_real = expected.cube_real.as_ref().unwrap();
            assert_close(
                actual_real
                    .dirty
                    .iter()
                    .map(|&value| Complex64::new(f64::from(value), 0.0)),
                expected_real.dirty[pixels.clone()]
                    .iter()
                    .map(|&value| Complex64::new(f64::from(value), 0.0)),
            );
            assert_close(
                actual_real
                    .psf
                    .iter()
                    .map(|&value| Complex64::new(f64::from(value), 0.0)),
                expected_real.psf[pixels.clone()]
                    .iter()
                    .map(|&value| Complex64::new(f64::from(value), 0.0)),
            );
            assert_eq!(
                actual.sensitivity().iter().collect::<Vec<_>>(),
                expected
                    .sensitivity()
                    .iter()
                    .skip(pixels.start)
                    .take(pixels.len())
                    .collect::<Vec<_>>()
            );
            assert_eq!(actual.sum_weights(), &expected.sum_weights()[core.clone()]);
            assert_eq!(
                actual.published_sum_weights(),
                &expected.published_sum_weights()[core.clone()]
            );
            assert_eq!(
                actual.channel_validity(),
                &expected.channel_validity()[core]
            );
            assert!(
                actual
                    .promote_major_cycle_residual(model.generation_id())
                    .is_ok()
            );
        }
    }
}

#[test]
fn channel_completion_moves_buffers_and_keeps_blank_unmapped_and_shape_checks() {
    let model = crate::ModelGenerationId(LogicalIdentity::from_sha256([37; 32]));
    let dirty = vec![2.0; 3];
    let residual = vec![1.0; 3];
    let psf = vec![3.0; 3];
    let residual_pointer = residual.as_ptr();
    let psf_pointer = psf.as_ptr();
    let images = BandImages {
        phase: BandPhase::Full,
        shape: [1, 1],
        core: 1..4,
        dirty,
        residual,
        psf,
        sum_weight: vec![2.0, 0.0, 0.0],
        mapped: vec![2, 1, 0],
    };
    let completed = SpectralOperatorPrimitives::from_cube_band(images, 4, model).unwrap();
    assert_eq!(
        completed.cube_real.as_ref().unwrap().dirty.as_ptr(),
        residual_pointer
    );
    assert_eq!(
        completed.cube_real.as_ref().unwrap().psf.as_ptr(),
        psf_pointer
    );
    assert_eq!(
        completed.channel_validity(),
        [
            crate::SpectralChannelValidity::Valid,
            crate::SpectralChannelValidity::Blank,
            crate::SpectralChannelValidity::Unmapped
        ]
    );
    let invalid = BandImages {
        phase: BandPhase::Full,
        shape: [1, 1],
        core: 0..1,
        dirty: Vec::new(),
        residual: Vec::new(),
        psf: Vec::new(),
        sum_weight: vec![1.0],
        mapped: vec![1],
    };
    assert!(SpectralOperatorPrimitives::from_cube_band(invalid, 4, model).is_err());
}

#[test]
fn empty_initial_and_residual_refresh_omit_dead_grids_and_do_not_load_prior_arrays() {
    let (empty, reads) = generation_with_origin(
        64,
        None::<fn(usize, usize, usize) -> casa_imaging_model::ModelSample>,
    );
    let (model, _) = generation(64, real_model);
    let output = [1e9, 1.001e9, 1.002e9, 1.003e9];
    let input = Input::new((0..8).map(|ch| 1e9 - 0.25e6 + ch as f64 * 0.7e6).collect());
    let polarization = polarization();
    let new = |phase| {
        BandWorkspace::new(
            geometry(),
            0..4,
            (0..4).collect(),
            PreparedFft::new([10, 10], 7690, 1).unwrap(),
            phase,
            None,
        )
    };
    assert!(matches!(
        EpochBand::prepare(new(BandPhase::InitialZero), &model, 0..8),
        Err(SpectralOperatorError::ReusableNormalStateMismatch)
    ));
    let consume = |job: &mut EpochBand<'_>| {
        let input_row = input.row(0..8);
        let mut row = job
            .workspace
            .begin_row(&input_row, &output, &polarization)
            .unwrap();
        row.push(0..8).unwrap();
        row.finish().unwrap();
    };
    let mut initial = EpochBand::prepare(new(BandPhase::InitialZero), &empty, 0..8).unwrap();
    assert!(
        reads.ranges.lock().unwrap().is_empty(),
        "certified zero is never loaded"
    );
    assert!(initial.workspace.forward.is_empty());
    assert!(initial.workspace.residual.is_empty());
    consume(&mut initial);
    let (initial, _) = initial.complete(&empty).unwrap();
    let BandResult::Initial(initial) = initial else {
        panic!("initial normal required")
    };
    let mut full = EpochBand::prepare(new(BandPhase::Full), &empty, 0..8).unwrap();
    consume(&mut full);
    let (expected, _) = full.complete(&empty).unwrap();
    let BandResult::Initial(expected) = expected else {
        panic!("initial normal required")
    };
    assert_eq!(
        initial.normal_state_content_identity(),
        expected.normal_state_content_identity()
    );
    let original_identity = initial.normal_state_content_identity();
    let mut refresh = EpochBand::prepare(new(BandPhase::Residual), &model, 0..8).unwrap();
    assert!(refresh.workspace.dirty.is_empty());
    assert!(refresh.workspace.psf.is_empty());
    assert!(refresh.workspace.sum_weight.is_empty());
    assert!(refresh.workspace.mapped.is_empty());
    consume(&mut refresh);
    let (actual, _) = refresh.complete(&model).unwrap();
    let BandResult::Residual(actual) = actual else {
        panic!("residual-only result required")
    };
    let mut full = EpochBand::prepare(new(BandPhase::Full), &model, 0..8).unwrap();
    consume(&mut full);
    let (expected, _) = full.complete(&model).unwrap();
    let BandResult::Initial(expected) = expected else {
        panic!("initial normal required")
    };
    for (&actual, expected) in actual.values.iter().zip(expected.dirty()) {
        assert!((f64::from(actual) - expected.re).abs() < 1e-6);
    }
    assert_eq!(actual.model, model.generation_id());
    assert_eq!(
        initial.normal_state_content_identity(),
        original_identity,
        "prior remains unchanged and independently owned"
    );
}

#[test]
fn interpolation_reuse_depends_only_on_admitted_window_and_original_pair() {
    use super::super::input::RowMetadata;
    let output = [1e9, 1.002e9, 1.004e9, 1.006e9];
    let polarization = polarization();
    let raw = model();
    for descending in [false, true] {
        let mut frequencies: Vec<_> = (0..12).map(|ch| 0.996e9 + ch as f64 * 1e6).collect();
        if descending {
            frequencies.reverse();
        }
        let input = Input::new(frequencies.clone());
        let layout = NativeLayout::new(
            input.row(0..12).address,
            input.channels.clone(),
            smallvec::smallvec![
                (0, CorrelationType::CircularRr),
                (1, CorrelationType::CircularLl)
            ],
        )
        .unwrap();
        for selected in [0..12, 2..10] {
            let mut block = NativeBlock::new(8, selected.len(), 2).unwrap();
            let mut row_hz = frequencies.clone();
            let mut pair = [frequencies[0], frequencies[1]];
            for row in 0..8 {
                match row {
                    1 => row_hz[2] += 0.05e6, // Outside the band's window, inside both source windows.
                    2 => row_hz[9] += 0.1e6,
                    3 => row_hz[4] += 0.1e6, // Admitted halo, even if this row narrows its support.
                    5 => row_hz[6] += 0.2e6,
                    6 => pair[1] += 0.1e6,
                    _ => {}
                }
                block.metadata[row] = RowMetadata {
                    physical_row: row as u64,
                    uvw_m: [7.0 + row as f64, -3.0, 0.0],
                    phase_shift_m: 0.017,
                    original_pair_hz: pair,
                };
                let cells = row * selected.len()..(row + 1) * selected.len();
                let samples = cells.start * 2..cells.end * 2;
                let source = selected.start * 2..selected.end * 2;
                block.frequencies_hz[cells].copy_from_slice(&row_hz[selected.clone()]);
                block.values[samples.clone()]
                    .copy_from_slice(&input.values.as_slice().unwrap()[source.clone()]);
                block.weights[samples.clone()]
                    .copy_from_slice(&input.weights.as_slice().unwrap()[source.clone()]);
                block.flags[samples.clone()]
                    .copy_from_slice(&input.flags.as_slice().unwrap()[source.clone()]);
                block.weight_flags[samples]
                    .copy_from_slice(&input.weight_flags.as_slice().unwrap()[source]);
            }
            for phase in [BandPhase::InitialZero, BandPhase::Full, BandPhase::Residual] {
                let make_band = || {
                    let mut band = BandWorkspace::new(
                        geometry(),
                        1..2,
                        (0..4).collect(),
                        PreparedFft::new([10, 10], 7690, 1).unwrap(),
                        phase,
                        None,
                    );
                    if phase != BandPhase::InitialZero {
                        band.prepare_model(raw.view()).unwrap();
                    }
                    band
                };
                let mut actual = make_band();
                actual
                    .consume_block(
                        block.view().unwrap(),
                        &layout,
                        selected.clone(),
                        4..9,
                        &output,
                        &polarization,
                    )
                    .unwrap();
                assert_eq!(
                    actual.stencil_builds, 4,
                    "outside-window changes must not rebuild"
                );
                let mut expected = make_band();
                for row in 0..8 {
                    let row = block
                        .row(&layout, row, selected.clone())
                        .unwrap()
                        .window(4 - selected.start..9 - selected.start)
                        .unwrap();
                    let native = BandSupport::native_window(
                        &output,
                        1..2,
                        row.frequencies_hz,
                        row.original_pair_hz,
                    )
                    .unwrap();
                    let channels = native.len();
                    let row = row.window(native).unwrap();
                    let mut accumulator = expected.begin_row(&row, &output, &polarization).unwrap();
                    accumulator.push(0..channels).unwrap();
                    accumulator.finish().unwrap();
                }
                assert_eq!(actual.dirty, expected.dirty);
                assert_eq!(actual.residual, expected.residual);
                assert_eq!(actual.psf, expected.psf);
                assert_eq!(actual.sum_weight, expected.sum_weight);
                assert_eq!(actual.mapped, expected.mapped);
            }
        }
    }
}

#[test]
fn shared_wide_window_narrows_row_dependent_support_without_copies() {
    use super::super::input::RowMetadata;
    let output = [1e9, 1.002e9, 1.004e9, 1.006e9];
    let inputs: Vec<_> = [-0.6e6, -0.6e6, 0.1e6, 0.6e6, 0.6e6]
        .into_iter()
        .map(|shift| {
            Input::new(
                (0..12)
                    .map(|ch| 0.996e9 + shift + ch as f64 * 1e6)
                    .collect(),
            )
        })
        .collect();
    let layout = NativeLayout::new(
        inputs[0].row(0..12).address,
        inputs[0].channels.clone(),
        smallvec::smallvec![
            (0, CorrelationType::CircularRr),
            (1, CorrelationType::CircularLl)
        ],
    )
    .unwrap();
    let mut block = NativeBlock::new(inputs.len(), 12, 2).unwrap();
    for (r, input) in inputs.iter().enumerate() {
        let row = input.row(0..12);
        block.metadata[r] = RowMetadata {
            physical_row: r as u64,
            uvw_m: row.uvw_m,
            phase_shift_m: row.phase_shift_m,
            original_pair_hz: row.original_pair_hz,
        };
        block.frequencies_hz[r * 12..(r + 1) * 12].copy_from_slice(row.frequencies_hz);
        block.values[r * 24..(r + 1) * 24].copy_from_slice(input.values.as_slice().unwrap());
        block.weights[r * 24..(r + 1) * 24].copy_from_slice(input.weights.as_slice().unwrap());
        block.flags[r * 24..(r + 1) * 24].copy_from_slice(input.flags.as_slice().unwrap());
        block.weight_flags[r * 24..(r + 1) * 24]
            .copy_from_slice(input.weight_flags.as_slice().unwrap());
    }
    let narrow = block.row(&layout, 1, 0..12).unwrap().window(3..7).unwrap();
    assert_eq!(narrow.values.as_ptr(), block.values[30..].as_ptr());
    assert_eq!(narrow.weights.as_ptr(), block.weights[30..].as_ptr());
    assert_eq!(
        narrow.frequencies_hz.as_ptr(),
        block.frequencies_hz[15..].as_ptr()
    );
    assert_eq!(narrow.original_pair_hz, block.metadata[1].original_pair_hz);
    assert!(block.row(&layout, 0, 0..12).unwrap().window(0..0).is_err());
    let polarization = polarization();
    let raw = model();
    let mut linear = workspace(0..4, (0..4).collect(), &raw);
    assert!(
        linear
            .begin_row(
                &block.row(&layout, 0, 0..12).unwrap().window(0..1).unwrap(),
                &output,
                &polarization
            )
            .is_err(),
        "multi-plane interpolation still requires a native pair"
    );
    for depth in [1, 2, 4] {
        for start in (0..4).step_by(depth) {
            let core = start..start + depth;
            let mut support = BandSupport {
                native: 0..0,
                model: Vec::new(),
            };
            for input in &inputs {
                support.include(
                    BandSupport::compile(
                        &output,
                        core.clone(),
                        &input.frequencies,
                        [input.frequencies[0], input.frequencies[1]],
                    )
                    .unwrap(),
                );
            }
            let mut actual = workspace(core.clone(), support.model.clone(), &raw);
            actual
                .consume_block(
                    block.view().unwrap(),
                    &layout,
                    0..12,
                    support.native.clone(),
                    &output,
                    &polarization,
                )
                .unwrap();
            let mut expected = workspace(core.clone(), support.model, &raw);
            for input in &inputs {
                let native = BandSupport::compile(
                    &output,
                    core.clone(),
                    &input.frequencies,
                    [input.frequencies[0], input.frequencies[1]],
                )
                .unwrap()
                .native;
                assert!(native.start >= support.native.start && native.end <= support.native.end);
                let count = native.len();
                let input_row = input.row(native);
                let mut row = expected
                    .begin_row(&input_row, &output, &polarization)
                    .unwrap();
                row.push(0..count).unwrap();
                row.finish().unwrap();
            }
            assert_eq!(actual.dirty, expected.dirty);
            assert_eq!(actual.residual, expected.residual);
            assert_eq!(actual.psf, expected.psf);
            assert_eq!(actual.sum_weight, expected.sum_weight);
            assert_eq!(actual.mapped, expected.mapped);
        }
    }
}
