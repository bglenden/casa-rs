// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::MuellerMatrix;
use casa_imaging_model::{
    CorrelationType, DirectionCoordinateSpec, DirectionFrame, FrequencyFrame, LogicalIdentity,
    MeasurementSetIdentity, Projection, SkyDirection,
};
use ndarray::{Array2, s};

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
    values: Array2<Complex64>,
    weights: Array2<f64>,
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
                Complex64::new(0.25 + ch as f64 * 0.12, -0.8 + corr as f64 * 0.2)
            }),
            weights: Array2::from_shape_fn((count, 2), |(ch, corr)| {
                if ch == 2 {
                    0.0
                } else {
                    0.3 + ch as f64 * 0.17 + corr as f64 * 0.11
                }
            }),
            flags: Array2::from_shape_fn((count, 2), |(ch, corr)| ch == 3 && corr == 1),
            weight_flags: Array2::from_shape_fn((count, 2), |(ch, _)| ch == 5),
        }
    }

    fn row(&self, native: Range<usize>) -> VisibilityRow<'_> {
        let frequency = self.frequencies[0];
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
            values: self.values.slice(s![native.clone(), ..]),
            weights: self.weights.slice(s![native.clone(), ..]),
            flags: self.flags.slice(s![native.clone(), ..]),
            weight_flags: self.weight_flags.slice(s![native, ..]),
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
        PreparedFft::new([10, 10], 7690).unwrap(),
        BandPhase::Full,
    );
    band.prepare_model(model.view()).unwrap();
    band
}

fn full_band(input: &Input, output: &[f64], model: &Array3<Complex64>) -> BandWorkspace {
    let mut band = workspace(0..4, (0..4).collect(), model);
    let polarization = polarization();
    let mut row = band
        .begin_row(input.row(0..input.channels.len()), output, &polarization)
        .unwrap();
    row.push(0..input.channels.len()).unwrap();
    row.finish().unwrap();
    band
}

fn assert_bits(
    actual: impl IntoIterator<Item = Complex64>,
    expected: impl IntoIterator<Item = Complex64>,
) {
    let bits = |values: Vec<Complex64>| {
        values
            .into_iter()
            .map(|value| [value.re.to_bits(), value.im.to_bits()])
            .collect::<Vec<_>>()
    };
    assert_eq!(
        bits(actual.into_iter().collect()),
        bits(expected.into_iter().collect())
    );
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
        assert_bits(
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
        assert_bits(
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
            .all(|value| *value == Complex64::default())
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
                            assert_bits(predicted, expected);
                        }
                        let mut row = band
                            .begin_row(input.row(support.native), &output, &polarization)
                            .unwrap();
                        // End a chunk inside the row, then resume the exact cursor.
                        row.push(0..1).unwrap();
                        row.push(1..count).unwrap();
                        row.finish().unwrap();
                        for (local, global) in core.enumerate() {
                            assert_bits(
                                band.dirty.index_axis(Axis(0), local).iter().copied(),
                                reference.dirty.index_axis(Axis(0), global).iter().copied(),
                            );
                            assert_bits(
                                band.residual.index_axis(Axis(0), local).iter().copied(),
                                reference
                                    .residual
                                    .index_axis(Axis(0), global)
                                    .iter()
                                    .copied(),
                            );
                            assert_bits(
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
        let mut row = band
            .begin_row(input.row(support.native.clone()), &output, &polarization)
            .unwrap();
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
    assert_bits(
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
    let mut band = workspace(0..4, vec![0, 1, 2, 3], &model());
    let allocation = band.forward.as_ptr();
    band.prepare_model(model().view()).unwrap();
    assert_eq!(band.forward.as_ptr(), allocation);
    let mut row = band
        .begin_row(input.row(0..4), &output, &polarization)
        .unwrap();
    row.push(0..2).unwrap();
    assert!(row.push(3..4).is_err());
    assert!(matches!(
        row.finish(),
        Err(SpectralOperatorError::IncompleteCoverage)
    ));
    assert!(matches!(
        band.begin_row(input.row(0..4), &output, &polarization)
            .unwrap()
            .finish(),
        Err(SpectralOperatorError::IncompleteCoverage)
    ));
    let (mut first, mut second) = band.dirty.view_mut().split_at(Axis(0), 2);
    first.fill(Complex64::new(1.0, 0.0));
    second.fill(Complex64::new(2.0, 0.0));
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
    let model = model();
    let mut expected = workspace(0..4, (0..4).collect(), &model);
    let mut actual = workspace(0..4, (0..4).collect(), &model);
    let polarization = polarization();
    for (workspace, row) in [(&mut expected, original), (&mut actual, row)] {
        let mut accumulator = workspace.begin_row(row, &output, &polarization).unwrap();
        accumulator.push(0..6).unwrap();
        accumulator.finish().unwrap();
    }
    assert_eq!(actual.dirty, expected.dirty);
    assert_eq!(actual.residual, expected.residual);
    assert_eq!(actual.psf, expected.psf);
    assert_eq!(actual.sum_weight, expected.sum_weight);
    assert_eq!(actual.mapped, expected.mapped);
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
    samples: Box<[casa_imaging_model::ModelSample]>,
    reads: std::sync::Arc<ModelReads>,
}

impl crate::ModelSampleStorage for ObservedModelStorage {
    fn sample_count(&self) -> usize {
        self.samples.len()
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
        destination.copy_from_slice(&self.samples[start..start + destination.len()]);
        Ok(())
    }
    fn write(
        &mut self,
        start: usize,
        samples: &[casa_imaging_model::ModelSample],
    ) -> Result<(), crate::ModelLifecycleError> {
        self.samples[start..start + samples.len()].copy_from_slice(samples);
        Ok(())
    }
}

impl crate::ModelStorageFactory for std::sync::Arc<ModelReads> {
    fn create(
        &self,
        count: usize,
    ) -> Result<Box<dyn crate::ModelSampleStorage>, crate::ModelLifecycleError> {
        Ok(Box::new(ObservedModelStorage {
            samples: vec![casa_imaging_model::ModelSample::invalid(); count].into(),
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
                phase,
                support: BandSupport {
                    native: 0..6,
                    model: (0..4).collect(),
                },
            };
            let memory = plan.memory().unwrap();
            let generation = if phase == BandPhase::InitialZero {
                &empty
            } else {
                &model
            };
            let job = plan.clone().prepare(generation, None).unwrap();
            assert_eq!(job.native_range, plan.native_range());
            let w = &job.workspace;
            let grids = [
                &w.forward,
                &w.dirty,
                &w.dirty_error,
                &w.residual,
                &w.residual_error,
                &w.psf,
                &w.psf_error,
            ];
            let payload = grids
                .iter()
                .map(|grid| grid.len() * size_of::<Complex64>())
                .sum::<usize>()
                + (w.sum_weight.capacity() + w.sum_weight_error.capacity()) * size_of::<f64>()
                + w.mapped.capacity() * size_of::<u64>()
                + w.model_channels.capacity() * size_of::<usize>()
                + w.forward_nonzero.capacity();
            let fft = fft_resident_complex_values_for_shape(geometry().grid_shape).unwrap()
                * size_of::<Complex64>();
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
                    + size_of::<(BandResult, PreparedFft)>()
                    + fft
            );
            let refresh = plan.residual_refresh();
            let memory = refresh.memory().unwrap();
            let residual_image = depth * 64 * size_of::<Complex64>();
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
            assert_eq!(
                updated.values.len() * size_of::<Complex64>(),
                residual_image
            );
            assert_eq!(
                memory.retained_bytes,
                residual_image + size_of::<(BandResult, PreparedFft)>() + fft
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
                    >= 4 * depth * grid * grid * std::mem::size_of::<Complex64>()
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
        PreparedFft::new([10, 10], 7690).unwrap(),
        BandPhase::Full,
    );
    let job = EpochBand::prepare(owned, &model, 0..8).unwrap();
    assert_bits(
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
        PreparedFft::new([10, 10], 7690).unwrap(),
        BandPhase::Full,
    );
    let mut job = EpochBand::prepare(prepared, &model, support.native.clone()).unwrap();
    let mut expected = workspace(1..2, support.model, &raw);
    let polarization = polarization();
    for band in [&mut job.workspace, &mut expected] {
        let mut row = band
            .begin_row(input.row(support.native.clone()), &output, &polarization)
            .unwrap();
        row.push(0..support.native.len()).unwrap();
        row.finish().unwrap();
    }
    assert_bits(
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
            PreparedFft::new([10, 10], 7690).unwrap(),
            BandPhase::Full,
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
            phase: BandPhase::Full,
            support,
        };
        let mut job = plan.prepare(&model, None).unwrap();
        let count = job.native_range.len();
        let mut row = job
            .workspace
            .begin_row(input.row(job.native_range.clone()), &output, &polarization)
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
    assert!(expected.dirty().iter().any(|value| value.norm() > 0.0));
    for depth in [1, 2] {
        for start in (0..4).step_by(depth) {
            let core = start..start + depth;
            let pixels = start * 64..(start + depth) * 64;
            let actual = complete(core.clone(), true);
            assert_bits(
                actual.dirty().iter().copied(),
                expected.dirty()[pixels.clone()].iter().copied(),
            );
            assert_bits(
                actual.psf().iter().copied(),
                expected.psf()[pixels.clone()].iter().copied(),
            );
            assert_eq!(actual.sensitivity(), &expected.sensitivity()[pixels]);
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
    let dirty = vec![Complex64::new(2.0, 0.0); 3];
    let residual = vec![Complex64::new(1.0, 0.0); 3];
    let psf = vec![Complex64::new(3.0, 0.0); 3];
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
    assert_eq!(completed.dirty().as_ptr(), residual_pointer);
    assert_eq!(completed.psf().as_ptr(), psf_pointer);
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
            PreparedFft::new([10, 10], 7690).unwrap(),
            phase,
        )
    };
    assert!(matches!(
        EpochBand::prepare(new(BandPhase::InitialZero), &model, 0..8),
        Err(SpectralOperatorError::ReusableNormalStateMismatch)
    ));
    let consume = |job: &mut EpochBand<'_>| {
        let mut row = job
            .workspace
            .begin_row(input.row(0..8), &output, &polarization)
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
    assert!(initial.workspace.residual_error.is_empty());
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
    assert!(refresh.workspace.dirty_error.is_empty());
    assert!(refresh.workspace.psf.is_empty());
    assert!(refresh.workspace.psf_error.is_empty());
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
    assert_eq!(actual.values.as_ref(), expected.dirty());
    assert_eq!(actual.model, model.generation_id());
    assert_eq!(
        initial.normal_state_content_identity(),
        original_identity,
        "prior remains unchanged and independently owned"
    );
}

#[test]
fn shared_wide_window_narrows_row_dependent_support_without_copies() {
    use super::super::input::RowMetadata;
    let output = [1e9, 1.002e9, 1.004e9, 1.006e9];
    let inputs: Vec<_> = [-0.6e6, 0.1e6, 0.6e6]
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
    let mut block = NativeBlock::new(3, 12, 2).unwrap();
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
    assert!(block.row(&layout, 0, 0..12).unwrap().window(0..1).is_err());
    let polarization = polarization();
    let raw = model();
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
                    &block,
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
                let mut row = expected
                    .begin_row(input.row(native), &output, &polarization)
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
