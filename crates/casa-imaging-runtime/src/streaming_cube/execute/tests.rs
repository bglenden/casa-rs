// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::managed_spill::tests::test_authority;
use crate::streaming_cube::input::{NativeStoreWriter, StorePlan};
use casa_imaging_model::*;
use casa_imaging_reconstruction::{
    ExecutableModelProblem, MajorCycleOwner, MajorCyclePreparation, ModelLifecycle,
    ModelStoragePlan, MuellerMatrix, SpectralChannelValidity, SpectralOperatorSpecification,
    WeightingExecutionLimits, WeightingReplaySummary, plan_weighting,
    runtime_adapter::{
        CompleteDataOwnerResult, CompleteDataOwnerSlabFold, NativeWeightingPreparation,
        NormalStoragePlan, RowMetadata, SpectralOperatorPass, WeightingSpectralCache,
    },
};
use num_complex::Complex64;

#[path = "../../../../casa-imaging-reconstruction/tests/support/streaming_cube.rs"]
mod fixture;

const OUTPUT: [f64; 4] = [1e9, 1.001e9, 1.002e9, 1.003e9];

fn native_input(
    problem: &CompiledProblem,
    mut consume: impl FnMut(u64, &[&NativeBlock], &NativeLayout) -> io::Result<()>,
) -> (WeightingReplaySummary, SelectedObservationGenerationId) {
    let plan = plan_weighting(problem, WeightingExecutionLimits::new(7, 1).unwrap()).unwrap();
    let mut coordinator = NativeWeightingPreparation::new(problem, &plan).unwrap();
    let samples = fixture::selected_samples(problem);
    let layout =
        NativeLayout::new(samples[0].address, (0..6).collect(), layout().correlations).unwrap();
    let mut workers: Vec<_> = (0..2)
        .map(|_| {
            (
                coordinator
                    .worker(
                        problem,
                        &plan,
                        layout.clone(),
                        NativeBlock::new(1, 6, 2).unwrap(),
                        FiniteValuePolicy::FlagInputRejectGenerated,
                    )
                    .unwrap(),
                WeightingSpectralCache::new(problem).unwrap(),
            )
        })
        .collect();
    let mut visited = 0;
    let mut ordinal = 0;
    let (selected, count) = problem
        .inspect_selected_observation(samples.into_iter().map(Ok::<_, io::Error>), |sample| {
            let worker_index = (visited / 12) % workers.len();
            let (worker, cache) = &mut workers[worker_index];
            if visited % 12 == 0 {
                worker.begin_batch()?;
            }
            let address = sample.address;
            let group =
                SelectedInputWeightGroup::parallel_hands(sample.input_weight, sample.input_weight)
                    .with_density_owner(address.correlation_index == 0)
                    .with_terminal_member(address.correlation_index == 1)
                    .with_imaging_flag(address.channel_index == 3);
            let row = SelectedRowSpectralGeometry::new(
                sample.as_view(),
                FrequencyFrame::Topocentric,
                6,
                (0, 0.999e9),
                Some((1, 1e9)),
            )
            .unwrap();
            let view = sample
                .as_view()
                .with_input_weight_group(group)
                .with_row_spectral_geometry(Some(row));
            let interval = SelectedSpectralInterval::new(
                address.frequency_centre_hz,
                address.frequency_lower_hz,
                address.frequency_upper_hz,
            )
            .unwrap();
            let evaluation = SelectedSpectralEvaluation::new(
                interval,
                interval,
                f64::from(sample.input_weight),
                !sample.channel_flag,
            )
            .unwrap()
            .with_row_geometry(row);
            let contributions = cache.compile(view, evaluation).map_err(io::Error::other)?;
            worker.consume(problem, view, address.frequency_centre_hz, contributions)?;
            visited += 1;
            if visited % 12 == 0 {
                worker.finish_batch()?;
                coordinator.commit(worker)?;
                if worker_index + 1 == workers.len() || visited == 60 {
                    let parts: Vec<_> = workers[..=worker_index]
                        .iter()
                        .map(|(worker, _)| worker.block())
                        .collect();
                    consume(ordinal, &parts, workers[0].0.layout())?;
                    ordinal += 1;
                }
            }
            Ok(())
        })
        .unwrap();
    let (_, replay) = coordinator.finish().unwrap();
    assert_eq!(count, 60);
    assert_eq!(replay.sample_count(), count);
    assert_eq!(ordinal, 3);
    (replay, selected)
}

#[test]
fn native_preparation_and_real_bands_feed_the_existing_fold_and_controller() {
    use crate::streaming_cube::prepare::{NativePreparation, PreparedNative};
    let problem = fixture::problem(SpectralSamplingLaw::LINEAR);
    let specifications: Vec<_> = (0..4)
        .map(|channel| SpectralOperatorSpecification::for_slab(&problem, channel, 1).unwrap())
        .collect();
    let bands = specifications
        .iter()
        .map(|spec| BandPlan::new(spec, SpectralOperatorPass::InitialMajor).unwrap())
        .collect();
    let budget = size_of::<NativeBlock>() + size_of::<Vec<u8>>() + 4 + 2 * (56 + 6 * 60 + 2 * 60);
    let plan = StorePlan::new(5, 6, 2, 2, budget, u64::MAX).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let (_, storage) = test_authority(directory.path(), plan.artifact_bytes);
    let mut input = NativePreparation::new(&storage, plan, &problem, &OUTPUT, bands).unwrap();
    let mut first_address = None;
    let (replay, selected) = native_input(&problem, |_, parts, layout| {
        first_address.get_or_insert(layout.address);
        input.consume(parts, layout)
    });
    let PreparedNative {
        mut store,
        bands,
        layout,
    } = input.finish(&replay).unwrap();
    assert_eq!(Some(layout.address), first_address);
    assert_eq!(layout.channels, [0, 1, 2, 3, 4, 5]);
    let mut lifecycle = lifecycle(&problem, 64);
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, lifecycle.initial_empty().unwrap(), None)
            .unwrap();
    let expected_model = preparation.final_model_generation();
    let mut measurements = None;
    let result = execute(
        &mut store,
        initial_jobs(&bands),
        preparation.final_model(),
        &layout,
        &OUTPUT,
        &polarization(),
        4,
        2,
        4096,
        4 << 30,
        0,
        &mut measurements,
    )
    .unwrap();
    let mut fold: Option<CompleteDataOwnerSlabFold> = None;
    for ((normal, _), specification) in result.bands.into_iter().zip(&specifications) {
        let pointers = (
            normal.dirty().as_ptr(),
            normal.psf().as_ptr(),
            normal.sensitivity().as_ptr(),
        );
        let band = CompleteDataOwnerResult::from_streaming_cube(
            specification,
            normal,
            &replay,
            selected,
            None,
        )
        .unwrap();
        assert_eq!(
            pointers,
            (
                band.primitives().dirty().as_ptr(),
                band.primitives().psf().as_ptr(),
                band.primitives().sensitivity().as_ptr()
            )
        );
        assert_eq!(band.completion().coverage(), replay.coverage());
        assert_eq!(band.completion().sample_count(), 60);
        assert_eq!(band.completion().coverage_proof_bytes(), 0);
        assert_eq!(band.completion().coverage_proof_hash_calls(), 0);
        fold = Some(match fold {
            None => {
                CompleteDataOwnerSlabFold::begin(band, &NormalStoragePlan::resident(1).unwrap())
                    .unwrap()
            }
            Some(prefix) => prefix.extend(band).unwrap(),
        });
    }
    let normal = fold.unwrap().finish().unwrap();
    assert_eq!(normal.completion().selected_generation(), selected);
    for channel in 0..4 {
        let window = normal.read_window(channel..channel + 1).unwrap();
        assert_eq!(
            window.primitives().slab().core_range(),
            channel..channel + 1
        );
        assert_eq!(
            window
                .primitives()
                .dirty()
                .iter()
                .any(|value| value.norm() > 0.0),
            window.primitives().channel_validity()[0] == SpectralChannelValidity::Valid
        );
    }
    let complete = MajorCycleOwner::from_complete_data(normal, preparation)
        .unwrap()
        .reconcile(&mut lifecycle)
        .unwrap();
    assert_eq!(complete.final_model().generation_id(), expected_model);
}

#[test]
fn native_preparation_rejects_missing_duplicate_and_out_of_order_row_parts() {
    use crate::streaming_cube::prepare::NativePreparation;
    let problem = fixture::problem(SpectralSamplingLaw::LINEAR);
    let budget = size_of::<NativeBlock>() + size_of::<Vec<u8>>() + 4 + 2 * (56 + 6 * 60 + 2 * 60);
    let plan = StorePlan::new(5, 6, 2, 2, budget, u64::MAX).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let (_, storage) = test_authority(directory.path(), plan.artifact_bytes);
    let create = || {
        NativePreparation::new(
            &storage,
            plan,
            &problem,
            &OUTPUT,
            vec![
                BandPlan::new(
                    &SpectralOperatorSpecification::new(&problem).unwrap(),
                    SpectralOperatorPass::InitialMajor,
                )
                .unwrap(),
            ],
        )
        .unwrap()
    };
    let mut missing = create();
    let (replay, _) = native_input(&problem, |ordinal, parts, layout| {
        if ordinal == 0 {
            missing.consume(parts, layout)?;
        }
        Ok(())
    });
    assert!(missing.finish(&replay).is_err());
    for duplicate in [true, false] {
        let mut input = create();
        let mut exercised = false;
        let (replay, _) = native_input(&problem, |ordinal, parts, layout| {
            if ordinal == 0 {
                if duplicate {
                    input.consume(parts, layout)?;
                    assert!(input.consume(parts, layout).is_err());
                } else {
                    let reversed: Vec<_> = parts.iter().copied().rev().collect();
                    assert!(input.consume(&reversed, layout).is_err());
                }
                assert!(input.consume(parts, layout).is_err());
                exercised = true;
            }
            Ok(())
        });
        assert!(exercised);
        assert!(input.finish(&replay).is_err());
    }
}

#[test]
fn native_preparation_rejects_shape_or_frequency_mismatch_before_creating_storage() {
    use crate::streaming_cube::prepare::NativePreparation;
    let problem = fixture::problem(SpectralSamplingLaw::LINEAR);
    let directory = tempfile::tempdir().unwrap();
    let (_, storage) = test_authority(directory.path(), 1 << 20);
    let bands = || {
        vec![
            BandPlan::new(
                &SpectralOperatorSpecification::new(&problem).unwrap(),
                SpectralOperatorPass::InitialMajor,
            )
            .unwrap(),
        ]
    };
    for (rows, channels, correlations) in [(4, 6, 2), (5, 5, 2), (5, 6, 1)] {
        let plan = StorePlan::new(rows, channels, correlations, 2, 4096, u64::MAX).unwrap();
        assert!(NativePreparation::new(&storage, plan, &problem, &OUTPUT, bands()).is_err());
    }
    let plan = StorePlan::new(5, 6, 2, 2, 4096, u64::MAX).unwrap();
    let mut changed = OUTPUT;
    changed[2] += 1.0;
    assert!(NativePreparation::new(&storage, plan, &problem, &changed, bands()).is_err());
    assert!(NativePreparation::new(&storage, plan, &problem, &OUTPUT[..3], bands()).is_err());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

fn layout() -> NativeLayout {
    NativeLayout::new(
        SelectedSampleAddress {
            measurement_set: MeasurementSetIdentity::new(LogicalIdentity::from_sha256([1; 32])),
            physical_row: 0,
            data_description_id: 0,
            spectral_window_id: 0,
            channel_index: 0,
            frequency_centre_hz: 1e9,
            frequency_lower_hz: 0.9995e9,
            frequency_upper_hz: 1.0005e9,
            channel_width_hz: 1e6,
            frequency_frame: FrequencyFrame::Topocentric,
            polarization_id: 0,
            correlation_index: 0,
            correlation_type: CorrelationType::CircularRr,
        },
        (0..6).collect(),
        [
            (0, CorrelationType::CircularRr),
            (1, CorrelationType::CircularLl),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap()
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

fn lifecycle(problem: &CompiledProblem, window: usize) -> ModelLifecycle {
    ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).unwrap(),
        ModelExecutionAttemptId::new(LogicalIdentity::from_sha256([81; 32])),
        1,
        ModelStoragePlan::resident(window).unwrap(),
    )
    .unwrap()
}

fn changed_model(owner: &ModelLifecycle, base: ModelGeneration) -> ModelGeneration {
    let delta = owner
        .compile_delta(
            &base,
            (0..4).map(|channel| {
                ModelDeltaTerm::new(
                    ModelCell::new(0, channel, 0, [3 + channel % 2, 4]),
                    ModelValue::new(0.17 + 0.3 * channel as f64).unwrap(),
                )
            }),
        )
        .unwrap();
    owner.apply_delta(base, delta).unwrap()
}

fn input(
    depth: usize,
    unmapped: bool,
) -> (
    tempfile::TempDir,
    NativeStore,
    Vec<BandPlan>,
    CompiledProblem,
) {
    let problem = fixture::problem(SpectralSamplingLaw::LINEAR);
    let mut bands: Vec<_> = (0..4)
        .step_by(depth)
        .map(|start| {
            BandPlan::new(
                &SpectralOperatorSpecification::for_slab(&problem, start, depth.min(4 - start))
                    .unwrap(),
                SpectralOperatorPass::InitialMajor,
            )
            .unwrap()
        })
        .collect();
    // Two rows per block, with a short final row block and native-channel tile.
    let budget = size_of::<NativeBlock>() + size_of::<Vec<u8>>() + 4 + 2 * (56 + 6 * 60 + 2 * 60);
    let plan = StorePlan::new(5, 6, 2, 2, budget, u64::MAX).unwrap();
    assert_eq!(plan.block_rows, 2);
    let directory = tempfile::tempdir().unwrap();
    let (_, storage) = test_authority(directory.path(), plan.artifact_bytes);
    let mut writer = NativeStoreWriter::create(&storage, plan).unwrap();
    let mut block = NativeBlock::new(2, 6, 2).unwrap();
    for ordinal in 0..plan.blocks() {
        block.set_shape(plan.rows_in(ordinal).unwrap(), 6).unwrap();
        for row in 0..block.metadata.len() {
            let absolute = ordinal * 2 + row as u64;
            let start = if unmapped {
                2e9
            } else {
                0.999e9 + 0.125e6 * absolute as f64
            };
            block.metadata[row] = RowMetadata {
                physical_row: absolute,
                uvw_m: [2.0 + absolute as f64, -3.0, 0.0],
                phase_shift_m: 0.017,
                original_pair_hz: [start, start + 1e6],
            };
            for channel in 0..6 {
                let cell = row * 6 + channel;
                block.frequencies_hz[cell] = start + channel as f64 * 1e6;
                for corr in 0..2 {
                    let sample = cell * 2 + corr;
                    block.values[sample] =
                        Complex64::new(0.25 + channel as f64 * 0.12, -0.8 + corr as f64 * 0.2);
                    block.weights[sample] = if channel == 2 {
                        0.0
                    } else {
                        0.3 + channel as f64 * 0.17 + corr as f64 * 0.11
                    };
                    block.flags[sample] = channel == 3 && corr == 1;
                    block.weight_flags[sample] = channel == 5;
                }
            }
        }
        for band in &mut bands {
            band.observe(&block, &OUTPUT).unwrap();
        }
        writer.append(&block).unwrap();
    }
    (directory, writer.finish().unwrap(), bands, problem)
}

fn initial_jobs(plans: &[BandPlan]) -> Vec<BandInput> {
    plans
        .iter()
        .map(|plan| BandInput {
            plan: plan.clone(),
            prior: None,
            fft: None,
        })
        .collect()
}

fn run(
    store: &mut NativeStore,
    jobs: Vec<BandInput>,
    model: &ModelGeneration,
    workers: usize,
    slots: usize,
) -> (WaveResult, BoundedStreamMeasurements) {
    let mut measured = None;
    let result = execute(
        store,
        jobs,
        model,
        &layout(),
        &OUTPUT,
        &polarization(),
        workers,
        slots,
        4096,
        4 << 30,
        0,
        &mut measured,
    )
    .unwrap();
    (result, measured.unwrap())
}

fn assert_same(
    expected: &SpectralOperatorPrimitives,
    bands: &[(SpectralOperatorPrimitives, PreparedFft)],
) {
    for (band, _) in bands {
        let core = band.slab().core_range();
        let pixels = core.start * 64..core.end * 64;
        assert_eq!(band.dirty(), &expected.dirty()[pixels.clone()]);
        assert_eq!(band.psf(), &expected.psf()[pixels.clone()]);
        assert_eq!(band.sensitivity(), &expected.sensitivity()[pixels]);
        assert_eq!(band.sum_weights(), &expected.sum_weights()[core.clone()]);
        assert_eq!(
            band.published_sum_weights(),
            &expected.published_sum_weights()[core.clone()]
        );
        assert_eq!(band.channel_validity(), &expected.channel_validity()[core]);
    }
}

#[test]
fn complete_band_jobs_match_across_workers_and_epochs_without_row_block_dispatch() {
    let (_dir, mut store, whole, problem) = input(4, false);
    let owner = lifecycle(&problem, 64);
    let empty = owner.initial_empty().unwrap();
    let (initial_reference, _) = run(&mut store, initial_jobs(&whole), &empty, 1, 1);
    let (mut initial, _) = run(&mut store, initial_jobs(&whole), &empty, 1, 1);
    let model = changed_model(&owner, empty);
    let (full_prior, fft) = initial.bands.pop().unwrap();
    let (expected, _) = run(
        &mut store,
        vec![BandInput {
            plan: whole[0].residual_refresh(),
            prior: Some(full_prior),
            fft: Some(fft),
        }],
        &model,
        1,
        1,
    );
    for depth in [1, 2, 4] {
        for workers in [1, 2, 4] {
            for slots in [1, 2] {
                let (_dir, mut store, plans, problem) = input(depth, false);
                let owner = lifecycle(&problem, 64);
                let empty = owner.initial_empty().unwrap();
                let (initial, measurements) =
                    run(&mut store, initial_jobs(&plans), &empty, workers, slots);
                assert_same(&initial_reference.bands[0].0, &initial.bands);
                assert_eq!(measurements.logical_units_filled, 0);
                assert_eq!(measurements.blocks_filled, 0);
                assert_eq!(
                    measurements.worker_threads_started,
                    if workers.min(plans.len()) == 1 {
                        0
                    } else {
                        workers.min(plans.len()) as u64
                    }
                );
                assert_eq!(measurements.partitions_executed, plans.len() as u64);
                assert!(
                    measurements.peak_live_source_capacity_bytes
                        <= measurements.planned_source_capacity_bytes
                );
                // These are complete resident jobs, not a producer stream.
                // Actual native I/O is measured by the shared file reader.
                assert_eq!(measurements.logical_source_bytes, 0);
                assert_eq!(measurements.source_read_operations, 0);
                assert!(initial.source.operations > 0);
                assert!(
                    initial.source.operations <= 12 * plans.len() as u64,
                    "each band reads at most one metadata and three tile frames per block"
                );
                let model = changed_model(&owner, empty);
                let pointers: Vec<_> = initial
                    .bands
                    .iter()
                    .map(|(normal, _)| (normal.psf().as_ptr(), normal.sensitivity().as_ptr()))
                    .collect();
                let jobs = plans
                    .iter()
                    .zip(initial.bands)
                    .map(|(plan, (normal, fft))| BandInput {
                        plan: plan.residual_refresh(),
                        prior: Some(normal),
                        fft: Some(fft),
                    })
                    .collect();
                let (refreshed, _) = run(&mut store, jobs, &model, workers, slots);
                assert_same(&expected.bands[0].0, &refreshed.bands);
                for ((normal, _), (psf, sensitivity)) in refreshed.bands.iter().zip(pointers) {
                    assert_eq!(normal.psf().as_ptr(), psf);
                    assert_eq!(normal.sensitivity().as_ptr(), sensitivity);
                }
            }
        }
    }
}

#[test]
fn native_read_failure_joins_complete_band_jobs_without_returning_products() {
    let (directory, mut store, bands, problem) = input(1, false);
    let empty = lifecycle(&problem, 64).initial_empty().unwrap();
    let path = std::fs::read_dir(directory.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(0)
        .unwrap();
    let mut measured = None;
    let error = execute(
        &mut store,
        initial_jobs(&bands),
        &empty,
        &layout(),
        &OUTPUT,
        &polarization(),
        4,
        2,
        4096,
        4 << 30,
        0,
        &mut measured,
    )
    .err()
    .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(measured.unwrap().worker_threads_started, 4);
}

#[test]
fn wave_admission_counts_all_jobs_and_rejects_before_work() {
    let (_dir, store, bands, _) = input(1, false);
    let jobs = initial_jobs(&bands);
    for workers in [1, 2, 4] {
        for slots in [1, 2] {
            let plan = WavePlan::new(&store, &jobs, workers, slots, 4096, u64::MAX).unwrap();
            assert!(WavePlan::new(&store, &jobs, workers, slots, 4096, plan.peak_bytes).is_ok());
            assert!(
                WavePlan::new(&store, &jobs, workers, slots, 4096, plan.peak_bytes - 1).is_err()
            );
            assert_eq!(plan.job_bytes.len(), 4);
            let dynamic: u64 = jobs
                .iter()
                .map(|job| job.plan.memory(None).unwrap().peak_bytes() as u64)
                .sum();
            assert!(plan.peak_bytes > dynamic);
        }
    }
    assert!(WavePlan::new(&store, &jobs, 0, 1, 0, u64::MAX).is_err());
    assert!(WavePlan::new(&store, &jobs, 4, 3, 0, u64::MAX).is_err());
    assert!(WavePlan::new(&store, &jobs, 4, 1, u64::MAX, u64::MAX).is_err());
    let mut reversed = jobs;
    reversed.reverse();
    assert!(WavePlan::new(&store, &reversed, 4, 1, 0, u64::MAX).is_err());
}

#[test]
fn initial_wave_selection_uses_shape_budget_and_drains_before_next_wave() {
    let (_dir, mut store, bands, problem) = input(1, false);
    let empty = lifecycle(&problem, 64).initial_empty().unwrap();
    let (expected, _) = run(&mut store, initial_jobs(&bands), &empty, 1, 1);
    for workers in [1, 2, 4] {
        for slots in [1, 2] {
            for count in 1..=bands.len() {
                let budget = WavePlan::project(
                    store.plan,
                    bands[..count].iter().map(|band| (band, None)),
                    workers,
                    slots,
                    4096,
                )
                .unwrap()
                .peak_bytes;
                assert_eq!(
                    WavePlan::initial_prefix(store.plan, &bands, workers, slots, 4096, budget,)
                        .unwrap(),
                    count
                );
                let below =
                    WavePlan::initial_prefix(store.plan, &bands, workers, slots, 4096, budget - 1);
                if count == 1 {
                    assert!(below.is_err());
                } else {
                    assert!(below.unwrap() < count);
                }
                // Edge bands have different support capacities. The boundary
                // check above uses the original owners, not Vec clones whose
                // spare capacity can shrink. Every later band must also fit.
                let budget = bands.iter().fold(budget, |budget, band| {
                    budget.max(
                        WavePlan::project(
                            store.plan,
                            std::iter::once((band, None)),
                            workers,
                            slots,
                            4096,
                        )
                        .unwrap()
                        .peak_bytes,
                    )
                });
                let mut start = 0;
                while start < bands.len() {
                    let depth = WavePlan::initial_prefix(
                        store.plan,
                        &bands[start..],
                        workers,
                        slots,
                        4096,
                        budget,
                    )
                    .unwrap();
                    let wave = execute(
                        &mut store,
                        initial_jobs(&bands[start..start + depth]),
                        &empty,
                        &layout(),
                        &OUTPUT,
                        &polarization(),
                        workers,
                        slots,
                        4096,
                        budget,
                        0,
                        &mut None,
                    )
                    .unwrap();
                    for (actual, reference) in wave.bands.iter().zip(&expected.bands[start..]) {
                        assert_eq!(actual.0.dirty(), reference.0.dirty());
                        assert_eq!(actual.0.psf(), reference.0.psf());
                        assert_eq!(actual.0.sensitivity(), reference.0.sensitivity());
                        assert_eq!(actual.0.sum_weights(), reference.0.sum_weights());
                    }
                    start += depth;
                    // Returned images/FFT owners leave scope before next selection.
                }
            }
        }
    }
    assert!(WavePlan::initial_prefix(store.plan, &bands, 0, 1, 0, u64::MAX).is_err());
    assert!(WavePlan::initial_prefix(store.plan, &bands, 4, 3, 0, u64::MAX).is_err());
    assert!(WavePlan::initial_prefix(store.plan, &[], 1, 1, 0, u64::MAX).is_err());
}

#[test]
fn wholly_unmapped_wave_uses_no_source_or_fabricated_rows() {
    let (_dir, mut store, bands, problem) = input(1, true);
    assert!(bands.iter().all(|band| band.native_range().is_empty()));
    let empty = lifecycle(&problem, 64).initial_empty().unwrap();
    let (result, measurements) = run(&mut store, initial_jobs(&bands), &empty, 4, 2);
    assert_eq!(result.source.bytes, 0);
    assert_eq!(result.source.operations, 0);
    assert_eq!(measurements.source_read_operations, 0);
    assert_eq!(measurements.logical_units_filled, 0);
    assert_eq!(measurements.worker_threads_started, 4);
    for (normal, _) in result.bands {
        assert_eq!(
            normal.channel_validity(),
            &[SpectralChannelValidity::Unmapped]
        );
        assert!(
            normal
                .dirty()
                .iter()
                .all(|&value| value == Complex64::default())
        );
    }
}

#[test]
fn model_read_failure_joins_the_wave_without_completion() {
    let (_dir, mut store, bands, problem) = input(1, false);
    let owner = lifecycle(&problem, 8);
    let empty = owner.initial_empty().unwrap();
    let (initial, _) = run(&mut store, initial_jobs(&bands), &empty, 4, 2);
    let model = changed_model(&owner, empty);
    let jobs = bands
        .iter()
        .zip(initial.bands)
        .map(|(plan, (normal, fft))| BandInput {
            plan: plan.residual_refresh(),
            prior: Some(normal),
            fft: Some(fft),
        })
        .collect();
    let mut measurements = None;
    let error = execute(
        &mut store,
        jobs,
        &model,
        &layout(),
        &OUTPUT,
        &polarization(),
        4,
        2,
        4096,
        4 << 30,
        1,
        &mut measurements,
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("model"), "{error}");
    assert!(
        measurements.is_some(),
        "failed execution still reports resource/work evidence"
    );
}
