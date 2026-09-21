// SPDX-License-Identifier: LGPL-3.0-or-later

//! Actual selected-MS traversal and scheduler-fence integration. The small
//! fixture composes paged model/normal owners and the native phase reservation.

use super::*;
use crate::complete_data_parallel_mfs_tests as support;
use crate::cube_state_plan::CubeStatePlan;
use crate::*;
use casa_imaging_model::*;
use casa_imaging_reconstruction::{WeightingExecutionLimits, plan_weighting};
use casa_ms::*;
use std::convert::Infallible;

#[path = "../../../casa-imaging-reconstruction/tests/support/streaming_cube.rs"]
#[allow(dead_code)]
mod fixture;

struct SourceFixture {
    _directory: tempfile::TempDir,
    problem: CompiledProblem,
    access: DeferredSelectedObservationAccess,
    authority: ResourceAuthority,
    storage: ManagedSpillStorage,
}

fn source_fixture(basis: SyntheticPolarizationBasis, channels: u32) -> SourceFixture {
    let directory = tempfile::tempdir().unwrap();
    let ms = directory.path().join("cube.ms");
    let artifacts = directory.path().join("artifacts");
    std::fs::create_dir(&artifacts).unwrap();
    let mut antennas = tutorial_vla_a_antennas();
    antennas.truncate(3);
    let mut request = SyntheticObservationRequest::vla_ppdisk("unused.fits", &ms, antennas);
    request.allow_below_elevation_limit = true;
    request.duration_seconds = 1.0;
    request.integration_seconds = 1.0;
    request.polarization_setup = SyntheticPolarizationSetup::new(basis, 2).unwrap();
    request.spectral_setup.channel_count = channels as usize;
    request.spectral_setup.start_frequency_hz = 0.999e9;
    request.spectral_setup.channel_width_hz = 1e6;
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    request.model = Some(SyntheticSkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("cube-fence".into()),
        components: vec![SyntheticAnalyticComponent::Point {
            name: None,
            l_rad: 0.0,
            m_rad: 0.0,
            spectrum: SyntheticAnalyticSpectrum {
                flux_jy: 1.0,
                spectral_index: 0.0,
                reference_frequency_hz: None,
                line_peak_jy: 0.0,
                line_center_fraction: 0.5,
                line_sigma_fraction: 0.1,
                absorption_peak_jy: 0.0,
                absorption_center_fraction: 0.5,
                absorption_sigma_fraction: 0.1,
            },
        }],
    });
    request.predict_model = true;
    generate_synthetic_observation_ms(&request).unwrap();
    initialize_measurement_set_owner_manifest(&ms).unwrap();
    let correlations = match basis {
        SyntheticPolarizationBasis::Circular => {
            [CorrelationType::CircularRr, CorrelationType::CircularLl]
        }
        SyntheticPolarizationBasis::Linear => {
            [CorrelationType::LinearXx, CorrelationType::LinearYy]
        }
    };
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(
            3,
            (0..3_u32).map(|row| SelectedMainRow::new(u64::from(row), 0)),
        )
        .unwrap(),
        RowSelection::new(
            IdSelection::All,
            TimeSelection::All,
            UvSelection::All,
            AntennaSelection::All,
            IdSelection::All,
            IdSelection::All,
            IntentSelection::All,
            IdSelection::All,
        ),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, (0..channels).collect())],
        vec![CorrelationSelection::new(
            0,
            correlations
                .into_iter()
                .enumerate()
                .map(|(i, correlation)| CorrelationProduct::new(i as u32, correlation))
                .collect(),
        )],
    );
    let (snapshot, access) =
        resolve_selected_observation(SelectedObservationResolutionRequest::new(
            ms.display().to_string(),
            LogicalIdentity::from_sha256([84; 32]),
            selection,
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(160 << 10, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        ))
        .unwrap()
        .into_parts();
    let problem = fixture::problem_with_inputs(
        SpectralSamplingLaw::LINEAR,
        ProblemInputIdentities::new(compile_observation(snapshot).unwrap()),
    );
    let authority = ResourceAuthority::with_inventory(support::runtime_inventory_with_roots(
        artifacts.clone(),
        ms,
    ))
    .unwrap();
    let storage =
        ManagedSpillStorage::bind(&authority, support::artifact_storage_io(), artifacts).unwrap();
    SourceFixture {
        _directory: directory,
        problem,
        access: access.into_deferred(),
        authority,
        storage,
    }
}

#[test]
fn streaming_cube_initial_source_fence_controls_runtime_reconciliation() {
    use SyntheticPolarizationBasis::{Circular, Linear};
    for (failure, workers, basis, channels) in [
        (Failure::None, 1, Circular, 6),
        (Failure::None, 4, Circular, 6),
        (Failure::None, 4, Linear, 8),
        (Failure::SourceFence, 1, Circular, 6),
        (Failure::ReconciliationNode, 1, Circular, 6),
    ] {
        let fixture = source_fixture(basis, channels);
        let problem = &fixture.problem;
        let registry = support::PlanningRegistry::new(problem);
        let policy = ResourcePolicy::Exclusive;
        let pass = SpectralPassIdentity::new(SpectralPassPhase::InitialMajor, 0);
        let weighting_limits = WeightingExecutionLimits::new(7, 1).unwrap();
        let execution_policy = SpectralCycleExecutionPolicy::new(
            support::implementation_id(),
            weighting_limits,
            fixture.access.certify_residency(problem).unwrap(),
            support::storage_io(),
            SpectralCyclePlanningLimits::new(1000, 1, 900000),
            fixture.authority.clone(),
            policy.clone(),
        );
        let (base, source) = crate::spectral_cycle_plan::base_physical(
            problem,
            &registry,
            &execution_policy,
            pass,
            None,
        )
        .unwrap();
        let read = base
            .execution_dag()
            .nodes()
            .values()
            .find(|node| node.kind == WorkKind::ObservationRead)
            .unwrap()
            .id
            .clone();
        let prepare = base
            .observation_transaction()
            .final_model_preparation()
            .unwrap()
            .clone();
        let reconcile = base
            .observation_transaction()
            .post_replay_reconciliation()
            .unwrap()
            .clone();
        let weighting = plan_weighting(problem, weighting_limits).unwrap();
        let physical = WeightingPlanFragment::streaming_for_pass(
            &weighting,
            read.clone(),
            source.clone(),
            support::implementation_id(),
            pass,
            WeightingStreamingMode::NaturalInitial,
            None,
        )
        .with_replay_preparation((workers > 1).then(|| {
            crate::weighting::ReplayPreparationPlan::new(problem, &weighting, workers).unwrap()
        }))
        .compose(&base)
        .unwrap();
        let cube_state = CubeStatePlan::streaming_cube(
            problem,
            &fixture.storage,
            1,
            prepare.clone(),
            reconcile.clone(),
            true,
        )
        .unwrap();
        let physical = cube_state
            .compose(
                &registry,
                support::implementation_id(),
                &fixture.storage,
                physical,
                &read,
                &reconcile,
            )
            .unwrap();
        let output_hz: Vec<_> = (0..problem.geometry().spectral().output_channels())
            .map(|channel| {
                problem
                    .geometry()
                    .spectral()
                    .channel_centre_hz(channel)
                    .unwrap()
            })
            .collect();
        let bands: Vec<_> = (0..output_hz.len())
            .map(|channel| {
                BandPlan::new(
                    &SpectralOperatorSpecification::for_slab(problem, channel, 1).unwrap(),
                    SpectralOperatorPass::InitialMajor,
                )
                .unwrap()
            })
            .collect();
        let shared = InitialCube::shared_bytes(problem, output_hz.capacity()).unwrap();
        let native_plan = NativePhasePlan::for_initial_source(
            &physical,
            &fixture.authority,
            &policy,
            &fixture.storage,
            problem,
            &bands,
            shared,
            workers,
            1,
        )
        .unwrap();
        let store = native_plan.store;
        assert_eq!(store.block_rows as u64, store.rows);
        assert!((1..=store.channels).contains(&store.tile_channels));
        let available = fixture
            .authority
            .remaining_planning_memory_bytes(
                &policy,
                physical.execution_dag().resource_alternative(),
            )
            .unwrap();
        let base_charge = fixture.authority.topology().memory_domains[0].capacity_bytes - available;
        let limited = |bytes| {
            ResourcePolicy::Explicit(ResourceOverride {
                memory_bytes: [(CapacityDomainId::new("host-memory"), base_charge + bytes)]
                    .into_iter()
                    .collect(),
                ..ResourceOverride::default()
            })
        };
        let narrower = NativePhasePlan::for_initial_source(
            &physical,
            &fixture.authority,
            &limited(native_plan.workspace_bytes / 2),
            &fixture.storage,
            problem,
            &bands,
            shared,
            workers,
            1,
        )
        .unwrap();
        assert!(narrower.store.tile_channels < store.channels);
        assert!(narrower.store.tile_channels <= store.tile_channels);
        assert_eq!(narrower.store.block_rows, store.block_rows);
        assert!(narrower.workspace_bytes <= native_plan.workspace_bytes / 2);
        let exact = NativePhasePlan::new(
            &physical,
            &fixture.authority,
            &limited(native_plan.workspace_bytes),
            &fixture.storage,
            store,
            &bands,
            shared,
            workers,
            1,
        )
        .unwrap();
        assert_eq!(exact.workspace_bytes, native_plan.workspace_bytes);
        assert!(
            NativePhasePlan::new(
                &physical,
                &fixture.authority,
                &limited(native_plan.shared_bytes + store.preparation_residency().unwrap() - 1),
                &fixture.storage,
                store,
                &bands,
                shared,
                workers,
                1,
            )
            .is_err()
        );
        assert!(
            NativePhasePlan::new(
                &physical,
                &fixture.authority,
                &policy,
                &fixture.storage,
                store,
                &bands,
                u64::MAX,
                workers,
                1,
            )
            .is_err()
        );
        let expected_physical = native_plan.compose(physical, &fixture.storage).unwrap();
        drop(bands);
        let (physical, mut executor) = InitialCube::plan(
            problem.clone(),
            &registry,
            execution_policy,
            fixture.storage,
            fixture.access,
            workers,
            1,
            0,
        )
        .unwrap();
        executor.failure = failure;
        assert_eq!(executor.native_plan.store, store);
        assert_eq!(
            executor.native_plan.workspace_bytes,
            native_plan.workspace_bytes
        );
        assert_eq!(
            physical.execution_dag().resource_alternative(),
            expected_physical.execution_dag().resource_alternative()
        );
        let directory = tempfile::tempdir().unwrap();
        let receipts = ExecutionReceiptStore::new(
            directory.path(),
            ReceiptRetention::new(2, 1 << 20).unwrap(),
        )
        .unwrap();
        let cost = PlannerCostModelProfileId::from_sha256([85; 32]);
        let plan = crate::plan(
            problem,
            PlanningBindings::new(
                registry.registry_id(),
                policy.clone(),
                PlannerCostModelProfileBootstrap::new(cost),
            ),
            &fixture.authority,
            &registry,
            &receipts,
            |_, _| Ok::<_, Infallible>(vec![physical.clone()]),
        )
        .unwrap();
        let registry = SpectralCycleRegistry::new(
            registry.registry_id(),
            support::implementation_id(),
            problem,
            executor,
        );
        let result = crate::run(
            &ExecutableModelProblem::from_compiled(problem.clone()).unwrap(),
            &plan,
            &RunBindings::new(problem.inputs().clone(), &policy, cost),
            &registry,
            &fixture.authority,
            &mut RunToCompletion,
            receipts.bind(ExecutionProvenance::new(
                ExecutionAttemptId::from_sha256([86; 32]),
                BuildIdentity::from_sha256([82; 32]),
            )),
        );
        let state = registry.implementation().state.lock().unwrap();
        if failure != Failure::None {
            let Err(RunError::Execution { source, .. }) = result else {
                panic!("expected runtime execution failure: {result:?}");
            };
            if failure == Failure::SourceFence {
                assert_eq!(source.raw_os_error(), Some(5));
            } else {
                assert_eq!(
                    source.to_string(),
                    CompleteDataOperatorError::ExecutionBinding.to_string()
                );
            }
            assert!(state.complete.is_none());
            assert!(state.native.is_none());
            assert!(!state.published);
        } else {
            assert_eq!(result.unwrap(), ExecutionOutcome::Succeeded);
            assert!(state.complete.is_some());
            assert!(state.published);
            assert_eq!(
                state.weighting.parallel_preparation_sample_count(),
                if workers > 1 {
                    3 * u64::from(channels) * 2
                } else {
                    0
                },
                "every selected sample uses the admitted preparation team"
            );
        }
    }
}
