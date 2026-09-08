// SPDX-License-Identifier: LGPL-3.0-or-later

//! Payload-free whole-phase admission. Frame counts are a conservative synthetic
//! bound, not a claim to reproduce the unretained full-run descriptor sequence.

use super::*;
use crate::complete_data_operator::GriddedNormalReplayWindowPlan;
use crate::complete_data_parallel_mfs_tests::{
    PlanningRegistry, artifact_storage_io, geometry_with_facets, implementation_id,
    problem_specification, runtime_inventory_with_roots, storage_io,
};
use casa_imaging_model::*;
use casa_imaging_reconstruction::{
    AwKernelLayout, AwOperatorError, AwPreparedCatalog, AwPreparedCellLease,
    AwPreparedCellMetadata, AwPreparedCellProvider, runtime_adapter::GriddedNormalStorageLayout,
};
use casa_ms::{
    BoundSelectedObservation, ObservationSourceBinding, SelectedObservationContentBudget,
};

#[path = "../../../casa-imaging-model/tests/common/mod.rs"]
mod model_fixture;

fn lifecycle() -> ModelLifecycleRequirements {
    ModelLifecycleRequirements::new(
        ModelBounds::new(
            100_000_000,
            100_000_000,
            100_000_000,
            100_000_000,
            1.0e30,
            1.0e30,
        )
        .unwrap(),
        NumericPrecision::F64,
        ModelInputCommitment::Empty,
    )
}

fn full_aw_problem() -> CompiledProblem {
    let geometry = geometry_with_facets(FacetLayout::Single);
    let base = compile(ImagingRequest::new(
        problem_specification(WeightingContract::new(
            WeightingScheme::Natural,
            WeightDensityScope::NotApplicable,
        )),
        geometry.clone(),
        model_fixture::problem_inputs(
            1,
            vec![
                (ReferenceDataKind::Measures, model_fixture::identity(90)),
                (ReferenceDataKind::Instrument, model_fixture::identity(91)),
            ],
            ModelStateIdentity::Empty,
        ),
        lifecycle(),
    ))
    .unwrap();
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [2048.0; 2],
        [-2.908_882_086_657_216e-6, 2.908_882_086_657_216e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let domain = geometry.domains()[0]
        .clone()
        .with_shape(ImageShape::new(4096, 4096))
        .with_direction(direction);
    let aw = AwProjectionContract::new(
        3.0,
        std::num::NonZeroUsize::new(1).unwrap(),
        false,
        false,
        false,
        true,
        false,
        [0.0; 2],
        360.0,
        360.0,
    )
    .unwrap();
    compile(ImagingRequest::new(
        ProblemSpecification::new(
            ScientificContract::new(
                base.science().spectral(),
                MeasurementEquationContract::new(
                    InstrumentResponse::PrimaryBeam,
                    base.science().measurement_equation().inner_products(),
                )
                .with_aw_projection(aw),
            )
            .with_instrument_model(InstrumentModel::CasaEvlaWidebandAwV1),
            ReconstructionContract::new(
                ReconstructionBasis::Taylor { terms: 2 },
                ReconstructionAlgorithm::Mtmfs {
                    scales_px: vec![0.0, 5.0, 12.0],
                    small_scale_bias: 0.0,
                },
                ReconstructionControls::new(10, 0.1, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(
                WeightingScheme::Briggs { robust: 1.0 },
                WeightDensityScope::GlobalSelection,
            ),
            ProductRequirements::new(
                vec![ProductKind::Psf, ProductKind::Residual, ProductKind::Model],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                base.products().validity(),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            base.numerics().clone(),
        ),
        geometry.with_domains(vec![domain]),
        base.inputs().clone(),
        lifecycle(),
    ))
    .unwrap()
}

#[derive(Clone)]
struct UnreadProvider;
impl AwPreparedCellProvider for UnreadProvider {
    fn load(
        &mut self,
        _: &AwPreparedCellMetadata,
        _: usize,
    ) -> Result<AwPreparedCellLease, AwOperatorError> {
        panic!("admission must not allocate or read CF payloads")
    }
}

fn projection() -> PreparedAwProjection {
    let layout = AwKernelLayout::new([50; 2], 1, [103; 2], [51; 2]).unwrap();
    let metadata = (0..1024)
        .map(|index| {
            let frequency = 1.0e9 + f64::from(index) * 1.0e6;
            let identity = PreparedArtifactScientificIdentity::convolution_function(
                PreparedArtifactCellSemantics::new(
                    frequency,
                    1.0,
                    0,
                    0,
                    0.0,
                    frequency,
                    0,
                    "EVLA",
                    "L",
                    25.0,
                    1.0,
                    PreparedArtifactAwInterpretation::Wavelength,
                    false,
                    "discrete-complex-sum",
                )
                .unwrap(),
            )
            .unwrap();
            AwPreparedCellMetadata::new(identity, frequency, 1.0, 1.0, 0, 0.0, layout, layout)
                .unwrap()
        })
        .collect();
    PreparedAwProjection::new(
        AwPreparedCatalog::new(metadata).unwrap(),
        UnreadProvider,
        false,
        402_653_184,
    )
    .unwrap()
}

fn inventory(root: &std::path::Path, memory: u64) -> HostInventory {
    let mut inventory = runtime_inventory_with_roots(root.to_owned(), root.join("unused-source"));
    inventory.topology.memory_domains[0].capacity_bytes = memory;
    inventory
        .pressure
        .memory_available_bytes
        .insert(CapacityDomainId::new("host-memory"), memory);
    for domain in &mut inventory.topology.storage_domains {
        domain.capacity_bytes = 64 << 30;
        inventory
            .pressure
            .storage_available_bytes
            .insert(domain.id.clone(), 64 << 30);
    }
    inventory.topology.cache_capacity_bytes = 8 << 30;
    inventory.pressure.cache_available_bytes = 8 << 30;
    inventory
}

#[test]
fn t51_full_aw_residual_phase_adapts_complete_allocations_and_rejects_below_floor() {
    let root = tempfile::tempdir().unwrap();
    let problem = full_aw_problem();
    let registry = PlanningRegistry::new(&problem);
    let bindings = problem
        .inputs()
        .observation_snapshot()
        .sources()
        .iter()
        .map(|source| {
            ObservationSourceBinding::new(
                ObservationSourceState::new(
                    source.identity(),
                    source.selection().rows().clone(),
                    source.generations().clone(),
                ),
                SelectedObservationContentBudget::new(2 << 20, 1, 4096),
            )
        })
        .collect::<Vec<_>>();
    let residency = BoundSelectedObservation::certify_residency(&problem, &bindings).unwrap();
    let memory = 17_728_272_996;
    let authority = ResourceAuthority::with_inventory(inventory(root.path(), memory)).unwrap();
    let storage =
        ManagedSpillStorage::bind(&authority, artifact_storage_io(), root.path()).unwrap();
    let policy = SpectralCycleExecutionPolicy::new(
        implementation_id(),
        WeightingExecutionLimits::new(4096, 4096).unwrap(),
        residency,
        storage_io(),
        SpectralCyclePlanningLimits::new(1_000, 725_000_000, 900_000),
        authority.clone(),
        ResourcePolicy::Exclusive,
    )
    .with_gridded_normal_storage(storage)
    .with_aw_projection(projection())
    .with_prepared_artifact_reader(PreparedArtifactReaderPlan::planning_fixture(
        problem.problem_id(),
        implementation_id(),
        StorageDomainId::new("atomic-output"),
        402_653_184,
        68_747_264,
        1_124_172,
        5_700_760_064,
    ));
    validate_aw_projection_binding(&problem, &policy).unwrap();
    let weighting = plan_weighting(&problem, policy.weighting_limits).unwrap();
    let frames = vec![(4096 * 96, 4096); 40_950];
    let layout = GriddedNormalStorageLayout::new([[4096; 2]], 2, 50, true).unwrap();
    let minimum_bytes =
        GriddedNormalReplayWindowPlan::minimum_working_set_bytes(&frames, 1).unwrap();
    let preview = |budget: Option<u64>| {
        GriddedNormalReplayWindowPlan::plan_frame_payloads(
            &frames,
            budget.unwrap_or(minimum_bytes),
            96,
            1,
            layout,
        )
        .map_err(SpectralCyclePlanError::from)
    };
    let preferred = preview(Some(minimum_bytes * 4)).unwrap();
    let phase = SpectralCyclePhasePlanning {
        pass: SpectralPassIdentity::new(SpectralPassPhase::FinalMajor, 1),
        include_minor: true,
        phase_input: Some(ArtifactIdentity::from_owner_digest([50; 32])),
        strategy: GriddedNormalStrategy::ReuseManagedSpill,
        artifact_budget: Some(
            crate::complete_data_operator::project_managed_spill_budget(&problem, 4096).unwrap(),
        ),
        gridded_replay_descriptor: Some(GriddedNormalReplayDescriptor::planning_fixture(
            16_106_938_800,
        )),
    };
    let compose = |window: &GriddedNormalReplayWindowPlan| {
        compose_major_physical(
            &problem,
            &registry,
            &policy,
            &weighting,
            phase,
            1,
            Some(window),
        )
    };
    let preferred_candidate = compose(&preferred).unwrap();
    assert!(
        authority
            .remaining_planning_memory_bytes(
                &policy.resource_policy,
                preferred_candidate
                    .physical
                    .execution_dag()
                    .resource_alternative()
            )
            .is_err()
    );
    let (selected, candidate) =
        select_gridded_window_plan(preferred.clone(), &policy, preview, compose).unwrap();
    let mut fixed_policy = policy.clone();
    fixed_policy.resource_policy = ResourcePolicy::Explicit(crate::ResourceOverride {
        memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), memory)]),
        workers: Some(1),
        ..crate::ResourceOverride::default()
    });
    for window in [&preferred, &selected] {
        let planned = compose_major_physical(
            &problem,
            &registry,
            &fixed_policy,
            &weighting,
            phase,
            1,
            Some(window),
        )
        .unwrap();
        let reader = planned.complete_data.prepared_artifact_reader().unwrap();
        let nodes = planned.physical.execution_dag().nodes();
        let join = adaptation_route_join_node(phase.pass);
        assert!(
            nodes[reader.node()]
                .dependencies
                .contains(&WorkDependency::Work(join.clone()))
        );
        assert!(
            nodes[planned.complete_data.replay_node()]
                .dependencies
                .contains(&WorkDependency::Work(reader.node().clone()))
        );
        for route in [
            retained_route_node(phase.pass),
            low_memory_io_route_node(phase.pass),
        ] {
            assert!(
                nodes[&join]
                    .dependencies
                    .contains(&WorkDependency::Fence(FenceId::new(route, FenceKind::Io)))
            );
        }
    }
    assert!(selected.maximum_records() < preferred.maximum_records());
    assert!(candidate.complete_data.residency().aw_prepared_pool_bytes() >= 472_524_620);
    assert_eq!(
        candidate
            .complete_data
            .residency()
            .major_cycle_model_bytes(),
        2_147_483_648
    );
    let receipts = ExecutionReceiptStore::new(
        root.path().join("receipts"),
        ReceiptRetention::new(1, 1 << 20).unwrap(),
    )
    .unwrap();
    let planning = PlanningBindings::new(
        registry.registry_id(),
        ResourcePolicy::Exclusive,
        PlannerCostModelProfileBootstrap::new(PlannerCostModelProfileId::from_sha256([51; 32])),
    );
    crate::plan(
        &problem,
        planning.clone(),
        &authority,
        &registry,
        &receipts,
        |_, _| Ok::<_, std::convert::Infallible>(vec![candidate.physical]),
    )
    .expect("whole selected residual phase is admitted");

    let minimum = preview(None).unwrap();
    let minimum_candidate = compose(&minimum).unwrap();
    let serial = minimum_candidate.physical.clone();
    let remaining = authority
        .remaining_planning_memory_bytes(
            &policy.resource_policy,
            serial.execution_dag().resource_alternative(),
        )
        .unwrap();
    let floor = memory - remaining;
    let mut at_floor = policy.clone();
    at_floor.authority = ResourceAuthority::with_inventory(inventory(root.path(), floor)).unwrap();
    for workers in [1, 4] {
        let (_, candidate) =
            select_gridded_window_plan(preferred.clone(), &at_floor, preview, |window| {
                compose_major_physical(
                    &problem,
                    &registry,
                    &at_floor,
                    &weighting,
                    phase,
                    workers,
                    Some(window),
                )
            })
            .unwrap();
        let quote = at_floor.authority.remaining_planning_memory_bytes(
            &at_floor.resource_policy,
            candidate.physical.execution_dag().resource_alternative(),
        );
        assert_eq!(
            quote.is_ok(),
            workers == 1,
            "the window search must quote this exact worker profile, not a serial projection"
        );
        assert_eq!(
            candidate
                .physical
                .execution_dag()
                .resource_alternative()
                .scaling
                .minimum_workers,
            workers
        );
        assert_eq!(
            candidate
                .physical
                .execution_dag()
                .resource_alternative()
                .scaling
                .maximum_workers,
            workers
        );
    }
    assert!(
        matches!(
            select_gridded_window_plan(preferred.clone(), &policy, preview, |_| {
                Err(SpectralCyclePlanError::Overflow)
            }),
            Err(SpectralCyclePlanError::Overflow)
        ),
        "invalid composition must not be treated as a capacity refusal"
    );
    for workers in [1, 4] {
        let physical = compose_major_physical(
            &problem,
            &registry,
            &policy,
            &weighting,
            phase,
            workers,
            Some(&minimum),
        )
        .unwrap()
        .physical;
        assert_eq!(
            physical.execution_dag().logical_allocations(),
            serial.execution_dag().logical_allocations()
        );
        crate::plan(
            &problem,
            planning.clone(),
            &authority,
            &registry,
            &receipts,
            |_, _| Ok::<_, std::convert::Infallible>(vec![physical]),
        )
        .unwrap();
    }
    let mut below = policy.clone();
    below.authority = ResourceAuthority::with_inventory(inventory(root.path(), floor - 1)).unwrap();
    let (_, rejected) = select_gridded_window_plan(preferred, &below, preview, |window| {
        compose_major_physical(
            &problem,
            &registry,
            &below,
            &weighting,
            phase,
            1,
            Some(window),
        )
    })
    .unwrap();
    assert!(
        crate::plan(
            &problem,
            planning,
            &below.authority,
            &registry,
            &receipts,
            |_, _| Ok::<_, std::convert::Infallible>(vec![rejected.physical])
        )
        .is_err()
    );
    eprintln!(
        "t51_full_aw_admission synthetic_frame_record_bound=4096 frames=40950 selected_records={} minimum_phase_bytes={floor} available_bytes={memory}",
        selected.maximum_records()
    );
}
