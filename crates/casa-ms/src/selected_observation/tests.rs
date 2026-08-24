// SPDX-License-Identifier: LGPL-3.0-or-later

use super::{
    BoundObservationSource, BoundSelectedObservation, ObservationSourceBinding,
    SelectedObservationContentBudget, SelectedObservationRow, SelectedObservationTraversalError,
    bound_observation::consume_validated_stream,
};
use crate::subtables::SubTable;
use crate::{
    MeasurementSet, MsSelectionIoBudget, SyntheticObservationRequest, SyntheticSpectralSetup,
    SyntheticWorkerPolicy, generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};
use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame, FacetLayout,
    FiniteValuePolicy, FlagPolicy, FrequencyFrame, GeometryInput, IdSelection, ImageAxis,
    ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest, InstrumentResponse,
    IntentSelection, LogicalIdentity, MeasurementEquationContract, MeasurementSetIdentity,
    MetadataGeneration, MetadataTableKind, MissingPointingPolicy, ModelBounds, ModelColumnState,
    ModelColumnWrite, ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements,
    ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage, NumericsContract,
    ObservationPointingLaw, ObservationSelection, ObservationSnapshotInput, ObservationSource,
    ObservationSourceInput, ObservationSourceProvenance, ObservationSourceState,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, ReferenceDataKind, RestFrequency, RestoringBeamPolicy,
    RowSelection, ScientificContract, SelectedColumns, SelectedMainRow,
    SelectedObservationGenerationId, SelectedObservationInspectionError,
    SelectedObservationPassError, SelectedObservationSample, SelectedRows,
    SelectedVisibilitySample, SelectionBound, SkyDirection, SourceGenerations, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSampling, SpectralWcs,
    SpectralWindowSelection, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    TimeRange, TimeSelection, UvSelection, UvwCoordinateLaw, VisibilityColumn,
    VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract, WeightingScheme,
    compile, compile_observation,
};
use casa_tables::ColumnSchema;
use casa_types::measures::{EopValues, MeasuresProvider, MeasuresProviderState};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use std::convert::Infallible;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

/// Canonical model-lifecycle commitment matching the compiled snapshot.
fn model_lifecycle(model: ModelStateIdentity) -> ModelLifecycleRequirements {
    let input = match model {
        ModelStateIdentity::Empty => ModelInputCommitment::Empty,
        ModelStateIdentity::Seed(source) => ModelInputCommitment::AlignedSeed {
            source,
            support: LogicalIdentity::from_sha256([0xa5; 32]),
        },
        ModelStateIdentity::Generation(generation) => ModelInputCommitment::Generation(generation),
    };
    ModelLifecycleRequirements::new(
        ModelBounds::new(
            10_000_000, 10_000_000, 10_000_000, 10_000_000, 1.0e30, 1.0e30,
        )
        .expect("valid model lifecycle bounds"),
        NumericPrecision::F32,
        input,
    )
}

#[derive(Debug)]
struct AccountedTestMeasures {
    state: Mutex<AccountedTestMeasuresState>,
}

#[derive(Debug)]
struct AccountedTestMeasuresState {
    identity_sha256: [u8; 32],
    retained: Vec<u8>,
    dut1_seconds: f64,
}

impl AccountedTestMeasures {
    fn with_heap_bytes(bytes: usize) -> Self {
        Self::with_identity(90, bytes)
    }

    fn with_identity(identity: u8, bytes: usize) -> Self {
        Self {
            state: Mutex::new(AccountedTestMeasuresState {
                identity_sha256: [identity; 32],
                retained: vec![0; bytes],
                dut1_seconds: 0.0,
            }),
        }
    }

    fn grow(&self, additional: usize) {
        self.state
            .lock()
            .expect("test Measures residency lock")
            .retained
            .reserve_exact(additional);
    }

    fn mutate_science(&self, identity: u8, dut1_seconds: f64) {
        let mut state = self.state.lock().expect("test Measures state lock");
        state.identity_sha256 = [identity; 32];
        state.dut1_seconds = dut1_seconds;
    }
}

impl MeasuresProvider for AccountedTestMeasures {
    fn prepare_bounded_state(&self) -> Result<Option<MeasuresProviderState>, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "test Measures state lock poisoned".to_string())?;
        Ok(Some(MeasuresProviderState::new(
            state.identity_sha256,
            state.retained.capacity(),
        )))
    }

    fn eop_values(&self, _utc_mjd: f64) -> Result<Option<EopValues>, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "test Measures state lock poisoned".to_string())?;
        Ok(Some(EopValues {
            dut1_seconds: state.dut1_seconds,
            x_arcsec: 0.0,
            y_arcsec: 0.0,
            dx_mas: 0.0,
            dy_mas: 0.0,
            is_predicted: false,
        }))
    }
}

#[derive(Debug)]
struct OpaqueTestMeasures;

impl MeasuresProvider for OpaqueTestMeasures {}

#[test]
fn retained_selected_samples_are_bounded_and_block_partition_invariant() {
    let directory = tempfile::tempdir().expect("temporary selected-observation fixture");
    let path = directory.path().join("selected.ms");
    generate_fixture(&path);

    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let one_row_budget = content_budget_for_rows(&problem, source, 1, 1);
    let two_row_budget = content_budget_for_rows(&problem, source, 2, 1);
    let one_row =
        BoundObservationSource::open(&problem, source, &source_state(source), one_row_budget)
            .expect("bind one-row physical blocks");
    let two_rows =
        BoundObservationSource::open(&problem, source, &source_state(source), two_row_budget)
            .expect("bind two-row physical blocks");
    assert_eq!(
        one_row.content_plan().rows_per_block(),
        1,
        "{:?}",
        one_row.content_plan()
    );
    assert_eq!(
        two_rows.content_plan().rows_per_block(),
        2,
        "{:?}",
        two_rows.content_plan()
    );
    assert_eq!(
        one_row.content_plan().bytes_per_row(),
        two_rows.content_plan().bytes_per_row()
    );
    assert!(
        one_row.content_plan().preparation_bytes_per_row() > one_row.content_plan().bytes_per_row()
    );
    assert!(one_row.content_plan().retained_bytes() > 0);
    assert!(one_row.content_plan().initialization_scratch_bytes() > 0);
    assert!(one_row.content_plan().maximum_resident_bytes() <= one_row_budget.available_bytes());
    assert!(two_rows.content_plan().maximum_resident_bytes() <= two_row_budget.available_bytes());
    assert!(
        one_row.content_plan().preparation_bytes_per_block()
            > one_row.content_plan().bytes_per_block()
    );

    let one_row_samples = one_row
        .selected_samples(&problem)
        .expect("prepare one-row selected stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read one-row selected stream");
    let two_row_samples = two_rows
        .selected_samples(&problem)
        .expect("prepare two-row selected stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read two-row selected stream");

    assert_eq!(one_row_samples.len(), 8);
    assert_eq!(one_row_samples, two_row_samples);
    assert_eq!(
        one_row_samples
            .iter()
            .map(|sample| {
                (
                    sample.address.physical_row,
                    sample.address.channel_index,
                    sample.address.correlation_index,
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (0, 0, 0),
            (0, 0, 1),
            (0, 2, 0),
            (0, 2, 1),
            (1, 0, 0),
            (1, 0, 1),
            (1, 2, 0),
            (1, 2, 1),
        ]
    );
    let first = one_row_samples[0];
    assert_eq!(first.address.frequency_centre_hz, 1.4e9);
    assert_eq!(first.address.frequency_lower_hz, 1.3995e9);
    assert_eq!(first.address.frequency_upper_hz, 1.4005e9);
    assert_eq!(first.address.channel_width_hz, 1.0e6);
    assert_eq!(first.address.frequency_frame, FrequencyFrame::Topocentric);
    assert_eq!(first.address.correlation_type, CorrelationType::CircularRr);
    assert_eq!(
        first.visibility,
        SelectedVisibilitySample::Complex32([0.0, 0.0])
    );
    assert_eq!(first.coordinates.density_uvw_m, first.coordinates.raw_uvw_m);
    assert_eq!(
        first.coordinates.transformed_uvw_m,
        first.coordinates.raw_uvw_m
    );
    assert_eq!(first.coordinates.phase_shift_m, 0.0);
    assert_eq!(
        first.coordinates.phase_direction,
        first.coordinates.delay_direction
    );
    assert_eq!(
        first.coordinates.phase_direction,
        first.coordinates.pointing_directions.antenna1
    );
    assert_eq!(
        first.coordinates.phase_direction.frame(),
        DirectionFrame::J2000
    );
    assert_eq!(first.metadata.antenna1, 0);
    assert_eq!(first.metadata.antenna2, 1);
    assert_eq!(first.metadata.feed1, 0);
    assert_eq!(first.metadata.feed2, 0);
    assert_eq!(
        inspect_samples(&problem, one_row_samples).expect("inspect exact bounded stream"),
        inspect_samples(&problem, two_row_samples).expect("inspect repartitioned bounded stream")
    );
}

#[test]
fn sparse_manifest_reads_only_selected_physical_rows() {
    let directory = tempfile::tempdir().expect("temporary sparse selected-observation fixture");
    let path = directory.path().join("sparse.ms");
    generate_fixture_with_rows(&path, 64);
    let selected_rows = SelectedRows::from_ordered_main_rows(
        64,
        [SelectedMainRow::new(0, 0), SelectedMainRow::new(63, 0)],
    )
    .expect("sparse exact row manifest");
    let measurement_set = MeasurementSet::open(&path).expect("open sparse fixture");
    let times = [0, 63].map(|row| main_time_mjd_seconds(&measurement_set, row));
    let sparse_filter = RowSelection::new(
        IdSelection::All,
        TimeSelection::Ranges(
            times
                .into_iter()
                .map(|time| {
                    TimeRange::new(
                        Some(SelectionBound::inclusive(time)),
                        Some(SelectionBound::inclusive(time)),
                    )
                })
                .collect(),
        ),
        UvSelection::All,
        AntennaSelection::All,
        IdSelection::All,
        IdSelection::All,
        IntentSelection::All,
        IdSelection::All,
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source_input_with_selected_rows_and_filter(
            &path,
            1,
            selected_rows,
            sparse_filter,
        )],
        vec![(ReferenceDataKind::Measures, identity(90))],
        ModelStateIdentity::Empty,
    ))
    .expect("compile sparse selected observation");
    let problem = compile(ImagingRequest::new(
        specification(),
        geometry(),
        ProblemInputIdentities::new(snapshot.clone()),
        model_lifecycle(snapshot.model()),
    ))
    .expect("compile sparse selected-observation problem");
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let samples = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 2, 1),
    )
    .expect("bind sparse selected observation")
    .selected_samples(&problem)
    .expect("prepare sparse selected stream")
    .collect::<Result<Vec<_>, _>>()
    .expect("read sparse selected stream");

    assert_eq!(samples.len(), 2 * 2 * 2);
    assert_eq!(
        samples
            .iter()
            .map(|sample| sample.address.physical_row)
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 63].into_iter().collect()
    );
}

#[test]
fn unconditional_sparse_manifest_is_rejected_without_scanning_intervening_rows() {
    let directory = tempfile::tempdir().expect("temporary incomplete-manifest fixture");
    let path = directory.path().join("incomplete-manifest.ms");
    generate_fixture_with_rows(&path, 64);
    let selected_rows = SelectedRows::from_ordered_main_rows(
        64,
        [SelectedMainRow::new(0, 0), SelectedMainRow::new(63, 0)],
    )
    .expect("corrupt unconditional row manifest");
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source_input_with_selected_rows(&path, 1, selected_rows)],
        vec![(ReferenceDataKind::Measures, identity(90))],
        ModelStateIdentity::Empty,
    ))
    .expect("compile incomplete unconditional manifest");
    let problem = compile(ImagingRequest::new(
        specification(),
        geometry(),
        ProblemInputIdentities::new(snapshot.clone()),
        model_lifecycle(snapshot.model()),
    ))
    .expect("compile incomplete-manifest problem");
    let source = &problem.inputs().observation_snapshot().sources()[0];

    assert!(matches!(
        BoundObservationSource::open(
            &problem,
            source,
            &source_state(source),
            content_budget_for_rows(&problem, source, 2, 1),
        ),
        Err(super::BoundObservationSourceError::IncompleteUnconditionalRowManifest)
    ));
}

#[test]
fn retained_metadata_is_rejected_before_content_blocks_are_planned() {
    let directory = tempfile::tempdir().expect("temporary metadata-budget fixture");
    let path = directory.path().join("metadata-budget.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let error = match BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        SelectedObservationContentBudget::new(1, 1, 4),
    ) {
        Ok(_) => {
            panic!("retained geometry and coordinate catalogs must fit before engine construction")
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
        )
    ));
}

#[test]
fn selected_observation_rejects_opaque_foreign_and_mutated_measures_providers() {
    let directory = tempfile::tempdir().expect("temporary Measures-binding fixture");
    let path = directory.path().join("measures-binding.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];

    assert!(matches!(
        super::SelectedObservationMeasures::new(Arc::new(OpaqueTestMeasures)),
        Err(super::SelectedObservationMeasuresError::UnaccountedProvider)
    ));

    let foreign = super::SelectedObservationMeasures::new(Arc::new(
        AccountedTestMeasures::with_identity(91, 0),
    ))
    .expect("acquire foreign provider state");
    let foreign_binding = ObservationSourceBinding::new(
        source_state(source),
        content_budget_for_rows(&problem, source, 1, 1),
    );
    assert!(matches!(
        BoundSelectedObservation::open(&problem, foreign, vec![foreign_binding]),
        Err(super::BoundSelectedObservationError::Measures(
            super::SelectedObservationMeasuresError::ReferenceIdentityMismatch { .. }
        ))
    ));

    let mutable_provider = Arc::new(AccountedTestMeasures::with_heap_bytes(64));
    let erased_provider: Arc<dyn MeasuresProvider> = mutable_provider.clone();
    let mutated = super::SelectedObservationMeasures::new(erased_provider)
        .expect("acquire mutable provider before mutation");
    mutable_provider.mutate_science(92, 0.25);
    let mutated_binding = ObservationSourceBinding::new(
        source_state(source),
        content_budget_for_rows(&problem, source, 1, 1),
    );
    assert!(matches!(
        BoundSelectedObservation::open(&problem, mutated, vec![mutated_binding]),
        Err(super::BoundSelectedObservationError::Measures(
            super::SelectedObservationMeasuresError::ProviderStateChanged { .. }
        ))
    ));
}

#[test]
fn measures_provider_growth_during_traversal_prevents_owner_completion() {
    let directory = tempfile::tempdir().expect("temporary Measures-mutation fixture");
    let path = directory.path().join("measures-mutation.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];

    let mutable_provider = Arc::new(AccountedTestMeasures::with_heap_bytes(64));
    let erased_provider: Arc<dyn MeasuresProvider> = mutable_provider.clone();
    let measures =
        super::SelectedObservationMeasures::new(erased_provider).expect("account mutable provider");
    let shared_bytes = selected_observation_shared_bytes(
        &measures,
        BoundObservationSource::retained_source_slot_bytes(),
        single_binding_graph_initialization_bytes(source),
    );
    let budget = content_budget_for_rows_with_shared_bytes(&problem, source, shared_bytes, 1, 1);
    let mut observation = BoundSelectedObservation::open(
        &problem,
        measures,
        vec![ObservationSourceBinding::new(source_state(source), budget)],
    )
    .expect("bind provider before mutation");
    let mut mutated = false;

    let error = observation
        .traverse(&problem, |_| {
            if !mutated {
                mutable_provider.grow(4_096);
                mutated = true;
            }
            Ok::<_, Infallible>(())
        })
        .expect_err("terminal provider mutation must prevent owner completion");

    assert!(matches!(
        error,
        SelectedObservationTraversalError::Source(super::BoundObservationSourceError::Storage(
            crate::MsError::MeasuresRuntime(_)
        ))
    ));
}

#[test]
fn measures_provider_residency_is_charged_once_and_rejected_under_a_tight_budget() {
    let directory = tempfile::tempdir().expect("temporary Measures-budget fixture");
    let path = directory.path().join("measures-budget.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];

    let baseline_measures = test_measures(&problem);
    let baseline_shared_bytes = selected_observation_shared_bytes(
        &baseline_measures,
        BoundObservationSource::retained_source_slot_bytes(),
        0,
    );
    let baseline_budget =
        content_budget_for_rows_with_shared_bytes(&problem, source, baseline_shared_bytes, 1, 1);
    let baseline = BoundObservationSource::open_with_measures(
        &problem,
        source,
        &source_state(source),
        &baseline_measures,
        baseline_shared_bytes,
        baseline_budget,
    )
    .expect("bind baseline provider residency");

    let large_provider = Arc::new(AccountedTestMeasures::with_heap_bytes(128 * 1_024));
    let erased_provider: Arc<dyn MeasuresProvider> = large_provider;
    let large_measures = super::SelectedObservationMeasures::new(erased_provider)
        .expect("account large provider residency");
    let large_shared_bytes = selected_observation_shared_bytes(
        &large_measures,
        BoundObservationSource::retained_source_slot_bytes(),
        0,
    );
    assert!(matches!(
        BoundObservationSource::open_with_measures(
            &problem,
            source,
            &source_state(source),
            &large_measures,
            large_shared_bytes,
            baseline_budget,
        ),
        Err(super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                | super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
        ))
    ));

    let large_budget =
        content_budget_for_rows_with_shared_bytes(&problem, source, large_shared_bytes, 1, 1);
    let large = BoundObservationSource::open_with_measures(
        &problem,
        source,
        &source_state(source),
        &large_measures,
        large_shared_bytes,
        large_budget,
    )
    .expect("bind admitted large provider residency");
    assert_eq!(
        large.content_plan().retained_bytes() - baseline.content_plan().retained_bytes(),
        large_measures.retained_bytes() - baseline_measures.retained_bytes(),
        "the shared provider allocation must have one exact retained owner"
    );
    assert!(large.content_plan().maximum_resident_bytes() <= large_budget.available_bytes());
    assert_eq!(
        large
            .selected_samples(&problem)
            .expect("prepare accounted provider traversal")
            .collect::<Result<Vec<_>, _>>()
            .expect("complete accounted provider traversal")
            .len(),
        8
    );
}

#[test]
fn retained_source_slots_are_charged_once_and_rejected_under_a_tight_budget() {
    let directory = tempfile::tempdir().expect("temporary source-slot budget fixture");
    let path = directory.path().join("source-slot-budget.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let slot_allocation_bytes = Vec::<BoundObservationSource>::with_capacity(1)
        .capacity()
        .checked_mul(BoundObservationSource::retained_source_slot_bytes())
        .expect("finite source-slot allocation");

    let omitted_measures = test_measures(&problem);
    let omitted_shared_bytes = selected_observation_shared_bytes(
        &omitted_measures,
        0,
        single_binding_graph_initialization_bytes(source),
    );
    let omitted_budget =
        content_budget_for_rows_with_shared_bytes(&problem, source, omitted_shared_bytes, 1, 1);
    let omitted_error = match BoundSelectedObservation::open(
        &problem,
        omitted_measures,
        vec![ObservationSourceBinding::new(
            source_state(source),
            omitted_budget,
        )],
    ) {
        Ok(_) => panic!("a budget omitting the source-slot allocation must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        omitted_error,
        super::BoundSelectedObservationError::Source { error, .. }
            if matches!(
                *error,
                super::BoundObservationSourceError::ContentPlan(
                    super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                        | super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
                )
            )
    ));

    let admitted_measures = test_measures(&problem);
    let measures_retained_bytes = admitted_measures.retained_bytes();
    let admitted_shared_bytes = selected_observation_shared_bytes(
        &admitted_measures,
        slot_allocation_bytes,
        single_binding_graph_initialization_bytes(source),
    );
    let admitted_budget =
        content_budget_for_rows_with_shared_bytes(&problem, source, admitted_shared_bytes, 1, 1);
    let admitted = BoundSelectedObservation::open(
        &problem,
        admitted_measures,
        vec![ObservationSourceBinding::new(
            source_state(source),
            admitted_budget,
        )],
    )
    .expect("bind an exactly admitted source-slot allocation");
    assert_eq!(
        admitted.source_slot_allocation_bytes(),
        slot_allocation_bytes,
        "the projection must use the retained Vec's actual slot capacity"
    );

    let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())
        .expect("open fixture for source-slot accounting comparison");
    let without_slots = super::content_plan::selected_content_plan(
        &measurement_set,
        &problem,
        source,
        super::content_plan::SelectedObservationSharedBytes::new(measures_retained_bytes, 0, 0),
        admitted_budget,
    )
    .expect("plan the same retained state without the source-slot owner");
    let bound_plan = admitted
        .source_content_plan(0)
        .expect("bound source content plan");
    assert_eq!(
        bound_plan.retained_bytes() - without_slots.retained_bytes(),
        slot_allocation_bytes,
        "source identity, owner headers, content plan, and inline padding are charged once"
    );
    assert!(bound_plan.maximum_resident_bytes() <= admitted_budget.available_bytes());
}

#[test]
fn consumed_binding_graph_is_charged_once_at_actual_capacity_under_a_tight_budget() {
    let directory = tempfile::tempdir().expect("temporary binding-graph budget fixture");
    let path = directory.path().join("binding-graph-budget.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let source_slot_bytes = Vec::<BoundObservationSource>::with_capacity(1)
        .capacity()
        .checked_mul(BoundObservationSource::retained_source_slot_bytes())
        .expect("finite source-slot allocation");

    let mut omitted_bindings = Vec::<ObservationSourceBinding>::with_capacity(4_096);
    let omitted_state = source_state(source);
    let omitted_binding_graph_bytes = expected_binding_graph_initialization_bytes(
        std::slice::from_ref(source),
        std::slice::from_ref(&omitted_state),
        omitted_bindings.capacity(),
    );
    let omitted_measures = test_measures(&problem);
    let omitted_shared_bytes =
        selected_observation_shared_bytes(&omitted_measures, source_slot_bytes, 0);
    let omitted_budget =
        content_budget_for_rows_with_shared_bytes(&problem, source, omitted_shared_bytes, 1, 1);
    omitted_bindings.push(ObservationSourceBinding::new(omitted_state, omitted_budget));
    let omitted_error =
        match BoundSelectedObservation::open(&problem, omitted_measures, omitted_bindings) {
            Ok(_) => panic!("a tight budget omitting the live binding graph must be rejected"),
            Err(error) => error,
        };
    assert!(matches!(
        omitted_error,
        super::BoundSelectedObservationError::Source { error, .. }
            if matches!(
                *error,
                super::BoundObservationSourceError::ContentPlan(
                    super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                )
            )
    ));

    let mut admitted_bindings = Vec::<ObservationSourceBinding>::with_capacity(4_096);
    let admitted_state = source_state(source);
    let binding_graph_bytes = expected_binding_graph_initialization_bytes(
        std::slice::from_ref(source),
        std::slice::from_ref(&admitted_state),
        admitted_bindings.capacity(),
    );
    assert_eq!(binding_graph_bytes, omitted_binding_graph_bytes);
    let admitted_measures = test_measures(&problem);
    let measures_retained_bytes = admitted_measures.retained_bytes();
    let admitted_shared_bytes = selected_observation_shared_bytes(
        &admitted_measures,
        source_slot_bytes,
        binding_graph_bytes,
    );
    let admitted_budget =
        content_budget_for_rows_with_shared_bytes(&problem, source, admitted_shared_bytes, 1, 1);
    admitted_bindings.push(ObservationSourceBinding::new(
        admitted_state,
        admitted_budget,
    ));
    let admitted = BoundSelectedObservation::open(&problem, admitted_measures, admitted_bindings)
        .expect("bind an exactly admitted oversized binding graph");

    let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())
        .expect("open fixture for binding-slot accounting comparison");
    let without_binding_graph = super::content_plan::selected_content_plan(
        &measurement_set,
        &problem,
        source,
        super::content_plan::SelectedObservationSharedBytes::new(
            measures_retained_bytes,
            source_slot_bytes,
            0,
        ),
        admitted_budget,
    )
    .expect("plan the same initialization without the consumed binding graph");
    let bound_plan = admitted
        .source_content_plan(0)
        .expect("bound source content plan");
    assert_eq!(
        bound_plan.initialization_scratch_bytes()
            - without_binding_graph.initialization_scratch_bytes(),
        binding_graph_bytes,
        "the complete consumed binding graph must be charged exactly once"
    );
    assert_eq!(
        bound_plan.retained_bytes(),
        without_binding_graph.retained_bytes(),
        "the consumed binding graph is not retained after initialization"
    );
    assert!(bound_plan.maximum_resident_bytes() <= admitted_budget.available_bytes());
}

#[test]
fn later_binding_generation_allocations_are_included_in_the_once_only_graph_peak() {
    let directory = tempfile::tempdir().expect("temporary multi-binding graph fixture");
    let first_path = directory.path().join("first-binding-graph.ms");
    let second_path = directory.path().join("second-binding-graph.ms");
    generate_fixture(&first_path);
    generate_fixture(&second_path);
    let problem = compiled_problem_with_sources(&[(&first_path, 1, 2), (&second_path, 2, 2)]);
    let sources = problem.inputs().observation_snapshot().sources();
    let source_slot_bytes = Vec::<BoundObservationSource>::with_capacity(sources.len())
        .capacity()
        .checked_mul(BoundObservationSource::retained_source_slot_bytes())
        .expect("finite multi-source slot allocation");

    let first_omitted_state = source_state(&sources[0]);
    let (second_omitted_state, second_generation_bytes) =
        source_state_with_generation_capacity(&sources[1], 8_192);
    let omitted_states = [first_omitted_state, second_omitted_state];
    let mut omitted_bindings = Vec::<ObservationSourceBinding>::with_capacity(sources.len());
    let full_binding_graph_bytes = expected_binding_graph_initialization_bytes(
        sources,
        &omitted_states,
        omitted_bindings.capacity(),
    );
    let graph_without_second_generations = full_binding_graph_bytes
        .checked_sub(second_generation_bytes)
        .expect("second generation allocations belong to the live graph");
    assert!(
        second_generation_bytes
            > sources[1]
                .generations()
                .retained_manifest_bytes()
                .expect("compiled second-source generation manifest"),
        "the second binding must retain deliberately oversized generation capacities"
    );

    let omitted_measures = test_measures(&problem);
    let first_omitted_budget = content_budget_for_rows_with_shared_bytes(
        &problem,
        &sources[0],
        selected_observation_shared_bytes(
            &omitted_measures,
            source_slot_bytes,
            graph_without_second_generations,
        ),
        1,
        1,
    );
    let second_omitted_budget = content_budget_for_rows_with_shared_bytes(
        &problem,
        &sources[1],
        super::content_plan::SelectedObservationSharedBytes::NONE,
        1,
        1,
    );
    let [first_omitted_state, second_omitted_state] = omitted_states;
    omitted_bindings.push(ObservationSourceBinding::new(
        first_omitted_state,
        first_omitted_budget,
    ));
    omitted_bindings.push(ObservationSourceBinding::new(
        second_omitted_state,
        second_omitted_budget,
    ));
    let omitted_error =
        match BoundSelectedObservation::open(&problem, omitted_measures, omitted_bindings) {
            Ok(_) => panic!("uncharged later-binding generation capacity must be rejected"),
            Err(error) => error,
        };
    assert!(matches!(
        omitted_error,
        super::BoundSelectedObservationError::Source {
            measurement_set,
            error,
        } if measurement_set == sources[0].identity()
            && matches!(
                *error,
                super::BoundObservationSourceError::ContentPlan(
                    super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                )
            )
    ));

    let first_admitted_state = source_state(&sources[0]);
    let (second_admitted_state, admitted_second_generation_bytes) =
        source_state_with_generation_capacity(&sources[1], 8_192);
    assert_eq!(admitted_second_generation_bytes, second_generation_bytes);
    let admitted_states = [first_admitted_state, second_admitted_state];
    let mut admitted_bindings = Vec::<ObservationSourceBinding>::with_capacity(sources.len());
    let admitted_graph_bytes = expected_binding_graph_initialization_bytes(
        sources,
        &admitted_states,
        admitted_bindings.capacity(),
    );
    assert_eq!(admitted_graph_bytes, full_binding_graph_bytes);
    let admitted_measures = test_measures(&problem);
    let measures_retained_bytes = admitted_measures.retained_bytes();
    let first_admitted_budget = content_budget_for_rows_with_shared_bytes(
        &problem,
        &sources[0],
        selected_observation_shared_bytes(
            &admitted_measures,
            source_slot_bytes,
            admitted_graph_bytes,
        ),
        1,
        1,
    );
    let second_admitted_budget = content_budget_for_rows_with_shared_bytes(
        &problem,
        &sources[1],
        super::content_plan::SelectedObservationSharedBytes::NONE,
        1,
        1,
    );
    let [first_admitted_state, second_admitted_state] = admitted_states;
    admitted_bindings.push(ObservationSourceBinding::new(
        first_admitted_state,
        first_admitted_budget,
    ));
    admitted_bindings.push(ObservationSourceBinding::new(
        second_admitted_state,
        second_admitted_budget,
    ));
    let admitted = BoundSelectedObservation::open(&problem, admitted_measures, admitted_bindings)
        .expect("admit the complete multi-binding graph exactly once");

    let measurement_set = MeasurementSet::open_retained_read(sources[0].provenance().locator())
        .expect("open first source for graph accounting comparison");
    let without_binding_graph = super::content_plan::selected_content_plan(
        &measurement_set,
        &problem,
        &sources[0],
        super::content_plan::SelectedObservationSharedBytes::new(
            measures_retained_bytes,
            source_slot_bytes,
            0,
        ),
        first_admitted_budget,
    )
    .expect("plan the first source without the shared binding graph");
    let bound_plan = admitted
        .source_content_plan(0)
        .expect("bound first-source content plan");
    assert_eq!(
        bound_plan.initialization_scratch_bytes()
            - without_binding_graph.initialization_scratch_bytes(),
        admitted_graph_bytes,
        "outer slots and every nested binding allocation are charged once at the first peak"
    );
    assert!(bound_plan.maximum_resident_bytes() <= first_admitted_budget.available_bytes());
}

#[test]
fn retained_opened_table_metadata_is_charged_once_for_oversized_variable_references() {
    let directory = tempfile::tempdir().expect("temporary oversized-MEASINFO fixture");
    let path = directory.path().join("oversized-measinfo.ms");
    generate_fixture(&path);
    let centres = CentreLaws::new(
        PhaseCentreLaw::Observation,
        DelayCentreLaw::PhaseTrackingCentre,
        PointingCentreLaw::Observation(ObservationPointingLaw::new(
            PointingDirectionColumn::Direction,
            PointingDirectionSemantic::AntennaBoresight,
            PointingTimeSampling::VisibilityTime,
            PointingInterpolation::Nearest,
            PointingExtrapolation::HoldNearest,
            MissingPointingPolicy::Reject,
        )),
    );
    let baseline_problem = compiled_problem_with_centres(&path, 2, centres.clone());
    let baseline_source = &baseline_problem.inputs().observation_snapshot().sources()[0];
    let baseline_budget = content_budget_for_rows(&baseline_problem, baseline_source, 1, 1);
    let baseline = BoundObservationSource::open(
        &baseline_problem,
        baseline_source,
        &source_state(baseline_source),
        baseline_budget,
    )
    .expect("bind baseline POINTING source");
    let baseline_retained_bytes = baseline.content_plan().retained_bytes();
    drop(baseline);
    let baseline_storage_bytes = MeasurementSet::open_retained_read(&path)
        .expect("open baseline retained MeasurementSet")
        .retained_read_metadata_bytes()
        .expect("project baseline retained MeasurementSet");

    const REFERENCE_COUNT: usize = 2_048;
    let mut measurement_set = MeasurementSet::open(&path).expect("open metadata fixture");
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        let table = pointing.table_mut();
        table
            .add_column(
                ColumnSchema::scalar("DIRECTION_REF", PrimitiveType::Int32),
                Some(Value::Scalar(ScalarValue::Int32(0))),
            )
            .expect("add variable reference column");
        let mut keywords = table
            .column_keywords("DIRECTION")
            .cloned()
            .expect("DIRECTION keywords");
        keywords.upsert(
            "MEASINFO",
            Value::Record(RecordValue::new(vec![
                RecordField::new(
                    "type",
                    Value::Scalar(ScalarValue::String("direction".to_string())),
                ),
                RecordField::new(
                    "VarRefCol",
                    Value::Scalar(ScalarValue::String("DIRECTION_REF".to_string())),
                ),
                RecordField::new(
                    "TabRefTypes",
                    Value::Array(ArrayValue::from_string_vec(vec![
                        "J2000".to_string();
                        REFERENCE_COUNT
                    ])),
                ),
                RecordField::new(
                    "TabRefCodes",
                    Value::Array(ArrayValue::from_i32_vec(
                        (0..REFERENCE_COUNT)
                            .map(|code| i32::try_from(code).expect("reference code fits i32"))
                            .collect(),
                    )),
                ),
            ])),
        );
        table.set_column_keywords("DIRECTION", keywords);
    }
    measurement_set.save().expect("save oversized MEASINFO");

    let problem = compiled_problem_with_centres(&path, 2, centres);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    assert!(matches!(
        BoundObservationSource::open(&problem, source, &source_state(source), baseline_budget,),
        Err(super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                | super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
        ))
    ));

    let inflated_budget = content_budget_for_rows(&problem, source, 1, 1);
    let inflated =
        BoundObservationSource::open(&problem, source, &source_state(source), inflated_budget)
            .expect("bind oversized variable-reference source");
    let inflated_storage_bytes = MeasurementSet::open_retained_read(&path)
        .expect("open inflated retained MeasurementSet")
        .retained_read_metadata_bytes()
        .expect("project inflated retained MeasurementSet");
    assert_eq!(
        inflated.content_plan().retained_bytes() - baseline_retained_bytes,
        inflated_storage_bytes - baseline_storage_bytes,
        "the opened MeasurementSet object graph must be the sole retained owner of persisted MEASINFO"
    );
    assert_eq!(
        inflated.content_plan().pointing_reference_scratch_bytes(),
        "DIRECTION_REF".len() + size_of::<Option<ScalarValue>>(),
        "borrowed TabRefTypes and TabRefCodes leave only the selected integer cell as scratch"
    );
    assert!(inflated.content_plan().maximum_resident_bytes() <= inflated_budget.available_bytes());
    let samples = inflated
        .selected_samples(&problem)
        .expect("prepare oversized variable-reference source")
        .collect::<Result<Vec<_>, _>>()
        .expect("evaluate borrowed TabRefTypes and TabRefCodes");
    assert_eq!(samples.len(), 8);
    assert_eq!(
        inflated.retained_storage_metadata_bytes(),
        Some(inflated_storage_bytes),
        "bounded traversal must not populate an uncharged retained table cache"
    );
}

#[test]
fn variable_pointing_reference_string_scratch_is_charged_once_per_peak() {
    let directory = tempfile::tempdir().expect("temporary variable-reference fixture");
    let path = directory.path().join("variable-reference.ms");
    generate_fixture(&path);
    let reference_column = format!("DIRECTION_REF_{}", "X".repeat(8_192));
    let mut measurement_set = MeasurementSet::open(&path).expect("open POINTING fixture");
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        let table = pointing.table_mut();
        table
            .add_column(
                ColumnSchema::scalar(&reference_column, PrimitiveType::String),
                Some(Value::Scalar(ScalarValue::String("J2000".to_string()))),
            )
            .expect("add string reference column");
        let mut keywords = table
            .column_keywords("DIRECTION")
            .cloned()
            .expect("DIRECTION keywords");
        keywords.upsert(
            "MEASINFO",
            Value::Record(RecordValue::new(vec![
                RecordField::new(
                    "type",
                    Value::Scalar(ScalarValue::String("direction".to_string())),
                ),
                RecordField::new(
                    "VarRefCol",
                    Value::Scalar(ScalarValue::String(reference_column.clone())),
                ),
            ])),
        );
        table.set_column_keywords("DIRECTION", keywords);
    }
    measurement_set
        .save()
        .expect("save string variable-reference POINTING metadata");

    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTime,
                PointingInterpolation::Nearest,
                PointingExtrapolation::HoldNearest,
                MissingPointingPolicy::Reject,
            )),
        ),
    );
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let one_row_budget = content_budget_for_rows(&problem, source, 1, 1);
    let two_row_budget = content_budget_for_rows(&problem, source, 2, 1);
    let one_row =
        BoundObservationSource::open(&problem, source, &source_state(source), one_row_budget)
            .expect("bind one-row variable-reference source");
    let two_rows =
        BoundObservationSource::open(&problem, source, &source_state(source), two_row_budget)
            .expect("bind two-row variable-reference source");
    let expected_scratch = reference_column
        .len()
        .checked_add("J2000".len())
        .and_then(|bytes| bytes.checked_add(size_of::<Option<ScalarValue>>()))
        .expect("variable-reference scratch fits usize");
    assert_eq!(
        one_row.content_plan().pointing_reference_scratch_bytes(),
        expected_scratch
    );
    assert_eq!(
        two_rows.content_plan().pointing_reference_scratch_bytes(),
        expected_scratch,
        "one-at-a-time string scratch must not be multiplied by block rows"
    );
    assert_eq!(
        two_rows.content_plan().preparation_bytes_per_block(),
        2 * one_row.content_plan().preparation_bytes_per_block(),
        "the separately charged reference scratch must not leak into per-row payload"
    );
    assert!(matches!(
        BoundObservationSource::open(
            &problem,
            source,
            &source_state(source),
            SelectedObservationContentBudget::new(
                one_row_budget.available_bytes() - expected_scratch,
                1,
                4,
            ),
        ),
        Err(super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                | super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
        ))
    ));
    assert!(two_rows.content_plan().maximum_resident_bytes() <= two_row_budget.available_bytes());
    let retained_storage_bytes = two_rows
        .retained_storage_metadata_bytes()
        .expect("project retained string-reference storage");
    let samples = two_rows
        .selected_samples(&problem)
        .expect("prepare variable-string reference source")
        .collect::<Result<Vec<_>, _>>()
        .expect("evaluate bounded variable-string references");
    assert_eq!(samples.len(), 8);
    assert_eq!(
        two_rows.retained_storage_metadata_bytes(),
        Some(retained_storage_bytes),
        "variable reference reads must not create hidden retained table state"
    );
}

#[test]
fn retained_predicate_catalog_is_charged_before_construction() {
    let directory = tempfile::tempdir().expect("temporary predicate-budget fixture");
    let path = directory.path().join("predicate-budget.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let admitted = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .expect("bind source with predicate allowance");
    assert_eq!(
        admitted.predicate_row_manifest_ptr(),
        Some(source.selection().rows().ordered_main_rows().as_ptr()),
        "the retained predicate must share the compiler-owned row manifest"
    );
    let predicate_bytes =
        super::row_selection::CompiledRowPredicate::shared_retained_heap_bytes(source)
            .expect("finite predicate projection");
    let old_unaccounted_budget = admitted
        .content_plan()
        .maximum_resident_bytes()
        .checked_sub(predicate_bytes)
        .expect("predicate contributes retained bytes");

    assert!(matches!(
        BoundObservationSource::open(
            &problem,
            source,
            &source_state(source),
            SelectedObservationContentBudget::new(old_unaccounted_budget, 1, 4),
        ),
        Err(super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
        ))
    ));
}

#[test]
fn post_compile_source_generation_changes_are_rejected_before_planning_or_streaming() {
    let directory = tempfile::tempdir().expect("temporary stale-generation fixture");
    let path = directory.path().join("stale-generations.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let changed_generation = identity(201);
    let changes = [
        (
            "DATA",
            generations_with_changed_column(source, MsColumnKind::Data, changed_generation),
        ),
        (
            "FLAG",
            generations_with_changed_column(source, MsColumnKind::Flag, changed_generation),
        ),
        (
            "WEIGHT",
            generations_with_changed_column(source, MsColumnKind::Weight, changed_generation),
        ),
        (
            "POINTING metadata",
            generations_with_changed_metadata(
                source,
                MetadataTableKind::Pointing,
                changed_generation,
            ),
        ),
        (
            "consistency token",
            SourceGenerations::new(
                ConsistencyToken::new(changed_generation),
                source.generations().columns().clone(),
                source.generations().metadata_generations().to_vec(),
                source.generations().model_column(),
            ),
        ),
    ];

    for (changed, generations) in changes {
        let current = ObservationSourceState::new(
            source.identity(),
            source.selection().rows().clone(),
            generations,
        );
        let error = match BoundObservationSource::open(
            &problem,
            source,
            &current,
            SelectedObservationContentBudget::new(1, 1, 4),
        ) {
            Ok(_) => panic!("a post-compile generation change must fail before budget admission"),
            Err(error) => error,
        };
        assert!(
            matches!(
                error,
                super::BoundObservationSourceError::StaleSourceGenerations
            ),
            "{changed} change returned {error:?}"
        );
    }
}

#[test]
fn post_compile_flag_storage_mutation_with_fresh_generation_is_rejected() {
    let directory = tempfile::tempdir().expect("temporary mutated-FLAG fixture");
    let path = directory.path().join("mutated-flag.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];

    let mut measurement_set = MeasurementSet::open(&path).expect("open fixture for FLAG mutation");
    let mut flags = match measurement_set
        .main_table()
        .cell_accessor(0, "FLAG")
        .and_then(|cell| cell.array())
        .expect("read compiled FLAG cell")
        .clone()
    {
        ArrayValue::Bool(flags) => flags,
        other => panic!("FLAG must be Bool, found {:?}", other.primitive_type()),
    };
    let first = flags.iter_mut().next().expect("nonempty FLAG cell");
    *first = !*first;
    measurement_set
        .main_table_mut()
        .cell_accessor_mut(0, "FLAG")
        .expect("open FLAG cell for mutation")
        .set(Value::Array(ArrayValue::Bool(flags)))
        .expect("mutate FLAG after compilation");
    measurement_set
        .save()
        .expect("persist post-compile FLAG mutation");
    drop(measurement_set);

    let current = ObservationSourceState::new(
        source.identity(),
        source.selection().rows().clone(),
        generations_with_changed_column(source, MsColumnKind::Flag, identity(202)),
    );
    let error = match BoundObservationSource::open(
        &problem,
        source,
        &current,
        SelectedObservationContentBudget::new(1, 1, 4),
    ) {
        Ok(_) => panic!("fresh FLAG generation must reject mutated storage before planning"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        super::BoundObservationSourceError::StaleSourceGenerations
    ));
}

#[test]
fn post_compile_pointing_storage_mutation_with_fresh_generation_is_rejected() {
    let directory = tempfile::tempdir().expect("temporary mutated-POINTING fixture");
    let path = directory.path().join("mutated-pointing.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];

    let mut measurement_set =
        MeasurementSet::open(&path).expect("open fixture for POINTING mutation");
    measurement_set
        .pointing_mut()
        .expect("POINTING subtable")
        .set_array(0, "DIRECTION", direction_array([0.125, -0.25]))
        .expect("mutate POINTING after compilation");
    measurement_set
        .save()
        .expect("persist post-compile POINTING mutation");
    drop(measurement_set);

    let current = ObservationSourceState::new(
        source.identity(),
        source.selection().rows().clone(),
        generations_with_changed_metadata(source, MetadataTableKind::Pointing, identity(203)),
    );
    let error = match BoundObservationSource::open(
        &problem,
        source,
        &current,
        SelectedObservationContentBudget::new(1, 1, 4),
    ) {
        Ok(_) => panic!("fresh POINTING generation must reject mutated metadata before planning"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        super::BoundObservationSourceError::StaleSourceGenerations
    ));
}

#[test]
fn post_compile_selected_row_change_is_rejected_before_streaming() {
    let directory = tempfile::tempdir().expect("temporary stale-row fixture");
    let path = directory.path().join("stale-rows.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let changed_rows = SelectedRows::from_ordered_main_rows(2, [SelectedMainRow::new(0, 0)])
        .expect("changed current row manifest");
    let current = ObservationSourceState::new(
        source.identity(),
        changed_rows,
        source.generations().clone(),
    );

    let error = match BoundObservationSource::open(
        &problem,
        source,
        &current,
        SelectedObservationContentBudget::new(1, 1, 4),
    ) {
        Ok(_) => panic!("a post-compile selection change must fail before budget admission"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        super::BoundObservationSourceError::StaleSelectedRows
    ));
}

#[test]
fn frontend_row_projection_uses_the_canonical_bounded_observation_evaluator() {
    let directory = tempfile::tempdir().expect("temporary row-projection fixture");
    let path = directory.path().join("row-projection.ms");
    generate_fixture(&path);
    let measurement_set = MeasurementSet::open(&path).expect("open row-projection fixture");
    let selection = measurement_set
        .selected_observation_row_selection(&[0], None, None, None)
        .expect("resolve frontend selectors to the native row contract");
    let mut rows = Vec::new();

    measurement_set
        .visit_selected_observation_rows(
            &selection,
            MsSelectionIoBudget {
                available_bytes: 2 * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                maximum_live_blocks: 2,
                requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                storage_alignment_rows: None,
            },
            |row| rows.push(row),
        )
        .expect("visit canonical selected rows");

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| (row.physical_row(), row.data_description_id()))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 0)]
    );
    assert!(rows.iter().all(|row| !row.flag_row()));
}

#[test]
fn terminal_poll_failure_prevents_owner_minted_completion() {
    let directory = tempfile::tempdir().expect("temporary terminal-poll fixture");
    let path = directory.path().join("terminal-poll.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let bound = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 2, 1),
    )
    .expect("bind terminal-poll source");
    let exact = bound
        .selected_samples(&problem)
        .expect("prepare exact source stream");
    let terminal_failure = exact.chain(std::iter::once(Err(
        super::BoundObservationSourceError::InvalidRowGeometry,
    )));
    let mut consumed = 0_usize;

    let error = consume_validated_stream(&problem, terminal_failure, |_| {
        consumed += 1;
        Ok::<_, Infallible>(())
    })
    .expect_err("a source error on the terminal poll must prevent completion");

    assert_eq!(consumed, 8, "all exact values precede the terminal failure");
    assert!(matches!(
        error,
        SelectedObservationTraversalError::Source(
            super::BoundObservationSourceError::InvalidRowGeometry
        )
    ));
}

#[test]
fn row_manifest_validation_occurs_in_the_sole_value_traversal() {
    let directory = tempfile::tempdir().expect("temporary one-pass fixture");
    let path = directory.path().join("one-pass.ms");
    generate_fixture(&path);
    let measurement_set = MeasurementSet::open(&path).expect("open one-pass fixture");
    let first_time = main_time_mjd_seconds(&measurement_set, 0);
    let selected_rows = SelectedRows::from_ordered_main_rows(1, [SelectedMainRow::new(0, 0)])
        .expect("stale one-row manifest");
    let first_row_filter = RowSelection::new(
        IdSelection::All,
        TimeSelection::Ranges(vec![TimeRange::new(
            Some(SelectionBound::inclusive(first_time)),
            Some(SelectionBound::inclusive(first_time)),
        )]),
        UvSelection::All,
        AntennaSelection::All,
        IdSelection::All,
        IdSelection::All,
        IntentSelection::All,
        IdSelection::All,
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source_input_with_selected_rows_and_filter(
            &path,
            1,
            selected_rows,
            first_row_filter,
        )],
        vec![(ReferenceDataKind::Measures, identity(90))],
        ModelStateIdentity::Empty,
    ))
    .expect("compile stale one-row observation");
    let problem = compile(ImagingRequest::new(
        specification(),
        geometry(),
        ProblemInputIdentities::new(snapshot.clone()),
        model_lifecycle(snapshot.model()),
    ))
    .expect("compile one-pass problem");
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let binding = ObservationSourceBinding::new(
        source_state(source),
        bound_content_budget_for_rows(&problem, source, 1, 1),
    );

    let mut bound =
        BoundSelectedObservation::open(&problem, test_measures(&problem), vec![binding])
            .expect("binding must not hide a preliminary MAIN traversal");
    let mut consumed = 0_usize;
    let error = bound
        .traverse(&problem, |_| {
            consumed += 1;
            Ok::<_, Infallible>(())
        })
        .expect_err("the authoritative traversal must reject the stale row manifest");

    assert_eq!(
        consumed, 4,
        "the mismatching second row is validated before reaching the consumer"
    );
    assert!(matches!(
        error,
        SelectedObservationTraversalError::Source(
            super::BoundObservationSourceError::SourceRowCountMismatch
        )
    ));
}

#[test]
fn selected_observation_residency_charges_cardinality_and_is_schedule_invariant() {
    let directory = tempfile::tempdir().expect("temporary residency fixtures");
    let small_path = directory.path().join("small.ms");
    let large_path = directory.path().join("large.ms");
    generate_fixture_with_rows(&small_path, 4);
    generate_fixture_with_rows(&large_path, 64);

    let small_problem = compiled_problem(&small_path, 4);
    let small_source = &small_problem.inputs().observation_snapshot().sources()[0];
    let synchronous_budget = content_budget_for_rows(&small_problem, small_source, 1, 1);
    let double_buffered_budget = content_budget_for_rows(&small_problem, small_source, 1, 2);
    let synchronous = BoundObservationSource::open(
        &small_problem,
        small_source,
        &source_state(small_source),
        synchronous_budget,
    )
    .expect("bind synchronous selected observation");
    let double_buffered = BoundObservationSource::open(
        &small_problem,
        small_source,
        &source_state(small_source),
        double_buffered_budget,
    )
    .expect("bind double-buffered selected observation");
    assert_eq!(synchronous.content_plan().rows_per_block(), 1);
    assert_eq!(double_buffered.content_plan().rows_per_block(), 1);
    assert_eq!(synchronous.content_plan().maximum_live_blocks(), 1);
    assert_eq!(double_buffered.content_plan().maximum_live_blocks(), 2);
    assert!(
        synchronous.content_plan().maximum_resident_bytes() <= synchronous_budget.available_bytes()
    );
    assert!(
        double_buffered.content_plan().maximum_resident_bytes()
            <= double_buffered_budget.available_bytes()
    );

    let synchronous_samples = synchronous
        .selected_samples(&small_problem)
        .expect("prepare synchronous replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read synchronous replay");
    let mut double_buffered_stream = double_buffered
        .selected_samples(&small_problem)
        .expect("prepare double-buffered replay");
    let mut double_buffered_samples = vec![
        double_buffered_stream
            .next()
            .expect("double-buffered replay has a first sample")
            .expect("read first double-buffered sample"),
    ];
    assert_eq!(
        double_buffered_stream.scheduling_state(),
        (Some(0), vec![(1, 1)], 2),
        "the owner must have a distinct second block resident ahead of the active block"
    );
    for _ in 0..3 {
        double_buffered_samples.push(
            double_buffered_stream
                .next()
                .expect("first row has four selected samples")
                .expect("read first-row sample"),
        );
    }
    double_buffered_samples.push(
        double_buffered_stream
            .next()
            .expect("double-buffered replay reaches its second row")
            .expect("read second-row sample"),
    );
    assert_eq!(
        double_buffered_stream.scheduling_state(),
        (Some(1), vec![(0, 2)], 2),
        "consumption must alternate buffers while the released slot reads ahead"
    );
    double_buffered_samples.extend(
        double_buffered_stream
            .collect::<Result<Vec<_>, _>>()
            .expect("read remaining double-buffered replay"),
    );
    assert_eq!(synchronous_samples, double_buffered_samples);
    assert_eq!(
        inspect_samples(&small_problem, synchronous_samples).expect("inspect synchronous replay"),
        inspect_samples(&small_problem, double_buffered_samples)
            .expect("inspect double-buffered replay")
    );

    let synchronous_plan = ObservationSourceBinding::new(
        source_state(small_source),
        bound_content_budget_for_rows(&small_problem, small_source, 1, 1),
    );
    let double_buffered_plan = ObservationSourceBinding::new(
        source_state(small_source),
        bound_content_budget_for_rows(&small_problem, small_source, 1, 2),
    );
    let mut synchronous_observation = BoundSelectedObservation::open(
        &small_problem,
        test_measures(&small_problem),
        vec![synchronous_plan],
    )
    .expect("bind synchronous owner traversal");
    let synchronous_completion = synchronous_observation
        .traverse(&small_problem, |_| Ok::<_, Infallible>(()))
        .expect("complete synchronous owner traversal");
    let mut double_buffered_observation = BoundSelectedObservation::open(
        &small_problem,
        test_measures(&small_problem),
        vec![double_buffered_plan],
    )
    .expect("bind double-buffered owner traversal");
    let double_buffered_completion = double_buffered_observation
        .traverse(&small_problem, |_| Ok::<_, Infallible>(()))
        .expect("complete double-buffered owner traversal");
    assert_eq!(
        synchronous_completion.generation_id(),
        double_buffered_completion.generation_id(),
        "read-ahead scheduling and alternating physical buffers are absent from content identity"
    );
    assert_eq!(
        synchronous_completion.sample_count(),
        double_buffered_completion.sample_count()
    );

    let large_problem = compiled_problem(&large_path, 64);
    let large_source = &large_problem.inputs().observation_snapshot().sources()[0];
    let large_budget = content_budget_for_rows(&large_problem, large_source, 1, 1);
    let large = BoundObservationSource::open(
        &large_problem,
        large_source,
        &source_state(large_source),
        large_budget,
    )
    .expect("bind large selected observation");
    assert_eq!(
        large.content_plan().bytes_per_row(),
        synchronous.content_plan().bytes_per_row(),
        "MAIN and POINTING table cardinality must not enter simultaneous residency"
    );
    assert_eq!(
        large.content_plan().bytes_per_block(),
        synchronous.content_plan().bytes_per_block()
    );
    assert!(
        large_source
            .selection()
            .rows()
            .retained_manifest_bytes()
            .expect("large retained row manifest byte count")
            > small_source
                .selection()
                .rows()
                .retained_manifest_bytes()
                .expect("small retained row manifest byte count")
    );
    assert!(large.content_plan().retained_bytes() > synchronous.content_plan().retained_bytes());
    assert_eq!(
        large.content_plan().initialization_scratch_bytes(),
        synchronous.content_plan().initialization_scratch_bytes(),
        "shared selected-row allocations are retained once, not recharged as validation scratch"
    );
    assert!(large_budget.available_bytes() > synchronous_budget.available_bytes());
    assert!(large.content_plan().maximum_resident_bytes() <= large_budget.available_bytes());
    assert!(matches!(
        BoundObservationSource::open(
            &large_problem,
            large_source,
            &source_state(large_source),
            synchronous_budget,
        ),
        Err(super::BoundObservationSourceError::ContentPlan(
            super::content_plan::SelectedObservationContentPlanError::InsufficientRetainedBudget { .. }
                | super::content_plan::SelectedObservationContentPlanError::InsufficientBudget { .. }
        ))
    ));
    assert_eq!(
        large
            .selected_samples(&large_problem)
            .expect("prepare large bounded replay")
            .try_fold(0_usize, |count, sample| sample.map(|_| count + 1))
            .expect("read large bounded replay"),
        64 * 2 * 2
    );
}

#[test]
fn retained_selected_observation_owns_canonical_multi_source_order() {
    let directory = tempfile::tempdir().expect("temporary multi-source fixture");
    let first_path = directory.path().join("first.ms");
    let second_path = directory.path().join("second.ms");
    generate_fixture(&first_path);
    generate_fixture(&second_path);
    let problem = compiled_problem_with_sources(&[(&first_path, 1, 2), (&second_path, 2, 2)]);
    let sources = problem.inputs().observation_snapshot().sources();
    let source_slot_allocation_bytes = Vec::<BoundObservationSource>::with_capacity(sources.len())
        .capacity()
        .checked_mul(BoundObservationSource::retained_source_slot_bytes())
        .expect("finite source-slot allocation");
    let binding_states: Vec<_> = sources.iter().map(source_state).collect();
    let binding_capacity = Vec::<ObservationSourceBinding>::with_capacity(sources.len()).capacity();
    let binding_graph_initialization_bytes =
        expected_binding_graph_initialization_bytes(sources, &binding_states, binding_capacity);
    let one_row_measures = test_measures(&problem);
    let one_row_bindings = sources
        .iter()
        .enumerate()
        .map(|(source_index, source)| {
            ObservationSourceBinding::new(
                source_state(source),
                content_budget_for_rows_with_shared_bytes(
                    &problem,
                    source,
                    if source_index == 0 {
                        selected_observation_shared_bytes(
                            &one_row_measures,
                            source_slot_allocation_bytes,
                            binding_graph_initialization_bytes,
                        )
                    } else {
                        super::content_plan::SelectedObservationSharedBytes::NONE
                    },
                    1,
                    1,
                ),
            )
        })
        .collect();
    let two_row_measures = test_measures(&problem);
    let mut two_row_bindings: Vec<_> = sources
        .iter()
        .enumerate()
        .map(|(source_index, source)| {
            ObservationSourceBinding::new(
                source_state(source),
                content_budget_for_rows_with_shared_bytes(
                    &problem,
                    source,
                    if source_index == 0 {
                        selected_observation_shared_bytes(
                            &two_row_measures,
                            source_slot_allocation_bytes,
                            binding_graph_initialization_bytes,
                        )
                    } else {
                        super::content_plan::SelectedObservationSharedBytes::NONE
                    },
                    2,
                    1,
                ),
            )
        })
        .collect();
    two_row_bindings.reverse();
    let mut one_row = BoundSelectedObservation::open(&problem, one_row_measures, one_row_bindings)
        .expect("bind canonical multi-source observation");
    let mut two_rows = BoundSelectedObservation::open(&problem, two_row_measures, two_row_bindings)
        .expect("bind reordered source states and budgets by typed identity");

    let shared_measures_bytes = test_measures(&problem).retained_bytes();
    for (source_index, source) in sources.iter().enumerate() {
        let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())
            .expect("open multi-source fixture for uncharged comparison");
        let uncharged = super::content_plan::selected_content_plan(
            &measurement_set,
            &problem,
            source,
            super::content_plan::SelectedObservationSharedBytes::NONE,
            content_budget_for_rows(&problem, source, 1, 1),
        )
        .expect("plan source without the shared Measures owner");
        let bound = one_row
            .source_content_plan(source_index)
            .expect("bound canonical source plan");
        assert_eq!(
            bound.retained_bytes() - uncharged.retained_bytes(),
            if source_index == 0 {
                shared_measures_bytes + one_row.source_slot_allocation_bytes()
            } else {
                0
            },
            "the provider and source-slot allocation must be charged only to the first canonical source"
        );
    }

    let mut one_row_samples = Vec::new();
    let one_row_completion = one_row
        .traverse(&problem, |sample| {
            one_row_samples.push(sample);
            Ok::<_, Infallible>(())
        })
        .expect("complete canonical multi-source traversal");
    let mut two_row_samples = Vec::new();
    let two_row_completion = two_rows
        .traverse(&problem, |sample| {
            two_row_samples.push(sample);
            Ok::<_, Infallible>(())
        })
        .expect("complete repartitioned multi-source traversal");

    assert_eq!(one_row_samples.len(), 16);
    assert_eq!(one_row_samples, two_row_samples);
    assert_eq!(one_row_completion.sample_count(), 16);
    assert_eq!(
        one_row_completion.generation_id(),
        two_row_completion.generation_id(),
        "physical source and row blocking are absent from content identity"
    );
    assert_eq!(
        one_row_completion.observation_snapshot_id(),
        problem.inputs().observation_snapshot().snapshot_id()
    );
    assert_eq!(
        one_row_completion.observation_provenance_id(),
        problem.inputs().observation_snapshot().provenance_id()
    );
    assert_eq!(
        one_row_completion.commitment_id(),
        problem.selected_observation().commitment_id()
    );
    assert_eq!(
        one_row_samples
            .chunks_exact(8)
            .map(|samples| samples[0].address.measurement_set)
            .collect::<Vec<_>>(),
        problem
            .selected_observation()
            .read_set()
            .sources()
            .iter()
            .map(|source| source.measurement_set())
            .collect::<Vec<_>>()
    );
    let repeated = one_row
        .traverse(&problem, |_| Ok::<_, Infallible>(()))
        .expect("mint a fresh completion for a repeated retained traversal");
    assert_eq!(
        one_row_completion.generation_id(),
        repeated.generation_id(),
        "content generation remains stable across attempts"
    );
    assert!(one_row_completion.precedes(&repeated));
    assert!(!one_row_completion.same_access_binding(&two_row_completion));
}

#[test]
fn retained_observation_cannot_be_rebound_to_equivalent_cross_provenance_problem() {
    let directory = tempfile::tempdir().expect("temporary provenance-binding fixture");
    let path = directory.path().join("provenance.ms");
    generate_fixture(&path);
    let compile_with_request = |selection_request| {
        let selected_rows = SelectedRows::from_ordered_main_rows(
            2,
            [SelectedMainRow::new(0, 0), SelectedMainRow::new(1, 0)],
        )
        .expect("selected provenance-test rows");
        let source = source_input_with_selected_rows_filter_and_request(
            &path,
            1,
            selected_rows,
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
            selection_request,
        );
        let snapshot = compile_observation(ObservationSnapshotInput::new(
            vec![source],
            vec![(ReferenceDataKind::Measures, identity(90))],
            ModelStateIdentity::Empty,
        ))
        .expect("compile provenance-test snapshot");
        compile(ImagingRequest::new(
            specification(),
            geometry(),
            ProblemInputIdentities::new(snapshot.clone()),
            model_lifecycle(snapshot.model()),
        ))
        .expect("compile provenance-test problem")
    };
    let first_problem = compile_with_request(identity(211));
    let second_problem = compile_with_request(identity(212));
    assert_eq!(
        first_problem.inputs().observation_snapshot().snapshot_id(),
        second_problem.inputs().observation_snapshot().snapshot_id(),
        "source provenance is deliberately absent from scientific snapshot identity"
    );
    assert_eq!(first_problem.problem_id(), second_problem.problem_id());
    assert_ne!(
        first_problem
            .inputs()
            .observation_snapshot()
            .provenance_id(),
        second_problem
            .inputs()
            .observation_snapshot()
            .provenance_id()
    );
    let source = &first_problem.inputs().observation_snapshot().sources()[0];
    let binding = ObservationSourceBinding::new(
        source_state(source),
        bound_content_budget_for_rows(&first_problem, source, 2, 1),
    );
    let mut retained = BoundSelectedObservation::open(
        &first_problem,
        test_measures(&first_problem),
        vec![binding],
    )
    .expect("bind first provenance exactly");
    let mut consumed = 0_usize;

    let error = retained
        .traverse(&second_problem, |_| {
            consumed += 1;
            Ok::<_, Infallible>(())
        })
        .expect_err("equivalent science cannot relabel retained access with new provenance");

    assert_eq!(consumed, 0);
    assert!(matches!(
        error,
        SelectedObservationTraversalError::Binding(
            super::BoundSelectedObservationError::ProblemMismatch
        )
    ));
}

#[test]
fn retained_selected_samples_evaluate_fixed_centres_and_uvw_coordinates() {
    let directory = tempfile::tempdir().expect("temporary fixed-centre fixture");
    let path = directory.path().join("fixed.ms");
    generate_fixture(&path);
    let phase = SkyDirection::new(DirectionFrame::J2000, 0.7, -0.2);
    let delay = SkyDirection::new(DirectionFrame::J2000, 0.8, -0.25);
    let pointing = SkyDirection::new(DirectionFrame::J2000, 0.9, -0.3);
    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Fixed(phase),
            DelayCentreLaw::Fixed(delay),
            PointingCentreLaw::Fixed(pointing),
        ),
    );
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let bound = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 2, 1),
    )
    .expect("bind fixed-centre source");
    let samples = bound
        .selected_samples(&problem)
        .expect("prepare fixed-centre stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("evaluate fixed-centre samples");

    assert_eq!(samples.len(), 8);
    for sample in &samples {
        assert_eq!(sample.coordinates.phase_direction, phase);
        assert_eq!(sample.coordinates.delay_direction, delay);
        assert_eq!(sample.coordinates.pointing_directions.antenna1, pointing);
        assert_eq!(sample.coordinates.pointing_directions.antenna2, pointing);
        assert_ne!(
            sample.coordinates.transformed_uvw_m,
            sample.coordinates.raw_uvw_m
        );
        assert_ne!(
            sample.coordinates.density_uvw_m,
            sample.coordinates.raw_uvw_m
        );
        assert_ne!(sample.coordinates.phase_shift_m, 0.0);
    }
    inspect_samples(&problem, samples).expect("inspect fixed-centre stream");
}

#[test]
fn retained_selected_samples_preserve_bounded_per_antenna_pointing_directions() {
    let directory = tempfile::tempdir().expect("temporary POINTING fixture");
    let path = directory.path().join("pointing.ms");
    generate_fixture(&path);
    let antenna1_pointing = [0.91, -0.31];
    let antenna2_pointing = [0.93, -0.29];
    let mut measurement_set = MeasurementSet::open(&path).expect("open POINTING fixture");
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        pointing
            .set_array(0, "DIRECTION", direction_array(antenna1_pointing))
            .expect("set antenna-0 POINTING direction");
        pointing
            .set_array(1, "DIRECTION", direction_array(antenna2_pointing))
            .expect("set antenna-1 POINTING direction");
    }
    measurement_set.save().expect("save POINTING fixture");

    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTime,
                PointingInterpolation::Nearest,
                PointingExtrapolation::HoldNearest,
                MissingPointingPolicy::Reject,
            )),
        ),
    );
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let one_row = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .expect("bind one-row POINTING stream");
    let two_rows = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 2, 1),
    )
    .expect("bind two-row POINTING stream");
    assert_eq!(one_row.content_plan().rows_per_block(), 1);
    assert_eq!(two_rows.content_plan().rows_per_block(), 2);
    assert!(
        one_row.content_plan().preparation_bytes_per_block()
            > one_row.content_plan().bytes_per_block()
    );
    let one_row_samples = one_row
        .selected_samples(&problem)
        .expect("prepare one-row POINTING stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read one-row POINTING stream");
    let two_row_samples = two_rows
        .selected_samples(&problem)
        .expect("prepare two-row POINTING stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read two-row POINTING stream");

    assert_eq!(one_row_samples, two_row_samples);
    for sample in &one_row_samples {
        assert_eq!(
            sample.coordinates.pointing_directions.antenna1,
            SkyDirection::new(
                DirectionFrame::J2000,
                antenna1_pointing[0],
                antenna1_pointing[1],
            )
        );
        assert_eq!(
            sample.coordinates.pointing_directions.antenna2,
            SkyDirection::new(
                DirectionFrame::J2000,
                antenna2_pointing[0],
                antenna2_pointing[1],
            )
        );
    }
    inspect_samples(&problem, one_row_samples).expect("inspect exact bounded POINTING stream");
}

#[test]
fn observation_pointing_missing_policy_is_explicit_and_fail_closed() {
    let directory = tempfile::tempdir().expect("temporary missing-POINTING fixture");
    let path = directory.path().join("missing-pointing.ms");
    generate_fixture(&path);
    let mut measurement_set = MeasurementSet::open(&path).expect("open POINTING fixture");
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        pointing
            .set_i32(0, "ANTENNA_ID", 98)
            .expect("detach first POINTING antenna");
        pointing
            .set_i32(1, "ANTENNA_ID", 99)
            .expect("detach second POINTING antenna");
    }
    measurement_set.save().expect("save POINTING fixture");

    let fallback_problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            observation_pointing(MissingPointingPolicy::UsePhaseTrackingCentre),
        ),
    );
    let source = &fallback_problem.inputs().observation_snapshot().sources()[0];
    let fallback = BoundObservationSource::open(
        &fallback_problem,
        source,
        &source_state(source),
        content_budget_for_rows(&fallback_problem, source, 2, 1),
    )
    .expect("bind fallback POINTING source")
    .selected_samples(&fallback_problem)
    .expect("prepare fallback POINTING stream")
    .collect::<Result<Vec<_>, _>>()
    .expect("explicitly fall back to phase centre");
    for sample in fallback {
        assert_eq!(
            sample.coordinates.pointing_directions.antenna1,
            sample.coordinates.phase_direction
        );
        assert_eq!(
            sample.coordinates.pointing_directions.antenna2,
            sample.coordinates.phase_direction
        );
    }

    let rejecting_problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            observation_pointing(MissingPointingPolicy::Reject),
        ),
    );
    let source = &rejecting_problem.inputs().observation_snapshot().sources()[0];
    let error = BoundObservationSource::open(
        &rejecting_problem,
        source,
        &source_state(source),
        content_budget_for_rows(&rejecting_problem, source, 2, 1),
    )
    .expect("bind rejecting POINTING source")
    .selected_samples(&rejecting_problem)
    .expect("prepare rejecting POINTING stream")
    .collect::<Result<Vec<_>, _>>()
    .expect_err("missing required POINTING must fail closed");
    assert!(matches!(
        error,
        super::BoundObservationSourceError::MissingPointingDirection { .. }
    ));
}

#[test]
fn observation_pointing_interpolates_each_antenna_on_the_shortest_arc() {
    let directory = tempfile::tempdir().expect("temporary interpolated-POINTING fixture");
    let path = directory.path().join("interpolated-pointing.ms");
    generate_fixture(&path);
    let mut measurement_set = MeasurementSet::open(&path).expect("open POINTING fixture");
    let first_time = match measurement_set
        .main_table()
        .cell_accessor(0, "TIME")
        .and_then(|cell| cell.scalar())
        .expect("MAIN.TIME row 0")
    {
        ScalarValue::Float64(value) => *value,
        other => panic!(
            "MAIN.TIME must be Float64, found {:?}",
            other.primitive_type()
        ),
    };
    let second_time = match measurement_set
        .main_table()
        .cell_accessor(1, "TIME")
        .and_then(|cell| cell.scalar())
        .expect("MAIN.TIME row 1")
    {
        ScalarValue::Float64(value) => *value,
        other => panic!(
            "MAIN.TIME must be Float64, found {:?}",
            other.primitive_type()
        ),
    };
    let before_time = first_time - 0.5;
    let after_time = second_time + 0.5;
    {
        let mut pointing = measurement_set.pointing_mut().expect("POINTING subtable");
        for (row, antenna, direction) in [(0, 0, [0.0, 0.0]), (1, 1, [0.4, 0.0])] {
            pointing
                .set_i32(row, "ANTENNA_ID", antenna)
                .expect("set POINTING antenna");
            pointing
                .set_f64(row, "TIME", before_time)
                .expect("set POINTING time");
            pointing
                .set_f64(row, "TIME_ORIGIN", before_time)
                .expect("set POINTING origin");
            pointing
                .set_f64(row, "INTERVAL", -1.0)
                .expect("set POINTING timestamp semantics");
            pointing
                .set_array(row, "DIRECTION", direction_array(direction))
                .expect("set POINTING direction");
        }
        pointing
            .table_mut()
            .add_row(pointing_row(0, after_time, [0.2, 0.0]))
            .expect("append antenna-0 bracket");
        pointing
            .table_mut()
            .add_row(pointing_row(1, after_time, [0.6, 0.0]))
            .expect("append antenna-1 bracket");
    }
    measurement_set.save().expect("save POINTING fixture");

    let problem = compiled_problem_with_centres(
        &path,
        2,
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTime,
                PointingInterpolation::GreatCircleShortestArc,
                PointingExtrapolation::Reject,
                MissingPointingPolicy::Reject,
            )),
        ),
    );
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let samples = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 2, 1),
    )
    .expect("bind interpolated POINTING source")
    .selected_samples(&problem)
    .expect("prepare interpolated POINTING stream")
    .collect::<Result<Vec<_>, _>>()
    .expect("interpolate POINTING directions");

    for sample in &samples[..4] {
        assert!(
            (sample
                .coordinates
                .pointing_directions
                .antenna1
                .longitude_rad()
                - 0.05)
                .abs()
                < 1.0e-12
        );
        assert!(
            (sample
                .coordinates
                .pointing_directions
                .antenna2
                .longitude_rad()
                - 0.45)
                .abs()
                < 1.0e-12
        );
    }
    for sample in &samples[4..] {
        assert!(
            (sample
                .coordinates
                .pointing_directions
                .antenna1
                .longitude_rad()
                - 0.15)
                .abs()
                < 1.0e-12
        );
        assert!(
            (sample
                .coordinates
                .pointing_directions
                .antenna2
                .longitude_rad()
                - 0.55)
                .abs()
                < 1.0e-12
        );
    }
}

#[test]
fn multi_spw_selection_is_block_invariant_across_prediction_and_residual_replays() {
    let directory = tempfile::tempdir().expect("temporary multi-SPW fixture");
    let path = directory.path().join("multi-spw.ms");
    generate_fixture(&path);
    extend_fixture_with_second_spw(&path);
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![multi_spw_source_input(&path, 1)],
        vec![(ReferenceDataKind::Measures, identity(90))],
        ModelStateIdentity::Empty,
    ))
    .expect("compile multi-SPW observation");
    let problem = compile(ImagingRequest::new(
        specification(),
        geometry(),
        ProblemInputIdentities::new(snapshot.clone()),
        model_lifecycle(snapshot.model()),
    ))
    .expect("compile multi-SPW problem");
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let one_row = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 1, 1),
    )
    .expect("bind one-row multi-SPW stream");
    let three_rows = BoundObservationSource::open(
        &problem,
        source,
        &source_state(source),
        content_budget_for_rows(&problem, source, 3, 1),
    )
    .expect("bind three-row multi-SPW stream");
    assert_eq!(one_row.content_plan().rows_per_block(), 1);
    assert_eq!(three_rows.content_plan().rows_per_block(), 3);

    let prediction = one_row
        .selected_samples(&problem)
        .expect("prepare prediction replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read prediction replay");
    let residual = one_row
        .selected_samples(&problem)
        .expect("prepare residual replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read residual replay");
    let repartitioned = three_rows
        .selected_samples(&problem)
        .expect("prepare repartitioned replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read repartitioned replay");

    assert_eq!(prediction, residual);
    assert_eq!(prediction, repartitioned);
    assert_eq!(prediction.len(), 16);
    assert_eq!(
        prediction
            .iter()
            .map(|sample| {
                (
                    sample.address.physical_row,
                    sample.address.data_description_id,
                    sample.address.spectral_window_id,
                    sample.address.channel_index,
                    sample.address.correlation_index,
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (0, 0, 0, 0, 0),
            (0, 0, 0, 0, 1),
            (0, 0, 0, 2, 0),
            (0, 0, 0, 2, 1),
            (1, 0, 0, 0, 0),
            (1, 0, 0, 0, 1),
            (1, 0, 0, 2, 0),
            (1, 0, 0, 2, 1),
            (2, 1, 1, 1, 0),
            (2, 1, 1, 1, 1),
            (2, 1, 1, 2, 0),
            (2, 1, 1, 2, 1),
            (3, 1, 1, 1, 0),
            (3, 1, 1, 1, 1),
            (3, 1, 1, 2, 0),
            (3, 1, 1, 2, 1),
        ]
    );
    assert_eq!(prediction[8].address.frequency_centre_hz, 1.501e9);
    assert_eq!(prediction[12].address.frequency_centre_hz, 1.501e9);
    let prediction_inspection =
        inspect_samples(&problem, prediction).expect("inspect prediction replay");
    let residual_inspection = inspect_samples(&problem, residual).expect("inspect residual replay");
    assert_eq!(prediction_inspection, residual_inspection);
}

fn inspect_samples(
    problem: &casa_imaging_model::CompiledProblem,
    samples: impl IntoIterator<Item = SelectedObservationSample>,
) -> Result<(SelectedObservationGenerationId, u64), SelectedObservationInspectionError> {
    match problem.inspect_selected_observation(samples.into_iter().map(Ok::<_, Infallible>), |_| {
        Ok::<_, Infallible>(())
    }) {
        Ok(inspection) => Ok(inspection),
        Err(SelectedObservationPassError::Inspection(error)) => Err(error),
        Err(SelectedObservationPassError::External(error)) => match error {},
    }
}

fn extend_fixture_with_second_spw(path: &std::path::Path) {
    let mut measurement_set = MeasurementSet::open(path).expect("open multi-SPW fixture");
    let mut spectral_window = measurement_set
        .spectral_window()
        .expect("SPECTRAL_WINDOW")
        .table()
        .rows()
        .expect("SPECTRAL_WINDOW rows")[0]
        .clone();
    spectral_window.upsert(
        "NAME",
        Value::Scalar(ScalarValue::String("second-spw".to_string())),
    );
    spectral_window.upsert(
        "CHAN_FREQ",
        Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![3], vec![1.5e9, 1.501e9, 1.502e9])
                .expect("second-SPW frequency shape"),
        )),
    );
    spectral_window.upsert(
        "REF_FREQUENCY",
        Value::Scalar(ScalarValue::Float64(1.501e9)),
    );
    measurement_set
        .spectral_window_mut()
        .expect("mutable SPECTRAL_WINDOW")
        .table_mut()
        .add_row(spectral_window)
        .expect("append second SPECTRAL_WINDOW");

    let mut data_description = measurement_set
        .data_description()
        .expect("DATA_DESCRIPTION")
        .table()
        .rows()
        .expect("DATA_DESCRIPTION rows")[0]
        .clone();
    data_description.upsert("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(1)));
    measurement_set
        .data_description_mut()
        .expect("mutable DATA_DESCRIPTION")
        .table_mut()
        .add_row(data_description)
        .expect("append second DATA_DESCRIPTION");

    let original_rows = measurement_set
        .main_table()
        .rows()
        .expect("MAIN rows")
        .to_vec();
    for mut row in original_rows {
        row.upsert("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(1)));
        measurement_set
            .main_table_mut()
            .add_row(row)
            .expect("append second-SPW MAIN row");
    }
    measurement_set.save().expect("save multi-SPW fixture");
}

fn pointing_row(antenna_id: i32, time_mjd_seconds: f64, direction: [f64; 2]) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("ANTENNA_ID", Value::Scalar(ScalarValue::Int32(antenna_id))),
        RecordField::new("DIRECTION", Value::Array(direction_array(direction))),
        RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(-1.0))),
        RecordField::new("NAME", Value::Scalar(ScalarValue::String(String::new()))),
        RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("TARGET", Value::Array(direction_array(direction))),
        RecordField::new(
            "TIME",
            Value::Scalar(ScalarValue::Float64(time_mjd_seconds)),
        ),
        RecordField::new(
            "TIME_ORIGIN",
            Value::Scalar(ScalarValue::Float64(time_mjd_seconds)),
        ),
        RecordField::new("TRACKING", Value::Scalar(ScalarValue::Bool(true))),
    ])
}

fn observation_pointing(missing: MissingPointingPolicy) -> PointingCentreLaw {
    PointingCentreLaw::Observation(ObservationPointingLaw::new(
        PointingDirectionColumn::Direction,
        PointingDirectionSemantic::AntennaBoresight,
        PointingTimeSampling::VisibilityTime,
        PointingInterpolation::Nearest,
        PointingExtrapolation::HoldNearest,
        missing,
    ))
}

fn direction_array(direction: [f64; 2]) -> ArrayValue {
    ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![2, 1], direction.to_vec())
            .expect("constant POINTING direction shape"),
    )
}

fn generate_fixture(path: &std::path::Path) {
    generate_fixture_with_rows(path, 2);
}

fn generate_fixture_with_rows(path: &std::path::Path, row_count: usize) {
    let mut antennas = tutorial_vla_a_antennas();
    antennas.truncate(2);
    let mut request = SyntheticObservationRequest::vla_ppdisk("unused.fits", path, antennas);
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.duration_seconds = row_count as f64;
    request.integration_seconds = 1.0;
    request.spectral_setup = SyntheticSpectralSetup {
        name: "three-channel".to_string(),
        start_frequency_hz: 1.4e9,
        channel_width_hz: 1.0e6,
        channel_count: 3,
    };
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    generate_synthetic_observation_ms(&request).expect("generate bounded disk fixture");
}

fn main_time_mjd_seconds(measurement_set: &MeasurementSet, row: usize) -> f64 {
    match measurement_set
        .main_table()
        .cell_accessor(row, "TIME")
        .and_then(|cell| cell.scalar())
        .expect("MAIN.TIME")
    {
        ScalarValue::Float64(value) => *value,
        other => panic!(
            "MAIN.TIME must be Float64, found {:?}",
            other.primitive_type()
        ),
    }
}

fn source_state(source: &ObservationSource) -> ObservationSourceState {
    ObservationSourceState::new(
        source.identity(),
        source.selection().rows().clone(),
        source.generations().clone(),
    )
}

fn source_state_with_generation_capacity(
    source: &ObservationSource,
    capacity: usize,
) -> (ObservationSourceState, usize) {
    let source_generations = source.generations();
    let selected_columns = source_generations.columns();
    let mut column_generations = Vec::with_capacity(capacity);
    column_generations.extend_from_slice(selected_columns.generations());
    let mut metadata_generations = Vec::with_capacity(capacity);
    metadata_generations.extend_from_slice(source_generations.metadata_generations());
    let retained_generation_bytes = column_generations
        .capacity()
        .checked_mul(size_of::<ColumnGeneration>())
        .and_then(|bytes| {
            metadata_generations
                .capacity()
                .checked_mul(size_of::<MetadataGeneration>())
                .and_then(|metadata| bytes.checked_add(metadata))
        })
        .expect("finite oversized generation allocations");
    let selected_rows = SelectedRows::from_ordered_main_rows(
        source.selection().rows().source_row_count(),
        source
            .selection()
            .rows()
            .ordered_main_rows()
            .iter()
            .copied(),
    )
    .expect("rebuild an independently allocated current row manifest");
    let selected_row_bytes = selected_rows
        .retained_manifest_bytes()
        .expect("finite independent current row manifest");
    let state = ObservationSourceState::new(
        source.identity(),
        selected_rows,
        SourceGenerations::new(
            source_generations.consistency_token(),
            SelectedColumns::new(
                selected_columns.visibility(),
                selected_columns.flags(),
                selected_columns.weights(),
                column_generations,
            ),
            metadata_generations,
            source_generations.model_column(),
        ),
    );
    assert_eq!(
        state.additional_retained_heap_bytes([source.selection().rows()]),
        selected_row_bytes.checked_add(retained_generation_bytes),
        "independent rows and generation vectors are both binding-owned"
    );
    (state, retained_generation_bytes)
}

fn test_measures(
    problem: &casa_imaging_model::CompiledProblem,
) -> super::SelectedObservationMeasures {
    super::measures::test_selected_observation_measures(problem)
        .expect("bind deterministic Measures provider")
}

fn content_budget_for_rows(
    problem: &casa_imaging_model::CompiledProblem,
    source: &ObservationSource,
    target_rows_per_block: usize,
    maximum_live_blocks: usize,
) -> SelectedObservationContentBudget {
    let measures = test_measures(problem);
    content_budget_for_rows_with_shared_bytes(
        problem,
        source,
        selected_observation_shared_bytes(
            &measures,
            BoundObservationSource::retained_source_slot_bytes(),
            0,
        ),
        target_rows_per_block,
        maximum_live_blocks,
    )
}

fn bound_content_budget_for_rows(
    problem: &casa_imaging_model::CompiledProblem,
    source: &ObservationSource,
    target_rows_per_block: usize,
    maximum_live_blocks: usize,
) -> SelectedObservationContentBudget {
    let measures = test_measures(problem);
    content_budget_for_rows_with_shared_bytes(
        problem,
        source,
        selected_observation_shared_bytes(
            &measures,
            BoundObservationSource::retained_source_slot_bytes(),
            single_binding_graph_initialization_bytes(source),
        ),
        target_rows_per_block,
        maximum_live_blocks,
    )
}

fn single_binding_graph_initialization_bytes(source: &ObservationSource) -> usize {
    let state = source_state(source);
    expected_binding_graph_initialization_bytes(
        std::slice::from_ref(source),
        std::slice::from_ref(&state),
        Vec::<ObservationSourceBinding>::with_capacity(1).capacity(),
    )
}

fn expected_binding_graph_initialization_bytes(
    sources: &[ObservationSource],
    states: &[ObservationSourceState],
    binding_capacity: usize,
) -> usize {
    assert_eq!(sources.len(), states.len());
    let binding_slot_bytes = binding_capacity
        .checked_mul(size_of::<ObservationSourceBinding>())
        .expect("finite binding slot allocation");
    states
        .iter()
        .enumerate()
        .try_fold(binding_slot_bytes, |bytes, (state_index, state)| {
            state
                .additional_retained_heap_bytes(
                    sources
                        .iter()
                        .map(|source| source.selection().rows())
                        .chain(
                            states[..state_index]
                                .iter()
                                .map(ObservationSourceState::selected_rows),
                        ),
                )
                .and_then(|additional| bytes.checked_add(additional))
        })
        .expect("finite binding graph allocation")
}

fn selected_observation_shared_bytes(
    measures: &super::SelectedObservationMeasures,
    source_slots_retained_bytes: usize,
    binding_graph_initialization_bytes: usize,
) -> super::content_plan::SelectedObservationSharedBytes {
    super::content_plan::SelectedObservationSharedBytes::new(
        measures.retained_bytes(),
        source_slots_retained_bytes,
        binding_graph_initialization_bytes,
    )
}

fn content_budget_for_rows_with_shared_bytes(
    problem: &casa_imaging_model::CompiledProblem,
    source: &ObservationSource,
    shared_bytes: super::content_plan::SelectedObservationSharedBytes,
    target_rows_per_block: usize,
    maximum_live_blocks: usize,
) -> SelectedObservationContentBudget {
    assert!(target_rows_per_block > 0);
    let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())
        .expect("open retained fixture while deriving its exact content budget");
    let admitted = |available_bytes| {
        super::content_plan::selected_content_plan(
            &measurement_set,
            problem,
            source,
            shared_bytes,
            SelectedObservationContentBudget::new(available_bytes, maximum_live_blocks, 4),
        )
        .ok()
        .is_some_and(|plan| plan.rows_per_block() >= target_rows_per_block)
    };
    let mut upper = 1_usize;
    while !admitted(upper) {
        upper = upper
            .checked_mul(2)
            .expect("fixture content budget fits usize");
    }
    let mut lower = 0_usize;
    while lower + 1 < upper {
        let middle = lower + (upper - lower) / 2;
        if admitted(middle) {
            upper = middle;
        } else {
            lower = middle;
        }
    }
    SelectedObservationContentBudget::new(upper, maximum_live_blocks, 4)
}

fn compiled_problem(
    path: &std::path::Path,
    row_count: usize,
) -> casa_imaging_model::CompiledProblem {
    compiled_problem_with_sources(&[(path, 1, row_count)])
}

fn compiled_problem_with_centres(
    path: &std::path::Path,
    row_count: usize,
    centres: CentreLaws,
) -> casa_imaging_model::CompiledProblem {
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source_input(path, 1, row_count)],
        vec![(ReferenceDataKind::Measures, identity(90))],
        ModelStateIdentity::Empty,
    ))
    .expect("compile fixed-centre observation");
    compile(ImagingRequest::new(
        specification(),
        geometry_with_centres(centres),
        ProblemInputIdentities::new(snapshot.clone()),
        model_lifecycle(snapshot.model()),
    ))
    .expect("compile fixed-centre problem")
}

fn compiled_problem_with_sources(
    sources: &[(&std::path::Path, u8, usize)],
) -> casa_imaging_model::CompiledProblem {
    let references = vec![(ReferenceDataKind::Measures, identity(90))];
    let sources = sources
        .iter()
        .map(|(path, source, row_count)| source_input(path, *source, *row_count))
        .collect();
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        sources,
        references,
        ModelStateIdentity::Empty,
    ))
    .expect("compile selected observation");
    compile(ImagingRequest::new(
        specification(),
        geometry(),
        ProblemInputIdentities::new(snapshot.clone()),
        model_lifecycle(snapshot.model()),
    ))
    .expect("compile selected-observation problem")
}

fn source_input(path: &std::path::Path, source: u8, row_count: usize) -> ObservationSourceInput {
    let selected_rows = SelectedRows::from_ordered_main_rows(
        row_count as u64,
        (0..row_count).map(|row| SelectedMainRow::new(row as u64, 0)),
    )
    .expect("selected row manifest");
    source_input_with_selected_rows(path, source, selected_rows)
}

fn source_input_with_selected_rows(
    path: &std::path::Path,
    source: u8,
    selected_rows: SelectedRows,
) -> ObservationSourceInput {
    source_input_with_selected_rows_and_filter(
        path,
        source,
        selected_rows,
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
    )
}

fn source_input_with_selected_rows_and_filter(
    path: &std::path::Path,
    source: u8,
    selected_rows: SelectedRows,
    rows_filter: RowSelection,
) -> ObservationSourceInput {
    source_input_with_selected_rows_filter_and_request(
        path,
        source,
        selected_rows,
        rows_filter,
        scoped_identity(source, 2),
    )
}

fn source_input_with_selected_rows_filter_and_request(
    path: &std::path::Path,
    source: u8,
    selected_rows: SelectedRows,
    rows_filter: RowSelection,
    selection_request: LogicalIdentity,
) -> ObservationSourceInput {
    let selection = ObservationSelection::new(
        selected_rows,
        rows_filter,
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, vec![0, 2])],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, CorrelationType::CircularRr),
                CorrelationProduct::new(1, CorrelationType::CircularLl),
            ],
        )],
    );
    let columns = [
        MsColumnKind::Data,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::Weight,
        MsColumnKind::Uvw,
        MsColumnKind::Time,
        MsColumnKind::TimeCentroid,
        MsColumnKind::Interval,
        MsColumnKind::Exposure,
        MsColumnKind::FieldId,
        MsColumnKind::DataDescriptionId,
        MsColumnKind::Antenna1,
        MsColumnKind::Antenna2,
        MsColumnKind::Feed1,
        MsColumnKind::Feed2,
        MsColumnKind::ScanNumber,
        MsColumnKind::StateId,
        MsColumnKind::ObservationId,
        MsColumnKind::ArrayId,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| ColumnGeneration::new(kind, scoped_identity(source, 10 + index as u8)))
    .collect();
    let metadata = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| MetadataGeneration::new(kind, scoped_identity(source, 40 + index as u8)))
    .collect();
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(scoped_identity(source, 1)),
        ObservationSourceProvenance::new(path.display().to_string(), selection_request),
        selection,
        SourceGenerations::new(
            ConsistencyToken::new(scoped_identity(source, 3)),
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
                columns,
            ),
            metadata,
            ModelColumnState::Absent,
        ),
    )
}

fn generations_with_changed_column(
    source: &ObservationSource,
    changed: MsColumnKind,
    generation: LogicalIdentity,
) -> SourceGenerations {
    let expected = source.generations();
    let columns = expected
        .columns()
        .generations()
        .iter()
        .copied()
        .map(|current| {
            if current.kind() == changed {
                ColumnGeneration::new(changed, generation)
            } else {
                current
            }
        })
        .collect();
    SourceGenerations::new(
        expected.consistency_token(),
        SelectedColumns::new(
            expected.columns().visibility(),
            expected.columns().flags(),
            expected.columns().weights(),
            columns,
        ),
        expected.metadata_generations().to_vec(),
        expected.model_column(),
    )
}

fn generations_with_changed_metadata(
    source: &ObservationSource,
    changed: MetadataTableKind,
    generation: LogicalIdentity,
) -> SourceGenerations {
    let expected = source.generations();
    let metadata = expected
        .metadata_generations()
        .iter()
        .copied()
        .map(|current| {
            if current.kind() == changed {
                MetadataGeneration::new(changed, generation)
            } else {
                current
            }
        })
        .collect();
    SourceGenerations::new(
        expected.consistency_token(),
        expected.columns().clone(),
        metadata,
        expected.model_column(),
    )
}

fn multi_spw_source_input(path: &std::path::Path, source: u8) -> ObservationSourceInput {
    let selected_rows = SelectedRows::from_ordered_main_rows(
        4,
        [
            SelectedMainRow::new(0, 0),
            SelectedMainRow::new(1, 0),
            SelectedMainRow::new(2, 1),
            SelectedMainRow::new(3, 1),
        ],
    )
    .expect("multi-SPW selected row manifest");
    let selection = ObservationSelection::new(
        selected_rows,
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
        vec![
            DataDescriptionSelection::new(0, 0, 0),
            DataDescriptionSelection::new(1, 1, 0),
        ],
        vec![
            SpectralWindowSelection::new(0, vec![0, 2]),
            SpectralWindowSelection::new(1, vec![1, 2]),
        ],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, CorrelationType::CircularRr),
                CorrelationProduct::new(1, CorrelationType::CircularLl),
            ],
        )],
    );
    let columns = [
        MsColumnKind::Data,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::Weight,
        MsColumnKind::Uvw,
        MsColumnKind::Time,
        MsColumnKind::TimeCentroid,
        MsColumnKind::Interval,
        MsColumnKind::Exposure,
        MsColumnKind::FieldId,
        MsColumnKind::DataDescriptionId,
        MsColumnKind::Antenna1,
        MsColumnKind::Antenna2,
        MsColumnKind::Feed1,
        MsColumnKind::Feed2,
        MsColumnKind::ScanNumber,
        MsColumnKind::StateId,
        MsColumnKind::ObservationId,
        MsColumnKind::ArrayId,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| ColumnGeneration::new(kind, scoped_identity(source, 10 + index as u8)))
    .collect();
    let metadata = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| MetadataGeneration::new(kind, scoped_identity(source, 40 + index as u8)))
    .collect();
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(scoped_identity(source, 1)),
        ObservationSourceProvenance::new(path.display().to_string(), scoped_identity(source, 2)),
        selection,
        SourceGenerations::new(
            ConsistencyToken::new(scoped_identity(source, 3)),
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
                columns,
            ),
            metadata,
            ModelColumnState::Absent,
        ),
    )
}

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

fn scoped_identity(source: u8, byte: u8) -> LogicalIdentity {
    let mut digest = [byte; 32];
    digest[0] = source;
    LogicalIdentity::from_sha256(digest)
}

fn specification() -> ProblemSpecification {
    ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(10, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            ProductValidityPolicies::new(
                PrimaryBeamValidityPolicy::new(
                    0.2,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )
                .expect("valid PB policy"),
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )
                .expect("valid Taylor policy"),
            ),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![NumericPrecision::F32],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    )
}

fn geometry() -> GeometryInput {
    geometry_with_centres(CentreLaws::new(
        PhaseCentreLaw::Observation,
        DelayCentreLaw::PhaseTrackingCentre,
        PointingCentreLaw::PhaseTrackingCentre,
    ))
}

fn geometry_with_centres(centres: CentreLaws) -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [15.0, 15.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(32, 32),
            direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )],
        centres,
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 3,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            casa_imaging_model::DopplerConvention::NotApplicable,
        ),
    )
}
