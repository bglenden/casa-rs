// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::BTreeMap,
    error::Error,
    io,
    sync::atomic::{AtomicUsize, Ordering},
};

use casa_imaging_model::{
    CompiledGeometryId, FieldGeometry, FiniteValuePolicy, GeometryContract, ImagingRequest,
    ImagingRequestVersion, InstrumentResponse, LogicalIdentity, MeasurementEquationContract,
    ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract, ObservationSnapshotId,
    PolarizationContract, PolarizationCoordinate, ProblemInputIdentities, ProblemSpecification,
    ProductKind, ProductNormalization, ProductRequirements, ProjectionGeometry,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, ReferenceDataKind, RestoringBeamPolicy, ScientificContract, SpectralContract,
    SpectralCoupling, SpectralFrame, SpectralSampling, StageErrorBudget, WeightDensityScope,
    WeightingContract, WeightingScheme, compile,
};
use casa_imaging_runtime::{
    BindingKind, ExecutionImplementation, ImplementationId, ImplementationRegistry,
    ImplementationRegistryId, PhysicalWorkBinding, PhysicalWorkId, PlannerCostModelProfileId,
    PlanningBindings, ResourcePolicy, RunBindings, RunError, plan, run,
};

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

fn request(observation: u8) -> ImagingRequest {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    let specification = ProblemSpecification::new(
        ScientificContract::new(
            GeometryContract::new(ProjectionGeometry::Coplanar, FieldGeometry::Single),
            SpectralContract::new(
                SpectralFrame::Native,
                SpectralSampling::Identity,
                SpectralCoupling::Independent,
                None,
            ),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            MeasurementEquationContract::new(InstrumentResponse::Scalar),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
        ),
        numerics,
    );
    ImagingRequest::new(
        specification,
        ProblemInputIdentities::new(
            ObservationSnapshotId::new(identity(observation)),
            CompiledGeometryId::new(identity(2)),
            Vec::new(),
            ModelStateIdentity::Empty,
        ),
    )
}

fn registry(byte: u8) -> ImplementationRegistryId {
    ImplementationRegistryId::from_sha256([byte; 32])
}

fn cost_model(byte: u8) -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256([byte; 32])
}

fn implementation(byte: u8) -> ImplementationId {
    ImplementationId::from_sha256([byte; 32])
}

#[derive(Debug)]
struct RecordingExecutor {
    id: ImplementationId,
    label: &'static str,
    failure: Option<&'static str>,
    calls: AtomicUsize,
}

impl ExecutionImplementation for RecordingExecutor {
    type Error = io::Error;
    type Output = &'static str;

    fn implementation_id(&self) -> ImplementationId {
        self.id
    }

    fn execute(
        &self,
        _problem: &casa_imaging_model::CompiledProblem,
        _plan: &casa_imaging_runtime::ExecutionPlan,
    ) -> Result<Self::Output, Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.failure {
            Some(message) => Err(io::Error::other(message)),
            None => Ok(self.label),
        }
    }
}

fn physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    PhysicalWorkBinding::new(
        PhysicalWorkId::from_sha256([5; 32]),
        implementation(implementation_byte),
    )
}

fn test_registry(
    registry_byte: u8,
    implementation_byte: u8,
    label: &'static str,
    failure: Option<&'static str>,
) -> TestRegistry {
    TestRegistry {
        id: registry(registry_byte),
        executors: BTreeMap::from([(
            implementation(implementation_byte),
            RecordingExecutor {
                id: implementation(implementation_byte),
                label,
                failure,
                calls: AtomicUsize::new(0),
            },
        )]),
    }
}

struct TestRegistry {
    id: ImplementationRegistryId,
    executors: BTreeMap<ImplementationId, RecordingExecutor>,
}

impl ImplementationRegistry for TestRegistry {
    type Implementation = RecordingExecutor;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: ImplementationId) -> Option<&Self::Implementation> {
        self.executors.get(&id)
    }
}

#[test]
fn run_can_invoke_only_the_implementation_identity_sealed_by_plan() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, ()>(PhysicalWorkBinding::new(
                PhysicalWorkId::from_sha256([5; 32]),
                implementation(6),
            ))
        },
    )
    .expect("physical planning");
    let selected = RecordingExecutor {
        id: implementation(6),
        label: "selected",
        failure: None,
        calls: AtomicUsize::new(0),
    };
    let different = RecordingExecutor {
        id: implementation(7),
        label: "different",
        failure: None,
        calls: AtomicUsize::new(0),
    };
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([
            (implementation(6), selected),
            (implementation(7), different),
        ]),
    };
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let output = run(&problem, &execution_plan, &current, &registry).expect("bound execution");

    assert_eq!(output, "selected");
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        registry.executors[&implementation(7)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn run_rejects_a_registry_that_cannot_resolve_the_bound_implementation() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let registry = test_registry(3, 7, "different", None);
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let result = run(&problem, &execution_plan, &current, &registry);

    assert!(matches!(
        result,
        Err(RunError::ImplementationUnavailable { implementation: id })
            if id == implementation(6)
    ));
    assert_eq!(
        registry.executors[&implementation(7)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn run_rejects_a_different_implementation_returned_under_the_bound_key() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let mut registry = test_registry(3, 6, "different", None);
    registry
        .executors
        .get_mut(&implementation(6))
        .expect("registered key")
        .id = implementation(7);
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let result = run(&problem, &execution_plan, &current, &registry);

    assert!(matches!(
        result,
        Err(RunError::BindingMismatch {
            binding: BindingKind::Implementation
        })
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn versioned_request_compiles_before_physical_planning() {
    let request = request(1);
    assert_eq!(request.version(), ImagingRequestVersion::V1);

    let problem = compile(request).expect("logical compilation");
    assert_eq!(problem.numerics_id().as_bytes().len(), 32);
}

#[test]
fn plan_seals_physical_work_and_every_required_binding() {
    let problem = compile(request(1)).expect("logical compilation");
    let expected_problem_id = problem.problem_id();
    let bindings = PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let execution_plan = plan(&problem, bindings.clone(), |problem, bindings| {
        assert_eq!(problem.problem_id(), expected_problem_id);
        assert_eq!(bindings.resource_policy(), &ResourcePolicy::Balanced);
        Ok::<_, ()>(physical_work(6))
    })
    .expect("physical planning");

    assert_eq!(execution_plan.problem_id(), problem.problem_id());
    assert_eq!(
        execution_plan.observation_snapshot_id(),
        problem.inputs().observation()
    );
    assert_eq!(execution_plan.numerics_id(), problem.numerics_id());
    assert_eq!(execution_plan.implementation_registry_id(), registry(3));
    assert_eq!(
        execution_plan.resource_policy_id(),
        bindings.resource_policy_id()
    );
    assert_eq!(
        execution_plan.planner_cost_model_profile_id(),
        cost_model(4)
    );
    assert_eq!(
        execution_plan.physical_work_id(),
        PhysicalWorkId::from_sha256([5; 32])
    );
    assert_eq!(execution_plan.implementation_id(), implementation(6));

    let repeated = plan(&problem, bindings, |_, _| Ok::<_, ()>(physical_work(6)))
        .expect("repeat physical planning");
    assert_eq!(execution_plan.plan_id(), repeated.plan_id());
    assert_eq!(
        execution_plan.plan_id().as_bytes(),
        [
            113, 113, 83, 196, 201, 219, 253, 182, 230, 19, 114, 198, 152, 111, 52, 7, 91, 205,
            247, 183, 148, 158, 244, 86, 14, 136, 164, 206, 21, 159, 89, 48,
        ]
    );
}

#[test]
fn run_rejects_changed_registry_policy_and_cost_model_bindings() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current =
        |policy, cost_model_id| RunBindings::new(problem.inputs().clone(), policy, cost_model_id);
    let reject = |bindings, registry| run(&problem, &execution_plan, &bindings, registry);
    let wrong_registry = test_registry(9, 6, "selected", None);
    let correct_registry = test_registry(3, 6, "selected", None);

    assert!(matches!(
        reject(
            current(&ResourcePolicy::Balanced, cost_model(4)),
            &wrong_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::ImplementationRegistry
        })
    ));
    assert!(matches!(
        reject(
            current(&ResourcePolicy::Exclusive, cost_model(4)),
            &correct_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::ResourcePolicy
        })
    ));
    assert!(matches!(
        reject(
            current(&ResourcePolicy::Balanced, cost_model(9)),
            &correct_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::PlannerCostModelProfile
        })
    ));
}

#[test]
fn run_rejects_every_stale_problem_input_before_calling_the_executor() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings = PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let execution_plan = plan(&problem, bindings.clone(), |_, _| {
        Ok::<_, ()>(physical_work(6))
    })
    .expect("physical planning");
    let registry = test_registry(3, 6, "selected", None);
    let stale_inputs = [
        (
            ProblemInputIdentities::new(
                ObservationSnapshotId::new(identity(9)),
                CompiledGeometryId::new(identity(2)),
                Vec::new(),
                ModelStateIdentity::Empty,
            ),
            BindingKind::ObservationSnapshot,
        ),
        (
            ProblemInputIdentities::new(
                ObservationSnapshotId::new(identity(1)),
                CompiledGeometryId::new(identity(9)),
                Vec::new(),
                ModelStateIdentity::Empty,
            ),
            BindingKind::CompiledGeometry,
        ),
        (
            ProblemInputIdentities::new(
                ObservationSnapshotId::new(identity(1)),
                CompiledGeometryId::new(identity(2)),
                vec![(ReferenceDataKind::Measures, identity(9))],
                ModelStateIdentity::Empty,
            ),
            BindingKind::ReferenceDataSnapshots,
        ),
        (
            ProblemInputIdentities::new(
                ObservationSnapshotId::new(identity(1)),
                CompiledGeometryId::new(identity(2)),
                Vec::new(),
                ModelStateIdentity::Seed(identity(9)),
            ),
            BindingKind::ModelState,
        ),
    ];

    for (inputs, expected) in stale_inputs {
        let stale = RunBindings::new(inputs, &ResourcePolicy::Balanced, cost_model(4));
        let result = run(&problem, &execution_plan, &stale, &registry);
        assert!(matches!(
            result,
            Err(RunError::BindingMismatch { binding }) if binding == expected
        ));
    }

    let changed_problem = compile(request(9)).expect("changed logical problem");
    let current = RunBindings::new(
        changed_problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let result = run(&changed_problem, &execution_plan, &current, &registry);
    assert!(matches!(
        result,
        Err(RunError::BindingMismatch {
            binding: BindingKind::CompiledProblem
        })
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn run_executes_one_exactly_bound_plan_without_routing_or_replanning() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings = PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let execution_plan = plan(&problem, bindings.clone(), |_, _| {
        Ok::<_, ()>(physical_work(6))
    })
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, "executed", None);

    let output = run(&problem, &execution_plan, &current, &registry).expect("exact execution");

    assert_eq!(output, "executed");
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        1
    );
}

#[test]
fn run_preserves_the_selected_executors_error_chain() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, "selected", Some("selected executor failed"));

    let error = run(&problem, &execution_plan, &current, &registry).expect_err("executor failure");

    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("selected executor failed")
    );
}
