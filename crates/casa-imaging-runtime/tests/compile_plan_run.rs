// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
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
    BindingKind, ImplementationRegistryId, PhysicalWorkId, PlannerCostModelProfileId,
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
        Ok::<_, ()>(PhysicalWorkId::from_sha256([5; 32]))
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

    let repeated = plan(&problem, bindings, |_, _| {
        Ok::<_, ()>(PhysicalWorkId::from_sha256([5; 32]))
    })
    .expect("repeat physical planning");
    assert_eq!(execution_plan.plan_id(), repeated.plan_id());
    assert_eq!(
        execution_plan.plan_id().as_bytes(),
        [
            62, 72, 52, 115, 0, 123, 86, 214, 10, 79, 159, 87, 245, 86, 220, 151, 210, 164, 157,
            206, 156, 161, 75, 218, 182, 140, 165, 107, 220, 77, 170, 252,
        ]
    );
}

#[test]
fn run_rejects_changed_registry_policy_and_cost_model_bindings() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(PhysicalWorkId::from_sha256([5; 32])),
    )
    .expect("physical planning");
    let current = |registry_id, policy, cost_model_id| {
        RunBindings::new(problem.inputs().clone(), registry_id, policy, cost_model_id)
    };
    let reject = |bindings| run(&problem, &execution_plan, &bindings, |_, _| Ok::<_, ()>(()));

    assert!(matches!(
        reject(current(
            registry(9),
            &ResourcePolicy::Balanced,
            cost_model(4)
        )),
        Err(RunError::BindingMismatch {
            binding: BindingKind::ImplementationRegistry
        })
    ));
    assert!(matches!(
        reject(current(
            registry(3),
            &ResourcePolicy::Exclusive,
            cost_model(4)
        )),
        Err(RunError::BindingMismatch {
            binding: BindingKind::ResourcePolicy
        })
    ));
    assert!(matches!(
        reject(current(
            registry(3),
            &ResourcePolicy::Balanced,
            cost_model(9)
        )),
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
        Ok::<_, ()>(PhysicalWorkId::from_sha256([5; 32]))
    })
    .expect("physical planning");
    let calls = AtomicUsize::new(0);
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
        let stale = RunBindings::new(
            inputs,
            registry(3),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let result = run(&problem, &execution_plan, &stale, |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(())
        });
        assert!(matches!(
            result,
            Err(RunError::BindingMismatch { binding }) if binding == expected
        ));
    }

    let changed_problem = compile(request(9)).expect("changed logical problem");
    let current = RunBindings::new(
        changed_problem.inputs().clone(),
        registry(3),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let result = run(&changed_problem, &execution_plan, &current, |_, _| {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok::<_, ()>(())
    });
    assert!(matches!(
        result,
        Err(RunError::BindingMismatch {
            binding: BindingKind::CompiledProblem
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn run_executes_one_exactly_bound_plan_without_routing_or_replanning() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings = PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let execution_plan = plan(&problem, bindings.clone(), |_, _| {
        Ok::<_, ()>(PhysicalWorkId::from_sha256([5; 32]))
    })
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        registry(3),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let calls = AtomicUsize::new(0);

    let output = run(
        &problem,
        &execution_plan,
        &current,
        |executed_problem, executed_plan| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(executed_problem.problem_id(), problem.problem_id());
            assert_eq!(executed_plan.plan_id(), execution_plan.plan_id());
            Ok::<_, ()>("executed")
        },
    )
    .expect("exact execution");

    assert_eq!(output, "executed");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn run_preserves_the_selected_executors_error_chain() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(PhysicalWorkId::from_sha256([5; 32])),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        registry(3),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let error = run(&problem, &execution_plan, &current, |_, _| {
        Err::<(), _>(io::Error::other("selected executor failed"))
    })
    .expect_err("executor failure");

    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("selected executor failed")
    );
}
