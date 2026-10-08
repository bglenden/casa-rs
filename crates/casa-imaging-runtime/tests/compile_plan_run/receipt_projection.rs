// SPDX-License-Identifier: LGPL-3.0-or-later

//! Durable receipts: exact identities, checksum-valid tamper rejection and
//! the versioned effective-problem projection.

use super::*;

#[test]
fn run_persists_a_reopenable_receipt_with_exact_identities_and_every_plan_node() {
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
    let registry = test_registry(&problem, 3, 6, None);
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([9; 32]),
        BuildIdentity::from_sha256([10; 32]),
    );
    let mut controller = RunToCompletion;

    let outcome = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen durable receipt");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert_eq!(receipt.schema_version(), 25);
    assert_eq!(receipt.status(), ReceiptStatus::Completed);
    assert_eq!(receipt.plan_identity(), execution_plan.plan_id().as_bytes());
    assert_eq!(receipt.problem_identity(), problem.problem_id().as_bytes());
    assert_eq!(
        receipt.product_graph_identity(),
        problem.product_graph().graph_id().as_bytes()
    );
    assert_eq!(
        receipt.geometry_identity(),
        problem.geometry().geometry_id().as_bytes()
    );
    assert_eq!(
        receipt.observation_identity(),
        problem.inputs().observation().identity().as_bytes()
    );
    assert_eq!(
        receipt.implementation_registry_identity(),
        registry.registry_id().as_bytes()
    );
    assert_eq!(
        receipt.resource_policy_identity(),
        execution_plan.resource_policy_id().as_bytes()
    );
    assert_eq!(receipt.cost_model_identity(), cost_model(4).as_bytes());
    assert_eq!(
        receipt.dag_identity(),
        execution_plan.physical_work_id().as_bytes()
    );
    assert_eq!(
        receipt.plan_node_count(),
        execution_plan.execution_dag().nodes().len()
    );
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Completed)
    );
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Completed)
    );
}

#[test]
fn receipt_rejects_checksum_valid_typed_projection_and_audit_forgery() {
    let problem = compile(request_with_products_and_initial_model(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual],
        ModelStateIdentity::Seed(identity(89)),
    ))
    .expect("two-product logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |problem, _| Ok::<_, ()>(reconstruction_physical_work_for_problem(problem, 6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), recording_executor(6, None, None))]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([85; 32]),
        BuildIdentity::from_sha256([86; 32]),
    );
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let path = only_receipt_path(receipts.root_path());
    let original = fs::read_to_string(&path).expect("serialized receipt");
    let checksum_marker = "\"payload_sha256\":\"";
    let checksum_start =
        original.find(checksum_marker).expect("payload checksum") + checksum_marker.len();
    assert_eq!(
        &original[checksum_start..checksum_start + 64],
        payload_sha256(&original),
        "the raw-test checksum reproduces receipt canonicalization"
    );
    let reopened = receipts
        .open(provenance.attempt_id())
        .expect("valid receipt");
    assert_eq!(
        reopened.product_graph_schema_version(),
        problem.product_graph().schema_version()
    );
    assert_eq!(reopened.product_graph_node_ordinals(), &[0, 1]);
    assert_eq!(
        reopened.product_graph_publication_member_ordinals(),
        &[0, 1]
    );

    let cases = [
        (
            "forged graph identity",
            with_forged_product_graph_identity(original.clone()),
        ),
        (
            "coordinated reprojection identity and audit forgery",
            with_forged_reprojection_identity(original.clone()),
        ),
        (
            "coordinated model lifecycle identity and audit forgery",
            with_forged_model_lifecycle_identity(original.clone(), &"d".repeat(64)),
        ),
        (
            "coordinated model input and audit forgery",
            with_forged_model_input_source_identity(original.clone(), &"c".repeat(64)),
        ),
        (
            "zero model lifecycle identity sentinel",
            with_forged_model_lifecycle_identity(original.clone(), &"0".repeat(64)),
        ),
        (
            "zero model input identity sentinel",
            with_forged_model_input_source_identity(original.clone(), &"0".repeat(64)),
        ),
        (
            "coordinated zero problem-model and audit identity",
            with_forged_problem_model_and_audit_identity(original.clone(), &"0".repeat(64)),
        ),
        (
            "coordinated parent problem and audit identity forgery",
            with_forged_parent_problem_identity(original.clone(), &"b".repeat(64)),
        ),
        (
            "coordinated zero parent problem and audit identity",
            with_forged_parent_problem_identity(original.clone(), &"0".repeat(64)),
        ),
        (
            "missing publication member",
            with_usize_array(original.clone(), "publication_member_ordinals", &[0]),
        ),
        (
            "extra unknown publication member",
            with_usize_array(original.clone(), "publication_member_ordinals", &[0, 1, 2]),
        ),
        (
            "duplicate publication member",
            with_usize_array(original.clone(), "publication_member_ordinals", &[0, 0, 1]),
        ),
        (
            "reordered publication members",
            with_usize_array(original.clone(), "publication_member_ordinals", &[1, 0]),
        ),
        (
            "audit node ordinal contradicts the typed projection",
            with_forged_audit_field(original.clone(), "products.graph.nodes.0.ordinal", "1"),
        ),
        (
            "audit publication member contradicts the typed projection",
            with_forged_audit_field(
                original.clone(),
                "products.graph.publication.members.0",
                "1",
            ),
        ),
        (
            "audit reprojection contract contradicts the typed projection",
            with_forged_audit_field(
                original.clone(),
                "model_lifecycle.reprojection.direction_registry",
                "forged",
            ),
        ),
    ];
    for (case, document) in cases {
        fs::write(&path, document).expect("rewrite checksum-valid receipt");
        assert!(
            matches!(
                receipts.open(provenance.attempt_id()),
                Err(casa_imaging_runtime::ReceiptError::IntegrityMismatch)
            ),
            "{case} must fail canonical typed projection validation"
        );
    }
}

#[test]
fn effective_problem_projection_normalizes_signed_zero_like_canonical_identities() {
    let compile_with_robust = |robust| {
        compile(request_with_geometry_references_and_weighting(
            81,
            geometry(255.0),
            Vec::new(),
            WeightingContract::new(
                WeightingScheme::Briggs { robust },
                WeightDensityScope::GlobalSelection,
            ),
        ))
        .expect("logical compilation")
    };
    let positive_zero = compile_with_robust(0.0);
    let negative_zero = compile_with_robust(-0.0);

    assert_eq!(positive_zero.problem_id(), negative_zero.problem_id());
    assert_eq!(
        positive_zero.weighting().commitment_id(),
        negative_zero.weighting().commitment_id()
    );

    let positive_projection = CompiledProblemEvidence::project(&positive_zero);
    let negative_projection = CompiledProblemEvidence::project(&negative_zero);
    assert_eq!(
        positive_projection.field("weighting.scheme.robust"),
        Some("f64:0000000000000000")
    );
    assert_eq!(positive_projection, negative_projection);
}

#[test]
fn effective_problem_projection_carries_mtmfs_scales_and_bias() {
    let compile_with_bias = |observation, bias| {
        compile(ImagingRequest::new(
            mtmfs_problem_specification(bias),
            geometry(255.0),
            problem_inputs(observation, Vec::new(), ModelStateIdentity::Empty),
            model_lifecycle(ModelStateIdentity::Empty),
        ))
        .expect("logical MT-MFS compilation")
    };
    let unbiased = compile_with_bias(91, 0.0);
    let biased = compile_with_bias(91, 0.2);
    let unbiased_projection = CompiledProblemEvidence::project(&unbiased);
    let biased_projection = CompiledProblemEvidence::project(&biased);

    assert_eq!(unbiased_projection.schema_version(), 12);
    assert_eq!(
        unbiased_projection.field("reconstruction.algorithm.kind"),
        Some("mtmfs")
    );
    assert_eq!(
        unbiased_projection.field("reconstruction.algorithm.scales_px.0"),
        Some("f64:0000000000000000")
    );
    assert_eq!(
        unbiased_projection.field("reconstruction.algorithm.scales_px.1"),
        Some("f64:4014000000000000")
    );
    assert_eq!(
        unbiased_projection.field("reconstruction.algorithm.small_scale_bias"),
        Some("f64:0000000000000000")
    );
    assert_eq!(
        biased_projection.field("reconstruction.algorithm.small_scale_bias"),
        Some("f64:3fc999999999999a")
    );
    assert_ne!(unbiased_projection, biased_projection);
}

#[test]
fn t41_receipt_projection_records_the_exact_primary_beam_instrument_model() {
    let specification = ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::PrimaryBeam,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        )
        .with_instrument_model(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::UnorderedWithinBudget,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    );
    let problem = compile(ImagingRequest::new(
        specification,
        geometry(255.0),
        problem_inputs(
            92,
            vec![
                (ReferenceDataKind::Measures, identity(90)),
                (ReferenceDataKind::Instrument, identity(91)),
            ],
            ModelStateIdentity::Empty,
        ),
        model_lifecycle(ModelStateIdentity::Empty),
    ))
    .expect("logical primary-beam compilation");

    let projection = CompiledProblemEvidence::project(&problem);
    assert_eq!(
        projection.field("science.measurement_equation.operator.transforms.3.instrument_model"),
        Some("casa-alma-aca-heterogeneous-interferometric-response-v1")
    );
}

#[test]
fn receipt_reopens_the_complete_versioned_effective_problem_projection() {
    let problem = compile(request_with_geometry_and_references(
        81,
        geometry(255.0),
        vec![(ReferenceDataKind::Measures, identity(82))],
    ))
    .expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |problem, _| Ok::<_, ()>(physical_work_for_problem(problem, 6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([83; 32]),
        BuildIdentity::from_sha256([84; 32]),
    );
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let reopened = receipts.open(provenance.attempt_id()).expect("receipt");
    let projected = reopened.compiled_problem_evidence();
    assert_eq!(
        reopened.product_graph_node_ordinals(),
        problem
            .product_graph()
            .nodes()
            .iter()
            .map(|node| node.node_id().ordinal())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        reopened.product_graph_publication_member_ordinals(),
        problem
            .product_graph()
            .publication()
            .members()
            .iter()
            .map(|member| member.ordinal())
            .collect::<Vec<_>>()
    );
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let data_generation = source
        .generations()
        .columns()
        .generation(MsColumnKind::Data)
        .expect("data generation")
        .to_string();
    let antenna_generation = source
        .generations()
        .metadata(MetadataTableKind::Antenna)
        .expect("antenna generation")
        .to_string();

    assert_eq!(projected.schema_version(), 12);
    assert_eq!(projected, &CompiledProblemEvidence::project(&problem));
    assert_eq!(
        reopened.model_lifecycle_identity(),
        problem.model_lifecycle().contract_id().as_bytes()
    );
    let lifecycle_identity = problem.model_lifecycle().contract_id().to_string();
    let target_shape_identity = problem.model_lifecycle().target().identity().to_string();
    assert_eq!(
        projected.field("model_lifecycle.identity"),
        Some(lifecycle_identity.as_str())
    );
    assert_eq!(
        projected.field("model_lifecycle.target_shape_identity"),
        Some(target_shape_identity.as_str())
    );
    assert_eq!(projected.field("model_lifecycle.input.kind"), Some("empty"));
    assert_eq!(
        projected.field("science.spectral.sampling"),
        Some("identity")
    );
    assert_eq!(
        projected.field("science.measurement_equation.instrument_response"),
        Some("scalar")
    );
    assert_eq!(
        projected.field("science.measurement_equation.inner_products.model"),
        Some("hermitian_euclidean")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.0.kind"),
        Some("spectral_basis")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.1.kind"),
        Some("polarization")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.2.kind"),
        Some("feed_response")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.3.kind"),
        Some("direction_dependent_response")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.4.kind"),
        Some("phase")
    );
    assert_eq!(
        projected.field("science.normal_equation.output.normalization"),
        Some("unnormalized")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.0"),
        Some("right_hand_side_a_star_w_d")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.1"),
        Some("residual_a_star_w_d_minus_a_x")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.2"),
        Some("normal_operator_a_star_w_a")
    );
    assert_eq!(
        projected.field("reconstruction.algorithm.kind"),
        Some("dirty")
    );
    assert_eq!(projected.field("weighting.scheme.kind"), Some("natural"));
    assert_eq!(
        projected.field("weighting.commitment.identity"),
        Some(
            problem
                .normal_equation()
                .weighting()
                .commitment_id()
                .to_string()
                .as_str()
        )
    );
    assert_eq!(
        projected.field("weighting.commitment.selected_observation"),
        Some(
            problem
                .selected_observation()
                .commitment_id()
                .to_string()
                .as_str()
        )
    );
    assert_eq!(
        projected.field("weighting.commitment.visibility_inner_product"),
        Some("hermitian_euclidean")
    );
    assert_eq!(
        projected.field("weighting.sources.0.flag_policy"),
        Some("flag_or_flag_row")
    );
    assert_eq!(
        projected.field("weighting.commitment.snapshot_identity"),
        Some(problem.inputs().observation().to_string().as_str())
    );
    assert_eq!(
        projected.field("weighting.sources.0.input_weight_column"),
        Some("weight")
    );
    assert_eq!(projected.field("products.requested.0"), Some("psf"));
    assert_eq!(
        projected.field("products.graph.identity"),
        Some(problem.product_graph().graph_id().to_string().as_str())
    );
    assert_eq!(projected.field("products.graph.schema_version"), Some("4"));
    assert_eq!(projected.field("products.graph.nodes.0.ordinal"), Some("0"));
    assert_eq!(
        projected.field("products.graph.publication.members.0"),
        Some("0")
    );
    assert_eq!(
        projected.field("products.normalization_boundary.input"),
        Some("unnormalized")
    );
    assert_eq!(
        projected.field("products.normalization_boundary.operations.0.kind"),
        Some("normalize")
    );
    assert_eq!(
        projected.field("products.normalization_boundary.operations.1.kind"),
        Some("convert_units")
    );
    assert_eq!(
        projected.field("numerics.reduction"),
        Some("unordered_within_budget")
    );
    assert_eq!(
        projected.field("geometry.domains.0.direction.projection"),
        Some("sin")
    );
    assert_eq!(
        projected.field("observation.sources.0.selection.rows.selected_count"),
        Some("1")
    );
    assert_eq!(
        projected.field("observation.sources.0.generations.columns.data"),
        Some(data_generation.as_str())
    );
    assert_eq!(
        projected.field("observation.sources.0.generations.metadata.antenna"),
        Some(antenna_generation.as_str())
    );
    assert_eq!(
        projected.field("observation.reference_data.measures"),
        Some(identity(82).to_string().as_str())
    );
    assert_eq!(projected.field("observation.model.kind"), Some("empty"));
    assert!(projected.fields().len() > 80);
}
