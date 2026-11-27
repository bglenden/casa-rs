// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

fn concept(id: &str, name: &str, value_domain: ParameterType) -> ParameterConcept {
    ParameterConcept {
        id: id.into(),
        semantic_revision: SemanticRevision(1),
        casa_name: name.to_string(),
        value_domain,
        normalization: NormalizationRule::Identity,
        base_constraints: Vec::new(),
        unit_dimension: None,
        semantic_role: ParameterRole::Algorithm,
        documentation: ParameterDocs {
            summary: name.to_string(),
            details: None,
            examples: Vec::new(),
        },
        persistence_class: PersistenceClass::Profile,
    }
}

fn binding(concept: &ParameterConcept, default: DefaultSpec) -> SurfaceParameterBinding {
    SurfaceParameterBinding {
        name: concept.casa_name.clone(),
        concept: concept.reference(),
        order: 0,
        default,
        required_when: Predicate::Never,
        active_when: Predicate::Always,
        refinements: Vec::new(),
        context_role: None,
        surface_note: None,
        projections: ParameterProjections {
            cli: None,
            provider: None,
            python: None,
            presentation: ParameterPresentation {
                label: concept.casa_name.clone(),
                group: "General".to_string(),
                advanced: false,
                hidden: false,
            },
        },
        reviewed_homonym: None,
        unimplemented: None,
        unimplemented_forms: None,
    }
}

fn bundle(concept: ParameterConcept, binding: SurfaceParameterBinding) -> SurfaceContractBundle {
    SurfaceContractBundle {
        schema_version: PARAMETER_CATALOG_SCHEMA_VERSION,
        surface: SurfaceDefinition::Task(TaskDefinition {
            id: "example".to_string(),
            contract_version: 1,
            display_name: "Example".to_string(),
            category: "Test".to_string(),
            summary: "Example".to_string(),
            provider_family: "test".to_string(),
            casa_task: None,
            execution: SurfaceExecutionProjection::default(),
            bindings: vec![binding],
        }),
        catalog: ParameterCatalog {
            schema_version: PARAMETER_CATALOG_SCHEMA_VERSION,
            concepts: vec![concept],
        },
    }
}

fn set_surface_id(bundle: &mut SurfaceContractBundle, id: &str) {
    let SurfaceDefinition::Task(definition) = &mut bundle.surface;
    definition.id = id.to_string();
}

#[test]
fn valid_bundle_passes() {
    let concept = concept("example.name", "name", ParameterType::String);
    let binding = binding(
        &concept,
        DefaultSpec::Literal {
            value: "default".into(),
        },
    );
    bundle(concept, binding).validate().unwrap();
}

#[test]
fn unsupported_pattern_constraints_are_rejected() {
    let serialized = serde_json::json!({"kind": "pattern", "pattern": "^value$"});
    assert!(serde_json::from_value::<Constraint>(serialized).is_err());
}

fn overwrite_bundle(default: bool) -> SurfaceContractBundle {
    let concept = concept(OVERWRITE_CONCEPT_ID, "overwrite", ParameterType::Bool);
    let binding = binding(
        &concept,
        DefaultSpec::Literal {
            value: ParameterValue::Bool(default),
        },
    );
    bundle(concept, binding)
}

#[test]
fn overwrite_defaults_to_false() {
    overwrite_bundle(false).validate().unwrap();
    let errors = overwrite_bundle(true).validate().unwrap_err();
    assert!(
        errors.iter().any(|error| error.code == "overwrite_default"),
        "{errors:?}"
    );
}

#[test]
fn known_casa_tasks_keep_their_overwrite_default() {
    for surface in CASA_OVERWRITE_BY_DEFAULT_TASKS {
        for (default, valid) in [(true, true), (false, false)] {
            let mut bundle = overwrite_bundle(default);
            let SurfaceDefinition::Task(definition) = &mut bundle.surface;
            definition.id = surface.to_string();
            assert_eq!(
                bundle.validate().is_ok(),
                valid,
                "{surface}: default {default}"
            );
        }
    }
}

#[test]
fn reserved_name_requires_designated_concept() {
    let concept = concept("wrong.imsize", "imsize", ParameterType::Integer);
    let binding = binding(
        &concept,
        DefaultSpec::Literal {
            value: 512_i64.into(),
        },
    );
    let errors = bundle(concept, binding).validate().unwrap_err();
    assert!(errors.iter().any(|error| error.code == "reserved_name"));
}

#[test]
fn runtime_authority_names_are_forbidden_from_profile_bindings() {
    for name in [
        "binary",
        "binary_path",
        "executable",
        "executable_args",
        "override_env",
        "override_env_path",
        "confirm_overwrite",
        "confirmation_token",
        "no_save_last",
        "no_save_session",
        "workspace",
        "workspace_root",
        "runtime_timeout",
        "telemetry_elapsed_ms",
        "ui_selected_tab",
    ] {
        let concept = concept(&format!("example.{name}"), name, ParameterType::String);
        let binding = binding(
            &concept,
            DefaultSpec::Literal {
                value: "value".into(),
            },
        );
        let errors = bundle(concept, binding).validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.code == "forbidden_profile_parameter_name"),
            "{name}: {errors:?}"
        );
    }

    let allowed = concept(
        "example.confirmatory_model",
        "confirmatory_model",
        ParameterType::String,
    );
    let allowed_binding = binding(
        &allowed,
        DefaultSpec::Literal {
            value: "science".into(),
        },
    );
    bundle(allowed, allowed_binding).validate().unwrap();
}

#[test]
fn quantity_defaults_use_real_unit_parsing_and_dimension_checks() {
    let mut quantity = concept(
        "example.angle",
        "angle",
        ParameterType::Quantity {
            dimension: UnitDimension::Angle,
            canonical_unit: "arcsec".to_string(),
            special_values: vec!["auto".to_string()],
        },
    );
    quantity.normalization = NormalizationRule::Quantity {
        canonical_unit: "arcsec".to_string(),
    };
    quantity.unit_dimension = Some(UnitDimension::Angle);

    for invalid in ["banana", "1bogus", "1s", "1e999arcsec"] {
        let binding = binding(
            &quantity,
            DefaultSpec::Literal {
                value: invalid.into(),
            },
        );
        let errors = bundle(quantity.clone(), binding).validate().unwrap_err();
        assert!(
            errors.iter().any(|error| error.code == "invalid_default"),
            "{invalid}: {errors:?}"
        );
    }

    for valid in ["1deg", "0.5arcsec", "1", "auto"] {
        let binding = binding(
            &quantity,
            DefaultSpec::Literal {
                value: valid.into(),
            },
        );
        bundle(quantity.clone(), binding)
            .validate()
            .unwrap_or_else(|errors| panic!("{valid}: {errors:?}"));
    }

    let pair = concept(
        "example.angle_pair",
        "angle_pair",
        ParameterType::Array {
            element: Box::new(quantity.value_domain.clone()),
            min_items: 2,
            max_items: Some(2),
            allow_scalar: true,
        },
    );
    let malformed_element = binding(
        &pair,
        DefaultSpec::Literal {
            value: ParameterValue::Array(vec!["1arcsec".into(), "banana".into()]),
        },
    );
    let errors = bundle(pair, malformed_element).validate().unwrap_err();
    assert!(errors.iter().any(|error| error.code == "invalid_default"));

    for (canonical_unit, expected_code) in [
        ("bananas", "invalid_value_domain"),
        ("s", "invalid_value_domain"),
    ] {
        let invalid_domain = concept(
            "example.invalid_angle",
            "invalid_angle",
            ParameterType::Quantity {
                dimension: UnitDimension::Angle,
                canonical_unit: canonical_unit.to_string(),
                special_values: vec!["auto".to_string()],
            },
        );
        let binding = binding(
            &invalid_domain,
            DefaultSpec::Literal {
                value: "auto".into(),
            },
        );
        let errors = bundle(invalid_domain, binding).validate().unwrap_err();
        assert!(
            errors.iter().any(|error| error.code == expected_code),
            "{canonical_unit}: {errors:?}"
        );
    }

    let unsupported_data_size = concept(
        "example.data_size",
        "data_size",
        ParameterType::Quantity {
            dimension: UnitDimension::DataSize,
            canonical_unit: String::new(),
            special_values: vec!["auto".to_string()],
        },
    );
    let binding = binding(
        &unsupported_data_size,
        DefaultSpec::Literal {
            value: "auto".into(),
        },
    );
    let errors = bundle(unsupported_data_size, binding)
        .validate()
        .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "invalid_value_domain")
    );
}

#[test]
fn scalar_is_valid_for_scalar_or_array_domain() {
    let domain = ParameterType::Array {
        element: Box::new(ParameterType::Integer),
        min_items: 2,
        max_items: Some(2),
        allow_scalar: true,
    };
    assert!(validate_parameter_value(&512_i64.into(), &domain).is_ok());
    assert!(
        validate_parameter_value(
            &ParameterValue::Array(vec![512_i64.into(), 256_i64.into()]),
            &domain
        )
        .is_ok()
    );
}

#[test]
fn divergent_concept_revision_is_rejected() {
    let first = concept("example.name", "name", ParameterType::String);
    let binding = binding(&first, DefaultSpec::Literal { value: "a".into() });
    let first_bundle = bundle(first.clone(), binding.clone());
    let mut second = first;
    second.documentation.summary = "different".to_string();
    let mut second_bundle = bundle(second, binding);
    set_surface_id(&mut second_bundle, "second");
    let errors = validate_surface_bundles(&[first_bundle, second_bundle]).unwrap_err();
    assert!(errors.iter().any(|error| error.code == "divergent_concept"));
}

#[test]
fn repeated_names_require_one_concept_or_an_explicit_homonym_review() {
    let primary = concept("example.a_primary", "shared", ParameterType::String);
    let primary_binding = binding(
        &primary,
        DefaultSpec::Literal {
            value: "primary".into(),
        },
    );
    let first = bundle(primary.clone(), primary_binding.clone());
    let mut same_concept = bundle(primary, primary_binding);
    set_surface_id(&mut same_concept, "same-concept");
    validate_surface_bundles(&[first.clone(), same_concept]).unwrap();

    let mut redundant = first.clone();
    set_surface_id(&mut redundant, "redundant");
    let SurfaceDefinition::Task(definition) = &mut redundant.surface;
    definition.bindings[0].reviewed_homonym = Some(ReviewedHomonym {
        review_id: "ADR-test".to_string(),
        reason: "Redundant evidence for an identical concept.".to_string(),
    });
    let errors = validate_surface_bundles(&[first.clone(), redundant]).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "redundant_homonym_review")
    );

    let alternate = concept("example.z_alternate", "shared", ParameterType::String);
    let alternate_binding = binding(
        &alternate,
        DefaultSpec::Literal {
            value: "alternate".into(),
        },
    );
    let mut unreviewed = bundle(alternate.clone(), alternate_binding.clone());
    set_surface_id(&mut unreviewed, "unreviewed");
    let errors = validate_surface_bundles(&[first.clone(), unreviewed]).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "unreviewed_homonym")
    );

    let mut reviewed = bundle(alternate, alternate_binding);
    set_surface_id(&mut reviewed, "reviewed");
    let SurfaceDefinition::Task(definition) = &mut reviewed.surface;
    definition.bindings[0].reviewed_homonym = Some(ReviewedHomonym {
        review_id: "ADR-test".to_string(),
        reason: "The shared spelling has a deliberately distinct meaning.".to_string(),
    });
    reviewed
        .validate()
        .expect("an isolated provider bundle cannot judge catalog-level homonyms");
    validate_surface_bundles(&[first, reviewed]).unwrap();
}

#[test]
fn bindings_reject_broadening_refinements_and_invalid_defaults() {
    let choice = concept(
        "example.choice",
        "choice",
        ParameterType::Choice {
            values: vec!["a".to_string(), "b".to_string()],
        },
    );
    let mut broadening = binding(&choice, DefaultSpec::Literal { value: "a".into() });
    broadening.refinements = vec![NarrowingConstraint::AllowedValues {
        values: vec!["a".to_string(), "c".to_string()],
    }];
    let errors = bundle(choice.clone(), broadening).validate().unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "non_narrowing_refinement")
    );

    let mut excluded_default = binding(&choice, DefaultSpec::Literal { value: "a".into() });
    excluded_default.refinements = vec![NarrowingConstraint::AllowedValues {
        values: vec!["b".to_string()],
    }];
    let errors = bundle(choice, excluded_default).validate().unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "default_refinement")
    );
    assert!(
        errors
            .iter()
            .all(|error| error.code != "non_narrowing_refinement")
    );

    let integer = concept("example.integer", "integer", ParameterType::Integer);
    let wrong_type = binding(
        &integer,
        DefaultSpec::Literal {
            value: "not-an-integer".into(),
        },
    );
    let errors = bundle(integer, wrong_type).validate().unwrap_err();
    assert!(errors.iter().any(|error| error.code == "invalid_default"));

    let mut constrained = concept("example.constrained", "constrained", ParameterType::Integer);
    constrained.base_constraints = vec![Constraint::NumberRange {
        min: Some(0.0),
        max: Some(10.0),
        min_inclusive: true,
        max_inclusive: true,
    }];
    let outside_base_range = binding(
        &constrained,
        DefaultSpec::Literal {
            value: 11_i64.into(),
        },
    );
    let errors = bundle(constrained, outside_base_range)
        .validate()
        .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "default_constraint")
    );

    let mut pair = concept(
        "example.pair",
        "pair",
        ParameterType::Array {
            element: Box::new(ParameterType::Integer),
            min_items: 2,
            max_items: Some(2),
            allow_scalar: false,
        },
    );
    pair.base_constraints = vec![Constraint::Positive];
    let nonpositive_element = binding(
        &pair,
        DefaultSpec::Literal {
            value: ParameterValue::Array(vec![1_i64.into(), 0_i64.into()]),
        },
    );
    let errors = bundle(pair.clone(), nonpositive_element)
        .validate()
        .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "default_constraint")
    );

    let mut out_of_range_element = binding(
        &pair,
        DefaultSpec::Literal {
            value: ParameterValue::Array(vec![2_i64.into(), 0_i64.into()]),
        },
    );
    out_of_range_element.refinements = vec![NarrowingConstraint::NumberRange {
        min: Some(1.0),
        max: None,
        min_inclusive: true,
        max_inclusive: true,
    }];
    let errors = bundle(pair, out_of_range_element).validate().unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "default_refinement")
    );
    assert!(
        errors
            .iter()
            .all(|error| error.code != "non_narrowing_refinement")
    );

    let required = concept("example.required", "required", ParameterType::String);
    let conditionally_optional = binding(&required, DefaultSpec::Required);
    let errors = bundle(required, conditionally_optional)
        .validate()
        .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "missing_optional_default")
    );
}

#[test]
fn selector_refinements_validate_defaults_and_capability_names() {
    let mut selector = concept("example.selector", "selector", ParameterType::String);
    selector.normalization = NormalizationRule::CasaSelector {
        grammar: SelectorGrammar::Field,
        capabilities: vec![
            "ids".into(),
            "names".into(),
            "ranges".into(),
            "wildcards".into(),
        ],
    };
    let mut wildcard_default = binding(
        &selector,
        DefaultSpec::Literal {
            value: "target*".into(),
        },
    );
    wildcard_default.refinements = vec![NarrowingConstraint::SelectorCapabilities {
        capabilities: vec!["ids".into(), "names".into(), "ranges".into()],
    }];
    let errors = bundle(selector.clone(), wildcard_default)
        .validate()
        .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "default_refinement")
    );

    let mut unknown = binding(
        &selector,
        DefaultSpec::Literal {
            value: "target".into(),
        },
    );
    unknown.refinements = vec![NarrowingConstraint::SelectorCapabilities {
        capabilities: vec!["ids".into(), "typo".into()],
    }];
    let errors = bundle(selector.clone(), unknown).validate().unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "non_narrowing_refinement")
    );

    let mut malformed = selector;
    let NormalizationRule::CasaSelector { capabilities, .. } = &mut malformed.normalization else {
        unreachable!()
    };
    capabilities.push("typo".into());
    let malformed_binding = binding(
        &malformed,
        DefaultSpec::Literal {
            value: "target".into(),
        },
    );
    let errors = bundle(malformed, malformed_binding).validate().unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "unknown_selector_capability")
    );
}

#[test]
fn conditional_default_cycles_are_rejected() {
    let first = concept("example.a", "a", ParameterType::String);
    let second = concept("example.b", "b", ParameterType::String);
    let mut first_binding = binding(
        &first,
        DefaultSpec::Conditional {
            cases: vec![ConditionalDefault {
                when: Predicate::IsSet {
                    parameter: "b".to_string(),
                },
                value: "x".into(),
            }],
            fallback: "y".into(),
        },
    );
    first_binding.order = 0;
    let mut second_binding = binding(
        &second,
        DefaultSpec::Conditional {
            cases: vec![ConditionalDefault {
                when: Predicate::IsSet {
                    parameter: "a".to_string(),
                },
                value: "x".into(),
            }],
            fallback: "y".into(),
        },
    );
    second_binding.order = 1;
    let bundle = SurfaceContractBundle {
        schema_version: PARAMETER_CATALOG_SCHEMA_VERSION,
        surface: SurfaceDefinition::Task(TaskDefinition {
            id: "cycle".to_string(),
            contract_version: 1,
            display_name: "Cycle".to_string(),
            category: "Test".to_string(),
            summary: "Cycle".to_string(),
            provider_family: "test".to_string(),
            casa_task: None,
            execution: SurfaceExecutionProjection::default(),
            bindings: vec![first_binding, second_binding],
        }),
        catalog: ParameterCatalog {
            schema_version: PARAMETER_CATALOG_SCHEMA_VERSION,
            concepts: vec![first, second],
        },
    };
    let errors = bundle.validate().unwrap_err();
    assert!(errors.iter().any(|error| error.code == "default_cycle"));
}

#[test]
fn input_and_output_context_roles_must_match_concept_roles() {
    let concept = concept("example.path", "path", ParameterType::String);
    let mut binding = binding(
        &concept,
        DefaultSpec::Literal {
            value: "input".into(),
        },
    );
    binding.context_role = Some(ContextRole::InputDataset);
    let errors = bundle(concept, binding).validate().unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == "context_role_mismatch")
    );
}
