// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolved profile states and original explicit intent are distinct.

use super::*;

#[test]
fn sparse_round_trip_normalizes_scalar_pair_and_quantity_units() {
    let bundle = bundle(SurfaceKind::Task);
    let source = r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
imsize = 1024
cell = "0.0002777777777777778deg"
"#;
    let resolved = resolve_profile(&parse_profile(source).unwrap(), &bundle).unwrap();
    assert_eq!(
        resolved.values["imsize"],
        ParameterValue::Array(vec![ParameterValue::Integer(1024); 2])
    );
    assert_eq!(
        resolved.values["cell"],
        ParameterValue::Array(vec![ParameterValue::String("1arcsec".into()); 2])
    );
    assert_eq!(
        resolved.explicit_overrides["imsize"],
        ParameterValue::Integer(1024)
    );
    assert_eq!(
        resolved.explicit_overrides["cell"],
        ParameterValue::String("0.0002777777777777778deg".into())
    );
    let rendered = render_sparse_profile(&bundle, &resolved.values).unwrap();
    assert!(rendered.contains("imsize = [1024, 1024]"));
    assert!(!rendered.contains("cell ="));
    assert_eq!(
        resolve_profile(&parse_profile(&rendered).unwrap(), &bundle)
            .unwrap()
            .values,
        resolved.values
    );
}

#[test]
fn original_alias_values_survive_under_canonical_binding_names() {
    let mut bundle = bundle(SurfaceKind::Task);
    let SurfaceDefinition::Task(definition) = &mut bundle.surface;
    definition
        .bindings
        .iter_mut()
        .find(|binding| binding.name == "imsize")
        .unwrap()
        .aliases
        .push("pixels".into());
    let source = r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1
[parameters]
pixels = 128
"#;
    let resolved = resolve_profile(&parse_profile(source).unwrap(), &bundle).unwrap();
    assert_eq!(
        resolved.explicit_overrides["imsize"],
        ParameterValue::Integer(128)
    );
    assert!(!resolved.explicit_overrides.contains_key("pixels"));
    assert_eq!(
        resolved.values["imsize"],
        ParameterValue::Array(vec![ParameterValue::Integer(128); 2])
    );
}

#[test]
fn exact_explicit_value_migrations_report_the_parameter_and_stored_conversion() {
    let mut bundle = bundle(SurfaceKind::Task);
    let SurfaceDefinition::Task(definition) = &mut bundle.surface;
    definition.contract_version = 2;
    definition.migrations = vec![casa_provider_contracts::SurfaceMigration {
        from_contract: 1,
        to_contract: 2,
        steps: vec![casa_provider_contracts::MigrationStep::ReplaceValue {
            parameter: "cell".into(),
            from: ParameterValue::String("60arcsec".into()),
            to: ParameterValue::String("1arcsec".into()),
        }],
        changed_defaults: vec![],
    }];
    for (version, text, converted) in [
        (1, "60arcsec", true),
        (1, "2arcsec", false),
        (2, "60arcsec", false),
    ] {
        let source = format!(
            "[casars]\nformat=1\nsurface=\"imager\"\nkind=\"task\"\ncontract={version}\n[parameters]\ncell=\"{text}\"\n"
        );
        let resolved = resolve_profile(&parse_profile(&source).unwrap(), &bundle).unwrap();
        let notices: Vec<_> = resolved
            .diagnostics
            .iter()
            .filter(|d| {
                d.code == DiagnosticCode::Migrated && d.parameter.as_deref() == Some("cell")
            })
            .collect();
        assert_eq!(notices.len(), usize::from(converted));
        if let Some(notice) = notices.first() {
            assert!(notice.message.contains("60arcsec") && notice.message.contains("1arcsec"));
            assert!(notice.message.contains("contract 1 to 2"));
            assert!(notice.message.contains("stored legacy representation"));
            assert_eq!(
                resolved.explicit_overrides["cell"],
                ParameterValue::String("1arcsec".into())
            );
        }
    }
}

#[test]
fn multi_hop_migrations_transform_and_preserve_explicit_old_values() {
    let mut bundle = bundle(SurfaceKind::Task);
    let SurfaceDefinition::Task(definition) = &mut bundle.surface;
    definition.contract_version = 3;
    definition.migrations = vec![
        SurfaceMigration {
            from_contract: 1,
            to_contract: 2,
            steps: vec![
                MigrationStep::Transform {
                    parameter: "imsize".into(),
                    transform: MigrationTransform::ScalarToArray { length: 2 },
                },
                MigrationStep::ReplaceValue {
                    parameter: "cell".into(),
                    from: ParameterValue::String("60arcsec".into()),
                    to: ParameterValue::String("1arcmin".into()),
                },
            ],
            changed_defaults: vec!["imsize".into()],
        },
        SurfaceMigration {
            from_contract: 2,
            to_contract: 3,
            steps: vec![MigrationStep::Transform {
                parameter: "cell".into(),
                transform: MigrationTransform::QuantityUnit {
                    canonical_unit: "arcsec".into(),
                },
            }],
            changed_defaults: vec!["cell".into()],
        },
    ];
    definition
        .bindings
        .iter_mut()
        .find(|binding| binding.name == "imsize")
        .unwrap()
        .default = DefaultSpec::Literal {
        value: ParameterValue::Integer(1024),
    };
    definition
        .bindings
        .iter_mut()
        .find(|binding| binding.name == "cell")
        .unwrap()
        .default = DefaultSpec::Literal {
        value: ParameterValue::String("2arcsec".into()),
    };

    let old = parse_profile(
        r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
imsize = 128
cell = "60arcsec"
"#,
    )
    .unwrap();
    let resolved = resolve_profile(&old, &bundle).unwrap();
    assert_eq!(
        resolved.values["imsize"],
        ParameterValue::Array(vec![ParameterValue::Integer(128); 2])
    );
    assert_eq!(
        resolved.values["cell"],
        ParameterValue::Array(vec![ParameterValue::String("60arcsec".into()); 2])
    );
    assert_eq!(
        resolved.explicit_overrides,
        BTreeMap::from([
            (
                "imsize".into(),
                ParameterValue::Array(vec![ParameterValue::Integer(128); 2])
            ),
            ("cell".into(), ParameterValue::String("60arcsec".into())),
        ]),
        "migration transforms explicit intent, but domain normalization does not replace it"
    );
    assert_eq!(
        resolved
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.code == DiagnosticCode::Migrated
                && diagnostic.parameter.is_none())
            .count(),
        2
    );
    assert!(
        resolved
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.code != DiagnosticCode::DefaultChanged)
    );
    let conversions: Vec<_> = resolved
        .diagnostics
        .iter()
        .filter(|diagnostic| {
            diagnostic.code == DiagnosticCode::Migrated
                && diagnostic.parameter.as_deref() == Some("cell")
        })
        .collect();
    assert_eq!(conversions.len(), 1);
    assert!(
        conversions[0].message.contains("60arcsec") && conversions[0].message.contains("1arcmin")
    );

    let saved = render_sparse_profile(&bundle, &resolved.values).unwrap();
    assert!(saved.contains("contract = 3"));
    assert!(saved.contains("imsize = [128, 128]"));
    assert!(saved.contains("cell = [\"60arcsec\", \"60arcsec\"]"));

    let mut incompatible = bundle.clone();
    let SurfaceDefinition::Task(definition) = &mut incompatible.surface;
    definition.migrations[1].steps = vec![MigrationStep::Transform {
        parameter: "cell".into(),
        transform: MigrationTransform::QuantityUnit {
            canonical_unit: "s".into(),
        },
    }];
    assert!(matches!(
        resolve_profile(&old, &incompatible),
        Err(ProfileError::Normalization(NormalizationError::Quantity(_)))
    ));
}
