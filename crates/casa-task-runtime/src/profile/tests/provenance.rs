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
