// SPDX-License-Identifier: LGPL-3.0-or-later
//! Aggregate validation must preserve the public bundle validator's diagnostics.

use casa_provider_contracts::{
    ContractValidationError, SurfaceCatalogBundle, SurfaceContractBundle, builtin_surface_catalog,
    validate_surface_bundles,
};

// The aggregate historically validates an owned bundle per surface, each
// carrying the full catalog. Keep that public operation as an independent
// reference for both valid inputs and malformed data.
fn validate_owned_bundles(
    catalog: &SurfaceCatalogBundle,
) -> Result<(), Vec<ContractValidationError>> {
    let bundles: Vec<_> = catalog
        .surfaces
        .iter()
        .cloned()
        .map(|surface| SurfaceContractBundle {
            schema_version: catalog.schema_version,
            surface,
            catalog: catalog.catalog.clone(),
        })
        .collect();
    validate_surface_bundles(&bundles)
}

#[test]
fn aggregate_validation_matches_owned_bundle_diagnostics() {
    let base = builtin_surface_catalog().unwrap();
    let mut cases = vec![("valid", base.clone())];

    let mut wrong_version = base.clone();
    wrong_version.schema_version += 1;
    cases.push(("schema version", wrong_version));

    let mut wrong_catalog_version = base.clone();
    wrong_catalog_version.catalog.schema_version += 1;
    cases.push(("catalog schema version", wrong_catalog_version));

    let mut duplicate_surface = base.clone();
    duplicate_surface.surfaces.push(base.surfaces[0].clone());
    cases.push(("duplicate surface", duplicate_surface));

    let mut missing_concept = base.clone();
    let reference = missing_concept.surfaces[0].bindings()[0].concept.clone();
    missing_concept
        .catalog
        .concepts
        .retain(|concept| concept.reference() != reference);
    cases.push(("missing concept", missing_concept));

    let mut divergent_concept = base.clone();
    let mut duplicate = divergent_concept.catalog.concepts[0].clone();
    duplicate.casa_name.push_str("_different");
    divergent_concept.catalog.concepts.push(duplicate);
    cases.push(("divergent concept", divergent_concept));

    let mut no_surfaces = base.clone();
    no_surfaces.surfaces.clear();
    cases.push(("no surfaces", no_surfaces));

    for (name, catalog) in cases {
        let expected = validate_owned_bundles(&catalog);
        match name {
            "valid" | "no surfaces" => assert!(expected.is_ok(), "{name}"),
            _ => {
                let errors = expected.as_ref().unwrap_err();
                let code = match name {
                    "schema version" | "catalog schema version" => "schema_version",
                    "duplicate surface" => "duplicate_surface",
                    "missing concept" => "missing_concept",
                    "divergent concept" => "divergent_concept",
                    _ => unreachable!(),
                };
                let count = errors.iter().filter(|error| error.code == code).count();
                assert!(count > 0, "{name} must report {code}");
                if matches!(
                    name,
                    "schema version" | "catalog schema version" | "divergent concept"
                ) {
                    assert_eq!(count, base.surfaces.len(), "{name} diagnostics per surface");
                }
            }
        }
        assert_eq!(catalog.validate(), expected, "{name}");
    }
}
