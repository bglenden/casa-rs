// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared validation for borrowed surface and catalog data.

use super::*;

/// A validation view avoids copying the catalog when many surfaces share it.
pub(super) struct SurfaceContractRef<'a> {
    pub(super) schema_version: u32,
    pub(super) surface: &'a SurfaceDefinition,
    pub(super) catalog: &'a ParameterCatalog,
}

impl<'a> From<&'a SurfaceContractBundle> for SurfaceContractRef<'a> {
    fn from(bundle: &'a SurfaceContractBundle) -> Self {
        Self {
            schema_version: bundle.schema_version,
            surface: &bundle.surface,
            catalog: &bundle.catalog,
        }
    }
}

pub(super) fn validate_surface_views(
    bundles: &[SurfaceContractRef<'_>],
    validate_cross_surface_names: bool,
) -> Result<(), Vec<ContractValidationError>> {
    let mut errors = Vec::new();
    let mut merged = BTreeMap::<(&ParameterConceptId, SemanticRevision), &ParameterConcept>::new();
    let mut surface_ids = BTreeSet::new();
    let mut last_stable_catalog: Option<&ParameterCatalog> = None;

    for bundle in bundles {
        if bundle.schema_version != PARAMETER_CATALOG_SCHEMA_VERSION
            || bundle.catalog.schema_version != PARAMETER_CATALOG_SCHEMA_VERSION
        {
            errors.push(ContractValidationError::new(
                "schema_version",
                format!(
                    "surface {} uses unsupported parameter schema {} / catalog {}",
                    bundle.surface.id(),
                    bundle.schema_version,
                    bundle.catalog.schema_version
                ),
            ));
        }
        // A former id names the surface in saved profiles, so it may name no
        // other surface, now or before.
        for id in std::iter::once(bundle.surface.id())
            .chain(bundle.surface.former_ids().iter().map(String::as_str))
        {
            if !surface_ids.insert(id.to_string()) {
                errors.push(ContractValidationError::new(
                    "duplicate_surface",
                    format!("duplicate surface {id}"),
                ));
            }
        }
        // Consecutive surfaces often borrow the very same catalog. A second
        // scan cannot add definitions or diagnostics if the first found no
        // divergent definitions. If it did, scan again: those errors are
        // intentionally reported for every surface.
        if !last_stable_catalog.is_some_and(|last| std::ptr::eq(last, bundle.catalog)) {
            let mut stable = true;
            for concept in &bundle.catalog.concepts {
                let key = (&concept.id, concept.semantic_revision);
                match merged.get(&key) {
                    Some(existing) if *existing != concept => {
                        stable = false;
                        errors.push(ContractValidationError::new(
                            "divergent_concept",
                            format!(
                                "concept {} revision {} has divergent definitions",
                                concept.id, concept.semantic_revision.0
                            ),
                        ));
                    }
                    Some(_) => {}
                    None => {
                        // A definition containing NaN is not equal to itself.
                        // The next surface must still compare it and report
                        // the same divergence as the full scan.
                        if !concept.eq(concept) {
                            stable = false;
                        }
                        if let NormalizationRule::CasaSelector { capabilities, .. } =
                            &concept.normalization
                        {
                            for capability in capabilities.iter().filter(|capability| {
                                !SELECTOR_CAPABILITY_NAMES.contains(&capability.as_str())
                            }) {
                                errors.push(ContractValidationError::new(
                                    "unknown_selector_capability",
                                    format!(
                                        "concept {} declares unknown selector capability {capability:?}",
                                        concept.id
                                    ),
                                ));
                            }
                        }
                        merged.insert(key, concept);
                    }
                }
            }
            last_stable_catalog = stable.then_some(bundle.catalog);
        }
        validate_one_surface(bundle, &mut errors);
    }

    if validate_cross_surface_names {
        validate_repeated_names(bundles, &mut errors);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reused_and_distinct_catalogs_preserve_error_order() {
        let base = crate::builtin_surface_catalog().unwrap();
        let first = &base.catalog;
        let mut different = first.clone();
        different.concepts[0].casa_name.push_str("_different");
        let surfaces = [
            (&base.surfaces[0], first, 999),
            (&base.surfaces[1], first, base.schema_version),
            (&base.surfaces[2], &different, base.schema_version),
            (&base.surfaces[0], first, base.schema_version),
        ];
        let views: Vec<_> = surfaces
            .iter()
            .map(|(surface, catalog, schema_version)| SurfaceContractRef {
                schema_version: *schema_version,
                surface,
                catalog,
            })
            .collect();
        let owned: Vec<_> = surfaces
            .iter()
            .map(|(surface, catalog, schema_version)| SurfaceContractBundle {
                schema_version: *schema_version,
                surface: (*surface).clone(),
                catalog: (*catalog).clone(),
            })
            .collect();
        let expected = validate_surface_bundles(&owned);
        let errors = expected.as_ref().unwrap_err();
        for code in ["schema_version", "divergent_concept", "duplicate_surface"] {
            assert!(errors.iter().any(|error| error.code == code), "{code}");
        }
        assert_eq!(validate_surface_views(&views, true), expected);
    }
}
