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
    let mut merged = BTreeMap::<(ParameterConceptId, SemanticRevision), &ParameterConcept>::new();
    let mut surface_ids = BTreeSet::new();

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
        for concept in &bundle.catalog.concepts {
            let key = (concept.id.clone(), concept.semantic_revision);
            match merged.get(&key) {
                Some(existing) if *existing != concept => {
                    errors.push(ContractValidationError::new(
                        "divergent_concept",
                        format!(
                            "concept {} revision {} has divergent definitions",
                            concept.id, concept.semantic_revision.0
                        ),
                    ))
                }
                Some(_) => {}
                None => {
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
