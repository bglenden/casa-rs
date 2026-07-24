// SPDX-License-Identifier: LGPL-3.0-or-later
//! Checked built-in aggregate catalog for every current configurable surface.

use std::sync::OnceLock;

use serde::Deserialize;

use crate::{ParameterCatalog, SurfaceCatalogBundle, SurfaceContractBundle, SurfaceDefinition};

const PARAMETER_CATALOG_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/parameter-catalog.json"
));
const PARAMETER_SURFACES_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/parameter-surfaces.json"
));

#[derive(Deserialize)]
struct SurfaceIndex {
    schema_version: u32,
    surfaces: Vec<SurfaceDefinition>,
}

/// Return the validated aggregate built into this library.
pub fn builtin_surface_catalog() -> Result<&'static SurfaceCatalogBundle, String> {
    static BUILTIN: OnceLock<Result<SurfaceCatalogBundle, String>> = OnceLock::new();
    BUILTIN
        .get_or_init(|| {
            let catalog = serde_json::from_str::<ParameterCatalog>(PARAMETER_CATALOG_JSON)
                .map_err(|error| format!("parse provider parameter-catalog.json: {error}"))?;
            let index = serde_json::from_str::<SurfaceIndex>(PARAMETER_SURFACES_JSON)
                .map_err(|error| format!("parse provider parameter-surfaces.json: {error}"))?;
            let bundle = SurfaceCatalogBundle {
                schema_version: index.schema_version,
                catalog,
                surfaces: index.surfaces,
            };
            bundle.validate().map_err(|errors| {
                errors
                    .into_iter()
                    .map(|error| {
                        format!(
                            "{}{}{}: {}",
                            error.code,
                            error
                                .surface
                                .as_deref()
                                .map_or_else(String::new, |surface| format!(" [{surface}]")),
                            error
                                .parameter
                                .as_deref()
                                .map_or_else(String::new, |parameter| format!(".{parameter}")),
                            error.message
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })?;
            Ok(bundle)
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// Return one self-contained surface bundle with only its referenced concepts.
pub fn builtin_surface_bundle(id: &str) -> Result<SurfaceContractBundle, String> {
    builtin_surface_catalog()?
        .embedded_surface(id)
        .ok_or_else(|| format!("unknown configurable surface {id:?}"))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::{
        CELL_CONCEPT_ID, IMSIZE_CONCEPT_ID, MigrationStep, OVERWRITE_CONCEPT_ID, ParameterRole,
        ParameterType, ParameterValue, Predicate, ResourceKind, RunProductSource, RunSafetyClass,
        SemanticRevision, SurfaceKind, SurfaceProductContract, ValueAdapter,
    };

    use super::*;

    #[test]
    fn builtins_cover_exact_current_configurable_catalog() {
        let catalog = builtin_surface_catalog().expect("valid built-in parameter catalog");
        assert_eq!(catalog.surfaces.len(), 42);
        assert_eq!(
            catalog
                .surfaces
                .iter()
                .filter(|surface| surface.kind() == SurfaceKind::Task)
                .count(),
            40
        );
        assert_eq!(
            catalog
                .surfaces
                .iter()
                .filter(|surface| surface.kind() == SurfaceKind::Session)
                .count(),
            2
        );
        let ids = catalog
            .surfaces
            .iter()
            .map(|surface| surface.id())
            .collect::<BTreeSet<_>>();
        assert!(ids.contains("msexplore"));
        assert!(ids.contains("imexplore"));
        assert!(ids.contains("tablebrowser"));
        assert!(!ids.contains("casars"));
    }

    #[test]
    fn imsize_and_cell_have_one_invariant_domain() {
        let catalog = builtin_surface_catalog().unwrap();
        let imsize = catalog
            .catalog
            .concepts
            .iter()
            .find(|concept| concept.id.as_str() == IMSIZE_CONCEPT_ID)
            .unwrap();
        assert!(matches!(
            imsize.value_domain,
            ParameterType::Optional {
                ref value,
                ref states,
            } if states == &["auto".to_string()] && matches!(
                value.as_ref(),
                ParameterType::Array {
                    min_items: 2,
                    max_items: Some(2),
                    allow_scalar: true,
                    ..
                }
            )
        ));
        let cell = catalog
            .catalog
            .concepts
            .iter()
            .find(|concept| concept.id.as_str() == CELL_CONCEPT_ID)
            .unwrap();
        assert!(matches!(
            cell.value_domain,
            ParameterType::Array {
                min_items: 2,
                max_items: Some(2),
                allow_scalar: true,
                ..
            }
        ));
        let mut imsize_bindings = 0;
        let mut cell_bindings = 0;
        for surface in &catalog.surfaces {
            for binding in surface
                .bindings()
                .iter()
                .filter(|binding| binding.name == "imsize")
            {
                assert_eq!(binding.concept, imsize.reference());
                imsize_bindings += 1;
            }
            for binding in surface
                .bindings()
                .iter()
                .filter(|binding| binding.name == "cell")
            {
                assert_eq!(binding.concept, cell.reference());
                cell_bindings += 1;
            }
        }
        assert_eq!(imsize_bindings, 3);
        assert_eq!(cell_bindings, 3);
    }

    #[test]
    fn shared_selectors_use_one_canonical_absence_value_and_adapter() {
        let catalog = builtin_surface_catalog().unwrap();
        for surface in &catalog.surfaces {
            for binding in surface.bindings().iter().filter(|binding| {
                binding.concept.id.as_str().starts_with("ms.selection.")
                    || binding.concept.id.as_str().starts_with("image.selection.")
            }) {
                let crate::DefaultSpec::Literal {
                    value: ParameterValue::String(default),
                } = &binding.default
                else {
                    continue;
                };
                if default == "none" {
                    assert_eq!(
                        binding
                            .projections
                            .provider
                            .as_ref()
                            .map(|projection| &projection.adapter),
                        Some(&crate::ValueAdapter::OmitNone),
                        "{}.{}",
                        surface.id(),
                        binding.name
                    );
                }
                assert_ne!(default, "", "{}.{}", surface.id(), binding.name);
            }
        }
    }

    #[test]
    fn every_overwrite_binding_has_the_exact_fail_closed_safety_rule() {
        let catalog = builtin_surface_catalog().expect("valid built-in parameter catalog");
        let surfaces = catalog
            .surfaces
            .iter()
            .filter_map(|surface| {
                surface
                    .bindings()
                    .iter()
                    .find(|binding| binding.concept.id.as_str() == OVERWRITE_CONCEPT_ID)
                    .map(|binding| (surface, binding))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            surfaces.len(),
            23,
            "update the safety inventory intentionally"
        );
        for (surface, binding) in surfaces {
            let rule = surface
                .safety_rules()
                .iter()
                .find(|rule| rule.class == RunSafetyClass::Overwrite)
                .unwrap_or_else(|| panic!("{} is missing overwrite safety", surface.id()));
            assert_eq!(
                rule.when,
                Predicate::Equals {
                    parameter: binding.name.clone(),
                    value: ParameterValue::Bool(true),
                },
                "{} must confirm every resolved overwrite=true",
                surface.id()
            );
        }
    }

    #[test]
    fn sessions_do_not_claim_one_shot_run_safety() {
        let catalog = builtin_surface_catalog().expect("valid built-in parameter catalog");
        assert!(
            catalog
                .surfaces
                .iter()
                .filter(|surface| surface.kind() == SurfaceKind::Session)
                .all(|surface| surface.safety_rules().is_empty())
        );
    }

    #[test]
    fn browser_content_modes_are_explicit_reviewed_homonyms() {
        let catalog = builtin_surface_catalog().expect("valid built-in parameter catalog");
        let mut bindings = catalog
            .surfaces
            .iter()
            .filter_map(|surface| {
                surface
                    .bindings()
                    .iter()
                    .find(|binding| binding.name == "contentmode")
                    .map(|binding| (surface.id(), binding))
            })
            .collect::<Vec<_>>();
        bindings.sort_by_key(|(surface, _)| *surface);
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].0, "imexplore");
        assert_eq!(
            bindings[0].1.concept.id.as_str(),
            "image.browser.contentmode"
        );
        assert!(bindings[0].1.reviewed_homonym.is_none());
        assert_eq!(bindings[1].0, "tablebrowser");
        assert_eq!(
            bindings[1].1.concept.id.as_str(),
            "tablebrowser.contentmode"
        );
        assert!(bindings[1].1.reviewed_homonym.is_some());
    }

    #[test]
    fn simobserve_modes_have_disjoint_provider_inputs_and_shared_worker_controls() {
        let catalog = builtin_surface_catalog().expect("valid built-in parameter catalog");
        let surface = catalog.surface("simobserve").unwrap();
        let predicate = |value: &str| Predicate::Equals {
            parameter: "request_kind".to_string(),
            value: ParameterValue::String(value.to_string()),
        };
        for name in ["model", "out", "inbright", "duration", "integration"] {
            let binding = surface
                .bindings()
                .iter()
                .find(|binding| binding.name == name)
                .unwrap();
            assert_eq!(binding.active_when, predicate("run"), "{name}");
        }
        for name in [
            "source_model",
            "telescope",
            "array_config",
            "band",
            "target_ms_size_gib",
            "output_ms",
            "ms_channels",
            "image_channels",
            "pointing_count",
            "time_sample_count",
            "integration_seconds",
            "start_time_mjd_seconds",
            "imaging_mode",
            "observation_mode",
            "measure_actual_size",
        ] {
            let binding = surface
                .bindings()
                .iter()
                .find(|binding| binding.name == name)
                .unwrap();
            assert_eq!(binding.active_when, predicate("family"), "{name}");
        }
        for name in [
            "request_kind",
            "overwrite",
            "polarizations",
            "worker_policy",
            "row_workers",
            "channel_workers",
        ] {
            let binding = surface
                .bindings()
                .iter()
                .find(|binding| binding.name == name)
                .unwrap();
            assert_eq!(binding.active_when, Predicate::Always, "{name}");
        }
        assert!(["model", "out"].into_iter().all(|name| {
            surface
                .bindings()
                .iter()
                .find(|binding| binding.name == name)
                .is_some_and(|binding| matches!(binding.default, crate::DefaultSpec::Required))
        }));
    }

    #[test]
    fn imager_vlass_controls_share_one_catalog_owned_awproject_surface() {
        let catalog = builtin_surface_catalog().unwrap();
        let surface = catalog.surface("imager").unwrap();
        assert_eq!(surface.contract_version(), 5);

        let awproject = Predicate::Equals {
            parameter: "gridder".to_string(),
            value: ParameterValue::String("awproject".to_string()),
        };
        for name in [
            "cfcache",
            "cf_resident_mb",
            "facets",
            "psfphasecenter",
            "vptable",
            "aterm",
            "psterm",
            "wbawp",
            "conjbeams",
            "computepastep",
            "rotatepastep",
            "pointingoffsetsigdev",
            "mosweight",
            "normtype",
        ] {
            let binding = surface
                .bindings()
                .iter()
                .find(|binding| binding.name == name)
                .unwrap_or_else(|| panic!("missing imager AWProject binding {name}"));
            assert_eq!(binding.active_when, awproject, "{name}");
            assert_eq!(
                binding.projections.presentation.group, "Advanced Wide-Field",
                "{name}"
            );
            assert!(binding.projections.presentation.advanced, "{name}");
            assert!(binding.projections.cli.is_some(), "{name}");
            assert!(binding.projections.python.is_some(), "{name}");
        }

        let usepointing = surface
            .bindings()
            .iter()
            .find(|binding| binding.name == "usepointing")
            .expect("missing imager POINTING binding");
        assert_eq!(usepointing.active_when, Predicate::Always);
        assert_eq!(
            usepointing.projections.presentation.group,
            "Advanced Wide-Field"
        );
        assert!(usepointing.projections.presentation.advanced);

        for name in ["uvrange", "intent", "stokes"] {
            assert!(
                surface
                    .bindings()
                    .iter()
                    .any(|binding| binding.name == name),
                "missing canonical selection binding {name}"
            );
        }
        let stokes = surface
            .bindings()
            .iter()
            .find(|binding| binding.name == "stokes")
            .unwrap();
        assert_eq!(stokes.concept.id.as_str(), "image.selection.stokes");
        assert_eq!(stokes.aliases, ["polarization"]);

        for name in [
            "imaging_memory_target_mb",
            "imaging_prepare_buffer_mb",
            "imaging_row_block_rows",
            "imaging_prepare_workers",
            "imaging_fft_precision",
        ] {
            let binding = surface
                .bindings()
                .iter()
                .find(|binding| binding.name == name)
                .unwrap_or_else(|| panic!("missing imager resource binding {name}"));
            assert_eq!(
                binding.projections.presentation.group, "Execution Resources",
                "{name}"
            );
            assert!(binding.projections.presentation.advanced, "{name}");
        }
    }

    #[test]
    fn repeated_builtin_names_share_a_concept_or_carry_a_homonym_review() {
        let catalog = builtin_surface_catalog().unwrap();
        let mut grouped = BTreeMap::<
            String,
            BTreeMap<crate::ParameterConceptRef, Vec<(&str, &crate::SurfaceParameterBinding)>>,
        >::new();
        for surface in &catalog.surfaces {
            for binding in surface.bindings() {
                grouped
                    .entry(binding.name.clone())
                    .or_default()
                    .entry(binding.concept.clone())
                    .or_default()
                    .push((surface.id(), binding));
            }
        }
        for (name, concepts) in grouped {
            for (position, (concept, bindings)) in concepts.into_iter().enumerate() {
                let reviews = bindings
                    .iter()
                    .filter(|(_, binding)| binding.reviewed_homonym.is_some())
                    .count();
                assert_eq!(
                    reviews,
                    usize::from(position > 0),
                    "{name}.{} must carry exactly one concept-level review when it differs from the lexical baseline",
                    concept.id
                );
            }
        }
    }

    #[test]
    fn exact_semantic_duplicates_share_one_concept() {
        let catalog = builtin_surface_catalog().unwrap();
        let binding = |surface: &str, name: &str| {
            catalog
                .surface(surface)
                .unwrap()
                .bindings()
                .iter()
                .find(|binding| binding.name == name)
                .unwrap()
        };
        for (surfaces, name, expected, reviewed_surface) in [
            (
                &[
                    "applycal",
                    "bandpass",
                    "calibrate",
                    "fluxscale",
                    "gaincal",
                    "gencal",
                    "uvcontsub",
                ][..],
                "output",
                "calibration.report.output",
                None,
            ),
            (
                &["msexplore", "plotms"][..],
                "output",
                "measurementset.summary.output",
                Some("msexplore"),
            ),
            (
                &["mstransform", "split"][..],
                "width",
                "ms.transform.width",
                Some("mstransform"),
            ),
            (
                &["simalma", "simanalyze"][..],
                "image",
                "simulation.image",
                Some("simalma"),
            ),
        ] {
            for surface in surfaces {
                let binding = binding(surface, name);
                assert_eq!(binding.concept.id.as_str(), expected, "{surface}.{name}");
                assert_eq!(
                    binding.reviewed_homonym.is_some(),
                    reviewed_surface == Some(*surface),
                    "{surface}.{name} must carry homonym evidence only on the shared concept's representative"
                );
            }
        }
    }

    #[test]
    fn importvla_archivefiles_preserves_a_nonempty_path_list() {
        let catalog = builtin_surface_catalog().unwrap();
        let concept = catalog
            .catalog
            .concepts
            .iter()
            .find(|concept| concept.id.as_str() == "parameter.archivefiles")
            .unwrap();
        assert_eq!(concept.semantic_revision, SemanticRevision(2));
        assert_eq!(
            concept.value_domain,
            ParameterType::Array {
                element: Box::new(ParameterType::Path {
                    resource_kind: Some(ResourceKind::Archive),
                }),
                min_items: 1,
                max_items: None,
                allow_scalar: true,
            }
        );

        let surface = catalog.surface("importvla").unwrap();
        assert_eq!(surface.contract_version(), 3);
        let archivefiles = surface
            .bindings()
            .iter()
            .find(|binding| binding.name == "archivefiles")
            .unwrap();
        assert_eq!(
            archivefiles.projections.provider.as_ref().unwrap().adapter,
            ValueAdapter::StringListCsv
        );
        assert!(matches!(
            &surface.migrations()[0].steps[..],
            [MigrationStep::Transform {
                parameter,
                transform: crate::MigrationTransform::ScalarToArray { length: 1 },
            }] if parameter == "archivefiles"
        ));
    }

    #[test]
    fn ambiguous_role_names_remain_explicit_concepts() {
        let catalog = builtin_surface_catalog().unwrap();
        let concept_ids = |name: &str| {
            catalog
                .surfaces
                .iter()
                .flat_map(|surface| surface.bindings())
                .filter(|binding| binding.name == name)
                .map(|binding| binding.concept.id.as_str().to_string())
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(
            concept_ids("vis"),
            ["data.input.vis", "data.output.vis"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
        assert_eq!(
            concept_ids("imagename"),
            ["image.input.imagename", "image.output.imagename"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
        assert_eq!(
            concept_ids("width"),
            ["imager.width", "impv.width", "ms.transform.width"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
        assert_eq!(
            concept_ids("mode"),
            [
                "calibrate.mode",
                "flagdata.mode",
                "flagmanager.mode",
                "imhead.mode",
                "impbcor.mode",
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        );

        let role = |id: &str| {
            catalog
                .catalog
                .concepts
                .iter()
                .find(|concept| concept.id.as_str() == id)
                .unwrap()
                .semantic_role
        };
        assert_eq!(role("data.input.vis"), ParameterRole::InputData);
        assert_eq!(role("data.output.vis"), ParameterRole::OutputData);
        assert_eq!(role("image.input.imagename"), ParameterRole::InputData);
        assert_eq!(role("image.output.imagename"), ParameterRole::OutputData);
        for id in concept_ids("width").into_iter().chain(concept_ids("mode")) {
            assert_eq!(role(&id), ParameterRole::Algorithm);
        }
    }

    #[test]
    fn every_surface_explicitly_classifies_its_products() {
        let catalog = builtin_surface_catalog().unwrap();
        for surface in &catalog.surfaces {
            let output_parameters = surface
                .bindings()
                .iter()
                .filter(|binding| {
                    catalog
                        .catalog
                        .concept(&binding.concept)
                        .is_some_and(|concept| concept.semantic_role == ParameterRole::OutputData)
                })
                .map(|binding| binding.name.as_str())
                .collect::<BTreeSet<_>>();
            match &surface.execution().products {
                SurfaceProductContract::NoProducts => assert!(
                    output_parameters.is_empty(),
                    "{} has output bindings but declares no products: {output_parameters:?}",
                    surface.id()
                ),
                SurfaceProductContract::Declared { products } => {
                    assert!(
                        !products.is_empty(),
                        "{} has an empty product declaration",
                        surface.id()
                    );
                    let ids = products
                        .iter()
                        .map(|product| product.id.as_str())
                        .collect::<BTreeSet<_>>();
                    assert_eq!(
                        ids.len(),
                        products.len(),
                        "{} has duplicate product IDs",
                        surface.id()
                    );
                    let parameter_sources = products
                        .iter()
                        .filter_map(|product| match &product.source {
                            RunProductSource::Parameter { parameter } => Some(parameter.as_str()),
                            RunProductSource::DecodedArtifacts => None,
                        })
                        .collect::<BTreeSet<_>>();
                    if surface.id() == "imager" {
                        assert!(products.iter().any(|product| {
                            matches!(product.source, RunProductSource::DecodedArtifacts)
                        }));
                    } else {
                        assert_eq!(
                            parameter_sources,
                            output_parameters,
                            "{} product descriptors drifted from output bindings",
                            surface.id()
                        );
                    }
                }
            }
        }
    }
}
