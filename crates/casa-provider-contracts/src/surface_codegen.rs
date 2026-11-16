// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generate surface bundles without changing the catalog's JSON values.

use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::value::RawValue;

#[cfg(not(test))]
use crate::ContractValidationError;
use crate::{SurfaceCatalogBundle, SurfaceContractBundle};

#[derive(Deserialize)]
struct RawCatalog {
    schema_version: u32,
    concepts: Vec<Box<RawValue>>,
}

#[derive(Deserialize)]
struct RawSurfaces {
    schema_version: u32,
    surfaces: Vec<Box<RawValue>>,
}

pub(crate) struct SurfaceSources {
    catalog: RawCatalog,
    surfaces: RawSurfaces,
}

impl SurfaceSources {
    pub(crate) fn parse(catalog_json: &str, surfaces_json: &str) -> Result<Self, String> {
        Ok(Self {
            catalog: serde_json::from_str(catalog_json).map_err(|error| error.to_string())?,
            surfaces: serde_json::from_str(surfaces_json).map_err(|error| error.to_string())?,
        })
    }

    /// Extract one bundle while retaining original numeric and string tokens.
    pub(crate) fn encode_surface(
        &self,
        aggregate: &SurfaceCatalogBundle,
        index: usize,
    ) -> Result<String, String> {
        if self.catalog.schema_version != aggregate.catalog.schema_version
            || self.surfaces.schema_version != aggregate.schema_version
            || self.catalog.concepts.len() != aggregate.catalog.concepts.len()
            || self.surfaces.surfaces.len() != aggregate.surfaces.len()
        {
            return Err("raw and validated catalog structures disagree".into());
        }
        let surface = aggregate.surfaces.get(index).ok_or("missing surface")?;
        let raw_surface = &self.surfaces.surfaces[index];
        let references: BTreeSet<_> = surface
            .bindings()
            .iter()
            .map(|binding| binding.concept.clone())
            .collect();
        let concepts = aggregate
            .catalog
            .concepts
            .iter()
            .zip(&self.catalog.concepts)
            .filter(|(concept, _)| references.contains(&concept.reference()))
            .map(|(_, raw)| raw.get())
            .collect::<Vec<_>>()
            .join(",");
        let raw = format!(
            "{{\"schema_version\":{},\"surface\":{},\"catalog\":{{\"schema_version\":{},\"concepts\":[{}]}}}}",
            aggregate.schema_version,
            raw_surface.get(),
            aggregate.catalog.schema_version,
            concepts,
        );
        let json = compact_json(&raw);
        let restored: SurfaceContractBundle =
            serde_json::from_str(&json).map_err(|error| error.to_string())?;
        let expected = aggregate
            .embedded_surface(surface.id())
            .ok_or("missing surface")?;
        let restored = serde_json::to_value(restored).map_err(|error| error.to_string())?;
        let expected = serde_json::to_value(expected).map_err(|error| error.to_string())?;
        if restored != expected {
            return Err("generated surface differs from the validated catalog".into());
        }
        Ok(json)
    }
}

/// Remove JSON whitespace outside strings without parsing or changing tokens.
fn compact_json(input: &str) -> String {
    let mut bytes = Vec::with_capacity(input.len());
    let (mut quoted, mut escaped) = (false, false);
    for &byte in input.as_bytes() {
        if quoted {
            bytes.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
            bytes.push(byte);
        } else if !matches!(byte, b' ' | b'\t' | b'\r' | b'\n') {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).expect("removing ASCII whitespace preserves UTF-8")
}

/// Whether an error is independent of floating-point parser configuration.
#[cfg(not(test))]
pub(crate) fn definitely_invalid(errors: &[ContractValidationError]) -> bool {
    errors.iter().any(|error| {
        matches!(
            error.code.as_str(),
            "schema_version"
                | "duplicate_surface"
                | "invalid_surface"
                | "duplicate_parameter"
                | "duplicate_order"
                | "missing_concept"
                | "noncanonical_name"
                | "nonprofile_concept"
                | "alias_canonical_collision"
                | "duplicate_alias"
                | "forbidden_profile_parameter_name"
                | "reserved_name"
                | "unknown_predicate_parameter"
                | "default_cycle"
                | "unreviewed_homonym"
                | "redundant_homonym_review"
                | "unknown_selector_capability"
                | "current_only_alias"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::SurfaceSources;
    use crate::build_catalog;
    use crate::catalog_number_probe::number_tokens;

    const CATALOG: &str = include_str!("../resources/parameter-catalog.json");
    const SURFACES: &str = include_str!("../resources/parameter-surfaces.json");

    fn generated_with_default(value: serde_json::Value) -> String {
        let mut raw: serde_json::Value = serde_json::from_str(SURFACES).unwrap();
        let surface = raw["surfaces"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|surface| surface["id"] == "imhead")
            .unwrap();
        let binding = surface["bindings"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|binding| binding["name"] == "hdvalue")
            .unwrap();
        binding["default"]["value"] = value;
        let surfaces_json = raw
            .to_string()
            .replace("\"__RAW_FLOAT__\"", "1.0000000000000002");
        let aggregate = build_catalog::parse_and_validate(CATALOG, &surfaces_json).unwrap();
        let sources = SurfaceSources::parse(CATALOG, &surfaces_json).unwrap();
        let index = aggregate
            .surfaces
            .iter()
            .position(|surface| surface.id() == "imhead")
            .unwrap();
        sources.encode_surface(&aggregate, index).unwrap()
    }

    #[test]
    fn generated_surface_preserves_nonfinite_defaults_and_float_tokens() {
        let json = generated_with_default(serde_json::json!({"kind":"float","value":"nan"}));
        assert!(json.contains("\"nan\""));
        let json =
            generated_with_default(serde_json::json!({"kind":"float","value":"__RAW_FLOAT__"}));
        assert!(json.contains("1.0000000000000002"));
    }

    #[test]
    fn numeric_tokens_ignore_escaped_strings_and_preserve_numbers() {
        let input = r#"{"quote":"a\\\" 99 \\\\ 88", "values":[-0,1.0000000000000002,3e-4]}"#;
        let tokens = number_tokens(&[input]);
        assert_eq!(
            tokens.into_iter().collect::<Vec<_>>(),
            ["-0", "1.0000000000000002", "3e-4"]
        );
    }

    #[test]
    fn a_valid_aggregate_can_exceed_the_generated_wrapper_recursion_limit() {
        let mut catalog: serde_json::Value = serde_json::from_str(CATALOG).unwrap();
        let concept = catalog["concepts"]
            .as_array()
            .unwrap()
            .iter()
            .position(|concept| concept["id"] == "parameter.hdvalue")
            .unwrap();
        let mut domain = serde_json::json!({"kind": "variant"});
        for depth in 1..130 {
            domain = serde_json::json!({"kind": "optional", "states": [], "value": domain});
            if depth < 100 {
                continue;
            }
            catalog["concepts"][concept]["value_domain"] = domain.clone();
            let json = catalog.to_string();
            let Ok(aggregate) = build_catalog::parse_and_validate(&json, SURFACES) else {
                continue;
            };
            let sources = SurfaceSources::parse(&json, SURFACES).unwrap();
            let index = aggregate
                .surfaces
                .iter()
                .position(|surface| surface.id() == "imhead")
                .unwrap();
            if let Err(error) = sources.encode_surface(&aggregate, index) {
                assert!(error.contains("recursion limit exceeded"), "{error}");
                return;
            }
        }
        panic!("expected an aggregate that fits but whose generated wrapper does not");
    }
}
