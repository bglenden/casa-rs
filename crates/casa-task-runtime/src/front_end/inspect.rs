// SPDX-License-Identifier: LGPL-3.0-or-later
//! The non-executing views of a task command line: the resolved state with
//! origins, a parameter or surface description, and profile validation.

use casa_provider_contracts::{
    ParameterConcept, ParameterValue, SurfaceCatalogBundle, SurfaceContractBundle,
    builtin_surface_catalog,
};

use crate::{ParameterSession, ParameterSessionError, ProfileError, parse_profile, resolve_profile};

/// Every resolved value with its origin, in the surface's order, and the
/// session's diagnostics.
pub fn render_session(session: &ParameterSession) -> String {
    let mut bindings = session
        .bundle()
        .surface
        .bindings()
        .iter()
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let mut text = format!(
        "{} {} (contract {}, source {:?})\n",
        session.bundle().surface.kind(),
        session.bundle().surface.id(),
        session.bundle().surface.contract_version(),
        session.base_source()
    );
    for binding in bindings {
        let state = &session.states()[&binding.name];
        let value = state
            .value
            .as_ref()
            .map(display_value)
            .unwrap_or_else(|| "<required>".to_string());
        text.push_str(&format!(
            "{:<24} = {:<28} # {:?}{}{}\n",
            binding.name,
            value,
            state.origin,
            if state.required { ", required" } else { "" },
            if state.active { "" } else { ", inactive" }
        ));
    }
    for diagnostic in session.diagnostics() {
        text.push_str(&format!("warning: {}\n", diagnostic.message));
    }
    text
}

/// A value as a reader types it on a command line or in a profile.
pub fn display_value(value: &ParameterValue) -> String {
    match value {
        ParameterValue::Bool(value) => value.to_string(),
        ParameterValue::Integer(value) => value.to_string(),
        ParameterValue::Float(value) => value.to_string(),
        ParameterValue::String(value) => format!("{value:?}"),
        ParameterValue::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(display_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ParameterValue::Table(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(name, value)| format!("{name}={}", display_value(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Describe a surface, or a parameter concept by id or CASA name, from the
/// built-in catalog.
pub fn describe(name: &str) -> Result<String, String> {
    let aggregate = builtin_surface_catalog()?;
    if let Some(surface) = aggregate.surface(name) {
        let mut text = format!(
            "{} {} — {}\nprovider family: {}\ncontract: {}\nparameters: {}\n\n",
            surface.kind(),
            surface.id(),
            surface.summary(),
            surface.provider_family(),
            surface.contract_version(),
            surface.bindings().len()
        );
        for binding in surface.bindings() {
            text.push_str(&format!(
                "  {:<24} {}@r{}\n",
                binding.name, binding.concept.id, binding.concept.semantic_revision.0
            ));
        }
        return Ok(text);
    }
    let concepts = aggregate
        .catalog
        .concepts
        .iter()
        .filter(|concept| concept.id.as_str() == name || concept.casa_name == name)
        .collect::<Vec<_>>();
    if concepts.is_empty() {
        return Err(format!("unknown parameter concept or surface {name:?}"));
    }
    let mut text = String::new();
    for concept in concepts {
        describe_concept(&mut text, concept, aggregate);
    }
    Ok(text)
}

fn describe_concept(text: &mut String, concept: &ParameterConcept, aggregate: &SurfaceCatalogBundle) {
    text.push_str(&format!(
        "{}@r{} ({})\nCASA name: {}\ntype: {:?}\nnormalization: {:?}\nrole: {:?}\npersistence: {:?}\n{}\nused by:",
        concept.id,
        concept.semantic_revision.0,
        concept.documentation.summary,
        concept.casa_name,
        concept.value_domain,
        concept.normalization,
        concept.semantic_role,
        concept.persistence_class,
        concept.documentation.details.as_deref().unwrap_or("")
    ));
    for surface in &aggregate.surfaces {
        if surface
            .bindings()
            .iter()
            .any(|binding| binding.concept == concept.reference())
        {
            text.push_str(&format!(" {}", surface.id()));
        }
    }
    text.push_str("\n\n");
}

/// Validate a profile's text, against `bundle` when given, else against the
/// surface the profile names; return the summary line and its warnings.
pub fn validate_profile_text(
    source: &str,
    bundle: Option<&SurfaceContractBundle>,
) -> Result<String, String> {
    let profile = parse_profile(source).map_err(format_profile_error)?;
    let named;
    let bundle = match bundle {
        Some(bundle) if bundle.surface.id() == profile.header.surface => bundle,
        Some(bundle) => {
            return Err(format!(
                "the profile is for {:?}, not {:?}",
                profile.header.surface,
                bundle.surface.id()
            ));
        }
        None => {
            named = casa_provider_contracts::builtin_surface_bundle(&profile.header.surface)?;
            &named
        }
    };
    let resolved = resolve_profile(&profile, bundle).map_err(format_profile_error)?;
    let count = resolved.explicit_overrides.len();
    let mut text = format!(
        "valid {} {} profile (contract {}, {count} explicit override{})\n",
        profile.header.kind,
        profile.header.surface,
        profile.header.contract,
        if count == 1 { "" } else { "s" }
    );
    for diagnostic in resolved.diagnostics {
        text.push_str(&format!("warning: {}\n", diagnostic.message));
    }
    Ok(text)
}

/// A profile error as its located, suggestion-bearing lines.
pub fn format_profile_error(error: ProfileError) -> String {
    match error {
        ProfileError::Diagnostics(diagnostics) => diagnostics
            .into_iter()
            .map(|diagnostic| {
                let location = diagnostic.location.map_or_else(String::new, |location| {
                    format!("{}:{}: ", location.line, location.column)
                });
                let suggestions = if diagnostic.suggestions.is_empty() {
                    String::new()
                } else {
                    format!("; did you mean {}?", diagnostic.suggestions.join(" or "))
                };
                format!("{location}{}{suggestions}", diagnostic.message)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => other.to_string(),
    }
}

/// A session error, with a profile error's located lines.
pub fn format_session_error(error: ParameterSessionError) -> String {
    match error {
        ParameterSessionError::Profile(error) => format_profile_error(error),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::builtin_surface_bundle;

    use super::*;

    #[test]
    fn describe_uses_the_shared_concept_catalog() {
        let text = describe("width").unwrap();
        assert!(text.contains("ms.transform.width@r1"));
        assert!(text.contains("mstransform"));
        assert!(text.contains("split"));
        assert!(describe("not-a-parameter").is_err());
    }

    #[test]
    fn a_profile_for_another_surface_is_refused() {
        let profile = "[casars]\nformat = 1\nsurface = \"imstat\"\nkind = \"task\"\ncontract = 3\n\n[parameters]\nimagename = [\"x.image\"]\n";
        let imstat = builtin_surface_bundle("imstat").unwrap();
        let text = validate_profile_text(profile, Some(&imstat)).unwrap();
        assert!(text.starts_with("valid task imstat profile"), "{text}");
        let imhead = builtin_surface_bundle("imhead").unwrap();
        let error = validate_profile_text(profile, Some(&imhead)).unwrap_err();
        assert!(error.contains("not \"imhead\""), "{error}");
    }
}
