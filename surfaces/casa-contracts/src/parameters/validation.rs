// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Stable structural validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ContractValidationError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter: Option<String>,
}

impl ContractValidationError {
    pub(super) fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            surface: None,
            parameter: None,
        }
    }

    pub(super) fn at(mut self, surface: &str, parameter: Option<&str>) -> Self {
        self.surface = Some(surface.to_string());
        self.parameter = parameter.map(str::to_string);
        self
    }
}

/// Validate one or more bundles together, including cross-surface invariants.
pub fn validate_surface_bundles(
    bundles: &[SurfaceContractBundle],
) -> Result<(), Vec<ContractValidationError>> {
    validate_surface_bundles_internal(bundles, true)
}

pub(super) fn validate_surface_bundles_internal(
    bundles: &[SurfaceContractBundle],
    validate_cross_surface_names: bool,
) -> Result<(), Vec<ContractValidationError>> {
    let views: Vec<_> = bundles.iter().map(SurfaceContractRef::from).collect();
    validate_surface_views(&views, validate_cross_surface_names)
}

pub(super) fn validate_one_surface(
    bundle: &SurfaceContractRef<'_>,
    errors: &mut Vec<ContractValidationError>,
) {
    let surface = &bundle.surface;
    let mut names = BTreeSet::new();
    let mut orders = BTreeSet::new();
    let known = surface
        .bindings()
        .iter()
        .map(|binding| binding.name.clone())
        .collect::<BTreeSet<_>>();

    if surface.id().is_empty() || surface.contract_version() == 0 {
        errors.push(
            ContractValidationError::new(
                "invalid_surface",
                "surface id must be non-empty and contract version must be positive",
            )
            .at(surface.id(), None),
        );
    }

    validate_overwrite_defaults(bundle, errors);
    casa_signature::validate_casa_signature(surface, errors);
    casa_signature::validate_unimplemented(bundle, &known, errors);

    for binding in surface.bindings() {
        if !names.insert(binding.name.clone()) {
            errors.push(
                ContractValidationError::new(
                    "duplicate_parameter",
                    format!("duplicate parameter {}", binding.name),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        if !orders.insert(binding.order) {
            errors.push(
                ContractValidationError::new(
                    "duplicate_order",
                    format!("duplicate presentation order {}", binding.order),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        validate_profile_parameter_name(binding, surface.id(), errors);
        let Some(concept) = bundle.catalog.concept(&binding.concept) else {
            errors.push(
                ContractValidationError::new(
                    "missing_concept",
                    format!(
                        "missing concept {} revision {}",
                        binding.concept.id, binding.concept.semantic_revision.0
                    ),
                )
                .at(surface.id(), Some(&binding.name)),
            );
            continue;
        };
        if let Err(reason) = validate_parameter_domain(&concept.value_domain) {
            errors.push(
                ContractValidationError::new(
                    "invalid_value_domain",
                    format!(
                        "concept {} has an invalid value domain: {reason}",
                        concept.id
                    ),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        if binding.name != concept.casa_name {
            errors.push(
                ContractValidationError::new(
                    "noncanonical_name",
                    format!(
                        "binding name {:?} differs from concept CASA name {:?}",
                        binding.name, concept.casa_name
                    ),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        if concept.persistence_class != PersistenceClass::Profile {
            errors.push(
                ContractValidationError::new(
                    "nonprofile_concept",
                    format!(
                        "concept {} is {:?} and cannot be bound in a profile",
                        concept.id, concept.persistence_class
                    ),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        match (binding.context_role, concept.semantic_role) {
            (Some(ContextRole::InputDataset), ParameterRole::InputData)
            | (Some(ContextRole::OutputProduct), ParameterRole::OutputData)
            | (None, _) => {}
            (Some(ContextRole::InputDataset), role) => errors.push(
                ContractValidationError::new(
                    "context_role_mismatch",
                    format!("input_dataset requires an input_data concept, got {role:?}"),
                )
                .at(surface.id(), Some(&binding.name)),
            ),
            (Some(ContextRole::OutputProduct), role) => errors.push(
                ContractValidationError::new(
                    "context_role_mismatch",
                    format!("output_product requires an output_data concept, got {role:?}"),
                )
                .at(surface.id(), Some(&binding.name)),
            ),
            (Some(_), _) => {}
        }
        validate_reserved_name(binding, surface.id(), errors);
        validate_predicate_refs(
            &binding.required_when,
            &known,
            surface.id(),
            &binding.name,
            errors,
        );
        validate_predicate_refs(
            &binding.active_when,
            &known,
            surface.id(),
            &binding.name,
            errors,
        );
        if let Some(predicate) = binding
            .projections
            .provider
            .as_ref()
            .and_then(|projection| projection.emit_when.as_ref())
        {
            validate_predicate_refs(predicate, &known, surface.id(), &binding.name, errors);
        }
        if !matches!(binding.required_when, Predicate::Always)
            && matches!(binding.default, DefaultSpec::Required)
        {
            errors.push(
                ContractValidationError::new(
                    "missing_optional_default",
                    "every conditionally optional parameter needs a typed default",
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
        for value in default_values(&binding.default) {
            if let Err(reason) = validate_parameter_value(value, &concept.value_domain) {
                errors.push(
                    ContractValidationError::new(
                        "invalid_default",
                        format!("default does not match concept domain: {reason}"),
                    )
                    .at(surface.id(), Some(&binding.name)),
                );
            }
            validate_constraints(
                value,
                &concept.base_constraints,
                surface.id(),
                binding,
                errors,
            );
            validate_refinement_value(
                value,
                &concept.normalization,
                &binding.refinements,
                surface.id(),
                binding,
                errors,
            );
        }
        validate_refinements(concept, binding, surface.id(), errors);
    }
    validate_default_cycles(surface, errors);
}

/// Runtime authority and UI/process state must never enter a portable profile.
/// Exact names cover existing controls; prefixes make the gate fail closed for
/// newly introduced variants in the same authority class.
pub(super) const FORBIDDEN_PROFILE_PARAMETER_NAMES: &[&str] = &[
    "binary",
    "confirm",
    "executable",
    "invocation_name",
    "no_save_last",
    "override_env",
    "provider_binary",
    "provider_executable",
    "runtime",
    "telemetry",
    "ui_state",
    "workspace",
];

pub(super) const FORBIDDEN_PROFILE_PARAMETER_PREFIXES: &[&str] = &[
    "binary_",
    "confirm_",
    "confirmation_",
    "executable_",
    "no_save_",
    "override_env_",
    "runtime_",
    "telemetry_",
    "ui_",
    "workspace_",
];

pub(super) fn validate_profile_parameter_name(
    binding: &SurfaceParameterBinding,
    surface: &str,
    errors: &mut Vec<ContractValidationError>,
) {
    let exact = FORBIDDEN_PROFILE_PARAMETER_NAMES.contains(&binding.name.as_str());
    let prefix = FORBIDDEN_PROFILE_PARAMETER_PREFIXES
        .iter()
        .find(|prefix| binding.name.starts_with(**prefix));
    if exact || prefix.is_some() {
        let policy = prefix.map_or("exact runtime-authority name", |prefix| *prefix);
        errors.push(
            ContractValidationError::new(
                "forbidden_profile_parameter_name",
                format!(
                    "parameter {:?} is reserved by the non-profile authority policy ({policy})",
                    binding.name
                ),
            )
            .at(surface, Some(&binding.name)),
        );
    }
}

/// An overwrite binding defaults to `false` except for the tasks listed in
/// [`CASA_OVERWRITE_BY_DEFAULT_TASKS`], whose CASA behavior replaces products.
pub(super) fn validate_overwrite_defaults(
    bundle: &SurfaceContractRef<'_>,
    errors: &mut Vec<ContractValidationError>,
) {
    let surface = &bundle.surface;
    let expected = CASA_OVERWRITE_BY_DEFAULT_TASKS.contains(&surface.id());
    for binding in surface
        .bindings()
        .iter()
        .filter(|binding| binding.concept.id.as_str() == OVERWRITE_CONCEPT_ID)
    {
        let matches = matches!(
            &binding.default,
            DefaultSpec::Literal { value: ParameterValue::Bool(value) } if *value == expected
        );
        if !matches {
            errors.push(
                ContractValidationError::new(
                    "overwrite_default",
                    format!("{} must default to {expected}", binding.name),
                )
                .at(surface.id(), Some(&binding.name)),
            );
        }
    }
}

pub(super) fn validate_reserved_name(
    binding: &SurfaceParameterBinding,
    surface: &str,
    errors: &mut Vec<ContractValidationError>,
) {
    // Simulation analysis uses zero/empty geometry to inherit the sky model.
    // Its CASA defaults differ from tclean's positive explicit geometry.
    let designated = match (surface, binding.name.as_str()) {
        ("tsdimaging", "imsize") => Some("single_dish.imsize"),
        ("tsdimaging", "cell") => Some("single_dish.cell"),
        ("simanalyze" | "simalma", "imsize") => Some("simulation.analysis.imsize"),
        ("simalma", "cell") => Some("simulation.alma.cell"),
        ("simanalyze", "cell") => Some("simulation.analysis.cell"),
        (_, "imsize") => Some(IMSIZE_CONCEPT_ID),
        (_, "cell") => Some(CELL_CONCEPT_ID),
        (_, "field") => Some(FIELD_CONCEPT_ID),
        (_, "spw") => Some(SPW_CONCEPT_ID),
        _ => None,
    };
    if let Some(designated) = designated
        && binding.concept.id.as_str() != designated
    {
        errors.push(
            ContractValidationError::new(
                "reserved_name",
                format!(
                    "reserved parameter {} must bind concept {}",
                    binding.name, designated
                ),
            )
            .at(surface, Some(&binding.name)),
        );
    }
}

pub(super) fn validate_predicate_refs(
    predicate: &Predicate,
    known: &BTreeSet<String>,
    surface: &str,
    parameter: &str,
    errors: &mut Vec<ContractValidationError>,
) {
    let mut refs = BTreeSet::new();
    predicate.referenced_parameters(&mut refs);
    for reference in refs.difference(known) {
        errors.push(
            ContractValidationError::new(
                "unknown_predicate_parameter",
                format!("predicate references unknown parameter {reference}"),
            )
            .at(surface, Some(parameter)),
        );
    }
}

pub(super) fn default_values(default: &DefaultSpec) -> Vec<&ParameterValue> {
    match default {
        DefaultSpec::Required => Vec::new(),
        DefaultSpec::Literal { value } => vec![value],
        DefaultSpec::Conditional { cases, fallback } => cases
            .iter()
            .map(|case| &case.value)
            .chain(std::iter::once(fallback))
            .collect(),
    }
}

pub(super) fn domain_is_numeric(domain: &ParameterType) -> bool {
    match domain {
        ParameterType::Integer | ParameterType::Float | ParameterType::Quantity { .. } => true,
        ParameterType::Array { element, .. } => domain_is_numeric(element),
        ParameterType::Optional { value, .. } => domain_is_numeric(value),
        _ => false,
    }
}

pub(super) fn numeric_values(value: &ParameterValue) -> Vec<f64> {
    match value {
        ParameterValue::Integer(value) => vec![*value as f64],
        ParameterValue::Float(value) => vec![*value],
        ParameterValue::String(value) => quantity_number(value).into_iter().collect(),
        ParameterValue::Array(values) => values.iter().flat_map(numeric_values).collect(),
        ParameterValue::Table(values) => values.values().flat_map(numeric_values).collect(),
        ParameterValue::ScalarArray(value) => numeric_values(value),
        ParameterValue::NumpyScalar { value, .. } => numeric_values(value),
        ParameterValue::Bool(_)
        | ParameterValue::Complex(_)
        | ParameterValue::EmptyArray { .. } => Vec::new(),
    }
}

pub(super) fn quantity_number(value: &str) -> Option<f64> {
    let value = value.trim();
    if let Ok(number) = value.parse::<f64>() {
        return Some(number);
    }
    let split = value
        .find(|character: char| {
            !character.is_ascii_digit()
                && character != '.'
                && character != '-'
                && character != '+'
                && character != 'e'
                && character != 'E'
        })
        .unwrap_or(value.len());
    (split > 0)
        .then(|| value[..split].parse::<f64>().ok())
        .flatten()
}

pub(super) fn value_len(value: &ParameterValue) -> Option<usize> {
    match value {
        ParameterValue::String(value) => Some(value.len()),
        ParameterValue::Array(value) => Some(value.len()),
        ParameterValue::Table(value) => Some(value.len()),
        _ => None,
    }
}

pub(super) fn validate_default_cycles(
    surface: &SurfaceDefinition,
    errors: &mut Vec<ContractValidationError>,
) {
    let mut graph = BTreeMap::<String, BTreeSet<String>>::new();
    for binding in surface.bindings() {
        let mut dependencies = BTreeSet::new();
        if let DefaultSpec::Conditional { cases, .. } = &binding.default {
            for case in cases {
                case.when.referenced_parameters(&mut dependencies);
            }
        }
        graph.insert(binding.name.clone(), dependencies);
    }
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for name in graph.keys() {
        if detects_cycle(name, &graph, &mut visiting, &mut visited) {
            errors.push(
                ContractValidationError::new(
                    "default_cycle",
                    format!("conditional default graph contains a cycle at {name}"),
                )
                .at(surface.id(), Some(name)),
            );
            break;
        }
    }
}

pub(super) fn detects_cycle(
    name: &str,
    graph: &BTreeMap<String, BTreeSet<String>>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> bool {
    if visited.contains(name) {
        return false;
    }
    if !visiting.insert(name.to_string()) {
        return true;
    }
    if let Some(dependencies) = graph.get(name) {
        for dependency in dependencies {
            if graph.contains_key(dependency) && detects_cycle(dependency, graph, visiting, visited)
            {
                return true;
            }
        }
    }
    visiting.remove(name);
    visited.insert(name.to_string());
    false
}

pub(super) fn validate_repeated_names(
    bundles: &[SurfaceContractRef<'_>],
    errors: &mut Vec<ContractValidationError>,
) {
    let mut grouped = BTreeMap::<
        String,
        BTreeMap<ParameterConceptRef, Vec<(&str, &SurfaceParameterBinding)>>,
    >::new();
    for bundle in bundles {
        for binding in bundle.surface.bindings() {
            grouped
                .entry(binding.name.clone())
                .or_default()
                .entry(binding.concept.clone())
                .or_default()
                .push((bundle.surface.id(), binding));
        }
    }

    for (name, concepts) in grouped {
        let baseline = concepts.keys().next().cloned();
        for (concept, bindings) in concepts {
            let reviewed = bindings
                .iter()
                .filter(|(_, binding)| binding.reviewed_homonym.is_some())
                .collect::<Vec<_>>();
            let needs_review = baseline
                .as_ref()
                .is_some_and(|baseline| baseline != &concept);
            if needs_review && reviewed.is_empty() {
                let (surface, binding) = bindings[0];
                errors.push(
                    ContractValidationError::new(
                        "unreviewed_homonym",
                        format!(
                            "canonical name {name} uses concept {} in addition to baseline {} without one concept-level review",
                            concept.id,
                            baseline.as_ref().expect("multi-concept name has baseline").id
                        ),
                    )
                    .at(surface, Some(&binding.name)),
                );
            }
            let allowed_reviews = usize::from(needs_review);
            for (surface, binding) in reviewed.into_iter().skip(allowed_reviews) {
                errors.push(
                    ContractValidationError::new(
                        "redundant_homonym_review",
                        format!(
                            "canonical name {name} concept {} already has sufficient concept-level homonym evidence",
                            concept.id
                        ),
                    )
                    .at(surface, Some(&binding.name)),
                );
            }
        }
    }
}
