// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_provider_contracts::{
    DefaultSpec, MigrationStep, MigrationTransform, ParameterValue, Predicate,
    SurfaceContractBundle, SurfaceKind,
};
use thiserror::Error;

use crate::diagnostic::{Diagnostic, DiagnosticCode, DiagnosticLevel, SourceLocation};
use crate::normalize::{
    NormalizationError, normalize_quantity_unit, normalize_value, semantic_eq, validate_value,
};

/// Current human-profile document format.
pub const PROFILE_FORMAT_VERSION: u32 = 1;

/// Version and target metadata from `[casars]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileHeader {
    pub format: u32,
    pub surface: String,
    pub kind: SurfaceKind,
    pub contract: u32,
}

/// Parsed sparse human profile before contract resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct ParameterProfile {
    pub header: ProfileHeader,
    pub parameters: BTreeMap<String, ParameterValue>,
    pub header_locations: BTreeMap<String, SourceLocation>,
    pub locations: BTreeMap<String, SourceLocation>,
}

/// Current canonical values and retained explicit sparse intent.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedProfile {
    pub values: BTreeMap<String, ParameterValue>,
    pub explicit_overrides: BTreeMap<String, ParameterValue>,
    pub diagnostics: Vec<Diagnostic>,
}

/// Parse a strict sparse TOML profile.
pub fn parse_profile(source: &str) -> Result<ParameterProfile, ProfileError> {
    let document = source
        .parse::<toml::Value>()
        .map_err(|error| ProfileError::Parse {
            message: error.to_string(),
            location: error.span().map(|span| offset_location(source, span.start)),
        })?;
    let root = document.as_table().ok_or_else(|| ProfileError::Parse {
        message: "profile root must be a TOML table".to_string(),
        location: Some(SourceLocation { line: 1, column: 1 }),
    })?;
    reject_unknown_keys(root.keys(), ["casars", "parameters"], "profile root")?;

    let header = root
        .get("casars")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| ProfileError::Parse {
            message: "profile requires a [casars] table".to_string(),
            location: None,
        })?;
    reject_unknown_keys(
        header.keys(),
        ["format", "surface", "kind", "contract"],
        "[casars]",
    )?;
    let format = required_u32(header, "format")?;
    let surface = required_string(header, "surface")?;
    let kind = match required_string(header, "kind")?.as_str() {
        "task" => SurfaceKind::Task,
        "session" => SurfaceKind::Session,
        other => {
            return Err(ProfileError::Parse {
                message: format!("invalid profile kind {other:?}; expected task or session"),
                location: locate_key(source, "kind", false),
            });
        }
    };
    let contract = required_u32(header, "contract")?;

    let parameter_table = root
        .get("parameters")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| ProfileError::Parse {
            message: "profile requires a [parameters] table".to_string(),
            location: None,
        })?;
    let mut parameters = BTreeMap::new();
    let mut locations = BTreeMap::new();
    for (name, value) in parameter_table {
        parameters.insert(name.clone(), from_toml(value)?);
        if let Some(location) = locate_key(source, name, true) {
            locations.insert(name.clone(), location);
        }
    }

    Ok(ParameterProfile {
        header: ProfileHeader {
            format,
            surface,
            kind,
            contract,
        },
        parameters,
        header_locations: ["format", "surface", "kind", "contract"]
            .into_iter()
            .filter_map(|key| locate_key(source, key, false).map(|location| (key.into(), location)))
            .collect(),
        locations,
    })
}

/// Resolve one sparse profile against current definitions and defaults.
pub fn resolve_profile(
    profile: &ParameterProfile,
    bundle: &SurfaceContractBundle,
) -> Result<ResolvedProfile, ProfileError> {
    if profile.header.format > PROFILE_FORMAT_VERSION {
        return Err(ProfileError::Diagnostics(vec![Diagnostic::error(
            DiagnosticCode::FutureFormat,
            format!(
                "profile format {} is newer than supported format {}",
                profile.header.format, PROFILE_FORMAT_VERSION
            ),
        )]));
    }
    if profile.header.format != PROFILE_FORMAT_VERSION {
        return Err(ProfileError::Diagnostics(vec![Diagnostic::error(
            DiagnosticCode::FutureFormat,
            format!("unsupported profile format {}", profile.header.format),
        )]));
    }
    if profile.header.surface != bundle.surface.id() {
        let mut diagnostic = Diagnostic::error(
            DiagnosticCode::WrongSurface,
            format!(
                "profile targets surface {:?}, not {:?}",
                profile.header.surface,
                bundle.surface.id()
            ),
        );
        diagnostic.location = profile.header_locations.get("surface").copied();
        diagnostic.suggestions = vec![bundle.surface.id().to_string()];
        return Err(ProfileError::Diagnostics(vec![diagnostic]));
    }
    if profile.header.kind != bundle.surface.kind() {
        return Err(ProfileError::Diagnostics(vec![Diagnostic::error(
            DiagnosticCode::WrongKind,
            format!(
                "profile kind {} does not match surface kind {}",
                profile.header.kind,
                bundle.surface.kind()
            ),
        )]));
    }
    if profile.header.contract > bundle.surface.contract_version() {
        return Err(ProfileError::Diagnostics(vec![Diagnostic::error(
            DiagnosticCode::FutureContract,
            format!(
                "profile contract {} is newer than current contract {}",
                profile.header.contract,
                bundle.surface.contract_version()
            ),
        )]));
    }

    let (parameters, mut diagnostics) = migrate_parameters(profile, bundle)?;
    let known = bundle
        .surface
        .bindings()
        .iter()
        .flat_map(|binding| {
            std::iter::once(binding.name.as_str()).chain(binding.aliases.iter().map(String::as_str))
        })
        .collect::<BTreeSet<_>>();
    let mut canonical = BTreeMap::new();
    let mut failures = Vec::new();

    for (source_name, value) in parameters {
        let Some(binding) = bundle.surface.bindings().iter().find(|binding| {
            binding.name == source_name || binding.aliases.iter().any(|alias| alias == &source_name)
        }) else {
            let mut diagnostic = Diagnostic::error(
                DiagnosticCode::UnknownParameter,
                format!("unknown parameter {source_name:?}"),
            )
            .for_parameter(source_name.clone());
            diagnostic.location = profile.locations.get(&source_name).copied();
            diagnostic.suggestions = spelling_suggestions(&source_name, &known);
            failures.push(diagnostic);
            continue;
        };
        if canonical.contains_key(&binding.name) {
            let mut diagnostic = Diagnostic::error(
                DiagnosticCode::DuplicateParameter,
                format!(
                    "parameter {:?} is specified more than once through canonical/alias spellings",
                    binding.name
                ),
            )
            .for_parameter(binding.name.clone());
            diagnostic.location = profile.locations.get(&source_name).copied();
            failures.push(diagnostic);
            continue;
        }
        let concept = bundle.catalog.concept(&binding.concept).ok_or_else(|| {
            ProfileError::Contract(format!(
                "missing concept {} revision {}",
                binding.concept.id, binding.concept.semantic_revision.0
            ))
        })?;
        match normalize_value(&value, concept).and_then(|value| {
            validate_value(&value, concept, &binding.refinements)?;
            Ok(value)
        }) {
            Ok(value) => {
                canonical.insert(binding.name.clone(), value);
            }
            Err(error) => {
                let mut diagnostic = Diagnostic::error(
                    DiagnosticCode::InvalidValue,
                    format!("invalid value for {}: {error}", binding.name),
                )
                .for_parameter(binding.name.clone());
                diagnostic.location = profile.locations.get(&source_name).copied();
                failures.push(diagnostic);
            }
        }
    }
    if !failures.is_empty() {
        return Err(ProfileError::Diagnostics(failures));
    }

    let values = resolve_values(bundle, &canonical)?;
    if let Err(ProfileError::Diagnostics(mut failures)) =
        validate_activity_and_required(bundle, &values, &canonical)
    {
        for diagnostic in &mut failures {
            if let Some(parameter) = diagnostic.parameter.as_deref() {
                diagnostic.location = profile.locations.get(parameter).copied();
            }
        }
        return Err(ProfileError::Diagnostics(failures));
    }
    diagnostics.retain(|diagnostic| diagnostic.level != DiagnosticLevel::Error);
    Ok(ResolvedProfile {
        values,
        explicit_overrides: canonical,
        diagnostics,
    })
}

/// Resolve current defaults without a base profile.
pub(crate) fn resolve_defaults(
    bundle: &SurfaceContractBundle,
) -> Result<BTreeMap<String, ParameterValue>, ProfileError> {
    let values = resolve_values(bundle, &BTreeMap::new())?;
    validate_activity_and_required(bundle, &values, &BTreeMap::new()).or_else(
        |error| match error {
            ProfileError::Diagnostics(diagnostics)
                if diagnostics
                    .iter()
                    .all(|diagnostic| diagnostic.code == DiagnosticCode::MissingRequired) =>
            {
                Ok(())
            }
            other => Err(other),
        },
    )?;
    Ok(values)
}

pub(crate) fn resolve_with_overrides(
    bundle: &SurfaceContractBundle,
    overrides: &BTreeMap<String, ParameterValue>,
) -> Result<BTreeMap<String, ParameterValue>, ProfileError> {
    let mut normalized = BTreeMap::new();
    for (name, value) in overrides {
        let binding = bundle
            .surface
            .bindings()
            .iter()
            .find(|binding| &binding.name == name)
            .ok_or_else(|| ProfileError::Contract(format!("unknown parameter {name:?}")))?;
        let concept = bundle
            .catalog
            .concept(&binding.concept)
            .ok_or_else(|| ProfileError::Contract(format!("missing concept for {name}")))?;
        let value = normalize_value(value, concept)?;
        validate_value(&value, concept, &binding.refinements)?;
        normalized.insert(name.clone(), value);
    }
    let values = resolve_values(bundle, &normalized)?;
    validate_activity_and_required(bundle, &values, &normalized)?;
    Ok(values)
}

/// Resolve an interactive draft while retaining missing-required diagnostics.
pub(crate) fn resolve_draft_with_overrides(
    bundle: &SurfaceContractBundle,
    overrides: &BTreeMap<String, ParameterValue>,
) -> Result<(BTreeMap<String, ParameterValue>, Vec<Diagnostic>), ProfileError> {
    let mut normalized = BTreeMap::new();
    for (name, value) in overrides {
        let binding = bundle
            .surface
            .bindings()
            .iter()
            .find(|binding| &binding.name == name)
            .ok_or_else(|| ProfileError::Contract(format!("unknown parameter {name:?}")))?;
        let concept = bundle
            .catalog
            .concept(&binding.concept)
            .ok_or_else(|| ProfileError::Contract(format!("missing concept for {name}")))?;
        let value = normalize_value(value, concept)?;
        validate_value(&value, concept, &binding.refinements)?;
        normalized.insert(name.clone(), value);
    }
    let values = resolve_values(bundle, &normalized)?;
    match validate_activity_and_required(bundle, &values, &normalized) {
        Ok(()) => Ok((values, Vec::new())),
        Err(ProfileError::Diagnostics(diagnostics))
            if diagnostics.iter().all(|diagnostic| {
                matches!(
                    diagnostic.code,
                    DiagnosticCode::MissingRequired | DiagnosticCode::InactiveParameter
                )
            }) =>
        {
            Ok((values, diagnostics))
        }
        Err(error) => Err(error),
    }
}

fn resolve_values(
    bundle: &SurfaceContractBundle,
    overrides: &BTreeMap<String, ParameterValue>,
) -> Result<BTreeMap<String, ParameterValue>, ProfileError> {
    let mut values = overrides.clone();
    let mut pending = bundle
        .surface
        .bindings()
        .iter()
        .filter(|binding| !values.contains_key(&binding.name))
        .collect::<Vec<_>>();

    while !pending.is_empty() {
        let before = pending.len();
        pending.retain(|binding| {
            let Some(default) = evaluate_default(&binding.default, &values, bundle) else {
                return !matches!(binding.default, DefaultSpec::Required);
            };
            let concept = bundle
                .catalog
                .concept(&binding.concept)
                .expect("validated surface references concepts");
            match normalize_value(&default, concept).and_then(|value| {
                validate_value(&value, concept, &binding.refinements)?;
                Ok(value)
            }) {
                Ok(value) => {
                    values.insert(binding.name.clone(), value);
                    false
                }
                Err(_) => true,
            }
        });
        if pending.len() == before {
            break;
        }
    }

    for binding in pending {
        if !matches!(binding.default, DefaultSpec::Required) {
            return Err(ProfileError::Contract(format!(
                "could not resolve default for {:?}; check predicate graph and default type",
                binding.name
            )));
        }
    }
    Ok(values)
}

fn evaluate_default(
    default: &DefaultSpec,
    values: &BTreeMap<String, ParameterValue>,
    bundle: &SurfaceContractBundle,
) -> Option<ParameterValue> {
    match default {
        DefaultSpec::Required => None,
        DefaultSpec::Literal { value } => Some(value.clone()),
        DefaultSpec::Conditional { cases, fallback } => {
            for case in cases {
                match evaluate_predicate(&case.when, values, bundle) {
                    Some(true) => return Some(case.value.clone()),
                    Some(false) => {}
                    None => return None,
                }
            }
            Some(fallback.clone())
        }
    }
}

pub(crate) fn evaluate_predicate(
    predicate: &Predicate,
    values: &BTreeMap<String, ParameterValue>,
    bundle: &SurfaceContractBundle,
) -> Option<bool> {
    match predicate {
        Predicate::Always => Some(true),
        Predicate::Never => Some(false),
        Predicate::IsSet { parameter } => Some(values.contains_key(parameter)),
        Predicate::Equals { parameter, value } => {
            let current = values.get(parameter)?;
            let binding = bundle
                .surface
                .bindings()
                .iter()
                .find(|binding| &binding.name == parameter)?;
            let concept = bundle.catalog.concept(&binding.concept)?;
            semantic_eq(current, value, concept).ok()
        }
        Predicate::Not { predicate } => evaluate_predicate(predicate, values, bundle).map(|v| !v),
        Predicate::All { predicates } => {
            let mut unknown = false;
            for predicate in predicates {
                match evaluate_predicate(predicate, values, bundle) {
                    Some(false) => return Some(false),
                    Some(true) => {}
                    None => unknown = true,
                }
            }
            (!unknown).then_some(true)
        }
        Predicate::Any { predicates } => {
            let mut unknown = false;
            for predicate in predicates {
                match evaluate_predicate(predicate, values, bundle) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => unknown = true,
                }
            }
            (!unknown).then_some(false)
        }
    }
}

fn validate_activity_and_required(
    bundle: &SurfaceContractBundle,
    values: &BTreeMap<String, ParameterValue>,
    explicit: &BTreeMap<String, ParameterValue>,
) -> Result<(), ProfileError> {
    let mut diagnostics = Vec::new();
    for binding in bundle.surface.bindings() {
        let active = evaluate_predicate(&binding.active_when, values, bundle).unwrap_or(false);
        if explicit.contains_key(&binding.name) && !active {
            diagnostics.push(
                Diagnostic::error(
                    DiagnosticCode::InactiveParameter,
                    format!(
                        "parameter {:?} is inactive for the resolved values",
                        binding.name
                    ),
                )
                .for_parameter(binding.name.clone()),
            );
        }
        let required =
            active && evaluate_predicate(&binding.required_when, values, bundle).unwrap_or(false);
        if required
            && !values
                .get(&binding.name)
                .is_some_and(required_value_is_complete)
        {
            diagnostics.push(
                Diagnostic::error(
                    DiagnosticCode::MissingRequired,
                    format!("required parameter {:?} is missing or empty", binding.name),
                )
                .for_parameter(binding.name.clone()),
            );
        }
    }
    if diagnostics.is_empty() {
        Ok(())
    } else {
        Err(ProfileError::Diagnostics(diagnostics))
    }
}

/// A required parameter must carry usable information, not merely occupy a
/// map entry. `none` is the canonical explicit absence state; `auto` is a real
/// provider instruction and therefore remains complete.
fn required_value_is_complete(value: &ParameterValue) -> bool {
    match value {
        ParameterValue::String(value) => {
            let value = value.trim();
            !value.is_empty() && !value.eq_ignore_ascii_case("none")
        }
        ParameterValue::Array(values) => {
            !values.is_empty() && values.iter().all(required_value_is_complete)
        }
        ParameterValue::Table(values) => {
            !values.is_empty() && values.values().all(required_value_is_complete)
        }
        ParameterValue::Bool(_) | ParameterValue::Integer(_) | ParameterValue::Float(_) => true,
    }
}

fn migrate_parameters(
    profile: &ParameterProfile,
    bundle: &SurfaceContractBundle,
) -> Result<(BTreeMap<String, ParameterValue>, Vec<Diagnostic>), ProfileError> {
    let mut version = profile.header.contract;
    let mut parameters = profile.parameters.clone();
    let mut diagnostics = Vec::new();
    while version < bundle.surface.contract_version() {
        let migration = bundle
            .surface
            .migrations()
            .iter()
            .find(|migration| migration.from_contract == version)
            .ok_or_else(|| {
                ProfileError::Contract(format!(
                    "surface {} has no migration from contract {} to {}",
                    bundle.surface.id(),
                    version,
                    bundle.surface.contract_version()
                ))
            })?;
        for name in &migration.changed_defaults {
            if !parameters.contains_key(name) {
                diagnostics.push(
                    Diagnostic::warning(
                        DiagnosticCode::DefaultChanged,
                        format!(
                            "default for {name:?} changed after contract {}; omitted value adopts the current default",
                            migration.from_contract
                        ),
                    )
                    .for_parameter(name.clone()),
                );
            }
        }
        for step in &migration.steps {
            apply_migration_step(step, &mut parameters)?;
        }
        diagnostics.push(Diagnostic::warning(
            DiagnosticCode::Migrated,
            format!(
                "migrated surface {} contract {} to {}",
                bundle.surface.id(),
                migration.from_contract,
                migration.to_contract
            ),
        ));
        version = migration.to_contract;
    }
    Ok((parameters, diagnostics))
}

fn apply_migration_step(
    step: &MigrationStep,
    parameters: &mut BTreeMap<String, ParameterValue>,
) -> Result<(), ProfileError> {
    match step {
        MigrationStep::Rename { from, to } => {
            if let Some(value) = parameters.remove(from) {
                if parameters.insert(to.clone(), value).is_some() {
                    return Err(ProfileError::Contract(format!(
                        "migration rename {from:?} -> {to:?} collides with an explicit value"
                    )));
                }
            }
        }
        MigrationStep::Remove { parameter } => {
            parameters.remove(parameter);
        }
        MigrationStep::ReplaceValue {
            parameter,
            from,
            to,
        } => {
            if parameters.get(parameter) == Some(from) {
                parameters.insert(parameter.clone(), to.clone());
            }
        }
        MigrationStep::Transform {
            parameter,
            transform,
        } => {
            if let Some(value) = parameters.remove(parameter) {
                parameters.insert(
                    parameter.clone(),
                    transform_value(value, transform, parameter)?,
                );
            }
        }
    }
    Ok(())
}

fn transform_value(
    value: ParameterValue,
    transform: &MigrationTransform,
    parameter: &str,
) -> Result<ParameterValue, ProfileError> {
    match transform {
        MigrationTransform::ScalarToArray { length } => {
            Ok(ParameterValue::Array(vec![value; *length]))
        }
        MigrationTransform::SingletonArrayToScalar => match value {
            ParameterValue::Array(mut values) if values.len() == 1 => Ok(values.remove(0)),
            other => Err(ProfileError::Contract(format!(
                "migration expected a singleton array for {parameter:?}, got {other:?}"
            ))),
        },
        MigrationTransform::IntegerToString => match value {
            ParameterValue::Integer(value) => Ok(ParameterValue::String(value.to_string())),
            other => Err(ProfileError::Contract(format!(
                "migration expected an integer for {parameter:?}, got {other:?}"
            ))),
        },
        MigrationTransform::QuantityUnit { canonical_unit } => {
            normalize_quantity_unit(value, canonical_unit).map_err(ProfileError::from)
        }
        MigrationTransform::Lowercase => match value {
            ParameterValue::String(value) => Ok(ParameterValue::String(value.to_ascii_lowercase())),
            other => Err(ProfileError::Contract(format!(
                "migration expected a string for {parameter:?}, got {other:?}"
            ))),
        },
        MigrationTransform::Trim => match value {
            ParameterValue::String(value) => Ok(ParameterValue::String(value.trim().to_string())),
            other => Err(ProfileError::Contract(format!(
                "migration expected a string for {parameter:?}, got {other:?}"
            ))),
        },
    }
}

/// Render resolved values as deterministic sparse TOML against current defaults.
pub fn render_sparse_profile(
    bundle: &SurfaceContractBundle,
    values: &BTreeMap<String, ParameterValue>,
) -> Result<String, ProfileError> {
    let mut normalized = BTreeMap::new();
    for (name, value) in values {
        let binding = bundle
            .surface
            .bindings()
            .iter()
            .find(|binding| &binding.name == name)
            .ok_or_else(|| ProfileError::Contract(format!("unknown parameter {name:?}")))?;
        let concept = bundle
            .catalog
            .concept(&binding.concept)
            .ok_or_else(|| ProfileError::Contract(format!("missing concept for {name}")))?;
        let value = normalize_value(value, concept)?;
        validate_value(&value, concept, &binding.refinements)?;
        normalized.insert(name.clone(), value);
    }
    let candidate = resolve_values(bundle, &normalized)?;
    let mut sparse = BTreeMap::new();
    for binding in bundle.surface.bindings() {
        let Some(value) = candidate.get(&binding.name) else {
            continue;
        };
        let include = match &binding.default {
            DefaultSpec::Required => true,
            default => {
                let default = evaluate_default(default, &candidate, bundle).ok_or_else(|| {
                    ProfileError::Contract(format!("unresolved default {}", binding.name))
                })?;
                let concept = bundle.catalog.concept(&binding.concept).ok_or_else(|| {
                    ProfileError::Contract(format!("missing concept {}", binding.name))
                })?;
                !semantic_eq(value, &default, concept)?
            }
        };
        if include {
            sparse.insert(binding.name.clone(), value.clone());
        }
    }
    let resolved = resolve_with_overrides(bundle, &sparse)?;
    let mut bindings = bundle.surface.bindings().iter().collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let mut lines = profile_header(bundle);
    lines.push(String::new());
    lines.push("[parameters]".to_string());
    for binding in bindings {
        let Some(value) = resolved.get(&binding.name) else {
            continue;
        };
        if sparse.contains_key(&binding.name) {
            lines.push(format!("{} = {}", binding.name, render_toml_value(value)?));
        }
    }
    lines.push(String::new());
    Ok(lines.join("\n"))
}

/// Render a commented, non-activating parameter reference template.
pub fn render_documented_template(bundle: &SurfaceContractBundle) -> Result<String, ProfileError> {
    let defaults = resolve_defaults(bundle)?;
    let mut bindings = bundle.surface.bindings().iter().collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let mut lines = profile_header(bundle);
    lines.push(String::new());
    lines.push("[parameters]".to_string());
    let mut group = String::new();
    for binding in bindings {
        if group != binding.projections.presentation.group {
            group.clone_from(&binding.projections.presentation.group);
            lines.push(String::new());
            lines.push(format!("# --- {group} ---"));
        }
        let concept = bundle
            .catalog
            .concept(&binding.concept)
            .ok_or_else(|| ProfileError::Contract(format!("missing concept {}", binding.name)))?;
        lines.push(format!("# {}", concept.documentation.summary));
        if let Some(unit) = &concept.unit_dimension {
            lines.push(format!("# Unit dimension: {unit:?}"));
        }
        if matches!(binding.default, DefaultSpec::Required) {
            lines.push("# Required: supply an explicit value before running.".to_string());
            if let Some(example) = concept.documentation.examples.first() {
                lines.push(format!("# {} = {}", binding.name, quote_string(example)));
            } else {
                lines.push(format!("# {} = <required>", binding.name));
            }
        } else if let Some(value) = defaults.get(&binding.name) {
            lines.push(format!(
                "# {} = {}  # default",
                binding.name,
                render_toml_value(value)?
            ));
        }
    }
    lines.push(String::new());
    Ok(lines.join("\n"))
}

fn profile_header(bundle: &SurfaceContractBundle) -> Vec<String> {
    vec![
        "[casars]".to_string(),
        format!("format = {PROFILE_FORMAT_VERSION}"),
        format!("surface = {}", quote_string(bundle.surface.id())),
        format!(
            "kind = {}",
            quote_string(&bundle.surface.kind().to_string())
        ),
        format!("contract = {}", bundle.surface.contract_version()),
    ]
}

fn render_toml_value(value: &ParameterValue) -> Result<String, ProfileError> {
    match value {
        ParameterValue::Bool(value) => Ok(value.to_string()),
        ParameterValue::Integer(value) => Ok(value.to_string()),
        ParameterValue::Float(value) if value.is_finite() => Ok(value.to_string()),
        ParameterValue::Float(_) => Err(ProfileError::Contract(
            "cannot render non-finite profile value".to_string(),
        )),
        ParameterValue::String(value) => Ok(quote_string(value)),
        ParameterValue::Array(values) => values
            .iter()
            .map(render_toml_value)
            .collect::<Result<Vec<_>, _>>()
            .map(|values| format!("[{}]", values.join(", "))),
        ParameterValue::Table(values) => values
            .iter()
            .map(|(name, value)| {
                Ok(format!(
                    "{} = {}",
                    quote_string(name),
                    render_toml_value(value)?
                ))
            })
            .collect::<Result<Vec<_>, ProfileError>>()
            .map(|values| format!("{{ {} }}", values.join(", "))),
    }
}

fn quote_string(value: &str) -> String {
    toml::Value::String(value.to_string()).to_string()
}

fn from_toml(value: &toml::Value) -> Result<ParameterValue, ProfileError> {
    match value {
        toml::Value::String(value) => Ok(ParameterValue::String(value.clone())),
        toml::Value::Integer(value) => Ok(ParameterValue::Integer(*value)),
        toml::Value::Float(value) if value.is_finite() => Ok(ParameterValue::Float(*value)),
        toml::Value::Float(_) => Err(ProfileError::Parse {
            message: "non-finite numbers are forbidden".to_string(),
            location: None,
        }),
        toml::Value::Boolean(value) => Ok(ParameterValue::Bool(*value)),
        toml::Value::Datetime(_) => Err(ProfileError::Parse {
            message: "TOML datetime values are not parameter values".to_string(),
            location: None,
        }),
        toml::Value::Array(values) => values
            .iter()
            .map(from_toml)
            .collect::<Result<Vec<_>, _>>()
            .map(ParameterValue::Array),
        toml::Value::Table(values) => values
            .iter()
            .map(|(name, value)| Ok((name.clone(), from_toml(value)?)))
            .collect::<Result<BTreeMap<_, _>, ProfileError>>()
            .map(ParameterValue::Table),
    }
}

fn required_u32(
    table: &toml::map::Map<String, toml::Value>,
    key: &str,
) -> Result<u32, ProfileError> {
    table
        .get(key)
        .and_then(toml::Value::as_integer)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| ProfileError::Parse {
            message: format!("[casars].{key} must be a non-negative integer"),
            location: None,
        })
}

fn required_string(
    table: &toml::map::Map<String, toml::Value>,
    key: &str,
) -> Result<String, ProfileError> {
    table
        .get(key)
        .and_then(toml::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| ProfileError::Parse {
            message: format!("[casars].{key} must be a string"),
            location: None,
        })
}

fn reject_unknown_keys<'a>(
    actual: impl Iterator<Item = &'a String>,
    allowed: impl IntoIterator<Item = &'static str>,
    scope: &str,
) -> Result<(), ProfileError> {
    let allowed = allowed.into_iter().collect::<BTreeSet<_>>();
    if let Some(key) = actual
        .into_iter()
        .find(|key| !allowed.contains(key.as_str()))
    {
        Err(ProfileError::Parse {
            message: format!("unknown key {key:?} in {scope}"),
            location: None,
        })
    } else {
        Ok(())
    }
}

fn offset_location(source: &str, offset: usize) -> SourceLocation {
    let prefix = &source[..offset.min(source.len())];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix
        .rsplit_once('\n')
        .map_or(prefix.len() + 1, |(_, tail)| tail.len() + 1);
    SourceLocation { line, column }
}

fn locate_key(source: &str, key: &str, parameters_only: bool) -> Option<SourceLocation> {
    let mut in_parameters = false;
    for (line_index, line) in source.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') {
            in_parameters = trimmed == "[parameters]";
            continue;
        }
        if parameters_only && !in_parameters {
            continue;
        }
        let Some(candidate) = trimmed.split_once('=').map(|(name, _)| name.trim()) else {
            continue;
        };
        if candidate.trim_matches(['\'', '"']) == key {
            return Some(SourceLocation {
                line: line_index + 1,
                column: line.len() - trimmed.len() + 1,
            });
        }
    }
    None
}

fn spelling_suggestions(name: &str, known: &BTreeSet<&str>) -> Vec<String> {
    let mut candidates = known
        .iter()
        .map(|candidate| (levenshtein(name, candidate), (*candidate).to_string()))
        .filter(|(distance, candidate)| {
            *distance <= 3 || candidate.starts_with(name) || name.starts_with(candidate)
        })
        .collect::<Vec<_>>();
    candidates.sort();
    candidates
        .into_iter()
        .take(3)
        .map(|(_, candidate)| candidate)
        .collect()
}

fn levenshtein(left: &str, right: &str) -> usize {
    let mut previous = (0..=right.chars().count()).collect::<Vec<_>>();
    for (left_index, left_char) in left.chars().enumerate() {
        let mut current = vec![left_index + 1];
        for (right_index, right_char) in right.chars().enumerate() {
            current.push(
                (previous[right_index + 1] + 1)
                    .min(current[right_index] + 1)
                    .min(previous[right_index] + usize::from(left_char != right_char)),
            );
        }
        previous = current;
    }
    previous.last().copied().unwrap_or(0)
}

/// Strict profile parse/resolution failure.
#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("profile parse error: {message}")]
    Parse {
        message: String,
        location: Option<SourceLocation>,
    },
    #[error("profile diagnostics: {0:?}")]
    Diagnostics(Vec<Diagnostic>),
    #[error("parameter contract error: {0}")]
    Contract(String),
    #[error(transparent)]
    Normalization(#[from] NormalizationError),
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::SurfaceParameterBinding;
    use casa_provider_contracts::{
        CELL_CONCEPT_ID, ContextRole, ParameterCatalog, ParameterConcept, ParameterConceptId,
        ParameterConceptRef, ParameterDocs, ParameterPresentation, ParameterProjections,
        ParameterRole, ParameterType, PersistenceClass, SemanticRevision, SessionDefinition,
        SurfaceDefinition, SurfaceExecutionProjection, SurfaceMigration, TaskDefinition,
        UnitDimension,
    };

    use super::*;

    fn bundle(kind: SurfaceKind) -> SurfaceContractBundle {
        let imsize = ParameterConcept {
            id: ParameterConceptId::new(casa_provider_contracts::IMSIZE_CONCEPT_ID),
            semantic_revision: SemanticRevision(1),
            casa_name: "imsize".into(),
            value_domain: ParameterType::Array {
                element: Box::new(ParameterType::Integer),
                min_items: 2,
                max_items: Some(2),
                allow_scalar: true,
            },
            normalization: casa_provider_contracts::NormalizationRule::Identity,
            base_constraints: Vec::new(),
            unit_dimension: None,
            semantic_role: ParameterRole::Geometry,
            documentation: ParameterDocs {
                summary: "Image size".into(),
                details: None,
                examples: vec!["512".into()],
            },
            persistence_class: PersistenceClass::Profile,
        };
        let cell = ParameterConcept {
            id: ParameterConceptId::new(CELL_CONCEPT_ID),
            semantic_revision: SemanticRevision(1),
            casa_name: "cell".into(),
            value_domain: ParameterType::Array {
                element: Box::new(ParameterType::Quantity {
                    dimension: UnitDimension::Angle,
                    canonical_unit: "arcsec".into(),
                    special_values: vec!["auto".into()],
                }),
                min_items: 2,
                max_items: Some(2),
                allow_scalar: true,
            },
            normalization: casa_provider_contracts::NormalizationRule::Quantity {
                canonical_unit: "arcsec".into(),
            },
            base_constraints: Vec::new(),
            unit_dimension: Some(UnitDimension::Angle),
            semantic_role: ParameterRole::Geometry,
            documentation: ParameterDocs {
                summary: "Pixel scale".into(),
                details: None,
                examples: vec!["0.2arcsec".into()],
            },
            persistence_class: PersistenceClass::Profile,
        };
        let bindings = vec![
            test_binding(
                "imsize",
                imsize.reference(),
                0,
                DefaultSpec::Literal {
                    value: ParameterValue::Integer(512),
                },
            ),
            test_binding(
                "cell",
                cell.reference(),
                1,
                DefaultSpec::Literal {
                    value: ParameterValue::String("1arcsec".into()),
                },
            ),
        ];
        let surface = match kind {
            SurfaceKind::Task => SurfaceDefinition::Task(TaskDefinition {
                id: "imager".into(),
                contract_version: 1,
                display_name: "Imager".into(),
                category: "Imaging".into(),
                summary: "Image".into(),
                provider_family: "imager".into(),
                execution: SurfaceExecutionProjection::default(),
                safety_rules: Vec::new(),
                bindings,
                migrations: Vec::new(),
            }),
            SurfaceKind::Session => SurfaceDefinition::Session(SessionDefinition {
                id: "imager".into(),
                contract_version: 1,
                display_name: "Imager".into(),
                category: "Imaging".into(),
                summary: "Image".into(),
                provider_family: "imager".into(),
                execution: SurfaceExecutionProjection::default(),
                bindings,
                migrations: Vec::new(),
            }),
        };
        SurfaceContractBundle {
            schema_version: 1,
            surface,
            catalog: ParameterCatalog {
                schema_version: 1,
                concepts: vec![imsize, cell],
            },
        }
    }

    fn test_binding(
        name: &str,
        concept: ParameterConceptRef,
        order: usize,
        default: DefaultSpec,
    ) -> SurfaceParameterBinding {
        SurfaceParameterBinding {
            name: name.into(),
            concept,
            order,
            default,
            required_when: Predicate::Never,
            active_when: Predicate::Always,
            refinements: Vec::new(),
            context_role: Some(ContextRole::Presentation),
            surface_note: None,
            projections: ParameterProjections {
                cli: None,
                provider: None,
                python: None,
                presentation: ParameterPresentation {
                    label: name.into(),
                    group: "Geometry".into(),
                    advanced: false,
                    hidden: false,
                },
            },
            aliases: Vec::new(),
            reviewed_homonym: None,
        }
    }

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
    fn every_supported_toml_value_shape_round_trips_deterministically() {
        let values = [
            ParameterValue::Bool(true),
            ParameterValue::Integer(-7),
            ParameterValue::Float(1.25),
            ParameterValue::String("plain ASCII profile text".into()),
            ParameterValue::Array(vec![ParameterValue::Integer(1), ParameterValue::Integer(2)]),
            ParameterValue::Table(BTreeMap::from([
                ("a.b".into(), ParameterValue::Bool(false)),
                (
                    "display name".into(),
                    ParameterValue::String("target".into()),
                ),
                (
                    "quoted\"key".into(),
                    ParameterValue::String("escaped".into()),
                ),
                (
                    "nested".into(),
                    ParameterValue::Table(BTreeMap::from([(
                        "inner.key".into(),
                        ParameterValue::Integer(3),
                    )])),
                ),
            ])),
        ];

        for value in values {
            let rendered = render_toml_value(&value).unwrap();
            let document = format!("value = {rendered}")
                .parse::<toml::Value>()
                .unwrap();
            let reparsed = from_toml(document.get("value").unwrap()).unwrap();
            assert_eq!(reparsed, value);
            assert_eq!(render_toml_value(&reparsed).unwrap(), rendered);
        }
    }

    #[test]
    fn rejects_wrong_surface_datetime_and_unknown_parameter_with_suggestion() {
        let bundle = bundle(SurfaceKind::Task);
        let wrong = r#"[casars]
format = 1
surface = "other"
kind = "task"
contract = 1
[parameters]
"#;
        let ProfileError::Diagnostics(diagnostics) =
            resolve_profile(&parse_profile(wrong).unwrap(), &bundle).unwrap_err()
        else {
            panic!("expected wrong-surface diagnostics")
        };
        assert_eq!(diagnostics[0].code, DiagnosticCode::WrongSurface);
        assert_eq!(diagnostics[0].location.unwrap().line, 3);
        assert_eq!(diagnostics[0].suggestions, vec!["imager"]);
        let datetime = r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1
[parameters]
imsize = 1979-05-27T07:32:00Z
"#;
        assert!(parse_profile(datetime).is_err());
        let unknown = r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1
[parameters]
imsze = 10
"#;
        let ProfileError::Diagnostics(diagnostics) =
            resolve_profile(&parse_profile(unknown).unwrap(), &bundle).unwrap_err()
        else {
            panic!("expected diagnostics")
        };
        assert_eq!(diagnostics[0].suggestions, vec!["imsize"]);
        assert_eq!(diagnostics[0].location.unwrap().line, 7);
    }

    #[test]
    fn inactive_explicit_parameters_report_their_source_location() {
        let mut bundle = bundle(SurfaceKind::Task);
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!()
        };
        definition
            .bindings
            .iter_mut()
            .find(|binding| binding.name == "cell")
            .unwrap()
            .active_when = Predicate::Never;
        let profile = parse_profile(
            r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1
[parameters]
cell = "2arcsec"
"#,
        )
        .unwrap();
        let ProfileError::Diagnostics(diagnostics) =
            resolve_profile(&profile, &bundle).unwrap_err()
        else {
            panic!("expected inactive-parameter diagnostic")
        };
        assert_eq!(diagnostics[0].code, DiagnosticCode::InactiveParameter);
        assert_eq!(diagnostics[0].location.unwrap().line, 7);
    }

    #[test]
    fn template_comments_do_not_activate_defaults() {
        let template = render_documented_template(&bundle(SurfaceKind::Task)).unwrap();
        let profile = parse_profile(&template).unwrap();
        assert!(profile.parameters.is_empty());
    }

    #[test]
    fn required_value_completeness_rejects_explicit_absence_but_accepts_auto() {
        assert!(!required_value_is_complete(&ParameterValue::String(
            String::new()
        )));
        assert!(!required_value_is_complete(&ParameterValue::String(
            "  ".into()
        )));
        assert!(!required_value_is_complete(&ParameterValue::String(
            "none".into()
        )));
        assert!(!required_value_is_complete(&ParameterValue::Array(vec![])));
        assert!(!required_value_is_complete(&ParameterValue::Array(vec![
            ParameterValue::String("none".into())
        ])));
        assert!(!required_value_is_complete(&ParameterValue::Table(
            BTreeMap::new()
        )));
        assert!(required_value_is_complete(&ParameterValue::String(
            "auto".into()
        )));
        assert!(required_value_is_complete(&ParameterValue::Bool(false)));
        assert!(required_value_is_complete(&ParameterValue::Array(vec![
            ParameterValue::String("input.ms".into())
        ])));
    }

    #[test]
    fn old_sparse_profile_migrates_explicit_values_and_adopts_changed_defaults() {
        let mut bundle = bundle(SurfaceKind::Task);
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!()
        };
        definition.contract_version = 2;
        definition.migrations = vec![SurfaceMigration {
            from_contract: 1,
            to_contract: 2,
            steps: vec![MigrationStep::Rename {
                from: "pixels".into(),
                to: "imsize".into(),
            }],
            changed_defaults: vec!["cell".into()],
        }];
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
pixels = 256
"#,
        )
        .unwrap();
        let resolved = resolve_profile(&old, &bundle).unwrap();
        assert_eq!(
            resolved.values["imsize"],
            ParameterValue::Array(vec![ParameterValue::Integer(256); 2])
        );
        assert_eq!(
            resolved.values["cell"],
            ParameterValue::Array(vec![ParameterValue::String("2arcsec".into()); 2])
        );
        assert!(
            resolved
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::DefaultChanged)
        );
        assert!(
            resolved
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::Migrated)
        );

        let saved = render_sparse_profile(&bundle, &resolved.values).unwrap();
        assert!(saved.contains(&format!("contract = {}", bundle.surface.contract_version())));
        assert!(saved.contains("imsize = [256, 256]"));
        assert!(!saved.contains("pixels"));
        assert!(!saved.contains("cell ="));
    }

    #[test]
    fn importvla_v1_scalar_archivefile_migrates_to_the_canonical_list() {
        let bundle = casa_provider_contracts::builtin_surface_bundle("importvla").unwrap();
        let old = parse_profile(
            r#"[casars]
format = 1
surface = "importvla"
kind = "task"
contract = 1

[parameters]
archivefiles = "raw/one.exp"
"#,
        )
        .unwrap();

        let resolved = resolve_profile(&old, &bundle).unwrap();
        assert_eq!(
            resolved.values["archivefiles"],
            ParameterValue::Array(vec![ParameterValue::String("raw/one.exp".into())])
        );
        assert!(
            resolved
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::Migrated)
        );

        let saved = render_sparse_profile(&bundle, &resolved.values).unwrap();
        assert!(saved.contains(&format!("contract = {}", bundle.surface.contract_version())));
        assert!(saved.contains("archivefiles = [\"raw/one.exp\"]"));
    }

    #[test]
    fn msexplore_v2_numeric_panel_key_migrates_to_the_string_contract() {
        let bundle = casa_provider_contracts::builtin_surface_bundle("msexplore").unwrap();
        let old = parse_profile(
            r#"[casars]
format = 1
surface = "msexplore"
kind = "task"
contract = 2

[parameters]
vis = "target.ms"
flag_panel = 17
"#,
        )
        .unwrap();

        let resolved = resolve_profile(&old, &bundle).unwrap();
        assert_eq!(
            resolved.values["flag_panel"],
            ParameterValue::String("17".into())
        );
        let saved = render_sparse_profile(&bundle, &resolved.values).unwrap();
        assert!(saved.contains(&format!("contract = {}", bundle.surface.contract_version())));
        assert!(saved.contains("flag_panel = \"17\""));
    }

    #[test]
    fn multi_hop_migrations_transform_and_preserve_explicit_old_values() {
        let mut bundle = bundle(SurfaceKind::Task);
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!()
        };
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
        assert_eq!(resolved.explicit_overrides, resolved.values);
        assert_eq!(
            resolved
                .diagnostics
                .iter()
                .filter(|diagnostic| diagnostic.code == DiagnosticCode::Migrated)
                .count(),
            2
        );
        assert!(
            resolved
                .diagnostics
                .iter()
                .all(|diagnostic| diagnostic.code != DiagnosticCode::DefaultChanged)
        );

        let saved = render_sparse_profile(&bundle, &resolved.values).unwrap();
        assert!(saved.contains("contract = 3"));
        assert!(saved.contains("imsize = [128, 128]"));
        assert!(saved.contains("cell = [\"60arcsec\", \"60arcsec\"]"));

        let mut incompatible = bundle.clone();
        let SurfaceDefinition::Task(definition) = &mut incompatible.surface else {
            unreachable!()
        };
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

    #[test]
    fn newly_required_parameter_needs_a_migration_or_reports_a_clear_error() {
        let mut bundle = bundle(SurfaceKind::Task);
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!()
        };
        definition.contract_version = 2;
        definition.migrations = vec![SurfaceMigration {
            from_contract: 1,
            to_contract: 2,
            steps: Vec::new(),
            changed_defaults: Vec::new(),
        }];
        let cell = definition
            .bindings
            .iter_mut()
            .find(|binding| binding.name == "cell")
            .unwrap();
        cell.default = DefaultSpec::Required;
        cell.required_when = Predicate::Always;

        let old_without_value = parse_profile(
            r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
imsize = 256
"#,
        )
        .unwrap();
        let ProfileError::Diagnostics(diagnostics) =
            resolve_profile(&old_without_value, &bundle).unwrap_err()
        else {
            panic!("expected a missing-required diagnostic")
        };
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DiagnosticCode::MissingRequired
                && diagnostic.parameter.as_deref() == Some("cell")
        }));

        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!()
        };
        definition.migrations[0].steps = vec![MigrationStep::Rename {
            from: "legacy_cell".into(),
            to: "cell".into(),
        }];
        let old_with_migratable_value = parse_profile(
            r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 1

[parameters]
imsize = 256
legacy_cell = "1arcsec"
"#,
        )
        .unwrap();
        let resolved = resolve_profile(&old_with_migratable_value, &bundle).unwrap();
        assert_eq!(
            resolved.values["cell"],
            ParameterValue::Array(vec![ParameterValue::String("1arcsec".into()); 2])
        );
    }

    #[test]
    fn future_surface_contract_is_rejected() {
        let future = parse_profile(
            r#"[casars]
format = 1
surface = "imager"
kind = "task"
contract = 99

[parameters]
"#,
        )
        .unwrap();
        let ProfileError::Diagnostics(diagnostics) =
            resolve_profile(&future, &bundle(SurfaceKind::Task)).unwrap_err()
        else {
            panic!("expected a future-contract diagnostic")
        };
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiagnosticCode::FutureContract)
        );
    }
}
