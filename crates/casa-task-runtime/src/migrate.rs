// SPDX-License-Identifier: LGPL-3.0-or-later
//! A saved profile's explicit overrides carried through the surface's
//! migrations to the current contract (ADR-0006).
//!
//! Each migration rewrites the values the profile set. A value it omitted
//! adopts the current default, and when the migration lists that default
//! as changed, the profile is warned. A value counts as set when the
//! profile gave it under its old name, or when a step fills it from other
//! explicit values because the profile left it unset (the old gaincal's
//! `gain_model_source='point'` without `smodel` is `smodel=[1, 0, 0, 0]`):
//! neither adopts the default.

use std::collections::{BTreeMap, BTreeSet};

use casa_provider_contracts::{
    MigrationStep, MigrationTransform, ParameterValue, SurfaceContractBundle, SurfaceMigration,
};

use crate::diagnostic::{Diagnostic, DiagnosticCode};
use crate::normalize::normalize_quantity_unit;
use crate::profile::{ParameterProfile, ProfileError};

/// The profile's explicit values at the bundle's contract, with a warning
/// for each changed default the profile omitted and one per migration run.
pub(crate) fn migrate_parameters(
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
        let explicit = migrate_once(
            migration,
            &mut parameters,
            bundle.surface.id(),
            &mut diagnostics,
        )?;
        for name in &migration.changed_defaults {
            if !explicit.contains(name) {
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

/// Run one migration's steps over `parameters`, and return the names whose
/// values the profile set, by the names they have after the migration.
fn migrate_once(
    migration: &SurfaceMigration,
    parameters: &mut BTreeMap<String, ParameterValue>,
    surface: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<BTreeSet<String>, ProfileError> {
    let mut explicit = parameters.keys().cloned().collect::<BTreeSet<_>>();
    for step in &migration.steps {
        match step {
            MigrationStep::Rename { from, to } if explicit.contains(from) => {
                explicit.insert(to.clone());
            }
            MigrationStep::Set {
                parameter, when, ..
            } if step.fills_when_unset(parameter) && when.holds_for_overrides(parameters) => {
                explicit.insert(parameter.clone());
            }
            // A value removed as the absence it was is omitted.
            MigrationStep::Remove {
                parameter,
                when: Some(when),
            } if when.holds_for_overrides(parameters) => {
                explicit.remove(parameter);
            }
            _ => {}
        }
        let replacement = match step {
            MigrationStep::ReplaceValue {
                parameter,
                from,
                to,
            } if from != to && parameters.get(parameter) == Some(from) => {
                Some((parameter, from, to))
            }
            _ => None,
        };
        apply_migration_step(step, parameters)?;
        if let Some((parameter, from, to)) = replacement {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::Migrated,
                    format!(
                        "migrated stored {parameter:?} from {} to {} for native {surface} contract {} to {}. This translates the stored legacy representation; it does not reconstruct earlier input or permit the old spelling in current calls",
                        from.to_plain_json(), to.to_plain_json(),
                        migration.from_contract, migration.to_contract,
                    ),
                )
                .for_parameter(parameter.clone()),
            );
        }
    }
    Ok(explicit)
}

fn apply_migration_step(
    step: &MigrationStep,
    parameters: &mut BTreeMap<String, ParameterValue>,
) -> Result<(), ProfileError> {
    match step {
        MigrationStep::Rename { from, to } => {
            if let Some(value) = parameters.remove(from)
                && parameters.insert(to.clone(), value).is_some()
            {
                return Err(ProfileError::Contract(format!(
                    "migration rename {from:?} -> {to:?} collides with an explicit value"
                )));
            }
        }
        MigrationStep::Remove { parameter, when } => {
            if when
                .as_ref()
                .is_none_or(|when| when.holds_for_overrides(parameters))
            {
                parameters.remove(parameter);
            }
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
        MigrationStep::Set {
            parameter,
            value,
            when,
        } => {
            if when.holds_for_overrides(parameters) {
                parameters.insert(parameter.clone(), value.clone());
            }
        }
        MigrationStep::Broadcast {
            parameter,
            to_length_of,
        } => crate::migration_list::broadcast(parameters, parameter, to_length_of),
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
        MigrationTransform::NumberList => crate::normalize::number_list(value, parameter),
        MigrationTransform::TextList { separator, element } => {
            crate::migration_list::text_list(value, separator, *element, parameter)
        }
    }
}
