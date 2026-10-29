// SPDX-License-Identifier: LGPL-3.0-or-later
//! Profile migrations between consecutive surface-contract versions
//! (ADR-0006): the steps a saved sparse profile takes to the current
//! contract, and their validation.
//!
//! A migration rewrites explicit overrides only. An omitted value adopts
//! the current default, with a warning when the migration lists it among
//! its `changed_defaults`.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    ContractValidationError, ListElement, ParameterValue, Predicate, SurfaceDefinition,
    migration_set,
};

/// Ordered migration between consecutive surface-contract versions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SurfaceMigration {
    pub from_contract: u32,
    pub to_contract: u32,
    #[serde(default)]
    pub steps: Vec<MigrationStep>,
    /// Omitted values whose meaning changes because the current default changed.
    #[serde(default)]
    pub changed_defaults: Vec<String>,
}

/// Migration of explicit sparse overrides only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MigrationStep {
    Rename {
        from: String,
        to: String,
    },
    Remove {
        parameter: String,
    },
    ReplaceValue {
        parameter: String,
        from: ParameterValue,
        to: ParameterValue,
    },
    Transform {
        parameter: String,
        transform: MigrationTransform,
    },
    /// Set `parameter` to `value` when the profile's explicit overrides
    /// satisfy `when`: a value that moves between parameters (#931). `when`
    /// holds only of values a profile set, so an omitted value still adopts
    /// the current default (ADR-0006); see [`Predicate::holds_for_overrides`].
    Set {
        parameter: String,
        value: ParameterValue,
        when: Predicate,
    },
    /// Repeat the one item of the explicit list `parameter` once for each
    /// item of the explicit list `to_length_of`: casa-rs's old per-table
    /// lists gave one value for every table, where CASA's are positional
    /// (#931).
    Broadcast {
        parameter: String,
        to_length_of: String,
    },
}

impl MigrationStep {
    /// Whether this step sets `parameter` from other explicit values only
    /// when the profile left `parameter` itself unset: a `Set` whose
    /// condition requires it unset. Such a step gives the omitted value a
    /// meaning of its own, so the value does not adopt the current default.
    pub fn fills_when_unset(&self, parameter: &str) -> bool {
        let Self::Set {
            parameter: target,
            when,
            ..
        } = self
        else {
            return false;
        };
        let unset = |predicate: &Predicate| {
            matches!(predicate, Predicate::Not { predicate }
                if matches!(&**predicate, Predicate::IsSet { parameter: name } if name == parameter))
        };
        target == parameter
            && match when {
                Predicate::All { predicates } => predicates.iter().any(unset),
                other => unset(other),
            }
    }
}

/// Closed set of safe deterministic migration transforms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MigrationTransform {
    ScalarToArray {
        length: usize,
    },
    SingletonArrayToScalar,
    IntegerToString,
    QuantityUnit {
        canonical_unit: String,
    },
    Lowercase,
    Trim,
    /// Comma-separated numbers (`"0.1,10"`) as a list of numbers, and the
    /// old explicit absence (`"none"` or empty) as CASA's `-1` (#931).
    NumberList,
    /// Text of `separator`-separated items as a list of `element` values,
    /// and the old explicit absence (`"none"` or empty) as an empty list
    /// (#931); see [`ListElement`].
    TextList {
        separator: String,
        element: ListElement,
    },
}

pub(super) fn validate_migrations(
    surface: &SurfaceDefinition,
    errors: &mut Vec<ContractValidationError>,
) {
    // An empty migration set is an explicit current-only contract: readers
    // accept exactly `contract_version` and retain no compatibility path.
    if surface.migrations().is_empty() {
        for binding in surface.bindings() {
            if !binding.aliases.is_empty() {
                errors.push(
                    ContractValidationError::new(
                        "current_only_alias",
                        "current-only surfaces cannot retain compatibility aliases",
                    )
                    .at(surface.id(), Some(&binding.name)),
                );
            }
        }
        return;
    }
    let mut expected = 1;
    let known = surface
        .bindings()
        .iter()
        .map(|binding| binding.name.as_str())
        .collect::<BTreeSet<_>>();
    for migration in surface.migrations() {
        if migration.from_contract != expected
            || migration.to_contract != migration.from_contract.saturating_add(1)
        {
            errors.push(
                ContractValidationError::new(
                    "migration_chain",
                    "migrations must be ordered and advance exactly one contract version",
                )
                .at(surface.id(), None),
            );
        }
        for name in &migration.changed_defaults {
            if !known.contains(name.as_str()) {
                errors.push(
                    ContractValidationError::new(
                        "unknown_changed_default",
                        format!("migration names unknown changed-default parameter {name:?}"),
                    )
                    .at(surface.id(), Some(name)),
                );
            }
        }
        for step in &migration.steps {
            validate_step(surface, &known, step, errors);
        }
        expected = migration.to_contract;
    }
    if expected != surface.contract_version() {
        errors.push(
            ContractValidationError::new(
                "migration_target",
                "migration chain must end at the current contract version",
            )
            .at(surface.id(), None),
        );
    }
}

fn validate_step(
    surface: &SurfaceDefinition,
    known: &BTreeSet<&str>,
    step: &MigrationStep,
    errors: &mut Vec<ContractValidationError>,
) {
    let current_name = match step {
        MigrationStep::Rename { to, .. } => Some(to),
        MigrationStep::ReplaceValue { parameter, .. }
        | MigrationStep::Transform { parameter, .. }
        | MigrationStep::Set { parameter, .. }
        | MigrationStep::Broadcast { parameter, .. } => Some(parameter),
        MigrationStep::Remove { .. } => None,
    };
    match step {
        MigrationStep::Set {
            parameter, when, ..
        } => migration_set::validate_condition(surface, parameter, when, errors),
        MigrationStep::Broadcast { to_length_of, .. } if !known.contains(to_length_of.as_str()) => {
            errors.push(
                ContractValidationError::new(
                    "unknown_migration_target",
                    format!("migration broadcasts to unknown parameter {to_length_of:?}"),
                )
                .at(surface.id(), Some(to_length_of)),
            );
        }
        _ => {}
    }
    if let Some(name) = current_name
        && !known.contains(name.as_str())
    {
        errors.push(
            ContractValidationError::new(
                "unknown_migration_target",
                format!("migration targets unknown current parameter {name:?}"),
            )
            .at(surface.id(), Some(name)),
        );
    }
}
