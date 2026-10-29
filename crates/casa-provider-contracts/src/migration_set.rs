// SPDX-License-Identifier: LGPL-3.0-or-later
//! The rules for [`MigrationStep::Set`](super::MigrationStep::Set) (#931).
//!
//! A sparse profile holds only the values its author set; an omitted value
//! adopts the current default, with a warning when that default changed
//! (ADR-0006). A `Set` step moves an explicit value between parameters: when
//! a surface takes CASA's names, casa-rs's old `action='unflag'` becomes
//! CASA's `mode='unflag'`, and mstransform's old `width` becomes CASA's
//! `chanaverage=True` with `chanbin`. Its condition may therefore name only
//! values a profile set: it is a conjunction of [`Predicate::IsSet`] and
//! [`Predicate::Equals`], so no step can pin a default the profile omitted.

use std::collections::BTreeMap;

use super::{ContractValidationError, ParameterValue, Predicate, SurfaceDefinition};

impl Predicate {
    /// Whether this condition holds of a profile's explicit `overrides`, by
    /// the parameter names they carry: `IsSet` when the profile set the
    /// parameter, `Equals` when it set it to exactly `value`, `All` when
    /// every part holds. Any other predicate never holds here; contract
    /// validation refuses it in a migration.
    pub fn holds_for_overrides(&self, overrides: &BTreeMap<String, ParameterValue>) -> bool {
        match self {
            Self::IsSet { parameter } => overrides.contains_key(parameter),
            Self::Equals { parameter, value } => overrides.get(parameter) == Some(value),
            Self::All { predicates } => {
                !predicates.is_empty()
                    && predicates
                        .iter()
                        .all(|predicate| predicate.holds_for_overrides(overrides))
            }
            Self::Always | Self::Never | Self::Not { .. } | Self::Any { .. } => false,
        }
    }
}

/// Whether `when` names only values a profile set.
fn names_only_set_values(when: &Predicate) -> bool {
    match when {
        Predicate::IsSet { .. } | Predicate::Equals { .. } => true,
        Predicate::All { predicates } => {
            !predicates.is_empty() && predicates.iter().all(names_only_set_values)
        }
        Predicate::Always | Predicate::Never | Predicate::Not { .. } | Predicate::Any { .. } => {
            false
        }
    }
}

/// Refuse a `Set` step whose condition could hold of a profile that set
/// none of the values it names.
pub(super) fn validate_condition(
    surface: &SurfaceDefinition,
    parameter: &str,
    when: &Predicate,
    errors: &mut Vec<ContractValidationError>,
) {
    if !names_only_set_values(when) {
        errors.push(
            ContractValidationError::new(
                "migration_set_condition",
                "a migration's set step needs a condition of explicit values only: \
                 is_set and equals, or all of them",
            )
            .at(surface.id(), Some(parameter)),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn equals(parameter: &str, value: &str) -> Predicate {
        Predicate::Equals {
            parameter: parameter.into(),
            value: ParameterValue::String(value.into()),
        }
    }

    #[test]
    fn a_condition_holds_only_of_values_a_profile_set() {
        let unflag = Predicate::All {
            predicates: vec![equals("mode", "manual"), equals("action", "unflag")],
        };
        let mut overrides = BTreeMap::from([(
            "action".to_string(),
            ParameterValue::String("unflag".into()),
        )]);
        // The old default mode is not assumed: the profile did not set it.
        assert!(!unflag.holds_for_overrides(&overrides));
        overrides.insert("mode".into(), ParameterValue::String("manual".into()));
        assert!(unflag.holds_for_overrides(&overrides));
        let width = Predicate::IsSet {
            parameter: "chanbin".into(),
        };
        assert!(!width.holds_for_overrides(&overrides));
        overrides.insert("chanbin".into(), ParameterValue::Integer(4));
        assert!(width.holds_for_overrides(&overrides));
        assert!(
            !Predicate::All { predicates: vec![] }.holds_for_overrides(&overrides),
            "an empty conjunction names nothing"
        );
    }

    #[test]
    fn a_condition_that_could_pin_an_omitted_default_is_refused() {
        let pins = [
            Predicate::Always,
            Predicate::Not {
                predicate: Box::new(Predicate::IsSet {
                    parameter: "mode".into(),
                }),
            },
            Predicate::Any {
                predicates: vec![equals("mode", "manual")],
            },
            Predicate::All {
                predicates: vec![
                    equals("mode", "manual"),
                    Predicate::Not {
                        predicate: Box::new(equals("action", "flag")),
                    },
                ],
            },
            Predicate::All { predicates: vec![] },
        ];
        for when in pins {
            assert!(!names_only_set_values(&when), "{when:?}");
        }
        assert!(names_only_set_values(&Predicate::All {
            predicates: vec![
                equals("mode", "manual"),
                Predicate::IsSet {
                    parameter: "action".into()
                }
            ],
        }));
    }
}
