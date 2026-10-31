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
//!
//! A conjunction holding at least one explicit value may also narrow it
//! with a [`Predicate::Not`]:
//! - of an `IsSet`, a parameter the step depends on was left unset: the old
//!   gaincal's explicit `gain_model_source='point'` meant a 1 Jy model when
//!   `smodel` was omitted;
//! - of an `Equals` on a parameter the conjunction names as set, that value
//!   is not one value: flagdata's old explicit `timedev` is a threshold
//!   unless it is the old absence `'none'`.

use std::collections::{BTreeMap, BTreeSet};

use super::{ContractValidationError, ParameterValue, Predicate, SurfaceDefinition};

impl Predicate {
    /// Whether this condition holds of a profile's explicit `overrides`, by
    /// the parameter names they carry: `IsSet` when the profile set the
    /// parameter, `Equals` when it set it to exactly `value`, `Not` of
    /// either when that does not hold, `All` when every part holds. Any
    /// other predicate never holds here; contract validation refuses it in
    /// a migration, and a `Not` outside a conjunction holding an explicit
    /// value.
    pub fn holds_for_overrides(&self, overrides: &BTreeMap<String, ParameterValue>) -> bool {
        match self {
            Self::IsSet { parameter } => overrides.contains_key(parameter),
            Self::Equals { parameter, value } => overrides.get(parameter) == Some(value),
            Self::Not { predicate } => {
                matches!(**predicate, Self::IsSet { .. } | Self::Equals { .. })
                    && !predicate.holds_for_overrides(overrides)
            }
            Self::All { predicates } => {
                !predicates.is_empty()
                    && predicates
                        .iter()
                        .all(|predicate| predicate.holds_for_overrides(overrides))
            }
            Self::Always | Self::Never | Self::Any { .. } => false,
        }
    }
}

/// Whether `when` names only values a profile set: an `IsSet` or `Equals`,
/// or a conjunction of at least one of them with any number of `Not` of an
/// `IsSet`, or of an `Equals` on a parameter the conjunction names, so it
/// never holds of a profile that set none of the values it names.
fn names_only_set_values(when: &Predicate) -> bool {
    match when {
        Predicate::IsSet { .. } | Predicate::Equals { .. } => true,
        Predicate::All { predicates } => {
            let named = predicates
                .iter()
                .filter_map(|predicate| match predicate {
                    Predicate::IsSet { parameter } | Predicate::Equals { parameter, .. } => {
                        Some(parameter)
                    }
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            !named.is_empty()
                && predicates.iter().all(|predicate| match predicate {
                    Predicate::Not { predicate } => match &**predicate {
                        Predicate::IsSet { .. } => true,
                        Predicate::Equals { parameter, .. } => named.contains(parameter),
                        _ => false,
                    },
                    other => names_only_set_values(other),
                })
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
                 is_set or equals, or all of at least one of them with any not(is_set) \
                 and not(equals) of a parameter the conjunction names",
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

    fn unset(parameter: &str) -> Predicate {
        Predicate::Not {
            predicate: Box::new(Predicate::IsSet {
                parameter: parameter.into(),
            }),
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
            unset("mode"),
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
            Predicate::Not {
                predicate: Box::new(equals("timedev", "none")),
            },
            // An unset parameter alone names no explicit value.
            Predicate::All {
                predicates: vec![unset("smodel")],
            },
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
        assert!(names_only_set_values(&explicit_threshold()));
    }

    #[test]
    fn a_condition_may_require_a_parameter_left_unset_beside_an_explicit_value() {
        // The old gaincal's explicit point model meant 1 Jy when smodel was
        // omitted, and the explicit smodel otherwise.
        let point = Predicate::All {
            predicates: vec![equals("gain_model_source", "point"), unset("smodel")],
        };
        assert!(names_only_set_values(&point));
        let mut overrides = BTreeMap::from([(
            "gain_model_source".to_string(),
            ParameterValue::String("point".into()),
        )]);
        assert!(point.holds_for_overrides(&overrides));
        overrides.insert("smodel".into(), ParameterValue::Array(Vec::new()));
        assert!(!point.holds_for_overrides(&overrides));
        assert!(!point.holds_for_overrides(&BTreeMap::new()));
    }

    /// flagdata's old `timedev` set to a threshold, not its old absence.
    fn explicit_threshold() -> Predicate {
        Predicate::All {
            predicates: vec![
                Predicate::IsSet {
                    parameter: "timedev".into(),
                },
                Predicate::Not {
                    predicate: Box::new(equals("timedev", "none")),
                },
            ],
        }
    }

    #[test]
    fn a_value_set_may_be_narrowed_to_differ_from_one_value() {
        let threshold = explicit_threshold();
        let profile = |value: Option<ParameterValue>| {
            value
                .map(|value| BTreeMap::from([("timedev".to_string(), value)]))
                .unwrap_or_default()
        };
        assert!(!threshold.holds_for_overrides(&profile(None)));
        assert!(
            !threshold.holds_for_overrides(&profile(Some(ParameterValue::String("none".into()))))
        );
        assert!(threshold.holds_for_overrides(&profile(Some(ParameterValue::Float(3.5)))));
    }
}
