// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's cInt/cFloat/cIntVec treat Python's bool as a numeric subclass.

use casa_contracts::ParameterValue;

pub(super) fn boolean_as_integer(value: &ParameterValue) -> ParameterValue {
    match value {
        ParameterValue::Bool(value) => ParameterValue::Integer(i64::from(*value)),
        ParameterValue::Array(values) => {
            ParameterValue::Array(values.iter().map(boolean_as_integer).collect())
        }
        // NumPy bool is not a Python int; preserve its type for validation.
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_contracts::{NumpyScalarType, builtin_surface_catalog};

    #[test]
    fn casa_numeric_bool_coercion_is_opt_in_and_keeps_numpy_bool_distinct() {
        let catalog = builtin_surface_catalog().unwrap();
        let mut concept = catalog
            .catalog
            .concepts
            .iter()
            .find(|c| c.id.as_str() == "scaling.fitorder" && c.semantic_revision.0 == 2)
            .unwrap()
            .clone();
        assert_eq!(
            super::super::normalize_value(&ParameterValue::Bool(true), &concept).unwrap(),
            ParameterValue::Integer(1)
        );
        let numpy = ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Bool,
            value: Box::new(ParameterValue::Bool(true)),
        };
        assert!(super::super::normalize_value(&numpy, &concept).is_err());
        concept.normalization = casa_contracts::NormalizationRule::Identity;
        assert!(super::super::normalize_value(&ParameterValue::Bool(true), &concept).is_err());
        assert_eq!(
            boolean_as_integer(&ParameterValue::Array(vec![ParameterValue::Bool(false)])),
            ParameterValue::Array(vec![ParameterValue::Integer(0)])
        );
    }
}
