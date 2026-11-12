// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA typed scalar outcomes behind selected simulation rows (#1053).

use casa_tables::taql::ast::{IndexStyle, Statement};
use casa_tables::taql::eval::{EvalContext, ExprValue, eval_expr};
use casa_types::RecordValue;
use serde_json::Value;

#[test]
fn predicate_arithmetic_matches_independent_casa_types_and_values() {
    let fixture: Value =
        serde_json::from_str(include_str!("data/casa_simulation_query_arithmetic.json")).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 26);
    let row = RecordValue::default();
    for case in fixture["cases"].as_array().unwrap() {
        let expression = case["expression"].as_str().unwrap();
        let Statement::Calc(calc) =
            casa_tables::taql::parse(&format!("CALC {expression}")).unwrap()
        else {
            panic!("CALC prefix")
        };
        let actual = eval_expr(
            &calc.expr,
            &EvalContext {
                row: &row,
                row_index: 0,
                style: IndexStyle::Python,
                measures: None,
            },
        );
        if case.get("error_type").is_some() {
            assert!(actual.is_err(), "CASA rejects {expression}: {actual:?}");
            continue;
        }
        let actual = actual.unwrap_or_else(|error| panic!("{expression}: {error}"));
        let expected = &case["values"][0];
        match (case["type"].as_str().unwrap(), actual) {
            ("int64", ExprValue::Int(value)) => {
                assert_eq!(value, expected.as_i64().unwrap(), "{expression}")
            }
            ("bool", ExprValue::Bool(value)) => {
                assert_eq!(value, expected.as_bool().unwrap(), "{expression}")
            }
            ("float64", ExprValue::Float(value)) => {
                if let Some(nonfinite) = expected["nonfinite"].as_str() {
                    assert!(
                        match nonfinite {
                            "nan" => value.is_nan(),
                            "+inf" => value == f64::INFINITY,
                            "-inf" => value == f64::NEG_INFINITY,
                            _ => panic!("nonfinite kind"),
                        },
                        "{expression}: {value}"
                    );
                } else {
                    assert_eq!(value, expected.as_f64().unwrap(), "{expression}");
                }
            }
            (kind, value) => panic!("{expression}: expected CASA {kind}, got {value:?}"),
        }
    }
}
