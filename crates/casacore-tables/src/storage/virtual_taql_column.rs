// SPDX-License-Identifier: LGPL-3.0-or-later
//! VirtualTaQLColumn engine — computes column values from a TaQL expression.
//!
//! A virtual column whose value is derived by evaluating a TaQL expression
//! against each row's already-loaded stored columns.
//!
//! # C++ equivalent
//!
//! `VirtualTaQLColumn` in `casacore/tables/DataMan/VirtualTaQLColumn.h`.
//!
//! The C++ implementation stores the expression string as a column keyword
//! named `_VirtualTaQLColumn_CalcExpr`.

use casacore_types::{RecordValue, ScalarValue, Value};

use super::StorageError;
use super::virtual_engine::{VirtualColumnEngine, VirtualContext};
use crate::taql;
use crate::taql::eval::{EvalContext, ExprValue, eval_expr};

/// The column keyword name that stores the TaQL expression.
///
/// C++ reference: `VirtualTaQLColumn::theirKeyName`.
const EXPR_KEYWORD: &str = "_VirtualTaQLColumn_CalcExpr";

/// Virtual engine that evaluates a TaQL expression per row.
///
/// The expression is read from the column keyword `_VirtualTaQLColumn_CalcExpr`
/// during materialization. Example: an expression `"flux * 2.0"` would produce
/// a virtual column whose value is twice the `flux` stored column.
///
/// # C++ equivalent
///
/// `VirtualTaQLColumn` in `casacore/tables/DataMan/VirtualTaQLColumn.h`.
#[derive(Debug)]
pub(crate) struct VirtualTaQLColumnEngine;

impl VirtualColumnEngine for VirtualTaQLColumnEngine {
    fn type_name(&self) -> &str {
        "VirtualTaQLColumn"
    }

    fn materialize(
        &self,
        ctx: &VirtualContext,
        bound_cols: &[(usize, &super::table_control::PlainColumnEntry)],
        rows: &mut [RecordValue],
    ) -> Result<(), StorageError> {
        for &(desc_idx, _col_entry) in bound_cols {
            let desc = &ctx.col_descs[desc_idx];
            let col_name = &desc.col_name;

            // Read the expression from column keywords.
            let expr_str = desc
                .keywords
                .fields()
                .iter()
                .find(|f| f.name == EXPR_KEYWORD)
                .and_then(|f| match &f.value {
                    Value::Scalar(ScalarValue::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "VirtualTaQLColumn: column '{col_name}' missing keyword '{EXPR_KEYWORD}'"
                    ))
                })?;

            // Parse the expression.
            let stmt = taql::parse(&format!("CALC {expr_str}")).map_err(|e| {
                StorageError::FormatMismatch(format!(
                    "VirtualTaQLColumn: failed to parse expression '{expr_str}': {e}"
                ))
            })?;

            let expr = match stmt {
                taql::ast::Statement::Calc(c) => c.expr,
                _ => {
                    return Err(StorageError::FormatMismatch(
                        "VirtualTaQLColumn: expression did not parse as CALC".to_string(),
                    ));
                }
            };

            // Evaluate for each row.
            for (i, row) in rows.iter_mut().enumerate() {
                let eval_ctx = EvalContext {
                    row,
                    row_index: i,
                    style: taql::ast::IndexStyle::default(),
                };
                let val = eval_expr(&expr, &eval_ctx).map_err(|e| {
                    StorageError::FormatMismatch(format!(
                        "VirtualTaQLColumn: evaluation error at row {i}: {e}"
                    ))
                })?;

                let table_val = expr_value_to_value(&val);
                row.upsert(col_name, table_val);
            }
        }
        Ok(())
    }
}

/// Convert an ExprValue to a casacore Value.
fn expr_value_to_value(val: &ExprValue) -> Value {
    match val {
        ExprValue::Bool(b) => Value::Scalar(ScalarValue::Bool(*b)),
        ExprValue::Int(n) => Value::Scalar(ScalarValue::Int64(*n)),
        ExprValue::Float(v) => Value::Scalar(ScalarValue::Float64(*v)),
        ExprValue::Complex(c) => Value::Scalar(ScalarValue::Complex64(*c)),
        ExprValue::String(s) => Value::Scalar(ScalarValue::String(s.clone())),
        ExprValue::DateTime(v) => Value::Scalar(ScalarValue::Float64(*v)),
        ExprValue::Array(_) => Value::Scalar(ScalarValue::Bool(false)), // TODO: array columns
        ExprValue::Regex { pattern, .. } => Value::Scalar(ScalarValue::String(pattern.clone())),
        ExprValue::Null => Value::Scalar(ScalarValue::Bool(false)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::data_type::CasacoreDataType;
    use crate::storage::table_control::{ColumnDescContents, PlainColumnEntry};
    use crate::storage::virtual_engine::VirtualContext;
    use casacore_types::{PrimitiveType, RecordField, RecordValue, Value};
    use std::path::Path;

    fn make_col_desc(name: &str, expr: &str) -> ColumnDescContents {
        let mut kw = RecordValue::default();
        kw.upsert(
            EXPR_KEYWORD,
            Value::Scalar(ScalarValue::String(expr.to_string())),
        );
        ColumnDescContents {
            class_name: String::new(),
            col_name: name.to_string(),
            comment: String::new(),
            data_manager_type: "VirtualTaQLColumn".to_string(),
            data_manager_group: "VirtualTaQLColumn".to_string(),
            data_type: CasacoreDataType::TpDouble,
            option: 0,
            nrdim: 0,
            shape: Vec::new(),
            max_length: 0,
            keywords: kw,
            is_array: false,
            primitive_type: Some(PrimitiveType::Float64),
        }
    }

    fn make_plain_col_entry(seq_nr: u32) -> PlainColumnEntry {
        PlainColumnEntry {
            original_name: String::new(),
            dm_seq_nr: seq_nr,
            is_array: false,
        }
    }

    #[test]
    fn eval_simple_expression() {
        let engine = VirtualTaQLColumnEngine;
        let desc = make_col_desc("doubled_flux", "flux * 2.0");
        let entry = make_plain_col_entry(0);

        let mut rows = vec![
            RecordValue::new(vec![RecordField::new(
                "flux",
                Value::Scalar(ScalarValue::Float64(1.5)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "flux",
                Value::Scalar(ScalarValue::Float64(3.0)),
            )]),
        ];

        let ctx = VirtualContext {
            col_descs: &[desc],
            rows: &rows.clone(),
            table_path: Path::new("/tmp/test"),
            nrrow: 2,
        };

        engine.materialize(&ctx, &[(0, &entry)], &mut rows).unwrap();

        // Check that doubled_flux was added
        assert_eq!(
            rows[0].get("doubled_flux"),
            Some(&Value::Scalar(ScalarValue::Float64(3.0)))
        );
        assert_eq!(
            rows[1].get("doubled_flux"),
            Some(&Value::Scalar(ScalarValue::Float64(6.0)))
        );
    }

    #[test]
    fn constant_expression() {
        let engine = VirtualTaQLColumnEngine;
        let desc = make_col_desc("const_col", "42.5");
        let entry = make_plain_col_entry(0);

        let mut rows = vec![RecordValue::default(), RecordValue::default()];

        let ctx = VirtualContext {
            col_descs: &[desc],
            rows: &rows.clone(),
            table_path: Path::new("/tmp/test"),
            nrrow: 2,
        };

        engine.materialize(&ctx, &[(0, &entry)], &mut rows).unwrap();

        for row in &rows {
            assert_eq!(
                row.get("const_col"),
                Some(&Value::Scalar(ScalarValue::Float64(42.5)))
            );
        }
    }

    #[test]
    fn missing_keyword_errors() {
        let engine = VirtualTaQLColumnEngine;
        // Column desc without the expression keyword
        let desc = ColumnDescContents {
            class_name: String::new(),
            col_name: "bad_col".to_string(),
            comment: String::new(),
            data_manager_type: "VirtualTaQLColumn".to_string(),
            data_manager_group: "VirtualTaQLColumn".to_string(),
            data_type: CasacoreDataType::TpDouble,
            option: 0,
            nrdim: 0,
            shape: Vec::new(),
            max_length: 0,
            keywords: RecordValue::default(),
            is_array: false,
            primitive_type: Some(PrimitiveType::Float64),
        };
        let entry = make_plain_col_entry(0);
        let mut rows = vec![RecordValue::default()];

        let ctx = VirtualContext {
            col_descs: &[desc],
            rows: &rows.clone(),
            table_path: Path::new("/tmp/test"),
            nrrow: 1,
        };

        let result = engine.materialize(&ctx, &[(0, &entry)], &mut rows);
        assert!(result.is_err());
    }
}
