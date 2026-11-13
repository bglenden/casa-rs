// SPDX-License-Identifier: LGPL-3.0-or-later
//! Vector literals and scalar broadcasting use the scalar TaQL operators.
use super::{ArrayValue, EvalContext, Expr, ExprValue, TaqlError, eval_binary, eval_expr};
use num_complex::Complex64;

pub(super) fn literal(values: &[Expr], ctx: &EvalContext<'_>) -> Result<ExprValue, TaqlError> {
    let mut data = values
        .iter()
        .map(|value| eval_expr(value, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    if data
        .iter()
        .any(|value| matches!(value, ExprValue::Array(_)))
    {
        return Err(TaqlError::Unsupported {
            message: "nested vector literals".into(),
        });
    }
    if data
        .iter()
        .any(|value| matches!(value, ExprValue::Complex(_)))
    {
        for value in &mut data {
            *value = ExprValue::Complex(match value {
                ExprValue::Complex(value) => *value,
                ExprValue::Float(value) => Complex64::new(*value, 0.),
                ExprValue::Int(value) => Complex64::new(*value as f64, 0.),
                _ => {
                    return Err(TaqlError::TypeError {
                        message: "incompatible vector element types".into(),
                    });
                }
            });
        }
    } else if data
        .iter()
        .any(|value| matches!(value, ExprValue::Float(_)))
    {
        for value in &mut data {
            *value = ExprValue::Float(value.to_float()?);
        }
    } else if let Some(first) = data.first()
        && data
            .iter()
            .any(|value| std::mem::discriminant(value) != std::mem::discriminant(first))
    {
        return Err(TaqlError::TypeError {
            message: "incompatible vector element types".into(),
        });
    }
    Ok(ExprValue::Array(ArrayValue {
        shape: vec![data.len()],
        data,
    }))
}

pub(super) fn binary(
    op: super::BinaryOp,
    lhs: &ExprValue,
    rhs: &ExprValue,
) -> Option<Result<ExprValue, TaqlError>> {
    let (left, right) = (
        if let ExprValue::Array(value) = lhs {
            Some(value)
        } else {
            None
        },
        if let ExprValue::Array(value) = rhs {
            Some(value)
        } else {
            None
        },
    );
    let array = left.or(right)?;
    if left
        .zip(right)
        .is_some_and(|(left, right)| left.shape != right.shape)
    {
        return Some(Err(TaqlError::TypeError {
            message: "array operand shapes differ".into(),
        }));
    }
    let values = (0..array.data.len())
        .map(|index| {
            eval_binary(
                op,
                left.map_or(lhs, |array| &array.data[index]).clone(),
                right.map_or(rhs, |array| &array.data[index]).clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>();
    Some(values.map(|data| {
        ExprValue::Array(ArrayValue {
            shape: array.shape.clone(),
            data,
        })
    }))
}
