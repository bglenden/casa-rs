// SPDX-License-Identifier: LGPL-3.0-or-later
//! Expression evaluator for TaQL.
//!
//! Evaluates [`Expr`] nodes against table row data,
//! producing [`ExprValue`] results with automatic type promotion.
//!
//! # Type promotion hierarchy
//!
//! Matches C++ TaQL: `Bool → Int → Float → Complex`.
//! All integer types widen to `i64`, all floats to `f64`.
//!
//! # C++ reference
//!
//! `TableExprNode.cc`, `TableExprNodeBinary.cc`.

use std::cmp::Ordering;
use std::fmt;

use casacore_types::{RecordValue, ScalarValue, Value};
use num_complex::Complex64;

use super::ast::*;
use super::error::TaqlError;

/// A dynamically typed value produced by expression evaluation.
///
/// This is the runtime representation of values in the TaQL evaluator.
/// Type promotion follows the C++ TaQL hierarchy: Bool → Int → Float → Complex.
///
/// Additional variants:
/// - `DateTime` stores Modified Julian Date (MJD) as f64, matching C++ casacore.
/// - `Array` stores a flat vector of homogeneous values with an n-dimensional shape.
#[derive(Debug, Clone)]
pub enum ExprValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Complex(Complex64),
    String(String),
    /// Modified Julian Date stored as fractional days (MJD = JD − 2_400_000.5).
    DateTime(f64),
    /// N-dimensional array with shape and flat data (row-major order).
    Array(ArrayValue),
    Null,
}

/// A dynamically-typed array value.
///
/// Elements are stored in row-major (C) order. `shape` gives the extent
/// of each axis; the product of shape elements equals `data.len()`.
///
/// C++ equivalent: `Array<T>` / `MaskedArray<T>` in `TableExprNode`.
#[derive(Debug, Clone)]
pub struct ArrayValue {
    /// Extent of each dimension (innermost-last, row-major).
    pub shape: Vec<usize>,
    /// Flat element storage in row-major order.
    pub data: Vec<ExprValue>,
}

impl PartialEq for ExprValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::Complex(a), Self::Complex(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::DateTime(a), Self::DateTime(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a.shape == b.shape && a.data == b.data,
            (Self::Null, Self::Null) => true,
            // Cross-type equality after promotion
            _ => {
                if let (Some(a), Some(b)) = (self.as_promoted(), other.as_promoted()) {
                    match (a, b) {
                        (ExprValue::Float(a), ExprValue::Float(b)) => a == b,
                        (ExprValue::Complex(a), ExprValue::Complex(b)) => a == b,
                        _ => false,
                    }
                } else {
                    false
                }
            }
        }
    }
}

impl fmt::Display for ExprValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool(b) => write!(f, "{b}"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Complex(c) => write!(f, "({} + {}i)", c.re, c.im),
            Self::String(s) => write!(f, "{s}"),
            Self::DateTime(mjd) => write!(f, "MJD({mjd})"),
            Self::Array(arr) => {
                write!(f, "Array[")?;
                for (i, v) in arr.data.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Self::Null => write!(f, "NULL"),
        }
    }
}

impl ExprValue {
    /// Returns a type-name string for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "Bool",
            Self::Int(_) => "Int",
            Self::Float(_) => "Float",
            Self::Complex(_) => "Complex",
            Self::String(_) => "String",
            Self::DateTime(_) => "DateTime",
            Self::Array(_) => "Array",
            Self::Null => "Null",
        }
    }

    /// Returns true if this value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Converts to bool for WHERE/HAVING conditions.
    pub fn to_bool(&self) -> Result<bool, TaqlError> {
        match self {
            Self::Bool(b) => Ok(*b),
            Self::Int(n) => Ok(*n != 0),
            Self::Null => Ok(false),
            other => Err(TaqlError::TypeError {
                message: format!(
                    "cannot convert {type_name} to Bool",
                    type_name = other.type_name()
                ),
            }),
        }
    }

    /// Converts to i64 for integer contexts.
    pub fn to_int(&self) -> Result<i64, TaqlError> {
        match self {
            Self::Bool(b) => Ok(if *b { 1 } else { 0 }),
            Self::Int(n) => Ok(*n),
            Self::Float(v) => Ok(*v as i64),
            other => Err(TaqlError::TypeError {
                message: format!(
                    "cannot convert {type_name} to Int",
                    type_name = other.type_name()
                ),
            }),
        }
    }

    /// Converts to f64 for float contexts.
    pub fn to_float(&self) -> Result<f64, TaqlError> {
        match self {
            Self::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            Self::Int(n) => Ok(*n as f64),
            Self::Float(v) => Ok(*v),
            Self::DateTime(mjd) => Ok(*mjd),
            other => Err(TaqlError::TypeError {
                message: format!(
                    "cannot convert {type_name} to Float",
                    type_name = other.type_name()
                ),
            }),
        }
    }

    /// Numeric type rank for promotion: Bool(0) < Int(1) < Float(2) < Complex(3).
    fn numeric_rank(&self) -> Option<u8> {
        match self {
            Self::Bool(_) => Some(0),
            Self::Int(_) => Some(1),
            Self::Float(_) => Some(2),
            Self::Complex(_) => Some(3),
            _ => None,
        }
    }

    /// Promote this value to the given rank.
    fn promote_to(&self, rank: u8) -> Result<ExprValue, TaqlError> {
        match rank {
            0 => match self {
                Self::Bool(b) => Ok(ExprValue::Bool(*b)),
                _ => Err(TaqlError::TypeError {
                    message: format!("cannot demote {} to Bool", self.type_name()),
                }),
            },
            1 => match self {
                Self::Bool(b) => Ok(ExprValue::Int(if *b { 1 } else { 0 })),
                Self::Int(n) => Ok(ExprValue::Int(*n)),
                _ => Err(TaqlError::TypeError {
                    message: format!("cannot demote {} to Int", self.type_name()),
                }),
            },
            2 => match self {
                Self::Bool(b) => Ok(ExprValue::Float(if *b { 1.0 } else { 0.0 })),
                Self::Int(n) => Ok(ExprValue::Float(*n as f64)),
                Self::Float(v) => Ok(ExprValue::Float(*v)),
                _ => Err(TaqlError::TypeError {
                    message: format!("cannot demote {} to Float", self.type_name()),
                }),
            },
            3 => match self {
                Self::Bool(b) => Ok(ExprValue::Complex(Complex64::new(
                    if *b { 1.0 } else { 0.0 },
                    0.0,
                ))),
                Self::Int(n) => Ok(ExprValue::Complex(Complex64::new(*n as f64, 0.0))),
                Self::Float(v) => Ok(ExprValue::Complex(Complex64::new(*v, 0.0))),
                Self::Complex(c) => Ok(ExprValue::Complex(*c)),
                _ => Err(TaqlError::TypeError {
                    message: format!("cannot convert {} to Complex", self.type_name()),
                }),
            },
            _ => Err(TaqlError::TypeError {
                message: format!("unknown type rank {rank}"),
            }),
        }
    }

    /// Promotes this value to Float if it's numeric, otherwise returns self unchanged.
    fn as_promoted(&self) -> Option<ExprValue> {
        match self {
            Self::Bool(b) => Some(ExprValue::Float(if *b { 1.0 } else { 0.0 })),
            Self::Int(n) => Some(ExprValue::Float(*n as f64)),
            Self::Float(v) => Some(ExprValue::Float(*v)),
            Self::Complex(c) => Some(ExprValue::Complex(*c)),
            _ => None,
        }
    }

    /// Compare two values for ordering (total_cmp for floats, NaN-safe).
    pub fn compare(&self, other: &ExprValue) -> Result<Ordering, TaqlError> {
        // NULL propagation: NULL is considered less than everything else
        match (self.is_null(), other.is_null()) {
            (true, true) => return Ok(Ordering::Equal),
            (true, false) => return Ok(Ordering::Less),
            (false, true) => return Ok(Ordering::Greater),
            _ => {}
        }

        // Same type fast path
        match (self, other) {
            (ExprValue::Bool(a), ExprValue::Bool(b)) => return Ok(a.cmp(b)),
            (ExprValue::Int(a), ExprValue::Int(b)) => return Ok(a.cmp(b)),
            (ExprValue::Float(a), ExprValue::Float(b)) => return Ok(a.total_cmp(b)),
            (ExprValue::String(a), ExprValue::String(b)) => return Ok(a.cmp(b)),
            (ExprValue::DateTime(a), ExprValue::DateTime(b)) => return Ok(a.total_cmp(b)),
            _ => {}
        }

        // Cross-type numeric promotion
        let ar = self.numeric_rank();
        let br = other.numeric_rank();
        if let (Some(ar), Some(br)) = (ar, br) {
            let target = ar.max(br);
            let a = self.promote_to(target)?;
            let b = other.promote_to(target)?;
            match (&a, &b) {
                (ExprValue::Int(x), ExprValue::Int(y)) => return Ok(x.cmp(y)),
                (ExprValue::Float(x), ExprValue::Float(y)) => return Ok(x.total_cmp(y)),
                _ => {}
            }
        }

        Err(TaqlError::TypeError {
            message: format!(
                "cannot compare {} with {}",
                self.type_name(),
                other.type_name()
            ),
        })
    }
}

/// Promote two values to a common numeric type.
pub fn promote(a: ExprValue, b: ExprValue) -> Result<(ExprValue, ExprValue), TaqlError> {
    let ar = a.numeric_rank();
    let br = b.numeric_rank();
    match (ar, br) {
        (Some(ar), Some(br)) => {
            let target = ar.max(br);
            Ok((a.promote_to(target)?, b.promote_to(target)?))
        }
        _ => Err(TaqlError::TypeError {
            message: format!(
                "cannot perform arithmetic on {} and {}",
                a.type_name(),
                b.type_name()
            ),
        }),
    }
}

/// Convert a ScalarValue to an ExprValue.
impl From<&ScalarValue> for ExprValue {
    fn from(sv: &ScalarValue) -> Self {
        match sv {
            ScalarValue::Bool(b) => ExprValue::Bool(*b),
            ScalarValue::UInt8(n) => ExprValue::Int(*n as i64),
            ScalarValue::UInt16(n) => ExprValue::Int(*n as i64),
            ScalarValue::UInt32(n) => ExprValue::Int(*n as i64),
            ScalarValue::Int16(n) => ExprValue::Int(*n as i64),
            ScalarValue::Int32(n) => ExprValue::Int(*n as i64),
            ScalarValue::Int64(n) => ExprValue::Int(*n),
            ScalarValue::Float32(v) => ExprValue::Float(*v as f64),
            ScalarValue::Float64(v) => ExprValue::Float(*v),
            ScalarValue::Complex32(c) => {
                ExprValue::Complex(Complex64::new(c.re as f64, c.im as f64))
            }
            ScalarValue::Complex64(c) => ExprValue::Complex(*c),
            ScalarValue::String(s) => ExprValue::String(s.clone()),
        }
    }
}

/// Convert a Value to an ExprValue.
impl From<&Value> for ExprValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Scalar(sv) => ExprValue::from(sv),
            Value::Array(arr) => array_value_to_expr(arr),
            // Records can't be directly used as ExprValues
            _ => ExprValue::Null,
        }
    }
}

/// Convert a casacore `ArrayValue` (typed ndarray) into an `ExprValue::Array`.
fn array_value_to_expr(arr: &casacore_types::ArrayValue) -> ExprValue {
    use casacore_types::ArrayValue as AV;
    let shape: Vec<usize> = arr.shape().to_vec();
    let data: Vec<ExprValue> = match arr {
        AV::Bool(a) => a.iter().map(|v| ExprValue::Bool(*v)).collect(),
        AV::UInt8(a) => a.iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::UInt16(a) => a.iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::UInt32(a) => a.iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::Int16(a) => a.iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::Int32(a) => a.iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::Int64(a) => a.iter().map(|v| ExprValue::Int(*v)).collect(),
        AV::Float32(a) => a.iter().map(|v| ExprValue::Float(*v as f64)).collect(),
        AV::Float64(a) => a.iter().map(|v| ExprValue::Float(*v)).collect(),
        AV::Complex32(a) => a
            .iter()
            .map(|v| ExprValue::Complex(Complex64::new(v.re as f64, v.im as f64)))
            .collect(),
        AV::Complex64(a) => a.iter().map(|v| ExprValue::Complex(*v)).collect(),
        AV::String(a) => a.iter().map(|v| ExprValue::String(v.clone())).collect(),
    };
    ExprValue::Array(ArrayValue { shape, data })
}

/// The evaluation context: provides column values for the current row.
pub struct EvalContext<'a> {
    /// The current row being evaluated.
    pub row: &'a RecordValue,
    /// The 0-based row index in the parent table.
    pub row_index: usize,
}

/// Evaluate an expression against a row context.
pub fn eval_expr(expr: &Expr, ctx: &EvalContext<'_>) -> Result<ExprValue, TaqlError> {
    match expr {
        Expr::Literal(lit) => Ok(eval_literal(lit)),
        Expr::ColumnRef(col_ref) => eval_column_ref(col_ref, ctx),
        Expr::Unary { op, operand } => {
            let val = eval_expr(operand, ctx)?;
            eval_unary(*op, val)
        }
        Expr::Binary { left, op, right } => {
            let lval = eval_expr(left, ctx)?;
            let rval = eval_expr(right, ctx)?;
            eval_binary(*op, lval, rval)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = eval_expr(expr, ctx)?;
            let lo = eval_expr(low, ctx)?;
            let hi = eval_expr(high, ctx)?;
            // NULL propagation
            if val.is_null() || lo.is_null() || hi.is_null() {
                return Ok(ExprValue::Null);
            }
            let ge_low = val.compare(&lo)? != Ordering::Less;
            let le_high = val.compare(&hi)? != Ordering::Greater;
            let result = ge_low && le_high;
            Ok(ExprValue::Bool(if *negated { !result } else { result }))
        }
        Expr::In {
            expr,
            values,
            negated,
        } => {
            let val = eval_expr(expr, ctx)?;
            if val.is_null() {
                return Ok(ExprValue::Null);
            }
            let mut found = false;
            for v in values {
                let item = eval_expr(v, ctx)?;
                if val == item {
                    found = true;
                    break;
                }
            }
            Ok(ExprValue::Bool(if *negated { !found } else { found }))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let val = eval_expr(expr, ctx)?;
            let pat = eval_expr(pattern, ctx)?;
            if val.is_null() || pat.is_null() {
                return Ok(ExprValue::Null);
            }
            match (&val, &pat) {
                (ExprValue::String(s), ExprValue::String(p)) => {
                    let result = if *case_insensitive {
                        like_match(&s.to_lowercase(), &p.to_lowercase())
                    } else {
                        like_match(s, p)
                    };
                    Ok(ExprValue::Bool(if *negated { !result } else { result }))
                }
                _ => Err(TaqlError::TypeError {
                    message: format!(
                        "LIKE requires String operands, got {} and {}",
                        val.type_name(),
                        pat.type_name()
                    ),
                }),
            }
        }
        Expr::IsNull { expr, negated } => {
            let val = eval_expr(expr, ctx)?;
            let is_null = val.is_null();
            Ok(ExprValue::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::Star => Ok(ExprValue::Null), // * in expression context is unusual
        Expr::RowNumber => Ok(ExprValue::Int(ctx.row_index as i64)),
        Expr::FunctionCall { name, args } => {
            let evaluated_args: Vec<ExprValue> = args
                .iter()
                .map(|a| eval_expr(a, ctx))
                .collect::<Result<_, _>>()?;
            super::functions::call_function(name, &evaluated_args, ctx)
        }
        Expr::Aggregate { .. } => {
            // Aggregates are handled at the execution level, not per-row evaluation.
            Err(TaqlError::TypeError {
                message: "aggregate functions cannot be evaluated per-row".to_string(),
            })
        }
    }
}

fn eval_literal(lit: &Literal) -> ExprValue {
    match lit {
        Literal::Int(n) => ExprValue::Int(*n),
        Literal::Float(v) => ExprValue::Float(*v),
        Literal::String(s) => ExprValue::String(s.clone()),
        Literal::Bool(b) => ExprValue::Bool(*b),
        Literal::Complex(c) => ExprValue::Complex(*c),
        Literal::Null => ExprValue::Null,
    }
}

fn eval_column_ref(col_ref: &ColumnRef, ctx: &EvalContext<'_>) -> Result<ExprValue, TaqlError> {
    // For now, ignore table qualifier — will be used in JOIN evaluation.
    let val = ctx.row.get(&col_ref.column);
    match val {
        Some(v) => Ok(ExprValue::from(v)),
        None => Err(TaqlError::ColumnNotFound {
            name: col_ref.to_string(),
        }),
    }
}

fn eval_unary(op: UnaryOp, val: ExprValue) -> Result<ExprValue, TaqlError> {
    if val.is_null() {
        return Ok(ExprValue::Null);
    }
    match op {
        UnaryOp::Negate => match val {
            ExprValue::Int(n) => Ok(ExprValue::Int(-n)),
            ExprValue::Float(v) => Ok(ExprValue::Float(-v)),
            ExprValue::Complex(c) => Ok(ExprValue::Complex(-c)),
            other => Err(TaqlError::TypeError {
                message: format!("cannot negate {}", other.type_name()),
            }),
        },
        UnaryOp::Not => {
            let b = val.to_bool()?;
            Ok(ExprValue::Bool(!b))
        }
        UnaryOp::BitNot => match val {
            ExprValue::Int(n) => Ok(ExprValue::Int(!n)),
            ExprValue::Bool(b) => Ok(ExprValue::Bool(!b)),
            other => Err(TaqlError::TypeError {
                message: format!("cannot apply bitwise NOT to {}", other.type_name()),
            }),
        },
    }
}

fn eval_binary(op: BinaryOp, lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    // NULL propagation for all operators
    if lhs.is_null() || rhs.is_null() {
        return Ok(ExprValue::Null);
    }

    match op {
        BinaryOp::And => {
            let a = lhs.to_bool()?;
            let b = rhs.to_bool()?;
            Ok(ExprValue::Bool(a && b))
        }
        BinaryOp::Or => {
            let a = lhs.to_bool()?;
            let b = rhs.to_bool()?;
            Ok(ExprValue::Bool(a || b))
        }
        BinaryOp::Eq => Ok(ExprValue::Bool(lhs == rhs)),
        BinaryOp::Ne => Ok(ExprValue::Bool(lhs != rhs)),
        BinaryOp::Lt => Ok(ExprValue::Bool(lhs.compare(&rhs)? == Ordering::Less)),
        BinaryOp::Le => Ok(ExprValue::Bool(lhs.compare(&rhs)? != Ordering::Greater)),
        BinaryOp::Gt => Ok(ExprValue::Bool(lhs.compare(&rhs)? == Ordering::Greater)),
        BinaryOp::Ge => Ok(ExprValue::Bool(lhs.compare(&rhs)? != Ordering::Less)),
        BinaryOp::Add => eval_arithmetic_add(lhs, rhs),
        BinaryOp::Sub => eval_arithmetic_sub(lhs, rhs),
        BinaryOp::Mul => eval_arithmetic_mul(lhs, rhs),
        BinaryOp::Div => eval_arithmetic_div(lhs, rhs),
        BinaryOp::IntDiv => eval_int_div(lhs, rhs),
        BinaryOp::Modulo => eval_modulo(lhs, rhs),
        BinaryOp::Power => eval_power(lhs, rhs),
    }
}

fn eval_arithmetic_add(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    // String concatenation
    if let (ExprValue::String(a), ExprValue::String(b)) = (&lhs, &rhs) {
        return Ok(ExprValue::String(format!("{a}{b}")));
    }
    let (a, b) = promote(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_add(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a + b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a + b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_sub(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = promote(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_sub(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a - b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a - b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_mul(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = promote(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_mul(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a * b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a * b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_div(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = promote(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => {
            if b == 0 {
                return Err(TaqlError::DivisionByZero);
            }
            // Integer division in TaQL: Int / Int -> Int (truncating)
            Ok(ExprValue::Int(a / b))
        }
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a / b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a / b)),
        _ => unreachable!(),
    }
}

fn eval_int_div(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let a = lhs.to_float()?;
    let b = rhs.to_float()?;
    if b == 0.0 {
        return Err(TaqlError::DivisionByZero);
    }
    Ok(ExprValue::Int((a / b).trunc() as i64))
}

fn eval_modulo(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = promote(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => {
            if b == 0 {
                return Err(TaqlError::DivisionByZero);
            }
            Ok(ExprValue::Int(a % b))
        }
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a % b)),
        _ => Err(TaqlError::TypeError {
            message: "modulo not supported for complex numbers".to_string(),
        }),
    }
}

fn eval_power(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = promote(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => {
            if b >= 0 && b <= u32::MAX as i64 {
                Ok(ExprValue::Int(a.wrapping_pow(b as u32)))
            } else {
                Ok(ExprValue::Float((a as f64).powf(b as f64)))
            }
        }
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a.powf(b))),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => {
            // Complex power: a^b = exp(b * ln(a))
            let result = if b.im == 0.0 {
                a.powf(b.re)
            } else {
                (b * a.ln()).exp()
            };
            Ok(ExprValue::Complex(result))
        }
        _ => unreachable!(),
    }
}

/// SQL LIKE pattern matching with `%` (any chars) and `_` (single char).
///
/// This is a hand-written matcher — no regex crate needed.
pub fn like_match(text: &str, pattern: &str) -> bool {
    like_match_recursive(text.as_bytes(), pattern.as_bytes())
}

fn like_match_recursive(text: &[u8], pattern: &[u8]) -> bool {
    let mut ti = 0;
    let mut pi = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == b'_' {
            // _ matches any single character
            ti += 1;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == b'%' {
            // % matches any sequence — remember this position
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == text[ti] {
            ti += 1;
            pi += 1;
        } else if star_pi != usize::MAX {
            // Backtrack: advance the text position after the last %
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    // Skip trailing % in pattern
    while pi < pattern.len() && pattern[pi] == b'%' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_promotion_int_float() {
        let (a, b) = promote(ExprValue::Int(2), ExprValue::Float(3.0)).unwrap();
        match (a, b) {
            (ExprValue::Float(a), ExprValue::Float(b)) => {
                assert_eq!(a, 2.0);
                assert_eq!(b, 3.0);
            }
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn type_promotion_bool_int() {
        let (a, b) = promote(ExprValue::Bool(true), ExprValue::Int(5)).unwrap();
        match (a, b) {
            (ExprValue::Int(a), ExprValue::Int(b)) => {
                assert_eq!(a, 1);
                assert_eq!(b, 5);
            }
            _ => panic!("expected Int"),
        }
    }

    #[test]
    fn add_int_float() {
        let result = eval_binary(BinaryOp::Add, ExprValue::Int(2), ExprValue::Float(3.0)).unwrap();
        assert_eq!(result, ExprValue::Float(5.0));
    }

    #[test]
    fn integer_division() {
        let result = eval_binary(BinaryOp::Div, ExprValue::Int(7), ExprValue::Int(2)).unwrap();
        assert_eq!(result, ExprValue::Int(3));
    }

    #[test]
    fn complex_arithmetic() {
        let a = ExprValue::Complex(Complex64::new(1.0, 2.0));
        let b = ExprValue::Complex(Complex64::new(3.0, 4.0));
        let result = eval_binary(BinaryOp::Add, a, b).unwrap();
        assert_eq!(result, ExprValue::Complex(Complex64::new(4.0, 6.0)));
    }

    #[test]
    fn comparison_across_types() {
        let a = ExprValue::Int(1);
        let b = ExprValue::Float(2.0);
        let result = eval_binary(BinaryOp::Lt, a, b).unwrap();
        assert_eq!(result, ExprValue::Bool(true));
    }

    #[test]
    fn like_basic() {
        assert!(like_match("hello", "h%"));
        assert!(like_match("hello", "h_llo"));
        assert!(like_match("hello", "%llo"));
        assert!(like_match("hello", "hello"));
        assert!(!like_match("hello", "world"));
        assert!(like_match("hello", "%"));
        assert!(like_match("", "%"));
        assert!(!like_match("", "_"));
    }

    #[test]
    fn like_case_insensitive() {
        // ILIKE is handled by lowercasing both strings before matching
        assert!(like_match(&"Hello".to_lowercase(), &"hello".to_lowercase()));
        assert!(like_match(
            &"HELLO WORLD".to_lowercase(),
            &"hello%".to_lowercase()
        ));
    }

    #[test]
    fn null_propagation() {
        let result = eval_binary(BinaryOp::Add, ExprValue::Null, ExprValue::Int(1)).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn null_propagation_compare() {
        let result = eval_binary(BinaryOp::Eq, ExprValue::Null, ExprValue::Int(1)).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn unary_negate() {
        let result = eval_unary(UnaryOp::Negate, ExprValue::Int(5)).unwrap();
        assert_eq!(result, ExprValue::Int(-5));
    }

    #[test]
    fn unary_not() {
        let result = eval_unary(UnaryOp::Not, ExprValue::Bool(true)).unwrap();
        assert_eq!(result, ExprValue::Bool(false));
    }

    #[test]
    fn division_by_zero() {
        let result = eval_binary(BinaryOp::Div, ExprValue::Int(1), ExprValue::Int(0));
        assert!(matches!(result, Err(TaqlError::DivisionByZero)));
    }

    #[test]
    fn power_int() {
        let result = eval_binary(BinaryOp::Power, ExprValue::Int(2), ExprValue::Int(10)).unwrap();
        assert_eq!(result, ExprValue::Int(1024));
    }

    #[test]
    fn power_float() {
        let result = eval_binary(
            BinaryOp::Power,
            ExprValue::Float(2.0),
            ExprValue::Float(0.5),
        )
        .unwrap();
        match result {
            ExprValue::Float(v) => assert!((v - std::f64::consts::SQRT_2).abs() < 1e-10),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn modulo_int() {
        let result = eval_binary(BinaryOp::Modulo, ExprValue::Int(7), ExprValue::Int(3)).unwrap();
        assert_eq!(result, ExprValue::Int(1));
    }

    #[test]
    fn string_concat() {
        let result = eval_binary(
            BinaryOp::Add,
            ExprValue::String("hello".to_string()),
            ExprValue::String(" world".to_string()),
        )
        .unwrap();
        assert_eq!(result, ExprValue::String("hello world".to_string()));
    }

    #[test]
    fn eval_scalar_value_conversion() {
        assert_eq!(ExprValue::from(&ScalarValue::Int32(42)), ExprValue::Int(42));
        assert_eq!(
            ExprValue::from(&ScalarValue::Float64(3.15)),
            ExprValue::Float(3.15)
        );
        assert_eq!(
            ExprValue::from(&ScalarValue::String("test".to_string())),
            ExprValue::String("test".to_string())
        );
        assert_eq!(
            ExprValue::from(&ScalarValue::Bool(true)),
            ExprValue::Bool(true)
        );
    }

    #[test]
    fn eval_expression_against_row() {
        // Build a row: {flux: 2.78, id: 5}
        let row = RecordValue::new(vec![
            casacore_types::RecordField::new("flux", Value::Scalar(ScalarValue::Float64(3.15))),
            casacore_types::RecordField::new("id", Value::Scalar(ScalarValue::Int32(5))),
        ]);
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };

        // Evaluate: flux * 2.0
        let expr = Expr::Binary {
            left: Box::new(Expr::ColumnRef(ColumnRef {
                table: None,
                column: "flux".to_string(),
            })),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Literal(Literal::Float(2.0))),
        };
        let result = eval_expr(&expr, &ctx).unwrap();
        assert_eq!(result, ExprValue::Float(6.30));
    }

    #[test]
    fn eval_row_number() {
        let row = RecordValue::new(vec![]);
        let ctx = EvalContext {
            row: &row,
            row_index: 42,
        };
        let result = eval_expr(&Expr::RowNumber, &ctx).unwrap();
        assert_eq!(result, ExprValue::Int(42));
    }

    #[test]
    fn like_complex_patterns() {
        assert!(like_match("abcdef", "a%d%f"));
        assert!(like_match("abcdef", "a_c_e_"));
        assert!(!like_match("abcdef", "a_c_e"));
        assert!(like_match("abc", "a%%c"));
    }

    // ── ExprValue type conversion tests ──

    #[test]
    fn type_name_all_variants() {
        assert_eq!(ExprValue::Bool(true).type_name(), "Bool");
        assert_eq!(ExprValue::Int(0).type_name(), "Int");
        assert_eq!(ExprValue::Float(0.0).type_name(), "Float");
        assert_eq!(
            ExprValue::Complex(Complex64::new(0.0, 0.0)).type_name(),
            "Complex"
        );
        assert_eq!(ExprValue::String("".into()).type_name(), "String");
        assert_eq!(ExprValue::DateTime(0.0).type_name(), "DateTime");
        assert_eq!(
            ExprValue::Array(ArrayValue {
                shape: vec![],
                data: vec![]
            })
            .type_name(),
            "Array"
        );
        assert_eq!(ExprValue::Null.type_name(), "Null");
    }

    #[test]
    fn to_bool_conversions() {
        assert!(ExprValue::Bool(true).to_bool().unwrap());
        assert!(!ExprValue::Bool(false).to_bool().unwrap());
        assert!(ExprValue::Int(1).to_bool().unwrap());
        assert!(!ExprValue::Int(0).to_bool().unwrap());
        assert!(!ExprValue::Null.to_bool().unwrap());
        assert!(ExprValue::Float(1.0).to_bool().is_err());
    }

    #[test]
    fn to_int_conversions() {
        assert_eq!(ExprValue::Bool(true).to_int().unwrap(), 1);
        assert_eq!(ExprValue::Bool(false).to_int().unwrap(), 0);
        assert_eq!(ExprValue::Int(42).to_int().unwrap(), 42);
        assert_eq!(ExprValue::Float(3.7).to_int().unwrap(), 3);
        assert!(ExprValue::String("x".into()).to_int().is_err());
    }

    #[test]
    fn to_float_conversions() {
        assert_eq!(ExprValue::Bool(true).to_float().unwrap(), 1.0);
        assert_eq!(ExprValue::Int(5).to_float().unwrap(), 5.0);
        assert_eq!(ExprValue::Float(2.5).to_float().unwrap(), 2.5);
        assert_eq!(ExprValue::DateTime(51544.0).to_float().unwrap(), 51544.0);
        assert!(ExprValue::String("x".into()).to_float().is_err());
    }

    #[test]
    fn compare_null_handling() {
        assert_eq!(
            ExprValue::Null.compare(&ExprValue::Null).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            ExprValue::Null.compare(&ExprValue::Int(1)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            ExprValue::Int(1).compare(&ExprValue::Null).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn compare_datetime() {
        let a = ExprValue::DateTime(51544.0);
        let b = ExprValue::DateTime(51545.0);
        assert_eq!(a.compare(&b).unwrap(), Ordering::Less);
        assert_eq!(b.compare(&a).unwrap(), Ordering::Greater);
        assert_eq!(a.compare(&a).unwrap(), Ordering::Equal);
    }

    #[test]
    fn compare_cross_type_bool_float() {
        let a = ExprValue::Bool(true);
        let b = ExprValue::Float(0.5);
        assert_eq!(a.compare(&b).unwrap(), Ordering::Greater);
    }

    #[test]
    fn compare_strings() {
        let a = ExprValue::String("apple".into());
        let b = ExprValue::String("banana".into());
        assert_eq!(a.compare(&b).unwrap(), Ordering::Less);
    }

    #[test]
    fn display_datetime() {
        let dt = ExprValue::DateTime(51544.5);
        let s = format!("{dt}");
        assert!(s.starts_with("MJD("));
    }

    #[test]
    fn display_array() {
        let arr = ExprValue::Array(ArrayValue {
            shape: vec![3],
            data: vec![ExprValue::Int(1), ExprValue::Int(2), ExprValue::Int(3)],
        });
        let s = format!("{arr}");
        assert!(s.starts_with("Array["));
    }

    // ── Promotion tests ──

    #[test]
    fn promote_bool_to_complex() {
        let (a, _) = promote(
            ExprValue::Bool(true),
            ExprValue::Complex(Complex64::new(2.0, 0.0)),
        )
        .unwrap();
        assert!(matches!(a, ExprValue::Complex(_)));
    }

    #[test]
    fn promote_int_to_complex() {
        let (a, _) = promote(
            ExprValue::Int(3),
            ExprValue::Complex(Complex64::new(0.0, 1.0)),
        )
        .unwrap();
        match a {
            ExprValue::Complex(c) => assert_eq!(c.re, 3.0),
            _ => panic!("expected Complex"),
        }
    }

    #[test]
    fn promote_float_to_complex() {
        let (a, _) = promote(
            ExprValue::Float(2.5),
            ExprValue::Complex(Complex64::new(0.0, 0.0)),
        )
        .unwrap();
        match a {
            ExprValue::Complex(c) => assert_eq!(c.re, 2.5),
            _ => panic!("expected Complex"),
        }
    }

    #[test]
    fn promote_string_fails() {
        assert!(promote(ExprValue::String("x".into()), ExprValue::Int(1)).is_err());
    }

    // ── Binary op additional tests ──

    #[test]
    fn string_equality_check() {
        let result = eval_binary(
            BinaryOp::Eq,
            ExprValue::String("a".into()),
            ExprValue::String("a".into()),
        )
        .unwrap();
        assert_eq!(result, ExprValue::Bool(true));
    }

    #[test]
    fn null_propagation_binary() {
        let result = eval_binary(BinaryOp::Add, ExprValue::Null, ExprValue::Int(1)).unwrap();
        assert_eq!(result, ExprValue::Null);
        let result = eval_binary(BinaryOp::Add, ExprValue::Int(1), ExprValue::Null).unwrap();
        assert_eq!(result, ExprValue::Null);
    }

    #[test]
    fn sub_and_mul_float() {
        assert_eq!(
            eval_binary(BinaryOp::Sub, ExprValue::Float(5.0), ExprValue::Float(3.0)).unwrap(),
            ExprValue::Float(2.0)
        );
        assert_eq!(
            eval_binary(BinaryOp::Mul, ExprValue::Float(2.0), ExprValue::Float(3.0)).unwrap(),
            ExprValue::Float(6.0)
        );
    }

    #[test]
    fn compare_ge_le() {
        assert_eq!(
            eval_binary(BinaryOp::Ge, ExprValue::Int(5), ExprValue::Int(3)).unwrap(),
            ExprValue::Bool(true)
        );
        assert_eq!(
            eval_binary(BinaryOp::Le, ExprValue::Int(3), ExprValue::Int(5)).unwrap(),
            ExprValue::Bool(true)
        );
        assert_eq!(
            eval_binary(BinaryOp::Ne, ExprValue::Int(1), ExprValue::Int(2)).unwrap(),
            ExprValue::Bool(true)
        );
    }

    #[test]
    fn bool_logic_and_or() {
        assert_eq!(
            eval_binary(BinaryOp::And, ExprValue::Bool(true), ExprValue::Bool(false)).unwrap(),
            ExprValue::Bool(false)
        );
        assert_eq!(
            eval_binary(BinaryOp::Or, ExprValue::Bool(false), ExprValue::Bool(true)).unwrap(),
            ExprValue::Bool(true)
        );
    }

    // ── eval_expr tests ──

    #[test]
    fn eval_literal_values() {
        let row = casacore_types::RecordValue::default();
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };
        assert_eq!(
            eval_expr(&Expr::Literal(Literal::Int(42)), &ctx).unwrap(),
            ExprValue::Int(42)
        );
        assert_eq!(
            eval_expr(&Expr::Literal(Literal::Float(2.78)), &ctx).unwrap(),
            ExprValue::Float(2.78)
        );
        assert_eq!(
            eval_expr(&Expr::Literal(Literal::String("hi".into())), &ctx).unwrap(),
            ExprValue::String("hi".into())
        );
        assert_eq!(
            eval_expr(&Expr::Literal(Literal::Bool(true)), &ctx).unwrap(),
            ExprValue::Bool(true)
        );
        assert_eq!(
            eval_expr(&Expr::Literal(Literal::Null), &ctx).unwrap(),
            ExprValue::Null
        );
    }

    #[test]
    fn eval_unary_not_expr() {
        let row = casacore_types::RecordValue::default();
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };
        let expr = Expr::Unary {
            op: UnaryOp::Not,
            operand: Box::new(Expr::Literal(Literal::Bool(true))),
        };
        assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(false));
    }

    #[test]
    fn eval_unary_negate_expr() {
        let row = casacore_types::RecordValue::default();
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };
        let expr = Expr::Unary {
            op: UnaryOp::Negate,
            operand: Box::new(Expr::Literal(Literal::Int(5))),
        };
        assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Int(-5));
    }

    #[test]
    fn eval_is_null_expr() {
        let row = casacore_types::RecordValue::default();
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };
        let expr = Expr::IsNull {
            expr: Box::new(Expr::Literal(Literal::Null)),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));

        let not_null = Expr::IsNull {
            expr: Box::new(Expr::Literal(Literal::Int(1))),
            negated: true,
        };
        assert_eq!(eval_expr(&not_null, &ctx).unwrap(), ExprValue::Bool(true));
    }

    #[test]
    fn eval_between_expr() {
        let row = casacore_types::RecordValue::default();
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };
        let expr = Expr::Between {
            expr: Box::new(Expr::Literal(Literal::Int(5))),
            low: Box::new(Expr::Literal(Literal::Int(1))),
            high: Box::new(Expr::Literal(Literal::Int(10))),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));

        let not_between = Expr::Between {
            expr: Box::new(Expr::Literal(Literal::Int(15))),
            low: Box::new(Expr::Literal(Literal::Int(1))),
            high: Box::new(Expr::Literal(Literal::Int(10))),
            negated: false,
        };
        assert_eq!(
            eval_expr(&not_between, &ctx).unwrap(),
            ExprValue::Bool(false)
        );
    }

    #[test]
    fn eval_in_expr() {
        let row = casacore_types::RecordValue::default();
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };
        let expr = Expr::In {
            expr: Box::new(Expr::Literal(Literal::Int(2))),
            values: vec![
                Expr::Literal(Literal::Int(1)),
                Expr::Literal(Literal::Int(2)),
                Expr::Literal(Literal::Int(3)),
            ],
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));

        let not_in = Expr::In {
            expr: Box::new(Expr::Literal(Literal::Int(5))),
            values: vec![Expr::Literal(Literal::Int(1))],
            negated: false,
        };
        assert_eq!(eval_expr(&not_in, &ctx).unwrap(), ExprValue::Bool(false));
    }

    #[test]
    fn eval_binary_expr() {
        let row = casacore_types::RecordValue::default();
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };
        let expr = Expr::Binary {
            left: Box::new(Expr::Literal(Literal::Int(3))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(4))),
        };
        assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Int(7));
    }

    #[test]
    fn eval_function_call_expr() {
        let row = casacore_types::RecordValue::default();
        let ctx = EvalContext {
            row: &row,
            row_index: 0,
        };
        let expr = Expr::FunctionCall {
            name: "abs".to_string(),
            args: vec![Expr::Literal(Literal::Float(-5.0))],
        };
        assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Float(5.0));
    }
}
