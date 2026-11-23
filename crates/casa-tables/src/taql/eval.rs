// SPDX-License-Identifier: LGPL-3.0-or-later
//! Expression evaluator for TaQL.
//!
//! Evaluates [`Expr`] nodes against table row data,
//! producing [`ExprValue`] results with automatic type promotion.
//!
//! # Type promotion hierarchy
//!
//! Arithmetic promotes `Int → Float → Complex`, matching C++ TaQL. Bool
//! requires an explicit conversion before arithmetic.
//! All integer types widen to `i64`, all floats to `f64`.
//!
//! # C++ reference
//!
//! `TableExprNode.cc`, `TableExprNodeBinary.cc`.

use std::cmp::Ordering;
use std::fmt;

use casa_types::quanta::{Quantity, Unit};
use casa_types::{RecordValue, ScalarValue, Value};
use num_complex::Complex64;
#[path = "eval_arrays.rs"]
mod arrays;

use super::ast::{self, *};
use super::error::TaqlError;

/// A dynamically typed value produced by expression evaluation.
///
/// This is the runtime representation of values in the TaQL evaluator.
/// Arithmetic promotes Int → Float → Complex; explicit conversion helpers
/// also accept Bool, which arithmetic operators reject.
///
/// Additional variants:
/// - `DateTime` stores Modified Julian Date (MJD) as f64, matching C++ casacore.
/// - `Array` stores a flat vector of homogeneous values with an n-dimensional shape.
#[derive(Debug, Clone)]
pub enum ExprValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Quantity(Quantity),
    Complex(Complex64),
    String(String),
    /// Modified Julian Date stored as fractional days (MJD = JD − 2_400_000.5).
    DateTime(f64),
    /// N-dimensional array with shape and flat data (row-major order).
    Array(ArrayValue),
    /// A compiled regex pattern for matching.
    Regex {
        pattern: String,
        flags: String,
    },
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
            (Self::Quantity(a), Self::Quantity(b)) => {
                a.unit().conformant(b.unit()) && a.get_si_value() == b.get_si_value()
            }
            (Self::Complex(a), Self::Complex(b)) => a == b,
            (Self::Complex(a), Self::Int(b)) | (Self::Int(b), Self::Complex(a)) => {
                a.im == 0. && a.re == *b as f64
            }
            (Self::Complex(a), Self::Float(b)) | (Self::Float(b), Self::Complex(a)) => {
                a.im == 0. && a.re == *b
            }
            (Self::String(a), Self::String(b)) => a == b,
            (Self::DateTime(a), Self::DateTime(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a.shape == b.shape && a.data == b.data,
            (
                Self::Regex {
                    pattern: a,
                    flags: af,
                },
                Self::Regex {
                    pattern: b,
                    flags: bf,
                },
            ) => a == b && af == bf,
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
            Self::Quantity(q) => write!(f, "{q}"),
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
            Self::Regex { pattern, flags } => write!(f, "p/{pattern}/{flags}"),
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
            Self::Quantity(_) => "Quantity",
            Self::Complex(_) => "Complex",
            Self::String(_) => "String",
            Self::DateTime(_) => "DateTime",
            Self::Array(_) => "Array",
            Self::Regex { .. } => "Regex",
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

    /// Convert to a Rust String, or error if not a String value.
    pub fn to_string_val(&self) -> Result<String, TaqlError> {
        match self {
            Self::String(s) => Ok(s.clone()),
            Self::Null => Err(TaqlError::TypeError {
                message: "cannot convert Null to String".to_string(),
            }),
            other => Ok(format!("{other}")),
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

        if let Some((lhs, rhs, _)) = comparable_quantities(self, other)? {
            return Ok(lhs.total_cmp(&rhs));
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
///
/// The flat data vector uses **column-major** (Fortran) order to match C++ casacore
/// conventions, where the first dimension varies fastest. Transposing (`.t()`) and
/// iterating in standard order yields column-major order regardless of whether the
/// source `ArrayD` uses C or Fortran memory layout.
fn array_value_to_expr(arr: &casa_types::ArrayValue) -> ExprValue {
    use casa_types::ArrayValue as AV;
    let shape: Vec<usize> = arr.shape().to_vec();

    let data: Vec<ExprValue> = match arr {
        AV::Bool(a) => a.t().iter().map(|v| ExprValue::Bool(*v)).collect(),
        AV::UInt8(a) => a.t().iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::UInt16(a) => a.t().iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::UInt32(a) => a.t().iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::Int16(a) => a.t().iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::Int32(a) => a.t().iter().map(|v| ExprValue::Int(*v as i64)).collect(),
        AV::Int64(a) => a.t().iter().map(|v| ExprValue::Int(*v)).collect(),
        AV::Float32(a) => a.t().iter().map(|v| ExprValue::Float(*v as f64)).collect(),
        AV::Float64(a) => a.t().iter().map(|v| ExprValue::Float(*v)).collect(),
        AV::Complex32(a) => a
            .t()
            .iter()
            .map(|v| ExprValue::Complex(Complex64::new(v.re as f64, v.im as f64)))
            .collect(),
        AV::Complex64(a) => a.t().iter().map(|v| ExprValue::Complex(*v)).collect(),
        AV::String(a) => a.t().iter().map(|v| ExprValue::String(v.clone())).collect(),
    };
    ExprValue::Array(ArrayValue { shape, data })
}

/// The evaluation context: provides column values for the current row.
pub struct EvalContext<'a> {
    /// The current row being evaluated.
    pub row: &'a RecordValue,
    /// The 0-based row index in the parent table.
    pub row_index: usize,
    /// Index style (Glish=1-based or Python=0-based).
    pub style: ast::IndexStyle,
    /// Explicit measures provider attached to the table being evaluated.
    pub measures: Option<&'a std::sync::Arc<dyn casa_types::measures::MeasuresProvider>>,
}

/// Evaluates a string argument that must be independent of any table row.
///
/// # Errors
///
/// Returns a type error for row-dependent or non-string expressions and
/// propagates evaluation errors from invalid constant expressions.
pub fn eval_constant_string(
    expr: &Expr,
    name: &str,
    style: ast::IndexStyle,
) -> Result<String, TaqlError> {
    fn error(name: &str) -> TaqlError {
        TaqlError::TypeError {
            message: format!("{name} requires a constant string argument"),
        }
    }

    fn check(expr: &mut Expr, name: &str) -> Result<(), TaqlError> {
        match expr {
            Expr::ColumnRef(_)
            | Expr::RowNumber
            | Expr::Subquery(_)
            | Expr::Aggregate { .. }
            | Expr::BoundRowLookup { .. }
            | Expr::BoundRowFunction(_) => return Err(error(name)),
            Expr::FunctionCall { name: func, .. }
                if [
                    "rownumber",
                    "rownr",
                    "rowid",
                    "iscolumn",
                    "iskeyword",
                    "rand",
                ]
                .iter()
                .any(|key| func.eq_ignore_ascii_case(key)) =>
            {
                return Err(error(name));
            }
            _ => {}
        }
        expr.try_for_each_child_mut(&mut |child| check(child, name))
    }

    check(&mut expr.clone(), name)?;
    let ctx = EvalContext {
        row: &RecordValue::default(),
        row_index: 0,
        style,
        measures: None,
    };
    match eval_expr(expr, &ctx)? {
        ExprValue::String(value) => Ok(value),
        _ => Err(error(name)),
    }
}

/// Evaluate an expression against a row context.
pub fn eval_expr(expr: &Expr, ctx: &EvalContext<'_>) -> Result<ExprValue, TaqlError> {
    match expr {
        Expr::Literal(lit) => Ok(eval_literal(lit)),
        Expr::ArrayLiteral(values) => arrays::literal(values, ctx),
        Expr::ColumnRef(col_ref) => eval_column_ref(col_ref, ctx),
        Expr::BoundRowLookup {
            original,
            index,
            values,
            missing,
        } => {
            let ExprValue::Int(row) = eval_expr(index, ctx)? else {
                return Err(TaqlError::TypeError {
                    message: format!("{original} requires an integer row ID"),
                });
            };
            usize::try_from(row)
                .ok()
                .and_then(|row| values.get(row))
                .or(missing.as_ref())
                .cloned()
                .ok_or_else(|| TaqlError::Table(format!("{original} row {row} is out of range")))
        }
        Expr::BoundRowFunction(function) => function.evaluate(ctx),
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
                match item {
                    ExprValue::Array(arr) => {
                        if contains_equal(&val, &arr.data)? {
                            found = true;
                            break;
                        }
                    }
                    other if equal_values(&val, &other)? => {
                        found = true;
                        break;
                    }
                    _ => {}
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
        Expr::RegexMatch {
            expr,
            pattern,
            negated,
        } => {
            let val = eval_expr(expr, ctx)?;
            let pat = eval_expr(pattern, ctx)?;
            if val.is_null() || pat.is_null() {
                return Ok(ExprValue::Null);
            }
            let s = match &val {
                ExprValue::String(s) => s.as_str(),
                _ => {
                    return Err(TaqlError::TypeError {
                        message: format!("regex match requires String, got {}", val.type_name()),
                    });
                }
            };
            let (re_pattern, case_insensitive) = match &pat {
                ExprValue::String(p) => (p.as_str(), false),
                ExprValue::Regex { pattern, flags } => (pattern.as_str(), flags.contains('i')),
                _ => {
                    return Err(TaqlError::TypeError {
                        message: format!(
                            "regex pattern must be String or Regex, got {}",
                            pat.type_name()
                        ),
                    });
                }
            };
            let re_str = if case_insensitive {
                format!("(?i){re_pattern}")
            } else {
                re_pattern.to_string()
            };
            let re = regex::Regex::new(&re_str).map_err(|e| TaqlError::TypeError {
                message: format!("invalid regex: {e}"),
            })?;
            let matched = re.is_match(s);
            Ok(ExprValue::Bool(if *negated { !matched } else { matched }))
        }
        Expr::InSet {
            expr,
            elements,
            negated,
        } => {
            let val = eval_expr(expr, ctx)?;
            if val.is_null() {
                return Ok(ExprValue::Null);
            }
            let found = in_set_contains(&val, elements, ctx)?;
            Ok(ExprValue::Bool(if *negated { !found } else { found }))
        }
        Expr::ArrayIndex { array, indices } => {
            let arr_val = eval_expr(array, ctx)?;
            match arr_val {
                ExprValue::Array(arr) => eval_array_index(&arr, indices, ctx),
                other => Err(TaqlError::TypeError {
                    message: format!("array indexing requires Array, got {}", other.type_name()),
                }),
            }
        }
        Expr::Star => Ok(ExprValue::Null), // * in expression context is unusual
        Expr::RowNumber => Ok(ExprValue::Int(ctx.row_index as i64)),
        Expr::Subquery(_) => {
            // Subquery evaluation requires a table context; in per-row eval
            // we return Null. Full subquery support needs the executor.
            Ok(ExprValue::Null)
        }
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

/// Test `val` (non-NULL) for membership in the elements of an `IN [...]`
/// set, evaluating elements in order and stopping at the first match.
///
/// A range element matches when `val` lies within its inclusive bounds and,
/// if a step is given, sits on the stride counted from the start (or 0).
fn in_set_contains(
    val: &ExprValue,
    elements: &[ast::InSetElement],
    ctx: &EvalContext<'_>,
) -> Result<bool, TaqlError> {
    for elem in elements {
        match elem {
            ast::InSetElement::Value(v) => {
                let item = eval_expr(v, ctx)?;
                if equal_values(val, &item)? {
                    return Ok(true);
                }
            }
            ast::InSetElement::Range { start, end, step } => {
                let lo = start.as_ref().map(|s| eval_expr(s, ctx)).transpose()?;
                let hi = end.as_ref().map(|e| eval_expr(e, ctx)).transpose()?;
                // Check range membership
                let ge_lo = match &lo {
                    Some(l) => val.compare(l)? != Ordering::Less,
                    None => true,
                };
                let le_hi = match &hi {
                    Some(h) => val.compare(h)? != Ordering::Greater,
                    None => true,
                };
                if ge_lo && le_hi {
                    // If step is given, check stride
                    if let Some(step_expr) = step {
                        let s_val = eval_expr(step_expr, ctx)?;
                        let step_f = s_val.to_float()?;
                        let v_f = val.to_float()?;
                        let lo_f = lo
                            .as_ref()
                            .map(|l| l.to_float())
                            .transpose()?
                            .unwrap_or(0.0);
                        if step_f != 0.0 {
                            let offset = v_f - lo_f;
                            let remainder = offset % step_f;
                            if remainder.abs() < 1e-10 {
                                return Ok(true);
                            }
                        }
                    } else {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(false)
}

fn eval_literal(lit: &Literal) -> ExprValue {
    match lit {
        Literal::Int(n) => ExprValue::Int(*n),
        Literal::Float(v) => ExprValue::Float(*v),
        Literal::Quantity { value, unit } => {
            ExprValue::Quantity(Quantity::new(*value, unit).expect("parser validates units"))
        }
        Literal::String(s) => ExprValue::String(s.clone()),
        Literal::Bool(b) => ExprValue::Bool(*b),
        Literal::Complex(c) => ExprValue::Complex(*c),
        Literal::Regex { pattern, flags } => ExprValue::Regex {
            pattern: pattern.clone(),
            flags: flags.clone(),
        },
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

pub(super) fn eval_unary(op: UnaryOp, val: ExprValue) -> Result<ExprValue, TaqlError> {
    if val.is_null() {
        return Ok(ExprValue::Null);
    }
    match op {
        UnaryOp::Negate => match val {
            ExprValue::Int(n) => Ok(ExprValue::Int(-n)),
            ExprValue::Float(v) => Ok(ExprValue::Float(-v)),
            ExprValue::Quantity(q) => Ok(ExprValue::Quantity(Quantity::with_unit(
                -q.value(),
                q.unit().clone(),
            ))),
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

pub(super) fn eval_binary(
    op: BinaryOp,
    lhs: ExprValue,
    rhs: ExprValue,
) -> Result<ExprValue, TaqlError> {
    // NULL propagation for all operators
    if lhs.is_null() || rhs.is_null() {
        return Ok(ExprValue::Null);
    }

    if let Some(result) = arrays::binary(op, &lhs, &rhs) {
        return result;
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
        BinaryOp::Eq => Ok(ExprValue::Bool(equal_values(&lhs, &rhs)?)),
        BinaryOp::Ne => Ok(ExprValue::Bool(!equal_values(&lhs, &rhs)?)),
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            eval_predicate_order(op, lhs, rhs)
        }
        BinaryOp::Add => eval_arithmetic_add(lhs, rhs),
        BinaryOp::Sub => eval_arithmetic_sub(lhs, rhs),
        BinaryOp::Mul => eval_arithmetic_mul(lhs, rhs),
        BinaryOp::Div => eval_arithmetic_div(lhs, rhs),
        BinaryOp::IntDiv => eval_int_div(lhs, rhs),
        BinaryOp::Modulo => eval_modulo(lhs, rhs),
        BinaryOp::Power => eval_power(lhs, rhs),
        BinaryOp::BitAnd => {
            let a = lhs.to_int()?;
            let b = rhs.to_int()?;
            Ok(ExprValue::Int(a & b))
        }
        BinaryOp::BitOr => {
            let a = lhs.to_int()?;
            let b = rhs.to_int()?;
            Ok(ExprValue::Int(a | b))
        }
        BinaryOp::BitXor => {
            let a = lhs.to_int()?;
            let b = rhs.to_int()?;
            Ok(ExprValue::Int(a ^ b))
        }
    }
}

// Ordering a query predicate follows IEEE comparisons. Sorting still uses
// ExprValue::compare's total order; a NaN is not less/greater/equal in a predicate.
fn eval_predicate_order(
    op: BinaryOp,
    lhs: ExprValue,
    rhs: ExprValue,
) -> Result<ExprValue, TaqlError> {
    let order = if let Some((lhs, rhs, _)) = comparable_quantities(&lhs, &rhs)? {
        lhs.partial_cmp(&rhs)
    } else {
        match (&lhs, &rhs) {
            (ExprValue::Float(a), ExprValue::Float(b)) => a.partial_cmp(b),
            (ExprValue::Int(a), ExprValue::Float(b)) => (*a as f64).partial_cmp(b),
            (ExprValue::Float(a), ExprValue::Int(b)) => a.partial_cmp(&(*b as f64)),
            _ => Some(lhs.compare(&rhs)?),
        }
    };
    Ok(ExprValue::Bool(order.is_some_and(|order| match op {
        BinaryOp::Lt => order == Ordering::Less,
        BinaryOp::Le => order != Ordering::Greater,
        BinaryOp::Gt => order == Ordering::Greater,
        BinaryOp::Ge => order != Ordering::Less,
        _ => unreachable!("ordered comparison"),
    })))
}

fn equal_values(lhs: &ExprValue, rhs: &ExprValue) -> Result<bool, TaqlError> {
    Ok(match comparable_quantities(lhs, rhs)? {
        Some((lhs, rhs, _)) => lhs == rhs,
        None => lhs == rhs,
    })
}

fn contains_equal(value: &ExprValue, candidates: &[ExprValue]) -> Result<bool, TaqlError> {
    for candidate in candidates {
        if equal_values(value, candidate)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A unitless real operand inherits the other operand's unit. If both have a
/// unit, use the first one and convert the second before comparing or adding.
fn comparable_quantities<'a>(
    lhs: &'a ExprValue,
    rhs: &'a ExprValue,
) -> Result<Option<(f64, f64, &'a Unit)>, TaqlError> {
    match (lhs, rhs) {
        (ExprValue::Quantity(lhs), ExprValue::Quantity(rhs)) => {
            let right = rhs
                .get_value_in(lhs.unit())
                .map_err(|error| TaqlError::TypeError {
                    message: error.to_string(),
                })?;
            Ok(Some((lhs.value(), right, lhs.unit())))
        }
        (ExprValue::Quantity(lhs), ExprValue::Int(_) | ExprValue::Float(_)) => {
            Ok(Some((lhs.value(), rhs.to_float()?, lhs.unit())))
        }
        (ExprValue::Int(_) | ExprValue::Float(_), ExprValue::Quantity(rhs)) => {
            Ok(Some((lhs.to_float()?, rhs.value(), rhs.unit())))
        }
        _ => Ok(None),
    }
}

fn quantity_product(lhs: &ExprValue, rhs: &ExprValue, divide: bool) -> Option<ExprValue> {
    fn quantity(value: &ExprValue) -> Option<Quantity> {
        match value {
            ExprValue::Quantity(value) => Some(value.clone()),
            ExprValue::Int(value) => Some(Quantity::dimensionless(*value as f64)),
            ExprValue::Float(value) => Some(Quantity::dimensionless(*value)),
            _ => None,
        }
    }

    if !matches!(lhs, ExprValue::Quantity(_)) && !matches!(rhs, ExprValue::Quantity(_)) {
        return None;
    }
    let (lhs, rhs) = (quantity(lhs)?, quantity(rhs)?);
    if divide && lhs.unit().conformant(rhs.unit()) {
        return Some(ExprValue::Float(
            lhs.value() / rhs.get_value_in(lhs.unit()).ok()?,
        ));
    }
    Some(ExprValue::Quantity(if divide {
        &lhs / &rhs
    } else {
        &lhs * &rhs
    }))
}

fn arithmetic_operands(
    lhs: ExprValue,
    rhs: ExprValue,
) -> Result<(ExprValue, ExprValue), TaqlError> {
    // CASA permits explicit numeric casts of Bool, not implicit arithmetic
    // operands. In particular Bool+Bool must return an error, never unwind.
    if matches!(lhs, ExprValue::Bool(_)) || matches!(rhs, ExprValue::Bool(_)) {
        return Err(TaqlError::TypeError {
            message: "boolean arithmetic operands".to_string(),
        });
    }
    promote(lhs, rhs)
}

fn eval_arithmetic_add(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    // String concatenation
    if let (ExprValue::String(a), ExprValue::String(b)) = (&lhs, &rhs) {
        return Ok(ExprValue::String(format!("{a}{b}")));
    }
    if let Some((lhs, rhs, unit)) = comparable_quantities(&lhs, &rhs)? {
        return Ok(ExprValue::Quantity(Quantity::with_unit(
            lhs + rhs,
            unit.clone(),
        )));
    }
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_add(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a + b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a + b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_sub(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    if let Some((lhs, rhs, unit)) = comparable_quantities(&lhs, &rhs)? {
        return Ok(ExprValue::Quantity(Quantity::with_unit(
            lhs - rhs,
            unit.clone(),
        )));
    }
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_sub(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a - b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a - b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_mul(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    if let Some(value) = quantity_product(&lhs, &rhs, false) {
        return Ok(value);
    }
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_mul(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a * b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a * b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_div(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    if let Some(value) = quantity_product(&lhs, &rhs, true) {
        return Ok(value);
    }
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        // TableExprNodeDivideDouble promotes integer operands to Double,
        // including IEEE infinity/NaN at a zero divisor.
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Float(a as f64 / b as f64)),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a / b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a / b)),
        _ => unreachable!(),
    }
}

fn eval_int_div(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    // TaQL spells this floor(a/b), retaining the real division's type.
    Ok(ExprValue::Float((a.to_float()? / b.to_float()?).floor()))
}

fn eval_modulo(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => {
            if b == 0 {
                return Err(TaqlError::DivisionByZero);
            }
            let mut remainder = a.checked_rem(b).unwrap_or(0);
            if remainder != 0 && (a < 0) != (b < 0) {
                remainder += b;
            }
            Ok(ExprValue::Int(remainder))
        }
        (ExprValue::Float(a), ExprValue::Float(b)) => {
            let mut remainder = a % b;
            if remainder != 0.0 && (a < 0.0) != (b < 0.0) {
                remainder += b;
            }
            Ok(ExprValue::Float(remainder))
        }
        _ => Err(TaqlError::TypeError {
            message: "modulo not supported for complex numbers".to_string(),
        }),
    }
}

fn eval_power(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Float((a as f64).powf(b as f64))),
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

/// Convert a user-supplied index to a 0-based index, respecting [`IndexStyle`].
///
/// - `one_based` (Glish/`base1`): subtract 1 (negative indices count from end).
/// - `0-based` (Python/`base0`): use as-is (negative indices count from end).
fn to_zero_based(idx: i64, len: usize, style: ast::IndexStyle) -> Result<usize, TaqlError> {
    let len_i = len as i64;
    let actual = if idx < 0 {
        len_i + idx
    } else if style.is_one_based() {
        idx - 1
    } else {
        idx
    };
    if actual < 0 || actual >= len_i {
        return Err(TaqlError::TypeError {
            message: format!("array index {idx} out of bounds (length {len})"),
        });
    }
    Ok(actual as usize)
}

/// Resolve a slice (`start:end[:step]`) to a Vec of 0-based indices.
fn resolve_slice(
    start: Option<i64>,
    end: Option<i64>,
    step: Option<i64>,
    dim_len: usize,
    style: ast::IndexStyle,
) -> Result<Vec<usize>, TaqlError> {
    let step = step.unwrap_or(1);
    if step == 0 {
        return Err(TaqlError::TypeError {
            message: "slice step cannot be zero".to_string(),
        });
    }
    if dim_len == 0 {
        return Ok(Vec::new());
    }
    let len_i = dim_len as i64;
    let offset = if style.is_one_based() { 1 } else { 0 };
    let to_idx = |v: i64| if v < 0 { len_i + v } else { v - offset };

    let mut result = Vec::new();
    if style.is_end_inclusive() {
        if step > 0 {
            let mut i = start.map_or(0, to_idx);
            if i < 0 {
                let hops = (-i + step - 1) / step;
                i += hops * step;
            }
            let e0 = end.map_or(len_i - 1, |e| to_idx(e).min(len_i - 1));
            while i <= e0 {
                result.push(i as usize);
                i += step;
            }
        } else {
            let mut i = start.map_or(len_i - 1, to_idx);
            if i >= len_i {
                let neg_step = -step;
                let hops = (i - (len_i - 1) + neg_step - 1) / neg_step;
                i += hops * step;
            }
            let e0 = end.map_or(0, |e| to_idx(e).max(0));
            while i >= e0 {
                result.push(i as usize);
                i += step;
            }
        }
    } else if step > 0 {
        let s0 = start.map_or(0, |s| to_idx(s).clamp(0, len_i));
        let e0 = end.map_or(len_i, |e| to_idx(e).clamp(0, len_i));
        let mut i = s0;
        while i < e0 {
            result.push(i as usize);
            i += step;
        }
    } else {
        let s0 = start.map_or(len_i - 1, |s| to_idx(s).clamp(-1, len_i - 1));
        let e0 = end.map_or(-1, |e| to_idx(e).clamp(-1, len_i - 1));
        let mut i = s0;
        while i > e0 {
            result.push(i as usize);
            i += step;
        }
    }
    Ok(result)
}

/// Evaluate array indexing/slicing with style-aware offset.
///
/// Supports N-dimensional arrays with single-element access and slicing.
/// When `!ctx.style.is_fortran_order()` (`CORDER` / `PYTHON`), index positions
/// map in reverse order (`indices[0]` → innermost-last Fortran axis).
///
/// C++ reference: `TableExprNodeArrayPart`.
pub(super) fn eval_array_index(
    arr: &ArrayValue,
    indices: &[ast::IndexElement],
    ctx: &EvalContext<'_>,
) -> Result<ExprValue, TaqlError> {
    let ndim = arr.shape.len();

    // If fewer indices than dimensions, remaining dims are taken in full.
    if indices.len() > ndim {
        return Err(TaqlError::TypeError {
            message: format!(
                "#indices mismatches array dimensionality ({} for {}-dimensional array)",
                indices.len(),
                ndim
            ),
        });
    }

    let fortran_order = ctx.style.is_fortran_order();
    let index_for_dim = |dim: usize| -> Option<&ast::IndexElement> {
        let user_pos = if fortran_order { dim } else { ndim - 1 - dim };
        indices.get(user_pos)
    };

    // Resolve each index dimension to a list of positions.
    let mut dim_indices: Vec<Vec<usize>> = Vec::with_capacity(ndim);
    for (dim, &dim_len) in arr.shape.iter().enumerate() {
        if let Some(elem) = index_for_dim(dim) {
            match elem {
                ast::IndexElement::Single(expr) => {
                    let val = eval_expr(expr, ctx)?;
                    let idx = val.to_int()?;
                    let pos = to_zero_based(idx, dim_len, ctx.style)?;
                    dim_indices.push(vec![pos]);
                }
                ast::IndexElement::Slice { start, end, step } => {
                    let s = start
                        .as_ref()
                        .map(|e| eval_expr(e, ctx)?.to_int())
                        .transpose()?;
                    let e = end
                        .as_ref()
                        .map(|e| eval_expr(e, ctx)?.to_int())
                        .transpose()?;
                    let st = step
                        .as_ref()
                        .map(|e| eval_expr(e, ctx)?.to_int())
                        .transpose()?;
                    dim_indices.push(resolve_slice(s, e, st, dim_len, ctx.style)?);
                }
            }
        } else {
            // Remaining dims: take all elements.
            dim_indices.push((0..dim_len).collect());
        }
    }

    // Compute output shape: dims with Single index are squeezed.
    let mut result_shape = Vec::new();
    for (dim, idxs) in dim_indices.iter().enumerate() {
        let is_single = matches!(index_for_dim(dim), Some(ast::IndexElement::Single(_)));
        if !is_single {
            result_shape.push(idxs.len());
        }
    }

    if dim_indices.iter().any(Vec::is_empty) {
        return Ok(ExprValue::Array(ArrayValue {
            shape: result_shape,
            data: Vec::new(),
        }));
    }

    // Collect elements using column-major (Fortran) traversal of the index
    // combinations, matching C++ casacore array storage order.
    let mut result_data = Vec::new();
    let mut combo = vec![0usize; ndim];
    loop {
        // Compute flat index from combo using column-major strides
        // (first dimension varies fastest, matching C++ casacore).
        let mut flat = 0;
        let mut stride = 1;
        for dim in 0..ndim {
            flat += dim_indices[dim][combo[dim]] * stride;
            stride *= arr.shape[dim];
        }
        if flat < arr.data.len() {
            result_data.push(arr.data[flat].clone());
        }

        // Advance combo (leftmost first = column-major traversal)
        let mut carry = true;
        for dim in 0..ndim {
            if carry {
                combo[dim] += 1;
                if combo[dim] < dim_indices[dim].len() {
                    carry = false;
                } else {
                    combo[dim] = 0;
                }
            }
        }
        if carry {
            break;
        }
    }

    // If the result is a scalar (all dims indexed with Single subscripts), return the single element.
    if result_shape.is_empty() {
        if let Some(val) = result_data.into_iter().next() {
            return Ok(val);
        }
        return Ok(ExprValue::Null);
    }

    Ok(ExprValue::Array(ArrayValue {
        shape: result_shape,
        data: result_data,
    }))
}

#[cfg(test)]
mod tests;
