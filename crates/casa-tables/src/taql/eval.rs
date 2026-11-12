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

use casa_types::quanta::Quantity;
use casa_types::{RecordValue, ScalarValue, Value};
use num_complex::Complex64;

use super::ast::{self, *};
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

        // Same type fast path
        match (self, other) {
            (ExprValue::Bool(a), ExprValue::Bool(b)) => return Ok(a.cmp(b)),
            (ExprValue::Int(a), ExprValue::Int(b)) => return Ok(a.cmp(b)),
            (ExprValue::Float(a), ExprValue::Float(b)) => return Ok(a.total_cmp(b)),
            (ExprValue::Quantity(a), ExprValue::Quantity(b)) => {
                if !a.unit().conformant(b.unit()) {
                    return Err(TaqlError::TypeError {
                        message: format!(
                            "cannot compare Quantity {} with Quantity {}",
                            a.unit().name(),
                            b.unit().name()
                        ),
                    });
                }
                return Ok(a.get_si_value().total_cmp(&b.get_si_value()));
            }
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
/// conventions, where the first dimension varies fastest. We use `as_slice_memory_order()`
/// since casacore arrays are stored in Fortran layout.
fn array_value_to_expr(arr: &casa_types::ArrayValue) -> ExprValue {
    use casa_types::ArrayValue as AV;
    let shape: Vec<usize> = arr.shape().to_vec();

    /// Iterate ndarray elements in storage (memory) order.
    /// For Fortran-layout arrays (as used by casacore), this gives column-major order.
    fn storage_order<T>(a: &ndarray::ArrayD<T>) -> &[T] {
        a.as_slice_memory_order()
            .expect("casacore arrays are always contiguous")
    }

    let data: Vec<ExprValue> = match arr {
        AV::Bool(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Bool(*v))
            .collect(),
        AV::UInt8(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Int(*v as i64))
            .collect(),
        AV::UInt16(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Int(*v as i64))
            .collect(),
        AV::UInt32(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Int(*v as i64))
            .collect(),
        AV::Int16(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Int(*v as i64))
            .collect(),
        AV::Int32(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Int(*v as i64))
            .collect(),
        AV::Int64(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Int(*v))
            .collect(),
        AV::Float32(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Float(*v as f64))
            .collect(),
        AV::Float64(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Float(*v))
            .collect(),
        AV::Complex32(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Complex(Complex64::new(v.re as f64, v.im as f64)))
            .collect(),
        AV::Complex64(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::Complex(*v))
            .collect(),
        AV::String(a) => storage_order(a)
            .iter()
            .map(|v| ExprValue::String(v.clone()))
            .collect(),
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
                if *val == item {
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

fn eval_unary(op: UnaryOp, val: ExprValue) -> Result<ExprValue, TaqlError> {
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
    let order = match (&lhs, &rhs) {
        (ExprValue::Float(a), ExprValue::Float(b)) => a.partial_cmp(b),
        (ExprValue::Int(a), ExprValue::Float(b)) => (*a as f64).partial_cmp(b),
        (ExprValue::Float(a), ExprValue::Int(b)) => a.partial_cmp(&(*b as f64)),
        _ => Some(lhs.compare(&rhs)?),
    };
    Ok(ExprValue::Bool(order.is_some_and(|order| match op {
        BinaryOp::Lt => order == Ordering::Less,
        BinaryOp::Le => order != Ordering::Greater,
        BinaryOp::Gt => order == Ordering::Greater,
        BinaryOp::Ge => order != Ordering::Less,
        _ => unreachable!("ordered comparison"),
    })))
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
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_add(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a + b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a + b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_sub(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_sub(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a - b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a - b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_mul(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
    let (a, b) = arithmetic_operands(lhs, rhs)?;
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => Ok(ExprValue::Int(a.wrapping_mul(b))),
        (ExprValue::Float(a), ExprValue::Float(b)) => Ok(ExprValue::Float(a * b)),
        (ExprValue::Complex(a), ExprValue::Complex(b)) => Ok(ExprValue::Complex(a * b)),
        _ => unreachable!(),
    }
}

fn eval_arithmetic_div(lhs: ExprValue, rhs: ExprValue) -> Result<ExprValue, TaqlError> {
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
/// - **Glish** (1-based): subtract 1.
/// - **Python** (0-based): use as-is. Negative indices count from end.
fn to_zero_based(idx: i64, len: usize, style: ast::IndexStyle) -> Result<usize, TaqlError> {
    let len_i = len as i64;
    let actual = match style {
        ast::IndexStyle::Glish => {
            // 1-based: 1 → 0, negative counts from end
            if idx < 0 { len_i + idx } else { idx - 1 }
        }
        ast::IndexStyle::Python => {
            if idx < 0 {
                len_i + idx
            } else {
                idx
            }
        }
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
    let len_i = dim_len as i64;
    match style {
        ast::IndexStyle::Glish => {
            // 1-based inclusive: start defaults to 1, end defaults to len
            let s = start.unwrap_or(1);
            let e = end.unwrap_or(len_i);
            let s0 = if s < 0 { len_i + s } else { s - 1 };
            let e0 = if e < 0 { len_i + e } else { e - 1 };
            let mut result = Vec::new();
            if step > 0 {
                let mut i = s0;
                while i <= e0 && i < len_i {
                    if i >= 0 {
                        result.push(i as usize);
                    }
                    i += step;
                }
            } else {
                let mut i = s0;
                while i >= e0 && i >= 0 {
                    if i < len_i {
                        result.push(i as usize);
                    }
                    i += step;
                }
            }
            Ok(result)
        }
        ast::IndexStyle::Python => {
            // 0-based exclusive end: start defaults to 0, end defaults to len
            let s = start.unwrap_or(if step > 0 { 0 } else { len_i - 1 });
            let e = end.unwrap_or(if step > 0 { len_i } else { -len_i - 1 });
            let s0 = if s < 0 {
                (len_i + s).max(0)
            } else {
                s.min(len_i)
            };
            let e0 = if e < 0 {
                (len_i + e).max(-1)
            } else {
                e.min(len_i)
            };
            let mut result = Vec::new();
            if step > 0 {
                let mut i = s0;
                while i < e0 {
                    result.push(i as usize);
                    i += step;
                }
            } else {
                let mut i = s0;
                while i > e0 {
                    result.push(i as usize);
                    i += step;
                }
            }
            Ok(result)
        }
    }
}

/// Evaluate array indexing/slicing with style-aware offset.
///
/// Supports N-dimensional arrays with single-element access and slicing.
///
/// C++ reference: `TableExprNodeArrayPart`.
fn eval_array_index(
    arr: &ArrayValue,
    indices: &[ast::IndexElement],
    ctx: &EvalContext<'_>,
) -> Result<ExprValue, TaqlError> {
    let ndim = arr.shape.len();

    // If fewer indices than dimensions, remaining dims are taken in full.
    // If more indices than dimensions for a 1-D array, allow as flat access.
    if indices.len() > ndim && ndim != 1 {
        return Err(TaqlError::TypeError {
            message: format!(
                "too many indices ({}) for {}-dimensional array",
                indices.len(),
                ndim
            ),
        });
    }

    // Resolve each index dimension to a list of positions.
    let mut dim_indices: Vec<Vec<usize>> = Vec::with_capacity(ndim);
    for dim in 0..ndim {
        let dim_len = arr.shape[dim];
        if dim < indices.len() {
            match &indices[dim] {
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

    // Compute output shape: dims with >1 element remain; dims with 1 element are squeezed.
    // But only squeeze if the user gave a Single index for that dim.
    let mut result_shape = Vec::new();
    for (dim, idxs) in dim_indices.iter().enumerate() {
        let is_single = dim < indices.len() && matches!(indices[dim], ast::IndexElement::Single(_));
        if !is_single {
            result_shape.push(idxs.len());
        }
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

    // If the result is a scalar (all dims squeezed), return the single element.
    if result_shape.is_empty() || result_shape.iter().all(|&d| d == 1) {
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
