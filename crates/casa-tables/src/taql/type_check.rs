// SPDX-License-Identifier: LGPL-3.0-or-later
//! Check casacore record selections against column types without reading rows.

use casa_types::PrimitiveType;

use super::ast::{BinaryOp, Expr, InSetElement, IndexElement, Literal, UnaryOp};
use super::{TaqlError, functions};
use crate::{ArrayShapeContract, ColumnType, Table};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Unknown,
    Bool,
    Int,
    Real,
    Complex,
    Text,
    Date,
    Regex,
}

impl Kind {
    fn numeric(self) -> bool {
        matches!(self, Self::Int | Self::Real | Self::Complex)
    }

    fn combine(self, other: Self) -> Result<Self, TaqlError> {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Ok(Self::Unknown),
            (left, right) if left == right => Ok(left),
            (left, right) if left.numeric() && right.numeric() => {
                if matches!(left, Self::Complex) || matches!(right, Self::Complex) {
                    Ok(Self::Complex)
                } else {
                    Ok(Self::Real)
                }
            }
            _ => Err(invalid(format!(
                "cannot combine {self:?} and {other:?} operands"
            ))),
        }
    }

    fn require(self, allowed: &[Self], expression: &str) -> Result<(), TaqlError> {
        if self == Self::Unknown || allowed.contains(&self) {
            Ok(())
        } else {
            Err(invalid(format!(
                "{expression} does not accept a {self:?} operand"
            )))
        }
    }
}

const NUMERIC: &[Kind] = &[Kind::Int, Kind::Real, Kind::Complex];
const REAL: &[Kind] = &[Kind::Int, Kind::Real];
const ORDERED: &[Kind] = &[Kind::Int, Kind::Real, Kind::Complex, Kind::Text, Kind::Date];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Unknown,
    Scalar,
    Array(Option<usize>),
}

impl Shape {
    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Scalar, other) | (other, Self::Scalar) => other,
            (Self::Array(left), Self::Array(right)) if left == right => Self::Array(left),
            (Self::Array(_), Self::Array(_)) => Self::Array(None),
            _ => Self::Unknown,
        }
    }
}

type ExprType = (Kind, Shape);

fn scalar(kind: Kind) -> ExprType {
    (kind, Shape::Scalar)
}

fn invalid(message: impl Into<String>) -> TaqlError {
    TaqlError::TypeError {
        message: message.into(),
    }
}

/// Validate an expression parsed with casacore's record-selection rules.
/// The expression itself may be non-Boolean. The result reports whether
/// it has a type that can be used as a row predicate.
/// # Errors
/// A known operand type, column, array index, or function call is invalid.
#[doc(hidden)]
pub fn validate_record_expression(
    expr: &Expr,
    table: &Table,
) -> Result<RecordExpressionType, TaqlError> {
    super::calc::validate_calc_columns(expr, table)?;
    let (kind, shape) = check(expr, table)?;
    Ok(match (kind, shape) {
        (Kind::Bool | Kind::Unknown, Shape::Array(_)) => RecordExpressionType::BooleanArray,
        (Kind::Bool | Kind::Unknown, _) => RecordExpressionType::PossibleRowPredicate,
        _ => RecordExpressionType::NonBoolean,
    })
}

/// Whether an expression can participate in a CASA Boolean row selection.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RecordExpressionType {
    /// Scalar Boolean, including expressions with a dynamic result type.
    PossibleRowPredicate,
    /// Boolean elements, but an array result cannot directly select rows.
    BooleanArray,
    /// A known non-Boolean element type.
    NonBoolean,
}

fn check(expr: &Expr, table: &Table) -> Result<ExprType, TaqlError> {
    use Expr::*;
    let expression_type = match expr {
        Literal(value) => scalar(literal_kind(value)),
        RowNumber => scalar(Kind::Int),
        ColumnRef(column) => table
            .schema()
            .and_then(|schema| schema.column(&column.column))
            .map(|column| {
                let shape = match column.column_type() {
                    ColumnType::Scalar => Shape::Scalar,
                    ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
                        Shape::Array(Some(shape.len()))
                    }
                    ColumnType::Array(ArrayShapeContract::Variable { ndim }) => Shape::Array(*ndim),
                    ColumnType::Record => Shape::Unknown,
                };
                (
                    column.data_type().map_or(Kind::Unknown, primitive_kind),
                    shape,
                )
            })
            .unwrap_or((Kind::Unknown, Shape::Unknown)),
        Unary { op, operand } => {
            let (kind, shape) = check(operand, table)?;
            match op {
                UnaryOp::Not => kind.require(&[Kind::Bool], "logical NOT")?,
                UnaryOp::BitNot => kind.require(&[Kind::Int], "bitwise NOT")?,
                UnaryOp::Negate => kind.require(NUMERIC, "negation")?,
            }
            (kind, shape)
        }
        Binary { left, op, right } => binary(*op, check(left, table)?, check(right, table)?)?,
        ArrayIndex { array, indices } => index(check(array, table)?, indices, table)?,
        ArrayLiteral(values) => {
            let mut kinds = values
                .iter()
                .map(|value| check(value, table).map(|(kind, _)| kind));
            let kind = kinds.next().transpose()?.unwrap_or(Kind::Unknown);
            let kind = kinds.try_fold(kind, |left, right| left.combine(right?))?;
            (kind, Shape::Array(None))
        }
        FunctionCall { name, args } => {
            functions::validate_function_call(name, args.len())?;
            let operands = args
                .iter()
                .map(|arg| check(arg, table))
                .collect::<Result<Vec<_>, _>>()?;
            function_type(name, &operands)?
        }
        Between {
            expr, low, high, ..
        } => {
            let left = check(expr, table)?;
            binary(BinaryOp::Ge, left, check(low, table)?)?;
            binary(BinaryOp::Le, left, check(high, table)?)?
        }
        In { expr, values, .. } => {
            let (left, shape) = check(expr, table)?;
            for value in values {
                left.combine(check(value, table)?.0)?;
            }
            (Kind::Bool, shape)
        }
        InSet { expr, elements, .. } => {
            if elements.is_empty() {
                return Err(invalid("set selection requires at least one element"));
            }
            let (left, shape) = check(expr, table)?;
            for element in elements {
                match element {
                    InSetElement::Value(value) => {
                        left.combine(check(value, table)?.0)?;
                    }
                    InSetElement::Range { start, end, step } => {
                        for value in [start, end, step].into_iter().flatten() {
                            left.combine(check(value, table)?.0)?;
                        }
                    }
                }
            }
            (Kind::Bool, shape)
        }
        Like { expr, pattern, .. } | RegexMatch { expr, pattern, .. } => {
            let (operand, shape) = check(expr, table)?;
            operand.require(&[Kind::Text], "pattern match")?;
            check(pattern, table)?
                .0
                .require(&[Kind::Text, Kind::Regex], "pattern match")?;
            (Kind::Bool, shape)
        }
        IsNull { expr, .. } => (Kind::Bool, check(expr, table)?.1),
        BoundRowLookup { index, .. } | Aggregate { arg: index, .. } => {
            check(index, table)?;
            (Kind::Unknown, Shape::Unknown)
        }
        PreparedConstant(_) | BoundRowFunction(_) | Star | Subquery(_) => {
            (Kind::Unknown, Shape::Unknown)
        }
    };
    Ok(expression_type)
}

fn primitive_kind(ty: PrimitiveType) -> Kind {
    use PrimitiveType::*;
    match ty {
        Bool => Kind::Bool,
        UInt8 | UInt16 | UInt32 | Int16 | Int32 | Int64 => Kind::Int,
        Float32 | Float64 => Kind::Real,
        Complex32 | Complex64 => Kind::Complex,
        String => Kind::Text,
    }
}

fn literal_kind(value: &Literal) -> Kind {
    match value {
        Literal::Bool(_) => Kind::Bool,
        Literal::Int(_) => Kind::Int,
        Literal::Float(_) | Literal::Quantity { .. } => Kind::Real,
        Literal::Complex(_) => Kind::Complex,
        Literal::String(_) => Kind::Text,
        Literal::Regex { .. } => Kind::Regex,
        Literal::Null => Kind::Unknown,
    }
}

fn binary(op: BinaryOp, left: ExprType, right: ExprType) -> Result<ExprType, TaqlError> {
    use BinaryOp::*;
    let shape = left.1.combine(right.1);
    if matches!(op, Eq | Ne)
        && matches!(
            (left.0, right.0),
            (Kind::Text, Kind::Regex) | (Kind::Regex, Kind::Text)
        )
    {
        return Ok((Kind::Bool, shape));
    }
    if matches!(op, Eq | Ne | Lt | Le | Gt | Ge)
        && matches!(
            (left.0, right.0),
            (Kind::Date, Kind::Int | Kind::Real | Kind::Text)
                | (Kind::Int | Kind::Real | Kind::Text, Kind::Date)
        )
    {
        return Ok((Kind::Bool, shape));
    }
    let expression = format!("{op:?}");
    if matches!(op, Add | Sub) && (left.0 == Kind::Date || right.0 == Kind::Date) {
        let kinds = (left.0, right.0);
        match kinds {
            (Kind::Date, Kind::Date) if op == Sub => return Ok((Kind::Real, shape)),
            (Kind::Date, Kind::Int | Kind::Real | Kind::Unknown) => return Ok((Kind::Date, shape)),
            (Kind::Int | Kind::Real | Kind::Unknown, Kind::Date) if op == Add => {
                return Ok((Kind::Date, shape));
            }
            _ => {}
        }
    }
    let compatible = left.0.combine(right.0)?;
    let inputs = match op {
        And | Or => &[Kind::Bool][..],
        BitAnd | BitOr | BitXor => &[Kind::Int][..],
        Eq | Ne => return Ok((Kind::Bool, shape)),
        Lt | Le | Gt | Ge => ORDERED,
        Add if left.0 == Kind::Text || right.0 == Kind::Text => &[Kind::Text][..],
        _ => NUMERIC,
    };
    left.0.require(inputs, &expression)?;
    right.0.require(inputs, &expression)?;
    let kind = match op {
        And | Or | Lt | Le | Gt | Ge => Kind::Bool,
        BitAnd | BitOr | BitXor => Kind::Int,
        Div | IntDiv if compatible == Kind::Int => Kind::Real,
        _ => compatible,
    };
    Ok((kind, shape))
}

fn index(array: ExprType, indices: &[IndexElement], table: &Table) -> Result<ExprType, TaqlError> {
    if array.1 == Shape::Scalar {
        return Err(invalid("indexing requires an array"));
    }
    let mut slices = 0;
    for element in indices {
        match element {
            IndexElement::Single(expr) => {
                let (kind, shape) = check(expr, table)?;
                if kind == Kind::Bool && matches!(shape, Shape::Array(_)) && indices.len() == 1 {
                    return Ok((array.0, Shape::Array(None)));
                }
                kind.require(&[Kind::Int], "array index")?;
            }
            IndexElement::Slice { start, end, step } => {
                slices += 1;
                for expr in [start, end, step].into_iter().flatten() {
                    check(expr, table)?.0.require(&[Kind::Int], "array slice")?;
                }
                if matches!(step, Some(Expr::Literal(Literal::Int(stride))) if *stride <= 0) {
                    return Err(invalid("array slice stride must be positive"));
                }
            }
        }
    }
    if matches!(array.1, Shape::Array(Some(ndim)) if ndim != indices.len()) {
        return Err(invalid(
            "number of subscripts does not match array dimensionality",
        ));
    }
    Ok((
        array.0,
        if slices == 0 {
            Shape::Scalar
        } else {
            Shape::Array(Some(slices))
        },
    ))
}

fn function_type(name: &str, args: &[ExprType]) -> Result<ExprType, TaqlError> {
    if functions::has_udf(name) {
        return Ok((Kind::Unknown, Shape::Unknown));
    }
    let (first, shape) = args
        .first()
        .copied()
        .unwrap_or((Kind::Unknown, Shape::Unknown));
    let result = match name.to_ascii_lowercase().as_str() {
        "abs" | "norm" => {
            first.require(NUMERIC, name)?;
            (
                if first == Kind::Complex {
                    Kind::Real
                } else {
                    first
                },
                shape,
            )
        }
        "arg" | "imag" => {
            first.require(NUMERIC, name)?;
            (Kind::Real, shape)
        }
        "asin" | "acos" | "atan" | "sin" | "cos" | "tan" | "sinh" | "cosh" | "tanh" | "exp"
        | "log" | "ln" | "log10" | "sqrt" | "conj" => {
            first.require(NUMERIC, name)?;
            (
                if first == Kind::Complex {
                    Kind::Complex
                } else {
                    Kind::Real
                },
                shape,
            )
        }
        "square" | "cube" => {
            first.require(NUMERIC, name)?;
            (first, shape)
        }
        "floor" | "ceil" | "round" | "sign" => {
            first.require(REAL, name)?;
            (first, shape)
        }
        "any" | "all" => {
            first.require(&[Kind::Bool], name)?;
            scalar(Kind::Bool)
        }
        "ntrue" | "nfalse" => {
            first.require(&[Kind::Bool], name)?;
            scalar(Kind::Int)
        }
        "isnan" | "isinf" | "isfinite" => {
            first.require(NUMERIC, name)?;
            (Kind::Bool, shape)
        }
        "real" | "double" | "float" => (Kind::Real, shape),
        "int" => (Kind::Int, shape),
        "bool" => (Kind::Bool, shape),
        "rownumber" | "rownr" | "rowid" | "nelements" | "ndim" => scalar(Kind::Int),
        "date" | "datetime" => (Kind::Date, shape),
        _ => (Kind::Unknown, Shape::Unknown),
    };
    Ok(result)
}
