// SPDX-License-Identifier: LGPL-3.0-or-later
//! Check all scalar operand types before evaluating a row, including branches
//! a Boolean short circuit would skip. CASA builds typed expression nodes.

use casa_tables::taql::ast::{BinaryOp, Expr, InSetElement, Literal, UnaryOp};

use crate::session::error::{SimulatorError, SimulatorResult};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Bool,
    Int,
    Real,
    Complex,
}

fn invalid() -> SimulatorError {
    SimulatorError::refused("Caught exception: invalid selection operand types")
}

fn require(accepts: bool) -> SimulatorResult<()> {
    accepts.then_some(()).ok_or_else(invalid)
}

fn numeric(left: Kind, right: Kind) -> SimulatorResult<Kind> {
    require(left != Kind::Bool && right != Kind::Bool)?;
    Ok(left.max(right))
}

fn comparable(left: Kind, right: Kind) -> SimulatorResult<()> {
    require((left == Kind::Bool) == (right == Kind::Bool))
}

pub(super) fn predicate(expression: &Expr) -> SimulatorResult<()> {
    require(kind(expression)? == Kind::Bool)
}

fn kind(expression: &Expr) -> SimulatorResult<Kind> {
    match expression {
        Expr::Literal(literal) => match literal {
            Literal::Bool(_) => Ok(Kind::Bool),
            Literal::Int(_) => Ok(Kind::Int),
            Literal::Float(_) => Ok(Kind::Real),
            Literal::Complex(_) => Ok(Kind::Complex),
            _ => Err(super::unsupported("this predicate literal")),
        },
        Expr::ColumnRef(column) => Ok(if column.column == "TIME" {
            Kind::Real
        } else {
            Kind::Int
        }),
        Expr::Unary { op, operand } => {
            let value = kind(operand)?;
            require(match op {
                UnaryOp::Negate => value != Kind::Bool,
                UnaryOp::Not => value == Kind::Bool,
                UnaryOp::BitNot => value == Kind::Int,
            })?;
            Ok(value)
        }
        Expr::Binary { op, left, right } => binary(*op, kind(left)?, kind(right)?),
        Expr::FunctionCall { name, args } => {
            let first = kind(&args[0])?;
            require(first != Kind::Bool)?;
            for (index, argument) in args.iter().enumerate().skip(1) {
                let value = kind(argument)?;
                numeric(first, value)?;
                if index == 2 {
                    require(value != Kind::Complex)?;
                }
            }
            Ok(if name.eq_ignore_ascii_case("abs") {
                first.min(Kind::Real)
            } else {
                Kind::Bool
            })
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let value = kind(expr)?;
            let common = numeric(numeric(value, kind(low)?)?, kind(high)?)?;
            if common == Kind::Complex {
                return Err(super::unsupported("complex ordered predicates"));
            }
            Ok(Kind::Bool)
        }
        Expr::In { expr, values, .. } => {
            let value = kind(expr)?;
            for item in values {
                comparable(value, kind(item)?)?;
            }
            Ok(Kind::Bool)
        }
        Expr::InSet { expr, elements, .. } => {
            let value = kind(expr)?;
            for element in elements {
                match element {
                    InSetElement::Value(item) => comparable(value, kind(item)?)?,
                    InSetElement::Range { start, end, step } => {
                        require(value != Kind::Bool && value != Kind::Complex)?;
                        for item in [start, end, step].into_iter().flatten() {
                            require(numeric(value, kind(item)?)? != Kind::Complex)?;
                        }
                    }
                }
            }
            Ok(Kind::Bool)
        }
        Expr::IsNull { expr, .. } => {
            kind(expr)?;
            Ok(Kind::Bool)
        }
        Expr::Like { .. } | Expr::RegexMatch { .. } => Err(invalid()),
        _ => Err(super::unsupported("this predicate expression")),
    }
}

fn binary(op: BinaryOp, left: Kind, right: Kind) -> SimulatorResult<Kind> {
    use BinaryOp::{
        Add, And, BitAnd, BitOr, BitXor, Div, Eq, Ge, Gt, IntDiv, Le, Lt, Modulo, Mul, Ne, Or,
        Power, Sub,
    };
    match op {
        And | Or => {
            require(left == Kind::Bool && right == Kind::Bool)?;
            Ok(Kind::Bool)
        }
        Eq | Ne => {
            comparable(left, right)?;
            Ok(Kind::Bool)
        }
        Lt | Le | Gt | Ge => {
            if numeric(left, right)? == Kind::Complex {
                return Err(super::unsupported("complex ordered predicates"));
            }
            Ok(Kind::Bool)
        }
        BitAnd | BitOr | BitXor => {
            require(left == Kind::Int && right == Kind::Int)?;
            Ok(Kind::Int)
        }
        Add | Sub | Mul => numeric(left, right),
        Div | Power => Ok(numeric(left, right)?.max(Kind::Real)),
        IntDiv | Modulo => {
            let result = numeric(left, right)?;
            require(result != Kind::Complex)?;
            Ok(if op == IntDiv { Kind::Real } else { result })
        }
    }
}
