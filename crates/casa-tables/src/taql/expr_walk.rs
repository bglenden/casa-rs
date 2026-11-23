// SPDX-License-Identifier: LGPL-3.0-or-later
//! Child traversal for TaQL expressions prepared against a table.

use super::ast::{Expr, InSetElement, IndexElement};

impl Expr {
    /// Visits the immediate expression operands in evaluation order.
    ///
    /// A subquery has its own statement and table context. A bound lookup
    /// visits its row index and keeps the prepared column and diagnostic intact.
    ///
    /// # Errors
    ///
    /// Propagates the first error returned by `visit`.
    pub fn try_for_each_child_mut<E>(
        &mut self,
        visit: &mut impl FnMut(&mut Expr) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Expr::Unary { operand, .. }
            | Expr::IsNull { expr: operand, .. }
            | Expr::Aggregate { arg: operand, .. }
            | Expr::BoundRowLookup { index: operand, .. } => visit(operand),
            Expr::Binary { left, right, .. }
            | Expr::Like {
                expr: left,
                pattern: right,
                ..
            }
            | Expr::RegexMatch {
                expr: left,
                pattern: right,
                ..
            } => {
                visit(left)?;
                visit(right)
            }
            Expr::FunctionCall { args, .. } | Expr::ArrayLiteral(args) => {
                args.iter_mut().try_for_each(visit)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                visit(expr)?;
                visit(low)?;
                visit(high)
            }
            Expr::In { expr, values, .. } => {
                visit(expr)?;
                values.iter_mut().try_for_each(visit)
            }
            Expr::InSet { expr, elements, .. } => {
                visit(expr)?;
                for element in elements {
                    match element {
                        InSetElement::Value(value) => visit(value)?,
                        InSetElement::Range { start, end, step } => {
                            [start, end, step]
                                .into_iter()
                                .filter_map(Option::as_mut)
                                .try_for_each(&mut *visit)?;
                        }
                    }
                }
                Ok(())
            }
            Expr::ArrayIndex { array, indices } => {
                visit(array)?;
                for element in indices {
                    match element {
                        IndexElement::Single(index) => visit(index)?,
                        IndexElement::Slice { start, end, step } => {
                            [start, end, step]
                                .into_iter()
                                .filter_map(Option::as_mut)
                                .try_for_each(&mut *visit)?;
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
