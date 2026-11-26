// SPDX-License-Identifier: LGPL-3.0-or-later
//! A TaQL expression bound to a row computation for a particular table.

use std::fmt;
use std::sync::Arc;

use super::TaqlError;
use super::eval::{EvalContext, ExprValue};

type EvaluateRow = dyn Fn(&EvalContext<'_>) -> Result<ExprValue, TaqlError> + Send + Sync;

/// A row computation with table metadata prepared once for the query.
///
/// Cloning shares the prepared function and its metadata. Independently
/// prepared functions compare unequal because they may refer to different tables.
#[derive(Clone)]
pub struct BoundRowFunction {
    expression: Arc<str>,
    evaluate: Arc<EvaluateRow>,
}

impl BoundRowFunction {
    /// Binds a computation that receives each row's normal evaluation context.
    pub fn new(
        expression: impl Into<Arc<str>>,
        evaluate: impl Fn(&EvalContext<'_>) -> Result<ExprValue, TaqlError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            expression: expression.into(),
            evaluate: Arc::new(evaluate),
        }
    }

    pub(super) fn evaluate(&self, context: &EvalContext<'_>) -> Result<ExprValue, TaqlError> {
        (self.evaluate)(context)
    }
}

impl PartialEq for BoundRowFunction {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.evaluate, &other.evaluate)
    }
}

impl fmt::Debug for BoundRowFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BoundRowFunction")
            .field(&self.expression)
            .finish()
    }
}

impl fmt::Display for BoundRowFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.expression)
    }
}
