// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scalar group identities use numeric equality, including signed zero.
use super::{ExprValue, SimulatorResult, error};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum GroupKey {
    Bool(bool),
    Integer(i64),
    Real(u64),
    Date(u64),
    String(String),
}

impl GroupKey {
    pub(super) fn new(value: ExprValue) -> SimulatorResult<Self> {
        Ok(match value {
            ExprValue::Bool(value) => Self::Bool(value),
            ExprValue::Int(value) => Self::Integer(value),
            ExprValue::Float(value) => {
                if value.fract() == 0. && value >= i64::MIN as f64 && value < -(i64::MIN as f64) {
                    Self::Integer(value as i64)
                } else {
                    Self::Real(value.to_bits())
                }
            }
            ExprValue::DateTime(value) => Self::Date(if value == 0. { 0 } else { value.to_bits() }),
            ExprValue::String(value) => Self::String(value),
            _ => {
                return Err(error(
                    "GROUPBY keys must be scalar Boolean, real, date or string",
                ));
            }
        })
    }
}
