// SPDX-License-Identifier: LGPL-3.0-or-later
//! Domain value representation for `imhead` header-editing operations.

use casa_core::{ArrayValue, RecordValue};

/// Domain-level value passed to `imhead` put/add/del metadata operations.
#[derive(Debug, Clone, PartialEq)]
pub enum ImheadValue {
    /// Text string value.
    String(String),
    /// 32-bit signed integer value (clamped to CASA's `Int` range by callers).
    Int(i32),
    /// 64-bit floating-point value.
    Float(f64),
    /// Boolean value.
    Bool(bool),
    /// Rectangular casacore array value.
    Array(ArrayValue),
    /// Casacore record value.
    Record(RecordValue),
    /// Homogeneous or mixed list (used for multi-value keys like `stokes`).
    List(Vec<ImheadValue>),
    /// Any other parameter type rejected by `imhead`.
    Unsupported,
}
