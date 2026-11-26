// SPDX-License-Identifier: LGPL-3.0-or-later
//! Wave 14 prep: verified upstream LEL inventory snapshot.
//!
//! This freezes the upstream casacore enum inventory that Wave 14 is expected
//! to account for. The lists were checked against:
//! - `casacore/lattices/LEL/LELUnaryEnums.h`
//! - `casacore/lattices/LEL/LELBinaryEnums.h`
//! - `casacore/lattices/LEL/LELFunctionEnums.h`

use std::collections::BTreeSet;

const UPSTREAM_UNARY: &[&str] = &["PLUS", "MINUS", "NOT"];
const SUPPORTED_UNARY: &[&str] = &["MINUS", "NOT"];
const REMAINING_UNARY: &[&str] = &["PLUS"];

const UPSTREAM_BINARY: &[&str] = &[
    "ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "AND", "OR", "EQ", "GT", "GE", "NE",
];
const SUPPORTED_BINARY: &[&str] = &[
    "ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "AND", "OR", "EQ", "GT", "GE", "NE",
];
const REMAINING_BINARY: &[&str] = &[];

const UPSTREAM_FUNCTIONS: &[&str] = &[
    "SIN",
    "SINH",
    "ASIN",
    "COS",
    "COSH",
    "ACOS",
    "TAN",
    "TANH",
    "ATAN",
    "ATAN2",
    "EXP",
    "LOG",
    "LOG10",
    "POW",
    "SQRT",
    "ROUND",
    "SIGN",
    "CEIL",
    "FLOOR",
    "ABS",
    "ARG",
    "REAL",
    "IMAG",
    "CONJ",
    "COMPLEX",
    "FMOD",
    "MIN",
    "MAX",
    "MIN1D",
    "MAX1D",
    "MEAN1D",
    "MEDIAN1D",
    "FRACTILE1D",
    "FRACTILERANGE1D",
    "SUM",
    "NELEM",
    "ALL",
    "ANY",
    "NTRUE",
    "NFALSE",
    "MASK",
    "VALUE",
    "IIF",
    "REPLACE",
    "NDIM",
    "LENGTH",
    "ISNAN",
    "INDEXIN",
];
const SUPPORTED_FUNCTIONS: &[&str] = &[
    "SIN", "SINH", "ASIN", "COS", "COSH", "ACOS", "TAN", "TANH", "ATAN", "ATAN2", "EXP", "LOG",
    "LOG10", "POW", "SQRT", "ROUND", "SIGN", "CEIL", "FLOOR", "ABS", "CONJ", "FMOD", "MIN", "MAX",
];
const REMAINING_FUNCTIONS: &[&str] = &[
    "ARG",
    "REAL",
    "IMAG",
    "COMPLEX",
    "MIN1D",
    "MAX1D",
    "MEAN1D",
    "MEDIAN1D",
    "FRACTILE1D",
    "FRACTILERANGE1D",
    "SUM",
    "NELEM",
    "ALL",
    "ANY",
    "NTRUE",
    "NFALSE",
    "MASK",
    "VALUE",
    "IIF",
    "REPLACE",
    "NDIM",
    "LENGTH",
    "ISNAN",
    "INDEXIN",
];

fn set(values: &[&'static str]) -> BTreeSet<&'static str> {
    values.iter().copied().collect()
}

fn assert_partition(
    upstream: &[&'static str],
    supported: &[&'static str],
    remaining: &[&'static str],
) {
    let upstream = set(upstream);
    let supported = set(supported);
    let remaining = set(remaining);

    assert!(
        supported.is_disjoint(&remaining),
        "supported and remaining inventories must not overlap"
    );

    let union: BTreeSet<_> = supported.union(&remaining).copied().collect();
    assert_eq!(union, upstream, "supported + remaining must equal upstream");
}

#[test]
fn wave14_remaining_inventory_partitions_verified_upstream_lel_surface() {
    assert_eq!(UPSTREAM_UNARY.len(), 3);
    assert_eq!(UPSTREAM_BINARY.len(), 10);
    assert_eq!(UPSTREAM_FUNCTIONS.len(), 48);

    assert_eq!(SUPPORTED_UNARY.len(), 2);
    assert_eq!(SUPPORTED_BINARY.len(), 10);
    assert_eq!(SUPPORTED_FUNCTIONS.len(), 24);

    assert_eq!(REMAINING_UNARY.len(), 1);
    assert_eq!(REMAINING_BINARY.len(), 0);
    assert_eq!(REMAINING_FUNCTIONS.len(), 24);

    assert_partition(UPSTREAM_UNARY, SUPPORTED_UNARY, REMAINING_UNARY);
    assert_partition(UPSTREAM_BINARY, SUPPORTED_BINARY, REMAINING_BINARY);
    assert_partition(UPSTREAM_FUNCTIONS, SUPPORTED_FUNCTIONS, REMAINING_FUNCTIONS);
}
