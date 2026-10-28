// SPDX-License-Identifier: LGPL-3.0-or-later
//! Array table keywords written by casa-rs, read by C++ casacore with each
//! element where casa-rs put it, whatever the array's memory layout (#952).
//!
//! casacore stores an array with its first axis fastest. casa-rs writes a
//! keyword array held in C layout, or as a strided view, in that order too,
//! so casacore's element (i, j) is the array's.
//!
//! These tests are skipped when `pkg-config casacore` is not available.

#![cfg(feature = "cpp-interop-tests")]

use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_test_support::{TableOracle, casacore_oracle_available};
use casa_types::{ArrayValue, PrimitiveType, Value};
use ndarray::{ArrayD, Dimension, IxDyn, ShapeBuilder, s};

/// The logical element at `index`, `i + 10 j + 100 k`: each distinct.
fn element(index: &[usize]) -> f64 {
    index
        .iter()
        .rev()
        .fold(0.0, |value, &i| 10.0 * value + i as f64)
}

/// `array`'s elements with the first axis fastest, by logical index.
fn first_axis_fastest(array: &ArrayD<f64>) -> Vec<f64> {
    array.t().iter().copied().collect()
}

#[test]
fn casacore_reads_keyword_arrays_as_written_whatever_their_layout() {
    if !casacore_oracle_available() {
        eprintln!("skipping keyword array test: C++ casacore not available");
        return;
    }
    let at = |index: IxDyn| element(index.slice());
    let c_layout = ArrayD::from_shape_fn(IxDyn(&[2, 3]), at);
    let fortran = ArrayD::from_shape_fn(IxDyn(&[2, 3]).f(), at);
    let wide = ArrayD::from_shape_fn(IxDyn(&[2, 6]), |index: IxDyn| {
        element(&[index[0], index[1] / 2]) + if index[1] % 2 == 1 { 100.0 } else { 0.0 }
    });
    let strided = wide.slice_move(s![.., ..;2]).into_dyn();
    let cube = ArrayD::from_shape_fn(IxDyn(&[2, 3, 4]), at);
    let keywords = [
        ("C_LAYOUT", &c_layout),
        ("FORTRAN", &fortran),
        ("STRIDED", &strided),
        ("CUBE", &cube),
    ];

    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("keywords.tbl");
    let schema = TableSchema::new(vec![ColumnSchema::scalar("TIME", PrimitiveType::Float64)])
        .expect("schema");
    let mut table = Table::with_schema(schema);
    for (name, value) in keywords {
        table
            .keywords_mut()
            .upsert(name, Value::Array(ArrayValue::Float64(value.clone())));
    }
    table.save(TableOptions::new(&path)).expect("save");

    for (name, written) in keywords {
        let (shape, stored) = TableOracle::keyword_double_array(&path, name)
            .unwrap_or_else(|error| panic!("C++ reads {name}: {error}"));
        assert_eq!(shape, written.shape(), "{name}");
        assert_eq!(stored, first_axis_fastest(written), "{name}");
    }
}
