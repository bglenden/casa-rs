// SPDX-License-Identifier: LGPL-3.0-or-later
//! A keyword array is written as the logical array it is, whatever its
//! memory layout (#952).
//!
//! casacore stores an array's elements with the first axis fastest, and the
//! keyword reader rebuilds them so. An array held in C layout, or a strided
//! view of one, must reach the disk in that order too, not in its memory
//! order, so that it reads back, here and in casacore, with its element
//! (i, j) where it was. `casa-test-support`'s
//! `tables_keyword_array_interop.rs` checks the casacore side.

use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, PrimitiveType, Value};
use ndarray::{ArrayD, Dimension, IxDyn, ShapeBuilder, s};

/// The logical element at `index`, `i + 10 j + 100 k`: each distinct.
fn element(index: &[usize]) -> f64 {
    index
        .iter()
        .rev()
        .fold(0.0, |value, &i| 10.0 * value + i as f64)
}

/// `shape`'s array of [`element`]s, in C layout or Fortran layout.
fn array(shape: &[usize], fortran: bool) -> ArrayD<f64> {
    let at = |index: IxDyn| element(index.slice());
    if fortran {
        ArrayD::from_shape_fn(IxDyn(shape).f(), at)
    } else {
        ArrayD::from_shape_fn(IxDyn(shape), at)
    }
}

#[test]
fn keyword_arrays_read_back_as_written_whatever_their_layout() {
    let c_layout = array(&[2, 3], false);
    let fortran = array(&[2, 3], true);
    // Every other column of a 2 × 6 array whose element (i, 2j) is (i, j)'s.
    let wide = ArrayD::from_shape_fn(IxDyn(&[2, 6]), |index| {
        element(&[index[0], index[1] / 2]) + if index[1] % 2 == 1 { 100.0 } else { 0.0 }
    });
    let strided = wide.slice_move(s![.., ..;2]).into_dyn();
    assert!(strided.as_slice_memory_order().is_none(), "a strided view");
    let cube = array(&[2, 3, 4], false);
    let names = [
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
    for (name, value) in names {
        table
            .keywords_mut()
            .upsert(name, Value::Array(ArrayValue::Float64(value.clone())));
    }
    table.save(TableOptions::new(&path)).expect("save");

    let table = Table::open(TableOptions::new(&path)).expect("open");
    for (name, written) in names {
        let Some(Value::Array(ArrayValue::Float64(read))) = table.keywords().get(name) else {
            panic!("{name} is a Float64 array keyword");
        };
        assert_eq!(read.shape(), written.shape(), "{name}");
        for (index, value) in written.indexed_iter() {
            assert_eq!(read[&index], *value, "{name} at {:?}", index.slice());
        }
    }
}
