// SPDX-License-Identifier: LGPL-3.0-or-later
//! Performance comparison for MeasurementSet create/open/read workloads.

mod common;

use std::time::Instant;

use casacore_ms::builder::MeasurementSetBuilder;
use casacore_ms::ms::MeasurementSet;
use casacore_ms::{OptionalMainColumn, VisibilityDataColumn};
use casacore_test_support::cpp_backend_available;
use casacore_test_support::ms_interop::cpp_ms_bench_create_open;
use casacore_types::ArrayValue;
use common::{populate_main_rows, populate_subtables};

#[test]
fn ms_create_open_read_perf_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping ms_perf_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: usize = 2_048;

    let dir = tempfile::tempdir().expect("create temp dir");
    let cpp_path = dir.path().join("cpp_perf.ms");
    let rust_path = dir.path().join("rust_perf.ms");

    let cpp = cpp_ms_bench_create_open(&cpp_path, NROWS as u64)
        .expect("C++ MeasurementSet benchmark should succeed");

    let t0 = Instant::now();
    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let mut ms = MeasurementSet::create(&rust_path, builder).unwrap();
    populate_subtables(&mut ms);
    populate_main_rows(&mut ms, NROWS);
    ms.save().unwrap();
    let rust_create_ns = t0.elapsed().as_nanos() as u64;

    let t0 = Instant::now();
    let ms = MeasurementSet::open(&rust_path).unwrap();
    let rust_open_ns = t0.elapsed().as_nanos() as u64;

    let data_col = ms.data_column(VisibilityDataColumn::Data).unwrap();
    let mut sink = 0.0f32;
    let t0 = Instant::now();
    for row in 0..NROWS {
        match data_col.get(row).unwrap() {
            ArrayValue::Complex32(arr) => sink += arr[[0, 0]].re,
            other => panic!(
                "expected Complex32 DATA column, got {:?}",
                other.primitive_type()
            ),
        }
    }
    let rust_read_ns = t0.elapsed().as_nanos() as u64;

    assert!(sink >= 0.0);

    let create_ratio = rust_create_ns as f64 / cpp.create_ns.max(1) as f64;
    let open_ratio = rust_open_ns as f64 / cpp.open_ns.max(1) as f64;
    let read_ratio = rust_read_ns as f64 / cpp.read_ns.max(1) as f64;

    eprintln!(
        "MeasurementSet perf ({NROWS} rows, DATA[{}/{}]):\n  \
         Create+save: C++ {:.1} ms, Rust {:.1} ms, ratio {create_ratio:.1}x\n  \
         Open:        C++ {:.1} ms, Rust {:.1} ms, ratio {open_ratio:.1}x\n  \
         Read DATA:   C++ {:.1} ms, Rust {:.1} ms, ratio {read_ratio:.1}x",
        common::NUM_CORR,
        common::NUM_CHAN,
        cpp.create_ns as f64 / 1e6,
        rust_create_ns as f64 / 1e6,
        cpp.open_ns as f64 / 1e6,
        rust_open_ns as f64 / 1e6,
        cpp.read_ns as f64 / 1e6,
        rust_read_ns as f64 / 1e6,
    );

    let max_ratio = create_ratio.max(open_ratio).max(read_ratio);
    if max_ratio > 2.0 {
        eprintln!("  warning: Rust MeasurementSet workload is {max_ratio:.1}x slower than C++");
    }
}
