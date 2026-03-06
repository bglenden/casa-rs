// SPDX-License-Identifier: LGPL-3.0-or-later
//! Performance comparison: Rust TaQL meas.* UDFs vs C++ direct measure conversions.
//!
//! These tests benchmark measure conversions called N times through the Rust
//! TaQL `meas.*` UDF layer and compare against C++ casacore's batch conversion
//! benchmark functions. Only runs in release mode.
#![cfg(has_casacore_cpp)]

use std::time::Instant;

use casacore_tables::taql::ast::IndexStyle;
use casacore_tables::taql::eval::{EvalContext, ExprValue};
use casacore_tables::taql::functions::call_function;
use casacore_test_support::measures_interop::{
    cpp_bench_direction_convert, cpp_bench_epoch_convert,
};
use casacore_types::RecordValue;

fn skip_unless_release() -> bool {
    cfg!(debug_assertions)
}

fn s(val: &str) -> ExprValue {
    ExprValue::String(val.to_string())
}
fn fl(val: f64) -> ExprValue {
    ExprValue::Float(val)
}

fn eval_meas(name: &str, args: &[ExprValue]) -> ExprValue {
    let dummy_row = RecordValue::new(vec![]);
    let ctx = EvalContext {
        row: &dummy_row,
        row_index: 0,
        style: IndexStyle::default(),
    };
    call_function(name, args, &ctx).unwrap()
}

#[test]
fn perf_epoch_conversion() {
    if skip_unless_release() {
        eprintln!("[perf] skipping meas epoch perf in debug mode");
        return;
    }
    let count: i32 = 10_000;
    let iterations: i32 = 5;

    // Rust: call meas.epoch N times
    let mut rust_times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        for i in 0..count {
            let mjd = 51544.5 + f64::from(i) * 0.001;
            let _ = eval_meas("meas.epoch", &[s("TAI"), fl(mjd)]);
        }
        rust_times.push(start.elapsed().as_nanos() as u64);
    }
    rust_times.sort_unstable();
    let rust_ns = rust_times[rust_times.len() / 2];

    // C++: batch epoch conversion
    let cpp_ns = cpp_bench_epoch_convert(51544.5, count, "UTC", "TAI", iterations)
        .expect("C++ bench failed");

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "[perf] epoch: Rust={:.2}ms  C++={:.2}ms  ratio={:.2}x",
        rust_ns as f64 / 1e6,
        cpp_ns as f64 / 1e6,
        ratio,
    );

    // Allow up to 5x overhead (Rust is pure, not using cached conversion engine)
    assert!(
        ratio <= 5.0,
        "epoch perf: Rust/C++ ratio {ratio:.2}x exceeds 5.0x threshold"
    );
}

#[test]
fn perf_direction_conversion() {
    if skip_unless_release() {
        eprintln!("[perf] skipping meas direction perf in debug mode");
        return;
    }
    let count: i32 = 10_000;
    let iterations: i32 = 5;

    // Rust: call meas.dir N times
    let mut rust_times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        for i in 0..count {
            let lon = f64::from(i) * 0.001;
            let _ = eval_meas("meas.dir", &[s("GALACTIC"), fl(lon), fl(0.5), s("J2000")]);
        }
        rust_times.push(start.elapsed().as_nanos() as u64);
    }
    rust_times.sort_unstable();
    let rust_ns = rust_times[rust_times.len() / 2];

    // C++: batch direction conversion
    let cpp_ns = cpp_bench_direction_convert(
        0.0, 0.5, count, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0, iterations,
    )
    .expect("C++ bench failed");

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "[perf] direction: Rust={:.2}ms  C++={:.2}ms  ratio={:.2}x",
        rust_ns as f64 / 1e6,
        cpp_ns as f64 / 1e6,
        ratio,
    );

    // Allow up to 5x overhead
    assert!(
        ratio <= 5.0,
        "direction perf: Rust/C++ ratio {ratio:.2}x exceeds 5.0x threshold"
    );
}
