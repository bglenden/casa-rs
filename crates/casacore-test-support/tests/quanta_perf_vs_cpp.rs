// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rust-vs-C++ performance comparison for quanta unit parsing and conversion.
//!
//! Use `cargo test --release` for meaningful ratios. The 2x threshold triggers
//! a warning (not a hard failure) so CI captures the ratio.

#![cfg(has_casacore_cpp)]

use casacore_test_support::quanta_interop::{cpp_bench_convert, cpp_bench_parse};
use casacore_types::quanta::{Quantity, Unit};
use std::time::Instant;

const ITERATIONS: i32 = 10_000;

/// Representative unit strings covering base, derived, compound, and prefixed units.
const BENCH_UNITS: &[&str] = &[
    "m", "kg", "s", "Hz", "Jy", "deg", "km/s", "W/m2/Hz", "kg.m.s-2", "MHz", "mJy", "arcsec", "AU",
    "pc", "J/K/mol", "eV",
];

#[test]
fn parse_throughput_vs_cpp() {
    // ── C++ timing ──
    let cpp_ns = cpp_bench_parse(BENCH_UNITS, ITERATIONS).expect("C++ bench_parse should succeed");

    // ── Rust timing ──
    // Warm up: parse all units once so the registry is populated
    for unit in BENCH_UNITS {
        let _ = Unit::new(unit).unwrap();
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for unit in BENCH_UNITS {
            let u = Unit::new(unit).unwrap();
            std::hint::black_box(&u);
        }
    }
    let rust_ns = start.elapsed().as_nanos() as u64;

    let total_ops = (ITERATIONS as u64) * (BENCH_UNITS.len() as u64);
    let rust_per_op = rust_ns as f64 / total_ops as f64;
    let cpp_per_op = cpp_ns as f64 / total_ops as f64;
    let ratio = rust_per_op / cpp_per_op;

    eprintln!("── parse throughput ──");
    eprintln!("  units:      {}", BENCH_UNITS.len());
    eprintln!("  iterations: {ITERATIONS}");
    eprintln!("  total ops:  {total_ops}");
    eprintln!("  Rust:       {rust_ns:>12} ns  ({rust_per_op:.1} ns/op)");
    eprintln!("  C++:        {cpp_ns:>12} ns  ({cpp_per_op:.1} ns/op)");
    eprintln!("  ratio:      {ratio:.2}x");

    if ratio > 2.0 {
        eprintln!("  ⚠ WARNING: Rust parse is {ratio:.1}x slower than C++ (threshold: 2.0x)");
    }
}

#[test]
fn conversion_throughput_vs_cpp() {
    let convert_pairs: &[(f64, &str, &str)] = &[
        (1.0, "km", "m"),
        (100.0, "MHz", "Hz"),
        (1.0, "Jy", "W/m2/Hz"),
        (45.0, "deg", "rad"),
        (1.0, "AU", "m"),
        (1.0, "pc", "m"),
        (1.0, "eV", "J"),
        (1.0, "atm", "Pa"),
    ];

    // ── C++ timing ──
    let mut cpp_total_ns = 0u64;
    for &(value, from, to) in convert_pairs {
        let ns = cpp_bench_convert(value, from, to, ITERATIONS)
            .unwrap_or_else(|| panic!("C++ bench_convert failed for {from}->{to}"));
        cpp_total_ns += ns;
    }

    // ── Rust timing ──
    // Pre-parse units outside the timing loop (matches C++ which constructs once).
    let pre_parsed: Vec<(Quantity, Unit)> = convert_pairs
        .iter()
        .map(|&(value, from, to)| (Quantity::new(value, from).unwrap(), Unit::new(to).unwrap()))
        .collect();

    // Warm up
    for (q, target) in &pre_parsed {
        std::hint::black_box(q.get_value_in(target).unwrap());
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for (q, target) in &pre_parsed {
            let v = q.get_value_in(target).unwrap();
            std::hint::black_box(v);
        }
    }
    let rust_total_ns = start.elapsed().as_nanos() as u64;

    let total_ops = (ITERATIONS as u64) * (convert_pairs.len() as u64);
    let rust_per_op = rust_total_ns as f64 / total_ops as f64;
    let cpp_per_op = cpp_total_ns as f64 / total_ops as f64;
    let ratio = rust_per_op / cpp_per_op;

    eprintln!("── conversion throughput ──");
    eprintln!("  pairs:      {}", convert_pairs.len());
    eprintln!("  iterations: {ITERATIONS}");
    eprintln!("  total ops:  {total_ops}");
    eprintln!("  Rust:       {rust_total_ns:>12} ns  ({rust_per_op:.1} ns/op)");
    eprintln!("  C++:        {cpp_total_ns:>12} ns  ({cpp_per_op:.1} ns/op)");
    eprintln!("  ratio:      {ratio:.2}x");

    if ratio > 2.0 {
        eprintln!("  ⚠ WARNING: Rust conversion is {ratio:.1}x slower than C++ (threshold: 2.0x)");
    }
}
