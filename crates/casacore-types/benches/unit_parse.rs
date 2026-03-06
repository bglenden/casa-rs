// SPDX-License-Identifier: LGPL-3.0-or-later
//! Criterion benchmarks for unit parsing and quantity conversion.

use criterion::{Criterion, criterion_group, criterion_main};

use casacore_types::quanta::{Quantity, Unit};

fn bench_parse_simple(c: &mut Criterion) {
    c.bench_function("parse 'm'", |b| {
        b.iter(|| {
            // Clear cache to measure actual parse cost.
            casacore_types::quanta::registry::global_registry().clear_cache();
            Unit::new("m").unwrap()
        })
    });
}

fn bench_parse_prefixed(c: &mut Criterion) {
    c.bench_function("parse 'km'", |b| {
        b.iter(|| {
            casacore_types::quanta::registry::global_registry().clear_cache();
            Unit::new("km").unwrap()
        })
    });
}

fn bench_parse_compound(c: &mut Criterion) {
    c.bench_function("parse 'kg.m.s-2'", |b| {
        b.iter(|| {
            casacore_types::quanta::registry::global_registry().clear_cache();
            Unit::new("kg.m.s-2").unwrap()
        })
    });
}

fn bench_parse_cached(c: &mut Criterion) {
    // Pre-parse to populate cache.
    let _ = Unit::new("km/s").unwrap();
    c.bench_function("parse 'km/s' (cached)", |b| {
        b.iter(|| Unit::new("km/s").unwrap())
    });
}

fn bench_conversion(c: &mut Criterion) {
    let km = Quantity::new(1.0, "km").unwrap();
    let m_unit = Unit::new("m").unwrap();
    c.bench_function("convert km->m", |b| {
        b.iter(|| km.get_value_in(&m_unit).unwrap())
    });
}

criterion_group!(
    benches,
    bench_parse_simple,
    bench_parse_prefixed,
    bench_parse_compound,
    bench_parse_cached,
    bench_conversion,
);
criterion_main!(benches);
