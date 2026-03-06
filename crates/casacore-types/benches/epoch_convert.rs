// SPDX-License-Identifier: LGPL-3.0-or-later
//! Criterion benchmarks for epoch conversions.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use casacore_types::measures::{EpochRef, MEpoch, MeasFrame};

const J2000_MJD: f64 = 51544.5;

fn bench_utc_to_tai(c: &mut Criterion) {
    let frame = MeasFrame::new();
    c.bench_function("epoch UTC→TAI", |b| {
        b.iter(|| {
            let epoch = MEpoch::from_mjd(black_box(J2000_MJD), EpochRef::UTC);
            epoch.convert_to(EpochRef::TAI, &frame).unwrap()
        })
    });
}

fn bench_utc_to_tt(c: &mut Criterion) {
    let frame = MeasFrame::new();
    c.bench_function("epoch UTC→TT", |b| {
        b.iter(|| {
            let epoch = MEpoch::from_mjd(black_box(J2000_MJD), EpochRef::UTC);
            epoch.convert_to(EpochRef::TT, &frame).unwrap()
        })
    });
}

fn bench_tt_to_tdb(c: &mut Criterion) {
    let frame = MeasFrame::new();
    c.bench_function("epoch TT→TDB", |b| {
        b.iter(|| {
            let epoch = MEpoch::from_mjd(black_box(J2000_MJD), EpochRef::TT);
            epoch.convert_to(EpochRef::TDB, &frame).unwrap()
        })
    });
}

fn bench_utc_to_tdb(c: &mut Criterion) {
    let frame = MeasFrame::new();
    c.bench_function("epoch UTC→TDB (chained)", |b| {
        b.iter(|| {
            let epoch = MEpoch::from_mjd(black_box(J2000_MJD), EpochRef::UTC);
            epoch.convert_to(EpochRef::TDB, &frame).unwrap()
        })
    });
}

criterion_group!(
    benches,
    bench_utc_to_tai,
    bench_utc_to_tt,
    bench_tt_to_tdb,
    bench_utc_to_tdb,
);
criterion_main!(benches);
