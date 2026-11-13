// SPDX-License-Identifier: LGPL-3.0-or-later
//! Criterion benchmarks for epoch conversions.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use casa_types::measures::baseline::local_apparent_sidereal_time;
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};

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

fn bench_simulation_sidereal(c: &mut Criterion) {
    let frames = [
        57_754.635_968_981_14,
        57_560.294_291_105_62,
        60_689.662_819_250_89,
        57_202.191_368_921_41,
        56_657.722_561_864_83,
    ]
    .map(|epoch| {
        MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(epoch, EpochRef::UT1))
            .with_position(MPosition::new_itrf(
                2_225_142.18,
                -5_440_307.37,
                -2_481_029.85,
            ))
    });
    c.bench_function("simulation LAST five epochs", |b| {
        b.iter(|| {
            for frame in &frames {
                black_box(local_apparent_sidereal_time(black_box(frame)).unwrap());
            }
        })
    });
}

criterion_group!(
    benches,
    bench_utc_to_tai,
    bench_utc_to_tt,
    bench_tt_to_tdb,
    bench_utc_to_tdb,
    bench_simulation_sidereal,
);
criterion_main!(benches);
