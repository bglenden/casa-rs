// SPDX-License-Identifier: LGPL-3.0-or-later
//! Performance comparison: Rust measures vs C++ casacore.
#![cfg(has_casacore_cpp)]

use casacore_test_support::measures_interop::{
    cpp_bench_direction_convert, cpp_bench_doppler_convert, cpp_bench_epoch_convert,
    cpp_bench_frequency_convert, cpp_bench_position_convert, cpp_bench_radvel_convert,
};
use casacore_types::measures::direction::{DirectionRef, MDirection};
use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};
use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casacore_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame, PositionRef};

const J2000_MJD: f64 = 51544.5;
const COUNT: i32 = 10_000;
const ITERATIONS: i32 = 1;

fn bench_epoch(ref_in: EpochRef, ref_out: EpochRef) {
    let frame = MeasFrame::new();
    let ref_in_str = ref_in.as_str();
    let ref_out_str = ref_out.as_str();

    // --- Rust ---
    let rust_start = std::time::Instant::now();
    for i in 0..COUNT {
        let mjd = J2000_MJD + (i as f64) * 0.001;
        let epoch = MEpoch::from_mjd(mjd, ref_in);
        let result = epoch.convert_to(ref_out, &frame).unwrap();
        std::hint::black_box(&result);
    }
    let rust_ns = rust_start.elapsed().as_nanos() as u64;

    // --- C++ ---
    let cpp_ns =
        cpp_bench_epoch_convert(J2000_MJD, COUNT, ref_in_str, ref_out_str, ITERATIONS).unwrap();

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "Epoch {ref_in_str}→{ref_out_str} ({COUNT} conversions): \
         Rust={rust_ns}ns, C++={cpp_ns}ns, ratio={ratio:.2}x"
    );

    if ratio > 2.0 {
        eprintln!("WARNING: Rust is >2x slower than C++ for {ref_in_str}→{ref_out_str}");
    }
}

fn bench_position(ref_in: PositionRef, ref_out: PositionRef) {
    let count = COUNT;
    let ref_in_str = ref_in.as_str();
    let ref_out_str = ref_out.as_str();

    // Starting values depend on direction
    let (x_start, y, z) = match ref_in {
        PositionRef::ITRF => (-1601185.4, -5041977.5, 3554875.9),
        PositionRef::WGS84 => (-107.6_f64.to_radians(), 34.1_f64.to_radians(), 2124.0),
    };

    // --- Rust ---
    let rust_start = std::time::Instant::now();
    for i in 0..count {
        let v0 = x_start
            + (i as f64)
                * if ref_in == PositionRef::ITRF {
                    1.0
                } else {
                    1e-7
                };
        let pos = match ref_in {
            PositionRef::ITRF => MPosition::new_itrf(v0, y, z),
            PositionRef::WGS84 => MPosition::new_wgs84(v0, y, z),
        };
        let result = pos.convert_to(ref_out).unwrap();
        std::hint::black_box(&result);
    }
    let rust_ns = rust_start.elapsed().as_nanos() as u64;

    // --- C++ ---
    let cpp_ns =
        cpp_bench_position_convert(x_start, y, z, count, ref_in_str, ref_out_str, ITERATIONS)
            .unwrap();

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "Position {ref_in_str}→{ref_out_str} ({count} conversions): \
         Rust={rust_ns}ns, C++={cpp_ns}ns, ratio={ratio:.2}x"
    );

    if ratio > 2.0 {
        eprintln!("WARNING: Rust is >2x slower than C++ for {ref_in_str}→{ref_out_str}");
    }
}

// ---------------------------------------------------------------------------
// Epoch perf tests
// ---------------------------------------------------------------------------

#[test]
fn perf_epoch_utc_to_tai() {
    bench_epoch(EpochRef::UTC, EpochRef::TAI);
}

#[test]
fn perf_epoch_utc_to_tt() {
    bench_epoch(EpochRef::UTC, EpochRef::TT);
}

#[test]
fn perf_epoch_tt_to_tdb() {
    bench_epoch(EpochRef::TT, EpochRef::TDB);
}

#[test]
fn perf_epoch_utc_to_tdb() {
    bench_epoch(EpochRef::UTC, EpochRef::TDB);
}

#[test]
fn perf_epoch_utc_to_tcb() {
    bench_epoch(EpochRef::UTC, EpochRef::TCB);
}

#[test]
fn perf_epoch_roundtrip_utc_tai() {
    let frame = MeasFrame::new();

    // --- Rust roundtrip ---
    let rust_start = std::time::Instant::now();
    for i in 0..COUNT {
        let mjd = J2000_MJD + (i as f64) * 0.001;
        let epoch = MEpoch::from_mjd(mjd, EpochRef::UTC);
        let tai = epoch.convert_to(EpochRef::TAI, &frame).unwrap();
        let back = tai.convert_to(EpochRef::UTC, &frame).unwrap();
        std::hint::black_box(&back);
    }
    let rust_ns = rust_start.elapsed().as_nanos() as u64;

    // --- C++ roundtrip: UTC→TAI + TAI→UTC ---
    let cpp_ns_fwd = cpp_bench_epoch_convert(J2000_MJD, COUNT, "UTC", "TAI", ITERATIONS).unwrap();
    let cpp_ns_rev = cpp_bench_epoch_convert(J2000_MJD, COUNT, "TAI", "UTC", ITERATIONS).unwrap();
    let cpp_ns = cpp_ns_fwd + cpp_ns_rev;

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "Epoch UTC↔TAI roundtrip ({COUNT} roundtrips): \
         Rust={rust_ns}ns, C++={cpp_ns}ns, ratio={ratio:.2}x"
    );
}

// ---------------------------------------------------------------------------
// Position perf tests
// ---------------------------------------------------------------------------

#[test]
fn perf_position_itrf_to_wgs84() {
    bench_position(PositionRef::ITRF, PositionRef::WGS84);
}

#[test]
fn perf_position_wgs84_to_itrf() {
    bench_position(PositionRef::WGS84, PositionRef::ITRF);
}

// ---------------------------------------------------------------------------
// Doppler perf tests
// ---------------------------------------------------------------------------

fn bench_doppler(ref_in: DopplerRef, ref_out: DopplerRef) {
    let frame = MeasFrame::new();
    let ref_in_str = ref_in.as_str();
    let ref_out_str = ref_out.as_str();

    let rust_start = std::time::Instant::now();
    for i in 0..COUNT {
        let v = 0.1 + (i as f64) * 0.001;
        let d = MDoppler::new(v, ref_in);
        let result = d.convert_to(ref_out, &frame).unwrap();
        std::hint::black_box(&result);
    }
    let rust_ns = rust_start.elapsed().as_nanos() as u64;

    let cpp_ns =
        cpp_bench_doppler_convert(0.1, COUNT, ref_in_str, ref_out_str, ITERATIONS).unwrap();

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "Doppler {ref_in_str}→{ref_out_str} ({COUNT} conversions): \
         Rust={rust_ns}ns, C++={cpp_ns}ns, ratio={ratio:.2}x"
    );
    if ratio > 2.0 {
        eprintln!("WARNING: Rust is >2x slower than C++ for Doppler {ref_in_str}→{ref_out_str}");
    }
}

#[test]
fn perf_doppler_radio_to_z() {
    bench_doppler(DopplerRef::RADIO, DopplerRef::Z);
}

#[test]
fn perf_doppler_beta_to_gamma() {
    bench_doppler(DopplerRef::BETA, DopplerRef::GAMMA);
}

// ---------------------------------------------------------------------------
// Direction perf tests
// ---------------------------------------------------------------------------

fn bench_direction(ref_in: DirectionRef, ref_out: DirectionRef, epoch_mjd: f64) {
    let ref_in_str = ref_in.as_str();
    let ref_out_str = ref_out.as_str();

    // ITRF needs position and dUT1; other conversions just need epoch
    let needs_position = ref_out == DirectionRef::ITRF || ref_in == DirectionRef::ITRF;
    let (obs_lon, obs_lat, obs_h) = if needs_position {
        (-1.878_283_2, 0.595_370_3, 2124.0) // VLA
    } else {
        (0.0, 0.0, 0.0)
    };

    let frame = if epoch_mjd != 0.0 {
        let mut f = MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC))
            .with_bundled_eop();
        if needs_position {
            f = f.with_position(MPosition::new_wgs84(obs_lon, obs_lat, obs_h));
        }
        f
    } else {
        MeasFrame::new()
    };

    let rust_start = std::time::Instant::now();
    for i in 0..COUNT {
        let lon = 1.0 + (i as f64) * 0.001;
        let d = MDirection::from_angles(lon, 0.5, ref_in);
        let result = d.convert_to(ref_out, &frame).unwrap();
        std::hint::black_box(&result);
    }
    let rust_ns = rust_start.elapsed().as_nanos() as u64;

    let cpp_ns = cpp_bench_direction_convert(
        1.0,
        0.5,
        COUNT,
        ref_in_str,
        ref_out_str,
        epoch_mjd,
        obs_lon,
        obs_lat,
        obs_h,
        ITERATIONS,
    )
    .unwrap();

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "Direction {ref_in_str}→{ref_out_str} ({COUNT} conversions): \
         Rust={rust_ns}ns, C++={cpp_ns}ns, ratio={ratio:.2}x"
    );
    if ratio > 2.0 {
        eprintln!("WARNING: Rust is >2x slower than C++ for Direction {ref_in_str}→{ref_out_str}");
    }
}

#[test]
fn perf_direction_j2000_to_galactic() {
    bench_direction(DirectionRef::J2000, DirectionRef::GALACTIC, 0.0);
}

#[test]
fn perf_direction_j2000_to_jmean() {
    bench_direction(DirectionRef::J2000, DirectionRef::JMEAN, J2000_MJD);
}

// ---------------------------------------------------------------------------
// Frequency perf tests
// ---------------------------------------------------------------------------

fn bench_frequency(ref_in: FrequencyRef, ref_out: FrequencyRef) {
    let ref_in_str = ref_in.as_str();
    let ref_out_str = ref_out.as_str();
    let dir_lon = 0.185_948_8;
    let dir_lat = 0.722_777_4;

    let dir = MDirection::from_angles(dir_lon, dir_lat, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);

    let rust_start = std::time::Instant::now();
    for i in 0..COUNT {
        let hz = 1.42e9 + (i as f64) * 1e3;
        let f = MFrequency::new(hz, ref_in);
        let result = f.convert_to(ref_out, &frame).unwrap();
        std::hint::black_box(&result);
    }
    let rust_ns = rust_start.elapsed().as_nanos() as u64;

    let cpp_ns = cpp_bench_frequency_convert(
        1.42e9,
        COUNT,
        ref_in_str,
        ref_out_str,
        dir_lon,
        dir_lat,
        "J2000",
        J2000_MJD,
        0.0,
        0.0,
        0.0,
        ITERATIONS,
    )
    .unwrap();

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "Frequency {ref_in_str}→{ref_out_str} ({COUNT} conversions): \
         Rust={rust_ns}ns, C++={cpp_ns}ns, ratio={ratio:.2}x"
    );
    if ratio > 2.0 {
        eprintln!("WARNING: Rust is >2x slower than C++ for Frequency {ref_in_str}→{ref_out_str}");
    }
}

#[test]
fn perf_frequency_lsrk_to_bary() {
    bench_frequency(FrequencyRef::LSRK, FrequencyRef::BARY);
}

#[test]
fn perf_frequency_bary_to_lgroup() {
    bench_frequency(FrequencyRef::BARY, FrequencyRef::LGROUP);
}

// ---------------------------------------------------------------------------
// Wave 5: Radial velocity perf tests
// ---------------------------------------------------------------------------

fn bench_radvel(ref_in: RadialVelocityRef, ref_out: RadialVelocityRef) {
    let ref_in_str = ref_in.as_str();
    let ref_out_str = ref_out.as_str();
    let dir_lon = 0.185_948_8;
    let dir_lat = 0.722_777_4;

    let dir = MDirection::from_angles(dir_lon, dir_lat, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);

    let rust_start = std::time::Instant::now();
    for i in 0..COUNT {
        let ms = 50_000.0 + (i as f64) * 10.0;
        let rv = MRadialVelocity::new(ms, ref_in);
        let result = rv.convert_to(ref_out, &frame).unwrap();
        std::hint::black_box(&result);
    }
    let rust_ns = rust_start.elapsed().as_nanos() as u64;

    let cpp_ns = cpp_bench_radvel_convert(
        50_000.0,
        COUNT,
        ref_in_str,
        ref_out_str,
        dir_lon,
        dir_lat,
        "J2000",
        J2000_MJD,
        0.0,
        0.0,
        0.0,
        ITERATIONS,
    )
    .unwrap();

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "RadVel {ref_in_str}→{ref_out_str} ({COUNT} conversions): \
         Rust={rust_ns}ns, C++={cpp_ns}ns, ratio={ratio:.2}x"
    );
    if ratio > 2.0 {
        eprintln!("WARNING: Rust is >2x slower than C++ for RadVel {ref_in_str}→{ref_out_str}");
    }
}

#[test]
fn perf_radvel_lsrk_to_bary() {
    bench_radvel(RadialVelocityRef::LSRK, RadialVelocityRef::BARY);
}

#[test]
fn perf_radvel_bary_to_geo() {
    bench_radvel(RadialVelocityRef::BARY, RadialVelocityRef::GEO);
}

// ---------------------------------------------------------------------------
// Wave 5: B1950 / ITRF direction perf tests
// ---------------------------------------------------------------------------

#[test]
fn perf_direction_j2000_to_b1950() {
    bench_direction(DirectionRef::J2000, DirectionRef::B1950, 0.0);
}

#[test]
fn perf_direction_j2000_to_itrf() {
    bench_direction(DirectionRef::J2000, DirectionRef::ITRF, J2000_MJD);
}

// ---------------------------------------------------------------------------
// Wave 5: Epoch GAST perf test
// ---------------------------------------------------------------------------

#[test]
fn perf_epoch_ut1_to_gast() {
    let vla = MPosition::new_wgs84(
        -1.878_283_2, // VLA_LON
        0.595_370_3,  // VLA_LAT
        2124.0,
    );
    let frame = MeasFrame::new().with_position(vla).with_dut1(0.3);

    let rust_start = std::time::Instant::now();
    for i in 0..COUNT {
        let mjd = J2000_MJD + (i as f64) * 0.001;
        let epoch = MEpoch::from_mjd(mjd, EpochRef::UT1);
        let result = epoch.convert_to(EpochRef::GAST, &frame).unwrap();
        std::hint::black_box(&result);
    }
    let rust_ns = rust_start.elapsed().as_nanos() as u64;

    // C++ epoch bench doesn't support frame fields, so use single-conversion timing
    // as a rough estimate (convert 10000 times manually).
    let cpp_start = std::time::Instant::now();
    for i in 0..COUNT {
        let mjd = J2000_MJD + (i as f64) * 0.001;
        let _ = casacore_test_support::measures_interop::cpp_epoch_convert_with_frame(
            mjd,
            "UT1",
            "GAST",
            -1.878_283_2,
            0.595_370_3,
            2124.0,
            0.3,
        )
        .unwrap();
    }
    let cpp_ns = cpp_start.elapsed().as_nanos() as u64;

    let ratio = rust_ns as f64 / cpp_ns as f64;
    eprintln!(
        "Epoch UT1→GAST ({COUNT} conversions): \
         Rust={rust_ns}ns, C++={cpp_ns}ns, ratio={ratio:.2}x"
    );
    if ratio > 2.0 {
        eprintln!("WARNING: Rust is >2x slower than C++ for Epoch UT1→GAST");
    }
}
