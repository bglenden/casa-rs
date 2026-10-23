// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws for the measures state the casacore C++ oracle shares with casa-rs:
//! the measures tables both sides read, and casacore's process-global IAU
//! model (#845).
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use casa_measures_data::MeasuresRuntime;
use casa_test_support::measures_interop::MeasuresOracle;
use casa_types::measures::MeasuresProvider;
use std::path::Path;

const J2000_MJD: f64 = 51_544.5;
const VLA_LON: f64 = -1.878_283_2;
const VLA_LAT: f64 = 0.595_370_3;
const VLA_H: f64 = 2124.0;

fn measures_runtime() -> MeasuresRuntime {
    MeasuresRuntime::open_discovered(Default::default()).expect("test measures runtime")
}

fn canonical(path: &Path) -> std::path::PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// The oracle once read Homebrew's `casacore-data` (IERSeop2000 of
/// 2025-11-10) while casa-rs read `~/.casa/data` (of 2026-10-21); dUT1 at
/// J2000 differed by 0.10 ms and failed the LAST and HADEC laws.
#[test]
fn oracle_reads_the_measures_tables_casa_rs_reads() {
    let runtime = measures_runtime();
    let root = canonical(runtime.root());
    for name in ["IERSeop2000", "IERSpredict2000", "TAI_UTC"] {
        let (path, vs_date) = MeasuresOracle::measures_table_identity(name).unwrap();
        let path = canonical(Path::new(&path));
        assert!(
            path.starts_with(&root),
            "casacore resolved {name} at {} (VS_DATE {vs_date}), outside the Rust runtime {}",
            path.display(),
            root.display()
        );
    }
}

/// With one table set, the Earth-orientation values both sides interpolate
/// agree to rounding, not to a data-vintage tolerance.
#[test]
fn oracle_and_runtime_read_the_same_earth_orientation() {
    let runtime = measures_runtime();
    for mjd in [48_622.0, 51_544.5, 55_000.25, 58_000.75, 60_000.5] {
        let rust = runtime
            .eop_values(mjd)
            .expect("read EOP")
            .expect("epoch inside the EOP table");
        let (dut1, xp, yp) = MeasuresOracle::eop_query(mjd).unwrap();
        assert!(
            (rust.dut1_seconds - dut1).abs() < 1e-9,
            "dUT1 at MJD {mjd}: Rust={} C++={dut1}",
            rust.dut1_seconds
        );
        assert!(
            (rust.x_arcsec - xp).abs() < 1e-9 && (rust.y_arcsec - yp).abs() < 1e-9,
            "polar motion at MJD {mjd}: Rust=({}, {}) C++=({xp}, {yp})",
            rust.x_arcsec,
            rust.y_arcsec
        );
    }
}

/// Switching casacore to IAU 2000A and back leaves no state behind: the
/// IAU 1976/1980 answer after the switch is the answer before it.
#[test]
fn iau_model_switch_leaves_no_casacore_state_behind() {
    let iau1976 = || {
        MeasuresOracle::direction_convert(
            1.0, 0.5, "J2000", "HADEC", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
        )
        .unwrap()
    };
    let before = iau1976();
    let iau2000a = MeasuresOracle::direction_convert_iau2000a(
        1.0, 0.5, "J2000", "HADEC", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let after = iau1976();
    assert_ne!(before, iau2000a, "the IAU 2000A switch took no effect");
    assert_eq!(
        before.0.to_bits(),
        after.0.to_bits(),
        "HA moved across the switch"
    );
    assert_eq!(
        before.1.to_bits(),
        after.1.to_bits(),
        "Dec moved across the switch"
    );
}
