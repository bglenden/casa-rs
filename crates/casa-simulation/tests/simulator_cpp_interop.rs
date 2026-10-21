// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore C++ reads the MeasurementSets the simulator session writes
//! (simulation Phase 1c, #841).
//!
//! Each session writes an MS through `Simulator::close`; C++ casacore opens
//! it as a `MeasurementSet` and digests every table, keyword and cell, and the
//! digest must equal the one casa-rs computes from the same files. The two
//! sessions cover what the writer varies: global and local antenna
//! coordinates, two and four correlations, autocorrelations on and off,
//! several fields, spectral windows, observations and states, elevation
//! flags, and the optional SOURCE subtable.

#![cfg(feature = "cpp-interop-tests")]

use std::path::Path;

use casa_ms::MeasurementSetWriteResources;
use casa_simulation::{
    ArrayConfig, CoordinateSystem, ObserveSpec, Simulator, SimulatorOptions, SpectralWindowSpec,
    StateSpec,
};
use casa_test_support::ms_digest;
use casa_test_support::ms_interop::MeasurementSetOracle;
use casa_test_support::{casacore_oracle_available, deterministic_measures_provider};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch, MPosition};

fn options() -> SimulatorOptions {
    SimulatorOptions {
        measures: deterministic_measures_provider(),
        write_resources: MeasurementSetWriteResources::from_system_memory(2)
            .expect("write resources"),
        // 2024-06-01 00:00 UTC.
        now: MEpoch::from_mjd(60_462.0, EpochRef::UTC),
    }
}

fn window(name: &str, frequency_hz: f64, frame: FrequencyRef, stokes: &str) -> SpectralWindowSpec {
    SpectralWindowSpec {
        name: name.to_string(),
        start_frequency_hz: frequency_hz,
        channel_width_hz: 2.0e6,
        resolution_hz: 2.0e6,
        frame,
        channel_count: 8,
        stokes: stokes.to_string(),
    }
}

fn scan(
    source: &str,
    window: &str,
    start_s: f64,
    stop_s: f64,
    new_observation: bool,
) -> ObserveSpec {
    ObserveSpec {
        source: source.to_string(),
        spectral_window: window.to_string(),
        start_s,
        stop_s,
        add_observation: new_observation,
        state: StateSpec {
            sig: true,
            reference: false,
            cal: 0.0,
            load: 0.0,
            sub_scan: 0,
            obs_mode: "OBSERVE_TARGET.ON_SOURCE".to_string(),
        },
        observer: "casa-rs".to_string(),
        project: "interop".to_string(),
    }
}

fn direction(ra_deg: f64, dec_deg: f64) -> MDirection {
    MDirection::from_angles(
        ra_deg.to_radians(),
        dec_deg.to_radians(),
        DirectionRef::J2000,
    )
}

/// Five VLA pads in ITRF, two correlations, autocorrelations, two fields
/// and two observations, in hour-angle mode.
fn write_vla_session(path: &Path) {
    let mut session = Simulator::open(path, options());
    let centre = [-1_601_185.0, -5_041_977.0, 3_554_875.0];
    let offsets = [
        [0.0, 0.0, 0.0],
        [120.0, -40.0, 15.0],
        [-80.0, 210.0, -30.0],
        [400.0, 90.0, 120.0],
        [-350.0, -260.0, -95.0],
    ];
    session
        .set_config(&ArrayConfig {
            telescope: "VLA".to_string(),
            x: offsets.iter().map(|offset| centre[0] + offset[0]).collect(),
            y: offsets.iter().map(|offset| centre[1] + offset[1]).collect(),
            z: offsets.iter().map(|offset| centre[2] + offset[2]).collect(),
            dish_diameter: vec![25.0],
            offset: vec![0.0],
            mount: vec!["alt-az".to_string()],
            antenna_names: (1..=5).map(|index| format!("ea{index:02}")).collect(),
            pad_names: (1..=5).map(|index| format!("W{index:02}")).collect(),
            coordinate_system: CoordinateSystem::Global,
        })
        .expect("setconfig");
    session
        .set_spectral_window(&window("LBand", 1.4e9, FrequencyRef::TOPO, "RR LL"))
        .expect("setspwindow");
    session
        .set_field("first", &direction(150.0, 34.0), "C", 0.0)
        .expect("setfield");
    session
        .set_field("second", &direction(160.0, 20.0), "", 0.0)
        .expect("setfield");
    session
        .set_times(10.0, true, MEpoch::from_mjd(60_462.0, EpochRef::UTC))
        .expect("settimes");
    session
        .observe(&scan("first", "LBand", -60.0, 0.0, false))
        .expect("observe first");
    session
        .observe(&scan("second", "LBand", 0.0, 40.0, true))
        .expect("observe second");
    session.close().expect("close");
}

/// Four ALMA antennas in local coordinates, four correlations, no
/// autocorrelations, two spectral windows in different frames, and an
/// elevation limit that flags part of the scan.
fn write_alma_session(path: &Path) {
    let mut session = Simulator::open(path, options());
    let reference = MPosition::new_wgs84(
        -67.754_929_f64.to_radians(),
        -23.022_886_f64.to_radians(),
        5056.8,
    );
    session
        .set_config(&ArrayConfig {
            telescope: "ALMA".to_string(),
            x: vec![0.0, 30.0, -25.0, 60.0],
            y: vec![0.0, 15.0, 40.0, -35.0],
            z: vec![0.0, 1.0, -2.0, 0.5],
            dish_diameter: vec![12.0, 12.0, 12.0, 7.0],
            offset: vec![0.0],
            mount: vec!["ALT-AZ".to_string()],
            antenna_names: vec![
                "DA41".to_string(),
                "DA42".to_string(),
                "DV01".to_string(),
                "CM01".to_string(),
            ],
            pad_names: vec![
                "A001".to_string(),
                "A002".to_string(),
                "A003".to_string(),
                "J501".to_string(),
            ],
            coordinate_system: CoordinateSystem::Local { reference },
        })
        .expect("setconfig");
    session
        .set_feed("perfect X Y", &[0.0], &[0.0], &["X".to_string()])
        .expect("setfeed");
    session
        .set_spectral_window(&window("B3a", 100.0e9, FrequencyRef::TOPO, "XX XY YX YY"))
        .expect("setspwindow");
    session
        .set_spectral_window(&window("B3b", 102.0e9, FrequencyRef::LSRK, "XX XY YX YY"))
        .expect("setspwindow");
    session
        .set_field("target", &direction(83.6, -5.4), "", 0.0)
        .expect("setfield");
    session.set_auto(0.0);
    session.set_limits(1.0e-6, 20.0_f64.to_radians());
    session
        .set_times(6.048, false, MEpoch::from_mjd(60_462.4, EpochRef::UTC))
        .expect("settimes");
    session
        .observe(&scan("target", "B3a", 0.0, 60.0, false))
        .expect("observe B3a");
    session
        .observe(&scan("target", "B3b", 60.0, 120.0, false))
        .expect("observe B3b");
    session.close().expect("close");
}

fn assert_cpp_reads(name: &str, write: fn(&Path)) {
    if !casacore_oracle_available() {
        eprintln!("skipping {name}: C++ casacore not available");
        return;
    }
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join(format!("{name}.ms"));
    write(&path);
    let rust_manifest = ms_digest::measurement_set_manifest(&path);
    let cpp_manifest = MeasurementSetOracle::digest_manifest(&path)
        .unwrap_or_else(|error| panic!("C++ casacore opens {name}: {error}"));
    let differences = rust_manifest
        .lines()
        .zip(cpp_manifest.lines())
        .filter(|(rust, cpp)| rust != cpp)
        .collect::<Vec<_>>();
    if differences.is_empty() {
        return;
    }
    for (rust, _) in &differences {
        if let Some(label) = rust
            .split_whitespace()
            .next()
            .and_then(|key| key.strip_suffix(":ROWS"))
        {
            print_first_differing_row(&path, label.trim_start_matches("EXTRA:"));
        }
    }
    panic!("{name}: casa-rs and casacore read different MeasurementSets: {differences:#?}");
}

/// Print, field by field, the first row of `label` whose Rust and C++
/// digests differ.
fn print_first_differing_row(path: &Path, label: &str) {
    let Some(table) = ms_digest::labelled_table(path, label) else {
        return;
    };
    for row in 0..table.row_count() {
        let cpp = MeasurementSetOracle::table_row_field_manifest(path, label, row as u64)
            .unwrap_or_else(|error| format!("C++ error: {error}\n"));
        let rust = ms_digest::table_row_field_manifest(&table, row, path);
        if rust != cpp {
            eprintln!("{label} row {row}, casa-rs:\n{rust}{label} row {row}, casacore:\n{cpp}");
            return;
        }
    }
}

#[test]
fn casacore_reads_a_vla_session() {
    assert_cpp_reads("vla_session", write_vla_session);
}

#[test]
fn casacore_reads_an_alma_session_in_local_coordinates() {
    assert_cpp_reads("alma_session", write_alma_session);
}
