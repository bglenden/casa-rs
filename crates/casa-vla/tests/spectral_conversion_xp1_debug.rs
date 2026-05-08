// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use std::process::Command;

use casa_ms::SubTable;
use casa_ms::ms::MeasurementSet;
use casa_test_support::measures_interop::{
    cpp_frequency_convert, cpp_frequency_convert_between_frames, cpp_frequency_convert_via_model,
    cpp_frequency_convert_via_mutated_model,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier, discover_casa_python};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::position::PositionRef;
use casa_types::measures::{MPosition, MeasFrame};
use casa_types::{ArrayValue, ScalarValue, Value};
use casa_vla::{CdaId, DirectionEpoch, VlaDiskReader};
use casa_vla::{ImportVlaOptions, import_archive_files_to_measurement_set_from_options};
use tempfile::TempDir;

fn direction_ref(epoch: DirectionEpoch) -> DirectionRef {
    match epoch {
        DirectionEpoch::J2000 => DirectionRef::J2000,
        DirectionEpoch::B1950Vla => DirectionRef::B1950,
        DirectionEpoch::Apparent => DirectionRef::APP,
        DirectionEpoch::Unknown(_) => DirectionRef::J2000,
    }
}

fn get_f64_scalar(row: &casa_types::RecordValue, column: &str) -> f64 {
    match row.get(column).expect("missing scalar column") {
        Value::Scalar(ScalarValue::Float64(value)) => *value,
        other => panic!("{column} had unexpected value {other:?}"),
    }
}

fn get_i32_scalar(row: &casa_types::RecordValue, column: &str) -> i32 {
    match row.get(column).expect("missing scalar column") {
        Value::Scalar(ScalarValue::Int32(value)) => *value,
        other => panic!("{column} had unexpected value {other:?}"),
    }
}

fn get_string_scalar(row: &casa_types::RecordValue, column: &str) -> String {
    match row.get(column).expect("missing scalar column") {
        Value::Scalar(ScalarValue::String(value)) => value.clone(),
        other => panic!("{column} had unexpected value {other:?}"),
    }
}

fn get_f64_array(row: &casa_types::RecordValue, column: &str) -> Vec<f64> {
    match row.get(column).expect("missing array column") {
        Value::Array(ArrayValue::Float64(values)) => values.iter().copied().collect(),
        other => panic!("{column} had unexpected value {other:?}"),
    }
}

fn get_i32_scalar_or(row: &casa_types::RecordValue, column: &str, default: i32) -> i32 {
    match row.get(column) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
        None => default,
        Some(other) => panic!("{column} had unexpected value {other:?}"),
    }
}

fn xp1_path() -> Option<std::path::PathBuf> {
    [
        "unittest/importvla/AS758_C030425.xp1",
        "other/AS758_C030425.xp1",
    ]
    .into_iter()
    .find_map(|relative| {
        casatestdata_path_for_tier(CasaTestDataTier::SlowParity, relative)
            .filter(|path| path.exists())
    })
}

#[test]
fn xp1_dump_spw_seed_sets_from_rust_and_casa_imports() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };
    let Some(casa_python) = discover_casa_python() else {
        eprintln!("skipping: CASA Python environment not found");
        return;
    };

    let tempdir = TempDir::new().expect("create tempdir");
    let rust_ms_path = tempdir.path().join("rust-import.ms");
    let casa_ms_path = tempdir.path().join("casa-import.ms");

    import_archive_files_to_measurement_set_from_options(&ImportVlaOptions {
        archivefiles: vec![path.to_path_buf()],
        vis: Some(rust_ms_path.clone()),
        ..ImportVlaOptions::default()
    })
    .expect("import archive into Rust MeasurementSet");

    let output = Command::new(&casa_python.program)
        .arg("-c")
        .arg(
            r#"
from casatasks import importvla
import sys
importvla(archivefiles=[sys.argv[1]], vis=sys.argv[2], frequencytol=150000.0)
"#,
        )
        .arg(path)
        .arg(&casa_ms_path)
        .output()
        .expect("run CASA importvla");
    assert!(
        output.status.success(),
        "CASA importvla failed: status={:?}\nstdout={}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let rust = MeasurementSet::open(&rust_ms_path).expect("open Rust MeasurementSet");
    let casa = MeasurementSet::open(&casa_ms_path).expect("open CASA MeasurementSet");

    let rust_spw_subtable = rust.spectral_window().expect("rust SPECTRAL_WINDOW");
    let rust_spw = rust_spw_subtable.table();
    let casa_spw_subtable = casa.spectral_window().expect("casa SPECTRAL_WINDOW");
    let casa_spw = casa_spw_subtable.table();
    assert_eq!(rust_spw.row_count(), casa_spw.row_count(), "spw row count");

    eprintln!("rust_spw_rows={}", rust_spw.row_count());
    for row_index in 0..rust_spw.row_count().min(8) {
        let rust_row = rust_spw
            .row_accessor()
            .row(row_index)
            .expect("read rust spw row");
        let casa_row = casa_spw
            .row_accessor()
            .row(row_index)
            .expect("read casa spw row");
        let rust_chan_freq = get_f64_array(rust_row, "CHAN_FREQ");
        let casa_chan_freq = get_f64_array(casa_row, "CHAN_FREQ");
        eprintln!("rust_spw_row[{row_index}]={rust_row:?}");
        eprintln!("casa_spw_row[{row_index}]={casa_row:?}");
        eprintln!(
            "spw[{row_index}] rust name={} ref={} chan0={} doppler_id={} if_chain={} meas_ref={} | casa name={} ref={} chan0={} doppler_id={} if_chain={} meas_ref={}",
            get_string_scalar(rust_row, "NAME"),
            get_f64_scalar(rust_row, "REF_FREQUENCY"),
            rust_chan_freq[0],
            get_i32_scalar_or(rust_row, "DOPPLER_ID", -1),
            get_i32_scalar(rust_row, "IF_CONV_CHAIN"),
            get_i32_scalar(rust_row, "MEAS_FREQ_REF"),
            get_string_scalar(casa_row, "NAME"),
            get_f64_scalar(casa_row, "REF_FREQUENCY"),
            casa_chan_freq[0],
            get_i32_scalar_or(casa_row, "DOPPLER_ID", -1),
            get_i32_scalar(casa_row, "IF_CONV_CHAIN"),
            get_i32_scalar(casa_row, "MEAS_FREQ_REF"),
        );
    }

    let mut rust_chan0 = (0..rust_spw.row_count())
        .map(|row_index| {
            let row = rust_spw
                .row_accessor()
                .row(row_index)
                .expect("read rust spw row");
            get_f64_array(row, "CHAN_FREQ")[0]
        })
        .collect::<Vec<_>>();
    let mut casa_chan0 = (0..casa_spw.row_count())
        .map(|row_index| {
            let row = casa_spw
                .row_accessor()
                .row(row_index)
                .expect("read casa spw row");
            get_f64_array(row, "CHAN_FREQ")[0]
        })
        .collect::<Vec<_>>();
    rust_chan0.sort_by(f64::total_cmp);
    casa_chan0.sort_by(f64::total_cmp);
    for (index, (rust_hz, casa_hz)) in rust_chan0.iter().zip(casa_chan0.iter()).enumerate() {
        eprintln!(
            "sorted_chan0[{index}] rust={} casa={} diff={}",
            rust_hz,
            casa_hz,
            rust_hz - casa_hz
        );
    }
}

#[test]
fn xp1_first_lsrk_channel_matches_cpp_conversion() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };

    let mut reader = VlaDiskReader::open(path).expect("open xp1");
    while let Some(record) = reader.next_record().expect("read logical record") {
        let rca = record.rca();
        let sda = record.sda().expect("decode SDA");
        for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca
                .cda_offset_bytes(cda_id.index())
                .expect("decode CDA offset")
                == 0
            {
                continue;
            }
            if sda
                .n_polarizations(cda_id)
                .expect("decode polarization count")
                == 0
            {
                continue;
            }
            if !sda
                .doppler_tracking(cda_id)
                .expect("decode doppler tracking")
            {
                continue;
            }

            let topo_hz = sda.edge_frequency_hz(cda_id).expect("edge frequency")
                + 0.5 * sda.channel_width_hz(cda_id).expect("channel width");
            let direction = sda
                .source_direction_radians()
                .expect("source direction radians");
            let direction = MDirection::from_angles(
                direction[0],
                direction[1],
                match sda.direction_epoch().expect("direction epoch") {
                    DirectionEpoch::J2000 => DirectionRef::J2000,
                    DirectionEpoch::B1950Vla => DirectionRef::B1950,
                    DirectionEpoch::Apparent => DirectionRef::APP,
                    DirectionEpoch::Unknown(_) => DirectionRef::J2000,
                },
            );
            let observatory =
                MPosition::from_observatory_name("VLA").expect("resolve VLA observatory");
            let frame = MeasFrame::new()
                .with_bundled_eop()
                .with_epoch(MEpoch::from_mjd(
                    (f64::from(rca.obs_day().expect("obs day")) * 86_400.0
                        + sda.observation_time_seconds().expect("observation time"))
                        / 86_400.0,
                    EpochRef::UTC,
                ))
                .with_position(observatory.clone())
                .with_direction(direction.clone());

            let rust_hz = MFrequency::new(topo_hz, FrequencyRef::TOPO)
                .convert_to(FrequencyRef::LSRK, &frame)
                .expect("Rust TOPO to LSRK")
                .hz();
            let observatory_wgs84 = observatory
                .convert_to(PositionRef::WGS84)
                .expect("observatory WGS84");
            let cpp_hz = cpp_frequency_convert(
                topo_hz,
                "TOPO",
                "LSRK",
                direction.longitude_rad(),
                direction.latitude_rad(),
                direction.refer().as_str(),
                frame.epoch().expect("frame epoch").value().as_mjd(),
                observatory_wgs84.longitude_rad(),
                observatory_wgs84.latitude_rad(),
                observatory_wgs84.values()[2],
            )
            .expect("C++ TOPO to LSRK");

            assert!(
                (rust_hz - cpp_hz).abs() < 1.0,
                "topo={topo_hz} rust={rust_hz} cpp={cpp_hz} diff={}",
                rust_hz - cpp_hz
            );
            return;
        }
    }

    panic!("did not find doppler-tracked CDA in xp1 archive");
}

#[test]
fn xp1_compare_direct_and_frame_bound_cpp_conversion() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };

    let mut reader = VlaDiskReader::open(path).expect("open xp1");
    while let Some(record) = reader.next_record().expect("read logical record") {
        let rca = record.rca();
        let sda = record.sda().expect("decode SDA");
        for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca
                .cda_offset_bytes(cda_id.index())
                .expect("decode CDA offset")
                == 0
            {
                continue;
            }
            if sda
                .n_polarizations(cda_id)
                .expect("decode polarization count")
                == 0
            {
                continue;
            }
            if !sda
                .doppler_tracking(cda_id)
                .expect("decode doppler tracking")
            {
                continue;
            }

            let topo_hz = sda.edge_frequency_hz(cda_id).expect("edge frequency")
                + 0.5 * sda.channel_width_hz(cda_id).expect("channel width");
            let direction = sda
                .source_direction_radians()
                .expect("source direction radians");
            let direction_epoch = sda.direction_epoch().expect("direction epoch");
            let direction =
                MDirection::from_angles(direction[0], direction[1], direction_ref(direction_epoch));
            let observatory =
                MPosition::from_observatory_name("VLA").expect("resolve VLA observatory");
            let frame = MeasFrame::new()
                .with_bundled_eop()
                .with_epoch(MEpoch::from_mjd(
                    (f64::from(rca.obs_day().expect("obs day")) * 86_400.0
                        + sda.observation_time_seconds().expect("observation time"))
                        / 86_400.0,
                    EpochRef::UTC,
                ))
                .with_position(observatory.clone())
                .with_direction(direction.clone());
            let observatory_wgs84 = observatory
                .convert_to(PositionRef::WGS84)
                .expect("observatory WGS84");
            let epoch_mjd = frame.epoch().expect("frame epoch").value().as_mjd();

            let direct_cpp = cpp_frequency_convert(
                topo_hz,
                "TOPO",
                "LSRK",
                direction.longitude_rad(),
                direction.latitude_rad(),
                direction.refer().as_str(),
                epoch_mjd,
                observatory_wgs84.longitude_rad(),
                observatory_wgs84.latitude_rad(),
                observatory_wgs84.values()[2],
            )
            .expect("direct C++ TOPO to LSRK");
            let frame_bound_cpp = cpp_frequency_convert_between_frames(
                topo_hz,
                "TOPO",
                "LSRK",
                direction.longitude_rad(),
                direction.latitude_rad(),
                direction.refer().as_str(),
                epoch_mjd,
                observatory_wgs84.longitude_rad(),
                observatory_wgs84.latitude_rad(),
                observatory_wgs84.values()[2],
                direction.longitude_rad(),
                direction.latitude_rad(),
                direction.refer().as_str(),
                epoch_mjd,
                observatory_wgs84.longitude_rad(),
                observatory_wgs84.latitude_rad(),
                observatory_wgs84.values()[2],
            )
            .expect("frame-bound C++ TOPO to LSRK");
            let via_model_cpp = cpp_frequency_convert_via_model(
                topo_hz,
                "TOPO",
                "LSRK",
                direction.longitude_rad(),
                direction.latitude_rad(),
                direction.refer().as_str(),
                epoch_mjd,
                observatory_wgs84.longitude_rad(),
                observatory_wgs84.latitude_rad(),
                observatory_wgs84.values()[2],
            )
            .expect("via-model C++ TOPO to LSRK");
            let via_mutated_model_cpp = cpp_frequency_convert_via_mutated_model(
                topo_hz,
                "TOPO",
                "LSRK",
                direction.longitude_rad(),
                direction.latitude_rad(),
                direction.refer().as_str(),
                epoch_mjd,
                observatory_wgs84.longitude_rad(),
                observatory_wgs84.latitude_rad(),
                observatory_wgs84.values()[2],
            )
            .expect("via-mutated-model C++ TOPO to LSRK");
            let rust_hz = MFrequency::new(topo_hz, FrequencyRef::TOPO)
                .convert_to(FrequencyRef::LSRK, &frame)
                .expect("Rust TOPO to LSRK")
                .hz();

            eprintln!(
                "topo_hz={} epoch_mjd={} dir_lon={} dir_lat={} dir_ref={} rust_hz={} direct_cpp={} frame_bound_cpp={} via_model_cpp={} via_mutated_model_cpp={} diff_direct={} diff_frame_bound={} diff_via_model={} diff_via_mutated_model={}",
                topo_hz,
                epoch_mjd,
                direction.longitude_rad(),
                direction.latitude_rad(),
                direction.refer().as_str(),
                rust_hz,
                direct_cpp,
                frame_bound_cpp,
                via_model_cpp,
                via_mutated_model_cpp,
                rust_hz - direct_cpp,
                rust_hz - frame_bound_cpp,
                rust_hz - via_model_cpp,
                rust_hz - via_mutated_model_cpp
            );
            return;
        }
    }

    panic!("did not find doppler-tracked CDA in xp1 archive");
}

#[test]
fn xp1_print_first_supported_records() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };

    let mut reader = VlaDiskReader::open(path).expect("open xp1");
    let mut logical_record_index = 0usize;
    let mut printed = 0usize;

    while let Some(record) = reader.next_record().expect("read logical record") {
        let sda = record.sda().expect("decode SDA");
        let mode = sda.observation_mode_code().expect("observation mode code");
        if !matches!(
            mode.as_str(),
            "  " | "H " | "S " | "SP" | "VA" | "VB" | "VL" | "VR" | "VX"
        ) {
            logical_record_index += 1;
            continue;
        }
        eprintln!(
            "supported logical_record_index={} mode={} source={} direction_epoch={:?} time={}",
            logical_record_index,
            mode,
            sda.source_name().expect("source name"),
            sda.direction_epoch().expect("direction epoch"),
            sda.observation_time_seconds().expect("observation time"),
        );
        printed += 1;
        if printed >= 16 {
            return;
        }
        logical_record_index += 1;
    }
}

#[test]
fn xp1_find_record_closest_to_casa_spw_seed() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };

    let target_hz = 23_692_506_802.643_28_f64;
    let mut reader = VlaDiskReader::open(path).expect("open xp1");
    let mut logical_record_index = 0usize;
    let mut best: Option<(usize, f64, f64, String)> = None;

    while let Some(record) = reader.next_record().expect("read logical record") {
        let rca = record.rca();
        let sda = record.sda().expect("decode SDA");
        let mode = sda.observation_mode_code().expect("observation mode code");
        if !matches!(
            mode.as_str(),
            "  " | "H " | "S " | "SP" | "VA" | "VB" | "VL" | "VR" | "VX"
        ) {
            logical_record_index += 1;
            continue;
        }
        for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca
                .cda_offset_bytes(cda_id.index())
                .expect("decode CDA offset")
                == 0
            {
                continue;
            }
            if sda
                .n_polarizations(cda_id)
                .expect("decode polarization count")
                == 0
            {
                continue;
            }
            if !sda
                .doppler_tracking(cda_id)
                .expect("decode doppler tracking")
            {
                continue;
            }

            let topo_hz = sda.edge_frequency_hz(cda_id).expect("edge frequency")
                + 0.5 * sda.channel_width_hz(cda_id).expect("channel width");
            let direction = sda
                .source_direction_radians()
                .expect("source direction radians");
            let direction_epoch = sda.direction_epoch().expect("direction epoch");
            let direction =
                MDirection::from_angles(direction[0], direction[1], direction_ref(direction_epoch));
            let observatory =
                MPosition::from_observatory_name("VLA").expect("resolve VLA observatory");
            let frame = MeasFrame::new()
                .with_bundled_eop()
                .with_epoch(MEpoch::from_mjd(
                    (f64::from(rca.obs_day().expect("obs day")) * 86_400.0
                        + sda.observation_time_seconds().expect("observation time"))
                        / 86_400.0,
                    EpochRef::UTC,
                ))
                .with_position(observatory)
                .with_direction(direction);
            let rust_hz = MFrequency::new(topo_hz, FrequencyRef::TOPO)
                .convert_to(FrequencyRef::LSRK, &frame)
                .expect("Rust TOPO to LSRK")
                .hz();
            let diff = (rust_hz - target_hz).abs();
            let source_name = sda.source_name().expect("source name");
            match &best {
                Some((_, best_diff, _, _)) if diff >= *best_diff => {}
                _ => best = Some((logical_record_index, diff, rust_hz, source_name)),
            }
        }
        logical_record_index += 1;
    }

    let (index, diff, rust_hz, source_name) = best.expect("best matching record");
    eprintln!(
        "best logical_record_index={index} source={source_name} rust_hz={rust_hz} target_hz={target_hz} diff_hz={diff}"
    );
}

#[test]
fn xp1_find_any_record_closest_to_casa_spw_seed() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };

    let target_hz = 23_692_506_802.643_28_f64;
    let mut reader = VlaDiskReader::open(path).expect("open xp1");
    let mut logical_record_index = 0usize;
    let mut best: Option<(usize, f64, f64, String, String)> = None;

    while let Some(record) = reader.next_record().expect("read logical record") {
        let rca = record.rca();
        let sda = record.sda().expect("decode SDA");
        let mode = sda.observation_mode_code().expect("observation mode code");
        for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca
                .cda_offset_bytes(cda_id.index())
                .expect("decode CDA offset")
                == 0
            {
                continue;
            }
            if sda
                .n_polarizations(cda_id)
                .expect("decode polarization count")
                == 0
            {
                continue;
            }
            if !sda
                .doppler_tracking(cda_id)
                .expect("decode doppler tracking")
            {
                continue;
            }

            let topo_hz = sda.edge_frequency_hz(cda_id).expect("edge frequency")
                + 0.5 * sda.channel_width_hz(cda_id).expect("channel width");
            let direction = sda
                .source_direction_radians()
                .expect("source direction radians");
            let direction_epoch = sda.direction_epoch().expect("direction epoch");
            let direction =
                MDirection::from_angles(direction[0], direction[1], direction_ref(direction_epoch));
            let observatory =
                MPosition::from_observatory_name("VLA").expect("resolve VLA observatory");
            let frame = MeasFrame::new()
                .with_bundled_eop()
                .with_epoch(MEpoch::from_mjd(
                    (f64::from(rca.obs_day().expect("obs day")) * 86_400.0
                        + sda.observation_time_seconds().expect("observation time"))
                        / 86_400.0,
                    EpochRef::UTC,
                ))
                .with_position(observatory)
                .with_direction(direction);
            let rust_hz = MFrequency::new(topo_hz, FrequencyRef::TOPO)
                .convert_to(FrequencyRef::LSRK, &frame)
                .expect("Rust TOPO to LSRK")
                .hz();
            let diff = (rust_hz - target_hz).abs();
            let source_name = sda.source_name().expect("source name");
            match &best {
                Some((_, best_diff, _, _, _)) if diff >= *best_diff => {}
                _ => {
                    best = Some((
                        logical_record_index,
                        diff,
                        rust_hz,
                        source_name,
                        mode.clone(),
                    ))
                }
            }
        }
        logical_record_index += 1;
    }

    let (index, diff, rust_hz, source_name, mode) = best.expect("best matching record");
    eprintln!(
        "best_any logical_record_index={index} mode={mode} source={source_name} rust_hz={rust_hz} target_hz={target_hz} diff_hz={diff}"
    );
}

#[test]
fn xp1_print_first_doppler_tracked_candidates() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };

    let mut reader = VlaDiskReader::open(path).expect("open xp1");
    let mut logical_record_index = 0usize;
    let mut printed = 0usize;

    while let Some(record) = reader.next_record().expect("read logical record") {
        let rca = record.rca();
        let sda = record.sda().expect("decode SDA");
        let mode = sda.observation_mode_code().expect("observation mode code");
        for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca
                .cda_offset_bytes(cda_id.index())
                .expect("decode CDA offset")
                == 0
            {
                continue;
            }
            if sda
                .n_polarizations(cda_id)
                .expect("decode polarization count")
                == 0
            {
                continue;
            }
            if !sda
                .doppler_tracking(cda_id)
                .expect("decode doppler tracking")
            {
                continue;
            }

            let topo_hz = sda.edge_frequency_hz(cda_id).expect("edge frequency")
                + 0.5 * sda.channel_width_hz(cda_id).expect("channel width");
            let direction = sda
                .source_direction_radians()
                .expect("source direction radians");
            let direction_epoch = sda.direction_epoch().expect("direction epoch");
            let direction =
                MDirection::from_angles(direction[0], direction[1], direction_ref(direction_epoch));
            let observatory =
                MPosition::from_observatory_name("VLA").expect("resolve VLA observatory");
            let frame = MeasFrame::new()
                .with_bundled_eop()
                .with_epoch(MEpoch::from_mjd(
                    (f64::from(rca.obs_day().expect("obs day")) * 86_400.0
                        + sda.observation_time_seconds().expect("observation time"))
                        / 86_400.0,
                    EpochRef::UTC,
                ))
                .with_position(observatory)
                .with_direction(direction);
            let rust_hz = MFrequency::new(topo_hz, FrequencyRef::TOPO)
                .convert_to(FrequencyRef::LSRK, &frame)
                .expect("Rust TOPO to LSRK")
                .hz();
            eprintln!(
                "candidate logical_record_index={} mode={} source={} direction_epoch={:?} time={} cda={:?} topo_hz={} lsrk_hz={}",
                logical_record_index,
                mode,
                sda.source_name().expect("source name"),
                direction_epoch,
                sda.observation_time_seconds().expect("observation time"),
                cda_id,
                topo_hz,
                rust_hz
            );
            printed += 1;
            if printed >= 8 {
                return;
            }
        }
        logical_record_index += 1;
    }
}

#[test]
fn xp1_find_time_direction_combo_closest_to_casa_spw_seed() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };

    #[derive(Clone)]
    struct Candidate {
        logical_record_index: usize,
        source_name: String,
        mode: String,
        topo_hz: f64,
        epoch_mjd: f64,
        direction: MDirection,
    }

    let target_hz = 23_692_506_802.643_28_f64;
    let mut reader = VlaDiskReader::open(path).expect("open xp1");
    let mut logical_record_index = 0usize;
    let mut candidates = Vec::new();

    while let Some(record) = reader.next_record().expect("read logical record") {
        let rca = record.rca();
        let sda = record.sda().expect("decode SDA");
        let mode = sda.observation_mode_code().expect("observation mode code");
        for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca
                .cda_offset_bytes(cda_id.index())
                .expect("decode CDA offset")
                == 0
            {
                continue;
            }
            if sda
                .n_polarizations(cda_id)
                .expect("decode polarization count")
                == 0
            {
                continue;
            }
            if !sda
                .doppler_tracking(cda_id)
                .expect("decode doppler tracking")
            {
                continue;
            }

            let topo_hz = sda.edge_frequency_hz(cda_id).expect("edge frequency")
                + 0.5 * sda.channel_width_hz(cda_id).expect("channel width");
            let raw_direction = sda
                .source_direction_radians()
                .expect("source direction radians");
            let direction_epoch = sda.direction_epoch().expect("direction epoch");
            let direction = MDirection::from_angles(
                raw_direction[0],
                raw_direction[1],
                direction_ref(direction_epoch),
            );
            let epoch_mjd = (f64::from(rca.obs_day().expect("obs day")) * 86_400.0
                + sda.observation_time_seconds().expect("observation time"))
                / 86_400.0;
            candidates.push(Candidate {
                logical_record_index,
                source_name: sda.source_name().expect("source name"),
                mode: mode.clone(),
                topo_hz,
                epoch_mjd,
                direction,
            });
        }
        logical_record_index += 1;
    }

    let observatory = MPosition::from_observatory_name("VLA")
        .expect("resolve VLA observatory")
        .convert_to(PositionRef::WGS84)
        .expect("observatory WGS84");
    let mut best: Option<(usize, usize, f64, f64)> = None;

    for (freq_index, freq_candidate) in candidates.iter().enumerate() {
        for (frame_index, frame_candidate) in candidates.iter().enumerate() {
            let cpp_hz = cpp_frequency_convert(
                freq_candidate.topo_hz,
                "TOPO",
                "LSRK",
                frame_candidate.direction.longitude_rad(),
                frame_candidate.direction.latitude_rad(),
                frame_candidate.direction.refer().as_str(),
                frame_candidate.epoch_mjd,
                observatory.longitude_rad(),
                observatory.latitude_rad(),
                observatory.values()[2],
            )
            .expect("C++ TOPO to LSRK");
            let diff = (cpp_hz - target_hz).abs();
            match best {
                Some((_, _, best_diff, _)) if diff >= best_diff => {}
                _ => best = Some((freq_index, frame_index, diff, cpp_hz)),
            }
        }
    }

    let (freq_index, frame_index, diff, cpp_hz) = best.expect("best combo");
    let freq_candidate = &candidates[freq_index];
    let frame_candidate = &candidates[frame_index];
    eprintln!(
        "best_combo freq_record={} freq_source={} freq_mode={} topo_hz={} frame_record={} frame_source={} frame_mode={} epoch_mjd={} cpp_hz={} target_hz={} diff_hz={}",
        freq_candidate.logical_record_index,
        freq_candidate.source_name,
        freq_candidate.mode,
        freq_candidate.topo_hz,
        frame_candidate.logical_record_index,
        frame_candidate.source_name,
        frame_candidate.mode,
        frame_candidate.epoch_mjd,
        cpp_hz,
        target_hz,
        diff
    );
}
