// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(has_casacore_cpp)]

use casa_ms::MeasurementSet;
use casa_ms::columns::main_ids;
use casa_ms::columns::time_columns::TimeColumn;
use casa_ms::derived::engine::MsCalEngine;
use casa_test_support::casatestdata_path;
use casa_test_support::measures_interop::{
    cpp_frequency_convert, cpp_frequency_convert_between_frames, cpp_radvel_convert,
};
use casa_types::measures::PositionRef;
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::epoch::EpochRef;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};

fn refim_cband_g37line_ms_path() -> Option<std::path::PathBuf> {
    casatestdata_path("measurementset/evla/refim_Cband.G37line.ms").filter(|path| path.exists())
}

#[test]
fn refim_cband_topo_to_lsrk_row_10557_stays_within_one_hz_of_casacore() {
    let Some(ms_path) = refim_cband_g37line_ms_path() else {
        eprintln!("skipping spectral-frame parity test: refim_Cband.G37line.ms not available");
        return;
    };

    let ms = MeasurementSet::open(&ms_path).expect("open refim_Cband.G37line.ms");
    let engine = MsCalEngine::new(&ms).expect("build calibration engine");
    let spectral_window = ms.spectral_window().expect("SPECTRAL_WINDOW accessor");
    let channel_frequencies_hz = spectral_window
        .chan_freq(0)
        .expect("SPW channel frequencies");
    let channel_widths_hz = spectral_window.chan_width(0).expect("SPW channel widths");
    let source_freq_ref =
        FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(0).expect("MEAS_FREQ_REF"))
            .expect("supported spectral reference");
    assert_eq!(source_freq_ref, FrequencyRef::TOPO);

    let row = 10_557usize;
    let row_time_mjd_sec = TimeColumn::new(ms.main_table())
        .get_mjd_seconds(row)
        .expect("row time");
    let field_id = main_ids::field_id(ms.main_table())
        .get(row)
        .expect("row field id") as usize;
    let frame = engine
        .spectral_frame_observatory(row_time_mjd_sec, field_id)
        .expect("spectral frame");
    let direction = frame.direction().expect("frame direction");
    let observatory = frame
        .position()
        .expect("frame position")
        .convert_to(PositionRef::WGS84)
        .expect("observatory WGS84");
    let epoch_mjd = frame.epoch().expect("frame epoch").value().as_mjd();

    for &channel in &[101usize, 102usize] {
        let rust_hz = MFrequency::new(channel_frequencies_hz[channel], source_freq_ref)
            .convert_to(FrequencyRef::LSRK, &frame)
            .expect("convert TOPO to LSRK")
            .hz();
        let rust_geo_hz = MFrequency::new(channel_frequencies_hz[channel], source_freq_ref)
            .convert_to(FrequencyRef::GEO, &frame)
            .expect("convert TOPO to GEO")
            .hz();
        let cpp_hz = cpp_frequency_convert(
            channel_frequencies_hz[channel],
            "TOPO",
            "LSRK",
            direction.longitude_rad(),
            direction.latitude_rad(),
            direction.refer().as_str(),
            epoch_mjd,
            observatory.longitude_rad(),
            observatory.latitude_rad(),
            observatory.values()[2],
        )
        .expect("C++ TOPO to LSRK");
        let cpp_geo_hz = cpp_frequency_convert(
            channel_frequencies_hz[channel],
            "TOPO",
            "GEO",
            direction.longitude_rad(),
            direction.latitude_rad(),
            direction.refer().as_str(),
            epoch_mjd,
            observatory.longitude_rad(),
            observatory.latitude_rad(),
            observatory.values()[2],
        )
        .expect("C++ TOPO to GEO");

        assert!(
            (rust_geo_hz - cpp_geo_hz).abs() < 0.001,
            "row {row} channel {channel} GEO mismatch: rust={rust_geo_hz} C++={cpp_geo_hz}"
        );

        assert!(
            (rust_hz - cpp_hz).abs() < 1.0,
            "row {row} channel {channel} LSRK mismatch: rust={rust_hz} C++={cpp_hz}"
        );
    }

    let field_ids = main_ids::field_id(ms.main_table());
    let first_field_one_row = (0..ms.main_table().row_count())
        .find(|&candidate| field_ids.get(candidate).expect("candidate field id") == 1)
        .expect("row for field 1");
    let first_row_time_mjd_sec = TimeColumn::new(ms.main_table())
        .get_mjd_seconds(first_field_one_row)
        .expect("first field-1 row time");
    let first_frame = engine
        .spectral_frame_observatory(first_row_time_mjd_sec, 1)
        .expect("first field-1 spectral frame");
    let first_direction = first_frame.direction().expect("first frame direction");
    let first_observatory = first_frame
        .position()
        .expect("first frame position")
        .convert_to(PositionRef::WGS84)
        .expect("first frame WGS84");
    let first_epoch_mjd = first_frame
        .epoch()
        .expect("first frame epoch")
        .value()
        .as_mjd();
    for channel in [98usize, 99usize, 105usize, 106usize] {
        let rust_hz = MFrequency::new(channel_frequencies_hz[channel], source_freq_ref)
            .convert_to(FrequencyRef::LSRK, &first_frame)
            .expect("convert first-row TOPO to LSRK")
            .hz();
        let cpp_hz = cpp_frequency_convert(
            channel_frequencies_hz[channel],
            "TOPO",
            "LSRK",
            first_direction.longitude_rad(),
            first_direction.latitude_rad(),
            first_direction.refer().as_str(),
            first_epoch_mjd,
            first_observatory.longitude_rad(),
            first_observatory.latitude_rad(),
            first_observatory.values()[2],
        )
        .expect("C++ first-row TOPO to LSRK");
        assert!(
            (rust_hz - cpp_hz).abs() < 1.0,
            "first field-1 row {first_field_one_row} channel {channel} LSRK mismatch: rust={rust_hz} C++={cpp_hz}"
        );
    }

    let row0_time_mjd_sec = TimeColumn::new(ms.main_table())
        .get_mjd_seconds(0)
        .expect("row 0 time");
    let row0_field_id = main_ids::field_id(ms.main_table())
        .get(0)
        .expect("row 0 field id") as usize;
    let row0_frame = engine
        .spectral_frame_observatory(row0_time_mjd_sec, row0_field_id)
        .expect("row 0 spectral frame");
    let row0_field1_frame = engine
        .spectral_frame_observatory(row0_time_mjd_sec, 1)
        .expect("row 0 time with field 1");
    for (label, frame) in [
        ("row0_field0", &row0_frame),
        ("row0_field1", &row0_field1_frame),
        ("first_field1", &first_frame),
    ] {
        let rust_geo_hz = MFrequency::new(channel_frequencies_hz[106], source_freq_ref)
            .convert_to(FrequencyRef::GEO, frame)
            .expect("convert comparison TOPO to GEO")
            .hz();
        let rust_bary_hz = MFrequency::new(channel_frequencies_hz[106], source_freq_ref)
            .convert_to(FrequencyRef::BARY, frame)
            .expect("convert comparison TOPO to BARY")
            .hz();
        let rust_lsrk_hz = MFrequency::new(channel_frequencies_hz[106], source_freq_ref)
            .convert_to(FrequencyRef::LSRK, frame)
            .expect("convert comparison TOPO to LSRK")
            .hz();
        let position_itrf = frame
            .position()
            .expect("frame position for debug")
            .as_itrf();
        let position_wgs84 = frame
            .position()
            .expect("frame position for debug")
            .convert_to(PositionRef::WGS84)
            .expect("frame WGS84 for debug");
        eprintln!(
            "{label} channel106 geo={rust_geo_hz} bary={rust_bary_hz} lsrk={rust_lsrk_hz} \
             obs_itrf={position_itrf:?} obs_wgs84=({}, {}, {})",
            position_wgs84.longitude_rad(),
            position_wgs84.latitude_rad(),
            position_wgs84.values()[2],
        );
    }
    eprintln!(
        "raw ch105={} ch106={} spacing={} width0={} width105={} width106={}",
        channel_frequencies_hz[105],
        channel_frequencies_hz[106],
        channel_frequencies_hz[106] - channel_frequencies_hz[105],
        channel_widths_hz[0],
        channel_widths_hz[105],
        channel_widths_hz[106],
    );
}

#[test]
fn debug_refim_cband_topo_frequency_steps_for_field1_channel106() {
    let Some(ms_path) = refim_cband_g37line_ms_path() else {
        eprintln!("skipping spectral-frame debug test: refim_Cband.G37line.ms not available");
        return;
    };

    let ms = MeasurementSet::open(&ms_path).expect("open refim_Cband.G37line.ms");
    let engine = MsCalEngine::new(&ms).expect("build calibration engine");
    let spectral_window = ms.spectral_window().expect("SPECTRAL_WINDOW accessor");
    let channel_frequencies_hz = spectral_window
        .chan_freq(0)
        .expect("SPW channel frequencies");
    let source_freq_ref =
        FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(0).expect("MEAS_FREQ_REF"))
            .expect("supported spectral reference");
    assert_eq!(source_freq_ref, FrequencyRef::TOPO);

    let field_ids = main_ids::field_id(ms.main_table());
    let first_field_one_row = (0..ms.main_table().row_count())
        .find(|&candidate| field_ids.get(candidate).expect("candidate field id") == 1)
        .expect("row for field 1");
    let first_row_time_mjd_sec = TimeColumn::new(ms.main_table())
        .get_mjd_seconds(first_field_one_row)
        .expect("first field-1 row time");
    let frame = engine
        .spectral_frame_observatory(first_row_time_mjd_sec, 1)
        .expect("first field-1 spectral frame");

    let topo_hz = channel_frequencies_hz[106];
    let geo_hz = MFrequency::new(topo_hz, source_freq_ref)
        .convert_to(FrequencyRef::GEO, &frame)
        .expect("convert TOPO to GEO")
        .hz();
    let bary_hz = MFrequency::new(topo_hz, source_freq_ref)
        .convert_to(FrequencyRef::BARY, &frame)
        .expect("convert TOPO to BARY")
        .hz();
    let lsrk_hz = MFrequency::new(topo_hz, source_freq_ref)
        .convert_to(FrequencyRef::LSRK, &frame)
        .expect("convert TOPO to LSRK")
        .hz();
    let rust_geo_to_topo_ms = MRadialVelocity::new(0.0, RadialVelocityRef::GEO)
        .convert_to(RadialVelocityRef::TOPO, &frame)
        .expect("convert GEO to TOPO radial velocity")
        .ms();
    let rust_topo_to_geo_ms = MRadialVelocity::new(0.0, RadialVelocityRef::TOPO)
        .convert_to(RadialVelocityRef::GEO, &frame)
        .expect("convert TOPO to GEO radial velocity")
        .ms();
    let position_itrf = frame.position().expect("frame position").as_itrf();
    let position_wgs84 = frame
        .position()
        .expect("frame position")
        .convert_to(PositionRef::WGS84)
        .expect("frame WGS84");
    let direction = frame.direction().expect("frame direction");
    let app_direction = direction
        .convert_to(DirectionRef::APP, &frame)
        .expect("J2000 to APP");
    let epoch = frame.epoch().expect("frame epoch");
    let ut1_epoch = epoch.convert_to(EpochRef::UT1, &frame).expect("UTC to UT1");
    let gmst_epoch = ut1_epoch
        .convert_to(EpochRef::GMST1, &frame)
        .expect("UT1 to GMST1");
    let gast_epoch = ut1_epoch
        .convert_to(EpochRef::GAST, &frame)
        .expect("UT1 to GAST");
    let last_epoch = epoch
        .convert_to(EpochRef::LAST, &frame)
        .expect("UTC to LAST");
    let tdb_epoch = epoch.convert_to(EpochRef::TDB, &frame).expect("UTC to TDB");
    let cpp_geo_to_topo_ms = cpp_radvel_convert(
        0.0,
        "GEO",
        "TOPO",
        direction.longitude_rad(),
        direction.latitude_rad(),
        direction.refer().as_str(),
        epoch.value().as_mjd(),
        position_wgs84.longitude_rad(),
        position_wgs84.latitude_rad(),
        position_wgs84.values()[2],
    )
    .expect("C++ GEO to TOPO radial velocity");
    let cpp_topo_to_geo_ms = cpp_radvel_convert(
        0.0,
        "TOPO",
        "GEO",
        direction.longitude_rad(),
        direction.latitude_rad(),
        direction.refer().as_str(),
        epoch.value().as_mjd(),
        position_wgs84.longitude_rad(),
        position_wgs84.latitude_rad(),
        position_wgs84.values()[2],
    )
    .expect("C++ TOPO to GEO radial velocity");
    let cpp_geo_hz = cpp_frequency_convert(
        topo_hz,
        "TOPO",
        "GEO",
        direction.longitude_rad(),
        direction.latitude_rad(),
        direction.refer().as_str(),
        epoch.value().as_mjd(),
        position_wgs84.longitude_rad(),
        position_wgs84.latitude_rad(),
        position_wgs84.values()[2],
    )
    .expect("C++ TOPO to GEO");
    let cpp_bary_hz = cpp_frequency_convert(
        topo_hz,
        "TOPO",
        "BARY",
        direction.longitude_rad(),
        direction.latitude_rad(),
        direction.refer().as_str(),
        epoch.value().as_mjd(),
        position_wgs84.longitude_rad(),
        position_wgs84.latitude_rad(),
        position_wgs84.values()[2],
    )
    .expect("C++ TOPO to BARY");
    let cpp_lsrk_hz = cpp_frequency_convert(
        topo_hz,
        "TOPO",
        "LSRK",
        direction.longitude_rad(),
        direction.latitude_rad(),
        direction.refer().as_str(),
        epoch.value().as_mjd(),
        position_wgs84.longitude_rad(),
        position_wgs84.latitude_rad(),
        position_wgs84.values()[2],
    )
    .expect("C++ TOPO to LSRK");
    eprintln!("first_field1_row={first_field_one_row} time_s={first_row_time_mjd_sec}");
    eprintln!(
        "direction ref={} lon={} lat={}",
        direction.refer().as_str(),
        direction.longitude_rad(),
        direction.latitude_rad(),
    );
    eprintln!(
        "app_direction lon={} lat={}",
        app_direction.longitude_rad(),
        app_direction.latitude_rad(),
    );
    eprintln!(
        "epoch ut1_mjd={} gmst_mjd={} gast_mjd={} last_mjd={} tdb_mjd={}",
        ut1_epoch.value().as_mjd(),
        gmst_epoch.value().as_mjd(),
        gast_epoch.value().as_mjd(),
        last_epoch.value().as_mjd(),
        tdb_epoch.value().as_mjd(),
    );
    eprintln!(
        "radvel geo_to_topo_ms={} cpp_geo_to_topo_ms={} topo_to_geo_ms={} cpp_topo_to_geo_ms={}",
        rust_geo_to_topo_ms, cpp_geo_to_topo_ms, rust_topo_to_geo_ms, cpp_topo_to_geo_ms,
    );
    eprintln!("observatory itrf={position_itrf:?}");
    eprintln!(
        "observatory wgs84=({}, {}, {})",
        position_wgs84.longitude_rad(),
        position_wgs84.latitude_rad(),
        position_wgs84.values()[2],
    );
    eprintln!(
        "channel106 topo={topo_hz} geo={geo_hz} cpp_geo={cpp_geo_hz} bary={bary_hz} cpp_bary={cpp_bary_hz} lsrk={lsrk_hz} cpp_lsrk={cpp_lsrk_hz}",
    );
}

#[test]
fn debug_refim_cband_dual_frame_topo_to_lsrk_for_field1_channel106() {
    let Some(ms_path) = refim_cband_g37line_ms_path() else {
        eprintln!("skipping dual-frame spectral debug test: refim_Cband.G37line.ms not available");
        return;
    };

    let ms = MeasurementSet::open(&ms_path).expect("open refim_Cband.G37line.ms");
    let engine = MsCalEngine::new(&ms).expect("build calibration engine");
    let spectral_window = ms.spectral_window().expect("SPECTRAL_WINDOW accessor");
    let channel_frequencies_hz = spectral_window
        .chan_freq(0)
        .expect("SPW channel frequencies");
    let topo_hz = channel_frequencies_hz[106];

    let field_ids = main_ids::field_id(ms.main_table());
    let field_one_rows = (0..ms.main_table().row_count())
        .filter(|&candidate| field_ids.get(candidate).expect("candidate field id") == 1)
        .collect::<Vec<_>>();
    assert!(
        field_one_rows.len() >= 3,
        "expected multiple field-1 rows in refim_Cband.G37line.ms"
    );

    let reference_row = field_one_rows[0];
    let reference_time_mjd_sec = TimeColumn::new(ms.main_table())
        .get_mjd_seconds(reference_row)
        .expect("reference row time");
    let output_frame = engine
        .spectral_frame_observatory(reference_time_mjd_sec, 1)
        .expect("output spectral frame");
    let output_direction = output_frame.direction().expect("output direction");
    let output_observatory = output_frame
        .position()
        .expect("output position")
        .convert_to(PositionRef::WGS84)
        .expect("output observatory WGS84");
    let output_epoch_mjd = output_frame.epoch().expect("output epoch").value().as_mjd();

    for &row in &[
        field_one_rows[field_one_rows.len() / 2],
        *field_one_rows.last().unwrap(),
    ] {
        let row_time_mjd_sec = TimeColumn::new(ms.main_table())
            .get_mjd_seconds(row)
            .expect("row time");
        let source_frame = engine
            .spectral_frame_observatory(row_time_mjd_sec, 1)
            .expect("source spectral frame");
        let source_direction = source_frame.direction().expect("source direction");
        let source_observatory = source_frame
            .position()
            .expect("source position")
            .convert_to(PositionRef::WGS84)
            .expect("source observatory WGS84");
        let source_epoch_mjd = source_frame.epoch().expect("source epoch").value().as_mjd();

        let rust_multi_hop_hz = MFrequency::new(topo_hz, FrequencyRef::TOPO)
            .convert_to(FrequencyRef::GEO, &source_frame)
            .expect("TOPO to GEO")
            .convert_to(FrequencyRef::BARY, &source_frame)
            .expect("GEO to BARY")
            .convert_to(FrequencyRef::LSRK, &output_frame)
            .expect("BARY to LSRK")
            .hz();
        let cpp_direct_hz = cpp_frequency_convert_between_frames(
            topo_hz,
            "TOPO",
            "LSRK",
            source_direction.longitude_rad(),
            source_direction.latitude_rad(),
            source_direction.refer().as_str(),
            source_epoch_mjd,
            source_observatory.longitude_rad(),
            source_observatory.latitude_rad(),
            source_observatory.values()[2],
            output_direction.longitude_rad(),
            output_direction.latitude_rad(),
            output_direction.refer().as_str(),
            output_epoch_mjd,
            output_observatory.longitude_rad(),
            output_observatory.latitude_rad(),
            output_observatory.values()[2],
        )
        .expect("C++ dual-frame TOPO to LSRK");

        eprintln!(
            "row={row} topo={topo_hz} rust_multi_hop={rust_multi_hop_hz} cpp_dual_frame={cpp_direct_hz} diff_hz={}",
            rust_multi_hop_hz - cpp_direct_hz
        );
    }
}
