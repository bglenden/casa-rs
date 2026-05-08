// SPDX-License-Identifier: LGPL-3.0-or-later

#![cfg(feature = "cpp-interop-tests")]

use casa_ms::MeasurementSet;
use casa_ms::columns::time_columns::TimeColumn;
use casa_ms::derived::engine::MsCalEngine;
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

fn refim_point_ms_path() -> Option<std::path::PathBuf> {
    casatestdata_path_for_tier(
        CasaTestDataTier::SlowParity,
        "measurementset/vla/refim_point.ms",
    )
    .filter(|path| path.exists())
}

#[test]
fn refim_point_topo_to_lsrk_rowwise_frequency_matches_casa() {
    let Some(ms_path) = refim_point_ms_path() else {
        eprintln!("skipping spectral-frame parity test: refim_point.ms not available");
        return;
    };

    let ms = MeasurementSet::open(&ms_path).expect("open refim_point.ms");
    let engine = MsCalEngine::new(&ms).expect("build calibration engine");
    let spectral_window = ms.spectral_window().expect("SPECTRAL_WINDOW accessor");
    let channel_frequencies_hz = spectral_window
        .chan_freq(0)
        .expect("SPW channel frequencies");
    let source_freq_ref =
        FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(0).expect("MEAS_FREQ_REF"))
            .expect("supported spectral reference");
    assert_eq!(source_freq_ref, FrequencyRef::TOPO);

    let time_column = TimeColumn::new(ms.main_table());
    let expected = [
        (0usize, 999_988_750.387_256_3, 1_049_988_187.906_619_1),
        (1_000usize, 999_988_754.393_651_1, 1_049_988_192.113_333_7),
        (5_000usize, 999_988_779.769_039_5, 1_049_988_218.757_491_6),
        (10_000usize, 999_988_812.195_408_6, 1_049_988_252.805_179_1),
    ];

    for (row, expected_chan0_hz, expected_chan1_hz) in expected {
        let row_time_mjd_sec = time_column.get_mjd_seconds(row).expect("row time");
        let frame = engine
            .spectral_frame_observatory(row_time_mjd_sec, 0)
            .expect("spectral frame");
        let rust_chan0_hz = MFrequency::new(channel_frequencies_hz[0], source_freq_ref)
            .convert_to(FrequencyRef::LSRK, &frame)
            .expect("convert channel 0 to LSRK")
            .hz();
        let rust_chan1_hz = MFrequency::new(channel_frequencies_hz[1], source_freq_ref)
            .convert_to(FrequencyRef::LSRK, &frame)
            .expect("convert channel 1 to LSRK")
            .hz();

        assert!(
            (rust_chan0_hz - expected_chan0_hz).abs() < 1.0,
            "row {row} channel 0 LSRK mismatch: rust={rust_chan0_hz} CASA={expected_chan0_hz}"
        );
        assert!(
            (rust_chan1_hz - expected_chan1_hz).abs() < 1.0,
            "row {row} channel 1 LSRK mismatch: rust={rust_chan1_hz} CASA={expected_chan1_hz}"
        );
    }
}
