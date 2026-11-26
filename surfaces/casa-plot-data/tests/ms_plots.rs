// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::File;
use std::path::{Path, PathBuf};

use casa_ms::{MeasurementSet, MeasurementSetSummaryOptions};
use casa_plot_data::{
    MeasurementSetPlotKind, MeasurementSetPlotPayload,
    build_measurement_set_plot_payload_from_summary, build_measurement_set_visibility_plot_payload,
};
use flate2::read::GzDecoder;
use tar::Archive;
use tempfile::tempdir;

fn unpack_fixture_ms(archive_name: &str) -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().expect("tempdir");
    let archive_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("crates")
        .join("casa-ms")
        .join("tests")
        .join("fixtures")
        .join(archive_name);
    let archive_file = File::open(&archive_path).expect("open fixture archive");
    let mut archive = Archive::new(GzDecoder::new(archive_file));
    archive.unpack(temp.path()).expect("unpack fixture archive");

    let ms_dir_name = archive_name
        .strip_suffix(".tgz")
        .expect("fixture archive suffix");
    let ms_path = temp.path().join(ms_dir_name);
    assert!(
        ms_path.is_dir(),
        "expected unpacked MS at {}",
        ms_path.display()
    );
    (temp, ms_path)
}

#[test]
fn antenna_plot_uses_offset_coordinates_for_real_alma_fixture() {
    let (_temp, ms_path) = unpack_fixture_ms("mssel_test_small.ms.tgz");
    let summary = MeasurementSet::open(&ms_path)
        .expect("open MS")
        .summary()
        .expect("summary");

    let spec = MeasurementSetPlotKind::AntennaLayout.default_spec();
    let payload =
        build_measurement_set_plot_payload_from_summary(&summary, &spec).expect("plot payload");
    let MeasurementSetPlotPayload::AntennaLayout(payload) = payload else {
        panic!("expected antenna layout payload");
    };

    assert_eq!(payload.x_label, "East Offset (m)");
    assert_eq!(payload.y_label, "North Offset (m)");
    assert!(
        payload
            .antennas
            .iter()
            .any(|point| point.x.abs() > 1.0 || point.y.abs() > 1.0)
    );
}

#[test]
fn curated_visibility_plots_build_stable_payloads_from_real_fixture() {
    let (_temp, ms_path) = unpack_fixture_ms("mssel_test_small_multifield_spw.ms.tgz");
    let ms = MeasurementSet::open(&ms_path).expect("open MS");
    let options = MeasurementSetSummaryOptions {
        field: Some("NGC4826-F3".to_string()),
        spw: Some("5".to_string()),
        ..MeasurementSetSummaryOptions::default()
    };

    let cases = [
        (
            MeasurementSetPlotKind::AmplitudeVsTime,
            "Time (MJD seconds)",
            "Amplitude",
            None,
            "field-5",
            "NGC4826-F3",
        ),
        (
            MeasurementSetPlotKind::PhaseVsTime,
            "Time (MJD seconds)",
            "Phase (deg)",
            Some((-180.0, 180.0)),
            "field-5",
            "NGC4826-F3",
        ),
        (
            MeasurementSetPlotKind::AmplitudeVsUvDistance,
            "UV Distance (m)",
            "Amplitude",
            None,
            "spw-5",
            "",
        ),
    ];

    for (kind, x_label, y_label, fixed_y_bounds, color_group, expected_label) in cases {
        let spec = kind.default_spec();
        let payload = build_measurement_set_visibility_plot_payload(&ms, &options, &spec)
            .expect("visibility plot payload");
        let MeasurementSetPlotPayload::VisibilityScatter(payload) = payload else {
            panic!("expected visibility scatter payload");
        };

        assert_eq!(payload.kind, kind);
        assert_eq!(payload.x_label, x_label);
        assert_eq!(payload.y_label, y_label);
        assert_eq!(payload.fixed_y_bounds, fixed_y_bounds);
        assert_eq!(payload.series.len(), 1);
        assert_eq!(payload.series[0].color_group, color_group);
        assert_eq!(payload.series[0].label, expected_label);
        assert!(payload.series[0].points.len() > 1);
        assert!(payload.summary.contains("Rows="));
        assert!(payload.summary.contains("Points="));
        assert!(payload.summary.contains("Data column=data"));
    }
}
