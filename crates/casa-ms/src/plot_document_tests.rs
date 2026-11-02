// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use flate2::read::GzDecoder;
use tempfile::TempDir;

use super::*;

fn unpack_small_ms() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fixture = File::open(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mssel_test_small.ms.tgz"),
    )
    .expect("small MS fixture");
    tar::Archive::new(GzDecoder::new(fixture))
        .unpack(dir.path())
        .expect("unpack MS fixture");
    let ms_path = dir.path().join("mssel_test_small.ms");
    (dir, ms_path)
}

fn request(
    ms_path: &Path,
    preset: MsPlotPreset,
    max_plot_points: usize,
) -> MeasurementSetPlotRequest {
    MeasurementSetPlotRequest {
        data_column: "DATA".to_string(),
        max_plot_points,
        ..MeasurementSetPlotRequest::new(ms_path, preset)
    }
}

fn assert_preset_document(preset: MsPlotPreset, plot: &MeasurementSetPlot) {
    assert_eq!(plot.preset, preset);
    assert_eq!(plot.data_column, "data");
    assert!(!plot.title.is_empty());
    assert!(!plot.x_axis.label.is_empty());
    assert!(!plot.y_axis.label.is_empty());
    assert_eq!(plot.document.title, plot.title);
    assert!(
        !plot.document.layers.is_empty() || !plot.document.panels.is_empty(),
        "plot document should expose manipulable layers or panels for {preset:?}"
    );
    match preset {
        MsPlotPreset::UvCoverage => {
            assert_eq!(
                plot.document.layers.len() as u64,
                plot.sampling.series_count
            );
            assert!(
                plot.document.layers.len() <= 32,
                "UV coverage should coalesce baseline tracks into coarse display groups"
            );
        }
        MsPlotPreset::AntennaLayout => {
            let layer = plot.document.layers.first().expect("antenna layer");
            assert_eq!(layer.x_values.len(), layer.point_labels.len());
            assert_eq!(layer.x_values.len(), layer.point_symbol_sizes.len());
        }
        MsPlotPreset::ScanTimeline | MsPlotPreset::SpectralWindowCoverage => {
            assert!(
                plot.document
                    .layers
                    .iter()
                    .any(|layer| !layer.interval_labels.is_empty()),
                "interval labels should survive document conversion for {preset:?}"
            );
            assert!(
                plot.document
                    .layers
                    .iter()
                    .any(|layer| !layer.color_group.is_empty()),
                "interval color groups should survive document conversion for {preset:?}"
            );
        }
        _ => {}
    }
    if matches!(
        preset,
        MsPlotPreset::AntennaLayout
            | MsPlotPreset::ScanTimeline
            | MsPlotPreset::SpectralWindowCoverage
    ) {
        assert!(plot.sampling.diagnostics.iter().any(|line| {
            line.contains("metadata-oriented plot") || line.contains("no drawable")
        }));
    } else {
        assert!(!plot.series.is_empty());
        assert!(plot.sampling.rendered_point_count > 0);
    }
    assert_eq!(plot.sampling.requested_max_points, 10_000);
    assert!(plot.selection_summary.contains("data column data"));
}

#[test]
fn every_preset_builds_a_typed_document() {
    let (_dir, ms_path) = unpack_small_ms();
    for preset in MsPlotPreset::ALL {
        let plot = match build_measurement_set_plot(&request(&ms_path, preset, 10_000)) {
            Ok(plot) => plot,
            Err(MeasurementSetPlotError::Plot(reason))
                if matches!(
                    preset,
                    MsPlotPreset::WeightSpectrumVsTime | MsPlotPreset::SigmaSpectrumVsTime
                ) && (reason.contains("requires") || reason.contains("shape [0, 0]")) =>
            {
                continue;
            }
            Err(error) => panic!("plot {preset:?}: {error}"),
        };
        assert_preset_document(preset, &plot);
    }
}

#[test]
fn documents_round_trip_through_json_with_snake_case_tags() {
    let (_dir, ms_path) = unpack_small_ms();
    for preset in [
        MsPlotPreset::ScanTimeline,
        MsPlotPreset::AmplitudePhaseVsTimeStacked,
    ] {
        let plot = build_measurement_set_plot(&request(&ms_path, preset, 10_000)).expect("plot");
        let json = serde_json::to_value(&plot).expect("serialize");
        assert_eq!(json["preset"], preset.as_str());
        let layer = json["document"]["layers"]
            .get(0)
            .or_else(|| json["document"]["panels"][0]["layers"].get(0))
            .expect("one layer");
        assert!(matches!(
            layer["kind"].as_str(),
            Some("scatter" | "interval")
        ));
        assert!(matches!(
            layer["payload_strategy"].as_str(),
            Some("point_cloud" | "intervals")
        ));
        let decoded: MeasurementSetPlot = serde_json::from_value(json).expect("deserialize");
        assert_eq!(decoded, plot);
    }
}

#[test]
fn request_selectors_are_trimmed_and_all_selects_everything() {
    let (_dir, ms_path) = unpack_small_ms();
    let everything =
        build_measurement_set_plot(&request(&ms_path, MsPlotPreset::AmplitudeVsTime, 10_000))
            .expect("plot");
    let spelled = build_measurement_set_plot(&MeasurementSetPlotRequest {
        field: Some(" all ".to_string()),
        spw: Some(String::new()),
        data_column: "  ".to_string(),
        ..request(&ms_path, MsPlotPreset::AmplitudeVsTime, 10_000)
    })
    .expect("plot");
    assert_eq!(spelled.selection_summary, "data column data");
    assert_eq!(spelled.series, everything.series);
    let selected = build_measurement_set_plot(&MeasurementSetPlotRequest {
        field: Some(" 1 ".to_string()),
        ..request(&ms_path, MsPlotPreset::AmplitudeVsTime, 10_000)
    })
    .expect("plot");
    assert_eq!(selected.selection_summary, "data column data, field 1");
}

#[test]
fn invalid_paths_and_options_name_their_reason() {
    let (_dir, ms_path) = unpack_small_ms();
    let missing = build_measurement_set_plot(&MeasurementSetPlotRequest::new(
        "missing.ms",
        MsPlotPreset::AmplitudeVsTime,
    ))
    .expect_err("missing MeasurementSet");
    assert_eq!(
        missing,
        MeasurementSetPlotError::InvalidPath("missing.ms is not a MeasurementSet directory".into())
    );
    let column = build_measurement_set_plot(&MeasurementSetPlotRequest {
        data_column: "nope".to_string(),
        ..request(&ms_path, MsPlotPreset::AmplitudeVsTime, 10_000)
    })
    .expect_err("unknown data column");
    assert_eq!(
        column.to_string(),
        format!(
            "{}: unsupported msexplore data column \"nope\"",
            ms_path.display()
        )
    );
}

#[test]
#[ignore = "diagnostic timing helper for local tutorial MeasurementSets"]
fn twhya_measurement_set_plot_timing_diagnostic() {
    let ms_path = std::env::var("CASA_RS_TWHYA_MS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/private/tmp/casa-rs-wave6-prof/twhya_calibrated.ms"));
    if !ms_path.is_dir() {
        eprintln!("skipping: {} is not staged", ms_path.display());
        return;
    }
    for preset in MsPlotPreset::ALL {
        let started = Instant::now();
        let plot = build_measurement_set_plot(&request(&ms_path, preset, 250_000))
            .unwrap_or_else(|error| panic!("plot {preset:?}: {error}"));
        eprintln!(
            "{preset:?}: total={} ms, points={}, layers={}, panels={}, diagnostics={:?}",
            started.elapsed().as_millis(),
            plot.sampling.rendered_point_count,
            plot.document.layers.len(),
            plot.document.panels.len(),
            plot.sampling.diagnostics
        );
    }
}
