// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cmp::Ordering;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use casacore_ms::{
    MeasurementSet, MsAxis, MsPlotPayload, MsPlotPreset, MsPlotSpec, MsSelectionSpec,
    build_msexplore_plot_payload,
};
use image::{GenericImageView, ImageReader};
use serde_json::json;
use tempfile::tempdir;

mod common;

use common::casa_plotms::{discover_casa_python, ngc5921_ms_path, skip_reason};

fn casa_plotms_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn amplitude_vs_time_txt_manifest_tracks_casa_plotms_line_count() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_time",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    let rust_points = count_rust_points(&rust);
    let casa_points = count_casa_points(&casa);
    assert!(
        rust_points > 0,
        "expected rust manifest to contain plotted points"
    );
    assert!(
        casa_points > 0,
        "expected CASA txt export to contain plotted points"
    );
    assert_eq!(rust_points, casa_points);
}

#[test]
fn amplitude_vs_channel_avgchannel_txt_manifest_tracks_casa_plotms_line_count() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_channel",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
        "--avgchannel",
        "8",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "chan"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
        ("avgchannel", "8"),
    ])
    .expect("run casa plotms");

    let rust_points = count_rust_points(&rust);
    let casa_points = count_casa_points(&casa);
    assert!(
        rust_points > 0,
        "expected rust manifest to contain plotted points"
    );
    assert!(
        casa_points > 0,
        "expected CASA txt export to contain plotted points"
    );
    assert_eq!(rust_points, casa_points);
}

#[test]
fn amplitude_and_phase_vs_time_match_separate_casa_plotms_txt_exports() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "time",
        "--yaxis",
        "amplitude",
        "--yaxis2",
        "phase",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let rust_by_axis = rust_dual_axis_points(&rust);

    let casa_amp = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms amp");
    let casa_phase = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "phase"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms phase");

    assert_points_match(
        rust_by_axis
            .get("amplitude")
            .expect("rust amplitude points")
            .as_slice(),
        &casa_xy_points(&casa_amp),
    );
    assert_points_match(
        rust_by_axis
            .get("phase")
            .expect("rust phase points")
            .as_slice(),
        &casa_xy_points(&casa_phase),
    );
}

#[test]
fn amplitude_and_phase_vs_time_png_export_tracks_casa_dual_axis_ranges() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let ms_path = ngc5921_ms_path().expect("shared ngc5921.ms");
    let ms = MeasurementSet::open(&ms_path).expect("open shared ngc5921.ms");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.y_axes.push(MsAxis::Phase);
    let selection = MsSelectionSpec {
        field: Some("0".to_string()),
        spw: Some("0".to_string()),
        scan: Some("1".to_string()),
        ..Default::default()
    };
    let payload =
        build_msexplore_plot_payload(&ms, &selection, &spec).expect("build rust dual-y payload");
    let MsPlotPayload::Scatter(payload) = payload else {
        panic!("expected scatter payload");
    };
    let rust_amp = numeric_range(
        payload
            .series
            .iter()
            .filter(|series| series.y_axis == MsAxis::Amplitude)
            .flat_map(|series| series.points.iter().map(|point| point.1)),
    )
    .expect("rust amplitude range");
    let rust_phase = numeric_range(
        payload
            .series
            .iter()
            .filter(|series| series.y_axis == MsAxis::Phase)
            .flat_map(|series| series.points.iter().map(|point| point.1)),
    )
    .expect("rust phase range");

    let rust_png = run_rust_msexplore_png(&[
        "--xaxis",
        "time",
        "--yaxis",
        "amplitude",
        "--yaxis2",
        "phase",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust dual-y png");
    let casa = run_casa_plotms_png_expr(
        &[
            ("xaxis", "time"),
            ("field", "0"),
            ("spw", "0"),
            ("scan", "1"),
        ],
        &[
            ("yaxis", "['amp', 'phase']"),
            ("yaxislocation", "['left', 'right']"),
        ],
    )
    .expect("run casa dual-y png");

    assert_same_image_dimensions(&rust_png, &casa.body);

    let casa_amp = parse_casa_axis_range(&casa.log, "Amp:data").expect("CASA amp range");
    let casa_phase = parse_casa_axis_range(&casa.log, "Phase:data").expect("CASA phase range");
    assert!(
        (rust_amp.0 - casa_amp.0).abs() <= 1.1e-3,
        "amp min mismatch: rust={} casa={}",
        rust_amp.0,
        casa_amp.0
    );
    assert!(
        (rust_amp.1 - casa_amp.1).abs() <= 1.1e-3,
        "amp max mismatch: rust={} casa={}",
        rust_amp.1,
        casa_amp.1
    );
    assert!(
        (rust_phase.0 - casa_phase.0).abs() <= 1.1e-3,
        "phase min mismatch: rust={} casa={}",
        rust_phase.0,
        casa_phase.0
    );
    assert!(
        (rust_phase.1 - casa_phase.1).abs() <= 1.1e-3,
        "phase max mismatch: rust={} casa={}",
        rust_phase.1,
        casa_phase.1
    );
}

#[test]
fn amplitude_phase_vs_time_stacked_matches_separate_casa_plotms_txt_exports() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_phase_vs_time_stacked",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust stacked msexplore");
    let rust_by_plot = rust_page_points_by_plot(&rust);

    let casa_amp = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms amp");
    let casa_phase = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "phase"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms phase");

    assert_points_match(
        rust_by_plot.get(&0).expect("stacked amplitude plot points"),
        &casa_xy_points(&casa_amp),
    );
    assert_points_match(
        rust_by_plot.get(&1).expect("stacked phase plot points"),
        &casa_xy_points(&casa_phase),
    );
}

#[test]
fn amplitude_phase_vs_time_stacked_png_export_matches_casa_multipanel_page() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust_png = run_rust_msexplore_png(&[
        "--preset",
        "amplitude_phase_vs_time_stacked",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust stacked png");
    let casa = run_casa_plotms_sequence_png(&[
        CasaPlotmsCall {
            kwargs: vec![
                ("gridrows", "2"),
                ("gridcols", "1"),
                ("rowindex", "0"),
                ("colindex", "0"),
                ("plotindex", "0"),
                ("xaxis", "'time'"),
                ("yaxis", "'amp'"),
                ("field", "'0'"),
                ("spw", "'0'"),
                ("scan", "'1'"),
                ("clearplots", "True"),
            ],
        },
        CasaPlotmsCall {
            kwargs: vec![
                ("gridrows", "2"),
                ("gridcols", "1"),
                ("rowindex", "1"),
                ("colindex", "0"),
                ("plotindex", "1"),
                ("xaxis", "'time'"),
                ("yaxis", "'phase'"),
                ("field", "'0'"),
                ("spw", "'0'"),
                ("scan", "'1'"),
                ("clearplots", "False"),
            ],
        },
    ])
    .expect("run casa stacked page png");

    assert_same_image_dimensions(&rust_png, &casa.body);
    assert_vertical_halves_have_signal(&rust_png);
    assert_vertical_halves_have_signal(&casa.body);
}

#[test]
fn generic_page_spec_side_by_side_matches_separate_casa_plotms_txt_exports() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore_page_spec(
        &json!({
            "page_title": "Amplitude and Phase Side by Side",
            "gridrows": 1,
            "gridcols": 2,
            "plots": [
                {
                    "preset": "amplitude_vs_time",
                    "plotindex": 0,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude vs Time"
                },
                {
                    "preset": "phase_vs_time",
                    "plotindex": 1,
                    "rowindex": 0,
                    "colindex": 1,
                    "title": "Phase vs Time"
                }
            ]
        }),
        &["--field", "0", "--spw", "0", "--scan", "1"],
    )
    .expect("run rust page-spec msexplore");
    let rust_by_plot = rust_page_points_by_plot(&rust);

    let casa_amp = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms amp");
    let casa_phase = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "phase"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms phase");

    assert_points_match(
        rust_by_plot.get(&0).expect("page amplitude plot points"),
        &casa_xy_points(&casa_amp),
    );
    assert_points_match(
        rust_by_plot.get(&1).expect("page phase plot points"),
        &casa_xy_points(&casa_phase),
    );
}

#[test]
fn generic_page_spec_side_by_side_png_export_matches_casa_multipanel_page() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust_png = run_rust_msexplore_page_spec_png(
        &json!({
            "page_title": "Amplitude and Phase Side by Side",
            "gridrows": 1,
            "gridcols": 2,
            "plots": [
                {
                    "preset": "amplitude_vs_time",
                    "plotindex": 0,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude vs Time"
                },
                {
                    "preset": "phase_vs_time",
                    "plotindex": 1,
                    "rowindex": 0,
                    "colindex": 1,
                    "title": "Phase vs Time"
                }
            ]
        }),
        &["--field", "0", "--spw", "0", "--scan", "1"],
    )
    .expect("run rust page-spec png");
    let casa = run_casa_plotms_sequence_png(&[
        CasaPlotmsCall {
            kwargs: vec![
                ("gridrows", "1"),
                ("gridcols", "2"),
                ("rowindex", "0"),
                ("colindex", "0"),
                ("plotindex", "0"),
                ("xaxis", "'time'"),
                ("yaxis", "'amp'"),
                ("field", "'0'"),
                ("spw", "'0'"),
                ("scan", "'1'"),
                ("clearplots", "True"),
            ],
        },
        CasaPlotmsCall {
            kwargs: vec![
                ("gridrows", "1"),
                ("gridcols", "2"),
                ("rowindex", "0"),
                ("colindex", "1"),
                ("plotindex", "1"),
                ("xaxis", "'time'"),
                ("yaxis", "'phase'"),
                ("field", "'0'"),
                ("spw", "'0'"),
                ("scan", "'1'"),
                ("clearplots", "False"),
            ],
        },
    ])
    .expect("run casa side-by-side page png");

    assert_same_image_dimensions(&rust_png, &casa.body);
    assert_horizontal_halves_have_signal(&rust_png);
    assert_horizontal_halves_have_signal(&casa.body);
}

#[test]
fn generic_page_spec_same_cell_overplot_matches_separate_casa_plotms_txt_exports() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore_page_spec(
        &json!({
            "page_title": "Amplitude Overplot",
            "exprange": "all",
            "gridrows": 1,
            "gridcols": 1,
            "plots": [
                {
                    "preset": "amplitude_vs_time",
                    "plotindex": 0,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude:vector"
                },
                {
                    "preset": "amplitude_vs_time",
                    "scalar": true,
                    "plotindex": 1,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude:scalar"
                }
            ]
        }),
        &["--field", "0", "--spw", "0", "--scan", "1"],
    )
    .expect("run rust overplot page-spec msexplore");
    let rust_by_plot = rust_page_points_by_plot(&rust);

    let casa_vector = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms vector");
    let casa_scalar = run_casa_plotms_expr(
        &[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("field", "0"),
            ("spw", "0"),
            ("scan", "1"),
        ],
        &[("scalar", "True")],
    )
    .expect("run casa plotms scalar");

    assert_points_match(
        rust_by_plot.get(&0).expect("overplot vector plot points"),
        &casa_xy_points(&casa_vector),
    );
    assert_points_match(
        rust_by_plot.get(&1).expect("overplot scalar plot points"),
        &casa_xy_points(&casa_scalar),
    );
}

#[test]
fn generic_page_spec_same_cell_overplot_png_export_matches_casa_page() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust_png = run_rust_msexplore_page_spec_png(
        &json!({
            "page_title": "Amplitude Overplot",
            "exprange": "all",
            "gridrows": 1,
            "gridcols": 1,
            "plots": [
                {
                    "preset": "amplitude_vs_time",
                    "plotindex": 0,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude:vector"
                },
                {
                    "preset": "amplitude_vs_time",
                    "scalar": true,
                    "plotindex": 1,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude:scalar"
                }
            ]
        }),
        &["--field", "0", "--spw", "0", "--scan", "1"],
    )
    .expect("run rust overplot page png");
    let casa = run_casa_plotms_sequence_png(&[
        CasaPlotmsCall {
            kwargs: vec![
                ("gridrows", "1"),
                ("gridcols", "1"),
                ("rowindex", "0"),
                ("colindex", "0"),
                ("plotindex", "0"),
                ("xaxis", "'time'"),
                ("yaxis", "'amp'"),
                ("field", "'0'"),
                ("spw", "'0'"),
                ("scan", "'1'"),
                ("clearplots", "True"),
            ],
        },
        CasaPlotmsCall {
            kwargs: vec![
                ("gridrows", "1"),
                ("gridcols", "1"),
                ("rowindex", "0"),
                ("colindex", "0"),
                ("plotindex", "1"),
                ("xaxis", "'time'"),
                ("yaxis", "'amp'"),
                ("scalar", "True"),
                ("field", "'0'"),
                ("spw", "'0'"),
                ("scan", "'1'"),
                ("clearplots", "False"),
            ],
        },
    ])
    .expect("run casa overplot page png");

    assert_same_image_dimensions(&rust_png, &casa.body);
    assert_image_has_signal(&rust_png);
    assert_image_has_signal(&casa.body);
}

#[test]
fn page_presentation_png_export_tracks_casa_header_and_legend_regions() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust_png = run_rust_msexplore_page_spec_png(
        &json!({
            "page_title": "Amplitude Overplot Presentation",
            "exprange": "all",
            "gridrows": 1,
            "gridcols": 1,
            "plots": [
                {
                    "preset": "amplitude_vs_time",
                    "plotindex": 0,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude:vector",
                    "showlegend": true,
                    "legendposition": "exteriorRight"
                },
                {
                    "preset": "amplitude_vs_time",
                    "scalar": true,
                    "plotindex": 1,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude:scalar",
                    "showlegend": true,
                    "legendposition": "exteriorRight"
                }
            ]
        }),
        &[
            "--field",
            "0",
            "--spw",
            "0",
            "--scan",
            "1",
            "--headeritems",
            "filename,observer,projid",
        ],
    )
    .expect("run rust presentation page png");
    let casa = run_casa_plotms_sequence_png(&[
        CasaPlotmsCall {
            kwargs: vec![
                ("gridrows", "1"),
                ("gridcols", "1"),
                ("rowindex", "0"),
                ("colindex", "0"),
                ("plotindex", "0"),
                ("xaxis", "'time'"),
                ("yaxis", "'amp'"),
                ("field", "'0'"),
                ("spw", "'0'"),
                ("scan", "'1'"),
                ("showlegend", "True"),
                ("legendposition", "'exteriorRight'"),
                ("headeritems", "'filename,observer,projid'"),
                ("title", "'Amplitude Overplot Presentation'"),
                ("clearplots", "True"),
            ],
        },
        CasaPlotmsCall {
            kwargs: vec![
                ("gridrows", "1"),
                ("gridcols", "1"),
                ("rowindex", "0"),
                ("colindex", "0"),
                ("plotindex", "1"),
                ("xaxis", "'time'"),
                ("yaxis", "'amp'"),
                ("scalar", "True"),
                ("field", "'0'"),
                ("spw", "'0'"),
                ("scan", "'1'"),
                ("showlegend", "True"),
                ("legendposition", "'exteriorRight'"),
                ("headeritems", "'filename,observer,projid'"),
                ("title", "'Amplitude Overplot Presentation'"),
                ("clearplots", "False"),
            ],
        },
    ])
    .expect("run casa presentation page png");

    assert_same_image_width(&rust_png, &casa.body);
    assert_image_has_signal(&rust_png);
    assert_image_has_signal(&casa.body);
    assert_top_band_has_signal(&rust_png);
    assert_top_band_has_signal(&casa.body);
    assert_right_band_has_signal(&rust_png);
    assert_right_band_has_signal(&casa.body);
}

#[test]
fn u_vs_v_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "u", "--yaxis", "v", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "u"),
        ("yaxis", "v"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn amplitude_vs_w_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "w",
        "--yaxis",
        "amplitude",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "w"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn amplitude_vs_velocity_png_export_tracks_casa_plotms_ranges() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "velocity",
        "--yaxis",
        "amplitude",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
        "--freqframe",
        "LSRK",
        "--restfreq",
        "1.420405752GHz",
        "--veldef",
        "RADIO",
    ])
    .expect("run rust msexplore");
    let rust_points = rust_xy_points(&rust);
    let rust_x = numeric_range(rust_points.iter().map(|point| point.0)).expect("rust x range");
    let rust_y = numeric_range(rust_points.iter().map(|point| point.1)).expect("rust y range");
    let rust_png = run_rust_msexplore_png(&[
        "--xaxis",
        "velocity",
        "--yaxis",
        "amplitude",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
        "--freqframe",
        "LSRK",
        "--restfreq",
        "1.420405752GHz",
        "--veldef",
        "RADIO",
    ])
    .expect("run rust msexplore png");
    let casa = run_casa_plotms_png(&[
        ("xaxis", "velocity"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
        ("freqframe", "LSRK"),
        ("restfreq", "1.420405752GHz"),
        ("veldef", "RADIO"),
    ])
    .expect("run casa plotms png");

    assert_same_image_dimensions(&rust_png, &casa.body);

    let point_count = parse_casa_point_count(&casa.log).expect("CASA point count");
    assert_eq!(rust_points.len(), point_count, "point count mismatch");

    let casa_x = parse_casa_axis_range(&casa.log, "Velocity").expect("CASA velocity range");
    let casa_y = parse_casa_axis_range(&casa.log, "Amp:data").expect("CASA amp range");
    assert!(
        (rust_x.0 - casa_x.0).abs() <= 0.05,
        "velocity min mismatch: rust={} casa={}",
        rust_x.0,
        casa_x.0
    );
    assert!(
        (rust_x.1 - casa_x.1).abs() <= 0.05,
        "velocity max mismatch: rust={} casa={}",
        rust_x.1,
        casa_x.1
    );
    assert!(
        (rust_y.0 - casa_y.0).abs() <= 1.0e-3,
        "amplitude min mismatch: rust={} casa={}",
        rust_y.0,
        casa_y.0
    );
    assert!(
        (rust_y.1 - casa_y.1).abs() <= 1.0e-3,
        "amplitude max mismatch: rust={} casa={}",
        rust_y.1,
        casa_y.1
    );
}

#[test]
fn amplitude_vs_time_iteraxis_scan_txt_manifest_tracks_separate_casa_plotms_scans() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_time",
        "--spw",
        "0",
        "--iteraxis",
        "scan",
    ])
    .expect("run rust msexplore");

    let panels = rust_iterated_xy_points(&rust);
    assert!(panels.len() > 1, "expected multiple scan panels");
    for (panel_key, panel_points) in panels {
        let scan = panel_key
            .strip_prefix("scan-")
            .expect("scan panel key prefix");
        let casa = run_casa_plotms(&[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("spw", "0"),
            ("scan", scan),
        ])
        .expect("run casa plotms");
        assert_points_match(&panel_points, &casa_xy_points(&casa));
    }
}

#[test]
fn amplitude_vs_time_iteraxis_field_txt_manifest_tracks_separate_casa_plotms_fields() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_time",
        "--spw",
        "0",
        "--iteraxis",
        "field",
    ])
    .expect("run rust msexplore");

    let panels = rust_iterated_xy_points(&rust);
    assert!(panels.len() > 1, "expected multiple field panels");
    for (panel_key, panel_points) in panels {
        let field = panel_key
            .strip_prefix("field-")
            .expect("field panel key prefix");
        let casa = run_casa_plotms(&[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("field", field),
            ("spw", "0"),
        ])
        .expect("run casa plotms");
        assert_points_match(&panel_points, &casa_xy_points(&casa));
    }
}

#[test]
fn weight_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "time", "--yaxis", "weight", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "wt"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn sigma_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "time", "--yaxis", "sigma", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "sigma"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn flag_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "time", "--yaxis", "flag", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "flag"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn weight_spectrum_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "time",
        "--yaxis",
        "weight_spectrum",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "wtsp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn sigma_spectrum_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis",
        "time",
        "--yaxis",
        "sigma_spectrum",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "sigmasp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn flagrow_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--xaxis", "time", "--yaxis", "flagrow", "--field", "0", "--spw", "0", "--scan", "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "flagrow"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn elevation_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "elevation_vs_time",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "elevation"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn hour_angle_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "hour_angle_vs_time",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "hourang"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn parallactic_angle_vs_time_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "parallactic_angle_vs_time",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "parang"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn azimuth_vs_elevation_txt_manifest_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "azimuth_vs_elevation",
        "--field",
        "0",
        "--spw",
        "0",
        "--scan",
        "1",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "elevation"),
        ("yaxis", "azimuth"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

fn plotms_available() -> bool {
    discover_casa_python().is_some_and(|python| python.plotms_available)
}

fn plotms_shared_dataset_available() -> bool {
    plotms_available() && ngc5921_ms_path().is_some()
}

fn run_rust_msexplore(extra_args: &[&str]) -> Result<String, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    run_rust_msexplore_on(&ms_path, extra_args)
}

fn run_rust_msexplore_page_spec(
    page_spec: &serde_json::Value,
    extra_args: &[&str],
) -> Result<String, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let page_spec_path = temp.path().join("page-spec.json");
    fs::write(&page_spec_path, page_spec.to_string())
        .map_err(|error| format!("write rust page spec: {error}"))?;
    run_rust_msexplore_on_with_page_spec(&ms_path, &page_spec_path, extra_args)
}

fn run_rust_msexplore_png(extra_args: &[&str]) -> Result<Vec<u8>, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("rust-msexplore.png");
    let result = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--plot-output"])
        .arg(&output)
        .args(["--plot-format", "png"])
        .args(["--plot-width", "1600", "--plot-height", "900"])
        .args(extra_args)
        .arg(ms_path)
        .output()
        .map_err(|error| format!("spawn rust msexplore png: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    fs::read(output).map_err(|error| format!("read rust png export: {error}"))
}

fn run_rust_msexplore_page_spec_png(
    page_spec: &serde_json::Value,
    extra_args: &[&str],
) -> Result<Vec<u8>, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("rust-msexplore-page.png");
    let page_spec_path = temp.path().join("page-spec.json");
    fs::write(&page_spec_path, page_spec.to_string())
        .map_err(|error| format!("write rust page spec: {error}"))?;
    let result = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--page-spec"])
        .arg(&page_spec_path)
        .args(["--plot-output"])
        .arg(&output)
        .args(["--plot-format", "png"])
        .args(["--plot-width", "1600", "--plot-height", "900"])
        .args(extra_args)
        .arg(ms_path)
        .output()
        .map_err(|error| format!("spawn rust msexplore page png: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    fs::read(output).map_err(|error| format!("read rust page png export: {error}"))
}

fn run_rust_msexplore_on(ms_path: &Path, extra_args: &[&str]) -> Result<String, String> {
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("rust-msexplore.txt");
    let result = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--plot-output"])
        .arg(&output)
        .args(["--plot-format", "txt"])
        .args(extra_args)
        .arg(ms_path)
        .output()
        .map_err(|error| format!("spawn rust msexplore: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    fs::read_to_string(output).map_err(|error| format!("read rust manifest: {error}"))
}

fn run_rust_msexplore_on_with_page_spec(
    ms_path: &Path,
    page_spec_path: &Path,
    extra_args: &[&str],
) -> Result<String, String> {
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("rust-msexplore-page.txt");
    let result = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--page-spec"])
        .arg(page_spec_path)
        .args(["--plot-output"])
        .arg(&output)
        .args(["--plot-format", "txt"])
        .args(extra_args)
        .arg(ms_path)
        .output()
        .map_err(|error| format!("spawn rust msexplore page spec: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    fs::read_to_string(output).map_err(|error| format!("read rust page manifest: {error}"))
}

fn run_casa_plotms(kwargs: &[(&str, &str)]) -> Result<String, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    Ok(
        String::from_utf8(run_casa_plotms_export_on(&ms_path, kwargs, "txt")?.body)
            .map_err(|error| format!("decode CASA txt export: {error}"))?,
    )
}

fn run_casa_plotms_expr(
    kwargs: &[(&str, &str)],
    expr_kwargs: &[(&str, &str)],
) -> Result<String, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    Ok(String::from_utf8(
        run_casa_plotms_export_on_with_expr(&ms_path, kwargs, expr_kwargs, "txt")?.body,
    )
    .map_err(|error| format!("decode CASA txt export: {error}"))?)
}

fn run_casa_plotms_png(kwargs: &[(&str, &str)]) -> Result<CasaPlotmsExport, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    run_casa_plotms_export_on(&ms_path, kwargs, "png")
}

#[derive(Debug)]
struct CasaPlotmsCall<'a> {
    kwargs: Vec<(&'a str, &'a str)>,
}

fn run_casa_plotms_sequence_png(calls: &[CasaPlotmsCall<'_>]) -> Result<CasaPlotmsExport, String> {
    let _guard = casa_plotms_lock().lock().expect("lock CASA plotms");
    let casa = discover_casa_python().ok_or_else(|| skip_reason(false))?;
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join("casa-plotms.png");

    let mut script = String::from(
        r#"
import os
try:
    from casatasks import plotms
except Exception:
    from casaplotms import plotms
"#,
    );
    for (index, call) in calls.iter().enumerate() {
        script.push_str("kwargs = {\n");
        script.push_str("    \"vis\": os.environ[\"CASA_VIS\"],\n");
        script.push_str("    \"showgui\": False,\n");
        script.push_str("    \"verbose\": True,\n");
        script.push_str("}\n");
        if index + 1 == calls.len() {
            script.push_str("kwargs[\"plotfile\"] = os.environ[\"CASA_OUT\"]\n");
            script.push_str("kwargs[\"expformat\"] = \"png\"\n");
            script.push_str("kwargs[\"overwrite\"] = True\n");
            script.push_str("kwargs[\"width\"] = 1600\n");
            script.push_str("kwargs[\"height\"] = 900\n");
        }
        for (key, value) in &call.kwargs {
            script.push_str(&format!("kwargs[{key:?}] = {value}\n"));
        }
        script.push_str("plotms(**kwargs)\n");
    }

    let result = Command::new(&casa.program)
        .current_dir(temp.path())
        .arg("-c")
        .arg(&script)
        .env("CASA_VIS", &ms_path)
        .env("CASA_OUT", &output)
        .env(
            "DISPLAY",
            std::env::var("DISPLAY").unwrap_or_else(|_| ":0".to_string()),
        )
        .output()
        .map_err(|error| format!("spawn casa plotms sequence: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    let body = fs::read(&output).map_err(|error| format!("read casa export: {error}"))?;
    let log = read_casa_log(temp.path())?;
    Ok(CasaPlotmsExport { body, log })
}

fn run_casa_plotms_png_expr(
    kwargs: &[(&str, &str)],
    expr_kwargs: &[(&str, &str)],
) -> Result<CasaPlotmsExport, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    run_casa_plotms_export_on_with_expr(&ms_path, kwargs, expr_kwargs, "png")
}

fn run_casa_plotms_export_on(
    ms_path: &Path,
    kwargs: &[(&str, &str)],
    expformat: &str,
) -> Result<CasaPlotmsExport, String> {
    run_casa_plotms_export_on_with_expr(ms_path, kwargs, &[], expformat)
}

fn run_casa_plotms_export_on_with_expr(
    ms_path: &Path,
    kwargs: &[(&str, &str)],
    expr_kwargs: &[(&str, &str)],
    expformat: &str,
) -> Result<CasaPlotmsExport, String> {
    let _guard = casa_plotms_lock().lock().expect("lock CASA plotms");
    let casa = discover_casa_python().ok_or_else(|| skip_reason(false))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let output = temp.path().join(match expformat {
        "png" => "casa-plotms.png",
        _ => "casa-plotms.txt",
    });
    let mut script = String::from(
        r#"
import os
try:
    from casatasks import plotms
except Exception:
    from casaplotms import plotms

kwargs = {
    "vis": os.environ["CASA_VIS"],
    "plotfile": os.environ["CASA_OUT"],
    "expformat": os.environ["CASA_EXPFORMAT"],
    "overwrite": True,
    "showgui": False,
    "verbose": True,
}
"#,
    );
    if expformat == "png" {
        script.push_str("kwargs[\"width\"] = 1600\n");
        script.push_str("kwargs[\"height\"] = 900\n");
    }
    for (key, value) in kwargs {
        script.push_str(&format!("kwargs[{key:?}] = {value:?}\n"));
    }
    for (key, value) in expr_kwargs {
        script.push_str(&format!("kwargs[{key:?}] = {value}\n"));
    }
    script.push_str("plotms(**kwargs)\n");

    let result = Command::new(&casa.program)
        .current_dir(temp.path())
        .arg("-c")
        .arg(&script)
        .env("CASA_VIS", ms_path)
        .env("CASA_OUT", &output)
        .env("CASA_EXPFORMAT", expformat)
        // `casaplotms` checks only for the presence of DISPLAY, even when
        // exporting with `showgui=False` on macOS.
        .env(
            "DISPLAY",
            std::env::var("DISPLAY").unwrap_or_else(|_| ":0".to_string()),
        )
        .output()
        .map_err(|error| format!("spawn casa plotms: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    let body = fs::read(&output).map_err(|error| format!("read casa export: {error}"))?;
    let log = read_casa_log(temp.path())?;
    Ok(CasaPlotmsExport { body, log })
}

fn count_rust_points(text: &str) -> usize {
    text.lines()
        .filter(|line| {
            !line.starts_with('#') && !line.starts_with("series_key") && !line.trim().is_empty()
        })
        .count()
}

fn count_casa_points(text: &str) -> usize {
    text.lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty()
                && !trimmed.starts_with('#')
                && trimmed
                    .split_whitespace()
                    .filter(|token| token.parse::<f64>().is_ok())
                    .count()
                    >= 2
        })
        .count()
}

fn rust_xy_points(text: &str) -> Vec<(f64, f64)> {
    let mut points = text
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("series_key")
        })
        .map(|line| {
            let mut parts = line.split('\t');
            let _key = parts.next().expect("series key");
            let _label = parts.next().expect("series label");
            let x = parts
                .next()
                .expect("x value")
                .parse::<f64>()
                .expect("parse rust x");
            let y = parts
                .next()
                .expect("y value")
                .parse::<f64>()
                .expect("parse rust y");
            (x, y)
        })
        .collect::<Vec<_>>();
    points.sort_by(point_order);
    points
}

fn rust_dual_axis_points(text: &str) -> std::collections::BTreeMap<String, Vec<(f64, f64)>> {
    let mut points = std::collections::BTreeMap::<String, Vec<(f64, f64)>>::new();
    for line in text.lines().filter(|line| {
        let trimmed = line.trim();
        !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("series_key")
    }) {
        let mut parts = line.split('\t');
        let _series_key = parts.next().expect("series key");
        let _series_label = parts.next().expect("series label");
        let y_axis = parts.next().expect("y axis").to_string();
        let x = parts
            .next()
            .expect("x value")
            .parse::<f64>()
            .expect("parse rust x");
        let y = parts
            .next()
            .expect("y value")
            .parse::<f64>()
            .expect("parse rust y");
        points.entry(y_axis).or_default().push((x, y));
    }
    for values in points.values_mut() {
        values.sort_by(point_order);
    }
    points
}

fn rust_iterated_xy_points(text: &str) -> std::collections::BTreeMap<String, Vec<(f64, f64)>> {
    let mut panels = std::collections::BTreeMap::<String, Vec<(f64, f64)>>::new();
    for line in text.lines().filter(|line| {
        let trimmed = line.trim();
        !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("panel_key")
    }) {
        let mut parts = line.split('\t');
        let panel_key = parts.next().expect("panel key").to_string();
        let _panel_label = parts.next().expect("panel label");
        let _series_key = parts.next().expect("series key");
        let _series_label = parts.next().expect("series label");
        let x = parts
            .next()
            .expect("x value")
            .parse::<f64>()
            .expect("parse rust x");
        let y = parts
            .next()
            .expect("y value")
            .parse::<f64>()
            .expect("parse rust y");
        panels.entry(panel_key).or_default().push((x, y));
    }
    for points in panels.values_mut() {
        points.sort_by(point_order);
    }
    panels
}

fn rust_page_points_by_plot(text: &str) -> std::collections::BTreeMap<usize, Vec<(f64, f64)>> {
    let mut plots = std::collections::BTreeMap::<usize, Vec<(f64, f64)>>::new();
    for line in text.lines().filter(|line| {
        let trimmed = line.trim();
        !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with("plotindex")
    }) {
        let mut parts = line.split('\t');
        let plotindex = parts
            .next()
            .expect("plot index")
            .parse::<usize>()
            .expect("parse plot index");
        let _rowindex = parts.next().expect("row index");
        let _colindex = parts.next().expect("col index");
        let _plot_title = parts.next().expect("plot title");
        let _x_axis = parts.next().expect("x axis");
        let _y_axis = parts.next().expect("y axis");
        let _series_key = parts.next().expect("series key");
        let _series_label = parts.next().expect("series label");
        let x = parts
            .next()
            .expect("x value")
            .parse::<f64>()
            .expect("parse rust x");
        let y = parts
            .next()
            .expect("y value")
            .parse::<f64>()
            .expect("parse rust y");
        plots.entry(plotindex).or_default().push((x, y));
    }
    for points in plots.values_mut() {
        points.sort_by(point_order);
    }
    plots
}

fn casa_xy_points(text: &str) -> Vec<(f64, f64)> {
    let mut points = text
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let mut parts = trimmed.split_whitespace();
            let x = parts.next()?.parse::<f64>().ok()?;
            let y = parts.next()?.parse::<f64>().ok()?;
            Some((x, y))
        })
        .collect::<Vec<_>>();
    points.sort_by(point_order);
    points
}

fn assert_points_match(rust: &[(f64, f64)], casa: &[(f64, f64)]) {
    const CASA_TXT_X_TOLERANCE: f64 = 1.1e-3;
    const CASA_TXT_Y_TOLERANCE: f64 = 1.1e-3;

    assert!(
        !rust.is_empty(),
        "expected rust manifest to contain plotted points"
    );
    assert!(
        !casa.is_empty(),
        "expected CASA txt export to contain plotted points"
    );
    let mut rust = rust
        .iter()
        .copied()
        .map(normalize_casa_txt_point)
        .collect::<Vec<_>>();
    let mut casa = casa
        .iter()
        .copied()
        .map(normalize_casa_txt_point)
        .collect::<Vec<_>>();
    rust.sort_by(point_order);
    casa.sort_by(point_order);

    assert_eq!(rust.len(), casa.len(), "point count mismatch");
    if points_match_pairwise(&rust, &casa, CASA_TXT_X_TOLERANCE, CASA_TXT_Y_TOLERANCE) {
        return;
    }

    let (rust_x, rust_y): (Vec<_>, Vec<_>) = rust.iter().copied().unzip();
    let (casa_x, casa_y): (Vec<_>, Vec<_>) = casa.iter().copied().unzip();
    assert_sorted_axis_matches("x", &rust_x, &casa_x, CASA_TXT_X_TOLERANCE);
    assert_sorted_axis_matches("y", &rust_y, &casa_y, CASA_TXT_Y_TOLERANCE);
}

fn points_match_pairwise(
    rust: &[(f64, f64)],
    casa: &[(f64, f64)],
    x_tolerance: f64,
    y_tolerance: f64,
) -> bool {
    rust.iter()
        .zip(casa.iter())
        .all(|(rust_point, casa_point)| {
            (rust_point.0 - casa_point.0).abs() <= x_tolerance
                && (rust_point.1 - casa_point.1).abs() <= y_tolerance
        })
}

fn assert_sorted_axis_matches(label: &str, rust: &[f64], casa: &[f64], tolerance: f64) {
    assert_eq!(rust.len(), casa.len(), "{label}-axis point count mismatch");
    let mut rust = rust.to_vec();
    let mut casa = casa.to_vec();
    rust.sort_by(f64::total_cmp);
    casa.sort_by(f64::total_cmp);

    for (index, (rust_value, casa_value)) in rust.iter().zip(casa.iter()).enumerate() {
        assert!(
            (rust_value - casa_value).abs() <= tolerance,
            "{label} mismatch at sorted index {index}: rust={} casa={}",
            rust_value,
            casa_value
        );
    }
}

fn point_order(left: &(f64, f64), right: &(f64, f64)) -> Ordering {
    left.0
        .total_cmp(&right.0)
        .then_with(|| left.1.total_cmp(&right.1))
}

fn normalize_casa_txt_point(point: (f64, f64)) -> (f64, f64) {
    (
        normalize_casa_txt_value(point.0),
        normalize_casa_txt_value(point.1),
    )
}

fn normalize_casa_txt_value(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

#[derive(Debug)]
struct CasaPlotmsExport {
    body: Vec<u8>,
    log: String,
}

fn read_casa_log(dir: &Path) -> Result<String, String> {
    let mut logs = fs::read_dir(dir)
        .map_err(|error| format!("read casa temp dir: {error}"))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("casa-") && name.ends_with(".log"))
        })
        .collect::<Vec<_>>();
    logs.sort();
    let path = logs
        .pop()
        .ok_or_else(|| "missing CASA log file".to_string())?;
    fs::read_to_string(path).map_err(|error| format!("read casa log: {error}"))
}

fn parse_casa_point_count(log: &str) -> Option<usize> {
    let marker = "Data selection will yield a total of ";
    log.lines().find_map(|line| {
        let start = line.find(marker)? + marker.len();
        let rest = &line[start..];
        let digits = rest
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .collect::<String>();
        digits.parse::<usize>().ok()
    })
}

fn parse_casa_axis_range(log: &str, axis_label: &str) -> Option<(f64, f64)> {
    let marker = format!("{axis_label}: ");
    log.lines().find_map(|line| {
        let start = line.find(&marker)? + marker.len();
        let rest = &line[start..];
        let (lower, rest) = rest.split_once(" to ")?;
        let upper = rest.split_whitespace().next()?;
        Some((lower.parse::<f64>().ok()?, upper.parse::<f64>().ok()?))
    })
}

fn numeric_range(values: impl Iterator<Item = f64>) -> Option<(f64, f64)> {
    let mut values = values;
    let first = values.next()?;
    let mut min = first;
    let mut max = first;
    for value in values {
        min = min.min(value);
        max = max.max(value);
    }
    Some((min, max))
}

fn assert_same_image_dimensions(left: &[u8], right: &[u8]) {
    let left = ImageReader::new(std::io::Cursor::new(left))
        .with_guessed_format()
        .expect("guess rust png format")
        .decode()
        .expect("decode rust png");
    let right = ImageReader::new(std::io::Cursor::new(right))
        .with_guessed_format()
        .expect("guess CASA png format")
        .decode()
        .expect("decode CASA png");
    assert_eq!(
        left.dimensions(),
        right.dimensions(),
        "image dimension mismatch"
    );
}

fn assert_same_image_width(left: &[u8], right: &[u8]) {
    let left = ImageReader::new(std::io::Cursor::new(left))
        .with_guessed_format()
        .expect("guess rust png format")
        .decode()
        .expect("decode rust png");
    let right = ImageReader::new(std::io::Cursor::new(right))
        .with_guessed_format()
        .expect("guess CASA png format")
        .decode()
        .expect("decode CASA png");
    assert_eq!(left.width(), right.width(), "image width mismatch");
}

fn assert_image_has_signal(image_bytes: &[u8]) {
    let image = ImageReader::new(std::io::Cursor::new(image_bytes))
        .with_guessed_format()
        .expect("guess png format")
        .decode()
        .expect("decode png")
        .to_rgba8();
    let has_signal = image.pixels().any(|pixel| pixel.0 != [255, 255, 255, 255]);
    assert!(has_signal, "expected image to contain plotted signal");
}

fn assert_top_band_has_signal(image_bytes: &[u8]) {
    let image = ImageReader::new(std::io::Cursor::new(image_bytes))
        .with_guessed_format()
        .expect("guess png format")
        .decode()
        .expect("decode png")
        .to_rgba8();
    let width = image.width();
    let height = image.height();
    let band_height = (height / 8).max(1);
    let has_signal = (0..band_height).any(|y| {
        (0..width).any(|x| {
            let pixel = image.get_pixel(x, y);
            pixel.0 != [255, 255, 255, 255]
        })
    });
    assert!(has_signal, "expected top header band to contain signal");
}

fn assert_right_band_has_signal(image_bytes: &[u8]) {
    let image = ImageReader::new(std::io::Cursor::new(image_bytes))
        .with_guessed_format()
        .expect("guess png format")
        .decode()
        .expect("decode png")
        .to_rgba8();
    let width = image.width();
    let height = image.height();
    let band_start = width.saturating_sub((width / 6).max(1));
    let has_signal = (0..height).any(|y| {
        (band_start..width).any(|x| {
            let pixel = image.get_pixel(x, y);
            pixel.0 != [255, 255, 255, 255]
        })
    });
    assert!(has_signal, "expected right legend band to contain signal");
}

fn assert_vertical_halves_have_signal(image_bytes: &[u8]) {
    let image = ImageReader::new(std::io::Cursor::new(image_bytes))
        .with_guessed_format()
        .expect("guess png format")
        .decode()
        .expect("decode png")
        .to_rgba8();
    let width = image.width();
    let height = image.height();
    let midpoint = height / 2;
    let top_has_signal = (0..midpoint).any(|y| {
        (0..width).any(|x| {
            let pixel = image.get_pixel(x, y);
            pixel.0 != [255, 255, 255, 255]
        })
    });
    let bottom_has_signal = (midpoint..height).any(|y| {
        (0..width).any(|x| {
            let pixel = image.get_pixel(x, y);
            pixel.0 != [255, 255, 255, 255]
        })
    });
    assert!(
        top_has_signal,
        "expected top half of stacked image to contain signal"
    );
    assert!(
        bottom_has_signal,
        "expected bottom half of stacked image to contain signal"
    );
}

fn assert_horizontal_halves_have_signal(image_bytes: &[u8]) {
    let image = ImageReader::new(std::io::Cursor::new(image_bytes))
        .with_guessed_format()
        .expect("guess png format")
        .decode()
        .expect("decode png")
        .to_rgba8();
    let width = image.width();
    let height = image.height();
    let midpoint = width / 2;
    let left_has_signal = (0..height).any(|y| {
        (0..midpoint).any(|x| {
            let pixel = image.get_pixel(x, y);
            pixel.0 != [255, 255, 255, 255]
        })
    });
    let right_has_signal = (0..height).any(|y| {
        (midpoint..width).any(|x| {
            let pixel = image.get_pixel(x, y);
            pixel.0 != [255, 255, 255, 255]
        })
    });
    assert!(
        left_has_signal,
        "expected left half of side-by-side image to contain signal"
    );
    assert!(
        right_has_signal,
        "expected right half of side-by-side image to contain signal"
    );
}
