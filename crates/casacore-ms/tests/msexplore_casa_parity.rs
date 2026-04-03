// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]

use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use casacore_ms::{
    DEFAULT_MAX_PLOT_POINTS, MeasurementSet, MsAxis, MsExploreSpec, MsFlagAction, MsFlagEditSpec,
    MsFlagRegion, MsIterationAxis, MsPageExportRange, MsPlotPayload, MsPlotPreset, MsPlotSpec,
    MsSelectionSpec, apply_msexplore_flag_edit, apply_msexplore_flag_edit_for_request,
    build_msexplore_plot_payload, preview_msexplore_flag_edit,
    preview_msexplore_flag_edit_for_request,
};
use casacore_test_support::casatestdata_path;
use casacore_types::ArrayValue;
use image::{GenericImageView, ImageReader};
use ndarray::Ix2;
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
fn amplitude_vs_time_avgtime_tracks_casa_plotms_xy_values() {
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
        "--correlation",
        "RR",
        "--color-by",
        "none",
        "--avgtime",
        "60",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms(&[
        ("xaxis", "time"),
        ("yaxis", "amp"),
        ("field", "0"),
        ("spw", "0"),
        ("scan", "1"),
        ("correlation", "RR"),
        ("avgtime", "60"),
    ])
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn amplitude_vs_time_avgscan_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_time",
        "--field",
        "1",
        "--spw",
        "0",
        "--correlation",
        "RR",
        "--color-by",
        "none",
        "--avgtime",
        "7200",
        "--avgscan",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms_expr(
        &[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("field", "1"),
            ("spw", "0"),
            ("correlation", "RR"),
            ("avgtime", "7200"),
        ],
        &[("avgscan", "True")],
    )
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn amplitude_vs_time_avgbaseline_tracks_casa_plotms_xy_values() {
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
        "--correlation",
        "RR",
        "--color-by",
        "none",
        "--avgbaseline",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms_expr(
        &[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("field", "0"),
            ("spw", "0"),
            ("scan", "1"),
            ("correlation", "RR"),
        ],
        &[("avgbaseline", "True")],
    )
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn amplitude_vs_time_avgantenna_tracks_casa_plotms_xy_values() {
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
        "--correlation",
        "RR",
        "--color-by",
        "none",
        "--avgantenna",
    ])
    .expect("run rust msexplore");
    let casa = run_casa_plotms_expr(
        &[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("field", "0"),
            ("spw", "0"),
            ("scan", "1"),
            ("correlation", "RR"),
        ],
        &[("avgantenna", "True")],
    )
    .expect("run casa plotms");

    assert_points_match(&rust_xy_points(&rust), &casa_xy_points(&casa));
}

#[test]
fn amplitude_vs_time_avgfield_tracks_casa_plotms_xy_values() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let rust_avgfield = run_rust_msexplore(&[
        "--preset",
        "amplitude_vs_time",
        "--spw",
        "0",
        "--correlation",
        "RR",
        "--color-by",
        "none",
        "--avgtime",
        "7200",
        "--avgfield",
    ])
    .expect("run rust avgfield");
    let casa_avgfield = run_casa_plotms_expr(
        &[
            ("xaxis", "time"),
            ("yaxis", "amp"),
            ("spw", "0"),
            ("correlation", "RR"),
            ("avgtime", "7200"),
        ],
        &[("avgfield", "True")],
    )
    .expect("run casa avgfield");
    assert_points_match(
        &rust_xy_points(&rust_avgfield),
        &casa_xy_points(&casa_avgfield),
    );
}

#[test]
fn amplitude_vs_time_avgspw_tracks_casa_plotms_xy_values() {
    if !plotms_multi_spw_dataset_available() {
        eprintln!(
            "CASA parity skipped: missing ref_vlass_wtsp_creation.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata"
        );
        return;
    }

    let ms_path = ref_vlass_ms_path().expect("shared multi-spw MS");
    let rust_avgspw = run_rust_msexplore_on(
        &ms_path,
        &[
            "--preset",
            "amplitude_vs_time",
            "--field",
            "0",
            "--scan",
            "189",
            "--correlation",
            "RR",
            "--color-by",
            "none",
            "--avgspw",
        ],
    )
    .expect("run rust avgspw");
    let casa_avgspw = String::from_utf8(
        run_casa_plotms_export_on_with_expr(
            &ms_path,
            &[
                ("xaxis", "time"),
                ("yaxis", "amp"),
                ("field", "0"),
                ("scan", "189"),
                ("correlation", "RR"),
            ],
            &[("avgspw", "True")],
            "txt",
        )
        .expect("run casa avgspw")
        .body,
    )
    .expect("decode casa avgspw");
    assert_points_match(&rust_xy_points(&rust_avgspw), &casa_xy_points(&casa_avgspw));
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

#[test]
fn flag_edit_single_sample_matches_casa_table_writeback_and_post_edit_plot() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let source = ngc5921_ms_path().expect("shared ngc5921.ms");
    let temp = tempdir().expect("tempdir");
    let rust_copy = temp.path().join("rust-edit.ms");
    let casa_copy = temp.path().join("casa-edit.ms");
    copy_measurement_set(&source, &rust_copy).expect("copy rust ms");
    copy_measurement_set(&source, &casa_copy).expect("copy casa ms");

    let selection = MsSelectionSpec {
        field: Some("0".to_string()),
        spw: Some("0".to_string()),
        scan: Some("1".to_string()),
        ..Default::default()
    };
    let region = first_point_region(&source, &selection, MsPlotPreset::AmplitudeVsTime)
        .expect("first point region");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region,
        plot_index: None,
        panel_key: None,
        extcorr: false,
        extchannel: false,
    });

    let preview = preview_msexplore_flag_edit(
        &MeasurementSet::open(&source).expect("open shared ms"),
        &selection,
        &spec,
    )
    .expect("preview");
    assert_eq!(preview.affected_samples, 1);

    let mut rust_ms = MeasurementSet::open(&rust_copy).expect("open rust copy");
    apply_msexplore_flag_edit(&mut rust_ms, &selection, &spec).expect("apply rust edit");
    rust_ms.save().expect("save rust copy");

    apply_casa_flag_preview(&casa_copy, &preview).expect("apply casa edit");

    assert_main_flag_state_equal(&rust_copy, &casa_copy);

    let rust_manifest = run_rust_msexplore_on(
        &rust_copy,
        &[
            "--preset",
            "amplitude_vs_time",
            "--field",
            "0",
            "--spw",
            "0",
            "--scan",
            "1",
        ],
    )
    .expect("run rust post-edit manifest");
    let casa_manifest = String::from_utf8(
        run_casa_plotms_export_on(
            &casa_copy,
            &[
                ("xaxis", "time"),
                ("yaxis", "amp"),
                ("field", "0"),
                ("spw", "0"),
                ("scan", "1"),
            ],
            "txt",
        )
        .expect("run casa post-edit manifest")
        .body,
    )
    .expect("decode casa txt");
    assert_points_match(
        &rust_xy_points(&rust_manifest),
        &casa_xy_points(&casa_manifest),
    );
}

#[test]
fn flag_edit_extcorr_extchannel_matches_casa_table_writeback_and_post_edit_plot() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let source = ngc5921_ms_path().expect("shared ngc5921.ms");
    let temp = tempdir().expect("tempdir");
    let rust_copy = temp.path().join("rust-edit-all.ms");
    let casa_copy = temp.path().join("casa-edit-all.ms");
    copy_measurement_set(&source, &rust_copy).expect("copy rust ms");
    copy_measurement_set(&source, &casa_copy).expect("copy casa ms");

    let selection = MsSelectionSpec {
        field: Some("0".to_string()),
        spw: Some("0".to_string()),
        scan: Some("1".to_string()),
        ..Default::default()
    };
    let region = first_point_region(&source, &selection, MsPlotPreset::AmplitudeVsTime)
        .expect("first point region");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region,
        plot_index: None,
        panel_key: None,
        extcorr: true,
        extchannel: true,
    });

    let preview = preview_msexplore_flag_edit(
        &MeasurementSet::open(&source).expect("open shared ms"),
        &selection,
        &spec,
    )
    .expect("preview");
    assert!(preview.affected_samples > 1);
    assert_eq!(preview.affected_rows, 1);
    assert!(preview.row_edits[0].new_flag_row);

    let mut rust_ms = MeasurementSet::open(&rust_copy).expect("open rust copy");
    apply_msexplore_flag_edit(&mut rust_ms, &selection, &spec).expect("apply rust edit");
    rust_ms.save().expect("save rust copy");

    apply_casa_flag_preview(&casa_copy, &preview).expect("apply casa edit");

    assert_main_flag_state_equal(&rust_copy, &casa_copy);

    let rust_manifest = run_rust_msexplore_on(
        &rust_copy,
        &[
            "--preset",
            "amplitude_vs_time",
            "--field",
            "0",
            "--spw",
            "0",
            "--scan",
            "1",
        ],
    )
    .expect("run rust post-edit manifest");
    let casa_manifest = String::from_utf8(
        run_casa_plotms_export_on(
            &casa_copy,
            &[
                ("xaxis", "time"),
                ("yaxis", "amp"),
                ("field", "0"),
                ("spw", "0"),
                ("scan", "1"),
            ],
            "txt",
        )
        .expect("run casa post-edit manifest")
        .body,
    )
    .expect("decode casa txt");
    assert_points_match(
        &rust_xy_points(&rust_manifest),
        &casa_xy_points(&casa_manifest),
    );
}

#[test]
fn flag_edit_iterated_scan_panel_matches_casa_table_writeback_and_post_edit_plot() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let source = ngc5921_ms_path().expect("shared ngc5921.ms");
    let temp = tempdir().expect("tempdir");
    let rust_copy = temp.path().join("rust-edit-iterated.ms");
    let casa_copy = temp.path().join("casa-edit-iterated.ms");
    copy_measurement_set(&source, &rust_copy).expect("copy rust ms");
    copy_measurement_set(&source, &casa_copy).expect("copy casa ms");

    let selection = MsSelectionSpec {
        field: Some("0".to_string()),
        spw: Some("0".to_string()),
        ..Default::default()
    };
    let region = first_iterated_panel_point_region(
        &source,
        &selection,
        MsPlotPreset::AmplitudeVsTime,
        MsIterationAxis::Scan,
        "scan-1",
    )
    .expect("first iterated point region");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region,
        plot_index: None,
        panel_key: Some("scan-1".to_string()),
        extcorr: false,
        extchannel: false,
    });

    let preview = preview_msexplore_flag_edit(
        &MeasurementSet::open(&source).expect("open shared ms"),
        &selection,
        &spec,
    )
    .expect("preview");
    assert_eq!(preview.panel_key.as_deref(), Some("scan-1"));
    assert_eq!(preview.affected_samples, 1);
    assert_eq!(preview.affected_rows, 1);

    let mut rust_ms = MeasurementSet::open(&rust_copy).expect("open rust copy");
    apply_msexplore_flag_edit(&mut rust_ms, &selection, &spec).expect("apply rust edit");
    rust_ms.save().expect("save rust copy");

    apply_casa_flag_preview(&casa_copy, &preview).expect("apply casa edit");

    assert_main_flag_state_equal(&rust_copy, &casa_copy);

    let rust_manifest = run_rust_msexplore_on(
        &rust_copy,
        &[
            "--preset",
            "amplitude_vs_time",
            "--field",
            "0",
            "--spw",
            "0",
            "--iteraxis",
            "scan",
        ],
    )
    .expect("run rust post-edit iterated manifest");
    let rust_panels = rust_iterated_xy_points(&rust_manifest);
    let casa_scan1 = String::from_utf8(
        run_casa_plotms_export_on(
            &casa_copy,
            &[
                ("xaxis", "time"),
                ("yaxis", "amp"),
                ("field", "0"),
                ("spw", "0"),
                ("scan", "1"),
            ],
            "txt",
        )
        .expect("run casa scan 1 post-edit manifest")
        .body,
    )
    .expect("decode casa scan 1 txt");

    assert_points_match(
        rust_panels.get("scan-1").expect("rust scan-1 panel"),
        &casa_xy_points(&casa_scan1),
    );
}

#[test]
fn flag_edit_stacked_page_plot_index_matches_casa_table_writeback_and_post_edit_plots() {
    if !plotms_shared_dataset_available() {
        eprintln!("{}", skip_reason(true));
        return;
    }

    let source = ngc5921_ms_path().expect("shared ngc5921.ms");
    let temp = tempdir().expect("tempdir");
    let rust_copy = temp.path().join("rust-edit-stacked.ms");
    let casa_copy = temp.path().join("casa-edit-stacked.ms");
    copy_measurement_set(&source, &rust_copy).expect("copy rust ms");
    copy_measurement_set(&source, &casa_copy).expect("copy casa ms");

    let selection = MsSelectionSpec {
        field: Some("0".to_string()),
        spw: Some("0".to_string()),
        scan: Some("1".to_string()),
        ..Default::default()
    };
    let region = first_point_region(&source, &selection, MsPlotPreset::AmplitudeVsTime)
        .expect("first point region");
    let explore = MsExploreSpec {
        ms_path: source.clone(),
        summary_format: casacore_ms::MeasurementSetSummaryOutputFormat::Text,
        selection: selection.clone(),
        header_items: Vec::new(),
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points: DEFAULT_MAX_PLOT_POINTS,
        plots: vec![MsPlotSpec::from_preset(
            MsPlotPreset::AmplitudePhaseVsTimeStacked,
        )],
    };
    let flag_edit = MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region,
        plot_index: Some(0),
        panel_key: None,
        extcorr: false,
        extchannel: false,
    };

    let preview = preview_msexplore_flag_edit_for_request(
        &MeasurementSet::open(&source).expect("open shared ms"),
        &explore,
        &flag_edit,
    )
    .expect("preview");
    assert_eq!(preview.plot_index, Some(0));
    assert_eq!(preview.affected_samples, 1);

    let mut rust_ms = MeasurementSet::open(&rust_copy).expect("open rust copy");
    let mut rust_explore = explore.clone();
    rust_explore.ms_path = rust_copy.clone();
    apply_msexplore_flag_edit_for_request(&mut rust_ms, &rust_explore, &flag_edit)
        .expect("apply rust edit");
    rust_ms.save().expect("save rust copy");

    apply_casa_flag_preview(&casa_copy, &preview).expect("apply casa edit");

    assert_main_flag_state_equal(&rust_copy, &casa_copy);

    let rust_manifest = run_rust_msexplore_on(
        &rust_copy,
        &[
            "--preset",
            "amplitude_phase_vs_time_stacked",
            "--field",
            "0",
            "--spw",
            "0",
            "--scan",
            "1",
        ],
    )
    .expect("run rust stacked post-edit manifest");
    let rust_plots = rust_page_points_by_plot(&rust_manifest);
    let casa_amp = String::from_utf8(
        run_casa_plotms_export_on(
            &casa_copy,
            &[
                ("xaxis", "time"),
                ("yaxis", "amp"),
                ("field", "0"),
                ("spw", "0"),
                ("scan", "1"),
            ],
            "txt",
        )
        .expect("run casa amplitude post-edit manifest")
        .body,
    )
    .expect("decode casa amplitude txt");
    let casa_phase = String::from_utf8(
        run_casa_plotms_export_on(
            &casa_copy,
            &[
                ("xaxis", "time"),
                ("yaxis", "phase"),
                ("field", "0"),
                ("spw", "0"),
                ("scan", "1"),
            ],
            "txt",
        )
        .expect("run casa phase post-edit manifest")
        .body,
    )
    .expect("decode casa phase txt");

    assert_points_match(
        rust_plots.get(&0).expect("rust amplitude plot"),
        &casa_xy_points(&casa_amp),
    );
    assert_points_match(
        rust_plots.get(&1).expect("rust phase plot"),
        &casa_xy_points(&casa_phase),
    );
}

fn plotms_available() -> bool {
    discover_casa_python().is_some_and(|python| python.plotms_available)
}

fn plotms_shared_dataset_available() -> bool {
    plotms_available() && ngc5921_ms_path().is_some()
}

fn ref_vlass_ms_path() -> Option<PathBuf> {
    casatestdata_path("measurementset/vla/ref_vlass_wtsp_creation.ms").filter(|path| path.exists())
}

fn plotms_multi_spw_dataset_available() -> bool {
    plotms_available() && ref_vlass_ms_path().is_some()
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
    String::from_utf8(run_casa_plotms_export_on(&ms_path, kwargs, "txt")?.body)
        .map_err(|error| format!("decode CASA txt export: {error}"))
}

fn run_casa_plotms_expr(
    kwargs: &[(&str, &str)],
    expr_kwargs: &[(&str, &str)],
) -> Result<String, String> {
    let ms_path = ngc5921_ms_path().ok_or_else(|| skip_reason(true))?;
    String::from_utf8(
        run_casa_plotms_export_on_with_expr(&ms_path, kwargs, expr_kwargs, "txt")?.body,
    )
    .map_err(|error| format!("decode CASA txt export: {error}"))
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

fn copy_measurement_set(source: &Path, destination: &Path) -> Result<(), String> {
    if source.is_dir() {
        fs::create_dir_all(destination).map_err(|error| {
            format!(
                "create destination directory {}: {error}",
                destination.display()
            )
        })?;
        for entry in fs::read_dir(source).map_err(|error| {
            format!(
                "read MeasurementSet directory {}: {error}",
                source.display()
            )
        })? {
            let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
            let child_source = entry.path();
            let child_destination = destination.join(entry.file_name());
            copy_measurement_set(&child_source, &child_destination)?;
        }
        return Ok(());
    }
    fs::copy(source, destination).map_err(|error| {
        format!(
            "copy MeasurementSet file {} -> {}: {error}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn first_point_region(
    ms_path: &Path,
    selection: &MsSelectionSpec,
    preset: MsPlotPreset,
) -> Result<MsFlagRegion, String> {
    let ms = MeasurementSet::open(ms_path).map_err(|error| format!("open source ms: {error}"))?;
    let spec = MsPlotSpec::from_preset(preset);
    let payload = build_msexplore_plot_payload(&ms, selection, &spec)?;
    let MsPlotPayload::Scatter(payload) = payload else {
        return Err("expected single scatter payload for flag-edit region selection".to_string());
    };
    let point = payload
        .series
        .iter()
        .flat_map(|series| series.points.iter().copied())
        .next()
        .ok_or_else(|| "scatter payload produced no points".to_string())?;
    Ok(MsFlagRegion {
        x_min: point.0 - 1e-6,
        x_max: point.0 + 1e-6,
        y_min: point.1 - 1e-6,
        y_max: point.1 + 1e-6,
    })
}

fn first_iterated_panel_point_region(
    ms_path: &Path,
    selection: &MsSelectionSpec,
    preset: MsPlotPreset,
    iteraxis: MsIterationAxis,
    panel_key: &str,
) -> Result<MsFlagRegion, String> {
    let ms = MeasurementSet::open(ms_path).map_err(|error| format!("open source ms: {error}"))?;
    let mut spec = MsPlotSpec::from_preset(preset);
    spec.iteration.iteraxis = Some(iteraxis);
    let payload = build_msexplore_plot_payload(&ms, selection, &spec)?;
    let MsPlotPayload::ScatterGrid(payload) = payload else {
        return Err("expected iterated scatter payload for flag-edit region selection".to_string());
    };
    let panel = payload
        .panels
        .iter()
        .find(|panel| panel.key == panel_key)
        .ok_or_else(|| {
            format!(
                "iterated payload did not contain panel_key {:?}; available panels: {}",
                panel_key,
                payload
                    .panels
                    .iter()
                    .map(|panel| panel.key.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
    let point = panel
        .series
        .iter()
        .flat_map(|series| series.points.iter().copied())
        .next()
        .ok_or_else(|| format!("iterated panel {:?} produced no points", panel_key))?;
    Ok(MsFlagRegion {
        x_min: point.0 - 1e-6,
        x_max: point.0 + 1e-6,
        y_min: point.1 - 1e-6,
        y_max: point.1 + 1e-6,
    })
}

fn apply_casa_flag_preview(
    ms_path: &Path,
    preview: &casacore_ms::MsFlagEditPreview,
) -> Result<(), String> {
    let casa = discover_casa_python().ok_or_else(|| skip_reason(false))?;
    let temp = tempdir().map_err(|error| format!("tempdir: {error}"))?;
    let preview_path = temp.path().join("flag-preview.json");
    fs::write(
        &preview_path,
        serde_json::to_string(preview).map_err(|error| format!("serialize preview: {error}"))?,
    )
    .map_err(|error| format!("write preview json: {error}"))?;
    let script = r#"
import json
import os
from casatools import table

with open(os.environ["CASA_FLAG_PREVIEW"], "r", encoding="utf-8") as handle:
    preview = json.load(handle)

row_edits = {entry["row"]: entry for entry in preview["row_edits"]}
sample_edits = {}
for entry in preview["sample_edits"]:
    sample_edits.setdefault(entry["row"], []).append(entry)

tb = table()
tb.open(os.environ["CASA_VIS"], nomodify=False)
try:
    for row, row_edit in row_edits.items():
        cell = tb.getcell("FLAG", row)
        for sample in sample_edits.get(row, []):
            cell[sample["corr"], sample["chan"]] = bool(sample["new_flag"])
        tb.putcell("FLAG", row, cell)
        tb.putcell("FLAG_ROW", row, bool(row_edit["new_flag_row"]))
    tb.flush()
finally:
    tb.close()
"#;
    let result = Command::new(&casa.program)
        .current_dir(temp.path())
        .arg("-c")
        .arg(script)
        .env("CASA_VIS", ms_path)
        .env("CASA_FLAG_PREVIEW", &preview_path)
        .output()
        .map_err(|error| format!("spawn casa flag preview apply: {error}"))?;
    if !result.status.success() {
        return Err(String::from_utf8_lossy(&result.stderr).to_string());
    }
    Ok(())
}

fn assert_main_flag_state_equal(left_path: &Path, right_path: &Path) {
    let left = MeasurementSet::open(left_path).expect("open left MeasurementSet");
    let right = MeasurementSet::open(right_path).expect("open right MeasurementSet");
    assert_eq!(left.row_count(), right.row_count(), "row-count mismatch");
    for row in 0..left.row_count() {
        assert_eq!(
            left.flag_row_column().get(row).expect("left flag_row"),
            right.flag_row_column().get(row).expect("right flag_row"),
            "FLAG_ROW mismatch on row {row}"
        );
        let left_flags = flag_matrix_for_row(&left, row);
        let right_flags = flag_matrix_for_row(&right, row);
        assert_eq!(left_flags, right_flags, "FLAG mismatch on row {row}");
    }
}

fn flag_matrix_for_row(ms: &MeasurementSet, row: usize) -> ndarray::Array2<bool> {
    match ms.flag_column().get(row).expect("flag cell") {
        ArrayValue::Bool(values) => values
            .view()
            .into_dimensionality::<Ix2>()
            .expect("2d flag cell")
            .to_owned(),
        other => panic!("unexpected FLAG cell type {:?}", other.primitive_type()),
    }
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
