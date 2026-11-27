// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
