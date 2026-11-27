// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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

    let selection = MsSelection {
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

    let selection = MsSelection {
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

    let selection = MsSelection {
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

    let selection = MsSelection {
        field: Some("0".to_string()),
        spw: Some("0".to_string()),
        scan: Some("1".to_string()),
        ..Default::default()
    };
    let region = first_point_region(&source, &selection, MsPlotPreset::AmplitudeVsTime)
        .expect("first point region");
    let explore = MsExploreSpec {
        ms_path: source.clone(),
        summary_format: casa_ms::MeasurementSetSummaryOutputFormat::Text,
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
