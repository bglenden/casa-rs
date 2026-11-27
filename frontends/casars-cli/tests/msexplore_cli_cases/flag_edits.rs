// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn preview_flag_edit_selects_one_unique_sample() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: None,
        panel_key: None,
        extcorr: false,
        extchannel: false,
    });

    let preview =
        preview_msexplore_flag_edit(&ms, &MsSelection::default(), &spec).expect("preview");
    assert_eq!(preview.matched_points, 1);
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(preview.affected_samples, 1);
    assert_eq!(preview.sample_edits.len(), 1);
    assert_eq!(preview.sample_edits[0].row, 0);
    assert_eq!(preview.sample_edits[0].corr, 0);
    assert_eq!(preview.sample_edits[0].chan, 0);
    assert!(!preview.sample_edits[0].old_flag);
    assert!(preview.sample_edits[0].new_flag);
    assert_eq!(preview.row_edits[0].row, 0);
    assert!(!preview.row_edits[0].old_flag_row);
    assert!(!preview.row_edits[0].new_flag_row);
}

#[test]
fn preview_flag_edit_expands_across_correlation_and_channel() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: None,
        panel_key: None,
        extcorr: true,
        extchannel: true,
    });

    let preview =
        preview_msexplore_flag_edit(&ms, &MsSelection::default(), &spec).expect("preview");
    assert_eq!(preview.matched_points, 1);
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(
        preview.affected_samples,
        common::NUM_CORR * common::NUM_CHAN
    );
    assert!(preview.row_edits[0].new_flag_row);
}

#[test]
fn apply_flag_edit_updates_flag_cells_and_flag_row() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: None,
        panel_key: None,
        extcorr: true,
        extchannel: true,
    });

    let preview =
        apply_msexplore_flag_edit(&mut ms, &MsSelection::default(), &spec).expect("apply");
    assert_eq!(
        preview.affected_samples,
        common::NUM_CORR * common::NUM_CHAN
    );
    assert!(ms.flag_row_column().get(0).expect("flag row"));
    let flags = row_flag_matrix(&ms, 0);
    assert!(flags.iter().all(|value| *value));
}

#[test]
fn cli_flag_preview_is_non_mutating_and_cli_apply_persists_changes() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let preview_path = temp.path().join("flag-preview.json");

    let preview_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--preset",
            "amplitude_vs_time",
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run preview");
    assert!(preview_output.status.success(), "{preview_output:?}");

    let preview_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&preview_path).expect("read preview json"))
            .expect("parse preview json");
    assert_eq!(preview_json["matched_points"], 1);
    assert_eq!(preview_json["affected_samples"], 1);
    let ms_after_preview = MeasurementSet::open(&ms_path).expect("reopen after preview");
    assert!(!row_flag_matrix(&ms_after_preview, 0)[(0, 0)]);

    let apply_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--overwrite",
            "--preset",
            "amplitude_vs_time",
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-apply",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run apply");
    assert!(apply_output.status.success(), "{apply_output:?}");

    let ms_after_apply = MeasurementSet::open(&ms_path).expect("reopen after apply");
    assert!(row_flag_matrix(&ms_after_apply, 0)[(0, 0)]);
}

#[test]
fn cli_flag_selected_applies_to_selected_rows_without_plot_region() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let preview_path = temp.path().join("selected-flag-preview.json");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--overwrite",
            "--scan",
            "2",
            "--flag-action",
            "flag",
            "--flag-selected",
            "--flag-apply",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run selected flag apply");
    assert!(output.status.success(), "{output:?}");

    let preview_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&preview_path).expect("read preview json"))
            .expect("parse preview json");
    assert_eq!(preview_json["affected_rows"], 1);
    assert_eq!(
        preview_json["affected_samples"],
        common::NUM_CORR * common::NUM_CHAN
    );

    let ms_after_apply = MeasurementSet::open(&ms_path).expect("reopen after apply");
    assert!(
        row_flag_matrix(&ms_after_apply, 0)
            .iter()
            .all(|value| !*value)
    );
    assert!(
        row_flag_matrix(&ms_after_apply, 1)
            .iter()
            .all(|value| *value)
    );
    assert!(
        row_flag_matrix(&ms_after_apply, 3)
            .iter()
            .all(|value| !*value)
    );
}

#[test]
fn preview_flag_edit_on_iterated_scan_grid_requires_panel_key() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: None,
        panel_key: None,
        extcorr: false,
        extchannel: false,
    });

    let error =
        preview_msexplore_flag_edit(&ms, &MsSelection::default(), &spec).expect_err("error");
    assert!(error.contains("requires panel_key"));
    assert!(error.contains("scan-1"));
    assert!(error.contains("scan-4"));
}

#[test]
fn preview_flag_edit_on_iterated_scan_panel_reports_panel_metadata() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: None,
        panel_key: Some("scan-1".to_string()),
        extcorr: false,
        extchannel: false,
    });

    let preview =
        preview_msexplore_flag_edit(&ms, &MsSelection::default(), &spec).expect("preview");
    assert_eq!(preview.panel_key.as_deref(), Some("scan-1"));
    assert_eq!(preview.panel_label.as_deref(), Some("Scan 1"));
    assert_eq!(preview.matched_points, 1);
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(preview.affected_samples, 1);
    assert_eq!(preview.sample_edits[0].row, 0);
}

#[test]
fn apply_flag_edit_on_iterated_scan_panel_updates_only_target_panel_rows() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: None,
        panel_key: Some("scan-1".to_string()),
        extcorr: false,
        extchannel: false,
    });

    let preview =
        apply_msexplore_flag_edit(&mut ms, &MsSelection::default(), &spec).expect("apply");
    assert_eq!(preview.panel_key.as_deref(), Some("scan-1"));
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(preview.sample_edits[0].row, 0);
    assert!(row_flag_matrix(&ms, 0)[(0, 0)]);
    for row in 1..4 {
        assert!(
            !row_flag_matrix(&ms, row)[(0, 0)],
            "unexpected flag change on row {row}"
        );
    }
}

#[test]
fn cli_flag_preview_on_iterated_scan_panel_includes_panel_key_and_apply_is_selective() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let preview_path = temp.path().join("iterated-flag-preview.json");

    let preview_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--preset",
            "amplitude_vs_time",
            "--iteraxis",
            "scan",
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-panel",
            "scan-1",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run iterated preview");
    assert!(preview_output.status.success(), "{preview_output:?}");

    let preview_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&preview_path).expect("read preview json"))
            .expect("parse preview json");
    assert_eq!(preview_json["panel_key"], "scan-1");
    assert_eq!(preview_json["panel_label"], "Scan 1");
    assert_eq!(preview_json["matched_points"], 1);
    assert_eq!(preview_json["affected_samples"], 1);

    let apply_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--overwrite",
            "--preset",
            "amplitude_vs_time",
            "--iteraxis",
            "scan",
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-panel",
            "scan-1",
            "--flag-apply",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run iterated apply");
    assert!(apply_output.status.success(), "{apply_output:?}");

    let ms_after_apply = MeasurementSet::open(&ms_path).expect("reopen after apply");
    assert!(row_flag_matrix(&ms_after_apply, 0)[(0, 0)]);
    for row in 1..4 {
        assert!(
            !row_flag_matrix(&ms_after_apply, row)[(0, 0)],
            "unexpected persisted flag change on row {row}"
        );
    }
}

#[test]
fn preview_flag_edit_on_stacked_page_requires_plot_index() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let explore = MsExploreSpec {
        ms_path: ms_path.clone(),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
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
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: None,
        panel_key: None,
        extcorr: false,
        extchannel: false,
    };

    let error =
        preview_msexplore_flag_edit_for_request(&ms, &explore, &flag_edit).expect_err("error");
    assert!(error.contains("requires plot_index"));
    assert!(error.contains("0"));
    assert!(error.contains("1"));
}

#[test]
fn preview_flag_edit_on_stacked_page_targets_selected_plot_index() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let explore = MsExploreSpec {
        ms_path: ms_path.clone(),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
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
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: Some(0),
        panel_key: None,
        extcorr: false,
        extchannel: false,
    };

    let preview =
        preview_msexplore_flag_edit_for_request(&ms, &explore, &flag_edit).expect("preview");
    assert_eq!(preview.plot_index, Some(0));
    assert_eq!(preview.plot_title, "Amplitude vs Time");
    assert_eq!(preview.matched_points, 1);
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(preview.affected_samples, 1);
}

#[test]
fn apply_flag_edit_on_stacked_page_updates_selected_plot_samples() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let explore = MsExploreSpec {
        ms_path: ms_path.clone(),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
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
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index: Some(0),
        panel_key: None,
        extcorr: false,
        extchannel: false,
    };

    let preview =
        apply_msexplore_flag_edit_for_request(&mut ms, &explore, &flag_edit).expect("apply");
    assert_eq!(preview.plot_index, Some(0));
    assert!(row_flag_matrix(&ms, 0)[(0, 0)]);
}

#[test]
fn cli_flag_preview_on_page_spec_includes_plot_index_and_apply_persists_changes() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let page_spec_path = temp.path().join("flag-page-spec.json");
    std::fs::write(
        &page_spec_path,
        serde_json::json!({
            "gridrows": 1,
            "gridcols": 2,
            "plots": [
                {
                    "plotindex": 0,
                    "rowindex": 0,
                    "colindex": 0,
                    "preset": "amplitude_vs_time"
                },
                {
                    "plotindex": 1,
                    "rowindex": 0,
                    "colindex": 1,
                    "preset": "phase_vs_time"
                }
            ]
        })
        .to_string(),
    )
    .expect("write page spec");
    let preview_path = temp.path().join("page-flag-preview.json");

    let preview_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--format", "json", "--page-spec"])
        .arg(&page_spec_path)
        .args([
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-plotindex",
            "0",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run page preview");
    assert!(preview_output.status.success(), "{preview_output:?}");

    let preview_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&preview_path).expect("read preview json"))
            .expect("parse preview json");
    assert_eq!(preview_json["plot_index"], 0);
    assert_eq!(preview_json["matched_points"], 1);
    assert_eq!(preview_json["affected_samples"], 1);

    let apply_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--format", "json", "--overwrite", "--page-spec"])
        .arg(&page_spec_path)
        .args([
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-plotindex",
            "0",
            "--flag-apply",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run page apply");
    assert!(apply_output.status.success(), "{apply_output:?}");

    let ms_after_apply = MeasurementSet::open(&ms_path).expect("reopen after apply");
    assert!(row_flag_matrix(&ms_after_apply, 0)[(0, 0)]);
}
