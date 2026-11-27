// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn msexplore_dual_y_payload_tracks_secondary_axis_and_style_flags() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.y_axes.push(MsAxis::Phase);
    spec.style.showlegend = true;
    spec.style.legendposition = MsLegendPosition::LowerLeft;
    spec.style.showmajorgrid = true;
    spec.style.showminorgrid = true;

    let payload = build_msexplore_plot_payload(&ms, &MsSelection::default(), &spec)
        .expect("build dual-y payload");
    let MsPlotPayload::Scatter(scatter) = payload else {
        panic!("expected scatter payload");
    };
    assert_eq!(scatter.y_axis, MsAxis::Amplitude);
    assert_eq!(scatter.secondary_y_axis, Some(MsAxis::Phase));
    assert_eq!(scatter.secondary_y_label.as_deref(), Some("Phase (deg)"));
    assert!(scatter.showlegend);
    assert_eq!(scatter.legend_position, MsLegendPosition::LowerLeft);
    assert!(scatter.showmajorgrid);
    assert!(scatter.showminorgrid);
    let series_axes = scatter
        .series
        .iter()
        .map(|series| series.y_axis.as_str().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        series_axes,
        ["amplitude".to_string(), "phase".to_string()]
            .into_iter()
            .collect()
    );
}

#[test]
fn msexplore_single_plot_manifest_emits_header_lines_and_legend_position() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-time-header.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_time",
            "--showlegend",
            "--legendposition",
            "lowerLeft",
            "--headeritems",
            "filename,ycolumn",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(
        manifest_header_value(&manifest, "legendposition"),
        Some("lowerLeft")
    );
    assert!(manifest.contains("# header_line=Filename:"));
    assert!(manifest.contains("Y Column: Amplitude"));
}

#[test]
fn msexplore_payload_resolves_page_header_items() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let payload = build_msexplore_payload(
        &ms,
        &MsExploreSpec {
            ms_path,
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: vec![MsPageHeaderItem::Filename, MsPageHeaderItem::YColumn],
            page_title: Some("Amplitude".to_string()),
            exprange: MsPageExportRange::Current,
            max_plot_points: DEFAULT_MAX_PLOT_POINTS,
            plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime)],
        },
    )
    .expect("build payload");

    let MsPlotPayload::Scatter(scatter) = payload else {
        panic!("expected scatter payload");
    };
    assert!(!scatter.header_lines.is_empty());
    assert!(
        scatter
            .header_lines
            .iter()
            .any(|line| line.contains("Filename:"))
    );
    assert!(
        scatter
            .header_lines
            .iter()
            .any(|line| line.contains("Y Column: Amplitude"))
    );
}

#[test]
fn msexplore_stacked_time_preset_manifest_emits_two_plots_on_one_page() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-phase-stacked.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_phase_vs_time_stacked",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(manifest_header_value(&manifest, "gridrows"), Some("2"));
    assert_eq!(manifest_header_value(&manifest, "gridcols"), Some("1"));

    let rows = page_manifest_rows(&plot_path);
    assert_eq!(rows.len(), 512);
    assert_eq!(
        rows.iter()
            .filter(|(plotindex, _, _, _, _, _, _, _, _, _)| *plotindex == 0)
            .count(),
        256
    );
    assert_eq!(
        rows.iter()
            .filter(|(plotindex, _, _, _, _, _, _, _, _, _)| *plotindex == 1)
            .count(),
        256
    );
    assert!(rows.iter().any(|(_, _, _, title, _, y_axis, _, _, _, _)| {
        title == "Amplitude vs Time" && y_axis == "amplitude"
    }));
    assert!(rows.iter().any(|(_, _, _, title, _, y_axis, _, _, _, _)| {
        title == "Phase vs Time" && y_axis == "phase"
    }));
}

#[test]
fn msexplore_stacked_time_preset_builds_two_row_page_payload() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let payload = build_msexplore_plot_payload(
        &ms,
        &MsSelection::default(),
        &MsPlotSpec::from_preset(MsPlotPreset::AmplitudePhaseVsTimeStacked),
    )
    .expect("build stacked page payload");
    let MsPlotPayload::ScatterPage(page) = payload else {
        panic!("expected scatter page payload");
    };
    assert_eq!(page.gridrows, 2);
    assert_eq!(page.gridcols, 1);
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].plotindex, 0);
    assert_eq!(page.items[0].rowindex, 0);
    assert_eq!(page.items[0].plot.y_axis, MsAxis::Amplitude);
    assert_eq!(page.items[1].plotindex, 1);
    assert_eq!(page.items[1].rowindex, 1);
    assert_eq!(page.items[1].plot.y_axis, MsAxis::Phase);
}

#[test]
fn msexplore_generic_page_spec_builds_side_by_side_page_payload() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let mut amplitude = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    amplitude.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 2,
        rowindex: 0,
        colindex: 0,
        plotindex: 0,
    };
    amplitude.style.title = Some("Amplitude vs Time".to_string());

    let mut phase = MsPlotSpec::from_preset(MsPlotPreset::PhaseVsTime);
    phase.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 2,
        rowindex: 0,
        colindex: 1,
        plotindex: 1,
    };
    phase.style.title = Some("Phase vs Time".to_string());
    phase.color_by = MsColorAxis::Scan;
    phase.data_column = MsDataColumn::Data;

    let payload = build_msexplore_payload(
        &ms,
        &MsExploreSpec {
            ms_path,
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: Vec::new(),
            page_title: Some("Amplitude and Phase Side by Side".to_string()),
            exprange: MsPageExportRange::Current,
            max_plot_points: DEFAULT_MAX_PLOT_POINTS,
            plots: vec![amplitude, phase],
        },
    )
    .expect("build generic page payload");
    let MsPlotPayload::ScatterPage(page) = payload else {
        panic!("expected scatter page payload");
    };
    assert_eq!(page.title, "Amplitude and Phase Side by Side");
    assert_eq!(page.gridrows, 1);
    assert_eq!(page.gridcols, 2);
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].colindex, 0);
    assert_eq!(page.items[0].plot.y_axis, MsAxis::Amplitude);
    assert_eq!(page.items[1].colindex, 1);
    assert_eq!(page.items[1].plot.y_axis, MsAxis::Phase);
}

#[test]
fn msexplore_page_spec_rejects_overplots_with_different_axes() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("mixed-overplot.txt");
    let page_spec_path = temp.path().join("mixed-page.json");

    std::fs::write(
        &page_spec_path,
        serde_json::json!({
            "gridrows": 1,
            "gridcols": 1,
            "plots": [
                {"preset": "amplitude_vs_time", "plotindex": 0, "rowindex": 0, "colindex": 0},
                {"preset": "amplitude_vs_channel", "plotindex": 1, "rowindex": 0, "colindex": 0}
            ]
        })
        .to_string(),
    )
    .expect("write page spec");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--page-spec"])
        .arg(&page_spec_path)
        .args(["--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(
        stderr.contains("cell (0, 0) mixes incompatible axes"),
        "{stderr}"
    );
    assert!(!plot_path.exists());
}

#[test]
fn msexplore_page_spec_manifest_emits_multiple_positions_on_one_page() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-phase-page.txt");
    let page_spec_path = temp.path().join("page.json");

    std::fs::write(
        &page_spec_path,
        serde_json::json!({
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
        })
        .to_string(),
    )
    .expect("write page spec");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--page-spec"])
        .arg(&page_spec_path)
        .args(["--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(
        manifest_header_value(&manifest, "exprange"),
        Some("current")
    );
    assert_eq!(manifest_header_value(&manifest, "gridrows"), Some("1"));
    assert_eq!(manifest_header_value(&manifest, "gridcols"), Some("2"));

    let rows = page_manifest_rows(&plot_path);
    assert_eq!(rows.len(), 512);
    assert_eq!(
        rows.iter()
            .filter(
                |(plotindex, rowindex, colindex, title, _, y_axis, _, _, _, _)| {
                    *plotindex == 0
                        && *rowindex == 0
                        && *colindex == 0
                        && title == "Amplitude vs Time"
                        && y_axis == "amplitude"
                }
            )
            .count(),
        256
    );
    assert_eq!(
        rows.iter()
            .filter(
                |(plotindex, rowindex, colindex, title, _, y_axis, _, _, _, _)| {
                    *plotindex == 1
                        && *rowindex == 0
                        && *colindex == 1
                        && title == "Phase vs Time"
                        && y_axis == "phase"
                }
            )
            .count(),
        256
    );
}

#[test]
fn msexplore_generic_page_spec_allows_same_cell_overplot_manifest() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let mut left = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    left.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 1,
        rowindex: 0,
        colindex: 0,
        plotindex: 0,
    };
    left.style.title = Some("Amplitude:vector".to_string());
    left.style.showlegend = true;

    let mut right = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    right.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 1,
        rowindex: 0,
        colindex: 0,
        plotindex: 1,
    };
    right.averaging.scalar = true;
    right.style.title = Some("Amplitude:scalar".to_string());
    right.style.showlegend = true;

    let payload = build_msexplore_payload(
        &ms,
        &MsExploreSpec {
            ms_path,
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: Vec::new(),
            page_title: Some("Amplitude Overplot".to_string()),
            exprange: MsPageExportRange::Current,
            max_plot_points: DEFAULT_MAX_PLOT_POINTS,
            plots: vec![left, right],
        },
    )
    .expect("build overplot page payload");
    let MsPlotPayload::ScatterPage(page) = &payload else {
        panic!("expected scatter page payload");
    };
    assert_eq!(page.exprange, MsPageExportRange::Current);
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].rowindex, 0);
    assert_eq!(page.items[1].rowindex, 0);
    assert_eq!(page.items[0].colindex, 0);
    assert_eq!(page.items[1].colindex, 0);

    let output = temp.path().join("overplot.txt");
    export_msexplore_plot(&payload, &output).expect("export overplot manifest");
    let rows = page_manifest_rows(&output);
    assert!(
        rows.iter()
            .any(|row| row.0 == 0 && row.1 == 0 && row.2 == 0)
    );
    assert!(
        rows.iter()
            .any(|row| row.0 == 1 && row.1 == 0 && row.2 == 0)
    );
}

#[test]
fn msexplore_payload_from_spec_opens_disk_backed_ms_for_scatter_pages() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());

    let mut amplitude = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    amplitude.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 2,
        rowindex: 0,
        colindex: 0,
        plotindex: 0,
    };
    amplitude.style.showlegend = true;
    amplitude.style.legendposition = MsLegendPosition::ExteriorRight;

    let mut phase = MsPlotSpec::from_preset(MsPlotPreset::PhaseVsTime);
    phase.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 2,
        rowindex: 0,
        colindex: 1,
        plotindex: 1,
    };
    phase.style.showlegend = true;
    phase.style.legendposition = MsLegendPosition::ExteriorRight;

    let payload = build_msexplore_payload_from_spec(&MsExploreSpec {
        ms_path,
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
        header_items: vec![MsPageHeaderItem::Filename, MsPageHeaderItem::YColumn],
        page_title: Some("Amplitude and Phase".to_string()),
        exprange: MsPageExportRange::Current,
        max_plot_points: DEFAULT_MAX_PLOT_POINTS,
        plots: vec![amplitude, phase],
    })
    .expect("build payload from spec");

    let MsPlotPayload::ScatterPage(page) = payload else {
        panic!("expected scatter page payload");
    };
    assert_eq!(page.gridrows, 1);
    assert_eq!(page.gridcols, 2);
    assert_eq!(page.items.len(), 2);
    assert!(
        page.header_lines
            .iter()
            .any(|line| line.contains("Filename:"))
    );
    assert!(
        page.header_lines
            .iter()
            .any(|line| line.contains("Y Column: Amplitude") || line.contains("Y Column: Phase"))
    );
}

#[test]
fn msexplore_export_api_writes_grid_and_page_manifests() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let mut iterated = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    iterated.iteration.iteraxis = Some(MsIterationAxis::Scan);
    iterated.iteration.xselfscale = true;
    iterated.style.showlegend = true;
    iterated.style.legendposition = MsLegendPosition::ExteriorRight;
    let grid_payload = build_msexplore_plot_payload(&ms, &MsSelection::default(), &iterated)
        .expect("build iterated payload");
    let MsPlotPayload::ScatterGrid(_) = &grid_payload else {
        panic!("expected scatter grid payload");
    };

    let grid_txt = temp.path().join("iter-grid.txt");
    export_msexplore_plot(&grid_payload, &grid_txt).expect("export grid txt");
    let grid_manifest = std::fs::read_to_string(&grid_txt).expect("read grid manifest");
    assert_eq!(
        manifest_header_value(&grid_manifest, "iteraxis"),
        Some("scan")
    );
    assert_eq!(
        manifest_header_value(&grid_manifest, "legendposition"),
        Some("exteriorRight")
    );

    let mut amplitude = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    amplitude.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 2,
        rowindex: 0,
        colindex: 0,
        plotindex: 0,
    };
    amplitude.style.showlegend = true;
    amplitude.style.legendposition = MsLegendPosition::ExteriorRight;

    let mut phase = MsPlotSpec::from_preset(MsPlotPreset::PhaseVsTime);
    phase.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 2,
        rowindex: 0,
        colindex: 1,
        plotindex: 1,
    };
    phase.style.showlegend = true;
    phase.style.legendposition = MsLegendPosition::ExteriorRight;

    let page_payload = build_msexplore_payload(
        &ms,
        &MsExploreSpec {
            ms_path,
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: vec![MsPageHeaderItem::Filename, MsPageHeaderItem::YColumn],
            page_title: Some("Amplitude and Phase".to_string()),
            exprange: MsPageExportRange::Current,
            max_plot_points: DEFAULT_MAX_PLOT_POINTS,
            plots: vec![amplitude, phase],
        },
    )
    .expect("build page payload");
    let MsPlotPayload::ScatterPage(_) = &page_payload else {
        panic!("expected scatter page payload");
    };

    let page_txt = temp.path().join("page.txt");
    export_msexplore_plot(&page_payload, &page_txt).expect("export page txt");
    let page_manifest = std::fs::read_to_string(&page_txt).expect("read page manifest");
    assert_eq!(manifest_header_value(&page_manifest, "gridrows"), Some("1"));
    assert_eq!(manifest_header_value(&page_manifest, "gridcols"), Some("2"));
    assert!(page_manifest.contains("# header_line=Filename:"));
}

#[test]
fn msexplore_export_api_writes_uv_coverage_manifest() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_geometry_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open geometry fixture");
    let coverage = MeasurementSetUvCoverage::from_ms(&ms).expect("build uv coverage");
    let payload = MsPlotPayload::ListObs(
        build_measurement_set_uv_plot_payload(
            &coverage,
            &MeasurementSetPlotSpec::new(MeasurementSetPlotKind::UvCoverage),
        )
        .expect("build listobs uv coverage payload"),
    );

    let output = temp.path().join("uv.txt");
    export_msexplore_plot(&payload, &output).expect("export uv coverage manifest");
    let manifest = std::fs::read_to_string(&output).expect("read uv coverage manifest");
    assert!(manifest.contains("# payload=listobs_uv_coverage"));
    assert!(manifest.contains("# x_axis=u_lambda"));
    assert!(manifest.contains("series_key\tseries_label\tmirrored\tx\ty"));
    assert!(manifest.contains("\tfalse\t"));
    assert!(manifest.contains("\ttrue\t"));
}

#[test]
fn msexplore_page_spec_manifest_keeps_duplicate_cell_coordinates_for_overplot() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-overplot.txt");
    let page_spec_path = temp.path().join("page-overplot.json");

    std::fs::write(
        &page_spec_path,
        serde_json::json!({
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
        })
        .to_string(),
    )
    .expect("write overplot page spec");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--page-spec"])
        .arg(&page_spec_path)
        .args(["--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(manifest_header_value(&manifest, "exprange"), Some("all"));
    assert_eq!(manifest_header_value(&manifest, "gridrows"), Some("1"));
    assert_eq!(manifest_header_value(&manifest, "gridcols"), Some("1"));

    let rows = page_manifest_rows(&plot_path);
    assert_eq!(rows.len(), 512);
    assert_eq!(
        rows.iter()
            .filter(|(plotindex, rowindex, colindex, title, _, _, _, _, _, _)| {
                *plotindex == 0 && *rowindex == 0 && *colindex == 0 && title == "Amplitude:vector"
            })
            .count(),
        256
    );
    assert_eq!(
        rows.iter()
            .filter(|(plotindex, rowindex, colindex, title, _, _, _, _, _, _)| {
                *plotindex == 1 && *rowindex == 0 && *colindex == 0 && title == "Amplitude:scalar"
            })
            .count(),
        256
    );
}
