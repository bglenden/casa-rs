// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::OsString;

use casa_task_runtime::{ParameterRuntime, TaskCommandLine};
use tempfile::tempdir;

use super::*;

fn cli_args(args: &[&str]) -> Vec<OsString> {
    args.iter().map(OsString::from).collect()
}

/// A command line as the front end resolves it, read into [`CliOptions`].
fn parse_args(args: Vec<OsString>) -> Result<CliOptions, String> {
    let bundle = casa_provider_contracts::builtin_surface_bundle("msexplore").unwrap();
    let line = TaskCommandLine::parse(&bundle, &args)?;
    let session = ParameterRuntime::default()
        .open_session(line.open_session_request(bundle))
        .map_err(|error| error.to_string())?;
    options_from(&TaskInvocation::new(&session, std::path::Path::new(".")))
        .map_err(|failure| failure.message)
}

#[test]
fn command_schema_describes_core_arguments_and_actions() {
    let schema = command_schema("msexplore-test");

    assert_eq!(schema.command_id, "msexplore");
    assert_eq!(schema.invocation_name, "msexplore-test");
    assert_eq!(schema.display_name, "MSExplore");
    assert_eq!(schema.category, "MeasurementSet");
    assert_eq!(schema.usage, "msexplore-test [parameters]");
    assert!(schema.render_help().contains("<ms-path>"));
    assert!(schema.render_help().contains("--plot-output"));

    let ms_path = schema.argument("vis").expect("canonical vis argument");
    assert!(matches!(
        ms_path.parser,
        UiArgumentParser::Positional { .. }
    ));
    assert_eq!(ms_path.group, "Input");
    assert!(ms_path.required);

    let format = schema.argument("format").expect("format argument");
    assert_eq!(format.default.as_deref(), Some("text"));
    assert_eq!(format.group, "Output");
    assert!(matches!(format.value_kind, UiValueKind::Choice));

    let selectdata = schema.argument("selectdata").expect("selectdata argument");
    assert_eq!(selectdata.default_bool(), Some(true));
    assert_eq!(selectdata.group, "Selection");
    assert!(matches!(selectdata.parser, UiArgumentParser::Toggle { .. }));

    let help = schema.argument("help").expect("help action");
    assert!(matches!(
        help.parser,
        UiArgumentParser::Action {
            action: UiActionKind::Help,
            ..
        }
    ));

    // The runner injects nothing, so the user's `format` decides stdout.
    assert!(schema.managed_output.is_none());
}

#[test]
fn parse_args_captures_dense_single_plot_configuration() {
    let options = parse_args(cli_args(&[
        "--format",
        "json",
        "-o",
        "summary.json",
        "--overwrite",
        "--field",
        "0",
        "--spw",
        "0:3~7",
        "--timerange",
        "09:00:00~10:00:00",
        "--uvrange",
        ">100m",
        "--antenna",
        "DV01&DV02",
        "--scan",
        "5",
        "--correlation",
        "RR,LL",
        "--observation",
        "0",
        "--array",
        "1",
        "--intent",
        "CALIBRATE_BANDPASS",
        "--feed",
        "0",
        "--msselect",
        "DATA_DESC_ID==0",
        "--preset",
        "amplitude_vs_frequency",
        "--data-column",
        "corrected_div_model_scalar",
        "--color-by",
        "correlation",
        "--avgchannel",
        "8",
        "--avgtime",
        "30.5",
        "--avgscan",
        "--avgfield",
        "--avgbaseline",
        "--scalar",
        "--freqframe",
        "BARY",
        "--restfreq",
        "1.420GHz",
        "--veldef",
        "OPTICAL",
        "--iteraxis",
        "spw",
        "--gridrows",
        "2",
        "--gridcols",
        "3",
        "--xselfscale",
        "--ysharedaxis",
        "--title",
        "Amplitude",
        "--xlabel",
        "Time",
        "--ylabel",
        "Amp",
        "--symbolsize",
        "2",
        "--showlegend",
        "--legendposition",
        "exteriorRight",
        "--showmajorgrid",
        "--showminorgrid",
        "--headeritems",
        "filename,observer",
        "--max-points",
        "12345",
        "--plot-output",
        "plot.txt",
        "--plot-format",
        "txt",
        "example.ms",
    ]))
    .expect("parse args");

    assert_eq!(options.ms_path, PathBuf::from("example.ms"));
    assert_eq!(
        options.summary_format,
        MeasurementSetSummaryOutputFormat::Json
    );
    assert_eq!(
        options.summary_output.as_deref(),
        Some(std::path::Path::new("summary.json"))
    );
    assert!(options.overwrite);
    assert_eq!(options.selection.field.as_deref(), Some("0"));
    assert_eq!(options.selection.spw.as_deref(), Some("0:3~7"));
    assert_eq!(
        options.selection.timerange.as_deref(),
        Some("09:00:00~10:00:00")
    );
    assert_eq!(options.selection.uvrange.as_deref(), Some(">100m"));
    assert_eq!(options.selection.antenna.as_deref(), Some("DV01&DV02"));
    assert_eq!(options.selection.scan.as_deref(), Some("5"));
    assert_eq!(options.selection.correlation.as_deref(), Some("RR,LL"));
    assert_eq!(options.selection.observation.as_deref(), Some("0"));
    assert_eq!(options.selection.array.as_deref(), Some("1"));
    assert_eq!(
        options.selection.intent.as_deref(),
        Some("CALIBRATE_BANDPASS")
    );
    assert_eq!(options.selection.feed.as_deref(), Some("0"));
    assert_eq!(
        options.selection.msselect.as_deref(),
        Some("DATA_DESC_ID==0")
    );
    assert_eq!(options.preset, Some(MsPlotPreset::AmplitudeVsFrequency));
    assert_eq!(options.data_column, MsDataColumn::CorrectedDivModelScalar);
    assert_eq!(options.color_by, MsColorAxis::Correlation);
    assert_eq!(options.avgchannel, Some(8));
    assert_eq!(options.avgtime, Some(30.5));
    assert!(options.avgscan);
    assert!(options.avgfield);
    assert!(options.avgbaseline);
    assert!(!options.avgantenna);
    assert!(!options.avgspw);
    assert!(options.scalar);
    assert_eq!(options.freqframe.as_deref(), Some("BARY"));
    assert_eq!(options.restfreq.as_deref(), Some("1.420GHz"));
    assert_eq!(options.veldef, "OPTICAL");
    assert_eq!(options.iteraxis, Some(MsIterationAxis::SpectralWindow));
    assert_eq!(options.gridrows, 2);
    assert_eq!(options.gridcols, 3);
    assert!(options.xselfscale);
    assert!(!options.yselfscale);
    assert!(!options.xsharedaxis);
    assert!(options.ysharedaxis);
    assert_eq!(options.title.as_deref(), Some("Amplitude"));
    assert_eq!(options.xlabel.as_deref(), Some("Time"));
    assert_eq!(options.ylabel.as_deref(), Some("Amp"));
    assert_eq!(options.symbol_size, Some(2));
    assert!(options.showlegend);
    assert_eq!(options.legendposition, MsLegendPosition::ExteriorRight);
    assert!(options.showmajorgrid);
    assert!(options.showminorgrid);
    assert_eq!(options.headeritems.as_deref(), Some("filename,observer"));
    assert_eq!(options.max_points, 12345);
    assert_eq!(
        options.plot_output.as_deref(),
        Some(std::path::Path::new("plot.txt"))
    );
    assert_eq!(options.plot_format, MsExportFormat::Txt);

    let spec = build_explore_spec(&options).expect("build spec");
    assert_eq!(spec.ms_path, PathBuf::from("example.ms"));
    assert_eq!(spec.summary_format, MeasurementSetSummaryOutputFormat::Json);
    assert_eq!(
        spec.header_items,
        vec![MsPageHeaderItem::Filename, MsPageHeaderItem::Observer]
    );
    assert_eq!(spec.max_plot_points, 12345);
    assert_eq!(spec.plots.len(), 1);
    let plot = &spec.plots[0];
    assert_eq!(plot.preset, Some(MsPlotPreset::AmplitudeVsFrequency));
    assert_eq!(plot.data_column, MsDataColumn::CorrectedDivModelScalar);
    assert_eq!(plot.color_by, MsColorAxis::Correlation);
    assert_eq!(plot.averaging.avgchannel, Some(8));
    assert_eq!(plot.averaging.avgtime, Some(30.5));
    assert!(plot.averaging.avgscan);
    assert!(plot.averaging.avgfield);
    assert!(plot.averaging.avgbaseline);
    assert!(!plot.averaging.avgantenna);
    assert!(!plot.averaging.avgspw);
    assert!(plot.averaging.scalar);
    assert_eq!(plot.transforms.freqframe.as_deref(), Some("BARY"));
    assert_eq!(plot.transforms.restfreq.as_deref(), Some("1.420GHz"));
    assert_eq!(plot.transforms.veldef, "OPTICAL");
    assert_eq!(
        plot.iteration.iteraxis,
        Some(MsIterationAxis::SpectralWindow)
    );
    assert!(plot.iteration.xselfscale);
    assert!(!plot.iteration.yselfscale);
    assert!(!plot.iteration.xsharedaxis);
    assert!(plot.iteration.ysharedaxis);
    assert_eq!(plot.style.title.as_deref(), Some("Amplitude"));
    assert_eq!(plot.style.xlabel.as_deref(), Some("Time"));
    assert_eq!(plot.style.ylabel.as_deref(), Some("Amp"));
    assert_eq!(plot.style.symbol_size, Some(2));
    assert!(plot.style.showlegend);
    assert_eq!(plot.style.legendposition, MsLegendPosition::ExteriorRight);
    assert!(plot.style.showmajorgrid);
    assert!(plot.style.showminorgrid);
}

#[test]
fn page_spec_build_merges_header_items_without_duplicates() {
    let temp = tempdir().expect("tempdir");
    let page_spec_path = temp.path().join("page.json");
    fs::write(
        &page_spec_path,
        r#"{
          "gridrows": 2,
          "gridcols": 2,
          "page_title": "Page Title",
          "exprange": "all",
          "headeritems": "filename,ycolumn",
          "plots": [
            {
              "preset": "amplitude_vs_frequency",
              "iteraxis": "field",
              "rowindex": 0,
              "colindex": 0,
              "plotindex": 0,
              "showlegend": true,
              "legendposition": "upperLeft",
              "showmajorgrid": true,
              "showminorgrid": true,
              "symbol_size": 1
            },
            {
              "preset": "amplitude_vs_frequency",
              "iteraxis": "field",
              "rowindex": 0,
              "colindex": 1,
              "plotindex": 1
            }
          ]
        }"#,
    )
    .expect("write page spec");

    let options = parse_args(vec![
        OsString::from("--format"),
        OsString::from("json"),
        OsString::from("--page-spec"),
        page_spec_path.as_os_str().to_os_string(),
        OsString::from("--headeritems"),
        OsString::from("telescope,filename"),
        OsString::from("example.ms"),
    ])
    .expect("parse args");
    let spec = build_explore_spec(&options).expect("build spec from page spec");

    assert_eq!(spec.ms_path, PathBuf::from("example.ms"));
    assert_eq!(spec.summary_format, MeasurementSetSummaryOutputFormat::Json);
    assert_eq!(spec.page_title.as_deref(), Some("Page Title"));
    assert_eq!(spec.exprange, MsPageExportRange::All);
    assert_eq!(
        spec.header_items,
        vec![
            MsPageHeaderItem::Filename,
            MsPageHeaderItem::YColumn,
            MsPageHeaderItem::Telescope,
        ]
    );
    assert_eq!(spec.plots.len(), 2);
    let plot = &spec.plots[0];
    assert_eq!(plot.preset, Some(MsPlotPreset::AmplitudeVsFrequency));
    assert_eq!(plot.layout.gridrows, 2);
    assert_eq!(plot.layout.gridcols, 2);
    assert_eq!(plot.layout.rowindex, 0);
    assert_eq!(plot.layout.colindex, 0);
    assert_eq!(plot.layout.plotindex, 0);
    assert!(plot.style.showlegend);
    assert_eq!(plot.style.legendposition, MsLegendPosition::UpperLeft);
    assert!(plot.style.showmajorgrid);
    assert!(plot.style.showminorgrid);
    assert_eq!(plot.style.symbol_size, Some(1));
    let second_plot = &spec.plots[1];
    assert_eq!(second_plot.preset, Some(MsPlotPreset::AmplitudeVsFrequency));
    assert_eq!(second_plot.layout.colindex, 1);
    assert_eq!(second_plot.layout.plotindex, 1);
}

#[test]
fn parse_and_build_helpers_reject_invalid_combinations() {
    assert!(
        parse_args(cli_args(&["--flag-xmin", "0.0", "example.ms"]))
            .unwrap_err()
            .contains("require --flag-action")
    );
    assert!(
        parse_args(cli_args(&[
            "--page-spec",
            "page.json",
            "--preset",
            "uv_coverage",
            "example.ms"
        ]))
        .unwrap_err()
        .contains("--page-spec cannot be combined")
    );
    assert!(
        parse_args(cli_args(&["--plot-output", "plot.txt", "example.ms"]))
            .unwrap_err()
            .contains("--plot-output requires either --preset or both --xaxis and --yaxis")
    );
    assert!(
        parse_args(cli_args(&[
            "--flag-action",
            "flag",
            "--flag-plotindex",
            "0",
            "--flag-panel",
            "panel",
            "--preset",
            "uv_coverage",
            "example.ms"
        ]))
        .unwrap_err()
        .contains("either --flag-plotindex or --flag-panel")
    );
    assert!(
        build_plot_spec_from_values(
            Some(MsPlotPreset::AmplitudePhaseVsTimeStacked),
            None,
            None,
            Some(MsAxis::Phase),
            MsDataColumn::Data,
            MsColorAxis::Field,
            None,
            None,
            false,
            false,
            false,
            false,
            false,
            false,
            None,
            None,
            "RADIO".to_string(),
            None,
            1,
            1,
            0,
            0,
            0,
            false,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            false,
            MsLegendPosition::UpperRight,
            false,
            false,
        )
        .unwrap_err()
        .contains("--yaxis2 is not supported")
    );
    let cases: [(&[&str], &str); 3] = [
        (
            &["--xselfscale", "--xsharedaxis"],
            "cannot request both self-scaled and shared axes",
        ),
        (&["--avgantenna", "--avgbaseline"], "mutually exclusive"),
        (
            &["--iteraxis", "spw", "--avgspw"],
            "cannot iterate by spectral window while averaging across spectral windows",
        ),
    ];
    for (controls, refusal) in cases {
        let mut line = vec!["--preset", "amplitude_vs_time"];
        line.extend(controls);
        line.push("example.ms");
        let options = parse_args(cli_args(&line)).expect("parse args");
        let error = build_explore_spec(&options).unwrap_err();
        assert!(error.contains(refusal), "{error}");
    }
}

#[test]
fn a_page_spec_from_a_profile_also_refuses_single_plot_controls() {
    // A control counts as set whether a flag or the base profile sets it.
    let temp = tempdir().expect("tempdir");
    let profile = temp.path().join("plot.toml");
    let bundle = casa_provider_contracts::builtin_surface_bundle("msexplore").unwrap();
    fs::write(
        &profile,
        format!(
            "[casars]\nformat = 1\nsurface = \"msexplore\"\nkind = \"task\"\ncontract = {}\n\n[parameters]\nvis = \"example.ms\"\npreset = \"uv_coverage\"\n",
            bundle.surface.contract_version()
        ),
    )
    .expect("write profile");
    let error = parse_args(vec![
        OsString::from("--params"),
        profile.into_os_string(),
        OsString::from("--page-spec"),
        OsString::from("page.json"),
    ])
    .unwrap_err();
    assert!(error.contains("--page-spec cannot be combined"), "{error}");
}

#[test]
fn build_flag_edit_spec_and_write_output_handle_expected_edges() {
    let options = parse_args(cli_args(&[
        "--preset",
        "amplitude_vs_time",
        "--flag-action",
        "flag",
        "--flag-xmin",
        "1.0",
        "--flag-xmax",
        "2.0",
        "--flag-ymin",
        "3.0",
        "--flag-ymax",
        "4.0",
        "--flag-plotindex",
        "7",
        "--flag-extcorr",
        "--flag-extchannel",
        "example.ms",
    ]))
    .expect("parse args");
    let flag_edit = build_flag_edit_spec(&options)
        .expect("build flag edit spec")
        .expect("flag edit");
    assert_eq!(flag_edit.action, MsFlagAction::Flag);
    assert_eq!(flag_edit.region.x_min, 1.0);
    assert_eq!(flag_edit.region.x_max, 2.0);
    assert_eq!(flag_edit.region.y_min, 3.0);
    assert_eq!(flag_edit.region.y_max, 4.0);
    assert_eq!(flag_edit.plot_index, Some(7));
    assert!(flag_edit.extcorr);
    assert!(flag_edit.extchannel);

    let temp = tempdir().expect("tempdir");
    let output_path = temp.path().join("out.txt");
    write_output(&output_path, false, "first").expect("write first");
    assert_eq!(fs::read_to_string(&output_path).unwrap(), "first");
    assert!(
        write_output(&output_path, false, "second")
            .unwrap_err()
            .contains("refusing to overwrite")
    );
    write_output(&output_path, true, "second").expect("overwrite");
    assert_eq!(fs::read_to_string(&output_path).unwrap(), "second");
}
