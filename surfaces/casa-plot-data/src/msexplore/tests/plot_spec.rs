// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn preset_enum_surfaces_round_trip_and_cover_aliases() {
    for preset in MsPlotPreset::ALL {
        assert_eq!(MsPlotPreset::parse(preset.as_str()).unwrap(), preset);
        assert_eq!(preset.to_string(), preset.as_str());
        assert!(!preset.display_name().is_empty());
    }

    let alias_cases = [
        ("uv", MsPlotPreset::UvCoverage),
        ("antennas", MsPlotPreset::AntennaLayout),
        ("scans", MsPlotPreset::ScanTimeline),
        ("spw_coverage", MsPlotPreset::SpectralWindowCoverage),
        ("spws", MsPlotPreset::SpectralWindowCoverage),
        ("amp_time", MsPlotPreset::AmplitudeVsTime),
        ("phase_time", MsPlotPreset::PhaseVsTime),
        (
            "amp_phase_time_stacked",
            MsPlotPreset::AmplitudePhaseVsTimeStacked,
        ),
        ("amplitude_vs_uvdist", MsPlotPreset::AmplitudeVsUvDistance),
        ("amp_uvdist", MsPlotPreset::AmplitudeVsUvDistance),
        ("wt_time", MsPlotPreset::WeightVsTime),
        ("sigma_time", MsPlotPreset::SigmaVsTime),
        ("flag_time", MsPlotPreset::FlagVsTime),
        ("wtsp_time", MsPlotPreset::WeightSpectrumVsTime),
        ("sigmasp_time", MsPlotPreset::SigmaSpectrumVsTime),
        ("flagrow_time", MsPlotPreset::FlagRowVsTime),
        ("elevation_time", MsPlotPreset::ElevationVsTime),
        ("azimuth_time", MsPlotPreset::AzimuthVsTime),
        ("hourang_vs_time", MsPlotPreset::HourAngleVsTime),
        ("hourang_time", MsPlotPreset::HourAngleVsTime),
        ("parang_vs_time", MsPlotPreset::ParallacticAngleVsTime),
        ("parang_time", MsPlotPreset::ParallacticAngleVsTime),
        ("azimuth_elevation", MsPlotPreset::AzimuthVsElevation),
        ("amp_channel", MsPlotPreset::AmplitudeVsChannel),
        ("phase_channel", MsPlotPreset::PhaseVsChannel),
        ("amp_frequency", MsPlotPreset::AmplitudeVsFrequency),
        ("phase_frequency", MsPlotPreset::PhaseVsFrequency),
        ("amp_velocity", MsPlotPreset::AmplitudeVsVelocity),
        ("phase_velocity", MsPlotPreset::PhaseVsVelocity),
        ("real_vs_imag", MsPlotPreset::RealVsImaginary),
    ];
    for (alias, expected) in alias_cases {
        assert_eq!(MsPlotPreset::parse(alias).unwrap(), expected);
    }
    assert!(
        MsPlotPreset::parse("nope")
            .unwrap_err()
            .contains("unsupported")
    );

    let metadata_pairs = [
        (ListObsPlotKind::UvCoverage, MsPlotPreset::UvCoverage),
        (ListObsPlotKind::AntennaLayout, MsPlotPreset::AntennaLayout),
        (ListObsPlotKind::ScanTimeline, MsPlotPreset::ScanTimeline),
        (
            ListObsPlotKind::SpectralWindowCoverage,
            MsPlotPreset::SpectralWindowCoverage,
        ),
        (
            ListObsPlotKind::AmplitudeVsTime,
            MsPlotPreset::AmplitudeVsTime,
        ),
        (ListObsPlotKind::PhaseVsTime, MsPlotPreset::PhaseVsTime),
        (
            ListObsPlotKind::AmplitudeVsUvDistance,
            MsPlotPreset::AmplitudeVsUvDistance,
        ),
    ];
    for (kind, preset) in metadata_pairs {
        assert_eq!(MsPlotPreset::from_listobs_kind(kind), preset);
    }
    assert_eq!(MsPlotPreset::UvCoverage.lowers_to_listobs_metadata(), None);
    assert_eq!(
        MsPlotPreset::AmplitudeVsFrequency.lowers_to_listobs_metadata(),
        None
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn axis_and_related_enums_cover_aliases_and_helper_flags() {
    let axis_cases = [
        (
            MsAxis::Amplitude,
            &["amp", "amplitude"][..],
            "Amplitude",
            true,
            false,
            true,
            false,
            true,
            false,
        ),
        (
            MsAxis::Phase,
            &["phase"][..],
            "Phase",
            true,
            false,
            true,
            false,
            true,
            false,
        ),
        (
            MsAxis::Real,
            &["real"][..],
            "Real",
            true,
            false,
            true,
            false,
            true,
            false,
        ),
        (
            MsAxis::Imaginary,
            &["imag", "imaginary"][..],
            "Imaginary",
            true,
            false,
            true,
            false,
            true,
            false,
        ),
        (
            MsAxis::Time,
            &["time"][..],
            "Time",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Channel,
            &["chan", "channel"][..],
            "Channel",
            false,
            false,
            true,
            false,
            false,
            false,
        ),
        (
            MsAxis::Frequency,
            &["freq", "frequency"][..],
            "Frequency",
            false,
            false,
            true,
            false,
            false,
            true,
        ),
        (
            MsAxis::Velocity,
            &["velocity", "vel"][..],
            "Velocity",
            false,
            false,
            true,
            false,
            false,
            true,
        ),
        (
            MsAxis::UvDistance,
            &["uvdist", "uv_distance", "uv-distance"][..],
            "UV Distance",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::U,
            &["u"][..],
            "U",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::V,
            &["v"][..],
            "V",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::W,
            &["w"][..],
            "W",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Scan,
            &["scan"][..],
            "Scan",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Field,
            &["field"][..],
            "Field",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::SpectralWindow,
            &["spw", "spectral_window", "spectral-window"][..],
            "Spectral Window",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Correlation,
            &["corr", "correlation"][..],
            "Correlation",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Baseline,
            &["baseline"][..],
            "Baseline",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Antenna,
            &["antenna"][..],
            "Antenna",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Azimuth,
            &["azimuth"][..],
            "Azimuth",
            false,
            true,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Elevation,
            &["elevation"][..],
            "Elevation",
            false,
            true,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::HourAngle,
            &["hourang", "hourangle", "hour_angle", "hour-angle"][..],
            "Hour Angle",
            false,
            true,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::ParallacticAngle,
            &["parang", "parallactic_angle", "parallactic-angle"][..],
            "Parallactic Angle",
            false,
            true,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::Weight,
            &["weight"][..],
            "Weight",
            false,
            false,
            false,
            false,
            true,
            false,
        ),
        (
            MsAxis::Sigma,
            &["sigma"][..],
            "Sigma",
            false,
            false,
            false,
            false,
            true,
            false,
        ),
        (
            MsAxis::WeightSpectrum,
            &[
                "wtsp",
                "weightspectrum",
                "weight_spectrum",
                "weight-spectrum",
            ][..],
            "Weight Spectrum",
            false,
            false,
            true,
            false,
            true,
            false,
        ),
        (
            MsAxis::SigmaSpectrum,
            &[
                "sigmasp",
                "sigmaspectrum",
                "sigma_spectrum",
                "sigma-spectrum",
            ][..],
            "Sigma Spectrum",
            false,
            false,
            true,
            false,
            true,
            false,
        ),
        (
            MsAxis::Flag,
            &["flag"][..],
            "Flag",
            false,
            false,
            true,
            true,
            true,
            false,
        ),
        (
            MsAxis::FlagRow,
            &["flagrow", "flag_row", "flag-row"][..],
            "Flag Row",
            false,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            MsAxis::AntRa,
            &["ant-ra", "ant_ra"][..],
            "Antenna RA",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
        (
            MsAxis::AntDec,
            &["ant-dec", "ant_dec"][..],
            "Antenna Dec",
            false,
            false,
            false,
            false,
            false,
            false,
        ),
    ];
    for (
        axis,
        aliases,
        display_name,
        visibility_math,
        derived_geometry,
        channel_bins,
        flag_rows,
        correlation_slots,
        spectral_coordinates,
    ) in axis_cases
    {
        assert_eq!(axis.to_string(), axis.as_str());
        assert_eq!(axis.display_name(), display_name);
        assert_eq!(axis.is_visibility_math(), visibility_math);
        assert_eq!(axis.uses_derived_geometry(), derived_geometry);
        assert_eq!(axis.uses_channel_bins(), channel_bins);
        assert_eq!(axis.uses_flag_rows(), flag_rows);
        assert_eq!(axis.uses_correlation_slots(), correlation_slots);
        assert_eq!(axis.uses_spectral_coordinates(), spectral_coordinates);
        for alias in aliases {
            assert_eq!(MsAxis::parse(alias).unwrap(), axis);
        }
    }
    assert!(MsAxis::parse("bogus").unwrap_err().contains("unsupported"));

    for axis in [MsAxis::UvWave, MsAxis::UWave, MsAxis::VWave, MsAxis::WWave] {
        let token = serde_json::to_value(axis).unwrap();
        assert_eq!(token.as_str(), Some(axis.as_str()));
        assert_eq!(serde_json::from_value::<MsAxis>(token).unwrap(), axis);
    }

    let data_columns = [
        (MsDataColumn::Data, &["data"][..]),
        (
            MsDataColumn::Corrected,
            &["corrected", "corrected_data"][..],
        ),
        (MsDataColumn::Model, &["model", "model_data"][..]),
        (
            MsDataColumn::CorrectedMinusModel,
            &["corrected-model", "corrected_minus_model"][..],
        ),
        (
            MsDataColumn::CorrectedMinusModelScalar,
            &["corrected-model_scalar", "corrected_minus_model_scalar"][..],
        ),
        (
            MsDataColumn::DataMinusModel,
            &["data-model", "data_minus_model"][..],
        ),
        (
            MsDataColumn::DataMinusModelScalar,
            &["data-model_scalar", "data_minus_model_scalar"][..],
        ),
        (
            MsDataColumn::CorrectedDivModel,
            &["corrected/model", "corrected_div_model"][..],
        ),
        (
            MsDataColumn::CorrectedDivModelScalar,
            &["corrected/model_scalar", "corrected_div_model_scalar"][..],
        ),
        (
            MsDataColumn::DataDivModel,
            &["data/model", "data_div_model"][..],
        ),
        (
            MsDataColumn::DataDivModelScalar,
            &["data/model_scalar", "data_div_model_scalar"][..],
        ),
    ];
    for (column, aliases) in data_columns {
        assert_eq!(column.to_string(), column.as_str());
        for alias in aliases {
            assert_eq!(MsDataColumn::parse(alias).unwrap(), column);
        }
    }
    assert!(
        MsDataColumn::parse("bad")
            .unwrap_err()
            .contains("unsupported")
    );

    let color_axes = [
        (MsColorAxis::None, &["none"][..]),
        (MsColorAxis::Field, &["field"][..]),
        (MsColorAxis::Scan, &["scan"][..]),
        (
            MsColorAxis::SpectralWindow,
            &["spw", "spectral_window", "spectral-window"][..],
        ),
        (MsColorAxis::Baseline, &["baseline"][..]),
        (MsColorAxis::Correlation, &["correlation", "corr"][..]),
    ];
    for (axis, aliases) in color_axes {
        assert_eq!(axis.to_string(), axis.as_str());
        for alias in aliases {
            assert_eq!(MsColorAxis::parse(alias).unwrap(), axis);
        }
    }
    assert!(
        MsColorAxis::parse("bad")
            .unwrap_err()
            .contains("unsupported")
    );

    assert_eq!(MsExportFormat::parse("txt").unwrap(), MsExportFormat::Txt);
    assert_eq!(MsExportFormat::Txt.extension(), "txt");
    for removed in ["png", "pdf"] {
        let error = MsExportFormat::parse(removed).unwrap_err();
        assert!(error.contains("removed (#833)"), "{error}");
        assert!(error.contains("--plot-format txt"), "{error}");
    }
    assert!(
        MsExportFormat::parse("jpg")
            .unwrap_err()
            .contains("unsupported")
    );
}

#[test]
fn selection_and_page_enums_preserve_stable_strings() {
    let selection = MsSelection {
        selectdata: false,
        field: Some("0".to_string()),
        spw: Some("1".to_string()),
        antenna: Some("ea01".to_string()),
        scan: Some("5".to_string()),
        observation: Some("2".to_string()),
        array: Some("1".to_string()),
        timerange: Some("09:00:00~10:00:00".to_string()),
        uvrange: Some(">100m".to_string()),
        correlation: Some("RR".to_string()),
        intent: Some("CALIBRATE".to_string()),
        data_description: None,
        state: None,
        msselect: Some("DATA_DESC_ID==0".to_string()),
        feed: Some("0".to_string()),
    };
    let summary = selection.to_summary_options();
    assert!(!summary.selectdata);
    assert_eq!(summary.field.as_deref(), Some("0"));
    assert_eq!(summary.spw.as_deref(), Some("1"));
    assert_eq!(summary.antenna.as_deref(), Some("ea01"));
    assert_eq!(summary.scan.as_deref(), Some("5"));
    assert_eq!(summary.observation.as_deref(), Some("2"));
    assert_eq!(summary.array.as_deref(), Some("1"));
    assert_eq!(summary.timerange.as_deref(), Some("09:00:00~10:00:00"));
    assert_eq!(summary.uvrange.as_deref(), Some(">100m"));
    assert_eq!(summary.correlation.as_deref(), Some("RR"));
    assert_eq!(summary.intent.as_deref(), Some("CALIBRATE"));
    assert_eq!(summary.msselect.as_deref(), Some("DATA_DESC_ID==0"));
    assert_eq!(summary.feed.as_deref(), Some("0"));
    assert!(!summary.listunfl);
    assert_eq!(summary.cachesize_mb, None);

    for (range, raw) in [
        (MsPageExportRange::Current, "current"),
        (MsPageExportRange::All, "all"),
    ] {
        assert_eq!(range.as_str(), raw);
        assert_eq!(range.to_string(), raw);
    }

    for position in [
        MsLegendPosition::UpperRight,
        MsLegendPosition::UpperLeft,
        MsLegendPosition::LowerRight,
        MsLegendPosition::LowerLeft,
        MsLegendPosition::ExteriorRight,
        MsLegendPosition::ExteriorLeft,
        MsLegendPosition::ExteriorTop,
        MsLegendPosition::ExteriorBottom,
    ] {
        assert_eq!(
            serde_json::from_str::<MsLegendPosition>(&format!("\"{}\"", position.as_str()))
                .unwrap(),
            position
        );
        assert_eq!(position.to_string(), position.as_str());
    }

    for item in [
        MsPageHeaderItem::Filename,
        MsPageHeaderItem::YColumn,
        MsPageHeaderItem::ObsDate,
        MsPageHeaderItem::ObsTime,
        MsPageHeaderItem::Observer,
        MsPageHeaderItem::ProjId,
        MsPageHeaderItem::Telescope,
        MsPageHeaderItem::TargName,
        MsPageHeaderItem::TargDir,
    ] {
        assert_eq!(item.to_string(), item.as_str());
    }

    for axis in MsIterationAxis::ALL {
        assert_eq!(MsIterationAxis::parse(axis.as_str()).unwrap(), axis);
        assert_eq!(axis.to_string(), axis.as_str());
        assert!(!axis.display_name().is_empty());
    }
    assert_eq!(
        MsIterationAxis::parse("spectralwindow").unwrap(),
        MsIterationAxis::SpectralWindow
    );
    assert_eq!(
        MsIterationAxis::parse("corr").unwrap(),
        MsIterationAxis::Correlation
    );
    assert!(
        MsIterationAxis::parse("bad")
            .unwrap_err()
            .contains("unsupported")
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn from_preset_and_validation_cover_supported_and_rejected_shapes() {
    for preset in MsPlotPreset::ALL {
        let spec = MsPlotSpec::from_preset(preset);
        assert_eq!(spec.preset, Some(preset));
        assert_eq!(spec.layout, MsLayoutSpec::default());
        assert_eq!(spec.iteration, MsIterationSpec::default());
        assert_eq!(spec.style, MsPlotStyleSpec::default());
        assert!(spec.flag_edit.is_none());
    }

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.y_axes.clear();
    assert!(spec.validate().unwrap_err().contains("at least one y axis"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.y_axes = vec![MsAxis::Amplitude, MsAxis::Phase, MsAxis::Weight];
    assert!(spec.validate().unwrap_err().contains("at most two y axes"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.y_axes = vec![MsAxis::Amplitude, MsAxis::Amplitude];
    assert!(spec.validate().unwrap_err().contains("duplicate y axes"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.layout.gridrows = 0;
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("must be positive integers")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Field);
    spec.y_axes.push(MsAxis::Phase);
    assert!(spec.validate().unwrap_err().contains("multi-y plots"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.xselfscale = true;
    spec.iteration.xsharedaxis = true;
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("x-axis iteration scaling")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.yselfscale = true;
    spec.iteration.ysharedaxis = true;
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("y-axis iteration scaling")
    );

    let finite_flag_edit = MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: 0.0,
            x_max: 1.0,
            y_min: 0.0,
            y_max: 1.0,
        },
        plot_index: None,
        panel_key: None,
        extcorr: false,
        extchannel: false,
    };

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        region: MsFlagRegion {
            x_min: f64::NAN,
            ..finite_flag_edit.region.clone()
        },
        ..finite_flag_edit.clone()
    });
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("finite region bounds")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        panel_key: Some("field=0".to_string()),
        ..finite_flag_edit.clone()
    });
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("only accepts panel_key")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        plot_index: Some(0),
        ..finite_flag_edit.clone()
    });
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("only accepts plot_index")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.averaging.avgscan = true;
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("avgscan requires avgtime")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.averaging.avgfield = true;
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("avgfield requires avgtime")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.averaging.avgbaseline = true;
    spec.averaging.avgantenna = true;
    assert!(spec.validate().unwrap_err().contains("mutually exclusive"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.averaging.avgtime = Some(30.0);
    spec.averaging.avgscan = true;
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    assert!(spec.validate().unwrap_err().contains("iterate by scan"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.averaging.avgtime = Some(30.0);
    spec.averaging.avgfield = true;
    spec.color_by = MsColorAxis::Field;
    assert!(spec.validate().unwrap_err().contains("color by field"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsChannel);
    spec.averaging.avgspw = true;
    spec.color_by = MsColorAxis::SpectralWindow;
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("color by spectral window")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.transforms.phasecenter = Some("J2000 1rad 2rad".to_string());
    assert!(spec.validate().unwrap_err().contains("not yet implemented"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.x_axis = MsAxis::Field;
    assert!(spec.validate().unwrap_err().contains("x axis field"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.x_axis = MsAxis::Amplitude;
    spec.y_axes = vec![MsAxis::Channel];
    spec.validate()
        .expect("channel ordinate uses the existing channel bins");
    spec.y_axes = vec![MsAxis::Scan];
    assert!(spec.validate().unwrap_err().contains("y axis scan"));

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.averaging.avgchannel = Some(8);
    spec.validate()
        .expect("CASA plotms accepts avgchannel for visibility amplitude plots");

    let bins = channel_bins(384, Some(10_000)).expect("high avgchannel");
    assert_eq!(bins.len(), 1);
    assert_eq!(bins[0].start, 0);
    assert_eq!(bins[0].end, 384);

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::UvCoverage);
    spec.averaging.avgchannel = Some(10_000);
    spec.validate()
        .expect("CASA plotms accepts avgchannel for UV coverage");

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::FlagVsTime);
    spec.x_axis = MsAxis::Channel;
    spec.averaging.avgchannel = Some(8);
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("flag plots do not yet support avgchannel")
    );

    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.layout.rowindex = 1;
    assert!(
        spec.validate()
            .unwrap_err()
            .contains("reserved for multi-plot page composition")
    );

    let request = MsExploreSpec {
        ms_path: PathBuf::from("demo.ms"),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
        header_items: Vec::new(),
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points: 10,
        plots: Vec::new(),
    };
    assert!(
        request
            .validate()
            .unwrap_err()
            .contains("at least one plot")
    );

    let request = MsExploreSpec {
        max_plot_points: 0,
        plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime)],
        ..request
    };
    assert!(request.validate().unwrap_err().contains("positive integer"));

    let mut first = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsFrequency);
    first.layout.gridrows = 1;
    first.layout.gridcols = 2;
    first.layout.rowindex = 0;
    first.layout.colindex = 0;
    first.layout.plotindex = 0;
    let mut second = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsFrequency);
    second.layout.gridrows = 2;
    second.layout.gridcols = 2;
    second.layout.rowindex = 0;
    second.layout.colindex = 1;
    second.layout.plotindex = 1;
    let request = MsExploreSpec {
        ms_path: PathBuf::from("demo.ms"),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
        header_items: Vec::new(),
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points: 10,
        plots: vec![first, second],
    };
    assert!(
        request
            .validate()
            .unwrap_err()
            .contains("agree on gridrows/gridcols")
    );

    let mut first = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsFrequency);
    first.layout.gridrows = 1;
    first.layout.gridcols = 2;
    first.layout.rowindex = 0;
    first.layout.colindex = 0;
    first.layout.plotindex = 0;
    let mut second = MsPlotSpec::from_preset(MsPlotPreset::PhaseVsFrequency);
    second.layout.gridrows = 1;
    second.layout.gridcols = 2;
    second.layout.rowindex = 0;
    second.layout.colindex = 1;
    second.layout.plotindex = 1;
    let request = MsExploreSpec {
        ms_path: PathBuf::from("demo.ms"),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
        header_items: Vec::new(),
        page_title: None,
        exprange: MsPageExportRange::All,
        max_plot_points: 10,
        plots: vec![first, second],
    };
    assert!(
        request
            .validate()
            .unwrap_err()
            .contains("share the same x/y axis configuration")
    );
}
