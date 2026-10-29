// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use casa_ms::MsSelection;
use casa_provider_contracts::{TaskFailureKind, builtin_surface_bundle};
use casa_task_runtime::{ParameterRuntime, TaskCommandLine, TaskFailure, TaskInvocation};
use tempfile::TempDir;

use super::read::{self, OutputFormat, Report};
use super::render;
use crate::{
    ApplyCalibrationTablePlan, ApplyCalibrationTableSpec, ApplyExecutionReport,
    ApplyExecutionTimings, ApplyInterpolationMode, ApplyMode, ApplyPlan, ApplySpwMapping,
    BandpassSolveCombine, BandpassSolveReport, BandpassType, CalibrationColumnSummary,
    CalibrationIndexedStats, CalibrationIssueSeverity, CalibrationKeywordSummary,
    CalibrationParameterFamily, CalibrationStatsAxis, CalibrationStatsReport,
    CalibrationSubtableSummary, CalibrationTableSummary, CalibrationTaskRequest,
    CalibrationTaskResult, CalibrationValidationIssue, CalibrationValueStats,
    ExecuteApplyTaskRequest, ExportCorrectedDataTaskRequest, FluxScaleFieldResult, FluxScaleReport,
    FluxScaleRequest, FluxScaleSpwResult, GainFieldSelector, GainSolveCombine, GainSolveInterval,
    GainSolveMode, GainSolveModelSource, GainSolveReport, GainType, GencalTaskRequest, GencalType,
    RefAntSelector, ResolvedGainField, ResolvedNearestGainField, SolveBandpassTaskRequest,
    SolveGainTaskRequest, StatsTaskRequest, SummaryTaskRequest, TimeCoverageSummary,
};

/// `args` on `surface`'s command line as the front end resolves them, read
/// by `read`.
fn reading<T>(
    surface: &str,
    args: &[&str],
    read: impl FnOnce(&TaskInvocation<'_>) -> Result<T, TaskFailure>,
) -> Result<T, TaskFailure> {
    let bundle = builtin_surface_bundle(surface).expect("calibration surface");
    let args = args.iter().map(OsString::from).collect::<Vec<_>>();
    let line = TaskCommandLine::parse(&bundle, &args).expect("command line");
    let session = ParameterRuntime::default()
        .open_session(line.open_session_request(bundle))
        .expect("session");
    read(&TaskInvocation::new(&session, Path::new(".")))
}

fn request(surface: &str, args: &[&str]) -> Result<CalibrationTaskRequest, TaskFailure> {
    reading(surface, args, read::task_request)
}

fn table(path: &str, calwt: bool) -> ApplyCalibrationTableSpec {
    let mut spec = ApplyCalibrationTableSpec::new(path);
    spec.calwt = calwt;
    spec
}

fn refusal(result: Result<CalibrationTaskRequest, TaskFailure>) -> TaskFailure {
    let failure = result.expect_err("a refusal");
    assert_eq!(failure.kind, TaskFailureKind::Validation, "{failure:?}");
    failure
}

#[test]
fn calibrate_defaults_to_apply_and_backs_up_flags_as_casa_does() {
    let request = request(
        "calibrate",
        &["--ms", "dataset.ms", "--gaintables", "phase.gcal"],
    );
    assert_eq!(
        request.expect("apply"),
        CalibrationTaskRequest::ExecuteApply(ExecuteApplyTaskRequest {
            measurement_set: PathBuf::from("dataset.ms"),
            selection: MsSelection::new(),
            calibration_tables: vec![table("phase.gcal", false)],
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            flagbackup: true,
        })
    );
}

#[test]
fn calibrate_apply_reads_its_chain_and_selection() {
    let request = request(
        "calibrate",
        &[
            "--ms",
            "dataset.ms",
            "--gaintables",
            "phase.gcal,bandpass.bcal",
            "--calwt",
            "true,false",
            "--apply-mode",
            "trial",
            "--field",
            "0",
            "--spw",
            "0,1",
            "--parang",
            "--no-flagbackup",
        ],
    );
    assert_eq!(
        request.expect("apply"),
        CalibrationTaskRequest::ExecuteApply(ExecuteApplyTaskRequest {
            measurement_set: PathBuf::from("dataset.ms"),
            selection: MsSelection {
                field: Some("0".into()),
                spw: Some("0,1".into()),
                ..MsSelection::new()
            },
            calibration_tables: vec![table("phase.gcal", true), table("bandpass.bcal", false)],
            apply_mode: ApplyMode::Trial,
            parang: true,
            flagbackup: false,
        })
    );
}

#[test]
fn aligned_chain_lists_reach_each_table() {
    let request = request(
        "calibrate",
        &[
            "--ms",
            "dataset.ms",
            "--gaintables",
            "phase.gcal,bandpass.bcal",
            "--gainfield",
            "nearest;0",
            "--interp",
            "linear;nearest,linear",
            "--spwmap",
            ";0,0",
        ],
    );
    let CalibrationTaskRequest::ExecuteApply(apply) = request.expect("apply") else {
        panic!("an apply");
    };
    let mut phase = table("phase.gcal", false);
    phase.gainfield = Some(GainFieldSelector::Nearest);
    let mut bandpass = table("bandpass.bcal", false);
    bandpass.gainfield = Some(GainFieldSelector::FieldId(0));
    bandpass.interp = ApplyInterpolationMode::NearestLinear;
    bandpass.spwmap = vec![0, 0];
    assert_eq!(apply.calibration_tables, [phase, bandpass]);

    let misaligned = refusal(request_with_gainfield("nearest;0;1"));
    assert!(
        misaligned
            .message
            .contains("expected one value or one per table"),
        "{misaligned:?}"
    );
    assert_eq!(misaligned.parameter.as_deref(), Some("gainfield"));
}

#[test]
fn repeated_table_flags_name_every_table_in_order() {
    // Astra's #967 P1: a repeated --gaintables kept only its last table, so
    // an apply could run without the phase calibration.
    let repeated = [
        "--gaintables",
        "phase.gcal",
        "--gaintables",
        "bandpass.bcal",
    ];
    let listed = ["--gaintables", "phase.gcal,bandpass.bcal"];
    let chain = |surface: &str, mode: &[&str], tables: &[&str]| {
        let mut args = vec!["--ms", "dataset.ms"];
        args.extend(mode);
        args.extend(tables);
        args.extend(["--gainfield", "nearest;0"]);
        match request(surface, &args).expect("a request") {
            CalibrationTaskRequest::ExecuteApply(apply) => apply.calibration_tables,
            CalibrationTaskRequest::SolveGain(solve) => solve.prior_calibration_tables,
            other => panic!("{surface}: {other:?}"),
        }
    };
    for (surface, mode) in [
        ("calibrate", &[][..]),
        (
            "calibrate",
            &[
                "--mode",
                "solve_gain",
                "--refant",
                "0",
                "--out",
                "gain.gcal",
            ][..],
        ),
    ] {
        let tables = chain(surface, mode, &repeated);
        assert_eq!(tables, chain(surface, mode, &listed), "{surface} {mode:?}");
        let paths = tables
            .iter()
            .map(|table| table.path.clone())
            .collect::<Vec<_>>();
        assert_eq!(paths, ["phase.gcal", "bandpass.bcal"].map(PathBuf::from));
        assert_eq!(tables[0].gainfield, Some(GainFieldSelector::Nearest));
        assert_eq!(tables[1].gainfield, Some(GainFieldSelector::FieldId(0)));
    }

    let summary = |paths: &[&str]| {
        let mut args = vec!["--mode", "summary"];
        args.extend(paths);
        request("calibrate", &args).expect("summary")
    };
    assert_eq!(
        summary(&["--summary-paths", "a.gcal", "--summary-paths", "b.gcal"]),
        summary(&["--summary-paths", "a.gcal,b.gcal"])
    );
}

#[test]
fn a_table_list_reads_alike_from_a_list_or_one_string() {
    // A profile or --json-run request may hold the tables as a list or as
    // one comma-separated string.
    let both = vec![PathBuf::from("phase.gcal"), PathBuf::from("bandpass.bcal")];
    for value in [
        casa_provider_contracts::ParameterValue::String("phase.gcal, bandpass.bcal".into()),
        casa_provider_contracts::ParameterValue::Array(vec![
            casa_provider_contracts::ParameterValue::String("phase.gcal".into()),
            casa_provider_contracts::ParameterValue::String("sub/../bandpass.bcal".into()),
        ]),
    ] {
        let mut session = casa_task_runtime::ParameterSession::defaults(
            builtin_surface_bundle("calibrate").unwrap(),
        )
        .unwrap();
        session
            .set(
                "vis",
                casa_provider_contracts::ParameterValue::String("dataset.ms".into()),
            )
            .unwrap();
        session.set("gaintables", value.clone()).unwrap();
        let CalibrationTaskRequest::ExecuteApply(apply) =
            read::task_request(&TaskInvocation::new(&session, Path::new("."))).unwrap()
        else {
            panic!("an apply");
        };
        let paths = apply
            .calibration_tables
            .iter()
            .map(|table| table.path.clone())
            .collect::<Vec<_>>();
        assert_eq!(paths, both, "{value:?}");
    }
}

fn request_with_gainfield(gainfield: &str) -> Result<CalibrationTaskRequest, TaskFailure> {
    request(
        "calibrate",
        &[
            "--ms",
            "dataset.ms",
            "--gaintables",
            "phase.gcal,bandpass.bcal",
            "--gainfield",
            gainfield,
        ],
    )
}

#[test]
fn a_callibrary_names_the_chain_alone() {
    let dir = TempDir::new().expect("tempdir");
    let callib = dir.path().join("apply.callib");
    std::fs::write(&callib, "caltable='phase.gcal' calwt=F tinterp='nearest'\n").expect("write");
    let callib = callib.to_str().expect("utf8");
    let CalibrationTaskRequest::ExecuteApply(apply) =
        request("calibrate", &["--ms", "dataset.ms", "--callib", callib]).expect("apply")
    else {
        panic!("an apply");
    };
    assert_eq!(apply.calibration_tables.len(), 1);
    assert_eq!(
        apply.calibration_tables[0].path,
        dir.path().join("phase.gcal")
    );

    let both = refusal(request(
        "calibrate",
        &[
            "--ms",
            "dataset.ms",
            "--callib",
            callib,
            "--gaintables",
            "phase.gcal",
        ],
    ));
    assert!(both.message.contains("not both"), "{both:?}");
    let none = refusal(request("calibrate", &["--ms", "dataset.ms"]));
    assert!(
        none.message.contains("requires calibration input"),
        "{none:?}"
    );
}

#[test]
fn summary_stats_and_export_are_calibrate_modes() {
    assert_eq!(
        request(
            "calibrate",
            &[
                "--mode",
                "summary",
                "--summary-paths",
                "phase.gcal,bandpass.bcal"
            ]
        )
        .expect("summary"),
        CalibrationTaskRequest::Summary(SummaryTaskRequest {
            paths: vec![PathBuf::from("phase.gcal"), PathBuf::from("bandpass.bcal")],
        })
    );
    let missing = refusal(request("calibrate", &["--mode", "summary"]));
    assert_eq!(missing.parameter.as_deref(), Some("summary_paths"));

    assert_eq!(
        request(
            "calibrate",
            &[
                "--mode",
                "stats",
                "--table",
                "example.gcal",
                "--axis",
                "phase",
                "--datacolumn",
                "CPARAM",
                "--use-flags",
            ]
        )
        .expect("stats"),
        CalibrationTaskRequest::Stats(StatsTaskRequest {
            path: PathBuf::from("example.gcal"),
            axis: CalibrationStatsAxis::Phase,
            datacolumn: Some("CPARAM".into()),
            use_flags: true,
        })
    );

    assert_eq!(
        request(
            "calibrate",
            &[
                "--mode",
                "export_corrected_data",
                "--ms",
                "calibrated.ms",
                "--output-ms",
                "selfcal.ms",
            ]
        )
        .expect("export"),
        CalibrationTaskRequest::ExportCorrectedData(ExportCorrectedDataTaskRequest {
            input_ms: PathBuf::from("calibrated.ms"),
            output_ms: PathBuf::from("selfcal.ms"),
            selection: MsSelection::new(),
        })
    );
}

#[test]
fn a_mode_reads_only_the_parameters_the_contract_emits_for_it() {
    // `field` is emitted for the apply and solve modes only, as the
    // provider projection was.
    let plain = request(
        "calibrate",
        &["--mode", "summary", "--summary-paths", "a.gcal"],
    );
    let with_field = request(
        "calibrate",
        &[
            "--mode",
            "summary",
            "--summary-paths",
            "a.gcal",
            "--field",
            "3",
            "--minsnr",
            "9",
        ],
    );
    assert_eq!(plain.expect("summary"), with_field.expect("summary"));
}

fn gain_solve() -> SolveGainTaskRequest {
    let mut prior = table("prior.gcal", true);
    prior.interp = ApplyInterpolationMode::Linear;
    SolveGainTaskRequest {
        measurement_set: PathBuf::from("dataset.ms"),
        selection: MsSelection {
            field: Some("0".into()),
            ..MsSelection::new()
        },
        output_table: PathBuf::from("phase.gcal"),
        gain_type: GainType::T,
        solve_mode: GainSolveMode::AmplitudePhase,
        solve_interval: GainSolveInterval::Seconds(30.0),
        combine: GainSolveCombine {
            scans: true,
            fields: true,
        },
        refant: RefAntSelector::AntennaName("VA15".into()),
        prior_calibration_tables: vec![prior],
        parang: false,
        model_source: GainSolveModelSource::ModelColumn,
        normalize_average_amplitude: true,
        min_snr: 2.5,
        min_baselines_per_antenna: 2,
        smodel: [2.5, 0.1, 0.0, -0.2],
    }
}

const GAIN_ARGS: [&str; 26] = [
    "--ms",
    "dataset.ms",
    "--out",
    "phase.gcal",
    "--refant",
    "VA15",
    "--gain-type",
    "t",
    "--mode-gain",
    "ap",
    "--solint",
    "30s",
    "--combine-gain",
    "scan,field",
    "--model-source",
    "model-column",
    "--smodel",
    "2.5,0.1,0.0,-0.2",
    "--minsnr",
    "2.5",
    "--minblperant",
    "2",
    "--gaintables",
    "prior.gcal",
    "--field",
    "0",
];

#[test]
fn calibrate_gain_solve_reads_every_parameter_it_emits() {
    let mut args = vec!["--mode", "solve_gain", "--solnorm"];
    args.extend(GAIN_ARGS);
    assert_eq!(
        request("calibrate", &args).expect("calibrate"),
        CalibrationTaskRequest::SolveGain(gain_solve())
    );
    args.push("--parang");
    let CalibrationTaskRequest::SolveGain(solve) = request("calibrate", &args).expect("calibrate")
    else {
        panic!("a gain solve");
    };
    assert_eq!(
        solve,
        SolveGainTaskRequest {
            parang: true,
            ..gain_solve()
        }
    );
    // CASA's gaincal default of four baselines per antenna.
    let CalibrationTaskRequest::SolveGain(defaults) = request(
        "calibrate",
        &[
            "--mode",
            "solve_gain",
            "--ms",
            "dataset.ms",
            "--out",
            "g.gcal",
            "--refant",
            "0",
        ],
    )
    .expect("calibrate") else {
        panic!("a gain solve");
    };
    assert_eq!(defaults.min_baselines_per_antenna, 4);
    assert_eq!(defaults.refant, RefAntSelector::AntennaId(0));
    assert_eq!(defaults.solve_interval, GainSolveInterval::Infinite);
}

#[test]
fn calibrate_bandpass_solve_reads_every_parameter_it_emits() {
    let args = [
        "--mode",
        "solve_bandpass",
        "--ms",
        "dataset.ms",
        "--out",
        "bandpass.bcal",
        "--refant",
        "7",
        "--combine-bandpass",
        "scan,field",
        "--bandtype",
        "bpoly",
        "--degamp",
        "5",
        "--degphase",
        "4",
        "--solnorm",
        "--smodel",
        "3,0,0,0",
        "--gaintables",
        "prior.gcal",
    ];
    assert_eq!(
        request("calibrate", &args).expect("calibrate"),
        CalibrationTaskRequest::SolveBandpass(SolveBandpassTaskRequest {
            measurement_set: PathBuf::from("dataset.ms"),
            selection: MsSelection::new(),
            output_table: PathBuf::from("bandpass.bcal"),
            refant: RefAntSelector::AntennaId(7),
            prior_calibration_tables: vec![table("prior.gcal", true)],
            parang: false,
            combine: BandpassSolveCombine {
                scans: true,
                fields: true,
            },
            band_type: BandpassType::BPoly,
            normalize_average_amplitude: true,
            amplitude_degree: 5,
            phase_degree: 4,
            smodel: [3.0, 0.0, 0.0, 0.0],
        })
    );
}

#[test]
fn calibrate_bandpass_reads_no_min_snr() {
    // `calibrate`'s `min_snr` is its gain solve's; its bandpass reads none.
    let calibrate = [
        "--mode",
        "solve_bandpass",
        "--minsnr",
        "2",
        "--ms",
        "dataset.ms",
        "--out",
        "b.bcal",
        "--refant",
        "0",
    ];
    request("calibrate", &calibrate).expect("calibrate's bandpass");
}

#[test]
fn calibrate_fluxscale_reads_every_parameter_it_emits() {
    let args = [
        "--mode",
        "fluxscale",
        "--in",
        "gain.gcal",
        "--out",
        "flux.gcal",
        "--reference",
        "1331+305",
        "--transfer",
        "1445+099",
        "--refspwmap",
        "0,0",
        "--gainthreshold",
        "0.2",
        "--incremental",
    ];
    assert_eq!(
        request("calibrate", &args).expect("fluxscale"),
        CalibrationTaskRequest::FluxScale(FluxScaleRequest {
            input_table: PathBuf::from("gain.gcal"),
            output_table: PathBuf::from("flux.gcal"),
            reference_fields: vec!["1331+305".into()],
            transfer_fields: vec!["1445+099".into()],
            refspwmap: vec![0, 0],
            gainthreshold: Some(0.2),
            incremental: true,
        })
    );
    let CalibrationTaskRequest::FluxScale(defaults) = request(
        "calibrate",
        &[
            "--mode",
            "fluxscale",
            "--in",
            "gain.gcal",
            "--out",
            "flux.gcal",
            "--reference",
            "3C286",
        ],
    )
    .expect("fluxscale") else {
        panic!("a fluxscale");
    };
    assert_eq!(defaults.gainthreshold, None);
    assert!(defaults.transfer_fields.is_empty());
    assert!(defaults.refspwmap.is_empty());

    let bad = refusal(request(
        "calibrate",
        &[
            "--mode",
            "fluxscale",
            "--in",
            "gain.gcal",
            "--out",
            "flux.gcal",
            "--reference",
            "3C286",
            "--refspwmap",
            "0,nope",
        ],
    ));
    assert_eq!(bad.parameter.as_deref(), Some("refspwmap"));
    assert!(bad.message.contains("parse refspwmap"), "{bad:?}");
}

#[test]
fn calibrate_gencal_reads_every_parameter_it_emits() {
    let args = [
        "--mode",
        "gencal",
        "--ms",
        "tutorial.ms",
        "--out",
        "cal.tau",
        "--caltype",
        "opac",
        "--spw",
        "0,1",
        "--parameter",
        "0.1,0.2",
    ];
    assert_eq!(
        request("calibrate", &args).expect("gencal"),
        CalibrationTaskRequest::Gencal(GencalTaskRequest {
            measurement_set: PathBuf::from("tutorial.ms"),
            output_table: PathBuf::from("cal.tau"),
            caltype: GencalType::Opac,
            antenna: String::new(),
            spw: "0,1".into(),
            parameter: vec![0.1, 0.2],
            gaincurve_table: None,
        })
    );
    // `calibrate`'s gencal mode needs its caltype set.
    let unset = refusal(request(
        "calibrate",
        &["--mode", "gencal", "--ms", "t.ms", "--out", "c.tau"],
    ));
    assert_eq!(unset.parameter.as_deref(), Some("caltype"));
}

#[test]
fn a_solve_needs_its_reference_antenna() {
    let missing = refusal(request(
        "calibrate",
        &[
            "--mode",
            "solve_gain",
            "--ms",
            "dataset.ms",
            "--out",
            "g.gcal",
        ],
    ));
    assert_eq!(missing.parameter.as_deref(), Some("refant"));
}

#[test]
fn the_report_controls_are_read_on_every_surface() {
    for surface in super::CALIBRATION_SURFACES {
        assert_eq!(
            reading(
                surface,
                &["--format", "json", "-o", "report.txt", "--overwrite"],
                read::report
            )
            .expect("report"),
            Report {
                format: OutputFormat::Json,
                output: Some(PathBuf::from("report.txt")),
                overwrite: true,
            },
            "{surface}"
        );
        assert_eq!(
            reading(surface, &[], read::report).expect("report"),
            Report {
                format: OutputFormat::Text,
                output: None,
                overwrite: false,
            },
            "{surface}"
        );
    }
}

#[test]
fn an_existing_report_is_refused_unless_overwrite_is_set() {
    let dir = TempDir::new().expect("tempdir");
    let report = dir.path().join("report.txt");
    std::fs::write(&report, "an earlier report").expect("seed");
    let refused = super::refuse_existing_report(&report, false).expect_err("refused");
    assert_eq!(refused.kind, TaskFailureKind::Validation);
    assert_eq!(refused.parameter.as_deref(), Some("output"));
    super::refuse_existing_report(&report, true).expect("overwrite");
    super::refuse_existing_report(&dir.path().join("new.txt"), false).expect("a new report");
}

#[test]
fn json_is_the_workflows_own_report() {
    let empty = CalibrationTaskResult::Summary(Vec::new());
    assert_eq!(
        render::render(OutputFormat::Json, &empty).expect("json"),
        "[]\n"
    );
    assert_eq!(
        render::render(OutputFormat::Text, &empty).expect("text"),
        ""
    );
}

#[test]
fn value_parsers_cover_their_edges() {
    assert_eq!(render::format_duration_ns(999), "999ns");
    assert_eq!(render::format_duration_ns(12_500), "12.500us");
    assert_eq!(render::format_duration_ns(1_250_000), "1.250ms");
    assert_eq!(render::format_duration_ns(1_500_000_000), "1.500s");
    assert!(
        read::parse_interp_value("bogus")
            .unwrap_err()
            .contains("is unsupported")
    );
    assert_eq!(
        read::parse_interp_value("").unwrap(),
        ApplyInterpolationMode::Linear
    );
    assert_eq!(
        read::parse_gainfield_value("nearest,0"),
        Some(GainFieldSelector::FieldName("nearest,0".into()))
    );
    assert_eq!(
        read::parse_gain_solve_interval("0s").unwrap(),
        GainSolveInterval::Seconds(0.0)
    );
}

fn sample_keywords() -> CalibrationKeywordSummary {
    CalibrationKeywordSummary {
        par_type: Some("Complex".into()),
        vis_cal: Some("G Jones".into()),
        ms_name: Some("dataset.ms".into()),
        pol_basis: Some("Circular".into()),
        casa_version: Some("6.7.0".into()),
    }
}

fn sample_summary(path: &str) -> CalibrationTableSummary {
    CalibrationTableSummary {
        path: PathBuf::from(path),
        table_type: "Calibration".into(),
        table_subtype: "G Jones".into(),
        row_count: 12,
        columns: vec!["TIME".into(), "CPARAM".into()],
        keywords: sample_keywords(),
        subtables: vec![CalibrationSubtableSummary {
            name: "FIELD".into(),
            stored_reference: Some("Table: field".into()),
            resolved_path: Some(PathBuf::from(format!("{path}/FIELD"))),
            exists: true,
            row_count: Some(4),
            open_error: None,
        }],
        parameter_family: CalibrationParameterFamily::Complex,
        parameter_column: CalibrationColumnSummary {
            parameter_column: Some("CPARAM".into()),
            parameter_primitive_type: Some("Complex".into()),
            first_cell_shape: Some(vec![2, 1]),
        },
        field_ids: vec![0, 1],
        spectral_window_ids: vec![0, 1],
        antenna1_ids: vec![0, 1],
        antenna2_ids: vec![0],
        observation_ids: vec![7],
        time_coverage: Some(TimeCoverageSummary {
            min_time: 1.0,
            max_time: 5.0,
            min_interval: Some(1.0),
            max_interval: Some(2.0),
        }),
        issues: vec![CalibrationValidationIssue {
            code: "warn/test".into(),
            severity: CalibrationIssueSeverity::Warning,
            message: "test issue".into(),
        }],
    }
}

fn sample_apply_plan() -> ApplyPlan {
    let mut spec = ApplyCalibrationTableSpec::new("phase.gcal");
    spec.apply_to.field_ids = vec![0];
    spec.apply_to.spectral_window_ids = vec![3];
    spec.apply_to.observation_ids = vec![7];
    spec.gainfield = Some(GainFieldSelector::Nearest);
    spec.calwt = true;
    ApplyPlan {
        measurement_set_path: Some(PathBuf::from("dataset.ms")),
        apply_mode: ApplyMode::CalFlag,
        requires_corrected_data_column: true,
        selected_rows: Vec::new(),
        selected_row_count: 11,
        parang: true,
        selected_field_ids: vec![0, 1],
        selected_data_desc_ids: vec![2],
        selected_data_spw_ids: vec![3],
        measurement_set_spectral_windows: Vec::new(),
        calibration_tables: vec![ApplyCalibrationTablePlan {
            spec,
            applicable_selected_row_count: 9,
            summary: sample_summary("phase.gcal"),
            resolved_gainfield: Some(ResolvedGainField {
                selector: GainFieldSelector::Nearest,
                field_id: 4,
                field_name: Some("calibrator".into()),
            }),
            resolved_nearest_gainfields: vec![ResolvedNearestGainField {
                measurement_set_field_id: 0,
                measurement_set_field_name: Some("target".into()),
                calibration_field_id: 4,
                calibration_field_name: Some("calibrator".into()),
                angular_separation_rad: 0.012_345,
            }],
            spw_mapping: vec![ApplySpwMapping {
                data_spw_id: 3,
                calibration_spw_id: 1,
            }],
            calibration_spectral_windows: Vec::new(),
            interp: ApplyInterpolationMode::NearestLinear,
            calwt: true,
        }],
    }
}

fn sample_stats() -> CalibrationValueStats {
    CalibrationValueStats {
        npts: 10,
        flagged_npts: 2,
        total_npts: 12,
        sum: 42.0,
        sumsq: 256.0,
        min: 1.0,
        max: 9.0,
        mean: 4.2,
        median: 4.0,
        medabsdevmed: 1.5,
        q1: 2.0,
        q3: 6.0,
        quartile: 4.0,
        var: 2.5,
        stddev: 1.581_138_830_084_189_8,
        rms: 5.0,
    }
}

#[test]
fn render_summary_and_apply_outputs_include_key_sections() {
    let summary_text = render::render_summary_text(&[
        sample_summary("phase.gcal"),
        CalibrationTableSummary {
            issues: Vec::new(),
            ..sample_summary("bandpass.bcal")
        },
    ]);
    assert!(summary_text.contains("Calibration Table: phase.gcal"));
    assert!(summary_text.contains("subtable FIELD exists=true"));
    assert!(summary_text.contains("issues=none"));

    let plan = sample_apply_plan();
    let plan_text = render::render_apply_plan_text(&plan);
    assert!(plan_text.contains("requires_corrected_data_column=true"));
    assert!(plan_text.contains("apply_to=field=[0] spw=[3] obs=[7]"));
    assert!(plan_text.contains("gainfield[ms_field=0] -> 4"));
    assert!(plan_text.contains("spw 3 -> 1"));

    let report = ApplyExecutionReport {
        plan: plan.clone(),
        created_corrected_data_column: true,
        wrote_measurement_set: true,
        updated_row_count: 9,
        flagged_row_count: 2,
        flagged_sample_count: 5,
        flag_backup_version: Some("applycal_1".to_string()),
        timings: ApplyExecutionTimings {
            planning_ns: 1_500_000,
            planning_selection_ns: 500_000,
            planning_selected_rows_ns: 250_000,
            planning_measurement_set_spectral_windows_ns: 750_000,
            planning_calibration_table_plans_ns: 900_000,
            open_measurement_set_ns: 2_000_000,
            row_field_index_lookup_ns: 2_500,
            ensure_corrected_data_ns: 3_000,
            correlation_lookup_ns: 4_000,
            calibration_load_ns: 5_000,
            row_compute_ns: 6_000,
            row_writeback_ns: 7_000,
            save_ns: 8_000,
            total_ns: 3_500_000,
        },
    };
    let apply_text = render::render_apply_report_text(&report);
    assert!(apply_text.contains("Calibration Apply Report: dataset.ms"));
    assert!(apply_text.contains("created_corrected_data_column=true"));
    assert!(apply_text.contains("flag_backup=applycal_1"));
    assert!(apply_text.contains("1.500ms"));
    assert!(apply_text.contains("gainfield=4 Some(\"calibrator\")"));
}

#[test]
fn render_stats_and_solver_outputs_include_grouped_details() {
    let report = CalibrationStatsReport {
        path: PathBuf::from("phase.gcal"),
        axis: CalibrationStatsAxis::Amplitude,
        datacolumn: Some("CPARAM".into()),
        row_count: 12,
        global: sample_stats(),
        by_field_id: vec![CalibrationIndexedStats {
            key: 3,
            stats: sample_stats(),
        }],
        by_spectral_window_id: vec![CalibrationIndexedStats {
            key: 1,
            stats: sample_stats(),
        }],
        by_antenna1_id: vec![CalibrationIndexedStats {
            key: 0,
            stats: sample_stats(),
        }],
        by_observation_id: vec![CalibrationIndexedStats {
            key: 7,
            stats: sample_stats(),
        }],
    };
    let stats_text = render::render_stats_text(&report);
    assert!(stats_text.contains("Calibration Stats: phase.gcal"));
    assert!(stats_text.contains("grouped_by_field_id"));
    assert!(stats_text.contains("npts=10 flagged_npts=2 total_npts=12"));

    let gain_text = render::render_gain_solve_report_text(&GainSolveReport {
        output_table: PathBuf::from("phase.gcal"),
        gain_type: GainType::G,
        refant_antenna_id: 3,
        field_ids: vec![0, 1],
        spectral_window_ids: vec![2],
        solution_row_count: 24,
        selected_data: Default::default(),
        solve_stats: Default::default(),
    });
    assert!(gain_text.contains("Gain Solve Report: phase.gcal"));
    assert!(gain_text.contains("solution_rows=24"));

    let bandpass_text = render::render_bandpass_solve_report_text(&BandpassSolveReport {
        output_table: PathBuf::from("bandpass.bcal"),
        table_subtype: "B Jones".into(),
        refant_antenna_id: 4,
        field_ids: vec![0],
        spectral_window_ids: vec![1, 2],
        solution_row_count: 8,
        channel_count: 64,
        selected_data: Default::default(),
        solve_stats: Default::default(),
    });
    assert!(bandpass_text.contains("Bandpass Solve Report: bandpass.bcal"));
    assert!(bandpass_text.contains("channel_count=64"));

    let fluxscale_text = render::render_fluxscale_report_text(&FluxScaleReport {
        output_table: PathBuf::from("fluxscale.gcal"),
        spw_ids: vec![0],
        spw_names: vec!["SPW0".into()],
        frequencies_hz: vec![1.42e9],
        fields: BTreeMap::from([(
            5,
            FluxScaleFieldResult {
                field_name: "target".into(),
                spw_results: BTreeMap::from([(
                    0,
                    FluxScaleSpwResult {
                        fluxd: [1.0, 0.0, 0.0, 0.0],
                        fluxd_err: [0.1, 0.0, 0.0, 0.0],
                        num_sol: [4.0, 0.0, 0.0, 0.0],
                    },
                )]),
                fit_ref_frequency_hz: 1.42e9,
                fit_fluxd: 1.0,
                fit_fluxd_err: 0.1,
                spidx: vec![0.0],
                spidx_err: vec![0.0],
                covar_mat: vec![vec![1.0]],
            },
        )]),
    });
    assert!(fluxscale_text.contains("FluxScale Report: fluxscale.gcal"));
    assert!(fluxscale_text.contains("field 5 (target)"));
    assert!(fluxscale_text.contains("fluxd=[1.0, 0.0, 0.0, 0.0]"));
}
