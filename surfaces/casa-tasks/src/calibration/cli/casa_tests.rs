// SPDX-License-Identifier: LGPL-3.0-or-later
//! The CASA-named calibration surfaces read CASA 6.7.6's values as CASA's
//! task scripts read them (#931).

use std::path::{Path, PathBuf};

use crate::{OpenSessionRequest, ParameterRuntime, ResolutionPatch, TaskFailure, TaskInvocation};
use casa_contracts::{ParameterValue, TaskFailureKind, builtin_surface_bundle};
use casa_ms::MsSelection;
use serde_json::{Value, json};

use super::read;
use crate::calibration::{
    CalibrationTaskRequest, ExecuteApplyTaskRequest, GencalTaskRequest, SolveGainTaskRequest,
};
use casa_calibration::{
    apply::{
        ApplyArgumentError, ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode,
        GainFieldSelector,
    },
    bandpass::{BandpassSolveCuts, BandpassType},
    fluxscale::FluxScaleRequest,
    gencal::{GencalRequest, GencalType},
    solve::{
        GainNormalization, GainSolveCombine, GainSolveInterval, GainSolveMode,
        GainSolveModelSource, GainSplineControls, GainType, RefAntSelector,
    },
};

/// The request `surface` makes for the plain-JSON `values`, set as a
/// `--json-run` request sets them.
fn casa(surface: &str, values: Value) -> Result<CalibrationTaskRequest, TaskFailure> {
    let bundle = builtin_surface_bundle(surface).expect("calibration surface");
    let mut request = OpenSessionRequest::defaults(bundle, ".");
    request.override_patch = ResolutionPatch {
        values: values
            .as_object()
            .expect("values")
            .iter()
            .map(|(name, value)| {
                let value = ParameterValue::from_plain_json(value).expect("a value");
                (name.clone(), value)
            })
            .collect(),
        ..ResolutionPatch::default()
    };
    request.managed_save = false;
    let session = ParameterRuntime::default()
        .open_session(request)
        .expect("session");
    read::task_request(&TaskInvocation::new(&session, Path::new(".")))
}

fn refused(result: Result<CalibrationTaskRequest, TaskFailure>, parameter: &str) -> String {
    let failure = result.expect_err("a refusal");
    assert_eq!(failure.kind, TaskFailureKind::Validation, "{failure:?}");
    assert_eq!(failure.parameter.as_deref(), Some(parameter), "{failure:?}");
    failure.message
}

fn apply(values: Value) -> ExecuteApplyTaskRequest {
    match casa("applycal", values).expect("applycal") {
        CalibrationTaskRequest::ExecuteApply(apply) => apply,
        other => panic!("{other:?}"),
    }
}

fn table(path: &str, calwt: bool) -> ApplyCalibrationTableSpec {
    let mut spec = ApplyCalibrationTableSpec::new(path);
    spec.calwt = calwt;
    spec
}

#[test]
fn applycal_lists_are_positional_and_short_lists_take_casas_defaults() {
    let request = apply(json!({
        "vis": "dataset.ms",
        "gaintable": ["phase.gcal", "", "bandpass.bcal", "flux.gcal"],
        "gainfield": ["nearest", "", "0"],
        "interp": ["", "", "nearest,linear"],
        "spwmap": [[0, 0], [], [-1]],
        "calwt": [true, true, false, true],
    }));
    let mut phase = table("phase.gcal", true);
    phase.gainfield = Some(GainFieldSelector::Nearest);
    phase.spwmap = vec![0, 0];
    let mut bandpass = table("bandpass.bcal", false);
    bandpass.gainfield = Some(GainFieldSelector::Expression("0".into()));
    bandpass.interp = ApplyInterpolationMode::NearestLinear;
    // The empty entry is skipped, its list positions with it; flux.gcal,
    // past every list but calwt, takes CASA's defaults.
    assert_eq!(
        request.calibration_tables,
        [phase, bandpass, table("flux.gcal", true)]
    );
    assert_eq!(request.apply_mode, ApplyMode::CalFlag);
    assert!(request.flagbackup);
}

#[test]
fn applycal_broadcasts_one_calwt_and_defers_missing_entries_until_setup() {
    let tables = json!(["phase.gcal", "bandpass.bcal", "flux.gcal"]);
    let request = apply(json!({"vis": "d.ms", "gaintable": tables, "calwt": [false]}));
    assert!(request.calibration_tables.iter().all(|table| !table.calwt));
    // CASA's default is [True].
    let request = apply(json!({"vis": "d.ms", "gaintable": tables}));
    assert!(request.calibration_tables.iter().all(|table| table.calwt));
    let request = apply(json!({"vis": "d.ms", "gaintable": tables, "calwt": [true, false]}));
    assert_eq!(
        request.argument_error,
        Some(ApplyArgumentError::MissingCalwt)
    );
    assert_eq!(request.calibration_tables.len(), 2);
}

#[test]
fn a_flat_spwmap_is_the_first_tables_map() {
    let request = apply(json!({
        "vis": "d.ms",
        "gaintable": ["phase.gcal", "bandpass.bcal"],
        "spwmap": [0, 0, 1],
    }));
    assert_eq!(request.calibration_tables[0].spwmap, [0, 0, 1]);
    assert!(request.calibration_tables[1].spwmap.is_empty());
}

#[test]
fn applymode_takes_casas_values() {
    let mode = |value: &str| {
        casa(
            "applycal",
            json!({"vis": "d.ms", "gaintable": ["g.cal"], "applymode": value}),
        )
    };
    for (value, expected) in [
        ("", ApplyMode::CalFlag),
        ("calflag", ApplyMode::CalFlag),
        ("calonly", ApplyMode::CalOnly),
        ("calflagstrict", ApplyMode::CalFlagStrict),
        ("flagonly", ApplyMode::FlagOnly),
        ("flagonlystrict", ApplyMode::FlagOnlyStrict),
        ("trial", ApplyMode::Trial),
    ] {
        let CalibrationTaskRequest::ExecuteApply(apply) = mode(value).expect(value) else {
            panic!("an apply");
        };
        assert_eq!(apply.apply_mode, expected, "{value}");
    }
}

#[test]
fn applycal_with_nothing_to_apply_still_reaches_ms_setup() {
    for gaintable in [json!([]), json!([""])] {
        let request = apply(json!({"vis": "d.ms", "gaintable": gaintable}));
        assert!(request.calibration_tables.is_empty());
        assert!(request.argument_error.is_none());
    }
    // PJones is a complete apply term even without a supplied table.
    let request = apply(json!({"vis": "d.ms", "parang": true}));
    assert!(request.parang);
    assert!(request.calibration_tables.is_empty());
}

#[test]
fn docallib_chooses_the_calibration_library() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let callib = dir.path().join("apply.callib");
    std::fs::write(&callib, "caltable='phase.gcal' calwt=F tinterp='nearest'\n").expect("write");
    let callib = callib.to_str().expect("utf8");
    // Without docallib, callib is not read: CASA applies gaintable.
    let request = apply(json!({"vis": "d.ms", "callib": callib, "gaintable": ["g.cal"]}));
    assert_eq!(request.calibration_tables, [table("g.cal", true)]);
    let request = apply(json!({
        "vis": "d.ms", "docallib": true, "callib": callib, "gaintable": ["g.cal"],
    }));
    assert_eq!(request.calibration_tables.len(), 1);
    assert_eq!(
        request.calibration_tables[0].path,
        std::env::current_dir().unwrap().join("phase.gcal")
    );
    // Public CASA opens the MS and selects/backs up before a missing library
    // raises RuntimeError. Preserve the diagnostic until the execution stage.
    let request = apply(json!({"vis": "d.ms", "docallib": true}));
    assert!(request.calibration_tables.is_empty());
    assert!(
        request
            .callibrary_error
            .as_deref()
            .unwrap()
            .contains("failed to read callibrary")
    );
}

#[test]
fn applycal_selectors_are_scalar_text_and_only_empty_text_is_absent() {
    let defaults = apply(json!({"vis":"d.ms", "gaintable":["g.cal"]}));
    assert!(defaults.selection.intent.is_none());
    assert!(defaults.selection.uvrange.is_none());
    for text in ["none", "auto", " CALIBRATE_PHASE* ", "/.*$/"] {
        let request = apply(json!({
            "vis":"d.ms", "gaintable":["g.cal"], "intent":text, "uvrange":text
        }));
        assert_eq!(request.selection.intent.as_deref(), Some(text));
        assert_eq!(request.selection.uvrange.as_deref(), Some(text));
    }
}

#[test]
fn selectdata_false_keeps_field_and_spw_and_drops_the_other_selectors() {
    let selection = |selectdata: bool| {
        apply(json!({
            "vis": "d.ms",
            "gaintable": ["g.cal"],
            "selectdata": selectdata,
            "field": "1",
            "spw": "0",
            "intent": "CALIBRATE_PHASE*",
            "uvrange": "<50m",
            "antenna": "A0",
            "scan": "2",
        }))
        .selection
    };
    let all = MsSelection {
        field: Some("1".into()),
        spw: Some("0".into()),
        intent: Some("CALIBRATE_PHASE*".into()),
        ..MsSelection::new()
    };
    assert_eq!(selection(false), all);
    assert_eq!(
        selection(true),
        MsSelection {
            antenna: Some("A0".into()),
            scan: Some("2".into()),
            uvrange: Some("<50m".into()),
            ..all
        }
    );
}

fn gain(values: Value) -> SolveGainTaskRequest {
    match casa("gaincal", values).expect("gaincal") {
        CalibrationTaskRequest::SolveGain(solve) => solve,
        other => panic!("{other:?}"),
    }
}

#[test]
fn gaincal_reads_casas_values() {
    let solve = gain(json!({
        "vis": "dataset.ms",
        "caltable": "phase.gcal",
        "field": "0",
        "solint": 30,
        "observation": 0,
        "uvrange": ">20m",
        "intent": "*PHASE*",
        "preavg": 12.5,
        "combine": "scan,field",
        "refant": "VA15",
        "refantmode": "strict",
        "minblperant": 2,
        "minsnr": 2.5,
        "solnorm": true,
        "normtype": "median",
        "gaintype": "T",
        "smodel": [2.5, 0, 0, 0],
        "calmode": "a",
        "gaintable": ["prior.gcal"],
        "gainfield": ["1"],
        "parang": true,
    }));
    let mut prior = table("prior.gcal", true);
    prior.gainfield = Some(GainFieldSelector::Expression("1".into()));
    assert_eq!(
        solve,
        SolveGainTaskRequest {
            append: false,
            measurement_set: PathBuf::from("dataset.ms"),
            selection: MsSelection {
                field: Some("0".into()),
                observation: Some("0".into()),
                uvrange: Some(">20m".into()),
                intent: Some("*PHASE*".into()),
                msselect: Some("ANTENNA1!=ANTENNA2".into()),
                ..MsSelection::new()
            },
            output_table: PathBuf::from("phase.gcal"),
            gain_type: GainType::T,
            spline_controls: None,
            fringe_controls: None,
            solve_mode: GainSolveMode::Amplitude,
            solve_interval: GainSolveInterval::Seconds(30.0),
            combine: GainSolveCombine {
                scans: true,
                fields: true,
                ..GainSolveCombine::default()
            },
            refant: RefAntSelector::Expression {
                selection: "VA15".into(),
                strict: true,
            },
            prior_calibration_tables: vec![prior],
            parang: true,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: true,
            preaverage_seconds: 12.5,
            correlation_dependent_flags: false,
            normalization_type: GainNormalization::Median,
            robustness: Default::default(),
            min_snr: 2.5,
            min_baselines_per_antenna: 2,
            smodel: [2.5, 0.0, 0.0, 0.0],
        }
    );
}

#[test]
fn gaincal_refuses_the_casa_values_it_does_not_implement_yet() {
    let base = json!({"vis": "d.ms", "caltable": "g.gcal", "refant": "0", "smodel": [1, 0, 0, 0]});
    let with = |name: &str, value: Value| {
        let mut values = base.clone();
        values[name] = value;
        casa("gaincal", values)
    };
    let spline = gain(json!({
        "vis": "d.ms", "caltable": "spline.gcal", "gaintype": "GSPLINE",
        "splinetime": 60.0, "npointaver": 5, "phasewrap": 150.0, "smodel": [1, 0, 0, 0],
    }));
    assert_eq!(spline.gain_type, GainType::GSpline);
    assert_eq!(
        spline.spline_controls,
        Some(GainSplineControls {
            splinetime: 60.,
            npointaver: 5,
            phasewrap: 150.
        })
    );
    for solint in ["nan", "1Jy", "soon"] {
        let message = refused(with("solint", json!(solint)), "solint");
        assert!(message.contains("not valid"), "{solint}: {message}");
    }
    // CASA 6.7.6.14's gaincal aborts on more than four Stokes values.
    assert!(refused(with("smodel", json!([1, 0, 0, 0, 0])), "smodel").contains("[I, Q, U, V]"));
    // Short models zero-fill omitted Stokes values.
    assert_eq!(
        gain(json!({"vis": "d.ms", "caltable": "g.gcal", "smodel": [1, 0.1]})).smodel,
        [1.0, 0.1, 0.0, 0.0]
    );
}

#[test]
fn gaincal_reads_solint_and_combine_as_casa_does() {
    // SolvableVisCal::setSolve: empty text, or text containing inf or int,
    // in any case; otherwise a time, in seconds without a unit; zero is inf
    // and a negative time int.
    let base = json!({"vis": "d.ms", "caltable": "g.gcal", "refant": "0", "smodel": [1, 0, 0, 0]});
    let read = |name: &str, value: &str| {
        let mut values = base.clone();
        values[name] = json!(value);
        gain(values)
    };
    assert_eq!(read("refant", "").refant, RefAntSelector::Unspecified);
    for (solint, interval) in [
        ("", GainSolveInterval::Infinite),
        ("inf,2ch", GainSolveInterval::Infinite),
        ("int,5MHz", GainSolveInterval::Integration),
        ("30s,unused", GainSolveInterval::Seconds(30.0)),
        ("INF", GainSolveInterval::Infinite),
        ("Int", GainSolveInterval::Integration),
        ("1min", GainSolveInterval::Seconds(60.0)),
        ("0.5h", GainSolveInterval::Seconds(1800.0)),
        ("45", GainSolveInterval::Seconds(45.0)),
        ("0s", GainSolveInterval::Infinite),
        ("-1s", GainSolveInterval::Integration),
    ] {
        assert_eq!(read("solint", solint).solve_interval, interval, "{solint}");
    }
    // combine names an axis anywhere in its text, in any case; text naming
    // none combines nothing.
    for (combine, scans, fields) in [
        ("Scan,Field", true, true),
        ("scan field", true, true),
        ("FIELD", false, true),
        ("time", false, false),
    ] {
        assert_eq!(
            read("combine", combine).combine,
            GainSolveCombine {
                scans,
                fields,
                ..GainSolveCombine::default()
            },
            "{combine}"
        );
    }
}

#[test]
fn a_short_smodel_is_a_point_source_with_the_missing_stokes_zero() {
    // As CASA 6.7.6.14 solves them (probed): [2], [2, 0] and [2, 0, 0]
    // give gaincal's and bandpass's [2, 0, 0, 0] solutions.
    for smodel in [json!([2]), json!([2, 0]), json!([2, 0, 0])] {
        let values = json!({"vis": "d.ms", "caltable": "g.gcal", "refant": "0", "smodel": smodel});
        let solve = gain(values.clone());
        assert_eq!(solve.model_source, GainSolveModelSource::PointSource);
        assert_eq!(solve.smodel, [2.0, 0.0, 0.0, 0.0], "{smodel}");
        let Ok(CalibrationTaskRequest::SolveBandpass(solve)) = casa("bandpass", values) else {
            panic!("a bandpass solve");
        };
        assert_eq!(solve.smodel, [2.0, 0.0, 0.0, 0.0], "{smodel}");
    }
}

#[test]
fn an_empty_smodel_needs_the_measurement_set_to_choose_the_model() {
    // CASA's default smodel=[] uses MODEL_DATA where the MeasurementSet has
    // it; the MeasurementSet is opened to find out.
    let message = refused(
        casa(
            "gaincal",
            json!({"vis": "missing.ms", "caltable": "g.gcal", "refant": "0"}),
        ),
        "vis",
    );
    assert!(message.contains("missing.ms"), "{message}");
}

#[test]
fn bandpass_reads_casas_values() {
    let CalibrationTaskRequest::SolveBandpass(solve) = casa(
        "bandpass",
        json!({
            "vis": "dataset.ms",
            "caltable": "bandpass.bcal",
            "refant": "7",
            "combine": "",
            "bandtype": "BPOLY",
            "degamp": 5,
            "degphase": 4,
            "solnorm": true,
            "smodel": [3, 0, 0, 0],
            "gaintable": "prior.gcal",
            "minblperant": 2,
            "minsnr": 1.5,
            "maskedge": 10,
        }),
    )
    .expect("bandpass") else {
        panic!("a bandpass solve");
    };
    assert_eq!(
        solve.refant,
        RefAntSelector::Expression {
            selection: "7".to_string(),
            strict: false,
        }
    );
    assert_eq!(solve.band_type, BandpassType::BPoly);
    assert!(!solve.combine.scans && !solve.combine.fields);
    assert_eq!((solve.amplitude_degree, solve.phase_degree), (5, 4));
    assert_eq!(solve.smodel, [3.0, 0.0, 0.0, 0.0]);
    assert_eq!(solve.prior_calibration_tables, [table("prior.gcal", true)]);
    assert_eq!(
        solve.cuts,
        Some(BandpassSolveCuts {
            min_baselines_per_antenna: 2,
            min_snr: 1.5,
            fill_gaps: 0,
            mask_edge_percent: 10.0,
            mask_center_channels: 0,
            normalize_visibilities: false,
            omit_polynomial: [false; 2],
        })
    );
    // CASA's bandpass combines scans by default, and cuts as CASA does.
    let defaults = json!({"vis": "d.ms", "caltable": "b.bcal", "refant": "0",
                          "smodel": [1, 0, 0, 0]});
    let CalibrationTaskRequest::SolveBandpass(solve) =
        casa("bandpass", defaults.clone()).expect("bandpass")
    else {
        panic!("a bandpass solve");
    };
    assert!(solve.combine.scans && !solve.combine.fields);
    assert_eq!(solve.band_type, BandpassType::B);
    assert_eq!(solve.cuts, Some(BandpassSolveCuts::CASA_DEFAULTS));
    // For an even channel count an edge mask of 50 leaves the center.
    let mut values = defaults;
    values["bandtype"] = json!("BPOLY");
    values["maskedge"] = json!(50);
    let CalibrationTaskRequest::SolveBandpass(solve) = casa("bandpass", values).expect("bandpass")
    else {
        panic!("a bandpass solve");
    };
    assert_eq!(solve.cuts.unwrap().mask_edge_percent, 50.0);
}

#[test]
fn fluxscale_reads_casas_values() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let vis = dir.path().join("dataset.ms");
    std::fs::create_dir(&vis).expect("vis");
    let vis = vis.to_str().expect("utf8");
    let request = |values: Value| match casa("fluxscale", values) {
        Ok(CalibrationTaskRequest::FluxScale(request)) => request,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        request(json!({
            "vis": vis,
            "caltable": "gain.gcal",
            "fluxtable": "flux.gcal",
            "reference": ["1331+305"],
            "transfer": ["1445+099", "0137+331,2253+161"],
            "refspwmap": [0, 0],
            "gainthreshold": 0.2,
            "incremental": true,
        })),
        FluxScaleRequest {
            options: casa_calibration::fluxscale::FluxScaleOptions {
                measurement_set: Some(PathBuf::from(vis)),
                ..Default::default()
            },
            append: false,
            input_table: PathBuf::from("gain.gcal"),
            output_table: PathBuf::from("flux.gcal"),
            reference_fields: vec!["1331+305".into()],
            transfer_fields: vec!["1445+099".into(), "0137+331,2253+161".into()],
            refspwmap: vec![0, 0],
            gainthreshold: Some(0.2),
            incremental: true,
        }
    );
    // CASA's refspwmap=[-1] and gainthreshold=-1 are none.
    let defaults = request(json!({
        "vis": vis, "caltable": "g.gcal", "fluxtable": "f.gcal", "reference": "3C286",
    }));
    assert!(defaults.refspwmap.is_empty());
    assert_eq!(defaults.gainthreshold, None);
    assert!(defaults.transfer_fields.is_empty());

    let message = refused(
        casa(
            "fluxscale",
            json!({"vis": vis, "caltable": "g.gcal", "fluxtable": "f.gcal"}),
        ),
        "reference",
    );
    assert!(
        message.contains("at least one reference field"),
        "{message}"
    );
    let missing = dir.path().join("missing.ms");
    let message = refused(
        casa(
            "fluxscale",
            json!({
                "vis": missing.to_str().unwrap(),
                "caltable": "g.gcal", "fluxtable": "f.gcal", "reference": "3C286",
            }),
        ),
        "vis",
    );
    assert!(message.contains("does not exist"), "{message}");
}

#[test]
fn gencal_reads_casas_values() {
    assert_eq!(
        casa(
            "gencal",
            json!({
                "vis": "tutorial.ms",
                "caltable": "cal.tau",
                "caltype": "opac",
                "spw": "0,1",
                "parameter": [0.1, 0.2],
            }),
        )
        .expect("gencal"),
        CalibrationTaskRequest::Gencal(GencalTaskRequest {
            prior: GencalRequest {
                measurement_set: PathBuf::from("tutorial.ms"),
                output_table: PathBuf::from("cal.tau"),
                caltype: GencalType::Opac,
                antenna: String::new(),
                spw: "0,1".into(),
                parameter: vec![0.1, 0.2],
                gaincurve_table: None,
            },
            polarization: String::new(),
            gaincurve_infile: None,
            antpos_infile: None,
            tec_image: None,
            eop_infile: None,
            ant_pos_time_limit: None,
            uniform: None,
        })
    );
    let caltype = |value: &str| {
        casa(
            "gencal",
            json!({"vis": "t.ms", "caltable": "c.cal", "caltype": value}),
        )
    };
    assert!(refused(caltype(""), "caltype").contains("requires a caltype"));
    assert!(caltype("tsys").is_ok());
    assert!(caltype("antposvla").is_ok());
    // EOP generation is implemented; retain the parsed task contract check.
    let CalibrationTaskRequest::Gencal(eop) = caltype("eop").expect("EOP request") else {
        panic!("expected gencal request");
    };
    assert_eq!(eop.prior.caltype, GencalType::Eop);
    assert_eq!(eop.eop_infile, None);
}

#[test]
fn gencal_keeps_the_gaincurve_input_key_on_the_wire() {
    for (caltype, key) in [("gc", "gaincurve_infile"), ("antpos", "antpos_infile")] {
        let CalibrationTaskRequest::Gencal(request) = casa(
            "gencal",
            json!({"vis": "t.ms", "caltable": "c.cal", "caltype": caltype, "infile": "prior"}),
        )
        .unwrap() else {
            panic!("expected gencal request");
        };
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(encoded[key], "prior");
        assert!(encoded.get("infile").is_none());
        assert_eq!(
            serde_json::from_value::<GencalTaskRequest>(encoded).unwrap(),
            request
        );
    }
}

#[test]
fn gencal_selects_opacity_antennas_and_reads_automatic_antpos_lookup() {
    let gencal = |values: Value| {
        let mut base = json!({"vis": "t.ms", "caltable": "c.cal"});
        base.as_object_mut()
            .unwrap()
            .extend(values.as_object().unwrap().clone());
        casa("gencal", base)
    };
    let CalibrationTaskRequest::Gencal(request) =
        gencal(json!({"caltype": "antpos", "ant_pos_time_limit": 42})).unwrap()
    else {
        panic!("expected gencal request");
    };
    assert!(request.prior.antenna.is_empty());
    assert_eq!(request.ant_pos_time_limit, Some(42));
    assert!(gencal(json!({"caltype":"opac","antenna":"VA01","parameter":[0.05]})).is_ok());
    assert!(
        gencal(json!({"caltype":"opac","antenna":"VA02,VA01","pol":"R",
                          "parameter":[0.05,0.06]}))
        .is_ok()
    );
    assert!(gencal(json!({"caltype": "opac", "parameter": [0.05]})).is_ok());
}
