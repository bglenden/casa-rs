// SPDX-License-Identifier: LGPL-3.0-or-later
//! The CASA-named calibration surfaces read CASA 6.7.6's values as CASA's
//! task scripts read them (#931).

use std::path::{Path, PathBuf};

use casa_ms::MsSelection;
use casa_provider_contracts::{ParameterValue, TaskFailureKind, builtin_surface_bundle};
use casa_task_runtime::{
    OpenSessionRequest, ParameterRuntime, ResolutionPatch, TaskFailure, TaskInvocation,
};
use serde_json::{Value, json};

use super::read;
use crate::{
    ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode, BandpassType,
    CalibrationTaskRequest, ExecuteApplyTaskRequest, FluxScaleRequest, GainFieldSelector,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainType,
    GencalTaskRequest, GencalType, RefAntSelector,
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
    bandpass.gainfield = Some(GainFieldSelector::FieldId(0));
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
fn applycal_takes_one_calwt_for_every_table_and_refuses_a_short_list() {
    let tables = json!(["phase.gcal", "bandpass.bcal", "flux.gcal"]);
    let request = apply(json!({"vis": "d.ms", "gaintable": tables, "calwt": [false]}));
    assert!(request.calibration_tables.iter().all(|table| !table.calwt));
    // CASA's default is [True].
    let request = apply(json!({"vis": "d.ms", "gaintable": tables}));
    assert!(request.calibration_tables.iter().all(|table| table.calwt));
    let message = refused(
        casa(
            "applycal",
            json!({"vis": "d.ms", "gaintable": tables, "calwt": [true, false]}),
        ),
        "calwt",
    );
    assert!(message.contains("2 values for 3 gain tables"), "{message}");
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
        ("trial", ApplyMode::Trial),
    ] {
        let CalibrationTaskRequest::ExecuteApply(apply) = mode(value).expect(value) else {
            panic!("an apply");
        };
        assert_eq!(apply.apply_mode, expected, "{value}");
    }
    for value in ["calflagstrict", "flagonly", "flagonlystrict"] {
        assert!(
            refused(mode(value), "applymode").contains("#979"),
            "{value}"
        );
    }
}

#[test]
fn applycal_with_nothing_to_apply_is_refused() {
    let message = refused(casa("applycal", json!({"vis": "d.ms"})), "gaintable");
    assert!(message.contains("nothing to apply"), "{message}");
    let message = refused(
        casa("applycal", json!({"vis": "d.ms", "gaintable": [""]})),
        "gaintable",
    );
    assert!(message.contains("nothing to apply"), "{message}");
    // The parallactic-angle correction alone is not implemented yet.
    let message = refused(
        casa("applycal", json!({"vis": "d.ms", "parang": true})),
        "parang",
    );
    assert!(message.contains("#979"), "{message}");
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
        dir.path().join("phase.gcal")
    );
    let message = refused(
        casa("applycal", json!({"vis": "d.ms", "docallib": true})),
        "callib",
    );
    assert!(message.contains("docallib requires callib"), "{message}");
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
            "antenna": "A0",
            "scan": "2",
        }))
        .selection
    };
    let all = MsSelection {
        field: Some("1".into()),
        spw: Some("0".into()),
        ..MsSelection::new()
    };
    assert_eq!(selection(false), all);
    assert_eq!(
        selection(true),
        MsSelection {
            antenna: Some("A0".into()),
            scan: Some("2".into()),
            ..all
        }
    );
}

fn gain(values: Value) -> crate::SolveGainTaskRequest {
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
        "solint": "30s",
        "combine": "scan,field",
        "refant": "VA15",
        "minblperant": 2,
        "minsnr": 2.5,
        "solnorm": true,
        "gaintype": "T",
        "smodel": [2.5, 0, 0, 0],
        "calmode": "p",
        "gaintable": ["prior.gcal"],
        "gainfield": ["1"],
        "parang": true,
    }));
    let mut prior = table("prior.gcal", true);
    prior.gainfield = Some(GainFieldSelector::FieldId(1));
    assert_eq!(
        solve,
        crate::SolveGainTaskRequest {
            measurement_set: PathBuf::from("dataset.ms"),
            selection: MsSelection {
                field: Some("0".into()),
                ..MsSelection::new()
            },
            output_table: PathBuf::from("phase.gcal"),
            gain_type: GainType::T,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Seconds(30.0),
            combine: GainSolveCombine {
                scans: true,
                fields: true,
            },
            refant: RefAntSelector::AntennaName("VA15".into()),
            prior_calibration_tables: vec![prior],
            parang: true,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: true,
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
    for (name, value) in [
        ("gaintype", json!("K")),
        ("gaintype", json!("GSPLINE")),
        ("calmode", json!("a")),
        ("combine", json!("spw")),
        ("combine", json!("scan,obs")),
        ("combine", json!("Scan,SPW")),
        ("solint", json!("inf,2ch")),
        ("refant", json!("A0,A1")),
        // CASA's default: a solve with no reference antenna.
        ("refant", json!("")),
        ("smodel", json!([1, 0.1, 0, 0])),
        ("interp", json!(["cubic"])),
    ] {
        let mut values = base.clone();
        values[name] = value.clone();
        if name == "interp" {
            values["gaintable"] = json!(["prior.gcal"]);
        }
        let message = refused(casa("gaincal", values), name);
        assert!(message.contains("#979"), "{name}={value}: {message}");
    }
    for solint in ["nan", "1Jy", "soon"] {
        let message = refused(with("solint", json!(solint)), "solint");
        assert!(message.contains("not valid"), "{solint}: {message}");
    }
    assert!(refused(with("smodel", json!([1, 0])), "smodel").contains("[I, Q, U, V]"));
    // CASA's numeric solint and observation are refused as the wrong type,
    // naming the issue.
    for (name, value) in [("solint", json!(30)), ("observation", json!(0))] {
        let bundle = builtin_surface_bundle("gaincal").expect("gaincal");
        let mut request = OpenSessionRequest::defaults(bundle, ".");
        request.override_patch = ResolutionPatch {
            values: [(
                name.to_string(),
                ParameterValue::from_plain_json(&value).expect("a value"),
            )]
            .into(),
            ..ResolutionPatch::default()
        };
        request.managed_save = false;
        let message = ParameterRuntime::default()
            .open_session(request)
            .expect_err("the wrong type")
            .to_string();
        assert!(message.contains("#979"), "{name}: {message}");
    }
}

#[test]
fn gaincal_reads_solint_and_combine_as_casa_does() {
    // SolvableVisCal::setSolve: text containing inf or int, in any case;
    // otherwise a time, in seconds without a unit; zero is inf and a
    // negative time int.
    let base = json!({"vis": "d.ms", "caltable": "g.gcal", "refant": "0", "smodel": [1, 0, 0, 0]});
    let read = |name: &str, value: &str| {
        let mut values = base.clone();
        values[name] = json!(value);
        gain(values)
    };
    for (solint, interval) in [
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
            GainSolveCombine { scans, fields },
            "{combine}"
        );
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
        }),
    )
    .expect("bandpass") else {
        panic!("a bandpass solve");
    };
    assert_eq!(solve.refant, RefAntSelector::AntennaId(7));
    assert_eq!(solve.band_type, BandpassType::BPoly);
    assert!(!solve.combine.scans && !solve.combine.fields);
    assert_eq!((solve.amplitude_degree, solve.phase_degree), (5, 4));
    assert_eq!(solve.smodel, [3.0, 0.0, 0.0, 0.0]);
    assert_eq!(solve.prior_calibration_tables, [table("prior.gcal", true)]);
    // CASA's bandpass combines scans by default.
    let CalibrationTaskRequest::SolveBandpass(defaults) = casa(
        "bandpass",
        json!({"vis": "d.ms", "caltable": "b.bcal", "refant": "0", "smodel": [1, 0, 0, 0]}),
    )
    .expect("bandpass") else {
        panic!("a bandpass solve");
    };
    assert!(defaults.combine.scans && !defaults.combine.fields);
    assert_eq!(defaults.band_type, BandpassType::B);
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
            input_table: PathBuf::from("gain.gcal"),
            output_table: PathBuf::from("flux.gcal"),
            reference_fields: vec!["1331+305".into()],
            transfer_fields: vec!["1445+099".into(), "0137+331".into(), "2253+161".into()],
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
            measurement_set: PathBuf::from("tutorial.ms"),
            output_table: PathBuf::from("cal.tau"),
            caltype: GencalType::Opac,
            antenna: String::new(),
            spw: "0,1".into(),
            parameter: vec![0.1, 0.2],
            gaincurve_table: None,
        })
    );
    let caltype = |value: &str| {
        casa(
            "gencal",
            json!({"vis": "t.ms", "caltable": "c.cal", "caltype": value}),
        )
    };
    assert!(refused(caltype(""), "caltype").contains("requires a caltype"));
    // CASA's gc is the gain curve alone; casa-rs's gceff always folds in
    // the efficiency.
    for value in ["amp", "gc", "eff", "tsys"] {
        assert!(
            refused(caltype(value), "caltype").contains("#979"),
            "{value}"
        );
    }
}

#[test]
fn gencal_refuses_the_antenna_and_opacity_forms_casa_reads_otherwise() {
    let gencal = |values: Value| {
        let mut base = json!({"vis": "t.ms", "caltable": "c.cal"});
        base.as_object_mut()
            .unwrap()
            .extend(values.as_object().unwrap().clone());
        casa("gencal", base)
    };
    // CASA looks antenna position corrections up when no antenna is named,
    // and writes an opacity for the named antennas alone.
    for values in [
        json!({"caltype": "antpos", "parameter": [0.01, 0, 0]}),
        json!({"caltype": "opac", "antenna": "VA01", "parameter": [0.05]}),
    ] {
        assert!(refused(gencal(values), "antenna").contains("#979"));
    }
    // With no window named, CASA takes one opacity, which holds for all.
    let message = refused(
        gencal(json!({"caltype": "opac", "parameter": [0.05, 0.06]})),
        "parameter",
    );
    assert!(message.contains("one opacity"), "{message}");
    assert!(gencal(json!({"caltype": "opac", "parameter": [0.05]})).is_ok());
}
