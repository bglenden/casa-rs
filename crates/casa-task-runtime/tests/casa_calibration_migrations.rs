// SPDX-License-Identifier: LGPL-3.0-or-later

//! Profiles saved before the calibration surfaces took CASA 6.7.6's names
//! and values (#931 PR 4d) load as the CASA runs they meant: the old
//! values migrate, the per-table text lists become CASA's positional
//! lists, and an omitted value adopts CASA's default with a warning
//! (ADR-0006).

use std::collections::BTreeMap;

use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{DiagnosticCode, ResolvedProfile, parse_profile, resolve_profile};

/// `parameters`, a contract-5 profile of `surface`, as it loads today.
fn load(surface: &str, parameters: &str) -> ResolvedProfile {
    let bundle = builtin_surface_bundle(surface).unwrap();
    let profile = parse_profile(&format!(
        "[casars]\nformat = 1\nsurface = \"{surface}\"\nkind = \"task\"\ncontract = 5\n\n\
         [parameters]\n{parameters}"
    ))
    .unwrap();
    resolve_profile(&profile, &bundle).unwrap()
}

fn text(value: &str) -> ParameterValue {
    ParameterValue::String(value.to_string())
}

fn texts(values: &[&str]) -> ParameterValue {
    ParameterValue::Array(values.iter().map(|value| text(value)).collect())
}

fn integers(values: &[i64]) -> ParameterValue {
    ParameterValue::Array(
        values
            .iter()
            .copied()
            .map(ParameterValue::Integer)
            .collect(),
    )
}

fn floats(values: &[f64]) -> ParameterValue {
    ParameterValue::Array(values.iter().copied().map(ParameterValue::Float).collect())
}

fn flags(values: &[bool]) -> ParameterValue {
    ParameterValue::Array(values.iter().copied().map(ParameterValue::Bool).collect())
}

fn lists(values: &[&[i64]]) -> ParameterValue {
    ParameterValue::Array(values.iter().map(|list| integers(list)).collect())
}

/// The values `resolved` holds for `names`.
fn values(resolved: &ResolvedProfile, names: &[&str]) -> BTreeMap<String, ParameterValue> {
    names
        .iter()
        .map(|name| (name.to_string(), resolved.values[*name].clone()))
        .collect()
}

fn expected(entries: &[(&str, ParameterValue)]) -> BTreeMap<String, ParameterValue> {
    entries
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

/// The parameters whose defaults changed under `resolved`, as it warns.
fn changed_defaults(resolved: &ResolvedProfile) -> Vec<String> {
    resolved
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.code == DiagnosticCode::DefaultChanged)
        .filter_map(|diagnostic| diagnostic.parameter.clone())
        .collect()
}

#[test]
fn applycal_chain_text_lists_become_casas_positional_lists() {
    let resolved = load(
        "applycal",
        "vis = \"in.ms\"\ngaintables = [\"phase.gcal\", \"bandpass.bcal\"]\n\
         gainfield = \"nearest;0\"\ninterp = \"linear;nearest,linear\"\nspwmap = \";0,0\"\n\
         calwt = \"true,false\"\napply_mode = \"trial\"\n",
    );
    assert_eq!(
        values(
            &resolved,
            &[
                "gaintable",
                "gainfield",
                "interp",
                "spwmap",
                "calwt",
                "applymode"
            ]
        ),
        expected(&[
            ("gaintable", texts(&["phase.gcal", "bandpass.bcal"])),
            ("gainfield", texts(&["nearest", "0"])),
            ("interp", texts(&["linear", "nearest,linear"])),
            ("spwmap", lists(&[&[], &[0, 0]])),
            ("calwt", flags(&[true, false])),
            ("applymode", text("trial")),
        ])
    );
}

#[test]
fn one_chain_value_held_for_every_table_is_repeated_for_each() {
    // casa-rs's one value applied to every table; CASA's lists are
    // positional, so it is repeated.
    let resolved = load(
        "applycal",
        "vis = \"in.ms\"\ngaintables = [\"a.gcal\", \"b.gcal\", \"c.gcal\"]\n\
         gainfield = \"nearest\"\ninterp = \"nearest\"\nspwmap = \"0,0\"\n",
    );
    assert_eq!(
        values(&resolved, &["gainfield", "interp", "spwmap"]),
        expected(&[
            ("gainfield", texts(&["nearest"; 3])),
            ("interp", texts(&["nearest"; 3])),
            ("spwmap", lists(&[&[0, 0], &[0, 0], &[0, 0]])),
        ])
    );
}

#[test]
fn applycal_without_calwt_adopts_casas_default_with_a_warning() {
    // casa-rs applied without calibrating the weights; CASA's default is
    // calwt=[True].
    let resolved = load("applycal", "vis = \"in.ms\"\ngaintables = [\"g.gcal\"]\n");
    assert_eq!(resolved.values["calwt"], flags(&[true]));
    assert_eq!(changed_defaults(&resolved), ["calwt"]);
}

#[test]
fn a_callib_profile_sets_docallib() {
    let resolved = load("applycal", "vis = \"in.ms\"\ncallib = \"apply.callib\"\n");
    assert_eq!(resolved.values["docallib"], ParameterValue::Bool(true));
    let resolved = load("applycal", "vis = \"in.ms\"\ngaintables = [\"g.gcal\"]\n");
    assert_eq!(resolved.values["docallib"], ParameterValue::Bool(false));
    // The old explicit absence, "none" or empty, names no library.
    for (surface, solve) in [
        ("applycal", ""),
        ("gaincal", "out_table = \"o.cal\"\nrefant = \"0\"\n"),
        ("bandpass", "out_table = \"o.cal\"\nrefant = \"0\"\n"),
    ] {
        for callib in ["none", ""] {
            let resolved = load(
                surface,
                &format!(
                    "vis = \"in.ms\"\ngaintables = [\"g.gcal\"]\ncallib = \"{callib}\"\n{solve}"
                ),
            );
            assert_eq!(
                values(&resolved, &["docallib", "callib"]),
                expected(&[
                    ("docallib", ParameterValue::Bool(false)),
                    ("callib", text(""))
                ]),
                "{surface} callib={callib:?}"
            );
        }
    }
}

#[test]
fn gaincal_old_names_and_values_take_casas() {
    let resolved = load(
        "gaincal",
        "vis = \"in.ms\"\nout_table = \"g.gcal\"\nrefant = \"0\"\ngain_type = \"t\"\n\
         solve_mode = \"p\"\ngain_combine = \"none\"\nmin_snr = 2.0\n\
         gain_model_source = \"point\"\nsmodel = \"2.5,0,0,0\"\n",
    );
    assert_eq!(
        values(
            &resolved,
            &[
                "caltable", "gaintype", "calmode", "combine", "minsnr", "smodel"
            ]
        ),
        expected(&[
            ("caltable", text("g.gcal")),
            ("gaintype", text("T")),
            ("calmode", text("p")),
            ("combine", text("")),
            ("minsnr", ParameterValue::Float(2.0)),
            ("smodel", floats(&[2.5, 0.0, 0.0, 0.0])),
        ])
    );
    assert!(!resolved.values.contains_key("gain_model_source"));
    // calmode was set, as solve_mode, and smodel too: neither adopts a
    // changed default.
    assert!(changed_defaults(&resolved).is_empty());
}

#[test]
fn gaincal_model_source_becomes_casas_smodel() {
    // The MODEL_DATA column is CASA's empty smodel.
    let resolved = load(
        "gaincal",
        "vis = \"in.ms\"\nout_table = \"g.gcal\"\nrefant = \"0\"\n\
         gain_model_source = \"model-column\"\nsmodel = \"3,0,0,0\"\n",
    );
    assert_eq!(resolved.values["smodel"], floats(&[]));
    // casa-rs's point source with its default smodel was 1 Jy; CASA's
    // default smodel=[] would use MODEL_DATA, so the 1 Jy is kept.
    let resolved = load(
        "gaincal",
        "vis = \"in.ms\"\nout_table = \"g.gcal\"\nrefant = \"0\"\ngain_model_source = \"point\"\n",
    );
    assert_eq!(resolved.values["smodel"], floats(&[1.0, 0.0, 0.0, 0.0]));
    // The migration gave the omitted smodel the point's meaning, so it does
    // not adopt the changed default; the omitted calmode does.
    assert_eq!(changed_defaults(&resolved), ["calmode"]);
}

#[test]
fn bandpass_old_names_and_values_take_casas() {
    let resolved = load(
        "bandpass",
        "vis = \"in.ms\"\nout_table = \"b.bcal\"\nrefant = \"0\"\nbandpass_combine = \"none\"\n\
         bandtype = \"bpoly\"\nsmodel = \"3,0,0,0\"\ngaintables = \"none\"\n",
    );
    assert_eq!(
        values(
            &resolved,
            &["caltable", "combine", "bandtype", "smodel", "gaintable"]
        ),
        expected(&[
            ("caltable", text("b.bcal")),
            ("combine", text("")),
            ("bandtype", text("BPOLY")),
            ("smodel", floats(&[3.0, 0.0, 0.0, 0.0])),
            ("gaintable", texts(&[])),
        ])
    );
    // combine was set, as bandpass_combine.
    assert!(changed_defaults(&resolved).is_empty());
}

#[test]
fn fluxscale_old_names_and_values_take_casas() {
    let resolved = load(
        "fluxscale",
        "vis = \"in.ms\"\nfluxscale_input = \"g.gcal\"\nout_table = \"f.gcal\"\n\
         reference_fields = \"3C286,3C48\"\ntransfer_fields = \"J1\"\nrefspwmap = \"0,0\"\n\
         gainthreshold = 0.2\n",
    );
    assert_eq!(
        values(
            &resolved,
            &[
                "caltable",
                "fluxtable",
                "reference",
                "transfer",
                "refspwmap",
                "gainthreshold"
            ]
        ),
        expected(&[
            ("caltable", text("g.gcal")),
            ("fluxtable", text("f.gcal")),
            ("reference", texts(&["3C286", "3C48"])),
            ("transfer", texts(&["J1"])),
            ("refspwmap", integers(&[0, 0])),
            ("gainthreshold", ParameterValue::Float(0.2)),
        ])
    );
    let resolved = load(
        "fluxscale",
        "vis = \"in.ms\"\nfluxscale_input = \"g.gcal\"\nout_table = \"f.gcal\"\n\
         reference_fields = \"3C286\"\nrefspwmap = \"none\"\ngainthreshold = \"none\"\n",
    );
    assert_eq!(
        values(&resolved, &["refspwmap", "gainthreshold"]),
        expected(&[
            ("refspwmap", integers(&[-1])),
            ("gainthreshold", ParameterValue::Float(-1.0)),
        ])
    );
}

#[test]
fn gencal_old_names_and_values_take_casas() {
    let resolved = load(
        "gencal",
        "vis = \"in.ms\"\nout_table = \"o.cal\"\ncaltype = \"opac\"\nparameter = \"0.1,0.2\"\n",
    );
    assert_eq!(
        values(&resolved, &["caltable", "caltype", "parameter"]),
        expected(&[
            ("caltable", text("o.cal")),
            ("caltype", text("opac")),
            ("parameter", floats(&[0.1, 0.2])),
        ])
    );
}
