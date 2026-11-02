// SPDX-License-Identifier: LGPL-3.0-or-later

//! tclean's continuation on an earlier run (#944, #980): `calcpsf=False`
//! and `calcres=False` read the PSF, sums of weights and residual an earlier
//! run left rather than form them (`task_tclean.py`), and `restart=True`
//! continues the model of a cube, a Taylor family and several Stokes planes
//! as of a single Stokes-I plane. Each sequence follows one of CASA's own
//! tclean tests and is compared with the same work done in one run.

use std::collections::BTreeSet;
use std::path::Path;

use casars_imager::ImagerCleanStopReason;
use serde_json::{Value, json};

use super::fixture::{Component, Observation, Products};

const POINT: Component = Component {
    offset_px: [6, -4],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: -0.7,
};

/// The products at `prefix`: every image whose name is `prefix.<suffix>`,
/// but the primary beam, which the fixture leaves off (`write_pb`).
fn inventory(prefix: &Path) -> BTreeSet<String> {
    let stem = format!("{}.", prefix.file_name().unwrap().to_string_lossy());
    std::fs::read_dir(prefix.parent().unwrap())
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            name.strip_prefix(&stem).map(str::to_string)
        })
        .filter(|suffix| !suffix.starts_with("pb"))
        .collect()
}

fn names(suffixes: &[&str]) -> BTreeSet<String> {
    suffixes
        .iter()
        .map(|suffix| (*suffix).to_string())
        .collect()
}

/// `‖a − b‖ / ‖b‖` over every value of two products.
fn nrms(actual: &Products, expected: &Products, suffix: &str) -> f64 {
    let (a, b) = (actual.get(suffix).values(), expected.get(suffix).values());
    assert_eq!(a.shape(), b.shape(), "{suffix}");
    let error: f64 = a
        .iter()
        .zip(&b)
        .map(|(&a, &b)| f64::from(a - b).powi(2))
        .sum();
    let power: f64 = b.iter().map(|&b| f64::from(b).powi(2)).sum();
    (error / power).sqrt()
}

fn with(base: &Value, overrides: Value) -> Value {
    let mut values = base.clone();
    values
        .as_object_mut()
        .unwrap()
        .extend(overrides.as_object().unwrap().clone());
    values
}

/// CASA's `test_onefield_restart_mfs`: the PSF alone, then the residual
/// with that PSF, then cleaning straight from both, twice; each step reads
/// what the one before left, and a residual read is not a major cycle.
#[test]
fn calcpsf_and_calcres_false_continue_an_mfs_run_as_casa_does() {
    let observation = Observation::synthesise_band(4, &[("point", POINT)]);
    let base = json!({"deconvolver": "clark", "gain": 0.1});
    let prefix = observation.scratch("steps");
    let step = |overrides: Value| observation.image("steps", with(&base, overrides));

    let (report, _) = step(json!({"niter": 0, "calcres": false}));
    assert_eq!(report.major_cycles, 0);
    assert_eq!(inventory(&prefix), names(&["psf", "sumwt"]));

    let (_, residual) = step(json!({
        "niter": 0, "calcpsf": false, "restoration": false,
    }));
    assert_eq!(
        inventory(&prefix),
        names(&["model", "psf", "residual", "sumwt"])
    );
    let (_, dirty) = observation.image("dirty", with(&base, json!({"niter": 0})));
    assert!(nrms(&residual, &dirty, ".residual") < 1e-6);
    assert!(nrms(&residual, &dirty, ".psf") < 1e-6);

    let (first, cleaned) = step(json!({"niter": 10, "calcpsf": false, "calcres": false}));
    assert_eq!(
        first.major_cycles, 1,
        "the residual read is not a major cycle"
    );
    assert_eq!(first.minor_iterations, 10);
    let (once, reference) = observation.image("once", with(&base, json!({"niter": 10})));
    assert_eq!(once.major_cycles, 2);
    for suffix in [".model", ".residual", ".image"] {
        let error = nrms(&cleaned, &reference, suffix);
        assert!(error < 1e-5, "{suffix}: {error}");
    }
    assert!(inventory(&prefix).contains("mask"));

    let flux = |products: &Products| products.get(".model").values().sum();
    let (again, continued) = step(json!({"niter": 10, "calcpsf": false, "calcres": false}));
    assert_eq!(again.major_cycles, 1);
    assert!(
        flux(&continued) > flux(&cleaned) + 0.05,
        "the second run continues the first's model"
    );
}

/// CASA's `test_onefield_restart_mtmfs` and `test_onefield_restore_mtmfs_niter0`:
/// a Taylor family's PSF moments, sums and residual terms are read as one
/// run forms them, and a run of no iterations restores what it read.
#[test]
fn a_taylor_family_reads_its_psf_moments_and_residual_terms() {
    let observation = Observation::synthesise_band(4, &[("point", POINT)]);
    let base = json!({"deconvolver": "mtmfs", "nterms": 2});
    let prefix = observation.scratch("taylor");
    let step = |overrides: Value| observation.image("taylor", with(&base, overrides));

    step(json!({"niter": 0, "calcres": false}));
    assert_eq!(
        inventory(&prefix),
        names(&[
            "psf.tt0",
            "psf.tt1",
            "psf.tt2",
            "sumwt.tt0",
            "sumwt.tt1",
            "sumwt.tt2"
        ])
    );
    step(json!({"niter": 0, "calcpsf": false, "restoration": false}));
    let (restored_report, restored) = step(json!({
        "niter": 0, "calcpsf": false, "calcres": false,
    }));
    assert_eq!(restored_report.major_cycles, 0);
    let (_, dirty) = observation.image("taylor-dirty", with(&base, json!({"niter": 0})));
    for suffix in [
        ".residual.tt0",
        ".residual.tt1",
        ".psf.tt2",
        ".image.tt0",
        ".image.tt1",
    ] {
        let error = nrms(&restored, &dirty, suffix);
        assert!(error < 1e-5, "{suffix}: {error}");
    }
    assert!(inventory(&prefix).contains("alpha"));

    let (cleaned_report, cleaned) = step(json!({
        "niter": 10, "calcpsf": false, "calcres": false,
    }));
    assert_eq!(cleaned_report.major_cycles, 1);
    let (_, reference) = observation.image("taylor-once", with(&base, json!({"niter": 10})));
    for suffix in [".model.tt0", ".model.tt1", ".residual.tt0", ".image.tt1"] {
        let error = nrms(&cleaned, &reference, suffix);
        assert!(error < 1e-5, "{suffix}: {error}");
    }
    // calcres=True forms the residual of the continued model: a major cycle.
    let (formed, _) = step(json!({"niter": 10, "calcpsf": false}));
    assert_eq!(formed.major_cycles, 2);
}

/// CASA's `test_cube_common_restoringbeam`: a cube cleaned without
/// restoration is restored later with a common beam from what it left, and
/// a cube's model continues channel by channel.
#[test]
fn a_cube_restores_and_continues_from_what_it_left() {
    let observation = Observation::synthesise_band(4, &[("point", POINT)]);
    let base = json!({"specmode": "cube", "deconvolver": "hogbom"});
    let (_, unrestored) = observation.image(
        "cube",
        with(&base, json!({"niter": 10, "restoration": false})),
    );
    assert!(!inventory(&observation.scratch("cube")).contains("image"));
    let (restore, restored) = observation.image(
        "cube",
        with(
            &base,
            json!({
                "niter": 0, "calcres": false, "calcpsf": false, "restoringbeam": "common",
            }),
        ),
    );
    assert_eq!(restore.major_cycles, 0, "nothing was formed");
    let (_, reference) = observation.image(
        "cube-once",
        with(&base, json!({"niter": 10, "restoringbeam": "common"})),
    );
    for suffix in [".image", ".residual", ".psf"] {
        let error = nrms(&restored, &reference, suffix);
        assert!(error < 1e-5, "{suffix}: {error}");
    }
    assert!(nrms(&unrestored, &reference, ".model") < 1e-6);

    // A continuation reads every channel's model: with no iterations it
    // forms the residual the first run left, and with more it cleans on.
    let (_, reformed) = observation.image("cube", with(&base, json!({"niter": 0})));
    assert_eq!(
        reformed.get(".model").values(),
        unrestored.get(".model").values()
    );
    let error = nrms(&reformed, &unrestored, ".residual");
    assert!(error < 1e-5, "residual of the read model: {error}");
    let (_, continued) = observation.image("cube", with(&base, json!({"niter": 10})));
    let flux = |products: &Products| products.get(".model").values().sum();
    let (before, after) = (flux(&unrestored), flux(&continued));
    assert!(after > before + 0.05, "{before} -> {after}");
}

/// CASA's `test_mosaic_cube` and `test_mosaic_mtmfs` restarts: a mosaic's
/// model is published in flat-noise units, so a continuation makes the
/// earlier model physical with the weights it forms (every Taylor term with
/// term 0's, `SIImageStoreMultiTerm::divideModelByWeight`) before it
/// predicts it; with no iterations it gives the earlier run's products.
#[test]
fn a_mosaic_continues_its_cube_and_taylor_models() {
    // Both on the two-channel band, whose lever arm a Taylor family needs.
    for (label, taylor) in [("mosaic-cube", false), ("mosaic-taylor", true)] {
        let (observation, mut controls) = super::mosaic::mosaic_run(true);
        controls["niter"] = json!(200);
        if taylor {
            controls["deconvolver"] = json!("mtmfs");
            controls["nterms"] = json!(2);
            controls["pbcor"] = json!(false);
            controls.as_object_mut().unwrap().remove("specmode");
        }
        let (_, deep) = observation.image(label, controls.clone());
        controls["niter"] = json!(0);
        let (_, continued) = observation.image(label, controls);
        let suffixes: &[&str] = if taylor {
            &[
                ".model.tt0",
                ".model.tt1",
                ".residual.tt0",
                ".residual.tt1",
                ".image.tt0",
            ]
        } else {
            &[".model", ".residual", ".image"]
        };
        for suffix in suffixes {
            let error = nrms(&continued, &deep, suffix);
            assert!(error < 1e-5, "{label} {suffix}: {error}");
        }
    }
}

/// A run of two Stokes planes continues each plane's model.
#[test]
fn several_stokes_planes_continue_their_models() {
    let observation = Observation::synthesise_band(2, &[("point", POINT)]);
    let base = json!({"stokes": "IV", "deconvolver": "hogbom", "niter": 10});
    let (_, first) = observation.image("stokes", base.clone());
    assert_eq!(first.get(".model").image.shape()[2], 2);
    // Both planes' PSF, sums and residual read restore as the run did.
    let (_, restored) = observation.image(
        "stokes",
        with(
            &base,
            json!({"niter": 0, "calcpsf": false, "calcres": false}),
        ),
    );
    for suffix in [".image", ".residual", ".psf"] {
        let error = nrms(&restored, &first, suffix);
        assert!(error < 1e-5, "{suffix}: {error}");
    }
    // Both planes' models are read: no iterations form the residual the
    // first run left, and more clean on.
    let (_, reformed) = observation.image("stokes", with(&base, json!({"niter": 0})));
    let error = nrms(&reformed, &first, ".residual");
    assert!(error < 1e-5, "residual of the read model: {error}");
    let (_, continued) = observation.image("stokes", base.clone());
    let plane = |products: &Products, stokes: usize| {
        let values = products.get(".model").values();
        values
            .index_axis(ndarray::Axis(2), stokes)
            .iter()
            .map(|&v| f64::from(v))
            .sum::<f64>()
    };
    assert!(plane(&continued, 0) > plane(&first, 0) + 0.05);
}

/// CASA's `test_iterbot_nmajor_0`: `nmajor=0` cleans nothing from a residual
/// it read and reports no major cycle, and restores what it read.
#[test]
fn nmajor_zero_on_a_read_residual_runs_no_major_cycle() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    observation.image("nmajor", json!({"niter": 0, "restoration": false}));
    let (report, products) = observation.image(
        "nmajor",
        json!({"niter": 500, "nmajor": 0, "calcres": false}),
    );
    assert_eq!(report.major_cycles, 0);
    assert_eq!(report.minor_iterations, 0);
    assert_eq!(
        report.clean_stop_reason,
        Some(ImagerCleanStopReason::MajorCycleLimitReached)
    );
    let suffixes = inventory(&observation.scratch("nmajor"));
    assert!(
        suffixes.contains("image") && suffixes.contains("mask"),
        "{suffixes:?}"
    );
    let (_, dirty) = observation.image("nmajor-dirty", json!({"niter": 0}));
    assert!(nrms(&products, &dirty, ".image") < 1e-5);
}

/// CASA's `test_modelvis_20`: a run of no iterations that reads its PSF and
/// residual predicts the model an earlier run left into `MODEL_DATA` and
/// leaves the residual as it was.
#[test]
fn a_read_model_is_predicted_and_the_read_residual_stands() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let (_, cleaned) = observation.image("modelvis", json!({"niter": 10}));
    let (report, predicted) = observation.image(
        "modelvis",
        json!({
            "niter": 0, "calcres": false, "calcpsf": false,
            "restoration": false, "savemodel": "modelcolumn",
        }),
    );
    assert!(report.visibility_products.is_some());
    assert!(nrms(&predicted, &cleaned, ".residual") < 1e-6);
    assert_eq!(
        predicted.get(".model").values(),
        cleaned.get(".model").values()
    );
    let model_jy = f64::from(cleaned.get(".model").values().sum());
    let (cross, power) =
        observation
            .data_and_model()
            .iter()
            .fold((0.0, 0.0), |(cross, power), (data, model)| {
                let (d, m) = (
                    [f64::from(data.re), f64::from(data.im)],
                    [f64::from(model.re), f64::from(model.im)],
                );
                (
                    cross + d[0] * m[0] + d[1] * m[1],
                    power + m[0] * m[0] + m[1] * m[1],
                )
            });
    // MODEL_DATA holds the model's flux: the data are a point of 1 Jy
    // at the band centre and the model a part of it.
    let gain = cross / power;
    assert!(
        (gain * model_jy - POINT.flux_jy).abs() < 0.05,
        "gain {gain}, model {model_jy} Jy"
    );
}

/// What casa-rs does not read is refused before anything is written.
#[test]
fn unsupported_or_missing_reads_are_refused() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let refusal = |name: &str, controls: Value| match observation.try_image(name, controls) {
        Ok(_) => panic!("{name} must be refused"),
        Err(message) => message,
    };
    let missing = refusal("missing", json!({"niter": 0, "calcpsf": false}));
    assert!(missing.contains("calcpsf=False reads the PSF"), "{missing}");
    let cleaning = refusal("cleaning", json!({"niter": 5, "calcres": false}));
    assert!(
        cleaning.contains("calcres=False with niter > 0"),
        "{cleaning}"
    );
    assert!(inventory(&observation.scratch("missing")).is_empty());
    assert!(inventory(&observation.scratch("cleaning")).is_empty());
}
