// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded default continuation and preservation on rejected restarts (#944).

use std::{collections::BTreeMap, fs, path::Path};

use casa_images::PagedImage;
use casa_provider_contracts::TaskReply;
use serde_json::json;

use super::{
    casa_return::json_run,
    fixture::{Component, Observation},
};

/// Diagnostic bytes of the small T1 output tree, never a production readback.
fn snapshot(directory: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(snapshot(&path));
        } else {
            files.insert(path.clone(), fs::read(path).unwrap());
        }
    }
    files
}

fn assert_unchanged(
    before: &BTreeMap<std::path::PathBuf, Vec<u8>>,
    after: &BTreeMap<std::path::PathBuf, Vec<u8>>,
) {
    assert_eq!(before.len(), after.len(), "output file count changed");
    for (path, bytes) in before {
        assert!(
            after.get(path) == Some(bytes),
            "previous output changed: {}",
            path.display()
        );
    }
}

#[test]
fn default_restart_continues_and_rejected_changes_preserve_the_previous_run() {
    let point = Component {
        offset_px: [6, -4],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise(&[("point", point)]);
    let directory = observation.scratch("restart-preservation");
    fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("image"),
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu", "niter": 10,
    });
    assert!(matches!(
        json_run(&directory, values.clone()),
        TaskReply::Succeeded { .. }
    ));
    let model_path = directory.join("image.model");
    let samples = || PagedImage::<f32>::open(&model_path).unwrap().get().unwrap();
    let first = samples();
    assert!(first.sum() > 0.0);
    // No minor cycle means exact reuse, with no interpolation of model pixels.
    values["niter"] = json!(0);
    let reply = json_run(&directory, values.clone());
    assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    assert_eq!(first, samples());
    values["niter"] = json!(10);
    assert!(matches!(
        json_run(&directory, values.clone()),
        TaskReply::Succeeded { .. }
    ));
    assert!(
        samples().sum() > first.sum() + 0.1,
        "second run must continue the previous model"
    );
    let before = snapshot(&directory);
    assert!(before.keys().any(|p| p.ends_with("image.summary.json")));
    for overrides in [
        json!({"startmodel": model_path}),
        json!({"imsize": [geometry.image_size[0] * 2, geometry.image_size[1]]}),
        json!({"cell": geometry.cell_arcsec.map(|cell| format!("{}arcsec", cell * 2.0))}),
        json!({"stokes": "Q"}),
        json!({"mask": "box[[1pix,1pix],[10pix,10pix]]"}),
        json!({"usemask": "auto-multithresh"}),
    ] {
        let mut rejected = values.clone();
        rejected
            .as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        match json_run(&directory, rejected) {
            TaskReply::Failed { message, .. } => assert!(message.contains("#944"), "{message}"),
            reply => panic!("incompatible restart must fail: {reply:?}"),
        }
        assert_unchanged(&before, &snapshot(&directory));
    }
    // Embedded model masks need preservation support before task continuation.
    // Unlike residual/image PB validity masks, native models have no pixel mask.
    {
        let mut image = PagedImage::<f32>::open(&model_path).unwrap();
        image.make_mask("external", true, false).unwrap();
        image.save().unwrap();
    }
    let mut explicit = values.clone();
    explicit["imagename"] = json!(directory.join("explicit"));
    explicit["startmodel"] = json!(model_path);
    explicit["niter"] = json!(0);
    assert!(matches!(
        json_run(&directory, explicit),
        TaskReply::Succeeded { .. }
    ));
    let explicit_model = PagedImage::<f32>::open(directory.join("explicit.model"))
        .unwrap()
        .get()
        .unwrap();
    assert!(explicit_model.iter().all(|&v| v == 0.0));
    let before = snapshot(&directory);
    match json_run(&directory, values) {
        TaskReply::Failed { message, .. } => {
            assert!(message.contains("model pixel mask"), "{message}")
        }
        reply => panic!("unsupported mask preservation must fail: {reply:?}"),
    }
    assert_unchanged(&before, &snapshot(&directory));
}

#[test]
fn an_existing_outlier_is_refused_before_the_main_image_or_summary_is_written() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("outlier.model.tt0"), "prior model").unwrap();
    let outliers = directory.path().join("outliers.txt");
    fs::write(
        &outliers,
        "imagename=outlier\nphasecenter=J2000 0deg -30deg\n",
    )
    .unwrap();
    let before = snapshot(directory.path());
    let reply = json_run(
        directory.path(),
        json!({
            "vis": "absent.ms", "imagename": directory.path().join("main"),
            "cell": "1arcsec", "outlierfile": outliers, "backend": "cpu",
        }),
    );
    match reply {
        TaskReply::Failed { message, .. } => assert!(message.contains("#944"), "{message}"),
        reply => panic!("outlier namespace must be checked before opening the MS: {reply:?}"),
    }
    assert_unchanged(&before, &snapshot(directory.path()));
}

#[test]
fn orphan_default_products_restart_with_a_zero_model() {
    let observation = Observation::synthesise(&[(
        "point",
        Component {
            offset_px: [6, -4],
            flux_jy: 1.0,
            fwhm_px: None,
            spectral_index: 0.0,
        },
    )]);
    let directory = observation.scratch("restart-orphans");
    fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    let values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("original"),
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu", "niter": 0,
    });
    assert!(matches!(
        json_run(&directory, values.clone()),
        TaskReply::Succeeded { .. }
    ));
    for suffix in ["psf", "residual", "image"] {
        let prefix = directory.join(format!("only_{suffix}"));
        fs::rename(
            directory.join(format!("original.{suffix}")),
            prefix.with_extension(suffix),
        )
        .unwrap();
        let mut restart = values.clone();
        restart["imagename"] = json!(prefix);
        assert!(matches!(
            json_run(&directory, restart),
            TaskReply::Succeeded { .. }
        ));
        let model = PagedImage::<f32>::open(prefix.with_extension("model"))
            .unwrap()
            .get()
            .unwrap();
        assert!(model.iter().all(|&value| value == 0.0));
    }
}

#[test]
fn existing_clean_mask_restricts_continuation_and_is_published_unchanged() {
    let point = |offset_px, flux_jy| Component {
        offset_px,
        flux_jy,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise(&[
        ("left", point([-12, 4], 1.0)),
        ("right", point([14, -6], 2.0)),
    ]);
    let directory = observation.scratch("restart-mask");
    fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("image"),
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu", "niter": 5,
    });
    let reply = json_run(&directory, values.clone());
    assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    let before = PagedImage::<f32>::open(directory.join("image.model"))
        .unwrap()
        .get()
        .unwrap();
    let mask = {
        let mut image = PagedImage::<f32>::open(directory.join("image.mask")).unwrap();
        let mask = ndarray::ArrayD::from_shape_fn(ndarray::IxDyn(image.shape()), |index| {
            if index[0] < geometry.image_size[0] / 2 {
                1.0
            } else {
                0.0
            }
        });
        image.put_slice(&mask, &[0; 4]).unwrap();
        image.save().unwrap();
        mask
    };
    values["niter"] = json!(10);
    let reply = json_run(&directory, values);
    assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    let after = PagedImage::<f32>::open(directory.join("image.model"))
        .unwrap()
        .get()
        .unwrap();
    let published = PagedImage::<f32>::open(directory.join("image.mask"))
        .unwrap()
        .get()
        .unwrap();
    assert_eq!(published, mask);
    let mut added = 0.0;
    for ((&a, &b), &mask) in before.iter().zip(&after).zip(&mask) {
        if mask == 0.0 {
            assert_eq!(a, b, "restart cleaned outside the existing mask");
        } else {
            added += (a - b).abs();
        }
    }
    assert!(
        added > 0.1,
        "restart must clean the source inside the old mask"
    );
}

#[test]
fn outlier_preflight_preserves_a_trailing_slash_and_does_not_allocate_its_mask() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("outlier");
    fs::create_dir(&output).unwrap();
    fs::write(output.join(".model"), "previous model").unwrap();
    let outliers = directory.path().join("outliers.txt");
    // This support would be enormous if the preflight compiled pixel masks.
    fs::write(&outliers, "imagename=outlier/\nimsize=1000000000\nphasecenter=J2000 0deg -30deg\nmask=circle[[5pix,5pix],2pix]\n").unwrap();
    let before = snapshot(directory.path());
    let reply = json_run(
        directory.path(),
        json!({
            "vis": "absent.ms", "imagename": directory.path().join("main"),
            "cell": "1arcsec", "outlierfile": outliers, "backend": "cpu",
        }),
    );
    match reply {
        TaskReply::Failed { message, .. } => assert!(message.contains("#944"), "{message}"),
        reply => panic!("occupied outlier should fail without compiling a mask: {reply:?}"),
    }
    assert_unchanged(&before, &snapshot(directory.path()));
}

#[test]
fn restart_false_public_run_selects_fresh_numbered_namespaces() {
    use casars_imager::{ImagingParameters, resolve_request, run};

    let observation = Observation::synthesise(&[(
        "point",
        Component {
            offset_px: [6, -4],
            flux_jy: 1.0,
            fwhm_px: None,
            spectral_index: 0.0,
        },
    )]);
    let directory = observation.scratch("restart-false-numbering");
    fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    let values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("image"),
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu", "niter": 10, "restart": true,
    });
    let (parameters, request) =
        resolve_request(&ImagingParameters(values.as_object().unwrap().clone())).unwrap();
    run(parameters.clone(), &request).unwrap();
    let model = |name: &str| {
        PagedImage::<f32>::open(directory.join(format!("{name}.model")))
            .unwrap()
            .get()
            .unwrap()
    };
    let first = model("image");
    assert!(first.sum() > 0.0);
    // Preparation-only bypass of the catalog's unfinished-value refusal. The
    // public execution path and its output-policy method are not bypassed.
    let mut numbered = parameters;
    numbered.0.insert("restart".into(), json!(false));
    for name in ["image_2", "image_3"] {
        let before = snapshot(&directory);
        let result = run(numbered.clone(), &request).unwrap();
        let selected = directory.join(name);
        assert!(!result.artifacts.is_empty());
        for artifact in &result.artifacts {
            assert!(
                artifact
                    .path
                    .starts_with(&format!("{}.", selected.display())),
                "{}",
                artifact.path
            );
            assert!(artifact.exists && Path::new(&artifact.path).exists());
        }
        assert!(directory.join(format!("{name}.summary.json")).is_file());
        let after = snapshot(&directory);
        for (path, bytes) in before {
            assert_eq!(
                after.get(&path),
                Some(&bytes),
                "previous output changed: {}",
                path.display()
            );
        }
        let fresh = model(name);
        let error: f64 = fresh
            .iter()
            .zip(first.iter())
            .map(|(&a, &b)| f64::from(a - b).powi(2))
            .sum();
        let power: f64 = first.iter().map(|&v| f64::from(v).powi(2)).sum();
        assert!(
            (error / power).sqrt() < 0.001,
            "numbered run must start fresh, not accumulate the old model"
        );
    }
}
