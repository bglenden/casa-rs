// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded default continuation and preservation on rejected restarts (#944).

use std::{collections::BTreeMap, fs, path::Path};

use casa_contracts::TaskReply;
use casa_images::PagedImage;
use serde_json::json;

#[path = "outlier_residual.rs"]
mod outlier_residual;

use super::{
    casa_return::{json_run, json_run_with},
    fixture::{Component, Observation},
    mosaic::mosaic_run,
};

/// Diagnostic bytes of the small T1 output tree, never a production readback.
pub(super) fn snapshot(directory: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
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

pub(super) fn assert_unchanged(
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

/// An ordinary mosaic continues on the saved sensitivity, keeping the
/// model in the right units for the next major cycle. The on-disk PSF,
/// PB, weights and sums stay in place while the residual and model advance.
#[test]
fn a_mosaic_reuses_the_saved_psf_sensitivity_and_pb_during_continuation() {
    check_mosaic_continuation(false);
}

#[test]
fn a_mosaic_cube_reuses_each_channels_saved_weight_and_pb() {
    check_mosaic_continuation(true);
}

fn check_mosaic_continuation(cube: bool) {
    let (observation, mut controls) = mosaic_run(cube);
    controls["niter"] = json!(8);
    controls["threshold"] = json!("0Jy");
    let (_, seed) = observation.image("mosaic-saved", controls.clone());
    observation.image("mosaic-refresh-psf", controls.clone());
    let saved = [".psf", ".sumwt", ".pb", ".weight"];
    let paths = saved.map(|suffix| observation.scratch(&format!("mosaic-saved{suffix}")));
    let before = paths.each_ref().map(|path| snapshot(path));
    controls["niter"] = json!(20);
    let (_, refreshed) = observation.image("mosaic-refresh-psf", controls.clone());
    controls["calcpsf"] = json!(false);

    let missing = observation.scratch("withheld-mosaic-sensitivity");
    fs::rename(&paths[3], &missing).unwrap();
    let Err(reason) = observation.try_image("mosaic-saved", controls.clone()) else {
        panic!("a mosaic must have its saved sensitivity");
    };
    assert!(reason.contains("saved weight and PB"), "{reason}");
    fs::rename(missing, &paths[3]).unwrap();

    let (_, continued) = observation.image("mosaic-saved", controls.clone());
    for ((suffix, path), before) in saved.into_iter().zip(&paths).zip(before) {
        assert!(!continued.suffixes().contains(suffix), "rewrote {suffix}");
        assert_unchanged(&before, &snapshot(path));
    }
    assert_ne!(
        seed.get(".model").values(),
        continued.get(".model").values()
    );
    let seed_pb_mask = seed.get(".pb").mask();
    for suffix in [".image", ".image.pbcor", ".residual"] {
        assert_eq!(
            continued.get(suffix).mask(),
            seed_pb_mask,
            "{suffix} saved mosaic PB mask"
        );
    }
    for suffix in [".image", ".image.pbcor", ".model", ".residual"] {
        let expected = refreshed.get(suffix).values();
        let reused = continued.get(suffix).values();
        let power: f64 = expected.iter().map(|value| f64::from(*value).powi(2)).sum();
        let error: f64 = expected
            .iter()
            .zip(&reused)
            .map(|(a, b)| f64::from(*a - *b).powi(2))
            .sum();
        assert!(
            power > 0.0 && (error / power).sqrt() < 2e-6,
            "{suffix}: {error}/{power}"
        );
    }
    let mut pb = PagedImage::<f32>::open(&paths[2]).unwrap();
    let pb_mask = pb.get_mask().unwrap().unwrap();
    let beam = pb.get().unwrap().mapv(|value| 0.75 * value);
    pb.put_slice(&beam, &[0; 4]).unwrap();
    pb.save().unwrap();
    drop(pb);
    controls["pblimit"] = json!(0.8);
    controls["niter"] = json!(0);
    observation.image("mosaic-saved", controls);
    let open = |suffix| PagedImage::<f32>::open(observation.scratch(suffix)).unwrap();
    let restored = open("mosaic-saved.image");
    let corrected = open("mosaic-saved.image.pbcor");
    let residual = open("mosaic-saved.residual");
    for image in [&restored, &corrected, &residual] {
        assert_eq!(
            image.get_mask().unwrap().unwrap(),
            pb_mask,
            "saved mosaic PB mask"
        );
    }
    let restored = restored.get().unwrap();
    let corrected = corrected.get().unwrap();
    assert!(corrected.iter().any(|value| value.abs() > 0.01));
    for (((restored, corrected), beam), valid) in
        restored.iter().zip(&corrected).zip(&beam).zip(&pb_mask)
    {
        if *valid && *beam > 0.0 {
            let expected = *restored / *beam;
            assert!(
                (*corrected - expected).abs() < 1e-6,
                "{corrected} vs {expected}"
            );
        }
    }
}

#[test]
fn a_mosaic_cleans_the_saved_residual_and_predicts_the_persisted_model() {
    let (observation, mut controls) = mosaic_run(false);
    controls["niter"] = json!(8);
    controls["threshold"] = json!("0Jy");
    observation.image("mosaic-reuse", controls.clone());
    observation.image("mosaic-recompute", controls.clone());
    let suffixes = [".psf", ".weight", ".sumwt", ".pb"];
    let paths = suffixes.map(|suffix| observation.scratch(&format!("mosaic-reuse{suffix}")));
    let preserved = paths.each_ref().map(|path| snapshot(path));
    let residual_path = observation.scratch("mosaic-reuse.residual");
    let mut residual = PagedImage::<f32>::open(&residual_path).unwrap();
    let mut mask = residual.get_mask().unwrap().unwrap();
    let [x, y] = observation.geometry().centre();
    let sentinel = [x + 12, y + 7, 0, 0];
    assert!(mask[sentinel]);
    mask[sentinel] = false;
    residual.put_mask_slice("mask0", &mask, &[0; 4]).unwrap();
    residual.save().unwrap();
    drop(residual);

    controls["niter"] = json!(5);
    controls["calcpsf"] = json!(false);
    let (recomputed, expected) = observation.image("mosaic-recompute", controls.clone());
    controls["calcres"] = json!(false);
    let (reused, actual) = observation.image("mosaic-reuse", controls);
    assert_eq!(reused.minor_iterations, 5);
    assert_eq!(reused.major_cycles + 1, recomputed.major_cycles);
    for suffix in [".image", ".residual"] {
        assert_eq!(actual.get(suffix).mask().as_ref(), Some(&mask));
    }
    assert!(actual.get(".image.pbcor").mask().unwrap()[sentinel]);
    for (path, bytes) in paths.iter().zip(&preserved) {
        assert_unchanged(bytes, &snapshot(path));
    }
    for suffix in [".residual", ".model", ".image", ".image.pbcor"] {
        let expected = expected.get(suffix).values();
        let actual = actual.get(suffix).values();
        let error: f64 = expected
            .iter()
            .zip(&actual)
            .map(|(a, b)| f64::from(*a - *b).powi(2))
            .sum();
        let power: f64 = expected.iter().map(|value| f64::from(*value).powi(2)).sum();
        assert!(
            power > 0.0 && (error / power).sqrt() < 2e-6,
            "{suffix}: {error}/{power}"
        );
    }
}

#[test]
fn a_mosaic_restores_the_saved_residual_and_uses_its_mask_to_stop_cleaning() {
    let (observation, mut controls) = mosaic_run(false);
    controls["niter"] = json!(8);
    controls["threshold"] = json!("0Jy");
    let (_, seed) = observation.image("mosaic-stopped", controls.clone());
    let path = observation.scratch("mosaic-stopped.residual");
    let mut residual = PagedImage::<f32>::open(&path).unwrap();
    let [cx, cy] = observation.geometry().centre();
    let hidden = [cx, cy, 0, 0];
    let target = [cx + 2, cy + 1, 0, 0];
    let mut mask = seed.get(".residual").mask().unwrap();
    assert!(mask[hidden] && mask[target]);
    mask[hidden] = false;
    let mut pixels = residual.get().unwrap();
    pixels[hidden] = 50.0;
    pixels[target] += 0.3;
    residual.put_slice(&pixels, &[0; 4]).unwrap();
    residual.put_mask_slice("mask0", &mask, &[0; 4]).unwrap();
    residual.save().unwrap();
    drop(residual);
    let before = snapshot(&path);

    controls["calcpsf"] = json!(false);
    controls["calcres"] = json!(false);
    controls["threshold"] = json!("2Jy");
    controls["pblimit"] = json!(-0.6);
    let (stopped, restored) = observation.image("mosaic-stopped", controls);
    assert_eq!((stopped.minor_iterations, stopped.major_cycles), (0, 0));
    assert_unchanged(&before, &snapshot(&path));
    assert_eq!(restored.get(".image").mask().as_ref(), Some(&mask));
    let difference = restored.get(".image").values()[target] - seed.get(".image").values()[target];
    assert!((difference - 0.3).abs() < 1e-5, "{difference}");
    let difference =
        restored.get(".image.pbcor").values()[target] - seed.get(".image.pbcor").values()[target];
    assert!(
        (difference - 0.3 / seed.get(".pb").values()[target]).abs() < 1e-5,
        "{difference}"
    );
}

#[test]
fn a_mosaic_cube_restart_reads_and_publishes_the_mask_of_each_channel() {
    let (observation, mut controls) = mosaic_run(true);
    controls["niter"] = json!(8);
    controls["threshold"] = json!("0Jy");
    let (_, seed) = observation.image("mosaic-cube-stopped", controls.clone());
    let path = observation.scratch("mosaic-cube-stopped.residual");
    let mut residual = PagedImage::<f32>::open(&path).unwrap();
    let [x, y] = observation.geometry().centre();
    let hidden = [x, y, 0, 1];
    let visible = [x + 3, y + 2, 0, 1];
    let mut mask = residual.get_mask().unwrap().unwrap();
    assert!(mask[hidden] && mask[visible] && mask[[x, y, 0, 0]]);
    mask[hidden] = false;
    let mut pixels = residual.get().unwrap();
    pixels[hidden] = 50.0;
    pixels[visible] += 0.3;
    residual.put_slice(&pixels, &[0; 4]).unwrap();
    residual.put_mask_slice("mask0", &mask, &[0; 4]).unwrap();
    residual.save().unwrap();
    drop(residual);
    let before = snapshot(&path);
    controls["calcpsf"] = json!(false);
    controls["calcres"] = json!(false);
    controls["threshold"] = json!("2Jy");
    let (stopped, restored) = observation.image("mosaic-cube-stopped", controls.clone());
    assert_eq!(
        (stopped.minor_iterations, stopped.major_cycles),
        (0, 0),
        "only the second channel masks the sentinel"
    );
    assert_unchanged(&before, &snapshot(&path));
    assert_eq!(restored.get(".image").mask().as_ref(), Some(&mask));
    let delta = restored.get(".image").values()[visible] - seed.get(".image").values()[visible];
    assert!(
        (delta - 0.3).abs() < 1e-5,
        "channel two restored delta {delta}"
    );
    controls["threshold"] = json!("0Jy");
    controls["niter"] = json!(2);
    let (clean, products) = observation.image("mosaic-cube-stopped", controls.clone());
    assert!(clean.minor_iterations > 0 && clean.major_cycles > 0);
    let pb_mask = seed.get(".pb").mask().unwrap();
    assert_eq!(products.get(".residual").mask().as_ref(), Some(&pb_mask));
    assert_eq!(products.get(".image").mask().as_ref(), Some(&pb_mask));

    let mut residual = PagedImage::<f32>::open(&path).unwrap();
    residual.put_mask_slice("mask0", &mask, &[0; 4]).unwrap();
    residual.save().unwrap();
    drop(residual);
    assert_mosaic_cube_negative_cutoff(&observation, &mut controls, &mask, &pb_mask);

    let mut pb = PagedImage::<f32>::open(observation.scratch("mosaic-cube-stopped.pb")).unwrap();
    pb.remove_mask("mask0").unwrap();
    pb.save().unwrap();
    drop(pb);
    controls["pblimit"] = json!(0.2);
    let (clean, products) = observation.image("mosaic-cube-stopped", controls.clone());
    assert!(clean.major_cycles > 0);
    assert_eq!(products.get(".residual").mask().as_ref(), Some(&mask));
    assert_eq!(products.get(".image").mask().as_ref(), Some(&mask));
    assert_eq!(products.get(".image.pbcor").mask().as_ref(), Some(&pb_mask));
    let mut corrected =
        PagedImage::<f32>::open(observation.scratch("mosaic-cube-stopped.image.pbcor")).unwrap();
    corrected.remove_mask("mask0").unwrap();
    corrected.save().unwrap();
    drop(corrected);
    controls["niter"] = json!(0);
    let (_, products) = observation.image("mosaic-cube-stopped", controls);
    assert!(products.get(".image.pbcor").mask().is_none());
}

/// A negative cutoff preserves the old mask, while prediction excludes
/// apparent-model samples below its magnitude on the temporary cube.
fn assert_mosaic_cube_negative_cutoff(
    observation: &Observation,
    controls: &mut serde_json::Value,
    mask: &ndarray::ArrayD<bool>,
    pb_mask: &ndarray::ArrayD<bool>,
) {
    let pb = PagedImage::<f32>::open(observation.scratch("mosaic-cube-stopped.pb"))
        .unwrap()
        .get()
        .unwrap();
    let corner = pb
        .indexed_iter()
        .find_map(|(index, &value)| {
            (index[3] == 1 && value > 0.25 && value < 0.5 && pb_mask[&index]).then_some(index)
        })
        .expect("a sample between cutoffs");
    let location: [usize; 4] = std::array::from_fn(|axis| corner[axis]);
    let mut model =
        PagedImage::<f32>::open(observation.scratch("mosaic-cube-stopped.model")).unwrap();
    let mut pixels = model.get().unwrap();
    pixels[location] = 0.125;
    model.put_slice(&pixels, &[0; 4]).unwrap();
    model.save().unwrap();
    drop(model);
    controls["pblimit"] = json!(-0.6);
    let (clean, products) = observation.image("mosaic-cube-stopped", controls.clone());
    assert!(clean.major_cycles > 0);
    assert_eq!(products.get(".residual").mask().as_ref(), Some(mask));
    assert_eq!(products.get(".image").mask().as_ref(), Some(mask));
    assert_eq!(
        products.get(".model").values()[location],
        0.125,
        "the persisted cube keeps an apparent model sample outside the new cutoff"
    );
}

#[test]
fn a_mosaic_resumed_with_positive_cutoff_copies_the_saved_pb_mask_after_gridding() {
    let (observation, mut controls) = mosaic_run(false);
    controls["niter"] = json!(8);
    controls["threshold"] = json!("0Jy");
    controls["pblimit"] = json!(-0.65);
    let (_, seed) = observation.image("mosaic-cutoff", controls.clone());
    assert!(seed.get(".residual").mask().is_none());
    let mut pb = PagedImage::<f32>::open(observation.scratch("mosaic-cutoff.pb")).unwrap();
    let mut mask = pb.get_mask().unwrap().unwrap();
    // A saved mask may have a smaller footprint than the new PB cutoff.
    // Leave one valid pixel away from the sources and the saved CLEAN peak.
    let sentinel = [10, 20, 0, 0];
    mask.fill(false);
    mask[sentinel] = true;
    pb.put_mask_slice("mask0", &mask, &[0; 4]).unwrap();
    pb.save().unwrap();
    drop(pb);

    controls["niter"] = json!(12);
    controls["cycleniter"] = json!(2);
    controls["threshold"] = json!("0.01Jy");
    controls["calcpsf"] = json!(false);
    controls["calcres"] = json!(false);
    controls["pblimit"] = json!(0.2);
    let (resumed, products) = observation.image("mosaic-cutoff", controls);
    assert_eq!(
        resumed.major_cycles, 1,
        "the saved PB masks the sources after gridding"
    );
    assert!(resumed.minor_iterations > 0 && resumed.minor_iterations < 12);
    for suffix in [".residual", ".image"] {
        assert_eq!(products.get(suffix).mask().as_ref(), Some(&mask));
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
        json_run_with(&directory, values.clone(), true),
        TaskReply::Succeeded { .. }
    ));
    let model_path = directory.join("image.model");
    let samples = || PagedImage::<f32>::open(&model_path).unwrap().get().unwrap();
    let first = samples();
    assert!(first.sum() > 0.0);
    // No minor cycle means exact reuse, with no interpolation of model pixels.
    values["niter"] = json!(0);
    let reply = json_run_with(&directory, values.clone(), true);
    assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    assert_eq!(first, samples());
    values["niter"] = json!(10);
    assert!(matches!(
        json_run_with(&directory, values.clone(), true),
        TaskReply::Succeeded { .. }
    ));
    assert!(
        samples().sum() > first.sum() + 0.1,
        "second run must continue the previous model"
    );
    // The run summary, asked for here, is part of what a rejected
    // continuation must leave as it was.
    let before = snapshot(&directory);
    assert!(before.keys().any(|p| p.ends_with("image.summary.json")));
    for overrides in [
        json!({"startmodel": model_path}),
        json!({"imsize": [geometry.image_size[0] * 2, geometry.image_size[1]]}),
        json!({"cell": geometry.cell_arcsec.map(|cell| format!("{}arcsec", cell * 2.0))}),
        json!({"stokes": "Q"}),
        json!({"mask": "box[[1pix,1pix],[10pix,10pix]]"}),
        json!({"usemask": "auto-multithresh", "calcpsf": false}),
    ] {
        let mut rejected = values.clone();
        rejected
            .as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        match json_run_with(&directory, rejected, true) {
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
        json_run_with(&directory, explicit, true),
        TaskReply::Succeeded { .. }
    ));
    let explicit_model = PagedImage::<f32>::open(directory.join("explicit.model"))
        .unwrap()
        .get()
        .unwrap();
    assert!(explicit_model.iter().all(|&v| v == 0.0));
    let before = snapshot(&directory);
    match json_run_with(&directory, values, true) {
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
            "cell": "1arcsec", "outlierfile": outliers, "backend": "cpu", "calcpsf": false,
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
    use casa_tasks::imaging::{ImagingParameters, resolve_request, run};

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
    run(parameters, &request).unwrap();
    let model = |name: &str| {
        PagedImage::<f32>::open(directory.join(format!("{name}.model")))
            .unwrap()
            .get()
            .unwrap()
    };
    let first = model("image");
    assert!(first.sum() > 0.0);
    // Resolve False through the activated public contract before execution.
    let mut numbered = values;
    numbered["restart"] = json!(false);
    let (numbered, request) =
        resolve_request(&ImagingParameters(numbered.as_object().unwrap().clone())).unwrap();
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
        // As CASA's tclean, a run writes nothing beside its products.
        assert!(!directory.join(format!("{name}.summary.json")).exists());
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

/// CASA uses one suffix for all fields, even when an outlier has a
/// higher existing suffix. A single-field call can skip the outlier.
#[test]
fn restart_false_numbers_the_main_and_outliers_together() {
    use casa_tasks::imaging::{ImagingParameters, resolve_request, run};

    let observation = Observation::synthesise(&[(
        "point",
        Component {
            offset_px: [6, -4],
            flux_jy: 1.0,
            fwhm_px: None,
            spectral_index: 0.0,
        },
    )]);
    let dir = observation.scratch("numbered-outliers");
    let main = dir.join("main");
    let fields = dir.join("fields");
    fs::create_dir_all(&fields).unwrap();
    let outlier = fields.join("other");
    let definition = dir.join("outlier.txt");
    fs::write(
        &definition,
        format!(
            "imagename={}\nphasecenter=J2000 0deg -30deg\nimsize=40\n",
            outlier.display()
        ),
    )
    .unwrap();
    let geometry = observation.geometry();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": main,
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu", "niter": 0, "restart": true,
    });
    let execute = |values: &serde_json::Value| {
        let (parameters, request) =
            resolve_request(&ImagingParameters(values.as_object().unwrap().clone())).unwrap();
        run(parameters, &request)
    };
    values["outlierfile"] = json!(definition);
    execute(&values).unwrap();
    // The outlier's higher existing scientific name determines the common
    // next suffix. Its unrelated orphan must itself be left intact.
    fs::write(fields.join("other_6.psf"), b"preserve old outlier").unwrap();
    let blocked = fields.join("other_7.pb");
    fs::write(&blocked, b"preserve orphan").unwrap();
    values["restart"] = json!(false);
    let before = snapshot(&dir);
    let error = execute(&values).unwrap_err();
    assert!(error.contains("other_7.pb"), "{error}");
    assert_unchanged(&before, &snapshot(&dir));
    fs::remove_file(blocked).unwrap();
    let before = snapshot(&dir);
    let both = execute(&values).unwrap();
    for (prefix, output) in [(dir.join("main_7"), &both), (fields.join("other_7"), &both)] {
        assert!(
            output
                .artifacts
                .iter()
                .any(|image| image.path == format!("{}.psf", prefix.display()))
        );
    }

    let mut main_only = values.clone();
    main_only.as_object_mut().unwrap().remove("outlierfile");
    execute(&main_only).unwrap();
    execute(&values).unwrap();
    assert!(dir.join("main_8.psf").exists());
    assert!(!fields.join("other_8.psf").exists());
    assert!(dir.join("main_9.psf").exists());
    assert!(fields.join("other_9.psf").exists());
    let after = snapshot(&dir);
    for (path, bytes) in before {
        assert_eq!(after.get(&path), Some(&bytes), "{}", path.display());
    }
}

/// CASA opens each field's image store at its own prefix. Resuming with
/// no new mask keeps the outlier's support and starts from both models.
#[test]
fn same_prefix_restart_reuses_each_fields_model_and_outlier_mask() {
    use super::fixture::{PHASE_CENTER_RAD, Setup};

    let main_component = Component {
        offset_px: [6, -4],
        flux_jy: 2.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let outlier_component = Component {
        offset_px: [70, 7],
        flux_jy: 1.0,
        ..main_component
    };
    let observation =
        Observation::synthesise(&[("main", main_component), ("outlier", outlier_component)]);
    let directory = observation.scratch("continue-outlier");
    fs::create_dir(&directory).unwrap();
    let geometry = observation.geometry();
    let field_centre = Setup::pointing([
        -f64::from(outlier_component.offset_px[0]) * geometry.cell_rad()[0],
        f64::from(outlier_component.offset_px[1]) * geometry.cell_rad()[1],
    ]);
    let definition = directory.join("outliers.txt");
    let field = format!(
        "imagename=field\nimsize=40\nphasecenter=J2000 {}rad {}rad\n",
        field_centre[0], field_centre[1]
    );
    fs::write(
        &definition,
        format!("{field}mask=circle[[20pix,20pix],4pix]\n"),
    )
    .unwrap();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": "main", "outlierfile": definition,
        "imsize": geometry.image_size, "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec")),
        "phasecenter": format!("J2000 {}rad {}rad", PHASE_CENTER_RAD[0], PHASE_CENTER_RAD[1]),
        "niter": 12, "cycleniter": 3, "backend": "cpu", "gain": 0.2,
    });
    let run = |values: &serde_json::Value| {
        let reply = json_run(&directory, values.clone());
        assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    };
    let read = |name: &str| {
        PagedImage::<f32>::open(directory.join(name))
            .unwrap()
            .get()
            .unwrap()
    };
    run(&values);
    let main_before = read("main.model");
    let field_before = read("field.model");
    let support = read("field.mask");
    assert!(main_before.sum() > 0.2 && field_before.sum() > 0.2);
    assert_eq!(support.sum(), 49.0);
    fs::write(&definition, &field).unwrap();
    let beam_of = |name: &str| {
        PagedImage::<f32>::open(directory.join(name))
            .unwrap()
            .image_info()
            .unwrap()
            .beam_set
            .single_beam()
            .unwrap()
    };
    let main_beam = beam_of("main.psf");
    let field_beam = casa_images::GaussianBeam {
        major: 10.0 * geometry.cell_rad()[0],
        minor: 8.0 * geometry.cell_rad()[0],
        position_angle: 0.25,
    };
    {
        let mut psf = PagedImage::<f32>::open(directory.join("field.psf")).unwrap();
        let mut info = psf.image_info().unwrap();
        info.beam_set = casa_images::ImageBeamSet::new(field_beam);
        psf.set_image_info(&info).unwrap();
        psf.save().unwrap();
    }

    // Recompute the dirty products from the two saved models before
    // applying any new components.
    values["niter"] = json!(0);
    run(&values);
    assert_eq!(read("main.model"), main_before);
    assert_eq!(read("field.model"), field_before);
    assert_eq!(read("field.mask"), support);
    assert_eq!(beam_of("main.psf"), main_beam);
    assert_eq!(beam_of("field.psf"), field_beam);
    assert_eq!(beam_of("field.image"), field_beam);

    values["niter"] = json!(4);
    run(&values);
    assert_eq!(read("field.mask"), support);
    let after = read("field.model");
    let mut changed = 0.0;
    for ((before, after), mask) in field_before.iter().zip(&after).zip(support.iter()) {
        if *mask == 0.0 {
            assert_eq!(before, after, "continued CLEAN went outside the saved mask");
        } else {
            changed += (before - after).abs();
        }
    }
    assert!(changed > 0.005, "the outlier did not CLEAN: {changed}");

    check_saved_outlier_psfs(&directory, &values, field_beam);

    // An outlier may already exist when the new main image has never run.
    let mut outlier_only = values.clone();
    outlier_only["imagename"] = json!("new-main");
    outlier_only["niter"] = json!(0);
    run(&outlier_only);
    assert_eq!(read("field.model"), after);
    assert_eq!(beam_of("field.psf"), field_beam);

    fs::write(&definition, field.replace("imsize=40", "imsize=42")).unwrap();
    let before = snapshot(&directory);
    match json_run_with(&directory, values, true) {
        TaskReply::Failed { message, .. } => {
            assert!(
                message.contains("field.") && message.contains("shape mismatch"),
                "{message}"
            );
        }
        reply => panic!("incompatible outlier should fail before publication: {reply:?}"),
    }
    assert_unchanged(&before, &snapshot(&directory));
}

/// Saved outlier sums scale their own residual, and absent inputs leave
/// all earlier products in place.
fn check_saved_outlier_psfs(
    directory: &Path,
    values: &serde_json::Value,
    field_beam: casa_images::GaussianBeam,
) {
    let mut values = values.clone();
    values["niter"] = json!(0);
    let dirty = |values: &serde_json::Value| {
        let reply = json_run(directory, values.clone());
        assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    };
    dirty(&values);
    let read = |name: &str| super::fixture::Product::open(&directory.join(name));
    let expected = read("field.residual").values();

    let mut sum = PagedImage::<f32>::open(directory.join("field.sumwt")).unwrap();
    let doubled = sum.get().unwrap().mapv(|v| 2.0 * v);
    sum.put_slice(&doubled, &[0; 4]).unwrap();
    sum.save().unwrap();
    drop(sum);
    let mut pb = PagedImage::<f32>::open(directory.join("field.pb")).unwrap();
    let pixels = pb.get().unwrap().mapv(|v| v * 0.8);
    pb.put_slice(&pixels, &[0; 4]).unwrap();
    pb.save().unwrap();
    drop(pb);
    let names = ["main", "field"]
        .into_iter()
        .flat_map(|field| {
            ["psf", "sumwt", "pb"].map(move |suffix| directory.join(format!("{field}.{suffix}")))
        })
        .collect::<Vec<_>>();
    let stable = |path: &Path| {
        let mut files = snapshot(path);
        files.retain(|path, _| !path.components().any(|part| part.as_os_str() == "logtable"));
        files
    };
    let saved = names.iter().map(|path| stable(path)).collect::<Vec<_>>();
    values["calcpsf"] = json!(false);
    let missing = directory.join("field.sumwt");
    let held = directory.join("saved-outlier-sums");
    fs::rename(&missing, &held).unwrap();
    let before = snapshot(directory);
    match json_run_with(directory, values.clone(), true) {
        TaskReply::Failed { message, .. } => assert!(message.contains("field.sumwt"), "{message}"),
        reply => panic!("missing outlier sumwt was accepted: {reply:?}"),
    }
    assert_unchanged(&before, &snapshot(directory));
    fs::rename(&held, &missing).unwrap();

    dirty(&values);
    for (path, files) in names.iter().zip(saved) {
        assert_unchanged(&files, &stable(path));
    }
    assert_eq!(read("field.image").restoring_beam(), field_beam);
    let actual = read("field.residual").values();
    let peak = expected.iter().map(|v| v.abs()).fold(0.0_f32, f32::max);
    assert!(peak > 0.01);
    let error = expected
        .iter()
        .zip(&actual)
        .map(|(a, b)| (0.5 * a - b).abs())
        .fold(0.0_f32, f32::max);
    assert!(error <= 1e-6 * peak, "saved outlier sumwt: {error}/{peak}");
    check_outlier_psf_edges(directory, values, field_beam);
}

fn check_outlier_psf_edges(
    directory: &Path,
    mut values: serde_json::Value,
    field_beam: casa_images::GaussianBeam,
) {
    let psf_path = directory.join("field.psf");
    let pixels = super::fixture::Product::open(&psf_path).values();
    let set_beam = |beams| {
        let mut image = PagedImage::<f32>::open(&psf_path).unwrap();
        let mut info = image.image_info().unwrap();
        info.beam_set = beams;
        image.set_image_info(&info).unwrap();
        image.save().unwrap();
    };
    set_beam(casa_images::ImageBeamSet::default());
    values["niter"] = json!(1);
    values["threshold"] = json!("100Jy");
    let run = |values: &serde_json::Value| {
        let reply = json_run(directory, values.clone());
        assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
        let image = super::fixture::Product::open(&psf_path);
        assert_eq!(image.values(), pixels);
        image.restoring_beam()
    };
    let default = run(&values);
    set_beam(casa_images::ImageBeamSet::default());
    values["psfcutoff"] = json!(0.8);
    assert_eq!(
        run(&values),
        default,
        "unlabelled outlier PSF uses the default beam fit"
    );

    check_refilled_field_sum_weight(directory, values);
    set_beam(casa_images::ImageBeamSet::new(field_beam));
}

/// The data pass supplies the weight of a field with an unfilled sum.
fn check_refilled_field_sum_weight(directory: &Path, mut values: serde_json::Value) {
    let field = directory.join("field.sumwt");
    let main = directory.join("main.sumwt");
    let read = |p: &Path| PagedImage::<f32>::open(p).unwrap().get().unwrap();
    let change = |path: &Path, factor: f32| {
        let mut image = PagedImage::<f32>::open(path).unwrap();
        image
            .put_slice(&image.get().unwrap().mapv(|v| v * factor), &[0; 4])
            .unwrap();
        image.save().unwrap();
    };
    // The field was deliberately doubled in check_saved_outlier_psfs.
    let gridded = read(&main);
    assert_eq!(read(&field), gridded.mapv(|weight| 2.0 * weight));
    change(&field, 0.0);
    let before = snapshot(directory);
    values["calcres"] = json!(false);
    match json_run(directory, values.clone()) {
        TaskReply::Failed { message, .. } => assert!(
            message.contains("field.sumwt") && message.contains("nonpositive"),
            "{message}"
        ),
        reply => panic!("a zero sum without initial gridding must fail: {reply:?}"),
    }
    assert_unchanged(&before, &snapshot(directory));

    values["calcres"] = json!(true);
    let resumed = |refilled: &str| {
        let reply = json_run(directory, values.clone());
        let TaskReply::Succeeded { result, .. } = reply else {
            panic!("{reply:?}")
        };
        let sums = result["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|image| image["kind"] == "sumwt")
            .map(|image| {
                Path::new(image["path"].as_str().unwrap())
                    .file_name()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(sums, [refilled], "only the refilled sumwt is published");
    };
    let scientific_table = |path: &Path| {
        let mut files = snapshot(path);
        files.retain(|path, _| {
            path.file_name().unwrap() != "table.lock"
                && !path.components().any(|part| part.as_os_str() == "logtable")
        });
        files
    };
    let main_before = scientific_table(&main);
    resumed("field.sumwt");
    assert_eq!(
        read(&field),
        gridded,
        "zero outlier sumwt is refilled from the gridding pass"
    );
    assert_unchanged(&main_before, &scientific_table(&main));
    change(&field, 1.5);
    change(&main, -1.0);
    let field_before = scientific_table(&field);
    resumed("main.sumwt");
    assert_eq!(
        read(&main),
        gridded,
        "negative main sumwt is refilled from the gridding pass"
    );
    assert_unchanged(&field_before, &scientific_table(&field));
    change(&field, 2.0 / 3.0);
}

/// CASA blanks a shared sky component in earlier fields for prediction
/// and restores it in their published model images after the major cycle.
#[test]
fn same_prefix_restart_predicts_a_shared_component_once() {
    use super::fixture::PHASE_CENTER_RAD;

    let observation = Observation::synthesise(&[(
        "point",
        Component {
            offset_px: [0, 0],
            flux_jy: 1.0,
            fwhm_px: None,
            spectral_index: 0.0,
        },
    )]);
    let directory = observation.scratch("overlap");
    fs::create_dir(&directory).unwrap();
    let geometry = observation.geometry();
    let phase = format!(
        "J2000 {}rad {}rad",
        PHASE_CENTER_RAD[0], PHASE_CENTER_RAD[1]
    );
    let definition = directory.join("outliers.txt");
    fs::write(
        &definition,
        format!("imagename=field\nphasecenter={phase}\n"),
    )
    .unwrap();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": "main", "outlierfile": definition,
        "imsize": geometry.image_size, "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec")),
        "phasecenter": phase, "niter": 0, "backend": "cpu",
    });
    let run = |values: &serde_json::Value| {
        let reply = json_run(&directory, values.clone());
        assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    };
    run(&values);
    let [x, y] = geometry.centre();
    let sample = ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1, 1, 1, 1]), 0.5_f32);
    for name in ["main.model", "field.model"] {
        let mut model = PagedImage::<f32>::open(directory.join(name)).unwrap();
        model.put_slice(&sample, &[x, y, 0, 0]).unwrap();
        model.save().unwrap();
    }
    run(&values);
    let read = |name: &str| {
        PagedImage::<f32>::open(directory.join(name))
            .unwrap()
            .get()
            .unwrap()
    };
    assert_eq!(read("main.model")[[x, y, 0, 0]], 0.5);
    assert_eq!(read("field.model")[[x, y, 0, 0]], 0.5);

    fs::rename(
        directory.join("field.model"),
        directory.join("single.model"),
    )
    .unwrap();
    values.as_object_mut().unwrap().remove("outlierfile");
    values["imagename"] = json!("single");
    run(&values);
    let single = read("single.residual");
    for name in ["main.residual", "field.residual"] {
        let residual = read(name);
        let error: f64 = residual
            .iter()
            .zip(&single)
            .map(|(both, one)| f64::from(both - one).powi(2))
            .sum();
        let power: f64 = single.iter().map(|one| f64::from(*one).powi(2)).sum();
        assert!(
            (error / power).sqrt() < 0.001,
            "{name} subtracts the shared source more than once"
        );
    }
}

/// CASA's updated-mask sequence names a separate input image under the
/// same prefix as the output, after deleting the previous CLEAN mask.
#[test]
fn restart_reads_a_replacement_mask_named_beside_its_products() {
    let point = |offset_px, flux_jy| Component {
        offset_px,
        flux_jy,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise(&[
        ("dim", point([-12, 4], 1.0)),
        ("bright", point([14, -6], 2.0)),
    ]);
    let directory = observation.scratch("replacement-mask");
    fs::create_dir(&directory).unwrap();
    let geometry = observation.geometry();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": "image",
        "imsize": geometry.image_size, "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec")),
        "backend": "cpu", "niter": 2, "savemodel": "none",
    });
    let run = |values: &serde_json::Value| {
        let reply = json_run(&directory, values.clone());
        assert!(matches!(reply, TaskReply::Succeeded { .. }), "{reply:?}");
    };
    run(&values);
    let replacement = directory.join("image.mask.dim.source");
    fs::rename(directory.join("image.mask"), &replacement).unwrap();
    let [middle, _] = geometry.centre();
    let source = {
        let mut image = PagedImage::<f32>::open(&replacement).unwrap();
        let mask = ndarray::ArrayD::from_shape_fn(ndarray::IxDyn(image.shape()), |pixel| {
            f32::from(pixel[0] < middle)
        });
        // Shift the input by one pixel. The named mask reader reprojects
        // it, whereas a saved output must share the requested grid.
        image.set_reference_pixel(0, middle as f64 + 1.0).unwrap();
        image.put_slice(&mask, &[0; 4]).unwrap();
        image.save().unwrap();
        ndarray::ArrayD::from_shape_fn(ndarray::IxDyn(image.shape()), |pixel| {
            f32::from(pixel[0] + 1 < middle)
        })
    };
    let scientific = |path: &Path| {
        let mut files = snapshot(path);
        // CASA's prefix-based task history also appends to the input
        // image's logtable. Compare the scientific table and its masks.
        files.retain(|name, _| !name.starts_with(path.join("logtable")));
        files
    };
    let previous = scientific(&replacement);
    let read = |name: &str| {
        PagedImage::<f32>::open(directory.join(name))
            .unwrap()
            .get()
            .unwrap()
    };
    let initial = read("image.model");
    values["mask"] = json!("image.mask.dim.source");
    values["niter"] = json!(1);
    values["calcpsf"] = json!(false);
    values["calcres"] = json!(false);
    run(&values);
    let final_model = read("image.model");
    assert_eq!(read("image.mask"), source);
    assert_unchanged(&previous, &scientific(&replacement));
    let [x, y] = geometry.centre();
    assert!(final_model[[x - 12, y + 4, 0, 0]] > initial[[x - 12, y + 4, 0, 0]]);
    assert_eq!(
        final_model[[x + 14, y - 6, 0, 0]],
        initial[[x + 14, y - 6, 0, 0]]
    );

    // A real output mask still conflicts with an explicit replacement.
    let previous = snapshot(&directory);
    let reply = json_run_with(&directory, values, true);
    assert!(matches!(reply, TaskReply::Failed { .. }), "{reply:?}");
    assert_unchanged(&previous, &snapshot(&directory));
}

/// CASA starts the next auto-multithresh boundary with the binary CLEAN
/// support on disk. An old isolated pixel survives alongside new support.
#[test]
fn a_saved_automask_seeds_the_first_resumed_boundary() {
    let observation = Observation::synthesise(&[(
        "point",
        Component {
            offset_px: [6, -4],
            flux_jy: 2.0,
            fwhm_px: None,
            spectral_index: 0.0,
        },
    )]);
    let directory = observation.scratch("saved-automask");
    fs::create_dir(&directory).unwrap();
    let geometry = observation.geometry();
    let values = json!({
        "vis": observation.measurement_set(), "imagename": "image",
        "imsize": geometry.image_size, "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec")),
        "backend": "cpu", "usemask": "auto-multithresh", "fastnoise": false,
        "niter": 4, "cycleniter": 2,
    });
    let run = || {
        let result = json_run(&directory, values.clone());
        assert!(matches!(result, TaskReply::Succeeded { .. }), "{result:?}");
    };
    run();
    let path = directory.join("image.mask");
    let sentinel = [8, 10, 0, 0];
    let initial;
    {
        let mut mask = PagedImage::<f32>::open(&path).unwrap();
        initial = mask.get().unwrap();
        assert_eq!(initial[sentinel], 0.0);
        let pixel = ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1, 1, 1, 1]), 1.0_f32);
        mask.put_slice(&pixel, &sentinel).unwrap();
        mask.save().unwrap();
    }
    let model_path = directory.join("image.model");
    let before = PagedImage::<f32>::open(&model_path).unwrap().get().unwrap();
    run();
    let after = PagedImage::<f32>::open(&model_path).unwrap().get().unwrap();
    let mask = PagedImage::<f32>::open(&path).unwrap().get().unwrap();
    assert_eq!(mask[sentinel], 1.0);
    let [centre_x, centre_y] = geometry.centre();
    assert_eq!(mask[[centre_x + 6, centre_y - 4, 0, 0]], 1.0);
    assert!(after.sum() > before.sum());
}
