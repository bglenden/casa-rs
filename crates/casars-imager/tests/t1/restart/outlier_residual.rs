// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Each field supplies its own residual pixels and default mask. A
/// threshold stop before the first major cycle keeps the saved tables.
#[test]
fn same_prefix_restart_reuses_both_fields_saved_residuals() {
    check_saved_outlier_residuals(false);
}

#[test]
fn same_prefix_restart_rebuilds_both_psfs_and_uses_saved_residuals() {
    check_saved_outlier_residuals(true);
}

fn check_saved_outlier_residuals(rebuild_psfs: bool) {
    let observation = Observation::synthesise(&[(
        "point",
        Component {
            offset_px: [0, 0],
            flux_jy: 1.0,
            fwhm_px: None,
            spectral_index: 0.0,
        },
    )]);
    let directory = observation.scratch("saved-outlier-residuals");
    fs::create_dir(&directory).unwrap();
    let definition = directory.join("outlier.txt");
    fs::write(
        &definition,
        "imagename=field\nimsize=40\nphasecenter=J2000 18h00m00s -23d00m10s\n",
    )
    .unwrap();
    let geometry = observation.geometry();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": "main", "outlierfile": definition,
        "imsize": geometry.image_size, "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec")),
        "niter": 2, "cycleniter": 1, "backend": "cpu", "fullsummary": true,
    });
    let run = |values: &serde_json::Value| {
        let reply = json_run(&directory, values.clone());
        let TaskReply::Succeeded { value, result, .. } = reply else {
            panic!("{reply:?}")
        };
        let record = serde_json::to_value(value).unwrap();
        let counts = ["iterdone", "nmajordone", "stopcode"].map(|key| {
            record["entries"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["key"] == key)
                .unwrap()["value"]["value"]
                .as_i64()
                .unwrap()
        });
        if !values["calcres"].as_bool().unwrap_or(true) && (counts[1] == 0 || counts[0] == 0) {
            assert!(
                result["artifacts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|image| image["kind"] != "residual"),
                "a run without gridding must not publish a residual table"
            );
        }
        counts
    };
    run(&values);

    let read = |name: &str| PagedImage::<f32>::open(directory.join(name)).unwrap();
    let reshape = |name: &str, peak: f32, excluded: Option<([usize; 4], f32)>| {
        let mut image = read(name);
        let [nx, ny, _, _] = image.shape().try_into().unwrap();
        let mut pixels = image.get().unwrap().mapv(|_| 0.0);
        pixels[[nx / 2, ny / 2, 0, 0]] = peak;
        let mut mask = pixels.mapv(|_| true);
        if let Some((index, value)) = excluded {
            pixels[index] = value;
            mask[index] = false;
        }
        image.put_slice(&pixels, &[0; 4]).unwrap();
        image.put_mask_slice("mask0", &mask, &[0; 4]).unwrap();
        image.set_default_mask("mask0").unwrap();
        image.save().unwrap();
        mask
    };
    let main_mask = reshape("main.residual", 2.0, None);
    let field_mask = reshape("field.residual", 3.0, Some(([21, 20, 0, 0], 8.0)));
    let saved = ["main.residual", "field.residual"].map(|name| directory.join(name));
    let stable = |path: &Path| {
        let mut files = snapshot(path);
        files.retain(|path, _| {
            path.file_name().unwrap() != "table.lock"
                && !path.components().any(|part| part.as_os_str() == "logtable")
        });
        files
    };
    let before = saved.each_ref().map(|path| stable(path));
    values["calcpsf"] = json!(rebuild_psfs);
    values["calcres"] = json!(false);
    values["threshold"] = json!("2.5Jy");
    let hidden = directory.join("withheld-residual");
    fs::rename(&saved[1], &hidden).unwrap();
    let no_outlier = snapshot(&directory);
    match json_run_with(&directory, values.clone(), true) {
        TaskReply::Failed { message, .. } => {
            assert!(message.contains("field.residual"), "{message}")
        }
        reply => panic!("missing saved outlier residual was accepted: {reply:?}"),
    }
    assert_unchanged(&no_outlier, &snapshot(&directory));
    fs::rename(hidden, &saved[1]).unwrap();

    // The retained 3 Jy outlier peak is above 2.5 Jy. The masked 8 Jy
    // pixel is excluded from the controller threshold, as in CASA.
    values["threshold"] = json!("4Jy");
    assert_eq!(run(&values), [0, 0, 2], "saved mask governs the first stop");
    for (path, bytes) in saved.iter().zip(&before) {
        assert_unchanged(bytes, &stable(path));
    }
    assert_eq!(read("main.image").get_mask().unwrap().unwrap(), main_mask);
    assert_eq!(read("field.image").get_mask().unwrap().unwrap(), field_mask);
    values["niter"] = json!(0);
    assert_eq!(run(&values), [0, 1, 3], "restoration-only summary");
    for (path, bytes) in saved.iter().zip(&before) {
        assert_unchanged(bytes, &stable(path));
    }

    values["niter"] = json!(2);
    values["threshold"] = json!("2.5Jy");
    let counters = run(&values);
    assert!(
        counters[0] > 0 && counters[1] > 0,
        "saved outlier peak entered CLEAN: {counters:?}"
    );
    assert_eq!(
        read("field.residual").get_mask().unwrap().unwrap(),
        field_mask
    );
    assert_eq!(read("field.image").get_mask().unwrap().unwrap(), field_mask);
    assert_ne!(
        stable(&saved[1]),
        before[1],
        "the major cycle replaces the residual"
    );

    if rebuild_psfs {
        check_rebuilt_field_auxiliaries(&directory, &values, &field_mask, run);
        return;
    }
    let pb_mask = field_mask.mapv(|pixel| !pixel);
    let mut pb = read("field.pb");
    pb.put_mask_slice("mask0", &pb_mask, &[0; 4]).unwrap();
    pb.set_default_mask("mask0").unwrap();
    pb.save().unwrap();
    let mut residual = read("field.residual");
    residual.remove_mask("mask0").unwrap();
    residual.save().unwrap();
    drop((pb, residual));
    values["threshold"] = json!("0Jy");
    assert!(run(&values)[1] > 0);
    assert_eq!(
        read("field.residual").get_mask().unwrap().unwrap(),
        pb_mask,
        "first major copies the saved PB mask if the residual has none"
    );
    assert_eq!(read("field.image").get_mask().unwrap().unwrap(), pb_mask);

    fs::remove_dir_all(directory.join("field.pb")).unwrap();
    fs::remove_dir_all(directory.join("field.image")).unwrap();
    values["niter"] = json!(0);
    run(&values);
    assert!(
        read("field.image").get_mask().unwrap().is_none(),
        "a new restored image has no mask without a saved PB"
    );
    let mut restored = read("field.image");
    restored
        .put_mask_slice("mask0", &field_mask, &[0; 4])
        .unwrap();
    restored.set_default_mask("mask0").unwrap();
    restored.save().unwrap();
    drop(restored);
    run(&values);
    assert_eq!(
        read("field.image").get_mask().unwrap().unwrap(),
        field_mask,
        "without a PB, restoration keeps the previous image mask"
    );
}

fn check_rebuilt_field_auxiliaries(
    directory: &Path,
    controls: &serde_json::Value,
    field_mask: &ndarray::ArrayD<bool>,
    run: impl Fn(&serde_json::Value) -> [i64; 3],
) {
    let read = |suffix: &str| PagedImage::<f32>::open(directory.join(suffix)).unwrap();
    let main_sum = read("main.sumwt").get().unwrap();
    let field_sum = read("field.sumwt").get().unwrap();
    let edited_main = main_sum.mapv(|sum| 0.75 * sum);
    for (name, pixels) in [
        ("main.sumwt", edited_main.clone()),
        ("field.sumwt", field_sum.mapv(|_| 0.0)),
    ] {
        let mut image = read(name);
        image.put_slice(&pixels, &[0; 4]).unwrap();
        image.save().unwrap();
    }

    let sum_path = directory.join("main.sumwt");
    let scientific_table = |path: &Path| {
        let mut files = snapshot(path);
        files.retain(|name, _| {
            name.file_name().unwrap() != "table.lock"
                && !name.components().any(|part| part.as_os_str() == "logtable")
        });
        files
    };
    #[cfg(unix)]
    let original_sum_header = fs::metadata(sum_path.join("table.dat")).unwrap();
    let before = scientific_table(&sum_path);

    let mut old = read("field.image");
    let old_mask = field_mask.mapv(|pixel| !pixel);
    old.put_mask_slice("mask0", &old_mask, &[0; 4]).unwrap();
    old.save().unwrap();
    drop(old);
    fs::remove_dir_all(directory.join("field.pb")).unwrap();

    let mut controls = controls.clone();
    controls["threshold"] = json!("100Jy");
    assert_eq!(run(&controls), [0, 0, 2]);
    assert_eq!(read("main.sumwt").get().unwrap(), edited_main);
    assert_unchanged(&before, &scientific_table(&sum_path));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let header = fs::metadata(sum_path.join("table.dat")).unwrap();
        assert_eq!(
            original_sum_header.ino(),
            header.ino(),
            "retained sum table was replaced"
        );
    }
    assert_eq!(read("field.sumwt").get().unwrap(), field_sum);
    assert_eq!(
        read("field.image").get_mask().unwrap().as_ref(),
        Some(field_mask)
    );
    let generated = read("field.pb").get_mask().unwrap().unwrap();

    let mut pb = read("field.pb");
    let mut residual = read("field.residual");
    pb.remove_mask("mask0").unwrap();
    residual.remove_mask("mask0").unwrap();
    pb.save().unwrap();
    residual.save().unwrap();
    drop((pb, residual));
    controls["threshold"] = json!("0Jy");
    assert!(run(&controls)[1] > 0);
    for name in ["field.pb", "field.residual", "field.image"] {
        assert_eq!(
            read(name).get_mask().unwrap().as_ref(),
            Some(&generated),
            "{name} takes the rebuilt PB mask"
        );
    }

    let mut pb = read("field.pb");
    pb.put_mask_slice("mask0", &old_mask, &[0; 4]).unwrap();
    pb.save().unwrap();
    drop(pb);
    controls["threshold"] = json!("100Jy");
    assert_eq!(run(&controls), [0, 0, 2]);
    assert_eq!(
        read("field.pb").get_mask().unwrap().as_ref(),
        Some(&old_mask)
    );
}

/// CASA uses custom residual support for the controller. Restoration
/// copies only mask0 when a PB exists; without a PB it keeps the prior mask.
#[test]
fn saved_outlier_user_pixel_masks_follow_casa_restoration() {
    let observation = Observation::synthesise(&[(
        "point",
        Component {
            offset_px: [0, 0],
            flux_jy: 1.0,
            fwhm_px: None,
            spectral_index: 0.0,
        },
    )]);
    let directory = observation.scratch("custom-field-masks");
    fs::create_dir(&directory).unwrap();
    let definition = directory.join("outlier.txt");
    fs::write(
        &definition,
        "imagename=field\nimsize=40\nphasecenter=J2000 18h00m00s -23d00m10s\n",
    )
    .unwrap();
    let geometry = observation.geometry();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": "main", "outlierfile": definition,
        "imsize": geometry.image_size, "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec")),
        "niter": 2, "cycleniter": 1, "backend": "cpu", "fullsummary": true,
    });
    let run = |values: &serde_json::Value| {
        let reply = json_run(&directory, values.clone());
        let TaskReply::Succeeded { value, .. } = reply else {
            panic!("{reply:?}");
        };
        let record = serde_json::to_value(value).unwrap();
        ["iterdone", "nmajordone", "stopcode"].map(|key| {
            record["entries"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["key"] == key)
                .unwrap()["value"]["value"]
                .as_i64()
                .unwrap()
        })
    };
    run(&values);
    let open = |name: &str| PagedImage::<f32>::open(directory.join(name)).unwrap();
    let mut main = open("main.residual");
    let zero = main.get().unwrap().mapv(|_| 0.0);
    main.put_slice(&zero, &[0; 4]).unwrap();
    main.save().unwrap();
    drop(main);

    let mut residual = open("field.residual");
    let mut pixels = residual.get().unwrap().mapv(|_| 0.0);
    let bright = [21, 20, 0, 0];
    pixels[bright] = 8.0;
    pixels[[20, 20, 0, 0]] = 2.0;
    let mut mask = pixels.mapv(|_| true);
    mask[bright] = false;
    residual.put_slice(&pixels, &[0; 4]).unwrap();
    residual.remove_mask("mask0").unwrap();
    residual.put_mask("saved_support", &mask).unwrap();
    residual.set_default_mask("saved_support").unwrap();
    residual.save().unwrap();
    drop(residual);

    values["calcpsf"] = json!(false);
    values["calcres"] = json!(false);
    values["threshold"] = json!("4Jy");
    let path = directory.join("field.residual");
    let stable = || {
        let mut files = snapshot(&path);
        files.retain(|path, _| {
            path.file_name().unwrap() != "table.lock"
                && !path.components().any(|part| part.as_os_str() == "logtable")
        });
        files
    };
    let before = stable();
    assert_eq!(run(&values), [0, 0, 2], "custom mask controls the stop");
    assert_unchanged(&before, &stable());
    assert!(
        open("field.image").default_mask_name().is_none(),
        "restore does not copy custom residual masks"
    );

    values["threshold"] = json!("0Jy");
    values["pblimit"] = json!(-0.2);
    assert!(run(&values)[1] > 0);
    let residual = open("field.residual");
    assert_eq!(
        residual.default_mask_name().as_deref(),
        Some("saved_support")
    );
    assert_eq!(residual.get_mask().unwrap().unwrap(), mask);
    assert!(open("field.image").default_mask_name().is_none());
    drop(residual);

    let mut prior = open("field.image");
    let prior_mask = mask.mapv(|valid| !valid);
    prior.put_mask("prior_support", &prior_mask).unwrap();
    prior.set_default_mask("prior_support").unwrap();
    prior.save().unwrap();
    drop(prior);
    values["niter"] = json!(0);
    for with_pb in [true, false] {
        if !with_pb {
            fs::remove_dir_all(directory.join("field.pb")).unwrap();
        }
        run(&values);
        let restored = open("field.image");
        assert_eq!(
            restored.default_mask_name().as_deref(),
            Some("prior_support")
        );
        assert_eq!(restored.get_mask().unwrap().unwrap(), prior_mask);
    }
    let path = directory.join("field.image");
    let options = casa_tables::TableOptions::new(&path);
    let mut table = casa_tables::Table::open_metadata_only(options.clone()).unwrap();
    let Some(casa_types::Value::Record(masks)) = table.keywords_mut().get_mut("masks") else {
        panic!("expected a saved mask record");
    };
    let support = masks.get("prior_support").unwrap().clone();
    // Casacore's defineRegion stores its logical name independently of
    // the mask table path. Leave the mask in prior_support and the history
    // table in logtable, like an image from that API.
    masks.upsert("logtable", support);
    table.keywords_mut().upsert(
        "Image_defaultmask",
        casa_types::Value::Scalar(casa_types::ScalarValue::String("logtable".into())),
    );
    table.save_metadata_only(options).unwrap();
    drop(table);
    let image = open("field.image");
    assert_eq!(image.get_mask().unwrap().unwrap(), prior_mask);
    drop(image);
    let snapshot_products = || {
        let mut files = snapshot(&directory);
        files.retain(|path, _| path.file_name().unwrap() != "table.lock");
        files
    };
    let before = snapshot_products();
    match json_run_with(&directory, values, false) {
        TaskReply::Failed { message, .. } => {
            assert!(
                message.contains("invalid output pixel-mask name"),
                "{message}"
            );
            assert!(message.contains("logtable"), "{message}");
        }
        reply => panic!("reserved output mask name was accepted: {reply:?}"),
    }
    assert_unchanged(&before, &snapshot_products());
}
