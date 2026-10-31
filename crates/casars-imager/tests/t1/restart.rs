// SPDX-License-Identifier: LGPL-3.0-or-later

//! Interim refusal preserves an existing run, not CASA restart parity (#944).

use std::{collections::BTreeMap, fs, path::Path};

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
fn a_second_tclean_preserves_every_previous_product_and_its_summary() {
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
    let before = snapshot(&directory);
    assert!(before.keys().any(|p| p.ends_with("image.summary.json")));
    values["niter"] = json!(40);
    match json_run(&directory, values) {
        TaskReply::Failed { message, .. } => assert!(message.contains("#944"), "{message}"),
        TaskReply::Succeeded { .. } => {
            panic!("restart succeeded before continuation was implemented")
        }
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
