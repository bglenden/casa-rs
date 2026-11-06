// SPDX-License-Identifier: LGPL-3.0-or-later

//! The admission laws of AW projection: an MT-MFS image and a cube.

use serde_json::json;

use super::{
    AMPLE, alone, check, evla_observation, evla_surface, imaging, mtmfs_controls, over_charge, warm,
};

/// A cube keeps each plane's weight image as a mosaic cube does, and its
/// admission covers it (#719). This catalog lies under the automatic bound's
/// floor, as in the MT-MFS law below, so it is not raised here.
#[test]
fn an_aw_cube_stays_within_its_admission() {
    if !alone("aw::an_aw_cube_stays_within_its_admission") {
        return;
    }
    check_aw_cube_admission("0:0", "linear");
}

/// #1121: a nearest-channel AW cube's live heap stays within its combined
/// admission when a selected window contains more than one channel.
#[test]
fn a_multichannel_nearest_aw_cube_stays_within_its_admission() {
    if !alone("aw::a_multichannel_nearest_aw_cube_stays_within_its_admission") {
        return;
    }
    check_aw_cube_admission("0", "nearest");
}

fn check_aw_cube_admission(spw: &str, interpolation: &str) {
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = evla_observation(root.path());
    let surface = evla_surface(root.path());
    warm(&measurement_set, root.path());
    let values = json!({
        "vis": measurement_set,
        "imagename": root.path().join("aw-cube"),
        "imsize": 1024,
        "cell": "10arcsec",
        "spw": spw,
        "specmode": "cube",
        "interpolation": interpolation,
        "deconvolver": "hogbom",
        "weighting": "natural",
        "niter": 200,
        "cycleniter": 50,
        "gain": 0.2,
        "threshold": "1mJy",
        "gridder": "awproject",
        "wprojplanes": 32,
        "aw_cf_source": "native-evla",
        "native_cf_cache": root.path().join("native-cf"),
        "evla_surface": surface,
        "native_cf_policy": "generate-missing",
        "native_cf_working_size": 2048,
        "native_cf_oversampling": 4,
        "native_cf_cache_bytes": 2_147_483_648_u64,
        "native_cf_maximum_cells": 128,
        "cf_resident_mb": "auto",
        "normtype": "flatnoise",
        "pbcor": true,
    });
    let mut breaches = Vec::new();
    check(
        &mut breaches,
        "aw cube",
        &imaging::request(values),
        1,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// #738: native A-projection MT-MFS at 1024², generating its catalog and
/// then reusing it, is admitted close to what it holds. Generation is
/// admitted before it starts: uncharged, it held 274 MB beyond the
/// admitted memory. A catalog whose groups all fit its bound is charged
/// those groups: charged the bound of `cf_resident_mb = 512`, the run
/// reserved 974 MB for a peak of 398 MB. An automatic bound
/// (`cf_resident_mb = auto`, #449) is admitted by the same path. This
/// catalog's 15.6 MB of cells lie under its 256 MiB floor, so the bound is
/// not raised here (#781 review): `aw::cache`'s laws pin the raise, and B3
/// measured it end to end.
#[test]
fn aw_mtmfs_is_admitted_close_to_what_it_holds() {
    if !alone("aw::aw_mtmfs_is_admitted_close_to_what_it_holds") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = evla_observation(root.path());
    let surface = evla_surface(root.path());
    warm(&measurement_set, root.path());
    let cache = root.path().join("native-cf");
    let mut breaches = Vec::new();
    for (label, resident) in [
        ("aw generating", json!(512)),
        ("aw reusing", json!(512)),
        ("aw automatic", json!("auto")),
    ] {
        let mut values = json!({
            "vis": measurement_set,
            "imagename": root.path().join(label.replace(' ', "-")),
            "imsize": 1024,
            "cell": "10arcsec",
            "weighting": "natural",
            "niter": 200,
            "cycleniter": 50,
            "gain": 0.2,
            "threshold": "1mJy",
            "gridder": "awproject",
            "wprojplanes": 32,
            "aw_cf_source": "native-evla",
            "native_cf_cache": cache,
            "evla_surface": surface,
            "native_cf_policy": "generate-missing",
            "native_cf_working_size": 2048,
            "native_cf_oversampling": 4,
            "native_cf_cache_bytes": 2_147_483_648_u64,
            "native_cf_maximum_cells": 128,
            "cf_resident_mb": resident,
            "normtype": "flatnoise",
            "pbcor": true,
        });
        values.as_object_mut().expect("AW controls").extend(
            mtmfs_controls()
                .as_object()
                .expect("MT-MFS controls")
                .clone(),
        );
        let measured = check(&mut breaches, label, &imaging::request(values), 1, AMPLE);
        breaches.extend(over_charge(label, &measured));
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
