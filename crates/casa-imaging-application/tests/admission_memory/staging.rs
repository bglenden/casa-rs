// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product staging admission laws.

use super::*;

/// The product owner keeps its staging admission through publication, even
/// when a single product plane is larger than the fixed process allowance.
#[test]
fn a_large_product_plane_stays_within_its_staging_admission() {
    if !alone("staging::a_large_product_plane_stays_within_its_staging_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 2);
    warm(&measurement_set, root.path());
    let dirty = request(
        &measurement_set,
        &root.path().join("large-product"),
        1024,
        json!({
            "specmode": "cube", "outframe": "TOPO", "spw": "0",
            "channel_start": 0, "channel_count": 2,
            "start": "0", "width": "1", "niter": 0,
        }),
    );
    let measured = measure(&dirty, 2, AMPLE);
    assert!(
        measured.outcome.is_ok(),
        "large cube: {:?}",
        measured.outcome.err()
    );
    assert!(
        admission_breach("large product staging", &measured).is_none(),
        "{}",
        admission_breach("large product staging", &measured).unwrap_or_default()
    );
}

/// Keep the normal Taylor product admission while staging one previously
/// saved PSF with a fitted beam.
#[test]
fn a_saved_taylor_psf_beam_stays_within_staging_admission() {
    if !alone("staging::a_saved_taylor_psf_beam_stays_within_staging_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let ms = observation(root.path(), 4);
    warm(&ms, root.path());
    let prefix = root.path().join("taylor");
    let initial = request(
        &ms,
        &prefix,
        512,
        json!({"deconvolver": "mtmfs", "nterms": 2, "niter": 0, "restoration": false}),
    );
    assert!(measure(&initial, 2, AMPLE).outcome.is_ok());
    let path = prefix.with_extension("psf.tt0");
    let mut psf = casa_images::PagedImage::<f32>::open(&path).unwrap();
    assert!(psf.remove_restoring_beams().unwrap());
    psf.save().unwrap();
    drop(psf);

    let mut restart = initial.with_tclean_restart().unwrap();
    restart.calcpsf = false;
    restart.calcres = false;
    restart.restoration = true;
    let mut breaches = Vec::new();
    let measured = check(&mut breaches, "saved Taylor PSF beam", &restart, 2, AMPLE);
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert_eq!(measured.outcome.unwrap().major_cycle_count, 0);
    let psf = casa_images::PagedImage::<f32>::open(path).unwrap();
    assert!(!psf.image_info().unwrap().beam_set.is_empty());
}
