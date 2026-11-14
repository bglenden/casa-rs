// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Keep the second channel's mask through the controller and publish its
/// restored product without materializing a second full image family.
#[test]
fn mosaic_cube_saved_residual_masks_stay_within_admission() {
    if !alone("mosaic_cube_restart::mosaic_cube_saved_residual_masks_stay_within_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let ms = observation_as(root.path(), 2, "ALMA");
    warm(&ms, root.path());
    let prefix = root.path().join("mosaic");
    let initial = cube(
        &ms,
        &prefix,
        json!({
            "gridder": "mosaic", "deconvolver": "hogbom", "channel_count": 2,
            "niter": 0, "pblimit": 0.2, "pbcor": true,
        }),
    );
    let mut breaches = Vec::new();
    drop(check(
        &mut breaches,
        "two-channel saved seed",
        &initial,
        2,
        AMPLE,
    ));
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    let mut resumed = initial.with_tclean_restart().unwrap();
    resumed.calcpsf = false;
    resumed.calcres = false;
    resumed.niter = 2;
    resumed.threshold = 1.0e6;
    let mut residual =
        casa_images::PagedImage::<f32>::open(prefix.with_extension("residual")).unwrap();
    let [nx, ny, _, _] = residual.shape().try_into().unwrap();
    let [x, y] = [nx / 2, ny / 2];
    let coordinate = [x, y, 0, 1];
    let flagged = ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1; 4]), false);
    let peak = ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1; 4]), 1.0e7_f32);
    assert!(residual.default_mask_name().is_some());
    residual.put_slice(&peak, &coordinate).unwrap();
    residual
        .put_mask_slice("mask0", &flagged, &coordinate)
        .unwrap();
    residual.save().unwrap();
    drop(residual);
    let measured = check(&mut breaches, "saved mosaic cube masks", &resumed, 2, AMPLE);
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert_eq!(measured.outcome.unwrap().major_cycle_count, 0);

    let mut residual =
        casa_images::PagedImage::<f32>::open(prefix.with_extension("residual")).unwrap();
    let blank = ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1; 4]), 0.0_f32);
    residual.put_slice(&blank, &coordinate).unwrap();
    residual.save().unwrap();
    drop(residual);
    resumed.threshold = 0.0;
    let measured = check(
        &mut breaches,
        "refreshed mosaic cube model",
        &resumed,
        2,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert!(measured.outcome.unwrap().major_cycle_count > 0);

    resumed.calcpsf = true;
    let measured = check(
        &mut breaches,
        "rebuilt mosaic cube PSF with a saved residual and model",
        &resumed,
        2,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert!(measured.outcome.unwrap().major_cycle_count > 0);
}
