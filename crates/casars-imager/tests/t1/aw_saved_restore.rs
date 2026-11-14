// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;

use casa_images::PagedImage;
use ndarray::Array4;
use serde_json::json;

use super::{
    super::restart::{assert_unchanged, snapshot},
    two_term_aw_restart,
};

/// CASA restores the principal Taylor residual obtained from the saved PSF
/// moments and the saved, already-normalized residuals. A saved model is
/// already in image units. Neither a new PSF nor a major pass is needed.
#[test]
fn aw_mtmfs_restores_normalized_residuals_with_a_standard_psf() {
    let (observation, mut controls) = two_term_aw_restart();
    let (_, dirty) = observation.image("aw", controls.clone());
    let (_, standard) = observation.image(
        "standard",
        json!({
            "spw": "0,1", "gridder": "standard", "deconvolver": "mtmfs",
            "nterms": 2, "scales": [0], "reffreq": "1.45GHz", "niter": 0
        }),
    );

    let centre = observation.geometry().pixel(super::CENTRE);
    let [x, y] = [centre[0] + 35, centre[1] - 19];
    let pb = dirty.get(".pb.tt0").pixels[[x, y]];
    assert!((0.2..0.95).contains(&pb), "off-axis fixture PB is {pb}");
    let hessian = [0, 1, 2].map(|term| {
        let suffix = format!(".psf.tt{term}");
        fs::remove_dir_all(observation.scratch(&format!("aw{suffix}"))).unwrap();
        fs::rename(
            observation.scratch(&format!("standard{suffix}")),
            observation.scratch(&format!("aw{suffix}")),
        )
        .unwrap();
        standard.get(&suffix).pixels[centre]
    });
    let residual = [0, 1].map(|term| dirty.get(&format!(".residual.tt{term}")).pixels[[x, y]]);
    assert!(residual.iter().any(|value| value.abs() > 1e-5));
    let model = [0.5, -0.25];
    for (term, flux) in model.into_iter().enumerate() {
        let path = observation.scratch(&format!("aw.model.tt{term}"));
        let mut image = PagedImage::<f32>::open(path).unwrap();
        image
            .put_slice(
                &Array4::from_elem((1, 1, 1, 1), flux).into_dyn(),
                &[x, y, 0, 0],
            )
            .unwrap();
        image.save().unwrap();
    }
    let mut residual0 = PagedImage::<f32>::open(observation.scratch("aw.residual.tt0")).unwrap();
    let mask = residual0.default_mask_name().unwrap();
    residual0
        .put_mask_slice(
            &mask,
            &Array4::from_elem((1, 1, 1, 1), false).into_dyn(),
            &[x, y, 0, 0],
        )
        .unwrap();
    residual0.save().unwrap();
    let mut residual1 = PagedImage::<f32>::open(observation.scratch("aw.residual.tt1")).unwrap();
    residual1.unset_default_mask().unwrap();
    residual1.save().unwrap();
    let saved = dirty
        .suffixes()
        .into_iter()
        .filter(|suffix| {
            [".psf", ".weight", ".sumwt", ".pb", ".residual"]
                .iter()
                .any(|prefix| suffix.starts_with(prefix))
        })
        .map(|suffix| {
            let path = observation.scratch(&format!("aw{suffix}"));
            let bytes = snapshot(&path);
            (suffix, path, bytes)
        })
        .collect::<Vec<_>>();
    assert_eq!(saved.len(), 12);

    controls["calcpsf"] = json!(false);
    controls["calcres"] = json!(false);
    let residual_path = observation.scratch("aw.residual.tt1");
    let withheld = observation.scratch("withheld");
    fs::rename(&residual_path, &withheld).unwrap();
    let Err(error) = observation.try_image("aw", controls.clone()) else {
        panic!("restore succeeded without a Taylor residual");
    };
    assert!(error.contains("aw.residual.tt1"), "{error}");
    fs::rename(withheld, residual_path).unwrap();

    let (summary, restored) = observation.image("aw", controls.clone());
    assert_eq!(summary.minor_iterations, 0);
    assert_eq!(summary.major_cycles, 0);
    for (suffix, path, bytes) in &saved {
        assert!(!restored.suffixes().contains(suffix), "rewrote {suffix}");
        assert_unchanged(bytes, &snapshot(path));
    }
    assert!(!restored.get(".image.tt0").mask().unwrap()[[x, y, 0, 0]]);
    assert!(restored.get(".image.tt1").mask().is_none());
    let [a, b, c] = hessian.map(f64::from);
    let [r0, r1] = residual.map(f64::from);
    let determinant = a * c - b * b;
    let principal = [(c * r0 - b * r1) * a, (a * r1 - b * r0) * a];
    for (term, flux) in model.into_iter().enumerate() {
        let image = restored.get(&format!(".image.tt{term}"));
        let expected = f64::from(flux) + principal[term] / determinant;
        assert!(
            (f64::from(image.pixels[[x, y]]) - expected).abs() < 2e-6,
            "Taylor term {term} should restore the saved model and principal residual"
        );
        assert_eq!(
            image.restoring_beam(),
            standard.get(".image.tt0").restoring_beam()
        );
        assert_eq!(
            restored.get(&format!(".model.tt{term}")).pixels[[x, y]],
            flux
        );
    }
}

/// CASA's lattice maximum for alpha uses the active default mask regardless
/// of name. Its image store copies only mask0 to the restored image.
#[test]
fn aw_saved_taylor_alpha_uses_a_user_named_default_residual_mask() {
    let (observation, mut controls) = two_term_aw_restart();
    let (_, dirty) = observation.image("aw", controls.clone());
    let residual_path = observation.scratch("aw.residual.tt0");
    let mut residual = PagedImage::<f32>::open(residual_path).unwrap();
    let mut mask = dirty.get(".residual.tt0").mask().unwrap();
    let image0 = &dirty.get(".image.tt0").pixels;
    let brightest = image0
        .indexed_iter()
        .filter(|&((x, y), _)| mask[[x, y, 0, 0]])
        .max_by(|(_, left), (_, right)| left.total_cmp(right))
        .unwrap();
    let ((x, y), &maximum) = brightest;
    assert!(maximum > 0.0);
    mask[[x, y, 0, 0]] = false;
    let next = image0
        .indexed_iter()
        .filter(|&((x, y), _)| mask[[x, y, 0, 0]])
        .map(|(_, &value)| value)
        .reduce(f32::max)
        .unwrap();
    assert!(maximum > next && next > 0.0);
    residual
        .put_mask_slice("user_mask", &mask, &[0; 4])
        .unwrap();
    residual.set_default_mask("user_mask").unwrap();
    residual.save().unwrap();

    controls["calcpsf"] = json!(false);
    controls["calcres"] = json!(false);
    let (_, restored) = observation.image("aw", controls);
    assert!(restored.get(".image.tt0").mask().is_none());
    let alpha_mask = restored.get(".alpha").mask().unwrap();
    assert!(alpha_mask[[x, y, 0, 0]]);
    let between_thresholds = image0
        .indexed_iter()
        .filter(|&(_, &value)| value > next * 0.1 + 2e-6 && value < maximum * 0.1 - 2e-6)
        .map(|((x, y), _)| [x, y, 0, 0])
        .collect::<Vec<_>>();
    assert!(
        !between_thresholds.is_empty(),
        "the fixture must distinguish the cutoffs"
    );
    for index in between_thresholds {
        assert!(
            alpha_mask[index],
            "alpha must use the named residual mask for the peak"
        );
    }
}

/// CASA cleans the saved residual in image units before the first major
/// cycle, then predicts from the PB-corrected model. A threshold stop
/// before that major cycle preserves the original residuals.
#[test]
fn aw_mtmfs_cleans_saved_residuals_before_its_first_major_cycle() {
    let (observation, mut controls) = two_term_aw_restart();
    controls["pblimit"] = json!(-0.01);
    let (_, dirty) = observation.image("aw", controls.clone());
    let (_, standard) = observation.image(
        "standard",
        json!({
            "spw": "0,1", "gridder": "standard", "deconvolver": "mtmfs",
            "nterms": 2, "scales": [0], "reffreq": "1.45GHz", "niter": 0
        }),
    );
    let centre = observation.geometry().pixel(super::CENTRE);
    let target = [centre[0] + 34, centre[1] - 20];
    let seed = [centre[0] - 28, centre[1] + 26];
    for pixel in [target, seed] {
        let pb = dirty.get(".pb.tt0").pixels[pixel];
        assert!((0.2..0.95).contains(&pb), "off-axis fixture PB is {pb}");
    }
    let hessian = [0, 1, 2].map(|term| {
        let suffix = format!(".psf.tt{term}");
        fs::remove_dir_all(observation.scratch(&format!("aw{suffix}"))).unwrap();
        fs::rename(
            observation.scratch(&format!("standard{suffix}")),
            observation.scratch(&format!("aw{suffix}")),
        )
        .unwrap();
        f64::from(standard.get(&suffix).pixels[centre])
    });
    let [a, b, c] = hessian;
    let component = [0.75_f64, -0.25];
    let saved_residual = [
        (a * component[0] + b * component[1]) / a,
        (b * component[0] + c * component[1]) / a,
    ];
    let saved_model = [0.3_f32, -0.125];
    let mut residuals = Vec::new();
    for term in 0..2 {
        let path = observation.scratch(&format!("aw.residual.tt{term}"));
        let mut residual = PagedImage::<f32>::open(&path).unwrap();
        let shape = residual.shape();
        let mut pixels = ndarray::ArrayD::zeros(ndarray::IxDyn(shape));
        pixels[[target[0], target[1], 0, 0]] = saved_residual[term] as f32;
        residual.put_slice(&pixels, &[0; 4]).unwrap();
        residual.save().unwrap();
        residuals.push((snapshot(&path), path));

        let mut model =
            PagedImage::<f32>::open(observation.scratch(&format!("aw.model.tt{term}"))).unwrap();
        model
            .put_slice(
                &Array4::from_elem((1, 1, 1, 1), saved_model[term]).into_dyn(),
                &[seed[0], seed[1], 0, 0],
            )
            .unwrap();
        model.save().unwrap();
    }
    let saved_psf = observation.scratch("aw.psf.tt0");
    let psf_before = snapshot(&saved_psf);
    controls["calcpsf"] = json!(false);
    controls["calcres"] = json!(false);
    controls["niter"] = json!(1);
    controls["threshold"] = json!("2Jy");
    let mut positive_limit = controls.clone();
    positive_limit["pblimit"] = json!(0.01);
    let Err(error) = observation.try_image("aw", positive_limit) else {
        panic!("saved CLEAN admitted a positive PB mask cutoff");
    };
    assert!(error.contains("#944"), "{error}");
    let residual0 = &residuals[0].1;
    let mut masked = PagedImage::<f32>::open(residual0).unwrap();
    let mut mask = ndarray::ArrayD::from_elem(ndarray::IxDyn(masked.shape()), true);
    mask[[target[0], target[1], 0, 0]] = false;
    masked.put_mask("user_mask", &mask).unwrap();
    masked.set_default_mask("user_mask").unwrap();
    masked.save().unwrap();
    let masked_before = snapshot(residual0);
    let Err(error) = observation.try_image("aw", controls.clone()) else {
        panic!("saved CLEAN admitted a user-masked Taylor residual");
    };
    assert!(error.contains("#944"), "{error}");
    assert_unchanged(&masked_before, &snapshot(residual0));
    masked.unset_default_mask().unwrap();
    masked.remove_mask("user_mask").unwrap();
    masked.save().unwrap();
    residuals[0].0 = snapshot(residual0);

    let (stopped, _) = observation.image("aw", controls.clone());
    assert_eq!((stopped.minor_iterations, stopped.major_cycles), (0, 0));
    for (bytes, path) in &residuals {
        assert_unchanged(bytes, &snapshot(path));
    }

    controls["threshold"] = json!("0.25Jy");
    controls["gain"] = json!(0.1);
    let (report, cleaned) = observation.image("aw", controls);
    assert_eq!((report.minor_iterations, report.major_cycles), (1, 1));
    for term in 0..2 {
        let image = cleaned.get(&format!(".model.tt{term}"));
        let expected = 0.1 * component[term];
        let actual = f64::from(image.pixels[target]);
        assert!(
            (actual - expected).abs() < 2e-6,
            "term {term}: {actual} vs {expected}"
        );
        assert!((image.pixels[seed] - saved_model[term]).abs() < 2e-6);
        let (before, path) = &residuals[term];
        assert_ne!(
            *before,
            snapshot(path),
            "term {term}: residual was not refreshed"
        );
    }
    assert_unchanged(&psf_before, &snapshot(&saved_psf));
}
