// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stokes parameters the data's correlations do not feed, as CASA images
//! them: `stokes='IQUV'` of parallel-hand data writes all four planes, with
//! Q and U empty and of zero `sumwt` (`StokesImageUtil::ToStokesSumWt`
//! takes `min(RL, LR)`), I and V of `min(RR, LL)`, every PSF plane Stokes
//! I's, and Stokes I as an I-only run images it (CASA 6.7.6.14's
//! `test_stokes_weighting`).

use ndarray::{ArrayD, Axis};
use serde_json::json;

use super::fixture::{Component, Observation};

const POINT: Component = Component {
    offset_px: [6, -4],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

fn plane(values: &ArrayD<f32>, stokes: usize) -> ArrayD<f32> {
    values.index_axis(Axis(2), stokes).to_owned()
}

fn nrms(left: &ArrayD<f32>, right: &ArrayD<f32>) -> f64 {
    let (error, power) =
        left.iter()
            .zip(right)
            .fold((0.0_f64, 0.0_f64), |(error, power), (a, b)| {
                let (a, b) = (f64::from(*a), f64::from(*b));
                (error + (a - b).powi(2), power + b * b)
            });
    (error / power).sqrt()
}

#[test]
fn full_stokes_of_parallel_hands_images_empty_cross_hand_planes() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let (_, i_only) = observation.image("i", json!({"niter": 10}));
    let (_, full) = observation.image("iquv", json!({"niter": 10, "stokes": "IQUV"}));
    let sumwt = full.get(".sumwt").channel_values();
    assert_eq!(sumwt.len(), 4);
    assert!(sumwt[0] > 0.0);
    assert_eq!(sumwt, [sumwt[0], 0.0, 0.0, sumwt[0]]);
    for suffix in [".residual", ".model", ".image"] {
        let values = full.get(suffix).values();
        for stokes in [1, 2] {
            assert!(
                plane(&values, stokes).iter().all(|value| *value == 0.0),
                "{suffix} plane {stokes} is empty"
            );
        }
        let i = plane(&i_only.get(suffix).values(), 0);
        let error = nrms(&plane(&values, 0), &i);
        assert!(error < 1e-6, "{suffix} Stokes I NRMS {error:e}");
    }
    let psf = full.get(".psf").values();
    for stokes in 1..4 {
        assert_eq!(plane(&psf, stokes), plane(&psf, 0), "PSF plane {stokes}");
    }
}
