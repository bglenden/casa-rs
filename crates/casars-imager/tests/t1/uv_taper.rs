// SPDX-License-Identifier: LGPL-3.0-or-later

//! `uvtaper` (#841 Phase 6b): CASA's Gaussian taper of the imaging weights.
//! An image-plane FWHM θ multiplies the weights by a uv Gaussian whose
//! transform is a Gaussian of FWHM θ, so the restoring beam widens past both
//! the untapered beam and θ, and a point source keeps its flux. For a
//! Gaussian PSF the width would be √(beam² + θ²); natural weighting's
//! main lobe fits wider, by 1.19 to 1.23 times on CASA's own `refim_point`
//! (60″ taper: 97.2″ × 91.8″ against 79.1″ × 77.0″). Agreement with CASA's
//! tapered images is the T1.5 rows'.

use serde_json::json;

use super::fixture::{Component, Observation, component_pixel, peak};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
const ARCSEC: f64 = std::f64::consts::PI / (180.0 * 3600.0);

#[test]
fn a_uv_taper_widens_the_beam_and_keeps_the_point_flux() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold": "0.005Jy",
    });
    let (_, plain) = observation.image("untapered", controls.clone());
    let fwhm = 0.1 * ARCSEC;
    let mut tapered = controls.clone();
    tapered["uvtaper"] = json!(["0.1arcsec"]);
    let (_, tapered) = observation.image("tapered", tapered);
    let before = plain.get(".image").restoring_beam();
    let after = tapered.get(".image").restoring_beam();
    eprintln!(
        "T1 uvtaper: beam [{:.4}, {:.4}]″ untapered, [{:.4}, {:.4}]″ tapered",
        before.major / ARCSEC,
        before.minor / ARCSEC,
        after.major / ARCSEC,
        after.minor / ARCSEC,
    );
    for (axis, untapered, tapered) in [
        ("major", before.major, after.major),
        ("minor", before.minor, after.minor),
    ] {
        let gaussian = untapered.hypot(fwhm);
        assert!(
            tapered > untapered.max(fwhm) && tapered < 1.4 * gaussian,
            "{axis}: {tapered} rad tapered, {untapered} rad untapered, √(beam² + θ²) = {gaussian}"
        );
    }
    let (peak_jy, peak_pixel, _) = peak(&tapered.get(".image").pixels);
    assert_eq!(peak_pixel, component_pixel(POINT));
    assert!(
        (f64::from(peak_jy) - POINT.flux_jy).abs() < 0.02 * POINT.flux_jy,
        "tapered point peak {peak_jy} Jy/beam"
    );

    // An elliptical taper turns the beam from its untapered position angle
    // toward the taper's.
    let mut elliptical = controls;
    elliptical["uvtaper"] = json!(["0.3arcsec", "0.1arcsec", "30deg"]);
    let (_, elliptical) = observation.image("elliptical", elliptical);
    let untapered = before.position_angle.to_degrees();
    let measured = elliptical
        .get(".image")
        .restoring_beam()
        .position_angle
        .to_degrees();
    eprintln!("T1 uvtaper: beam at {untapered:.2}° untapered, {measured:.2}° with a 30° taper");
    assert!(
        measured > untapered + 5.0 && measured < 30.0,
        "the beam turned from {untapered}° to {measured}°, not toward the taper's 30°"
    );
}
