// SPDX-License-Identifier: LGPL-3.0-or-later

//! Ordinary tclean writes its PB; casa-rs's explicit opt-out remains available.

use casa_images::PagedImage;
use casa_provider_contracts::TaskReply;
use serde_json::json;

use super::{
    casa_return::json_run,
    fixture::{Component, Observation, Setup},
};

#[test]
fn a_default_dirty_run_writes_a_readable_pb_and_an_explicit_opt_out_does_not() {
    let point = Component {
        offset_px: [0, 0],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise(&[("point", point)]);
    let directory = observation.scratch("pb-default");
    std::fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    let base = json!({"vis": observation.measurement_set(), "niter": 0, "backend": "cpu",
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec"))});
    for (name, opt_out) in [("default", false), ("opt-out", true)] {
        let mut values = base.clone();
        values["imagename"] = json!(directory.join(name));
        if opt_out {
            values["write_pb"] = json!(false);
        }
        assert!(matches!(
            json_run(&directory, values),
            TaskReply::Succeeded { .. }
        ));
        let path = directory.join(format!("{name}.pb"));
        if opt_out {
            assert!(!path.exists());
        } else {
            let pb = PagedImage::<f32>::open(path).expect("persisted PB reopens");
            assert_eq!(&pb.shape()[..2], &geometry.image_size);
            assert!(pb.get().unwrap().iter().all(|value| value.is_finite()));
        }
    }
}

#[test]
fn alma_labelled_seven_metre_dishes_write_the_aca_primary_beam_by_default() {
    let point = Component {
        offset_px: [0, 0],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let mut beams = Vec::new();
    for telescope in ["ALMA", "ACA"] {
        let mut setup = Setup::vla_q_band(2);
        setup.telescope = telescope.to_string();
        for antenna in &mut setup.antennas {
            antenna.dish_diameter_m = 7.0;
        }
        let observation = Observation::synthesise_setup(setup, &[("point", point)]);
        let directory = observation.scratch("seven-metre");
        std::fs::create_dir_all(&directory).unwrap();
        let geometry = observation.geometry();
        let values = json!({
            "vis": observation.measurement_set(), "imagename": directory.join("image"),
            "niter": 0, "backend": "cpu", "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec")),
        });
        let reply = json_run(&directory, values);
        assert!(
            matches!(reply, TaskReply::Succeeded { .. }),
            "{telescope}: {reply:?}"
        );
        beams.push(
            PagedImage::<f32>::open(directory.join("image.pb"))
                .unwrap()
                .get()
                .unwrap(),
        );
    }
    assert_eq!(beams[0].shape(), beams[1].shape());
    assert!(
        beams
            .iter()
            .all(|beam| beam.iter().all(|value| value.is_finite()))
    );
    let worst = beams[0]
        .iter()
        .zip(&beams[1])
        .fold(0.0_f32, |m, (a, b)| m.max((a - b).abs()));
    assert!(worst <= 1e-6, "ALMA/ACA 7 m PB difference {worst:e}");
}

/// CASA selects the VLA beam using the first cube plane in LSRK, then
/// evaluates that beam at each output plane's BARY frequency. The LSRK
/// frequency is 999.99 MHz while BARY is 1000.030 MHz, on opposite sides
/// of the L-band boundary (CASA 6.7.6.14, issue #1138).
#[test]
fn bary_cube_uses_the_lsrk_band_and_bary_plane_frequencies() {
    let mut setup = Setup::vla_q_band(5);
    setup.spectral_windows[0].start_frequency_hz = 999_990_000.0;
    setup.spectral_windows[0].channel_width_hz = 50.0e6;
    setup.geometry = super::fixture::Geometry::square(100, 8.0);
    setup.noise_jy = 0.0;
    let point = Component {
        offset_px: [50, 50],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise_setup(setup, &[("point", point)]);
    let (_, products) = observation.image(
        "bary-vla-band",
        json!({
            "specmode": "cube", "spw": "0", "start": 0, "width": 1, "nchan": 5,
            "niter": 1, "outframe": "BARY", "weighting": "natural", "write_pb": true,
            "usemask": "pb", "pbmask": 0.99,
        }),
    );
    let pb = products.get(".pb");
    // The minor-cycle PB mask uses the same selected aperture as publication.
    let mask = products.get(".mask").plane(0);
    assert_eq!(mask[(50, 50)], 1.0);
    assert_eq!(mask[(75, 50)], 0.0);
    let reference = [
        (0.9846422076_f64, 0.9419874549_f64),
        (0.9830494523, 0.9362540245),
        (0.9814318419, 0.9301775098),
        (0.9797421098, 0.9238516092),
        (0.9779247642, 0.9173852205),
    ];
    assert!(
        pb.channel_frequency_hz(0) > 1.0e9,
        "BARY lies above the band edge"
    );
    for (channel, (near, far)) in reference.into_iter().enumerate() {
        let plane = pb.plane(channel);
        assert!(
            (f64::from(plane[(75, 50)]) - near).abs() < 1e-5,
            "channel {channel}: near {} vs {near}",
            plane[(75, 50)]
        );
        assert!(
            (f64::from(plane[(99, 50)]) - far).abs() < 1e-5,
            "channel {channel}: far {} vs {far}",
            plane[(99, 50)]
        );
    }
}
