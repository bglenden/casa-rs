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
