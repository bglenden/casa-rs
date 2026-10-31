// SPDX-License-Identifier: LGPL-3.0-or-later

//! Ordinary tclean writes its PB; casa-rs's explicit opt-out remains available.

use casa_images::PagedImage;
use casa_provider_contracts::TaskReply;
use serde_json::json;

use super::{
    casa_return::json_run,
    fixture::{Component, Observation},
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
        }
    }
}
