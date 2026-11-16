// SPDX-License-Identifier: LGPL-3.0-or-later
//! All polarized image planes and grids retain their owner's charge.
use super::polynomial_fields::measured;
use super::{PredictSpec, alone, charge_process_caches, configured, resources, scan};
use casa_images::PagedImage;
use ndarray::{ArrayD, IxDyn};
use std::path::Path;

pub(super) fn model(path: &Path, size: usize) {
    let source = PagedImage::<f32>::open(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_image_layouts/iquv.image"),
    )
    .unwrap();
    let coordinates = source.coordinates().clone();
    drop(source);
    let shape = vec![size, size, 4, 4];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let mut values = ArrayD::zeros(IxDyn(&shape));
    for channel in 0..4 {
        for (polarization, flux) in [1., 0.2, -0.1, 0.05].into_iter().enumerate() {
            values[[32, 32, polarization, channel]] = flux;
        }
    }
    image.put_slice(&values, &[0, 0, 0, 0]).unwrap();
    image.set_units("Jy/pixel").unwrap();
    image.save().unwrap();
}

#[test]
fn polarized_planes_and_grids_hold_admission() {
    if !alone("image_stokes::polarized_planes_and_grids_hold_admission") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for beam in [false, true] {
        for size in [64usize, 128, 512] {
            let path = directory.path().join(format!("model-{size}-{beam}.image"));
            model(&path, size);
            let mut sm = configured(
                &directory.path().join("simulation.ms"),
                &measures,
                512 << 20,
                4,
            );
            sm.observe(&scan(1)).unwrap();
            if beam {
                sm.set_vp(&super::VoltagePatternSpec {
                    do_squint: false,
                    ..super::VoltagePatternSpec::default()
                })
                .unwrap();
            }
            let predict = || {
                sm.predict(&PredictSpec {
                    images: vec![path],
                    component_lists: Vec::new(),
                    incremental: false,
                })
                .unwrap();
                sm
            };
            if size == 64 {
                drop(predict());
            } else {
                drop(measured(
                    &format!("{size} squared IQUV cube, beam={beam}"),
                    predict,
                ));
            }
        }
    }
}
