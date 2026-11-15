// SPDX-License-Identifier: LGPL-3.0-or-later
//! Whole-task memory: observations, TP concatenation/gridding and combination.
use super::*;
use casa_images::PagedImage;
use casa_simulation::simalma::{simalma, simalma_with_plots};
use serde_json::json;
#[path = "../common/simalma_fixture.rs"]
mod fixture;

#[test]
fn alma_orchestration_stays_within_admission_at_two_image_sizes() {
    if !alone("simalma::alma_orchestration_stays_within_admission_at_two_image_sizes") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(1 << 30);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let (_scratch, mut request, _oracle) = fixture::fixture(policy);
    for size in [128, 256] {
        request
            .parameters
            .insert("imsize".into(), json!([size, size]));
        drop(simalma(&request).unwrap());
    }
    for size in [128, 256] {
        request
            .parameters
            .insert("imsize".into(), json!([size, size]));
        let baseline = LIVE.load(Ordering::Relaxed);
        let reserved = reserved_memory();
        PEAK.store(baseline, Ordering::Relaxed);
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        let window = ReservationWindow::open();
        drop(simalma(&request).unwrap());
        let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
        let admitted = window.close().saturating_sub(reserved);
        let beyond = EXCESS.load(Ordering::Relaxed) - before;
        let retained = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        let kept = reserved_memory().saturating_sub(reserved);
        println!(
            "simalma {size}: peak {peak}/{admitted}, unadmitted {beyond}, retained {retained}/{kept}"
        );
        assert!(peak <= admitted + ALLOWANCE_BYTES);
        assert!(beyond <= ALLOWANCE_BYTES as i64);
        assert!(retained <= kept + ALLOWANCE_BYTES);
    }
}

#[test]
fn composed_simulation_figures_stay_within_admission_at_two_image_sizes() {
    if !alone("simalma::composed_simulation_figures_stay_within_admission_at_two_image_sizes") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(1 << 30);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let (scratch, mut request, _oracle) = fixture::fixture(policy);
    request.parameters.insert("graphics".into(), json!("file"));
    let payload = scratch.path().join("figures.bin");
    std::fs::File::create(&payload).unwrap();
    let original = request.parameters["skymodel"].as_str().unwrap().to_owned();
    let cube = scratch.path().join("cube.image");
    let source = PagedImage::<f32>::open(&original).unwrap();
    let plane = source.get().unwrap();
    let mut shape = source.shape().to_vec();
    shape[3] = 8;
    let pixels = ndarray::ArrayD::from_shape_fn(ndarray::IxDyn(&shape), |index| {
        plane[[index[0], index[1], index[2], 0]]
    });
    let mut image = PagedImage::<f32>::create(shape, source.coordinates().clone(), &cube).unwrap();
    image.put_slice(&pixels, &[0; 4]).unwrap();
    image.set_units(source.units()).unwrap();
    image.save().unwrap();
    drop((source, image, pixels, plane));
    // Vary channels independently, then vary the output/source size ratio.
    for (size, channels) in [(128, 1), (128, 8), (256, 8)] {
        request.parameters.insert(
            "skymodel".into(),
            if channels == 1 {
                json!(original)
            } else {
                json!(cube)
            },
        );
        request
            .parameters
            .insert("imsize".into(), json!([size, size]));
        drop(simalma_with_plots(&request, &payload).unwrap());
        let baseline = LIVE.load(Ordering::Relaxed);
        let reserved = reserved_memory();
        PEAK.store(baseline, Ordering::Relaxed);
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        let window = ReservationWindow::open();
        drop(simalma_with_plots(&request, &payload).unwrap());
        let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
        let admitted = window.close().saturating_sub(reserved);
        let beyond = EXCESS.load(Ordering::Relaxed) - before;
        let retained = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        let kept = reserved_memory().saturating_sub(reserved);
        println!(
            "simulation figures {size} channels={channels}: peak {peak}/{admitted}, unadmitted {beyond}, retained {retained}/{kept}"
        );
        assert!(peak <= admitted + ALLOWANCE_BYTES);
        assert!(beyond <= ALLOWANCE_BYTES as i64);
        assert!(retained <= kept + ALLOWANCE_BYTES);
    }
}
