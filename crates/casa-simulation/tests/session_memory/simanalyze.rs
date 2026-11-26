// SPDX-License-Identifier: LGPL-3.0-or-later
//! Heap admission across composed concat, TP, seed-model and feather phases.
use super::*;
use casa_simulation::simanalyze::{SimanalyzeRequest, simanalyze};
use flate2::read::GzDecoder;
use serde_json::json;

#[test]
fn composed_simulation_analysis_stays_within_its_admissions() {
    if !alone("simanalyze::composed_simulation_analysis_stays_within_its_admissions") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let scratch = tempfile::tempdir().unwrap();
    tar::Archive::new(GzDecoder::new(
        &include_bytes!("../data/simanalyze_multi_inputs.tar.gz")[..],
    ))
    .unpack(scratch.path())
    .unwrap();
    let policy = resources(1 << 30);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let request = |case: &str, size: usize| {
        let mut parameters = json!({"project": scratch.path().join(case), "imsize": [size,size],
            "image": true, "analyze": false, "niter": 0, "graphics": "none", "overwrite": true});
        match case {
            "multi" => parameters["vis"] = json!("multi.small.ms,multi.compact.ms"),
            "seed" => {
                parameters["vis"] = json!("seed.small.ms,seed.small.sd.ms");
                parameters["modelimage"] = json!("$project.sd.image");
            }
            "feather" => {
                parameters["vis"] = json!("feather.small.ms");
                parameters["featherimage"] = json!(scratch.path().join("tp/tp.sd.image"));
            }
            _ => {}
        }
        SimanalyzeRequest {
            workspace: None,
            parameters: parameters.as_object().unwrap().clone(),
            resources: policy,
        }
    };
    // Warm every geometry/phase, including FFT plans, before counting.
    for size in [128, 256] {
        for case in ["multi", "tp", "seed", "feather"] {
            simanalyze(&request(case, size)).unwrap();
        }
    }
    for size in [128, 256] {
        for case in ["multi", "tp", "seed", "feather"] {
            let request = request(case, size);
            let baseline = LIVE.load(Ordering::Relaxed);
            let reserved = reserved_memory();
            PEAK.store(baseline, Ordering::Relaxed);
            let before = excess_now();
            EXCESS.store(before, Ordering::Relaxed);
            let window = ReservationWindow::open();
            drop(simanalyze(&request).unwrap());
            let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
            let admitted = window.close().saturating_sub(reserved);
            let beyond = EXCESS.load(Ordering::Relaxed) - before;
            let retained = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
            let kept = reserved_memory().saturating_sub(reserved);
            println!(
                "{case} {size}: peak {peak}/{admitted}, unadmitted {beyond}, retained {retained}/{kept}"
            );
            assert!(peak <= admitted + ALLOWANCE_BYTES);
            assert!(beyond <= ALLOWANCE_BYTES as i64);
            assert!(retained <= kept + ALLOWANCE_BYTES);
        }
    }
}
