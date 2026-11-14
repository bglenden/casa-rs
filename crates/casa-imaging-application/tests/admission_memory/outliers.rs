// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Resume on different image grids, holding each saved field's pixels
/// only while copying it into the first generation.
#[test]
fn outlier_restart_seed_stays_within_admission() {
    if !alone("outliers::outlier_restart_seed_stays_within_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let ms = observation(root.path(), 6);
    warm(&ms, root.path());
    let prefix = root.path().join("main");
    let outlier = root.path().join("outlier");
    let definition = root.path().join("outliers.txt");
    std::fs::write(
        &definition,
        format!(
            "imagename={}\nimsize=1280\nphasecenter=J2000 18h00m00s -23d00m10s\n",
            outlier.display()
        ),
    )
    .unwrap();
    let controls = json!({"niter": 0, "outlierfile": definition});
    let initial = request(&ms, &prefix, 1536, controls);
    casa_imaging_application::execute(
        &initial,
        RunContext {
            host: HOST,
            ..imaging::context(ResourcePolicy::Explicit {
                workers: 1,
                memory: AMPLE,
            })
        },
    )
    .unwrap();
    assert!(root.path().join("outlier.model").is_dir());
    let resumed = initial.with_tclean_restart().unwrap();
    let mut breaches = Vec::new();
    SEED_BYTES.store(0, Ordering::Relaxed);
    let measured = check(&mut breaches, "saved outlier models", &resumed, 1, AMPLE);
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert_eq!(measured.outcome.unwrap().major_cycle_count, 1);
    let admitted = SEED_BYTES.load(Ordering::Relaxed);
    // The current direct reader holds a Float32 input and emitted samples
    // of the largest domain together, beside the new model generation.
    assert!(
        admitted >= 8 * 1536 * 1536,
        "model seed held only {admitted} bytes"
    );
}

static SEED_BYTES: AtomicU64 = AtomicU64::new(0);

pub(super) fn record_seed(event: &tracing::Event<'_>) {
    struct Seed;
    impl tracing::field::Visit for Seed {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            if field.name() == "admitted_bytes" {
                SEED_BYTES.store(value, Ordering::Relaxed);
            }
        }
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    }
    event.record(&mut Seed);
}
