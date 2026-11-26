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
    let mut saved_psf = resumed.clone();
    saved_psf.calcpsf = false;
    OUTLIER_READ_BYTES.store(0, Ordering::Relaxed);
    let measured = check(&mut breaches, "saved outlier PSFs", &saved_psf, 1, AMPLE);
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert_eq!(measured.outcome.unwrap().major_cycle_count, 1);
    let outlier_admitted = OUTLIER_READ_BYTES.load(Ordering::Relaxed);
    assert!(
        outlier_admitted >= 12 * 1280 * 1280,
        "outlier PSF and residual read held only {outlier_admitted} admitted bytes"
    );
    // The first resumed measurement holds both fields' independently
    // read masks while the controller evaluates the requested threshold.
    for path in [
        prefix.with_extension("residual"),
        outlier.with_extension("residual"),
    ] {
        let mut image = casa_images::PagedImage::<f32>::open(path).unwrap();
        if image.default_mask_name().is_none() {
            image.make_mask("mask0", true, true).unwrap();
            image.save().unwrap();
        }
    }
    let mut saved_residual = saved_psf;
    saved_residual.calcres = false;
    saved_residual.niter = 1;
    saved_residual.threshold = 1.0e6;
    OUTLIER_MASK_BYTES.store(0, Ordering::Relaxed);
    let measured = check(
        &mut breaches,
        "saved outlier residuals",
        &saved_residual,
        1,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert_eq!(measured.outcome.unwrap().major_cycle_count, 0);
    let mask_admitted = OUTLIER_MASK_BYTES.load(Ordering::Relaxed);
    assert!(
        mask_admitted >= 2 * 1280 * 1280,
        "outlier residual mask read held only {mask_admitted} admitted bytes"
    );
    let mut rebuilt_psfs = saved_residual.clone();
    rebuilt_psfs.calcpsf = true;
    OUTLIER_MASK_BYTES.store(0, Ordering::Relaxed);
    let measured = check(
        &mut breaches,
        "rebuilt outlier PSFs with saved residuals",
        &rebuilt_psfs,
        1,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert_eq!(measured.outcome.unwrap().major_cycle_count, 0);
    assert!(OUTLIER_MASK_BYTES.load(Ordering::Relaxed) >= 2 * 1280 * 1280);

    // An existing image may hold a user mask while restoration is
    // staging a new outlier image without a saved PB for that field.
    let path = outlier.with_extension("image");
    let mut image = casa_images::PagedImage::<f32>::open(&path).unwrap();
    image.make_mask("prior_support", true, false).unwrap();
    image.save().unwrap();
    drop(image);
    assert!(!outlier.with_extension("pb").exists());
    let mut restore = saved_residual.clone();
    restore.niter = 0;
    let measured = check(
        &mut breaches,
        "saved outlier image mask without PB",
        &restore,
        1,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    drop(measured);
    let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(image.default_mask_name().as_deref(), Some("prior_support"));
    drop(image);

    let sum_path = outlier.with_extension("sumwt");
    let mut sum = casa_images::PagedImage::<f32>::open(&sum_path).unwrap();
    let zero = sum.get().unwrap().mapv(|_| 0.0);
    sum.put_slice(&zero, &[0; 4]).unwrap();
    sum.save().unwrap();
    drop(sum);
    saved_residual.calcres = true;
    let measured = check(
        &mut breaches,
        "refilled outlier sum",
        &saved_residual,
        1,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    let outcome = measured.outcome.unwrap();
    let normal = outcome.scientific().normal_state();
    assert!(normal.refilled_sum_weight(1));
    assert!(!normal.refilled_sum_weight(0));
    assert!(normal.read_plane(1, 0, 0).unwrap().published_sum_weight() > 0.0);
    drop(outcome);
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

pub(super) static OUTLIER_READ_BYTES: AtomicU64 = AtomicU64::new(0);
pub(super) static OUTLIER_MASK_BYTES: AtomicU64 = AtomicU64::new(0);

pub(super) fn record_read(event: &tracing::Event<'_>, counter: &AtomicU64) {
    #[derive(Default)]
    struct Read {
        domain: u64,
        admitted: u64,
    }
    impl tracing::field::Visit for Read {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            match field.name() {
                "domain" => self.domain = value,
                "admitted_bytes" => self.admitted = value,
                _ => {}
            }
        }
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    }
    let mut read = Read::default();
    event.record(&mut read);
    if read.domain == 1 {
        counter.store(read.admitted, Ordering::Relaxed);
    }
}

/// Fresh fields each regrid their own image while holding only the largest
/// source's scratch alongside the first model generation.
#[test]
fn explicit_outlier_seeds_are_admitted_one_field_at_a_time() {
    if !alone("outliers::explicit_outlier_seeds_are_admitted_one_field_at_a_time") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let ms = observation(root.path(), 6);
    warm(&ms, root.path());
    let define = |name: &str, outlier: &str, model: Option<&Path>| {
        let path = root.path().join(name);
        let seed = model.map_or_else(String::new, |model| {
            format!("startmodel={}\n", model.display())
        });
        std::fs::write(
            &path,
            format!(
                "imagename={}\nimsize=384\nphasecenter=J2000 18h00m00s -23d00m10s\n{seed}",
                root.path().join(outlier).display()
            ),
        )
        .unwrap();
        path
    };
    let initial = request(
        &ms,
        &root.path().join("initial-main"),
        512,
        json!({"niter":0, "outlierfile":define("initial.txt", "initial-outlier", None)}),
    );
    let mut breaches = Vec::new();
    assert_eq!(
        check(&mut breaches, "unseeded control", &initial, 1, AMPLE)
            .outcome
            .unwrap()
            .major_cycle_count,
        1
    );

    let main = root.path().join("initial-main.model");
    let outlier = root.path().join("initial-outlier.model");
    let expected = |path: &Path| {
        let image = casa_images::PagedImage::<f32>::open(path).unwrap();
        let template = casa_imageanalysis::RegridTemplate {
            coordinates: image.coordinates().clone(),
            shape: image.shape().to_vec(),
        };
        casa_imaging_application::start_model_law::seeding_bytes(path, template).unwrap()
    };
    let largest = expected(&main).max(expected(&outlier));
    let fresh = request(
        &ms,
        &root.path().join("fresh-main"),
        512,
        json!({"niter":0, "startmodel":main, "outlierfile":define("fresh.txt","fresh-outlier",Some(&outlier))}),
    );
    SEED_BYTES.store(0, Ordering::Relaxed);
    let run = check(&mut breaches, "fresh outlier models", &fresh, 1, AMPLE);
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert_eq!(run.outcome.unwrap().major_cycle_count, 1);
    let admitted = SEED_BYTES.load(Ordering::Relaxed);
    eprintln!("fresh outlier model read: {admitted} B, largest field {largest} B");
    assert_eq!(admitted, largest, "admit just one field's source scratch");
}
