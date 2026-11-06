// SPDX-License-Identifier: LGPL-3.0-or-later
//! Count request cloning while MVC prepares its cube and Taylor views.

use super::*;

const LARGE_FIELD: u64 = 1 << 22;
static TRACK: AtomicBool = AtomicBool::new(false);
static COPIES: AtomicUsize = AtomicUsize::new(0);
static COUNT_THRESHOLD: AtomicU64 = AtomicU64::new(LARGE_FIELD);
static MEMORY_TRACK: AtomicBool = AtomicBool::new(false);
static BASE_LIVE: AtomicU64 = AtomicU64::new(0);
static BASE_RESERVED: AtomicU64 = AtomicU64::new(0);
static EXCESS_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);

pub(super) fn allocation(bytes: u64, live: u64) {
    if TRACK.load(Ordering::Relaxed) && bytes >= COUNT_THRESHOLD.load(Ordering::Relaxed) {
        COPIES.fetch_add(1, Ordering::Relaxed);
    }
    if MEMORY_TRACK.load(Ordering::Relaxed) {
        let used = live.saturating_sub(BASE_LIVE.load(Ordering::Relaxed));
        let reserved = casa_imaging_runtime::reserved_memory()
            .saturating_sub(BASE_RESERVED.load(Ordering::Relaxed));
        PEAK_BYTES.fetch_max(used, Ordering::Relaxed);
        EXCESS_BYTES.fetch_max(used.saturating_sub(reserved), Ordering::Relaxed);
    }
}

/// An MFS-only unused `start` is cloned once into the cube request, which
/// immediately replaces it. Taylor preparation need not clone it again.
#[test]
fn taylor_view_does_not_clone_unused_request_fields() {
    if !alone("mvc_view::taylor_view_does_not_clone_unused_request_fields") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    let mut input = request(
        &measurement_set,
        &root.path().join("mvc-view"),
        64,
        json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "backend": "cpu", "write_pb": true,
        }),
    );
    input.start = Some("x".repeat(LARGE_FIELD as usize));
    let probe = casa_imaging_application::mvc_view_law::MvcViewProbe::new(input);
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: AMPLE,
        })
    };
    probe.prepare(&context).unwrap();
    COPIES.store(0, Ordering::Relaxed);
    TRACK.store(true, Ordering::Relaxed);
    let result = probe.prepare(&context);
    TRACK.store(false, Ordering::Relaxed);
    result.unwrap();
    assert_eq!(
        COPIES.load(Ordering::Relaxed),
        1,
        "Taylor preparation copied the unused request field"
    );
}

/// The cube request copy is admitted before its inputs are cloned, including
/// large fields that the cube request subsequently replaces.
#[test]
fn cube_request_copy_stays_within_its_admission() {
    if !alone("mvc_view::cube_request_copy_stays_within_its_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    let mut input = request(
        &measurement_set,
        &root.path().join("mvc-copy"),
        64,
        json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "backend": "cpu",
        }),
    );
    let source = input.vis[0].clone();
    input.vis.extend((0..4095).map(|_| source.clone()));
    input.start = Some("s".repeat(LARGE_FIELD as usize));
    input.width = Some("w".repeat((LARGE_FIELD / 2) as usize));
    input.vis[0].intent = Some("i".repeat((LARGE_FIELD / 4) as usize));
    input.imagename = std::path::PathBuf::from("o".repeat((LARGE_FIELD / 4) as usize));
    input.outlierfile = Some(std::path::PathBuf::from(
        "f".repeat((LARGE_FIELD / 4) as usize),
    ));
    input.mask_image = Some(std::path::PathBuf::from(
        "m".repeat((LARGE_FIELD / 4) as usize),
    ));
    input.startmodel = vec![None; 1 << 15];
    input.startmodel[0] = Some(std::path::PathBuf::from(
        "a".repeat((LARGE_FIELD / 4) as usize),
    ));
    input.mask_box = vec![casa_imaging_application::UserMaskBox::new([0, 0, 1, 1]); 1 << 15];
    input.stokes = vec![casa_imaging_application::PolarizationCoordinate::StokesI; 1 << 20];
    input.scales = vec![1.0; 1 << 17];
    input.restfreq = vec![1e9; 1 << 16];
    let probe = casa_imaging_application::mvc_view_law::MvcViewProbe::new(input);
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: AMPLE,
        })
    };
    // A real run has already initialized the admission ledger before it
    // reaches this copy; establish that process baseline first.
    drop(probe.copy_cube(&context).unwrap());
    BASE_LIVE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    BASE_RESERVED.store(casa_imaging_runtime::reserved_memory(), Ordering::Relaxed);
    EXCESS_BYTES.store(0, Ordering::Relaxed);
    PEAK_BYTES.store(0, Ordering::Relaxed);
    MEMORY_TRACK.store(true, Ordering::Relaxed);
    let copied = probe.copy_cube(&context);
    MEMORY_TRACK.store(false, Ordering::Relaxed);
    let copied = copied.unwrap();
    assert!(copied.admitted_bytes() >= LARGE_FIELD);
    let peak = PEAK_BYTES.load(Ordering::Relaxed);
    eprintln!(
        "cube request peak={peak} admitted={}",
        copied.admitted_bytes()
    );
    assert!(
        copied.admitted_bytes() <= peak + 4096,
        "request admission is not tightly bounded"
    );
    assert_eq!(
        EXCESS_BYTES.load(Ordering::Relaxed),
        0,
        "cube request allocation exceeded its reservation"
    );
    assert_eq!(
        casa_imaging_runtime::reserved_memory(),
        BASE_RESERVED.load(Ordering::Relaxed) + copied.admitted_bytes()
    );
    drop(copied);
    assert_eq!(
        casa_imaging_runtime::reserved_memory(),
        BASE_RESERVED.load(Ordering::Relaxed)
    );
    let tight = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: 1,
        })
    };
    assert!(matches!(probe.copy_cube(&tight), Err(error) if error.contains("MVC cube request")));
}

/// Even an otherwise small request needs space for the two new axis strings.
#[test]
fn cube_request_small_copy_stays_within_its_admission() {
    if !alone("mvc_view::cube_request_small_copy_stays_within_its_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    let input = request(
        &measurement_set,
        &root.path().join("mvc-small"),
        64,
        json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "backend": "cpu",
        }),
    );
    let probe = casa_imaging_application::mvc_view_law::MvcViewProbe::new(input);
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: AMPLE,
        })
    };
    drop(probe.copy_cube(&context).unwrap());
    BASE_LIVE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    BASE_RESERVED.store(casa_imaging_runtime::reserved_memory(), Ordering::Relaxed);
    EXCESS_BYTES.store(0, Ordering::Relaxed);
    MEMORY_TRACK.store(true, Ordering::Relaxed);
    let copied = probe.copy_cube(&context);
    MEMORY_TRACK.store(false, Ordering::Relaxed);
    let _copied = copied.unwrap();
    assert_eq!(
        EXCESS_BYTES.load(Ordering::Relaxed),
        0,
        "axis formatting exceeded the cube request reservation"
    );
}

/// AW-specific request buffers are accounted even though public MVC is
/// still guarded; an internal diagnostic must not under-admit a clone.
#[test]
fn cube_request_copy_accounts_for_aw_owned_buffers() {
    if !alone("mvc_view::cube_request_copy_accounts_for_aw_owned_buffers") {
        return;
    }
    use casa_imaging_application::{
        AwCfSource, AwProjection, Gridder, NativeAwCachePolicy, ProductNormalization,
    };
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    let sources = [
        AwCfSource::CasaImport {
            cfcache: std::path::PathBuf::from("c".repeat(1 << 20)),
        },
        AwCfSource::NativeEvla {
            native_cf_cache: std::path::PathBuf::from("n".repeat(1 << 20)),
            evla_surface: std::path::PathBuf::from("e".repeat(1 << 20)),
            native_cf_policy: NativeAwCachePolicy::ReuseOnly,
            native_cf_working_size: 128,
            native_cf_oversampling: 2,
            native_cf_cache_bytes: 1 << 20,
            native_cf_maximum_cells: 1,
        },
    ];
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: AMPLE,
        })
    };
    for source in sources {
        let mut input = request(
            &measurement_set,
            &root.path().join("mvc-aw-copy"),
            64,
            json!({
                "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2, "niter": 0,
            }),
        );
        input.gridder = Gridder::Awproject(AwProjection {
            wprojplanes: None,
            aterm: true,
            psterm: true,
            wbawp: false,
            conjbeams: false,
            usepointing: false,
            normtype: ProductNormalization::UnitResponse,
            cf_resident_mb: None,
            pointingoffsetsigdev: vec![1.0; 1 << 17],
            cf_source: source,
        });
        let probe = casa_imaging_application::mvc_view_law::MvcViewProbe::new(input);
        drop(probe.copy_cube(&context).unwrap());
        BASE_LIVE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
        BASE_RESERVED.store(casa_imaging_runtime::reserved_memory(), Ordering::Relaxed);
        EXCESS_BYTES.store(0, Ordering::Relaxed);
        MEMORY_TRACK.store(true, Ordering::Relaxed);
        let copied = probe.copy_cube(&context);
        MEMORY_TRACK.store(false, Ordering::Relaxed);
        let _copied = copied.unwrap();
        assert_eq!(
            EXCESS_BYTES.load(Ordering::Relaxed),
            0,
            "AW request allocation exceeded its reservation"
        );
    }
}

/// Projection planning scans the existing window arrays without cloning them.
#[test]
fn projection_frequency_range_does_not_clone_window_arrays() {
    if !alone("mvc_view::projection_frequency_range_does_not_clone_window_arrays") {
        return;
    }
    let mut frequencies = vec![2.0e9; (LARGE_FIELD as usize) / size_of::<f64>()];
    frequencies[0] = 1.0e9;
    *frequencies.last_mut().unwrap() = 3.0e9;
    let windows = vec![frequencies];
    COPIES.store(0, Ordering::Relaxed);
    COUNT_THRESHOLD.store(1 << 19, Ordering::Relaxed);
    TRACK.store(true, Ordering::Relaxed);
    let range = casa_imaging_application::mvc_view_law::projection_frequency_range(&windows);
    TRACK.store(false, Ordering::Relaxed);
    assert_eq!(range, [1.0e9, 3.0e9]);
    assert_eq!(
        COPIES.load(Ordering::Relaxed),
        0,
        "projection range cloned a frequency array"
    );
}

/// A Taylor view borrows the survey's channel selections without cloning
/// their potentially large vectors.
#[test]
fn taylor_spectral_selection_borrows_its_channel_vectors() {
    if !alone("mvc_view::taylor_spectral_selection_borrows_its_channel_vectors") {
        return;
    }
    let mut window = std::collections::BTreeMap::new();
    window.insert(
        0,
        vec![1_usize; (LARGE_FIELD as usize) / size_of::<usize>()],
    );
    let windows = vec![window];
    COPIES.store(0, Ordering::Relaxed);
    COUNT_THRESHOLD.store(1 << 19, Ordering::Relaxed);
    TRACK.store(true, Ordering::Relaxed);
    let shared = casa_imaging_application::mvc_view_law::selected_channels_are_borrowed(&windows);
    TRACK.store(false, Ordering::Relaxed);
    assert_eq!(
        COPIES.load(Ordering::Relaxed),
        0,
        "Taylor preparation cloned channel vectors"
    );
    assert!(
        shared,
        "the Taylor selection must borrow its source window list"
    );
}

/// Taylor geometry preparation must not build a second user-mask box list;
/// the run owns its mask separately from the image-coordinate view.
#[test]
fn taylor_view_does_not_rebuild_the_run_mask() {
    if !alone("mvc_view::taylor_view_does_not_rebuild_the_run_mask") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    let mut input = request(
        &measurement_set,
        &root.path().join("mvc-mask"),
        64,
        json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "backend": "cpu", "usemask": "user",
        }),
    );
    input.mask_box = vec![casa_imaging_application::UserMaskBox::new([0, 0, 1, 1]); 1 << 17];
    let probe = casa_imaging_application::mvc_view_law::MvcViewProbe::new(input);
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: AMPLE,
        })
    };
    probe.prepare(&context).unwrap();
    COPIES.store(0, Ordering::Relaxed);
    COUNT_THRESHOLD.store(1 << 19, Ordering::Relaxed);
    TRACK.store(true, Ordering::Relaxed);
    let result = probe.prepare(&context);
    TRACK.store(false, Ordering::Relaxed);
    result.unwrap();
    assert!(
        COPIES.load(Ordering::Relaxed) <= 3,
        "run and Taylor preparation allocated too many large mask buffers: {}",
        COPIES.load(Ordering::Relaxed)
    );
    assert_eq!(probe.run_mask_box_count(&context).unwrap(), Some(1 << 17));
}

/// The secondary Taylor geometry allocates no large mask vector while the
/// run's independently prepared mask still retains every requested box.
#[test]
fn taylor_only_geometry_does_not_copy_the_run_mask() {
    if !alone("mvc_view::taylor_only_geometry_does_not_copy_the_run_mask") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    let mut input = request(
        &measurement_set,
        &root.path().join("mvc-mask-specific"),
        64,
        json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "backend": "cpu", "usemask": "user",
        }),
    );
    const BOXES: usize = 1 << 17;
    input.mask_box = vec![casa_imaging_application::UserMaskBox::new([0, 0, 1, 1]); BOXES];
    let probe = casa_imaging_application::mvc_view_law::MvcViewProbe::new(input);
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: AMPLE,
        })
    };
    probe.prepare(&context).unwrap();
    let before = || {
        COPIES.store(0, Ordering::Relaxed);
        COUNT_THRESHOLD.store(1 << 19, Ordering::Relaxed);
        TRACK.store(true, Ordering::Relaxed);
    };
    let after = || TRACK.store(false, Ordering::Relaxed);
    let result = probe.prepare_taylor_with_hooks(&context, &before, &after);
    TRACK.store(false, Ordering::Relaxed);
    result.unwrap();
    assert_eq!(
        COPIES.load(Ordering::Relaxed),
        0,
        "Taylor geometry allocated a large mask copy"
    );
    assert_eq!(
        probe.run_mask_box_count(&context).unwrap(),
        Some(BOXES),
        "the run must retain every requested mask box"
    );
}

/// The Taylor specification takes ownership of its scale list instead of
/// cloning that list a second time during reconstruction planning.
#[test]
fn taylor_view_copies_the_reconstruction_scales_only_once() {
    if !alone("mvc_view::taylor_view_copies_the_reconstruction_scales_only_once") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    let mut input = request(
        &measurement_set,
        &root.path().join("mvc-scales"),
        64,
        json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "backend": "cpu",
        }),
    );
    input.scales = vec![0.0; 1 << 17];
    let probe = casa_imaging_application::mvc_view_law::MvcViewProbe::new(input);
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: AMPLE,
        })
    };
    probe.prepare(&context).unwrap();
    let before = || {
        COPIES.store(0, Ordering::Relaxed);
        COUNT_THRESHOLD.store(1 << 19, Ordering::Relaxed);
        TRACK.store(true, Ordering::Relaxed);
    };
    let after = || TRACK.store(false, Ordering::Relaxed);
    let result = probe.prepare_taylor_with_hooks(&context, &before, &after);
    TRACK.store(false, Ordering::Relaxed);
    result.unwrap();
    assert!(
        COPIES.load(Ordering::Relaxed) <= 1,
        "Taylor preparation duplicated the reconstruction scales: {} allocations",
        COPIES.load(Ordering::Relaxed)
    );
}

/// Both phases of the extra Taylor view stay under the same reservation,
/// including large rest-frequency and reconstruction-scale lists. The view
/// remains charged after preparation and while its compiled form is made.
#[test]
fn taylor_view_preparation_and_compile_stay_within_admission() {
    if !alone("mvc_view::taylor_view_preparation_and_compile_stay_within_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    let mut input = request(
        &measurement_set,
        &root.path().join("mvc-view-admission"),
        64,
        json!({
            "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
            "niter": 0, "backend": "cpu",
        }),
    );
    input.restfreq = vec![1e9; 1 << 17];
    input.scales = vec![0.0; 1 << 17];
    let probe = casa_imaging_application::mvc_view_law::MvcViewProbe::new(input);
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: AMPLE,
        })
    };
    probe.prepare(&context).unwrap();
    let retained = std::cell::Cell::new(0_u64);
    let prepare_peak = std::cell::Cell::new(0_u64);
    let held = std::cell::Cell::new(0_u64);
    let before_prepare = || {
        BASE_LIVE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
        BASE_RESERVED.store(casa_imaging_runtime::reserved_memory(), Ordering::Relaxed);
        EXCESS_BYTES.store(0, Ordering::Relaxed);
        PEAK_BYTES.store(0, Ordering::Relaxed);
        MEMORY_TRACK.store(true, Ordering::Relaxed);
    };
    let after_prepare = || {
        MEMORY_TRACK.store(false, Ordering::Relaxed);
        retained.set(
            LIVE.load(Ordering::Relaxed)
                .saturating_sub(BASE_LIVE.load(Ordering::Relaxed)),
        );
        prepare_peak.set(PEAK_BYTES.load(Ordering::Relaxed));
        held.set(
            casa_imaging_runtime::reserved_memory()
                .saturating_sub(BASE_RESERVED.load(Ordering::Relaxed)),
        );
    };
    let before_compile = |reservation: u64| {
        assert_eq!(
            reservation,
            held.get(),
            "view charge did not survive preparation"
        );
        BASE_LIVE.store(
            LIVE.load(Ordering::Relaxed).saturating_sub(retained.get()),
            Ordering::Relaxed,
        );
        BASE_RESERVED.store(
            casa_imaging_runtime::reserved_memory().saturating_sub(reservation),
            Ordering::Relaxed,
        );
        MEMORY_TRACK.store(true, Ordering::Relaxed);
    };
    let after_compile = || {
        MEMORY_TRACK.store(false, Ordering::Relaxed);
        assert_eq!(
            casa_imaging_runtime::reserved_memory()
                .saturating_sub(BASE_RESERVED.load(Ordering::Relaxed)),
            held.get(),
            "compiled Taylor view must retain its charge",
        );
    };
    let result = probe.compile_taylor_with_hooks(
        &context,
        &before_prepare,
        &after_prepare,
        &before_compile,
        &after_compile,
    );
    MEMORY_TRACK.store(false, Ordering::Relaxed);
    result.unwrap();
    eprintln!(
        "Taylor view prepare peak={} combined peak={} admitted={}",
        prepare_peak.get(),
        PEAK_BYTES.load(Ordering::Relaxed),
        held.get()
    );
    assert!(held.get() > 3 * (1 << 20));
    assert_eq!(
        EXCESS_BYTES.load(Ordering::Relaxed),
        0,
        "Taylor view exceeded its held admission"
    );
}

/// The real MVC run holds its cube, Taylor conversion, both staged product
/// families and their reservations together without exceeding admission.
#[test]
fn mvc_full_run_stays_within_its_held_admission() {
    if !alone("mvc_view::mvc_full_run_stays_within_its_held_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    warm(&measurement_set, root.path());
    let request = request(
        &measurement_set,
        &root.path().join("mvc-full-admission"),
        1024,
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "niter": 0, "backend": "cpu", "write_pb": true,
            "restoration": false,
        }),
    );
    let measured = measure(&request, 1, AMPLE);
    assert!(
        measured.outcome.is_ok(),
        "MVC run: {:?}",
        measured.outcome.err()
    );
    assert!(
        admission_breach("MVC full run", &measured).is_none(),
        "{}",
        admission_breach("MVC full run", &measured).unwrap_or_default()
    );
}

/// A cleaned MVC run holds the cube and Taylor states together while the
/// Taylor model is projected and the cube residual is refreshed (#1069).
#[test]
fn mvc_clean_run_stays_within_its_held_admission() {
    if !alone("mvc_view::mvc_clean_run_stays_within_its_held_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let measurement_set = observation(root.path(), 6);
    warm(&measurement_set, root.path());
    let request = request(
        &measurement_set,
        &root.path().join("mvc-clean-admission"),
        1024,
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "niter": 2, "cycleniter": 1, "pblimit": -0.1,
            "backend": "cpu", "write_pb": true,
        }),
    );
    let measured = measure(&request, 1, AMPLE);
    let outcome = measured.outcome.as_ref().expect("MVC cleaning run");
    assert!(
        outcome.total_actual_minor_iterations >= 2,
        "both minor cycles ran"
    );
    assert!(outcome.major_cycle_count >= 3, "both cube refreshes ran");
    assert!(
        admission_breach("MVC clean run", &measured).is_none(),
        "{}",
        admission_breach("MVC clean run", &measured).unwrap_or_default()
    );
}
