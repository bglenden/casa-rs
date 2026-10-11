// SPDX-License-Identifier: LGPL-3.0-or-later
//! Admission covers every live phase (plan section 7 as R3 amended it).
//!
//! This binary's global allocator counts the process's live heap bytes. At
//! every allocation of a measured run it compares the bytes the run holds
//! beyond what was live when it started with the memory its phases hold
//! admitted at that instant. A global allocator applies to this test binary
//! alone, so no other test is affected.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use casa_imaging_application::{
    Admission, ApplicationDispatchError, HostResources, ImagingOutcome, ImagingRequest,
    ResourcePolicy, RunContext,
};
use casa_ms::{
    SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticObservationRequest,
    SyntheticSkyModel, SyntheticSpectralSetup, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};
use serde_json::{Value, json};

#[path = "common/imaging.rs"]
mod imaging;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes.
struct CountingHeap;

/// Heap bytes live in the process.
static LIVE: AtomicU64 = AtomicU64::new(0);
/// Whether a run is being measured.
static MEASURING: AtomicBool = AtomicBool::new(false);
/// `LIVE` when the measured run started.
static BASELINE: AtomicU64 = AtomicU64::new(0);
/// The measured run's memory ceiling.
static CEILING: AtomicU64 = AtomicU64::new(0);
/// The largest `LIVE − BASELINE`.
static PEAK: AtomicI64 = AtomicI64::new(0);
/// The largest `LIVE − BASELINE − reserved`.
static EXCESS: AtomicI64 = AtomicI64::new(i64::MIN);
/// The largest reserved total seen.
static RESERVED_PEAK: AtomicU64 = AtomicU64::new(0);
/// Nanoseconds into the run when `EXCESS` was last raised.
static EXCESS_AT: AtomicU64 = AtomicU64::new(0);
/// When the measured run started.
static STARTED: OnceLock<Instant> = OnceLock::new();

/// Timeline samples `(nanoseconds, used, reserved)`, for the report.
const SAMPLES: usize = 1 << 14;
static TIMELINE: [[AtomicI64; 3]; SAMPLES] = [const { [const { AtomicI64::new(0) }; 3] }; SAMPLES];
static RECORDED: AtomicUsize = AtomicUsize::new(0);
/// The last sample's `used` and `reserved`.
static LAST: [AtomicI64; 2] = [const { AtomicI64::new(0) }; 2];

/// The host every measured run sees: enough threads and free memory that
/// the ceiling alone bounds the run.
const HOST: HostResources = HostResources {
    threads: 64,
    performance_cores: 64,
    available_memory: 1 << 50,
    metal: false,
};

// SAFETY: every method forwards to `System` with the caller's arguments and
// only adds atomic bookkeeping, which allocates nothing.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`.
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old = layout.size() as u64;
        let new = new_size as u64;
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if moved != pointer {
                // The block moved: the old and the new bytes were held at
                // once while it was copied.
                observe(LIVE.load(Ordering::Relaxed) + new);
            }
            if new >= old {
                grow(new - old);
            } else {
                LIVE.fetch_sub(old - new, Ordering::Relaxed);
            }
        }
        moved
    }
}

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    observe(live);
}

/// Bytes the live reservations of the process hold.
fn reserved() -> u64 {
    let ceiling = CEILING.load(Ordering::Relaxed);
    let policy = ResourcePolicy::Explicit {
        workers: 1,
        memory: ceiling,
    };
    ceiling - casa_imaging_runtime::free_memory(&HOST, &policy)
}

fn observe(live: u64) {
    if !MEASURING.load(Ordering::Relaxed) {
        return;
    }
    let reserved = reserved();
    RESERVED_PEAK.fetch_max(reserved, Ordering::Relaxed);
    let reserved = reserved as i64;
    let used = live as i64 - BASELINE.load(Ordering::Relaxed) as i64;
    PEAK.fetch_max(used, Ordering::Relaxed);
    let at = STARTED
        .get()
        .map_or(0, |started| started.elapsed().as_nanos() as u64);
    if EXCESS.fetch_max(used - reserved, Ordering::Relaxed) < used - reserved {
        EXCESS_AT.store(at, Ordering::Relaxed);
    }
    let [last_used, last_reserved] = &LAST;
    if (used - last_used.load(Ordering::Relaxed)).abs() >= 1 << 20
        || reserved != last_reserved.load(Ordering::Relaxed)
    {
        last_used.store(used, Ordering::Relaxed);
        last_reserved.store(reserved, Ordering::Relaxed);
        let index = RECORDED.fetch_add(1, Ordering::Relaxed);
        if let Some(sample) = TIMELINE.get(index) {
            sample[0].store(at as i64, Ordering::Relaxed);
            sample[1].store(used, Ordering::Relaxed);
            sample[2].store(reserved, Ordering::Relaxed);
        }
    }
}

/// One process-wide measurement at a time: the allocator and the
/// reservations are global.
static MEASUREMENT: Mutex<()> = Mutex::new(());

/// What a measured run held.
struct Measured {
    /// The run, or its refusal.
    outcome: Result<ImagingOutcome, ApplicationDispatchError>,
    /// Largest live heap beyond the baseline.
    peak: i64,
    /// Largest live heap beyond the baseline and the admitted memory.
    excess: i64,
    /// Largest admitted total.
    reserved: u64,
}

/// Run `request` on `workers` workers under a `ceiling`-byte policy,
/// measuring its heap from the call on.
fn measure(request: &ImagingRequest, workers: usize, ceiling: u64) -> Measured {
    // The process holds the table-read cache's charge from its first run on.
    let process = held_by_process();
    assert!(
        process == 0 || process == casa_ms::table_read_cache_bytes() as u64,
        "no reservation of a run outlives it: {process} bytes held"
    );
    CEILING.store(ceiling, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    EXCESS.store(i64::MIN, Ordering::Relaxed);
    RESERVED_PEAK.store(0, Ordering::Relaxed);
    RECORDED.store(0, Ordering::Relaxed);
    LAST[0].store(0, Ordering::Relaxed);
    LAST[1].store(0, Ordering::Relaxed);
    let started = *STARTED.get_or_init(Instant::now);
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers,
            memory: ceiling,
        })
    };
    let offset = started.elapsed().as_nanos() as i64;
    BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let outcome = casa_imaging_application::execute(request, context);
    MEASURING.store(false, Ordering::Relaxed);
    let measured = Measured {
        peak: PEAK.load(Ordering::Relaxed),
        excess: EXCESS.load(Ordering::Relaxed),
        reserved: RESERVED_PEAK.load(Ordering::Relaxed),
        outcome,
    };
    report(&measured, offset);
    measured
}

/// Bytes the process's live reservations hold, whatever a run's ceiling.
fn held_by_process() -> u64 {
    let policy = ResourcePolicy::Explicit {
        workers: 1,
        memory: HOST.available_memory,
    };
    HOST.available_memory - casa_imaging_runtime::free_memory(&HOST, &policy)
}

/// Print the run's phases, its peak and excess, and where the excess was
/// greatest, in MB.
fn report(measured: &Measured, offset: i64) {
    let mb = |bytes: i64| bytes as f64 / 1.0e6;
    let at = (EXCESS_AT.load(Ordering::Relaxed) as i64 - offset) as f64 / 1.0e9;
    eprintln!(
        "peak {:.3} MB, reserved {:.3} MB, excess {:.3} MB at {at:.3} s",
        mb(measured.peak),
        mb(measured.reserved as i64),
        mb(measured.excess)
    );
    if let Ok(outcome) = &measured.outcome {
        for phase in &outcome.summary.phases {
            eprintln!("  phase {:>24} {:.3} s", phase.name, phase.seconds);
        }
    }
    let recorded = RECORDED.load(Ordering::Relaxed).min(SAMPLES);
    for sample in &TIMELINE[..recorded] {
        let [time, used, reserved] = sample.each_ref().map(|value| value.load(Ordering::Relaxed));
        eprintln!(
            "  {:>8.3} s used {:>9.3} reserved {:>9.3} excess {:>9.3}",
            (time - offset) as f64 / 1.0e9,
            mb(used),
            mb(reserved),
            mb(used - reserved)
        );
    }
}

/// A point and a Gaussian observed by a short VLA-A Q-band track in
/// `channels` 128 MHz channels.
fn observation(root: &Path, channels: usize) -> PathBuf {
    let measurement_set = root.join("admission.ms");
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = 600.0;
    request.integration_seconds = 60.0;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "admission".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: 128.0e6,
        channel_count: channels,
    }];
    let cell_rad = (CELL_ARCSEC / 3_600.0_f64).to_radians();
    let spectrum = |flux_jy: f64, spectral_index: f64| SyntheticAnalyticSpectrum {
        flux_jy,
        spectral_index,
        reference_frequency_hz: Some(44.2e9),
        line_peak_jy: 0.0,
        line_center_fraction: 0.5,
        line_sigma_fraction: 0.1,
        absorption_peak_jy: 0.0,
        absorption_center_fraction: 0.5,
        absorption_sigma_fraction: 0.1,
    };
    request.model = Some(SyntheticSkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("admission".to_string()),
        components: vec![
            SyntheticAnalyticComponent::Point {
                name: Some("point".to_string()),
                l_rad: -20.0 * cell_rad,
                m_rad: 12.0 * cell_rad,
                spectrum: spectrum(1.0, -0.7),
            },
            SyntheticAnalyticComponent::Gaussian {
                name: Some("gaussian".to_string()),
                l_rad: 30.0 * cell_rad,
                m_rad: -25.0 * cell_rad,
                major_fwhm_rad: 8.0 * cell_rad,
                minor_fwhm_rad: 8.0 * cell_rad,
                position_angle_rad: 0.0,
                spectrum: spectrum(0.5, 0.0),
            },
        ],
    });
    generate_synthetic_observation_ms(&request).expect("synthesise the admission MS");
    measurement_set
}

/// Cell of every measured image; the natural beam spans about four.
const CELL_ARCSEC: f64 = 0.012;

/// The request imaging `measurement_set` to `image_name` at `imsize`
/// pixels with `controls`.
fn request(
    measurement_set: &Path,
    image_name: &Path,
    imsize: usize,
    controls: Value,
) -> ImagingRequest {
    let mut values = json!({
        "vis": measurement_set,
        "imagename": image_name,
        "imsize": imsize,
        "cell": format!("{CELL_ARCSEC}arcsec"),
        "weighting": "natural",
        "niter": 200,
        "minor_cycle_length": 50,
        "gain": 0.2,
        "threshold": "1mJy",
    });
    values
        .as_object_mut()
        .expect("controls")
        .extend(controls.as_object().expect("overrides").clone());
    imaging::request(values)
}

/// A generous ceiling: every phase fits, so every plane is resident.
const AMPLE: u64 = 4 << 30;

/// Why `measured` broke the law, if it did: a run that completed or was
/// refused must never have held more live heap than it had admitted.
fn admission_breach(label: &str, measured: &Measured) -> Option<String> {
    (measured.excess > 0).then(|| {
        format!(
            "{label}: the live heap exceeded the admitted memory by {} bytes (peak {} bytes, \
             {} reserved)",
            measured.excess, measured.peak, measured.reserved
        )
    })
}

/// Measure `request` and record a breach, or a failure to run, under
/// `label`.
fn check(
    breaches: &mut Vec<String>,
    label: &str,
    request: &ImagingRequest,
    workers: usize,
    ceiling: u64,
) -> Measured {
    eprintln!("{label}");
    let measured = measure(request, workers, ceiling);
    if let Err(error) = &measured.outcome {
        breaches.push(format!("{label}: {error}"));
    }
    breaches.extend(admission_breach(label, &measured));
    measured
}

/// Every deconvolver, under natural, uniform and Briggs weighting, at one
/// worker and at four with images large enough that a charge which scales
/// with the image cannot hide in the fixed costs.
#[test]
fn continuum_runs_stay_within_their_admission() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 4);
    let mut breaches = Vec::new();
    for (label, controls) in [
        ("hogbom", json!({ "deconvolver": "hogbom" })),
        ("clark", json!({ "deconvolver": "clark" })),
        (
            "multiscale",
            json!({ "deconvolver": "multiscale", "scales": "0,4,8" }),
        ),
        ("mtmfs", json!({ "deconvolver": "mtmfs", "nterms": 2 })),
        (
            "hogbom uniform",
            json!({ "deconvolver": "hogbom", "weighting": "uniform" }),
        ),
        (
            "clark briggs",
            json!({ "deconvolver": "clark", "weighting": "briggs", "robust": 0.5 }),
        ),
    ] {
        for (imsize, workers) in [(256, 1), (1024, 4)] {
            let image = root
                .path()
                .join(format!("{}-{imsize}-{workers}", label.replace(' ', "-")));
            check(
                &mut breaches,
                &format!("{label} {imsize} px {workers} workers"),
                &request(&measurement_set, &image, imsize, controls.clone()),
                workers,
                AMPLE,
            );
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// Channels of the measured cubes.
const CUBE_CHANNELS: usize = 16;

/// A cleaned cube of every channel of `measurement_set`, 256 × 256, with
/// `controls`.
fn cube(measurement_set: &Path, image_name: &Path, controls: Value) -> ImagingRequest {
    let mut values = json!({
        "deconvolver": "clark",
        "specmode": "cube",
        "outframe": "TOPO",
        "spw": "0",
        "channel_start": 0,
        "channel_count": CUBE_CHANNELS,
        "start": "0",
        "width": "1",
    });
    values
        .as_object_mut()
        .expect("cube controls")
        .extend(controls.as_object().expect("overrides").clone());
    request(measurement_set, image_name, 256, values)
}

/// Cleaned cubes, resident and in waves, and a Briggs cube whose weight
/// density is per channel: their passes run after a minor cycle, with a
/// model, and the waved cube pages its state.
#[test]
fn cubes_stay_within_their_admission_resident_and_in_waves() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), CUBE_CHANNELS);
    let mut breaches = Vec::new();
    let resident = check(
        &mut breaches,
        "cube resident",
        &cube(&measurement_set, &root.path().join("resident"), json!({})),
        4,
        AMPLE,
    );
    let resident_waves = resident
        .outcome
        .as_ref()
        .ok()
        .and_then(|outcome| outcome.planes_per_wave);
    assert_eq!(resident_waves, None, "every plane fits {AMPLE} bytes");
    // The outcome keeps its state charged until it drops.
    drop(resident);
    // Raise the ceiling from 1 MiB by what each refusal reports missing,
    // and a third more, until the cube runs: just above what it needs, so in
    // waves. A refused attempt keeps the law as a completed one does.
    let waved = cube(&measurement_set, &root.path().join("waved"), json!({}));
    let mut memory = 1 << 20;
    let mut refusals = 0;
    let waves = loop {
        let label = format!("cube at {memory} bytes");
        eprintln!("{label}");
        let measured = measure(&waved, 4, memory);
        breaches.extend(admission_breach(&label, &measured));
        match &measured.outcome {
            Ok(outcome) => break outcome.planes_per_wave,
            Err(ApplicationDispatchError::Admission(Admission {
                required,
                available,
                ..
            })) => {
                let gap = required - available;
                memory += gap + gap.div_ceil(3);
                refusals += 1;
                assert!(refusals < 32, "the ceiling converges");
            }
            Err(error) => panic!("only admission refuses a small ceiling: {error}"),
        }
    };
    assert!(
        waves.is_some_and(|planes| (planes as usize) < CUBE_CHANNELS),
        "the cube runs in waves just above what it needs: {waves:?}"
    );
    check(
        &mut breaches,
        "briggs cube with per-channel density",
        &cube(
            &measurement_set,
            &root.path().join("briggs"),
            json!({ "weighting": "briggs", "robust": 0.5, "perchanweightdensity": true }),
        ),
        4,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// A ceiling too small for the run is refused before the run allocates
/// what it was not admitted.
#[test]
fn a_refused_run_holds_no_more_than_it_was_admitted() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 4);
    let measured = measure(
        &request(
            &measurement_set,
            &root.path().join("refused"),
            1024,
            json!({ "deconvolver": "clark", "weighting": "uniform" }),
        ),
        4,
        1 << 20,
    );
    assert!(
        matches!(
            measured.outcome,
            Err(ApplicationDispatchError::Admission(_))
        ),
        "1 MiB holds no 1024-pixel run"
    );
    let breach = admission_breach("refused", &measured);
    assert!(breach.is_none(), "{breach:?}");
}
