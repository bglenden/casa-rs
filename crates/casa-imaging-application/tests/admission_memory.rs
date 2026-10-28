// SPDX-License-Identifier: LGPL-3.0-or-later
//! Admission covers every live phase (plan section 7 as R3 amended it).
//!
//! This binary's global allocator counts the process's live heap bytes. At
//! every allocation of a measured run it compares the bytes the run holds
//! beyond what was live when it started with the memory its phases hold
//! admitted at that instant. A global allocator applies to this test binary
//! alone, so no other test is affected, and each law runs alone in a
//! process of its own ([`alone`]).
//!
//! The comparison leaves out the process caches' charge: each test first
//! runs its MeasurementSet once unmeasured ([`warm`]), so the caches are
//! charged and the table cache holds the MeasurementSet's tiles before the
//! baseline is taken, and FFTW's plan cache is C heap this allocator does
//! not see. Only the MeasurementSet's size on disk stays in the comparison,
//! for tiles a measured run reads that the warming did not. What remains
//! unspent of the fixed charges is then the Measures catalogs' (16 MiB,
//! of which a run holds 3.3 MB to 10.6 MB) and that allowance: the law
//! resolves a missing charge of about 15 MB, two 1024² `f64` planes.
//!
//! The converse law bounds the over-charge (#738): for MT-MFS runs, with
//! and without A-projection, the largest reservation of the run and of
//! each pass and minor cycle stays within a proportion of what the run
//! held at that point ([`over_charge`]). Each phase's measurement runs from
//! the end of the phase before it to its `stage_timing` event.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use casa_imaging_application::{
    Admission, ApplicationDispatchError, HostResources, ImagingOutcome, ImagingRequest,
    ResourcePolicy, RunContext, SourceRoute,
};
use casa_ms::{
    SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticObservationRequest,
    SyntheticSkyModel, SyntheticSpectralSetup, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};
use serde_json::{Value, json};

#[path = "common/imaging.rs"]
mod imaging;

#[path = "common/synthetic_observation.rs"]
mod synthetic_observation;
use synthetic_observation::{CELL_ARCSEC, observation, observation_as};

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
/// The largest `LIVE − BASELINE`.
static PEAK: AtomicI64 = AtomicI64::new(0);
/// The largest `LIVE − BASELINE − reserved`.
static EXCESS: AtomicI64 = AtomicI64::new(i64::MIN);
/// The largest reserved total seen.
static RESERVED_PEAK: AtomicU64 = AtomicU64::new(0);
/// Reserved bytes the comparison leaves out: the process caches' charge,
/// less the warmed MeasurementSet's allowance ([`warm`]).
static FIXED: AtomicU64 = AtomicU64::new(0);
/// The warmed MeasurementSet's size on disk.
static ALLOWANCE: AtomicU64 = AtomicU64::new(0);
/// Nanoseconds into the run when `EXCESS` was last raised.
static EXCESS_AT: AtomicU64 = AtomicU64::new(0);
/// The largest `LIVE − BASELINE` since the current phase began.
static PHASE_PEAK: AtomicI64 = AtomicI64::new(0);
/// The largest reserved total since the current phase began.
static PHASE_RESERVED: AtomicU64 = AtomicU64::new(0);
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

fn observe(live: u64) {
    if !MEASURING.load(Ordering::Relaxed) {
        return;
    }
    let reserved = held_by_process().saturating_sub(FIXED.load(Ordering::Relaxed));
    RESERVED_PEAK.fetch_max(reserved, Ordering::Relaxed);
    PHASE_RESERVED.fetch_max(reserved, Ordering::Relaxed);
    let reserved = reserved as i64;
    let used = live as i64 - BASELINE.load(Ordering::Relaxed) as i64;
    PEAK.fetch_max(used, Ordering::Relaxed);
    PHASE_PEAK.fetch_max(used, Ordering::Relaxed);
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

/// The environment variable that names the law a child process of
/// [`alone`] runs.
const LAW_VAR: &str = "CASA_RS_COUNTING_LAW";

/// The test a law's process runs before the law ([`alone`]).
const SETTLE: &str = "a_counting_law_process_settles";

/// Whether [`SETTLE`] ran in this process.
static SETTLED: AtomicBool = AtomicBool::new(false);

/// Runs first in a law's process ([`alone`]): libtest's main thread sets up
/// its wait for results while the first test it starts runs, so the law,
/// which runs second, measures none of it. The pause makes sure the main
/// thread is waiting before this test reports.
#[test]
#[ignore = "alone runs it before a law, in the law's process"]
fn a_counting_law_process_settles() {
    std::thread::sleep(std::time::Duration::from_millis(10));
    SETTLED.store(true, Ordering::Relaxed);
}

/// Whether this process runs the law `name` alone; if it does not, run the
/// law alone and assert that it passed.
///
/// The counters see every allocation in the process, and libtest runs a
/// binary's tests in one process: under `cargo test` its main thread starts
/// tests and reports their results, and the binary's other tests run, while
/// a law measures. So each law begins `if !alone("law") { return; }`, with
/// its own name. In the harness's process it runs [`SETTLE`], then the law
/// (libtest runs tests in name order), in a child process of this binary
/// with one test thread, so that the harness only waits while the law runs;
/// in the child it returns true.
fn alone(name: &str) -> bool {
    // libtest names each test's thread after the test.
    assert_eq!(
        std::thread::current().name(),
        Some(name),
        "a law runs alone under its own name"
    );
    let ran = format!("{name} ran alone");
    if std::env::var_os(LAW_VAR).is_some_and(|law| law == name) {
        assert!(
            SETTLED.load(Ordering::Relaxed),
            "{SETTLE} runs before {name}"
        );
        println!("{ran}");
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            SETTLE,
            name,
            "--exact",
            "--include-ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(LAW_VAR, name)
        .output()
        .expect("the law's process runs");
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{name} alone: {}\n{stdout}",
        output.status
    );
    assert!(stdout.contains(&ran), "{name} did not run alone:\n{stdout}");
    false
}

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
    /// Each phase the run logged with `stage_timing`, in order.
    phases: Vec<PhaseMeasure>,
}

/// What a run held during one phase: from the end of the phase before it
/// to the phase's `stage_timing` event.
#[derive(Clone, Debug)]
struct PhaseMeasure {
    /// The phase's name in the run summary.
    name: String,
    /// The largest live heap beyond the run's baseline.
    peak: i64,
    /// The largest admitted total, as the run's other reservations count.
    reserved: u64,
}

/// Records a [`PhaseMeasure`] at each `stage_timing` event and starts the
/// next phase's.
struct PhaseRecorder {
    phases: std::sync::Arc<Mutex<Vec<PhaseMeasure>>>,
}

/// The `phase` field of a `stage_timing` event, without its quotes.
struct PhaseName(Option<String>);

impl tracing::field::Visit for PhaseName {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "phase" {
            self.0 = Some(format!("{value:?}").trim_matches('"').to_string());
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PhaseRecorder {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != casa_imaging_runtime::STAGE_TIMING {
            return;
        }
        let mut name = PhaseName(None);
        event.record(&mut name);
        let used = LIVE.load(Ordering::Relaxed) as i64 - BASELINE.load(Ordering::Relaxed) as i64;
        let reserved = held_by_process().saturating_sub(FIXED.load(Ordering::Relaxed));
        let measure = PhaseMeasure {
            name: name.0.unwrap_or_default(),
            peak: PHASE_PEAK.swap(used, Ordering::Relaxed),
            reserved: PHASE_RESERVED.swap(reserved, Ordering::Relaxed),
        };
        self.phases.lock().expect("phases").push(measure);
    }
}

/// Run `request` on `workers` workers under a `ceiling`-byte policy,
/// measuring its heap from the call on.
fn measure(request: &ImagingRequest, workers: usize, ceiling: u64) -> Measured {
    // The process holds its caches' charge from its first run, the warming,
    // on.
    let process = held_by_process();
    assert_eq!(
        process,
        casa_imaging_runtime::process_cache_bytes(),
        "the MeasurementSet was warmed, and no reservation of a run outlives it"
    );
    FIXED.store(
        process.saturating_sub(ALLOWANCE.load(Ordering::Relaxed)),
        Ordering::Relaxed,
    );
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
    let phases = std::sync::Arc::new(Mutex::new(Vec::new()));
    let recorder = tracing_subscriber::layer::SubscriberExt::with(
        tracing_subscriber::registry(),
        PhaseRecorder {
            phases: phases.clone(),
        },
    );
    PHASE_PEAK.store(0, Ordering::Relaxed);
    PHASE_RESERVED.store(0, Ordering::Relaxed);
    BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let outcome = tracing::subscriber::with_default(recorder, || {
        casa_imaging_application::execute(request, context)
    });
    MEASURING.store(false, Ordering::Relaxed);
    let phases = phases.lock().expect("phases").clone();
    let measured = Measured {
        peak: PEAK.load(Ordering::Relaxed),
        excess: EXCESS.load(Ordering::Relaxed),
        reserved: RESERVED_PEAK.load(Ordering::Relaxed),
        outcome,
        phases,
    };
    report(&measured, offset);
    measured
}

/// Run `measurement_set` once unmeasured, so that the process caches are
/// charged and the table cache holds its tiles before a measured run takes
/// its baseline; its size on disk bounds what a measured run can add to
/// the table cache, and stays in the comparison.
fn warm(measurement_set: &Path, root: &Path) {
    casa_imaging_application::execute(
        &request(
            measurement_set,
            &root.join("warm"),
            64,
            json!({ "niter": 0 }),
        ),
        RunContext {
            host: HOST,
            ..imaging::context(ResourcePolicy::Explicit {
                workers: 1,
                memory: AMPLE,
            })
        },
    )
    .expect("the warming run");
    ALLOWANCE.store(disk_bytes(measurement_set), Ordering::Relaxed);
}

/// Bytes of the files under `path`.
fn disk_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .expect("a MeasurementSet directory")
        .map(|entry| {
            let entry = entry.expect("a directory entry");
            let metadata = entry.metadata().expect("metadata");
            if metadata.is_dir() {
                disk_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
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
    for phase in &measured.phases {
        eprintln!(
            "  held {:>28} peak {:>9.3} reserved {:>9.3} MB",
            phase.name,
            mb(phase.peak),
            mb(phase.reserved as i64)
        );
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
    if !alone("continuum_runs_stay_within_their_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 4);
    warm(&measurement_set, root.path());
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
    if !alone("cubes_stay_within_their_admission_resident_and_in_waves") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), CUBE_CHANNELS);
    warm(&measurement_set, root.path());
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
    let waved = cube(&measurement_set, &root.path().join("waved"), json!({}));
    let waves = just_above(&mut breaches, "cube", &waved);
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

/// Run `request` on four workers just above what it needs, recording
/// breaches under `label`: the ceiling rises from 1 MiB by what each
/// refusal reports missing, and a third more, until the run completes, so
/// a cube runs in waves. A refused attempt keeps the law as a completed
/// one does. Each solved plane's terms are admitted as it finishes, and
/// such a refusal misses only the plane's terms, so the ceiling grows by
/// an eighth at least. The completed run's planes per wave.
fn just_above(breaches: &mut Vec<String>, label: &str, request: &ImagingRequest) -> Option<u32> {
    let mut memory = 1 << 20;
    let mut refusals = 0;
    loop {
        let label = format!("{label} at {memory} bytes");
        eprintln!("{label}");
        let measured = measure(request, 4, memory);
        breaches.extend(admission_breach(&label, &measured));
        match &measured.outcome {
            Ok(outcome) => return outcome.planes_per_wave,
            Err(ApplicationDispatchError::Admission(Admission {
                required,
                available,
                ..
            })) => {
                let gap = required - available;
                memory += (gap + gap.div_ceil(3)).max(memory / 8);
                refusals += 1;
                assert!(refusals < 48, "the ceiling converges");
            }
            Err(error) => panic!("only admission refuses a small ceiling: {error}"),
        }
    }
}

/// The observation-batch route (#932, step 4) holds no more than it is
/// admitted: its batches, their geometry, the parallactic cache, the
/// opening's index and the channel-window survey of a cube's restricted
/// waves. Continuum runs under natural and uniform weighting, at one worker
/// and at four, and a cube resident and in waves.
#[test]
fn the_observation_batch_route_stays_within_its_admission() {
    if !alone("the_observation_batch_route_stays_within_its_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), CUBE_CHANNELS);
    warm(&measurement_set, root.path());
    let batches = |mut request: ImagingRequest| {
        request.source_route = SourceRoute::ObservationBatches;
        request
    };
    let mut breaches = Vec::new();
    for (label, controls) in [
        ("hogbom", json!({ "deconvolver": "hogbom" })),
        (
            "clark uniform",
            json!({ "deconvolver": "clark", "weighting": "uniform" }),
        ),
    ] {
        for (imsize, workers) in [(256, 1), (1024, 4)] {
            let image = root.path().join(format!(
                "batches-{}-{imsize}-{workers}",
                label.replace(' ', "-")
            ));
            check(
                &mut breaches,
                &format!("batches {label} {imsize} px {workers} workers"),
                &batches(request(&measurement_set, &image, imsize, controls.clone())),
                workers,
                AMPLE,
            );
        }
    }
    check(
        &mut breaches,
        "batches cube resident",
        &batches(cube(
            &measurement_set,
            &root.path().join("batches-resident"),
            json!({}),
        )),
        4,
        AMPLE,
    );
    let waves = just_above(
        &mut breaches,
        "batches cube",
        &batches(cube(
            &measurement_set,
            &root.path().join("batches-waved"),
            json!({}),
        )),
    );
    assert!(
        waves.is_some_and(|planes| (planes as usize) < CUBE_CHANNELS),
        "the cube runs in waves just above what it needs: {waves:?}"
    );
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// Products on fewer lanes than the run's workers (#763 review) hold at most
/// what they were admitted. A dirty cube's products scratch grows with its
/// lanes; the ceiling rises from 1 MiB until the cube runs on all four
/// workers. Every run, refused or completed, keeps the law, and on some
/// completed run the products took fewer lanes than the workers.
#[test]
fn products_on_fewer_lanes_stay_within_their_admission() {
    if !alone("products_on_fewer_lanes_stay_within_their_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), CUBE_CHANNELS);
    warm(&measurement_set, root.path());
    let dirty = cube(
        &measurement_set,
        &root.path().join("dirty"),
        json!({ "niter": 0 }),
    );
    let mut breaches = Vec::new();
    let mut fewer_lanes = None;
    let mut memory = 1 << 20;
    for _ in 0..64 {
        let label = format!("dirty cube at {memory} bytes");
        eprintln!("{label}");
        let measured = measure(&dirty, 4, memory);
        breaches.extend(admission_breach(&label, &measured));
        match &measured.outcome {
            Ok(outcome) => {
                let summary = &outcome.summary;
                if summary.product_lanes < outcome.workers {
                    fewer_lanes.get_or_insert((summary.product_lanes, outcome.workers));
                }
                if outcome.workers == 4 {
                    break;
                }
                memory += memory / 8;
            }
            Err(ApplicationDispatchError::Admission(Admission {
                required,
                available,
                ..
            })) => {
                let gap = required - available;
                memory += (gap + gap.div_ceil(3)).max(memory / 8);
            }
            Err(error) => panic!("only admission refuses a small ceiling: {error}"),
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
    let (lanes, workers) = fewer_lanes.expect("some run's products took fewer lanes");
    assert!(
        lanes >= 1 && lanes < workers,
        "{lanes} lanes, {workers} workers"
    );
}

/// A ceiling too small for the run is refused before the run allocates
/// what it was not admitted.
#[test]
fn a_refused_run_holds_no_more_than_it_was_admitted() {
    if !alone("a_refused_run_holds_no_more_than_it_was_admitted") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 4);
    warm(&measurement_set, root.path());
    let refusals = [
        (
            "clark uniform at 1 MiB",
            request(
                &measurement_set,
                &root.path().join("refused"),
                1024,
                json!({ "deconvolver": "clark", "weighting": "uniform" }),
            ),
            1 << 20,
        ),
        // Astra's probe on #700: 64 W planes at 1024² are refused before
        // the operator builds them.
        (
            "wproject at 128 MiB",
            request(
                &measurement_set,
                &root.path().join("wproject-refused"),
                1024,
                json!({ "gridder": "wproject", "wprojplanes": 64, "niter": 0 }),
            ),
            128 << 20,
        ),
    ];
    for (label, request, ceiling) in &refusals {
        eprintln!("{label}");
        let measured = measure(request, 1, *ceiling);
        assert!(
            matches!(
                measured.outcome,
                Err(ApplicationDispatchError::Admission(_))
            ),
            "{label}: the run is refused, not {:?}",
            measured.outcome.as_ref().err()
        );
        let breach = admission_breach(label, &measured);
        assert!(breach.is_none(), "{breach:?}");
    }
}

/// A cleaned run that writes its model to `MODEL_DATA`: its last pass
/// predicts every selected sample and hands each block to the writer.
#[test]
fn a_visibility_write_stays_within_its_admission() {
    if !alone("a_visibility_write_stays_within_its_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 4);
    warm(&measurement_set, root.path());
    let mut breaches = Vec::new();
    for (imsize, workers) in [(256, 1), (1024, 4)] {
        check(
            &mut breaches,
            &format!("savemodel {imsize} px {workers} workers"),
            &request(
                &measurement_set,
                &root.path().join(format!("savemodel-{imsize}")),
                imsize,
                json!({ "deconvolver": "hogbom", "savemodel": "modelcolumn" }),
            ),
            workers,
            AMPLE,
        );
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// The shapes IF-10's closeout rows added stay within their admission
/// (#722): a 2048² image in 2×2 facets, whose pass also holds the stitched
/// full-size residual and PSF, and an eight-channel mosaic cube, whose paged
/// state also holds each channel's weight image. Both charges are smaller
/// than the slack of an ample admission, so this law does not resolve them
/// alone; `a_stitch_charges_its_full_images_and_one_held_facet_psf` and
/// `a_dense_sensitivity_charges_a_second_invariant_plane_per_plane` pin
/// their sizes.
#[test]
fn facets_and_mosaic_cubes_stay_within_their_admission() {
    if !alone("facets_and_mosaic_cubes_stay_within_their_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), CUBE_CHANNELS);
    warm(&measurement_set, root.path());
    let mut breaches = Vec::new();
    check(
        &mut breaches,
        "facets 2 at 2048",
        &request(
            &measurement_set,
            &root.path().join("facets"),
            2048,
            json!({ "facets": 2, "deconvolver": "hogbom", "niter": 20 }),
        ),
        2,
        AMPLE,
    );
    let alma = observation_as(root.path(), CUBE_CHANNELS, "ALMA");
    warm(&alma, root.path());
    let mut mosaic = cube(
        &alma,
        &root.path().join("mosaic-cube"),
        json!({ "gridder": "mosaic", "deconvolver": "hogbom", "channel_count": 8 }),
    );
    mosaic.imsize = 1024;
    check(&mut breaches, "mosaic cube 8 × 1024²", &mosaic, 2, AMPLE);
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// Runs whose phases the other laws do not reach: W-projection, whose
/// kernels are built and held before the passes, and a linear cube of a
/// sparse selection, whose rows place far more samples than they have
/// channels (Astra's probe on #700: `0:0~1;100` of 101 channels 128 MHz
/// apart onto ten channels 1.28 GHz wide).
#[test]
fn kernel_sets_and_sparse_selections_stay_within_their_admission() {
    if !alone("kernel_sets_and_sparse_selections_stay_within_their_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 101);
    warm(&measurement_set, root.path());
    let mut breaches = Vec::new();
    check(
        &mut breaches,
        "wproject 16 planes",
        &request(
            &measurement_set,
            &root.path().join("wproject"),
            256,
            json!({ "gridder": "wproject", "wprojplanes": 16, "deconvolver": "hogbom" }),
        ),
        2,
        AMPLE,
    );
    check(
        &mut breaches,
        "sparse linear cube",
        &request(
            &measurement_set,
            &root.path().join("sparse"),
            256,
            json!({
                "specmode": "cube",
                "outframe": "TOPO",
                "spw": "0:0~1;100",
                "channel_count": 10,
                "start": "44.64GHz",
                "width": "1.28GHz",
                "interpolation": "linear",
                "niter": 0,
            }),
        ),
        2,
        2 << 30,
    );
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// The fixed charges a run makes whatever its size, which the over-charge
/// bound allows beside its proportion: the Measures catalogs (16 MiB), the
/// walks and envelope of the MeasurementSet, a kernel set's index.
const FIXED_ALLOWANCE: u64 = 64 << 20;
/// How far a run's reservations may stand above what it holds, beyond
/// [`FIXED_ALLOWANCE`] (#738).
const OVER_CHARGE: f64 = 1.2;

/// Why `measured` broke the over-charge bound, if it did: its largest
/// reservation, or that of one of its passes, minor-cycle preparations,
/// minor cycles or its products, stood more than [`OVER_CHARGE`] times
/// above the most the run held, beyond [`FIXED_ALLOWANCE`].
fn over_charge(label: &str, measured: &Measured) -> Vec<String> {
    let bound = |peak: i64| (OVER_CHARGE * peak.max(0) as f64) as u64 + FIXED_ALLOWANCE;
    let mut breaches = Vec::new();
    if measured.phases.is_empty() {
        breaches.push(format!("{label}: no phase was measured"));
    }
    if measured.reserved > bound(measured.peak) {
        breaches.push(format!(
            "{label}: reserved {} bytes for a peak of {}",
            measured.reserved, measured.peak
        ));
    }
    for phase in &measured.phases {
        let bounded = [
            "major cycle",
            "minor-cycle preparation",
            "minor cycle",
            "products",
        ]
        .iter()
        .any(|prefix| phase.name.starts_with(prefix));
        if bounded && phase.reserved > bound(phase.peak) {
            breaches.push(format!(
                "{label} {}: reserved {} bytes for a peak of {}",
                phase.name, phase.reserved, phase.peak
            ));
        }
    }
    breaches
}

/// #738's deconvolver: MT-MFS of two Taylor terms on scales of 0, 5 and 12
/// pixels, each phase logged so that it is measured.
fn mtmfs_controls() -> Value {
    json!({
        "deconvolver": "mtmfs",
        "nterms": 2,
        "scales": "0,5,12",
        "stage_timing": true,
    })
}

/// #738: MT-MFS at 1024² and 2048² on one worker, and at 1024² on four,
/// whose per-worker charges (chunks, transform planes, minor-cycle loads)
/// grow with the workers, is admitted close to what it holds. Before the
/// charges followed their owners' footprints the 2048² run reserved 2.25 GB
/// for a peak of 1.57 GB: the minor cycle charged the plane loads and the
/// Taylor solver's transients as if they all coexisted, and the first pass
/// its stages' sum.
#[test]
fn mtmfs_is_admitted_close_to_what_it_holds() {
    if !alone("mtmfs_is_admitted_close_to_what_it_holds") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 4);
    warm(&measurement_set, root.path());
    let mut breaches = Vec::new();
    for (imsize, workers) in [(1024, 1), (1024, 4), (2048, 1)] {
        let label = format!("mtmfs {imsize} px {workers} workers");
        let measured = check(
            &mut breaches,
            &label,
            &request(
                &measurement_set,
                &root.path().join(format!("mtmfs-{imsize}-{workers}")),
                imsize,
                mtmfs_controls(),
            ),
            workers,
            AMPLE,
        );
        breaches.extend(over_charge(&label, &measured));
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// An EVLA track at L band for native A-projection, as T1's: the VLA-A
/// pads drawn 35 times towards their centroid (a 41″ beam), a 1 Jy point
/// at the phase centre, two 8 MHz channels at 1.5 GHz over an hour.
fn evla_observation(root: &Path) -> PathBuf {
    let measurement_set = root.join("admission-EVLA.ms");
    let mut antennas = tutorial_vla_a_antennas();
    let count = antennas.len() as f64;
    let centroid = antennas.iter().fold([0.0; 3], |sum, antenna| {
        [
            sum[0] + antenna.position_m[0] / count,
            sum[1] + antenna.position_m[1] / count,
            sum[2] + antenna.position_m[2] / count,
        ]
    });
    for antenna in &mut antennas {
        for (position, centre) in antenna.position_m.iter_mut().zip(centroid) {
            *position = centre + (*position - centre) / 35.0;
        }
    }
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        antennas,
    );
    request.telescope_name = "EVLA".to_string();
    request.duration_seconds = 3_600.0;
    request.integration_seconds = 60.0;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "admission-lband".to_string(),
        start_frequency_hz: 1.5e9,
        channel_width_hz: 8.0e6,
        channel_count: 2,
    }];
    request.model = Some(SyntheticSkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("admission-aw".to_string()),
        components: vec![SyntheticAnalyticComponent::Point {
            name: Some("point".to_string()),
            l_rad: 0.0,
            m_rad: 0.0,
            spectrum: SyntheticAnalyticSpectrum {
                flux_jy: 1.0,
                spectral_index: -0.7,
                reference_frequency_hz: Some(1.5e9),
                line_peak_jy: 0.0,
                line_center_fraction: 0.5,
                line_sigma_fraction: 0.1,
                absorption_peak_jy: 0.0,
                absorption_center_fraction: 0.5,
                absorption_sigma_fraction: 0.1,
            },
        }],
    });
    generate_synthetic_observation_ms(&request).expect("synthesise the EVLA MS");
    measurement_set
}

/// A paraboloid EVLA dish surface in the radius/height/slope text format,
/// 12.5 m sampled every centimetre, written under `root`.
fn evla_surface(root: &Path) -> PathBuf {
    let path = root.join("evla.surface");
    let text = (0..=1250)
        .map(|index| {
            let radius = f64::from(index) / 100.0;
            format!(
                "{radius:.2} {:.8} {:.8}\n",
                0.028 * radius * radius,
                0.056 * radius
            )
        })
        .collect::<String>();
    std::fs::write(&path, text).expect("write the EVLA surface");
    path
}

/// #738: native A-projection MT-MFS at 1024², generating its catalog and
/// then reusing it, is admitted close to what it holds. Generation is
/// admitted before it starts: uncharged, it held 274 MB beyond the
/// admitted memory. A catalog whose groups all fit its bound is charged
/// those groups: charged the bound of `cf_resident_mb = 512`, the run
/// reserved 974 MB for a peak of 398 MB. An automatic bound
/// (`cf_resident_mb = auto`, #449) is admitted by the same path. This
/// catalog's 15.6 MB of cells lie under its 256 MiB floor, so the bound is
/// not raised here (#781 review): `aw::cache`'s laws pin the raise, and B3
/// measured it end to end.
#[test]
fn aw_mtmfs_is_admitted_close_to_what_it_holds() {
    if !alone("aw_mtmfs_is_admitted_close_to_what_it_holds") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = evla_observation(root.path());
    let surface = evla_surface(root.path());
    warm(&measurement_set, root.path());
    let cache = root.path().join("native-cf");
    let mut breaches = Vec::new();
    for (label, resident) in [
        ("aw generating", json!(512)),
        ("aw reusing", json!(512)),
        ("aw automatic", json!("auto")),
    ] {
        let mut values = json!({
            "vis": measurement_set,
            "imagename": root.path().join(label.replace(' ', "-")),
            "imsize": 1024,
            "cell": "10arcsec",
            "weighting": "natural",
            "niter": 200,
            "minor_cycle_length": 50,
            "gain": 0.2,
            "threshold": "1mJy",
            "gridder": "awproject",
            "wprojplanes": 32,
            "aw_cf_source": "native-evla",
            "native_cf_cache": cache,
            "evla_surface": surface,
            "native_cf_policy": "generate-missing",
            "native_cf_working_size": 2048,
            "native_cf_oversampling": 4,
            "native_cf_cache_bytes": 2_147_483_648_u64,
            "native_cf_maximum_cells": 128,
            "cf_resident_mb": resident,
            "normtype": "flatnoise",
            "pbcor": true,
        });
        values.as_object_mut().expect("AW controls").extend(
            mtmfs_controls()
                .as_object()
                .expect("MT-MFS controls")
                .clone(),
        );
        let measured = check(&mut breaches, label, &imaging::request(values), 1, AMPLE);
        breaches.extend(over_charge(label, &measured));
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
