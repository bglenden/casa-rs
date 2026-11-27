// SPDX-License-Identifier: LGPL-3.0-or-later

//! Process-global heap counter and subprocess isolation for private
//! allocation-counting laws inside `casa-imaging`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use serde_json::Value;

use crate::{
    ApplicationDispatchError, Cancel, HostResources, ImagingOutcome, ImagingRequest,
    ResourcePolicy, RunContext,
};

#[path = "../tests/common/synthetic_observation.rs"]
pub(crate) mod synthetic_observation;

pub(crate) struct CountingHeap;

pub(crate) static LIVE: AtomicU64 = AtomicU64::new(0);
pub(crate) static MEASURING: AtomicBool = AtomicBool::new(false);
pub(crate) static BASELINE: AtomicU64 = AtomicU64::new(0);
pub(crate) static PEAK: AtomicU64 = AtomicU64::new(0);
pub(crate) static SIGNED_PEAK: AtomicI64 = AtomicI64::new(0);
pub(crate) static EXCESS: AtomicI64 = AtomicI64::new(i64::MIN);
pub(crate) static RESERVED_PEAK: AtomicU64 = AtomicU64::new(0);
pub(crate) static FIXED: AtomicU64 = AtomicU64::new(0);
pub(crate) static ALLOWANCE: AtomicU64 = AtomicU64::new(0);
pub(crate) static EXCESS_AT: AtomicU64 = AtomicU64::new(0);
static STARTED: OnceLock<Instant> = OnceLock::new();

pub(crate) static MVC_TRACK: AtomicBool = AtomicBool::new(false);
pub(crate) static MVC_COPIES: AtomicUsize = AtomicUsize::new(0);
pub(crate) static MVC_COUNT_THRESHOLD: AtomicU64 = AtomicU64::new(1 << 22);
pub(crate) static MVC_MEMORY_TRACK: AtomicBool = AtomicBool::new(false);
pub(crate) static MVC_BASE_LIVE: AtomicU64 = AtomicU64::new(0);
pub(crate) static MVC_BASE_RESERVED: AtomicU64 = AtomicU64::new(0);
pub(crate) static MVC_EXCESS_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static MVC_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static SEED_BYTES: AtomicU64 = AtomicU64::new(0);

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
                let peak = LIVE.load(Ordering::Relaxed) + new;
                observe_mvc(new, peak);
                observe(peak);
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
    observe_mvc(bytes, live);
    observe(live);
}

fn observe_mvc(bytes: u64, live: u64) {
    if MVC_TRACK.load(Ordering::Relaxed) && bytes >= MVC_COUNT_THRESHOLD.load(Ordering::Relaxed) {
        MVC_COPIES.fetch_add(1, Ordering::Relaxed);
    }
    if MVC_MEMORY_TRACK.load(Ordering::Relaxed) {
        let used = live.saturating_sub(MVC_BASE_LIVE.load(Ordering::Relaxed));
        let reserved = casa_imaging_runtime::reserved_memory()
            .saturating_sub(MVC_BASE_RESERVED.load(Ordering::Relaxed));
        MVC_PEAK_BYTES.fetch_max(used, Ordering::Relaxed);
        MVC_EXCESS_BYTES.fetch_max(used.saturating_sub(reserved), Ordering::Relaxed);
    }
}

fn observe(live: u64) {
    if !MEASURING.load(Ordering::Relaxed) {
        return;
    }
    let baseline = BASELINE.load(Ordering::Relaxed);
    PEAK.fetch_max(live.saturating_sub(baseline), Ordering::Relaxed);
    let used = live as i64 - baseline as i64;
    SIGNED_PEAK.fetch_max(used, Ordering::Relaxed);
    let reserved = held_by_process().saturating_sub(FIXED.load(Ordering::Relaxed));
    RESERVED_PEAK.fetch_max(reserved, Ordering::Relaxed);
    if EXCESS.fetch_max(used - reserved as i64, Ordering::Relaxed) < used - reserved as i64 {
        let at = STARTED
            .get()
            .map_or(0, |started| started.elapsed().as_nanos() as u64);
        EXCESS_AT.store(at, Ordering::Relaxed);
    }
}

const LAW_VAR: &str = "CASA_RS_COUNTING_LAW";

/// Run the current test alone in a dedicated child process of this test binary.
pub(crate) fn alone(name: &str) -> bool {
    assert_eq!(
        std::thread::current().name(),
        Some(name),
        "a law runs alone under its own name"
    );
    let ran = format!("{name} ran alone");
    if std::env::var_os(LAW_VAR).is_some_and(|law| law == name) {
        // Let libtest's initial thread and runner allocations settle in this fresh process.
        std::thread::sleep(std::time::Duration::from_millis(10));
        println!("{ran}");
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args([
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

pub(crate) const AMPLE: u64 = 4 << 30;

pub(crate) const HOST: HostResources = HostResources {
    threads: 64,
    performance_cores: 64,
    available_memory: 1 << 50,
    metal: false,
};

pub(crate) fn request(values: Value) -> ImagingRequest {
    let Value::Object(mut values) = values else {
        panic!("request values are a JSON object");
    };
    values.entry("backend").or_insert_with(|| "cpu".into());
    values.entry("imsize").or_insert_with(|| 512.into());
    values.entry("cell").or_insert_with(|| "1arcsec".into());
    values.entry("cycleniter").or_insert_with(|| 1000.into());
    values.entry("write_pb").or_insert_with(|| false.into());
    match values.get("gridder").and_then(Value::as_str) {
        Some("wproject") => {
            values.entry("wprojplanes").or_insert_with(|| (-1).into());
        }
        Some("awproject") => {
            values.entry("conjbeams").or_insert_with(|| true.into());
        }
        Some("mosaic") => {
            values.entry("mosweight").or_insert_with(|| false.into());
        }
        _ => {}
    }
    ImagingRequest::from_tclean_with_defaults(&values)
        .unwrap_or_else(|error| panic!("imaging request: {error}"))
}

pub(crate) fn context(policy: ResourcePolicy) -> RunContext {
    RunContext {
        host: HostResources::detect().expect("host"),
        policy,
        cancel: Cancel::new(),
        summary: None,
    }
}

pub(crate) fn admission_request(
    measurement_set: &Path,
    image_name: &Path,
    imsize: usize,
    controls: Value,
) -> ImagingRequest {
    let mut values = serde_json::json!({
        "vis": measurement_set,
        "imagename": image_name,
        "imsize": imsize,
        "cell": format!("{}arcsec", synthetic_observation::CELL_ARCSEC),
        "weighting": "natural",
        "niter": 200,
        "cycleniter": 50,
        "gain": 0.2,
        "threshold": "1mJy",
    });
    values
        .as_object_mut()
        .expect("controls")
        .extend(controls.as_object().expect("overrides").clone());
    request(values)
}

pub(crate) fn held_by_process() -> u64 {
    let policy = ResourcePolicy::Explicit {
        workers: 1,
        memory: HOST.available_memory,
    };
    HOST.available_memory - casa_imaging_runtime::free_memory(&HOST, &policy)
}

pub(crate) fn warm(measurement_set: &Path, root: &Path) {
    crate::execute(
        &admission_request(
            measurement_set,
            &root.join("warm"),
            64,
            serde_json::json!({ "niter": 0 }),
        ),
        RunContext {
            host: HOST,
            ..context(ResourcePolicy::Explicit {
                workers: 1,
                memory: AMPLE,
            })
        },
    )
    .expect("the warming run");
    ALLOWANCE.store(disk_bytes(measurement_set), Ordering::Relaxed);
}

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

pub(crate) struct Measured {
    pub(crate) outcome: Result<ImagingOutcome, ApplicationDispatchError>,
    pub(crate) peak: i64,
    pub(crate) excess: i64,
    pub(crate) reserved: u64,
}

struct SeedRecorder;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SeedRecorder {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "casa_imaging::model_seed" {
            return;
        }
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
}

pub(crate) fn measure(request: &ImagingRequest, workers: usize, ceiling: u64) -> Measured {
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
    SIGNED_PEAK.store(0, Ordering::Relaxed);
    EXCESS.store(i64::MIN, Ordering::Relaxed);
    RESERVED_PEAK.store(0, Ordering::Relaxed);
    let _ = STARTED.get_or_init(Instant::now);
    let context = RunContext {
        host: HOST,
        ..context(ResourcePolicy::Explicit {
            workers,
            memory: ceiling,
        })
    };
    let recorder = tracing_subscriber::layer::SubscriberExt::with(
        tracing_subscriber::registry(),
        SeedRecorder,
    );
    BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let outcome = tracing::subscriber::with_default(recorder, || crate::execute(request, context));
    MEASURING.store(false, Ordering::Relaxed);
    Measured {
        outcome,
        peak: SIGNED_PEAK.load(Ordering::Relaxed),
        excess: EXCESS.load(Ordering::Relaxed),
        reserved: RESERVED_PEAK.load(Ordering::Relaxed),
    }
}

pub(crate) fn admission_breach(label: &str, measured: &Measured) -> Option<String> {
    (measured.excess > 0).then(|| {
        format!(
            "{label}: the live heap exceeded the admitted memory by {} bytes (peak {} bytes, \
             {} reserved)",
            measured.excess, measured.peak, measured.reserved
        )
    })
}

pub(crate) fn check(
    breaches: &mut Vec<String>,
    label: &str,
    request: &ImagingRequest,
    workers: usize,
    ceiling: u64,
) -> Measured {
    let measured = measure(request, workers, ceiling);
    if let Err(error) = &measured.outcome {
        breaches.push(format!("{label}: {error}"));
    }
    breaches.extend(admission_breach(label, &measured));
    measured
}
