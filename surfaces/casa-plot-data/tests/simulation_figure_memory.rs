// SPDX-License-Identifier: LGPL-3.0-or-later
//! Whole-task plot admission: composed ALMA/ACA/TP figures stay within admission.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use casa_images::PagedImage;
use casa_imaging_runtime::{
    HostResources, ReservationWindow, ResourcePolicy, charge_process_caches, process_cache_bytes,
    reserved_memory,
};
use casa_plot_data::simulation::simalma_with_plots;
use casa_simulation::{BackendRequest, SessionResources};
use serde_json::json;

#[path = "../../../libs/casa/casa-simulation/tests/common/simalma_fixture.rs"]
mod fixture;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
static EXCESS: AtomicI64 = AtomicI64::new(i64::MIN);

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
    EXCESS.fetch_max(live as i64 - reserved_memory() as i64, Ordering::Relaxed);
}

fn shrink(bytes: u64) {
    LIVE.fetch_sub(bytes, Ordering::Relaxed);
}

fn excess_now() -> i64 {
    LIVE.load(Ordering::Relaxed) as i64 - reserved_memory() as i64
}

unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded directly to `System` with identical layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        shrink(layout.size() as u64);
        // SAFETY: `pointer` was allocated via `System` with `layout`.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded directly to `System` with identical layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `pointer` was allocated via `System` with `layout`.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            let old = layout.size() as u64;
            let new = new_size as u64;
            if new >= old {
                grow(new - old);
            } else {
                shrink(old - new);
            }
        }
        resized
    }
}

const LAW_VAR: &str = "CASA_RS_COUNTING_LAW";
const SETTLE: &str = "a_counting_law_process_settles";
const ALLOWANCE_BYTES: u64 = 512 * 1024;
static SETTLED: AtomicBool = AtomicBool::new(false);

#[test]
#[ignore = "alone runs it before a law, in the law's process"]
fn a_counting_law_process_settles() {
    std::thread::sleep(std::time::Duration::from_millis(10));
    SETTLED.store(true, Ordering::Relaxed);
}

fn alone(name: &str) -> bool {
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
    println!("{stdout}");
    assert!(
        output.status.success(),
        "{name} alone: {}\n{stdout}",
        output.status
    );
    assert!(stdout.contains(&ran), "{name} did not run alone:\n{stdout}");
    false
}

fn resources(budget: u64) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 2,
            performance_cores: 2,
            available_memory: 8 << 30,
            metal: false,
        },
        ResourcePolicy::Explicit {
            workers: 2,
            memory: process_cache_bytes() + budget,
        },
        BackendRequest::Cpu,
    )
    .expect("resources")
}

#[test]
fn composed_simulation_figures_stay_within_admission_at_two_image_sizes() {
    if !alone("composed_simulation_figures_stay_within_admission_at_two_image_sizes") {
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
