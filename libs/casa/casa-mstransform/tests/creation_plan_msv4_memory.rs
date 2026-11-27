// SPDX-License-Identifier: LGPL-3.0-or-later
//! A creation plan over an MSv4 input holds what it admits
//! ([`CreationPlan::charges`]): making it holds at most its planning charge,
//! and writing it at most its writing charge beyond what the plan keeps.
//!
//! This binary's global allocator counts the live heap; each law compares
//! a peak beyond the heap live before it with the charge.
#![cfg(feature = "msv4")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_ms::observation_io::Msv4Codec;
use casa_ms::observation_io::creation::{
    CreationInput, CreationOptions, CreationPlan, Destination, LossKind, MemoryCharges, PaddingRows,
};
use casa_mstransform::convert::{ConversionOptions, to_msv4};
use casa_observation::{MergeTolerance, ObservationError};
use casa_simulation::synthetic::{
    Field, ObservationRequest, generate_unflagged, tutorial_vla_a_antennas,
};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their largest value.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: forwards to `System` with the caller's arguments and adds atomic
// bookkeeping only.
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
        let (old, new) = (layout.size() as u64, new_size as u64);
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if moved != pointer {
                // Old and new were held at once while the block moved.
                PEAK.fetch_max(LIVE.load(Ordering::Relaxed) + new, Ordering::Relaxed);
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

/// Run `measured`, returning its value and its peak beyond the heap live
/// before it.
fn peak_of<T>(measured: impl FnOnce() -> T) -> (T, u64) {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let value = measured();
    (value, PEAK.load(Ordering::Relaxed) - baseline)
}

/// What the charges leave out: paths and messages.
const ALLOWANCE: u64 = 64 << 10;

/// Every loss creating the fixture takes.
const ACCEPT: [LossKind; 4] = [
    LossKind::Sigma,
    LossKind::Tables,
    LossKind::Columns,
    LossKind::Defaulted,
];

fn options(batch_bytes: usize, memory_bytes: usize) -> CreationOptions {
    CreationOptions {
        copy_pointing: true,
        data_groups: None,
        accept: ACCEPT.to_vec(),
        batch_bytes,
        memory_bytes,
        padding: PaddingRows::Omitted,
        tolerance: MergeTolerance::default(),
        history: Vec::new(),
        creation_date: "2026-10-29T00:00:00".to_string(),
    }
}

/// A VLA-A track `name` of 6 ten-second integrations of 32 channels
/// towards one field, created with three data groups.
fn track(root: &Path, name: &str) -> PathBuf {
    let synthetic = root.join(format!("synthetic-{name}.ms"));
    let mut request = ObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &synthetic,
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = 60.0;
    request.integration_seconds = 10.0;
    request.predict_model = false;
    request.spectral_windows[0].channel_count = 32;
    request.spectral_windows[0].channel_width_hz = 1.0e6;
    request.fields = vec![Field {
        name: request.field_name.clone(),
        phase_center_rad: request.phase_center_rad,
    }];
    generate_unflagged(&request).expect("synthesise the MS");
    let three = root.join(format!("{name}.ms"));
    let groups = ["base", "corrected", "model"].map(String::from).to_vec();
    CreationPlan::new(
        &[CreationInput::new(&synthetic)],
        &CreationOptions {
            data_groups: Some(groups),
            ..options(1 << 24, 1 << 30)
        },
        Destination::New(three.clone()),
    )
    .and_then(CreationPlan::write)
    .expect("three data groups");
    std::fs::remove_dir_all(&synthetic).expect("remove the synthetic MS");
    three
}

/// The plan over `inputs` at `destination`.
fn plan(
    inputs: &[CreationInput],
    destination: &Path,
    options: &CreationOptions,
) -> Result<CreationPlan, ObservationError> {
    CreationPlan::new(inputs, options, Destination::New(destination.to_path_buf()))
}

/// Make and write the plan over `inputs` at `destination`, asserting that
/// each holds at most its charge; the charges.
fn holds_its_charges(
    inputs: &[CreationInput],
    destination: &Path,
    options: &CreationOptions,
) -> MemoryCharges {
    let before = LIVE.load(Ordering::Relaxed);
    let (made, planning) = peak_of(|| plan(inputs, destination, options));
    let kept = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    let made = made.expect("the plan within its budget");
    let charges = made.charges();
    assert!(
        kept <= charges.kept as u64 + ALLOWANCE,
        "the plan keeps {kept} bytes, beyond its {} charged",
        charges.kept
    );
    let (written, writing) = peak_of(|| made.write());
    let rows = written.expect("written within its budget").rows;
    eprintln!(
        "{rows} rows, batch {} and budget {}: kept {kept} of {}, planning peak {planning} of \
         {}, writing peak {writing} of {} (writer {}, batch {}), least {}",
        options.batch_bytes,
        options.memory_bytes,
        charges.kept,
        charges.planning,
        charges.writing,
        charges.writer,
        charges.batch,
        charges.least
    );
    assert!(
        planning <= charges.planning as u64 + ALLOWANCE,
        "making the plan held {planning} bytes, beyond its {} charged",
        charges.planning
    );
    assert!(
        writing <= charges.writing as u64 + ALLOWANCE,
        "writing held {writing} bytes, beyond its {} charged",
        charges.writing
    );
    assert!(
        charges.kept + charges.writing <= options.memory_bytes
            && charges.planning <= options.memory_bytes
    );
    std::fs::remove_dir_all(destination).expect("remove the output");
    charges
}

#[test]
fn a_creation_plan_over_msv4_holds_what_it_admits() {
    casa_tables::set_table_cache_budget_bytes(0);
    let root = tempfile::tempdir().expect("test root");
    let source = track(root.path(), "input0");
    let destination = root.path().join("created.ms");
    let generous = 1 << 30;
    let msv4 = root.path().join("input0.ps.zarr");
    to_msv4(
        &source,
        &msv4,
        &ConversionOptions {
            data_groups: None,
            accept: ACCEPT.to_vec(),
            batch_bytes: 1 << 24,
            workers: 1,
            chunks: [5, 0, 0, 0],
            codec: Msv4Codec::BloscLz4,
            padding: PaddingRows::Flagged,
            creator: "casa-rs".to_string(),
            creation_date: "2026-10-29T00:00:00+00:00".to_string(),
        },
    )
    .expect("an MSv4 input");
    let msv4 = [CreationInput::new(msv4)];
    CreationPlan::new(
        &msv4,
        &options(1 << 24, generous),
        Destination::New(destination.clone()),
    )
    .and_then(CreationPlan::write)
    .expect("warm");
    std::fs::remove_dir_all(&destination).expect("remove the output");
    holds_its_charges(&msv4, &destination, &options(1 << 24, generous));
}
