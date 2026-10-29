// SPDX-License-Identifier: LGPL-3.0-or-later
//! A creation plan holds what it admits ([`CreationPlan::charges`]):
//! making it holds at most its planning charge, and writing it at most its
//! writing charge beyond what the plan keeps, for eight inputs of three
//! data groups with their pointing; for `HISTORY` rows of megabytes, an
//! input whose names and schedule are long strings, a long track's
//! `POINTING`, and an MSv4 input. A batch that cannot hold one integration
//! is refused, and so is a budget one byte below the least the plan is
//! made with, or `HISTORY` rows beyond the budget, each holding at most
//! its budget and leaving nothing behind.
//!
//! This binary's global allocator counts the live heap; each law compares
//! a peak beyond the heap live before it with the charge. The tiles and
//! buckets a read loads go to the table layer's shared cache, which is
//! charged on its own, so the law gives it no budget, and the inputs are
//! read once before anything is measured.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_ms::observation_io::creation::{
    CreationInput, CreationOptions, CreationPlan, Destination, HistoryRow, LossKind, MemoryCharges,
    PaddingRows,
};
use casa_ms::{
    MeasurementSet, SubtableId, SyntheticField, SyntheticObservationRequest,
    generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};
use casa_observation::{MergeTolerance, ObservationError};
use casa_types::{ArrayValue, ScalarValue, Value};

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

/// Inputs.
const INPUTS: usize = 8;

/// Ten-second integrations of each input.
const INTEGRATIONS: usize = 6;

/// Ten-second integrations of the long track: twenty times an input's
/// `POINTING`.
const LONG_TRACK: usize = 120;

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
        data_groups: None,
        accept: ACCEPT.to_vec(),
        batch_bytes,
        memory_bytes,
        padding: PaddingRows::Omitted,
        tolerance: MergeTolerance::default(),
        history: Vec::new(),
    }
}

/// A VLA-A track `name` of `integrations` ten-second integrations of
/// `channels` channels towards one field, `offset_s` after the first
/// input, so `POINTING` holds a row per antenna and integration; created
/// again with `CORRECTED_DATA` (from `DATA`) and `MODEL_DATA` (the initial
/// model) for three data groups.
fn track(
    root: &Path,
    name: &str,
    (offset_s, integrations, channels): (f64, usize, usize),
) -> PathBuf {
    let synthetic = root.join(format!("synthetic-{name}.ms"));
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &synthetic,
        tutorial_vla_a_antennas(),
    );
    request.start_time_mjd_seconds += offset_s;
    request.duration_seconds = 10.0 * integrations as f64;
    request.integration_seconds = 10.0;
    request.predict_model = false;
    request.spectral_windows[0].channel_count = channels;
    request.spectral_windows[0].channel_width_hz = 1.0e6;
    request.fields = vec![SyntheticField {
        name: request.field_name.clone(),
        phase_center_rad: request.phase_center_rad,
    }];
    generate_synthetic_observation_ms(&request).expect("synthesise the MS");
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

/// Input `index`: a track of [`INTEGRATIONS`] integrations of 32 channels
/// (16 for an odd `index`), `index` minutes after the first; alternate
/// inputs have another cell shape, so the MAIN writers' rows change cube at
/// every input.
fn input(root: &Path, index: usize) -> PathBuf {
    let channels = if index.is_multiple_of(2) { 32 } else { 16 };
    track(
        root,
        &format!("input{index}"),
        (60.0 * index as f64, INTEGRATIONS, channels),
    )
}

/// An input whose observing session states long strings: a project, an
/// observer and a `SCHEDULE` line of 16 KiB each (casa-tables writes a
/// string within one 32 KiB bucket).
fn stated(root: &Path) -> PathBuf {
    let path = track(root, "stated", (0.0, INTEGRATIONS, 32));
    let mut ms = MeasurementSet::open(&path).expect("ms");
    let observation = ms
        .subtable_mut(SubtableId::Observation)
        .expect("OBSERVATION");
    let text = |length: usize| "s".repeat(length);
    let lines = |lines: Vec<String>| {
        let count = lines.len();
        Value::Array(ArrayValue::String(
            ndarray::ArrayD::from_shape_vec(vec![count], lines).expect("lines"),
        ))
    };
    for (column, value) in [
        (
            "PROJECT",
            Value::Scalar(ScalarValue::String(text(16 << 10))),
        ),
        (
            "OBSERVER",
            Value::Scalar(ScalarValue::String(text(16 << 10))),
        ),
        ("SCHEDULE", lines(vec![text(16 << 10)])),
    ] {
        observation
            .row_accessor_mut()
            .set_cell(0, column, value)
            .unwrap_or_else(|error| panic!("{column}: {error}"));
    }
    ms.save().expect("save");
    path
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

/// A plan over `inputs` at `destination` under `options` is refused, having
/// held at most its budget; the refusal.
fn refused_within_budget(
    inputs: &[CreationInput],
    destination: &Path,
    options: &CreationOptions,
) -> ObservationError {
    let (refused, held) = peak_of(|| plan(inputs, destination, options).err());
    let refused = refused.expect("refused");
    eprintln!(
        "refused under budget {}: held {held}: {refused}",
        options.memory_bytes
    );
    assert!(
        held <= options.memory_bytes as u64 + ALLOWANCE,
        "a refused plan held {held} bytes, beyond its budget of {}",
        options.memory_bytes
    );
    nothing_left(destination);
    refused
}

/// Nothing a refused plan made is left beside its destination.
fn nothing_left(destination: &Path) {
    assert!(!destination.exists(), "{}", destination.display());
    let parent = destination.parent().expect("a parent");
    let name = destination.file_name().expect("a name").to_string_lossy();
    let left: Vec<_> = std::fs::read_dir(parent)
        .expect("the parent")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|entry| entry.contains(name.as_ref()))
        .collect();
    assert!(left.is_empty(), "left beside the destination: {left:?}");
}

/// `rows` `HISTORY` rows, each message `bytes` long.
fn history(rows: usize, bytes: usize) -> Vec<HistoryRow> {
    let row = HistoryRow {
        time_s: 5.0e9,
        observation: 0,
        message: "h".repeat(bytes),
        priority: "NORMAL".to_string(),
        origin: "concat".to_string(),
        application: "casa-rs".to_string(),
        parameters: vec!["vis=['a.ms']".to_string()],
        command: Vec::new(),
    };
    vec![row; rows]
}

/// The laws, one after another: the allocator counts every thread's heap,
/// so this binary holds one test.
#[test]
fn a_creation_plan_holds_what_it_admits() {
    casa_tables::set_table_cache_budget_bytes(0);
    let root = tempfile::tempdir().expect("test root");
    let inputs: Vec<CreationInput> = (0..INPUTS)
        .map(|index| CreationInput::new(input(root.path(), index)))
        .collect();
    let destination = root.path().join("created.ms");
    let generous = 1 << 30;
    // The first plan and write load what the table layer caches.
    let warm = |inputs: &[CreationInput]| {
        plan(inputs, &destination, &options(1 << 24, generous))
            .and_then(CreationPlan::write)
            .expect("warm");
        std::fs::remove_dir_all(&destination).expect("remove the output");
    };
    warm(&inputs);

    let integration =
        holds_its_charges(&inputs, &destination, &options(1 << 24, generous)).integration;

    // One integration of every group is the least batch: one byte less is
    // refused, not rounded up.
    let refused = refused_within_budget(&inputs, &destination, &options(integration - 1, generous));
    assert!(
        matches!(refused, ObservationError::OverBudget { required, .. } if required == integration),
        "{refused:?}"
    );
    let at_one = holds_its_charges(&inputs, &destination, &options(integration, generous));
    assert_eq!(at_one.batch, integration, "a batch of one integration");

    // The least budget plans and writes within its charges; one byte less
    // is refused before anything is written.
    let least = at_one.least;
    let charges = holds_its_charges(&inputs, &destination, &options(integration, least));
    assert_eq!(charges.least, least);
    let refused = refused_within_budget(&inputs, &destination, &options(integration, least - 1));
    assert!(
        matches!(refused, ObservationError::OverBudget { .. }),
        "{refused:?}"
    );

    // HISTORY rows are charged as the plan keeps them and as writing holds
    // them (4 MiB of messages, each within a string bucket); rows beyond
    // the budget are refused before they are copied.
    let with_history = |(rows, bytes): (usize, usize), memory_bytes: usize| CreationOptions {
        history: history(rows, bytes),
        ..options(integration, memory_bytes)
    };
    holds_its_charges(
        &inputs,
        &destination,
        &with_history((256, 16 << 10), generous),
    );
    let message = 8 << 20;
    let heavy = with_history((1, message), least + (1 << 20));
    let refused = refused_within_budget(&inputs, &destination, &heavy);
    assert!(
        matches!(refused, ObservationError::OverBudget { .. }),
        "{refused:?}"
    );
    let (_, held) = peak_of(|| plan(&inputs, &destination, &heavy).err());
    assert!(
        held < message as u64,
        "a refused HISTORY was copied: {held} bytes"
    );

    // Long strings are charged at their lengths.
    let stated = [CreationInput::new(stated(root.path())), inputs[1].clone()];
    warm(&stated);
    holds_its_charges(&stated, &destination, &options(1 << 24, generous));

    // A long track's POINTING is read, not kept.
    let long = [CreationInput::new(track(
        root.path(),
        "long",
        (0.0, LONG_TRACK, 16),
    ))];
    warm(&long);
    holds_its_charges(&long, &destination, &options(1 << 24, generous));

    #[cfg(feature = "msv4")]
    {
        use casa_ms::observation_io::Msv4Codec;
        use casa_ms::observation_io::convert::{ConversionOptions, to_msv4};
        let msv4 = root.path().join("input0.ps.zarr");
        to_msv4(
            &inputs[0].path,
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
        warm(&msv4);
        holds_its_charges(&msv4, &destination, &options(1 << 24, generous));
    }
}
