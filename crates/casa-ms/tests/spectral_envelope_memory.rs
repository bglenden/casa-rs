// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MFS spectral envelope reducer holds at most what it charges
//! ([`SelectedObservationSpectralEnvelopeReducer::retained_bytes`]), through
//! `observe` and `finish`, and what it charges follows the selection's
//! distinct `TIME·(DATA_DESC_ID+1)` keys, not MAIN's rows (#722).
//!
//! This binary's global allocator counts the live heap, and the law runs
//! alone in a process of its own ([`alone`]). The rows are read into memory
//! and one reduction is run unmeasured first, so the walk's buffers, the
//! table-read cache and the frame engine's Measures caches are live before
//! the baseline is taken.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{
    MeasurementSet, MsSelectionIoBudget, SPECTRAL_ENVELOPE_KEY_BYTES, SelectedObservationRow,
    SelectedObservationSpectralWindow, SyntheticObservationRequest, SyntheticSpectralSetup,
    generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};
use casa_types::measures::frequency::FrequencyRef;

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

/// A track of `integrations` one-second integrations over `antennas` in
/// two spectral windows, so two data descriptions: `antennas·(antennas−1)/2`
/// rows per integration and data description, two keys per integration.
fn track(root: &Path, antennas: usize, integrations: usize) -> PathBuf {
    let measurement_set = root.join(format!("envelope-{antennas}-{integrations}.ms"));
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas()
            .into_iter()
            .take(antennas)
            .collect(),
    );
    request.duration_seconds = integrations as f64;
    request.integration_seconds = 1.0;
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.spectral_windows = [44.0e9, 45.0e9]
        .map(|start_frequency_hz| SyntheticSpectralSetup {
            name: format!("{start_frequency_hz}"),
            start_frequency_hz,
            channel_width_hz: 1.0e6,
            channel_count: 1,
        })
        .to_vec();
    generate_synthetic_observation_ms(&request).expect("synthesise the envelope MS");
    measurement_set
}

/// Every selected row of both data descriptions, read into memory.
fn rows(ms: &MeasurementSet) -> Vec<SelectedObservationRow> {
    let selection = ms
        .selected_observation_row_selection(&[0, 1], None, None, None)
        .expect("selection");
    let mut rows = Vec::with_capacity(ms.row_count());
    ms.visit_selected_observation_rows(
        &selection,
        MsSelectionIoBudget {
            available_bytes: 4 << 20,
            maximum_live_blocks: 2,
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| rows.push(row),
    )
    .expect("walk the rows");
    rows
}

/// The window edges of both spectral windows.
struct Windows {
    frequencies_hz: Vec<Vec<f64>>,
    widths_hz: Vec<Vec<f64>>,
    channels: Vec<usize>,
}

impl Windows {
    fn read(ms: &MeasurementSet) -> Self {
        let window = ms.spectral_window().expect("SPECTRAL_WINDOW");
        Self {
            frequencies_hz: (0..2)
                .map(|spw| window.chan_freq(spw).expect("CHAN_FREQ"))
                .collect(),
            widths_hz: (0..2)
                .map(|spw| window.chan_width(spw).expect("CHAN_WIDTH"))
                .collect(),
            channels: vec![0],
        }
    }

    fn borrowed(&self) -> [SelectedObservationSpectralWindow<'_>; 2] {
        [0, 1].map(|spw| {
            SelectedObservationSpectralWindow::borrow_selected(
                spw as u32,
                FrequencyRef::TOPO,
                &self.frequencies_hz[spw],
                &self.widths_hz[spw],
                &self.channels,
            )
        })
    }
}

/// Reduce `rows` under `available` bytes: the envelope, or the reducer's
/// refusal, with the most it charged.
fn reduce(
    ms: &MeasurementSet,
    engine: &MsCalEngine,
    windows: &Windows,
    rows: &[SelectedObservationRow],
    available: usize,
) -> (Result<[f64; 2], String>, usize) {
    let selection = ms
        .selected_observation_row_selection(&[0, 1], None, None, None)
        .expect("selection");
    let mut reducer = match ms.selected_observation_spectral_envelope_reducer(
        &selection,
        windows.borrowed(),
        FrequencyRef::LSRK,
        engine,
        available,
    ) {
        Ok(reducer) => reducer,
        Err(error) => return (Err(error.to_string()), 0),
    };
    for row in rows {
        if let Err(error) = reducer.observe(*row) {
            return (Err(error.to_string()), reducer.retained_bytes());
        }
    }
    let charged = reducer.retained_bytes();
    (
        reducer
            .finish()
            .map(|envelope| envelope.edges_hz())
            .map_err(|error| error.to_string()),
        charged,
    )
}

/// The distinct `TIME·(DATA_DESC_ID+1)` keys of `rows`.
fn distinct_keys(rows: &[SelectedObservationRow]) -> usize {
    rows.iter()
        .map(|row| {
            (row.time_mjd_seconds() * (f64::from(row.data_description_id()) + 1.0)).to_bits()
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

/// `rows` in a fixed scrambled order, so the map's nodes split as they do
/// for rows that do not arrive in key order.
fn scrambled(rows: &[SelectedObservationRow]) -> Vec<SelectedObservationRow> {
    let mut order = (0..rows.len()).collect::<Vec<_>>();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for index in (1..order.len()).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        order.swap(index, (state >> 33) as usize % (index + 1));
    }
    order.into_iter().map(|index| rows[index]).collect()
}

/// Many distinct keys, with every key's rows repeated non-consecutively in
/// the scrambled order: the live heap of `observe` and `finish` stays
/// within the reducer's charge, which is its fixed part and
/// [`SPECTRAL_ENVELOPE_KEY_BYTES`] per key.
#[test]
fn observe_and_finish_hold_at_most_what_the_envelope_charges() {
    if !alone("observe_and_finish_hold_at_most_what_the_envelope_charges") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let ms = MeasurementSet::open(track(root.path(), 3, 6_000)).expect("open");
    let engine = MsCalEngine::new(&ms).expect("frame engine");
    let windows = Windows::read(&ms);
    let in_order = rows(&ms);
    let keys = distinct_keys(&in_order);
    assert!(keys >= 10_000, "{keys} keys");
    let mut breaches = Vec::new();
    for (label, rows) in [
        ("in order", in_order.clone()),
        ("scrambled", scrambled(&in_order)),
    ] {
        // Warm the engine's Measures caches and the allocator's pools.
        let (warm, _) = reduce(&ms, &engine, &windows, &rows, usize::MAX);
        let warm = warm.expect("warm reduction");
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let (edges, charged) = reduce(&ms, &engine, &windows, &rows, usize::MAX);
        let peak = PEAK.load(Ordering::Relaxed) - baseline;
        assert_eq!(edges.expect("measured reduction"), warm, "{label}");
        eprintln!(
            "{label}: {keys} keys, {} rows: peak {peak} bytes, charged {charged} \
             ({:.1} bytes a key)",
            rows.len(),
            peak as f64 / keys as f64
        );
        if peak > charged as u64 {
            breaches.push(format!(
                "{label}: the reduction held {peak} bytes, more than its {charged}"
            ));
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// A MAIN of many baselines per integration, so far more rows than keys:
/// the charge follows the keys. A budget of the fixed part and a key's
/// bytes per key suffices, where one row's 16 bytes per MAIN row would not;
/// one key's bytes fewer is refused, naming the keys.
#[test]
fn the_envelope_charge_follows_the_keys_not_the_rows() {
    let root = tempfile::tempdir().expect("test root");
    let ms = MeasurementSet::open(track(root.path(), 27, 20)).expect("open");
    let engine = MsCalEngine::new(&ms).expect("frame engine");
    let windows = Windows::read(&ms);
    let rows = rows(&ms);
    let keys = distinct_keys(&rows);
    assert_eq!(rows.len(), keys * 351, "351 baselines per key");
    let (_, charged) = reduce(&ms, &engine, &windows, &rows, usize::MAX);
    let (no_keys, fixed) = reduce(&ms, &engine, &windows, &[], usize::MAX);
    no_keys.expect_err("no rows, no envelope");
    assert_eq!(charged, fixed + keys * SPECTRAL_ENVELOPE_KEY_BYTES);
    assert!(
        charged < 16 * ms.row_count(),
        "{charged} bytes for {} rows",
        ms.row_count()
    );
    let (enough, _) = reduce(&ms, &engine, &windows, &rows, charged);
    enough.expect("the keys' bytes suffice");
    let (short, _) = reduce(&ms, &engine, &windows, &rows, charged - 1);
    let refusal = short.expect_err("one key's bytes short");
    assert!(
        refusal.contains("distinct TIME·(DATA_DESC_ID+1) keys"),
        "{refusal}"
    );
}

/// Parser peaks must depend on term count, not interval width. The immutable
/// observation manifest then owns the resolved predicates, and its actual peak
/// allocation must stay within the same charge used by imaging admission.
#[test]
fn numeric_selector_width_does_not_grow_the_heap_and_its_owner_covers_its_peak() {
    use casa_imaging_model::{
        CorrelationProduct, CorrelationSelection, DataDescriptionSelection, ObservationSelection,
        SelectedMainRow, SelectedRows, SpectralWindowSelection,
    };
    use casa_types::Correlation;

    if !alone("numeric_selector_width_does_not_grow_the_heap_and_its_owner_covers_its_peak") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let ms = MeasurementSet::open(track(root.path(), 2, 1)).expect("open");
    for (narrow, wide) in [
        ("0~1", "0~2147483647"),
        ("1~2,5~6", "1~2,5~2147483647"),
        (
            "0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38",
            "0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38~2147483647",
        ),
    ] {
        let mut measurements = [(0, 0, 0, 0); 2];
        for (index, text) in [narrow, wide].into_iter().enumerate() {
            // DDID metadata and the MS are already held by the survey. Measure
            // just numeric parsing here, independently of manifest ownership.
            let base = ms
                .selected_observation_row_selection(&[0], None, None, None)
                .expect("base row selection");
            let before = LIVE.load(Ordering::Relaxed);
            PEAK.store(before, Ordering::Relaxed);
            let parsed = base
                .with_numeric_row_selectors(Some(text), Some(text))
                .expect("numeric predicates");
            let parse_peak = PEAK.load(Ordering::Relaxed) - before;
            let parse_held = LIVE.load(Ordering::Relaxed) - before;
            let predicate_charge = parsed.rows().scans().allocated_bytes().unwrap()
                + parsed.rows().observations().allocated_bytes().unwrap();
            assert_eq!(parse_held, predicate_charge as u64);

            // This is the production survey-to-manifest transfer: clone the
            // resolved predicates, then keep them beside exact selected rows
            // and coordinate catalogs. No bulk visibility sample is needed.
            let before = LIVE.load(Ordering::Relaxed);
            PEAK.store(before, Ordering::Relaxed);
            let owner = ObservationSelection::new(
                SelectedRows::from_ordered_main_rows(1, [SelectedMainRow::new(0, 0)])
                    .expect("selected row"),
                parsed.rows().clone(),
                vec![DataDescriptionSelection::new(0, 0, 0)],
                vec![SpectralWindowSelection::new(0, vec![0])],
                vec![CorrelationSelection::new(
                    0,
                    vec![CorrelationProduct::new(0, Correlation::XX)],
                )],
            );
            let owner_peak = PEAK.load(Ordering::Relaxed) - before;
            let charged = owner.retained_manifest_bytes().expect("manifest bytes") as u64;
            assert!(
                owner_peak > 0 && owner_peak <= charged,
                "owner peak {owner_peak} B exceeds its {charged} B charge"
            );
            assert!(owner.rows_filter().scans().holds(1) || owner.rows_filter().scans().holds(0));
            measurements[index] = (parse_peak, parse_held, owner_peak, charged);
            eprintln!(
                "{text}: parse peak {parse_peak} B, retained predicates {parse_held} B, owner peak {owner_peak} B, owner charge {charged} B"
            );
        }
        assert_eq!(
            measurements[0], measurements[1],
            "interval width changes neither parsing nor owner allocation"
        );
    }
}
