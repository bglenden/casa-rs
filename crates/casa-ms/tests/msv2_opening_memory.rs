// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2 adapter's memory bounds hold what they say
//! ([`Msv2Observation::opening_bytes`], [`Msv2Observation::pointing_bytes`]),
//! so a caller can admit them before it opens or queries.
//!
//! This binary's global allocator counts the live heap; each law compares a
//! peak beyond the heap live before it with the bound. The tiles and
//! buckets a read loads go to the table layer's caches, which are charged
//! on their own, so each is read once before it is measured.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_ms::observation_io::{Msv2Observation, OpenOptions};
use casa_ms::{
    MeasurementSet, SyntheticField, SyntheticObservationRequest, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};
use casa_observation::{
    AntennaId, Observation, ObservationError, PointingDirection, PointingRequest,
};
use casa_types::{ScalarValue, Value};

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

/// A VLA-A track of `integrations` ten-second integrations towards one
/// field of its own, so `POINTING` holds a row per antenna and integration:
/// 351 baseline rows per integration.
fn pointed_track(root: &Path, integrations: usize) -> PathBuf {
    let measurement_set = root.join("pointed.ms");
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = 10.0 * integrations as f64;
    request.integration_seconds = 10.0;
    request.predict_model = false;
    request.fields = vec![SyntheticField {
        name: request.field_name.clone(),
        phase_center_rad: request.phase_center_rad,
    }];
    generate_synthetic_observation_ms(&request).expect("synthesise the MS");
    measurement_set
}

/// The laws, one after another: the allocator counts every thread's heap,
/// so this binary holds one test.
#[test]
fn the_msv2_adapter_holds_at_most_its_bounds() {
    let root = tempfile::tempdir().expect("test root");
    opening_and_pointing_hold_at_most_their_bounds(&root.path().join("regular"));
    an_opening_beyond_its_budget_is_refused_before_its_grids(&root.path().join("jittered"));
}

/// A grant that refuses a total beyond `budget`, recording the largest
/// total it granted and the total it last refused.
fn budget_grant<'a>(
    budget: usize,
    granted: &'a mut usize,
    refused: &'a mut Option<usize>,
) -> impl FnMut(usize) -> Result<(), ObservationError> + 'a {
    move |total| {
        if total > budget {
            *refused = Some(total);
            return Err(ObservationError::OverBudget {
                what: "the opening",
                required: total,
                budget,
            });
        }
        *granted = (*granted).max(total);
        Ok(())
    }
}

/// Opening a track holds at most [`Msv2Observation::opening_bytes`], and
/// granted ([`Msv2Observation::open_granted`]) at most what it asked for;
/// a pointing query under [`Msv2Observation::pointing_bytes`] is answered
/// and holds at most it.
fn opening_and_pointing_hold_at_most_their_bounds(root: &Path) {
    std::fs::create_dir(root).expect("a fixture directory");
    let path = pointed_track(root, 120);
    let rows = MeasurementSet::open(&path).expect("open").row_count();
    let bound = Msv2Observation::opening_bytes(rows);
    // One opening loads what the table layer caches.
    drop(Msv2Observation::open(&path).expect("open"));
    let before = LIVE.load(Ordering::Relaxed);
    let (opened, opening) = peak_of(|| Msv2Observation::open(&path).expect("open"));
    let retained = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    assert!(
        retained <= opened.retained_bytes() as u64,
        "the open observation retains {retained} bytes, beyond its {}-byte charge",
        opened.retained_bytes()
    );
    eprintln!("opening {rows} rows: peak {opening} bytes, bound {bound}");
    assert!(
        opening <= bound as u64,
        "opening held {opening} bytes, beyond {bound}"
    );
    let ((granted, asked), held) = peak_of(|| {
        let (mut asked, mut refused) = (0, None);
        let granted = Msv2Observation::open_granted(
            &path,
            &OpenOptions::default(),
            &mut budget_grant(usize::MAX, &mut asked, &mut refused),
        );
        (granted, asked)
    });
    eprintln!("opening granted asked for {asked} bytes and held {held}");
    granted.expect("an opening granted");
    assert!(
        held <= asked as u64,
        "the opening held {held} bytes, beyond the {asked} it asked for"
    );

    let budget = opened.pointing_bytes().expect("pointing bytes");
    let id = opened.catalogue().partition_ids()[0];
    let request = PointingRequest {
        partition: id,
        direction: PointingDirection::Beam,
        antennas: (0..opened.catalogue().antennas.len() as u32)
            .map(AntennaId)
            .collect(),
        times: f64::MIN..=f64::MAX,
        budget_bytes: budget,
    };
    drop(opened.pointing(&request).expect("warm"));
    let (series, query) = peak_of(|| opened.pointing(&request).expect("within its bound"));
    eprintln!(
        "a query of every POINTING row: peak {query} bytes, bound {budget}, series {}",
        series.held_bytes()
    );
    assert!(
        query <= budget as u64,
        "the query held {query} bytes, beyond {budget}"
    );
    assert!(series.held_bytes() <= budget);
}

/// A track whose times differ by baseline group (every 50 rows) fills each
/// time of its grid with 50 baselines of 351: the first pass fits the
/// opening's budget and its grids, seven times its rows' cells, do not, so
/// they are refused before they are allocated, and opening holds at most
/// that budget but for the catalogue it read first.
fn an_opening_beyond_its_budget_is_refused_before_its_grids(root: &Path) {
    std::fs::create_dir(root).expect("a fixture directory");
    let path = pointed_track(root, 120);
    let mut ms = MeasurementSet::open(&path).expect("open");
    let rows = ms.row_count();
    let times = ms
        .main_table()
        .column_accessor("TIME")
        .and_then(|column| column.scalar_cells_owned_for_rows(&(0..rows).collect::<Vec<_>>()))
        .expect("TIME");
    for (row, time) in times.into_iter().enumerate() {
        let Some(ScalarValue::Float64(time)) = time else {
            panic!("TIME of row {row} is {time:?}");
        };
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "TIME",
                Value::Scalar(ScalarValue::Float64(time + (row / 50) as f64 * 1.0e-6)),
            )
            .expect("TIME");
    }
    ms.save().expect("save");
    drop(ms);
    let grids = rows.div_ceil(50) * 351 * size_of::<u32>();
    let budget = Msv2Observation::opening_bytes(rows);
    let open = |granted: &mut usize, refused: &mut Option<usize>| {
        Msv2Observation::open_granted(
            &path,
            &OpenOptions::default(),
            &mut budget_grant(budget, granted, refused),
        )
    };
    drop(open(&mut 0, &mut None));
    let (mut granted, mut refused) = (0, None);
    let (opened, held) = peak_of(|| open(&mut granted, &mut refused));
    eprintln!(
        "a jittered opening: peak {held} bytes, budget {budget}, granted {granted}, refused \
         {refused:?}, grids {grids}"
    );
    assert!(
        matches!(opened, Err(ObservationError::OverBudget { .. })),
        "{:?}",
        opened.err()
    );
    // The first pass was granted; the total refused holds the grids.
    let refused = refused.expect("a refused total");
    assert!(granted <= budget && refused > budget && refused >= grids);
    // The tables and catalogue, asked for first, are in the budget.
    assert!(
        held <= budget as u64,
        "the refused opening held {held} bytes, beyond its budget of {budget}"
    );
}
