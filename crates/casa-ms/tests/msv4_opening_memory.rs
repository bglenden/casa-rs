// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv4 adapter's opening is granted before it decodes and holds at
//! most what it was granted, and an opened processing set holds at most
//! [`Msv4Observation::held_bytes`] (Astra's review of #1013): over XRADIO's
//! conversions of the trial fixtures and over conversions growing in times,
//! baselines, channels and data groups. An opening beyond its budget is
//! refused before it decodes a dataset.
//!
//! This binary's global allocator counts the live heap; each law compares a
//! peak, or what stays live, beyond the heap live before it with the bound.
//! Each store is opened once before it is measured, so that what the first
//! opening of a process initialises once is not counted.
#![cfg(all(feature = "msv4", unix))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
use casa_ms::observation_io::{Msv4Codec, Msv4Observation, OpenOptions};
use casa_ms::{
    MeasurementSet, SyntheticField, SyntheticObservationRequest, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};
use casa_observation::ObservationError;
use casa_tables::ColumnSchema;
use casa_types::PrimitiveType;
use casa_types::{ArrayValue, Value};

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

/// Run `measured`, returning its value, its peak beyond the heap live
/// before it and what stays live after it.
fn measured<T>(measured: impl FnOnce() -> T) -> (T, u64, u64) {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let value = measured();
    let peak = PEAK.load(Ordering::Relaxed) - baseline;
    let kept = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    (value, peak, kept)
}

/// The trial fixture `name`, unpacked into `root`.
fn fixture(root: &Path, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ms_trial")
        .join(format!("{name}.tgz"));
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).expect("the fixture"),
    ))
    .unpack(root)
    .expect("unpacked");
    root.join(name)
}

/// The dimensions a converted store grows in.
#[derive(Clone, Copy, Debug)]
struct Shape {
    integrations: usize,
    antennas: usize,
    channels: usize,
    corrected: bool,
}

/// A VLA-A track of `shape` towards one field, converted to MSv4: with
/// `corrected`, a `CORRECTED_DATA` beside `DATA`, so that it has two data
/// groups.
fn converted(root: &Path, label: &str, shape: Shape) -> PathBuf {
    let measurement_set = root.join(format!("{label}.ms"));
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas()[..shape.antennas].to_vec(),
    );
    request.duration_seconds = 10.0 * shape.integrations as f64;
    request.integration_seconds = 10.0;
    request.predict_model = false;
    request.fields = vec![SyntheticField {
        name: request.field_name.clone(),
        phase_center_rad: request.phase_center_rad,
    }];
    request.spectral_windows.truncate(1);
    request.spectral_windows[0].channel_count = shape.channels;
    generate_synthetic_observation_ms(&request).expect("synthesise the MS");
    if shape.corrected {
        let mut ms = MeasurementSet::open(&measurement_set).expect("open");
        let Some(Value::Array(ArrayValue::Complex32(data))) = ms
            .main_table()
            .column_accessor("DATA")
            .expect("DATA")
            .get(0)
            .expect("row 0")
            .cloned()
        else {
            panic!("DATA is a Complex32 array");
        };
        let zero = ndarray::ArrayD::zeros(data.shape());
        ms.main_table_mut()
            .add_column(
                ColumnSchema::array_fixed(
                    "CORRECTED_DATA",
                    PrimitiveType::Complex32,
                    data.shape().to_vec(),
                ),
                Some(Value::Array(ArrayValue::Complex32(zero))),
            )
            .expect("CORRECTED_DATA");
        ms.save().expect("saved");
    }
    let processing_set = root.join(format!("{label}.ps.zarr"));
    to_msv4(
        &measurement_set,
        &processing_set,
        &ConversionOptions {
            data_groups: None,
            accept: vec![
                LossKind::Sigma,
                LossKind::RowFlags,
                LossKind::FlagCategories,
                LossKind::Tables,
                LossKind::Columns,
            ],
            batch_bytes: 64 << 20,
            workers: 1,
            chunks: [0, 0, 0, 0],
            codec: Msv4Codec::BloscLz4,
            padding: PaddingRows::Flagged,
            creator: "msv4 opening law".into(),
            creation_date: "2026-10-30T00:00:00+00:00".into(),
        },
    )
    .expect("converted");
    processing_set
}

/// A grant recording the largest total asked for, refusing beyond
/// `budget`.
fn recording(
    budget: usize,
    granted: &mut usize,
) -> impl FnMut(usize) -> Result<(), ObservationError> + '_ {
    move |total| {
        if total > budget {
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

/// The laws, one after another: the allocator counts every thread's heap,
/// so this binary holds one test.
#[test]
fn the_msv4_opening_holds_at_most_its_grant_and_its_count() {
    let root = tempfile::tempdir().expect("test root");
    let mut stores: Vec<(String, PathBuf)> = [
        "ms_trial.ps.zarr",
        "ms_trial_ephemeris.ps.zarr",
        "ms_trial_ephemeris_interpolated.ps.zarr",
        "ms_trial_lines.ps.zarr",
    ]
    .into_iter()
    .map(|name| (name.to_string(), fixture(root.path(), name)))
    .collect();
    let base = Shape {
        integrations: 10,
        antennas: 6,
        channels: 4,
        corrected: false,
    };
    for (label, shape) in [
        ("base", base),
        (
            "times",
            Shape {
                integrations: 400,
                ..base
            },
        ),
        (
            "baselines",
            Shape {
                antennas: 27,
                ..base
            },
        ),
        (
            "channels",
            Shape {
                channels: 256,
                ..base
            },
        ),
        (
            "groups",
            Shape {
                integrations: 400,
                corrected: true,
                ..base
            },
        ),
    ] {
        stores.push((label.to_string(), converted(root.path(), label, shape)));
    }
    for (label, path) in &stores {
        drop(Msv4Observation::open(path).expect("open"));
        let mut granted = 0;
        let (opened, peak, kept) = measured(|| {
            Msv4Observation::open_granted(
                path,
                &OpenOptions::default(),
                &mut recording(usize::MAX, &mut granted),
            )
            .expect("open")
        });
        let held = opened.held_bytes();
        eprintln!(
            "{label}: granted {granted}, peak {peak}, kept {kept}, held_bytes {held}, groups {}",
            opened.catalogue().partitions[0].data_groups.len()
        );
        assert!(
            peak <= granted as u64,
            "{label}: the opening held {peak} bytes, beyond the {granted} granted"
        );
        assert!(
            kept <= held as u64,
            "{label}: the opened store holds {kept} bytes, beyond its count of {held}"
        );
        drop(opened);
    }
    a_store_beyond_its_budget_is_refused_before_it_decodes(&stores[5].1);
}

/// An opening whose projection exceeds its budget is refused before it
/// decodes any dataset: it holds no more than the metadata it read to
/// project.
fn a_store_beyond_its_budget_is_refused_before_it_decodes(path: &Path) {
    let mut projected = 0;
    drop(
        Msv4Observation::open_granted(
            path,
            &OpenOptions::default(),
            &mut recording(usize::MAX, &mut projected),
        )
        .expect("open"),
    );
    let budget = projected - 1;
    let mut granted = 0;
    let (opened, peak, _) = measured(|| {
        Msv4Observation::open_granted(
            path,
            &OpenOptions::default(),
            &mut recording(budget, &mut granted),
        )
    });
    assert!(
        matches!(opened, Err(ObservationError::OverBudget { .. })),
        "{:?}",
        opened.err()
    );
    // What projecting reads: the metadata documents.
    let metadata = 1 << 20;
    eprintln!("refused at {budget} bytes, peak {peak}, projection {projected}");
    assert!(
        peak <= metadata,
        "the refused opening held {peak} bytes before its refusal"
    );
}
