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
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
use casa_ms::observation_io::{Msv4Codec, Msv4Observation, OpenOptions};
use casa_ms::{
    MeasurementSet, SyntheticField, SyntheticObservationRequest, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};
use casa_observation::{
    AntennaId, Observation, ObservationError, PartitionId, PointingDirection, PointingRequest,
};
use casa_tables::ColumnSchema;
use casa_types::PrimitiveType;
use casa_types::{ArrayValue, Value};
use zarrs::array::{Array, ArraySubset};
use zarrs::filesystem::FilesystemStore;

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
    if label == "long_labels" {
        request.antennas[0].name = "LONG_".to_string() + &"antenna".repeat(32);
    }
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
                integrations: 1024,
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
    stores.push((
        "long_labels".into(),
        converted(root.path(), "long_labels", base),
    ));
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
    a_dense_pointing_read_holds_at_most_its_budget(&stores[5].1);
    a_pointing_query_holds_at_most_its_charge(&stores[4].1);
    a_pointing_query_holds_at_most_its_charge(&stores[5].1);
    a_pointing_query_holds_at_most_its_charge(&stores[9].1);
    a_pointing_projection_charges_full_chunks_and_refuses_before_time_decode(&stores[4].1);
    a_source_chunk_larger_than_its_logical_array_stays_within_the_opening_grant(&stores[3].1);
}

/// A one-line SOURCE variable can store its value in a much larger Zstd
/// chunk. Admission must charge that physical decode before opening it.
fn a_source_chunk_larger_than_its_logical_array_stays_within_the_opening_grant(path: &Path) {
    let dataset = std::fs::read_dir(path)
        .expect("store entries")
        .map(|entry| entry.expect("entry").path())
        .find(|entry| {
            entry
                .join("field_and_source_base_xds/LINE_REST_FREQUENCY")
                .is_dir()
        })
        .expect("source dataset");
    let variable = dataset.join("field_and_source_base_xds/LINE_REST_FREQUENCY");
    let name = dataset.file_name().expect("dataset name").to_string_lossy();
    let key = format!("/{name}/field_and_source_base_xds/LINE_REST_FREQUENCY");
    let store = Arc::new(FilesystemStore::new(path).expect("store"));
    let original = Array::open(store.clone(), &key).expect("line array");
    let values: Vec<f64> = original
        .retrieve_array_subset(&ArraySubset::new_with_shape(original.shape().to_vec()))
        .expect("original lines");
    let metadata = variable.join("zarr.json");
    let mut document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&metadata).expect("line metadata"))
            .expect("line document");
    let chunk_lines = 1_048_576;
    document["chunk_grid"]["configuration"]["chunk_shape"][1] = chunk_lines.into();
    std::fs::write(
        &metadata,
        serde_json::to_vec(&document).expect("line document"),
    )
    .expect("oversized chunk metadata");
    let array = Array::open(store, &key).expect("oversized line array");
    let mut chunk = vec![f64::NAN; chunk_lines as usize];
    chunk[..values.len()].copy_from_slice(&values);
    array
        .store_chunk(&[0, 0], &chunk)
        .expect("compressed chunk");
    assert!(
        std::fs::metadata(variable.join("c/0/0"))
            .expect("stored chunk")
            .len()
            < 1 << 20,
        "the large decoded chunk must remain compressed on disk"
    );
    drop(Msv4Observation::open(path).expect("warm oversized source"));
    let mut granted = 0;
    let (opened, peak, _) = measured(|| {
        Msv4Observation::open_granted(
            path,
            &OpenOptions::default(),
            &mut recording(usize::MAX, &mut granted),
        )
        .expect("open oversized source")
    });
    assert_eq!(opened.catalogue().sources.len(), 1);
    assert!(
        peak <= granted as u64,
        "oversized source chunk held {peak} bytes, beyond its {granted} grant"
    );
}

/// Enlarging a regular label chunk increases the universal bound, and an
/// oversized time coordinate is refused from metadata before it is decoded.
fn a_pointing_projection_charges_full_chunks_and_refuses_before_time_decode(path: &Path) {
    let opened = Msv4Observation::open(path).expect("open pointing store");
    let dataset = std::fs::read_dir(path)
        .expect("store entries")
        .map(|entry| entry.expect("entry").path())
        .find(|entry| entry.join("pointing_xds").is_dir())
        .expect("pointing dataset");
    let pointing = dataset.join("pointing_xds");
    let name_metadata = pointing.join("antenna_name/zarr.json");
    let mut names: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&name_metadata).expect("name metadata"))
            .expect("name document");
    let original = names["chunk_grid"]["configuration"]["chunk_shape"][0]
        .as_u64()
        .expect("name chunk");
    let width = names["data_type"]["configuration"]["length_bytes"]
        .as_u64()
        .expect("name width");
    let before = opened.pointing_bytes().expect("pointing bound");
    let enlarged = original * 4;
    names["chunk_grid"]["configuration"]["chunk_shape"][0] = enlarged.into();
    std::fs::write(
        &name_metadata,
        serde_json::to_vec(&names).expect("name document"),
    )
    .expect("larger name chunk");
    let after = opened.pointing_bytes().expect("larger pointing bound");
    assert!(
        after >= before + 2 * (enlarged - original) as usize * width as usize,
        "pointing bound {before} -> {after} omits the enlarged name chunk"
    );

    for variable in ["POINTING_BEAM", "time_pointing"] {
        let metadata = pointing.join(variable).join("zarr.json");
        let mut document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&metadata).expect("array metadata"))
                .expect("array document");
        document["shape"][0] = 1_000_000_u64.into();
        std::fs::write(
            &metadata,
            serde_json::to_vec(&document).expect("array document"),
        )
        .expect("long time coordinate");
    }
    let request = PointingRequest {
        partition: PartitionId(0),
        direction: PointingDirection::Beam,
        antennas: (0..opened.catalogue().antennas.len())
            .map(|antenna| AntennaId(antenna as u32))
            .collect(),
        times: 0.0..=1.0,
        budget_bytes: 1 << 20,
    };
    let (answer, peak, _) = measured(|| opened.pointing(&request));
    assert!(matches!(answer, Err(ObservationError::OverBudget { .. })));
    assert!(
        peak <= request.budget_bytes as u64,
        "refusal read {peak} bytes before the time coordinate admission"
    );
}

/// Reading a full dense pointing span must fit the request's decoded-array
/// and final-series budget even while it builds the last antenna's series.
fn a_dense_pointing_read_holds_at_most_its_budget(path: &Path) {
    let opened = Msv4Observation::open(path).expect("open dense pointing store");
    let (times, antennas) = (1024, 6);
    // The fixed allowance covers Zarr's array handles and coordinate labels.
    // The series and decoded values must stay within the size-dependent part.
    let budget = (times + times * antennas * 2) * size_of::<f64>()
        + antennas * (times + 2) * 4 * size_of::<f64>()
        + (16 << 10);
    let request = PointingRequest {
        partition: PartitionId(0),
        direction: PointingDirection::Beam,
        antennas: (0..antennas)
            .map(|antenna| AntennaId(antenna as u32))
            .collect(),
        times: -1.0e12..=1.0e12,
        budget_bytes: usize::MAX,
    };
    drop(opened.pointing(&request).expect("warm pointing read"));
    let (series, peak, _) = measured(|| opened.pointing(&request).expect("pointing read"));
    assert_eq!(series.antennas.len(), antennas);
    assert!(
        peak <= budget as u64,
        "the dense pointing read held {peak} bytes, beyond the {budget} decoded and series bytes"
    );
}

/// The production charge includes handles and labels, and rejects one byte
/// less than the full-span query requires. Exercise small and long labels.
fn a_pointing_query_holds_at_most_its_charge(path: &Path) {
    let opened = Msv4Observation::open(path).expect("open pointing store");
    let mut request = PointingRequest {
        partition: PartitionId(0),
        direction: PointingDirection::Beam,
        antennas: (0..opened.catalogue().antennas.len())
            .map(|antenna| AntennaId(antenna as u32))
            .collect(),
        times: -1.0e12..=1.0e12,
        budget_bytes: 0,
    };
    loop {
        match opened.pointing(&request) {
            Ok(_) => break,
            Err(ObservationError::OverBudget { required, .. }) => {
                assert!(required > request.budget_bytes);
                request.budget_bytes = required;
            }
            Err(error) => panic!("pointing charge: {error}"),
        }
    }
    let required = request.budget_bytes;
    let (series, peak, _) = measured(|| opened.pointing(&request).expect("pointing read"));
    assert_eq!(series.antennas.len(), request.antennas.len());
    assert!(
        peak <= required as u64,
        "pointing held {peak}, charged {required}"
    );
    assert!(opened.pointing_bytes().expect("universal bound") >= required);
    request.budget_bytes -= 1;
    assert!(
        matches!(
            opened.pointing(&request),
            Err(ObservationError::OverBudget { required: refused, .. }) if refused == required
        ),
        "pointing must refuse one byte less than its charge"
    );
    request.budget_bytes = usize::MAX;
    request.antennas.push(request.antennas[0]);
    assert!(matches!(
        opened.pointing(&request),
        Err(ObservationError::InvalidRequest { .. })
    ));
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
