// SPDX-License-Identifier: LGPL-3.0-or-later
//! The counting law of the observation-batch route's source (#932, step 4):
//! reading the source through a run's traversals holds no more than the
//! route's admission charges it, the one block it fills, and the tiles the
//! table layer stages, which no imaging source charges yet (#965). The route's
//! charge without the passes' blocks includes the opening's index, which
//! the reading does not allocate: a few bytes a sample. A whole run's law
//! (`admission_memory.rs`) cannot see this charge beside the rest of the
//! run's. This law sees the batches, the source's largest term; each
//! smaller term is held to its bound by its owner's law: the geometry and
//! the parallactic cache by `casa-imaging-model`'s `batch_geometry_memory`,
//! a traversal's batches (the survey's among them) by the adapters' laws,
//! and the pointing and opening bounds by `casa-ms`'s
//! `msv2_opening_memory`. This binary's global allocator counts the live
//! heap; it holds this one law, so the law runs in a process of its own.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};

use casa_imaging_application::source_law::{SourceDrive, drive_batch_source};
use casa_imaging_application::{HostResources, ImagingRequest, ResourcePolicy, RunContext};
use serde_json::{Value, json};

#[path = "common/imaging.rs"]
mod imaging;

#[path = "common/synthetic_observation.rs"]
mod synthetic_observation;
use synthetic_observation::{CELL_ARCSEC, observation, pointed_observation_as};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their peak.
struct CountingHeap;

static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

// SAFETY: every method forwards to `System` with the caller's arguments and
// only adds atomic bookkeeping, which allocates nothing.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size() as i64);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size() as i64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`.
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            grow(new_size as i64 - layout.size() as i64);
        }
        moved
    }
}

fn grow(bytes: i64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

/// A host whose memory no policy below exceeds.
const HOST: HostResources = HostResources {
    threads: 4,
    performance_cores: 4,
    available_memory: 1 << 50,
    metal: false,
};

/// A request for `measurement_set` imaged `imsize` pixels square, with
/// `controls`.
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
        "niter": 0,
    });
    values
        .as_object_mut()
        .expect("controls")
        .extend(controls.as_object().expect("overrides").clone());
    imaging::request(values)
}

/// Tiles a tiled column's read session holds while the column is read:
/// casa-tables' `TILE_READ_SESSION_RECENT_TILE_COUNT`.
const RECENT_TILES: usize = 8;

/// What the table layer stages while `measurement_set`'s columns are read,
/// outside every imaging source's charge on either route (#965): the most
/// that one column's read session holds, its recent tiles but no more tiles
/// than the column has.
fn tile_staging(measurement_set: &Path) -> i64 {
    let ms = casa_ms::MeasurementSet::open(measurement_set).expect("the fixture");
    let main = ms.main_table();
    let rows = ms.row_count();
    ["DATA", "FLAG", "WEIGHT", "SIGMA", "UVW"]
        .into_iter()
        .filter_map(|column| {
            let tile = main.array_column_tile_shape(column).expect("tile shape")?;
            let bytes = main.array_column_tile_bytes(column).expect("tile bytes")?;
            let cell = main.array_shape(0, column).expect("a cell")?;
            let (rows_per_tile, cell_tile) = tile.split_last().expect("a tiled cell");
            let tiles = cell
                .iter()
                .zip(cell_tile)
                .map(|(length, step)| length.div_ceil(*step))
                .product::<usize>()
                * rows.div_ceil(*rows_per_tile);
            Some((tiles.min(RECENT_TILES) * bytes) as i64)
        })
        .max()
        .unwrap_or(0)
}

/// Drive `request`'s batch source under a `memory`-byte policy in waves of
/// `planes_per_wave` planes: the peak live heap of its measured reading
/// ([`drive_batch_source`]), beside what it admitted.
fn measure(request: &ImagingRequest, planes_per_wave: u32, memory: u64) -> (SourceDrive, i64) {
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit { workers: 2, memory })
    };
    let mut baseline = 0;
    let drive = drive_batch_source(request, &context, planes_per_wave, &mut || {
        baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
    })
    .expect("the source reads");
    (drive, PEAK.load(Ordering::Relaxed) - baseline)
}

/// Continuum and cube sources, a cube resident and in waves, a window of
/// 101 channels whole and gapped (`0:0~1;100`, whose batches span every
/// channel between) and a mosaic that reads pointing: each holds at most
/// its charge and its block, and the drives fill blocks.
#[test]
fn the_batch_source_holds_at_most_its_charge() {
    let root = tempfile::tempdir().expect("test root");
    let directory = |name: &str| {
        let directory = root.path().join(name);
        std::fs::create_dir(&directory).expect("a fixture directory");
        directory
    };
    let narrow = observation(&directory("narrow"), 16);
    let wide = observation(&directory("wide"), 101);
    let alma = pointed_observation_as(&directory("alma"), 16, "ALMA");
    let cube = json!({
        "specmode": "cube",
        "outframe": "LSRK",
        "spw": "0",
        "channel_start": 0,
        "channel_count": 16,
        "start": "0",
        "width": "1",
    });
    let cases: [(&str, &Path, Value, u32); 6] = [
        ("continuum", &narrow, json!({}), 1),
        ("cube resident", &narrow, cube.clone(), 16),
        ("cube in waves", &narrow, cube, 3),
        ("continuum of 101 channels", &wide, json!({}), 1),
        (
            "gapped linear cube in waves",
            &wide,
            json!({
                "specmode": "cube",
                "outframe": "LSRK",
                "spw": "0:0~1;100",
                "channel_count": 10,
                "start": "44.64GHz",
                "width": "1.28GHz",
                "interpolation": "linear",
            }),
            3,
        ),
        (
            "mosaic with pointing",
            &alma,
            json!({ "gridder": "mosaic", "usepointing": true }),
            1,
        ),
    ];
    let mut breaches = Vec::new();
    for (label, measurement_set, controls, planes_per_wave) in cases {
        let image = root.path().join(label.replace(' ', "-"));
        let (drive, peak) = measure(
            &request(measurement_set, &image, 256, controls),
            planes_per_wave,
            4 << 30,
        );
        let held = peak - drive.block as i64 - tile_staging(measurement_set);
        eprintln!(
            "{label}: {held} bytes held of {} charged ({} blocks of {} bytes, tiles staged \
             apart)",
            drive.charged, drive.blocks, drive.block
        );
        assert!(drive.blocks > 0, "{label}: the drive filled no block");
        if held > drive.charged as i64 {
            breaches.push(format!(
                "{label}: the source held {held} bytes, {} more than its charge",
                held - drive.charged as i64
            ));
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
