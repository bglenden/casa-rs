// SPDX-License-Identifier: LGPL-3.0-or-later

//! Counting-law tests for the observation-batch source and visibility writer.

use std::path::Path;
use std::sync::atomic::Ordering;

use casa_imaging_runtime::pass::OVERLAPPED_BLOCKS;
use serde_json::{Value, json};

use super::{SourceDrive, drive_batch_source, drive_batch_write};
use crate::counting_heap::{
    BASELINE, HOST, LIVE, MEASURING, SIGNED_PEAK, alone, context, request as base_request,
    synthetic_observation::{
        CELL_ARCSEC, observation, pointed_observation_as, two_window_observation,
    },
};
use crate::{ImagingRequest, ResourcePolicy, RunContext};

fn request(
    measurement_sets: &[&Path],
    image_name: &Path,
    imsize: usize,
    controls: Value,
) -> ImagingRequest {
    let vis = match measurement_sets {
        [one] => json!(one),
        several => json!(several),
    };
    let mut values = json!({
        "vis": vis,
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
    base_request(values)
}

const RECENT_TILES: usize = 8;

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

fn drive_without_measuring(
    request: &ImagingRequest,
    planes_per_wave: u32,
    memory: u64,
) -> SourceDrive {
    let context = RunContext {
        host: HOST,
        ..context(ResourcePolicy::Explicit { workers: 2, memory })
    };
    let mut noop = || {};
    if request.savemodel == crate::SaveModel::ModelColumn || request.save_continuum_residual {
        drive_batch_write(request, &context, &mut noop).expect("the write")
    } else {
        drive_batch_source(request, &context, planes_per_wave, &mut noop).expect("the source reads")
    }
}

fn measure(request: &ImagingRequest, planes_per_wave: u32, memory: u64) -> (SourceDrive, i64) {
    let context = RunContext {
        host: HOST,
        ..context(ResourcePolicy::Explicit { workers: 2, memory })
    };
    let mut baseline = 0_u64;
    let mut mark = || {
        baseline = LIVE.load(Ordering::Relaxed);
        BASELINE.store(baseline, Ordering::Relaxed);
        SIGNED_PEAK.store(0, Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
    };
    let drive = if request.savemodel == crate::SaveModel::ModelColumn
        || request.save_continuum_residual
    {
        drive_batch_write(request, &context, &mut mark).expect("the write")
    } else {
        drive_batch_source(request, &context, planes_per_wave, &mut mark).expect("the source reads")
    };
    MEASURING.store(false, Ordering::Relaxed);
    (drive, SIGNED_PEAK.load(Ordering::Relaxed))
}

#[test]
fn the_batch_source_holds_at_most_its_charge() {
    if !alone("source_law::memory_tests::the_batch_source_holds_at_most_its_charge") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let directory = |name: &str| {
        let directory = root.path().join(name);
        std::fs::create_dir(&directory).expect("a fixture directory");
        directory
    };
    let narrow = observation(&directory("narrow"), 16);
    let wide = observation(&directory("wide"), 101);
    let alma = pointed_observation_as(&directory("alma"), 16, "ALMA");
    let alma_again = pointed_observation_as(&directory("alma-again"), 16, "ALMA");
    let narrow_again = observation(&directory("narrow-again"), 16);
    let cube = json!({
        "specmode": "cube",
        "outframe": "LSRK",
        "spw": "0",
        "channel_start": 0,
        "channel_count": 16,
        "start": "0",
        "width": "1",
    });
    let gapped = json!({
        "specmode": "cube",
        "outframe": "LSRK",
        "spw": "0:0~1;100",
        "channel_count": 10,
        "start": "44.64GHz",
        "width": "1.28GHz",
        "interpolation": "linear",
    });
    let mut gapped_write = gapped.clone();
    gapped_write.as_object_mut().expect("controls").extend([
        ("savemodel".to_string(), json!("modelcolumn")),
        ("niter".to_string(), json!(10)),
    ]);
    let save = json!({ "savemodel": "modelcolumn", "niter": 10 });
    use casa_core::measures::frequency::FrequencyRef;
    let topo = observation(&directory("topo"), 16);
    let mut ms = casa_ms::MeasurementSet::open(&topo).expect("the fixture");
    ms.spectral_window_mut()
        .expect("its windows")
        .set_i32(0, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
        .expect("its frame");
    ms.save().expect("the frame saved");
    drop(ms);
    let drift = json!({
        "specmode": "cube",
        "outframe": "TOPO",
        "spw": "0",
        "start": "0",
        "width": "1",
        "channel_count": 8,
        "interpolation": "linear",
        "savemodel": "modelcolumn",
        "niter": 10,
    });
    let ample = 4 << 30;
    let pointed_mosaic = json!({ "gridder": "mosaic", "usepointing": true });
    let part = json!({
        "specmode": "cube",
        "outframe": "LSRK",
        "spw": "0",
        "start": "0",
        "width": "1",
        "channel_count": 4,
        "weighting": "uniform",
        "perchanweightdensity": false,
    });
    let tracked_wide = json!({
        "specmode": "cube",
        "outframe": "TOPO",
        "spw": "0",
        "start": "0",
        "width": "2",
        "channel_count": 12,
        "interpolation": "linear",
    });
    let cases: [(&str, Vec<&Path>, Value, u32, u64); 15] = [
        ("continuum", vec![&narrow], json!({}), 1, ample),
        (
            "wide TOPO cube on LSRK data in waves",
            vec![&wide],
            tracked_wide,
            3,
            ample,
        ),
        ("cube resident", vec![&narrow], cube.clone(), 16, ample),
        ("cube in waves", vec![&narrow], cube, 3, ample),
        (
            "continuum of 101 channels",
            vec![&wide],
            json!({}),
            1,
            ample,
        ),
        ("gapped linear cube in waves", vec![&wide], gapped, 3, ample),
        (
            "mosaic with pointing",
            vec![&alma],
            pointed_mosaic.clone(),
            1,
            ample,
        ),
        (
            "continuum saving the model",
            vec![&narrow],
            save.clone(),
            1,
            ample,
        ),
        (
            "gapped cube saving the model",
            vec![&wide],
            gapped_write,
            10,
            ample,
        ),
        (
            "cube on part of a window with a whole-selection density",
            vec![&wide],
            part,
            4,
            ample,
        ),
        (
            "cube saving the model of the channels its band selects",
            vec![&topo],
            drift,
            8,
            ample,
        ),
        (
            "continuum saving the model in batches",
            vec![&narrow],
            save.clone(),
            1,
            128 << 20,
        ),
        (
            "continuum over two MeasurementSets",
            vec![&narrow, &wide],
            json!({}),
            1,
            ample,
        ),
        (
            "mosaic with pointing over two MeasurementSets",
            vec![&alma, &alma_again],
            pointed_mosaic,
            1,
            ample,
        ),
        (
            "continuum saving the model into two MeasurementSets",
            vec![&narrow, &narrow_again],
            save,
            1,
            ample,
        ),
    ];
    let mut breaches = Vec::new();
    for (label, measurement_sets, controls, planes_per_wave, memory) in cases {
        let image = root.path().join(label.replace(' ', "-"));
        let (drive, peak) = measure(
            &request(&measurement_sets, &image, 256, controls),
            planes_per_wave,
            memory,
        );
        let staging = measurement_sets
            .iter()
            .map(|measurement_set| tile_staging(measurement_set))
            .max()
            .unwrap_or(0);
        let held = peak - drive.block as i64 - staging;
        assert!(drive.blocks > 0, "{label}: the drive filled no block");
        assert!(
            memory == ample || drive.blocks > 1,
            "{label}: the drive filled one block"
        );
        if held > drive.charged as i64 {
            breaches.push(format!(
                "{label}: the source held {held} bytes, {} more than its charge",
                held - drive.charged as i64
            ));
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}

#[test]
fn a_window_the_axis_misses_is_read_by_the_density_and_a_write_alone() {
    const TIMES: usize = 10;
    let root = tempfile::tempdir().expect("test root");
    let two = two_window_observation(root.path(), 16);
    let cube = |name: &str, spw: &str, write: bool| {
        let mut controls = json!({
            "specmode": "cube",
            "outframe": "LSRK",
            "spw": spw,
            "start": "0",
            "width": "1",
            "channel_count": 8,
        });
        if write {
            controls["savemodel"] = json!("modelcolumn");
            controls["niter"] = json!(10);
        }
        let image = root.path().join(name);
        drive_without_measuring(&request(&[&two], &image, 256, controls), 8, 4 << 30)
    };
    let batches = TIMES.div_ceil(TIMES.div_ceil(OVERLAPPED_BLOCKS));
    let first = cube("first", "0", false);
    assert_eq!(first.blocks, 2 * batches, "{first:?}");
    let both = cube("both", "*", false);
    assert_eq!(both.blocks, 3 * batches, "{both:?}");
    let writes_first = cube("writes-first", "0", true);
    let writes_both = cube("writes-both", "*", true);
    assert_eq!(
        writes_both.blocks,
        2 * writes_first.blocks,
        "{writes_first:?} {writes_both:?}"
    );
}

#[test]
fn a_pass_streams_in_overlapped_batches() {
    const TIMES: usize = 10;
    let root = tempfile::tempdir().expect("test root");
    let narrow = observation(root.path(), 16);
    let drive = drive_without_measuring(
        &request(&[&narrow], &root.path().join("overlapped"), 256, json!({})),
        1,
        4 << 30,
    );
    let batches = TIMES.div_ceil(TIMES.div_ceil(OVERLAPPED_BLOCKS));
    assert_eq!(drive.blocks, 2 * batches, "{drive:?}");
}
