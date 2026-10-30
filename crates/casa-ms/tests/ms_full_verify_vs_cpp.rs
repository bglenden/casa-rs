// SPDX-License-Identifier: LGPL-3.0-or-later
//! Whole-MeasurementSet Rust↔C++ verification.
//!
//! This test compares a stable digest manifest produced from Rust
//! (`casa_test_support::ms_digest`) with one produced by C++ casacore. The
//! manifest covers the main table, standard MS subtables, table/column
//! keywords, and all row values. Any additional linked tables reachable
//! through `TpTable` keyword references are also included.

#![cfg(feature = "cpp-interop-tests")]

mod common;

use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::process::Command;
use std::time::Instant;

use casa_ms::OptionalMainColumn;
use casa_ms::SubTable;
use casa_ms::builder::MeasurementSetBuilder;
use casa_ms::ms::MeasurementSet;
use casa_ms::observation_io::{Msv2Observation, OpenOptions, open_update};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, FieldSelection, Grouping, PatchValues,
    ReadEvent, Roles, TimeSelection, UpdateTarget, WeightRequest, WritableRole,
};
use casa_test_support::casacore_oracle_available;
use casa_test_support::ms_digest;
use casa_test_support::ms_interop::MeasurementSetOracle;
use casa_types::{ArrayValue, ScalarValue, Value};

use common::{populate_main_rows, populate_subtables};

/// Print the first rows of `label` whose Rust and C++ digests differ, field
/// by field.
fn print_table_row_diagnostics(ms_path: &Path, label: &str) {
    let table = ms_digest::labelled_table(ms_path, label)
        .unwrap_or_else(|| panic!("unknown MeasurementSet table label: {label}"));
    let limit = table.row_count().min(8);
    for row_index in 0..limit {
        let rust_digest = ms_digest::table_row_digest(&table, row_index, ms_path);
        let cpp_digest = MeasurementSetOracle::table_row_digest(ms_path, label, row_index as u64)
            .unwrap_or_else(|err| format!("ERROR:{err}"));
        if rust_digest == cpp_digest {
            continue;
        }
        eprintln!("{label} row {row_index} digest mismatch: rust={rust_digest} cpp={cpp_digest}");
        let rust_fields = ms_digest::table_row_field_manifest(&table, row_index, ms_path);
        let cpp_fields =
            MeasurementSetOracle::table_row_field_manifest(ms_path, label, row_index as u64)
                .unwrap_or_else(|err| format!("ERROR:{err}\n"));
        let rust_lines: Vec<_> = rust_fields.lines().collect();
        let cpp_lines: Vec<_> = cpp_fields.lines().collect();
        let max_len = rust_lines.len().max(cpp_lines.len());
        let mut shown = 0;
        for i in 0..max_len {
            let rust = rust_lines.get(i).copied().unwrap_or("<missing>");
            let cpp = cpp_lines.get(i).copied().unwrap_or("<missing>");
            if rust != cpp {
                eprintln!("{label} row {row_index} field rust: {rust}");
                eprintln!("{label} row {row_index} field cpp : {cpp}");
                shown += 1;
                if shown >= 20 {
                    break;
                }
            }
        }
        break;
    }
}

fn rust_model_data_sample(
    path: &Path,
    row: usize,
    correlation: usize,
    channel: usize,
) -> casa_types::Complex32 {
    let measurement_set = MeasurementSet::open(path).expect("open MODEL_DATA probe in Rust");
    let value = measurement_set
        .main_table()
        .column_accessor("MODEL_DATA")
        .expect("MODEL_DATA accessor")
        .get(row)
        .expect("read MODEL_DATA cell")
        .expect("defined MODEL_DATA cell");
    let Value::Array(ArrayValue::Complex32(values)) = value else {
        panic!("MODEL_DATA is not Complex32")
    };
    values[[correlation, channel]]
}

/// MAIN's `(row, correlation, channel, value)` cells written into
/// `MODEL_DATA` at `path` by casa-rs's imaging writer: an update session
/// with a model target, which creates a `MODEL_DATA` MAIN lacks, seeded
/// with CASA's initial model. Each cell is found in its partition's batches
/// by its row's time, baseline and data description, and written alone:
/// every other cell keeps what the session seeds or the column holds.
fn write_model_cells(path: &Path, cells: &[(usize, usize, usize, casa_types::Complex32)]) {
    let keys = {
        let ms = MeasurementSet::open(path).expect("open the MS");
        let main = ms.main_table();
        let cell = |row: usize, column: &str| {
            main.column_accessor(column)
                .expect("MAIN column")
                .get(row)
                .expect("read")
                .expect("defined")
                .clone()
        };
        cells
            .iter()
            .map(|&(row, ..)| {
                let Value::Scalar(ScalarValue::Float64(time_s)) = cell(row, "TIME") else {
                    panic!("TIME is Float64")
                };
                let int = |column| match cell(row, column) {
                    Value::Scalar(ScalarValue::Int32(value)) => value,
                    other => panic!("{column} is {other:?}"),
                };
                (
                    time_s,
                    int("ANTENNA1"),
                    int("ANTENNA2"),
                    int("DATA_DESC_ID"),
                )
            })
            .collect::<Vec<_>>()
    };
    let partition_of = {
        let opened = Msv2Observation::open(path).expect("open");
        opened
            .catalogue()
            .partition_ids()
            .into_iter()
            .map(|id| (opened.data_description_id(id), id))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let model = UpdateTarget {
        create: true,
        ..UpdateTarget::new("model", WritableRole::Visibilities)
    };
    let session = open_update(path, &OpenOptions::default(), &[model]).expect("session");
    let observation = session.observation();
    let mut written = 0;
    for (&data_description, &id) in &partition_of {
        let channels = observation.catalogue().partition(id).frequencies_hz.len();
        let request = AccessRequest {
            partitions: vec![id],
            data_group: "base".to_string(),
            roles: Roles::ALL,
            channels: 0..channels,
            times: TimeSelection::All,
            fields: FieldSelection::All,
            weights: WeightRequest::Stored,
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: usize::MAX,
        };
        let mut updater = session.updater(&request, &[0]).expect("updater");
        let mut traversal = observation.bind_read(&request).expect("traversal");
        let mut buffers = BatchBuffers::default();
        while let Some(event) = traversal.next(&mut buffers).expect("read") {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            let shape = batch.shape();
            let mut values = ndarray::Array3::zeros(shape);
            let mut mask = ndarray::Array3::from_elem(shape, false);
            let origin_s = batch.time.origin().as_mjd() * 86_400.0;
            for sample in 0..batch.samples() {
                let coordinate = batch.sample(sample);
                let time_s = origin_s + coordinate.time_offset_s;
                for (
                    &(_, correlation, channel, value),
                    &(key_time_s, antenna1, antenna2, key_ddid),
                ) in cells.iter().zip(&keys)
                {
                    if i64::from(key_ddid) == i64::from(data_description)
                        && (key_time_s - time_s).abs() < 1e-3
                        && i64::from(antenna1) == i64::from(coordinate.baseline.antenna1.0)
                        && i64::from(antenna2) == i64::from(coordinate.baseline.antenna2.0)
                    {
                        values[[sample, channel, correlation]] = value;
                        mask[[sample, channel, correlation]] = true;
                        written += 1;
                    }
                }
            }
            updater
                .write(
                    &batch,
                    &[PatchValues::Visibilities {
                        values: values.view(),
                        written: Some(mask.view()),
                    }],
                )
                .expect("write the cells");
        }
        updater.finish().expect("finish");
    }
    assert_eq!(written, cells.len(), "every cell is a sample's");
}

#[test]
fn ms_full_manifest_matches_cpp_for_basic_fixture() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping ms_full_manifest_matches_cpp_for_basic_fixture: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("full_manifest_basic.ms");

    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let mut ms = MeasurementSet::create(&ms_path, builder).unwrap();
    populate_subtables(&mut ms);
    populate_main_rows(&mut ms, 6);
    ms.save().unwrap();

    MeasurementSet::open(&ms_path).unwrap();
    let rust_manifest = ms_digest::measurement_set_manifest(&ms_path);
    let cpp_manifest = MeasurementSetOracle::digest_manifest(&ms_path).unwrap();
    assert_eq!(rust_manifest, cpp_manifest);
}

#[test]
fn model_data_write_read_interoperability_matrix() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping model_data_write_read_interoperability_matrix: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("model_data_commit.ms");
    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let mut ms = MeasurementSet::create(&ms_path, builder).unwrap();
    populate_subtables(&mut ms);
    populate_main_rows(&mut ms, 6);
    ms.save().unwrap();

    let rust_written = casa_types::Complex32::new(4.5, -1.25);
    write_model_cells(&ms_path, &[(0, 0, 0, rust_written)]);

    MeasurementSet::open(&ms_path).expect("reopen committed MS in Rust");
    let rust_manifest = ms_digest::measurement_set_manifest(&ms_path);
    let cpp_manifest = MeasurementSetOracle::digest_manifest(&ms_path).unwrap();
    assert_eq!(rust_manifest, cpp_manifest);
    assert_eq!(rust_model_data_sample(&ms_path, 0, 0, 0), rust_written); // Rust write, Rust read.
    assert_eq!(
        MeasurementSetOracle::read_model_data_sample(&ms_path, 0, 0, 0)
            .expect("Rust write, C++ read"),
        rust_written
    );
    // Every cell the run did not predict holds CASA's initial model, as
    // tclean creates the column: 1 in RR and LL, 0 in RL and LR.
    for (correlation, initial) in [(0, 1.0), (1, 0.0), (2, 0.0), (3, 1.0)] {
        let initial = casa_types::Complex32::new(initial, 0.0);
        for (row, channel) in [(0, 15), (5, 0)] {
            assert_eq!(
                rust_model_data_sample(&ms_path, row, correlation, channel),
                initial,
                "Rust read of row {row} product {correlation} channel {channel}"
            );
            assert_eq!(
                MeasurementSetOracle::read_model_data_sample(
                    &ms_path,
                    row as u64,
                    correlation as u32,
                    channel as u32
                )
                .expect("C++ read of the initial model"),
                initial,
                "C++ read of row {row} product {correlation} channel {channel}"
            );
        }
    }

    let cpp_path = dir.path().join("cpp_model_data_write.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::ModelData);
    let mut cpp_written_ms = MeasurementSet::create(&cpp_path, builder).unwrap();
    populate_subtables(&mut cpp_written_ms);
    populate_main_rows(&mut cpp_written_ms, 6);
    cpp_written_ms.save().unwrap();
    drop(cpp_written_ms);
    let cpp_written = casa_types::Complex32::new(-7.25, 3.5);
    MeasurementSetOracle::write_model_data_sample(&cpp_path, 1, 0, 0, cpp_written)
        .expect("C++ MODEL_DATA write");
    assert_eq!(rust_model_data_sample(&cpp_path, 1, 0, 0), cpp_written); // C++ write, Rust read.
    assert_eq!(
        MeasurementSetOracle::read_model_data_sample(&cpp_path, 1, 0, 0)
            .expect("C++ write, C++ read"),
        cpp_written
    );
}

#[test]
fn model_data_clone_preserves_cpp_heterogeneous_tiled_shape_storage() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping model_data_clone_preserves_cpp_heterogeneous_tiled_shape_storage: C++ casacore not available"
        );
        return;
    }

    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("cpp-heterogeneous-tiled-shape.ms");
    MeasurementSetOracle::write_heterogeneous_tiled_shape_fixture(&path)
        .expect("C++ creates heterogeneous TiledShapeStMan fixture");
    let rust_first = casa_types::Complex32::new(4.25, -0.75);
    let rust_second = casa_types::Complex32::new(-3.0, 2.5);
    write_model_cells(&path, &[(0, 3, 15, rust_first), (1, 2, 7, rust_second)]);

    MeasurementSetOracle::verify_heterogeneous_model_clone(&path)
        .expect("C++ verifies cloned TiledShapeStMan and heterogeneous row shapes");
    assert_eq!(rust_model_data_sample(&path, 0, 3, 15), rust_first);
    assert_eq!(
        MeasurementSetOracle::read_model_data_sample(&path, 0, 3, 15)
            .expect("Rust write, C++ read"),
        rust_first
    );

    let cpp_written = casa_types::Complex32::new(9.5, 1.25);
    MeasurementSetOracle::write_model_data_sample(&path, 1, 1, 6, cpp_written)
        .expect("C++ writes heterogeneous MODEL_DATA cell");
    assert_eq!(rust_model_data_sample(&path, 1, 1, 6), cpp_written);
    assert_eq!(
        MeasurementSetOracle::read_model_data_sample(&path, 1, 1, 6).expect("C++ write, C++ read"),
        cpp_written
    );
}

/// Run a whole-MS Rust↔C++ parity check against an external MeasurementSet.
///
/// Set `CASA_RS_VERIFY_MS_PATH` to the MS root directory. This is intended
/// for downloaded benchmark datasets kept outside the repo, for example:
///
/// `CASA_RS_VERIFY_MS_PATH=/path/to/ref_vlass_wtsp_creation.ms cargo test -p casa-ms --test ms_full_verify_vs_cpp verify_external_ms_matches_cpp -- --nocapture`
#[test]
fn verify_external_ms_matches_cpp() {
    if !casacore_oracle_available() {
        eprintln!("skipping verify_external_ms_matches_cpp: C++ casacore not available");
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!("skipping verify_external_ms_matches_cpp: CASA_RS_VERIFY_MS_PATH not set");
        return;
    };
    let ms_path = PathBuf::from(path);
    if !ms_path.is_dir() {
        eprintln!(
            "skipping verify_external_ms_matches_cpp: path is not a directory: {}",
            ms_path.display()
        );
        return;
    }

    let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
    let rust_manifest = ms_digest::measurement_set_manifest(&ms_path);
    let cpp_manifest =
        MeasurementSetOracle::digest_manifest(&ms_path).expect("open MeasurementSet in C++");
    if rust_manifest != cpp_manifest {
        eprintln!("row counts:");
        eprintln!("MAIN {}", ms.row_count());
        eprintln!(
            "DATA_DESCRIPTION {}",
            ms.data_description().unwrap().table().row_count()
        );
        eprintln!(
            "POLARIZATION {}",
            ms.polarization().unwrap().table().row_count()
        );
        eprintln!("PROCESSOR {}", ms.processor().unwrap().table().row_count());
        eprintln!("STATE {}", ms.state().unwrap().table().row_count());
        eprintln!("SOURCE {}", ms.source().unwrap().table().row_count());
        eprintln!("SYSCAL {}", ms.syscal().unwrap().table().row_count());
        eprintln!("WEATHER {}", ms.weather().unwrap().table().row_count());
        eprintln!("HISTORY {}", ms.history().unwrap().table().row_count());
        let rust_lines: Vec<_> = rust_manifest.lines().collect();
        let cpp_lines: Vec<_> = cpp_manifest.lines().collect();
        let max_len = rust_lines.len().max(cpp_lines.len());
        let mut shown = 0;
        eprintln!("first manifest diffs:");
        for i in 0..max_len {
            let rust = rust_lines.get(i).copied().unwrap_or("<missing>");
            let cpp = cpp_lines.get(i).copied().unwrap_or("<missing>");
            if rust != cpp {
                eprintln!("line {} rust: {}", i + 1, rust);
                eprintln!("line {} cpp : {}", i + 1, cpp);
                shown += 1;
                if shown >= 20 {
                    break;
                }
            }
        }
        for label in ["MAIN", "SOURCE", "SYSCAL"] {
            print_table_row_diagnostics(&ms_path, label);
        }
    }
    assert_eq!(rust_manifest, cpp_manifest);
}

/// casacore and casa-rs read `alma/multiobs.ms` alike, and read alike what
/// casa-rs saves of it. Its SOURCE holds 0-dimensional cells, casacore's
/// empty `IPosition`: string cells in StandardStMan buckets (`TRANSITION`)
/// and numeric cells in its array file (`POSITION`, `REST_FREQUENCY`,
/// `SYSVEL`), which casa-rs once read as one element (#923).
///
/// casa-rs, which has no 0-dimensional empty array, reads such a cell as
/// `[0]` and saves it so: casacore then reads `[0]` where it read `[]`,
/// either holding no elements. The C++ digest counts the two alike, so the
/// manifests compare what each table holds.
#[test]
fn multiobs_reads_and_resaves_as_casacore_reads_it() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping multiobs_reads_and_resaves_as_casacore_reads_it: C++ casacore not available"
        );
        return;
    }
    let Some(source) = casa_test_support::casatestdata_path("measurementset/alma/multiobs.ms")
        .filter(|path| path.is_dir())
    else {
        eprintln!(
            "skipping multiobs_reads_and_resaves_as_casacore_reads_it: alma/multiobs.ms not available"
        );
        return;
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let ms_path = directory.path().join("multiobs.ms");
    copy_tree(&source, &ms_path);

    let original = MeasurementSetOracle::digest_manifest(&ms_path).expect("C++ manifest");
    assert_eq!(ms_digest::measurement_set_manifest(&ms_path), original);

    let mut ms = MeasurementSet::open(&ms_path).expect("open multiobs.ms");
    ms.save().expect("resave multiobs.ms");
    drop(ms);
    let resaved =
        MeasurementSetOracle::digest_manifest(&ms_path).expect("C++ manifest of the resave");
    assert_eq!(ms_digest::measurement_set_manifest(&ms_path), resaved);
    assert_eq!(resaved, original);
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create directory");
    for entry in std::fs::read_dir(from).expect("read directory") {
        let entry = entry.expect("directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy file");
        }
    }
}

#[test]
#[ignore = "debug helper for inspecting one external MeasurementSet row"]
fn debug_external_ms_row_vs_cpp() {
    if !casacore_oracle_available() {
        eprintln!("skipping debug_external_ms_row_vs_cpp: C++ casacore not available");
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!("skipping debug_external_ms_row_vs_cpp: CASA_RS_VERIFY_MS_PATH not set");
        return;
    };
    let label = std::env::var("CASA_RS_VERIFY_MS_LABEL").unwrap_or_else(|_| "MAIN".to_string());
    let row_index = std::env::var("CASA_RS_VERIFY_MS_ROW")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    let ms_path = PathBuf::from(path);
    let table = ms_digest::labelled_table(&ms_path, &label)
        .unwrap_or_else(|| panic!("unknown MeasurementSet table label: {label}"));
    eprintln!(
        "rust digest: {}",
        ms_digest::table_row_digest(&table, row_index, &ms_path)
    );
    eprintln!(
        "cpp  digest: {}",
        MeasurementSetOracle::table_row_digest(&ms_path, &label, row_index as u64)
            .expect("cpp row digest"),
    );
    eprintln!(
        "rust fields:\n{}",
        ms_digest::table_row_field_manifest(&table, row_index, &ms_path)
    );
    eprintln!(
        "cpp fields:\n{}",
        MeasurementSetOracle::table_row_field_manifest(&ms_path, &label, row_index as u64)
            .expect("cpp row field manifest"),
    );
}

#[cfg(target_os = "macos")]
fn purge_disk_cache() -> Result<(), String> {
    let purge = Path::new("/usr/sbin/purge");
    if !purge.is_file() {
        return Err(format!("expected macOS purge tool at {}", purge.display()));
    }
    let status = Command::new(purge)
        .status()
        .map_err(|err| format!("run macOS purge: {err}"))?;
    if !status.success() {
        return Err(format!("macOS purge exited with status {status}"));
    }
    Ok(())
}

/// Benchmark reading all rows of the MAIN table from an external MeasurementSet.
///
/// This reuses the same definedness-aware MAIN-row digest that the verifier uses,
/// but only times the MAIN table scan rather than traversing the full MS.
///
/// Set `CASA_RS_VERIFY_MS_PATH` to the external MS root and run, for example:
///
/// `CASA_RS_VERIFY_MS_PATH=/path/to/uid___A002_Xb9dfa4_X4724_target_spw16.ms cargo test --release -p casa-ms --test ms_full_verify_vs_cpp bench_external_main_rows_vs_cpp -- --ignored --nocapture`
#[test]
#[ignore = "performance helper for external MeasurementSet MAIN-table scans"]
fn bench_external_main_rows_vs_cpp() {
    if !casacore_oracle_available() {
        eprintln!("skipping bench_external_main_rows_vs_cpp: C++ casacore not available");
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!("skipping bench_external_main_rows_vs_cpp: CASA_RS_VERIFY_MS_PATH not set");
        return;
    };
    let ms_path = PathBuf::from(path);
    if !ms_path.is_dir() {
        eprintln!(
            "skipping bench_external_main_rows_vs_cpp: path is not a directory: {}",
            ms_path.display()
        );
        return;
    }

    let cpp = MeasurementSetOracle::bench_main_rows(&ms_path).expect("benchmark MAIN rows in C++");

    let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
    let t0 = Instant::now();
    let rust_digest = ms_digest::table_rows_digest(ms.main_table(), &ms_path);
    let rust_read_ns = t0.elapsed().as_nanos() as u64;

    assert_eq!(rust_digest, cpp.rows_digest);

    let read_ratio = rust_read_ns as f64 / cpp.read_ns.max(1) as f64;
    eprintln!(
        "MAIN-table full scan ({} rows):\n  C++ {:.1} ms\n  Rust {:.1} ms\n  ratio {:.2}x",
        ms.row_count(),
        cpp.read_ns as f64 / 1e6,
        rust_read_ns as f64 / 1e6,
        read_ratio,
    );
    if read_ratio > 5.0 {
        eprintln!("  warning: Rust MAIN-table scan is {read_ratio:.2}x slower than C++");
    }
}

/// Benchmark opening an external MeasurementSet and then streaming the full MAIN
/// table, without attempting to purge the OS file cache.
///
/// This is still a fresh-process `open + read` measurement, so it captures Rust
/// and C++ open-path costs even when a cold-cache approximation is unavailable.
#[test]
#[ignore = "performance helper for external MeasurementSet open + MAIN scans"]
fn bench_external_open_and_main_rows_vs_cpp() {
    if !casacore_oracle_available() {
        eprintln!("skipping bench_external_open_and_main_rows_vs_cpp: C++ casacore not available");
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp: CASA_RS_VERIFY_MS_PATH not set"
        );
        return;
    };
    let ms_path = PathBuf::from(path);
    if !ms_path.is_dir() {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp: path is not a directory: {}",
            ms_path.display()
        );
        return;
    }

    let cpp = MeasurementSetOracle::bench_open_main_rows(&ms_path)
        .expect("benchmark MAIN open+scan in C++");

    let t0 = Instant::now();
    let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
    let rust_digest = ms_digest::table_rows_digest(ms.main_table(), &ms_path);
    let rust_open_and_read_ns = t0.elapsed().as_nanos() as u64;

    assert_eq!(rust_digest, cpp.rows_digest);

    let total_ratio = rust_open_and_read_ns as f64 / cpp.open_and_read_ns.max(1) as f64;
    eprintln!(
        "MAIN-table open + full scan ({} rows):\n  C++ {:.1} ms\n  Rust {:.1} ms\n  ratio {:.2}x",
        ms.row_count(),
        cpp.open_and_read_ns as f64 / 1e6,
        rust_open_and_read_ns as f64 / 1e6,
        total_ratio,
    );
    if total_ratio > 5.0 {
        eprintln!("  warning: Rust MAIN open+scan is {total_ratio:.2}x slower than C++");
    }
}

/// Benchmark opening an external MeasurementSet and then streaming the full MAIN
/// table under a cold-ish macOS buffer-cache approximation.
///
/// This test is macOS-only because it uses `/usr/sbin/purge` between the C++ and
/// Rust passes. It is still not a true cold-boot measurement, but it is much
/// closer to real `open + read` timing than the warm post-open scan benchmark.
///
/// Set `CASA_RS_VERIFY_MS_PATH` to the external MS root and run, for example:
///
/// `CASA_RS_VERIFY_MS_PATH=/path/to/uid___A002_Xb9dfa4_X4724_target_spw16.ms cargo test --release -p casa-ms --test ms_full_verify_vs_cpp bench_external_open_and_main_rows_vs_cpp_coldish -- --ignored --nocapture`
#[test]
#[ignore = "performance helper for macOS cold-ish external MeasurementSet open + MAIN scans"]
#[cfg(target_os = "macos")]
fn bench_external_open_and_main_rows_vs_cpp_coldish() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp_coldish: C++ casacore not available"
        );
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp_coldish: CASA_RS_VERIFY_MS_PATH not set"
        );
        return;
    };
    let ms_path = PathBuf::from(path);
    if !ms_path.is_dir() {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp_coldish: path is not a directory: {}",
            ms_path.display()
        );
        return;
    }

    let Err(err) = purge_disk_cache() else {
        let cpp = MeasurementSetOracle::bench_open_main_rows(&ms_path)
            .expect("benchmark MAIN open+scan in C++");

        let Err(err) = purge_disk_cache() else {
            let t0 = Instant::now();
            let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
            let rust_digest = ms_digest::table_rows_digest(ms.main_table(), &ms_path);
            let rust_open_and_read_ns = t0.elapsed().as_nanos() as u64;

            assert_eq!(rust_digest, cpp.rows_digest);

            let total_ratio = rust_open_and_read_ns as f64 / cpp.open_and_read_ns.max(1) as f64;
            eprintln!(
                "MAIN-table cold-ish open + full scan ({} rows):\n  C++ {:.1} ms\n  Rust {:.1} ms\n  ratio {:.2}x",
                ms.row_count(),
                cpp.open_and_read_ns as f64 / 1e6,
                rust_open_and_read_ns as f64 / 1e6,
                total_ratio,
            );
            if total_ratio > 5.0 {
                eprintln!("  warning: Rust MAIN open+scan is {total_ratio:.2}x slower than C++");
            }
            return;
        };
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp_coldish: second purge failed: {err}"
        );
        return;
    };
    eprintln!(
        "skipping bench_external_open_and_main_rows_vs_cpp_coldish: purge unavailable: {err}"
    );
}
