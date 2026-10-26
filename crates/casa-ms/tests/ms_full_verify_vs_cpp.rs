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

use casa_imaging_model::{
    CorrelationProduct, CorrelationSelection, DataDescriptionSelection, IdSelection,
    IntentSelection, MsColumnKind, ObservationSelection, RowSelection, SelectedMainRow,
    SelectedRows, SpectralWindowSelection, UvSelection, VisibilityColumn, WeightColumn,
};
use casa_ms::OptionalMainColumn;
use casa_ms::SubTable;
use casa_ms::builder::MeasurementSetBuilder;
use casa_ms::ms::MeasurementSet;
use casa_ms::{
    SelectedObservationContentBudget, SelectedObservationResolutionRequest,
    SelectedVisibilityWrite, SelectedVisibilityWriteTargets, resolve_selected_observation,
};
use casa_test_support::casacore_oracle_available;
use casa_test_support::ms_digest;
use casa_test_support::ms_interop::MeasurementSetOracle;
use casa_types::Correlation;
use casa_types::{ArrayValue, Value};

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

    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(
            6,
            (0_u32..6).map(|row| SelectedMainRow::new(u64::from(row), 0)),
        )
        .unwrap(),
        RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, (0..16).collect())],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, Correlation::RR),
                CorrelationProduct::new(1, Correlation::RL),
                CorrelationProduct::new(2, Correlation::LR),
                CorrelationProduct::new(3, Correlation::LL),
            ],
        )],
    );
    let request = SelectedObservationResolutionRequest::new(
        ms_path.display().to_string(),
        selection,
        VisibilityColumn::Data,
        WeightColumn::Weight,
        SelectedObservationContentBudget::new(1 << 20, 6, 64),
        casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
    );
    resolve_selected_observation(request).expect("resolve the MS without preparation");
    let mut writer =
        SelectedVisibilityWrite::begin(&ms_path, SelectedVisibilityWriteTargets::new(true, false))
            .expect("begin MODEL_DATA write");
    let rust_written = casa_types::Complex32::new(4.5, -1.25);
    writer
        .write(MsColumnKind::ModelData, 0, 0, 0, rust_written)
        .expect("write prediction");
    writer.complete().expect("complete MODEL_DATA write");

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
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(
            2,
            [SelectedMainRow::new(0, 0), SelectedMainRow::new(1, 1)],
        )
        .expect("two heterogeneous rows"),
        RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
        vec![
            DataDescriptionSelection::new(0, 0, 0),
            DataDescriptionSelection::new(1, 1, 0),
        ],
        vec![
            SpectralWindowSelection::new(0, (0..16).collect()),
            SpectralWindowSelection::new(1, (0..8).collect()),
        ],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, Correlation::RR),
                CorrelationProduct::new(1, Correlation::RL),
                CorrelationProduct::new(2, Correlation::LR),
                CorrelationProduct::new(3, Correlation::LL),
            ],
        )],
    );
    let request = SelectedObservationResolutionRequest::new(
        path.display().to_string(),
        selection.clone(),
        VisibilityColumn::Data,
        WeightColumn::Weight,
        SelectedObservationContentBudget::new(1 << 20, 2, 64),
        casa_test_support::deterministic_measures_provider_for_identity([91; 32]),
    );
    resolve_selected_observation(request).expect("resolve C++ fixture");

    let mut writer =
        SelectedVisibilityWrite::begin(&path, SelectedVisibilityWriteTargets::new(true, false))
            .expect("clone and initialize MODEL_DATA");
    let rust_first = casa_types::Complex32::new(4.25, -0.75);
    let rust_second = casa_types::Complex32::new(-3.0, 2.5);
    writer
        .write(MsColumnKind::ModelData, 0, 15, 3, rust_first)
        .expect("write 4x16 cell");
    writer
        .write(MsColumnKind::ModelData, 1, 7, 2, rust_second)
        .expect("write 4x8 cell");
    writer.complete().expect("complete Rust write");

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
