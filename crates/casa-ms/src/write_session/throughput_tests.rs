// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::time::Instant;

use casa_tables::{ColumnOverrides, GeneratedScalarColumn};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use num_complex::Complex32;
use serde_json::json;

use super::*;
use crate::builder::MeasurementSetBuilder;
use crate::schema::SubtableId;
use crate::schema::main_table::OptionalMainColumn;

const DEFAULT_TARGET_BYTES: u64 = 100_000_000_000;
const CORRELATION_COUNT: usize = 2;
const BENCHMARK_BATCH_BYTES: usize = 64 * 1024 * 1024;
const RAW_IO_BLOCK_BYTES: usize = 16 * 1024 * 1024;

#[test]
#[ignore = "explicit large-file disk throughput baseline"]
fn ignored_raw_disk_throughput_fixture() {
    let root = std::env::var_os("CASA_RS_MS_THROUGHPUT_ROOT")
        .map(PathBuf::from)
        .expect("CASA_RS_MS_THROUGHPUT_ROOT must name an empty benchmark directory");
    let target_bytes = std::env::var("CASA_RS_MS_THROUGHPUT_TARGET_BYTES")
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .expect("target bytes must be an integer")
        })
        .unwrap_or(DEFAULT_TARGET_BYTES);
    fs::create_dir_all(&root).expect("create raw benchmark root");
    let output = root.join("raw-throughput.bin");
    assert!(!output.exists(), "refusing to replace {}", output.display());

    let pattern = (0..RAW_IO_BLOCK_BYTES)
        .map(|index| {
            let mixed = (index as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .rotate_left(17);
            (mixed ^ (mixed >> 29)) as u8
        })
        .collect::<Vec<_>>();
    let write_started = Instant::now();
    let mut file = File::create(&output).expect("create raw throughput file");
    let mut remaining = target_bytes;
    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(pattern.len() as u64)).expect("chunk fits usize");
        file.write_all(&pattern[..chunk]).expect("write raw block");
        remaining -= chunk as u64;
    }
    file.sync_all().expect("sync raw throughput file");
    let write_seconds = write_started.elapsed().as_secs_f64();

    let read_started = Instant::now();
    let mut file = File::open(&output).expect("open raw throughput file");
    let mut buffer = vec![0u8; RAW_IO_BLOCK_BYTES];
    let mut bytes_read = 0u64;
    let mut checksum = 0u64;
    loop {
        let read = file.read(&mut buffer).expect("read raw block");
        if read == 0 {
            break;
        }
        bytes_read += read as u64;
        checksum = checksum.wrapping_add(
            buffer[..read]
                .chunks(4096)
                .map(|chunk| u64::from(chunk[0]))
                .sum::<u64>(),
        );
    }
    let read_seconds = read_started.elapsed().as_secs_f64();
    assert_eq!(bytes_read, target_bytes);
    assert_ne!(checksum, 0);
    let report = json!({
        "schema_version": 1,
        "kind": "raw_disk_throughput_fixture",
        "output_file": output,
        "target_bytes": target_bytes,
        "block_bytes": RAW_IO_BLOCK_BYTES,
        "write_seconds": write_seconds,
        "read_seconds": read_seconds,
        "write_mb_per_second": rate(target_bytes, write_seconds),
        "read_mb_per_second": rate(target_bytes, read_seconds),
        "checksum": checksum,
        "peak_rss_bytes": peak_rss_bytes(),
    });
    println!(
        "CASA_RS_RAW_THROUGHPUT_REPORT={}",
        serde_json::to_string(&report).expect("serialize raw report")
    );
}

#[test]
#[ignore = "explicit 100 GB internal-disk MeasurementSet throughput probe"]
fn ignored_main_table_throughput_fixture() {
    let root = std::env::var_os("CASA_RS_MS_THROUGHPUT_ROOT")
        .map(PathBuf::from)
        .expect("CASA_RS_MS_THROUGHPUT_ROOT must name an empty internal-disk directory");
    let channel_count = std::env::var("CASA_RS_MS_THROUGHPUT_CHANNELS")
        .expect("CASA_RS_MS_THROUGHPUT_CHANNELS must be 16 or 1024")
        .parse::<usize>()
        .expect("channel count must be an integer");
    assert!(matches!(channel_count, 16 | 1024));
    let target_bytes = std::env::var("CASA_RS_MS_THROUGHPUT_TARGET_BYTES")
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .expect("target bytes must be an integer")
        })
        .unwrap_or(DEFAULT_TARGET_BYTES);
    fs::create_dir_all(&root).expect("create benchmark root");

    let output = root.join(format!("main-{channel_count}ch.ms"));
    assert!(!output.exists(), "refusing to replace {}", output.display());
    let estimated_bytes_per_row = estimated_physical_bytes_per_row(channel_count);
    let row_count = usize::try_from(target_bytes.div_ceil(estimated_bytes_per_row as u64))
        .expect("row count fits usize");
    let total_started = Instant::now();

    let output_target = MeasurementSetCreateTarget::prepare(&output, false).expect("output target");
    let staging = output_target.staging_path().to_path_buf();
    let mut measurement_set = MeasurementSet::create(
        &staging,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create MeasurementSet descriptor");
    fs::create_dir_all(&staging).expect("create staging directory");
    populate_minimal_metadata(&mut measurement_set, channel_count);

    let resources = MeasurementSetWriteResources {
        available_bytes: 512 * 1024 * 1024,
        maximum_live_batches: 2,
        tiled_column_buffer_bytes: STREAMING_TILED_COLUMN_BUFFER_BYTES,
    };
    let mut plan = MeasurementSetWritePlan::visibility_creation(
        row_count,
        CORRELATION_COUNT,
        channel_count,
        "VLA",
        resources,
    )
    .expect("visibility write plan");
    // The benchmark scalars are constants. Generated overrides exercise the
    // normal StandardStMan/IncrementalStMan save path without a second set of
    // row-proportional construction files competing for the measured disk.
    plan.scalar_columns.clear();
    let planned_maximum_resident_bytes = plan.maximum_resident_bytes();
    let planned_batch_rows = plan.batch_rows();
    let session = MeasurementSetWriteSession::start(&staging, plan).expect("write session");

    let data_row = deterministic_data_row(channel_count);
    let logical_row_bytes = visibility_logical_bytes_per_row(channel_count);
    let benchmark_batch_rows = (BENCHMARK_BATCH_BYTES / logical_row_bytes)
        .max(1)
        .min(planned_batch_rows.max(1));
    let producer_started = Instant::now();
    let mut remaining = row_count;
    while remaining > 0 {
        let batch_rows = remaining.min(benchmark_batch_rows);
        session
            .send_batch(MeasurementSetWriteBatch::Repeated {
                data_row: data_row.clone(),
                flag_row: false,
                uvw_row: [1.0, 2.0, 3.0],
                row_count: batch_rows,
            })
            .expect("send repeated visibility batch");
        remaining -= batch_rows;
    }
    let producer_enqueue_seconds = producer_started.elapsed().as_secs_f64();

    let mut overrides = constant_main_column_overrides(row_count);
    for column in ["DATA", "FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"] {
        overrides.insert_deferred(column);
    }
    let telemetry = session
        .save_and_finish(&mut measurement_set, &overrides)
        .expect("finish MeasurementSet");
    output_target.commit().expect("publish MeasurementSet");
    let application_write_seconds = total_started.elapsed().as_secs_f64();

    let sync_started = Instant::now();
    sync_tree(&output);
    let durable_sync_seconds = sync_started.elapsed().as_secs_f64();
    let (logical_size_bytes, allocated_size_bytes, file_count) = tree_size(&output);
    let durable_write_seconds = total_started.elapsed().as_secs_f64();

    let report = json!({
        "schema_version": 1,
        "kind": "measurement_set_main_throughput_fixture",
        "output_ms": output,
        "channel_count": channel_count,
        "correlation_count": CORRELATION_COUNT,
        "row_count": row_count,
        "target_bytes": target_bytes,
        "estimated_physical_bytes_per_row": estimated_bytes_per_row,
        "logical_visibility_bytes_per_row": logical_row_bytes,
        "logical_size_bytes": logical_size_bytes,
        "allocated_size_bytes": allocated_size_bytes,
        "file_count": file_count,
        "planned_batch_rows": planned_batch_rows,
        "benchmark_batch_rows": benchmark_batch_rows,
        "planned_maximum_resident_bytes": planned_maximum_resident_bytes,
        "producer_enqueue_seconds": producer_enqueue_seconds,
        "application_write_seconds": application_write_seconds,
        "durable_sync_seconds": durable_sync_seconds,
        "durable_write_seconds": durable_write_seconds,
        "application_logical_mb_per_second": rate(logical_size_bytes, application_write_seconds),
        "durable_logical_mb_per_second": rate(logical_size_bytes, durable_write_seconds),
        "application_allocated_mb_per_second": rate(allocated_size_bytes, application_write_seconds),
        "durable_allocated_mb_per_second": rate(allocated_size_bytes, durable_write_seconds),
        "writer_telemetry": telemetry,
        "peak_rss_bytes": peak_rss_bytes(),
    });
    println!(
        "CASA_RS_MS_THROUGHPUT_REPORT={}",
        serde_json::to_string(&report).expect("serialize report")
    );
}

fn estimated_physical_bytes_per_row(channel_count: usize) -> usize {
    let visibility = CORRELATION_COUNT * channel_count * std::mem::size_of::<Complex32>();
    let packed_flags = (CORRELATION_COUNT * channel_count).div_ceil(8);
    let uvw = 3 * std::mem::size_of::<f64>();
    let weights = 2 * CORRELATION_COUNT * std::mem::size_of::<f32>();
    let standard_scalars = 3 * std::mem::size_of::<i32>() + std::mem::size_of::<bool>();
    visibility + packed_flags + uvw + weights + standard_scalars
}

fn visibility_logical_bytes_per_row(channel_count: usize) -> usize {
    let samples = CORRELATION_COUNT * channel_count;
    samples * (std::mem::size_of::<Complex32>() + std::mem::size_of::<bool>())
        + 3 * std::mem::size_of::<f64>()
        + 2 * CORRELATION_COUNT * std::mem::size_of::<f32>()
}

fn deterministic_data_row(channel_count: usize) -> Vec<Complex32> {
    (0..CORRELATION_COUNT * channel_count)
        .map(|index| {
            let mixed = (index as u32).wrapping_mul(0x9E37_79B9).rotate_left(13);
            Complex32::new(
                f32::from_bits(0x3f00_0000 | (mixed & 0x007f_ffff)),
                f32::from_bits(0xbf00_0000 | (mixed.rotate_left(7) & 0x007f_ffff)),
            )
        })
        .collect()
}

fn constant_main_column_overrides(row_count: usize) -> ColumnOverrides {
    let mut overrides = ColumnOverrides::for_row_count(row_count);
    for plan in standard_main_scalar_column_plans() {
        let value = match plan.value_type {
            StreamedScalarType::Bool => ScalarValue::Bool(false),
            StreamedScalarType::Int32 => ScalarValue::Int32(0),
            StreamedScalarType::Float32 => ScalarValue::Float32(1.0),
            StreamedScalarType::Float64 => ScalarValue::Float64(1.0),
        };
        overrides.insert_generated_scalar(
            plan.name,
            GeneratedScalarColumn::constant(row_count, Some(value)),
        );
    }
    overrides
}

fn populate_minimal_metadata(measurement_set: &mut MeasurementSet, channel_count: usize) {
    measurement_set
        .antenna_mut()
        .expect("ANTENNA")
        .add_antenna(
            "BENCH00",
            "BENCH00",
            "GROUND-BASED",
            "ALT-AZ",
            [-1_601_185.0, -5_041_977.0, 3_554_875.0],
            [0.0; 3],
            25.0,
        )
        .expect("add antenna");

    let correlation_type = ArrayValue::Int32(
        ArrayD::from_shape_vec(vec![CORRELATION_COUNT], vec![9, 12])
            .expect("correlation type shape"),
    );
    let correlation_product = ArrayValue::Int32(
        ArrayD::from_shape_vec(vec![2, CORRELATION_COUNT], vec![0, 1, 0, 1])
            .expect("correlation product shape"),
    );
    measurement_set
        .subtable_mut(SubtableId::Polarization)
        .expect("POLARIZATION")
        .add_row(RecordValue::new(vec![
            RecordField::new("CORR_PRODUCT", Value::Array(correlation_product)),
            RecordField::new("CORR_TYPE", Value::Array(correlation_type)),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "NUM_CORR",
                Value::Scalar(ScalarValue::Int32(CORRELATION_COUNT as i32)),
            ),
        ]))
        .expect("add POLARIZATION row");

    let frequencies = (0..channel_count)
        .map(|channel| 1.0e9 + channel as f64 * 1.0e6)
        .collect::<Vec<_>>();
    let widths = vec![1.0e6; channel_count];
    let f64_array = |values: &[f64]| {
        Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![values.len()], values.to_vec()).expect("f64 array shape"),
        ))
    };
    measurement_set
        .subtable_mut(SubtableId::SpectralWindow)
        .expect("SPECTRAL_WINDOW")
        .add_row(RecordValue::new(vec![
            RecordField::new("CHAN_FREQ", f64_array(&frequencies)),
            RecordField::new("CHAN_WIDTH", f64_array(&widths)),
            RecordField::new("EFFECTIVE_BW", f64_array(&widths)),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "FREQ_GROUP_NAME",
                Value::Scalar(ScalarValue::String("benchmark".to_string())),
            ),
            RecordField::new("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
            RecordField::new(
                "NAME",
                Value::Scalar(ScalarValue::String(format!("{channel_count}ch"))),
            ),
            RecordField::new("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new(
                "NUM_CHAN",
                Value::Scalar(ScalarValue::Int32(channel_count as i32)),
            ),
            RecordField::new("REF_FREQUENCY", Value::Scalar(ScalarValue::Float64(1.0e9))),
            RecordField::new("RESOLUTION", f64_array(&widths)),
            RecordField::new(
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(channel_count as f64 * 1.0e6)),
            ),
        ]))
        .expect("add SPECTRAL_WINDOW row");

    measurement_set
        .subtable_mut(SubtableId::DataDescription)
        .expect("DATA_DESCRIPTION")
        .add_row(RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
        ]))
        .expect("add DATA_DESCRIPTION row");
}

fn sync_tree(root: &Path) {
    visit_files(root, &mut |path, _| {
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap_or_else(|error| panic!("open {} for sync: {error}", path.display()))
            .sync_all()
            .unwrap_or_else(|error| panic!("sync {}: {error}", path.display()));
    });
}

fn tree_size(root: &Path) -> (u64, u64, usize) {
    let mut logical = 0u64;
    let mut allocated = 0u64;
    let mut files = 0usize;
    visit_files(root, &mut |_, metadata| {
        use std::os::unix::fs::MetadataExt;
        logical = logical.saturating_add(metadata.len());
        allocated = allocated.saturating_add(metadata.blocks().saturating_mul(512));
        files += 1;
    });
    (logical, allocated, files)
}

fn visit_files(root: &Path, visitor: &mut impl FnMut(&Path, &fs::Metadata)) {
    for entry in
        fs::read_dir(root).unwrap_or_else(|error| panic!("read {}: {error}", root.display()))
    {
        let entry = entry.expect("directory entry");
        let path = entry.path();
        let metadata = entry.metadata().expect("entry metadata");
        if metadata.is_dir() {
            visit_files(&path, visitor);
        } else if metadata.is_file() {
            visitor(&path, &metadata);
        }
    }
}

fn rate(bytes: u64, seconds: f64) -> f64 {
    if seconds > 0.0 {
        bytes as f64 / seconds / 1_000_000.0
    } else {
        0.0
    }
}

#[cfg(target_os = "macos")]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    (result == 0).then(|| unsafe { usage.assume_init().ru_maxrss as u64 })
}

#[cfg(not(target_os = "macos"))]
fn peak_rss_bytes() -> Option<u64> {
    None
}
