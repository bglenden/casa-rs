// SPDX-License-Identifier: LGPL-3.0-or-later
//! `ms-read-probe` - visibility-buffer read timing and diagnostics.

use std::env;
use std::path::PathBuf;
use std::process;
use std::time::{Duration, Instant};

use casa_ms::{
    MeasurementSet, VisibilityBuffer, VisibilityBufferColumnReport, VisibilityBufferRequest,
    VisibilityBufferTimings, VisibilityComplexSamples, VisibilityDataColumn,
    VisibilityFloatSamples,
};
use casa_types::ArrayValue;
use serde::Serialize;

fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("Error: {error}");
            process::exit(1);
        }
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{}", usage());
        return Ok(());
    }
    let options = ProbeOptions::parse(&args)?;
    let ms = MeasurementSet::open(&options.ms_path).map_err(|error| error.to_string())?;
    let first_row = options.row_start;
    if first_row >= ms.row_count() {
        return Err(format!(
            "--row-start {} is outside MS row count {}",
            options.row_start,
            ms.row_count()
        ));
    }
    let (corr_count, full_channel_count) =
        data_shape(&ms, options.data_column, first_row).map_err(|error| error.to_string())?;
    let channel_count = options
        .channel_count
        .unwrap_or_else(|| full_channel_count.saturating_sub(options.channel_start));
    if options.channel_start.saturating_add(channel_count) > full_channel_count {
        return Err(format!(
            "requested channels {}..{} exceed full channel count {full_channel_count}",
            options.channel_start,
            options.channel_start + channel_count
        ));
    }
    let row_count = options
        .row_count
        .unwrap_or_else(|| ms.row_count().saturating_sub(options.row_start));
    let selected_end = options
        .row_start
        .checked_add(row_count)
        .ok_or_else(|| "row range overflow".to_string())?
        .min(ms.row_count());
    let selected_rows = selected_end.saturating_sub(options.row_start);
    if selected_rows == 0 {
        return Err("selected row count is zero".to_string());
    }

    let mut buffer = VisibilityBuffer::default();
    let mut block_reports = Vec::new();
    let mut aggregate = AggregateFill::default();
    let probe_started = Instant::now();
    for repeat_index in 0..options.repeat {
        let mut block_start = options.row_start;
        while block_start < selected_end {
            let block_end = block_start
                .saturating_add(options.block_rows)
                .min(selected_end);
            let row_indices = (block_start..block_end).collect::<Vec<_>>();
            let request = VisibilityBufferRequest::imaging(
                options.data_column,
                row_indices,
                options.channel_start,
                channel_count,
            );
            let started = Instant::now();
            let fill_report = ms
                .fill_visibility_buffer(&request, &mut buffer)
                .map_err(|error| error.to_string())?;
            let elapsed = started.elapsed();
            aggregate.add(&fill_report);
            block_reports.push(BlockProbeReport {
                repeat_index,
                row_start: block_start,
                row_count: block_end - block_start,
                elapsed_ns: elapsed.as_nanos(),
                logical_output_bytes: fill_report.logical_output_bytes,
                modeled_physical_read_bytes: fill_report.modeled_physical_read_bytes,
                allocation_reused_buffers: fill_report.allocation.reused_buffers,
                allocation_grown_or_retyped_buffers: fill_report
                    .allocation
                    .grown_or_retyped_buffers,
            });
            block_start = block_end;
        }
    }
    let probe_elapsed = probe_started.elapsed();
    let verification = options
        .verify_full_read_rows
        .map(|limit| {
            verify_full_read(
                &ms,
                options.data_column,
                options.row_start,
                selected_rows.min(limit),
                options.channel_start,
                channel_count,
                full_channel_count,
            )
        })
        .transpose()?;

    let block_stats = BlockStats::from_blocks(&block_reports);
    let probe_report = MsReadProbeReport {
        ms_path: options.ms_path.display().to_string(),
        data_column: options.data_column.name().to_string(),
        dataset: DatasetShapeReport {
            row_count: ms.row_count(),
            corr_count,
            full_channel_count,
            data_manager_info: data_manager_info(&ms),
        },
        selection: SelectionReport {
            row_start: options.row_start,
            row_count: selected_rows,
            channel_start: options.channel_start,
            channel_count,
            block_rows: options.block_rows,
            repeat: options.repeat,
        },
        elapsed_ns: probe_elapsed.as_nanos(),
        aggregate,
        block_stats,
        columns: last_column_reports(&ms, &options, channel_count)?,
        verification,
        peak_rss_bytes: peak_rss_bytes(),
        throughput: ThroughputReport::from_totals(
            aggregate.logical_output_bytes,
            aggregate.modeled_physical_read_bytes,
            probe_elapsed,
        ),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&probe_report).map_err(|error| error.to_string())?
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct ProbeOptions {
    ms_path: PathBuf,
    data_column: VisibilityDataColumn,
    row_start: usize,
    row_count: Option<usize>,
    channel_start: usize,
    channel_count: Option<usize>,
    block_rows: usize,
    repeat: usize,
    verify_full_read_rows: Option<usize>,
}

impl ProbeOptions {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut ms_path = None;
        let mut data_column = VisibilityDataColumn::Data;
        let mut row_start = 0usize;
        let mut row_count = None;
        let mut channel_start = 0usize;
        let mut channel_count = None;
        let mut block_rows = 8192usize;
        let mut repeat = 1usize;
        let mut verify_full_read_rows = None;

        let mut index = 0usize;
        while index < args.len() {
            match args[index].as_str() {
                "--ms" | "--vis" => {
                    index += 1;
                    ms_path = Some(PathBuf::from(args.get(index).ok_or_else(usage)?));
                }
                "--datacolumn" | "--data-column" => {
                    index += 1;
                    data_column = parse_data_column(args.get(index).ok_or_else(usage)?)?;
                }
                "--row-start" => {
                    index += 1;
                    row_start = parse_usize(args.get(index).ok_or_else(usage)?, "--row-start")?;
                }
                "--row-count" => {
                    index += 1;
                    row_count = Some(parse_usize(
                        args.get(index).ok_or_else(usage)?,
                        "--row-count",
                    )?);
                }
                "--channel-start" | "--chan-start" => {
                    index += 1;
                    channel_start =
                        parse_usize(args.get(index).ok_or_else(usage)?, "--channel-start")?;
                }
                "--channel-count" | "--chan-count" => {
                    index += 1;
                    channel_count = Some(parse_usize(
                        args.get(index).ok_or_else(usage)?,
                        "--channel-count",
                    )?);
                }
                "--block-rows" => {
                    index += 1;
                    block_rows = parse_usize(args.get(index).ok_or_else(usage)?, "--block-rows")?;
                }
                "--repeat" => {
                    index += 1;
                    repeat = parse_usize(args.get(index).ok_or_else(usage)?, "--repeat")?;
                }
                "--verify-full-read" => {
                    index += 1;
                    verify_full_read_rows = Some(parse_usize(
                        args.get(index).ok_or_else(usage)?,
                        "--verify-full-read",
                    )?);
                }
                other => return Err(format!("unknown argument {other:?}\n{}", usage())),
            }
            index += 1;
        }
        if block_rows == 0 {
            return Err("--block-rows must be greater than zero".to_string());
        }
        if repeat == 0 {
            return Err("--repeat must be greater than zero".to_string());
        }
        Ok(Self {
            ms_path: ms_path.ok_or_else(usage)?,
            data_column,
            row_start,
            row_count,
            channel_start,
            channel_count,
            block_rows,
            repeat,
            verify_full_read_rows,
        })
    }
}

#[derive(Debug, Serialize)]
struct MsReadProbeReport {
    ms_path: String,
    data_column: String,
    dataset: DatasetShapeReport,
    selection: SelectionReport,
    elapsed_ns: u128,
    aggregate: AggregateFill,
    block_stats: BlockStats,
    columns: Vec<VisibilityBufferColumnReport>,
    verification: Option<VerificationReport>,
    peak_rss_bytes: Option<u64>,
    throughput: ThroughputReport,
}

#[derive(Debug, Serialize)]
struct DatasetShapeReport {
    row_count: usize,
    corr_count: usize,
    full_channel_count: usize,
    data_manager_info: Vec<DataManagerReport>,
}

#[derive(Debug, Serialize)]
struct DataManagerReport {
    seq_nr: u32,
    dm_type: String,
    columns: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SelectionReport {
    row_start: usize,
    row_count: usize,
    channel_start: usize,
    channel_count: usize,
    block_rows: usize,
    repeat: usize,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct AggregateFill {
    fills: usize,
    logical_output_bytes: u64,
    modeled_physical_read_bytes: u64,
    timings: VisibilityBufferTimings,
}

impl AggregateFill {
    fn add(&mut self, report: &casa_ms::VisibilityBufferFillReport) {
        self.fills += 1;
        self.logical_output_bytes = self
            .logical_output_bytes
            .saturating_add(report.logical_output_bytes);
        self.modeled_physical_read_bytes = self
            .modeled_physical_read_bytes
            .saturating_add(report.modeled_physical_read_bytes);
        self.timings.total_ns = self
            .timings
            .total_ns
            .saturating_add(report.timings.total_ns);
        self.timings.data_ns = self.timings.data_ns.saturating_add(report.timings.data_ns);
        self.timings.flags_ns = self
            .timings
            .flags_ns
            .saturating_add(report.timings.flags_ns);
        self.timings.weights_ns = self
            .timings
            .weights_ns
            .saturating_add(report.timings.weights_ns);
        self.timings.weight_spectrum_ns = self
            .timings
            .weight_spectrum_ns
            .saturating_add(report.timings.weight_spectrum_ns);
        self.timings.uvw_ns = self.timings.uvw_ns.saturating_add(report.timings.uvw_ns);
        self.timings.scalar_ns = self
            .timings
            .scalar_ns
            .saturating_add(report.timings.scalar_ns);
    }
}

#[derive(Debug, Clone, Serialize)]
struct BlockProbeReport {
    repeat_index: usize,
    row_start: usize,
    row_count: usize,
    elapsed_ns: u128,
    logical_output_bytes: u64,
    modeled_physical_read_bytes: u64,
    allocation_reused_buffers: usize,
    allocation_grown_or_retyped_buffers: usize,
}

#[derive(Debug, Default, Serialize)]
struct BlockStats {
    block_count: usize,
    min_elapsed_ns: u128,
    median_elapsed_ns: u128,
    max_elapsed_ns: u128,
    slowest_row_start: usize,
    slowest_row_count: usize,
}

impl BlockStats {
    fn from_blocks(blocks: &[BlockProbeReport]) -> Self {
        if blocks.is_empty() {
            return Self::default();
        }
        let mut elapsed = blocks
            .iter()
            .map(|block| block.elapsed_ns)
            .collect::<Vec<_>>();
        elapsed.sort_unstable();
        let slowest = blocks
            .iter()
            .max_by_key(|block| block.elapsed_ns)
            .expect("nonempty blocks");
        Self {
            block_count: blocks.len(),
            min_elapsed_ns: elapsed[0],
            median_elapsed_ns: elapsed[elapsed.len() / 2],
            max_elapsed_ns: *elapsed.last().expect("nonempty elapsed"),
            slowest_row_start: slowest.row_start,
            slowest_row_count: slowest.row_count,
        }
    }
}

#[derive(Debug, Serialize)]
struct VerificationReport {
    rows_checked: usize,
    channel_start: usize,
    channel_count: usize,
    data_equal: bool,
    flags_equal: bool,
    weight_spectrum_equal: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ThroughputReport {
    logical_output_gib_per_s: f64,
    modeled_physical_gib_per_s: f64,
}

impl ThroughputReport {
    fn from_totals(logical_bytes: u64, physical_bytes: u64, elapsed: Duration) -> Self {
        let seconds = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
        Self {
            logical_output_gib_per_s: logical_bytes as f64 / (1024.0 * 1024.0 * 1024.0) / seconds,
            modeled_physical_gib_per_s: physical_bytes as f64
                / (1024.0 * 1024.0 * 1024.0)
                / seconds,
        }
    }
}

fn last_column_reports(
    ms: &MeasurementSet,
    options: &ProbeOptions,
    channel_count: usize,
) -> Result<Vec<VisibilityBufferColumnReport>, String> {
    let rows = (options.row_start..options.row_start + options.block_rows.min(1)).collect();
    let request = VisibilityBufferRequest::imaging(
        options.data_column,
        rows,
        options.channel_start,
        channel_count,
    );
    let mut buffer = VisibilityBuffer::default();
    let report = ms
        .fill_visibility_buffer(&request, &mut buffer)
        .map_err(|error| error.to_string())?;
    Ok(report.columns)
}

fn verify_full_read(
    ms: &MeasurementSet,
    data_column: VisibilityDataColumn,
    row_start: usize,
    rows: usize,
    channel_start: usize,
    channel_count: usize,
    full_channel_count: usize,
) -> Result<VerificationReport, String> {
    if rows == 0 {
        return Ok(VerificationReport {
            rows_checked: 0,
            channel_start,
            channel_count,
            data_equal: true,
            flags_equal: true,
            weight_spectrum_equal: None,
        });
    }
    let row_indices = (row_start..row_start + rows).collect::<Vec<_>>();
    let sliced_request = VisibilityBufferRequest::imaging(
        data_column,
        row_indices.clone(),
        channel_start,
        channel_count,
    );
    let full_request =
        VisibilityBufferRequest::imaging(data_column, row_indices, 0, full_channel_count);
    let mut sliced = VisibilityBuffer::default();
    let mut full = VisibilityBuffer::default();
    ms.fill_visibility_buffer(&sliced_request, &mut sliced)
        .map_err(|error| error.to_string())?;
    ms.fill_visibility_buffer(&full_request, &mut full)
        .map_err(|error| error.to_string())?;
    let data_equal = compare_complex_window(
        sliced.data.as_ref(),
        full.data.as_ref(),
        rows,
        sliced.corr_count,
        channel_start,
        channel_count,
    );
    let flags_equal = compare_bool_window(
        sliced.flags.as_deref(),
        full.flags.as_deref(),
        rows,
        sliced.corr_count,
        channel_start,
        channel_count,
    );
    let weight_spectrum_equal = match (&sliced.weight_spectrum, &full.weight_spectrum) {
        (None, None) => None,
        (left, right) => Some(compare_float_window(
            left.as_ref(),
            right.as_ref(),
            rows,
            sliced.corr_count,
            channel_start,
            channel_count,
        )),
    };
    Ok(VerificationReport {
        rows_checked: rows,
        channel_start,
        channel_count,
        data_equal,
        flags_equal,
        weight_spectrum_equal,
    })
}

fn compare_complex_window(
    sliced: Option<&VisibilityComplexSamples>,
    full: Option<&VisibilityComplexSamples>,
    row_count: usize,
    corr_count: usize,
    channel_start: usize,
    channel_count: usize,
) -> bool {
    match (sliced, full) {
        (
            Some(VisibilityComplexSamples::Complex32(left)),
            Some(VisibilityComplexSamples::Complex32(right)),
        ) => compare_window(
            left,
            right,
            row_count,
            corr_count,
            channel_start,
            channel_count,
        ),
        (
            Some(VisibilityComplexSamples::Complex64(left)),
            Some(VisibilityComplexSamples::Complex64(right)),
        ) => compare_window(
            left,
            right,
            row_count,
            corr_count,
            channel_start,
            channel_count,
        ),
        _ => false,
    }
}

fn compare_float_window(
    sliced: Option<&VisibilityFloatSamples>,
    full: Option<&VisibilityFloatSamples>,
    row_count: usize,
    corr_count: usize,
    channel_start: usize,
    channel_count: usize,
) -> bool {
    match (sliced, full) {
        (
            Some(VisibilityFloatSamples::Float32(left)),
            Some(VisibilityFloatSamples::Float32(right)),
        ) => compare_window(
            left,
            right,
            row_count,
            corr_count,
            channel_start,
            channel_count,
        ),
        (
            Some(VisibilityFloatSamples::Float64(left)),
            Some(VisibilityFloatSamples::Float64(right)),
        ) => compare_window(
            left,
            right,
            row_count,
            corr_count,
            channel_start,
            channel_count,
        ),
        _ => false,
    }
}

fn compare_bool_window(
    sliced: Option<&[bool]>,
    full: Option<&[bool]>,
    row_count: usize,
    corr_count: usize,
    channel_start: usize,
    channel_count: usize,
) -> bool {
    match (sliced, full) {
        (Some(left), Some(right)) => compare_window(
            left,
            right,
            row_count,
            corr_count,
            channel_start,
            channel_count,
        ),
        _ => false,
    }
}

fn compare_window<T: PartialEq>(
    sliced: &[T],
    full: &[T],
    row_count: usize,
    corr_count: usize,
    channel_start: usize,
    channel_count: usize,
) -> bool {
    for channel_slot in 0..channel_count {
        for row_slot in 0..row_count {
            for corr_slot in 0..corr_count {
                let left_index =
                    sample_index(channel_slot, row_slot, corr_slot, row_count, corr_count);
                let right_index = sample_index(
                    channel_start + channel_slot,
                    row_slot,
                    corr_slot,
                    row_count,
                    corr_count,
                );
                if sliced.get(left_index) != full.get(right_index) {
                    return false;
                }
            }
        }
    }
    true
}

fn sample_index(
    channel_slot: usize,
    row_slot: usize,
    corr_slot: usize,
    row_count: usize,
    corr_count: usize,
) -> usize {
    (channel_slot * row_count + row_slot) * corr_count + corr_slot
}

fn data_shape(
    ms: &MeasurementSet,
    data_column: VisibilityDataColumn,
    row: usize,
) -> Result<(usize, usize), casa_ms::MsError> {
    let value = ms
        .main_table()
        .column_accessor(data_column.name())?
        .array_cell(row)?;
    match value {
        ArrayValue::Complex32(values) => Ok((values.shape()[0], values.shape()[1])),
        ArrayValue::Complex64(values) => Ok((values.shape()[0], values.shape()[1])),
        other => Err(casa_ms::MsError::InvalidInput(format!(
            "{} row {row} must be complex rank-2, found {:?}",
            data_column.name(),
            other.primitive_type()
        ))),
    }
}

fn data_manager_info(ms: &MeasurementSet) -> Vec<DataManagerReport> {
    ms.main_table()
        .data_manager_info()
        .iter()
        .map(|manager| DataManagerReport {
            seq_nr: manager.seq_nr,
            dm_type: manager.dm_type.clone(),
            columns: manager.columns.clone(),
        })
        .collect()
}

fn parse_data_column(value: &str) -> Result<VisibilityDataColumn, String> {
    match value.to_ascii_uppercase().as_str() {
        "DATA" => Ok(VisibilityDataColumn::Data),
        "CORRECTED" | "CORRECTED_DATA" => Ok(VisibilityDataColumn::CorrectedData),
        "MODEL" | "MODEL_DATA" => Ok(VisibilityDataColumn::ModelData),
        _ => Err(format!("unknown data column {value:?}")),
    }
}

fn parse_usize(value: &str, name: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("{name} must be a non-negative integer: {error}"))
}

#[cfg(unix)]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    let max_rss = unsafe { usage.assume_init() }.ru_maxrss;
    if max_rss < 0 {
        return None;
    }
    #[cfg(target_os = "macos")]
    {
        Some(max_rss as u64)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Some((max_rss as u64).saturating_mul(1024))
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> Option<u64> {
    None
}

fn usage() -> String {
    "Usage: ms-read-probe --ms PATH [--datacolumn DATA|CORRECTED_DATA|MODEL_DATA] \
     [--row-start N] [--row-count N] [--channel-start N] [--channel-count N] \
     [--block-rows N] [--repeat N] [--verify-full-read N]"
        .to_string()
}
