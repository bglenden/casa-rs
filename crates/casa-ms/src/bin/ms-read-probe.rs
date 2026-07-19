// SPDX-License-Identifier: LGPL-3.0-or-later
//! `ms-read-probe` - visibility-buffer read timing and diagnostics.

use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;
use std::process;
use std::time::{Duration, Instant};

use casa_ms::{
    MeasurementSet, MsSelection, MsSelectionIoBudget, ResolvedMsSelectionRow, SourcePartition,
    SourcePartitionId, VisibilityBuffer, VisibilityBufferColumnReport, VisibilityBufferRequest,
    VisibilityBufferTimings, VisibilityComplexSamples, VisibilityDataColumn,
    VisibilityFloatSamples,
};
use casa_types::{ArrayValue, ScalarValue};
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
    let source_partition =
        source_partition_for_row(&ms, first_row, corr_count, full_channel_count)?;
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
    let row_blocks = if options.visibility_selection {
        visibility_selection_row_blocks(&ms, &source_partition, options.block_rows)?
    } else {
        contiguous_row_blocks(options.row_start, selected_end, options.block_rows)
    };
    let selected_rows = row_blocks.iter().map(Vec::len).sum::<usize>();
    if selected_rows == 0 {
        return Err("selected row count is zero after visibility selection".to_string());
    }

    let mut buffer = VisibilityBuffer::default();
    let mut block_reports = Vec::new();
    let mut aggregate = AggregateFill::default();
    let mut column_aggregates = ColumnAggregates::default();
    let probe_started = Instant::now();
    for repeat_index in 0..options.repeat {
        for row_indices in &row_blocks {
            let request = options.sidecars.apply_to(
                VisibilityBufferRequest::imaging(
                    options.data_column,
                    row_indices.clone(),
                    options.channel_start,
                    channel_count,
                )
                .with_source_partition(source_partition.clone()),
            );
            let request = options.columns.apply_to(request);
            let started = Instant::now();
            let fill_report = ms
                .fill_visibility_buffer(&request, &mut buffer)
                .map_err(|error| error.to_string())?;
            let elapsed = started.elapsed();
            column_aggregates.add(&fill_report.columns);
            aggregate.add(&fill_report);
            block_reports.push(BlockProbeReport {
                repeat_index,
                row_start: row_indices.first().copied().unwrap_or(0),
                row_count: row_indices.len(),
                elapsed_ns: elapsed.as_nanos(),
                logical_output_bytes: fill_report.logical_output_bytes,
                modeled_physical_read_bytes: fill_report.modeled_physical_read_bytes,
                allocation_reused_buffers: fill_report.allocation.reused_buffers,
                allocation_grown_or_retyped_buffers: fill_report
                    .allocation
                    .grown_or_retyped_buffers,
            });
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
            source_partition: SourcePartitionReport::from_partition(&source_partition),
            data_manager_info: data_manager_info(&ms),
        },
        selection: SelectionReport {
            row_start: options.row_start,
            row_count: selected_rows,
            channel_start: options.channel_start,
            channel_count,
            block_rows: options.block_rows,
            repeat: options.repeat,
            sidecars: options.sidecars,
            columns: options.columns,
            visibility_selection: options.visibility_selection,
        },
        elapsed_ns: probe_elapsed.as_nanos(),
        aggregate,
        block_stats,
        columns: column_aggregates.into_reports(),
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
    sidecars: ProbeSidecarMode,
    columns: ProbeColumnSelection,
    verify_full_read_rows: Option<usize>,
    visibility_selection: bool,
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
        let mut sidecars = ProbeSidecarMode::Imaging;
        let mut columns = ProbeColumnSelection::default();
        let mut verify_full_read_rows = None;
        let mut visibility_selection = false;

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
                "--sidecars" => {
                    index += 1;
                    sidecars = parse_sidecars(args.get(index).ok_or_else(usage)?)?;
                }
                "--columns" => {
                    index += 1;
                    columns = parse_columns(args.get(index).ok_or_else(usage)?)?;
                }
                "--verify-full-read" => {
                    index += 1;
                    verify_full_read_rows = Some(parse_usize(
                        args.get(index).ok_or_else(usage)?,
                        "--verify-full-read",
                    )?);
                }
                "--visibility-selection" => {
                    visibility_selection = true;
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
            sidecars,
            columns,
            verify_full_read_rows,
            visibility_selection,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProbeSidecarMode {
    Minimal,
    Imaging,
    Full,
}

impl ProbeSidecarMode {
    fn apply_to(self, mut request: VisibilityBufferRequest) -> VisibilityBufferRequest {
        match self {
            Self::Minimal => {
                request.include_antenna_ids = false;
                request.include_data_desc_ids = false;
                request.include_field_ids = false;
                request.include_flag_row = false;
                request.include_time = false;
                request.include_interval = false;
                request.include_exposure = false;
                request.include_array_ids = false;
                request.include_observation_ids = false;
                request.include_scan_numbers = false;
                request.include_state_ids = false;
            }
            Self::Imaging => {}
            Self::Full => {
                request.include_time = true;
                request.include_interval = true;
                request.include_exposure = true;
                request.include_array_ids = true;
                request.include_observation_ids = true;
                request.include_scan_numbers = true;
                request.include_state_ids = true;
            }
        }
        request
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct ProbeColumnSelection {
    data: bool,
    flags: bool,
    weights: bool,
    weight_spectrum: bool,
    uvw: bool,
}

impl Default for ProbeColumnSelection {
    fn default() -> Self {
        Self {
            data: true,
            flags: true,
            weights: true,
            weight_spectrum: true,
            uvw: true,
        }
    }
}

impl ProbeColumnSelection {
    fn apply_to(self, mut request: VisibilityBufferRequest) -> VisibilityBufferRequest {
        request.include_data = self.data;
        request.include_flags = self.flags;
        request.include_weights = self.weights;
        request.include_weight_spectrum = self.weight_spectrum;
        request.include_uvw = self.uvw;
        request
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
    columns: Vec<ColumnAggregateReport>,
    verification: Option<VerificationReport>,
    peak_rss_bytes: Option<u64>,
    throughput: ThroughputReport,
}

#[derive(Debug, Serialize)]
struct DatasetShapeReport {
    row_count: usize,
    corr_count: usize,
    full_channel_count: usize,
    source_partition: SourcePartitionReport,
    data_manager_info: Vec<DataManagerReport>,
}

#[derive(Debug, Serialize)]
struct SourcePartitionReport {
    id: usize,
    ms_id: usize,
    data_desc_id: i32,
    spectral_window_id: i32,
    polarization_id: i32,
    channel_count: usize,
    corr_count: usize,
}

impl SourcePartitionReport {
    fn from_partition(partition: &SourcePartition) -> Self {
        Self {
            id: partition.id.0,
            ms_id: partition.ms_id,
            data_desc_id: partition.data_desc_id,
            spectral_window_id: partition.spw_id,
            polarization_id: partition.polarization_id,
            channel_count: partition.shape.channel_count,
            corr_count: partition.shape.corr_count,
        }
    }
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
    sidecars: ProbeSidecarMode,
    columns: ProbeColumnSelection,
    visibility_selection: bool,
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

#[derive(Debug, Default)]
struct ColumnAggregates {
    columns: BTreeMap<String, ColumnAggregateReport>,
}

impl ColumnAggregates {
    fn add(&mut self, reports: &[VisibilityBufferColumnReport]) {
        for report in reports {
            let aggregate = self
                .columns
                .entry(report.column.clone())
                .or_insert_with(|| ColumnAggregateReport {
                    column: report.column.clone(),
                    primitive_type: report.primitive_type.clone(),
                    channelized: report.channelized,
                    channel_read_granularity: report.channel_read_granularity,
                    element_bytes: report.element_bytes,
                    fills: 0,
                    logical_output_bytes: 0,
                    modeled_physical_read_bytes: 0,
                    data_manager_types: report.data_manager_types.clone(),
                });
            aggregate.fills += 1;
            aggregate.logical_output_bytes = aggregate
                .logical_output_bytes
                .saturating_add(report.logical_output_bytes);
            aggregate.modeled_physical_read_bytes = aggregate
                .modeled_physical_read_bytes
                .saturating_add(report.modeled_physical_read_bytes);
        }
    }

    fn into_reports(self) -> Vec<ColumnAggregateReport> {
        self.columns.into_values().collect()
    }
}

#[derive(Debug, Clone, Serialize)]
struct ColumnAggregateReport {
    column: String,
    primitive_type: String,
    channelized: bool,
    channel_read_granularity: casa_ms::VisibilityChannelReadGranularity,
    element_bytes: usize,
    fills: usize,
    logical_output_bytes: u64,
    modeled_physical_read_bytes: u64,
    data_manager_types: Vec<String>,
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
        .array_cells_owned_uncached(&[row])?
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| {
            casa_ms::MsError::InvalidInput(format!("{} row {row} is missing", data_column.name()))
        })?;
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

fn source_partition_for_row(
    ms: &MeasurementSet,
    row: usize,
    corr_count: usize,
    full_channel_count: usize,
) -> Result<SourcePartition, String> {
    let values = ms
        .main_table()
        .column_accessor("DATA_DESC_ID")
        .and_then(|column| column.scalar_cells_owned_for_rows(&[row]))
        .map_err(|error| format!("read DATA_DESC_ID row {row}: {error}"))?;
    let value = values
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| format!("DATA_DESC_ID row {row} is missing"))?;
    let data_desc_id = match value {
        ScalarValue::Int32(value) if value >= 0 => value,
        ScalarValue::Int32(value) => {
            return Err(format!("DATA_DESC_ID row {row} is negative: {value}"));
        }
        other => {
            return Err(format!(
                "DATA_DESC_ID row {row} must be Int32, found {:?}",
                other.primitive_type()
            ));
        }
    };
    let data_description = ms
        .data_description()
        .map_err(|error| format!("open DATA_DESCRIPTION: {error}"))?;
    let spw_id = data_description
        .spectral_window_id(data_desc_id as usize)
        .map_err(|error| format!("read DATA_DESCRIPTION.SPECTRAL_WINDOW_ID: {error}"))?;
    let polarization_id = data_description
        .polarization_id(data_desc_id as usize)
        .map_err(|error| format!("read DATA_DESCRIPTION.POLARIZATION_ID: {error}"))?;
    if spw_id < 0 || polarization_id < 0 {
        return Err(format!(
            "DATA_DESCRIPTION row {data_desc_id} has negative SPW/POLARIZATION ids: {spw_id}/{polarization_id}"
        ));
    }
    Ok(SourcePartition::new(
        SourcePartitionId(0),
        0,
        data_desc_id,
        spw_id,
        polarization_id,
        full_channel_count,
        corr_count,
    ))
}

fn contiguous_row_blocks(
    row_start: usize,
    selected_end: usize,
    block_rows: usize,
) -> Vec<Vec<usize>> {
    let mut blocks = Vec::new();
    let mut block_start = row_start;
    while block_start < selected_end {
        let block_end = block_start.saturating_add(block_rows).min(selected_end);
        blocks.push((block_start..block_end).collect());
        block_start = block_end;
    }
    blocks
}

fn visibility_selection_row_blocks(
    ms: &MeasurementSet,
    source_partition: &SourcePartition,
    block_rows: usize,
) -> Result<Vec<Vec<usize>>, String> {
    let row_bytes = std::mem::size_of::<ResolvedMsSelectionRow>();
    let available_bytes = block_rows
        .checked_mul(row_bytes)
        .ok_or_else(|| "visibility selection row-block byte accounting overflowed".to_string())?;
    let plan = ms
        .resolve_selection(
            &MsSelection::new().data_description(&[source_partition.data_desc_id]),
            MsSelectionIoBudget {
                available_bytes,
                maximum_live_blocks: 1,
                requested_bytes_per_row: row_bytes,
                storage_alignment_rows: None,
            },
        )
        .map_err(|error| format!("select visibility rows: {error}"))?;
    let active_rows = plan
        .selected_rows
        .into_iter()
        .filter_map(|row| (!row.flag_row).then_some(row.row_index))
        .collect::<Vec<_>>();
    Ok(active_rows
        .chunks(block_rows)
        .map(|chunk| chunk.to_vec())
        .collect())
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

fn parse_sidecars(value: &str) -> Result<ProbeSidecarMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "minimal" => Ok(ProbeSidecarMode::Minimal),
        "imaging" => Ok(ProbeSidecarMode::Imaging),
        "full" => Ok(ProbeSidecarMode::Full),
        _ => Err(format!(
            "unknown sidecar mode {value:?}; expected minimal, imaging, or full"
        )),
    }
}

fn parse_columns(value: &str) -> Result<ProbeColumnSelection, String> {
    let mut selection = ProbeColumnSelection {
        data: false,
        flags: false,
        weights: false,
        weight_spectrum: false,
        uvw: false,
    };
    for item in value.split(',') {
        match item.trim().to_ascii_lowercase().as_str() {
            "" => {}
            "all" => selection = ProbeColumnSelection::default(),
            "data" => selection.data = true,
            "flag" | "flags" => selection.flags = true,
            "weight" | "weights" => selection.weights = true,
            "weight_spectrum" | "weight-spectrum" | "weightspectrum" => {
                selection.weight_spectrum = true;
            }
            "uvw" => selection.uvw = true,
            other => {
                return Err(format!(
                    "unknown read-probe column {other:?}; expected all, data, flags, weights, weight_spectrum, or uvw"
                ));
            }
        }
    }
    if !(selection.data
        || selection.flags
        || selection.weights
        || selection.weight_spectrum
        || selection.uvw)
    {
        return Err("--columns must select at least one visibility column".to_string());
    }
    Ok(selection)
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
     [--block-rows N] [--repeat N] [--sidecars minimal|imaging|full] \
     [--columns all|data,flags,weights,weight_spectrum,uvw] \
     [--verify-full-read N]"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_columns_accepts_single_and_multiple_visibility_columns() {
        let data_only = parse_columns("data").expect("data column selection");
        assert!(data_only.data);
        assert!(!data_only.flags);
        assert!(!data_only.weights);
        assert!(!data_only.weight_spectrum);
        assert!(!data_only.uvw);

        let mixed = parse_columns("flags,weight-spectrum,uvw").expect("mixed columns");
        assert!(!mixed.data);
        assert!(mixed.flags);
        assert!(!mixed.weights);
        assert!(mixed.weight_spectrum);
        assert!(mixed.uvw);
    }

    #[test]
    fn parse_columns_rejects_empty_and_unknown_selections() {
        assert!(parse_columns("").is_err());
        assert!(parse_columns("data,phase").is_err());
    }

    #[test]
    fn probe_column_selection_controls_visibility_request_columns() {
        let request = VisibilityBufferRequest::imaging(VisibilityDataColumn::Data, vec![0], 0, 4);
        let request = parse_columns("data,uvw")
            .expect("column selection")
            .apply_to(request);
        assert!(request.include_data);
        assert!(!request.include_flags);
        assert!(!request.include_weights);
        assert!(!request.include_weight_spectrum);
        assert!(request.include_uvw);
        assert!(request.include_antenna_ids);
    }
}
