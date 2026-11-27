// SPDX-License-Identifier: LGPL-3.0-or-later
//! Caller-owned columnar visibility buffers for efficient MeasurementSet scans.
//!
//! The buffer API is intended for streaming imaging and diagnostic readers
//! that want to reuse allocations across row blocks while reading only the
//! source channels needed by a schedule candidate.

use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;
use std::ops::Range;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
use casa_core::ScalarValue;
use casa_core::{ArrayValue, Complex32, Complex64, PrimitiveType};
use casa_tables::{
    RequiredScalarColumnValues, SelectedArray1DCells, SelectedArray2DCellsMut, Table,
};
use ndarray::Ix1;
use serde::Serialize;

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;
use crate::schema::main_table::VisibilityDataColumn;

mod reads;
mod telemetry;

use reads::*;
use telemetry::*;

/// Request for filling a caller-owned [`VisibilityBuffer`].
#[derive(Debug, Clone)]
pub struct VisibilityBufferRequest {
    /// Homogeneous source partition represented by this request.
    pub source_partition: Option<SourcePartition>,
    /// Complex visibility column to read when [`include_data`](Self::include_data) is true.
    pub data_column: VisibilityDataColumn,
    /// Main-table row indices to read, in output order.
    pub row_indices: Vec<usize>,
    /// First source channel to read from channelized 2-D array columns.
    pub channel_start: usize,
    /// Number of source channels to read from channelized 2-D array columns.
    pub channel_count: usize,
    /// Read the selected complex visibility data column.
    pub include_data: bool,
    /// Read the selected `FLAG` channel range.
    pub include_flags: bool,
    /// Read per-correlation `WEIGHT` arrays.
    pub include_weights: bool,
    /// Read `WEIGHT_SPECTRUM` when the column exists.
    pub include_weight_spectrum: bool,
    /// Read per-row `UVW` coordinates.
    pub include_uvw: bool,
    /// Read `ANTENNA1` and `ANTENNA2`.
    pub include_antenna_ids: bool,
    /// Read `DATA_DESC_ID`.
    pub include_data_desc_ids: bool,
    /// Read `FIELD_ID`.
    pub include_field_ids: bool,
    /// Read `FLAG_ROW`.
    pub include_flag_row: bool,
    /// Read `TIME`.
    pub include_time: bool,
    /// Read `INTERVAL`.
    pub include_interval: bool,
    /// Read `EXPOSURE`.
    pub include_exposure: bool,
    /// Read `ARRAY_ID`.
    pub include_array_ids: bool,
    /// Read `OBSERVATION_ID`.
    pub include_observation_ids: bool,
    /// Read `SCAN_NUMBER`.
    pub include_scan_numbers: bool,
    /// Read `STATE_ID`.
    pub include_state_ids: bool,
}

impl VisibilityBufferRequest {
    /// Create an imaging-oriented request for selected rows and channels.
    pub fn imaging(
        data_column: VisibilityDataColumn,
        row_indices: Vec<usize>,
        channel_start: usize,
        channel_count: usize,
    ) -> Self {
        Self {
            source_partition: None,
            data_column,
            row_indices,
            channel_start,
            channel_count,
            include_data: true,
            include_flags: true,
            include_weights: true,
            include_weight_spectrum: true,
            include_uvw: true,
            include_antenna_ids: true,
            include_data_desc_ids: true,
            include_field_ids: true,
            include_flag_row: true,
            include_time: false,
            include_interval: false,
            include_exposure: false,
            include_array_ids: false,
            include_observation_ids: false,
            include_scan_numbers: false,
            include_state_ids: false,
        }
    }

    /// Attach a homogeneous source partition invariant to this request.
    pub fn with_source_partition(mut self, source_partition: SourcePartition) -> Self {
        self.source_partition = Some(source_partition);
        self
    }
}

/// Contiguous source-channel range read from channelized visibility columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityChannelReadRange {
    /// First source channel to read.
    pub start: usize,
    /// Number of source channels to read.
    pub count: usize,
}

impl VisibilityChannelReadRange {
    /// Create a contiguous source-channel read range.
    pub fn new(start: usize, count: usize) -> Self {
        Self { start, count }
    }

    /// Create a read range spanning a full source-channel axis.
    pub fn full(channel_count: usize) -> Self {
        Self {
            start: 0,
            count: channel_count,
        }
    }

    /// Exclusive end of the read range.
    pub fn end_exclusive(self) -> usize {
        self.start.saturating_add(self.count)
    }

    /// Build a range when the provided channel indices are exactly contiguous.
    pub fn from_contiguous_indices(indices: &[usize]) -> Option<Self> {
        let &start = indices.first()?;
        indices
            .iter()
            .enumerate()
            .all(|(offset, &channel)| channel == start + offset)
            .then_some(Self {
                start,
                count: indices.len(),
            })
    }

    /// Build the smallest range covering the provided channel indices.
    pub fn covering_indices<I>(indices: I) -> Option<Self>
    where
        I: IntoIterator<Item = usize>,
    {
        let mut first_index = None::<usize>;
        let mut last_index = None::<usize>;
        for index in indices {
            first_index = Some(first_index.map_or(index, |current| current.min(index)));
            last_index = Some(last_index.map_or(index, |current| current.max(index)));
        }
        let start = first_index?;
        let end = last_index.expect("first_index implies last_index");
        Some(Self {
            start,
            count: end - start + 1,
        })
    }
}

/// Stable identity for a homogeneous MeasurementSet source partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourcePartitionId(pub usize);

/// Shape invariant for a homogeneous source partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityShape {
    /// Number of source channels for this partition.
    pub channel_count: usize,
    /// Number of correlations for this partition.
    pub corr_count: usize,
}

/// Homogeneous source partition read by a visibility buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePartition {
    /// Caller-owned partition identifier.
    pub id: SourcePartitionId,
    /// Source MeasurementSet identity within a larger imaging request.
    pub ms_id: usize,
    /// Main-table `DATA_DESC_ID`.
    pub data_desc_id: i32,
    /// Spectral-window id.
    pub spw_id: i32,
    /// Polarization id.
    pub polarization_id: i32,
    /// Full source shape for this homogeneous partition.
    pub shape: VisibilityShape,
}

impl SourcePartition {
    /// Create a source partition invariant.
    pub fn new(
        id: SourcePartitionId,
        ms_id: usize,
        data_desc_id: i32,
        spw_id: i32,
        polarization_id: i32,
        channel_count: usize,
        corr_count: usize,
    ) -> Self {
        Self {
            id,
            ms_id,
            data_desc_id,
            spw_id,
            polarization_id,
            shape: VisibilityShape {
                channel_count,
                corr_count,
            },
        }
    }
}

/// Physical read plan for one homogeneous visibility block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityReadBlockPlan {
    /// Homogeneous source partition represented by this block.
    pub source_partition: SourcePartition,
    /// Main-table row indices to read, in output order.
    pub row_indices: Vec<usize>,
    /// Source-channel range to read from channelized columns.
    pub channel_range: VisibilityChannelReadRange,
}

impl VisibilityReadBlockPlan {
    /// Create a block read plan from already-selected rows and channels.
    pub fn new(
        source_partition: SourcePartition,
        row_indices: Vec<usize>,
        channel_range: VisibilityChannelReadRange,
    ) -> Self {
        Self {
            source_partition,
            row_indices,
            channel_range,
        }
    }

    /// Convert this block plan into a visibility-buffer fill request.
    pub fn to_buffer_request(
        &self,
        data_column: VisibilityDataColumn,
        include_data: bool,
    ) -> VisibilityBufferRequest {
        let mut request = VisibilityBufferRequest::imaging(
            data_column,
            self.row_indices.clone(),
            self.channel_range.start,
            self.channel_range.count,
        )
        .with_source_partition(self.source_partition.clone());
        request.include_data = include_data;
        request
    }
}

/// Reusable columnar visibility buffer filled by [`MeasurementSet::fill_visibility_buffer`].
#[derive(Debug, Clone, Default)]
pub struct VisibilityBuffer {
    /// Row indices represented in this buffer.
    pub row_indices: Vec<usize>,
    /// First source channel represented in channelized sample arrays.
    pub channel_start: usize,
    /// Number of represented source channels.
    pub channel_count: usize,
    /// Number of correlations per row/channel sample.
    pub corr_count: usize,
    /// Homogeneous source partition represented by this buffer.
    pub source_partition: Option<SourcePartition>,
    /// Complex samples, laid out as `[channel][row][correlation]`.
    pub data: Option<VisibilityComplexSamples>,
    /// Flags, laid out as `[channel][row][correlation]`.
    pub flags: Option<Vec<bool>>,
    /// Per-row weights, laid out as `[row][correlation]`.
    pub weights: Option<VisibilityFloatSamples>,
    /// Per-channel weights, laid out as `[channel][row][correlation]`.
    pub weight_spectrum: Option<VisibilityFloatSamples>,
    /// UVW coordinates, laid out as `[row][uvw_axis]`.
    pub uvw: Option<Vec<f64>>,
    /// `ANTENNA1` values by row slot.
    pub antenna1: Option<Vec<i32>>,
    /// `ANTENNA2` values by row slot.
    pub antenna2: Option<Vec<i32>>,
    /// `DATA_DESC_ID` values by row slot.
    pub data_desc_ids: Option<Vec<i32>>,
    /// `FIELD_ID` values by row slot.
    pub field_ids: Option<Vec<i32>>,
    /// `FLAG_ROW` values by row slot.
    pub flag_row: Option<Vec<bool>>,
    /// `TIME` values by row slot.
    pub time: Option<Vec<f64>>,
    /// `INTERVAL` values by row slot.
    pub interval: Option<Vec<f64>>,
    /// `EXPOSURE` values by row slot.
    pub exposure: Option<Vec<f64>>,
    /// `ARRAY_ID` values by row slot.
    pub array_ids: Option<Vec<i32>>,
    /// `OBSERVATION_ID` values by row slot.
    pub observation_ids: Option<Vec<i32>>,
    /// `SCAN_NUMBER` values by row slot.
    pub scan_numbers: Option<Vec<i32>>,
    /// `STATE_ID` values by row slot.
    pub state_ids: Option<Vec<i32>>,
}

impl VisibilityBuffer {
    /// Number of selected rows currently represented.
    pub fn row_count(&self) -> usize {
        self.row_indices.len()
    }

    fn clear_for_request(&mut self, request: &VisibilityBufferRequest) {
        self.row_indices.clear();
        self.row_indices.extend_from_slice(&request.row_indices);
        self.channel_start = request.channel_start;
        self.channel_count = request.channel_count;
        self.corr_count = 0;
        self.source_partition = request.source_partition.clone();
        if !request.include_data {
            self.data = None;
        }
        if !request.include_flags {
            self.flags = None;
        }
        if !request.include_weights {
            self.weights = None;
        }
        if !request.include_weight_spectrum {
            self.weight_spectrum = None;
        }
        if !request.include_uvw {
            self.uvw = None;
        }
        if !request.include_antenna_ids {
            self.antenna1 = None;
            self.antenna2 = None;
        }
        if !request.include_data_desc_ids {
            self.data_desc_ids = None;
        }
        if !request.include_field_ids {
            self.field_ids = None;
        }
        if !request.include_flag_row {
            self.flag_row = None;
        }
        if !request.include_time {
            self.time = None;
        }
        if !request.include_interval {
            self.interval = None;
        }
        if !request.include_exposure {
            self.exposure = None;
        }
        if !request.include_array_ids {
            self.array_ids = None;
        }
        if !request.include_observation_ids {
            self.observation_ids = None;
        }
        if !request.include_scan_numbers {
            self.scan_numbers = None;
        }
        if !request.include_state_ids {
            self.state_ids = None;
        }
    }

    /// Source-channel range represented in channelized arrays.
    pub fn channel_range(&self) -> Range<usize> {
        self.channel_start..self.channel_start.saturating_add(self.channel_count)
    }

    /// Index into `[channel][row][correlation]` sample arrays.
    pub fn channel_row_corr_index(
        &self,
        channel_slot: usize,
        row_slot: usize,
        corr_slot: usize,
    ) -> usize {
        channel_row_corr_index(
            channel_slot,
            row_slot,
            corr_slot,
            self.row_count(),
            self.corr_count,
        )
    }
}

/// Typed complex sample storage for a visibility buffer.
#[derive(Debug, Clone)]
pub enum VisibilityComplexSamples {
    /// Native `Complex32` samples.
    Complex32(Vec<Complex32>),
    /// Native `Complex64` samples.
    Complex64(Vec<Complex64>),
}

impl VisibilityComplexSamples {
    /// Number of complex samples.
    pub fn len(&self) -> usize {
        match self {
            Self::Complex32(values) => values.len(),
            Self::Complex64(values) => values.len(),
        }
    }

    /// Returns `true` when no complex samples are stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Complex32(values) => values.capacity(),
            Self::Complex64(values) => values.capacity(),
        }
    }

    fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Complex32(_) => PrimitiveType::Complex32,
            Self::Complex64(_) => PrimitiveType::Complex64,
        }
    }
}

/// Typed real-valued sample storage for weights.
#[derive(Debug, Clone)]
pub enum VisibilityFloatSamples {
    /// Native `Float32` samples.
    Float32(Vec<f32>),
    /// Native `Float64` samples.
    Float64(Vec<f64>),
}

impl VisibilityFloatSamples {
    /// Number of real samples.
    pub fn len(&self) -> usize {
        match self {
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
        }
    }

    /// Returns `true` when no real samples are stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Float32(values) => values.capacity(),
            Self::Float64(values) => values.capacity(),
        }
    }

    fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Float32(_) => PrimitiveType::Float32,
            Self::Float64(_) => PrimitiveType::Float64,
        }
    }
}

/// Fill diagnostics for a visibility-buffer request.
#[derive(Debug, Clone, Serialize)]
pub struct VisibilityBufferFillReport {
    /// Number of rows requested and filled.
    pub row_count: usize,
    /// First source channel requested.
    pub channel_start: usize,
    /// Number of source channels requested.
    pub channel_count: usize,
    /// Number of correlations per row/channel sample.
    pub corr_count: usize,
    /// Data column name used for complex visibility samples.
    pub data_column: String,
    /// Column read and adaptation timings.
    pub timings: VisibilityBufferTimings,
    /// Logical bytes stored in the output buffer.
    pub logical_output_bytes: u64,
    /// Modeled bytes the storage path must read for this request.
    pub modeled_physical_read_bytes: u64,
    /// Bytes explicitly copied while arranging selected rows into the retained output layout.
    pub explicit_adaptation_copy_bytes: u64,
    /// Bytes copied outside the named arrangement paths instrumented by this fill.
    pub implicit_copy_bytes: u64,
    /// Per-column storage and byte-count facts.
    pub columns: Vec<VisibilityBufferColumnReport>,
    /// Allocation reuse details for the caller-owned buffer.
    pub allocation: VisibilityBufferAllocationReport,
}

/// Nanosecond timings for filling a visibility buffer.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct VisibilityBufferTimings {
    /// Total wall time for the fill call.
    pub total_ns: u128,
    /// Wall time spent reading and adapting the data column.
    pub data_ns: u128,
    /// Wall time spent reading and adapting `FLAG`.
    pub flags_ns: u128,
    /// Wall time spent reading and adapting `WEIGHT`.
    pub weights_ns: u128,
    /// Wall time spent reading and adapting `WEIGHT_SPECTRUM`.
    pub weight_spectrum_ns: u128,
    /// Wall time spent reading and adapting `UVW`.
    pub uvw_ns: u128,
    /// Wall time spent reading scalar metadata columns.
    pub scalar_ns: u128,
}

/// Storage and modeled-byte facts for one filled column.
#[derive(Debug, Clone, Serialize)]
pub struct VisibilityBufferColumnReport {
    /// Main-table column name.
    pub column: String,
    /// Primitive element type from the table schema.
    pub primitive_type: String,
    /// Whether the column is channelized for this read.
    pub channelized: bool,
    /// Channel granularity inferred from the bound storage manager.
    pub channel_read_granularity: VisibilityChannelReadGranularity,
    /// Element width in bytes.
    pub element_bytes: usize,
    /// Logical bytes written into the caller-owned buffer.
    pub logical_output_bytes: u64,
    /// Modeled physical bytes read from the table storage path.
    pub modeled_physical_read_bytes: u64,
    /// Data-manager types bound to this column.
    pub data_manager_types: Vec<String>,
}

/// Channel-read granularity exposed by the current table storage binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum VisibilityChannelReadGranularity {
    /// The requested channel range can be read without modeling a full-cell read.
    RequestedRange,
    /// The column read is modeled as a full-row-cell read.
    FullCell,
    /// The column is not a channelized 2-D array.
    NotChannelized,
}

/// Allocation-reuse details for one fill call.
#[derive(Debug, Clone, Default, Serialize)]
pub struct VisibilityBufferAllocationReport {
    /// Buffers that could be reused without exceeding existing capacity.
    pub reused_buffers: usize,
    /// Buffers that needed new capacity or a storage type change.
    pub grown_or_retyped_buffers: usize,
    /// Capacity before the fill, in logical elements by buffer name.
    pub capacity_before: BTreeMap<String, usize>,
    /// Capacity after the fill, in logical elements by buffer name.
    pub capacity_after: BTreeMap<String, usize>,
}

impl MeasurementSet {
    /// Fill caller-owned columnar visibility buffers for selected rows/channels.
    ///
    /// Channelized arrays (`DATA`, `FLAG`, and `WEIGHT_SPECTRUM`) use the
    /// table layer's typed selected-channel API. Complex visibility samples and
    /// per-channel flags/weights are laid out as `[channel][row][corr]` so
    /// plane-oriented imaging code can scan one output channel group without a
    /// per-row structure penalty.
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    pub fn fill_visibility_buffer(
        &self,
        request: &VisibilityBufferRequest,
        buffer: &mut VisibilityBuffer,
    ) -> MsResult<VisibilityBufferFillReport> {
        if request.channel_count == 0 {
            return Err(MsError::InvalidInput(
                "channel_count must be greater than zero".to_string(),
            ));
        }
        for &row_index in &request.row_indices {
            if row_index >= self.row_count() {
                return Err(MsError::InvalidIndex {
                    index: row_index,
                    max: self.row_count(),
                    context: "main-table row index".to_string(),
                });
            }
        }

        let total_started = Instant::now();
        let capacity_before = collect_capacities(buffer);
        let mut explicit_adaptation_copy_bytes =
            u64::try_from(request.row_indices.len().saturating_mul(size_of::<usize>()))
                .unwrap_or(u64::MAX);
        let mut timings = VisibilityBufferTimings::default();
        let mut columns = Vec::new();
        buffer.clear_for_request(request);

        let data_existing = buffer.data.take();
        let flags_existing = buffer.flags.take();
        let weight_spectrum_existing = buffer.weight_spectrum.take();
        let uvw_existing = buffer.uvw.take();
        let scalar_existing = ScalarColumnExistingBuffers {
            antenna1: buffer.antenna1.take(),
            antenna2: buffer.antenna2.take(),
            data_desc_ids: buffer.data_desc_ids.take(),
            field_ids: buffer.field_ids.take(),
            flag_row: buffer.flag_row.take(),
            time: buffer.time.take(),
            interval: buffer.interval.take(),
            exposure: buffer.exposure.take(),
            array_ids: buffer.array_ids.take(),
            observation_ids: buffer.observation_ids.take(),
            scan_numbers: buffer.scan_numbers.take(),
            state_ids: buffer.state_ids.take(),
        };
        let weights_existing = buffer.weights.take();
        let read_weight_spectrum = request.include_weight_spectrum
            && main_table_has_column(self.main_table(), "WEIGHT_SPECTRUM");
        if request.include_weight_spectrum && !read_weight_spectrum {
            buffer.weight_spectrum = None;
        }
        let read_scalars = request.include_antenna_ids
            || request.include_data_desc_ids
            || request.include_field_ids
            || request.include_flag_row
            || request.include_time
            || request.include_interval
            || request.include_exposure
            || request.include_array_ids
            || request.include_observation_ids
            || request.include_scan_numbers
            || request.include_state_ids;

        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let parallel_reads = thread::scope(|scope| {
            let data_handle = request.include_data.then(|| {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        let started = Instant::now();
                        read_complex_channel_column(
                            self,
                            request.data_column.name(),
                            &request.row_indices,
                            request.channel_start,
                            request.channel_count,
                            data_existing,
                        )
                        .map(|result| (result, started.elapsed()))
                    })
                })
            });
            let flags_handle = request.include_flags.then(|| {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        let started = Instant::now();
                        read_bool_channel_column(
                            self,
                            "FLAG",
                            &request.row_indices,
                            request.channel_start,
                            request.channel_count,
                            flags_existing,
                        )
                        .map(|result| (result, started.elapsed()))
                    })
                })
            });
            let weights_handle = request.include_weights.then(|| {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        let started = Instant::now();
                        read_float_row_column(
                            self,
                            "WEIGHT",
                            &request.row_indices,
                            weights_existing,
                        )
                        .map(|result| (result, started.elapsed()))
                    })
                })
            });
            let weight_spectrum_handle = read_weight_spectrum.then(|| {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        let started = Instant::now();
                        read_float_channel_column(
                            self,
                            "WEIGHT_SPECTRUM",
                            &request.row_indices,
                            request.channel_start,
                            request.channel_count,
                            weight_spectrum_existing,
                        )
                        .map(|result| (result, started.elapsed()))
                    })
                })
            });
            let uvw_handle = request.include_uvw.then(|| {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        let started = Instant::now();
                        read_uvw_column(self, &request.row_indices, uvw_existing)
                            .map(|result| (result, started.elapsed()))
                    })
                })
            });
            let scalar_handle = read_scalars.then(|| {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        let started = Instant::now();
                        read_scalar_columns(self, request, scalar_existing)
                            .map(|result| (result, started.elapsed()))
                    })
                })
            });
            Ok::<_, MsError>(ParallelVisibilityReads {
                data: join_visibility_buffer_worker(data_handle)?,
                flags: join_visibility_buffer_worker(flags_handle)?,
                weights: join_visibility_buffer_worker(weights_handle)?,
                weight_spectrum: join_visibility_buffer_worker(weight_spectrum_handle)?,
                uvw: join_visibility_buffer_worker(uvw_handle)?,
                scalars: join_visibility_buffer_worker(scalar_handle)?,
            })
        })?;

        let mut corr_count = 0usize;
        if let Some(((data, row_corr_count, report), elapsed)) = parallel_reads.data {
            timings.data_ns = elapsed_ns(elapsed);
            corr_count = merge_corr_count(corr_count, row_corr_count)?;
            buffer.data = Some(data);
            columns.push(report);
        }
        if let Some(((flags, row_corr_count, report), elapsed)) = parallel_reads.flags {
            timings.flags_ns = elapsed_ns(elapsed);
            corr_count = merge_corr_count(corr_count, row_corr_count)?;
            buffer.flags = Some(flags);
            columns.push(report);
        }
        if let Some(((weights, row_corr_count, report, copied_bytes), elapsed)) =
            parallel_reads.weights
        {
            timings.weights_ns = elapsed_ns(elapsed);
            corr_count = merge_corr_count(corr_count, row_corr_count)?;
            buffer.weights = Some(weights);
            explicit_adaptation_copy_bytes = explicit_adaptation_copy_bytes
                .checked_add(copied_bytes)
                .ok_or_else(|| {
                    invalid_input("visibility adaptation copy bytes overflow".to_string())
                })?;
            columns.push(report);
        }
        if let Some((weights, elapsed)) = parallel_reads.weight_spectrum {
            timings.weight_spectrum_ns = elapsed_ns(elapsed);
            if let Some((weights, row_corr_count, report, copied_bytes)) = weights {
                debug_assert_eq!(copied_bytes, 0);
                corr_count = merge_corr_count(corr_count, row_corr_count)?;
                buffer.weight_spectrum = Some(weights);
                columns.push(report);
            }
        }
        if let Some(((uvw, report), elapsed)) = parallel_reads.uvw {
            timings.uvw_ns = elapsed_ns(elapsed);
            explicit_adaptation_copy_bytes = explicit_adaptation_copy_bytes
                .checked_add(report.logical_output_bytes)
                .ok_or_else(|| {
                    invalid_input("visibility adaptation copy bytes overflow".to_string())
                })?;
            buffer.uvw = Some(uvw);
            columns.push(report);
        }
        if let Some((scalars, elapsed)) = parallel_reads.scalars {
            timings.scalar_ns = elapsed_ns(elapsed);
            explicit_adaptation_copy_bytes = scalars.reports.iter().try_fold(
                explicit_adaptation_copy_bytes,
                |total, report| {
                    if scalar_output_reuses_retained_buffer(
                        report.column.as_str(),
                        request.row_indices.len(),
                        &capacity_before,
                    ) {
                        total
                            .checked_add(report.logical_output_bytes)
                            .ok_or_else(|| {
                                invalid_input(
                                    "visibility adaptation copy bytes overflow".to_string(),
                                )
                            })
                    } else {
                        Ok(total)
                    }
                },
            )?;
            buffer.antenna1 = scalars.antenna1;
            buffer.antenna2 = scalars.antenna2;
            buffer.data_desc_ids = scalars.data_desc_ids;
            buffer.field_ids = scalars.field_ids;
            buffer.flag_row = scalars.flag_row;
            buffer.time = scalars.time;
            buffer.interval = scalars.interval;
            buffer.exposure = scalars.exposure;
            buffer.array_ids = scalars.array_ids;
            buffer.observation_ids = scalars.observation_ids;
            buffer.scan_numbers = scalars.scan_numbers;
            buffer.state_ids = scalars.state_ids;
            columns.extend(scalars.reports);
        }

        validate_source_partition(request, corr_count)?;
        buffer.corr_count = corr_count;
        let logical_output_bytes = columns
            .iter()
            .map(|column| column.logical_output_bytes)
            .sum::<u64>();
        let modeled_physical_read_bytes = columns
            .iter()
            .map(|column| column.modeled_physical_read_bytes)
            .sum::<u64>();
        let capacity_after = collect_capacities(buffer);
        let allocation = allocation_report(capacity_before, capacity_after);
        timings.total_ns = elapsed_ns(total_started.elapsed());

        Ok(VisibilityBufferFillReport {
            row_count: request.row_indices.len(),
            channel_start: request.channel_start,
            channel_count: request.channel_count,
            corr_count,
            data_column: request.data_column.name().to_string(),
            timings,
            logical_output_bytes,
            modeled_physical_read_bytes,
            explicit_adaptation_copy_bytes,
            implicit_copy_bytes: 0,
            columns,
            allocation,
        })
    }
}

fn ensure_row_shape(
    column_name: &str,
    row_slot: usize,
    shape: &[usize],
    corr_count: usize,
) -> MsResult<()> {
    if shape == [corr_count] {
        Ok(())
    } else {
        Err(invalid_input(format!(
            "{column_name} row {row_slot} shape {shape:?}; expected [{corr_count}]"
        )))
    }
}

fn channel_row_corr_index(
    channel_slot: usize,
    row_slot: usize,
    corr_slot: usize,
    row_count: usize,
    corr_count: usize,
) -> usize {
    (channel_slot * row_count + row_slot) * corr_count + corr_slot
}

fn merge_corr_count(current: usize, next: usize) -> MsResult<usize> {
    if current == 0 || current == next {
        Ok(next)
    } else {
        Err(invalid_input(format!(
            "correlation count mismatch: {current} versus {next}"
        )))
    }
}

fn invalid_input(message: String) -> MsError {
    MsError::InvalidInput(message)
}

fn column_type_error(column_name: &str, expected: &str, found: PrimitiveType) -> MsError {
    MsError::ColumnTypeMismatch {
        column: column_name.to_string(),
        table: "MAIN".to_string(),
        expected: expected.to_string(),
        found: format!("{found:?}"),
    }
}

#[cfg(test)]
mod tests;
