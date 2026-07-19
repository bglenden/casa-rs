// SPDX-License-Identifier: LGPL-3.0-or-later
//! Caller-owned columnar visibility buffers for efficient MeasurementSet scans.
//!
//! The buffer API is intended for streaming imaging and diagnostic readers
//! that want to reuse allocations across row blocks while reading only the
//! source channels needed by a schedule candidate.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::thread;
use std::time::{Duration, Instant};

use casa_imaging::{
    GeometryRoutePlan, GridderRoutePlan, ImagingSourceBlockView, ModelRoutePlan,
    PolarizationRoutePlan, SpectralRoutePlan, VisibilityBlockView, VisibilityComplexSamplesRef,
    VisibilityFloatSamplesRef, VisibilitySourcePartition, VisibilitySourcePartitionId,
    VisibilitySourceShape, WeightingRoutePlan,
};
use casa_tables::{RequiredScalarColumnValues, SelectedArray1DCells, SelectedArray2DCells, Table};
#[cfg(test)]
use casa_types::ScalarValue;
use casa_types::{ArrayValue, Complex32, Complex64, PrimitiveType};
use ndarray::Ix1;
use serde::Serialize;

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;
use crate::schema::main_table::VisibilityDataColumn;

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

    /// Convert this MeasurementSet partition into the imaging source contract.
    pub fn to_visibility_source_partition(&self) -> VisibilitySourcePartition {
        VisibilitySourcePartition {
            id: VisibilitySourcePartitionId(self.id.0),
            ms_id: self.ms_id,
            data_desc_id: self.data_desc_id,
            spectral_window_id: self.spw_id,
            polarization_id: self.polarization_id,
            shape: VisibilitySourceShape {
                channel_count: self.shape.channel_count,
                correlation_count: self.shape.corr_count,
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

    /// Borrow this buffer as a validated neutral imaging visibility block.
    pub fn as_visibility_block_view(&self) -> MsResult<VisibilityBlockView<'_>> {
        let partition = self.source_partition.as_ref().ok_or_else(|| {
            MsError::InvalidInput("visibility buffer requires a source partition".to_string())
        })?;
        let view = VisibilityBlockView {
            partition: partition.to_visibility_source_partition(),
            row_indices: &self.row_indices,
            channel_start: self.channel_start,
            channel_count: self.channel_count,
            data: self
                .data
                .as_ref()
                .map(VisibilityComplexSamples::as_visibility_ref),
            flags: self.flags.as_deref(),
            weights: self
                .weights
                .as_ref()
                .map(VisibilityFloatSamples::as_visibility_ref),
            weight_spectrum: self
                .weight_spectrum
                .as_ref()
                .map(VisibilityFloatSamples::as_visibility_ref),
            uvw_m: self.uvw.as_deref(),
            flag_row: self.flag_row.as_deref(),
            antenna1: self.antenna1.as_deref(),
            antenna2: self.antenna2.as_deref(),
            field_ids: self.field_ids.as_deref(),
            time: self.time.as_deref(),
        };
        view.validate()
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        Ok(view)
    }

    /// Borrow this buffer with imaging route plans attached.
    pub fn as_imaging_source_block_view<'a>(
        &'a self,
        spectral: &'a SpectralRoutePlan,
        polarization: &'a PolarizationRoutePlan,
        geometry: &'a GeometryRoutePlan,
        weighting: &'a WeightingRoutePlan,
        gridder: &'a GridderRoutePlan,
        model: Option<&'a ModelRoutePlan>,
    ) -> MsResult<ImagingSourceBlockView<'a>> {
        let view = ImagingSourceBlockView {
            source: self.as_visibility_block_view()?,
            spectral,
            polarization,
            geometry,
            weighting,
            gridder,
            model,
        };
        view.validate()
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        Ok(view)
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

    /// Borrow samples through the neutral imaging visibility-block contract.
    pub fn as_visibility_ref(&self) -> VisibilityComplexSamplesRef<'_> {
        match self {
            Self::Complex32(values) => VisibilityComplexSamplesRef::Complex32(values),
            Self::Complex64(values) => VisibilityComplexSamplesRef::Complex64(values),
        }
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

    /// Borrow samples through the neutral imaging visibility-block contract.
    pub fn as_visibility_ref(&self) -> VisibilityFloatSamplesRef<'_> {
        match self {
            Self::Float32(values) => VisibilityFloatSamplesRef::Float32(values),
            Self::Float64(values) => VisibilityFloatSamplesRef::Float64(values),
        }
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
        let mut timings = VisibilityBufferTimings::default();
        let mut columns = Vec::new();
        buffer.clear_for_request(request);

        if request.include_data {
            buffer.data = None;
        }
        if request.include_flags {
            buffer.flags = None;
        }
        if request.include_weight_spectrum {
            buffer.weight_spectrum = None;
        }
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

        let parallel_reads = thread::scope(|scope| {
            let data_handle = request.include_data.then(|| {
                scope.spawn(move || {
                    let started = Instant::now();
                    read_complex_channel_column(
                        self,
                        request.data_column.name(),
                        &request.row_indices,
                        request.channel_start,
                        request.channel_count,
                    )
                    .map(|result| (result, started.elapsed()))
                })
            });
            let flags_handle = request.include_flags.then(|| {
                scope.spawn(move || {
                    let started = Instant::now();
                    read_bool_channel_column(
                        self,
                        "FLAG",
                        &request.row_indices,
                        request.channel_start,
                        request.channel_count,
                    )
                    .map(|result| (result, started.elapsed()))
                })
            });
            let weights_handle = request.include_weights.then(|| {
                scope.spawn(move || {
                    let started = Instant::now();
                    read_float_row_column(self, "WEIGHT", &request.row_indices, weights_existing)
                        .map(|result| (result, started.elapsed()))
                })
            });
            let weight_spectrum_handle = read_weight_spectrum.then(|| {
                scope.spawn(move || {
                    let started = Instant::now();
                    read_float_channel_column(
                        self,
                        "WEIGHT_SPECTRUM",
                        &request.row_indices,
                        request.channel_start,
                        request.channel_count,
                    )
                    .map(|result| (result, started.elapsed()))
                })
            });
            let uvw_handle = request.include_uvw.then(|| {
                scope.spawn(move || {
                    let started = Instant::now();
                    read_uvw_column(self, &request.row_indices, uvw_existing)
                        .map(|result| (result, started.elapsed()))
                })
            });
            let scalar_handle = read_scalars.then(|| {
                scope.spawn(move || {
                    let started = Instant::now();
                    read_scalar_columns(self, request, scalar_existing)
                        .map(|result| (result, started.elapsed()))
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
        if let Some(((weights, row_corr_count, report), elapsed)) = parallel_reads.weights {
            timings.weights_ns = elapsed_ns(elapsed);
            corr_count = merge_corr_count(corr_count, row_corr_count)?;
            buffer.weights = Some(weights);
            columns.push(report);
        }
        if let Some((weights, elapsed)) = parallel_reads.weight_spectrum {
            timings.weight_spectrum_ns = elapsed_ns(elapsed);
            if let Some((weights, row_corr_count, report)) = weights {
                corr_count = merge_corr_count(corr_count, row_corr_count)?;
                buffer.weight_spectrum = Some(weights);
                columns.push(report);
            }
        }
        if let Some(((uvw, report), elapsed)) = parallel_reads.uvw {
            timings.uvw_ns = elapsed_ns(elapsed);
            buffer.uvw = Some(uvw);
            columns.push(report);
        }
        if let Some((scalars, elapsed)) = parallel_reads.scalars {
            timings.scalar_ns = elapsed_ns(elapsed);
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
            columns,
            allocation,
        })
    }
}

type TimedRead<T> = Option<(T, Duration)>;
type DataReadResult = (
    VisibilityComplexSamples,
    usize,
    VisibilityBufferColumnReport,
);
type BoolChannelReadResult = (Vec<bool>, usize, VisibilityBufferColumnReport);
type FloatReadResult = (VisibilityFloatSamples, usize, VisibilityBufferColumnReport);
type UvwReadResult = (Vec<f64>, VisibilityBufferColumnReport);

struct ParallelVisibilityReads {
    data: TimedRead<DataReadResult>,
    flags: TimedRead<BoolChannelReadResult>,
    weights: TimedRead<FloatReadResult>,
    weight_spectrum: TimedRead<Option<FloatReadResult>>,
    uvw: TimedRead<UvwReadResult>,
    scalars: TimedRead<ScalarColumnReadResult>,
}

struct ScalarColumnExistingBuffers {
    antenna1: Option<Vec<i32>>,
    antenna2: Option<Vec<i32>>,
    data_desc_ids: Option<Vec<i32>>,
    field_ids: Option<Vec<i32>>,
    flag_row: Option<Vec<bool>>,
    time: Option<Vec<f64>>,
    interval: Option<Vec<f64>>,
    exposure: Option<Vec<f64>>,
    array_ids: Option<Vec<i32>>,
    observation_ids: Option<Vec<i32>>,
    scan_numbers: Option<Vec<i32>>,
    state_ids: Option<Vec<i32>>,
}

struct ScalarColumnReadResult {
    antenna1: Option<Vec<i32>>,
    antenna2: Option<Vec<i32>>,
    data_desc_ids: Option<Vec<i32>>,
    field_ids: Option<Vec<i32>>,
    flag_row: Option<Vec<bool>>,
    time: Option<Vec<f64>>,
    interval: Option<Vec<f64>>,
    exposure: Option<Vec<f64>>,
    array_ids: Option<Vec<i32>>,
    observation_ids: Option<Vec<i32>>,
    scan_numbers: Option<Vec<i32>>,
    state_ids: Option<Vec<i32>>,
    reports: Vec<VisibilityBufferColumnReport>,
}

fn join_visibility_buffer_worker<T>(
    handle: Option<thread::ScopedJoinHandle<'_, MsResult<(T, Duration)>>>,
) -> MsResult<Option<(T, Duration)>> {
    handle
        .map(|handle| {
            handle
                .join()
                .map_err(|_| invalid_input("visibility buffer read worker panicked".to_string()))?
        })
        .transpose()
}

fn read_scalar_columns(
    ms: &MeasurementSet,
    request: &VisibilityBufferRequest,
    mut existing: ScalarColumnExistingBuffers,
) -> MsResult<ScalarColumnReadResult> {
    let mut result = ScalarColumnReadResult {
        antenna1: None,
        antenna2: None,
        data_desc_ids: None,
        field_ids: None,
        flag_row: None,
        time: None,
        interval: None,
        exposure: None,
        array_ids: None,
        observation_ids: None,
        scan_numbers: None,
        state_ids: None,
        reports: Vec::new(),
    };
    let mut column_names = Vec::with_capacity(12);
    if request.include_antenna_ids {
        column_names.extend(["ANTENNA1", "ANTENNA2"]);
    }
    if request.include_data_desc_ids {
        column_names.push("DATA_DESC_ID");
    }
    if request.include_field_ids {
        column_names.push("FIELD_ID");
    }
    if request.include_flag_row {
        column_names.push("FLAG_ROW");
    }
    if request.include_time {
        column_names.push("TIME");
    }
    if request.include_interval {
        column_names.push("INTERVAL");
    }
    if request.include_exposure {
        column_names.push("EXPOSURE");
    }
    if request.include_array_ids {
        column_names.push("ARRAY_ID");
    }
    if request.include_observation_ids {
        column_names.push("OBSERVATION_ID");
    }
    if request.include_scan_numbers {
        column_names.push("SCAN_NUMBER");
    }
    if request.include_state_ids {
        column_names.push("STATE_ID");
    }
    let mut columns = ms
        .main_table()
        .required_scalar_columns_owned_for_rows(&column_names, &request.row_indices)?;

    if request.include_antenna_ids {
        let (antenna1, report) = take_i32_scalar_column(
            ms,
            "ANTENNA1",
            &mut columns,
            request.row_indices.len(),
            existing.antenna1.take(),
        )?;
        let (antenna2, report2) = take_i32_scalar_column(
            ms,
            "ANTENNA2",
            &mut columns,
            request.row_indices.len(),
            existing.antenna2.take(),
        )?;
        result.antenna1 = Some(antenna1);
        result.antenna2 = Some(antenna2);
        result.reports.push(report);
        result.reports.push(report2);
    }
    if request.include_data_desc_ids {
        let (data_desc_ids, report) = take_i32_scalar_column(
            ms,
            "DATA_DESC_ID",
            &mut columns,
            request.row_indices.len(),
            existing.data_desc_ids.take(),
        )?;
        result.data_desc_ids = Some(data_desc_ids);
        result.reports.push(report);
    }
    if request.include_field_ids {
        let (field_ids, report) = take_i32_scalar_column(
            ms,
            "FIELD_ID",
            &mut columns,
            request.row_indices.len(),
            existing.field_ids.take(),
        )?;
        result.field_ids = Some(field_ids);
        result.reports.push(report);
    }
    if request.include_flag_row {
        let (flag_row, report) = take_bool_scalar_column(
            ms,
            "FLAG_ROW",
            &mut columns,
            request.row_indices.len(),
            existing.flag_row.take(),
        )?;
        result.flag_row = Some(flag_row);
        result.reports.push(report);
    }
    if request.include_time {
        let (time, report) = take_f64_scalar_column(
            ms,
            "TIME",
            &mut columns,
            request.row_indices.len(),
            existing.time.take(),
        )?;
        result.time = Some(time);
        result.reports.push(report);
    }
    if request.include_interval {
        let (interval, report) = take_f64_scalar_column(
            ms,
            "INTERVAL",
            &mut columns,
            request.row_indices.len(),
            existing.interval.take(),
        )?;
        result.interval = Some(interval);
        result.reports.push(report);
    }
    if request.include_exposure {
        let (exposure, report) = take_f64_scalar_column(
            ms,
            "EXPOSURE",
            &mut columns,
            request.row_indices.len(),
            existing.exposure.take(),
        )?;
        result.exposure = Some(exposure);
        result.reports.push(report);
    }
    if request.include_array_ids {
        let (array_ids, report) = take_i32_scalar_column(
            ms,
            "ARRAY_ID",
            &mut columns,
            request.row_indices.len(),
            existing.array_ids.take(),
        )?;
        result.array_ids = Some(array_ids);
        result.reports.push(report);
    }
    if request.include_observation_ids {
        let (observation_ids, report) = take_i32_scalar_column(
            ms,
            "OBSERVATION_ID",
            &mut columns,
            request.row_indices.len(),
            existing.observation_ids.take(),
        )?;
        result.observation_ids = Some(observation_ids);
        result.reports.push(report);
    }
    if request.include_scan_numbers {
        let (scan_numbers, report) = take_i32_scalar_column(
            ms,
            "SCAN_NUMBER",
            &mut columns,
            request.row_indices.len(),
            existing.scan_numbers.take(),
        )?;
        result.scan_numbers = Some(scan_numbers);
        result.reports.push(report);
    }
    if request.include_state_ids {
        let (state_ids, report) = take_i32_scalar_column(
            ms,
            "STATE_ID",
            &mut columns,
            request.row_indices.len(),
            existing.state_ids.take(),
        )?;
        result.state_ids = Some(state_ids);
        result.reports.push(report);
    }
    Ok(result)
}

fn validate_source_partition(request: &VisibilityBufferRequest, corr_count: usize) -> MsResult<()> {
    let Some(source_partition) = &request.source_partition else {
        return Ok(());
    };
    let requested_end = request
        .channel_start
        .checked_add(request.channel_count)
        .ok_or_else(|| invalid_input("visibility buffer channel range overflow".to_string()))?;
    if requested_end > source_partition.shape.channel_count {
        return Err(invalid_input(format!(
            "requested channel range {}..{} exceeds source partition channel count {}",
            request.channel_start, requested_end, source_partition.shape.channel_count
        )));
    }
    if corr_count != 0 && corr_count != source_partition.shape.corr_count {
        return Err(invalid_input(format!(
            "visibility buffer correlation count {corr_count} does not match source partition correlation count {}",
            source_partition.shape.corr_count
        )));
    }
    Ok(())
}

fn ensure_typed_channel_block_shape(
    column_name: &str,
    actual_rows: usize,
    axis0_count: usize,
    actual_channels: usize,
    expected_rows: usize,
    expected_channels: usize,
) -> MsResult<usize> {
    if actual_rows != expected_rows {
        return Err(invalid_input(format!(
            "{column_name} typed selected-row block has {actual_rows} rows, expected {expected_rows}"
        )));
    }
    if actual_channels != expected_channels {
        return Err(invalid_input(format!(
            "{column_name} typed selected-row block has {actual_channels} channels, expected {expected_channels}"
        )));
    }
    if axis0_count == 0 {
        return Err(invalid_input(format!(
            "{column_name} typed selected-row block has empty axis 0"
        )));
    }
    Ok(axis0_count)
}

struct TypedChannelBlock {
    primitive: PrimitiveType,
    corr_count: usize,
    cells: SelectedArray2DCells,
}

fn read_typed_channel_block(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    channel_start: usize,
    channel_count: usize,
) -> MsResult<Option<TypedChannelBlock>> {
    let cells = ms
        .main_table()
        .column_accessor(column_name)?
        .array_cells_2d_channel_range_typed_uncached(row_indices, channel_start, channel_count)?;
    let Some(cells) = cells else {
        return Ok(None);
    };
    let primitive = cells.primitive_type();
    let corr_count = ensure_typed_channel_block_shape(
        column_name,
        cells.row_count(),
        cells.axis0_count(),
        cells.channel_count(),
        row_indices.len(),
        channel_count,
    )?;
    Ok(Some(TypedChannelBlock {
        primitive,
        corr_count,
        cells,
    }))
}

fn channel_block_report(
    ms: &MeasurementSet,
    column_name: &str,
    primitive: PrimitiveType,
    channel_start: usize,
    channel_count: usize,
    corr_count: usize,
    row_count: usize,
) -> MsResult<VisibilityBufferColumnReport> {
    column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: true,
        channel_start,
        requested_channels: channel_count,
        elements_per_channel_or_row: corr_count,
        row_count,
    })
}

fn read_complex_channel_column(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    channel_start: usize,
    channel_count: usize,
) -> MsResult<(
    VisibilityComplexSamples,
    usize,
    VisibilityBufferColumnReport,
)> {
    let block =
        read_typed_channel_block(ms, column_name, row_indices, channel_start, channel_count)?
            .ok_or_else(|| {
                invalid_input(format!(
                    "required visibility column {column_name} is undefined for the selected rows"
                ))
            })?;
    let primitive = block.primitive;
    let corr_count = block.corr_count;
    let samples = match block.cells {
        SelectedArray2DCells::Complex32(values) => {
            VisibilityComplexSamples::Complex32(values.into_values())
        }
        SelectedArray2DCells::Complex64(values) => {
            VisibilityComplexSamples::Complex64(values.into_values())
        }
        other => {
            return Err(column_type_error(
                column_name,
                "Complex32 or Complex64 array",
                other.primitive_type(),
            ));
        }
    };
    let report = channel_block_report(
        ms,
        column_name,
        primitive,
        channel_start,
        channel_count,
        corr_count,
        row_indices.len(),
    )?;
    Ok((samples, corr_count, report))
}

fn read_bool_channel_column(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    channel_start: usize,
    channel_count: usize,
) -> MsResult<(Vec<bool>, usize, VisibilityBufferColumnReport)> {
    let block =
        read_typed_channel_block(ms, column_name, row_indices, channel_start, channel_count)?
            .ok_or_else(|| {
                invalid_input(format!(
                    "required visibility column {column_name} is undefined for the selected rows"
                ))
            })?;
    let primitive = block.primitive;
    let corr_count = block.corr_count;
    let SelectedArray2DCells::Bool(values) = block.cells else {
        return Err(column_type_error(column_name, "Bool array", primitive));
    };
    let out = values.into_values();
    let report = channel_block_report(
        ms,
        column_name,
        primitive,
        channel_start,
        channel_count,
        corr_count,
        row_indices.len(),
    )?;
    Ok((out, corr_count, report))
}

fn read_float_channel_column(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    channel_start: usize,
    channel_count: usize,
) -> MsResult<Option<(VisibilityFloatSamples, usize, VisibilityBufferColumnReport)>> {
    let Some(block) =
        read_typed_channel_block(ms, column_name, row_indices, channel_start, channel_count)?
    else {
        return Ok(None);
    };
    let primitive = block.primitive;
    let corr_count = block.corr_count;
    let samples = match block.cells {
        SelectedArray2DCells::Float32(values) => {
            VisibilityFloatSamples::Float32(values.into_values())
        }
        SelectedArray2DCells::Float64(values) => {
            VisibilityFloatSamples::Float64(values.into_values())
        }
        other => {
            return Err(column_type_error(
                column_name,
                "Float32 or Float64 array",
                other.primitive_type(),
            ));
        }
    };
    let report = channel_block_report(
        ms,
        column_name,
        primitive,
        channel_start,
        channel_count,
        corr_count,
        row_indices.len(),
    )?;
    Ok(Some((samples, corr_count, report)))
}

fn read_float_row_column(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    existing: Option<VisibilityFloatSamples>,
) -> MsResult<(VisibilityFloatSamples, usize, VisibilityBufferColumnReport)> {
    if let Ok(cells) = ms
        .main_table()
        .column_accessor(column_name)?
        .array_cells_1d_typed_uncached(row_indices)
    {
        let primitive = cells.primitive_type();
        let corr_count = cells.axis0_count();
        let samples = match cells {
            SelectedArray1DCells::Float32(values) => VisibilityFloatSamples::Float32(
                reuse_or_replace_f32(existing, values.into_values()),
            ),
            SelectedArray1DCells::Float64(values) => VisibilityFloatSamples::Float64(
                reuse_or_replace_f64(existing, values.into_values()),
            ),
            other => {
                return Err(column_type_error(
                    column_name,
                    "Float32 or Float64 array",
                    other.primitive_type(),
                ));
            }
        };
        let report = column_report(ColumnReportInput {
            table: ms.main_table(),
            column_name,
            primitive,
            channelized: false,
            channel_start: 0,
            requested_channels: 1,
            elements_per_channel_or_row: corr_count,
            row_count: row_indices.len(),
        })?;
        return Ok((samples, corr_count, report));
    }

    let values = ms
        .main_table()
        .column_accessor(column_name)?
        .array_cells_owned_uncached(row_indices)?;
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    let corr_count = first_1d_count(&values, column_name)?;
    let sample_count = row_indices.len().saturating_mul(corr_count);
    let samples = match primitive {
        PrimitiveType::Float32 => {
            let mut out = match existing {
                Some(VisibilityFloatSamples::Float32(mut values)) => {
                    values.clear();
                    values.reserve(sample_count.saturating_sub(values.capacity()));
                    values
                }
                _ => Vec::with_capacity(sample_count),
            };
            out.resize(sample_count, 0.0);
            for (row_slot, value) in values.into_iter().enumerate() {
                let row = require_array(value, column_name, row_slot)?;
                let ArrayValue::Float32(array) = row else {
                    return Err(column_type_error(
                        column_name,
                        "Float32 array",
                        row.primitive_type(),
                    ));
                };
                let array = array.into_dimensionality::<Ix1>().map_err(|error| {
                    invalid_input(format!("{column_name} row {row_slot} rank: {error}"))
                })?;
                ensure_row_shape(column_name, row_slot, array.shape(), corr_count)?;
                for corr in 0..corr_count {
                    out[row_slot * corr_count + corr] = array[corr];
                }
            }
            VisibilityFloatSamples::Float32(out)
        }
        PrimitiveType::Float64 => {
            let mut out = match existing {
                Some(VisibilityFloatSamples::Float64(mut values)) => {
                    values.clear();
                    values.reserve(sample_count.saturating_sub(values.capacity()));
                    values
                }
                _ => Vec::with_capacity(sample_count),
            };
            out.resize(sample_count, 0.0);
            for (row_slot, value) in values.into_iter().enumerate() {
                let row = require_array(value, column_name, row_slot)?;
                let ArrayValue::Float64(array) = row else {
                    return Err(column_type_error(
                        column_name,
                        "Float64 array",
                        row.primitive_type(),
                    ));
                };
                let array = array.into_dimensionality::<Ix1>().map_err(|error| {
                    invalid_input(format!("{column_name} row {row_slot} rank: {error}"))
                })?;
                ensure_row_shape(column_name, row_slot, array.shape(), corr_count)?;
                for corr in 0..corr_count {
                    out[row_slot * corr_count + corr] = array[corr];
                }
            }
            VisibilityFloatSamples::Float64(out)
        }
        other => {
            return Err(column_type_error(
                column_name,
                "Float32 or Float64 array",
                other,
            ));
        }
    };
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: corr_count,
        row_count: row_indices.len(),
    })?;
    Ok((samples, corr_count, report))
}

fn reuse_or_replace_f32(
    existing: Option<VisibilityFloatSamples>,
    replacement: Vec<f32>,
) -> Vec<f32> {
    if let Some(VisibilityFloatSamples::Float32(mut values)) = existing {
        values.clear();
        values.extend_from_slice(&replacement);
        values
    } else {
        replacement
    }
}

fn reuse_or_replace_f64(
    existing: Option<VisibilityFloatSamples>,
    replacement: Vec<f64>,
) -> Vec<f64> {
    if let Some(VisibilityFloatSamples::Float64(mut values)) = existing {
        values.clear();
        values.extend_from_slice(&replacement);
        values
    } else {
        replacement
    }
}

fn read_uvw_column(
    ms: &MeasurementSet,
    row_indices: &[usize],
    existing: Option<Vec<f64>>,
) -> MsResult<(Vec<f64>, VisibilityBufferColumnReport)> {
    if let Ok(cells) = ms
        .main_table()
        .column_accessor("UVW")?
        .array_cells_1d_typed_uncached(row_indices)
    {
        let primitive = cells.primitive_type();
        if primitive != PrimitiveType::Float64 {
            return Err(column_type_error("UVW", "Float64 array", primitive));
        }
        let SelectedArray1DCells::Float64(values) = cells else {
            return Err(column_type_error("UVW", "Float64 array", primitive));
        };
        if values.axis0_count() != 3 {
            return Err(invalid_input(format!(
                "UVW typed selected 1-D rows have axis length {}, expected 3",
                values.axis0_count()
            )));
        }
        let replacement = values.into_values();
        let mut out = existing.unwrap_or_else(|| Vec::with_capacity(replacement.len()));
        out.clear();
        out.extend_from_slice(&replacement);
        let report = column_report(ColumnReportInput {
            table: ms.main_table(),
            column_name: "UVW",
            primitive,
            channelized: false,
            channel_start: 0,
            requested_channels: 1,
            elements_per_channel_or_row: 3,
            row_count: row_indices.len(),
        })?;
        return Ok((out, report));
    }

    let values = ms
        .main_table()
        .column_accessor("UVW")?
        .array_cells_owned_uncached(row_indices)?;
    let primitive = main_column_primitive_type(ms.main_table(), "UVW")?;
    if primitive != PrimitiveType::Float64 {
        return Err(column_type_error("UVW", "Float64 array", primitive));
    }
    let sample_count = row_indices.len().saturating_mul(3);
    let mut out = existing.unwrap_or_else(|| Vec::with_capacity(sample_count));
    out.clear();
    out.reserve(sample_count.saturating_sub(out.capacity()));
    out.resize(sample_count, 0.0);
    for (row_slot, value) in values.into_iter().enumerate() {
        let row = require_array(value, "UVW", row_slot)?;
        let ArrayValue::Float64(array) = row else {
            return Err(column_type_error(
                "UVW",
                "Float64 array",
                row.primitive_type(),
            ));
        };
        let array = array
            .into_dimensionality::<Ix1>()
            .map_err(|error| invalid_input(format!("UVW row {row_slot} rank: {error}")))?;
        ensure_row_shape("UVW", row_slot, array.shape(), 3)?;
        for axis in 0..3 {
            out[row_slot * 3 + axis] = array[axis];
        }
    }
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name: "UVW",
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: 3,
        row_count: row_indices.len(),
    })?;
    Ok((out, report))
}

fn take_i32_scalar_column(
    ms: &MeasurementSet,
    column_name: &str,
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    row_count: usize,
    existing: Option<Vec<i32>>,
) -> MsResult<(Vec<i32>, VisibilityBufferColumnReport)> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    if primitive != PrimitiveType::Int32 {
        return Err(column_type_error(column_name, "Int32 scalar", primitive));
    }
    let Some(RequiredScalarColumnValues::Int32(values)) = columns.remove(column_name) else {
        return Err(invalid_input(format!(
            "required Int32 scalar column {column_name} was not loaded"
        )));
    };
    let out = reuse_or_replace_vec(existing, values);
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: 1,
        row_count,
    })?;
    Ok((out, report))
}

fn take_f64_scalar_column(
    ms: &MeasurementSet,
    column_name: &str,
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    row_count: usize,
    existing: Option<Vec<f64>>,
) -> MsResult<(Vec<f64>, VisibilityBufferColumnReport)> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    if primitive != PrimitiveType::Float64 {
        return Err(column_type_error(column_name, "Float64 scalar", primitive));
    }
    let Some(RequiredScalarColumnValues::Float64(values)) = columns.remove(column_name) else {
        return Err(invalid_input(format!(
            "required Float64 scalar column {column_name} was not loaded"
        )));
    };
    let out = reuse_or_replace_vec(existing, values);
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: 1,
        row_count,
    })?;
    Ok((out, report))
}

fn take_bool_scalar_column(
    ms: &MeasurementSet,
    column_name: &str,
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    row_count: usize,
    existing: Option<Vec<bool>>,
) -> MsResult<(Vec<bool>, VisibilityBufferColumnReport)> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    if primitive != PrimitiveType::Bool {
        return Err(column_type_error(column_name, "Bool scalar", primitive));
    }
    let Some(RequiredScalarColumnValues::Bool(values)) = columns.remove(column_name) else {
        return Err(invalid_input(format!(
            "required Bool scalar column {column_name} was not loaded"
        )));
    };
    let out = reuse_or_replace_vec(existing, values);
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: 1,
        row_count,
    })?;
    Ok((out, report))
}

fn reuse_or_replace_vec<T: Clone>(existing: Option<Vec<T>>, replacement: Vec<T>) -> Vec<T> {
    let Some(mut existing) = existing.filter(|values| values.capacity() >= replacement.len())
    else {
        return replacement;
    };
    existing.clear();
    existing.extend_from_slice(&replacement);
    existing
}

struct ColumnReportInput<'a> {
    table: &'a Table,
    column_name: &'a str,
    primitive: PrimitiveType,
    channelized: bool,
    channel_start: usize,
    requested_channels: usize,
    elements_per_channel_or_row: usize,
    row_count: usize,
}

fn column_report(input: ColumnReportInput<'_>) -> MsResult<VisibilityBufferColumnReport> {
    let ColumnReportInput {
        table,
        column_name,
        primitive,
        channelized,
        channel_start,
        requested_channels,
        elements_per_channel_or_row,
        row_count,
    } = input;
    let element_bytes = primitive.fixed_width_bytes().ok_or_else(|| {
        MsError::InvalidInput(format!(
            "{column_name} has variable-width type {primitive:?}"
        ))
    })?;
    let data_manager_types = data_manager_types_for_column(table, column_name);
    let granularity = if channelized {
        channel_granularity_for_column(table, column_name)
    } else {
        VisibilityChannelReadGranularity::NotChannelized
    };
    let logical_elements = row_count
        .saturating_mul(requested_channels)
        .saturating_mul(elements_per_channel_or_row);
    let logical_output_bytes = logical_elements.saturating_mul(element_bytes) as u64;
    let physical_channels = match granularity {
        VisibilityChannelReadGranularity::RequestedRange => modeled_tile_aligned_channel_count(
            table,
            column_name,
            channel_start,
            requested_channels,
        )
        .unwrap_or(requested_channels),
        VisibilityChannelReadGranularity::FullCell if channelized => {
            modeled_full_cell_channel_count(table, column_name).unwrap_or(requested_channels)
        }
        VisibilityChannelReadGranularity::FullCell
        | VisibilityChannelReadGranularity::NotChannelized => requested_channels,
    };
    let physical_bytes = row_count
        .saturating_mul(physical_channels)
        .saturating_mul(elements_per_channel_or_row)
        .saturating_mul(element_bytes) as u64;
    Ok(VisibilityBufferColumnReport {
        column: column_name.to_string(),
        primitive_type: format!("{primitive:?}"),
        channelized,
        channel_read_granularity: granularity,
        element_bytes,
        logical_output_bytes,
        modeled_physical_read_bytes: physical_bytes,
        data_manager_types,
    })
}

fn main_column_primitive_type(table: &Table, column_name: &str) -> MsResult<PrimitiveType> {
    let schema = table
        .schema()
        .ok_or_else(|| MsError::InvalidInput("main table has no schema".to_string()))?;
    schema
        .column(column_name)
        .ok_or_else(|| MsError::MissingColumn {
            column: column_name.to_string(),
            table: "MAIN".to_string(),
        })?
        .data_type()
        .ok_or_else(|| {
            MsError::InvalidInput(format!(
                "main table {column_name} column has no primitive data type"
            ))
        })
}

fn main_table_has_column(table: &Table, column_name: &str) -> bool {
    table
        .schema()
        .is_some_and(|schema| schema.contains_column(column_name))
}

fn channel_granularity_for_column(
    table: &Table,
    column_name: &str,
) -> VisibilityChannelReadGranularity {
    let uses_tiled_shape_stman = table.data_manager_info().iter().any(|manager| {
        manager.dm_type == "TiledShapeStMan"
            && manager.columns.iter().any(|name| name == column_name)
    });
    if uses_tiled_shape_stman {
        VisibilityChannelReadGranularity::RequestedRange
    } else {
        VisibilityChannelReadGranularity::FullCell
    }
}

fn data_manager_types_for_column(table: &Table, column_name: &str) -> Vec<String> {
    table
        .data_manager_info()
        .iter()
        .filter(|manager| manager.columns.iter().any(|name| name == column_name))
        .map(|manager| manager.dm_type.clone())
        .collect()
}

fn modeled_full_cell_channel_count(_table: &Table, _column_name: &str) -> Option<usize> {
    None
}

fn modeled_tile_aligned_channel_count(
    table: &Table,
    column_name: &str,
    channel_start: usize,
    requested_channels: usize,
) -> Option<usize> {
    let tile_width = table
        .array_column_2d_channel_tile_width(column_name)
        .ok()
        .flatten()
        .filter(|width| *width > 0)?;
    let requested_end = channel_start.checked_add(requested_channels)?;
    let physical_start = (channel_start / tile_width) * tile_width;
    let physical_end = requested_end.div_ceil(tile_width) * tile_width;
    Some(
        physical_end
            .saturating_sub(physical_start)
            .max(requested_channels),
    )
}

fn first_1d_count(values: &[Option<ArrayValue>], column_name: &str) -> MsResult<usize> {
    for (row_slot, value) in values.iter().enumerate() {
        let Some(value) = value else {
            continue;
        };
        if value.ndim() != 1 {
            return Err(invalid_input(format!(
                "{column_name} row {row_slot} must be rank-1, found rank {}",
                value.ndim()
            )));
        }
        return Ok(value.shape()[0]);
    }
    Err(invalid_input(format!(
        "{column_name} has no defined selected rows"
    )))
}

fn require_array(
    value: Option<ArrayValue>,
    column_name: &str,
    row_slot: usize,
) -> MsResult<ArrayValue> {
    value.ok_or_else(|| {
        invalid_input(format!(
            "{column_name} missing for selected row slot {row_slot}"
        ))
    })
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

fn collect_capacities(buffer: &VisibilityBuffer) -> BTreeMap<String, usize> {
    let mut capacities = BTreeMap::new();
    capacities.insert("row_indices".to_string(), buffer.row_indices.capacity());
    if let Some(samples) = &buffer.data {
        capacities.insert("data".to_string(), samples.capacity());
        capacities.insert(
            "data_type".to_string(),
            primitive_type_capacity_marker(samples.primitive_type()),
        );
    }
    if let Some(values) = &buffer.flags {
        capacities.insert("flags".to_string(), values.capacity());
    }
    if let Some(samples) = &buffer.weights {
        capacities.insert("weights".to_string(), samples.capacity());
        capacities.insert(
            "weights_type".to_string(),
            primitive_type_capacity_marker(samples.primitive_type()),
        );
    }
    if let Some(samples) = &buffer.weight_spectrum {
        capacities.insert("weight_spectrum".to_string(), samples.capacity());
        capacities.insert(
            "weight_spectrum_type".to_string(),
            primitive_type_capacity_marker(samples.primitive_type()),
        );
    }
    if let Some(values) = &buffer.uvw {
        capacities.insert("uvw".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.antenna1 {
        capacities.insert("antenna1".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.antenna2 {
        capacities.insert("antenna2".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.data_desc_ids {
        capacities.insert("data_desc_ids".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.field_ids {
        capacities.insert("field_ids".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.flag_row {
        capacities.insert("flag_row".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.time {
        capacities.insert("time".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.interval {
        capacities.insert("interval".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.exposure {
        capacities.insert("exposure".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.array_ids {
        capacities.insert("array_ids".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.observation_ids {
        capacities.insert("observation_ids".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.scan_numbers {
        capacities.insert("scan_numbers".to_string(), values.capacity());
    }
    if let Some(values) = &buffer.state_ids {
        capacities.insert("state_ids".to_string(), values.capacity());
    }
    capacities
}

fn allocation_report(
    capacity_before: BTreeMap<String, usize>,
    capacity_after: BTreeMap<String, usize>,
) -> VisibilityBufferAllocationReport {
    let mut reused_buffers = 0usize;
    let mut grown_or_retyped_buffers = 0usize;
    for (name, after) in &capacity_after {
        if name.ends_with("_type") {
            continue;
        }
        let before = capacity_before.get(name).copied().unwrap_or(0);
        if before >= *after && before > 0 {
            reused_buffers += 1;
        } else {
            grown_or_retyped_buffers += 1;
        }
    }
    VisibilityBufferAllocationReport {
        reused_buffers,
        grown_or_retyped_buffers,
        capacity_before,
        capacity_after,
    }
}

fn primitive_type_capacity_marker(primitive: PrimitiveType) -> usize {
    match primitive {
        PrimitiveType::Complex32 => 32,
        PrimitiveType::Complex64 => 64,
        PrimitiveType::Float32 => 132,
        PrimitiveType::Float64 => 164,
        _ => 0,
    }
}

fn elapsed_ns(duration: Duration) -> u128 {
    duration.as_nanos()
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
mod tests {
    use casa_tables::Table;
    use casa_types::{RecordField, RecordValue, Value};
    use ndarray::ArrayD;

    use super::*;
    use crate::builder::MeasurementSetBuilder;
    use crate::schema::main_table::OptionalMainColumn;
    use crate::test_helpers::default_value;

    #[test]
    fn visibility_channel_read_range_tracks_contiguous_and_covering_ranges() {
        assert_eq!(
            VisibilityChannelReadRange::from_contiguous_indices(&[2, 3, 4]),
            Some(VisibilityChannelReadRange::new(2, 3))
        );
        assert_eq!(
            VisibilityChannelReadRange::from_contiguous_indices(&[2, 4]),
            None
        );
        assert_eq!(
            VisibilityChannelReadRange::covering_indices([4, 2, 7]),
            Some(VisibilityChannelReadRange::new(2, 6))
        );
        assert_eq!(VisibilityChannelReadRange::full(5).end_exclusive(), 5);
    }

    #[test]
    fn visibility_read_block_plan_builds_buffer_request() {
        let source_partition = SourcePartition::new(SourcePartitionId(3), 1, 7, 8, 9, 16, 4);
        let plan = VisibilityReadBlockPlan::new(
            source_partition.clone(),
            vec![11, 10],
            VisibilityChannelReadRange::new(4, 6),
        );

        let request = plan.to_buffer_request(VisibilityDataColumn::CorrectedData, false);

        assert_eq!(request.source_partition, Some(source_partition));
        assert_eq!(request.data_column, VisibilityDataColumn::CorrectedData);
        assert_eq!(request.row_indices, vec![11, 10]);
        assert_eq!(request.channel_start, 4);
        assert_eq!(request.channel_count, 6);
        assert!(!request.include_data);
        assert!(request.include_flags);
        assert!(request.include_weights);
        assert!(request.include_weight_spectrum);
        assert!(request.include_uvw);
    }

    #[test]
    fn fill_visibility_buffer_reads_selected_channels_columnar() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ms_path = dir.path().join("visibility-buffer.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new()
                .with_main_column(OptionalMainColumn::Data)
                .with_main_column(OptionalMainColumn::WeightSpectrum),
        )
        .unwrap();
        add_visibility_test_row(ms.main_table_mut(), 0);
        add_visibility_test_row(ms.main_table_mut(), 1);
        ms.save().expect("save visibility-buffer test MS");
        let ms = MeasurementSet::open(&ms_path).expect("reopen visibility-buffer test MS");

        let mut request =
            VisibilityBufferRequest::imaging(VisibilityDataColumn::Data, vec![1, 0], 1, 2)
                .with_source_partition(SourcePartition::new(
                    SourcePartitionId(0),
                    0,
                    0,
                    0,
                    0,
                    4,
                    2,
                ));
        request.include_time = true;
        request.include_interval = true;
        request.include_exposure = true;
        request.include_array_ids = true;
        request.include_observation_ids = true;
        request.include_scan_numbers = true;
        request.include_state_ids = true;
        request.include_weight_spectrum = false;
        let mut buffer = VisibilityBuffer::default();
        let report = ms.fill_visibility_buffer(&request, &mut buffer).unwrap();

        assert_eq!(buffer.row_indices, vec![1, 0]);
        assert_eq!(buffer.channel_start, 1);
        assert_eq!(buffer.channel_range(), 1..3);
        assert_eq!(buffer.channel_count, 2);
        assert_eq!(buffer.corr_count, 2);
        assert_eq!(
            buffer
                .source_partition
                .as_ref()
                .map(|partition| partition.id),
            Some(SourcePartitionId(0))
        );
        assert_eq!(report.row_count, 2);
        assert_eq!(report.channel_count, 2);
        assert!(report.logical_output_bytes > 0);
        assert!(report.modeled_physical_read_bytes >= report.logical_output_bytes);

        let Some(VisibilityComplexSamples::Complex32(data)) = &buffer.data else {
            panic!("expected Complex32 data");
        };
        assert_eq!(data.len(), 8);
        assert_eq!(
            data[sample_index(0, 0, 0, 2, 2)],
            Complex32::new(110.0, -110.0)
        );
        assert_eq!(
            data[sample_index(0, 1, 0, 2, 2)],
            Complex32::new(10.0, -10.0)
        );
        assert_eq!(
            data[sample_index(1, 0, 1, 2, 2)],
            Complex32::new(121.0, -121.0)
        );
        assert_eq!(
            data[sample_index(1, 1, 1, 2, 2)],
            Complex32::new(21.0, -21.0)
        );

        let flags = buffer.flags.as_ref().expect("flags");
        assert!(flags[sample_index(0, 0, 0, 2, 2)]);
        assert!(!flags[sample_index(0, 1, 0, 2, 2)]);

        let Some(VisibilityFloatSamples::Float32(weights)) = &buffer.weights else {
            panic!("expected Float32 weights");
        };
        assert_eq!(weights, &[11.0, 12.0, 1.0, 2.0]);

        let uvw = buffer.uvw.as_ref().expect("uvw");
        assert_eq!(&uvw[0..3], &[101.0, 102.0, 103.0]);
        assert_eq!(&uvw[3..6], &[1.0, 2.0, 3.0]);
        assert_eq!(buffer.antenna1.as_deref(), Some(&[11, 1][..]));
        assert_eq!(buffer.antenna2.as_deref(), Some(&[12, 2][..]));
        assert_eq!(buffer.data_desc_ids.as_deref(), Some(&[13, 3][..]));
        assert_eq!(buffer.field_ids.as_deref(), Some(&[14, 4][..]));
        assert_eq!(buffer.flag_row.as_deref(), Some(&[false, true][..]));
        assert_eq!(buffer.time.as_deref(), Some(&[1001.0, 1.0][..]));
        assert_eq!(buffer.interval.as_deref(), Some(&[1010.0, 10.0][..]));
        assert_eq!(buffer.exposure.as_deref(), Some(&[1020.0, 20.0][..]));
        assert_eq!(buffer.array_ids.as_deref(), Some(&[15, 5][..]));
        assert_eq!(buffer.observation_ids.as_deref(), Some(&[16, 6][..]));
        assert_eq!(buffer.scan_numbers.as_deref(), Some(&[17, 7][..]));
        assert_eq!(buffer.state_ids.as_deref(), Some(&[18, 8][..]));

        let view = buffer.as_visibility_block_view().unwrap();
        assert_eq!(view.partition.id, VisibilitySourcePartitionId(0));
        assert_eq!(view.partition.shape.channel_count, 4);
        assert_eq!(view.partition.shape.correlation_count, 2);
        assert_eq!(view.row_indices, &[1, 0]);
        assert_eq!(view.channel_range(), 1..3);
        let Some(VisibilityComplexSamplesRef::Complex32(view_data)) = view.data else {
            panic!("expected Complex32 view data");
        };
        assert_eq!(view_data.len(), data.len());
        assert_eq!(
            view_data[view.channel_row_corr_index(1, 1, 1)],
            Complex32::new(21.0, -21.0)
        );
        let Some(VisibilityFloatSamplesRef::Float32(view_weights)) = view.weights else {
            panic!("expected Float32 view weights");
        };
        assert_eq!(view_weights, &[11.0, 12.0, 1.0, 2.0]);
        let spectral = SpectralRoutePlan::identity_for_block(view);
        let polarization = PolarizationRoutePlan {
            output_stokes: casa_imaging::PlaneStokes::I,
        };
        let geometry = GeometryRoutePlan {
            geometry: casa_imaging::ImageGeometry {
                image_shape: [64, 64],
                cell_size_rad: [1.0e-6, 1.0e-6],
            },
        };
        let weighting = WeightingRoutePlan {
            weighting: casa_imaging::WeightingMode::Natural,
        };
        let gridder = GridderRoutePlan {
            gridder_mode: casa_imaging::GridderMode::Standard,
        };
        let imaging_view = buffer
            .as_imaging_source_block_view(
                &spectral,
                &polarization,
                &geometry,
                &weighting,
                &gridder,
                None,
            )
            .unwrap();
        assert_eq!(imaging_view.spectral.channel_route_count(), 2);
        assert_eq!(imaging_view.source.channel_range(), 1..3);

        let second_report = ms.fill_visibility_buffer(&request, &mut buffer).unwrap();
        assert!(second_report.allocation.reused_buffers > 0);
    }

    #[test]
    fn fill_visibility_buffer_rejects_source_partition_shape_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ms_path = dir.path().join("visibility-buffer-shape-mismatch.ms");
        let mut ms = MeasurementSet::create(
            &ms_path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_visibility_test_row(ms.main_table_mut(), 0);
        ms.save().expect("save visibility-buffer mismatch test MS");
        let ms = MeasurementSet::open(&ms_path).expect("reopen visibility-buffer mismatch test MS");

        let request = VisibilityBufferRequest::imaging(VisibilityDataColumn::Data, vec![0], 1, 2)
            .with_source_partition(SourcePartition::new(SourcePartitionId(0), 0, 0, 0, 0, 2, 2));
        let mut buffer = VisibilityBuffer::default();
        let error = ms
            .fill_visibility_buffer(&request, &mut buffer)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exceeds source partition channel count"),
            "{error}"
        );
    }

    fn add_visibility_test_row(table: &mut Table, row_id: i32) {
        let fields = table
            .schema()
            .unwrap()
            .columns()
            .iter()
            .map(|column| {
                let value = match column.name() {
                    "DATA" => Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(vec![2, 4], complex_row(row_id)).unwrap(),
                    )),
                    "FLAG" => Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(vec![2, 4], flag_row(row_id)).unwrap(),
                    )),
                    "WEIGHT" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![2],
                            vec![row_id as f32 * 10.0 + 1.0, row_id as f32 * 10.0 + 2.0],
                        )
                        .unwrap(),
                    )),
                    "WEIGHT_SPECTRUM" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2, 4], weight_spectrum_row(row_id)).unwrap(),
                    )),
                    "UVW" => Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            vec![3],
                            vec![
                                row_id as f64 * 100.0 + 1.0,
                                row_id as f64 * 100.0 + 2.0,
                                row_id as f64 * 100.0 + 3.0,
                            ],
                        )
                        .unwrap(),
                    )),
                    "ANTENNA1" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 1)),
                    "ANTENNA2" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 2)),
                    "DATA_DESC_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 3)),
                    "FIELD_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 4)),
                    "FLAG_ROW" => Value::Scalar(ScalarValue::Bool(row_id == 0)),
                    "TIME" => Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 1.0)),
                    "INTERVAL" => {
                        Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 10.0))
                    }
                    "EXPOSURE" => {
                        Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 20.0))
                    }
                    "ARRAY_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 5)),
                    "OBSERVATION_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 6)),
                    "SCAN_NUMBER" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 7)),
                    "STATE_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 8)),
                    _ => default_value(column.name()),
                };
                RecordField::new(column.name(), value)
            })
            .collect::<Vec<_>>();
        table.add_row(RecordValue::new(fields)).unwrap();
    }

    fn complex_row(row_id: i32) -> Vec<Complex32> {
        let mut values = Vec::new();
        for corr in 0..2 {
            for chan in 0..4 {
                let value = row_id as f32 * 100.0 + chan as f32 * 10.0 + corr as f32;
                values.push(Complex32::new(value, -value));
            }
        }
        values
    }

    fn flag_row(row_id: i32) -> Vec<bool> {
        let mut values = Vec::new();
        for corr in 0..2 {
            for chan in 0..4 {
                values.push((row_id + chan + corr) % 2 == 0);
            }
        }
        values
    }

    fn weight_spectrum_row(row_id: i32) -> Vec<f32> {
        let mut values = Vec::new();
        for corr in 0..2 {
            for chan in 0..4 {
                values.push(row_id as f32 * 100.0 + chan as f32 * 10.0 + corr as f32 + 0.5);
            }
        }
        values
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
}
