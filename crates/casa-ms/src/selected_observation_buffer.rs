// SPDX-License-Identifier: LGPL-3.0-or-later

use std::time::Instant;

use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValues, RequiredScalarColumnValuesMut,
    SelectedArray1DCellsMut, SelectedArray2DCellsMut,
};

use crate::{
    MeasurementSet, MsError, MsResult, VisibilityChannelReadRange,
    schema::main_table::VisibilityDataColumn,
};

const SELECTED_SCALAR_COLUMN_COUNT: u64 = 15;
const SELECTED_ARRAY_COLUMN_COUNT: u64 = 4;
const SELECTED_READ_OPERATION_COUNT: u64 =
    SELECTED_SCALAR_COLUMN_COUNT + SELECTED_ARRAY_COLUMN_COUNT;

/// Exact MeasurementSet visibility column read for selected-observation input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectedVisibilityColumn {
    /// MAIN `DATA` complex visibility values.
    Data,
    /// MAIN `CORRECTED_DATA` complex visibility values.
    CorrectedData,
    /// MAIN `FLOAT_DATA` real visibility values.
    FloatData,
}

impl SelectedVisibilityColumn {
    const fn name(self) -> &'static str {
        match self {
            Self::Data => VisibilityDataColumn::Data.name(),
            Self::CorrectedData => VisibilityDataColumn::CorrectedData.name(),
            Self::FloatData => "FLOAT_DATA",
        }
    }
}

/// Exact MeasurementSet input-weight column read for selected-observation input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectedWeightColumn {
    /// MAIN per-row/per-correlation `WEIGHT`, broadcast over channels.
    Weight,
    /// MAIN per-channel `WEIGHT_SPECTRUM` with no existence fallback.
    WeightSpectrum,
}

/// One closed bounded selected-observation storage read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SelectedObservationBufferRequest<'a> {
    visibility: SelectedVisibilityColumn,
    weight: SelectedWeightColumn,
    row_indices: &'a [usize],
    channel_range: VisibilityChannelReadRange,
}

impl<'a> SelectedObservationBufferRequest<'a> {
    /// Construct one exact contiguous-channel storage request.
    #[must_use]
    pub(crate) const fn new(
        visibility: SelectedVisibilityColumn,
        weight: SelectedWeightColumn,
        row_indices: &'a [usize],
        channel_range: VisibilityChannelReadRange,
    ) -> Self {
        Self {
            visibility,
            weight,
            row_indices,
            channel_range,
        }
    }
}

/// Visibility value preserved in its standard MeasurementSet storage precision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SelectedStoredVisibility {
    /// Single-precision real `FLOAT_DATA` value.
    Float32(f32),
    /// Single-precision complex `DATA` or `CORRECTED_DATA` value.
    Complex32([f32; 2]),
}

#[derive(Debug)]
enum SelectedStoredVisibilities {
    Float32(Vec<f32>),
    Complex32(Vec<casa_types::Complex32>),
}

#[derive(Debug)]
enum SelectedStoredWeights {
    PerRow(Vec<f32>),
    PerChannel(Vec<f32>),
}

/// Caller-owned bounded storage block for exact selected-observation values.
#[derive(Debug)]
pub(crate) struct SelectedObservationBuffer {
    row_indices: Vec<usize>,
    channel_range: VisibilityChannelReadRange,
    correlation_count: usize,
    visibility: Option<SelectedStoredVisibilities>,
    flags: Vec<bool>,
    weights: Option<SelectedStoredWeights>,
    row_flag: Vec<bool>,
    uvw_m: Vec<f64>,
    data_description_ids: Vec<i32>,
    field_ids: Vec<i32>,
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    feed1: Vec<i32>,
    feed2: Vec<i32>,
    time_mjd_seconds: Vec<f64>,
    time_centroid_mjd_seconds: Vec<f64>,
    interval_seconds: Vec<f64>,
    exposure_seconds: Vec<f64>,
    scan_numbers: Vec<i32>,
    state_ids: Vec<i32>,
    observation_ids: Vec<i32>,
    array_ids: Vec<i32>,
}

/// Heap residency of one selected-observation buffer and its fill operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SelectedObservationBufferResidency {
    /// Bytes retained after a successful fill.
    pub(crate) resident_bytes: usize,
    /// Peak bytes while the request and all fill temporaries are simultaneously live.
    pub(crate) fill_peak_bytes: usize,
}

/// Factual diagnostics for one closed selected-observation buffer fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SelectedObservationBufferFillReport {
    pub(crate) block_count: u64,
    pub(crate) row_count: u64,
    pub(crate) sample_count: u64,
    pub(crate) logical_output_bytes: u64,
    pub(crate) modeled_physical_read_bytes: Option<u64>,
    pub(crate) read_operation_count: u64,
    pub(crate) request_handoff_bytes: u64,
    pub(crate) retained_current_bytes: u64,
    pub(crate) retained_capacity_bytes: u64,
    pub(crate) allocation: SelectedObservationBufferAllocationReport,
    pub(crate) timings: SelectedObservationBufferTimings,
}

/// Allocation outcome for the caller-owned selected-observation buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SelectedObservationBufferAllocationReport {
    pub(crate) reused_storage_buffers: u64,
    pub(crate) allocated_storage_buffers: u64,
}

#[derive(Clone, Copy)]
struct AllocationProbe {
    pointer: usize,
    capacity: usize,
}

impl AllocationProbe {
    fn capture<T>(values: &Vec<T>) -> Self {
        Self {
            pointer: values.as_ptr() as usize,
            capacity: values.capacity(),
        }
    }

    fn reused<T>(self, values: &[T]) -> bool {
        self.capacity > 0
            && self.capacity >= values.len()
            && self.pointer == values.as_ptr() as usize
    }
}

#[derive(Default)]
struct AllocationCounter {
    reused: u64,
    allocated: u64,
}

impl AllocationCounter {
    fn observe<T>(&mut self, probe: AllocationProbe, values: &[T]) {
        if probe.reused(values) {
            self.reused += 1;
        } else if !values.is_empty() {
            self.allocated += 1;
        }
    }

    const fn report(self) -> SelectedObservationBufferAllocationReport {
        SelectedObservationBufferAllocationReport {
            reused_storage_buffers: self.reused,
            allocated_storage_buffers: self.allocated,
        }
    }
}

/// Nanosecond timings for the sequential selected-observation fill stages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SelectedObservationBufferTimings {
    pub(crate) total_fill_nanos: u128,
    pub(crate) visibility_read_nanos: u128,
    pub(crate) flag_read_nanos: u128,
    pub(crate) weight_read_nanos: u128,
    pub(crate) scalar_read_nanos: u128,
    pub(crate) uvw_read_nanos: u128,
    pub(crate) assembly_nanos: u128,
}

impl SelectedObservationBufferTimings {
    pub(crate) const fn storage_read_nanos(self) -> u128 {
        self.visibility_read_nanos
            + self.flag_read_nanos
            + self.weight_read_nanos
            + self.scalar_read_nanos
            + self.uvw_read_nanos
    }
}

/// Project the allocations performed by [`MeasurementSet::fill_selected_observation_buffer`].
pub(crate) fn selected_observation_buffer_residency(
    rows: usize,
    packed_samples: usize,
    weight_values: usize,
    visibility_bytes: usize,
) -> Option<SelectedObservationBufferResidency> {
    let row_indices = rows.checked_mul(size_of::<usize>())?;
    let visibility = packed_samples.checked_mul(visibility_bytes)?;
    let flags = packed_samples.checked_mul(size_of::<bool>())?;
    let weights = weight_values.checked_mul(size_of::<f32>())?;
    let scalar_payload =
        rows.checked_mul(10 * size_of::<i32>() + 4 * size_of::<f64>() + size_of::<bool>())?;
    let uvw = rows.checked_mul(size_of::<[f64; 3]>())?;
    let resident_bytes = row_indices
        .checked_add(visibility)?
        .checked_add(flags)?
        .checked_add(weights)?
        .checked_add(scalar_payload)?
        .checked_add(uvw)?;

    // The typed scalar batch owns fifteen named vectors until they move into
    // the destination buffer. Hashbrown keeps at most 7/8 load, so derive the
    // allocated bucket count from the actual column count rather than hiding a
    // fixture-specific allowance.
    const SCALAR_COLUMNS: [&str; 15] = [
        "DATA_DESC_ID",
        "FIELD_ID",
        "ANTENNA1",
        "ANTENNA2",
        "FEED1",
        "FEED2",
        "TIME",
        "TIME_CENTROID",
        "INTERVAL",
        "EXPOSURE",
        "SCAN_NUMBER",
        "STATE_ID",
        "OBSERVATION_ID",
        "ARRAY_ID",
        "FLAG_ROW",
    ];
    let minimum_buckets = SCALAR_COLUMNS
        .len()
        .checked_mul(8)?
        .checked_add(6)?
        .checked_div(7)?;
    let scalar_buckets = minimum_buckets.checked_next_power_of_two()?;
    let scalar_bucket_bytes = scalar_buckets.checked_mul(
        size_of::<String>()
            .checked_add(size_of::<RequiredScalarColumnValues>())?
            .checked_add(1)?,
    )?;
    let scalar_map_bytes = scalar_bucket_bytes
        .checked_add(SCALAR_COLUMNS.iter().map(|name| name.len()).sum::<usize>())?;
    let fill_peak_bytes = resident_bytes
        .checked_add(row_indices)?
        .checked_add(scalar_map_bytes)?;
    Some(SelectedObservationBufferResidency {
        resident_bytes,
        fill_peak_bytes,
    })
}

impl Default for SelectedObservationBuffer {
    fn default() -> Self {
        Self {
            row_indices: Vec::new(),
            channel_range: VisibilityChannelReadRange::new(0, 0),
            correlation_count: 0,
            visibility: None,
            flags: Vec::new(),
            weights: None,
            row_flag: Vec::new(),
            uvw_m: Vec::new(),
            data_description_ids: Vec::new(),
            field_ids: Vec::new(),
            antenna1: Vec::new(),
            antenna2: Vec::new(),
            feed1: Vec::new(),
            feed2: Vec::new(),
            time_mjd_seconds: Vec::new(),
            time_centroid_mjd_seconds: Vec::new(),
            interval_seconds: Vec::new(),
            exposure_seconds: Vec::new(),
            scan_numbers: Vec::new(),
            state_ids: Vec::new(),
            observation_ids: Vec::new(),
            array_ids: Vec::new(),
        }
    }
}

impl SelectedObservationBuffer {
    /// Number of selected MAIN rows in this block.
    #[must_use]
    pub(crate) fn row_count(&self) -> usize {
        self.row_indices.len()
    }

    pub(crate) fn retained_current_bytes(&self) -> Option<usize> {
        self.retained_bytes(false)
    }

    pub(crate) fn retained_capacity_bytes(&self) -> Option<usize> {
        self.retained_bytes(true)
    }

    fn retained_bytes(&self, capacity: bool) -> Option<usize> {
        let count = |len: usize, cap: usize| if capacity { cap } else { len };
        let mut bytes = 0_usize;
        macro_rules! add_vec {
            ($values:expr, $type:ty) => {{
                let values = $values;
                bytes = bytes.checked_add(
                    count(values.len(), values.capacity()).checked_mul(size_of::<$type>())?,
                )?;
            }};
        }
        add_vec!(&self.row_indices, usize);
        match self.visibility.as_ref() {
            Some(SelectedStoredVisibilities::Float32(values)) => add_vec!(values, f32),
            Some(SelectedStoredVisibilities::Complex32(values)) => {
                add_vec!(values, casa_types::Complex32)
            }
            None => {}
        }
        add_vec!(&self.flags, bool);
        match self.weights.as_ref() {
            Some(SelectedStoredWeights::PerRow(values))
            | Some(SelectedStoredWeights::PerChannel(values)) => add_vec!(values, f32),
            None => {}
        }
        add_vec!(&self.row_flag, bool);
        add_vec!(&self.uvw_m, f64);
        add_vec!(&self.data_description_ids, i32);
        add_vec!(&self.field_ids, i32);
        add_vec!(&self.antenna1, i32);
        add_vec!(&self.antenna2, i32);
        add_vec!(&self.feed1, i32);
        add_vec!(&self.feed2, i32);
        add_vec!(&self.time_mjd_seconds, f64);
        add_vec!(&self.time_centroid_mjd_seconds, f64);
        add_vec!(&self.interval_seconds, f64);
        add_vec!(&self.exposure_seconds, f64);
        add_vec!(&self.scan_numbers, i32);
        add_vec!(&self.state_ids, i32);
        add_vec!(&self.observation_ids, i32);
        add_vec!(&self.array_ids, i32);
        Some(bytes)
    }

    /// Return one stored sample by channel, row, and correlation block offsets.
    #[must_use]
    pub(crate) fn sample(
        &self,
        channel_offset: usize,
        row_offset: usize,
        correlation_offset: usize,
    ) -> Option<SelectedStoredSample> {
        if channel_offset >= self.channel_range.count
            || row_offset >= self.row_count()
            || correlation_offset >= self.correlation_count
        {
            return None;
        }
        let sample_index = packed_sample_index(
            channel_offset,
            row_offset,
            correlation_offset,
            self.row_count(),
            self.correlation_count,
        );
        let row_weight_index = row_offset * self.correlation_count + correlation_offset;
        let visibility = match self.visibility.as_ref()? {
            SelectedStoredVisibilities::Float32(values) => {
                SelectedStoredVisibility::Float32(*values.get(sample_index)?)
            }
            SelectedStoredVisibilities::Complex32(values) => {
                let value = values.get(sample_index)?;
                SelectedStoredVisibility::Complex32([value.re, value.im])
            }
        };
        let input_weight = match self.weights.as_ref()? {
            SelectedStoredWeights::PerRow(values) => *values.get(row_weight_index)?,
            SelectedStoredWeights::PerChannel(values) => *values.get(sample_index)?,
        };
        let uvw_start = row_offset.checked_mul(3)?;
        let uvw = self.uvw_m.get(uvw_start..uvw_start + 3)?;
        Some(SelectedStoredSample {
            physical_row: *self.row_indices.get(row_offset)?,
            data_description_id: *self.data_description_ids.get(row_offset)?,
            visibility,
            channel_flag: *self.flags.get(sample_index)?,
            row_flag: *self.row_flag.get(row_offset)?,
            input_weight,
            uvw_m: [uvw[0], uvw[1], uvw[2]],
            time_mjd_seconds: *self.time_mjd_seconds.get(row_offset)?,
            time_centroid_mjd_seconds: *self.time_centroid_mjd_seconds.get(row_offset)?,
            interval_seconds: *self.interval_seconds.get(row_offset)?,
            exposure_seconds: *self.exposure_seconds.get(row_offset)?,
            field_id: *self.field_ids.get(row_offset)?,
            antenna1: *self.antenna1.get(row_offset)?,
            antenna2: *self.antenna2.get(row_offset)?,
            feed1: *self.feed1.get(row_offset)?,
            feed2: *self.feed2.get(row_offset)?,
            scan_number: *self.scan_numbers.get(row_offset)?,
            state_id: *self.state_ids.get(row_offset)?,
            observation_id: *self.observation_ids.get(row_offset)?,
            array_id: *self.array_ids.get(row_offset)?,
        })
    }
}

/// One stored selected sample and its exact MAIN provenance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SelectedStoredSample {
    physical_row: usize,
    data_description_id: i32,
    visibility: SelectedStoredVisibility,
    channel_flag: bool,
    row_flag: bool,
    input_weight: f32,
    uvw_m: [f64; 3],
    time_mjd_seconds: f64,
    time_centroid_mjd_seconds: f64,
    interval_seconds: f64,
    exposure_seconds: f64,
    field_id: i32,
    antenna1: i32,
    antenna2: i32,
    feed1: i32,
    feed2: i32,
    scan_number: i32,
    state_id: i32,
    observation_id: i32,
    array_id: i32,
}

macro_rules! selected_sample_getter {
    ($name:ident, $field:ident, $type:ty, $doc:literal) => {
        #[doc = $doc]
        #[must_use]
        pub(crate) const fn $name(self) -> $type {
            self.$field
        }
    };
}

impl SelectedStoredSample {
    selected_sample_getter!(
        physical_row,
        physical_row,
        usize,
        "Return the physical MAIN row."
    );
    selected_sample_getter!(
        data_description_id,
        data_description_id,
        i32,
        "Return `DATA_DESC_ID`."
    );
    selected_sample_getter!(
        visibility,
        visibility,
        SelectedStoredVisibility,
        "Return the stored visibility."
    );
    selected_sample_getter!(
        channel_flag,
        channel_flag,
        bool,
        "Return the selected `FLAG` value."
    );
    selected_sample_getter!(row_flag, row_flag, bool, "Return `FLAG_ROW`.");
    selected_sample_getter!(
        input_weight,
        input_weight,
        f32,
        "Return the exact selected input weight."
    );
    selected_sample_getter!(uvw_m, uvw_m, [f64; 3], "Return raw MAIN UVW metres.");
    selected_sample_getter!(
        time_mjd_seconds,
        time_mjd_seconds,
        f64,
        "Return MAIN `TIME` in MJD seconds."
    );
    selected_sample_getter!(
        time_centroid_mjd_seconds,
        time_centroid_mjd_seconds,
        f64,
        "Return MAIN `TIME_CENTROID` in MJD seconds."
    );
    selected_sample_getter!(
        interval_seconds,
        interval_seconds,
        f64,
        "Return MAIN `INTERVAL` seconds."
    );
    selected_sample_getter!(
        exposure_seconds,
        exposure_seconds,
        f64,
        "Return MAIN `EXPOSURE` seconds."
    );
    selected_sample_getter!(field_id, field_id, i32, "Return `FIELD_ID`.");
    selected_sample_getter!(antenna1, antenna1, i32, "Return `ANTENNA1`.");
    selected_sample_getter!(antenna2, antenna2, i32, "Return `ANTENNA2`.");
    selected_sample_getter!(feed1, feed1, i32, "Return `FEED1`.");
    selected_sample_getter!(feed2, feed2, i32, "Return `FEED2`.");
    selected_sample_getter!(scan_number, scan_number, i32, "Return `SCAN_NUMBER`.");
    selected_sample_getter!(state_id, state_id, i32, "Return `STATE_ID`.");
    selected_sample_getter!(
        observation_id,
        observation_id,
        i32,
        "Return `OBSERVATION_ID`."
    );
    selected_sample_getter!(array_id, array_id, i32, "Return `ARRAY_ID`.");
}

impl MeasurementSet {
    /// Fill one bounded block with the closed selected-observation storage column set.
    ///
    /// This selected-channel operation requires a lazily reopened disk-backed MeasurementSet with
    /// no pending array-cell writes. It never falls back to cloning complete array cells.
    pub(crate) fn fill_selected_observation_buffer(
        &self,
        request: &SelectedObservationBufferRequest<'_>,
        buffer: &mut SelectedObservationBuffer,
    ) -> MsResult<SelectedObservationBufferFillReport> {
        let fill_started = Instant::now();
        validate_request(self, request)?;
        let mut allocation = AllocationCounter::default();
        buffer.row_indices.clear();
        buffer.row_indices.extend_from_slice(request.row_indices);
        buffer.channel_range = request.channel_range;

        let visibility_started = Instant::now();
        let visibility_shape = match request.visibility {
            SelectedVisibilityColumn::FloatData => {
                if !matches!(
                    buffer.visibility,
                    Some(SelectedStoredVisibilities::Float32(_))
                ) {
                    buffer.visibility = Some(SelectedStoredVisibilities::Float32(Vec::new()));
                }
                let Some(SelectedStoredVisibilities::Float32(values)) = buffer.visibility.as_mut()
                else {
                    unreachable!("FLOAT_DATA destination installed above")
                };
                let probe = AllocationProbe::capture(values);
                let shape = self
                    .main_table()
                    .column_accessor(request.visibility.name())?
                    .fill_array_cells_2d_channel_range_typed_uncached(
                        request.row_indices,
                        request.channel_range.start,
                        request.channel_range.count,
                        SelectedArray2DCellsMut::Float32(values),
                    )?
                    .ok_or_else(|| {
                        invalid(format!(
                            "required MAIN {} cells are undefined",
                            request.visibility.name()
                        ))
                    })?;
                allocation.observe(probe, values);
                shape
            }
            SelectedVisibilityColumn::Data | SelectedVisibilityColumn::CorrectedData => {
                if !matches!(
                    buffer.visibility,
                    Some(SelectedStoredVisibilities::Complex32(_))
                ) {
                    buffer.visibility = Some(SelectedStoredVisibilities::Complex32(Vec::new()));
                }
                let Some(SelectedStoredVisibilities::Complex32(values)) =
                    buffer.visibility.as_mut()
                else {
                    unreachable!("complex visibility destination installed above")
                };
                let probe = AllocationProbe::capture(values);
                let shape = self
                    .main_table()
                    .column_accessor(request.visibility.name())?
                    .fill_array_cells_2d_channel_range_typed_uncached(
                        request.row_indices,
                        request.channel_range.start,
                        request.channel_range.count,
                        SelectedArray2DCellsMut::Complex32(values),
                    )?
                    .ok_or_else(|| {
                        invalid(format!(
                            "required MAIN {} cells are undefined",
                            request.visibility.name()
                        ))
                    })?;
                allocation.observe(probe, values);
                shape
            }
        };
        let visibility_read_nanos = visibility_started.elapsed().as_nanos();
        require_shape(
            request.visibility.name(),
            visibility_shape.row_count,
            visibility_shape.channel_count,
            visibility_shape.axis0_count,
            request,
            visibility_shape.axis0_count,
        )?;
        let correlation_count = visibility_shape.axis0_count;
        buffer.correlation_count = correlation_count;

        let flag_started = Instant::now();
        let flag_probe = AllocationProbe::capture(&buffer.flags);
        let flag_shape = self
            .main_table()
            .column_accessor("FLAG")?
            .fill_array_cells_2d_channel_range_typed_uncached(
                request.row_indices,
                request.channel_range.start,
                request.channel_range.count,
                SelectedArray2DCellsMut::Bool(&mut buffer.flags),
            )?
            .ok_or_else(|| invalid("required MAIN FLAG cells are undefined"))?;
        allocation.observe(flag_probe, &buffer.flags);
        require_shape(
            "FLAG",
            flag_shape.row_count,
            flag_shape.channel_count,
            flag_shape.axis0_count,
            request,
            correlation_count,
        )?;
        let flag_read_nanos = flag_started.elapsed().as_nanos();

        let weight_started = Instant::now();
        let weight_shape = match request.weight {
            SelectedWeightColumn::Weight => {
                if !matches!(buffer.weights, Some(SelectedStoredWeights::PerRow(_))) {
                    buffer.weights = Some(SelectedStoredWeights::PerRow(Vec::new()));
                }
                let Some(SelectedStoredWeights::PerRow(values)) = buffer.weights.as_mut() else {
                    unreachable!("per-row weight destination installed above")
                };
                let probe = AllocationProbe::capture(values);
                let shape = self
                    .main_table()
                    .column_accessor("WEIGHT")?
                    .fill_array_cells_1d_typed_uncached(
                        request.row_indices,
                        SelectedArray1DCellsMut::Float32(values),
                    )?;
                allocation.observe(probe, values);
                if shape.row_count != request.row_indices.len()
                    || shape.axis0_count != correlation_count
                {
                    return Err(invalid(
                        "MAIN WEIGHT shape differs from selected visibility shape",
                    ));
                }
                None
            }
            SelectedWeightColumn::WeightSpectrum => {
                if !matches!(buffer.weights, Some(SelectedStoredWeights::PerChannel(_))) {
                    buffer.weights = Some(SelectedStoredWeights::PerChannel(Vec::new()));
                }
                let Some(SelectedStoredWeights::PerChannel(values)) = buffer.weights.as_mut()
                else {
                    unreachable!("per-channel weight destination installed above")
                };
                let probe = AllocationProbe::capture(values);
                let shape = self
                    .main_table()
                    .column_accessor("WEIGHT_SPECTRUM")?
                    .fill_array_cells_2d_channel_range_typed_uncached(
                        request.row_indices,
                        request.channel_range.start,
                        request.channel_range.count,
                        SelectedArray2DCellsMut::Float32(values),
                    )?
                    .ok_or_else(|| invalid("required MAIN WEIGHT_SPECTRUM cells are undefined"))?;
                allocation.observe(probe, values);
                Some(shape)
            }
        };
        if let Some(shape) = weight_shape {
            require_shape(
                "WEIGHT_SPECTRUM",
                shape.row_count,
                shape.channel_count,
                shape.axis0_count,
                request,
                correlation_count,
            )?;
        }
        let weight_read_nanos = weight_started.elapsed().as_nanos();

        let scalar_started = Instant::now();
        let scalar_probes = [
            AllocationProbe::capture(&buffer.data_description_ids),
            AllocationProbe::capture(&buffer.field_ids),
            AllocationProbe::capture(&buffer.antenna1),
            AllocationProbe::capture(&buffer.antenna2),
            AllocationProbe::capture(&buffer.feed1),
            AllocationProbe::capture(&buffer.feed2),
            AllocationProbe::capture(&buffer.time_mjd_seconds),
            AllocationProbe::capture(&buffer.time_centroid_mjd_seconds),
            AllocationProbe::capture(&buffer.interval_seconds),
            AllocationProbe::capture(&buffer.exposure_seconds),
            AllocationProbe::capture(&buffer.scan_numbers),
            AllocationProbe::capture(&buffer.state_ids),
            AllocationProbe::capture(&buffer.observation_ids),
            AllocationProbe::capture(&buffer.array_ids),
            AllocationProbe::capture(&buffer.row_flag),
        ];
        {
            let mut scalar_destinations = [
                RequiredScalarColumnDestination::new(
                    "DATA_DESC_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.data_description_ids),
                ),
                RequiredScalarColumnDestination::new(
                    "FIELD_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.field_ids),
                ),
                RequiredScalarColumnDestination::new(
                    "ANTENNA1",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.antenna1),
                ),
                RequiredScalarColumnDestination::new(
                    "ANTENNA2",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.antenna2),
                ),
                RequiredScalarColumnDestination::new(
                    "FEED1",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.feed1),
                ),
                RequiredScalarColumnDestination::new(
                    "FEED2",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.feed2),
                ),
                RequiredScalarColumnDestination::new(
                    "TIME",
                    RequiredScalarColumnValuesMut::Float64(&mut buffer.time_mjd_seconds),
                ),
                RequiredScalarColumnDestination::new(
                    "TIME_CENTROID",
                    RequiredScalarColumnValuesMut::Float64(&mut buffer.time_centroid_mjd_seconds),
                ),
                RequiredScalarColumnDestination::new(
                    "INTERVAL",
                    RequiredScalarColumnValuesMut::Float64(&mut buffer.interval_seconds),
                ),
                RequiredScalarColumnDestination::new(
                    "EXPOSURE",
                    RequiredScalarColumnValuesMut::Float64(&mut buffer.exposure_seconds),
                ),
                RequiredScalarColumnDestination::new(
                    "SCAN_NUMBER",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.scan_numbers),
                ),
                RequiredScalarColumnDestination::new(
                    "STATE_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.state_ids),
                ),
                RequiredScalarColumnDestination::new(
                    "OBSERVATION_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.observation_ids),
                ),
                RequiredScalarColumnDestination::new(
                    "ARRAY_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut buffer.array_ids),
                ),
                RequiredScalarColumnDestination::new(
                    "FLAG_ROW",
                    RequiredScalarColumnValuesMut::Bool(&mut buffer.row_flag),
                ),
            ];
            self.main_table().required_scalar_columns_for_rows_into(
                request.row_indices,
                &mut scalar_destinations,
            )?;
        }
        allocation.observe(scalar_probes[0], &buffer.data_description_ids);
        allocation.observe(scalar_probes[1], &buffer.field_ids);
        allocation.observe(scalar_probes[2], &buffer.antenna1);
        allocation.observe(scalar_probes[3], &buffer.antenna2);
        allocation.observe(scalar_probes[4], &buffer.feed1);
        allocation.observe(scalar_probes[5], &buffer.feed2);
        allocation.observe(scalar_probes[6], &buffer.time_mjd_seconds);
        allocation.observe(scalar_probes[7], &buffer.time_centroid_mjd_seconds);
        allocation.observe(scalar_probes[8], &buffer.interval_seconds);
        allocation.observe(scalar_probes[9], &buffer.exposure_seconds);
        allocation.observe(scalar_probes[10], &buffer.scan_numbers);
        allocation.observe(scalar_probes[11], &buffer.state_ids);
        allocation.observe(scalar_probes[12], &buffer.observation_ids);
        allocation.observe(scalar_probes[13], &buffer.array_ids);
        allocation.observe(scalar_probes[14], &buffer.row_flag);
        let scalar_read_nanos = scalar_started.elapsed().as_nanos();

        let uvw_started = Instant::now();
        let uvw_probe = AllocationProbe::capture(&buffer.uvw_m);
        let uvw_shape = self
            .main_table()
            .column_accessor("UVW")?
            .fill_array_cells_1d_typed_uncached(
                request.row_indices,
                SelectedArray1DCellsMut::Float64(&mut buffer.uvw_m),
            )?;
        allocation.observe(uvw_probe, &buffer.uvw_m);
        if uvw_shape.row_count != request.row_indices.len() || uvw_shape.axis0_count != 3 {
            return Err(invalid("MAIN UVW shape must be exactly [row][3]"));
        }
        let uvw_read_nanos = uvw_started.elapsed().as_nanos();

        let assembly_started = Instant::now();
        validate_buffer_lengths(buffer)?;
        let assembly_nanos = assembly_started.elapsed().as_nanos();
        let row_count = u64::try_from(request.row_indices.len())
            .map_err(|_| invalid("selected-observation row count exceeds diagnostics domain"))?;
        let sample_count = row_count
            .checked_mul(u64::try_from(request.channel_range.count).map_err(|_| {
                invalid("selected-observation channel count exceeds diagnostics domain")
            })?)
            .and_then(|count| count.checked_mul(u64::try_from(correlation_count).ok()?))
            .ok_or_else(|| {
                invalid("selected-observation sample count exceeds diagnostics domain")
            })?;
        let retained_current_bytes = buffer
            .retained_current_bytes()
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| {
                invalid("selected-observation current bytes exceed diagnostics domain")
            })?;
        let retained_capacity_bytes = buffer
            .retained_capacity_bytes()
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| {
                invalid("selected-observation capacity bytes exceed diagnostics domain")
            })?;
        let request_handoff_bytes = row_count
            .checked_mul(size_of::<usize>() as u64)
            .ok_or_else(|| {
                invalid("selected-observation handoff bytes exceed diagnostics domain")
            })?;
        let logical_output_bytes = retained_current_bytes
            .checked_sub(request_handoff_bytes)
            .ok_or_else(|| invalid("selected-observation logical byte accounting underflowed"))?;
        let timings = SelectedObservationBufferTimings {
            total_fill_nanos: fill_started.elapsed().as_nanos(),
            visibility_read_nanos,
            flag_read_nanos,
            weight_read_nanos,
            scalar_read_nanos,
            uvw_read_nanos,
            assembly_nanos,
        };
        Ok(SelectedObservationBufferFillReport {
            block_count: 1,
            row_count,
            sample_count,
            logical_output_bytes,
            // This private path does not yet expose trustworthy storage-manager
            // granularity, so it deliberately reports no physical-byte model.
            modeled_physical_read_bytes: None,
            read_operation_count: SELECTED_READ_OPERATION_COUNT,
            request_handoff_bytes,
            retained_current_bytes,
            retained_capacity_bytes,
            allocation: allocation.report(),
            timings,
        })
    }
}

fn validate_request(
    ms: &MeasurementSet,
    request: &SelectedObservationBufferRequest<'_>,
) -> MsResult<()> {
    if request.row_indices.is_empty() {
        return Err(invalid(
            "selected-observation buffer requires at least one row",
        ));
    }
    if request.channel_range.count == 0 {
        return Err(invalid(
            "selected-observation buffer requires at least one channel",
        ));
    }
    if request.row_indices.iter().any(|row| *row >= ms.row_count()) {
        return Err(invalid("selected-observation buffer row lies outside MAIN"));
    }
    Ok(())
}

fn require_shape(
    column: &str,
    rows: usize,
    channels: usize,
    correlations: usize,
    request: &SelectedObservationBufferRequest<'_>,
    expected_correlations: usize,
) -> MsResult<()> {
    if rows != request.row_indices.len()
        || channels != request.channel_range.count
        || correlations != expected_correlations
    {
        return Err(invalid(format!(
            "MAIN {column} shape differs from selected visibility shape"
        )));
    }
    Ok(())
}

fn validate_buffer_lengths(buffer: &SelectedObservationBuffer) -> MsResult<()> {
    let rows = buffer.row_count();
    let lengths = [
        buffer.row_flag.len(),
        buffer.data_description_ids.len(),
        buffer.field_ids.len(),
        buffer.antenna1.len(),
        buffer.antenna2.len(),
        buffer.feed1.len(),
        buffer.feed2.len(),
        buffer.time_mjd_seconds.len(),
        buffer.time_centroid_mjd_seconds.len(),
        buffer.interval_seconds.len(),
        buffer.exposure_seconds.len(),
        buffer.scan_numbers.len(),
        buffer.state_ids.len(),
        buffer.observation_ids.len(),
        buffer.array_ids.len(),
    ];
    if lengths.into_iter().any(|length| length != rows) {
        return Err(invalid("selected-observation scalar column lengths differ"));
    }
    if buffer.uvw_m.len() != rows.saturating_mul(3) {
        return Err(invalid("selected-observation UVW length differs"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}

const fn packed_sample_index(
    channel: usize,
    row: usize,
    correlation: usize,
    row_count: usize,
    correlation_count: usize,
) -> usize {
    (channel * row_count + row) * correlation_count + correlation
}

#[cfg(test)]
mod tests {
    use casa_tables::{ColumnBinding, DataManagerKind};
    use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    use crate::{
        MeasurementSet, MeasurementSetBuilder, OptionalMainColumn, SelectedObservationBuffer,
        SelectedObservationBufferRequest, SelectedStoredVisibility, SelectedVisibilityColumn,
        SelectedWeightColumn, VisibilityChannelReadRange, test_helpers::default_value,
    };

    #[test]
    fn selected_observation_buffer_reads_exact_closed_content_and_provenance() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selected-observation-buffer.ms");
        let mut ms = MeasurementSet::create(
            &path,
            MeasurementSetBuilder::new()
                .with_main_column(OptionalMainColumn::Data)
                .with_main_column(OptionalMainColumn::WeightSpectrum),
        )
        .unwrap();
        add_row(&mut ms, 0);
        add_row(&mut ms, 1);
        ms.save().unwrap();
        drop(ms);
        let ms = MeasurementSet::open(&path).unwrap();
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::Data,
            SelectedWeightColumn::WeightSpectrum,
            &[1, 0],
            VisibilityChannelReadRange::new(1, 2),
        );
        let mut buffer = SelectedObservationBuffer::default();
        let report = ms
            .fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();

        assert_eq!(buffer.row_count(), 2);
        assert_eq!(report.block_count, 1);
        assert_eq!(report.row_count, 2);
        assert_eq!(report.sample_count, 8);
        assert_eq!(report.logical_output_bytes, 298);
        assert_eq!(report.modeled_physical_read_bytes, None);
        assert_eq!(report.allocation.reused_storage_buffers, 0);
        assert_eq!(report.allocation.allocated_storage_buffers, 19);
        assert_eq!(report.retained_current_bytes, 314);
        assert!(report.retained_capacity_bytes >= report.retained_current_bytes);
        assert_eq!(buffer.channel_range, VisibilityChannelReadRange::new(1, 2));
        assert_eq!(buffer.correlation_count, 2);
        let sample = buffer.sample(0, 0, 1).unwrap();
        assert_eq!(sample.physical_row(), 1);
        assert_eq!(sample.data_description_id(), 13);
        assert_eq!(
            sample.visibility(),
            SelectedStoredVisibility::Complex32([111.0, -111.0])
        );
        assert!(!sample.channel_flag());
        assert!(sample.row_flag());
        assert_eq!(sample.input_weight(), 111.5);
        assert_eq!(sample.uvw_m(), [101.0, 102.0, 103.0]);
        assert_eq!(sample.time_mjd_seconds(), 1001.0);
        assert_eq!(sample.time_centroid_mjd_seconds(), 1002.0);
        assert_eq!(sample.interval_seconds(), 1010.0);
        assert_eq!(sample.exposure_seconds(), 1020.0);
        assert_eq!(sample.field_id(), 14);
        assert_eq!(sample.antenna1(), 11);
        assert_eq!(sample.antenna2(), 12);
        assert_eq!(sample.feed1(), 19);
        assert_eq!(sample.feed2(), 20);
        assert_eq!(sample.scan_number(), 17);
        assert_eq!(sample.state_id(), 18);
        assert_eq!(sample.observation_id(), 16);
        assert_eq!(sample.array_id(), 15);
    }

    #[test]
    fn selected_observation_buffer_refills_compatible_storage_in_place() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join("selected-observation-buffer-reuse.ms");
        let mut ms = MeasurementSet::create(
            &path,
            MeasurementSetBuilder::new()
                .with_main_column(OptionalMainColumn::Data)
                .with_main_column(OptionalMainColumn::WeightSpectrum),
        )
        .unwrap();
        add_row(&mut ms, 0);
        add_row(&mut ms, 1);
        ms.save().unwrap();
        drop(ms);
        let ms = MeasurementSet::open(&path).unwrap();
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::Data,
            SelectedWeightColumn::WeightSpectrum,
            &[1, 0],
            VisibilityChannelReadRange::new(1, 2),
        );
        let mut buffer = SelectedObservationBuffer::default();

        let first = ms
            .fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();
        let first_storage = storage_pointers(&buffer);
        let first_samples = (0..2)
            .flat_map(|row| {
                let buffer = &buffer;
                (0..2).flat_map(move |channel| {
                    (0..2).map(move |correlation| buffer.sample(channel, row, correlation).unwrap())
                })
            })
            .collect::<Vec<_>>();

        let second = ms
            .fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();
        let second_samples = (0..2)
            .flat_map(|row| {
                let buffer = &buffer;
                (0..2).flat_map(move |channel| {
                    (0..2).map(move |correlation| buffer.sample(channel, row, correlation).unwrap())
                })
            })
            .collect::<Vec<_>>();

        assert_eq!(first.allocation.allocated_storage_buffers, 19);
        assert_eq!(first.allocation.reused_storage_buffers, 0);
        assert_eq!(second.allocation.allocated_storage_buffers, 0);
        assert_eq!(second.allocation.reused_storage_buffers, 19);
        assert_eq!(storage_pointers(&buffer), first_storage);
        assert_eq!(second_samples, first_samples);
    }

    #[test]
    fn selected_observation_buffer_never_falls_back_from_weight_spectrum() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selected-observation-no-spectrum.ms");
        let mut ms = MeasurementSet::create(
            &path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
        )
        .unwrap();
        add_row(&mut ms, 0);
        ms.save().unwrap();
        drop(ms);
        let ms = MeasurementSet::open(&path).unwrap();
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::Data,
            SelectedWeightColumn::WeightSpectrum,
            &[0],
            VisibilityChannelReadRange::new(0, 1),
        );
        let error = ms
            .fill_selected_observation_buffer(&request, &mut SelectedObservationBuffer::default())
            .unwrap_err();
        assert!(error.to_string().contains("WEIGHT_SPECTRUM"));
    }

    fn storage_pointers(buffer: &SelectedObservationBuffer) -> [usize; 19] {
        let visibility = match buffer.visibility.as_ref().unwrap() {
            super::SelectedStoredVisibilities::Float32(values) => values.as_ptr() as usize,
            super::SelectedStoredVisibilities::Complex32(values) => values.as_ptr() as usize,
        };
        let weights = match buffer.weights.as_ref().unwrap() {
            super::SelectedStoredWeights::PerRow(values)
            | super::SelectedStoredWeights::PerChannel(values) => values.as_ptr() as usize,
        };
        [
            visibility,
            buffer.flags.as_ptr() as usize,
            weights,
            buffer.row_flag.as_ptr() as usize,
            buffer.uvw_m.as_ptr() as usize,
            buffer.data_description_ids.as_ptr() as usize,
            buffer.field_ids.as_ptr() as usize,
            buffer.antenna1.as_ptr() as usize,
            buffer.antenna2.as_ptr() as usize,
            buffer.feed1.as_ptr() as usize,
            buffer.feed2.as_ptr() as usize,
            buffer.time_mjd_seconds.as_ptr() as usize,
            buffer.time_centroid_mjd_seconds.as_ptr() as usize,
            buffer.interval_seconds.as_ptr() as usize,
            buffer.exposure_seconds.as_ptr() as usize,
            buffer.scan_numbers.as_ptr() as usize,
            buffer.state_ids.as_ptr() as usize,
            buffer.observation_ids.as_ptr() as usize,
            buffer.array_ids.as_ptr() as usize,
        ]
    }

    #[test]
    fn selected_observation_buffer_preserves_float_data_and_weight_broadcast() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selected-observation-float.ms");
        let mut ms = MeasurementSet::create(
            &path,
            MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::FloatData),
        )
        .unwrap();
        add_row(&mut ms, 0);
        ms.save().unwrap();
        let mut bindings = crate::ms::measurement_set_main_table_bindings(ms.main_table());
        bindings.insert(
            "FLOAT_DATA".to_string(),
            ColumnBinding {
                data_manager: DataManagerKind::TiledShapeStMan,
                tile_shape: Some(vec![2, 4, 1]),
            },
        );
        ms.main_table()
            .save_with_bindings(crate::ms::measurement_set_table_options(&path), &bindings)
            .unwrap();
        drop(ms);
        let ms = MeasurementSet::open(&path).unwrap();
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::FloatData,
            SelectedWeightColumn::Weight,
            &[0],
            VisibilityChannelReadRange::new(1, 2),
        );
        let mut buffer = SelectedObservationBuffer::default();
        ms.fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();

        let first_channel = buffer.sample(0, 0, 1).unwrap();
        let second_channel = buffer.sample(1, 0, 1).unwrap();
        assert_eq!(
            first_channel.visibility(),
            SelectedStoredVisibility::Float32(11.0)
        );
        assert_eq!(
            second_channel.visibility(),
            SelectedStoredVisibility::Float32(21.0)
        );
        assert_eq!(first_channel.input_weight(), 2.0);
        assert_eq!(second_channel.input_weight(), 2.0);
    }

    fn add_row(ms: &mut MeasurementSet, row_id: i32) {
        let fields = ms
            .main_table()
            .schema()
            .unwrap()
            .columns()
            .iter()
            .map(|column| {
                let value = match column.name() {
                    "DATA" => Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(vec![2, 4], complex_row(row_id)).unwrap(),
                    )),
                    "FLOAT_DATA" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2, 4], float_row(row_id)).unwrap(),
                    )),
                    "FLAG" => Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(vec![2, 4], flag_values(row_id)).unwrap(),
                    )),
                    "WEIGHT" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 2.0]).unwrap(),
                    )),
                    "WEIGHT_SPECTRUM" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2, 4], spectrum_weights(row_id)).unwrap(),
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
                    "ARRAY_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 5)),
                    "OBSERVATION_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 6)),
                    "SCAN_NUMBER" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 7)),
                    "STATE_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 8)),
                    "FEED1" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 9)),
                    "FEED2" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 10)),
                    "FLAG_ROW" => Value::Scalar(ScalarValue::Bool(row_id == 1)),
                    "TIME" => Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 1.0)),
                    "TIME_CENTROID" => {
                        Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 2.0))
                    }
                    "INTERVAL" => {
                        Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 10.0))
                    }
                    "EXPOSURE" => {
                        Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 20.0))
                    }
                    _ => default_value(column.name()),
                };
                RecordField::new(column.name(), value)
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }

    fn complex_row(row_id: i32) -> Vec<Complex32> {
        (0..2)
            .flat_map(|corr| {
                (0..4).map(move |channel| {
                    let value = row_id as f32 * 100.0 + channel as f32 * 10.0 + corr as f32;
                    Complex32::new(value, -value)
                })
            })
            .collect()
    }

    fn float_row(row_id: i32) -> Vec<f32> {
        (0..2)
            .flat_map(|corr| {
                (0..4)
                    .map(move |channel| row_id as f32 * 100.0 + channel as f32 * 10.0 + corr as f32)
            })
            .collect()
    }

    fn flag_values(row_id: i32) -> Vec<bool> {
        (0..2)
            .flat_map(|corr| (0..4).map(move |channel| (row_id + channel + corr) % 2 == 0))
            .collect()
    }

    fn spectrum_weights(row_id: i32) -> Vec<f32> {
        (0..2)
            .flat_map(|corr| {
                (0..4).map(move |channel| {
                    row_id as f32 * 100.0 + channel as f32 * 10.0 + corr as f32 + 0.5
                })
            })
            .collect()
    }
}
