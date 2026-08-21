// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashMap;

use casa_tables::{RequiredScalarColumnValues, SelectedArray1DCells, SelectedArray2DCells};

use crate::{
    MeasurementSet, MsError, MsResult, VisibilityChannelReadRange,
    schema::main_table::VisibilityDataColumn,
};

/// Exact MeasurementSet visibility column read for selected-observation input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedVisibilityColumn {
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
pub enum SelectedWeightColumn {
    /// MAIN per-row/per-correlation `WEIGHT`, broadcast over channels.
    Weight,
    /// MAIN per-channel `WEIGHT_SPECTRUM` with no existence fallback.
    WeightSpectrum,
}

/// One closed bounded selected-observation storage read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedObservationBufferRequest {
    visibility: SelectedVisibilityColumn,
    weight: SelectedWeightColumn,
    row_indices: Vec<usize>,
    channel_range: VisibilityChannelReadRange,
}

impl SelectedObservationBufferRequest {
    /// Construct one exact contiguous-channel storage request.
    #[must_use]
    pub const fn new(
        visibility: SelectedVisibilityColumn,
        weight: SelectedWeightColumn,
        row_indices: Vec<usize>,
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
pub enum SelectedStoredVisibility {
    /// Single-precision real `FLOAT_DATA` value.
    Float32(f32),
    /// Single-precision complex `DATA` or `CORRECTED_DATA` value.
    Complex32([f32; 2]),
}

#[derive(Debug, Clone)]
enum SelectedStoredVisibilities {
    Float32(Vec<f32>),
    Complex32(Vec<casa_types::Complex32>),
}

#[derive(Debug, Clone)]
enum SelectedStoredWeights {
    PerRow(Vec<f32>),
    PerChannel(Vec<f32>),
}

/// Caller-owned bounded storage block for exact selected-observation values.
#[derive(Debug, Clone)]
pub struct SelectedObservationBuffer {
    row_indices: Vec<usize>,
    channel_range: VisibilityChannelReadRange,
    correlation_count: usize,
    visibility: Option<SelectedStoredVisibilities>,
    flags: Vec<bool>,
    weights: Option<SelectedStoredWeights>,
    row_flag: Vec<bool>,
    uvw_m: Vec<[f64; 3]>,
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
    pub fn row_count(&self) -> usize {
        self.row_indices.len()
    }

    /// Contiguous native-channel range retained by this block.
    #[must_use]
    pub const fn channel_range(&self) -> VisibilityChannelReadRange {
        self.channel_range
    }

    /// Number of stored correlations per row/channel.
    #[must_use]
    pub const fn correlation_count(&self) -> usize {
        self.correlation_count
    }

    /// Return one stored sample by channel, row, and correlation block offsets.
    #[must_use]
    pub fn sample(
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
        Some(SelectedStoredSample {
            physical_row: *self.row_indices.get(row_offset)?,
            data_description_id: *self.data_description_ids.get(row_offset)?,
            visibility,
            channel_flag: *self.flags.get(sample_index)?,
            row_flag: *self.row_flag.get(row_offset)?,
            input_weight,
            uvw_m: *self.uvw_m.get(row_offset)?,
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
pub struct SelectedStoredSample {
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
        pub const fn $name(self) -> $type {
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
    pub fn fill_selected_observation_buffer(
        &self,
        request: &SelectedObservationBufferRequest,
        buffer: &mut SelectedObservationBuffer,
    ) -> MsResult<()> {
        validate_request(self, request)?;
        *buffer = SelectedObservationBuffer::default();
        let visibility_cells = read_required_2d(
            self,
            request.visibility.name(),
            &request.row_indices,
            request.channel_range,
        )?;
        let (visibility, visibility_rows, visibility_channels, correlation_count) =
            match (request.visibility, visibility_cells) {
                (SelectedVisibilityColumn::FloatData, SelectedArray2DCells::Float32(values)) => {
                    let shape = (
                        values.row_count(),
                        values.channel_count(),
                        values.axis0_count(),
                    );
                    (
                        SelectedStoredVisibilities::Float32(values.into_values()),
                        shape.0,
                        shape.1,
                        shape.2,
                    )
                }
                (
                    SelectedVisibilityColumn::Data | SelectedVisibilityColumn::CorrectedData,
                    SelectedArray2DCells::Complex32(values),
                ) => {
                    let shape = (
                        values.row_count(),
                        values.channel_count(),
                        values.axis0_count(),
                    );
                    (
                        SelectedStoredVisibilities::Complex32(values.into_values()),
                        shape.0,
                        shape.1,
                        shape.2,
                    )
                }
                (SelectedVisibilityColumn::FloatData, other) => {
                    return Err(column_type_error(
                        request.visibility.name(),
                        "Float32 2-D array",
                        &other,
                    ));
                }
                (_, other) => {
                    return Err(column_type_error(
                        request.visibility.name(),
                        "Complex32 2-D array",
                        &other,
                    ));
                }
            };
        require_shape(
            request.visibility.name(),
            visibility_rows,
            visibility_channels,
            correlation_count,
            request,
            correlation_count,
        )?;
        let flag_cells =
            read_required_2d(self, "FLAG", &request.row_indices, request.channel_range)?;
        let SelectedArray2DCells::Bool(flags) = flag_cells else {
            return Err(MsError::ColumnTypeMismatch {
                column: "FLAG".to_string(),
                table: "MAIN".to_string(),
                expected: "Bool 2-D array".to_string(),
                found: "different array primitive".to_string(),
            });
        };
        require_shape(
            "FLAG",
            flags.row_count(),
            flags.channel_count(),
            flags.axis0_count(),
            request,
            correlation_count,
        )?;
        let flags = flags.into_values();
        let weights = read_weights(self, request, correlation_count)?;
        let mut scalars = self.main_table().required_scalar_columns_owned_for_rows(
            &[
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
            ],
            &request.row_indices,
        )?;
        let uvw_m = read_strict_uvw(self, &request.row_indices)?;
        *buffer = SelectedObservationBuffer {
            row_indices: request.row_indices.clone(),
            channel_range: request.channel_range,
            correlation_count,
            visibility: Some(visibility),
            flags,
            weights: Some(weights),
            row_flag: take_bool(&mut scalars, "FLAG_ROW")?,
            uvw_m,
            data_description_ids: take_i32(&mut scalars, "DATA_DESC_ID")?,
            field_ids: take_i32(&mut scalars, "FIELD_ID")?,
            antenna1: take_i32(&mut scalars, "ANTENNA1")?,
            antenna2: take_i32(&mut scalars, "ANTENNA2")?,
            feed1: take_i32(&mut scalars, "FEED1")?,
            feed2: take_i32(&mut scalars, "FEED2")?,
            time_mjd_seconds: take_f64(&mut scalars, "TIME")?,
            time_centroid_mjd_seconds: take_f64(&mut scalars, "TIME_CENTROID")?,
            interval_seconds: take_f64(&mut scalars, "INTERVAL")?,
            exposure_seconds: take_f64(&mut scalars, "EXPOSURE")?,
            scan_numbers: take_i32(&mut scalars, "SCAN_NUMBER")?,
            state_ids: take_i32(&mut scalars, "STATE_ID")?,
            observation_ids: take_i32(&mut scalars, "OBSERVATION_ID")?,
            array_ids: take_i32(&mut scalars, "ARRAY_ID")?,
        };
        validate_buffer_lengths(buffer)?;
        Ok(())
    }
}

fn validate_request(
    ms: &MeasurementSet,
    request: &SelectedObservationBufferRequest,
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

fn read_required_2d(
    ms: &MeasurementSet,
    column: &str,
    rows: &[usize],
    channels: VisibilityChannelReadRange,
) -> MsResult<SelectedArray2DCells> {
    ms.main_table()
        .column_accessor(column)?
        .array_cells_2d_channel_range_typed_uncached(rows, channels.start, channels.count)?
        .ok_or_else(|| invalid(format!("required MAIN {column} cells are undefined")))
}

fn read_weights(
    ms: &MeasurementSet,
    request: &SelectedObservationBufferRequest,
    correlation_count: usize,
) -> MsResult<SelectedStoredWeights> {
    match request.weight {
        SelectedWeightColumn::Weight => {
            let cells = ms
                .main_table()
                .column_accessor("WEIGHT")?
                .array_cells_1d_typed_uncached(&request.row_indices)?;
            let SelectedArray1DCells::Float32(values) = cells else {
                return Err(invalid("MAIN WEIGHT must be a Float32 1-D array"));
            };
            if values.row_count() != request.row_indices.len()
                || values.axis0_count() != correlation_count
            {
                return Err(invalid(
                    "MAIN WEIGHT shape differs from selected visibility shape",
                ));
            }
            Ok(SelectedStoredWeights::PerRow(values.into_values()))
        }
        SelectedWeightColumn::WeightSpectrum => {
            let cells = read_required_2d(
                ms,
                "WEIGHT_SPECTRUM",
                &request.row_indices,
                request.channel_range,
            )?;
            let SelectedArray2DCells::Float32(values) = cells else {
                return Err(invalid("MAIN WEIGHT_SPECTRUM must be a Float32 2-D array"));
            };
            require_shape(
                "WEIGHT_SPECTRUM",
                values.row_count(),
                values.channel_count(),
                values.axis0_count(),
                request,
                correlation_count,
            )?;
            Ok(SelectedStoredWeights::PerChannel(values.into_values()))
        }
    }
}

fn require_shape(
    column: &str,
    rows: usize,
    channels: usize,
    correlations: usize,
    request: &SelectedObservationBufferRequest,
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

fn read_strict_uvw(ms: &MeasurementSet, rows: &[usize]) -> MsResult<Vec<[f64; 3]>> {
    let cells = ms
        .main_table()
        .column_accessor("UVW")?
        .array_cells_1d_typed_uncached(rows)?;
    let SelectedArray1DCells::Float64(values) = cells else {
        return Err(invalid("MAIN UVW must be a Float64 1-D array"));
    };
    if values.row_count() != rows.len() || values.axis0_count() != 3 {
        return Err(invalid("MAIN UVW shape must be exactly [row][3]"));
    }
    Ok(values
        .into_values()
        .chunks_exact(3)
        .map(|uvw| [uvw[0], uvw[1], uvw[2]])
        .collect())
}

fn validate_buffer_lengths(buffer: &SelectedObservationBuffer) -> MsResult<()> {
    let rows = buffer.row_count();
    let lengths = [
        buffer.row_flag.len(),
        buffer.uvw_m.len(),
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
    Ok(())
}

fn take_i32(
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    column: &str,
) -> MsResult<Vec<i32>> {
    match columns.remove(column) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        Some(_) => Err(invalid(format!("MAIN {column} must be Int32"))),
        None => Err(invalid(format!("MAIN {column} is missing"))),
    }
}

fn take_f64(
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    column: &str,
) -> MsResult<Vec<f64>> {
    match columns.remove(column) {
        Some(RequiredScalarColumnValues::Float64(values)) => Ok(values),
        Some(_) => Err(invalid(format!("MAIN {column} must be Float64"))),
        None => Err(invalid(format!("MAIN {column} is missing"))),
    }
}

fn take_bool(
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    column: &str,
) -> MsResult<Vec<bool>> {
    match columns.remove(column) {
        Some(RequiredScalarColumnValues::Bool(values)) => Ok(values),
        Some(_) => Err(invalid(format!("MAIN {column} must be Bool"))),
        None => Err(invalid(format!("MAIN {column} is missing"))),
    }
}

fn column_type_error(column: &str, expected: &str, actual: &SelectedArray2DCells) -> MsError {
    MsError::ColumnTypeMismatch {
        column: column.to_string(),
        table: "MAIN".to_string(),
        expected: expected.to_string(),
        found: format!("{:?} 2-D array", actual.primitive_type()),
    }
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
            vec![1, 0],
            VisibilityChannelReadRange::new(1, 2),
        );
        let mut buffer = SelectedObservationBuffer::default();
        ms.fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();

        assert_eq!(buffer.row_count(), 2);
        assert_eq!(
            buffer.channel_range(),
            VisibilityChannelReadRange::new(1, 2)
        );
        assert_eq!(buffer.correlation_count(), 2);
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
            vec![0],
            VisibilityChannelReadRange::new(0, 1),
        );
        let error = ms
            .fill_selected_observation_buffer(&request, &mut SelectedObservationBuffer::default())
            .unwrap_err();
        assert!(error.to_string().contains("WEIGHT_SPECTRUM"));
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
        drop(ms);
        let ms = MeasurementSet::open(&path).unwrap();
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::FloatData,
            SelectedWeightColumn::Weight,
            vec![0],
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
