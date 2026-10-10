// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_tables::{SelectedArray1DCellsMut, SelectedArray2DCellsMut};

use crate::{
    MeasurementSet, MsError, MsResult, SelectedObservationRow, VisibilityChannelReadRange,
    schema::main_table::VisibilityDataColumn,
};

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
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SelectedObservationBufferRequest<'a> {
    visibility: SelectedVisibilityColumn,
    weight: SelectedWeightColumn,
    rows: &'a [SelectedObservationRow],
    channel_range: VisibilityChannelReadRange,
}

impl<'a> SelectedObservationBufferRequest<'a> {
    /// Construct one exact contiguous-channel storage request for `rows`,
    /// the stored MAIN rows the row walk already read.
    #[must_use]
    pub(crate) const fn new(
        visibility: SelectedVisibilityColumn,
        weight: SelectedWeightColumn,
        rows: &'a [SelectedObservationRow],
        channel_range: VisibilityChannelReadRange,
    ) -> Self {
        Self {
            visibility,
            weight,
            rows,
            channel_range,
        }
    }
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

pub use casa_imaging_model::{SelectedNumericVisibility, SelectedNumericWeights};

/// Bounded numeric columns of one selected source block. All channelized slices
/// use `[row][channel][correlation]`; the owner cannot refill while borrowed.
#[derive(Clone, Copy)]
pub struct SelectedObservationNumericColumns<'a> {
    /// Selected physical MAIN rows in canonical output order.
    pub physical_rows: &'a [usize],
    /// First physical source channel and number of contiguous stored channels.
    pub channel_range: VisibilityChannelReadRange,
    /// Number of stored correlations per selected row/channel.
    pub correlation_count: usize,
    /// Visibility payload in its stored precision.
    pub visibility: SelectedNumericVisibility<'a>,
    /// Per-sample channel flags in row-major order.
    pub flags: &'a [bool],
    /// Input weights in their original broadcast or per-channel shape.
    pub weights: SelectedNumericWeights<'a>,
    /// Per-row `FLAG_ROW` values.
    pub row_flags: &'a [bool],
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
    time_mjd_seconds: Vec<f64>,
    time_centroid_mjd_seconds: Vec<f64>,
}

/// Bytes the typed column reads of one block fill hold besides the buffer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SelectedObservationReadStaging {
    /// Held per selected row.
    pub(crate) bytes_per_row: usize,
    /// Held once per fill.
    pub(crate) fixed_bytes: usize,
}

/// Project what the column reads of one block fill hold besides the buffer,
/// for MAIN cells of `correlations` x `stored_channels` samples: the row
/// walk's `UVW` read, then the visibility, `FLAG` and weight reads of
/// [`MeasurementSet::fill_selected_observation_buffer`].
///
/// A column its data manager streams holds nothing per row. A column read
/// cell by cell holds the whole stored cells of the selected rows, all
/// channels, however narrow the selected channel range
/// ([`casa_tables::SelectedReadFootprint`]). The fill reads one column at a
/// time, so the staging is the largest any one read holds.
pub(crate) fn selected_observation_read_staging(
    main: &casa_tables::Table,
    visibility: SelectedVisibilityColumn,
    weight: SelectedWeightColumn,
    correlations: usize,
    stored_channels: usize,
) -> Option<SelectedObservationReadStaging> {
    let samples = correlations.checked_mul(stored_channels)?;
    let visibility_bytes = match visibility {
        SelectedVisibilityColumn::FloatData => size_of::<f32>(),
        SelectedVisibilityColumn::Data | SelectedVisibilityColumn::CorrectedData => {
            size_of::<num_complex::Complex32>()
        }
    };
    let weight_read = match weight {
        SelectedWeightColumn::Weight => (
            main.selected_cell_read_footprint("WEIGHT"),
            correlations.checked_mul(size_of::<f32>())?,
        ),
        SelectedWeightColumn::WeightSpectrum => (
            main.selected_channel_read_footprint("WEIGHT_SPECTRUM"),
            samples.checked_mul(size_of::<f32>())?,
        ),
    };
    [
        (
            main.selected_channel_read_footprint(visibility.name()),
            samples.checked_mul(visibility_bytes)?,
        ),
        (
            main.selected_channel_read_footprint("FLAG"),
            samples.checked_mul(size_of::<bool>())?,
        ),
        weight_read,
        (
            main.selected_cell_read_footprint("UVW"),
            size_of::<[f64; 3]>(),
        ),
    ]
    .into_iter()
    .try_fold(
        SelectedObservationReadStaging::default(),
        |staging, (footprint, stored_cell_bytes)| {
            Some(SelectedObservationReadStaging {
                bytes_per_row: staging
                    .bytes_per_row
                    .max(footprint.staging_bytes_per_row(stored_cell_bytes)?),
                fixed_bytes: staging
                    .fixed_bytes
                    .max(footprint.staging_fixed_bytes(stored_cell_bytes)?),
            })
        },
    )
}

/// Project the bytes a buffer holds after
/// [`MeasurementSet::fill_selected_observation_buffer`] fills it with `rows`
/// rows. The fill copies the request's stored rows and reads the channelized
/// columns straight into these vectors, so this is also its peak besides
/// the read staging ([`selected_observation_read_staging`]).
pub(crate) fn selected_observation_buffer_resident_bytes(
    rows: usize,
    packed_samples: usize,
    weight_values: usize,
    visibility_bytes: usize,
) -> Option<usize> {
    let row_indices = rows.checked_mul(size_of::<usize>())?;
    let visibility = packed_samples.checked_mul(visibility_bytes)?;
    let flags = packed_samples.checked_mul(size_of::<bool>())?;
    let weights = weight_values.checked_mul(size_of::<f32>())?;
    let scalar_payload =
        rows.checked_mul(4 * size_of::<i32>() + 2 * size_of::<f64>() + size_of::<bool>())?;
    let uvw = rows.checked_mul(size_of::<[f64; 3]>())?;
    row_indices
        .checked_add(visibility)?
        .checked_add(flags)?
        .checked_add(weights)?
        .checked_add(scalar_payload)?
        .checked_add(uvw)
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
            time_mjd_seconds: Vec::new(),
            time_centroid_mjd_seconds: Vec::new(),
        }
    }
}

impl SelectedObservationBuffer {
    pub(crate) fn numeric_columns(&self) -> Option<SelectedObservationNumericColumns<'_>> {
        let visibility = match self.visibility.as_ref()? {
            SelectedStoredVisibilities::Float32(values) => {
                SelectedNumericVisibility::Float32(values)
            }
            SelectedStoredVisibilities::Complex32(values) => {
                SelectedNumericVisibility::Complex32(values)
            }
        };
        let weights = match self.weights.as_ref()? {
            SelectedStoredWeights::PerRow(values) => SelectedNumericWeights::PerRow(values),
            SelectedStoredWeights::PerChannel(values) => SelectedNumericWeights::PerChannel(values),
        };
        Some(SelectedObservationNumericColumns {
            physical_rows: &self.row_indices,
            channel_range: self.channel_range,
            correlation_count: self.correlation_count,
            visibility,
            flags: &self.flags,
            weights,
            row_flags: &self.row_flag,
        })
    }
    /// Number of selected MAIN rows in this block.
    #[must_use]
    pub(crate) fn row_count(&self) -> usize {
        self.row_indices.len()
    }

    /// Return the MAIN metadata of one row of this block.
    #[must_use]
    pub(crate) fn row(&self, row_offset: usize) -> Option<SelectedStoredRow> {
        let uvw_start = row_offset.checked_mul(3)?;
        let uvw = self.uvw_m.get(uvw_start..uvw_start + 3)?;
        Some(SelectedStoredRow {
            physical_row: *self.row_indices.get(row_offset)?,
            data_description_id: *self.data_description_ids.get(row_offset)?,
            row_flag: *self.row_flag.get(row_offset)?,
            uvw_m: [uvw[0], uvw[1], uvw[2]],
            time_mjd_seconds: *self.time_mjd_seconds.get(row_offset)?,
            time_centroid_mjd_seconds: *self.time_centroid_mjd_seconds.get(row_offset)?,
            field_id: *self.field_ids.get(row_offset)?,
            antenna1: *self.antenna1.get(row_offset)?,
            antenna2: *self.antenna2.get(row_offset)?,
        })
    }
}

/// The MAIN metadata of one selected row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SelectedStoredRow {
    physical_row: usize,
    data_description_id: i32,
    row_flag: bool,
    uvw_m: [f64; 3],
    time_mjd_seconds: f64,
    time_centroid_mjd_seconds: f64,
    field_id: i32,
    antenna1: i32,
    antenna2: i32,
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

impl SelectedStoredRow {
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
    selected_sample_getter!(row_flag, row_flag, bool, "Return `FLAG_ROW`.");
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
    selected_sample_getter!(field_id, field_id, i32, "Return `FIELD_ID`.");
    selected_sample_getter!(antenna1, antenna1, i32, "Return `ANTENNA1`.");
    selected_sample_getter!(antenna2, antenna2, i32, "Return `ANTENNA2`.");
}

impl MeasurementSet {
    /// Fill one bounded block with the closed selected-observation storage column set.
    ///
    /// The row metadata (`DATA_DESC_ID`, `FIELD_ID`, the antennas, `TIME`,
    /// `TIME_CENTROID`, `FLAG_ROW` and `UVW`) is copied from the request's
    /// stored rows, which the row walk read; only the visibility, `FLAG` and
    /// weight columns are read here.
    ///
    /// This selected-channel operation requires a lazily reopened disk-backed MeasurementSet with
    /// no pending array-cell writes. It never falls back to cloning complete array cells.
    pub(crate) fn fill_selected_observation_buffer(
        &self,
        request: &SelectedObservationBufferRequest<'_>,
        buffer: &mut SelectedObservationBuffer,
    ) -> MsResult<()> {
        validate_request(self, request)?;
        buffer.copy_stored_rows(request.rows);
        buffer.channel_range = request.channel_range;
        let row_indices = buffer.row_indices.as_slice();

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
                self.main_table()
                    .column_accessor(request.visibility.name())?
                    .fill_array_cells_2d_channel_range_typed_uncached(
                        row_indices,
                        request.channel_range.start,
                        request.channel_range.count,
                        SelectedArray2DCellsMut::RowChannelFloat32(values),
                    )?
                    .ok_or_else(|| {
                        invalid(format!(
                            "required MAIN {} cells are undefined",
                            request.visibility.name()
                        ))
                    })?
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
                self.main_table()
                    .column_accessor(request.visibility.name())?
                    .fill_array_cells_2d_channel_range_typed_uncached(
                        row_indices,
                        request.channel_range.start,
                        request.channel_range.count,
                        SelectedArray2DCellsMut::RowChannelComplex32(values),
                    )?
                    .ok_or_else(|| {
                        invalid(format!(
                            "required MAIN {} cells are undefined",
                            request.visibility.name()
                        ))
                    })?
            }
        };
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

        let flag_shape = self
            .main_table()
            .column_accessor("FLAG")?
            .fill_array_cells_2d_channel_range_typed_uncached(
                row_indices,
                request.channel_range.start,
                request.channel_range.count,
                SelectedArray2DCellsMut::RowChannelBool(&mut buffer.flags),
            )?
            .ok_or_else(|| invalid("required MAIN FLAG cells are undefined"))?;
        require_shape(
            "FLAG",
            flag_shape.row_count,
            flag_shape.channel_count,
            flag_shape.axis0_count,
            request,
            correlation_count,
        )?;

        let weight_shape = match request.weight {
            SelectedWeightColumn::Weight => {
                if !matches!(buffer.weights, Some(SelectedStoredWeights::PerRow(_))) {
                    buffer.weights = Some(SelectedStoredWeights::PerRow(Vec::new()));
                }
                let Some(SelectedStoredWeights::PerRow(values)) = buffer.weights.as_mut() else {
                    unreachable!("per-row weight destination installed above")
                };
                let shape = self
                    .main_table()
                    .column_accessor("WEIGHT")?
                    .fill_array_cells_1d_typed_uncached(
                        row_indices,
                        SelectedArray1DCellsMut::Float32(values),
                    )?;
                if shape.row_count != row_indices.len() || shape.axis0_count != correlation_count {
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
                Some(
                    self.main_table()
                        .column_accessor("WEIGHT_SPECTRUM")?
                        .fill_array_cells_2d_channel_range_typed_uncached(
                            row_indices,
                            request.channel_range.start,
                            request.channel_range.count,
                            SelectedArray2DCellsMut::RowChannelFloat32(values),
                        )?
                        .ok_or_else(|| {
                            invalid("required MAIN WEIGHT_SPECTRUM cells are undefined")
                        })?,
                )
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
        Ok(())
    }
}

impl SelectedObservationBuffer {
    /// Replace the row indices and row metadata with those of `rows`,
    /// keeping the vectors' storage.
    fn copy_stored_rows(&mut self, rows: &[SelectedObservationRow]) {
        self.row_indices.clear();
        self.data_description_ids.clear();
        self.field_ids.clear();
        self.antenna1.clear();
        self.antenna2.clear();
        self.time_mjd_seconds.clear();
        self.time_centroid_mjd_seconds.clear();
        self.row_flag.clear();
        self.uvw_m.clear();
        for row in rows {
            self.row_indices.push(row.physical_row);
            self.data_description_ids.push(row.data_description_id);
            self.field_ids.push(row.field_id);
            self.antenna1.push(row.antenna1);
            self.antenna2.push(row.antenna2);
            self.time_mjd_seconds.push(row.time_mjd_seconds);
            self.time_centroid_mjd_seconds
                .push(row.time_centroid_mjd_seconds);
            self.row_flag.push(row.flag_row);
            self.uvw_m.extend_from_slice(&row.uvw_m);
        }
    }
}

fn validate_request(
    ms: &MeasurementSet,
    request: &SelectedObservationBufferRequest<'_>,
) -> MsResult<()> {
    if request.rows.is_empty() {
        return Err(invalid(
            "selected-observation buffer requires at least one row",
        ));
    }
    if request.channel_range.count == 0 {
        return Err(invalid(
            "selected-observation buffer requires at least one channel",
        ));
    }
    if request
        .rows
        .iter()
        .any(|row| row.physical_row >= ms.row_count())
    {
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
    if rows != request.rows.len()
        || channels != request.channel_range.count
        || correlations != expected_correlations
    {
        return Err(invalid(format!(
            "MAIN {column} shape differs from selected visibility shape"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}

#[cfg(test)]
mod tests {
    use casa_tables::{ColumnBinding, DataManagerKind};
    use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    use crate::{
        MeasurementSet, MeasurementSetBuilder, MsReadPlan, MsSelectionIoBudget, OptionalMainColumn,
        SelectedNumericVisibility, SelectedNumericWeights, SelectedObservationBuffer,
        SelectedObservationBufferRequest, SelectedObservationRow, SelectedVisibilityColumn,
        SelectedWeightColumn, VisibilityChannelReadRange, test_helpers::default_value,
    };

    /// The stored MAIN rows at `physical_rows`, in that order, as the row
    /// walk reads them.
    fn stored_rows(ms: &MeasurementSet, physical_rows: &[usize]) -> Vec<SelectedObservationRow> {
        let plan = MsReadPlan::new(
            ms.row_count(),
            MsSelectionIoBudget {
                available_bytes: ms.row_count() * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                maximum_live_blocks: 1,
                requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                storage_alignment_rows: None,
            },
        )
        .unwrap();
        let mut cursor = ms.main_row_selection_cursor(plan).unwrap();
        let mut rows = Vec::new();
        while let Some(row) = cursor.next(ms).unwrap() {
            rows.push(row);
        }
        physical_rows.iter().map(|row| rows[*row]).collect()
    }

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
        let rows = stored_rows(&ms, &[1, 0]);
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::Data,
            SelectedWeightColumn::WeightSpectrum,
            &rows,
            VisibilityChannelReadRange::new(1, 2),
        );
        let mut buffer = SelectedObservationBuffer::default();
        ms.fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();

        assert_eq!(buffer.row_count(), 2);
        assert_eq!(buffer.channel_range, VisibilityChannelReadRange::new(1, 2));
        assert_eq!(buffer.correlation_count, 2);
        let expected = [110.0, 111.0, 120.0, 121.0, 10.0, 11.0, 20.0, 21.0];
        let super::SelectedStoredVisibilities::Complex32(values) =
            buffer.visibility.as_ref().unwrap()
        else {
            panic!("selected DATA must remain Complex32");
        };
        assert_eq!(
            values.as_slice(),
            expected
                .map(|value| Complex32::new(value, -value))
                .as_slice(),
            "selected numeric payload must be [row][channel][correlation]"
        );
        let super::SelectedStoredWeights::PerChannel(weights) = buffer.weights.as_ref().unwrap()
        else {
            panic!("selected WEIGHT_SPECTRUM must remain per-channel");
        };
        assert_eq!(
            weights.as_slice(),
            expected.map(|value| value + 0.5).as_slice()
        );
        let columns = buffer.numeric_columns().unwrap();
        assert_eq!(columns.physical_rows, [1, 0]);
        assert_eq!(columns.row_flags, [true, false]);
        // `(row + channel + correlation)` even, for rows 1 and 0 and channels 1 and 2.
        assert_eq!(
            columns.flags,
            [true, false, false, true, false, true, true, false]
        );
        let row = buffer.row(0).unwrap();
        assert_eq!(row.physical_row(), 1);
        assert_eq!(row.data_description_id(), 13);
        assert!(row.row_flag());
        assert_eq!(row.uvw_m(), [101.0, 102.0, 103.0]);
        assert_eq!(row.time_mjd_seconds(), 1001.0);
        assert_eq!(row.time_centroid_mjd_seconds(), 1002.0);
        assert_eq!(row.field_id(), 14);
        assert_eq!(row.antenna1(), 11);
        assert_eq!(row.antenna2(), 12);
        assert!(buffer.row(2).is_none());
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
        let rows = stored_rows(&ms, &[1, 0]);
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::Data,
            SelectedWeightColumn::WeightSpectrum,
            &rows,
            VisibilityChannelReadRange::new(1, 2),
        );
        let mut buffer = SelectedObservationBuffer::default();

        let snapshot = |buffer: &SelectedObservationBuffer| {
            let columns = buffer.numeric_columns().unwrap();
            let SelectedNumericVisibility::Complex32(visibility) = columns.visibility else {
                panic!("selected DATA must remain Complex32");
            };
            (
                visibility.to_vec(),
                columns.flags.to_vec(),
                (0..buffer.row_count())
                    .map(|row| buffer.row(row).unwrap())
                    .collect::<Vec<_>>(),
            )
        };
        ms.fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();
        let first_storage = storage_pointers(&buffer);
        let first = snapshot(&buffer);

        ms.fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();

        assert_eq!(storage_pointers(&buffer), first_storage);
        assert_eq!(snapshot(&buffer), first);
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
        let rows = stored_rows(&ms, &[0]);
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::Data,
            SelectedWeightColumn::WeightSpectrum,
            &rows,
            VisibilityChannelReadRange::new(0, 1),
        );
        let error = ms
            .fill_selected_observation_buffer(&request, &mut SelectedObservationBuffer::default())
            .unwrap_err();
        assert!(error.to_string().contains("WEIGHT_SPECTRUM"));
    }

    fn storage_pointers(buffer: &SelectedObservationBuffer) -> [usize; 11] {
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
            buffer.time_mjd_seconds.as_ptr() as usize,
            buffer.time_centroid_mjd_seconds.as_ptr() as usize,
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
        let rows = stored_rows(&ms, &[0]);
        let request = SelectedObservationBufferRequest::new(
            SelectedVisibilityColumn::FloatData,
            SelectedWeightColumn::Weight,
            &rows,
            VisibilityChannelReadRange::new(1, 2),
        );
        let mut buffer = SelectedObservationBuffer::default();
        ms.fill_selected_observation_buffer(&request, &mut buffer)
            .unwrap();

        let columns = buffer.numeric_columns().unwrap();
        let SelectedNumericVisibility::Float32(visibility) = columns.visibility else {
            panic!("selected FLOAT_DATA must remain Float32");
        };
        // `[row][channel][correlation]`: correlation 1 of channels 1 and 2.
        assert_eq!(visibility[1], 11.0);
        assert_eq!(visibility[3], 21.0);
        let SelectedNumericWeights::PerRow(weights) = columns.weights else {
            panic!("selected WEIGHT must stay broadcast per row");
        };
        assert_eq!(weights, [1.0, 2.0]);
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
