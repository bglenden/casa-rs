// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MeasurementSet v2 adapter: MAIN rows as listed samples.

use std::path::Path;

use casa_observation::{
    AccessRequest, AntennaEntry, BatchBuffers, BatchShape, Catalogue, Correlation, DataGroupEntry,
    FieldEntry, IoCounters, Layout, ObservationError, PartitionEntry, Precision, ReadEvent,
    Realization, TopologyShape, VisibilityCursor, VisibilityUnits,
};
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, SelectedArray1DCellsMut,
    SelectedArray2DCellsMut, Table,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MjdHighPrec};
use casa_types::{ScalarValue, Value};

use super::grouping::GroupIndex;
use crate::columns::direction_columns::DirectionColumn;
use crate::{MeasurementSet, MsError};

fn storage(error: impl Into<MsError>) -> ObservationError {
    ObservationError::Storage(Box::new(error.into()))
}

fn table_error(error: casa_tables::TableError) -> ObservationError {
    storage(MsError::from(error))
}

/// A column's `MEASINFO.Ref`, if it names one.
fn measure_reference(table: &Table, column: &str) -> Option<String> {
    let Value::Record(measinfo) = table.column_keywords(column)?.get("MEASINFO")? else {
        return None;
    };
    let Value::Scalar(ScalarValue::String(reference)) = measinfo.get("Ref")? else {
        return None;
    };
    Some(reference.clone())
}

/// One data description of the MeasurementSet: a partition.
#[derive(Debug)]
struct Partition {
    data_description: i32,
}

/// An opened MeasurementSet v2 and its catalogue.
///
/// Each `DATA_DESCRIPTION` row is a partition; its one data group, `base`,
/// associates `DATA`, `FLAG`, `WEIGHT_SPECTRUM` (when MAIN has it, else
/// `WEIGHT`) and `UVW`. Antenna and field identities are the subtable row
/// numbers of this opening.
pub struct Msv2Observation {
    ms: MeasurementSet,
    catalogue: Catalogue,
    partitions: Vec<Partition>,
    uvw_frame: DirectionRef,
    spectral_weights: bool,
}

impl Msv2Observation {
    /// Open the MeasurementSet at `path` and read its catalogue.
    ///
    /// The tables stay open with casacore read locks, and their rows stay on
    /// disk: the typed column fills read only the selected cells, as the
    /// production selected-observation source does.
    ///
    /// # Errors
    ///
    /// When the MeasurementSet cannot be opened or a subtable the
    /// catalogue needs is missing or malformed.
    #[cfg(unix)]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ObservationError> {
        Self::from_measurement_set(MeasurementSet::open_retained_read(path).map_err(storage)?)
    }

    fn from_measurement_set(ms: MeasurementSet) -> Result<Self, ObservationError> {
        let antenna = ms.antenna().map_err(storage)?;
        let antennas = (0..antenna.row_count())
            .map(|row| {
                Ok(AntennaEntry {
                    name: antenna.name(row).map_err(storage)?,
                    station: antenna.station(row).map_err(storage)?,
                })
            })
            .collect::<Result<_, ObservationError>>()?;
        let field = ms.field().map_err(storage)?;
        let field_table = ms
            .subtable(crate::SubtableId::Field)
            .ok_or_else(|| invalid("no FIELD subtable"))?;
        let phase_frame = measure_reference(field_table, "PHASE_DIR")
            .map(|name| {
                name.parse::<DirectionRef>()
                    .map_err(|_| invalid(format!("unknown PHASE_DIR reference {name}")))
            })
            .transpose()?
            .unwrap_or(DirectionRef::J2000);
        let phase_dir = DirectionColumn::phase_dir(field_table);
        let fields = (0..field.row_count())
            .map(|row| {
                let (longitude, latitude) =
                    phase_dir.get_direction(row).map_err(storage)?.as_angles();
                Ok(FieldEntry {
                    name: field.name(row).map_err(storage)?,
                    phase_centre: MDirection::from_angles(longitude, latitude, phase_frame),
                })
            })
            .collect::<Result<_, ObservationError>>()?;
        let (partitions, entries) = read_partitions(&ms)?;
        let main = ms.main_table();
        let uvw_frame = match measure_reference(main, "UVW") {
            Some(name) => name
                .parse::<DirectionRef>()
                .map_err(|_| invalid(format!("unknown UVW reference {name}")))?,
            // casacore's Muvw default.
            None => DirectionRef::ITRF,
        };
        let spectral_weights = main
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"));
        Ok(Self {
            catalogue: Catalogue {
                antennas,
                fields,
                partitions: entries,
            },
            partitions,
            uvw_frame,
            spectral_weights,
            ms,
        })
    }

    /// The catalogue.
    #[must_use]
    pub fn catalogue(&self) -> &Catalogue {
        &self.catalogue
    }

    /// The MeasurementSet.
    #[must_use]
    pub fn measurement_set(&self) -> &MeasurementSet {
        &self.ms
    }

    /// Bind a traversal of `request`.
    ///
    /// The binding reads MAIN's `DATA_DESC_ID`, `TIME` and `FIELD_ID` once
    /// to select, group and order the partition's rows: an index of three
    /// words per row.
    ///
    /// # Errors
    ///
    /// When the request names an unknown partition, data group or channel,
    /// asks for the channel-major layout or a precision the trial does not
    /// implement, or groups rows that are not in time order.
    pub fn bind_read(&self, request: &AccessRequest) -> Result<Msv2Cursor<'_>, ObservationError> {
        let partition = self
            .partitions
            .get(request.partition.0 as usize)
            .ok_or_else(|| invalid(format!("no partition {:?}", request.partition)))?;
        let entry = self.catalogue.partition(request.partition);
        check_request(request, entry)?;
        let main = self.ms.main_table();
        let all_rows = (0..main.row_count()).collect::<Vec<_>>();
        let (mut descriptions, mut all_times, mut all_fields) =
            (Vec::new(), Vec::new(), Vec::new());
        main.required_scalar_columns_for_rows_into(
            &all_rows,
            &mut [
                RequiredScalarColumnDestination::new(
                    "DATA_DESC_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut descriptions),
                ),
                RequiredScalarColumnDestination::new(
                    "TIME",
                    RequiredScalarColumnValuesMut::Float64(&mut all_times),
                ),
                RequiredScalarColumnDestination::new(
                    "FIELD_ID",
                    RequiredScalarColumnValuesMut::Int32(&mut all_fields),
                ),
            ],
        )
        .map_err(table_error)?;
        let mut rows = Vec::new();
        let mut times = Vec::new();
        let mut fields = Vec::new();
        for row in all_rows {
            if descriptions[row] == partition.data_description {
                rows.push(row);
                times.push(all_times[row]);
                fields.push(
                    u32::try_from(all_fields[row])
                        .map_err(|_| invalid(format!("row {row} has a negative FIELD_ID")))?,
                );
            }
        }
        let groups = GroupIndex::new(
            request.partition,
            request.grouping,
            request.closure,
            &times,
            &fields,
        )?;
        let samples_per_batch = request.max_samples.min(rows.len()).max(1);
        let channels = request.channels.len();
        let correlations = entry.correlations.len();
        let roles = request.roles;
        let per_sample = channels * correlations;
        let selected = rows.len()
            * (usize::from(roles.values) * per_sample
                + usize::from(roles.flags) * per_sample
                + usize::from(roles.weights)
                    * if self.spectral_weights {
                        per_sample
                    } else {
                        correlations
                    }
                + usize::from(roles.uvw) * 3);
        let realization = Realization {
            layout: Layout::SampleMajor,
            factored: false,
            samples_per_batch,
            batches: rows.len().div_ceil(samples_per_batch),
            batch_payload_bytes: samples_per_batch * (per_sample * (8 + 1 + 4) + 3 * 8 + 3 * 8),
            decode_units: rows.len(),
            decoded_elements: selected,
            selected_elements: selected,
        };
        Ok(Msv2Cursor {
            observation: self,
            request: request.clone(),
            rows,
            times,
            fields,
            groups,
            next: 0,
            antenna1: Vec::new(),
            antenna2: Vec::new(),
            realization,
            counters: IoCounters::default(),
        })
    }
}

fn invalid(reason: impl Into<String>) -> ObservationError {
    ObservationError::InvalidStore {
        reason: reason.into(),
    }
}

/// Refuse what the trial's v2 adapter does not implement.
fn check_request(request: &AccessRequest, entry: &PartitionEntry) -> Result<(), ObservationError> {
    if request.layout != Layout::SampleMajor {
        return Err(ObservationError::Unsupported {
            what: "the channel-major layout",
        });
    }
    // DATA is complex64: both precisions deliver it as stored.
    let (Precision::Native | Precision::Complex32 { .. }) = request.precision;
    if !entry
        .data_groups
        .iter()
        .any(|group| group.name == request.data_group)
    {
        return Err(ObservationError::InvalidRequest {
            reason: format!("no data group {}", request.data_group),
        });
    }
    if request.channels.is_empty() || request.channels.end > entry.frequencies_hz.len() {
        return Err(ObservationError::InvalidRequest {
            reason: format!(
                "channels {:?} outside the partition's {}",
                request.channels,
                entry.frequencies_hz.len()
            ),
        });
    }
    Ok(())
}

fn read_partitions(
    ms: &MeasurementSet,
) -> Result<(Vec<Partition>, Vec<PartitionEntry>), ObservationError> {
    let descriptions = ms.data_description().map_err(storage)?;
    let windows = ms.spectral_window().map_err(storage)?;
    let polarizations = ms.polarization().map_err(storage)?;
    let mut partitions = Vec::new();
    let mut entries = Vec::new();
    for row in 0..descriptions.row_count() {
        let window = usize::try_from(descriptions.spectral_window_id(row).map_err(storage)?)
            .map_err(|_| invalid("negative SPECTRAL_WINDOW_ID"))?;
        let polarization = usize::try_from(descriptions.polarization_id(row).map_err(storage)?)
            .map_err(|_| invalid("negative POLARIZATION_ID"))?;
        let frame_code = windows.meas_freq_ref(window).map_err(storage)?;
        let frequency_frame = FrequencyRef::from_casacore_code(frame_code)
            .ok_or_else(|| invalid(format!("unknown MEAS_FREQ_REF {frame_code}")))?;
        let correlations = polarizations
            .corr_type(polarization)
            .map_err(storage)?
            .into_iter()
            .map(|code| {
                Correlation::from_casacore_code(code)
                    .ok_or_else(|| invalid(format!("unsupported CORR_TYPE {code}")))
            })
            .collect::<Result<_, _>>()?;
        partitions.push(Partition {
            data_description: i32::try_from(row)
                .map_err(|_| invalid("too many data descriptions"))?,
        });
        entries.push(PartitionEntry {
            spectral_window: windows.name(window).map_err(storage)?,
            frequencies_hz: windows.chan_freq(window).map_err(storage)?,
            frequency_frame,
            channel_widths_hz: windows.chan_width(window).map_err(storage)?,
            correlations,
            data_groups: vec![DataGroupEntry {
                name: "base".to_string(),
                visibilities: "DATA".to_string(),
                flags: "FLAG".to_string(),
                weights: if ms
                    .main_table()
                    .schema()
                    .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"))
                {
                    "WEIGHT_SPECTRUM".to_string()
                } else {
                    "WEIGHT".to_string()
                },
                uvw: "UVW".to_string(),
                units: VisibilityUnits::Undeclared,
            }],
        });
    }
    Ok((partitions, entries))
}

/// A bound traversal of an MSv2 partition: its rows in table order, in
/// batches of at most the request's samples.
pub struct Msv2Cursor<'o> {
    observation: &'o Msv2Observation,
    request: AccessRequest,
    rows: Vec<usize>,
    times: Vec<f64>,
    fields: Vec<u32>,
    groups: Option<GroupIndex>,
    next: usize,
    /// Stored `ANTENNA1` and `ANTENNA2` of the batch, before conversion.
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    realization: Realization,
    counters: IoCounters,
}

impl Msv2Cursor<'_> {
    fn fill(
        &mut self,
        buffers: &mut BatchBuffers,
        end: usize,
    ) -> Result<BatchShape, ObservationError> {
        let observation = self.observation;
        let main = observation.ms.main_table();
        let entry = observation.catalogue.partition(self.request.partition);
        let rows = &self.rows[self.next..end];
        let capacity = buffers.capacity_bytes();
        buffers.clear();
        buffers.group_name.clone_from(&self.request.data_group);
        buffers.units = Some(VisibilityUnits::Undeclared);
        buffers.correlations.extend_from_slice(&entry.correlations);
        buffers
            .frequency_hz
            .extend_from_slice(&entry.frequencies_hz[self.request.channels.clone()]);
        buffers
            .time_offsets_s
            .extend_from_slice(&self.times[self.next..end]);
        buffers
            .field
            .extend_from_slice(&self.fields[self.next..end]);
        main.required_scalar_columns_for_rows_into(
            rows,
            &mut [
                RequiredScalarColumnDestination::new(
                    "ANTENNA1",
                    RequiredScalarColumnValuesMut::Int32(&mut self.antenna1),
                ),
                RequiredScalarColumnDestination::new(
                    "ANTENNA2",
                    RequiredScalarColumnValuesMut::Int32(&mut self.antenna2),
                ),
                RequiredScalarColumnDestination::new(
                    "FLAG_ROW",
                    RequiredScalarColumnValuesMut::Bool(&mut buffers.row_flags),
                ),
            ],
        )
        .map_err(table_error)?;
        // The contract's antennas are unsigned; casacore's are Int32.
        for (stored, ids) in [
            (&self.antenna1, &mut buffers.antenna1),
            (&self.antenna2, &mut buffers.antenna2),
        ] {
            for &antenna in stored {
                ids.push(antenna_id(antenna)?);
            }
        }
        self.counters.copied_elements += 2 * rows.len();
        self.counters.read_calls += 1;
        let channel_start = self.request.channels.start;
        let channel_count = self.request.channels.len();
        let roles = self.request.roles;
        if roles.values {
            main.column_accessor("DATA")
                .map_err(table_error)?
                .fill_array_cells_2d_channel_range_typed_uncached(
                    rows,
                    channel_start,
                    channel_count,
                    SelectedArray2DCellsMut::RowChannelComplex32(&mut buffers.values),
                )
                .map_err(table_error)?
                .ok_or_else(|| invalid("undefined DATA cells"))?;
            self.counters.read_calls += 1;
        }
        if roles.flags {
            main.column_accessor("FLAG")
                .map_err(table_error)?
                .fill_array_cells_2d_channel_range_typed_uncached(
                    rows,
                    channel_start,
                    channel_count,
                    SelectedArray2DCellsMut::RowChannelBool(&mut buffers.flags),
                )
                .map_err(table_error)?
                .ok_or_else(|| invalid("undefined FLAG cells"))?;
            self.counters.read_calls += 1;
        }
        if roles.weights {
            let column = main
                .column_accessor(if observation.spectral_weights {
                    "WEIGHT_SPECTRUM"
                } else {
                    "WEIGHT"
                })
                .map_err(table_error)?;
            if observation.spectral_weights {
                column
                    .fill_array_cells_2d_channel_range_typed_uncached(
                        rows,
                        channel_start,
                        channel_count,
                        SelectedArray2DCellsMut::RowChannelFloat32(&mut buffers.weights),
                    )
                    .map_err(table_error)?
                    .ok_or_else(|| invalid("undefined WEIGHT_SPECTRUM cells"))?;
            } else {
                column
                    .fill_array_cells_1d_typed_uncached(
                        rows,
                        SelectedArray1DCellsMut::Float32(&mut buffers.weights),
                    )
                    .map_err(table_error)?;
            }
            self.counters.read_calls += 1;
        }
        if roles.uvw {
            main.column_accessor("UVW")
                .map_err(table_error)?
                .fill_array_cells_1d_typed_uncached(
                    rows,
                    SelectedArray1DCellsMut::Float64(&mut buffers.uvw),
                )
                .map_err(table_error)?;
            self.counters.read_calls += 1;
        }
        if let Some(groups) = self.groups.as_mut() {
            groups.fragments(self.next..end, 1, &mut buffers.fragments);
        }
        self.counters.delivered_bytes += buffers.values.len() * size_of::<num_complex::Complex32>()
            + buffers.flags.len()
            + buffers.weights.len() * size_of::<f32>()
            + buffers.uvw.len() * size_of::<f64>();
        if buffers.capacity_bytes() > capacity {
            self.counters.buffer_growths += 1;
        }
        Ok(BatchShape {
            partition: self.request.partition,
            topology: TopologyShape::Irregular {
                samples: rows.len(),
            },
            channels: self.request.channels.clone(),
            roles,
            spectral_weights: observation.spectral_weights,
            time_scale: EpochRef::UTC,
            time_origin: MjdHighPrec::new(0.0, 0.0),
            frequency_frame: entry.frequency_frame,
            uvw_frame: observation.uvw_frame,
            row_flags: true,
        })
    }
}

fn antenna_id(value: i32) -> Result<u32, ObservationError> {
    u32::try_from(value).map_err(|_| invalid(format!("negative antenna {value}")))
}

impl VisibilityCursor for Msv2Cursor<'_> {
    fn realization(&self) -> &Realization {
        &self.realization
    }

    fn next<'b>(
        &mut self,
        buffers: &'b mut BatchBuffers,
    ) -> Result<Option<ReadEvent<'b>>, ObservationError> {
        if let Some(key) = self.groups.as_mut().and_then(GroupIndex::pop_end) {
            return Ok(Some(ReadEvent::GroupEnd(key)));
        }
        if self.next == self.rows.len() {
            return Ok(None);
        }
        let end = (self.next + self.realization.samples_per_batch).min(self.rows.len());
        let shape = self.fill(buffers, end)?;
        self.counters.batches += 1;
        self.counters.decode_units += end - self.next;
        self.next = end;
        let buffers: &'b BatchBuffers = buffers;
        buffers
            .view(&shape)
            .map(|batch| Some(ReadEvent::Batch(batch)))
    }

    fn counters(&self) -> IoCounters {
        self.counters
    }
}
