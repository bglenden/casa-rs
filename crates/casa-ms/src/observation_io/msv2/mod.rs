// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MeasurementSet v2 adapter: MAIN rows as listed samples.

mod catalogue;

#[cfg(feature = "msv4")]
pub(crate) use catalogue::{metres, radians};

use std::path::Path;

use casa_observation::{
    AccessRequest, BatchBuffers, BatchHeader, BoundCursor, Catalogue, Fill, IdLimits, IoCounters,
    Layout, Observation, ObservationError, PartitionEntry, Precision, Realization, Roles, Sharing,
    TopologyShape, UvwFrame, VisibilityCursor,
};
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, SelectedArray1DCellsMut,
    SelectedArray2DCellsMut,
};
use casa_types::measures::{EpochRef, MjdHighPrec};

use super::OpenOptions;
use super::grouping::GroupIndex;
use crate::{MeasurementSet, MsError};

fn storage(error: impl Into<MsError>) -> ObservationError {
    ObservationError::Storage(Box::new(error.into()))
}

fn table_error(error: casa_tables::TableError) -> ObservationError {
    storage(MsError::from(error))
}

fn invalid(reason: impl Into<String>) -> ObservationError {
    ObservationError::InvalidStore {
        reason: reason.into(),
    }
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
/// `WEIGHT`) and `UVW`. Antenna, field and scan identities are numbered by
/// this opening: subtable rows for antennas and fields, distinct
/// `SCAN_NUMBER`s in ascending order for scans.
///
/// Opening checks every unit and measure reference the batches rely on:
/// `TIME` and `TIME_CENTROID` share one fixed reference, which the epoch
/// views carry (a variable reference is refused); the UVW convention is
/// resolved to the phase centres' frame ([`UvwFrame`]); positions,
/// diameters, angles and frequencies are in the units their types claim.
pub struct Msv2Observation {
    ms: MeasurementSet,
    catalogue: Catalogue,
    partitions: Vec<Partition>,
    uvw_frame: UvwFrame,
    time_scale: EpochRef,
    spectral_weights: bool,
    /// Each MAIN row's scan.
    row_scans: Vec<u32>,
    options: OpenOptions,
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
    /// When the MeasurementSet cannot be opened, a subtable the catalogue
    /// needs is missing or malformed, or a unit or measure reference is
    /// outside the trial profile.
    #[cfg(unix)]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ObservationError> {
        Self::open_with(path, &OpenOptions::default())
    }

    /// [`Self::open`] with `options`.
    ///
    /// # Errors
    ///
    /// As [`Self::open`].
    #[cfg(unix)]
    pub fn open_with(
        path: impl AsRef<Path>,
        options: &OpenOptions,
    ) -> Result<Self, ObservationError> {
        let ms = MeasurementSet::open_retained_read(path).map_err(storage)?;
        Self::from_measurement_set(ms, options)
    }

    fn from_measurement_set(
        ms: MeasurementSet,
        options: &OpenOptions,
    ) -> Result<Self, ObservationError> {
        let antennas = catalogue::read_antennas(&ms)?;
        let (fields, uvw_frame) = catalogue::fields_and_uvw(&ms)?;
        let main = ms.main_table();
        let time_scale = catalogue::time_scale(main)?;
        let units = catalogue::data_units(main);
        let (partitions, entries) = catalogue::read_partitions(&ms, &units, uvw_frame)?;
        let (scans, row_scans) = catalogue::read_scans(&ms)?;
        let spectral_weights = main
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"));
        let observation = ms.observation().map_err(storage)?;
        let (telescope, observers, project) = if observation.row_count() > 0 {
            (
                observation.string(0, "TELESCOPE_NAME").map_err(storage)?,
                vec![observation.string(0, "OBSERVER").map_err(storage)?],
                observation.string(0, "PROJECT").map_err(storage)?,
            )
        } else {
            (String::new(), Vec::new(), String::new())
        };
        Ok(Self {
            catalogue: Catalogue {
                telescope,
                observers,
                project,
                antennas,
                fields,
                scans,
                partitions: entries,
            },
            partitions,
            uvw_frame,
            time_scale,
            spectral_weights,
            row_scans,
            options: options.clone(),
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
    /// words per row. Every role is a required MSv2 column, so none is
    /// missing.
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
            .ok_or_else(|| ObservationError::InvalidRequest {
                reason: format!("no partition {:?}", request.partition),
            })?;
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
            self.options.group_closure,
            &times,
            &fields,
        )?;
        let realization = self.realization(request, entry, rows.len());
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

    fn realization(
        &self,
        request: &AccessRequest,
        entry: &PartitionEntry,
        rows: usize,
    ) -> Realization {
        let samples_per_batch = request.max_samples.min(rows).max(1);
        let channels = request.channels.len();
        let correlations = entry.correlations.len();
        let roles = request.roles;
        let per_sample = channels * correlations;
        let weights = if self.spectral_weights {
            per_sample
        } else {
            correlations
        };
        let scalars = [
            roles.time_centroid,
            roles.interval,
            roles.exposure,
            roles.scan,
        ]
        .into_iter()
        .filter(|wanted| *wanted)
        .count();
        let selected = rows.saturating_mul(
            usize::from(roles.values) * per_sample
                + usize::from(roles.flags) * per_sample
                + usize::from(roles.weights) * weights
                + usize::from(roles.uvw) * 3
                + scalars,
        );
        Realization {
            layout: Layout::SampleMajor,
            factored: false,
            samples_per_batch,
            batches: rows.div_ceil(samples_per_batch),
            batch_payload_bytes: samples_per_batch
                .saturating_mul(per_sample * (8 + 1 + 4) + 3 * 8 + 3 * 8 + scalars * 8),
            decode_units: rows,
            decoded_elements: selected,
            selected_elements: selected,
        }
    }
}

impl Observation for Msv2Observation {
    fn catalogue(&self) -> &Catalogue {
        &self.catalogue
    }

    fn bind_read(&self, request: &AccessRequest) -> Result<BoundCursor<'_>, ObservationError> {
        Ok(Box::new(Msv2Observation::bind_read(self, request)?))
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
    fn header(&self, samples: usize) -> BatchHeader {
        let observation = self.observation;
        let catalogue = &observation.catalogue;
        BatchHeader {
            partition: self.request.partition,
            topology: TopologyShape::Irregular { samples },
            first_channel: self.request.channels.start,
            roles: self.request.roles,
            spectral_weights: observation.spectral_weights,
            row_flags: true,
            time_scale: observation.time_scale,
            time_origin: MjdHighPrec::new(0.0, 0.0),
            frequency_frame: catalogue.partition(self.request.partition).frequency_frame,
            uvw_frame: observation.uvw_frame.effective,
            interval: Sharing::PerSample,
            exposure: Sharing::PerSample,
            scan: Sharing::PerSample,
            limits: IdLimits {
                antennas: catalogue.antennas.len(),
                fields: catalogue.fields.len(),
                scans: catalogue.scans.len(),
            },
        }
    }

    fn fill_batch(
        &mut self,
        buffers: &mut BatchBuffers,
        end: usize,
    ) -> Result<(), ObservationError> {
        let observation = self.observation;
        let main = observation.ms.main_table();
        let entry = observation.catalogue.partition(self.request.partition);
        let rows = &self.rows[self.next..end];
        let capacity = buffers.capacity_bytes();
        let header = self.header(rows.len());
        let mut fill = buffers.begin(header);
        let arrays = fill.arrays();
        arrays.group_name.clone_from(&self.request.data_group);
        arrays.units.clone_from(&entry.data_groups[0].units);
        arrays.correlations.extend_from_slice(&entry.correlations);
        arrays
            .frequency_hz
            .extend_from_slice(&entry.frequencies_hz[self.request.channels.clone()]);
        arrays
            .time_offsets_s
            .extend_from_slice(&self.times[self.next..end]);
        arrays.field.extend_from_slice(&self.fields[self.next..end]);
        let roles = self.request.roles;
        if roles.scan {
            arrays
                .scan
                .extend(rows.iter().map(|&row| observation.row_scans[row]));
        }
        fill_scalars(
            main,
            rows,
            roles,
            &mut self.antenna1,
            &mut self.antenna2,
            arrays,
        )?;
        self.counters.read_calls += 1;
        // The contract's antennas are unsigned; casacore's are Int32.
        for (stored, ids) in [
            (&self.antenna1, &mut arrays.antenna1),
            (&self.antenna2, &mut arrays.antenna2),
        ] {
            for &antenna in stored {
                ids.push(
                    u32::try_from(antenna)
                        .map_err(|_| invalid(format!("negative antenna {antenna}")))?,
                );
            }
        }
        self.counters.copied_elements += 2 * rows.len();
        self.counters.read_calls += fill_arrays(
            main,
            rows,
            &self.request,
            observation.spectral_weights,
            arrays,
        )?;
        if let Some(groups) = self.groups.as_mut() {
            groups.fragments(self.next..end, 1, &mut arrays.fragments);
        }
        self.counters.delivered_bytes += arrays.values.len() * size_of::<num_complex::Complex32>()
            + arrays.flags.len()
            + arrays.weights.len() * size_of::<f32>()
            + arrays.uvw.len() * size_of::<f64>();
        fill.finish()?;
        if buffers.capacity_bytes() > capacity {
            self.counters.buffer_growths += 1;
        }
        Ok(())
    }
}

/// MAIN's scalar columns for `rows`, in one call: the antennas, `FLAG_ROW`
/// and the requested time auxiliaries.
fn fill_scalars(
    main: &casa_tables::Table,
    rows: &[usize],
    roles: Roles,
    antenna1: &mut Vec<i32>,
    antenna2: &mut Vec<i32>,
    arrays: &mut casa_observation::BatchArrays,
) -> Result<(), ObservationError> {
    let mut destinations = vec![
        RequiredScalarColumnDestination::new(
            "ANTENNA1",
            RequiredScalarColumnValuesMut::Int32(antenna1),
        ),
        RequiredScalarColumnDestination::new(
            "ANTENNA2",
            RequiredScalarColumnValuesMut::Int32(antenna2),
        ),
        RequiredScalarColumnDestination::new(
            "FLAG_ROW",
            RequiredScalarColumnValuesMut::Bool(&mut arrays.row_flags),
        ),
    ];
    for (wanted, column, values) in [
        (
            roles.time_centroid,
            "TIME_CENTROID",
            &mut arrays.centroid_offsets_s,
        ),
        (roles.interval, "INTERVAL", &mut arrays.interval_s),
        (roles.exposure, "EXPOSURE", &mut arrays.exposure_s),
    ] {
        if wanted {
            destinations.push(RequiredScalarColumnDestination::new(
                column,
                RequiredScalarColumnValuesMut::Float64(values),
            ));
        }
    }
    main.required_scalar_columns_for_rows_into(rows, &mut destinations)
        .map_err(table_error)
}

/// MAIN's array columns for `rows`: the payload roles of `request`, read
/// straight into the batch arrays by the typed fills. Returns the read
/// calls issued.
fn fill_arrays(
    main: &casa_tables::Table,
    rows: &[usize],
    request: &AccessRequest,
    spectral_weights: bool,
    arrays: &mut casa_observation::BatchArrays,
) -> Result<usize, ObservationError> {
    let channel_start = request.channels.start;
    let channel_count = request.channels.len();
    let roles = request.roles;
    let mut calls = 0;
    let channel_range = |column: &str, destination: SelectedArray2DCellsMut<'_>| {
        main.column_accessor(column)
            .map_err(table_error)?
            .fill_array_cells_2d_channel_range_typed_uncached(
                rows,
                channel_start,
                channel_count,
                destination,
            )
            .map_err(table_error)?
            .ok_or_else(|| invalid(format!("undefined {column} cells")))
    };
    if roles.values {
        channel_range(
            "DATA",
            SelectedArray2DCellsMut::RowChannelComplex32(&mut arrays.values),
        )?;
        calls += 1;
    }
    if roles.flags {
        channel_range(
            "FLAG",
            SelectedArray2DCellsMut::RowChannelBool(&mut arrays.flags),
        )?;
        calls += 1;
    }
    if roles.weights {
        if spectral_weights {
            channel_range(
                "WEIGHT_SPECTRUM",
                SelectedArray2DCellsMut::RowChannelFloat32(&mut arrays.weights),
            )?;
        } else {
            main.column_accessor("WEIGHT")
                .map_err(table_error)?
                .fill_array_cells_1d_typed_uncached(
                    rows,
                    SelectedArray1DCellsMut::Float32(&mut arrays.weights),
                )
                .map_err(table_error)?;
        }
        calls += 1;
    }
    if roles.uvw {
        main.column_accessor("UVW")
            .map_err(table_error)?
            .fill_array_cells_1d_typed_uncached(
                rows,
                SelectedArray1DCellsMut::Float64(&mut arrays.uvw),
            )
            .map_err(table_error)?;
        calls += 1;
    }
    Ok(calls)
}

impl VisibilityCursor for Msv2Cursor<'_> {
    fn realization(&self) -> &Realization {
        &self.realization
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<Option<Fill>, ObservationError> {
        if let Some(key) = self.groups.as_mut().and_then(GroupIndex::pop_end) {
            buffers.clear();
            return Ok(Some(Fill::GroupEnd(key)));
        }
        if self.next == self.rows.len() {
            return Ok(None);
        }
        let end = (self.next + self.realization.samples_per_batch).min(self.rows.len());
        self.fill_batch(buffers, end)?;
        self.counters.batches += 1;
        self.counters.decode_units += end - self.next;
        self.next = end;
        Ok(Some(Fill::Batch))
    }

    fn counters(&self) -> IoCounters {
        self.counters
    }
}
