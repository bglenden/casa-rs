// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MeasurementSet v2 adapter: MAIN rows assembled onto the time ×
//! baseline grid of their partition at opening, delivered as the batches
//! MSv4 holds natively.

pub(super) mod catalogue;
mod cursor;
mod index;
pub(super) mod update;

#[cfg(feature = "msv4")]
pub(crate) use catalogue::{metres, radians};
pub use cursor::Msv2Cursor;

use std::path::Path;
use std::sync::Arc;

use casa_observation::{
    AccessRequest, BoundCursor, Catalogue, DataGroupEntry, Estimate, NaturalBlocks, Observation,
    ObservationError, ObservationId, PartitionEntry, PartitionEstimate, PartitionId,
    ResolvedRequest, Roles, TimeBaselineGrid, Traversal, WeightRequest, WeightRule, WeightSource,
    times_per_batch,
};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};
use casa_types::measures::{EpochRef, MjdHighPrec};

use super::OpenOptions;
use crate::{MeasurementSet, MsError};
use index::{Grid, Index, ROWS_PER_CHUNK};

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

/// One partition of the MeasurementSet: its rows on their grid.
#[derive(Debug)]
struct Partition {
    grid: Grid,
    /// The `SPECTRAL_WINDOW` row of its data description.
    spectral_window_id: u32,
}

/// An opened MeasurementSet v2 and its catalogue.
///
/// Opening reads MAIN's key columns in row chunks and partitions the rows the way
/// XRADIO's converter does (`DATA_DESC_ID` × `OBS_MODE` × `OBSERVATION_ID`
/// × `EPHEMERIS_ID` and the opening's
/// [`PartitionScheme`](super::PartitionScheme)), and further by the feed
/// pair, which XRADIO does not split by. A row whose `STATE_ID` is -1 has
/// no intents here, where XRADIO's converter gives it the last `STATE`
/// row's `OBS_MODE` (NumPy's index -1), so an MS mixing such rows with
/// `STATE` rows partitions differently. Each partition's rows are
/// placed on its time × baseline grid (the `index` module). Each partition
/// has one data group, `base`, associating `DATA`, `FLAG`,
/// `WEIGHT_SPECTRUM` (when MAIN has it, else `WEIGHT`) and `UVW`. Antenna,
/// field and observation identities are the subtable rows; scans are the
/// distinct `SCAN_NUMBER`s in ascending order.
///
/// Opening checks every unit and measure reference the batches rely on:
/// `TIME` and `TIME_CENTROID` share one fixed reference (a variable
/// reference is refused); the UVW convention is resolved to the phase
/// centres' frame ([`UvwFrame`](casa_observation::UvwFrame)); positions,
/// diameters, angles and frequencies are in the units their types claim.
pub struct Msv2Observation {
    ms: MeasurementSet,
    catalogue: Arc<Catalogue>,
    partitions: Vec<Partition>,
    spectral_weights: bool,
    /// Whether MAIN has `SIGMA_SPECTRUM`.
    spectral_sigmas: bool,
    options: OpenOptions,
}

impl Msv2Observation {
    /// Open the MeasurementSet at `path` and read its catalogue.
    ///
    /// The tables stay open with casacore read locks, and their payload
    /// rows stay on disk: the typed column fills read only the selected
    /// cells, as the production selected-observation source does.
    ///
    /// # Errors
    ///
    /// When the MeasurementSet cannot be opened, a subtable the catalogue
    /// needs is missing or malformed, a unit or measure reference is
    /// outside the trial profile, or the rows do not fit their grids (two
    /// rows in one cell, two fields or scans at one time, several
    /// `PROCESSOR_ID`s in one partition).
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
        let catalogue::Fields {
            entries: fields,
            ephemerides,
            uvw_frame,
        } = catalogue::fields_and_uvw(&ms)?;
        let observations = catalogue::read_observations(&ms)?;
        let main = ms.main_table();
        let time_scale = catalogue::time_scale(main)?;
        let setups = catalogue::spectral_setups(&ms)?;
        let modes = catalogue::read_state_modes(&ms)?;
        let has = |column: &str| {
            main.schema()
                .is_some_and(|schema| schema.contains_column(column))
        };
        let (spectral_weights, spectral_sigmas) = (has("WEIGHT_SPECTRUM"), has("SIGMA_SPECTRUM"));
        let index = Index::build(
            main,
            options.partition_scheme,
            &modes,
            &ephemerides,
            ROWS_PER_CHUNK,
        )?;
        let groups = data_groups(main, spectral_weights, uvw_frame);
        let mut partitions = Vec::new();
        let mut entries = Vec::new();
        for (key, grid) in index.partitions {
            let setup = usize::try_from(key.data_description)
                .ok()
                .and_then(|row| setups.get(row))
                .ok_or_else(|| {
                    invalid(format!(
                        "DATA_DESC_ID {} is not in DATA_DESCRIPTION",
                        key.data_description
                    ))
                })?;
            let observation = u32::try_from(key.observation)
                .ok()
                .filter(|id| (*id as usize) < observations.len())
                .map(ObservationId)
                .ok_or_else(|| {
                    invalid(format!(
                        "OBSERVATION_ID {} is not in OBSERVATION",
                        key.observation
                    ))
                })?;
            let feed = |feed: i32| {
                u32::try_from(feed).map_err(|_| invalid(format!("negative feed {feed}")))
            };
            let feeds = [feed(key.feeds[0])?, feed(key.feeds[1])?];
            entries.push(PartitionEntry {
                spectral_window: setup.spectral_window.clone(),
                frequencies_hz: setup.frequencies_hz.clone(),
                frequency_frame: setup.frequency_frame,
                channel_widths_hz: setup.channel_widths_hz.clone(),
                correlations: setup.correlations.clone(),
                intents: catalogue::intents(&key.obs_mode),
                observation,
                feeds: Some(feeds),
                fields: super::distinct(&grid.field_per_time),
                scans: super::distinct(&grid.scan_per_time),
                baselines: grid.baselines.clone(),
                time_scale,
                time_origin: MjdHighPrec::new(0.0, 0.0),
                data_groups: groups.clone(),
            });
            partitions.push(Partition {
                grid,
                spectral_window_id: setup.spectral_window_id,
            });
        }
        Ok(Self {
            catalogue: Arc::new(Catalogue {
                antennas,
                fields,
                scans: index.scans,
                observations,
                partitions: entries,
            }),
            partitions,
            spectral_weights,
            spectral_sigmas,
            options: options.clone(),
            ms,
        })
    }

    /// The catalogue.
    #[must_use]
    pub fn catalogue(&self) -> &Arc<Catalogue> {
        &self.catalogue
    }

    /// The MeasurementSet it reads.
    pub(crate) fn into_measurement_set(self) -> MeasurementSet {
        self.ms
    }

    /// The `SPECTRAL_WINDOW` row of partition `id`'s data description:
    /// the identifier XRADIO's converter names an MSv2's spectral window by
    /// (`NAME_<id>`), which the format-neutral catalogue does not carry.
    ///
    /// # Panics
    ///
    /// If `id` is not a partition of the observation.
    #[must_use]
    pub fn spectral_window_id(&self, id: PartitionId) -> u32 {
        self.partitions[id.0 as usize].spectral_window_id
    }

    /// The first MAIN row of partition `id`, `None` when it holds none:
    /// XRADIO's converter numbers an MSv2's partitions in the order their
    /// first rows appear.
    ///
    /// # Panics
    ///
    /// If `id` is not a partition of the observation.
    #[must_use]
    pub fn first_row(&self, id: PartitionId) -> Option<u32> {
        self.partitions[id.0 as usize]
            .grid
            .cells
            .iter()
            .copied()
            .filter(|row| *row != index::NO_ROW)
            .min()
    }

    /// The MeasurementSet.
    #[must_use]
    pub fn measurement_set(&self) -> &MeasurementSet {
        &self.ms
    }

    /// The time scale of MAIN `TIME` (every partition's).
    #[must_use]
    pub fn time_scale(&self) -> EpochRef {
        self.catalogue
            .partitions
            .first()
            .map_or(EpochRef::UTC, |partition| partition.time_scale)
    }

    fn partition(
        &self,
        id: PartitionId,
    ) -> Result<(&Partition, &PartitionEntry), ObservationError> {
        match (
            self.partitions.get(id.0 as usize),
            self.catalogue.partitions.get(id.0 as usize),
        ) {
            (Some(partition), Some(entry)) => Ok((partition, entry)),
            _ => Err(ObservationError::InvalidRequest {
                reason: format!("no partition {id:?}"),
            }),
        }
    }

    /// What `request` costs on partition `id`, from the grid and the tile
    /// shape: every MSv2 role is a required column, so binding refuses
    /// nothing the request can ask for beyond the request's own
    /// consistency.
    fn estimate_partition(
        &self,
        request: &AccessRequest,
        id: PartitionId,
    ) -> Result<(PartitionEstimate, ResolvedRequest), ObservationError> {
        let (partition, entry) = self.partition(id)?;
        let resolved = request.resolve(entry, &partition.grid.times_s)?;
        let roles = request.roles;
        let weights = weight_source(
            request.weights,
            &entry.data_groups[resolved.data_group],
            self.spectral_weights,
            self.spectral_sigmas,
        );
        let per_channel = weights.per_channel;
        let channels = request.channels.len();
        let (delivered, stored) = (
            resolved.correlations.products(entry).len(),
            entry.correlations.len(),
        );
        let scalars = [roles.time_centroid, roles.interval, roles.exposure]
            .into_iter()
            .filter(|wanted| *wanted)
            .count();
        let bytes_per_sample = roles.sample_bytes(channels, delivered, per_channel, scalars);
        let grid = &partition.grid;
        // The cursor's scratch: each present row's number, sample, FLAG_ROW
        // and time auxiliaries; and, when a subset is delivered, every stored
        // product of the cube roles, read before the delivered ones are
        // gathered. Padding needs none: rows are spread in place.
        let gathered = Roles {
            uvw: false,
            ..roles
        };
        let scratch_bytes_per_sample = 2 * size_of::<usize>()
            + usize::from(roles.flags)
            + 8 * scalars
            + if resolved.correlations.indices().is_some() {
                gathered.sample_bytes(channels, stored, per_channel, 0)
            } else {
                0
            };
        let per_sample = |correlations: usize| {
            usize::from(roles.values) * channels * correlations
                + usize::from(roles.flags) * channels * correlations
                + usize::from(roles.weights)
                    * if per_channel {
                        channels * correlations
                    } else {
                        correlations
                    }
                + usize::from(roles.uvw) * 3
                + scalars
        };
        let (times, baselines) = (resolved.times.len(), grid.baselines.len());
        // The selection's rows: its cells that a row fills.
        let cells = grid.cells(resolved.times.clone());
        let rows = cells.iter().filter(|row| **row != index::NO_ROW).count();
        let bytes_per_time = roles.time_bytes(0);
        let times_per_batch = times_per_batch(
            request.batch_bytes,
            baselines * (bytes_per_sample + scratch_bytes_per_sample) + bytes_per_time,
            times,
        )?;
        let group = &entry.data_groups[resolved.data_group];
        let tile = self
            .ms
            .main_table()
            .array_column_tile_shape(&group.visibilities)
            .map_err(table_error)?;
        let estimate = PartitionEstimate {
            partition: id,
            times,
            first_time: resolved.times.start,
            weights: roles.weights.then_some(weights),
            baselines,
            bytes_per_sample,
            bytes_per_time,
            scratch_bytes_per_sample,
            times_per_batch,
            batches: if times_per_batch == 0 {
                0
            } else {
                times.div_ceil(times_per_batch)
            },
            decode_units: rows,
            decoded_elements: rows * per_sample(stored),
            selected_elements: rows * per_sample(delivered),
            natural_blocks: match tile.as_deref() {
                // A cell tile is [correlation, channel, rows].
                Some([_, channels, rows, ..]) => NaturalBlocks {
                    times: (*rows / baselines.max(1)).max(1),
                    channels: *channels,
                },
                _ => NaturalBlocks {
                    times: 1,
                    channels: entry.frequencies_hz.len(),
                },
            },
            padded_fraction: if cells.is_empty() {
                0.0
            } else {
                1.0 - rows as f64 / cells.len() as f64
            },
        };
        Ok((estimate, resolved))
    }

    /// What `request` costs ([`Observation::estimate`]).
    ///
    /// # Errors
    ///
    /// When the request names an unknown partition, data group, channel or
    /// correlation product, or budgets less than one time.
    pub fn estimate(&self, request: &AccessRequest) -> Result<Estimate, ObservationError> {
        Ok(Estimate {
            partitions: request
                .partitions
                .iter()
                .map(|&id| Ok(self.estimate_partition(request, id)?.0))
                .collect::<Result<_, ObservationError>>()?,
        })
    }

    /// Bind a traversal of `request`: one cursor per partition, whole times
    /// per batch ([`Observation::bind_read`]).
    ///
    /// # Errors
    ///
    /// As [`Self::estimate`].
    pub fn bind_read(&self, request: &AccessRequest) -> Result<Traversal<'_>, ObservationError> {
        let bound = request
            .partitions
            .iter()
            .map(|&id| self.estimate_partition(request, id))
            .collect::<Result<Vec<_>, _>>()?;
        let estimate = Estimate {
            partitions: bound.iter().map(|(estimate, _)| estimate.clone()).collect(),
        };
        let cursors = bound
            .into_iter()
            .map(|(estimate, resolved)| {
                Msv2Cursor::bind(self, request, estimate, resolved)
                    .map(|cursor| Box::new(cursor) as BoundCursor<'_>)
            })
            .collect::<Result<_, _>>()?;
        Ok(Traversal::new(cursors, estimate))
    }

    /// Partition `id`'s grid ([`Observation::grid`]): its times, fields and
    /// scans from the opening index, and the one `INTERVAL` of its rows,
    /// read in bounded row chunks. Padding has no row, so no interval.
    ///
    /// # Errors
    ///
    /// As [`Observation::grid`].
    pub fn grid(
        &self,
        id: PartitionId,
        data_group: &str,
    ) -> Result<TimeBaselineGrid, ObservationError> {
        let (partition, entry) = self.partition(id)?;
        if entry.data_group(data_group).is_none() {
            return Err(ObservationError::InvalidRequest {
                reason: format!("no data group {data_group} in {id:?}"),
            });
        }
        let grid = &partition.grid;
        let rows: Vec<usize> = grid
            .cells
            .iter()
            .filter(|&&row| row != index::NO_ROW)
            .map(|&row| row as usize)
            .collect();
        let main = self.ms.main_table();
        let mut interval = None;
        let mut values = Vec::new();
        for chunk in rows.chunks(ROWS_PER_CHUNK) {
            values.clear();
            main.required_scalar_columns_for_rows_into(
                chunk,
                &mut [RequiredScalarColumnDestination::new(
                    "INTERVAL",
                    RequiredScalarColumnValuesMut::Float64(&mut values),
                )],
            )
            .map_err(table_error)?;
            for value in &values {
                if *interval.get_or_insert(*value) != *value {
                    return Err(ObservationError::Unsupported {
                        what: "a grid with more than one integration interval",
                    });
                }
            }
        }
        Ok(TimeBaselineGrid {
            baselines: grid.baselines.clone(),
            time_scale: entry.time_scale,
            time_origin: entry.time_origin,
            times_s: grid.times_s.clone(),
            field_per_time: grid.field_per_time.clone(),
            scan_per_time: grid.scan_per_time.clone(),
            interval_s: interval.ok_or_else(|| ObservationError::InvalidRequest {
                reason: format!("{id:?} has no rows"),
            })?,
        })
    }
}

impl Observation for Msv2Observation {
    fn catalogue(&self) -> &Arc<Catalogue> {
        &self.catalogue
    }

    fn estimate(&self, request: &AccessRequest) -> Result<Estimate, ObservationError> {
        Msv2Observation::estimate(self, request)
    }

    fn bind_read(&self, request: &AccessRequest) -> Result<Traversal<'_>, ObservationError> {
        Msv2Observation::bind_read(self, request)
    }

    fn grid(
        &self,
        id: PartitionId,
        data_group: &str,
    ) -> Result<TimeBaselineGrid, ObservationError> {
        Msv2Observation::grid(self, id, data_group)
    }
}

/// What the weights role of data group `group` delivers under `request`,
/// on a MAIN with or without `WEIGHT_SPECTRUM` and `SIGMA_SPECTRUM`: the
/// group's stored weights, or 1/σ² from `SIGMA_SPECTRUM` when MAIN has both
/// it and `WEIGHT_SPECTRUM` (CASA's rule, `VisBufferImpl2`), else from
/// `SIGMA`. Every MSv2 has `SIGMA`, so no fallback is taken.
fn weight_source(
    request: WeightRequest,
    group: &DataGroupEntry,
    spectral_weights: bool,
    spectral_sigmas: bool,
) -> WeightSource {
    match request {
        WeightRequest::Stored => WeightSource {
            rule: WeightRule::Stored,
            variable: group.weights.clone(),
            per_channel: spectral_weights,
        },
        WeightRequest::InverseSigmaSquared { .. } => {
            let per_channel = spectral_weights && spectral_sigmas;
            WeightSource {
                rule: WeightRule::InverseSigmaSquared,
                variable: if per_channel {
                    "SIGMA_SPECTRUM"
                } else {
                    "SIGMA"
                }
                .to_string(),
                per_channel,
            }
        }
    }
}

/// The data groups MAIN's visibility columns form, named as XRADIO's
/// converter names them: `base` (`DATA`), and `corrected`
/// (`CORRECTED_DATA`) and `model` (`MODEL_DATA`) where the columns exist.
/// Every group reads the same flags, weights and UVW.
pub(super) fn data_groups(
    main: &casa_tables::Table,
    spectral_weights: bool,
    uvw_frame: casa_observation::UvwFrame,
) -> Vec<DataGroupEntry> {
    let weights = if spectral_weights {
        "WEIGHT_SPECTRUM"
    } else {
        "WEIGHT"
    };
    let has = |column: &str| {
        main.schema()
            .is_some_and(|schema| schema.contains_column(column))
    };
    [
        ("base", "DATA"),
        ("corrected", "CORRECTED_DATA"),
        ("model", "MODEL_DATA"),
    ]
    .into_iter()
    .filter(|(name, column)| *name == "base" || has(column))
    .map(|(name, column)| DataGroupEntry {
        name: name.to_string(),
        visibilities: column.to_string(),
        flags: "FLAG".to_string(),
        weights: weights.to_string(),
        uvw: "UVW".to_string(),
        units: catalogue::data_units(main, column),
        uvw_frame,
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use casa_observation::{
        DataGroupEntry, SigmaFallback, UvwFrame, VisibilityUnits, WeightRequest, WeightRule,
    };

    use casa_types::measures::direction::DirectionRef;

    use super::weight_source;

    /// CASA's rule: per-channel sigmas only when MAIN has both
    /// `SIGMA_SPECTRUM` and `WEIGHT_SPECTRUM`, else `SIGMA`; stored weights
    /// are the group's.
    #[test]
    fn sigma_weights_follow_casas_column_rule() {
        let group = DataGroupEntry {
            name: "base".to_string(),
            visibilities: "DATA".to_string(),
            flags: "FLAG".to_string(),
            weights: "WEIGHT_SPECTRUM".to_string(),
            uvw: "UVW".to_string(),
            units: VisibilityUnits::Undeclared,
            uvw_frame: UvwFrame {
                effective: DirectionRef::J2000,
                stored_label: None,
            },
        };
        let sigma = WeightRequest::InverseSigmaSquared {
            fallback: SigmaFallback::Refuse,
        };
        let cases = [
            (true, true, "SIGMA_SPECTRUM", true),
            (true, false, "SIGMA", false),
            (false, true, "SIGMA", false),
            (false, false, "SIGMA", false),
        ];
        for (weights, sigmas, variable, per_channel) in cases {
            let source = weight_source(sigma, &group, weights, sigmas);
            assert_eq!(source.rule, WeightRule::InverseSigmaSquared);
            assert_eq!(
                (source.variable.as_str(), source.per_channel),
                (variable, per_channel),
                "WEIGHT_SPECTRUM {weights}, SIGMA_SPECTRUM {sigmas}"
            );
        }
        let stored = weight_source(WeightRequest::Stored, &group, true, true);
        assert_eq!(
            (stored.rule, stored.variable.as_str(), stored.per_channel),
            (WeightRule::Stored, "WEIGHT_SPECTRUM", true)
        );
    }
}
