// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MeasurementSet v2 adapter: MAIN rows assembled onto the time ×
//! baseline grid of their partition at opening, delivered as the batches
//! MSv4 holds natively.

mod catalogue;
mod cursor;
mod index;

#[cfg(feature = "msv4")]
pub(crate) use catalogue::{metres, radians};
pub use cursor::Msv2Cursor;

use std::path::Path;

use casa_observation::{
    AccessRequest, BoundCursor, Catalogue, DataGroupEntry, Estimate, NaturalBlocks, Observation,
    ObservationError, ObservationId, PartitionEntry, PartitionEstimate, PartitionId, Traversal,
    times_per_batch,
};
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
}

/// An opened MeasurementSet v2 and its catalogue.
///
/// Opening reads MAIN's key columns in row chunks and partitions the rows the way
/// XRADIO's converter does (`DATA_DESC_ID` × `OBS_MODE` × `OBSERVATION_ID`
/// × `EPHEMERIS_ID`, the feed pair, and the opening's
/// [`PartitionScheme`](super::PartitionScheme)); each partition's rows are
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
    catalogue: Catalogue,
    partitions: Vec<Partition>,
    spectral_weights: bool,
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
        let units = catalogue::data_units(main);
        let setups = catalogue::spectral_setups(&ms)?;
        let modes = catalogue::read_state_modes(&ms)?;
        let spectral_weights = main
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"));
        let index = Index::build(
            main,
            options.partition_scheme,
            &modes,
            &ephemerides,
            ROWS_PER_CHUNK,
        )?;
        let group = DataGroupEntry {
            name: "base".to_string(),
            visibilities: "DATA".to_string(),
            flags: "FLAG".to_string(),
            weights: if spectral_weights {
                "WEIGHT_SPECTRUM"
            } else {
                "WEIGHT"
            }
            .to_string(),
            uvw: "UVW".to_string(),
            units,
            uvw_frame,
        };
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
                baselines: grid.baselines.clone(),
                time_scale,
                time_origin: MjdHighPrec::new(0.0, 0.0),
                data_groups: vec![group.clone()],
            });
            partitions.push(Partition { grid });
        }
        Ok(Self {
            catalogue: Catalogue {
                antennas,
                fields,
                scans: index.scans,
                observations,
                partitions: entries,
            },
            partitions,
            spectral_weights,
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
    ) -> Result<PartitionEstimate, ObservationError> {
        let (partition, entry) = self.partition(id)?;
        let resolved = request.resolve(entry)?;
        let grid = &partition.grid;
        let roles = request.roles;
        let channels = request.channels.len();
        let (delivered, stored) = (resolved.correlations.len(), entry.correlations.len());
        let scalars = [roles.time_centroid, roles.interval, roles.exposure]
            .into_iter()
            .filter(|wanted| *wanted)
            .count();
        let bytes_per_sample =
            roles.sample_bytes(channels, delivered, self.spectral_weights, scalars);
        let per_sample = |correlations: usize| {
            usize::from(roles.values) * channels * correlations
                + usize::from(roles.flags) * channels * correlations
                + usize::from(roles.weights)
                    * if self.spectral_weights {
                        channels * correlations
                    } else {
                        correlations
                    }
                + usize::from(roles.uvw) * 3
                + scalars
        };
        let (times, baselines) = (grid.times(), grid.baselines.len());
        let times_per_batch =
            times_per_batch(request.batch_bytes, bytes_per_sample, baselines, times)?;
        let group = &entry.data_groups[resolved.data_group];
        let tile = self
            .ms
            .main_table()
            .array_column_tile_shape(&group.visibilities)
            .map_err(table_error)?;
        Ok(PartitionEstimate {
            partition: id,
            times,
            baselines,
            bytes_per_sample,
            times_per_batch,
            batches: if times_per_batch == 0 {
                0
            } else {
                times.div_ceil(times_per_batch)
            },
            decode_units: grid.rows,
            decoded_elements: grid.rows * per_sample(stored),
            selected_elements: grid.rows * per_sample(delivered),
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
            padded_fraction: grid.padded_fraction(),
        })
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
                .map(|&id| self.estimate_partition(request, id))
                .collect::<Result<_, _>>()?,
        })
    }

    /// Bind a traversal of `request`: one cursor per partition, whole times
    /// per batch ([`Observation::bind_read`]).
    ///
    /// # Errors
    ///
    /// As [`Self::estimate`].
    pub fn bind_read(&self, request: &AccessRequest) -> Result<Traversal<'_>, ObservationError> {
        let estimate = self.estimate(request)?;
        let cursors = estimate
            .partitions
            .iter()
            .map(|estimate| {
                Msv2Cursor::bind(self, request, estimate.clone())
                    .map(|cursor| Box::new(cursor) as BoundCursor<'_>)
            })
            .collect::<Result<_, _>>()?;
        Ok(Traversal::new(cursors, estimate))
    }
}

impl Observation for Msv2Observation {
    fn catalogue(&self) -> &Catalogue {
        &self.catalogue
    }

    fn estimate(&self, request: &AccessRequest) -> Result<Estimate, ObservationError> {
        Msv2Observation::estimate(self, request)
    }

    fn bind_read(&self, request: &AccessRequest) -> Result<Traversal<'_>, ObservationError> {
        Msv2Observation::bind_read(self, request)
    }
}
