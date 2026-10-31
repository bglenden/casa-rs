// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MeasurementSet v2 adapter: MAIN rows assembled onto the time ×
//! baseline grid of their partition at opening, delivered as the batches
//! MSv4 holds natively.

pub(super) mod catalogue;
mod cursor;
pub(super) mod held;
mod index;
mod metadata;
mod pointing;
pub(super) mod update;

#[cfg(feature = "msv4")]
pub(crate) use catalogue::{metres, radians};
pub use cursor::Msv2Cursor;
#[cfg(unix)]
pub(crate) use pointing::{POLYNOMIAL_TERMS, scan_rows};

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use casa_observation::{
    AccessRequest, AntennaId, BoundCursor, Catalogue, DataGroupEntry, Estimate, FieldId,
    FieldSelection, GroupKey, Grouping, NaturalBlocks, Observation, ObservationError,
    ObservationId, OrderedBaseline, PartitionEntry, PartitionEstimate, PartitionId,
    PointingRequest, PointingSeries, ProcessorId, Receptors, ResolvedRequest, Roles, ScanId,
    StateId, TimeBaselineGrid, Traversal, UvwFrame, WeightRequest, WeightRule, WeightSource,
    WindowId, times_per_batch,
};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};
use casa_types::measures::{EpochRef, MjdHighPrec};

use super::{OpenOptions, heap};
use crate::{MeasurementSet, MsError};
use index::{Grid, Index, ROWS_PER_CHUNK};

/// The most rows [`Msv2Observation::grid`] reads at once.
pub(crate) const GRID_READ_ROWS: usize = ROWS_PER_CHUNK;

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
    /// The `STATE::OBS_MODE` its rows share; empty for rows without a
    /// state.
    obs_mode: String,
    /// Its `DATA_DESCRIPTION` row.
    data_description_id: u32,
    /// The `SPECTRAL_WINDOW` row of its data description.
    spectral_window_id: u32,
    /// Whether its weights are `WEIGHT_SPECTRUM` ([`has_content`] at its
    /// first row): CASA's iterator asks again at each new spectral window,
    /// on the first row it meets there.
    spectral_weights: bool,
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
/// `WEIGHT_SPECTRUM` (when its first row has content, else `WEIGHT`) and
/// `UVW`. Antenna,
/// field and observation identities are the subtable rows; scans are the
/// distinct `SCAN_NUMBER`s in ascending order.
///
/// Opening checks every unit and measure reference the batches rely on:
/// `TIME` and `TIME_CENTROID` share one fixed reference (a variable
/// reference is refused); the UVW convention is resolved to the phase
/// centres' frame ([`UvwFrame`]); positions,
/// diameters, angles and frequencies are in the units their types claim.
pub struct Msv2Observation {
    ms: MeasurementSet,
    catalogue: Arc<Catalogue>,
    partitions: Vec<Partition>,
    /// Each `STATE` row's `OBS_MODE`.
    state_modes: Vec<String>,
    /// The `STATE_ID`s MAIN's rows hold.
    main_states: BTreeSet<i32>,
    /// Whether MAIN's first row has `SIGMA_SPECTRUM` content: CASA's
    /// iterator asks once per MeasurementSet.
    spectral_sigmas: bool,
    /// Each data group's visibility column and its tile shape, read when
    /// the observation opened: an estimate does not read `table.dat` again,
    /// which an update session over the same MeasurementSet rewrites as it
    /// saves each batch.
    tile_shapes: Vec<(String, Option<Vec<usize>>)>,
    options: OpenOptions,
    /// The most its open tables hold: their descriptions and keywords
    /// ([`held::opening_bytes`]) and the subtables' rows the opening loaded
    /// ([`held::read_bytes`]).
    tables_bytes: usize,
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
        Self::open_granted(path, options, &mut |_| Ok(()))
    }

    /// [`Self::open_with`], asking `grant` for the bytes the opening will
    /// hold before it allocates them: its tables' descriptions and keywords
    /// (bounded by their files), the rows of the subtables its catalogue is
    /// read from (a table loads every row on first touch and keeps them)
    /// and what is read from them, then its index (its chunk of MAIN's key
    /// columns, each partition's distinct times, baselines and processors as
    /// the first pass finds them, at most one row's beyond what was granted,
    /// then the scans and the grids) and its partitions' entries. The
    /// totals asked for only grow, a few at a time; a caller that admits
    /// memory admits each increase. An error from `grant` refuses the
    /// opening with nothing more allocated.
    /// Temporary payload staging in the spectral-weight content probes is
    /// not yet separately charged; issue #1041 tracks that inherited gap.
    ///
    /// # Errors
    ///
    /// As [`Self::open`], and whatever `grant` returns.
    #[cfg(unix)]
    pub fn open_granted(
        path: impl AsRef<Path>,
        options: &OpenOptions,
        grant: &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    ) -> Result<Self, ObservationError> {
        let described = held::opening_bytes(path.as_ref());
        grant(described)?;
        let ms = MeasurementSet::open_retained_read(path).map_err(storage)?;
        Self::open_described_on(ms, options, described, grant)
    }

    /// The entities `ms`'s subtables state, read as opening reads them:
    /// antennas, fields, windows, sources, states, processors and observing
    /// sessions, and the frame of every data group's UVW that MAIN's `UVW`
    /// keywords give. Scans and partitions, which MAIN's rows give, are
    /// empty. How a creation plan states what reopening its MeasurementSet
    /// gives.
    ///
    /// # Errors
    ///
    /// As [`Self::open`], for the subtables.
    pub(crate) fn subtable_catalogue(
        ms: &MeasurementSet,
    ) -> Result<(Catalogue, UvwFrame), ObservationError> {
        let windows = metadata::read_windows(ms)?;
        let sources = metadata::read_sources(ms, windows.len())?;
        let catalogue::Fields {
            entries: fields,
            uvw_frame,
            ..
        } = catalogue::fields_and_uvw(ms, &sources.ids)?;
        let subtables = Catalogue {
            antennas: catalogue::read_antennas(ms)?,
            fields,
            scans: Vec::new(),
            observations: catalogue::read_observations(ms)?,
            partitions: Vec::new(),
            windows,
            sources: sources.entries,
            states: metadata::read_states(ms)?,
            processors: metadata::read_processors(ms)?,
        };
        Ok((subtables, uvw_frame))
    }

    /// [`Self::open_granted`] on `ms`, a MeasurementSet already open for a
    /// retained read ([`MeasurementSet::open_retained_read`]), so that a
    /// caller who reads MAIN's row count to admit the opening opens it once.
    /// The caller must admit [`Self::opening_table_bytes`] before opening
    /// `ms`; this method includes those bytes in each subsequent grant.
    ///
    /// # Errors
    ///
    /// As [`Self::open_granted`].
    pub fn open_granted_on(
        ms: MeasurementSet,
        options: &OpenOptions,
        grant: &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    ) -> Result<Self, ObservationError> {
        let described = ms.path().map_or(0, held::opening_bytes);
        grant(described)?;
        Self::open_described_on(ms, options, described, grant)
    }

    fn open_described_on(
        ms: MeasurementSet,
        options: &OpenOptions,
        described: usize,
        grant: &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    ) -> Result<Self, ObservationError> {
        let mut observation =
            Self::from_measurement_set(ms, options, &mut |total| grant(described + total))?;
        observation.tables_bytes += described;
        Ok(observation)
    }

    fn from_measurement_set(
        ms: MeasurementSet,
        options: &OpenOptions,
        grant: &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    ) -> Result<Self, ObservationError> {
        // The rows the catalogue's readers load stay loaded while the
        // observation is open; the entities read from them copy at most as
        // much again.
        let tables = held::read_bytes(&ms);
        grant(2 * tables)?;
        let antennas = catalogue::read_antennas(&ms)?;
        let feeds = catalogue::read_feeds(&ms)?;
        if let Some((antenna, feed, window)) = feeds.varying() {
            tracing::warn!(
                "FEED states receptors that vary in time (first for antenna {antenna}, feed \
                 {feed}, window {window}); the last row of each antenna, feed and window is \
                 used, as casacore's MSIter uses it"
            );
        }
        let windows = metadata::read_windows(&ms)?;
        let sources = metadata::read_sources(&ms, windows.len())?;
        let states = metadata::read_states(&ms)?;
        let processors = metadata::read_processors(&ms)?;
        let catalogue::Fields {
            entries: fields,
            ephemerides,
            uvw_frame,
        } = catalogue::fields_and_uvw(&ms, &sources.ids)?;
        let observations = catalogue::read_observations(&ms)?;
        let main = ms.main_table();
        let time_scale = catalogue::time_scale(main)?;
        let read = 2 * tables + catalogue::setups_bytes(&ms)?;
        grant(read)?;
        let setups = catalogue::spectral_setups(&ms)?;
        let modes = catalogue::read_state_modes(&ms)?;
        let spectral_sigmas = has_content(main, "SIGMA_SPECTRUM", 0)?;
        let mut indexed = 0;
        let index = Index::build(
            main,
            options.partition_scheme,
            (&modes, &ephemerides),
            ROWS_PER_CHUNK,
            &mut |total| {
                grant(read + total)?;
                indexed = total;
                Ok(())
            },
        )?;
        let described = (feeds.largest_receptors_bytes(), antennas.len());
        // The spectral name is longer than the scalar one, so it bounds
        // either choice without reading each partition's first cell twice.
        let groups = data_groups(main, true, uvw_frame);
        let entries_bytes: usize = index
            .partitions
            .iter()
            .map(|partition| entry_bytes(partition, &setups, &groups, described))
            .sum();
        grant(read + indexed + entries_bytes)?;
        // A shape is a hint for an estimate's natural blocks: a header that
        // cannot be read leaves its group the hint's fallback, as a column
        // without tiles has, and refuses nothing until the group is read.
        let tile_shapes = visibility_columns(main)
            .map(|(_, column)| {
                let shape = main.array_column_tile_shape(column);
                (column.to_string(), shape.ok().flatten())
            })
            .collect();
        let mut partitions = Vec::new();
        let mut entries = Vec::new();
        for (key, grid) in index.partitions {
            let data_description_id = u32::try_from(key.data_description)
                .ok()
                .filter(|row| (*row as usize) < setups.len())
                .ok_or_else(|| {
                    invalid(format!(
                        "DATA_DESC_ID {} is not in DATA_DESCRIPTION",
                        key.data_description
                    ))
                })?;
            let setup = &setups[data_description_id as usize];
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
            let receptors = partition_receptors(
                &feeds,
                &grid.baselines,
                key.feeds,
                setup.spectral_window_id as i32,
            )?;
            let feeds = [feed(key.feeds[0])?, feed(key.feeds[1])?];
            let spectral_weights = has_content(main, "WEIGHT_SPECTRUM", grid.first_time_row())?;
            // A state or processor a row names that its table does not hold
            // (CASA's simulator writes PROCESSOR_ID 0 without a row) has no
            // entry.
            let known = |id: i32, rows: usize| usize::try_from(id).ok().filter(|id| *id < rows);
            let partition_states = grid
                .states
                .iter()
                .filter_map(|state| known(*state, states.len()))
                .map(|state| StateId(state as u32))
                .collect();
            let processor = grid
                .processor
                .and_then(|processor| known(processor, processors.len()))
                .map(|processor| ProcessorId(processor as u32));
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
                data_groups: data_groups(main, spectral_weights, uvw_frame),
                receptors,
                window: WindowId(setup.spectral_window_id),
                states: partition_states,
                processor,
            });
            partitions.push(Partition {
                grid,
                obs_mode: key.obs_mode,
                data_description_id,
                spectral_window_id: setup.spectral_window_id,
                spectral_weights,
            });
        }
        Ok(Self {
            catalogue: Arc::new(Catalogue {
                antennas,
                fields,
                scans: index.scans,
                observations,
                partitions: entries,
                windows,
                sources: sources.entries,
                states,
                processors,
            }),
            partitions,
            state_modes: modes,
            main_states: index.states,
            spectral_sigmas,
            tile_shapes,
            options: options.clone(),
            tables_bytes: tables,
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

    /// The `DATA_DESCRIPTION` row of partition `id`.
    ///
    /// # Panics
    ///
    /// If `id` is not a partition of the observation.
    #[must_use]
    pub fn data_description_id(&self, id: PartitionId) -> u32 {
        self.partitions[id.0 as usize].data_description_id
    }

    /// The `STATE::OBS_MODE` the rows of partition `id` share: partitions
    /// are keyed by the whole string, so two modes listing the same intents
    /// in another order are two partitions. Empty for the rows without a
    /// state.
    #[must_use]
    pub fn obs_mode(&self, id: PartitionId) -> &str {
        &self.partitions[id.0 as usize].obs_mode
    }

    /// The `OBS_MODE` a row of `STATE_ID` `state` is partitioned by: its
    /// `STATE` row's, or empty for -1 or a state beyond `STATE`.
    #[must_use]
    pub fn state_mode(&self, state: i32) -> &str {
        usize::try_from(state)
            .ok()
            .and_then(|row| self.state_modes.get(row))
            .map_or("", String::as_str)
    }

    /// The `STATE_ID`s MAIN's rows hold, -1 and those beyond `STATE`
    /// included.
    #[must_use]
    pub fn main_states(&self) -> &BTreeSet<i32> {
        &self.main_states
    }

    /// The bytes opening a MeasurementSet of `main_rows` MAIN rows holds
    /// when its grids are full and its times hold a few rows each: a cell
    /// and eight bytes of time tables per row, and one chunk of key
    /// columns. Padding cells, which only opening finds, and times of one
    /// or two rows hold more, which [`Self::open_granted`] asks for before
    /// it allocates them.
    #[must_use]
    pub const fn opening_bytes(main_rows: usize) -> usize {
        let chunk = if main_rows < index::ROWS_PER_CHUNK {
            main_rows
        } else {
            index::ROWS_PER_CHUNK
        };
        main_rows * (size_of::<u32>() + size_of::<f64>()) + chunk * index::CHUNK_ROW_BYTES
    }

    /// Bound the table descriptions and keywords loaded when `path` opens.
    #[must_use]
    pub fn opening_table_bytes(path: &Path) -> usize {
        held::opening_bytes(path)
    }

    /// The tables, index and catalogue retained while this observation is open.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        let partitions = self.partitions.capacity() * size_of::<Partition>()
            + self
                .partitions
                .iter()
                .map(|partition| {
                    partition.obs_mode.capacity()
                        + partition.grid.states.capacity() * size_of::<i32>()
                })
                .sum::<usize>();
        let states = super::heap::strings(&self.state_modes) + self.main_states.len() * 128;
        let tiles = self.tile_shapes.capacity() * size_of::<(String, Option<Vec<usize>>)>()
            + self
                .tile_shapes
                .iter()
                .map(|(name, shape)| {
                    name.capacity()
                        + shape
                            .as_ref()
                            .map_or(0, |shape| shape.capacity() * size_of::<usize>())
                })
                .sum::<usize>();
        self.tables_bytes
            + self.index_bytes()
            + super::heap::catalogue_bytes(&self.catalogue)
            + partitions
            + states
            + tiles
    }

    /// MAIN's rows when the observation opened.
    pub(in crate::observation_io) fn main_rows(&self) -> usize {
        self.ms.main_table().row_count()
    }

    /// The MeasurementSet's path.
    pub(in crate::observation_io) fn path(&self) -> Result<&Path, ObservationError> {
        self.ms
            .path()
            .ok_or_else(|| invalid("the MeasurementSet has no path to write back to"))
    }

    /// Bytes the opening's index holds while the observation is open: each
    /// partition's grid, a MAIN row number per cell with its time and
    /// baseline tables.
    #[must_use]
    pub fn index_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| {
                let grid = &partition.grid;
                grid.cells.capacity() * size_of::<u32>()
                    + grid.times_s.capacity() * size_of::<f64>()
                    + grid.field_per_time.capacity() * size_of::<FieldId>()
                    + grid.scan_per_time.capacity() * size_of::<ScanId>()
                    + grid.baselines.capacity() * size_of::<OrderedBaseline>()
            })
            .sum()
    }

    /// The most its open tables hold while it is open: their descriptions
    /// and keywords ([`held::opening_bytes`]) and the rows of the subtables
    /// its catalogue was read from ([`held::read_bytes`]).
    pub(crate) fn tables_bytes(&self) -> usize {
        self.tables_bytes
    }

    /// The MAIN row partition `id` holds at `time_s` (MAIN `TIME`, seconds
    /// from MJD 0, as its batches deliver it) on `baseline`; `None` where
    /// its grid is padded or the partition has no such time or baseline.
    ///
    /// # Panics
    ///
    /// If `id` is not a partition of the observation.
    #[must_use]
    pub fn main_row(&self, id: PartitionId, time_s: f64, baseline: OrderedBaseline) -> Option<u32> {
        let grid = &self.partitions[id.0 as usize].grid;
        let time = grid
            .times_s
            .binary_search_by(|stored| stored.total_cmp(&time_s))
            .ok()?;
        let baseline = grid.baselines.binary_search(&baseline).ok()?;
        let row = grid.cells[time * grid.baselines.len() + baseline];
        (row != index::NO_ROW).then_some(row)
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

    /// The `STATE_ID`s partition `id`'s rows hold, ascending: -1 and those
    /// beyond `STATE` included, which its catalogue entry's states leave
    /// out.
    ///
    /// # Panics
    ///
    /// If `id` is not a partition of the observation.
    pub(crate) fn row_states(&self, id: PartitionId) -> &[i32] {
        &self.partitions[id.0 as usize].grid.states
    }

    /// The `STATE_ID` of each cell of partition `id`'s grid at times
    /// `times` (positions on it), in its batches' sample order (time, then
    /// baseline), into `states`; `rows` is scratch. A padded cell takes the
    /// state of its time's first row (every time holds one), so a row
    /// written for it keeps its partition. A batch of those times holds
    /// `states.len()` samples.
    ///
    /// # Errors
    ///
    /// Times beyond the grid, or a failed read.
    pub(crate) fn cell_states_into(
        &self,
        id: PartitionId,
        times: Range<usize>,
        (rows, states): (&mut Vec<usize>, &mut Vec<i32>),
    ) -> Result<(), ObservationError> {
        let grid = &self.partitions[id.0 as usize].grid;
        if times.end > grid.times_s.len() {
            return Err(invalid(format!(
                "times {times:?} of a grid of {} times",
                grid.times_s.len()
            )));
        }
        let cells = grid.cells(times);
        rows.clear();
        rows.extend(
            cells
                .iter()
                .filter(|&&row| row != index::NO_ROW)
                .map(|&row| row as usize),
        );
        states.clear();
        self.ms
            .main_table()
            .required_scalar_columns_for_rows_into(
                rows.as_slice(),
                &mut [RequiredScalarColumnDestination::new(
                    "STATE_ID",
                    RequiredScalarColumnValuesMut::Int32(states),
                )],
            )
            .map_err(table_error)?;
        // Spread the rows' states over the cells, last first, so each is
        // read before its place is written.
        let per_time = grid.baselines.len();
        let mut read = states.len();
        states.resize(cells.len(), -1);
        for (time, cells) in cells.chunks(per_time.max(1)).enumerate().rev() {
            let held = cells.iter().filter(|row| **row != index::NO_ROW).count();
            let first = if held == 0 { -1 } else { states[read - held] };
            for (baseline, row) in cells.iter().enumerate().rev() {
                states[time * per_time + baseline] = if *row == index::NO_ROW {
                    first
                } else {
                    read -= 1;
                    states[read]
                };
            }
        }
        Ok(())
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
        let resolved = request.resolve(
            entry,
            &partition.grid.times_s,
            &partition.grid.field_per_time,
        )?;
        let roles = request.roles;
        let weights = weight_source(
            request.weights,
            &entry.data_groups[resolved.data_group],
            partition.spectral_weights,
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
        // The selection's rows: its cells that a row fills, counted at
        // opening for the whole grid.
        let cells = times * baselines;
        let rows = if times == grid.times_s.len() {
            grid.rows
        } else {
            resolved
                .times
                .runs()
                .iter()
                .map(|run| {
                    grid.cells(run.clone())
                        .iter()
                        .filter(|row| **row != index::NO_ROW)
                        .count()
                })
                .sum()
        };
        let bytes_per_time = roles.time_bytes(0);
        let times_per_batch = times_per_batch(
            request.batch_bytes,
            baselines * (bytes_per_sample + scratch_bytes_per_sample) + bytes_per_time,
            times,
        )?;
        let group = &entry.data_groups[resolved.data_group];
        let tile = self
            .tile_shapes
            .iter()
            .find(|(column, _)| *column == group.visibilities)
            .and_then(|(_, shape)| shape.clone());
        let estimate = PartitionEstimate {
            partition: id,
            times,
            time_runs: resolved.times.clone(),
            weights: roles.weights.then_some(weights),
            baselines,
            bytes_per_sample,
            bytes_per_time,
            scratch_bytes_per_sample,
            times_per_batch,
            batches: if times_per_batch == 0 {
                0
            } else {
                resolved.times.blocks(times_per_batch).count()
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
            padded_fraction: if cells == 0 {
                0.0
            } else {
                1.0 - rows as f64 / cells as f64
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
    /// read in chunks of at most 262,144 rows. Padding has no
    /// row, so no interval.
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
        let mut rows = grid
            .cells
            .iter()
            .filter(|&&row| row != index::NO_ROW)
            .map(|&row| row as usize);
        let main = self.ms.main_table();
        let mut interval = None;
        let mut chunk = Vec::with_capacity(GRID_READ_ROWS.min(grid.cells.len()));
        let mut values = Vec::with_capacity(chunk.capacity());
        loop {
            chunk.clear();
            chunk.extend(rows.by_ref().take(GRID_READ_ROWS));
            if chunk.is_empty() {
                break;
            }
            values.clear();
            main.required_scalar_columns_for_rows_into(
                &chunk,
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

/// The most partition `(key, grid)`'s catalogue entry holds before it is
/// built: its setup's copy, its intents (the parts of its `OBS_MODE`), its
/// fields and scans (at most one of each per time), baselines, states and
/// data groups (`groups`), and the receptors of each antenna its baselines
/// name, `antennas` at most, each at most `receptors` bytes.
fn entry_bytes(
    (key, grid): &(index::PartitionKey, index::Grid),
    setups: &[catalogue::SpectralSetup],
    groups: &[DataGroupEntry],
    (receptors, antennas): (usize, usize),
) -> usize {
    let setup = usize::try_from(key.data_description)
        .ok()
        .and_then(|row| setups.get(row))
        .map_or(0, catalogue::SpectralSetup::heap_bytes);
    let intents =
        2 * key.obs_mode.len() + (key.obs_mode.matches(',').count() + 1) * size_of::<String>();
    let groups: usize = groups
        .iter()
        .map(|group| size_of::<DataGroupEntry>() + heap::group_bytes(group))
        .sum();
    let receptor_count = (2 * grid.baselines.len()).min(antennas);
    size_of::<PartitionEntry>()
        + setup
        + intents
        + (grid.field_per_time.len() + grid.scan_per_time.len()) * size_of::<u32>()
        + size_of_val(grid.baselines.as_slice())
        + grid.states.len() * size_of::<StateId>()
        + groups
        + casa_types::memory::btree_node_bytes::<AntennaId, Receptors>(receptor_count)
        + receptor_count * receptors
}

/// The receptors of each antenna `baselines` name, through the feed pair
/// `feeds` (`FEED1` for the first antenna, `FEED2` for the second) in
/// spectral window `window`; an antenna named through two feeds must have
/// the same receptors through both.
fn partition_receptors(
    feeds: &catalogue::Feeds,
    baselines: &[OrderedBaseline],
    pair: [i32; 2],
    window: i32,
) -> Result<BTreeMap<AntennaId, Receptors>, ObservationError> {
    let mut receptors = BTreeMap::new();
    for baseline in baselines {
        for (antenna, feed) in [(baseline.antenna1, pair[0]), (baseline.antenna2, pair[1])] {
            let id = i32::try_from(antenna.0).map_err(|_| invalid("an antenna beyond i32"))?;
            let Some(own) = feeds.receptors(id, feed, window) else {
                continue;
            };
            if receptors
                .insert(antenna, own.clone())
                .is_some_and(|known| &known != own)
            {
                return Err(ObservationError::Unsupported {
                    what: "an MSv2 antenna with different receptors through its two feeds",
                });
            }
        }
    }
    Ok(receptors)
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

    fn pointing(&self, request: &PointingRequest) -> Result<PointingSeries, ObservationError> {
        self.read_pointing(request)
    }

    fn group_keys(
        &self,
        id: PartitionId,
        data_group: &str,
        grouping: Grouping,
        fields: &FieldSelection,
    ) -> Result<Option<Vec<Option<GroupKey>>>, ObservationError> {
        let (partition, entry) = self.partition(id)?;
        if entry.data_group(data_group).is_none() {
            return Err(ObservationError::InvalidRequest {
                reason: format!("{id:?} has no data group {data_group}"),
            });
        }
        let grid = &partition.grid;
        super::grouping::group_keys(
            id,
            grouping,
            &grid.times_s,
            &grid.field_per_time,
            Some(&grid.scan_per_time),
            fields,
        )
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

/// Whether MAIN's `column` holds content at `row`, as casacore's
/// `TableColumn::hasContent` tells, by which CASA's `VisibilityIteratorImpl2`
/// picks `WEIGHT_SPECTRUM` and `SIGMA_SPECTRUM` over `WEIGHT` and `SIGMA`
/// (`weightSpectrumExists`, `sigmaSpectrumExists`): the column is there,
/// the cell is defined, and no axis of it is empty. CASA asks at the first
/// row of the rows it iterates: for `WEIGHT_SPECTRUM` again at each new
/// spectral window, for `SIGMA_SPECTRUM` once per MeasurementSet. A column
/// added and never filled holds none.
fn has_content(
    main: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<bool, ObservationError> {
    if row >= main.row_count()
        || !main
            .schema()
            .is_some_and(|schema| schema.contains_column(column))
    {
        return Ok(false);
    }
    // One cell, read without the row cache the opening's lazy fills need
    // left empty. A cell this reader cannot decode (a storage manager it
    // lacks) is not proof of no content: the column is taken as holding
    // content, and reading it reports the error, as before.
    let Ok(first) = main
        .column_accessor(column)
        .and_then(|cells| cells.array_cells_owned_uncached(&[row]))
    else {
        return Ok(true);
    };
    Ok(first.into_iter().next().flatten().is_some_and(|cell| {
        let shape = cell.shape();
        !shape.is_empty() && shape.iter().all(|axis| *axis > 0)
    }))
}

/// MAIN's visibility columns and the data groups they form, named as
/// XRADIO's converter names them: `base` (`DATA`), and `corrected`
/// (`CORRECTED_DATA`) and `model` (`MODEL_DATA`) where the columns exist.
fn visibility_columns(
    main: &casa_tables::Table,
) -> impl Iterator<Item = (&'static str, &'static str)> + '_ {
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
    .filter(move |(name, column)| *name == "base" || has(column))
}

/// The data groups of a partition ([`visibility_columns`]), whose weights
/// are `WEIGHT_SPECTRUM` when `spectral_weights` says so, else `WEIGHT`.
/// Every group reads the same flags, weights and UVW.
pub(super) fn data_groups(
    main: &casa_tables::Table,
    spectral_weights: bool,
    uvw_frame: UvwFrame,
) -> Vec<DataGroupEntry> {
    let weights = if spectral_weights {
        "WEIGHT_SPECTRUM"
    } else {
        "WEIGHT"
    };
    visibility_columns(main)
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
