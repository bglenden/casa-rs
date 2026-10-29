// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2 adapter's opening index: every MAIN row's partition and its
//! cell on that partition's time × baseline grid, built once at opening so
//! that estimates are arithmetic and every read is addressed by cell.
//!
//! Rows are partitioned the way XRADIO's converter does by default
//! (`DATA_DESC_ID` × `OBS_MODE` × `OBSERVATION_ID` × `EPHEMERIS_ID`), plus
//! the feed pair, which MSv4 has no axis for, plus whatever the
//! [`PartitionScheme`] adds. Within a partition the grid is the unique
//! times (exact values; no tolerance merges jittered times) by the unique
//! ordered baselines; a cell two rows claim is refused, naming both rows,
//! and a cell no row fills is padding.
//!
//! MAIN's key columns are read [`ROWS_PER_CHUNK`] rows at a time, in two
//! passes: the first gathers each partition's times, baselines and
//! processors and each scan's states, the second places every row in its
//! cell. Opening therefore holds the grids (four bytes a cell) and one
//! chunk of columns, whatever the MeasurementSet's size; a MAIN that fits
//! in one chunk is read once.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use casa_observation::{AntennaId, FieldId, ObservationError, OrderedBaseline, ScanEntry, ScanId};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, Table};

use super::{catalogue, invalid, table_error};
use crate::observation_io::PartitionScheme;

/// The cell of a grid no row fills.
pub(super) const NO_ROW: u32 = u32::MAX;

/// MAIN rows whose key columns are read at once while the index is built:
/// about 15 MiB of columns and row numbers.
pub(super) const ROWS_PER_CHUNK: usize = 1 << 18;

/// Bytes a chunk of key columns ([`KeyColumns`]) holds per row: a row
/// number, a time and ten integer keys.
pub(super) const CHUNK_ROW_BYTES: usize =
    size_of::<usize>() + size_of::<f64>() + 10 * size_of::<i32>();

/// At most the bytes a node of a B-tree of `entry`-byte entries holds:
/// room for 11 entries, the 12 edges of an internal node, and its header.
const fn node_bytes(entry: usize) -> usize {
    11 * entry + 12 * size_of::<usize>() + 16
}

/// At most the bytes a B-tree holds an entry of `entry` bytes besides its
/// root ([`node_bytes`]): a node but the root holds at least 5 entries.
const fn entry_bytes(entry: usize) -> usize {
    node_bytes(entry).div_ceil(5)
}

/// The step the opening asks its grant for beyond what it needs, so that
/// a growing first pass asks it rarely.
const GRANT_STEP: usize = 1 << 20;

/// What the opening asks to hold ([`Index::build`]'s `grant`): the bytes
/// the index will hold in all, in totals that only grow.
pub(super) type OpeningGrant<'a> = dyn FnMut(usize) -> Result<(), ObservationError> + 'a;

/// Bytes a grid holds a time while its rows are placed ([`Placing`]): the
/// time, its field and its scan.
const PLACING_TIME_BYTES: usize =
    size_of::<f64>() + size_of::<Option<FieldId>>() + size_of::<Option<ScanId>>();

/// One chunk of MAIN's key columns, refilled for every chunk.
#[derive(Default)]
struct KeyColumns {
    /// The chunk's MAIN rows.
    rows: Vec<usize>,
    data_description: Vec<i32>,
    time: Vec<f64>,
    field: Vec<i32>,
    scan_number: Vec<i32>,
    state: Vec<i32>,
    observation: Vec<i32>,
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    feed1: Vec<i32>,
    feed2: Vec<i32>,
    processor: Vec<i32>,
}

impl KeyColumns {
    /// Read the key columns of MAIN rows `rows`.
    fn read(&mut self, main: &Table, rows: Range<usize>) -> Result<(), ObservationError> {
        let Self {
            rows: indices,
            data_description,
            time,
            field,
            scan_number,
            state,
            observation,
            antenna1,
            antenna2,
            feed1,
            feed2,
            processor,
        } = self;
        indices.clear();
        indices.extend(rows);
        let mut destinations = vec![RequiredScalarColumnDestination::new(
            "TIME",
            RequiredScalarColumnValuesMut::Float64(time),
        )];
        for (name, values) in [
            ("DATA_DESC_ID", data_description),
            ("FIELD_ID", field),
            ("SCAN_NUMBER", scan_number),
            ("STATE_ID", state),
            ("OBSERVATION_ID", observation),
            ("ANTENNA1", antenna1),
            ("ANTENNA2", antenna2),
            ("FEED1", feed1),
            ("FEED2", feed2),
            ("PROCESSOR_ID", processor),
        ] {
            destinations.push(RequiredScalarColumnDestination::new(
                name,
                RequiredScalarColumnValuesMut::Int32(values),
            ));
        }
        main.required_scalar_columns_for_rows_into(indices, &mut destinations)
            .map_err(table_error)
    }

    /// The baseline of chunk row `i`, MAIN row `row`.
    fn baseline(&self, i: usize, row: usize) -> Result<OrderedBaseline, ObservationError> {
        let antenna = |value: i32| {
            u32::try_from(value)
                .map(AntennaId)
                .map_err(|_| invalid(format!("row {row} has a negative antenna {value}")))
        };
        Ok(OrderedBaseline {
            antenna1: antenna(self.antenna1[i])?,
            antenna2: antenna(self.antenna2[i])?,
        })
    }
}

/// What defines a partition of MAIN rows.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PartitionKey {
    pub(super) data_description: i32,
    /// `STATE::OBS_MODE` of the rows' state; empty without a state.
    pub(super) obs_mode: String,
    pub(super) observation: i32,
    /// The rows' field's `EPHEMERIS_ID`, when the column exists.
    pub(super) ephemeris: Option<i32>,
    pub(super) feeds: [i32; 2],
    /// `FIELD_ID`, when the scheme splits by field.
    pub(super) field: Option<i32>,
    /// `SCAN_NUMBER`, when the scheme splits by scan.
    pub(super) scan: Option<i32>,
}

/// A row's [`PartitionKey`] with its `OBS_MODE` as the mode's rank among
/// the distinct modes: it orders as the key does and costs no allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RowKey {
    data_description: i32,
    obs_mode: usize,
    observation: i32,
    ephemeris: Option<i32>,
    feeds: [i32; 2],
    field: Option<i32>,
    scan: Option<i32>,
}

/// The subtables' part of every row's key.
struct Keys<'a> {
    scheme: PartitionScheme,
    /// Each `FIELD` row's `EPHEMERIS_ID`.
    field_ephemeris: &'a [Option<i32>],
    /// The distinct `OBS_MODE`s and the empty mode, ascending.
    modes: Vec<String>,
    /// Each `STATE` row's mode, by its rank in `modes`.
    state_mode: Vec<usize>,
    /// The empty mode's rank: a row's without a state.
    no_mode: usize,
}

impl<'a> Keys<'a> {
    fn new(
        scheme: PartitionScheme,
        state_modes: &[String],
        field_ephemeris: &'a [Option<i32>],
    ) -> Self {
        let modes: Vec<String> = state_modes
            .iter()
            .cloned()
            .chain([String::new()])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let rank = |mode: &str| {
            modes
                .binary_search_by(|known| known.as_str().cmp(mode))
                .expect("every mode is collected")
        };
        let state_mode = state_modes.iter().map(|mode| rank(mode)).collect();
        let no_mode = rank("");
        Self {
            scheme,
            field_ephemeris,
            modes,
            state_mode,
            no_mode,
        }
    }

    /// The key and field of chunk row `i`, MAIN row `row`.
    fn of(
        &self,
        columns: &KeyColumns,
        i: usize,
        row: usize,
    ) -> Result<(RowKey, FieldId), ObservationError> {
        let field = columns.field[i];
        let ephemeris = usize::try_from(field)
            .ok()
            .and_then(|index| self.field_ephemeris.get(index))
            .ok_or_else(|| invalid(format!("row {row} has FIELD_ID {field} outside FIELD")))?;
        let obs_mode = usize::try_from(columns.state[i])
            .ok()
            .and_then(|state| self.state_mode.get(state))
            .copied()
            .unwrap_or(self.no_mode);
        let key = RowKey {
            data_description: columns.data_description[i],
            obs_mode,
            observation: columns.observation[i],
            ephemeris: *ephemeris,
            feeds: [columns.feed1[i], columns.feed2[i]],
            field: self.scheme.fields.then_some(field),
            scan: self.scheme.scans.then_some(columns.scan_number[i]),
        };
        Ok((key, FieldId(field.unsigned_abs())))
    }

    fn partition_key(&self, key: RowKey) -> PartitionKey {
        PartitionKey {
            data_description: key.data_description,
            obs_mode: self.modes[key.obs_mode].clone(),
            observation: key.observation,
            ephemeris: key.ephemeris,
            feeds: key.feeds,
            field: key.field,
            scan: key.scan,
        }
    }
}

/// What the first pass learns of one partition.
#[derive(Default)]
struct Gathered {
    /// Distinct `TIME`s, by their bits.
    times: BTreeSet<u64>,
    /// The last row's `TIME`: rows of one time usually follow each other.
    last_time: Option<u64>,
    baselines: BTreeSet<OrderedBaseline>,
    processors: BTreeSet<i32>,
    /// The `STATE_ID`s of its rows (#937).
    states: BTreeSet<i32>,
    /// The last row's `STATE_ID`: rows of one state usually follow each
    /// other.
    last_state: Option<i32>,
    rows: usize,
}

impl Gathered {
    /// At most the bytes a new partition's sets hold for their roots.
    const ROOTS: usize = node_bytes(size_of::<u64>())
        + node_bytes(size_of::<OrderedBaseline>())
        + 2 * node_bytes(size_of::<i32>());

    /// The bytes the partition's grid holds while its rows are placed: a
    /// cell a time and baseline, each time's and baseline's tables, and its
    /// place among the grids and their keys.
    fn grid_bytes(&self) -> usize {
        let (times, baselines) = (self.times.len(), self.baselines.len());
        times
            .saturating_mul(baselines)
            .saturating_mul(size_of::<u32>())
            .saturating_add(times * PLACING_TIME_BYTES)
            .saturating_add(baselines * size_of::<OrderedBaseline>())
            .saturating_add(self.states.len() * size_of::<i32>())
            .saturating_add(size_of::<Placing>() + size_of::<RowKey>())
    }
}

/// What the first pass learns of all of MAIN.
#[derive(Default)]
struct Survey {
    partitions: BTreeMap<RowKey, Gathered>,
    /// At most the bytes its sets and maps hold: each tree's root when it
    /// is made, and [`entry_bytes`] an entry.
    bytes: usize,
    /// Each `SCAN_NUMBER`'s `STATE_ID`s.
    scan_states: BTreeMap<i32, BTreeSet<i32>>,
}

impl Survey {
    /// Gather the rows of `columns`, MAIN rows from `first`, asking
    /// `ensure` for `base` and what the survey holds after each row that
    /// added a key: at most one row's keys beyond what was granted are
    /// held before it answers.
    fn gather(
        &mut self,
        keys: &Keys<'_>,
        (columns, first): (&KeyColumns, usize),
        base: usize,
        ensure: &mut dyn FnMut(usize) -> Result<(), ObservationError>,
    ) -> Result<(), ObservationError> {
        const PARTITION: usize = size_of::<(RowKey, Gathered)>();
        const SCAN: usize = size_of::<(i32, BTreeSet<i32>)>();
        let mut last_scan = None;
        for i in 0..columns.time.len() {
            let row = first + i;
            let (key, _) = keys.of(columns, i, row)?;
            let mut added = 0;
            let first_partition = self.partitions.is_empty();
            let partition = self.partitions.entry(key).or_insert_with(|| {
                added += entry_bytes(PARTITION) + Gathered::ROOTS;
                if first_partition {
                    added += node_bytes(PARTITION);
                }
                Gathered::default()
            });
            let time = columns.time[i].to_bits();
            if partition.last_time != Some(time) {
                added += usize::from(partition.times.insert(time)) * entry_bytes(8);
                partition.last_time = Some(time);
            }
            added += usize::from(partition.baselines.insert(columns.baseline(i, row)?))
                * entry_bytes(size_of::<OrderedBaseline>());
            added += usize::from(partition.processors.insert(columns.processor[i]))
                * entry_bytes(size_of::<i32>());
            if partition.last_state != Some(columns.state[i]) {
                added += usize::from(partition.states.insert(columns.state[i]))
                    * entry_bytes(size_of::<i32>());
                partition.last_state = Some(columns.state[i]);
            }
            partition.rows += 1;
            let scan = (columns.scan_number[i], columns.state[i]);
            if last_scan != Some(scan) {
                let first_scan = self.scan_states.is_empty();
                let states = self.scan_states.entry(scan.0).or_insert_with(|| {
                    added += entry_bytes(SCAN) + node_bytes(size_of::<i32>());
                    if first_scan {
                        added += node_bytes(SCAN);
                    }
                    BTreeSet::new()
                });
                added += usize::from(states.insert(scan.1)) * entry_bytes(size_of::<i32>());
                last_scan = Some(scan);
            }
            if added > 0 {
                self.bytes += added;
                ensure(base + self.bytes)?;
            }
        }
        Ok(())
    }
}

/// The bytes the scans and their tables hold: each entry with its name
/// and intents, the scan identifiers by number, and the states MAIN holds.
fn scans_bytes(scans: &[ScanEntry], numbers: usize, states: usize) -> usize {
    let entries: usize = scans
        .iter()
        .map(|scan| {
            size_of::<ScanEntry>()
                + scan.name.capacity()
                + scan.intents.capacity() * size_of::<String>()
                + scan.intents.iter().map(String::capacity).sum::<usize>()
        })
        .sum();
    let numbers =
        node_bytes(size_of::<(i32, ScanId)>()) + numbers * entry_bytes(size_of::<(i32, ScanId)>());
    let states = node_bytes(size_of::<i32>()) + states * entry_bytes(size_of::<i32>());
    entries + numbers + states
}

/// One partition's grid while the second pass places its rows.
struct Placing {
    times_s: Vec<f64>,
    baselines: Vec<OrderedBaseline>,
    cells: Vec<u32>,
    field_per_time: Vec<Option<FieldId>>,
    scan_per_time: Vec<Option<ScanId>>,
    rows: usize,
    /// The last row's `TIME` bits and time index.
    last_time: Option<(u64, usize)>,
    /// The partition's `STATE_ID`s, ascending, and its one `PROCESSOR_ID`.
    states: Vec<i32>,
    processor: Option<i32>,
}

impl Placing {
    /// The empty grid of a partition the first pass gathered.
    ///
    /// # Errors
    ///
    /// When the partition's rows have several `PROCESSOR_ID`s (XRADIO
    /// requires one per partition).
    fn new(gathered: Gathered) -> Result<Self, ObservationError> {
        if gathered.processors.len() > 1 {
            return Err(ObservationError::Unsupported {
                what: "rows of one partition with several PROCESSOR_IDs",
            });
        }
        let mut times_s: Vec<f64> = gathered.times.into_iter().map(f64::from_bits).collect();
        times_s.sort_by(f64::total_cmp);
        let baselines: Vec<OrderedBaseline> = gathered.baselines.into_iter().collect();
        let times = times_s.len();
        Ok(Self {
            cells: vec![NO_ROW; times * baselines.len()],
            field_per_time: vec![None; times],
            scan_per_time: vec![None; times],
            times_s,
            baselines,
            rows: gathered.rows,
            last_time: None,
            states: gathered.states.into_iter().collect(),
            processor: gathered.processors.first().copied(),
        })
    }

    /// Place chunk row `i`, MAIN row `row`, of field `field`.
    ///
    /// # Errors
    ///
    /// When its cell already holds a row, or its time already holds another
    /// field or scan.
    fn place(
        &mut self,
        columns: &KeyColumns,
        i: usize,
        row: usize,
        field: FieldId,
        scans: &BTreeMap<i32, ScanId>,
    ) -> Result<(), ObservationError> {
        let bits = columns.time[i].to_bits();
        let t = match self.last_time {
            Some((last, t)) if last == bits => t,
            _ => {
                let t = self
                    .times_s
                    .binary_search_by(|time| time.total_cmp(&columns.time[i]))
                    .expect("every row's time is on the grid");
                self.last_time = Some((bits, t));
                t
            }
        };
        let b = self
            .baselines
            .binary_search(&columns.baseline(i, row)?)
            .expect("every row's baseline is on the grid");
        let cell = &mut self.cells[t * self.baselines.len() + b];
        if *cell != NO_ROW {
            return Err(ObservationError::InvalidStore {
                reason: format!(
                    "rows {} and {row} are two acquisitions of one time and baseline; \
                     the partition keys do not separate them",
                    *cell
                ),
            });
        }
        *cell = u32::try_from(row).expect("rows are counted below NO_ROW");
        let scan = *scans
            .get(&columns.scan_number[i])
            .expect("every SCAN_NUMBER is catalogued");
        one_per_time(
            &mut self.field_per_time[t],
            field,
            "two fields at one time of one partition",
        )?;
        one_per_time(
            &mut self.scan_per_time[t],
            scan,
            "two scans at one time of one partition",
        )
    }

    fn finish(self) -> Grid {
        Grid {
            times_s: self.times_s,
            field_per_time: self
                .field_per_time
                .into_iter()
                .map(|field| field.expect("every time has a row"))
                .collect(),
            scan_per_time: self
                .scan_per_time
                .into_iter()
                .map(|scan| scan.expect("every time has a row"))
                .collect(),
            baselines: self.baselines,
            cells: self.cells,
            rows: self.rows,
            states: self.states,
            processor: self.processor,
        }
    }
}

/// The opening index of a MAIN table.
#[derive(Debug, PartialEq)]
pub(super) struct Index {
    /// Every partition's key and grid, in key order.
    pub(super) partitions: Vec<(PartitionKey, Grid)>,
    /// The scans, by ascending `SCAN_NUMBER`.
    pub(super) scans: Vec<ScanEntry>,
    /// The `STATE_ID`s MAIN's rows hold, -1 and those beyond `STATE`
    /// included.
    pub(super) states: BTreeSet<i32>,
}

impl Index {
    /// Partition `main`'s rows and place them on their grids, reading the
    /// key columns `rows_per_chunk` rows at a time.
    ///
    /// `state_modes` is each `STATE` row's `OBS_MODE` and `field_ephemeris`
    /// each `FIELD` row's `EPHEMERIS_ID`.
    ///
    /// # Errors
    ///
    /// When a row's field is not in `FIELD` or an antenna is negative, two
    /// rows share a (time, baseline) cell, a time holds rows of two fields
    /// or two scans, a partition's rows have several `PROCESSOR_ID`s, or
    /// MAIN has too many rows to number in 32 bits; whatever `grant`
    /// returns when it refuses.
    ///
    /// Before it allocates them, the build asks `grant` for the bytes the
    /// index will hold in all, in totals that only grow: its chunk of key
    /// columns, the first pass's sets and maps (in steps, as keys come),
    /// then the scans and the grids. A refusal stops it with nothing more
    /// allocated.
    pub(super) fn build(
        main: &Table,
        scheme: PartitionScheme,
        (state_modes, field_ephemeris): (&[String], &[Option<i32>]),
        rows_per_chunk: usize,
        grant: &mut OpeningGrant<'_>,
    ) -> Result<Self, ObservationError> {
        let rows = main.row_count();
        if rows >= NO_ROW as usize {
            return Err(invalid("more than 2^32 - 1 MAIN rows"));
        }
        let chunks: Vec<Range<usize>> = (0..rows)
            .step_by(rows_per_chunk)
            .map(|start| start..start.saturating_add(rows_per_chunk).min(rows))
            .collect();
        let chunk_bytes = rows_per_chunk.min(rows) * CHUNK_ROW_BYTES;
        let mut granted = 0;
        let mut ensure = |needed: usize| -> Result<(), ObservationError> {
            if needed > granted {
                let step = needed.max(granted + granted / 2).max(granted + GRANT_STEP);
                if grant(step).is_err() {
                    grant(needed)?;
                    granted = needed;
                } else {
                    granted = step;
                }
            }
            Ok(())
        };
        ensure(chunk_bytes)?;
        let keys = Keys::new(scheme, state_modes, field_ephemeris);
        let mut columns = KeyColumns::default();
        let mut survey = Survey::default();
        for chunk in &chunks {
            columns.read(main, chunk.clone())?;
            survey.gather(&keys, (&columns, chunk.start), chunk_bytes, &mut ensure)?;
        }
        let states: BTreeSet<i32> = survey.scan_states.values().flatten().copied().collect();
        let (scans, scan_ids) = catalogue::scans(survey.scan_states, state_modes);
        let scans_bytes = scans_bytes(&scans, scan_ids.len(), states.len());
        let grids: usize = survey.partitions.values().map(Gathered::grid_bytes).sum();
        ensure(chunk_bytes + survey.bytes + scans_bytes + grids)?;
        let order: Vec<RowKey> = survey.partitions.keys().copied().collect();
        let mut grids = survey
            .partitions
            .into_values()
            .map(Placing::new)
            .collect::<Result<Vec<_>, _>>()?;
        for chunk in &chunks {
            if chunks.len() > 1 {
                columns.read(main, chunk.clone())?;
            }
            let mut last: Option<(RowKey, usize)> = None;
            for i in 0..columns.time.len() {
                let row = chunk.start + i;
                let (key, field) = keys.of(&columns, i, row)?;
                let partition = match last {
                    Some((known, partition)) if known == key => partition,
                    _ => {
                        let partition = order
                            .binary_search(&key)
                            .expect("the first pass saw every key");
                        last = Some((key, partition));
                        partition
                    }
                };
                grids[partition].place(&columns, i, row, field, &scan_ids)?;
            }
        }
        Ok(Self {
            partitions: order
                .into_iter()
                .zip(grids)
                .map(|(key, grid)| (keys.partition_key(key), grid.finish()))
                .collect(),
            scans,
            states,
        })
    }
}

/// One partition's time × baseline grid and the row at each cell.
#[derive(Debug, PartialEq)]
pub(super) struct Grid {
    /// Unique times, ascending (MAIN `TIME`, seconds from MJD 0).
    pub(super) times_s: Vec<f64>,
    pub(super) field_per_time: Vec<FieldId>,
    pub(super) scan_per_time: Vec<ScanId>,
    /// Unique ordered baselines, ascending.
    pub(super) baselines: Vec<OrderedBaseline>,
    /// The MAIN row at cell `t × baselines.len() + b`, or [`NO_ROW`].
    pub(super) cells: Vec<u32>,
    /// Rows placed: cells that are not padding.
    pub(super) rows: usize,
    /// Its rows' `STATE_ID`s, ascending (−1 for rows without a state).
    pub(super) states: Vec<i32>,
    /// Its rows' one `PROCESSOR_ID`.
    pub(super) processor: Option<i32>,
}

impl Grid {
    /// The cells of times `times`.
    pub(super) fn cells(&self, times: Range<usize>) -> &[u32] {
        let per_time = self.baselines.len();
        &self.cells[times.start * per_time..times.end * per_time]
    }

    /// The first of its rows in time order, as CASA's iteration meets them:
    /// the lowest MAIN row of its first time.
    pub(super) fn first_row(&self) -> usize {
        self.cells(0..1)
            .iter()
            .copied()
            .filter(|cell| *cell != NO_ROW)
            .min()
            .expect("a partition's first time holds a row") as usize
    }
}

/// Record `value` at a time, which every row of that time must share.
fn one_per_time<T: Copy + PartialEq>(
    slot: &mut Option<T>,
    value: T,
    what: &'static str,
) -> Result<(), ObservationError> {
    if slot.is_some_and(|known| known != value) {
        return Err(ObservationError::Unsupported { what });
    }
    *slot = Some(value);
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs::File;
    use std::path::Path;

    use super::*;
    use crate::MeasurementSet;

    /// The index does not depend on how many rows each chunk reads, with
    /// either partition scheme: one row, a ragged last chunk, or all of
    /// MAIN at once.
    #[test]
    fn the_index_does_not_depend_on_the_chunk_size() {
        let root = tempfile::tempdir().expect("root");
        let archive =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ms_trial/ms_trial.ms.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(
            File::open(archive).expect("fixture"),
        ))
        .unpack(root.path())
        .expect("unpack");
        let ms = MeasurementSet::open_retained_read(root.path().join("ms_trial.ms")).expect("open");
        let modes = catalogue::read_state_modes(&ms).expect("modes");
        let ephemerides = catalogue::fields_and_uvw(&ms, &BTreeMap::new())
            .expect("fields")
            .ephemerides;
        let main = ms.main_table();
        assert!(main.row_count() > 64, "several chunks of 7 and 64");
        for scheme in [
            PartitionScheme::default(),
            PartitionScheme {
                fields: true,
                scans: true,
            },
        ] {
            let build = |rows_per_chunk| {
                Index::build(
                    main,
                    scheme,
                    (&modes, &ephemerides),
                    rows_per_chunk,
                    &mut |_| Ok(()),
                )
                .expect("index")
            };
            let whole = build(usize::MAX);
            assert!(!whole.partitions.is_empty() && !whole.scans.is_empty());
            for rows_per_chunk in [1, 7, 64] {
                assert_eq!(
                    build(rows_per_chunk),
                    whole,
                    "{scheme:?}, {rows_per_chunk} rows"
                );
            }
        }
    }
}
