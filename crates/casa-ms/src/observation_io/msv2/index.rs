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

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use casa_observation::{AntennaId, FieldId, ObservationError, OrderedBaseline, ScanId};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, Table};

use super::{invalid, table_error};
use crate::observation_io::PartitionScheme;

/// The cell of a grid no row fills.
pub(super) const NO_ROW: u32 = u32::MAX;

/// MAIN's key columns, every row: about 56 bytes per row while the index
/// is built, released once the grids exist.
pub(super) struct MainColumns {
    pub(super) data_description: Vec<i32>,
    pub(super) time: Vec<f64>,
    pub(super) field: Vec<i32>,
    pub(super) scan_number: Vec<i32>,
    pub(super) state: Vec<i32>,
    pub(super) observation: Vec<i32>,
    pub(super) antenna1: Vec<i32>,
    pub(super) antenna2: Vec<i32>,
    pub(super) feed1: Vec<i32>,
    pub(super) feed2: Vec<i32>,
    pub(super) processor: Vec<i32>,
}

impl MainColumns {
    /// Read the key columns of every row of `main`.
    pub(super) fn read(main: &Table) -> Result<Self, ObservationError> {
        let rows = (0..main.row_count()).collect::<Vec<_>>();
        let mut columns = Self {
            data_description: Vec::new(),
            time: Vec::new(),
            field: Vec::new(),
            scan_number: Vec::new(),
            state: Vec::new(),
            observation: Vec::new(),
            antenna1: Vec::new(),
            antenna2: Vec::new(),
            feed1: Vec::new(),
            feed2: Vec::new(),
            processor: Vec::new(),
        };
        let mut destinations = vec![RequiredScalarColumnDestination::new(
            "TIME",
            RequiredScalarColumnValuesMut::Float64(&mut columns.time),
        )];
        for (name, values) in [
            ("DATA_DESC_ID", &mut columns.data_description),
            ("FIELD_ID", &mut columns.field),
            ("SCAN_NUMBER", &mut columns.scan_number),
            ("STATE_ID", &mut columns.state),
            ("OBSERVATION_ID", &mut columns.observation),
            ("ANTENNA1", &mut columns.antenna1),
            ("ANTENNA2", &mut columns.antenna2),
            ("FEED1", &mut columns.feed1),
            ("FEED2", &mut columns.feed2),
            ("PROCESSOR_ID", &mut columns.processor),
        ] {
            destinations.push(RequiredScalarColumnDestination::new(
                name,
                RequiredScalarColumnValuesMut::Int32(values),
            ));
        }
        main.required_scalar_columns_for_rows_into(&rows, &mut destinations)
            .map_err(table_error)?;
        Ok(columns)
    }

    /// Rows read.
    pub(super) fn rows(&self) -> usize {
        self.time.len()
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

/// Every row's partition, in key order.
pub(super) fn partition_rows(
    columns: &MainColumns,
    scheme: PartitionScheme,
    modes: &[String],
    field_ephemeris: &[Option<i32>],
) -> Result<BTreeMap<PartitionKey, Vec<u32>>, ObservationError> {
    let mut partitions = BTreeMap::<PartitionKey, Vec<u32>>::new();
    for row in 0..columns.rows() {
        let field = usize::try_from(columns.field[row])
            .ok()
            .filter(|field| *field < field_ephemeris.len())
            .ok_or_else(|| {
                invalid(format!(
                    "row {row} has FIELD_ID {} outside FIELD",
                    columns.field[row]
                ))
            })?;
        let obs_mode = usize::try_from(columns.state[row])
            .ok()
            .and_then(|state| modes.get(state))
            .cloned()
            .unwrap_or_default();
        let key = PartitionKey {
            data_description: columns.data_description[row],
            obs_mode,
            observation: columns.observation[row],
            ephemeris: field_ephemeris[field],
            feeds: [columns.feed1[row], columns.feed2[row]],
            field: scheme.fields.then_some(columns.field[row]),
            scan: scheme.scans.then_some(columns.scan_number[row]),
        };
        partitions
            .entry(key)
            .or_default()
            .push(u32::try_from(row).map_err(|_| invalid("more than 2^32 rows"))?);
    }
    Ok(partitions)
}

/// One partition's time × baseline grid and the row at each cell.
#[derive(Debug)]
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
}

impl Grid {
    /// Place `rows` on their grid.
    ///
    /// # Errors
    ///
    /// When two rows share a (time, baseline) cell, a time holds rows of two
    /// fields or two scans, the rows have several `PROCESSOR_ID`s (XRADIO
    /// requires one per partition), or an identity is negative or unknown.
    pub(super) fn build(
        rows: &[u32],
        columns: &MainColumns,
        scans: &BTreeMap<i32, ScanId>,
        fields: usize,
    ) -> Result<Self, ObservationError> {
        let processors: BTreeSet<i32> = rows
            .iter()
            .map(|&row| columns.processor[row as usize])
            .collect();
        if processors.len() > 1 {
            return Err(ObservationError::Unsupported {
                what: "rows of one partition with several PROCESSOR_IDs",
            });
        }
        let mut times_s: Vec<f64> = rows.iter().map(|&row| columns.time[row as usize]).collect();
        times_s.sort_by(f64::total_cmp);
        times_s.dedup_by(|a, b| a.to_bits() == b.to_bits());
        let antenna = |value: i32, row: u32| {
            u32::try_from(value)
                .map(AntennaId)
                .map_err(|_| invalid(format!("row {row} has a negative antenna {value}")))
        };
        let mut baselines: Vec<OrderedBaseline> = rows
            .iter()
            .map(|&row| {
                Ok(OrderedBaseline {
                    antenna1: antenna(columns.antenna1[row as usize], row)?,
                    antenna2: antenna(columns.antenna2[row as usize], row)?,
                })
            })
            .collect::<Result<_, ObservationError>>()?;
        baselines.sort_unstable();
        baselines.dedup();
        let (times, count) = (times_s.len(), baselines.len());
        let mut cells = vec![NO_ROW; times * count];
        let mut field_per_time: Vec<Option<FieldId>> = vec![None; times];
        let mut scan_per_time: Vec<Option<ScanId>> = vec![None; times];
        for &row in rows {
            let index = row as usize;
            let t = times_s
                .binary_search_by(|time| time.total_cmp(&columns.time[index]))
                .expect("every row's time is on the grid");
            let baseline = OrderedBaseline {
                antenna1: antenna(columns.antenna1[index], row)?,
                antenna2: antenna(columns.antenna2[index], row)?,
            };
            let b = baselines
                .binary_search(&baseline)
                .expect("every row's baseline is on the grid");
            let cell = &mut cells[t * count + b];
            if *cell != NO_ROW {
                return Err(ObservationError::InvalidStore {
                    reason: format!(
                        "rows {} and {row} are two acquisitions of one time and baseline; \
                         the partition keys do not separate them",
                        *cell
                    ),
                });
            }
            *cell = row;
            let field = u32::try_from(columns.field[index])
                .ok()
                .filter(|field| (*field as usize) < fields)
                .map(FieldId)
                .ok_or_else(|| invalid(format!("row {row} has an unknown FIELD_ID")))?;
            let scan = *scans
                .get(&columns.scan_number[index])
                .expect("every SCAN_NUMBER is catalogued");
            one_per_time(
                &mut field_per_time[t],
                field,
                "two fields at one time of one partition",
            )?;
            one_per_time(
                &mut scan_per_time[t],
                scan,
                "two scans at one time of one partition",
            )?;
        }
        Ok(Self {
            times_s,
            field_per_time: field_per_time
                .into_iter()
                .map(|field| field.expect("every time has a row"))
                .collect(),
            scan_per_time: scan_per_time
                .into_iter()
                .map(|scan| scan.expect("every time has a row"))
                .collect(),
            baselines,
            cells,
            rows: rows.len(),
        })
    }

    /// Times on the grid.
    pub(super) fn times(&self) -> usize {
        self.times_s.len()
    }

    /// The fraction of cells no row fills.
    pub(super) fn padded_fraction(&self) -> f64 {
        if self.cells.is_empty() {
            0.0
        } else {
            1.0 - self.rows as f64 / self.cells.len() as f64
        }
    }

    /// The cells of times `times`.
    pub(super) fn cells(&self, times: Range<usize>) -> &[u32] {
        let per_time = self.baselines.len();
        &self.cells[times.start * per_time..times.end * per_time]
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
