// SPDX-License-Identifier: LGPL-3.0-or-later
//! One scan of an MSv2's `POINTING`, finding what a conversion's reads of
//! it would carry ([`scan`]): which rows each partition reads, as the MSv2
//! adapter reads a span of times (`PreparedSelectedPointingCatalog`'s
//! retention), the frames and polynomial terms of those rows, the rows a
//! read would refuse, and the rows nothing reads.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;

use casa_observation::{AntennaId, ObservationError};
use casa_tables::table_measures::{MeasRefDesc, TableMeasDesc};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, Table};
use casa_types::measures::direction::DirectionRef;

use super::losses::Destination;
use crate::derived::engine::resolve_direction_reference_selected;
use crate::observation_io::msv2::POLYNOMIAL_TERMS;
use crate::observation_io::update::storage;

/// `POINTING` rows scanned at a time.
const SCAN_ROWS: usize = 1 << 16;

/// One partition as the scan sees it: the antennas its baselines name and
/// its times.
pub(super) struct Partition {
    pub(super) antennas: Vec<AntennaId>,
    pub(super) times: Option<RangeInclusive<f64>>,
}

impl Partition {
    fn slot(&self, antenna: i32) -> Option<usize> {
        u32::try_from(antenna)
            .ok()
            .and_then(|id| self.antennas.binary_search(&AntennaId(id)).ok())
    }
}

/// One `POINTING` row's scalar metadata.
#[derive(Clone, Copy)]
struct Candidate {
    row: usize,
    antenna: i32,
    time: f64,
    interval: f64,
    terms: usize,
}

impl Candidate {
    /// Whether the MSv2 adapter reads this row for a span of `times`: its
    /// interval meets them or its time is within them, as
    /// `PreparedSelectedPointingCatalog` retains rows. Into an MSv2 a read
    /// spans every row.
    fn within(&self, times: &RangeInclusive<f64>, destination: Destination) -> bool {
        let half = 0.5 * self.interval;
        destination == Destination::Msv2
            || times.contains(&self.time)
            || (half > 0.0
                && self.time + half >= *times.start()
                && self.time - half <= *times.end())
    }
}

/// What one scan of an MSv2's `POINTING` finds.
#[derive(Default)]
pub(super) struct Scan {
    /// The frames of the rows each partition reads.
    pub(super) frames: Vec<Vec<DirectionRef>>,
    /// The most polynomial terms of a row some partition reads.
    pub(super) most_terms: usize,
    /// A row of an antenna some partition names that the MSv2 adapter
    /// would refuse: polynomial terms beyond its bound, or times that are
    /// not finite.
    pub(super) refused: Option<&'static str>,
    /// The earliest and latest time of a row some partition names.
    pub(super) times: Option<(f64, f64)>,
    /// Rows no partition reads.
    pub(super) outside: usize,
    /// Each partition's rows within its times.
    pub(super) within: Vec<usize>,
    /// Each named antenna's rows.
    pub(super) antenna_rows: BTreeMap<AntennaId, usize>,
    /// Rows a partition reads at the (antenna, time) of another it reads.
    pub(super) repeated: usize,
    /// Each partition's antennas' nearest rows before and after its times,
    /// as the MSv2 adapter picks them: the latest before and the earliest
    /// after, the lowest row of a tie.
    nearest: Vec<Vec<[Option<Candidate>; 2]>>,
    /// The (antenna, time, partition) of each row a partition reads.
    keys: Vec<(i32, u64, usize)>,
}

impl Scan {
    /// Account for `candidate` among `partitions`; `frame` resolves a row's
    /// direction frame.
    fn add(
        &mut self,
        candidate: Candidate,
        partitions: &[Partition],
        destination: Destination,
        frame: &impl Fn(usize) -> Result<DirectionRef, ObservationError>,
    ) -> Result<(), ObservationError> {
        let mut read = false;
        let mut named = false;
        for (index, partition) in partitions.iter().enumerate() {
            let (Some(slot), Some(times)) = (partition.slot(candidate.antenna), &partition.times)
            else {
                continue;
            };
            named = true;
            if candidate.within(times, destination) {
                self.within[index] += 1;
                self.read_by(index, candidate, frame)?;
                read = true;
                continue;
            }
            let [before, after] = &mut self.nearest[index][slot];
            if candidate.time < *times.start()
                && before.is_none_or(|known| {
                    candidate.time > known.time
                        || (candidate.time == known.time && candidate.row < known.row)
                })
            {
                *before = Some(candidate);
            }
            if candidate.time > *times.end()
                && after
                    .is_none_or(|known| (candidate.time, candidate.row) < (known.time, known.row))
            {
                *after = Some(candidate);
            }
        }
        if named {
            // The MSv2 adapter refuses a read meeting such a row, read or not.
            if candidate.terms > POLYNOMIAL_TERMS {
                self.refused = Some("rows of more than four polynomial terms");
            }
            if !(candidate.time.is_finite() && candidate.interval.is_finite()) {
                self.refused = Some("rows of non-finite times");
            }
            *self
                .antenna_rows
                .entry(AntennaId(candidate.antenna as u32))
                .or_default() += 1;
            self.times = Some(
                self.times
                    .map_or((candidate.time, candidate.time), |(first, last)| {
                        (first.min(candidate.time), last.max(candidate.time))
                    }),
            );
        }
        if !read {
            self.outside += 1;
        }
        Ok(())
    }

    /// `candidate` is read by partition `index`.
    fn read_by(
        &mut self,
        index: usize,
        candidate: Candidate,
        frame: &impl Fn(usize) -> Result<DirectionRef, ObservationError>,
    ) -> Result<(), ObservationError> {
        let own = frame(candidate.row)?;
        if !self.frames[index].contains(&own) {
            self.frames[index].push(own);
        }
        self.most_terms = self.most_terms.max(candidate.terms);
        self.keys
            .push((candidate.antenna, candidate.time.to_bits(), index));
        Ok(())
    }

    /// Account for each partition's antennas' nearest rows either side, and
    /// count the rows a partition reads at the (antenna, time) of another.
    fn finish(
        &mut self,
        partitions: &[Partition],
        destination: Destination,
        frame: &impl Fn(usize) -> Result<DirectionRef, ObservationError>,
    ) -> Result<(), ObservationError> {
        let nearest: Vec<(usize, Candidate)> = std::mem::take(&mut self.nearest)
            .into_iter()
            .enumerate()
            .flat_map(|(index, sides)| {
                sides
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(move |candidate| (index, candidate))
            })
            .collect();
        let mut beyond = Vec::new();
        for (index, candidate) in nearest {
            self.read_by(index, candidate, frame)?;
            let read_within = partitions.iter().any(|partition| {
                partition.slot(candidate.antenna).is_some()
                    && partition
                        .times
                        .as_ref()
                        .is_some_and(|times| candidate.within(times, destination))
            });
            if !read_within {
                beyond.push(candidate.row);
            }
        }
        beyond.sort_unstable();
        beyond.dedup();
        self.outside -= beyond.len();
        self.keys.sort_unstable();
        self.repeated = self
            .keys
            .windows(2)
            .filter(|pair| pair[0] == pair[1])
            .count();
        self.keys = Vec::new();
        Ok(())
    }
}

/// Scan `POINTING`'s antennas, times, intervals and polynomial orders, and
/// the frames of the rows partitions read ([`Scan`]).
pub(super) fn scan(
    table: &Table,
    partitions: &[Partition],
    destination: Destination,
) -> Result<Scan, ObservationError> {
    let fixed = match TableMeasDesc::reconstruct(table, "DIRECTION") {
        Some(description) => matches!(description.ref_desc(), MeasRefDesc::Fixed { .. }),
        None => true,
    };
    let resolve = |row: usize| {
        resolve_direction_reference_selected(table, "POINTING", "DIRECTION", row).map_err(storage)
    };
    let column_frame = if fixed { Some(resolve(0)?) } else { None };
    let frame = |row: usize| column_frame.map_or_else(|| resolve(row), Ok);
    let mut scan = Scan {
        frames: vec![Vec::new(); partitions.len()],
        within: vec![0; partitions.len()],
        nearest: partitions
            .iter()
            .map(|partition| vec![[None, None]; partition.antennas.len()])
            .collect(),
        ..Scan::default()
    };
    let rows = table.row_count();
    let (mut antennas, mut orders) = (Vec::new(), Vec::new());
    let (mut times, mut intervals, mut origins) = (Vec::new(), Vec::new(), Vec::new());
    for start in (0..rows).step_by(SCAN_ROWS) {
        let chunk: Vec<usize> = (start..rows.min(start + SCAN_ROWS)).collect();
        for values in [&mut antennas, &mut orders] {
            values.clear();
        }
        for values in [&mut times, &mut intervals, &mut origins] {
            values.clear();
        }
        let mut destinations = [
            RequiredScalarColumnDestination::new(
                "ANTENNA_ID",
                RequiredScalarColumnValuesMut::Int32(&mut antennas),
            ),
            RequiredScalarColumnDestination::new(
                "TIME",
                RequiredScalarColumnValuesMut::Float64(&mut times),
            ),
            RequiredScalarColumnDestination::new(
                "INTERVAL",
                RequiredScalarColumnValuesMut::Float64(&mut intervals),
            ),
            RequiredScalarColumnDestination::new(
                "TIME_ORIGIN",
                RequiredScalarColumnValuesMut::Float64(&mut origins),
            ),
            RequiredScalarColumnDestination::new(
                "NUM_POLY",
                RequiredScalarColumnValuesMut::Int32(&mut orders),
            ),
        ];
        table
            .required_scalar_columns_for_rows_into(&chunk, &mut destinations)
            .map_err(storage)?;
        for (slot, row) in chunk.iter().enumerate() {
            let candidate = Candidate {
                row: *row,
                antenna: antennas[slot],
                time: times[slot],
                // A non-finite origin refuses a read as a non-finite time does.
                interval: if origins[slot].is_finite() {
                    intervals[slot]
                } else {
                    f64::NAN
                },
                terms: usize::try_from(orders[slot]).map_or(usize::MAX, |order| order + 1),
            };
            scan.add(candidate, partitions, destination, &frame)?;
        }
    }
    scan.finish(partitions, destination, &frame)?;
    Ok(scan)
}
