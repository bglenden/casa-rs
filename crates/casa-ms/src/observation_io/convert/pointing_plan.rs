// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a conversion carries of the source's pointing, decided with the
//! losses that implies before anything is written ([`plan`]).
//!
//! Each partition's pointing is read through the boundary
//! ([`Observation::pointing`]) over a span of its times:
//! - into an MSv4, over the partition's own times, as XRADIO's converter
//!   takes the `POINTING` rows within each dataset's times (the boundary
//!   adds each antenna's nearest sample either side);
//! - into an MSv2, over every time the source's pointing holds, so that an
//!   MSv2 source's rows are carried whole.
//!
//! An MSv2 source's `POINTING` is scanned once for what that cannot carry:
//! rows outside every span, rows at the antenna and time of an earlier one
//! (an MSv4 holds one sample there), polynomial terms beyond the direction
//! (an MSv4 holds one term), and the columns neither destination holds.
//! Pointing the destination cannot hold at all (directions in several
//! frames, or in a frame MSv4 has no name for, or polynomials beyond the
//! reader's bound) is lost whole, as a reported loss, rather than refused
//! part way through writing. A partition whose pointing would hold more
//! than the batch budget refuses the conversion.

use std::mem::size_of;
use std::ops::RangeInclusive;
use std::path::Path;

use casa_observation::{
    AntennaId, Observation, ObservationError, PartitionId, PointingDirection, PointingRequest,
};
use casa_tables::table_measures::{MeasRefDesc, TableMeasDesc};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, Table};
use casa_types::measures::direction::DirectionRef;

use super::Loss;
use super::losses::Destination;
use crate::derived::engine::resolve_direction_reference_selected;
use crate::observation_io::msv4::POINTING_FRAMES;
use crate::observation_io::update::{request_error, storage};
use crate::schema::SubtableId;

/// `POINTING` rows scanned at a time.
const SCAN_ROWS: usize = 1 << 16;

/// The most polynomial terms a `POINTING` row may have (`NUM_POLY` + 1):
/// the bound the MSv2 adapter reads with.
const MAXIMUM_TERMS: usize = 4;

/// What a conversion carries of the source's pointing.
#[derive(Debug, Default)]
pub(super) struct PointingPlan {
    /// Each partition's span of times to read, in its time system; `None`
    /// carries none.
    spans: Vec<Option<RangeInclusive<f64>>>,
}

impl PointingPlan {
    /// The pointing request for `partition`'s `antennas`, when its pointing
    /// is carried. The plan has checked its size against the budget.
    pub(super) fn request(
        &self,
        partition: PartitionId,
        antennas: Vec<AntennaId>,
    ) -> Option<PointingRequest> {
        let times = self.spans.get(partition.0 as usize)?.clone()?;
        Some(PointingRequest {
            partition,
            direction: PointingDirection::Beam,
            antennas,
            times,
            budget_bytes: usize::MAX,
        })
    }
}

/// One partition as the plan sees it: its antennas and its times.
struct Partition {
    antennas: Vec<AntennaId>,
    times: Option<RangeInclusive<f64>>,
}

/// What converting `observation` (at `source`) into `destination` carries
/// of its pointing, and the losses that implies; each partition's pointing
/// within `budget` bytes.
///
/// # Errors
///
/// [`ObservationError::InvalidRequest`] when a partition's pointing would
/// hold more than `budget`; any error reading the source.
pub(super) fn plan(
    source: &Path,
    observation: &dyn Observation,
    destination: Destination,
    budget: usize,
) -> Result<(PointingPlan, Vec<Loss>), ObservationError> {
    let partitions = partitions(observation)?;
    if source.join("zarr.json").is_file() {
        return msv4(observation, &partitions, budget);
    }
    let ms = crate::MeasurementSet::open_retained_read(source).map_err(storage)?;
    let Some(table) = ms.subtable(SubtableId::Pointing) else {
        return Ok((PointingPlan::default(), Vec::new()));
    };
    if table.row_count() == 0 {
        return Ok((PointingPlan::default(), Vec::new()));
    }
    msv2(table, &partitions, destination, budget)
}

/// Each partition's antennas (those its baselines name) and times.
fn partitions(observation: &dyn Observation) -> Result<Vec<Partition>, ObservationError> {
    let catalogue = observation.catalogue();
    catalogue
        .partition_ids()
        .into_iter()
        .map(|id| {
            let Some(group) = catalogue.partition(id).data_groups.first() else {
                return Ok(Partition {
                    antennas: Vec::new(),
                    times: None,
                });
            };
            let grid = observation.grid(id, &group.name)?;
            let mut antennas: Vec<AntennaId> = grid
                .baselines
                .iter()
                .flat_map(|baseline| [baseline.antenna1, baseline.antenna2])
                .collect();
            antennas.sort_unstable();
            antennas.dedup();
            let times = grid
                .times_s
                .first()
                .zip(grid.times_s.last())
                .map(|(first, last)| *first..=*last);
            Ok(Partition { antennas, times })
        })
        .collect()
}

/// An MSv4 source: each dataset's pointing whole, read once here to find
/// what the destination cannot hold.
fn msv4(
    observation: &dyn Observation,
    partitions: &[Partition],
    budget: usize,
) -> Result<(PointingPlan, Vec<Loss>), ObservationError> {
    let mut spans = Vec::with_capacity(partitions.len());
    let mut frames = Vec::new();
    for (index, partition) in partitions.iter().enumerate() {
        let request = PointingRequest {
            partition: PartitionId(index as u32),
            direction: PointingDirection::Beam,
            antennas: partition.antennas.clone(),
            times: f64::MIN..=f64::MAX,
            budget_bytes: budget,
        };
        let series = match observation.pointing(&request) {
            Ok(series) => series,
            Err(ObservationError::Unsupported { what }) => {
                let plan = PointingPlan::default();
                return Ok((plan, vec![Loss::Table(format!("pointing_xds ({what})"))]));
            }
            Err(error) => return Err(error),
        };
        if series.antennas.is_empty() {
            spans.push(None);
            continue;
        }
        if !frames.contains(&series.frame) {
            frames.push(series.frame);
        }
        spans.push(Some(f64::MIN..=f64::MAX));
    }
    if frames.len() > 1 {
        let loss = Loss::Table("pointing_xds (directions in several frames)".to_string());
        return Ok((PointingPlan::default(), vec![loss]));
    }
    Ok((PointingPlan { spans }, Vec::new()))
}

/// One `POINTING` row's scalar metadata.
#[derive(Clone, Copy)]
struct Candidate {
    row: usize,
    antenna: i32,
    time: f64,
    interval: f64,
}

impl Candidate {
    /// Whether the MSv2 adapter reads this row for a span of `times`: its
    /// interval meets them or its time is within them (as
    /// `PreparedSelectedPointingCatalog` retains rows); into an MSv2 the
    /// span is every row's.
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
struct Scan {
    frames: Vec<DirectionRef>,
    most_terms: usize,
    /// The earliest and latest row time.
    times: Option<(f64, f64)>,
    /// Rows no partition reads within its times.
    beyond: usize,
    /// Each partition's rows within its times.
    carried: Vec<usize>,
    /// Each partition's antennas' nearest rows before and after its times,
    /// as the MSv2 adapter picks them (the latest before and the earliest
    /// after, the lowest row of a tie).
    nearest: Vec<Vec<[Option<Candidate>; 2]>>,
    /// The (antenna, time) of the rows some partition reads.
    keys: Vec<(i32, u64)>,
}

impl Scan {
    /// Account for `candidate` among `partitions`.
    fn add(&mut self, candidate: Candidate, partitions: &[Partition], destination: Destination) {
        self.times = Some(
            self.times
                .map_or((candidate.time, candidate.time), |(first, last)| {
                    (first.min(candidate.time), last.max(candidate.time))
                }),
        );
        let mut within = false;
        for (index, partition) in partitions.iter().enumerate() {
            let Some(slot) = u32::try_from(candidate.antenna)
                .ok()
                .and_then(|id| partition.antennas.binary_search(&AntennaId(id)).ok())
            else {
                continue;
            };
            let Some(times) = &partition.times else {
                continue;
            };
            if candidate.within(times, destination) {
                self.carried[index] += 1;
                within = true;
                continue;
            }
            let [before, after] = &mut self.nearest[index][slot];
            let earlier = |known: &Candidate| (known.time, known.row);
            if candidate.time < *times.start()
                && before.is_none_or(|known| {
                    candidate.time > known.time
                        || (candidate.time == known.time && candidate.row < known.row)
                })
            {
                *before = Some(candidate);
            }
            if candidate.time > *times.end()
                && after.is_none_or(|known| (candidate.time, candidate.row) < earlier(&known))
            {
                *after = Some(candidate);
            }
        }
        if within {
            self.keys
                .push((candidate.antenna, candidate.time.to_bits()));
        } else {
            self.beyond += 1;
        }
    }

    /// The rows read beyond every partition's times: each antenna's nearest
    /// either side, once each, that no partition reads within its times.
    fn nearest_beyond(&mut self, partitions: &[Partition], destination: Destination) -> usize {
        let mut nearest: Vec<Candidate> = self
            .nearest
            .iter()
            .flatten()
            .flatten()
            .flatten()
            .copied()
            .collect();
        nearest.sort_unstable_by_key(|candidate| candidate.row);
        nearest.dedup_by_key(|candidate| candidate.row);
        let mut count = 0;
        for candidate in nearest {
            let read_within = partitions.iter().any(|partition| {
                u32::try_from(candidate.antenna)
                    .is_ok_and(|id| partition.antennas.binary_search(&AntennaId(id)).is_ok())
                    && partition
                        .times
                        .as_ref()
                        .is_some_and(|times| candidate.within(times, destination))
            });
            if !read_within {
                count += 1;
                self.keys
                    .push((candidate.antenna, candidate.time.to_bits()));
            }
        }
        count
    }
}

/// An MSv2 source: scan `POINTING` once.
fn msv2(
    table: &Table,
    partitions: &[Partition],
    destination: Destination,
    budget: usize,
) -> Result<(PointingPlan, Vec<Loss>), ObservationError> {
    let mut scan = scan(table, partitions, destination)?;
    let outside = scan.beyond - scan.nearest_beyond(partitions, destination);
    let whole = |reason: &str| {
        Ok((
            PointingPlan::default(),
            vec![Loss::Table(format!("POINTING ({reason})"))],
        ))
    };
    let frame = match scan.frames.as_slice() {
        [frame] => *frame,
        _ => return whole("directions in several frames"),
    };
    if destination == Destination::Msv4 && !POINTING_FRAMES.iter().any(|(_, known)| *known == frame)
    {
        return whole(&format!(
            "directions in {}, which MSv4 does not name",
            frame.as_str()
        ));
    }
    if scan.most_terms > MAXIMUM_TERMS {
        return whole("rows of more than four polynomial terms");
    }
    let mut losses = lost_columns(table, destination);
    if destination == Destination::Msv4 && scan.most_terms > 1 {
        losses.push(Loss::Table(
            "POINTING polynomial terms (each row's direction at its own time is kept)".to_string(),
        ));
    }
    let mut keys = scan.keys;
    keys.sort_unstable();
    let repeated = keys.windows(2).filter(|pair| pair[0] == pair[1]).count();
    if destination == Destination::Msv4 && repeated > 0 {
        losses.push(Loss::Table(format!(
            "POINTING rows at the antenna and time of an earlier one ({repeated})"
        )));
    }
    if outside > 0 {
        losses.push(Loss::Table(match destination {
            Destination::Msv4 => format!(
                "POINTING rows beyond every partition's times and each antenna's nearest \
                 either side ({outside})"
            ),
            Destination::Msv2 => {
                format!("POINTING rows of antennas no partition names ({outside})")
            }
        }));
    }
    // A sample's time, interval and origin, and its coefficients.
    let sample = (3 + 2 * scan.most_terms) * size_of::<f64>();
    for (index, (partition, rows)) in partitions.iter().zip(&scan.carried).enumerate() {
        let bytes = (rows + 2 * partition.antennas.len()) * sample;
        if bytes > budget {
            return Err(request_error(format!(
                "the pointing of partition {index} holds about {bytes} bytes, beyond the batch \
                 budget of {budget}"
            )));
        }
    }
    let span = scan.times.map(|(first, last)| first..=last);
    let spans = partitions
        .iter()
        .map(|partition| match destination {
            Destination::Msv4 => partition.times.clone(),
            Destination::Msv2 => partition.times.as_ref().and(span.clone()),
        })
        .collect();
    Ok((PointingPlan { spans }, losses))
}

/// Scan `POINTING`'s antennas, times, intervals, polynomial orders and
/// direction frames ([`Scan`]).
fn scan(
    table: &Table,
    partitions: &[Partition],
    destination: Destination,
) -> Result<Scan, ObservationError> {
    let variable = match TableMeasDesc::reconstruct(table, "DIRECTION") {
        Some(description) => !matches!(description.ref_desc(), MeasRefDesc::Fixed { .. }),
        None => false,
    };
    let frame = |row: usize| {
        resolve_direction_reference_selected(table, "POINTING", "DIRECTION", row).map_err(storage)
    };
    let mut scan = Scan {
        carried: vec![0; partitions.len()],
        nearest: partitions
            .iter()
            .map(|partition| vec![[None, None]; partition.antennas.len()])
            .collect(),
        ..Scan::default()
    };
    if !variable {
        scan.frames.push(frame(0)?);
    }
    let rows = table.row_count();
    let (mut antennas, mut times, mut intervals, mut orders) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for start in (0..rows).step_by(SCAN_ROWS) {
        let chunk: Vec<usize> = (start..rows.min(start + SCAN_ROWS)).collect();
        for values in [&mut antennas, &mut orders] {
            values.clear();
        }
        times.clear();
        intervals.clear();
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
                "NUM_POLY",
                RequiredScalarColumnValuesMut::Int32(&mut orders),
            ),
        ];
        table
            .required_scalar_columns_for_rows_into(&chunk, &mut destinations)
            .map_err(storage)?;
        for (slot, row) in chunk.iter().enumerate() {
            if variable {
                let own = frame(*row)?;
                if !scan.frames.contains(&own) {
                    scan.frames.push(own);
                }
            }
            let terms = usize::try_from(orders[slot]).map_or(usize::MAX, |order| order + 1);
            scan.most_terms = scan.most_terms.max(terms);
            let candidate = Candidate {
                row: *row,
                antenna: antennas[slot],
                time: times[slot],
                interval: intervals[slot],
            };
            scan.add(candidate, partitions, destination);
        }
    }
    Ok(scan)
}

/// `POINTING`'s columns the destination does not hold, as one table loss:
/// both hold each row's antenna, time and direction (its polynomial in
/// time and the origin of it), and an MSv2 its interval; MSv4 states no
/// interval and no target.
fn lost_columns(table: &Table, destination: Destination) -> Vec<Loss> {
    let mut carried = vec!["ANTENNA_ID", "TIME", "DIRECTION", "NUM_POLY", "TIME_ORIGIN"];
    if destination == Destination::Msv2 {
        carried.push("INTERVAL");
    }
    let lost: Vec<String> = table
        .schema()
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .filter(|name| !carried.contains(&name.as_str()))
                .collect()
        })
        .unwrap_or_default();
    if lost.is_empty() {
        Vec::new()
    } else {
        vec![Loss::Table(format!("POINTING {}", lost.join(", ")))]
    }
}
