// SPDX-License-Identifier: LGPL-3.0-or-later
//! What each output partition's grids hold, read before anything is
//! written: its contributors' grids (one interval, and one field and scan
//! at each time, as the MSv2 reader requires of a partition), the times
//! they span, and the rows it will hold: its grids' cells, or with padding
//! omitted its acquisitions, counted from the weights. With several
//! contributors an acquired cell is kept as one bit of the combined grid,
//! and a cell two inputs both acquire is refused, naming them. Times are
//! compared as `MainRows` writes them, MJD seconds from the grid's origin.

use casa_observation::{ObservationError, ObservationId, OrderedBaseline, Roles, TimeBaselineGrid};
use casa_types::measures::MjdHighPrec;
use ndarray::Axis;

use super::combine::Combined;
use super::inputs::Input;
use super::msv2_rows::{Occupancy, Spans, acquired};
use super::{PaddingRows, lockstep};
use crate::observation_io::update::request_error;

/// The occupancy pass's roles: the weights, whose non-finite values are
/// padding's one mark.
const WEIGHTS: Roles = Roles {
    values: false,
    flags: false,
    weights: true,
    uvw: false,
    time_centroid: false,
    interval: false,
    exposure: false,
    scan: false,
};

/// Seconds per day, for times as MJD seconds.
const SECONDS_PER_DAY: f64 = 86_400.0;

/// `grid`'s time `t` as MJD seconds, as a batch of its partition states
/// it (`EpochView::seconds_since` from MJD 0).
fn seconds(grid: &TimeBaselineGrid, t: usize) -> f64 {
    let origin = grid.time_origin;
    let zero = MjdHighPrec::new(0.0, 0.0);
    ((origin.day() - zero.day()) + (origin.frac() - zero.frac())) * SECONDS_PER_DAY
        + grid.times_s[t]
}

/// The times grids span, gathered grid by grid.
struct Spanning {
    range: [f64; 2],
    sessions: Vec<[f64; 2]>,
}

impl Spanning {
    const EMPTY: [f64; 2] = [f64::INFINITY, f64::NEG_INFINITY];

    fn new(sessions: usize) -> Self {
        Self {
            range: Self::EMPTY,
            sessions: vec![Self::EMPTY; sessions],
        }
    }

    /// Widen the spans by `grid`, of observing session `session`.
    fn add(&mut self, grid: &TimeBaselineGrid, session: ObservationId) {
        if grid.times_s.is_empty() {
            return;
        }
        let (first, last) = (seconds(grid, 0), seconds(grid, grid.times_s.len() - 1));
        let span =
            |known: [f64; 2], half: f64| [known[0].min(first - half), known[1].max(last + half)];
        self.range = span(self.range, 0.0);
        // A session's TIME_RANGE spans its integrations, edge to edge, as
        // fillers write it.
        let half = if grid.interval_s.is_finite() {
            grid.interval_s / 2.0
        } else {
            0.0
        };
        if let Some(known) = self.sessions.get_mut(session.0 as usize) {
            *known = span(*known, half);
        }
    }

    fn finish(self) -> Spans {
        let spanned = |range: [f64; 2]| {
            if range[0] > range[1] {
                [0.0, 0.0]
            } else {
                range
            }
        };
        Spans {
            time_range: spanned(self.range),
            observation_ranges: self.sessions.into_iter().map(spanned).collect(),
        }
    }
}

/// One output partition's combined grid: its times, and the field and
/// scan number of each.
struct Combination {
    times: Vec<f64>,
    fields: Vec<Option<i32>>,
    scans: Vec<Option<i32>>,
}

impl Combination {
    /// The combined grid of `grids`, each with its contributor's field and
    /// scan number at each time (`identities`).
    fn new(
        grids: &[TimeBaselineGrid],
        identities: &[Vec<(i32, i32)>],
    ) -> Result<Self, ObservationError> {
        let mut times: Vec<f64> = grids
            .iter()
            .flat_map(|grid| (0..grid.times_s.len()).map(|t| seconds(grid, t)))
            .collect();
        times.sort_by(f64::total_cmp);
        times.dedup_by(|a, b| a.to_bits() == b.to_bits());
        let mut combination = Self {
            fields: vec![None; times.len()],
            scans: vec![None; times.len()],
            times,
        };
        for (grid, identities) in grids.iter().zip(identities) {
            for (t, (field, scan)) in identities.iter().enumerate() {
                let index = combination.time(seconds(grid, t))?;
                for (known, value, what) in [
                    (&mut combination.fields[index], *field, "fields"),
                    (&mut combination.scans[index], *scan, "scans"),
                ] {
                    match known {
                        Some(known) if *known != value => {
                            return Err(request_error(format!(
                                "the inputs combined into one partition hold two {what} at time \
                                 {} s; an MSv2 partition holds one",
                                combination.times[index]
                            )));
                        }
                        Some(_) => {}
                        None => *known = Some(value),
                    }
                }
            }
        }
        Ok(combination)
    }

    /// The position of time `seconds` on the combined grid.
    fn time(&self, seconds: f64) -> Result<usize, ObservationError> {
        self.times
            .binary_search_by(|time| time.total_cmp(&seconds))
            .map_err(|_| request_error("a batch's time is not on its partition's grid"))
    }
}

/// What the output partitions' grids hold, read from the indexes, before
/// any weight is.
pub(super) struct Grids {
    combinations: Vec<Combination>,
    intervals: Vec<f64>,
    /// Each output partition's grid cells: its rows with padding flagged.
    cells: Vec<usize>,
    /// The times the grids span.
    pub(super) spans: Spans,
}

/// The grids of `combined`'s output partitions, from `inputs`: each one's
/// combined grid checked for one interval and one field and scan at a
/// time, and the times they span.
pub(super) fn grids(inputs: &[Input], combined: &Combined) -> Result<Grids, ObservationError> {
    let output = &combined.catalogue;
    let mut spanned = Spanning::new(output.observations.len());
    let count = output.partitions.len();
    let mut read = Grids {
        combinations: Vec::with_capacity(count),
        intervals: Vec::with_capacity(count),
        cells: Vec::with_capacity(count),
        spans: Spanning::new(0).finish(),
    };
    for (index, contributors) in combined.contributors.iter().enumerate() {
        let mut grids = Vec::with_capacity(contributors.len());
        let mut identities = Vec::with_capacity(contributors.len());
        for (input, id) in contributors {
            let map = &combined.rows[*input];
            let placement = &map.partitions[id.0 as usize];
            let grid = inputs[*input].observation.grid(*id, &placement.groups[0])?;
            spanned.add(&grid, output.partitions[index].observation);
            identities.push(
                grid.field_per_time
                    .iter()
                    .zip(&grid.scan_per_time)
                    .map(|(field, scan)| {
                        (
                            map.fields[field.0 as usize],
                            map.scans[scan.0 as usize] + placement.scan_offset,
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            grids.push(grid);
        }
        read.intervals.push(one_interval(&grids)?);
        read.cells.push(
            grids
                .iter()
                .map(|grid| grid.times_s.len() * grid.baselines.len())
                .sum(),
        );
        read.combinations
            .push(Combination::new(&grids, &identities)?);
    }
    read.spans = spanned.finish();
    Ok(read)
}

impl Grids {
    /// The occupancy of `combined`'s output partitions under `padding`:
    /// their grid cells, or with padding omitted their acquisitions, read
    /// from `inputs`' weights with `budget` bytes per batch.
    pub(super) fn occupancy(
        self,
        inputs: &[Input],
        combined: &Combined,
        (padding, budget): (PaddingRows, usize),
    ) -> Result<Occupancy, ObservationError> {
        let rows = match padding {
            PaddingRows::Flagged => self.cells,
            PaddingRows::Omitted => combined
                .contributors
                .iter()
                .zip(&self.combinations)
                .zip(&combined.catalogue.partitions)
                .map(|((contributors, combination), entry)| {
                    acquisitions(
                        inputs,
                        combined,
                        (contributors, &entry.baselines),
                        combination,
                        budget,
                    )
                })
                .collect::<Result<_, _>>()?,
        };
        Ok(Occupancy {
            rows,
            intervals: self.intervals,
            spans: self.spans,
        })
    }
}

/// The times one observation's grids of `group` span, for subtables
/// written without rows.
#[cfg(feature = "msv4")]
pub(super) fn spans(
    observation: &dyn casa_observation::Observation,
    group: &str,
) -> Result<Spans, ObservationError> {
    let catalogue = observation.catalogue();
    let mut spanned = Spanning::new(catalogue.observations.len());
    for id in catalogue.partition_ids() {
        spanned.add(
            &observation.grid(id, group)?,
            catalogue.partition(id).observation,
        );
    }
    Ok(spanned.finish())
}

/// The one integration interval of `grids`, which an MSv2 partition
/// shares; a grid of no acquisition states none.
fn one_interval(grids: &[TimeBaselineGrid]) -> Result<f64, ObservationError> {
    let mut stated = grids
        .iter()
        .map(|grid| grid.interval_s)
        .filter(|interval| interval.is_finite());
    let Some(first) = stated.next() else {
        return Ok(f64::NAN);
    };
    if stated.any(|interval| interval.to_bits() != first.to_bits()) {
        return Err(ObservationError::Unsupported {
            what: "combining partitions of different integration intervals",
        });
    }
    Ok(first)
}

/// The acquisitions of an output partition's `contributors`, whose
/// combined grid is `combination` over its `baselines`: each contributor's
/// weights read, and a cell acquired twice refused.
fn acquisitions(
    inputs: &[Input],
    combined: &Combined,
    (contributors, baselines): (
        &[(usize, casa_observation::PartitionId)],
        &[OrderedBaseline],
    ),
    combination: &Combination,
    budget: usize,
) -> Result<usize, ObservationError> {
    let cells = combination.times.len() * baselines.len();
    let mut filled = if contributors.len() > 1 {
        vec![0_u64; cells.div_ceil(64)]
    } else {
        Vec::new()
    };
    let mut count = 0;
    for (position, (input, id)) in contributors.iter().enumerate() {
        let map = &combined.rows[*input];
        let placement = &map.partitions[id.0 as usize];
        let groups = [placement.groups[0].clone()];
        lockstep::read(
            inputs[*input].observation.as_ref(),
            *id,
            &groups,
            WEIGHTS,
            (budget, None),
            |batches| {
                let batch = &batches[0];
                let weights = batch
                    .group
                    .weights
                    .ok_or_else(|| request_error("an occupancy batch carries its weights"))?;
                let origin = MjdHighPrec::new(0.0, 0.0);
                for (sample, weights) in weights.axis_iter(Axis(0)).enumerate() {
                    if !acquired(weights)? {
                        continue;
                    }
                    count += 1;
                    if filled.is_empty() {
                        continue;
                    }
                    let time = combination.time(
                        batch
                            .time
                            .seconds_since(sample / batch.samples_per_time(), origin),
                    )?;
                    let baseline = batch.sample(sample).baseline;
                    let antenna = |antenna: casa_observation::AntennaId| {
                        casa_observation::AntennaId(map.antennas[antenna.0 as usize] as u32)
                    };
                    let remapped = OrderedBaseline {
                        antenna1: antenna(baseline.antenna1),
                        antenna2: antenna(baseline.antenna2),
                    };
                    let b = baselines
                        .binary_search(&remapped)
                        .map_err(|_| request_error("a batch's baseline is not on its grid"))?;
                    let cell = time * baselines.len() + b;
                    let (word, bit) = (cell / 64, 1_u64 << (cell % 64));
                    if filled[word] & bit != 0 {
                        let earlier: Vec<usize> = contributors[..position]
                            .iter()
                            .map(|(input, _)| *input)
                            .collect();
                        return Err(request_error(format!(
                            "input {input} acquires baseline {}-{} at {} s, which input(s) \
                             {earlier:?} also acquire; an MSv2 holds one row there",
                            remapped.antenna1.0, remapped.antenna2.0, combination.times[time]
                        )));
                    }
                    filled[word] |= bit;
                }
                Ok(())
            },
        )?;
    }
    Ok(count)
}
