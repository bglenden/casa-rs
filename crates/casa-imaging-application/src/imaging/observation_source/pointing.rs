// SPDX-License-Identifier: LGPL-3.0-or-later
//! The pointing samples batch geometry reads: one query a partition, over
//! the span of its selected times. An antenna's pointing is a property of
//! the antenna and the time, so partitions that ask for the same antennas
//! over the same span in the same time system share an answer, whatever
//! their spectral windows or intents. The source keeps every answer that
//! fits the pointing charge, across partitions and passes, so a store
//! whose query reads a whole table (the MSv2 adapter scans `POINTING`)
//! reads it once a distinct query, not once a partition read; an answer
//! that does not fit takes the place of those least recently read.

use casa_imaging_model::{CompiledProblem, PointingCentreLaw, PointingDirectionColumn};
use casa_imaging_runtime::pass::SourceError;
use casa_observation::{AntennaId, PointingDirection, PointingRequest, PointingSeries};
use casa_types::measures::EpochRef;

use super::BatchRoute;
use super::selection::SelectedPartition;

/// What a pointing query asked for, but the partition: its direction,
/// antennas and span (as bits), and the time system the span is in (the
/// partition's time origin, as bits, and scale).
#[derive(Debug, PartialEq)]
struct Query {
    direction: PointingDirection,
    antennas: Vec<AntennaId>,
    span: [u64; 2],
    origin: [u64; 2],
    scale: EpochRef,
}

/// One answer kept, with the bytes it holds.
#[derive(Debug)]
struct Held {
    query: Query,
    series: PointingSeries,
    bytes: usize,
}

/// The pointing answers the source keeps, least recently read first: the
/// last is the partition being read's.
#[derive(Debug, Default)]
pub(super) struct PointingCache {
    held: Vec<Held>,
}

impl PointingCache {
    /// The samples of the partition being read.
    pub(super) fn series(&self) -> Option<&PointingSeries> {
        self.held.last().map(|held| &held.series)
    }

    /// Hold `partition`'s samples over `span`, in the direction `problem`'s
    /// pointing law reads, within the route's admitted budget: the answer
    /// kept already when its query was the same. A new query may hold what
    /// the answers kept leave of the budget; if the store refuses it that,
    /// they go and it is asked again with the whole budget, under which no
    /// query is refused for its size.
    pub(super) fn hold(
        &mut self,
        route: &BatchRoute,
        problem: &CompiledProblem,
        partition: &SelectedPartition,
        span: [f64; 2],
    ) -> Result<(), SourceError> {
        let entry = route.observation.catalogue().partition(partition.id);
        let mut antennas: Vec<AntennaId> = entry
            .baselines
            .iter()
            .flat_map(|baseline| [baseline.antenna1, baseline.antenna2])
            .collect();
        antennas.sort_unstable();
        antennas.dedup();
        let query = Query {
            direction: pointing_direction(problem),
            antennas,
            span: span.map(f64::to_bits),
            origin: [
                entry.time_origin.day().to_bits(),
                entry.time_origin.frac().to_bits(),
            ],
            scale: entry.time_scale,
        };
        if let Some(index) = self.held.iter().position(|held| held.query == query) {
            let held = self.held.remove(index);
            self.held.push(held);
            return Ok(());
        }
        let request = |budget_bytes| PointingRequest {
            partition: partition.id,
            direction: query.direction,
            antennas: query.antennas.clone(),
            times: span[0]..=span[1],
            budget_bytes,
        };
        let budget = route.pointing_budget;
        let kept = (!self.held.is_empty())
            .then(|| {
                let free = budget.saturating_sub(self.held_bytes());
                route.observation.pointing(&request(free)).ok()
            })
            .flatten();
        let series = match kept {
            Some(series) => series,
            None => {
                self.held.clear();
                route.observation.pointing(&request(budget))?
            }
        };
        let bytes = series.held_bytes();
        while !self.held.is_empty() && self.held_bytes() + bytes > budget {
            self.held.remove(0);
        }
        self.held.push(Held {
            query,
            series,
            bytes,
        });
        Ok(())
    }

    /// Bytes the answers kept hold.
    fn held_bytes(&self) -> usize {
        self.held.iter().map(|held| held.bytes).sum()
    }
}

/// The pointing direction `problem`'s pointing law reads.
pub(crate) fn pointing_direction(problem: &CompiledProblem) -> PointingDirection {
    match problem.geometry().centres().pointing() {
        PointingCentreLaw::Observation(law)
            if law.direction_column() == PointingDirectionColumn::Target =>
        {
            PointingDirection::Target
        }
        _ => PointingDirection::Beam,
    }
}
