// SPDX-License-Identifier: LGPL-3.0-or-later
//! The pointing samples batch geometry reads: one query a partition, over
//! the span of its selected times, kept while the next partition read, in
//! this traversal or the next, asks for the same antennas over the same
//! span. A store reads its whole pointing table for a query (the MSv2
//! adapter scans `POINTING`), so a query a batch would read the table once
//! a batch; a partition's spectral windows observed together share one.

use casa_imaging_model::{CompiledProblem, PointingCentreLaw, PointingDirectionColumn};
use casa_imaging_runtime::pass::SourceError;
use casa_observation::{AntennaId, PointingDirection, PointingRequest, PointingSeries};

use super::BatchRoute;
use super::selection::SelectedPartition;

/// What a pointing query asked for, but the partition: its direction,
/// antennas and span (as bits).
type Query = (PointingDirection, Vec<AntennaId>, [u64; 2]);

/// The pointing samples of the partition being read.
#[derive(Debug, Default)]
pub(super) struct PointingCache {
    held: Option<(Query, PointingSeries)>,
}

impl PointingCache {
    /// The samples held.
    pub(super) fn series(&self) -> Option<&PointingSeries> {
        self.held.as_ref().map(|(_, series)| series)
    }

    /// Hold `partition`'s samples over `span`, in the direction `problem`'s
    /// pointing law reads, within the route's admitted budget: the samples
    /// held already when their query was the same.
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
        let direction = match problem.geometry().centres().pointing() {
            PointingCentreLaw::Observation(law)
                if law.direction_column() == PointingDirectionColumn::Target =>
            {
                PointingDirection::Target
            }
            _ => PointingDirection::Beam,
        };
        let query = (direction, antennas, span.map(f64::to_bits));
        if self.held.as_ref().is_some_and(|(held, _)| *held == query) {
            return Ok(());
        }
        // The samples held go before the next are read: the charge holds
        // one query's reading and its answer at once.
        self.held = None;
        let series = route.observation.pointing(&PointingRequest {
            partition: partition.id,
            direction,
            antennas: query.1.clone(),
            times: span[0]..=span[1],
            budget_bytes: route.pointing_budget,
        })?;
        self.held = Some((query, series));
        Ok(())
    }
}
