// SPDX-License-Identifier: LGPL-3.0-or-later
//! The pointing samples batch geometry reads: one query a partition, over
//! the span of its selected times. An antenna's pointing is a property of
//! the antenna and the time ([`Observation::pointing`]), so partitions that
//! ask for the same antennas over the same span in the same time system
//! share an answer, whatever their spectral windows or intents. The source
//! keeps the answers across partitions and passes, so a store whose query
//! reads a whole table (the MSv2 adapter scans `POINTING`) reads it once a
//! distinct query, not once a partition read. A new query may hold what
//! the answers kept leave of the pointing charge; when the store refuses it
//! that, the answers kept go and it is asked again with all of it.

use casa_imaging_model::{CompiledProblem, PointingCentreLaw, PointingDirectionColumn};
use casa_imaging_runtime::pass::SourceError;
use casa_observation::{
    AntennaId, Observation, ObservationError, PartitionId, PointingDirection, PointingRequest,
    PointingSeries,
};
use casa_types::measures::EpochRef;

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

/// The pointing answers the source keeps, least recently read first.
#[derive(Debug, Default)]
pub(super) struct PointingCache {
    held: Vec<Held>,
    /// Whether the last kept answer is the partition being read's: not
    /// after [`Self::release`].
    active: bool,
}

impl PointingCache {
    /// The samples of the partition being read; `None` when it was not
    /// given any ([`Self::release`]).
    pub(super) fn series(&self) -> Option<&PointingSeries> {
        self.active
            .then(|| self.held.last().map(|held| &held.series))
            .flatten()
    }

    /// The partition being read asks for no samples (it has no selected
    /// times); the answers stay kept.
    pub(super) fn release(&mut self) {
        self.active = false;
    }

    /// Hold `partition`'s samples in `direction` over `span` from
    /// `observation`, the answers kept within `budget` bytes: the answer
    /// kept already when its query was the same. A new query may hold what
    /// the answers kept leave of `budget`; if the store refuses it that
    /// ([`ObservationError::OverBudget`]), they go and it is asked again
    /// with the whole budget, under which no query is refused for its size.
    pub(super) fn hold(
        &mut self,
        observation: &dyn Observation,
        budget: usize,
        (direction, partition, span): (PointingDirection, PartitionId, [f64; 2]),
    ) -> Result<(), SourceError> {
        let entry = observation.catalogue().partition(partition);
        let mut antennas: Vec<AntennaId> = entry
            .baselines
            .iter()
            .flat_map(|baseline| [baseline.antenna1, baseline.antenna2])
            .collect();
        antennas.sort_unstable();
        antennas.dedup();
        let query = Query {
            direction,
            antennas,
            span: span.map(f64::to_bits),
            origin: [
                entry.time_origin.day().to_bits(),
                entry.time_origin.frac().to_bits(),
            ],
            scale: entry.time_scale,
        };
        self.active = true;
        if let Some(index) = self.held.iter().position(|held| held.query == query) {
            let held = self.held.remove(index);
            self.held.push(held);
            return Ok(());
        }
        let request = |budget_bytes| PointingRequest {
            partition,
            direction,
            antennas: query.antennas.clone(),
            times: span[0]..=span[1],
            budget_bytes,
        };
        let series = if self.held.is_empty() {
            observation.pointing(&request(budget))?
        } else {
            let free = budget.saturating_sub(self.held_bytes());
            match observation.pointing(&request(free)) {
                Ok(series) => series,
                Err(ObservationError::OverBudget { .. }) => {
                    self.held.clear();
                    observation.pointing(&request(budget))?
                }
                Err(error) => return Err(error.into()),
            }
        };
        let bytes = series.held_bytes();
        // The store bounds an answer within what it was given, by its own
        // count; by the series' count the answers kept stay within the
        // budget too.
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use casa_observation::{
        AccessRequest, AntennaPointing, Catalogue, Estimate, FieldSelection, GroupKey, Grouping,
        OrderedBaseline, PartitionEntry, TimeBaselineGrid, Traversal,
    };
    use casa_types::Correlation;
    use casa_types::measures::MjdHighPrec;
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;

    use super::*;

    /// An observation whose pointing answers are counted: each holds one
    /// sample an antenna and needs `need` bytes of budget, or the query is
    /// refused for its size; `fail` makes every query fail otherwise.
    struct Counted {
        catalogue: Arc<Catalogue>,
        need: usize,
        fail: bool,
        budgets: Mutex<Vec<usize>>,
    }

    impl Observation for Counted {
        fn catalogue(&self) -> &Arc<Catalogue> {
            &self.catalogue
        }
        fn estimate(&self, _: &AccessRequest) -> Result<Estimate, ObservationError> {
            unimplemented!("a pointing-only observation")
        }
        fn bind_read(&self, _: &AccessRequest) -> Result<Traversal<'_>, ObservationError> {
            unimplemented!("a pointing-only observation")
        }
        fn grid(&self, _: PartitionId, _: &str) -> Result<TimeBaselineGrid, ObservationError> {
            unimplemented!("a pointing-only observation")
        }
        fn group_keys(
            &self,
            _: PartitionId,
            _: &str,
            _: Grouping,
            _: &FieldSelection,
        ) -> Result<Option<Vec<Option<GroupKey>>>, ObservationError> {
            unimplemented!("a pointing-only observation")
        }
        fn pointing(&self, request: &PointingRequest) -> Result<PointingSeries, ObservationError> {
            self.budgets
                .lock()
                .expect("budgets")
                .push(request.budget_bytes);
            if self.fail {
                return Err(ObservationError::InvalidStore {
                    reason: "unreadable POINTING".to_string(),
                });
            }
            if request.budget_bytes < self.need {
                return Err(ObservationError::OverBudget {
                    what: "a pointing query",
                    required: self.need,
                    budget: request.budget_bytes,
                });
            }
            let mut series = PointingSeries::empty(DirectionRef::J2000);
            for antenna in &request.antennas {
                let samples = AntennaPointing::new(
                    vec![*request.times.start()],
                    None,
                    vec![*request.times.start()],
                    1,
                    vec![0.0, 0.0],
                )?;
                series.antennas.insert(*antenna, samples);
            }
            Ok(series)
        }
    }

    /// A partition of antennas 0 and 1 whose times count from `origin`.
    fn partition(origin: f64) -> PartitionEntry {
        PartitionEntry {
            spectral_window: "spw".to_string(),
            frequencies_hz: vec![1.0e9],
            frequency_frame: FrequencyRef::LSRK,
            channel_widths_hz: vec![1.0e6],
            correlations: vec![Correlation::XX],
            intents: Vec::new(),
            observation: casa_observation::ObservationId(0),
            feeds: None,
            fields: Vec::new(),
            scans: Vec::new(),
            baselines: vec![OrderedBaseline {
                antenna1: AntennaId(0),
                antenna2: AntennaId(1),
            }],
            time_scale: EpochRef::UTC,
            time_origin: MjdHighPrec::new(origin, 0.0),
            data_groups: Vec::new(),
            receptors: BTreeMap::new(),
        }
    }

    /// Partitions 0 and 1 in one time system, 2 in another.
    fn observation(need: usize, fail: bool) -> Counted {
        Counted {
            catalogue: Arc::new(Catalogue {
                partitions: vec![partition(0.0), partition(0.0), partition(1.0)],
                ..Catalogue::default()
            }),
            need,
            fail,
            budgets: Mutex::new(Vec::new()),
        }
    }

    fn ask(
        cache: &mut PointingCache,
        observation: &Counted,
        budget: usize,
        partition: u32,
        span: [f64; 2],
    ) -> Result<(), SourceError> {
        cache.hold(
            observation,
            budget,
            (PointingDirection::Beam, PartitionId(partition), span),
        )
    }

    /// Partitions of one time system share an answer, also when another
    /// span was read between them; a partition in another time system does
    /// not.
    #[test]
    fn answers_are_shared_by_query_and_time_system() {
        let observation = observation(1, false);
        let mut cache = PointingCache::default();
        let budget = 1 << 20;
        for (partition, span) in [(0, [0.0, 10.0]), (1, [20.0, 30.0]), (1, [0.0, 10.0])] {
            ask(&mut cache, &observation, budget, partition, span).expect("pointing");
        }
        assert_eq!(observation.budgets.lock().expect("budgets").len(), 2);
        ask(&mut cache, &observation, budget, 2, [0.0, 10.0]).expect("pointing");
        assert_eq!(observation.budgets.lock().expect("budgets").len(), 3);
        assert!(cache.series().is_some());
        assert!(cache.held_bytes() <= budget);
    }

    /// A query the answers kept leave too little for is refused, and asked
    /// again with the whole budget once they go; every query holds at most
    /// what the answers kept leave.
    #[test]
    fn a_refused_query_is_asked_again_with_the_whole_budget() {
        let probe = observation(1, false);
        let mut cache = PointingCache::default();
        ask(&mut cache, &probe, 1 << 20, 0, [0.0, 10.0]).expect("pointing");
        let answer = cache.held_bytes();
        // Room for one answer and the store's need beside it, not two.
        let need = 2 * answer;
        let budget = answer + need - 1;
        let observation = observation(need, false);
        let mut cache = PointingCache::default();
        ask(&mut cache, &observation, budget, 0, [0.0, 10.0]).expect("first");
        ask(&mut cache, &observation, budget, 0, [20.0, 30.0]).expect("second");
        let budgets = observation.budgets.lock().expect("budgets").clone();
        assert_eq!(budgets, [budget, budget - answer, budget]);
        assert_eq!(cache.held.len(), 1, "the first answer went");
        assert!(cache.held_bytes() <= budget);
    }

    /// A store error other than a refusal for size is not asked again.
    #[test]
    fn a_store_error_is_not_asked_again() {
        let observation = observation(1, true);
        let mut cache = PointingCache::default();
        assert!(ask(&mut cache, &observation, 1 << 20, 0, [0.0, 10.0]).is_err());
        assert_eq!(observation.budgets.lock().expect("budgets").len(), 1);
    }

    /// A partition without selected times gives the geometry no samples,
    /// and the answers kept stay for the next.
    #[test]
    fn a_released_partition_reads_no_samples() {
        let observation = observation(1, false);
        let mut cache = PointingCache::default();
        ask(&mut cache, &observation, 1 << 20, 0, [0.0, 10.0]).expect("pointing");
        cache.release();
        assert!(cache.series().is_none());
        ask(&mut cache, &observation, 1 << 20, 1, [0.0, 10.0]).expect("pointing");
        assert!(cache.series().is_some());
        assert_eq!(observation.budgets.lock().expect("budgets").len(), 1);
    }
}
