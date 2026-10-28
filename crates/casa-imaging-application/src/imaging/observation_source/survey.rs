// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the source learns of each partition before it reads payload, from
//! one traversal that reads none: the span of its selected times, which a
//! pointing query covers, and, for restricted waves, the range of
//! output-frame conversion factors its selected (field, time)s span.

use std::collections::HashMap;

use casa_imaging_runtime::pass::SourceError;
use casa_observation::{BatchBuffers, ReadEvent};
use casa_types::measures::MjdHighPrec;

use super::BatchRoute;
use super::requests;
use super::window::FactorRange;

/// One partition surveyed.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct PartitionSurvey {
    /// The first and last of its selected times, their time centroids
    /// included when geometry reads them, in seconds from the partition's
    /// time origin; `None` when no time is selected.
    pub(super) span: Option<[f64; 2]>,
    /// The conversion factors its selected (field, time)s span, when
    /// surveyed.
    pub(super) factors: Option<FactorRange>,
}

/// Each of `route`'s partitions surveyed, with their conversion factors
/// when `factors`. The survey's batches hold at most its admitted bytes
/// ([`super::Batching::survey_bytes`]), in buffers of their own.
/// Partitions observed at the same times towards the same field in the same
/// frame share each conversion.
pub(super) fn survey(
    route: &BatchRoute,
    factors: bool,
) -> Result<Vec<PartitionSurvey>, SourceError> {
    let catalogue = route.observation.catalogue();
    let mut conversions = HashMap::new();
    let mut buffers = BatchBuffers::default();
    let origin = MjdHighPrec::new(0.0, 0.0);
    route
        .selection
        .partitions
        .iter()
        .map(|partition| {
            let entry = catalogue.partition(partition.id);
            let request = requests::survey(
                (&route.selection, &route.plan),
                entry,
                partition,
                route.batching.survey_bytes(),
            );
            let mut traversal = route.observation.bind_read(&request)?;
            let mut span: Option<[f64; 2]> = None;
            let mut range = FactorRange::EMPTY;
            while let Some(event) = traversal.next(&mut buffers)? {
                let ReadEvent::Batch(batch) = event else {
                    continue;
                };
                let centroids = batch.timing.centroid.map(|centroid| centroid.offsets_s());
                let times = batch.time.offsets_s();
                for time_s in times.iter().chain(centroids.iter().flatten()) {
                    let [low, high] = span.get_or_insert([*time_s; 2]);
                    *low = low.min(*time_s);
                    *high = high.max(*time_s);
                }
                if !factors {
                    continue;
                }
                let scale = batch.time.reference();
                for time in 0..batch.times() {
                    let time_mjd_s = batch.time.seconds_since(time, origin);
                    let field = batch.field[time];
                    let key = (entry.frequency_frame, field, scale, time_mjd_s.to_bits());
                    let factor = match conversions.get(&key) {
                        Some(factor) => *factor,
                        None => {
                            let factor = route.plan.frequency_factor(
                                entry.frequency_frame,
                                field,
                                time_mjd_s,
                                scale,
                            )?;
                            if !(factor.is_finite() && factor > 0.0) {
                                return Err(SourceError::from(
                                    "an output-frame conversion factor is not positive",
                                ));
                            }
                            conversions.insert(key, factor);
                            factor
                        }
                    };
                    range = range.with(factor);
                }
            }
            Ok(PartitionSurvey {
                span,
                factors: factors.then_some(range),
            })
        })
        .collect()
}
