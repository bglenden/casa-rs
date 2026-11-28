// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the source learns of each partition before it reads payload, from
//! one traversal that reads none: the span of its selected times, which a
//! pointing query covers, and, for restricted waves, the range of
//! output-frame conversion factors its selected (field, time)s span.

use std::collections::HashMap;

use casa_core::measures::{MjdHighPrec, epoch::EpochRef};
use casa_imaging_model::FrequencyBands;
use casa_imaging_runtime::pass::SourceError;
use casa_observation::{BatchBuffers, FieldId, PartitionEntry, ReadEvent};

use super::StoreRoute;
use super::pack::channel_meets_band;
use super::requests;
use super::selection::{PartitionSelection, SelectedPartition};
use super::window::{FactorRange, InterpolationPartners};

/// One partition surveyed.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct PartitionSurvey {
    /// The first and last of its selected times, their time centroids
    /// included when geometry reads them, in seconds from the partition's
    /// time origin; `None` when no time is selected.
    pub(super) span: Option<[f64; 2]>,
    /// The conversion factors its selected (field, time)s span, when
    /// surveyed.
    pub(super) factors: Option<FactorRange>,
    /// Adjacent interpolation pairs exposed by moving frequency bands.
    pub(super) partners: Option<InterpolationPartners>,
}

/// Bytes for the retained survey entries and the pairs of a moving linear
/// selection. The batching admission holds both while the source is open.
pub(super) fn retained_bytes(selection: &PartitionSelection, linear: bool) -> usize {
    size_of::<PartitionSurvey>() * selection.partitions.len()
        + selection
            .partitions
            .iter()
            .filter(|partition| linear && partition.frequency_bands.is_some())
            .map(|partition| InterpolationPartners::bytes(partition.channels.len()))
            .sum::<usize>()
}

/// Each of one store's `route`'s partitions surveyed, with their conversion
/// factors when `factors`. The survey's batches hold at most its admitted
/// `survey_bytes` ([`super::Batching::survey_bytes`]), in buffers of their
/// own. Partitions observed at the same times towards the same field in
/// the same frame share each conversion.
pub(super) fn survey(
    route: &StoreRoute,
    survey_bytes: usize,
    factors: bool,
    linear: bool,
) -> Result<Vec<PartitionSurvey>, SourceError> {
    let catalogue = route.observation().catalogue();
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
                survey_bytes,
            );
            let mut traversal = route.observation().bind_read(&request)?;
            let mut span: Option<[f64; 2]> = None;
            let mut range = FactorRange::EMPTY;
            let bands = partition
                .frequency_bands
                .as_ref()
                .filter(|_| factors && linear);
            let mut partners = bands.map(|_| InterpolationPartners::new(partition.channels.len()));
            while let Some(event) = traversal.next(&mut buffers)? {
                let ReadEvent::Batch(batch) = event else {
                    continue;
                };
                let centroids = batch.timing.centroid.map(|centroid| centroid.offsets_s());
                let times = batch.time.offsets_s();
                let scale = batch.time.reference();
                let baselines = batch.samples_per_time();
                for time in 0..batch.times() {
                    if !route.selection.time_holds(&batch, time)? {
                        continue;
                    }
                    let mut include = |time_s: f64| {
                        let [low, high] = span.get_or_insert([time_s; 2]);
                        *low = low.min(time_s);
                        *high = high.max(time_s);
                    };
                    include(times[time]);
                    if let Some(centroids) = &centroids {
                        for sample in time * baselines..(time + 1) * baselines {
                            include(centroids[sample]);
                        }
                    }
                    if !factors {
                        continue;
                    }
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
                    if let Some((bands, partners)) = bands.zip(partners.as_mut()) {
                        include_partners(
                            route,
                            partition,
                            entry,
                            bands,
                            (field, time_mjd_s, scale),
                            partners,
                        )?;
                    }
                }
            }
            Ok(PartitionSurvey {
                span,
                factors: factors.then_some(range),
                partners,
            })
        })
        .collect()
}

/// Find a time's actual input partners using the same converted centres,
/// widths and band predicate as the batch packer. Keep their ordinals in
/// the union selection so the planner can widen a wave before binding it.
fn include_partners(
    route: &StoreRoute,
    partition: &SelectedPartition,
    entry: &PartitionEntry,
    bands: &FrequencyBands,
    (field, epoch, scale): (FieldId, f64, EpochRef),
    partners: &mut InterpolationPartners,
) -> Result<(), SourceError> {
    let plan = &route.plan;
    let convert = plan.frequency_converter(entry.frequency_frame, field, epoch, scale)?;
    let factor = plan.frequency_factor(bands.frame().frequency_ref(), field, epoch, scale)?;
    partners.include_epoch(partition.channels.iter().enumerate().filter_map(
        |(ordinal, &index)| {
            let index = index as usize;
            let native = entry.frequencies_hz[index];
            let half_width = entry.channel_widths_hz[index].abs() / 2.0;
            channel_meets_band(bands, factor, convert(native), native, half_width)
                .then_some(ordinal)
        },
    ));
    Ok(())
}
