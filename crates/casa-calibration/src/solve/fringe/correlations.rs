// SPDX-License-Identifier: LGPL-3.0-or-later
//! Form the combined parallel-hand visibility before the fringe search.

use std::collections::BTreeMap;

use super::{FringeBaseline, FringeCorrelation, FringeSample};

pub(super) fn combine(
    hands: [Vec<FringeBaseline>; 2],
    combination: FringeCorrelation,
) -> Vec<FringeBaseline> {
    let mut by_baseline = BTreeMap::<_, BTreeMap<_, [Option<FringeSample>; 2]>>::new();
    for (hand, baselines) in hands.into_iter().enumerate() {
        for baseline in baselines {
            let by_channel = by_baseline.entry(baseline.antennas).or_default();
            for sample in baseline.samples {
                let key = (
                    sample.data_description,
                    sample.field_id,
                    sample.time_seconds.to_bits(),
                    sample.local_channel,
                );
                by_channel.entry(key).or_default()[hand] = Some(sample);
            }
        }
    }
    by_baseline
        .into_iter()
        .filter_map(|(antennas, channels)| {
            let samples = channels
                .into_values()
                .filter_map(|hands| combine_sample(hands, combination))
                .collect::<Vec<_>>();
            (!samples.is_empty()).then_some(FringeBaseline { antennas, samples })
        })
        .collect()
}

fn combine_sample(
    hands: [Option<FringeSample>; 2],
    combination: FringeCorrelation,
) -> Option<FringeSample> {
    match hands {
        [Some(left), Some(right)] => {
            let (visibility, weight) = match combination {
                FringeCorrelation::Stokes => (
                    (left.visibility + right.visibility) * 0.5,
                    4. / (left.weight.recip() + right.weight.recip()),
                ),
                FringeCorrelation::Parallel => {
                    let total = left.weight + right.weight;
                    (
                        (left.visibility * left.weight + right.visibility * right.weight) / total,
                        total,
                    )
                }
                FringeCorrelation::Separate => unreachable!("combining mode"),
            };
            Some(FringeSample {
                visibility,
                weight,
                ..left
            })
        }
        [Some(value), None] | [None, Some(value)] if combination == FringeCorrelation::Parallel => {
            Some(value)
        }
        _ => None,
    }
}
