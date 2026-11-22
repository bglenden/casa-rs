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
                by_channel.entry(sample.buffer_channel_key()).or_default()[hand] = Some(sample);
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
            samples
                .iter()
                .any(|sample| sample.weight > 0.)
                .then_some(FringeBaseline { antennas, samples })
        })
        .collect()
}

fn combine_sample(
    hands: [Option<FringeSample>; 2],
    combination: FringeCorrelation,
) -> Option<FringeSample> {
    let empty = hands[0].or(hands[1]).map(|sample| FringeSample {
        weight: 0.,
        visibility: Default::default(),
        ..sample
    });
    match hands.map(|hand| hand.filter(|sample| sample.weight > 0.)) {
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
        _ => empty,
    }
}
