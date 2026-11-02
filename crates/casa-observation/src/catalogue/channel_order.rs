// SPDX-License-Identifier: LGPL-3.0-or-later
//! Which order two stores keep one spectral window's channels in.
//!
//! A spectral window whose channels descend in frequency (negative channel
//! widths, as lower-sideband data are often stored) keeps that order in an
//! MSv2 and in casa-rs's own MSv4 conversion, which follow CASA's channel
//! numbering. XRADIO's converter writes every window in ascending frequency,
//! reversing the channel axis and making the widths positive. Both
//! conventions are allowed (#1014): the same window then holds the same
//! channels in opposite orders, and a [`Binding`](super::Binding) states
//! which, so a consumer carrying per-channel results from one store to the
//! other maps channel `c` to [`ChannelOrder::channel`].

use std::ops::Range;

use super::PartitionEntry;

/// How a partition's channels lie along another store's partition of the
/// same content ([`PartitionEntry::channel_order`],
/// [`Binding::channel_orders`](super::Binding::channel_orders)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelOrder {
    /// Channel `c` is the other partition's channel `c`.
    Same,
    /// Channel `c` of `n` is the other partition's channel `n - 1 - c`.
    Reversed,
}

impl ChannelOrder {
    /// The other partition's channel holding channel `channel` of `count`.
    ///
    /// # Panics
    ///
    /// When reversed and `channel` is not below `count`.
    #[must_use]
    pub fn channel(self, channel: usize, count: usize) -> usize {
        match self {
            Self::Same => channel,
            Self::Reversed => {
                assert!(channel < count, "channel {channel} of {count}");
                count - 1 - channel
            }
        }
    }

    /// The other partition's channels holding `channels` of `count`: the
    /// same channels, which run the other way along its axis when
    /// reversed.
    ///
    /// # Panics
    ///
    /// When reversed and `channels` does not lie within `count`.
    #[must_use]
    pub fn channels(self, channels: Range<usize>, count: usize) -> Range<usize> {
        match self {
            Self::Same => channels,
            Self::Reversed => {
                assert!(
                    channels.start <= channels.end && channels.end <= count,
                    "channels {channels:?} of {count}"
                );
                count - channels.end..count - channels.start
            }
        }
    }
}

impl PartitionEntry {
    /// Whether `other` holds this partition's spectral and product content
    /// ([`Self::same_content`]) and in which channel order: the same, or
    /// reversed, when `other`'s channel frequencies are this partition's in
    /// the reverse order and its widths have the same magnitudes in that
    /// order (bit for bit; XRADIO makes a descending window's widths
    /// positive). The same order is preferred, so a partition of one
    /// channel, or of equal frequencies, is never reversed. `None` when
    /// the content differs.
    #[must_use]
    pub fn channel_order(&self, other: &Self) -> Option<ChannelOrder> {
        if self.same_content(other) {
            return Some(ChannelOrder::Same);
        }
        let reversed = |ours: &[f64], theirs: &[f64], bits: fn(f64) -> u64| {
            ours.len() == theirs.len()
                && ours
                    .iter()
                    .rev()
                    .zip(theirs)
                    .all(|(a, b)| bits(*a) == bits(*b))
        };
        (self.frequency_frame == other.frequency_frame
            && reversed(&self.frequencies_hz, &other.frequencies_hz, f64::to_bits)
            && reversed(&self.channel_widths_hz, &other.channel_widths_hz, |width| {
                width.abs().to_bits()
            })
            && self.correlations == other.correlations
            && self.intents == other.intents)
            .then_some(ChannelOrder::Reversed)
    }
}

#[cfg(test)]
mod tests {
    use casa_types::measures::frequency::FrequencyRef;

    use super::*;
    use crate::catalogue::tests::{observed, partition, session};
    use crate::catalogue::{BindingTolerance, PartitionId};

    /// `partition`'s three channels from 676 GHz at −1 MHz, and the same
    /// window as XRADIO writes it: ascending, its widths positive.
    fn descending_and_ascending() -> (PartitionEntry, PartitionEntry) {
        let mut descending = partition(0, &[0], &[0]);
        descending.frequencies_hz = vec![676.0e9, 676.0e9 - 1.0e6, 676.0e9 - 2.0e6];
        descending.channel_widths_hz = vec![-1.0e6; 3];
        let mut ascending = descending.clone();
        ascending.frequencies_hz.reverse();
        ascending.channel_widths_hz = vec![1.0e6; 3];
        (descending, ascending)
    }

    /// A window and its channels reversed bind either way, reversed; a
    /// window and itself in the same order; and anything else of the
    /// content not at all.
    #[test]
    fn a_window_binds_to_its_reversed_channels() {
        let (descending, ascending) = descending_and_ascending();
        assert_eq!(
            descending.channel_order(&ascending),
            Some(ChannelOrder::Reversed)
        );
        assert_eq!(
            ascending.channel_order(&descending),
            Some(ChannelOrder::Reversed)
        );
        assert_eq!(
            descending.channel_order(&descending),
            Some(ChannelOrder::Same)
        );
        let differs = |change: fn(&mut PartitionEntry)| {
            let mut other = ascending.clone();
            change(&mut other);
            descending.channel_order(&other)
        };
        assert_eq!(differs(|p| p.channel_widths_hz[0] = 2.0e6), None);
        assert_eq!(differs(|p| p.frequencies_hz[1] += 1.0), None);
        assert_eq!(differs(|p| p.frequency_frame = FrequencyRef::TOPO), None);
        assert_eq!(differs(|p| p.correlations.reverse()), None);
        assert_eq!(differs(|p| p.intents.push("CAL".to_string())), None);
        assert_eq!(differs(|p| p.frequencies_hz.truncate(2)), None);
        // One channel is the same channel either way: never reversed.
        let mut one = descending.clone();
        one.frequencies_hz.truncate(1);
        one.channel_widths_hz.truncate(1);
        assert_eq!(one.channel_order(&one), Some(ChannelOrder::Same));
    }

    /// A catalogue binds to another holding its descending window
    /// ascending, either way round, and the binding records each
    /// partition's order. A catalogue holding the window both ways cannot
    /// tell which is meant, and the binding is refused.
    #[test]
    fn a_binding_records_each_partitions_channel_order() {
        let (descending, ascending) = descending_and_ascending();
        let other_window = partition(0, &[1], &[1]);
        let ours = observed(
            vec![session("p", None)],
            vec![descending.clone(), other_window.clone()],
        );
        let theirs = observed(
            vec![session("p", None)],
            vec![other_window.clone(), ascending.clone()],
        );
        // Each binding lists the orders of the catalogue bound from.
        for (a, b, orders) in [
            (&ours, &theirs, [ChannelOrder::Reversed, ChannelOrder::Same]),
            (&theirs, &ours, [ChannelOrder::Same, ChannelOrder::Reversed]),
        ] {
            let binding = a.bind_to(b, &BindingTolerance::EXACT).expect("binds");
            assert_eq!(binding.partitions, [PartitionId(1), PartitionId(0)]);
            assert_eq!(binding.channel_orders, orders);
        }
        let both = observed(vec![session("p", None)], vec![descending, ascending]);
        let refused = both
            .bind_to(&both, &BindingTolerance::EXACT)
            .expect_err("ambiguous");
        assert!(refused.to_string().contains("more than one"), "{refused}");
    }

    /// Channel `c` of `n` maps to `n - 1 - c` reversed; a range maps to the
    /// same channels, running the other way.
    #[test]
    fn channels_map_through_the_order() {
        assert_eq!(ChannelOrder::Same.channel(2, 64), 2);
        assert_eq!(ChannelOrder::Reversed.channel(0, 64), 63);
        assert_eq!(ChannelOrder::Reversed.channel(63, 64), 0);
        assert_eq!(ChannelOrder::Same.channels(2..6, 64), 2..6);
        assert_eq!(ChannelOrder::Reversed.channels(2..6, 64), 58..62);
        assert_eq!(ChannelOrder::Reversed.channels(0..64, 64), 0..64);
        assert_eq!(ChannelOrder::Reversed.channels(5..5, 8), 3..3);
        for order in [ChannelOrder::Same, ChannelOrder::Reversed] {
            let mapped = order.channels(2..6, 64);
            assert!((2..6).all(|channel| mapped.contains(&order.channel(channel, 64))));
            assert_eq!(mapped.len(), 4);
        }
    }
}
