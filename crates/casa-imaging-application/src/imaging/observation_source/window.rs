// SPDX-License-Identifier: LGPL-3.0-or-later
//! The channels a restricted wave reads on the batch route: those whose
//! output-frame frequencies reach the wave's planes at some selected time,
//! with both linear-interpolation partners at each edge.
//!
//! A traversal binds its channels before it reads a batch, so the window
//! is decided per partition, from the range of conversion factors its
//! selected (field, time)s span: a channel reaches the planes when its
//! frequency times some factor of the range does. That window holds every
//! channel any one time's would (each row's channels converted at its own
//! time and field); the extra channels a wider range adds reach no plane of
//! the wave and place nothing.

use std::ops::Range;

/// Relative slack on a factor range: a conversion applies its hops one
/// after another, so a channel's converted frequency and its native one
/// times the factor differ in the last bits.
const ROUNDING: f64 = 1.0e-12;

/// The output-frame conversion factors one partition's selected samples
/// span: each channel's output-frame frequency at any of its times is its
/// native one times a factor in `[low, high]`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct FactorRange {
    low: f64,
    high: f64,
}

impl FactorRange {
    /// The range of no factor yet.
    pub(super) const EMPTY: Self = Self {
        low: f64::INFINITY,
        high: f64::NEG_INFINITY,
    };

    /// The range widened to hold `factor`.
    pub(super) fn with(self, factor: f64) -> Self {
        Self {
            low: self.low.min(factor),
            high: self.high.max(factor),
        }
    }

    /// Whether no factor was added.
    pub(super) fn is_empty(self) -> bool {
        self.low > self.high
    }

    /// The output-frame frequencies native frequency `hz` takes over the
    /// range, widened by the rounding slack.
    fn span(self, hz: f64) -> (f64, f64) {
        let (a, b) = (
            hz * self.low * (1.0 - ROUNDING),
            hz * self.high * (1.0 + ROUNDING),
        );
        (a.min(b), a.max(b))
    }
}

/// The ordinals of `frequencies_hz` (a partition's selected channels'
/// native frequencies, in channel order) a wave of output-frame bounds
/// `bounds_hz` reads under `factors`: a channel whose span meets the
/// bounds, and both channels of an adjacent pair whose spans together
/// straddle a bound. `None` when no channel reaches the wave.
///
/// This is the rule for one time's channels with each channel's frequency
/// at that time replaced by its span over the partition's times, so it
/// holds every channel the one-time rule keeps at any time.
pub(super) fn wave_window(
    frequencies_hz: &[f64],
    factors: FactorRange,
    bounds_hz: [f64; 2],
) -> Option<Range<usize>> {
    let (lower, upper) = (
        bounds_hz[0].min(bounds_hz[1]),
        bounds_hz[0].max(bounds_hz[1]),
    );
    let mut window: Option<(usize, usize)> = None;
    let mut include = |first: usize, last: usize| {
        window = Some(window.map_or((first, last), |(start, end)| {
            (start.min(first), end.max(last))
        }));
    };
    let mut previous: Option<(usize, (f64, f64))> = None;
    for (ordinal, hz) in frequencies_hz.iter().copied().enumerate() {
        let (low, high) = factors.span(hz);
        if low <= upper && high >= lower {
            include(ordinal, ordinal);
        }
        if let Some((before, (before_low, before_high))) = previous
            && before_low.min(low) <= upper
            && before_high.max(high) >= lower
        {
            include(before, ordinal);
        }
        previous = Some((ordinal, (low, high)));
    }
    window.map(|(first, last)| first..last + 1)
}

#[cfg(test)]
mod tests {
    use super::{FactorRange, wave_window};

    fn range(factors: &[f64]) -> FactorRange {
        factors
            .iter()
            .fold(FactorRange::EMPTY, |range, factor| range.with(*factor))
    }

    /// One factor gives one time's window: the channels inside the bounds
    /// and the straddling partner at each edge.
    #[test]
    fn one_factor_keeps_both_partners_at_each_edge() {
        let channels = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        assert_eq!(
            wave_window(&channels, range(&[1.0]), [2.5, 4.5]),
            Some(1..5)
        );
        assert_eq!(
            wave_window(&channels, range(&[1.0]), [3.0, 4.0]),
            Some(1..5),
            "a bound on a channel keeps the pair that meets it"
        );
        assert_eq!(wave_window(&channels, range(&[1.0]), [7.5, 8.0]), None);
        assert_eq!(
            wave_window(&channels, range(&[1.0]), [6.5, 7.0]),
            None,
            "no pair straddles bounds beyond the last channel"
        );
    }

    /// A range of factors keeps every channel any of its factors would,
    /// and descending channels are windowed as ascending ones.
    #[test]
    fn a_factor_range_holds_every_factors_window() {
        let channels = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0];
        let bounds = [12.2, 12.8];
        let factors = [0.95, 1.0, 1.05];
        let union = factors
            .iter()
            .filter_map(|factor| wave_window(&channels, range(&[*factor]), bounds))
            .reduce(|a, b| a.start.min(b.start)..a.end.max(b.end))
            .expect("some factor reaches the wave");
        let window = wave_window(&channels, range(&factors), bounds).expect("a window");
        assert!(
            window.start <= union.start && window.end >= union.end,
            "{window:?} holds {union:?}"
        );
        let descending = channels.iter().rev().copied().collect::<Vec<_>>();
        let reversed = wave_window(&descending, range(&factors), bounds).expect("a window");
        assert_eq!(
            reversed,
            channels.len() - window.end..channels.len() - window.start
        );
    }
}
