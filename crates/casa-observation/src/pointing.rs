// SPDX-License-Identifier: LGPL-3.0-or-later
//! Where the antennas pointed: one partition's pointing samples, per
//! antenna, as MSv4's `pointing_xds` holds them and the MSv2 adapter
//! assembles them from `POINTING` (`docs/measurement-set/imaging-port.md`
//! §2.2.1).
//!
//! The boundary does not interpolate. [`PointingSeries::bracket`] returns,
//! for an (antenna, time) query, the samples around the time, each
//! evaluated at it; a consumer's law (covering sample, nearest,
//! great-circle interpolation, hold or reject outside coverage) chooses
//! among them.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::ops::RangeInclusive;

use casa_types::measures::direction::DirectionRef;

use crate::ObservationError;
use crate::catalogue::{AntennaId, PartitionId};

/// Which of a store's pointing directions to read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PointingDirection {
    /// Where the beam pointed: MSv2 `POINTING.DIRECTION`, MSv4
    /// `POINTING_BEAM`.
    Beam,
    /// The intended tracking target: MSv2 `POINTING.TARGET`. MSv4 holds no
    /// target, so a request for it there is refused.
    Target,
}

/// A request for one partition's pointing
/// ([`Observation::pointing`](crate::Observation::pointing)).
#[derive(Clone, Debug, PartialEq)]
pub struct PointingRequest {
    /// The partition whose antennas and time system the samples use.
    pub partition: PartitionId,
    /// Which direction to read.
    pub direction: PointingDirection,
    /// The antennas whose samples to read.
    pub antennas: Vec<AntennaId>,
    /// The times the samples are for, in seconds since the partition's
    /// time origin and in its time scale (`PartitionEntry::time_origin`,
    /// `time_scale`): the store delivers the samples within the span, and
    /// each antenna's nearest one before and after it.
    pub times: RangeInclusive<f64>,
    /// The most bytes the query may hold while it reads and once it
    /// answers; more is refused ([`ObservationError::OverBudget`]).
    pub budget_bytes: usize,
}

/// One partition's pointing samples, in MSv4's shape: per antenna, a
/// series of directions in time, each a polynomial in time.
#[derive(Clone, Debug, PartialEq)]
pub struct PointingSeries {
    /// The frame of every direction. A store whose samples are in several
    /// frames is refused: MSv4 states one per `pointing_xds`.
    pub frame: DirectionRef,
    /// The most polynomial terms per axis any antenna's samples have: 1
    /// for a direction per sample (MSv2 `NUM_POLY` + 1; MSv4 holds one).
    pub terms: usize,
    /// Each antenna's samples.
    pub antennas: BTreeMap<AntennaId, AntennaPointing>,
}

/// One antenna's pointing samples.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AntennaPointing {
    times_s: Vec<f64>,
    intervals_s: Option<Vec<f64>>,
    origins_s: Vec<f64>,
    terms: usize,
    coefficients: Vec<f64>,
    /// Half the longest interval: the furthest a covering sample can be.
    reach_s: f64,
}

impl AntennaPointing {
    /// Samples at `times_s`, ascending (samples at equal times keep the
    /// store's order), each with its polynomial origin (`origins_s`: MSv2
    /// `TIME_ORIGIN`, an MSv4 sample's own time) and `[sample][axis][term]`
    /// `coefficients`, `terms` per axis in increasing power of the time
    /// since the origin, radians (a sample with fewer terms has zeros after
    /// its own); and the interval, centred on its time, for which the store
    /// states each holds (MSv2 `INTERVAL`), or `None` when the store states
    /// none, as MSv4.
    ///
    /// # Errors
    ///
    /// [`ObservationError::InvalidStore`] when there are no terms, the
    /// lengths disagree, the times are not finite and ascending, or an
    /// interval is not finite.
    pub fn new(
        times_s: Vec<f64>,
        intervals_s: Option<Vec<f64>>,
        origins_s: Vec<f64>,
        terms: usize,
        coefficients: Vec<f64>,
    ) -> Result<Self, ObservationError> {
        let samples = times_s.len();
        let invalid = |reason: &str| ObservationError::InvalidStore {
            reason: format!("pointing samples: {reason}"),
        };
        if terms == 0 {
            return Err(invalid("no polynomial terms"));
        }
        if origins_s.len() != samples
            || coefficients.len() != samples * 2 * terms
            || intervals_s
                .as_ref()
                .is_some_and(|intervals| intervals.len() != samples)
        {
            return Err(invalid(
                "a time, origin, interval and coefficients per sample",
            ));
        }
        if !times_s.iter().all(|time| time.is_finite())
            || times_s.windows(2).any(|pair| pair[1] < pair[0])
        {
            return Err(invalid("times are not finite and ascending"));
        }
        let mut reach_s = 0.0_f64;
        if let Some(intervals) = &intervals_s {
            if !intervals.iter().all(|interval| interval.is_finite()) {
                return Err(invalid("an interval is not finite"));
            }
            reach_s = intervals
                .iter()
                .fold(0.0, |reach, interval| reach.max(0.5 * interval));
        }
        Ok(Self {
            times_s,
            intervals_s,
            origins_s,
            terms,
            coefficients,
            reach_s,
        })
    }

    /// The sample times, ascending.
    #[must_use]
    pub fn times_s(&self) -> &[f64] {
        &self.times_s
    }

    /// Each sample's interval, when the store states them.
    #[must_use]
    pub fn intervals_s(&self) -> Option<&[f64]> {
        self.intervals_s.as_deref()
    }

    /// Each sample's polynomial origin.
    #[must_use]
    pub fn origins_s(&self) -> &[f64] {
        &self.origins_s
    }

    /// Polynomial terms per axis of every sample.
    #[must_use]
    pub fn terms(&self) -> usize {
        self.terms
    }

    /// `[sample][axis][term]` coefficients.
    #[must_use]
    pub fn coefficients(&self) -> &[f64] {
        &self.coefficients
    }

    /// The direction of sample `sample` at `time_s`, in radians: its
    /// polynomial in the time since its origin, by Horner's rule from the
    /// highest term.
    #[must_use]
    pub fn direction_at(&self, sample: usize, time_s: f64) -> [f64; 2] {
        let offset = time_s - self.origins_s[sample];
        let start = sample * 2 * self.terms;
        [0, 1].map(|axis| {
            self.coefficients[start + axis * self.terms..start + (axis + 1) * self.terms]
                .iter()
                .rev()
                .fold(0.0, |value, term| value * offset + term)
        })
    }
}

/// One pointing sample of an antenna, evaluated at a query time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PointingCandidate {
    /// The sample's index in its antenna's series: two candidates with the
    /// same index are the same sample.
    pub sample: usize,
    /// The sample's time, in the partition's time system.
    pub time_s: f64,
    /// The direction at the query time, as (longitude, latitude) in
    /// radians in the series' frame.
    pub direction_rad: [f64; 2],
}

/// The samples around one (antenna, time) query.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PointingBracket {
    /// The sample whose interval holds the time (nearest the time, then
    /// first in the series); `None` without one, and always when the store
    /// states no intervals.
    pub covering: Option<PointingCandidate>,
    /// The first of the samples at the latest time at or before the query.
    pub before: Option<PointingCandidate>,
    /// The first sample at or after the query.
    pub after: Option<PointingCandidate>,
}

impl PointingSeries {
    /// No samples, in `frame`: a store without pointing.
    #[must_use]
    pub fn empty(frame: DirectionRef) -> Self {
        Self {
            frame,
            terms: 1,
            antennas: BTreeMap::new(),
        }
    }

    /// The samples around `time_s` (in the partition's time system) for
    /// `antenna`, each evaluated at `time_s`. An antenna without samples
    /// has an empty bracket.
    ///
    /// # Errors
    ///
    /// [`ObservationError::InvalidStore`] when a sample evaluates to a
    /// non-finite direction.
    pub fn bracket(
        &self,
        antenna: AntennaId,
        time_s: f64,
    ) -> Result<PointingBracket, ObservationError> {
        let Some(series) = self.antennas.get(&antenna) else {
            return Ok(PointingBracket::default());
        };
        let times = &series.times_s;
        let first_after = times.partition_point(|time| *time <= time_s);
        let first_at_or_after = times.partition_point(|time| *time < time_s);
        let before = first_after
            .checked_sub(1)
            .map(|last| times[..first_after].partition_point(|time| *time < times[last]));
        let after = (first_at_or_after < times.len()).then_some(first_at_or_after);
        let covering = series.intervals_s.as_ref().and_then(|intervals| {
            let near = times.partition_point(|time| *time < time_s - series.reach_s)
                ..times.partition_point(|time| *time <= time_s + series.reach_s);
            near.filter(|sample| {
                intervals[*sample] > 0.0
                    && (times[*sample] - time_s).abs() <= 0.5 * intervals[*sample]
            })
            .min_by(|left, right| {
                (times[*left] - time_s)
                    .abs()
                    .total_cmp(&(times[*right] - time_s).abs())
                    .then(left.cmp(right))
            })
        });
        let candidate = |sample: Option<usize>| {
            sample
                .map(|sample| Self::evaluate(antenna, series, sample, time_s))
                .transpose()
        };
        Ok(PointingBracket {
            covering: candidate(covering)?,
            before: candidate(before)?,
            after: candidate(after)?,
        })
    }

    /// Sample `sample` of `series` at `time_s`.
    fn evaluate(
        antenna: AntennaId,
        series: &AntennaPointing,
        sample: usize,
        time_s: f64,
    ) -> Result<PointingCandidate, ObservationError> {
        let direction_rad = series.direction_at(sample, time_s);
        if !direction_rad.iter().all(|angle| angle.is_finite()) {
            return Err(ObservationError::InvalidStore {
                reason: format!(
                    "pointing sample {sample} of {antenna:?} evaluates to a non-finite direction"
                ),
            });
        }
        Ok(PointingCandidate {
            sample,
            time_s: series.times_s[sample],
            direction_rad,
        })
    }

    /// At most the bytes the series holds on the heap: every antenna's
    /// samples, by their capacities, and the map's nodes. A B-tree node has
    /// room for 11 entries and, but for the root, holds at least 5, so the
    /// nodes number at most one more than a fifth of the entries; each is
    /// counted with its edges.
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        const NODE_ENTRIES: usize = 11;
        let node = NODE_ENTRIES * size_of::<(AntennaId, AntennaPointing)>()
            + (NODE_ENTRIES + 1) * size_of::<usize>()
            + 2 * size_of::<usize>();
        let samples: usize = self
            .antennas
            .values()
            .map(|series| {
                (series.times_s.capacity()
                    + series.intervals_s.as_ref().map_or(0, Vec::capacity)
                    + series.origins_s.capacity()
                    + series.coefficients.capacity())
                    * size_of::<f64>()
            })
            .sum();
        samples + (self.antennas.len() / 5 + 1) * node
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(intervals: Option<Vec<f64>>) -> PointingSeries {
        // Antenna 0: samples at 10, 20, 20, 30 s; a rate term in the time
        // since 5 s before each sample.
        let times = vec![10.0, 20.0, 20.0, 30.0];
        let origins = times.iter().map(|time| time - 5.0).collect();
        let coefficients = (0..4)
            .flat_map(|sample| {
                let base = f64::from(sample);
                [base, 0.01, -base, 0.02]
            })
            .collect();
        let mut series = PointingSeries {
            terms: 2,
            ..PointingSeries::empty(DirectionRef::J2000)
        };
        series.antennas.insert(
            AntennaId(0),
            AntennaPointing::new(times, intervals, origins, 2, coefficients).expect("samples"),
        );
        series
    }

    /// Before is the first of the samples at the latest time at or before;
    /// after is the first at or after; the covering sample is the nearest
    /// whose interval holds the time, the first of equals; each is its
    /// polynomial at the query time.
    #[test]
    fn brackets_name_the_samples_around_a_time() {
        let with = series(Some(vec![4.0, 6.0, 30.0, 0.0]));
        let bracket = with.bracket(AntennaId(0), 21.0).expect("bracket");
        let sample = |candidate: Option<PointingCandidate>| candidate.map(|c| c.sample);
        assert_eq!(
            (
                sample(bracket.covering),
                sample(bracket.before),
                sample(bracket.after)
            ),
            (Some(1), Some(1), Some(3))
        );
        // Sample 1 at 21 s: 1 + 0.01 * (21 - 15), -1 + 0.02 * 6.
        let covering = bracket.covering.expect("covering");
        assert_eq!(
            covering.direction_rad,
            [1.0 + 0.01 * 6.0, -1.0 + 0.02 * 6.0]
        );
        // At 20 s exactly, before and after are the first sample at 20 s.
        let at = with.bracket(AntennaId(0), 20.0).expect("bracket");
        assert_eq!((sample(at.before), sample(at.after)), (Some(1), Some(1)));
        // Sample 3 states no interval (0), so 30 s is held by sample 2's.
        let late = with.bracket(AntennaId(0), 30.0).expect("bracket");
        assert_eq!(sample(late.covering), Some(2));
        // Outside the samples: one side only, and nothing covers.
        let early = with.bracket(AntennaId(0), 1.0).expect("bracket");
        assert_eq!(
            (
                sample(early.covering),
                sample(early.before),
                sample(early.after)
            ),
            (None, None, Some(0))
        );
        let without = series(None);
        assert_eq!(
            without
                .bracket(AntennaId(0), 21.0)
                .expect("bracket")
                .covering,
            None
        );
        assert_eq!(
            without.bracket(AntennaId(7), 21.0).expect("bracket"),
            PointingBracket::default()
        );
    }

    /// Samples are refused unless their lengths agree and their times
    /// ascend.
    #[test]
    fn samples_must_be_whole_and_in_time_order() {
        let new = |times: Vec<f64>, coefficients: usize| {
            let origins = times.clone();
            AntennaPointing::new(times, None, origins, 1, vec![0.0; coefficients])
        };
        assert!(new(vec![1.0, 2.0], 4).is_ok());
        assert!(new(vec![2.0, 1.0], 4).is_err());
        assert!(new(vec![1.0, 2.0], 3).is_err());
        assert!(new(vec![1.0, f64::NAN], 4).is_err());
    }
}
