// SPDX-License-Identifier: LGPL-3.0-or-later
//! Borrowed array views of measures: many values sharing one reference.
//!
//! A scalar measure such as [`MEpoch`] or [`MFrequency`] carries its own
//! reference. Observation data hold thousands of values that share one,
//! so these views pair a borrowed [`ndarray`] view of the numbers with the
//! reference and unit they share, stated once. They are the same Measures
//! vocabulary as the scalar types, not an observation-specific copy of it:
//! [`EpochView::get`] and [`FrequencyView::get`] give the scalar measure of
//! one element on the stack, and the references are the scalar types' own
//! [`EpochRef`], [`FrequencyRef`] and [`DirectionRef`].
//!
//! The unit of each view is fixed by its type: frequencies are hertz, epoch
//! offsets and durations seconds, UVW metres. A consumer that needs those
//! units reads them without inspecting a unit string; a source holding other
//! units scales them when it fills the view, never after.
//!
//! These views are the first part of the bulk Measures interface the
//! MSv2/MSv4 trial needs (`docs/measurement-set/contracts.md`, "Shared
//! Measures types and bulk conversion"); prepared bulk conversions over them
//! are later work.
//!
//! # Examples
//!
//! ```
//! use casa_types::measures::frequency::FrequencyRef;
//! use casa_types::measures::{EpochRef, EpochView, FrequencyView, MjdHighPrec};
//! use ndarray::array;
//!
//! // MeasurementSet v2 TIME: seconds since MJD 0, UTC.
//! let seconds = array![4_895_231_290.0, 4_895_231_300.0];
//! let times = EpochView::new(EpochRef::UTC, MjdHighPrec::new(0.0, 0.0), seconds.view());
//! assert_eq!(times.len(), 2);
//! assert!((times.get(1).value().as_mjd() - 56_657.769_675_9).abs() < 1e-6);
//!
//! let hz = array![44.0e9, 44.128e9];
//! let frequencies = FrequencyView::new(FrequencyRef::LSRK, hz.view());
//! assert_eq!(frequencies.hz()[1], 44.128e9);
//! ```

use std::ops::Range;

use ndarray::{ArrayView1, ArrayView2, Axis, s};

use super::direction::DirectionRef;
use super::epoch::{EpochRef, MEpoch, MjdHighPrec};
use super::frequency::{FrequencyRef, MFrequency};

/// Seconds per day.
const SECONDS_PER_DAY: f64 = 86_400.0;

/// Epochs as offsets in seconds from one origin, in one time scale.
///
/// The origin is a split-precision [`MjdHighPrec`] and each element an f64
/// offset from it, which is how the stores hold time: MeasurementSet v2
/// `TIME` is seconds from MJD 0, MSv4 `time` with `format: unix` is seconds
/// from MJD 40587. Keeping the source's origin keeps its precision: no value
/// is rounded into another representation to make the view.
#[derive(Clone, Copy, Debug)]
pub struct EpochView<'a> {
    reference: EpochRef,
    origin: MjdHighPrec,
    offsets_s: ArrayView1<'a, f64>,
}

impl<'a> EpochView<'a> {
    /// Epochs `origin + offsets_s[i]` seconds in time scale `reference`.
    #[must_use]
    pub const fn new(
        reference: EpochRef,
        origin: MjdHighPrec,
        offsets_s: ArrayView1<'a, f64>,
    ) -> Self {
        Self {
            reference,
            origin,
            offsets_s,
        }
    }

    /// The time scale every epoch is in.
    #[must_use]
    pub const fn reference(&self) -> EpochRef {
        self.reference
    }

    /// The epoch the offsets count from.
    #[must_use]
    pub const fn origin(&self) -> MjdHighPrec {
        self.origin
    }

    /// Offsets from [`Self::origin`] in seconds.
    #[must_use]
    pub fn offsets_s(&self) -> ArrayView1<'a, f64> {
        self.offsets_s
    }

    /// Number of epochs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.offsets_s.len()
    }

    /// Whether the view holds no epochs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.offsets_s.is_empty()
    }

    /// Epoch `index` as a scalar measure.
    ///
    /// # Panics
    ///
    /// If `index` is out of range.
    #[must_use]
    pub fn get(&self, index: usize) -> MEpoch {
        let offset = self.offsets_s[index];
        let days = (offset / SECONDS_PER_DAY).floor();
        let seconds = offset - days * SECONDS_PER_DAY;
        MEpoch::new(
            MjdHighPrec::new(
                self.origin.day() + days,
                self.origin.frac() + seconds / SECONDS_PER_DAY,
            ),
            self.reference,
        )
    }

    /// The epochs `range`, sharing this view's origin and scale.
    ///
    /// # Panics
    ///
    /// If `range` is out of bounds.
    #[must_use]
    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            offsets_s: self.offsets_s.slice_move(s![range]),
            ..*self
        }
    }

    /// Seconds from `origin` to epoch `index`, for epochs with the same
    /// scale: the offset re-expressed against another origin, computed from
    /// the split origins so their whole days cancel exactly.
    ///
    /// # Panics
    ///
    /// If `index` is out of range.
    #[must_use]
    pub fn seconds_since(&self, index: usize, origin: MjdHighPrec) -> f64 {
        let days = self.origin.day() - origin.day();
        let fraction = self.origin.frac() - origin.frac();
        (days + fraction) * SECONDS_PER_DAY + self.offsets_s[index]
    }
}

/// Frequencies in hertz in one spectral reference frame.
#[derive(Clone, Copy, Debug)]
pub struct FrequencyView<'a> {
    reference: FrequencyRef,
    hz: ArrayView1<'a, f64>,
}

impl<'a> FrequencyView<'a> {
    /// Frequencies `hz` in frame `reference`.
    #[must_use]
    pub const fn new(reference: FrequencyRef, hz: ArrayView1<'a, f64>) -> Self {
        Self { reference, hz }
    }

    /// The spectral frame.
    #[must_use]
    pub const fn reference(&self) -> FrequencyRef {
        self.reference
    }

    /// The frequencies in hertz.
    #[must_use]
    pub fn hz(&self) -> ArrayView1<'a, f64> {
        self.hz
    }

    /// Number of frequencies.
    #[must_use]
    pub fn len(&self) -> usize {
        self.hz.len()
    }

    /// Whether the view holds no frequencies.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hz.is_empty()
    }

    /// Frequency `index` as a scalar measure.
    ///
    /// # Panics
    ///
    /// If `index` is out of range.
    #[must_use]
    pub fn get(&self, index: usize) -> MFrequency {
        MFrequency::new(self.hz[index], self.reference)
    }

    /// The frequencies `range`, in this view's frame.
    ///
    /// # Panics
    ///
    /// If `range` is out of bounds.
    #[must_use]
    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            hz: self.hz.slice_move(s![range]),
            ..*self
        }
    }
}

/// Baseline coordinates `[u, v, w]` in metres, one row per sample, in one
/// reference frame.
///
/// casacore's `Muvw` frames are the direction frames, so the reference is a
/// [`DirectionRef`]; casa-types has no scalar UVW measure yet.
#[derive(Clone, Copy, Debug)]
pub struct UvwView<'a> {
    reference: DirectionRef,
    metres: ArrayView2<'a, f64>,
}

impl<'a> UvwView<'a> {
    /// UVW `metres`, shape `[sample, 3]`, in frame `reference`.
    ///
    /// # Panics
    ///
    /// If the second axis does not have length 3.
    #[must_use]
    pub fn new(reference: DirectionRef, metres: ArrayView2<'a, f64>) -> Self {
        assert_eq!(metres.len_of(Axis(1)), 3, "UVW has three components");
        Self { reference, metres }
    }

    /// The reference frame.
    #[must_use]
    pub const fn reference(&self) -> DirectionRef {
        self.reference
    }

    /// The coordinates in metres, `[sample, 3]`.
    #[must_use]
    pub fn metres(&self) -> ArrayView2<'a, f64> {
        self.metres
    }

    /// Sample `index`'s `[u, v, w]` in metres.
    ///
    /// # Panics
    ///
    /// If `index` is out of range.
    #[must_use]
    pub fn get(&self, index: usize) -> [f64; 3] {
        let row = self.metres.row(index);
        [row[0], row[1], row[2]]
    }

    /// Number of samples.
    #[must_use]
    pub fn len(&self) -> usize {
        self.metres.len_of(Axis(0))
    }

    /// Whether the view holds no samples.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The samples `range`, in this view's frame.
    ///
    /// # Panics
    ///
    /// If `range` is out of bounds.
    #[must_use]
    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            metres: self.metres.slice_move(s![range, ..]),
            ..*self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    #[test]
    fn epochs_keep_their_origin_and_offsets() {
        let unix = MjdHighPrec::new(40_587.0, 0.0);
        let offsets = array![1_388_967_290.0, 1_388_967_300.5];
        let times = EpochView::new(EpochRef::UTC, unix, offsets.view());
        let second = times.get(1);
        assert_eq!(second.refer(), EpochRef::UTC);
        let expected = 40_587.0 + 1_388_967_300.5 / SECONDS_PER_DAY;
        assert!((second.value().as_mjd() - expected).abs() < 1e-9);
        // Re-expressed from MJD 0 the offset is the v2 TIME value, exactly:
        // the whole days of the two origins cancel before any rounding.
        let mjd_seconds = times.seconds_since(1, MjdHighPrec::new(0.0, 0.0));
        assert_eq!(mjd_seconds, 40_587.0 * SECONDS_PER_DAY + 1_388_967_300.5);
        assert_eq!(times.slice(1..2).offsets_s()[0], 1_388_967_300.5);
    }

    #[test]
    fn slices_borrow_without_copying() {
        let hz = array![1.0e9, 1.1e9, 1.2e9];
        let frequencies = FrequencyView::new(FrequencyRef::TOPO, hz.view());
        let tail = frequencies.slice(1..3);
        assert_eq!(tail.hz().as_ptr(), hz.as_ptr().wrapping_add(1));
        assert_eq!(tail.get(0).refer(), FrequencyRef::TOPO);

        let uvw = array![[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]];
        let view = UvwView::new(DirectionRef::J2000, uvw.view());
        assert_eq!(view.slice(1..2).get(0), [4.0, 5.0, 6.0]);
    }
}
