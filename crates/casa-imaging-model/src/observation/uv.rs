// SPDX-License-Identifier: LGPL-3.0-or-later
//! UV-distance selection: its intervals, their canonical form, and the
//! rule that selects a sample by its projected baseline length.

use std::cmp::Ordering;

use super::CompileObservationError;

/// One finite scalar selection boundary.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectionBound {
    value: f64,
    inclusive: bool,
}

impl SelectionBound {
    /// Construct an inclusive boundary.
    #[must_use]
    pub const fn inclusive(value: f64) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }

    /// Construct an exclusive boundary.
    #[must_use]
    pub const fn exclusive(value: f64) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }

    /// Return the scalar value.
    #[must_use]
    pub const fn value(self) -> f64 {
        self.value
    }

    /// Return whether the boundary includes its exact value.
    #[must_use]
    pub const fn is_inclusive(self) -> bool {
        self.inclusive
    }

    fn canonicalize(&mut self) -> Result<(), CompileObservationError> {
        if !self.value.is_finite() {
            return Err(CompileObservationError::InvalidScalarRange);
        }
        self.value = canonical_zero(self.value);
        Ok(())
    }
}

/// Unit in which a UV-distance predicate is evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UvDistanceUnit {
    /// Projected baseline length in metres.
    Meters,
    /// Projected baseline length divided by the row's DDID-linked SPW reference wavelength.
    Wavelengths,
}

/// One resolved UV-distance interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UvDistanceRange {
    lower: Option<SelectionBound>,
    upper: Option<SelectionBound>,
    unit: UvDistanceUnit,
}

impl UvDistanceRange {
    /// Construct a possibly one-sided UV-distance interval.
    #[must_use]
    pub const fn new(
        lower: Option<SelectionBound>,
        upper: Option<SelectionBound>,
        unit: UvDistanceUnit,
    ) -> Self {
        Self { lower, upper, unit }
    }

    /// Return the lower boundary, if bounded.
    #[must_use]
    pub const fn lower(self) -> Option<SelectionBound> {
        self.lower
    }

    /// Return the upper boundary, if bounded.
    #[must_use]
    pub const fn upper(self) -> Option<SelectionBound> {
        self.upper
    }

    /// Return the distance unit.
    #[must_use]
    pub const fn unit(self) -> UvDistanceUnit {
        self.unit
    }
}

/// Exact resolved UV-distance selection union.
#[derive(Debug, Clone, PartialEq)]
pub enum UvSelection {
    /// Select every projected baseline length.
    All,
    /// Select the union of these intervals.
    Ranges(Vec<UvDistanceRange>),
}

impl UvSelection {
    /// Whether a sample of stored UVW `uvw_m` is selected: its projected
    /// length `hypot(u, v)` lies in one of the intervals, in metres or in
    /// wavelengths of `wavelength_m`, its window's reference wavelength. A
    /// length that is not finite, or an interval in wavelengths without a
    /// wavelength, selects nothing.
    #[must_use]
    pub fn holds(&self, uvw_m: [f64; 3], wavelength_m: Option<f64>) -> bool {
        let Self::Ranges(ranges) = self else {
            return true;
        };
        let distance_m = uvw_m[0].hypot(uvw_m[1]);
        distance_m.is_finite()
            && ranges.iter().any(|range| {
                let value = match range.unit() {
                    UvDistanceUnit::Meters => distance_m,
                    UvDistanceUnit::Wavelengths => match wavelength_m {
                        Some(wavelength_m) => distance_m / wavelength_m,
                        None => return false,
                    },
                };
                range.lower().is_none_or(|bound| {
                    if bound.is_inclusive() {
                        value >= bound.value()
                    } else {
                        value > bound.value()
                    }
                }) && range.upper().is_none_or(|bound| {
                    if bound.is_inclusive() {
                        value <= bound.value()
                    } else {
                        value < bound.value()
                    }
                })
            })
    }

    /// Whether an interval is in wavelengths, so that evaluating the
    /// selection needs each window's reference wavelength.
    #[must_use]
    pub fn needs_wavelengths(&self) -> bool {
        matches!(self, Self::Ranges(ranges) if ranges.iter().any(|range| range.unit() == UvDistanceUnit::Wavelengths))
    }
}

pub(super) fn canonicalize_uv_selection(
    selection: &mut UvSelection,
) -> Result<(), CompileObservationError> {
    if let UvSelection::Ranges(ranges) = selection {
        if ranges.is_empty() {
            return Err(CompileObservationError::InvalidScalarRange);
        }
        for range in ranges.iter_mut() {
            canonicalize_bounds(&mut range.lower, &mut range.upper, true)?;
        }
        ranges.sort_unstable_by(compare_uv_ranges);
        let mut merged: Vec<UvDistanceRange> = Vec::with_capacity(ranges.len());
        for range in ranges.drain(..) {
            if let Some(last) = merged.last_mut()
                && last.unit == range.unit
                && ranges_overlap(last.upper, range.lower)
            {
                last.upper = union_upper(last.upper, range.upper);
            } else {
                merged.push(range);
            }
        }
        *ranges = merged;
    }
    Ok(())
}

fn canonicalize_bounds(
    lower: &mut Option<SelectionBound>,
    upper: &mut Option<SelectionBound>,
    non_negative: bool,
) -> Result<(), CompileObservationError> {
    if lower.is_none() && upper.is_none() {
        return Err(CompileObservationError::InvalidScalarRange);
    }
    if let Some(bound) = lower {
        bound.canonicalize()?;
        if non_negative && bound.value < 0.0 {
            return Err(CompileObservationError::NegativeUvDistance);
        }
    }
    if let Some(bound) = upper {
        bound.canonicalize()?;
        if non_negative && bound.value < 0.0 {
            return Err(CompileObservationError::NegativeUvDistance);
        }
    }
    if let (Some(lower), Some(upper)) = (*lower, *upper)
        && (lower.value > upper.value
            || (lower.value == upper.value && !(lower.inclusive && upper.inclusive)))
    {
        return Err(CompileObservationError::InvalidScalarRange);
    }
    Ok(())
}

fn compare_uv_ranges(left: &UvDistanceRange, right: &UvDistanceRange) -> Ordering {
    left.unit
        .cmp(&right.unit)
        .then_with(|| compare_lower(left.lower, right.lower))
}

fn compare_lower(left: Option<SelectionBound>, right: Option<SelectionBound>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => left
            .value
            .total_cmp(&right.value)
            .then_with(|| right.inclusive.cmp(&left.inclusive)),
    }
}

fn ranges_overlap(upper: Option<SelectionBound>, lower: Option<SelectionBound>) -> bool {
    match (upper, lower) {
        (None, _) | (_, None) => true,
        (Some(upper), Some(lower)) => {
            upper.value > lower.value
                || (upper.value == lower.value && (upper.inclusive || lower.inclusive))
        }
    }
}

fn union_upper(
    left: Option<SelectionBound>,
    right: Option<SelectionBound>,
) -> Option<SelectionBound> {
    match (left, right) {
        (None, _) | (_, None) => None,
        (Some(left), Some(right)) => match left.value.total_cmp(&right.value) {
            Ordering::Less => Some(right),
            Ordering::Greater => Some(left),
            Ordering::Equal => Some(SelectionBound {
                value: left.value,
                inclusive: left.inclusive || right.inclusive,
            }),
        },
    }
}

const fn canonical_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}
