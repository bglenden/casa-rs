// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's cubic spectral interpolation: the polynomial through four points
//! about a frequency (casacore `InterpolateArray1D::polynomialInterpolation`
//! with order 3, as `FTMachine::interpolateFrequencyTogrid` and
//! `interpolateFrequencyFromgrid` call it for `interpolation='cubic'`).

use crate::native::STENCIL;

/// The points a cubic interpolation reads and the factor of each: the
/// Lagrange basis of the polynomial through them, evaluated at the
/// interpolated frequency, so the value is `Σ factors · y[first..]`.
///
/// casacore evaluates the same polynomial by Neville's algorithm; the two
/// agree to rounding, and exactly at a point, where the factors are one
/// and zeros.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Stencil {
    /// Index of the first point.
    pub(super) first: usize,
    /// Number of points, four but on an axis of fewer.
    pub(super) len: usize,
    /// Factor of each point; zero past `len`.
    pub(super) factors: [f64; STENCIL],
}

impl Stencil {
    /// The stencil `polynomialInterpolation` takes for `frequency_hz` over
    /// `count` points at `point(i)`, ascending or descending, where
    /// `bracket` is casacore's `binarySearchBrackets` result: the index of
    /// the point equal to the frequency, else of the first point beyond it
    /// in the axis's order (`count` past the last). The two points on each
    /// side of the frequency, shifted inwards at either end of the axis.
    ///
    /// An axis of fewer than four points takes the polynomial through all
    /// of them, where casacore reads past its end.
    pub(super) fn cubic(
        count: usize,
        bracket: usize,
        frequency_hz: f64,
        point: impl Fn(usize) -> f64,
    ) -> Self {
        let len = count.min(STENCIL);
        let first = if count <= STENCIL || bracket <= 1 {
            0
        } else if bracket < count - 1 {
            bracket - 2
        } else {
            count - STENCIL
        };
        let mut factors = [0.0; STENCIL];
        for (i, factor) in factors.iter_mut().enumerate().take(len) {
            let at = point(first + i);
            *factor = (0..len)
                .filter(|j| *j != i)
                .map(|j| {
                    let other = point(first + j);
                    (frequency_hz - other) / (at - other)
                })
                .product();
        }
        Self {
            first,
            len,
            factors,
        }
    }

    /// The points with a non-zero factor, as `(index, factor)`.
    pub(super) fn terms(self) -> impl Iterator<Item = (usize, f64)> {
        (0..self.len)
            .map(move |i| (self.first + i, self.factors[i]))
            .filter(|(_, factor)| *factor != 0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cubic through four points reproduces a cubic polynomial, and
    /// takes a point's own value there.
    #[test]
    fn the_stencil_reproduces_a_cubic_and_its_points() {
        let points = [1.0, 2.5, 3.0, 4.5, 6.0, 7.0];
        let cubic = |x: f64| 2.0 - x + 0.5 * x * x - 0.25 * x * x * x;
        for (x, bracket) in [(2.7, 2), (5.1, 4), (1.2, 1), (6.5, 5), (3.0, 2)] {
            let stencil = Stencil::cubic(points.len(), bracket, x, |i| points[i]);
            let value: f64 = stencil
                .terms()
                .map(|(index, factor)| factor * cubic(points[index]))
                .sum();
            assert!((value - cubic(x)).abs() < 1e-12, "{x}: {value}");
        }
        let at_point = Stencil::cubic(points.len(), 2, 3.0, |i| points[i]);
        assert_eq!(at_point.terms().collect::<Vec<_>>(), vec![(2, 1.0)]);
    }

    /// `polynomialInterpolation`'s window: two points on each side, moved
    /// inwards at the ends; descending axes alike.
    #[test]
    fn the_window_follows_casacore() {
        let first = |count, bracket| Stencil::cubic(count, bracket, 0.5, |i| i as f64).first;
        assert_eq!(first(10, 0), 0);
        assert_eq!(first(10, 1), 0);
        assert_eq!(first(10, 2), 0);
        assert_eq!(first(10, 5), 3);
        assert_eq!(first(10, 8), 6);
        assert_eq!(first(10, 9), 6);
        assert_eq!(first(10, 10), 6);
        let short = Stencil::cubic(3, 1, 0.5, |i| i as f64);
        assert_eq!((short.first, short.len), (0, 3));
    }
}
