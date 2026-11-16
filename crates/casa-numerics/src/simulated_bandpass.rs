// SPDX-License-Identifier: LGPL-3.0-or-later
// Adapted from casacore's SimButterworthBandpass.h/.tcc.
// Copyright (C) 2000-2003 Associated Universities, Inc.
// Original LGPL-2.0-or-later; this adaptation uses LGPL-3.0-or-later.
//! CASA's asymmetric simulated Butterworth bandpass.

/// A bandpass with separately controlled lower and upper roll-off orders.
///
/// On either side of `center`, the amplitude is
/// `peak / sqrt(1 + abs((x-center)/(cutoff-center))^(2*order))`.
/// At the centre it is exactly `peak`. This follows casacore's simulation
/// function, including its order-zero response; it is not a filter designer.
/// The abscissa and both cutoffs use the same units.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SimulatedBandpass {
    /// Lower-side Butterworth order.
    pub lower_order: u32,
    /// Upper-side Butterworth order.
    pub upper_order: u32,
    /// Lower-side half-power position.
    pub lower_cutoff: f64,
    /// Upper-side half-power position.
    pub upper_cutoff: f64,
    /// Position of the peak.
    pub center: f64,
    /// Peak amplitude.
    pub peak: f64,
}

impl Default for SimulatedBandpass {
    fn default() -> Self {
        Self::new(0, 0, -1., 1., 0., 1.)
    }
}

impl SimulatedBandpass {
    /// Construct the six-parameter simulation response, as casacore does.
    #[must_use]
    pub const fn new(
        lower_order: u32,
        upper_order: u32,
        lower_cutoff: f64,
        upper_cutoff: f64,
        center: f64,
        peak: f64,
    ) -> Self {
        Self {
            lower_order,
            upper_order,
            lower_cutoff,
            upper_cutoff,
            center,
            peak,
        }
    }

    /// Evaluate the response at `x`, using its side's cutoff and order.
    #[must_use]
    pub fn evaluate(self, x: f64) -> f64 {
        let (order, cutoff) = if x < self.center {
            (self.lower_order, self.lower_cutoff)
        } else if x > self.center {
            (self.upper_order, self.upper_cutoff)
        } else {
            return self.peak;
        };
        let distance = ((x - self.center) / (cutoff - self.center)).abs();
        self.peak / (1. + distance.powf(f64::from(order.wrapping_mul(2)))).sqrt()
    }

    /// The typed equivalent of casacore's `minOrder`/`maxOrder` mode record.
    #[must_use]
    pub const fn orders(self) -> [u32; 2] {
        [self.lower_order, self.upper_order]
    }

    /// Update both mode orders without changing the four real parameters.
    pub fn set_orders(&mut self, [lower, upper]: [u32; 2]) {
        self.lower_order = lower;
        self.upper_order = upper;
    }
}

#[cfg(test)]
mod tests {
    use super::SimulatedBandpass;

    #[test]
    fn upstream_sim_butterworth_parameters_response_and_mode() {
        // casacore/scimath/Functionals/test/tSimButterworthBandpass.cc.
        let mut response = SimulatedBandpass::new(1, 1, -1., 1., 0., 1.);
        assert_eq!((response.center, response.peak), (0., 1.));
        assert_eq!(response.orders(), [1, 1]);
        assert_eq!((response.lower_cutoff, response.upper_cutoff), (-1., 1.));
        response.peak = 10.;
        response.center = 5.;
        response.lower_order = 4;
        response.upper_order = 5;
        response.lower_cutoff = 1.;
        response.upper_cutoff = 9.;
        assert_eq!(response.evaluate(response.center), response.peak);
        assert!(
            response.evaluate(6. * response.lower_cutoff - 5. * response.center)
                < 0.01 * response.peak
        );
        assert!(
            response.evaluate(6. * response.upper_cutoff + 5. * response.center)
                < 0.01 * response.peak
        );
        response.set_orders([2, 3]);
        assert_eq!(response.orders(), [2, 3]);
        assert_eq!((response.center, response.peak), (5., 10.));
        assert_eq!((response.lower_cutoff, response.upper_cutoff), (1., 9.));
    }

    #[test]
    fn asymmetric_half_power_and_closed_form_values() {
        let response = SimulatedBandpass::new(2, 3, 1., 9., 5., 10.);
        for cutoff in [1., 9.] {
            assert!((response.evaluate(cutoff) - 10. / 2f64.sqrt()).abs() < 1e-14);
        }
        assert!((response.evaluate(-3.) - 10. / 17f64.sqrt()).abs() < 1e-14);
        assert!((response.evaluate(13.) - 10. / 65f64.sqrt()).abs() < 1e-14);
        let flat_order = SimulatedBandpass::default();
        assert_eq!(flat_order.evaluate(0.), 1.);
        assert_eq!(flat_order.evaluate(-1.), 1. / 2f64.sqrt());
    }
}
