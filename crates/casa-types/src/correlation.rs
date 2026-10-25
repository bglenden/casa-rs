// SPDX-License-Identifier: LGPL-3.0-or-later
//! Correlation products: the feed-pair vocabulary shared by observation
//! stores and numerical consumers, with casacore's `Stokes` codes and the
//! MSv4 labels as its persistent forms.

/// A correlation product, by its two receptors.
///
/// The casacore `Stokes::StokesTypes` codes are the persistent form in
/// MeasurementSet v2 (`POLARIZATION::CORR_TYPE`); MSv4 stores the labels
/// (`"XX"`) in its `polarization` coordinate. This is the one definition
/// observation adapters and consumers share; Stokes parameters and mixed
/// feeds are not correlation products a store delivers and are not here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Correlation {
    /// Circular RR.
    RR,
    /// Circular RL.
    RL,
    /// Circular LR.
    LR,
    /// Circular LL.
    LL,
    /// Linear XX.
    XX,
    /// Linear XY.
    XY,
    /// Linear YX.
    YX,
    /// Linear YY.
    YY,
}

impl Correlation {
    /// Every product, in casacore code order.
    pub const ALL: [Self; 8] = [
        Self::RR,
        Self::RL,
        Self::LR,
        Self::LL,
        Self::XX,
        Self::XY,
        Self::YX,
        Self::YY,
    ];

    /// The casacore `Stokes::StokesTypes` code.
    #[must_use]
    pub const fn casacore_code(self) -> i32 {
        match self {
            Self::RR => 5,
            Self::RL => 6,
            Self::LR => 7,
            Self::LL => 8,
            Self::XX => 9,
            Self::XY => 10,
            Self::YX => 11,
            Self::YY => 12,
        }
    }

    /// The correlation with casacore `Stokes` code `code`, if it is one of
    /// the circular or linear products.
    #[must_use]
    pub const fn from_casacore_code(code: i32) -> Option<Self> {
        Some(match code {
            5 => Self::RR,
            6 => Self::RL,
            7 => Self::LR,
            8 => Self::LL,
            9 => Self::XX,
            10 => Self::XY,
            11 => Self::YX,
            12 => Self::YY,
            _ => return None,
        })
    }

    /// The label MSv4 stores in its `polarization` coordinate.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::RR => "RR",
            Self::RL => "RL",
            Self::LR => "LR",
            Self::LL => "LL",
            Self::XX => "XX",
            Self::XY => "XY",
            Self::YX => "YX",
            Self::YY => "YY",
        }
    }

    /// The correlation an MSv4 `polarization` label names.
    #[must_use]
    pub fn from_label(label: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|correlation| correlation.label() == label)
    }

    /// The receptor indices `(p, q)` of the product `<E_p conj(E_q)>`: 0 for
    /// R or X, 1 for L or Y.
    #[must_use]
    pub const fn receptors(self) -> (usize, usize) {
        match self {
            Self::RR | Self::XX => (0, 0),
            Self::RL | Self::XY => (0, 1),
            Self::LR | Self::YX => (1, 0),
            Self::LL | Self::YY => (1, 1),
        }
    }

    /// Whether the product is a parallel hand (both receptors the same).
    #[must_use]
    pub const fn is_parallel_hand(self) -> bool {
        let (p, q) = self.receptors();
        p == q
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlations_round_trip_their_persistent_forms() {
        for code in 5..=12 {
            let correlation = Correlation::from_casacore_code(code).expect("product");
            assert_eq!(correlation.casacore_code(), code);
            assert_eq!(
                Correlation::from_label(correlation.label()),
                Some(correlation)
            );
        }
        assert_eq!(Correlation::from_casacore_code(1), None);
        assert_eq!(Correlation::XY.receptors(), (0, 1));
        assert!(Correlation::LL.is_parallel_hand());
        assert!(!Correlation::YX.is_parallel_hand());
    }
}
