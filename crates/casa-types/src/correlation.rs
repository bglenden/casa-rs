// SPDX-License-Identifier: LGPL-3.0-or-later
//! Correlation types: what one entry of a visibility's polarization axis
//! holds, shared by observation stores, imaging and calibration, with
//! casacore's `Stokes` codes and the MSv4 labels as its persistent forms.

/// The type of one entry on a visibility's polarization axis.
///
/// These are casacore's `Stokes::StokesTypes`, every defined value but
/// `Undefined`, and the same 32 values the MSv4 schema allows in its
/// `polarization` coordinate. The persistent forms are the casacore code,
/// stored by MeasurementSet v2 in `POLARIZATION::CORR_TYPE`, and the label
/// (`"XX"`, `"RCircular"`) MSv4 stores. Variant names are the labels.
///
/// Most stores hold receptor products (`XX`, `RL`); a store may also hold
/// Stokes parameters, single-dish polarizations or derived quantities, and
/// a consumer refuses the types it cannot use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Correlation {
    /// Stokes I.
    I,
    /// Stokes Q.
    Q,
    /// Stokes U.
    U,
    /// Stokes V.
    V,
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
    /// Mixed RX.
    RX,
    /// Mixed RY.
    RY,
    /// Mixed LX.
    LX,
    /// Mixed LY.
    LY,
    /// Mixed XR.
    XR,
    /// Mixed XL.
    XL,
    /// Mixed YR.
    YR,
    /// Mixed YL.
    YL,
    /// General quasi-orthogonal PP.
    PP,
    /// General quasi-orthogonal PQ.
    PQ,
    /// General quasi-orthogonal QP.
    QP,
    /// General quasi-orthogonal QQ.
    QQ,
    /// Single-dish right-circular polarization.
    RCircular,
    /// Single-dish left-circular polarization.
    LCircular,
    /// Single-dish linear polarization.
    Linear,
    /// Polarized intensity, `sqrt(Q² + U² + V²)`.
    Ptotal,
    /// Linearly polarized intensity, `sqrt(Q² + U²)`.
    Plinear,
    /// Polarization fraction, `Ptotal / I`.
    PFtotal,
    /// Linear polarization fraction, `Plinear / I`.
    PFlinear,
    /// Linear polarization angle, `0.5 arctan(U / Q)` in radians.
    Pangle,
}

impl Correlation {
    /// Every type, in casacore code order.
    pub const ALL: [Self; 32] = [
        Self::I,
        Self::Q,
        Self::U,
        Self::V,
        Self::RR,
        Self::RL,
        Self::LR,
        Self::LL,
        Self::XX,
        Self::XY,
        Self::YX,
        Self::YY,
        Self::RX,
        Self::RY,
        Self::LX,
        Self::LY,
        Self::XR,
        Self::XL,
        Self::YR,
        Self::YL,
        Self::PP,
        Self::PQ,
        Self::QP,
        Self::QQ,
        Self::RCircular,
        Self::LCircular,
        Self::Linear,
        Self::Ptotal,
        Self::Plinear,
        Self::PFtotal,
        Self::PFlinear,
        Self::Pangle,
    ];

    /// The casacore `Stokes::StokesTypes` code, 1 to 32.
    #[must_use]
    pub const fn casacore_code(self) -> i32 {
        self as i32 + 1
    }

    /// The type with casacore `Stokes` code `code`, or `None` for
    /// `Undefined` (0) and unknown codes.
    #[must_use]
    pub const fn from_casacore_code(code: i32) -> Option<Self> {
        if code >= 1 && code <= Self::ALL.len() as i32 {
            Some(Self::ALL[code as usize - 1])
        } else {
            None
        }
    }

    /// The label MSv4 stores in its `polarization` coordinate.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::I => "I",
            Self::Q => "Q",
            Self::U => "U",
            Self::V => "V",
            Self::RR => "RR",
            Self::RL => "RL",
            Self::LR => "LR",
            Self::LL => "LL",
            Self::XX => "XX",
            Self::XY => "XY",
            Self::YX => "YX",
            Self::YY => "YY",
            Self::RX => "RX",
            Self::RY => "RY",
            Self::LX => "LX",
            Self::LY => "LY",
            Self::XR => "XR",
            Self::XL => "XL",
            Self::YR => "YR",
            Self::YL => "YL",
            Self::PP => "PP",
            Self::PQ => "PQ",
            Self::QP => "QP",
            Self::QQ => "QQ",
            Self::RCircular => "RCircular",
            Self::LCircular => "LCircular",
            Self::Linear => "Linear",
            Self::Ptotal => "Ptotal",
            Self::Plinear => "Plinear",
            Self::PFtotal => "PFtotal",
            Self::PFlinear => "PFlinear",
            Self::Pangle => "Pangle",
        }
    }

    /// The type an MSv4 `polarization` label names.
    #[must_use]
    pub fn from_label(label: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|correlation| correlation.label() == label)
    }

    /// Whether this is a parallel-hand product: two like receptors of one
    /// feed (`RR`, `LL`, `XX`, `YY`, `PP`, `QQ`).
    #[must_use]
    pub const fn is_parallel_hand(self) -> bool {
        matches!(
            self,
            Self::RR | Self::LL | Self::XX | Self::YY | Self::PP | Self::QQ
        )
    }

    /// Whether a stored value enters a Stokes I estimate directly: Stokes I
    /// itself, or a parallel hand of a circular or linear feed
    /// (`I = (RR + LL) / 2 = (XX + YY) / 2`).
    #[must_use]
    pub const fn contributes_to_stokes_i(self) -> bool {
        matches!(self, Self::I | Self::RR | Self::LL | Self::XX | Self::YY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlations_round_trip_their_persistent_forms() {
        for code in 1..=32 {
            let correlation = Correlation::from_casacore_code(code).expect("defined code");
            assert_eq!(correlation.casacore_code(), code);
            assert_eq!(
                Correlation::from_label(correlation.label()),
                Some(correlation)
            );
        }
        for code in [-1, 0, 33] {
            assert_eq!(Correlation::from_casacore_code(code), None);
        }
        assert_eq!(Correlation::from_label("Undefined"), None);
    }

    /// The codes are casacore's `Stokes::StokesTypes` and the labels are the
    /// names MSv4 (XRADIO's `stokes_types`) gives them.
    #[test]
    fn codes_and_labels_are_casacore_and_msv4() {
        for (code, label) in [
            (1, "I"),
            (4, "V"),
            (5, "RR"),
            (9, "XX"),
            (12, "YY"),
            (13, "RX"),
            (20, "YL"),
            (21, "PP"),
            (24, "QQ"),
            (25, "RCircular"),
            (27, "Linear"),
            (28, "Ptotal"),
            (32, "Pangle"),
        ] {
            assert_eq!(
                Correlation::from_casacore_code(code).map(Correlation::label),
                Some(label)
            );
        }
    }

    #[test]
    fn hands_and_stokes_i() {
        assert!(Correlation::LL.is_parallel_hand());
        assert!(Correlation::QQ.is_parallel_hand());
        assert!(!Correlation::YX.is_parallel_hand());
        assert!(!Correlation::RX.is_parallel_hand());
        assert!(!Correlation::I.is_parallel_hand());
        let stokes_i: Vec<_> = Correlation::ALL
            .into_iter()
            .filter(|correlation| correlation.contributes_to_stokes_i())
            .collect();
        assert_eq!(
            stokes_i,
            [
                Correlation::I,
                Correlation::RR,
                Correlation::LL,
                Correlation::XX,
                Correlation::YY
            ]
        );
    }
}
