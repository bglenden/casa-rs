// SPDX-License-Identifier: LGPL-3.0-or-later
//! Correlation ordering for single-dish spectra supplied as real planes.

use casa_ms::{MsError, MsResult};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum Basis {
    Linear,
    Circular,
    Stokes,
    LinearPolarization,
}

impl Basis {
    pub(super) fn parse(text: &str) -> MsResult<Self> {
        match text {
            "linear" => Ok(Self::Linear),
            "circular" => Ok(Self::Circular),
            "stokes" => Ok(Self::Stokes),
            "linpol" => Ok(Self::LinearPolarization),
            _ => Err(MsError::InvalidInput(format!(
                "Invalid polarization basis {text}"
            ))),
        }
    }

    pub(super) fn has_parallel_hands(self) -> bool {
        matches!(self, Basis::Linear | Basis::Circular)
    }

    pub(super) fn feeds(self) -> [&'static str; 2] {
        if self == Self::Circular {
            ["R", "L"]
        } else {
            ["X", "Y"]
        }
    }

    pub(super) fn correlations(self, planes: &[usize]) -> MsResult<Vec<(i32, usize)>> {
        let codes: &[i32] = match self {
            Self::Linear => &[9, 12, 10, 11],
            Self::Circular => &[5, 8, 6, 7],
            Self::Stokes => &[1, 2, 3, 4],
            Self::LinearPolarization => &[29, 32],
        };
        let valid = match self {
            Self::Linear | Self::Circular => {
                matches!(planes, [0] | [1] | [0, 1] | [0, 1, 2, 3])
            }
            Self::Stokes => matches!(planes, [0] | [0, 1, 2, 3]),
            Self::LinearPolarization => matches!(planes, [0] | [0, 1]),
        };
        if !valid {
            return Err(MsError::InvalidInput(format!(
                "Non-conforming {self:?} polarization planes: {planes:?}"
            )));
        }
        let mut result = planes
            .iter()
            .map(|&plane| (codes[plane], plane))
            .collect::<Vec<_>>();
        result.sort_unstable();
        Ok(result)
    }
}
