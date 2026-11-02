// SPDX-License-Identifier: LGPL-3.0-or-later

//! Per-sample antenna quantities of the batch geometry: each antenna's
//! pointing direction and aperture response class.

use crate::geometry::SkyDirection;

/// Per-antenna pointing directions evaluated for one baseline sample.
///
/// Retaining both directions preserves antenna-dependent boresight offsets. A downstream
/// operator may derive a baseline-effective direction according to its own closed response law.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedPointingDirections {
    /// Evaluated direction for MAIN `ANTENNA1`.
    pub antenna1: SkyDirection,
    /// Evaluated direction for MAIN `ANTENNA2`.
    pub antenna2: SkyDirection,
}

/// CASA aperture class selected from owner-controlled ANTENNA metadata.
///
/// These are scientific response identities rather than physical antenna IDs.
/// Multiple antennas with the same class therefore share one cached response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AntennaResponseClass {
    /// Physical 12 m ALMA dish evaluated with CASA's 10.7 m effective aperture.
    CasaAlma12m,
    /// Physical 7 m ACA dish evaluated with CASA's 6.25 m effective aperture.
    CasaAca7m,
}

impl AntennaResponseClass {
    /// Whether CASA treats `telescope` as ALMA or ACA, whose dishes it
    /// classes by [`Self::casa_alma_aca`].
    #[must_use]
    pub fn casa_alma_aca_telescope(telescope: &str) -> bool {
        matches!(telescope.trim(), "ALMA" | "ACA")
    }

    /// CASA's class of an ALMA or ACA dish `diameter_m` across: 12 m
    /// within 0.5 m, 7 m within 1 m; `None` for another dish.
    #[must_use]
    pub fn casa_alma_aca(diameter_m: f64) -> Option<Self> {
        if (diameter_m - 12.0).abs() < 0.5 {
            Some(Self::CasaAlma12m)
        } else if (diameter_m - 7.0).abs() < 1.0 {
            Some(Self::CasaAca7m)
        } else {
            None
        }
    }

    /// The largest response of an antenna family whose dishes are
    /// `classes`: the family envelope CASA's heterogeneous mosaic sizes its
    /// kernels by.
    #[must_use]
    pub fn family_envelope(classes: &[Option<Self>]) -> Option<Self> {
        [Self::CasaAlma12m, Self::CasaAca7m]
            .into_iter()
            .find(|class| classes.contains(&Some(*class)))
    }
}

/// Ordered response classes for one selected interferometric baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedAntennaResponses {
    /// Response class of MAIN `ANTENNA1`.
    pub antenna1: AntennaResponseClass,
    /// Response class of MAIN `ANTENNA2`.
    pub antenna2: AntennaResponseClass,
    /// Largest response class present in this MeasurementSet response family.
    ///
    /// CASA crops every heterogeneous convolution plane to one family-wide
    /// extent so resampling has the same boundary domain for every pair.
    pub family_envelope: AntennaResponseClass,
}
