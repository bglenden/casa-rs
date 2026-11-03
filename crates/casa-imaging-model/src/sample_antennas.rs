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
    /// Every antenna of an EVLA MeasurementSet, whatever its dish: CASA's
    /// common EVLA beam (`PBMath1DEVLA`), which
    /// `HetArrayConvFunc::findAntennaSizes` gives the whole array when
    /// `SynthesisImagerVi2::createMosFTMachine` builds it for
    /// `PBMath::EVLA` (`PBMathInterface::COMMONPB`).
    CasaEvla,
    /// A dish of a telescope CASA's voltage-pattern manager has no beam
    /// for ([`Self::casa_airy_telescope`]), `dish_diameter_um` micrometres
    /// across: the Airy disk of the dish itself that
    /// `HetArrayConvFunc::findAntennaSizes` forms for
    /// `PBMathInterface::AIRY`, one per dish diameter (CASA keys them by
    /// the diameter's text).
    CasaAiry {
        /// The ANTENNA table's `DISH_DIAMETER`, in micrometres.
        dish_diameter_um: u32,
    },
}

impl AntennaResponseClass {
    /// Whether CASA treats `telescope` as ALMA or ACA, whose dishes it
    /// classes by [`Self::casa_alma_aca`].
    #[must_use]
    pub fn casa_alma_aca_telescope(telescope: &str) -> bool {
        matches!(telescope.trim(), "ALMA" | "ACA")
    }

    /// Whether CASA's mosaic gives `telescope` the common EVLA beam
    /// ([`Self::CasaEvla`]): `PBMath::enumerateCommonPB` names it
    /// `EVLA`, exactly.
    #[must_use]
    pub fn casa_evla_telescope(telescope: &str) -> bool {
        telescope.trim() == "EVLA"
    }

    /// Whether CASA's mosaic forms an Airy disk of each dish of
    /// `telescope` ([`Self::CasaAiry`]): `SynthesisImager::getVPRecord`
    /// finds no default voltage pattern under the name (`VPManager` lists
    /// those of `PBMath::nameCommonPB` and the observatories with antenna
    /// responses, and matches the name exactly), so it leaves
    /// `PBMath::UNKNOWN`, for which `SynthesisImagerVi2::createMosFTMachine`
    /// gives `HetArrayConvFunc` its `PBMathInterface::AIRY` class.
    /// `NGVLA1`, an ngVLA simulation's name, is one; `NGVLA`, whose common
    /// beam the mosaic applies instead, is not. Nor are `UNKNOWN` and
    /// `OSF`, which have default records but no common beam the mosaic
    /// can form.
    #[must_use]
    pub fn casa_airy_telescope(telescope: &str) -> bool {
        !CASA_DEFAULT_VOLTAGE_PATTERNS.contains(&telescope)
    }

    /// CASA's class of a dish `diameter_m` across of a telescope without a
    /// default voltage pattern ([`Self::casa_airy_telescope`]); `None` for a
    /// diameter that is not positive and finite, or does not fit
    /// [`Self::CasaAiry`]'s micrometres (`findAntennaSizes` gives no dish
    /// to one that is not positive).
    #[must_use]
    pub fn casa_airy(diameter_m: f64) -> Option<Self> {
        let micrometres = (diameter_m * 1.0e6).round();
        let fits = (1.0..=f64::from(u32::MAX)).contains(&micrometres);
        (diameter_m.is_finite() && fits).then_some(Self::CasaAiry {
            dish_diameter_um: micrometres as u32,
        })
    }

    /// The dish diameter of a [`Self::CasaAiry`] class, in metres; `None`
    /// for the other classes, whose apertures CASA fixes.
    #[must_use]
    pub fn airy_dish_diameter_m(self) -> Option<f64> {
        match self {
            Self::CasaAiry { dish_diameter_um } => Some(f64::from(dish_diameter_um) * 1.0e-6),
            Self::CasaAlma12m | Self::CasaAca7m | Self::CasaEvla => None,
        }
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
    /// kernels by; of Airy dishes, the widest.
    #[must_use]
    pub fn family_envelope(classes: &[Option<Self>]) -> Option<Self> {
        [Self::CasaAlma12m, Self::CasaAca7m, Self::CasaEvla]
            .into_iter()
            .find(|class| classes.contains(&Some(*class)))
            .or_else(|| {
                classes
                    .iter()
                    .flatten()
                    .copied()
                    .filter(|class| class.airy_dish_diameter_m().is_some())
                    .max()
            })
    }
}

/// The telescope names CASA's `VPManager::reset` gives a default voltage
/// pattern (CASA 6.7.6): `PBMath::nameCommonPB` of every common beam from
/// `ATCA_L1` to `NGVLA` (`NRAO140FT`'s name is `UNKNOWN`), and the
/// observatories whose antenna responses the Observatories table names
/// (`ALMA`, `ACA` and `OSF`).
const CASA_DEFAULT_VOLTAGE_PATTERNS: [&str; 42] = [
    "ATCA_L1",
    "ATCA_L2",
    "ATCA_L3",
    "ATCA_S",
    "ATCA_C",
    "ATCA_X",
    "GBT",
    "GMRT",
    "HATCREEK",
    "NRAO12M",
    "UNKNOWN",
    "OVRO",
    "VLA",
    "VLA_INVERSE",
    "VLA_NVSS",
    "VLA_2NULL",
    "VLA_4",
    "VLA_P",
    "VLA_L",
    "VLA_C",
    "VLA_X",
    "VLA_U",
    "VLA_K",
    "VLA_Q",
    "WSRT",
    "WSRT_LOW",
    "ALMA",
    "ALMASD",
    "ACA",
    "IRAMPDB",
    "IRAM30M",
    "SMA",
    "ATA",
    "ATCA_C_RI",
    "ATCA",
    "ATCA_16",
    "ATCA_K",
    "ATCA_Q",
    "ATCA_W",
    "EVLA",
    "NGVLA",
    "OSF",
];

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

#[cfg(test)]
mod tests {
    use super::*;

    /// `HetArrayConvFunc::findAntennaSizes` gives every positive dish
    /// diameter its own Airy disk and none to a dish that is not positive;
    /// the widest is the family's envelope.
    #[test]
    fn airy_dishes_are_classed_by_their_diameter() {
        let [eighteen, six] = [18.0, 6.0]
            .map(|diameter| AntennaResponseClass::casa_airy(diameter).expect("a positive dish"));
        assert_eq!(
            eighteen,
            AntennaResponseClass::CasaAiry {
                dish_diameter_um: 18_000_000
            }
        );
        assert_eq!(eighteen.airy_dish_diameter_m(), Some(18.0));
        assert_eq!(six.airy_dish_diameter_m(), Some(6.0));
        assert_eq!(
            AntennaResponseClass::CasaAlma12m.airy_dish_diameter_m(),
            None
        );
        for absent in [0.0, -6.0, f64::NAN, f64::INFINITY] {
            assert_eq!(AntennaResponseClass::casa_airy(absent), None, "{absent}");
        }
        assert_eq!(
            AntennaResponseClass::family_envelope(&[Some(six), None, Some(eighteen), Some(six)]),
            Some(eighteen)
        );
        assert!(AntennaResponseClass::casa_airy_telescope("NGVLA1"));
        assert!(!AntennaResponseClass::casa_airy_telescope("NGVLA"));
        assert!(!AntennaResponseClass::casa_airy_telescope("ALMA"));
    }
}
