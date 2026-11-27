// SPDX-License-Identifier: LGPL-3.0-or-later

//! The restoring beam a problem's restored products are published with.
//!
//! CASA's `tclean` restores each plane with the Gaussian fitted to its PSF
//! (`restoringbeam=''`), with one beam enclosing every plane's fitted beam
//! (`'common'`), or with a beam the user gives by its axes and angle
//! (`restoringbeam=['10arcsec','8arcsec','45deg']`). In the last two the
//! residual is first brought to the restoring beam's resolution
//! (`SIImageStore::restore`, `rescaleResolution`).

use thiserror::Error;

/// Restoring-beam requirement for published products.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoringBeamPolicy {
    /// Do not create a restored product.
    None,
    /// Fit and use an independent beam for each plane.
    PerPlane,
    /// Use one common enclosing beam across all spectral planes.
    Common,
    /// Use the given beam for every plane, as CASA restores with a
    /// `restoringbeam` given by its axes and angle.
    Fixed(FixedRestoringBeam),
}

/// A restoring beam given by the request: its full widths at half maximum
/// and position angle, in radians.
///
/// Equality compares the three values bit for bit, so the beam can be part
/// of a policy that is compared exactly.
#[derive(Debug, Clone, Copy)]
pub struct FixedRestoringBeam {
    major_fwhm_rad: f64,
    minor_fwhm_rad: f64,
    position_angle_rad: f64,
}

/// Why a restoring beam's axes and angle do not make a beam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "a restoring beam needs finite, positive axes with the major axis at least the minor, \
     and a finite position angle"
)]
pub struct InvalidRestoringBeam;

impl FixedRestoringBeam {
    /// The beam with these full widths at half maximum and position angle
    /// (east from north), in radians.
    ///
    /// # Errors
    ///
    /// [`InvalidRestoringBeam`] when an axis is not finite and positive, the
    /// minor axis exceeds the major, or the angle is not finite: casacore's
    /// `GaussianBeam::setMajorMinor` refuses the same axes, and a zero axis
    /// is its null beam, which restores with the fitted beams instead.
    pub fn new(
        major_fwhm_rad: f64,
        minor_fwhm_rad: f64,
        position_angle_rad: f64,
    ) -> Result<Self, InvalidRestoringBeam> {
        let positive = |axis: f64| axis.is_finite() && axis > 0.0;
        if !(positive(major_fwhm_rad)
            && positive(minor_fwhm_rad)
            && major_fwhm_rad >= minor_fwhm_rad
            && position_angle_rad.is_finite())
        {
            return Err(InvalidRestoringBeam);
        }
        Ok(Self {
            major_fwhm_rad,
            minor_fwhm_rad,
            position_angle_rad,
        })
    }

    /// Major-axis full width at half maximum in radians.
    #[must_use]
    pub const fn major_fwhm_rad(self) -> f64 {
        self.major_fwhm_rad
    }

    /// Minor-axis full width at half maximum in radians.
    #[must_use]
    pub const fn minor_fwhm_rad(self) -> f64 {
        self.minor_fwhm_rad
    }

    /// Position angle, east from north, in radians, as given.
    #[must_use]
    pub const fn position_angle_rad(self) -> f64 {
        self.position_angle_rad
    }
}

impl PartialEq for FixedRestoringBeam {
    fn eq(&self, other: &Self) -> bool {
        self.major_fwhm_rad.to_bits() == other.major_fwhm_rad.to_bits()
            && self.minor_fwhm_rad.to_bits() == other.minor_fwhm_rad.to_bits()
            && self.position_angle_rad.to_bits() == other.position_angle_rad.to_bits()
    }
}

impl Eq for FixedRestoringBeam {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fixed_beam_takes_casas_axes_and_compares_exactly() {
        let beam = FixedRestoringBeam::new(2.0e-5, 1.0e-5, 0.5).expect("a beam");
        assert_eq!(
            (
                beam.major_fwhm_rad(),
                beam.minor_fwhm_rad(),
                beam.position_angle_rad()
            ),
            (2.0e-5, 1.0e-5, 0.5)
        );
        assert_eq!(
            RestoringBeamPolicy::Fixed(beam),
            RestoringBeamPolicy::Fixed(FixedRestoringBeam::new(2.0e-5, 1.0e-5, 0.5).unwrap())
        );
        assert_ne!(
            RestoringBeamPolicy::Fixed(beam),
            RestoringBeamPolicy::Fixed(FixedRestoringBeam::new(2.0e-5, 1.0e-5, 0.0).unwrap())
        );
        assert_ne!(
            RestoringBeamPolicy::Fixed(beam),
            RestoringBeamPolicy::Common
        );
        // A circular beam is a beam.
        FixedRestoringBeam::new(1.0e-5, 1.0e-5, 0.0).expect("a circular beam");
        for (major, minor, angle) in [
            (1.0e-5, 2.0e-5, 0.0),
            (0.0, 0.0, 0.0),
            (-1.0e-5, -2.0e-5, 0.0),
            (f64::INFINITY, 1.0e-5, 0.0),
            (1.0e-5, f64::NAN, 0.0),
            (2.0e-5, 1.0e-5, f64::NAN),
        ] {
            assert_eq!(
                FixedRestoringBeam::new(major, minor, angle),
                Err(InvalidRestoringBeam),
                "{major} {minor} {angle}"
            );
        }
    }
}
