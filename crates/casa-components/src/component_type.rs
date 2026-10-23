// SPDX-License-Identifier: LGPL-3.0-or-later
//! The names CASA gives shapes, spectral models and polarisation
//! representations, and its rules for parsing them (`ComponentType`).
//!
//! A component-list table stores each name as a string. CASA writes the
//! canonical names below and reads a name back by capitalising every word
//! (casacore's `String::capitalize`) and then:
//!
//! - for a **shape**, taking the first canonical name that *contains* the
//!   capitalised string, in the order point, Gaussian, disk, limb-darkened
//!   disk (a minimum match: `"gaus"` is a Gaussian, `"disk"` a disk);
//! - for a **spectral model** or a **polarisation**, requiring the
//!   capitalised string to *equal* a canonical name.

use crate::error::ComponentError;

/// The kind of a component's shape, CASA's `ComponentType::Shape`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShapeType {
    /// A point source (`POINT`).
    Point,
    /// An elliptical Gaussian (`GAUSSIAN`).
    Gaussian,
    /// A uniform elliptical disk (`DISK`).
    Disk,
    /// A limb-darkened elliptical disk (`LDISK`).
    LimbDarkenedDisk,
}

impl ShapeType {
    /// Every shape, in CASA's enumeration order (the order names are matched).
    pub const ALL: [Self; 4] = [
        Self::Point,
        Self::Gaussian,
        Self::Disk,
        Self::LimbDarkenedDisk,
    ];

    /// The name CASA writes in a component list's `Shape` column
    /// (`ComponentType::name`).
    pub fn casa_name(self) -> &'static str {
        match self {
            Self::Point => "Point",
            Self::Gaussian => "Gaussian",
            Self::Disk => "Disk",
            Self::LimbDarkenedDisk => "Limbdarkeneddisk",
        }
    }

    /// Parse a shape name as `ComponentType::shape` does: capitalise each
    /// word, then take the first canonical name containing the result.
    ///
    /// # Errors
    ///
    /// [`ComponentError::UnknownShape`] when no canonical name contains it.
    pub fn parse_casa(name: &str) -> Result<Self, ComponentError> {
        let canonical = capitalize_words(name);
        Self::ALL
            .into_iter()
            .find(|shape| shape.casa_name().contains(canonical.as_str()))
            .ok_or_else(|| ComponentError::UnknownShape(name.to_string()))
    }
}

/// The kind of a component's spectral model, CASA's
/// `ComponentType::SpectralShape`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpectralType {
    /// A flat spectrum (`CONSTANT_SPECTRUM`).
    Constant,
    /// A power law in frequency (`SPECTRAL_INDEX`).
    SpectralIndex,
    /// Flux tabulated against frequency (`TABULAR_SPECTRUM`).
    Tabular,
    /// A power law whose exponent is a polynomial in the logarithm of the
    /// frequency ratio (`PLP`).
    PowerLogPolynomial,
}

impl SpectralType {
    /// Every spectral model, in CASA's enumeration order.
    pub const ALL: [Self; 4] = [
        Self::Constant,
        Self::SpectralIndex,
        Self::Tabular,
        Self::PowerLogPolynomial,
    ];

    /// The name CASA writes in a component list's `Spectrum_Shape` column and
    /// in a spectral record's `type` field (`ComponentType::name`).
    pub fn casa_name(self) -> &'static str {
        match self {
            Self::Constant => "Constant",
            Self::SpectralIndex => "Spectral Index",
            Self::Tabular => "Tabular Spectrum",
            Self::PowerLogPolynomial => "Power Logarithmic Polynomial",
        }
    }

    /// Parse a spectral-model name as `ComponentType::spectralShape` does:
    /// capitalise each word, then require an exact match.
    ///
    /// # Errors
    ///
    /// [`ComponentError::UnknownSpectrum`] when no canonical name matches.
    pub fn parse_casa(name: &str) -> Result<Self, ComponentError> {
        let canonical = capitalize_words(name);
        Self::ALL
            .into_iter()
            .find(|spectrum| spectrum.casa_name() == canonical)
            .ok_or_else(|| ComponentError::UnknownSpectrum(name.to_string()))
    }
}

/// How a flux's four values represent the polarisation, CASA's
/// `ComponentType::Polarisation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Polarisation {
    /// I, Q, U and V (`STOKES`). The values are real.
    Stokes,
    /// XX, XY, YX and YY (`LINEAR`), complex.
    Linear,
    /// RR, RL, LR and LL (`CIRCULAR`), complex.
    Circular,
}

impl Polarisation {
    /// Every representation, in CASA's enumeration order.
    pub const ALL: [Self; 3] = [Self::Stokes, Self::Linear, Self::Circular];

    /// The name CASA writes in a component list's `Flux_Polarisation` column
    /// (`ComponentType::name`).
    pub fn casa_name(self) -> &'static str {
        match self {
            Self::Stokes => "Stokes",
            Self::Linear => "Linear",
            Self::Circular => "Circular",
        }
    }

    /// Parse a representation name as `ComponentType::polarisation` does:
    /// capitalise each word, then require an exact match.
    ///
    /// # Errors
    ///
    /// [`ComponentError::UnknownPolarisation`] when no canonical name
    /// matches.
    pub fn parse_casa(name: &str) -> Result<Self, ComponentError> {
        let canonical = capitalize_words(name);
        Self::ALL
            .into_iter()
            .find(|polarisation| polarisation.casa_name() == canonical)
            .ok_or_else(|| ComponentError::UnknownPolarisation(name.to_string()))
    }
}

/// casacore's `String::capitalize`: the first letter or digit of every word
/// (a run of ASCII letters and digits) upper case, the rest lower case.
fn capitalize_words(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_word = false;
    for character in text.chars() {
        if character.is_ascii_alphanumeric() {
            if in_word {
                out.push(character.to_ascii_lowercase());
            } else {
                in_word = true;
                out.push(character.to_ascii_uppercase());
            }
        } else {
            in_word = false;
            out.push(character);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capitalisation_follows_casacore_words() {
        assert_eq!(capitalize_words("spectral index"), "Spectral Index");
        assert_eq!(capitalize_words("LIMB-darkened DISK"), "Limb-Darkened Disk");
        assert_eq!(capitalize_words("tabular  spectrum"), "Tabular  Spectrum");
        assert_eq!(capitalize_words("2nd"), "2nd");
    }

    #[test]
    fn shapes_match_by_containment_in_enumeration_order() {
        assert_eq!(ShapeType::parse_casa("gaus"), Ok(ShapeType::Gaussian));
        assert_eq!(ShapeType::parse_casa("POINT"), Ok(ShapeType::Point));
        // "Disk" is contained in "Disk" before "Limbdarkeneddisk" is tried.
        assert_eq!(ShapeType::parse_casa("disk"), Ok(ShapeType::Disk));
        assert_eq!(
            ShapeType::parse_casa("limbdarkeneddisk"),
            Ok(ShapeType::LimbDarkenedDisk)
        );
        // casacore's capitalisation makes "Limb-Darkened Disk", which no
        // canonical name contains.
        assert!(ShapeType::parse_casa("limb-darkened disk").is_err());
        assert!(ShapeType::parse_casa("ring").is_err());
        for shape in ShapeType::ALL {
            assert_eq!(ShapeType::parse_casa(shape.casa_name()), Ok(shape));
        }
    }

    #[test]
    fn spectra_and_polarisations_need_exact_matches() {
        assert_eq!(
            SpectralType::parse_casa("spectral index"),
            Ok(SpectralType::SpectralIndex)
        );
        assert!(SpectralType::parse_casa("spectral").is_err());
        assert_eq!(
            Polarisation::parse_casa("CIRCULAR"),
            Ok(Polarisation::Circular)
        );
        assert!(Polarisation::parse_casa("lin").is_err());
        for spectrum in SpectralType::ALL {
            assert_eq!(SpectralType::parse_casa(spectrum.casa_name()), Ok(spectrum));
        }
        for polarisation in Polarisation::ALL {
            assert_eq!(
                Polarisation::parse_casa(polarisation.casa_name()),
                Ok(polarisation)
            );
        }
    }
}
