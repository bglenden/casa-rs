// SPDX-License-Identifier: LGPL-3.0-or-later

//! The visibility-weighting contract: CASA's imaging-weight formulas
//! (`SynthesisImagerVi2::weight`), the scope and cells of the visibility
//! density the density formulas divide by, and the Gaussian uv taper
//! (`VisImagingWeight::setFilter`).
//!
//! CASA's density formulas are one law, `w / (D·f² + d²)` over the gridded
//! weight density `D` (`VisImagingWeight::weightUniform`,
//! `BriggsCubeWeightor::getWeightUniform`): uniform is `f² = 1`, `d² = 0`,
//! and Briggs's two robustness modes choose `f²` and `d²`. What CASA's
//! `npixels` changes is the grid `D` lives on ([`DensityCells`]).

use std::num::NonZeroUsize;

/// Visibility-weighting formula.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WeightingScheme {
    /// Natural weighting.
    Natural,
    /// CASA `radial`: each sample's weight times its uv distance in
    /// wavelengths (`VisImagingWeight::weightRadial`); no density.
    Radial,
    /// Uniform density weighting, `w / D`: CASA `uniform`, and CASA
    /// `superuniform` with a [`DensityCells::Box`] density.
    Uniform,
    /// Briggs robust weighting (CASA `rmode = "norm"`):
    /// `f² = (5·10^{−robust})² / (ΣD² / ΣD)`, `d² = 1`.
    Briggs {
        /// Robustness in the conventional interval `[-2, 2]`.
        robust: f64,
    },
    /// CASA `briggsabs` (`rmode = "abs"`): `f² = robust²` and
    /// `d² = 2·noise²`, the expected noise of one visibility of unit weight.
    BriggsAbsolute {
        /// Robustness in the conventional interval `[-2, 2]`.
        robust: f64,
        /// The noise in Jy; it and `robust` are not both zero.
        noise_jy: f64,
    },
    /// Briggs bandwidth-taper weighting.
    BriggsBandwidthTaper {
        /// Robustness in the conventional interval `[-2, 2]`.
        robust: f64,
    },
}

impl WeightingScheme {
    /// Whether the formula divides by a gridded visibility density.
    #[must_use]
    pub const fn uses_density(self) -> bool {
        !matches!(self, Self::Natural | Self::Radial)
    }
}

/// Domain over which visibility-density weights are derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightDensityScope {
    /// The weighting formula does not use a density generation.
    NotApplicable,
    /// All selected data contributing to the logical product.
    GlobalSelection,
    /// An explicit global density generation per output channel.
    PerOutputChannel,
}

/// The cells a visibility density is gridded on: CASA's `npixels`, whose
/// meaning `SynthesisImagerVi2::weight` takes from the weighting and the
/// density scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DensityCells {
    /// The image's own uv cells; a sample adds to the cell it falls in.
    #[default]
    Image,
    /// An `n × n` grid spanning the image's uv extent, so each cell is
    /// `N / n` image uv cells wide along an image axis of `N` pixels: CASA's
    /// continuum (`VisImagingWeight`) uniform and Briggs density with
    /// `npixels = n`, its sidelobes suppressed over the inner `n` pixels.
    Square(NonZeroUsize),
    /// The image's uv cells, each sample spread over the `(2b + 1)²` cells
    /// about its own: CASA `superuniform`'s box (`VisImagingWeight`'s
    /// `uBox = vBox = b`, with `b = npixels`, or 3 when `npixels ≤ 0`), and a
    /// cube's per-channel density with `npixels = b`
    /// (`BriggsCubeWeightor`'s `superUniformBox`).
    Box(NonZeroUsize),
}

/// Gaussian taper in the UV plane, expressed as baseline HWHM wavelengths.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UvTaper {
    major_lambda: f64,
    minor_lambda: f64,
    position_angle_rad: f64,
}

impl UvTaper {
    /// Construct a Gaussian UV taper from major/minor baseline HWHM and angle.
    #[must_use]
    pub const fn new(major_lambda: f64, minor_lambda: f64, position_angle_rad: f64) -> Self {
        Self {
            major_lambda,
            minor_lambda,
            position_angle_rad,
        }
    }

    /// Return the major-axis baseline HWHM in wavelengths.
    #[must_use]
    pub const fn major_lambda(self) -> f64 {
        self.major_lambda
    }

    /// Return the minor-axis baseline HWHM in wavelengths.
    #[must_use]
    pub const fn minor_lambda(self) -> f64 {
        self.minor_lambda
    }

    /// Return the position angle in radians.
    #[must_use]
    pub const fn position_angle_rad(self) -> f64 {
        self.position_angle_rad
    }
}

/// Complete logical visibility-weighting requirements.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightingContract {
    scheme: WeightingScheme,
    density_scope: WeightDensityScope,
    density_cells: DensityCells,
    uv_taper: Option<UvTaper>,
    casa_cube_density_padding: Option<usize>,
}

impl WeightingContract {
    /// Construct weighting requirements on the image's own density cells.
    #[must_use]
    pub const fn new(scheme: WeightingScheme, density_scope: WeightDensityScope) -> Self {
        Self {
            scheme,
            density_scope,
            density_cells: DensityCells::Image,
            uv_taper: None,
            casa_cube_density_padding: None,
        }
    }

    /// Grid the density on `cells` (CASA's `npixels`).
    #[must_use]
    pub const fn with_density_cells(mut self, cells: DensityCells) -> Self {
        self.density_cells = cells;
        self
    }

    /// Add a Gaussian UV taper to the weighting metric.
    #[must_use]
    pub const fn with_uv_taper(mut self, uv_taper: UvTaper) -> Self {
        self.uv_taper = Some(uv_taper);
        self
    }

    /// Bind CASA's cube density and native-weight transfer law, with a
    /// metadata-derived number of density channels on each side of the image.
    /// Zero padding still selects the cube law. The published axis is unchanged.
    #[must_use]
    pub const fn with_casa_cube_density_padding(mut self, channels_per_side: usize) -> Self {
        self.casa_cube_density_padding = Some(channels_per_side);
        self
    }

    /// Return the bound CASA cube law's symmetric density-axis padding.
    #[must_use]
    pub const fn casa_cube_density_padding(self) -> Option<usize> {
        self.casa_cube_density_padding
    }

    /// Return the weighting formula.
    #[must_use]
    pub const fn scheme(self) -> WeightingScheme {
        self.scheme
    }

    /// Return the density-generation scope.
    #[must_use]
    pub const fn density_scope(self) -> WeightDensityScope {
        self.density_scope
    }

    /// Return the cells the density is gridded on.
    #[must_use]
    pub const fn density_cells(self) -> DensityCells {
        self.density_cells
    }

    /// Return the optional Gaussian UV taper.
    #[must_use]
    pub const fn uv_taper(self) -> Option<UvTaper> {
        self.uv_taper
    }

    /// Why the requirements are outside the logical domain, if they are.
    pub(crate) fn validate(self) -> Result<(), &'static str> {
        if self.casa_cube_density_padding.is_some()
            && self.density_scope != WeightDensityScope::PerOutputChannel
        {
            return Err("CASA cube density requires per-output-channel weighting");
        }
        let density = self.scheme.uses_density();
        if !density && self.density_scope != WeightDensityScope::NotApplicable {
            return Err("natural and radial weighting have no density generation");
        }
        if density && self.density_scope == WeightDensityScope::NotApplicable {
            return Err("density weighting requires an explicit global density scope");
        }
        match self.density_cells {
            DensityCells::Image => {}
            _ if !density => return Err("a weighting without a density has no density cells"),
            DensityCells::Square(_)
                if self.density_scope != WeightDensityScope::GlobalSelection =>
            {
                return Err("a square density grid is CASA's continuum density");
            }
            _ if matches!(self.scheme, WeightingScheme::BriggsBandwidthTaper { .. }) => {
                return Err("CASA's bandwidth-tapered Briggs density has the image's cells");
            }
            DensityCells::Square(_) | DensityCells::Box(_) => {}
        }
        let robust = match self.scheme {
            WeightingScheme::Briggs { robust }
            | WeightingScheme::BriggsAbsolute { robust, .. }
            | WeightingScheme::BriggsBandwidthTaper { robust } => Some(robust),
            WeightingScheme::Natural | WeightingScheme::Radial | WeightingScheme::Uniform => None,
        };
        if robust.is_some_and(|value| !(value.is_finite() && (-2.0..=2.0).contains(&value))) {
            return Err("Briggs robustness must be finite and in [-2, 2]");
        }
        if let WeightingScheme::BriggsAbsolute { robust, noise_jy } = self.scheme
            && (!noise_jy.is_finite() || (robust == 0.0 && noise_jy == 0.0))
        {
            // CASA's denominator robust²·D + 2·noise² would vanish
            // (`SynthesisImagerVi2::weight` refuses it for cubes).
            return Err("absolute Briggs noise must be finite, and not zero with robust zero");
        }
        if self.uv_taper.is_some_and(|taper| {
            !(taper.major_lambda.is_finite()
                && taper.major_lambda > 0.0
                && taper.minor_lambda.is_finite()
                && taper.minor_lambda > 0.0
                && taper.position_angle_rad.is_finite())
        }) {
            return Err("UV taper axes must be finite and positive and its angle must be finite");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).expect("non-zero")
    }

    fn contract(scheme: WeightingScheme, scope: WeightDensityScope) -> WeightingContract {
        WeightingContract::new(scheme, scope)
    }

    #[test]
    fn density_cells_follow_casas_npixels_readings() {
        use WeightDensityScope::{GlobalSelection, NotApplicable, PerOutputChannel};
        let briggs = WeightingScheme::Briggs { robust: 0.5 };
        let absolute = WeightingScheme::BriggsAbsolute {
            robust: 1.0,
            noise_jy: 0.1,
        };
        for valid in [
            contract(WeightingScheme::Radial, NotApplicable),
            contract(WeightingScheme::Uniform, GlobalSelection)
                .with_density_cells(DensityCells::Square(cells(32))),
            contract(briggs, GlobalSelection).with_density_cells(DensityCells::Square(cells(7))),
            contract(absolute, GlobalSelection),
            // Superuniform is uniform over a box.
            contract(WeightingScheme::Uniform, GlobalSelection)
                .with_density_cells(DensityCells::Box(cells(3))),
            contract(absolute, PerOutputChannel)
                .with_density_cells(DensityCells::Box(cells(2)))
                .with_casa_cube_density_padding(4),
        ] {
            assert_eq!(valid.validate(), Ok(()), "{valid:?}");
        }
        for invalid in [
            contract(WeightingScheme::Radial, GlobalSelection),
            contract(WeightingScheme::Natural, NotApplicable)
                .with_density_cells(DensityCells::Box(cells(3))),
            contract(briggs, PerOutputChannel).with_density_cells(DensityCells::Square(cells(8))),
            contract(
                WeightingScheme::BriggsBandwidthTaper { robust: 0.5 },
                PerOutputChannel,
            )
            .with_density_cells(DensityCells::Box(cells(1))),
            contract(
                WeightingScheme::BriggsAbsolute {
                    robust: 0.0,
                    noise_jy: 0.0,
                },
                GlobalSelection,
            ),
            contract(
                WeightingScheme::BriggsAbsolute {
                    robust: 2.5,
                    noise_jy: 1.0,
                },
                GlobalSelection,
            ),
            contract(
                WeightingScheme::BriggsAbsolute {
                    robust: 0.5,
                    noise_jy: f64::NAN,
                },
                GlobalSelection,
            ),
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        // Either term alone keeps the absolute denominator positive.
        for (robust, noise_jy) in [(0.0, 1.0), (-1.0, 0.0)] {
            let scheme = WeightingScheme::BriggsAbsolute { robust, noise_jy };
            assert_eq!(contract(scheme, GlobalSelection).validate(), Ok(()));
        }
    }
}
