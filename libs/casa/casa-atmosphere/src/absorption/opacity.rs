// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMRefractiveIndexProfile.cc, (c) Instituto de
// Estructura de la Materia, Pardo et al., LGPL-2.1-or-later.
//! Zenith opacities, phase delays and path lengths of an
//! [`AbsorptionProfile`].
//!
//! Channels are addressed by their flat index in the grid
//! ([`SpectralGrid::flat_index`](crate::SpectralGrid::flat_index)); an index
//! past the grid panics, as slice indexing does. Wet quantities take the
//! water column (metres) they are wanted for, and scale the profile's own
//! by it, as ATM's `RefractiveIndexProfile` accessors with an
//! `integratedwatercolumn` do.

use super::{
    AbsorptionChannel, AbsorptionProfile, ChannelAbsorption, LayerAbsorption, LayerTotals,
};
use num_complex::Complex64;

/// Degrees per radian as ATM writes it.
const DEGREES_PER_RADIAN: f64 = 57.29578;
/// The speed of light, m/s.
const SPEED_OF_LIGHT: f64 = 299_792_458.0;

/// A species family whose opacity, phase delay or path length a full
/// profile gives on its own (ATM's per-species accessors). Wet families are
/// for the profile's own water column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Absorber {
    /// All O2 lines.
    O2Lines,
    /// (16)O(16)O ground-state lines.
    O2Ground,
    /// (16)O(16)O first-vibrational-state lines.
    O2Vibrational,
    /// (16)O(18)O lines.
    O2Isotope18,
    /// (16)O(17)O lines.
    O2Isotope17,
    /// The dry continuum.
    DryContinuum,
    /// All O3 lines.
    O3Lines,
    /// (16)O3 ground-state lines.
    O3Ground,
    /// (16)O3 ν1 lines.
    O3Nu1,
    /// (16)O3 ν2 lines.
    O3Nu2,
    /// (16)O3 ν3 lines.
    O3Nu3,
    /// (16)O(16)O(18)O lines.
    O3Isotope668,
    /// (16)O(18)O(16)O lines.
    O3Isotope686,
    /// (16)O(16)O(17)O lines.
    O3Isotope667,
    /// (16)O(17)O(16)O lines.
    O3Isotope676,
    /// CO lines.
    CoLines,
    /// N2O lines.
    N2oLines,
    /// NO2 lines.
    No2Lines,
    /// SO2 lines.
    So2Lines,
    /// HCl lines.
    HclLines,
    /// HCN lines.
    HcnLines,
    /// All H2O lines.
    H2oLines,
    /// H2(16)O ground-state lines.
    H2oGround,
    /// H2(16)O bending-state lines.
    H2oBending,
    /// H2(17)O lines.
    H2oIsotope17,
    /// H2(18)O lines.
    H2oIsotope18,
    /// HDO lines.
    Hdo,
    /// The water continuum.
    H2oContinuum,
}

impl Absorber {
    /// This family's refractivity in a layer.
    #[must_use]
    pub fn of(self, layer: &LayerAbsorption) -> Complex64 {
        match self {
            Self::O2Lines => layer.o2,
            Self::O2Ground => layer.o2_16o16o,
            Self::O2Vibrational => layer.o2_vibrational,
            Self::O2Isotope18 => layer.o2_16o18o,
            Self::O2Isotope17 => layer.o2_16o17o,
            Self::DryContinuum => layer.dry_continuum,
            Self::O3Lines => layer.o3,
            Self::O3Ground => layer.o3_ground,
            Self::O3Nu1 => layer.o3_nu1,
            Self::O3Nu2 => layer.o3_nu2,
            Self::O3Nu3 => layer.o3_nu3,
            Self::O3Isotope668 => layer.o3_668,
            Self::O3Isotope686 => layer.o3_686,
            Self::O3Isotope667 => layer.o3_667,
            Self::O3Isotope676 => layer.o3_676,
            Self::CoLines => layer.co,
            Self::N2oLines => layer.n2o,
            Self::No2Lines => layer.no2,
            Self::So2Lines => layer.so2,
            Self::HclLines => layer.hcl,
            Self::HcnLines => layer.hcn,
            Self::H2oLines => layer.h2o,
            Self::H2oGround => layer.hh16o,
            Self::H2oBending => layer.hh16o_bending,
            Self::H2oIsotope17 => layer.hh17o,
            Self::H2oIsotope18 => layer.hh18o,
            Self::Hdo => layer.hdo,
            Self::H2oContinuum => layer.h2o_continuum,
        }
    }
}

impl<C: AbsorptionChannel> AbsorptionProfile<C> {
    /// Σ f(layer totals)·Δz over the layers of `channel`.
    fn integrate(&self, channel: usize, f: impl Fn(&LayerTotals) -> f64) -> f64 {
        let column = &self.channels[channel];
        let mut sum = 0.0;
        for (j, layer) in self.atmosphere.layers().iter().enumerate() {
            sum += f(&column.layer_totals(j)) * layer.thickness_m;
        }
        sum
    }

    /// The ratio of `water_column_m` to the profile's own column.
    fn water_scale(&self, water_column_m: f64) -> f64 {
        water_column_m / self.ground_water_column_m
    }

    /// The wavelength of `channel`, metres.
    fn wavelength_m(&self, channel: usize) -> f64 {
        SPEED_OF_LIGHT / self.channels[channel].frequency_hz()
    }

    /// The zenith dry opacity of `channel`, nepers (ATM's `getDryOpacity`).
    #[must_use]
    pub fn dry_opacity(&self, channel: usize) -> f64 {
        self.integrate(channel, LayerTotals::dry_absorption)
    }

    /// The zenith dry-continuum opacity, nepers (`getDryContOpacity`).
    #[must_use]
    pub fn dry_continuum_opacity(&self, channel: usize) -> f64 {
        self.integrate(channel, |l| l.dry_continuum.im)
    }

    /// The zenith wet opacity for `water_column_m`, nepers
    /// (`getWetOpacity(integratedwatercolumn, …)`).
    #[must_use]
    pub fn wet_opacity(&self, channel: usize, water_column_m: f64) -> f64 {
        self.integrate(channel, LayerTotals::wet_absorption) * self.water_scale(water_column_m)
    }

    /// The zenith H2O-lines opacity for `water_column_m`, nepers.
    #[must_use]
    pub fn h2o_lines_opacity(&self, channel: usize, water_column_m: f64) -> f64 {
        self.integrate(channel, |l| l.h2o_lines.im) * self.water_scale(water_column_m)
    }

    /// The zenith water-continuum opacity for `water_column_m`, nepers.
    #[must_use]
    pub fn h2o_continuum_opacity(&self, channel: usize, water_column_m: f64) -> f64 {
        self.integrate(channel, |l| l.h2o_continuum.im) * self.water_scale(water_column_m)
    }

    /// The zenith dispersive (H2O lines) wet phase delay for
    /// `water_column_m`, degrees (`getDispersiveH2OPhaseDelay`).
    #[must_use]
    pub fn dispersive_h2o_phase_delay_deg(&self, channel: usize, water_column_m: f64) -> f64 {
        self.integrate(channel, |l| l.h2o_lines.re)
            * self.water_scale(water_column_m)
            * DEGREES_PER_RADIAN
    }

    /// The zenith non-dispersive (water continuum) wet phase delay for
    /// `water_column_m`, degrees (`getNonDispersiveH2OPhaseDelay`).
    #[must_use]
    pub fn non_dispersive_h2o_phase_delay_deg(&self, channel: usize, water_column_m: f64) -> f64 {
        self.integrate(channel, |l| l.h2o_continuum.re)
            * self.water_scale(water_column_m)
            * DEGREES_PER_RADIAN
    }

    /// The zenith dispersive dry phase delay (the dry lines), degrees
    /// (`getDispersiveDryPhaseDelay`).
    #[must_use]
    pub fn dispersive_dry_phase_delay_deg(&self, channel: usize) -> f64 {
        self.integrate(channel, |l| l.dry_lines.re) * DEGREES_PER_RADIAN
    }

    /// The zenith non-dispersive dry phase delay (the dry continuum),
    /// degrees (`getNonDispersiveDryPhaseDelay`).
    #[must_use]
    pub fn non_dispersive_dry_phase_delay_deg(&self, channel: usize) -> f64 {
        self.integrate(channel, |l| l.dry_continuum.re) * DEGREES_PER_RADIAN
    }

    /// The path length a phase delay of `phase_deg` in `channel` amounts
    /// to, metres: (λ/360°)·φ, as ATM converts.
    #[must_use]
    pub fn path_length_m(&self, channel: usize, phase_deg: f64) -> f64 {
        (self.wavelength_m(channel) / 360.0) * phase_deg
    }

    /// The zenith dispersive wet path length for `water_column_m`, metres.
    #[must_use]
    pub fn dispersive_h2o_path_length_m(&self, channel: usize, water_column_m: f64) -> f64 {
        self.path_length_m(
            channel,
            self.dispersive_h2o_phase_delay_deg(channel, water_column_m),
        )
    }

    /// The zenith non-dispersive wet path length for `water_column_m`,
    /// metres.
    #[must_use]
    pub fn non_dispersive_h2o_path_length_m(&self, channel: usize, water_column_m: f64) -> f64 {
        self.path_length_m(
            channel,
            self.non_dispersive_h2o_phase_delay_deg(channel, water_column_m),
        )
    }

    /// The zenith dispersive dry path length, metres.
    #[must_use]
    pub fn dispersive_dry_path_length_m(&self, channel: usize) -> f64 {
        self.path_length_m(channel, self.dispersive_dry_phase_delay_deg(channel))
    }

    /// The zenith non-dispersive dry path length, metres.
    #[must_use]
    pub fn non_dispersive_dry_path_length_m(&self, channel: usize) -> f64 {
        self.path_length_m(channel, self.non_dispersive_dry_phase_delay_deg(channel))
    }

    /// The dry and wet absorption coefficients (1/m) of layer `layer` in
    /// `channel`, the wet one for the profile's own water (ATM's
    /// `getAbsTotalDry` and `getAbsTotalWet`).
    #[must_use]
    pub fn layer_absorption(&self, channel: usize, layer: usize) -> (f64, f64) {
        let totals = self.channels[channel].layer_totals(layer);
        (totals.dry_absorption(), totals.wet_absorption())
    }

    /// The mean of `value` over the channels of spectral window `window`,
    /// as ATM's `getAverage…` accessors take it (the sum over the channels
    /// divided by their number).
    ///
    /// # Panics
    ///
    /// If `window` is not a window of the grid.
    pub fn window_average(&self, window: usize, value: impl Fn(usize) -> f64) -> f64 {
        let first = self
            .grid
            .flat_index(window, 0)
            .expect("a spectral window of the grid");
        let count = self.grid.windows()[window].num_channels();
        let sum = (first..first + count).fold(0.0, |sum, channel| sum + value(channel));
        sum / count as f64
    }
}

impl AbsorptionProfile<ChannelAbsorption> {
    /// The refractivities of layer `layer` in `channel`, by species.
    #[must_use]
    pub fn layer(&self, channel: usize, layer: usize) -> &LayerAbsorption {
        &self.channels[channel].layers()[layer]
    }

    fn integrate_absorber(
        &self,
        channel: usize,
        absorber: Absorber,
        part: impl Fn(Complex64) -> f64,
    ) -> f64 {
        let column = self.channels[channel].layers();
        let mut sum = 0.0;
        for (layer, atmosphere) in column.iter().zip(self.atmosphere.layers()) {
            sum += part(absorber.of(layer)) * atmosphere.thickness_m;
        }
        sum
    }

    /// The zenith opacity of one species family, nepers (ATM's
    /// `getO2LinesOpacity` and its siblings; a wet family for the profile's
    /// own water column).
    #[must_use]
    pub fn absorber_opacity(&self, channel: usize, absorber: Absorber) -> f64 {
        self.integrate_absorber(channel, absorber, |n| n.im)
    }

    /// The zenith phase delay of one species family, degrees (ATM's
    /// `getO2LinesPhaseDelay` and its siblings).
    #[must_use]
    pub fn absorber_phase_delay_deg(&self, channel: usize, absorber: Absorber) -> f64 {
        self.integrate_absorber(channel, absorber, |n| n.re) * DEGREES_PER_RADIAN
    }

    /// The zenith path length of one species family, metres (ATM's
    /// `getO2LinesPathLength` and its siblings).
    #[must_use]
    pub fn absorber_path_length_m(&self, channel: usize, absorber: Absorber) -> f64 {
        self.path_length_m(channel, self.absorber_phase_delay_deg(channel, absorber))
    }
}
