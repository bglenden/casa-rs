// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMRefractiveIndexProfile.cc, (c) Instituto de
// Estructura de la Materia, Pardo et al., LGPL-2.1-or-later.
//! The atmosphere's absorption and phase over a spectral grid (ATM's
//! `RefractiveIndexProfile`).
//!
//! For every channel and layer, the layer's refractivity per unit path
//! N = N′ + iN″ comes from [`crate::refractivity`] at the layer's mean
//! pressure, temperature and water vapour and the gases' number densities.
//! Integrating over the layers gives, at the zenith,
//!
//! - the opacity τ = Σ N″ⱼ Δzⱼ (nepers), and
//! - the phase delay φ = Σ N′ⱼ Δzⱼ (radians; reported in degrees) and the
//!   path length L = (λ/2π) φ.
//!
//! ATM separates the *dry* atmosphere (the O2, O3, CO, N2O, NO2, SO2, HCl
//! and HCN lines and the dry continuum) from the *wet* one (the H2O lines
//! and the water continuum), and the *dispersive* delay (the lines) from
//! the *non-dispersive* one (the continua). The wet terms are computed for
//! the profile's own water column ([`AtmosphereProfile::ground_water_column_m`])
//! and scale linearly to any other: a wet quantity for column w is the
//! profile's times w / w₀.
//!
//! # Two forms
//!
//! [`ChannelAbsorption`] keeps every species ATM keeps (28 complex values a
//! layer) and answers every per-species accessor. [`ChannelTotals`] keeps
//! only the dry lines, dry continuum, H2O lines and H2O continuum (4 a
//! layer), which is all that opacities, path lengths and sky brightness
//! need. [`AbsorptionProfile`] works over either; the per-species accessors
//! need the full form.
//!
//! # Building in parallel
//!
//! Each channel is a pure function of the atmosphere and its frequency
//! ([`channel_absorption`], [`channel_totals`]). A caller that wants
//! channels computed on its own threads computes them and passes them to
//! [`AbsorptionProfile::from_channels`]; [`AbsorptionProfile::new`] computes
//! them serially. The crate starts no threads. [`AbsorptionProfile::bytes`]
//! gives the memory a profile keeps, so a caller can admit it first.

mod opacity;

pub use opacity::Absorber;

use crate::refractivity::{Species, h2o_per_path, o2_per_path, specific_refractivity};
use crate::{AtmosphereError, AtmosphereProfile, Layer, SpectralGrid};
use num_complex::Complex64;

/// One layer's refractivity per unit path at one frequency, by species,
/// (rad/m, 1/m): everything ATM's `RefractiveIndexProfile` keeps.
///
/// The O2, H2O and O3 totals are ATM's own sums of their parts, so they
/// equal ATM's to the bit.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LayerAbsorption {
    /// All O2 lines: the next four summed.
    pub o2: Complex64,
    /// (16)O(16)O lines from the ground state.
    pub o2_16o16o: Complex64,
    /// (16)O(16)O lines from the first vibrational state.
    pub o2_vibrational: Complex64,
    /// (16)O(18)O lines.
    pub o2_16o18o: Complex64,
    /// (16)O(17)O lines (zero; see [`Species::O2Isotope17`]).
    pub o2_16o17o: Complex64,
    /// The dry continuum.
    pub dry_continuum: Complex64,
    /// All H2O lines: the next five summed.
    pub h2o: Complex64,
    /// H2(16)O lines from the ground state.
    pub hh16o: Complex64,
    /// H2(16)O lines from the first bending state.
    pub hh16o_bending: Complex64,
    /// H2(17)O lines.
    pub hh17o: Complex64,
    /// H2(18)O lines.
    pub hh18o: Complex64,
    /// HDO lines.
    pub hdo: Complex64,
    /// The water continuum.
    pub h2o_continuum: Complex64,
    /// All O3 lines: the next eight summed.
    pub o3: Complex64,
    /// (16)O3 lines from the ground state.
    pub o3_ground: Complex64,
    /// (16)O3 lines from the ν1 state.
    pub o3_nu1: Complex64,
    /// (16)O3 lines from the ν2 state.
    pub o3_nu2: Complex64,
    /// (16)O3 lines from the ν3 state.
    pub o3_nu3: Complex64,
    /// (16)O(16)O(18)O lines.
    pub o3_668: Complex64,
    /// (16)O(18)O(16)O lines.
    pub o3_686: Complex64,
    /// (16)O(16)O(17)O lines.
    pub o3_667: Complex64,
    /// (16)O(17)O(16)O lines.
    pub o3_676: Complex64,
    /// CO lines.
    pub co: Complex64,
    /// N2O lines.
    pub n2o: Complex64,
    /// NO2 lines.
    pub no2: Complex64,
    /// SO2 lines.
    pub so2: Complex64,
    /// HCl lines.
    pub hcl: Complex64,
    /// HCN lines.
    pub hcn: Complex64,
}

/// One layer's refractivity per unit path at one frequency, in the four
/// totals opacities, path lengths and sky brightness need, (rad/m, 1/m).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LayerTotals {
    /// The dry lines: O2, O3, CO, N2O, NO2, SO2, HCl and HCN.
    pub dry_lines: Complex64,
    /// The dry continuum.
    pub dry_continuum: Complex64,
    /// The H2O lines.
    pub h2o_lines: Complex64,
    /// The water continuum.
    pub h2o_continuum: Complex64,
}

impl LayerAbsorption {
    /// The species' refractivities in `layer` at `frequency_ghz`, as ATM's
    /// `mkRefractiveIndexProfile` forms them: each species once, the O2,
    /// H2O and O3 totals as ATM sums them, and nothing for a gas the layer
    /// has none of.
    #[must_use]
    pub fn of_layer(layer: &Layer, frequency_ghz: f64) -> Self {
        let (t, p, nu) = (layer.temperature_k, layer.pressure_hpa, frequency_ghz);
        // ATM's water vapour partial pressure from g/m³.
        let e = layer.water_vapour_kg_m3 * 1000.0 * t / 217.0;
        let s = |species| specific_refractivity(species, t, p, e, nu);
        let mut a = Self::default();

        let o2 = [
            Species::O2,
            Species::O2Vibrational,
            Species::O2Isotope18,
            Species::O2Isotope17,
        ]
        .map(s);
        a.o2 = o2_per_path(o2[0] + o2[1] + o2[2] + o2[3], t, p);
        [a.o2_16o16o, a.o2_vibrational, a.o2_16o18o, a.o2_16o17o] =
            o2.map(|x| o2_per_path(x, t, p));
        a.h2o_continuum = s(Species::WaterContinuum);
        a.dry_continuum = s(Species::DryContinuum);

        if layer.water_vapour_kg_m3 > 0.0 {
            let w = [
                Species::H2o,
                Species::H2oBending,
                Species::H2oIsotope18,
                Species::H2oIsotope17,
                Species::Hdo,
            ]
            .map(s);
            a.h2o = h2o_per_path(w[0] + w[1] + w[2] + w[3] + w[4], t, e);
            [a.hh16o, a.hh16o_bending, a.hh18o, a.hh17o, a.hdo] = w.map(|x| h2o_per_path(x, t, e));
        }
        if layer.o3_per_m3 > 0.0 {
            let density = layer.o3_per_m3 * 1e-6 * 1e6;
            let o3 = [
                Species::O3,
                Species::O3Nu1,
                Species::O3Nu2,
                Species::O3Nu3,
                Species::O3Isotope668,
                Species::O3Isotope686,
                Species::O3Isotope667,
                Species::O3Isotope676,
            ]
            .map(|species| specific_refractivity(species, t, p, 0.0, nu) * density);
            [
                a.o3_ground,
                a.o3_nu1,
                a.o3_nu2,
                a.o3_nu3,
                a.o3_668,
                a.o3_686,
                a.o3_667,
                a.o3_676,
            ] = o3;
            a.o3 = o3[0] + o3[1] + o3[2] + o3[3] + o3[4] + o3[5] + o3[6] + o3[7];
        }
        let minor = |species, density: f64| {
            if density > 0.0 {
                specific_refractivity(species, t, p, 0.0, nu) * (density * 1e-6) * 1e6
            } else {
                Complex64::new(0.0, 0.0)
            }
        };
        a.co = minor(Species::Co, layer.co_per_m3);
        a.n2o = minor(Species::N2o, layer.n2o_per_m3);
        a.no2 = minor(Species::No2, layer.no2_per_m3);
        a.hcl = minor(Species::Hcl, layer.hcl_per_m3);
        a.hcn = minor(Species::Hcn, layer.hcn_per_m3);
        a.so2 = minor(Species::So2, layer.so2_per_m3);
        a
    }

    /// The four totals.
    #[must_use]
    pub fn totals(&self) -> LayerTotals {
        LayerTotals {
            dry_lines: self.o2
                + self.o3
                + self.co
                + self.n2o
                + self.no2
                + self.so2
                + self.hcl
                + self.hcn,
            dry_continuum: self.dry_continuum,
            h2o_lines: self.h2o,
            h2o_continuum: self.h2o_continuum,
        }
    }
}

impl LayerTotals {
    /// The dry absorption coefficient (1/m): ATM's `getAbsTotalDry`.
    #[must_use]
    pub fn dry_absorption(&self) -> f64 {
        self.dry_lines.im + self.dry_continuum.im
    }

    /// The wet absorption coefficient (1/m) for the profile's own water:
    /// ATM's `getAbsTotalWet`.
    #[must_use]
    pub fn wet_absorption(&self) -> f64 {
        (self.h2o_lines + self.h2o_continuum).im
    }
}

/// What a profile keeps for one channel: its frequency and its layers.
pub trait AbsorptionChannel {
    /// The channel's frequency, Hz.
    fn frequency_hz(&self) -> f64;
    /// The number of layers.
    fn layer_count(&self) -> usize;
    /// The totals of layer `layer`.
    fn layer_totals(&self, layer: usize) -> LayerTotals;
    /// The heap bytes a channel keeps for `layers` layers.
    fn heap_bytes(layers: usize) -> u64;
}

/// Every species' refractivity in every layer of one channel.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelAbsorption {
    frequency_hz: f64,
    layers: Vec<LayerAbsorption>,
}

impl ChannelAbsorption {
    /// The layers, from the ground up.
    #[must_use]
    pub fn layers(&self) -> &[LayerAbsorption] {
        &self.layers
    }

    /// The compact form of this channel.
    #[must_use]
    pub fn totals(&self) -> ChannelTotals {
        ChannelTotals {
            frequency_hz: self.frequency_hz,
            layers: self.layers.iter().map(LayerAbsorption::totals).collect(),
        }
    }
}

/// The four totals in every layer of one channel.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelTotals {
    frequency_hz: f64,
    layers: Vec<LayerTotals>,
}

impl ChannelTotals {
    /// The layers, from the ground up.
    #[must_use]
    pub fn layers(&self) -> &[LayerTotals] {
        &self.layers
    }
}

impl AbsorptionChannel for ChannelAbsorption {
    fn frequency_hz(&self) -> f64 {
        self.frequency_hz
    }
    fn layer_count(&self) -> usize {
        self.layers.len()
    }
    fn layer_totals(&self, layer: usize) -> LayerTotals {
        self.layers[layer].totals()
    }
    fn heap_bytes(layers: usize) -> u64 {
        (layers * size_of::<LayerAbsorption>()) as u64
    }
}

impl AbsorptionChannel for ChannelTotals {
    fn frequency_hz(&self) -> f64 {
        self.frequency_hz
    }
    fn layer_count(&self) -> usize {
        self.layers.len()
    }
    fn layer_totals(&self, layer: usize) -> LayerTotals {
        self.layers[layer]
    }
    fn heap_bytes(layers: usize) -> u64 {
        (layers * size_of::<LayerTotals>()) as u64
    }
}

/// Every species' refractivity in every layer of `atmosphere` at
/// `frequency_hz`: one channel of a full [`AbsorptionProfile`]. Pure, so
/// channels can be computed on any threads.
#[must_use]
pub fn channel_absorption(atmosphere: &AtmosphereProfile, frequency_hz: f64) -> ChannelAbsorption {
    let nu = 1.0e-9 * frequency_hz;
    let mut layers = Vec::with_capacity(atmosphere.layers().len());
    layers.extend(
        atmosphere
            .layers()
            .iter()
            .map(|layer| LayerAbsorption::of_layer(layer, nu)),
    );
    ChannelAbsorption {
        frequency_hz,
        layers,
    }
}

/// The four totals in every layer of `atmosphere` at `frequency_hz`: one
/// channel of a compact [`AbsorptionProfile`]. Pure, so channels can be
/// computed on any threads.
#[must_use]
pub fn channel_totals(atmosphere: &AtmosphereProfile, frequency_hz: f64) -> ChannelTotals {
    let nu = 1.0e-9 * frequency_hz;
    let mut layers = Vec::with_capacity(atmosphere.layers().len());
    layers.extend(
        atmosphere
            .layers()
            .iter()
            .map(|layer| LayerAbsorption::of_layer(layer, nu).totals()),
    );
    ChannelTotals {
        frequency_hz,
        layers,
    }
}

/// The absorption and phase of an atmosphere over a spectral grid (ATM's
/// `RefractiveIndexProfile`), in the full ([`ChannelAbsorption`]) or
/// compact ([`ChannelTotals`]) form.
#[derive(Clone, Debug, PartialEq)]
pub struct AbsorptionProfile<C = ChannelAbsorption> {
    atmosphere: AtmosphereProfile,
    grid: SpectralGrid,
    channels: Vec<C>,
    ground_water_column_m: f64,
}

impl AbsorptionProfile<ChannelAbsorption> {
    /// The full profile of `atmosphere` over `grid`, computed serially.
    #[must_use]
    pub fn new(atmosphere: AtmosphereProfile, grid: SpectralGrid) -> Self {
        let channels = grid
            .frequencies_hz()
            .map(|frequency| channel_absorption(&atmosphere, frequency))
            .collect();
        Self::assemble(atmosphere, grid, channels)
    }
}

impl AbsorptionProfile<ChannelTotals> {
    /// The compact profile of `atmosphere` over `grid`, computed serially.
    #[must_use]
    pub fn new_totals(atmosphere: AtmosphereProfile, grid: SpectralGrid) -> Self {
        let channels = grid
            .frequencies_hz()
            .map(|frequency| channel_totals(&atmosphere, frequency))
            .collect();
        Self::assemble(atmosphere, grid, channels)
    }
}

impl<C: AbsorptionChannel> AbsorptionProfile<C> {
    /// A profile from channels computed elsewhere (for example in parallel
    /// with [`channel_absorption`] or [`channel_totals`]), one per grid
    /// channel in the grid's flat order.
    ///
    /// # Errors
    ///
    /// [`AtmosphereError::InvalidParameter`] if the channel count, a
    /// channel's frequency or its layer count does not match `grid` and
    /// `atmosphere`.
    pub fn from_channels(
        atmosphere: AtmosphereProfile,
        grid: SpectralGrid,
        channels: Vec<C>,
    ) -> Result<Self, AtmosphereError> {
        if channels.len() != grid.total_channels() {
            return Err(AtmosphereError::InvalidParameter(format!(
                "{} channels for a grid of {}",
                channels.len(),
                grid.total_channels()
            )));
        }
        let layers = atmosphere.layers().len();
        for (index, (channel, frequency)) in channels.iter().zip(grid.frequencies_hz()).enumerate()
        {
            if channel.frequency_hz().to_bits() != frequency.to_bits() {
                return Err(AtmosphereError::InvalidParameter(format!(
                    "channel {index} is at {} Hz, the grid's at {frequency} Hz",
                    channel.frequency_hz()
                )));
            }
            if channel.layer_count() != layers {
                return Err(AtmosphereError::InvalidParameter(format!(
                    "channel {index} has {} layers, the atmosphere {layers}",
                    channel.layer_count()
                )));
            }
        }
        Ok(Self::assemble(atmosphere, grid, channels))
    }

    fn assemble(atmosphere: AtmosphereProfile, grid: SpectralGrid, channels: Vec<C>) -> Self {
        let ground_water_column_m = atmosphere.ground_water_column_m();
        Self {
            atmosphere,
            grid,
            channels,
            ground_water_column_m,
        }
    }

    /// The bytes a profile of this form keeps for `grid` and an atmosphere
    /// of `layers` layers: its channels (exactly sized), the grid's
    /// frequencies and windows and the atmosphere's layers. Building it
    /// holds nothing more: each layer is formed in place.
    #[must_use]
    pub fn bytes(grid: &SpectralGrid, layers: usize) -> u64 {
        let channels = grid.total_channels() as u64;
        channels * (size_of::<C>() as u64 + C::heap_bytes(layers))
            + channels * size_of::<f64>() as u64
            + size_of_val(grid.windows()) as u64
            + (layers * size_of::<Layer>()) as u64
    }

    /// The atmosphere.
    #[must_use]
    pub fn atmosphere(&self) -> &AtmosphereProfile {
        &self.atmosphere
    }

    /// The spectral grid.
    #[must_use]
    pub fn grid(&self) -> &SpectralGrid {
        &self.grid
    }

    /// The channels, in the grid's flat order.
    #[must_use]
    pub fn channels(&self) -> &[C] {
        &self.channels
    }

    /// The water column the wet terms are computed for, metres
    /// ([`AtmosphereProfile::ground_water_column_m`]).
    #[must_use]
    pub fn ground_water_column_m(&self) -> f64 {
        self.ground_water_column_m
    }
}
